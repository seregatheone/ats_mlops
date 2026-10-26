"""One immutable executable source boundary around a mutable four-stage DVC DAG.

Init and cache receipts use stdlib only. ML/GX imports occur inside workers.
Admission/bundle construction is deliberately separate from init/repro.
"""

from copy import deepcopy
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import uuid

from ats_mlops.data.dataset import canonical_json, sha256
from ats_mlops.lineage.manifest import file_hash, inventory_hash, tree_inventory, write_json

PARTITIONS = ("train", "validation")
POINTERS = {"train": "data/train.jsonl.dvc", "validation": "data/validation.jsonl.dvc",
            "split_lists": "data/split.json.dvc"}
STAGES = ("validate", "prepare", "train", "evaluate")


def run_command(args, *, cwd=None, env=None):
    return subprocess.run(args, cwd=cwd, env=env, check=True, capture_output=True, text=True).stdout.strip()


def safe_path(root: Path, relative: str) -> Path:
    """Manifest and bundle paths never escape their own materialization root."""
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute() or ".." in Path(relative).parts:
        raise ValueError("unsafe_artifact_path")
    path = root / relative
    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("unsafe_artifact_path")
    return path


def read_json(path):
    return json.loads(Path(path).read_text())


def pointer_binding(workspace: Path, manifest: dict, *, admission=False) -> dict:
    import yaml

    result = {}
    for name, relative in POINTERS.items():
        pointer = safe_path(workspace, relative)
        document = yaml.safe_load(pointer.read_text())
        if (not isinstance(document, dict) or set(document) != {"outs"}
                or not isinstance(document["outs"], list) or len(document["outs"]) != 1
                or not isinstance(document["outs"][0], dict)):
            raise ValueError("invalid_dvc_pointer")
        out = document["outs"][0]
        if (out.get("hash") != "md5" or not isinstance(out.get("md5"), str)
                or not re.fullmatch(r"[0-9a-f]{32}", out["md5"])
                or not isinstance(out.get("size"), int) or out["size"] <= 0):
            raise ValueError("invalid_dvc_pointer")
        target = safe_path(pointer.parent, out["path"])
        if target != safe_path(workspace, manifest["artifacts"][name]["path"]):
            raise ValueError("pointer_manifest_path_mismatch")
        if admission or name in PARTITIONS:
            content = target.read_bytes()
            if (sha256(content) != manifest["artifacts"][name]["sha256"]
                    or hashlib.md5(content).hexdigest() != out["md5"] or len(content) != out["size"]):
                raise ValueError("pointer_data_hash_mismatch")
        result[name] = {"pointer_path": relative, "pointer_sha256": file_hash(pointer),
                        "artifact_path": manifest["artifacts"][name]["path"],
                        "md5": out["md5"], "size": out["size"],
                        "sha256": manifest["artifacts"][name]["sha256"]}
    return result


def check_binding(workspace: Path) -> dict:
    from ats_mlops.data.dataset import load_manifest

    manifest = load_manifest(workspace / "data_manifest.json")
    binding = read_json(workspace / "dataset-binding.json")
    if (binding.get("schema_version") != 1 or binding.get("evidence_scope") != "demo_only"
            or binding.get("manifest_sha256") != file_hash(workspace / "data_manifest.json")
            or binding.get("admission_sha256") != file_hash(workspace / "admission/data-admission.json")
            or binding.get("workspace_pointers") != pointer_binding(workspace, manifest)):
        raise ValueError("dataset_binding_mismatch")
    return binding


def create_bundle(source_root: Path, config_path: Path, output: Path) -> dict:
    """One-time admission, with split bytes; never invoked by init or DVC stages."""
    from ats_mlops.data.dataset import generate_dataset
    from ats_mlops.data.validate import validate_dataset

    if not Path(__file__).resolve().is_relative_to(source_root.resolve() / "src"):
        raise ValueError("runtime_source_checkout_mismatch")
    if output.exists():
        raise ValueError("new_bundle_directory_required")
    with tempfile.TemporaryDirectory(prefix="ats-admission-", dir=output.parent) as temporary:
        staging = Path(temporary)
        run_command(["dvc", "init", "--no-scm", "-q"], cwd=staging)
        generate_dataset(config_path, staging / "data_manifest.json", staging / "data")
        receipt = validate_dataset(staging / "data_manifest.json", staging / "admission")
        if receipt["outcome"] != "passed":
            raise ValueError("admission_failed")
        run_command(["dvc", "add", "data/train.jsonl", "data/validation.jsonl", "data/split.json"], cwd=staging)
        manifest = read_json(staging / "data_manifest.json")
        binding = {"schema_version": 1, "evidence_scope": "demo_only",
                   "manifest_sha256": file_hash(staging / "data_manifest.json"),
                   "admission_sha256": file_hash(staging / "admission/data-admission.json"),
                   "workspace_pointers": pointer_binding(staging, manifest, admission=True)}
        write_json(staging / "dataset-binding.json", binding)
        files = ["data_manifest.json", "admission/data-admission.json", "dataset-binding.json",
                 manifest["reservation"]["policy"]["path"],
                 *POINTERS.values(), *(manifest["artifacts"][name]["path"] for name in PARTITIONS)]
        output.mkdir()
        for name in files:
            target = safe_path(output, name)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(safe_path(staging, name), target)
        inventory = tree_inventory(output)
        descriptor = {"schema_version": 1, "files": inventory, "sha256": inventory_hash(inventory),
                      "read_partitions": ["train", "validation"], "admission_complete": True,
                      "generator_config_sha256": file_hash(config_path)}
        write_json(output / "bundle.json", descriptor)
    return descriptor


def verify_bundle(bundle: Path, expected_hash: str) -> dict:
    from ats_mlops.data.dataset import load_manifest
    from ats_mlops.data.reservation import manifest_policy

    descriptor = read_json(bundle / "bundle.json")
    files = descriptor.get("files", {})
    required = {"data_manifest.json", "admission/data-admission.json", "dataset-binding.json", *POINTERS.values()}
    if (descriptor.get("schema_version") != 1 or not required.issubset(files)
            or expected_hash != inventory_hash(files) or descriptor.get("sha256") != expected_hash):
        raise ValueError("bundle_identity_mismatch")
    manifest = load_manifest(bundle / "data_manifest.json")
    manifest_policy(bundle / "data_manifest.json", manifest)
    allowed = required | {manifest["artifacts"][name]["path"] for name in PARTITIONS} | {
        manifest["reservation"]["policy"]["path"]}
    if set(files) != allowed:
        raise ValueError("bundle_files_must_be_train_validation_only")
    for name, digest in files.items():
        if file_hash(safe_path(bundle, name)) != digest:
            raise ValueError("bundle_content_mismatch")
    return descriptor


def initialize(repo: Path, git_sha: str, directory: Path, bundle: Path, bundle_sha: str,
               image_identity: dict, *, runtime_python="/opt/ml-venv/bin/python") -> dict:
    """Clone executable source separately; install an already admitted bundle."""
    if not re.fullmatch(r"[0-9a-f]{40}", git_sha):
        raise ValueError("invalid_source_sha")
    if not isinstance(image_identity, dict) or not re.fullmatch(r"sha256:[0-9a-f]{64}", image_identity.get("id", "")):
        raise ValueError("resolved_image_identity_required")
    descriptor = verify_bundle(bundle, bundle_sha)
    directory.mkdir(parents=True, exist_ok=False)
    for name in ("source", "workspace"):
        run_command(["git", "clone", "--no-local", str(repo.resolve()), str(directory / name)])
        run_command(["git", "checkout", "--detach", git_sha], cwd=directory / name)
    source, workspace = directory / "source", directory / "workspace"
    if run_command(["git", "status", "--porcelain", "--untracked-files=all"], cwd=source):
        raise ValueError("clean_source_commit_required")
    for name in descriptor["files"]:
        target = safe_path(workspace, name)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(safe_path(bundle, name), target)
    spec = {"schema_version": 1, "git_sha": git_sha, "source_location": "../source",
            "source_tree_sha256": inventory_hash(tree_inventory(source / "src/ats_mlops")),
            "recipe_sha256": file_hash(source / "dvc.yaml"),
            "lock_sha256": file_hash(source / "requirements-ml.lock"),
            "build_lock_sha256": file_hash(source / "requirements-build.lock"),
            "source_configs": {name: file_hash(source / name) for name in ("configs/model.json", "configs/search.json")},
            "image": image_identity, "runtime_python": runtime_python,
            "bundle_sha256": bundle_sha, "bundle_files": descriptor["files"]}
    write_json(workspace / "pipeline-inputs.json", spec)
    return spec


def context(workspace: Path, source_root: Path) -> tuple[dict, dict]:
    from ats_mlops.lineage.manifest import source_environment

    workspace, source_root = workspace.resolve(), source_root.resolve()
    if source_root != workspace.parent / "source" or source_root == workspace:
        raise ValueError("separate_sibling_source_required")
    spec = read_json(workspace / "pipeline-inputs.json")
    if Path(sys.executable).resolve() != Path(spec["runtime_python"]).resolve():
        raise ValueError("pinned_interpreter_required")
    if not Path(__file__).resolve().is_relative_to(source_root / "src"):
        raise ValueError("runtime_source_checkout_mismatch")
    actual = source_environment(source_root, spec["image"], runtime_lock=Path("/opt/requirements-ml.lock"))
    expected_arch = {"arm64": "aarch64", "amd64": "x86_64"}.get(spec["image"].get("architecture"))
    if (spec["image"].get("os") != platform.system().lower() or expected_arch != platform.machine()):
        raise ValueError("runtime_image_platform_mismatch")
    if (actual["git_sha"] != spec["git_sha"] or actual["source_tree_sha256"] != spec["source_tree_sha256"]
            or actual["environment"]["lockfile_sha256"] != spec["lock_sha256"]
            or actual["environment"]["build_lockfile_sha256"] != spec["build_lock_sha256"]
            or spec["recipe_sha256"] != file_hash(workspace / "dvc.yaml")
            or file_hash(source_root / "dvc.yaml") != spec["recipe_sha256"]
            or any(file_hash(source_root / name) != digest for name, digest in spec["source_configs"].items())):
        raise ValueError("source_recipe_or_environment_mismatch")
    if any(file_hash(safe_path(workspace, name)) != digest for name, digest in spec["bundle_files"].items()
           if name not in {"data/train.jsonl", "data/validation.jsonl"}):
        raise ValueError("admitted_bundle_metadata_changed")
    return spec, actual


def selection_mode(search, smoke):
    """Lightweight request preflight, also used before orchestration effects."""
    if not isinstance(search, dict) or type(smoke) is not bool:
        raise ValueError("invalid_selection_mode")
    mode = search.get("mode", "optuna")
    if type(mode) is not str or mode not in {"optuna", "frozen"}:
        raise ValueError("invalid_selection_mode")
    if mode == "frozen" and (set(search) != {"mode"} or smoke):
        raise ValueError("invalid_frozen_selection")
    return mode


def parameters(workspace: Path, source_root: Path) -> dict:
    import yaml
    from ats_mlops.model.training import make_pipeline
    from ats_mlops.model.tune import validate_search

    params = yaml.safe_load((workspace / "params.yaml").read_text())
    if (not isinstance(params, dict) or set(params) != {"model", "search", "smoke", "evaluation"}
            or type(params["smoke"]) is not bool or not isinstance(params["model"], dict)
            or not isinstance(params["search"], dict) or set(params["evaluation"]) != {"score_decimals"}
            or type(params["evaluation"]["score_decimals"]) is not int
            or not 6 <= params["evaluation"]["score_decimals"] <= 15):
        raise ValueError("invalid_pipeline_params")

    def merge(base, updates):
        for name, value in updates.items():
            if name not in base:
                raise ValueError("unknown_parameter")
            if isinstance(base[name], dict):
                if not isinstance(value, dict):
                    raise ValueError("invalid_parameter_type")
                merge(base[name], value)
            else:
                base[name] = deepcopy(value)
        return base

    model = merge(read_json(source_root / "configs/model.json"), params["model"])
    mode = selection_mode(params["search"], params["smoke"])
    search = ({"mode": "frozen"} if mode == "frozen" else merge(read_json(source_root / "configs/search.json"),
              {key: value for key, value in params["search"].items() if key != "mode"}))
    make_pipeline(model)
    if mode != "frozen":
        validate_search(search)
    return {"model": model, "search": search, "smoke": params["smoke"]}


def execution_event(workspace: Path, stage: str, payload: dict) -> None:
    invocation = os.environ.get("ATS_PIPELINE_INVOCATION", "direct-" + uuid.uuid4().hex)
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", invocation):
        raise ValueError("invalid_invocation_id")
    write_json(workspace / ".local/executions" / invocation / (stage + ".json"),
               {"stage": stage, "executed": True, **payload})


def classify_repro(returncode: int, events: dict) -> str:
    return "failed" if returncode else "new_training" if "train" in events else "cache_hit"


def verify_prepared(workspace: Path, source_root: Path, binding: dict) -> dict:
    """Reject substituted configs or another valid admitted dataset before fit."""
    from ats_mlops.features.pairs import FEATURE_CONTRACT

    prepared = workspace / "artifacts/prepared"
    expected = parameters(workspace, source_root)
    if (file_hash(prepared / "data_manifest.json") != binding["manifest_sha256"]
            or file_hash(prepared / "admission.json") != binding["admission_sha256"]
            or (prepared / "fit-params.json").read_bytes() != canonical_json(expected)
            or (prepared / "model.json").read_bytes() != canonical_json(expected["model"])
            or (prepared / "search.json").read_bytes() != canonical_json(expected["search"])
            or (prepared / "feature-contract.json").read_bytes() != canonical_json(FEATURE_CONTRACT)):
        raise ValueError("prepared_snapshot_mismatch")
    return expected


def frozen_selection(prepared, source):
    """A config selection receipt, not an Optuna study or a training result."""
    selected = read_json(prepared / "model.json")
    return {"schema_version": 1, "kind": "frozen", "outcome": "selected", "new_trials": 0,
            "selected_config": selected, "selected_config_sha256": file_hash(prepared / "model.json"),
            "effective_fit_sha256": file_hash(prepared / "fit-params.json"),
            "prepared_inventory_sha256": inventory_hash(tree_inventory(prepared)),
            "data_manifest_sha256": file_hash(prepared / "data_manifest.json"),
            "admission_sha256": file_hash(prepared / "admission.json"),
            "source": {"git_sha": source["git_sha"], "source_tree_sha256": source["source_tree_sha256"],
                       "image_id": source["environment"]["image"]["id"],
                       "lockfile_sha256": source["environment"]["lockfile_sha256"]}}


def stage(name: str, workspace: Path, source_root: Path) -> dict:
    """Real stages publish fresh generated outputs; old immutable studies stay intact."""
    from ats_mlops.data.dataset import load_manifest
    from ats_mlops.data.validate import require_valid

    if name not in STAGES:
        raise ValueError("unknown_pipeline_stage")
    workspace, source_root = workspace.resolve(), source_root.resolve()
    spec, source = context(workspace, source_root)
    binding = check_binding(workspace)
    parameters(workspace, source_root)  # Reject a bad mode before publishing any stage effects.
    artifacts = workspace / "artifacts"
    artifacts.mkdir(exist_ok=True)
    output = artifacts / {"validate": "validation", "prepare": "prepared", "train": "training", "evaluate": "evaluation"}[name]
    # DVC normally removes its stage-owned output before a command; direct calls
    # must never overwrite previous studies or evidence.
    if output.exists():
        raise ValueError("new_stage_output_required")
    (workspace / ".local").mkdir(exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=name + "-", dir=workspace / ".local"))
    try:
        manifest_path = workspace / "data_manifest.json"
        receipt = workspace / "admission/data-admission.json"
        if name in {"validate", "prepare"}:
            rows = require_valid(manifest_path, staging / "guard", admission_report=receipt)
            manifest = load_manifest(manifest_path)
            result = {"binding_sha256": file_hash(workspace / "dataset-binding.json"),
                      "manifest_sha256": file_hash(manifest_path), "outcome": "passed"}
            if name == "prepare":
                for partition in PARTITIONS:
                    content = b"".join(canonical_json(row) for row in rows[partition])
                    if sha256(content) != manifest["artifacts"][partition]["sha256"]:
                        raise ValueError("prepared_data_mismatch")
                    path = safe_path(staging, manifest["artifacts"][partition]["path"])
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(content)
                shutil.copyfile(manifest_path, staging / "data_manifest.json")
                shutil.copyfile(receipt, staging / "admission.json")
                relative = manifest["reservation"]["policy"]["path"]
                target = safe_path(staging, relative)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(safe_path(workspace, relative), target)
                params = parameters(workspace, source_root)
                for key in ("model", "search"):
                    write_json(staging / (key + ".json"), params[key])
                write_json(staging / "fit-params.json", params)
                from ats_mlops.features.pairs import FEATURE_CONTRACT
                write_json(staging / "feature-contract.json", FEATURE_CONTRACT)
            write_json(staging / "prepared.json" if name == "prepare" else staging / "validated.json", result)
        elif name == "train":
            from ats_mlops.model.training import train

            prepared = artifacts / "prepared"
            fit_params = verify_prepared(workspace, source_root, binding)
            if selection_mode(fit_params["search"], fit_params["smoke"]) == "frozen":
                selection = frozen_selection(prepared, source)
                write_json(staging / "selection.json", selection)
                selected_path = prepared / "model.json"
                selected_sha = selection["selected_config_sha256"]
            else:
                from ats_mlops.model.tune import tune
                study = tune(prepared / "data_manifest.json", staging / "study", admission_report=prepared / "admission.json",
                             config_path=prepared / "model.json", search_path=prepared / "search.json",
                             source_root=source_root, image_identity=spec["image"], smoke=fit_params["smoke"])
                if study["outcome"] != "passed":
                    raise ValueError("study_failed")
                selected_path = staging / "study/selected-model.json"
                selected_sha = study["selected_config_sha256"]
            result = train(prepared / "data_manifest.json", staging / "final", admission_report=prepared / "admission.json",
                           config_path=selected_path, source_root=source_root,
                           image_identity=spec["image"], experiment="ats-t9", registered_model="ats-pair-ranking")
            if (result["params_sha256"] != selected_sha
                    or result["data_manifest_sha256"] != binding["manifest_sha256"]):
                raise ValueError("selected_config_or_data_binding_mismatch")
        else:
            result = evaluate(workspace, source_root, staging, source, binding)
        output.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(output)
        event = {"git_sha": spec["git_sha"], "outcome": "passed", "output": output.relative_to(workspace).as_posix()}
        if name == "train":
            event["mlflow"] = result["mlflow"]
        execution_event(workspace, name, event)
        return event
    except Exception:
        # Failed stage diagnostics stay outside DVC outs; do not publish partial
        # study/model evidence as a reusable successful output.
        failed = workspace / ".local/failed" / (name + "-" + uuid.uuid4().hex)
        failed.parent.mkdir(parents=True, exist_ok=True)
        if staging.exists():
            staging.rename(failed)
        raise


def evaluate(workspace, source_root, output, source, binding):
    import mlflow.sklearn
    import numpy as np
    from ats_mlops.data.dataset import load_manifest
    from ats_mlops.data.validate import require_valid
    from ats_mlops.evaluation.ranking import evaluate_ranking
    from ats_mlops.model.scoring import pair_frame, score_rows

    prepared, training = workspace / "artifacts/prepared", workspace / "artifacts/training"
    fit_params = verify_prepared(workspace, source_root, binding)
    record = read_json(training / "final/run_manifest.json")
    frozen = selection_mode(fit_params["search"], fit_params["smoke"]) == "frozen"
    if frozen:
        receipt = read_json(training / "selection.json")
        if canonical_json(receipt) != canonical_json(frozen_selection(prepared, source)) or (training / "study").exists():
            raise ValueError("frozen_selection_binding_mismatch")
        lineage = {"selection": {"kind": "frozen", "receipt": receipt,
                                  "receipt_sha256": file_hash(training / "selection.json")}}
        selected_sha = receipt["selected_config_sha256"]
    else:
        study = read_json(training / "study/study.json")
        if (study["outcome"] != "passed" or (training / "selection.json").exists()
                or file_hash(training / "study/selected-model.json") != study["selected_config_sha256"]):
            raise ValueError("training_model_or_study_binding_mismatch")
        selected_sha = study["selected_config_sha256"]
        lineage = {"study": {"sha256": file_hash(training / "study/study.json"), "configured_budget": study["configured_budget"],
                             "effective_budget": study["effective_budget"], "outcome": study["outcome"]}}
    if (record["git_sha"] != source["git_sha"]
            or record["data_manifest_sha256"] != binding["manifest_sha256"]
            or record["params_sha256"] != selected_sha
            or tree_inventory(training / "final/model") != record["model"]["files"]):
        raise ValueError("training_model_or_study_binding_mismatch")
    rows = require_valid(prepared / "data_manifest.json", output / "guard", admission_report=prepared / "admission.json")
    manifest = load_manifest(prepared / "data_manifest.json")
    model = mlflow.sklearn.load_model(str(training / "final/model"))
    scores = score_rows(model, rows["validation"])
    metrics = evaluate_ranking(rows["validation"], scores, group_ids=manifest["splits"]["validation"]["vacancy_ids"])
    metrics.pop("elapsed_seconds")
    write_json(output / "metrics.json", metrics)
    import yaml
    evaluation_params = yaml.safe_load((workspace / "params.yaml").read_text())["evaluation"]
    decimals = evaluation_params["score_decimals"]
    ordered_scores = [{"vacancy_id": key[0], "candidate_id": key[1], "score": round(value, decimals)}
                      for key, value in sorted(scores.items())]
    write_json(output / "scores.json", {"rows": ordered_scores, "parameters": evaluation_params})
    transformer = model.named_steps["pair_features"]
    features = {name: sha256(np.asarray(transformer.transform(pair_frame(rows[name])), dtype="<f8").tobytes())
                for name in PARTITIONS}
    fitted = {"vocabulary": {token: int(index) for token, index in transformer.vectorizer_.vocabulary_.items()},
              "idf": transformer.vectorizer_.idf_.tolist(), "transformed_sha256": features}
    write_json(output / "features.json", fitted)
    result = {"schema_version": 1, "outcome": "passed", "evidence_scope": "demo_only", "source": source,
              "inputs": {"binding": binding, "prepared_manifest_sha256": file_hash(prepared / "data_manifest.json"),
                         "prepared_files": tree_inventory(prepared),
                         "pipeline_inputs_sha256": file_hash(workspace / "pipeline-inputs.json")},
              "parameters": {"params_file_sha256": file_hash(workspace / "params.yaml"),
                             "effective_fit": read_json(prepared / "fit-params.json"),
                             "selected_config_sha256": selected_sha,
                             "evaluation": evaluation_params},
              **lineage,
              "training": {"run_manifest_sha256": file_hash(training / "final/run_manifest.json"),
                           "mlflow": record["mlflow"], "model_sha256": record["model"]["sha256"],
                           "legacy_source_pointers_role": "source_snapshot_only",
                           "authoritative_dataset_pointers": "inputs.binding.workspace_pointers"},
              "evaluation": {name: file_hash(output / name) for name in ("metrics.json", "scores.json", "features.json")},
              "limitations": record["limitations"], "test_read": False}
    from ats_mlops.quality.artifacts import validate_selection
    validate_selection(result, record, result["inputs"]["prepared_files"])
    write_json(output / "pipeline_manifest.json", result)
    return result


def reproduce(workspace: Path, source_root: Path, *, force=False) -> dict:
    """Audit ordinary DVC without changing its skip or run-cache behavior."""
    workspace, source_root = workspace.resolve(), source_root.resolve()
    invocation = uuid.uuid4().hex
    command = [str(Path(sys.executable).parent / "dvc"), "repro"]
    if force:
        command.extend(["-f", "--no-run-cache"])
    try:
        context(workspace, source_root)
        parameters(workspace, source_root)
    except Exception as error:
        result = {"schema_version": 1, "invocation": invocation, "command": ["dvc", *command[1:]],
                  "exit_code": 1, "outcome": "failed", "events": {}, "preflight": {
                      "error_type": type(error).__name__, "retryable": isinstance(error, OSError),
                      "reason": str(error) if re.fullmatch(r"[a-z_]+", str(error)) else "preflight_failed"}}
        write_json(workspace / ".local/repro" / (invocation + ".json"), result)
        return result
    env = os.environ.copy()
    env.update({"ATS_PIPELINE_INVOCATION": invocation, "ATS_ML_PYTHON": sys.executable,
                "PYTHONPATH": str(source_root / "src"), "PYTHONDONTWRITEBYTECODE": "1"})
    process = subprocess.run(command, cwd=workspace, env=env, text=True, capture_output=True)
    events_dir = workspace / ".local/executions" / invocation
    events = {path.stem: read_json(path) for path in events_dir.glob("*.json")}
    result = {"schema_version": 1, "invocation": invocation, "command": ["dvc", *command[1:]],
              "exit_code": process.returncode, "outcome": classify_repro(process.returncode, events), "events": events,
              "source_git_sha": read_json(workspace / "pipeline-inputs.json")["git_sha"],
              "dvc_lock_sha256": file_hash(workspace / "dvc.lock") if (workspace / "dvc.lock").exists() else None,
              "pipeline_manifest_sha256": file_hash(workspace / "artifacts/evaluation/pipeline_manifest.json")
                  if (workspace / "artifacts/evaluation/pipeline_manifest.json").exists() else None}
    receipt = workspace / ".local/repro" / (invocation + ".json")
    write_json(receipt, result)
    receipt.with_suffix(".log").write_text(process.stdout + process.stderr)
    return result


def add_parser(commands):
    parser = commands.add_parser("pipeline")
    sub = parser.add_subparsers(dest="pipeline_command", required=True)
    bundle = sub.add_parser("bundle", help="one-time synthetic admission before the regular DAG")
    bundle.add_argument("--source-root", type=Path, required=True)
    bundle.add_argument("--config", type=Path, required=True)
    bundle.add_argument("--output", type=Path, required=True)
    init = sub.add_parser("init", help="install an already admitted bundle into sibling source/workspace clones")
    for name in ("repo", "directory", "bundle", "image-identity"):
        init.add_argument("--" + name, type=Path, required=True)
    init.add_argument("--git-sha", required=True)
    init.add_argument("--bundle-sha", required=True)
    for command in ("stage", "repro"):
        child = sub.add_parser(command)
        child.add_argument("--workspace", type=Path, required=True)
        child.add_argument("--source-root", type=Path, required=True)
        if command == "stage":
            child.add_argument("stage", choices=STAGES)
        else:
            child.add_argument("--force", action="store_true", help="dvc repro -f --no-run-cache")


def dispatch(args):
    command = args.pipeline_command
    if command == "bundle":
        return create_bundle(args.source_root, args.config, args.output)
    if command == "init":
        return initialize(args.repo, args.git_sha, args.directory, args.bundle, args.bundle_sha, read_json(args.image_identity))
    if command == "stage":
        try:
            return stage(args.stage, args.workspace, args.source_root)
        except Exception as error:
            execution_event(args.workspace.resolve(), "failure-" + args.stage,
                            {"error_type": type(error).__name__, "retryable": isinstance(error, OSError),
                             "remote_commit_uncertain": args.stage == "train" and not isinstance(error, ValueError),
                             "reason": str(error) if re.fullmatch(r"[a-z_]+", str(error)) else "stage_failed"})
            raise
    return reproduce(args.workspace, args.source_root, force=args.force)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="ats-mlops")
    add_parser(parser.add_subparsers(required=True))
    args = parser.parse_args(["pipeline", *(sys.argv[1:] if argv is None else argv)])
    try:
        result = dispatch(args)
    except Exception as error:
        reason = str(error)
        print(json.dumps({"outcome": "failed", "error_type": type(error).__name__,
                          "reason": reason if re.fullmatch(r"[a-z_]+", reason) else "pipeline_failed"}), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 1 if result.get("outcome") == "failed" else 0
