"""Strict local artifact and T9 authoritative input bindings before execution."""

import hashlib
import json
from pathlib import Path, PurePosixPath
import re

from ats_mlops.data.dataset import canonical_json, load_manifest, load_partition, sha256
from ats_mlops.data.validate import _valid_receipt
from ats_mlops.quality.policy import GateError, require


def read_document(path):
    """Return the parsed document and identity of the same validated bytes."""
    content = Path(path).read_bytes()
    return json.loads(content), sha256(content)


def digest(path):
    return sha256(Path(path).read_bytes())


def valid_hash(value, length=64):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{%d}" % length, value) is not None


def safe_relative(value):
    require(isinstance(value, str) and bool(value) and "\\" not in value, "unsafe_inventory_path")
    path = PurePosixPath(value)
    require(not path.is_absolute() and path.as_posix() == value
            and all(part not in {".", "..", ""} for part in path.parts), "unsafe_inventory_path")
    return value


def hash_map(mapping):
    require(isinstance(mapping, dict) and bool(mapping), "missing_hash_inventory")
    for name, value in mapping.items():
        safe_relative(name)
        require(valid_hash(value), "invalid_inventory_hash")
    return mapping


def strict_inventory(directory: Path):
    require(directory.is_dir() and not directory.is_symlink(), "invalid_model_directory")
    files = {}
    for path in sorted(directory.rglob("*")):
        require(not path.is_symlink(), "artifact_symlink_forbidden")
        require("__pycache__" not in path.relative_to(directory).parts
                and path.suffix.lower() not in {".pyc", ".pyo"}, "artifact_bytecode_forbidden")
        if path.is_file():
            files[safe_relative(path.relative_to(directory).as_posix())] = digest(path)
        else:
            require(path.is_dir(), "unsupported_artifact_entry")
    return files


def validation_snapshot(manifest_path: Path, admission_path: Path, binding_path: Path):
    original = manifest_path.read_bytes()
    manifest = load_manifest(manifest_path)
    require(manifest == json.loads(original), "validation_manifest_changed")
    receipt, admission_sha = read_document(admission_path)
    require(_valid_receipt(receipt, sha256(original)), "invalid_validation_admission")
    rows = load_partition(manifest_path, "validation")
    require(manifest_path.read_bytes() == original, "validation_manifest_changed")
    content = b"".join(canonical_json(row) for row in rows)
    binding, binding_sha = read_document(binding_path)
    require(binding["schema_version"] == 1 and binding["evidence_scope"] == manifest["evidence_scope"]
            and binding["manifest_sha256"] == sha256(original)
            and binding["admission_sha256"] == admission_sha, "current_dataset_binding_mismatch")
    require(set(binding["workspace_pointers"]) == {"train", "validation", "split_lists"},
            "missing_current_workspace_pointers")
    for name, pointer in binding["workspace_pointers"].items():
        relative = safe_relative(pointer["pointer_path"])
        path = binding_path.parent / relative
        require(not path.is_symlink() and path.resolve().is_relative_to(binding_path.parent.resolve()),
                "invalid_current_pointer_path")
        pointer_bytes = path.read_bytes()
        require(pointer["artifact_path"] == manifest["artifacts"][name]["path"]
                and pointer["sha256"] == manifest["artifacts"][name]["sha256"]
                and sha256(pointer_bytes) == pointer["pointer_sha256"], "current_pointer_binding_mismatch")
        # The pinned DVC writer emits this single-file pointer form. No YAML
        # interpreter or reserved dataset bytes are needed by the host gate.
        require(valid_hash(pointer["md5"], 32) and type(pointer["size"]) is int and pointer["size"] > 0,
                "invalid_current_pointer")
        expected = (f"outs:\n- md5: {pointer['md5']}\n  size: {pointer['size']}\n  hash: md5\n"
                    f"  path: {PurePosixPath(pointer['artifact_path']).name}\n")
        require(pointer_bytes.decode("utf-8") == expected, "current_pointer_metadata_mismatch")
    point = binding["workspace_pointers"]["validation"]
    require(point["md5"] == hashlib.md5(content).hexdigest() and point["size"] == len(content),
            "current_validation_pointer_mismatch")
    return {"rows": rows, "manifest": manifest,
            "manifest_sha256": sha256(original), "admission_sha256": admission_sha,
            "validation_sha256": sha256(content), "validation_md5": hashlib.md5(content).hexdigest(),
            "validation_size": len(content),
            "split_sha256": manifest["artifacts"]["split_lists"]["sha256"],
            "inventory_sha256": sha256(canonical_json(manifest["splits"]["validation"])),
            "groups": manifest["splits"]["validation"]["vacancy_ids"],
            "binding": binding, "dataset_binding_sha256": binding_sha,
            "evidence_scope": manifest["evidence_scope"]}


def validate_selection(pipeline, record, prepared):
    """Discriminate a real frozen config receipt from legacy Optuna lineage."""
    effective = pipeline["parameters"]["effective_fit"]
    search = effective["search"]
    require(isinstance(search, dict) and search.get("mode", "optuna") in {"optuna", "frozen"}, "invalid_selection_mode")
    selected_sha = record["params_sha256"]
    require(pipeline["parameters"]["selected_config_sha256"] == selected_sha, "selected_configuration_mismatch")
    if search.get("mode") != "frozen":
        require("selection" not in pipeline and pipeline["study"]["outcome"] == "passed"
                and valid_hash(pipeline["study"]["sha256"]), "incomplete_pipeline_lineage")
        return "optuna"
    require("study" not in pipeline and set(search) == {"mode"} and effective["smoke"] is False
            and set(effective) == {"model", "search", "smoke"}, "ambiguous_frozen_selection")
    selection = pipeline["selection"]
    require(set(selection) == {"kind", "receipt", "receipt_sha256"} and selection["kind"] == "frozen", "invalid_frozen_selection")
    receipt = selection["receipt"]
    require(set(receipt) == {"schema_version", "kind", "outcome", "new_trials", "selected_config",
            "selected_config_sha256", "effective_fit_sha256", "prepared_inventory_sha256",
            "data_manifest_sha256", "admission_sha256", "source"}
            and type(receipt["schema_version"]) is int and receipt["schema_version"] == 1
            and receipt["kind"] == "frozen" and receipt["outcome"] == "selected"
            and type(receipt["new_trials"]) is int and receipt["new_trials"] == 0
            and sha256(canonical_json(receipt)) == selection["receipt_sha256"], "invalid_frozen_receipt")
    require(receipt["selected_config_sha256"] == sha256(canonical_json(receipt["selected_config"]))
            == sha256(canonical_json(effective["model"])) == selected_sha
            and prepared["model.json"] == selected_sha
            and prepared["fit-params.json"] == receipt["effective_fit_sha256"] == sha256(canonical_json(effective))
            and prepared["search.json"] == sha256(canonical_json({"mode": "frozen"})), "frozen_configuration_mismatch")
    require(receipt["prepared_inventory_sha256"] == sha256(canonical_json(prepared))
            and receipt["data_manifest_sha256"] == record["data_manifest_sha256"] == prepared["data_manifest.json"]
            and receipt["admission_sha256"] == record["reports_sha256"]["admission"] == prepared["admission.json"],
            "frozen_prepared_binding_mismatch")
    require(receipt["source"] == {"git_sha": record["git_sha"], "source_tree_sha256": record["source_tree_sha256"],
            "image_id": record["environment"]["image"]["id"],
            "lockfile_sha256": record["environment"]["lockfile_sha256"]}, "frozen_source_binding_mismatch")
    return "frozen"


def validate_model(spec: dict, snapshot: dict, *, candidate: bool):
    require(isinstance(spec, dict), "missing_model_descriptor")
    record_path, pipeline_path = Path(spec["run_manifest"]), Path(spec["pipeline_manifest"])
    artifact = Path(spec["model_dir"])
    record, record_sha = read_document(record_path)
    pipeline, pipeline_sha = read_document(pipeline_path)
    identity = record["mlflow"]
    require(set(identity) == {"model_name", "model_version", "run_id", "experiment_id", "exact_uri", "source"}
            and all(isinstance(value, str) for value in identity.values()), "invalid_model_identity")
    version = identity["model_version"]
    require(isinstance(version, str) and version.isdecimal() and int(version) > 0
            and str(int(version)) == version, "exact_numeric_model_version_required")
    require(re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", identity["model_name"]) is not None
            and identity["experiment_id"].isdecimal()
            and str(int(identity["experiment_id"])) == identity["experiment_id"]
            and re.fullmatch(r"models:/m-[0-9a-f]{32}", identity["source"]) is not None
            and identity["exact_uri"] == f"models:/{identity['model_name']}/{version}"
            and valid_hash(identity["run_id"], 32), "invalid_model_identity")
    require(type(record["schema_version"]) is int and type(pipeline["schema_version"]) is int
            and record["schema_version"] == pipeline["schema_version"] == 1
            and pipeline["outcome"] == "passed" and record["dirty_worktree"] is False
            and record["evidence_scope"] in {"demo_only", "independently_labeled"}
            and pipeline["evidence_scope"] == record["evidence_scope"]
            and valid_hash(record["git_sha"], 40), "invalid_model_lineage")
    source = hash_map(record["source_sha256"])
    require(sha256(canonical_json(source)) == record["source_tree_sha256"], "source_inventory_mismatch")
    for key in ("git_sha", "dirty_worktree", "source_sha256", "source_tree_sha256", "environment", "dvc_pointers"):
        require(pipeline["source"][key] == record[key], "pipeline_source_mismatch")
    for pointer in record["dvc_pointers"].values():
        require(digest_text(pointer["pointer_text"]) == pointer["pointer_sha256"], "source_pointer_mismatch")
    require(set(record["dvc_pointers"]) == {"train.jsonl.dvc", "validation.jsonl.dvc", "split.json.dvc"},
            "missing_source_pointers")
    files = strict_inventory(artifact)
    expected = hash_map(record["model"]["files"])
    require(files == expected and sha256(canonical_json(files)) == record["model"]["sha256"],
            "model_inventory_mismatch")
    require(valid_hash(record["model"]["payload_sha256"])
            and files.get("model.pkl") == record["model"]["payload_sha256"], "model_payload_mismatch")
    code = {name.removeprefix("code/ats_mlops/"): value for name, value in files.items()
            if name.startswith("code/ats_mlops/")}
    require(all(not name.startswith("code/") or name.startswith("code/ats_mlops/") for name in files),
            "unexpected_bundled_code")
    require(code == source, "bundled_source_mismatch")
    require(record["features_sha256"] == sha256(canonical_json(record["features"]))
            and record["params_sha256"] == sha256(canonical_json(record["params"]))
            and record["features"]["parameters"] == record["params"]["features"]
            and record["features"]["version"] == record["params"]["feature_version"]
            and type(record["seed"]) is int and record["seed"] == record["params"]["seed"], "model_configuration_mismatch")
    embedded, embedded_sha = read_document(artifact / "extra_files/input-provenance.json")
    require(embedded_sha == files["extra_files/input-provenance.json"], "embedded_provenance_changed")
    require(all(key in record and record[key] == value for key, value in embedded.items())
            and {"git_sha", "source_sha256", "source_tree_sha256", "environment", "data_sha256",
                 "data_manifest_sha256", "features_sha256", "params_sha256", "split_sha256"} <= set(embedded),
            "embedded_provenance_mismatch")
    training = pipeline["training"]
    require(training["run_manifest_sha256"] == record_sha
            and training["model_sha256"] == record["model"]["sha256"]
            and training["mlflow"] == identity
            and training["authoritative_dataset_pointers"] == "inputs.binding.workspace_pointers"
            and training["legacy_source_pointers_role"] == "source_snapshot_only", "pipeline_model_binding_mismatch")
    inputs = pipeline["inputs"]
    binding = inputs["binding"]
    require(binding["schema_version"] == 1 and binding["evidence_scope"] == record["evidence_scope"]
            and binding["manifest_sha256"] == inputs["prepared_manifest_sha256"] == record["data_manifest_sha256"]
            and binding["admission_sha256"] == record["reports_sha256"]["admission"], "pipeline_data_binding_mismatch")
    prepared = hash_map(inputs["prepared_files"])
    require(prepared["data_manifest.json"] == record["data_manifest_sha256"]
            and prepared["admission.json"] == binding["admission_sha256"]
            and valid_hash(inputs["pipeline_inputs_sha256"]), "prepared_binding_mismatch")
    pointers = binding["workspace_pointers"]
    require(set(pointers) == {"train", "validation", "split_lists"}, "missing_authoritative_pointers")
    for name, pointer in pointers.items():
        require(set(pointer) == {"pointer_path", "pointer_sha256", "artifact_path", "md5", "size", "sha256"}
                and all(valid_hash(pointer[key]) for key in ("sha256", "pointer_sha256"))
                and valid_hash(pointer["md5"], 32) and type(pointer["size"]) is int and pointer["size"] > 0,
                "invalid_authoritative_pointer")
        safe_relative(pointer["pointer_path"])
        safe_relative(pointer["artifact_path"])
        require(pointer["pointer_path"] == pointer["artifact_path"] + ".dvc", "pointer_path_mismatch")
        expected_hash = record["split_sha256"] if name == "split_lists" else record["data_sha256"][name]
        require(pointer["sha256"] == expected_hash, "pointer_data_mismatch")
        if name != "split_lists":
            require(prepared[pointer["artifact_path"]] == expected_hash, "prepared_data_mismatch")
    validate_selection(pipeline, record, prepared)
    require(all(valid_hash(pipeline["evaluation"][name]) for name in ("metrics.json", "scores.json", "features.json")),
            "incomplete_pipeline_lineage")
    environment = record["environment"]
    require(valid_hash(environment["lockfile_sha256"]) and valid_hash(environment["build_lockfile_sha256"])
            and re.fullmatch(r"sha256:[0-9a-f]{64}", environment["image"]["id"]) is not None
            and bool(environment["packages"]) and isinstance(environment["python"], str), "invalid_runtime_lineage")
    if candidate:
        current = snapshot["manifest"]
        point = pointers["validation"]
        require(record["data_manifest_sha256"] == snapshot["manifest_sha256"]
                and binding == snapshot["binding"]
                and binding["admission_sha256"] == snapshot["admission_sha256"]
                and record["split_sha256"] == snapshot["split_sha256"]
                and record["data_sha256"]["validation"] == snapshot["validation_sha256"]
                and record["partition_inventory_sha256"]["validation"] == snapshot["inventory_sha256"]
                and point["md5"] == snapshot["validation_md5"] and point["size"] == snapshot["validation_size"]
                and point["artifact_path"] == current["artifacts"]["validation"]["path"]
                and record["evidence_scope"] == snapshot["evidence_scope"], "candidate_validation_snapshot_mismatch")
        require(all(record["data_sha256"][name] == current["artifacts"][name]["sha256"]
                    and record["partition_inventory_sha256"][name] == sha256(canonical_json(current["splits"][name]))
                    for name in ("train", "validation")), "candidate_data_inventory_mismatch")
    else:
        require(isinstance(spec.get("release_id"), str)
                and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", spec["release_id"]) is not None
                and type(spec.get("release_is_fixture", False)) is bool, "incumbent_release_identity_required")
    public = {**identity, "run_manifest_sha256": record_sha,
              "pipeline_manifest_sha256": pipeline_sha, "model_sha256": record["model"]["sha256"],
              "model_payload_sha256": record["model"]["payload_sha256"],
              "training_git_sha": record["git_sha"], "source_tree_sha256": record["source_tree_sha256"],
              "authoritative_workspace_pointers": pointers}
    if not candidate:
        public.update({key: spec.get(key, False) for key in ("release_id", "release_is_fixture")})
    return {"record": record, "artifact": artifact, "public": public}


def digest_text(text):
    require(isinstance(text, str), "invalid_source_pointer")
    return sha256(text.encode())
