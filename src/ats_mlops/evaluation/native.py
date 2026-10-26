"""Explicit native experiment adapters over the audited pipeline and release code.

Importing this module has no effects. Callers supply a frozen spec and a new
attempt directory. Runtime inputs live in a hash-bound environment descriptor;
there is no default core, credential fallback, model fit, or test read.
"""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import importlib.util
import base64
import json
import math
import os
from pathlib import Path
import re
import select
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from urllib.parse import urlparse

from .experiment import ExperimentError, canonical, decode, digest, immutable, read, require, sha, validate_spec

BASELINE_FILES = ("model/baseline.py", "data/dataset.py")
ENV_KEYS = {"schema_version", "project", "repo", "bundle", "bundle_sha256", "image", "network", "tracking_uri",
            "threads", "worker_memory_mib", "release", "core", "fixtures", "qualified", "holdout", "E4"}


def document(reference):
    require(isinstance(reference, dict) and set(reference) == {"path", "sha256"} and digest(reference["sha256"]), "invalid_native_reference")
    path = Path(reference["path"])
    require(path.suffix.lower() in (".json", ".yaml", ".yml"), "metadata_file_required_before_read")
    require(path.is_file() and not path.is_symlink() and path.resolve().suffix.lower() in (".json", ".yaml", ".yml"), "missing_native_input")
    content = path.read_bytes()
    require(sha(content) == reference["sha256"], "native_input_hash_mismatch")
    return decode(content)


def adapter_binding(series):
    require(series in ("E1", "E3", "E4", "E5"), "unsupported_native_adapter")
    name = "evaluate_e5" if series == "E5" else series.lower()
    require(callable(globals().get(name)), "native_adapter_not_implemented")
    return {"entrypoint": __name__ + ":" + name, "source_sha256": sha(Path(__file__).read_bytes())}


def wire_adapters(spec, *, require_complete=False):
    """Pure descriptor wiring; an unfinished native E4 cannot become a freeze."""
    from . import experiment
    value = deepcopy(spec)
    for series in ("E1", "E3", "E4", "E5"):
        try:value["adapter_bindings"][series] = adapter_binding(series)
        except ExperimentError:
            require(not require_complete, "native_adapter_not_implemented")
            value["adapter_bindings"][series] = None
    value["adapter_bindings"]["E2"] = {"entrypoint": experiment.__name__ + ":ingest_human_measurement",
        "source_sha256": sha(Path(experiment.__file__).read_bytes())}
    validate_spec(value, template=not require_complete)
    return value


def source_guard(repo, expected):
    repo = Path(repo).resolve()
    def git(*arguments):
        return subprocess.run(["git", "-c", f"safe.directory={repo}", *arguments], cwd=repo,
                              capture_output=True, text=True, check=True).stdout.strip()
    require(git("rev-parse", "HEAD") == expected and not git("status", "--porcelain", "--untracked-files=all"), "native_clean_source_required")
    require(Path(__file__).resolve() == repo / "src/ats_mlops/evaluation/native.py", "native_source_namespace_mismatch")
    for name, module in tuple(sys.modules.items()):
        if name == "ats_mlops" or name.startswith("ats_mlops."):
            require(Path(module.__file__).resolve().is_relative_to(repo / "src"), "native_source_namespace_mismatch")
        if name == "service" or name.startswith("service."):
            require(Path(module.__file__).resolve().is_relative_to(repo / "service"), "native_source_namespace_mismatch")
    if str(repo) not in sys.path:sys.path.insert(0, str(repo))
    return repo


def environment(spec, *, series, qualification=False):
    """All mutable locations and effect targets are part of the frozen binding."""
    validate_spec(spec, template=qualification)
    if qualification:
        require(isinstance(spec["source_git_sha"], str) and re.fullmatch(r"[0-9a-f]{40}", spec["source_git_sha"])
                and all(digest(spec["bindings"][key]) for key in
                        ("environment_sha256", "data_manifest_sha256", "admission_sha256", "root_policy_sha256", "split_sha256")),
                "qualification_inputs_not_pinned")
        require(all(spec["bindings"][key] is None for key in
                    ("model_artifact_sha256", "model_lineage_sha256")) and spec["qualified_model_source_git_sha"] is None,
                "qualification_must_precede_candidate_selection")
    path = os.environ.get("ATS_EXPERIMENT_ENVIRONMENT")
    require(path is not None and Path(path).suffix.lower() == ".json" and not Path(path).is_symlink(), "native_environment_required")
    content = Path(path).read_bytes()
    require(sha(content) == spec["bindings"]["environment_sha256"], "native_environment_hash_mismatch")
    value = decode(content)
    require(isinstance(value, dict) and set(value) == ENV_KEYS and type(value["schema_version"]) is int
            and value["schema_version"] == 1, "invalid_native_environment")
    require(isinstance(value["project"], str) and re.fullmatch(r"ats-(?:acceptance|experiment)-[a-z0-9][a-z0-9-]{2,40}", value["project"]), "isolated_project_required")
    require(value["network"] == value["project"] + "_default" and value["tracking_uri"] == "http://mlflow:5000"
            and type(value["threads"]) is int and value["threads"] == 1
            and type(value["worker_memory_mib"]) is int and value["worker_memory_mib"] == 1536, "unfrozen_native_transport")
    require(isinstance(value["image"], dict) and re.fullmatch(r"sha256:[0-9a-f]{64}", value["image"].get("id", "")), "pinned_native_image_required")
    require(spec["adapter_bindings"][series] == adapter_binding(series), "native_adapter_code_changed")
    source_guard(value["repo"], spec["source_git_sha"])
    return value


def owned_core(commands, config):
    """Prove namespace/role ownership before any worker, disconnect or stop."""
    core = config["core"]
    require(isinstance(core, dict) and set(core) == {"containers", "ports", "images"} and set(core["containers"]) == {"postgres", "minio", "mlflow"}, "isolated_core_identity_required")
    observations = {}
    for role, name in core["containers"].items():
        require(isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9_.-]{2,127}", name), "invalid_owned_container")
        fmt = '{"Id":{{json .Id}},"Image":{{json .Image}},"State":{{json .State}},"Labels":{{json .Config.Labels}},"Networks":{{json .NetworkSettings.Networks}}}'
        value = decode(commands.run(["docker", "inspect", "--format", fmt, name]).stdout)
        labels = value["Labels"]
        require(labels["com.docker.compose.project"] == config["project"] and labels["com.docker.compose.service"] == role
                and value["Image"] == core["images"][role]
                and value["State"]["Running"] is True and value["State"]["OOMKilled"] is False
                and value["State"]["Health"]["Status"] == "healthy"
                and set(value["Networks"]) == {config["network"]}, "foreign_or_unhealthy_core")
        observations[role] = {"id": value["Id"], "image": value["Image"], "project": config["project"],
                              "aliases": value["Networks"][config["network"]]["Aliases"]}
    image = commands.run(["docker", "image", "inspect", "--format", "{{.Id}}", config["image"]["id"]]).stdout.decode().strip()
    require(image == config["image"]["id"], "native_image_identity_mismatch")
    immutable(commands.root / "isolation.json", observations)
    return sha(canonical(observations))


class Commands:
    """Exact argv/exit/stdout/stderr capture; no shell or caller-written success flag."""
    def __init__(self, root):
        self.root = Path(root)
        self.records = []
        (self.root / "commands").mkdir(exist_ok=False)

    def run(self, argv, *, cwd=None, env=None, accepted=(0,), timeout=180):
        number = len(self.records) + 1
        completed = subprocess.run(list(map(str, argv)), cwd=cwd, env=env, capture_output=True, timeout=timeout)
        paths = {}
        for stream in ("stdout", "stderr"):
            path = self.root / "commands" / f"{number:03}-{stream}.txt"
            content = getattr(completed, stream);path.write_bytes(content)
            paths[stream] = {"path": path.relative_to(self.root).as_posix(), "sha256": sha(content)}
        self.records.append({"argv": list(map(str, argv)), "exit_code": completed.returncode,
                             "accepted_exit_codes": list(accepted), **paths})
        immutable(self.root / "commands" / f"{number:03}.json", self.records[-1])
        require(completed.returncode in accepted, "native_command_failed")
        return completed

    def finish(self):
        value = {"schema_version": 1, "commands": self.records}
        immutable(self.root / "commands.json", value)
        return sha(canonical(value))


def worker(commands, config, run, arguments, *, accepted=(0,), network=True, timeout=300):
    name = config["project"] + "-worker-" + uuid.uuid4().hex[:8]
    command = ["docker", "run", "--rm", "--name", name, "--pull=never", "--cpus", "1", "--memory", "1536m", "--read-only",
        "--user", f"{os.getuid()}:{os.getgid()}", "--workdir", "/run/workspace", "--network", config["network"] if network else "none",
        "--tmpfs", "/tmp:rw,size=256m,mode=1777", "--tmpfs", "/var/tmp/dvc:rw,size=32m,mode=1777",
        "-e", "HOME=/tmp", "-e", "USER=ats-experiment", "-e", "LOGNAME=ats-experiment", "-e", "PYTHONPATH=/run/source/src",
        "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
        "-e", "MLFLOW_TRACKING_URI=" + config["tracking_uri"], "-e", "MLFLOW_HTTP_REQUEST_MAX_RETRIES=0", "-e", "MLFLOW_HTTP_REQUEST_TIMEOUT=5",
        "--mount", f"type=bind,source={run / 'source'},target=/run/source,readonly",
        "--mount", f"type=bind,source={run / 'workspace'},target=/run/workspace",
        "--entrypoint", "/opt/ml-venv/bin/python", config["image"]["id"], "-B", *arguments]
    try:
        return commands.run(command, accepted=accepted, timeout=timeout)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=20)


def prepare_run(spec, config, root):
    from ats_mlops.pipeline import initialize, verify_bundle
    require(verify_bundle(Path(config["bundle"]), config["bundle_sha256"])["sha256"] == config["bundle_sha256"], "native_bundle_mismatch")
    run = Path(root) / "run"
    initialize(Path(config["repo"]), spec["source_git_sha"], run, Path(config["bundle"]), config["bundle_sha256"], config["image"])
    params = {"model": {"seed": spec["execution"]["model_seed"], "features": {"max_features": spec["execution"]["max_features"]},
                        "classifier": {"C": spec["execution"]["C"]}},
              "search": {"mode": "frozen"}, "smoke": False, "evaluation": {"score_decimals": 12}}
    (run / "workspace/params.yaml").write_bytes(canonical(params))
    return run


def run_pipeline(spec, config, root, commands, *, corrupt=False):
    run = prepare_run(spec, config, root)
    if corrupt:
        (run / "workspace/data/train.jsonl").write_bytes(b"{}\n")
    started = time.monotonic()
    completed = worker(commands, config, run, ["-m", "ats_mlops", "pipeline", "repro", "--workspace", "/run/workspace", "--source-root", "/run/source", "--force"],
                       accepted=(0, 1) if corrupt else (0,))
    repro = decode(completed.stdout.strip().splitlines()[-1])
    immutable(Path(root) / "repro.json", repro)
    source_guard(config["repo"], spec["source_git_sha"])
    if corrupt:
        require(repro["outcome"] == "failed" and not repro["events"].get("train"), "invalid_data_reached_training")
        script = r'''
import json
from pathlib import Path
from unittest.mock import patch
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from ats_mlops.model.training import train
from ats_mlops.data.validate import ValidationError
calls={'pipeline':0,'classifier':0}
def forbid(name):
 def fit(*a,**k):calls[name]+=1;raise RuntimeError('fit_must_not_run')
 return fit
with patch.object(Pipeline,'fit',forbid('pipeline')),patch.object(LogisticRegression,'fit',forbid('classifier')):
 try:
  train(Path('/run/workspace/data_manifest.json'),Path('/run/workspace/.local/negative-guard'),admission_report=Path('/run/workspace/admission/data-admission.json'),config_path=Path('/run/source/configs/model.json'),source_root=Path('/run/source'),image_identity={})
 except ValidationError as error:
  assert calls=={'pipeline':0,'classifier':0} and error.report['outcome']=='rejected'
  assert not any(reason.startswith('admission_') for reason in error.report['reasons'])
  print(json.dumps({'fit_calls':calls,'validation':error.report}))
 else:raise AssertionError('corrupt_train_accepted')
'''
        proof = decode(worker(commands, config, run, ["-c", script], network=False).stdout.strip().splitlines()[-1])
        require(proof["fit_calls"] == {"pipeline": 0, "classifier": 0}, "real_fit_guard_not_proven")
        immutable(Path(root) / "fit-guard.json", proof)
        return run, repro, started
    require(repro["outcome"] == "new_training" and repro["command"] == ["dvc", "repro", "-f", "--no-run-cache"]
            and set(repro["events"]) == {"validate", "prepare", "train", "evaluate"}, "actual_forced_fit_required")
    return run, repro, started


def e1(spec, root):
    config = environment(spec, series="E1");root = Path(root);commands = Commands(root)
    owned_core(commands, config)
    run, repro, _ = run_pipeline(spec, config, root, commands)
    workspace = run / "workspace"
    record = read(workspace / "artifacts/training/final/run_manifest.json")
    pipeline = read(workspace / "artifacts/evaluation/pipeline_manifest.json")
    features = read(workspace / "artifacts/evaluation/features.json")
    scores = read(workspace / "artifacts/evaluation/scores.json")
    metrics = read(workspace / "artifacts/evaluation/metrics.json")
    require(record["git_sha"] == spec["source_git_sha"] and record["dirty_worktree"] is False
            and record["test_read"] is False and record["read_partitions"] == ["train", "validation"]
            and record["params_sha256"] == spec["bindings"]["params_sha256"]
            and record["features_sha256"] == spec["bindings"]["features_sha256"]
            and record["data_manifest_sha256"] == spec["bindings"]["data_manifest_sha256"], "native_fit_binding_mismatch")
    selection = pipeline["selection"]
    require(selection["kind"] == "frozen" and selection["receipt"]["new_trials"] == 0
            and selection["receipt"]["selected_config"] == record["params"], "native_hpo_not_disabled")
    identity = record["mlflow"]
    registry_code = "import json,mlflow; c=mlflow.MlflowClient(); v=c.get_model_version(" + repr(identity["model_name"]) + "," + repr(identity["model_version"]) + ");r=c.get_run(" + repr(identity["run_id"]) + ");print(json.dumps({'version':str(v.version),'run_id':v.run_id,'source':v.source,'status':r.info.status}))"
    remote = decode(worker(commands, config, run, ["-c", registry_code]).stdout.strip().splitlines()[-1])
    require(remote == {"version": identity["model_version"], "run_id": identity["run_id"], "source": identity["source"], "status": "FINISHED"}, "native_registry_binding_mismatch")
    observation = {"source_git_sha": record["git_sha"], "forced": True, "run_cache": False, "cache_hit": False,
        "final_fit_events": int("train" in repro["events"]), "workspace_id": repro["invocation"],
        "training_run_id": record["mlflow"]["run_id"], "model_version": record["mlflow"]["model_version"],
        "data_sha256": record["data_manifest_sha256"], "split_sha256": record["split_sha256"],
        "features_sha256": record["features_sha256"], "vocabulary_sha256": sha(canonical({key: features[key] for key in ("vocabulary", "idf", "transformed_sha256")})),
        "coordinates_sha256": sha(canonical(spec["bindings"])),
        "scores": [{"group_id": row["vacancy_id"], "candidate_id": row["candidate_id"], "score": row["score"]} for row in scores["rows"]],
        "metrics": {key: metrics[key] for key in ("ndcg_at_10", "recall_at_10")}}
    immutable(root / "native-observations.json", observation)
    return {"outcome": "passed", "observations": observation, "evidence_sha256": commands.finish()}


def baseline_identity(record):
    files = {name: record["source_sha256"][name] for name in BASELINE_FILES}
    return sha(canonical({"version": "jaccard-v1", "source_sha256": files}))


def release_config(config, root):
    from ats_mlops.release.controller import DEFAULT_CONFIG, checked_config
    value = config["release"]
    require(set(value) == {"port", "registry_uri", "model_name", "ready_timeout_seconds"}, "invalid_private_release_transport")
    uri = urlparse(value["registry_uri"])
    require(uri.hostname in ("127.0.0.1", "localhost") and uri.port is not None and 10000 <= uri.port <= 65535
            and type(value["port"]) is int and 10000 <= value["port"] <= 65535, "private_release_ports_required")
    case = config["project"] + "-" + sha(str(Path(root).resolve()).encode())[:10]
    return checked_config({**deepcopy(DEFAULT_CONFIG), **value, "project": case, "alias": case,
                           "events_volume": case + "-events", "network": case + "-serving"})


def checked_fixture(config, role):
    from ats_mlops.release.controller import load_package
    value = config["fixtures"][role]
    require(set(value) == {"package", "check", "manifest_sha256"}, "invalid_release_fixture")
    document(value["check"])
    manifest, manifest_sha, _ = load_package(Path(value["package"]), Path(value["check"]["path"]))
    require(manifest_sha == value["manifest_sha256"], "release_fixture_changed")
    return value, manifest


def _controller(config, root, *, unready_release=None):
    from ats_mlops.release.controller import Controller
    from ats_mlops.release.runtime import DockerRuntime
    settings = release_config(config, root)
    class UnreadyRuntime(DockerRuntime):
        def compose(self, launch):
            value = super().compose(launch)
            if launch["package_path"] == f"releases/{unready_release}/package":
                value["services"]["ranking"]["environment"] = {"ATS_EVENT_DB": "/release/forbidden.sqlite3"}
            return value
    state = Path(root) / "release-state"
    return Controller(state, settings, runtime=UnreadyRuntime(state, settings) if unready_release else None)


def _consistent(controller):
    state = controller.store.read()
    active = state["active"]
    observed = controller.runtime.observe()
    require(active is not None and state["pending"] is None and controller.runtime.matches(observed, active)
            and controller.registry.get() == active["candidate"]["model_version"], "native_release_not_consistent")
    return state, observed


def _cleanup_release(controller, commands):
    """Only resources carrying this exact journal's owner label can be removed."""
    controller.runtime.stop()
    for kind, key in (("volume", "events_volume"), ("network", "network")):
        name = controller.config[key]
        result = commands.run(["docker", kind, "inspect", "--format", '{{index .Labels "ats.release.owner"}}', name], accepted=(0, 1))
        if result.returncode == 0:
            require(result.stdout.decode().strip() == str(controller.root), "foreign_cleanup_resource")
            commands.run(["docker", kind, "rm", name])
    if controller.registry.get() is not None:
        controller.registry.set(None)  # unique private alias, never default champion


def _sample_http(controller, samples, stop):
    from urllib.request import Request, urlopen
    while not stop.is_set():
        observed = {"t": time.monotonic(), "ok": False}
        try:
            with urlopen(Request(f"http://127.0.0.1:{controller.config['port']}/metadata", data=b"{}", headers={"Content-Type": "application/json"}), timeout=.4) as response:
                observed["ok"] = decode(response.read()).get("ready") is True
        except Exception:
            pass
        samples.append(observed)
        stop.wait(.1)


def _package_io_attempt(spec, config, root, commands, *, expect_failure=True, inputs=None, release_id=None, output_name="io-package"):
    """Fresh Bento store/download through private MLflow; no credential mounts."""
    inputs = config["fixtures"]["package_inputs"] if inputs is None else inputs
    require(set(inputs) == {"gate_report", "run_manifest", "pipeline_manifest", "serving_identity"}, "package_fixture_inputs_required")
    private = Path(root) / "package-inputs";private.mkdir(exist_ok=True)
    request = {"release_id": release_id or config["project"] + "-io-fault"}
    for name, reference in inputs.items():
        document(reference)
        (private / (name + ".json")).write_bytes(Path(reference["path"]).read_bytes())
        require(sha((private / (name + ".json")).read_bytes()) == reference["sha256"], "package_fixture_changed")
        request[name] = "/input/" + name + ".json"
    (private / "request.json").write_bytes(canonical(request))
    require(re.fullmatch(r"[a-z][a-z0-9-]{0,50}", output_name), "invalid_package_output_name")
    code = "import sys,json;from pathlib import Path;sys.path.insert(0,'/source');from service.package import package;package(json.loads(Path('/input/request.json').read_bytes()),Path('/output/" + output_name + "'))"
    name = config["project"] + "-package-" + uuid.uuid4().hex[:8]
    command = ["docker", "run", "--rm", "--name", name, "--network", config["network"], "--cpus", "1", "--memory", "1536m",
        "--user", f"{os.getuid()}:{os.getgid()}", "--tmpfs", "/tmp:rw,size=256m,mode=1777", "-e", "HOME=/tmp", "-e", "USER=ats-package", "-e", "LOGNAME=ats-package",
        "-e", "MLFLOW_TRACKING_URI=" + config["tracking_uri"], "-e", "MLFLOW_HTTP_REQUEST_MAX_RETRIES=0", "-e", "MLFLOW_HTTP_REQUEST_TIMEOUT=5",
        "--mount", f"type=bind,source={Path(config['repo']).resolve()},target=/source,readonly", "--mount", f"type=bind,source={private},target=/input,readonly",
        "--mount", f"type=bind,source={Path(root).resolve()},target=/output", "--entrypoint", "/opt/ml-venv/bin/python", config["image"]["id"], "-I", "-B", "-c", code]
    try:
        result = commands.run(command, accepted=(0, 1), timeout=90)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=20)
    if expect_failure:
        require(result.returncode != 0 and not (Path(root) / output_name).exists(), "minio_fault_was_not_observed")
    else:
        require(result.returncode == 0 and (Path(root) / output_name / "release_manifest.json").is_file(), "package_io_not_verified")


def declare_qualification(spec, config, root):
    """Choose S1's FIRST required fit before metrics; never select best of runs."""
    value = {"schema_version": 1, "outcome": "declared", "slot": "S1",
        "selection_rule": "first_required_fit_before_metrics", "source_git_sha": spec["source_git_sha"],
        "execution": spec["execution"], "bundle_sha256": config["bundle_sha256"],
        "qualification_environment_sha256": spec["bindings"]["environment_sha256"],
        "runtime": {key: config[key] for key in ("project", "image", "network", "threads", "worker_memory_mib", "core")},
        "data_manifest_sha256": spec["bindings"]["data_manifest_sha256"],
        "admission_sha256": spec["bindings"]["admission_sha256"], "split_sha256": spec["bindings"]["split_sha256"],
        "qualification_recomputations": 3, "formal_E1": "not_run", "declared_at": datetime.now(timezone.utc).isoformat()}
    immutable(Path(root) / "qualification-plan.json", value)
    return value


def select_qualification(plan, record, root):
    """Bind the first S1 fit immediately, before invoking any quality gate."""
    require(plan == read(Path(root) / "qualification-plan.json") and plan["slot"] == "S1"
            and plan["selection_rule"] == "first_required_fit_before_metrics"
            and record["git_sha"] == plan["source_git_sha"] and record["dirty_worktree"] is False
            and record["test_read"] is False and record["read_partitions"] == ["train", "validation"]
            and record["data_manifest_sha256"] == plan["data_manifest_sha256"]
            and record["split_sha256"] == plan["split_sha256"], "qualification_first_fit_binding_mismatch")
    value = {"schema_version": 1, "outcome": "selected", "slot": "S1", "selection_rule": plan["selection_rule"],
        "plan_sha256": sha(canonical(plan)), "source_git_sha": record["git_sha"],
        "model_artifact_sha256": record["model"]["sha256"], "model_lineage_sha256": sha(canonical(record)),
        "mlflow": record["mlflow"], "selected_at": datetime.now(timezone.utc).isoformat(),
        "scope": "T18 qualification; selected before quality metrics; not formal E1"}
    immutable(Path(root) / "qualified-selection.json", value)
    return value


def core_overlay(images):
    """The logical initializer key maps to the actual persistent-core service."""
    require(set(images)=={"postgres","minio","minio-init","mlflow"}
        and all(re.fullmatch(r"sha256:[0-9a-f]{64}",image) for image in images.values()),"pinned_core_images_required")
    return {"services":{("storage-init" if name=="minio-init" else name):{"image":image} for name,image in images.items()}}


def clean_start(spec, root):
    """S1's real isolated core → fit → T11/T12 → A3 → API/catalog/Registry.

    Explicit invocation only in root's maintenance window. Existing resources
    cause rejection; this routine never stops another core to make room.
    """
    from ats_mlops.quality.gate import run_gate
    from ats_mlops.release.controller import load_package
    config = environment(spec, series="E1", qualification=True);root = Path(root);commands = Commands(root)
    from service.release_check import stage
    plan = declare_qualification(spec, config, root)
    require(not (root / "core").exists(), "fresh_core_state_required")
    for kind in ("ps", "volume", "network"):
        argv = ["docker", "ps", "-aq"] if kind == "ps" else ["docker", kind, "ls", "-q"]
        existing = commands.run([*argv, "--filter", "label=com.docker.compose.project=" + config["project"]]).stdout.strip()
        require(not existing, "fresh_core_namespace_required")
    ports = config["core"]["ports"]
    require(set(ports) == {"minio", "console", "mlflow"} and len(set(ports.values())) == 3
            and all(type(port) is int and 10000 <= port <= 65535 for port in ports.values())
            and urlparse(config["release"]["registry_uri"]).port == ports["mlflow"], "private_core_ports_required")
    repo = Path(config["repo"])
    commands.run([sys.executable, "-B", repo / "scripts/bootstrap_core.py", "init", "--state-dir", root / "core", "--project", config["project"],
        "--minio-port", ports["minio"], "--console-port", ports["console"], "--mlflow-port", ports["mlflow"]])
    images = config["core"]["images"]
    immutable(root / "core-images.json", core_overlay(images))
    compose = ["docker", "compose", "--project-directory", str(repo), "--env-file", str(root / "core/core.env"), "-p", config["project"],
               "-f", str(repo / "compose.yaml"), "-f", str(root / "core-images.json"), "--profile", "core"]
    commands.run([*compose, "up", "-d", "--no-build", "--pull", "never", "--wait", "--wait-timeout", "180"], timeout=210)
    owned_core(commands, config)
    run, repro, _ = run_pipeline(spec, config, root, commands)
    workspace = run / "workspace";final = workspace / "artifacts/training/final"
    selected = select_qualification(plan, read(final / "run_manifest.json"), root)
    gate_request = {"validation_manifest": str(workspace / "artifacts/prepared/data_manifest.json"),
        "admission_report": str(workspace / "artifacts/prepared/admission.json"), "dataset_binding": str(workspace / "dataset-binding.json"),
        "thresholds": str(repo / "configs/quality.yaml"), "candidate": {"run_manifest": str(final / "run_manifest.json"),
            "pipeline_manifest": str(workspace / "artifacts/evaluation/pipeline_manifest.json"), "model_dir": str(final / "model")}, "incumbent": None}
    gate = run_gate(gate_request, root / "model-gate.json")
    require(gate["outcome"] == "passed", "clean_start_model_gate_rejected")
    def reference(path):
        return {"path": str(path), "sha256": sha(Path(path).read_bytes())}
    inputs = {"gate_report": reference(root / "model-gate.json"), "run_manifest": reference(final / "run_manifest.json"),
              "pipeline_manifest": reference(workspace / "artifacts/evaluation/pipeline_manifest.json"),
              "serving_identity": config["fixtures"]["serving_identity"]}
    _package_io_attempt(spec, config, root, commands, expect_failure=False, inputs=inputs,
                        release_id=config["project"] + "-startup", output_name="package")
    record = read(final / "run_manifest.json");identity = record["mlflow"]
    code = "import mlflow,json;c=mlflow.MlflowClient();v=c.create_model_version(" + repr(identity["model_name"]) + "," + repr(identity["source"]) + ",run_id=" + repr(identity["run_id"]) + ",model_id=" + repr(identity["source"].removeprefix("models:/")) + ");print(json.dumps({'version':str(v.version)}))"
    alias_version = decode(worker(commands, config, run, ["-c", code]).stdout.strip().splitlines()[-1])["version"]
    check_request = {"package": str(root / "package"), **{key: gate_request[key] for key in ("validation_manifest", "admission_report", "dataset_binding")},
                     "alias_other_version": alias_version, "registry_transport": {"network": config["network"], "tracking_uri": config["tracking_uri"]}}
    checked = stage(check_request, root / "release-check.json")
    require(checked["outcome"] == "passed", "clean_start_release_check_rejected")
    manifest, manifest_sha, check_sha = load_package(root / "package", root / "release-check.json")
    controller = _controller(config, root)
    controller.stage(root / "package", root / "release-check.json")
    controller.approve(manifest["release_id"], "agent-operator", "A3")
    controller.activate(manifest["release_id"])
    state, observed = _consistent(controller)
    result = {"schema_version": 1, "outcome": "passed", "source_git_sha": spec["source_git_sha"],
        "repro": repro, "mlflow": identity, "selected_qualification_sha256": sha(canonical(selected)), "release_id": manifest["release_id"], "manifest_sha256": manifest_sha,
        "check_sha256": check_sha, "alias_fixture_version": alias_version, "state": state, "observation": observed,
        "command_evidence_sha256": commands.finish(), "retained_private_core": True,
        "scope": "T18 clean startup qualification; no E1 experimental observation"}
    immutable(root / "startup.json", result)
    return result


def _launch_baseline(config, root):
    baseline, manifest = checked_fixture(config, "baseline")
    require(manifest["expected_incumbent"] is None, "actual_first_release_fixture_required")
    controller = _controller(config, root)
    require(controller.registry.get() is None, "fresh_private_alias_required")
    controller.stage(Path(baseline["package"]), Path(baseline["check"]["path"]))
    controller.approve(manifest["release_id"], "agent-operator", "A3")
    controller.activate(manifest["release_id"])
    _consistent(controller)
    return controller, manifest


def _rank_probe(controller, config, request_id):
    from ats_mlops.data.dataset import load_partition
    from ats_mlops.release.runtime import http_json
    rows = load_partition(Path(config["bundle"]) / "data_manifest.json", "validation")
    require(rows, "validation_probe_required")
    vacancy = sorted({row["vacancy_id"] for row in rows})[0]
    group = [row for row in rows if row["vacancy_id"] == vacancy]
    payload = {"request_id": request_id, "vacancy": {"id": vacancy, "text": group[0]["vacancy_text"]},
               "candidates": [{"id": row["candidate_id"], "text": row["candidate_text"]} for row in group], "top_k": 100}
    response = http_json(f"http://127.0.0.1:{controller.config['port']}/rank", payload)
    require(response["request_id"] == request_id, "fresh_inference_required")
    return {key: response[key] for key in ("ranking", "model_version", "release_id")}


def managed_release(spec, root):
    """S4 uses real T13 denials and fresh inference after a candidate alias move."""
    from ats_mlops.release.runtime import Registry
    from ats_mlops.release.state import ReleaseError
    config = environment(spec, series="E3");root = Path(root);commands = Commands(root)
    from service.integrity import inventory
    isolation_sha = owned_core(commands, config)
    controller, baseline_manifest = _launch_baseline(config, root)
    target, target_manifest = checked_fixture(config, "target")
    candidate_alias = Registry({**controller.config, "alias": controller.config["alias"] + "-candidate"})
    checks = {}
    try:
        before = controller.store.read()
        missing = root / "missing-gate";shutil.copytree(target["package"], missing)
        path = missing / "model_gate_report.json";path.chmod(0o600);path.unlink()
        try:controller.stage(missing, Path(target["check"]["path"]))
        except (ReleaseError, ValueError, OSError):pass
        else:raise ExperimentError("missing_gate_was_accepted")
        require(controller.store.read() == before, "denied_gate_changed_catalog")
        checks["missing_gate_blocks_stage"] = True
        controller.stage(Path(target["package"]), Path(target["check"]["path"]))
        before = controller.store.read()
        try:controller.activate(target_manifest["release_id"])
        except ReleaseError as error:require(str(error) == "approval_required", "unexpected_approval_denial")
        else:raise ExperimentError("approval_bypass_accepted")
        require(controller.store.read() == before, "denied_approval_changed_catalog")
        checks["missing_approval_blocks_activation"] = True
        controller.approve(target_manifest["release_id"], "agent-operator", "A3")
        controller.activate(target_manifest["release_id"])
        before_inventory = inventory(Path(target["package"]))
        candidate_alias.set(target_manifest["candidate"]["model_version"])
        first = _rank_probe(controller, config, controller.config["project"] + "-before")
        immutable(root / "first-rank.json", first)
        candidate_alias.set(baseline_manifest["candidate"]["model_version"])
        second = _rank_probe(controller, config, controller.config["project"] + "-after")
        immutable(root / "second-rank.json", second)
        require(first == second and second["release_id"] == target_manifest["release_id"]
                and second["model_version"] == target_manifest["candidate"]["model_version"]
                and inventory(Path(target["package"])) == before_inventory, "packaged_release_followed_candidate_alias")
        checks["candidate_alias_move_fresh_inference_package_unchanged"] = True
        state, observed = _consistent(controller)
        proof = {"schema_version": 1, "outcome": "passed", "source_git_sha": spec["source_git_sha"], "checks": checks,
                 "isolation_sha256": isolation_sha, "state": state, "observation": observed,
                 "package_inventory_sha256": sha(canonical(before_inventory)), "fresh_rank_sha256": sha(canonical(second)),
                 "journal_sha256": sha((controller.root / "journal.jsonl").read_bytes())}
        immutable(root / "managed-release.json", proof)
    finally:
        try:
            if candidate_alias.get() is not None:candidate_alias.set(None)
            _cleanup_release(controller, commands)
        finally:evidence_sha = commands.finish()
    return {"outcome": "passed", "native_receipt_sha256": sha(canonical(proof)), "evidence_sha256": evidence_sha, "checks": checks}


def registry_outage(spec, root):
    """S5 additional Registry failure: stop only the proven private MLflow service."""
    from ats_mlops.release.state import ReleaseError
    config = environment(spec, series="E3");root = Path(root);commands = Commands(root)
    isolation_sha = owned_core(commands, config)
    controller, baseline_manifest = _launch_baseline(config, root)
    target, target_manifest = checked_fixture(config, "target")
    controller.stage(Path(target["package"]), Path(target["check"]["path"]))
    controller.approve(target_manifest["release_id"], "agent-operator", "A3")
    service = config["core"]["containers"]["mlflow"]
    stopped = False;error_code = None
    try:
        before = controller.store.read();injected = time.monotonic()
        commands.run(["docker", "stop", service]);stopped = True
        try:controller.activate(target_manifest["release_id"])
        except ReleaseError as error:error_code = str(error)
        else:raise ExperimentError("registry_outage_marked_success")
        detected = time.monotonic()
        require(error_code == "registry_unavailable" and controller.store.read() == before
                and controller.runtime.matches(controller.runtime.observe(), before["active"]), "registry_outage_not_fail_closed")
        commands.run(["docker", "start", service]);stopped = False
        deadline = time.monotonic() + 90
        while True:
            try:
                state, observed = _consistent(controller);break
            except ReleaseError:
                require(time.monotonic() < deadline, "registry_recovery_timeout");time.sleep(.25)
        proof = {"schema_version": 1, "outcome": "passed", "source_git_sha": spec["source_git_sha"],
                 "isolation_sha256": isolation_sha, "error_code": error_code, "state": state, "observation": observed,
                 "injected_monotonic": injected, "detected_monotonic": detected, "restored_monotonic": time.monotonic(),
                 "blocked_next_release": state["active"]["release_id"] == baseline_manifest["release_id"]}
        immutable(root / "registry-outage.json", proof)
    finally:
        try:
            if stopped:commands.run(["docker", "start", service])
            _cleanup_release(controller, commands)
        finally:evidence_sha = commands.finish()
    return {"outcome": "passed", "native_receipt_sha256": sha(canonical(proof)), "evidence_sha256": evidence_sha}


def collect_ci(spec, root, *, repository, positive_run, negative_run, negative_head):
    """S7 fetches real upstream metadata and the exact public Actions artifact."""
    environment(spec, series="E1");root = Path(root);commands = Commands(root)
    require(repository == "seregatheone/ats_mlops" and type(positive_run) is int and positive_run > 0
            and type(negative_run) is int and negative_run > 0 and positive_run != negative_run
            and isinstance(negative_head, str) and re.fullmatch(r"[0-9a-f]{40}", negative_head), "invalid_actions_identity")
    metadata = {}
    for role, identifier in (("positive", positive_run), ("negative", negative_run)):
        prefix = f"repos/{repository}/actions/runs/{identifier}"
        run = decode(commands.run(["gh", "api", prefix]).stdout)
        jobs = decode(commands.run(["gh", "api", prefix + "/jobs?per_page=100&filter=latest"]).stdout)
        require(run["status"] == "completed" and run["repository"]["full_name"] == repository
                and run["head_repository"]["full_name"] == repository and run["path"] == ".github/workflows/ci.yml"
                and jobs["total_count"] == len(jobs["jobs"]), "unverified_actions_run")
        expected = spec["source_git_sha"] if role == "positive" else negative_head
        require(run["head_sha"] == expected, "actions_head_mismatch")
        states = {job["name"]: job["conclusion"] for job in jobs["jobs"]}
        require(set(states) == {"static", "lifecycle"}, "actions_job_inventory_changed")
        if role == "positive":
            require(run["conclusion"] == "success" and all(value == "success" for value in states.values()), "actual_positive_ci_required")
        else:
            require(run["conclusion"] == "failure" and states == {"static": "failure", "lifecycle": "skipped"}, "actual_negative_ci_required")
            log = commands.run(["gh", "run", "view", str(identifier), "--repo", repository, "--log-failed"]).stdout
            require(b"AssertionError" in log and b"docker/ci/static.py" in log, "wrong_negative_ci_failure")
        metadata[role] = {"run": run, "jobs": jobs}
    negative_commit = decode(commands.run(["gh", "api", f"repos/{repository}/commits/{negative_head}"]).stdout)
    require({item["filename"] for item in negative_commit["files"]} == {"tests/fixtures/ci-contract.json"}, "negative_ci_not_isolated_contract_change")
    # Fetch fixture bytes at BOTH immutable heads, not the current working tree.
    fixtures = {}
    for role, commit in (("positive", spec["source_git_sha"]), ("negative", negative_head)):
        value = decode(commands.run(["gh", "api", f"repos/{repository}/contents/tests/fixtures/ci-contract.json?ref={commit}"]).stdout)
        require(value["encoding"] == "base64", "unexpected_github_contents_encoding")
        fixtures[role] = decode(base64.b64decode(value["content"], validate=False))
        immutable(root / (role + "-fixture.json"), fixtures[role])
    require(canonical(fixtures["negative"]) != canonical(fixtures["positive"]), "negative_fixture_not_broken")
    attempt = metadata["positive"]["run"]["run_attempt"]
    artifact_name = f"isolated-ci-{positive_run}-{attempt}"
    artifacts = decode(commands.run(["gh", "api", f"repos/{repository}/actions/runs/{positive_run}/artifacts?per_page=100"]).stdout)
    found = [item for item in artifacts["artifacts"] if item["name"] == artifact_name and item["expired"] is False]
    require(len(found) == 1, "actual_ci_artifact_unavailable")
    destination = root / "positive-artifact"
    commands.run(["gh", "run", "download", str(positive_run), "--repo", repository, "--name", artifact_name, "--dir", destination])
    require([path.name for path in destination.iterdir()] == ["ci.json"] and not (destination / "ci.json").is_symlink(), "unexpected_ci_public_artifact")
    receipt = read(destination / "ci.json")
    required = {"full_unit_boundary", "synthetic_gx_admission_dvc_roundtrip", "smoke_two_then_final_fit", "unchanged_dvc_cache_hit",
                "offline_validation_only_model_gate", "actual_api_sqlite_privacy_isolation", "ephemeral_release_approve_activate_idempotence",
                "reserved_bytes_absent", "scoped_cleanup"}
    require(receipt["outcome"] == "passed" and receipt["git_sha"] == spec["source_git_sha"]
            and required <= set(receipt["checks"]) and all(value is True for value in receipt["checks"].values())
            and type(receipt["unit_tests"]) is int and receipt["unit_tests"] > 0
            and len(receipt["api_checks"]) == 12 and all(value is True for value in receipt["api_checks"].values()), "incomplete_actual_ci_lifecycle")
    proof = {"schema_version": 1, "outcome": "passed", "source_git_sha": spec["source_git_sha"],
        "positive_run_id": positive_run, "negative_run_id": negative_run, "negative_head": negative_head,
        "positive_artifact_id": found[0]["id"], "positive_artifact_name": artifact_name,
        "positive_receipt_sha256": sha((destination / "ci.json").read_bytes()), "metadata": metadata,
        "fixtures_sha256": {role: sha(canonical(value)) for role, value in fixtures.items()},
        "scope": "genuine hosted Actions; historical negative explicitly attributed to its exact head"}
    immutable(root / "actions.json", proof)
    return {"outcome": "passed", "native_receipt_sha256": sha(canonical(proof)), "evidence_sha256": commands.finish()}


def claim_traceability(register, evidence_root):
    """S8 checks numeric claims against retained JSON values; no invented E results."""
    require(isinstance(register, dict) and set(register) == {"schema_version", "evidence_scope", "independent_real_corpus", "experiments", "claims"}
            and type(register["schema_version"]) is int and register["schema_version"] == 1
            and register["evidence_scope"] == "demo_only" and register["independent_real_corpus"] is False, "unqualified_claim_register")
    states = register["experiments"]
    require(set(states) == {"E1", "E2", "E3", "E4", "E5"}
            and all(state in ("not_run", "passed", "failed", "incomplete", "recovery_required") for state in states.values()), "invalid_experiment_statuses")
    require(isinstance(register["claims"], list), "invalid_claims")
    metric_units = {"ndcg_at_10": "ratio", "recall_at_10": "ratio", "wall_seconds": "seconds", "active_human_seconds": "seconds",
        "setup_seconds": "seconds", "warmup_seconds": "seconds", "elapsed_seconds": "seconds", "model_seconds": "seconds",
        "baseline_seconds": "seconds", "replicas": "count", "new_runs": "count", "new_versions": "count", "unit_tests": "count",
        "pairs": "count", "groups": "count"}
    root = Path(evidence_root).resolve();ids = set();checked = []
    for claim in register["claims"]:
        require(set(claim) == {"id", "kind", "experiment", "metric", "value", "units", "repetitions", "limitation", "evidence", "selector"}
                and isinstance(claim["id"], str) and claim["id"] not in ids, "invalid_claim_identity")
        ids.add(claim["id"])
        require(claim["kind"] in ("engineering_qualification", "synthetic_experiment")
                and type(claim["value"]) in (int, float) and math.isfinite(claim["value"])
                and type(claim["repetitions"]) is int and claim["repetitions"] > 0
                and isinstance(claim["units"], str) and claim["units"] and isinstance(claim["limitation"], str) and claim["limitation"], "unqualified_numeric_claim")
        require(claim["metric"] in metric_units and claim["units"] == metric_units[claim["metric"]], "claim_metric_units_mismatch")
        if claim["kind"] == "engineering_qualification":
            require(claim["experiment"] is None, "acceptance_cannot_be_experimental_repeat")
        else:
            require(claim["experiment"] in states and states[claim["experiment"]] == "passed", "claim_without_executed_experiment")
        reference = claim["evidence"]
        relative = Path(reference["path"])
        require(not relative.is_absolute() and relative.parts and ".." not in relative.parts, "invalid_claim_evidence_path")
        current = root
        for part in relative.parts:
            current = current / part
            require(not current.is_symlink(), "invalid_claim_evidence_path")
        value = document({"path": str(root / relative), "sha256": reference["sha256"]})
        require(isinstance(claim["selector"], list) and claim["selector"] and claim["metric"] in claim["selector"], "explicit_claim_selector_required")
        for key in claim["selector"]:
            require(isinstance(key, str) or type(key) is int and key >= 0, "invalid_claim_selector")
            value = value[key]
        require(type(value) in (int, float) and math.isfinite(value) and value == claim["value"], "claim_differs_from_raw_measurement")
        require(claim["units"] != "count" or type(value) is int and value >= 0, "invalid_count_claim")
        require(claim["units"] != "seconds" or value >= 0, "invalid_duration_claim")
        checked.append({"id": claim["id"], "source_sha256": reference["sha256"], "value": value, "repetitions": claim["repetitions"]})
    return {"schema_version": 1, "outcome": "passed", "checked_claims": checked, "experiments": states,
            "independent_real_corpus": False, "evidence_scope": "demo_only",
            "limitations": ["Synthetic engineering qualification is not real hiring evidence.", "E2 remains not_run until five actual human pairs exist."]}


def degraded_fixture(spec, root):
    """Build one honestly registered controlled fault for later real T11 denials.

    It creates zero fits and is never eligible as the selected S1/E5 candidate.
    The normal training implementation and its input rows are not patched.
    """
    from service.integrity import release_binding
    config = environment(spec, series="E3");root = Path(root);commands = Commands(root)
    owned_core(commands,config)
    baseline,_ = checked_fixture(config,"baseline")
    manifest,_,parent = release_binding(Path(baseline["package"]))
    require(parent["model"]["sha256"]==spec["bindings"]["model_artifact_sha256"]
            and parent["git_sha"]==spec["source_git_sha"],"negative_fixture_requires_selected_S1_parent")
    run = prepare_run(spec,config,root);workspace = run/"workspace"
    target = workspace/"negative-parent";target.mkdir()
    shutil.copytree(Path(baseline["package"])/manifest["model_relative_path"],target/"model")
    for name in ("run_manifest.json","pipeline_manifest.json"):
        shutil.copyfile(Path(baseline["package"])/name,target/name)
    (workspace/"negative-image.json").write_bytes(canonical(config["image"]))
    worker(commands,config,run,["-m","ats_mlops.evaluation.negative_model","--source-root","/run/source",
        "--parent","/run/workspace/negative-parent","--manifest","/run/workspace/data_manifest.json",
        "--admission","/run/workspace/admission/data-admission.json","--output","/run/workspace/.local/controlled-negative",
        "--image","/run/workspace/negative-image.json"])
    output = workspace/".local/controlled-negative";proof = read(output/"fault-provenance.json")
    require(proof["scope"]=="controlled_negative_artifact_fault" and type(proof["fit_events"]) is int and proof["fit_events"]==0
            and proof["selected_for_E5"] is False and proof["candidate_mlflow"]["run_id"]!=parent["mlflow"]["run_id"],"negative_fixture_not_fresh_or_attributed")
    request = {"validation_manifest":str(workspace/"data_manifest.json"),"admission_report":str(workspace/"admission/data-admission.json"),
        "dataset_binding":str(workspace/"dataset-binding.json"),"thresholds":str(Path(config["repo"])/"configs/quality.yaml"),
        "candidate":{"model_dir":str(output/"model"),"run_manifest":str(output/"run_manifest.json"),"pipeline_manifest":str(output/"pipeline_manifest.json")},
        "incumbent":{"model_dir":str(Path(baseline["package"])/manifest["model_relative_path"]),
            "run_manifest":str(Path(baseline["package"])/"run_manifest.json"),"pipeline_manifest":str(Path(baseline["package"])/"pipeline_manifest.json"),
            "release_id":manifest["release_id"],"release_is_fixture":False}}
    immutable(root/"bad-candidate-request.json",request)
    immutable(root/"fault-provenance.json",proof)
    return {"outcome":"created_controlled_fault","candidate_mlflow":proof["candidate_mlflow"],"fit_events":0,
            "request_sha256":sha(canonical(request)),"evidence_sha256":commands.finish()}


def schema_rejection(spec, root):
    """S3 runs real GX on a fresh owned negative fixture, not frozen test bytes."""
    config = environment(spec, series="E3");root = Path(root);commands = Commands(root)
    run = prepare_run(spec, config, root)
    original = document(config["fixtures"]["schema_fixture_config"])
    require(type(original.get("vacancy_groups")) is int and 3 <= original["vacancy_groups"] <= 20, "bounded_schema_fixture_required")
    fixture_config = dict(original, namespace="acc-" + sha(config["project"].encode())[:16] + "-schema")
    (run / "workspace/.local").mkdir(exist_ok=True)
    (run / "workspace/.local/schema-config.json").write_bytes(canonical(fixture_config))
    script = r'''
import json,hashlib
from pathlib import Path
from ats_mlops.data.dataset import generate_dataset,canonical_json
from ats_mlops.data.validate import validate_dataset
root=Path('/run/workspace/.local/schema-fixture');root.mkdir()
manifest_path=root/'data_manifest.json'
generate_dataset(Path('/run/workspace/.local/schema-config.json'),manifest_path,root/'data')
manifest=json.loads(manifest_path.read_bytes())
source=root/manifest['artifacts']['source']['path'];rows=[json.loads(line) for line in source.read_bytes().splitlines()]
rows[0].pop('candidate_text');content=b''.join(canonical_json(row) for row in rows);source.write_bytes(content)
manifest['artifacts']['source']['sha256']=hashlib.sha256(content).hexdigest()
manifest_path.write_bytes(canonical_json(manifest))
report=validate_dataset(manifest_path,Path('/run/workspace/.local/schema-report'),mode='admission')
assert report['outcome']=='rejected' and any(batch['scope']=='source' and batch['success'] is False for batch in report['gx'])
print(json.dumps({'report':report,'fresh_fixture_test_sha256':manifest['artifacts']['test']['sha256']}))
'''
    result = decode(worker(commands, config, run, ["-c", script], network=False).stdout.strip().splitlines()[-1])
    require(result["fresh_fixture_test_sha256"] != spec["bindings"]["test_partition_sha256"], "original_test_cannot_be_admission_fixture")
    immutable(root / "schema-rejection.json", {"schema_version": 1, "outcome": "passed", "source_git_sha": spec["source_git_sha"],
        "validation": result["report"], "fresh_fixture_test_sha256": result["fresh_fixture_test_sha256"],
        "original_test_read": False, "training_invoked": False, "scope": "new synthetic GX negative admission only"})
    return {"outcome": "passed", "native_receipt_sha256": sha((root / "schema-rejection.json").read_bytes()), "evidence_sha256": commands.finish()}


def e3(spec, root):
    """Six real scoped fault drivers; nothing runs merely by importing the module."""
    from .experiment import FAULTS
    from ats_mlops.release.state import ReleaseError
    root = Path(root);fault, repeat = root.name.rsplit("-", 1)
    require(fault in FAULTS and repeat in ("1", "2", "3"), "invalid_native_fault_slot")
    config = environment(spec, series="E3");commands = Commands(root)
    isolation_sha = owned_core(commands, config)
    baseline, baseline_manifest = checked_fixture(config, "baseline")
    target, target_manifest = checked_fixture(config, "target")
    require(baseline_manifest["expected_incumbent"] is None, "actual_first_release_fixture_required")
    controller = _controller(config, root, unready_release=target_manifest["release_id"] if fault == "unready_container" else None)
    samples, stop = [], threading.Event();sampler = None
    disconnected = None
    try:
        require(controller.registry.get() is None, "fresh_private_alias_required")
        controller.stage(Path(baseline["package"]), Path(baseline["check"]["path"]))
        controller.approve(baseline_manifest["release_id"], "agent-operator", "A3")
        controller.activate(baseline_manifest["release_id"])
        _consistent(controller)
        sampler = threading.Thread(target=_sample_http, args=(controller, samples, stop), daemon=True);sampler.start()
        deadline = time.monotonic() + 5
        while not samples and time.monotonic() < deadline:time.sleep(.02)
        require(samples and samples[-1]["ok"], "initial_http_observation_required")
        injected = time.monotonic()
        if fault == "corrupt_data":
            run_pipeline(spec, config, root, commands, corrupt=True)
        elif fault == "bad_candidate":
            from ats_mlops.quality.gate import run_gate
            gate = run_gate(document(config["fixtures"]["bad_candidate_request"]), root / "bad-candidate-gate.json")
            require(gate["outcome"] == "rejected" and any(reason.startswith("candidate.ndcg_drop_against_") for reason in gate["reasons"]), "genuine_degraded_model_required")
        elif fault == "incomplete_artifact":
            damaged = root / "damaged-package";shutil.copytree(target["package"], damaged)
            payload = damaged / target_manifest["model_relative_path"] / "model.pkl";payload.chmod(0o600);payload.unlink()
            try:controller.stage(damaged, Path(target["check"]["path"]))
            except (ReleaseError, ValueError):pass
            else:raise ExperimentError("incomplete_artifact_was_accepted")
        elif fault == "minio_unavailable":
            container = config["core"]["containers"]["minio"]
            aliases = decode(commands.run(["docker", "inspect", "--format", '{{json (index .NetworkSettings.Networks "' + config["network"] + '").Aliases}}', container]).stdout)
            commands.run(["docker", "network", "disconnect", config["network"], container]);disconnected = (container, aliases)
            _package_io_attempt(spec, config, root, commands)
        else:
            controller.stage(Path(target["package"]), Path(target["check"]["path"]))
            controller.approve(target_manifest["release_id"], "agent-operator", "A3")
            if fault == "unready_container":
                try:controller.activate(target_manifest["release_id"])
                except ReleaseError as error:require(str(error) == "new_service_failed_previous_restored", "unexpected_startup_failure")
                else:raise ExperimentError("unready_container_was_accepted")
            elif fault == "switch_alias_crash":
                payload = {"state": str(controller.root), "config": controller.config, "release_id": target_manifest["release_id"]}
                immutable(root / "crash-request.json", payload)
                code = "import json,os;from pathlib import Path;from ats_mlops.release.controller import Controller;q=json.loads(Path(__import__('sys').argv[1]).read_bytes());Controller(q['state'],q['config'],checkpoint=lambda phase:os._exit(73) if phase=='target_ready' else None).activate(q['release_id'])"
                commands.run([sys.executable, "-B", "-c", code, str(root / "crash-request.json")], env=os.environ | {"PYTHONPATH": str(Path(config["repo"]) / "src")}, accepted=(73,), timeout=90)
                require(controller.store.read()["pending"]["phase"] == "target_ready", "durable_switch_crash_not_observed")
            else:raise ExperimentError("unhandled_native_fault")
        detected = time.monotonic()
        if disconnected:
            container, aliases = disconnected
            command = ["docker", "network", "connect"]
            for alias in aliases or []:command.extend(["--alias", alias])
            commands.run([*command, config["network"], container]);disconnected = None
            _package_io_attempt(spec, config, root, commands, expect_failure=False)
        controller.reconcile()
        if controller.store.read()["active"]["release_id"] != baseline_manifest["release_id"]:
            controller.rollback(baseline_manifest["release_id"])
        state, observed = _consistent(controller);restored = time.monotonic()
        deadline = time.monotonic() + 5
        while (not samples or samples[-1]["t"] <= restored) and time.monotonic() < deadline:time.sleep(.02)
        require(samples[-1]["t"] > restored and samples[-1]["ok"], "final_http_observation_required")
        proof = {"fault": fault, "state": state, "observation": observed, "injected_monotonic": injected,
                 "detected_monotonic": detected, "restored_monotonic": restored}
        immutable(root / "fault.json", proof)
        observation = {"fault": fault, "repeat": int(repeat), "own_resources_only": True, "isolation_sha256": isolation_sha,
            "fault_receipt_sha256": sha(canonical(proof)), "final_consistent": True, "terminal": "expected_rejection",
            "injected_monotonic": injected, "detected_monotonic": detected, "restored_monotonic": restored, "http_samples": samples}
    finally:
        stop.set()
        if sampler:sampler.join(timeout=2)
        if disconnected:
            container, aliases = disconnected;command = ["docker", "network", "connect"]
            for alias in aliases or []:command.extend(["--alias", alias])
            commands.run([*command, config["network"], container])
        try:
            _cleanup_release(controller, commands)
        finally:
            evidence_sha = commands.finish()
    return {"outcome": "expected_rejection", "observations": observation, "evidence_sha256": evidence_sha}


def declare_monitoring_child(spec, output):
    """Bind the accepted structural child; never regenerate or select by quality."""
    config = environment(spec,series="E4")
    parent = read(Path(config["bundle"])/"data_manifest.json")
    value=document(config["E4"]["child_plan"])
    require(value["source_git_sha"]==spec["source_git_sha"] and value["seed"]==20261027
            and type(value["config"]["vacancy_groups"]) is int and value["config"]["vacancy_groups"]==64
            and value["config"]["namespace"]=="ats-t18-e4-child-v2" and value["parent_bundle_sha256"]==config["bundle_sha256"]
            and value["parent_policy_sha256"]==parent["reservation"]["policy"]["sha256"],"accepted_E4_child_binding_required")
    immutable(Path(output),value)
    return value


def prepare_monitoring(spec, root):
    """Explicit private qualification setup; never initialize a default policy.

    The caller must have provisioned a fresh owned Airflow/policy volume, pinned
    overlay and matched numeric reference. No model fit occurs in this setup.
    Formal E4 later uses another fresh campaign so qualification is not measured
    twice or silently cleared. No reset operation is exposed.
    """
    from ats_mlops.monitoring.retraining import NativePolicy
    config = environment(spec, series="E4");root = Path(root)
    require(root.resolve() == Path(config["E4"]["root"]).resolve() and not root.exists(), "fresh_monitoring_campaign_required")
    root.mkdir(parents=True, mode=0o700);commands = Commands(root)
    native = NativePolicy(config["E4"]["batch"]["airflow_container"],config["E4"]["batch"]["training_image_id"],project=config["project"])
    require(native.call("snapshot", {}) == {"state": "uninitialized"}, "fresh_policy_volume_required")
    owned_core(commands, config)
    controller = _controller(config,root)
    _launch_baseline(config,root)
    state,observed = _consistent(controller)
    value = {"schema_version":1,"outcome":"prepared","source_git_sha":spec["source_git_sha"],"E4":"not_run",
             "state":state,"observation":observed,"command_evidence_sha256":commands.finish()}
    immutable(root/"prepared.json",value)
    return value


def e4(spec, root):
    """One real frozen T17 scenario; no synthetic WindowEvidence or callbacks."""
    from .native_monitoring import Campaign
    config = environment(spec, series="E4");root = Path(root);commands = Commands(root)
    owned_core(commands,config)
    observed = Campaign(spec,config,commands).run(root.name,root)
    return {"outcome":"passed","observations":observed,"evidence_sha256":commands.finish()}


def _qualified(spec, config):
    """Metadata and validation only; this runs before initializing the E5 ledger."""
    from ats_mlops.release.controller import load_package
    from service.integrity import release_binding
    from ats_mlops.quality.artifacts import validation_snapshot
    from ats_mlops.data.dataset import load_partition
    from ats_mlops.data.reservation import manifest_policy, validate_partitions
    value = config["qualified"]
    require(set(value) == {"package", "check", "training_manifest", "admission_report", "dataset_binding", "selection", "plan"}, "invalid_qualified_inputs")
    for key in ("check", "training_manifest", "admission_report", "dataset_binding"):document(value[key])
    package = Path(value["package"])
    require(not any(path.suffix.lower() == ".jsonl" for path in package.rglob("*")), "raw_partition_in_model_package")
    manifest, _, _ = load_package(package, Path(value["check"]["path"]))
    _, _, record = release_binding(package)
    require(record["git_sha"] == spec["qualified_model_source_git_sha"]
            and sha((package / "run_manifest.json").read_bytes()) == spec["bindings"]["model_lineage_sha256"]
            and record["model"]["sha256"] == spec["bindings"]["model_artifact_sha256"]
            and record["features_sha256"] == spec["bindings"]["features_sha256"]
            and record["params_sha256"] == spec["bindings"]["params_sha256"]
            and record["data_manifest_sha256"] == spec["bindings"]["data_manifest_sha256"]
            and baseline_identity(record) == spec["bindings"]["baseline_artifact_sha256"], "qualified_model_binding_mismatch")
    selected, plan = document(value["selection"]), document(value["plan"])
    require(selected["slot"] == plan["slot"] == "S1" and selected["outcome"] == "selected"
            and selected["selection_rule"] == plan["selection_rule"] == "first_required_fit_before_metrics"
            and selected["plan_sha256"] == sha(canonical(plan)) and selected["mlflow"] == record["mlflow"]
            and selected["model_artifact_sha256"] == record["model"]["sha256"]
            and selected["model_lineage_sha256"] == sha(canonical(record))
            and selected["source_git_sha"] == plan["source_git_sha"] == record["git_sha"], "qualified_first_S1_selection_mismatch")
    training_metadata = document(value["training_manifest"])
    require(type(training_metadata["schema_version"]) is int and training_metadata["schema_version"] == 2
            and record["read_partitions"] == ["train", "validation"] and record["test_read"] is False
            and record["dirty_worktree"] is False, "guarded_schema2_model_required")
    manifest_path = Path(value["training_manifest"]["path"])
    policy = manifest_policy(manifest_path, training_metadata)
    partitions = {name: load_partition(manifest_path, name) for name in ("train", "validation")}
    validate_partitions(partitions, training_metadata["duplicate_detection"]["threshold"], policy)
    snapshot = validation_snapshot(manifest_path, Path(value["admission_report"]["path"]), Path(value["dataset_binding"]["path"]))
    require(snapshot["manifest_sha256"] == record["data_manifest_sha256"] and snapshot["rows"], "qualified_warmup_binding_mismatch")
    group = sorted({row["vacancy_id"] for row in snapshot["rows"]})[0]
    warmup = [row for row in snapshot["rows"] if row["vacancy_id"] == group]
    return {"artifact": package / manifest["model_relative_path"], "record": record}, warmup


class HoldoutSession:
    """READY before durable consume; the original test snapshot travels only stdin.

    Docker receives one standalone worker file, exact artifact, validation-only
    preflight and an empty numeric output directory. No training checkout,
    credentials, network or daemon socket enters the scoring process.
    """
    def __init__(self, model, warmup, *, baseline_artifact_sha256, output):
        self.model, self.warmup = model, warmup
        self.baseline_sha = baseline_artifact_sha256
        self.output = Path(output)
        self.image = model["record"]["environment"]["image"]["id"]
        self.worker = Path(__file__).with_name("holdout_worker.py").resolve()
        self.worker_sha = sha(self.worker.read_bytes())
        self.name = "ats-final-score-" + uuid.uuid4().hex[:12]
        self.process = self.private = self.stderr = None
        self.scored = False

    def _line(self, timeout):
        require(select.select([self.process.stdout], [], [], timeout)[0], "holdout_worker_timeout")
        line = self.process.stdout.readline(65537)
        require(bool(line) and len(line) <= 65536 and line.endswith(b"\n"), "invalid_holdout_protocol")
        return decode(line)

    def __enter__(self):
        try:
            require(subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", self.image],
                capture_output=True, text=True, check=True).stdout.strip() == self.image, "final_runtime_image_unavailable")
            require(not self.output.exists() and not self.output.is_symlink(), "fresh_worker_output_required")
            self.output.mkdir(parents=True, mode=0o700)
            self.private = tempfile.TemporaryDirectory(prefix="ats-final-preflight-")
            preflight = {"schema_version": 1, "record": self.model["record"], "warmup_rows": self.warmup,
                         "baseline_artifact_sha256": self.baseline_sha}
            immutable(Path(self.private.name) / "preflight.json", preflight)
            self.stderr = (self.output.parent / (self.output.name + "-stderr.txt")).open("xb")
            argv = ["docker", "run", "--rm", "-i", "--pull=never", "--name", self.name, "--network", "none", "--read-only",
                "--cpus", "1", "--memory", "1536m", "--user", f"{os.getuid()}:{os.getgid()}", "--workdir", "/tmp",
                "--tmpfs", "/tmp:rw,size=128m,mode=1777", "--mount", f"type=bind,source={self.model['artifact'].resolve()},target=/artifact,readonly",
                "--mount", f"type=bind,source={self.private.name},target=/input,readonly",
                "--mount", f"type=bind,source={self.worker},target=/worker.py,readonly",
                "--mount", f"type=bind,source={self.output.resolve()},target=/out",
                "-e", "HOME=/tmp", "-e", "USER=ats-final", "-e", "LOGNAME=ats-final", "-e", "PYTHONPATH=", "-e", "MLFLOW_TRACKING_URI=",
                "-e", "AWS_ACCESS_KEY_ID=", "-e", "AWS_SECRET_ACCESS_KEY=", "-e", "AWS_SESSION_TOKEN=",
                "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
                "--entrypoint", "/opt/ml-venv/bin/python", self.image, "-I", "-B", "/worker.py", "--request", "/input/preflight.json", "--output", "/out"]
            self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr)
            ready = self._line(180)
            expected = {"status": "ready", "worker_sha256": self.worker_sha, "warmup": "train_validation_only",
                "receipt": {"isolated": True, "bytecode_disabled": True, "prior_project_import": False, "network": False,
                "training_checkout_mounted": False, "credentials": False, "model_sha256": self.model["record"]["model"]["sha256"],
                "baseline_artifact_sha256": self.baseline_sha, "source_tree_sha256": self.model["record"]["source_tree_sha256"]}}
            require(canonical(ready) == canonical(expected) and canonical(read(self.output / "ready.json")) == canonical(ready),
                    "final_scoring_preflight_mismatch")
            self.ready = ready
            return self
        except BaseException:
            self.close()
            raise

    def score(self, snapshot):
        from .holdout_worker import validate_numeric, numeric_seal
        require(not self.scored and self.process is not None, "one_snapshot_only")
        self.scored = True
        message = {"schema_version": 1, "snapshot_sha256": snapshot["sha256"], "pair_ids_sha256": snapshot["pair_ids_sha256"],
                   "ground_truth_sha256": snapshot["ground_truth_sha256"], "snapshot": {"rows": snapshot["rows"]}}
        # Generic evaluate_once has already fsynced and SQL-bound numeric truth.
        stdout, _ = self.process.communicate(canonical(message), timeout=180)
        require(self.process.returncode == 0 and len(stdout) <= 65536 and len(stdout.splitlines()) == 1, "final_scoring_failed")
        final = decode(stdout)
        numeric, truth, commit = (read(self.output / name) for name in ("numeric.json", "truth.json", "commit.json"))
        validate_numeric(self.ready, truth, numeric)
        hashes = {key: sha((self.output / filename).read_bytes()) for key, filename in
                  (("ready_sha256", "ready.json"), ("truth_sha256", "truth.json"), ("numeric_sha256", "numeric.json"))}
        seal_path = self.output / "score-seal.json"
        seal = read(seal_path)
        require(seal_path.read_bytes() == canonical(seal) == canonical(numeric_seal(self.ready, truth, numeric))
                and all(seal[key] == value for key, value in hashes.items()), "final_numeric_score_seal_mismatch")
        expected = {"schema_version": 1, "status": "completed", "worker_sha256": self.worker_sha, **hashes,
                    "score_seal_sha256": sha(seal_path.read_bytes()),
                    "rows": len(truth["rows"]), "groups": len(numeric["groups"]),
                    **{key: snapshot[key if key != "snapshot_sha256" else "sha256"] for key in
                       ("snapshot_sha256", "pair_ids_sha256", "ground_truth_sha256")}}
        require(canonical(commit) == canonical(expected) and canonical(final) == canonical({"status": "completed",
                "numeric_sha256": hashes["numeric_sha256"], "commit_sha256": sha((self.output / "commit.json").read_bytes())}),
                "final_numeric_commit_mismatch")
        return numeric

    def close(self):
        if self.process is not None:
            if self.process.poll() is None:
                self.process.kill()
            self.process.wait(timeout=20)
            for stream in (self.process.stdin, self.process.stdout):
                if stream and not stream.closed:stream.close()
        if self.stderr is not None:self.stderr.close()
        if self.private is not None:self.private.cleanup()
        # Exact unique container only. Never prune or stop another namespace.
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True, timeout=20)

    def __exit__(self, *error):
        self.close()


def holdout_ledger():
    """One OS-account ledger, independent of study, candidate and namespace.

    Resolve account metadata, never a caller path or HOME environment override.
    Preparation checks metadata/symlinks only: no directory/SQLite is created.
    This is an operational boundary, not protection from an OS administrator.
    """
    import pwd
    home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    require(home.is_absolute() and home == home.resolve(), "canonical_account_home_required")
    path = home / ".local/state/ats-mlops/evaluation/holdout.sqlite3"
    current = path
    while current != current.parent:
        require(not current.is_symlink(), "holdout_ledger_symlink_forbidden")
        current = current.parent
    identity = {"schema_version": 1, "scope": "one_OS_account_all_ATS_studies",
        "path_rule": "OS_account_home/.local/state/ats-mlops/evaluation/holdout.sqlite3",
        "os_uid": os.getuid(), "canonical_path_sha256": sha(str(path).encode())}
    return path, identity


def evaluate_e5(directory, authorization):
    """No public reset/force; root must authorize this exact frozen one-use run.

    Metadata preflight occurs before the ledger. evaluate_once owns the durable
    consume BEFORE the read callback. This function is not called in preparation.
    """
    from .experiment import evaluate_once, holdout_key, study
    from ats_mlops.data.dataset import load_partition
    from ats_mlops.data.reservation import Registry, ROOT_POLICY_SHA256

    spec, header = study(directory)
    config = environment(spec, series="E5")
    require(isinstance(authorization, dict) and set(authorization) == {"spec_sha256", "holdout_lineage_sha256", "actor", "scope"}
            and authorization["spec_sha256"] == header["spec_sha256"] and authorization["holdout_lineage_sha256"] == holdout_key(spec)
            and authorization["actor"] == "root-coordinator" and authorization["scope"] == "one_attempt_synthetic_final_test", "explicit_final_evaluation_authorization_required")
    guard_path, ledger_identity = holdout_ledger()
    require(sha(canonical(ledger_identity)) == spec["bindings"]["holdout_ledger_sha256"], "frozen_global_holdout_ledger_mismatch")
    model, warmup = _qualified(spec, config)
    holdout = config["holdout"]
    require(set(holdout) == {"manifest", "policy"}, "invalid_holdout_metadata")
    manifest, policy = document(holdout["manifest"]), document(holdout["policy"])
    Registry(policy)
    root_policy = policy
    while root_policy["parent"] is not None:root_policy = root_policy["parent"]
    protected = [entry for entry in root_policy["documents"] if entry["fold"] == "test"]
    require(sha(canonical(root_policy)) == ROOT_POLICY_SHA256 == spec["bindings"]["root_policy_sha256"]
            and sha(canonical(protected)) == spec["bindings"]["protected_test_documents_sha256"]
            and manifest["artifacts"]["test"]["sha256"] == spec["bindings"]["test_partition_sha256"]
            and manifest["splits"]["test"]["rows"] == spec["test_inventory"]["pairs"]
            and manifest["splits"]["test"]["groups"] == spec["test_inventory"]["groups"]
            and sha(canonical(sorted(manifest["splits"]["test"]["vacancy_ids"]))) == spec["test_inventory"]["group_ids_sha256"]
            and sha(canonical(sorted(manifest["splits"]["test"]["pair_ids"]))) == spec["test_inventory"]["pair_ids_sha256"], "holdout_metadata_binding_mismatch")
    def read_snapshot():
        require(document(holdout["manifest"]) == manifest, "holdout_metadata_changed")
        rows = load_partition(Path(holdout["manifest"]["path"]), "test", final_evaluation=True)
        return {"sha256": manifest["artifacts"]["test"]["sha256"], "rows": rows}
    # Model/metadata checks and validation-only warmup finish BEFORE the ledger
    # is initialized or the durable holdout intent is consumed.
    with HoldoutSession(model, warmup, baseline_artifact_sha256=spec["bindings"]["baseline_artifact_sha256"],
                        output=Path(directory) / "E5-worker") as session:
        return evaluate_once(directory, guard_path, read_snapshot, session.score)
