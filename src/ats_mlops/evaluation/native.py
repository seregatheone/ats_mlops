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
import json
import math
import os
from pathlib import Path
import re
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
    require(path.is_file() and not path.is_symlink(), "missing_native_input")
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


def environment(spec, *, series):
    """All mutable locations and effect targets are part of the frozen binding."""
    validate_spec(spec)
    path = os.environ.get("ATS_EXPERIMENT_ENVIRONMENT")
    require(path is not None and not Path(path).is_symlink(), "native_environment_required")
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
    require(isinstance(core, dict) and set(core) == {"containers"} and set(core["containers"]) == {"postgres", "minio", "mlflow"}, "isolated_core_identity_required")
    observations = {}
    for role, name in core["containers"].items():
        require(isinstance(name, str) and re.fullmatch(r"[a-z0-9][a-z0-9_.-]{2,127}", name), "invalid_owned_container")
        fmt = '{"Id":{{json .Id}},"Image":{{json .Image}},"State":{{json .State}},"Labels":{{json .Config.Labels}},"Networks":{{json .NetworkSettings.Networks}}}'
        value = decode(commands.run(["docker", "inspect", "--format", fmt, name]).stdout)
        labels = value["Labels"]
        require(labels["com.docker.compose.project"] == config["project"] and labels["com.docker.compose.service"] == role
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
        self.records.append({"argv": list(map(str, argv)), "exit_code": completed.returncode, **paths})
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


def _package_io_attempt(spec, config, root, commands, *, expect_failure=True):
    """Fresh Bento store/download through private MLflow; no credential mounts."""
    inputs = config["fixtures"]["package_inputs"]
    require(set(inputs) == {"gate_report", "run_manifest", "pipeline_manifest", "serving_identity"}, "package_fixture_inputs_required")
    private = Path(root) / "package-inputs";private.mkdir(exist_ok=True)
    request = {"release_id": config["project"] + "-io-fault"}
    for name, reference in inputs.items():
        document(reference)
        (private / (name + ".json")).write_bytes(Path(reference["path"]).read_bytes())
        require(sha((private / (name + ".json")).read_bytes()) == reference["sha256"], "package_fixture_changed")
        request[name] = "/input/" + name + ".json"
    (private / "request.json").write_bytes(canonical(request))
    code = "import sys,json;from pathlib import Path;sys.path.insert(0,'/source');from service.package import package;package(json.loads(Path('/input/request.json').read_bytes()),Path('/output/io-package'))"
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
        require(result.returncode != 0 and not (Path(root) / "io-package").exists(), "minio_fault_was_not_observed")
    else:
        require(result.returncode == 0 and (Path(root) / "io-package/release_manifest.json").is_file(), "minio_recovery_not_verified")


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


def e5_bootstrap():
    """Keep the full existing T11 load/identity guard; replace only score output.

    The unique exact marker is checked before any test input exists. This avoids
    a second, weaker copy of the native/pyfunc/runtime/model/source validation.
    """
    from ats_mlops.quality.isolated import BOOTSTRAP
    marker = "    scores=score_rows(model,request['rows'])"
    require(BOOTSTRAP.count(marker) == 1, "audited_scorer_interface_changed")
    prefix = BOOTSTRAP.split(marker)[0]
    return prefix + r'''
    import time
    from collections import defaultdict
    from ats_mlops.model.baseline import score_rows as baseline_scores,MODEL_VERSION
    check(MODEL_VERSION=='jaccard-v1')
    baseline_files={name:source[name] for name in ('model/baseline.py','data/dataset.py')}
    baseline_sha=hashlib.sha256(canonical({'version':'jaccard-v1','source_sha256':baseline_files})).hexdigest()
    check(baseline_sha==request['baseline_artifact_sha256'])
    warmup=request['warmup_rows'];check(warmup)
    # Warmup is independently admitted validation only, never a test group.
    baseline_scores(warmup);score_rows(model,warmup)
    groups=defaultdict(list)
    for row in request['rows']:groups[row['vacancy_id']].append(row)
    output=[];latencies=[]
    for group,rows in sorted(groups.items()):
        start=time.perf_counter();baseline=baseline_scores(rows);baseline_seconds=time.perf_counter()-start
        start=time.perf_counter();predictions=score_rows(model,rows);model_seconds=time.perf_counter()-start
        check(set(baseline)==set(predictions)=={(row['vacancy_id'],row['candidate_id']) for row in rows})
        for row in sorted(rows,key=lambda row:row['candidate_id']):
            key=(group,row['candidate_id'])
            check(type(row['relevance']) is int and row['relevance'] in (0,1))
            output.append({'group_id':group,'candidate_id':key[1],'relevance':row['relevance'],
                           'baseline_score':float(baseline[key]),'model_score':float(predictions[key])})
        latencies.append({'group_id':group,'baseline_seconds':baseline_seconds,'model_seconds':model_seconds})
    for name,module in list(sys.modules.items()):
        if name=='ats_mlops' or name.startswith('ats_mlops.'):check(pathlib.Path(module.__file__).resolve().is_relative_to(artifact/'code'))
    print(json.dumps({'outcome':'passed','observations':{'rows':output,'groups':sorted(groups),'latencies':latencies,
       'snapshot_sha256':request['snapshot_sha256'],'model_artifact_sha256':record['model']['sha256'],
       'baseline_artifact_sha256':baseline_sha,'evaluated_groups_once':len(groups),'warmup':'train_validation_only'},
       'receipt':{'isolated':True,'bytecode_disabled':True,'prior_project_import':False,'network':False,
       'training_checkout_mounted':False,'credentials':False,'model_sha256':record['model']['sha256'],
       'baseline_artifact_sha256':baseline_sha,'source_tree_sha256':record['source_tree_sha256']}},allow_nan=False))
except Exception:
    print(json.dumps({'outcome':'rejected','reason':'isolated_final_scoring_failed'}));raise SystemExit(1)
'''


def _qualified(spec, config):
    """Metadata and validation only; this runs before initializing the E5 ledger."""
    from ats_mlops.release.controller import load_package
    from service.integrity import release_binding
    from ats_mlops.quality.artifacts import validation_snapshot
    value = config["qualified"]
    require(set(value) == {"package", "check", "training_manifest", "admission_report", "dataset_binding"}, "invalid_qualified_inputs")
    for key in ("check", "training_manifest", "admission_report", "dataset_binding"):document(value[key])
    package = Path(value["package"])
    manifest, _, _ = load_package(package, Path(value["check"]["path"]))
    _, _, record = release_binding(package)
    require(record["git_sha"] == spec["qualified_model_source_git_sha"]
            and sha((package / "run_manifest.json").read_bytes()) == spec["bindings"]["model_lineage_sha256"]
            and record["model"]["sha256"] == spec["bindings"]["model_artifact_sha256"]
            and record["features_sha256"] == spec["bindings"]["features_sha256"]
            and record["params_sha256"] == spec["bindings"]["params_sha256"]
            and record["data_manifest_sha256"] == spec["bindings"]["data_manifest_sha256"]
            and baseline_identity(record) == spec["bindings"]["baseline_artifact_sha256"], "qualified_model_binding_mismatch")
    snapshot = validation_snapshot(Path(value["training_manifest"]["path"]), Path(value["admission_report"]["path"]), Path(value["dataset_binding"]["path"]))
    require(snapshot["manifest_sha256"] == record["data_manifest_sha256"] and snapshot["rows"], "qualified_warmup_binding_mismatch")
    group = sorted({row["vacancy_id"] for row in snapshot["rows"]})[0]
    warmup = [row for row in snapshot["rows"] if row["vacancy_id"] == group]
    return {"artifact": package / manifest["model_relative_path"], "record": record}, warmup


def final_score(model, rows, warmup, *, snapshot_sha256, baseline_artifact_sha256, output):
    """One offline artifact-only process, one scoring pass per complete test group."""
    output = Path(output)
    image = model["record"]["environment"]["image"]["id"]
    bootstrap = e5_bootstrap()
    name = "ats-final-score-" + uuid.uuid4().hex[:12]
    inspected = subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", image], capture_output=True, text=True, check=True)
    require(inspected.stdout.strip() == image, "final_runtime_image_unavailable")
    with tempfile.TemporaryDirectory(prefix="ats-final-private-") as private:
        request = {"record": model["record"], "rows": rows, "warmup_rows": warmup,
                   "snapshot_sha256": snapshot_sha256, "baseline_artifact_sha256": baseline_artifact_sha256}
        Path(private, "request.json").write_bytes(canonical(request))
        command = ["docker", "run", "--rm", "--pull=never", "--name", name, "--network", "none", "--read-only", "--cpus", "1", "--memory", "1536m",
            "--user", f"{os.getuid()}:{os.getgid()}", "--workdir", "/tmp", "--tmpfs", "/tmp:rw,size=128m,mode=1777",
            "--mount", f"type=bind,source={model['artifact'].resolve()},target=/artifact,readonly", "--mount", f"type=bind,source={private},target=/input,readonly",
            "-e", "HOME=/tmp", "-e", "USER=ats-final", "-e", "LOGNAME=ats-final", "-e", "PYTHONPATH=", "-e", "MLFLOW_TRACKING_URI=",
            "-e", "AWS_ACCESS_KEY_ID=", "-e", "AWS_SECRET_ACCESS_KEY=", "-e", "AWS_SESSION_TOKEN=",
            "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
            "--entrypoint", "/opt/ml-venv/bin/python", image, "-I", "-B", "-c", bootstrap]
        try:
            completed = subprocess.run(command, capture_output=True, timeout=180)
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=20)
    # The only durable scoring payload is numeric; text/labels input temp is gone.
    result = decode(completed.stdout.strip().splitlines()[-1])
    require(completed.returncode == 0 and result["outcome"] == "passed", "final_scoring_failed")
    receipt = result["receipt"]
    require(receipt["model_sha256"] == model["record"]["model"]["sha256"]
            and receipt["baseline_artifact_sha256"] == baseline_artifact_sha256
            and receipt["source_tree_sha256"] == model["record"]["source_tree_sha256"]
            and receipt["isolated"] is True and receipt["bytecode_disabled"] is True
            and all(receipt[key] is False for key in ("prior_project_import", "network", "training_checkout_mounted", "credentials")), "final_scoring_binding_mismatch")
    immutable(output / "scorer.json", {"image_id": image, "bootstrap_sha256": sha(bootstrap.encode()), **receipt})
    return result["observations"]


def evaluate_e5(directory, guard_path, authorization):
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
            and sha(canonical(sorted(manifest["splits"]["test"]["vacancy_ids"]))) == spec["test_inventory"]["group_ids_sha256"], "holdout_metadata_binding_mismatch")
    def read_snapshot():
        require(document(holdout["manifest"]) == manifest, "holdout_metadata_changed")
        rows = load_partition(Path(holdout["manifest"]["path"]), "test", final_evaluation=True)
        return {"sha256": manifest["artifacts"]["test"]["sha256"], "rows": rows}
    def score_both(snapshot):
        return final_score(model, snapshot["rows"], warmup, snapshot_sha256=snapshot["sha256"],
                           baseline_artifact_sha256=spec["bindings"]["baseline_artifact_sha256"], output=Path(directory) / "E5")
    return evaluate_once(directory, guard_path, read_snapshot, score_both)
