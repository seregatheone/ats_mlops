"""Disposable synthetic CI lifecycle. The same entry point runs locally and in Actions."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import re
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "src"), str(ROOT)]
from scripts.bootstrap_core import initialize


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")


def free_ports():
    listeners = [socket.socket() for _ in range(4)]
    try:
        for item in listeners:
            item.bind(("127.0.0.1", 0))
        return [item.getsockname()[1] for item in listeners]
    finally:
        for item in listeners:
            item.close()


class Lifecycle:
    def __init__(self, directory, public):
        self.root, self.public = directory.resolve(), public.resolve()
        if self.root.exists():
            raise ValueError("new_external_ci_directory_required")
        self.root.mkdir(parents=True, mode=0o700)
        self.project = "ats-ci-" + uuid.uuid4().hex[:12]
        self.ports = free_ports()
        self.values = initialize(self.root / "private", self.project, tuple(self.ports[:3]))
        self.values.update({"ATS_CI_SOURCE": str(ROOT), **{
            "ATS_CI_" + key.upper() + "_IMAGE": self.project + "-" + key + ":test"
            for key in ("ml", "postgres", "minio", "mc")}})
        self.env = {**os.environ, **self.values, "COMPOSE_PARALLEL_LIMIT": "1"}
        self.compose = ["docker", "compose", "--env-file", str(self.root / "private/core.env"),
                        "-p", self.project, "-f", str(ROOT / "docker/ci/compose.yaml"), "--profile", "core"]
        self.report = {"schema_version": 1, "outcome": "failed", "evidence_scope": "demo_only", "checks": {},
                       "not_run": ["Airflow full LocalExecutor", "production deployment", "amd64 compatibility"],
                       "runner": {"machine": platform.machine(), "system": platform.system(), "cpus": os.cpu_count()},
                       "timings_seconds": {}}
        self.counter = 0
        write(self.root / "control.json", {"project": self.project})
        self.ml_image = None

    def command(self, label, args, *, env=None, timeout=180, output=False):
        self.counter += 1
        print(json.dumps({"step": label, "status": "started"}), flush=True)
        started = time.monotonic()
        path = self.root / "private" / f"{self.counter:03}-{label}.log"
        with path.open("w") as log:
            result = subprocess.run(list(map(str, args)), cwd=ROOT, env=env or self.env,
                                    stdout=subprocess.PIPE if output else log, stderr=log, text=True, timeout=timeout)
        self.report["timings_seconds"][label] = round(time.monotonic() - started, 3)
        if result.returncode:
            raise RuntimeError(label + "_failed")
        print(json.dumps({"step": label, "status": "passed"}), flush=True)
        return result.stdout if output else path

    def image(self, tag):
        return json.loads(self.command("image-inspect", ["docker", "image", "inspect", tag, "--format",
            '{"id":{{json .Id}},"os":{{json .Os}},"architecture":{{json .Architecture}},"repo_digests":{{json .RepoDigests}}}'], output=True))

    def worker(self, phase, *, dvc=False):
        source = self.root / ("run/source" if phase == "fit" else "checkout")
        args = ["docker", "run", "--rm", "--pull=never", "--network", self.project + "_default",
                "--name", self.project + "-worker", "--cpus", "1", "--memory", "1536m",
                "--user", f"{os.getuid()}:{os.getgid()}", "--workdir", str(self.root),
                "--mount", f"type=bind,source={self.root},target={self.root}",
                "-e", "HOME=/tmp", "-e", "USER=ats-ci", "-e", "LOGNAME=ats-ci", "-e", f"PYTHONPATH={source / 'src'}:{source}",
                "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1",
                "-e", "MKL_NUM_THREADS=1", "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000",
                "-e", "MLFLOW_ENABLE_ARTIFACTS_PROGRESS_BAR=false", "-e", "BENTOML_DO_NOT_TRACK=1",
                "-e", "ATS_CI_SHA=" + self.report["git_sha"]]
        if dvc:
            args += ["--env-file", str(self.root / "private/dvc.env")]
        args += ["--entrypoint", "/opt/ml-venv/bin/python", self.ml_image["id"], "-B",
                 source / "docker/ci/worker.py", phase, self.root]
        self.command(phase, args, timeout=600)

    def execute(self):
        status = subprocess.run(["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT,
                                capture_output=True, text=True, check=True).stdout
        if status:
            raise ValueError("committed_clean_ci_source_required")
        self.report["git_sha"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        self.report["runner"]["docker"] = json.loads(self.command("docker-resources", ["docker", "info", "--format",
            '{"architecture":{{json .Architecture}},"cpus":{{json .NCPU}},"memory_bytes":{{json .MemTotal}}}'], output=True))
        self.command("static", [sys.executable, "-B", ROOT / "docker/ci/static.py"])
        self.command("compose-config", [*self.compose, "config", "--quiet"])
        self.command("build", [*self.compose, "build"], timeout=2400)
        self.ml_image = self.image(self.values["ATS_CI_ML_IMAGE"])
        write(self.root / "ml-image.json", self.ml_image)
        self.report["images"] = {name: self.image(self.values["ATS_CI_" + name.upper() + "_IMAGE"])
                                 for name in ("ml", "postgres", "minio", "mc")}
        self.command("clone", ["git", "clone", "--no-local", ROOT, self.root / "checkout"])
        source = self.root / "checkout"
        unit_log = self.command("unit", ["docker", "run", "--rm", "--network", "none", "--cpus", "1", "--memory", "1536m",
            "--mount", f"type=bind,source={source},target=/source,readonly", "--workdir", "/source",
            "-e", "PYTHONPATH=/source/src:/source", "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1",
            "--entrypoint", "/opt/ml-venv/bin/python", self.ml_image["id"], "-B", "-m", "unittest", "discover", "-s", "tests/unit"], timeout=300)
        match = re.search(r"Ran (\d+) tests?", unit_log.read_text())
        if not match:
            raise ValueError("unit_count_missing")
        self.report["unit_tests"] = int(match[1])
        self.report["checks"]["full_unit_boundary"] = True
        self.command("core-up", [*self.compose, "up", "-d", "--no-build", "--wait", "--wait-timeout", "240"], timeout=300)
        self.worker("bundle", dvc=True)
        self.report["checks"]["synthetic_gx_admission_dvc_roundtrip"] = read(self.root / "dvc-roundtrip.json")["passed"]
        self.worker("fit")
        workspace = self.root / "run/workspace"
        prepared = workspace / "artifacts/prepared"
        final = workspace / "artifacts/training/final"
        pipeline = workspace / "artifacts/evaluation/pipeline_manifest.json"
        record = read(final / "run_manifest.json")
        study = read(workspace / "artifacts/training/study/study.json")
        assert study["attempted_trials"] == study["effective_budget"] == 2 and study["configured_budget"] == 20
        self.report["checks"]["smoke_two_then_final_fit"] = True
        self.report["checks"]["unchanged_dvc_cache_hit"] = read(self.root / "cache.json")["outcome"] == "cache_hit"
        self.report["training"] = record["mlflow"]
        self.report["model_sha256"] = record["model"]["sha256"]
        self.report["bundle_sha256"] = read(self.root / "bundle/bundle.json")["sha256"]
        from ats_mlops.evaluation.baseline import validation_report
        baseline = validation_report(prepared / "data_manifest.json")
        assert baseline["test_read"] is False and baseline["metrics"]["status"] == "defined"
        from ats_mlops.quality.gate import run_gate
        request = {"validation_manifest": str(prepared / "data_manifest.json"), "admission_report": str(prepared / "admission.json"),
                   "dataset_binding": str(workspace / "dataset-binding.json"), "thresholds": str(source / "configs/quality.yaml"),
                   "candidate": {"run_manifest": str(final / "run_manifest.json"), "pipeline_manifest": str(pipeline), "model_dir": str(final / "model")},
                   "incumbent": None}
        gate = run_gate(request, self.root / "model-gate.json")
        assert gate["outcome"] == "passed", gate["reasons"]
        self.report["checks"]["offline_validation_only_model_gate"] = True
        serving_tag = self.project + "-serving:test"
        self.command("serving-build", ["docker", "build", "--build-arg", "BASE_IMAGE=" + self.values["ATS_CI_ML_IMAGE"],
             "--build-arg", "SOURCE_SHA=" + self.report["git_sha"], "-f", "docker/serving/Dockerfile", "-t", serving_tag, "."], timeout=300)
        serving_image = self.image(serving_tag)
        environment = json.loads(self.command("serving-environment", ["docker", "run", "--rm", "--network", "none",
            "--entrypoint", "/opt/ml-venv/bin/python", serving_image["id"], "-I", "-B", "-c",
            "import sys,json;sys.path.insert(0,'/opt/serving');from service.runtime import environment;print(json.dumps(environment()))"], output=True))
        from service.integrity import inventory
        serving_identity = {"git_sha": self.report["git_sha"], "source_sha256": inventory(ROOT / "service"),
                            "image": serving_image, "environment": environment}
        write(self.root / "serving-identity.json", serving_identity)
        self.report["images"]["serving"] = serving_image
        write(self.root / "package-request.json", {"release_id": self.project, "gate_report": str(self.root / "model-gate.json"),
            "run_manifest": str(final / "run_manifest.json"), "pipeline_manifest": str(pipeline),
            "serving_identity": str(self.root / "serving-identity.json")})
        self.worker("package")
        self.worker("alias")
        from service.release_check import stage
        check_request = {"package": str(self.root / "package"), **{key: request[key] for key in
                         ("validation_manifest", "admission_report", "dataset_binding")},
                         "alias_other_version": read(self.root / "alias-fixture.json")["model_version"],
                         "registry_transport": {"network": self.project + "_default", "tracking_uri": "http://mlflow:5000"}}
        checked = stage(check_request, self.root / "release-check.json")
        assert checked["outcome"] == "passed", {key: checked.get(key) for key in ("failed_step", "reasons", "error_type")}
        self.report["checks"]["actual_api_sqlite_privacy_isolation"] = all(checked["checks"].values())
        self.report["api_checks"] = checked["checks"]
        self.report["score_comparison"] = checked["score_comparison"]
        self.release(checked)
        self.report["checks"]["reserved_bytes_absent"] = all(not (workspace / "data" / name).exists()
            for name in ("test.jsonl", "source.jsonl", "corpus.jsonl", "split.json"))
        self.report["artifacts_sha256"] = {name: hashlib.sha256((self.root / name).read_bytes()).hexdigest()
            for name in ("model-gate.json", "release-check.json", "package/release_manifest.json", "repro.json", "cache.json")}
        self.report["outcome"] = "passed"

    def release(self, checked):
        from ats_mlops.release.controller import Controller, DEFAULT_CONFIG
        config = {**DEFAULT_CONFIG, "project": self.project + "-release", "port": self.ports[3],
                  "registry_uri": f"http://127.0.0.1:{self.ports[2]}", "alias": "ci_champion",
                  "events_volume": self.project + "-events", "network": self.project + "-release-net"}
        write(self.root / "release-config.json", config)
        controller = Controller(self.root / "release-state", config)
        controller.stage(self.root / "package", self.root / "release-check.json")
        controller.approve(self.project, "agent-operator")
        controller.activate(self.project)
        before = controller.store.read()["generation"]
        controller.activate(self.project)
        assert controller.store.read()["generation"] == before == 1
        assert controller.registry.get() == checked["candidate"]["model_version"]
        assert controller.runtime.observe()["ready"] is True
        self.report["checks"]["ephemeral_release_approve_activate_idempotence"] = True

    def cleanup(self):
        failures = []
        # Only the request's exact worker and owned release resources; never global prune.
        subprocess.run(["docker", "rm", "-f", self.project + "-worker"], capture_output=True, timeout=30)
        release_config = self.root / "release-config.json"
        if release_config.exists():
            from ats_mlops.release.runtime import DockerRuntime
            config = read(release_config)
            try:
                DockerRuntime(self.root / "release-state", config).stop()
                for kind, key in (("network", "network"), ("volume", "events_volume")):
                    result = subprocess.run(["docker", kind, "rm", config[key]], capture_output=True, timeout=30)
                    if result.returncode:
                        failures.append(kind)
            except Exception:
                failures.append("release")
        try:
            self.command("core-down", [*self.compose, "down", "--volumes", "--remove-orphans"], timeout=120)
        except Exception:
            failures.append("core")
        self.report["checks"]["scoped_cleanup"] = not failures
        return not failures


def cleanup_existing(directory):
    """Actions always-step fallback, restricted to this invocation's saved project."""
    directory = directory.resolve()
    control = directory / "control.json"
    if not control.exists():
        return
    project = read(control)["project"]
    if not re.fullmatch(r"ats-ci-[0-9a-f]{12}", project):
        raise ValueError("invalid_ci_cleanup_owner")
    subprocess.run(["docker", "rm", "-f", project + "-worker"], capture_output=True, timeout=30)
    config_path = directory / "release-config.json"
    if config_path.exists():
        from ats_mlops.release.runtime import DockerRuntime
        config = read(config_path)
        if (config["project"], config["events_volume"], config["network"]) != (project + "-release", project + "-events", project + "-release-net"):
            raise ValueError("invalid_ci_cleanup_owner")
        DockerRuntime(directory / "release-state", config).stop()
        for kind, key in (("network", "network"), ("volume", "events_volume")):
            subprocess.run(["docker", kind, "rm", config[key]], capture_output=True, timeout=30)
    subprocess.run(["docker", "compose", "--env-file", directory / "private/core.env", "-p", project,
                    "-f", ROOT / "docker/ci/compose.yaml", "--profile", "core", "down", "--volumes", "--remove-orphans"],
                   env={**os.environ, "ATS_CI_SOURCE": str(ROOT)}, capture_output=True, check=True, timeout=120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True, help="new private directory outside checkout")
    parser.add_argument("--public", type=Path, required=True, help="only compact allowlisted report is published")
    parser.add_argument("--cleanup", action="store_true")
    args = parser.parse_args()
    if args.cleanup:
        cleanup_existing(args.directory)
        return 0
    run = Lifecycle(args.directory, args.public)
    try:
        run.execute()
    except Exception as error:
        run.report["error_type"] = type(error).__name__
        if (run.root / "worker-failure.json").exists():
            run.report["worker_failure"] = read(run.root / "worker-failure.json")
        # Only enumerated command labels; exception payloads may contain request data/credentials.
        run.report["failed_step"] = str(error) if isinstance(error, RuntimeError) and re.fullmatch(r"[a-z-]+_failed", str(error)) else "lifecycle_contract"
        print(json.dumps({"outcome": "failed", "step": run.report["failed_step"], "type": type(error).__name__}), flush=True)
    finally:
        if not run.cleanup():
            run.report["outcome"] = "failed"
        write(run.public / "ci.json", run.report)
    return 0 if run.report["outcome"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
