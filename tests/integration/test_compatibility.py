"""Exercise the real T-2 stack, including a scheduled LocalExecutor task.

Run with ``ATS_RUN_COMPATIBILITY=1 python -m unittest discover -s
tests/integration -p test_compatibility.py``. A skipped test is not compatibility
evidence. This module uses only the host Python standard library; ML packages
belong to the container venvs.
"""

from __future__ import annotations

import base64
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import unittest
from urllib import error, parse, request
import uuid


ROOT = Path(__file__).resolve().parents[2]
DAG_ID = "compatibility_smoke"


class CompatibilityRunner:
    """Own one uniquely named Compose project and retain sanitized evidence."""

    def __init__(self, root: Path = ROOT) -> None:
        self.root = root.resolve()
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"{stamp}-{uuid.uuid4().hex[:10]}"
        self.project = f"ats-compat-{self.run_id.lower()}"
        self.state_dir = self.root / ".local" / "compat" / self.run_id
        self.state_dir.mkdir(parents=True, mode=0o700)
        self.state_dir.chmod(0o700)
        self.secret_values: list[str] = []
        self.env = os.environ.copy()
        self.token = ""
        self.started = time.monotonic()
        self.compose_attempted = False
        self.command_number = 0
        self.build_timeout = self._timeout("ATS_COMPAT_BUILD_TIMEOUT", 1800)
        self.service_timeout = self._timeout("ATS_COMPAT_SERVICE_TIMEOUT", 180)
        self.dag_timeout = self._timeout("ATS_COMPAT_DAG_TIMEOUT", 300)
        self.evidence: dict = {
            "run_id": self.run_id,
            "project": self.project,
            "status": "running",
            "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "checks": {name: "not_run" for name in (
                "builds", "services_startup", "airflow_api", "local_executor", "positive_dag",
                "positive_report", "serving_predictions", "serving_metadata",
                "serving_no_source_mount", "negative_dag", "lock_mismatch",
                "cleanup",
            )},
            "timings_seconds": {},
        }
        self._configure_environment()
        self.compose = [
            "docker", "compose", "--project-directory", str(self.root),
            "--env-file", str(self.state_dir / ".env"), "-p", self.project,
            "-f", str(self.root / "docker" / "compat" / "compose.yaml"),
        ]

    @staticmethod
    def _timeout(name: str, default: int) -> int:
        value = int(os.environ.get(name, default))
        if not 1 <= value <= 7200:
            raise ValueError(f"{name} must be between 1 and 7200 seconds")
        return value

    @staticmethod
    def _private_write(path: Path, content: str) -> None:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(content)

    def _configure_environment(self) -> None:
        values = {
            name: secrets.token_hex(24) for name in (
                "POSTGRES_PASSWORD", "MLFLOW_DB_PASSWORD", "AIRFLOW_DB_PASSWORD",
                "MINIO_ROOT_PASSWORD", "AIRFLOW_ADMIN_PASSWORD",
            )
        }
        values["MINIO_ROOT_USER"] = "atscompatadmin"
        values["AIRFLOW_JWT_SECRET"] = secrets.token_hex(32)
        values["AIRFLOW_FERNET_KEY"] = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")
        self.secret_values.extend(values.values())
        auth_file = self.state_dir / "airflow-passwords.json"
        self._private_write(auth_file, json.dumps({"compat": values["AIRFLOW_ADMIN_PASSWORD"]}))
        values.update({
            "ATS_SOURCE_REPO": str(self.root),
            "ATS_COMPAT_STATE_DIR": str(self.state_dir),
            "ATS_COMPAT_AUTH_FILE": str(auth_file),
            "ATS_COMPAT_RUN_ID": self.run_id,
        })
        # Single quotes preserve spaces and prevent Compose dotenv interpolation.
        self._private_write(
            self.state_dir / ".env",
            "".join(f"{key}='{value.replace(chr(39), chr(92) + chr(39))}'\n" for key, value in values.items()),
        )
        self.env.update(values)

    def redact(self, value: str) -> str:
        for secret in sorted(self.secret_values, key=len, reverse=True):
            value = value.replace(secret, "<redacted>")
        value = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._~+/=-]+", r"\1<redacted>", value)
        value = re.sub(r"(?i)([\"']?(?:access_token|password|secret)[\"']?\s*[:=]\s*)[\"']?[^\s,\"'}]+", r"\1<redacted>", value)
        return value

    def write_json(self, name: str, value: object) -> None:
        (self.state_dir / name).write_text(
            self.redact(json.dumps(value, indent=2, sort_keys=True)) + "\n", encoding="utf-8",
        )

    def command(self, args: list[str], *, timeout: int = 60, check: bool = True) -> str:
        self.command_number += 1
        label = f"command-{self.command_number:03d}.log"
        started = time.monotonic()
        try:
            result = subprocess.run(
                args, cwd=self.root, env=self.env, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout,
                check=False,
            )
            output = self.redact(result.stdout)
            code = result.returncode
        except subprocess.TimeoutExpired as exc:
            raw = exc.stdout or ""
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            output = self.redact(raw) + f"\nCommand timed out after {timeout}s."
            code = -1
        (self.state_dir / label).write_text(
            self.redact("$ " + " ".join(args)) + "\n" + output, encoding="utf-8",
        )
        self.evidence.setdefault("commands", []).append({
            "args": args, "returncode": code, "log": label,
            "seconds": round(time.monotonic() - started, 3),
        })
        if check and code != 0:
            raise AssertionError(f"Command failed ({code}); {label}:\n{output[-6000:]}")
        return output.strip()

    def dc(self, *args: str, timeout: int = 60, check: bool = True) -> str:
        return self.command(self.compose + list(args), timeout=timeout, check=check)

    def http(self, base: str, path: str, *, method: str = "GET", body: object = None,
             authenticated: bool = False) -> dict | list:
        headers = {"Content-Type": "application/json"}
        if authenticated:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = None if body is None else json.dumps(body).encode("utf-8")
        req = request.Request(base + path, data=payload, headers=headers, method=method)
        try:
            with request.urlopen(req, timeout=10) as response:
                raw = response.read().decode("utf-8")
        except error.HTTPError as exc:
            detail = self.redact(exc.read().decode("utf-8", errors="replace"))
            raise RuntimeError(f"HTTP {exc.code} {method} {path}: {detail[:1500]}") from None
        return json.loads(raw) if raw else {}

    def wait_for(self, description: str, operation, timeout: int):
        deadline = time.monotonic() + timeout
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                result = operation()
                if result:
                    return result
            except (OSError, RuntimeError, ValueError) as exc:
                last_error = self.redact(str(exc))
            time.sleep(min(2, max(0, deadline - time.monotonic())))
        raise AssertionError(f"Timed out waiting for {description} after {timeout}s: {last_error}")

    def base_url(self, service: str, port: int) -> str:
        address = self.dc("port", service, str(port)).splitlines()[0]
        host, host_port = address.rsplit(":", 1)
        if host != "127.0.0.1":
            raise AssertionError(f"{service} port must bind to loopback, got {address}")
        return f"http://127.0.0.1:{int(host_port)}"

    def authenticate(self, base: str) -> bool:
        response = self.http(base, "/auth/token", method="POST", body={
            "username": "compat", "password": self.env["AIRFLOW_ADMIN_PASSWORD"],
        })
        self.token = response["access_token"]
        self.secret_values.append(self.token)
        return True

    def dag_run(self, base: str, run_id: str, conf: dict) -> dict:
        path = f"/api/v2/dags/{DAG_ID}/dagRuns"
        self.http(base, path, method="POST", authenticated=True, body={
            "dag_run_id": run_id,
            "logical_date": dt.datetime.now(dt.timezone.utc).isoformat(),
            "conf": conf,
        })
        states: list[dict] = []

        def terminal_state():
            response = self.http(base, f"{path}/{parse.quote(run_id, safe='')}", authenticated=True)
            state = response.get("state")
            if not states or states[-1]["state"] != state:
                states.append({"state": state, "at": dt.datetime.now(dt.timezone.utc).isoformat()})
            return response if state in ("success", "failed") else None

        try:
            return self.wait_for(f"DAG run {run_id}", terminal_state, self.dag_timeout)
        finally:
            self.evidence.setdefault("dag_states", {})[run_id] = states

    def read_report(self, run_id: str, name: str) -> dict:
        output = self.dc("exec", "-T", "airflow", "cat", f"/work/reports/{run_id}/{name}")
        result = json.loads(output)
        self.write_json(f"{run_id}-{name}", result)
        return result

    def require(self, condition: bool, description: str) -> None:
        if not condition:
            raise AssertionError(description)

    def check_report(self, report: dict, git_sha: str, lock_sha: str) -> None:
        self.require(report.get("run_id") == self.run_id, "Report belongs to a different run")
        self.require(report.get("fixture_only") is True and report.get("training_executed") is True, "Positive report must describe executed fixture training")
        for name in ("gx_valid", "gx_invalid_rejected", "dvc_roundtrip", "mlflow_registered", "bento_import"):
            self.require(report.get("checks", {}).get(name) is True, f"Report check {name} must be true")
        self.require(report.get("git_sha") == git_sha, "Report Git SHA differs from committed host HEAD")
        self.require(report.get("lock_sha256") == lock_sha, "Report ML lock hash differs from host checkout")
        manifest = report["manifest"]
        self.require(manifest["git_sha"] == git_sha, "Manifest Git SHA differs from requested HEAD")
        self.require(manifest["lock_sha256"] == lock_sha, "Manifest lock hash differs from host checkout")
        self.require(manifest["python_executable"] == "/opt/ml-venv/bin/python", "ML CLI did not use /opt/ml-venv/bin/python")
        self.require(manifest["checkout_path"] == f"/work/{self.run_id}/checkout", "ML CLI did not use the isolated run checkout")

    def capture_metadata(self) -> None:
        self.evidence["docker_version"] = self.command(["docker", "version", "--format", "{{json .Server}}"], check=False)
        self.evidence["docker_resources"] = self.command([
            "docker", "info", "--format", '{{json .NCPU}} CPUs; {{json .MemTotal}} bytes; {{.OSType}}/{{.Architecture}}',
        ], check=False)
        self.evidence["containers"] = self.dc("ps", "--all", "--format", "json", check=False)
        image_ids = self.dc("images", "-q", check=False).splitlines()
        if image_ids:
            self.evidence["images"] = self.command([
                "docker", "image", "inspect", "--format",
                '{{json .Id}} {{json .Os}}/{{json .Architecture}} {{json .RepoTags}}',
                *sorted(set(image_ids)),
            ], check=False)
        container_ids = self.dc("ps", "-q", check=False).splitlines()
        if container_ids:
            self.evidence["resources"] = self.command([
                "docker", "stats", "--no-stream", "--format", "{{json .}}", *container_ids,
            ], timeout=30, check=False)

    def run(self) -> None:
        checks = self.evidence["checks"]
        active_check = "builds"
        try:
            git_sha = self.command(["git", "rev-parse", "HEAD"])
            self.require(re.fullmatch(r"[0-9a-f]{40}", git_sha) is not None, "HEAD must be a full Git SHA")
            lock_sha = hashlib.sha256((self.root / "requirements-ml.lock").read_bytes()).hexdigest()
            self.evidence.update({"git_sha": git_sha, "lock_sha256": lock_sha})
            self.compose_attempted = True
            self.dc("config", "--quiet")
            # serving and auth-init share the MLflow image built by this step.
            for service in ("postgres", "minio", "mlflow", "airflow"):
                self.dc("build", service, timeout=self.build_timeout)
            checks[active_check] = "passed"
            active_check = "services_startup"
            self.dc("up", "-d", "postgres", "minio", "mlflow", "airflow", timeout=self.service_timeout)
            checks[active_check] = "passed"
            active_check = "airflow_api"
            airflow = self.base_url("airflow", 8080)
            self.wait_for("Airflow API authentication", lambda: self.authenticate(airflow), self.service_timeout)
            dag_path = f"/api/v2/dags/{DAG_ID}"
            self.wait_for("parsed compatibility DAG", lambda: self.http(airflow, dag_path, authenticated=True), self.service_timeout)
            self.http(airflow, dag_path, method="PATCH", body={"is_paused": False}, authenticated=True)
            checks[active_check] = "passed"
            active_check = "local_executor"
            self.require(self.dc("exec", "-T", "airflow", "airflow", "config", "get-value", "core", "executor").splitlines()[-1] == "LocalExecutor", "Airflow must use LocalExecutor")
            checks[active_check] = "passed"
            active_check = "positive_dag"
            positive = self.dag_run(airflow, self.run_id, {"run_id": self.run_id, "git_sha": git_sha})
            self.write_json("positive-dag.json", positive)
            self.require(positive["state"] == "success", "Positive DAG failed; inspect services.log and task report")
            checks[active_check] = "passed"
            active_check = "positive_report"
            report = self.read_report(self.run_id, "report.json")
            self.check_report(report, git_sha, lock_sha)
            checks[active_check] = "passed"
            self.dc("up", "-d", "serving", timeout=self.service_timeout)
            serving = self.base_url("serving", 3000)
            active_check = "serving_predictions"
            prediction = self.wait_for("BentoML prediction API", lambda: self.http(
                serving, "/predict", method="POST", body={"values": report["probe_values"]},
            ), self.service_timeout)
            actual = prediction
            self.require(isinstance(actual, list), "Serving must return a JSON array")
            expected = report["expected_predictions"]
            self.require(len(actual) == len(expected) and len(expected) > 0, "Serving returned an unexpected prediction count")
            self.require(all(math.isclose(float(a), float(b), rel_tol=1e-5, abs_tol=1e-6) for a, b in zip(actual, expected)), "Serving predictions differ from the ML CLI probe")
            self.write_json("serving-predictions.json", prediction)
            checks[active_check] = "passed"
            active_check = "serving_metadata"
            metadata = self.http(serving, "/metadata", method="POST", body={})
            self.require(metadata["bento_tag"] == report["bento_tag"], "Serving loaded a different Bento model")
            self.write_json("serving-metadata.json", metadata)
            checks[active_check] = "passed"
            active_check = "serving_no_source_mount"
            serving_id = self.dc("ps", "-q", "serving")
            mounts = json.loads(self.command(["docker", "inspect", "--format", "{{json .Mounts}}", serving_id]))
            self.require(bool(mounts), "Serving must receive its artifact volume")
            for mount in mounts:
                self.require(mount["Type"] != "bind", "Serving has an unexpected host bind mount")
                self.require(str(self.root) not in mount.get("Source", ""), "Serving mounts the source repository")
            self.write_json("serving-mounts.json", mounts)
            checks[active_check] = "passed"
            active_check = "negative_dag"
            negative_id = f"{self.run_id}-bad-lock"
            negative = self.dag_run(airflow, negative_id, {
                "run_id": negative_id, "git_sha": git_sha, "expected_lock_sha256": "0" * 64,
            })
            self.write_json("negative-dag.json", negative)
            self.require(negative["state"] == "failed", "DAG accepted a mismatching lock hash")
            checks[active_check] = "passed"
            active_check = "lock_mismatch"
            failure = self.read_report(negative_id, "error.json")
            self.require(failure["kind"] == "lock_mismatch", "Negative DAG failed for a reason other than lock mismatch")
            self.require(failure["run_id"] == negative_id, "Lock rejection belongs to a different run")
            self.dc("exec", "-T", "airflow", "test", "!", "-e", f"/work/reports/{negative_id}/report.json")
            checks[active_check] = "passed"
            self.evidence["status"] = "passed"
        except BaseException as exc:
            checks[active_check] = "failed"
            self.evidence["status"] = "failed"
            self.evidence["error"] = self.redact(f"{type(exc).__name__}: {exc}")
            raise
        finally:
            if self.compose_attempted:
                try:
                    self.capture_metadata()
                    logs = self.dc("logs", "--no-color", "--tail", "1200", timeout=30, check=False)
                    (self.state_dir / "services.log").write_text(logs + "\n", encoding="utf-8")
                    if self.evidence["status"] == "failed":
                        errors = [line for line in logs.splitlines() if any(
                            word in line for word in (
                                "Error:", "Exception:", "Traceback", "[error",
                                "Permission denied", "Read-only file system",
                            )
                        )]
                        relevant = errors[-30:] if errors else logs.splitlines()[-15:]
                        print("Service diagnostics (sanitized):\n" + "\n".join(relevant), flush=True)
                except Exception as exc:
                    self.evidence["diagnostics_error"] = self.redact(str(exc))
                try:
                    self.dc("down", "--volumes", "--remove-orphans", timeout=120)
                    checks["cleanup"] = "passed"
                except Exception as exc:
                    checks["cleanup"] = "failed"
                    self.evidence["cleanup_error"] = self.redact(str(exc))
                    self.evidence["status"] = "failed"
            self.evidence["timings_seconds"]["total"] = round(time.monotonic() - self.started, 3)
            self.evidence["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
            self.write_json("evidence.json", self.evidence)
            print(f"Compatibility evidence ({self.evidence['status']}): {self.state_dir / 'evidence.json'}", flush=True)
        self.require(checks["cleanup"] == "passed", "Temporary Compose project cleanup failed; see evidence.json")


@unittest.skipUnless(
    os.environ.get("ATS_RUN_COMPATIBILITY") == "1",
    "real Docker compatibility not run: set ATS_RUN_COMPATIBILITY=1; a skip is not a pass",
)
class CompatibilityIntegrationTest(unittest.TestCase):
    def test_real_localexecutor_mlflow_bento_roundtrip(self) -> None:
        CompatibilityRunner().run()


if __name__ == "__main__":
    unittest.main(verbosity=2)
