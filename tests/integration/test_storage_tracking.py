"""Opt-in T-4 check: real DVC/SQL/Registry persistence and denied operations."""
from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("bootstrap_core", ROOT / "scripts/bootstrap_core.py")
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


def available_ports() -> tuple[int, int, int]:
    sockets = [socket.socket() for _ in range(3)]
    try:
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return tuple(listener.getsockname()[1] for listener in sockets)
    finally:
        for listener in sockets:
            listener.close()


class StorageRunner:
    def __init__(self):
        self.run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + secrets.token_hex(4)
        self.project = "ats-core-test-" + self.run_id.lower()
        self.directory = ROOT / ".local/core-tests" / self.run_id
        self.values = bootstrap.initialize(self.directory, self.project, available_ports())
        self.compose = bootstrap.compose_command(self.directory, self.project)
        self.log_number = 0
        self.report = {"run_id": self.run_id, "project": self.project, "fixture_only": True,
                       "evidence_scope": "demo_only", "checks": {}, "timings_seconds": {}}

    def command(self, args: list[str], *, timeout: int = 60, check: bool = True) -> str:
        self.log_number += 1
        result = subprocess.run(args, cwd=ROOT, env=os.environ | self.values,
                                capture_output=True, text=True, timeout=timeout, check=False)
        output = result.stdout + result.stderr
        for key in bootstrap.SECRET_KEYS:
            output = output.replace(self.values[key], "<redacted>")
        (self.directory / f"command-{self.log_number:03d}.log").write_text(output)
        if check and result.returncode:
            raise AssertionError(f"Core command {self.log_number} failed ({result.returncode}); see sanitized log")
        return output

    def phase(self, name: str) -> dict:
        output = self.command(self.compose + ["run", "--rm", "--no-deps", "storage-check",
            "/opt/ml-venv/bin/python", "/opt/core/check_storage.py", name], timeout=180)
        lines = [line for line in output.splitlines() if line.startswith("ATS_CORE_RESULT=")]
        if len(lines) != 1:
            raise AssertionError("missing exact worker evidence")
        result = json.loads(lines[0].split("=", 1)[1])
        self.report["checks"][name] = result["checks"]
        return result["checks"]

    def assert_network_boundaries(self):
        ids = self.command(self.compose + ["ps", "-aq"]).splitlines()
        info = json.loads(self.command(["docker", "inspect", *ids]))
        summary = {}
        for container in info:
            service = container["Config"]["Labels"]["com.docker.compose.service"]
            bindings = container["HostConfig"]["PortBindings"] or {}
            if service == "postgres":
                assert not bindings, "PostgreSQL must not publish a host port"
            for entries in bindings.values():
                assert entries and all(item["HostIp"] == "127.0.0.1" for item in entries)
            summary[service] = {"host_bindings": bindings,
                                "image_id": container["Image"]}
        self.report["network"] = summary
        self.report["checks"]["loopback_only_no_database_host_port"] = True

    def save(self):
        (self.directory / "evidence.json").write_text(json.dumps(self.report, indent=2, sort_keys=True) + "\n")

    def run(self):
        started = time.monotonic()
        try:
            self.report["git_sha"] = self.command(["git", "rev-parse", "HEAD"]).strip()
            self.report["dirty_worktree"] = bool(self.command(["git", "status", "--porcelain"]).strip())
            self.command(self.compose + ["config", "--quiet"])
            self.command(self.compose + ["--profile", "check", "build"], timeout=900)
            self.report["checks"]["build_and_config"] = True
            self.command(self.compose + ["up", "-d", "--wait", "--wait-timeout", "180"], timeout=240)
            self.assert_network_boundaries()
            self.phase("seed")
            self.phase("security")
            self.command(self.compose + ["restart", "postgres", "minio", "mlflow"], timeout=90)
            self.command(self.compose + ["up", "-d", "--wait", "--wait-timeout", "180"], timeout=240)
            self.phase("verify")
            self.report["checks"]["restart_retained"] = True
            # Recreate containers with the same named volumes as a stronger retention check.
            self.command(self.compose + ["down", "--remove-orphans"], timeout=90)
            self.command(self.compose + ["up", "-d", "--wait", "--wait-timeout", "180"], timeout=240)
            self.phase("verify")
            self.phase("security")
            self.report["checks"]["down_up_retained"] = True
            ids = self.command(self.compose + ["ps", "-q"]).splitlines()
            self.report["resource_sample"] = self.command(["docker", "stats", "--no-stream", "--format", "{{json .}}", *ids]).splitlines()
            self.report["outcome"] = "passed"
        except Exception as exc:
            self.report["outcome"] = "failed"
            self.report["error_type"] = type(exc).__name__
            self.command(self.compose + ["logs", "--no-color", "--tail", "100"], check=False)
            raise
        finally:
            self.command(self.compose + ["--profile", "check", "down", "--volumes", "--remove-orphans"],
                         timeout=90, check=False)
            remaining = {}
            for kind in ("container", "volume", "network"):
                remaining[kind] = self.command(["docker", kind, "ls", "-q", "--filter", f"label=com.docker.compose.project={self.project}"]).splitlines()
            self.report["remaining_resources"] = remaining
            self.report["checks"]["cleanup"] = not any(remaining.values())
            if any(remaining.values()):
                self.report["outcome"] = "failed"
                self.report["error_type"] = "CleanupError"
            self.report["timings_seconds"]["total"] = round(time.monotonic() - started, 3)
            self.save()
            if any(remaining.values()):
                raise AssertionError("isolated core test cleanup incomplete")
            print(f"Core evidence: {self.directory / 'evidence.json'}")


@unittest.skipUnless(os.environ.get("ATS_RUN_STORAGE_TRACKING") == "1",
                     "real core Docker integration not run; set ATS_RUN_STORAGE_TRACKING=1; skip is not a pass")
class StorageTrackingIntegrationTest(unittest.TestCase):
    def test_persistence_and_scoped_credentials(self):
        StorageRunner().run()


if __name__ == "__main__":
    unittest.main(verbosity=2)
