"""Run a locked ML environment against an isolated, immutable source revision.

This module deliberately uses only the standard library: Airflow's environment
must never import the ML dependencies installed in the separate ML virtualenv.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any


class CompatibilityError(RuntimeError):
    """A failure with a safe, machine-readable category."""

    def __init__(self, kind: str):
        super().__init__(kind)
        self.kind = kind


def validate_run_id(run_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", run_id):
        raise CompatibilityError("invalid_run_id")
    return run_id


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _command(command: list[str], *, cwd: Path | None = None,
             env: dict[str, str] | None = None) -> None:
    # Tool errors can contain endpoint URLs or credentials; never relay output.
    subprocess.run(command, cwd=cwd, env=env, check=True,
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def run_compatibility(git_sha: str, run_id: str,
                      expected_lock_sha256: str | None = None) -> Path:
    """Clone a revision, verify its environment, and execute its own worker."""
    validate_run_id(run_id)
    work_root = Path(os.environ.get("ATS_WORK_ROOT", "/work")).resolve()
    report_dir = work_root / "reports" / run_id
    report_path = report_dir / "report.json"
    try:
        if not re.fullmatch(r"[a-fA-F0-9]{40}", git_sha):
            raise CompatibilityError("invalid_git_sha")
        git_sha = git_sha.lower()
        if expected_lock_sha256 is not None:
            if not re.fullmatch(r"[a-fA-F0-9]{64}", expected_lock_sha256):
                raise CompatibilityError("invalid_lock_sha256")
            expected_lock_sha256 = expected_lock_sha256.lower()
        # A run identifier is single-use, so a successful cached model or a
        # previous report cannot be mistaken for a fresh training execution.
        run_root = work_root / run_id
        run_root.mkdir(parents=True, exist_ok=False)
        checkout = run_root / "checkout"
        source = Path(os.environ.get("ATS_SOURCE_REPO", "/source")).resolve()
        ml_python = Path(os.environ.get("ATS_ML_PYTHON", "/opt/ml-venv/bin/python"))
        if not ml_python.is_absolute():
            raise CompatibilityError("invalid_ml_python")
        image_lock = Path(os.environ.get("ATS_ML_LOCK_FILE", "/opt/requirements-ml.lock"))
        _command(["git", "-c", f"safe.directory={source}",
                  "-c", f"safe.directory={source / '.git'}",
                  "clone", "--no-local", "--no-checkout", "--",
                  str(source), str(checkout)])
        _command(["git", "checkout", "--detach", git_sha], cwd=checkout)
        actual_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=checkout, check=True,
            capture_output=True, text=True,
        ).stdout.strip()
        if actual_sha != git_sha:
            raise CompatibilityError("checkout_mismatch")
        lock_sha256 = file_sha256(checkout / "requirements-ml.lock")
        if (lock_sha256 != file_sha256(image_lock)
                or (expected_lock_sha256 is not None
                    and lock_sha256 != expected_lock_sha256)):
            raise CompatibilityError("lock_mismatch")
        env = os.environ.copy()
        env["PYTHONPATH"] = str(checkout / "src")
        env["PATH"] = str(ml_python.parent) + os.pathsep + os.defpath
        for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
            env[variable] = "1"
        # Git and lock identity are recomputed inside the worker as well.
        _command([str(ml_python), "-m", "ats_mlops", "compatibility-worker",
                  "--run-id", run_id, "--report-path", str(report_path)],
                 cwd=checkout, env=env)
        report = json.loads(report_path.read_text())
        required_checks = {"gx_valid", "gx_invalid_rejected", "dvc_roundtrip",
                           "mlflow_registered", "bento_import"}
        checks = report.get("checks", {})
        if (report.get("git_sha") != git_sha
                or report.get("lock_sha256") != lock_sha256
                or report.get("run_id") != run_id
                or not all(checks.get(check) is True for check in required_checks)):
            raise CompatibilityError("invalid_worker_report")
        return report_path
    except Exception as exc:
        kind = exc.kind if isinstance(exc, CompatibilityError) else "runner_failed"
        # No exception strings or subprocess output: both may include secrets.
        write_json(report_dir / "error.json", {"kind": kind, "run_id": run_id})
        raise CompatibilityError(kind) from None
