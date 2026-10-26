#!/usr/bin/env python3
"""Manage only the explicitly selected local persistent Compose project."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import stat
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SECRET_KEYS = ("MINIO_ROOT_PASSWORD", "DVC_SECRET_ACCESS_KEY", "MLFLOW_SECRET_ACCESS_KEY",
               "POSTGRES_PASSWORD", "MLFLOW_DB_PASSWORD", "AIRFLOW_DB_PASSWORD")


def private_write(path: Path, content: str) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
        stream.write(content)


def ensure_private(path: Path, content: str) -> None:
    if path.is_symlink():
        raise ValueError("private configuration cannot be a symlink")
    if path.exists():
        if not stat.S_ISREG(path.stat().st_mode) or path.stat().st_mode & 0o077:
            raise ValueError("existing configuration must be a private regular file")
        if path.read_text() != content:
            raise ValueError("derived configuration differs from saved credentials")
    else:
        private_write(path, content)


def initialize(state_dir: Path, project: str, ports: tuple[int, int, int]) -> dict[str, str]:
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,62}", project):
        raise ValueError("invalid Compose project name")
    if len(set(ports)) != 3 or any(not 1 <= port <= 65535 for port in ports):
        raise ValueError("three distinct valid local ports are required")
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    if state_dir.is_symlink():
        raise ValueError("state directory cannot be a symlink")
    os.chmod(state_dir, 0o700)
    path = state_dir / "core.env"
    if path.is_symlink():
        raise ValueError("credentials cannot be a symlink")
    if path.exists():
        if not stat.S_ISREG(path.stat().st_mode) or path.stat().st_mode & 0o077:
            raise ValueError("existing credentials must be a private regular file")
        values = dict(line.split("=", 1) for line in path.read_text().splitlines() if line)
        if values.get("COMPOSE_PROJECT_NAME") != project:
            raise ValueError("existing state belongs to another project")
        if not set((*SECRET_KEYS, "DVC_ACCESS_KEY_ID", "MLFLOW_ACCESS_KEY_ID", "MINIO_ROOT_USER",
                    "MINIO_API_PORT", "MINIO_CONSOLE_PORT", "MLFLOW_PORT")) <= values.keys():
            raise ValueError("incomplete existing credentials")
    else:
        values = {"COMPOSE_PROJECT_NAME": project, "MINIO_API_PORT": str(ports[0]),
                  "MINIO_CONSOLE_PORT": str(ports[1]), "MLFLOW_PORT": str(ports[2]),
                  "MINIO_ROOT_USER": "root" + secrets.token_hex(8),
                  "DVC_ACCESS_KEY_ID": "dvc" + secrets.token_hex(8),
                  "MLFLOW_ACCESS_KEY_ID": "mlf" + secrets.token_hex(8)}
        values.update({key: secrets.token_hex(24) for key in SECRET_KEYS})
        private_write(path, "".join(f"{key}={value}\n" for key, value in values.items()))
    ensure_private(state_dir / "dvc.env", "".join((
        f"AWS_ACCESS_KEY_ID={values['DVC_ACCESS_KEY_ID']}\n",
        f"AWS_SECRET_ACCESS_KEY={values['DVC_SECRET_ACCESS_KEY']}\n",
        "AWS_DEFAULT_REGION=us-east-1\n")))
    ensure_private(state_dir / "mlflow.env", f"MLFLOW_TRACKING_URI=http://127.0.0.1:{values['MLFLOW_PORT']}\n")
    ensure_private(state_dir / "client.json", json.dumps({
        "dvc_remote": "s3://ats-dvc/data", "s3_endpoint_host": f"http://127.0.0.1:{values['MINIO_API_PORT']}",
        "s3_endpoint_container": "http://minio:9000", "mlflow_uri_host": f"http://127.0.0.1:{values['MLFLOW_PORT']}",
        "mlflow_uri_container": "http://mlflow:5000"}, indent=2) + "\n")
    return values


def compose_command(state_dir: Path, project: str, compose_file: Path = ROOT / "compose.yaml") -> list[str]:
    return ["docker", "compose", "--project-directory", str(ROOT), "--env-file",
            str(state_dir / "core.env"), "-p", project, "-f", str(compose_file.resolve()), "--profile", "core"]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "config", "up", "restart", "down", "status"))
    parser.add_argument("--state-dir", type=Path, default=ROOT / ".local/core")
    parser.add_argument("--project", default="ats-core")
    parser.add_argument("--compose-file", type=Path, default=ROOT / "compose.yaml")
    parser.add_argument("--minio-port", type=int, default=9000)
    parser.add_argument("--console-port", type=int, default=9001)
    parser.add_argument("--mlflow-port", type=int, default=5050)
    args = parser.parse_args()
    state = args.state_dir.absolute()
    try:
        values = initialize(state, args.project, (args.minio_port, args.console_port, args.mlflow_port))
        if args.action == "init":
            print(f"Private core configuration ready: {state}")
            return 0
        operations = {"config": ["config", "--quiet"], "up": ["up", "--build", "--wait", "--wait-timeout", "180"],
                      "restart": ["restart", "postgres", "minio", "mlflow"], "down": ["down", "--remove-orphans"],
                      "status": ["ps"]}
        return subprocess.run(compose_command(state, args.project, args.compose_file) + operations[args.action],
                              env=os.environ | values, check=False).returncode
    except (OSError, ValueError) as exc:
        print(f"Core configuration failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
