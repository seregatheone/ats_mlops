#!/usr/bin/env python3
"""Operate only Airflow on the already healthy ats-core project."""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import stat
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def private(path, value):
    if path.is_symlink():
        raise ValueError("symlink_rejected")
    if path.exists():
        if not stat.S_ISREG(path.stat().st_mode) or path.stat().st_mode & 0o077:
            raise ValueError("private_regular_file_required")
    else:
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
            stream.write(value)


def command(args, *, env=None, capture=False):
    return subprocess.run(args, env=env, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None,
                          stderr=subprocess.PIPE if capture else None)


def configure(state, core, port, repository=None):
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    if state.is_symlink():
        raise ValueError("symlink_rejected")
    state.chmod(0o700)
    private(core, "")
    values = dict(line.split("=", 1) for line in core.read_text().splitlines() if line)
    if values.get("COMPOSE_PROJECT_NAME") != "ats-core":
        raise ValueError("existing_ats_core_required")
    private(state / "airflow.env", "AIRFLOW_FERNET_KEY=" + base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
            + "\nAIRFLOW_JWT_SECRET=" + secrets.token_hex(32) + "\n")
    private(state / "passwords.json", json.dumps({"operator": secrets.token_urlsafe(32)}))
    private(state / "faults.json", "{}\n")
    private(state / "image.json", "{}\n")
    values.update(dict(line.split("=", 1) for line in (state / "airflow.env").read_text().splitlines() if line))
    values.update({"ATS_ORCHESTRATION_GIT": str(state / "source.git"), "ATS_ORCHESTRATION_IMAGE_FILE": str(state / "image.json"),
                   "ATS_ORCHESTRATION_PASSWORD_FILE": str(state / "passwords.json"), "ATS_ORCHESTRATION_FAULT_FILE": str(state / "faults.json"),
                   "AIRFLOW_PORT": str(port)})
    if not (state / "source.git").exists():
        # A bare clone has no ignored host files, working tree, hooks or credentials.
        source = str(repository) if repository else "https://github.com/seregatheone/ats_mlops.git"
        command(["git", "clone", "--bare", "--no-local", source, str(state / "source.git")], capture=True)
        command(["git", "--git-dir", str(state / "source.git"), "config", "--unset-all", "remote.origin.url"], capture=True)
    return values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("init", "refresh-source", "config", "build", "up", "stop", "status"))
    parser.add_argument("--state-dir", type=Path, default=ROOT / ".local/orchestration")
    parser.add_argument("--core-env", type=Path, default=ROOT / ".local/core/core.env")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--sanitized-git-source", type=Path, help="explicit local Git source; only a new bare clone is mounted")
    args = parser.parse_args()
    state = args.state_dir.absolute()
    try:
        if not args.core_env.is_file():
            raise ValueError("existing_core_credentials_required")
        values = configure(state, args.core_env, args.port, args.sanitized_git_source)
        env = {**os.environ, **values}
        base = ["docker", "compose", "--project-directory", str(ROOT), "--env-file", str(args.core_env),
                "-p", "ats-core", "-f", str(ROOT / "compose.yaml"), "--profile", "orchestration"]
        if args.action == "init":
            print("Private orchestration configuration ready")
        elif args.action == "refresh-source":
            source = str(args.sanitized_git_source) if args.sanitized_git_source else "https://github.com/seregatheone/ats_mlops.git"
            command(["git", "--git-dir", str(state / "source.git"), "fetch", "--no-tags", source,
                     "+refs/heads/*:refs/heads/*"], capture=True)
            print("Sanitized source objects refreshed")
        elif args.action == "up":
            command([*base, "run", "--rm", "--no-deps", "orchestration-init"], env=env, capture=True)
            command([*base, "up", "-d", "--no-deps", "--wait", "--wait-timeout", "240", "airflow"], env=env, capture=True)
            print("Airflow ready on loopback")
        elif args.action == "build":
            command([*base, "build", "airflow"], env=env, capture=True)
            identity = json.loads(command(["docker", "image", "inspect", "--format",
                '{"Id":{{json .Id}},"RepoDigests":{{json .RepoDigests}},"Architecture":{{json .Architecture}},"Os":{{json .Os}}}',
                "ats-core-airflow:t10"], capture=True).stdout)
            (state / "image.json").write_text(json.dumps({"id": identity["Id"], "reference": "ats-core-airflow:t10",
                    "repo_digests": identity["RepoDigests"], "architecture": identity["Architecture"], "os": identity["Os"]}))
            print("Pinned orchestration image built")
        else:
            operations = {"config": ["config", "--quiet"], "stop": ["stop", "airflow"], "status": ["ps", "airflow"]}
            command([*base, *operations[args.action]], env=env)
        return 0
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        if isinstance(error, subprocess.CalledProcessError):
            diagnostic = state / ("operation-error-" + secrets.token_hex(4) + ".log")
            private(diagnostic, "")
            diagnostic.write_text((error.stdout or "") + (error.stderr or ""))
        print("Orchestration operation failed; inspect private local evidence", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
