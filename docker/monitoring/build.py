"""Bound the dependency installation to one ephemeral container, not a daemon service."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[2]
PYTHON = "python:3.11.17-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dependencies-only", action="store_true")
    parser.add_argument("--dependency-base", default=PYTHON, help="optional previous bounded dependency image for a narrow lock repair")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    name = "ats-monitoring-build-" + uuid.uuid4().hex[:12]
    lock = ROOT / "requirements-monitoring.lock"
    digest = hashlib.sha256(lock.read_bytes()).hexdigest()
    base_tag = "ats-monitoring-deps:" + digest[:16]
    with (args.output / "build.log").open("w") as log:
        def run(*command, capture=False):
            completed = subprocess.run(command, cwd=ROOT, check=True, text=True,
                                       stdout=subprocess.PIPE if capture else log, stderr=log)
            return completed.stdout.strip() if capture else None
        try:
            run("docker", "create", "--name", name, "--cpus", "1", "--memory", "1536m", "-e", "DO_NOT_TRACK=1",
                "--entrypoint", "/bin/sh", args.dependency_base, "-ec",
                "python -m venv /opt/monitoring-venv; /opt/monitoring-venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /opt/requirements-monitoring.lock")
            run("docker", "cp", str(lock), name + ":/opt/requirements-monitoring.lock")
            run("docker", "start", "-a", name)
            state = json.loads(run("docker", "inspect", "--format", "{{json .State}}", name, capture=True))
            if state["ExitCode"] != 0 or state["OOMKilled"]:
                raise RuntimeError("bounded_dependency_install_failed")
            run("docker", "commit", "--change", "LABEL ats.monitoring.lock=" + digest, name, base_tag)
            sha = run("git", "rev-parse", "HEAD", capture=True)
            target = base_tag
            if not args.dependencies_only:
                if run("git", "status", "--porcelain", "--untracked-files=all", capture=True):
                    raise RuntimeError("clean_committed_runtime_source_required")
                run("docker", "build", "--build-arg", "BASE_IMAGE=" + base_tag, "--build-arg", "SOURCE_SHA=" + sha,
                    "-f", "docker/monitoring/Dockerfile", "-t", "ats-monitoring:t16", ".")
                target = "ats-monitoring:t16"
            identity = json.loads(run("docker", "image", "inspect", "--format",
                '{"id":{{json .Id}},"os":{{json .Os}},"architecture":{{json .Architecture}}}', target, capture=True))
            (args.output / "image.json").write_text(json.dumps({**identity, "git_sha": sha, "lock_sha256": digest,
                "dependencies_only": args.dependencies_only,
                "bounded_install": {"cpus": 1, "memory_bytes": 1536 * 1024 * 1024, "oom_killed": False}}, sort_keys=True) + "\n")
            print(json.dumps({"outcome": "passed", "image": identity["id"], "lock_sha256": digest}))
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=30)


if __name__ == "__main__":
    main()
