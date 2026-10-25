"""Small standard-library entry point; ML imports happen only in the worker."""

from __future__ import annotations

import argparse
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
import platform
import sys

from ats_mlops.runner import CompatibilityError, run_compatibility


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ats-mlops")
    commands = parser.add_subparsers(dest="command", required=True)
    worker = commands.add_parser("compatibility-worker")
    worker.add_argument("--run-id", required=True)
    worker.add_argument("--report-path", type=Path, required=True)
    runner = commands.add_parser("compatibility-run")
    runner.add_argument("--git-sha", required=True)
    runner.add_argument("--run-id", required=True)
    runner.add_argument("--expected-lock-sha256")
    commands.add_parser("environment")
    args = parser.parse_args(argv)
    try:
        if args.command == "compatibility-run":
            print(run_compatibility(args.git_sha, args.run_id, args.expected_lock_sha256))
        elif args.command == "compatibility-worker":
            from ats_mlops.compatibility import run_worker

            run_worker(args.run_id, args.report_path)
        else:
            packages = {}
            for package in ("mlflow", "bentoml", "great-expectations", "dvc",
                            "scikit-learn", "pandas", "numpy"):
                try:
                    packages[package] = version(package)
                except PackageNotFoundError:
                    packages[package] = None
            print(json.dumps({"python": platform.python_version(),
                              "platform": platform.platform(), "packages": packages},
                             sort_keys=True))
        return 0
    except Exception as exc:
        kind = exc.kind if isinstance(exc, CompatibilityError) else "compatibility_failed"
        print(kind, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
