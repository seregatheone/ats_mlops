"""Small standard-library entry point; ML imports happen only in the worker."""

from __future__ import annotations

import argparse
from importlib.metadata import PackageNotFoundError, version
import json
from pathlib import Path
import platform
import sys

from ats_mlops.runner import CompatibilityError, run_compatibility
from ats_mlops.data import DataError, audit_manifest, generate_dataset


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["monitoring"]:
        from ats_mlops.monitoring.drift import main as monitoring_main

        return monitoring_main(argv[1:])
    if argv[:1] == ["release"]:
        from ats_mlops.release.cli import main as release_main

        return release_main(argv[1:])
    if argv[:1] == ["orchestration"]:
        from ats_mlops.orchestration import main as orchestration_main

        return orchestration_main(argv[1:])
    if argv[:1] == ["pipeline"]:
        from ats_mlops.pipeline import main as pipeline_main

        return pipeline_main(argv[1:])
    parser = argparse.ArgumentParser(prog="ats-mlops")
    commands = parser.add_subparsers(dest="command", required=True)
    worker = commands.add_parser("compatibility-worker")
    worker.add_argument("--run-id", required=True)
    worker.add_argument("--report-path", type=Path, required=True)
    runner = commands.add_parser("compatibility-run")
    runner.add_argument("--git-sha", required=True)
    runner.add_argument("--run-id", required=True)
    runner.add_argument("--expected-lock-sha256")
    data = commands.add_parser("data")
    data_commands = data.add_subparsers(dest="data_command", required=True)
    generate = data_commands.add_parser("generate")
    generate.add_argument("--config", type=Path, default=Path("configs/data.json"))
    generate.add_argument("--manifest", type=Path, default=Path("data_manifest.json"))
    generate.add_argument("--output-dir", type=Path, default=Path("data"))
    audit = data_commands.add_parser("audit")
    audit.add_argument("--manifest", type=Path, default=Path("data_manifest.json"))
    validate = data_commands.add_parser("validate")
    validate.add_argument("--manifest", type=Path, default=Path("data_manifest.json"))
    validate.add_argument("--report-dir", type=Path, default=Path("reports"))
    validate.add_argument("--mode", choices=("admission", "training"), default="admission")
    validate.add_argument("--admission-report", type=Path)
    commands.add_parser("environment")
    commands.add_parser("pipeline", help="immutable-source DVC pipeline; use pipeline --help")
    args = parser.parse_args(argv)
    try:
        if args.command == "data" and args.data_command == "validate":
            from ats_mlops.data.validate import validate_dataset

            report = validate_dataset(args.manifest, args.report_dir, mode=args.mode,
                                      admission_report=args.admission_report)
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            return 0 if report["outcome"] == "passed" else 1
        if args.command == "data":
            report = (generate_dataset(args.config, args.manifest, args.output_dir)
                      if args.data_command == "generate" else audit_manifest(args.manifest))
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        elif args.command == "compatibility-run":
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
        kind = (str(exc) if isinstance(exc, DataError) else
                exc.kind if isinstance(exc, CompatibilityError) else "data_invalid" if args.command == "data"
                else "compatibility_failed")
        print(kind, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
