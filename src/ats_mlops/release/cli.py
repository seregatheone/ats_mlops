"""Release CLI; run from the audited source checkout, with Python stdlib only."""

import argparse
import json
from pathlib import Path
import sys

from .controller import Controller
from .state import ReleaseError


def main(argv=None):
    parser = argparse.ArgumentParser(prog="ats-mlops release")
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--config", type=Path, help="immutable initial config; omit after initialization")
    commands = parser.add_subparsers(dest="command", required=True)
    stage = commands.add_parser("stage")
    stage.add_argument("--package", required=True, type=Path)
    stage.add_argument("--release-check", required=True, type=Path)
    approve = commands.add_parser("approve")
    approve.add_argument("release_id")
    approve.add_argument("--actor", required=True, choices=("agent-operator",))
    activate = commands.add_parser("activate")
    activate.add_argument("release_id")
    rollback = commands.add_parser("rollback")
    rollback.add_argument("--release-id")
    commands.add_parser("reconcile")
    commands.add_parser("status")
    args = parser.parse_args(argv)
    try:
        config = None if args.config is None else json.loads(args.config.read_text())
        controller = Controller(args.state_dir, config)
        if args.command == "stage":
            result = controller.stage(args.package, args.release_check)
        elif args.command == "approve":
            result = controller.approve(args.release_id, args.actor)
        elif args.command == "activate":
            result = controller.activate(args.release_id)
        elif args.command == "rollback":
            result = controller.rollback(args.release_id)
        else:
            result = getattr(controller, args.command)()
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"outcome": "blocked", "reason": str(exc) if isinstance(exc, ReleaseError) else "release_operation_failed"}), file=sys.stderr)
        return 1
