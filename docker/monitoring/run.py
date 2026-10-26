"""Monitoring venv contains numeric reporting only; no model artifact is loaded."""
import importlib.metadata
from pathlib import Path
import re
import sys

assert sys.flags.isolated and sys.dont_write_bytecode
for line in Path("/opt/requirements-monitoring.lock").read_text().splitlines():
    matched = re.match(r"^([A-Za-z0-9._-]+)==([^ ;\\]+)", line)
    if matched:
        assert importlib.metadata.version(matched[1]) == matched[2]
sys.path.insert(0, "/opt/monitoring/src")
from ats_mlops.monitoring.drift import main

raise SystemExit(main())
