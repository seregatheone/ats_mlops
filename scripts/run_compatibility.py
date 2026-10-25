#!/usr/bin/env python3
"""Run the explicitly requested compatibility check with standard-library unittest."""

import os
from pathlib import Path
import subprocess
import sys


if __name__ == "__main__":
    environment = os.environ.copy()
    environment["ATS_RUN_COMPATIBILITY"] = "1"
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests/integration", "-p", "test_compatibility.py", "-v"],
        cwd=root, env=environment, check=False,
    )
    sys.exit(result.returncode)
