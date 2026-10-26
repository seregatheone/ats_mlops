"""Dependency-free syntax and frozen HTTP contract check, run before builds."""
import ast
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from service.contracts import API_SCHEMA


def check():
    files = [p for folder in ("src", "service", "scripts", "tests", "docker")
             for p in (ROOT / folder).rglob("*.py") if "__pycache__" not in p.parts]
    for path in files:
        ast.parse(path.read_text(), filename=str(path))
    fixture = json.loads((ROOT / "tests/fixtures/ci-contract.json").read_text())
    required = {"routes", "max_candidates", "top_k"}
    if not isinstance(fixture, dict) or set(fixture) != required:
        raise AssertionError("CI HTTP contract fixture requires the complete boundary")
    actual = {key: API_SCHEMA[key] for key in required}
    if fixture != actual:
        raise AssertionError("CI HTTP contract fixture differs from implemented API_SCHEMA")
    print(json.dumps({"syntax_files": len(files), "http_contract": "passed"}))
    return len(files)


if __name__ == "__main__":
    check()
