"""Small CI safety boundaries; real Docker/Actions evidence is separate."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ci_run", ROOT / "docker/ci/run.py")
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class CITests(unittest.TestCase):
    def test_cleanup_rejects_foreign_project_before_docker(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(ci.subprocess, "run") as run:
            (Path(folder) / "control.json").write_text(json.dumps({"project": "ats-core"}))
            with self.assertRaisesRegex(ValueError, "invalid_ci_cleanup_owner"):
                ci.cleanup_existing(Path(folder))
            run.assert_not_called()

    def test_public_report_does_not_copy_private_configuration(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            instance = ci.Lifecycle(root / "work", root / "public")
            report = json.dumps(instance.report)
            for key in ci.initialize.__globals__["SECRET_KEYS"]:
                self.assertNotIn(instance.values[key], report)
                self.assertNotIn(key, report)
            self.assertTrue(instance.project.startswith("ats-ci-"))
            self.assertNotIn("ats-core_default", json.dumps(instance.compose))

    def test_contract_fixture_is_consumed_by_real_api_contract(self):
        from service.contracts import API_SCHEMA
        fixture = json.loads((ROOT / "tests/fixtures/ci-contract.json").read_text())
        self.assertEqual(fixture, {key: API_SCHEMA[key] for key in fixture})
