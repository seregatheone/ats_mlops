"""Small CI safety boundaries; real Docker/Actions evidence is separate."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ci_run", ROOT / "docker/ci/run.py")
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class CITests(unittest.TestCase):
    def test_offline_scorer_can_read_private_input_without_broadening_permissions(self):
        from ats_mlops.quality.isolated import DockerScorer
        image = "sha256:" + "a" * 64
        record = {"environment": {"image": {"id": image}}, "model": {"sha256": "b" * 64}, "source_tree_sha256": "c" * 64}

        def transport(args, **kwargs):
            if args[1:3] == ["image", "inspect"]:
                return SimpleNamespace(stdout=json.dumps([{"Id": image}]))
            self.assertEqual(args[args.index("--user") + 1], f"{os.getuid()}:{os.getgid()}")
            mount = next(value for value in args if value.startswith("type=bind,source=") and value.endswith("target=/input,readonly"))
            source = Path(mount.split("source=", 1)[1].split(",target=", 1)[0])
            self.assertEqual(source.stat().st_mode & 0o777, 0o700)
            self.assertEqual(source.stat().st_uid, os.getuid())
            self.assertEqual(args[args.index("--network") + 1], "none")
            self.assertIn("--read-only", args)
            self.assertEqual(args[-4:-1], ["-I", "-B", "-c"])
            receipt = {"model_sha256": "b" * 64, "source_tree_sha256": "c" * 64, "isolated": True, "prior_project_import": False}
            return SimpleNamespace(returncode=0, stdout=json.dumps({"outcome": "passed", "receipt": receipt, "scores": []}))

        with tempfile.TemporaryDirectory() as folder, patch("ats_mlops.quality.isolated.subprocess.run", side_effect=transport):
            scores, receipt = DockerScorer()({"record": record, "artifact": Path(folder)}, [])
            self.assertEqual(scores, {})
            self.assertEqual(receipt["runtime_image_id"], image)

    def test_empty_or_partial_contract_cannot_pass(self):
        spec = importlib.util.spec_from_file_location("ci_static", ROOT / "docker/ci/static.py")
        static = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(static)
        for fixture in ({}, {"max_candidates": 100}, {"routes": {"/rank": "POST"}, "max_candidates": 100, "top_k": [1, 100]},
                        {"routes": {"/rank": "POST", "/metadata": "POST"}, "max_candidates": 100.0, "top_k": [True, 100]}):
            with self.subTest(fixture=fixture), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "tests/fixtures/ci-contract.json"
                path.parent.mkdir(parents=True)
                path.write_text(json.dumps(fixture))
                with patch.object(static, "ROOT", Path(folder)), self.assertRaises(AssertionError):
                    static.check()

    def test_private_outputs_cannot_enter_checkout(self):
        with self.assertRaisesRegex(ValueError, "external_separate"):
            ci.Lifecycle(ROOT / ".local/forbidden-ci", Path("/tmp/ci-public"))

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
