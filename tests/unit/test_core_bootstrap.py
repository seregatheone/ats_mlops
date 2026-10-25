"""Regression coverage for interrupted/private core initialization."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("bootstrap_core", ROOT / "scripts/bootstrap_core.py")
bootstrap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bootstrap)


class CoreBootstrapTest(unittest.TestCase):
    def test_partial_bootstrap_repairs_clients_without_rotating_credentials(self):
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "core"
            first = bootstrap.initialize(state, "ats-test", (19000, 19001, 15050))
            saved = (state / "core.env").read_bytes()
            for name in ("client.json", "dvc.env", "mlflow.env"):
                (state / name).unlink()
            second = bootstrap.initialize(state, "ats-test", (9000, 9001, 5050))
            self.assertEqual(first, second)
            self.assertEqual(saved, (state / "core.env").read_bytes())
            self.assertEqual(json.loads((state / "client.json").read_text())["mlflow_uri_host"],
                             "http://127.0.0.1:15050")
            for file in state.iterdir():
                self.assertEqual(file.stat().st_mode & 0o777, 0o600)

    def test_symlinks_are_rejected_before_read_or_write(self):
        with tempfile.TemporaryDirectory() as folder:
            actual = Path(folder) / "actual"
            actual.mkdir()
            link = Path(folder) / "link"
            link.symlink_to(actual, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "symlink"):
                bootstrap.initialize(link, "ats-test", (19000, 19001, 15050))
            (actual / "core.env").symlink_to(Path(folder) / "missing")
            with self.assertRaisesRegex(ValueError, "symlink"):
                bootstrap.initialize(actual, "ats-test", (19000, 19001, 15050))


if __name__ == "__main__":
    unittest.main()
