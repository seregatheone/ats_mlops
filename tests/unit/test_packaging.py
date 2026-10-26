"""Immutable directory and bytecode guards must run before any unpickle."""

from pathlib import Path
import tempfile
import unittest

from service.contracts import canonical, sha
from service.integrity import inventory
from service.package import exclusive, portable_permissions


class PackagingTests(unittest.TestCase):
    def test_delivery_permissions_preserve_bytes_and_private_parent(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "package"
            model = root / "models/tag/mlflow_model"
            model.mkdir(parents=True, mode=0o700)
            model.parent.chmod(0o700)
            payload = model / "model.pkl"
            payload.write_bytes(b"never unpickled")
            payload.chmod(0o600)
            before = inventory(root)
            portable_permissions(root)
            self.assertEqual(inventory(root), before)
            self.assertEqual(parent.stat().st_mode & 0o777, 0o700)
            for path in [root, *root.rglob("*")]:
                self.assertEqual(path.stat().st_mode & 0o777, 0o755 if path.is_dir() else 0o444)

    def test_inventory_and_exclusive_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            exclusive(root / "report.json", canonical({"outcome": "passed"}))
            self.assertEqual(inventory(root), {"report.json": sha((root / "report.json").read_bytes())})
            with self.assertRaises(FileExistsError):
                exclusive(root / "report.json", b"changed")

    def test_bytecode_empty_cache_and_symlink_rejected(self):
        for kind in ("pyc", "pyo", "cache", "symlink"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                if kind == "cache":
                    (root / "__pycache__").mkdir()
                elif kind == "symlink":
                    (root / "link").symlink_to("/tmp")
                else:
                    (root / ("module." + kind)).write_bytes(b"x")
                with self.assertRaises(ValueError):
                    inventory(root)
