"""Immutable directory and bytecode guards must run before any unpickle."""

from pathlib import Path
import tempfile
import unittest

from service.contracts import canonical, sha
from service.integrity import inventory
from service.package import exclusive


class PackagingTests(unittest.TestCase):
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
