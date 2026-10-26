"""CI transport stays on the coordinator side, independent of production defaults."""
import unittest
from pathlib import Path

from scripts.bootstrap_core import compose_command
from service.release_check import registry_transport


class TransportTests(unittest.TestCase):
    def test_explicit_transport(self):
        value = registry_transport({"registry_transport": {"network": "ci-123_default", "tracking_uri": "http://mlflow:5000"}})
        self.assertEqual(value, ["--network", "ci-123_default", "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000"])

    def test_invalid_transport(self):
        for transport in ({}, {"network": "--host", "tracking_uri": "http://mlflow"},
                          {"network": "ci", "tracking_uri": "http://user:secret@mlflow"},
                          {"network": "ci", "tracking_uri": "http://mlflow?secret=x"}):
            with self.subTest(transport=transport), self.assertRaises(ValueError):
                registry_transport({"registry_transport": transport})

    def test_explicit_compose(self):
        result = compose_command(Path("/tmp/ci-state"), "ci-123", Path("/tmp/ci-compose.yaml"))
        self.assertIn(str(Path("/tmp/ci-compose.yaml").resolve()), result)
        self.assertNotIn(str(Path.cwd() / "compose.yaml"), result)
