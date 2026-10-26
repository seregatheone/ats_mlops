"""Final acceptance gate: missing actual evidence is failure, never a skip."""
import os
from pathlib import Path
import unittest

from tests.e2e.audit import validate_freeze


class ActualAcceptanceTests(unittest.TestCase):
    def test_eight_actual_scenarios_qualify_frozen_protocol(self):
        path = os.environ.get("ATS_ACCEPTANCE_FREEZE")
        digest = os.environ.get("ATS_ACCEPTANCE_FREEZE_SHA256")
        self.assertTrue(path and digest, "acceptance_not_run: actual eight-scenario freeze receipt and expected SHA are required")
        frozen = validate_freeze(Path(path), digest)
        self.assertEqual("passed", frozen["outcome"])


if __name__ == "__main__":
    unittest.main()
