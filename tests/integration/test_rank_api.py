"""Actual HTTP/package acceptance. Skip without opt-in is never acceptance evidence."""

import json
import os
from pathlib import Path
import unittest

from service.release_check import stage


@unittest.skipUnless(os.environ.get("ATS_RUN_RANK_API") == "1", "requires exact local T12 package and Docker")
class RankAPITests(unittest.TestCase):
    def test_actual_fresh_container_release(self):
        request = json.loads(Path(os.environ["ATS_T12_REQUEST"]).read_text())
        output = Path(os.environ["ATS_T12_EVIDENCE_DIR"]) / "release_check_report.json"
        result = stage(request, output)
        self.assertEqual(result["outcome"], "passed", result["checks"])
        self.assertEqual(result["release_manifest_sha256"], result["release_manifest_sha256_after"])
