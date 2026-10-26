"""Strict monitoring cards and undefined numeric/report boundaries, without Evidently."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.monitoring import drift
from ats_mlops.monitoring.feedback import (NUMERIC, admit_feedback, canonical, checked_bytes, decode_json,
    export_events, publication, sha)
from tests.unit.test_monitoring import complete, request, SOURCES
from service.events import EventStore


def numeric_rows():
    return [{name: value for name in NUMERIC} for value in (.2, .4, .6)]


def reference(root):
    matrices = {}
    for role in ("train", "validation"):
        content = b"".join(canonical(row) for row in numeric_rows())
        (root / (role + ".jsonl")).write_bytes(content)
        groups = [str(i) * 64 for i in range(3)]
        (root / (role + "-groups.json")).write_bytes(canonical(groups))
        matrices[role] = {"sha256": sha(content), "pairs": 3, "groups": 3, "groups_sha256": sha(canonical(groups))}
    model = dict.fromkeys(("release_manifest_sha256", "release_check_sha256", "model_sha256", "feature_state_sha256", "feature_schema_sha256", "run_manifest_sha256"), "a" * 64)
    model.update(model_name="ats-pair-ranking", model_version="13", release_id="fixture-13", lineage_status="guarded_schema2")
    worker = dict(isolated=True, bytecode_disabled=True, credentials=False, network=False, training_checkout_mounted=False,
                  model_sha256="a" * 64, feature_state_sha256="a" * 64, image_id="sha256:" + "b" * 64,
                  bootstrap_sha256=sha(drift.FEATURE_BOOTSTRAP.encode()))
    data = dict.fromkeys(("manifest_sha256", "admission_sha256", "bundle_sha256", "reservation_policy_sha256"), "c" * 64)
    data.update(partition_sha256=drift.CANONICAL_PARTITIONS, schema_version=2, read_partitions=["train", "validation"])
    card = dict(schema_version=2, evidence_scope="demo_only", columns=list(NUMERIC), model=model, matrices=matrices,
                data=data, feature_worker=worker, test_read=False, limitations=[])
    return card


class DriftBoundaryTests(unittest.TestCase):
    def test_numeric_empty_nonfinite_missing_bool_and_ties(self):
        for rows in ([], [{}], [dict(numeric_rows()[0], score=True)], [dict(numeric_rows()[0], score=float("inf"))]):
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                drift.numeric_rows(rows)
        rows = [numeric_rows()[0]] * 4
        self.assertEqual(set(NUMERIC), set(drift.constants(rows)))
        with self.assertRaisesRegex(ValueError, "degenerate_constant_reference"):
            drift.render(rows, rows, {}, .1, Path("unused"), timestamp="2026-01-01T00:00:00Z")
        with self.assertRaisesRegex(ValueError, "constant_reference_changed"):
            drift.render(rows, numeric_rows(), {}, .1, Path("unused"), timestamp="2026-01-01T00:00:00Z")

    def test_json_duplicate_keys_and_nonfinite_reject(self):
        for content in (b'{"score":0,"score":1}', b'{"score":NaN}', b'{"x":Infinity}'):
            with self.assertRaises(ValueError):
                decode_json(content)

    def test_reference_hash_coordinate_counts_schema_and_symlink(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); original = reference(root)
            def load(card):
                content = canonical(card); (root / "reference.json").write_bytes(content)
                return drift.load_reference(root, sha(content))
            self.assertEqual(3, len(load(original)[1]["train"]))
            changes = [("schema_version", True), ("test_read", True), ("columns", list(reversed(NUMERIC)))]
            for key, value in changes:
                changed = deepcopy(original); changed[key] = value
                with self.subTest(key=key), self.assertRaises(ValueError): load(changed)
            for area, key, value in (("model", "model_version", 13), ("feature_worker", "network", True),
                                     ("feature_worker", "model_sha256", "d" * 64), ("data", "read_partitions", ["test"])):
                changed = deepcopy(original); changed[area][key] = value
                with self.subTest(area=area, key=key), self.assertRaises(ValueError): load(changed)
            changed = deepcopy(original);changed["matrices"]["train"]["groups"] = True
            with self.assertRaises(ValueError):load(changed)
            (root / "train-groups.json").write_bytes(canonical(["a" * 64]))
            with self.assertRaisesRegex(ValueError, "counts_mismatch"):load(original)
            reference(root)
            numeric = root / "train.jsonl"; numeric.rename(root / "hidden")
            numeric.symlink_to(root / "hidden")
            with self.assertRaisesRegex(ValueError, "input_symlink"):load(original)
            with self.assertRaisesRegex(ValueError, "invalid_snapshot_path"):checked_bytes(root, "../escape")

    def test_evidently_adapter_rejects_undefined_incomplete_and_wrong_method(self):
        config = {"method": "wasserstein", "drift_share": .5}
        value = {"metrics": [{"metric_name": "ValueDrift", "config": {"column": "score", "method": "wasserstein", "threshold": .1}, "value": .2},
                             {"metric_name": "DriftedColumnsCount", "value": {"share": 1., "count": 1.}}],
                 "tests": [{"status": "FAIL"}]}
        self.assertTrue(drift.normalize_evidently(value, ["score"], config, .1, {})["detected"])
        for changed in (dict(value, tests=[{"status": "ERROR"}]), dict(value, tests=[]), dict(value, metrics=[])):
            with self.assertRaises(ValueError):drift.normalize_evidently(changed, ["score"], config, .1, {})
        changed = deepcopy(value);changed["metrics"][0]["value"] = float("nan")
        with self.assertRaises(ValueError):drift.normalize_evidently(changed, ["score"], config, .1, {})
        changed = deepcopy(value);changed["metrics"][1]["value"]["share"] = 0.
        with self.assertRaisesRegex(ValueError, "decision_mismatch"):drift.normalize_evidently(changed, ["score"], config, .1, {})

    def test_existing_feedback_output_cannot_mutate_ledger(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder);output = root / "exists";output.mkdir()
            with self.assertRaisesRegex(ValueError, "immutable_output_exists"):
                admit_feedback(root / "missing-export", [], output, SOURCES, ledger=root / "ledger")
            self.assertFalse((root / "ledger").exists())

    def test_failed_numeric_artifacts_retained_without_completed_card(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "calibration"
            with self.assertRaisesRegex(ValueError, "report_failed"):
                with publication(output) as staging:
                    (staging / "evidently.json").write_bytes(b'{"metrics":[]}')
                    raise ValueError("report_failed")
            self.assertFalse(output.exists())
            failed = list(Path(folder).glob("calibration.failed-*"))
            self.assertEqual(1, len(failed))
            self.assertEqual(b'{"metrics":[]}', (failed[0] / "evidently.json").read_bytes())

    def test_ordinary_import_never_imports_evidently(self):
        import subprocess, sys
        code = "import sys;from ats_mlops.cli import main;import ats_mlops.monitoring.drift;assert not any(n=='evidently' or n.startswith('evidently.') for n in sys.modules)"
        result = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True)
        self.assertEqual(0, result.returncode, result.stderr)


if __name__ == "__main__":
    unittest.main()
