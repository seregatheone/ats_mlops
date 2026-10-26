"""Strict monitoring cards and undefined numeric/report boundaries, without Evidently."""
from copy import deepcopy
from dataclasses import replace
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
    def test_approved_v2_reference_uses_matched_noncanonical_data_and_hashes_approval(self):
        from ats_mlops.monitoring import policy as protocol
        from ats_mlops.data.reservation import ROOT_POLICY_SHA256
        from tests.unit.test_retraining_policy import approval
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); card = reference(root)
            approved = approval(label_source='approved_synthetic_fixture')
            proof = {'kind':'approved_synthetic_teacher_v2','actor':approved.actor,'approved_at':protocol.stamp(approved.approved_at),
                'previous_labelset_sha256':approved.parent_labelset_sha256,'labelset_sha256':approved.labelset_sha256,
                'new_label_count':approved.new_label_count,'manifest_sha256':approved.manifest_sha256,
                'bundle_sha256':approved.bundle_inventory_sha256}
            approved = replace(approved,approval_sha256=protocol.digest(proof))
            receipt = {'approval':protocol.payload(approved),'proof':proof}
            approval_bytes = canonical(receipt); (root/'dataset-approval.json').write_bytes(approval_bytes)
            card['schema_version'] = 3
            card['data'].update(manifest_sha256=approved.manifest_sha256,admission_sha256=approved.admission_sha256,
                bundle_sha256=approved.bundle_inventory_sha256,reservation_policy_sha256=approved.reservation_policy_sha256,
                partition_sha256={'train':'d'*64,'validation':'e'*64})
            guard = dict(model_sha256=card['model']['model_sha256'],manifest_sha256=approved.manifest_sha256,
                admission_sha256=approved.admission_sha256,partition_sha256=card['data']['partition_sha256'],
                policy_sha256=approved.reservation_policy_sha256,root_policy_sha256=ROOT_POLICY_SHA256,
                guard_report_sha256='f'*64,read_partitions=['train','validation'],test_read=False,
                image_id=card['feature_worker']['image_id'],bootstrap_sha256=sha(drift.ADMISSION_BOOTSTRAP.encode()))
            card['admission'] = dict(approval_file_sha256=sha(approval_bytes),guard=guard)
            from ats_mlops.data.validate import GX_ROOT,SCHEMA
            suite=decode_json((GX_ROOT/'pairs.json').read_bytes())
            checks=[dict(expectation=item['type'],column=item['kwargs'].get('column'),success=True,reason=item['meta']['reason']) for item in suite['expectations']]
            checks += [dict(expectation='expect_column_values_to_not_be_null',column=name,success=True,reason='missing_field_or_null') for name in SCHEMA]
            report=dict(schema_version=1,mode='training',outcome='passed',evidence_scope='demo_only',manifest_sha256=approved.manifest_sha256,
                engine='great_expectations',engine_version='1.24.1',reasons=[],artifact_hashes=card['data']['partition_sha256'],
                counts={'train':3,'validation':3},config_hashes={name:sha((GX_ROOT/name).read_bytes()) for name in ('pairs.json','checkpoint.json')},
                reservation=dict(policy_sha256=approved.reservation_policy_sha256,root_policy_sha256=ROOT_POLICY_SHA256,
                    checked_partitions=['train','validation'],semantic_components=6),
                gx=[dict(scope=role,rows=3,success=True,checks=checks) for role in ('train','validation')])
            def save_report(value,outer):
                value=dict(value);value.pop('report_sha256',None);value['report_sha256']=sha(canonical(value))
                content=canonical(value);(root/'data-training-guard.json').write_bytes(content)
                outer['admission']['guard']['guard_report_sha256']=sha(content)
            save_report(report,card)
            def load(value):
                content=canonical(value); (root/'reference.json').write_bytes(content)
                return drift.load_reference(root,sha(content))
            self.assertEqual('guarded_schema2',load(card)[0]['model']['lineage_status'])
            for field,value in (('mode','admission'),('outcome','rejected'),('artifact_hashes',drift.CANONICAL_PARTITIONS),
                                ('counts',{'train':True,'validation':3}),('reasons',['private@example.invalid'])):
                changed=deepcopy(card);bad=deepcopy(report);bad[field]=value;save_report(bad,changed)
                with self.subTest(report_field=field),self.assertRaises(ValueError):load(changed)
            save_report(report,card)
            content=(root/'data-training-guard.json').read_bytes();(root/'data-training-guard.json').write_bytes(content+b'\n')
            with self.assertRaisesRegex(ValueError,'reference_guard_report_changed'):load(card)
            (root/'data-training-guard.json').unlink()
            with self.assertRaises((ValueError,OSError)):load(card)
            save_report(report,card)
            for field,value in (('manifest_sha256','1'*64),('root_policy_sha256','2'*64),
                                ('test_read',True),('partition_sha256',drift.CANONICAL_PARTITIONS)):
                changed=deepcopy(card);changed['admission']['guard'][field]=value
                with self.subTest(field=field),self.assertRaises(ValueError):load(changed)
            changed=deepcopy(card);changed['model']['lineage_status']='legacy_engineering_only'
            with self.assertRaises(ValueError):load(changed)
            # Rehashing all outer bytes cannot turn a changed actor/time/bundle
            # into the original approval, nor accept a Python-equal bool count.
            for field,value in (('actor','other-actor'),('approved_at','2026-10-26T01:00:00Z'),
                                ('bundle_sha256','3'*64),('new_label_count',True)):
                changed=deepcopy(receipt);changed['proof'][field]=value
                changed['approval']['approval_sha256']=protocol.digest(changed['proof'])
                with self.subTest(field=field),self.assertRaises(ValueError):drift.approved_receipt(changed)
            (root/'dataset-approval.json').write_bytes(approval_bytes+b'\n')
            with self.assertRaisesRegex(ValueError,'reference_approval_changed'):load(card)

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
