import json
import hashlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops import orchestration as runner
from ats_mlops.lineage.manifest import inventory_hash


def request():
    params = {"model": {}, "search": {}, "smoke": True, "evaluation": {"score_decimals": 12}}
    return {"request_id": "unit-1", "run_id": "unit-1", "dataset_version": "ci-unit", "git_sha": "a" * 40,
            "bundle_uri": "s3://ats-dvc/orchestration/bundles/ci-unit/bundle.json", "bundle_inventory_sha256": "b" * 64,
            "parameters": params, "params_hash": runner.digest(params)}


class OrchestrationTests(unittest.TestCase):
    def test_forbidden_bundle_bytes_are_rejected_before_s3_get(self):
        required = {"data_manifest.json", "admission/data-admission.json", "dataset-binding.json",
                    "data/train.jsonl.dvc", "data/validation.jsonl.dvc", "data/split.json.dvc"}
        for forbidden in ("data/test.jsonl", "reserved/hidden-test.jsonl"):
            with self.subTest(forbidden=forbidden), tempfile.TemporaryDirectory() as temporary:
                manifest = {"artifacts": {name: {"path": path} for name, path in {
                    "train": "data/train.jsonl", "validation": "data/validation.jsonl", "test": forbidden,
                    "source": "data/source.jsonl", "corpus": "data/corpus.jsonl", "split_lists": "data/split.json"}.items()}}
                metadata = json.dumps(manifest).encode()
                files = {name: "a" * 64 for name in required | {"data/train.jsonl", "data/validation.jsonl", forbidden}}
                files["data_manifest.json"] = hashlib.sha256(metadata).hexdigest()
                descriptor = {"schema_version": 1, "files": files, "sha256": inventory_hash(files)}
                conf = {**request(), "bundle_inventory_sha256": descriptor["sha256"]}
                calls = []

                class RecordingS3:
                    def get_object(self, Bucket, Key):
                        name = Key.rsplit("/", 1)[-1]
                        calls.append(Key)
                        if name == "bundle.json":
                            return {"Body": io.BytesIO(json.dumps(descriptor).encode())}
                        if name == "data_manifest.json":
                            return {"Body": io.BytesIO(metadata)}
                        raise AssertionError("partition bytes fetched before ingress plan approval")

                with self.assertRaisesRegex(runner.Failure, "data_invalid"):
                    runner.fetch_bundle(RecordingS3(), conf, Path(temporary) / "bundle")
                self.assertTrue(all(key.endswith(("/bundle.json", "/data_manifest.json")) for key in calls))
                self.assertFalse(any(key.endswith("/" + forbidden) for key in calls))

    def test_request_hash_and_uri_fail_closed(self):
        conf = request()
        self.assertEqual(runner.validate(conf), conf)
        for change in ({"bundle_uri": "s3://ats-mlflow/bundle.json"}, {"git_sha": "main"}, {"params_hash": "0" * 64}, {"fault": True}):
            with self.subTest(change=change), self.assertRaises(runner.Failure):
                runner.validate({**conf, **change})

    def test_frozen_mode_uses_same_request_protocol_and_bad_mode_has_no_effects(self):
        conf = request()
        conf["parameters"].update(search={"mode": "frozen"}, smoke=False)
        conf["params_hash"] = runner.digest(conf["parameters"])
        self.assertEqual(runner.validate(conf), conf)
        for search, smoke in (({"mode": "frozen", "n_trials": 1}, False), ({"mode": "frozen"}, True),
                              ({"mode": "unknown"}, False), ({"mode": 1}, False)):
            conf["parameters"].update(search=search, smoke=smoke)
            conf["params_hash"] = runner.digest(conf["parameters"])
            with patch.object(Path, "mkdir") as mkdir, patch.object(runner, "download") as download:
                with self.assertRaisesRegex(runner.Failure, "invalid_selection_mode"):
                    runner.run(conf)
            mkdir.assert_not_called()
            download.assert_not_called()

    def test_conflict_and_completed_replay_do_not_claim_another_attempt(self):
        with tempfile.TemporaryDirectory() as temporary, runner.database(Path(temporary) / "ledger") as db:
            self.assertIsNone(runner.claim(db, "r", "payload"))
            attempt = runner.begin_attempt(db, "r")
            runner.finish(db, "r", attempt, "completed", receipt_hash="receipt")
            row = runner.claim(db, "r", "payload")
            self.assertEqual((row["status"], row["attempts"], row["receipt_hash"]), ("completed", 1, "receipt"))
            with self.assertRaisesRegex(runner.Failure, "request_conflict"):
                runner.claim(db, "r", "different")

    def test_retry_budget_survives_reopen_and_retrigger(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "ledger"
            for attempt in (1, 2, 3):
                with runner.database(path) as db:
                    runner.claim(db, "r", "payload")
                    self.assertEqual(runner.begin_attempt(db, "r"), attempt)
                    runner.finish(db, "r", attempt, "retryable", kind="io_transient")
            with runner.database(path) as db:
                with self.assertRaisesRegex(runner.Failure, "retry_budget_exhausted"):
                    runner.claim(db, "r", "payload")
                self.assertEqual(db.execute("SELECT count(*) FROM attempts").fetchone()[0], 3)

    def test_terminal_data_and_uncertain_commit_cannot_refit(self):
        for status in ("failed", "recovery_required"):
            with tempfile.TemporaryDirectory() as temporary, runner.database(Path(temporary) / "ledger") as db:
                runner.claim(db, "r", "payload")
                runner.finish(db, "r", runner.begin_attempt(db, "r"), status, kind=status)
                with self.assertRaisesRegex(runner.Failure, status):
                    runner.claim(db, "r", "payload")

    def test_real_t9_train_failure_shape_is_ambiguous_without_optional_flag(self):
        receipt = {"outcome": "failed", "events": {"failure-train": {"error_type": "ValueError", "retryable": False, "reason": "registered_run_mismatch"}}}
        self.assertEqual(runner.repro_failure_kind(receipt), "recovery_required")
        self.assertEqual(runner.repro_failure_kind({"events": {}, "outcome": "failed"}), "recovery_required")
        self.assertEqual(runner.repro_failure_kind({"events": {"failure-validate": {"error_type": "DataError"}}}), "data_or_config_invalid")

    def test_recovery_completes_running_attempt_in_same_transaction(self):
        with tempfile.TemporaryDirectory() as temporary, runner.database(Path(temporary) / "ledger") as db:
            runner.claim(db, "r", "payload")
            runner.begin_attempt(db, "r")
            runner.recovered(db, "r", "receipt")
            self.assertEqual(db.execute("SELECT status FROM requests").fetchone()[0], "completed")
            self.assertEqual(tuple(db.execute("SELECT phase,outcome FROM attempts").fetchone()), ("recovered", "completed"))

    def test_third_attempt_crash_can_recover_complete_receipt_without_fourth_attempt(self):
        with tempfile.TemporaryDirectory() as temporary, runner.database(Path(temporary) / "ledger") as db:
            for _ in range(3):
                runner.claim(db, "r", "payload")
                attempt = runner.begin_attempt(db, "r")
                if attempt < 3:
                    runner.finish(db, "r", attempt, "retryable")
            self.assertEqual(runner.claim(db, "r", "payload")["attempts"], 3)
            with self.assertRaisesRegex(runner.Failure, "retry_budget_exhausted"):
                runner.begin_attempt(db, "r")
            runner.recovered(db, "r", "receipt")
            self.assertEqual(runner.claim(db, "r", "payload")["status"], "completed")
            self.assertEqual(db.execute("SELECT count(*) FROM attempts").fetchone()[0], 3)

    def test_ml_environment_drops_airflow_sql_and_root_credentials(self):
        with patch.dict(os.environ, {"AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": "secret", "MINIO_ROOT_PASSWORD": "secret",
                                     "MLFLOW_SECRET_ACCESS_KEY": "secret", "AWS_SECRET_ACCESS_KEY": "dvc", "MLFLOW_TRACKING_URI": "http://mlflow:5000"}):
            env = runner.ml_env(Path("/source"))
        self.assertEqual(env["AWS_SECRET_ACCESS_KEY"], "dvc")
        self.assertEqual(env["PYTHONPATH"], "/source/src")
        self.assertFalse(any(name in env for name in ("MINIO_ROOT_PASSWORD", "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "MLFLOW_SECRET_ACCESS_KEY")))

    def test_cache_receipt_uses_existing_model_and_xcom_only_ids_uris(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            model = {"run_id": "old-run", "exact_uri": "models:/model/7"}
            path = root / "run/workspace/artifacts/evaluation/pipeline_manifest.json"
            training = root / "run/workspace/artifacts/training/final"
            (training / "model").mkdir(parents=True)
            (training / "model/payload").write_text("model")
            files = {"payload": runner.file_hash(training / "model/payload")}
            runner.write(training / "run_manifest.json", {"mlflow": model, "model": {"files": files, "sha256": inventory_hash(files)}})
            runner.write(path, {"training": {"mlflow": model, "run_manifest_sha256": runner.file_hash(training / "run_manifest.json"), "model_sha256": inventory_hash(files)}})
            receipt = {"request_id": "r", "candidate_id": "c", "outcome": "cache_hit", "mlflow": model,
                       "receipt_uri": (root / "candidate.json").as_uri(), "pipeline_manifest_sha256": runner.file_hash(path)}
            runner.write(root / "candidate.json", receipt)
            result = runner.verify_result(root, runner.file_hash(root / "candidate.json"))
            xcom = runner.xcom(result)
            self.assertEqual(xcom["mlflow_run_id"], "old-run")
            self.assertNotIn("outcome", xcom)
            self.assertTrue(all(key.endswith(("_id", "_uri")) for key in xcom))
            record = runner.read(training / "run_manifest.json")
            runner.write(training / "run_manifest.json", {**record, "seed": 999})
            with patch.object(runner, "execute", side_effect=AssertionError("must not execute")), self.assertRaisesRegex(runner.Failure, "completed_training_manifest_mismatch"):
                runner.verify_result(root)
            path.write_text("{}")
            with self.assertRaisesRegex(runner.Failure, "completed_manifest_mismatch"):
                runner.verify_result(root)
