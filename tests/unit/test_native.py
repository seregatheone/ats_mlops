"""Native adapter contract tests. No Docker, fits, API, original test or real ledger."""
from copy import deepcopy
import json
import io
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

from ats_mlops.evaluation import experiment, native
from tests.unit.test_experiment import spec, make_study, numeric

ROOT = Path(__file__).resolve().parents[2]


class NativeTransportTests(unittest.TestCase):
    def test_metadata_wrong_role_is_rejected_without_reading_reserved_bytes(self):
        with patch.object(Path, "read_bytes", side_effect=AssertionError("must not read")) as read:
            with self.assertRaisesRegex(experiment.ExperimentError, "metadata_file_required"):
                native.document({"path": "/fake/test.jsonl", "sha256": "a"*64})
            read.assert_not_called()

    def test_wiring_uses_real_functions_and_blocks_unfinished_e4(self):
        value = experiment.read(ROOT / "configs/experiment.template.json")
        wired = native.wire_adapters(value)
        self.assertIsNone(wired["adapter_bindings"]["E4"])
        self.assertEqual("ats_mlops.evaluation.native:evaluate_e5", wired["adapter_bindings"]["E5"]["entrypoint"])
        self.assertEqual("ats_mlops.evaluation.experiment:ingest_human_measurement", wired["adapter_bindings"]["E2"]["entrypoint"])
        self.assertTrue(callable(native.e1) and callable(native.e3) and callable(native.evaluate_e5))
        with self.assertRaises(experiment.ExperimentError):native.wire_adapters(value, require_complete=True)

    def test_environment_hash_and_adapter_identity_precede_source_or_effects(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "environment.json";path.write_bytes(b"{}\n")
            value = spec();value["adapter_bindings"]["E1"] = native.adapter_binding("E1")
            with patch.dict(os.environ, {"ATS_EXPERIMENT_ENVIRONMENT": str(path)}), patch.object(native, "source_guard") as source:
                with self.assertRaisesRegex(experiment.ExperimentError, "environment_hash"):native.environment(value, series="E1")
                source.assert_not_called()

    def test_owned_core_rejects_foreign_project_before_any_mutation(self):
        with tempfile.TemporaryDirectory() as folder:
            commands = native.Commands(Path(folder))
            value = {"Id": "id", "Image": "sha256:" + "a" * 64, "State": {"Running": True, "OOMKilled": False, "Health": {"Status": "healthy"}},
                     "Labels": {"com.docker.compose.project": "ats-core", "com.docker.compose.service": "postgres"}, "Networks": {"ats-core_default": {"Aliases": []}}}
            result = subprocess.CompletedProcess([], 0, stdout=experiment.canonical(value), stderr=b"")
            with patch.object(commands, "run", return_value=result) as run:
                with self.assertRaisesRegex(experiment.ExperimentError, "foreign_or_unhealthy"):
                    native.owned_core(commands, {"project": "ats-experiment-case", "network": "ats-experiment-case_default",
                        "core": {"containers": dict.fromkeys(("postgres", "minio", "mlflow"), "some-container"), "ports": {}, "images": {"postgres": "sha256:" + "a"*64}}})
                self.assertEqual(1, run.call_count)
                self.assertEqual(["docker", "inspect"], run.call_args.args[0][:2])
                self.assertNotIn(".Config.Env", run.call_args.args[0][3])

    def test_release_transport_cannot_point_to_default_ports(self):
        config = {"project": "ats-acceptance-toy", "release": {"port": 3000, "registry_uri": "http://127.0.0.1:5050", "model_name": "ats-pair-ranking", "ready_timeout_seconds": 10}}
        with self.assertRaisesRegex(experiment.ExperimentError, "private_release_ports"):native.release_config(config, Path("/tmp/toy"))
        config["release"].update(port=13017, registry_uri="http://127.0.0.1:15050")
        settings = native.release_config(config, Path("/tmp/toy"))
        self.assertTrue(settings["alias"].startswith("ats-acceptance-toy-"))
        self.assertNotEqual("champion", settings["alias"])

    def test_commands_capture_real_exit_and_never_success_marker(self):
        with tempfile.TemporaryDirectory() as folder:
            commands = native.Commands(folder)
            result = commands.run([__import__('sys').executable, "-B", "-c", "print('sandbox-only')"])
            self.assertEqual(b"sandbox-only\n", result.stdout)
            with self.assertRaisesRegex(experiment.ExperimentError, "command_failed"):
                commands.run([__import__('sys').executable, "-B", "-c", "raise SystemExit(23)"])
            self.assertEqual(23, commands.records[-1]["exit_code"])
            digest = commands.finish();self.assertEqual(digest, experiment.sha((Path(folder) / "commands.json").read_bytes()))

    def test_source_guard_rejects_dirty_git_before_namespace_or_effects(self):
        with patch.object(native.subprocess, "run", side_effect=[Mock(stdout="a"*40), Mock(stdout=" M src/file.py")]) as run:
            with self.assertRaisesRegex(experiment.ExperimentError, "clean_source"):native.source_guard(ROOT, "a"*40)
            self.assertEqual(2, run.call_count)


class FinalScorerTests(unittest.TestCase):
    def test_first_qualification_slot_is_declared_before_fit_and_not_replaceable(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder);value = spec();config = {"bundle_sha256": "f"*64}
            plan = native.declare_qualification(value, config, root)
            self.assertEqual("S1", plan["slot"])
            self.assertFalse((root / "qualified-selection.json").exists())
            record = {"git_sha": value["source_git_sha"], "dirty_worktree": False, "test_read": False,
                "read_partitions": ["train", "validation"], "data_manifest_sha256": plan["data_manifest_sha256"],
                "split_sha256": plan["split_sha256"], "model": {"sha256": "a"*64}, "mlflow": {"run_id": "1"*32, "model_version": "1"}}
            selected = native.select_qualification(plan, record, root)
            self.assertEqual(record["mlflow"], selected["mlflow"])
            for changed in (deepcopy(record), deepcopy(record)):
                changed["mlflow"]["run_id"] = "2"*32
                with self.assertRaisesRegex(experiment.ExperimentError, "immutable_receipt"):
                    native.select_qualification(plan, changed, root)
            with self.assertRaisesRegex(experiment.ExperimentError, "immutable_receipt"):
                native.declare_qualification(value, config, root)

    def test_streaming_worker_is_ready_before_test_snapshot_and_keeps_only_numeric(self):
        from tests.unit.test_holdout_worker import w, ready, message, toy_session
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder);artifact = root / "model";artifact.mkdir();output = root / "worker"
            record = {"environment": {"image": {"id": "sha256:"+"a"*64}}, "model": {"sha256": "a"*64}, "source_tree_sha256": "c"*64}
            calls = []
            test = self
            class Process:
                def __init__(self, argv, **kwargs):
                    test.assertIn("none", argv);test.assertIn("--read-only", argv)
                    test.assertIn("-I", argv);test.assertIn("-B", argv);test.assertIn("-i", argv)
                    mounts = [argv[i+1] for i,item in enumerate(argv[:-1]) if item == "--mount"]
                    test.assertEqual(4, len(mounts));test.assertEqual(3, sum("readonly" in item for item in mounts))
                    test.assertFalse(any("docker.sock" in item or "target=/source" in item for item in mounts))
                    preflight = Path(next(item.split("source=",1)[1].split(",",1)[0] for item in mounts if "target=/input" in item)) / "preflight.json"
                    request = experiment.read(preflight)
                    test.assertEqual({"schema_version", "record", "warmup_rows", "baseline_artifact_sha256"}, set(request))
                    test.assertNotIn("snapshot", request)
                    w.publish(output / "ready.json", ready())
                    self.stdout = io.BytesIO(w.canonical(ready()));self.stdin = io.BytesIO();self.returncode = None
                    calls.append("ready")
                def communicate(self, payload, timeout):
                    calls.append("snapshot");rows, truth = w.validate_snapshot(w.decode(payload))
                    w.publish(output / "truth.json", truth)
                    observed = w.evaluate(toy_session([], output), rows, truth)
                    w.publish(output / "numeric.json", observed);final = w.recover(output)
                    self.returncode = 0
                    return w.canonical(final), None
                def poll(self):return self.returncode
                def wait(self, timeout):return self.returncode
            def run(argv, **kwargs):
                return subprocess.CompletedProcess(argv, 0, stdout=record["environment"]["image"]["id"]+"\n")
            payload = message()
            snapshot = dict(sha256=payload["snapshot_sha256"], rows=payload["snapshot"]["rows"],
                pair_ids_sha256=payload["pair_ids_sha256"], ground_truth_sha256=payload["ground_truth_sha256"])
            with patch.object(native.subprocess, "run", side_effect=run), patch.object(native.subprocess, "Popen", Process), patch.object(native.select, "select", return_value=([True], [], [])):
                with native.HoldoutSession({"record": record, "artifact": artifact}, [], baseline_artifact_sha256="b"*64, output=output) as session:
                    self.assertEqual(["ready"], calls)
                    observed = session.score(snapshot)
                    self.assertEqual(4, len(observed["rows"]))
                    with self.assertRaisesRegex(experiment.ExperimentError, "one_snapshot"):
                        session.score(snapshot)
            self.assertEqual(["ready", "snapshot"], calls)
            self.assertFalse(any("PRIVATE" in path.read_text() for path in output.iterdir() if path.is_file()))

    def test_failed_worker_preflight_never_consumes_guard_or_reads_test(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder);directory = make_study(root)
            with patch.object(native, "environment", return_value={}), patch.object(native, "_qualified", side_effect=experiment.ExperimentError("preflight")), patch.object(experiment, "HoldoutGuard") as guard:
                authorization = {"spec_sha256": experiment.study(directory)[1]["spec_sha256"], "holdout_lineage_sha256": experiment.holdout_key(spec()),
                                 "actor": "root-coordinator", "scope": "one_attempt_synthetic_final_test"}
                with self.assertRaisesRegex(experiment.ExperimentError, "preflight"):
                    native.evaluate_e5(directory, root / "absent.sqlite", authorization)
                guard.assert_not_called()
            self.assertFalse((root / "absent.sqlite").exists())

    def test_no_authorization_reaches_neither_model_preflight_nor_test_guard(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder);directory = make_study(root)
            with patch.object(native, "environment", return_value={}), patch.object(native, "_qualified") as qualified, patch.object(experiment, "HoldoutGuard") as guard:
                with self.assertRaisesRegex(experiment.ExperimentError, "authorization"):
                    native.evaluate_e5(directory, root / "must-not-create.sqlite3", {})
                qualified.assert_not_called();guard.assert_not_called()
            self.assertFalse((root / "must-not-create.sqlite3").exists())


class ClaimAuditTests(unittest.TestCase):
    def test_numeric_claims_bind_source_units_and_executed_series(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder);content = experiment.canonical({"replicas": 3, "ndcg_at_10": .8})
            (root / "source.json").write_bytes(content)
            claim = {"id": "qualification-count", "kind": "engineering_qualification", "experiment": None,
                     "metric": "replicas", "value": 3, "units": "count", "repetitions": 3,
                     "limitation": "engineering acceptance, not E1/generalization", "evidence": {"path": "source.json", "sha256": experiment.sha(content)},
                     "selector": ["replicas"]}
            register = {"schema_version": 1, "evidence_scope": "demo_only", "independent_real_corpus": False,
                        "experiments": dict.fromkeys(experiment.SERIES, "not_run"), "claims": [claim]}
            self.assertEqual("passed", native.claim_traceability(register, root)["outcome"])
            for key, value in (("units", "seconds"), ("value", 4), ("value", True), ("experiment", "E1")):
                changed = deepcopy(register);changed["claims"][0][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(experiment.ExperimentError):native.claim_traceability(changed, root)
            changed = deepcopy(register);changed["claims"][0].update(kind="synthetic_experiment", experiment="E5")
            with self.assertRaisesRegex(experiment.ExperimentError, "without_executed"):native.claim_traceability(changed, root)
            link = root / "outside";link.symlink_to(root, target_is_directory=True)
            changed = deepcopy(register);changed["claims"][0]["evidence"]["path"] = "outside/source.json"
            with self.assertRaisesRegex(experiment.ExperimentError, "evidence_path"):native.claim_traceability(changed, root)


if __name__ == "__main__":
    unittest.main()
