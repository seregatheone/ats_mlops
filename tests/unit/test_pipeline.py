"""Admission binding, safe materialization, identity and cache-event boundaries."""

from copy import deepcopy
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from ats_mlops.data.dataset import canonical_json
from ats_mlops.lineage.manifest import file_hash, write_json
from ats_mlops import pipeline

ROOT = Path(__file__).resolve().parents[2]


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shared = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.shared.cleanup)
        cls.bundle = Path(cls.shared.name) / "bundle"
        cls.descriptor = pipeline.create_bundle(ROOT, ROOT / "tests/fixtures/data/ci-config.json", cls.bundle)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.workspace = Path(self.temporary.name) / "workspace"
        self.workspace.mkdir()
        for name in self.descriptor["files"]:
            target = self.workspace / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.bundle / name, target)
        shutil.copyfile(ROOT / "params.yaml", self.workspace / "params.yaml")

    def test_bundle_is_exact_train_validation_inventory_and_immutable(self):
        verified = pipeline.verify_bundle(self.bundle, self.descriptor["sha256"])
        self.assertEqual(verified, self.descriptor)
        names = set(verified["files"])
        self.assertIn("data/train.jsonl", names)
        self.assertIn("data/validation.jsonl", names)
        self.assertTrue(names.isdisjoint({"data/test.jsonl", "data/source.jsonl", "data/corpus.jsonl", "data/split.json"}))
        with self.assertRaisesRegex(ValueError, "bundle_identity_mismatch"):
            pipeline.verify_bundle(self.bundle, "0" * 64)
        with self.assertRaisesRegex(ValueError, "new_bundle_directory_required"):
            pipeline.create_bundle(ROOT, ROOT / "configs/data.json", self.bundle)

    def test_real_dvc_accepts_recipe_and_declared_dependencies(self):
        pipeline.run_command(["dvc", "init", "--no-scm", "-q"], cwd=self.workspace)
        shutil.copyfile(ROOT / "dvc.yaml", self.workspace / "dvc.yaml")
        graph = pipeline.run_command(["dvc", "dag", "--dot"], cwd=self.workspace)
        self.assertIn("validate", graph)
        self.assertIn("evaluate", graph)

    def test_manifest_paths_and_symlinks_cannot_escape_root(self):
        for relative in ("../secret", "/tmp/secret", "data/../../secret", ""):
            with self.subTest(relative=relative), self.assertRaisesRegex(ValueError, "unsafe_artifact_path"):
                pipeline.safe_path(self.workspace, relative)
        (self.workspace / "link").symlink_to(self.workspace.parent, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "unsafe_artifact_path"):
            pipeline.safe_path(self.workspace, "link/secret")

    def test_actual_pointer_data_and_metadata_are_bound(self):
        binding = pipeline.check_binding(self.workspace)
        self.assertEqual(set(binding["workspace_pointers"]), {"train", "validation", "split_lists"})
        path = self.workspace / "data/train.jsonl"
        path.write_bytes(path.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "pointer_data_hash_mismatch"):
            pipeline.check_binding(self.workspace)

    def test_pointer_and_receipt_tampering_rejected_without_reserved_bytes(self):
        pointer = self.workspace / "data/split.json.dvc"
        original = pointer.read_text()
        pointer.write_text(original.replace("md5:", "md5: 0 #"))
        with self.assertRaisesRegex(ValueError, "invalid_dvc_pointer"):
            pipeline.check_binding(self.workspace)
        pointer.write_text(original)
        receipt = self.workspace / "admission/data-admission.json"
        receipt.write_bytes(receipt.read_bytes() + b"\n")
        with self.assertRaisesRegex(ValueError, "dataset_binding_mismatch"):
            pipeline.check_binding(self.workspace)

    def test_prepare_materializes_consumed_bytes_without_reserved_reads(self):
        original = Path.read_bytes
        reserved = {"test.jsonl", "source.jsonl", "corpus.jsonl", "split.json"}

        def guarded(path):
            if path.name in reserved:
                raise AssertionError("reserved bytes opened")
            return original(path)

        with patch("ats_mlops.pipeline.context", return_value=({"git_sha": "a" * 40}, {})), \
                patch.object(Path, "read_bytes", guarded):
            result = pipeline.stage("prepare", self.workspace, ROOT)
        self.assertEqual(result["outcome"], "passed")
        prepared = self.workspace / "artifacts/prepared"
        self.assertEqual((prepared / "data_manifest.json").read_bytes(), (self.workspace / "data_manifest.json").read_bytes())
        self.assertEqual((prepared / "admission.json").read_bytes(), (self.workspace / "admission/data-admission.json").read_bytes())
        for name in ("train", "validation"):
            self.assertEqual((prepared / "data" / (name + ".jsonl")).read_bytes(),
                             (self.workspace / "data" / (name + ".jsonl")).read_bytes())
        with patch("ats_mlops.pipeline.context", return_value=({}, {})), \
                self.assertRaisesRegex(ValueError, "new_stage_output_required"):
            pipeline.stage("prepare", self.workspace, ROOT)

    def test_actual_stage_binding_failure_never_fits_real_estimators(self):
        (self.workspace / "data/train.jsonl").write_bytes(b"invalid\n")
        with patch("ats_mlops.pipeline.context", return_value=({}, {})), \
                patch.object(Pipeline, "fit") as pipeline_fit, \
                patch.object(LogisticRegression, "fit") as classifier_fit:
            with self.assertRaisesRegex(ValueError, "pointer_data_hash_mismatch"):
                pipeline.stage("train", self.workspace, ROOT)
            pipeline_fit.assert_not_called()
            classifier_fit.assert_not_called()

    def test_mutable_params_snapshots_are_validated_and_budget_stays_twenty(self):
        original = pipeline.parameters(self.workspace, ROOT)
        self.assertEqual(original["search"]["n_trials"], 20)
        params = pipeline.read_json(self.workspace / "params.yaml")
        params.update(model={"classifier": {"C": .25}}, smoke=True)
        write_json(self.workspace / "params.yaml", params)
        changed = pipeline.parameters(self.workspace, ROOT)
        self.assertEqual(changed["model"]["classifier"]["C"], .25)
        self.assertTrue(changed["smoke"])
        self.assertEqual(changed["search"]["n_trials"], 20)
        params["search"] = {"n_trials": 2}
        write_json(self.workspace / "params.yaml", params)
        with self.assertRaisesRegex(ValueError, "invalid_search_config"):
            pipeline.parameters(self.workspace, ROOT)
        params["search"] = {"unknown": 1}
        write_json(self.workspace / "params.yaml", params)
        with self.assertRaisesRegex(ValueError, "unknown_parameter"):
            pipeline.parameters(self.workspace, ROOT)

    def test_swapped_prepared_snapshots_rejected_by_actual_stage_before_fit(self):
        with patch("ats_mlops.pipeline.context", return_value=({"git_sha": "a" * 40}, {})):
            pipeline.stage("prepare", self.workspace, ROOT)
        prepared = self.workspace / "artifacts/prepared"
        for name in ("model.json", "search.json", "data_manifest.json", "admission.json"):
            path = prepared / name
            original = path.read_bytes()
            document = json.loads(original)
            if name == "model.json":
                document["seed"] += 1
            elif name == "search.json":
                document["sampler_seed"] += 1
            elif name == "data_manifest.json":
                document["generator"]["config"]["namespace"] = "foreign-valid-bundle"
            else:
                document["manifest_sha256"] = "0" * 64
            path.write_bytes(canonical_json(document))
            with self.subTest(snapshot=name), \
                    patch("ats_mlops.pipeline.context", return_value=({"git_sha": "a" * 40}, {})), \
                    patch.object(Pipeline, "fit") as pipeline_fit, \
                    patch.object(LogisticRegression, "fit") as classifier_fit:
                with self.assertRaisesRegex(ValueError, "prepared_snapshot_mismatch"):
                    pipeline.stage("train", self.workspace, ROOT)
                pipeline_fit.assert_not_called()
                classifier_fit.assert_not_called()
            path.write_bytes(original)

    def test_cache_classification_uses_execution_events_not_restored_run_ids(self):
        self.assertEqual(pipeline.classify_repro(0, {}), "cache_hit")
        self.assertEqual(pipeline.classify_repro(0, {"evaluate": {}}), "cache_hit")
        self.assertEqual(pipeline.classify_repro(0, {"train": {"mlflow": {"run_id": "new"}}}), "new_training")
        self.assertEqual(pipeline.classify_repro(1, {"train": {}}), "failed")
        with patch.dict("os.environ", {"ATS_PIPELINE_INVOCATION": "inv-123"}):
            pipeline.execution_event(self.workspace, "train", {"mlflow": {"run_id": "new"}})
        self.assertEqual(pipeline.read_json(self.workspace / ".local/executions/inv-123/train.json")["mlflow"]["run_id"], "new")
        with patch.dict("os.environ", {"ATS_PIPELINE_INVOCATION": "../../escape"}), \
                self.assertRaisesRegex(ValueError, "invalid_invocation_id"):
            pipeline.execution_event(self.workspace, "train", {})

    def test_separate_source_and_real_import_binding_are_required(self):
        with self.assertRaisesRegex(ValueError, "separate_sibling_source_required"):
            pipeline.context(self.workspace, self.workspace)
        source = self.workspace.parent / "source"
        source.mkdir()
        write_json(self.workspace / "pipeline-inputs.json", {"runtime_python": sys.executable})
        with self.assertRaisesRegex(ValueError, "runtime_source_checkout_mismatch"):
            pipeline.context(self.workspace, source)
        write_json(self.workspace / "pipeline-inputs.json", {"runtime_python": "/wrong/python"})
        with self.assertRaisesRegex(ValueError, "pinned_interpreter_required"):
            pipeline.context(self.workspace, source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
