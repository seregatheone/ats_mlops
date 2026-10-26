"""Gate policy and adversarial structural fixtures; no training or registry writes."""

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.data.dataset import canonical_json, generate_dataset, sha256
from ats_mlops.data.validate import validate_dataset
from ats_mlops.lineage.manifest import tree_inventory
from ats_mlops.model.baseline import score_rows
from ats_mlops.quality.artifacts import digest, strict_inventory, validation_snapshot
from ats_mlops.quality.gate import run_gate
from ats_mlops.quality.policy import assess_scores, comparison, GateError

ROOT = Path(__file__).resolve().parents[2]


def write(path, document):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json(document))


def binding_for(root, manifest):
    pointers = {}
    for name in ("train", "validation", "split_lists"):
        relative = manifest["artifacts"][name]["path"]
        data = (root / relative).read_bytes()
        md5 = hashlib.md5(data).hexdigest()
        pointer = root / (relative + ".dvc")
        pointer.write_text(f"outs:\n- md5: {md5}\n  size: {len(data)}\n  hash: md5\n  path: {Path(relative).name}\n")
        pointers[name] = {"artifact_path": relative, "pointer_path": relative + ".dvc",
                          "pointer_sha256": digest(pointer), "md5": md5, "size": len(data), "sha256": sha256(data)}
    return {"schema_version": 1, "evidence_scope": "demo_only", "manifest_sha256": digest(root / "manifest.json"),
            "admission_sha256": digest(root / "admission/data-admission.json"), "workspace_pointers": pointers}


class GateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.template = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.template.cleanup)
        root = Path(cls.template.name)
        generate_dataset(ROOT / "tests/fixtures/data/ci-config.json", root / "manifest.json", root / "data")
        assert validate_dataset(root / "manifest.json", root / "admission")["outcome"] == "passed"
        manifest = json.loads((root / "manifest.json").read_text())
        write(root / "dataset-binding.json", binding_for(root, manifest))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        shutil.copytree(self.template.name, self.root, dirs_exist_ok=True)
        self.manifest = json.loads((self.root / "manifest.json").read_text())
        self.binding = json.loads((self.root / "dataset-binding.json").read_text())
        self.snapshot = validation_snapshot(self.root / "manifest.json", self.root / "admission/data-admission.json",
                                            self.root / "dataset-binding.json")
        self.request = {"validation_manifest": str(self.root / "manifest.json"),
                        "admission_report": str(self.root / "admission/data-admission.json"),
                        "dataset_binding": str(self.root / "dataset-binding.json"),
                        "thresholds": str(ROOT / "configs/quality.yaml"),
                        "candidate": self.model_fixture("candidate"), "incumbent": None}

    def model_fixture(self, name, *, older=False):
        """Hash-consistent structural fixture, deliberately not a runnable pickle."""
        directory = self.root / name
        artifact = directory / "model"
        artifact.mkdir(parents=True)
        shutil.copytree(ROOT / "src/ats_mlops", artifact / "code/ats_mlops",
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"))
        record = json.loads((ROOT / "reports/recompute/b/run_manifest.json").read_text())
        pipeline = json.loads((ROOT / "reports/recompute/b/pipeline_manifest.json").read_text())
        source = tree_inventory(artifact / "code/ats_mlops")
        record.update({"source_sha256": source, "source_tree_sha256": sha256(canonical_json(source)),
                       "data_manifest_sha256": self.snapshot["manifest_sha256"],
                       "data_sha256": {part: self.manifest["artifacts"][part]["sha256"] for part in ("train", "validation")},
                       "split_sha256": self.snapshot["split_sha256"],
                       "partition_inventory_sha256": {part: sha256(canonical_json(self.manifest["splits"][part]))
                                                      for part in ("train", "validation")}})
        record["reports_sha256"]["admission"] = self.snapshot["admission_sha256"]
        binding = deepcopy(self.binding)
        if older:
            record["data_manifest_sha256"] = binding["manifest_sha256"] = "a" * 64
            record["reports_sha256"]["admission"] = binding["admission_sha256"] = "b" * 64
            record["split_sha256"] = binding["workspace_pointers"]["split_lists"]["sha256"] = "c" * 64
            for index, part in enumerate(("train", "validation")):
                record["data_sha256"][part] = binding["workspace_pointers"][part]["sha256"] = str(index + 1) * 64
                record["partition_inventory_sha256"][part] = str(index + 3) * 64
            record["mlflow"].update(model_version="4", exact_uri="models:/ats-pair-ranking/4", run_id="1" * 32)
        embedded = {key: value for key, value in record.items() if key not in {"model", "mlflow", "reports_sha256"}}
        write(artifact / "extra_files/input-provenance.json", embedded)
        for filename in ("MLmodel", "registered_model_meta", "model.pkl"):
            (artifact / filename).write_bytes(b"unit structural fixture; never executed\n")
        pipeline["inputs"]["binding"] = binding
        pipeline["inputs"]["prepared_manifest_sha256"] = record["data_manifest_sha256"]
        prepared = pipeline["inputs"]["prepared_files"]
        prepared["data_manifest.json"] = record["data_manifest_sha256"]
        prepared["admission.json"] = binding["admission_sha256"]
        for part in ("train", "validation"):
            prepared[binding["workspace_pointers"][part]["artifact_path"]] = record["data_sha256"][part]
        spec = {"model_dir": str(artifact), "run_manifest": str(directory / "run.json"),
                "pipeline_manifest": str(directory / "pipeline.json")}
        self.refresh(spec, record=record, pipeline=pipeline)
        if older:
            spec.update(release_id="fixture-historical-release", release_is_fixture=True)
        return spec

    def refresh(self, spec, *, record=None, pipeline=None):
        record = record or json.loads(Path(spec["run_manifest"]).read_text())
        pipeline = pipeline or json.loads(Path(spec["pipeline_manifest"]).read_text())
        files = strict_inventory(Path(spec["model_dir"]))
        record["model"] = {"files": files, "sha256": sha256(canonical_json(files)), "payload_sha256": files["model.pkl"]}
        for key in ("git_sha", "dirty_worktree", "source_sha256", "source_tree_sha256", "environment", "dvc_pointers"):
            pipeline["source"][key] = record[key]
        write(Path(spec["run_manifest"]), record)
        pipeline["training"].update(run_manifest_sha256=digest(Path(spec["run_manifest"])),
                                    model_sha256=record["model"]["sha256"], mlflow=record["mlflow"])
        write(Path(spec["pipeline_manifest"]), pipeline)

    @staticmethod
    def scorer(model, rows):
        return score_rows(rows), {"fixture": True, "model_sha256": model["record"]["model"]["sha256"]}

    def gate(self, scorer=None):
        return run_gate(self.request, self.root / "gate.json", scorer=scorer or self.scorer)

    def test_first_release_passes_and_report_is_bound_text_free_and_immutable(self):
        result = self.gate()
        self.assertEqual(result["outcome"], "passed")
        self.assertTrue(result["first_release"])
        self.assertIsNone(result["expected_incumbent"])
        self.assertEqual(result["candidate"]["model_version"], "5")
        self.assertEqual(result["validation"]["split_sha256"], self.snapshot["split_sha256"])
        self.assertEqual(result["comparisons"]["baseline"]["ndcg_drop"], 0)
        self.assertNotIn("vacancy_text", (self.root / "gate.json").read_text())
        with self.assertRaisesRegex(GateError, "new_gate_report_required"):
            self.gate()

    def test_incumbent_with_older_training_data_is_rescored_on_current_snapshot(self):
        self.request["incumbent"] = self.model_fixture("older", older=True)
        calls = []
        def scorer(model, rows):
            calls.append(rows)
            return self.scorer(model, rows)
        result = self.gate(scorer)
        self.assertEqual(result["outcome"], "passed")
        self.assertIs(calls[0], calls[1])
        self.assertFalse(result["first_release"])
        self.assertTrue(result["expected_incumbent"]["release_is_fixture"])
        self.assertEqual(set(result["comparisons"]), {"baseline", "incumbent"})

    def test_bad_candidate_rejected_against_baseline_and_incumbent(self):
        self.request["incumbent"] = self.model_fixture("older", older=True)
        def scorer(model, rows):
            scores, receipt = self.scorer(model, rows)
            if model["public"]["model_version"] == "5":
                scores = {key: -value for key, value in scores.items()}
            return scores, receipt
        result = self.gate(scorer)
        self.assertEqual(result["outcome"], "rejected")
        self.assertFalse(result["comparisons"]["baseline"]["passed"])
        self.assertFalse(result["comparisons"]["incumbent"]["passed"])

    def test_threshold_boundary_is_inclusive_without_extra_tolerance(self):
        self.assertTrue(comparison(.995, 1.0, .005)["passed"])
        self.assertEqual(comparison(.995, 1.0, .005)["ndcg_drop"], .005)
        self.assertFalse(comparison(.9949999999, 1.0, .005)["passed"])

    def test_incumbent_comparison_can_reject_while_baseline_passes(self):
        rows = [{"vacancy_id": "v", "candidate_id": "positive", "vacancy_text": "python",
                 "candidate_text": "other", "relevance": 1},
                {"vacancy_id": "v", "candidate_id": "negative", "vacancy_text": "python",
                 "candidate_text": "python", "relevance": 0}]
        candidate = score_rows(rows)
        incumbent = {("v", "positive"): 1.0, ("v", "negative"): 0.0}
        result = assess_scores(rows, ["v"], candidate, incumbent)
        self.assertEqual(result["outcome"], "rejected")
        self.assertTrue(result["comparisons"]["baseline"]["passed"])
        self.assertFalse(result["comparisons"]["incumbent"]["passed"])
        self.assertEqual(result["reasons"], ["candidate.ndcg_drop_against_incumbent"])

    def test_invalid_scores_rejected_in_both_evidence_scopes(self):
        rows = self.snapshot["rows"]
        good = score_rows(rows)
        first = next(iter(good))
        invalids = [{}, {**good, ("extra", "candidate"): .2}]
        invalids += [{**good, first: value} for value in (float("nan"), float("inf"), -float("inf"), True)]
        for scope in ("demo_only", "independently_labeled"):
            for scores in invalids:
                with self.subTest(scope=scope):
                    result = assess_scores(rows, self.snapshot["groups"], scores, evidence_scope=scope)
                    self.assertEqual(result["outcome"], "rejected")
                    self.assertEqual(result["evidence_scope"], scope)

    def test_zero_eligible_groups_never_pass(self):
        rows = [{**row, "relevance": 0} for row in self.snapshot["rows"]]
        result = assess_scores(rows, self.snapshot["groups"], score_rows(rows))
        self.assertEqual(result["outcome"], "rejected")
        self.assertIsNone(result["metrics"]["candidate"]["ndcg_at_10"])
        self.assertEqual(result["metrics"]["candidate"]["eligible_group_count"], 0)

    def test_gate_reads_validation_and_pointer_metadata_only(self):
        forbidden = {self.root / item["path"] for name, item in self.manifest["artifacts"].items()
                     if name != "validation"}
        for path in forbidden:
            path.unlink()
        original = Path.open
        def guarded(path, *args, **kwargs):
            if path in forbidden:
                raise AssertionError("reserved data read")
            return original(path, *args, **kwargs)
        with patch.object(Path, "open", guarded):
            self.assertEqual(self.gate()["outcome"], "passed")

    def test_tampered_validation_and_pointer_binding_rejected_before_scoring(self):
        with (self.root / self.manifest["artifacts"]["validation"]["path"]).open("ab") as stream:
            stream.write(b"\n")
        with patch.object(self, "scorer") as scorer:
            self.assertEqual(self.gate(scorer)["outcome"], "rejected")
            scorer.assert_not_called()

    def test_wrong_authoritative_pointer_hash_is_rejected(self):
        path = Path(self.request["candidate"]["pipeline_manifest"])
        pipeline = json.loads(path.read_text())
        pipeline["inputs"]["binding"]["workspace_pointers"]["validation"]["pointer_sha256"] = "0" * 64
        write(path, pipeline)
        self.assertEqual(self.gate()["outcome"], "rejected")

    def test_changed_split_binding_is_rejected(self):
        path = Path(self.request["candidate"]["pipeline_manifest"])
        pipeline = json.loads(path.read_text())
        pipeline["inputs"]["binding"]["workspace_pointers"]["split_lists"]["sha256"] = "0" * 64
        write(path, pipeline)
        self.assertEqual(self.gate()["outcome"], "rejected")

    def test_missing_lineage_and_alias_are_rejected(self):
        path = Path(self.request["candidate"]["run_manifest"])
        record = json.loads(path.read_text())
        record["mlflow"]["model_version"] = "latest"
        write(path, record)
        result = self.gate()
        self.assertEqual(result["outcome"], "rejected")
        self.assertIn("candidate.exact_numeric_model_version_required", result["reasons"])

    def test_missing_manifest_field_is_rejected(self):
        path = Path(self.request["candidate"]["run_manifest"])
        record = json.loads(path.read_text())
        del record["source_sha256"]
        write(path, record)
        self.assertEqual(self.gate()["outcome"], "rejected")

    def test_tampered_payload_is_rejected(self):
        (Path(self.request["candidate"]["model_dir"]) / "model.pkl").write_bytes(b"changed")
        self.assertEqual(self.gate()["outcome"], "rejected")

    def test_rehashed_bundled_code_does_not_bypass_source_inventory(self):
        artifact = Path(self.request["candidate"]["model_dir"])
        path = artifact / "code/ats_mlops/features/pairs.py"
        path.write_text(path.read_text() + "\n# changed\n")
        self.refresh(self.request["candidate"])
        result = self.gate()
        self.assertIn("candidate.bundled_source_mismatch", result["reasons"])

    def test_cached_bytecode_is_rejected_even_though_legacy_inventory_ignores_it(self):
        artifact = Path(self.request["candidate"]["model_dir"])
        (artifact / "code/ats_mlops/rogue.pyc").write_bytes(b"bytecode")
        self.assertNotIn("code/ats_mlops/rogue.pyc", tree_inventory(artifact))
        result = self.gate()
        self.assertIn("candidate.artifact_bytecode_forbidden", result["reasons"])

    def test_symlink_file_is_rejected(self):
        artifact = Path(self.request["candidate"]["model_dir"])
        (artifact / "code/ats_mlops/linked.py").symlink_to(artifact / "code/ats_mlops/__init__.py")
        self.assertIn("candidate.artifact_symlink_forbidden", self.gate()["reasons"])

    def test_symlink_directory_and_empty_pycache_are_rejected(self):
        artifact = Path(self.request["candidate"]["model_dir"])
        cache = artifact / "code/ats_mlops/__pycache__"
        cache.mkdir()
        with self.assertRaisesRegex(GateError, "artifact_bytecode_forbidden"):
            strict_inventory(artifact)
        cache.rmdir()
        (artifact / "linked-directory").symlink_to(artifact / "code", target_is_directory=True)
        with self.assertRaisesRegex(GateError, "artifact_symlink_forbidden"):
            strict_inventory(artifact)

    def test_changed_threshold_cannot_relax_policy(self):
        policy = self.root / "changed-policy.yaml"
        write(policy, {"schema_version": 1, "metric": "ndcg_at_10", "max_ndcg_drop": .1,
                       "recall_policy": "report_only"})
        self.request["thresholds"] = str(policy)
        self.assertEqual(self.gate()["outcome"], "rejected")


if __name__ == "__main__":
    unittest.main()
