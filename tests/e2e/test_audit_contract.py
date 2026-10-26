"""Pure/sandbox tests of the auditor, never evidence for actual T18 acceptance."""
from copy import deepcopy
from pathlib import Path
import tempfile
import os
import subprocess
import sys
import unittest

from tests.e2e.audit import (Artifacts, EvidenceError, SCENARIOS, canonical, compare_recomputations,
    decode, derive_holdout_lineage, digest, execution, inspect_scenarios, validate_freeze)

SOURCE = "a" * 40
SPEC = "b" * 64


def save(root, name, value):
    path = root / name;path.parent.mkdir(parents=True, exist_ok=True)
    content = canonical(value);path.write_bytes(content)
    return {"path": name, "sha256": digest(content)}


def receipt():
    return {"schema_version": 1, "outcome": "passed", "execution_kind": "actual_acceptance", "evidence_scope": "demo_only",
            "source_git_sha": SOURCE, "spec_sha256": SPEC, "started_at": "2026-10-26T00:00:00Z", "finished_at": "2026-10-26T00:00:01Z",
            "scenario": "S2", "name": SCENARIOS["S2"][0], "commands": [{"id": "run-1", "argv": ["dvc", "repro", "-f", "--no-run-cache"],
            "exit_code": 0, "expected_exit_code": 0, "stdout": {"path": "stdout.txt", "sha256": digest(b"executed")},
            "stderr": {"path": "stderr.txt", "sha256": digest(b"")}}]}


def replicas(root):
    result = []
    params = {"seed": 20261026, "classifier": {"C": 1.0}, "features": {"max_features": 2000}}
    for index in range(3):
        base = f"replica-{index}/"
        identity = {"run_id": str(index) * 32, "model_version": str(index + 1), "model_name": "ats-pair-ranking"}
        run = {"mlflow": identity, "git_sha": SOURCE, "dirty_worktree": False, "test_read": False,
               "read_partitions": ["train", "validation"], "model": {"sha256": "c" * 64}, "params": params,
               "data_manifest_sha256": "d" * 64, "data_sha256": {"train": "e" * 64, "validation": "f" * 64},
               "environment": {"image": {"id": "sha256:" + "1" * 64}}}
        run_ref = save(root, base + "run.json", run)
        selection = {"kind": "frozen", "outcome": "selected", "new_trials": 0, "selected_config": params, "data_manifest_sha256": "d" * 64}
        pipeline = {"outcome": "passed", "test_read": False, "training": {"mlflow": identity, "run_manifest_sha256": run_ref["sha256"], "model_sha256": "c" * 64},
                    "selection": {"kind": "frozen", "receipt": selection, "receipt_sha256": digest(canonical(selection))},
                    "inputs": {"prepared_files": {"train.jsonl": "e" * 64, "validation.jsonl": "f" * 64}}}
        result.append({"workspace_id": f"fresh-{index}", "run": run_ref,
            "pipeline": save(root, base + "pipeline.json", pipeline),
            "repro": save(root, base + "repro.json", {"outcome": "new_training", "command": ["dvc", "repro", "-f", "--no-run-cache"],
                "events": {"validate": {}, "prepare": {}, "train": {"mlflow": identity}, "evaluate": {}}}),
            "features": save(root, base + "features.json", {"idf": [1., 2.], "vocabulary": {"x": 0, "y": 1}, "transformed_sha256": "2" * 64}),
            "scores": save(root, base + "scores.json", {"rows": [{"vacancy_id": "v", "candidate_id": "c", "score": .5}]}),
            "metrics": save(root, base + "metrics.json", {"status": "defined", "ndcg_at_10": 1., "recall_at_10": 1.,
                "groups": [{"ranking": [{"candidate_id": "c"}]}]})})
    return result


class ArtifactTests(unittest.TestCase):
    def test_no_evidence_final_gate_fails_instead_of_skip(self):
        environment = {key: value for key, value in os.environ.items() if key not in ("ATS_ACCEPTANCE_FREEZE", "ATS_ACCEPTANCE_FREEZE_SHA256")}
        result = subprocess.run([sys.executable, "-B", "-m", "unittest", "tests.e2e.test_acceptance"],
                                capture_output=True, text=True, env=environment)
        self.assertNotEqual(0, result.returncode)
        self.assertIn("acceptance_not_run", result.stderr)
        self.assertNotIn("skipped", result.stderr)

    def test_hash_json_types_symlink_and_reserved_byte_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory);artifacts = Artifacts(root)
            reference = save(root, "report.json", {"outcome": "passed"})
            self.assertEqual("passed", artifacts.document(reference)["outcome"])
            for path in ("../outside", "/outside", "./report.json", "a/../report.json", "a\\report.json", "test.jsonl"):
                with self.subTest(path=path), self.assertRaises(EvidenceError):artifacts.content(dict(reference, path=path))
            (root / "link.json").symlink_to(root / "report.json")
            with self.assertRaisesRegex(EvidenceError, "symlink"):artifacts.content(dict(reference, path="link.json"))
            (root / "report.json").write_bytes(b"{}")
            with self.assertRaisesRegex(EvidenceError, "hash_mismatch"):artifacts.document(reference)
        for content in (b'{"x":0,"x":1}', b'{"x":NaN}', b'not-json'):
            with self.assertRaises(EvidenceError):decode(content)

    def test_marker_skip_stale_source_and_bad_exit_never_pass(self):
        original = receipt();execution(original, SOURCE, SPEC)
        for key, value in (("outcome", "skipped"), ("execution_kind", "unit"), ("source_git_sha", "c" * 40),
                           ("spec_sha256", "d" * 64), ("commands", []), ("schema_version", True)):
            changed = deepcopy(original);changed[key] = value
            with self.subTest(key=key), self.assertRaises(EvidenceError):execution(changed, SOURCE, SPEC)
        changed = deepcopy(original);changed["commands"][0]["exit_code"] = 77
        with self.assertRaises(EvidenceError):execution(changed, SOURCE, SPEC)

    def test_missing_native_adapters_and_receipts_block_matrix_and_freeze(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = inspect_scenarios(root, {}, source_sha=SOURCE, spec_sha=SPEC, validators={})
            self.assertEqual("blocked", result["outcome"])
            self.assertEqual(8, len(result["scenarios"]))
            self.assertTrue(all(row["outcome"] == "blocked" for row in result["requirements"].values()))
            (root / "stdout.txt").write_bytes(b"executed");(root / "stderr.txt").write_bytes(b"")
            reference = save(root, "S2.json", receipt())
            result = inspect_scenarios(root, {"S2": reference}, source_sha=SOURCE, spec_sha=SPEC, validators={})
            self.assertEqual("blocked", result["scenarios"]["S2"]["outcome"])
            frozen = save(root, "freeze.json", {"outcome": "passed", "checks": dict.fromkeys(SCENARIOS, True)})
            with self.assertRaises(EvidenceError):validate_freeze(root / "freeze.json", frozen["sha256"])


class RecomputeAuditTests(unittest.TestCase):
    def test_three_numeric_snapshots_checked_not_binary_model_equality(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory);values = replicas(root)
            result = compare_recomputations(Artifacts(root), values, source_sha=SOURCE)
            self.assertEqual(3, result["replicas"])
            for mutation in ("missing", "cached", "same_workspace", "unforced", "score", "rank", "features", "bool_score", "nonfinite"):
                values = replicas(root)
                if mutation == "missing":values = values[:2]
                elif mutation == "same_workspace":values[2]["workspace_id"] = values[0]["workspace_id"]
                else:
                    name = "repro" if mutation in ("cached", "unforced") else "scores" if mutation in ("score", "bool_score", "nonfinite") else "metrics" if mutation == "rank" else "features"
                    path = root / values[2][name]["path"];document = decode(path.read_bytes())
                    if mutation == "cached":document["outcome"] = "cache_hit"
                    if mutation == "unforced":document["command"] = ["dvc", "repro"]
                    if mutation == "score":document["rows"][0]["score"] = .8
                    if mutation == "bool_score":document["rows"][0]["score"] = True
                    if mutation == "nonfinite":
                        path.write_bytes(b'{"rows":[{"vacancy_id":"v","candidate_id":"c","score":NaN}]}')
                        values[2][name]["sha256"] = digest(path.read_bytes())
                    else:
                        if mutation == "rank":document["groups"][0]["ranking"][0]["candidate_id"] = "other"
                        if mutation == "features":document["idf"][0] = 1.1
                        values[2][name] = save(root, values[2][name]["path"], document)
                with self.subTest(mutation=mutation), self.assertRaises(EvidenceError):compare_recomputations(Artifacts(root), values, source_sha=SOURCE)


class HoldoutMetadataTests(unittest.TestCase):
    def test_semantic_guard_does_not_reset_for_child_namespace_or_candidate(self):
        from ats_mlops.data.reservation import root_policy
        root = root_policy()  # hashed reservation metadata only; no test row read
        first = derive_holdout_lineage(root)
        child = deepcopy(root);child.update(policy_id="different-namespace", parent=root, parent_sha256=digest(canonical(root)), anchor=None, documents=[])
        self.assertEqual(first, derive_holdout_lineage(child))
        self.assertEqual(first["holdout_lineage_sha256"], digest(canonical({key: first[key] for key in ("schema_version", "root_policy_sha256", "protected_test_documents_sha256")})))
        root["documents"][0]["tokens"][0] = "0" * 64
        with self.assertRaises(ValueError):derive_holdout_lineage(root)


if __name__ == "__main__":
    unittest.main()
