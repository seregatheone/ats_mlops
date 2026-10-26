"""Semantic holdout protection; no training, scoring or original test file reads."""

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from ats_mlops.data.dataset import (
    DataError, _summary, canonical_json, clean_rows, connected_components, generate_dataset,
    generate_rows, generator_signature, load_manifest, sha256, split_rows,
)
from ats_mlops.data.reservation import (
    LEGACY_GENERATOR_SHA256, ROOT_POLICY_SHA256, ROW_GENERATOR_SHA256, Registry,
    extend_policy, assignments, root_policy, token_hashes, validate_partitions,
)
from ats_mlops.data.validate import ValidationError, require_valid

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / "tests/fixtures/data/ci-config.json"


class ReservationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original = json.loads((ROOT / "data_manifest.json").read_text())
        cls.config = cls.original["generator"]["config"]
        cls.rows = clean_rows(generate_rows(cls.config))[0]
        cls.parts, cls.components, _ = split_rows(cls.rows, cls.config["seed"], cls.config["split_ratios"], .9)

    def test_original_canonical_all_three_partition_bytes_are_preserved(self):
        for name, rows in self.parts.items():
            self.assertEqual(sha256(b"".join(canonical_json(row) for row in rows)),
                             self.original["artifacts"][name]["sha256"])
        self.assertEqual(generator_signature(), ROW_GENERATOR_SHA256)
        self.assertEqual(sha256(canonical_json(root_policy())), ROOT_POLICY_SHA256)
        self.assertEqual(len(Registry(root_policy()).documents), 1260)

    def test_namespace_arbitrary_ids_seed_and_labels_cannot_move_protected_rows(self):
        expected = {token_hashes(row["candidate_text"]): name for name, part in self.parts.items() for row in part}
        renamed = deepcopy(self.rows)
        vacancies = {value: f"new-v{index}" for index, value in enumerate(sorted({r["vacancy_id"] for r in renamed}))}
        for index, row in enumerate(renamed):
            row.update(vacancy_id=vacancies[row["vacancy_id"]], candidate_id=f"new-c{index}", relevance=1 - row["relevance"])
        parts, _, _ = split_rows(clean_rows(renamed)[0], self.config["seed"] + 7, self.config["split_ratios"], .9)
        self.assertEqual({token_hashes(row["candidate_text"]): name for name, part in parts.items() for row in part}, expected)
        namespace = {**self.config, "namespace": "t9-v1"}
        renamed_parts, _, _ = split_rows(clean_rows(generate_rows(namespace))[0], namespace["seed"], namespace["split_ratios"], .9)
        self.assertEqual({token_hashes(row["candidate_text"]): name for name, part in renamed_parts.items() for row in part}, expected)

    def test_bridge_between_protected_folds_and_weaker_threshold_fail_closed(self):
        rows = deepcopy(self.rows)
        index = rows.index(self.parts["train"][0])
        rows[index]["candidate_text"] = self.parts["test"][0]["candidate_text"]
        # Conflicting protected documents are joined by the vacancy/candidate pair.
        with self.assertRaisesRegex(DataError, "reservation_bridge_conflict"):
            split_rows(clean_rows(rows)[0], self.config["seed"], self.config["split_ratios"], .9)
        with self.assertRaisesRegex(DataError, "reservation_threshold_weakened"):
            split_rows(self.rows, self.config["seed"], self.config["split_ratios"], .91)

    def test_unrelated_ci_components_and_explicit_parent_extension(self):
        config = json.loads(CI.read_text())
        rows = clean_rows(generate_rows(config))[0]
        parts, components, _ = split_rows(rows, config["seed"], config["split_ratios"], .9)
        assigned, _ = assignments(rows, components, config["seed"], config["split_ratios"], .9, root_policy())
        policy = extend_policy(root_policy(), rows, components, assigned)
        self.assertEqual(policy["parent_sha256"], ROOT_POLICY_SHA256)
        self.assertEqual(validate_partitions(parts, .9, policy)["semantic_components"], 9)
        renamed = [{**row, "vacancy_id": "x-" + row["vacancy_id"], "candidate_id": "x-" + row["candidate_id"]} for row in rows]
        actual, _, _ = split_rows(renamed, 99, config["split_ratios"], .9, reservation_policy=policy)
        self.assertEqual({token_hashes(r["candidate_text"]): n for n, p in parts.items() for r in p},
                         {token_hashes(r["candidate_text"]): n for n, p in actual.items() for r in p})
        changed = deepcopy(policy)
        changed["parent"]["policy_id"] = "tampered"
        with self.assertRaises(DataError):
            Registry(changed)

    def test_near_duplicate_extension_cannot_override_parent_fold(self):
        root = root_policy()
        document = root["documents"][0]
        tokens = sorted([*document["tokens"], sha256(b"new-near-token")])
        self.assertGreaterEqual(len(document["tokens"]) / len(tokens), .9)
        child = {**root, "policy_id": "bad-extension", "anchor": None, "parent": root,
                 "parent_sha256": ROOT_POLICY_SHA256,
                 "documents": [{"tokens": tokens, "fold": "test" if document["fold"] != "test" else "train"}]}
        with self.assertRaisesRegex(DataError, "reservation_parent_conflict"):
            Registry(child)

    def test_legacy_manifest_remains_readable_but_cannot_cross_training_boundary(self):
        trainer = Mock()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "data_manifest.json"
            legacy = deepcopy(self.original)
            legacy["generator"]["source_sha256"] = LEGACY_GENERATOR_SHA256
            path.write_bytes(canonical_json(legacy))
            self.assertEqual(load_manifest(path), legacy)
            with patch("ats_mlops.data.validate._gx_validate", side_effect=AssertionError("no GX needed")):
                with self.assertRaisesRegex(ValidationError, "reservation_metadata_required"):
                    rows = require_valid(path, Path(temporary) / "reports", admission_report=Path(temporary) / "absent.json")
                    trainer.fit(rows["train"])
            trainer.fit.assert_not_called()

    def test_training_guard_reads_only_train_validation_and_hash_policy_metadata(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            generate_dataset(CI, manifest, root / "data")
            receipt = root / "receipt.json"
            receipt.write_text("{}")
            original_read = Path.read_bytes
            accessed = []

            def guarded(path):
                accessed.append(path.name)
                if path.name in {"test.jsonl", "source.jsonl", "corpus.jsonl", "split.json"}:
                    raise AssertionError("reserved body read")
                return original_read(path)

            # These are structural guard probes. Pinned real GX is covered separately.
            with patch.object(Path, "read_bytes", guarded), \
                    patch("ats_mlops.data.validate._valid_receipt", return_value=True), \
                    patch("ats_mlops.data.validate._gx_validate", side_effect=lambda rows, scope: {
                        "scope": scope, "rows": len(rows), "success": True, "checks": []}):
                rows = require_valid(manifest, root / "reports", admission_report=receipt)
            self.assertEqual(set(rows), {"train", "validation"})
            self.assertIn("reservation_policy.json", accessed)
            report = json.loads((root / "reports/data-training.json").read_text())
            self.assertEqual(report["reservation"]["checked_partitions"], ["train", "validation"])

    def test_rebound_partition_with_protected_test_text_still_fails_before_fit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            generate_dataset(CI, manifest, root / "data")
            document = json.loads(manifest.read_text())
            path = root / document["artifacts"]["train"]["path"]
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows[0]["candidate_text"] = self.parts["test"][0]["candidate_text"]
            content = b"".join(canonical_json(row) for row in rows)
            path.write_bytes(content)
            document["artifacts"]["train"]["sha256"] = sha256(content)
            components, _ = connected_components(rows, .9)
            document["splits"]["train"] = _summary(rows, components)
            manifest.write_bytes(canonical_json(document))
            receipt = root / "receipt.json"
            receipt.write_text("{}")
            trainer = Mock()
            with patch("ats_mlops.data.validate._valid_receipt", return_value=True), \
                    patch("ats_mlops.data.validate._gx_validate", side_effect=lambda rows, scope: {
                        "scope": scope, "rows": len(rows), "success": True, "checks": []}):
                with self.assertRaisesRegex(ValidationError, "reservation_partition_violation"):
                    admitted = require_valid(manifest, root / "reports", admission_report=receipt)
                    trainer.fit(admitted["train"])
            trainer.fit.assert_not_called()

    def test_policy_cannot_alias_reserved_body_even_with_matching_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest = root / "manifest.json"
            generate_dataset(CI, manifest, root / "data")
            document = json.loads(manifest.read_text())
            document["reservation"]["policy"] = document["artifacts"]["test"]
            manifest.write_bytes(canonical_json(document))
            with self.assertRaisesRegex(DataError, "reservation_policy_invalid"):
                load_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
