"""Data admission, duplicate-component boundaries and the frozen-test contract."""

from copy import deepcopy
import json
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.cli import main
from ats_mlops.data.dataset import (
    DataError, audit_manifest, canonical_json, clean_rows, connected_components,
    generate_dataset, generate_rows, load_manifest, load_partition, pair_id,
    prepare_dataset, sha256, split_rows, text_tokens,
)


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests/fixtures/data"


class SplitTests(unittest.TestCase):
    def setUp(self):
        self.config_path = FIXTURES / "ci-config.json"
        self.config = json.loads(self.config_path.read_text())
        self.rows = generate_rows(self.config)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = self.root / "manifest.json"

    def generate(self):
        return generate_dataset(self.config_path, self.manifest, self.root / "data")

    def test_generation_and_all_artifacts_are_byte_deterministic(self):
        first = self.generate()
        original = {path.relative_to(self.root): path.read_bytes() for path in self.root.rglob("*") if path.is_file()}
        self.assertEqual(first, self.generate())
        self.assertEqual(original, {path.relative_to(self.root): path.read_bytes() for path in self.root.rglob("*") if path.is_file()})
        self.assertEqual(first["duplicates"]["components"], 9)
        self.assertEqual(sum(part["rows"] for part in first["counts"].values()), 108)
        self.assertTrue(all(not value for counts in first["intersections"].values() for value in counts.values()))

    def test_disjoint_ci_namespace_and_seed_from_research_corpus(self):
        research = generate_rows(json.loads((ROOT / "configs/data.json").read_text()))
        for key in ("vacancy_id", "candidate_id"):
            self.assertFalse({row[key] for row in research} & {row[key] for row in self.rows})
        self.assertNotEqual(self.config["seed"], json.loads((ROOT / "configs/data.json").read_text())["seed"])
        _, counts = connected_components(research + self.rows, .9)
        self.assertEqual(counts["components"], 69)

    def test_split_is_independent_of_source_row_order(self):
        cleaned, _ = clean_rows(self.rows)
        original, _, _ = split_rows(cleaned, self.config["seed"], self.config["split_ratios"], .9)
        shuffled = deepcopy(self.rows)
        random.Random(99).shuffle(shuffled)
        actual, _, _ = split_rows(clean_rows(shuffled)[0], self.config["seed"], self.config["split_ratios"], .9)
        self.assertEqual(original, actual)

    def test_shared_candidate_joins_vacancy_groups_transitively(self):
        rows = deepcopy(self.rows)
        for group in (1, 2):
            index = group * self.config["candidates_per_group"]
            rows[index]["candidate_id"] = rows[0]["candidate_id"]
            rows[index]["candidate_text"] = rows[0]["candidate_text"]
        cleaned, _ = clean_rows(rows)
        _, components, counts = split_rows(cleaned, 2, self.config["split_ratios"], .9)
        self.assertEqual(len({components[pair_id(rows[index])] for index in (0, 12, 24)}), 1)
        self.assertEqual(counts["components"], 7)

    def test_duplicate_cases_case_punctuation_order_and_near_threshold(self):
        cases = json.loads((FIXTURES / "duplicate-cases.json").read_text())
        self.assertEqual(text_tokens(cases["base"]), text_tokens(cases["same_tokens"]))
        rows = [{"vacancy_id": f"v{index}", "candidate_id": f"c{index}",
                 "vacancy_text": f"uniquevacancy{index}", "candidate_text": cases[key], "relevance": index % 2}
                for index, key in enumerate(("base", "same_tokens", "near", "different"))]
        components, counts = connected_components(rows, .9)
        self.assertEqual(len({components[pair_id(row)] for row in rows[:3]}), 1)
        self.assertNotEqual(components[pair_id(rows[0])], components[pair_id(rows[3])])
        self.assertGreater(counts["exact_token_duplicate_pairs"], 0)
        self.assertGreater(counts["near_duplicate_pairs"], 0)

    def test_near_duplicates_form_transitive_components(self):
        cases = json.loads((FIXTURES / "duplicate-cases.json").read_text())
        rows = [{"vacancy_id": f"v{index}", "candidate_id": f"c{index}",
                 "vacancy_text": f"uniquevacancy{index}", "candidate_text": cases[key], "relevance": index % 2}
                for index, key in enumerate(("transitive_a", "transitive_b", "transitive_c"))]
        components, counts = connected_components(rows, .9)
        self.assertEqual(counts["near_duplicate_pairs"], 2)
        self.assertEqual(len(set(components.values())), 1)

    def test_duplicate_text_across_entity_kinds_is_connected(self):
        rows = deepcopy(self.rows[:2])
        rows[1]["vacancy_id"] = "new-vacancy"
        rows[1]["vacancy_text"] = rows[0]["candidate_text"]
        components, _ = connected_components(rows, .9)
        self.assertEqual(len(set(components.values())), 1)

    def test_degenerate_shared_entity_corpus_is_rejected(self):
        rows = deepcopy(self.rows)
        for row in rows:
            row["candidate_id"] = "one-shared-candidate"
            row["candidate_text"] = "single shared experience"
        with self.assertRaisesRegex(DataError, "insufficient_independent_components"):
            split_rows(rows, 1, self.config["split_ratios"], .9)
        rows = deepcopy(self.rows)
        for row in rows:
            row["candidate_text"] = "single shared experience"
        with self.assertRaisesRegex(DataError, "insufficient_independent_components"):
            split_rows(rows, 1, self.config["split_ratios"], .9)

    def test_missing_class_or_nonviable_groups_are_rejected(self):
        rows = deepcopy(self.rows)
        for row in rows:
            row["relevance"] = 1
        with self.assertRaisesRegex(DataError, "missing_class"):
            split_rows(rows, 1, self.config["split_ratios"], .9)
        rows = [self.rows[index] for index in range(0, len(self.rows), 12)]
        for index, row in enumerate(rows):
            row["relevance"] = index % 2
        with self.assertRaises(DataError):
            split_rows(rows, 1, self.config["split_ratios"], .9)

    def test_malformed_rows_and_conflicting_identities_are_rejected(self):
        invalid = json.loads((FIXTURES / "invalid-relevance.jsonl").read_text())
        cases = [invalid, {**self.rows[0], "relevance": True}, {**self.rows[0], "candidate_text": " !!! "},
                 {**self.rows[0], "vacancy_id": "contains/contact@example.com"}, {**self.rows[0], "unknown": "field"}]
        for row in cases:
            with self.subTest(row=row), self.assertRaises(DataError):
                clean_rows([row])
        with self.assertRaisesRegex(DataError, "conflicting_entity_text"):
            clean_rows([self.rows[0], {**self.rows[0], "candidate_text": "changed content"}])
        with self.assertRaisesRegex(DataError, "conflicting_pair"):
            clean_rows([self.rows[0], {**self.rows[0], "relevance": 0}])

    def test_cleaning_deduplicates_identical_pairs_and_whitespace(self):
        cleaned, counts = clean_rows([self.rows[0], {**self.rows[0], "candidate_text": "  " + self.rows[0]["candidate_text"] + "\n"}])
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(counts["identical_pair_rows_removed"], 1)

    def test_admission_replays_source_cleaning_receipt(self):
        source = self.rows + [deepcopy(self.rows[0])]
        report = prepare_dataset(source, self.config, self.root / "data", self.manifest)
        self.assertEqual(report["cleaning"], {"input_rows": 109, "retained_rows": 108,
                                              "identical_pair_rows_removed": 1})
        self.assertEqual(audit_manifest(self.manifest), report)

    def test_hash_corruption_and_manifest_tampering_are_rejected(self):
        self.generate()
        document = json.loads(self.manifest.read_text())
        test = self.root / document["artifacts"]["test"]["path"]
        original = test.read_bytes()
        test.write_bytes(original + b"\n")
        with self.assertRaisesRegex(DataError, "artifact_hash_mismatch"):
            audit_manifest(self.manifest)
        # Even changing both stored hashes cannot hide changed test bytes/content.
        document["artifacts"]["test"]["sha256"] = sha256(test.read_bytes())
        document["frozen_test"]["sha256"] = sha256(test.read_bytes())
        self.manifest.write_bytes(canonical_json(document))
        with self.assertRaisesRegex(DataError, "test_content_mismatch"):
            audit_manifest(self.manifest)

    def test_split_inventory_overlap_and_path_escape_are_rejected(self):
        self.generate()
        document = json.loads(self.manifest.read_text())
        document["splits"]["test"]["candidate_ids"].append(document["splits"]["train"]["candidate_ids"][0])
        self.manifest.write_bytes(canonical_json(document))
        with self.assertRaisesRegex(DataError, "split_leakage"):
            load_manifest(self.manifest)
        self.generate()  # same test freeze; restores deterministic metadata
        document = json.loads(self.manifest.read_text())
        document["artifacts"]["train"]["path"] = "../outside.jsonl"
        self.manifest.write_bytes(canonical_json(document))
        with self.assertRaisesRegex(DataError, "artifact_outside_manifest_directory"):
            load_partition(self.manifest, "train")

    def test_test_freeze_cannot_be_overwritten_by_new_seed(self):
        self.generate()
        original = self.manifest.read_bytes()
        changed = {**self.config, "seed": self.config["seed"] + 1}
        path = self.root / "changed-config.json"
        path.write_bytes(canonical_json(changed))
        with self.assertRaisesRegex(DataError, "frozen_test_change_requires_new_dataset_version"):
            generate_dataset(path, self.manifest, self.root / "data")
        self.assertEqual(original, self.manifest.read_bytes())

    def test_partition_loader_never_reads_other_partition_bytes(self):
        self.generate()
        original = Path.read_bytes
        accessed = []

        def guarded(path):
            accessed.append(path.name)
            if path.name in {"test.jsonl", "validation.jsonl", "corpus.jsonl", "source.jsonl", "split.json"}:
                raise AssertionError("training touched another partition")
            return original(path)

        with patch.object(Path, "read_bytes", guarded):
            rows = load_partition(self.manifest, "train")
        self.assertEqual(len(rows), 84)
        self.assertIn("train.jsonl", accessed)
        with self.assertRaisesRegex(DataError, "test_reserved_for_final_evaluation"):
            load_partition(self.manifest, "test")
        self.assertGreater(len(load_partition(self.manifest, "test", final_evaluation=True)), 0)

    def test_cli_audit_failure_returns_nonzero(self):
        self.generate()
        document = json.loads(self.manifest.read_text())
        document["frozen_test"]["sha256"] = "0" * 64
        self.manifest.write_bytes(canonical_json(document))
        self.assertEqual(main(["data", "audit", "--manifest", str(self.manifest)]), 1)


if __name__ == "__main__":
    unittest.main()
