"""Hand-computed ranking, strict full-score contract and scoped validation IO."""

from copy import deepcopy
import hashlib
import json
import math
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.data import generate_dataset
from ats_mlops.evaluation.baseline import main, validation_report
from ats_mlops.evaluation.ranking import evaluate_ranking
from ats_mlops.model.baseline import jaccard_score, score_rows


ROOT = Path(__file__).resolve().parents[2]


def row(candidate, relevance=1, vacancy="v", text="python"):
    return {"vacancy_id": vacancy, "candidate_id": candidate, "relevance": relevance,
            "vacancy_text": "python sql", "candidate_text": text}


def without_timing(report):
    report = deepcopy(report)
    report.pop("elapsed_seconds")
    return report


class RankingMetricsTests(unittest.TestCase):
    def test_hand_calculation_binary_gain_with_positive_outside_cutoff(self):
        rows = [row(f"c{i:02}", int(i in (1, 3, 10))) for i in range(11)]
        scores = {("v", item["candidate_id"]): 11 - i for i, item in enumerate(rows)}
        actual = evaluate_ranking(rows, scores)
        # Relevant at one-based ranks 2, 4, 11: the last is outside top 10.
        expected = (1 / math.log2(3) + 1 / math.log2(5)) / (1 + 1 / math.log2(3) + .5)
        self.assertEqual(actual["ndcg_at_10"], expected)
        self.assertEqual(actual["recall_at_10"], 2 / 3)
        self.assertEqual(actual["eligible_group_count"], 1)
        self.assertEqual(actual["groups"][0]["k"], 10)

    def test_jaccard_shared_unicode_tokenizer_and_empty_union(self):
        self.assertEqual(jaccard_score("ＰＹＴＨＯＮ, SQL python", "sql java"), 1 / 3)
        self.assertEqual(jaccard_score("Кот кот", "КОТ"), 1)
        self.assertEqual(jaccard_score("", "!!!"), 0)
        self.assertEqual(jaccard_score("python", ""), 0)
        with self.assertRaises(ValueError):
            jaccard_score(None, "python")

    def test_baseline_ranking_ties_and_input_shuffle_are_deterministic(self):
        rows = [row("z", 0), row("a", 1), row("b", 1, text="java")]
        expected = without_timing(evaluate_ranking(rows, score_rows(rows)))
        self.assertEqual([item["candidate_id"] for item in expected["groups"][0]["ranking"]], ["a", "z", "b"])
        for seed in range(10):
            random.Random(seed).shuffle(rows)
            self.assertEqual(without_timing(evaluate_ranking(rows, score_rows(rows))), expected)

    def test_macro_uses_identical_eligible_groups_and_counts_exclusions(self):
        rows = [row("a", 1, "good"), row("a", 0, "zero"),
                row("a", 0, "bad"), row("b", 1, "bad")]
        scores = {(item["vacancy_id"], item["candidate_id"]): 0 for item in rows}
        actual = evaluate_ranking(rows, scores, group_ids=["zero", "good", "empty", "bad"])
        self.assertEqual(actual["eligible_group_ids"], ["bad", "good"])
        self.assertEqual(actual["ndcg_at_10"], (1 + 1 / math.log2(3)) / 2)
        self.assertEqual(actual["recall_at_10"], 1)
        self.assertEqual(actual["group_count"], 4)
        self.assertEqual(actual["unevaluated_group_count"], 2)
        self.assertEqual(actual["zero_positive_group_count"], 2)
        self.assertEqual(actual["zero_positive_group_fraction"], .5)
        self.assertEqual(actual["empty_group_count"], 1)
        self.assertTrue(all(item["k"] == min(10, item["candidate_count"]) for item in actual["groups"]))

    def test_no_eligible_group_is_undefined_and_json_null(self):
        for rows, groups in (([], []), ([], ["empty"]), ([row("a", 0)], ["v"])):
            with self.subTest(groups=groups):
                actual = evaluate_ranking(rows, score_rows(rows), group_ids=groups)
                self.assertEqual(actual["status"], "undefined")
                self.assertIsNone(actual["ndcg_at_10"])
                self.assertIsNone(actual["recall_at_10"])
                self.assertIn('"ndcg_at_10": null', json.dumps(actual, allow_nan=False))

    def test_cutoff_recall_uses_all_positives_and_short_groups(self):
        for size in (1, 2, 10, 12):
            rows = [row(f"c{i:02}") for i in range(size)]
            actual = evaluate_ranking(rows, score_rows(rows))
            self.assertEqual(actual["ndcg_at_10"], 1)
            self.assertEqual(actual["recall_at_10"], min(10, size) / size)

    def test_duplicate_pair_is_rejected_by_scorer_and_evaluator(self):
        rows = [row("a"), row("a")]
        for call in (lambda: score_rows(rows), lambda: evaluate_ranking(rows, {("v", "a"): 1})):
            with self.assertRaisesRegex(ValueError, "duplicate_pair"):
                call()

    def test_invalid_and_nonfinite_scores_are_rejected_even_in_excluded_groups(self):
        for invalid in (float("nan"), float("inf"), -float("inf"), "1", None, True, 1j, 10 ** 1000):
            with self.subTest(score=invalid), self.assertRaisesRegex(ValueError, "finite_numbers"):
                evaluate_ranking([row("a", 0)], {("v", "a"): invalid})

    def test_missing_extra_or_wrong_score_keys_cannot_change_cohort(self):
        for scores in ({}, {("v", "a"): 1, ("v", "extra"): 1}, {("other", "a"): 1}):
            with self.subTest(scores=scores), self.assertRaisesRegex(ValueError, "score_keys"):
                evaluate_ranking([row("a")], scores)

    def test_invalid_labels_and_group_inventory_are_rejected(self):
        for invalid in (-1, 2, .5, "1", True, None):
            with self.subTest(label=invalid), self.assertRaisesRegex(ValueError, "invalid_relevance"):
                evaluate_ranking([row("a", invalid)], {("v", "a"): 1})
        for groups in (["v", "v"], [""], ["other"]):
            with self.subTest(groups=groups), self.assertRaises(ValueError):
                evaluate_ranking([row("a")], {("v", "a"): 1}, group_ids=groups)
        with self.assertRaisesRegex(ValueError, "invalid_pair_id"):
            evaluate_ranking([row("")], {("v", ""): 1})


class BaselineReportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = self.root / "manifest.json"
        generate_dataset(ROOT / "tests/fixtures/data/ci-config.json", self.manifest, self.root / "data")

    def test_validation_report_never_opens_test_or_full_corpus(self):
        original = Path.open
        seen = []

        def guarded(path, *args, **kwargs):
            seen.append(path.name)
            if path.name in ("test.jsonl", "corpus.jsonl", "source.jsonl"):
                raise AssertionError(f"forbidden data read: {path}")
            return original(path, *args, **kwargs)

        (self.root / "data/test.jsonl").unlink()
        with patch.object(Path, "open", guarded):
            report = validation_report(self.manifest)
        self.assertEqual(report["metrics"]["status"], "defined")
        self.assertEqual(report["evidence_scope"], "demo_only")
        self.assertEqual(report["read_partitions"], ["train", "validation"])
        self.assertFalse(report["test_read"])
        self.assertIn("train.jsonl", seen)
        self.assertIn("validation.jsonl", seen)
        self.assertEqual(report["manifest_sha256"], hashlib.sha256(self.manifest.read_bytes()).hexdigest())
        for name in ("train", "validation"):
            self.assertEqual(report["data_sha256"][name], hashlib.sha256((self.root / f"data/{name}.jsonl").read_bytes()).hexdigest())
        self.assertEqual(report["source_sha256"]["src/ats_mlops/model/baseline.py"],
                         hashlib.sha256((ROOT / "src/ats_mlops/model/baseline.py").read_bytes()).hexdigest())

    def test_corrupt_validation_bytes_are_rejected_before_scoring(self):
        with (self.root / "data/validation.jsonl").open("a") as stream:
            stream.write("\n")
        with patch("ats_mlops.evaluation.baseline.score_rows") as scorer:
            self.assertEqual(main(["--manifest", str(self.manifest), "--output", str(self.root / "out.json")]), 1)
            scorer.assert_not_called()
        self.assertFalse((self.root / "out.json").exists())

    def test_corrupt_split_metadata_is_rejected_before_scoring(self):
        with (self.root / "data/split.json").open("a") as stream:
            stream.write("\n")
        with patch("ats_mlops.evaluation.baseline.score_rows") as scorer:
            self.assertEqual(main(["--manifest", str(self.manifest), "--output", str(self.root / "out.json")]), 1)
            scorer.assert_not_called()
        self.assertFalse((self.root / "out.json").exists())

    def test_cli_writes_report_and_undefined_is_nonzero(self):
        output = self.root / "out.json"
        self.assertEqual(main(["--manifest", str(self.manifest), "--output", str(output)]), 0)
        report = json.loads(output.read_text())
        report["metrics"].update(status="undefined", ndcg_at_10=None, recall_at_10=None)
        with patch("ats_mlops.evaluation.baseline.validation_report", return_value=report):
            self.assertEqual(main(["--manifest", str(self.manifest), "--output", str(output)]), 2)
        self.assertIsNone(json.loads(output.read_text())["metrics"]["ndcg_at_10"])


if __name__ == "__main__":
    unittest.main()
