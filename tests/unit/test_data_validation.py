"""Real pinned-GX acceptance checks, privacy and the training admission boundary."""

from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

from ats_mlops.cli import main
from ats_mlops.data.dataset import audit_manifest, load_partition, canonical_json, generate_dataset, prepare_dataset, generate_rows, sha256
from ats_mlops.data.validate import ValidationError, require_valid, validate_dataset


ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "tests/fixtures/data/ci-config.json"


class DataValidationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = self.root / "manifest.json"
        self.reports = self.root / "reports"
        generate_dataset(CONFIG, self.manifest, self.root / "data")
        self.document = json.loads(self.manifest.read_text())
        self.receipt = self.reports / "data-admission.json"

    def rewrite(self, name, rows):
        content = b"".join(canonical_json(row) for row in rows)
        (self.root / self.document["artifacts"][name]["path"]).write_bytes(content)
        self.document["artifacts"][name]["sha256"] = sha256(content)
        self.manifest.write_bytes(canonical_json(self.document))

    def rows(self, name):
        return [json.loads(line) for line in (self.root / self.document["artifacts"][name]["path"]).read_text().splitlines()]

    def admission(self):
        return validate_dataset(self.manifest, self.reports)

    def test_real_gx_checkpoint_passes_and_persists_private_reports(self):
        report = self.admission()
        self.assertEqual(report["outcome"], "passed", report["reasons"])
        self.assertEqual(report["engine_version"], "1.24.1")
        self.assertEqual([batch["scope"] for batch in report["gx"]], ["source", "train", "validation", "test"])
        self.assertTrue(all(len(batch["checks"]) == 17 for batch in report["gx"]))
        self.assertEqual(report, json.loads(self.receipt.read_text()))
        fingerprint = report.pop("report_sha256")
        self.assertEqual(fingerprint, sha256(canonical_json(report)))
        text = self.receipt.read_text() + (self.reports / "data-admission.md").read_text()
        self.assertNotIn(self.rows("train")[0]["candidate_text"], text)
        self.assertNotIn("unexpected_list", text)

    def test_invalid_source_fixtures_have_concrete_reasons_before_cleaning(self):
        originals = self.rows("source")
        cases = json.loads((ROOT / "tests/fixtures/invalid/cases.json").read_text())
        cases += [{"name": name, "field": "relevance", "value": value, "reason": "invalid_relevance"}
                  for name, value in (("nan", float("nan")), ("inf", float("inf")), ("neg_inf", -float("inf")))]
        for case in cases:
            with self.subTest(case=case["name"]):
                rows = deepcopy(originals)
                if "remove" in case:
                    del rows[0][case["remove"]]
                elif case.get("duplicate"):
                    rows.append(deepcopy(rows[0]))
                else:
                    rows[0][case["field"]] = case["value"]
                self.rewrite("source", rows)
                report = self.admission()
                self.assertEqual(report["outcome"], "rejected")
                self.assertIn(case["reason"], report["reasons"])
                self.assertFalse(report["gx"][0]["success"])

    def test_admission_rejects_duplicate_source_even_when_t3_cleaning_deduplicated_it(self):
        config = json.loads(CONFIG.read_text())
        rows = generate_rows(config)
        prepare_dataset(rows + [deepcopy(rows[0])], config, self.root / "data", self.manifest)
        self.assertIn("duplicate_pair", self.admission()["reasons"])

    def test_split_intersection_and_mismatched_partition_fail(self):
        self.document["splits"]["validation"]["candidate_ids"].append(
            self.document["splits"]["train"]["candidate_ids"][0])
        self.manifest.write_bytes(canonical_json(self.document))
        self.assertIn("split_leakage", self.admission()["reasons"])
        self.document["splits"]["validation"]["candidate_ids"].pop()
        rows = self.rows("train")
        rows[0]["candidate_id"] = "mismatched-candidate"
        rows.sort(key=lambda row: (row["vacancy_id"], row["candidate_id"]))
        self.rewrite("train", rows)
        self.assertIn("partition_inventory_mismatch", self.admission()["reasons"])

    def test_training_preflight_reads_only_requested_partitions_and_returns_checked_rows(self):
        self.assertEqual(self.admission()["outcome"], "passed")
        original_bytes = Path.read_bytes
        original_text = Path.read_text
        accesses = []

        def guard(path, method, *args, **kwargs):
            accesses.append(path.name)
            if path.name in {"test.jsonl", "source.jsonl", "corpus.jsonl", "split.json"}:
                raise AssertionError("preflight opened a reserved artifact")
            return method(path, *args, **kwargs)

        with patch.object(Path, "read_bytes", lambda path, *a, **k: guard(path, original_bytes, *a, **k)), \
                patch.object(Path, "read_text", lambda path, *a, **k: guard(path, original_text, *a, **k)):
            rows = require_valid(self.manifest, self.reports, admission_report=self.receipt)
        self.assertEqual(set(rows), {"train", "validation"})
        self.assertEqual(rows["train"], self.rows("train"))
        self.assertIn("train.jsonl", accesses)
        self.assertIn("validation.jsonl", accesses)

    def test_training_requires_complete_bound_untampered_admission_receipt(self):
        with self.assertRaisesRegex(ValidationError, "admission_receipt_required"):
            require_valid(self.manifest, self.reports, admission_report=self.receipt)
        self.admission()
        original = json.loads(self.receipt.read_text())
        for mutation in ("manifest", "checks", "outcome", "fingerprint"):
            with self.subTest(mutation=mutation):
                receipt = deepcopy(original)
                if mutation == "manifest":
                    receipt["manifest_sha256"] = "0" * 64
                elif mutation == "checks":
                    receipt["gx"][0]["checks"] = []
                elif mutation == "outcome":
                    receipt["outcome"] = "rejected"
                else:
                    receipt["report_sha256"] = "0" * 64
                # Rehash incomplete/incorrect receipts: fingerprint alone is insufficient.
                if mutation != "fingerprint":
                    receipt.pop("report_sha256")
                    receipt["report_sha256"] = sha256(canonical_json(receipt))
                self.receipt.write_bytes(canonical_json(receipt))
                with self.assertRaisesRegex(ValidationError, "admission_receipt_mismatch"):
                    require_valid(self.manifest, self.reports, admission_report=self.receipt)

    def test_raw_training_duplicates_are_checked_before_partition_cleaning(self):
        self.admission()
        rows = self.rows("train")
        self.rewrite("train", rows + [deepcopy(rows[0])])
        # Bind the receipt to the modified manifest to reach the per-partition GX
        # guard independently of the prior admission binding guard.
        receipt = json.loads(self.receipt.read_text())
        receipt["manifest_sha256"] = sha256(self.manifest.read_bytes())
        receipt.pop("report_sha256")
        receipt["report_sha256"] = sha256(canonical_json(receipt))
        self.receipt.write_bytes(canonical_json(receipt))
        with self.assertRaisesRegex(ValidationError, "duplicate_pair"):
            require_valid(self.manifest, self.reports, admission_report=self.receipt)

    def test_critical_preflight_raises_before_caller_can_invoke_training(self):
        trainer = Mock()
        with self.assertRaises(ValidationError):
            rows = require_valid(self.manifest, self.reports, admission_report=self.receipt)
            trainer.fit(rows["train"])
        trainer.fit.assert_not_called()
        # T7 additionally tests this boundary against the concrete fit stage.

    def test_cli_negative_exit_and_json_and_readable_reports_in_fresh_process(self):
        rows = self.rows("source")
        rows.append(deepcopy(rows[0]))
        self.rewrite("source", rows)
        process = subprocess.run([sys.executable, "-m", "ats_mlops.cli", "data", "validate",
                                  "--manifest", str(self.manifest), "--report-dir", str(self.reports)],
                                 capture_output=True, text=True)
        self.assertEqual(process.returncode, 1, process.stderr)
        report = json.loads(process.stdout)
        self.assertIn("duplicate_pair", report["reasons"])
        self.assertTrue((self.reports / "data-admission.md").is_file())
        self.assertNotIn(rows[0]["candidate_text"], process.stdout + process.stderr)

    def test_cli_positive_exit_and_training_mode(self):
        with redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(["data", "validate", "--manifest", str(self.manifest),
                                   "--report-dir", str(self.reports)]), 0)
        self.assertEqual(json.loads(output.getvalue())["outcome"], "passed")
        with redirect_stdout(io.StringIO()):
            self.assertEqual(main(["data", "validate", "--mode", "training", "--manifest", str(self.manifest),
                                   "--report-dir", str(self.reports), "--admission-report", str(self.receipt)]), 0)

    def test_manifest_swap_between_hash_and_load_cannot_use_old_receipt(self):
        self.assertEqual(self.admission()["outcome"], "passed")
        original_read = Path.read_bytes
        swapped = False

        def swap_after_hash(path):
            nonlocal swapped
            content = original_read(path)
            if path == self.manifest and not swapped:
                swapped = True
                document = json.loads(content)
                document["cleaning"]["input_rows"] += 1
                path.write_bytes(canonical_json(document))
            return content

        with patch.object(Path, "read_bytes", swap_after_hash):
            with self.assertRaisesRegex(ValidationError, "manifest_changed_during_validation"):
                require_valid(self.manifest, self.reports, admission_report=self.receipt)
        self.assertTrue(swapped)

    def test_late_manifest_swap_during_admission_cannot_create_passed_receipt(self):
        def swap_after_audit(path):
            result = audit_manifest(path)
            document = json.loads(path.read_text())
            document["cleaning"]["input_rows"] += 1
            path.write_bytes(canonical_json(document))
            return result

        with patch("ats_mlops.data.validate.audit_manifest", swap_after_audit):
            report = self.admission()
        self.assertEqual(report["outcome"], "rejected")
        self.assertIn("manifest_changed_during_validation", report["reasons"])
        self.assertEqual(json.loads(self.receipt.read_text())["outcome"], "rejected")

    def test_late_manifest_swap_during_training_cannot_return_rows(self):
        self.assertEqual(self.admission()["outcome"], "passed")

        def swap_after_load(path, partition, **kwargs):
            rows = load_partition(path, partition, **kwargs)
            if partition == "validation":
                document = json.loads(path.read_text())
                document["cleaning"]["input_rows"] += 1
                path.write_bytes(canonical_json(document))
            return rows

        with patch("ats_mlops.data.validate.load_partition", swap_after_load):
            with self.assertRaisesRegex(ValidationError, "manifest_changed_during_validation"):
                require_valid(self.manifest, self.reports, admission_report=self.receipt)

    def test_gx_engine_failure_is_fail_closed_and_does_not_echo_exception_data(self):
        with patch("ats_mlops.data.validate._gx_validate", side_effect=RuntimeError("private-text-sentinel")):
            report = self.admission()
        self.assertEqual(report["outcome"], "rejected")
        self.assertIn("gx_execution_failed", report["reasons"])
        self.assertNotIn("private-text-sentinel", self.receipt.read_text())


if __name__ == "__main__":
    unittest.main()
