"""Deterministic objective, bounded failure and admitted-row boundary checks."""

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.data.dataset import generate_dataset
from ats_mlops.data.validate import require_valid, validate_dataset, ValidationError
from ats_mlops.evaluation.ranking import evaluate_ranking
from ats_mlops.model.scoring import score_rows
from ats_mlops.model.training import fit_rows
from ats_mlops.model.tune import main, run_study, selected_config, tune, validate_search

ROOT = Path(__file__).resolve().parents[2]
CONFIG = json.loads((ROOT / "configs/model.json").read_text())
SEARCH = json.loads((ROOT / "configs/search.json").read_text())


def rows():
    return [{"vacancy_id": "v", "candidate_id": str(index), "vacancy_text": "python sql",
             "candidate_text": "python sql" if index % 2 else "design illustration",
             "relevance": index % 2} for index in range(12)]


class TuningTests(unittest.TestCase):
    def test_main_configuration_has_twenty_sequential_seeded_trials(self):
        validate_search(SEARCH)
        self.assertEqual(SEARCH["n_trials"], 20)
        self.assertEqual(SEARCH["n_jobs"], 1)
        self.assertEqual(SEARCH["sampler_seed"], 20261026)
        report = run_study(rows(), rows(), ["v", "empty"], CONFIG, SEARCH)
        self.assertEqual(report["attempted_trials"], 20)
        self.assertEqual(report["outcome"], "passed")
        self.assertFalse(report["smoke_override"])
        self.assertEqual(report["eligible_group_ids"], ["v"])

    def test_same_seed_same_trials_and_tie_selects_lowest_number(self):
        first = run_study(rows(), rows(), ["v"], CONFIG, SEARCH, smoke=True)
        second = run_study(rows(), rows(), ["v"], CONFIG, SEARCH, smoke=True)
        self.assertEqual(first["trials"], second["trials"])
        self.assertEqual(first["best"], second["best"])
        self.assertEqual(first["best"]["number"], 0)
        self.assertEqual(first["effective_budget"], 2)
        self.assertEqual(first["configured_budget"], 20)
        self.assertEqual(SEARCH["n_trials"], 20)

    def test_objective_is_actual_validation_ndcg_using_fresh_train_fits(self):
        train = rows()
        validation = deepcopy(train)
        validation[0]["candidate_text"] = "validationonly"
        with patch("ats_mlops.model.tune.fit_rows", wraps=fit_rows) as fitted:
            report = run_study(train, validation, ["v", "empty"], CONFIG, SEARCH, smoke=True)
        self.assertEqual(fitted.call_count, 2)
        for trial in report["trials"]:
            model = fit_rows(train, selected_config(CONFIG, trial["params"]))
            expected = evaluate_ranking(validation, score_rows(model, validation), group_ids=["v", "empty"])
            self.assertEqual(trial["value"], expected["ndcg_at_10"])
            self.assertNotIn("validationonly", model.named_steps["pair_features"].vectorizer_.vocabulary_)
        self.assertIsNot(fitted.call_args_list[0].args[1], fitted.call_args_list[1].args[1])
        self.assertEqual(CONFIG["classifier"]["C"], 1.0)
        self.assertEqual(report["selected_config"]["seed"], CONFIG["seed"])

    def test_failed_trial_is_saved_sanitized_and_never_selected(self):
        actual_fit = fit_rows(rows(), CONFIG)
        with patch("ats_mlops.model.tune.fit_rows", side_effect=[ValueError("PRIVATE RAW TEXT"), actual_fit]):
            report = run_study(rows(), rows(), ["v"], CONFIG, SEARCH, smoke=True)
        self.assertEqual(report["trials"][0]["state"], "FAIL")
        self.assertIsNone(report["trials"][0]["value"])
        self.assertEqual(report["trials"][0]["error_code"], "trial_fit_or_evaluation_failed")
        self.assertEqual(report["best"]["number"], 1)
        self.assertNotIn("PRIVATE", json.dumps(report))

    def test_all_failed_and_undefined_trials_never_produce_best(self):
        with patch("ats_mlops.model.tune.fit_rows", side_effect=RuntimeError("PRIVATE")):
            failed = run_study(rows(), rows(), ["v"], CONFIG, SEARCH, smoke=True)
        undefined_rows = rows()
        for row in undefined_rows:
            row["relevance"] = 0
        undefined = run_study(rows(), undefined_rows, ["v"], CONFIG, SEARCH, smoke=True)
        for report in (failed, undefined):
            self.assertEqual(report["outcome"], "failed")
            self.assertEqual(report["reason"], "no_complete_trials")
            self.assertIsNone(report["best"])
            self.assertIsNone(report["selected_config"])
        self.assertEqual(undefined["trials"][0]["error_code"], "undefined_or_invalid_objective")

    def test_nonfinite_or_changed_cohort_objective_is_failed(self):
        for value, cohort in ((float("nan"), ["v"]), (float("inf"), ["v"]), (0.9, ["other"])):
            actual = evaluate_ranking
            calls = 0

            def evaluate(*args, **kwargs):
                nonlocal calls
                calls += 1
                report = actual(*args, **kwargs)
                if calls > 1:
                    report.update(ndcg_at_10=value, eligible_group_ids=cohort)
                return report

            with patch("ats_mlops.model.tune.evaluate_ranking", side_effect=evaluate):
                report = run_study(rows(), rows(), ["v"], CONFIG, SEARCH, smoke=True)
            self.assertEqual(report["outcome"], "failed")
            self.assertIsNone(report["best"])

    def test_timeout_empty_study_cannot_report_success(self):
        with patch("optuna.study.Study.optimize") as optimize:
            report = run_study(rows(), rows(), ["v"], CONFIG, SEARCH)
        self.assertEqual(optimize.call_args.kwargs["n_jobs"], 1)
        self.assertEqual(optimize.call_args.kwargs["n_trials"], 20)
        self.assertEqual(report["reason"], "incomplete_budget")
        self.assertIsNone(report["best"])

    def test_invalid_budget_space_timeout_rejected_before_fit(self):
        configurations = []
        for key, value in (("n_trials", 2), ("n_trials", 0), ("n_trials", True), ("n_jobs", 2),
                           ("sampler_seed", True), ("timeout_seconds", -1), ("timeout_seconds", float("inf"))):
            config = deepcopy(SEARCH)
            config[key] = value
            configurations.append(config)
        for choices in ([], [True], [0], [float("nan")], [1, 1], [1.5]):
            config = deepcopy(SEARCH)
            config["space"]["features.max_features"] = choices
            configurations.append(config)
        with patch("ats_mlops.model.tune.fit_rows") as fitted:
            for config in configurations:
                with self.subTest(config=config), self.assertRaises(ValueError):
                    run_study(rows(), rows(), ["v"], CONFIG, config)
            fitted.assert_not_called()

    def test_stage_guard_rejects_before_study(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            manifest = temp / "manifest.json"
            generate_dataset(ROOT / "tests/fixtures/data/ci-config.json", manifest, temp / "data")
            with patch("ats_mlops.model.tune.source_environment", return_value={}), \
                    patch("ats_mlops.model.tune.run_study") as study:
                with self.assertRaises(ValidationError):
                    tune(manifest, temp / "reports", admission_report=temp / "missing",
                         config_path=ROOT / "configs/model.json", search_path=ROOT / "configs/search.json",
                         source_root=ROOT, image_identity={})
                study.assert_not_called()

    def test_stage_uses_bound_rows_once_after_all_partition_files_disappear(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            manifest = temp / "manifest.json"
            generate_dataset(ROOT / "tests/fixtures/data/ci-config.json", manifest, temp / "data")
            self.assertEqual(validate_dataset(manifest, temp / "admission")["outcome"], "passed")
            receipt = temp / "admission/data-admission.json"
            document = json.loads(manifest.read_text())

            def consume(*args, **kwargs):
                admitted = require_valid(*args, **kwargs)
                for artifact in document["artifacts"].values():
                    (temp / artifact["path"]).unlink(missing_ok=True)
                return admitted

            with patch("ats_mlops.model.tune.source_environment", return_value={"git_sha": "a" * 40}), \
                    patch("ats_mlops.model.tune.require_valid", side_effect=consume) as guard:
                result = tune(manifest, temp / "reports", admission_report=receipt,
                              config_path=ROOT / "configs/model.json", search_path=ROOT / "configs/search.json",
                              source_root=ROOT, image_identity={}, smoke=True)
            guard.assert_called_once()
            self.assertEqual(result["outcome"], "passed")
            self.assertEqual(json.loads((temp / "reports/study.json").read_text()), result)
            self.assertEqual(json.loads((temp / "reports/selected-model.json").read_text()), result["selected_config"])
            self.assertFalse(result["test_read"])
            with self.assertRaisesRegex(ValueError, "new_report_directory_required"):
                tune(manifest, temp / "reports", admission_report=receipt,
                     config_path=ROOT / "configs/model.json", search_path=ROOT / "configs/search.json",
                     source_root=ROOT, image_identity={})

    def test_cli_failure_never_echoes_raw_exception(self):
        import io

        args = [item for name in ("manifest", "report-dir", "admission-report", "config", "search", "source-root", "image-identity")
                for item in ("--" + name, "unused")]
        with patch.object(Path, "read_text", return_value="{}"), \
                patch("ats_mlops.model.tune.tune", side_effect=ValueError("PRIVATE")), \
                patch("sys.stderr", new_callable=io.StringIO) as output:
            self.assertEqual(main(args), 1)
        self.assertNotIn("PRIVATE", output.getvalue())


if __name__ == "__main__":
    unittest.main(verbosity=2)
