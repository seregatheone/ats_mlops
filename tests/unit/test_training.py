"""Meaningful leakage, feature, score and concrete stage-boundary tests."""

from copy import deepcopy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
from sklearn.pipeline import Pipeline

from ats_mlops.data.dataset import generate_dataset
from ats_mlops.data.validate import ValidationError, require_valid, validate_dataset
from ats_mlops.features.pairs import FEATURE_NAMES, PairFeatures, tokenize
from ats_mlops.model.scoring import pair_frame, positive_scores, score_rows
from ats_mlops.model.training import fit_rows, make_pipeline, portable_code, train
from ats_mlops.lineage.manifest import tree_inventory

ROOT = Path(__file__).resolve().parents[2]
CONFIG = json.loads((ROOT / "configs/model.json").read_text())


class PortableCodeTests(unittest.TestCase):
    def test_readonly_source_copy_is_byte_identical_and_cleanable(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            package = source / "src/ats_mlops"
            package.mkdir(parents=True)
            (package / "module.py").write_bytes(b"immutable source\n")
            (package / "module.py").chmod(0o444)
            package.chmod(0o555)
            try:
                with portable_code(source) as snapshot:
                    self.assertEqual((snapshot / "module.py").read_bytes(), b"immutable source\n")
                    self.assertTrue(snapshot.stat().st_mode & 0o200)
                    self.assertTrue((snapshot / "module.py").stat().st_mode & 0o200)
                    # Exercise pinned MLflow's actual copy/cleanup operations;
                    # this also runs under UID50000 in the runtime smoke probe.
                    from mlflow.utils.model_utils import _validate_and_copy_code_paths
                    artifact = source / "artifact"
                    artifact.mkdir()
                    _validate_and_copy_code_paths([str(snapshot)], str(artifact))
                    self.assertEqual(tree_inventory(artifact / "code/ats_mlops"), tree_inventory(package))
                    shutil.rmtree(artifact)
                    snapshot_path = snapshot
                self.assertFalse(snapshot_path.exists())
                self.assertFalse(package.stat().st_mode & 0o200)
                self.assertFalse((package / "module.py").stat().st_mode & 0o200)
            finally:
                package.chmod(0o755)


def fixture():
    return [
        {"vacancy_id": "v1", "candidate_id": "a", "vacancy_text": "Python SQL vacancyonly",
         "candidate_text": "Python SQL candidateonly", "relevance": 1},
        {"vacancy_id": "v1", "candidate_id": "b", "vacancy_text": "Python SQL vacancyonly",
         "candidate_text": "Illustration design layout", "relevance": 0},
        {"vacancy_id": "v2", "candidate_id": "c", "vacancy_text": "Java Kotlin Android",
         "candidate_text": "Kotlin Android Java", "relevance": 1},
        {"vacancy_id": "v2", "candidate_id": "d", "vacancy_text": "Java Kotlin Android",
         "candidate_text": "Accounting budget finance", "relevance": 0},
    ]


class TrainingTests(unittest.TestCase):
    def test_only_train_both_sides_fit_and_validation_never_refits(self):
        rows = fixture()
        model = fit_rows(rows, CONFIG)
        transformer = model.named_steps["pair_features"]
        vocabulary = dict(transformer.vectorizer_.vocabulary_)
        idf = transformer.vectorizer_.idf_.copy()
        train_tokens = {token for row in rows for column in ("vacancy_text", "candidate_text")
                        for token in tokenize(row[column])}
        self.assertEqual(set(vocabulary), train_tokens)
        self.assertIn("vacancyonly", vocabulary)
        self.assertIn("candidateonly", vocabulary)
        validation = deepcopy(rows)
        validation[0]["candidate_text"] = "validationsentinel Python"
        scores = score_rows(model, validation)
        self.assertEqual(len(scores), len(rows))
        self.assertNotIn("validationsentinel", transformer.vectorizer_.vocabulary_)
        self.assertEqual(vocabulary, transformer.vectorizer_.vocabulary_)
        np.testing.assert_array_equal(idf, transformer.vectorizer_.idf_)
        repeated = fit_rows(rows, CONFIG)
        np.testing.assert_allclose(model.predict_proba(pair_frame(validation)),
                                   repeated.predict_proba(pair_frame(validation)), rtol=0, atol=1e-12)
        self.assertIsInstance(model, Pipeline)

    def test_manual_features_normalization_order_and_oov(self):
        frame = pair_frame([{"vacancy_text": "Ａ A b", "candidate_text": "a B B"}])
        transformer = PairFeatures().fit(frame)
        left = transformer.vectorizer_.transform(frame.vacancy_text).toarray()[0]
        right = transformer.vectorizer_.transform(frame.candidate_text).toarray()[0]
        expected = [[float(left @ right), 1.0, np.log(4), np.log(4), 1.0]]
        np.testing.assert_allclose(transformer.transform(frame), expected)
        self.assertEqual(list(transformer.get_feature_names_out()), list(FEATURE_NAMES))
        oov = pair_frame([{"vacancy_text": "novel", "candidate_text": "other novel"},
                          {"vacancy_text": "!", "candidate_text": "?"}])
        actual = transformer.transform(oov)
        np.testing.assert_allclose(actual[0], [0, .5, np.log(2), np.log(3), .5])
        np.testing.assert_array_equal(actual[1], [0, 0, 0, 0, 0])
        self.assertEqual(transformer.transform(pair_frame([])).shape, (0, 5))

    def test_empty_vocabulary_single_class_empty_and_invalid_labels(self):
        for label in (True, None, "1", float("nan"), float("inf"), 2):
            rows = fixture()
            rows[0]["relevance"] = label
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, "invalid_relevance"):
                fit_rows(rows, CONFIG)
        rows = fixture()
        for row in rows:
            row["relevance"] = 1
        with self.assertRaisesRegex(ValueError, "single_class_training"):
            fit_rows(rows, CONFIG)
        with self.assertRaisesRegex(ValueError, "empty_training_rows"):
            fit_rows([], CONFIG)
        rows = fixture()
        for row in rows:
            row.update(vacancy_text="!", candidate_text="?")
        with self.assertRaisesRegex(ValueError, "empty_vocabulary"):
            fit_rows(rows, CONFIG)

    def test_bad_inputs_and_duplicate_ids_fail_safely(self):
        model = fit_rows(fixture(), CONFIG)
        for text in (None, 3, float("nan"), "", " \t\n"):
            rows = fixture()
            rows[0]["candidate_text"] = text
            with self.subTest(text=text), self.assertRaisesRegex(ValueError, "invalid_pair_text"):
                score_rows(model, rows)
        rows = fixture()
        del rows[0]["vacancy_text"]
        with self.assertRaisesRegex(ValueError, "invalid_pair_text"):
            score_rows(model, rows)
        with self.assertRaisesRegex(ValueError, "duplicate_pair"):
            score_rows(model, fixture() * 2)
        rows = fixture()
        rows[0]["candidate_id"] = ""
        with self.assertRaisesRegex(ValueError, "invalid_pair_id"):
            score_rows(model, rows)
        self.assertEqual(score_rows(model, []), {})

    def test_positive_score_contract_rejects_bad_classes_shape_and_values(self):
        np.testing.assert_array_equal(positive_scores([[.2, .8]], [0, 1], expected_rows=1), [.8])
        for classes in ([1, 0], [0], [0, 1, 2]):
            with self.assertRaisesRegex(ValueError, "invalid_model_classes"):
                positive_scores([[.2, .8]], classes)
        for values in ([.2, .8], [[.2]], [[[.2, .8]]], [[.2, .8], [.1, .9]]):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "invalid_probability_shape"):
                positive_scores(values, [0, 1], expected_rows=1)
        for values in ([[float("nan"), .8]], [[float("inf"), .8]], [[-.2, 1.2]], [[.2, .7]]):
            with self.subTest(values=values), self.assertRaisesRegex(ValueError, "invalid_probability_values"):
                positive_scores(values, [0, 1])

    def test_invalid_config_is_rejected(self):
        config = deepcopy(CONFIG)
        config["feature_version"] = "unbound-version"
        with self.assertRaisesRegex(ValueError, "invalid_model_config"):
            make_pipeline(config)

    def test_actual_stage_rejected_guard_never_calls_pipeline_fit(self):
        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            manifest = temp / "manifest.json"
            generate_dataset(ROOT / "tests/fixtures/data/ci-config.json", manifest, temp / "data")
            with patch.object(Pipeline, "fit") as fitted:
                with self.assertRaisesRegex(ValidationError, "admission_receipt_required"):
                    train(manifest, temp / "reports", admission_report=temp / "missing.json",
                          config_path=ROOT / "configs/model.json", source_root=ROOT, image_identity={})
                fitted.assert_not_called()
            report = json.loads((temp / "reports/data-training.json").read_text())
            self.assertEqual(report["outcome"], "rejected")

    def test_actual_stage_fits_bound_rows_after_data_becomes_unavailable(self):
        import mlflow

        with tempfile.TemporaryDirectory() as directory:
            temp = Path(directory)
            manifest = temp / "manifest.json"
            generate_dataset(ROOT / "tests/fixtures/data/ci-config.json", manifest, temp / "data")
            self.assertEqual(validate_dataset(manifest, temp / "admission")["outcome"], "passed")
            receipt = temp / "admission/data-admission.json"
            document = json.loads(manifest.read_text())

            def consume(*args, **kwargs):
                rows = require_valid(*args, **kwargs)
                for artifact in document["artifacts"].values():
                    (temp / artifact["path"]).unlink(missing_ok=True)
                return rows

            provenance = {"git_sha": "a" * 40, "environment": {"packages": {}}}
            with patch("ats_mlops.model.training.require_valid", side_effect=consume), \
                    patch("ats_mlops.model.training.source_environment", return_value=provenance), \
                    patch("ats_mlops.model.training.fit_rows", wraps=fit_rows) as fitted, \
                    patch.object(mlflow, "set_experiment", side_effect=RuntimeError("stop_before_network")):
                with self.assertRaisesRegex(RuntimeError, "stop_before_network"):
                    train(manifest, temp / "reports", admission_report=receipt,
                          config_path=ROOT / "configs/model.json", source_root=ROOT, image_identity={})
            fitted.assert_called_once()
            report = json.loads((temp / "reports/validation.json").read_text())
            self.assertEqual(report["metrics"]["status"], "defined")


if __name__ == "__main__":
    unittest.main(verbosity=2)
