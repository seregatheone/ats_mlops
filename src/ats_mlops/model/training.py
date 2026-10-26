"""Train admitted rows, log/register one portable Pipeline, freeze its lineage."""

import argparse
from collections.abc import Mapping
from contextlib import contextmanager
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile
import warnings

from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from ats_mlops.data.dataset import canonical_json, load_manifest, sha256
from ats_mlops.data.validate import require_valid
from ats_mlops.evaluation.ranking import evaluate_ranking
from ats_mlops.features.pairs import FEATURE_CONTRACT, FEATURE_VERSION, PairFeatures
from ats_mlops.lineage.manifest import (
    file_hash, inventory_hash, source_environment, tree_inventory, write_json,
)
from ats_mlops.model.scoring import pair_frame, positive_scores, score_rows

LIMITATIONS = ["Synthetic rule labels; demo_only engineering evidence.",
               "No independent hiring outcomes or real-world hiring quality claim.",
               "Validation only; no release gate and no test evaluation."]


def make_pipeline(config: dict) -> Pipeline:
    if (not isinstance(config, dict) or config.get("schema_version") != 1 or config.get("feature_version") != FEATURE_VERSION
            or type(config.get("seed")) is not int
            or set(config.get("features", {})) != {"max_features", "ngram_range"}
            or set(config.get("classifier", {})) != {"C", "solver", "max_iter"}):
        raise ValueError("invalid_model_config")
    features = dict(config["features"])
    features["ngram_range"] = tuple(features["ngram_range"])
    return Pipeline([("pair_features", PairFeatures(**features)),
                     ("classifier", LogisticRegression(**config["classifier"], random_state=config["seed"]))])


def fit_rows(rows: list[dict], config: dict) -> Pipeline:
    """Pure fitting API reused by Optuna; the stage owns admission beforehand."""
    if not rows:
        raise ValueError("empty_training_rows")
    if any(not isinstance(row, Mapping) for row in rows):
        raise ValueError("invalid_training_row")
    labels = [row.get("relevance") for row in rows]
    if any(type(label) is not int or label not in (0, 1) for label in labels):
        raise ValueError("invalid_relevance")
    if set(labels) != {0, 1}:
        raise ValueError("single_class_training")
    frame = pair_frame(rows)
    model = make_pipeline(config)
    with warnings.catch_warnings():
        warnings.simplefilter("error", ConvergenceWarning)
        try:
            model.fit(frame, labels)
        except ConvergenceWarning as error:
            raise ValueError("training_did_not_converge") from error
    positive_scores(model.predict_proba(frame), model.classes_, expected_rows=len(rows))
    return model


@contextmanager
def portable_code(source_root: Path):
    """Keep source immutable while MLflow copies/cleans a writable byte snapshot.

    MLflow preserves directory modes through copytree, then removes its own
    temporary code tree. A read-only source mode otherwise breaks that cleanup
    for the unprivileged LocalExecutor user after remote work may have started.
    """
    original = source_root / "src/ats_mlops"
    with tempfile.TemporaryDirectory(prefix="ats-model-code-") as temporary:
        snapshot = Path(temporary) / "ats_mlops"
        shutil.copytree(original, snapshot, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        for path in [snapshot, *snapshot.rglob("*")]:
            path.chmod(0o755 if path.is_dir() else 0o644)
        if tree_inventory(snapshot) != tree_inventory(original):
            raise ValueError("packaged_source_snapshot_mismatch")
        yield snapshot


def train(manifest_path: Path, report_dir: Path, *, admission_report: Path,
          config_path: Path, source_root: Path, image_identity: dict,
          runtime_lock: Path = Path("/opt/requirements-ml.lock"),
          experiment: str = "ats-training", registered_model: str = "ats-pair-ranking") -> dict:
    """Run the real stage; no corpus/test/split bytes or post-guard data reopen."""
    report_dir = report_dir.resolve()
    if report_dir.is_relative_to(source_root.resolve()):
        raise ValueError("reports_must_be_outside_source_checkout")
    rows = require_valid(manifest_path, report_dir, admission_report=admission_report)
    training_report = json.loads((report_dir / "data-training.json").read_text())
    manifest = load_manifest(manifest_path)
    if file_hash(manifest_path) != training_report["manifest_sha256"]:
        raise ValueError("manifest_changed_after_validation")
    provenance = source_environment(source_root, image_identity, runtime_lock=runtime_lock)
    config = json.loads(config_path.read_text())
    model = fit_rows(rows["train"], config)
    scores = score_rows(model, rows["validation"])
    metrics = evaluate_ranking(rows["validation"], scores,
                               group_ids=manifest["splits"]["validation"]["vacancy_ids"])
    if metrics["status"] != "defined":
        raise ValueError("undefined_validation_metrics")
    feature_config = {**FEATURE_CONTRACT, "parameters": config["features"]}
    provenance.update({
        "schema_version": 1, "evidence_scope": "demo_only", "seed": config["seed"],
        "data_manifest_sha256": training_report["manifest_sha256"],
        "data_sha256": training_report["artifact_hashes"],
        "generator": manifest["generator"],
        "split_sha256": manifest["artifacts"]["split_lists"]["sha256"],
        "split_hash_source": "admitted_manifest_reference; split_bytes_not_read",
        "partition_inventory_sha256": {name: sha256(canonical_json(manifest["splits"][name]))
                                       for name in ("train", "validation")},
        "features": feature_config, "features_sha256": sha256(canonical_json(feature_config)),
        "params": config, "params_sha256": sha256(canonical_json(config)),
        "effective_classifier_params": model.named_steps["classifier"].get_params(),
        "read_partitions": ["train", "validation"], "test_read": False,
        "limitations": LIMITATIONS,
    })
    write_json(report_dir / "input-provenance.json", provenance)
    write_json(report_dir / "validation.json", {"partition": "validation", "metrics": metrics,
                                               "evidence_scope": "demo_only", "limitations": LIMITATIONS})
    probabilities = model.predict_proba(pair_frame(rows["validation"]))
    write_json(report_dir / "reference.json", {
        "rows": [{key: row[key] for key in ("vacancy_id", "candidate_id", "vacancy_text", "candidate_text")}
                 for row in rows["validation"]],
        "classes": model.classes_.tolist(), "probabilities": probabilities.tolist(),
        "scores": positive_scores(probabilities, model.classes_).tolist(),
        "feature_version": FEATURE_VERSION,
        "vocabulary": {token: int(index) for token, index in model.named_steps["pair_features"].vectorizer_.vocabulary_.items()},
        "idf": model.named_steps["pair_features"].vectorizer_.idf_.tolist(),
    })

    import mlflow
    from mlflow.models import infer_signature

    mlflow.set_experiment(experiment)
    with mlflow.start_run(tags={"training_git_sha": provenance["git_sha"], "evidence_scope": "demo_only",
                                "features_sha256": provenance["features_sha256"],
                                "params_sha256": provenance["params_sha256"]}) as run:
        mlflow.log_params({"seed": config["seed"], "feature_version": FEATURE_VERSION,
                           **{f"features.{k}": v for k, v in config["features"].items()},
                           **{f"classifier.{k}": v for k, v in config["classifier"].items()}})
        mlflow.log_metrics({name: metrics[name] for name in ("ndcg_at_10", "recall_at_10", "eligible_group_count")})
        example = pair_frame(rows["validation"][:2])
        with portable_code(source_root) as code:
            info = mlflow.sklearn.log_model(
                model, name="pair-model", code_paths=[str(code)],
                serialization_format="cloudpickle", pyfunc_predict_fn="predict_proba",
                pip_requirements=[f"{name}=={value}" for name, value in provenance["environment"]["packages"].items()],
                signature=infer_signature(example, model.predict_proba(example)), input_example=example,
                metadata={"feature_version": FEATURE_VERSION, "classes": [0, 1],
                          "training_git_sha": provenance["git_sha"], "features_sha256": provenance["features_sha256"]},
                extra_files=[str(report_dir / "input-provenance.json")],
            )
        version = mlflow.register_model(info.model_uri, registered_model)
        exact_uri = f"models:/{registered_model}/{version.version}"
        registered = mlflow.MlflowClient().get_model_version(registered_model, version.version)
        if registered.run_id != run.info.run_id:
            raise ValueError("registered_run_mismatch")
        download = report_dir / "download"
        download.mkdir()
        local = Path(mlflow.artifacts.download_artifacts(artifact_uri=exact_uri, dst_path=str(download)))
        shutil.copytree(local, report_dir / "model")
        shutil.rmtree(download)
        inventory = tree_inventory(report_dir / "model")
        result = {**provenance, "mlflow": {"run_id": run.info.run_id, "experiment_id": run.info.experiment_id,
                  "model_name": registered_model, "model_version": str(version.version),
                  "exact_uri": exact_uri, "source": registered.source},
                  "model": {"files": inventory, "sha256": inventory_hash(inventory),
                            "payload_sha256": inventory["model.pkl"]},
                  "reports_sha256": {name: file_hash(path) for name, path in {
                      "admission": admission_report, "training": report_dir / "data-training.json",
                      "validation": report_dir / "validation.json", "reference": report_dir / "reference.json",
                  }.items()}}
        write_json(report_dir / "run_manifest.json", result)
        for name in ("data-training.json", "data-training.md", "validation.json", "input-provenance.json", "run_manifest.json"):
            mlflow.log_artifact(str(report_dir / name), artifact_path="lineage")
        mlflow.log_artifact(str(admission_report), artifact_path="lineage")
        mlflow.set_tag("run_manifest_sha256", file_hash(report_dir / "run_manifest.json"))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "report-dir", "admission-report", "config", "source-root", "image-identity"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--experiment", default="ats-training")
    parser.add_argument("--registered-model", default="ats-pair-ranking")
    args = parser.parse_args(argv)
    try:
        result = train(args.manifest, args.report_dir, admission_report=args.admission_report,
                       config_path=args.config, source_root=args.source_root,
                       image_identity=json.loads(args.image_identity.read_text()),
                       experiment=args.experiment, registered_model=args.registered_model)
    except Exception as error:
        # Never echo upstream exceptions: they can include input texts or endpoints.
        reason = str(error)
        safe_reason = reason if re.fullmatch(r"[a-z_]+(?::[a-z_,]+)?", reason) else "training_failed"
        print(json.dumps({"outcome": "failed", "error_type": type(error).__name__, "reason": safe_reason}), file=sys.stderr)
        return 1
    print(json.dumps({"outcome": "passed", "run_id": result["mlflow"]["run_id"],
                      "model_uri": result["mlflow"]["exact_uri"], "model_sha256": result["model"]["sha256"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
