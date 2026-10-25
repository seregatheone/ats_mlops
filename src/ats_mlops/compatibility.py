"""Real compatibility probes using a tiny synthetic classification fixture.

This is not the ATS ranker, its dataset, or evidence of model quality. Each
successful worker execution fits a fresh estimator and registers a new version.
"""

from __future__ import annotations

import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tempfile

from ats_mlops.runner import CompatibilityError, file_sha256, validate_run_id, write_json


def _ensure_bucket(s3, bucket: str) -> None:
    from botocore.exceptions import ClientError

    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if str(exc.response["Error"]["Code"]) not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        s3.create_bucket(Bucket=bucket)


def _dvc_roundtrip(s3, run_id: str) -> bool:
    prefix = f"{run_id}/"
    bucket = "ats-compat-dvc"
    with tempfile.TemporaryDirectory(prefix="ats-compat-dvc-") as directory:
        repo = Path(directory)

        def dvc(*args: str) -> None:
            subprocess.run([sys.executable, "-m", "dvc", *args], cwd=repo,
                           check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            dvc("init", "--no-scm")
            dvc("config", "cache.dir", str(repo / ".dvc" / "cache"))
            payload = repo / "payload.txt"
            payload.write_text("ATS synthetic compatibility fixture\n", encoding="utf-8")
            expected = file_sha256(payload)
            dvc("add", "payload.txt")
            dvc("remote", "add", "--default", "compat", f"s3://{bucket}/{prefix}")
            dvc("remote", "modify", "--local", "compat", "endpointurl",
                os.environ.get("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000"))
            dvc("push")
            payload.unlink()
            shutil.rmtree(repo / ".dvc" / "cache")
            dvc("pull")
            if file_sha256(payload) != expected:
                raise CompatibilityError("dvc_payload_mismatch")
            return True
        finally:
            # Remove only this run's objects; no shared bucket or other run data.
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
                if objects:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})


def _validate_fixture(frame) -> tuple[bool, bool]:
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name="compatibility")
    asset = source.add_dataframe_asset(name="synthetic_fixture")
    definition = asset.add_batch_definition_whole_dataframe("whole_fixture")
    expectation = gx.expectations.ExpectColumnValuesToBeInSet(
        column="relevance", value_set=[0, 1],
    )
    valid = definition.get_batch(batch_parameters={"dataframe": frame})
    gx_valid = bool(valid.validate(expectation).success)
    invalid_frame = frame.copy()
    invalid_frame.loc[0, "relevance"] = 99
    invalid = definition.get_batch(batch_parameters={"dataframe": invalid_frame})
    gx_invalid_rejected = not bool(invalid.validate(expectation).success)
    if not gx_valid or not gx_invalid_rejected:
        raise CompatibilityError("gx_validation_failed")
    return gx_valid, gx_invalid_rejected


def run_worker(run_id: str, report_path: Path) -> dict:
    validate_run_id(run_id)
    try:
        for variable in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"):
            os.environ[variable] = "1"
        import bentoml.mlflow
        import boto3
        import mlflow
        import mlflow.sklearn
        from mlflow.models import infer_signature
        import numpy as np
        import pandas as pd
        from sklearn.linear_model import LogisticRegression

        checkout = Path.cwd()
        git_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=checkout,
                                 check=True, capture_output=True, text=True).stdout.strip()
        lock_sha256 = file_sha256(checkout / "requirements-ml.lock")
        frame = pd.DataFrame({"x": [-3.0, -2.0, -1.0, 1.0, 2.0, 3.0],
                              "relevance": [0, 0, 0, 1, 1, 1]})
        gx_valid, gx_invalid_rejected = _validate_fixture(frame)
        s3 = boto3.client("s3", endpoint_url=os.environ.get(
            "MLFLOW_S3_ENDPOINT_URL", "http://minio:9000"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))
        _ensure_bucket(s3, "ats-compat-dvc")
        _ensure_bucket(s3, "ats-compat-mlflow")
        dvc_roundtrip = _dvc_roundtrip(s3, run_id)
        # Fit unconditionally: this smoke does not use DVC run cache or a model
        # left by a previous run as evidence of a new training execution.
        model = LogisticRegression(random_state=42, solver="liblinear")
        model.fit(frame[["x"]], frame["relevance"])
        probe_values = [-1.0, 1.0]
        probe = pd.DataFrame({"x": probe_values})
        expected_predictions = [float(value) for value in model.predict(probe)]
        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
        experiment_id = mlflow.create_experiment(
            f"ats-compat-{run_id}", artifact_location=f"s3://ats-compat-mlflow/{run_id}",
        )
        mlflow.set_experiment(experiment_id=experiment_id)
        model_name = "ats_compatibility_fixture"
        with mlflow.start_run(run_name=run_id) as run:
            mlflow.set_tags({"ats.run_id": run_id, "ats.git_sha": git_sha,
                             "ats.lock_sha256": lock_sha256,
                             "ats.purpose": "synthetic_compatibility_fixture"})
            mlflow.log_params({"seed": 42, "fixture_rows": len(frame)})
            logged = mlflow.sklearn.log_model(
                model, name="compatibility_fixture", serialization_format="cloudpickle",
                signature=infer_signature(probe, model.predict(probe)), input_example=probe,
            )
            registered = mlflow.register_model(logged.model_uri, model_name,
                                               await_registration_for=120)
            mlflow_run_id = run.info.run_id
        model_version = str(registered.version)
        if not model_version.isdigit() or registered.status != "READY":
            raise CompatibilityError("invalid_model_version")
        model_uri = f"models:/{model_name}/{model_version}"
        registered_model = mlflow.sklearn.load_model(model_uri)
        np.testing.assert_allclose(registered_model.predict(probe), expected_predictions,
                                   atol=1e-6, rtol=1e-5)
        bento = bentoml.mlflow.import_model(
            "ats_compatibility_fixture", model_uri=model_uri,
            signatures={"predict": {"batchable": True}},
            metadata={"git_sha": git_sha, "model_version": model_version,
                      "mlflow_run_id": mlflow_run_id, "fixture_only": True},
        )
        loaded_bento = bentoml.mlflow.load_model(bento)
        np.testing.assert_allclose(loaded_bento.predict(probe), expected_predictions,
                                   atol=1e-6, rtol=1e-5)
        bento_tag = str(bento.tag)
        report = {"run_id": run_id, "git_sha": git_sha, "lock_sha256": lock_sha256,
                  "manifest": {"git_sha": git_sha, "lock_sha256": lock_sha256,
                               "python_executable": sys.executable,
                               "checkout_path": str(checkout.resolve())},
                  "checks": {"gx_valid": gx_valid, "gx_invalid_rejected": gx_invalid_rejected,
                             "dvc_roundtrip": dvc_roundtrip, "mlflow_registered": True,
                             "bento_import": True},
                  "model_version": model_version, "model_name": model_name,
                  "mlflow_run_id": mlflow_run_id, "bento_tag": bento_tag,
                  "probe_values": probe_values, "expected_predictions": expected_predictions,
                  "fixture_only": True, "training_executed": True,
                  "platform": {"system": platform.system(), "machine": platform.machine(),
                               "python": platform.python_version()}}
        tag_file = Path(os.environ.get(
            "ATS_BENTO_TAG_FILE", "/artifacts/compat/bento_model_tag.txt"))
        tag_file.parent.mkdir(parents=True, exist_ok=True)
        temporary_tag = tag_file.with_suffix(".tmp")
        temporary_tag.write_text(bento_tag + "\n", encoding="utf-8")
        temporary_tag.replace(tag_file)
        write_json(report_path, report)
        return report
    except Exception as exc:
        kind = exc.kind if isinstance(exc, CompatibilityError) else "worker_failed"
        write_json(report_path.parent / "error.json", {"kind": kind, "run_id": run_id})
        raise CompatibilityError(kind) from None
