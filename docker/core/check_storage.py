"""Real, synthetic-only persistence and least-privilege probes for T-4."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from urllib import error, request

import boto3
from botocore.exceptions import ClientError
import mlflow
import mlflow.sklearn
import numpy as np
import psycopg2
from sklearn.linear_model import LogisticRegression

WORK = Path("/work/core-fixture")
STATE = WORK / "state.json"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def s3(identity: str):
    return boto3.client("s3", endpoint_url="http://minio:9000", region_name="us-east-1",
                        aws_access_key_id=os.environ[f"{identity}_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ[f"{identity}_SECRET_ACCESS_KEY"])


def dvc(*args: str) -> None:
    env = os.environ.copy()
    env.update(AWS_ACCESS_KEY_ID=env["DVC_ACCESS_KEY_ID"],
               AWS_SECRET_ACCESS_KEY=env["DVC_SECRET_ACCESS_KEY"])
    result = subprocess.run([sys.executable, "-m", "dvc", *args], cwd=WORK / "dvc",
                            env=env, capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(f"DVC {args[0]} failed: {result.stderr[-2000:]}")


def seed() -> dict:
    WORK.mkdir(parents=True, exist_ok=True)
    repo = WORK / "dvc"
    repo.mkdir()
    dvc("init", "--no-scm")
    dvc("config", "cache.dir", str(repo / ".dvc/cache"))
    payload = repo / "dataset.jsonl"
    payload.write_text('{"purpose":"synthetic_storage_fixture","relevance":1}\n')
    dvc("add", "dataset.jsonl")
    dvc("remote", "add", "--default", "core", "s3://ats-dvc/storage-fixture")
    dvc("remote", "modify", "--local", "core", "endpointurl", "http://minio:9000")
    dvc("push")
    x = np.array([[-3.0], [-2.0], [-1.0], [1.0], [2.0], [3.0]])
    model = LogisticRegression(random_state=42, solver="liblinear").fit(x, [0, 0, 0, 1, 1, 1])
    experiment = mlflow.create_experiment("core_storage_fixture")
    mlflow.set_experiment(experiment_id=experiment)
    artifact = WORK / "artifact.json"
    artifact.write_text('{"purpose":"synthetic_storage_fixture"}\n')
    with mlflow.start_run() as run:
        mlflow.set_tag("evidence_scope", "demo_only")
        mlflow.log_param("seed", 42)
        mlflow.log_artifact(str(artifact))
        logged = mlflow.sklearn.log_model(model, name="storage_fixture",
                                          serialization_format="cloudpickle")
        version = mlflow.register_model(logged.model_uri, "ats_storage_fixture",
                                         await_registration_for=120)
    state = {"run_id": run.info.run_id, "model_version": str(version.version),
             "dataset_sha256": digest(payload), "artifact_sha256": digest(artifact),
             "expected_predictions": model.predict([[-1.0], [1.0]]).tolist()}
    STATE.write_text(json.dumps(state, sort_keys=True) + "\n")
    return {"seeded": True, **state}


def verify() -> dict:
    state = json.loads(STATE.read_text())
    repo = WORK / "dvc"
    (repo / "dataset.jsonl").unlink(missing_ok=True)
    shutil.rmtree(repo / ".dvc/cache", ignore_errors=True)
    dvc("pull")
    assert digest(repo / "dataset.jsonl") == state["dataset_sha256"]
    client = mlflow.MlflowClient()
    run = client.get_run(state["run_id"])
    assert run.data.params["seed"] == "42" and run.data.tags["evidence_scope"] == "demo_only"
    artifact = Path(client.download_artifacts(state["run_id"], "artifact.json", str(WORK)))
    assert digest(artifact) == state["artifact_sha256"]
    version = client.get_model_version("ats_storage_fixture", state["model_version"])
    assert version.status == "READY"
    model = mlflow.sklearn.load_model(f"models:/ats_storage_fixture/{state['model_version']}")
    np.testing.assert_allclose(model.predict([[-1.0], [1.0]]), state["expected_predictions"])
    return {"dvc_restored": True, "run_retained": True, "artifact_retained": True,
            "registry_model_retained": True, **state}


def denied(action) -> None:
    try:
        action()
    except ClientError as exc:
        assert exc.response["ResponseMetadata"]["HTTPStatusCode"] == 403
    else:
        raise AssertionError("S3 operation unexpectedly succeeded")


def security() -> dict:
    dvc_client, mlflow_client = s3("DVC"), s3("MLFLOW")
    for client, own, other in ((dvc_client, "ats-dvc", "ats-mlflow"),
                                (mlflow_client, "ats-mlflow", "ats-dvc")):
        client.put_object(Bucket=own, Key="permissions/probe", Body=b"fixture")
        assert client.get_object(Bucket=own, Key="permissions/probe")["Body"].read() == b"fixture"
        denied(lambda: client.list_objects_v2(Bucket=other))
        denied(lambda: client.get_object(Bucket=other, Key="permissions/probe"))
        denied(lambda: client.put_object(Bucket=other, Key="permissions/forbidden", Body=b"no"))
    bad = boto3.client("s3", endpoint_url="http://minio:9000", region_name="us-east-1",
                       aws_access_key_id=os.environ["DVC_ACCESS_KEY_ID"],
                       aws_secret_access_key="deliberately-invalid-fixture-password")
    denied(lambda: bad.put_object(Bucket="ats-dvc", Key="forbidden", Body=b"no"))
    for role, password_name, own, other in (("mlflow", "MLFLOW_DB_PASSWORD", "mlflow", "airflow"),
                                            ("airflow", "AIRFLOW_DB_PASSWORD", "airflow", "mlflow")):
        args = dict(host="postgres", user=role, password=os.environ[password_name], connect_timeout=5)
        with psycopg2.connect(dbname=own, **args) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT rolsuper, rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname=current_user")
                assert cursor.fetchone() == (False, False, False)
        try:
            psycopg2.connect(dbname=other, **args)
        except psycopg2.OperationalError as exc:
            assert "permission denied" in str(exc)
        else:
            raise AssertionError("SQL cross-database connection unexpectedly succeeded")
        args["password"] = "deliberately-invalid-fixture-password"
        try:
            psycopg2.connect(dbname=own, **args)
        except psycopg2.OperationalError as exc:
            assert "password authentication failed" in str(exc)
        else:
            raise AssertionError("SQL invalid credentials unexpectedly succeeded")
    bad_host = request.Request("http://mlflow:5000/health", headers={"Host": "untrusted.invalid"})
    try:
        request.urlopen(bad_host, timeout=10)
    except error.HTTPError as exc:
        assert exc.code in (400, 403)
    else:
        raise AssertionError("MLflow accepted an untrusted Host")
    return {"own_bucket_read_write": True, "cross_bucket_denied": True,
            "bad_s3_credentials_denied": True, "sql_roles_scoped": True,
            "bad_sql_credentials_denied": True, "untrusted_mlflow_host_denied": True}


if __name__ == "__main__":
    phase = sys.argv[1]
    result = {"seed": seed, "verify": verify, "security": security}[phase]()
    print("ATS_CORE_RESULT=" + json.dumps({"phase": phase, "checks": result}, sort_keys=True))
