"""Durable, bounded Airflow adapter; ML execution imports the requested source."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import sys
from urllib.parse import urlsplit


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def file_hash(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


class Failure(Exception):
    def __init__(self, kind, retryable=False):
        self.kind, self.retryable = kind, retryable
        super().__init__(kind)


def validate(conf):
    required = {"request_id", "run_id", "git_sha", "dataset_version", "bundle_uri", "bundle_inventory_sha256", "params_hash", "parameters"}
    if set(conf) != required:
        raise Failure("invalid_request_fields")
    for name in ("request_id", "run_id", "dataset_version"):
        if not isinstance(conf[name], str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", conf[name]):
            raise Failure("invalid_identifier")
    for name, length in (("git_sha", 40), ("bundle_inventory_sha256", 64), ("params_hash", 64)):
        if not isinstance(conf[name], str) or not re.fullmatch(r"[0-9a-f]{%d}" % length, conf[name]):
            raise Failure("invalid_hash")
    params = conf["parameters"]
    if (not isinstance(params, dict) or set(params) != {"model", "search", "smoke", "evaluation"}
            or not isinstance(params["smoke"], bool) or digest(params) != conf["params_hash"]):
        raise Failure("params_hash_mismatch")
    from ats_mlops.pipeline import selection_mode
    try:
        selection_mode(params["search"], params["smoke"])
    except ValueError:
        raise Failure("invalid_selection_mode") from None
    uri = urlsplit(conf["bundle_uri"])
    if (uri.scheme != "s3" or uri.netloc != "ats-dvc" or uri.query or uri.fragment
            or not re.fullmatch(r"orchestration/bundles/[A-Za-z0-9_.-]+/bundle.json", uri.path.lstrip("/"))):
        raise Failure("untrusted_bundle_uri")
    return conf


def _numeric_snapshot(path):
    # Monitoring metadata is best effort and cannot change pipeline outcomes.
    try:
        from ats_mlops.monitoring.technical import write_pipeline_snapshot
        write_pipeline_snapshot(path)
    except Exception:
        pass


def _snapshot_after(function):
    @wraps(function)
    def monitored(db, *args, **kwargs):
        try:
            return function(db, *args, **kwargs)
        finally:
            try:
                path = db.execute("PRAGMA database_list").fetchone()[2]
                if path:
                    _numeric_snapshot(Path(path))
            except Exception:
                pass
    return monitored


@contextmanager
def database(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS requests(
          request_id TEXT PRIMARY KEY, payload_hash TEXT NOT NULL, status TEXT NOT NULL,
          attempts INTEGER NOT NULL DEFAULT 0, receipt_hash TEXT, kind TEXT);
        CREATE TABLE IF NOT EXISTS attempts(
          request_id TEXT NOT NULL, attempt INTEGER NOT NULL, phase TEXT NOT NULL,
          outcome TEXT NOT NULL, kind TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY(request_id,attempt));
    """)
    try:
        yield connection
    finally:
        connection.close()
        _numeric_snapshot(path)


@_snapshot_after
def claim(db, request_id, payload_hash):
    with db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT * FROM requests WHERE request_id=?", (request_id,)).fetchone()
        if row and row["payload_hash"] != payload_hash:
            raise Failure("request_conflict")
        if row and row["status"] == "completed":
            return row
        if row and row["status"] in {"failed", "recovery_required"}:
            raise Failure(row["kind"] or "request_failed")
        if row and row["attempts"] >= 3 and row["status"] != "in_progress":
            raise Failure("retry_budget_exhausted")
        if not row:
            db.execute("INSERT INTO requests(request_id,payload_hash,status) VALUES(?,?,'ready')", (request_id, payload_hash))
        return row


@_snapshot_after
def begin_attempt(db, request_id):
    with db:
        changed = db.execute("UPDATE requests SET attempts=attempts+1,status='in_progress' WHERE request_id=? AND attempts<3 AND status IN ('ready','retryable','in_progress')", (request_id,))
        if changed.rowcount != 1:
            raise Failure("retry_budget_exhausted")
        attempt = db.execute("SELECT attempts FROM requests WHERE request_id=?", (request_id,)).fetchone()[0]
        db.execute("INSERT INTO attempts(request_id,attempt,phase,outcome) VALUES(?,?,'download','running')", (request_id, attempt))
    return attempt


@_snapshot_after
def finish(db, request_id, attempt, status, *, kind=None, receipt_hash=None, phase="download"):
    with db:
        db.execute("UPDATE requests SET status=?,kind=?,receipt_hash=? WHERE request_id=?", (status, kind, receipt_hash, request_id))
        db.execute("UPDATE attempts SET phase=?,outcome=?,kind=? WHERE request_id=? AND attempt=?", (phase, status, kind, request_id, attempt))


@_snapshot_after
def recovered(db, request_id, receipt_hash):
    with db:
        db.execute("UPDATE requests SET status='completed',kind=NULL,receipt_hash=? WHERE request_id=?", (receipt_hash, request_id))
        db.execute("UPDATE attempts SET phase='recovered',outcome='completed',kind=NULL WHERE request_id=? AND outcome='running'", (request_id,))


def repro_failure_kind(receipt):
    events = receipt.get("events", {})
    # A train failure may follow a remote registration, including a ValueError
    # in post-registration verification. Missing diagnostics are also ambiguous.
    if "failure-train" in events or "train" in events or (not events and "preflight" not in receipt):
        return "recovery_required"
    return "data_or_config_invalid"


def ml_env(source=None):
    # Do not inherit Airflow SQL/auth/JWT, host credentials or MLflow bucket IAM.
    allowed = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION", "MLFLOW_TRACKING_URI")
    env = {name: os.environ[name] for name in allowed if name in os.environ}
    env.update({"HOME": "/tmp", "PATH": "/opt/ml-venv/bin:/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1",
                "PYTHONPATH": str(source / "src") if source else "/opt/project/src", "GIT_OPTIONAL_LOCKS": "0",
                "OMP_NUM_THREADS": "1", "OPENBLAS_NUM_THREADS": "1", "MKL_NUM_THREADS": "1",
                "ATS_ML_PYTHON": "/opt/ml-venv/bin/python"})
    return env


def command(args, *, env=None, cwd=None):
    process = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    if process.returncode:
        raise Failure("source_or_worker_failed")
    return process.stdout


def fetch_bundle(client, conf, output):
    """Validate the complete ingress plan before requesting any partition bytes."""
    from ats_mlops.lineage.manifest import inventory_hash

    prefix = urlsplit(conf["bundle_uri"]).path.lstrip("/").removesuffix("bundle.json")

    def fetch(name, expected=None, limit=16777216):
        body = client.get_object(Bucket="ats-dvc", Key=prefix + name)["Body"]
        try:
            content = body.read(limit + 1)
        finally:
            body.close()
        if len(content) > limit or (expected and hashlib.sha256(content).hexdigest() != expected):
            raise Failure("bundle_content_mismatch")
        return content

    descriptor = json.loads(fetch("bundle.json", limit=131072))
    files = descriptor.get("files", {})
    if (descriptor.get("schema_version") != 1 or not isinstance(files, dict) or len(files) > 16
            or descriptor.get("sha256") != conf["bundle_inventory_sha256"]
            or inventory_hash(files) != conf["bundle_inventory_sha256"]):
        raise Failure("bundle_identity_mismatch")
    required = {"data_manifest.json", "admission/data-admission.json", "dataset-binding.json",
                "data/train.jsonl.dvc", "data/validation.jsonl.dvc", "data/split.json.dvc"}
    reserved = {"data/test.jsonl", "data/source.jsonl", "data/corpus.jsonl", "data/split.json"}
    if not required.issubset(files) or reserved.intersection(files):
        raise Failure("data_invalid")
    for name, expected in files.items():
        if (not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_./-]+", name)
                or name.startswith("/") or ".." in name.split("/")
                or not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected)):
            raise Failure("unsafe_bundle_path")
    # Only trusted metadata is fetched until the exact train/validation plan is known.
    manifest_bytes = fetch("data_manifest.json", files["data_manifest.json"])
    manifest = json.loads(manifest_bytes)
    partitions = {manifest["artifacts"][name]["path"] for name in ("train", "validation")}
    forbidden = {manifest["artifacts"][name]["path"] for name in ("source", "corpus", "test", "split_lists")}
    # Policy metadata carries hashes only. It is explicitly admitted alongside
    # train/validation, never substituted for a reserved raw artifact.
    policy_path = manifest.get("reservation", {}).get("policy", {}).get("path")
    if (manifest.get("schema_version") != 2 or not isinstance(policy_path, str)
            or not policy_path.endswith("/reservation_policy.json")
            or policy_path in forbidden | partitions | required
            or partitions.intersection(forbidden) or set(files) != required | partitions | {policy_path}):
        raise Failure("data_invalid")
    output.mkdir(parents=True, exist_ok=True)
    write(output / "bundle.json", descriptor)
    for name, expected in files.items():
        path = output / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(manifest_bytes if name == "data_manifest.json" else fetch(name, expected))
    from ats_mlops.pipeline import verify_bundle
    verify_bundle(output, conf["bundle_inventory_sha256"])


def download(conf, output, attempt):
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError, ConnectTimeoutError, ConnectionClosedError, EndpointConnectionError, ReadTimeoutError

    endpoint = "http://minio:9000"
    # Private operator fixture, never accepted from DAG conf.
    fault = os.environ.get("ATS_ORCHESTRATION_FAULT_FILE")
    if fault:
        plan = read(fault)
        if attempt <= plan.get(conf["request_id"], 0):
            endpoint = "http://127.0.0.1:9"
    client = boto3.client("s3", endpoint_url=endpoint, config=Config(connect_timeout=2, read_timeout=10, retries={"total_max_attempts": 1}))
    try:
        fetch_bundle(client, conf, output)
    except (EndpointConnectionError, ConnectTimeoutError, ConnectionClosedError, ReadTimeoutError):
        raise Failure("io_transient", True) from None
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
        raise Failure("io_transient" if status >= 500 else "bundle_access_failed", status >= 500) from None
    except (ValueError, KeyError, TypeError):
        raise Failure("data_invalid") from None


def verify_result(root, expected_hash=None, conf=None):
    result_path = root / "candidate.json"
    if expected_hash and file_hash(result_path) != expected_hash:
        raise Failure("completed_receipt_mismatch")
    result = read(result_path)
    pipeline = root / "run/workspace/artifacts/evaluation/pipeline_manifest.json"
    if file_hash(pipeline) != result["pipeline_manifest_sha256"]:
        raise Failure("completed_manifest_mismatch")
    pipeline_training = read(pipeline)["training"]
    if pipeline_training["mlflow"] != result["mlflow"]:
        raise Failure("completed_model_mismatch")
    if conf and result.get("request_sha256") != digest(conf):
        raise Failure("completed_request_mismatch")
    training = root / "run/workspace/artifacts/training/final"
    record = read(training / "run_manifest.json")
    if file_hash(training / "run_manifest.json") != pipeline_training["run_manifest_sha256"]:
        raise Failure("completed_training_manifest_mismatch")
    from ats_mlops.lineage.manifest import inventory_hash, tree_inventory
    actual = tree_inventory(training / "model")
    if (record["model"]["files"] != actual or record["mlflow"] != result["mlflow"]
            or inventory_hash(actual) != record["model"]["sha256"]
            or record["model"]["sha256"] != pipeline_training["model_sha256"]):
        raise Failure("completed_model_mismatch")
    return result


def xcom(result):
    return {"request_id": result["request_id"], "candidate_id": result["candidate_id"],
            "mlflow_run_id": result["mlflow"]["run_id"], "model_uri": result["mlflow"]["exact_uri"],
            "receipt_uri": result["receipt_uri"]}


def run(conf):
    validate(conf)
    work = Path(os.environ.get("ATS_WORK_ROOT", "/work"))
    state = Path(os.environ.get("ATS_STATE_ROOT", "/state"))
    root = work / "requests" / conf["request_id"]
    root.mkdir(parents=True, exist_ok=True)
    identity = read(os.environ["ATS_RUNTIME_IMAGE_FILE"])
    payload_hash = digest({"conf": conf, "image": identity, "lock": file_hash("/opt/requirements-ml.lock")})
    with (root / "request.lock").open("a") as lock, database(state / "requests.sqlite3") as db:
        fcntl.flock(lock, fcntl.LOCK_EX)
        row = claim(db, conf["request_id"], payload_hash)
        if row and row["status"] == "completed":
            return xcom(verify_result(root, row["receipt_hash"], conf))
        # Candidate published before a lost SQLite commit: recover exact evidence.
        if (root / "candidate.json").exists():
            result = verify_result(root, conf=conf)
            recovered(db, conf["request_id"], file_hash(root / "candidate.json"))
            return xcom(result)
        if (root / "repro-intent.json").exists():
            # Recover a complete audited repro receipt; never run DVC again here.
            source = root / "run/source"
            process = subprocess.run(["/opt/ml-venv/bin/python", "-m", "ats_mlops.orchestration", "recover", "--root", str(root)],
                                     env=ml_env(source), capture_output=True, text=True)
            if process.returncode:
                with db:
                    db.execute("UPDATE requests SET status='recovery_required',kind='recovery_required' WHERE request_id=?", (conf["request_id"],))
                raise Failure("recovery_required")
            result = verify_result(root, conf=conf)
            recovered(db, conf["request_id"], file_hash(root / "candidate.json"))
            return xcom(result)
        if row and row["attempts"] >= 3:
            finish(db, conf["request_id"], row["attempts"], "failed", kind="retry_budget_exhausted", phase="recovered")
            raise Failure("retry_budget_exhausted")
        attempt = begin_attempt(db, conf["request_id"])
        phase = "download"
        try:
            download(conf, root / "bundle", attempt)
            phase = "initialize"
            run_root = root / "run"
            if run_root.exists():
                shutil.rmtree(run_root)
            from ats_mlops.pipeline import initialize
            repository = root / "repository.git"
            if not repository.exists():
                external = Path(os.environ["ATS_SOURCE_REPO"])
                command(["git", "-c", f"safe.directory={external}", "-c", f"safe.directory={external / '.git'}",
                         "clone", "--bare", "--no-local", "--", str(external), str(repository)], env=ml_env())
            initialize(repository, conf["git_sha"], run_root, root / "bundle",
                       conf["bundle_inventory_sha256"], identity)
            source, workspace = run_root / "source", run_root / "workspace"
            if file_hash(source / "requirements-ml.lock") != file_hash("/opt/requirements-ml.lock"):
                raise Failure("lock_mismatch")
            if not (source / "src/ats_mlops/orchestration.py").is_file():
                raise Failure("unsupported_source_revision")
            manifest = read(workspace / "data_manifest.json")
            if manifest["generator"]["config"]["namespace"] != conf["dataset_version"]:
                raise Failure("dataset_version_mismatch")
            write(workspace / "params.yaml", conf["parameters"])
            command(["/opt/ml-venv/bin/dvc", "cache", "dir", str(work / "dvc-cache")], cwd=workspace, env=ml_env(source))
            for path in sorted(source.rglob("*"), reverse=True):
                if not path.is_symlink():
                    path.chmod(path.stat().st_mode & ~0o222)
            source.chmod(source.stat().st_mode & ~0o222)
            write(root / "request.json", conf)
            phase = "repro"
            write(root / "repro-intent.json", {"request_id": conf["request_id"], "payload_hash": payload_hash, "attempt": attempt})
            process = subprocess.run(["/opt/ml-venv/bin/python", "-m", "ats_mlops.orchestration", "execute", "--root", str(root)],
                                     cwd=workspace, env=ml_env(source), capture_output=True, text=True)
            if process.returncode:
                failure = read(root / "worker-error.json") if (root / "worker-error.json").exists() else {"kind": "recovery_required"}
                raise Failure(failure["kind"])
            result = verify_result(root, conf=conf)
            finish(db, conf["request_id"], attempt, "completed", receipt_hash=file_hash(root / "candidate.json"), phase=phase)
            return xcom(result)
        except Exception as error:
            error = error if isinstance(error, Failure) else Failure("invalid_input_or_runtime")
            retryable = error.retryable and attempt < 3 and phase == "download"
            status = "retryable" if retryable else "recovery_required" if error.kind == "recovery_required" else "failed"
            finish(db, conf["request_id"], attempt, status, kind=error.kind, phase=phase)
            write(root / f"attempt-{attempt}.json", {"attempt": attempt, "phase": phase, "kind": error.kind, "retryable": retryable})
            raise Failure(error.kind, retryable) from None


def execute(root, recover=False):
    """Runs from exact source PYTHONPATH, with only the ML credential allowlist."""
    from ats_mlops.pipeline import context, reproduce
    source, workspace = root / "run/source", root / "run/workspace"
    conf = read(root / "request.json")
    if recover:
        context(workspace, source)
        receipts = [read(path) for path in (workspace / ".local/repro").glob("*.json")]
        if len(receipts) != 1 or receipts[0]["outcome"] not in {"new_training", "cache_hit"}:
            raise Failure("recovery_required")
        result = receipts[0]
        if (result["pipeline_manifest_sha256"] != file_hash(workspace / "artifacts/evaluation/pipeline_manifest.json")
                or result["dvc_lock_sha256"] != file_hash(workspace / "dvc.lock")):
            raise Failure("recovery_required")
    else:
        result = reproduce(workspace, source)
    write(root / "repro.json", result)
    if result["outcome"] == "failed":
        raise Failure(repro_failure_kind(result))
    manifest_path = workspace / "artifacts/evaluation/pipeline_manifest.json"
    manifest = read(manifest_path)
    mlflow = manifest["training"]["mlflow"]
    receipt = {"schema_version": 1, "request_id": conf["request_id"], "run_id": conf["run_id"], "request_sha256": digest(conf),
               "candidate_id": "candidate-" + digest({"request": conf["request_id"], "model": mlflow})[:24],
               "outcome": result["outcome"], "mlflow": mlflow, "git_sha": conf["git_sha"],
               "bundle_inventory_sha256": conf["bundle_inventory_sha256"], "params_hash": conf["params_hash"],
               "pipeline_manifest_sha256": file_hash(manifest_path), "dvc_lock_sha256": result["dvc_lock_sha256"],
               "receipt_uri": (root / "candidate.json").as_uri()}
    write(root / "candidate.json", receipt)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("run", "execute", "recover"))
    parser.add_argument("--root", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.action in {"execute", "recover"}:
            execute(args.root, recover=args.action == "recover")
        else:
            print(json.dumps({"result": run(json.load(sys.stdin))}))
        return 0
    except Exception as error:
        failure = error if isinstance(error, Failure) else Failure("invalid_input_or_runtime")
        protocol = {"kind": failure.kind, "retryable": failure.retryable}
        if args.action in {"execute", "recover"}:
            write(args.root / "worker-error.json", protocol)
        print(json.dumps(protocol))
        return 75 if failure.retryable else 1


if __name__ == "__main__":
    raise SystemExit(main())
