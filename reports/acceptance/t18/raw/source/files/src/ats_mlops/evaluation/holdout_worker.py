"""Standalone offline holdout scorer: execute as /worker.py with Python -I -B.

Only stdlib imports occur until the exact MLflow artifact has passed the T11
boundary. READY precedes the single stdin snapshot. The caller owns the durable
holdout guard and the approved train/validation warmup identity.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import importlib.metadata
import importlib.util
import json
import math
from numbers import Real
import os
from pathlib import Path
import re
import sys
import tempfile
from time import perf_counter
from typing import Callable

MAX_BYTES = 16 * 1024 * 1024
ROW_KEYS = {"vacancy_id", "candidate_id", "vacancy_text", "candidate_text", "relevance"}
OBSERVATION_KEYS = {"rows", "groups", "latencies", "snapshot_sha256", "model_artifact_sha256",
                    "baseline_artifact_sha256", "evaluated_groups_once", "warmup"}
HEX = re.compile(r"[0-9a-f]{64}\Z")
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
WARMUP = "train_validation_only"


class WorkerError(ValueError):
    pass


def require(condition, reason="holdout_worker_invalid"):
    if not condition:
        raise WorkerError(reason)


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def sha(content):
    return hashlib.sha256(content).hexdigest()


def worker_identity():
    return sha(Path(__file__).read_bytes())


def valid_hash(value):
    return isinstance(value, str) and HEX.fullmatch(value) is not None


def identifier(value):
    return isinstance(value, str) and ID.fullmatch(value) is not None


def decode(content):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    require(len(content) <= MAX_BYTES, "input_size_limit")
    return json.loads(content, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(WorkerError("nonfinite_json")))


def read_content(path):
    path = Path(path)
    require(path.is_file() and not path.is_symlink(), "invalid_json_path")
    with path.open("rb") as stream:
        content = stream.read(MAX_BYTES + 1)
    require(len(content) <= MAX_BYTES, "input_size_limit")
    return content


def read_json(path):
    return decode(read_content(path))


@contextmanager
def quiet():
    # Incidental library banners/warnings never become protocol or text evidence.
    saved = (os.dup(1), os.dup(2))
    try:
        with open(os.devnull, "w") as sink:
            os.dup2(sink.fileno(), 1);os.dup2(sink.fileno(), 2)
            with redirect_stdout(sink), redirect_stderr(sink):
                yield
    finally:
        os.dup2(saved[0], 1);os.dup2(saved[1], 2)
        os.close(saved[0]);os.close(saved[1])


def _fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish(path, value):
    """Publish complete numeric/metadata bytes exactly once, never a torn final."""
    path = Path(path)
    require(path.parent.is_dir() and not path.parent.is_symlink(), "unsafe_output_directory")
    require(not path.exists() and not path.is_symlink(), "immutable_output_exists")
    descriptor, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical(value));stream.flush();os.fsync(stream.fileno())
        os.link(temporary, path)
        _fsync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def project_absent():
    require(not any(name == "ats_mlops" or name.startswith("ats_mlops.") for name in sys.modules), "prior_project_import")
    require(importlib.util.find_spec("ats_mlops") is None, "prior_project_namespace")


def verify_environment(record):
    require(sys.flags.isolated and sys.dont_write_bytecode, "isolated_bytecode_disabled_required")
    project_absent()
    require(not any(os.environ.get(name) for name in (
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "MLFLOW_TRACKING_URI",
        "MLFLOW_TRACKING_TOKEN", "DATABASE_URL", "PGPASSWORD")), "credentials_forbidden")
    environment = record["environment"]
    require(sys.version.split()[0] == environment["python"], "python_version_mismatch")
    require(isinstance(environment["packages"], dict) and environment["packages"], "missing_package_pins")
    for name, version in environment["packages"].items():
        require(importlib.metadata.version(name) == version, "package_version_mismatch")
    require(sha(Path("/opt/requirements-ml.lock").read_bytes()) == environment["lockfile_sha256"], "lockfile_mismatch")


def verify_artifact(record, artifact):
    artifact = Path(artifact)
    require(artifact.is_dir() and not artifact.is_symlink(), "invalid_artifact_root")
    files = {}
    for path in sorted(artifact.rglob("*")):
        require(not path.is_symlink(), "artifact_symlink")
        relative = path.relative_to(artifact)
        require("__pycache__" not in relative.parts and path.suffix.lower() not in (".pyc", ".pyo"), "artifact_bytecode")
        if path.is_file():
            files[relative.as_posix()] = sha(path.read_bytes())
        else:
            require(path.is_dir(), "artifact_special_file")
    require(isinstance(record["model"]["files"], dict) and files == record["model"]["files"]
            and files and all(valid_hash(value) for value in files.values()), "model_inventory_mismatch")
    require(sha(canonical(files)) == record["model"]["sha256"]
            and files["model.pkl"] == record["model"]["payload_sha256"], "model_payload_mismatch")
    source = {name.removeprefix("code/ats_mlops/"): value for name, value in files.items()
              if name.startswith("code/ats_mlops/")}
    require(source == record["source_sha256"] and sha(canonical(source)) == record["source_tree_sha256"], "source_inventory_mismatch")
    require(record["dirty_worktree"] is False and type(record["schema_version"]) is int and record["schema_version"] == 1
            and re.fullmatch(r"[0-9a-f]{40}", record["git_sha"]) is not None, "invalid_model_record")
    return files


def baseline_identity(record):
    source = {name: record["source_sha256"][name] for name in ("model/baseline.py", "data/dataset.py")}
    require(all(valid_hash(value) for value in source.values()), "missing_baseline_source")
    return sha(canonical({"version": "jaccard-v1", "source_sha256": source}))


def check_metadata(record, metadata, registered):
    native, pyfunc = metadata["flavors"]["sklearn"], metadata["flavors"]["python_function"]
    require(native["code"] == "code" and native["pickled_model"] == "model.pkl"
            and native["serialization_format"] == "cloudpickle", "invalid_native_flavor")
    require(pyfunc["code"] == "code" and pyfunc["model_path"] == "model.pkl"
            and pyfunc["loader_module"] == "mlflow.sklearn" and pyfunc["predict_fn"] == "predict_proba", "invalid_pyfunc_flavor")
    identity = record["mlflow"]
    require(isinstance(identity["model_version"], str) and identity["model_version"].isdecimal()
            and str(int(identity["model_version"])) == identity["model_version"] and int(identity["model_version"]) > 0,
            "exact_model_version_required")
    require(metadata["run_id"] == identity["run_id"] and identity["source"] == "models:/" + metadata["model_id"]
            and registered["model_name"] == identity["model_name"]
            and str(registered["model_version"]) == identity["model_version"], "registered_identity_mismatch")
    require(metadata["metadata"] == {"classes": [0, 1], "feature_version": record["features"]["version"],
            "features_sha256": record["features_sha256"], "training_git_sha": record["git_sha"]}, "model_metadata_mismatch")


def _metadata(record, artifact):
    import yaml
    check_metadata(record, yaml.safe_load((artifact / "MLmodel").read_text()),
                   yaml.safe_load((artifact / "registered_model_meta").read_text()))


def _load_model(artifact):
    import mlflow.sklearn
    project_absent()  # importing MLflow must not have claimed the project namespace
    return mlflow.sklearn.load_model(str(artifact))


def check_namespace(artifact):
    for name, module in list(sys.modules.items()):
        if name == "ats_mlops" or name.startswith("ats_mlops."):
            require(Path(module.__file__).resolve().is_relative_to(artifact / "code"), "artifact_namespace_pollution")


def _helpers(model, record, artifact):
    # All project imports are after the exact artifact load, never before it.
    from ats_mlops.model import baseline, scoring
    import ats_mlops.features.pairs as features
    from sklearn.linear_model import LogisticRegression
    require(baseline.MODEL_VERSION == "jaccard-v1" and features.FEATURE_VERSION == record["features"]["version"], "feature_version_mismatch")
    transformer = model.named_steps["pair_features"]
    require(transformer.feature_version_ == record["features"]["version"]
            and list(transformer.get_feature_names_out()) == record["features"]["names"]
            and transformer.max_features == record["params"]["features"]["max_features"]
            and list(transformer.ngram_range) == record["params"]["features"]["ngram_range"]
            and transformer.vectorizer_.max_features == transformer.max_features
            and transformer.vectorizer_.ngram_range == transformer.ngram_range, "fitted_feature_mismatch")
    classifier = model.named_steps["classifier"]
    require(isinstance(classifier, LogisticRegression) and classifier.get_params() == record["effective_classifier_params"]
            and all(classifier.get_params()[name] == value for name, value in record["params"]["classifier"].items())
            and classifier.random_state == record["seed"], "fitted_classifier_mismatch")
    check_namespace(artifact)
    return baseline.score_rows, lambda rows: scoring.score_rows(model, rows)


def validated_rows(rows):
    require(isinstance(rows, list) and rows, "empty_snapshot")
    seen = set()
    for row in rows:
        require(isinstance(row, dict) and set(row) == ROW_KEYS and identifier(row["vacancy_id"])
                and identifier(row["candidate_id"]) and type(row["relevance"]) is int and row["relevance"] in (0, 1)
                and all(isinstance(row[key], str) and row[key].strip() for key in ("vacancy_text", "candidate_text")), "invalid_snapshot_row")
        key = row["vacancy_id"], row["candidate_id"]
        require(key not in seen, "duplicate_snapshot_pair");seen.add(key)
    return rows


def truth_rows(rows):
    return [{"group_id": row["vacancy_id"], "candidate_id": row["candidate_id"], "relevance": row["relevance"]}
            for row in sorted(rows, key=lambda item: (item["vacancy_id"], item["candidate_id"]))]


def pair_digest(rows):
    return sha(canonical(sorted(row["group_id"] + "/" + row["candidate_id"] for row in rows)))


def score(value):
    # Explicit native conversion also covers NumPy real scalars from frozen helpers.
    require(not isinstance(value, bool) and isinstance(value, Real), "invalid_numeric_score")
    result = float(value)
    require(math.isfinite(result) and 0 <= result <= 1, "invalid_numeric_score")
    return result


def checked_scores(values, rows):
    expected = {(row["vacancy_id"], row["candidate_id"]) for row in rows}
    require(isinstance(values, dict) and set(values) == expected, "incomplete_scored_pairs")
    return {key: score(value) for key, value in values.items()}


@dataclass
class Session:
    baseline: Callable
    model: Callable
    namespace: Callable
    ready: dict


def prepare(request, artifact=Path("/artifact")):
    require(isinstance(request, dict) and set(request) == {"schema_version", "record", "warmup_rows", "baseline_artifact_sha256"}
            and type(request["schema_version"]) is int and request["schema_version"] == 1, "invalid_preflight_request")
    record = request["record"];artifact = Path(artifact)
    require(not artifact.is_symlink(), "invalid_artifact_root")
    artifact = artifact.resolve()
    warmup = validated_rows(request["warmup_rows"])
    require(len({row["vacancy_id"] for row in warmup}) == 1, "one_approved_warmup_group_required")
    verify_environment(record)
    verify_artifact(record, artifact)
    require(baseline_identity(record) == request["baseline_artifact_sha256"], "baseline_identity_mismatch")
    with quiet():
        _metadata(record, artifact)
        model = _load_model(artifact)
        baseline, trained = _helpers(model, record, artifact)
        checked_scores(baseline(deepcopy(warmup)), warmup)
        checked_scores(trained(deepcopy(warmup)), warmup)
        check_namespace(artifact)
    ready = {"status": "ready", "worker_sha256": worker_identity(), "receipt": {"isolated": True, "bytecode_disabled": True,
        "prior_project_import": False, "network": False, "training_checkout_mounted": False, "credentials": False,
        "model_sha256": record["model"]["sha256"], "baseline_artifact_sha256": request["baseline_artifact_sha256"],
        "source_tree_sha256": record["source_tree_sha256"]}, "warmup": WARMUP}
    return Session(baseline, trained, lambda: check_namespace(artifact), ready)


def validate_snapshot(message):
    require(isinstance(message, dict) and set(message) == {"schema_version", "snapshot_sha256", "pair_ids_sha256",
            "ground_truth_sha256", "snapshot"} and type(message["schema_version"]) is int and message["schema_version"] == 1
            and all(valid_hash(message[key]) for key in ("snapshot_sha256", "pair_ids_sha256", "ground_truth_sha256"))
            and isinstance(message["snapshot"], dict) and set(message["snapshot"]) == {"rows"}, "invalid_snapshot_message")
    rows = validated_rows(message["snapshot"]["rows"])
    truth = truth_rows(rows)
    require(sha(b"".join(canonical(row) for row in rows)) == message["snapshot_sha256"], "snapshot_bytes_mismatch")
    require(pair_digest(truth) == message["pair_ids_sha256"] and sha(canonical(truth)) == message["ground_truth_sha256"], "snapshot_truth_mismatch")
    return rows, {"schema_version": 1, "rows": truth, **{key: message[key] for key in
                  ("snapshot_sha256", "pair_ids_sha256", "ground_truth_sha256")}}


def evaluate(session, rows, truth, *, clock=perf_counter):
    output, latencies = [], []
    groups = sorted({row["vacancy_id"] for row in rows})
    for group in groups:
        batch = [row for row in rows if row["vacancy_id"] == group]
        with quiet():
            start = clock();baseline = checked_scores(session.baseline(deepcopy(batch)), batch);baseline_time = float(clock() - start)
            start = clock();model = checked_scores(session.model(deepcopy(batch)), batch);model_time = float(clock() - start)
            session.namespace()
        require(all(math.isfinite(value) and value >= 0 for value in (baseline_time, model_time)), "invalid_group_latency")
        latencies.append({"group_id": group, "baseline_seconds": baseline_time, "model_seconds": model_time})
        for row in sorted(batch, key=lambda item: item["candidate_id"]):
            key = group, row["candidate_id"]
            output.append({"group_id": group, "candidate_id": key[1], "relevance": row["relevance"],
                           "baseline_score": baseline[key], "model_score": model[key]})
    receipt = session.ready["receipt"]
    return {"rows": output, "groups": groups, "latencies": latencies, "snapshot_sha256": truth["snapshot_sha256"],
            "model_artifact_sha256": receipt["model_sha256"], "baseline_artifact_sha256": receipt["baseline_artifact_sha256"],
            "evaluated_groups_once": len(groups), "warmup": WARMUP}


def validate_numeric(ready, truth, numeric):
    require(set(ready) == {"status", "worker_sha256", "receipt", "warmup"} and ready["status"] == "ready"
            and ready["worker_sha256"] == worker_identity() and ready["warmup"] == WARMUP, "invalid_ready_receipt")
    receipt = ready["receipt"]
    require(set(receipt) == {"isolated", "bytecode_disabled", "prior_project_import", "network", "training_checkout_mounted",
            "credentials", "model_sha256", "baseline_artifact_sha256", "source_tree_sha256"}
            and receipt["isolated"] is True and receipt["bytecode_disabled"] is True
            and all(receipt[key] is False for key in ("prior_project_import", "network", "training_checkout_mounted", "credentials"))
            and all(valid_hash(receipt[key]) for key in ("model_sha256", "baseline_artifact_sha256", "source_tree_sha256")), "invalid_ready_receipt")
    require(set(truth) == {"schema_version", "rows", "snapshot_sha256", "pair_ids_sha256", "ground_truth_sha256"}
            and type(truth["schema_version"]) is int and truth["schema_version"] == 1
            and all(valid_hash(truth[key]) for key in ("snapshot_sha256", "pair_ids_sha256", "ground_truth_sha256")), "invalid_saved_truth")
    gt = truth["rows"]
    require(isinstance(gt, list) and gt and all(set(row) == {"group_id", "candidate_id", "relevance"}
            and identifier(row["group_id"]) and identifier(row["candidate_id"]) and type(row["relevance"]) is int
            and row["relevance"] in (0, 1) for row in gt), "invalid_saved_truth")
    ids = [(row["group_id"], row["candidate_id"]) for row in gt]
    require(ids == sorted(set(ids)) and pair_digest(gt) == truth["pair_ids_sha256"]
            and sha(canonical(gt)) == truth["ground_truth_sha256"], "saved_truth_changed")
    groups = sorted({row["group_id"] for row in gt})
    require(set(numeric) == OBSERVATION_KEYS and numeric["groups"] == groups
            and type(numeric["evaluated_groups_once"]) is int and numeric["evaluated_groups_once"] == len(groups)
            and numeric["snapshot_sha256"] == truth["snapshot_sha256"] and numeric["warmup"] == WARMUP
            and numeric["model_artifact_sha256"] == receipt["model_sha256"]
            and numeric["baseline_artifact_sha256"] == receipt["baseline_artifact_sha256"], "numeric_binding_changed")
    require(isinstance(numeric["rows"], list) and len(numeric["rows"]) == len(gt), "numeric_pair_count_mismatch")
    for row, original in zip(numeric["rows"], gt, strict=True):
        require(set(row) == {"group_id", "candidate_id", "relevance", "baseline_score", "model_score"}
                and canonical({key: row[key] for key in original}) == canonical(original), "numeric_ground_truth_changed")
        score(row["baseline_score"]);score(row["model_score"])
    latencies = numeric["latencies"]
    require(isinstance(latencies, list) and [row["group_id"] for row in latencies] == groups
            and all(set(row) == {"group_id", "baseline_seconds", "model_seconds"}
                    and all(type(row[key]) in (int, float) and math.isfinite(row[key]) and row[key] >= 0
                            for key in ("baseline_seconds", "model_seconds")) for row in latencies), "invalid_saved_latency")


def numeric_seal(ready, truth, numeric):
    """Freeze the computed bytes while still in memory, BEFORE publishing them."""
    return {"schema_version": 1, "phase": "scores_frozen", "worker_sha256": worker_identity(),
        "ready_sha256": sha(canonical(ready)), "truth_sha256": sha(canonical(truth)),
        "numeric_sha256": sha(canonical(numeric)), "model_artifact_sha256": numeric["model_artifact_sha256"],
        "baseline_artifact_sha256": numeric["baseline_artifact_sha256"],
        "source_tree_sha256": ready["receipt"]["source_tree_sha256"],
        "rows": len(truth["rows"]), "groups": len(numeric["groups"]),
        **{key: truth[key] for key in ("snapshot_sha256", "pair_ids_sha256", "ground_truth_sha256")}}


def recover(output):
    """Finish only complete persisted numeric results; never load or score again."""
    output = Path(output)
    require(output.is_dir() and not output.is_symlink(), "unsafe_output_directory")
    # No seal is ever synthesized here: a crash before sealing is unknown.
    seal_bytes = read_content(output / "score-seal.json")
    seal = decode(seal_bytes)
    require(seal_bytes == canonical(seal), "noncanonical_score_seal")
    contents = {name: read_content(output / (name + ".json")) for name in ("ready", "truth", "numeric")}
    ready, truth, numeric = (decode(contents[name]) for name in ("ready", "truth", "numeric"))
    validate_numeric(ready, truth, numeric)
    require(canonical(seal) == canonical(numeric_seal(ready, truth, numeric))
            and all(sha(content) == seal[name + "_sha256"] for name, content in contents.items()), "numeric_score_seal_changed")
    commit = {"schema_version": 1, "status": "completed", "worker_sha256": worker_identity(),
        "ready_sha256": seal["ready_sha256"], "truth_sha256": seal["truth_sha256"],
        "numeric_sha256": seal["numeric_sha256"], "score_seal_sha256": sha(seal_bytes),
        "rows": len(truth["rows"]), "groups": len(numeric["groups"]),
        **{key: truth[key] for key in ("snapshot_sha256", "pair_ids_sha256", "ground_truth_sha256")}}
    if (output / "commit.json").exists():
        require(canonical(read_json(output / "commit.json")) == canonical(commit), "numeric_commit_changed")
    else:
        publish(output / "commit.json", commit)
    return {"status": "completed", "numeric_sha256": commit["numeric_sha256"],
            "commit_sha256": sha((output / "commit.json").read_bytes())}


def serve(session, output, input_stream, output_stream):
    output = Path(output)
    require(output.is_dir() and not output.is_symlink() and not any(output.iterdir()), "fresh_output_required")
    publish(output / "ready.json", session.ready)
    output_stream.write(canonical(session.ready).decode());output_stream.flush()
    # Parent sends this one message and EOF only AFTER its native guard commit.
    rows, truth = validate_snapshot(decode(input_stream.read(MAX_BYTES + 1)))
    publish(output / "truth.json", truth)  # immutable numeric GT before either scorer
    numeric = evaluate(session, rows, truth)
    validate_numeric(session.ready, truth, numeric)
    publish(output / "score-seal.json", numeric_seal(session.ready, truth, numeric))
    publish(output / "numeric.json", numeric)
    completed = recover(output)
    output_stream.write(canonical(completed).decode());output_stream.flush()
    return completed


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--request", type=Path)
    mode.add_argument("--recover", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.recover:
            print(canonical(recover(args.output)).decode(), end="", flush=True)
        else:
            session = prepare(read_json(args.request))
            serve(session, args.output, sys.stdin.buffer, sys.stdout)
        return 0
    except BaseException:
        # Never echo parser/library exceptions, texts, paths, or credentials.
        print('{"status":"failed","reason":"holdout_worker_failed"}', flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
