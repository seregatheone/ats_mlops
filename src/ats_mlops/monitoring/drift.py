"""Batch numeric reports. Evidently imports only inside the dedicated renderer."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import subprocess
import tempfile
import uuid

from .feedback import (FEATURES, NUMERIC, FeedbackStore, admit_feedback, canonical, checked_bytes, decode_json, export_events,
                       identifier, load_export, load_feedback, numeric, publication, read_json,
                       require, sha, utc, validate_events, verified, version, write_document)
from .quality import assess

CONFIG_KEYS = {"schema_version", "method", "columns", "threshold_grid", "drift_share", "minimum_pairs",
               "minimum_vacancies", "calibration_seed", "label_sources", "constant_policy", "scope"}
CANONICAL_PARTITIONS = {"train": "adef4de05783391cef598098b8886221bf4d8f28d9e296d74f94e8cf66a72ea7",
                        "validation": "1cdd5f3189ba4241f5bb2399f33dac47f6b38e21a04819aa0c07900d1e29e0a8"}


def configuration(path, *, with_sha=False):
    require(not Path(path).is_symlink(), "input_symlink")
    content = Path(path).read_bytes()
    value = decode_json(content)
    require(isinstance(value, dict) and set(value) == CONFIG_KEYS and type(value["schema_version"]) is int
            and value["schema_version"] == 1, "invalid_monitoring_config")
    require(value["method"] == "wasserstein" and value["columns"] == list(NUMERIC), "unsupported_drift_schema")
    require(type(value["minimum_pairs"]) is int and value["minimum_pairs"] >= 200
            and type(value["minimum_vacancies"]) is int and value["minimum_vacancies"] >= 10, "unsafe_window_minimum")
    grid = value["threshold_grid"]
    require(isinstance(grid, list) and grid and all(numeric(x) and x > 0 for x in grid)
            and grid == sorted(set(grid)), "invalid_threshold_grid")
    require(numeric(value["drift_share"], unit=True) and value["drift_share"] > 0
            and type(value["calibration_seed"]) is int, "invalid_calibration_config")
    require(isinstance(value["label_sources"], list) and value["label_sources"]
            and set(value["label_sources"]) <= {"independent-fixture-v1", "human-review-v1"}
            and len(set(value["label_sources"])) == len(value["label_sources"]), "unapproved_label_sources")
    require(value["scope"] == "demo_only" and value["constant_policy"] == "exclude_unchanged; changed_or_all_constant_is_undefined", "unsupported_monitoring_scope")
    return (value, sha(content)) if with_sha else value


def numeric_rows(rows):
    require(isinstance(rows, list) and rows, "empty_numeric_data")
    result = []
    for row in rows:
        require(isinstance(row, dict) and set(row) == set(NUMERIC), "numeric_schema_mismatch")
        require(all(numeric(value, unit=key not in ("log1p_vacancy_tokens", "log1p_candidate_tokens"))
                    for key, value in row.items()), "nonfinite_or_invalid_numeric_data")
        result.append({key: float(row[key]) for key in NUMERIC})
    return result


def read_numeric(path):
    return numeric_rows([decode_json(line) for line in Path(path).read_bytes().splitlines()])


def constants(reference):
    reference = numeric_rows(reference)
    return {column: reference[0][column] for column in NUMERIC
            if all(row[column] == reference[0][column] for row in reference)}


def render(reference, current, config, threshold, output, *, timestamp):
    """Real pinned Evidently Report. Undefined references never become no-drift."""
    reference, current = numeric_rows(reference), numeric_rows(current)
    fixed = constants(reference)
    require(all(all(row[name] == value for row in current) for name, value in fixed.items()), "constant_reference_changed")
    columns = [name for name in NUMERIC if name not in fixed]
    require(columns, "degenerate_constant_reference")
    # The ordinary ML/CLI/policy path never imports this environment.
    import pandas as pd
    from evidently import Report
    from evidently.presets import DataDriftPreset

    require(importlib.metadata.version("evidently") == "0.7.23", "evidently_version_mismatch")
    report = Report([DataDriftPreset(columns=columns, num_method=config["method"], num_threshold=threshold,
                                   drift_share=config["drift_share"])], include_tests=True)
    snapshot = report.run(pd.DataFrame(current, columns=columns, dtype="float64"),
                          pd.DataFrame(reference, columns=columns, dtype="float64"), timestamp=utc(timestamp))
    output = Path(output)
    snapshot.save_json(str(output / "evidently.json"))
    snapshot.save_html(str(output / "evidently.html"))
    # The released dict() contains numpy.float64 values. Cross the actual JSON
    # interface before enforcing the same strict JSON-number contract as inputs.
    return normalize_evidently(decode_json(snapshot.json()), columns, config, threshold, fixed)


def normalize_evidently(value, columns, config, threshold, fixed):
    """Pinned0.7.23 adapter; populated from actual smoke, unknown layouts fail closed."""
    require(isinstance(value, dict) and isinstance(value.get("metrics"), list), "evidently_result_schema")
    distances = {}
    shares, counts = [], []
    require(len(value["metrics"]) == len(columns) + 1 and isinstance(value.get("tests"), list) and value["tests"]
            and all(test.get("status") in ("SUCCESS", "FAIL") for test in value["tests"]), "undefined_evidently_tests")
    for metric in value["metrics"]:
        name = metric.get("metric_name", "")
        if name.startswith("ValueDrift"):
            column = metric.get("config", {}).get("column")
            require(column in columns and column not in distances, "evidently_column_schema")
            require(metric["config"].get("method") == config["method"] and metric["config"].get("threshold") == threshold, "evidently_method_mismatch")
            distance = metric.get("value")
            require(numeric(distance), "undefined_evidently_distance")
            distances[column] = float(distance)
        elif name.startswith("DriftedColumnsCount"):
            result = metric.get("value")
            require(isinstance(result, dict) and numeric(result.get("share"), unit=True)
                    and numeric(result.get("count")) and result["count"] == int(result["count"]), "undefined_evidently_share")
            shares.append(float(result["share"]))
            counts.append(int(result["count"]))
        else:
            raise ValueError("unexpected_evidently_metric")
    require(set(distances) == set(columns) and len(shares) == 1, "incomplete_evidently_result")
    drifted = [name for name in columns if distances[name] >= threshold]
    require(counts == [len(drifted)] and abs(shares[0] - len(drifted) / len(columns)) <= 1e-12, "evidently_decision_mismatch")
    return {"status": "defined", "detected": shares[0] >= config["drift_share"], "share": shares[0],
            "distances": distances, "drifted_columns": drifted, "constant_columns": fixed,
            "method": config["method"], "threshold": threshold, "drift_share": config["drift_share"]}


def load_reference(directory, expected_sha):
    root = Path(directory)
    content = checked_bytes(root, "reference.json")
    require(sha(content) == expected_sha, "reference_hash_mismatch")
    value = decode_json(content)
    require(isinstance(value, dict) and set(value) == {"schema_version", "evidence_scope", "columns", "model", "matrices", "data", "feature_worker", "test_read", "limitations"}
            and type(value["schema_version"]) is int and value["schema_version"] == 2 and value["columns"] == list(NUMERIC)
            and value["evidence_scope"] == "demo_only" and value["test_read"] is False, "reference_schema_mismatch")
    model, data, worker = value["model"], value["data"], value["feature_worker"]
    model_hashes = {"release_manifest_sha256", "release_check_sha256", "model_sha256", "feature_state_sha256", "feature_schema_sha256", "run_manifest_sha256"}
    require(set(model) == model_hashes | {"model_name", "model_version", "release_id", "lineage_status"}
            and identifier(model["model_name"]) and identifier(model["release_id"]) and version(model["model_version"])
            and all(isinstance(model[key], str) and re.fullmatch(r"[0-9a-f]{64}", model[key]) for key in model_hashes)
            and model["lineage_status"] in ("guarded_schema2", "legacy_engineering_only"), "invalid_reference_model")
    require(set(data) == {"manifest_sha256", "admission_sha256", "bundle_sha256", "reservation_policy_sha256", "partition_sha256", "schema_version", "read_partitions"}
            and type(data["schema_version"]) is int and data["schema_version"] == 2
            and data["read_partitions"] == ["train", "validation"] and data["partition_sha256"] == CANONICAL_PARTITIONS
            and all(isinstance(data[key], str) and re.fullmatch(r"[0-9a-f]{64}", data[key])
                    for key in ("manifest_sha256", "admission_sha256", "bundle_sha256", "reservation_policy_sha256")), "invalid_reference_data")
    require(set(worker) == {"isolated", "bytecode_disabled", "credentials", "network", "training_checkout_mounted", "model_sha256", "feature_state_sha256", "image_id", "bootstrap_sha256"}
            and worker["isolated"] is True and worker["bytecode_disabled"] is True
            and all(worker[key] is False for key in ("credentials", "network", "training_checkout_mounted"))
            and worker["model_sha256"] == model["model_sha256"] and worker["feature_state_sha256"] == model["feature_state_sha256"]
            and isinstance(worker["image_id"], str) and re.fullmatch(r"sha256:[0-9a-f]{64}", worker["image_id"])
            and worker["bootstrap_sha256"] == sha(FEATURE_BOOTSTRAP.encode()), "invalid_reference_worker")
    require(set(value["matrices"]) == {"train", "validation"}, "invalid_reference_roles")
    matrices = {}
    for role in ("train", "validation"):
        descriptor = value["matrices"][role]
        require(set(descriptor) == {"sha256", "pairs", "groups", "groups_sha256"}
                and all(type(descriptor[key]) is int and descriptor[key] > 0 for key in ("pairs", "groups")), "invalid_reference_counts")
        content = checked_bytes(root, role + ".jsonl")
        require(sha(content) == descriptor["sha256"], "reference_numeric_hash_mismatch")
        matrices[role] = numeric_rows([decode_json(line) for line in content.splitlines()])
        content = checked_bytes(root, role + "-groups.json")
        groups = decode_json(content)
        require(isinstance(groups, list) and all(isinstance(group, str) and re.fullmatch(r"[0-9a-f]{64}", group) for group in groups)
                and sha(content) == descriptor["groups_sha256"] and len(groups) == len(matrices[role]) == descriptor["pairs"]
                and len(set(groups)) == descriptor["groups"], "reference_numeric_counts_mismatch")
    return value, matrices


def calibrate(reference, reference_sha, config_path, output):
    config, config_sha = configuration(config_path, with_sha=True)
    card, matrices = load_reference(reference, reference_sha)
    require(all(card["matrices"][role]["pairs"] >= config["minimum_pairs"] and card["matrices"][role]["groups"] >= config["minimum_vacancies"]
                for role in ("train", "validation")), "insufficient_calibration_data")
    ref, control = matrices["train"], matrices["validation"]
    shuffled = list(control)
    random.Random(config["calibration_seed"]).shuffle(shuffled)
    fixed = constants(ref)
    active = [name for name in NUMERIC if name not in fixed]
    require(active, "degenerate_constant_reference")
    shifted = []
    for row in control:
        current = dict(row)
        for name in active:
            if name in ("log1p_vacancy_tokens", "log1p_candidate_tokens"):
                current[name] = max(x[name] for x in ref) + 5.
            else:
                current[name] = 1. if sum(x[name] for x in ref) / len(ref) < .5 else 0.
        shifted.append(current)
    observations = []
    chosen = None
    with publication(output) as root:
        for index, threshold in enumerate(config["threshold_grid"]):
            results = {}
            for name, current in (("control", control), ("permuted_control", shuffled), ("shifted", shifted)):
                folder = root / f"threshold-{index}-{name}"
                folder.mkdir()
                results[name] = render(ref, current, config, threshold, folder, timestamp="2026-01-01T00:00:00Z")
            observations.append({"threshold": threshold, "results": results})
            if not results["control"]["detected"] and not results["permuted_control"]["detected"] and results["shifted"]["detected"]:
                chosen = threshold
                break
        require(chosen is not None, "calibration_failed")
        files = {path.relative_to(root).as_posix(): sha(path.read_bytes()) for path in root.rglob("*") if path.is_file()}
        result = {"schema_version": 1, "outcome": "passed", "evidence_scope": "demo_only", "reference_sha256": reference_sha,
                  "config_sha256": config_sha, "method": config["method"], "threshold": chosen,
                  "drift_share": config["drift_share"], "columns": list(NUMERIC), "constant_columns": fixed,
                  "seed": config["calibration_seed"], "observations": observations, "files": files,
                  "scenario": "canonical train reference; validation control/permutation; explicit synthetic numeric perturbation, not model truth",
                  "runtime": runtime()}
        write_document(root / "calibration.json", result)
    return result


def runtime():
    source_sha = os.environ.get("ATS_MONITORING_SOURCE_SHA")
    require(isinstance(source_sha, str) and re.fullmatch(r"[0-9a-f]{40}", source_sha), "missing_monitoring_source_identity")
    return {"python": platform.python_version(), "packages": {name: importlib.metadata.version(name)
            for name in ("evidently", "pandas", "numpy", "scipy", "scikit-learn", "plotly")},
            "monitoring_lock_sha256": sha(Path("/opt/requirements-monitoring.lock").read_bytes()),
            "source_git_sha": source_sha,
            "code_sha256": {name: sha(Path(__file__).with_name(name).read_bytes()) for name in ("drift.py", "feedback.py", "quality.py")}}


FEATURE_BOOTSTRAP = r'''
import hashlib,importlib.util,importlib.metadata,json,math,os,pathlib,sys
def canonical(x):return (json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False)+'\n').encode()
def sha(x):return hashlib.sha256(x).hexdigest()
try:
 assert sys.flags.isolated and sys.dont_write_bytecode
 assert importlib.util.find_spec('ats_mlops') is None
 assert not any(n=='ats_mlops' or n.startswith('ats_mlops.') for n in sys.modules)
 assert not any(os.environ.get(n) for n in ('MLFLOW_TRACKING_URI','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN'))
 request=json.loads(pathlib.Path('/input/request.json').read_text());record=request['record'];artifact=pathlib.Path('/artifact')
 files={}
 for p in artifact.rglob('*'):
  assert not p.is_symlink() and '__pycache__' not in p.parts and p.suffix.lower() not in ('.pyc','.pyo')
  if p.is_file():files[p.relative_to(artifact).as_posix()]=sha(p.read_bytes())
 assert files==record['model']['files'] and sha(canonical(files))==record['model']['sha256']
 assert files['model.pkl']==record['model']['payload_sha256']
 source={n.removeprefix('code/ats_mlops/'):v for n,v in files.items() if n.startswith('code/ats_mlops/')}
 assert source==record['source_sha256'] and sha(canonical(source))==record['source_tree_sha256']
 assert sys.version.split()[0]==record['environment']['python']
 assert sha(pathlib.Path('/opt/requirements-ml.lock').read_bytes())==record['environment']['lockfile_sha256']
 for n,v in record['environment']['packages'].items():assert importlib.metadata.version(n)==v
 import yaml
 metadata=yaml.safe_load((artifact/'MLmodel').read_text());native=metadata['flavors']['sklearn']
 assert native['code']=='code' and native['pickled_model']=='model.pkl' and native['serialization_format']=='cloudpickle'
 assert metadata['run_id']==record['mlflow']['run_id'] and record['mlflow']['source']=='models:/'+metadata['model_id']
 registered=yaml.safe_load((artifact/'registered_model_meta').read_text())
 assert registered['model_name']==record['mlflow']['model_name'] and str(registered['model_version'])==record['mlflow']['model_version']
 import mlflow.sklearn
 assert importlib.util.find_spec('ats_mlops') is None
 model=mlflow.sklearn.load_model(str(artifact))
 from ats_mlops.model.scoring import pair_frame,positive_scores
 frame=pair_frame(request['rows']);transformer=model.named_steps['pair_features'];features=transformer.transform(frame)
 names=list(transformer.get_feature_names_out());assert names==record['features']['names']
 scores=positive_scores(model.predict_proba(frame),model.classes_,expected_rows=len(frame))
 assert features.shape==(len(frame),len(names))
 rows=[]
 for values,score in zip(features,scores,strict=True):
  row=dict(zip(names,map(float,values),strict=True));row['score']=float(score)
  assert all(math.isfinite(v) for v in row.values());rows.append(row)
 for n,module in list(sys.modules.items()):
  if n=='ats_mlops' or n.startswith('ats_mlops.'):assert pathlib.Path(module.__file__).resolve().is_relative_to(artifact/'code')
 state={'version':transformer.feature_version_,'vocabulary':sorted((str(k),int(v)) for k,v in transformer.vectorizer_.vocabulary_.items()),'idf':transformer.vectorizer_.idf_.tolist(),'parameters':record['params']['features']}
 print(json.dumps({'outcome':'passed','rows':rows,'receipt':{'isolated':True,'bytecode_disabled':True,'credentials':False,'network':False,'training_checkout_mounted':False,'model_sha256':record['model']['sha256'],'feature_state_sha256':sha(canonical(state))}},allow_nan=False))
except Exception as error:
 print(json.dumps({'outcome':'failed','reason':'isolated_feature_worker_failed','error_type':type(error).__name__}));raise SystemExit(1)
'''


def artifact_features(artifact, record, rows):
    """Artifact-only fresh container, never a subprocess inheriting source mounts."""
    image = record["environment"]["image"]["id"]
    require(re.fullmatch(r"sha256:[0-9a-f]{64}", image), "invalid_feature_image")
    inspected = subprocess.run(["docker", "image", "inspect", "--format", "{{.Id}}", image], check=True, capture_output=True, text=True)
    require(inspected.stdout.strip() == image, "feature_image_mismatch")
    with tempfile.TemporaryDirectory(prefix="ats-reference-features-") as directory:
        request = {"record": record, "rows": [{key: row[key] for key in ("vacancy_text", "candidate_text")} for row in rows]}
        Path(directory, "request.json").write_bytes(canonical(request))
        name = "ats-reference-" + uuid.uuid4().hex[:12]
        command = ["docker", "run", "--name", name, "--rm", "--pull=never", "--network", "none", "--read-only", "--cpus", "1", "--memory", "1536m",
                   "--user", f"{os.getuid()}:{os.getgid()}", "--workdir", "/tmp", "--tmpfs", "/tmp:rw,size=128m,mode=1777",
                   "--mount", f"type=bind,source={Path(artifact).resolve()},target=/artifact,readonly",
                   "--mount", f"type=bind,source={directory},target=/input,readonly", "-e", "HOME=/tmp", "-e", "USER=ats-reference",
                   "-e", "PYTHONPATH=", "-e", "MLFLOW_TRACKING_URI=", "-e", "AWS_ACCESS_KEY_ID=", "-e", "AWS_SECRET_ACCESS_KEY=", "-e", "AWS_SESSION_TOKEN=",
                   "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
                   "--entrypoint", "/opt/ml-venv/bin/python", image, "-I", "-B", "-c", FEATURE_BOOTSTRAP]
        try:
            completed = subprocess.run(command, check=False, capture_output=True, text=True, timeout=120)
        finally:
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        result = json.loads(completed.stdout.strip().splitlines()[-1])
        require(completed.returncode == 0 and result["outcome"] == "passed", "feature_worker_failed")
        require(result["receipt"]["model_sha256"] == record["model"]["sha256"] and len(result["rows"]) == len(rows), "feature_worker_binding_mismatch")
        result["rows"] = numeric_rows(result["rows"])
        result["receipt"].update(image_id=image, bootstrap_sha256=sha(FEATURE_BOOTSTRAP.encode()))
        return result


def build_reference(request, output):
    require(set(request) == {"bundle", "bundle_sha256", "package", "release_check", "release_manifest_sha256", "scope"}
            and request["scope"] == "engineering_fixture", "invalid_reference_request")
    # All imports below are stdlib-only validators. No model is loaded into this process.
    from ats_mlops.pipeline import verify_bundle
    from ats_mlops.data.dataset import load_manifest, load_partition
    from ats_mlops.data.reservation import manifest_policy, validate_partitions
    from ats_mlops.data.validate import _valid_receipt
    from ats_mlops.release.controller import load_package
    from service.integrity import release_binding

    bundle = Path(request["bundle"])
    descriptor = verify_bundle(bundle, request["bundle_sha256"])
    manifest_path = bundle / "data_manifest.json"
    manifest = load_manifest(manifest_path)
    require(manifest["schema_version"] == 2, "anchored_schema2_reference_required")
    policy = manifest_policy(manifest_path, manifest)
    receipt_path = bundle / "admission/data-admission.json"
    require(_valid_receipt(read_json(receipt_path), sha(manifest_path.read_bytes())), "reference_admission_mismatch")
    partitions = {name: load_partition(manifest_path, name) for name in ("train", "validation")}
    validate_partitions(partitions, manifest["duplicate_detection"]["threshold"], policy)
    canonical_hashes = CANONICAL_PARTITIONS
    require(all(manifest["artifacts"][name]["sha256"] == digest for name, digest in canonical_hashes.items()), "canonical_reference_required")
    package = Path(request["package"])
    released, release_sha, check_sha = load_package(package, Path(request["release_check"]))
    _, _, record = release_binding(package)
    require(release_sha == request["release_manifest_sha256"], "release_manifest_mismatch")
    rows = partitions["train"] + partitions["validation"]
    derived = artifact_features(package / released["model_relative_path"], record, rows)
    size = len(partitions["train"])
    matrices = {}
    with publication(output) as root:
        for role, values in (("train", derived["rows"][:size]), ("validation", derived["rows"][size:])):
            content = b"".join(canonical(row) for row in values)
            (root / (role + ".jsonl")).write_bytes(content)
            groups = [sha(row["vacancy_id"].encode()) for row in partitions[role]]
            write_document(root / (role + "-groups.json"), groups)
            matrices[role] = {"sha256": sha(content), "pairs": len(values), "groups": len(set(groups)), "groups_sha256": sha(canonical(groups))}
        model = {"model_name": record["mlflow"]["model_name"], "model_version": record["mlflow"]["model_version"],
                 "release_id": released["release_id"], "release_manifest_sha256": release_sha, "release_check_sha256": check_sha,
                 "model_sha256": record["model"]["sha256"], "feature_state_sha256": derived["receipt"]["feature_state_sha256"],
                 "run_manifest_sha256": sha((package / "run_manifest.json").read_bytes()),
                 "feature_schema_sha256": sha(canonical(record["features"])),
                 "lineage_status": "guarded_schema2" if record["data_manifest_sha256"] == sha(manifest_path.read_bytes()) else "legacy_engineering_only"}
        result = {"schema_version": 2, "evidence_scope": "demo_only", "columns": list(NUMERIC), "model": model, "matrices": matrices,
                  "data": {"manifest_sha256": sha(manifest_path.read_bytes()), "admission_sha256": sha(receipt_path.read_bytes()),
                           "bundle_sha256": descriptor["sha256"], "reservation_policy_sha256": manifest["reservation"]["policy"]["sha256"],
                           "partition_sha256": canonical_hashes, "schema_version": 2, "read_partitions": ["train", "validation"]},
                  "feature_worker": derived["receipt"], "test_read": False,
                  "limitations": ["Reference input is canonical train/validation only; no CF/T9 training rows.",
                                  "Private checked package is not an ACTIVE release; later reference refresh must follow approved ACTIVE catalog.",
                                  "Legacy A4 coordinates, if used, remain engineering-only and cannot establish E5 independence."]}
        write_document(root / "reference.json", result)
    return result


def report_window(request, output):
    require(set(request) == {"reference", "reference_sha256", "export", "feedback", "calibration", "calibration_sha256", "config", "start", "end", "cutoff"}, "invalid_window_request")
    start, end, cutoff = (utc(request[key]) for key in ("start", "end", "cutoff"))
    require(start < end <= cutoff, "invalid_window_interval")
    request = dict(request, start=start.isoformat(), end=end.isoformat(), cutoff=cutoff.isoformat())
    config, config_sha = configuration(request["config"], with_sha=True)
    card, reference = load_reference(request["reference"], request["reference_sha256"])
    calibration_root = Path(request["calibration"])
    calibration_bytes = checked_bytes(calibration_root, "calibration.json")
    calibration = decode_json(calibration_bytes)
    require(sha(calibration_bytes) == request["calibration_sha256"] and calibration["outcome"] == "passed"
            and calibration["reference_sha256"] == request["reference_sha256"] and calibration["config_sha256"] == config_sha
            and calibration["columns"] == list(NUMERIC) and calibration["runtime"] == runtime()
            and calibration["method"] == config["method"] and calibration["threshold"] in config["threshold_grid"]
            and calibration["drift_share"] == config["drift_share"], "stale_calibration")
    for name, digest in calibration["files"].items():
        require(sha(checked_bytes(calibration_root, name)) == digest, "calibration_artifact_mismatch")
    events, event_manifest = load_export(request["export"])
    require(utc(event_manifest["cutoff"]) >= end, "export_before_window_end")
    records, receipt = load_feedback(request["feedback"])
    require(receipt["label_sources"] == sorted(config["label_sources"])
            and receipt["events_sha256"] == event_manifest["events_sha256"], "feedback_policy_mismatch")
    selected = [e for e in events if start <= utc(e["event_time"]) < end
                and (e["model_version"], e["release_id"]) == (card["model"]["model_version"], card["model"]["release_id"])]
    counts = {"requests": len(selected), "pairs": sum(len(e["candidates"]) for e in selected),
              "vacancies": len({e["vacancy_id"] for e in selected}), "legacy_unverified_requests": sum(not verified(e) for e in selected),
              "excluded_cohort_requests": sum(start <= utc(e["event_time"]) < end for e in events) - len(selected)}
    quality = assess(events, records, request["cutoff"], config["label_sources"], selected_events=selected)
    window_id = sha(canonical({"reference_sha256": request["reference_sha256"], "start": request["start"], "end": request["end"]}))
    outcome = "data_quality_failed" if counts["legacy_unverified_requests"] else "insufficient_data" if counts["pairs"] < config["minimum_pairs"] or counts["vacancies"] < config["minimum_vacancies"] else "passed"
    drift = {"status": "undefined", "detected": None}
    with publication(output) as root:
        if outcome == "passed":
            current = [dict(item["features"], score=item["score"]) for event in selected for item in event["candidates"]]
            try:
                drift = render(reference["train"], current, config, calibration["threshold"], root, timestamp=request["end"])
            except ValueError as error:
                if str(error) not in ("constant_reference_changed", "degenerate_constant_reference"):
                    raise
                outcome = "data_quality_failed"
                drift = {"status": "undefined", "detected": None, "reason": str(error)}
        write_document(root / "quality.json", quality)
        files = {path.name: sha(path.read_bytes()) for path in root.iterdir() if path.is_file()}
        result = {"schema_version": 1, "window_id": window_id, "outcome": outcome, "evidence_scope": "demo_only",
                  "reference_sha256": request["reference_sha256"], "model": card["model"],
                  "interval": {key: request[key] for key in ("start", "end", "cutoff")}, "counts": counts,
                  "minimum": {"pairs": config["minimum_pairs"], "vacancies": config["minimum_vacancies"]},
                  "drift": drift, "quality": quality, "config_sha256": config_sha,
                  "calibration_sha256": request["calibration_sha256"], "events_sha256": event_manifest["events_sha256"],
                  "feedback": {"snapshot_sha256": receipt["feedback_sha256"], "receipt_sha256": sha(canonical(receipt)), "ledger_id": receipt["ledger_id"]},
                  "files": files, "runtime": runtime(), "test_read": False,
                  "policy_actions": "none; T17 owns confirmation/cooldown/outbox/training/release"}
        write_document(root / "window.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(prog="ats-mlops monitoring")
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("calibrate", "report", "reference"):
        child = commands.add_parser(command)
        child.add_argument("--request", type=Path, required=True)
        child.add_argument("--output", type=Path, required=True)
    export = commands.add_parser("export")
    export.add_argument("--database", type=Path, required=True)
    export.add_argument("--cutoff", required=True)
    export.add_argument("--output", type=Path, required=True)
    labels = commands.add_parser("feedback")
    for key in ("export", "input", "ledger", "config", "output"):
        labels.add_argument("--" + key, type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "export":
            result = export_events(args.database, args.output, args.cutoff)
        elif args.command == "feedback":
            incoming = [decode_json(line) for line in args.input.read_bytes().splitlines()]
            result = admit_feedback(args.export, incoming, args.output, configuration(args.config)["label_sources"], ledger=args.ledger)
        else:
            request = read_json(args.request)
            if args.command == "calibrate":
                require(set(request) == {"reference", "reference_sha256", "config"}, "invalid_calibration_request")
                result = calibrate(request["reference"], request["reference_sha256"], request["config"], args.output)
            elif args.command == "reference":
                result = build_reference(request, args.output)
            else:
                result = report_window(request, args.output)
        print(json.dumps({"outcome": result.get("outcome", "passed"), "command": args.command}))
        return 0 if result.get("outcome", "passed") in ("passed", "insufficient_data") else 1
    except Exception as error:
        reason = str(error)
        safe = reason if reason and all(c.islower() or c == "_" for c in reason) else "monitoring_failed"
        print(json.dumps({"outcome": "failed", "reason": safe, "error_type": type(error).__name__}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
