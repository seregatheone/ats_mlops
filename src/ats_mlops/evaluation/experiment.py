"""Immutable experiment receipts and numeric-only audit/rebuild.

Effects are explicit injected adapters over the existing audited pipeline. This
module never imports a model loader, opens a dataset partition, or manufactures
human measurements. The preparation CLI deliberately exposes no effect runner.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import csv
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import random
import re
import sqlite3
import statistics
import tempfile
from typing import Callable

from .ranking import evaluate_ranking

HEX = re.compile(r"[0-9a-f]{64}\Z")
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
ROOT_POLICY_SHA256 = "4ca6c42155d3e34e1436596381838bf31cc36339986c12b91278d760df5d265b"
SERIES = ("E1", "E2", "E3", "E4", "E5")
FAULTS = ("corrupt_data", "minio_unavailable", "incomplete_artifact", "bad_candidate", "unready_container", "switch_alias_crash")
BINDINGS = {"protocol_sha256", "data_manifest_sha256", "admission_sha256", "root_policy_sha256",
            "protected_test_documents_sha256", "test_partition_sha256", "split_sha256", "features_sha256",
            "params_sha256", "environment_sha256", "baseline_artifact_sha256", "model_artifact_sha256",
            "model_lineage_sha256", "calibration_sha256"}
EXECUTION = dict(search_mode="frozen", smoke=False, C=1.0, max_features=2000,
                 model_seed=20261026, data_seed=20261027, threads=1)
POLICIES = {
    "E1": dict(replicas=3, atol=1e-6, rtol=1e-5),
    "E2": dict(pairs=5, order="alternating_manual_auto", actor="human", hpo=False),
    "E3": dict(faults=list(FAULTS), repetitions=3),
    "E4": dict(seed=20261029, calibration_seed=1707, minimum_pairs=200, minimum_vacancies=10,
               consecutive_windows=2, cooldown_seconds=86400,
               scenarios=["control", "shifted_no_labels", "shifted_labeled", "replay", "cooldown", "false_signal", "insufficient"]),
    "E5": dict(bootstrap_replicates=2000, bootstrap_seed=20261030, bootstrap_min_groups=10,
               k=10, latency="one_pass_per_group", warmup="train_validation_only"),
}


class ExperimentError(ValueError):
    """Sanitized fail-closed experiment boundary."""


def require(condition, reason):
    if not condition:
        raise ExperimentError(reason)


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def sha(content):
    return hashlib.sha256(content).hexdigest()


def decode(content):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "duplicate_json_key")
            value[key] = item
        return value
    return json.loads(content, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ExperimentError("nonfinite_json")))


def read(path):
    path = Path(path)
    require(not path.is_symlink() and path.is_file(), "invalid_receipt_path")
    return decode(path.read_bytes())


def number(value, *, minimum=0):
    return type(value) in (int, float) and math.isfinite(value) and value >= minimum


def digest(value):
    return isinstance(value, str) and HEX.fullmatch(value) is not None


def identifier(value):
    return isinstance(value, str) and IDENTITY.fullmatch(value) is not None


def _sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def immutable(path, value):
    """Publish a fully fsynced JSON file exactly once, including across processes."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    require(not path.exists() and not path.is_symlink(), "immutable_receipt_exists")
    descriptor, temporary = tempfile.mkstemp(prefix=".receipt-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(canonical(value));stream.flush();os.fsync(stream.fileno())
        os.link(temporary, path)  # atomic O_EXCL publication, never replace
        _sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def locked(directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".lock").open("ab") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def validate_spec(value, *, template=False):
    """Authoritative schema; draft nulls are allowed only for preparation."""
    require(isinstance(value, dict) and set(value) == {"schema_version", "study_id", "evidence_scope",
        "source_git_sha", "qualified_model_source_git_sha", "bindings", "test_inventory", "registry_before",
        "adapter_bindings", "execution", *SERIES}, "invalid_spec_fields")
    require(type(value["schema_version"]) is int and value["schema_version"] == 1
            and identifier(value["study_id"]) and value["evidence_scope"] in ("demo_only", "independently_labeled"), "invalid_spec")
    for key in ("source_git_sha", "qualified_model_source_git_sha"):
        require(template and value[key] is None or isinstance(value[key], str) and re.fullmatch(r"[0-9a-f]{40}", value[key]), "unqualified_source")
    require(isinstance(value["bindings"], dict) and set(value["bindings"]) == BINDINGS, "invalid_binding_fields")
    require(all(digest(v) or template and v is None for v in value["bindings"].values()), "unqualified_bindings")
    require(value["bindings"]["root_policy_sha256"] == ROOT_POLICY_SHA256
            or template and value["bindings"]["root_policy_sha256"] is None, "unknown_root_policy")
    require(canonical(value["execution"]) == canonical(EXECUTION), "unfrozen_execution")
    for series in SERIES:
        require(canonical(value[series]) == canonical(POLICIES[series]), "unfrozen_experiment_policy")
    inventory = value["test_inventory"]
    require(isinstance(inventory, dict) and set(inventory) == {"pairs", "groups", "group_ids_sha256", "pair_ids_sha256"}, "invalid_test_inventory")
    for key in ("pairs", "groups"):
        require(template and inventory[key] is None or type(inventory[key]) is int and inventory[key] > 0, "unqualified_test_inventory")
    require(digest(inventory["group_ids_sha256"]) or template and inventory["group_ids_sha256"] is None, "unqualified_test_groups")
    require(digest(inventory["pair_ids_sha256"]) or template and inventory["pair_ids_sha256"] is None, "unqualified_test_pairs")
    registry = value["registry_before"]
    require(isinstance(registry, dict) and set(registry) == {"runs", "versions"}, "invalid_registry_before")
    for key in registry:
        require(isinstance(registry[key], list) and len(set(registry[key])) == len(registry[key])
                and all(identifier(x) for x in registry[key]), "invalid_registry_before")
    require(isinstance(value["adapter_bindings"], dict) and set(value["adapter_bindings"]) == set(SERIES), "invalid_adapters")
    for adapter in value["adapter_bindings"].values():
        require(template and adapter is None or isinstance(adapter, dict) and set(adapter) == {"entrypoint", "source_sha256"}
                and isinstance(adapter["entrypoint"], str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*", adapter["entrypoint"])
                and digest(adapter["source_sha256"]), "unwired_adapter")
    return value


def holdout_key(spec):
    """Stable semantic holdout identity; candidate/namespace/protocol are excluded."""
    return sha(canonical({"schema_version": 1, **{key: spec["bindings"][key] for key in
                         ("root_policy_sha256", "protected_test_documents_sha256")}}))


def initialize(spec_path, output, freeze_path, expected_freeze_sha256, *, verifier: Callable):
    """T18's native closure validator is injected; no duplicate readiness audit."""
    spec = validate_spec(read(spec_path))
    freeze_path = Path(freeze_path)
    require(sha(freeze_path.read_bytes()) == expected_freeze_sha256, "freeze_hash_mismatch")
    checked = verifier(freeze_path, expected_freeze_sha256)
    spec_sha = sha(canonical(spec))
    require(checked["outcome"] == "passed" and checked["spec_sha256"] == spec_sha
            and checked["source_git_sha"] == spec["source_git_sha"] and checked["source_clean"] is True
            and checked["inputs"] == spec["bindings"], "freeze_spec_mismatch")
    output = Path(output);output.mkdir(parents=True, exist_ok=False)
    immutable(output / "spec.json", spec)
    immutable(output / "freeze.json", read(freeze_path))
    immutable(output / "study.json", {"schema_version": 1, "spec_sha256": spec_sha,
        "freeze_sha256": sha(canonical(read(freeze_path))), "validated_freeze_input_sha256": expected_freeze_sha256,
        "holdout_lineage_sha256": holdout_key(spec)})
    return output


def study(directory):
    directory = Path(directory)
    spec, receipt = validate_spec(read(directory / "spec.json")), read(directory / "study.json")
    require(sha(canonical(spec)) == receipt["spec_sha256"]
            and sha((directory / "freeze.json").read_bytes()) == receipt["freeze_sha256"]
            and receipt["holdout_lineage_sha256"] == holdout_key(spec), "frozen_study_changed")
    return spec, receipt


def slots(spec, series):
    if series == "E1":return [str(i) for i in range(1, 4)]
    if series == "E2":return [f"{i}-{mode}" for i in range(1, 6) for mode in ("manual", "auto")]
    if series == "E3":return [f"{fault}-{i}" for fault in FAULTS for i in range(1, 4)]
    if series == "E4":return spec["E4"]["scenarios"]
    if series == "E5":return ["1"]
    raise ExperimentError("invalid_series")


def _attempt(directory, series, slot, operation, *, human=False):
    spec, receipt = study(directory)
    require(slot in slots(spec, series) and series != "E5", "invalid_attempt_slot")
    require(series != "E2" or human, "human_measurement_required")
    with locked(directory):
        root = Path(directory) / "attempts" / series / slot
        root.mkdir(parents=True, exist_ok=False)
        intent = {"schema_version": 1, "series": series, "slot": slot, "spec_sha256": receipt["spec_sha256"],
                  "adapter": spec["adapter_bindings"][series], "started_at": datetime.now(timezone.utc).isoformat(),
                  "human_attested_import": human}
        immutable(root / "intent.json", intent)
    try:
        # Adapter owns real audited commands and retained logs. No default/fake runner.
        result = operation(spec, root)
        require(isinstance(result, dict) and set(result) == {"outcome", "observations", "evidence_sha256"}
                and result["outcome"] in ("passed", "expected_rejection", "failed", "recovery_required")
                and digest(result["evidence_sha256"]) and isinstance(result["observations"], dict), "invalid_adapter_result")
        immutable(root / "raw.json", result)
        outcome = {"outcome": result["outcome"], "intent_sha256": sha(canonical(intent)), "raw_sha256": sha(canonical(result))}
    except BaseException as error:
        immutable(root / "outcome.json", {"outcome": "recovery_required", "intent_sha256": sha(canonical(intent)),
                                           "error_type": type(error).__name__})
        raise
    immutable(root / "outcome.json", outcome)
    return result


def run_attempt(directory, series, slot, adapter: Callable):
    return _attempt(directory, series, slot, adapter)


def ingest_human_measurement(directory, pair, mode, artifact, *, attest: Callable):
    """Import externally measured rows; the harness never times a fake human."""
    require(type(pair) is int and mode in ("manual", "auto"), "invalid_human_pair")
    value = read(artifact)
    require(attest(value) is True, "human_attestation_required")
    return _attempt(directory, "E2", f"{pair}-{mode}", lambda _spec, _root: value, human=True)


def _score_map(rows):
    require(isinstance(rows, list) and rows, "empty_numeric_results")
    result = {}
    for row in rows:
        require(set(row) == {"group_id", "candidate_id", "score"} and identifier(row["group_id"])
                and identifier(row["candidate_id"]) and number(row["score"]) and row["score"] <= 1, "invalid_numeric_score")
        key = row["group_id"], row["candidate_id"]
        require(key not in result, "duplicate_numeric_score");result[key] = row["score"]
    return result


def _ranks(scores):
    return {group: [candidate for g, candidate in sorted(scores, key=lambda key: (-scores[key], key[1])) if g == group]
            for group in sorted({key[0] for key in scores})}


def _audit_e1(spec, rows):
    baseline = rows[0];reference = _score_map(baseline["scores"])
    invariant = ("data_sha256", "split_sha256", "features_sha256", "vocabulary_sha256", "coordinates_sha256")
    for row in rows:
        require(row["source_git_sha"] == spec["source_git_sha"] and row["forced"] is True
                and row["run_cache"] is False and row["cache_hit"] is False and type(row["final_fit_events"]) is int
                and row["final_fit_events"] == 1, "e1_requires_actual_fresh_fit")
        require(all(digest(row[key]) and row[key] == baseline[key] for key in invariant)
                and row["coordinates_sha256"] == sha(canonical(spec["bindings"])), "e1_coordinate_mismatch")
        require(all(row[key] == spec["bindings"][binding] for key,binding in
                    (("data_sha256","data_manifest_sha256"),("split_sha256","split_sha256"),("features_sha256","features_sha256"))), "e1_input_binding_mismatch")
        scores = _score_map(row["scores"])
        require(scores.keys() == reference.keys() and _ranks(scores) == _ranks(reference), "e1_ranks_differ")
        require(all(abs(scores[key] - value) <= spec["E1"]["atol"] + spec["E1"]["rtol"] * abs(value)
                    for key, value in reference.items()), "e1_scores_differ")
        require(isinstance(row["metrics"], dict) and row["metrics"] and row["metrics"].keys() == baseline["metrics"].keys()
                and all(number(v) and abs(v - baseline["metrics"][k]) <= spec["E1"]["atol"] + spec["E1"]["rtol"] * abs(baseline["metrics"][k])
                        for k, v in row["metrics"].items()), "e1_metrics_differ")
    for key in ("workspace_id", "training_run_id", "model_version"):
        require(all(identifier(row[key]) for row in rows) and len({row[key] for row in rows}) == 3, "e1_not_three_new_replicas")
    require(not {r["training_run_id"] for r in rows} & set(spec["registry_before"]["runs"])
            and not {r["model_version"] for r in rows} & set(spec["registry_before"]["versions"]), "e1_preexisting_run_or_version")
    return {"replicas": 3, "new_runs": 3, "new_versions": 3, "binary_model_equality_required": False}


def _audit_e2(spec, records):
    values = {slot: raw for slot, raw in records.items()};wall,active = {},{}
    require(len({row["measurement_id"] for row in records.values()})==10
            and all(identifier(row["measurement_id"]) for row in records.values()), "e2_reused_measurement")
    for pair in range(1, 6):
        manual, auto = values[f"{pair}-manual"], values[f"{pair}-auto"]
        require(manual["cache_state_sha256"] == auto["cache_state_sha256"] and digest(manual["cache_state_sha256"]), "e2_unpaired_cache")
        for mode,row in (("manual",manual),("auto",auto)):
            require(type(row["pair"]) is int and row["pair"]==pair and row["mode"]==mode,"e2_pair_identity_mismatch")
            require(row["actor_kind"] == "human" and identifier(row["actor_id"]) and digest(row["attestation_sha256"])
                    and digest(row["journal_sha256"]) and row["hpo_enabled"] is False
                    and row["coordinates_sha256"] == sha(canonical(spec["bindings"])), "e2_not_genuine_bound_measurement")
            expected_first = "manual" if pair % 2 else "auto"
            require(type(row["order"]) is int and row["order"] == (0 if mode == expected_first else 1), "e2_order_mismatch")
            require(all(number(row[k]) for k in ("wall_seconds","active_human_seconds","setup_seconds","warmup_seconds"))
                    and row["active_human_seconds"] <= row["wall_seconds"]
                    and all(type(row[k]) is int and row[k] >= 0 for k in ("actions","errors")), "invalid_human_measurement")
            wall.setdefault(mode,[]).append(row["wall_seconds"]);active.setdefault(mode,[]).append(row["active_human_seconds"])
    return {"pairs":5,"wall_seconds":{k:{"median":statistics.median(v),"min":min(v),"max":max(v)} for k,v in wall.items()},
            "active_human_seconds":{k:statistics.median(v) for k,v in active.items()}}


def outage_bounds(samples):
    require(isinstance(samples,list) and len(samples)>=2 and all(set(x)=={"t","ok"} and number(x["t"]) and type(x["ok"]) is bool for x in samples)
            and all(a["t"]<b["t"] for a,b in zip(samples,samples[1:])), "invalid_http_samples")
    lower=upper=0.;index=0
    while index<len(samples):
        if samples[index]["ok"]:index+=1;continue
        first=index
        while index+1<len(samples) and not samples[index+1]["ok"]:index+=1
        lower+=samples[index]["t"]-samples[first]["t"]
        if first==0 or index==len(samples)-1:return {"lower_seconds":lower,"upper_seconds":None}
        upper+=samples[index+1]["t"]-samples[first-1]["t"];index+=1
    if all(x["ok"] for x in samples):upper=max(b["t"]-a["t"] for a,b in zip(samples,samples[1:]))
    return {"lower_seconds":lower,"upper_seconds":upper}


def _audit_e3(records):
    bounds=[]
    for slot,row in records.items():
        fault,repeat=slot.rsplit("-",1)
        require(row["fault"]==fault and type(row["repeat"]) is int and row["repeat"]==int(repeat),"e3_wrong_fault_repeat")
        require(row["own_resources_only"] is True and digest(row["isolation_sha256"]) and digest(row["fault_receipt_sha256"])
                and row["final_consistent"] is True and row["terminal"] in ("expected_rejection","restored"), "e3_incomplete_recovery")
        require(all(number(row[k]) for k in ("injected_monotonic","detected_monotonic","restored_monotonic"))
                and row["injected_monotonic"] <= row["detected_monotonic"] <= row["restored_monotonic"], "invalid_fault_timing")
        bound=outage_bounds(row["http_samples"])
        require(bound["upper_seconds"] is not None and row["http_samples"][0]["t"] <= row["injected_monotonic"]
                and row["http_samples"][-1]["t"] >= row["restored_monotonic"], "e3_unbounded_outage")
        bounds.append(bound)
    return {"trials":18,"outage_bounds":bounds}


def _audit_e4(spec, records):
    for scenario,row in records.items():
        require(all(type(row[k]) is int and row[k]>=0 for k in ("new_requests","new_runs","new_versions")),"e4_invalid_effect_counts")
        require(row["seed"] == spec["E4"]["seed"] and type(row["seed"]) is int and digest(row["t17_receipt_sha256"])
                and row["active_catalog_verified"] is True and row["policy_checks"] and all(v is True for v in row["policy_checks"].values()), "e4_missing_actual_t17_evidence")
        if scenario in ("control","replay","cooldown","false_signal","insufficient"):
            require(row["new_requests"] == 0 and row["new_runs"] == 0 and row["new_versions"] == 0, "e4_duplicate_effect")
        if scenario == "shifted_no_labels":
            require(row["quality"] is None and row["terminal"] == "waiting_for_labels" and row["new_requests"] == 1
                    and row["new_runs"] == row["new_versions"] == 0, "e4_self_label_or_refit")
            windows=row["windows"]
            require(len(windows)==2 and windows[0]["id"]!=windows[1]["id"] and windows[0]["end"]==windows[1]["start"]
                    and all(type(w["pairs"]) is int and w["pairs"]>=200 and type(w["vacancies"]) is int
                            and w["vacancies"]>=10 and w["closed"] is True for w in windows), "e4_invalid_signal_windows")
        if scenario == "shifted_labeled":
            require(row["logical_request_id"] == records["shifted_no_labels"]["logical_request_id"]
                    and row["new_requests"] == 0 and row["new_runs"] == row["new_versions"] == 1
                    and digest(row["approved_labels_sha256"]) and row["reference_refreshed_after_active"] is True, "e4_unverified_resume")
    return {"scenarios":list(records),"seed":spec["E4"]["seed"],"policy_algorithm":"reused T17 receipts"}


def summarize(directory):
    spec, header = study(directory);result={}
    for series in SERIES:
        if series == "E5":continue
        expected=slots(spec,series);observations={};outcomes=[]
        parent=Path(directory)/"attempts"/series
        if parent.exists():require({p.name for p in parent.iterdir()}<=set(expected),"unexpected_attempt_slot")
        for slot in expected:
            root=parent/slot
            if not root.exists():continue
            intent=read(root/"intent.json")
            require(intent["spec_sha256"]==header["spec_sha256"] and intent["series"]==series and intent["slot"]==slot,"attempt_binding_mismatch")
            if not (root/"outcome.json").exists():outcomes.append("recovery_required");continue
            outcome=read(root/"outcome.json");require(outcome["intent_sha256"]==sha(canonical(intent)),"attempt_intent_changed")
            require(set(outcome)==({"outcome","intent_sha256","error_type"} if outcome["outcome"]=="recovery_required"
                                  and "raw_sha256" not in outcome else {"outcome","intent_sha256","raw_sha256"}),"invalid_outcome_record")
            outcomes.append(outcome["outcome"])
            if "raw_sha256" in outcome:
                raw=read(root/"raw.json");require(sha(canonical(raw))==outcome["raw_sha256"] and raw["outcome"]==outcome["outcome"],"attempt_raw_changed")
                observations[slot]=raw["observations"]
                if series=="E2":require(intent["human_attested_import"] is True,"e2_missing_human_attestation")
        success=("passed","expected_rejection") if series=="E3" else ("passed",)
        status="not_run" if not outcomes else "recovery_required" if "recovery_required" in outcomes else "failed" if any(x not in success for x in outcomes) else "incomplete" if len(outcomes)!=len(expected) else "passed"
        details={}
        if status=="passed":
            try:
                if series=="E1":details=_audit_e1(spec,list(observations.values()))
                elif series=="E2":details=_audit_e2(spec,observations)
                elif series=="E3":details=_audit_e3(observations)
                else:details=_audit_e4(spec,observations)
            except (ValueError,KeyError,TypeError):status="failed";details={"reason":"series_audit_failed"}
        result[series]={"status":status,"expected":len(expected),"attempted":len(outcomes),"outcomes":outcomes,"details":details}
    numeric=Path(directory)/"E5"/"numeric.json"
    result["E5"]={"status":"not_run" if not (Path(directory)/"E5"/"intent.json").exists() else "recovery_required"}
    if numeric.exists() and (numeric.parent/"completed.json").exists():
        try:
            raw=read(numeric);intent=read(numeric.parent/"intent.json");completed=read(numeric.parent/"completed.json")
            require(raw["binding"]==_e5_binding(spec,header)
                    and intent["holdout_lineage_sha256"]==holdout_key(spec)
                    and raw['snapshot']==read(numeric.parent/'snapshot.json')
                    and completed=={"outcome":"completed","holdout_lineage_sha256":holdout_key(spec),"binding_sha256":sha(canonical(raw["binding"])),"numeric_sha256":sha(canonical(raw)),
                        "ground_truth_snapshot_sha256":sha((numeric.parent/'snapshot.json').read_bytes())},"e5_completion_mismatch")
            rebuild_numeric(numeric);result["E5"]={"status":"passed","numeric_sha256":sha(numeric.read_bytes())}
        except (ValueError,KeyError,TypeError):result["E5"]={"status":"failed"}
    statuses=[value["status"] for value in result.values()]
    status="passed" if all(s=="passed" for s in statuses) else "recovery_required" if "recovery_required" in statuses else "failed" if "failed" in statuses else "not_run" if all(s=="not_run" for s in statuses) else "incomplete"
    return {"schema_version":1,"spec_sha256":header["spec_sha256"],"evidence_scope":spec["evidence_scope"],"series":result,"status":status}


class HoldoutGuard:
    """One durable native-volume ledger shared by ALL studies/candidates. No reset."""
    def __init__(self,path):
        self.path=Path(path);require(not self.path.is_symlink(),"invalid_guard_path");self.path.parent.mkdir(parents=True,exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS holdouts (lineage TEXT PRIMARY KEY,binding_sha256 TEXT NOT NULL,binding_json TEXT NOT NULL,intent_sha256 TEXT NOT NULL,result_root TEXT NOT NULL,status TEXT NOT NULL,numeric_sha256 TEXT,snapshot_sha256 TEXT)")
            require([row[1] for row in db.execute('PRAGMA table_info(holdouts)')] == ['lineage','binding_sha256','binding_json','intent_sha256','result_root','status','numeric_sha256','snapshot_sha256'], 'holdout_ledger_schema_mismatch')
        _sync_directory(self.path.parent)

    @contextmanager
    def connection(self):
        db=sqlite3.connect(self.path,timeout=10)
        try:
            db.execute("PRAGMA synchronous=FULL")
            with db:yield db
        finally:db.close()

    def consume(self,key,binding,intent,result_root):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            require(db.execute("SELECT lineage FROM holdouts WHERE lineage=?",(key,)).fetchone() is None,"holdout_already_consumed")
            db.execute("INSERT INTO holdouts VALUES (?,?,?,?,?,'consumed_intent',NULL,NULL)",(key,sha(canonical(binding)),canonical(binding).decode(),sha(canonical(intent)),str(Path(result_root).resolve())))

    def bind_snapshot(self,key,binding,snapshot_sha256):
        require(digest(snapshot_sha256),'invalid_ground_truth_snapshot_hash')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT binding_sha256,snapshot_sha256 FROM holdouts WHERE lineage=?',(key,)).fetchone()
            require(row is not None and row[0]==sha(canonical(binding)) and row[1] in (None,snapshot_sha256),'holdout_snapshot_conflict')
            if row[1] is None:db.execute("UPDATE holdouts SET snapshot_sha256=?,status='snapshot_committed' WHERE lineage=?",(snapshot_sha256,key))

    def bind_numeric(self,key,binding,numeric_sha256):
        require(digest(numeric_sha256),'invalid_retained_numeric_hash')
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT binding_sha256,snapshot_sha256,numeric_sha256 FROM holdouts WHERE lineage=?',(key,)).fetchone()
            require(row is not None and row[0]==sha(canonical(binding)) and digest(row[1]) and row[2] in (None,numeric_sha256),'holdout_numeric_conflict')
            if row[2] is None:db.execute("UPDATE holdouts SET numeric_sha256=?,status='numeric_retained' WHERE lineage=?",(numeric_sha256,key))

    def complete(self,key,binding):
        with self.connection() as db:
            db.execute("BEGIN IMMEDIATE")
            row=db.execute("SELECT binding_sha256,intent_sha256,result_root,numeric_sha256,snapshot_sha256 FROM holdouts WHERE lineage=?",(key,)).fetchone()
            require(row is not None and row[0]==sha(canonical(binding)) and digest(row[3]) and digest(row[4]),"holdout_binding_or_retained_result_missing")
            root=Path(row[2]);raw=read(root/"numeric.json");snapshot=read(root/'snapshot.json')
            require(sha((root/'numeric.json').read_bytes())==row[3] and sha((root/'snapshot.json').read_bytes())==row[4]
                    and raw['snapshot']==snapshot and raw["binding"]==binding,"holdout_retained_result_changed")
            expected={"binding_sha256":row[0],"intent_sha256":row[1],"numeric_sha256":row[3],"ground_truth_snapshot_sha256":row[4]}
            # Numeric-only crash recovery: no loader or scorer callback is used.
            if not (root/'commit.json').exists():immutable(root/'commit.json',expected)
            commit=read(root/'commit.json')
            require(commit==expected,"holdout_numeric_commit_mismatch")
            rebuild_numeric(root/"numeric.json")  # numeric-only recovery, never loader/scorer callbacks
            db.execute("UPDATE holdouts SET status='completed',numeric_sha256=? WHERE lineage=?",(commit["numeric_sha256"],key))
        completion={"outcome":"completed","holdout_lineage_sha256":key,"binding_sha256":sha(canonical(binding)),"numeric_sha256":sha(canonical(raw)),"ground_truth_snapshot_sha256":row[4]}
        if (root/"completed.json").exists():require(read(root/"completed.json")==completion,"holdout_completion_changed")
        else:immutable(root/"completed.json",completion)
        return raw


def _e5_binding(spec,header):
    return {"spec_sha256":header["spec_sha256"],"freeze_sha256":header["freeze_sha256"],"bindings":spec["bindings"],
            "test_inventory":spec["test_inventory"],"policy":spec["E5"],"evidence_scope":spec["evidence_scope"]}

def _truth(rows,group_key):
    require(isinstance(rows,list) and rows,'missing_original_numeric_snapshot')
    result=[];seen=set()
    for row in rows:
        group,candidate=row[group_key],row['candidate_id'];label=row['relevance']
        require(identifier(group) and identifier(candidate) and type(label) is int and label in (0,1),'invalid_ground_truth_row')
        require((group,candidate) not in seen,'duplicate_ground_truth_pair');seen.add((group,candidate))
        result.append({'group_id':group,'candidate_id':candidate,'relevance':label})
    return sorted(result,key=lambda row:(row['group_id'],row['candidate_id']))

def _verify_snapshot(snapshot,binding):
    require(isinstance(snapshot,dict) and set(snapshot)=={'schema_version','snapshot_sha256','pair_ids_sha256','ground_truth_sha256','rows'}
            and type(snapshot['schema_version']) is int and snapshot['schema_version']==1,'invalid_ground_truth_snapshot')
    rows=_truth(snapshot['rows'],'group_id');inventory=binding['test_inventory']
    groups=sorted({row['group_id'] for row in rows});pairs=sorted(row['group_id']+'/'+row['candidate_id'] for row in rows)
    require(rows==snapshot['rows'] and snapshot['snapshot_sha256']==binding['bindings']['test_partition_sha256']
            and snapshot['ground_truth_sha256']==sha(canonical(rows))
            and snapshot['pair_ids_sha256']==sha(canonical(pairs))==inventory['pair_ids_sha256']
            and len(rows)==inventory['pairs'] and len(groups)==inventory['groups']
            and sha(canonical(groups))==inventory['group_ids_sha256'],'ground_truth_inventory_mismatch')
    return rows


def evaluate_once(directory,guard_path,read_snapshot:Callable,score_both:Callable):
    spec,header=study(directory);root=Path(directory)/"E5";binding=_e5_binding(spec,header)
    intent={"schema_version":1,"holdout_lineage_sha256":holdout_key(spec),"binding_sha256":sha(canonical(binding)),
            "started_at":datetime.now(timezone.utc).isoformat()}
    guard=HoldoutGuard(guard_path)
    with locked(directory):
        root.mkdir(exist_ok=False)
        immutable(root/"intent.json",intent)
        # COMMIT/FULL precedes both callbacks, including the first test byte read.
        guard.consume(holdout_key(spec),binding,intent,root)
    try:
        snapshot=read_snapshot()
        require(snapshot["sha256"]==spec["bindings"]["test_partition_sha256"],"holdout_snapshot_mismatch")
        truth=_truth(snapshot['rows'],'vacancy_id')
        frozen_snapshot={'schema_version':1,'snapshot_sha256':snapshot['sha256'],
            'pair_ids_sha256':sha(canonical(sorted(row['group_id']+'/'+row['candidate_id'] for row in truth))),
            'ground_truth_sha256':sha(canonical(truth)),'rows':truth}
        _verify_snapshot(frozen_snapshot,binding)
        immutable(root/'snapshot.json',frozen_snapshot)
        guard.bind_snapshot(holdout_key(spec),binding,sha(canonical(frozen_snapshot)))
        snapshot=dict(snapshot,pair_ids_sha256=frozen_snapshot['pair_ids_sha256'],ground_truth_sha256=frozen_snapshot['ground_truth_sha256'])
        scored=score_both(snapshot)
        raw={"schema_version":1,"binding":binding,"snapshot":frozen_snapshot,"observations":scored}
        _numeric_results(raw)
        immutable(root/"numeric.json",raw)
        guard.bind_numeric(holdout_key(spec),binding,sha(canonical(raw)))
        immutable(root/"commit.json",{"binding_sha256":sha(canonical(binding)),"intent_sha256":sha(canonical(intent)),"numeric_sha256":sha(canonical(raw)),"ground_truth_snapshot_sha256":sha(canonical(frozen_snapshot))})
        return guard.complete(holdout_key(spec),binding)
    except BaseException as error:
        immutable(root/"failure.json",{"outcome":"consumed_unknown","error_type":type(error).__name__})
        raise


def _numeric_results(raw):
    require(set(raw)=={"schema_version","binding","snapshot","observations"} and type(raw["schema_version"]) is int and raw["schema_version"]==1,"invalid_numeric_record")
    observed=raw["observations"];binding=raw["binding"];policy=binding["policy"]
    truth=_verify_snapshot(raw['snapshot'],binding)
    require(canonical(policy)==canonical(POLICIES["E5"]),"unfrozen_bootstrap")
    require(set(observed)=={"rows","groups","latencies","snapshot_sha256","model_artifact_sha256","baseline_artifact_sha256","evaluated_groups_once","warmup"},"invalid_e5_observations")
    require(all(observed[k]==binding["bindings"][k] for k in ("model_artifact_sha256","baseline_artifact_sha256"))
            and observed["snapshot_sha256"]==binding["bindings"]["test_partition_sha256"] and observed["warmup"]==policy["warmup"],"e5_artifact_mismatch")
    groups=observed["groups"];require(isinstance(groups,list) and all(identifier(g) for g in groups) and groups==sorted(set(groups))
        and sha(canonical(groups))==binding["test_inventory"]["group_ids_sha256"] and len(groups)==binding["test_inventory"]["groups"],"e5_group_inventory_mismatch")
    require(type(observed["evaluated_groups_once"]) is int and observed["evaluated_groups_once"]==len(groups),"e5_repeated_scoring")
    data=[];scores={"baseline":{},"model":{}}
    for row in observed["rows"]:
        require(set(row)=={"group_id","candidate_id","relevance","baseline_score","model_score"} and row["group_id"] in groups
                and identifier(row["candidate_id"]) and type(row["relevance"]) is int and row["relevance"] in (0,1),"invalid_e5_numeric_row")
        key=row["group_id"],row["candidate_id"]
        require(key not in scores["baseline"],"duplicate_e5_pair")
        for name in scores:
            value=row[name+"_score"];require(number(value) and value<=1,"invalid_e5_score");scores[name][key]=value
        data.append({"vacancy_id":key[0],"candidate_id":key[1],"relevance":row["relevance"]})
    require(len(data)==binding["test_inventory"]["pairs"],"e5_pair_inventory_mismatch")
    require(canonical(_truth(observed['rows'],'group_id'))==canonical(truth),'e5_ids_or_ground_truth_substituted')
    latencies=observed["latencies"]
    require(isinstance(latencies,list) and len(latencies)==len(groups) and {r["group_id"] for r in latencies}==set(groups)
            and all(set(r)=={"group_id","baseline_seconds","model_seconds"} and all(number(r[k]) for k in ("baseline_seconds","model_seconds")) for r in latencies),"invalid_e5_latency")
    metrics={name:evaluate_ranking(data,values,group_ids=groups) for name,values in scores.items()}
    for value in metrics.values():value.pop("elapsed_seconds",None)
    return metrics


def _percentile(values,p):
    ordered=sorted(values);position=(len(ordered)-1)*p;lower=int(position);upper=math.ceil(position)
    return ordered[lower]+(ordered[upper]-ordered[lower])*(position-lower)


def rebuild_numeric(path):
    """Only persisted numeric observations; never model/dataset reads or rescoring."""
    path=Path(path);raw=read(path);commit=read(path.parent/"commit.json")
    require(commit["numeric_sha256"]==sha(canonical(raw)) and commit["binding_sha256"]==sha(canonical(raw["binding"])),"numeric_result_changed")
    metrics=_numeric_results(raw);policy=raw["binding"]["policy"]
    by_model={name:{r["vacancy_id"]:r for r in value["groups"] if r["status"]=="defined"} for name,value in metrics.items()}
    require(by_model["model"].keys()==by_model["baseline"].keys(),"unequal_eligible_groups")
    groups=sorted(by_model["model"]);intervals={}
    for metric in ("ndcg_at_10","recall_at_10"):
        differences=[by_model["model"][g][metric]-by_model["baseline"][g][metric] for g in groups]
        interval=None
        if len(groups)>=policy["bootstrap_min_groups"]:
            rng=random.Random(policy["bootstrap_seed"])
            samples=[statistics.mean(rng.choices(differences,k=len(differences))) for _ in range(policy["bootstrap_replicates"])]
            interval=[_percentile(samples,.025),_percentile(samples,.975)]
        intervals[metric]={"delta":statistics.mean(differences) if differences else None,"percentile_95":interval,
                           "reason":None if interval is not None else "insufficient_eligible_groups"}
    return {"schema_version":1,"evidence_scope":raw["binding"]["evidence_scope"],"numeric_sha256":sha(canonical(raw)),
            "metrics":metrics,"paired_group_bootstrap":intervals,"bootstrap_policy":policy,"eligible_groups":len(groups)}


def rebuild(path,output,*,figures=False):
    result=rebuild_numeric(path);output=Path(output);output.mkdir(parents=True,exist_ok=False)
    immutable(output/"summary.json",result)
    with (output/"groups.csv").open("x",newline="") as stream:
        writer=csv.writer(stream);writer.writerow(["model","group_id","eligible","ndcg_at_10","recall_at_10"])
        for name,value in result["metrics"].items():
            for row in value["groups"]:writer.writerow([name,row["vacancy_id"],row["status"]=="defined",row["ndcg_at_10"],row["recall_at_10"]])
    if figures:
        try:import matplotlib
        except ImportError as error:raise ExperimentError("matplotlib_unavailable_no_dependency_installed") from error
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        figure,axis=plt.subplots()
        values=[result["metrics"][name]["ndcg_at_10"] for name in ("baseline","model")]
        require(all(v is not None for v in values),"undefined_quality_figure")
        axis.bar(["baseline","model"],values);axis.set_ylabel("NDCG@10; "+result["evidence_scope"])
        figure.savefig(output/"ndcg.png");plt.close(figure)
    return result


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);commands=parser.add_subparsers(dest="command",required=True)
    validate=commands.add_parser("validate-spec");validate.add_argument("path",type=Path);validate.add_argument("--template",action="store_true")
    summary=commands.add_parser("summarize");summary.add_argument("study",type=Path)
    build=commands.add_parser("rebuild");build.add_argument("numeric",type=Path);build.add_argument("output",type=Path);build.add_argument("--figures",action="store_true")
    args=parser.parse_args(argv)
    try:
        if args.command=="validate-spec":validate_spec(read(args.path),template=args.template);result={"status":"draft" if args.template else "valid"}
        elif args.command=="summarize":result=summarize(args.study)
        else:result=rebuild(args.numeric,args.output,figures=args.figures)
        print(json.dumps(result,allow_nan=False));return 0
    except Exception as error:
        print(json.dumps({"status":"failed","reason":"experiment_failed","error_type":type(error).__name__}));return 1


if __name__=="__main__":raise SystemExit(main())
