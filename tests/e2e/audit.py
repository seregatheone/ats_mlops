"""Read-only semantic acceptance auditor. File presence and skips never mean PASS.

Preparation code only: scenario runners are existing audited integration scripts.
This module never starts services, fits a model, triggers a DAG or reads test rows.
"""
from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import re

SCENARIOS = {
    "S1": ("clean_startup", ("R-1", "R-3", "R-4", "R-5")),
    "S2": ("three_recomputations", ("R-1", "R-3")),
    "S3": ("bad_data_and_model", ("R-2", "R-5")),
    "S4": ("controlled_release", ("R-4", "R-5")),
    "S5": ("failure_and_recovery", ("R-4", "R-5")),
    "S6": ("monitoring_and_retraining", ("R-3", "R-5", "R-6")),
    "S7": ("genuine_ci", ("R-7",)),
    "S8": ("claim_traceability", ("R-8",)),
}


class EvidenceError(ValueError):
    """Missing, stale, inconsistent or non-executed evidence blocks readiness."""


def require(condition, reason):
    if not condition:
        raise EvidenceError(reason)


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(content):
    return hashlib.sha256(content).hexdigest()


def hash_value(value, length=64):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{" + str(length) + "}", value) is not None


def decode(content):
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, "duplicate_json_key")
            value[key] = item
        return value
    try:
        return json.loads(content, object_pairs_hook=pairs,
                          parse_constant=lambda _: (_ for _ in ()).throw(EvidenceError("nonfinite_json")))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise EvidenceError("invalid_json") from error


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def utc(value):
    require(isinstance(value, str), "invalid_timestamp")
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise EvidenceError("invalid_timestamp") from error
    require(stamp.tzinfo is not None and stamp.utcoffset().total_seconds() == 0, "utc_required")
    return stamp


class Artifacts:
    """Closed hash-bound inputs, rejecting symlinks, traversal and equivocation."""

    def __init__(self, root):
        self.root = Path(root)
        require(self.root.is_dir() and not self.root.is_symlink(), "invalid_evidence_root")
        self.seen = {}

    def content(self, reference):
        require(isinstance(reference, dict) and set(reference) == {"path", "sha256"}
                and hash_value(reference["sha256"]), "invalid_artifact_reference")
        name = reference["path"]
        require(isinstance(name, str) and name and "\\" not in name, "invalid_artifact_path")
        relative = Path(name)
        require(not relative.is_absolute() and relative.as_posix() == name
                and all(part not in ("..", ".") for part in relative.parts), "invalid_artifact_path")
        # Acceptance needs manifests and numeric outputs, never reserved row bytes.
        require(relative.name not in ("test.jsonl", "corpus.jsonl", "source.jsonl", "split.json"), "reserved_bytes_forbidden")
        path = self.root
        for part in relative.parts:
            path = path / part
            require(not path.is_symlink(), "artifact_symlink")
        require(path.is_file(), "missing_artifact")
        content = path.read_bytes()
        actual = digest(content)
        require(actual == reference["sha256"], "artifact_hash_mismatch")
        require(name not in self.seen or self.seen[name] == actual, "artifact_equivocation")
        self.seen[name] = actual
        return content

    def document(self, reference):
        value = decode(self.content(reference))
        require(isinstance(value, dict), "object_artifact_required")
        return value


def derive_holdout_lineage(policy):
    """Only hashed reservation metadata; no candidate/spec/partition byte identity.

    Test entries retain the pinned root policy's fingerprint-sorted order. Each
    entry is {fold:'test', tokens:[sorted SHA256 tokens]}; canonical JSON includes
    the trailing newline, just like the existing dataset contract.
    """
    from ats_mlops.data.reservation import ROOT_POLICY_SHA256, Registry

    Registry(policy)  # verifies the complete immutable parent chain and anchor
    root = policy
    while root["parent"] is not None:
        root = root["parent"]
    root_sha = digest(canonical(root))
    require(root_sha == ROOT_POLICY_SHA256, "unanchored_holdout_policy")
    protected = [entry for entry in root["documents"] if entry["fold"] == "test"]
    require(protected, "missing_protected_test_metadata")
    identity = {"schema_version": 1, "root_policy_sha256": root_sha,
                "protected_test_documents_sha256": digest(canonical(protected))}
    return {**identity, "holdout_lineage_sha256": digest(canonical(identity))}


def execution(receipt, source_sha, spec_sha):
    require(isinstance(receipt, dict) and receipt.get("schema_version") == 1
            and type(receipt["schema_version"]) is int, "invalid_receipt_schema")
    require(receipt.get("outcome") == "passed" and receipt.get("execution_kind") == "actual_acceptance"
            and receipt.get("source_git_sha") == source_sha and receipt.get("spec_sha256") == spec_sha
            and receipt.get("evidence_scope") == "demo_only", "nonexecuted_or_stale_receipt")
    require(utc(receipt["started_at"]) < utc(receipt["finished_at"]), "invalid_execution_interval")
    commands = receipt.get("commands")
    require(isinstance(commands, list) and commands, "missing_actual_commands")
    ids = set()
    for command in commands:
        require(set(command) == {"id", "argv", "exit_code", "expected_exit_code", "stdout", "stderr"}
                and isinstance(command["id"], str) and command["id"] not in ids, "invalid_command_record")
        ids.add(command["id"])
        require(isinstance(command["argv"], list) and command["argv"] and all(isinstance(value, str) and value for value in command["argv"])
                and type(command["exit_code"]) is int and type(command["expected_exit_code"]) is int
                and command["exit_code"] == command["expected_exit_code"], "unexpected_command_result")
    return receipt


def compare_recomputations(artifacts, replicas, *, source_sha):
    """Three actual fits; native DVC events and numeric outputs, not model bytes."""
    require(isinstance(replicas, list) and len(replicas) == 3, "three_acceptance_recomputations_required")
    normalized, run_ids, versions, workspaces = [], set(), set(), set()
    for replica in replicas:
        require(set(replica) == {"workspace_id", "repro", "pipeline", "run", "features", "scores", "metrics"}, "invalid_replica")
        require(isinstance(replica["workspace_id"], str) and replica["workspace_id"] not in workspaces, "fresh_workspace_required")
        workspaces.add(replica["workspace_id"])
        repro, pipeline, run, features, scores, metrics = [artifacts.document(replica[key]) for key in ("repro", "pipeline", "run", "features", "scores", "metrics")]
        require(repro["outcome"] == "new_training" and repro["command"] == ["dvc", "repro", "-f", "--no-run-cache"]
                and set(repro["events"]) == {"validate", "prepare", "train", "evaluate"}, "cached_or_unexecuted_recomputation")
        identity = run["mlflow"]
        require(hash_value(identity["run_id"], 32) and isinstance(identity["model_version"], str)
                and identity["model_version"].isdecimal() and str(int(identity["model_version"])) == identity["model_version"]
                and int(identity["model_version"]) > 0, "invalid_registered_run_identity")
        require(run["git_sha"] == source_sha and run["dirty_worktree"] is False and run["test_read"] is False
                and run["read_partitions"] == ["train", "validation"]
                and repro["events"]["train"]["mlflow"] == identity, "recomputation_lineage_mismatch")
        require(identity["run_id"] not in run_ids and identity["model_version"] not in versions, "distinct_actual_runs_required")
        run_ids.add(identity["run_id"]);versions.add(identity["model_version"])
        require(pipeline["outcome"] == "passed" and pipeline["test_read"] is False
                and pipeline["training"]["mlflow"] == identity
                and pipeline["training"]["run_manifest_sha256"] == replica["run"]["sha256"]
                and pipeline["training"]["model_sha256"] == run["model"]["sha256"], "pipeline_run_binding_mismatch")
        selection = pipeline.get("selection")
        require(isinstance(selection, dict) and selection.get("kind") == "frozen", "frozen_selection_required")
        selected = selection["receipt"]
        require(selection["receipt_sha256"] == digest(canonical(selected)) and selected["kind"] == "frozen"
                and selected["outcome"] == "selected" and type(selected["new_trials"]) is int and selected["new_trials"] == 0
                and selected["selected_config"] == run["params"]
                and selected["data_manifest_sha256"] == run["data_manifest_sha256"], "frozen_selection_binding_mismatch")
        rows = scores.get("rows")
        require(isinstance(rows, list) and rows, "missing_numeric_predictions")
        keys, values = [], []
        for row in rows:
            key = row["vacancy_id"], row["candidate_id"]
            require(all(isinstance(value, str) and value for value in key) and key not in keys
                    and finite(row["score"]) and 0 <= row["score"] <= 1, "invalid_numeric_predictions")
            keys.append(key);values.append(float(row["score"]))
        require(metrics.get("status") == "defined" and all(finite(metrics.get(key)) for key in ("ndcg_at_10", "recall_at_10")), "undefined_metrics")
        ranks = [[item["candidate_id"] for item in group["ranking"]] for group in metrics["groups"]]
        normalized.append({"prepared": pipeline["inputs"]["prepared_files"], "features": features,
            "params": run["params"], "data": run["data_sha256"], "environment": run["environment"],
            "keys": keys, "values": values, "ranks": ranks, "metrics": [metrics[key] for key in ("ndcg_at_10", "recall_at_10")]})
    first = normalized[0]
    for other in normalized[1:]:
        require(all(first[key] == other[key] for key in ("prepared", "features", "params", "data", "environment", "keys", "ranks")), "recomputation_structure_mismatch")
        require(all(math.isclose(a, b, abs_tol=1e-6, rel_tol=1e-5) for a, b in zip(first["values"], other["values"], strict=True))
                and all(math.isclose(a, b, abs_tol=1e-6, rel_tol=1e-5) for a, b in zip(first["metrics"], other["metrics"], strict=True)), "recomputation_numeric_mismatch")
    return {"replicas": 3, "run_ids": sorted(run_ids), "model_versions": sorted(versions), "atol": 1e-6, "rtol": 1e-5,
            "scope": "T18 engineering acceptance; not the E1 experiment series"}


def inspect_scenarios(directory, references, *, source_sha, spec_sha, validators):
    """No implicit default validator, skipped case or historical leaf substitution.

    Each adapter must validate its existing native report schemas. During early
    preparation absent/unfrozen adapters are explicitly blocked. Only root's
    later runtime window may produce the eight actual acceptance receipts.
    """
    require(hash_value(source_sha, 40) and hash_value(spec_sha), "invalid_qualification_identity")
    require(isinstance(references, dict) and set(references) <= set(SCENARIOS), "unknown_scenario")
    require(isinstance(validators, dict) and set(validators) <= set(SCENARIOS), "unknown_scenario_adapter")
    artifacts = Artifacts(directory)
    results = {}
    for identifier, (name, requirements) in SCENARIOS.items():
        if identifier not in references:
            results[identifier] = {"outcome": "blocked", "reason": "missing_actual_receipt"}
            continue
        try:
            receipt = execution(artifacts.document(references[identifier]), source_sha, spec_sha)
            require(receipt.get("scenario") == identifier and receipt.get("name") == name, "scenario_identity_mismatch")
            for command in receipt["commands"]:
                artifacts.content(command["stdout"])
                artifacts.content(command["stderr"])
            require(identifier in validators, "native_adapter_not_frozen")
            result = validators[identifier](artifacts, receipt)
            require(isinstance(result, dict) and result.get("outcome") == "passed"
                    and type(result.get("native_evidence_count")) is int and result["native_evidence_count"] > 0,
                    "native_evidence_not_verified")
            results[identifier] = {"outcome": "passed", "name": name, "requirements": list(requirements),
                                   "receipt_sha256": references[identifier]["sha256"], "validation": result}
        except (EvidenceError, KeyError, TypeError, OSError, ValueError):
            results[identifier] = {"outcome": "blocked", "reason": "invalid_or_unverified_actual_evidence"}
    matrix = {identifier: {"scenarios": [scenario for scenario, (_, requirements) in SCENARIOS.items() if identifier in requirements]}
              for identifier in ("R-" + str(index) for index in range(1, 9))}
    for row in matrix.values():
        row["outcome"] = "passed" if all(results[scenario]["outcome"] == "passed" for scenario in row["scenarios"]) else "blocked"
    return {"schema_version": 1, "outcome": "passed" if all(row["outcome"] == "passed" for row in results.values()) else "blocked",
            "source_git_sha": source_sha, "spec_sha256": spec_sha, "scenarios": results, "requirements": matrix,
            "artifact_sha256": dict(artifacts.seen), "scope": "read-only evidence audit; no runtime effects"}


def validate_freeze(path, expected_sha256):
    """Replay the complete native closure; never turn a missing result into PASS.

    A private evidence bundle may be relocated as a whole: every artifact path
    is relative to this receipt's parent. No test rows are part of that bundle.
    The application source is the BEFORE-evidence Git object, not a docs HEAD.
    """
    from ats_mlops.evaluation.experiment import validate_spec
    from .native import VALIDATORS
    import subprocess

    path=Path(path)
    require(hash_value(expected_sha256) and path.is_file() and not path.is_symlink(),"missing_freeze_receipt")
    content=path.read_bytes();require(digest(content)==expected_sha256,"freeze_hash_mismatch")
    value=decode(content)
    keys={"schema_version","outcome","spec_sha256","source_git_sha","source_clean","inputs","spec","environment",
          "source","holdout","scenarios","checks","scope","E2","independent_real_corpus","open_blocking_defects"}
    require(isinstance(value,dict) and set(value)==keys and type(value["schema_version"]) is int and value["schema_version"]==1
        and value["outcome"]=="passed" and value["source_clean"] is True
        and value["scope"]=="T18_readiness_before_E1_E5" and value["E2"]=="not_run"
        and value["independent_real_corpus"] is False and value["open_blocking_defects"]==[] ,"final_freeze_not_qualified")
    artifacts=Artifacts(path.parent);spec=validate_spec(artifacts.document(value["spec"]))
    require(value["spec_sha256"]==value["spec"]["sha256"]==digest(canonical(spec))
        and value["source_git_sha"]==spec["source_git_sha"] and value["inputs"]==spec["bindings"],"freeze_spec_mismatch")
    environment=artifacts.document(value["environment"])
    require(value["environment"]["sha256"]==spec["bindings"]["environment_sha256"],"freeze_environment_mismatch")
    require(isinstance(environment["project"],str) and re.fullmatch(r"ats-(?:acceptance|experiment)-[a-z0-9][a-z0-9-]{2,40}",environment["project"])
        and environment["network"]==environment["project"]+"_default" and type(environment["threads"]) is int and environment["threads"]==1
        and type(environment["worker_memory_mib"]) is int and environment["worker_memory_mib"]==1536,"freeze_runtime_resources_mismatch")
    require(set(value["holdout"])=={"manifest","policy","ledger"},"missing_holdout_metadata")
    from ats_mlops.evaluation.native import holdout_ledger
    _,ledger=holdout_ledger()
    require(canonical(value["holdout"]["ledger"])==canonical(ledger)
        and digest(canonical(ledger))==spec["bindings"]["holdout_ledger_sha256"],"freeze_global_ledger_mismatch")
    manifest,policy=(artifacts.document(value["holdout"][key]) for key in ("manifest","policy"))
    lineage=derive_holdout_lineage(policy)
    require(all(lineage[key]==spec["bindings"][key] for key in ("root_policy_sha256","protected_test_documents_sha256"))
        and manifest["artifacts"]["test"]["sha256"]==spec["bindings"]["test_partition_sha256"]
        and environment["holdout"]["manifest"]["sha256"]==value["holdout"]["manifest"]["sha256"]
        and environment["holdout"]["policy"]["sha256"]==value["holdout"]["policy"]["sha256"],"freeze_holdout_lineage_mismatch")
    inventory=manifest["splits"]["test"]
    require(type(inventory["rows"]) is int and inventory["rows"]==spec["test_inventory"]["pairs"]
        and type(inventory["groups"]) is int and inventory["groups"]==spec["test_inventory"]["groups"]
        and digest(canonical(sorted(inventory["vacancy_ids"])))==spec["test_inventory"]["group_ids_sha256"]
        and digest(canonical(sorted(inventory["pair_ids"])))==spec["test_inventory"]["pair_ids_sha256"],"freeze_holdout_inventory_mismatch")
    source=value["source"]
    require(isinstance(source,dict) and set(source)=={"head","status","files"},"invalid_frozen_source_receipt")
    require(artifacts.content(source["head"])==(spec["source_git_sha"]+"\n").encode()
        and artifacts.content(source["status"])==b"","before_evidence_source_was_not_clean")
    files=source["files"]
    required={"src/ats_mlops/evaluation/native.py","src/ats_mlops/evaluation/native_monitoring.py",
        "src/ats_mlops/evaluation/negative_model.py",
        "src/ats_mlops/evaluation/holdout_worker.py","src/ats_mlops/evaluation/experiment.py",
        "tests/e2e/audit.py","tests/e2e/native.py","configs/experiment.template.json","configs/monitoring.yaml",
        "configs/quality.yaml","requirements-ml.lock","requirements-monitoring.lock","requirements-build.lock","docs/experiment-protocol.md"}
    require(isinstance(files,dict) and required<=set(files),"incomplete_frozen_executable_source")
    for name,reference in files.items():
        relative=Path(name)
        require(not relative.is_absolute() and ".." not in relative.parts and relative.name not in
            {"test.jsonl","source.jsonl","corpus.jsonl","split.json"},"unsafe_frozen_source_path")
        # Exact Git object bytes cryptographically bind source/locks/protocol.
        # git show is read-only; it neither imports code nor opens a data fold.
        actual=subprocess.run(["git","show",spec["source_git_sha"]+":"+name],cwd=environment["repo"],
            capture_output=True,timeout=15)
        require(actual.returncode==0 and artifacts.content(reference)==actual.stdout,"frozen_Git_source_bytes_mismatch")
    require(files["docs/experiment-protocol.md"]["sha256"]==spec["bindings"]["protocol_sha256"],"protocol_was_not_frozen")
    for series,descriptor in spec["adapter_bindings"].items():
        expected_module="ats_mlops.evaluation.experiment" if series=="E2" else "ats_mlops.evaluation.native"
        expected_function="ingest_human_measurement" if series=="E2" else "evaluate_e5" if series=="E5" else series.lower()
        name="src/"+expected_module.replace(".","/")+".py"
        require(descriptor=={"entrypoint":expected_module+":"+expected_function,"source_sha256":files[name]["sha256"]},"unfrozen_native_adapter")
    require(set(value["scenarios"])==set(SCENARIOS),"eight_actual_scenarios_required")
    result=inspect_scenarios(path.parent,value["scenarios"],source_sha=spec["source_git_sha"],spec_sha=value["spec_sha256"],validators=VALIDATORS)
    require(result["outcome"]=="passed" and set(value["checks"])==set(result["requirements"])
        and all(item is True for item in value["checks"].values()),"actual_acceptance_incomplete")
    s1=result["scenarios"]["S1"]["validation"]
    require(s1["first_S1_selection_sha256"]==environment["qualified"]["selection"]["sha256"],"freeze_selected_a_different_candidate")
    require(result["scenarios"]["S6"]["validation"]["effective_config_sha256"]==spec["bindings"]["effective_config_sha256"],"freeze_effective_rule_mismatch")
    s1_inputs=artifacts.document(value["scenarios"]["S1"])["native"]
    selected,plan=(artifacts.document(s1_inputs[key]) for key in ("selection","plan"))
    require(canonical(plan["execution"])==canonical(spec["execution"])
        and all(canonical(plan["runtime"][key])==canonical(environment[key]) for key in plan["runtime"]),"qualification_runtime_or_params_changed")
    run=artifacts.document(s1_inputs["run"])
    require(all(run[key]==spec["bindings"][key] for key in ("data_manifest_sha256","split_sha256","features_sha256","params_sha256"))
        and run["reports_sha256"]["admission"]==spec["bindings"]["admission_sha256"]
        and run["environment"]["lockfile_sha256"]==files["requirements-ml.lock"]["sha256"]
        and run["environment"]["build_lockfile_sha256"]==files["requirements-build.lock"]["sha256"],"freeze_actual_data_model_or_lock_mismatch")
    recomputed=result["scenarios"]["S2"]["validation"]["comparison"]
    require(selected["mlflow"]["run_id"] not in recomputed["run_ids"] and selected["mlflow"]["model_version"] not in recomputed["model_versions"],
        "four_qualification_fits_not_distinct")
    s2_inputs=artifacts.document(value["scenarios"]["S2"])["native"]["replicas"]
    require(all(item["features"]["sha256"]==s1["fitted_features_sha256"] for item in s2_inputs),"qualified_S1_feature_state_different")
    require(selected["model_artifact_sha256"]==spec["bindings"]["model_artifact_sha256"]
        and selected["model_lineage_sha256"]==spec["bindings"]["model_lineage_sha256"]
        and selected["source_git_sha"]==spec["qualified_model_source_git_sha"],"frozen_model_differs_from_first_S1")
    return value
