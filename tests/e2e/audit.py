"""Read-only semantic acceptance auditor. File presence and skips never mean PASS.

Preparation code only: scenario runners are existing audited integration scripts.
This module never starts services, fits a model, triggers a DAG or reads test rows.
"""
from __future__ import annotations

from datetime import datetime
import ast
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess

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
        require(set(command) == {"id", "argv", "exit_code", "expected_exit_code", "accepted_exit_codes", "stdout", "stderr"}
                and isinstance(command["id"], str) and command["id"] not in ids, "invalid_command_record")
        ids.add(command["id"])
        require(isinstance(command["argv"], list) and command["argv"] and all(isinstance(value, str) and value for value in command["argv"])
                and type(command["exit_code"]) is int and type(command["expected_exit_code"]) is int
                and isinstance(command["accepted_exit_codes"],list) and command["accepted_exit_codes"]
                and all(type(code) is int for code in command["accepted_exit_codes"])
                and command["exit_code"] == command["expected_exit_code"] and command["exit_code"] in command["accepted_exit_codes"], "unexpected_command_result")
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


_SOURCE_REQUIRED = {"src/ats_mlops/evaluation/native.py", "src/ats_mlops/evaluation/native_monitoring.py",
    "src/ats_mlops/evaluation/negative_model.py", "src/ats_mlops/evaluation/holdout_worker.py",
    "src/ats_mlops/evaluation/experiment.py", "tests/e2e/audit.py", "tests/e2e/native.py", "tests/e2e/driver.py",
    "configs/experiment.template.json", "configs/monitoring.yaml", "configs/quality.yaml", "requirements-ml.lock",
    "requirements-monitoring.lock", "requirements-build.lock", "docs/experiment-protocol.md"}
_SOURCE_CHANGES = {"src/ats_mlops/evaluation/native_monitoring.py", "src/ats_mlops/evaluation/negative_model.py",
    "tests/e2e/audit.py", "tests/e2e/driver.py", "tests/unit/test_negative_model_contract.py",
    "tests/unit/test_monitoring_launch_contract.py", "tests/unit/test_acceptance_execution_sources.py"}
_AUDIT_SEMANTICS = ("SCENARIOS", "EvidenceError", "require", "Artifacts", "canonical", "digest", "hash_value",
    "decode", "finite", "utc", "derive_holdout_lineage", "execution", "compare_recomputations")
_DRIVER_SEMANTICS = ("PHASES", "authorize", "phase", "Collector.target", "Collector.copy", "Collector.commands")


def _git(repository, *arguments):
    result = subprocess.run(["git", *arguments], cwd=repository, capture_output=True, timeout=15)
    require(result.returncode == 0, "missing_or_invalid_execution_Git_object")
    return result.stdout


def _source_snapshot(artifacts, snapshot, source_sha, repository):
    require(isinstance(snapshot, dict) and set(snapshot) == {"head", "status", "files"}, "invalid_execution_source_snapshot")
    require(artifacts.content(snapshot["head"]) == (source_sha + "\n").encode()
            and artifacts.content(snapshot["status"]) == b"", "execution_source_was_not_clean")
    require(isinstance(snapshot["files"], dict) and _SOURCE_REQUIRED <= set(snapshot["files"]), "incomplete_execution_source_snapshot")
    for name, reference in snapshot["files"].items():
        require(isinstance(name, str) and name, "unsafe_execution_source_path")
        relative = Path(name)
        require("\\" not in name and not relative.is_absolute()
                and relative.as_posix() == name and ".." not in relative.parts
                and relative.name not in {"test.jsonl", "source.jsonl", "corpus.jsonl", "split.json"}, "unsafe_execution_source_path")
        require(artifacts.content(reference) == _git(repository, "show", source_sha + ":" + name), "execution_Git_source_bytes_mismatch")


def _node_bytes(content, name):
    text = content.decode(); body = ast.parse(text).body
    for part in name.split("."):
        nodes = [node for node in body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name == part
                 or isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == part for target in node.targets)]
        require(len(nodes) == 1, "missing_preserved_source_node")
        node = nodes[0]; body = getattr(node, "body", [])
    return ast.get_source_segment(text, node).encode()


def validate_execution_sources(artifacts, mapping, historical, *, source_sha, qualified_sha, repository):
    """Only the selected ancestor and a narrow verified code carry-forward."""
    require(hash_value(source_sha, 40) and hash_value(qualified_sha, 40)
            and isinstance(mapping, dict) and set(mapping) == set(SCENARIOS), "invalid_execution_source_map")
    expected = {key: qualified_sha if key in {"S1", "S2", "S3", "S4", "S5"} else source_sha for key in SCENARIOS}
    require(mapping == expected and isinstance(historical, dict)
            and set(historical) == (set() if source_sha == qualified_sha else {qualified_sha}), "misassigned_execution_source")
    if source_sha == qualified_sha:
        return mapping
    _source_snapshot(artifacts, historical[qualified_sha], qualified_sha, repository)
    _git(repository, "merge-base", "--is-ancestor", qualified_sha, source_sha)
    changes = _git(repository, "diff", "--no-renames", "--name-status", qualified_sha, source_sha, "--").decode().splitlines()
    for line in changes:
        status, name = line.split("\t", 1)
        require(name in _SOURCE_CHANGES and status in {"A", "M"}
                and (not name.startswith("tests/unit/") or status == "A"), "historical_runtime_code_changed")
    for name, preserved in (("tests/e2e/audit.py", _AUDIT_SEMANTICS), ("tests/e2e/driver.py", _DRIVER_SEMANTICS)):
        old, current = (_git(repository, "show", sha + ":" + name) for sha in (qualified_sha, source_sha))
        require(all(_node_bytes(old, key) == _node_bytes(current, key) for key in preserved), "historical_semantic_checks_changed")
    return mapping


def _attested_commands(artifacts, attestation, receipt_commands, selected):
    aggregate = artifacts.document(attestation["commands"])
    require(type(aggregate.get("schema_version")) is int and aggregate["schema_version"] == 1
            and isinstance(aggregate.get("commands"), list) and aggregate["commands"], "invalid_phase_commands")
    fields = [key for key in ("evidence_sha256", "command_evidence_sha256") if key in selected]
    require(len(fields) == 1 and selected[fields[0]] == attestation["commands"]["sha256"], "phase_command_aggregate_not_bound")
    ids = attestation["command_ids"]
    require(isinstance(ids, list) and len(ids) == len(aggregate["commands"]) and len(set(ids)) == len(ids)
            and all(isinstance(value, str) and value in receipt_commands for value in ids), "invalid_phase_command_ids")
    positions = [receipt_commands[value][0] for value in ids]
    require(positions == sorted(positions), "phase_command_order_changed")
    captured_stdout = []
    for raw, identifier in zip(aggregate["commands"], ids, strict=True):
        captured = receipt_commands[identifier][1]
        require(isinstance(raw, dict) and set(raw) == {"argv", "exit_code", "accepted_exit_codes", "stdout", "stderr"}
                and all(raw[key] == captured[key] for key in ("argv", "exit_code", "accepted_exit_codes")), "phase_command_record_changed")
        for stream in ("stdout", "stderr"):
            reference = raw[stream]
            require(isinstance(reference, dict) and set(reference) == {"path", "sha256"}
                    and hash_value(reference["sha256"]) and reference["sha256"] == captured[stream]["sha256"], "phase_command_stream_changed")
            content = artifacts.content(captured[stream])
            if stream == "stdout": captured_stdout.append(content)
    return set(ids), captured_stdout


def _attested_phase(artifacts, attestation, receipt, source_sha, commands):
    keys = {"phase", "intent", "spec", "environment", "authorization", "commands", "command_ids", "result_index", "leaves"}
    require(isinstance(attestation, dict) and set(attestation) == keys, "invalid_phase_attestation")
    phase, intent, spec, environment, authorization = [artifacts.document(attestation[key]) for key in
        ("phase", "intent", "spec", "environment", "authorization")]
    require(type(phase.get("schema_version")) is int and phase["schema_version"] == 1 and phase.get("outcome") == "completed"
            and type(intent.get("schema_version")) is int and intent["schema_version"] == 1
            and phase.get("source_git_sha") == intent.get("source_git_sha") == spec.get("source_git_sha") == source_sha
            and phase.get("phase") == intent.get("phase") and phase["started_at"] == intent["started_at"]
            and utc(receipt["started_at"]) <= utc(phase["started_at"]) < utc(phase["finished_at"]) <= utc(receipt["finished_at"]), "phase_identity_or_interval_mismatch")
    name = phase["phase"]
    from ats_mlops.evaluation.experiment import validate_spec
    from .driver import PHASES
    validate_spec(spec, template=name == "S1")
    require(attestation["spec"]["sha256"] == digest(canonical(spec)) == intent.get("input_spec_sha256")
            and spec["bindings"]["environment_sha256"] == attestation["environment"]["sha256"], "phase_spec_or_environment_changed")
    require(set(authorization) == {"schema_version", "scope", "actor", "source_git_sha", "project", "input_spec_sha256", "phases"}
            and type(authorization["schema_version"]) is int and authorization["schema_version"] == 1
            and authorization["scope"] == "T18_private_qualification" and authorization["actor"] == "root-coordinator"
            and authorization["source_git_sha"] == source_sha and authorization["project"] == environment["project"]
            and authorization["input_spec_sha256"] == attestation["spec"]["sha256"]
            and isinstance(authorization["phases"], list) and name in authorization["phases"]
            and all(isinstance(item, str) for item in authorization["phases"])
            and set(authorization["phases"]) <= set(PHASES)
            and intent.get("authorization_sha256") == attestation["authorization"]["sha256"], "phase_authorization_changed")
    if "input_spec_sha256" in phase:
        require(phase["input_spec_sha256"] == intent["input_spec_sha256"], "phase_result_spec_changed")
    if "intent_sha256" in phase:
        require(phase["intent_sha256"] == attestation["intent"]["sha256"], "phase_result_intent_changed")
    index = attestation["result_index"]; selected = phase["result"]
    if index is not None:
        require(name == "S2" and type(index) is int and 0 <= index <= 2 and isinstance(selected, list) and len(selected) == 3, "invalid_phase_result_index")
        selected = selected[index]
    else:
        require(name != "S2", "indexed_S2_phase_required")
    require(isinstance(selected, dict) and selected.get("outcome") ==
            ("expected_rejection" if name.startswith("fault-") else "passed"), "contradictory_phase_result")
    ids, stdout = _attested_commands(artifacts, attestation, commands, selected)
    leaves = attestation["leaves"]
    require(isinstance(leaves, dict) and set(leaves) <= {"result", "observations", "fault", "receipt", "stdout_json", "input_request", "bad_gate"}, "invalid_phase_leaf")
    values = {key: artifacts.document(reference) for key, reference in leaves.items()}
    for key, value in values.items():
        if key == "result": require(value == selected, "phase_result_leaf_changed")
        elif key == "observations": require(value == selected["observations"], "phase_observations_leaf_changed")
        elif key == "fault": require(leaves[key]["sha256"] == selected["observations"]["fault_receipt_sha256"], "phase_fault_leaf_changed")
        elif key == "receipt": require(leaves[key]["sha256"] == selected["native_receipt_sha256"], "phase_native_receipt_changed")
        elif key == "input_request":
            require(leaves[key]["sha256"] == environment["fixtures"]["bad_candidate_request"]["sha256"], "phase_bad_request_changed")
        elif key == "stdout_json":
            matched = False
            for output in stdout:
                try: matched = matched or decode(output.strip().splitlines()[-1]) == value
                except (EvidenceError, IndexError): pass
            require(matched, "phase_stdout_json_not_observed")
    return {"name": name, "index": index, "leaves": leaves, "values": values, "selected": selected, "spec": spec, "command_ids": ids}


def phase_attestations(artifacts, receipt, source_sha):
    """Bind historical leaves to their original authorized phase and raw logs."""
    value = receipt.get("phase_attestations")
    require(isinstance(value, list) and value, "historical_phase_attestations_required")
    commands = {command["id"]: (index, command) for index, command in enumerate(receipt["commands"])}
    phases = [_attested_phase(artifacts, item, receipt, source_sha, commands) for item in value]
    ids = [identifier for phase in phases for identifier in phase["command_ids"]]
    require(len(ids) == len(set(ids)) and set(ids) == set(commands), "unattested_or_reused_scenario_commands")
    native = receipt["native"]; scenario = receipt["scenario"]
    def exact(phase, name, leaves):
        require(phase["name"] == name and phase["index"] is None and phase["leaves"] == leaves, "historical_phase_coverage_mismatch")
    if scenario == "S1":
        require(len(phases) == 1, "S1_phase_coverage_mismatch")
        exact(phases[0], "S1", {"result": native["startup"]})
    elif scenario == "S2":
        require(len(phases) == 3 and {phase["index"] for phase in phases} == {0, 1, 2}, "S2_phase_coverage_mismatch")
        for phase in phases:
            require(phase["name"] == "S2" and set(phase["leaves"]) == {"observations"}, "S2_observations_required")
            replica = native["replicas"][phase["index"]]; observed = phase["values"]["observations"]
            run, features, scores, metrics = [artifacts.document(replica[key]) for key in ("run", "features", "scores", "metrics")]
            require(observed["source_git_sha"] == source_sha and observed["workspace_id"] == replica["workspace_id"]
                    and observed["training_run_id"] == run["mlflow"]["run_id"] and observed["model_version"] == run["mlflow"]["model_version"]
                    and observed["data_sha256"] == run["data_manifest_sha256"] and observed["split_sha256"] == run["split_sha256"]
                    and observed["features_sha256"] == run["features_sha256"]
                    and observed["coordinates_sha256"] == digest(canonical(phase["spec"]["bindings"]))
                    and observed["vocabulary_sha256"] == digest(canonical({key: features[key] for key in ("vocabulary", "idf", "transformed_sha256")}))
                    and observed["scores"] == [{"group_id": row["vacancy_id"], "candidate_id": row["candidate_id"], "score": row["score"]} for row in scores["rows"]]
                    and observed["metrics"] == {key: metrics[key] for key in ("ndcg_at_10", "recall_at_10")}, "S2_phase_numeric_inputs_changed")
    elif scenario == "S3":
        require(len(phases) == 3 and {phase["name"] for phase in phases} == {"schema", "fault-corrupt_data", "fault-bad_candidate"}, "S3_phase_coverage_mismatch")
        for phase in phases:
            if phase["name"] == "schema": exact(phase, "schema", {"receipt": native["schema"]})
            elif phase["name"] == "fault-corrupt_data": exact(phase, "fault-corrupt_data", {"stdout_json": native["guard"]})
            else:
                require(phase["index"] is None and set(phase["leaves"]) == {"input_request", "bad_gate", "observations", "fault"}
                        and phase["leaves"]["bad_gate"] == native["bad_gate"]
                        and phase["values"]["observations"]["fault"] == phase["values"]["fault"]["fault"] == "bad_candidate", "S3_bad_retry_not_bound")
    elif scenario == "S4":
        require(len(phases) == 1, "S4_phase_coverage_mismatch")
        exact(phases[0], "managed", {"receipt": native["managed"]})
    elif scenario == "S5":
        require(len(phases) == 5 and {phase["name"] for phase in phases} == {"registry", *("fault-" + name for name in native["faults"])}, "S5_phase_coverage_mismatch")
        for phase in phases:
            if phase["name"] == "registry": exact(phase, "registry", {"receipt": native["registry"]})
            else:
                name = phase["name"].removeprefix("fault-"); expected = native["faults"][name]
                exact(phase, phase["name"], {"observations": expected["observations"], "fault": expected["fault"]})
    else:
        raise EvidenceError("unexpected_historical_scenario")
    return len(phases)


def inspect_scenarios(directory, references, *, source_sha, spec_sha, validators, execution_sources=None):
    """No implicit default validator, skipped case or historical leaf substitution.

    Each adapter must validate its existing native report schemas. During early
    preparation absent/unfrozen adapters are explicitly blocked. Only root's
    later runtime window may produce the eight actual acceptance receipts.
    """
    require(hash_value(source_sha, 40) and hash_value(spec_sha), "invalid_qualification_identity")
    require(isinstance(references, dict) and set(references) <= set(SCENARIOS), "unknown_scenario")
    require(isinstance(validators, dict) and set(validators) <= set(SCENARIOS), "unknown_scenario_adapter")
    if execution_sources is not None:
        require(isinstance(execution_sources, dict) and set(execution_sources) == set(SCENARIOS)
                and hash_value(execution_sources["S1"], 40)
                and all(execution_sources[key] == (execution_sources["S1"] if key in {"S1", "S2", "S3", "S4", "S5"} else source_sha)
                        for key in SCENARIOS), "invalid_scenario_execution_source")
    artifacts = Artifacts(directory)
    results = {}
    for identifier, (name, requirements) in SCENARIOS.items():
        if identifier not in references:
            results[identifier] = {"outcome": "blocked", "reason": "missing_actual_receipt"}
            continue
        try:
            actual_source = source_sha if execution_sources is None else execution_sources[identifier]
            receipt = execution(artifacts.document(references[identifier]), actual_source, spec_sha)
            require(receipt.get("scenario") == identifier and receipt.get("name") == name, "scenario_identity_mismatch")
            for command in receipt["commands"]:
                artifacts.content(command["stdout"])
                artifacts.content(command["stderr"])
            if actual_source != source_sha:
                phase_attestations(artifacts, receipt, actual_source)
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
          "source","holdout","scenarios","checks","scope","E2","independent_real_corpus","open_blocking_defects",
          "execution_sources","historical_sources"}
    require(isinstance(value,dict) and set(value)==keys and type(value["schema_version"]) is int and value["schema_version"]==2
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
        "tests/e2e/audit.py","tests/e2e/native.py","tests/e2e/driver.py","configs/experiment.template.json","configs/monitoring.yaml",
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
    sources=validate_execution_sources(artifacts,value["execution_sources"],value["historical_sources"],
        source_sha=spec["source_git_sha"],qualified_sha=spec["qualified_model_source_git_sha"],repository=environment["repo"])
    require(set(value["scenarios"])==set(SCENARIOS),"eight_actual_scenarios_required")
    result=inspect_scenarios(path.parent,value["scenarios"],source_sha=spec["source_git_sha"],spec_sha=value["spec_sha256"],
        validators=VALIDATORS,execution_sources=sources)
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
    negative=result["scenarios"]["S3"]["validation"]
    require(negative["parent_run_manifest_sha256"]==s1_inputs["run"]["sha256"]
        and negative["parent_model_sha256"]==selected["model_artifact_sha256"]
        and negative["incumbent_release_id"]==artifacts.document(s1_inputs["startup"])["release_id"],"negative_fixture_not_derived_from_selected_S1")
    return value
