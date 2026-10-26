"""Read-only native receipt validators for the eight T18 qualification scenarios.

No service, model loader, partition reader, network client, or effect callback.
All inputs pass the closed hash graph before semantic validation. Actual runtime
producers are the published evaluation.native adapters, not these assertions.
"""
from .audit import Artifacts, SCENARIOS, canonical, compare_recomputations, decode, digest, finite, hash_value, require, utc


def native(receipt, keys):
    value=receipt["native"]
    require(isinstance(value,dict) and set(value)==set(keys),"invalid_native_receipt_fields")
    return value


def passed(count, **details):
    return {"outcome":"passed","native_evidence_count":count,**details}


def journal(artifacts, reference):
    # This is the existing T13 byte format, not the experiment canonical format.
    from ats_mlops.release.state import canonical as release_canonical
    previous="0"*64;sequence=0;state=None;kinds=[]
    for line in artifacts.content(reference).splitlines(keepends=True):
        require(line.endswith(b"\n"),"incomplete_release_journal")
        event=decode(line);body=dict(event);payload=body.pop("payload_sha256")
        require(type(event["sequence"]) is int and event["sequence"]==sequence+1
            and event["previous_sha256"]==previous and payload==digest(release_canonical(body)),"release_journal_chain_mismatch")
        state=event["state"];sequence=event["sequence"];previous=digest(line);kinds.append(event["kind"])
        require(type(state["sequence"]) is int and state["sequence"]==sequence,"release_journal_projection_mismatch")
    require(state is not None,"empty_release_journal")
    state=dict(state,journal_sha256=previous)
    return state,kinds


def consistent(state, observation):
    require(state["active"] is not None and state["pending"] is None and observation["ready"] is True,"release_not_consistent")
    active=state["active"]
    require(observation["image_id"]==active["image_id"] and all(observation["metadata"].get(key)==value
        for key,value in active["metadata"].items()),"release_API_identity_mismatch")
    require(type(state["generation"]) is int and state["generation"]>0,"release_generation_missing")


def validation_gate(gate, *, accepted):
    require(type(gate["schema_version"]) is int and gate["schema_version"]==1 and gate["test_read"] is False
        and gate["evidence_scope"]=="demo_only" and gate["validation"]["read_partitions"]==["validation"],"gate_not_validation_only")
    require(gate["outcome"]==("passed" if accepted else "rejected"),"unexpected_gate_outcome")
    require(gate["candidate"] is not None and set(gate["scoring_receipts"])>={"candidate"},"gate_did_not_reach_real_scoring")
    require(set(gate["metrics"])>={"baseline","candidate"},"missing_defined_gate_metrics")
    groups=None
    for value in gate["metrics"].values():
        require(value["status"]=="defined" and all(finite(value[name]) for name in ("ndcg_at_10","recall_at_10")),"undefined_gate_quality")
        cohort=value["eligible_group_ids"];require(cohort and (groups is None or cohort==groups),"gate_mixed_validation_cohort");groups=cohort
    require(gate["cohort_sha256"]==digest(canonical(groups)),"gate_cohort_hash_mismatch")
    from ats_mlops.quality.policy import comparison
    rejected=[]
    for role in ("baseline","incumbent"):
        if role not in gate["metrics"]:continue
        expected=comparison(gate["metrics"]["candidate"]["ndcg_at_10"],gate["metrics"][role]["ndcg_at_10"],.005)
        require(canonical(expected)==canonical(gate["comparisons"][role]),"gate_threshold_math_mismatch")
        if not expected["passed"]:rejected.append("candidate.ndcg_drop_against_"+role)
    require(gate["reasons"]==rejected and (not rejected)==accepted,"gate_not_performance_decision")
    for role,value in gate["scoring_receipts"].items():
        require(value["isolated"] is True and value["bytecode_disabled"] is True
            and all(value[name] is False for name in ("prior_project_import","network","training_checkout_mounted","credentials"))
            and value["model_sha256"]==gate[role]["model_sha256"] and hash_value(value["scores_sha256"]),"gate_scorer_boundary_mismatch")


def startup(artifacts, receipt):
    refs=native(receipt,("startup","plan","selection","run","pipeline","gate","package","check","journal","package_files"))
    values={key:artifacts.document(value) for key,value in refs.items() if key not in {"journal","package_files"}}
    start,plan,selection,run,pipeline,gate,package,check=(values[key] for key in ("startup","plan","selection","run","pipeline","gate","package","check"))
    require(start["source_git_sha"]==run["git_sha"]==plan["source_git_sha"]==selection["source_git_sha"]==receipt["source_git_sha"]
        and run["dirty_worktree"] is False and run["test_read"] is False and run["read_partitions"]==["train","validation"],"startup_source_or_partition_mismatch")
    require(plan["slot"]==selection["slot"]=="S1" and plan["selection_rule"]==selection["selection_rule"]=="first_required_fit_before_metrics"
        and plan["formal_E1"]=="not_run" and type(plan["qualification_recomputations"]) is int and plan["qualification_recomputations"]==3
        and utc(plan["declared_at"])<=utc(selection["selected_at"]),"qualification_selection_not_predeclared")
    require(selection["plan_sha256"]==refs["plan"]["sha256"] and selection["model_lineage_sha256"]==refs["run"]["sha256"]
        and selection["model_artifact_sha256"]==run["model"]["sha256"] and selection["mlflow"]==run["mlflow"]==start["mlflow"]
        and start["selected_qualification_sha256"]==refs["selection"]["sha256"],"qualified_candidate_changed")
    require(pipeline["training"]["run_manifest_sha256"]==refs["run"]["sha256"]
        and pipeline["training"]["mlflow"]==run["mlflow"] and pipeline["training"]["model_sha256"]==run["model"]["sha256"]
        and pipeline["selection"]["kind"]=="frozen" and type(pipeline["selection"]["receipt"]["new_trials"]) is int
        and pipeline["selection"]["receipt"]["new_trials"]==0,"startup_pipeline_binding_mismatch")
    validation_gate(gate,accepted=True)
    require(gate["first_release"] is True and gate["expected_incumbent"] is None
        and gate["candidate"]["run_manifest_sha256"]==refs["run"]["sha256"] and gate["candidate"]["model_sha256"]==run["model"]["sha256"],"startup_gate_binding_mismatch")
    require(package["expected_incumbent"] is None and package["gate_report_sha256"]==refs["gate"]["sha256"]
        and check["outcome"]=="passed" and check["release_manifest_sha256"]==check["release_manifest_sha256_after"]==refs["package"]["sha256"]
        and check["gate_report_sha256"]==refs["gate"]["sha256"] and check["candidate"]==package["candidate"]
        and len(check["checks"])==12 and all(v is True for v in check["checks"].values()),"startup_package_check_mismatch")
    files=refs["package_files"]
    require(set(files)==set(package["files"]),"incomplete_package_evidence")
    for name,reference in files.items():
        require(reference["sha256"]==package["files"][name],"package_evidence_inventory_mismatch");artifacts.content(reference)
    model_files={name:package["files"][package["model_relative_path"]+"/"+name] for name in run["model"]["files"]}
    require(model_files==run["model"]["files"] and digest(canonical(model_files))==run["model"]["sha256"],"startup_model_inventory_mismatch")
    state,kinds=journal(artifacts,refs["journal"])
    require(state==start["state"] and "active_committed" in kinds and state["active"]["candidate"]==package["candidate"]
        and state["active"]["manifest_sha256"]==refs["package"]["sha256"],"startup_catalog_mismatch")
    consistent(state,start["observation"])
    return passed(len(refs)+len(files),qualified_model=run["mlflow"],first_S1_selection_sha256=refs["selection"]["sha256"])


def recomputations(artifacts, receipt):
    values=native(receipt,("replicas",))
    result=compare_recomputations(artifacts,values["replicas"],source_sha=receipt["source_git_sha"])
    return passed(21,comparison=result)


def bad_data(artifacts,receipt):
    refs=native(receipt,("schema","guard","bad_gate","fault_provenance"))
    schema,guard,gate,fault=(artifacts.document(refs[key]) for key in refs)
    require(schema["outcome"]=="passed" and schema["original_test_read"] is False and schema["training_invoked"] is False
        and schema["validation"]["outcome"]=="rejected" and any(batch["scope"]=="source" and batch["success"] is False
        for batch in schema["validation"]["gx"]),"bad_schema_not_actually_rejected")
    require(set(guard["fit_calls"])=={"pipeline","classifier"} and all(type(v) is int and v==0 for v in guard["fit_calls"].values())
        and guard["validation"]["outcome"]=="rejected","bad_data_reached_real_fit")
    validation_gate(gate,accepted=False)
    require(fault["scope"]=="controlled_negative_artifact_fault" and fault["source_git_sha"]==receipt["source_git_sha"]
        and fault["candidate_model_sha256"]==gate["candidate"]["model_sha256"]
        and fault["candidate_mlflow"]=={key:gate["candidate"][key] for key in ("model_name","model_version","run_id","experiment_id","exact_uri","source")}
        and fault["selected_for_E5"] is False,"unattributed_negative_model_fixture")
    return passed(4)


def controlled_release(artifacts,receipt):
    refs=native(receipt,("managed","journal","first_rank","second_rank"))
    proof=artifacts.document(refs["managed"]);state,kinds=journal(artifacts,refs["journal"])
    checks={"missing_gate_blocks_stage","missing_approval_blocks_activation","candidate_alias_move_fresh_inference_package_unchanged"}
    require(proof["source_git_sha"]==receipt["source_git_sha"] and set(proof["checks"])==checks
        and all(v is True for v in proof["checks"].values()) and state==proof["state"],"unverified_release_bypass_checks")
    consistent(state,proof["observation"])
    before,after=(artifacts.document(refs[key]) for key in ("first_rank","second_rank"))
    require(before==after and before["release_id"]==state["active"]["release_id"]
        and before["model_version"]==state["active"]["candidate"]["model_version"]
        and digest(canonical(after))==proof["fresh_rank_sha256"],"alias_move_changed_fresh_inference")
    require("approved" in kinds and "active_committed" in kinds,"release_not_actually_approved")
    return passed(4)


def failures(artifacts,receipt):
    refs=native(receipt,("faults","registry"));faults=refs["faults"]
    required={"minio_unavailable","incomplete_artifact","unready_container","switch_alias_crash"}
    require(set(faults)==required,"missing_actual_failure_class")
    for name,inputs in faults.items():
        require(set(inputs)=={"observations","fault","journal"},"invalid_fault_evidence")
        observed=artifacts.document(inputs["observations"]);fault=artifacts.document(inputs["fault"])
        require(observed["fault"]==fault["fault"]==name and observed["fault_receipt_sha256"]==inputs["fault"]["sha256"]
            and observed["final_consistent"] is True and observed["own_resources_only"] is True,"fault_receipt_mismatch")
        from ats_mlops.evaluation.experiment import outage_bounds
        bounds=outage_bounds(observed["http_samples"])
        require(bounds["upper_seconds"] is not None and observed["injected_monotonic"]<=observed["detected_monotonic"]<=observed["restored_monotonic"],"fault_not_bounded_by_actual_HTTP")
        state,_=journal(artifacts,inputs["journal"]);require(state==fault["state"],"fault_catalog_not_replayed");consistent(state,fault["observation"])
    registry=artifacts.document(refs["registry"])
    require(registry["source_git_sha"]==receipt["source_git_sha"] and registry["error_code"]=="registry_unavailable"
        and registry["blocked_next_release"] is True,"registry_failure_falsely_succeeded")
    consistent(registry["state"],registry["observation"])
    return passed(13)


def monitoring(artifacts,receipt):
    refs=native(receipt,("scenarios",));values=refs["scenarios"]
    from ats_mlops.evaluation.experiment import POLICIES,_audit_e4
    require(set(values)==set(POLICIES["E4"]["scenarios"]),"incomplete_real_monitoring_scenarios")
    observed={};effective=set();logical=set()
    for scenario,inputs in values.items():
        require(set(inputs)=={"observations","policy"},"invalid_monitoring_evidence")
        row,proof=artifacts.document(inputs["observations"]),artifacts.document(inputs["policy"])
        require(proof["scenario"]==scenario and proof["source_git_sha"]==receipt["source_git_sha"] and proof["seed"]==20261029
            and row["t17_receipt_sha256"]==inputs["policy"]["sha256"] and hash_value(proof["effective_config_sha256"]),"monitoring_receipt_binding_mismatch")
        require(proof["after"]["requests"]-proof["before"]["requests"]==row["new_requests"],"monitoring_request_count_mismatch")
        created={key:value for key,value in proof["registry_after"].items() if key not in proof["registry_before"]}
        require(len(created)==row["new_versions"] and len(set(proof["registry_after"].values())-set(proof["registry_before"].values()))==row["new_runs"],"monitoring_Registry_effect_mismatch")
        effective.add(proof["effective_config_sha256"]);observed[scenario]=row
    require(len(effective)==1,"monitoring_effective_rule_changed")
    _audit_e4({"E4":POLICIES["E4"]},observed)
    return passed(14,effective_config_sha256=next(iter(effective)))


def genuine_ci(artifacts,receipt):
    refs=native(receipt,("actions","positive_ci","negative_log","positive_fixture","negative_fixture"))
    proof,ci=artifacts.document(refs["actions"]),artifacts.document(refs["positive_ci"])
    require(proof["source_git_sha"]==receipt["source_git_sha"]==ci["git_sha"] and ci["outcome"]=="passed"
        and proof["positive_receipt_sha256"]==refs["positive_ci"]["sha256"] and type(ci["unit_tests"]) is int and ci["unit_tests"]>0
        and len(ci["api_checks"])==12 and all(v is True for v in ci["api_checks"].values())
        and ci["checks"] and all(v is True for v in ci["checks"].values()),"incomplete_actual_CI")
    for role in ("positive","negative"):
        metadata=proof["metadata"][role];run=metadata["run"];states={job["name"]:job["conclusion"] for job in metadata["jobs"]["jobs"]}
        require(run["status"]=="completed" and run["repository"]["full_name"]==run["head_repository"]["full_name"]=="seregatheone/ats_mlops"
            and run["path"]==".github/workflows/ci.yml" and type(run["id"]) is int and run["id"]==proof[role+"_run_id"],"unverified_Actions_metadata")
        require(run["head_sha"]==(receipt["source_git_sha"] if role=="positive" else proof["negative_head"])
            and states==({"static":"success","lifecycle":"success"} if role=="positive" else {"static":"failure","lifecycle":"skipped"})
            and run["conclusion"]==("success" if role=="positive" else "failure"),"Actions_outcome_or_source_mismatch")
    log=artifacts.content(refs["negative_log"]);require(b"AssertionError" in log and b"docker/ci/static.py" in log,"wrong_negative_Actions_failure")
    positive,negative=(artifacts.document(refs[key]) for key in ("positive_fixture","negative_fixture"))
    require(canonical(positive)!=canonical(negative),"CI_fixture_not_restored")
    return passed(5,positive_run=proof["positive_run_id"],negative_run=proof["negative_run_id"])


def claims(artifacts,receipt):
    refs=native(receipt,("register","sources"));register=artifacts.document(refs["register"])
    require(register["evidence_scope"]=="demo_only" and register["independent_real_corpus"] is False
        and set(register["experiments"])=={"E1","E2","E3","E4","E5"}
        and all(value=="not_run" for value in register["experiments"].values()),"qualification_misreported_as_experiment")
    require(register["claims"] and set(refs["sources"])=={row["evidence"]["path"] for row in register["claims"]},"unbound_numeric_claims")
    for name,reference in refs["sources"].items():
        require(name==reference["path"],"claim_source_path_mismatch");artifacts.document(reference)
    from ats_mlops.evaluation.native import claim_traceability
    result=claim_traceability(register,artifacts.root)
    require(len(result["checked_claims"])==len(register["claims"]),"unverified_claim_count")
    return passed(len(refs["sources"])+1)


VALIDATORS=dict(zip(SCENARIOS,(startup,recomputations,bad_data,controlled_release,failures,monitoring,genuine_ci,claims)))
