"""Native E4 over verified T16 cards, the real T17 policy, DAG and T13 catalog.

No effects on import. The driver refuses a default core/project, caller-provided
WindowEvidence, changed thresholds, or a pre-existing policy request. It is only
invoked after the acceptance freeze and an explicit runtime window.
"""
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import base64
import json
import os
from pathlib import Path
import random
import re
import shutil
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .experiment import canonical, immutable, read, require, sha

ORDER = ("control", "false_signal", "insufficient", "shifted_no_labels", "shifted_labeled", "replay", "cooldown")
SETTINGS = {"root", "batch", "monitoring_image", "monitoring_sources", "reference", "calibration", "config", "port",
            "approved_bundle", "approved_bundle_sha256", "approved_at", "bundle_uri", "dvc_env_file"}
BATCH = {"airflow_container", "training_image_id", "airflow_url", "password_file", "serving_identity", "packager_image", "registry_transport"}


def payloads(rows, *, mode, seed, prefix):
    """Predeclared textual perturbation; never inspect score, labels or drift."""
    require(mode in {"control", "shifted", "insufficient"} and type(seed) is int and seed == 20261029,
            "unfrozen_E4_fixture")
    groups = defaultdict(list)
    for row in rows:groups[row["vacancy_id"]].append(row)
    require(len(groups) >= 10 and len(rows) >= 200, "E4_validation_cohort_too_small")
    ordered = sorted(groups);generator = random.Random(seed);generator.shuffle(ordered)
    if mode == "insufficient":ordered = ordered[:1]
    output = []
    for index, group in enumerate(ordered):
        batch = groups[group]
        candidates = sorted(batch, key=lambda row: row["candidate_id"]);generator.shuffle(candidates)
        output.append({"request_id": prefix + "-" + str(index), "vacancy": {"id": group, "text": batch[0]["vacancy_text"]},
            "candidates": [{"id": row["candidate_id"], "text": row["candidate_text"] if mode != "shifted"
                            else " ".join(["Python"] * generator.randint(1, 4))} for row in candidates], "top_k": 3})
    return output


def expected_modes(scenario):
    require(scenario in ORDER, "invalid_E4_scenario")
    return {"control": ("control", "control"), "false_signal": ("shifted", "control"),
            "insufficient": ("insufficient", "insufficient"), "shifted_no_labels": ("shifted", "shifted"),
            "cooldown": ("shifted", "shifted")}.get(scenario, ())


def require_rule(card, spec):
    require(card["outcome"] == "passed" and card["method"] == "wasserstein" and card["threshold"] == .1
            and type(card["threshold"]) in (int, float) and card["drift_share"] == .5
            and sha(canonical(card)) == spec["bindings"]["calibration_sha256"], "E4_frozen_calibration_changed")


class Campaign:
    def __init__(self, spec, config, commands):
        # These modules exist only after the reviewed T17 dependency is merged.
        # There is no fallback to the pure policy or an injected callback.
        from ats_mlops.monitoring.retraining import NativePolicy, AirflowAPI
        from ats_mlops.monitoring.retraining_batch import HostBatch, HostOperations
        from .native import document
        self.spec,self.config,self.commands = spec,config,commands
        self.settings = config["E4"]
        require(isinstance(self.settings, dict) and set(self.settings) == SETTINGS, "invalid_E4_environment")
        settings = self.settings;batch = settings["batch"]
        require(isinstance(batch, dict) and set(batch) == BATCH and batch["registry_transport"] ==
                {"network": config["network"], "tracking_uri": config["tracking_uri"]}, "private_E4_transport_required")
        require(settings["monitoring_sources"] == [spec["source_git_sha"]] and
                all(re.fullmatch(r"sha256:[0-9a-f]{64}", image) for image in
                    (settings["monitoring_image"], batch["training_image_id"], batch["packager_image"])), "E4_runtime_not_pinned")
        require(type(settings["port"]) is int and 10000 <= settings["port"] <= 65535
                and settings["port"] != config["release"]["port"], "private_E4_port_required")
        self.root = Path(settings["root"])
        require(self.root.is_dir() and not self.root.is_symlink(), "E4_campaign_must_be_prepared")
        self.controller_root = self.root / "release-state"
        self.batch = HostBatch(native=NativePolicy(batch["airflow_container"],batch["training_image_id"],project=config["project"]),
            airflow=AirflowAPI(batch["airflow_url"],batch["password_file"]),release_state=self.controller_root,
            artifacts=self.root / "artifacts",source_git_sha=spec["source_git_sha"],source_repository=config["repo"],
            operations=HostOperations(config["repo"],batch["serving_identity"],packager_image=batch["packager_image"],
                                      registry_transport=batch["registry_transport"]))
        self.reference = Path(settings["reference"]["path"])
        self.calibration = Path(settings["calibration"]["path"])
        self.reference_sha = settings["reference"]["sha256"]
        require(sha((self.reference / "reference.json").read_bytes()) == self.reference_sha, "E4_reference_changed")
        require(sha((self.calibration / "calibration.json").read_bytes()) == settings["calibration"]["sha256"]
                == spec["bindings"]["calibration_sha256"], "E4_calibration_changed")
        require_rule(read(self.calibration / "calibration.json"), spec)
        document(settings["config"])
        self.monitoring_config = Path(settings["config"]["path"])
        self.bundle = Path(config["bundle"])

    def _active(self):
        from ats_mlops.monitoring.retraining import active_state
        return active_state(self.controller_root)

    def versions(self):
        uri = self.config["release"]["registry_uri"] + "/api/2.0/mlflow/model-versions/search?" + urlencode({
            "filter": "name='" + self.config["release"]["model_name"] + "'", "max_results": 1000})
        with urlopen(uri, timeout=10) as response:value = json.load(response)
        require(not value.get("next_page_token"), "E4_registry_pagination_required")
        return {item["version"]: item["run_id"] for item in value.get("model_versions", [])}

    def before(self, scenario):
        require(scenario in ORDER, "invalid_E4_scenario")
        index = ORDER.index(scenario)
        require(all((self.root / (previous + ".json")).is_file() for previous in ORDER[:index])
                and not any((self.root / (later + ".json")).exists() for later in ORDER[index:]), "E4_fixed_order_required")
        snapshot = self.batch.native.call("snapshot", {})
        if snapshot == {"state": "uninitialized"}:
            require(index == 0, "E4_policy_history_missing")
            snapshot = {**snapshot, "requests": 0, "windows": 0, "inflight": None}
        require(snapshot["requests"] == (0 if index <= 3 else 1), "E4_unexpected_policy_history")
        with self._active() as observed:
            record = observed["record"]
            if scenario not in {"replay", "cooldown"}:
                require(record["model"]["sha256"] == self.spec["bindings"]["model_artifact_sha256"], "E4_wrong_qualified_model")
            journal = observed["journal_sha256"]
        return snapshot,self.versions(),journal

    def wait_monitor(self, saved, run_id=None):
        run_id = run_id or "monitor__" + saved["installed"]["receipt_id"]
        deadline = time.monotonic()+180
        while time.monotonic() < deadline:
            run = self.batch.airflow.lookup("monitor_and_retrain",run_id,saved["installed"])
            if run and run["state"] == "success":return self.batch.native.call("snapshot",{})
            require(not run or run["state"] != "failed", "E4_real_monitor_DAG_failed")
            time.sleep(1)
        raise TimeoutError("E4_monitor_timeout")

    def renderer(self, action, request, root):
        request_path = root.with_name(root.name+"-request.json");immutable(request_path,request)
        require(not root.exists(), "E4_fresh_numeric_output_required")
        self.commands.run(["docker","run","--rm","--pull=never","--network","none","--read-only","--cpus","1","--memory","512m",
            "--user",f"{os.getuid()}:{os.getgid()}","--tmpfs","/tmp:rw,size=128m,mode=1777",
            "--mount",f"type=bind,source={request_path.parent.resolve()},target={request_path.parent.resolve()}",
            self.settings["monitoring_image"],action,"--request",request_path,"--output",root],timeout=180)
        return read(root / ("window.json" if action == "report" else "calibration.json"))

    def _copy_numeric(self, case):
        shutil.copytree(self.reference,case / "reference")
        shutil.copytree(self.calibration,case / "calibration")
        (case / "monitoring.json").write_bytes(self.monitoring_config.read_bytes())
        return case / "reference",case / "calibration",case / "monitoring.json"

    def windows(self, scenario, attempt):
        """Real inference full groups, native WAL export, actual Evidently/DAGs."""
        from ats_mlops.data.dataset import load_partition
        from ats_mlops.monitoring.feedback import load_export,admit_feedback
        from ats_mlops.monitoring.drift import configuration
        from ats_mlops.monitoring.retraining_batch import execution_plan
        from .native import _controller,_cleanup_release
        case = Path(attempt) / "cohort";case.mkdir()
        reference,calibration,settings = self._copy_numeric(case)
        rows = load_partition(self.bundle / "data_manifest.json","validation")
        # The inference replica is owned by this case and never uses the working
        # catalog's event volume. Its package is read from verified ACTIVE state.
        with self._active() as observed:
            package,active,record = observed["package"],observed["active"],observed["record"]
        fixture_config = deepcopy(self.config);fixture_config["release"]["port"] = self.settings["port"]
        fixture = _controller(fixture_config,case)
        manifest = read(package / "release_manifest.json")
        # A serving clone does not create a fake T13 catalog or call stage with
        # the ACTIVE model's old incumbent gate. DockerRuntime launches the exact
        # already checked ACTIVE package, with its immutable manifest/check.
        launch = {**active,"package_path":"release/package","launch_path":"fixture-launch.json"}
        copied = fixture.root / "release/package";copied.parent.mkdir(parents=True);shutil.copytree(package,copied)
        immutable(fixture.root / "fixture-launch.json", {"schema_version": 1, "package_path": launch["package_path"],
            "image_id": active["image_id"], "config_sha256": sha(canonical(fixture.config))})
        intervals=[]
        try:
            fixture.runtime.launch(launch)
            fixture.runtime.wait(launch)
            def post(route,value):
                with urlopen(Request(f"http://127.0.0.1:{self.settings['port']}"+route,data=canonical(value),
                    headers={"Content-Type":"application/json"}),timeout=10) as response:return json.load(response)
            deadline=time.monotonic()+60
            while True:
                try:
                    metadata=post("/metadata",{})
                    if metadata.get("ready"):break
                except Exception:pass
                require(time.monotonic()<deadline,"E4_private_API_not_ready");time.sleep(.2)
            require(metadata["release_id"]==active["release_id"] and metadata["model_version"]==record["mlflow"]["model_version"],"E4_fixture_model_changed")
            start=datetime.now(timezone.utc).isoformat()
            for index,mode in enumerate(expected_modes(scenario)):
                for payload in payloads(rows,mode=mode,seed=self.spec["E4"]["seed"],prefix=self.config["project"]+"-"+scenario+str(index)):
                    response=post("/rank",payload)
                    require(response["release_id"]==active["release_id"] and len(response["ranking"])==3,"E4_rank_contract_changed")
                end=datetime.now(timezone.utc).isoformat();intervals.append({"start":start,"end":end});start=end
            cutoff=datetime.now(timezone.utc).isoformat()
            code="""import sys,json,base64
from pathlib import Path
sys.path.insert(0,'/source/src')
from ats_mlops.monitoring.feedback import export_events
export_events('/events/predictions.sqlite3','/tmp/export',sys.argv[1])
print(json.dumps({name:base64.b64encode(Path('/tmp/export',name).read_bytes()).decode() for name in ('events.jsonl','export.json')}))
"""
            output=self.commands.run(["docker","run","--rm","--pull=never","--network","none","--read-only","--cpus",".5","--memory","512m",
                "--user","50000:50000","--tmpfs","/tmp:rw,size=64m,mode=1777",
                "--mount",f"type=volume,source={fixture.config['events_volume']},target=/events",
                "--mount",f"type=bind,source={Path(self.config['repo']).resolve()},target=/source,readonly",
                "--entrypoint","/opt/ml-venv/bin/python",self.settings["batch"]["packager_image"],"-I","-B","-c",code,cutoff])
            exported=json.loads(output.stdout);directory=case / "export";directory.mkdir()
            require(set(exported)=={"events.jsonl","export.json"},"E4_invalid_export")
            for name,content in exported.items():(directory/name).write_bytes(base64.b64decode(content,validate=True))
            load_export(directory)
        finally:
            _cleanup_release(fixture,self.commands)
        admit_feedback(case/"export",[],case/"feedback",configuration(settings)["label_sources"],ledger=self.root/"feedback.sqlite3")
        saved=[]
        for index,interval in enumerate(intervals):
            request={"reference":str(reference),"reference_sha256":self.reference_sha,"export":str(case/"export"),
                "feedback":str(case/"feedback"),"calibration":str(calibration),"calibration_sha256":sha((calibration/"calibration.json").read_bytes()),
                "config":str(settings),**interval,"cutoff":cutoff}
            card=self.renderer("report",request,case/("window-"+str(index)))
            expected=expected_modes(scenario)[index]
            require(card["drift"]["detected"] is (None if expected=="insufficient" else expected=="shifted"),"E4_predeclared_signal_not_observed")
            value={key:request[key] for key in ("reference","reference_sha256","calibration","calibration_sha256","config","export","feedback")}
            value.update(window=str(case/("window-"+str(index))),window_sha256=sha(canonical(card)),bundle=str(self.bundle),monitoring_sources=self.settings["monitoring_sources"])
            dispatched=self.batch.install_window(value,execution_plan(self.spec["source_git_sha"],self.settings["batch"]["training_image_id"]))
            snapshot=self.wait_monitor(dispatched)
            require(snapshot["reference"]["cohort"]["effective_config_sha256"]==self.spec["bindings"]["effective_config_sha256"],"E4_effective_rule_changed")
            saved.append({"card":card,"inputs":value,"dispatch":dispatched,"policy":snapshot})
        return saved

    def labeled(self):
        """Resume the original waiting request; no new policy/request/fit IDs."""
        from ats_mlops.pipeline import verify_bundle
        from ats_mlops.monitoring import policy as p
        from .native import document
        previous=read(self.root/"shifted_no_labels.json");request_id=previous["logical_request_id"]
        child=Path(self.settings["approved_bundle"])
        verify_bundle(child,self.settings["approved_bundle_sha256"])
        require(self.settings["bundle_uri"].startswith("s3://ats-dvc/orchestration/bundles/"),"private_approved_bundle_URI_required")
        # Upload only the admitted train/validation carrier. Credentials are the
        # private core's local env-file; neither values nor config are logged.
        secret=Path(self.settings["dvc_env_file"])
        require(secret.is_file() and not secret.is_symlink() and secret.stat().st_mode & 0o077==0
                and ".local/core" not in str(secret) and self.config["project"] in str(secret.parent),"private_core_credentials_required")
        prefix=self.settings["bundle_uri"].removeprefix("s3://ats-dvc/").removesuffix("/bundle.json")
        code="""import boto3,json,sys
from pathlib import Path
root=Path('/bundle');descriptor=json.loads((root/'bundle.json').read_bytes());c=boto3.client('s3',endpoint_url='http://minio:9000')
for name in [*sorted(descriptor['files']),'bundle.json']:
 data=(root/name).read_bytes();key=sys.argv[1]+'/'+name
 try:old=c.get_object(Bucket='ats-dvc',Key=key)['Body'].read()
 except c.exceptions.NoSuchKey:old=None
 if old is None:c.put_object(Bucket='ats-dvc',Key=key,Body=data)
 else:assert old==data
 assert c.get_object(Bucket='ats-dvc',Key=key)['Body'].read()==data
print(json.dumps({'inventory_sha256':descriptor['sha256']}))
"""
        uploaded=self.commands.run(["docker","run","--rm","--pull=never","--network",self.config["network"],"--cpus","1","--memory","512m",
            "--env-file",secret,"--mount",f"type=bind,source={child.resolve()},target=/bundle,readonly","--entrypoint","/opt/ml-venv/bin/python",
            self.settings["batch"]["packager_image"],"-I","-B","-c",code,prefix])
        require(json.loads(uploaded.stdout)["inventory_sha256"]==self.settings["approved_bundle_sha256"],"E4_bundle_upload_mismatch")
        saved=self.batch.install_dataset(request_id,self.bundle,child,actor="agent-operator",bundle_uri=self.settings["bundle_uri"],
                                        approved_at=p.parse_stamp(self.settings["approved_at"]))
        self.wait_monitor(saved)
        deadline=time.monotonic()+900
        while True:
            result=self.batch.advance(request_id)
            if result["state"]=="waiting_approval":break
            require(result["state"] not in {"terminal_failed","gate_rejected"} and time.monotonic()<deadline,"E4_candidate_did_not_qualify")
            time.sleep(2)
        request=self.batch.native.call("request",{"request_id":request_id});candidate=request["candidate"]
        from ats_mlops.release.controller import Controller
        controller=Controller(self.controller_root)
        controller.approve(candidate["release_id"],"agent-operator","A3");controller.activate(candidate["release_id"])
        directory=self.root/"artifacts"/request_id
        reference_request={"bundle":str(child),"bundle_sha256":self.settings["approved_bundle_sha256"],"package":str(directory/"package"),
            "release_check":str(directory/"release-check.json"),"release_manifest_sha256":sha((directory/"package/release_manifest.json").read_bytes()),
            "scope":"approved_training","dataset_approval":str(directory/"dataset-approval.json")}
        # Host build_reference is stdlib+artifact-only worker, no model import in
        # host. The admission was genuine and belongs to this exact candidate.
        from ats_mlops.monitoring.drift import build_reference
        reference=self.root/"reference-v2";card=build_reference(reference_request,reference)
        reference_sha=sha((reference/"reference.json").read_bytes())
        # Re-express the SAME declared rule for the new matched reference; fail
        # if the provider would select a different threshold, never retune E4.
        numeric=self.root/"reference-v2-numeric";numeric.mkdir()
        shutil.copytree(reference,numeric/"reference");(numeric/"monitoring.json").write_bytes(self.monitoring_config.read_bytes())
        calibration=self.renderer("calibrate",{"reference":str(numeric/"reference"),"reference_sha256":reference_sha,
            "config":str(numeric/"monitoring.json")},numeric/"calibration")
        require(calibration["threshold"]==.1 and calibration["drift_share"]==.5,"E4_rule_refresh_cannot_retune")
        refreshed=self.batch.update_reference(request_id,reference,reference_sha,child,config=self.monitoring_config,
            calibration=numeric/"calibration",calibration_sha256=sha(canonical(calibration)),monitoring_sources=self.settings["monitoring_sources"])
        require(refreshed["state"]=="active","E4_reference_not_ACTIVE")
        return {"request_id":request_id,"request":self.batch.native.call("request",{"request_id":request_id}),
            "reference":str(reference),"reference_sha256":reference_sha,"calibration":str(numeric/"calibration"),
            "bundle":str(child),"approved_labels_sha256":sha((directory/"dataset-approval.json").read_bytes()),"refresh":refreshed}

    def run(self, scenario, attempt):
        before,versions_before,journal_before=self.before(scenario)
        details={};windows=[]
        if scenario=="shifted_labeled":
            details=self.labeled()
        elif scenario=="replay":
            original=read(self.root/"shifted_no_labels.json")
            for index,window in enumerate(original["native"]["windows"]):
                installed=window["dispatch"]["installed"];run_id="monitor_replay__"+installed["receipt_id"]
                self.batch.airflow.trigger("monitor_and_retrain",run_id,installed)
                self.wait_monitor(window["dispatch"],run_id)
        else:
            if scenario=="cooldown":
                refreshed=read(self.root/"shifted_labeled.json")["native"]["labeled"]
                self.reference=Path(refreshed["reference"]);self.reference_sha=refreshed["reference_sha256"]
                self.calibration=Path(refreshed["calibration"]);self.bundle=Path(refreshed["bundle"])
            windows=self.windows(scenario,attempt)
        after=self.batch.native.call("snapshot",{});versions_after=self.versions()
        created={version:run for version,run in versions_after.items() if version not in versions_before}
        new_runs=set(versions_after.values())-set(versions_before.values())
        require(len(created)==len(new_runs)==(1 if scenario=="shifted_labeled" else 0),"E4_unexpected_model_effect")
        expected_requests=1 if scenario=="shifted_no_labels" else 0
        require(after["requests"]-before["requests"]==expected_requests,"E4_unexpected_request_effect")
        request_id=after["inflight"] if scenario=="shifted_no_labels" else details.get("request_id")
        if scenario=="shifted_no_labels":
            request=self.batch.native.call("request",{"request_id":request_id})
            require(request["state"]=="waiting_for_labels" and all(item["card"]["quality"]["quality"] is None for item in windows),"E4_quality_without_independent_labels")
        with self._active() as observed:journal_after=observed["journal_sha256"]
        proof={"schema_version":1,"scenario":scenario,"source_git_sha":self.spec["source_git_sha"],"seed":20261029,
            "effective_config_sha256":self.spec["bindings"]["effective_config_sha256"],"before":before,"after":after,
            "registry_before":versions_before,"registry_after":versions_after,"journal_before":journal_before,"journal_after":journal_after,
            "windows":windows,"labeled":details}
        immutable(Path(attempt)/"native-policy.json",proof)
        observation={"seed":20261029,"t17_receipt_sha256":sha(canonical(proof)),"active_catalog_verified":True,
            "policy_checks":{"native_SQL":True,"real_DAG":True,"effective_rule_unchanged":True,"exact_Registry":True},
            "new_requests":expected_requests,"new_runs":len(new_runs),"new_versions":len(created)}
        if scenario=="shifted_no_labels":
            observation.update(quality=None,terminal="waiting_for_labels",logical_request_id=request_id,
                windows=[{"id":window["card"]["window_id"],"start":window["card"]["interval"]["start"],"end":window["card"]["interval"]["end"],
                    "pairs":window["card"]["counts"]["pairs"],"vacancies":window["card"]["counts"]["vacancies"],"closed":True} for window in windows])
        if scenario=="shifted_labeled":observation.update(logical_request_id=request_id,
            approved_labels_sha256=details["approved_labels_sha256"],reference_refreshed_after_active=True)
        immutable(self.root/(scenario+".json"),{"observations":observation,"logical_request_id":request_id,"native":proof})
        return observation
