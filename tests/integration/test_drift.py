"""Actual batch/HTTP/WAL gate; no fits, admission rerun or production activation.

Run as a script with an already admitted canonical bundle and a clean gate-passed
candidate. Every Docker resource is unique, owned here and removed in finally.
"""
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import argparse
import json
import os
from pathlib import Path
import subprocess
import shutil
import sys
import time
import uuid
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))
from ats_mlops.monitoring.feedback import (FeedbackStore, admit_feedback, canonical, export_events,
    load_export, load_feedback, read_json, sha, utc)
from ats_mlops.monitoring.drift import build_reference, load_reference
from ats_mlops.release.controller import load_package
from ats_mlops.monitoring.quality import assess
from service.integrity import inventory, release_binding
from service.release_check import stage


def write(path, value):
    Path(path).write_bytes(canonical(value))


def stamp():
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser()
    for key in ("output", "bundle", "fit", "gate"):
        parser.add_argument("--" + key, type=Path, required=True)
    parser.add_argument("--monitoring-image", required=True)
    parser.add_argument("--serving-image", required=True)
    parser.add_argument("--old-serving-image", required=True)
    parser.add_argument("--port", type=int, default=3017)
    parser.add_argument("--reuse-checked", type=Path, help="hash-verify and explicitly reuse an earlier passed T12 package/reference subgate")
    args = parser.parse_args()
    out = args.output.resolve();out.mkdir(parents=True, exist_ok=False)
    (out / "monitoring.yaml").write_bytes((ROOT / "configs/monitoring.yaml").read_bytes())
    write(out / "limits.json", {"services": {"ranking": {"cpus": 1, "mem_limit": "1536m"}}})
    assert not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT)
    source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    project = "ats-t16-" + uuid.uuid4().hex[:10]
    report = {"schema_version": 1, "outcome": "failed", "git_sha": source, "evidence_scope": "demo_only", "checks": {},
              "training": "reused clean candidate; no fits/HPO/Registry versions created by this gate", "default_service": "untouched"}
    checks = report["checks"]
    log = (out / "commands.log").open("w")
    def run(command, *, capture=False, timeout=180):
        log.write(json.dumps(command) + "\n");log.flush()
        result = subprocess.run(command, cwd=ROOT, stdout=subprocess.PIPE if capture else log,
                                stderr=log, text=True, timeout=timeout)
        assert result.returncode == 0, "command_failed"
        return result.stdout.strip() if capture else None
    def image(identity):
        return json.loads(run(["docker", "image", "inspect", "--format", '{"id":{{json .Id}},"os":{{json .Os}},"architecture":{{json .Architecture}}}', identity], capture=True))
    def numeric(command, request, output):
        request_path = out / (output + "-request.json");write(request_path, request)
        run(["docker", "run", "--rm", "--name", project + "-numeric", "--network", "none", "--read-only", "--cpus", "1", "--memory", "512m",
             "--user", f"{os.getuid()}:{os.getgid()}", "--tmpfs", "/tmp:rw,size=128m,mode=1777",
             "--mount", f"type=bind,source={out},target={out}", args.monitoring_image,
             command, "--request", str(request_path), "--output", str(out / output)], timeout=180)
    compose = ["docker", "compose", "-p", project, "-f", str(ROOT / "docker/serving/compose.yaml"), "-f", str(out / "limits.json")]
    env = dict(os.environ, ATS_SERVING_IMAGE=args.serving_image, ATS_RELEASE_PACKAGE=str(out / "package"), ATS_SERVING_PORT=str(args.port))
    def compose_run(*command):
        result = subprocess.run([*compose, *command], cwd=ROOT, env=env, stdout=log, stderr=log, timeout=90)
        assert result.returncode == 0, "compose_failed"
    try:
        fit = args.fit.resolve();prepared = fit / "prepared";final = fit / "training/final"
        record = read_json(final / "run_manifest.json")
        report["candidate"] = record["mlflow"]
        report["images"] = {"monitoring": image(args.monitoring_image), "serving": image(args.serving_image), "old_serving": image(args.old_serving_image)}
        if args.reuse_checked:
            previous = args.reuse_checked.resolve()
            reused = read_json(previous / "report.json")
            manifest, _, _ = load_package(previous / "package", previous / "release-check.json")
            assert reused["checks"]["actual_checked_package_api"] is True and reused["checks"]["canonical_train_validation_only_reference"] is True
            assert manifest["serving"]["source_sha256"] == inventory(ROOT / "service")
            assert manifest["serving"]["image"]["id"] == image(args.serving_image)["id"]
            assert manifest["candidate"]["model_sha256"] == record["model"]["sha256"]
            ref_sha = sha((previous / "reference/reference.json").read_bytes())
            load_reference(previous / "reference", ref_sha)
            for name in ("package", "reference"):
                shutil.copytree(previous / name, out / name)
            shutil.copyfile(previous / "release-check.json", out / "release-check.json")
            report["reused"] = {"source_git_sha": reused["git_sha"], "receipt_sha256": sha((previous / "report.json").read_bytes()),
                                "release_check_sha256": sha((previous / "release-check.json").read_bytes()), "reference_sha256": ref_sha,
                                "scope": "passed T12 package/API and isolated canonical reference subgates only; failed batch outcome not credited"}
        else:
            environment = json.loads(run(["docker", "run", "--rm", "--network", "none", "--entrypoint", "/opt/ml-venv/bin/python", args.serving_image,
                "-I", "-B", "-c", "import sys,json;sys.path.insert(0,'/opt/serving');from service.runtime import environment;print(json.dumps(environment()))"], capture=True))
            write(out / "serving-identity.json", {"git_sha": source, "source_sha256": inventory(ROOT / "service"), "image": image(args.serving_image), "environment": environment})
            package_request = {"release_id": project, "gate_report": str(args.gate.resolve()), "run_manifest": str(final / "run_manifest.json"),
                               "pipeline_manifest": str(fit / "evaluation/pipeline_manifest.json"), "serving_identity": str(out / "serving-identity.json")}
            write(out / "package-request.json", package_request)
            # Source is used only by this packaging worker, never the inference process.
            code = "import sys,json;from pathlib import Path;sys.path.insert(0,'/source');from service.package import package;package(json.loads(Path(sys.argv[1]).read_bytes()),Path(sys.argv[2]))"
            run(["docker", "run", "--rm", "--name", project + "-package", "--network", "ats-core_default", "--cpus", "1", "--memory", "1536m",
                 "--user", f"{os.getuid()}:{os.getgid()}", "--workdir", "/tmp", "--tmpfs", "/tmp:rw,size=256m,mode=1777",
                 "-e", "HOME=/tmp", "-e", "USER=ats-package", "-e", "LOGNAME=ats-package", "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000",
                 "--mount", f"type=bind,source={ROOT},target=/source,readonly", "--mount", f"type=bind,source={fit},target={fit},readonly",
                 "--mount", f"type=bind,source={args.gate.resolve()},target={args.gate.resolve()},readonly",
                 "--mount", f"type=bind,source={out},target={out}", "--entrypoint", "/opt/ml-venv/bin/python", record["environment"]["image"]["id"],
                 "-I", "-B", "-c", code, str(out / "package-request.json"), str(out / "package")])
            stage_request = {"package": str(out / "package"), "validation_manifest": str(prepared / "data_manifest.json"),
                             "admission_report": str(prepared / "admission.json"), "dataset_binding": str(fit.parent / "dataset-binding.json"),
                             "alias_other_version": "4"}
            # Caller supplies actual workspace dataset binding, not a manufactured path.
            if not Path(stage_request["dataset_binding"]).exists():
                stage_request["dataset_binding"] = str(args.bundle / "dataset-binding.json")
            write(out / "release-check-request.json", stage_request)
            checked = stage(stage_request, out / "release-check.json")
            assert checked["outcome"] == "passed", checked.get("failed_step")
            checks["actual_checked_package_api"] = all(checked["checks"].values())
            manifest, manifest_sha, _ = release_binding(out / "package")
            reference_request = {"bundle": str(args.bundle.resolve()), "bundle_sha256": read_json(args.bundle / "bundle.json")["sha256"],
                "package": str(out / "package"), "release_check": str(out / "release-check.json"),
                "release_manifest_sha256": manifest_sha, "scope": "engineering_fixture"}
            write(out / "reference-request.json", reference_request)
            reference = build_reference(reference_request, out / "reference")
            reference_sha = sha((out / "reference/reference.json").read_bytes())
            checks["canonical_train_validation_only_reference"] = reference["test_read"] is False and reference["model"]["lineage_status"] == "guarded_schema2"
        checks["actual_checked_package_api"] = True
        checks["canonical_train_validation_only_reference"] = True
        manifest, manifest_sha, _ = release_binding(out / "package")
        reference_sha = sha((out / "reference/reference.json").read_bytes())
        numeric("calibrate", {"reference": str(out / "reference"), "reference_sha256": reference_sha, "config": str(out / "monitoring.yaml")}, "calibration")
        calibration = read_json(out / "calibration/calibration.json")
        checks["real_evidently_json_html_control_shift"] = calibration["outcome"] == "passed" and len(calibration["files"]) >= 6
        compose_run("up", "-d", "--no-build", "--wait", "--wait-timeout", "60")
        container = subprocess.check_output([*compose, "ps", "-q", "ranking"], cwd=ROOT, env=env, text=True).strip()
        for _ in range(60):
            try:
                metadata_request = Request(f"http://127.0.0.1:{args.port}/metadata", data=b"{}", headers={"Content-Type": "application/json"})
                with urlopen(metadata_request, timeout=2) as response:
                    if json.load(response)["ready"]:break
            except Exception:time.sleep(.2)
        else:raise AssertionError("fixture_not_ready")
        rows = [json.loads(line) for line in (args.bundle / "data/validation.jsonl").read_bytes().splitlines()]
        grouped = defaultdict(list)
        for row in rows:grouped[row["vacancy_id"]].append(row)
        request_file = out / "old-replay-request.json"
        start = stamp()
        for index, (vacancy, group) in enumerate(sorted(grouped.items())):
            payload = {"request_id": f"canonical-{index}", "vacancy": {"id": vacancy, "text": group[0]["vacancy_text"]},
                       "candidates": [{"id": row["candidate_id"], "text": row["candidate_text"]} for row in group], "top_k": 3}
            with urlopen(Request(f"http://127.0.0.1:{args.port}/rank", data=canonical(payload), headers={"Content-Type": "application/json"}), timeout=10) as response:
                assert len(json.load(response)["ranking"]) == 3
        payload = {"request_id": "full100", "vacancy": {"id": "private-vacancy", "text": rows[0]["vacancy_text"]},
                   "candidates": [{"id": f"private-{i:03}", "text": "Python private fixture"} for i in range(100)], "top_k": 3}
        write(request_file, payload)
        with urlopen(Request(f"http://127.0.0.1:{args.port}/rank", data=canonical(payload), headers={"Content-Type": "application/json"}), timeout=10) as response:
            original = json.load(response);assert len(original["ranking"]) == 3
        end = stamp()
        # Export executes on Linux against live DB+WAL, never a Desktop file copy.
        volume = json.loads(run(["docker", "inspect", "--format", "{{json .Mounts}}", container], capture=True))
        volume = next(item["Name"] for item in volume if item["Destination"] == "/events")
        code = """import sys,json,hashlib
from pathlib import Path
sys.path.insert(0,'/source/src')
from ats_mlops.monitoring.feedback import export_events
def fingerprint():
 return {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in Path('/events').iterdir() if p.is_file() and (p.name.endswith('.sqlite3') or (p.name.endswith('-wal') and p.stat().st_size))}
before=fingerprint()
manifest=export_events('/events/predictions.sqlite3','/tmp/export',sys.argv[1])
assert before==fingerprint()
print(json.dumps({'manifest':manifest,'events':Path('/tmp/export/events.jsonl').read_text(),'db_nonempty_wal_bytes_unchanged':True}))
"""
        exported_bytes = json.loads(run(["docker", "run", "--rm", "--name", project + "-export", "--network", "none", "--read-only", "--user", "50000:50000",
             "--cpus", "1", "--memory", "512m", "--tmpfs", "/tmp:rw,size=32m,mode=1777",
             "--mount", f"type=volume,source={volume},target=/events", "--mount", f"type=bind,source={ROOT},target=/source,readonly",
             "--entrypoint", "/opt/ml-venv/bin/python", args.old_serving_image,
             "-I", "-B", "-c", code, end], capture=True))
        # mode=ro/query_only guards DB data; native SHM locking needs same UID and
        # writable volume. No directory chmod, immutable shortcut or WAL copying.
        (out / "export").mkdir()
        (out / "export/events.jsonl").write_text(exported_bytes["events"])
        write(out / "export/export.json", exported_bytes["manifest"])
        checks["native_wal_readonly_sql_transaction_db_nonempty_wal_bytes_unchanged"] = exported_bytes["db_nonempty_wal_bytes_unchanged"] is True
        events, exported = load_export(out / "export")
        full = next(event for event in events if event["request_id"] == "full100")
        checks["top3_100_full_scores_writer_witness"] = full["completeness"]["candidate_count"] == len(full["candidates"]) == 100 and exported["full_group"]
        # Frozen old image must replay new nested witness without producing/model load.
        old_code = "import sys,json;from pathlib import Path;sys.path.insert(0,'/opt/serving');from service.events import EventStore;request=json.loads(Path(sys.argv[1]).read_bytes());result=EventStore('/events/predictions.sqlite3').predict(request,lambda _:(_ for _ in ()).throw(AssertionError('produce_forbidden')),{'model_version':'999','release_id':'other'});print(json.dumps(result))"
        replay = json.loads(run(["docker", "run", "--rm", "--network", "none", "--cpus", "1", "--memory", "512m", "--user", "50000:50000",
             "--mount", f"type=volume,source={volume},target=/events", "--mount", f"type=bind,source={request_file},target=/input.json,readonly",
             "--entrypoint", "/opt/ml-venv/bin/python", args.old_serving_image, "-I", "-B", "-c", old_code, "/input.json"], capture=True))
        checks["frozen_old_writer_replays_original_without_produce"] = replay == original
        labels = []
        for event in events:
            for index, candidate in enumerate(event["candidates"]):
                labels.append({"feedback_event_id": event["request_id"] + "-" + str(index), "request_id": event["request_id"],
                    "vacancy_id": event["vacancy_id"], "candidate_id": candidate["candidate_id"], "model_version": event["model_version"],
                    "relevance": int(candidate["candidate_id"] == min(c["candidate_id"] for c in event["candidates"])),
                    "event_time": event["event_time"], "label_source": "independent-fixture-v1", "label_revision": 1})
        # Synthetic judgments use candidate ID only, never model score/rank as truth.
        ledger = out / "feedback.sqlite3"
        admit_feedback(out / "export", [], out / "feedback-empty", ["independent-fixture-v1", "human-review-v1"], ledger=ledger)
        assert assess(events, [], stamp(), ["independent-fixture-v1", "human-review-v1"])["quality"] is None
        store = FeedbackStore(ledger)
        assert store.admit(events, labels[:1], ["independent-fixture-v1", "human-review-v1"]) == 1
        first = store.snapshot()[1];cutoff_before = stamp()
        assert store.admit(events, labels[:1], ["independent-fixture-v1", "human-review-v1"]) == 0
        assert first == store.snapshot()[1]
        assert store.admit(events, labels[1:], ["independent-fixture-v1", "human-review-v1"]) == len(labels)-1
        records = store.snapshot()[1]
        assert assess(events, records, cutoff_before, ["independent-fixture-v1", "human-review-v1"])["quality"] is None
        conflict = dict(labels[0], relevance=1-labels[0]["relevance"])
        try:store.admit(events, [conflict], ["independent-fixture-v1", "human-review-v1"])
        except ValueError:pass
        else:raise AssertionError("conflict_accepted")
        revision = dict(labels[0], feedback_event_id="revision-two", label_revision=2, relevance=1-labels[0]["relevance"])
        assert store.admit(events, [revision], ["independent-fixture-v1", "human-review-v1"]) == 1
        admit_feedback(out / "export", [], out / "feedback-final", ["independent-fixture-v1", "human-review-v1"], ledger=ledger)
        checks["independent_labels_late_duplicate_conflict_revision_cutoff"] = True
        request = {"reference": str(out / "reference"), "reference_sha256": reference_sha, "export": str(out / "export"),
                   "feedback": str(out / "feedback-final"), "calibration": str(out / "calibration"),
                   "calibration_sha256": sha((out / "calibration/calibration.json").read_bytes()), "config": str(out / "monitoring.yaml"),
                   "start": start, "end": end, "cutoff": stamp()}
        numeric("report", request, "window")
        window = read_json(out / "window/window.json")
        checks["actual_window_pair_group_coverage_and_versions"] = window["outcome"] == "passed" and window["counts"]["pairs"] >= 200 and window["counts"]["vacancies"] >= 10 and window["quality"]["quality"] is not None
        report.update(outcome="passed", reference_sha256=reference_sha, release_manifest_sha256=manifest_sha,
            calibration_sha256=request["calibration_sha256"], window_sha256=sha((out / "window/window.json").read_bytes()))
        assert all(value is True for value in checks.values())
    finally:
        subprocess.run([*compose, "down", "--volumes", "--remove-orphans"], cwd=ROOT, env=env, stdout=log, stderr=log, timeout=60)
        for name in (project + "-numeric", project + "-export", project + "-package"):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        write(out / "report.json", report)
        log.close()
    print(json.dumps({"outcome": report["outcome"], "checks": len(checks)}))


if __name__ == "__main__":
    main()
