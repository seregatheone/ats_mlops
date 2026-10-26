"""Actual isolated HTTP/container check; writes an immutable report outside the package."""

import argparse
from collections import defaultdict
from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

from service.contracts import canonical, require, sha
from service.integrity import document, inventory, release_binding
from service.package import exclusive

SENTINEL = "T12_PRIVATE_TEXT_contact@example.invalid"


def command(args, *, timeout=60):
    completed = subprocess.run(args, capture_output=True, timeout=timeout)
    require(completed.returncode == 0, "container_operation_failed")
    return completed.stdout.decode()


def image_identity(image):
    data = json.loads(command(["docker", "image", "inspect", "--format",
                              '{{json .Id}}', image]))
    require(data == image, "image_identity_changed")
    return data


def http(port, route, payload=None, *, raw=None, headers=None):
    body = canonical(payload) if raw is None else raw
    request = urllib.request.Request(f"http://127.0.0.1:{port}{route}", data=body,
                                     headers=headers or {"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def stage(request, output):
    root = Path(request["package"]).resolve()
    manifest, manifest_sha, record = release_binding(root)
    image = manifest["serving"]["image"]["id"]
    image_identity(image)
    require(not Path(output).exists(), "new_release_check_required")
    report = {"schema_version": 1, "outcome": "rejected", "evidence_scope": manifest["evidence_scope"],
              "release_id": manifest["release_id"], "candidate": manifest["candidate"],
              "bento_tag": manifest["bento_tag"], "image_id": image,
              "gate_report_sha256": manifest["gate_report_sha256"],
              "api_schema_sha256": manifest["api_schema_sha256"],
              "release_manifest_sha256": manifest_sha, "checks": {}, "reasons": []}
    name = "ats-t12-" + uuid.uuid4().hex[:12]
    network = name + "-net"
    logs = []
    created_network = False
    current = None
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    try:
        # Host-only T11 imports; they never enter the model worker or serving image.
        from ats_mlops.quality.artifacts import validation_snapshot
        from ats_mlops.quality.isolated import DockerScorer

        snapshot = validation_snapshot(Path(request["validation_manifest"]), Path(request["admission_report"]),
                                       Path(request["dataset_binding"]))
        gate, _ = document(root / "model_gate_report.json")
        require(all(snapshot[key] == gate["validation"][key] for key in
                    ("manifest_sha256", "admission_sha256", "validation_sha256", "split_sha256", "inventory_sha256", "dataset_binding_sha256")))
        artifact = root / manifest["model_relative_path"]
        reference, receipt = DockerScorer()({"record": record, "artifact": artifact}, snapshot["rows"])
        require(receipt["scores_sha256"] == gate["scoring_receipts"]["candidate"]["scores_sha256"])
        report["reference_receipt"] = receipt
        report["checks"]["gate_bound_native_reference"] = True
        grouped = defaultdict(list)
        for row in snapshot["rows"]:
            grouped[row["vacancy_id"]].append(row)
        command(["docker", "network", "create", "--internal", network])
        created_network = True
        with tempfile.TemporaryDirectory(prefix="ats-t12-events-") as directory:
            events = Path(directory) / "events"
            events.mkdir(mode=0o777)
            events.chmod(0o777)
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]

            def stop():
                nonlocal current
                if current:
                    result = subprocess.run(["docker", "logs", current], capture_output=True, timeout=20)
                    logs.append(result.stdout + result.stderr)
                    command(["docker", "rm", "-f", current])
                    current = None

            def start(package=root, *, bad_store=False):
                nonlocal current
                current = name
                args = ["docker", "run", "-d", "--pull=never", "--name", name, "--network", network,
                        "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                        "--cpus", "1", "--memory", "1536m", "--user", "50000:50000",
                        "--tmpfs", "/tmp:rw,nosuid,nodev,uid=50000,gid=50000,size=128m",
                        "--tmpfs", "/runtime:rw,nosuid,nodev,uid=50000,gid=50000,size=128m",
                        "-p", f"127.0.0.1:{port}:3000", "--mount", f"type=bind,source={package},target=/release,readonly",
                        "--mount", f"type=bind,source={package / 'bentoml/models'},target=/runtime/bento/models,readonly",
                        "--mount", f"type=bind,source={events},target=/events"]
                if bad_store:
                    args += ["-e", "ATS_EVENT_DB=/release/forbidden.sqlite3"]
                command([*args, image])
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    try:
                        code, metadata = http(port, "/metadata", {})
                        if code == 200:
                            return metadata
                    except (OSError, ValueError):
                        pass
                    time.sleep(.2)
                raise ValueError("staging_start_failed")

            try:
                metadata = start()
                require(metadata["ready"] is True and metadata["release_manifest_sha256"] == manifest_sha
                        and metadata["model_version"] == manifest["candidate"]["model_version"]
                        and metadata["serving_image_id"] == image)
                report["metadata"] = metadata
                require(metadata["runtime"]["environment"] == manifest["serving"]["environment"])
                require(json.loads(command(["docker", "inspect", "--format", '{{json .Image}}', name])) == image)
                mounts = json.loads(command(["docker", "inspect", "--format", '{{json .Mounts}}', name]))
                require(all(not item["RW"] for item in mounts if item["Destination"] in ("/release", "/runtime/bento/models")))
                report["checks"]["metadata_image_and_readonly_mounts"] = True
                isolated = command(["docker", "exec", name, "/opt/ml-venv/bin/python", "-I", "-B", "-c",
                                    "import importlib.util,os;from pathlib import Path;assert not Path('/opt/project/src').exists();assert importlib.util.find_spec('ats_mlops') is None;assert not any(os.environ.get(k) for k in ('MLFLOW_TRACKING_URI','AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN'));print('isolated')"])
                require(isolated.strip() == "isolated")
                report["checks"]["source_and_credentials_isolation"] = True
                max_difference = 0.
                samples = []
                for index, (vacancy, rows) in enumerate(sorted(grouped.items())):
                    payload = {"request_id": f"validation-{index}", "vacancy": {"id": vacancy, "text": rows[0]["vacancy_text"]},
                               "candidates": [{"id": row["candidate_id"], "text": row["candidate_text"]} for row in rows], "top_k": 100}
                    code, response = http(port, "/rank", payload)
                    require(code == 200 and len(response["ranking"]) == len(rows))
                    expected = sorted(rows, key=lambda row: (-reference[(vacancy, row["candidate_id"])], row["candidate_id"]))
                    require([item["candidate_id"] for item in response["ranking"]] == [row["candidate_id"] for row in expected])
                    for item in response["ranking"]:
                        max_difference = max(max_difference, abs(item["score"] - reference[(vacancy, item["candidate_id"])]))
                    samples.append((payload, response))
                require(max_difference <= 1e-12)
                report["score_comparison"] = {"pairs": len(reference), "groups": len(grouped), "max_abs_difference": max_difference, "atol": 1e-12}
                report["checks"]["http_matches_passed_model"] = True
                payload = {"request_id": "private-full", "vacancy": {"id": "v-private", "text": SENTINEL},
                           "candidates": [{"id": "b", "text": SENTINEL}, {"id": "a", "text": SENTINEL}], "top_k": 1}
                code, original = http(port, "/rank", payload)
                require(code == 200 and original["ranking"][0]["candidate_id"] == "a")
                with sqlite3.connect(events / "predictions.sqlite3") as db:
                    event = json.loads(db.execute("SELECT event_json FROM prediction_events WHERE request_id='private-full'").fetchone()[0])
                    require(len(event["candidates"]) == 2 and all(len(item["features"]) == 5 for item in event["candidates"]))
                require(http(port, "/rank", payload) == (200, original))
                changed = deepcopy(payload)
                changed["top_k"] = 2
                require(http(port, "/rank", changed)[0] == 422)
                empty = deepcopy(payload)
                empty.update(request_id="empty", candidates=[], top_k=100)
                require(http(port, "/rank", empty)[1]["ranking"] == [])
                for value in (True, 0, 101, 1.0, "1"):
                    bad = deepcopy(payload)
                    bad["top_k"] = value
                    require(http(port, "/rank", bad)[0] == 422)
                for text in ("", " \u00a0 "):
                    bad = deepcopy(payload)
                    bad["vacancy"]["text"] = text
                    require(http(port, "/rank", bad)[0] == 422)
                bad = deepcopy(payload)
                bad["candidates"] *= 2
                require(http(port, "/rank", bad)[0] == 422)
                bad["candidates"] = []
                bad["vacancy"]["text"] = "x" * 10001
                require(http(port, "/rank", bad)[0] == 413)
                require(http(port, "/rank", raw=b"x" * 1048577)[0] == 413)
                require(http(port, "/rank", raw=('{"private":"' + SENTINEL).encode())[0] == 422)
                require(http(port, "/rank", raw=b'{"request_id":"one","request_id":"two"}')[0] == 422)
                require(http(port, "/metadata", raw=b"")[0] == 200)
                require(http(port, "/metadata", [SENTINEL])[0] == 422)
                report["checks"]["strict_limits_empty_ties_and_privacy_errors"] = True
                report["checks"]["full_event_before_top_k_and_idempotency"] = True

                # A fixture-only alias points to already existing versions; never champion/candidate.
                alias = "t12_fixture_" + uuid.uuid4().hex[:12]
                alias_request = {"name": manifest["candidate"]["model_name"], "alias": alias,
                                 "from_version": manifest["candidate"]["model_version"], "to_version": request["alias_other_version"]}
                require(alias_request["to_version"] != alias_request["from_version"])
                alias_code = "import json,mlflow,os; x=json.loads(os.environ['ATS_ALIAS_REQUEST']); c=mlflow.MlflowClient(); "
                for target in ("from_version", "to_version"):
                    command(["docker", "run", "--rm", "--pull=never", "--network", "ats-core_default", "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000",
                             "-e", "ATS_ALIAS_REQUEST=" + json.dumps(alias_request), "--entrypoint", "/opt/ml-venv/bin/python",
                             record["environment"]["image"]["id"], "-I", "-B", "-c", alias_code +
                             f"c.set_registered_model_alias(x['name'],x['alias'],x['{target}']); assert c.get_model_version_by_alias(x['name'],x['alias']).version==x['{target}']"])
                    require(http(port, "/rank", payload) == (200, original))
                command(["docker", "run", "--rm", "--pull=never", "--network", "ats-core_default", "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000",
                         "-e", "ATS_ALIAS_REQUEST=" + json.dumps(alias_request), "--entrypoint", "/opt/ml-venv/bin/python",
                         record["environment"]["image"]["id"], "-I", "-B", "-c", alias_code + "c.delete_registered_model_alias(x['name'],x['alias'])"])
                report["checks"]["actual_registry_alias_move_independent"] = True
                stop()
                require(start()["ready"] is True)
                require(http(port, "/rank", payload) == (200, original) and http(port, "/rank", changed)[0] == 422)
                report["checks"]["restart_original_response"] = True
                stop()
                # A missing model still permits replay from the durable prior-release event.
                bad_package = Path(directory) / "missing-model"
                shutil.copytree(root, bad_package)
                (bad_package / manifest["model_relative_path"] / "model.pkl").unlink()
                require(start(bad_package)["ready"] is False)
                require(http(port, "/rank", payload) == (200, original))
                fresh = deepcopy(payload)
                fresh["request_id"] = "missing-model-new"
                require(http(port, "/rank", fresh)[0] == 503)
                report["checks"]["missing_model_503_and_original_release_replay"] = True
                stop()
                require(start(bad_store=True)["ready"] is False)
                require(http(port, "/rank", fresh)[0] == 503)
                report["checks"]["event_write_fail_closed_503"] = True
                stop()
                with sqlite3.connect(events / "predictions.sqlite3") as db:
                    records = db.execute("SELECT event_json,response_json FROM prediction_events ORDER BY request_id").fetchall()
                    require(all(response is not None for _, response in records))
                    report["staging_event_count"] = len(records)
                    report["staging_events_sha256"] = sha(canonical(records))
                require(all(SENTINEL.encode() not in path.read_bytes() for path in events.iterdir() if path.is_file()))
                require(all(SENTINEL.encode() not in log and b"Traceback" not in log for log in logs))
                report["checks"]["no_private_text_in_events_or_logs"] = True
            finally:
                stop()
        _, after, _ = release_binding(root)
        report["release_manifest_sha256_after"] = after
        require(after == manifest_sha)
        report["checks"]["frozen_package_unchanged"] = True
        report["outcome"] = "passed"
    except Exception:
        report["reasons"].append("release_check_failed")
    finally:
        if current:
            subprocess.run(["docker", "rm", "-f", current], capture_output=True, timeout=30)
        if created_network:
            subprocess.run(["docker", "network", "rm", network], capture_output=True, timeout=30)
    exclusive(output, canonical(report))
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = stage(json.loads(args.request.read_text()), args.output)
        print(json.dumps({"outcome": report["outcome"], "checks": report["checks"], "reasons": report["reasons"]}))
        return 0 if report["outcome"] == "passed" else 1
    except Exception:
        print(json.dumps({"outcome": "rejected", "reasons": ["invalid_release_check_request"]}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
