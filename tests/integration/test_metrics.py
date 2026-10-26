"""Real Prometheus/Grafana/HTTP checks; no training and no working-demo events."""

import json
import os
from pathlib import Path
import re
import subprocess
import time
import unittest
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ats_mlops.release.runtime import docker
from ats_mlops.release.state import atomic_json, sha


SENTINEL = "T15_PRIVATE_TEXT_contact@example.invalid"


def http(url, payload=None):
    request = Request(url, data=None if payload is None else json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=5) as response:
            return response.status, response.read()
    except HTTPError as error:
        try:
            return error.code, error.read()
        finally:
            error.close()


def query(expression, **kwargs):
    status, raw = http("http://127.0.0.1:9090/api/v1/query?" + urlencode(dict(query=expression, **kwargs)))
    if status != 200:
        raise ValueError("prometheus_query_failed")
    result = json.loads(raw)
    if result["status"] != "success":
        raise ValueError("prometheus_query_failed")
    return result["data"]["result"]


def wait_for(function, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = function()
            if value:
                return value
        except Exception:
            pass
        time.sleep(.5)
    raise AssertionError("metrics_wait_timeout")


def sample(text, metric, status):
    pattern = re.compile(r"^" + re.escape(metric) + r'\{endpoint="/rank",method="POST",status="' + status + r'"\} ([0-9.e+]+)$', re.M)
    match = pattern.search(text)
    return 0. if match is None else float(match[1])


@unittest.skipUnless(os.environ.get("ATS_RUN_METRICS") == "1", "requires pinned metrics package and actual monitoring stack")
class MetricsIntegrationTests(unittest.TestCase):
    def test_actual_metrics_and_dashboard_persistence(self):
        request = json.loads(Path(os.environ["ATS_METRICS_REQUEST"]).read_text())
        output = Path(os.environ["ATS_METRICS_EVIDENCE"])
        output.mkdir(parents=True, exist_ok=False)
        package = Path(request["package"]).resolve()
        image = request["image_id"]
        name, volume, port = "ats-t15-metrics-proof", "ats-t15-metrics-proof-events", 3016
        targets = Path(request["monitoring_state"]) / "targets/acceptance.json"
        report = {"schema_version": 1, "outcome": "failed", "checks": {}, "queries": {},
                  "executable_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                  "image_id": image, "manifest_sha256": sha((package / "release_manifest.json").read_bytes()),
                  "no_fit_or_registration": True, "default_demo_prediction_events_written": False}

        def launch(fault=False):
            existing = docker("ps", "-aq", "--filter", "name=^/" + name + "$")
            if existing:
                owner = docker("inspect", "--format", '{{index .Config.Labels "ats.metrics.proof"}}', existing)
                self.assertEqual(str(output), owner)
                docker("rm", "-f", existing)
            arguments = ["run", "-d", "--pull=never", "--name", name,
                         "--label", "ats.metrics.proof=" + str(output), "--network", "ats-serving-network",
                         "--network-alias", "metrics-acceptance", "--user", "50000:50000", "--read-only",
                         "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true", "--cpus", "1", "--memory", "1536m",
                         "--tmpfs", "/tmp:uid=50000,gid=50000", "--tmpfs", "/runtime:uid=50000,gid=50000",
                         "-p", f"127.0.0.1:{port}:3000", "--mount", f"type=bind,src={package},dst=/release,readonly",
                         "--mount", f"type=bind,src={package}/bentoml/models,dst=/models,readonly",
                         "--mount", f"type=volume,src={volume},dst=/events"]
            if fault:
                arguments += ["-e", "ATS_EVENT_DB=/release/forbidden.sqlite3"]
            container = docker(*arguments, image)
            wait_for(lambda: http(f"http://127.0.0.1:{port}/metrics")[0] == 200)
            return container

        def value(expression):
            result = query(expression)
            return float(result[0]["value"][1]) if result else None

        def metrics():
            status, raw = http(f"http://127.0.0.1:{port}/metrics")
            self.assertEqual(200, status)
            return raw.decode()

        def count():
            code = "import sqlite3;print(sqlite3.connect('file:/events/predictions.sqlite3?mode=ro',uri=True).execute('select count(*) from prediction_events').fetchone()[0])"
            return int(docker("exec", name, "/opt/ml-venv/bin/python", "-I", "-B", "-c", code))

        def payload(index):
            return {"request_id": "t15-proof-" + str(index), "vacancy": {"id": "v", "text": SENTINEL},
                    "candidates": [{"id": "c", "text": SENTINEL}], "top_k": 1}

        created = False
        try:
            self.assertFalse(docker("volume", "ls", "-q", "--filter", "name=^" + volume + "$"), "fresh evidence/volume required")
            docker("volume", "create", "--label", "ats.metrics.proof=" + str(output), volume)
            created = True
            docker("run", "--rm", "--network", "none", "--user", "0:0", "--entrypoint", "/bin/sh",
                   "--mount", f"type=volume,src={volume},dst=/events", image, "-c", "chown 50000:50000 /events && chmod 700 /events")
            container = launch()
            targets.write_text(json.dumps([{"targets": ["metrics-acceptance:3000"]}]) + "\n")
            self.assertEqual(200, http(f"http://127.0.0.1:{port}/rank", payload(0))[0])
            expression = 'ats_http_requests_total{job="acceptance",endpoint="/rank",status="200"}'
            wait_for(lambda: value(expression) == 1)
            before = metrics()
            for index in range(1, 5):
                self.assertEqual(200, http(f"http://127.0.0.1:{port}/rank", payload(index))[0])
            self.assertEqual(200, http(f"http://127.0.0.1:{port}/rank", payload(0))[0])
            self.assertEqual(422, http(f"http://127.0.0.1:{port}/rank", {"private": SENTINEL})[0])
            self.assertEqual(404, http(f"http://127.0.0.1:{port}/" + "PRIVATE_ID")[0])
            after = metrics()
            self.assertEqual(5, sample(after, "ats_http_requests_total", "200") - sample(before, "ats_http_requests_total", "200"))
            self.assertEqual(1, sample(after, "ats_http_errors_total", "422"))
            self.assertEqual(6, sample(after, "ats_http_request_duration_seconds_count", "200"))
            events = count();metrics();metrics();self.assertEqual(events, count());self.assertEqual(5, events)
            report["checks"]["actual_200_422_latency_counter_deltas"] = True
            report["checks"]["scrapes_and_replay_create_no_extra_events"] = True
            self.assertNotIn(SENTINEL, after)
            self.assertNotIn("t15-proof-", after)
            (output / "healthy.prom").write_text(after)
            wait_for(lambda: value(expression) == 6)
            report["queries"]["rank200"] = query(expression)
            report["queries"]["latency"] = query('ats_http_request_duration_seconds_count{job="acceptance",endpoint="/rank",status="200"}')
            for source in ("pipeline", "release"):
                wait_for(lambda: value('ats_technical_source_up{source="' + source + '"}') == 1)
            report["queries"]["pipeline"] = query("ats_pipeline_requests")
            report["queries"]["release"] = query("ats_release_active")
            self.assertTrue(any(float(item["value"][1]) > 0 for item in report["queries"]["pipeline"]))
            self.assertEqual(1, float(report["queries"]["release"][0]["value"][1]))
            report["checks"]["real_pipeline_and_release_sources_scraped"] = True
            launch(fault=True)
            self.assertEqual(503, http(f"http://127.0.0.1:{port}/rank", payload(90))[0])
            fault_expression = 'ats_http_errors_total{job="acceptance",endpoint="/rank",status="503"}'
            wait_for(lambda: value(fault_expression) == 1)
            for index in range(91, 94):
                self.assertEqual(503, http(f"http://127.0.0.1:{port}/rank", payload(index))[0])
            fault = metrics()
            self.assertEqual(4, sample(fault, "ats_http_errors_total", "503"))
            self.assertNotIn(SENTINEL, fault)
            (output / "fault.prom").write_text(fault)
            wait_for(lambda: value(fault_expression) == 4)
            report["queries"]["error503"] = query(fault_expression)
            report["checks"]["actual_controlled503_scraped"] = True
            historical_time = str(time.time())
            historical = query(fault_expression, time=historical_time)
            status, raw = http("http://127.0.0.1:3001/api/dashboards/uid/ats-technical")
            self.assertEqual(200, status)
            dashboard = json.loads(raw)
            self.assertEqual(8, len(dashboard["dashboard"]["panels"]))
            subprocess.run(["python3", "monitoring/bootstrap.py", "restart"], check=True, capture_output=True)
            wait_for(lambda: http("http://127.0.0.1:3001/api/health")[0] == 200)
            wait_for(lambda: bool(query(fault_expression, time=historical_time)))
            self.assertEqual(historical, query(fault_expression, time=historical_time))
            status, raw = http("http://127.0.0.1:3001/api/dashboards/uid/ats-technical")
            after_dashboard = json.loads(raw)
            self.assertEqual(dashboard["dashboard"]["uid"], after_dashboard["dashboard"]["uid"])
            self.assertEqual(dashboard["dashboard"]["panels"], after_dashboard["dashboard"]["panels"])
            self.assertTrue(after_dashboard["meta"]["provisioned"])
            self.assertNotIn(SENTINEL, raw.decode())
            report["checks"]["dashboard_and_tsdb_survive_restart"] = True
            label_status, label_raw = http("http://127.0.0.1:9090/api/v1/series?" + urlencode({"match[]": "ats_http_requests_total"}))
            self.assertEqual(200, label_status)
            series = json.loads(label_raw)["data"]
            self.assertTrue(series)
            for labels in series:
                self.assertLessEqual(set(labels), {"__name__", "endpoint", "method", "status", "job", "instance"})
                self.assertIn(labels["endpoint"], ("/rank", "/metadata", "/metrics", "other"))
                self.assertNotIn(SENTINEL, json.dumps(labels))
                self.assertNotIn("t15-proof-", json.dumps(labels))
            report["checks"]["metrics_tsdb_dashboard_have_no_private_ids_or_text"] = True
            report["series"] = series
            container = launch()
            for index in range(100, 110):
                self.assertEqual(200, http(f"http://127.0.0.1:{port}/rank", payload(index))[0])
            wait_for(lambda: value(expression) == 10)
            report["ui_load_successes"] = 10
            report["fixture_container"] = container
            report["prediction_events"] = count()
            report["oom_killed"] = json.loads(docker("inspect", "--format", '{{json .State.OOMKilled}}', container))
            report["restarts"] = int(docker("inspect", "--format", '{{.RestartCount}}', container))
            self.assertIs(report["oom_killed"], False)
            self.assertEqual(0, report["restarts"])
            report["outcome"] = "passed"
        finally:
            report["fixture_preserved_for_UI"] = created
            atomic_json(output / "report.json", report)
