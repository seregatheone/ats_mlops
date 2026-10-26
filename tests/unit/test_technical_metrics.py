import asyncio
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ats_mlops.monitoring.technical import collect, pipeline
from ats_mlops.release.state import Store, atomic_json, sha
from service.http_boundary import HTTPBoundary
from service.metrics import HTTPMetrics


SENTINEL = "PRIVATE_request_candidate_contact@example.invalid"


class HTTPMetricTests(unittest.TestCase):
    def test_all_http_responses_count_with_bounded_labels_without_input(self):
        metrics = HTTPMetrics()

        async def call(path, method, content, response=200):
            async def app(scope, receive, send):
                await send({"type": "http.response.start", "status": response, "headers": []})
                await send({"type": "http.response.body", "body": b"{}"})

            async def receive():
                return {"type": "http.request", "body": content}

            messages = []
            async def send(message):
                messages.append(message)
            await HTTPBoundary(app, metrics)({"type": "http", "path": path, "method": method, "headers": [],
                                              "query_string": SENTINEL.encode()}, receive, send)
            return messages

        payload = json.dumps({"request_id": SENTINEL, "vacancy": {"id": "v", "text": SENTINEL},
                              "candidates": [], "top_k": 1}).encode()
        # Invalid ID reaches the boundary's sanitized 422; a valid request can
        # return a controlled SDK503 without revealing its body in any metric.
        asyncio.run(call("/rank", "POST", payload))
        valid = json.dumps({"request_id": "r", "vacancy": {"id": "v", "text": SENTINEL}, "candidates": [], "top_k": 1}).encode()
        asyncio.run(call("/rank", "POST", valid))
        asyncio.run(call("/rank", "POST", valid, 503))
        asyncio.run(call("/" + SENTINEL, SENTINEL, b""))
        result = asyncio.run(call("/metrics", "GET", SENTINEL.encode()))
        self.assertEqual(200, result[0]["status"])
        text = result[1]["body"].decode()
        self.assertNotIn(SENTINEL, text)
        for status in ("200", "422", "503"):
            self.assertIn(f'ats_http_requests_total{{endpoint="/rank",method="POST",status="{status}"}} 1.0', text)
        self.assertIn('ats_http_errors_total{endpoint="/rank",method="POST",status="503"} 1.0', text)
        self.assertIn('ats_http_request_duration_seconds_count{endpoint="/rank",method="POST",status="200"} 1.0', text)
        self.assertIn('endpoint="other",method="other",status="404"', text)
        self.assertNotIn("python_info", text)

    def test_scrape_does_not_enter_business_sdk(self):
        async def check():
            async def forbidden(*args):
                self.fail("metrics invoked SDK/events")
            messages = []
            async def send(message):
                messages.append(message)
            await HTTPBoundary(forbidden, HTTPMetrics())({"type": "http", "path": "/metrics", "method": "GET"}, forbidden, send)
            self.assertEqual(200, messages[0]["status"])
        asyncio.run(check())


class CollectorTests(unittest.TestCase):
    def test_live_wal_counts_are_readonly_and_unknown_labels_are_collapsed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "ledger.sqlite3"
            writer = sqlite3.connect(path)
            self.addCleanup(writer.close)
            writer.execute("PRAGMA journal_mode=WAL")
            writer.executescript("CREATE TABLE requests(status TEXT,attempts INTEGER); CREATE TABLE attempts(phase TEXT,outcome TEXT);")
            writer.execute("INSERT INTO requests VALUES('completed',2)")
            writer.execute("INSERT INTO requests VALUES(?,1)", (SENTINEL,))
            writer.execute("INSERT INTO attempts VALUES('repro','completed')")
            writer.execute("INSERT INTO attempts VALUES(?,?)", (SENTINEL, SENTINEL))
            writer.commit()
            store = Store(root / "release")
            state = store.read()
            state["generation"] = 1
            state["active"] = {"release_id": SENTINEL}
            store.append(state, "active_committed")
            before = {p.name: sha(p.read_bytes()) for p in store.root.iterdir()}
            text = collect(path, store.root).decode()
            self.assertIn('ats_technical_source_up{source="pipeline"} 1', text)
            self.assertIn('ats_technical_source_up{source="release"} 1', text)
            self.assertIn('ats_pipeline_requests{status="completed"} 1', text)
            self.assertIn('ats_pipeline_attempts{phase="repro",outcome="completed"} 1', text)
            self.assertIn("ats_pipeline_retries 1", text)
            self.assertNotIn(SENTINEL, text)
            self.assertEqual(before, {p.name: sha(p.read_bytes()) for p in store.root.iterdir()})
            writer.execute("INSERT INTO requests VALUES('failed',1)")
            writer.commit()
            self.assertEqual(1, pipeline(path)[0]["failed"])
            # Corrupt the final valid JSON payload; no release health/payload
            # series may survive from the previous successful collection.
            journal = store.root / "journal.jsonl"
            event = json.loads(journal.read_text());event["state"]["generation"] = 0
            journal.write_text(json.dumps(event) + "\n")
            failed = collect(path, store.root).decode()
            self.assertIn('ats_technical_source_up{source="release"} 0', failed)
            self.assertNotIn("ats_release_active", failed)

    def test_missing_sources_fail_closed_without_creating_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            text = collect(root / "missing.db", root / "missing").decode()
            self.assertIn('ats_technical_source_up{source="pipeline"} 0', text)
            self.assertIn('ats_technical_source_up{source="release"} 0', text)
            self.assertEqual([], list(root.iterdir()))


if __name__ == "__main__":
    unittest.main()
