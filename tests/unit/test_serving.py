"""Real SQLite and request-boundary regressions, independent of ML dependencies."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from service.contracts import BoundaryError, MAX_BODY, canonical, decode, rank_request
from service.events import EventStore
from service.http_boundary import HTTPBoundary

REQUEST = {"request_id": "r1", "vacancy": {"id": "v1", "text": "Python"},
           "candidates": [{"id": "c1", "text": "Python"}, {"id": "c2", "text": "SQL"}], "top_k": 1}
IDENTITY = {"model_version": "4", "release_id": "release-a"}


class ServingTests(unittest.TestCase):
    def test_strict_schema(self):
        self.assertEqual(rank_request(deepcopy(REQUEST)), REQUEST)
        for value in (True, 0, 101, 1.0, "1", None):
            request = deepcopy(REQUEST)
            request["top_k"] = value
            with self.assertRaises(BoundaryError):
                rank_request(request)
        for value in ("", " \u00a0 ", None, 1):
            request = deepcopy(REQUEST)
            request["vacancy"]["text"] = value
            with self.assertRaises(BoundaryError):
                rank_request(request)
        request = deepcopy(REQUEST)
        request["candidates"] *= 2
        with self.assertRaises(BoundaryError):
            rank_request(request)
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'not-json', b'\xff'):
            with self.assertRaises(BoundaryError):
                decode(raw)

    def test_size_and_empty(self):
        request = deepcopy(REQUEST)
        request["candidates"] = []
        rank_request(request)
        request["vacancy"]["text"] = "a" * 10000
        rank_request(request)
        request["vacancy"]["text"] += "a"
        with self.assertRaises(BoundaryError) as failure:
            rank_request(request)
        self.assertEqual(failure.exception.status, 413)

    def test_atomic_events_retry_restart_conflict_and_old_release(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.sqlite3"
            store = EventStore(path)
            complete = [{"candidate_id": "c1", "score": .8, "features": {"f": 1.}},
                        {"candidate_id": "c2", "score": .2, "features": {"f": 0.}}]
            result = store.predict(REQUEST, lambda _: complete, IDENTITY)
            self.assertEqual(len(result["ranking"]), 1)
            with sqlite3.connect(path) as db:
                event, response = db.execute("SELECT event_json,response_json FROM prediction_events").fetchone()
                self.assertEqual(len(json.loads(event)["candidates"]), 2)
                self.assertEqual(json.loads(response), result)
                self.assertNotIn("Python", event)
            def forbidden(_):
                self.fail("Replay executed inference")
            replay = EventStore(path).predict(REQUEST, forbidden, {"model_version": "5", "release_id": "new"})
            self.assertEqual(result, replay)
            changed = deepcopy(REQUEST)
            changed["top_k"] = 2
            with self.assertRaises(BoundaryError):
                store.predict(changed, forbidden, IDENTITY)
            def broken(_):
                raise OSError("private-secret")
            changed["request_id"] = "failed"
            with self.assertRaises(OSError):
                store.predict(changed, broken, IDENTITY)
            with sqlite3.connect(path) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM prediction_events").fetchone()[0], 1)

    def test_concurrent_same_id_has_one_event(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.sqlite3"
            EventStore(path)
            calls = []
            def produce(_):
                calls.append(1)
                return []
            def invoke(_):
                return EventStore(path).predict(REQUEST, produce, IDENTITY)
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(invoke, range(8)))
            self.assertEqual(len(calls), 1)
            self.assertTrue(all(item == results[0] for item in results))

    def test_boundary_sanitizes_before_sdk_and_counts_actual_chunks(self):
        async def check(body, status):
            messages = []
            async def forbidden(*_):
                self.fail("Invalid input reached SDK")
            chunks = iter(({"type": "http.request", "body": body[:10], "more_body": True},
                           {"type": "http.request", "body": body[10:], "more_body": False}))
            async def receive():
                return next(chunks)
            async def send(value):
                messages.append(value)
            await HTTPBoundary(forbidden)({"type": "http", "path": "/rank", "method": "POST", "headers": []}, receive, send)
            self.assertEqual(messages[0]["status"], status)
            self.assertNotIn(b"private-secret", messages[1]["body"])
        asyncio.run(check(b'{"private-secret":', 422))
        asyncio.run(check(b"private-secret" + b"a" * MAX_BODY, 413))


if __name__ == "__main__":
    unittest.main()
