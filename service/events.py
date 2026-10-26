"""Atomic durable full-group events and original-response retries; no text storage."""

from datetime import datetime, timezone
import json
import math
from pathlib import Path
import sqlite3

from service.contracts import BoundaryError, canonical, identifier, input_hash, require


class EventStore:
    def __init__(self, path, *, timeout=5):
        self.path, self.timeout = str(path), timeout
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS prediction_events (request_id TEXT PRIMARY KEY NOT NULL, "
                       "input_hash TEXT NOT NULL, event_json TEXT NOT NULL, response_json TEXT)")

    def connect(self):
        db = sqlite3.connect(self.path, timeout=self.timeout, isolation_level=None)
        db.execute("PRAGMA synchronous=FULL")
        return db

    def predict(self, request, produce, identity):
        digest = input_hash(request)
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT input_hash,event_json,response_json FROM prediction_events WHERE request_id=?",
                             (request["request_id"],)).fetchone()
            if row:
                if row[0] != digest:
                    raise BoundaryError(422, "request_id_conflict")
                event, result = json.loads(row[1]), json.loads(row[2])
                require(event["schema_version"] == 1 and event["request_id"] == request["request_id"]
                        and event["input_hash"] == digest and event["vacancy_id"] == request["vacancy"]["id"]
                        and event["top_k"] == request["top_k"] and identifier(event["release_id"])
                        and event["model_version"].isdecimal() and int(event["model_version"]) > 0,
                        "stored_event_invalid")
                complete = event["candidates"]
                require(len(complete) == len(request["candidates"])
                        and {item["candidate_id"] for item in complete} == {item["id"] for item in request["candidates"]},
                        "stored_event_invalid")
                for item in complete:
                    require(type(item["score"]) in (float, int) and math.isfinite(item["score"])
                            and 0 <= item["score"] <= 1 and isinstance(item["features"], dict)
                            and all(type(value) in (int, float) and math.isfinite(value) for value in item["features"].values()),
                            "stored_event_invalid")
                require(complete == sorted(complete, key=lambda item: (-item["score"], item["candidate_id"])))
                expected = {"request_id": event["request_id"], "model_version": event["model_version"],
                            "release_id": event["release_id"],
                            "ranking": [{"candidate_id": item["candidate_id"], "score": item["score"]}
                                        for item in complete[:event["top_k"]]]}
                require(result == expected, "stored_response_invalid")
                db.commit()
                return result
            complete = produce(request)
            event = {"schema_version": 1, "request_id": request["request_id"], "input_hash": digest,
                     "vacancy_id": request["vacancy"]["id"], "top_k": request["top_k"], **identity,
                     "event_time": datetime.now(timezone.utc).isoformat(), "candidates": complete}
            # No transaction reservation is ever committed. The full event precedes top_k.
            db.execute("INSERT INTO prediction_events VALUES (?,?,?,NULL)",
                       (request["request_id"], digest, canonical(event).decode()))
            response = {"request_id": request["request_id"], **identity,
                        "ranking": [{"candidate_id": item["candidate_id"], "score": item["score"]}
                                    for item in complete[:request["top_k"]]]}
            db.execute("UPDATE prediction_events SET response_json=? WHERE request_id=?",
                       (canonical(response).decode(), request["request_id"]))
            db.commit()
            return response
        finally:
            if db.in_transaction:
                db.rollback()
            db.close()
