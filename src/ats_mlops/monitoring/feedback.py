"""Immutable full-group exports and append-only independent feedback snapshots."""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import uuid
from urllib.parse import quote

FEATURES = ("tfidf_cosine", "token_jaccard", "log1p_vacancy_tokens", "log1p_candidate_tokens", "length_ratio")
NUMERIC = (*FEATURES, "score")
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
EVENT_KEYS = {"schema_version", "request_id", "input_hash", "vacancy_id", "top_k", "model_version", "release_id", "event_time", "candidates"}
LABEL_KEYS = {"feedback_event_id", "request_id", "vacancy_id", "candidate_id", "model_version", "relevance", "event_time", "label_source", "label_revision"}


def require(value, reason="monitoring_invalid"):
    if not value:
        raise ValueError(reason)


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def decode_json(content):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    return json.loads(content, object_pairs_hook=pairs,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite_json")))


def read_json(path):
    require(not Path(path).is_symlink(), "input_symlink")
    return decode_json(Path(path).read_bytes())


def checked_bytes(root, relative):
    root, relative = Path(root), Path(relative)
    require(not root.is_symlink() and not relative.is_absolute() and relative.parts
            and not any(part in ("..", ".") for part in relative.parts), "invalid_snapshot_path")
    current = root
    for part in relative.parts:
        current = current / part
        require(not current.is_symlink(), "input_symlink")
    require(current.is_file(), "missing_snapshot_file")
    return current.read_bytes()


def utc(value):
    require(isinstance(value, str), "invalid_timestamp")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(stamp.tzinfo is not None and stamp.utcoffset().total_seconds() == 0, "utc_timestamp_required")
    return stamp


def identifier(value):
    return isinstance(value, str) and ID.fullmatch(value) is not None


def version(value):
    return isinstance(value, str) and value.isdecimal() and str(int(value)) == value and int(value) > 0


def numeric(value, *, unit=False):
    return type(value) in (float, int) and math.isfinite(value) and (0 <= value <= 1 if unit else value >= 0)


def validate_event(event):
    require(isinstance(event, dict) and set(event) in (EVENT_KEYS, EVENT_KEYS | {"completeness"}), "invalid_event_fields")
    require(type(event["schema_version"]) is int and event["schema_version"] == 1, "invalid_event_schema")
    require(all(identifier(event[key]) for key in ("request_id", "vacancy_id", "release_id"))
            and version(event["model_version"]), "invalid_event_identity")
    require(isinstance(event["input_hash"], str) and re.fullmatch(r"[a-f0-9]{64}", event["input_hash"]), "invalid_input_hash")
    require(type(event["top_k"]) is int and 1 <= event["top_k"] <= 100, "invalid_top_k")
    utc(event["event_time"])
    candidates = event["candidates"]
    require(isinstance(candidates, list) and len(candidates) <= 100, "invalid_candidates")
    ids = set()
    for item in candidates:
        require(isinstance(item, dict) and set(item) == {"candidate_id", "score", "features"}, "invalid_candidate_fields")
        require(identifier(item["candidate_id"]) and item["candidate_id"] not in ids, "duplicate_or_invalid_candidate")
        ids.add(item["candidate_id"])
        require(numeric(item["score"], unit=True), "invalid_score")
        features = item["features"]
        require(isinstance(features, dict) and set(features) == set(FEATURES), "feature_schema_mismatch")
        require(all(numeric(value, unit=name in ("tfidf_cosine", "token_jaccard", "length_ratio"))
                    for name, value in features.items()), "invalid_features")
    require(candidates == sorted(candidates, key=lambda item: (-item["score"], item["candidate_id"])), "invalid_ranking_order")
    if "completeness" in event:
        witness = event["completeness"]
        require(isinstance(witness, dict) and set(witness) == {"schema_version", "candidate_count", "candidate_ids_sha256"}
                and type(witness["schema_version"]) is int and witness["schema_version"] == 2
                and type(witness["candidate_count"]) is int and witness["candidate_count"] == len(candidates)
                and witness["candidate_ids_sha256"] == sha(canonical(sorted(ids))), "completeness_mismatch")
    return event


def verified(event):
    validate_event(event)
    return "completeness" in event


def validate_events(events):
    require(isinstance(events, list), "invalid_events")
    unique = {}
    for event in events:
        validate_event(event)
        key = event["request_id"]
        require(key not in unique or canonical(unique[key]) == canonical(event), "request_id_conflict")
        unique[key] = event
    return [unique[key] for key in sorted(unique)]


def new_directory(output):
    output = Path(output)
    require(not output.exists(), "immutable_output_exists")
    output.mkdir(parents=True)
    return output


@contextmanager
def publication(output):
    """Exclusive atomic publication; never replace a prior completed snapshot."""
    output = Path(output)
    require(not output.exists() and not output.is_symlink(), "immutable_output_exists")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".monitoring-", dir=output.parent) as directory:
        staging = Path(directory) / "complete"
        staging.mkdir()
        yield staging
        for path in staging.rglob("*"):
            require(not path.is_symlink(), "invalid_output_tree")
            if path.is_file():
                with path.open("rb") as stream:
                    os.fsync(stream.fileno())
        output.mkdir(mode=0o700)  # cross-process exclusive claim
        try:
            os.replace(staging, output)
        except BaseException:
            output.rmdir()
            raise
        descriptor = os.open(output.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def write_document(path, value):
    with Path(path).open("xb") as stream:
        stream.write(canonical(value))


def export_events(database, output, cutoff):
    """Run in Linux on the native volume; mode=ro sees WAL, never immutable=1."""
    limit = utc(cutoff)
    connection = sqlite3.connect("file:" + quote(str(Path(database).resolve())) + "?mode=ro", uri=True, timeout=5)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        rows = connection.execute("SELECT request_id,input_hash,event_json,response_json FROM prediction_events ORDER BY request_id").fetchall()
    finally:
        connection.close()
    events = []
    for request_id, input_hash, raw, response in rows:
        event = validate_event(decode_json(raw))
        require(event["request_id"] == request_id and event["input_hash"] == input_hash and response is not None, "incomplete_event")
        expected = {"request_id": request_id, "model_version": event["model_version"], "release_id": event["release_id"],
                    "ranking": [{"candidate_id": item["candidate_id"], "score": item["score"]}
                                for item in event["candidates"][:event["top_k"]]]}
        require(canonical(decode_json(response)) == canonical(expected), "stored_response_mismatch")
        if utc(event["event_time"]) <= limit:
            events.append(event)
    events = validate_events(events)
    content = b"".join(canonical(item) for item in events)
    manifest = {"schema_version": 1, "cutoff": cutoff, "events_sha256": sha(content), "groups": len(events),
                "pairs": sum(len(item["candidates"]) for item in events), "full_group": all(verified(item) for item in events),
                "verified_groups": sum(verified(item) for item in events),
                "legacy_unverified_groups": sum(not verified(item) for item in events),
                "read_mode": "sqlite mode=ro; BEGIN consistent transaction; committed responses", "contains_text": False}
    with publication(output) as root:
        (root / "events.jsonl").write_bytes(content)
        write_document(root / "export.json", manifest)
    return manifest


def load_export(directory):
    root = Path(directory)
    manifest = decode_json(checked_bytes(root, "export.json"))
    content = checked_bytes(root, "events.jsonl")
    require(set(manifest) == {"schema_version", "cutoff", "events_sha256", "groups", "pairs", "full_group",
            "verified_groups", "legacy_unverified_groups", "read_mode", "contains_text"}
            and type(manifest["schema_version"]) is int and manifest["schema_version"] == 1
            and all(type(manifest[key]) is int and manifest[key] >= 0 for key in ("groups", "pairs", "verified_groups", "legacy_unverified_groups"))
            and manifest["contains_text"] is False and manifest["events_sha256"] == sha(content), "export_hash_mismatch")
    events = validate_events([decode_json(line) for line in content.splitlines()])
    require(len(events) == manifest["groups"] and sum(len(e["candidates"]) for e in events) == manifest["pairs"], "export_counts_mismatch")
    require(all(utc(e["event_time"]) <= utc(manifest["cutoff"]) for e in events), "export_cutoff_mismatch")
    require(manifest["full_group"] is all(verified(e) for e in events)
            and manifest["verified_groups"] == sum(verified(e) for e in events)
            and manifest["legacy_unverified_groups"] == sum(not verified(e) for e in events), "export_completeness_mismatch")
    return events, manifest


def validate_label(label, events, sources):
    require(isinstance(label, dict) and set(label) == LABEL_KEYS, "invalid_feedback_fields")
    require(all(identifier(label[key]) for key in ("feedback_event_id", "request_id", "vacancy_id", "candidate_id")), "invalid_feedback_identity")
    require(type(label["relevance"]) is int and label["relevance"] in (0, 1)
            and type(label["label_revision"]) is int and label["label_revision"] > 0, "invalid_label")
    require(label["label_source"] in sources, "unapproved_label_source")
    utc(label["event_time"])
    event = events.get(label["request_id"])
    require(event is not None and label["vacancy_id"] == event["vacancy_id"] and label["model_version"] == event["model_version"]
            and label["candidate_id"] in {x["candidate_id"] for x in event["candidates"]}, "feedback_snapshot_mismatch")
    require(verified(event), "legacy_completeness_unverified")


def append_labels(events, history, incoming, admitted_at, sources):
    """Pure append-only admission. The first trusted admission time never changes."""
    current = {e["request_id"]: e for e in validate_events(events)}
    stamp = utc(admitted_at)
    records, ids, revisions = [], {}, {}
    for record in history:
        require(set(record) == {"feedback", "admitted_at", "payload_sha256", "prediction_sha256"}, "invalid_feedback_record")
        label = record["feedback"]
        validate_label(label, current, sources)
        require(record["payload_sha256"] == sha(canonical(label)) and utc(label["event_time"]) <= utc(record["admitted_at"]), "feedback_history_invalid")
        require(record["prediction_sha256"] == sha(canonical(current[label["request_id"]])), "feedback_prediction_changed")
        key = label["request_id"], label["candidate_id"], label["model_version"], label["label_revision"]
        require(label["feedback_event_id"] not in ids and key not in revisions, "duplicate_feedback_history")
        ids[label["feedback_event_id"]] = record
        revisions[key] = label["feedback_event_id"]
        records.append(record)
    for label in incoming:
        validate_label(label, current, sources)
        require(utc(label["event_time"]) <= stamp, "future_label")
        previous = ids.get(label["feedback_event_id"])
        if previous:
            require(previous["payload_sha256"] == sha(canonical(label)), "feedback_id_conflict")
            continue
        key = label["request_id"], label["candidate_id"], label["model_version"], label["label_revision"]
        require(key not in revisions, "label_revision_conflict")
        record = {"feedback": label, "admitted_at": admitted_at, "payload_sha256": sha(canonical(label)),
                  "prediction_sha256": sha(canonical(current[label["request_id"]]))}
        ids[label["feedback_event_id"]] = record
        revisions[key] = label["feedback_event_id"]
        records.append(record)
    return records


class FeedbackStore:
    """One native SQLite ledger: transactional IDs/revisions and first trusted clock."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        require(not self.path.is_symlink(), "invalid_feedback_ledger")
        with self.database() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY,value TEXT NOT NULL)")
            db.execute("INSERT OR IGNORE INTO metadata VALUES ('ledger_id',?)", (uuid.uuid4().hex,))
            db.execute("CREATE TABLE IF NOT EXISTS feedback (event_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,candidate_id TEXT NOT NULL,model_version TEXT NOT NULL,revision INTEGER NOT NULL,record_json TEXT NOT NULL,UNIQUE(request_id,candidate_id,model_version,revision))")

    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.execute("PRAGMA synchronous=FULL")
        return db

    @contextmanager
    def database(self):
        db = self.connect()
        try:
            with db:
                yield db
        finally:
            db.close()

    def admit(self, events, incoming, sources, *, clock=None):
        with self.database() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = [decode_json(row[0]) for row in db.execute("SELECT record_json FROM feedback ORDER BY rowid")]
            stamp = (clock or (lambda: datetime.now(timezone.utc)))().isoformat()
            records = append_labels(events, existing, incoming, stamp, sources)
            for record in records[len(existing):]:
                label = record["feedback"]
                db.execute("INSERT INTO feedback VALUES (?,?,?,?,?,?)", (label["feedback_event_id"], label["request_id"], label["candidate_id"], label["model_version"], label["label_revision"], canonical(record).decode()))
            return len(records) - len(existing)

    def snapshot(self):
        with self.database() as db:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            ledger_id = db.execute("SELECT value FROM metadata WHERE key='ledger_id'").fetchone()[0]
            records = [decode_json(row[0]) for row in db.execute("SELECT record_json FROM feedback ORDER BY rowid")]
        return ledger_id, records


def admit_feedback(export, incoming, output, sources, *, ledger):
    require(not Path(output).exists() and not Path(output).is_symlink(), "immutable_output_exists")
    events, event_manifest = load_export(export)
    store = FeedbackStore(ledger)
    store.admit(events, incoming, sources)
    ledger_id, records = store.snapshot()
    content = b"".join(canonical(record) for record in records)
    receipt = {"schema_version": 1, "feedback_sha256": sha(content), "records": len(records),
               "ledger_id": ledger_id, "events_sha256": event_manifest["events_sha256"], "label_sources": sorted(sources),
               "selection": "max integer label_revision with event_time and first admitted_at <= cutoff; equal revision conflicts rejected"}
    with publication(output) as root:
        (root / "feedback.jsonl").write_bytes(content)
        write_document(root / "feedback.json", receipt)
    return receipt


def load_feedback(directory):
    root = Path(directory)
    receipt = decode_json(checked_bytes(root, "feedback.json"))
    content = checked_bytes(root, "feedback.jsonl")
    require(set(receipt) == {"schema_version", "feedback_sha256", "records", "ledger_id", "events_sha256", "label_sources", "selection"}
            and type(receipt["schema_version"]) is int and receipt["schema_version"] == 1
            and type(receipt["records"]) is int and receipt["records"] >= 0
            and isinstance(receipt["ledger_id"], str) and re.fullmatch(r"[0-9a-f]{32}", receipt["ledger_id"])
            and receipt["feedback_sha256"] == sha(content), "feedback_hash_mismatch")
    records = [decode_json(line) for line in content.splitlines()]
    require(len(records) == receipt["records"], "feedback_counts_mismatch")
    return records, receipt
