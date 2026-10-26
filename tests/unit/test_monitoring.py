"""Snapshot completeness, durable independent feedback and null quality."""
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from service.contracts import canonical as service_canonical, sha as service_sha
from service.events import EventStore
from ats_mlops.monitoring.feedback import FEATURES, FeedbackStore, append_labels, canonical, export_events, load_export, sha, validate_event, verified
from ats_mlops.monitoring.quality import assess

TIME = "2026-01-01T00:00:00+00:00"
T1 = "2026-01-02T00:00:00+00:00"
T2 = "2026-01-03T00:00:00+00:00"
SOURCES = ["independent-fixture-v1", "human-review-v1"]


def request(n=2):
    return {"request_id": "r", "vacancy": {"id": "v", "text": "Python"},
            "candidates": [{"id": f"c{i:03}", "text": "Python"} for i in range(n)], "top_k": min(3, n) or 1}


def complete(n):
    return [{"candidate_id": f"c{i:03}", "score": .5, "features": {name: .5 for name in FEATURES}} for i in range(n)]


def event(n=2):
    ids = [item["candidate_id"] for item in complete(n)]
    return {"schema_version": 1, "request_id": "r", "input_hash": "a" * 64, "vacancy_id": "v", "top_k": min(n, 3) or 1,
            "model_version": "4", "release_id": "old-release", "event_time": TIME, "candidates": complete(n),
            "completeness": {"schema_version": 2, "candidate_count": n, "candidate_ids_sha256": sha(canonical(ids))}}


def label(index=0, *, revision=1, relevance=1, source="independent-fixture-v1", feedback_id=None):
    return {"feedback_event_id": feedback_id or f"f-{index}-{revision}", "request_id": "r", "vacancy_id": "v", "candidate_id": f"c{index:03}",
            "model_version": "4", "relevance": relevance, "event_time": TIME, "label_source": source, "label_revision": revision}


class WitnessTests(unittest.TestCase):
    def test_top3_persists100_and_export_witness(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); db = root / "events.sqlite3"
            response = EventStore(db).predict(request(100), lambda _: complete(100), {"model_version": "4", "release_id": "release"})
            self.assertEqual(3, len(response["ranking"]))
            with sqlite3.connect(db) as connection:
                e = json.loads(connection.execute("SELECT event_json FROM prediction_events").fetchone()[0])
            self.assertEqual(1, e["schema_version"])
            self.assertEqual(100, len(e["candidates"]))
            self.assertEqual(100, e["completeness"]["candidate_count"])
            self.assertEqual(service_sha(service_canonical([item["id"] for item in request(100)["candidates"]])), e["completeness"]["candidate_ids_sha256"])
            export_events(db, root / "export", "2099-01-01T00:00:00Z")
            events, metadata = load_export(root / "export")
            self.assertEqual(100, metadata["pairs"])
            self.assertTrue(verified(events[0]))

    def test_typed_witness_replay_corruption_never_produces_or_rewrites(self):
        mutations = [{"candidate_count": True}, {"candidate_count": 1.0}, {"schema_version": 2.0}, {"extra": 1}]
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "db"; store = EventStore(path)
                store.predict(request(1), lambda _: complete(1), {"model_version": "4", "release_id": "release"})
                with sqlite3.connect(path) as db:
                    e = json.loads(db.execute("SELECT event_json FROM prediction_events").fetchone()[0]);e["completeness"].update(mutation)
                    raw = canonical(e).decode();db.execute("UPDATE prediction_events SET event_json=?", (raw,))
                calls = []
                with self.assertRaises(ValueError):
                    store.predict(request(1), lambda _: calls.append(True), {"model_version": "5", "release_id": "new"})
                self.assertEqual([], calls)
                with sqlite3.connect(path) as db:self.assertEqual(raw, db.execute("SELECT event_json FROM prediction_events").fetchone()[0])
                with self.assertRaises(ValueError):validate_event(e)

    def test_incomplete_duplicate_and_extra_producer_fail_before_commit(self):
        for bad in (complete(1), [complete(2)[0]] * 2, complete(3)):
            with self.subTest(bad=bad), tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "db"; store = EventStore(path)
                with self.assertRaises(ValueError):store.predict(request(2), lambda _: bad, {"model_version": "4", "release_id": "release"})
                with sqlite3.connect(path) as db:self.assertEqual(0, db.execute("SELECT count(*) FROM prediction_events").fetchone()[0])

    def test_legacy_cached_replay_unchanged_and_not_quality_eligible(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "db";store = EventStore(path)
            original = store.predict(request(), lambda _: complete(2), {"model_version": "4", "release_id": "release"})
            with sqlite3.connect(path) as db:
                e = json.loads(db.execute("SELECT event_json FROM prediction_events").fetchone()[0]);e.pop("completeness");raw = canonical(e).decode();db.execute("UPDATE prediction_events SET event_json=?", (raw,))
            self.assertFalse(verified(e))
            self.assertEqual(original, store.predict(request(), lambda _: self.fail("producer called"), {"model_version": "5", "release_id": "new"}))
            with sqlite3.connect(path) as db:self.assertEqual(raw, db.execute("SELECT event_json FROM prediction_events").fetchone()[0])
            with self.assertRaisesRegex(ValueError, "legacy_completeness"):
                append_labels([e], [], [label()], T1, SOURCES)

    def test_feature_schema_domains_and_candidate_identity(self):
        values = []
        e = event();e["candidates"][0]["features"] = {"arbitrary": .5};values.append(e)
        e = event();e["candidates"][0]["features"][FEATURES[0]] = True;values.append(e)
        e = event();e["candidates"][0]["features"][FEATURES[0]] = float("nan");values.append(e)
        e = event();e["candidates"][0]["features"][FEATURES[0]] = 1.1;values.append(e)
        e = event();e["candidates"][0]["candidate_id"] = "contact@example.invalid";values.append(e)
        for e in values:
            with self.subTest(event=e), self.assertRaises(ValueError):validate_event(e)


class FeedbackTests(unittest.TestCase):
    def test_first_admission_persists_cold_reimport_and_cutoff(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "feedback.sqlite3";events = [event()]
            store = FeedbackStore(path)
            self.assertEqual(1, store.admit(events, [label()], SOURCES, clock=lambda: datetime.fromisoformat(T1)))
            _, first = store.snapshot()
            self.assertEqual(0, FeedbackStore(path).admit(events, [label()], SOURCES, clock=lambda: datetime.fromisoformat(T2)))
            _, second = FeedbackStore(path).snapshot()
            self.assertEqual(first, second)
            self.assertEqual(T1, second[0]["admitted_at"])
            self.assertIsNone(assess(events, second, TIME, SOURCES)["quality"])
            self.assertEqual(0.5, assess(events, second, T1, SOURCES)["pair_coverage"])

    def test_partial_late_revision_and_original_model_binding(self):
        events = [event()]
        records = append_labels(events, [], [label()], T1, SOURCES)
        result = assess(events, records, T1, SOURCES)
        self.assertIsNone(result["quality"]);self.assertEqual(0., result["group_coverage"])
        records = append_labels(events, records, [label(1, relevance=0)], T2, SOURCES)
        self.assertIsNone(assess(events, records, T1, SOURCES)["quality"])
        self.assertEqual(1., assess(events, records, T2, SOURCES)["quality"]["ndcg_at_10"])
        revision = label(0, revision=2, relevance=0)
        records = append_labels(events, records, [revision], T2, SOURCES)
        self.assertIsNone(assess(events, records, T2, SOURCES)["quality"])
        bad = label();bad["model_version"] = "5"
        with self.assertRaisesRegex(ValueError, "snapshot_mismatch"):append_labels(events, [], [bad], T1, SOURCES)

    def test_atomic_batch_conflict_and_crosssource_revision(self):
        with tempfile.TemporaryDirectory() as folder:
            store = FeedbackStore(Path(folder) / "db");events = [event()]
            store.admit(events, [label()], SOURCES)
            changed = label(relevance=0)
            with self.assertRaisesRegex(ValueError, "feedback_id_conflict"):store.admit(events, [label(1), changed], SOURCES)
            self.assertEqual(1, len(store.snapshot()[1]))
            cross = label(source="human-review-v1", feedback_id="other-source")
            with self.assertRaisesRegex(ValueError, "revision_conflict"):store.admit(events, [cross], SOURCES)

    def test_concurrent_admissions_no_lost_update(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "db";FeedbackStore(path);events = [event()]
            with ThreadPoolExecutor(2) as workers:
                results = list(workers.map(lambda index: FeedbackStore(path).admit(events, [label(index)], SOURCES), [0, 1]))
            self.assertEqual([1, 1], results)
            self.assertEqual(2, len(FeedbackStore(path).snapshot()[1]))

    def test_no_labels_empty_and_no_positives_are_undefined(self):
        for events in ([event()], [event(0)], []):
            self.assertIsNone(assess(events, [], T1, SOURCES)["quality"])
        events = [event()]
        records = append_labels(events, [], [label(0, relevance=0), label(1, relevance=0)], T1, SOURCES)
        result = assess(events, records, T1, SOURCES)
        self.assertEqual(1., result["group_coverage"])
        self.assertIsNone(result["quality"])
        self.assertEqual(1, result["counts"]["groups_no_positives"])
