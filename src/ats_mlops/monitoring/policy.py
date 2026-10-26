"""Durable retraining decisions and outbox; no provider, training or release I/O.

Typed evidence is an internal boundary. Future adapters must verify the frozen
monitoring/admission/release receipts before constructing it. This module never
treats caller-supplied hashes or ACTIVE check names as external verification.
"""

from contextlib import contextmanager
from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3


VERSION = "retraining-policy-v1"
MIN_PAIRS, MIN_JOBS = 200, 10
COOLDOWN = timedelta(hours=24)
ACTIVE_CHECKS = ("image", "journal", "metadata", "no_pending", "registry")


class PolicyError(ValueError):
    """Only safe fixed reason codes cross the protocol boundary."""


def require(condition, code):
    if not condition:
        raise PolicyError(code)


def identifier(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value), "invalid_identifier")


def fingerprint(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "invalid_fingerprint")


def instant(value):
    require(type(value) is datetime and value.tzinfo is not None and value.utcoffset() is not None, "invalid_clock")
    return value.astimezone(timezone.utc)


def stamp(value):
    return instant(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_stamp(value):
    try:
        return instant(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (AttributeError, TypeError, ValueError):
        raise PolicyError("storage_corrupt") from None


def payload(value):
    if is_dataclass(value):
        return {field.name: payload(getattr(value, field.name)) for field in fields(value)}
    if type(value) is datetime:
        return stamp(value)
    if isinstance(value, (tuple, list)):
        return [payload(item) for item in value]
    if isinstance(value, dict):
        return {key: payload(item) for key, item in value.items()}
    return value


def canonical(value):
    return json.dumps(payload(value), sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def packed(value):
    return canonical({"value": value, "sha256": digest(value)})


def unpacked(raw):
    try:
        item = json.loads(raw)
        require(set(item) == {"value", "sha256"} and digest(item["value"]) == item["sha256"], "storage_corrupt")
        return item["value"]
    except (TypeError, KeyError, ValueError):
        raise PolicyError("storage_corrupt") from None


@dataclass(frozen=True)
class ModelIdentity:
    name: str
    version: str
    run_id: str
    artifact_sha256: str

    def __post_init__(self):
        identifier(self.name)
        identifier(self.run_id)
        require(isinstance(self.version, str) and re.fullmatch(r"[1-9][0-9]*", self.version), "invalid_model_version")
        fingerprint(self.artifact_sha256)


@dataclass(frozen=True)
class Cohort:
    reference_sha256: str
    model: ModelIdentity
    release_id: str
    epoch: int
    config_sha256: str

    def __post_init__(self):
        fingerprint(self.reference_sha256)
        fingerprint(self.config_sha256)
        require(type(self.model) is ModelIdentity, "invalid_model_identity")
        identifier(self.release_id)
        require(type(self.epoch) is int and self.epoch > 0, "invalid_epoch")


@dataclass(frozen=True)
class ReferenceBinding:
    cohort: Cohort
    dataset_version: str
    manifest_sha256: str
    reservation_policy_sha256: str
    labelset_sha256: str

    def __post_init__(self):
        require(type(self.cohort) is Cohort, "invalid_cohort")
        identifier(self.dataset_version)
        for value in (self.manifest_sha256, self.reservation_policy_sha256, self.labelset_sha256):
            fingerprint(value)


@dataclass(frozen=True)
class WindowEvidence:
    window_id: str
    start: datetime
    end: datetime
    cohort: Cohort
    report_sha256: str
    pairs: int
    jobs: int
    drifted: bool | None
    closed: bool = True

    def __post_init__(self):
        identifier(self.window_id)
        require(instant(self.start) < instant(self.end), "invalid_window_range")
        require(type(self.cohort) is Cohort, "invalid_cohort")
        fingerprint(self.report_sha256)
        require(type(self.pairs) is int and type(self.jobs) is int and 0 <= self.jobs <= self.pairs, "invalid_window_counts")
        require(self.drifted is None or type(self.drifted) is bool, "invalid_drift_flag")
        require(type(self.closed) is bool, "invalid_closed_flag")


@dataclass(frozen=True)
class DatasetApproval:
    dataset_version: str
    manifest_schema: int
    manifest_sha256: str
    reservation_policy_sha256: str
    reservation_parent_sha256: str
    bundle_uri: str
    bundle_inventory_sha256: str
    admission_sha256: str
    labelset_sha256: str
    parent_labelset_sha256: str
    new_label_count: int
    label_source: str
    approval_sha256: str
    actor: str
    approved_at: datetime
    approved: bool
    independent: bool

    def __post_init__(self):
        identifier(self.dataset_version)
        identifier(self.actor)
        require(type(self.manifest_schema) is int and self.manifest_schema == 2, "schema2_required")
        for name in ("manifest_sha256", "reservation_policy_sha256", "reservation_parent_sha256",
                     "bundle_inventory_sha256", "admission_sha256", "labelset_sha256",
                     "parent_labelset_sha256", "approval_sha256"):
            fingerprint(getattr(self, name))
        require(isinstance(self.bundle_uri, str) and re.fullmatch(
            r"s3://ats-dvc/orchestration/bundles/[A-Za-z0-9_.-]+/bundle.json", self.bundle_uri), "invalid_bundle_uri")
        require(type(self.new_label_count) is int and self.new_label_count >= 0, "invalid_label_count")
        require(self.label_source in ("independent_feedback", "approved_synthetic_fixture"), "untrusted_label_source")
        instant(self.approved_at)
        require(type(self.approved) is bool and type(self.independent) is bool, "invalid_approval_flags")


@dataclass(frozen=True)
class ExecutionPlan:
    source_git_sha: str
    training_image_id: str
    parameters_json: str
    parameters_sha256: str
    effective_model_sha256: str

    def __post_init__(self):
        require(isinstance(self.source_git_sha, str) and re.fullmatch(r"[0-9a-f]{40}", self.source_git_sha), "invalid_source_identity")
        require(isinstance(self.training_image_id, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", self.training_image_id), "invalid_image_identity")
        fingerprint(self.parameters_sha256)
        fingerprint(self.effective_model_sha256)
        try:
            parameters = json.loads(self.parameters_json)
            require(self.parameters_json == canonical(parameters), "noncanonical_execution_parameters")
            require(set(parameters) == {"model", "search", "smoke", "evaluation"}
                    and parameters["search"] == {"mode": "frozen"} and parameters["smoke"] is False,
                    "frozen_execution_required")
            # Same canonical hash as the existing T10 request protocol.
            raw = json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode()
            require(hashlib.sha256(raw).hexdigest() == self.parameters_sha256
                    and hashlib.sha256((canonical(parameters["model"]) + "\n").encode()).hexdigest() == self.effective_model_sha256,
                    "execution_parameter_hash_mismatch")
        except (TypeError, ValueError, KeyError):
            raise PolicyError("invalid_execution_parameters") from None


@dataclass(frozen=True)
class CandidateEvidence:
    candidate_id: str
    model: ModelIdentity
    gate_sha256: str
    passed: bool
    release_id: str | None = None
    release_manifest_sha256: str | None = None
    release_check_sha256: str | None = None
    image_id: str | None = None

    def __post_init__(self):
        identifier(self.candidate_id)
        require(type(self.model) is ModelIdentity and type(self.passed) is bool, "invalid_candidate")
        fingerprint(self.gate_sha256)
        if self.passed:
            identifier(self.release_id)
            fingerprint(self.release_manifest_sha256)
            fingerprint(self.release_check_sha256)
            require(isinstance(self.image_id, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id), "invalid_image_identity")
        else:
            require(all(value is None for value in (self.release_id, self.release_manifest_sha256,
                                                    self.release_check_sha256, self.image_id)), "rejected_release_binding")


@dataclass(frozen=True)
class VerifiedActive:
    reference: ReferenceBinding
    release_manifest_sha256: str
    release_check_sha256: str
    approval_sha256: str
    journal_sha256: str
    image_id: str
    checks: tuple[str, ...]

    def __post_init__(self):
        require(type(self.reference) is ReferenceBinding, "invalid_reference")
        for value in (self.release_manifest_sha256, self.release_check_sha256, self.approval_sha256, self.journal_sha256):
            fingerprint(value)
        require(isinstance(self.image_id, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id), "invalid_image_identity")
        require(type(self.checks) is tuple and self.checks == ACTIVE_CHECKS, "active_not_verified")


class PolicyStore:
    """One ledger, one inflight request, immutable intents before external effects."""

    def __init__(self, path, initial_reference=None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                version = db.execute("PRAGMA user_version").fetchone()[0]
                if version == 0:
                    require(type(initial_reference) is ReferenceBinding, "initial_reference_required")
                    require(not db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall(), "unsupported_ledger")
                    for statement in (
                        "CREATE TABLE metadata (slot INTEGER PRIMARY KEY CHECK(slot=1), payload TEXT NOT NULL)",
                        "CREATE TABLE windows (id TEXT PRIMARY KEY, digest TEXT NOT NULL, evidence TEXT NOT NULL, decision TEXT NOT NULL)",
                        "CREATE TABLE requests (id TEXT PRIMARY KEY, payload TEXT NOT NULL)",
                        "CREATE TABLE inflight (slot INTEGER PRIMARY KEY CHECK(slot=1), request_id TEXT UNIQUE NOT NULL REFERENCES requests(id))",
                        "CREATE TABLE outbox (id TEXT PRIMARY KEY, request_id TEXT NOT NULL REFERENCES requests(id), kind TEXT NOT NULL, state TEXT NOT NULL, payload TEXT NOT NULL, ack TEXT, UNIQUE(request_id,kind))",
                        "CREATE TABLE transitions (seq INTEGER PRIMARY KEY, request_id TEXT, payload TEXT NOT NULL)",
                    ):
                        db.execute(statement)
                    meta = {"version": VERSION, "reference": payload(initial_reference), "clock": None,
                            "last_window": None, "signal_window": None, "last_accepted_at": None}
                    db.execute("INSERT INTO metadata VALUES(1,?)", (packed(meta),))
                    db.execute("PRAGMA user_version=1")
                else:
                    require(version == 1, "unsupported_ledger")
                    meta = self._meta(db)
                    require(initial_reference is None or meta["reference"] == payload(initial_reference), "initial_reference_conflict")
                db.commit()
            except Exception:
                db.rollback()
                raise

    @contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=FULL")
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self, now):
        now = instant(now)
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                meta = self._meta(db)
                require(meta["clock"] is None or now >= parse_stamp(meta["clock"]), "backward_clock")
                yield db, meta, now
                meta["clock"] = stamp(now)
                db.execute("UPDATE metadata SET payload=? WHERE slot=1", (packed(meta),))
                db.commit()
            except Exception:
                db.rollback()
                raise

    def _meta(self, db):
        row = db.execute("SELECT payload FROM metadata WHERE slot=1").fetchone()
        require(row is not None, "storage_corrupt")
        meta = unpacked(row[0])
        require(meta["version"] == VERSION, "unsupported_ledger")
        return meta

    def _request(self, db, request_id):
        row = db.execute("SELECT payload FROM requests WHERE id=?", (request_id,)).fetchone()
        require(row is not None, "request_unknown")
        value = unpacked(row[0])
        require(value["request_id"] == request_id, "storage_corrupt")
        return value

    def _save(self, db, request, transition, now):
        db.execute("UPDATE requests SET payload=? WHERE id=?", (packed(request), request["request_id"]))
        db.execute("INSERT INTO transitions(request_id,payload) VALUES(?,?)", (
            request["request_id"], packed({"event": transition, "time": stamp(now), "state": request["state"]})))

    def _outbox(self, db, request_id, kind, value):
        event_id = kind + "-" + digest(value)
        record = {"event_id": event_id, "request_id": request_id, "kind": kind, "intent": value}
        db.execute("INSERT INTO outbox VALUES(?,?,?,'pending',?,NULL)", (event_id, request_id, kind, packed(record)))
        return event_id

    def _checked_outbox(self, row):
        record = unpacked(row["payload"])
        require(record["event_id"] == row["id"] == row["kind"] + "-" + digest(record["intent"])
                and record["request_id"] == row["request_id"] and record["kind"] == row["kind"], "storage_corrupt")
        require((row["state"] in ("pending", "cancelled") and row["ack"] is None)
                or (row["state"] == "acknowledged" and row["ack"] is not None), "storage_corrupt")
        if row["ack"] is not None:
            unpacked(row["ack"])
        return record

    def request(self, request_id):
        with self._connection() as db:
            return self._request(db, request_id)

    def snapshot(self):
        with self._connection() as db:
            db.execute("BEGIN")
            meta = self._meta(db)
            row = db.execute("SELECT request_id FROM inflight WHERE slot=1").fetchone()
            return {**meta, "inflight": row[0] if row else None,
                    "windows": db.execute("SELECT count(*) FROM windows").fetchone()[0],
                    "requests": db.execute("SELECT count(*) FROM requests").fetchone()[0]}

    def pending_outbox(self):
        with self._connection() as db:
            return [self._checked_outbox(row) for row in db.execute("SELECT * FROM outbox WHERE state='pending' ORDER BY id")]

    def observe(self, window, *, now):
        require(type(window) is WindowEvidence, "invalid_window_evidence")
        require(window.closed and instant(window.end) <= instant(now), "window_not_closed")
        value, window_hash = payload(window), digest(window)
        with self._transaction(now) as (db, meta, now):
            previous = db.execute("SELECT digest,decision FROM windows WHERE id=?", (window.window_id,)).fetchone()
            if previous:
                require(previous[0] == window_hash, "window_id_conflict")
                return unpacked(previous[1])
            require(value["cohort"] == meta["reference"]["cohort"], "reference_mismatch")
            last = meta["last_window"]
            require(last is None or instant(window.start) >= parse_stamp(last["end"]), "window_overlap_or_out_of_order")
            slot = db.execute("SELECT request_id FROM inflight WHERE slot=1").fetchone()
            cooling = meta["last_accepted_at"] is not None and now < parse_stamp(meta["last_accepted_at"]) + COOLDOWN
            eligible = window.pairs >= MIN_PAIRS and window.jobs >= MIN_JOBS and window.drifted is True
            adjacent = last is not None and value["start"] == last["end"]
            confirmed = eligible and adjacent and meta["signal_window"] is not None
            decision = {"window_id": window.window_id, "request_id": None, "state": "observed", "reason": "no_signal"}
            if slot or cooling:
                decision.update(request_id=slot[0] if slot else None, reason="inflight" if slot else "cooldown")
                meta["signal_window"] = None
            elif confirmed:
                signal = {"policy": VERSION, "cohort": value["cohort"],
                          "windows": [meta["signal_window"], {"window_id": window.window_id, "sha256": window_hash}]}
                request_id = "retrain-" + digest(signal)[:48]
                request = {"request_id": request_id, "run_id": "retrain__" + digest(signal),
                           "signal": signal, "accepted_at": stamp(now), "state": "waiting_for_labels",
                           "incumbent": meta["reference"], "dataset": None, "candidate": None,
                           "failure": None, "reference_intent": None, "execution": None}
                db.execute("INSERT INTO requests VALUES(?,?)", (request_id, packed(request)))
                db.execute("INSERT INTO inflight VALUES(1,?)", (request_id,))
                self._save(db, request, "request_accepted", now)
                meta["last_accepted_at"], meta["signal_window"] = stamp(now), None
                decision.update(request_id=request_id, state="waiting_for_labels", reason="labels_required")
            else:
                meta["signal_window"] = {"window_id": window.window_id, "sha256": window_hash} if eligible else None
                decision["reason"] = "first_signal" if eligible else "insufficient_or_control"
            meta["last_window"] = {"window_id": window.window_id, "end": value["end"]}
            db.execute("INSERT INTO windows VALUES(?,?,?,?)", (window.window_id, window_hash, packed(value), packed(decision)))
            return decision

    def bind_execution(self, request_id, execution, *, now):
        require(type(execution) is ExecutionPlan, "invalid_execution_plan")
        value = payload(execution)
        with self._transaction(now) as (db, meta, now):
            request = self._request(db, request_id)
            if request.get("execution") is not None:
                require(request["execution"] == value, "execution_plan_conflict")
                return request
            require(request["state"] == "waiting_for_labels" and request["dataset"] is None, "execution_binding_too_late")
            request["execution"] = value
            self._save(db, request, "execution_bound", now)
            return request

    def accept_labels(self, request_id, approval, *, now):
        require(type(approval) is DatasetApproval, "invalid_dataset_approval")
        require(instant(approval.approved_at) <= instant(now), "approval_from_future")
        value = payload(approval)
        with self._transaction(now) as (db, meta, now):
            request = self._request(db, request_id)
            if request["dataset"] is not None:
                require(request["dataset"] == value, "approval_conflict")
                return request
            require(request["state"] == "waiting_for_labels", "request_not_waiting")
            require(request["incumbent"] == meta["reference"], "incumbent_changed")
            incumbent = request["incumbent"]
            genuine = (approval.approved and approval.independent and approval.new_label_count > 0
                       and approval.labelset_sha256 != incumbent["labelset_sha256"]
                       and approval.manifest_sha256 != incumbent["manifest_sha256"]
                       and approval.dataset_version != incumbent["dataset_version"])
            if not genuine:
                return request
            require(approval.parent_labelset_sha256 == incumbent["labelset_sha256"], "labelset_parent_mismatch")
            require(approval.reservation_policy_sha256 == incumbent["reservation_policy_sha256"]
                    or approval.reservation_parent_sha256 == incumbent["reservation_policy_sha256"], "reservation_parent_mismatch")
            require(request.get("execution") is not None, "execution_plan_required")
            request["dataset"], request["state"] = value, "queued"
            self._outbox(db, request_id, "candidate", {"request_id": request_id, "run_id": request["run_id"],
                "incumbent": request["incumbent"], "dataset_approval": value, "execution": request["execution"]})
            self._save(db, request, "candidate_intent", now)
            return request

    def ack_dispatch(self, event_id, receipt_sha256, *, now):
        fingerprint(receipt_sha256)
        with self._transaction(now) as (db, meta, now):
            row = db.execute("SELECT * FROM outbox WHERE id=?", (event_id,)).fetchone()
            require(row is not None and row["kind"] == "candidate", "dispatch_unknown")
            self._checked_outbox(row)
            request = self._request(db, row["request_id"])
            if row["ack"] is not None:
                require(unpacked(row["ack"]) == receipt_sha256, "dispatch_receipt_conflict")
                return request
            require(request["state"] in ("queued", "recovery_required"), "request_not_queued")
            db.execute("UPDATE outbox SET state='acknowledged',ack=? WHERE id=?", (packed(receipt_sha256), event_id))
            request["state"] = "candidate_running"
            self._save(db, request, "dispatch_acknowledged", now)
            return request

    def record_candidate(self, request_id, evidence, *, now):
        require(type(evidence) is CandidateEvidence, "invalid_candidate_evidence")
        value = payload(evidence)
        with self._transaction(now) as (db, meta, now):
            request = self._request(db, request_id)
            if request["candidate"] is not None:
                require(request["candidate"] == value, "candidate_result_conflict")
                return request
            require(request["state"] in ("candidate_running", "recovery_required"), "candidate_not_running")
            delivery = db.execute("SELECT * FROM outbox WHERE request_id=? AND kind='candidate'", (request_id,)).fetchone()
            require(delivery is not None and delivery["state"] == "acknowledged", "dispatch_receipt_required")
            self._checked_outbox(delivery)
            require(request["incumbent"] == meta["reference"], "incumbent_changed")
            require(value["model"]["name"] == request["incumbent"]["cohort"]["model"]["name"], "candidate_model_name_mismatch")
            request["candidate"] = value
            request["state"] = "waiting_approval" if evidence.passed else "gate_rejected"
            if not evidence.passed:
                db.execute("DELETE FROM inflight WHERE request_id=?", (request_id,))
            self._save(db, request, "candidate_checked", now)
            return request

    def record_failure(self, request_id, *, ambiguous, now):
        require(type(ambiguous) is bool, "invalid_failure_flag")
        with self._transaction(now) as (db, meta, now):
            request = self._request(db, request_id)
            if request["state"] in ("terminal_failed", "recovery_required") and request["failure"] == {"ambiguous": ambiguous}:
                return request
            require(request["state"] in ("queued", "candidate_running", "recovery_required"), "failure_transition_invalid")
            request["state"] = "recovery_required" if ambiguous else "terminal_failed"
            request["failure"] = {"ambiguous": ambiguous}
            if not ambiguous:
                db.execute("DELETE FROM inflight WHERE request_id=?", (request_id,))
                db.execute("UPDATE outbox SET state='cancelled' WHERE request_id=? AND state='pending'", (request_id,))
            self._save(db, request, "candidate_failure", now)
            return request

    def begin_reference_update(self, request_id, verified, *, now):
        require(type(verified) is VerifiedActive, "active_not_verified")
        value = payload(verified)
        with self._transaction(now) as (db, meta, now):
            request = self._request(db, request_id)
            if request["reference_intent"] is not None:
                require(request["reference_intent"] == value, "reference_intent_conflict")
                return request
            require(request["state"] == "waiting_approval", "candidate_not_waiting_approval")
            candidate, dataset = request["candidate"], request["dataset"]
            reference, incumbent = value["reference"], request["incumbent"]
            require(meta["reference"] == incumbent, "incumbent_changed")
            require(reference["cohort"]["model"] == candidate["model"]
                    and reference["cohort"]["release_id"] == candidate["release_id"]
                    and reference["cohort"]["epoch"] == incumbent["cohort"]["epoch"] + 1
                    and reference["cohort"]["config_sha256"] == incumbent["cohort"]["config_sha256"], "active_target_mismatch")
            require(all(reference[key] == dataset[key] for key in (
                "dataset_version", "manifest_sha256", "reservation_policy_sha256", "labelset_sha256")), "active_dataset_mismatch")
            require(all(value[key] == candidate[key] for key in (
                "release_manifest_sha256", "release_check_sha256", "image_id")), "active_release_binding_mismatch")
            request["reference_intent"], request["state"] = value, "reference_update_pending"
            self._outbox(db, request_id, "reference", value)
            self._save(db, request, "reference_update_intent", now)
            return request

    def reconcile_reference(self, event_id, verified, *, now):
        require(type(verified) is VerifiedActive, "active_not_verified")
        value = payload(verified)
        with self._transaction(now) as (db, meta, now):
            row = db.execute("SELECT * FROM outbox WHERE id=?", (event_id,)).fetchone()
            require(row is not None and row["kind"] == "reference", "reference_intent_unknown")
            record = self._checked_outbox(row)
            request = self._request(db, row["request_id"])
            # Fresh verified journal bytes may differ after harmless later stage
            # entries; the exact active target/dataset/epoch must remain identical.
            require(all(value[key] == record["intent"][key] for key in value if key != "journal_sha256"), "active_target_changed")
            if row["state"] == "acknowledged":
                return request
            require(request["state"] == "reference_update_pending"
                    and meta["reference"] == request["incumbent"], "reference_transition_invalid")
            meta["reference"], meta["signal_window"] = value["reference"], None
            request["state"] = "active"
            db.execute("UPDATE outbox SET state='acknowledged',ack=? WHERE id=?", (packed(value), event_id))
            db.execute("DELETE FROM inflight WHERE request_id=?", (request["request_id"],))
            self._save(db, request, "reference_updated", now)
            return request
