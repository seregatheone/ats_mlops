"""Exclusive operator lock and fsynced, hash chained source-of-truth journal."""

from contextlib import contextmanager
from copy import deepcopy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import time


class ReleaseError(Exception):
    """Sanitized public failure code."""


def require(condition, code):
    if not condition:
        raise ReleaseError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def sha(value):
    return hashlib.sha256(value).hexdigest()


def fsync_dir(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as stream:
        stream.write(canonical(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    fsync_dir(path.parent)


def immutable_json(path, value):
    path = Path(path)
    with path.open("xb") as stream:
        stream.write(canonical(value) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    fsync_dir(path.parent)
    return sha(path.read_bytes())


class Store:
    def __init__(self, root):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)

    @contextmanager
    def lock(self):
        with (self.root / "operator.lock").open("a+") as stream:
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ReleaseError("operator_busy") from exc
            try:
                yield
            finally:
                fcntl.flock(stream, fcntl.LOCK_UN)

    def read(self):
        state = {"schema_version": 1, "generation": 0, "active": None, "previous": [],
                 "successful": [], "staged": {}, "pending": None, "sequence": 0,
                 "journal_sha256": "0" * 64}
        journal = self.root / "journal.jsonl"
        if not journal.exists():
            require(not (self.root / "state.json").exists(), "journal_missing")
            return state
        try:
            for line in journal.read_bytes().splitlines(keepends=True):
                require(line.endswith(b"\n"), "journal_corrupt")
                event = json.loads(line)
                payload = dict(event)
                digest = payload.pop("payload_sha256", None)
                require(digest == sha(canonical(payload)), "journal_corrupt")
                require(event["sequence"] == state["sequence"] + 1
                        and event["previous_sha256"] == state["journal_sha256"], "journal_corrupt")
                state = event["state"]
                require(state["sequence"] == event["sequence"], "journal_corrupt")
                state["journal_sha256"] = sha(line)
        except ReleaseError:
            raise
        except Exception as exc:
            raise ReleaseError("journal_corrupt") from exc
        return state

    def append(self, state, kind, detail=None):
        projected = deepcopy(state)
        previous = projected.pop("journal_sha256")
        projected["sequence"] += 1
        event = {"schema_version": 1, "sequence": projected["sequence"], "previous_sha256": previous,
                 "timestamp_ns": time.time_ns(), "kind": kind, "detail": detail or {}, "state": projected}
        event["payload_sha256"] = sha(canonical(event))
        line = canonical(event) + b"\n"
        with (self.root / "journal.jsonl").open("ab") as stream:
            stream.write(line)
            stream.flush()
            os.fsync(stream.fileno())
        fsync_dir(self.root)
        projected["journal_sha256"] = sha(line)
        atomic_json(self.root / "state.json", projected)
        state.clear()
        state.update(projected)
        return state
