"""Stdlib read-only Linux ledger/journal collector with bounded labels."""

import argparse
from collections import Counter
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from urllib.parse import quote


STATUSES = ("ready", "in_progress", "retryable", "completed", "failed", "recovery_required", "other")
OUTCOMES = (*STATUSES, "running")
PHASES = ("download", "initialize", "repro", "recovered", "other")
TRANSITIONS = ("staged", "approved", "switch_intent", "previous_stopped", "target_started", "target_ready",
               "alias_intent", "alias_confirmed", "active_committed", "restore_intent", "previous_ready",
               "previous_restored", "previous_restored_after_interruption", "other")


def bounded(value, allowed):
    return value if value in allowed else "other"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def pipeline(path):
    # mode=ro honors the live WAL. Never use immutable=1 or the DDL writer helper.
    connection = sqlite3.connect("file:" + quote(str(Path(path).resolve())) + "?mode=ro", uri=True, timeout=1)
    try:
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        requests = Counter()
        for status, count in connection.execute("SELECT status,count(*) FROM requests GROUP BY status"):
            requests[bounded(status, STATUSES)] += count
        attempts = Counter()
        for phase, outcome, count in connection.execute("SELECT phase,outcome,count(*) FROM attempts GROUP BY phase,outcome"):
            attempts[(bounded(phase, PHASES), bounded(outcome, OUTCOMES))] += count
        retries = connection.execute("SELECT coalesce(sum(max(attempts-1,0)),0) FROM requests").fetchone()[0]
        return requests, attempts, retries
    finally:
        connection.close()


def release(root):
    root = Path(root)
    sequence, previous, state = 0, "0" * 64, None
    transitions = Counter()
    with (root / "journal.jsonl").open("rb") as stream:
        total = 0
        while True:
            line = stream.readline(2 * 1024 * 1024 + 1)
            if not line:
                break
            total += len(line)
            if not line.endswith(b"\n") or len(line) > 2 * 1024 * 1024 or total > 64 * 1024 * 1024:
                raise ValueError("journal_invalid")
            event = json.loads(line)
            payload = dict(event)
            checksum = payload.pop("payload_sha256", None)
            if (checksum != digest(canonical(payload)) or event["sequence"] != sequence + 1
                    or event["previous_sha256"] != previous or event["state"]["sequence"] != event["sequence"]):
                raise ValueError("journal_invalid")
            sequence, previous, state = event["sequence"], digest(line), event["state"]
            transitions[bounded(event["kind"], TRANSITIONS)] += 1
    if state is None or type(state["generation"]) is not int or state["generation"] < 0:
        raise ValueError("journal_invalid")
    cached = json.loads((root / "state.json").read_bytes())
    cache_current = cached.get("sequence") == sequence and cached.get("journal_sha256") == previous
    return state, transitions, cache_current


def metric(lines, name, help_text, samples):
    lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} gauge"))
    for labels, value in samples:
        suffix = "{" + ",".join(f'{key}="{val}"' for key, val in labels.items()) + "}" if labels else ""
        lines.append(f"{name}{suffix} {int(value)}")


def collect(ledger, release_root):
    lines, health = [], {}
    try:
        requests, attempts, retries = pipeline(ledger)
        health["pipeline"] = 1
        metric(lines, "ats_pipeline_requests", "Requests currently in each durable ledger status",
               [({"status": status}, requests[status]) for status in STATUSES])
        metric(lines, "ats_pipeline_attempts", "Ledger attempts by phase and current outcome",
               [({"phase": phase, "outcome": outcome}, attempts[(phase, outcome)]) for phase in PHASES for outcome in OUTCOMES])
        metric(lines, "ats_pipeline_retries", "Persisted attempts beyond the first per request", [({}, retries)])
    except Exception:
        health["pipeline"] = 0
    try:
        state, transitions, current = release(release_root)
        samples = []
        metric(samples, "ats_release_active", "Journal has an active release", [({}, state["active"] is not None)])
        metric(samples, "ats_release_pending", "Release has an unresolved durable intent", [({}, state["pending"] is not None)])
        metric(samples, "ats_release_blocked", "Release intent requires reconciliation", [({}, state["pending"] is not None)])
        metric(samples, "ats_release_generation", "Committed release generation", [({}, state["generation"])])
        metric(samples, "ats_release_cache_current", "Cached state head matches journal head", [({}, current)])
        metric(samples, "ats_release_transitions", "Journal entries by bounded transition kind",
               [({"outcome": outcome}, transitions[outcome]) for outcome in TRANSITIONS])
        lines.extend(samples)
        health["release"] = 1
    except Exception:
        health["release"] = 0
    metric(lines, "ats_technical_source_up", "Read-only source was validated during this scrape",
           [({"source": source}, value) for source, value in health.items()])
    return ("\n".join(lines) + "\n").encode()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=Path("/orchestration/requests.sqlite3"))
    parser.add_argument("--release-root", type=Path, default=Path("/releases"))
    parser.add_argument("--port", type=int, default=9101)
    args = parser.parse_args(argv)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            body = collect(args.ledger, args.release_root)
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
