"""Snapshot-bound partial-label coverage; undefined quality is never zero."""
from collections import Counter

from ats_mlops.evaluation.ranking import evaluate_ranking
from .feedback import append_labels, sha, canonical, utc


def assess(events, records, cutoff, sources, *, selected_events=None):
    records = append_labels(events, records, [], cutoff, sources)
    latest = {}
    end = utc(cutoff)
    for record in records:
        label = record["feedback"]
        if utc(label["event_time"]) > end or utc(record["admitted_at"]) > end:
            continue
        key = label["request_id"], label["candidate_id"], label["model_version"]
        if key not in latest or label["label_revision"] > latest[key]["label_revision"]:
            latest[key] = label
    counts = Counter()
    rows, scores, groups = [], {}, []
    versions = set()
    for event in events if selected_events is None else selected_events:
        versions.add((event["model_version"], event["release_id"]))
        candidates = event["candidates"]
        counts["groups_total"] += 1
        counts["pairs_total"] += len(candidates)
        known = [latest.get((event["request_id"], item["candidate_id"], event["model_version"])) for item in candidates]
        counts["pairs_labeled"] += sum(label is not None for label in known)
        if not candidates:
            counts["groups_empty"] += 1
            continue
        counts["groups_nonempty"] += 1
        if any(label is None for label in known):
            counts["groups_incomplete"] += 1
            continue
        counts["groups_fully_labeled"] += 1
        if not any(label["relevance"] == 1 for label in known):
            counts["groups_no_positives"] += 1
            continue
        counts["groups_eligible"] += 1
        groups.append(event["request_id"])
        for item, label in zip(candidates, known, strict=True):
            row = {"vacancy_id": event["request_id"], "candidate_id": item["candidate_id"], "relevance": label["relevance"]}
            rows.append(row)
            scores[(row["vacancy_id"], row["candidate_id"])] = item["score"]
    metrics = evaluate_ranking(rows, scores, group_ids=groups) if groups else None
    if metrics is not None:
        metrics.pop("elapsed_seconds", None)  # observational timing is not a feedback result
    totals = {key: counts[key] for key in ("groups_total", "groups_empty", "groups_nonempty", "groups_incomplete", "groups_fully_labeled", "groups_no_positives", "groups_eligible", "pairs_total", "pairs_labeled")}
    return {"schema_version": 1, "status": "defined" if groups else "undefined", "quality": metrics,
            "counts": totals, "pair_coverage": counts["pairs_labeled"] / counts["pairs_total"] if counts["pairs_total"] else None,
            "group_coverage": counts["groups_fully_labeled"] / counts["groups_nonempty"] if counts["groups_nonempty"] else None,
            "cohorts": [{"model_version": model, "release_id": release} for model, release in sorted(versions)],
            "cutoff": cutoff, "selected_labels_sha256": sha(canonical([latest[key] for key in sorted(latest)])),
            "grouping": "request_id full snapshot; original vacancy retained by event export", "label_scope": "independent source admission; synthetic fixtures remain demo_only"}
