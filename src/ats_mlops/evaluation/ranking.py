"""Binary NDCG@10 and Recall@10 on one model-independent eligible cohort."""

from collections.abc import Iterable, Mapping
import math
from numbers import Real
from time import perf_counter


CUTOFF = 10


def _identity(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def evaluate_ranking(
    rows: Iterable[Mapping],
    scores: Mapping[tuple[str, str], Real],
    *,
    group_ids: Iterable[str] | None = None,
) -> dict:
    """Evaluate full labelled groups, rejecting partial/extra/nonfinite scores.

    ``group_ids`` is an optional complete inventory, including empty groups.
    Completeness of rows against an external dataset is the caller's boundary:
    use load_partition for admitted data. The same rows/group_ids must be used
    for candidate and incumbent. Zero-positive and empty groups are excluded
    from both macro means; zero-positive counts include empty groups. Fractions
    use all inventoried groups as denominator.
    """
    started = perf_counter()
    groups = {}
    if group_ids is not None:
        for group in group_ids:
            if not _identity(group) or group in groups:
                raise ValueError("invalid_or_duplicate_group_id")
            groups[group] = []
    keys = set()
    for row in rows:
        group, candidate = row["vacancy_id"], row["candidate_id"]
        if not _identity(group) or not _identity(candidate):
            raise ValueError("invalid_pair_id")
        if group_ids is not None and group not in groups:
            raise ValueError("group_outside_inventory")
        key = (group, candidate)
        if key in keys:
            raise ValueError("duplicate_pair")
        keys.add(key)
        label = row["relevance"]
        if type(label) is not int or label not in (0, 1):
            raise ValueError("invalid_relevance")
        groups.setdefault(group, []).append((candidate, label))
    if not isinstance(scores, Mapping) or set(scores) != keys:
        raise ValueError("score_keys_must_match_complete_rows")
    for score in scores.values():
        try:
            valid = not isinstance(score, bool) and isinstance(score, Real) and math.isfinite(score)
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError("scores_must_be_finite_numbers")

    results = []
    eligible = []
    for group, candidates in sorted(groups.items()):
        ranked = sorted(candidates, key=lambda item: (-scores[group, item[0]], item[0]))
        positives = sum(label for _, label in candidates)
        k = min(CUTOFF, len(ranked))
        reason = "empty_group" if not ranked else "no_positive_labels" if not positives else None
        ndcg = recall = None
        if reason is None:
            dcg = math.fsum(label / math.log2(rank + 2) for rank, (_, label) in enumerate(ranked[:k]))
            ideal = math.fsum(1 / math.log2(rank + 2) for rank in range(min(k, positives)))
            ndcg = dcg / ideal
            recall = sum(label for _, label in ranked[:k]) / positives
            eligible.append(group)
        results.append({
            "vacancy_id": group, "candidate_count": len(ranked), "positive_count": positives,
            "k": k, "status": "defined" if reason is None else "undefined", "reason": reason,
            "ndcg_at_10": ndcg, "recall_at_10": recall,
            "ranking": [{"candidate_id": candidate, "score": float(scores[group, candidate])}
                        for candidate, _ in ranked],
        })
    count = len(groups)
    zero_positive = sum(item["positive_count"] == 0 for item in results)
    empty = sum(item["reason"] == "empty_group" for item in results)
    return {
        "status": "defined" if eligible else "undefined", "cutoff": CUTOFF,
        "group_count": count, "eligible_group_count": len(eligible),
        "unevaluated_group_count": count - len(eligible), "eligible_group_ids": eligible,
        "zero_positive_group_count": zero_positive,
        "zero_positive_group_fraction": zero_positive / count if count else 0.0,
        "empty_group_count": empty,
        "ndcg_at_10": math.fsum(item["ndcg_at_10"] for item in results if item["reason"] is None) / len(eligible) if eligible else None,
        "recall_at_10": math.fsum(item["recall_at_10"] for item in results if item["reason"] is None) / len(eligible) if eligible else None,
        "groups": results, "elapsed_seconds": perf_counter() - started,
    }
