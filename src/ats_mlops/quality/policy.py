"""One cohort, explicit evidence scope, and decimal threshold comparisons."""

from decimal import Decimal
import json
from pathlib import Path

from ats_mlops.data.dataset import canonical_json, sha256
from ats_mlops.evaluation.ranking import evaluate_ranking
from ats_mlops.model.baseline import score_rows as baseline_scores


class GateError(ValueError):
    """Only fixed, text-free reason codes enter public reports."""


def require(condition, reason):
    if not condition:
        raise GateError(reason)


def thresholds(path: Path, *, content: bytes | None = None) -> dict:
    # JSON is a strict YAML subset; the host coordinator needs no YAML dependency.
    document = json.loads(path.read_bytes() if content is None else content)
    require(type(document.get("schema_version")) is int
            and document == {"schema_version": 1, "metric": "ndcg_at_10",
                         "max_ndcg_drop": 0.005, "recall_policy": "report_only"},
            "invalid_threshold_configuration")
    return document


def comparison(candidate: float, reference: float, maximum: float) -> dict:
    # No extra epsilon silently widens the policy at the exact 0.005 boundary.
    delta = Decimal(str(candidate)) - Decimal(str(reference))
    return {"ndcg_delta": float(delta), "ndcg_drop": float(-delta),
            "max_ndcg_drop": maximum, "passed": delta >= -Decimal(str(maximum))}


def _summary(metrics):
    return {key: value for key, value in metrics.items()
            if key not in {"groups", "elapsed_seconds"}}


def assess_scores(rows, groups, candidate_scores, incumbent_scores=None, *,
                  evidence_scope="demo_only", policy=None):
    policy = policy or {"schema_version": 1, "metric": "ndcg_at_10",
                        "max_ndcg_drop": 0.005, "recall_policy": "report_only"}
    require(type(policy.get("schema_version")) is int
            and policy == {"schema_version": 1, "metric": "ndcg_at_10", "max_ndcg_drop": 0.005,
                       "recall_policy": "report_only"}, "invalid_threshold_configuration")
    require(evidence_scope in {"demo_only", "independently_labeled"}, "invalid_evidence_scope")
    result = {"outcome": "rejected", "evidence_scope": evidence_scope,
              "reasons": [], "metrics": {}, "comparisons": {}}
    scores = {"baseline": baseline_scores(rows), "candidate": candidate_scores}
    if incumbent_scores is not None:
        scores["incumbent"] = incumbent_scores
    for name, values in scores.items():
        try:
            metrics = evaluate_ranking(rows, values, group_ids=groups)
            result["metrics"][name] = _summary(metrics)
            if metrics["status"] != "defined":
                result["reasons"].append(name + ".undefined_validation_quality")
        except (ValueError, KeyError, TypeError, OverflowError):
            result["reasons"].append(name + ".invalid_or_incomplete_scores")
    if result["reasons"]:
        return result
    cohort = result["metrics"]["baseline"]["eligible_group_ids"]
    require(all(item["eligible_group_ids"] == cohort for item in result["metrics"].values()),
            "eligible_cohort_mismatch")
    result["cohort_sha256"] = sha256(canonical_json(cohort))
    for reference in ("baseline", "incumbent"):
        if reference not in result["metrics"]:
            continue
        compared = comparison(result["metrics"]["candidate"]["ndcg_at_10"],
                              result["metrics"][reference]["ndcg_at_10"], policy["max_ndcg_drop"])
        result["comparisons"][reference] = compared
        if not compared["passed"]:
            result["reasons"].append("candidate.ndcg_drop_against_" + reference)
    result["outcome"] = "rejected" if result["reasons"] else "passed"
    return result
