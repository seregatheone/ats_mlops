"""Stateless token-set Jaccard baseline; no fitted vocabulary or labels."""

from collections.abc import Iterable, Mapping

from ats_mlops.data.dataset import text_tokens


MODEL_VERSION = "jaccard-v1"
TOKENIZER = "NFKC, whitespace collapse, Unicode casefold, Unicode word token sets"


def jaccard_score(vacancy_text: str, candidate_text: str) -> float:
    """Return intersection/union; an empty union has similarity zero."""
    if not isinstance(vacancy_text, str) or not isinstance(candidate_text, str):
        raise ValueError("texts_must_be_strings")
    vacancy, candidate = text_tokens(vacancy_text), text_tokens(candidate_text)
    union = vacancy | candidate
    return len(vacancy & candidate) / len(union) if union else 0.0


def score_rows(rows: Iterable[Mapping]) -> dict[tuple[str, str], float]:
    """Score each complete pair exactly once, keyed independently of row order."""
    scores = {}
    for row in rows:
        key = (row["vacancy_id"], row["candidate_id"])
        if any(not isinstance(value, str) or not value.strip() for value in key):
            raise ValueError("invalid_pair_id")
        if key in scores:
            raise ValueError("duplicate_pair")
        scores[key] = jaccard_score(row["vacancy_text"], row["candidate_text"])
    return scores
