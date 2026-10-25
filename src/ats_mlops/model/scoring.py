"""Small shared scoring contract for native Pipeline, gates and serving."""

import numpy as np
import pandas as pd

from ats_mlops.features.pairs import TEXT_COLUMNS, validate_frame


def pair_frame(rows) -> pd.DataFrame:
    try:
        frame = pd.DataFrame([{name: row[name] for name in TEXT_COLUMNS} for row in rows],
                             columns=list(TEXT_COLUMNS))
    except (KeyError, TypeError) as error:
        raise ValueError("invalid_pair_text") from error
    validate_frame(frame)
    return frame


def positive_scores(probabilities, classes, *, expected_rows=None) -> np.ndarray:
    """Extract class 1 only after enforcing the frozen binary probability API."""
    if list(classes) != [0, 1]:
        raise ValueError("invalid_model_classes")
    try:
        values = np.asarray(probabilities, dtype=np.float64)
    except (TypeError, ValueError) as error:
        raise ValueError("invalid_probability_values") from error
    if (values.ndim != 2 or values.shape[1] != 2
            or (expected_rows is not None and values.shape[0] != expected_rows)):
        raise ValueError("invalid_probability_shape")
    if (not np.isfinite(values).all() or (values < 0).any() or (values > 1).any()
            or not np.allclose(values.sum(axis=1), 1.0, rtol=0, atol=1e-12)):
        raise ValueError("invalid_probability_values")
    return values[:, 1]


def score_rows(model, rows) -> dict[tuple[str, str], float]:
    """Keep IDs out of the model, reject duplicate/missing identities."""
    rows = list(rows)
    keys = []
    for row in rows:
        try:
            key = (row["vacancy_id"], row["candidate_id"])
        except (KeyError, TypeError) as error:
            raise ValueError("invalid_pair_id") from error
        if any(not isinstance(value, str) or not value.strip() for value in key):
            raise ValueError("invalid_pair_id")
        keys.append(key)
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate_pair")
    if not rows:
        return {}
    values = positive_scores(model.predict_proba(pair_frame(rows)), model.classes_, expected_rows=len(rows))
    return dict(zip(keys, map(float, values), strict=True))
