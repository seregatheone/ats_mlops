"""Portable, train-only TF-IDF and deterministic pair features."""

import re

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.utils.validation import check_is_fitted

from ats_mlops.data.dataset import normalize_text

FEATURE_VERSION = "pair-tfidf-v1"
TEXT_COLUMNS = ("vacancy_text", "candidate_text")
FEATURE_NAMES = ("tfidf_cosine", "token_jaccard", "log1p_vacancy_tokens",
                 "log1p_candidate_tokens", "length_ratio")
FEATURE_CONTRACT = {
    "version": FEATURE_VERSION, "names": list(FEATURE_NAMES), "dtype": "float64",
    "normalization": "NFKC; whitespace collapse; Unicode casefold; Unicode word tokens",
    "tfidf_fit": "both text columns of train rows only; repeated tokens retained",
    "token_counts": "Unicode word occurrences including repetitions",
    "length_ratio": "min(counts)/max(counts); both zero => 0",
    "empty_cosine_and_jaccard": 0,
}


def tokenize(text: str) -> list[str]:
    """The same normalization as the Jaccard baseline, retaining token counts."""
    return re.findall(r"\w+", normalize_text(text).casefold(), flags=re.UNICODE)


def validate_frame(frame: pd.DataFrame) -> None:
    if not isinstance(frame, pd.DataFrame) or list(frame.columns) != list(TEXT_COLUMNS):
        raise ValueError("invalid_text_frame")
    for column in TEXT_COLUMNS:
        if any(not isinstance(text, str) or not normalize_text(text) for text in frame[column]):
            raise ValueError("invalid_pair_text")


class PairFeatures(TransformerMixin, BaseEstimator):
    """Own the fitted vocabulary/IDF; never fit while transforming validation."""

    def __init__(self, max_features=10000, ngram_range=(1, 1)):
        self.max_features = max_features
        self.ngram_range = ngram_range

    def fit(self, X, y=None):
        validate_frame(X)
        if X.empty:
            raise ValueError("empty_training_rows")
        self.vectorizer_ = TfidfVectorizer(
            tokenizer=tokenize, token_pattern=None, lowercase=False,
            norm="l2", max_features=self.max_features, ngram_range=self.ngram_range,
        )
        try:
            self.vectorizer_.fit([*X[TEXT_COLUMNS[0]], *X[TEXT_COLUMNS[1]]])
        except ValueError as error:
            if "empty vocabulary" in str(error):
                raise ValueError("empty_vocabulary") from error
            raise
        self.feature_version_ = FEATURE_VERSION
        self.n_features_in_ = 2
        self.feature_names_in_ = np.asarray(TEXT_COLUMNS, dtype=object)
        return self

    def transform(self, X):
        check_is_fitted(self, "vectorizer_")
        validate_frame(X)
        if X.empty:
            return np.empty((0, len(FEATURE_NAMES)), dtype=np.float64)
        left = self.vectorizer_.transform(X[TEXT_COLUMNS[0]])
        right = self.vectorizer_.transform(X[TEXT_COLUMNS[1]])
        cosine = np.asarray(left.multiply(right).sum(axis=1)).ravel()
        result = []
        for index, (vacancy, candidate) in enumerate(X.itertuples(index=False, name=None)):
            v_tokens, c_tokens = tokenize(vacancy), tokenize(candidate)
            v_set, c_set = set(v_tokens), set(c_tokens)
            union = v_set | c_set
            largest = max(len(v_tokens), len(c_tokens))
            result.append((
                cosine[index], len(v_set & c_set) / len(union) if union else 0.0,
                np.log1p(len(v_tokens)), np.log1p(len(c_tokens)),
                min(len(v_tokens), len(c_tokens)) / largest if largest else 0.0,
            ))
        return np.asarray(result, dtype=np.float64)

    def get_feature_names_out(self, input_features=None):
        return np.asarray(FEATURE_NAMES, dtype=object)
