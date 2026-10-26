"""Frozen JSON boundary shared by the SDK, packaging and acceptance checks."""

import hashlib
import json
import re
import unicodedata

MAX_BODY = 1048576
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
API_SCHEMA = {"version": 1, "routes": {"/rank": "POST", "/metadata": "POST"},
              "request": ["request_id", "vacancy:{id,text}", "candidates:[{id,text}]", "top_k"],
              "response": ["request_id", "model_version", "release_id", "ranking:[{candidate_id,score}]"],
              "max_bytes": MAX_BODY, "max_candidates": 100, "max_text_characters": 10000,
              "top_k": [1, 100], "id_pattern": ID.pattern, "tie": "candidate_id ascending"}


def canonical(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                       allow_nan=False) + "\n").encode("utf-8")


def sha(content):
    return hashlib.sha256(content).hexdigest()


class BoundaryError(Exception):
    def __init__(self, status=422, code="invalid_request"):
        self.status, self.code = status, code
        super().__init__(code)


def require(condition, code="invalid_artifact"):
    if not condition:
        raise ValueError(code)


def identifier(value):
    return isinstance(value, str) and ID.fullmatch(value) is not None


def rank_request(value):
    if not isinstance(value, dict) or set(value) != {"request_id", "vacancy", "candidates", "top_k"}:
        raise BoundaryError()
    if not identifier(value["request_id"]) or type(value["top_k"]) is not int or not 1 <= value["top_k"] <= 100:
        raise BoundaryError()
    candidates = value["candidates"]
    if not isinstance(candidates, list):
        raise BoundaryError()
    if len(candidates) > 100:
        raise BoundaryError(413, "request_too_large")
    for item in [value["vacancy"], *candidates]:
        if not isinstance(item, dict) or set(item) != {"id", "text"} or not identifier(item["id"]):
            raise BoundaryError()
        text = item["text"]
        if not isinstance(text, str) or not " ".join(unicodedata.normalize("NFKC", text).split()):
            raise BoundaryError()
        try:
            text.encode("utf-8")
        except UnicodeError:
            raise BoundaryError() from None
        if len(text) > 10000:
            raise BoundaryError(413, "request_too_large")
    if len({item["id"] for item in candidates}) != len(candidates):
        raise BoundaryError()
    return value


def input_hash(value):
    return sha(canonical({key: value[key] for key in ("vacancy", "candidates", "top_k")}))


def decode(content):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise BoundaryError()
            result[key] = value
        return result

    def invalid(_):
        raise BoundaryError()

    try:
        return json.loads(content, object_pairs_hook=pairs, parse_constant=invalid)
    except (ValueError, UnicodeError, RecursionError):
        raise BoundaryError() from None
