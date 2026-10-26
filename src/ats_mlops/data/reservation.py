"""Immutable semantic fold reservations, independent of entity IDs and labels."""

from collections import Counter
from importlib.resources import files
import json
import re

from .dataset import DataError, SPLITS, canonical_json, pair_id, sha256, text_tokens

ROOT_POLICY_SHA256 = "4ca6c42155d3e34e1436596381838bf31cc36339986c12b91278d760df5d265b"
LEGACY_GENERATOR_SHA256 = "4c86731bffef1a480220c702665cc4f90be79fc2687ccfe32058f2b62b08e105"
ROW_GENERATOR_SHA256 = "8d363ace727035a88e0b5b769717faeb3a5f6a3a7211745d4ef62de626653ff7"
POLICY_KEYS = {"schema_version", "policy_id", "normalization", "threshold", "anchor",
               "parent", "parent_sha256", "documents"}


def require(condition, code="reservation_policy_invalid"):
    if not condition:
        raise DataError(code)


def token_hashes(text):
    return frozenset(sha256(token.encode("utf-8")) for token in text_tokens(text))


def fingerprint(tokens):
    return sha256(canonical_json(sorted(tokens)))


def root_policy():
    content = files("ats_mlops.data").joinpath("reservations/canonical-v1.json").read_bytes()
    require(sha256(content) == ROOT_POLICY_SHA256)
    return json.loads(content)


class Registry:
    """Read-only, explicitly versioned registry; no mutable global reservations."""

    def __init__(self, policy):
        self.policy = policy
        self.documents = {}
        self._load(policy, 0)
        self.exact = {tokens: fold for tokens, fold in self.documents.values()}

    def _load(self, policy, depth):
        require(depth < 20 and isinstance(policy, dict) and set(policy) == POLICY_KEYS)
        require(type(policy["schema_version"]) is int and policy["schema_version"] == 1)
        require(isinstance(policy["policy_id"], str) and 0 < len(policy["policy_id"]) <= 128)
        parent = policy["parent"]
        if parent is None:
            require(sha256(canonical_json(policy)) == ROOT_POLICY_SHA256)
            self.threshold = policy["threshold"]
            self.normalization = policy["normalization"]
        else:
            require(policy["parent_sha256"] == sha256(canonical_json(parent)))
            self._load(parent, depth + 1)
            require(policy["anchor"] is None and policy["threshold"] == self.threshold
                    and policy["normalization"] == self.normalization)
        require(isinstance(policy["documents"], list))
        previous = ""
        additions = []
        for entry in policy["documents"]:
            require(isinstance(entry, dict) and set(entry) == {"tokens", "fold"})
            tokens = entry["tokens"]
            require(isinstance(tokens, list) and tokens and tokens == sorted(set(tokens))
                    and all(isinstance(token, str) and re.fullmatch(r"[0-9a-f]{64}", token) for token in tokens)
                    and entry["fold"] in SPLITS)
            key = fingerprint(tokens)
            require(key > previous and key not in self.documents)
            previous = key
            additions.append((key, frozenset(tokens), entry["fold"]))
        for key, tokens, fold in additions:
            if parent is not None:
                require(self.matches(tokens, self.threshold) <= {fold}, "reservation_parent_conflict")
            self.documents[key] = (tokens, fold)

    def matches(self, tokens, threshold):
        if threshold == self.threshold:
            exact = getattr(self, "exact", {}).get(tokens)
            if exact is not None:
                return {exact}
        result = set()
        for known, fold in self.documents.values():
            if min(len(tokens), len(known)) < threshold * max(len(tokens), len(known)):
                continue
            intersection = len(tokens & known)
            if intersection / (len(tokens) + len(known) - intersection) >= threshold:
                result.add(fold)
        return result


def component_documents(rows, components):
    result = {}
    for row in rows:
        key = components[pair_id(row)]
        documents = result.setdefault(key, set())
        documents.update((token_hashes(row["vacancy_text"]), token_hashes(row["candidate_text"])))
    return result


def assignments(rows, components, seed, ratios, threshold, policy):
    registry = Registry(policy)
    require(threshold <= registry.threshold, "reservation_threshold_weakened")
    documents = component_documents(rows, components)
    assigned, semantic = {}, {}
    for component, values in documents.items():
        folds = set().union(*(registry.matches(tokens, threshold) for tokens in values))
        require(len(folds) <= 1, "reservation_bridge_conflict")
        if folds:
            assigned[component] = next(iter(folds))
        semantic[component] = sha256(canonical_json(sorted(fingerprint(tokens) for tokens in values)))
    counts = {name: int(len(documents) * ratios[name]) for name in SPLITS}
    counts["train"] += len(documents) - sum(counts.values())
    require(all(counts.values()), "empty_split")
    anchored = Counter(assigned.values())
    require(all(anchored[name] <= counts[name] for name in SPLITS), "reservation_split_ratio_conflict")
    ordered = sorted((key for key in documents if key not in assigned),
                     key=lambda key: sha256(f"{seed}:{semantic[key]}".encode()))
    offset = 0
    for name in SPLITS:
        needed = counts[name] - anchored[name]
        assigned.update({key: name for key in ordered[offset:offset + needed]})
        offset += needed
    require(offset == len(ordered))
    return assigned, semantic


def extend_policy(parent, rows, components, assigned):
    registry = Registry(parent)
    additions = {}
    for component, values in component_documents(rows, components).items():
        for tokens in values:
            key = fingerprint(tokens)
            if key not in registry.documents:
                entry = {"tokens": sorted(tokens), "fold": assigned[component]}
                require(key not in additions or additions[key] == entry, "reservation_bridge_conflict")
                additions[key] = entry
    if not additions:
        return parent
    documents = [additions[key] for key in sorted(additions)]
    child = {"schema_version": 1, "policy_id": "extension-" + sha256(canonical_json(documents)),
             "normalization": registry.normalization, "threshold": registry.threshold, "anchor": None,
             "parent": parent, "parent_sha256": sha256(canonical_json(parent)), "documents": documents}
    Registry(child)
    return child


def validate_partitions(partitions, threshold, policy):
    """Only supplied train/validation rows + reservation metadata are inspected."""
    from .dataset import connected_components

    registry = Registry(policy)
    require(threshold <= registry.threshold, "reservation_threshold_weakened")
    rows = [row for part in partitions.values() for row in part]
    components, _ = connected_components(rows, threshold)
    folds = {}
    for name, part in partitions.items():
        for row in part:
            component = components[pair_id(row)]
            folds.setdefault(component, set()).add(name)
            for kind in ("vacancy", "candidate"):
                matched = registry.matches(token_hashes(row[kind + "_text"]), threshold)
                require(matched == {name}, "reservation_partition_violation")
    require(all(len(names) == 1 for names in folds.values()), "reservation_bridge_conflict")
    return {"policy_sha256": sha256(canonical_json(policy)), "root_policy_sha256": ROOT_POLICY_SHA256,
            "checked_partitions": list(partitions), "semantic_components": len(folds)}


def manifest_policy(manifest_path, manifest, *, required=True):
    from .dataset import _read_artifact

    descriptor = manifest.get("reservation")
    if descriptor is None:
        require(not required, "reservation_metadata_required")
        return None
    require(manifest["schema_version"] == 2 and isinstance(descriptor, dict)
            and set(descriptor) == {"schema_version", "algorithm", "policy", "assignments_sha256"}
            and type(descriptor["schema_version"]) is int and descriptor["schema_version"] == 1
            and descriptor["algorithm"] == "anchored-semantic-v1"
            and isinstance(descriptor["assignments_sha256"], str)
            and re.fullmatch(r"[0-9a-f]{64}", descriptor["assignments_sha256"]))
    require(isinstance(descriptor["policy"], dict) and set(descriptor["policy"]) == {"path", "sha256"})
    relative = descriptor["policy"]["path"]
    require(isinstance(relative, str) and relative.endswith("/reservation_policy.json")
            and relative not in {item["path"] for item in manifest["artifacts"].values()})
    path = manifest_path.resolve().parent / relative
    require(not path.is_symlink() and all(not parent.is_symlink() for parent in path.parents
                                         if parent != manifest_path.resolve().parent
                                         and parent.is_relative_to(manifest_path.resolve().parent)))
    content = _read_artifact(manifest_path.resolve().parent, descriptor["policy"])
    policy = json.loads(content)
    require(content == canonical_json(policy))
    Registry(policy)
    return policy
