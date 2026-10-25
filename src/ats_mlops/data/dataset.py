"""Deterministic corpus preparation; no fitted transforms or model imports."""

from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import random
import re
import unicodedata


SCHEMA = ("vacancy_id", "candidate_id", "vacancy_text", "candidate_text", "relevance")
SPLITS = ("train", "validation", "test")
GENERATOR_VERSION = "synthetic-skills-v1"
DUPLICATE_ALGORITHM = "global token-set Jaccard with union-find connected components"
DUPLICATE_NORMALIZATION = "NFKC, whitespace collapse, Unicode casefold, Unicode word tokens; order/punctuation ignored"
SKILLS = (
    "python java kotlin scala rust golang javascript typescript sql bash ruby swift "
    "docker kubernetes linux airflow mlflow dvc bentoml evidently prometheus grafana "
    "postgresql mysql redis mongodb clickhouse kafka rabbitmq nginx terraform ansible "
    "pandas numpy scipy sklearn pytorch tensorflow opencv spark hadoop dbt snowflake "
    "react vue angular svelte html css accessibility playwright selenium pytest junit "
    "git cicd rest grpc graphql oauth openapi networking observability testing "
    "statistics algebra optimization ranking classification regression clustering nlp "
    "etl analytics warehousing monitoring encryption transactions caching profiling"
).split()
PROVENANCE = {
    "source": "project-generated synthetic skill matching scenarios",
    "usage_terms": "Owner-authorized publication for engineering experiments and CI; no third-party records.",
    "label_origin": "Fixed generator rubric: at least 5 of 7 required skills; never model predictions.",
    "label_independence": "synthetic_rule_only; no independently observed hiring outcomes",
    "id_origin": "Generator counters scoped by corpus namespace; no person identifiers.",
    "personal_data": False,
}


class DataError(ValueError):
    """A corpus or manifest does not satisfy the data contract."""


def canonical_json(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()


def sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def normalize_text(text: str) -> str:
    """Canonical text storage; token normalization separately ignores punctuation/case."""
    return " ".join(unicodedata.normalize("NFKC", text).split())


def text_tokens(text: str) -> frozenset[str]:
    return frozenset(re.findall(r"\w+", normalize_text(text).casefold(), flags=re.UNICODE))


def _require(condition: bool, code: str) -> None:
    if not condition:
        raise DataError(code)


def clean_rows(rows: list[dict]) -> tuple[list[dict], dict]:
    """Validate entity identity, normalize whitespace and remove repeated identical pairs."""
    _require(isinstance(rows, list) and bool(rows), "empty_corpus")
    identities: dict[tuple[str, str], str] = {}
    pairs: dict[tuple[str, str], dict] = {}
    for original in rows:
        _require(isinstance(original, dict) and set(original) == set(SCHEMA), "invalid_row_schema")
        row = dict(original)
        for kind in ("vacancy", "candidate"):
            identity, text = row[f"{kind}_id"], row[f"{kind}_text"]
            _require(isinstance(identity, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", identity) is not None,
                     "invalid_entity_id")
            _require(isinstance(text, str) and 0 < len(text) <= 10_000, "invalid_text")
            text = normalize_text(text)
            _require(bool(text_tokens(text)), "empty_text")
            row[f"{kind}_text"] = text
            key = (kind, identity)
            _require(key not in identities or identities[key] == text, "conflicting_entity_text")
            identities[key] = text
        _require(type(row["relevance"]) is int and row["relevance"] in (0, 1), "invalid_relevance")
        key = (row["vacancy_id"], row["candidate_id"])
        _require(key not in pairs or pairs[key] == row, "conflicting_pair")
        pairs[key] = row
    cleaned = [pairs[key] for key in sorted(pairs)]
    return cleaned, {"input_rows": len(rows), "retained_rows": len(cleaned),
                     "identical_pair_rows_removed": len(rows) - len(cleaned)}


def connected_components(rows: list[dict], threshold: float) -> tuple[dict[str, str], dict]:
    """Union shared IDs and global exact/near-duplicate text across both entity kinds."""
    _require(type(threshold) in (float, int) and 0 < threshold <= 1, "invalid_duplicate_threshold")
    parent: dict[str, str] = {}

    def root(key: str) -> str:
        parent.setdefault(key, key)
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    def union(left: str, right: str) -> None:
        a, b = root(left), root(right)
        if a != b:
            parent[max(a, b)] = min(a, b)

    documents: dict[str, frozenset[str]] = {}
    for row in rows:
        vacancy, candidate = "v:" + row["vacancy_id"], "c:" + row["candidate_id"]
        union(vacancy, candidate)
        documents[vacancy] = text_tokens(row["vacancy_text"])
        documents[candidate] = text_tokens(row["candidate_text"])
    items = sorted(documents.items())
    exact, near = 0, 0
    for position, (left_id, left) in enumerate(items):
        for right_id, right in items[position + 1:]:
            # Size bound avoids costly intersections that cannot meet Jaccard threshold.
            if min(len(left), len(right)) < threshold * max(len(left), len(right)):
                continue
            intersection = len(left & right)
            similarity = intersection / (len(left) + len(right) - intersection)
            if similarity >= threshold:
                union(left_id, right_id)
                exact += left == right
                near += left != right
    members: dict[str, list[str]] = {}
    for entity in sorted(parent):
        members.setdefault(root(entity), []).append(entity)
    ids = {entity: sha256(canonical_json(group)) for group in members.values() for entity in group}
    pair_components = {pair_id(row): ids["v:" + row["vacancy_id"]] for row in rows}
    return pair_components, {"entity_documents": len(items), "exact_token_duplicate_pairs": exact,
                             "near_duplicate_pairs": near, "components": len(members)}


def pair_id(row: dict) -> str:
    # IDs cannot contain the separator, so the pair representation is unambiguous.
    return row["vacancy_id"] + "/" + row["candidate_id"]


def split_rows(rows: list[dict], seed: int, ratios: dict, threshold: float) -> tuple[dict, dict, dict]:
    _require(type(seed) is int, "invalid_seed")
    _require(set(ratios) == set(SPLITS) and all(type(x) in (float, int) and x > 0 for x in ratios.values())
             and math.isclose(sum(ratios.values()), 1.0, abs_tol=1e-12), "invalid_split_ratios")
    components, duplicates = connected_components(rows, threshold)
    ordered = sorted(set(components.values()), key=lambda key: sha256(f"{seed}:{key}".encode()))
    _require(len(ordered) >= 3, "insufficient_independent_components")
    counts = {name: int(len(ordered) * ratios[name]) for name in SPLITS}
    counts["train"] += len(ordered) - sum(counts.values())
    _require(all(counts.values()), "empty_split")
    assignment, offset = {}, 0
    for name in SPLITS:
        assignment.update({key: name for key in ordered[offset:offset + counts[name]]})
        offset += counts[name]
    splits = {name: [row for row in rows if assignment[components[pair_id(row)]] == name] for name in SPLITS}
    for name, part in splits.items():
        _require({row["relevance"] for row in part} == {0, 1}, f"{name}_missing_class")
        groups = Counter(row["vacancy_id"] for row in part)
        positive_groups = {row["vacancy_id"] for row in part if row["relevance"] == 1}
        _require(bool(positive_groups) and all(size >= 2 for size in groups.values()), f"{name}_ineligible_groups")
    return splits, components, duplicates


def _summary(rows: list[dict], components: dict[str, str]) -> dict:
    groups = Counter(row["vacancy_id"] for row in rows)
    positive = {row["vacancy_id"] for row in rows if row["relevance"] == 1}
    return {"rows": len(rows), "positive_labels": sum(row["relevance"] for row in rows),
            "vacancy_ids": sorted(groups), "candidate_ids": sorted({row["candidate_id"] for row in rows}),
            "pair_ids": [pair_id(row) for row in rows],
            "component_ids": sorted({components[pair_id(row)] for row in rows}),
            "groups": len(groups), "eligible_groups": len(positive), "zero_positive_groups": len(groups) - len(positive),
            "min_group_size": min(groups.values()), "max_group_size": max(groups.values())}


def _relative(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError as exc:
        raise DataError("artifact_outside_manifest_directory") from exc


def _read_artifact(root: Path, item: dict) -> bytes:
    _require(isinstance(item, dict) and isinstance(item.get("path"), str), "invalid_artifact")
    path = (root / item["path"]).resolve()
    _relative(path, root)
    _require(path.is_file(), "missing_artifact")
    content = path.read_bytes()
    _require(sha256(content) == item.get("sha256"), "artifact_hash_mismatch")
    return content


def prepare_dataset(rows: list[dict], config: dict, output_dir: Path, manifest_path: Path,
                    provenance: dict | None = None) -> dict:
    """Admit synthetic pairs, split components and persist a test freeze before training."""
    source_rows = rows
    rows, cleaning = clean_rows(rows)
    threshold = config["near_duplicate_jaccard"]
    splits, components, duplicates = split_rows(rows, config["seed"], config["split_ratios"], threshold)
    root = manifest_path.resolve().parent
    output_dir = output_dir.resolve()
    _relative(output_dir, root)
    raw = b"".join(canonical_json(row) for row in rows)
    contents = {name: b"".join(canonical_json(row) for row in part) for name, part in splits.items()}
    freeze = {"sha256": sha256(contents["test"]),
              "pair_ids_sha256": sha256(canonical_json([pair_id(row) for row in splits["test"]])),
              "policy": "final_evaluation_only; never fit, tune, gate, or retrain"}
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text())
        _require(previous.get("frozen_test") == freeze, "frozen_test_change_requires_new_dataset_version")
    split_lists = {name: [pair_id(row) for row in part] for name, part in splits.items()}
    split_content = canonical_json({"seed": config["seed"], "pair_ids": split_lists,
                                    "pair_components": components})
    artifacts = {"source": (output_dir / "source.jsonl", b"".join(canonical_json(row) for row in source_rows)),
                 "corpus": (output_dir / "corpus.jsonl", raw),
                 "split_lists": (output_dir / "split.json", split_content)}
    artifacts.update({name: (output_dir / f"{name}.jsonl", content) for name, content in contents.items()})
    manifest = {
        "schema_version": 1, "evidence_scope": "demo_only", "provenance": provenance or PROVENANCE,
        "schema": {"fields": list(SCHEMA), "relevance": [0, 1], "encoding": "utf-8-jsonl"},
        "generator": {"version": GENERATOR_VERSION, "config": config,
                      "config_sha256": sha256(canonical_json(config)),
                      "source_sha256": sha256(Path(__file__).read_bytes())},
        "cleaning": cleaning,
        "duplicate_detection": {"algorithm": DUPLICATE_ALGORITHM,
                                "normalization": DUPLICATE_NORMALIZATION,
                                "threshold": threshold, **duplicates},
        "splits": {name: _summary(part, components) for name, part in splits.items()},
        "frozen_test": freeze,
        "artifacts": {name: {"path": _relative(path, root), "sha256": sha256(content)}
                      for name, (path, content) in artifacts.items()},
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    for path, content in artifacts.values():
        path.write_bytes(content)
    manifest_path.write_bytes(canonical_json(manifest))
    return audit_manifest(manifest_path)


def generate_rows(config: dict) -> list[dict]:
    _require(config.get("generator") == GENERATOR_VERSION and config.get("schema_version") == 1, "unknown_generator")
    groups, candidates, positives = (config.get(key) for key in ("vacancy_groups", "candidates_per_group", "positives_per_group"))
    _require(all(type(value) is int for value in (groups, candidates, positives))
             and groups >= 3 and 2 <= candidates <= 100 and 0 < positives < candidates, "invalid_generator_size")
    namespace = config.get("namespace")
    _require(isinstance(namespace, str) and re.fullmatch(r"[a-z0-9_-]{1,32}", namespace) is not None, "invalid_namespace")
    _require(type(config.get("seed")) is int, "invalid_seed")
    randomizer = random.Random(config["seed"])
    rows = []
    for group in range(groups):
        required = randomizer.sample(SKILLS, 7)
        others = sorted(set(SKILLS) - set(required))
        vacancy_text = "Требуются навыки: " + ", ".join(required) + ". Работа с командой продукта."
        for index in range(candidates):
            covered = randomizer.randint(5, 7) if index < positives else randomizer.randint(0, 3)
            skills = randomizer.sample(required, covered) + randomizer.sample(others, randomizer.randint(3, 6))
            randomizer.shuffle(skills)
            rows.append({"vacancy_id": f"{namespace}-v{group:04d}",
                         "candidate_id": f"{namespace}-c{group:04d}-{index:03d}",
                         "vacancy_text": vacancy_text,
                         "candidate_text": "Практический опыт: " + ", ".join(skills) + ". Разработка и поддержка проектов.",
                         "relevance": int(covered >= 5)})
    return rows


def generate_dataset(config_path: Path, manifest_path: Path, output_dir: Path) -> dict:
    config = json.loads(config_path.read_text())
    return prepare_dataset(generate_rows(config), config, output_dir, manifest_path)


def load_manifest(manifest_path: Path) -> dict:
    """Validate metadata and split inventories without opening any corpus/split bytes."""
    manifest = json.loads(manifest_path.read_text())
    _require(manifest.get("schema_version") == 1 and manifest.get("evidence_scope") == "demo_only", "invalid_manifest_scope")
    _require(manifest.get("provenance") == PROVENANCE, "unadmitted_provenance")
    _require(manifest.get("schema") == {"fields": list(SCHEMA), "relevance": [0, 1], "encoding": "utf-8-jsonl"}, "invalid_manifest_schema")
    generator = manifest["generator"]
    config = generator["config"]
    _require(generator.get("version") == GENERATOR_VERSION
             and generator.get("config_sha256") == sha256(canonical_json(config))
             and generator.get("source_sha256") == sha256(Path(__file__).read_bytes()), "generator_lineage_mismatch")
    _require(set(manifest["splits"]) == set(SPLITS), "invalid_split_inventory")
    _require(manifest["frozen_test"]["sha256"] == manifest["artifacts"]["test"]["sha256"], "frozen_test_mismatch")
    for position, name in enumerate(SPLITS):
        part = manifest["splits"][name]
        _require(part["rows"] == len(part["pair_ids"]) == len(set(part["pair_ids"])) and part["rows"] > 0,
                 "invalid_split_row_inventory")
        for other in SPLITS[position + 1:]:
            _require(all(not (set(part[kind]) & set(manifest["splits"][other][kind]))
                         for kind in ("vacancy_ids", "candidate_ids", "component_ids", "pair_ids")), "split_leakage")
    return manifest


def load_partition(manifest_path: Path, partition: str, *, final_evaluation: bool = False) -> list[dict]:
    """Read only the requested partition; test requires explicit final-evaluation intent.

    Training and Optuna should call this for train/validation and never call full
    audit_manifest: their verification must not read the frozen test file.
    """
    _require(partition in SPLITS, "unknown_partition")
    _require(partition != "test" or final_evaluation, "test_reserved_for_final_evaluation")
    manifest = load_manifest(manifest_path)
    content = _read_artifact(manifest_path.resolve().parent, manifest["artifacts"][partition])
    rows, _ = clean_rows([json.loads(line) for line in content.splitlines()])
    _require(content == b"".join(canonical_json(row) for row in rows), "noncanonical_partition")
    summary = manifest["splits"][partition]
    _require([pair_id(row) for row in rows] == summary["pair_ids"]
             and sorted({row["vacancy_id"] for row in rows}) == summary["vacancy_ids"]
             and sorted({row["candidate_id"] for row in rows}) == summary["candidate_ids"]
             and sum(row["relevance"] for row in rows) == summary["positive_labels"], "partition_inventory_mismatch")
    _require({row["relevance"] for row in rows} == {0, 1}, "partition_missing_class")
    return rows


def audit_manifest(manifest_path: Path) -> dict:
    """Verify all bytes and recompute boundaries; ingestion/final audit only, never fitting."""
    manifest = load_manifest(manifest_path)
    config = manifest["generator"]["config"]
    root = manifest_path.resolve().parent
    artifacts = manifest["artifacts"]
    _require(set(artifacts) == {"source", "corpus", "split_lists", *SPLITS}, "invalid_artifact_inventory")
    content = {name: _read_artifact(root, item) for name, item in artifacts.items()}
    rows, cleaning = clean_rows([json.loads(line) for line in content["source"].splitlines()])
    _require(content["corpus"] == b"".join(canonical_json(row) for row in rows), "noncanonical_corpus")
    _require(manifest["cleaning"] == cleaning, "cleaning_summary_mismatch")
    expected_rows, _ = clean_rows(generate_rows(config))
    _require(rows == expected_rows, "generator_corpus_mismatch")
    splits, components, duplicates = split_rows(rows, config["seed"], config["split_ratios"], config["near_duplicate_jaccard"])
    expected_lists = {"seed": config["seed"], "pair_ids": {name: [pair_id(row) for row in part] for name, part in splits.items()},
                      "pair_components": components}
    _require(content["split_lists"] == canonical_json(expected_lists), "split_inventory_mismatch")
    _require(manifest["duplicate_detection"] == {"algorithm": DUPLICATE_ALGORITHM,
             "normalization": DUPLICATE_NORMALIZATION, "threshold": config["near_duplicate_jaccard"],
             **duplicates}, "duplicate_summary_mismatch")
    intersections = {}
    for name, part in splits.items():
        _require(content[name] == b"".join(canonical_json(row) for row in part), f"{name}_content_mismatch")
        _require(manifest["splits"][name] == _summary(part, components), f"{name}_summary_mismatch")
        for other in SPLITS:
            if name >= other:
                continue
            intersections[name + "/" + other] = {
                kind: len(set(manifest["splits"][name][kind]) & set(manifest["splits"][other][kind]))
                for kind in ("vacancy_ids", "candidate_ids", "component_ids", "pair_ids")}
    _require(not any(value for pair in intersections.values() for value in pair.values()), "split_leakage")
    freeze = {"sha256": sha256(content["test"]),
              "pair_ids_sha256": sha256(canonical_json(expected_lists["pair_ids"]["test"])),
              "policy": "final_evaluation_only; never fit, tune, gate, or retrain"}
    _require(manifest["frozen_test"] == freeze, "frozen_test_mismatch")
    return {"outcome": "passed", "evidence_scope": "demo_only", "dataset_sha256": sha256(content["corpus"]),
            "test_sha256": freeze["sha256"], "counts": {name: {key: summary[key] for key in
                ("rows", "positive_labels", "groups", "eligible_groups", "zero_positive_groups", "min_group_size", "max_group_size")}
                for name, summary in manifest["splits"].items()},
            "cleaning": cleaning, "duplicates": duplicates, "intersections": intersections}
