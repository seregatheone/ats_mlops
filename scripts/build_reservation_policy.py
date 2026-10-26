"""Reproduce hash-only reservations from pinned generator + original metadata.

Never opens source/corpus/train/validation/test files. All generated rows remain
in memory; all three original partition SHA256 values are mandatory oracles.
"""

import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess


PINNED_COMMIT = "f6ac71fc6a9d94f9433385c48de7702f28e4e7bb"
GENERATOR_SHA = "4c86731bffef1a480220c702665cc4f90be79fc2687ccfe32058f2b62b08e105"
MANIFEST_SHA = "81bde9b3a96ecdf6a970ee4a04bb1c2ea06c3af6da4123c1471cf94a780ef0f9"
POLICY_SHA = "4ca6c42155d3e34e1436596381838bf31cc36339986c12b91278d760df5d265b"


def build(repo):
    metadata = (repo / "data_manifest.json").read_bytes()
    assert hashlib.sha256(metadata).hexdigest() == MANIFEST_SHA, "original_manifest_required"
    manifest = json.loads(metadata)
    source = subprocess.run(["git", "show", PINNED_COMMIT + ":src/ats_mlops/data/dataset.py"],
                            cwd=repo, check=True, capture_output=True).stdout
    assert hashlib.sha256(source).hexdigest() == GENERATOR_SHA, "pinned_generator_required"
    legacy = {"__name__": "pinned_reservation_generator", "__file__": "pinned-dataset.py"}
    exec(compile(source, "pinned-dataset.py", "exec"), legacy)
    canonical, digest = legacy["canonical_json"], legacy["sha256"]
    config = manifest["generator"]["config"]
    rows, _ = legacy["clean_rows"](legacy["generate_rows"](config))
    splits, _, _ = legacy["split_rows"](rows, config["seed"], config["split_ratios"], config["near_duplicate_jaccard"])
    partitions = {name: digest(b"".join(canonical(row) for row in part)) for name, part in splits.items()}
    assert all(value == manifest["artifacts"][name]["sha256"] for name, value in partitions.items()), "partition_oracle_mismatch"
    function = next(node for node in ast.parse(source).body if isinstance(node, ast.FunctionDef) and node.name == "generate_rows")
    signature = digest(canonical({"version": legacy["GENERATOR_VERSION"], "skills": legacy["SKILLS"],
                                  "function_source": ast.get_source_segment(source.decode(), function).strip()}))
    documents = {}
    for fold, part in splits.items():
        for row in part:
            for kind in ("vacancy", "candidate"):
                tokens = sorted(digest(token.encode("utf-8")) for token in legacy["text_tokens"](row[kind + "_text"]))
                key = digest(canonical(tokens))
                entry = {"tokens": tokens, "fold": fold}
                assert key not in documents or documents[key] == entry, "protected_fold_conflict"
                documents[key] = entry
    policy = {"schema_version": 1, "policy_id": "canonical-demo-v1",
              "normalization": legacy["DUPLICATE_NORMALIZATION"], "threshold": config["near_duplicate_jaccard"],
              "anchor": {"manifest_sha256": MANIFEST_SHA, "generator_source_sha256": GENERATOR_SHA,
                         "row_generator_sha256": signature, "generator_config": config,
                         "partitions_sha256": partitions,
                         "split_lists_sha256": manifest["artifacts"]["split_lists"]["sha256"]},
              "parent": None, "parent_sha256": None, "documents": [documents[key] for key in sorted(documents)]}
    content = canonical(policy)
    assert digest(content) == POLICY_SHA, "reservation_oracle_mismatch"
    return content, {"policy_sha256": POLICY_SHA, "documents": len(documents), "partitions_sha256": partitions,
                     "input_scope": "pinned_generator_and_manifest_metadata; no_partition_files"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, help="optional new policy file; never overwrites an existing file")
    args = parser.parse_args()
    content, receipt = build(args.repo)
    if args.output is not None:
        with args.output.open("xb") as output:
            output.write(content)
    else:
        installed = args.repo / "src/ats_mlops/data/reservations/canonical-v1.json"
        assert installed.read_bytes() == content, "bundled_policy_mismatch"
    print(json.dumps(receipt, sort_keys=True))
