"""Run the stateless baseline on validation without opening frozen test bytes."""

import argparse
import hashlib
import json
from pathlib import Path
import platform
import sys

from ats_mlops.data import DataError, load_manifest, load_partition
from ats_mlops.data.dataset import canonical_json
from ats_mlops.evaluation.ranking import evaluate_ranking
from ats_mlops.model.baseline import MODEL_VERSION, TOKENIZER, score_rows


ROOT = Path(__file__).resolve().parents[3]


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validation_report(manifest_path: Path) -> dict:
    """Verify only admitted train/validation, then score complete validation groups."""
    manifest_path = manifest_path.resolve()
    manifest = load_manifest(manifest_path)
    split_artifact = manifest["artifacts"]["split_lists"]
    train = load_partition(manifest_path, "train")
    validation = load_partition(manifest_path, "validation")
    metrics = evaluate_ranking(validation, score_rows(validation),
                               group_ids=manifest["splits"]["validation"]["vacancy_ids"])
    sources = [
        "src/ats_mlops/data/dataset.py", "src/ats_mlops/model/baseline.py",
        "src/ats_mlops/evaluation/ranking.py", "src/ats_mlops/evaluation/baseline.py",
    ]
    return {
        "schema_version": 1, "evidence_scope": manifest["evidence_scope"],
        "partition": "validation", "model_version": MODEL_VERSION,
        "tokenizer": TOKENIZER, "fitted": False, "tie_break": "candidate_id ascending",
        "read_partitions": ["train", "validation"], "test_read": False,
        "train_rows_verified": len(train), "validation_rows_verified": len(validation),
        "manifest_sha256": _hash(manifest_path),
        "split_sha256": split_artifact["sha256"],
        "split_sha256_source": "admitted_manifest_reference; split_bytes_not_read",
        "partition_inventory_sha256": {
            name: hashlib.sha256(canonical_json(manifest["splits"][name])).hexdigest()
            for name in ("train", "validation")
        },
        "data_sha256": {name: manifest["artifacts"][name]["sha256"] for name in ("train", "validation")},
        "source_sha256": {name: _hash(ROOT / name) for name in sources},
        "environment": {"python": platform.python_version(), "dependencies": "stdlib only"},
        "metrics": metrics,
        "limitations": [
            "Synthetic rule labels only; no independent hiring outcomes or real-world quality claim.",
            "No model fitting or tuning; this is engineering validation, not a release gate.",
            "Timing is observational and varies between runs; scores and metrics are deterministic.",
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data_manifest.json"))
    parser.add_argument("--output", type=Path, default=Path("reports/baseline/validation.json"))
    args = parser.parse_args(argv)
    try:
        report = validation_report(args.manifest)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n")
    except (DataError, ValueError, KeyError, OSError) as error:
        print(f"baseline evaluation failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"status": report["metrics"]["status"], "report": str(args.output)}))
    return 0 if report["metrics"]["status"] == "defined" else 2


if __name__ == "__main__":
    raise SystemExit(main())
