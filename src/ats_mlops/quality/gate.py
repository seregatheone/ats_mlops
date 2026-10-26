"""Create an immutable validation-only gate report for exact local model artifacts."""

import argparse
import json
from pathlib import Path
import platform
import sys

from ats_mlops.data.dataset import canonical_json, sha256
from ats_mlops.quality.artifacts import digest, validate_model, validation_snapshot
from ats_mlops.quality.isolated import DockerScorer
from ats_mlops.quality.policy import GateError, assess_scores, require, thresholds


def run_gate(request: dict, output: Path, *, scorer=None):
    require(not output.exists(), "new_gate_report_required")
    report = {"schema_version": 1, "outcome": "rejected", "evidence_scope": "demo_only",
              "reasons": [], "candidate": None, "incumbent": None,
              "expected_incumbent": None, "first_release": request.get("incumbent") is None,
              "metrics": {}, "comparisons": {}, "scoring_receipts": {}, "test_read": False,
              "limitations": ["Model gate only; release checks, approval and activation are separate.",
                              "Evidence scope describes current validation labels; synthetic quality is demo_only."]}
    role = "validation"
    try:
        require(set(request) == {"validation_manifest", "admission_report", "dataset_binding", "thresholds", "candidate", "incumbent"},
                "invalid_gate_request")
        policy_path = Path(request["thresholds"])
        policy = thresholds(policy_path)
        report["thresholds"] = {"config": policy, "sha256": sha256(canonical_json(policy)),
                                "file_sha256": digest(policy_path)}
        snapshot = validation_snapshot(Path(request["validation_manifest"]), Path(request["admission_report"]),
                                       Path(request["dataset_binding"]))
        report["evidence_scope"] = snapshot["evidence_scope"]
        report["validation"] = {key: snapshot[key] for key in ("manifest_sha256", "admission_sha256",
                                "validation_sha256", "split_sha256", "inventory_sha256", "dataset_binding_sha256")}
        report["validation"]["read_partitions"] = ["validation"]
        models = {}
        for role in ("candidate", "incumbent"):
            if role == "incumbent" and request[role] is None:
                continue
            models[role] = validate_model(request[role], snapshot, candidate=role == "candidate")
            report[role] = models[role]["public"]
        if "incumbent" in models:
            report["expected_incumbent"] = {key: report["incumbent"][key] for key in
                ("model_name", "model_version", "run_id", "model_sha256", "release_id", "release_is_fixture")}
        scores = {}
        for role, model in models.items():
            scores[role], receipt = (scorer or DockerScorer())(model, snapshot["rows"])
            report["scoring_receipts"][role] = receipt
        role = "policy"
        assessment = assess_scores(snapshot["rows"], snapshot["groups"], scores["candidate"], scores.get("incumbent"),
                                   evidence_scope=snapshot["evidence_scope"], policy=policy)
        report.update(assessment)
        report["gate_runtime"] = {"python": platform.python_version(), "coordinator": "stdlib host; offline pinned model containers",
                                  "metric_code_sha256": digest(Path(__file__).parents[1] / "evaluation/ranking.py"),
                                  "quality_source_sha256": {path.name: digest(path) for path in sorted(Path(__file__).parent.glob("*.py"))}}
    except GateError as error:
        report["reasons"].append(role + "." + str(error))
    except (ValueError, KeyError, TypeError, OSError, OverflowError):
        report["reasons"].append(role + ".incomplete_or_invalid_lineage")
    except Exception:
        report["reasons"].append(role + ".gate_execution_failed")
    report["reasons"] = sorted(set(report["reasons"]))
    if report["reasons"]:
        report["outcome"] = "rejected"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n")
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        report = run_gate(json.loads(args.request.read_text()), args.output)
    except Exception:
        print(json.dumps({"outcome": "rejected", "reason": "gate_request_or_output_invalid"}), file=sys.stderr)
        return 1
    print(json.dumps({"outcome": report["outcome"], "evidence_scope": report["evidence_scope"], "reasons": report["reasons"]}))
    return 0 if report["outcome"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
