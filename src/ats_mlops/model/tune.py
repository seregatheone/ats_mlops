"""Seeded sequential validation-only Optuna search; final training is separate."""

import argparse
from copy import deepcopy
from importlib.metadata import version
import json
import math
from pathlib import Path
import sys

from ats_mlops.data.dataset import canonical_json, load_manifest, sha256
from ats_mlops.data.validate import require_valid
from ats_mlops.evaluation.ranking import evaluate_ranking
from ats_mlops.lineage.manifest import file_hash, source_environment, write_json
from ats_mlops.model.scoring import score_rows
from ats_mlops.model.training import LIMITATIONS, fit_rows


class TrialFailure(ValueError):
    """Only sanitized reason codes cross the Optuna exception boundary."""


SAFE_FAILURE_CODES = {"empty_training_rows", "invalid_training_row", "invalid_relevance",
                      "single_class_training", "empty_vocabulary", "training_did_not_converge",
                      "invalid_model_config", "invalid_pair_text", "invalid_pair_id", "duplicate_pair",
                      "invalid_model_classes", "invalid_probability_shape", "invalid_probability_values",
                      "score_keys_must_match_complete_rows", "scores_must_be_finite_numbers",
                      "undefined_or_invalid_objective"}


def validate_search(search: dict) -> None:
    if (not isinstance(search, dict)
            or set(search) != {"schema_version", "sampler_seed", "n_trials", "n_jobs", "timeout_seconds", "space"}
            or search["schema_version"] != 1 or type(search["sampler_seed"]) is not int
            or type(search["n_trials"]) is not int or search["n_trials"] != 20
            or type(search["n_jobs"]) is not int or search["n_jobs"] != 1):
        raise ValueError("invalid_search_config_or_budget")
    timeout = search["timeout_seconds"]
    if timeout is not None and (type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0):
        raise ValueError("invalid_search_timeout")
    space = search["space"]
    if not isinstance(space, dict) or set(space) != {"classifier.C", "features.max_features"}:
        raise ValueError("invalid_search_space")
    for name, choices in space.items():
        if (not isinstance(choices, list) or not choices
                or any(type(value) not in ((int, float) if name == "classifier.C" else (int,))
                       or not math.isfinite(value) or value <= 0 for value in choices)
                or len(set(choices)) != len(choices)):
            raise ValueError("invalid_search_space")


def selected_config(base: dict, params: dict) -> dict:
    config = deepcopy(base)
    for name, value in params.items():
        section, key = name.split(".")
        config[section][key] = value
    return config


def run_study(train_rows: list[dict], validation_rows: list[dict], group_ids: list[str],
              base_config: dict, search: dict, *, smoke: bool = False) -> dict:
    """Fit fresh pipelines on admitted train rows; never reopen any data files."""
    import optuna

    validate_search(search)
    if type(smoke) is not bool:
        raise ValueError("invalid_smoke_override")
    budget = 2 if smoke else search["n_trials"]
    # Eligibility is independent of trial predictions, including empty groups.
    cohort = evaluate_ranking(validation_rows,
                              {(row["vacancy_id"], row["candidate_id"]): 0.0 for row in validation_rows},
                              group_ids=group_ids)["eligible_group_ids"]

    def objective(trial):
        params = {name: trial.suggest_categorical(name, choices)
                  for name, choices in sorted(search["space"].items())}
        try:
            model = fit_rows(train_rows, selected_config(base_config, params))
            metrics = evaluate_ranking(validation_rows, score_rows(model, validation_rows), group_ids=group_ids)
            value = metrics["ndcg_at_10"]
            if (metrics["status"] != "defined" or metrics["eligible_group_ids"] != cohort
                    or type(value) not in (int, float) or not math.isfinite(value) or not 0 <= value <= 1):
                raise TrialFailure("undefined_or_invalid_objective")
            return float(value)
        except Exception as error:
            code = str(error) if str(error) in SAFE_FAILURE_CODES else "trial_fit_or_evaluation_failed"
            trial.set_user_attr("error_code", code)
            trial.set_user_attr("error_type", next((kind.__name__ for kind in
                                (TrialFailure, ValueError, TypeError, RuntimeError, OSError)
                                if isinstance(error, kind)), "Exception"))
            # Never retain upstream exception messages: they may contain raw text.
            raise TrialFailure(code) from None

    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=search["sampler_seed"]))
    verbosity = optuna.logging.get_verbosity()
    try:
        optuna.logging.set_verbosity(optuna.logging.CRITICAL)
        study.optimize(objective, n_trials=budget, n_jobs=1, timeout=search["timeout_seconds"],
                       catch=(TrialFailure,))
    finally:
        optuna.logging.set_verbosity(verbosity)
    trials = [{"number": trial.number, "params": trial.params, "value": trial.value,
               "state": trial.state.name, "error_code": trial.user_attrs.get("error_code"),
               "error_type": trial.user_attrs.get("error_type")}
              for trial in study.trials]
    completed = [trial for trial in trials if trial["state"] == "COMPLETE"
                 and trial["value"] is not None and math.isfinite(trial["value"])]
    outcome = "passed" if len(trials) == budget and completed else "failed"
    reason = None if outcome == "passed" else "incomplete_budget" if len(trials) != budget else "no_complete_trials"
    best = min(completed, key=lambda trial: (-trial["value"], trial["number"])) if outcome == "passed" else None
    return {"schema_version": 1, "evidence_scope": "demo_only", "outcome": outcome, "reason": reason,
            "objective": "validation.ndcg_at_10", "direction": "maximize",
            "tie_policy": "maximum_value_then_lowest_trial_number", "sampler": "TPESampler",
            "optuna_version": version("optuna"), "sampler_seed": search["sampler_seed"], "n_jobs": 1,
            "configured_budget": search["n_trials"], "effective_budget": budget, "smoke_override": smoke,
            "timeout_seconds": search["timeout_seconds"], "attempted_trials": len(trials),
            "complete_trials": len(completed), "eligible_group_ids": cohort,
            "validation_group_inventory_sha256": sha256(canonical_json(group_ids)),
            "trials": trials, "best": best,
            "selected_config": selected_config(base_config, best["params"]) if best else None,
            "search_config": search, "search_config_sha256": sha256(canonical_json(search)),
            "base_config": base_config, "base_config_sha256": sha256(canonical_json(base_config)),
            "read_partitions": ["train", "validation"], "test_read": False, "limitations": LIMITATIONS}


def tune(manifest_path: Path, report_dir: Path, *, admission_report: Path,
         config_path: Path, search_path: Path, source_root: Path, image_identity: dict,
         runtime_lock: Path = Path("/opt/requirements-ml.lock"), smoke: bool = False) -> dict:
    """Admit once, freeze source/input identities, and persist an immutable study."""
    report_dir = report_dir.resolve()
    if report_dir.is_relative_to(source_root.resolve()):
        raise ValueError("reports_must_be_outside_source_checkout")
    if report_dir.exists() and any(report_dir.iterdir()):
        raise ValueError("new_report_directory_required")
    search = json.loads(search_path.read_text())
    config = json.loads(config_path.read_text())
    validate_search(search)
    provenance = source_environment(source_root, image_identity, runtime_lock=runtime_lock)
    rows = require_valid(manifest_path, report_dir, admission_report=admission_report)
    guard = json.loads((report_dir / "data-training.json").read_text())
    manifest = load_manifest(manifest_path)
    if file_hash(manifest_path) != guard["manifest_sha256"]:
        raise ValueError("manifest_changed_after_validation")
    result = run_study(rows["train"], rows["validation"], manifest["splits"]["validation"]["vacancy_ids"],
                       config, search, smoke=smoke)
    result.update({"provenance": provenance, "data_manifest_sha256": guard["manifest_sha256"],
                   "data_sha256": guard["artifact_hashes"],
                   "split_sha256": manifest["artifacts"]["split_lists"]["sha256"],
                   "split_hash_source": "admitted_manifest_reference; split_bytes_not_read",
                   "partition_inventory_sha256": {name: sha256(canonical_json(manifest["splits"][name]))
                                                  for name in ("train", "validation")},
                   "metric_code_sha256": file_hash(source_root / "src/ats_mlops/evaluation/ranking.py"),
                   "admission_report_sha256": file_hash(admission_report),
                   "training_guard_sha256": file_hash(report_dir / "data-training.json"),
                   "search_file_sha256": file_hash(search_path), "base_config_file_sha256": file_hash(config_path)})
    if result["selected_config"] is not None:
        result["selected_config_sha256"] = sha256(canonical_json(result["selected_config"]))
        write_json(report_dir / "selected-model.json", result["selected_config"])
    write_json(report_dir / "study.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("manifest", "report-dir", "admission-report", "config", "search", "source-root", "image-identity"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--smoke", action="store_true", help="separate two-trial override; never changes the configured budget")
    args = parser.parse_args(argv)
    try:
        result = tune(args.manifest, args.report_dir, admission_report=args.admission_report,
                      config_path=args.config, search_path=args.search, source_root=args.source_root,
                      image_identity=json.loads(args.image_identity.read_text()), smoke=args.smoke)
    except Exception:
        print(json.dumps({"outcome": "failed", "reason": "tuning_preflight_or_execution_failed"}), file=sys.stderr)
        return 1
    print(json.dumps({"outcome": result["outcome"], "reason": result["reason"],
                      "attempted_trials": result["attempted_trials"], "smoke_override": result["smoke_override"]}))
    return 0 if result["outcome"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
