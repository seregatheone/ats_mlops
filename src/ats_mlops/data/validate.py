"""GX admission and fail-closed training preflight with text-free reports.

Admission is the only mode allowed to open source/test. Training requires its
receipt bound to the exact manifest, then validates only requested split bytes.
"""

from __future__ import annotations

from collections import Counter
from importlib.resources import files
import json
from pathlib import Path

from ats_mlops.data.dataset import (
    SCHEMA, DataError, _read_artifact, audit_manifest, canonical_json,
    clean_rows, load_manifest, load_partition, sha256,
)


GX_ROOT = files("ats_mlops.data").joinpath("expectations")
REPORT_SCHEMA = 1


class ValidationError(DataError):
    """A persisted failed report; the training boundary must not be crossed."""

    def __init__(self, report: dict):
        self.report = report
        super().__init__("data_validation_failed:" + ",".join(report["reasons"]))


def _gx_validate(rows: list, scope: str) -> dict:
    # Import only in the pinned ML worker, keeping the ordinary CLI lightweight.
    import great_expectations as gx
    import pandas as pd

    if gx.__version__ != "1.24.1":
        raise DataError("gx_version_mismatch")
    suite_config = json.loads((GX_ROOT / "pairs.json").read_text())
    checkpoint_config = json.loads((GX_ROOT / "checkpoint.json").read_text())
    records = []
    for original in rows:
        row = original if isinstance(original, dict) else {}
        record = {key: row.get(key) for key in SCHEMA}
        record.update({
            "_schema_valid": isinstance(original, dict) and set(row) == set(SCHEMA),
            "_types_valid": all(isinstance(row.get(key), str) for key in SCHEMA[:-1]),
            # bool equals 1 in Python/pandas, and NaN/inf are not valid labels.
            "_relevance_valid": type(row.get("relevance")) is int and row["relevance"] in (0, 1),
        })
        records.append(record)
    frame = pd.DataFrame(records, columns=[*SCHEMA, "_schema_valid", "_types_valid", "_relevance_valid"])
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = {"globally": False}
    source = context.data_sources.add_pandas(name="ats")
    definition = source.add_dataframe_asset(name="pairs").add_batch_definition_whole_dataframe("whole")
    suite = gx.ExpectationSuite(**suite_config)
    for key in SCHEMA:
        suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(
            column=key, meta={"reason": "missing_field_or_null"}))
    suite = context.suites.add(suite)
    validation = context.validation_definitions.add(gx.ValidationDefinition(
        name=checkpoint_config["validation_definition"], data=definition, suite=suite))
    checkpoint = context.checkpoints.add(gx.Checkpoint(
        name=checkpoint_config["name"], validation_definitions=[validation],
        result_format=checkpoint_config["result_format"], actions=[]))
    result = checkpoint.run(batch_parameters={"dataframe": frame})
    checks = []
    for validation_result in result.run_results.values():
        for check in validation_result.results:
            config = check.expectation_config
            checks.append({"expectation": config.type,
                           "column": config.kwargs.get("column"),
                           "success": bool(check.success),
                           "reason": config.meta["reason"]})
    # Never serialize GX raw results, exceptions, unexpected values or data docs.
    return {"scope": scope, "rows": len(rows), "success": bool(result.success), "checks": checks}


def _raw_rows(manifest_path: Path, manifest: dict, name: str) -> list:
    content = _read_artifact(manifest_path.resolve().parent, manifest["artifacts"][name])
    try:
        return [json.loads(line) for line in content.splitlines()]
    except (ValueError, UnicodeError) as exc:
        raise DataError("invalid_jsonl") from exc


def _config_hashes() -> dict:
    return {name: sha256((GX_ROOT / name).read_bytes()) for name in ("pairs.json", "checkpoint.json")}


def _valid_receipt(receipt: dict, manifest_sha256: str) -> bool:
    fingerprint = receipt.get("report_sha256")
    document = {key: value for key, value in receipt.items() if key != "report_sha256"}
    config = json.loads((GX_ROOT / "pairs.json").read_text())
    expected = [(item["type"], item["kwargs"].get("column"), item["meta"]["reason"])
                for item in config["expectations"]]
    expected += [("expect_column_values_to_not_be_null", key, "missing_field_or_null") for key in SCHEMA]
    batches = receipt.get("gx", [])
    return (fingerprint == sha256(canonical_json(document))
            and receipt.get("schema_version") == REPORT_SCHEMA
            and receipt.get("mode") == "admission" and receipt.get("outcome") == "passed"
            and receipt.get("manifest_sha256") == manifest_sha256
            and receipt.get("engine") == "great_expectations"
            and receipt.get("engine_version") == "1.24.1"
            and receipt.get("config_hashes") == _config_hashes()
            and receipt.get("reasons") == []
            and [batch["scope"] for batch in batches] == ["source", "train", "validation", "test"]
            and all(batch["success"] is True and batch["rows"] > 0
                    and Counter((check["expectation"], check["column"], check["reason"])
                                for check in batch["checks"]) == Counter(expected)
                    and all(check["success"] is True for check in batch["checks"])
                    for batch in batches))


def _persist(report: dict, report_dir: Path) -> None:
    report["report_sha256"] = sha256(canonical_json(report))
    report_dir.mkdir(parents=True, exist_ok=True)
    stem = "data-" + report["mode"]
    (report_dir / (stem + ".json")).write_bytes(canonical_json(report))
    lines = ["# Data validation", "", f"Outcome: {report['outcome']}",
             f"Mode: {report['mode']}", f"Manifest SHA256: {report['manifest_sha256']}",
             "Evidence scope: demo_only", "", "## Reasons", ""]
    lines.extend("- " + reason for reason in report["reasons"])
    if not report["reasons"]:
        lines.append("- All checks passed.")
    lines.extend(["", "## Great Expectations", ""])
    for batch in report["gx"]:
        lines.append(f"- {batch['scope']}: rows={batch['rows']}; success={batch['success']}")
        for check in batch["checks"]:
            if not check["success"]:
                lines.append(f"  - {check['expectation']}: {check['reason']}")
    (report_dir / (stem + ".md")).write_text("\n".join(lines) + "\n")


def _validate(manifest_path: Path, report_dir: Path, mode: str,
              admission_report: Path | None) -> tuple[dict, dict[str, list[dict]]]:
    report = {"schema_version": REPORT_SCHEMA, "mode": mode, "outcome": "rejected",
              "evidence_scope": "demo_only", "manifest_sha256": None,
              "engine": "great_expectations", "engine_version": "1.24.1",
              "gx": [], "reasons": [], "artifact_hashes": {}, "counts": {},
              "config_hashes": {}}
    loaded = {}
    try:
        if mode not in {"admission", "training"}:
            raise DataError("unknown_validation_mode")
        manifest_bytes = manifest_path.read_bytes()
        report["manifest_sha256"] = sha256(manifest_bytes)
        report["config_hashes"] = _config_hashes()
        manifest = load_manifest(manifest_path)
        from .reservation import manifest_policy, validate_partitions

        policy = manifest_policy(manifest_path, manifest)
        if manifest != json.loads(manifest_bytes):
            raise DataError("manifest_changed_during_validation")
        if mode == "training":
            if admission_report is None or not admission_report.is_file():
                raise DataError("admission_receipt_required")
            try:
                receipt = json.loads(admission_report.read_text())
            except (ValueError, UnicodeError) as exc:
                raise DataError("invalid_admission_receipt") from exc
            if not _valid_receipt(receipt, report["manifest_sha256"]):
                raise DataError("admission_receipt_mismatch")
        names = ("source", "train", "validation", "test") if mode == "admission" else ("train", "validation")
        for name in names:
            rows = _raw_rows(manifest_path, manifest, name)
            batch = _gx_validate(rows, name)
            report["gx"].append(batch)
            report["counts"][name] = len(rows)
            report["artifact_hashes"][name] = manifest["artifacts"][name]["sha256"]
            report["reasons"].extend(check["reason"] for check in batch["checks"] if not check["success"])
            if not batch["success"]:
                continue
            # Identity conflicts and other structural invariants are additional
            # checks, never substitutes for running the persisted GX suite.
            clean_rows(rows)
            if name != "source":
                loaded[name] = load_partition(manifest_path, name, final_evaluation=mode == "admission")
                if loaded[name] != rows:
                    raise DataError("artifact_changed_during_validation")
        if not report["reasons"]:
            report["reservation"] = validate_partitions(loaded, manifest["generator"]["config"]["near_duplicate_jaccard"], policy)
            if mode == "admission":
                audit_manifest(manifest_path)
            if manifest_path.read_bytes() != manifest_bytes:
                raise DataError("manifest_changed_during_validation")
            report["outcome"] = "passed"
    except DataError as exc:
        report["reasons"].append(str(exc))
    except (OSError, ValueError, KeyError, TypeError):
        report["reasons"].append("invalid_data_or_manifest")
    except Exception:
        # GX internals may echo unexpected data in exception messages.
        report["reasons"].append("gx_execution_failed")
    report["reasons"] = sorted(set(report["reasons"]))
    _persist(report, report_dir)
    return report, loaded


def validate_dataset(manifest_path: Path, report_dir: Path, *, mode: str = "admission",
                     admission_report: Path | None = None) -> dict:
    """Persist machine/readable reports and return passed/rejected (never raw text)."""
    return _validate(manifest_path, report_dir, mode, admission_report)[0]


def require_valid(manifest_path: Path, report_dir: Path, *,
                  admission_report: Path) -> dict[str, list[dict]]:
    """Training boundary: fail closed, return the exact validated train/validation rows.

    Consumers must fit these in-memory rows, rather than reopening mutable files.
    No test, source, corpus or split-list bytes are read in this mode.
    """
    report, rows = _validate(manifest_path, report_dir, "training", admission_report)
    if report["outcome"] != "passed":
        raise ValidationError(report)
    return rows
