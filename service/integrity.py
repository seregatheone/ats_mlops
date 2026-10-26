"""Stdlib identity validation before executing the delivered model code."""

import json
from pathlib import Path
import re

from service.contracts import API_SCHEMA, canonical, identifier, require, sha


def document(path):
    content = Path(path).read_bytes()
    return json.loads(content), sha(content)


def inventory(root, *, exclude=()):
    root = Path(root)
    require(root.is_dir() and not root.is_symlink())
    result = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        require(not path.is_symlink() and "__pycache__" not in relative.parts
                and path.suffix.lower() not in (".pyc", ".pyo"))
        if path.is_file():
            if relative.as_posix() not in exclude:
                result[relative.as_posix()] = sha(path.read_bytes())
        else:
            require(path.is_dir())
    return result


def model_binding(model_dir, record, gate, pipeline, record_sha, pipeline_sha):
    candidate = gate["candidate"]
    identity = record["mlflow"]
    require(gate["schema_version"] == 1 and gate["outcome"] == "passed" and not gate["reasons"]
            and gate["evidence_scope"] in ("demo_only", "independently_labeled"))
    version = identity["model_version"]
    require(isinstance(version, str) and version.isdecimal() and str(int(version)) == version and int(version) > 0)
    require(identity["exact_uri"] == f"models:/{identity['model_name']}/{version}")
    require(all(candidate[key] == value for key, value in identity.items()))
    require(candidate["run_manifest_sha256"] == record_sha and candidate["pipeline_manifest_sha256"] == pipeline_sha)
    require(record["dirty_worktree"] is False and record["evidence_scope"] == gate["evidence_scope"])
    files = inventory(model_dir)
    require(files == record["model"]["files"] and sha(canonical(files)) == candidate["model_sha256"] == record["model"]["sha256"])
    require(files["model.pkl"] == candidate["model_payload_sha256"] == record["model"]["payload_sha256"])
    code = {key.removeprefix("code/ats_mlops/"): value for key, value in files.items() if key.startswith("code/ats_mlops/")}
    require(code == record["source_sha256"] and sha(canonical(code)) == record["source_tree_sha256"] == candidate["source_tree_sha256"])
    require(all(not key.startswith("code/") or key.startswith("code/ats_mlops/") for key in files))
    require(candidate["training_git_sha"] == record["git_sha"] and re.fullmatch(r"[a-f0-9]{40}", record["git_sha"]))
    require(pipeline["outcome"] == "passed" and pipeline["training"]["run_manifest_sha256"] == record_sha
            and pipeline["training"]["model_sha256"] == candidate["model_sha256"]
            and pipeline["training"]["mlflow"] == identity)
    embedded, _ = document(Path(model_dir) / "extra_files/input-provenance.json")
    require(all(key in record and record[key] == value for key, value in embedded.items()))
    require(gate["validation"]["manifest_sha256"] == record["data_manifest_sha256"]
            and gate["validation"]["validation_sha256"] == record["data_sha256"]["validation"]
            and gate["validation"]["split_sha256"] == record["split_sha256"])
    require(gate["scoring_receipts"]["candidate"]["model_sha256"] == candidate["model_sha256"])


def release_binding(root):
    root = Path(root)
    manifest, digest = document(root / "release_manifest.json")
    require(set(manifest) == {"schema_version", "release_id", "evidence_scope", "candidate", "bento_tag",
                             "model_relative_path", "files", "gate_report_sha256", "api_schema", "api_schema_sha256",
                             "serving", "training_environment", "expected_incumbent"})
    require(manifest["schema_version"] == 1 and manifest["api_schema"] == API_SCHEMA
            and manifest["api_schema_sha256"] == sha(canonical(API_SCHEMA)))
    require(identifier(manifest["release_id"]) and re.fullmatch(r"ats_ranker:m[0-9a-f]{60}", manifest["bento_tag"])
            and manifest["bento_tag"] == "ats_ranker:m" + manifest["candidate"]["model_sha256"][:60])
    require(inventory(root, exclude=("release_manifest.json",)) == manifest["files"])
    require(manifest["model_relative_path"] == "bentoml/models/" + manifest["bento_tag"].replace(":", "/") + "/mlflow_model")
    gate, gate_sha = document(root / "model_gate_report.json")
    record, record_sha = document(root / "run_manifest.json")
    pipeline, pipeline_sha = document(root / "pipeline_manifest.json")
    require(gate_sha == manifest["gate_report_sha256"] and gate["candidate"] == manifest["candidate"]
            and gate["evidence_scope"] == manifest["evidence_scope"]
            and gate["expected_incumbent"] == manifest["expected_incumbent"]
            and record["environment"] == manifest["training_environment"])
    model_binding(root / manifest["model_relative_path"], record, gate, pipeline, record_sha, pipeline_sha)
    return manifest, digest, record
