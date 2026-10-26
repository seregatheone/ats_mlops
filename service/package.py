"""Import one gate-passed numeric MLflow version and freeze a local release package."""

import argparse
import json
import os
from pathlib import Path
import shutil
import tempfile

from service.contracts import API_SCHEMA, canonical, identifier, require, sha
from service.integrity import document, inventory, model_binding, release_binding
from service.runtime import environment


def exclusive(path, content):
    with Path(path).open("xb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def package(request, output):
    output = Path(output)
    require(not output.exists(), "new_package_required")
    require(identifier(request["release_id"]))
    documents = {}
    for name, key in (("model_gate_report.json", "gate_report"), ("run_manifest.json", "run_manifest"),
                      ("pipeline_manifest.json", "pipeline_manifest")):
        content = Path(request[key]).read_bytes()
        documents[name] = (content, json.loads(content), sha(content))
    gate, gate_sha = documents["model_gate_report.json"][1:]
    record, record_sha = documents["run_manifest.json"][1:]
    pipeline, pipeline_sha = documents["pipeline_manifest.json"][1:]
    candidate = gate["candidate"]
    require(gate["outcome"] == "passed" and not gate["reasons"])
    require(candidate["exact_uri"] == f"models:/{candidate['model_name']}/{candidate['model_version']}"
            and str(int(candidate["model_version"])) == candidate["model_version"])
    require(candidate["run_manifest_sha256"] == record_sha and candidate["pipeline_manifest_sha256"] == pipeline_sha)
    serving, _ = document(request["serving_identity"])
    require(serving["image"]["id"].startswith("sha256:") and len(serving["image"]["id"]) == 71)
    require(serving["source_sha256"] == inventory(Path(__file__).parent))
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".package-", dir=output.parent) as directory:
        root = Path(directory) / "release"
        root.mkdir()
        store = Path(directory) / "store"
        # This process must not have resolved Bento's store before setting its home.
        os.environ["BENTOML_HOME"] = str(store)
        import bentoml.mlflow

        tag = "ats_ranker:m" + candidate["model_sha256"][:60]
        model = bentoml.mlflow.import_model(tag, candidate["exact_uri"],
                                          signatures={"predict": {"batchable": False}})
        require(str(model.tag) == tag)
        relative = "bentoml/models/" + tag.replace(":", "/")
        destination = root / relative
        destination.parent.mkdir(parents=True)
        shutil.copytree(model.path, destination)
        model_binding(destination / "mlflow_model", record, gate, pipeline, record_sha, pipeline_sha)
        for name, (content, _, _) in documents.items():
            exclusive(root / name, content)
        manifest = {"schema_version": 1, "release_id": request["release_id"],
                    "evidence_scope": gate["evidence_scope"], "candidate": candidate,
                    "bento_tag": tag, "model_relative_path": relative + "/mlflow_model",
                    "files": inventory(root), "gate_report_sha256": gate_sha, "api_schema": API_SCHEMA,
                    "api_schema_sha256": sha(canonical(API_SCHEMA)), "serving": serving,
                    "training_environment": record["environment"], "expected_incumbent": gate["expected_incumbent"]}
        exclusive(root / "release_manifest.json", canonical(manifest))
        release_binding(root)
        # Atomic publication of a complete immutable package. Existing output is never replaced.
        require(not output.exists(), "new_package_required")
        # mkdir is an exclusive cross-process claim, replaced atomically by the complete tree.
        output.mkdir()
        try:
            os.rename(root, output)
            fd = os.open(output, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except Exception:
            shutil.rmtree(output)
            raise
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        manifest = package(json.loads(args.request.read_text()), args.output)
        print(json.dumps({"outcome": "packaged", "release_id": manifest["release_id"],
                          "model_version": manifest["candidate"]["model_version"]}))
        return 0
    except Exception:
        print(json.dumps({"outcome": "rejected", "reason": "packaging_failed"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
