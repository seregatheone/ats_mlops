"""Pinned-runtime CI phases; reuse admitted data, DVC and training APIs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import traceback


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    Path(path).write_text(json.dumps(value, sort_keys=True, allow_nan=False) + "\n")


def bundle(root):
    from ats_mlops.pipeline import create_bundle, initialize
    source = root / "checkout"
    descriptor = create_bundle(source, source / "tests/fixtures/data/ci-config.json", root / "bundle")
    initialize(source, os.environ["ATS_CI_SHA"], root / "run", root / "bundle", descriptor["sha256"], read(root / "ml-image.json"))
    workspace = root / "run/workspace"
    params = read(workspace / "params.yaml")
    params["smoke"] = True
    write(workspace / "params.yaml", params)
    # Only train/validation objects are round-tripped through this run's scoped bucket.
    def dvc(*args):
        subprocess.run(["dvc", *args], cwd=workspace, check=True, capture_output=True)
    dvc("remote", "add", "--local", "-d", "ci", "s3://ats-dvc/data")
    dvc("remote", "modify", "--local", "ci", "endpointurl", "http://minio:9000")
    paths = [workspace / "data" / (name + ".jsonl") for name in ("train", "validation")]
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    # init installs admitted bytes/pointers, not the previous admission's DVC cache.
    dvc("commit", "--force", "data/train.jsonl.dvc", "data/validation.jsonl.dvc")
    dvc("push", "data/train.jsonl.dvc", "data/validation.jsonl.dvc")
    for path in paths:
        path.unlink()
    shutil.rmtree(workspace / ".dvc/cache")
    dvc("pull", "data/train.jsonl.dvc", "data/validation.jsonl.dvc")
    assert hashes == {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths}
    write(root / "dvc-roundtrip.json", {"passed": True, "partitions": hashes, "bundle_sha256": descriptor["sha256"]})


def fit(root):
    from ats_mlops.pipeline import reproduce
    workspace, source = root / "run/workspace", root / "run/source"
    for name in ("test.jsonl", "source.jsonl", "corpus.jsonl", "split.json"):
        assert not (workspace / "data" / name).exists()
    result = reproduce(workspace, source, force=True)
    write(root / "repro.json", result)
    assert result["outcome"] == "new_training", result.get("preflight", result["events"])
    cached = reproduce(workspace, source)
    write(root / "cache.json", cached)
    assert cached["outcome"] == "cache_hit" and not cached["events"]
    study = read(workspace / "artifacts/training/study/study.json")
    assert study["outcome"] == "passed" and study["attempted_trials"] == study["effective_budget"] == 2
    write(root / "study-check.json", {"passed": True, "study_sha256": hashlib.sha256((workspace / "artifacts/training/study/study.json").read_bytes()).hexdigest()})


def alias_fixture(root):
    import mlflow
    record = read(root / "run/workspace/artifacts/training/final/run_manifest.json")
    identity = record["mlflow"]
    client = mlflow.MlflowClient()
    before = list(client.search_runs([identity["experiment_id"]]))
    # An alias-only existing-version fixture reuses the same fitted source/run.
    other = client.create_model_version(identity["model_name"], identity["source"], run_id=identity["run_id"])
    after = list(client.search_runs([identity["experiment_id"]]))
    assert len(before) == len(after) == 1
    write(root / "alias-fixture.json", {"model_version": other.version, "new_training_runs": 0, "training_runs": len(after)})


def package(root):
    from service.package import package as create_package
    create_package(read(root / "package-request.json"), root / "package")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("bundle", "fit", "alias", "package"))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    try:
        {"bundle": bundle, "fit": fit, "alias": alias_fixture, "package": package}[args.phase](args.root)
    except Exception as error:
        # No exception values, request content or environment enter this diagnostic.
        write(args.root / "worker-failure.json", {"phase": args.phase, "type": type(error).__name__,
              "frames": [{"file": Path(frame.filename).name, "line": frame.lineno}
                         for frame in traceback.extract_tb(error.__traceback__)[-6:]]})
        raise
