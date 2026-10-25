"""Actual persistent-core training and no-checkout/native+pyfunc portability.

Run on the Docker host with ATS_RUN_MLFLOW_LINEAGE=1. This reuses the existing
default core; it never builds or starts/restarts a storage stack. A skip is not
evidence. The clean committed source is cloned before executing training.
"""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
IMAGE = os.environ.get("ATS_T7_IMAGE", "ats-core-ml:t4")

PREPARE = """
from pathlib import Path
import json
from ats_mlops.data.dataset import generate_dataset
from ats_mlops.data.validate import validate_dataset
root = Path('/work')
generate_dataset(Path('/source/configs/data.json'), root/'data_manifest.json', root/'data')
assert (root/'data_manifest.json').read_bytes() == Path('/source/data_manifest.json').read_bytes()
report = validate_dataset(root/'data_manifest.json', root/'admission')
assert report['outcome'] == 'passed', report['reasons']
for name in ('test.jsonl', 'source.jsonl', 'corpus.jsonl', 'split.json'):
    (root/'data'/name).unlink()
print(json.dumps({'admission': 'passed', 'reserved_files_removed': True}))
"""

ISOLATED = """
import hashlib, importlib.util, json, os, pathlib, sys
assert sys.flags.isolated
assert importlib.util.find_spec('ats_mlops') is None
assert not any(name.startswith('ats_mlops') for name in sys.modules)
assert not any(name in os.environ for name in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','MLFLOW_TRACKING_URI'))
assert not any('/source' in path or '/opt/project' in path for path in sys.path)
import mlflow, numpy as np, pandas as pd
artifact = pathlib.Path('/artifact')
fixture = json.loads(pathlib.Path('/fixture/reference.json').read_text())
manifest = json.loads(pathlib.Path('/fixture/run_manifest.json').read_text())
def file_hash(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def canonical(value): return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\\n').encode()
files = {p.relative_to(artifact).as_posix(): file_hash(p) for p in sorted(artifact.rglob('*')) if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc'}
assert files == manifest['model']['files']
assert hashlib.sha256(canonical(files)).hexdigest() == manifest['model']['sha256']
frame = pd.DataFrame([{key: row[key] for key in ('vacancy_text','candidate_text')} for row in fixture['rows']])
if sys.argv[1] == 'native':
    model = mlflow.sklearn.load_model(str(artifact))
    probabilities = model.predict_proba(frame)
    classes = model.classes_
    vectorizer = model.named_steps['pair_features'].vectorizer_
    assert vectorizer.vocabulary_ == fixture['vocabulary']
    np.testing.assert_array_equal(vectorizer.idf_, fixture['idf'])
else:
    model = mlflow.pyfunc.load_model(str(artifact))
    probabilities = model.predict(frame)
    classes = model.metadata.metadata['classes']
from ats_mlops.model.scoring import positive_scores
import ats_mlops.features.pairs as features
assert pathlib.Path(features.__file__).resolve().is_relative_to(artifact/'code')
assert features.FEATURE_VERSION == fixture['feature_version']
for name, digest in manifest['source_sha256'].items():
    assert file_hash(artifact/'code'/'ats_mlops'/name) == digest
scores = positive_scores(probabilities, classes, expected_rows=len(fixture['rows']))
np.testing.assert_allclose(probabilities, fixture['probabilities'], rtol=0, atol=1e-12)
np.testing.assert_allclose(scores, fixture['scores'], rtol=0, atol=1e-12)
print(json.dumps({'outcome': 'passed', 'flavor': sys.argv[1], 'rows': len(scores),
                  'max_score_difference': float(np.max(np.abs(scores-np.asarray(fixture['scores'])))),
                  'feature_module': str(features.__file__), 'model_sha256': manifest['model']['sha256'],
                  'training_git_sha': manifest['git_sha'], 'isolated': True, 'credentials': False}))
"""


def command(args, *, cwd=None):
    process = subprocess.run(args, cwd=cwd, text=True, capture_output=True)
    if process.returncode:
        raise AssertionError(f"command failed ({process.returncode}): {args[0]}\n{process.stdout[-2500:]}\n{process.stderr[-5000:]}")
    return process.stdout.strip()


@unittest.skipUnless(os.environ.get("ATS_RUN_MLFLOW_LINEAGE") == "1",
                     "real MLflow lineage integration not run; skip is not a pass")
class MlflowLineageIntegrationTests(unittest.TestCase):
    def test_real_registry_and_isolated_loading_without_reserved_data(self):
        directory = Path(os.environ.get("ATS_T7_EVIDENCE_DIR", tempfile.mkdtemp(prefix="ats-t7-"))).resolve()
        directory.mkdir(parents=True, exist_ok=True)
        checkout = directory / "checkout"
        command(["git", "clone", "--no-local", str(ROOT), str(checkout)])
        source_sha = command(["git", "rev-parse", "HEAD"], cwd=checkout)
        self.assertFalse(command(["git", "status", "--porcelain"], cwd=checkout))
        inspected = json.loads(command(["docker", "image", "inspect", IMAGE]))[0]
        identity = {"id": inspected["Id"], "reference": IMAGE, "repo_digests": inspected["RepoDigests"],
                    "os": inspected["Os"], "architecture": inspected["Architecture"]}
        (directory / "image.json").write_text(json.dumps(identity))

        def worker(network, *args):
            return command(["docker", "run", "--rm", "--network", network,
                            "--user", "0:0", "--cpus", "1", "--memory", "1536m",
                            "-e", "PYTHONPATH=/source/src", "-e", "OMP_NUM_THREADS=1",
                            "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
                            "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000",
                            "-v", f"{checkout}:/source:ro", "-v", f"{directory}:/work", "-w", "/source",
                            "--entrypoint", "/opt/ml-venv/bin/python", identity["id"], *args])

        prepare = worker("none", "-c", PREPARE)
        trained = worker("ats-core_default", "-m", "ats_mlops.model.training",
                         "--manifest", "/work/data_manifest.json", "--report-dir", "/work/training",
                         "--admission-report", "/work/admission/data-admission.json",
                         "--config", "/source/configs/model.json", "--source-root", "/source",
                         "--image-identity", "/work/image.json", "--experiment", "ats-t7",
                         "--registered-model", "ats-pair-ranking")
        manifest = json.loads((directory / "training/run_manifest.json").read_text())
        self.assertEqual(manifest["git_sha"], source_sha)
        self.assertEqual(manifest["read_partitions"], ["train", "validation"])
        self.assertFalse(manifest["test_read"])
        model = directory / "training/model"
        results = []

        def isolated(artifact, mode, *, script=ISOLATED):
            return subprocess.run([
                "docker", "run", "--rm", "--network", "none", "--read-only",
                "--tmpfs", "/tmp:rw,size=64m", "--cpus", "1", "--memory", "1024m",
                "-e", "PYTHONPATH=", "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1",
                "-e", "MKL_NUM_THREADS=1", "-v", f"{artifact}:/artifact:ro",
                "-v", f"{directory / 'training'}:/fixture:ro", "-w", "/tmp",
                "--entrypoint", "/opt/ml-venv/bin/python", identity["id"], "-I", "-c", script, mode,
            ], text=True, capture_output=True)

        for mode in ("native", "pyfunc"):
            loaded = isolated(model, mode)
            self.assertEqual(loaded.returncode, 0, loaded.stderr)
            results.append(json.loads(loaded.stdout))
        missing_code = directory / "missing-code-model"
        shutil.copytree(model, missing_code)
        shutil.rmtree(missing_code / "code/ats_mlops")
        negative_script = """
import importlib.util
assert importlib.util.find_spec('ats_mlops') is None
import mlflow.sklearn
mlflow.sklearn.load_model('/artifact')
"""
        negative = isolated(missing_code, "native", script=negative_script)
        self.assertNotEqual(negative.returncode, 0)
        self.assertIn("ModuleNotFoundError", negative.stderr)
        self.assertIn("ats_mlops", negative.stderr)
        evidence = {"schema_version": 1, "outcome": "passed", "evidence_scope": "demo_only",
                    "training_git_sha": source_sha, "image": identity,
                    "run_manifest_sha256": hashlib.sha256((directory / "training/run_manifest.json").read_bytes()).hexdigest(),
                    "mlflow": manifest["mlflow"], "model_sha256": manifest["model"]["sha256"],
                    "prepare": json.loads(prepare.splitlines()[-1]), "training": json.loads(trained.splitlines()[-1]),
                    "isolated_loads": results, "missing_code_rejected": True,
                    "reserved_files_unavailable": True,
                    "limitations": manifest["limitations"]}
        (directory / "portability.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(f"Actual T7 evidence: {directory / 'portability.json'}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
