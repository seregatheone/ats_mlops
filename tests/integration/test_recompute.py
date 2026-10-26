"""Two real fresh DVC/MLflow runs plus cache/restore/invalidation on existing core.

Host entry point; no host ML packages, new storage stack or image build needed.
ATS_RUN_RECOMPUTE=1 enables the gate. A skip is not PASS.
"""

import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


def command(args, *, cwd=None, env=None, allow_failure=False):
    process = subprocess.run(args, cwd=cwd, env=env, capture_output=True, text=True)
    if process.returncode and not allow_failure:
        raise AssertionError(f"command failed ({process.returncode}): {args[0]}\n{process.stdout[-2500:]}\n{process.stderr[-4000:]}")
    return process.stdout.strip()


def read(path):
    return json.loads(Path(path).read_text())


@unittest.skipUnless(os.environ.get("ATS_RUN_RECOMPUTE") == "1", "actual recompute gate not run; skip is not PASS")
class RecomputeIntegrationTests(unittest.TestCase):
    def test_two_fresh_forced_runs_and_cache_and_invalidation(self):
        directory = Path(os.environ.get("ATS_RECOMPUTE_EVIDENCE_DIR") or tempfile.mkdtemp(prefix="ats-recompute-")).resolve()
        directory.mkdir(parents=True, exist_ok=True)
        source_sha = command(["git", "rev-parse", "HEAD"], cwd=ROOT)
        self.assertFalse(command(["git", "status", "--porcelain", "--untracked-files=all"], cwd=ROOT))
        image_ref = os.environ.get("ATS_RECOMPUTE_IMAGE", "ats-core-ml:t4")
        inspected = read_output(command(["docker", "image", "inspect", image_ref]))[0]
        identity = {"id": inspected["Id"], "reference": image_ref, "repo_digests": inspected["RepoDigests"],
                    "architecture": inspected["Architecture"], "os": inspected["Os"]}
        image_file = directory / "image.json"
        image_file.write_text(json.dumps(identity))
        bootstrap = directory / "bootstrap"
        command(["git", "clone", "--no-local", str(ROOT), str(bootstrap)])
        command(["git", "checkout", "--detach", source_sha], cwd=bootstrap)
        config = read(bootstrap / "configs/data.json")
        config["namespace"] = "ci-t9-" + source_sha[:12]
        (directory / "ci-config.json").write_text(json.dumps(config))

        def bootstrap_worker(*args):
            return command(["docker", "run", "--rm", "--network", "none", "--user", "0:0",
                            "--cpus", "1", "--memory", "1536m", "-e", "PYTHONPATH=/source/src",
                            "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
                            "-v", f"{bootstrap}:/source:ro", "-v", f"{directory}:/evidence", "-w", "/source",
                            "--entrypoint", "/opt/ml-venv/bin/python", identity["id"], *args])

        bootstrap_worker("-m", "ats_mlops", "pipeline", "bundle", "--source-root", "/source",
                         "--config", "/evidence/ci-config.json", "--output", "/evidence/bundle")
        bundle_hash = read(directory / "bundle/bundle.json")["sha256"]
        host_env = {**os.environ, "PYTHONPATH": str(ROOT / "src"), "PYTHONDONTWRITEBYTECODE": "1"}
        runs = []
        for name in ("a", "b"):
            run = directory / name
            command([sys.executable, "-m", "ats_mlops", "pipeline", "init", "--repo", str(ROOT),
                     "--git-sha", source_sha, "--directory", str(run), "--bundle", str(directory / "bundle"),
                     "--bundle-sha", bundle_hash, "--image-identity", str(image_file)], env=host_env)
            runs.append(run)

        credentials = Path(os.environ["ATS_DVC_ENV_FILE"])
        self.assertTrue(credentials.is_file())

        def worker(run, *args, credentials_file=False, allow_failure=False):
            base = ["docker", "run", "--rm", "--network", "ats-core_default", "--user", "0:0",
                    "--cpus", "1", "--memory", "1536m", "-e", "PYTHONPATH=/run/source/src",
                    "-e", "ATS_ML_PYTHON=/opt/ml-venv/bin/python", "-e", "PYTHONDONTWRITEBYTECODE=1",
                    "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
                    "-e", "MLFLOW_TRACKING_URI=http://mlflow:5000",
                    "-v", f"{run}:/run", "-v", f"{run / 'source'}:/run/source:ro", "-w", "/run/workspace"]
            if credentials_file:
                base.extend(["--env-file", str(credentials)])
            return command([*base, "--entrypoint", "/opt/ml-venv/bin/python", identity["id"], *args], allow_failure=allow_failure)

        transfer = """
import json,subprocess,sys
from pathlib import Path
def cmd(*args):subprocess.run(args,check=True,capture_output=True,text=True)
cmd('dvc','remote','add','--local','-d','core','s3://ats-dvc/data')
cmd('dvc','remote','modify','--local','core','endpointurl','http://minio:9000')
if sys.argv[1]=='push':
    cmd('dvc','commit','data/train.jsonl.dvc','data/validation.jsonl.dvc')
    cmd('dvc','push','data/train.jsonl.dvc','data/validation.jsonl.dvc')
for name in ('train','validation'):Path('data/'+name+'.jsonl').unlink()
import shutil
shutil.rmtree('.dvc/cache',ignore_errors=True)
cmd('dvc','pull','data/train.jsonl.dvc','data/validation.jsonl.dvc')
assert all(not Path('data/'+name).exists() for name in ('source.jsonl','corpus.jsonl','test.jsonl','split.json'))
print(json.dumps({'minio_selective_pull':True,'reserved_bytes_absent':True}))
"""
        transfers = [read_output(worker(run, "-c", transfer, "push" if index == 0 else "pull", credentials_file=True).splitlines()[-1])
                     for index, run in enumerate(runs)]

        count_script = """
import json,mlflow
c=mlflow.MlflowClient();e=c.get_experiment_by_name('ats-t9')
print(json.dumps(sorted(r.info.run_id for r in c.search_runs([e.experiment_id],max_results=10000)) if e else []))
"""

        def run_ids():
            return read_output(worker(runs[0], "-c", count_script).splitlines()[-1])

        def repro(run, force=False, expect_failure=False):
            args = ["-m", "ats_mlops", "pipeline", "repro", "--workspace", ".", "--source-root", "../source"]
            if force:
                args.append("--force")
            return read_output(worker(run, *args, allow_failure=expect_failure).splitlines()[-1])

        baseline_ids = run_ids()
        forced = []
        for name, run in zip(("a", "b"), runs, strict=True):
            receipt = repro(run, force=True)
            self.assertEqual(receipt["outcome"], "new_training")
            self.assertEqual(receipt["command"], ["dvc", "repro", "-f", "--no-run-cache"])
            self.assertEqual(set(receipt["events"]), {"validate", "prepare", "train", "evaluate"})
            self.assertFalse(command(["git", "status", "--porcelain"], cwd=run / "source"))
            self.assertTrue((run / "workspace/dvc.lock").exists())
            snapshot = directory / "snapshots" / name
            snapshot.parent.mkdir(exist_ok=True)
            shutil.copytree(run / "workspace/artifacts", snapshot)
            shutil.copyfile(run / "workspace/dvc.lock", directory / (name + "-dvc.lock"))
            (directory / (name + "-dag.txt")).write_text(worker(run, "-c", "import subprocess;subprocess.run(['dvc','dag'],check=True)"))
            for target, filename in (([], "full"), (["dvc.yaml"], "pipeline")):
                status = worker(run, "-c", "import subprocess,sys;subprocess.run(['dvc','status','--json',*sys.argv[1:]],check=True)", *target)
                (directory / (name + "-status-" + filename + ".json")).write_text(status + "\n")
                if filename == "pipeline":
                    self.assertEqual(read_output(status), {})
            forced.append(receipt)
        after_forced = run_ids()
        self.assertEqual(len(set(after_forced) - set(baseline_ids)), 2)
        self.assertNotEqual(forced[0]["events"]["train"]["mlflow"]["run_id"], forced[1]["events"]["train"]["mlflow"]["run_id"])
        self.compare_snapshots(directory / "snapshots/a", directory / "snapshots/b")

        unchanged = repro(runs[0])
        self.assertEqual(unchanged["outcome"], "cache_hit")
        self.assertEqual(unchanged["events"], {})
        shutil.rmtree(runs[0] / "workspace/artifacts/training")
        restored = repro(runs[0])
        self.assertEqual(restored["outcome"], "cache_hit")
        self.assertEqual(restored["events"], {})
        self.assertEqual(run_ids(), after_forced)

        params_path = runs[0] / "workspace/params.yaml"
        params = read(params_path)
        params["evaluation"]["score_decimals"] = 11
        params_path.write_text(json.dumps(params))
        evaluation_only = repro(runs[0])
        self.assertEqual(evaluation_only["outcome"], "cache_hit")
        self.assertEqual(set(evaluation_only["events"]), {"evaluate"})
        self.assertEqual(run_ids(), after_forced)
        params["model"] = {"classifier": {"max_iter": 1200}}
        params_path.write_text(json.dumps(params))
        model_changed = repro(runs[0])
        self.assertEqual(model_changed["outcome"], "new_training")
        self.assertEqual(set(model_changed["events"]), {"prepare", "train", "evaluate"})
        after_model = run_ids()
        self.assertEqual(len(set(after_model) - set(after_forced)), 1)
        raw = runs[0] / "workspace/data/train.jsonl"
        raw.write_bytes(raw.read_bytes() + b"\n")
        invalid = repro(runs[0], expect_failure=True)
        self.assertEqual(invalid["outcome"], "failed")
        self.assertNotIn("train", invalid["events"])
        self.assertEqual(run_ids(), after_model)
        self.assertFalse(command(["git", "status", "--porcelain"], cwd=runs[0] / "source"))
        evidence = {"schema_version": 1, "outcome": "passed", "evidence_scope": "demo_only",
                    "source_git_sha": source_sha, "image": identity, "bundle_sha256": bundle_hash,
                    "dvc_minio": transfers, "baseline_run_ids": baseline_ids,
                    "forced": forced, "unchanged": unchanged, "restored": restored,
                    "evaluation_only": evaluation_only, "model_changed": model_changed, "invalid_data": invalid,
                    "after_forced_run_ids": after_forced, "after_model_change_run_ids": after_model,
                    "comparison": {"prepared_hashes": "equal", "feature_hashes_vocabulary_idf": "equal",
                                   "ranks": "equal", "scores_metrics": "within atol=1e-6,rtol=1e-5"},
                    "limitations": ["Synthetic demo_only engineering evidence; no real hiring quality claim.",
                                    "Two fresh forced runs; the third independent E1 run is deferred.",
                                    "Full dvc status lists absent reserved standalone outputs; pipeline status is separately recorded."]}
        (directory / "evidence.json").write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
        print(f"Actual T9 evidence: {directory / 'evidence.json'}")

    def compare_snapshots(self, left, right):
        a = read(left / "evaluation/pipeline_manifest.json")
        b = read(right / "evaluation/pipeline_manifest.json")
        self.assertEqual(a["inputs"]["prepared_files"], b["inputs"]["prepared_files"])
        self.assertEqual(read(left / "evaluation/features.json"), read(right / "evaluation/features.json"))
        for record in (a, b):
            self.assertEqual(record["study"]["configured_budget"], 20)
            self.assertEqual(record["study"]["effective_budget"], 20)
            self.assertEqual(record["training"]["legacy_source_pointers_role"], "source_snapshot_only")
            self.assertNotEqual(record["inputs"]["binding"]["workspace_pointers"]["train"]["pointer_sha256"],
                                record["source"]["dvc_pointers"]["train.jsonl.dvc"]["pointer_sha256"])
        x, y = read(left / "evaluation/scores.json")["rows"], read(right / "evaluation/scores.json")["rows"]
        self.assertEqual(len(x), len(y))
        for first, second in zip(x, y, strict=True):
            self.assertEqual((first["vacancy_id"], first["candidate_id"]), (second["vacancy_id"], second["candidate_id"]))
            self.assertTrue(math.isclose(first["score"], second["score"], abs_tol=1e-6, rel_tol=1e-5))
        x, y = read(left / "evaluation/metrics.json"), read(right / "evaluation/metrics.json")
        for metric in ("ndcg_at_10", "recall_at_10"):
            self.assertTrue(math.isclose(x[metric], y[metric], abs_tol=1e-6, rel_tol=1e-5))
        ranks = lambda metrics: [[item["candidate_id"] for item in group["ranking"]] for group in metrics["groups"]]
        self.assertEqual(ranks(x), ranks(y))


def read_output(value):
    return json.loads(value)


if __name__ == "__main__":
    unittest.main(verbosity=2)
