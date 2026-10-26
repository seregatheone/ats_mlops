"""Real Airflow3 LocalExecutor gate on existing core; skip is not acceptance."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from urllib import error, parse, request
import uuid

ROOT = Path(__file__).resolve().parents[2]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@unittest.skipUnless(os.environ.get("ATS_RUN_AIRFLOW") == "1", "real Airflow gate not run; skip is not PASS")
class AirflowIntegrationTests(unittest.TestCase):
    def test_real_localexecutor_retry_fatal_replay_and_cache(self):
        self.state = Path(os.environ.get("ATS_AIRFLOW_STATE", ROOT / ".local/orchestration-t10")).resolve()
        self.evidence_dir = self.state / ("evidence-" + uuid.uuid4().hex[:10])
        self.evidence_dir.mkdir(parents=True, mode=0o700)
        self.number = 0
        self.evidence = {"schema_version": 1, "status": "running", "scope": "demo_only", "dag_runs": {}}
        source_sha = os.environ.get("ATS_AIRFLOW_SOURCE_SHA") or self.command(["git", "rev-parse", "HEAD"]).strip()
        self.evidence["source_git_sha"] = source_sha
        self.base = "http://127.0.0.1:" + os.environ.get("ATS_AIRFLOW_PORT", "8088")
        self.token = ""
        before = self.resources()
        try:
            self.assertFalse(self.command(["git", "status", "--porcelain", "--untracked-files=all"]).strip(), "source must be committed before runtime evidence")
            # Isolated admitted synthetic bundle prepared BEFORE any regular DAG.
            self.command(["git", "clone", "--no-local", str(ROOT), str(self.evidence_dir / "source")])
            self.command(["git", "checkout", "--detach", source_sha], cwd=self.evidence_dir / "source")
            config = json.loads((ROOT / "configs/data.json").read_text())
            namespace = "ci-t10-" + uuid.uuid4().hex[:12]
            config["namespace"] = namespace
            config["vacancy_groups"] = 20
            (self.evidence_dir / "config.json").write_text(json.dumps(config))
            image = json.loads((self.state / "image.json").read_text())
            self.evidence["image"] = image
            docker = ["docker", "run", "--rm", "--user", "0:0", "--cpus", "1", "--memory", "1536m",
                      "-e", "PATH=/opt/ml-venv/bin:/usr/bin:/bin",
                      "-e", "OMP_NUM_THREADS=1", "-e", "OPENBLAS_NUM_THREADS=1", "-e", "MKL_NUM_THREADS=1",
                      "-e", "PYTHONDONTWRITEBYTECODE=1", "-e", "PYTHONPATH=/source/src",
                      "-v", f"{self.evidence_dir / 'source'}:/source:ro", "-v", f"{self.evidence_dir}:/fixture"]
            self.command([*docker, "--network", "none", "--entrypoint", "/opt/ml-venv/bin/python", image["id"],
                          "-m", "ats_mlops", "pipeline", "bundle", "--source-root", "/source", "--config", "/fixture/config.json", "--output", "/fixture/bundle"])
            descriptor = json.loads((self.evidence_dir / "bundle/bundle.json").read_text())
            bundle_hash = descriptor["sha256"]
            self.evidence["bundle_inventory_sha256"] = bundle_hash
            prefix = "orchestration/bundles/" + namespace
            upload = """
import boto3,json,sys
from pathlib import Path
c=boto3.client('s3',endpoint_url='http://minio:9000')
for p in Path('/fixture/bundle').rglob('*'):
 if p.is_file():c.upload_file(str(p),'ats-dvc',sys.argv[1]+'/'+p.relative_to('/fixture/bundle').as_posix())
"""
            dvc_env = os.environ["ATS_DVC_ENV_FILE"]
            self.command([*docker, "--network", "ats-core_default", "--env-file", dvc_env,
                          "--entrypoint", "/opt/ml-venv/bin/python", image["id"], "-c", upload, prefix])
            # Authentication secret remains in the private state, never evidence.
            password = json.loads((self.state / "passwords.json").read_text())["operator"]
            self.token = self.http("/auth/token", method="POST", body={"username": "operator", "password": password})["access_token"]
            dag = self.http("/api/v2/dags/train_candidate")
            self.assertEqual(dag["max_active_runs"], 1)
            self.http("/api/v2/dags/train_candidate", method="PATCH", body={"is_paused": False})
            executor = self.exec("airflow", "config", "get-value", "core", "executor")
            self.assertEqual(executor.splitlines()[-1], "LocalExecutor")
            params = {"model": {}, "search": {}, "smoke": True, "evaluation": {"score_decimals": 12}}

            def conf(name):
                identifier = namespace + "-" + name
                return {"request_id": identifier, "run_id": identifier, "dataset_version": namespace,
                        "git_sha": source_sha, "bundle_uri": "s3://ats-dvc/" + prefix + "/bundle.json",
                        "bundle_inventory_sha256": bundle_hash, "params_hash": digest(params), "parameters": params}

            initial_counts = self.mlflow_ids()
            normal = conf("normal")
            self.trigger(normal, "normal", "success")
            normal_receipt = self.receipt(normal)
            self.assertEqual(normal_receipt["outcome"], "new_training")
            after_normal = self.mlflow_ids()
            self.assertEqual(len(set(after_normal) - set(initial_counts)), 1)
            self.trigger(normal, "replay", "success")
            self.assertEqual(self.receipt(normal), normal_receipt)
            self.assertEqual(self.mlflow_ids(), after_normal)
            changed = {**normal, "run_id": normal["run_id"] + "-conflict"}
            self.trigger(changed, "conflict", "failed")
            self.assertEqual(self.mlflow_ids(), after_normal)

            cached = conf("cached")
            self.trigger(cached, "cached", "success")
            cached_receipt = self.receipt(cached)
            self.assertEqual(cached_receipt["outcome"], "cache_hit")
            self.assertEqual(cached_receipt["mlflow"], normal_receipt["mlflow"])
            self.assertEqual(self.mlflow_ids(), after_normal)

            transient = conf("transient")
            # Force a distinct real fit after the one request-scoped failed download.
            transient["parameters"] = {**params, "model": {"classifier": {"max_iter": 1100}}}
            transient["params_hash"] = digest(transient["parameters"])
            exhausted = conf("exhausted")
            fault_plan = json.dumps({transient["request_id"]: 1, exhausted["request_id"]: 3})
            self.exec("python", "-c", "from pathlib import Path;Path('/state/faults.json').write_text(" + repr(fault_plan) + ")")
            self.trigger(transient, "transient", "success")
            attempts = self.attempts(transient)
            self.assertEqual([row["attempt"] for row in attempts], [1, 2])
            self.assertEqual([row["outcome"] for row in attempts], ["retryable", "completed"])
            self.assertEqual(len(set(self.mlflow_ids()) - set(after_normal)), 1)
            after_io = self.mlflow_ids()
            self.trigger(exhausted, "exhausted", "failed")
            self.assertEqual(len(self.attempts(exhausted)), 3)
            self.trigger(exhausted, "exhausted-replay", "failed")
            self.assertEqual(len(self.attempts(exhausted)), 3)
            self.assertEqual(self.mlflow_ids(), after_io)

            invalid = {**conf("invalid"), "bundle_inventory_sha256": "0" * 64}
            self.trigger(invalid, "invalid", "failed")
            self.assertEqual(len(self.attempts(invalid)), 1)
            self.assertEqual(self.mlflow_ids(), after_io)
            self.assertEqual(self.exec("test", "!", "-e", "/work/requests/" + invalid["request_id"] + "/repro-intent.json"), "")
            for item in (normal, cached, transient):
                source = "/work/requests/" + item["request_id"] + "/run/source"
                self.assertEqual(self.exec("git", "-C", source, "status", "--porcelain"), "")
                self.assertEqual(self.exec("git", "-C", source, "rev-parse", "HEAD"), source_sha)
                self.assertEqual(self.exec("sha256sum", source + "/requirements-ml.lock").split()[0],
                                 self.exec("sha256sum", "/opt/requirements-ml.lock").split()[0])
            self.evidence.update({"status": "passed", "normal": normal_receipt, "cached": cached_receipt,
                                  "transient": self.receipt(transient), "io_attempts": attempts,
                                  "exhausted_attempts": self.attempts(exhausted), "invalid_attempts": self.attempts(invalid),
                                  "mlflow_run_ids_before": initial_counts, "mlflow_run_ids_after": after_io})
        finally:
            if self.evidence["status"] == "running":
                self.evidence["status"] = "failed"
            after = self.resources()
            self.evidence.update({"resources_before": before, "resources_after": after,
                                  "limitations": ["demo_only synthetic smoke2; not capacity/peak benchmark", "ambiguous remote commit requires operator recovery"]})
            (self.evidence_dir / "report.json").write_text(json.dumps(self.evidence, indent=2, sort_keys=True))

    def command(self, args, cwd=ROOT):
        self.number += 1
        result = subprocess.run(args, cwd=cwd, text=True, capture_output=True)
        path = self.evidence_dir / (str(self.number) + ".log")
        path.write_text(result.stdout + result.stderr)
        path.chmod(0o600)
        if result.returncode:
            raise AssertionError(f"command failed ({result.returncode}); private diagnostic {path}")
        return result.stdout.strip()

    def exec(self, *args):
        return self.command(["docker", "exec", "ats-core-airflow-1", *args])

    def http(self, path, method="GET", body=None):
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        req = request.Request(self.base + path, headers=headers, method=method,
                              data=None if body is None else json.dumps(body).encode())
        try:
            with request.urlopen(req, timeout=15) as response:
                return json.loads(response.read() or "{}")
        except error.HTTPError as exc:
            raise AssertionError(f"HTTP {exc.code}: {method} {path}") from None

    def trigger(self, conf, suffix, state):
        identifier = conf["request_id"] + "-" + suffix + "-" + uuid.uuid4().hex[:6]
        path = "/api/v2/dags/train_candidate/dagRuns/" + parse.quote(identifier, safe="")
        self.http("/api/v2/dags/train_candidate/dagRuns", method="POST", body={"dag_run_id": identifier,
                  "logical_date": datetime.now(timezone.utc).isoformat(), "conf": conf})
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            result = self.http(path)
            if result["state"] in ("success", "failed"):
                break
            time.sleep(2)
        else:
            raise AssertionError("DAG did not terminate")
        self.evidence["dag_runs"][identifier] = {"state": result["state"]}
        self.assertEqual(result["state"], state, identifier)
        tasks = self.http(path + "/taskInstances")
        self.evidence["dag_runs"][identifier]["tasks"] = [{k: v.get(k) for k in ("task_id", "state", "try_number")} for v in tasks["task_instances"]]
        if state == "success":
            values = self.http(path + "/taskInstances/train_candidate/xcomEntries/return_value")
            value = values["value"]
            if isinstance(value, str):
                value = json.loads(value)
            self.assertTrue(all(k.endswith(("_id", "_uri")) and isinstance(v, str) for k, v in value.items()))
            self.evidence["dag_runs"][identifier]["xcom"] = value

    def receipt(self, conf):
        return json.loads(self.exec("cat", "/work/requests/" + conf["request_id"] + "/candidate.json"))

    def attempts(self, conf):
        code = "import sqlite3,json; c=sqlite3.connect('/state/requests.sqlite3');c.row_factory=sqlite3.Row; print(json.dumps([dict(r) for r in c.execute('SELECT attempt,phase,outcome,kind FROM attempts WHERE request_id=? ORDER BY attempt',(" + repr(conf["request_id"]) + ",))]))"
        return json.loads(self.exec("python", "-c", code))

    def mlflow_ids(self):
        code = "import mlflow,json;c=mlflow.MlflowClient();e=c.get_experiment_by_name('ats-t9');print(json.dumps(sorted(r.info.run_id for r in c.search_runs([e.experiment_id],max_results=10000)) if e else []))"
        return json.loads(self.exec("/opt/ml-venv/bin/python", "-c", code))

    def resources(self):
        names = ["ats-core-postgres-1", "ats-core-minio-1", "ats-core-mlflow-1", "ats-core-airflow-1", "rujobradar", "gitlab-runner-python"]
        result = {}
        for name in names:
            output = self.command(["docker", "inspect", "--format", '{"oom":{{json .State.OOMKilled}},"restarts":{{.RestartCount}},"memory_limit":{{.HostConfig.Memory}},"status":{{json .State.Status}}}', name])
            result[name] = json.loads(output)
        result["snapshot"] = self.command(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}", *names]).splitlines()
        return result
