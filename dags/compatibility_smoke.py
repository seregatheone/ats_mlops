"""Airflow 3 LocalExecutor probe of the isolated ML environment."""

from datetime import datetime, timezone
import os
import subprocess

from airflow.sdk import DAG, get_current_context, task


with DAG(dag_id="compatibility_smoke", schedule=None,
         start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
         max_active_runs=1, tags=["compatibility", "synthetic"]):

    @task(task_id="run_compatibility", retries=0)
    def run_compatibility() -> dict[str, str]:
        context = get_current_context()
        conf = context["dag_run"].conf or {}
        git_sha = conf.get("git_sha")
        run_id = conf.get("run_id")
        if not isinstance(git_sha, str) or not isinstance(run_id, str):
            raise ValueError("dag_run.conf requires string git_sha and run_id")
        ml_python = os.environ.get("ATS_ML_PYTHON", "/opt/ml-venv/bin/python")
        if not os.path.isabs(ml_python):
            raise ValueError("ATS_ML_PYTHON must be absolute")
        command = [ml_python, "-m", "ats_mlops", "compatibility-run",
                   "--git-sha", git_sha, "--run-id", run_id]
        expected_hash = conf.get("expected_lock_sha256")
        if expected_hash is not None:
            if not isinstance(expected_hash, str):
                raise ValueError("expected_lock_sha256 must be a string")
            command.extend(["--expected-lock-sha256", expected_hash])
        env = os.environ.copy()
        env["PYTHONPATH"] = "/opt/project/src"
        env["PATH"] = os.path.dirname(ml_python) + os.pathsep + os.defpath
        # Never relay subprocess output: failures are recorded in error.json.
        subprocess.run(command, check=True, env=env,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return {"run_id": run_id, "report_path": os.path.join(
            os.environ.get("ATS_WORK_ROOT", "/work"), "reports", run_id, "report.json")}

    run_compatibility()
