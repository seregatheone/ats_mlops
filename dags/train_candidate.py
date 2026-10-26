"""One durable request invokes the audited DVC pipeline in the ML virtualenv."""
from datetime import datetime, timedelta, timezone
import json
import os
import subprocess

from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

with DAG(dag_id="train_candidate", schedule=None,
         start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False,
         max_active_runs=1, max_active_tasks=1, tags=["ats", "candidate"]):

    @task(retries=2, retry_delay=timedelta(seconds=5))
    def train_candidate():
        conf = get_current_context()["dag_run"].conf or {}
        allowed = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION", "MLFLOW_TRACKING_URI",
                   "ATS_SOURCE_REPO", "ATS_WORK_ROOT", "ATS_STATE_ROOT", "ATS_RUNTIME_IMAGE_FILE", "ATS_ORCHESTRATION_FAULT_FILE")
        env = {name: os.environ[name] for name in allowed if name in os.environ}
        env.update({"PYTHONPATH": "/opt/project/src", "PATH": "/opt/ml-venv/bin:/usr/bin:/bin",
                    "PYTHONDONTWRITEBYTECODE": "1", "HOME": "/tmp"})
        process = subprocess.run(["/opt/ml-venv/bin/python", "-m", "ats_mlops.orchestration", "run"],
                                 input=json.dumps(conf), text=True, capture_output=True, env=env)
        try:
            protocol = json.loads(process.stdout)
        except (ValueError, TypeError):
            raise AirflowFailException("invalid_runner_protocol") from None
        if process.returncode:
            if process.returncode == 75 and protocol.get("retryable") is True:
                raise RuntimeError("io_transient")
            raise AirflowFailException(protocol.get("kind", "runner_failed"))
        return protocol["result"]

    train_candidate()
