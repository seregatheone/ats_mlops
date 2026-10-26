"""Apply a host-verified immutable receipt to the native policy ledger.

The host batch verifier owns T16 artifact/ACTIVE checks and T11/T12/T13 work.
This DAG has no Docker socket, host release-state mount or provider environment.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess

from airflow.sdk import DAG, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException


with DAG(dag_id='monitor_and_retrain',schedule=None,start_date=datetime(2026,1,1,tzinfo=timezone.utc),
         catchup=False,max_active_runs=1,max_active_tasks=1,tags=['ats','monitoring','retraining']):

    @task(retries=2,retry_delay=timedelta(seconds=5))
    def apply_verified_window():
        conf = get_current_context()['dag_run'].conf or {}
        if (set(conf) != {'receipt_id','receipt_sha256'} or any(type(value) is not str
                or re.fullmatch(r'[0-9a-f]{64}',value) is None for value in conf.values())):
            raise AirflowFailException('verified_receipt_required')
        env = {'PYTHONPATH':'/opt/project/src','PATH':'/opt/ml-venv/bin:/usr/bin:/bin',
               'PYTHONDONTWRITEBYTECODE':'1','HOME':'/tmp'}
        result = subprocess.run(['/opt/ml-venv/bin/python','-B','-m','ats_mlops.monitoring.policy_worker',
                                 'apply','--root','/state/retraining'],input=json.dumps(conf),
                                capture_output=True,text=True,env=env)
        try:
            body = json.loads(result.stdout)
            base = Path('/opt/project/src/ats_mlops/monitoring')
            expected = {key:hashlib.sha256((base/name).read_bytes()).hexdigest()
                        for key,name in (('worker_sha256','policy_worker.py'),('policy_sha256','policy.py'))}
            if result.returncode or body['outcome'] != 'passed' or any(body[key] != value for key,value in expected.items()):
                raise ValueError
            data = body['result']
            return {key:data[key] for key in ('request_id','state') if key in data}
        except (ValueError,KeyError,TypeError,OSError):
            raise AirflowFailException('policy_receipt_failed') from None

    apply_verified_window()
