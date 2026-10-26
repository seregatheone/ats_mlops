"""Preparation/default-adapter contracts; no real Docker/API/model execution."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import Request

from ats_mlops.monitoring.exercise import DropAcceptedPost,Exercise
from ats_mlops.monitoring.retraining import AirflowAPI,NativePolicy
from ats_mlops.monitoring.retraining_batch import HostOperations,configured_batch
from ats_mlops.monitoring import policy as p


class ExercisePreparationTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory();self.addCleanup(temporary.cleanup);self.root=Path(temporary.name)
        self.batch={'source_repository':str(self.root),'source_git_sha':'a'*40,'airflow_container':'ats-core-airflow-1',
            'training_image_id':'sha256:'+'b'*64,'airflow_url':'http://127.0.0.1:8080','password_file':str(self.root/'private-passwords.json'),
            'release_state':str(self.root/'release'),'artifacts':str(self.root/'artifacts'),'serving_identity':str(self.root/'serving.json'),
            'packager_image':'sha256:'+'c'*64,'registry_transport':{'network':'ats-core_default','tracking_uri':'http://mlflow:5000'}}
        (self.root/'batch.json').write_text(json.dumps(self.batch))
        self.config={'batch_config':str(self.root/'batch.json'),'output':str(self.root/'evidence'),'project':'ats-t17-fixture','port':3018}

    def test_factory_and_recipe_use_concrete_default_adapters_without_effects(self):
        with patch('subprocess.run',side_effect=AssertionError('effect forbidden')):
            batch=configured_batch(self.batch);exercise=Exercise(self.config);recipe=exercise.recipe()
        self.assertIs(type(batch.native),NativePolicy);self.assertIs(type(batch.airflow),AirflowAPI)
        self.assertIs(type(batch.operations),HostOperations)
        self.assertEqual(0,recipe['effects_executed']);self.assertIn('reference-ready',recipe['steps'])
        self.assertFalse((self.root/'private-passwords.json').exists())

    def test_cleanup_cannot_delete_foreign_resource_with_matching_name(self):
        exercise=Exercise(self.config)
        with patch('subprocess.run',return_value=subprocess.CompletedProcess([],0,json.dumps('/other-owner'),'')) as run:
            with self.assertRaisesRegex(p.PolicyError,'foreign_fixture_resource'):exercise.cleanup()
        self.assertEqual(1,run.call_count);self.assertNotIn('rm',run.call_args.args[0])

    def test_non_private_port_and_foreign_project_rejected(self):
        for change in ({'port':3000},{'port':True},{'project':'working-demo'}):
            with self.subTest(change=change),self.assertRaises(p.PolicyError):Exercise({**self.config,**change})

    def test_source_only_context_is_committed_allowlist_without_local_state(self):
        module_path=Path(__file__).resolve().parents[2]/'docker/retraining/prepare.py'
        spec=importlib.util.spec_from_file_location('overlay_prepare',module_path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        repo=self.root/'source';repo.mkdir()
        names=['src/ats_mlops/a.py','dags/monitor_and_retrain.py','docker/monitoring/run.py',
               'docker/retraining/airflow.Dockerfile','docker/retraining/monitoring.Dockerfile','.local/core.env']
        for name in names:
            path=repo/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text('safe fixture\n')
        def git(*args):subprocess.run(['git',*args],cwd=repo,check=True,capture_output=True)
        git('init','-q');git('add','.');git('-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','-qm','fixture')
        receipt=module.prepare(repo,self.root/'context')
        self.assertEqual(set(names)-{'.local/core.env'},set(receipt['files']))
        self.assertFalse((self.root/'context/.local').exists());self.assertEqual(0,receipt['builds'])
        (repo/'src/ats_mlops/a.py').write_text('changed')
        with self.assertRaisesRegex(ValueError,'clean_committed_source_required'):module.prepare(repo,self.root/'badcontext')

    def test_monitoring_dependency_overlay_sets_actual_numeric_entrypoint(self):
        root=Path(__file__).resolve().parents[2]
        airflow=(root/'docker/retraining/airflow.Dockerfile').read_text()
        numeric=(root/'docker/retraining/monitoring.Dockerfile').read_text()
        self.assertIn('COPY dags/ /opt/airflow/dags/',airflow)
        self.assertIn('/opt/monitoring-venv/bin/python',numeric)
        self.assertIn('ENTRYPOINT',numeric)
        for content in (airflow,numeric):
            self.assertNotIn('pip install',content);self.assertNotIn('COPY . ',content)

    def test_build_requires_verified_local_base_tag_and_unchanged_committed_context(self):
        import hashlib
        root=Path(__file__).resolve().parents[2];spec=importlib.util.spec_from_file_location('overlay_build',root/'docker/retraining/build.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        content=b'FROM cached\n';context=self.root/'context';context.mkdir();name='docker/retraining/airflow.Dockerfile'
        path=context/name;path.parent.mkdir(parents=True);path.write_bytes(content)
        (context/'context.json').write_text(json.dumps({'source_git_sha':'a'*40,'files':{name:hashlib.sha256(content).hexdigest()}}))
        calls=[]
        def run(command,**kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command,0,content if command[0]=='git' else 'sha256:'+'b'*64+'\n','')
        command=module.checked_command(context,'airflow','ats-t17-base-airflow:frozen','sha256:'+'b'*64,'ats-core-airflow:t17',run=run)
        self.assertIn('BASE_IMAGE=ats-t17-base-airflow:frozen',command);self.assertIn('--pull=false',command)
        self.assertEqual(2,len(calls));self.assertFalse(any(value[:2]==['docker','build'] for value in calls))
        with self.assertRaisesRegex(ValueError,'frozen_owned_local_base_tag_required'):
            module.checked_command(context,'airflow','sha256:'+'b'*64,'sha256:'+'b'*64,'ats-core-airflow:t17',run=run)
        with self.assertRaisesRegex(ValueError,'cached_base_tag_id_mismatch'):
            module.checked_command(context,'airflow','ats-t17-base-airflow:frozen','sha256:'+'c'*64,'ats-core-airflow:t17',run=run)
        path.write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError,'overlay_context_changed'):
            module.checked_command(context,'airflow','ats-t17-base-airflow:frozen','sha256:'+'b'*64,'ats-core-airflow:t17',run=run)

    def test_transport_fault_exits_only_after_accepted_exact_real_interface_reply_and_durable_marker(self):
        # Protocol fixture only. Actual mode uses the real urllib opener and
        # Airflow backend; it never accepts a backend callback from CLI config.
        class ExitExpected(BaseException):pass
        class Response:
            status=200
            def __init__(self,body):self.body=body;self.closed=False
            def read(self,size):return json.dumps(self.body).encode()
            def close(self):self.closed=True
        marker=self.root/'marker.json';conf={'version':1}
        body={'dag_run_id':'original-run','conf':conf};request=Request('http://127.0.0.1:8080/api/v2/dags/train_candidate/dagRuns',
            data=json.dumps(body).encode(),headers={'Authorization':'Bearer private-token'},method='POST')
        response=Response(body);fault=DropAcceptedPost(marker,{'request_id':'original-request','run_id':'original-run','outbox_event_id':'original-event'})
        with patch('ats_mlops.monitoring.exercise.urlopen',return_value=response), \
             patch('ats_mlops.monitoring.exercise.os._exit',side_effect=ExitExpected) as terminate:
            with self.assertRaises(ExitExpected):fault(request,timeout=10)
        self.assertTrue(response.closed);terminate.assert_called_once_with(86)
        proof=json.loads(marker.read_bytes());self.assertEqual('original-run',proof['dag_run_id'])
        self.assertNotIn('private-token',marker.read_text());self.assertEqual(200,proof['http_status'])
        bad=Response({'dag_run_id':'original-run','conf':{'version':True}})
        with patch('ats_mlops.monitoring.exercise.urlopen',return_value=bad),patch('ats_mlops.monitoring.exercise.os._exit') as terminate:
            with self.assertRaisesRegex(p.PolicyError,'actual_post_acceptance_mismatch'):fault(request,timeout=10)
            terminate.assert_not_called()


if __name__=='__main__':unittest.main()
