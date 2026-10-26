"""Preparation/default-adapter contracts; no real Docker/API/model execution."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.monitoring.exercise import Exercise
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


if __name__=='__main__':unittest.main()
