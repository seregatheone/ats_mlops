"""Preparation metadata/clone tests using only isolated synthetic files."""
from pathlib import Path
from types import SimpleNamespace
import json
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.evaluation import experiment as e,native
from tests.e2e import prepare


class PreparationTests(unittest.TestCase):
    def test_private_ports_include_selected_28080_and_reject_busy_without_stopping(self):
        self.assertEqual(28080,prepare.PORTS['airflow']);self.assertNotIn(18080,prepare.PORTS.values())
        with patch.object(prepare.socket,'socket') as socket:
            socket.return_value.__enter__.return_value.connect_ex.return_value=0
            with self.assertRaises(e.ExperimentError):prepare.free_ports(prepare.PORTS)

    def test_readonly_image_inventory_never_requests_credentials_or_starts_container(self):
        value=dict(id='sha256:'+'a'*64,repo_digests=[],architecture='arm64',os='linux',revision=None)
        with patch.object(prepare.subprocess,'check_output',return_value=json.dumps(value).encode()) as command:
            result=prepare.inspect_image('toy-cache')
        self.assertEqual(value['id'],result['id'])
        argv=command.call_args.args[0];self.assertEqual(['docker','image','inspect'],argv[:3])
        self.assertNotIn('.Config.Env',argv[4])

    def test_actual_compose_initializer_is_pinned_without_phantom_service(self):
        images=dict.fromkeys(('postgres','minio','minio-init','mlflow'),'sha256:'+'a'*64)
        overlay=native.core_overlay(images)
        self.assertEqual({'postgres','minio','storage-init','mlflow'},set(overlay['services']))
        self.assertEqual(images['minio-init'],overlay['services']['storage-init']['image'])
        source=(Path(__file__).resolve().parents[2]/'compose.yaml').read_text()
        for name in overlay['services']:self.assertIn('\n  '+name+':\n',source)
        self.assertNotIn('minio-init',overlay['services'])

    def test_source_symlink_is_rejected_before_source_or_docker_probes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder).resolve();actual=root/'actual';actual.mkdir();alias=root/'alias';alias.symlink_to(actual,target_is_directory=True)
            with patch.object(prepare.native,'source_guard') as guard,patch.object(prepare.subprocess,'check_output') as command:
                with self.assertRaises(e.ExperimentError):prepare.preflight(alias,'a'*40,root/'out',actual,root/'manifest.json',actual)
                guard.assert_not_called();command.assert_not_called()

    def test_pre_model_inputs_cannot_be_frozen_or_contain_a_selected_model(self):
        root=Path(__file__).resolve().parents[2]
        value=native.wire_adapters(e.read(root/'configs/experiment.template.json'))
        e.validate_spec(value,template=True)
        with self.assertRaises(e.ExperimentError):e.validate_spec(value)
        self.assertIsNone(value['bindings']['model_artifact_sha256'])
        self.assertIsNone(value['qualified_model_source_git_sha'])

if __name__=='__main__':unittest.main()
