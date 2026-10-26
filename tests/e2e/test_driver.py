"""Driver/collector sandbox checks only: no qualification or experiment effects."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock,patch

from ats_mlops.evaluation import experiment as e
from tests.e2e import driver
from tests.unit.test_experiment import spec


class DriverTests(unittest.TestCase):
    def test_recipe_has_zero_effects_and_no_E5_runtime_phase(self):
        stream=io.StringIO()
        with patch.object(driver.native,'environment',side_effect=AssertionError('effect')),redirect_stdout(stream):
            self.assertEqual(0,driver.main(['recipe']))
        self.assertEqual(0,e.decode(stream.getvalue())['effects'])
        self.assertNotIn('E5',driver.PHASES)

    def test_exact_source_spec_project_and_phase_authorization_precede_callbacks(self):
        value=spec()
        grant=dict(schema_version=1,scope='T18_private_qualification',actor='root-coordinator',source_git_sha=value['source_git_sha'],
                   project='ats-acceptance-toy',input_spec_sha256=e.sha(e.canonical(value)),phases=['S2'])
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'grant.json';e.immutable(path,grant)
            with patch.object(driver.native,'environment',return_value={'project':grant['project']}) as environment:
                driver.authorize(value,path,'S2');environment.assert_called_once_with(value,series='E1',qualification=False)
            for key,changed in (('source_git_sha','e'*40),('input_spec_sha256','a'*64),('actor','AI-as-human'),('phases',['E5'])):
                broken=deepcopy(grant);broken[key]=changed;other=Path(folder)/(key+'.json');e.immutable(other,broken)
                with patch.object(driver.native,'environment') as environment:
                    with self.assertRaises(e.ExperimentError):driver.authorize(value,other,'S2')
                    environment.assert_not_called()

    def test_collector_closed_paths_and_credentials_never_get_read(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder).resolve();collector=driver.Collector(root/'bundle')
            for source,name in ((root/'test.jsonl','test.jsonl'),(root/'core.env','core.env'),(root/'innocent.json','../outside.json')):
                with patch.object(Path,'read_bytes',side_effect=AssertionError('read forbidden')) as read:
                    with self.assertRaises(e.ExperimentError):collector.copy(source,name)
                    read.assert_not_called()
            (collector.root/'alias').symlink_to(root,target_is_directory=True)
            with self.assertRaises(e.ExperimentError):collector.json('alias/escape.json',{'outcome':'passed'})
            self.assertFalse((root/'escape.json').exists())

    def test_collector_hashes_exact_bytes_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder).resolve();source=root/'input.json';source.write_bytes(b'{"value":0}\n')
            collector=driver.Collector(root/'bundle');reference=collector.copy(source,'native/input.json')
            self.assertEqual(e.sha(source.read_bytes()),reference['sha256'])
            with self.assertRaises(e.ExperimentError):collector.copy(source,'native/input.json')
            self.assertEqual(source.read_bytes(),(collector.root/'native/input.json').read_bytes())

    def test_collector_does_not_set_expected_exit_to_arbitrary_observed_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder).resolve();case=root/'case';case.mkdir();collector=driver.Collector(root/'bundle')
            e.immutable(case/'commands.json',{'commands':[{'argv':['toy'],'exit_code':7,'accepted_exit_codes':[0]}]})
            with self.assertRaisesRegex(e.ExperimentError,'unexpected_native_command_exit'):collector.commands(case,'case')

if __name__=='__main__':unittest.main()
