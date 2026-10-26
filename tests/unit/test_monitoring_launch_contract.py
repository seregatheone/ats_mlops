"""Cohort fixture record crosses the real DockerRuntime config boundary."""
from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.evaluation.experiment import canonical, sha
from ats_mlops.evaluation.native_monitoring import fixture_launch_document
from ats_mlops.release.controller import DEFAULT_CONFIG
from ats_mlops.release.runtime import DockerRuntime
from ats_mlops.release.state import ReleaseError


class ReachedDockerBoundary(RuntimeError):pass


class MonitoringLaunchContractTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory(prefix="ats-cohort-launch-contract-")
        self.addCleanup(self.directory.cleanup);self.root=Path(self.directory.name)
        self.config=deepcopy(DEFAULT_CONFIG);self.config['port']=13001
        self.image='sha256:'+'1'*64;self.reference={'launch_path':'fixture-launch.json'}

    def save(self,value):
        (self.root/'fixture-launch.json').write_text(json.dumps(value))

    def test_campaign_document_passes_actual_runtime_config_check_before_Docker(self):
        self.save(fixture_launch_document(self.config,self.image,'release/package'))
        runtime=DockerRuntime(self.root,self.config)
        with patch('ats_mlops.release.runtime.docker',side_effect=ReachedDockerBoundary) as operation:
            with self.assertRaises(ReachedDockerBoundary):runtime.launch(self.reference)
        self.assertEqual(1,operation.call_count)
        self.assertEqual(('volume','ls','-q'),operation.call_args.args[:3])

    def test_historical_newline_encoding_fails_before_any_Docker_operation(self):
        self.save({'schema_version':1,'package_path':'release/package','image_id':self.image,
            'config_sha256':sha(canonical(self.config))})
        with patch('ats_mlops.release.runtime.docker') as operation:
            with self.assertRaisesRegex(ReleaseError,'config_changed'):DockerRuntime(self.root,self.config).launch(self.reference)
        operation.assert_not_called()

    def test_runtime_still_rejects_changed_configuration(self):
        self.save(fixture_launch_document(self.config,self.image,'release/package'))
        changed=deepcopy(self.config);changed['port']=13002
        with patch('ats_mlops.release.runtime.docker') as operation:
            with self.assertRaisesRegex(ReleaseError,'config_changed'):DockerRuntime(self.root,changed).launch(self.reference)
        operation.assert_not_called()


if __name__=='__main__':unittest.main()
