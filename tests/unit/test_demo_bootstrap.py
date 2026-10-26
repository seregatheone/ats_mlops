"""Public demo namespaces over real private config files; no Docker/service IO."""
import json
from pathlib import Path
import re
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from scripts import bootstrap_core as core
from scripts import bootstrap_orchestration as airflow


class DemoBootstrapTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def core(self, project='ats-core', prefix=None, folder='core'):
        path = self.root / folder
        values = core.initialize(path, project, (19100, 19101, 15150), image_prefix=prefix)
        return path, values

    def state(self, name='airflow'):
        path = self.root / name; path.mkdir(); (path / 'source.git').mkdir()
        return path

    def test_defaults_and_legacy_credentials_stay_compatible_without_rewrite(self):
        path, first = self.core()
        expected = {'ATS_ML_IMAGE': 'ats-core-ml:t4', 'ATS_POSTGRES_IMAGE': 'ats-core-postgres:t4',
            'ATS_MINIO_IMAGE': 'ats-core-minio:source9e49d5e', 'ATS_MC_IMAGE': 'ats-core-mc:source7394ce0',
            'ATS_AIRFLOW_IMAGE': 'ats-core-airflow:t10'}
        self.assertEqual(expected, {key: first[key] for key in expected})
        names = {'ATS_IMAGE_PREFIX', *expected}
        legacy = ''.join(line + '\n' for line in (path / 'core.env').read_text().splitlines() if line.split('=', 1)[0] not in names)
        (path / 'core.env').write_text(legacy)
        before = (path / 'core.env').read_bytes()
        second = core.initialize(path, 'ats-core', (9000, 9001, 5050))
        self.assertEqual(before, (path / 'core.env').read_bytes())
        self.assertEqual(expected, {key: second[key] for key in expected})
        self.assertEqual({key: first[key] for key in core.SECRET_KEYS}, {key: second[key] for key in core.SECRET_KEYS})
        state = self.state()
        with patch.object(airflow, 'command') as operation:
            selected = airflow.configure(state, path / 'core.env', 28150)
        operation.assert_not_called()
        self.assertEqual('ats-core-airflow:t10', selected['ATS_AIRFLOW_IMAGE'])

    def test_custom_prefix_persists_without_rotating_secrets_or_ports(self):
        path, first = self.core('ats-demo-test', 'ats-demo-build')
        saved = (path / 'core.env').read_bytes()
        second = core.initialize(path, 'ats-demo-test', (9000, 9001, 5050))
        self.assertEqual(first, second); self.assertEqual(saved, (path / 'core.env').read_bytes())
        self.assertEqual('ats-demo-build-airflow:t10', second['ATS_AIRFLOW_IMAGE'])
        self.assertEqual('ats-demo-build-ml:t4', second['ATS_ML_IMAGE'])
        self.assertEqual('15150', second['MLFLOW_PORT'])
        self.assertEqual(0o600, (path / 'core.env').stat().st_mode & 0o777)
        for requested in ('ats-core', 'other-build'):
            with self.subTest(prefix=requested), self.assertRaisesRegex(ValueError, 'existing_image_prefix_mismatch'):
                core.initialize(path, 'ats-demo-test', (19100, 19001, 15050), image_prefix=requested)
        self.assertEqual(saved, (path / 'core.env').read_bytes())

    def test_invalid_prefix_and_foreign_core_project_stop_before_configuration(self):
        for prefix in ('', '../foreign', 'other/image', 'image:tag', 'UPPER', 'a\nATS_ML_IMAGE=foreign'):
            with self.subTest(prefix=prefix), self.assertRaisesRegex(ValueError, 'invalid_image_prefix'):
                core.initialize(self.root / 'invalid', 'ats-demo-test', (19100, 19101, 15150), image_prefix=prefix)
        self.assertFalse((self.root / 'invalid').exists())
        path, _ = self.core('ats-demo-test', 'ats-demo-build')
        state = self.state()
        with patch.object(airflow, 'command') as operation:
            with self.assertRaisesRegex(ValueError, 'matching_explicit_core_project_required'):
                airflow.configure(state, path / 'core.env', 28150)
        operation.assert_not_called(); self.assertFalse((state / 'passwords.json').exists())

    def test_namespace_tag_tampering_is_rejected_before_orchestration(self):
        path, _ = self.core('ats-demo-test', 'ats-demo-build')
        credentials = path / 'core.env'
        credentials.write_text(credentials.read_text().replace('ATS_AIRFLOW_IMAGE=ats-demo-build-airflow:t10', 'ATS_AIRFLOW_IMAGE=ats-core-airflow:t10'))
        with patch.object(airflow, 'command') as operation:
            with self.assertRaisesRegex(ValueError, 'saved_image_namespace_mismatch'):
                airflow.configure(self.state(), credentials, 28150, project='ats-demo-test')
        operation.assert_not_called()

    def test_existing_orchestration_state_cannot_be_rebound_to_another_core(self):
        first, _ = self.core('ats-demo-one', 'ats-build-one', 'one')
        second, _ = self.core('ats-demo-two', 'ats-build-two', 'two')
        state = self.state()
        airflow.configure(state, first / 'core.env', 28150, project='ats-demo-one')
        saved = {p.name: p.read_bytes() for p in state.iterdir() if p.is_file()}
        with patch.object(airflow, 'command') as operation:
            with self.assertRaisesRegex(ValueError, 'orchestration_state_core_mismatch'):
                airflow.configure(state, second / 'core.env', 28150, project='ats-demo-two')
        operation.assert_not_called()
        self.assertEqual(saved, {p.name: p.read_bytes() for p in state.iterdir() if p.is_file()})

    def test_unbound_existing_custom_state_requires_operator_review(self):
        path, _ = self.core('ats-demo-test', 'ats-build-test'); state = self.state()
        (state / 'image.json').write_text('{}')
        with patch.object(airflow, 'command') as operation:
            with self.assertRaisesRegex(ValueError, 'unbound_existing_orchestration_state'):
                airflow.configure(state, path / 'core.env', 28150, project='ats-demo-test')
        operation.assert_not_called(); self.assertFalse((state / 'core-binding.json').exists())

    def test_build_uses_exact_selected_project_and_persisted_image_reference(self):
        path, values = self.core('ats-demo-test', 'ats-build-test'); state = self.state()
        image = {'Id': 'sha256:' + 'a' * 64, 'RepoDigests': [], 'Architecture': 'arm64', 'Os': 'linux'}
        args = ['bootstrap_orchestration.py', 'build', '--project', 'ats-demo-test', '--core-env', str(path / 'core.env'),
                '--state-dir', str(state), '--port', '28150']
        with patch('sys.argv', args), patch.object(airflow, 'command', return_value=SimpleNamespace(stdout=json.dumps(image))) as operation:
            airflow.main()
        rows = [call.args[0] for call in operation.call_args_list]
        self.assertEqual(2, len(rows)); self.assertEqual('ats-demo-test', rows[0][rows[0].index('-p') + 1])
        self.assertEqual(['build', 'airflow'], rows[0][-2:])
        self.assertEqual(values['ATS_AIRFLOW_IMAGE'], rows[1][-1])
        recorded = json.loads((state / 'image.json').read_bytes())
        self.assertEqual(image['Id'], recorded['id']); self.assertEqual(values['ATS_AIRFLOW_IMAGE'], recorded['reference'])

    def test_compose_image_roles_resolve_defaults_and_custom_namespace(self):
        text = (Path(__file__).resolve().parents[2] / 'compose.yaml').read_text()
        roles = re.findall(r'^\s*image: \$\{(ATS_[A-Z_]+):-(.+)\}$', text, re.M)
        self.assertEqual(6, len(roles)); self.assertEqual(2, sum(name == 'ATS_AIRFLOW_IMAGE' for name, _ in roles))
        defaults = core.image_tags('ats-core'); custom = core.image_tags('ats-build-test')
        for name, fallback in roles:
            self.assertEqual(defaults[name], fallback)
            self.assertTrue(custom[name].startswith('ats-build-test-'))


if __name__ == '__main__': unittest.main()
