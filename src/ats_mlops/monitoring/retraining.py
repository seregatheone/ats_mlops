"""Host-side verification and transports for the bounded retraining batch.

No command runs at import. Provider rendering, Docker builds, release approval
and activation are deliberately outside this module's automatic operations.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import base64
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
from urllib import error, parse, request

from . import policy as p
from . import policy_worker as worker


def file_sha(path):
    path = Path(path)
    p.require(path.is_file() and not path.is_symlink(), 'unsafe_input_file')
    return worker.sha(path.read_bytes())


def document(path, expected=None):
    p.require(Path(path).is_file() and not Path(path).is_symlink(),'unsafe_input_file')
    content = Path(path).read_bytes()
    p.require(len(content) <= worker.MAX_BYTES, 'unsafe_input_file')
    value = worker.decode(content)
    if expected is not None:
        p.require(worker.sha(content) == expected, 'input_hash_mismatch')
    return value


def safe_name(value):
    p.require(isinstance(value, str) and value and '\\' not in value, 'unsafe_artifact_name')
    path = PurePosixPath(value)
    p.require(not path.is_absolute() and path.as_posix() == value and '..' not in path.parts
              and all(not part.startswith('.') for part in path.parts), 'unsafe_artifact_name')
    return value


def checked_files(directory, inventory):
    directory = Path(directory)
    p.require(isinstance(inventory, dict) and len(inventory) <= 128 and not directory.is_symlink(), 'invalid_artifact_inventory')
    for name, expected in inventory.items():
        safe_name(name); p.fingerprint(expected)
        path = directory / name
        p.require(path.resolve().is_relative_to(directory.resolve()) and file_sha(path) == expected, 'artifact_inventory_mismatch')


@contextmanager
def active_state(directory):
    """Real T13 journal/package/API/image/Registry consistency under its lock."""
    from ats_mlops.release.controller import Controller
    directory = Path(directory)
    p.require(all((directory/name).is_file() and not (directory/name).is_symlink()
                  for name in ('config.json','journal.jsonl')), 'existing_release_state_required')
    controller = Controller(directory)
    with controller.store.lock():
        state = controller.store.read()
        controller._consistent(state)
        active = state['active']
        p.require(active is not None, 'active_release_required')
        package = directory / active['package_path']
        record = document(package/'run_manifest.json')
        yield {'controller':controller,'state':state,'active':active,'record':record,
               'package':package,'journal_sha256':file_sha(directory/'journal.jsonl')}


def same_incumbent(binding, observed):
    active, identity = observed['active'], observed['record']['mlflow']
    cohort = binding['cohort']
    expected = {'name':identity['model_name'],'version':identity['model_version'],'run_id':identity['run_id'],
                'artifact_sha256':observed['record']['model']['sha256']}
    p.require(cohort['model'] == expected and cohort['release_id'] == active['release_id']
              and cohort['epoch'] == observed['state']['generation'], 'incumbent_changed')


def reference_binding(card, reference_sha256, bundle, observed):
    """A checked fixture card never substitutes for actual ACTIVE observation."""
    from ats_mlops.data.dataset import canonical_json, load_manifest
    active, record = observed['active'], observed['record']
    model = card['model']
    expected = {'model_name':record['mlflow']['model_name'],'model_version':record['mlflow']['model_version'],
                'release_id':active['release_id'],'release_manifest_sha256':active['manifest_sha256'],
                'release_check_sha256':active['check_sha256'],'model_sha256':record['model']['sha256'],
                'run_manifest_sha256':file_sha(observed['package']/'run_manifest.json'),
                'feature_schema_sha256':worker.sha(canonical_json(record['features']))}
    p.require(all(model.get(key) == value for key,value in expected.items()), 'reference_not_current_active')
    manifest = load_manifest(Path(bundle)/'data_manifest.json')
    p.require(model['lineage_status'] == 'guarded_schema2' and manifest['schema_version'] == 2
              and card['data']['manifest_sha256'] == record['data_manifest_sha256'] == file_sha(Path(bundle)/'data_manifest.json'),
              'guarded_matched_reference_required')
    labels, _ = semantic_labels(bundle)
    cohort = p.Cohort(reference_sha256,p.ModelIdentity(expected['model_name'],expected['model_version'],
        record['mlflow']['run_id'],expected['model_sha256']),active['release_id'],observed['state']['generation'],card['_config_sha256'])
    return p.ReferenceBinding(cohort,manifest['generator']['config']['namespace'],record['data_manifest_sha256'],
                              manifest['reservation']['policy']['sha256'],labels)


def semantic_labelset(rows):
    from ats_mlops.data.dataset import canonical_json
    from ats_mlops.data.reservation import token_hashes
    labels = {}
    for row in rows:
        p.require(type(row['relevance']) is int and row['relevance'] in (0,1),'invalid_independent_label')
        key = worker.sha(canonical_json({'vacancy':sorted(token_hashes(row['vacancy_text'])),
                                        'candidate':sorted(token_hashes(row['candidate_text']))}))
        p.require(key not in labels or labels[key] == row['relevance'],'semantic_label_conflict')
        labels[key] = row['relevance']
    return worker.sha(canonical_json(labels)),labels


def semantic_labels(bundle):
    """Ignore namespace/entity IDs, order and punctuation; never load test/source."""
    from ats_mlops.data.dataset import generate_rows, load_manifest, load_partition
    from ats_mlops.data.reservation import manifest_policy, validate_partitions
    from ats_mlops.data.validate import _valid_receipt
    from ats_mlops.pipeline import verify_bundle
    root = Path(bundle)
    descriptor = document(root/'bundle.json')
    verify_bundle(root, descriptor['sha256'])
    manifest = load_manifest(root/'data_manifest.json')
    p.require(manifest['schema_version'] == 2 and _valid_receipt(document(root/'admission/data-admission.json'),
              file_sha(root/'data_manifest.json')), 'approved_schema2_admission_required')
    policy = manifest_policy(root/'data_manifest.json',manifest)
    parts = {name:load_partition(root/'data_manifest.json',name) for name in ('train','validation')}
    validate_partitions(parts,manifest['duplicate_detection']['threshold'],policy)
    result,labels = semantic_labelset([row for rows in parts.values() for row in rows])
    # Recreate the pinned synthetic teacher in memory, never opening reserved
    # bodies or using model predictions as labels. Admission already binds config.
    _,teacher = semantic_labelset(generate_rows(manifest['generator']['config']))
    p.require(all(teacher.get(key) == value for key,value in labels.items()),'synthetic_teacher_mismatch')
    return result,labels


def approved_synthetic_v2(previous, current, *, actor, approved_at, bundle_uri):
    """Explicit teacher-data approval; no real human ingestion or self-labeling."""
    from ats_mlops.data.dataset import load_manifest
    from ats_mlops.data.reservation import manifest_policy
    before, old = semantic_labels(previous)
    after, new = semantic_labels(current)
    previous, current = Path(previous), Path(current)
    manifest = load_manifest(current/'data_manifest.json')
    ancestor = load_manifest(previous/'data_manifest.json')
    p.require(manifest['generator']['config']['seed'] == 20261027, 'frozen_v2_seed_required')
    policy = manifest_policy(current/'data_manifest.json',manifest)
    prior = ancestor['reservation']['policy']['sha256']
    policy_sha = manifest['reservation']['policy']['sha256']
    p.require(policy_sha == prior or policy['parent_sha256'] == prior, 'reservation_parent_mismatch')
    changed = sum(key not in old or old[key] != value for key,value in new.items())
    p.require(after != before and changed > 0, 'new_independent_labelset_required')
    proof = {'kind':'approved_synthetic_teacher_v2','actor':actor,'approved_at':p.stamp(approved_at),
             'previous_labelset_sha256':before,'labelset_sha256':after,'new_label_count':changed,
             'manifest_sha256':file_sha(current/'data_manifest.json'),'bundle_sha256':document(current/'bundle.json')['sha256']}
    return p.DatasetApproval(manifest['generator']['config']['namespace'],2,proof['manifest_sha256'],policy_sha,prior,
        bundle_uri,proof['bundle_sha256'],file_sha(current/'admission/data-admission.json'),after,before,changed,
        'approved_synthetic_fixture',p.digest(proof),actor,approved_at,True,True), proof


class MonitoringReader:
    def load(self, spec):
        try:
            from .drift import configuration, load_reference
            from .feedback import load_export, load_feedback, verified
            from .quality import assess
        except ImportError:
            raise p.PolicyError('monitoring_not_integrated') from None
        config, config_sha = configuration(spec['config'],with_sha=True)
        reference, _ = load_reference(spec['reference'],spec['reference_sha256'])
        events, export = load_export(spec['export'])
        records, feedback = load_feedback(spec['feedback'])
        return {'config':config,'config_sha':config_sha,'reference':reference,'events':events,
                'export':export,'records':records,'feedback':feedback,'assess':assess,'verified':verified}


def verify_window(spec, observed, *, reader=None, source_repository):
    required = {'window','window_sha256','reference','reference_sha256','calibration','calibration_sha256',
                'config','export','feedback','bundle','monitoring_sources'}
    p.require(set(spec) == required, 'invalid_window_inputs')
    card = document(Path(spec['window'])/'window.json',spec['window_sha256'])
    loaded = (reader or MonitoringReader()).load(spec)
    config, reference = loaded['config'], dict(loaded['reference'])
    p.require(type(card['schema_version']) is int and card['schema_version'] == 1 and card['outcome'] in ('passed','insufficient_data')
              and card['evidence_scope'] == 'demo_only' and card['test_read'] is False, 'invalid_window_card')
    checked_files(spec['window'],card['files'])
    p.require({path.name for path in Path(spec['window']).iterdir()} == set(card['files']) | {'window.json'}, 'unexpected_window_files')
    runtime = card['runtime']; source = runtime['source_git_sha']
    p.require(source in spec['monitoring_sources'] and re.fullmatch(r'[0-9a-f]{40}',source), 'unapproved_monitoring_source')
    for name in ('drift.py','feedback.py','quality.py'):
        blob = subprocess.run(['git','show',source+':src/ats_mlops/monitoring/'+name],cwd=source_repository,
                              capture_output=True,check=True).stdout
        p.require(worker.sha(blob) == runtime['code_sha256'][name], 'monitoring_source_mismatch')
    lock = subprocess.run(['git','show',source+':requirements-monitoring.lock'],cwd=source_repository,
                          capture_output=True,check=True).stdout
    p.require(worker.sha(lock) == runtime['monitoring_lock_sha256'] and runtime['python'].startswith('3.11.'), 'monitoring_runtime_mismatch')
    calibration = document(Path(spec['calibration'])/'calibration.json',spec['calibration_sha256'])
    checked_files(spec['calibration'],calibration['files'])
    p.require(calibration['outcome'] == 'passed' and calibration['runtime'] == runtime
              and calibration['reference_sha256'] == card['reference_sha256'] == spec['reference_sha256']
              and calibration['config_sha256'] == card['config_sha256'] == loaded['config_sha']
              and calibration['columns'] == config['columns'] and calibration['method'] == config['method']
              and calibration['threshold'] in config['threshold_grid'] and calibration['drift_share'] == config['drift_share'], 'calibration_binding_mismatch')
    p.require(card['model'] == reference['model'] and p.canonical(card['minimum']) == p.canonical({'pairs':200,'vacancies':10})
              and config['minimum_pairs'] == 200 and config['minimum_vacancies'] == 10, 'window_reference_mismatch')
    start,end,cutoff = (p.parse_stamp(card['interval'][key]) for key in ('start','end','cutoff'))
    p.require(start < end <= cutoff, 'invalid_window_interval')
    selected = [event for event in loaded['events'] if start <= p.parse_stamp(event['event_time']) < end
                and (event['model_version'],event['release_id']) == (card['model']['model_version'],card['model']['release_id'])]
    counts = {'requests':len(selected),'pairs':sum(len(event['candidates']) for event in selected),
              'vacancies':len({event['vacancy_id'] for event in selected}),
              'legacy_unverified_requests':sum(not loaded['verified'](event) for event in selected),
              'excluded_cohort_requests':sum(start <= p.parse_stamp(event['event_time']) < end for event in loaded['events'])-len(selected)}
    p.require(p.canonical(card['counts']) == p.canonical(counts) and counts['legacy_unverified_requests'] == 0, 'window_count_or_completeness_mismatch')
    p.require(p.parse_stamp(loaded['export']['cutoff']) >= end and card['events_sha256'] == loaded['export']['events_sha256']
              == loaded['feedback']['events_sha256'], 'window_export_mismatch')
    feedback = loaded['feedback']
    p.require(feedback['label_sources'] == sorted(config['label_sources'])
              and card['feedback'] == {'snapshot_sha256':feedback['feedback_sha256'],
                'receipt_sha256':worker.sha((p.canonical(feedback)+'\n').encode()),'ledger_id':feedback['ledger_id']}, 'window_feedback_mismatch')
    quality = loaded['assess'](loaded['events'],loaded['records'],card['interval']['cutoff'],config['label_sources'],selected_events=selected)
    p.require(p.canonical(quality) == p.canonical(card['quality']) == p.canonical(document(Path(spec['window'])/'quality.json')), 'window_quality_mismatch')
    expected_id = worker.sha((p.canonical({'reference_sha256':spec['reference_sha256'],
        'start':card['interval']['start'],'end':card['interval']['end']})+'\n').encode())
    p.require(expected_id == card['window_id'], 'window_identity_mismatch')
    defined = card['outcome'] == 'passed'
    p.require((defined and counts['pairs'] >= 200 and counts['vacancies'] >= 10 and card['drift']['status'] == 'defined'
               and type(card['drift']['detected']) is bool)
              or (not defined and (counts['pairs'] < 200 or counts['vacancies'] < 10) and card['drift']['status'] == 'undefined'
                  and card['drift']['detected'] is None), 'window_eligibility_mismatch')
    if defined:
        p.require(card['drift']['method'] == calibration['method'] and card['drift']['threshold'] == calibration['threshold']
                  and card['drift']['drift_share'] == calibration['drift_share'], 'window_drift_config_mismatch')
    reference['_config_sha256'] = loaded['config_sha']
    binding = reference_binding(reference,spec['reference_sha256'],spec['bundle'],observed)
    evidence = p.WindowEvidence(card['window_id'],start,end,binding.cohort,spec['window_sha256'],
                                counts['pairs'],counts['vacancies'],card['drift']['detected'],True)
    return evidence,binding


class NativePolicy:
    ACTIONS = {'install','apply','snapshot','pending','outbox','runtime','request','ack','candidate','failure','reference-intent','reconcile-reference','export'}

    def __init__(self, container, image_id, *, project='ats-core', run=subprocess.run):
        p.identifier(container); p.identifier(project)
        p.require(re.fullmatch(r'sha256:[0-9a-f]{64}',image_id), 'invalid_image_identity')
        self.container,self.image_id,self.project,self.run = container,image_id,project,run

    def call(self, action, value):
        p.require(action in self.ACTIONS, 'native_action_forbidden')
        template = '{"image":{{json .Image}},"project":{{json (index .Config.Labels "com.docker.compose.project")}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},"running":{{json .State.Running}}}'
        checked = self.run(['docker','inspect',self.container,'--format',template],capture_output=True,text=True,check=True,timeout=15)
        actual = worker.decode(checked.stdout)
        p.require(actual == {'image':self.image_id,'project':self.project,'service':'airflow','running':True}, 'native_container_identity_mismatch')
        command = ['docker','exec','--user','50000:0','-i',self.container,'/opt/ml-venv/bin/python','-B','-m',
                   'ats_mlops.monitoring.policy_worker',action,'--root','/state/retraining']
        response = self.run(command,input=p.canonical(value),capture_output=True,text=True,timeout=120)
        p.require(len(response.stdout) <= 2*worker.MAX_BYTES, 'native_response_limit')
        result = worker.decode(response.stdout)
        p.require(result.get('worker_sha256') == file_sha(Path(worker.__file__))
                  and result.get('policy_sha256') == file_sha(Path(p.__file__)), 'native_source_mismatch')
        p.require(response.returncode == 0 and result.get('outcome') == 'passed', 'native_operation_failed')
        return result['result']


class AirflowAPI:
    def __init__(self, base_url, password_file, *, open_url=request.urlopen):
        uri = parse.urlsplit(base_url)
        p.require(uri.scheme == 'http' and uri.hostname in {'127.0.0.1','localhost'} and not uri.username
                  and not uri.password and not uri.query and not uri.fragment and uri.path in ('','/'), 'local_airflow_api_required')
        self.base,self.password_file,self.open_url,self.token = base_url.rstrip('/'),Path(password_file),open_url,None

    def _call(self, path, *, method='GET', body=None, allow_missing=False):
        if self.token is None:
            credentials = document(self.password_file)
            token_request = request.Request(self.base+'/auth/token',data=json.dumps({'username':'operator','password':credentials['operator']}).encode(),
                                            headers={'Content-Type':'application/json'},method='POST')
            try:
                with self.open_url(token_request,timeout=15) as response:
                    self.token = worker.decode(response.read())['access_token']
            except Exception:
                raise p.PolicyError('airflow_auth_unavailable') from None
        call = request.Request(self.base+path,data=None if body is None else p.canonical(body).encode(),method=method,
                               headers={'Content-Type':'application/json','Authorization':'Bearer '+self.token})
        try:
            with self.open_url(call,timeout=20) as response:
                content = response.read(worker.MAX_BYTES+1)
                p.require(len(content) <= worker.MAX_BYTES,'airflow_response_limit')
                return worker.decode(content)
        except error.HTTPError as failure:
            if allow_missing and failure.code == 404:
                return None
            raise p.PolicyError('airflow_api_unavailable') from None
        except (OSError,ValueError):
            raise p.PolicyError('airflow_api_uncertain') from None

    def lookup(self, dag_id, run_id, conf):
        p.require(dag_id in {'train_candidate','monitor_and_retrain'}, 'dag_not_allowed'); p.identifier(run_id)
        path = '/api/v2/dags/'+dag_id+'/dagRuns/'+parse.quote(run_id,safe='')
        existing = self._call(path,allow_missing=True)
        if existing is not None:
            p.require(existing['dag_run_id'] == run_id and p.canonical(existing['conf']) == p.canonical(conf), 'dag_run_conflict')
        return existing

    def trigger(self, dag_id, run_id, conf):
        existing = self.lookup(dag_id,run_id,conf)
        if existing is None:
            try:
                self._call('/api/v2/dags/'+dag_id+'/dagRuns',method='POST',body={'dag_run_id':run_id,
                    'logical_date':datetime.now(timezone.utc).isoformat(),'conf':conf})
            except p.PolicyError:
                # Lost POST acknowledgement or conflict: lookup the SAME ID;
                # never create a second run or hash mutable response metadata.
                existing = self.lookup(dag_id,run_id,conf)
                p.require(existing is not None,'airflow_dispatch_uncertain')
            else:
                existing = self.lookup(dag_id,run_id,conf)
                p.require(existing is not None,'airflow_dispatch_uncertain')
        receipt = {'dag_id':dag_id,'dag_run_id':run_id,'conf_sha256':p.digest(conf)}
        return receipt,existing


def candidate_conf(intent):
    from ats_mlops.orchestration import validate
    plan = worker.typed(p.ExecutionPlan,intent['execution'])
    approval = worker.typed(p.DatasetApproval,intent['dataset_approval'])
    value = {'request_id':intent['request_id'],'run_id':intent['run_id'],'git_sha':plan.source_git_sha,
             'dataset_version':approval.dataset_version,'bundle_uri':approval.bundle_uri,
             'bundle_inventory_sha256':approval.bundle_inventory_sha256,'params_hash':plan.parameters_sha256,
             'parameters':worker.decode(plan.parameters_json)}
    return validate(value)


def save_candidate_export(root, result):
    p.require(result['state'] == 'completed' and len(result['files']) <= 2048, 'candidate_not_complete')
    root = Path(root); root.mkdir(parents=True,exist_ok=True)
    total = 0
    for name,item in result['files'].items():
        safe_name(name)
        p.require(name.startswith(('artifacts/prepared/','artifacts/training/','artifacts/evaluation/','data/'))
                  or name in {'dataset-binding.json','pipeline-inputs.json','params.yaml','dvc.lock'}, 'candidate_export_path_forbidden')
        p.require(Path(name).name not in {'test.jsonl','source.jsonl','corpus.jsonl','split.json'}, 'reserved_candidate_body')
        content = base64.b64decode(item['base64'],validate=True); total += len(content)
        p.require(total <= worker.MAX_BYTES and worker.sha(content) == item['sha256'], 'candidate_export_mismatch')
        path = root/name
        p.require(not path.is_symlink() and path.resolve().is_relative_to(root.resolve()), 'unsafe_export_path')
        path.parent.mkdir(parents=True,exist_ok=True)
        if path.exists():
            p.require(path.read_bytes() == content,'immutable_candidate_changed')
        else:
            with path.open('xb') as stream:
                stream.write(content)
    return root
