"""One-shot host batch: verify receipts, deliver outbox, stage checked candidate.

No daemon and no automatic release approval/activation. Native policy storage is
accessed only through the fixed Docker-exec helper, never host SQLite/bind mounts.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import subprocess
import tempfile

from . import policy as p
from . import policy_worker as worker
from .retraining import (AirflowAPI, NativePolicy, active_state, approved_synthetic_v2, candidate_conf,
    checked_files, document, file_sha, reference_binding, safe_name, same_incumbent, save_candidate_export, verify_window)


def write_once(path, value):
    path = Path(path); content = (p.canonical(value)+'\n').encode()
    path.parent.mkdir(parents=True,exist_ok=True)
    p.require(not path.is_symlink(), 'unsafe_output_path')
    if path.exists():
        p.require(path.read_bytes() == content, 'immutable_output_conflict')
    else:
        with path.open('xb') as stream:
            stream.write(content); stream.flush(); os.fsync(stream.fileno())
        fd = os.open(path.parent,os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    return worker.sha(content)


def proof(observed, source_git_sha, inventory):
    return {'source_git_sha':source_git_sha,'input_inventory_sha256':p.digest(inventory),
            'active_journal_sha256':observed['journal_sha256'],'active_epoch':observed['state']['generation'],
            'active_release_id':observed['active']['release_id']}


def snapshot_window_inputs(spec, store):
    """Copy only declared T16 artifacts and schema2 train/val carrier, then freeze.

    Raw reserved data paths and unexpected carrier files are rejected before
    their bytes are opened. Subsequent verification uses this immutable copy.
    """
    spec = dict(spec); files = {}
    for role, card_name in (('window','window.json'),('calibration','calibration.json')):
        card = document(Path(spec[role])/card_name)
        names = {card_name,*card['files']}
        for name in names:
            safe_name(name)
            p.require(Path(name).suffix in {'.json','.html'},'unexpected_monitoring_artifact')
            files[role+'/'+name] = (Path(spec[role])/name,Path(spec[role]))
    for role,names in (('reference',('reference.json','train.jsonl','validation.jsonl','train-groups.json','validation-groups.json')),
                       ('export',('export.json','events.jsonl')),('feedback',('feedback.json','feedback.jsonl'))):
        if role == 'reference' and document(Path(spec[role])/'reference.json')['schema_version'] == 3:
            names = (*names,'dataset-approval.json')
        for name in names:
            files[role+'/'+name] = (Path(spec[role])/name,Path(spec[role]))
    bundle = Path(spec['bundle']); descriptor = document(bundle/'bundle.json'); manifest = document(bundle/'data_manifest.json')
    from ats_mlops.pipeline import bundle_files
    p.require(set(descriptor['files']) == bundle_files(manifest),'unsafe_schema2_carrier')
    for name in {'bundle.json',*descriptor['files']}:
        safe_name(name)
        p.require(Path(name).name not in {'test.jsonl','source.jsonl','corpus.jsonl','split.json'},'reserved_carrier_body')
        files['bundle/'+name] = (bundle/name,bundle)
    files['config.json'] = (Path(spec['config']),Path(spec['config']).parent)
    contents,inventory,total = {},{},0
    for name,(path,base) in sorted(files.items()):
        p.require(not base.is_symlink(),'input_symlink')
        current = base
        for part in path.relative_to(base).parts:
            current = current/part
            p.require(not current.is_symlink(),'input_symlink')
        content = path.read_bytes(); total += len(content)
        p.require(total <= 2*worker.MAX_BYTES,'monitoring_snapshot_limit')
        contents[name],inventory[name] = content,worker.sha(content)
    identity = p.digest(inventory); store = Path(store); store.mkdir(parents=True,exist_ok=True)
    target = store/identity
    if target.exists():
        checked_files(target,inventory)
    else:
        with tempfile.TemporaryDirectory(dir=store,prefix='.snapshot-') as temporary:
            staging = Path(temporary)/'artifact'; staging.mkdir()
            for name,content in contents.items():
                path = staging/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(content); path.chmod(0o444)
            write_once(staging/'inventory.json',inventory)
            (staging/'inventory.json').chmod(0o444)
            for path in sorted(staging.rglob('*'),reverse=True):
                if path.is_dir():path.chmod(0o555)
            staging.chmod(0o555)
            os.rename(staging,target)
    for role in ('window','reference','calibration','export','feedback','bundle'):
        spec[role] = str(target/role)
    spec['config'] = str(target/'config.json')
    return spec,inventory


@contextmanager
def batch_lock(root, request_id):
    p.identifier(request_id)
    root = Path(root)/request_id
    root.mkdir(parents=True,exist_ok=True)
    with (root/'batch.lock').open('a') as stream:
        try:
            fcntl.flock(stream,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise p.PolicyError('batch_busy') from None
        try:
            yield root
        finally:
            fcntl.flock(stream,fcntl.LOCK_UN)


def execution_plan(source_git_sha, image_id):
    from ats_mlops.orchestration import digest
    model = {'schema_version':1,'feature_version':'pair-tfidf-v1','seed':20261026,
             'features':{'max_features':2000,'ngram_range':[1,1]},
             'classifier':{'C':1.0,'solver':'lbfgs','max_iter':1000}}
    parameters = {'model':model,'search':{'mode':'frozen'},'smoke':False,'evaluation':{'score_decimals':12}}
    return p.ExecutionPlan(source_git_sha,image_id,p.canonical(parameters),digest(parameters),worker.sha((p.canonical(model)+'\n').encode()))


class HostOperations:
    """Reuse T11/T12/T13; generic serving image/identity must already exist."""

    def __init__(self, source, serving_identity, *, packager_image, registry_transport, run=subprocess.run):
        self.source,self.serving_identity = Path(source).resolve(),Path(serving_identity).resolve()
        p.require(packager_image.startswith('sha256:') and len(packager_image) == 71,'invalid_packager_image')
        self.packager_image,self.transport,self.run = packager_image,registry_transport,run

    def gate(self, directory, observed, request):
        from ats_mlops.quality.gate import run_gate
        from ats_mlops.quality.artifacts import validate_model,validation_snapshot
        directory = Path(directory); final = directory/'artifacts/training/final'
        descriptor = {'model_dir':str(final/'model'),'run_manifest':str(final/'run_manifest.json'),
                      'pipeline_manifest':str(directory/'artifacts/evaluation/pipeline_manifest.json')}
        snapshot = validation_snapshot(directory/'artifacts/prepared/data_manifest.json',directory/'artifacts/prepared/admission.json',directory/'dataset-binding.json')
        validated = validate_model(descriptor,snapshot,candidate=True)
        record = validated['record']; plan = request['execution']; dataset = request['dataset']
        p.require(record['git_sha'] == plan['source_git_sha'] and record['environment']['image']['id'] == plan['training_image_id']
                  and record['params_sha256'] == plan['effective_model_sha256']
                  and record['data_manifest_sha256'] == dataset['manifest_sha256'], 'candidate_plan_mismatch')
        incumbent = {'model_dir':str(observed['package']/document(observed['package']/'release_manifest.json')['model_relative_path']),
                     'run_manifest':str(observed['package']/'run_manifest.json'),'pipeline_manifest':str(observed['package']/'pipeline_manifest.json'),
                     'release_id':observed['active']['release_id'],'release_is_fixture':False}
        intent = {'candidate':validated['public'],'incumbent':request['incumbent']['cohort'],
                  'thresholds_sha256':file_sha(self.source/'configs/quality.yaml'),
                  'quality_code':{path.name:file_sha(path) for path in (self.source/'src/ats_mlops/quality').glob('*.py')}}
        write_once(directory/'gate-intent.json',intent)
        path = directory/'model-gate.json'
        if path.exists():
            report = document(path)
            p.require(report['candidate'] == validated['public'] and report['validation']['manifest_sha256'] == dataset['manifest_sha256']
                      and report['thresholds']['file_sha256'] == intent['thresholds_sha256'], 'saved_gate_mismatch')
            if report.get('expected_incumbent'):
                p.require(report['expected_incumbent']['release_id'] == observed['active']['release_id']
                          and report['expected_incumbent']['model_sha256'] == observed['record']['model']['sha256'], 'saved_incumbent_mismatch')
            return report
        return run_gate({'validation_manifest':str(directory/'artifacts/prepared/data_manifest.json'),
            'admission_report':str(directory/'artifacts/prepared/admission.json'),'dataset_binding':str(directory/'dataset-binding.json'),
            'thresholds':str(self.source/'configs/quality.yaml'),'candidate':descriptor,'incumbent':incumbent},path)

    def package(self, directory, request):
        from service.integrity import inventory,release_binding
        directory = Path(directory).resolve()
        identity = document(self.serving_identity)
        p.require(identity['source_sha256'] == inventory(self.source/'service'), 'serving_source_mismatch')
        write_once(directory/'serving-identity.json',identity)
        target = directory/'package'
        gate_sha = file_sha(directory/'model-gate.json')
        if target.exists():
            manifest,_,_ = release_binding(target)
            p.require(manifest['gate_report_sha256'] == gate_sha and manifest['serving']['image']['id'] == identity['image']['id'], 'saved_package_mismatch')
            return manifest
        request_path = directory/'package-request.json'
        write_once(request_path,{'release_id':request['request_id']+'-r1','gate_report':str(directory/'model-gate.json'),
            'run_manifest':str(directory/'artifacts/training/final/run_manifest.json'),
            'pipeline_manifest':str(directory/'artifacts/evaluation/pipeline_manifest.json'),
            'serving_identity':str(directory/'serving-identity.json')})
        # Only a scoped model-packager worker. No Docker socket or secrets enter it.
        command = ['docker','run','--rm','--pull=never','--cpus','1','--memory','1536m',
            '--network',self.transport['network'],'--workdir',str(self.source),
            '--mount',f'type=bind,source={self.source},target={self.source},readonly',
            '--mount',f'type=bind,source={directory},target={directory}',
            '-e','PYTHONPATH='+str(self.source/'src')+':'+str(self.source),'-e','PYTHONDONTWRITEBYTECODE=1',
            '-e','MLFLOW_TRACKING_URI='+self.transport['tracking_uri'],'-e','MLFLOW_ENABLE_ARTIFACTS_PROGRESS_BAR=false',
            '-e','OMP_NUM_THREADS=1','-e','OPENBLAS_NUM_THREADS=1','--entrypoint','/opt/ml-venv/bin/python',
            self.packager_image,'-B','-m','service.package','--request',str(request_path),'--output',str(target)]
        completed = self.run(command,capture_output=True,text=True,timeout=180)
        p.require(completed.returncode == 0,'packaging_incomplete')
        return release_binding(target)[0]

    def release_check(self, directory, incumbent_version):
        from service.release_check import stage
        from ats_mlops.release.controller import load_package
        directory = Path(directory); output = directory/'release-check.json'
        if not output.exists():
            p.require(not (directory/'release-check-intent.json').exists(),'release_check_recovery_required')
            write_once(directory/'release-check-intent.json',{'package_sha256':file_sha(directory/'package/release_manifest.json')})
            stage({'package':str(directory/'package'),'validation_manifest':str(directory/'artifacts/prepared/data_manifest.json'),
                'admission_report':str(directory/'artifacts/prepared/admission.json'),'dataset_binding':str(directory/'dataset-binding.json'),
                'alias_other_version':incumbent_version,'registry_transport':self.transport},output)
        report = document(output)
        if report['outcome'] == 'passed':
            load_package(directory/'package',output)
        return report


class HostBatch:
    def __init__(self, *, native, airflow, release_state, artifacts, source_git_sha, source_repository, operations=None):
        self.native,self.airflow,self.release_state = native,airflow,Path(release_state)
        self.artifacts,self.source_git_sha,self.repository,self.operations = Path(artifacts),source_git_sha,Path(source_repository),operations

    def install_window(self, spec, execution):
        spec,inventory = snapshot_window_inputs(spec,self.artifacts/'monitoring-inputs')
        with active_state(self.release_state) as observed:
            window,reference = verify_window(spec,observed,source_repository=self.repository)
            receipt = {'schema_version':1,'kind':'window','value':{'window':p.payload(window),'reference':p.payload(reference),
                       'execution':p.payload(execution)},'proof':proof(observed,self.source_git_sha,inventory)}
            installed = self.native.call('install',{'receipt':receipt})
        acknowledgement,_ = self.airflow.trigger('monitor_and_retrain','monitor__'+installed['receipt_id'],installed)
        return {'installed':installed,'dispatch_sha256':p.digest(acknowledgement)}

    def install_dataset(self, request_id, previous, current, *, actor, bundle_uri, approved_at):
        request = self.native.call('request',{'request_id':request_id})
        with active_state(self.release_state) as observed:
            same_incumbent(request['incumbent'],observed)
            p.require(file_sha(Path(previous)/'data_manifest.json') == request['incumbent']['manifest_sha256'], 'previous_dataset_mismatch')
            approval,approval_proof = approved_synthetic_v2(previous,current,actor=actor,approved_at=approved_at,bundle_uri=bundle_uri)
            p.require(approval.parent_labelset_sha256 == request['incumbent']['labelset_sha256'], 'dataset_parent_changed')
            # The artifact is immutable and actor/time remain fixed on retries.
            write_once(self.artifacts/request_id/'dataset-approval.json',{'approval':p.payload(approval),'proof':approval_proof})
            receipt = {'schema_version':1,'kind':'dataset','value':{'request_id':request_id,'approval':p.payload(approval)},
                       'proof':proof(observed,self.source_git_sha,approval_proof)}
            installed = self.native.call('install',{'receipt':receipt})
        acknowledgement,_ = self.airflow.trigger('monitor_and_retrain','monitor__'+installed['receipt_id'],installed)
        return {'installed':installed,'dispatch_sha256':p.digest(acknowledgement)}

    def advance(self, request_id):
        with batch_lock(self.artifacts,request_id) as directory:
            try:
                return self._advance(request_id,directory)
            except Exception:
                # A transport/process failure cannot establish that remote fit
                # did not commit. Hold the original IDs/slot for exact lookup.
                request = self.native.call('request',{'request_id':request_id})
                if request['state'] not in {'queued','candidate_running','recovery_required'}:
                    raise
                self.native.call('failure',{'request_id':request_id,'ambiguous':True})
                write_once(directory/'recovery.json',{'request_id':request_id,'run_id':request['run_id'],
                    'state':'recovery_required','reason':'candidate_recovery_required'})
                return {'request_id':request_id,'state':'recovery_required','reason':'candidate_recovery_required'}

    def _advance(self, request_id, directory):
        request = self.native.call('request',{'request_id':request_id})
        if request['state'] in {'waiting_for_labels','waiting_approval','reference_update_pending','active','gate_rejected','terminal_failed'}:
            return {'request_id':request_id,'state':request['state']}
        p.require(request['execution']['training_image_id'] == self.native.image_id, 'training_image_changed')
        intent = {'request_id':request_id,'run_id':request['run_id'],'execution':request['execution'],
                  'dataset_approval':request['dataset'],'incumbent':request['incumbent']}
        delivery = self.native.call('outbox',{'request_id':request_id,'kind':'candidate'})
        p.require(p.canonical(delivery['record']['intent']) == p.canonical(intent),'outbox_request_mismatch')
        p.require(self.native.call('runtime',{})['image_id'] == request['execution']['training_image_id'],'runtime_image_file_changed')
        conf = candidate_conf(intent)
        with active_state(self.release_state) as observed:
            same_incumbent(request['incumbent'],observed)
            ack,run = self.airflow.trigger('train_candidate',request['run_id'],conf)
            events = self.native.call('pending',{})
            event = next((event for event in events if event['kind'] == 'candidate' and event['request_id'] == request_id),None)
            if event is not None:
                self.native.call('ack',{'request_id':request_id,'event_id':event['event_id'],'receipt_sha256':p.digest(ack)})
        if run['state'] != 'success':
            if run['state'] == 'failed':
                self.native.call('failure',{'request_id':request_id,'ambiguous':True})
            return {'request_id':request_id,'state':'recovery_required' if run['state']=='failed' else 'candidate_running'}
        exported = self.native.call('export',{'request_id':request_id})
        if exported['state'] != 'completed':
            return {'request_id':request_id,'state':'recovery_required'}
        save_candidate_export(directory,exported)
        record = document(directory/'artifacts/training/final/run_manifest.json')
        model = p.ModelIdentity(record['mlflow']['model_name'],record['mlflow']['model_version'],record['mlflow']['run_id'],record['model']['sha256'])
        p.require(self.operations is not None,'host_operations_required')
        with active_state(self.release_state) as observed:
            same_incumbent(request['incumbent'],observed)
            gate = self.operations.gate(directory,observed,request)
            if gate['outcome'] != 'passed':
                evidence = p.CandidateEvidence(exported['candidate_receipt']['candidate_id'],model,file_sha(directory/'model-gate.json'),False)
                return self.native.call('candidate',{'request_id':request_id,'evidence':p.payload(evidence)})
            manifest = self.operations.package(directory,request)
            checked = self.operations.release_check(directory,observed['record']['mlflow']['model_version'])
            if checked['outcome'] != 'passed':
                return self.native.call('failure',{'request_id':request_id,'ambiguous':False})
            controller = observed['controller']
        # T13 takes its own lock and atomically rechecks this exact epoch.
        with controller.store.lock():
            state = controller.store.read()
            controller._consistent(state)
            p.require(state['generation'] == request['incumbent']['cohort']['epoch'],'incumbent_changed')
            existing = state['staged'].get(manifest['release_id'])
            if existing is not None:
                p.require(existing['generation'] == state['generation']
                          and existing['manifest_sha256'] == file_sha(directory/'package/release_manifest.json')
                          and existing['check_sha256'] == file_sha(directory/'release-check.json'),'saved_stage_mismatch')
                controller._validate(existing)
        if existing is None:
            staged = controller.stage(directory/'package',directory/'release-check.json',
                                        expected_generation=request['incumbent']['cohort']['epoch'])
        else:
            staged = existing
        evidence = p.CandidateEvidence(exported['candidate_receipt']['candidate_id'],model,file_sha(directory/'model-gate.json'),True,
            staged['release_id'],staged['manifest_sha256'],staged['check_sha256'],staged['image_id'])
        return self.native.call('candidate',{'request_id':request_id,'evidence':p.payload(evidence)})

    def update_reference(self, request_id, reference, reference_sha256, bundle, config_sha256):
        from .drift import load_reference
        request = self.native.call('request',{'request_id':request_id})
        with active_state(self.release_state) as observed:
            card,_ = load_reference(reference,reference_sha256)
            card = dict(card,_config_sha256=config_sha256)
            binding = reference_binding(card,reference_sha256,bundle,observed)
            active = observed['active']
            verified = p.VerifiedActive(binding,active['manifest_sha256'],active['check_sha256'],active['approval_sha256'],
                observed['journal_sha256'],active['image_id'],p.ACTIVE_CHECKS)
            receipt = {'schema_version':1,'kind':'reference','value':{'request_id':request_id,'active':p.payload(verified)},
                       'proof':proof(observed,self.source_git_sha,{'reference_sha256':reference_sha256,'config_sha256':config_sha256})}
            installed = self.native.call('install',{'receipt':receipt})
            if request['state'] == 'active':
                p.require(request['reference_intent']['reference'] == p.payload(binding),'active_reference_changed')
                return {'request_id':request_id,'state':'active'}
            if request['state'] == 'waiting_approval':
                self.native.call('reference-intent',installed)
            event = next(event for event in self.native.call('pending',{}) if event['kind']=='reference' and event['request_id']==request_id)
            return self.native.call('reconcile-reference',{**installed,'event_id':event['event_id']})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('window','dataset','advance','reference','snapshot'))
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--request',type=Path,required=True)
    args = parser.parse_args(argv)
    try:
        config = document(args.config); value = document(args.request)
        native = NativePolicy(config['airflow_container'],config['training_image_id'])
        api = AirflowAPI(config['airflow_url'],config['password_file'])
        operations = HostOperations(config['source_repository'],config['serving_identity'],packager_image=config['packager_image'],
                                    registry_transport=config['registry_transport'])
        batch = HostBatch(native=native,airflow=api,release_state=config['release_state'],artifacts=config['artifacts'],
                          source_git_sha=config['source_git_sha'],source_repository=config['source_repository'],operations=operations)
        if args.action == 'window':
            result = batch.install_window(value,execution_plan(config['source_git_sha'],config['training_image_id']))
        elif args.action == 'dataset':
            result = batch.install_dataset(value['request_id'],value['previous_bundle'],value['bundle'],actor=value['actor'],
                bundle_uri=value['bundle_uri'],approved_at=p.parse_stamp(value['approved_at']))
        elif args.action == 'advance':
            result = batch.advance(value['request_id'])
        elif args.action == 'reference':
            result = batch.update_reference(value['request_id'],value['reference'],value['reference_sha256'],value['bundle'],value['config_sha256'])
        else:
            p.require(value == {},'unexpected_fields'); result = native.call('snapshot',{})
        print(json.dumps({'outcome':'passed','result':result},sort_keys=True))
        return 0
    except Exception as failure:
        reason = str(failure) if isinstance(failure,p.PolicyError) else 'retraining_batch_failed'
        print(json.dumps({'outcome':'failed','reason':reason},sort_keys=True))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
