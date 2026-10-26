"""Explicit real T17 operator phases; no callbacks, automatic A3 or deploy.

Every action except recipe has real, bounded side effects and requires a clean
published source plus the separately reviewed runtime image/overlay. This module
is never invoked by import, the monitoring DAG, or ordinary unit discovery.
"""
from collections import defaultdict
from datetime import datetime,timezone
import argparse
import base64
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import time
from urllib.request import Request,urlopen
from urllib.parse import urlencode

from . import policy as p
from .feedback import canonical,load_export,admit_feedback
from .retraining import active_state,document,file_sha,verified_calibration
from .retraining_batch import configured_batch,execution_plan,write_once


def stamp():return datetime.now(timezone.utc).isoformat()


class Exercise:
    def __init__(self,config):
        self.config=config;self.batch_config=document(config['batch_config'])
        self.batch=configured_batch(self.batch_config)
        self.repository=Path(self.batch_config['source_repository']).resolve()
        self.output=Path(config['output']).resolve();self.output.mkdir(parents=True,exist_ok=True,mode=0o700)
        p.require(config['project'].startswith('ats-t17-') and len(config['project'])<=50,'owned_project_required')
        p.identifier(config['project'])
        p.require(type(config['port']) is int and 1024<=config['port']<=65535 and config['port']!=3000,'private_port_required')
        self.project=config['project'];self.phase=None

    def run(self,command,*,timeout=180):
        result=subprocess.run(command,capture_output=True,text=True,timeout=timeout)
        if result.returncode:
            code='exercise_command_failed'
            for allowed in ('reservation_bridge_conflict','reservation_partition_violation','reservation_threshold_weakened',
                            'admission_failed','new_admission_report_required','bundle_identity_mismatch'):
                if re.search(r'\b'+allowed+r'\b',result.stderr):code=allowed;break
            write_once(self.output/('command-failure-'+str(time.time_ns())+'.json'),
                {'phase':self.phase,'exit_code':result.returncode,'reason':code,'stderr_sha256':hashlib.sha256(result.stderr.encode()).hexdigest()})
            raise p.PolicyError(code)
        return result.stdout.strip()

    def preflight(self):
        actual=self.run(['git','rev-parse','HEAD']) if Path.cwd()==self.repository else subprocess.check_output(['git','rev-parse','HEAD'],cwd=self.repository,text=True).strip()
        p.require(actual==self.batch_config['source_git_sha'],'exercise_source_mismatch')
        p.require(not subprocess.check_output(['git','status','--porcelain','--untracked-files=all'],cwd=self.repository).strip(),'clean_source_required')
        published=subprocess.check_output(['git','rev-parse','origin/feature/issue-18'],cwd=self.repository,text=True).strip()
        p.require(published==actual,'published_source_required')
        image=json.loads(self.run(['docker','image','inspect','--format','{"id":{{json .Id}},"revision":{{json (index .Config.Labels "org.opencontainers.image.revision")}}}',self.config['monitoring_image']]))
        p.require(image=={'id':self.config['monitoring_image'],'revision':actual},'monitoring_overlay_source_mismatch')
        p.require(self.batch.native.call('runtime',{})['image_id']==self.batch_config['training_image_id'],'native_image_mismatch')
        for dag in ('monitor_and_retrain','train_candidate'):
            status=self.batch.airflow._call('/api/v2/dags/'+dag)
            p.require(status['is_paused'] is False and status['max_active_runs']==1,'real_dag_not_ready')
        with active_state(self.batch.release_state) as observed:
            p.require(observed['controller'].config['port']!=self.config['port'],'working_port_forbidden')
            result={'source_git_sha':actual,'active_release_id':observed['active']['release_id'],
                'model_version':observed['record']['mlflow']['model_version'],'epoch':observed['state']['generation'],
                'journal_sha256':observed['journal_sha256']}
        return result

    def registry_versions(self):
        with active_state(self.batch.release_state) as observed:
            config=observed['controller'].config
            url=config['registry_uri']+'/api/2.0/mlflow/model-versions/search?'+urlencode({'filter':"name='"+config['model_name']+"'",'max_results':1000})
        with urlopen(url,timeout=10) as response:value=json.load(response)
        p.require(not value.get('next_page_token'),'registry_snapshot_pagination_required')
        return {item['version']:item['run_id'] for item in value.get('model_versions',[])}

    def working_events(self):
        with active_state(self.batch.release_state) as observed:volume=observed['controller'].config['events_volume']
        code="""import sqlite3,json
db=sqlite3.connect('file:/events/predictions.sqlite3?mode=ro',uri=True)
try:
 db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
 print(json.dumps({'count':db.execute('SELECT count(*) FROM prediction_events').fetchone()[0]}))
finally:db.close()
"""
        value=json.loads(self.run(['docker','run','--rm','--pull=never','--network','none','--read-only','--cpus','.5','--memory','256m',
            '--user','50000:50000','--tmpfs','/tmp:rw,size=32m,mode=1777','--mount',f'type=volume,source={volume},target=/events',
            '--entrypoint','/opt/ml-venv/bin/python',self.batch_config['packager_image'],'-I','-B','-c',code]))
        p.require(type(value['count']) is int,'invalid_working_event_count');return value['count']

    def cleanup(self):
        for kind,name in (('container',self.project+'-api'),('volume',self.project+'-events'),('network',self.project+'-net')):
            result=subprocess.run(['docker',kind,'inspect','--format',
                '{{json (index .Config.Labels "ats.t17.owner")}}' if kind=='container' else '{{json (index .Labels "ats.t17.owner")}}',name],capture_output=True,text=True)
            if result.returncode:continue
            p.require(json.loads(result.stdout)==str(self.output),'foreign_fixture_resource')
            self.run(['docker',kind,'rm',*(['-f'] if kind=='container' else []),name])
        return {'owned_fixture_cleanup':'complete'}

    def renderer(self,action,request,folder):
        request_path=self.output/(folder+'-request.json');write_once(request_path,request)
        target=self.output/folder
        p.require(not target.exists(),'new_numeric_output_required')
        self.run(['docker','run','--rm','--name',self.project+'-numeric','--pull=never','--network','none','--read-only',
            '--cpus','1','--memory','512m','--user',f'{os.getuid()}:{os.getgid()}',
            '--tmpfs','/tmp:rw,size=128m,mode=1777','--mount',f'type=bind,source={self.output},target={self.output}',
            self.config['monitoring_image'],action,'--request',str(request_path),'--output',str(target)])
        return target

    def wait_monitor(self,saved):
        run_id='monitor__'+saved['installed']['receipt_id'];deadline=time.monotonic()+180
        while time.monotonic()<deadline:
            run=self.batch.airflow.lookup('monitor_and_retrain',run_id,saved['installed'])
            if run and run['state']=='success':return self.batch.native.call('snapshot',{})
            p.require(not run or run['state']!='failed','monitor_dag_failed')
            time.sleep(1)
        raise p.PolicyError('monitor_dag_timeout')

    def cohort(self):
        """Private API/event volume, genuine numeric reports, real monitor DAGs."""
        fresh=self.preflight();p.require(not (self.output/'cohort.json').exists(),'cohort_already_recorded')
        registry_before=self.registry_versions()
        working_before=self.working_events();p.require(working_before==0,'working_events_not_zero')
        from .drift import load_reference
        from ats_mlops.pipeline import verify_bundle
        bundle=Path(self.config['baseline_bundle']);verify_bundle(bundle,document(bundle/'bundle.json')['sha256'])
        reference=Path(self.config['baseline_reference']);ref_sha=file_sha(reference/'reference.json');card,_=load_reference(reference,ref_sha)
        with active_state(self.batch.release_state) as observed:
            p.require(card['model']['release_id']==observed['active']['release_id']
                and card['model']['model_sha256']==observed['record']['model']['sha256'],'reference_not_active')
            package=observed['package'];manifest=document(package/'release_manifest.json');image=manifest['serving']['image']['id']
        target=self.output/'reference';shutil.copytree(reference,target)
        config=self.output/'monitoring.yaml';config.write_bytes(Path(self.config['monitoring_config']).read_bytes())
        calibration=self.renderer('calibrate',{'reference':str(target),'reference_sha256':ref_sha,'config':str(config)},'calibration')
        rows=[]
        from ats_mlops.data.dataset import load_manifest,load_partition
        metadata=load_manifest(bundle/'data_manifest.json');rows=load_partition(bundle/'data_manifest.json','validation')
        groups=defaultdict(list)
        for row in rows:groups[row['vacancy_id']].append(row)
        p.require(len(groups)>=10 and len(rows)>=200,'cohort_size_required')
        network=self.project+'-net';volume=self.project+'-events';name=self.project+'-api'
        own_network=own_volume=own_container=False
        def post(route,value):
            with urlopen(Request(f'http://127.0.0.1:{self.config["port"]}'+route,data=canonical(value),headers={'Content-Type':'application/json'}),timeout=10) as response:
                return json.load(response)
        try:
            p.require(not self.run(['docker','volume','ls','-q','--filter','name=^'+volume+'$'])
                and not self.run(['docker','ps','-aq','--filter','name=^/'+name+'$']),'fixture_resources_exist')
            self.run(['docker','network','create','--label','ats.t17.owner='+str(self.output),network]);own_network=True
            self.run(['docker','volume','create','--label','ats.t17.owner='+str(self.output),volume]);own_volume=True
            self.run(['docker','run','-d','--name',name,'--pull=never','--network',network,'--read-only','--cap-drop','ALL',
                '--label','ats.t17.owner='+str(self.output),
                '--security-opt','no-new-privileges:true','--cpus','1','--memory','1536m','--user','50000:50000',
                '--tmpfs','/tmp:rw,nosuid,nodev,uid=50000,gid=50000,size=128m',
                '--tmpfs','/runtime:rw,nosuid,nodev,uid=50000,gid=50000,size=128m',
                '-p',f'127.0.0.1:{self.config["port"]}:3000','--mount',f'type=bind,source={package},target=/release,readonly',
                '--mount',f'type=bind,source={package/"bentoml/models"},target=/models,readonly',
                '--mount',f'type=volume,source={volume},target=/events',image]);own_container=True
            deadline=time.monotonic()+60
            while True:
                try:
                    ready=post('/metadata',{})
                    if ready.get('ready'):break
                except Exception:pass
                p.require(time.monotonic()<deadline,'private_api_not_ready');time.sleep(.2)
            p.require(ready['release_id']==fresh['active_release_id'],'private_api_release_mismatch')
            start=stamp();intervals=[]
            for index in (0,1):
                for position,(vacancy,group) in enumerate(sorted(groups.items())):
                    payload={'request_id':f'{self.project}-w{index}-{position}','vacancy':{'id':vacancy,'text':group[0]['vacancy_text']},
                        'candidates':[{'id':row['candidate_id'],'text':'Python private fixture'} for row in group],'top_k':3}
                    result=post('/rank',payload)
                    p.require(result['model_version']==fresh['model_version'],'cohort_model_changed')
                end=stamp();intervals.append({'start':start,'end':end});start=end
            cutoff=stamp()
            # Linux native WAL export: same UID, readonly SQL, no macOS DB copy.
            code="""import sys,json,base64
from pathlib import Path
sys.path.insert(0,'/source/src')
from ats_mlops.monitoring.feedback import export_events
manifest=export_events('/events/predictions.sqlite3','/tmp/export',sys.argv[1])
print(json.dumps({name:base64.b64encode(Path('/tmp/export',name).read_bytes()).decode() for name in ('events.jsonl','export.json')}))
"""
            exported=json.loads(self.run(['docker','run','--rm','--pull=never','--network','none','--read-only','--cpus','.5','--memory','512m',
                '--user','50000:50000','--tmpfs','/tmp:rw,size=64m,mode=1777','--mount',f'type=volume,source={volume},target=/events',
                '--mount',f'type=bind,source={self.repository},target=/source,readonly','--entrypoint','/opt/ml-venv/bin/python',
                self.batch_config['packager_image'],'-I','-B','-c',code,cutoff]))
            directory=self.output/'export';directory.mkdir()
            for filename,content in exported.items():(directory/filename).write_bytes(base64.b64decode(content,validate=True))
            load_export(directory)
        finally:
            if own_container:self.run(['docker','rm','-f',name])
            if own_volume:self.run(['docker','volume','rm',volume])
            if own_network:self.run(['docker','network','rm',network])
        from .drift import configuration
        admit_feedback(self.output/'export',[],self.output/'feedback-empty',configuration(config)['label_sources'],ledger=self.output/'feedback.sqlite3')
        saved=[]
        for index,interval in enumerate(intervals):
            request={'reference':str(target),'reference_sha256':ref_sha,'export':str(self.output/'export'),
                'feedback':str(self.output/'feedback-empty'),'calibration':str(calibration),'calibration_sha256':file_sha(calibration/'calibration.json'),
                'config':str(config),**interval,'cutoff':cutoff}
            folder=self.renderer('report',request,'window-'+str(index));card=document(folder/'window.json')
            p.require(card['outcome']=='passed' and card['drift']['detected'] is True,'signal_window_required')
            spec={'window':str(folder),'window_sha256':file_sha(folder/'window.json'),'reference':str(target),'reference_sha256':ref_sha,
                'calibration':str(calibration),'calibration_sha256':request['calibration_sha256'],'config':str(config),
                'export':str(self.output/'export'),'feedback':str(self.output/'feedback-empty'),'bundle':str(bundle),
                'monitoring_sources':[self.batch_config['source_git_sha']]}
            write_once(self.output/('window-'+str(index)+'-inputs.json'),spec)
            dispatched=self.batch.install_window(spec,execution_plan(self.batch_config['source_git_sha'],self.batch_config['training_image_id']))
            snapshot=self.wait_monitor(dispatched);saved.append({'dispatch':dispatched,'snapshot':snapshot})
        request_id=saved[-1]['snapshot']['inflight'];request=self.batch.native.call('request',{'request_id':request_id})
        p.require(request['state']=='waiting_for_labels' and saved[-1]['snapshot']['requests']==1,'single_waiting_request_required')
        working_after=self.working_events();p.require(working_after==working_before,'working_events_changed')
        proof={'source_git_sha':fresh['source_git_sha'],'incumbent':fresh,'request_id':request_id,'run_id':request['run_id'],
            'state':request['state'],'windows':saved,'reference_sha256':ref_sha,'registry_before':registry_before,
            'training_runs_created':0,'working_events_before':working_before,'working_events_after':working_after}
        write_once(self.output/'cohort.json',proof);return proof

    def dataset(self):
        """Genuine fixed-seed admission/upload, then the real monitoring DAG."""
        self.preflight();cohort=document(self.output/'cohort.json');bundle=self.output/'bundle-v2'
        config=document(self.repository/'configs/data.json');config.update(seed=20261027,namespace=self.config['v2_namespace'])
        write_once(self.output/'v2-config.json',config)
        previous=Path(self.config['baseline_bundle']);parent=document(previous/'data_manifest.json')['reservation']['policy']['path']
        write_once(self.output/'v2-intent.json',{'source_git_sha':self.batch_config['source_git_sha'],
            'generator_config_sha256':file_sha(self.output/'v2-config.json'),'seed':20261027,
            'parent_policy_sha256':file_sha(previous/parent),'label_source':'approved_synthetic_fixture'})
        if not bundle.exists():
            self.run(['docker','run','--rm','--pull=never','--network','none','--read-only','--cpus','1','--memory','1536m',
                '--user',f'{os.getuid()}:{os.getgid()}','--tmpfs','/tmp:rw,size=512m,mode=1777','--tmpfs','/var/tmp/dvc:rw,size=128m,mode=1777',
                '--mount',f'type=bind,source={self.repository/"src"},target=/source/src,readonly','--mount',f'type=bind,source={previous},target=/parent,readonly',
                '--mount',f'type=bind,source={self.output},target=/fixture','-e','PYTHONPATH=/source/src','-e','HOME=/tmp','-e','USER=ats-admission',
                '-e','LOGNAME=ats-admission','-e','PATH=/opt/ml-venv/bin:/usr/bin:/bin','-e','OMP_NUM_THREADS=1','-e','OPENBLAS_NUM_THREADS=1',
                '--entrypoint','/opt/ml-venv/bin/python',self.batch_config['packager_image'],'-B','-m','ats_mlops','pipeline','bundle',
                '--source-root','/source','--config','/fixture/v2-config.json','--reservation-parent','/parent/'+parent,
                '--admission-report','/fixture/v2-admission.json','--output','/fixture/bundle-v2'])
        from ats_mlops.pipeline import verify_bundle
        descriptor=document(bundle/'bundle.json');verify_bundle(bundle,descriptor['sha256'])
        prefix='orchestration/bundles/'+self.config['v2_namespace']
        upload="""import boto3,json,sys,hashlib
from pathlib import Path
c=boto3.client('s3',endpoint_url='http://minio:9000');root=Path('/bundle')
descriptor=json.loads((root/'bundle.json').read_bytes())
for name in [*sorted(descriptor['files']),'bundle.json']:
 data=(root/name).read_bytes();key=sys.argv[1]+'/'+name
 try:old=c.get_object(Bucket='ats-dvc',Key=key)['Body'].read()
 except c.exceptions.NoSuchKey:old=None
 if old is None:c.put_object(Bucket='ats-dvc',Key=key,Body=data)
 else:assert old==data,'immutable_bundle_conflict'
 assert c.get_object(Bucket='ats-dvc',Key=key)['Body'].read()==data
print(json.dumps({'outcome':'passed','inventory_sha256':descriptor['sha256']}))
"""
        self.run(['docker','run','--rm','--pull=never','--network',self.batch_config['registry_transport']['network'],'--cpus','1','--memory','512m',
            '--env-file',self.config['dvc_env_file'],'--mount',f'type=bind,source={bundle},target=/bundle,readonly',
            '--entrypoint','/opt/ml-venv/bin/python',self.batch_config['packager_image'],'-I','-B','-c',upload,prefix])
        approved=self.output/'approved-at.json'
        if not approved.exists():write_once(approved,{'approved_at':p.stamp(datetime.now(timezone.utc))})
        result=self.batch.install_dataset(cohort['request_id'],previous,bundle,actor='agent-operator',
            bundle_uri='s3://ats-dvc/'+prefix+'/bundle.json',approved_at=p.parse_stamp(document(approved)['approved_at']))
        self.wait_monitor(result);request=self.batch.native.call('request',{'request_id':cohort['request_id']})
        p.require(request['state']=='queued','same_request_not_queued')
        proof={'request_id':request['request_id'],'run_id':request['run_id'],'state':request['state'],'dataset':request['dataset'],'dispatch':result}
        write_once(self.output/'dataset.json',proof);return proof

    def advance(self):
        self.preflight();request_id=document(self.output/'cohort.json')['request_id']
        result=self.batch.advance(request_id)
        # One invocation only; operator repeats the same command/IDs while DAG
        # is running. Failed/ambiguous results never clear/refit the T10 run.
        if result.get('state') in {'waiting_approval','gate_rejected','terminal_failed'}:
            request=self.batch.native.call('request',{'request_id':request_id})
            before=document(self.output/'cohort.json')['registry_before'];after=self.registry_versions()
            created={version:run for version,run in after.items() if version not in before}
            p.require(len(created)<=1 and (request['state']=='terminal_failed' or len(created)==1),'exactly_one_new_model_run_required')
            p.require(request['candidate'] is None or request['candidate']['model']['run_id'] in created.values(),'registered_candidate_mismatch')
            result={'request_id':request_id,'run_id':request['run_id'],'state':request['state'],
                'candidate':request['candidate'],'registry_created':created,'refits_on_retry':0}
            write_once(self.output/'candidate.json',result)
        return result

    def reference_ready(self):
        """Real model-own guard/features + provider calibration BEFORE root A3."""
        self.preflight();cohort=document(self.output/'cohort.json');request=self.batch.native.call('request',{'request_id':cohort['request_id']})
        p.require(request['state']=='waiting_approval','candidate_not_waiting')
        from .drift import build_reference,configuration,load_reference
        directory=Path(self.batch_config['artifacts'])/request['request_id'];reference=self.output/'reference-v2'
        if not reference.exists():
            manifest=document(directory/'package/release_manifest.json')
            build_reference({'bundle':str(self.output/'bundle-v2'),'bundle_sha256':document(self.output/'bundle-v2/bundle.json')['sha256'],
                'package':str(directory/'package'),'release_check':str(directory/'release-check.json'),
                'release_manifest_sha256':file_sha(directory/'package/release_manifest.json'),'scope':'approved_training',
                'dataset_approval':str(directory/'dataset-approval.json')},reference)
        reference_sha=file_sha(reference/'reference.json');calibration=self.output/'calibration-v2'
        if not calibration.exists():self.renderer('calibrate',{'reference':str(reference),'reference_sha256':reference_sha,'config':str(self.output/'monitoring.yaml')},'calibration-v2')
        config,config_sha=configuration(self.output/'monitoring.yaml',with_sha=True);_,matrices=load_reference(reference,reference_sha)
        spec={'reference_sha256':reference_sha,'calibration':str(calibration),'calibration_sha256':file_sha(calibration/'calibration.json'),
              'monitoring_sources':[self.batch_config['source_git_sha']]}
        _,effective=verified_calibration(spec,{'config':config,'config_sha':config_sha,'reference_matrices':matrices},self.repository)
        p.require(effective==request['incumbent']['cohort']['effective_config_sha256'],'reference_effective_rule_changed')
        value={'request_id':request['request_id'],'reference':str(reference),'reference_sha256':reference_sha,'bundle':str(self.output/'bundle-v2'),
            'config':str(self.output/'monitoring.yaml'),'calibration':str(calibration),'calibration_sha256':spec['calibration_sha256'],
            'monitoring_sources':spec['monitoring_sources']}
        write_once(self.output/'reference-inputs.json',value)
        proof={'state':'waiting_separate_root_A3','release_id':request['candidate']['release_id'],'effective_config_sha256':effective,
            'reference_sha256':reference_sha,'guard_report_sha256':file_sha(reference/'data-training-guard.json')}
        write_once(self.output/'reference-ready.json',proof);return proof

    def reference(self):
        self.preflight();value=document(self.output/'reference-inputs.json')
        result=self.batch.update_reference(**value)
        p.require(result['state']=='active','reference_not_reconciled')
        write_once(self.output/'reference-active.json',result);return result

    def recipe(self):
        steps=['preflight','cohort','dataset','advance','reference-ready','reference']
        return {'outcome':'prepared_only','steps':steps,'advance':'Repeat exact command until waiting_approval; no new IDs/refit on failure.',
            'A3':'Between reference-ready and reference: root separately approves/activates the named staged release using existing T13.',
            'source_overlay':'Root builds/installs reviewed source+DAG and monitoring overlays before preflight; SQL0active first.',
            'effects_executed':0}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=('recipe','preflight','cohort','dataset','advance','reference-ready','reference','cleanup'))
    parser.add_argument('--config',type=Path,required=True);args=parser.parse_args(argv)
    exercise=None
    try:
        exercise=Exercise(document(args.config));exercise.phase=args.action
        result=getattr(exercise,args.action.replace('-','_'))()
        print(json.dumps({'outcome':'passed','phase':args.action,'result':result},sort_keys=True));return 0
    except Exception as failure:
        reason=str(failure) if isinstance(failure,p.PolicyError) else 'actual_exercise_failed'
        receipt={'outcome':'failed','phase':args.action,'reason':reason,'type':type(failure).__name__}
        if exercise is not None:
            path=exercise.output/(args.action+'-failure-'+str(time.time_ns())+'.json');write_once(path,receipt)
        print(json.dumps(receipt,sort_keys=True));return 1


if __name__=='__main__':raise SystemExit(main())
