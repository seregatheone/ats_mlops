"""Protocol/boundary fixtures only; no provider, Docker, API or model effects."""

from copy import deepcopy
from contextlib import nullcontext
from dataclasses import replace
from datetime import timedelta
import base64
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from ats_mlops.monitoring import policy as p, policy_worker as worker
from ats_mlops.monitoring.retraining import AirflowAPI, NativePolicy, candidate_conf, effective_rule_sha256, file_sha, monitoring_decisions, reference_binding, save_candidate_export, semantic_labelset
from ats_mlops.monitoring.retraining_batch import HostBatch, execution_plan
from tests.unit.test_retraining_policy import START,HOUR,MINUTE,REFERENCE,MODEL,h,window,approval


def install_child(root,receipt,phase,queue=None):
    if phase=='after_write':
        original=worker.os.fdopen
        class ExitAfterWrite:
            def __init__(self,stream):self.stream=stream
            def __enter__(self):return self
            def __exit__(self,*args):self.stream.close()
            def write(self,content):self.stream.write(content);os._exit(81)
        worker.os.fdopen=lambda descriptor,mode:ExitAfterWrite(original(descriptor,mode))
    elif phase in {'before_publish','after_publish'}:
        original=worker.os.link
        def crash(*args,**kwargs):
            if phase=='after_publish':original(*args,**kwargs)
            os._exit(82)
        worker.os.link=crash
    result=worker.install(Path(root),receipt)
    if queue is not None:queue.put(result)


class AdapterTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.plan = execution_plan('a'*40,'sha256:'+h('runtime-image'))
        self.proof = {'source_git_sha':'a'*40,'input_inventory_sha256':h('inputs'),
                      'active_journal_sha256':h('journal'),'active_epoch':1,'active_release_id':'release-A'}

    def receipt(self,index):
        return {'schema_version':1,'kind':'window','value':{'window':p.payload(window(index)),
                'reference':p.payload(REFERENCE),'execution':p.payload(self.plan)},'proof':self.proof}

    def accepted(self):
        for index in (0,1):
            installed = worker.execute('install',{'receipt':self.receipt(index)},self.root)
            result = worker.execute('apply',installed,self.root,now=START+(index+1)*HOUR)
        return result['request_id']

    def queued(self):
        request_id = self.accepted()
        receipt = {'schema_version':1,'kind':'dataset','value':{'request_id':request_id,'approval':p.payload(approval())},'proof':self.proof}
        installed = worker.execute('install',{'receipt':receipt},self.root)
        worker.execute('apply',installed,self.root,now=START+2*HOUR+MINUTE)
        return request_id

    def test_execution_plan_is_bound_before_outbox_and_cannot_change_after_queue(self):
        request_id = self.queued()
        store = p.PolicyStore(self.root/'policy.sqlite3')
        request = store.request(request_id); event = store.pending_outbox()[0]
        self.assertEqual(p.payload(self.plan),request['execution'])
        self.assertEqual(request['execution'],event['intent']['execution'])
        with self.assertRaisesRegex(p.PolicyError,'execution_plan_conflict'):
            store.bind_execution(request_id,replace(self.plan,source_git_sha='b'*40),now=START+2*HOUR+MINUTE)
        conf = candidate_conf(event['intent'])
        self.assertEqual(conf['request_id'],request_id)
        self.assertEqual(conf['params_hash'],self.plan.parameters_sha256)
        self.assertEqual(conf['parameters']['model']['features']['max_features'],2000)

    def test_host_uncertain_trigger_is_durable_and_retry_preserves_original_run(self):
        request_id=self.queued(); store=p.PolicyStore(self.root/'policy.sqlite3')
        calls=[]; image=self.plan.training_image_id
        class Native:
            image_id=image
            def call(inner,action,value):
                calls.append(action)
                if action=='runtime':return {'image_id':image}
                return worker.execute(action,value,self.root,now=START+3*HOUR)
        class API:
            def trigger(inner,dag,run,conf):
                calls.append((dag,run,conf))
                raise OSError('sensitive remote response is never persisted')
        batch=HostBatch(native=Native(),airflow=API(),release_state=self.root/'release',
            artifacts=self.root/'artifacts',source_git_sha='a'*40,source_repository=self.root)
        with patch('ats_mlops.monitoring.retraining_batch.active_state',return_value=nullcontext({})), \
             patch('ats_mlops.monitoring.retraining_batch.same_incumbent'):
            first=batch.advance(request_id); after=store.request(request_id)
            count=store.snapshot()
            self.assertEqual(first,batch.advance(request_id))
        self.assertEqual('recovery_required',after['state'])
        self.assertEqual(count,store.snapshot())
        triggers=[value for value in calls if isinstance(value,tuple)]
        self.assertEqual(2,len(triggers));self.assertEqual(triggers[0],triggers[1])
        self.assertEqual(after['run_id'],triggers[0][1])
        self.assertEqual(1,len(store.pending_outbox()))
        recovery=(self.root/'artifacts'/request_id/'recovery.json').read_text()
        self.assertNotIn('sensitive',recovery)
        self.assertEqual('candidate_recovery_required',json.loads(recovery)['reason'])

    def test_ingress_rejects_reserved_path_alias_before_manifest_loader_or_body_reads(self):
        from ats_mlops import pipeline
        from ats_mlops.lineage.manifest import inventory_hash
        manifest={'schema_version':2,'artifacts':{key:{'path':'data/'+key+'.jsonl'} for key in ('source','corpus','train','validation','test','split_lists')},
                  'reservation':{'policy':{'path':'data/reservation_policy.json'}}}
        manifest['artifacts']['train']['path']=manifest['artifacts']['test']['path']
        files={key:'a'*64 for key in {'data_manifest.json','admission/data-admission.json','dataset-binding.json',
            *pipeline.POINTERS.values(),'data/test.jsonl','data/validation.jsonl','data/reservation_policy.json'}}
        (self.root/'data_manifest.json').write_text(json.dumps(manifest))
        descriptor={'schema_version':1,'files':files,'sha256':inventory_hash(files)}
        (self.root/'bundle.json').write_text(json.dumps(descriptor))
        with patch('ats_mlops.data.dataset.load_manifest',side_effect=AssertionError('loader reached')), \
             patch('ats_mlops.pipeline.file_hash',side_effect=AssertionError('body opened')):
            with self.assertRaisesRegex(ValueError,'unsafe_train_validation_carrier'):
                pipeline.verify_bundle(self.root,descriptor['sha256'])

    def test_monitoring_dag_accepts_only_receipt_and_sanitizes_helper_output(self):
        captured=[]; context={'dag_run':types.SimpleNamespace(conf={'receipt_id':'a'*64,'receipt_sha256':'b'*64})}
        class DAG:
            def __init__(inner,**kwargs):inner.config=kwargs
            def __enter__(inner):return inner
            def __exit__(inner,*args):pass
        class Fail(Exception):pass
        def task(**kwargs):
            def decorate(function):
                captured.append(function)
                return lambda:None  # DAG construction does not execute tasks.
            return decorate
        sdk=types.ModuleType('airflow.sdk');sdk.DAG=DAG;sdk.task=task;sdk.get_current_context=lambda:context
        exceptions=types.ModuleType('airflow.sdk.exceptions');exceptions.AirflowFailException=Fail
        path=Path(__file__).resolve().parents[2]/'dags/monitor_and_retrain.py'
        spec=importlib.util.spec_from_file_location('test_monitoring_dag',path);module=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,{'airflow':types.ModuleType('airflow'),'airflow.sdk':sdk,'airflow.sdk.exceptions':exceptions}):
            spec.loader.exec_module(module)
        expected={'worker_sha256':worker.sha(b'code'),'policy_sha256':worker.sha(b'code'),'outcome':'passed',
                  'result':{'request_id':'retrain-id','state':'waiting_for_labels','private':'secret'}}
        with patch.object(module.subprocess,'run',return_value=subprocess.CompletedProcess([],0,json.dumps(expected),'')) as run, \
             patch.object(Path,'read_bytes',return_value=b'code'):
            self.assertEqual({'request_id':'retrain-id','state':'waiting_for_labels'},captured[0]())
            command=run.call_args.args[0];env=run.call_args.kwargs['env']
            self.assertEqual(command[-3:],['apply','--root','/state/retraining'])
            self.assertEqual({'PATH','PYTHONPATH','PYTHONDONTWRITEBYTECODE','HOME'},set(env))
            for conf in ({'window':{}},{'receipt_id':'a'*64,'receipt_sha256':'b'*64,'active':True},
                         {'receipt_id':True,'receipt_sha256':'b'*64}):
                context['dag_run'].conf=conf
                with self.assertRaisesRegex(Fail,'verified_receipt_required'):captured[0]()
            self.assertEqual(1,run.call_count)
            context['dag_run'].conf={'receipt_id':'a'*64,'receipt_sha256':'b'*64}
            expected['worker_sha256']='c'*64
            run.return_value=subprocess.CompletedProcess([],0,json.dumps(expected),'private raw error')
            with self.assertRaisesRegex(Fail,'policy_receipt_failed'):captured[0]()

    def test_policy_signal_is_derived_from_raw_provider_results_and_chosen_calibration(self):
        from ats_mlops.monitoring.drift import normalize_evidently
        from ats_mlops.monitoring.feedback import NUMERIC
        config={'method':'wasserstein','drift_share':.5,'threshold_grid':[.1,.2],'calibration_seed':7}
        def result(distance):
            metrics=[{'metric_name':'ValueDrift','config':{'column':name,'method':'wasserstein','threshold':.1},'value':distance} for name in NUMERIC]
            metrics.append({'metric_name':'DriftedColumnsCount','value':{'share':float(distance>=.1),'count':len(NUMERIC) if distance>=.1 else 0}})
            return {'metrics':metrics,'tests':[{'status':'FAIL' if distance>=.1 else 'SUCCESS'}]}
        raw={name:result(distance) for name,distance in (('control',0.),('permuted_control',0.),('shifted',.2))}
        observations={name:normalize_evidently(value,list(NUMERIC),config,.1,{}) for name,value in raw.items()}
        calibration={'constant_columns':{},'seed':7,'threshold':.1,'observations':[{'threshold':.1,'results':observations}],'files':{}}
        for name,value in raw.items():
            folder=self.root/('threshold-0-'+name);folder.mkdir()
            (folder/'evidently.json').write_text(json.dumps(value))
            calibration['files'].update({f'threshold-0-{name}/evidently.json':'a'*64,f'threshold-0-{name}/evidently.html':'b'*64})
        (self.root/'evidently.json').write_text(json.dumps(result(.2)))
        card={'outcome':'passed','files':dict.fromkeys(('quality.json','evidently.json','evidently.html'),'c'*64),
              'drift':normalize_evidently(result(.2),list(NUMERIC),config,.1,{})}
        loaded={'config':config,'reference_matrices':{'train':[dict.fromkeys(NUMERIC,x) for x in (.2,.4)]}}
        selected=[{'candidates':[{'features':{name:.3 for name in NUMERIC if name!='score'},'score':.3}]}]
        spec={'calibration':str(self.root),'window':str(self.root)}
        monitoring_decisions(spec,loaded,calibration,card,selected)
        changed=deepcopy(card);changed['drift']['detected']=False
        with self.assertRaisesRegex(p.PolicyError,'window_drift_decision_mismatch'):
            monitoring_decisions(spec,loaded,calibration,changed,selected)
        changed=deepcopy(calibration);changed['observations'][0]['results']['control']['detected']=True
        with self.assertRaisesRegex(p.PolicyError,'calibration_decision_mismatch'):
            monitoring_decisions(spec,loaded,changed,card,selected)
        changed=deepcopy(calibration);changed['threshold']=.2
        with self.assertRaisesRegex(p.PolicyError,'calibration_selection_mismatch'):
            monitoring_decisions(spec,loaded,changed,card,selected)

    def test_effective_threshold_seals_windows_and_reference_but_excludes_new_model_stats(self):
        from ats_mlops.monitoring.drift import normalize_evidently
        from ats_mlops.monitoring.feedback import NUMERIC
        from tests.unit.test_retraining_policy import execution,candidate,active
        config={'method':'wasserstein','drift_share':.5,'threshold_grid':[.1,.2],'calibration_seed':7,
                'columns':list(NUMERIC),'constant_policy':'exclude_unchanged; changed_or_all_constant_is_undefined',
                'minimum_pairs':200,'minimum_vacancies':10}
        runtime={'python':'3.11.17','monitoring_lock_sha256':h('lock'),'packages':{'evidently':'0.7.23'}}
        algorithm=h('numeric-algorithm');rules=[]
        for label,chosen,control_distance in (('A',.1,.05),('B',.2,.15)):
            base=self.root/label;observations=[];files={}
            def raw(threshold,distance):
                metrics=[{'metric_name':'ValueDrift','config':{'column':name,'method':'wasserstein','threshold':threshold},'value':distance} for name in NUMERIC]
                metrics.append({'metric_name':'DriftedColumnsCount','value':{'share':float(distance>=threshold),'count':len(NUMERIC) if distance>=threshold else 0}})
                return {'metrics':metrics,'tests':[{'status':'FAIL' if distance>=threshold else 'SUCCESS'}]}
            for index,threshold in enumerate(config['threshold_grid']):
                results={}
                for name,distance in (('control',control_distance),('permuted_control',control_distance),('shifted',.9)):
                    stem=f'threshold-{index}-{name}/evidently';path=base/'calibration'/(stem+'.json');path.parent.mkdir(parents=True,exist_ok=True)
                    path.write_text(json.dumps(raw(threshold,distance)));files.update({stem+'.json':h('json'),stem+'.html':h('html')})
                    results[name]=normalize_evidently(raw(threshold,distance),list(NUMERIC),config,threshold,{})
                observations.append({'threshold':threshold,'results':results})
                if threshold==chosen:break
            calibration={'constant_columns':{},'seed':7,'threshold':chosen,'method':'wasserstein','drift_share':.5,
                'observations':observations,'files':files,'reference_sha256':h(label+'-ref'),'runtime':runtime}
            path=base/'window/evidently.json';path.parent.mkdir();path.write_text(json.dumps(raw(chosen,.9)))
            card={'outcome':'passed','files':dict.fromkeys(('quality.json','evidently.json','evidently.html'),h('file')),
                  'drift':normalize_evidently(raw(chosen,.9),list(NUMERIC),config,chosen,{})}
            monitoring_decisions({'calibration':str(base/'calibration'),'window':str(base/'window')},
                {'config':config,'reference_matrices':{'train':[dict.fromkeys(NUMERIC,x) for x in (.2,.4)]}},calibration,card,
                [{'candidates':[{'features':{name:.3 for name in NUMERIC if name!='score'},'score':.3}]}])
            rules.append(effective_rule_sha256(config,calibration,runtime,algorithm))
        self.assertNotEqual(*rules)
        cohort=replace(REFERENCE.cohort,effective_config_sha256=rules[0]);reference=replace(REFERENCE,cohort=cohort)
        store=p.PolicyStore(self.root/'rules.sqlite3',reference)
        store.observe(window(0,cohort=cohort),now=START+HOUR)
        with self.assertRaisesRegex(p.PolicyError,'reference_mismatch'):
            store.observe(window(1,cohort=replace(cohort,effective_config_sha256=rules[1])),now=START+2*HOUR)
        self.assertEqual(0,store.snapshot()['requests']);self.assertEqual(1,store.snapshot()['windows'])
        result=store.observe(window(1,cohort=cohort),now=START+2*HOUR);request_id=result['request_id']
        store.bind_execution(request_id,execution(),now=START+2*HOUR)
        store.accept_labels(request_id,approval(),now=START+2*HOUR+MINUTE)
        event=store.pending_outbox()[0];store.ack_dispatch(event['event_id'],h('ack'),now=START+3*HOUR)
        store.record_candidate(request_id,candidate(),now=START+3*HOUR)
        target=active();target=replace(target,reference=replace(target.reference,cohort=replace(target.reference.cohort,effective_config_sha256=rules[1])))
        with self.assertRaisesRegex(p.PolicyError,'active_target_mismatch'):
            store.begin_reference_update(request_id,target,now=START+4*HOUR)
        same_rule=deepcopy(calibration);same_rule.update(threshold=.1,reference_sha256=h('new-model-reference'),observations=[],files={})
        self.assertEqual(rules[0],effective_rule_sha256(config,same_rule,runtime,algorithm))
        target=replace(target,reference=replace(target.reference,cohort=replace(target.reference.cohort,effective_config_sha256=rules[0])))
        store.begin_reference_update(request_id,target,now=START+4*HOUR)
        event=next(event for event in store.pending_outbox() if event['kind']=='reference')
        bad=replace(target,reference=replace(target.reference,cohort=replace(target.reference.cohort,effective_config_sha256=rules[1])))
        with self.assertRaisesRegex(p.PolicyError,'active_target_changed'):
            store.reconcile_reference(event['event_id'],bad,now=START+4*HOUR+MINUTE)
        store.reconcile_reference(event['event_id'],target,now=START+4*HOUR+MINUTE)
        self.assertEqual('active',store.request(request_id)['state'])
        for key,value in (('threshold',True),('drift_share',True)):
            changed=deepcopy(same_rule);changed[key]=value
            with self.assertRaisesRegex(p.PolicyError,'invalid_effective_monitoring_rule'):
                effective_rule_sha256(config,changed,runtime,algorithm)
        integer=deepcopy(config);integer['threshold_grid']=[1]
        floating=deepcopy(config);floating['threshold_grid']=[1.0]
        self.assertNotEqual(effective_rule_sha256(integer,dict(same_rule,threshold=1),runtime,algorithm),
                            effective_rule_sha256(floating,dict(same_rule,threshold=1.0),runtime,algorithm))

    def test_host_reference_changed_effective_rule_rejects_before_receipt_or_intent(self):
        request_id=self.queued(); calls=[];store=p.PolicyStore(self.root/'policy.sqlite3')
        class Native:
            def call(inner,action,value):
                calls.append(action)
                if action=='request':return store.request(request_id)
                raise AssertionError('mutation reached')
        batch=HostBatch(native=Native(),airflow=None,release_state=self.root/'release',artifacts=self.root/'artifacts',
            source_git_sha='a'*40,source_repository=self.root)
        with patch('ats_mlops.monitoring.drift.configuration',return_value=({},REFERENCE.cohort.config_sha256)), \
             patch('ats_mlops.monitoring.drift.load_reference',return_value=({},{})), \
             patch('ats_mlops.monitoring.retraining_batch.active_state',return_value=nullcontext({})), \
             patch('ats_mlops.monitoring.retraining_batch.verified_calibration',return_value=({},h('other-threshold'))):
            with self.assertRaisesRegex(p.PolicyError,'reference_effective_rule_changed'):
                batch.update_reference(request_id,self.root,h('reference'),self.root,config=self.root/'config',
                    calibration=self.root/'calibration',calibration_sha256=h('calibration'),monitoring_sources=['a'*40])
        self.assertEqual(['request'],calls)

    def test_native_helper_cannot_accept_raw_window_or_active_claim(self):
        with self.assertRaises(p.PolicyError):
            worker.execute('apply',{'window':p.payload(window(0)),'active':True},self.root,now=START+HOUR)
        self.assertFalse((self.root/'policy.sqlite3').exists())
        self.assertEqual({'state':'uninitialized'},worker.execute('snapshot',{},self.root))
        self.assertEqual([],worker.execute('pending',{},self.root))
        self.assertFalse((self.root/'policy.sqlite3').exists())

    def test_installed_receipt_hash_and_source_epoch_are_checked_before_state(self):
        receipt = self.receipt(0)
        bad = deepcopy(receipt); bad['proof']['active_epoch']=2
        with self.assertRaisesRegex(p.PolicyError,'window_proof_mismatch'):
            worker.execute('install',{'receipt':bad},self.root)
        installed = worker.execute('install',{'receipt':receipt},self.root)
        path = self.root/'receipts'/(installed['receipt_id']+'.json')
        path.chmod(0o640); path.write_text('{}')
        with self.assertRaisesRegex(p.PolicyError,'verified_receipt_changed'):
            worker.execute('apply',installed,self.root,now=START+HOUR)
        self.assertFalse((self.root/'policy.sqlite3').exists())

    def test_receipt_process_exit_never_exposes_torn_final_and_same_retry_recovers(self):
        context=multiprocessing.get_context('spawn');receipt=self.receipt(0)
        content=(p.canonical(receipt)+'\n').encode();identity=worker.sha(content)
        for phase in ('after_write','before_publish','after_publish'):
            root=self.root/phase;child=context.Process(target=install_child,args=(str(root),receipt,phase))
            child.start();child.join(10)
            self.assertFalse(child.is_alive());self.assertEqual(81 if phase=='after_write' else 82,child.exitcode)
            path=root/'receipts'/(identity+'.json')
            if path.exists():self.assertEqual(content,path.read_bytes())
            saved=worker.install(root,receipt)
            self.assertEqual({'receipt_id':identity,'receipt_sha256':identity},saved)
            self.assertEqual(content,path.read_bytes())
            self.assertFalse((root/'policy.sqlite3').exists())
            worker.execute('apply',saved,root,now=START+HOUR)
            self.assertEqual(1,p.PolicyStore(root/'policy.sqlite3').snapshot()['windows'])

    def test_concurrent_same_and_different_receipts_publish_complete_immutable_files(self):
        context=multiprocessing.get_context('spawn');queue=context.Queue()
        receipts=[self.receipt(0),self.receipt(0),self.receipt(1)]
        children=[context.Process(target=install_child,args=(str(self.root),receipt,None,queue)) for receipt in receipts]
        for child in children:child.start()
        for child in children:
            child.join(10);self.assertFalse(child.is_alive());self.assertEqual(0,child.exitcode)
        saved=[queue.get(timeout=2) for _ in children];queue.close();queue.join_thread()
        self.assertEqual(2,len({value['receipt_id'] for value in saved}))
        self.assertEqual(2,len(list((self.root/'receipts').glob('*.json'))))
        self.assertFalse(list((self.root/'receipts').glob('.pending-*')))
        for value in saved:worker.load_receipt(self.root,value)
        self.assertFalse((self.root/'policy.sqlite3').exists())

    def test_receipt_replay_and_wrong_ack_request_have_no_extra_effects(self):
        request_id = self.queued(); store = p.PolicyStore(self.root/'policy.sqlite3')
        installed = worker.execute('install',{'receipt':self.receipt(1)},self.root)
        worker.execute('apply',installed,self.root,now=START+2*HOUR+2*MINUTE)
        self.assertEqual(1,store.snapshot()['requests'])
        event = store.pending_outbox()[0]
        with self.assertRaisesRegex(p.PolicyError,'ack_request_mismatch'):
            worker.execute('ack',{'request_id':'wrong','event_id':event['event_id'],'receipt_sha256':h('ack')},self.root,
                           now=START+2*HOUR+3*MINUTE)
        self.assertEqual('queued',store.request(request_id)['state'])
        self.assertEqual([event],store.pending_outbox())

    def test_semantic_labelset_ignores_namespace_ids_and_formatting(self):
        rows = [{'vacancy_id':'old-v','candidate_id':'old-c','vacancy_text':'Python SQL','candidate_text':'SQL Python','relevance':1}]
        renamed = [{**rows[0],'vacancy_id':'new-v','candidate_id':'new-c','vacancy_text':' SQL, python! ',
                    'candidate_text':'python sql'}]
        self.assertEqual(semantic_labelset(rows),semantic_labelset(renamed))
        self.assertNotEqual(semantic_labelset(rows)[0],semantic_labelset([{**renamed[0],'relevance':0}])[0])
        with self.assertRaisesRegex(p.PolicyError,'semantic_label_conflict'):
            semantic_labelset(rows+[{**renamed[0],'relevance':0}])

    def test_incomplete_or_mutated_execution_hash_is_rejected_before_dispatch(self):
        value = p.payload(self.plan); value['parameters_sha256']='0'*64
        with self.assertRaises(p.PolicyError):
            worker.typed(p.ExecutionPlan,value)
        request_id = self.queued(); event = p.PolicyStore(self.root/'policy.sqlite3').pending_outbox()[0]
        intent = deepcopy(event['intent']); intent['execution']=None
        with self.assertRaises(p.PolicyError):
            candidate_conf(intent)

    def test_native_transport_checks_owned_container_and_fixed_source_helper(self):
        calls = []
        def run(command,**kwargs):
            calls.append((command,kwargs))
            if command[1] == 'inspect':
                return subprocess.CompletedProcess(command,0,json.dumps({'image':self.plan.training_image_id,
                    'project':'ats-core','service':'airflow','running':True}), '')
            result = {'worker_sha256':file_sha(Path(worker.__file__)),'policy_sha256':file_sha(Path(p.__file__)),
                      'outcome':'passed','result':{'state':'uninitialized'}}
            return subprocess.CompletedProcess(command,0,json.dumps(result),'')
        native = NativePolicy('ats-core-airflow-1',self.plan.training_image_id,run=run)
        self.assertEqual({'state':'uninitialized'},native.call('snapshot',{}))
        command = calls[-1][0]
        self.assertEqual(command[:5],['docker','exec','--user','50000:0','-i'])
        self.assertIn('ats_mlops.monitoring.policy_worker',command)
        self.assertNotIn('/var/run/docker.sock',str(calls))
        with self.assertRaisesRegex(p.PolicyError,'native_action_forbidden'):
            native.call('run-any-shell',{})
        self.assertEqual(2,len(calls))

    def test_foreign_container_fails_before_exec(self):
        calls=[]
        def run(command,**kwargs):
            calls.append(command)
            return subprocess.CompletedProcess(command,0,json.dumps({'image':self.plan.training_image_id,
                'project':'foreign','service':'airflow','running':True}),'')
        with self.assertRaisesRegex(p.PolicyError,'native_container_identity_mismatch'):
            NativePolicy('ats-core-airflow-1',self.plan.training_image_id,run=run).call('snapshot',{})
        self.assertEqual(1,len(calls))

    def test_airflow_lost_post_ack_recovers_exact_id_and_stable_ack_hash(self):
        class API(AirflowAPI):
            def __init__(self):self.saved=None;self.posts=0;self.status='running'
            def _call(self,path,**kwargs):
                if kwargs.get('method') == 'POST':
                    self.posts+=1; body=kwargs['body']; self.saved={'dag_run_id':body['dag_run_id'],'conf':body['conf']}
                    raise p.PolicyError('lost_response')
                return None if self.saved is None else {**self.saved,'state':self.status,'updated_at':str(self.posts)}
        request_id = self.queued(); event = p.PolicyStore(self.root/'policy.sqlite3').pending_outbox()[0]
        conf = candidate_conf(event['intent']); api=API()
        first,_ = api.trigger('train_candidate',conf['run_id'],conf)
        api.status='success'; second,_=api.trigger('train_candidate',conf['run_id'],conf)
        self.assertEqual(first,second); self.assertEqual(1,api.posts)
        self.assertEqual(first,{'dag_id':'train_candidate','dag_run_id':conf['run_id'],'conf_sha256':p.digest(conf)})

    def test_airflow_existing_run_conflict_rejects_typed_bool_even_python_equal(self):
        class API(AirflowAPI):
            def __init__(self):self.posts=0
            def _call(self,path,**kwargs):
                self.assert_no_post = kwargs.get('method') != 'POST'
                return {'dag_run_id':'run-1','conf':{'value':True},'state':'running'}
        with self.assertRaisesRegex(p.PolicyError,'dag_run_conflict'):
            API().trigger('train_candidate','run-1',{'value':1})

    def test_candidate_export_rejects_reserved_paths_and_immutable_changes(self):
        content=b'canonical metadata\n'; encoded=base64.b64encode(content).decode()
        result={'state':'completed','files':{'artifacts/prepared/data/model.json':{'sha256':worker.sha(content),'base64':encoded}}}
        save_candidate_export(self.root/'export',result)
        save_candidate_export(self.root/'export',result)
        changed=b'changed\n'; result['files']['artifacts/prepared/data/model.json']={'sha256':worker.sha(changed),'base64':base64.b64encode(changed).decode()}
        with self.assertRaisesRegex(p.PolicyError,'immutable_candidate_changed'):
            save_candidate_export(self.root/'export',result)
        for name in ('../secret','artifacts/prepared/data/test.jsonl','auth/credentials.json'):
            with self.assertRaises(p.PolicyError):
                save_candidate_export(self.root/'unsafe',{'state':'completed','files':{name:{'sha256':worker.sha(content),'base64':encoded}}})

    def test_checked_fixture_active_flag_cannot_override_real_observed_model(self):
        record={'mlflow':{'model_name':'ranking','model_version':'4','run_id':'run-4'},
                'model':{'sha256':h('model4')},'features':{}}
        package=self.root/'package';package.mkdir();(package/'run_manifest.json').write_text(json.dumps(record))
        observed={'active':{'release_id':'release-A','manifest_sha256':h('releaseA'),'check_sha256':h('checkA')},
                  'record':record,'package':package,'state':{'generation':1}}
        card={'model':{'model_name':'ranking','model_version':'13','release_id':'fixture13','active':True}}
        with self.assertRaisesRegex(p.PolicyError,'reference_not_current_active'):
            reference_binding(card,h('reference'),self.root/'not-opened-bundle',observed)


if __name__ == '__main__':
    unittest.main()
