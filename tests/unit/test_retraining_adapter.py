"""Protocol/boundary fixtures only; no provider, Docker, API or model effects."""

from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
import base64
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.monitoring import policy as p, policy_worker as worker
from ats_mlops.monitoring.retraining import AirflowAPI, NativePolicy, candidate_conf, file_sha, reference_binding, save_candidate_export, semantic_labelset
from ats_mlops.monitoring.retraining_batch import execution_plan
from tests.unit.test_retraining_policy import START,HOUR,MINUTE,REFERENCE,MODEL,h,window,approval


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
