"""Controlled-clock protocol integration; real native ledger, fixture transports.

This executable contract does not claim actual Airflow/model/release execution.
The separately retained runtime receipt must bind those real effects and source.
"""
from contextlib import nullcontext
from copy import deepcopy
import base64
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from ats_mlops.monitoring import policy as p, policy_worker as worker
from ats_mlops.monitoring.retraining import AirflowAPI
from ats_mlops.monitoring.retraining_batch import HostBatch, execution_plan
from tests.unit.test_retraining_policy import START,HOUR,MINUTE,REFERENCE,h,window,approval,active


class RetrainingProtocolIntegration(unittest.TestCase):
    def test_native_waiting_late_labels_exact_dispatch_gate_and_reference_reconcile(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); native_root=root/'native'; plan=execution_plan('a'*40,'sha256:'+h('image'))
            proof={'source_git_sha':'a'*40,'input_inventory_sha256':h('inputs'),'active_journal_sha256':h('journal'),
                   'active_epoch':1,'active_release_id':'release-A'}
            def apply(kind,value,clock):
                receipt={'schema_version':1,'kind':kind,'value':value,'proof':proof}
                saved=worker.execute('install',{'receipt':receipt},native_root)
                return worker.execute('apply',saved,native_root,now=clock)
            for index in (0,1):
                result=apply('window',{'window':p.payload(window(index)),'reference':p.payload(REFERENCE),
                    'execution':p.payload(plan)},START+(index+1)*HOUR)
            request_id=result['request_id']; store=p.PolicyStore(native_root/'policy.sqlite3')
            self.assertEqual('waiting_for_labels',store.request(request_id)['state'])
            self.assertEqual([],store.pending_outbox())
            # Approved labels resume the original request well within cooldown.
            apply('dataset',{'request_id':request_id,'approval':p.payload(approval())},START+2*HOUR+MINUTE)
            self.assertEqual(request_id,store.snapshot()['inflight'])
            model={'mlflow':{'model_name':'ranking','model_version':'5','run_id':'run-5'},'model':{'sha256':h('model-5')}}
            content=(p.canonical(model)+'\n').encode()
            exported={'state':'completed','candidate_receipt':{'candidate_id':'candidate-B'},
                'files':{'artifacts/training/final/run_manifest.json':{'sha256':worker.sha(content),'base64':base64.b64encode(content).decode()}}}
            class Native:
                image_id=plan.training_image_id
                def call(inner,action,value):
                    if action=='runtime':return {'image_id':plan.training_image_id}
                    if action=='export':return deepcopy(exported)
                    return worker.execute(action,value,native_root,now=START+3*HOUR)
            class API(AirflowAPI):
                def __init__(inner):inner.saved=None;inner.posts=0
                def _call(inner,path,**kwargs):
                    if kwargs.get('method')=='POST':
                        inner.posts+=1;body=kwargs['body'];inner.saved={'dag_run_id':body['dag_run_id'],'conf':body['conf'],'state':'success'}
                        raise p.PolicyError('lost_post_response')
                    return inner.saved
            staged={'release_id':'release-B','manifest_sha256':h('release-manifest'),
                    'check_sha256':h('release-check'),'image_id':'sha256:'+h('image')}
            controller=types.SimpleNamespace(store=types.SimpleNamespace(lock=lambda:nullcontext(),read=lambda:{'generation':1,'staged':{}}),
                _consistent=lambda state:None,stage=lambda *args,**kwargs:staged)
            observed={'controller':controller,'active':{'release_id':'release-A'},'state':{'generation':1},
                'record':{'mlflow':{'model_name':'ranking','model_version':'4','run_id':'run-4'},'model':{'sha256':h('model-4')}}}
            class Operations:
                def gate(inner,*args):
                    path=args[0]/'model-gate.json';path.write_text('{}');return {'outcome':'passed'}
                def package(inner,*args):return {'release_id':'release-B'}
                def release_check(inner,*args):return {'outcome':'passed'}
            api=API();batch=HostBatch(native=Native(),airflow=api,release_state=root/'release',artifacts=root/'artifacts',
                source_git_sha='a'*40,source_repository=root,operations=Operations())
            with patch('ats_mlops.monitoring.retraining_batch.active_state',return_value=nullcontext(observed)):
                first=batch.advance(request_id);second=batch.advance(request_id)
            self.assertEqual('waiting_approval',first['state']);self.assertEqual('waiting_approval',second['state'])
            self.assertEqual(1,api.posts);self.assertEqual(REFERENCE,store.reference())
            self.assertEqual(1,store.snapshot()['requests'])
            # An explicit separately verified ACTIVE is required; intent alone
            # and a fresh process do not move the cursor before reconcile.
            target=active();proof.update(active_epoch=2,active_release_id='release-B')
            receipt={'schema_version':1,'kind':'reference','value':{'request_id':request_id,'active':p.payload(target)},'proof':proof}
            saved=worker.execute('install',{'receipt':receipt},native_root)
            worker.execute('reference-intent',saved,native_root,now=START+4*HOUR)
            store=p.PolicyStore(native_root/'policy.sqlite3')
            self.assertEqual(REFERENCE,store.reference())
            event=next(event for event in store.pending_outbox() if event['kind']=='reference')
            worker.execute('reconcile-reference',{**saved,'event_id':event['event_id']},native_root,now=START+4*HOUR+MINUTE)
            self.assertEqual(target.reference,store.reference())
            self.assertIsNone(store.snapshot()['inflight'])
            self.assertEqual('active',store.request(request_id)['state'])


if __name__=='__main__':
    unittest.main()
