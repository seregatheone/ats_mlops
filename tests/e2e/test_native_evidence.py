"""Synthetic receipt mutations only; these tests never qualify real acceptance."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from tests.e2e.audit import Artifacts,EvidenceError,canonical,digest
from tests.e2e.native import journal,validation_gate,bad_data
from tests.e2e.test_audit_contract import save


def gate_fixture(accepted=True):
    from ats_mlops.quality.policy import assess_scores
    rows=[dict(vacancy_id='v',candidate_id=name,vacancy_text='python',candidate_text=text,relevance=label)
          for name,text,label in [('a','java',0),('z','python',1)]]
    scores={('v','a'):.1 if accepted else .9,('v','z'):.9 if accepted else .1}
    value=assess_scores(rows,['v'],scores)
    value.update(schema_version=1,test_read=False,validation={'read_partitions':['validation']},
        candidate={'model_sha256':'a'*64},scoring_receipts={'candidate':dict(isolated=True,bytecode_disabled=True,
        prior_project_import=False,network=False,training_checkout_mounted=False,credentials=False,model_sha256='a'*64,scores_sha256='b'*64)})
    return value


def negative_fixture(root):
    """Complete TOY provenance carrier; no model/API/Registry effect claimed."""
    source='a'*40
    identity=dict(model_name='ats-pair-ranking',model_version='1',run_id='1'*32,experiment_id='1',exact_uri='models:/ats-pair-ranking/1',source='models:/m-'+'1'*32)
    original={"model.pkl":digest(b'ORIGINAL TOY BYTES')}
    parent=dict(git_sha=source,dirty_worktree=False,test_read=False,read_partitions=['train','validation'],mlflow=identity,
        model=dict(files=original,sha256=digest(canonical(original)),payload_sha256=original['model.pkl']),
        params={'seed':20261026},params_sha256='a'*64,features={'version':'toy'},features_sha256='b'*64,
        data_manifest_sha256='c'*64,data_sha256={'train':'d'*64,'validation':'e'*64},split_sha256='f'*64,
        environment={'image':{'id':'sha256:'+'1'*64}},source_sha256={'toy.py':'a'*64},source_tree_sha256='b'*64)
    parent_ref=save(root,'parent.json',parent)
    intent=dict(scope='controlled_negative_artifact_fault',operation='negate_fitted_logistic_coefficients_and_intercept',
        source_git_sha=source,parent_model_sha256=parent['model']['sha256'],parent_run_manifest_sha256=parent_ref['sha256'],
        parent_training_mlflow=identity,fit_events=0,selected_for_E5=False)
    candidate=dict(identity,model_version='2',run_id='2'*32,exact_uri='models:/ats-pair-ranking/2',source='models:/m-'+'2'*32)
    payload=root/'negative-model.pkl';payload.write_bytes(b'NEGATIVE TOY BYTES')
    model_files={'model.pkl':digest(payload.read_bytes())}
    run=deepcopy(parent);run.update(mlflow=candidate,artifact_derivation=intent,
        model=dict(files=model_files,sha256=digest(canonical(model_files)),payload_sha256=model_files['model.pkl']))
    run_ref=save(root,'candidate-run.json',run)
    pipeline=dict(training=dict(mlflow=candidate,model_sha256=run['model']['sha256'],run_manifest_sha256=run_ref['sha256']),
        artifact_derivation=intent,execution_kind='controlled_fault_derivation; toy inherited carrier')
    pipeline_ref=save(root,'candidate-pipeline.json',pipeline)
    fault=dict(intent,candidate_mlflow=candidate,candidate_model_sha256=run['model']['sha256'],
        candidate_run_manifest_sha256=run_ref['sha256'],candidate_pipeline_manifest_sha256=pipeline_ref['sha256'])
    gate=gate_fixture(False)
    # Retain real pure policy math against BOTH Jaccard and the parent incumbent.
    gate['metrics']['incumbent']=deepcopy(gate['metrics']['baseline'])
    gate['comparisons']['incumbent']=deepcopy(gate['comparisons']['baseline'])
    gate['reasons'].append('candidate.ndcg_drop_against_incumbent')
    gate['candidate']={**candidate,'model_sha256':run['model']['sha256'],'run_manifest_sha256':run_ref['sha256'],'pipeline_manifest_sha256':pipeline_ref['sha256']}
    gate['incumbent']={**identity,'model_sha256':parent['model']['sha256'],'run_manifest_sha256':parent_ref['sha256']}
    gate['expected_incumbent']={**{key:gate['incumbent'][key] for key in ('model_name','model_version','run_id','model_sha256')},
        'release_id':'first-S1','release_is_fixture':False}
    gate['scoring_receipts']['candidate']['model_sha256']=run['model']['sha256']
    gate['scoring_receipts']['incumbent']=dict(gate['scoring_receipts']['candidate'],model_sha256=parent['model']['sha256'])
    refs=dict(schema=save(root,'schema.json',dict(outcome='passed',original_test_read=False,training_invoked=False,
            validation=dict(outcome='rejected',gx=[dict(scope='source',success=False)]))),
        guard=save(root,'guard.json',dict(fit_calls=dict(pipeline=0,classifier=0),validation=dict(outcome='rejected'))),
        bad_gate=save(root,'gate.json',gate),fault_provenance=save(root,'fault.json',fault),parent_run=parent_ref,
        candidate_run=run_ref,candidate_pipeline=pipeline_ref,fault_intent=save(root,'intent.json',intent),
        candidate_files={'model.pkl':{'path':payload.name,'sha256':model_files['model.pkl']}})
    return {'source_git_sha':source,'native':refs}


class NativeEvidenceTests(unittest.TestCase):
    def test_real_pure_gate_math_replayed_from_synthetic_defined_metrics(self):
        validation_gate(gate_fixture(),accepted=True)
        validation_gate(gate_fixture(False),accepted=False)
        for variant in ('nonfinite','threshold','cohort','test','credentials','missing_model','lineage_only_rejection'):
            value=gate_fixture(False)
            if variant=='nonfinite':value['metrics']['candidate']['ndcg_at_10']=float('nan')
            elif variant=='threshold':value['comparisons']['baseline']['max_ndcg_drop']=.1
            elif variant=='cohort':value['metrics']['candidate']['eligible_group_ids']=['other']
            elif variant=='test':value['test_read']=True
            elif variant=='credentials':value['scoring_receipts']['candidate']['credentials']=True
            elif variant=='missing_model':value['candidate']=None
            else:value['reasons']=['candidate.model_inventory_mismatch']
            with self.subTest(variant=variant),self.assertRaises(EvidenceError):validation_gate(value,accepted=False)

    def test_native_T13_journal_hash_chain_and_projection_are_replayed(self):
        from ats_mlops.release.state import Store
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);store=Store(root/'catalog');state=store.read()
            store.append(state,'staged',{'release_id':'toy'})
            store.append(state,'approved',{'release_id':'toy'})
            store.append(state,'active_committed',{'release_id':'toy'})
            content=(store.root/'journal.jsonl').read_bytes();path=root/'journal.jsonl';path.write_bytes(content)
            reference={'path':'journal.jsonl','sha256':digest(content)}
            observed,kinds=journal(Artifacts(root),reference)
            self.assertEqual(state,observed);self.assertEqual(['staged','approved','active_committed'],kinds)
            for broken in (content[:-1],content.replace(b'"staged"',b'"forged"',1)):
                path.write_bytes(broken);changed=dict(reference,sha256=digest(broken))
                with self.assertRaises(EvidenceError):journal(Artifacts(root),changed)

    def test_negative_fixture_contradictory_fit_parent_and_candidate_provenance_reject(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);receipt=negative_fixture(root)
            self.assertEqual('passed',bad_data(Artifacts(root),receipt)['outcome'])
            original=deepcopy(receipt)
            for kind in ('fit7','fitbool','fitfloat','sameparent','wrongparenthash','selected','candidatehash','changedintent'):
                changed=deepcopy(original);refs=changed['native']
                key='fault_intent' if kind=='changedintent' else 'fault_provenance'
                value=__import__('json').loads((root/refs[key]['path']).read_bytes())
                if kind=='fit7':value['fit_events']=7
                elif kind=='fitbool':value['fit_events']=False
                elif kind=='fitfloat':value['fit_events']=0.0
                elif kind=='sameparent':value['parent_training_mlflow']=value['candidate_mlflow']
                elif kind=='wrongparenthash':value['parent_run_manifest_sha256']='f'*64
                elif kind=='selected':value['selected_for_E5']=True
                elif kind=='candidatehash':value['candidate_run_manifest_sha256']='d'*64
                else:value['operation']='arbitrary_model_change'
                refs[key]=save(root,'mutated-'+kind+'.json',value)
                with self.subTest(kind=kind),self.assertRaises(EvidenceError):bad_data(Artifacts(root),changed)

    def test_all_eight_native_validators_are_explicit_not_default_success(self):
        from tests.e2e.native import VALIDATORS
        from tests.e2e.audit import SCENARIOS
        self.assertEqual(set(SCENARIOS),set(VALIDATORS))
        with tempfile.TemporaryDirectory() as folder:
            for name,validator in VALIDATORS.items():
                with self.subTest(name=name),self.assertRaises((KeyError,EvidenceError,TypeError)):
                    validator(Artifacts(Path(folder)),{'outcome':'passed','native':{}})


if __name__=='__main__':unittest.main()
