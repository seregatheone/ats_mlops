"""Only toy callbacks/temporary fixtures: no actual experiment or holdout read."""
from copy import deepcopy
import json
import multiprocessing
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.evaluation import experiment as e

ROOT = Path(__file__).resolve().parents[2]


def spec(groups=2):
    value=e.read(ROOT/'configs/experiment.template.json')
    value['source_git_sha']='a'*40;value['qualified_model_source_git_sha']='b'*40
    value['bindings']=dict.fromkeys(e.BINDINGS,'b'*64)
    value['bindings']['root_policy_sha256']=e.ROOT_POLICY_SHA256
    value['test_inventory']={'pairs':2*groups,'groups':groups,'group_ids_sha256':e.sha(e.canonical([f'g{i:02}' for i in range(groups)])), 'pair_ids_sha256':e.sha(e.canonical([f'g{i:02}/{c}' for i in range(groups) for c in ('a','b')]))}
    value['registry_before']={'runs':['old-run'],'versions':['13']}
    value['adapter_bindings']={name:{'entrypoint':'tests.unit.test_experiment:toy_adapter','source_sha256':'c'*64} for name in e.SERIES}
    return value


def make_study(root,name='study',value=None):
    value=value or spec();path=root/(name+'-spec.json');e.immutable(path,value)
    freeze={'outcome':'passed','spec_sha256':e.sha(e.canonical(value)),'source_git_sha':value['source_git_sha'],
            'source_clean':True,'inputs':value['bindings']}
    frozen=root/(name+'-freeze.json');e.immutable(frozen,freeze)
    return e.initialize(path,root/name,frozen,e.sha(frozen.read_bytes()),verifier=lambda p,h:e.read(p))


def result(observed,outcome='passed'):
    return {'outcome':outcome,'observations':observed,'evidence_sha256':'d'*64}


def replica(value,index):
    return dict(source_git_sha=value['source_git_sha'],forced=True,run_cache=False,cache_hit=False,final_fit_events=1,
                data_sha256=value['bindings']['data_manifest_sha256'],split_sha256=value['bindings']['split_sha256'],
                features_sha256=value['bindings']['features_sha256'],vocabulary_sha256='e'*64,
                coordinates_sha256=e.sha(e.canonical(value['bindings'])),workspace_id=f'workspace-{index}',
                training_run_id=f'new-run-{index}',model_version=str(20+index),
                scores=[dict(group_id='g',candidate_id='a',score=.9),dict(group_id='g',candidate_id='b',score=.1)],metrics={'ndcg':1.})


def numeric(value):
    groups=[f'g{i:02}' for i in range(value['test_inventory']['groups'])]
    return dict(rows=[dict(group_id=g,candidate_id=c,relevance=int(c=='a'),baseline_score=.2 if c=='a' else .8,
                          model_score=.8 if c=='a' else .2) for g in groups for c in ('a','b')],
                groups=groups,latencies=[dict(group_id=g,baseline_seconds=.001,model_seconds=.002) for g in groups],
                snapshot_sha256=value['bindings']['test_partition_sha256'],model_artifact_sha256=value['bindings']['model_artifact_sha256'],
                baseline_artifact_sha256=value['bindings']['baseline_artifact_sha256'],evaluated_groups_once=len(groups),warmup='train_validation_only')


def snapshot(value,observed=None):
    observed=observed or numeric(value)
    return {'sha256':value['bindings']['test_partition_sha256'], 'rows':[{'vacancy_id':row['group_id'],'candidate_id':row['candidate_id'],'relevance':row['relevance']} for row in observed['rows']]}


def concurrent_evaluate(study,guard,marker):
    value=e.study(study)[0]
    def load():
        with Path(marker).open('a') as stream:stream.write('read\n')
        return snapshot(value)
    try:e.evaluate_once(study,guard,load,lambda _:numeric(value))
    except e.ExperimentError:pass


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)

    def test_draft_is_not_freezable_and_policy_types_are_strict(self):
        draft=e.read(ROOT/'configs/experiment.template.json');e.validate_spec(draft,template=True)
        with self.assertRaises(e.ExperimentError):e.validate_spec(draft)
        for path,value in [('E1',dict(e.POLICIES['E1'],replicas=True)),('E5',dict(e.POLICIES['E5'],bootstrap_replicates=1999)),('execution',dict(e.EXECUTION,smoke=0))]:
            bad=spec();bad[path]=value
            with self.subTest(path=path),self.assertRaises(e.ExperimentError):e.validate_spec(bad)

    def test_bad_freeze_rejected_before_study_directory(self):
        s=spec();p=self.root/'spec';e.immutable(p,s);f=self.root/'freeze';e.immutable(f,{'outcome':'draft'})
        with self.assertRaises(e.ExperimentError):e.initialize(p,self.root/'study',f,'0'*64,verifier=lambda *_:self.fail())
        self.assertFalse((self.root/'study').exists())

    def test_no_measurements_are_not_run_and_unknown_is_never_passed(self):
        directory=make_study(self.root);summary=e.summarize(directory)
        self.assertTrue(all(x['status']=='not_run' for x in summary['series'].values()))
        with self.assertRaises(RuntimeError):e.run_attempt(directory,'E1','1',lambda *_:(_ for _ in ()).throw(RuntimeError('private secret')))
        current=e.summarize(directory);self.assertEqual('recovery_required',current['series']['E1']['status'])
        self.assertNotIn('private secret',(directory/'attempts/E1/1/outcome.json').read_text())
        with self.assertRaises(FileExistsError):e.run_attempt(directory,'E1','1',lambda *_:self.fail('rerun'))

    def test_e1_three_new_forced_fits_and_numerical_tolerance(self):
        directory=make_study(self.root);value=e.study(directory)[0]
        for index in range(1,4):
            row=replica(value,index);row['scores'][0]['score']+=index*1e-8
            e.run_attempt(directory,'E1',str(index),lambda *_args,row=row:result(row))
        self.assertEqual('passed',e.summarize(directory)['series']['E1']['status'])
        self.assertEqual('not_run',e.summarize(directory)['series']['E2']['status'])

    def test_e1_cache_or_reused_version_or_rank_swap_never_replica(self):
        for key,val in [('cache_hit',True),('model_version','13'),('final_fit_events',True),('data_sha256','0'*64)]:
            value=spec();rows=[replica(value,i) for i in range(1,4)];rows[-1][key]=val
            with self.subTest(key=key),self.assertRaises(e.ExperimentError):e._audit_e1(value,rows)
        rows=[replica(spec(),i) for i in range(1,4)];rows[1]['scores'][0]['score']=.05
        with self.assertRaises(e.ExperimentError):e._audit_e1(spec(),rows)

    def test_e1_incomplete_and_raw_tamper_do_not_pass(self):
        directory=make_study(self.root);value=e.study(directory)[0]
        e.run_attempt(directory,'E1','1',lambda *_:result(replica(value,1)))
        self.assertEqual('incomplete',e.summarize(directory)['series']['E1']['status'])
        path=directory/'attempts/E1/1/raw.json';raw=e.read(path);raw['observations']['cache_hit']=True;path.write_bytes(e.canonical(raw))
        with self.assertRaises(e.ExperimentError):e.summarize(directory)

    def test_e2_cannot_run_agent_as_manual_or_import_without_attestation(self):
        directory=make_study(self.root)
        with self.assertRaises(e.ExperimentError):e.run_attempt(directory,'E2','1-manual',lambda *_:self.fail())
        artifact=self.root/'agent';e.immutable(artifact,result({'actor_kind':'agent'}))
        with self.assertRaises(e.ExperimentError):e.ingest_human_measurement(directory,1,'manual',artifact,attest=lambda _:False)
        self.assertEqual('not_run',e.summarize(directory)['series']['E2']['status'])

    def test_e2_five_pairs_order_cache_actor_and_missing_values(self):
        value=spec();rows={}
        for pair in range(1,6):
            for mode in ('manual','auto'):
                rows[f'{pair}-{mode}']=dict(pair=pair,mode=mode,measurement_id=f'm-{pair}-{mode}',actor_kind='human',actor_id='toy-human',
                    attestation_sha256='a'*64,journal_sha256='b'*64,hpo_enabled=False,coordinates_sha256=e.sha(e.canonical(value['bindings'])),
                    cache_state_sha256='c'*64,order=0 if mode==('manual' if pair%2 else 'auto') else 1,
                    wall_seconds=10.,active_human_seconds=2.,setup_seconds=1.,warmup_seconds=1.,actions=2,errors=0)
        self.assertEqual(5,e._audit_e2(value,rows)['pairs'])
        for key,val in [('actor_kind','agent'),('cache_state_sha256','0'*64),('wall_seconds',None),('measurement_id','m-1-manual'),('order',True)]:
            bad=deepcopy(rows);bad['5-auto'][key]=val
            with self.subTest(key=key),self.assertRaises((e.ExperimentError,TypeError)):e._audit_e2(value,bad)

    def test_e3_exact_six_by_three_and_http_outage_bounds(self):
        rows={}
        for fault in e.FAULTS:
            for repeat in range(1,4):
                rows[f'{fault}-{repeat}']=dict(fault=fault,repeat=repeat,own_resources_only=True,isolation_sha256='a'*64,
                    fault_receipt_sha256='b'*64,final_consistent=True,terminal='restored',injected_monotonic=1.,detected_monotonic=2.,
                    restored_monotonic=4.,http_samples=[{'t':0.,'ok':True},{'t':2.,'ok':False},{'t':3.,'ok':False},{'t':5.,'ok':True}])
        self.assertEqual(18,e._audit_e3(rows)['trials'])
        self.assertEqual({'lower_seconds':1.,'upper_seconds':5.},e.outage_bounds(next(iter(rows.values()))['http_samples']))
        bad=deepcopy(rows);next(iter(bad.values()))['fault']='wrong'
        with self.assertRaises(e.ExperimentError):e._audit_e3(bad)
        self.assertIsNone(e.outage_bounds([{'t':0.,'ok':False},{'t':2.,'ok':True}])['upper_seconds'])

    def test_e4_no_labels_resume_same_request_and_no_repeated_effects(self):
        value=spec();records={s:dict(seed=20261029,t17_receipt_sha256='a'*64,active_catalog_verified=True,policy_checks={'actual':True},new_requests=0,new_runs=0,new_versions=0) for s in value['E4']['scenarios']}
        records['shifted_no_labels'].update(quality=None,terminal='waiting_for_labels',new_requests=1,logical_request_id='same',windows=[dict(id='w1',start=0,end=1,pairs=200,vacancies=10,closed=True),dict(id='w2',start=1,end=2,pairs=200,vacancies=10,closed=True)])
        records['shifted_labeled'].update(logical_request_id='same',new_runs=1,new_versions=1,approved_labels_sha256='a'*64,reference_refreshed_after_active=True)
        e._audit_e4(value,records)
        for scenario,key,val in [('replay','new_runs',1),('cooldown','new_requests',True),('shifted_no_labels','quality',0),('shifted_labeled','logical_request_id','different')]:
            bad=deepcopy(records);bad[scenario][key]=val
            with self.subTest(scenario=scenario,key=key),self.assertRaises(e.ExperimentError):e._audit_e4(value,bad)

    def test_holdout_key_excludes_candidate_protocol_namespace_bytes(self):
        value=spec();other=deepcopy(value)
        for key in ('model_artifact_sha256','protocol_sha256','test_partition_sha256'):other['bindings'][key]='f'*64
        self.assertEqual(e.holdout_key(value),e.holdout_key(other))
        other['bindings']['protected_test_documents_sha256']='e'*64
        self.assertNotEqual(e.holdout_key(value),e.holdout_key(other))

    def test_metadata_only_holdout_key_matches_t18_shared_contract(self):
        value=spec();value['bindings']['protected_test_documents_sha256']='4448c775192e2b35b9164c8b64fa9ee92fdca5f620e0500c96cc38dce5d6a723'
        self.assertEqual('a42196e9a4aaf7b4b06544e16df350d946586e0fcdf0d706840281734108ce6f',e.holdout_key(value))

    def test_guard_precedes_reader_and_prevents_cross_candidate_bypass(self):
        value=spec();directory=make_study(self.root,value=value);guard=self.root/'native-guard.sqlite3';calls=[]
        def load():calls.append('read');return snapshot(value)
        e.evaluate_once(directory,guard,load,lambda snapshot:(calls.append('both') or numeric(value)))
        self.assertEqual(['read','both'],calls);self.assertEqual('passed',e.summarize(directory)['series']['E5']['status'])
        other=deepcopy(value);other['bindings']['model_artifact_sha256']='c'*64;other['bindings']['protocol_sha256']='d'*64
        second=make_study(self.root,'second',other)
        with self.assertRaises(e.ExperimentError):e.evaluate_once(second,guard,lambda:self.fail('second test read'),lambda _:self.fail('second score'))

    def test_failure_after_intent_consumes_holdout_without_reset(self):
        directory=make_study(self.root);guard=self.root/'guard'
        with self.assertRaises(RuntimeError):e.evaluate_once(directory,guard,lambda:(_ for _ in ()).throw(RuntimeError('private data')),lambda _:self.fail())
        self.assertEqual('recovery_required',e.summarize(directory)['series']['E5']['status'])
        self.assertNotIn('private data',(directory/'E5/failure.json').read_text())
        second=make_study(self.root,'second')
        with self.assertRaises(e.ExperimentError):e.evaluate_once(second,guard,lambda:self.fail(),lambda _:self.fail())

    def test_recovery_only_committed_numeric_no_reader_or_model(self):
        directory=make_study(self.root);value=e.study(directory)[0];guard=self.root/'guard'
        with patch.object(e.HoldoutGuard,'complete',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):e.evaluate_once(directory,guard,lambda:snapshot(value),lambda _:numeric(value))
        self.assertEqual('recovery_required',e.summarize(directory)['series']['E5']['status'])
        binding=e.read(directory/'E5/numeric.json')['binding']
        (directory/'E5/commit.json').unlink()  # crash after SQL-bound numeric, before commit publication
        e.HoldoutGuard(guard).complete(e.holdout_key(value),binding)
        self.assertEqual('passed',e.summarize(directory)['series']['E5']['status'])
        self.assertTrue((directory/'E5/failure.json').exists())

    def test_ground_truth_is_durable_before_callback_and_cannot_be_substituted(self):
        for mutation in ('candidate','ground_truth','duplicate','bool'):
            value=spec();directory=make_study(self.root,mutation,value);guard=self.root/(mutation+'-guard')
            def score(_snapshot):
                original=e.read(directory/'E5/snapshot.json')
                with e.HoldoutGuard(guard).connection() as db:
                    row=db.execute('SELECT snapshot_sha256 FROM holdouts').fetchone()
                self.assertEqual(e.sha(e.canonical(original)),row[0])
                result=numeric(value)
                if mutation=='candidate':result['rows'][0]['candidate_id']='substituted'
                if mutation=='ground_truth':result['rows'][0]['relevance']=0
                if mutation=='duplicate':result['rows'][1]=deepcopy(result['rows'][0])
                if mutation=='bool':result['rows'][0]['relevance']=True
                return result
            with self.subTest(mutation=mutation),self.assertRaises(e.ExperimentError):
                e.evaluate_once(directory,guard,lambda:snapshot(value),score)
            self.assertFalse((directory/'E5/numeric.json').exists())
            self.assertEqual('recovery_required',e.summarize(directory)['series']['E5']['status'])

    def test_pair_metadata_is_checked_before_score_and_snapshot_bind_is_no_overwrite(self):
        value=spec();directory=make_study(self.root,'wrongpair',value);guard=self.root/'wrongpair-guard'
        wrong=snapshot(value);wrong['rows'][0]['candidate_id']='replacement'
        with self.assertRaisesRegex(e.ExperimentError,'inventory_mismatch'):
            e.evaluate_once(directory,guard,lambda:wrong,lambda _:self.fail('scoring must not start'))
        directory=make_study(self.root,'correct',value);guard=self.root/'correct-guard'
        e.evaluate_once(directory,guard,lambda:snapshot(value),lambda _:numeric(value))
        raw=e.read(directory/'E5/numeric.json');ledger=e.HoldoutGuard(guard);key=e.holdout_key(value)
        snapshot_sha=e.sha((directory/'E5/snapshot.json').read_bytes());numeric_sha=e.sha((directory/'E5/numeric.json').read_bytes())
        ledger.bind_snapshot(key,raw['binding'],snapshot_sha);ledger.bind_numeric(key,raw['binding'],numeric_sha)
        with self.assertRaisesRegex(e.ExperimentError,'snapshot_conflict'):ledger.bind_snapshot(key,raw['binding'],'f'*64)
        with self.assertRaisesRegex(e.ExperimentError,'numeric_conflict'):ledger.bind_numeric(key,raw['binding'],'f'*64)

    def test_retained_numeric_or_snapshot_tamper_blocks_recovery(self):
        directory=make_study(self.root);value=e.study(directory)[0];guard=self.root/'guard'
        with patch.object(e.HoldoutGuard,'complete',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):e.evaluate_once(directory,guard,lambda:snapshot(value),lambda _:numeric(value))
        raw=e.read(directory/'E5/numeric.json');truth=e.read(directory/'E5/snapshot.json')
        truth['rows'][0]['relevance']=0;(directory/'E5/snapshot.json').write_bytes(e.canonical(truth))
        with self.assertRaisesRegex(e.ExperimentError,'retained_result_changed'):
            e.HoldoutGuard(guard).complete(e.holdout_key(value),raw['binding'])

    def test_two_processes_one_holdout_read(self):
        first=make_study(self.root,'first');second=make_study(self.root,'second');guard=self.root/'guard';marker=self.root/'reads'
        e.HoldoutGuard(guard);ctx=multiprocessing.get_context('fork')
        processes=[ctx.Process(target=concurrent_evaluate,args=(path,guard,marker)) for path in (first,second)]
        for p in processes:p.start()
        for p in processes:p.join(10);self.assertEqual(0,p.exitcode)
        self.assertEqual(['read'],marker.read_text().splitlines())

    def test_numeric_rebuild_without_test_or_model_and_bootstrap(self):
        value=spec(groups=10);directory=make_study(self.root,value=value)
        e.evaluate_once(directory,self.root/'guard',lambda:snapshot(value),lambda _:numeric(value))
        raw=directory/'E5/numeric.json';first=e.rebuild(raw,self.root/'tables');second=e.rebuild_numeric(raw)
        self.assertEqual(first,second);self.assertEqual(10,first['eligible_groups'])
        self.assertIsNotNone(first['paired_group_bootstrap']['ndcg_at_10']['percentile_95'])
        self.assertTrue((self.root/'tables/groups.csv').exists())
        with self.assertRaises(FileExistsError):e.rebuild(raw,self.root/'tables')

    def test_small_or_zero_positive_quality_no_fabricated_ci(self):
        value=spec();directory=make_study(self.root,value=value);raw=numeric(value)
        for row in raw['rows']:row['relevance']=0
        e.evaluate_once(directory,self.root/'guard',lambda:snapshot(value,raw),lambda _:raw)
        result=e.rebuild_numeric(directory/'E5/numeric.json')
        self.assertEqual(0,result['eligible_groups']);self.assertIsNone(result['paired_group_bootstrap']['ndcg_at_10']['delta'])
        self.assertIsNone(result['paired_group_bootstrap']['ndcg_at_10']['percentile_95'])

    def test_numeric_corruption_or_wrong_snapshot_never_silently_rebuilds(self):
        directory=make_study(self.root);value=e.study(directory)[0]
        e.evaluate_once(directory,self.root/'guard',lambda:snapshot(value),lambda _:numeric(value))
        p=directory/'E5/numeric.json';raw=e.read(p);raw['observations']['rows'][0]['model_score']=True;p.write_bytes(e.canonical(raw))
        with self.assertRaises(e.ExperimentError):e.rebuild_numeric(p)
        second=make_study(self.root,'second',dict(spec(),study_id='second'))
        # A different private ledger is only a test fixture, never a public reset API.
        with self.assertRaises(e.ExperimentError):e.evaluate_once(second,self.root/'toy-other-guard',lambda:{'sha256':'0'*64},lambda _:self.fail())

    def test_duplicate_json_keys_and_immutable_writes(self):
        with self.assertRaises(e.ExperimentError):e.decode(b'{"x":1,"x":2}')
        p=self.root/'receipt';e.immutable(p,{'x':1})
        with self.assertRaises(e.ExperimentError):e.immutable(p,{'x':2})
        self.assertEqual({'x':1},e.read(p))


if __name__=='__main__':unittest.main()
