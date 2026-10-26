"""Standalone worker contract with toy bytes/callbacks only; no ML or holdout IO."""
import ast
from copy import deepcopy
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[2] / 'src/ats_mlops/evaluation/holdout_worker.py'
SPEC = importlib.util.spec_from_file_location('toy_holdout_worker_under_test', SOURCE)
w = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = w
SPEC.loader.exec_module(w)


def rows(group='g1'):
    return [dict(vacancy_id=group,candidate_id=name,vacancy_text='PRIVATE VACANCY SENTINEL',
                 candidate_text='PRIVATE CANDIDATE SENTINEL',relevance=int(name=='a')) for name in ('a','b')]


def message(values=None):
    values = rows()+rows('g2') if values is None else values
    truth=w.truth_rows(values)
    return dict(schema_version=1,snapshot_sha256=w.sha(b''.join(w.canonical(row) for row in values)),
                pair_ids_sha256=w.pair_digest(truth),ground_truth_sha256=w.sha(w.canonical(truth)),snapshot={'rows':values})


def ready():
    return dict(status='ready',worker_sha256=w.worker_identity(),warmup=w.WARMUP,
                receipt=dict(isolated=True,bytecode_disabled=True,prior_project_import=False,network=False,
                    training_checkout_mounted=False,credentials=False,model_sha256='a'*64,
                    baseline_artifact_sha256='b'*64,source_tree_sha256='c'*64))


def toy_session(calls,output=None):
    class NativeFloat(float):pass
    def scorer(name,batch):
        if output is not None:
            assert (output/'truth.json').exists(), 'GT was not persisted before scoring'
        calls.append((name,len(batch)))
        return {(row['vacancy_id'],row['candidate_id']):NativeFloat(.8 if row['candidate_id']=='a' else .2) for row in batch}
    return w.Session(lambda batch:scorer('baseline',batch),lambda batch:scorer('model',batch),lambda:None,ready())


def toy_artifact(root):
    root.mkdir()
    for name,content in {'model.pkl':b'NOT A PICKLE','MLmodel':b'toy metadata','registered_model_meta':b'toy registration',
                         'code/ats_mlops/__init__.py':b'', 'code/ats_mlops/model/baseline.py':b'# toy baseline source',
                         'code/ats_mlops/data/dataset.py':b'# toy tokenizer source'}.items():
        path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(content)
    files={path.relative_to(root).as_posix():w.sha(path.read_bytes()) for path in root.rglob('*') if path.is_file()}
    source={name.removeprefix('code/ats_mlops/'):value for name,value in files.items() if name.startswith('code/ats_mlops/')}
    return dict(schema_version=1,dirty_worktree=False,git_sha='d'*40,
        model=dict(files=files,sha256=w.sha(w.canonical(files)),payload_sha256=files['model.pkl']),
        source_sha256=source,source_tree_sha256=w.sha(w.canonical(source)),
        features=dict(version='pair-tfidf-v1',names=['toy']),features_sha256='e'*64,
        mlflow=dict(model_name='ranking',model_version='14',run_id='f'*32,source='models:/m-'+'a'*32),
        environment=dict(python='3.11.17',packages={'toy-package':'1.0'},lockfile_sha256=w.sha(b'toy lock')))


def metadata(record):
    return dict(flavors=dict(sklearn=dict(code='code',pickled_model='model.pkl',serialization_format='cloudpickle'),
            python_function=dict(code='code',model_path='model.pkl',loader_module='mlflow.sklearn',predict_fn='predict_proba')),
        run_id=record['mlflow']['run_id'],model_id='m-'+'a'*32,metadata=dict(classes=[0,1],feature_version='pair-tfidf-v1',
            features_sha256=record['features_sha256'],training_git_sha=record['git_sha']))


class HoldoutWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)

    def test_module_has_no_top_level_project_or_ml_imports(self):
        tree=ast.parse(SOURCE.read_text())
        names=[]
        for node in tree.body:
            if isinstance(node,ast.Import):names.extend(item.name for item in node.names)
            elif isinstance(node,ast.ImportFrom):names.append(node.module or '')
        self.assertFalse(any(name.startswith(('ats_mlops','mlflow','sklearn','numpy','pandas','yaml')) for name in names))

    def test_inventory_payload_and_source_verified_before_loader(self):
        artifact=self.root/'artifact';record=toy_artifact(artifact)
        self.assertEqual(record['model']['files'],w.verify_artifact(record,artifact))
        request=dict(schema_version=1,record=record,warmup_rows=rows(),baseline_artifact_sha256=w.baseline_identity(record))
        (artifact/'model.pkl').write_bytes(b'corrupt toy bytes')
        with patch.object(w,'verify_environment'),patch.object(w,'_load_model',side_effect=AssertionError('unpickle reached')):
            with self.assertRaises(w.WorkerError):w.prepare(request,artifact)

    def test_symlink_bytecode_extra_file_and_source_tamper_reject(self):
        for variant in ('symlink','bytecode','extra','source'):
            artifact=self.root/variant;record=toy_artifact(artifact)
            if variant=='symlink':(artifact/'link').symlink_to(artifact/'model.pkl')
            elif variant=='bytecode':(artifact/'cache.pyc').write_bytes(b'toy')
            elif variant=='extra':(artifact/'unexpected').write_bytes(b'toy')
            else:record['source_tree_sha256']='0'*64
            with self.subTest(variant=variant),self.assertRaises(w.WorkerError):w.verify_artifact(record,artifact)

    def test_root_symlink_is_not_hidden_by_resolve(self):
        artifact=self.root/'artifact';record=toy_artifact(artifact);link=self.root/'link';link.symlink_to(artifact)
        request=dict(schema_version=1,record=record,warmup_rows=rows(),baseline_artifact_sha256=w.baseline_identity(record))
        with self.assertRaises(w.WorkerError):w.prepare(request,link)

    def test_exact_flavors_registered_identity_and_baseline_binding(self):
        record=toy_artifact(self.root/'artifact');model=metadata(record);registered={'model_name':'ranking','model_version':14}
        w.check_metadata(record,model,registered)
        for change in ('loader','version','source'):
            bad=deepcopy(model);reg=deepcopy(registered)
            if change=='loader':bad['flavors']['python_function']['predict_fn']='predict'
            elif change=='version':reg['model_version']=13
            else:bad['model_id']='m-'+'b'*32
            with self.subTest(change=change),self.assertRaises(w.WorkerError):w.check_metadata(record,bad,reg)
        changed=deepcopy(record);changed['source_sha256']['data/dataset.py']='0'*64
        self.assertNotEqual(w.baseline_identity(record),w.baseline_identity(changed))

    def test_environment_pins_namespace_credentials_and_lock_fail_closed(self):
        record=toy_artifact(self.root/'artifact')
        fake_sys=types.SimpleNamespace(flags=types.SimpleNamespace(isolated=True),dont_write_bytecode=True,version='3.11.17 toy')
        with patch.object(w,'sys',fake_sys),patch.object(w,'project_absent'),patch.object(w.importlib.metadata,'version',return_value='1.0'),patch.object(Path,'read_bytes',return_value=b'toy lock'),patch.dict(w.os.environ,{},clear=True):
            w.verify_environment(record)
            with patch.dict(w.os.environ,{'AWS_SECRET_ACCESS_KEY':'toy secret'}):
                with self.assertRaises(w.WorkerError):w.verify_environment(record)
            with patch.object(w.importlib.metadata,'version',return_value='other'):
                with self.assertRaises(w.WorkerError):w.verify_environment(record)
            with patch.object(Path,'read_bytes',return_value=b'wrong lock'):
                with self.assertRaises(w.WorkerError):w.verify_environment(record)
        with patch.dict(sys.modules,{'ats_mlops':types.SimpleNamespace()}):
            with self.assertRaises(w.WorkerError):w.project_absent()

    def test_prepare_order_and_approved_warmup_before_ready_without_real_model(self):
        artifact=self.root/'artifact';record=toy_artifact(artifact);calls=[]
        request=dict(schema_version=1,record=record,warmup_rows=rows(),baseline_artifact_sha256=w.baseline_identity(record))
        def warm(name,batch):calls.append(name);return {(r['vacancy_id'],r['candidate_id']):.5 for r in batch}
        with (patch.object(w,'verify_environment',side_effect=lambda _:calls.append('environment')),
             patch.object(w,'_metadata',side_effect=lambda *_:calls.append('metadata')),
             patch.object(w,'_load_model',side_effect=lambda *_:(calls.append('load') or object())),
             patch.object(w,'_helpers',side_effect=lambda *_:(lambda batch:warm('baseline-warmup',batch),lambda batch:warm('model-warmup',batch))),
             patch.object(w,'check_namespace')):
            session=w.prepare(request,artifact)
        self.assertEqual(['environment','metadata','load','baseline-warmup','model-warmup'],calls)
        self.assertEqual(w.worker_identity(),session.ready['worker_sha256'])

    def test_ready_before_stdin_truth_before_scores_and_each_group_once(self):
        output=self.root/'out';output.mkdir();calls=[];transcript=io.StringIO();payload=message();session=toy_session(calls,output)
        class AfterReady(io.BytesIO):
            def read(inner,*args):
                self.assertEqual('ready',json.loads(transcript.getvalue())['status']);self.assertEqual([],calls)
                return super().read(*args)
        result=w.serve(session,output,AfterReady(w.canonical(payload)),transcript)
        self.assertEqual('completed',result['status'])
        self.assertEqual([('baseline',2),('model',2)]*2,calls)
        numeric=w.read_json(output/'numeric.json');self.assertTrue(all(type(r['model_score']) is float for r in numeric['rows']))
        self.assertEqual(['ready','completed'],[json.loads(line)['status'] for line in transcript.getvalue().splitlines()])
        for path in output.glob('*.json'):
            self.assertNotIn('PRIVATE',path.read_text());self.assertNotIn('vacancy_text',path.read_text())

    def test_snapshot_pair_inventory_ground_truth_and_canonical_bytes(self):
        original=message();w.validate_snapshot(original)
        for variant in ('pair','label','text','duplicate','bool'):
            bad=deepcopy(original);row=bad['snapshot']['rows'][0]
            if variant=='pair':row['candidate_id']='changed'
            elif variant=='label':row['relevance']=0
            elif variant=='text':row['candidate_text']='changed'
            elif variant=='duplicate':bad['snapshot']['rows'].append(deepcopy(row))
            else:row['relevance']=True
            if variant in ('pair','label'):
                bad['snapshot_sha256']=w.sha(b''.join(w.canonical(r) for r in bad['snapshot']['rows']))
            with self.subTest(variant=variant),self.assertRaises(w.WorkerError):w.validate_snapshot(bad)

    def test_nan_bool_missing_extra_scored_pair_reject(self):
        batch=rows();good={(r['vacancy_id'],r['candidate_id']):.5 for r in batch}
        for invalid in (True,float('nan'),float('inf'),-1,1.1):
            bad=dict(good);bad[next(iter(bad))]=invalid
            with self.subTest(invalid=invalid),self.assertRaises(w.WorkerError):w.checked_scores(bad,batch)
        with self.assertRaises(w.WorkerError):w.checked_scores({},batch)
        with self.assertRaises(w.WorkerError):w.checked_scores(dict(good,extra=.5),batch)

    def saved(self):
        output=self.root/'out';output.mkdir();payload=message();batch,truth=w.validate_snapshot(payload)
        session=toy_session([]);numeric=w.evaluate(session,batch,truth,clock=lambda:1.)
        for name,value in [('ready.json',session.ready),('truth.json',truth),('numeric.json',numeric)]:w.publish(output/name,value)
        return output,numeric

    def test_recover_complete_numeric_only_without_model_loader_or_scorer(self):
        output,_=self.saved()
        with patch.object(w,'prepare',side_effect=AssertionError('model load')),patch.object(w,'evaluate',side_effect=AssertionError('rescore')):
            result=w.recover(output);self.assertEqual(result,w.recover(output))
        self.assertEqual('completed',result['status'])

    def test_missing_or_partial_numeric_never_recovers_or_rescores(self):
        output,_=self.saved();(output/'numeric.json').unlink()
        with self.assertRaises(w.WorkerError):w.recover(output)
        (output/'numeric.json').write_bytes(b'{')
        with self.assertRaises(ValueError):w.recover(output)
        self.assertFalse((output/'commit.json').exists())

    def test_recovery_rejects_pair_gt_model_and_latency_mutations(self):
        output,original=self.saved()
        for variant in ('pair','gt','model','bool','latency'):
            value=deepcopy(original)
            if variant=='pair':value['rows'][0]['candidate_id']='changed'
            elif variant=='gt':value['rows'][0]['relevance']=0
            elif variant=='model':value['model_artifact_sha256']='0'*64
            elif variant=='bool':value['rows'][0]['relevance']=True
            else:value['latencies'][0]['model_seconds']=float('inf')
            if variant=='latency':(output/'numeric.json').write_text(json.dumps(value))
            else:(output/'numeric.json').write_bytes(w.canonical(value))
            with self.subTest(variant=variant),self.assertRaises(w.WorkerError):w.recover(output)

    def test_atomic_publish_failure_does_not_create_torn_final(self):
        target=self.root/'receipt.json'
        with patch.object(w.os,'link',side_effect=OSError('toy failure')):
            with self.assertRaises(OSError):w.publish(target,{'numeric':1})
        self.assertFalse(target.exists());self.assertFalse(list(self.root.glob('.pending-*')))
        w.publish(target,{'numeric':1})
        with self.assertRaises(w.WorkerError):w.publish(target,{'numeric':2})
        self.assertEqual({'numeric':1},w.read_json(target))

    def test_recovery_commit_has_strict_canonical_types_and_worker_identity(self):
        output,_=self.saved();w.recover(output);commit=w.read_json(output/'commit.json');commit['schema_version']=True
        (output/'commit.json').write_bytes(w.canonical(commit))
        with self.assertRaises(w.WorkerError):w.recover(output)
        (output/'commit.json').unlink();identity=w.read_json(output/'ready.json');identity['worker_sha256']='0'*64
        (output/'ready.json').write_bytes(w.canonical(identity))
        with self.assertRaises(w.WorkerError):w.recover(output)

    def test_fresh_isolated_recovery_process_needs_no_artifact_or_project_import(self):
        output,_=self.saved()
        result=subprocess.run([sys.executable,'-I','-B',str(SOURCE),'--recover','--output',str(output)],capture_output=True,text=True)
        self.assertEqual(0,result.returncode,result.stderr);self.assertFalse(result.stderr)
        self.assertEqual('completed',json.loads(result.stdout)['status'])

    def test_main_failure_is_sanitized_and_duplicate_json_rejected(self):
        request=self.root/'request.json';request.write_text('{"private":"secret.person@example.invalid"}')
        captured=io.StringIO()
        with redirect_stdout(captured):self.assertEqual(1,w.main(['--request',str(request),'--output',str(self.root)]))
        self.assertEqual({'status':'failed','reason':'holdout_worker_failed'},json.loads(captured.getvalue()))
        self.assertNotIn('secret.person',captured.getvalue())
        with self.assertRaises(w.WorkerError):w.decode(b'{"schema_version":1,"schema_version":1}')


if __name__=='__main__':unittest.main()
