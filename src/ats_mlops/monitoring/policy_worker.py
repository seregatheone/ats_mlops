"""Fixed native-volume helper. Host installs proofs; DAG only applies their IDs.

No Docker socket, provider runtime, training, Registry or release-controller API
is used here. Only host-side batch code may invoke install/ack/result operations.
"""

import argparse
import base64
from dataclasses import fields
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import uuid

from . import policy as p


MAX_BYTES = 32 * 1024 * 1024


def sha(content):
    return hashlib.sha256(content).hexdigest()


def decode(content):
    def pairs(items):
        result = {}
        for key, value in items:
            p.require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    return json.loads(content, object_pairs_hook=pairs, parse_constant=lambda value: (_ for _ in ()).throw(p.PolicyError("nonfinite_json")))


def typed(kind, value):
    p.require(isinstance(value, dict) and set(value) == {f.name for f in fields(kind)}, "invalid_typed_receipt")
    value = dict(value)
    if kind is p.Cohort:
        value["model"] = typed(p.ModelIdentity, value["model"])
    elif kind is p.ReferenceBinding:
        value["cohort"] = typed(p.Cohort, value["cohort"])
    elif kind is p.WindowEvidence:
        value["cohort"] = typed(p.Cohort, value["cohort"])
        for key in ("start", "end"):
            value[key] = p.parse_stamp(value[key])
    elif kind is p.DatasetApproval:
        value["approved_at"] = p.parse_stamp(value["approved_at"])
    elif kind is p.CandidateEvidence:
        value["model"] = typed(p.ModelIdentity, value["model"])
    elif kind is p.VerifiedActive:
        value["reference"] = typed(p.ReferenceBinding, value["reference"])
        p.require(isinstance(value["checks"], list), "invalid_active_checks")
        value["checks"] = tuple(value["checks"])
    return kind(**value)


def checked_receipt(receipt):
    p.require(isinstance(receipt, dict) and set(receipt) == {"schema_version", "kind", "value", "proof"}
              and type(receipt["schema_version"]) is int and receipt["schema_version"] == 1
              and receipt["kind"] in {"window", "dataset", "reference"}, "invalid_verified_receipt")
    proof = receipt["proof"]
    p.require(set(proof) == {"source_git_sha", "input_inventory_sha256", "active_journal_sha256", "active_epoch", "active_release_id"}, "invalid_verifier_proof")
    p.require(isinstance(proof["source_git_sha"], str) and len(proof["source_git_sha"]) == 40
              and all(c in '0123456789abcdef' for c in proof["source_git_sha"]), "invalid_verifier_source")
    p.fingerprint(proof["input_inventory_sha256"])
    p.fingerprint(proof["active_journal_sha256"])
    p.identifier(proof["active_release_id"])
    p.require(type(proof["active_epoch"]) is int and proof["active_epoch"] > 0, "invalid_epoch")
    value = receipt["value"]
    if receipt["kind"] == "window":
        p.require(set(value) == {"window", "reference", "execution"}, "invalid_window_receipt")
        window = typed(p.WindowEvidence, value["window"])
        reference = typed(p.ReferenceBinding, value["reference"])
        typed(p.ExecutionPlan, value["execution"])
        p.require(window.cohort == reference.cohort and window.cohort.epoch == proof["active_epoch"]
                  and window.cohort.release_id == proof["active_release_id"], "window_proof_mismatch")
    elif receipt["kind"] == "dataset":
        p.require(set(value) == {"request_id", "approval"}, "invalid_dataset_receipt")
        p.identifier(value["request_id"])
        typed(p.DatasetApproval, value["approval"])
    else:
        p.require(set(value) == {"request_id", "active"}, "invalid_reference_receipt")
        p.identifier(value["request_id"])
        active = typed(p.VerifiedActive, value["active"])
        p.require(active.reference.cohort.epoch == proof["active_epoch"]
                  and active.reference.cohort.release_id == proof["active_release_id"], "active_proof_mismatch")
    return receipt


def install(root, receipt):
    checked_receipt(receipt)
    content = (p.canonical(receipt) + '\n').encode()
    identity = sha(content)
    directory = root / 'receipts'
    directory.mkdir(parents=True, exist_ok=True)
    p.require(not root.is_symlink() and not directory.is_symlink(), "unsafe_receipt_directory")
    path = directory / (identity + '.json')
    if path.exists():
        p.require(not path.is_symlink() and path.read_bytes() == content, "receipt_conflict")
    else:
        temporary=directory/('.pending-'+uuid.uuid4().hex)
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o440)
            with os.fdopen(fd, 'wb') as stream:
                stream.write(content); stream.flush(); os.fsync(stream.fileno())
            try:
                # Same-filesystem hard-link publish is atomic and exclusive.
                # A final receipt never names unflushed/partially written bytes.
                os.link(temporary,path,follow_symlinks=False)
            except FileExistsError:
                p.require(not path.is_symlink() and path.read_bytes()==content,'receipt_conflict')
        finally:
            temporary.unlink(missing_ok=True)
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return {"receipt_id": identity, "receipt_sha256": identity}


def load_receipt(root, request):
    p.require(set(request) == {"receipt_id", "receipt_sha256"}, "invalid_receipt_reference")
    p.fingerprint(request['receipt_id']); p.fingerprint(request['receipt_sha256'])
    path = root / 'receipts' / (request['receipt_id'] + '.json')
    p.require(not path.is_symlink() and not path.parent.is_symlink(), "unsafe_receipt_path")
    content = path.read_bytes()
    p.require(len(content) <= MAX_BYTES and sha(content) == request['receipt_sha256'] == request['receipt_id'], "verified_receipt_changed")
    receipt = checked_receipt(decode(content))
    p.require(content == (p.canonical(receipt) + '\n').encode(), "noncanonical_receipt")
    return receipt


def export_candidate(root, request_id):
    """Export only completed T10 artifacts; never config.local/auth/SQLite/source."""
    from ats_mlops import orchestration
    from ats_mlops.quality.artifacts import strict_inventory
    store = p.PolicyStore(root / 'policy.sqlite3')
    request = store.request(request_id)
    work = Path(os.environ.get('ATS_WORK_ROOT', '/work')) / 'requests' / request_id
    state = Path(os.environ.get('ATS_STATE_ROOT', '/state')) / 'requests.sqlite3'
    p.require(state.exists(), "candidate_not_complete")
    db = sqlite3.connect(f'file:{state}?mode=ro', uri=True)
    try:
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        row = db.execute('SELECT status,receipt_hash FROM requests WHERE request_id=?', (request_id,)).fetchone()
    finally:
        db.close()
    if row is None or row[0] != 'completed':
        return {'state': row[0] if row else 'not_started', 'request_id': request_id}
    conf = orchestration.read(work / 'request.json')
    orchestration.validate(conf)
    p.require(conf['run_id'] == request['run_id'] and conf['git_sha'] == request['execution']['source_git_sha']
              and conf['params_hash'] == request['execution']['parameters_sha256']
              and p.canonical(conf['parameters']) == request['execution']['parameters_json'], "candidate_execution_mismatch")
    orchestration.verify_result(work, row[1], conf)
    workspace = work / 'run/workspace'
    record = orchestration.read(workspace/'artifacts/training/final/run_manifest.json')
    p.require(record['params_sha256'] == request['execution']['effective_model_sha256']
              and record['environment']['image']['id'] == request['execution']['training_image_id']
              and record['data_manifest_sha256'] == request['dataset']['manifest_sha256'],'candidate_execution_mismatch')
    strict_inventory(workspace / 'artifacts/training/final/model')
    roots = ('artifacts/prepared', 'artifacts/training', 'artifacts/evaluation')
    paths = [path for relative in roots for path in (workspace / relative).rglob('*') if path.is_file()]
    paths += [workspace / name for name in ('dataset-binding.json','pipeline-inputs.json','params.yaml','dvc.lock',
               'data/train.jsonl.dvc','data/validation.jsonl.dvc','data/split.json.dvc')]
    p.require(len(paths) <= 2048, "candidate_export_limit")
    files, total = {}, 0
    for path in sorted(paths):
        relative = path.relative_to(workspace).as_posix()
        p.require(not path.is_symlink() and path.resolve().is_relative_to(workspace.resolve())
                  and path.name not in {'source.jsonl','corpus.jsonl','test.jsonl','split.json'}, "unsafe_candidate_export")
        content = path.read_bytes(); total += len(content)
        p.require(total <= MAX_BYTES, "candidate_export_limit")
        files[relative] = {'sha256': sha(content), 'base64': base64.b64encode(content).decode('ascii')}
    p.require(all(sha((workspace / name).read_bytes()) == item['sha256'] for name, item in files.items()), "candidate_changed_during_export")
    return {'state':'completed','request_id':request_id,'candidate_receipt':orchestration.read(work/'candidate.json'), 'files':files}


def execute(action, request, root, *, now=None):
    now = now or datetime.now(timezone.utc)
    p.require(not root.is_symlink(), "unsafe_state_root")
    if action == 'install':
        p.require(set(request) == {'receipt'}, "invalid_install_request")
        return install(root, request['receipt'])
    if action == 'runtime':
        p.require(request == {},'unexpected_fields')
        identity = decode(Path(os.environ['ATS_RUNTIME_IMAGE_FILE']).read_bytes())
        p.require(isinstance(identity['id'],str) and identity['id'].startswith('sha256:') and len(identity['id']) == 71,'invalid_runtime_identity')
        return {'image_id':identity['id']}
    if action in {'apply', 'reference-intent', 'reconcile-reference'}:
        event_id = request.get('event_id')
        receipt = load_receipt(root, {key:value for key,value in request.items() if key != 'event_id'})
        value = receipt['value']
        if action == 'apply' and receipt['kind'] == 'window':
            initial = None if (root/'policy.sqlite3').exists() else typed(p.ReferenceBinding, value['reference'])
            store = p.PolicyStore(root/'policy.sqlite3', initial)
            result = store.observe(typed(p.WindowEvidence, value['window']), now=now)
            if result['state'] == 'waiting_for_labels':
                store.bind_execution(result['request_id'], typed(p.ExecutionPlan, value['execution']), now=now)
            return result
        p.require((root/'policy.sqlite3').exists(),'policy_not_initialized')
        store = p.PolicyStore(root/'policy.sqlite3')
        if action == 'apply' and receipt['kind'] == 'dataset':
            request = store.request(value['request_id'])
            p.require(request['incumbent']['cohort']['epoch'] == receipt['proof']['active_epoch']
                      and request['incumbent']['cohort']['release_id'] == receipt['proof']['active_release_id'], "stale_dataset_proof")
            result = store.accept_labels(value['request_id'], typed(p.DatasetApproval, value['approval']), now=now)
        elif action == 'reference-intent' and receipt['kind'] == 'reference':
            result = store.begin_reference_update(value['request_id'], typed(p.VerifiedActive, value['active']), now=now)
        elif action == 'reconcile-reference' and receipt['kind'] == 'reference':
            result = store.reconcile_reference(event_id, typed(p.VerifiedActive, value['active']), now=now)
        else:
            raise p.PolicyError('receipt_action_forbidden')
        return {'request_id':result['request_id'],'state':result['state'],'run_id':result['run_id']}
    if not (root/'policy.sqlite3').exists():
        if action == 'snapshot' and request == {}:
            return {'state':'uninitialized'}
        if action == 'pending' and request == {}:
            return []
        raise p.PolicyError('policy_not_initialized')
    store = p.PolicyStore(root/'policy.sqlite3')
    if action == 'snapshot':
        p.require(request == {}, "unexpected_fields"); return store.snapshot()
    if action == 'pending':
        p.require(request == {}, "unexpected_fields"); return store.pending_outbox()
    p.identifier(request['request_id'])
    if action == 'request':
        p.require(set(request) == {'request_id'}, "unexpected_fields"); return store.request(request['request_id'])
    if action == 'outbox':
        p.require(set(request) == {'request_id','kind'} and request['kind'] in {'candidate','reference'},'unexpected_fields')
        with store._connection() as db:
            row = db.execute('SELECT * FROM outbox WHERE request_id=? AND kind=?',(request['request_id'],request['kind'])).fetchone()
            p.require(row is not None,'outbox_unknown')
            return {'record':store._checked_outbox(row),'state':row['state']}
    if action == 'export':
        p.require(set(request) == {'request_id'}, "unexpected_fields"); return export_candidate(root, request['request_id'])
    if action == 'ack':
        p.require(set(request) == {'request_id','event_id','receipt_sha256'}, "unexpected_fields")
        with store._connection() as db:
            row = db.execute('SELECT request_id FROM outbox WHERE id=?',(request['event_id'],)).fetchone()
            p.require(row is not None and row[0] == request['request_id'], 'ack_request_mismatch')
        result = store.ack_dispatch(request['event_id'], request['receipt_sha256'], now=now)
    elif action == 'candidate':
        p.require(set(request) == {'request_id','evidence'}, "unexpected_fields")
        result = store.record_candidate(request['request_id'], typed(p.CandidateEvidence, request['evidence']), now=now)
    elif action == 'failure':
        p.require(set(request) == {'request_id','ambiguous'}, "unexpected_fields")
        result = store.record_failure(request['request_id'], ambiguous=request['ambiguous'], now=now)
    else:
        raise p.PolicyError('action_forbidden')
    return {'request_id':result['request_id'],'state':result['state'],'run_id':result['run_id']}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=('install','apply','snapshot','pending','outbox','runtime','request','ack','candidate','failure',
                                         'reference-intent','reconcile-reference','export'))
    parser.add_argument('--root', type=Path, default=Path('/state/retraining'))
    args = parser.parse_args(argv)
    envelope = {'schema_version':1,'worker_sha256':sha(Path(__file__).read_bytes()),
                'policy_sha256':sha(Path(p.__file__).read_bytes()),'outcome':'failed'}
    try:
        content = sys.stdin.buffer.read(MAX_BYTES + 1)
        p.require(len(content) <= MAX_BYTES, 'request_limit')
        envelope['result'] = execute(args.action, decode(content), args.root)
        envelope['outcome'] = 'passed'
    except Exception as error:
        reason = str(error) if isinstance(error, p.PolicyError) else 'policy_worker_failed'
        envelope['reason'] = reason
    print(json.dumps(envelope,sort_keys=True,allow_nan=False))
    return 0 if envelope['outcome'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
