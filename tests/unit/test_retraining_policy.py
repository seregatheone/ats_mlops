"""Policy/storage-only checks: controlled clocks, concurrent writers and exits."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import multiprocessing
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ats_mlops.monitoring.policy import (
    ACTIVE_CHECKS, CandidateEvidence, Cohort, DatasetApproval, ExecutionPlan, ModelIdentity,
    PolicyError, PolicyStore, ReferenceBinding, VerifiedActive, WindowEvidence, canonical, payload,
)


def h(value):
    return hashlib.sha256(value.encode()).hexdigest()


START = datetime(2026, 10, 26, 12, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)
MODEL = ModelIdentity('ranking', '4', 'run-4', h('model-4'))
COHORT = Cohort(h('reference-1'), MODEL, 'release-A', 1, h('monitoring-config'))
REFERENCE = ReferenceBinding(COHORT, 'dataset-v1', h('manifest-1'), h('policy-1'), h('labels-1'))
NEXT_MODEL = ModelIdentity('ranking', '5', 'run-5', h('model-5'))


def execution():
    parameters = {'model': {'seed': 20261026}, 'search': {'mode': 'frozen'}, 'smoke': False,
                  'evaluation': {'score_decimals': 12}}
    raw = __import__('json').dumps(parameters, sort_keys=True, separators=(',', ':'))
    return ExecutionPlan('a' * 40, 'sha256:' + h('image'), canonical(parameters), h(raw), h(canonical(parameters['model']) + '\n'))


def window(index, **changes):
    return replace(WindowEvidence(f'window-{index}', START + index * HOUR, START + (index + 1) * HOUR,
                                  COHORT, h(f'report-{index}'), 200, 10, True), **changes)


def approval(**changes):
    return replace(DatasetApproval('dataset-v2', 2, h('manifest-2'), h('policy-2'), h('policy-1'),
        's3://ats-dvc/orchestration/bundles/dataset-v2/bundle.json', h('bundle-2'), h('admission-2'),
        h('labels-2'), h('labels-1'), 20, 'independent_feedback', h('dataset-approval'),
        'agent-operator', START + 2 * HOUR, True, True), **changes)


def candidate(passed=True):
    if not passed:
        return CandidateEvidence('candidate-B', NEXT_MODEL, h('gate'), False)
    return CandidateEvidence('candidate-B', NEXT_MODEL, h('gate'), True, 'release-B',
                             h('release-manifest'), h('release-check'), 'sha256:' + h('image'))


def active(**changes):
    reference = ReferenceBinding(Cohort(h('reference-2'), NEXT_MODEL, 'release-B', 2, h('monitoring-config')),
                                 'dataset-v2', h('manifest-2'), h('policy-2'), h('labels-2'))
    return replace(VerifiedActive(reference, h('release-manifest'), h('release-check'),
                                  h('release-approval'), h('journal'), 'sha256:' + h('image'), ACTIVE_CHECKS), **changes)


def observe_child(path, evidence, queue):
    try:
        queue.put(PolicyStore(path).observe(evidence, now=START + 2 * HOUR))
    except Exception as error:
        queue.put({'error': type(error).__name__})


def labels_exit(path, request_id, before_commit):
    store = PolicyStore(path)
    if before_commit:
        def exit_before_commit(*args):
            os._exit(41)
        store._save = exit_before_commit
    store.accept_labels(request_id, approval(), now=START + 2 * HOUR + MINUTE)
    os._exit(42)


class RetrainingPolicyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'policy.sqlite3'
        self.store = PolicyStore(self.path, REFERENCE)

    def accepted(self):
        self.assertEqual('first_signal', self.store.observe(window(0), now=START + HOUR)['reason'])
        result = self.store.observe(window(1), now=START + 2 * HOUR)
        self.assertEqual('waiting_for_labels', result['state'])
        self.store.bind_execution(result['request_id'], execution(), now=START + 2 * HOUR)
        return result['request_id']

    def dispatched(self):
        request_id = self.accepted()
        self.store.accept_labels(request_id, approval(), now=START + 2 * HOUR + MINUTE)
        event = self.store.pending_outbox()[0]
        self.store.ack_dispatch(event['event_id'], h('run-receipt'), now=START + 2 * HOUR + 2 * MINUTE)
        return request_id

    def waiting_approval(self):
        request_id = self.dispatched()
        self.store.record_candidate(request_id, candidate(), now=START + 2 * HOUR + 3 * MINUTE)
        return request_id

    def test_reference_refresh_cannot_replace_frozen_monitoring_config(self):
        request_id=self.waiting_approval(); target=active()
        changed=replace(target,reference=replace(target.reference,cohort=replace(target.reference.cohort,config_sha256=h('other-config'))))
        before=self.store.snapshot()
        with self.assertRaisesRegex(PolicyError,'active_target_mismatch'):
            self.store.begin_reference_update(request_id,changed,now=START+4*HOUR)
        self.assertEqual(before,self.store.snapshot())
        self.assertEqual('waiting_approval',self.store.request(request_id)['state'])

    def test_two_windows_accept_once_and_window_hash_conflicts_fail(self):
        request_id = self.accepted()
        original = self.store.observe(window(1), now=START + 2 * HOUR)
        self.assertEqual(request_id, original['request_id'])
        self.assertEqual(original, self.store.observe(window(1), now=START + 2 * HOUR + MINUTE))
        self.assertEqual(1, self.store.snapshot()['requests'])
        self.assertEqual([], self.store.pending_outbox())
        with self.assertRaisesRegex(PolicyError, 'window_id_conflict'):
            self.store.observe(window(1, report_sha256=h('different')), now=START + 2 * HOUR + MINUTE)
        self.assertEqual(2, self.store.snapshot()['windows'])

    def test_closed_overlap_order_and_backward_clock_are_rejected_without_commit(self):
        with self.assertRaisesRegex(PolicyError, 'window_not_closed'):
            self.store.observe(window(0, closed=False), now=START + HOUR)
        with self.assertRaisesRegex(PolicyError, 'window_not_closed'):
            self.store.observe(window(0), now=START)
        self.store.observe(window(0), now=START + 2 * HOUR)
        with self.assertRaisesRegex(PolicyError, 'backward_clock'):
            self.store.observe(window(0), now=START + HOUR)
        for evidence in (window(1, start=START + HOUR / 2), window(-1)):
            with self.assertRaisesRegex(PolicyError, 'window_overlap_or_out_of_order'):
                self.store.observe(evidence, now=START + 2 * HOUR)
        self.assertEqual(1, self.store.snapshot()['windows'])

    def test_typed_counts_flags_fingerprints_and_naive_time_fail(self):
        for changes in ({'pairs': True}, {'pairs': 200.0}, {'jobs': 201}, {'drifted': 1},
                        {'closed': 1}, {'report_sha256': 'bad'}, {'start': START.replace(tzinfo=None)}):
            with self.subTest(changes=changes), self.assertRaises(PolicyError):
                window(0, **changes)
        with self.assertRaisesRegex(PolicyError, 'invalid_epoch'):
            replace(COHORT, epoch=True)
        with self.assertRaisesRegex(PolicyError, 'invalid_clock'):
            self.store.observe(window(0), now=START.replace(tzinfo=None))

    def test_normalized_offsets_replay_same_window(self):
        original = self.store.observe(window(0), now=START + HOUR)
        offset = timezone(timedelta(hours=3))
        same = window(0, start=START.astimezone(offset), end=(START + HOUR).astimezone(offset))
        self.assertEqual(original, self.store.observe(same, now=START + HOUR))

    def test_control_undefined_small_or_gap_break_streak(self):
        for changes in ({'pairs': 199}, {'jobs': 9}, {'drifted': False}, {'drifted': None}):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as directory:
                store = PolicyStore(Path(directory) / 'db', REFERENCE)
                store.observe(window(0), now=START + HOUR)
                self.assertIsNone(store.observe(window(1, **changes), now=START + 2 * HOUR)['request_id'])
                self.assertEqual('first_signal', store.observe(window(2), now=START + 3 * HOUR)['reason'])
                self.assertIsNotNone(store.observe(window(3), now=START + 4 * HOUR)['request_id'])
        self.store.observe(window(0), now=START + HOUR)
        self.assertEqual('first_signal', self.store.observe(window(2), now=START + 3 * HOUR)['reason'])

    def test_each_exact_cohort_coordinate_is_enforced(self):
        self.store.observe(window(0), now=START + HOUR)
        variants = (replace(COHORT, reference_sha256=h('other-ref')), replace(COHORT, config_sha256=h('other-config')),
                    replace(COHORT, model=replace(MODEL, version='6')), replace(COHORT, model=replace(MODEL, run_id='other-run')),
                    replace(COHORT, release_id='other-release'), replace(COHORT, epoch=2))
        for cohort in variants:
            with self.subTest(cohort=cohort), self.assertRaisesRegex(PolicyError, 'reference_mismatch'):
                self.store.observe(window(1, cohort=cohort), now=START + 2 * HOUR)
        self.assertEqual(1, self.store.snapshot()['windows'])

    def test_singleton_holds_even_after_cooldown_while_waiting_labels(self):
        request_id = self.accepted()
        for index in (26, 27):
            result = self.store.observe(window(index), now=START + (index + 1) * HOUR)
            self.assertEqual(('inflight', request_id), (result['reason'], result['request_id']))
        self.assertEqual(1, self.store.snapshot()['requests'])

    def test_namespace_hash_only_unapproved_or_nonindependent_labels_stay_waiting(self):
        request_id = self.accepted()
        for changes in ({'labelset_sha256': REFERENCE.labelset_sha256}, {'new_label_count': 0},
                        {'approved': False}, {'independent': False}, {'dataset_version': REFERENCE.dataset_version},
                        {'manifest_sha256': REFERENCE.manifest_sha256}):
            with self.subTest(changes=changes):
                result = self.store.accept_labels(request_id, approval(**changes), now=START + 2 * HOUR + MINUTE)
                self.assertEqual('waiting_for_labels', result['state'])
                self.assertEqual([], self.store.pending_outbox())
        for changes in ({'parent_labelset_sha256': h('wrong-parent')}, {'reservation_parent_sha256': h('wrong-policy')}):
            with self.assertRaises(PolicyError):
                self.store.accept_labels(request_id, approval(**changes), now=START + 2 * HOUR + MINUTE)

    def test_approved_v2_resumes_same_request_inside_cooldown_with_one_outbox(self):
        request_id = self.accepted()
        accepted = self.store.request(request_id)['accepted_at']
        result = self.store.accept_labels(request_id, approval(), now=START + 2 * HOUR + MINUTE)
        self.assertEqual((request_id, 'queued', accepted), (result['request_id'], result['state'], result['accepted_at']))
        pending = self.store.pending_outbox()
        self.assertEqual(1, len(pending))
        self.assertEqual(result, self.store.accept_labels(request_id, approval(), now=START + 2 * HOUR + 2 * MINUTE))
        self.assertEqual(pending, PolicyStore(self.path).pending_outbox())
        with self.assertRaisesRegex(PolicyError, 'approval_conflict'):
            self.store.accept_labels(request_id, approval(approval_sha256=h('other')), now=START + 2 * HOUR + 2 * MINUTE)

    def test_approval_types_sources_and_future_time_fail_closed(self):
        for changes in ({'manifest_schema': True}, {'manifest_schema': 1}, {'new_label_count': True},
                        {'approved': 1}, {'label_source': 'prediction'}, {'bundle_uri': 's3://foreign/data'}):
            with self.subTest(changes=changes), self.assertRaises(PolicyError):
                approval(**changes)
        request_id = self.accepted()
        with self.assertRaisesRegex(PolicyError, 'approval_from_future'):
            self.store.accept_labels(request_id, approval(approved_at=START + 3 * HOUR), now=START + 2 * HOUR)

    def test_concurrent_processes_create_only_one_deterministic_request(self):
        self.store.observe(window(0), now=START + HOUR)
        context = multiprocessing.get_context('spawn')
        queue = context.Queue()
        children = [context.Process(target=observe_child, args=(self.path, window(1), queue)) for _ in range(2)]
        for child in children:
            child.start()
        results = [queue.get(timeout=10) for _ in children]
        for child in children:
            child.join(10)
            self.assertEqual(0, child.exitcode)
        self.assertEqual(results[0], results[1])
        self.assertEqual(1, self.store.snapshot()['requests'])
        with tempfile.TemporaryDirectory() as directory:
            other = PolicyStore(Path(directory) / 'db', REFERENCE)
            other.observe(window(0), now=START + HOUR)
            self.assertEqual(results[0]['request_id'], other.observe(window(1), now=START + 2 * HOUR)['request_id'])

    def test_exit_before_commit_rolls_back_and_after_commit_preserves_intent(self):
        request_id = self.accepted()
        context = multiprocessing.get_context('spawn')
        for before, code in ((True, 41), (False, 42)):
            child = context.Process(target=labels_exit, args=(self.path, request_id, before))
            child.start(); child.join(10)
            self.assertEqual(code, child.exitcode)
            reopened = PolicyStore(self.path)
            self.assertEqual('waiting_for_labels' if before else 'queued', reopened.request(request_id)['state'])
            self.assertEqual(0 if before else 1, len(reopened.pending_outbox()))

    def test_accepted_dispatch_crash_uses_same_intent_then_recovers_without_duplicate(self):
        request_id = self.accepted()
        self.store.accept_labels(request_id, approval(), now=START + 2 * HOUR + MINUTE)
        event = self.store.pending_outbox()[0]
        self.store.record_failure(request_id, ambiguous=True, now=START + 2 * HOUR + 2 * MINUTE)
        reopened = PolicyStore(self.path)
        self.assertEqual([event], reopened.pending_outbox())
        self.assertEqual(request_id, reopened.snapshot()['inflight'])
        resumed = reopened.ack_dispatch(event['event_id'], h('existing-DAG-run'), now=START + 2 * HOUR + 3 * MINUTE)
        self.assertEqual('candidate_running', resumed['state'])
        reopened.record_candidate(request_id, candidate(), now=START + 2 * HOUR + 4 * MINUTE)
        self.assertEqual('waiting_approval', reopened.ack_dispatch(event['event_id'], h('existing-DAG-run'), now=START + 2 * HOUR + 5 * MINUTE)['state'])
        with self.assertRaisesRegex(PolicyError, 'dispatch_receipt_conflict'):
            reopened.ack_dispatch(event['event_id'], h('different-run'), now=START + 2 * HOUR + 5 * MINUTE)

    def test_bad_candidate_or_unambiguous_failure_keeps_reference(self):
        request_id = self.dispatched()
        result = self.store.record_candidate(request_id, candidate(False), now=START + 2 * HOUR + 3 * MINUTE)
        self.assertEqual('gate_rejected', result['state'])
        self.assertEqual(payload(REFERENCE), self.store.snapshot()['reference'])
        self.assertIsNone(self.store.snapshot()['inflight'])
        self.assertEqual(result, self.store.record_candidate(request_id, candidate(False), now=START + 2 * HOUR + 4 * MINUTE))

    def test_ambiguous_unacked_dispatch_must_recover_delivery_receipt_before_result(self):
        request_id = self.accepted()
        self.store.accept_labels(request_id, approval(), now=START + 2 * HOUR + MINUTE)
        event = self.store.pending_outbox()[0]
        self.store.record_failure(request_id, ambiguous=True, now=START + 2 * HOUR + 2 * MINUTE)
        with self.assertRaisesRegex(PolicyError, 'dispatch_receipt_required'):
            self.store.record_candidate(request_id, candidate(), now=START + 2 * HOUR + 3 * MINUTE)
        self.assertEqual([event], self.store.pending_outbox())
        self.store.ack_dispatch(event['event_id'], h('existing-run-receipt'), now=START + 2 * HOUR + 3 * MINUTE)
        self.assertEqual('waiting_approval', self.store.record_candidate(request_id, candidate(), now=START + 2 * HOUR + 4 * MINUTE)['state'])

    def test_unambiguous_failure_cancels_pending_outbox_and_is_idempotent(self):
        request_id = self.accepted()
        self.store.accept_labels(request_id, approval(), now=START + 2 * HOUR + MINUTE)
        result = self.store.record_failure(request_id, ambiguous=False, now=START + 2 * HOUR + 2 * MINUTE)
        self.assertEqual('terminal_failed', result['state'])
        self.assertEqual(result, self.store.record_failure(request_id, ambiguous=False, now=START + 2 * HOUR + 3 * MINUTE))
        self.assertEqual([], self.store.pending_outbox())
        self.assertEqual(payload(REFERENCE), self.store.snapshot()['reference'])

    def test_cooldown_boundary_is_durable_from_accepted_intent_not_last_retry(self):
        request_id = self.dispatched()
        self.store.record_candidate(request_id, candidate(False), now=START + 2 * HOUR + 3 * MINUTE)
        boundary = START + 26 * HOUR
        result = PolicyStore(self.path).observe(window(22), now=boundary - timedelta(microseconds=1))
        self.assertEqual('cooldown', result['reason'])
        self.assertEqual('first_signal', self.store.observe(window(24), now=boundary)['reason'])
        result = self.store.observe(window(25), now=boundary)
        self.assertEqual('waiting_for_labels', result['state'])
        self.assertNotEqual(request_id, result['request_id'])
        self.assertEqual(2, self.store.snapshot()['requests'])

    def test_passed_candidate_waits_approval_without_reference_update(self):
        request_id = self.waiting_approval()
        self.assertEqual('waiting_approval', self.store.request(request_id)['state'])
        self.assertEqual(payload(REFERENCE), self.store.snapshot()['reference'])
        self.assertEqual(request_id, self.store.snapshot()['inflight'])
        with self.assertRaisesRegex(PolicyError, 'active_not_verified'):
            self.store.begin_reference_update(request_id, {'active': True}, now=START + 2 * HOUR + 4 * MINUTE)
        with self.assertRaisesRegex(PolicyError, 'active_not_verified'):
            active(checks=('metadata',))

    def test_active_target_epoch_dataset_and_image_are_exactly_bound(self):
        request_id = self.waiting_approval()
        good = active()
        bad_refs = (replace(good.reference, cohort=replace(good.reference.cohort, epoch=1)),
                    replace(good.reference, cohort=replace(good.reference.cohort, epoch=3)),
                    replace(good.reference, cohort=replace(good.reference.cohort, model=MODEL)),
                    replace(good.reference, manifest_sha256=h('other-data')))
        for reference in bad_refs:
            with self.assertRaises(PolicyError):
                self.store.begin_reference_update(request_id, replace(good, reference=reference), now=START + 2 * HOUR + 4 * MINUTE)
        with self.assertRaisesRegex(PolicyError, 'active_release_binding_mismatch'):
            self.store.begin_reference_update(request_id, replace(good, image_id='sha256:' + h('other-image')), now=START + 2 * HOUR + 4 * MINUTE)
        self.assertEqual(payload(REFERENCE), self.store.snapshot()['reference'])

    def test_reference_intent_reopen_reconcile_updates_only_verified_active(self):
        request_id = self.waiting_approval()
        verified = active()
        self.store.begin_reference_update(request_id, verified, now=START + 2 * HOUR + 4 * MINUTE)
        self.assertEqual(payload(REFERENCE), self.store.snapshot()['reference'])
        reopened = PolicyStore(self.path)
        event = reopened.pending_outbox()[0]
        self.assertEqual('reference', event['kind'])
        changed = replace(verified, reference=replace(verified.reference, cohort=replace(verified.reference.cohort, epoch=3)))
        with self.assertRaisesRegex(PolicyError, 'active_target_changed'):
            reopened.reconcile_reference(event['event_id'], changed, now=START + 2 * HOUR + 5 * MINUTE)
        verified = replace(verified, journal_sha256=h('later-harmless-stage-entry'))
        result = reopened.reconcile_reference(event['event_id'], verified, now=START + 2 * HOUR + 5 * MINUTE)
        self.assertEqual('active', result['state'])
        self.assertEqual(payload(verified.reference), reopened.snapshot()['reference'])
        self.assertIsNone(reopened.snapshot()['inflight'])
        self.assertEqual([], reopened.pending_outbox())
        self.assertEqual(result, reopened.reconcile_reference(event['event_id'], verified, now=START + 2 * HOUR + 6 * MINUTE))
        self.assertEqual(2, reopened.snapshot()['windows'])

    def test_corrupt_valid_json_storage_fails_closed(self):
        with sqlite3.connect(self.path) as db:
            db.execute("UPDATE metadata SET payload='{}'")
        with self.assertRaisesRegex(PolicyError, 'storage_corrupt'):
            PolicyStore(self.path)


if __name__ == '__main__':
    unittest.main()
