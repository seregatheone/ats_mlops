"""UNIT-ONLY aggregate fixtures; no experiment, scorer, model or ledger runs."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest


REPORT_PATH = Path(__file__).resolve().parents[2] / 'scripts/rebuild_experiment_report.py'
SPEC = importlib.util.spec_from_file_location('experiment_report', REPORT_PATH)
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


def statistics(value, n=3):
    return {'n': n, 'min': value, 'mean': value, 'median': value, 'max': value}


def unit_only_aggregate():
    """Complete scalar schema fixture, not a retained or publishable report."""
    replicas = [
        {'slot': slot, 'final_fit_events': 1, 'forced': True, 'run_cache': False,
         'cache_hit': False, 'pairs': 200, 'groups': 10, 'max_score_abs_diff': 0,
         'max_metric_abs_diff': 0, 'within_tolerance': True}
        for slot in (1, 2, 3)
    ]
    faults = [
        {'fault': fault, 'completed': 3, 'expected': 3,
         'outage_lower_seconds': statistics(.1), 'outage_upper_seconds': statistics(.2),
         'detection_seconds': statistics(.2), 'restoration_seconds': statistics(.3)}
        for fault in REPORT.FAULTS
    ]
    cases = []
    for scenario in REPORT.CASES:
        windows = 0 if scenario in ('shifted_labeled', 'replay') else 2
        pairs, vacancies = (20, 1) if scenario == 'insufficient' else (200, 10)
        cases.append({'scenario': scenario, 'new_requests': int(scenario == 'shifted_no_labels'),
            'new_runs': int(scenario == 'shifted_labeled'), 'new_versions': int(scenario == 'shifted_labeled'),
            'windows': windows, 'minimum_window_pairs': pairs if windows else None,
            'maximum_window_pairs': pairs if windows else None,
            'minimum_window_vacancies': vacancies if windows else None,
            'maximum_window_vacancies': vacancies if windows else None})
    return {
        'schema_version': 1, 'scope': 'synthetic_formal_experiments', 'evidence_scope': 'demo_only',
        'source_git_sha': 'a' * 40, 'qualified_model_source_git_sha': 'b' * 40,
        'private_freeze_sha256': 'c' * 64, 'spec_sha256': 'd' * 64,
        'private_backing_required': True, 'public_projection_is_replayable': False,
        'overall_status': 'incomplete', 'source_receipts_sha256': {'UNIT-ONLY.fixture': 'e' * 64},
        'campaign_accounting': {'primary_completed_fits': 4, 'primary_required_fits': 4,
            'retained_aborted_campaigns': 1, 'retained_completed_fits': 3,
            'retained_successful_fault_trials': 15, 'retained_incomplete_precondition_trials': 1,
            'qualification_fits': 5, 'total_recorded_private_fits': 12, 'isolated_CI_excluded': True},
        'series': {
            'E1': {'status': 'passed', 'expected': 3, 'completed': 3, 'replicas': replicas,
                   'new_runs': 3, 'new_versions': 3},
            'E2': {'status': 'not_run', 'expected_pairs': 5, 'completed_pairs': 0},
            'E3': {'status': 'passed', 'expected': 18, 'completed': 18, 'faults': faults},
            'E4': {'status': 'passed', 'expected': 7, 'completed': 7, 'scenarios': cases,
                   'resumed_same_request': True},
            'E5': {'status': 'passed', 'expected': 1, 'completed': 1, 'eligible_groups': 10,
                'inventory_pairs': 200, 'inventory_groups': 10,
                'metrics': {name: {'status': 'defined', 'ndcg_at_10': value, 'recall_at_10': value,
                    'eligible_group_count': 10, 'group_count': 10}
                    for name, value in (('baseline', .25), ('model', .5))},
                'paired_group_bootstrap': {metric: {'delta': .25, 'percentile_95': [-.1, .5], 'reason': None}
                    for metric in ('ndcg_at_10', 'recall_at_10')},
                'latency_seconds': {name: statistics(.01, 10) | {'p95': .01}
                    for name in ('baseline', 'model')},
                'bootstrap_policy': {'bootstrap_min_groups': 10, 'bootstrap_replicates': 2000,
                    'bootstrap_seed': 20261030, 'k': 10, 'latency': 'one_pass_per_group',
                    'warmup': 'train_validation_only'}},
        },
    }


class ExperimentReportSemanticRegressionTests(unittest.TestCase):
    def setUp(self):
        self.aggregate = unit_only_aggregate()
        REPORT.validate(self.aggregate)

    def test_reported_tolerance_success_cannot_hide_large_E1_error(self):
        # The original validator trusted within_tolerance=True without the scalar check.
        for field in ('max_score_abs_diff', 'max_metric_abs_diff'):
            with self.subTest(field=field):
                value = deepcopy(self.aggregate)
                value['series']['E1']['replicas'][0][field] = 1.0
                with self.assertRaisesRegex(ValueError, 'recompute_diff_outside_frozen_tolerance'):
                    REPORT.validate(value)

    def test_locally_valid_E3_statistics_cannot_reverse_outage_bounds(self):
        # Both statistics objects remain valid; their lower/upper relationship was missing.
        value = deepcopy(self.aggregate)
        value['series']['E3']['faults'][0]['outage_lower_seconds'] = statistics(.9)
        with self.assertRaisesRegex(ValueError, 'reversed_aggregate_outage_bounds'):
            REPORT.validate(value)

    def test_E5_delta_matches_its_displayed_baseline_and_model_metrics(self):
        # The prior report accepted delta=.99 while displaying .50-.25.
        for metric in ('ndcg_at_10', 'recall_at_10'):
            with self.subTest(metric=metric):
                value = deepcopy(self.aggregate)
                value['series']['E5']['paired_group_bootstrap'][metric]['delta'] = .99
                with self.assertRaisesRegex(ValueError, 'paired_delta_metric_mismatch'):
                    REPORT.validate(value)

    def test_E5_difference_interval_stays_inside_unit_metric_difference_range(self):
        # Finite sorted endpoints alone previously admitted impossible intervals.
        for interval in ([-2, .5], [-.1, 3], [-2, 3]):
            with self.subTest(interval=interval):
                value = deepcopy(self.aggregate)
                value['series']['E5']['paired_group_bootstrap']['ndcg_at_10']['percentile_95'] = interval
                with self.assertRaisesRegex(ValueError, 'bootstrap_difference_outside_range'):
                    REPORT.validate(value)


if __name__ == '__main__':
    unittest.main()
