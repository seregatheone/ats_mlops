# Synthetic formal experiments

Aggregate projection; complete validation requires the private receipts. E2 is not_run; overall study is incomplete.

| Series | Status | Completed | Expected |
|---|---|---:|---:|
| E1 | passed | 3 | 3 |
| E2 | not_run | 0 | 5 |
| E3 | passed | 18 | 18 |
| E4 | passed | 7 | 7 |
| E5 | passed | 1 | 1 |

## Campaign accounting

Only the new primary campaign contributes to the experiment tables below. The previous campaign stopped on an operator import precondition before the intended crash checkpoint; all its evidence is retained separately.

| Fit scope | Count |
|---|---:|
| Primary completed fits / required | 4 / 4 |
| Retained aborted campaign fits | 3 |
| Earlier qualification fits | 5 |
| Recorded private fits so far | 12 |

The aborted campaign also retains 15 completed fault trials and one incomplete precondition failure. These are supplementary and are not merged into the primary repetitions. Isolated CI has its own lifecycle and is excluded from these fit counts.

## E1 reproducibility

| Slot | Pairs | Groups | Maximum score difference | Maximum metric difference |
|---|---:|---:|---:|---:|
| 1 | 220 | 11 | 0 | 0 |
| 2 | 220 | 11 | 0 | 0 |
| 3 | 220 | 11 | 0 | 0 |

## E3 recovery

Finite sampling gives outage bounds, not exact downtime.

| Fault | Repetitions | Mean lower bound, s | Mean upper bound, s | Mean recovery, s |
|---|---:|---:|---:|---:|
| corrupt_data | 3 | 0 | 0.179673 | 7.1228 |
| minio_unavailable | 3 | 0 | 0.112389 | 9.62326 |
| incomplete_artifact | 3 | 0 | 0.106831 | 0.166708 |
| bad_candidate | 3 | 0 | 0.180506 | 4.12258 |
| unready_container | 3 | 63.4013 | 63.6145 | 64.0576 |
| switch_alias_crash | 3 | 4.93548 | 5.35494 | 6.08079 |

## E4 monitoring

| Scenario | New requests | New fits/runs | New versions |
|---|---:|---:|---:|
| control | 0 | 0 | 0 |
| false_signal | 0 | 0 | 0 |
| insufficient | 0 | 0 | 0 |
| shifted_no_labels | 1 | 0 | 0 |
| shifted_labeled | 0 | 1 | 1 |
| replay | 0 | 0 | 0 |
| cooldown | 0 | 0 | 0 |

## E5 final test

Status: passed.

| Metric | Baseline | Frozen model | Paired delta | Percentile 95% interval |
|---|---:|---:|---:|---|
| ndcg_at_10 | 1.0 | 1.0 | 0.0 | [0.0, 0.0] |
| recall_at_10 | 1.0 | 1.0 | 0.0 | [0.0, 0.0] |

Bootstrap: 2000 paired group resamples, seed20261030; minimum10 eligible groups. Latency is one measured pass per group after train/validation-only warmup.

All data are synthetic. These results do not establish real hiring quality.
