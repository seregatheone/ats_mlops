# Batch drift and independent feedback

T16 produces monitoring evidence; it does not train, change aliases or activate a
release. The demonstrated coordinate is clean schema2 model **13**, a checked
private fixture. Its reference is canonical train (720 pairs / 36 vacancies) and
validation (240 / 12). Original test and historical CF/T9 training rows are never
reference inputs. These synthetic results describe engineering behavior, not
hiring quality or E5 independence.

Evidently **0.7.23** runs in a separate Python 3.11 environment. The ML lock is
unchanged. A real import rejected pandas 3.1, so the monitoring lock constrains
`pandas>=2.2,<3` (resolved 2.3.3). The released API is
`Report([DataDriftPreset(...)], include_tests=True).run(current, reference)`;
the resulting Snapshot saves JSON/HTML. Its JSON interface converts NumPy
scalars before strict result validation. See the [released version metadata](https://pypi.org/project/evidently/0.7.23/)
and [official Report API](https://evidentlyai.github.io/evidently/api-reference/main/evidently/core/report.html).
No Evidently server is required; `DO_NOT_TRACK=1` disables batch telemetry.

## Inputs and commands

The ordinary flat CLI remains compatible and imports Evidently only in the
dedicated renderer. Host export, feedback, reference metadata validation and the
future Airflow policy use stdlib. Dataset admission is a separate earlier step;
reference construction verifies the existing real GX receipt and schema2 policy,
then reads train/validation only. It never repeats full admission or fits a model.

```sh
PYTHONPATH=src python3 -B -m ats_mlops monitoring export \
  --database /native/events/predictions.sqlite3 --cutoff 2026-10-26T06:00:00Z \
  --output /private/evidence/export-001
PYTHONPATH=src python3 -B -m ats_mlops monitoring feedback \
  --export /private/evidence/export-001 --input /private/independent-labels.jsonl \
  --ledger /private/feedback.sqlite3 --config configs/monitoring.yaml \
  --output /private/evidence/feedback-001
PYTHONPATH=src python3 -B -m ats_mlops monitoring reference \
  --request /private/reference-request.json --output /private/evidence/reference-001
```

`reference-request.json` has exactly `bundle`, `bundle_sha256`, `package`,
`release_check`, `release_manifest_sha256`, `scope: "engineering_fixture"`.
The bundle is genuinely admitted schema2; package and completed T12 check are
hash-bound. Reference feature extraction uses a fresh pinned training image,
`-I -B`, no network/credentials/source checkout/socket, and a readonly exact model
artifact. It loads the artifact before importing its own helpers. Numeric rows
have exactly the five canonical features plus score. Vocabulary/IDF, model,
feature schema/state, package/check, admission and reservation hashes are saved.

Export must run on Linux against the native DB/WAL, not a Desktop copy. It uses
`mode=ro`, `query_only` and one SQL transaction. The trusted export worker uses
the serving UID and a writable native volume for SQLite SHM locking; DB data is
read only. A read may create empty WAL/SHM files. The actual gate verifies DB and
nonempty WAL bytes remain unchanged. Do not substitute `immutable=1` on a live DB.

New events retain outer schema1 for cached replay compatibility. Their nested
`completeness` has schema2, original candidate count and SHA256 of the canonical
sorted candidate-ID inventory. Writer checks precede inference/commit; all scores
are persisted before top-k. Legacy events are unchanged and explicitly
completeness-unverified; they cannot support label quality or eligible windows.

Feedback JSONL requires exactly `feedback_event_id`, `request_id`, `vacancy_id`,
`candidate_id`, `model_version`, integer binary `relevance`, UTC `event_time`,
approved `label_source`, positive integer `label_revision`. IDs are opaque;
contact/text fields and arbitrary feature maps are rejected. Labels must bind to
the original full prediction snapshot. The native SQLite ledger transactionally
enforces event IDs and cross-source revision uniqueness. Identical reimports are
no-ops and preserve first trusted `admitted_at`; conflicting IDs/revisions reject
the entire batch. Retain the ledger/ledger ID across imports. If publication
fails after durable admission, retry into a new output directory: committed IDs
remain no-ops. Snapshot publication never overwrites a prior result.

Create the separate image with `python3 -B docker/monitoring/build.py --output
/private/build-evidence` from committed clean source. The recipe bounds dependency
installation to CPU1 / 1536MiB and records image/lock identities. The batch image
contains its own reporting source and exact pinned lock; it has no service.
Run its `calibrate` / `report` entrypoint with `--request` and a new `--output`.
Give it numeric reference/event/feedback/config inputs and writable report storage;
no ML environment or Docker socket is needed by the renderer.

Calibration request keys are `reference`, `reference_sha256`, `config`. Window
request keys add `export`, `feedback`, `calibration`, `calibration_sha256`,
`start`, `end`, `cutoff`; timestamps are UTC and `start < end <= cutoff`.
Container requests use paths visible inside that container. Actual command lines
and input/output hashes are retained in the private gate receipt.

## Meaning and failure boundaries

Calibration uses canonical train as reference and validation as control. A seeded
permutation preserves control; an explicit numeric perturbation produces the
shifted fixture. Method, grid and drift-share come from `configs/monitoring.yaml`.
The selected threshold **0.1**, Wasserstein and share **0.5** are frozen by the
calibration hash before later event experiments. Control/permutation share0;
shifted share1. Unchanged constant reference columns are excluded. Changed
constants/all-constant reference, empty/nonfinite data, unexpected/missing results
or errored tests never become a green no-drift result. Failed partial Report
artifacts are retained privately under a distinct `.failed-*` directory.

The window contains one exact model/release cohort and `[start,end)` predictions.
It needs at least 200 pairs and 10 distinct vacancies. Labels are chosen by the
highest revision whose event time **and first admission time** are at/before
cutoff; a late revision cannot rewrite an earlier cutoff. Pair/group coverage is
separate from quality. NDCG@10/Recall@10 use only complete nonempty request groups
with positives. No labels/no eligible groups means `quality: null`, never zero.
No-positive/empty/incomplete groups remain explicit counts. Synthetic fixture
judgments are selected by candidate ID, never by model predictions as truth.

`window.json` binds reference/model/release/feature state, interval/cutoff, counts,
drift tests, threshold/config, feedback snapshot/ledger, numeric files and actual
runtime/source identities. JSON and real Evidently HTML are produced together.
Outcomes are `passed`, `insufficient_data`, `data_quality_failed`; undefined drift
is `detected: null`. Window identity normalizes UTC instants and excludes cutoff,
so later label revisions retain the same window identity with a new receipt hash.

The private checked fixture has no ACTIVE epoch. T17 must independently bind the
receipt to the actual approved T13 ACTIVE release before policy actions. T17 owns
two-window confirmation, cooldown, waiting-for-labels, outbox and retraining.
This batch has no such effects. Refresh reference binding after approved release
changes; legacy A4 remains engineering-only.

## Verification

Executable batch source `d13c70c`, serving source `7235b01`, final fixture runner
`87f43fc`. Actual007 passed eight checks; its T12 package/API and isolated reference
subgates explicitly reuse their hash-verified passing leaves from actual003.
Actual003's overall failed batch is not credited. Subsequent attempts fixed only
Evidently JSON scalar conversion and runner transport, without fitting or changing
feature math. One full canonical unit run passed **179 tests in 55.082s**; an
earlier readonly-DVC tmpdir failure is retained separately.

The gate independently exercised real JSON/HTML control/shift, 340 pairs/13 jobs,
top3 with100 stored scores, frozen old-image replay with produce forbidden,
Linux WAL export, duplicate/conflicting/late/revised feedback and a real window.
Independent acceptance adds native-Linux concurrent/crash feedback probes and
recomputes quality/null-label behavior; it does not repeat the full gate.

See [public receipt](../reports/monitoring/drift.json) and numeric reports under
`reports/monitoring/drift/`. The actual standalone window HTML is gzip-compressed
without changing its bytes: `gzip -dc reports/monitoring/drift/window.html.gz >
/tmp/ats-window.html`. Source/test/contact text and package/model binaries are
excluded from publication. Real failed logs and full commands remain private.
