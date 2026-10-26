# Retraining: component checkpoint and batch boundary

Implementation is in progress. Policy-only21 tests and the frozen-params189-unit
checkpoint are separate proofs; full T17 acceptance, adapters and fault/runtime
evidence must pass before this task is marked complete. No E5/test metric claim.

`monitoring.policy` owns one native SQLite ledger and immutable outbox. Two closed
adjacent eligible signal windows (200pairs/10distinct vacancies, same exact
reference/model/release/epoch/config) create one request. A duplicate window ID
with changed bytes is rejected. Insufficient/control/gap breaks the streak;
cooldown/inflight windows do not accumulate a future streak. Cooldown24h starts
at accepted request intent. Waiting labels holds the same singleton request.
The late approved dataset resumes it inside cooldown, with the same run ID.

Before labels can queue a candidate, an immutable ExecutionPlan binds published
source Git, actual training image, canonical model/search/smoke/evaluation params
and their hashes. Mode is frozen, C1/max_features2000/ngram[1,1]/seed20261026;
no new Optuna trials or fake study. A conflict cannot replace a bound plan.
Actual clean model13 source a803dad is an earlier frozen-path proof, not a
completed retraining loop.

The host batch is run explicitly as
`PYTHONPATH=src:. python -B -m ats_mlops.monitoring.retraining_batch ACTION --config CONFIG --request INPUT`.
Actions are window/dataset/advance/reference/snapshot. No daemon or new external
service is installed. The audited host config selects source repository/SHA,
existing Airflow container/image/API/private password file, release-state root,
artifact directory, existing generic serving identity and packager image, and
existing Registry transport. Credentials never enter request receipts/logs.

Host verification reads actual T16 reference/window/calibration/export/feedback
inventories and source/lock hashes, recomputes cohort counts and independent
quality joins using the T16 code, then checks T13 journal/package/API metadata,
actual image and Registry under its lock. A `checked_fixture` model13 reference
does not establish ACTIVE: the actual release/model/manifest/check/epoch must
match. Numeric reference data must match the guarded model's training manifest;
approved schema2-v2 reference support is integrated serially after T16 merge.
Validated inputs are copied to an immutable hash-addressed host artifact store.
The only mutable reference cursor is the policy DB; old artifacts remain intact.

The host installs a canonical verified receipt through the fixed
`policy_worker` Docker-exec helper. It validates actual owned Airflow
container/image/service labels and exact worker/policy source hashes. The helper
runs as UID50000 and accesses `/state/retraining/policy.sqlite3` on the existing
native Airflow state volume. Host code never opens that live SQLite/WAL through a
macOS bind mount. No Docker socket, host release-state mount, provider venv,
Registry/S3 credentials or privileged mount is added to the monitoring DAG.

`dags/monitor_and_retrain.py` accepts only installed receipt ID/SHA, then invokes
the same fixed helper. It never accepts raw ACTIVE flags, arbitrary windows,
commands or filesystem paths from DAG conf. It returns only request/state IDs.
The DAG is batch-triggered (`schedule=None`, max_active_runs/tasks1); root must
install the audited **source and DAG** overlay while Airflow SQL has0active runs,
then enable the DAGs. This implementation does not change the running overlay.

`dataset` explicitly approves only the current admitted synthetic teacher source
as demo_only. Both previous/current bundles must be genuine schema2/GX/explicit
reservation-parent carriers containing train/validation and hash-only metadata.
Labelset fingerprints and changed-label count use normalized semantic pair keys
plus labels, ignoring namespace/IDs/order/punctuation. Predictions never become
truth; real human ingestion is not invented. V2 generator seed20261027 is fixed
before metrics; structural admission failure is retained and investigated, not
handled by searching another seed. Publish the approved immutable bundle with
the existing T10 scoped DVC/MinIO procedure before candidate dispatch.

`advance` reads the native outbox and creates/looks up the ordinary
`train_candidate` DAG run using its original deterministic ID and exact typed
conf hash. A lost POST reply triggers lookup of the same ID, never another run.
The ack hash covers DAG/run/conf only; mutable status/timestamps are excluded.
An uncertain post-registration outcome stays recovery_required/inflight. Retry
or recovery must use the same T10 request/run/artifact; no second fit/new ID.

Completed T10 artifacts are exported via a bounded allowlist helper, excluding
auth/config.local/SQLite/source/test/corpus/split bodies. Host T11 compares the
candidate with baseline/current incumbent on the same validation snapshot.
Rejected candidates keep the incumbent. Passed candidates reuse T12 package and
release-check protocols and T13 stage with an atomic expected-generation guard;
no approval/activation is automatic. Staged artifacts and current epoch are
rechecked on retries; same-release ABA is stale even at the same model version.

After a separate authorized A3 activation, `reference` obtains fresh real T13
consistency evidence, persists a reference-update intent, then reconciles the
same verified target/next epoch into the native DB cursor. A crash leaves an
inflight pending intent; reference remains old until verified reconcile. Batch
code never calls approve/activate and never treats cached rank replay as ready.

Runtime acceptance remains separate: source publication/overlay review, genuine
T16 ACTIVE-bound windows, waiting→approvedv2→ordinary candidate→gate→waiting
approval, explicit release/reference update, concurrency/cooldown and crash
deduplication evidence. CLI root registration is integrated only after T16;
standalone modules keep ownership separate during implementation.
