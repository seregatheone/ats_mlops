# T17 actual operator exercise

This is an executable recipe for the concrete native adapters in
`monitoring.exercise`. It is not an execution receipt. The controlled-clock
unittest uses fixture transports; only the explicit commands below exercise real
Airflow/native volumes/provider/MLflow/T11/T12/T13. No command approves or
activates automatically. Run from the clean committed/pushed issue18 checkout.

## Source-only overlays, coordinator only

The running cached Airflow53ee read-only config query confirmed
`core.dags_folder=/opt/airflow/dags`. Its actual runtime is Airflow3.3.2 /
Python3.11.16; the ML574 worker is Python3.11.17. Training and numeric provider
runtime identities are recorded separately. No dependency/lock install occurs.

```sh
python3 -B docker/retraining/prepare.py --repository "$PWD" --output /private/tmp/ats-t17-overlay-context
```

The context is an explicit committed Git archive of src, dags, two overlay
Dockerfiles and the monitoring entrypoint. It excludes local state/credentials,
source/test/corpus/split bodies, caches and Git internals. Context inventory and
source SHA are saved in `context.json`. Root supplies the exact frozen SHA as
`SOURCE_SHA`; source changes require another context/image identity.

After root verifies SQL0active runs, backup existing runtime override/image
receipt/private airflow.env identities and sanitized source Git identity. Root
alone may assign owned local base tags, verify their exact IDs, then execute
source-only builds (network none, cached dependencies). Raw sha256 image IDs are
valid run/inspect references but are not used as Dockerfile FROM references:

```sh
docker tag sha256:53ee2a88e991a0e00c0eed85505ce0d53731ccdc8b11b0d90605f0454f362e6f ats-t17-base-airflow:53ee2a88e991
docker tag sha256:7aace0273f8ee4468063e45d54d726ce3daf310f092308824e4043e84463b556 ats-t17-base-monitoring:7aace0273f8e
python3 -B docker/retraining/build.py --context /private/tmp/ats-t17-overlay-context --kind airflow --base-tag ats-t17-base-airflow:53ee2a88e991 --expected-base-id sha256:53ee2a88e991a0e00c0eed85505ce0d53731ccdc8b11b0d90605f0454f362e6f --tag ats-core-airflow:t17-source --execute
python3 -B docker/retraining/build.py --context /private/tmp/ats-t17-overlay-context --kind monitoring --base-tag ats-t17-base-monitoring:7aace0273f8e --expected-base-id sha256:7aace0273f8ee4468063e45d54d726ce3daf310f092308824e4043e84463b556 --tag ats-monitoring:t17-source --execute
```

The build helper rejects raw-ID base arguments, verifies tag→ID and every
context file against the committed Git blob before invoking build; without
`--execute` it checks and prints the command only. The monitoring base is the
existing Pandas2 dependency-only image; the overlay supplies its actual
entrypoint/CMD. Root must retain the old image
receipt, save the new actual image ID/OS/architecture/labels and update the
existing native `/state/image.json` and runtime override coherently before
recreating **only** Airflow. Existing core/foreign services, volumes, credentials
and mounts remain. Refresh the already sanitized Git source to the exact
published commit. The new source and both DAG files must appear in the image;
unpause monitor_and_retrain/train_candidate through the public API only after
root's SDK/serialization/source/image checks. No Docker socket/host state mount.

## Concrete private inputs after images exist

`TRAINING_IMAGE_ID` and `MONITORING_IMAGE_ID` are the actual built image digests,
not tags. The command reads metadata and file existence only; password/dvc.env
contents are never read or copied. It saves private mode0600 input JSON.

```sh
python3 -B docker/retraining/configure.py --repository "$PWD" --evidence /private/tmp/ats-issue-18-actual-20261026 --training-image "$TRAINING_IMAGE_ID" --monitoring-image "$MONITORING_IMAGE_ID" --orchestration-state "$ORCHESTRATION_STATE" --core-state "$CORE_STATE" --baseline-bundle "$BASELINE_BUNDLE" --baseline-reference "$BASELINE_REFERENCE" --airflow-url http://127.0.0.1:8080 --port 3018
```

The default T13 state is `~/.local/state/ats-mlops`; override `--release-state`
explicitly if root selects another state. Default ACTIVE13/epoch3 must match the
reused checked reference exactly during fresh host T13 consistency checks.
The private API/event namespace is `ats-t17-<source-prefix>`, port3018. Native
policy is `/state/retraining/policy.sqlite3`, separate from the T10 ledger.

## Actual phases, each explicitly authorized by root's resource window

Use the generated `exercise.json` for every phase. `recipe` only describes the
steps; the other commands execute real effects. No callbacks are accepted.

```sh
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise recipe --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise preflight --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise cohort --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise replay --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise dataset --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise advance-drop-ack --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise advance --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
```

Preflight checks committed/published source, actual numeric image label/ID,
owned native Airflow/helper/image-file, unpaused real DAGs and fresh T13
ACTIVE/API/image/Registry/journal consistency. Cohort clones the exact current
package onto its own private bridge/event volume, submits two adjacent real
240pair/12vacancy windows (no labels), exports native Linux WAL via readonly SQL,
runs actual calibrated Evidently reports, then installs/dispatches real verified
receipts through monitor_and_retrain. Working events are measured before/after
and must stay0. The private resources are removed in finally; cleanup after a
process crash verifies exact owner labels before removal.

Dataset freezes seed20261027/C1/maxfeatures2000/model seed20261026 before metrics,
admits a genuine schema2 bundle with the incumbent's explicit reservation parent,
retains sanitized admission JSON, publishes only immutable train/val carrier
files with the existing scoped DVC identity, and resumes the same request. A
structural failure retains intent/config/parent hashes and sanitized error code;
do not change/search seed. No predictions become labels; teacher source is
demo_only. No original test body or E5 access.

Repeat the **same** advance command while candidate_running. It performs exact
T10 public run-ID/conf lookup, never clears/refits or invents another ID. It uses
ordinary frozen DVC validation/prepare/train/eval (one new registered model/run,
zero new Optuna trials), actual T11 validation gate, credential-free T12 import
worker, private T12 API/restart/privacy/idempotency checks and guarded T13 stage.
Bad/ambiguous outcome preserves the incumbent; no tuning after rejection.

`replay` sends each exact installed window receipt through an additional real
monitor DAG delivery, checks success and proves native windows/request/inflight
and Registry counts did not change. Its deterministic replay run IDs are test
delivery IDs; the policy receipt/window/request IDs remain original.

`advance-drop-ack` is an explicit real transport fault, expected process exit86.
It uses the normal real urllib opener and Airflow backend. After the actual T10
POST returns2xx and the exact run/conf match, it fsyncs a sanitized acceptance
marker and exits before native ACK. It cannot inject a fake backend or another
run ID. Preserve the marker; start a fresh normal `advance` process. That process
GETs the same public DAG run/conf and the same native pending outbox/request,
then resumes acknowledgment/gates without clear/new run/refit. Exit86 is not a
generic PASS: acceptance must verify the marker, fresh lookup and exactly one
new registered model/run at the terminal state. These fixed T17 fault fixtures
are integration evidence, not formal E4 experiment measurements.

Before any A3, exercise the new artifact-own GX/features and actual provider
calibration and require the same effective rule:

```sh
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise reference-ready --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
```

Review `actual/candidate.json`, staged gate/check/package hashes and
`actual/reference-ready.json`. Only root may separately approve/activate that
named staged release via existing T13 (`ats-mlops release --state-dir STATE
approve RELEASE --actor agent-operator`, then `activate RELEASE`). This is the
separate coordinator-controlled operator step; existing agent-operator
authorization applies and no additional user confirmation is requested.
No activation is implied by waiting_approval.
After root's actual ready/alias/journal check:

```sh
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise reference --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise cooldown --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
```

Fresh target ACTIVE+epoch and actual new reference/config/calibration are checked
again, then native intent/reconcile updates the single policy cursor. Required
schema3 backing files include full sanitized `data-training-guard.json`, approval,
numeric rows and groups; hashes are auditable after workers exit. A changed rule
fails closed and requires a separate protocol revision. Evidence does not claim
ACTIVATION until root A3 has actually completed.

After actual cursor reconciliation, `cooldown` creates two fresh real signal
windows on the new ACTIVE/reference using its own labelled fixture resources
and the same native policy. Real monitor DAG delivery plus exact receipt replay
must report reason=cooldown, inflight=None, requests=1 and unchanged Registry.
It does not advance a production clock, age the ledger, reset state or fit.

Own crash cleanup command (no native policy, default service or foreign cleanup):

```sh
PYTHONPATH=src:. python3 -B -m ats_mlops.monitoring.exercise cleanup --config /private/tmp/ats-issue-18-actual-20261026/exercise.json
```
