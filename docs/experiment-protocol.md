# Frozen procedure for E1–E5

Status: preparation; no experimental result is established by this document.
The executable procedure, statistical methods and this file must be committed
before T18 qualification and the later experiment freeze. Exact source, inputs,
candidate, image IDs and resources are supplied by the hash-closed freeze receipt,
not inferred from a later documentation HEAD.
The authoritative design is the experiment section of
[the original plan](plans/reproducible-ml-ats/plan.md), T18/T19 in its
[tasks](plans/reproducible-ml-ats/tasks.md), and the shared
[data/metric contract](data-contract.md). This revision fixes their remaining
parameters; it does not reduce the original number of repetitions.

The data are publishable synthetic skill matching fixtures. There is no
independent real hiring corpus. Engineering acceptance and experiments measure
this implementation; they do not establish hiring usefulness or superiority of
the stack. Negative and failed results are retained.

## Qualification and fixed coordinates

Choose the first S1 required qualification fit before observing metrics, with an
exclusive fsynced declaration. Bind its exact artifact/run/version immediately
after fit. Do not select the best of repeated runs. S1 plus three separate S2
qualification recomputations are four positive qualification fits, not E1.
Controlled bad-model artifact faults are separately attributed and cannot become
the qualified candidate. E5 always uses the selected S1 model, even if S6/E4
later trains or activates another private model.

Use the admitted schema2 v2 fixture, data seed20261027, model seed20261026,
C1/max_features2000, frozen selection, zero HPO trials and one numeric thread.
Source, data/admission/root reservation/split, feature state, model parameters,
ML and monitoring locks, actual image IDs/platform and resource limits are
immutable inputs. Private core/T13/Airflow/Registry identities are genuine;
default Registry model numbers are not copied into a different backend.
For the negative-artifact case, reverse the fitted logistic coefficients and
intercept by a predeclared operation, create zero fits, and log a distinct real
MLflow fault run/version. Preserve parent training identity plus current source,
artifact hashes and a derivation manifest. Validation must reach actual isolated
scoring and fail the performance threshold; lineage corruption is a separate
rejection. This fault never replaces the first S1 candidate or counts as E1.

The source Git and selected model's original source Git are separate fields.
The before-evidence executable source is retained in a clean immutable checkout.
Later reports/docs commits never reattribute training. Source, protocol and
input artifacts point into the final receipt; no artifact hashes itself.

## E1 — independent recomputation

Run exactly three fresh workspaces on the same platform and pinned image. Each
uses audited `pipeline repro --force`, recording the exact underlying
`dvc repro -f --no-run-cache`, all four stage execution events and one new final
training run/version. Cache hits and run-cache restores are not recomputations.
Compare prepared hashes, parameters, vocabulary/IDF/feature state, pair inventory
and ranks; compare scores and metrics with atol1e-6/rtol1e-5. Binary pickle equality
is not required. Report successful attempts/three and preserve every failure.

## E2 — human/manual versus automated lifecycle

Exactly five pairs, with identical bound inputs/model and HPO disabled. Alternate
order: manual first for pairs1/3/5, automated first for pairs2/4. A real human
follows the same CLI procedure; the automated half uses the audited Airflow path.
Record cache state, independent wall-clock time, active human time, action/error
counts and separately setup/warmup. Retain the human action journal, actor and
attestation. Report medians and min/max; do not promise a speedup. One familiar
operator is a limitation. AI execution is never a manual measurement. Procedure
readiness permits T18 freeze, while E2 remains `not_run` until genuine pairs exist.

## E3 — controlled faults

For each of corrupt data, unavailable MinIO, incomplete model artifact, degraded
candidate, unready container and switch/alias crash, perform three attempts with
separate owned resources/journals. Inject only after an observed healthy private
API. Capture monotonic injection/detection/restoration times and repeated real
HTTP samples. Report an outage interval from observations, not invented exact
zero downtime. Require blocked erroneous release and final catalog/API/alias
consistency or explicit recovery-required state. Never clear IDs to refit after
an ambiguous remote commit. T18 single-scenario qualification is not an E3 repeat.

## E4 — predeclared drift and feedback

Seed20261029 is declared independently of integration calibration seed1707.
Freeze Wasserstein threshold0.1/share0.5, minimum200pairs/10vacancies,
two adjacent closed windows, cooldown86400s and one request in flight. Freeze the
effective-rule fingerprint separately from raw config/calibration hashes. The
fingerprint excludes data-dependent statistics and stays equal after ACTIVE
reference refresh. Do not tune thresholds using E4 labels or observed outcomes.

Order: control, false signal (one shifted then one control window), insufficient
sample, shifted/no-labels, same request with approved labels, exact receipt replay,
cooldown. Control preserves full validation texts; shifted fixtures keep vacancy
text and replace candidate text using the fixed seeded Python-token perturbation.
One-group windows establish insufficiency. Preserve all candidate events despite
top_k3. Reports come from actual artifact-bound features/scores and Evidently;
the real T17 policy/DAG consumes verified cards. No labels means quality null and
coverage, never quality zero. Labels are a genuine admitted independent synthetic
carrier, not model predictions. Resume the original request, one new model only,
then genuine private A3/ACTIVE and matched reference refresh. Replays/cooldown
create no duplicate requests/runs/versions. Report the scenario-specific outcome
even if the predeclared perturbation fails to produce the intended signal.

Before E4, predeclare a genuine child of S1's v2 reservation: seed20261027,
62 groups (the 60-group parent plus2), fixed namespace/config/source and exact
parent/root/bundle/approval identities. Preflight must find new semantic TRAIN
labels compared with all parent train/validation labels; renamed IDs or only new
validation labels do not qualify. Retain structural admission failures and resolve
them before freeze, without observing/tuning model quality. Pin the admitted child
inventory before any E4 metrics; this carrier never changes the E5 candidate.

## E5 — one attempted final synthetic holdout evaluation

Use the same selected S1 model and committed Jaccard baseline. Before the attempt,
validate the exact artifacts and warm up only an admitted validation group in a
fresh offline Python -I/-B process without source checkout, credentials or socket.
Wait for READY before consuming the durable semantic-holdout ledger intent.
Only explicit root authorization for this frozen one-use attempt permits the
original test read. No fit, HPO, feature fitting, calibration or warmup uses test.

The ledger key derives from the pinned root policy and protected hashed test
documents; changing candidate/protocol/namespace cannot reset it. The partition,
full pair-ID inventory and GT snapshot are separate immutable bindings. Commit
the intent before reading; fsync and SQL-bind numeric IDs/labels before scorers.
The standalone worker receives one snapshot via stdin, scores each complete
vacancy once per method, freezes a score seal before numeric publication, and
returns hashes. No test text is persisted in the worker input/output directories.

Compute NDCG@10/Recall@10, counts and one-pass per-vacancy baseline/model latency.
Use the same eligible groups for both methods, macro-average by vacancy and
report excluded/no-positive groups and undefined quality explicitly, according
to the shared metric contract.
Bootstrap paired vacancy groups2000 times with seed20261030 only when at least10
eligible groups exist; otherwise intervals are undefined with the reason. Retain
private numeric IDs/GT/results and publish aggregate results only. Numeric recovery
verifies retained seals/SQL hashes; it cannot reread text or rescore. A consumed
intent without trusted complete numeric evidence stays `consumed_unknown`.

The native API has one persistent ledger per OS account, derived from the account
database at `.local/state/ats-mlops/evaluation/holdout.sqlite3` under that account's
canonical home. It accepts no ledger-path override, ignores HOME environment
changes and rejects symlink aliases before warmup/consume. Its path-rule/identity
hash is frozen independently of study/project/model/protocol. The generic guard's
temporary path API exists for sandbox tests, not the native operational entrypoint.
There is no public reset/force. This does not claim protection from the OS owner.
