# T18 acceptance and experiment freeze — preparation

Status: **preparation / blocked**, not acceptance PASS. T17 implementation and
independent acceptance are still pending. No E1–E5 measurement or original test
read is authorized by preparing this document. This phase executes only pure
contract/sandbox unit tests. A missing report, skip, mocked service or a file named
`passed.json` cannot qualify a scenario.

The authoritative scope is T18/#19 and the eight final scenarios in
`docs/plans/reproducible-ml-ats/plan.md`. T18's three engineering recomputations are
separate from the later three E1 experimental runs. E2 remains **not_run** until
five real human/automation pairs exist; an AI run cannot be the manual half.
The available corpus is publishable synthetic data (`demo_only`). No independent
real hiring corpus or independent real hiring-quality result is established.

## Evidence matrix

| Scenario | Required actual evidence and semantic checks | R requirements |
|---|---|---|
| S1 clean startup | Fresh clone/private state, unique isolated core; real admitted input → DVC/Airflow candidate → T11 gate → checked package → approval/activation. Native catalog, Registry alias and API metadata agree on exact release/model/artifact. Startup logs/resource identities retained. | R1,R3,R4,R5 |
| S2 reproducibility | Three distinct fresh workspaces, exact `dvc repro -f --no-run-cache`, all four actual stage events, three new runs/versions. Same prepared hashes/vocabulary/IDF/features/params/platform/image; same IDs/ranks; scores/metrics `atol=1e-6,rtol=1e-5`. Cache restore is separate. | R1,R3 |
| S3 bad data/model | Corrupt-schema actual validation failure before real estimator fit (fit trace0, no new training run); genuinely degraded candidate rejected by T11 on validation; active release unchanged. | R2,R5 |
| S4 managed release | Missing gate, incomplete artifact and missing approval reject activation. Actual candidate-alias movement after packaging changes neither immutable package nor exact served model. No generic blanket approval. | R4,R5 |
| S5 failures/recovery | Own fixture only: unready container, switch/alias crash, Registry outage and MinIO I/O fault. Preserve failure and recovery journals, no false success; recovered API/catalog/alias agree or next release stays blocked with explicit reason. | R4,R5 |
| S6 monitor/retrain | Actual frozen T17 policy/DAG: eligible two-window signal → one durable request; absent labels → waiting; approved schema2 labeled child → ordinary validation/fit/gates/release; replay/cooldown create no duplicate. ACTIVE reference binding required. | R3,R5,R6 |
| S7 genuine CI | Exact restored/final source successful Actions lifecycle + actual deliberate negative contract run. Verify upstream run/head/job metadata and downloaded artifact hashes; local tests are not Actions evidence. Synthetic isolated credentials/services only. | R7 |
| S8 claim traceability | A claim register links every numerical statement to native raw measurements, versions/config, repeats, units and limitations. Freeze the complete E1–E5 executable protocol; unrun experiments and absent human observations stay not_run. | R8 |

Every receipt binds scenario, source Git SHA, spec SHA, UTC interval, exact
argv/exit status, hashed stdout/stderr and native evidence references. A receipt's
`outcome` or boolean checks alone are insufficient. `tests/e2e/audit.py` verifies
the closed artifact graph and delegates each scenario to an explicit native
schema validator; absent/unfrozen validators return blocked. The early
`validate_freeze(path, expected_sha256)` API deliberately refuses final freeze
until serial dependencies, all native adapters and actual evidence are qualified.
It starts no services and never reads reserved test row files.

## Freeze sequence and one-use test boundary

T4 owns `evaluation/experiment.py` and `configs/experiment.template.json` in the
T19 preparation lane. Root will transfer its frozen prep files serially into T18
before final qualification; there is one protocol algorithm. T18 consumes
`validate_spec` and writes a later immutable `reports/acceptance/freeze.json`.
The freeze references a **before-evidence executable Git SHA**, spec bytes,
qualified model's original training SHA and exact native evidence hashes.
Evidence/docs commits do not reattribute training. No source↔freeze self-hash cycle.

Frozen inputs must include clean candidate/run/artifact and source; authoritative
schema2 dataset/admission/DVC/split/reservation identity; feature state/config;
ML/monitoring locks; exact runtime images/platform; one thread and measured
resource limits. Draft placeholders are blocked, never passing defaults.

Declared policies before E1–E5:

- Data v2 seed20261027; model seed20261026; C1/max_features2000, frozen selection,
  zero Optuna trials; threads1.
- Drift Wasserstein0.1/share0.5, minimum200pairs/10vacancies, two consecutive
  windows, cooldown86400s and one request in flight. Seed1707 calibration is an
  integration fixture; E4 uses separately declared seed20261029.
- E1 exactly3 fresh actual fits; E2 exactly5 human/auto pairs; E3 six declared
  faults at least3 repetitions each. E4 keeps control, false/insufficient,
  no-label, labeled, replay and cooldown scenarios.
- E5 same frozen candidate/baseline before any test read; one attempted test
  evaluation, group bootstrap2000/seed20261030 only with at least10 eligible
  groups. No HPO, preprocessing fit, warmup or calibration on test.

The one-use ledger key is SHA256 of canonical JSON
`{schema_version:1,root_policy_sha256,protected_test_documents_sha256}`.
The protected digest is the pinned root policy's `fold=test` document entries in
their fingerprint-sorted order; entries contain hashed tokens, not source text.
Canonical JSON uses sorted keys, compact separators and a trailing newline.
Candidate/protocol/namespace changes do not reset it. Exact test-partition SHA is
a separate immutable input binding: row-ID serialization cannot create a new
semantic holdout. `derive_holdout_lineage` verifies the anchored parent chain
and reads only reservation metadata. No E5 ledger is initialized in preparation.
The local guard is an operational control, not a signature against an administrator;
there is no public force/reset path.

The exact full-pair inventory digest is separately derived from sorted manifest
`splits.test.pair_ids` metadata, without reading test rows. After durable consume,
the reader produces one snapshot. Its numeric IDs/ground truth are fsynced and
SQL-bound **before** invoking any scoring callback. Dropped, duplicated or
substituted candidate IDs and any label substitution (including bools) fail.
Retained numeric results include this truth binding; recovery verifies its
ledger-bound hash and the retained result hash, then commits without rereading
text or rescoring. Failure after intent without a retained result remains
consumed/unknown. Numeric IDs/GT are private proof; publication is aggregate only.

## Later runtime window — not an instruction to execute now

Only after T17 actual acceptance, zero open material defects and executable source
commit may root authorize a resource window. The future harness uses existing
audited scripts: `bootstrap_core.py` for unique private state/config, pipeline
init/repro, T11/T12 checks, `test_release.py`, frozen T17 callbacks/DAG and genuine
Actions receipts. Exact commands/transport adapters remain pending T17 freeze;
current historical receipts cannot stand in for the eight new actual scenarios.

All resources have an `ats-acceptance-*` namespace, separate ports, network,
credentials, Registry/artifact buckets and native event volume. No production
alias/deploy, default-service predictions, global Docker prune, foreign resources
or primary `.local/core` reuse. With the 7.75GiB VM, root may stop only its own idle
ATS core in a declared maintenance window after checking active DAGs; no implicit
shared-stop command exists in this preparation. Default artifact-only serving may
remain. Do not run three fits or services in parallel; CPU1/1536MiB workers and
measured per-service limits are explicit inputs. Source-only Airflow overlay is
provided after T17 freeze; do not rebuild full Airflow here.

Later execution order: isolated startup/admission → three separate qualification
recomputations → API/release and scoped failure controls → T17 monitoring/retrain
→ final Actions binding → claim/defect review → immutable freeze. Preserve failed
attempts and negative outcomes. Cleanup removes only this run's resources.
Root owns final merge and any default release changes.

Preparation check: `PYTHONPATH=src python3 -B -m unittest
tests.e2e.test_audit_contract`. This checks the auditor, not the eight actual
scenarios. The final `python -m unittest discover -s tests/e2e` gate will require
actual receipt inputs and cannot report acceptance PASS from skips or placeholders.
