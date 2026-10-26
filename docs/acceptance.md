# T18 acceptance and frozen experiment protocol

Status: **PASS for eight actual scenarios S1–S8 and requirements R1–R8**.
The [public summary](../reports/acceptance/t18/summary.json) and
[native audit](../reports/acceptance/t18/raw/acceptance-audit.json) record the result.
The complete private freeze has SHA256
`3d081dba7f13ee46366b10417b1a8f7472da64bbe62220bb5c9de090807d6481`;
its spec SHA256 is `74eff657e0974f1877af04645ca017aa2184c43afee58399c7aea7cec9d6049d`.
The public projection contains an explicit safe subset and a hash map of included
and omitted files. It is not the complete replayable freeze: binary models and
private operational inputs remain in the private artifact graph.

The authoritative scope is T18/#19 and the eight final scenarios in
`docs/plans/reproducible-ml-ats/plan.md`. T18's three engineering recomputations are
separate from the later three E1 experimental runs. E2 remains **not_run** until
five real human/automation pairs exist; an AI run cannot be the manual half.
The available corpus is publishable synthetic data (`demo_only`). No independent
real hiring corpus or independent real hiring-quality result is established.
At this freeze, E1–E5 are all **not_run**, original test rows have not been read,
and the account-wide one-use ledger has not been opened.

S1–S5 executed on `01ccf13d123857ea7b0c8c36fef6c29a13935b78`; S6–S8 and
the final executable protocol use `14be8edffb68a2320404e8e1a03f37a740694466`.
The audit checks Git ancestry, exact historical source bytes, a narrow allowed
change set and the original phase/spec/environment/authorization bindings.
Training identity is preserved across later runner and audit fixes. The negative
model producer has its own explicit Git/file identity; it does not replace S1.

Observed qualification results:

- Three fresh S2 fits produced identical prepared features, ranks and scores:
  maximum score difference0 across220 pairs and11 groups.
- Invalid data failed before fitting; the zero-fit degraded model was rejected
  by actual isolated scoring with validation NDCG@10 of0 against baseline/incumbent1.
- Managed-release and fault scenarios exercised approval/gate checks, immutable
  serving, Registry/MinIO failure, failed readiness and crash reconciliation.
  Outage bounds reflect finite sampling; zero downtime is not asserted.
- S6 created one logical request after two shifted windows of11 vacancies/220
  pairs, waited for labels, then performed one fit and activated model8. The
  refreshed child reference uses12 vacancies/240 pairs per full window. Replay
  and cooldown produced no new requests, runs or versions.
- Private qualification used five fits: one S1, three S2 and one S6. Additional
  model versions include zero-fit fixtures; CI uses its own isolated lifecycle.
- [Positive Actions40362504417](https://github.com/seregatheone/ats_mlops/actions/runs/40362504417)
  passed364 unit tests,11 lifecycle checks and12 API checks. The retained
  [negative Actions40260037093](https://github.com/seregatheone/ats_mlops/actions/runs/40260037093)
  verifies rejection of the deliberately broken contract at its historical head.
- Full L5 discovery passed16 tests, including actual eight-scenario validation.
  Independent metadata review verified the733-file private graph and the
  original S6/S7 command, cohort, source and model bindings.

The qualified candidate is the **first required S1 qualification fit**, declared
before fitting or inspecting metrics in `qualification-plan.json`. Its exact
private Registry run/version and model are immediately bound in
`qualified-selection.json`; rejection blocks qualification rather than selecting
another result. Later three T18 recomputations establish reproducibility and do
not replace that candidate. The S1 fit plus those three qualification fits are
separate from formal E1. The private T13 catalog, Airflow and Registry use their
own real identities; a model number from the default Registry is not transplanted.

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
schema validator; absent/unfrozen validators return blocked.
`validate_freeze(path, expected_sha256)` verifies the complete private graph,
including all native adapters and actual evidence. It starts no services and
never reads reserved test row files. Missing reports and skipped checks cannot
qualify a scenario.

## Freeze sequence and one-use test boundary

`evaluation/experiment.py` and `configs/experiment.template.json` define the
shared T18/T19 protocol. T18 validates and freezes the populated private spec.
The public `reports/acceptance/t18/summary.json` identifies that private freeze;
it is not a replacement input for `validate_freeze`.
The freeze references a **before-evidence executable Git SHA**, spec bytes,
qualified model's original training SHA and exact native evidence hashes.
Evidence/docs commits do not reattribute training. No source↔freeze self-hash cycle.

Frozen inputs must include clean candidate/run/artifact and source; authoritative
schema2 dataset/admission/DVC/split/reservation identity; feature state/config;
ML/monitoring locks; exact runtime images/platform; one thread and measured
resource limits. Draft placeholders are blocked, never passing defaults.
The effective monitoring-rule fingerprint is frozen separately from raw config
and calibration artifact hashes. It binds selected Wasserstein threshold/share,
feature columns/constants, provider identity and eligibility minima, excluding
data-dependent statistics. E4 uses the qualified model's exact reference and this
same rule across windows and the subsequent ACTIVE reference refresh.

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
and reads only reservation metadata. No E5 ledger was initialized by T18.
The local guard is an operational control, not a signature against an administrator;
there is no public force/reset path.
The native `evaluate_e5(directory, authorization)` API derives one persistent
ledger from the OS account's canonical home, independent of the study directory.
It accepts no guard path and ignores HOME overrides; ancestor symlinks reject
before warmup. Freeze binds the stable path-rule/ledger identity. T18 did not
create or consume this real ledger.

The exact full-pair inventory digest is separately derived from sorted manifest
`splits.test.pair_ids` metadata, without reading test rows. After durable consume,
the reader produces one snapshot. Its numeric IDs/ground truth are fsynced and
SQL-bound **before** invoking any scoring callback. Dropped, duplicated or
substituted candidate IDs and any label substitution (including bools) fail.
Retained numeric results include this truth binding; recovery verifies its
ledger-bound hash and the retained result hash, then commits without rereading
text or rescoring. Failure after intent without a retained result remains
consumed/unknown. Numeric IDs/GT are private proof; publication is aggregate only.

## Runtime isolation and replay

The actual harness used private core state/config, forced pipeline recomputation,
T11/T12 checks, native release controllers, T17 callbacks/DAGs and genuine Actions
receipts. Every retained attempt is attributed to its actual executable source.
Failed launch-hash, negative-model metadata and host/VM clock attempts were kept.
The final monitoring runner derives windows from the event-store clock and
checks the exact predeclared request inventory before inspecting drift.

All resources have an `ats-acceptance-*` namespace, separate ports, network,
credentials, Registry/artifact buckets and native event volume. No production
alias/deploy, default-service predictions, global Docker prune, foreign resources
or primary `.local/core` reuse. With the 7.75GiB VM, root may stop only its own idle
ATS core in a declared maintenance window after checking active DAGs. Default artifact-only serving may
remain. Do not run three fits or services in parallel; CPU1/1536MiB workers and
measured per-service limits are explicit inputs. Source-only Airflow overlay is
pins the actual source and cached dependencies.

Execution order: isolated startup/admission → three separate qualification
recomputations → API/release and scoped failure controls → T17 monitoring/retrain
→ final Actions binding → claim/defect review → immutable freeze. Preserve failed
attempts and negative outcomes. Cleanup removes only this run's resources.
Root owns final merge and any default release changes.

The private freeze records planned fresh native state/work volumes for formal
E4. Qualification volumes and history remain intact; declaration of the formal
volumes does not execute an experiment or reset a policy.

To revalidate the retained private artifact graph from the pinned executable
checkout, set its full path and the expected digest explicitly:

```sh
export ATS_ACCEPTANCE_FREEZE=/absolute/path/to/private/freeze.json
export ATS_ACCEPTANCE_FREEZE_SHA256=3d081dba7f13ee46366b10417b1a8f7472da64bbe62220bb5c9de090807d6481
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -B -m unittest discover -s tests/e2e -v
```

This verifies retained evidence without rerunning services or reading original
test rows. A new machine must perform its own private qualification and preserve
the resulting complete graph; the public subset alone intentionally fails this
gate. Formal E1–E5 results belong to T19 and must not reuse qualification timings
or label agent activity as human E2 observations.
