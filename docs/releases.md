# Local release lifecycle

Run the host controller from the audited repository root with Python 3.11 and
`PYTHONPATH=src:. python -B -m ats_mlops release`. It reuses the T11 gate and T12
package/check validators; it does not fit, tune or read the test partition.

State defaults to `$XDG_STATE_HOME/ats-mlops`, or `~/.local/state/ats-mlops`.
Use `--state-dir` before the subcommand for an isolated exercise. Keep it outside
Git and cloud-synced folders. `--config configs/releases/local.json` initializes
an immutable local configuration. Later commands load that saved configuration.

```sh
PYTHONPATH=src:. python -B -m ats_mlops release stage --package PACKAGE --release-check CHECK
PYTHONPATH=src:. python -B -m ats_mlops release approve RELEASE_ID --actor agent-operator
PYTHONPATH=src:. python -B -m ats_mlops release activate RELEASE_ID
PYTHONPATH=src:. python -B -m ats_mlops release status
PYTHONPATH=src:. python -B -m ats_mlops release rollback --release-id PREVIOUS_ID
PYTHONPATH=src:. python -B -m ats_mlops release reconcile
```

Stage copies and validates an immutable T12 package and its separate passed
release-check report. Both quality and release reports must pass and match the
exact release/model/image/manifest hashes. Approval is a separate immutable
receipt outside the package. The implemented approval authority is A3's
`agent-operator` for `demo_only`; other scopes need a separately authorized
operator policy. No human signature or production approval is implied.

Approve and activate recheck the actual incumbent under a nonblocking OS file
lock, including release ID and generation. A candidate checked against A is
stale after B activates, even when A/B have the same numeric model version.
Rolling back to A does not refresh the stale candidate. Obtain a new T11 gate,
package, release check and approval revision against the current release.

Activation uses one managed `ranking` service, a fixed loopback port, an exact
image ID, readonly package/model mounts, UID 50000, one CPU and 1536 MiB. The
native named events volume survives every switch and rollback. The stable
named bridge also survives switches; a local monitoring container can
join it and scrape `ranking:3000`. The controller never runs Compose `down` or
prunes images, networks or volumes. It refuses resources owned by another
controller state directory. Retained current and two previous successful
releases remain available; the catalog does not automatically delete history.

The fsynced hash-chained journal is authoritative; each entry also verifies a
payload checksum, including the final entry. `state.json` is an atomic projection.
These checks detect corruption; they are not signatures against an operator
who can rewrite the entire controller directory. Unchecked development journal
entries are rejected. Intent precedes Docker effects, readiness precedes alias intent,
and MLflow champion changes only after exact POST `/metadata` and Docker image,
mount, volume and port verification. An old `/rank` replay is not readiness.
On a fixed port, stop/start causes measurable downtime. There is no distributed
atomic transaction or Registry CAS; an unexpected external alias drift blocks
recovery. Use one operator for the managed alias/project.

If startup fails, the previous verified service is restored where possible.
If Registry fails after target readiness, that healthy API remains running with
a pending intent; further release operations block. Restore Registry access and
run `reconcile`. It completes a known ready target or restores the known previous
release, and is idempotent after commit. Never edit `state.json`, the journal,
approval or packages to bypass a block. A corrupt/partial journal tail, unknown
ready API, missing previous artifacts or third-party identity requires explicit
operator investigation. A lagging state cache is rebuilt from the journal.

Run focused tests with `PYTHONPATH=src:. python -B -m unittest tests.unit.test_release`.
The opt-in actual gate is `tests.integration.test_release`, with
`ATS_RUN_RELEASE=1`, `ATS_RELEASE_REQUEST` pointing to an external JSON containing
`config` and `releases.{A,B,C,D}.{package,check,release_id}`, and
`ATS_RELEASE_EVIDENCE` naming a new external directory. A is first-release,
B/C are independently gated against A (the same model version is intentional),
D against B. It uses existing fitted artifacts, real Docker/HTTP/Registry,
isolated alias/port/volume/network, actual process exits at crash windows and
a scoped connection-refused Registry fault. Skip is not passing evidence.
The healthy fixture endpoint is preserved for independent review; clean up
only its owned resources after review, retaining raw receipts and state.

The [T13 receipt](../reports/releases/t13.json) binds actual executable
`5fd94b02fdc5f5e44ccb2fa1c03a9b1150c5fd50`, the T12 generic image and all four
package/check identities. Fourteen focused and 135 canonical unit tests passed
in locked Python 3.11.17; the real lifecycle test passed all 18 checks in 37.128s.
The host stdlib coordinator used Python 3.14.3; ML scoring and serving used
their recorded pinned Python 3.11 environments. No model fitting or registration
was performed for T13.

Actual probes covered rejected/tampered inputs, missing approval, concurrent
process refusal, A/B/C with identical model4, stale C at approve and activate,
an unhealthy B combined with Registry connection refusal, local A restoration,
real child-process exits after target-ready and alias-confirmed, reconcile,
update to model5, explicit rollback, and original-response replay from the same
Linux events volume. Both previous packages remained available. Independent
review verified the live final API, image, mounts, credential isolation, journal
chain/checksums, approval and Registry alias/run binding, and reran both fixed
semantic failure reproducers. The private fixture was then backed up and cleaned
up; its raw journal, event DB and reports remain external.

The 0.1-second HTTP poller measured normal outage upper bounds of 2.20–2.44s;
the injected unhealthy-start/Registry-outage case took at most 23.21s between
surrounding healthy probes, including its deliberate 20s readiness timeout.
OOM=false and restarts=0. These are one local exercise's observations, not a
capacity or availability guarantee. Earlier missing-alias and Docker Desktop
bind-prefix failures were preserved and are listed in the receipt.

The original T13 durable local demo was activated at `http://127.0.0.1:3000`, using the default
state directory, project `ats-serving`, volume `ats-serving-prediction-events`
and bridge `ats-serving-network`. Its original release was `t12-model4-59967b7`, model4,
champion4 and generation1. A3 approval is separate, and champion was updated
after readiness. Activation/status probes used metadata only. A fresh CLI
idempotent activate confirmed the same generation. Check current metadata with:

```sh
curl -s -X POST http://127.0.0.1:3000/metadata -H 'Content-Type: application/json' -d '{}'
```

T15 subsequently activated `t15-metrics-23145b9`, generation2, on the same model4,
champion4, network and events volume. See [monitoring](monitoring.md) for the
current deployment and its separately attributed receipts; the original T13
receipt remains historical evidence.

The final documentation/receipt commit also removes two EOF-only blank lines;
it does not replace the tested executable source identity above.
