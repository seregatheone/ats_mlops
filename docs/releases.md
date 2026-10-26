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

The fsynced hash-chained journal is authoritative; `state.json` is an atomic
projection. Intent precedes Docker effects, readiness precedes alias intent,
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
