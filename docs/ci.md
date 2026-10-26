# Isolated CI

`isolated-ci` runs on PRs, pushes to main and manual dispatch. The public
repository uses GitHub's [standard hosted ARM runner](https://docs.github.com/en/actions/reference/runners/github-hosted-runners), `ubuntu-24.04-arm`. Checkout and upload
actions are pinned to upstream commit IDs; checkout retains no credentials.
Only `contents: read` is granted. There are no repository secrets, deployments
or image pushes. Each lifecycle gets its own random Compose project, freshly
generated private credentials and Linux named volumes.

Run the same entry points locally from a clean committed checkout:

```sh
python3 -B docker/ci/static.py
python3 -B docker/ci/run.py --directory /tmp/ats-ci-UNIQUE --public /tmp/ats-ci-public-UNIQUE
```

The directory must be new and outside the source tree. Reserve memory before
running this beside other local stacks. Builds use the existing digest-pinned
Dockerfiles and hash-locked ML environment. The runner records its actual
platform, CPU/memory resources and resolved image IDs. Additional amd64
compatibility is not claimed. Local results do not replace actual Actions runs.

The static job parses Python source without writing bytecode and compares the
complete consumed `tests/fixtures/ci-contract.json` with the implemented HTTP
schema using canonical JSON, including JSON types. Empty or partial fixtures fail.
The lifecycle builds ML/core images sequentially, runs the complete unit suite
inside the pinned ML runtime, and starts disposable PostgreSQL/MinIO/MLflow.
`docker compose -f docker/ci/compose.yaml config --quiet` is executed using only
the invocation's generated private env file and explicit source/image selectors.

A small synthetic bundle is generated and admitted once with GX. Train and
validation objects are pushed to the scoped temporary DVC bucket, deleted along
with their local cache, then pulled and checked byte-for-byte. Source/test/corpus/
split bytes are absent from every regular pipeline stage. The immutable source
clone stays clean while the sibling DVC workspace changes. Smoke mode runs two
Optuna trials with the main configured budget still 20, followed by a separate
final fit and exact numeric MLflow version. An unchanged audited repro must be a
cache hit. An alias-only second Registry version reuses the same fitted run;
it is not reported as another training run.

The existing validation-only baseline, offline quality gate, Bento packaging
and full actual HTTP/SQLite release-check implementations are reused. Serving
gets artifact-only readonly mounts and no Registry credentials, training source
or Docker socket. Its fixture alias transport explicitly selects the CI network,
while the tested serving container uses a separate bridge. The release controller
then stages, approves and activates this demo package in a separate temporary
state/port/alias/volume, and confirms idempotent activation. This exercises T13's
positive path; its full crash/rollback matrix and Airflow LocalExecutor are
explicitly `not_run` here. The durable local demo is untouched.

Offline scoring runs as the host input-directory owner, preserving its private
0700 mode. An actual Linux named-volume probe checks rejection of another UID
and successful reading by the owner. Delivery packaging separately normalizes
only the release artifact to directories 0755/files 0444 before freezing; its
private staging parent stays 0700 and byte hashes stay unchanged. A fresh
networkless readonly UID50000 container hashes the complete delivered package
without loading a model, before the actual API checks.

The coordinator uses `finally` cleanup; an Actions `always()` step repeats scoped
cleanup after cancellation/failure. Only the invocation's exact project and
owned release resources are removed, never global Docker prune. Private raw
diagnostics remain in the temporary run directory. The sole uploaded artifact
is `ci.json`: outcomes, counts, SHA/image/platform identities and safe exception
types/frame names. It contains no credentials, raw text, HTTP payloads, SQLite,
model files or raw logs. Successful and failed receipts use the same allowlist.

The acceptance record must link the actual failing contract-fixture commit/run,
its ordinary restoration commit and passing run, plus the passing final head.
Those remote results are pending until recorded in the issue receipt; a skipped
or locally simulated workflow is never credited as Actions evidence.
