# Technical monitoring

T15 adds a separate technical GET `/metrics` route without changing the frozen
business API schema or rank/prediction-event contract. HTTP requests, errors and
latency histograms use only bounded endpoint/method/status labels. Unknown paths
and methods become `other`. No text, query string, request/candidate/vacancy ID,
input hash or exception message becomes a label. A private process registry
avoids SDK/default runtime collectors. A scrape bypasses Bento's business SDK
and prediction event store; a replay counts as HTTP traffic without refitting
or creating another event. Histogram `le` is a fixed bucket label.

The stdlib technical collector reads the live Linux orchestration ledger with
SQLite `mode=ro`, `query_only=ON` and one read transaction. It respects the WAL;
it never uses `immutable=1`, creates tables or calls the orchestration writer.
It exports aggregate request statuses, attempt phase/outcome counts and retries.
These are gauges of durable rows, not an additional pipeline state machine.
Unknown status/outcome strings become `other`.

An idle WAL-mode database can have neither `-wal` nor `-shm`; SQLite then needs
coordination files that a read-only volume cannot create. The writer publishes
an atomic checksummed numeric sidecar after durable transitions and after final
connection close/checkpoint. It compares DB/WAL content fingerprints before and
after its read-only SQL transaction/close and does not publish a moving snapshot.
Bootstrap exports existing idle ledgers once, with no DML/DDL. The collector
falls back to this sidecar only after verifying its checksum, bounded schema and
current DB/WAL fingerprints twice. A source change or invalid sidecar yields
source_up=0 without payload series. `ats_pipeline_from_snapshot` identifies this
path. The Airflow runner must be rebuilt with the transition hooks; failed
monitoring publication never changes a training/request outcome.
`monitoring/Dockerfile.orchestration` can overlay the exact committed `src/`
onto the previously verified Airflow image by digest, with no dependency or
lock changes. Keep its prior digest/tag and all state/auth/work volumes. Update
the runtime image identity sidecar and recreate only the idle Airflow service;
do not claim that an old running image executes the new hooks.
For a durable local overlay, store a private `runtime-override.json` in the
existing orchestration state directory with exactly
`{"services":{"airflow":{"image":"sha256:ACTUAL_IMAGE_ID"}}}` and matching
`image.json.id`. The normal orchestration bootstrap validates and includes that
image-only override on future up/config/status/stop operations. Extra services,
mounts or environment changes are rejected. A normal build is blocked while
the override is active: build the next overlay separately, update identity and
override together, then recreate only idle Airflow. Old image/state receipts
remain historical evidence; no automatic refit/retry is implied by maintenance.

Release metrics derive from the read-only checksummed journal, including its
final entry, rather than trusting the cached JSON alone. They expose active,
pending/blocked, generation, cache-head freshness and bounded transition counts.
A failed read suppresses that source's payload series and sets
`ats_technical_source_up{source="pipeline|release"}` to zero. Old values are not
reused as fresh. Journal parsing is bounded to 64 MiB total and 2 MiB per entry.
Collector state metrics describe the journal/ledger, rather than asserting an
atomic observation of HTTP availability or Registry state.

The collector has only read-only orchestration-state and release-state mounts,
no auth volume, Registry/S3 credentials or Docker socket. Its single mounted
source file has no training/ML imports. Root plus `DAC_READ_SEARCH` allows it to
read a private host state directory; all mounted sources are read-only, other
capabilities are dropped and privilege escalation is disabled. The inference
container gains no collector/state/secret mounts.

Images are pinned by digest in [images.json](../monitoring/images.json):
[Prometheus 3.15.1](https://prometheus.io/download/) and
[Grafana OSS 13.2.4](https://grafana.com/grafana/download), plus the cached pinned
Python stdlib collector image. Prometheus/Grafana use persistent native volumes.
Their limits are 256 MiB / 0.5 CPU for Prometheus and 384 MiB / 0.5 CPU for
Grafana; the collector is 128 MiB / 0.25 CPU. Grafana's initial startup measured
222.7 MiB against a trial 256 MiB limit, so its limit was raised before the gate.
Prometheus configures seven-day / 256 MB retention; WAL/head add storage overhead. Limits do not prove
capacity; check actual memory and OOM/restart state alongside core/Airflow.

After the new metrics image/package has passed T11/T12 and been approved and
activated via [T13](releases.md), start monitoring from the audited checkout:

```sh
python3 monitoring/bootstrap.py config
python3 monitoring/bootstrap.py up
python3 monitoring/bootstrap.py status
python3 monitoring/bootstrap.py restart
python3 monitoring/bootstrap.py stop
```

Prometheus is at `http://127.0.0.1:9090`; Grafana is at
`http://127.0.0.1:3001/d/ats-technical`. Both publish loopback only. Grafana grants
anonymous Viewer access for this local demo. Its operator password is generated
in `~/.local/state/ats-mlops-monitoring/grafana-password` with mode0600 and copied
to a separate Linux auth volume readable only by Grafana's UID. The password is
never a container environment value or committed artifact. No admin login is
needed to view the dashboard. Datasource/panel JSON is provisioned, with stable
UIDs `ats-prometheus` and `ats-technical`.

Prometheus joins the stable `ats-serving-network` and scrapes `ranking:3000`;
it also joins the separate monitoring bridge for `collector:9101`. No
`host.docker.internal` routing or inference container mutation is needed. Leave
the serving bridge/events volume intact during releases and rollbacks. An old
historical image without `/metrics` can still be rolled back under its original
business schema; Prometheus will report that target down until a metrics-capable
release is active again.

The opt-in gate is `tests/integration/test_metrics.py`. Set `ATS_RUN_METRICS=1`,
`ATS_METRICS_REQUEST` to external JSON containing `package`, `image_id` and
`monitoring_state`, and `ATS_METRICS_EVIDENCE` to a new directory. It uses a
separate serving fixture/events volume to verify actual 200/422/503 counter and
latency deltas, scrape privacy, real collector sources and Prometheus queries,
then restarts monitoring and rechecks historical TSDB values and the provisioned
dashboard. Its temporary file-discovery target is outside Git. The working
default demo receives metadata probes only. Skip is not acceptance evidence.
The fixture remains briefly for manual dashboard viewing; clear its discovery
file and remove only its owned resources afterward. Provisioning/persistent
monitoring and the approved default release remain active.
