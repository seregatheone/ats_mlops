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
Their limits are 256 MiB / 0.5 CPU each; the collector is 128 MiB / 0.25 CPU.
Prometheus retains at most seven days / 256 MB of TSDB data. Limits do not prove
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
