"""One private registry; labels never derive from request identities or text."""

from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest


class HTTPMetrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        labels = ("endpoint", "method", "status")
        self.requests = Counter("ats_http_requests", "Completed HTTP requests", labels, registry=self.registry)
        self.errors = Counter("ats_http_errors", "HTTP responses with status >=400", labels, registry=self.registry)
        self.latency = Histogram("ats_http_request_duration_seconds", "HTTP response latency", labels,
                                 buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10), registry=self.registry)

    def observe(self, path, method, status, seconds):
        endpoint = path if path in ("/rank", "/metadata", "/metrics") else "other"
        method = method if method in ("GET", "POST") else "other"
        code = str(status) if status in (200, 400, 404, 405, 413, 422, 500, 503) else "other"
        labels = (endpoint, method, code)
        self.requests.labels(*labels).inc()
        if status >= 400:
            self.errors.labels(*labels).inc()
        self.latency.labels(*labels).observe(max(0, seconds))

    def render(self):
        return generate_latest(self.registry)


METRICS = HTTPMetrics()
