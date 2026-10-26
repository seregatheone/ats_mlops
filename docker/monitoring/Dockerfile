ARG BASE_IMAGE=python:3.11.17-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf
FROM ${BASE_IMAGE}
ENV DO_NOT_TRACK=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    HOME=/tmp USER=ats-monitoring LOGNAME=ats-monitoring \
    OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
COPY requirements-monitoring.lock /tmp/monitoring.lock
RUN if ! test -f /opt/requirements-monitoring.lock || ! cmp -s /opt/requirements-monitoring.lock /tmp/monitoring.lock; then \
      python -m venv /opt/monitoring-venv && \
      /opt/monitoring-venv/bin/pip install --no-cache-dir --require-hashes --only-binary=:all: -r /tmp/monitoring.lock; \
    fi && mv /tmp/monitoring.lock /opt/requirements-monitoring.lock
COPY src/ /opt/monitoring/src/
COPY docker/monitoring/run.py /opt/monitoring/run.py
ARG SOURCE_SHA
LABEL org.opencontainers.image.revision=${SOURCE_SHA}
ENV ATS_MONITORING_SOURCE_SHA=${SOURCE_SHA}
USER 50000:50000
WORKDIR /tmp
ENTRYPOINT ["/opt/monitoring-venv/bin/python", "-I", "-B", "/opt/monitoring/run.py"]
CMD ["--help"]
