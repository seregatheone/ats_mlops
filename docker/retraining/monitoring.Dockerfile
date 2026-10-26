ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
COPY src/ /opt/monitoring/src/
COPY docker/monitoring/run.py /opt/monitoring/run.py
ARG SOURCE_SHA
LABEL org.opencontainers.image.revision=${SOURCE_SHA}
ENV ATS_MONITORING_SOURCE_SHA=${SOURCE_SHA}
ENV DO_NOT_TRACK=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    HOME=/tmp USER=ats-monitoring LOGNAME=ats-monitoring \
    OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
USER 50000:50000
WORKDIR /tmp
ENTRYPOINT ["/opt/monitoring-venv/bin/python", "-I", "-B", "/opt/monitoring/run.py"]
CMD ["--help"]
