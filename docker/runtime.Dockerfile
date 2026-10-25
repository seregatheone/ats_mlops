FROM python:3.11.17-slim-bookworm@sha256:f06d01d4ba3f8ad5af45a3ec87df19c1d317d8e8961edc0fabda61811af6f7cf AS ml_runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VIRTUAL_ENV=/opt/ml-venv \
    PATH=/opt/ml-venv/bin:$PATH \
    PYTHONPATH=/opt/project/src

RUN apt-get update \
    && apt-get install --no-install-recommends -y git \
    && rm -rf /var/lib/apt/lists/* \
    && python -m venv /opt/ml-venv
COPY requirements-build.lock /opt/requirements-build.lock
RUN /opt/ml-venv/bin/pip install --no-cache-dir --require-hashes -r /opt/requirements-build.lock
COPY requirements-ml.lock /opt/requirements-ml.lock
RUN /opt/ml-venv/bin/pip install --no-cache-dir --require-hashes --no-build-isolation \
    --only-binary=:all: --no-binary=antlr4-python3-runtime -r /opt/requirements-ml.lock

COPY src/ /opt/project/src/
COPY docker/compat/service.py docker/compat/serve.py /opt/compat/
COPY docker/compat/init_auth.py /opt/compat/init_auth.py
RUN useradd --uid 50000 --create-home --shell /usr/sbin/nologin ats \
    && mkdir -p /artifacts/bento /artifacts/compat /work \
    && chown -R 50000:50000 /artifacts /work
USER 50000:50000
WORKDIR /opt/compat

FROM apache/airflow:3.3.2-python3.11@sha256:a6685a7a708ed6fa910c2c05458d7f2b0db9cca5c3bf58a72ded389767ad3e65 AS airflow_runtime

USER root
COPY --from=ml_runtime /opt/ml-venv /opt/ml-venv
COPY --from=ml_runtime /opt/requirements-ml.lock /opt/requirements-ml.lock
COPY src/ /opt/project/src/
COPY dags/ /opt/airflow/dags/
RUN mkdir -p /work /artifacts /opt/airflow/auth \
    && chown -R 50000:0 /work /artifacts /opt/airflow/auth
ENV PYTHONPATH=/opt/project/src \
    ATS_ML_PYTHON=/opt/ml-venv/bin/python \
    ATS_ML_LOCK_FILE=/opt/requirements-ml.lock
USER airflow

FROM ml_runtime AS core_runtime
COPY docker/core/ /opt/core/
