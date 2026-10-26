ARG BASE_IMAGE
FROM ${BASE_IMAGE}
USER root
COPY src/ /opt/project/src/
COPY dags/ /opt/airflow/dags/
ARG SOURCE_SHA
LABEL org.opencontainers.image.revision=${SOURCE_SHA}
ENV ATS_RETRAINING_SOURCE_SHA=${SOURCE_SHA}
USER airflow
