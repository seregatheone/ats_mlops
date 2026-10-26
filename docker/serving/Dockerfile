ARG BASE_IMAGE=ats-core-ml:t4
FROM ${BASE_IMAGE}
USER root
RUN rm -rf /opt/project/src /opt/compat
COPY service/ /opt/serving/service/
COPY docker/serving/serve.py /opt/serving/serve.py
ARG SOURCE_SHA
LABEL org.opencontainers.image.revision=${SOURCE_SHA}
ENV PYTHONPATH="" BENTOML_DO_NOT_TRACK=1 BENTOML_HOME=/runtime/bento \
    ATS_RELEASE_DIR=/release ATS_EVENT_DB=/events/predictions.sqlite3 \
    OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1
USER 50000:50000
WORKDIR /opt/serving
EXPOSE 3000
ENTRYPOINT ["/opt/ml-venv/bin/python", "-I", "-B", "/opt/serving/serve.py"]
