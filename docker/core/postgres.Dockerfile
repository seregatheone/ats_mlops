FROM postgres:18.3-alpine@sha256:54451ecb8ab38c24c3ec123f2fd501303a3a1856a5c66e98cecf2460d5e1e9d7
COPY --chmod=0644 docker/core/postgres-init.sh /docker-entrypoint-initdb.d/10-databases.sh
