#!/usr/bin/env bash
set -euo pipefail

psql --set ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set mlflow_password="$MLFLOW_DB_PASSWORD" \
  --set airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE USER mlflow WITH PASSWORD :'mlflow_password';
CREATE DATABASE mlflow OWNER mlflow;
CREATE USER airflow WITH PASSWORD :'airflow_password';
CREATE DATABASE airflow OWNER airflow;
SQL
