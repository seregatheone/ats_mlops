#!/bin/sh
set -eu
export MC_CONFIG_DIR=/tmp/mc
mc alias set core http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc mb --ignore-existing core/ats-dvc >/dev/null
mc mb --ignore-existing core/ats-mlflow >/dev/null
mc admin policy create core ats-dvc /opt/core/policies/dvc.json >/dev/null
mc admin policy create core ats-mlflow /opt/core/policies/mlflow.json >/dev/null
mc admin user add core "$DVC_ACCESS_KEY_ID" "$DVC_SECRET_ACCESS_KEY" >/dev/null
mc admin user add core "$MLFLOW_ACCESS_KEY_ID" "$MLFLOW_SECRET_ACCESS_KEY" >/dev/null
mc admin policy attach core ats-dvc --user "$DVC_ACCESS_KEY_ID" >/dev/null
mc admin policy attach core ats-mlflow --user "$MLFLOW_ACCESS_KEY_ID" >/dev/null
printf 'Core buckets and scoped identities initialized.\n'
