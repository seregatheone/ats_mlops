"""Read the local runtime credentials without embedding them in Compose args."""
import os
from urllib.parse import quote

uri = "postgresql+psycopg2://mlflow:{}@postgres:5432/mlflow".format(
    quote(os.environ["MLFLOW_DB_PASSWORD"], safe="")
)
hosts = ",".join(("mlflow", "mlflow:5000", "localhost", "127.0.0.1",
                  "localhost:5000", "127.0.0.1:5000",
                  f"localhost:{os.environ['MLFLOW_PORT']}", f"127.0.0.1:{os.environ['MLFLOW_PORT']}"))
os.execv("/opt/ml-venv/bin/mlflow", ["mlflow", "server", "--host", "0.0.0.0",
    "--port", "5000", "--workers", "1", "--backend-store-uri", uri,
    "--artifacts-destination", "s3://ats-mlflow", "--allowed-hosts", hosts])
