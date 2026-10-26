"""Initialize only orchestration volumes and the existing airflow database."""
import os
from pathlib import Path
import shutil
import subprocess

for name in ("/work", "/state", "/auth", "/opt/airflow/logs"):
    path = Path(name)
    path.mkdir(parents=True, exist_ok=True)
    os.chown(path, 50000, 0)
shutil.copyfile("/bootstrap/passwords.json", "/auth/passwords.json")
os.chmod("/auth/passwords.json", 0o600)
os.chown("/auth/passwords.json", 50000, 0)
os.setgid(0)
os.setuid(50000)
subprocess.run(["airflow", "db", "migrate"], check=True)
