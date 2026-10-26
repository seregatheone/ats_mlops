"""Initialize only orchestration volumes and the existing airflow database."""
import os
from pathlib import Path
import shutil
import subprocess

for name in ("/work", "/state", "/auth", "/opt/airflow/logs"):
    path = Path(name)
    path.mkdir(parents=True, exist_ok=True)
    os.chown(path, 50000, 0)
for source, target in (("passwords.json", "/auth/passwords.json"), ("image.json", "/state/image.json"), ("faults.json", "/state/faults.json")):
    shutil.copyfile("/bootstrap/" + source, target)
    os.chmod(target, 0o600)
    os.chown(target, 50000, 0)
os.setgid(0)
os.setuid(50000)
subprocess.run(["airflow", "db", "migrate"], check=True)
