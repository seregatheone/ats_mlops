"""Copy runtime-only test credentials into an Airflow-owned named volume."""

import json
import os
from pathlib import Path


def main() -> None:
    values = json.loads(Path("/run/bootstrap/passwords.json").read_text())
    if set(values) != {"compat"} or not isinstance(values["compat"], str):
        raise ValueError("invalid compatibility auth fixture")
    target = Path("/auth/passwords.json")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(values))
    target.chmod(0o600)
    os.chown(target, 50000, 0)
    for name in ("/work", "/artifacts", "/artifacts/bento", "/artifacts/compat"):
        path = Path(name)
        path.mkdir(parents=True, exist_ok=True)
        os.chown(path, 50000, 0)


if __name__ == "__main__":
    main()
