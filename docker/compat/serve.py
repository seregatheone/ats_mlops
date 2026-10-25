"""Select the exact imported artifact before starting BentoML."""

import os
from pathlib import Path
import sys


def main() -> None:
    tag_file = Path(os.environ.get("ATS_BENTO_TAG_FILE", "/artifacts/compat/bento_model_tag.txt"))
    tag = tag_file.read_text(encoding="utf-8").strip()
    if ":" not in tag or tag.endswith(":latest") or any(char.isspace() for char in tag):
        raise ValueError("The tag file must contain one exact Bento model tag")
    os.environ["ATS_COMPAT_BENTO_TAG"] = tag
    os.chdir(Path(__file__).resolve().parent)
    os.execv(sys.executable, [sys.executable, "-m", "bentoml", "serve",
                             "service:CompatibilityService", "--host", "0.0.0.0",
                             "--port", "3000"])


if __name__ == "__main__":
    main()
