"""Launch Bento in isolated Python, with only delivery code on the import path."""

import os
from pathlib import Path
import sys

assert sys.flags.isolated and sys.dont_write_bytecode
os.environ.pop("PYTHONPATH", None)
home = Path(os.environ["BENTOML_HOME"])
home.mkdir(parents=True, exist_ok=True)
# Docker creates nested mount parents as root. Keep the writable home owned by
# the serving UID and mount only the immutable model store at its own root.
(home / "models").symlink_to("/models", target_is_directory=True)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from service.service import RankingService
import uvicorn

# Single-process public Bento ASGI adapter preserves -I/-B in the model worker.
uvicorn.run(RankingService.to_asgi(), host="0.0.0.0", port=3000, access_log=False,
            log_level="critical")
