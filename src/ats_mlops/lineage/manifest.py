"""Canonical input/source/environment and model artifact identities."""

from importlib.metadata import version
from pathlib import Path
import platform
import re
import subprocess

from ats_mlops.data.dataset import canonical_json, sha256

RUNTIME_PACKAGES = ("mlflow", "scikit-learn", "cloudpickle", "numpy", "pandas",
                    "scipy", "joblib", "threadpoolctl")


def file_hash(path: Path) -> str:
    return sha256(path.read_bytes())


def tree_inventory(directory: Path) -> dict[str, str]:
    return {path.relative_to(directory).as_posix(): file_hash(path)
            for path in sorted(directory.rglob("*"))
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"}


def inventory_hash(inventory: dict) -> str:
    return sha256(canonical_json(inventory))


def write_json(path: Path, document: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json(document))


def source_environment(source_root: Path, image_identity: dict, *, runtime_lock: Path) -> dict:
    """Require a real clean source revision and the same pinned runtime lock."""
    root = source_root.resolve()

    def git(*args):
        return subprocess.run(["git", "-c", f"safe.directory={root}", *args], cwd=root,
                              check=True, capture_output=True, text=True).stdout.strip()

    git_sha = git("rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", git_sha) or git("status", "--porcelain", "--untracked-files=all"):
        raise ValueError("clean_source_commit_required")
    import ats_mlops.features.pairs as features
    import ats_mlops.model.training as training
    for module in (features, training):
        if not Path(module.__file__).resolve().is_relative_to(root / "src"):
            raise ValueError("runtime_source_checkout_mismatch")
    lock_hash = file_hash(root / "requirements-ml.lock")
    if lock_hash != file_hash(runtime_lock):
        raise ValueError("runtime_lock_mismatch")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", image_identity.get("id", "")):
        raise ValueError("resolved_image_identity_required")
    packages = {name: version(name) for name in RUNTIME_PACKAGES}
    source_hashes = tree_inventory(root / "src" / "ats_mlops")
    return {
        "git_sha": git_sha, "dirty_worktree": False, "source_sha256": source_hashes,
        "source_tree_sha256": inventory_hash(source_hashes),
        "dvc_pointers": {
            name: {"pointer_sha256": file_hash(root / "data" / name),
                   "pointer_text": (root / "data" / name).read_text()}
            for name in ("train.jsonl.dvc", "validation.jsonl.dvc", "split.json.dvc")
        },
        "environment": {"python": platform.python_version(), "system": platform.system(),
                        "machine": platform.machine(), "platform": platform.platform(),
                        "packages": packages, "lockfile_sha256": lock_hash,
                        "build_lockfile_sha256": file_hash(root / "requirements-build.lock"),
                        "image": image_identity},
    }
