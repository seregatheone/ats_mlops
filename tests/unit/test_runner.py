"""Exercise the environment gate with real Git revisions and lockfile bytes."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops import runner


class CompatibilityRunnerTests(unittest.TestCase):
    def test_image_lock_mismatch_blocks_worker_without_expected_override(self):
        with tempfile.TemporaryDirectory(prefix="ats-runner-test-") as directory:
            root = Path(directory)
            source = root / "source"
            source.mkdir()
            # Ignore local machine configuration and hooks. Identity is scoped
            # to this fixture commit and never changes the user's Git config.
            git_env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull,
                       "GIT_CONFIG_NOSYSTEM": "1"}

            def git(*arguments):
                return subprocess.run(
                    ["git", *arguments], cwd=source, env=git_env, check=True,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                ).stdout.strip()

            git("-c", "init.defaultBranch=main", "init", "--quiet")
            (source / "requirements-ml.lock").write_text(
                "# source environment\nmlflow==3.17.2\n", encoding="utf-8")
            git("add", "requirements-ml.lock")
            git("-c", "user.name=ATS runner fixture", "-c",
                "user.email=runner-fixture@example.invalid", "commit", "--quiet",
                "-m", "Create compatibility lock fixture")
            git_sha = git("rev-parse", "HEAD")
            image_lock = root / "image-requirements-ml.lock"
            image_lock.write_text(
                "# different image environment\nmlflow==3.17.1\n", encoding="utf-8")
            work_root = root / "work"
            run_id = "image-lock-mismatch"
            environment = {
                "ATS_SOURCE_REPO": str(source), "ATS_WORK_ROOT": str(work_root),
                "ATS_ML_LOCK_FILE": str(image_lock),
                # This path must not be executed when the environment differs.
                "ATS_ML_PYTHON": str(root / "unused-ml-venv" / "bin" / "python"),
                "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1",
            }
            with patch.dict(os.environ, environment), patch.object(
                    runner, "_command", wraps=runner._command) as command_spy:
                with self.assertRaises(runner.CompatibilityError) as raised:
                    # Deliberately omit expected_lock_sha256: the image's real
                    # lockfile, not a caller-provided override, must gate work.
                    runner.run_compatibility(git_sha, run_id)

            self.assertEqual(raised.exception.kind, "lock_mismatch")
            report_dir = work_root / "reports" / run_id
            self.assertEqual(
                json.loads((report_dir / "error.json").read_text()),
                {"kind": "lock_mismatch", "run_id": run_id},
            )
            self.assertFalse((report_dir / "report.json").exists())
            checkout_lock = work_root / run_id / "checkout" / "requirements-ml.lock"
            self.assertEqual(checkout_lock.read_bytes(),
                             (source / "requirements-ml.lock").read_bytes())
            self.assertGreaterEqual(command_spy.call_count, 2)
            self.assertFalse(any(
                "compatibility-worker" in call.args[0]
                for call in command_spy.call_args_list
            ), "A different image lock must block the worker before invocation")


if __name__ == "__main__":
    unittest.main()
