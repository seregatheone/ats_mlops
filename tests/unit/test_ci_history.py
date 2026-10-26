"""Detached Actions checkout -> private CI clone retains remote-only history."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


PROJECT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ci_history_runner", PROJECT / "docker/ci/run.py")
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class NestedCheckoutHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = os.environ | {
            "GIT_AUTHOR_NAME": "sandbox", "GIT_AUTHOR_EMAIL": "sandbox@example.invalid",
            "GIT_COMMITTER_NAME": "sandbox", "GIT_COMMITTER_EMAIL": "sandbox@example.invalid",
        }

    def git(self, root, *args, check=True):
        return subprocess.run(["git", "-C", str(root), *args], env=self.env, check=check,
                              capture_output=True, text=True)

    def outer(self):
        source = self.root / "outer"
        source.mkdir()
        self.git(source, "init", "-q")
        (source / "snapshot.txt").write_text("historical exact bytes\n")
        self.git(source, "add", ".")
        self.git(source, "commit", "-q", "-m", "historical source")
        old = self.git(source, "rev-parse", "HEAD").stdout.strip()
        self.git(source, "checkout", "--orphan", "squashed-current")
        (source / "snapshot.txt").write_text("current exact bytes\n")
        self.git(source, "add", ".")
        self.git(source, "commit", "-q", "-m", "squashed current source")
        current = self.git(source, "rev-parse", "HEAD").stdout.strip()
        self.git(source, "update-ref", "refs/remotes/origin/feature/issue-19", old)
        self.git(source, "checkout", "--detach", current)
        for ref in self.git(source, "for-each-ref", "--format=%(refname)", "refs/heads").stdout.splitlines():
            self.git(source, "update-ref", "-d", ref)
        return source, old, current

    def test_remote_only_commit_survives_nested_clone_without_changing_checkout(self):
        source, old, current = self.outer()
        checkout = self.root / "inner"
        self.git(self.root, "clone", "--no-local", str(source), str(checkout))
        self.assertEqual(0, self.git(source, "cat-file", "-e", old + "^{commit}").returncode)
        self.assertNotEqual(0, self.git(checkout, "cat-file", "-e", old + "^{commit}", check=False).returncode)
        before = (self.git(checkout, "rev-parse", "HEAD").stdout,
                  self.git(checkout, "rev-parse", "HEAD^{tree}").stdout,
                  self.git(checkout, "status", "--porcelain").stdout)
        runner.retain_checkout_history(source, checkout)
        self.assertEqual("historical exact bytes\n", self.git(checkout, "show", old + ":snapshot.txt").stdout)
        self.assertEqual(old, self.git(checkout, "rev-parse", "refs/codex-ci-history/feature/issue-19").stdout.strip())
        self.assertEqual(current, self.git(checkout, "rev-parse", "HEAD").stdout.strip())
        self.assertEqual(before, (self.git(checkout, "rev-parse", "HEAD").stdout,
                                 self.git(checkout, "rev-parse", "HEAD^{tree}").stdout,
                                 self.git(checkout, "status", "--porcelain").stdout))
        self.assertFalse((checkout / ".git/FETCH_HEAD").exists())
        self.assertEqual("", self.git(checkout, "for-each-ref", "--format=%(refname)", "refs/heads").stdout)

    def test_local_checkout_without_remote_refs_remains_supported(self):
        source, _, current = self.outer()
        self.git(source, "update-ref", "-d", "refs/remotes/origin/feature/issue-19")
        checkout = self.root / "inner"
        self.git(self.root, "clone", "--no-local", str(source), str(checkout))
        runner.retain_checkout_history(source, checkout)
        self.assertEqual(current, self.git(checkout, "rev-parse", "HEAD").stdout.strip())
        self.assertEqual("", self.git(checkout, "for-each-ref", "refs/codex-ci-history").stdout)
