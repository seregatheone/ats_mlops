"""State-machine faults use fake effects; real Docker evidence is a separate opt-in gate."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from ats_mlops.release.controller import CHECKS, Controller, DEFAULT_CONFIG, load_package
from ats_mlops.release.runtime import DockerRuntime
from ats_mlops.release.state import ReleaseError, Store, atomic_json, sha


class Interrupted(BaseException):
    pass


class MemoryRuntime:
    def __init__(self):
        self.active = None
        self.effects = []
        self.fail = set()
        self.intent_assertion = lambda: None

    def observe(self):
        return None if self.active is None else {"ready": True, "reference": self.active}

    def matches(self, observed, reference):
        return observed is not None and observed["reference"]["release_id"] == reference["release_id"]

    def stop(self):
        self.intent_assertion()
        self.effects.append("stop")
        self.active = None

    def launch(self, reference):
        self.intent_assertion()
        self.effects.append("launch:" + reference["release_id"])
        if reference["release_id"] in self.fail:
            raise ReleaseError("new_service_not_ready")
        self.active = deepcopy(reference)

    def wait(self, reference):
        if not self.matches(self.observe(), reference):
            raise ReleaseError("new_service_not_ready")


class MemoryRegistry:
    def __init__(self):
        self.version = None
        self.unavailable = False
        self.changes = []

    def get(self):
        if self.unavailable:
            raise ReleaseError("registry_unavailable")
        return self.version

    def set(self, version):
        self.get()
        self.changes.append(version)
        self.version = version


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)
        self.runtime, self.registry = MemoryRuntime(), MemoryRegistry()
        self.patch = patch("ats_mlops.release.controller.load_package", side_effect=self.binding)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.controller = Controller(self.base / "state", runtime=self.runtime, registry=self.registry)
        self.runtime.intent_assertion = lambda: self.assertIsNotNone(self.controller.store.read()["pending"])

    @staticmethod
    def binding(package, report):
        manifest_bytes = (Path(package) / "release_manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
        value = json.loads(Path(report).read_text())
        if value["outcome"] != "passed":
            raise ReleaseError("release_check_rejected")
        return manifest, sha(manifest_bytes), sha(Path(report).read_bytes())

    def fixture(self, name, incumbent=None, version="4"):
        root = self.base / name
        root.mkdir()
        package = root / "package"
        package.mkdir()
        expected = None
        if incumbent:
            active = self.controller.store.read()["staged"][incumbent]
            expected = {key: active["candidate"][key] for key in ("model_name", "model_version", "run_id", "model_sha256")}
            expected.update(release_id=incumbent, release_is_fixture=False)
        manifest = {"release_id": name, "candidate": {"model_name": "ats-pair-ranking", "model_version": version,
                    "run_id": "run" + version, "model_sha256": "a" * 64, "source_tree_sha256": "b" * 64},
                    "expected_incumbent": expected, "evidence_scope": "demo_only", "bento_tag": "ats_ranker:mabc",
                    "api_schema_sha256": "c" * 64, "gate_report_sha256": "d" * 64,
                    "serving": {"image": {"id": "sha256:" + "e" * 64}}}
        atomic_json(package / "release_manifest.json", manifest)
        atomic_json(root / "check.json", {"outcome": "passed"})
        return package, root / "check.json"

    def stage(self, name, incumbent=None, version="4", approve=True):
        self.controller.stage(*self.fixture(name, incumbent, version))
        if approve:
            self.controller.approve(name, "agent-operator")

    def bootstrap(self):
        self.stage("A")
        self.controller.activate("A")

    def test_incumbent_release_and_generation_prevent_stale_and_aba(self):
        self.bootstrap()
        self.stage("B", "A")
        self.stage("C", "A")
        self.stage("C-unapproved", "A", approve=False)
        self.controller.activate("B")
        with self.assertRaisesRegex(ReleaseError, "stale_generation"):
            self.controller.approve("C-unapproved", "agent-operator")
        with self.assertRaisesRegex(ReleaseError, "stale_generation"):
            self.controller.activate("C")
        with self.assertRaisesRegex(ReleaseError, "stale_incumbent"):
            self.controller.stage(*self.fixture("C-fresh-epoch-old-incumbent", "A"))
        self.controller.rollback("A")
        with self.assertRaisesRegex(ReleaseError, "stale_generation"):
            self.controller.activate("C")

    def test_missing_approval_and_tampered_bytes_never_stop_current(self):
        self.bootstrap()
        self.stage("B", "A", approve=False)
        count = len(self.runtime.effects)
        with self.assertRaisesRegex(ReleaseError, "approval_required"):
            self.controller.activate("B")
        self.controller.approve("B", "agent-operator")
        path = self.controller.root / "releases/B/package/release_manifest.json"
        path.chmod(0o644)
        value = json.loads(path.read_text())
        value["evidence_scope"] = "rejected"
        atomic_json(path, value)
        with self.assertRaisesRegex(ReleaseError, "receipt_changed"):
            self.controller.activate("B")
        self.assertEqual(count, len(self.runtime.effects))

    def test_rejected_stage_has_no_effects(self):
        package, check = self.fixture("bad")
        atomic_json(check, {"outcome": "rejected"})
        with self.assertRaisesRegex(ReleaseError, "release_check_rejected"):
            self.controller.stage(package, check)
        self.assertEqual([], self.runtime.effects)

    def test_unhealthy_target_restores_previous(self):
        self.bootstrap()
        self.stage("B", "A")
        self.runtime.fail.add("B")
        with self.assertRaisesRegex(ReleaseError, "new_service_failed_previous_restored"):
            self.controller.activate("B")
        state = self.controller.status()
        self.assertEqual("A", state["active"]["release_id"])
        self.assertIsNone(state["pending"])
        self.assertEqual("4", self.registry.version)

    def test_crash_after_ready_and_alias_reconcile_once(self):
        self.bootstrap()
        for name, old, checkpoint, version in (("B", "A", "target_ready", "4"), ("D", "B", "alias_confirmed", "5")):
            self.stage(name, old, version)
            self.controller.checkpoint = lambda phase: (_ for _ in ()).throw(Interrupted()) if phase == checkpoint else None
            with self.assertRaises(Interrupted):
                self.controller.activate(name)
            self.assertIsNotNone(self.controller.store.read()["pending"])
            self.controller.checkpoint = lambda phase: None
            state = self.controller.reconcile()
            generation = state["generation"]
            self.assertEqual(name, state["active"]["release_id"])
            self.assertEqual(version, self.registry.version)
            self.assertEqual(generation, self.controller.reconcile()["generation"])
        self.assertEqual(["B", "A"], [item["release_id"] for item in state["previous"]])
        self.assertTrue(all((self.controller.root / item["package_path"]).is_dir() for item in state["previous"]))

    def test_registry_failure_preserves_healthy_target_pending(self):
        self.bootstrap()
        self.stage("B", "A", "5")
        self.controller.checkpoint = lambda phase: setattr(self.registry, "unavailable", True) if phase == "target_ready" else None
        with self.assertRaisesRegex(ReleaseError, "registry_unavailable"):
            self.controller.activate("B")
        self.assertEqual("B", self.runtime.active["release_id"])
        self.assertEqual("A", self.controller.store.read()["active"]["release_id"])
        with self.assertRaisesRegex(ReleaseError, "recovery_required"):
            self.controller.approve("B", "agent-operator")
        self.registry.unavailable = False
        self.controller.checkpoint = lambda phase: None
        self.assertEqual("B", self.controller.reconcile()["active"]["release_id"])

    def test_journal_replays_lagging_cache_and_rejects_corrupt_tail(self):
        self.bootstrap()
        atomic_json(self.controller.root / "state.json", {"old": True})
        self.assertEqual("A", self.controller.status()["active"]["release_id"])
        count = len(self.runtime.effects)
        with (self.controller.root / "journal.jsonl").open("ab") as stream:
            stream.write(b'{"partial":')
        with self.assertRaisesRegex(ReleaseError, "journal_corrupt"):
            self.controller.status()
        self.assertEqual(count, len(self.runtime.effects))

    def test_separate_process_lock_is_nonblocking(self):
        script = ("from ats_mlops.release.state import Store,ReleaseError\n"
                  "import sys\ntry:\n with Store(sys.argv[1]).lock(): pass\n"
                  "except ReleaseError as e:\n print(e);sys.exit(7)\n")
        with self.controller.store.lock():
            result = subprocess.run([sys.executable, "-c", script, str(self.controller.root)], capture_output=True, text=True)
        self.assertEqual(7, result.returncode, result.stderr)
        self.assertEqual("operator_busy", result.stdout.strip())

    def test_network_volume_and_launch_security_stay_constant(self):
        runtime = DockerRuntime(self.controller.root, DEFAULT_CONFIG)
        a = runtime.compose({"image_id": "sha256:a", "package_path": "releases/A/package"})
        b = runtime.compose({"image_id": "sha256:b", "package_path": "releases/B/package"})
        self.assertEqual(a["networks"], b["networks"])
        self.assertEqual(a["volumes"], b["volumes"])
        service = a["services"]["ranking"]
        self.assertEqual("127.0.0.1", service["ports"][0]["host_ip"])
        self.assertTrue(service["read_only"])
        self.assertNotIn("environment", service)
        self.assertNotIn("/var/run/docker.sock", json.dumps(service))


class CheckBindingTests(unittest.TestCase):
    def test_report_exact_required_checks_and_immutable_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "check.json"
            manifest = {"release_id": "A", "candidate": {}, "evidence_scope": "demo_only", "bento_tag": "opaque",
                        "serving": {"image": {"id": "sha256:a"}}, "gate_report_sha256": "gate", "api_schema_sha256": "api"}
            report = dict(manifest, schema_version=1, outcome="passed", reasons=[], image_id="sha256:a",
                          release_manifest_sha256="manifest", release_manifest_sha256_after="manifest",
                          checks={check: True for check in CHECKS})
            report.pop("serving")
            with patch("service.integrity.release_binding", return_value=(manifest, "manifest", {})):
                atomic_json(path, report)
                self.assertEqual(manifest, load_package(directory, path)[0])
                for key, value in (("outcome", "rejected"), ("release_manifest_sha256_after", "tamper"),
                                   ("image_id", "wrong"), ("checks", {check: True for check in list(CHECKS)[:-1]})):
                    changed = dict(report, **{key: value})
                    atomic_json(path, changed)
                    with self.assertRaises(ReleaseError):
                        load_package(directory, path)


if __name__ == "__main__":
    unittest.main()
