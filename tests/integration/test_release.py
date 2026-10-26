"""Real release switches. Opt-in fixtures are existing fitted T11/T12 artifacts only."""

from copy import deepcopy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import time
import unittest
from urllib.request import Request, urlopen

from ats_mlops.release.controller import Controller
from ats_mlops.release.runtime import DockerRuntime, Registry, docker, http_json
from ats_mlops.release.state import ReleaseError, atomic_json, sha


class BadEventRuntime(DockerRuntime):
    """Scoped actual startup fault, applied only to this test's B container."""
    def __init__(self, root, config, release_id):
        super().__init__(root, config)
        self.failed_release_id = release_id

    def compose(self, launch):
        value = super().compose(launch)
        if launch["package_path"] == f"releases/{self.failed_release_id}/package":
            value["services"]["ranking"]["environment"] = {"ATS_EVENT_DB": "/release/forbidden.sqlite3"}
        return value


@unittest.skipUnless(os.environ.get("ATS_RUN_RELEASE") == "1", "requires prepared T11/T12 packages, Docker and local Registry")
class ReleaseIntegrationTests(unittest.TestCase):
    def test_actual_release_lifecycle(self):
        specification = json.loads(Path(os.environ["ATS_RELEASE_REQUEST"]).read_text())
        output = Path(os.environ["ATS_RELEASE_EVIDENCE"])
        output.mkdir(parents=True, exist_ok=False)
        root = output / "state"
        controller = Controller(root, specification["config"])
        fixtures = specification["releases"]
        report = {"schema_version": 1, "outcome": "running", "checks": {}, "operations": [],
                  "executable_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                  "config": specification["config"], "fixtures": {}, "no_fit_or_tune": True}
        for key, fixture in fixtures.items():
            report["fixtures"][key] = {"manifest_sha256": sha((Path(fixture["package"]) / "release_manifest.json").read_bytes()),
                                       "check_sha256": sha(Path(fixture["check"]).read_bytes())}
        samples, stop = [], threading.Event()
        port = specification["config"]["port"]

        def monitor():
            while not stop.is_set():
                observation = {"monotonic": time.monotonic()}
                try:
                    request = Request(f"http://127.0.0.1:{port}/metadata", data=b"{}", headers={"Content-Type": "application/json"})
                    with urlopen(request, timeout=.4) as response:
                        value = json.load(response)
                    observation.update(ready=value.get("ready") is True, release_id=value.get("release_id"))
                except Exception:
                    observation["ready"] = False
                samples.append(observation)
                stop.wait(.1)

        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()

        def operation(name, function):
            start = time.monotonic()
            try:
                return function()
            finally:
                report["operations"].append({"name": name, "started_monotonic": start,
                                              "ended_monotonic": time.monotonic()})

        def stage(key):
            return controller.stage(fixtures[key]["package"], fixtures[key]["check"])

        def refused(name, function, reason):
            before = controller.runtime.ids()
            with self.assertRaisesRegex(ReleaseError, reason):
                function()
            self.assertEqual(before, controller.runtime.ids())
            report["checks"][name] = True

        def crash(key, phase):
            # A real abrupt process exit, preserving the pending fsynced intent.
            code = ("import os,sys;from ats_mlops.release.controller import Controller;"
                    "c=Controller(sys.argv[1],checkpoint=lambda p:os._exit(23) if p==sys.argv[3] else None);"
                    "c.activate(sys.argv[2])")
            result = subprocess.run([sys.executable, "-B", "-c", code, str(root), fixtures[key]["release_id"], phase],
                                    capture_output=True, timeout=120)
            self.assertEqual(23, result.returncode, result.stderr.decode()[-1000:])
            pending = controller.store.read()["pending"]
            self.assertEqual(phase, pending["phase"])
            report["checks"]["process_exit_" + phase] = True

        try:
            rejected = output / "rejected-check.json"
            value = json.loads(Path(fixtures["A"]["check"]).read_text())
            value["outcome"] = "rejected"
            atomic_json(rejected, value)
            refused("rejected_check", lambda: controller.stage(fixtures["A"]["package"], rejected), "release_check_rejected")
            tampered = output / "tampered-package"
            shutil.copytree(fixtures["A"]["package"], tampered)
            tampered_manifest = tampered / "run_manifest.json"
            tampered_manifest.chmod(0o600)
            tampered_manifest.write_bytes(b"{}")
            refused("tampered_package", lambda: controller.stage(tampered, fixtures["A"]["check"]), "package_binding_invalid")
            a = stage("A")["release_id"]
            refused("missing_approval", lambda: controller.activate(a), "approval_required")
            controller.approve(a, "agent-operator")
            operation("first_A", lambda: controller.activate(a))
            report["checks"]["first_release_ready_alias"] = controller.status()["active"]["release_id"] == a
            payload = {"request_id": "t13-original-A", "vacancy": {"id": "synthetic-v", "text": "Python engineer"},
                       "candidates": [{"id": "synthetic-c", "text": "Python engineer"}], "top_k": 1}
            original = http_json(f"http://127.0.0.1:{port}/rank", payload)
            b, c, d = (fixtures[key]["release_id"] for key in ("B", "C", "D"))
            stage("B")
            stage("C")
            controller.approve(b, "agent-operator")
            controller.approve(c, "agent-operator")
            with controller.store.lock():
                code = "from ats_mlops.release.controller import Controller;import sys;Controller(sys.argv[1]).status()"
                completed = subprocess.run([sys.executable, "-B", "-c", code, str(root)], capture_output=True)
                self.assertNotEqual(0, completed.returncode)
                self.assertIn(b"operator_busy", completed.stderr)
            report["checks"]["concurrent_process_refused"] = True
            normal = controller.runtime
            controller.runtime = BadEventRuntime(root, specification["config"], b)
            healthy_registry = controller.registry

            def unavailable_during_start(phase):
                if phase == "previous_stopped":
                    controller.registry = Registry(dict(specification["config"], registry_uri="http://127.0.0.1:1"))

            controller.checkpoint = unavailable_during_start
            with self.assertRaisesRegex(ReleaseError, "registry_unavailable"):
                operation("unhealthy_B_restore_A", lambda: controller.activate(b))
            self.assertEqual(a, controller.runtime.observe()["metadata"]["release_id"])
            self.assertEqual("previous_ready", controller.store.read()["pending"]["phase"])
            report["checks"]["unhealthy_target_registry_outage_local_restore"] = True
            controller.registry, controller.checkpoint = healthy_registry, lambda phase: None
            controller.runtime = normal
            operation("reconcile_restored_A", controller.reconcile)
            self.assertEqual(a, controller.status()["active"]["release_id"])
            report["checks"]["actual_unhealthy_container_restores_A"] = True
            operation("crash_ready_B", lambda: crash("B", "target_ready"))
            operation("reconcile_B", controller.reconcile)
            self.assertEqual(b, controller.status()["active"]["release_id"])
            refused("stale_C_approve", lambda: controller.approve(c, "agent-operator"), "stale_generation")
            refused("stale_C_activate", lambda: controller.activate(c), "stale_generation")
            self.assertEqual(original, http_json(f"http://127.0.0.1:{port}/rank", payload))
            report["checks"]["old_response_survives_update"] = True
            stage("D")
            controller.approve(d, "agent-operator")
            operation("crash_alias_D", lambda: crash("D", "alias_confirmed"))
            operation("reconcile_D", controller.reconcile)
            state = controller.status()
            self.assertEqual([b, a], [item["release_id"] for item in state["previous"]])
            report["checks"]["two_previous_retained"] = all((root / item["package_path"]).is_dir() for item in state["previous"])
            operation("rollback_B", lambda: controller.rollback(b))
            self.assertEqual(original, http_json(f"http://127.0.0.1:{port}/rank", payload))
            report["checks"]["explicit_rollback_persistent_original_response"] = True
            refused("ABA_C_activate", lambda: controller.activate(c), "stale_generation")
            registry = controller.registry

            def unavailable(phase):
                if phase == "target_ready":
                    bad = dict(specification["config"], registry_uri="http://127.0.0.1:1")
                    controller.registry = Registry(bad)

            controller.checkpoint = unavailable
            with self.assertRaisesRegex(ReleaseError, "registry_unavailable"):
                operation("registry_connection_refused_after_ready_D", lambda: controller.rollback(d))
            self.assertEqual(d, controller.runtime.observe()["metadata"]["release_id"])
            self.assertIsNotNone(controller.store.read()["pending"])
            report["checks"]["registry_outage_keeps_healthy_API_pending"] = True
            controller.registry, controller.checkpoint = registry, lambda phase: None
            operation("reconcile_registry_recovered_D", controller.reconcile)
            state = controller.status()
            generation = state["generation"]
            self.assertEqual(generation, controller.reconcile()["generation"])
            report["checks"]["reconcile_idempotent"] = True
            report["checks"]["API_state_alias_consistent"] = state["active"]["release_id"] == d
            report["final_state"] = state
            report["final_observation"] = controller.runtime.observe()
            report["journal_sha256"] = sha((root / "journal.jsonl").read_bytes())
            container = controller.runtime.ids()[0]
            report["resources"] = json.loads(docker("inspect", "--format", '{{json .State.OOMKilled}}', container))
            self.assertIs(report["resources"], False)
            report["container_restarts"] = int(docker("inspect", "--format", '{{.RestartCount}}', container))
            self.assertEqual(0, report["container_restarts"])
            report["outcome"] = "passed"
        finally:
            stop.set()
            thread.join(timeout=2)
            report["metadata_samples"] = samples
            report["probe_interval_seconds"] = .1
            # Observed unavailable intervals bounded by surrounding successful probes.
            bounds, last_good, failed = [], None, False
            for sample in samples:
                if sample["ready"]:
                    if failed and last_good is not None:
                        bounds.append(sample["monotonic"] - last_good)
                    last_good, failed = sample["monotonic"], False
                else:
                    failed = True
            report["observed_outage_upper_bounds_seconds"] = bounds
            atomic_json(output / "report.json", report)
            # Intentionally preserve the isolated healthy endpoint for independent read-only acceptance.
