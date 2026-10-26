"""Audited local release lifecycle; the journal precedes every external effect."""

from copy import deepcopy
import json
import os
from pathlib import Path
import re
import shutil
import time
from urllib.parse import urlparse

from .runtime import DockerRuntime, Registry
from .state import ReleaseError, Store, atomic_json, canonical, fsync_dir, immutable_json, require, sha


CHECKS = frozenset(("actual_registry_alias_move_independent", "event_write_fail_closed_503",
    "frozen_package_unchanged", "full_event_before_top_k_and_idempotency", "gate_bound_native_reference",
    "http_matches_passed_model", "metadata_image_and_readonly_mounts", "missing_model_503_and_original_release_replay",
    "no_private_text_in_events_or_logs", "restart_original_response", "source_and_credentials_isolation",
    "strict_limits_empty_ties_and_privacy_errors"))
DEFAULT_CONFIG = {"schema_version": 1, "project": "ats-serving", "port": 3000,
    "registry_uri": "http://127.0.0.1:5050", "model_name": "ats-pair-ranking", "alias": "champion",
    "events_volume": "ats-serving-prediction-events", "network": "ats-serving-network",
    "ready_timeout_seconds": 60}


def default_state_dir():
    return Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state"))) / "ats-mlops"


def checked_config(value):
    require(set(value) == set(DEFAULT_CONFIG) and value["schema_version"] == 1, "config_invalid")
    for key in ("project", "events_volume", "network", "alias", "model_name"):
        require(isinstance(value[key], str) and re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}", value[key]), "config_invalid")
    require(type(value["port"]) is int and 1024 <= value["port"] <= 65535, "config_invalid")
    require(type(value["ready_timeout_seconds"]) is int and 1 <= value["ready_timeout_seconds"] <= 300, "config_invalid")
    uri = urlparse(value["registry_uri"])
    require(uri.scheme == "http" and uri.hostname in ("127.0.0.1", "localhost")
            and not uri.username and not uri.password and not uri.query and not uri.fragment
            and uri.path in ("", "/"), "config_invalid")
    return value


def load_package(root, report_path):
    # Reuse T12's inventory/model/gate validator, without importing/executing ML code.
    from service.integrity import document, release_binding

    try:
        manifest, manifest_sha, _ = release_binding(root)
        report, report_sha = document(report_path)
        require(report["schema_version"] == 1 and report["outcome"] == "passed" and not report["reasons"], "release_check_rejected")
        require(set(report["checks"]) == CHECKS and all(value is True for value in report["checks"].values()), "release_check_incomplete")
        expected = {"release_id": manifest["release_id"], "candidate": manifest["candidate"],
                    "evidence_scope": manifest["evidence_scope"], "bento_tag": manifest["bento_tag"],
                    "image_id": manifest["serving"]["image"]["id"],
                    "gate_report_sha256": manifest["gate_report_sha256"], "api_schema_sha256": manifest["api_schema_sha256"],
                    "release_manifest_sha256": manifest_sha, "release_manifest_sha256_after": manifest_sha}
        require(all(report.get(key) == value for key, value in expected.items()), "release_check_binding_invalid")
        require(manifest["evidence_scope"] in ("demo_only", "independently_labeled"), "evidence_scope_invalid")
        return manifest, manifest_sha, report_sha
    except ReleaseError:
        raise
    except Exception as exc:
        raise ReleaseError("package_binding_invalid") from exc


def metadata_for(manifest, digest):
    return {"ready": True, "release_id": manifest["release_id"], "model_version": manifest["candidate"]["model_version"],
            "bento_tag": manifest["bento_tag"], "model_sha256": manifest["candidate"]["model_sha256"],
            "source_tree_sha256": manifest["candidate"]["source_tree_sha256"], "release_manifest_sha256": digest,
            "api_schema_sha256": manifest["api_schema_sha256"], "evidence_scope": manifest["evidence_scope"],
            "serving_image_id": manifest["serving"]["image"]["id"]}


class Controller:
    def __init__(self, root=None, config=None, *, runtime=None, registry=None, checkpoint=None):
        self.store = Store(root or default_state_dir())
        self.root = self.store.root
        with self.store.lock():
            path = self.root / "config.json"
            if path.exists():
                saved = checked_config(json.loads(path.read_text()))
                require(config is None or config == saved, "config_immutable")
                self.config = saved
            else:
                self.config = checked_config(deepcopy(DEFAULT_CONFIG) if config is None else config)
                immutable_json(path, self.config)
        self.runtime = runtime or DockerRuntime(self.root, self.config)
        self.registry = registry or Registry(self.config)
        self.checkpoint = checkpoint or (lambda name: None)

    def _directory(self, release_id):
        require(isinstance(release_id, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", release_id), "release_id_invalid")
        return self.root / "releases" / release_id

    def _validate(self, reference, approval=False):
        manifest, digest, report_sha = load_package(self.root / reference["package_path"], self.root / reference["check_path"])
        require(digest == reference["manifest_sha256"] and report_sha == reference["check_sha256"]
                and sha((self.root / reference["launch_path"]).read_bytes()) == reference["launch_sha256"]
                and sha((self._directory(reference["release_id"]) / "stage.json").read_bytes()) == reference["stage_sha256"], "receipt_changed")
        launch = json.loads((self.root / reference["launch_path"]).read_text())
        require(launch == {"schema_version": 1, "package_path": reference["package_path"], "image_id": reference["image_id"],
                           "config_sha256": sha(canonical(self.config))}
                and reference["image_id"] == manifest["serving"]["image"]["id"], "receipt_changed")
        require(reference["metadata"] == metadata_for(manifest, digest)
                and reference["candidate"] == manifest["candidate"]
                and reference["expected_incumbent"] == manifest["expected_incumbent"], "receipt_changed")
        if approval:
            path = self.root / reference["approval_path"]
            value = json.loads(path.read_text())
            require(sha(path.read_bytes()) == reference["approval_sha256"]
                    and value["stage_sha256"] == reference["stage_sha256"]
                    and value["generation"] == reference["generation"]
                    and value["manifest_sha256"] == digest and value["check_sha256"] == report_sha,
                    "approval_changed")
        return manifest

    def _consistent(self, state):
        require(state["pending"] is None, "recovery_required")
        active = state["active"]
        observed = self.runtime.observe()
        alias = self.registry.get()
        if active:
            self._validate(active, approval=True)
            require(self.runtime.matches(observed, active) and alias == active["candidate"]["model_version"], "active_state_inconsistent")
        else:
            require(observed is None and alias is None, "bootstrap_incumbent_not_empty")

    def _incumbent(self, state, reference):
        require(state["generation"] == reference["generation"], "stale_generation")
        expected, active = reference["expected_incumbent"], state["active"]
        if active is None:
            require(expected is None, "stale_incumbent")
            return
        identity = {key: active["candidate"][key] for key in ("model_name", "model_version", "run_id", "model_sha256")}
        identity.update(release_id=active["release_id"], release_is_fixture=False)
        require(expected == identity, "stale_incumbent")

    def stage(self, package, report):
        with self.store.lock():
            state = self.store.read()
            self._consistent(state)
            manifest, digest, check_sha = load_package(package, report)
            require(manifest["candidate"]["model_name"] == self.config["model_name"], "model_name_mismatch")
            release_id = manifest["release_id"]
            directory = self._directory(release_id)
            require(release_id not in state["staged"] and not directory.exists(), "release_id_exists")
            reference = {"release_id": release_id, "generation": state["generation"], "candidate": manifest["candidate"],
                "expected_incumbent": manifest["expected_incumbent"], "manifest_sha256": digest,
                "check_sha256": check_sha, "image_id": manifest["serving"]["image"]["id"],
                "metadata": metadata_for(manifest, digest), "package_path": f"releases/{release_id}/package",
                "check_path": f"releases/{release_id}/release_check_report.json",
                "launch_path": f"releases/{release_id}/launch.json"}
            self._incumbent(state, reference)
            directory.parent.mkdir(exist_ok=True)
            temporary = directory.with_name(directory.name + ".staging")
            require(not temporary.exists(), "incomplete_stage_requires_inspection")
            temporary.mkdir()
            shutil.copytree(package, temporary / "package")
            shutil.copyfile(report, temporary / "release_check_report.json")
            # Validate the exact bytes after copying, before publishing the catalog.
            copied, copied_sha, copied_check = load_package(temporary / "package", temporary / "release_check_report.json")
            require(copied == manifest and copied_sha == digest and copied_check == check_sha, "stage_copy_changed")
            launch = {"schema_version": 1, "package_path": reference["package_path"], "image_id": reference["image_id"],
                      "config_sha256": sha(canonical(self.config))}
            reference["launch_sha256"] = immutable_json(temporary / "launch.json", launch)
            reference["stage_sha256"] = immutable_json(temporary / "stage.json", reference)
            for path in temporary.rglob("*"):
                if path.is_file():
                    with path.open("rb") as stream:
                        os.fsync(stream.fileno())
                    path.chmod(0o444)
                elif path.is_dir():
                    path.chmod(0o755)
            for path in sorted((p for p in temporary.rglob("*") if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
                fsync_dir(path)
            fsync_dir(temporary)
            os.rename(temporary, directory)
            fsync_dir(directory.parent)
            state["staged"][release_id] = reference
            self.store.append(state, "staged", {"release_id": release_id})
            return reference

    def approve(self, release_id, actor, policy="A3"):
        with self.store.lock():
            state = self.store.read()
            self._consistent(state)
            require(release_id in state["staged"], "release_not_staged")
            reference = state["staged"][release_id]
            manifest = self._validate(reference)
            self._incumbent(state, reference)
            require("approval_sha256" not in reference, "already_approved")
            require(actor == "agent-operator" and policy == "A3" and manifest["evidence_scope"] == "demo_only", "approval_policy_not_authorized")
            approval = {"schema_version": 1, "release_id": release_id, "actor": actor, "policy": policy,
                        "evidence_scope": manifest["evidence_scope"], "timestamp_ns": time.time_ns(),
                        "generation": state["generation"], "stage_sha256": reference["stage_sha256"],
                        "manifest_sha256": reference["manifest_sha256"], "check_sha256": reference["check_sha256"],
                        "gate_sha256": manifest["gate_report_sha256"], "expected_incumbent": reference["expected_incumbent"]}
            reference["approval_path"] = f"releases/{release_id}/approval.json"
            reference["approval_sha256"] = immutable_json(self.root / reference["approval_path"], approval)
            self.store.append(state, "approved", {"release_id": release_id, "actor": actor, "policy": policy})
            return approval

    def _intent(self, state, target, kind):
        state["pending"] = {"kind": kind, "from": deepcopy(state["active"]), "to": deepcopy(target),
                            "generation": state["generation"], "started_ns": time.time_ns(), "phase": "intent"}
        self.store.append(state, "switch_intent", {"kind": kind, "release_id": target["release_id"]})
        self.checkpoint("after_intent")

    def _phase(self, state, phase):
        state["pending"]["phase"] = phase
        self.store.append(state, phase)
        self.checkpoint(phase)

    def _commit(self, state):
        pending = state["pending"]
        target, old = pending["to"], pending["from"]
        state["generation"] += 1
        state["active"] = deepcopy(target)
        history = ([old] if old else []) + state["previous"]
        seen = {target["release_id"]}
        previous = []
        for item in history:
            if item["release_id"] not in seen:
                previous.append(item)
                seen.add(item["release_id"])
        state["previous"] = previous[:2]
        if target["release_id"] not in state["successful"]:
            state["successful"].append(target["release_id"])
        state["last_switch"] = {"kind": pending["kind"], "from": None if old is None else old["release_id"],
                                "to": target["release_id"], "elapsed_seconds": (time.time_ns() - pending["started_ns"]) / 1e9}
        state["pending"] = None
        self.store.append(state, "active_committed", state["last_switch"])
        return state

    def _promote(self, state):
        target, old = state["pending"]["to"], state["pending"]["from"]
        allowed = {None if old is None else old["candidate"]["model_version"], target["candidate"]["model_version"]}
        require(self.registry.get() in allowed, "registry_drift_recovery_required")
        self._phase(state, "alias_intent")
        self.registry.set(target["candidate"]["model_version"])
        self._phase(state, "alias_confirmed")
        return self._commit(state)

    def _restore(self, state):
        old, target = state["pending"]["from"], state["pending"]["to"]
        require(old is not None, "recovery_required_no_previous")
        self._validate(old, approval=True)
        self._phase(state, "restore_intent")
        self.runtime.stop()
        self.runtime.launch(old)
        self.runtime.wait(old)
        self._phase(state, "previous_ready")
        # Restore local availability first; Registry failure leaves this healthy
        # previous API with pending intent until its alias can reconcile.
        require(self.registry.get() in (old["candidate"]["model_version"], target["candidate"]["model_version"]), "registry_drift_recovery_required")
        self.registry.set(old["candidate"]["model_version"])
        state["last_restoration"] = {"release_id": old["release_id"], "failed_release_id": target["release_id"],
                                     "elapsed_seconds": (time.time_ns() - state["pending"]["started_ns"]) / 1e9}
        state["pending"] = None
        self.store.append(state, "previous_restored", state["last_restoration"])
        return state

    def _switch(self, state, target, kind):
        self._intent(state, target, kind)
        try:
            self.runtime.stop()
            self._phase(state, "previous_stopped")
            self.runtime.launch(target)
            self._phase(state, "target_started")
            self.runtime.wait(target)
        except Exception:
            # Failed startup restores the previous service, if this can be proved.
            self._restore(state)
            raise ReleaseError("new_service_failed_previous_restored")
        self._phase(state, "target_ready")
        # Registry errors leave the already healthy API running with durable pending intent.
        return self._promote(state)

    def activate(self, release_id):
        with self.store.lock():
            state = self.store.read()
            self._consistent(state)
            if state["active"] and state["active"]["release_id"] == release_id:
                return state
            require(release_id in state["staged"], "release_not_staged")
            target = state["staged"][release_id]
            require("approval_sha256" in target, "approval_required")
            self._validate(target, approval=True)
            self._incumbent(state, target)
            return self._switch(state, target, "activate")

    def rollback(self, release_id=None):
        with self.store.lock():
            state = self.store.read()
            self._consistent(state)
            require(state["previous"], "rollback_unavailable")
            targets = [item for item in state["previous"] if release_id is None or item["release_id"] == release_id]
            require(targets, "release_not_retained_previous")
            target = targets[0]
            self._validate(target, approval=True)
            # A rollback reuses a previously activated approval, with a new journaled intent.
            return self._switch(state, target, "rollback")

    def reconcile(self):
        with self.store.lock():
            state = self.store.read()
            pending = state["pending"]
            if pending is None:
                self._consistent(state)
                return state
            require(pending["generation"] == state["generation"], "recovery_generation_invalid")
            target, old = pending["to"], pending["from"]
            self._validate(target, approval=True)
            require(pending["kind"] == "rollback" or target["generation"] == state["generation"], "stale_recovery_intent")
            observed = self.runtime.observe()
            if self.runtime.matches(observed, target):
                if pending["phase"] == "restore_intent":
                    return self._restore(state)
                return self._promote(state)
            if old and self.runtime.matches(observed, old):
                self._validate(old, approval=True)
                require(self.registry.get() in (old["candidate"]["model_version"], target["candidate"]["model_version"]), "registry_drift_recovery_required")
                self.registry.set(old["candidate"]["model_version"])
                state["pending"] = None
                self.store.append(state, "previous_restored_after_interruption", {"release_id": old["release_id"]})
                return state
            # Never kill an unknown healthy container while trying to recover.
            require(not observed or not observed.get("ready"), "unknown_ready_service_recovery_required")
            return self._restore(state)

    def status(self):
        with self.store.lock():
            state = self.store.read()
            self._consistent(state)
            return state
