"""Sandbox Git/source and phase-binding regressions; no runtime/model/test IO."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from ats_mlops.data.reservation import ROOT_POLICY_SHA256
from tests.e2e import audit
from tests.e2e.driver import Collector

PROJECT = Path(__file__).resolve().parents[2]
ORIGINAL = "01ccf13d123857ea7b0c8c36fef6c29a13935b78"
Q = "1" * 40
Z = "2" * 40


class Files:
    def __init__(self, root): self.root = root

    def put(self, name, value):
        path = self.root / name; path.parent.mkdir(parents=True, exist_ok=True)
        content = value if isinstance(value, bytes) else audit.canonical(value)
        path.write_bytes(content)
        return {"path": name, "sha256": audit.digest(content)}


class SourceGraphs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.seed = {name: subprocess.check_output(["git", "show", ORIGINAL + ":" + name], cwd=PROJECT)
                    for name in audit._SOURCE_REQUIRED}
        cls.current = {name: (PROJECT / name).read_bytes() for name in ("tests/e2e/audit.py", "tests/e2e/driver.py")}

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name); self.repo = root / "git"; self.repo.mkdir()
        self.files = Files(root / "evidence"); self.files.root.mkdir()
        self.env = os.environ | {"GIT_AUTHOR_NAME": "sandbox", "GIT_AUTHOR_EMAIL": "sandbox@example.invalid",
                                 "GIT_COMMITTER_NAME": "sandbox", "GIT_COMMITTER_EMAIL": "sandbox@example.invalid"}
        self.git("init", "-q")
        for name, content in self.seed.items():
            path = self.repo / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(content)
        self.old = self.commit("qualified ancestor")
        self.snapshot = {"head": self.files.put("old/head", (self.old + "\n").encode()),
                         "status": self.files.put("old/status", b""), "files": {}}
        for name, content in self.seed.items(): self.snapshot["files"][name] = self.files.put("old/files/" + name, content)
        for name, content in self.current.items(): (self.repo / name).write_bytes(content)
        path = self.repo / "src/ats_mlops/evaluation/native_monitoring.py"
        path.write_bytes(path.read_bytes() + b"\n# new S6 fixture correction\n")
        path = self.repo / "src/ats_mlops/evaluation/negative_model.py"
        path.write_bytes(path.read_bytes() + b"\n# attributed external producer correction\n")
        self.new = self.commit("current readiness")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, env=self.env, stderr=subprocess.PIPE)

    def commit(self, message):
        self.git("add", "."); self.git("commit", "-q", "-m", message)
        return self.git("rev-parse", "HEAD").decode().strip()

    def mapping(self):
        return {key: self.old if key in {"S1", "S2", "S3", "S4", "S5"} else self.new for key in audit.SCENARIOS}

    def validate(self, mapping=None, historical=None):
        return audit.validate_execution_sources(audit.Artifacts(self.files.root), self.mapping() if mapping is None else mapping,
            {self.old: self.snapshot} if historical is None else historical, source_sha=self.new,
            qualified_sha=self.old, repository=self.repo)

    def test_verified_ancestor_keeps_historical_case_sources(self):
        self.assertEqual(self.validate(), self.mapping())

    def test_unknown_or_misassigned_case_source_rejected(self):
        for key, value in (("S1", "f" * 40), ("S4", self.new), ("S6", self.old), ("S7", self.old)):
            with self.subTest(case=key):
                mapping = self.mapping(); mapping[key] = value
                with self.assertRaises(audit.EvidenceError): self.validate(mapping)

    def test_missing_map_case_or_snapshot_rejected(self):
        mapping = self.mapping(); del mapping["S8"]
        with self.assertRaises(audit.EvidenceError): self.validate(mapping)
        with self.assertRaises(audit.EvidenceError): self.validate(historical={})

    def test_missing_historical_blob_rejected(self):
        del self.snapshot["files"]["src/ats_mlops/evaluation/native.py"]
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_tampered_snapshot_byte_rejected(self):
        (self.files.root / self.snapshot["files"]["requirements-ml.lock"]["path"]).write_bytes(b"changed")
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_unknown_snapshot_or_unclean_status_rejected(self):
        with self.assertRaises(audit.EvidenceError): self.validate(historical={self.old: self.snapshot, "f" * 40: self.snapshot})
        self.snapshot["status"] = self.files.put("old/status", b" M configs/model.json\n")
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_unrelated_git_commit_rejected(self):
        self.new = self.git("commit-tree", self.git("rev-parse", "HEAD^{tree}").decode().strip(), "-m", "unrelated").decode().strip()
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_changed_runtime_lock_or_service_rejected(self):
        path = self.repo / "requirements-ml.lock"; path.write_bytes(path.read_bytes() + b"\n")
        self.new = self.commit("changed training environment")
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_changed_semantic_helper_rejected_despite_allowed_audit_path(self):
        path = self.repo / "tests/e2e/audit.py"
        path.write_text(path.read_text().replace('raise EvidenceError(reason)', 'raise EvidenceError("waived")', 1))
        self.new = self.commit("weakened assertion")
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_changed_runtime_phase_rejected_despite_allowed_driver_path(self):
        path = self.repo / "tests/e2e/driver.py"; original = path.read_text()
        changed = original.replace('require(action in PHASES,"unknown_qualification_phase")', 'require(True,"unknown_qualification_phase")', 1)
        self.assertNotEqual(original, changed); path.write_text(changed)
        self.new = self.commit("weakened phase scope")
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_same_source_needs_no_fake_historical_snapshot(self):
        mapping = {key: self.new for key in audit.SCENARIOS}
        self.assertEqual(audit.validate_execution_sources(audit.Artifacts(self.files.root), mapping, {},
            source_sha=self.new, qualified_sha=self.new, repository=self.repo), mapping)

    def producer_inputs(self, producer=None, *, omit=()):
        name = "src/ats_mlops/evaluation/negative_model.py"
        content = self.git("show", self.new + ":" + name)
        if producer is None:
            producer = {"schema_version": 1, "source_git_sha": self.new, "repository_path": name, "file_sha256": audit.digest(content)}
        inputs = {}
        for key in ("candidate_run", "candidate_pipeline", "fault_provenance", "embedded"):
            value = {"application_source_git_sha": self.old}
            if key not in omit: value["artifact_producer"] = deepcopy(producer)
            reference = self.files.put("producer/" + key + ".json", value)
            if key == "embedded": inputs["candidate_files"] = {"extra_files/input-provenance.json": reference}
            else: inputs[key] = reference
        return inputs, {name: {"path": "unused-already-validated-current-source", "sha256": audit.digest(content)}}, producer

    def validate_producer(self, inputs, files, *, required=True):
        return audit.validate_negative_producer(audit.Artifacts(self.files.root), inputs, files,
            source_sha=self.new, repository=self.repo, required=required)

    def test_negative_producer_exact_component_git_bytes_pass(self):
        inputs, files, producer = self.producer_inputs()
        self.assertEqual(self.validate_producer(inputs, files), producer)

    def test_missing_or_contradictory_producer_top_fields_rejected(self):
        for omitted in (("candidate_run",), ("embedded",), ("candidate_run", "candidate_pipeline", "fault_provenance", "embedded")):
            with self.subTest(omitted=omitted):
                inputs, files, _ = self.producer_inputs(omit=omitted)
                with self.assertRaises(audit.EvidenceError): self.validate_producer(inputs, files)
        inputs, files, producer = self.producer_inputs()
        producer["source_git_sha"] = self.old
        inputs["fault_provenance"] = self.files.put("producer/contradiction.json", {"artifact_producer": producer})
        with self.assertRaises(audit.EvidenceError): self.validate_producer(inputs, files)

    def test_negative_producer_wrong_git_path_hash_or_type_rejected(self):
        _, _, identity = self.producer_inputs()
        for key, value in (("source_git_sha", "f" * 40), ("repository_path", "../replacement.py"),
                           ("file_sha256", "0" * 64), ("schema_version", True)):
            with self.subTest(key=key):
                inputs, files, _ = self.producer_inputs({**identity, key: value})
                with self.assertRaises(audit.EvidenceError): self.validate_producer(inputs, files)

    def test_negative_producer_unrelated_commit_rejected_even_for_identical_blob(self):
        _, _, identity = self.producer_inputs()
        orphan = self.git("commit-tree", self.git("rev-parse", "HEAD^{tree}").decode().strip(), "-m", "unrelated producer").decode().strip()
        inputs, files, _ = self.producer_inputs({**identity, "source_git_sha": orphan})
        with self.assertRaises(audit.EvidenceError): self.validate_producer(inputs, files)

    def test_fresh_single_source_may_omit_all_optional_producer_fields(self):
        inputs, files, _ = self.producer_inputs(omit=("candidate_run", "candidate_pipeline", "fault_provenance", "embedded"))
        self.assertIsNone(self.validate_producer(inputs, files, required=False))


class PhaseFixture:
    def __init__(self, files, prefix="one", name="S1", kind="result"):
        self.files, self.prefix = files, prefix
        def put(name, value): return files.put(prefix + "/" + name, value)
        self.put = put
        stdout = put("captured/stdout", b'{"fit_calls":{"pipeline":0,"classifier":0},"validation":{"outcome":"rejected"}}\n')
        stderr = put("captured/stderr", b"")
        self.command = {"id": prefix + "-command", "argv": ["python", "-B", "phase.py"], "exit_code": 0,
                        "expected_exit_code": 0, "accepted_exit_codes": [0], "stdout": stdout, "stderr": stderr}
        raw = {key: deepcopy(self.command[key]) for key in ("argv", "exit_code", "accepted_exit_codes", "stdout", "stderr")}
        raw["stdout"]["path"] = "commands/001-stdout.txt"; raw["stderr"]["path"] = "commands/001-stderr.txt"
        self.aggregate = {"schema_version": 1, "commands": [raw]}
        commands = put("commands.json", self.aggregate)
        environment = {"project": "ats-acceptance-sandbox", "fixtures": {"bad_candidate_request": {"path": "/private/request.json", "sha256": "a" * 64}}}
        envref = put("environment.json", environment)
        self.spec = json.loads((PROJECT / "configs/experiment.template.json").read_bytes())
        self.spec["source_git_sha"] = Q
        if name != "S1":
            self.spec["qualified_model_source_git_sha"] = Q
            self.spec["bindings"] = {key: "a" * 64 for key in self.spec["bindings"]}
            self.spec["bindings"]["root_policy_sha256"] = ROOT_POLICY_SHA256
            self.spec["test_inventory"] = {"pairs": 20, "groups": 10, "group_ids_sha256": "a" * 64, "pair_ids_sha256": "b" * 64}
            self.spec["adapter_bindings"] = {key: {"entrypoint": "sandbox.module:run", "source_sha256": "a" * 64} for key in self.spec["adapter_bindings"]}
        self.spec["bindings"]["environment_sha256"] = envref["sha256"]
        specref = put("spec.json", self.spec)
        self.auth = {"schema_version": 1, "scope": "T18_private_qualification", "actor": "root-coordinator",
            "source_git_sha": Q, "project": environment["project"], "input_spec_sha256": specref["sha256"], "phases": [name]}
        authref = put("authorization.json", self.auth)
        self.intent = {"schema_version": 1, "source_git_sha": Q, "phase": name, "started_at": "2026-01-01T00:00:01+00:00",
            "input_spec_sha256": specref["sha256"], "authorization_sha256": authref["sha256"]}
        intentref = put("intent.json", self.intent)
        self.selected = {"source_git_sha": Q, "outcome": "passed",
            "command_evidence_sha256" if name == "S1" else "evidence_sha256": commands["sha256"]}
        if kind == "result": leaves = {"result": put("startup.json", self.selected)}
        elif kind == "receipt":
            receipt = put("native.json", {"source_git_sha": Q, "outcome": "passed"})
            self.selected["native_receipt_sha256"] = receipt["sha256"]; leaves = {"receipt": receipt}
        else:
            self.selected["outcome"] = "expected_rejection"
            fault = put("fault.json", {"fault": name.removeprefix("fault-"), "state": {"active": "previous"}})
            observation = {"fault": name.removeprefix("fault-"), "fault_receipt_sha256": fault["sha256"], "final_consistent": True}
            self.selected["observations"] = observation
            leaves = {"observations": put("observations.json", observation), "fault": fault}
        self.phase = {"schema_version": 1, "source_git_sha": Q, "outcome": "completed", "phase": name,
            "started_at": self.intent["started_at"], "finished_at": "2026-01-01T00:00:02+00:00",
            "input_spec_sha256": specref["sha256"], "result": self.selected}
        self.attestation = {"phase": put("phase.json", self.phase), "intent": intentref, "spec": specref,
            "environment": envref, "authorization": authref, "commands": commands,
            "command_ids": [self.command["id"]], "result_index": None, "leaves": leaves}

    def envelope(self):
        return {"scenario": "S1", "started_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T00:00:03+00:00",
            "commands": [self.command], "native": {"startup": self.attestation["leaves"]["result"]},
            "phase_attestations": [self.attestation]}


class PhaseAttestations(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.files = Files(Path(self.temp.name)); self.fixture = PhaseFixture(self.files)

    def validate(self, value=None):
        return audit.phase_attestations(audit.Artifacts(self.files.root), self.fixture.envelope() if value is None else value, Q)

    def test_original_authorized_S1_result_and_commands_pass(self):
        self.assertEqual(self.validate(), 1)

    def test_wrong_phase_source_rejected(self):
        self.fixture.phase["source_git_sha"] = Z
        self.fixture.attestation["phase"] = self.fixture.put("phase.json", self.fixture.phase)
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_raw_spec_or_environment_hash_mismatch_rejected(self):
        for key, document in (("spec", {**self.fixture.spec, "study_id": "different"}), ("environment", {"project": "foreign"})):
            with self.subTest(key=key):
                value = self.fixture.envelope(); attestation = deepcopy(self.fixture.attestation)
                attestation[key] = self.fixture.put("foreign-" + key + ".json", document); value["phase_attestations"] = [attestation]
                with self.assertRaises(audit.EvidenceError): self.validate(value)

    def test_invented_authorization_cannot_replace_root_scope(self):
        self.fixture.auth["actor"] = "caller"
        authref = self.fixture.put("authorization.json", self.fixture.auth)
        self.fixture.intent["authorization_sha256"] = authref["sha256"]
        self.fixture.attestation["authorization"] = authref
        self.fixture.attestation["intent"] = self.fixture.put("intent.json", self.fixture.intent)
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_arbitrary_passed_result_leaf_rejected(self):
        self.fixture.attestation["leaves"]["result"] = self.fixture.put("fake-result.json", {"outcome": "passed"})
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_completed_phase_with_failed_selected_result_rejected(self):
        self.fixture.selected["outcome"] = "failed"
        self.fixture.attestation["phase"] = self.fixture.put("phase.json", self.fixture.phase)
        self.fixture.attestation["leaves"]["result"] = self.fixture.put("startup.json", self.fixture.selected)
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_foreign_source_on_selected_leaf_rejected(self):
        self.fixture.selected["source_git_sha"] = Z
        self.fixture.attestation["phase"] = self.fixture.put("phase.json", self.fixture.phase)
        self.fixture.attestation["leaves"]["result"] = self.fixture.put("startup.json", self.fixture.selected)
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_changed_commands_hash_rejected(self):
        self.fixture.selected["command_evidence_sha256"] = "f" * 64
        self.fixture.attestation["phase"] = self.fixture.put("phase.json", self.fixture.phase)
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_raw_command_argv_must_equal_captured_command(self):
        self.fixture.aggregate["commands"][0]["argv"] = ["invented-success"]
        ref = self.fixture.put("commands.json", self.fixture.aggregate)
        self.fixture.selected["command_evidence_sha256"] = ref["sha256"]
        self.fixture.attestation["commands"] = ref
        self.fixture.attestation["phase"] = self.fixture.put("phase.json", self.fixture.phase)
        self.fixture.attestation["leaves"]["result"] = self.fixture.put("startup.json", self.fixture.selected)
        with self.assertRaises(audit.EvidenceError): self.validate()

    def test_unattested_extra_command_rejected(self):
        value = self.fixture.envelope(); command = deepcopy(self.fixture.command); command["id"] = "unbound"
        value["commands"] = [self.fixture.command, command]
        with self.assertRaises(audit.EvidenceError): self.validate(value)

    def test_contradictory_phase_observations_rejected(self):
        fixture = PhaseFixture(self.files, "fault", "fault-minio_unavailable", "fault")
        fixture.attestation["leaves"]["observations"] = fixture.put("observations.json", {"fault": "different", "final_consistent": True})
        with self.assertRaises(audit.EvidenceError):
            audit._attested_phase(audit.Artifacts(self.files.root), fixture.attestation,
                {"started_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T00:00:03+00:00"}, Q,
                {fixture.command["id"]: (0, fixture.command)})

    def test_S5_requires_all_four_faults_and_registry_actual_receipts(self):
        fixtures = [PhaseFixture(self.files, name, "fault-" + name, "fault") for name in
                    ("minio_unavailable", "incomplete_artifact", "unready_container", "switch_alias_crash")]
        registry = PhaseFixture(self.files, "registry", "registry", "receipt"); fixtures.append(registry)
        value = {"scenario": "S5", "started_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T00:00:03+00:00",
            "commands": [fixture.command for fixture in fixtures], "phase_attestations": [fixture.attestation for fixture in fixtures],
            "native": {"faults": {fixture.phase["phase"].removeprefix("fault-"): fixture.attestation["leaves"] for fixture in fixtures[:-1]},
                       "registry": registry.attestation["leaves"]["receipt"]}}
        self.assertEqual(self.validate(value), 5)
        value["phase_attestations"] = value["phase_attestations"][:-1]; value["commands"] = value["commands"][:-1]
        with self.assertRaises(audit.EvidenceError): self.validate(value)

    def test_unknown_leaf_or_unindexed_S2_rejected(self):
        self.fixture.attestation["leaves"]["passed"] = self.fixture.put("flag.json", {"passed": True})
        with self.assertRaises(audit.EvidenceError): self.validate()

    def s2_envelope(self):
        fixtures = [PhaseFixture(self.files, "replica-" + str(index), "S2") for index in range(3)]
        results, replicas = [], []
        for index, fixture in enumerate(fixtures):
            run = {"mlflow": {"run_id": str(index + 1) * 32, "model_version": str(index + 3)},
                "data_manifest_sha256": "d" * 64, "split_sha256": "e" * 64, "features_sha256": "f" * 64}
            features = {"vocabulary": {"one": 0}, "idf": [1.0], "transformed_sha256": {"validation": "a" * 64}}
            scores = {"rows": [{"vacancy_id": "v", "candidate_id": "c", "score": .5}]}
            metrics = {"ndcg_at_10": .8, "recall_at_10": .5}
            observed = {"source_git_sha": Q, "workspace_id": "workspace-" + str(index),
                "training_run_id": run["mlflow"]["run_id"], "model_version": run["mlflow"]["model_version"],
                "data_sha256": run["data_manifest_sha256"], "split_sha256": run["split_sha256"], "features_sha256": run["features_sha256"],
                "coordinates_sha256": audit.digest(audit.canonical(fixture.spec["bindings"])),
                "vocabulary_sha256": audit.digest(audit.canonical(features)),
                "scores": [{"group_id": "v", "candidate_id": "c", "score": .5}], "metrics": metrics}
            selected = {"outcome": "passed", "observations": observed, "evidence_sha256": fixture.attestation["commands"]["sha256"]}
            results.append(selected)
            replica = {"workspace_id": observed["workspace_id"], **{key: fixture.put(key + ".json", value)
                for key, value in (("run", run), ("features", features), ("scores", scores), ("metrics", metrics))}}
            replicas.append(replica)
            fixture.attestation["result_index"] = index
            fixture.attestation["leaves"] = {"observations": fixture.put("native-observations.json", observed)}
        phase = {**fixtures[0].phase, "result": results}; reference = self.files.put("common-S2-phase.json", phase)
        for fixture in fixtures: fixture.attestation["phase"] = reference
        return {"scenario": "S2", "started_at": "2026-01-01T00:00:00+00:00", "finished_at": "2026-01-01T00:00:03+00:00",
            "commands": [fixture.command for fixture in fixtures], "phase_attestations": [fixture.attestation for fixture in fixtures],
            "native": {"replicas": replicas}}

    def test_three_indexed_S2_results_bind_original_numeric_replicas(self):
        self.assertEqual(self.validate(self.s2_envelope()), 3)

    def test_S2_index_bool_out_of_range_or_unbound_numeric_replica_rejected(self):
        for index in (None, True, 3):
            with self.subTest(index=index):
                value = self.s2_envelope(); value["phase_attestations"][0]["result_index"] = index
                with self.assertRaises(audit.EvidenceError): self.validate(value)
        value = self.s2_envelope(); value["native"]["replicas"][0]["workspace_id"] = "foreign-workspace"
        with self.assertRaises(audit.EvidenceError): self.validate(value)


class CollectorSources(unittest.TestCase):
    def test_envelope_records_actual_source_without_changing_final_spec_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            collector = Collector(Path(directory).resolve() / "bundle")
            spec = {"source_git_sha": Z}
            reference = collector.envelope("S1", spec, "2026-01-01T00:00:00+00:00", "2026-01-01T00:00:01+00:00",
                {}, [], execution_source=Q, phase_attestations=[])
            value = audit.Artifacts(collector.root).document(reference)
            self.assertEqual(value["source_git_sha"], Q)
            self.assertEqual(value["spec_sha256"], audit.digest(audit.canonical(spec)))
            with self.assertRaises(ValueError): collector.envelope("S2", spec, "a", "b", {}, [], execution_source="unknown")


if __name__ == "__main__": unittest.main()
