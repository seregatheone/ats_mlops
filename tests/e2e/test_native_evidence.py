"""Synthetic receipt mutations only; these tests never qualify real acceptance."""
from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from tests.e2e.audit import Artifacts,EvidenceError,canonical,digest
from tests.e2e.native import journal,validation_gate
from tests.e2e.test_audit_contract import save


def gate_fixture(accepted=True):
    from ats_mlops.quality.policy import assess_scores
    rows=[dict(vacancy_id='v',candidate_id=name,vacancy_text='python',candidate_text=text,relevance=label)
          for name,text,label in [('a','java',0),('z','python',1)]]
    scores={('v','a'):.1 if accepted else .9,('v','z'):.9 if accepted else .1}
    value=assess_scores(rows,['v'],scores)
    value.update(schema_version=1,test_read=False,validation={'read_partitions':['validation']},
        candidate={'model_sha256':'a'*64},scoring_receipts={'candidate':dict(isolated=True,bytecode_disabled=True,
        prior_project_import=False,network=False,training_checkout_mounted=False,credentials=False,model_sha256='a'*64,scores_sha256='b'*64)})
    return value


class NativeEvidenceTests(unittest.TestCase):
    def test_real_pure_gate_math_replayed_from_synthetic_defined_metrics(self):
        validation_gate(gate_fixture(),accepted=True)
        validation_gate(gate_fixture(False),accepted=False)
        for variant in ('nonfinite','threshold','cohort','test','credentials','missing_model','lineage_only_rejection'):
            value=gate_fixture(False)
            if variant=='nonfinite':value['metrics']['candidate']['ndcg_at_10']=float('nan')
            elif variant=='threshold':value['comparisons']['baseline']['max_ndcg_drop']=.1
            elif variant=='cohort':value['metrics']['candidate']['eligible_group_ids']=['other']
            elif variant=='test':value['test_read']=True
            elif variant=='credentials':value['scoring_receipts']['candidate']['credentials']=True
            elif variant=='missing_model':value['candidate']=None
            else:value['reasons']=['candidate.model_inventory_mismatch']
            with self.subTest(variant=variant),self.assertRaises(EvidenceError):validation_gate(value,accepted=False)

    def test_native_T13_journal_hash_chain_and_projection_are_replayed(self):
        from ats_mlops.release.state import Store
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);store=Store(root/'catalog');state=store.read()
            store.append(state,'staged',{'release_id':'toy'})
            store.append(state,'approved',{'release_id':'toy'})
            store.append(state,'active_committed',{'release_id':'toy'})
            content=(store.root/'journal.jsonl').read_bytes();path=root/'journal.jsonl';path.write_bytes(content)
            reference={'path':'journal.jsonl','sha256':digest(content)}
            observed,kinds=journal(Artifacts(root),reference)
            self.assertEqual(state,observed);self.assertEqual(['staged','approved','active_committed'],kinds)
            for broken in (content[:-1],content.replace(b'"staged"',b'"forged"',1)):
                path.write_bytes(broken);changed=dict(reference,sha256=digest(broken))
                with self.assertRaises(EvidenceError):journal(Artifacts(root),changed)

    def test_all_eight_native_validators_are_explicit_not_default_success(self):
        from tests.e2e.native import VALIDATORS
        from tests.e2e.audit import SCENARIOS
        self.assertEqual(set(SCENARIOS),set(VALIDATORS))
        with tempfile.TemporaryDirectory() as folder:
            for name,validator in VALIDATORS.items():
                with self.subTest(name=name),self.assertRaises((KeyError,EvidenceError,TypeError)):
                    validator(Artifacts(Path(folder)),{'outcome':'passed','native':{}})


if __name__=='__main__':unittest.main()
