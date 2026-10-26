"""Toy E4 perturbation/ordering tests, no API/model/policy/provider/runtime."""
from copy import deepcopy
from pathlib import Path
import ast
import tempfile
import unittest

from ats_mlops.evaluation import experiment as e
from ats_mlops.evaluation import native_monitoring as m
from tests.unit.test_experiment import spec


def rows():
    return [{"vacancy_id":f"g{group:02}","candidate_id":f"c{candidate:02}","vacancy_text":"python sql vacancy",
             "candidate_text":"python sql candidate", "relevance":int(candidate < 5)} for group in range(10) for candidate in range(20)]


class NativeMonitoringTests(unittest.TestCase):
    def test_predeclared_fixture_is_reproducible_and_never_selects_by_labels(self):
        data=rows();first=m.payloads(data,mode="shifted",seed=20261029,prefix="toy")
        changed=deepcopy(data)
        for row in changed:row["relevance"]=1-row["relevance"]
        self.assertEqual(first,m.payloads(changed,mode="shifted",seed=20261029,prefix="toy"))
        self.assertEqual(first,m.payloads(data,mode="shifted",seed=20261029,prefix="toy"))
        self.assertEqual(10,len(first));self.assertEqual(200,sum(len(value["candidates"]) for value in first))
        self.assertTrue(all(value["top_k"]==3 for value in first))
        self.assertTrue(all("relevance" not in candidate for value in first for candidate in value["candidates"]))
        self.assertEqual({row["vacancy_id"] for row in data},{value["vacancy"]["id"] for value in first})

    def test_control_preserves_exact_text_and_all_candidates(self):
        data=rows();actual=m.payloads(data,mode="control",seed=20261029,prefix="toy")
        expected={(row["vacancy_id"],row["candidate_id"]):row["candidate_text"] for row in data}
        self.assertEqual(expected,{(value["vacancy"]["id"],candidate["id"]):candidate["text"] for value in actual for candidate in value["candidates"]})
        small=m.payloads(data,mode="insufficient",seed=20261029,prefix="toy")
        self.assertEqual(1,len(small));self.assertEqual(20,len(small[0]["candidates"]))

    def test_unknown_seed_mode_and_ineligible_base_do_not_get_silent_defaults(self):
        for kwargs in ({"seed":1707,"mode":"control"},{"seed":True,"mode":"control"},{"seed":20261029,"mode":"unknown"}):
            with self.subTest(kwargs=kwargs),self.assertRaises(e.ExperimentError):m.payloads(rows(),prefix="toy",**kwargs)
        with self.assertRaises(e.ExperimentError):m.payloads(rows()[:20],mode="control",seed=20261029,prefix="toy")

    def test_false_signal_and_minimum_probes_precede_confirmed_request(self):
        self.assertLess(m.ORDER.index("false_signal"),m.ORDER.index("shifted_no_labels"))
        self.assertLess(m.ORDER.index("insufficient"),m.ORDER.index("shifted_no_labels"))
        self.assertEqual(("shifted","control"),m.expected_modes("false_signal"))
        self.assertEqual(("shifted","shifted"),m.expected_modes("shifted_no_labels"))
        self.assertLess(m.ORDER.index("shifted_no_labels"),m.ORDER.index("shifted_labeled"))
        self.assertLess(m.ORDER.index("shifted_labeled"),m.ORDER.index("cooldown"))

    def test_frozen_calibration_cannot_be_retuned(self):
        value=spec();card={"outcome":"passed","method":"wasserstein","threshold":.1,"drift_share":.5}
        value["bindings"]["calibration_sha256"]=e.sha(e.canonical(card));m.require_rule(card,value)
        for key,changed in (("threshold",.2),("threshold",True),("drift_share",1),("method","ks"),("outcome","undefined")):
            bad=dict(card);bad[key]=changed
            with self.subTest(key=key),self.assertRaises(e.ExperimentError):m.require_rule(bad,value)

    def test_monitoring_dependency_import_is_lazy_and_no_exercise_or_default_project(self):
        source=Path(m.__file__).read_text();tree=ast.parse(source)
        imports=[node.module for node in tree.body if isinstance(node,ast.ImportFrom)]
        self.assertFalse(any("monitoring" in (name or "") for name in imports))
        self.assertNotIn("configured_batch(",source)
        self.assertNotIn("Exercise(",source)
        self.assertNotIn('project="ats-core"',source)


if __name__=="__main__":unittest.main()
