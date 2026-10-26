"""Controlled negative artifact code only; no ML libraries/load/log/fit."""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import ast
import unittest

from ats_mlops.evaluation.negative_model import invert_coefficients


class Vector:
    def __init__(self,values,shape):self.values=values;self.shape=shape
    def __neg__(self):return Vector([-value for value in self.values],self.shape)


class NegativeFixtureTests(unittest.TestCase):
    def model(self):
        classifier=SimpleNamespace(classes_=SimpleNamespace(tolist=lambda:[0,1]),coef_=Vector([.4,-.2],(1,2)),intercept_=Vector([.3],(1,)))
        return SimpleNamespace(named_steps={'classifier':classifier,'pair_features':object()})

    def test_only_declared_fitted_decision_coordinates_change_without_fit(self):
        model=self.model();feature=model.named_steps['pair_features'];classifier=model.named_steps['classifier']
        self.assertIs(model,invert_coefficients(model))
        self.assertIs(feature,model.named_steps['pair_features'])
        self.assertEqual([-.4,.2],classifier.coef_.values);self.assertEqual([-.3],classifier.intercept_.values)

    def test_nonbinary_shape_rejects_before_mutation(self):
        model=self.model();classifier=model.named_steps['classifier'];classifier.coef_.shape=(2,2)
        with self.assertRaises(ValueError):invert_coefficients(model)
        self.assertEqual([.4,-.2],classifier.coef_.values)

    def test_module_has_no_eager_ML_import_or_fit_or_test_partition_access(self):
        import ats_mlops.evaluation.negative_model as module
        source=Path(module.__file__).read_text();tree=ast.parse(source)
        imports=[node.module or '' for node in tree.body if isinstance(node,ast.ImportFrom)]
        imports += [item.name for node in tree.body if isinstance(node,ast.Import) for item in node.names]
        self.assertFalse(any(name.startswith(('mlflow','numpy','sklearn','ats_mlops')) for name in imports))
        self.assertNotIn('.fit(',source);self.assertNotIn('final_evaluation=True',source)
        self.assertLess(source.index('write_json(output/"fault-intent.json"'),source.index('model=invert_coefficients'))
        self.assertIn('controlled_negative_artifact_fault',source)

if __name__=='__main__':unittest.main()
