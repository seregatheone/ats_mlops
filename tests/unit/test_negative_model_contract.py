"""Real MLflow metadata roundtrip against the existing isolated consumer.

The estimator is intentionally unfitted: this boundary test serializes only,
never fits, deserializes, scores, calls a service or reads a corpus partition.
"""
import ast
from copy import deepcopy
import hashlib
from pathlib import Path
import tempfile
import unittest

from ats_mlops.evaluation import negative_model
from ats_mlops.quality.isolated import BOOTSTRAP


def metadata_check():
    for node in ast.walk(ast.parse(BOOTSTRAP)):
        if (isinstance(node,ast.Compare) and isinstance(node.left,ast.Subscript)
                and isinstance(node.left.value,ast.Name) and node.left.value.id=="metadata"
                and isinstance(node.left.slice,ast.Constant) and node.left.slice.value=="metadata"
                and len(node.comparators)==1 and isinstance(node.comparators[0],ast.Dict)):
            return compile(ast.Expression(body=node),"isolated_metadata_contract","eval")
    raise AssertionError("real_isolated_metadata_contract_missing")


class NegativeMetadataContractTests(unittest.TestCase):
    def test_actual_mlflow_metadata_matches_strict_consumer_and_extra_origin_fails(self):
        import mlflow
        import mlflow.sklearn
        import sklearn
        from sklearn.linear_model import LogisticRegression
        import yaml

        record={"features":{"version":"contract-feature-v1"},"features_sha256":"2"*64,"git_sha":"1"*40}
        expression=metadata_check();original_uri=mlflow.get_tracking_uri()
        self.assertIsNone(mlflow.active_run())
        try:
            with tempfile.TemporaryDirectory(prefix="ats-negative-metadata-contract-") as directory:
                root=Path(directory)
                mlflow.set_tracking_uri("sqlite:///"+str(root/"tracking.sqlite3"))
                experiment=mlflow.MlflowClient().create_experiment("negative-metadata-contract",artifact_location=(root/"artifacts").as_uri())
                with mlflow.start_run(experiment_id=experiment):
                    for name,extra,expected in (("strict",{},True),("historical-extra",{"artifact_origin":"controlled_negative_artifact_fault"},False)):
                        info=mlflow.sklearn.log_model(LogisticRegression(),name=name,
                            pip_requirements=["scikit-learn=="+sklearn.__version__],
                            metadata={**negative_model.model_metadata(record,record["git_sha"]),**extra})
                        local=Path(mlflow.artifacts.download_artifacts(artifact_uri=info.model_uri,dst_path=str(root/name)))
                        metadata=yaml.safe_load((local/"MLmodel").read_text())
                        self.assertEqual(expected,eval(expression,{"__builtins__":{}},{"metadata":metadata,"record":record}))
                        self.assertEqual(set(negative_model.model_metadata(record,record["git_sha"]))|set(extra),set(metadata["metadata"]))
        finally:
            mlflow.set_tracking_uri(original_uri)

    def test_external_producer_identity_binds_executed_bytes_without_changing_derivation(self):
        identity={"schema_version":1,"source_git_sha":"3"*40,
            "repository_path":"src/ats_mlops/evaluation/negative_model.py",
            "file_sha256":hashlib.sha256(Path(negative_model.__file__).read_bytes()).hexdigest()}
        actual=negative_model.checked_producer_identity(identity)
        self.assertEqual(identity,actual);self.assertIsNot(identity,actual)
        self.assertIsNone(negative_model.checked_producer_identity(None))
        for key,value in (("schema_version",True),("source_git_sha","not-a-Git-SHA"),
                          ("repository_path","other.py"),("file_sha256","0"*64)):
            broken=deepcopy(identity);broken[key]=value
            with self.subTest(key=key),self.assertRaisesRegex(ValueError,"producer_identity_mismatch"):
                negative_model.checked_producer_identity(broken)


if __name__=="__main__":unittest.main()
