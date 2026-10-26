"""Load exact artifact first, then import its own feature/scoring helpers."""

import importlib.metadata
import importlib.util
import math
import os
from pathlib import Path
import platform
import sys

from service.contracts import require, sha
from service.integrity import inventory, release_binding


def environment():
    return {"python": platform.python_version(),
            "packages": {name: importlib.metadata.version(name) for name in
                         ("bentoml", "mlflow", "scikit-learn", "pandas", "numpy", "cloudpickle")},
            "lockfile_sha256": sha(Path("/opt/requirements-ml.lock").read_bytes()),
            "build_lockfile_sha256": sha(Path("/opt/requirements-build.lock").read_bytes())}


class Runtime:
    def __init__(self, root):
        require(sys.flags.isolated and sys.dont_write_bytecode)
        require(importlib.util.find_spec("ats_mlops") is None)
        require(not any(name == "ats_mlops" or name.startswith("ats_mlops.") for name in sys.modules))
        require(not any(os.environ.get(name) for name in ("MLFLOW_TRACKING_URI", "AWS_ACCESS_KEY_ID",
                                                        "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")))
        self.manifest, self.manifest_sha, self.record = release_binding(root)
        require(inventory(Path(__file__).parent) == self.manifest["serving"]["source_sha256"])
        require(environment() == self.manifest["serving"]["environment"])
        import bentoml.mlflow
        import yaml

        entry = bentoml.models.get(self.manifest["bento_tag"])
        require(str(entry.tag) == self.manifest["bento_tag"])
        artifact = Path(entry.path_of("mlflow_model"))
        expected_store = Path(os.environ["BENTOML_HOME"]) / "models" / self.manifest["bento_tag"].replace(":", "/") / "mlflow_model"
        require(artifact.resolve() == expected_store.resolve()
                and inventory(artifact) == self.record["model"]["files"])
        metadata = yaml.safe_load((artifact / "MLmodel").read_text())
        pyfunc = metadata["flavors"]["python_function"]
        require(pyfunc["code"] == "code" and pyfunc["model_path"] == "model.pkl"
                and pyfunc["loader_module"] == "mlflow.sklearn" and pyfunc["predict_fn"] == "predict_proba")
        require(metadata["run_id"] == self.record["mlflow"]["run_id"]
                and self.record["mlflow"]["source"] == "models:/" + metadata["model_id"])
        registered = yaml.safe_load((artifact / "registered_model_meta").read_text())
        require(registered["model_name"] == self.record["mlflow"]["model_name"]
                and str(registered["model_version"]) == self.record["mlflow"]["model_version"])
        self.model = bentoml.mlflow.load_model(entry)
        from ats_mlops.model.scoring import pair_frame, positive_scores
        from ats_mlops.features.pairs import FEATURE_VERSION, FEATURE_NAMES

        self.frame, self.positive_scores = pair_frame, positive_scores
        self.raw = self.model.get_raw_model()
        self.transformer = self.raw.named_steps["pair_features"]
        require(list(self.raw.classes_) == [0, 1] and FEATURE_VERSION == self.record["features"]["version"]
                and list(FEATURE_NAMES) == self.record["features"]["names"]
                and self.transformer.feature_version_ == FEATURE_VERSION)
        self.names = list(FEATURE_NAMES)
        for name, module in list(sys.modules.items()):
            if name == "ats_mlops" or name.startswith("ats_mlops."):
                require(Path(module.__file__).resolve().is_relative_to(artifact.resolve() / "code"))
        self.receipt = {"isolated": True, "bytecode_disabled": True, "prior_project_import": False,
                        "training_checkout_mounted": False, "credentials": False, "environment": environment()}

    def predict(self, request):
        if not request["candidates"]:
            return []
        rows = [{"vacancy_text": request["vacancy"]["text"], "candidate_text": item["text"]}
                for item in request["candidates"]]
        frame = self.frame(rows)
        scores = self.positive_scores(self.model.predict(frame), self.raw.classes_, expected_rows=len(rows))
        features = self.transformer.transform(frame)
        require(features.shape == (len(rows), len(self.names)))
        result = []
        for item, score, values in zip(request["candidates"], scores, features, strict=True):
            numeric = dict(zip(self.names, map(float, values), strict=True))
            require(all(math.isfinite(value) for value in numeric.values()))
            result.append({"candidate_id": item["id"], "score": float(score), "features": numeric})
        return sorted(result, key=lambda item: (-item["score"], item["candidate_id"]))
