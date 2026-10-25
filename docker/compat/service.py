"""Fixture-only serving without access to the training checkout."""

import os

import bentoml
import bentoml.mlflow
import pandas as pd


@bentoml.service(workers=1)
class CompatibilityService:
    def __init__(self):
        self.bento_tag = os.environ["ATS_COMPAT_BENTO_TAG"]
        if ":" not in self.bento_tag or self.bento_tag.endswith(":latest"):
            raise ValueError("An exact Bento model tag is required")
        self.model = bentoml.mlflow.load_model(self.bento_tag)

    @bentoml.api
    def predict(self, values: list[float]) -> list[float]:
        predictions = self.model.predict(pd.DataFrame({"x": values}))
        return [float(value) for value in predictions]

    @bentoml.api
    def metadata(self) -> dict[str, str]:
        return {"bento_tag": self.bento_tag}
