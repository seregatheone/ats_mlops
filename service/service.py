"""Bento HTTP service. No ats_mlops import is permitted before artifact load."""

import os

import bentoml

from service.contracts import BoundaryError
from service.events import EventStore
from service.http_boundary import HTTPBoundary
from service.runtime import Runtime


@bentoml.service(workers=1, logging={"access": {"enabled": False}}, metrics={"enabled": False})
class RankingService:
    def __init__(self):
        self.runtime = self.events = None
        try:
            self.runtime = Runtime(os.environ["ATS_RELEASE_DIR"])
        except Exception:
            pass  # Never expose an artifact/ML framework exception or its inputs.
        try:
            self.events = EventStore(os.environ["ATS_EVENT_DB"])
        except Exception:
            pass

    @bentoml.api(route="/rank")
    def rank(self, payload: dict, /, ctx: bentoml.Context) -> dict:
        try:
            if self.events is None:
                raise ValueError("event_store_unavailable")

            def produce(request):
                if self.runtime is None:
                    raise ValueError("model_unavailable")
                return self.runtime.predict(request)

            identity = ({key: self.runtime.manifest["candidate"][key] for key in ("model_version",)}
                        | {"release_id": self.runtime.manifest["release_id"]}) if self.runtime else {}
            return self.events.predict(payload, produce, identity)
        except BoundaryError as error:
            ctx.response.status_code = error.status
            return {"error": error.code}
        except Exception:
            ctx.response.status_code = 503
            return {"error": "prediction_unavailable"}

    @bentoml.api(route="/metadata")
    def metadata(self) -> dict:
        if self.runtime is None:
            return {"ready": False, "error": "model_unavailable"}
        manifest = self.runtime.manifest
        return {"ready": self.events is not None, "model_version": manifest["candidate"]["model_version"],
                "release_id": manifest["release_id"], "bento_tag": manifest["bento_tag"],
                "model_sha256": manifest["candidate"]["model_sha256"],
                "source_tree_sha256": manifest["candidate"]["source_tree_sha256"],
                "release_manifest_sha256": self.runtime.manifest_sha,
                "api_schema_sha256": manifest["api_schema_sha256"], "evidence_scope": manifest["evidence_scope"],
                "serving_image_id": manifest["serving"]["image"]["id"], "runtime": self.runtime.receipt}


RankingService.add_asgi_middleware(HTTPBoundary)
