"""Reject untrusted input before Bento's validation/traceback logger sees it."""

from service.contracts import BoundaryError, MAX_BODY, canonical, decode, rank_request


class HTTPBoundary:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") not in ("/rank", "/metadata"):
            return await self.app(scope, receive, send)
        try:
            if scope["method"] != "POST":
                raise BoundaryError(405, "method_not_allowed")
            headers = dict(scope.get("headers", []))
            if headers.get(b"content-encoding", b"identity") != b"identity":
                raise BoundaryError()
            if headers.get(b"content-type", b"application/json").split(b";")[0].strip() != b"application/json":
                raise BoundaryError()
            content = bytearray()
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    return
                content.extend(message.get("body", b""))
                if len(content) > MAX_BODY:
                    raise BoundaryError(413, "request_too_large")
                if not message.get("more_body", False):
                    break
            value = decode(bytes(content) or b"{}")
            if scope["path"] == "/rank":
                rank_request(value)
            elif value != {}:
                raise BoundaryError()
            body = canonical(value)
            # Only validated canonical JSON reaches the SDK. No content-length mismatch.
            scope = dict(scope, headers=[(k, v) for k, v in scope.get("headers", []) if k != b"content-length"]
                         + [(b"content-length", str(len(body)).encode())])
            consumed = False

            async def replay():
                nonlocal consumed
                if not consumed:
                    consumed = True
                    return {"type": "http.request", "body": body, "more_body": False}
                return await receive()

            return await self.app(scope, replay, send)
        except BoundaryError as error:
            body = canonical({"error": error.code})
            await send({"type": "http.response.start", "status": error.status,
                        "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
