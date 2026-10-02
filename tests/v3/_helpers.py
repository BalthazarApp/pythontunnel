"""Shared helpers for the v3 bridge tests (imported by the tests and the conftest).

``Client`` is a tiny stdlib HTTP caller that drives the bridge's ``/call`` endpoint
directly: it sends ``X-BLT-User-Id`` itself (the auth-bypassed transport the spec
asks for), always opts into the parts protocol, and reassembles a split download so
a test sees one decoded reply.
"""

from __future__ import annotations

import importlib.util
import json
import os
import urllib.error
import urllib.request
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
BRIDGE_PATH = os.path.join(REPO, "flows", "tunnel_bridge.py")
REMOTE_CLIENT_PATH = os.path.join(REPO, "remoteblt4", "balthazar_remote.py")

OWNER = "owner-user"
_UNSET = object()


class Client:
    """A direct HTTP caller for the bridge's ``/call`` endpoint."""

    def __init__(self, base_url: str, user: str = OWNER):
        self.base = base_url
        self.user = user

    def send(self, body: bytes, headers: dict | None = None, user=_UNSET):
        who = self.user if user is _UNSET else user
        req = urllib.request.Request(self.base + "/call", data=body, method="POST")
        if who is not None:
            req.add_header("X-BLT-User-Id", who)
        req.add_header("Content-Type", "application/json")
        for key, value in (headers or {}).items():
            req.add_header(key, value)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as error:
            return error.code, dict(error.headers), error.read()

    def get(self, path="/", user=_UNSET, headers=None):
        who = self.user if user is _UNSET else user
        req = urllib.request.Request(self.base + path, method="GET")
        if who is not None:
            req.add_header("X-BLT-User-Id", who)
        for key, value in (headers or {}).items():
            req.add_header(key, value)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, resp.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def post(self, payload: dict, user=_UNSET, headers=None):
        """POST one request; return ``(status, headers, raw_bytes)`` verbatim."""
        merged = {"X-Bridge-Parts": "1"}
        merged.update(headers or {})
        return self.send(json.dumps(payload).encode(), merged, user=user)

    def op(self, payload: dict, user=_UNSET, headers=None):
        """POST a request and return the decoded reply, following the parts protocol."""
        status, _, body = self.post(payload, user=user, headers=headers)
        reply = json.loads(body)
        if reply.get("parts"):
            reply = json.loads(self._receive(reply["parts"], user=user))
        return reply

    def _receive(self, parts, user=_UNSET):
        chunks = []
        for index in range(parts["count"]):
            request = json.dumps({"op": "part", "id": parts["id"], "index": index}).encode()
            status, _, chunk = self.send(request, {"X-Bridge-Parts": "1"}, user=user)
            assert status == 200, (status, chunk)
            chunks.append(chunk)
        return zlib.decompress(b"".join(chunks))

    def tunnel(self, name: str, user=_UNSET, **kwargs):
        reply = self.op({"op": "tunnel", "name": name, "args": [], "kwargs": kwargs}, user=user)
        assert reply.get("ok"), reply
        return reply["result"]


def load_remote_client():
    """Import ``remoteblt4/balthazar_remote.py`` by path (the wire-compat client)."""
    spec = importlib.util.spec_from_file_location("remoteblt4_client", REMOTE_CLIENT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
