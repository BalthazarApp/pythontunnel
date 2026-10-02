"""A single local HTTP server faking the three things an app-tunnel login touches.

Not a test module (leading underscore): a helper shared by ``test_app_auth.py`` and
``test_shim_app_transport.py``. One ``ThreadingHTTPServer`` answers:

* ``GET  /api/frontend/config`` — site discovery (``oidcAuthority`` / ``appTunnelOrigin``);
* ``POST {authority}/protocol/openid-connect/auth/device`` + ``.../token`` — a tiny
  Keycloak: password, refresh-token and device-code (fast interval) grants;
* ``POST {site}/api/app-tunnel/grant-access/{runner}/{flow}`` — issues the
  ``blt_tunnel_<runner>_<flow>`` cookie;
* ``POST /app-tunnel/{runner}/{flow}/rpc`` — the tunnelled RPC endpoint.

It runs over plain ``http`` on ``127.0.0.1``; tests set ``BALTHAZAR_TUNNEL_ALLOW_HTTP=1``
so ``_app_auth._Http`` will speak to it. No real browser is ever opened.
"""

from __future__ import annotations

import json
import threading
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"


class RpcError:
    """Return this from a responder to make ``/rpc`` reply ``{ok: false, error}``."""

    def __init__(self, type, message, traceback=None):
        self.type = type
        self.message = message
        self.traceback = traceback


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # keep test output clean
        return

    @property
    def fake(self):
        return self.server.fake

    def _send(self, status, obj=None, headers=None, raw=None):
        body = raw if raw is not None else json.dumps(obj if obj is not None else {}).encode()
        self.send_response(status)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length)

    def do_GET(self):
        fake = self.fake
        if urllib.parse.urlsplit(self.path).path == "/api/frontend/config":
            return self._send(200, {"oidcAuthority": fake.authority, "appTunnelOrigin": fake.origin})
        return self._send(404, {"error": "not found"})

    def do_POST(self):
        fake = self.fake
        path = urllib.parse.urlsplit(self.path).path
        data = self._body()
        if path.endswith("/protocol/openid-connect/auth/device"):
            return self._send(
                200,
                {
                    "device_code": "DEV-CODE",
                    "user_code": "WXYZ",
                    "verification_uri": fake.origin + "/device",
                    "interval": 0.01,
                    "expires_in": 5,
                },
            )
        if path.endswith("/protocol/openid-connect/token"):
            return self._token(data)
        if path.startswith("/api/app-tunnel/grant-access/"):
            return self._grant()
        if path == "/app-tunnel/%s/%s/rpc" % (fake.runner, fake.flow):
            return self._rpc(data)
        return self._send(404, {"error": "not found"})

    def _token(self, data):
        fake = self.fake
        form = {k: v[0] for k, v in urllib.parse.parse_qs(data.decode()).items()}
        grant = form.get("grant_type")
        fake.token_grants.append(grant)
        if grant == "refresh_token":
            if form.get("refresh_token") in fake.valid_refresh:
                return self._send(200, fake.issue())
            return self._send(400, {"error": "invalid_grant", "error_description": "stale refresh token"})
        if grant == "password":
            if form.get("username") == fake.username and form.get("password") == fake.password:
                return self._send(200, fake.issue())
            return self._send(401, {"error": "invalid_grant", "error_description": "bad credentials"})
        if grant == DEVICE_GRANT:
            if fake.device_pending > 0:
                fake.device_pending -= 1
                return self._send(400, {"error": "authorization_pending"})
            return self._send(200, fake.issue())
        if grant == "authorization_code":
            return self._send(200, fake.issue())
        return self._send(400, {"error": "unsupported_grant_type"})

    def _grant(self):
        fake = self.fake
        if not (self.headers.get("Authorization") or "").startswith("Bearer "):
            return self._send(401, {"error": "no bearer token"})
        if not self.headers.get("X-BLT-Space-Id"):
            return self._send(400, {"error": "missing space id"})
        fake.grant_count += 1
        cookie = "blt_tunnel_%s_%s=GRANT-%d; Path=/; HttpOnly" % (fake.runner, fake.flow, fake.grant_count)
        return self._send(200, {"granted": True}, headers={"Set-Cookie": cookie})

    def _rpc(self, data):
        fake = self.fake
        if fake.rpc_http_status is not None:
            return self._send(fake.rpc_http_status, raw=b"upstream said no")
        if fake.fail_401 > 0:
            fake.fail_401 -= 1
            return self._send(401, {"error": "stale cookie"})
        cookie = self.headers.get("Cookie") or ""
        if fake.require_cookie and not cookie.startswith("blt_tunnel_"):
            return self._send(401, {"error": "missing cookie"})
        if self.headers.get("Authorization"):
            # The proxy strips Authorization; the client must never send it here.
            return self._send(400, {"error": "authorization header must not reach the app listener"})
        payload = json.loads(data.decode())
        op, kwargs = payload["op"], payload.get("kwargs") or {}
        fake.rpc_calls.append((op, kwargs, cookie))
        result = fake.responder(op, kwargs)
        if isinstance(result, RpcError):
            return self._send(
                200,
                {"ok": False, "error": {"type": result.type, "message": result.message, "traceback": result.traceback}},
            )
        return self._send(200, {"ok": True, "result": result})


class FakeServer:
    """A running fake Balthazar. ``stop()`` when done (or use the pytest fixture)."""

    def __init__(self):
        self.runner = str(uuid.uuid4())
        self.flow = str(uuid.uuid4())
        self.space_id = "space-1"
        self.context_id = "ctx-1"
        self.user = "user-1"
        self.username = "alice"
        self.password = "s3cret"
        self.valid_refresh: set[str] = set()
        self._issued = 0
        self.device_pending = 0
        self.grant_count = 0
        self.token_grants: list[str] = []
        self.rpc_calls: list[tuple] = []
        self.require_cookie = True
        self.rpc_http_status = None
        self.fail_401 = 0
        self.responder = self.default_responder

        self._httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._httpd.fake = self
        self.port = self._httpd.server_address[1]
        self.origin = "http://127.0.0.1:%d" % self.port
        self.authority = self.origin + "/auth/realms/balthazar"
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()

    def issue(self):
        self._issued += 1
        refresh = "refresh-%d" % self._issued
        self.valid_refresh = {refresh}  # a fresh refresh token supersedes the old
        return {
            "access_token": "access-%d" % self._issued,
            "refresh_token": refresh,
            "token_type": "Bearer",
            "expires_in": 300,
        }

    def cache_key(self, client_id="blt-frontend2"):
        return "%s|%s" % (self.authority, client_id)

    def app_url(self):
        return "%s/app-tunnel/%s/%s/?space_id=%s&context_id=%s" % (
            self.origin,
            self.runner,
            self.flow,
            self.space_id,
            self.context_id,
        )

    def default_responder(self, op, kwargs):
        if op == "ping":
            return {
                "transport": "app",
                "user": self.user,  # the server reports the caller as ``user``
                "flow_run_id": "run-1",
                "flow_name": "IV sweep",
                "flow_id": "flow-1",
                "session_id": "sess-1",
                "depth": 0,
                "stack": [],
                "device_indexes": {},
            }
        return {"op": op, "kwargs": kwargs}

    def stop(self):
        self._httpd.shutdown()
        self._httpd.server_close()
