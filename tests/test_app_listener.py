"""Tests for the v2 tunnel's app-tunnel listener and the loopback/app split (spec §7).

Unlike ``test_server_ops`` (which calls ``_op_*`` directly), these exercise the HTTP
layer over **real sockets** on ``127.0.0.1`` ephemeral ports, because the auth policy,
the browser rejection, the ``Host`` handling and the ``GET /`` page all live in the
request handlers, not the ops. The ``fake_blt`` fixture installs the fake *real*
balthazar; each test imports the flow module fresh so its ``_TOKEN``, ``_JOBS`` and
``_allowed_users`` start clean.

The executor thread is started only for the handful of tests that need an op to
actually run (the fast ``_DISPATCH`` ops hand work to it). Auth rejections, the GET
page and worker-dispatch ops (``space_schema``) need no executor, so they don't start
one.
"""

from __future__ import annotations

import contextlib
import http.client
import importlib.util
import json
import os
import threading
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
FLOW_PATH = os.path.join(REPO, "flows", "tunnel_session_server.py")


@pytest.fixture
def server(fake_blt):
    spec = importlib.util.spec_from_file_location("tunnel_app_under_test", FLOW_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _spin_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


@contextlib.contextmanager
def _running_listener(server, handler_cls, *, with_executor=False):
    """Start ``handler_cls`` on an ephemeral loopback port; yield the port. Optionally
    run the main-thread executor so ``_DISPATCH`` ops can be served."""
    executor = None
    if with_executor:
        executor = threading.Thread(target=server._run_executor, name="exec", daemon=True)
        executor.start()
        assert _spin_until(server._executor_live.is_set)

    httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    httpd.daemon_threads = True
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, name="http", daemon=True).start()
    try:
        yield port
    finally:
        httpd.shutdown()
        httpd.server_close()
        if executor is not None:
            server._stop.set()
            executor.join(timeout=5.0)


def _post(port, op, *, kwargs=None, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    body = json.dumps({"op": op, "kwargs": kwargs or {}})
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    conn.request("POST", "/rpc", body=body, headers=hdrs)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def _get(port, path="/", *, headers=None):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", path, headers=headers or {})
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


def _post_with_host(port, op, *, host, headers=None):
    """POST with an explicit (possibly non-loopback) Host header, to prove the app
    listener does no Host check and the loopback one still does."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    body = json.dumps({"op": op, "kwargs": {}}).encode("utf-8")
    conn.putrequest("POST", "/rpc", skip_host=True, skip_accept_encoding=True)
    conn.putheader("Host", host)
    conn.putheader("Content-Type", "application/json")
    conn.putheader("Content-Length", str(len(body)))
    for key, value in (headers or {}).items():
        conn.putheader(key, value)
    conn.endheaders()
    conn.send(body)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data


# ---------------------------------------------------------------------------
# App listener auth matrix (spec §7)
# ---------------------------------------------------------------------------


def test_app_owner_authorized(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    with _running_listener(server, server._AppHandler) as port:
        # Case-insensitive match against blt.user. space_schema is a worker op, so it
        # runs without the executor.
        status, data = _post(port, "space_schema", headers={"X-BLT-User-Id": "OWNER-1"})
    assert status == 200
    assert json.loads(data)["ok"] is True


def test_app_other_user_forbidden(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    with _running_listener(server, server._AppHandler) as port:
        status, data = _post(port, "space_schema", headers={"X-BLT-User-Id": "stranger"})
    assert status == 403
    assert json.loads(data)["ok"] is False


def test_app_allowed_user_authorized(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    server._allowed_users = {"friend"}  # stored lowercased by _parse_allowed_users
    with _running_listener(server, server._AppHandler) as port:
        status, data = _post(port, "space_schema", headers={"X-BLT-User-Id": "FRIEND"})
    assert status == 200
    assert json.loads(data)["ok"] is True


def test_app_wildcard_allows_any_user(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    server._allow_all_users = True
    with _running_listener(server, server._AppHandler) as port:
        status, data = _post(port, "space_schema", headers={"X-BLT-User-Id": "anyone-999"})
    assert status == 200
    assert json.loads(data)["ok"] is True


def test_app_missing_user_header_forbidden(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    with _running_listener(server, server._AppHandler) as port:
        status, _ = _post(port, "space_schema")  # no X-BLT-User-Id
    assert status == 403


@pytest.mark.parametrize(
    "browser_header",
    [{"Origin": "https://evil.example"}, {"Sec-Fetch-Site": "cross-site"}],
)
def test_app_rejects_browser_post_even_for_owner(server, monkeypatch, browser_header):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    headers = {"X-BLT-User-Id": "owner-1", **browser_header}
    with _running_listener(server, server._AppHandler) as port:
        status, data = _post(port, "space_schema", headers=headers)
    assert status == 403
    assert "browser" in json.loads(data)["error"]["message"].lower()


def test_app_does_no_host_check(server, monkeypatch):
    # The proxy always presents Host: 127.0.0.1:port; a non-loopback Host still passes.
    monkeypatch.setattr(server.blt, "user", "owner-1")
    with _running_listener(server, server._AppHandler) as port:
        status, data = _post_with_host(
            port, "space_schema", host="tunnel.balthazar.app",
            headers={"X-BLT-User-Id": "owner-1"},
        )
    assert status == 200
    assert json.loads(data)["ok"] is True


# ---------------------------------------------------------------------------
# GET / connection snippet page: owner only (spec §7)
# ---------------------------------------------------------------------------


def test_app_get_root_owner_serves_snippet_page(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    with _running_listener(server, server._AppHandler) as port:
        status, data = _get(port, "/", headers={"X-BLT-User-Id": "owner-1"})
    assert status == 200
    text = data.decode("utf-8")
    assert "blt-tunnel connect" in text
    assert "window.location.href" in text  # snippet assembled client-side
    assert "copy" in text.lower()


def test_app_get_root_non_owner_forbidden(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    with _running_listener(server, server._AppHandler) as port:
        status, _ = _get(port, "/", headers={"X-BLT-User-Id": "stranger"})
        # Allowed (non-owner) users do NOT get the page — owner only.
        server._allowed_users = {"stranger"}
        status2, _ = _get(port, "/", headers={"X-BLT-User-Id": "stranger"})
    assert status == 403
    assert status2 == 403


def test_app_get_unknown_path_404(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    with _running_listener(server, server._AppHandler) as port:
        status, _ = _get(port, "/rpc", headers={"X-BLT-User-Id": "owner-1"})
    assert status == 404


def test_app_ping_reports_app_transport_and_user(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-xyz")
    # ping is a _DISPATCH op, so it needs the executor.
    with _running_listener(server, server._AppHandler, with_executor=True) as port:
        status, data = _post(port, "ping", headers={"X-BLT-User-Id": "OWNER-XYZ"})
    assert status == 200
    result = json.loads(data)["result"]
    assert result["transport"] == "app"
    assert result["user"] == "OWNER-XYZ"  # the raw header value is echoed back


# ---------------------------------------------------------------------------
# serve_app wiring (spec §7)
# ---------------------------------------------------------------------------


def test_serve_app_called_with_bound_port(server, monkeypatch):
    monkeypatch.setattr(server.blt, "user", "owner-1")
    httpd = server._start_app_listener()
    try:
        assert httpd is not None
        calls = server.blt.serve_app_calls()
        assert len(calls) == 1
        assert calls[0] == httpd.server_address[1]  # the ephemeral port it bound
    finally:
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()


def test_serve_app_failure_logs_and_keeps_loopback(server, monkeypatch):
    def boom(port):
        raise RuntimeError("serve_app is not available on this Runner")

    monkeypatch.setattr(server.blt, "serve_app", boom)
    httpd = server._start_app_listener()
    assert httpd is None  # app listener not started...
    # ...and the failure was logged as a blt.error, so the flow can keep loopback up.
    assert any(
        level == "error" and "serve_app" in msg
        for level, msg in server.blt.logged_messages()
    )


def test_parse_allowed_users(server):
    assert server._parse_allowed_users("") == (set(), False)
    assert server._parse_allowed_users(None) == (set(), False)
    assert server._parse_allowed_users("Alice, bob") == ({"alice", "bob"}, False)
    ids, allow_all = server._parse_allowed_users("alice, *, bob")
    assert ids == {"alice", "bob"} and allow_all is True


# ---------------------------------------------------------------------------
# Loopback listener is unchanged: token auth, Host check, loopback transport
# ---------------------------------------------------------------------------


def test_loopback_ping_reports_loopback_transport(server):
    with _running_listener(server, server._LoopbackHandler, with_executor=True) as port:
        status, data = _post(
            port, "ping", headers={"Authorization": f"Bearer {server._TOKEN}"}
        )
    assert status == 200
    result = json.loads(data)["result"]
    assert result["transport"] == "loopback"
    assert result["user"] is None


def test_loopback_rejects_bad_token(server):
    with _running_listener(server, server._LoopbackHandler) as port:
        status, data = _post(port, "ping", headers={"Authorization": "Bearer nope"})
    assert status == 401
    assert json.loads(data)["error"]["type"] == "PermissionError"


def test_loopback_rejects_missing_token(server):
    with _running_listener(server, server._LoopbackHandler) as port:
        status, _ = _post(port, "ping")
    assert status == 401


def test_loopback_rejects_nonloopback_host(server):
    with _running_listener(server, server._LoopbackHandler) as port:
        status, _ = _post_with_host(
            port, "ping", host="evil.example.com",
            headers={"Authorization": f"Bearer {server._TOKEN}"},
        )
    assert status == 401  # the loopback listener still enforces the Host check
