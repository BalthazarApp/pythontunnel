"""Tests for the stdlib-only app-tunnel login client ``session_tunnel/_app_auth.py``.

They stand up one local fake (``_app_tunnel_fake.FakeServer``) that plays Keycloak,
the grant-access endpoint and the ``/rpc`` tunnel over plain HTTP, and drive
``AppSession`` directly: the three login modes, refresh-token reuse from the cache,
cookie grant/re-grant on 401, ``forget``, and the non-interactive guard. No real
network, no browser.
"""

from __future__ import annotations

import importlib.util
import json
import os

import pytest

from _app_tunnel_fake import FakeServer

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
APP_AUTH_PATH = os.path.join(REPO, "session_tunnel", "_app_auth.py")


@pytest.fixture
def app_auth(tmp_path, monkeypatch):
    """Load ``_app_auth`` fresh with HTTP allowed and the token cache in a tmp dir."""
    spec = importlib.util.spec_from_file_location("app_auth_under_test", APP_AUTH_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module._ALLOW_INSECURE_HTTP = True
    monkeypatch.setattr(module, "TOKEN_CACHE", tmp_path / "remote.json")
    return module


@pytest.fixture
def server():
    s = FakeServer()
    try:
        yield s
    finally:
        s.stop()


def _rpc(session, op="ping", **kwargs):
    status, body = session.post_json("/rpc", json.dumps({"op": op, "kwargs": kwargs}).encode(), 10)
    return status, json.loads(body.decode())


# ---------------------------------------------------------------------------
# login modes
# ---------------------------------------------------------------------------


def test_password_login_then_rpc(app_auth, server):
    session = app_auth.AppSession(
        server.app_url(), login="password", username=server.username, password=server.password
    )
    status, reply = _rpc(session)
    assert status == 200 and reply["ok"] is True
    assert reply["result"]["user"] == server.user
    assert server.token_grants == ["password"]
    assert server.grant_count == 1  # one grant-access -> one cookie


def test_bad_password_raises(app_auth, server):
    session = app_auth.AppSession(
        server.app_url(), login="password", username=server.username, password="wrong"
    )
    with pytest.raises(app_auth.AppAuthError):
        _rpc(session)


def test_device_login_polls_until_authorized(app_auth, server):
    server.device_pending = 1  # one authorization_pending, then success
    session = app_auth.AppSession(server.app_url(), login="device")
    status, reply = _rpc(session)
    assert status == 200 and reply["ok"] is True
    # Every poll is a device_code token request; one pending + one success.
    from _app_tunnel_fake import DEVICE_GRANT

    assert server.token_grants == [DEVICE_GRANT, DEVICE_GRANT]


def test_refresh_token_reused_from_cache(app_auth, server):
    # Seed the cache with a refresh token the server will honour.
    app_auth.TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    app_auth.TOKEN_CACHE.write_text(json.dumps({server.cache_key(): "seed-refresh"}))
    server.valid_refresh = {"seed-refresh"}

    session = app_auth.AppSession(server.app_url(), login="device")
    status, reply = _rpc(session)
    assert status == 200 and reply["ok"] is True
    # The cached refresh token is used; no device login is started.
    assert server.token_grants == ["refresh_token"]


def test_refresh_token_cached_after_login(app_auth, server):
    session = app_auth.AppSession(server.app_url(), login="device")
    _rpc(session)
    cached = json.loads(app_auth.TOKEN_CACHE.read_text())
    assert server.cache_key() in cached
    assert cached[server.cache_key()] in server.valid_refresh


def test_password_login_is_not_remembered(app_auth, server):
    session = app_auth.AppSession(
        server.app_url(), login="password", username=server.username, password=server.password
    )
    _rpc(session)
    assert not app_auth.TOKEN_CACHE.exists()  # password login never writes the cache


# ---------------------------------------------------------------------------
# cookie re-grant on 401
# ---------------------------------------------------------------------------


def test_cookie_regranted_on_401(app_auth, server):
    server.fail_401 = 1  # first /rpc POST 401s; post_json regrants and retries
    session = app_auth.AppSession(
        server.app_url(), login="password", username=server.username, password=server.password
    )
    status, reply = _rpc(session)
    assert status == 200 and reply["ok"] is True
    assert server.grant_count == 2  # initial cookie + one re-grant


# ---------------------------------------------------------------------------
# site discovery
# ---------------------------------------------------------------------------


def test_explicit_site_skips_discovery(app_auth, server):
    session = app_auth.AppSession(
        server.app_url(), site=server.origin, login="password",
        username=server.username, password=server.password,
    )
    status, _ = _rpc(session)
    assert status == 200


def test_no_authorization_header_reaches_rpc(app_auth, server):
    # The fake /rpc 400s if an Authorization header arrives (the proxy strips it).
    session = app_auth.AppSession(
        server.app_url(), login="password", username=server.username, password=server.password
    )
    status, reply = _rpc(session)
    assert status == 200 and reply["ok"] is True


# ---------------------------------------------------------------------------
# forget + non-interactive guard
# ---------------------------------------------------------------------------


def test_forget_removes_cached_token_offline(app_auth, server):
    app_auth.TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    app_auth.TOKEN_CACHE.write_text(json.dumps({server.cache_key(): "seed", "other|x": "keep"}))
    # No network: site not discovered, so forget drops entries by client_id suffix.
    session = app_auth.AppSession(server.app_url(), login="device")
    session.forget()
    remaining = json.loads(app_auth.TOKEN_CACHE.read_text())
    assert server.cache_key() not in remaining
    assert remaining == {"other|x": "keep"}  # other client ids are left alone


def test_non_interactive_raises_login_required(app_auth, server):
    session = app_auth.AppSession(server.app_url(), login="device", interactive=False)
    with pytest.raises(app_auth.LoginRequired) as excinfo:
        _rpc(session)
    assert "blt-tunnel connect" in str(excinfo.value)
    assert server.token_grants == []  # no login attempt was made


def test_non_interactive_still_uses_cached_refresh(app_auth, server):
    app_auth.TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    app_auth.TOKEN_CACHE.write_text(json.dumps({server.cache_key(): "seed-refresh"}))
    server.valid_refresh = {"seed-refresh"}
    session = app_auth.AppSession(server.app_url(), login="device", interactive=False)
    status, reply = _rpc(session)
    assert status == 200 and reply["ok"] is True
    assert server.token_grants == ["refresh_token"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def test_bad_app_url_rejected(app_auth, server):
    with pytest.raises(app_auth.AppAuthError):
        app_auth.AppSession("https://host/not-a-tunnel")


def test_missing_space_id_rejected(app_auth, server):
    url = "%s/app-tunnel/%s/%s/" % (server.origin, server.runner, server.flow)
    with pytest.raises(app_auth.AppAuthError):
        app_auth.AppSession(url)


def test_site_candidates_and_origin_key(app_auth):
    assert app_auth._origin_key("https://h.example:443") == app_auth._origin_key("https://h.example")
    cands = app_auth._site_candidates("https://app-tunnel.acme.dev")
    assert "https://core.acme.dev" in cands


def test_tunnel_failure_messages(app_auth):
    assert "not running" in str(app_auth._tunnel_failure(404, b"", "x"))
    assert "60 s" in str(app_auth._tunnel_failure(504, b"", "x"))
