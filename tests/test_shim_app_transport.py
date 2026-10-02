"""End-to-end tests for the v2 shim's **app transport** (SPEC §7).

They load ``session_tunnel/balthazar.py`` by path with ``$HOME`` pointed at a tmp dir
(so the connection profile and the refresh-token cache land there) and drive its real
``_call`` against the local ``_app_tunnel_fake.FakeServer`` — exercising the cookie
transport, transport selection/precedence, ``tunnel_connect``/``tunnel_disconnect``,
long-op polling, byte-budgeted page following, remote-error mapping, the 404/504
messages, numpy ``tolist()`` conversion, the non-interactive guard and the heartbeat.
The loopback transport is covered elsewhere and stays untouched.
"""

from __future__ import annotations

import importlib.util
import json
import os
import stat
import uuid

import pytest

from _app_tunnel_fake import FakeServer, RpcError

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SHIM_PATH = os.path.join(REPO, "session_tunnel", "balthazar.py")

_TUNNEL_ENVS = (
    "BALTHAZAR_SESSION_TUNNEL_URL",
    "BALTHAZAR_SESSION_TUNNEL_TOKEN",
    "BALTHAZAR_SESSION_TUNNEL_APP_URL",
    "BALTHAZAR_TUNNEL_NONINTERACTIVE",
    "BLT_TUNNEL_SHIM",
)


@pytest.fixture
def home(tmp_path, monkeypatch):
    place = tmp_path / "home"
    place.mkdir()
    monkeypatch.setenv("HOME", str(place))
    monkeypatch.setenv("BALTHAZAR_TUNNEL_ALLOW_HTTP", "1")
    for name in _TUNNEL_ENVS:
        monkeypatch.delenv(name, raising=False)
    return place


@pytest.fixture
def shim(home, monkeypatch):
    spec = importlib.util.spec_from_file_location("blt_shim_app_under_test", SHIM_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_POLL_INTERVAL_S", 0)  # no real 2 s sleeps in tests
    return module


@pytest.fixture
def server():
    s = FakeServer()
    try:
        yield s
    finally:
        s.stop()


def _write_profile(shim, profile):
    with open(shim.CONNECTION_FILE, "w", encoding="utf-8") as fh:
        json.dump(profile, fh)
    shim._reset_transport()


def _dummy_app_url():
    return "https://host.example/app-tunnel/%s/%s/?space_id=s1" % (uuid.uuid4(), uuid.uuid4())


def _connect_password(shim, server):
    return shim.tunnel_connect(
        server.app_url(), login="password", username=server.username, password=server.password
    )


# ---------------------------------------------------------------------------
# transport selection / precedence
# ---------------------------------------------------------------------------


def test_transport_precedence(shim, server, monkeypatch):
    # nothing configured
    assert shim.tunnel_transport() == "none"

    # loopback profile
    _write_profile(shim, {"url": "http://127.0.0.1:9/", "token": "T"})
    assert shim.tunnel_transport() == "loopback"

    # app profile beats nothing
    _write_profile(shim, {"transport": "app", "app_url": _dummy_app_url(), "login": "device"})
    assert shim.tunnel_transport() == "app"

    # env app url beats a loopback profile
    _write_profile(shim, {"url": "http://127.0.0.1:9/", "token": "T"})
    monkeypatch.setenv("BALTHAZAR_SESSION_TUNNEL_APP_URL", _dummy_app_url())
    shim._reset_transport()
    assert shim.tunnel_transport() == "app"
    monkeypatch.delenv("BALTHAZAR_SESSION_TUNNEL_APP_URL")

    # loopback env, no file
    os.remove(shim.CONNECTION_FILE)
    monkeypatch.setenv("BALTHAZAR_SESSION_TUNNEL_URL", "http://127.0.0.1:9/")
    monkeypatch.setenv("BALTHAZAR_SESSION_TUNNEL_TOKEN", "T")
    shim._reset_transport()
    assert shim.tunnel_transport() == "loopback"


# ---------------------------------------------------------------------------
# connect / disconnect
# ---------------------------------------------------------------------------


def test_connect_writes_profile_and_returns_info(shim, server):
    info = _connect_password(shim, server)
    assert info["transport"] == "app"
    assert info["user_id"] == server.user      # server reports ``user`` -> mapped
    assert info["flow_run_id"] == "run-1"
    assert info["flow_name"] == "IV sweep"
    assert shim.tunnel_transport() == "app"

    st = os.stat(shim.CONNECTION_FILE)
    assert stat.S_IMODE(st.st_mode) == 0o600
    text = open(shim.CONNECTION_FILE, encoding="utf-8").read()
    for secret in ("token", "access", "refresh", server.password):
        assert secret not in text              # no credentials of any kind in the profile
    profile = json.loads(text)
    assert profile == {
        "transport": "app", "app_url": server.app_url(),
        "login": "password", "client_id": "blt-frontend2", "username": server.username,
    }


def test_connect_session_is_reused(shim, server):
    _connect_password(shim, server)
    grants_before = list(server.token_grants)
    shim.ping()                                 # a follow-up call must not log in again
    assert server.token_grants == grants_before


def test_disconnect_removes_profile_and_forgets_token(shim, server):
    shim.tunnel_connect(server.app_url(), login="device")   # device login caches a token
    cache = shim._app_auth_module.TOKEN_CACHE
    assert os.path.exists(shim.CONNECTION_FILE)
    assert cache.exists() and server.cache_key() in json.loads(cache.read_text())

    shim.tunnel_disconnect(forget=True)
    assert not os.path.exists(shim.CONNECTION_FILE)
    assert shim.tunnel_transport() == "none"
    remaining = json.loads(cache.read_text()) if cache.exists() else {}
    assert server.cache_key() not in remaining


# ---------------------------------------------------------------------------
# ops route through the app transport
# ---------------------------------------------------------------------------


def test_read_op_uses_cookie_only(shim, server):
    server.responder = lambda op, kw: [{"id": "f1", "name": "IV"}] if op == "search_flows" \
        else server.default_responder(op, kw)
    _connect_password(shim, server)
    flows = shim.search_flows(name="IV*")
    assert [f.id for f in flows] == ["f1"]
    call = next(c for c in server.rpc_calls if c[0] == "search_flows")
    assert call[2].startswith("blt_tunnel_")    # the grant cookie carried the call


def test_numpy_args_are_tolisted(shim, server):
    captured = {}
    server.responder = lambda op, kw: captured.update(kw) or {"done": True}
    _connect_password(shim, server)

    class FakeArray:
        def tolist(self):
            return [1, 2, 3]

    shim._call("custom_op", values=FakeArray(), nested={"a": FakeArray()})
    assert captured["values"] == [1, 2, 3]
    assert captured["nested"] == {"a": [1, 2, 3]}


def test_heartbeat_works_on_app_transport(shim, server):
    _connect_password(shim, server)
    shim._call("heartbeat")
    assert "heartbeat" in [c[0] for c in server.rpc_calls]


# ---------------------------------------------------------------------------
# error mapping + 404/504
# ---------------------------------------------------------------------------


def test_remote_error_maps_to_builtin_with_traceback(shim, server):
    _connect_password(shim, server)
    server.responder = lambda op, kw: RpcError("KeyError", "boom", "Traceback…\nKeyError")
    with pytest.raises(KeyError) as excinfo:
        shim._call("whatever")
    assert excinfo.value.remote_traceback == "Traceback…\nKeyError"


def test_unknown_error_type_becomes_tunnel_error(shim, server):
    _connect_password(shim, server)
    server.responder = lambda op, kw: RpcError("WeirdError", "nope", "T")
    with pytest.raises(shim.TunnelError) as excinfo:
        shim._call("whatever")
    assert excinfo.value.remote_traceback == "T"


def test_404_message(shim, server):
    _connect_password(shim, server)
    server.rpc_http_status = 404
    with pytest.raises(shim.TunnelError) as excinfo:
        shim.ping()
    assert "not active" in str(excinfo.value) or "not running" in str(excinfo.value)


def test_504_message(shim, server):
    _connect_password(shim, server)
    server.rpc_http_status = 504
    with pytest.raises(shim.TunnelError) as excinfo:
        shim.ping()
    assert "60 s" in str(excinfo.value)


# ---------------------------------------------------------------------------
# long-op polling
# ---------------------------------------------------------------------------


def test_space_schema_polls_until_ready(shim, server, capsys):
    digest = {"version": 1, "totals": {"devices": 5}}
    state = {"polls": 0}

    def responder(op, kw):
        if op == "space_schema":
            return {"state": "building", "progress": {"loaded_flows": 0, "total_flows": 3}}
        if op == "space_schema_status":
            state["polls"] += 1
            if state["polls"] >= 2:
                return {"state": "ready", "digest": digest,
                        "progress": {"loaded_flows": 3, "total_flows": 3}}
            return {"state": "building", "progress": {"loaded_flows": 1, "total_flows": 3}}
        return server.default_responder(op, kw)

    server.responder = responder
    _connect_password(shim, server)
    assert shim.tunnel_space_schema() == digest
    assert state["polls"] >= 2
    assert "space schema" in capsys.readouterr().err   # one-line progress notice


def test_refresh_device_cache_polls_until_ready(shim, server):
    state = {"polls": 0}

    def responder(op, kw):
        if op == "refresh_device_cache":
            return {"state": "loading"}
        if op == "device_cache_status":
            state["polls"] += 1
            return {"state": "ready", "count": 10, "loaded": 10} if state["polls"] >= 2 \
                else {"state": "loading", "loaded": state["polls"] * 5}
        return server.default_responder(op, kw)

    server.responder = responder
    _connect_password(shim, server)
    status = shim.refresh_device_cache(wait=True)
    assert status["state"] == "ready"


def test_cached_devices_cold_cache_loads_then_serves(shim, server, capsys):
    records = [{"id": "w0", "name": "w0", "type": "Die", "params": {"wafer": "W1"}}]
    state = {"polls": 0}

    def responder(op, kw):
        if op == "device_cache_status":
            state["polls"] += 1
            return {"state": "ready"} if state["polls"] >= 2 else {"state": "loading", "loaded": 5}
        if op == "cached_devices":
            if state["polls"] >= 2:
                return {"state": "ready", "devices": records, "next_offset": None, "total": 1, "offset": 0}
            return {"state": "loading", "devices": [], "next_offset": None, "total": 0,
                    "offset": 0, "loaded": 5, "count": 0}
        return server.default_responder(op, kw)

    server.responder = responder
    _connect_password(shim, server)
    out = shim.cached_devices("wafer", "W1")
    assert [d.id for d in out] == ["w0"]
    assert "loading device cache" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# byte-budgeted page following
# ---------------------------------------------------------------------------


def test_cached_devices_query_follows_next_offset(shim, server):
    records = [{"id": f"d{i}", "name": f"d{i}", "type": "Die", "params": {}} for i in range(6)]

    def responder(op, kw):
        if op == "device_cache_status":
            return {"state": "ready"}
        if op == "cached_devices_query":
            off = kw.get("offset") or 0
            page = records[off:off + 2]
            nxt = off + 2 if off + 2 < len(records) else None
            return {"devices": page, "total": len(records), "offset": off,
                    "limit": kw.get("limit"), "next_offset": nxt}
        return server.default_responder(op, kw)

    server.responder = responder
    _connect_password(shim, server)
    out = shim.tunnel_cached_devices_query(device_type="Die")
    assert [d.id for d in out] == [f"d{i}" for i in range(6)]
    query = next(c for c in server.rpc_calls if c[0] == "cached_devices_query")
    assert "max_bytes" in query[1]                      # byte budget was sent


def test_cached_devices_index_follows_pages_to_bare_list(shim, server):
    records = [{"id": f"w{i}", "name": f"w{i}", "type": "Die", "params": {"wafer": "W1"}} for i in range(4)]

    def responder(op, kw):
        if op == "device_cache_status":
            return {"state": "ready"}
        if op == "cached_devices":
            off = kw.get("offset") or 0
            page = records[off:off + 2]
            nxt = off + 2 if off + 2 < len(records) else None
            return {"state": "ready", "devices": page, "next_offset": nxt, "total": len(records), "offset": off}
        return server.default_responder(op, kw)

    server.responder = responder
    _connect_password(shim, server)
    out = shim.cached_devices("wafer", "W1")
    assert [d.id for d in out] == [f"w{i}" for i in range(4)]
    assert all(isinstance(d, shim.Device) for d in out)


# ---------------------------------------------------------------------------
# non-interactive guard
# ---------------------------------------------------------------------------


def test_non_interactive_raises_login_required(shim, server, monkeypatch):
    _write_profile(shim, {"transport": "app", "app_url": server.app_url(),
                          "login": "device", "client_id": "blt-frontend2"})
    monkeypatch.setenv("BALTHAZAR_TUNNEL_NONINTERACTIVE", "1")
    shim._reset_transport()
    with pytest.raises(shim.TunnelLoginRequired):
        shim.ping()
    assert server.token_grants == []            # no login flow was started
