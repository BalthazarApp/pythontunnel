"""v3 reflection-bridge integration for ``blt_analytics`` (SPEC "blt_analytics
integration", Client items 5 & 11).

Everything here runs against small in-process stubs — a v3 ``blt`` object exposing a
``tunnel`` namespace and ``_session.description``, and a stub ``balthazar_remote``
module — injected by monkeypatching the seams (``_blt.get_blt``,
``cli._import_balthazar_remote``). No network, and no dependency on the concurrent
agent's ``bridge/`` files existing. The assumed client contract is:

* tunnel ops live under ``blt.tunnel`` — ``device_cache_status()``,
  ``cached_devices_query(device_type=, keys=)``, ``cached_devices(index, value,
  refresh=)``, ``space_schema(refresh=)`` returning ``{state, progress, digest?}``;
* the Remote's ``describe`` reply is reachable as ``remote._session.description``
  (so ``blt._session.description`` reaches it through the drop-in);
* ``balthazar_remote.connect(...) -> Remote`` (protocol-checked inside), plus
  ``save_profile``, ``LoginRequired(BridgeError)``, ``BridgeError``.
"""

from __future__ import annotations

import json
import types

import pandas as pd
import pytest

import blt_analytics._blt as blt_locator
from blt_analytics import cli, frames, mcp_server, schema
from fakes import fixture_space


# ---------------------------------------------------------------------------
# v3 stubs
# ---------------------------------------------------------------------------


class _Dev:
    def __init__(self, record: dict):
        self.id = record["id"]
        self.name = record.get("name", "")
        self.type = record.get("type", "device")
        self.fabrication_date = record.get("fabrication_date")
        self.tags = list(record.get("tags") or [])
        self.params = dict(record.get("params") or {})


def _extract(params: dict, dotted: str):
    cur = params
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


class _V3Tunnel:
    """The ``blt.tunnel`` namespace: tunnel ops that record how they were called."""

    def __init__(self, records, *, cache_state="ready", indexes=None, schema_results=None):
        self._records = list(records)
        self._state = cache_state
        self._indexes = dict(indexes or {})
        self._schema_results = list(schema_results or [])
        self.calls: list = []

    def device_cache_status(self):
        self.calls.append(("device_cache_status",))
        return {
            "state": self._state,
            "count": len(self._records),
            "indexes": {n: {"path": p} for n, p in self._indexes.items()},
        }

    def cached_devices_query(self, device_type=None, keys=None):
        self.calls.append(("query", device_type, tuple(keys) if keys else None))
        recs = self._records
        if device_type is not None:
            recs = [r for r in recs if r.get("type") == device_type]
        return [_Dev(r) for r in recs]

    def cached_devices(self, index, value, *, refresh=False):
        self.calls.append(("cached_devices", index, value, refresh))
        path = self._indexes[index]
        return [
            _Dev(r) for r in self._records
            if str(_extract(dict(r.get("params") or {}), path)) == str(value)
        ]

    def refresh_device_cache(self, wait=False):
        self.calls.append(("refresh_device_cache", wait))
        return self.device_cache_status()

    def space_schema(self, refresh=False):
        self.calls.append(("space_schema", refresh))
        if self._schema_results:
            return self._schema_results.pop(0)
        return {"state": "ready", "digest": {"version": 1, "totals": {}}}


class _V3Blt:
    """A stand-in for the v3 drop-in module: the marker, a tunnel namespace, and a
    delegated ``_session.description``."""

    def __init__(self, tunnel, description=None):
        self.__balthazar_tunnel__ = 3
        self.tunnel = tunnel
        self._session = types.SimpleNamespace(description=dict(description or {}))


def _records():
    return fixture_space.to_records()["devices"]


def _use_v3(monkeypatch, blt):
    """Point the locator at a v3 stub (is_tunnel / bridge_version / tunnel_ns compute
    naturally from the marker — no need to patch those too)."""
    monkeypatch.setattr(blt_locator, "get_blt", lambda: blt)
    return blt


@pytest.fixture(autouse=True)
def _cache_and_schema(tmp_path, monkeypatch):
    monkeypatch.setenv("BLT_ANALYTICS_CACHE_DIR", str(tmp_path / "cache"))
    schema.reset()
    yield
    schema.reset()


# ---------------------------------------------------------------------------
# frames: v3 tunnel fast paths
# ---------------------------------------------------------------------------


def test_v3_devices_df_uses_tunnel_query(monkeypatch):
    tunnel = _V3Tunnel(_records())
    _use_v3(monkeypatch, _V3Blt(tunnel))
    df = frames.devices_df()
    assert len(df) == len(_records())
    assert any(c[0] == "query" for c in tunnel.calls)


def test_v3_devices_df_type_and_projection(monkeypatch):
    tunnel = _V3Tunnel(_records())
    _use_v3(monkeypatch, _V3Blt(tunnel))
    df = frames.devices_df("Chip", columns=["hierarchy.lot", "resistance.value"])
    assert set(df["type"]) == {"Chip"}
    assert set(df.columns) == {
        "id", "name", "type", "fabrication_date", "tags",
        "hierarchy.lot", "resistance.value",
    }
    query = next(c for c in tunnel.calls if c[0] == "query")
    assert query[1] == "Chip" and query[2] == ("hierarchy", "resistance")


def test_v3_cold_cache_falls_back_to_paging(monkeypatch):
    # No tunnel.cached_devices_query is used while the cache is cold; devices_df pages
    # search_devices on the module instead. Give the stub a search_devices for that.
    tunnel = _V3Tunnel(_records(), cache_state="loading")
    blt = _V3Blt(tunnel)
    paged = {"called": False}

    def search_devices(*, type=None, archived=None, **_ignored):
        paged["called"] = True
        assert "keys" not in _ignored  # v3 must NOT push the shim-only projection
        recs = _records()
        if type is not None:
            recs = [r for r in recs if r.get("type") == type]
        return [_Dev(r) for r in recs]

    blt.search_devices = search_devices
    _use_v3(monkeypatch, blt)
    df = frames.devices_df("Chip", columns=["resistance.value"])
    assert paged["called"] is True
    assert set(df["type"]) == {"Chip"}


def test_v3_index_value_uses_tunnel_cached_devices(monkeypatch):
    tunnel = _V3Tunnel(_records(), indexes={"wafer": "hierarchy.wafer"})
    _use_v3(monkeypatch, _V3Blt(tunnel))
    df = frames.devices_df(index="wafer", value="SECRET_wafer_17", refresh=True)
    assert list(df["id"]) == ["dev-chip-1"]
    call = next(c for c in tunnel.calls if c[0] == "cached_devices")
    assert call[1:] == ("wafer", "SECRET_wafer_17", True)


# ---------------------------------------------------------------------------
# schema: v3 space_schema (polling) + device indexes from describe
# ---------------------------------------------------------------------------


_DIGEST = {
    "version": 1, "built_at": "2026-10-02T00:00:00Z",
    "totals": {"devices": 3, "flows": 1, "runs": 2},
    "device_types": {"Chip": {"count": 3, "params": {}}},
    "flows": {},
}


def test_v3_get_digest_via_space_schema(monkeypatch):
    tunnel = _V3Tunnel([], schema_results=[{"state": "ready", "digest": _DIGEST}])
    _use_v3(monkeypatch, _V3Blt(tunnel))
    d = schema.get_digest()
    assert d is _DIGEST
    assert ("space_schema", False) in tunnel.calls


def test_v3_space_schema_polls_until_ready(monkeypatch):
    monkeypatch.setattr(schema, "_SCHEMA_POLL_INTERVAL_S", 0)
    tunnel = _V3Tunnel(
        [],
        schema_results=[
            {"state": "building", "progress": 0.3},
            {"state": "building", "progress": 0.8},
            {"state": "ready", "digest": _DIGEST},
        ],
    )
    _use_v3(monkeypatch, _V3Blt(tunnel))
    d = schema.get_digest()
    assert d is _DIGEST
    assert sum(1 for c in tunnel.calls if c[0] == "space_schema") == 3


def test_v3_space_schema_error_raises(monkeypatch):
    monkeypatch.setattr(schema, "_SCHEMA_POLL_INTERVAL_S", 0)
    tunnel = _V3Tunnel([], schema_results=[{"state": "error", "error": "digest import failed"}])
    _use_v3(monkeypatch, _V3Blt(tunnel))
    with pytest.raises(RuntimeError, match="digest import failed"):
        schema.get_digest()


def test_v3_overview_device_indexes_from_describe(monkeypatch):
    tunnel = _V3Tunnel([], schema_results=[{"state": "ready", "digest": _DIGEST}])
    blt = _V3Blt(tunnel, description={"device_indexes": {"wafer": "hierarchy.wafer"}})
    _use_v3(monkeypatch, blt)
    out = schema.overview()
    assert out["device_indexes"] == {"wafer": "hierarchy.wafer"}


# ---------------------------------------------------------------------------
# CLI: connect / disconnect / doctor (v3)
# ---------------------------------------------------------------------------


class _FakeRemote:
    def __init__(self, description):
        self._session = types.SimpleNamespace(description=dict(description))


def _stub_remote_module(*, connect_result=None, connect_error=None):
    mod = types.ModuleType("balthazar_remote")

    class BridgeError(RuntimeError):
        pass

    class LoginRequired(BridgeError):
        pass

    calls = {"connect": [], "save_profile": []}

    def connect(app_url, *, site=None, login="device", username=None,
                password=None, ca_file=None, interactive=True):
        calls["connect"].append(
            {"app_url": app_url, "site": site, "login": login, "username": username,
             "password": password, "ca_file": ca_file, "interactive": interactive}
        )
        if connect_error is not None:
            raise connect_error
        return connect_result

    def save_profile(app_url, login, site=None, ca_file=None):
        calls["save_profile"].append(
            {"app_url": app_url, "login": login, "site": site, "ca_file": ca_file}
        )

    mod.BridgeError = BridgeError
    mod.LoginRequired = LoginRequired
    mod.connect = connect
    mod.save_profile = save_profile
    mod.calls = calls
    return mod


def test_v3_connect_saves_profile_and_prints_summary(monkeypatch, capsys):
    remote = _FakeRemote(
        {"user": "caller-7", "owner": "owner-1", "shared": False, "flow_run_id": "run-42"}
    )
    mod = _stub_remote_module(connect_result=remote)
    monkeypatch.setattr(cli, "_import_balthazar_remote", lambda: mod)

    rc = cli.run_connect("https://host/app-tunnel/a/b/?space_id=s", login="device")
    out = capsys.readouterr().out
    assert rc == 0
    assert "caller-7" in out and "owner-1" in out and "private" in out
    assert "run-42" in out
    assert mod.calls["save_profile"] == [
        {"app_url": "https://host/app-tunnel/a/b/?space_id=s",
         "login": "device", "site": None, "ca_file": None}
    ]
    assert mod.calls["connect"][0]["interactive"] is True


def test_v3_connect_shared_mode_and_params(monkeypatch, capsys):
    remote = _FakeRemote({"user": "u", "owner": "o", "shared": True, "flow_run": "r"})
    mod = _stub_remote_module(connect_result=remote)
    monkeypatch.setattr(cli, "_import_balthazar_remote", lambda: mod)
    monkeypatch.setattr("sys.stdin", __import__("io").StringIO("pw\n"))

    rc = cli.run_connect("https://x/app-tunnel/a/b/", login="password",
                         username="alice", password_stdin=True,
                         site="https://core", ca_file="/tmp/ca.pem")
    out = capsys.readouterr().out
    assert rc == 0 and "shared" in out
    call = mod.calls["connect"][0]
    assert call["login"] == "password" and call["username"] == "alice"
    assert call["password"] == "pw" and call["site"] == "https://core"
    assert call["ca_file"] == "/tmp/ca.pem"


def test_v3_connect_login_required_is_clean_failure(monkeypatch, capsys):
    mod = _stub_remote_module()
    mod.connect = lambda *a, **k: (_ for _ in ()).throw(
        mod.LoginRequired("run blt-tunnel connect in a terminal")
    )
    monkeypatch.setattr(cli, "_import_balthazar_remote", lambda: mod)
    rc = cli.run_connect("https://x/app-tunnel/a/b/", login="device")
    err = capsys.readouterr().err
    assert rc == 1 and "could not connect" in err


def test_v3_disconnect_removes_profile(tmp_path, capsys):
    home = tmp_path / "home"
    home.mkdir()
    profile = home / ".balthazar_bridge.json"
    profile.write_text(json.dumps({"app_url": "https://x", "login": "device"}))
    rc = cli.run_disconnect(home=str(home))
    out = capsys.readouterr().out
    assert rc == 0 and "disconnected the v3 bridge" in out
    assert not profile.exists()


def test_v3_disconnect_forget_removes_token(tmp_path, capsys):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".balthazar_bridge.json").write_text("{}")
    token = tmp_path / "home" / ".config" / "balthazar" / "remote.json"
    token.parent.mkdir(parents=True)
    token.write_text(json.dumps({"a": "t"}))
    rc = cli.run_disconnect(forget=True, home=str(home))
    out = capsys.readouterr().out
    assert rc == 0 and "forgot the cached login token" in out
    assert not token.exists()


def test_v3_doctor_reports_bridge(monkeypatch, tmp_path, capsys):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".balthazar_bridge.json").write_text("{}")
    token = home / ".config" / "balthazar" / "remote.json"
    token.parent.mkdir(parents=True)
    token.write_text(json.dumps({"a": "t"}))

    tunnel = _V3Tunnel([], schema_results=[{"state": "ready", "digest": _DIGEST}])
    blt = _V3Blt(tunnel, description={"user": "caller-7", "owner": "owner-1", "shared": False})
    monkeypatch.setattr(blt_locator, "get_blt", lambda: blt)

    project = tmp_path / "proj"
    project.mkdir()
    rc = cli.run_doctor(project=str(project), home=str(home))
    out = capsys.readouterr().out
    assert "[PASS] bridge: v3 reflection bridge" in out
    assert "[PASS] connection:" in out and "cached login token found" in out
    assert "[PASS] describe:" in out and "caller-7" in out and "owner-1" in out
    assert "[PASS] space_schema:" in out


def test_v3_doctor_without_token_skips_describe(monkeypatch, tmp_path, capsys):
    home = tmp_path / "home"
    home.mkdir()
    (home / ".balthazar_bridge.json").write_text("{}")  # profile but no token cache

    tunnel = _V3Tunnel([])
    blt = _V3Blt(tunnel)
    monkeypatch.setattr(blt_locator, "get_blt", lambda: blt)

    project = tmp_path / "proj"
    project.mkdir()
    rc = cli.run_doctor(project=str(project), home=str(home))
    out = capsys.readouterr().out
    assert rc == 1
    assert "[FAIL] connection:" in out and "no cached login token" in out
    assert "[FAIL] describe:" in out and "blt-tunnel connect" in out


# ---------------------------------------------------------------------------
# MCP: non-interactive, clear LoginRequired errors (v3)
# ---------------------------------------------------------------------------


def test_mcp_sets_noninteractive_env():
    # Importing mcp_server pins non-interactive mode before any lazy connect.
    import os
    assert os.environ.get("BALTHAZAR_TUNNEL_NONINTERACTIVE") == "1"


def test_mcp_v3_unavailable_without_token(monkeypatch):
    monkeypatch.setattr(blt_locator, "bridge_version", lambda: 3)
    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://host/app-tunnel/a/b/")
    monkeypatch.setattr(mcp_server, "_has_cached_token", lambda: False)
    schema.reset()
    err = mcp_server._tunnel_unavailable()
    assert err is not None and "blt-tunnel connect" in err["error"]


def test_mcp_v3_ready_runs_call(monkeypatch):
    monkeypatch.setattr(blt_locator, "bridge_version", lambda: 3)
    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://host/app-tunnel/a/b/")
    monkeypatch.setattr(mcp_server, "_has_cached_token", lambda: True)
    schema.reset()
    assert mcp_server._tunnel_unavailable() is None


def test_mcp_guard_converts_login_required(monkeypatch):
    # A LoginRequired raised by a schema call becomes a clear, actionable tool error.
    monkeypatch.setattr(mcp_server, "_tunnel_unavailable", lambda: None)

    class LoginRequired(RuntimeError):
        pass

    def boom():
        raise LoginRequired("need login")

    result = mcp_server._guard(boom)
    assert "blt-tunnel connect" in result["error"]
    assert result.get("hint") == "run blt-tunnel connect"
