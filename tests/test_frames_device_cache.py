"""Tests for ``devices_df``'s server-side device-cache paths (SPEC §6).

These do not touch the real shim or server (owned by the Tunnel agent). Instead a
small stub module that implements the assumed shim contract —
``device_cache_status``, ``cached_devices``, ``tunnel_cached_devices_query``,
``ping`` and ``search_devices`` — is injected by monkeypatching
``frames._blt.get_blt`` / ``frames._blt.is_tunnel`` (the same technique the schema
tests use for the tunnel op). The assumed function names are:

* ``blt.device_cache_status() -> {"state": ...}``
* ``blt.cached_devices(index, value, *, refresh=False) -> list[Device]``
* ``blt.tunnel_cached_devices_query(device_type=None, keys=None) -> list[Device]``
* ``blt.refresh_device_cache(wait=True)`` (not exercised here)
"""

from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest

from blt_analytics import frames
from fakes import fixture_space


# ---------------------------------------------------------------------------
# Stub shim
# ---------------------------------------------------------------------------


class _Dev:
    """A device object with just the attributes ``_devices_to_frame`` reads."""

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


class _StubShim:
    """A stub tunnel shim with a ready (configurable) device cache."""

    __balthazar_tunnel__ = True

    def __init__(self, records, *, state="ready", indexes=None,
                 has_cache_ops=True, has_cached_devices=True):
        self._records = list(records)
        self._state = state
        self._indexes = dict(indexes or {})
        self.calls: list = []
        hidden: set[str] = set()
        if not has_cache_ops:
            # Model a shim too old for the device cache: these ops are absent.
            hidden |= {"device_cache_status", "tunnel_cached_devices_query"}
        if not has_cached_devices:
            hidden.add("cached_devices")
        self._hidden = hidden

    def __getattribute__(self, name):
        hidden = object.__getattribute__(self, "__dict__").get("_hidden", ())
        if name in hidden:
            raise AttributeError(name)  # the op does not exist on this (old) shim
        return object.__getattribute__(self, name)

    # --- device cache ops --------------------------------------------------
    def device_cache_status(self):
        self.calls.append(("device_cache_status",))
        return {
            "state": self._state,
            "count": len(self._records),
            "indexes": {n: {"path": p} for n, p in self._indexes.items()},
        }

    def tunnel_cached_devices_query(self, device_type=None, keys=None):
        self.calls.append(("query", device_type, tuple(keys) if keys else None))
        recs = self._records
        if device_type is not None:
            recs = [r for r in recs if r.get("type") == device_type]
        return [_Dev(r) for r in recs]

    def cached_devices(self, index, value, *, refresh=False):
        self.calls.append(("cached_devices", index, value, refresh))
        path = self._indexes[index]  # KeyError models "unknown index" upstream
        return [
            _Dev(r) for r in self._records
            if str(_extract(dict(r.get("params") or {}), path)) == str(value)
        ]

    def refresh_device_cache(self, wait=True):
        self.calls.append(("refresh_device_cache", wait))
        return self.device_cache_status()

    def ping(self):
        return {"flow_id": "root", "device_indexes": dict(self._indexes)}

    # --- paging fallback ---------------------------------------------------
    def search_devices(self, *, type=None, archived=None, keys=None, **_ignored):
        self.calls.append(("search_devices", type, tuple(keys) if keys else None))
        recs = self._records
        if type is not None:
            recs = [r for r in recs if r.get("type") == type]
        return [_Dev(r) for r in recs]


@pytest.fixture(autouse=True)
def _tmp_cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("BLT_ANALYTICS_CACHE_DIR", str(tmp_path))
    yield tmp_path


def _records():
    return fixture_space.to_records()["devices"]


def _install(monkeypatch, stub, *, tunnel=True):
    monkeypatch.setattr(frames._blt, "get_blt", lambda: stub)
    monkeypatch.setattr(frames._blt, "is_tunnel", lambda: tunnel)
    return stub


# ---------------------------------------------------------------------------
# cached_devices_query path (whole-space / type-filtered)
# ---------------------------------------------------------------------------


def test_ready_cache_reads_via_query_not_paging(monkeypatch):
    stub = _install(monkeypatch, _StubShim(_records()))
    df = frames.devices_df()
    assert len(df) == len(_records())
    assert any(c[0] == "query" for c in stub.calls)
    assert not any(c[0] == "search_devices" for c in stub.calls)


def test_ready_cache_type_filter_and_projection_pushdown(monkeypatch):
    stub = _install(monkeypatch, _StubShim(_records()))
    df = frames.devices_df("Chip", columns=["hierarchy.lot", "resistance.value"])
    assert set(df["type"]) == {"Chip"}
    # The columns project client-side, identity + exactly those two paths.
    assert set(df.columns) == {
        "id", "name", "type", "fabrication_date", "tags",
        "hierarchy.lot", "resistance.value",
    }
    # Top-level keys are pushed down to the query op.
    query = next(c for c in stub.calls if c[0] == "query")
    assert query[1] == "Chip"
    assert query[2] == ("hierarchy", "resistance")


def test_ready_cache_values_are_real(monkeypatch):
    _install(monkeypatch, _StubShim(_records()))
    df = frames.devices_df("Chip").set_index("id")
    assert df.loc["dev-chip-1", "resistance.value"] == fixture_space.N_RESISTANCE


def test_cold_cache_falls_back_to_paging(monkeypatch):
    stub = _install(monkeypatch, _StubShim(_records(), state="loading"))
    df = frames.devices_df("Chip")
    assert set(df["type"]) == {"Chip"}
    assert any(c[0] == "search_devices" for c in stub.calls)
    assert not any(c[0] == "query" for c in stub.calls)


def test_old_shim_without_cache_ops_falls_back_to_paging(monkeypatch):
    stub = _install(monkeypatch, _StubShim(_records(), has_cache_ops=False))
    df = frames.devices_df()
    assert len(df) == len(_records())
    assert any(c[0] == "search_devices" for c in stub.calls)


def test_real_runner_unaffected_uses_paging(monkeypatch):
    # Not a tunnel: the cache fast path is skipped entirely, even if ops exist.
    stub = _install(monkeypatch, _StubShim(_records()), tunnel=False)
    df = frames.devices_df()
    assert len(df) == len(_records())
    assert any(c[0] == "search_devices" for c in stub.calls)
    assert not any(c[0] in ("query", "device_cache_status") for c in stub.calls)


def test_server_cache_bypasses_local_pickle_cache(monkeypatch):
    # First read sees all devices; then the server set shrinks. A second read with
    # no refresh must reflect the new data — proving the local frame cache is skipped
    # for the server-cache path (a local pickle would still show the old count).
    stub = _install(monkeypatch, _StubShim(_records()))
    first = frames.devices_df()
    assert len(first) == len(_records())

    stub._records = _records()[:2]
    second = frames.devices_df()  # no refresh=
    assert len(second) == 2


# ---------------------------------------------------------------------------
# index= / value= shortcut (cached_devices)
# ---------------------------------------------------------------------------


def test_index_value_shortcut_fetches_that_value(monkeypatch):
    stub = _install(
        monkeypatch, _StubShim(_records(), indexes={"wafer": "hierarchy.wafer"})
    )
    df = frames.devices_df(index="wafer", value="SECRET_wafer_17")
    assert list(df["id"]) == ["dev-chip-1"]
    call = next(c for c in stub.calls if c[0] == "cached_devices")
    assert call[1:] == ("wafer", "SECRET_wafer_17", False)


def test_index_value_forwards_refresh(monkeypatch):
    stub = _install(
        monkeypatch, _StubShim(_records(), indexes={"wafer": "hierarchy.wafer"})
    )
    frames.devices_df(index="wafer", value="SECRET_wafer_17", refresh=True)
    call = next(c for c in stub.calls if c[0] == "cached_devices")
    assert call[3] is True  # refresh forwarded as the per-value known-id refresh


def test_index_value_respects_columns_projection(monkeypatch):
    _install(monkeypatch, _StubShim(_records(), indexes={"wafer": "hierarchy.wafer"}))
    df = frames.devices_df(
        index="wafer", value="SECRET_wafer_17", columns=["resistance.value"]
    )
    assert set(df.columns) == {
        "id", "name", "type", "fabrication_date", "tags", "resistance.value",
    }
    assert df.iloc[0]["resistance.value"] == fixture_space.N_RESISTANCE


def test_index_without_value_is_an_error(monkeypatch):
    _install(monkeypatch, _StubShim(_records(), indexes={"wafer": "hierarchy.wafer"}))
    with pytest.raises(ValueError):
        frames.devices_df(index="wafer")
    with pytest.raises(ValueError):
        frames.devices_df(value="W1")


def test_index_value_requires_tunnel(monkeypatch):
    _install(monkeypatch, _StubShim(_records()), tunnel=False)
    with pytest.raises(RuntimeError, match="session tunnel"):
        frames.devices_df(index="wafer", value="W1")


def test_index_value_old_shim_without_cached_devices_errors(monkeypatch):
    _install(
        monkeypatch,
        _StubShim(_records(), indexes={"wafer": "hierarchy.wafer"},
                  has_cached_devices=False),
    )
    with pytest.raises(RuntimeError, match="cached_devices"):
        frames.devices_df(index="wafer", value="W1")
