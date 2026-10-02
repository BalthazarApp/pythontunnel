"""``overview`` surfaces the configured server-side device indexes (SPEC §6).

The index names/paths come from the tunnel's extended ``ping`` (``device_indexes``).
These tests inject a stub tunnel via ``schema._blt`` (never the real shim/server,
owned by the Tunnel agent). Values never surface — only names and dotted paths.
"""

from __future__ import annotations

import types

import pytest

from blt_analytics import schema

BUILT_AT = "2026-10-02T12:00:00Z"
_EMPTY_DIGEST = {
    "version": 1, "built_at": BUILT_AT, "totals": {}, "device_types": {}, "flows": {},
}


@pytest.fixture(autouse=True)
def _reset_schema():
    schema.reset()
    yield
    schema.reset()


def _tunnel(monkeypatch, *, ping):
    stub = types.SimpleNamespace(
        __balthazar_tunnel__=True,
        tunnel_space_schema=lambda refresh=False: dict(_EMPTY_DIGEST),
        ping=lambda: ping,
    )
    monkeypatch.setattr(schema._blt, "get_blt", lambda: stub)
    monkeypatch.setattr(schema._blt, "is_tunnel", lambda: True)
    return stub


def test_overview_reports_device_indexes(monkeypatch):
    _tunnel(monkeypatch, ping={"flow_id": "r", "device_indexes": {"wafer": "hierarchy.wafer"}})
    out = schema.overview()
    assert out["device_indexes"] == {"wafer": "hierarchy.wafer"}


def test_overview_flattens_object_form_to_path(monkeypatch):
    _tunnel(
        monkeypatch,
        ping={"device_indexes": {"die": {"path": "hierarchy.die", "device_type": "Die"}}},
    )
    out = schema.overview()
    assert out["device_indexes"] == {"die": "hierarchy.die"}


def test_overview_omits_key_when_no_indexes(monkeypatch):
    _tunnel(monkeypatch, ping={"flow_id": "r", "device_indexes": {}})
    assert "device_indexes" not in schema.overview()


def test_overview_omits_key_when_ping_lacks_field(monkeypatch):
    _tunnel(monkeypatch, ping={"flow_id": "r"})
    assert "device_indexes" not in schema.overview()


def test_injected_digest_stays_offline(monkeypatch):
    # With a digest injected (tests), overview must not reach for ping at all.
    called = {"ping": False}

    def boom():
        called["ping"] = True
        raise AssertionError("ping must not be called with an injected digest")

    stub = types.SimpleNamespace(__balthazar_tunnel__=True, ping=boom)
    monkeypatch.setattr(schema._blt, "get_blt", lambda: stub)
    monkeypatch.setattr(schema._blt, "is_tunnel", lambda: True)

    schema.set_digest(dict(_EMPTY_DIGEST))
    out = schema.overview()
    assert "device_indexes" not in out
    assert called["ping"] is False


def test_ping_failure_is_swallowed(monkeypatch):
    def boom():
        raise RuntimeError("no tunnel connection")

    stub = types.SimpleNamespace(
        __balthazar_tunnel__=True,
        tunnel_space_schema=lambda refresh=False: dict(_EMPTY_DIGEST),
        ping=boom,
    )
    monkeypatch.setattr(schema._blt, "get_blt", lambda: stub)
    monkeypatch.setattr(schema._blt, "is_tunnel", lambda: True)
    out = schema.overview()  # must not raise
    assert "device_indexes" not in out
