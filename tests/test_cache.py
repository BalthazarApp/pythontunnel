"""Tests for the on-disk per-space frame cache (``blt_analytics.cache``).

Every test points ``$BLT_ANALYTICS_CACHE_DIR`` at a ``tmp_path`` so nothing touches
the real ``~/.cache``, and installs the ``fake_blt`` real module so ``space_key``
resolves deterministically (to the fake's flow id).
"""

from __future__ import annotations

import time
import types

import pandas as pd
import pytest

from blt_analytics import cache


@pytest.fixture(autouse=True)
def _tmp_cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("BLT_ANALYTICS_CACHE_DIR", str(tmp_path))
    yield tmp_path


def test_builds_once_then_hits_cache(fake_blt):
    calls = {"n": 0}

    def builder():
        calls["n"] += 1
        return {"value": calls["n"]}

    first = cache.cached("thing", builder, key={"a": 1})
    second = cache.cached("thing", builder, key={"a": 1})

    assert first == {"value": 1}
    assert second == {"value": 1}  # same payload, served from disk
    assert calls["n"] == 1  # builder ran only once


def test_refresh_rebuilds_and_overwrites(fake_blt):
    calls = {"n": 0}

    def builder():
        calls["n"] += 1
        return calls["n"]

    assert cache.cached("thing", builder, key={}) == 1
    assert cache.cached("thing", builder, key={}, refresh=True) == 2
    assert calls["n"] == 2
    # The refreshed value is now what a plain read returns.
    assert cache.cached("thing", builder, key={}) == 2


def test_ttl_expiry_forces_rebuild(fake_blt):
    calls = {"n": 0}

    def builder():
        calls["n"] += 1
        return calls["n"]

    assert cache.cached("thing", builder, key={}, ttl=100) == 1
    # A non-positive TTL makes any stored entry stale on the next read.
    assert cache.cached("thing", builder, key={}, ttl=0) == 2
    assert calls["n"] == 2


def test_distinct_keys_do_not_collide(fake_blt):
    cache.cached("thing", lambda: "A", key={"q": 1})
    cache.cached("thing", lambda: "B", key={"q": 2})
    assert cache.cached("thing", lambda: "X", key={"q": 1}) == "A"
    assert cache.cached("thing", lambda: "X", key={"q": 2}) == "B"


def test_dataframe_round_trips_values(fake_blt):
    df = pd.DataFrame(
        {
            "x": [1.5, 2.5, 3.5],
            "tags": [["a"], [], ["b", "c"]],  # object column of lists
            "name": ["p", "q", "r"],
        }
    )
    cache.cached("frame", lambda: df, key={})
    out = cache.cached("frame", lambda: pd.DataFrame(), key={})
    pd.testing.assert_frame_equal(out, df)


def test_key_default_str_handles_non_json_types(fake_blt):
    import datetime

    # A datetime in the key must not blow up the hash (default=str covers it).
    stamp = datetime.datetime(2025, 1, 1)
    assert cache.cached("frame", lambda: 7, key={"since": stamp}) == 7
    assert cache.cached("frame", lambda: 9, key={"since": stamp}) == 7  # same key -> hit


def test_space_key_scopes_the_directory(fake_blt, tmp_path):
    cache.cached("thing", lambda: 1, key={})
    space = cache.space_key()
    assert space == "flow-tunnel"  # derived from the fake real module's flow id
    assert (tmp_path / space).is_dir()
    assert list((tmp_path / space).glob("thing__*"))


# ---------------------------------------------------------------------------
# space_key on the v3 bridge: the describe() memoization (perf fix)
# ---------------------------------------------------------------------------


def _install_fake_tunnel(monkeypatch, describe):
    """Point the cache's locator at a fake v3 bridge whose ``describe_info`` is given.

    ``space_key`` resolves tunnel-ness via ``_blt.is_tunnel()`` and the root id via
    ``_blt.describe_info()`` (both ignore the ``blt`` argument), so all three seams
    are steered.
    """
    module = types.SimpleNamespace(__balthazar_tunnel__=3)
    monkeypatch.setattr(cache._blt, "get_blt", lambda: module)
    monkeypatch.setattr(cache._blt, "is_tunnel", lambda: True)
    monkeypatch.setattr(cache._blt, "describe_info", describe)
    return module


def test_space_key_memoizes_describe_across_lookups(monkeypatch):
    cache._ROOT_CACHE.clear()
    counter = {"n": 0}

    def describe():
        counter["n"] += 1
        return {"flow_run": "root-123"}

    _install_fake_tunnel(monkeypatch, describe)
    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://tunnel.example/one")

    keys = {cache.space_key() for _ in range(3)}
    assert len(keys) == 1  # stable
    assert next(iter(keys)).startswith("tunnel-")
    assert counter["n"] == 1  # three lookups, a single describe


def test_space_key_requeries_when_bridge_url_changes(monkeypatch):
    cache._ROOT_CACHE.clear()
    counter = {"n": 0}

    def describe():
        counter["n"] += 1
        return {"flow_run": "root-123"}

    _install_fake_tunnel(monkeypatch, describe)

    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://tunnel.example/one")
    first = cache.space_key()
    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://tunnel.example/two")
    second = cache.space_key()

    assert first != second  # switching bridges changes the namespace
    assert counter["n"] == 2  # and re-queries describe for the new URL


def test_space_key_does_not_memoize_a_failed_describe(monkeypatch):
    cache._ROOT_CACHE.clear()
    state = {"fail": True, "n": 0}

    def describe():
        state["n"] += 1
        if state["fail"]:
            raise RuntimeError("bridge offline")
        return {"flow_run": "root-xyz"}

    _install_fake_tunnel(monkeypatch, describe)
    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://tunnel.example/x")

    cache.space_key()  # describe raises -> url-only key, nothing memoized
    state["fail"] = False
    cache.space_key()  # recovers -> queries again rather than reusing the failure
    assert state["n"] == 2


def test_clear_removes_entries(fake_blt):
    cache.cached("a", lambda: 1, key={})
    cache.cached("b", lambda: 2, key={})
    assert cache.clear(name="a") == 1
    # 'a' rebuilds, 'b' still cached.
    calls = {"n": 0}

    def rebuild_a():
        calls["n"] += 1
        return 10

    cache.cached("a", rebuild_a, key={})
    assert calls["n"] == 1
    assert cache.clear() >= 1
