"""Tests for ``blt_analytics._blt`` — locating the right balthazar module.

The situations the locator must get right now that the bridge is the only transport:

* an importable **real** module (here, the fake) is used as-is, and is not a tunnel;
* with no real module importable but a bridge profile/env configured, the **bridge
  drop-in** is loaded by file path;
* with nothing importable and no bridge profile, ``get_blt`` raises;
* the bridge marker, ``tunnel_ns`` and ``describe_info`` are read correctly.
"""

from __future__ import annotations

import os
import sys

import pytest

import blt_analytics._blt as blt_locator


@pytest.fixture
def isolate(monkeypatch, tmp_path):
    """Remove any installed ``balthazar`` and clear the path cache.

    Lets each test control what ``get_blt`` resolves to without a bare
    ``import balthazar`` short-circuiting the path logic. Also neutralises the
    bridge trigger — a tmp ``HOME`` with no ``~/.balthazar_bridge.json`` and no
    ``$BALTHAZAR_BRIDGE_URL`` — so a test only sees the bridge drop-in when it opts in.
    Restores state on teardown.
    """
    monkeypatch.delenv("BALTHAZAR_BRIDGE_URL", raising=False)
    monkeypatch.delenv("BLT_BRIDGE_DIR", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))  # no ~/.balthazar_bridge.json here
    saved = sys.modules.pop("balthazar", None)
    saved_path = list(sys.path)  # the bridge loader prepends bridge dirs; don't leak them
    # The drop-in loader prepends the repo ``bridge/`` dir and never removes it, so an
    # earlier test can leave it on ``sys.path``; strip it for the duration of the test so
    # a bare ``import balthazar`` can't resolve to the real drop-in and short-circuit the
    # path logic under test.
    repo_bridge = os.path.abspath(blt_locator._bridge_dir())
    sys.path[:] = [p for p in sys.path if os.path.abspath(p) != repo_bridge]
    blt_locator._PATH_SHIMS.clear()
    try:
        yield blt_locator
    finally:
        blt_locator._PATH_SHIMS.clear()
        sys.path[:] = saved_path
        sys.modules.pop("balthazar", None)
        if saved is not None:
            sys.modules["balthazar"] = saved


def test_real_module_used_as_is(fake_blt):
    # The fake real module is installed by the fixture via sys.modules.
    assert blt_locator.get_blt() is fake_blt
    assert blt_locator.is_tunnel() is False


def test_no_module_and_no_profile_raises(isolate):
    # Nothing importable as ``balthazar`` and no bridge profile/env -> a clear error.
    with pytest.raises(RuntimeError):
        blt_locator.get_blt()


# ---------------------------------------------------------------------------
# reflection bridge drop-in (SPEC "blt_analytics integration")
# ---------------------------------------------------------------------------

# A minimal stand-in for bridge/balthazar.py: the bridge marker plus a ``tunnel``
# namespace whose functions resolve dynamically, exactly like the real drop-in's
# Remote delegation. Written to a tmp dir so these tests never need the real
# bridge/ files to exist.
_BRIDGE_SRC = '''
__balthazar_tunnel__ = 3


class _Tunnel:
    def __getattr__(self, name):
        def _call(*a, **k):
            return {"name": name, "args": a, "kwargs": k}
        return _call


tunnel = _Tunnel()


class _Session:
    description = {
        "protocol": 3, "bridge_version": "3.0.0", "user": "caller-7",
        "owner": "owner-1", "shared": False, "device_indexes": {"wafer": "hierarchy.wafer"},
    }


_session = _Session()
'''


def _make_bridge_dir(tmp_path):
    bridge = tmp_path / "bridge"
    bridge.mkdir()
    (bridge / "balthazar.py").write_text(_BRIDGE_SRC, encoding="utf-8")
    return bridge


def test_bridge_loaded_from_profile_env(isolate, monkeypatch, tmp_path):
    bridge = _make_bridge_dir(tmp_path)
    monkeypatch.setenv("BLT_BRIDGE_DIR", str(bridge))
    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://host/app-tunnel/a/b/")

    module = blt_locator.get_blt()
    assert getattr(module, "__balthazar_tunnel__", None) == 3
    assert blt_locator.is_tunnel() is True


def test_tunnel_ns_and_describe(isolate, monkeypatch, tmp_path):
    bridge = _make_bridge_dir(tmp_path)
    monkeypatch.setenv("BLT_BRIDGE_DIR", str(bridge))
    monkeypatch.setenv("BALTHAZAR_BRIDGE_URL", "https://host/app-tunnel/a/b/")

    ns = blt_locator.tunnel_ns()
    assert ns is not None
    # The namespace resolves tunnel functions dynamically.
    assert ns.space_schema(refresh=False)["name"] == "space_schema"
    # describe_info reaches the Remote's _session.description through the drop-in.
    desc = blt_locator.describe_info()
    assert desc["user"] == "caller-7" and desc["owner"] == "owner-1"
    assert desc["device_indexes"] == {"wafer": "hierarchy.wafer"}


def test_bridge_not_used_without_profile(isolate, monkeypatch, tmp_path):
    # Bridge files exist, but no profile/env is configured -> get_blt raises (there is
    # no other fallback to load).
    bridge = _make_bridge_dir(tmp_path)
    monkeypatch.setenv("BLT_BRIDGE_DIR", str(bridge))
    # Neither BALTHAZAR_BRIDGE_URL nor ~/.balthazar_bridge.json present (tmp HOME).
    with pytest.raises(RuntimeError):
        blt_locator.get_blt()


def test_real_module_is_not_a_bridge(fake_blt):
    assert blt_locator.is_tunnel() is False
    assert blt_locator.tunnel_ns() is None
    assert blt_locator.describe_info() == {}
