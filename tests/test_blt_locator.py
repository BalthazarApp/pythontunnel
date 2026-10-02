"""Tests for ``blt_analytics._blt`` — locating the right balthazar module.

Four situations the locator must get right:

* an importable **real** module (here, the fake) is used as-is, and is not a tunnel;
* with no real/v2 module importable, the **v2 shim** is loaded by file path;
* ``$BLT_TUNNEL_SHIM`` overrides which file is loaded;
* the **v1 one-shot shim** is refused loudly rather than used.
"""

from __future__ import annotations

import os
import sys

import pytest

import blt_analytics._blt as blt_locator

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
V2_SHIM = os.path.join(REPO, "session_tunnel", "balthazar.py")
V1_SHIM = os.path.join(REPO, "balthazar.py")


@pytest.fixture
def isolate(monkeypatch):
    """Remove any installed ``balthazar`` and clear the path-shim cache.

    Lets each test control what ``get_blt`` resolves to without a bare
    ``import balthazar`` (which, from the repo root, would find the v1 shim)
    short-circuiting the path logic. Restores state on teardown.
    """
    monkeypatch.delenv("BLT_TUNNEL_SHIM", raising=False)
    saved = sys.modules.pop("balthazar", None)
    blt_locator._PATH_SHIMS.clear()
    try:
        yield blt_locator
    finally:
        blt_locator._PATH_SHIMS.clear()
        if saved is not None:
            sys.modules["balthazar"] = saved
        else:
            sys.modules.pop("balthazar", None)


def test_real_module_used_as_is(fake_blt):
    # The fake real module is installed by the fixture via sys.modules.
    assert blt_locator.get_blt() is fake_blt
    assert blt_locator.is_tunnel() is False


def test_v2_shim_loaded_by_default_path(isolate):
    module = blt_locator.get_blt()
    assert getattr(module, "__balthazar_tunnel__", False) is True
    assert hasattr(module, "tunnel_state")  # the v2-only marker
    assert blt_locator.is_tunnel() is True


def test_v2_shim_cached_across_calls(isolate):
    first = blt_locator.get_blt()
    second = blt_locator.get_blt()
    # Same object: the shim holds live state, so it must not be re-executed.
    assert first is second


def test_env_override_selects_shim(isolate, monkeypatch):
    monkeypatch.setenv("BLT_TUNNEL_SHIM", V2_SHIM)
    module = blt_locator.get_blt()
    assert hasattr(module, "tunnel_state")
    assert blt_locator.is_tunnel() is True


def test_v1_shim_rejected(isolate, monkeypatch):
    monkeypatch.setenv("BLT_TUNNEL_SHIM", V1_SHIM)
    with pytest.raises(RuntimeError) as excinfo:
        blt_locator.get_blt()
    assert "v1" in str(excinfo.value).lower()


def test_missing_shim_path_errors(isolate, monkeypatch, tmp_path):
    monkeypatch.setenv("BLT_TUNNEL_SHIM", str(tmp_path / "does_not_exist.py"))
    with pytest.raises(RuntimeError):
        blt_locator.get_blt()
