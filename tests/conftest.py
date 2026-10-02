"""Shared pytest fixtures for the analytics tests.

The ``fakes`` package is importable because ``pythonpath = ["tests"]`` in
pyproject puts the tests directory on ``sys.path`` — deliberately *not* the repo
root, so a bare ``import balthazar`` resolves the way it does in production. The
fake real module is injected explicitly via ``sys.modules`` here instead.
"""

from __future__ import annotations

import sys

import pytest

from fakes import fake_blt as _fake_blt_module
from fakes import fixture_space


@pytest.fixture
def fake_blt():
    """Install the fake *real* balthazar as ``sys.modules["balthazar"]``.

    Restores whatever was there before (or removes it) on teardown, and resets the
    fake's injected faults and the locator's path-shim cache either side, so tests
    cannot leak state into one another.
    """
    import blt_analytics._blt as blt_locator

    _fake_blt_module.reset_faults()
    blt_locator._PATH_SHIMS.clear()

    saved = sys.modules.get("balthazar")
    sys.modules["balthazar"] = _fake_blt_module
    try:
        yield _fake_blt_module
    finally:
        if saved is not None:
            sys.modules["balthazar"] = saved
        else:
            sys.modules.pop("balthazar", None)
        _fake_blt_module.reset_faults()
        blt_locator._PATH_SHIMS.clear()


@pytest.fixture
def fixture_records():
    """The fixture space as §1 wire-format records: ``{devices, flows, runs}``."""
    return fixture_space.to_records()
