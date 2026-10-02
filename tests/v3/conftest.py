"""Fixtures for the v3 bridge tests.

Every test runs the real HTTP handler from ``flows/tunnel_bridge.py`` on
``127.0.0.1:0`` with the fake *real* ``balthazar`` injected as
``sys.modules["balthazar"]``. The flow module is imported *fresh* per test (as the
v2 server tests do), so its ``import balthazar as blt`` binds the fake and its
module-level state — refs, device cache, schema cache — starts clean, and the
import-time ``_audit_info = blt.info`` capture points at the fake's logger.

The ``bridge_server`` factory stands up the handler with a chosen set of flow
params and returns ``(url, module)``; tests build a :class:`v3._helpers.Client`
against that url (the auth-bypassed transport the spec calls for).
"""

from __future__ import annotations

import importlib.util
import sys
import threading
from http.server import ThreadingHTTPServer

import pytest

from fakes import fake_blt as _fake_blt_module
from v3._helpers import BRIDGE_PATH, OWNER, Client


@pytest.fixture
def fake_blt():
    """Install the fake *real* balthazar and set ``blt.user`` to the known owner."""
    _fake_blt_module.reset_faults()
    _fake_blt_module.user = OWNER
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


@pytest.fixture
def bridge(fake_blt):
    """A freshly imported ``flows/tunnel_bridge.py``, with the fake already installed."""
    spec = importlib.util.spec_from_file_location("tunnel_bridge_under_test", BRIDGE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def bridge_server(bridge):
    """Factory: ``url, mod = bridge_server(**flow_params)`` starts the handler."""
    servers = []

    def _make(**params):
        bridge.configure(params)
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), bridge.Handler)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        servers.append(httpd)
        return "http://127.0.0.1:%d" % httpd.server_address[1], bridge

    yield _make
    for httpd in servers:
        httpd.shutdown()
        httpd.server_close()


@pytest.fixture
def client_factory():
    """The :class:`Client` constructor (so a test can make several callers)."""
    return Client
