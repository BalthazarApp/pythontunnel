"""Compatibility of the v3 client with the UNMODIFIED remoteblt4 bridge handler.

The remoteblt4 handler speaks the parts protocol but not protocol 3, so:

* a normal ``connect`` must raise the clear protocol error, and
* with the documented ``_skip_protocol_check`` test flag a basic call, a >2 MiB
  result and a >2 MiB upload must all still work (the parts protocol is shared
  verbatim between remoteblt4 and the v3 client).

The handler is loaded by path with ``tests/fakes/fake_blt.py`` injected as the
``balthazar`` module; the bridge runs ``SHARED = True`` so any ``X-BLT-User-Id``
is accepted.
"""

import base64
import contextlib
import os
import sys
import threading
from http.server import ThreadingHTTPServer

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import client_fake  # noqa: E402

from fakes import fake_blt  # noqa: E402

mod = client_fake.remote_module()


@contextlib.contextmanager
def remoteblt4_server():
    fake_blt.reset_faults()
    fake_blt.set_synthetic_devices(None)
    fake_blt.user = "owner-user"
    bridge_module = client_fake.load_remoteblt4_bridge(fake_blt)
    server = ThreadingHTTPServer(("127.0.0.1", 0), bridge_module.Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = "http://127.0.0.1:%d/" % server.server_address[1]
    try:
        yield base_url
    finally:
        server.shutdown()
        server.server_close()
        fake_blt.reset_faults()
        fake_blt.set_synthetic_devices(None)


def _big_records(count=300, blob_bytes=9000):
    records = []
    for n in range(count):
        blob = base64.b64encode(os.urandom(blob_bytes)).decode("ascii")
        records.append({
            "id": "syn-%05d" % n,
            "name": "synthetic-%05d" % n,
            "type": "sample",
            "params": {"blob": blob, "n": n},
        })
    return records


def test_remoteblt4_without_protocol_is_rejected():
    with remoteblt4_server() as base_url:
        with pytest.raises(mod.BridgeError) as excinfo:
            mod.connect(_test_base_url=base_url, _test_user_id="owner-user")
        assert "protocol 3" in str(excinfo.value)


def test_remoteblt4_basic_call_with_skip_flag():
    with remoteblt4_server() as base_url:
        blt = mod.connect(
            _test_base_url=base_url, _test_user_id="owner-user", _skip_protocol_check=True,
        )
        try:
            devices = blt.search_devices()
            assert isinstance(devices, list)
            assert devices and all(hasattr(device, "name") for device in devices)
        finally:
            blt.close()


def test_remoteblt4_large_upload_and_large_result():
    records = _big_records()
    with remoteblt4_server() as base_url:
        blt = mod.connect(
            _test_base_url=base_url, _test_user_id="owner-user", _skip_protocol_check=True,
        )
        try:
            # >2 MiB upload: the synthetic records are packed and split on the way up.
            blt.set_synthetic_devices(records)
            # >2 MiB result: searching returns all of them, split on the way down.
            devices = blt.search_devices()
            assert len(devices) == len(records)
            assert devices[0].params["blob"]
        finally:
            blt.close()
