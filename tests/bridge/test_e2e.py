"""True end-to-end tests: the real server + the real client, in one process.

Unlike ``test_bridge_client.py`` (real client vs. a hand-written fake bridge) and
``test_bridge_server.py`` (real server vs. a raw HTTP caller), every test here wires
the **real** ``flows/tunnel_bridge.py`` handler to the **real**
``bridge/balthazar_remote.py`` client over loopback HTTP, with
``tests/fakes/fake_blt.py`` standing in for the Runner's ``balthazar`` module. The
client connects through its documented login-free test hook
(``connect(_test_base_url=..., _test_user_id=...)``), so the whole protocol — value
tags, the parts protocol, pending/poll, heartbeat/release, context emulation,
``plt.show`` capture and the tunnel namespace — is exercised for real.

The last group drives the ``blt_analytics`` integration (``devices_df`` / ``runs_df``
/ ``schema.overview`` / ``blt-tunnel doctor``) through the real drop-in against the
running test server, via the ``$BLT_BRIDGE_TEST_BASE_URL`` override in
``blt_analytics._blt.get_blt``.
"""

from __future__ import annotations

import contextlib
import gc
import importlib.util
import json
import os
import sys
import threading
import time

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from bridge._helpers import OWNER, Client  # noqa: E402

REPO = os.path.dirname(os.path.dirname(_HERE))
CLIENT_PATH = os.path.join(REPO, "bridge", "balthazar_remote.py")

MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# Loading / connecting the real client
# ---------------------------------------------------------------------------
def _load_client():
    """Import the real client (``bridge/balthazar_remote.py``) by path, with the
    stderr-notice / tunnel-poll delays turned down so the tests stay fast."""
    spec = importlib.util.spec_from_file_location("bridge_balthazar_remote_e2e", CLIENT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.POLL_NOTICE_SECONDS = 0.0
    module.TUNNEL_POLL_SECONDS = 0.02
    return module


@contextlib.contextmanager
def real_client(url, user=OWNER, **tune):
    """Connect the real client to ``url`` as ``user`` via the login-free test hook."""
    client = _load_client()
    for key, value in tune.items():
        setattr(client, key, value)
    blt = client.connect(_test_base_url=url, _test_user_id=user)
    try:
        yield client, blt
    finally:
        blt.close()


# ===========================================================================
# Basic reflection: search, attributes, params snapshot + write roundtrip
# ===========================================================================
def test_search_devices_attributes_and_param_write(bridge_server, fake_blt):
    url, _ = bridge_server()
    with real_client(url) as (_client, blt):
        devices = blt.search_devices()
        assert len(devices) == 10  # the whole fixture space

        chip = blt.search_devices(id=["dev-chip-1"])[0]
        assert chip.type == "Chip"
        assert chip.name == "SECRET_chip_one"
        # a nested params snapshot crosses as a plain dict
        assert chip.params["resistance"]["value"] == pytest.approx(123.456789)
        assert chip.params["hierarchy"]["wafer"] == "SECRET_wafer_17"
        import datetime
        assert isinstance(chip.fabrication_date, datetime.date)

        # Write a whole params attribute back; the ``self`` re-encode the server
        # returns after a ref-targeted ``set`` refreshes the local snapshot.
        updated = dict(chip.params)
        updated["yield_pct"] = 999.5
        chip.params = updated
        assert chip.params["yield_pct"] == 999.5


# ===========================================================================
# Large payloads: the parts protocol, both directions
# ===========================================================================
def test_parts_large_download_multipart(bridge_server, fake_blt):
    url, _ = bridge_server()
    with real_client(url) as (_client, blt):
        data = blt.noise(5 * MIB)  # incompressible -> genuine multi-part download
        assert isinstance(data, (bytes, bytearray))
        assert len(data) == 5 * MIB


def test_parts_large_upload_and_download_roundtrip(bridge_server, fake_blt):
    payload = os.urandom(3 * MIB)  # > part_bytes both ways
    url, _ = bridge_server()
    with real_client(url) as (_client, blt):
        assert blt.echo(payload) == payload


# ===========================================================================
# Long calls: a slow call turns into a pending job the client polls transparently
# ===========================================================================
def test_slow_call_pending_then_polled_transparently(bridge_server, fake_blt, monkeypatch):
    url, _ = bridge_server(call_timeout=0.2)

    gate = threading.Event()
    original = fake_blt.search_devices

    def slow(**kwargs):
        gate.wait(5)
        return original(**kwargs)

    monkeypatch.setattr(fake_blt, "search_devices", slow)

    with real_client(url) as (_client, blt):
        # Release the gate shortly after the first call has already gone pending.
        threading.Timer(0.35, gate.set).start()
        start = time.monotonic()
        devices = blt.search_devices()     # blocks: pending -> poll -> result
        assert len(devices) == 10
        assert time.monotonic() - start >= 0.2  # it really waited past call_timeout


# ===========================================================================
# Flow-run context emulation: output, logs, search attribution, plt.show, failure
# ===========================================================================
def test_enter_new_flow_run_output_logs_and_plt_show(bridge_server, fake_blt):
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.close("all")
    url, _ = bridge_server()
    try:
        with real_client(url) as (client, blt):
            with blt.enter_new_flow_run(name="child") as run:
                assert run.flow_run_id is not None
                blt.output["result"] = 42
                blt.info("hi from child")
                blt.search_devices()               # attributed to the child run
                plt.figure(1)
                plt.plot([1, 2, 3])
                plt.show()                          # Agg -> SVG uploaded to the context
                plt.show()                          # unchanged figure -> deduplicated

            ctx = fake_blt.flow_run_contexts()[-1]
            assert ctx.exited == "success"
            assert ctx.output["result"] == 42
            assert ("info", "hi from child") in ctx.logs
            assert ctx.searched is not None        # search routed through the context
            assert len(ctx.visualizations) == 1
            builder = ctx.visualizations[0]
            # The client sends the raw matplotlib figure number as figure_id (an int
            # on the real client, see bridge/balthazar_remote.py); compare as text so
            # the identity of figure 1 is what we assert.
            assert str(builder.figure_id) == "1"
            assert builder.type is fake_blt.VisualizationDataType.SVG
            assert builder.filename.endswith(".svg")
    finally:
        plt.close("all")


def test_enter_new_flow_run_exception_fails_and_reraises(bridge_server, fake_blt):
    url, _ = bridge_server()
    with real_client(url) as (_client, blt):
        with pytest.raises(ValueError):
            with blt.enter_new_flow_run(name="boom"):
                raise ValueError("kaboom")
        ctx = fake_blt.flow_run_contexts()[-1]
        assert ctx.exited[0] == "error"
        assert ctx.exited[1] == "RuntimeError"      # server rebuilds it as a RuntimeError
        assert "kaboom" in ctx.exited[2]


# ===========================================================================
# Watchdog: a tiny idle_timeout reclaims a caller that stops heartbeating
# ===========================================================================
def test_watchdog_reclaims_silent_caller(bridge_server, fake_blt):
    url, mod = bridge_server(idle_timeout=0.3, call_timeout=5)
    stop = threading.Event()
    threading.Thread(target=mod._watchdog_loop, args=(stop,), daemon=True).start()
    try:
        # A raw caller that never heartbeats: enter a context, then go silent.
        c = Client(url, OWNER)
        ref = c.op({"op": "call", "path": ["new_flow_run_context"], "args": [],
                    "kwargs": {"name": "wd"}})["result"]["ref"]
        assert c.op({"op": "enter", "ref": ref})["ok"]
        ctx = fake_blt.flow_run_contexts()[-1]

        deadline = time.time() + 5
        while ctx.exited is None and time.time() < deadline:
            time.sleep(0.05)

        assert ctx.exited[0] == "error" and ctx.exited[1] == "RuntimeError"
        assert "silent" in ctx.exited[2]
        assert OWNER not in mod._callers               # the caller was reclaimed
        orphan = c.op({"op": "get", "ref": ref, "path": ["name"]})
        assert orphan["ok"] is False and orphan["error"]["type"] == "LookupError"
    finally:
        stop.set()


# ===========================================================================
# Release batching reaches the server and frees the refs
# ===========================================================================
def test_release_batching_frees_refs_on_the_server(bridge_server, fake_blt):
    url, mod = bridge_server()
    with real_client(url, RELEASE_INTERVAL_S=60.0) as (_client, blt):
        devices = blt.search_devices()
        refs = {device._ref for device in devices}
        assert refs
        state = mod._callers[OWNER]
        assert refs <= set(state.refs)                 # server is holding them

        del devices
        gc.collect()
        blt._session._releaser.flush()                 # synchronous release op

        assert not (refs & set(state.refs))            # ... and now it isn't


# ===========================================================================
# Tunnel namespace: device cache, get_<index>_devices, query paging, write-through
# ===========================================================================
def test_tunnel_device_cache_indexes_and_query_paging(bridge_server, fake_blt, tmp_path):
    url, _ = bridge_server(
        device_indexes=json.dumps({"wafer": "hierarchy.wafer"}),
        device_cache_dir=str(tmp_path / "cache"),
    )
    with real_client(url) as (client, blt):
        status = blt.tunnel.refresh_device_cache(wait=True)   # load synchronously
        assert status["state"] == "ready"

        # get_<index>_devices generated from the describe'd device_indexes
        cached = blt.get_wafer_devices("SECRET_wafer_17")
        assert len(cached) == 1
        only = cached[0]
        assert isinstance(only, client.CachedDevice)
        assert only.id == "dev-chip-1"
        assert only.params["resistance"]["value"] == pytest.approx(123.456789)
        with pytest.raises(AttributeError):
            only.name = "nope"                                # read-only
        live = only.live()                                    # writable Device
        assert live.id == "dev-chip-1" and live._cls == "Device"

        # cached_devices_query pages transparently (tiny budget -> one record/page)
        records = blt.tunnel.cached_devices_query(max_bytes=1)
        assert len(records) == 10
        assert all(isinstance(r, dict) for r in records)

        # write-through: a param write on a cached device updates the server cache
        chip = blt.search_devices(id=["dev-chip-1"])[0]
        newparams = dict(chip.params)
        newparams["yield_pct"] = 777.0
        chip.params = newparams
        again = blt.tunnel.cached_devices_query(device_type="Chip")
        record = next(r for r in again if r["id"] == "dev-chip-1")
        assert record["params"]["yield_pct"] == 777.0


# ===========================================================================
# blt_analytics integration through the real drop-in against the test server
# ===========================================================================
@pytest.fixture
def dropin(monkeypatch, tmp_path):
    """Point ``blt_analytics._blt.get_blt`` at the real drop-in, connected to the
    running test server over the login-free test transport. Yields a factory."""
    import blt_analytics._blt as blt_locator
    from blt_analytics import schema

    def _activate(url, user=OWNER):
        monkeypatch.setenv("BLT_BRIDGE_TEST_BASE_URL", url)
        monkeypatch.setenv("BLT_BRIDGE_TEST_USER", user)
        monkeypatch.setenv("BLT_ANALYTICS_CACHE_DIR", str(tmp_path / "frames-cache"))
        schema.reset()
        module = blt_locator.get_blt()                 # triggers the lazy connect
        module._load_remote_module().TUNNEL_POLL_SECONDS = 0.02
        return module

    yield _activate
    schema.reset()
    blt_locator._reset_test_bridges()


def test_dropin_get_blt_classifies_as_bridge(bridge_server, fake_blt, dropin):
    import blt_analytics._blt as blt_locator

    url, _ = bridge_server()
    module = dropin(url)
    assert module.__balthazar_tunnel__ == 3
    assert blt_locator.is_tunnel() is True
    desc = blt_locator.describe_info()
    assert desc.get("owner") == OWNER and desc.get("protocol") == 3


def test_dropin_devices_df_and_runs_df(bridge_server, fake_blt, dropin):
    from blt_analytics import frames

    url, _ = bridge_server()
    dropin(url)

    devices = frames.devices_df()                      # reflection paging path
    assert len(devices) == 10
    for col in ("id", "name", "type", "fabrication_date", "tags"):
        assert col in devices.columns
    assert "resistance.value" in devices.columns

    runs = frames.runs_df()
    assert len(runs) == 10
    assert "run_id" in runs.columns and "status" in runs.columns
    assert set(runs["status"]) - {
        "FINISHED", "FAILED", "RUNNING", "KILLED", "ABORTED",
        "PREPARING", "READY", "FAILED_TO_START",
    } == set()


def test_dropin_schema_overview_via_space_schema(bridge_server, fake_blt, dropin):
    from blt_analytics import schema

    url, _ = bridge_server(device_indexes=json.dumps({"wafer": "hierarchy.wafer"}))
    dropin(url)

    out = schema.overview()                            # tunnel.space_schema -> digest
    assert out["version"] == 1
    assert out["totals"]["devices"] == 10
    assert "Chip" in out["device_types"]
    # device indexes come from the describe payload (names + paths only)
    assert out["device_indexes"] == {"wafer": "hierarchy.wafer"}


def test_dropin_doctor_reports_bridge(bridge_server, fake_blt, dropin, tmp_path, capsys):
    from blt_analytics import cli

    home = tmp_path / "home"
    home.mkdir()
    (home / ".balthazar_bridge.json").write_text("{}")
    token = home / ".config" / "balthazar" / "remote.json"
    token.parent.mkdir(parents=True)
    token.write_text(json.dumps({"entry": "t"}))

    url, _ = bridge_server()
    dropin(url)

    project = tmp_path / "proj"
    project.mkdir()
    cli.run_doctor(project=str(project), home=str(home))
    out = capsys.readouterr().out
    assert "[PASS] bridge: reflection bridge" in out
    assert "[PASS] describe:" in out and OWNER in out
    assert "[PASS] space_schema:" in out


# ===========================================================================
# MCP: a non-interactive LoginRequired is produced and converted to a clear error
# ===========================================================================
def test_mcp_noninteractive_login_required(monkeypatch, tmp_path):
    """With no test transport and no token cache, the real client raises
    ``LoginRequired`` non-interactively (no prompt, no network), and the MCP guard
    turns exactly that exception into an actionable ``blt-tunnel connect`` error."""
    from blt_analytics import mcp_server

    client = _load_client()
    # Avoid the real token cache and the network site-discovery probe.
    monkeypatch.setattr(client, "TOKEN_CACHE", tmp_path / "no-token.json")
    monkeypatch.setattr(client._Session, "_find_site",
                        lambda self, site: ("https://core", "https://auth"))
    monkeypatch.setenv(client.NONINTERACTIVE_ENV, "1")

    app_url = "https://host/app-tunnel/%s/%s/?space_id=s" % ("a" * 36, "b" * 36)
    with pytest.raises(client.LoginRequired):
        client.connect(app_url, login="device", interactive=False)

    # The MCP server converts a LoginRequired from a schema call into a tool error.
    monkeypatch.setattr(mcp_server, "_tunnel_unavailable", lambda: None)

    def boom():
        raise client.LoginRequired("run blt-tunnel connect in a terminal")

    result = mcp_server._guard(boom)
    assert "blt-tunnel connect" in result["error"]
    assert result.get("hint") == "run blt-tunnel connect"
