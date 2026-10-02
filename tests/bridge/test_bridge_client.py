"""Client tests against an in-process protocol-3 fake bridge.

Covers SPEC ``Client`` items 1-12 plus the parts protocol (both directions, 410
expiry, a split poll reply). The fake bridge and loaders live in
``tests/bridge/client_fake.py``.
"""

import contextlib
import datetime
import gc
import json
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import client_fake  # noqa: E402

mod = client_fake.remote_module()


@contextlib.contextmanager
def connected(**bridge_opts):
    bridge = client_fake.FakeBridge(**bridge_opts)
    blt = None
    try:
        blt = client_fake.connect(bridge)
        yield bridge, blt
    finally:
        if blt is not None:
            blt.close()
        bridge.close()


# ---------------------------------------------------------------------------
# §1 interactive / LoginRequired / env var, §2 protocol check
# ---------------------------------------------------------------------------
def test_resolve_interactive(monkeypatch):
    monkeypatch.setenv(mod.NONINTERACTIVE_ENV, "1")
    assert mod._resolve_interactive(True) is False
    monkeypatch.delenv(mod.NONINTERACTIVE_ENV, raising=False)
    assert mod._resolve_interactive(True) is True
    assert mod._resolve_interactive(False) is False


def test_login_required_when_noninteractive():
    session = object.__new__(mod._Session)
    session._access_token = None
    session._access_exp = 0.0
    session._access_lifetime = 300.0
    session._refresh_token = None
    session._login_mode = "device"
    session._interactive = False
    with pytest.raises(mod.LoginRequired):
        session._access(0.0)


def test_protocol_check_rejects_wrong_protocol():
    bridge = client_fake.FakeBridge(protocol=2)
    try:
        with pytest.raises(mod.BridgeError) as excinfo:
            client_fake.connect(bridge)
        message = str(excinfo.value)
        assert "protocol 2" in message and "protocol 3" in message
    finally:
        bridge.close()


def test_describe_fields_exposed():
    with connected() as (bridge, blt):
        assert blt.protocol == 3
        assert blt.bridge_version == client_fake.BRIDGE_VERSION
        assert blt.user == bridge.owner
        assert blt.owner == bridge.owner
        assert blt.shared is True


# ---------------------------------------------------------------------------
# basic calls, reflection, params write-through
# ---------------------------------------------------------------------------
def test_basic_call_and_param_write():
    with connected() as (bridge, blt):
        devices = blt.search_devices()
        assert len(devices) == 5
        device = blt.search_devices(id=["dev-000"])[0]
        assert device.params["power_mw"] == 0.0
        device.params.update({"power_mw": 42.0})
        assert device.params["power_mw"] == 42.0  # snapshot refreshed after the write


# ---------------------------------------------------------------------------
# §10 isinstance via metaclass + constant symbols
# ---------------------------------------------------------------------------
def test_isinstance_and_enum_constants():
    with connected() as (bridge, blt):
        device = blt.search_devices()[0]
        assert isinstance(device, blt.Device)
        assert not isinstance(device, blt.FlowRun)
        run = blt.demo_run()
        assert run.status == blt.FlowRunStatus.FINISHED
        assert blt.FlowRunStatus.FINISHED == run.status
        assert run.status != blt.FlowRunStatus.FAILED


# ---------------------------------------------------------------------------
# §9 output primitives
# ---------------------------------------------------------------------------
def test_output_primitive_guard():
    with connected() as (bridge, blt):
        blt.output["r"] = 1.5
        blt.output["label"] = "ok"
        blt.output["flags"] = [1, 2, 3]
        assert blt.output["r"] == 1.5
        assert bridge.backend.output["label"] == "ok"
        for bad in ({"a": 1}, datetime.date(2024, 1, 1), [{"x": 1}]):
            with pytest.raises(TypeError):
                blt.output["bad"] = bad


# ---------------------------------------------------------------------------
# §6 enter_new_flow_run routing + failure + §8 cell source
# ---------------------------------------------------------------------------
def test_enter_new_flow_run_routes_to_context():
    with connected() as (bridge, blt):
        with blt.enter_new_flow_run(name="child") as run:
            assert run.flow_run_id is not None
            blt.output["result"] = 42
            blt.info("hi from child")
            blt.search_devices()
        ctx = bridge.backend.contexts[-1]
        assert bridge.backend.closed_contexts[-1][1] == "FINISHED"
        assert ctx.output["result"] == 42
        assert ("info", "hi from child", ctx._id) in bridge.backend.messages
        assert ctx.search_calls == 1
        # module output untouched by the context write
        assert "result" not in bridge.backend.output


def test_enter_new_flow_run_failure_marks_failed():
    with connected() as (bridge, blt):
        with pytest.raises(ValueError):
            with blt.enter_new_flow_run(name="boom"):
                raise ValueError("kaboom")
        _, status, error = bridge.backend.closed_contexts[-1]
        assert status == "FAILED"
        assert "kaboom" in error


def test_cell_source_logged_on_enter():
    with connected() as (bridge, blt):
        cell = type("Cell", (), {"raw_cell": "blt.output['a'] = 1  # notebook cell"})()
        blt._capture_cell(cell)
        with blt.enter_new_flow_run(name="c"):
            pass
        assert any("notebook cell" in message for _, message, _ in bridge.backend.messages)

        # turning it off stops the source from being shipped
        blt.log_cell_source = False
        blt._capture_cell(type("Cell", (), {"raw_cell": "secret_source"})())
        with blt.enter_new_flow_run(name="c2"):
            pass
        assert not any("secret_source" in message for _, message, _ in bridge.backend.messages)


def test_nesting_is_sibling_only():
    with connected() as (bridge, blt):
        with blt.enter_new_flow_run(name="outer"):
            with blt.enter_new_flow_run(name="inner"):
                assert blt.parents() == []   # sibling-only: no ancestor reported
                assert blt.parent() is None


# ---------------------------------------------------------------------------
# §7 plt.show capture (context + module fallback) + dedup
# ---------------------------------------------------------------------------
def test_plt_show_capture_and_dedup():
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.close("all")
    try:
        with connected() as (bridge, blt):
            with blt.enter_new_flow_run(name="plots"):
                plt.figure(1)
                plt.plot([1, 2, 3])
                plt.show()
                ctx = bridge.backend.contexts[-1]
                assert len(ctx.stored_visualizations) == 1
                builder = ctx.stored_visualizations[0]
                assert isinstance(builder, client_fake.VisualizationBuilder)
                assert builder.figure_id == 1  # real int fignum, matching the stub type
                assert builder.type is client_fake.VisualizationDataType.SVG
                assert builder.filename.endswith(".svg")
                plt.show()  # unchanged figure -> deduplicated
                assert len(ctx.stored_visualizations) == 1
    finally:
        plt.close("all")


def test_plt_show_module_fallback():
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.close("all")
    try:
        with connected() as (bridge, blt):
            plt.figure()
            plt.plot([3, 1, 2])
            plt.show()  # no open context -> figure goes to the bridge run (module)
            assert len(bridge.backend.module_visualizations) == 1
    finally:
        plt.close("all")


def test_show_hook_survives_context_close():
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.close("all")
    try:
        with connected() as (bridge, blt):
            with blt.enter_new_flow_run(name="ctx"):
                plt.figure(1)
                plt.plot([1, 2, 3])
                plt.show()
            plt.close("all")
            # context closed: the hook must stay installed so a later module-level
            # plt.show() is still captured (to the bridge run). Without the fix the
            # hook is torn down here and this capture produces nothing.
            plt.figure(2)
            plt.plot([3, 2, 1])
            plt.show()
            assert len(bridge.backend.module_visualizations) == 1
    finally:
        plt.close("all")


def test_figure_label_used_in_filename():
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.close("all")
    try:
        with connected() as (bridge, blt):
            with blt.enter_new_flow_run(name="labeled"):
                fig = plt.figure("myplot")
                plt.plot([1, 2, 3])
                plt.show()
                builder = bridge.backend.contexts[-1].stored_visualizations[-1]
                assert builder.filename == "myplot.svg"  # label, via fig.get_label()
                # figure_id is the real int fignum, consistent with the stored name
                assert builder.figure_id == fig.number
                assert isinstance(builder.figure_id, int)
    finally:
        plt.close("all")


def test_figure_id_is_int_fignum():
    matplotlib = pytest.importorskip("matplotlib")
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.close("all")
    try:
        with connected() as (bridge, blt):
            with blt.enter_new_flow_run(name="fid"):
                plt.figure(7)
                plt.plot([1, 2, 3])
                plt.show()
                builder = bridge.backend.contexts[-1].stored_visualizations[-1]
                assert builder.figure_id == 7
                assert isinstance(builder.figure_id, int)
                assert builder.filename == "figure_7.svg"  # unlabeled -> fignum name
    finally:
        plt.close("all")


# ---------------------------------------------------------------------------
# §3/§4 heartbeat + release batching
# ---------------------------------------------------------------------------
def test_heartbeat_thread_and_op():
    with connected() as (bridge, blt):
        assert blt._session._hb_thread is not None
        assert blt._session._hb_thread.is_alive()
        blt._session.invoke({"op": "heartbeat"})
        assert bridge.heartbeats >= 1


def test_release_batching_frees_refs():
    with connected() as (bridge, blt):
        devices = blt.search_devices()
        refs = {device._ref for device in devices}
        assert refs
        del devices
        gc.collect()
        blt._session._releaser.flush()
        released = set(bridge.released_refs(bridge.owner))
        assert refs <= released


# ---------------------------------------------------------------------------
# §11 tunnel namespace: paging, CachedDevice, .live(), indexes, polling
# ---------------------------------------------------------------------------
def test_tunnel_cached_devices_paging_and_live():
    bridge = client_fake.FakeBridge()
    bridge.backend.page_size = 2
    try:
        blt = client_fake.connect(bridge)
        cached = blt.tunnel.cached_devices("wafer", "W1")
        assert len(cached) == 3  # dev-000..002
        assert all(isinstance(item, mod.CachedDevice) for item in cached)
        assert isinstance(cached[0].fabrication_date, (datetime.datetime, datetime.date))
        with pytest.raises(AttributeError):
            cached[0].name = "nope"  # read-only
        live = cached[0].live()
        assert live.id == cached[0].id
        assert live._cls == "Device"
        blt.close()
    finally:
        bridge.close()


def test_tunnel_cached_devices_query_returns_plain_records():
    bridge = client_fake.FakeBridge()
    bridge.backend.page_size = 2
    try:
        blt = client_fake.connect(bridge)
        records = blt.tunnel.cached_devices_query(device_type="sample")
        assert len(records) == 5
        assert all(isinstance(record, dict) for record in records)
        blt.close()
    finally:
        bridge.close()


def test_tunnel_cached_devices_polls_while_loading(monkeypatch):
    monkeypatch.setattr(mod, "TUNNEL_POLL_SECONDS", 0.0)
    bridge = client_fake.FakeBridge()
    bridge.backend.cache_ready_after = 3  # two loading replies, then ready
    try:
        blt = client_fake.connect(bridge)
        cached = blt.tunnel.cached_devices("wafer", "W2")
        assert len(cached) == 2
        blt.close()
    finally:
        bridge.close()


def test_tunnel_space_schema_polls_until_ready(monkeypatch):
    monkeypatch.setattr(mod, "TUNNEL_POLL_SECONDS", 0.0)
    bridge = client_fake.FakeBridge()
    bridge.backend.schema_ready_after = 3
    try:
        blt = client_fake.connect(bridge)
        schema = blt.tunnel.space_schema()
        assert schema["state"] == "ready"
        assert schema["digest"]["device_count"] == 5
        blt.close()
    finally:
        bridge.close()


def test_tunnel_status_calls_return_without_polling(monkeypatch):
    # A long poll interval would hang the call if status replies were polled; the
    # fix returns them raw because their state IS the answer.
    monkeypatch.setattr(mod, "TUNNEL_POLL_SECONDS", 999.0)
    with connected() as (bridge, blt):
        status = blt.tunnel.device_cache_status()
        assert status["state"] == "empty"          # loading-looking, but returned as-is
        refreshed = blt.tunnel.refresh_device_cache()
        assert refreshed["state"] == "ready"


def test_tunnel_refresh_sent_only_on_first_page():
    bridge = client_fake.FakeBridge()
    bridge.backend.page_size = 1  # 3 matching records -> 3 pages for wafer W1
    try:
        blt = client_fake.connect(bridge)
        cached = blt.tunnel.cached_devices("wafer", "W1", refresh=True)
        assert len(cached) == 3
        # refresh rebuilds the cache; only the first page asks for it
        assert bridge.backend.cache_refresh_calls == [True, False, False]
        blt.close()
    finally:
        bridge.close()


def test_dynamic_get_index_devices_accessor():
    with connected() as (bridge, blt):
        cached = blt.get_wafer_devices("W2")
        assert len(cached) == 2
        assert all(isinstance(item, mod.CachedDevice) for item in cached)
        with pytest.raises(AttributeError):
            blt.get_missing_devices  # unknown index -> module get -> not a function


# ---------------------------------------------------------------------------
# §12 remote error mapping
# ---------------------------------------------------------------------------
def test_remote_error_mapping_owner_gets_traceback():
    with connected() as (bridge, blt):
        with pytest.raises(KeyError) as excinfo:
            blt.boom()
        assert isinstance(excinfo.value, mod.RemoteError)
        assert excinfo.value.remote_type == "KeyError"
        assert excinfo.value.remote_traceback


def test_remote_error_non_owner_gets_no_traceback():
    with connected() as (bridge, blt):
        other = client_fake.connect(bridge, user="someone-else")
        try:
            with pytest.raises(KeyError) as excinfo:
                other.boom()
            assert excinfo.value.remote_traceback is None
        finally:
            other.close()


# ---------------------------------------------------------------------------
# Large payloads: the parts protocol (SPEC "Large payloads")
# ---------------------------------------------------------------------------
def test_parts_download_multipart():
    with connected() as (bridge, blt):
        data = blt.noise(5 * 1024 * 1024)
        assert isinstance(data, bytes)
        assert len(data) == 5 * 1024 * 1024


def test_parts_roundtrip_large_upload_and_download():
    payload = os.urandom(3 * 1024 * 1024)
    with connected() as (bridge, blt):
        echoed = blt.echo(payload)
        assert echoed == payload


def test_parts_410_on_expired_download():
    bridge = client_fake.FakeBridge(expire_downloads=True)
    try:
        blt = client_fake.connect(bridge)
        with pytest.raises(mod.BridgeError):
            blt.noise(5 * 1024 * 1024)
        blt.close()
    finally:
        bridge.close()


# ---------------------------------------------------------------------------
# §2 pending -> poll, including a poll reply that is itself split
# ---------------------------------------------------------------------------
def test_pending_poll_roundtrip(monkeypatch):
    monkeypatch.setattr(mod, "POLL_NOTICE_SECONDS", 0.0)
    with connected(pending_funcs=("slow",), extra_polls=1) as (bridge, blt):
        assert blt.slow("hello") == "hello"


def test_pending_poll_reply_is_split():
    payload = os.urandom(3 * 1024 * 1024)
    with connected(pending_funcs=("slow",)) as (bridge, blt):
        result = blt.slow(payload)
        assert result == payload


# ---------------------------------------------------------------------------
# §5 profile save / load / env override, connect_from_profile
# ---------------------------------------------------------------------------
def test_save_profile_is_0600_and_tokenless(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "BRIDGE_PROFILE", tmp_path / "bridge.json")
    path = mod.save_profile(
        "https://host/app-tunnel/a/b/?space_id=s", "device", site="https://core",
    )
    assert (os.stat(path).st_mode & 0o777) == 0o600
    data = json.loads(path.read_text())
    assert "token" not in json.dumps(data).lower()
    assert data["app_url"].endswith("space_id=s")
    assert data["login"] == "device"
    assert data["site"] == "https://core"


def test_profile_app_url_env_override(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "BRIDGE_PROFILE", tmp_path / "bridge.json")
    mod.save_profile("https://host/app-tunnel/a/b/?space_id=s", "device")
    monkeypatch.delenv(mod.BRIDGE_URL_ENV, raising=False)
    assert mod._profile_app_url(mod.load_profile()).endswith("space_id=s")
    monkeypatch.setenv(mod.BRIDGE_URL_ENV, "https://override/app")
    assert mod._profile_app_url(mod.load_profile()) == "https://override/app"


def test_dropin_marker_and_delegation(monkeypatch):
    dropin = client_fake.load_dropin()
    assert dropin.__balthazar_tunnel__ == 3
    bridge = client_fake.FakeBridge()
    try:
        remote_mod = dropin._load_remote_module()
        monkeypatch.setattr(
            remote_mod, "connect_from_profile",
            lambda *a, **k: remote_mod.connect(
                _test_base_url=bridge.base_url, _test_user_id=bridge.owner,
            ),
        )
        # first attribute access connects lazily and then delegates to the Remote
        assert len(dropin.search_devices()) == 5
        assert dropin.user == bridge.owner
        dropin._get_remote().close()
    finally:
        bridge.close()


def test_connect_from_profile_uses_test_hooks(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "BRIDGE_PROFILE", tmp_path / "bridge.json")
    mod.save_profile("https://host/app-tunnel/a/b/?space_id=s", "device")
    bridge = client_fake.FakeBridge()
    try:
        blt = mod.connect_from_profile(
            _test_base_url=bridge.base_url, _test_user_id=bridge.owner,
        )
        assert blt.user == bridge.owner
        blt.close()
    finally:
        bridge.close()
