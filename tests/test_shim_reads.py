"""Tests for the v2 shim's read-only client functions.

These load ``session_tunnel/balthazar.py`` by path and monkeypatch its ``_call``
with a tiny in-process fake RPC, so the tests exercise exactly the client's job:
passing kwargs through to the right op, chunking large requests, and parsing the
wire records into the light read-only classes (timestamps back to datetime,
``FlowRun.device_ids``, ``status`` as a plain string, decoded visualization bytes).
No tunnel, no network.
"""

from __future__ import annotations

import base64
import datetime
import importlib.util
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
SHIM_PATH = os.path.join(REPO, "session_tunnel", "balthazar.py")


@pytest.fixture
def shim():
    """Load the v2 shim fresh under a private name (never ``balthazar``)."""
    spec = importlib.util.spec_from_file_location("blt_session_shim_under_test", SHIM_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _capture(monkeypatch, shim, result):
    """Patch ``shim._call`` to record (op, kwargs) and return ``result``."""
    captured = {}

    def fake_call(op, **kwargs):
        captured["op"] = op
        captured["kwargs"] = kwargs
        return result

    monkeypatch.setattr(shim, "_call", fake_call)
    return captured


# ---------------------------------------------------------------------------
# search_flows
# ---------------------------------------------------------------------------


def test_search_flows_passthrough_and_parse(shim, monkeypatch):
    record = {
        "id": "flow-1", "name": "IV sweep", "description": "d", "branch": "main",
        "script_filename": "iv.py", "tags": ["t"],
        "created_time": "2025-01-01T09:00:00+00:00", "username": "ava",
        "parameters": {"bias_max_v": {"type": "float", "default": 1.0, "description": "max"}},
    }
    captured = _capture(monkeypatch, shim, [record])

    [flow] = shim.search_flows(name="IV*", tags=["t"], limit=5, offset=2)
    assert captured["op"] == "search_flows"
    assert captured["kwargs"] == {
        "name": "IV*", "flow_ids": None, "tags": ["t"], "limit": 5, "offset": 2,
    }
    assert isinstance(flow, shim.Flow)
    assert flow.id == "flow-1"
    assert flow.branch == "main"
    assert flow.parameters["bias_max_v"]["type"] == "float"
    assert isinstance(flow.created_time, datetime.datetime)
    assert flow.created_time.year == 2025


# ---------------------------------------------------------------------------
# search_flow_run_history (op name differs from the function name)
# ---------------------------------------------------------------------------


def test_search_flow_run_history_passthrough_and_parse(shim, monkeypatch):
    record = {
        "id": "run-1", "flow_id": "flow-1", "flow_name": "IV sweep", "status": "FINISHED",
        "created_time": "2025-07-01T10:00:00+00:00",
        "started_time": "2025-07-01T10:00:00+00:00", "finished_time": None,
        "username": "ava", "tags": [], "comment": None,
        "device_ids": ["dev-chip-1"], "params": {"bias_max_v": 1.0},
        "output": {"r_zero_ohm": 2.0}, "visualization_ids": ["viz-1"],
    }
    captured = _capture(monkeypatch, shim, [record])

    [run] = shim.search_flow_run_history(flow_id="flow-1", device_id="dev-chip-1", limit=10)
    # The shim function mirrors the real API name but drives the server's op name.
    assert captured["op"] == "search_flow_runs"
    assert captured["kwargs"] == {
        "flow_id": "flow-1", "device_id": "dev-chip-1", "flow_run_ids": None,
        "limit": 10, "offset": 0,
    }
    assert isinstance(run, shim.FlowRun)
    assert run.device_ids == ["dev-chip-1"]      # ids, not Device objects
    assert run.status == "FINISHED"              # a plain string
    assert isinstance(run.created_time, datetime.datetime)
    assert run.finished_time is None
    assert run.params == {"bias_max_v": 1.0}
    assert run.output == {"r_zero_ohm": 2.0}
    assert run.visualization_ids == ["viz-1"]
    assert run.name == "IV sweep"


# ---------------------------------------------------------------------------
# fetch_visualizations: chunk into 20s, decode bytes
# ---------------------------------------------------------------------------


def test_fetch_visualizations_chunks_and_decodes(shim, monkeypatch):
    chunk_sizes = []

    def fake_call(op, **kwargs):
        assert op == "fetch_visualizations"
        chunk = kwargs["ids"]
        chunk_sizes.append(len(chunk))
        return [
            {
                "id": vid, "type": "SVG", "filename": f"{vid}.svg", "flow_run_id": "r",
                "timestamp": "2025-07-01T11:00:00+00:00",
                "data_b64": base64.b64encode(f"svg-{vid}".encode()).decode("ascii"),
            }
            for vid in chunk
        ]

    monkeypatch.setattr(shim, "_call", fake_call)

    ids = [f"viz-{n}" for n in range(45)]
    out = shim.fetch_visualizations(ids)

    assert chunk_sizes == [20, 20, 5]            # batched into chunks of 20
    assert len(out) == 45
    assert isinstance(out["viz-0"], shim.Visualization)
    assert out["viz-0"].data == b"svg-viz-0"     # decoded bytes
    assert isinstance(out["viz-0"].timestamp, datetime.datetime)


def test_fetch_visualizations_empty_makes_no_call(shim, monkeypatch):
    calls = []
    monkeypatch.setattr(shim, "_call", lambda op, **kw: calls.append(kw) or [])
    assert shim.fetch_visualizations([]) == {}
    assert calls == []


def test_fetch_visualizations_accepts_bare_string(shim, monkeypatch):
    seen = []

    def fake_call(op, **kwargs):
        seen.append(kwargs["ids"])
        return []

    monkeypatch.setattr(shim, "_call", fake_call)
    shim.fetch_visualizations("viz-9")
    assert seen == [["viz-9"]]


# ---------------------------------------------------------------------------
# search_devices projection kwargs pass through the shim only
# ---------------------------------------------------------------------------


def test_search_devices_passes_projection_kwargs(shim, monkeypatch):
    captured = _capture(monkeypatch, shim, [])
    shim.search_devices(type="Chip", keys=["resistance"], scalars_only=True, include_params=False)
    assert captured["op"] == "search_devices"
    assert captured["kwargs"]["type"] == "Chip"
    assert captured["kwargs"]["keys"] == ["resistance"]
    assert captured["kwargs"]["scalars_only"] is True
    assert captured["kwargs"]["include_params"] is False


def test_search_devices_defaults_are_the_full_record(shim, monkeypatch):
    record = {
        "id": "d1", "name": "n", "type": "Chip", "description": None,
        "fabrication_date": None, "tags": [], "params": {"a": 1},
    }
    captured = _capture(monkeypatch, shim, [record])
    devices = shim.search_devices()
    # Defaults ask for the full record, matching pre-analytics behaviour.
    assert captured["kwargs"]["keys"] is None
    assert captured["kwargs"]["scalars_only"] is False
    assert captured["kwargs"]["include_params"] is True
    assert isinstance(devices[0], shim.Device)
    assert devices[0].id == "d1"


# ---------------------------------------------------------------------------
# tunnel_space_schema
# ---------------------------------------------------------------------------


def test_tunnel_space_schema_passthrough(shim, monkeypatch):
    captured = _capture(monkeypatch, shim, {"version": 1, "totals": {}})
    digest = shim.tunnel_space_schema(refresh=True)
    assert captured["op"] == "space_schema"
    assert captured["kwargs"] == {"refresh": True}
    assert digest == {"version": 1, "totals": {}}


def test_tunnel_space_schema_defaults_to_cached(shim, monkeypatch):
    captured = _capture(monkeypatch, shim, {})
    shim.tunnel_space_schema()
    assert captured["kwargs"] == {"refresh": False}


# ---------------------------------------------------------------------------
# A trailing 'Z' timestamp parses (Python < 3.11 compatibility)
# ---------------------------------------------------------------------------


def test_timestamp_with_trailing_z_parses(shim):
    parsed = shim._parse_dt("2025-07-01T11:00:00Z")
    assert isinstance(parsed, datetime.datetime)
    assert parsed.utcoffset() == datetime.timedelta(0)


def test_date_only_timestamp_parses(shim):
    parsed = shim._parse_dt("2025-01-03")
    assert isinstance(parsed, datetime.date)


def test_read_classes_are_exported(shim):
    for name in ("Flow", "FlowRun", "Visualization", "search_flows",
                 "search_flow_run_history", "fetch_visualizations", "tunnel_space_schema"):
        assert name in shim.__all__


# ---------------------------------------------------------------------------
# Device cache (spec §6): generic accessor, dynamic get_<index>_devices,
# status/refresh passthrough, transparent query paging, long timeout
# ---------------------------------------------------------------------------


def _device_record(did, wafer="W1"):
    return {"id": did, "name": did, "type": "Die", "description": None,
            "fabrication_date": None, "tags": [], "params": {"wafer": wafer}}


def test_cached_devices_passthrough_and_long_timeout(shim, monkeypatch):
    seen = {}

    def fake_call(op, **kwargs):
        if op == "device_cache_status":
            return {"state": "ready"}
        seen["op"] = op
        seen["kwargs"] = kwargs
        return [_device_record("die-1")]

    monkeypatch.setattr(shim, "_call", fake_call)

    [dev] = shim.cached_devices("wafer", "W1", refresh=True)
    assert seen["op"] == "cached_devices"
    assert seen["kwargs"]["index"] == "wafer"
    assert seen["kwargs"]["value"] == "W1"
    assert seen["kwargs"]["refresh"] is True
    # Calls that may trigger a 250k load use the long (1800s) timeout.
    assert seen["kwargs"]["_timeout"] == shim._LOAD_TIMEOUT_S
    assert isinstance(dev, shim.Device)
    assert dev.id == "die-1"


def test_loading_notice_printed_when_cache_cold(shim, monkeypatch, capsys):
    def fake_call(op, **kwargs):
        if op == "device_cache_status":
            return {"state": "loading"}
        return []

    monkeypatch.setattr(shim, "_call", fake_call)
    shim.cached_devices("wafer", "W1")
    assert "loading device cache" in capsys.readouterr().err


def test_dynamic_get_index_devices_accessor(shim, monkeypatch):
    seen = {}

    def fake_call(op, **kwargs):
        if op == "ping":
            return {"flow_id": "f", "flow_name": "n", "session_id": "s",
                    "flow_run_id": "r", "owner": None, "depth": 0, "stack": [],
                    "device_indexes": {"wafer": "hierarchy.wafer"}}
        if op == "device_cache_status":
            return {"state": "ready"}
        if op == "cached_devices":
            seen["kwargs"] = kwargs
            return [_device_record("die-9")]
        raise AssertionError(op)

    monkeypatch.setattr(shim, "_call", fake_call)

    accessor = shim.get_wafer_devices           # resolved via module __getattr__
    assert callable(accessor)
    [dev] = accessor("W123", refresh=True)
    assert seen["kwargs"]["index"] == "wafer"
    assert seen["kwargs"]["value"] == "W123"
    assert seen["kwargs"]["refresh"] is True
    assert dev.id == "die-9"
    assert "tunnel-only" in shim.get_wafer_devices.__doc__.lower()


def test_unadvertised_index_accessor_raises_attribute_error(shim, monkeypatch):
    monkeypatch.setattr(shim, "_call", lambda op, **k: {"device_indexes": {"wafer": "p"}})
    with pytest.raises(AttributeError):
        _ = shim.get_bogus_devices


def test_dir_includes_dynamic_accessors(shim, monkeypatch):
    monkeypatch.setattr(shim, "_call", lambda op, **k: {"device_indexes": {"wafer": "p"}})
    assert "get_wafer_devices" in dir(shim)


def test_device_cache_status_and_refresh_passthrough(shim, monkeypatch):
    seen = []

    def fake_call(op, **kwargs):
        seen.append((op, kwargs))
        return {"state": "ready"}

    monkeypatch.setattr(shim, "_call", fake_call)
    assert shim.device_cache_status() == {"state": "ready"}
    shim.refresh_device_cache(wait=False)
    ops = [op for op, _ in seen]
    assert "device_cache_status" in ops
    # refresh forwards wait and uses the long timeout.
    refresh = next(kw for op, kw in seen if op == "refresh_device_cache")
    assert refresh["wait"] is False
    assert refresh["_timeout"] == shim._LOAD_TIMEOUT_S


def test_tunnel_cached_devices_query_pages_transparently(shim, monkeypatch):
    monkeypatch.setattr(shim, "_QUERY_PAGE_SIZE", 2)
    records = [_device_record(f"die-{i}") for i in range(5)]

    def fake_call(op, **kwargs):
        if op == "device_cache_status":
            return {"state": "ready"}
        assert op == "cached_devices_query"
        offset, limit = kwargs["offset"], kwargs["limit"]
        return {"devices": records[offset:offset + limit], "total": len(records),
                "offset": offset, "limit": limit}

    monkeypatch.setattr(shim, "_call", fake_call)
    out = shim.tunnel_cached_devices_query(device_type="Die", keys=["wafer"])
    assert [d.id for d in out] == [f"die-{i}" for i in range(5)]
    assert all(isinstance(d, shim.Device) for d in out)


def test_device_cache_functions_exported(shim):
    for name in ("cached_devices", "device_cache_status", "refresh_device_cache",
                 "tunnel_cached_devices_query"):
        assert name in shim.__all__
