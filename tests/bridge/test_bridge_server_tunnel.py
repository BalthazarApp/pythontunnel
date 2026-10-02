"""Bridge tunnel-namespace tests: the server-side device cache and space_schema,
driven through the ``tunnel`` op over real HTTP.
"""

from __future__ import annotations

import importlib.util
import json
import os
import time

from bridge._helpers import BRIDGE_PATH, OWNER

INDEXES = json.dumps({"by_wafer": {"path": "hierarchy.wafer", "device_type": "Chip"}})


def _wait(client, name, predicate, tries=100, **kwargs):
    result = client.tunnel(name, **kwargs)
    for _ in range(tries):
        if predicate(result):
            return result
        time.sleep(0.03)
        result = client.tunnel(name, **kwargs)
    return result


def _chips(count, wafer="W1", pad=0):
    blob = "p" * pad
    return [
        {"id": f"c{i}", "type": "Chip", "name": f"chip-{i}",
         "params": {"hierarchy": {"wafer": wafer}, "v": i, "blob": blob}}
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# Load / status
# ---------------------------------------------------------------------------


def test_refresh_loads_cache_and_status_reports_indexes(bridge_server, client_factory, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    c = client_factory(url, OWNER)
    status = c.tunnel("refresh_device_cache", wait=True)
    assert status["state"] == "ready" and status["count"] == 10
    # Two chips carry a wafer value (the third has none), so the index has two values.
    assert status["indexes"]["by_wafer"]["values"] == 2
    assert status["indexes"]["by_wafer"]["path"] == "hierarchy.wafer"


def test_cold_cache_reports_loading_then_becomes_ready(bridge_server, client_factory, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    c = client_factory(url, OWNER)
    first = c.tunnel("cached_devices", index="by_wafer", value="SECRET_wafer_17")
    assert first["state"] == "loading"
    ready = _wait(c, "cached_devices", lambda r: r["state"] == "ready",
                  index="by_wafer", value="SECRET_wafer_17")
    assert ready["state"] == "ready"
    assert [d["id"] for d in ready["devices"]] == ["dev-chip-1"]


def test_unknown_index_is_rejected(bridge_server, client_factory, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)
    reply = c.op({"op": "tunnel", "name": "cached_devices",
                  "args": [], "kwargs": {"index": "nope", "value": "x"}})
    assert reply["ok"] is False and reply["error"]["type"] == "ValueError"


# ---------------------------------------------------------------------------
# Paging (byte budget)
# ---------------------------------------------------------------------------


def test_cached_devices_pages_by_byte_budget(bridge_server, client_factory, fake_blt, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    fake_blt.set_synthetic_devices(_chips(40, wafer="W1", pad=1000))
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)

    collected, offset, pages = [], 0, 0
    while True:
        page = c.tunnel("cached_devices", index="by_wafer", value="W1",
                        offset=offset, max_bytes=5000)
        collected.extend(d["id"] for d in page["devices"])
        pages += 1
        assert page["total"] == 40
        if page["next_offset"] is None:
            break
        offset = page["next_offset"]
    assert pages > 1 and len(collected) == 40 and len(set(collected)) == 40


def test_cached_devices_query_filters_by_type_and_pages(bridge_server, client_factory, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)
    first = c.tunnel("cached_devices_query", device_type="Chip", max_bytes=2_000_000)
    assert first["total"] == 3
    assert {d["type"] for d in first["devices"]} == {"Chip"}
    # keys projection keeps only the requested top-level param keys.
    projected = c.tunnel("cached_devices_query", device_type="Chip", keys=["serial"])
    assert all(set(d["params"]) <= {"serial"} for d in projected["devices"])


# ---------------------------------------------------------------------------
# Refresh-by-value + write-through
# ---------------------------------------------------------------------------


def test_refresh_by_value_repicks_changed_records(bridge_server, client_factory, fake_blt, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    chips = _chips(5, wafer="W1")
    fake_blt.set_synthetic_devices(chips)
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)

    before = c.tunnel("cached_devices", index="by_wafer", value="W1")
    assert {d["params"]["v"] for d in before["devices"]} == {0, 1, 2, 3, 4}

    chips[0]["params"]["v"] = 999
    fake_blt.set_synthetic_devices(chips)
    after = c.tunnel("cached_devices", index="by_wafer", value="W1", refresh=True)
    picked = {d["id"]: d["params"]["v"] for d in after["devices"]}
    assert picked["c0"] == 999


def test_write_through_updates_record_and_moves_index(bridge_server, client_factory, fake_blt, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    fake_blt.set_synthetic_devices(_chips(3, wafer="W1"))
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)

    ref = c.op({"op": "call", "path": ["search_devices"], "args": [], "kwargs": {"id": ["c0"]}})["result"][0]["ref"]

    # A plain attribute write refreshes the cached record in place.
    assert c.op({"op": "set", "ref": ref, "path": ["name"], "value": "renamed"})["ok"]
    query = c.tunnel("cached_devices_query", device_type="Chip")
    record = next(d for d in query["devices"] if d["id"] == "c0")
    assert record["name"] == "renamed"

    # Replacing the indexed param moves the device between buckets.
    assert c.op({"op": "set", "ref": ref, "path": ["params"],
                 "value": {"hierarchy": {"wafer": "W2"}}})["ok"]
    w1 = c.tunnel("cached_devices", index="by_wafer", value="W1")
    w2 = c.tunnel("cached_devices", index="by_wafer", value="W2")
    assert "c0" not in [d["id"] for d in w1["devices"]]
    assert "c0" in [d["id"] for d in w2["devices"]]


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def test_cache_persists_and_reloads_from_disk(bridge_server, client_factory, fake_blt, tmp_path):
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    c = client_factory(url, OWNER)
    status = c.tunnel("refresh_device_cache", wait=True)
    assert os.path.exists(status["persisted_path"])
    assert os.path.exists(os.path.join(os.path.dirname(status["persisted_path"]), "meta.json"))

    # A fresh server instance loads the persisted store without re-fetching.
    spec = importlib.util.spec_from_file_location("tunnel_bridge_reload", BRIDGE_PATH)
    reloaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reloaded)
    reloaded.configure({"device_indexes": INDEXES, "device_cache_dir": str(tmp_path)})
    assert reloaded._load_device_cache_from_disk() is True
    assert reloaded._cache_meta["state"] == "ready"
    assert reloaded._cache_meta["count"] == 10


# ---------------------------------------------------------------------------
# space_schema
# ---------------------------------------------------------------------------


def test_space_schema_builds_in_background_then_ready(bridge_server, client_factory):
    url, _ = bridge_server()
    c = client_factory(url, OWNER)
    started = c.tunnel("space_schema")
    assert started["state"] in ("building", "ready")
    ready = _wait(c, "space_schema", lambda r: r["state"] == "ready")
    assert ready["state"] == "ready"
    digest = ready["digest"]
    assert digest["version"] and "totals" in digest and "flows" in digest


def test_space_schema_reports_error_when_digest_unavailable(bridge_server, client_factory, monkeypatch):
    url, mod = bridge_server()

    def _boom():
        raise ImportError("blt_analytics not installed on this Runner")

    monkeypatch.setattr(mod, "_import_digest", _boom)
    c = client_factory(url, OWNER)
    result = _wait(c, "space_schema", lambda r: r["state"] == "error")
    assert result["state"] == "error"
    assert "blt_analytics" in (result["error"] or "")
