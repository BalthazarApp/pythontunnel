"""Tests for the server-side device cache (spec §6).

These drive ``flows/tunnel_session_server.py`` directly: the ``fake_blt`` fixture
installs the fake *real* balthazar as ``sys.modules["balthazar"]``, we import the
flow module fresh (so its module-level cache state starts clean), point its cache
dir at a tmp path, and call the ``_op_*`` / helper functions by hand. No HTTP
server; the ops are plain functions on the main thread, and ``_call_on_main`` runs
each ``blt.*`` read inline (no executor thread) unless a test starts one.
"""

from __future__ import annotations

import importlib.util
import json
import os
import threading
import time

import pytest

from fakes import fixture_space

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
FLOW_PATH = os.path.join(REPO, "flows", "tunnel_session_server.py")

WAFER = {"wafer": {"path": "hierarchy.wafer", "device_type": None}}


def _fresh_module():
    spec = importlib.util.spec_from_file_location("tunnel_dc_under_test", FLOW_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def server(fake_blt, tmp_path):
    """A freshly imported flow module with its device-cache dir under tmp_path."""
    module = _fresh_module()
    module._device_cache_dir = str(tmp_path / "cache")
    return module


def _configure(server, indexes):
    server._device_indexes = dict(indexes)


# synthetic device records ---------------------------------------------------


def _die(i, wafer, x=0):
    return {
        "id": f"die-{i:06d}", "type": "Die", "name": f"die {i}",
        "fabrication_date": None, "description": None, "tags": [],
        "params": {"hierarchy": {"wafer": wafer}, "x": x},
    }


# ---------------------------------------------------------------------------
# Config parsing
# ---------------------------------------------------------------------------


def test_parse_device_indexes_string_and_object_forms(server):
    parsed = server._parse_device_indexes(
        '{"wafer": "hierarchy.wafer", "die": {"path": "hierarchy.die", "device_type": "Die"}}'
    )
    assert parsed["wafer"] == {"path": "hierarchy.wafer", "device_type": None}
    assert parsed["die"] == {"path": "hierarchy.die", "device_type": "Die"}


def test_parse_device_indexes_bad_json_logs_error_and_returns_empty(server, fake_blt):
    assert server._parse_device_indexes("{not json") == {}
    assert any(
        level == "error" and "not valid JSON" in msg
        for level, msg in fake_blt.logged_messages()
    )


def test_parse_device_indexes_skips_malformed_entry(server, fake_blt):
    parsed = server._parse_device_indexes('{"ok": "a.b", "bad": {"nopath": 1}}')
    assert parsed == {"ok": {"path": "a.b", "device_type": None}}
    assert any("bad" in msg for level, msg in fake_blt.logged_messages() if level == "error")


# ---------------------------------------------------------------------------
# Index build
# ---------------------------------------------------------------------------


def test_index_build_over_fixture_chips(server):
    _configure(server, WAFER)
    server._reload_device_cache(force=True)
    index = server._device_store["index"]["wafer"]
    # Chip 1/2 carry hierarchy.wafer; chip 3 is missing it, and no other type has it.
    assert index["SECRET_wafer_17"] == ["dev-chip-1"]
    assert index["SECRET_wafer_42"] == ["dev-chip-2"]
    assert "dev-chip-3" not in {i for ids in index.values() for i in ids}


def test_index_build_respects_device_type_restriction(server):
    # Serial exists on Chips only; restricting to a different type yields nothing.
    _configure(server, {"serial": {"path": "serial", "device_type": "Wafer"}})
    server._reload_device_cache(force=True)
    assert server._device_store["index"]["serial"] == {}

    _configure(server, {"serial": {"path": "serial", "device_type": "Chip"}})
    server._reload_device_cache(force=True)
    assert set(server._device_store["index"]["serial"]) == {
        "SECRET_serial_0001", "SECRET_serial_0002", "SECRET_serial_0003"
    }


def test_missing_and_none_values_are_not_indexed(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([
        _die(0, "W1"),
        {"id": "die-1", "type": "Die", "params": {"hierarchy": {"wafer": None}}},
        {"id": "die-2", "type": "Die", "params": {"hierarchy": {}}},
    ])
    server._reload_device_cache(force=True)
    index = server._device_store["index"]["wafer"]
    assert index == {"W1": ["die-000000"]}


# ---------------------------------------------------------------------------
# Cold load
# ---------------------------------------------------------------------------


def test_cold_load_triggered_by_cached_devices(server):
    _configure(server, WAFER)
    assert server._cache_meta["state"] == "empty"

    out = server._op_cached_devices({"index": "wafer", "value": "SECRET_wafer_17"})
    assert server._cache_meta["state"] == "ready"
    assert [d["id"] for d in out] == ["dev-chip-1"]
    assert server._cache_meta["count"] == len(fixture_space.raw_devices())


def test_full_load_pages_at_1000(server, monkeypatch):
    _configure(server, WAFER)
    seen = []
    real = server.blt.search_devices

    def spy(**kwargs):
        seen.append(kwargs)
        return real(**kwargs)

    monkeypatch.setattr(server.blt, "search_devices", spy)
    server._reload_device_cache(force=True)
    # The paged full load uses limit 1000 (spec §6).
    assert seen and all(s.get("limit") == server._DEVICE_CACHE_PAGE_SIZE == 1000 for s in seen)


# ---------------------------------------------------------------------------
# Persistence round-trip + restart
# ---------------------------------------------------------------------------


def test_persistence_round_trip_and_restart(fake_blt, tmp_path, monkeypatch):
    cache_dir = str(tmp_path / "cache")
    params = {"device_indexes": '{"wafer": "hierarchy.wafer"}', "device_cache_dir": cache_dir}

    first = _fresh_module()
    monkeypatch.setattr(first.blt, "params", params, raising=False)
    first._init_device_cache()                 # parses config; no disk yet
    assert first._warm_device_cache is True     # defaults on when indexes configured
    first._reload_device_cache(force=True)      # loads + persists
    built_at = first._cache_meta["built_at"]
    pkl = first._device_cache_path()
    meta = first._device_cache_meta_path()
    assert os.path.exists(pkl) and os.path.exists(meta)

    # File/dir permissions: it holds space data (spec §6).
    assert oct(os.stat(pkl).st_mode & 0o777) == "0o600"
    assert oct(os.stat(os.path.dirname(pkl)).st_mode & 0o777) == "0o700"

    # A fresh "restart": a new module instance loads from disk, no re-fetch.
    second = _fresh_module()
    monkeypatch.setattr(second.blt, "params", params, raising=False)
    monkeypatch.setattr(second.blt, "search_devices",
                        lambda **k: pytest.fail("restart must not re-page devices"))
    second._init_device_cache()
    assert second._cache_meta["state"] == "ready"
    assert second._cache_meta["built_at"] == built_at
    assert second._device_store["records"] == first._device_store["records"]
    assert second._device_store["index"] == first._device_store["index"]


def test_disk_load_rebuilds_index_when_config_changed(server, monkeypatch):
    _configure(server, WAFER)
    server._reload_device_cache(force=True)

    # A fresh module with the SAME records on disk but a DIFFERENT index config must
    # rebuild the index from the records rather than re-fetch.
    other = _fresh_module()
    other._device_cache_dir = server._device_cache_dir
    other._device_indexes = {"serial": {"path": "serial", "device_type": "Chip"}}
    monkeypatch.setattr(other.blt, "search_devices",
                        lambda **k: pytest.fail("must not re-page on a disk load"))
    assert other._load_device_cache_from_disk() is True
    assert set(other._device_store["index"]["serial"]) == {
        "SECRET_serial_0001", "SECRET_serial_0002", "SECRET_serial_0003"
    }


def test_space_id_branch_keys_the_directory(server, monkeypatch):
    # With a module-level blt.space present, the cache dir is keyed by the space id.
    monkeypatch.setattr(server.blt, "space", server.blt._Ident("space-xyz"), raising=False)
    assert server._space_cache_key() == "space-xyz"
    assert server._device_cache_space_dir().endswith(os.path.join("cache", "space-xyz"))


# ---------------------------------------------------------------------------
# Refresh by value: updated / vanished / non-discovery of new
# ---------------------------------------------------------------------------


def test_refresh_by_value_updates_and_drops_vanished(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(0, "W1", x=1), _die(1, "W1", x=2), _die(2, "W2")])
    server._reload_device_cache(force=True)
    assert server._device_store["index"]["wafer"]["W1"] == ["die-000000", "die-000001"]

    # die-0 updated (x changes), die-1 removed, die-3 added anew under W1.
    server.blt.set_synthetic_devices([_die(0, "W1", x=99), _die(2, "W2"), _die(3, "W1")])

    out = server._op_cached_devices({"index": "wafer", "value": "W1", "refresh": True})
    ids = sorted(d["id"] for d in out)
    # Updated kept (with new value), vanished dropped, NEW not discovered by value refresh.
    assert ids == ["die-000000"]
    assert out[0]["params"]["x"] == 99
    assert "die-000001" not in server._device_store["records"]
    assert "die-000003" not in server._device_store["records"]


def test_full_refresh_discovers_new_devices(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(0, "W1")])
    server._reload_device_cache(force=True)
    assert set(server._device_store["index"]["wafer"]["W1"]) == {"die-000000"}

    server.blt.set_synthetic_devices([_die(0, "W1"), _die(3, "W1")])
    # A value refresh cannot see die-3...
    server._op_cached_devices({"index": "wafer", "value": "W1", "refresh": True})
    assert set(server._device_store["index"]["wafer"]["W1"]) == {"die-000000"}
    # ...but a full refresh does.
    server._op_refresh_device_cache({"wait": True})
    assert set(server._device_store["index"]["wafer"]["W1"]) == {"die-000000", "die-000003"}


# ---------------------------------------------------------------------------
# Full refresh: atomic swap on success, keep old on failure
# ---------------------------------------------------------------------------


def test_full_refresh_atomic_swap_on_success(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(0, "W1"), _die(1, "W2")])
    server._reload_device_cache(force=True)
    assert set(server._device_store["records"]) == {"die-000000", "die-000001"}

    server.blt.set_synthetic_devices([_die(2, "W3")])
    status = server._op_refresh_device_cache({"wait": True})
    assert status["state"] == "ready"
    assert set(server._device_store["records"]) == {"die-000002"}


def test_failed_refresh_keeps_old_cache(server, monkeypatch):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(0, "W1"), _die(1, "W2")])
    server._reload_device_cache(force=True)
    good_records = dict(server._device_store["records"])

    def boom(deadline):
        raise RuntimeError("fetch exploded mid-reload")

    monkeypatch.setattr(server, "_fetch_device_cache_pages", boom)
    status = server._op_refresh_device_cache({"wait": True})

    # Old data intact, state back to ready, error recorded in status.
    assert server._device_store["records"] == good_records
    assert status["state"] == "ready"
    assert "fetch exploded" in status["error"]


def test_failed_cold_load_marks_error_and_raises(server, monkeypatch):
    _configure(server, WAFER)

    def boom(deadline):
        raise RuntimeError("cold boom")

    monkeypatch.setattr(server, "_fetch_device_cache_pages", boom)
    with pytest.raises(RuntimeError):
        server._op_cached_devices({"index": "wafer", "value": "W1"})
    assert server._cache_meta["state"] == "error"
    assert "cold boom" in server._cache_meta["error"]


# ---------------------------------------------------------------------------
# Write-through after update_device_params
# ---------------------------------------------------------------------------


def test_write_through_moves_index_entry(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(0, "W1"), _die(1, "W2")])
    server._reload_device_cache(force=True)

    server._op_update_device_params({
        "client_id": "c", "id": "die-000000", "values": {"hierarchy": {"wafer": "W2"}},
    })

    index = server._device_store["index"]["wafer"]
    assert "W1" not in index                       # emptied bucket is dropped
    assert set(index["W2"]) == {"die-000000", "die-000001"}
    assert server._device_store["records"]["die-000000"]["params"]["hierarchy"]["wafer"] == "W2"


def test_write_through_updates_record_without_index_change(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(0, "W1", x=1)])
    server._reload_device_cache(force=True)

    server._op_update_device_params({
        "client_id": "c", "id": "die-000000", "values": {"x": 42},
    })
    assert server._device_store["records"]["die-000000"]["params"]["x"] == 42
    assert server._device_store["index"]["wafer"]["W1"] == ["die-000000"]


def test_write_through_noop_when_cache_not_ready(server):
    _configure(server, WAFER)
    # No load yet: a write must not crash and must not fabricate a cache.
    server._op_update_device_params({
        "client_id": "c", "id": "dev-chip-1", "values": {"serial": "SECRET_serial_X"},
    })
    assert server._cache_meta["state"] == "empty"
    assert server._device_store["records"] == {}


# ---------------------------------------------------------------------------
# Unknown index
# ---------------------------------------------------------------------------


def test_unknown_index_raises_value_error_naming_configured(server):
    _configure(server, WAFER)
    with pytest.raises(ValueError) as excinfo:
        server._op_cached_devices({"index": "nope", "value": "x"})
    assert "nope" in str(excinfo.value)
    assert "wafer" in str(excinfo.value)


# ---------------------------------------------------------------------------
# cached_devices_query: type filter, key projection, paging
# ---------------------------------------------------------------------------


def test_cached_devices_query_type_filter_and_projection(server):
    _configure(server, WAFER)
    server._reload_device_cache(force=True)

    out = server._op_cached_devices_query({"device_type": "Chip", "keys": ["serial"]})
    assert out["total"] == 3
    assert [d["id"] for d in out["devices"]] == ["dev-chip-1", "dev-chip-2", "dev-chip-3"]
    assert all(set(d["params"]) <= {"serial"} for d in out["devices"])


def test_cached_devices_query_paging(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(i, "W1") for i in range(5)])
    server._reload_device_cache(force=True)

    first = server._op_cached_devices_query({"offset": 0, "limit": 2})
    second = server._op_cached_devices_query({"offset": 2, "limit": 2})
    third = server._op_cached_devices_query({"offset": 4, "limit": 2})
    assert first["total"] == second["total"] == 5
    assert [d["id"] for d in first["devices"]] == ["die-000000", "die-000001"]
    assert [d["id"] for d in second["devices"]] == ["die-000002", "die-000003"]
    assert [d["id"] for d in third["devices"]] == ["die-000004"]


# ---------------------------------------------------------------------------
# status + ping
# ---------------------------------------------------------------------------


def test_status_shape_before_and_after_load(server):
    _configure(server, WAFER)
    before = server._op_device_cache_status({})
    assert before["state"] == "empty"
    assert before["indexes"]["wafer"]["path"] == "hierarchy.wafer"
    assert before["persisted_path"].endswith("devices.pkl")

    server._reload_device_cache(force=True)
    after = server._op_device_cache_status({})
    assert after["state"] == "ready"
    assert after["count"] == len(fixture_space.raw_devices())
    assert after["indexes"]["wafer"]["values"] == 2   # two distinct wafer values


def test_ping_advertises_device_indexes(server):
    _configure(server, {"wafer": {"path": "hierarchy.wafer", "device_type": None},
                        "die": {"path": "hierarchy.die", "device_type": "Die"}})
    pong = server._op_ping({})
    assert pong["device_indexes"] == {"wafer": "hierarchy.wafer", "die": "hierarchy.die"}


# ---------------------------------------------------------------------------
# space_schema uses the cache when ready
# ---------------------------------------------------------------------------


def test_space_schema_uses_cache_when_ready(server, monkeypatch):
    _configure(server, WAFER)
    server._reload_device_cache(force=True)

    monkeypatch.setattr(server, "_fetch_devices_paged",
                        lambda deadline=None: pytest.fail("space_schema must use the cache"))
    digest = server._op_space_schema({})
    assert digest["totals"]["devices"] == len(fixture_space.raw_devices())


# ---------------------------------------------------------------------------
# Reads never take ownership of the context stack
# ---------------------------------------------------------------------------


def test_cache_reads_do_not_take_ownership(server):
    _configure(server, WAFER)
    server._reload_device_cache(force=True)
    server._stack = [{"ctx": None, "flow_run_id": "run-other", "name": "owned"}]
    server._owner = "other-client"

    server._op_device_cache_status({"client_id": "me"})
    server._op_cached_devices({"client_id": "me", "index": "wafer", "value": "SECRET_wafer_17"})
    server._op_cached_devices_query({"client_id": "me"})
    server._op_refresh_device_cache({"client_id": "me", "wait": True})

    assert server._owner == "other-client"
    assert len(server._stack) == 1


# ---------------------------------------------------------------------------
# A normal op is served while a device-cache load is in progress
# ---------------------------------------------------------------------------


def _spin_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def test_other_op_served_while_device_cache_loads(server, monkeypatch):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(i, "W1") for i in range(5)])

    started = threading.Event()
    release = threading.Event()
    real = server._fetch_device_cache_pages

    def gated(deadline):
        started.set()
        assert release.wait(timeout=10.0)   # park the load mid-flight
        return real(deadline)

    monkeypatch.setattr(server, "_fetch_device_cache_pages", gated)

    executor = threading.Thread(target=server._run_executor, name="exec", daemon=True)
    executor.start()
    assert _spin_until(server._executor_live.is_set)

    done = threading.Event()

    def build():
        try:
            server._op_cached_devices({"index": "wafer", "value": "W1"})
        finally:
            done.set()

    threading.Thread(target=build, name="build", daemon=True).start()
    assert _spin_until(started.is_set)       # load is parked, holding the build lock
    assert not done.is_set()

    # A normal op is served now, mid-load, through the executor.
    job = server._Job(op="ping", kwargs={"client_id": "probe"})
    server._JOBS.put(job)
    ok, payload = job.reply.get(timeout=5.0)
    assert ok and payload["depth"] == 0
    assert not done.is_set()

    release.set()
    assert done.wait(timeout=10.0)

    server._stop.set()
    executor.join(timeout=5.0)


def test_concurrent_cached_devices_callers_load_once(server, monkeypatch):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(0, "W1")])

    builds = []
    gate = threading.Event()
    real = server._fetch_device_cache_pages

    def slow(deadline):
        builds.append(1)
        gate.wait(timeout=10.0)      # hold the first load inside the build lock
        return real(deadline)

    monkeypatch.setattr(server, "_fetch_device_cache_pages", slow)

    def call():
        server._op_cached_devices({"index": "wafer", "value": "W1"})

    first = threading.Thread(target=call, daemon=True)
    first.start()
    assert _spin_until(lambda: len(builds) == 1)

    second = threading.Thread(target=call, daemon=True)
    second.start()
    time.sleep(0.1)
    assert len(builds) == 1           # second is blocked on the build lock

    gate.set()
    first.join(timeout=5.0)
    second.join(timeout=5.0)
    assert len(builds) == 1           # loaded exactly once; the second reused the cache


# ---------------------------------------------------------------------------
# 20k-device synthetic perf sanity (time-bounded, not flaky)
# ---------------------------------------------------------------------------


def test_perf_sanity_20k_devices(server):
    _configure(server, WAFER)
    n, wafers = 20_000, 50
    server.blt.set_synthetic_devices([_die(i, f"W{i % wafers:03d}") for i in range(n)])

    start = time.monotonic()
    server._reload_device_cache(force=True)
    load_s = time.monotonic() - start

    assert server._cache_meta["count"] == n
    assert len(server._device_store["index"]["wafer"]) == wafers

    start = time.monotonic()
    out = server._op_cached_devices({"index": "wafer", "value": "W007"})
    lookup_s = time.monotonic() - start
    assert len(out) == n // wafers

    # Generous bounds so this catches a pathological regression without being flaky.
    assert load_s < 20.0, f"20k load took {load_s:.1f}s"
    assert lookup_s < 1.0, f"lookup took {lookup_s:.2f}s"


# ---------------------------------------------------------------------------
# Byte-budgeted pages for the cache-backed ops (spec §7). The reply shape stays the
# bare list for old callers; new kwargs opt into the paged dict shape.
# ---------------------------------------------------------------------------


def _big_die(i, wafer, payload_len=5000):
    return {
        "id": f"die-{i:06d}", "type": "Die", "name": f"die {i}",
        "fabrication_date": None, "description": None, "tags": [],
        "params": {"hierarchy": {"wafer": wafer}, "blob": "x" * payload_len},
    }


def test_cached_devices_bare_list_without_new_kwargs(server):
    # No wait/max_bytes/offset -> byte-for-byte the pre-§7 bare-list reply.
    _configure(server, WAFER)
    server._reload_device_cache(force=True)
    out = server._op_cached_devices({"index": "wafer", "value": "SECRET_wafer_17"})
    assert isinstance(out, list)
    assert [d["id"] for d in out] == ["dev-chip-1"]


def test_cached_devices_byte_budget_paging_concatenates_to_full(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_big_die(i, "W1", 5000) for i in range(10)])
    server._reload_device_cache(force=True)

    collected, offset, pages = [], 0, 0
    while True:
        out = server._op_cached_devices(
            {"index": "wafer", "value": "W1", "offset": offset, "max_bytes": 12_000}
        )
        pages += 1
        assert out["state"] == "ready"
        assert 1 <= len(out["devices"]) <= 2  # ~5 KB records, 12 KB budget -> 2/page
        collected.extend(out["devices"])
        if out["next_offset"] is None:
            break
        offset = out["next_offset"]
        assert pages < 50
    assert pages > 1  # the budget really did split it into several pages
    assert [d["id"] for d in collected] == [f"die-{i:06d}" for i in range(10)]
    assert out["total"] == 10


def test_cached_devices_query_byte_budget_paging_concatenates_to_full(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_big_die(i, "W1", 5000) for i in range(10)])
    server._reload_device_cache(force=True)

    collected, offset, pages = [], 0, 0
    while True:
        out = server._op_cached_devices_query({"offset": offset, "max_bytes": 12_000})
        pages += 1
        assert 1 <= len(out["devices"]) <= 2
        assert out["total"] == 10
        collected.extend(out["devices"])
        if out["next_offset"] is None:
            break
        offset = out["next_offset"]
        assert pages < 50
    assert pages > 1
    assert [d["id"] for d in collected] == [f"die-{i:06d}" for i in range(10)]


def test_byte_budget_single_oversize_record_returned_alone(server):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_big_die(0, "W1", 50_000)])
    server._reload_device_cache(force=True)

    # A record far larger than the budget still comes back alone, never wedged.
    q = server._op_cached_devices_query({"max_bytes": 1000})
    assert len(q["devices"]) == 1
    assert q["next_offset"] is None

    c = server._op_cached_devices(
        {"index": "wafer", "value": "W1", "offset": 0, "max_bytes": 1000}
    )
    assert len(c["devices"]) == 1
    assert c["next_offset"] is None


# ---------------------------------------------------------------------------
# cached_devices(wait=False) on a cold cache returns a loading state and does not
# block; the shim polls until ready (spec §7).
# ---------------------------------------------------------------------------


def test_cached_devices_wait_false_returns_loading_without_blocking(server, monkeypatch):
    _configure(server, WAFER)
    server.blt.set_synthetic_devices([_die(i, "W1") for i in range(3)])

    started = threading.Event()
    release = threading.Event()
    real = server._fetch_device_cache_pages

    def gated(deadline):
        started.set()
        assert release.wait(timeout=10.0)  # park the load mid-flight
        return real(deadline)

    monkeypatch.setattr(server, "_fetch_device_cache_pages", gated)

    reply = server._op_cached_devices({"index": "wafer", "value": "W1", "wait": False})
    assert reply["state"] == "loading"
    assert reply["devices"] == []
    assert _spin_until(started.is_set)  # a background load really started

    release.set()
    assert _spin_until(lambda: server._cache_meta["state"] == "ready")

    ready = server._op_cached_devices({"index": "wafer", "value": "W1", "wait": False})
    assert ready["state"] == "ready"
    assert {d["id"] for d in ready["devices"]} == {"die-000000", "die-000001", "die-000002"}
    assert ready["next_offset"] is None
