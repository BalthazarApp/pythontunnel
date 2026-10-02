"""Focused regression tests for four review findings in ``flows/tunnel_bridge.py``:

1. the watchdog's reclaim decision is race-free (it will not unwind a caller that
   has a job running or that became active under the lock);
2. ``_write_through`` mutates the *current* device store under the cache lock;
3. ``_refresh_index_value`` fetches outside the cache lock and merges concurrent
   write-throughs instead of discarding them;
4. ``cached_devices_query``'s ``limit`` is a *total* cap counted from offset 0.

The race tests are deterministic: they inject the worst-case interleaving through a
monkeypatch (no timing sleeps), so they fail on the unfixed code and pass on the fix.
"""

from __future__ import annotations

import json

from bridge._helpers import OWNER

INDEXES = json.dumps({"by_wafer": {"path": "hierarchy.wafer", "device_type": "Chip"}})


def _chips(count, wafer="W1", pad=0, start=0):
    blob = "p" * pad
    return [
        {"id": f"c{i}", "type": "Chip", "name": f"chip-{i}",
         "params": {"hierarchy": {"wafer": wafer}, "v": i, "blob": blob}}
        for i in range(start, start + count)
    ]


# ---------------------------------------------------------------------------
# Finding 1: watchdog TOCTOU
# ---------------------------------------------------------------------------


def test_watchdog_does_not_reclaim_caller_with_running_job(bridge_server, client_factory):
    """An idle-looking caller whose job is still executing must not be unwound."""
    url, mod = bridge_server(idle_timeout=100)
    state = mod.caller_state("worker")
    state.last_seen -= 10_000  # long past the idle timeout

    job = mod._Job("worker", {"op": "heartbeat"})  # ``done`` is unset == still running
    with state.lock:
        state.jobs["j1"] = job

    mod.reclaim_silent()
    assert "worker" in mod._callers          # the running job blocked reclaim
    assert mod._callers["worker"] is state

    job.done.set()                           # the job finished
    mod.reclaim_silent()
    assert "worker" not in mod._callers      # now it is reclaimed


def test_watchdog_rechecks_last_seen_under_lock_before_reclaim(bridge_server, client_factory, monkeypatch):
    """A request that lands during the watchdog's locked decision (bumping last_seen)
    must be seen by the re-check, so an active caller is never reclaimed."""
    url, mod = bridge_server(idle_timeout=100)
    state = mod.caller_state("active")
    state.last_seen -= 10_000  # looks idle at the top of the sweep

    hits = {"n": 0}

    def arriving_request(st):
        # Stand in for a request arriving at the worst moment: under _callers_lock,
        # inside reclaim's decision, it bumps last_seen exactly as caller_state would.
        hits["n"] += 1
        st.last_seen = mod.time.monotonic()
        return False  # no job running

    monkeypatch.setattr(mod, "_has_running_job", arriving_request)

    mod.reclaim_silent()
    assert hits["n"] == 1
    assert "active" in mod._callers          # the fresh last_seen was re-read; kept
    assert mod._callers["active"] is state


# ---------------------------------------------------------------------------
# Finding 2: _write_through mutates the current store under the lock
# ---------------------------------------------------------------------------


def test_write_through_targets_current_store_under_lock(bridge_server, client_factory, fake_blt, tmp_path, monkeypatch):
    """If the store object is swapped (a concurrent reload) while a write-through is in
    flight, the write must land in the *current* store, not a stale captured one."""
    url, mod = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    fake_blt.set_synthetic_devices(_chips(3, wafer="W1"))
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)

    ref = c.op({"op": "call", "path": ["search_devices"], "args": [],
                "kwargs": {"id": ["c0"]}})["result"][0]["ref"]

    original = mod._device_to_dict
    swapped = {"done": False}

    def swap_store_then_convert(device):
        record = original(device)
        if not swapped["done"] and getattr(device, "id", None) == "c0":
            swapped["done"] = True
            # Simulate a concurrent reload publishing a brand-new store object.
            fresh_records = {rid: dict(r) for rid, r in mod._device_store["records"].items()}
            fresh_index = mod._build_index(fresh_records)
            mod._publish_device_store(fresh_records, fresh_index, mod._device_store["built_at"])
        return record

    monkeypatch.setattr(mod, "_device_to_dict", swap_store_then_convert)

    assert c.op({"op": "set", "ref": ref, "path": ["name"], "value": "renamed"})["ok"]
    assert swapped["done"]

    query = c.tunnel("cached_devices_query", device_type="Chip")
    record = next(d for d in query["devices"] if d["id"] == "c0")
    assert record["name"] == "renamed"       # landed in the swapped-in (current) store


# ---------------------------------------------------------------------------
# Finding 3: _refresh_index_value fetches outside the lock and merges
# ---------------------------------------------------------------------------


def test_refresh_index_value_fetches_outside_lock_and_merges(bridge_server, client_factory, fake_blt, tmp_path, monkeypatch):
    """The per-value refresh must not hold the cache lock across the network fetch, and
    a write-through landing *during* the fetch must be merged, not discarded."""
    url, mod = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    devices = _chips(5, wafer="W1") + [
        {"id": "cx", "type": "Chip", "name": "chip-x",
         "params": {"hierarchy": {"wafer": "W2"}, "v": 42}}
    ]
    fake_blt.set_synthetic_devices(devices)
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)

    devices[0]["params"]["v"] = 999          # c0 changes; the refresh must re-pick it
    fake_blt.set_synthetic_devices(devices)

    original_search = fake_blt.search_devices
    observed = {"lock_free": None, "touched": False}

    def search_with_concurrent_write(**kwargs):
        if kwargs.get("id") and not observed["touched"]:
            got = mod._device_cache_lock.acquire(blocking=False)
            observed["lock_free"] = got      # must be free: refresh fetched outside it
            if got:
                observed["touched"] = True
                # A concurrent write-through on an unrelated (not-refreshed) device.
                mod._device_store["records"]["cx"]["name"] = "touched-mid-fetch"
                mod._device_cache_lock.release()
        return original_search(**kwargs)

    monkeypatch.setattr(fake_blt, "search_devices", search_with_concurrent_write)

    after = c.tunnel("cached_devices", index="by_wafer", value="W1", refresh=True)
    assert observed["lock_free"] is True                 # fetch happened lock-free
    assert {d["id"]: d["params"]["v"] for d in after["devices"]}["c0"] == 999

    w2 = c.tunnel("cached_devices", index="by_wafer", value="W2")
    cx = next(d for d in w2["devices"] if d["id"] == "cx")
    assert cx["name"] == "touched-mid-fetch"             # concurrent write merged, not lost


# ---------------------------------------------------------------------------
# Finding 4: cached_devices_query limit is a total cap from offset 0
# ---------------------------------------------------------------------------


def test_cached_devices_query_limit_is_total_cap(bridge_server, client_factory, fake_blt, tmp_path):
    """``limit`` caps the total number of devices returned across all pages; the client
    re-sends it per page, so paging must end at the cap, not yield limit-per-page."""
    url, _ = bridge_server(device_indexes=INDEXES, device_cache_dir=str(tmp_path))
    fake_blt.set_synthetic_devices(_chips(10, wafer="W1", pad=400))
    c = client_factory(url, OWNER)
    c.tunnel("refresh_device_cache", wait=True)

    # One generous page: the cap trims to 4 and ends paging immediately.
    whole = c.tunnel("cached_devices_query", device_type="Chip", limit=4)
    assert [d["id"] for d in whole["devices"]] == ["c0", "c1", "c2", "c3"]
    assert whole["total"] == 10
    assert whole["next_offset"] is None

    # A tight byte budget forces one record per page; the total across pages is still
    # exactly the cap (the buggy code re-applied limit per page and paged all 10).
    collected, offset, guard = [], 0, 0
    while True:
        guard += 1
        assert guard <= 20, "paging did not stop at the limit"
        page = c.tunnel("cached_devices_query", device_type="Chip",
                        limit=4, offset=offset, max_bytes=300)
        collected.extend(d["id"] for d in page["devices"])
        if page["next_offset"] is None:
            break
        offset = page["next_offset"]
    assert collected == ["c0", "c1", "c2", "c3"]
