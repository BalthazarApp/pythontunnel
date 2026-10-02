"""Tests for the v2 tunnel server's analytics read ops.

These exercise ``flows/tunnel_session_server.py`` directly: the ``fake_blt``
fixture installs the fake *real* balthazar as ``sys.modules["balthazar"]``, we
import the flow module fresh (so its ``import balthazar as blt`` binds the fake,
and its module-level state — the schema cache, stats, context stack — starts
clean), and we call the ``_op_*`` functions by hand. No HTTP server, no executor
thread: the ops are plain functions on the main thread here.

The fake carries no ``__balthazar_tunnel__`` marker, so the flow module's
"this must be the real module" guard passes on import.
"""

from __future__ import annotations

import base64
import importlib.util
import os
import threading
import time

import pytest

from fakes import fixture_space

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
FLOW_PATH = os.path.join(REPO, "flows", "tunnel_session_server.py")


@pytest.fixture
def server(fake_blt):
    """A freshly imported flow module, with the fake balthazar already installed.

    Depends on ``fake_blt`` so the injection happens first; a fresh module per
    test keeps the ``space_schema`` cache and the ``_stats`` counters isolated.
    """
    spec = importlib.util.spec_from_file_location(
        "tunnel_session_server_under_test", FLOW_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Record equality with the canonical wire format (spec §1)
# ---------------------------------------------------------------------------


def _by_id(records):
    return sorted(records, key=lambda r: r["id"])


def test_search_devices_matches_to_records(server, fixture_records):
    got = server._op_search_devices({})
    assert _by_id(got) == _by_id(fixture_records["devices"])


def test_search_flows_matches_to_records(server, fixture_records):
    got = server._op_search_flows({})
    assert _by_id(got) == _by_id(fixture_records["flows"])


def test_search_flow_runs_matches_to_records(server, fixture_records):
    got = server._op_search_flow_runs({})
    assert _by_id(got) == _by_id(fixture_records["runs"])


def test_run_record_collapses_devices_to_ids_and_stringifies_status(server):
    [run] = server._op_search_flow_runs({"flow_run_ids": ["run-f1-001"]})
    # FlowRun.devices (objects) become a list of ids; status is the bare name.
    assert run["device_ids"] == ["dev-chip-1"]
    assert run["status"] == "FINISHED"
    [failed] = server._op_search_flow_runs({"flow_run_ids": ["run-f1-004"]})
    assert failed["status"] == "FAILED"
    assert failed["finished_time"] is None


# ---------------------------------------------------------------------------
# Projection kwargs on search_devices / search_flow_runs
# ---------------------------------------------------------------------------


def test_search_devices_keys_projection(server):
    [full] = server._op_search_devices({"id": ["dev-chip-1"]})
    assert set(full["params"]) == {"hierarchy", "resistance", "serial", "yield_pct", "active"}

    [narrow] = server._op_search_devices({"id": ["dev-chip-1"], "keys": ["resistance", "serial"]})
    assert set(narrow["params"]) == {"resistance", "serial"}
    # Values of the kept keys are untouched.
    assert narrow["params"]["resistance"] == full["params"]["resistance"]


def test_search_devices_scalars_only(server):
    [narrow] = server._op_search_devices({"id": ["dev-chip-1"], "scalars_only": True})
    # hierarchy and resistance are dicts -> dropped; scalars survive.
    assert set(narrow["params"]) == {"serial", "yield_pct", "active"}


def test_search_devices_include_params_false(server):
    [stripped] = server._op_search_devices({"id": ["dev-chip-1"], "include_params": False})
    assert stripped["params"] == {}
    # The identity fields remain.
    assert stripped["id"] == "dev-chip-1"
    assert stripped["name"] == "SECRET_chip_one"
    assert stripped["type"] == "Chip"


def test_search_flow_runs_include_flags(server):
    [run] = server._op_search_flow_runs({"flow_run_ids": ["run-f1-001"]})
    assert run["params"] and run["output"]

    [stripped] = server._op_search_flow_runs(
        {"flow_run_ids": ["run-f1-001"], "include_params": False, "include_output": False}
    )
    assert stripped["params"] == {}
    assert stripped["output"] == {}
    # Identity survives the projection.
    assert stripped["device_ids"] == ["dev-chip-1"]
    assert stripped["status"] == "FINISHED"


# ---------------------------------------------------------------------------
# fetch_visualizations: base64 and the 20-id limit
# ---------------------------------------------------------------------------


def test_fetch_visualizations_base64_roundtrip(server):
    out = server._op_fetch_visualizations({"ids": ["viz-1"]})
    assert len(out) == 1
    rec = out[0]
    assert rec["id"] == "viz-1"
    assert rec["flow_run_id"] == "run-f1-001"
    assert base64.b64decode(rec["data_b64"]) == b"<svg>SECRET_plot_bytes</svg>"


def test_fetch_visualizations_rejects_over_twenty(server):
    with pytest.raises(ValueError):
        server._op_fetch_visualizations({"ids": [f"viz-{i}" for i in range(21)]})
    # Exactly 20 is allowed (even if unknown ids just come back empty).
    assert server._op_fetch_visualizations({"ids": [f"x-{i}" for i in range(20)]}) == []


# ---------------------------------------------------------------------------
# The per-flow pager wiring. The shrink/skip/abandon/dedupe matrix is tested once
# against the shared pager in test_paging.py; here we only check that the server
# wires it up — §1 record conversion, the truncation flag, and its log voice.
# ---------------------------------------------------------------------------


def test_pager_clean_returns_records_and_hit_cap(server):
    records, hit_cap = server._page_flow_runs("flow-2", 10_000)
    assert sorted(r["id"] for r in records) == ["run-f2-001", "run-f2-002", "run-f2-003"]
    # Runs come back as §1 records (dict with the wire keys), not run objects.
    assert all(isinstance(r, dict) and "flow_id" in r for r in records)
    assert hit_cap is False


def test_pager_logs_skips_in_the_tunnel_voice(server, fake_blt):
    fake_blt.fail_run("run-f1-002")
    records, hit_cap = server._page_flow_runs("flow-1", 10_000)
    assert sorted(r["id"] for r in records) == ["run-f1-001", "run-f1-003", "run-f1-004"]
    assert hit_cap is False
    assert any("skipping poison run" in msg for _, msg in fake_blt.logged_messages())


# ---------------------------------------------------------------------------
# space_schema: build, cache, refresh, truncation, import failure
# ---------------------------------------------------------------------------


def test_space_schema_matches_local_digest(server, fixture_records):
    from blt_analytics import digest as digest_mod

    built = server._op_space_schema({})
    local = digest_mod.build_digest(
        fixture_records["devices"],
        fixture_records["flows"],
        fixture_records["runs"],
        built_at=built["built_at"],
    )
    assert built == local


def test_space_schema_cached_until_refresh(server):
    first = server._op_space_schema({})
    second = server._op_space_schema({})
    assert first is second  # memoized: same object

    refreshed = server._op_space_schema({"refresh": True})
    assert refreshed is not first  # rebuilt
    assert refreshed["totals"] == first["totals"]


def test_space_schema_truncation_flags(server):
    digest = server._op_space_schema({"max_runs_per_flow": 1})
    assert digest["totals"]["runs_truncated"] is True
    # Every flow has at least one run, so each is marked truncated.
    for name in ("IV sweep", "Transport map", "Yield audit"):
        assert digest["flows"][name]["truncated"] is True


def test_space_schema_not_truncated_by_default(server):
    digest = server._op_space_schema({})
    assert digest["totals"]["runs_truncated"] is False
    assert digest["flows"]["IV sweep"]["truncated"] is False


def test_space_schema_import_failure_raises_runtimeerror(server, monkeypatch):
    def boom():
        raise ImportError("blt_analytics not installed on this Runner")

    monkeypatch.setattr(server, "_import_digest", boom)
    with pytest.raises(RuntimeError) as excinfo:
        server._op_space_schema({"refresh": True})
    assert "space_schema unavailable" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Reads never take ownership of the context stack
# ---------------------------------------------------------------------------


def test_reads_do_not_take_ownership_while_another_client_owns(server):
    # Pretend another client holds an open context.
    server._stack = [{"ctx": None, "flow_run_id": "run-other", "name": "owned"}]
    server._owner = "other-client"

    # Read ops by a different client succeed and leave ownership untouched.
    assert server._op_search_devices({"client_id": "me"})
    assert server._op_search_flows({"client_id": "me"})
    assert server._op_search_flow_runs({"client_id": "me"}) is not None
    server._op_space_schema({"client_id": "me"})
    assert server._owner == "other-client"
    assert len(server._stack) == 1

    # A context op by that client is rejected — this is the line reads do not cross.
    with pytest.raises(PermissionError):
        server._op_enter_flow_run({"client_id": "me", "name": "x"})


def test_reads_increment_the_stats_counter(server):
    before = server._stats["reads_served"]
    server._op_search_devices({})
    server._op_search_flows({})
    server._op_fetch_visualizations({"ids": ["viz-1"]})
    assert server._stats["reads_served"] == before + 3


def test_device_cache_ops_are_registered(server):
    # Status never loads -> a fast executor job; the loaders are worker-orchestrated so
    # the executor never runs one inline (which would deadlock on its own sub-jobs).
    assert "device_cache_status" in server._DISPATCH
    for op in ("cached_devices", "cached_devices_query", "refresh_device_cache"):
        assert op in server._WORKER_DISPATCH
        assert op not in server._DISPATCH
    assert {"device_cache_status", "cached_devices", "cached_devices_query",
            "refresh_device_cache"} <= server._ALL_OPS


def test_fixture_sanity_numeric_and_secret_literals_present():
    # A guard that the leak-test corpus is non-empty, so other suites that grep
    # these literals are meaningful.
    assert fixture_space.fixture_numeric_values()
    assert fixture_space.fixture_secret_strings()


# ---------------------------------------------------------------------------
# Enum stringification: real Runner PyO3 enums expose only __repr__ (no .name)
# ---------------------------------------------------------------------------


class _ReprOnlyEnum:
    """An enum-like value that mimics a real Runner PyO3 enum: it defines *only*
    ``__repr__`` (so ``str()`` falls back to it, giving ``"<EnumName>.<MEMBER>"``)
    and exposes no ``.name`` attribute. ``__slots__`` makes the missing ``.name``
    a hard ``AttributeError``, so a serializer that reaches for ``.name`` breaks."""

    __slots__ = ("_qualname",)

    def __init__(self, qualname):
        self._qualname = qualname

    def __repr__(self):
        return self._qualname


def test_enum_name_takes_last_segment_and_passes_none(server):
    # "FlowRunStatus.FINISHED" -> "FINISHED"; a bare name is unchanged; None -> None.
    assert server._enum_name(_ReprOnlyEnum("FlowRunStatus.FINISHED")) == "FINISHED"
    assert server._enum_name(_ReprOnlyEnum("VisualizationDataType.SVG")) == "SVG"
    assert server._enum_name("FAILED") == "FAILED"
    assert server._enum_name(None) is None


def test_run_status_uses_repr_only_enum(server):
    class _Run:
        id = "run-x"
        flow_id = "flow-x"
        flow_name = "X"
        status = _ReprOnlyEnum("FlowRunStatus.FINISHED")  # no .name, no bare str()
        created_time = started_time = finished_time = None
        username = comment = None
        tags = []
        device_ids = ["dev-1"]
        params = {}
        output = {}
        visualization_ids = []
        devices = None

    record = server._run_to_dict(_Run())
    # str(status) would be "FlowRunStatus.FINISHED"; _enum_name must reduce it.
    assert record["status"] == "FINISHED"


def test_viz_type_uses_repr_only_enum(server):
    assert server._viz_type_str(_ReprOnlyEnum("VisualizationDataType.PNG")) == "PNG"
    assert server._viz_type_str(None) is None


# ---------------------------------------------------------------------------
# space_schema orchestration: hands each blt.* read over as a discrete main-thread
# job (observed via a synchronous recording runner), and interleaves under threads
# ---------------------------------------------------------------------------


def test_space_schema_hands_work_over_as_discrete_jobs(server, monkeypatch):
    """With no executor thread, the build runs inline — but it still routes every
    blt.* read through ``_call_on_main``. Spy on that seam to prove it submits many
    small jobs (a devices page, the flows fetch, a run page per flow) rather than
    doing the whole pull in one blocking call."""
    calls = []
    real = server._call_on_main

    def spy(fn, **kw):
        calls.append(fn)
        return real(fn, **kw)

    monkeypatch.setattr(server, "_call_on_main", spy)

    digest = server._op_space_schema({})

    # At least: one devices page + the flows fetch + one run page per flow (3).
    assert len(calls) >= 1 + 1 + 3
    assert digest["totals"]["flows"] == 3
    assert digest["totals"]["devices"] == 10
    # The executor was never started, so the reads ran inline on this thread.
    assert not server._executor_live.is_set()


def _spin_until(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return predicate()


def test_other_op_served_while_space_schema_builds(server, fake_blt, monkeypatch):
    """A normal op is served mid-build, through the same ``_JOBS`` queue.

    The build is held in its orchestration (worker thread) after the first flow —
    which parks the *worker*, not the executor — so the executor is free to answer a
    ``ping`` while a space_schema build is still open. No timing races: the probe is
    sent only once the build is provably parked, and released only after it replies.
    """
    ping_released = threading.Event()
    seen_flows = []
    real_page = server._page_flow_runs

    def gated_page(flow_id, max_runs, deadline=None):
        seen_flows.append(flow_id)
        if len(seen_flows) == 2:  # before paging the 2nd flow, wait for the probe
            assert ping_released.wait(timeout=10.0)
        return real_page(flow_id, max_runs, deadline)

    monkeypatch.setattr(server, "_page_flow_runs", gated_page)

    executor = threading.Thread(target=server._run_executor, name="exec", daemon=True)
    executor.start()
    assert _spin_until(server._executor_live.is_set)

    build_done = threading.Event()

    def build():
        try:
            server._op_space_schema({})
        finally:
            build_done.set()

    builder = threading.Thread(target=build, name="build", daemon=True)
    builder.start()

    # Build has paged the first flow and is now parked before the second.
    assert _spin_until(lambda: len(seen_flows) >= 2)
    assert not build_done.is_set()

    # A normal op is served now, mid-build, promptly.
    job = server._Job(op="ping", kwargs={"client_id": "probe"})
    server._JOBS.put(job)
    ok, payload = job.reply.get(timeout=5.0)
    assert ok
    assert payload["depth"] == 0
    assert not build_done.is_set()  # ping did not wait out the whole build

    ping_released.set()  # let the build finish
    assert build_done.wait(timeout=10.0)

    server._stop.set()
    executor.join(timeout=5.0)
    assert _spin_until(lambda: not server._executor_live.is_set())


def test_space_schema_concurrent_builds_do_not_double_build(server, monkeypatch):
    """Two concurrent space_schema calls share one build: the lock makes the second
    wait, and both get the same cached object (the builder runs once)."""
    builds = []
    real_fetch = server._fetch_devices_paged
    gate = threading.Event()

    def slow_fetch(deadline=None):
        builds.append(1)
        gate.wait(timeout=10.0)  # hold the first build inside the lock
        return real_fetch(deadline)

    monkeypatch.setattr(server, "_fetch_devices_paged", slow_fetch)

    results = {}

    def call(tag):
        results[tag] = server._op_space_schema({})

    first = threading.Thread(target=call, args=("a",), daemon=True)
    first.start()
    assert _spin_until(lambda: len(builds) == 1)  # first build is inside the lock

    second = threading.Thread(target=call, args=("b",), daemon=True)
    second.start()
    # Second is blocked on the lock; it must not have started its own fetch.
    time.sleep(0.1)
    assert len(builds) == 1

    gate.set()
    first.join(timeout=5.0)
    second.join(timeout=5.0)

    assert len(builds) == 1  # built exactly once
    assert results["a"] is results["b"]  # same cached object
