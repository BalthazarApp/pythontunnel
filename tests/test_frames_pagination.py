"""``runs_df`` paging *orchestration* tests (ported from Planckian's
``test_flow_run_pagination``).

The generic shrink/skip/abandon/dedupe/offset/cap matrix is tested once against the
shared pager in ``test_paging.py``. What remains here is frames-specific: the
per-flow fan-out, the global-fallback shape, the ``max_runs`` cap warning, the
chunked frame build, and the empty-history contract columns.

These need full control over page results, failures and run counts, so they inject
a purpose-built fake ``balthazar`` module (not the fixture's ``fake_blt``) via
``sys.modules`` — the same technique ``conftest`` uses for the real-module fake.
``blt_analytics._blt.get_blt`` then returns it (no ``__balthazar_tunnel__`` marker,
so it is treated as a real Runner module and no projection kwargs are passed).
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pandas as pd
import pytest

from blt_analytics import frames


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeUUID:
    """A flow id that is not a str but is str()-able (the loader must str() it)."""

    def __init__(self, value):
        self._value = value

    def __str__(self):
        return self._value


class FakeRun:
    """A flow-run object exposing exactly what ``_run_identity`` / flattening read."""

    def __init__(self, run_id, flow_id="F", flow_name="flow", status="FINISHED",
                 params=None, output=None):
        self.id = run_id
        self.flow_id = flow_id
        self.flow_name = flow_name
        self.status = status
        self.created_time = None
        self.started_time = None
        self.finished_time = None
        self.username = None
        self.tags = []
        self.devices = []
        self.device_ids = []
        self.visualization_ids = []
        self.params = dict(params or {})
        self.output = dict(output or {})


class FakeBackend:
    """In-memory stand-in for the blt read API the loader touches.

    ``flows_runs`` is ``[(flow_id, flow_name, [FakeRun, ...]), ...]``. ``fail`` is an
    optional ``callable(flow_key, offset, limit) -> bool``; when True the fetch
    raises (a page too large to decode). ``flows_error`` makes ``search_flows`` raise.
    """

    def __init__(self, flows_runs, *, flows_error=None, fail=None):
        self.infos: list = []
        self.warns: list = []
        self.calls: list = []
        self.flows_error = flows_error
        self.fail = fail
        self._flows = [SimpleNamespace(id=fid, name=name) for (fid, name, _) in flows_runs]
        self._runs = {str(fid): list(runs) for (fid, _, runs) in flows_runs}

    def search_devices(self, *a, **k):
        return []

    def search_flows(self, *a, **k):
        if self.flows_error is not None:
            raise self.flows_error
        return list(self._flows)

    def search_flow_run_history(self, *, flow_id=None, limit=None, offset=0, **k):
        key = str(flow_id) if flow_id is not None else None
        self.calls.append(SimpleNamespace(flow_id=key, offset=offset, limit=limit))
        if self.fail is not None and self.fail(key, offset, limit):
            raise RuntimeError(f"decode cap (flow={key}, offset={offset}, limit={limit})")
        if key is None:
            runs = [r for lst in self._runs.values() for r in lst]
        else:
            runs = self._runs.get(key, [])
        return list(runs[offset : offset + (limit or 0)])

    def info(self, message):
        self.infos.append(message)

    def warn(self, message):
        self.warns.append(message)

    def error(self, message):
        pass


def mk(prefix, n, flow_id, flow_name):
    return [FakeRun(f"{prefix}{i}", flow_id=flow_id, flow_name=flow_name) for i in range(n)]


@pytest.fixture
def install(monkeypatch, tmp_path):
    monkeypatch.setenv("BLT_ANALYTICS_CACHE_DIR", str(tmp_path))

    def _install(backend):
        module = types.ModuleType("balthazar")
        for name in (
            "search_devices", "search_flows", "search_flow_run_history",
            "info", "warn", "error",
        ):
            setattr(module, name, getattr(backend, name))
        monkeypatch.setitem(sys.modules, "balthazar", module)
        return backend

    return _install


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_per_flow_fanout_collects_all_and_keeps_offsets_shallow(install):
    be = install(FakeBackend([
        (FakeUUID("A"), "flow_a", mk("a", 5, "A", "flow_a")),
        ("B", "flow_b", mk("b", 3, "B", "flow_b")),
        (FakeUUID("C"), "flow_c", mk("c", 7, "C", "flow_c")),
    ]))

    df = frames.runs_df()

    assert len(df) == 15
    assert set(df["run_id"]) == (
        {f"a{i}" for i in range(5)} | {f"b{i}" for i in range(3)} | {f"c{i}" for i in range(7)}
    )
    assert all(c.flow_id is not None for c in be.calls)  # per-flow path, never global
    sizes = {"A": 5, "B": 3, "C": 7}
    for c in be.calls:
        assert c.offset <= sizes[c.flow_id]


def test_consecutive_failures_abandon_flow_without_aborting_others(install):
    be = install(FakeBackend([
        ("BAD", "flow_bad", mk("bad", 4, "BAD", "flow_bad")),
        ("GOOD", "flow_good", mk("good", 4, "GOOD", "flow_good")),
    ], fail=lambda key, off, lim: key == "BAD"))

    df = frames.runs_df()

    ids = set(df["run_id"])
    assert not any(i.startswith("bad") for i in ids)  # BAD abandoned
    assert ids == {f"good{i}" for i in range(4)}  # GOOD untouched
    assert any("abandoning this flow" in m for m in be.warns)


def test_max_runs_cap_stops_and_warns(install):
    be = install(FakeBackend([
        ("A", "flow_a", mk("a", 10, "A", "flow_a")),
        ("B", "flow_b", mk("b", 10, "B", "flow_b")),
        ("C", "flow_c", mk("c", 10, "C", "flow_c")),
    ]))

    df = frames.runs_df(max_runs=15)

    assert 15 <= len(df) <= 15 + 250  # cap respected within one page's overshoot
    assert any("stopped at max_runs=15" in m for m in be.warns)


def test_fallback_to_global_when_search_flows_raises(install):
    be = install(FakeBackend([
        ("A", "flow_a", mk("a", 4, "A", "flow_a")),
        ("B", "flow_b", mk("b", 3, "B", "flow_b")),
    ], flows_error=RuntimeError("search_flows boom")))

    df = frames.runs_df()

    assert len(df) == 7
    assert any(c.flow_id is None for c in be.calls)
    assert any("falling back to global" in m for m in be.warns)


def test_fallback_to_global_when_search_flows_empty(install):
    be = install(FakeBackend([]))
    be._runs["ghost"] = mk("g", 3, "ghost", "ghost")  # reachable only via global paging

    df = frames.runs_df()

    assert set(df["run_id"]) == {f"g{i}" for i in range(3)}
    assert any(c.flow_id is None for c in be.calls)


def test_chunked_build_across_boundary_reconciles_dtypes(install):
    a_runs = [
        FakeRun(f"a{i}", "A", "flow_a", params={"node_id": f"a{i}", "alpha": i},
                output={"x": float(i)})
        for i in range(1500)
    ]
    b_runs = [
        FakeRun(f"b{i}", "B", "flow_b", params={"node_id": f"b{i}", "beta": i},
                output={"y": float(i)})
        for i in range(600)
    ]
    install(FakeBackend([("A", "flow_a", a_runs), ("B", "flow_b", b_runs)]))

    df = frames.runs_df()

    assert len(df) == 2100  # crosses the 2000-record chunk boundary -> concat path
    # Columns seen in only some chunks reconcile to NaN-backed float64.
    assert str(df["param.alpha"].dtype) == "float64"
    assert str(df["param.beta"].dtype) == "float64"
    indexed = df.set_index("run_id")
    assert indexed.loc["a0", "param.alpha"] == 0
    assert pd.isna(indexed.loc["b0", "param.alpha"])  # flow_b rows lack alpha


def test_empty_history_returns_contract_columns(install):
    install(FakeBackend([]))

    df = frames.runs_df()

    assert len(df) == 0
    assert list(df.columns) == frames._RUN_IDENTITY
