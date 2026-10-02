"""Tests for ``blt_analytics.publish``.

``publish`` is exercised against a stub ``blt`` (exposing ``enter_new_flow_run`` and
``output``) injected by monkeypatching ``blt_analytics._blt.get_blt``. The Agg
backend is forced before pyplot is imported so nothing tries to open a window.
``plt.show`` is replaced by a recorder so the test can observe *which* figures were
registered with pyplot at capture time — standing in for the shim's upload hook /
the real Runner backend, both of which render exactly ``plt.get_fignums()``.
"""

from __future__ import annotations

import matplotlib

matplotlib.use("Agg")  # before pyplot import

import matplotlib.pyplot as plt  # noqa: E402
import pytest  # noqa: E402

import blt_analytics._blt as blt_locator  # noqa: E402
from blt_analytics import publish as publish_mod  # noqa: E402
from blt_analytics.publish import publish  # noqa: E402


class _CM:
    """A shim-style context handle, carrying its own ``flow_run_id``."""

    def __init__(self, run_id):
        self.flow_run_id = run_id

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _Ident:
    def __init__(self, id_):
        self.id = id_


class StubBlt:
    """Minimal ``blt`` exposing ``enter_new_flow_run`` and ``output``.

    ``expose_run_id`` picks how the run id is reachable: via the context handle's
    ``flow_run_id`` (the shim) or via the rebound ``blt.flow_run.id`` (a real
    Runner, whose context manager carries no id of its own).
    """

    def __init__(self, *, expose_run_id="cm"):
        self.output: dict = {}
        self.calls: list = []
        self._n = 0
        self._expose = expose_run_id
        self.flow_run = None

    def enter_new_flow_run(self, name=None, *, devices=None, parameters=None, **extra):
        self._n += 1
        run_id = f"run-{self._n}"
        self.calls.append({"name": name, "devices": devices, "parameters": parameters})
        self.flow_run = _Ident(run_id)
        if self._expose == "cm":
            return _CM(run_id)
        cm = _CM(run_id)
        cm.flow_run_id = None  # force the blt.flow_run.id path
        return cm


@pytest.fixture(autouse=True)
def _clean_figures():
    plt.close("all")
    yield
    plt.close("all")


@pytest.fixture
def record_show(monkeypatch):
    """Replace ``plt.show`` with a recorder of the open figure numbers at call time."""
    shown: list = []

    def fake_show(*a, **k):
        shown.append(set(plt.get_fignums()))

    monkeypatch.setattr(plt, "show", fake_show)
    return shown


def _use_stub(monkeypatch, stub):
    monkeypatch.setattr(blt_locator, "get_blt", lambda: stub)


def test_publish_shows_only_given_figures(monkeypatch, record_show):
    fa, _ = plt.subplots()
    fb, _ = plt.subplots()
    fc, _ = plt.subplots()
    stub = StubBlt()
    _use_stub(monkeypatch, stub)

    run_id = publish([fa, fc], "My plots", devices=["d"], parameters={"p": 1}, output={"k": 2})

    assert run_id == "run-1"
    # Only the two requested figures were registered with pyplot during capture.
    assert record_show == [{fa.number, fc.number}]
    # The others survive — nothing was closed or destroyed.
    assert set(plt.get_fignums()) == {fa.number, fb.number, fc.number}


def test_publish_passes_run_metadata_and_output(monkeypatch, record_show):
    fig, _ = plt.subplots()
    stub = StubBlt()
    _use_stub(monkeypatch, stub)

    publish(fig, "Yield", devices=["dev-1"], parameters={"lot": "A"}, output={"median": 0.9})

    assert stub.calls == [{"name": "Yield", "devices": ["dev-1"], "parameters": {"lot": "A"}}]
    assert stub.output == {"median": 0.9}


def test_publish_accepts_a_single_figure(monkeypatch, record_show):
    fig, _ = plt.subplots()
    stub = StubBlt()
    _use_stub(monkeypatch, stub)

    run_id = publish(fig, "One")

    assert run_id == "run-1"
    assert record_show == [{fig.number}]


def test_publish_without_output_does_not_write(monkeypatch, record_show):
    fig, _ = plt.subplots()
    stub = StubBlt()
    _use_stub(monkeypatch, stub)

    publish(fig, "No output")

    assert stub.output == {}


def test_publish_uses_rebound_flow_run_id_on_real_runner(monkeypatch, record_show):
    fig, _ = plt.subplots()
    stub = StubBlt(expose_run_id="flow_run")  # cm has no id; blt.flow_run.id must be used
    _use_stub(monkeypatch, stub)

    run_id = publish(fig, "Real runner")

    assert run_id == "run-1"


def test_publish_simulated_shim_upload_only_sends_targets(monkeypatch):
    """Simulate the shim's hook: ``plt.show`` renders each open figure; assert only
    the targets are captured, and unrelated open figures are left intact."""
    fa, _ = plt.subplots()
    fb, _ = plt.subplots()
    fc, _ = plt.subplots()
    uploaded: list = []

    def shim_show(*a, **k):
        # Mirrors the shim's _render_open_figures: iterate plt.get_fignums().
        for num in plt.get_fignums():
            uploaded.append(num)

    monkeypatch.setattr(plt, "show", shim_show)
    stub = StubBlt()
    _use_stub(monkeypatch, stub)

    publish([fb], "Just B")

    assert uploaded == [fb.number]
    assert set(plt.get_fignums()) == {fa.number, fb.number, fc.number}


def test_publish_rejects_empty_figures(monkeypatch, record_show):
    stub = StubBlt()
    _use_stub(monkeypatch, stub)
    with pytest.raises(ValueError):
        publish([], "Nothing")
