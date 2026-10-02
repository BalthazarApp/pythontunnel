"""Tests for the shared flow-run pager, :mod:`blt_analytics.paging`.

The shrink-and-retry / poison-skip / abandon / dedupe / cap matrix is tested here
once, against the generic ``page_flow_runs`` with a hand-built ``fetch_page``. The
three call sites (tunnel server, ``schema`` local build, ``frames.runs_df``) carry
only thin wiring tests of their own — record conversion, log voice, filtering and
the cap→truncation plumbing — in their respective suites.
"""

from __future__ import annotations

from types import SimpleNamespace

from blt_analytics import paging


class Run:
    """A minimal run object: all the pager needs is an ``.id``."""

    def __init__(self, rid):
        self.id = rid


def pager(pages, *, fail=None, page_size=250, max_runs=10_000, record=None):
    """Drive ``page_flow_runs`` over an in-memory backend.

    ``pages`` is the full ordered list of runs; ``fetch`` serves ``[offset:offset+limit]``.
    ``fail(offset, limit) -> bool`` injects a raising fetch. ``record`` (a list)
    collects the fetch ``(offset, limit)`` calls.
    """
    def fetch(offset, limit):
        if record is not None:
            record.append((offset, limit))
        if fail is not None and fail(offset, limit):
            raise RuntimeError(f"injected fault at offset={offset} limit={limit}")
        return pages[offset : offset + limit]

    return paging.page_flow_runs(fetch, page_size=page_size, max_runs=max_runs)


# ---------------------------------------------------------------------------
# Clean paging, offsets and dedupe
# ---------------------------------------------------------------------------


def test_clean_single_page():
    runs, truncated = pager([Run("a"), Run("b"), Run("c")])
    assert [r.id for r in runs] == ["a", "b", "c"]
    assert truncated is False


def test_empty_history():
    runs, truncated = pager([])
    assert runs == []
    assert truncated is False


def test_offsets_advance_by_raw_page_length():
    calls: list = []
    runs, truncated = pager([Run(f"r{i}") for i in range(300)], page_size=250, record=calls)
    assert len(runs) == 300
    assert calls == [(0, 250), (250, 250)]  # raw length advance, then a short page ends it
    assert truncated is False


def test_dedupe_by_id_within_and_across_pages():
    calls: list = []
    # page 0 -> [a, b], page 2 -> [b, c] (b repeats), page 4 -> []
    pages = [Run("a"), Run("b"), Run("b"), Run("c")]
    runs, truncated = pager(pages, page_size=2, record=calls)
    assert sorted(r.id for r in runs) == ["a", "b", "c"]
    assert [r.id for r in runs].count("b") == 1
    assert calls == [(0, 2), (2, 2), (4, 2)]
    assert truncated is False


# ---------------------------------------------------------------------------
# Shrink-and-retry
# ---------------------------------------------------------------------------


def test_shrink_and_retry_same_offset_then_succeeds():
    calls: list = []
    # Only the first full-size fetch fails; the shrunk retry at the same offset works.
    fail = lambda off, lim: off == 0 and lim == 250  # noqa: E731
    runs, truncated = pager([Run("a"), Run("b")], page_size=250, fail=fail, record=calls)
    assert [r.id for r in runs] == ["a", "b"]
    assert calls[0] == (0, 250)
    assert calls[1] == (0, 62)  # 250 // 4, same offset
    assert truncated is False


def test_on_skip_reports_shrink_with_new_size():
    events: list = []

    def fetch(offset, limit):
        if limit > 1:
            raise RuntimeError("too large")  # force a shrink down to size 1
        return []  # size 1 succeeds with an empty page -> paging ends cleanly

    paging.page_flow_runs(fetch, page_size=4, max_runs=10, on_skip=events.append)
    shrinks = [e for e in events if e.kind == "shrink"]
    assert shrinks and all(e.offset == 0 for e in shrinks)
    assert shrinks[0].size == 1  # 4 // 4 == 1, the reduced size is reported


# ---------------------------------------------------------------------------
# Poison-run skip and abandon
# ---------------------------------------------------------------------------


def test_poison_run_is_isolated_and_skipped():
    events: list = []
    pages = [Run("a"), Run("bad"), Run("c")]
    # Any fetch whose returned slice would include "bad" raises.
    def fetch(offset, limit):
        page = pages[offset : offset + limit]
        if any(r.id == "bad" for r in page):
            raise RuntimeError("poison")
        return page

    runs, truncated = paging.page_flow_runs(fetch, page_size=250, max_runs=10, on_skip=events.append)
    assert sorted(r.id for r in runs) == ["a", "c"]
    assert truncated is False
    assert any(e.kind == "skip" for e in events)
    assert not any(e.kind == "abandon" for e in events)


def test_abandon_after_three_consecutive_size_one_failures():
    events: list = []

    # Every fetch raises -> shrink to 1, then size-1 failures: skip, skip, abandon.
    def fetch(offset, limit):
        raise RuntimeError("always")

    runs, truncated = paging.page_flow_runs(fetch, page_size=250, max_runs=10, on_skip=events.append)
    assert runs == []
    assert truncated is False
    kinds = [e.kind for e in events]
    assert kinds.count("skip") == 2  # two skips, then abandon on the third size-1 failure
    assert kinds[-1] == "abandon"


# ---------------------------------------------------------------------------
# max_runs cap -> truncated
# ---------------------------------------------------------------------------


def test_max_runs_cap_sets_truncated():
    runs, truncated = pager([Run(f"r{i}") for i in range(500)], page_size=250, max_runs=250)
    assert truncated is True
    assert len(runs) >= 250  # the page that crossed the cap is kept whole


def test_cap_not_hit_leaves_truncated_false():
    runs, truncated = pager([Run(f"r{i}") for i in range(10)], max_runs=10_000)
    assert len(runs) == 10
    assert truncated is False


def test_abandon_returns_truncated_false_not_cap():
    # Abandon must not masquerade as a cap hit.
    def fetch(offset, limit):
        raise RuntimeError("always")

    runs, truncated = paging.page_flow_runs(fetch, page_size=4, max_runs=1)
    assert runs == []
    assert truncated is False
