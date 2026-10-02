"""One shrink-and-retry pager for Balthazar flow-run history (stdlib only).

Three call sites page a flow's run history the same robust way — the v2 tunnel
server's ``space_schema`` builder, :mod:`blt_analytics.schema`'s local-build
fallback, and :mod:`blt_analytics.frames`'s ``runs_df`` loader. That logic
(shrink-and-retry, poison-run skip, abandon-after-N, dedupe by id, offset by raw
page length) used to be copied into each. It lives here once instead.

The pager is deliberately generic and dependency-free: it knows nothing about
``blt``, records, filtering or logging. The caller supplies a ``fetch_page``
closure (how to get one page) and an optional ``on_skip`` callback (how to log, in
its own voice), and converts/filters the returned run objects afterwards.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

__all__ = ["page_flow_runs", "PageRetry"]

DEFAULT_PAGE_SIZE = 250
MAX_CONSECUTIVE_SKIPS = 3


@dataclass(frozen=True)
class PageRetry:
    """A recovery step the pager took, handed to ``on_skip`` for logging.

    ``kind`` is one of:

    * ``"shrink"`` — a fetch raised with size ``> 1`` (a page too large to return);
      ``size`` is the reduced size the *same* ``offset`` is retried with.
    * ``"skip"`` — a fetch raised at size 1 (a single poison run); it is skipped and
      ``offset`` advanced past it.
    * ``"abandon"`` — the size-1 failures reached :data:`MAX_CONSECUTIVE_SKIPS`;
      paging stops and returns what was gathered.

    ``offset`` is the offset in play when the step was taken and ``error`` the
    exception that triggered it.
    """

    kind: str
    offset: int
    size: int
    error: BaseException


def _run_id(run: Any):
    """A run's identity for dedupe: its ``.id`` attribute (or mapping ``id`` key)."""
    rid = getattr(run, "id", None)
    if rid is None and isinstance(run, dict):
        rid = run.get("id")
    return run if rid is None else rid


def page_flow_runs(
    fetch_page: Callable[[int, int], Any],
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_runs: int,
    on_skip: Optional[Callable[[PageRetry], None]] = None,
) -> tuple[list, bool]:
    """Page a run history robustly, returning ``(runs, truncated)``.

    ``fetch_page(offset, limit)`` returns one page of run objects (each carrying an
    ``.id``). Runs are deduped by ``.id`` and returned in first-seen order; the
    offset advances by the **raw** page length so it stays aligned with the backend
    even when a page repeats ids.

    On a fetch that raises: with the current size ``> 1`` the page is treated as too
    large — the size is cut to ``size // 4`` and the *same* offset retried; at size 1
    the lone run is a poison record — it is skipped and the offset advanced; after
    :data:`MAX_CONSECUTIVE_SKIPS` consecutive size-1 failures the run history is
    abandoned (returning what was gathered, ``truncated=False``). A clean fetch
    resets both the size and the consecutive-failure count.

    ``truncated`` is True only when ``max_runs`` was reached. ``on_skip`` (optional)
    is called with a :class:`PageRetry` for every shrink, skip and abandon so the
    caller can log; the pager itself never logs.
    """
    runs_by_id: dict = {}
    offset = 0
    size = page_size
    consecutive = 0
    while True:
        try:
            page = list(fetch_page(offset, size))
        except Exception as exc:  # noqa: BLE001 - recovery is the pager's whole job
            if size > 1:
                size = max(1, size // 4)
                if on_skip is not None:
                    on_skip(PageRetry("shrink", offset, size, exc))
                continue
            consecutive += 1
            if consecutive >= MAX_CONSECUTIVE_SKIPS:
                if on_skip is not None:
                    on_skip(PageRetry("abandon", offset, size, exc))
                return list(runs_by_id.values()), False
            if on_skip is not None:
                on_skip(PageRetry("skip", offset, size, exc))
            offset += 1
            size = page_size
            continue

        consecutive = 0
        if not page:
            return list(runs_by_id.values()), False
        for run in page:
            runs_by_id[_run_id(run)] = run
        offset += len(page)  # RAW length — dedupe must not perturb the offset
        if len(runs_by_id) >= max_runs:
            return list(runs_by_id.values()), True
        if len(page) < size:
            return list(runs_by_id.values()), False
        size = page_size  # grow back after a clean fetch
