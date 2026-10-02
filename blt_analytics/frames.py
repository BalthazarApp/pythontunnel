"""The pandas data path: devices and flow runs as DataFrames of **real values**.

Where :mod:`blt_analytics.schema` withholds measurements, this module hands them
over — it is the user's own process, and ``frames`` exists to pull the data in and
shape it. It talks to whatever :func:`blt_analytics._blt.get_blt` locates (the v2
tunnel shim or a real Runner module), using only the attributes the two share, and
passes the shim-only projection kwargs (``keys=``/``scalars_only=``) solely when
:func:`blt_analytics._blt.is_tunnel` is true.

Column flattening mirrors :mod:`blt_analytics.digest` so the dotted paths the schema
tools report work verbatim as ``columns=``: a small, stable nested dict flattens to
dotted columns (``hierarchy.lot``); a dict with more than ``MAX_KEYS`` keys (a *map*,
e.g. measurements-by-run) stays whole in one object column at its path, its keys
never spread across columns; lists and matrices stay whole in object columns too.

The flow-run loader is a trimmed port of Planckian's ``prompt_plot_flow`` data
layer — the generic parts only (per-flow paging with shrink-and-retry, poison-run
skip, consecutive-failure abandon, dedupe by id, offset by raw page length, chunked
frame construction, and the ``search_flows`` → per-flow → global-fallback shape).
The Planckian-specific melt (qubits / node_type / DesignData) is left out.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, Sequence

from . import _blt, _compat, cache, paging

__all__ = [
    "devices_df",
    "runs_df",
    "explode_devices",
    "series_to_df",
    "matrix_to_df",
]

# Flatten rules, kept in step with digest.build_digest's defaults.
MAX_KEYS = 50
MAX_DEPTH = 4

# Paging / frame-build tuning, ported from prompt_plot_flow.
_PAGE_SIZE = 250
_CHUNK = 2000

_MISSING = object()

_DEVICE_IDENTITY = ["id", "name", "type", "fabrication_date", "tags"]
_RUN_IDENTITY = [
    "run_id", "flow_id", "flow_name", "status", "created_time", "started_time",
    "finished_time", "duration_s", "username", "tags", "device_ids",
    "visualization_ids",
]


def _pd():
    import pandas as pd

    return pd


# ---------------------------------------------------------------------------
# Small shared helpers
# ---------------------------------------------------------------------------


def _as_dict(obj: Any) -> dict:
    """A plain dict from a dict or a Mapping (e.g. the shim's ``device.params``)."""
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    try:
        return dict(obj)
    except Exception:  # noqa: BLE001 - not mapping-like
        return {}


def _nan():
    return float("nan")


def _info(blt: Any, message: str) -> None:
    try:
        blt.info(message)
    except Exception:  # noqa: BLE001 - logging is best-effort
        pass


def _warn(blt: Any, message: str) -> None:
    try:
        blt.warn(message)
    except Exception:  # noqa: BLE001
        pass


def _extract(container: dict, dotted: str) -> Any:
    """Value at a dotted path, or ``_MISSING`` if any segment is absent or ``None``."""
    cur: Any = container
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return _MISSING
    return _MISSING if cur is None else cur


def _flatten_many(records: Sequence[dict]) -> tuple[list[str], list[dict]]:
    """Flatten a collection of container dicts into aligned ``{path: value}`` rows.

    Returns ``(sorted_paths, rows)`` where ``rows[i]`` holds only the paths present
    (and non-None) in ``records[i]``. The rules match the digest:

    * the top level is always flattened (its keys become depth-1 paths);
    * a nested path whose values are *all* dicts flattens further while the union of
      their keys is ``<= MAX_KEYS`` and depth ``< MAX_DEPTH``; otherwise the dict
      stays whole (a *map* or a depth-capped dict) under that single path;
    * anything else (scalar, list, matrix, or a kind that varies) stays whole under
      its path.
    """
    rows: list[dict] = [dict() for _ in records]
    columns: list[str] = []
    seen: set[str] = set()

    def add_column(path: str, vals_by_idx: dict[int, Any]) -> None:
        if path not in seen:
            seen.add(path)
            columns.append(path)
        for idx, value in vals_by_idx.items():
            rows[idx][path] = value

    def emit(path: str, vals_by_idx: dict[int, Any], depth: int) -> None:
        present = list(vals_by_idx.values())
        if present and all(isinstance(v, dict) for v in present):
            union: set = set()
            for value in present:
                union |= set(value.keys())
            if len(union) > MAX_KEYS or depth >= MAX_DEPTH:
                add_column(path, vals_by_idx)  # map / depth-capped dict: keep whole
                return
            for child_key in sorted(union):
                child = {
                    idx: value[child_key]
                    for idx, value in vals_by_idx.items()
                    if isinstance(value, dict)
                    and child_key in value
                    and value[child_key] is not None
                }
                if child:
                    emit(f"{path}.{child_key}", child, depth + 1)
            return
        add_column(path, vals_by_idx)

    top: set = set()
    for record in records:
        if isinstance(record, dict):
            top |= set(record.keys())

    for key in sorted(top):
        vals = {
            idx: record[key]
            for idx, record in enumerate(records)
            if isinstance(record, dict) and key in record and record[key] is not None
        }
        if vals:
            emit(key, vals, 1)

    return sorted(columns), rows


def _assemble(
    rows: list[dict], identity_cols: list[str], extra_cols: list[str], *, chunked: bool
):
    """Build a DataFrame from row dicts, with identity columns first.

    Columns absent from every row (a projected path with no data) are added as
    all-NaN. ``chunked`` builds the frame in ``_CHUNK``-sized pieces and concatenates
    — the memory-peak mitigation ported from ``_records_to_frame`` — which also
    reconciles columns seen in only some chunks to NaN-backed dtypes.
    """
    pd = _pd()
    ordered = list(identity_cols) + [c for c in extra_cols if c not in identity_cols]

    if not rows:
        return pd.DataFrame({c: pd.Series(dtype="object") for c in ordered})

    if chunked and len(rows) > _CHUNK:
        frames = [pd.DataFrame(rows[i : i + _CHUNK]) for i in range(0, len(rows), _CHUNK)]
        df = pd.concat(frames, ignore_index=True, sort=False)
    else:
        df = pd.DataFrame(rows)

    for col in ordered:
        if col not in df.columns:
            df[col] = _nan()

    tail = [c for c in df.columns if c not in ordered]
    return df[[c for c in ordered if c in df.columns] + tail]


# ---------------------------------------------------------------------------
# devices_df
# ---------------------------------------------------------------------------


def devices_df(
    device_type: Optional[str] = None,
    columns: Optional[Sequence[str]] = None,
    *,
    include_archived: bool = False,
    refresh: bool = False,
    index: Optional[str] = None,
    value: Any = None,
):
    """Devices as one row each, with real param values.

    Identity columns are always present: ``id``, ``name``, ``type``,
    ``fabrication_date`` (``datetime64``) and ``tags`` (object, a list). With
    ``columns=None`` every scalar leaf is flattened to a dotted column and lists /
    maps / leftover dicts stay as object columns. With ``columns=[...]`` only those
    dotted paths are produced; a path with no data anywhere becomes an all-NaN
    column.

    **Server-side device cache (tunnel-only, SPEC §6).** On a space with a huge
    device count, paging ``search_devices`` on every call is slow, so the tunnel
    flow can keep a server-side device cache (keyed by id, with configured indexes).
    Two paths lean on it:

    * ``index=`` + ``value=`` — a shortcut for "every device whose index param
      equals this value", served by ``blt.cached_devices(index, value,
      refresh=refresh)``. The index name comes from the flow's ``device_indexes``
      configuration (``schema.overview()`` lists the configured names/paths). This
      needs the tunnel; on a real Runner it raises a clear error. Note the
      **different ``refresh`` meaning here**: ``refresh=True`` re-fetches only the
      *already-known* device ids for that value and re-indexes them (dropping ids
      that vanished) — it cannot discover brand-new devices of that value. For that,
      reload the whole cache with ``blt.refresh_device_cache()`` first.
    * no ``index``/``value`` — when talking to the tunnel and the device cache
      reports ``state == "ready"``, the whole (optionally type-filtered) set is read
      from the cache via ``cached_devices_query`` instead of being paged. If the
      cache is cold, or the shim is too old to host these ops, the behaviour is
      unchanged (page ``search_devices``).

    Both server-cache paths **bypass the on-disk frame cache** (see
    :func:`_build_devices_df`'s module notes): the server already holds the
    authoritative, write-through copy, so a second local pickle would only risk
    serving a pre-refresh snapshot after ``refresh_device_cache`` or a tunnel param
    write. ``refresh=`` therefore does not touch a local file on these paths; on the
    ``cached_devices_query`` path it is a no-op (reload server-side with
    ``blt.refresh_device_cache()``), and on the ``index=`` path it is forwarded to
    ``cached_devices`` with the per-value meaning above.
    """
    cols = list(columns) if columns is not None else None

    # Index shortcut: one index value's devices, straight from the server cache.
    if index is not None or value is not None:
        if index is None or value is None:
            raise ValueError(
                "devices_df(index=, value=): pass both together "
                "(e.g. index='wafer', value='W123'), or neither."
            )
        return _devices_df_by_index(device_type, cols, index, value, refresh)

    # Fast path: the server-side device cache, when it is ready on the tunnel.
    served = _devices_df_from_server_cache(device_type, cols, refresh)
    if served is not None:
        return served

    # Default path: page search_devices, memoized to disk.
    def build():
        return _build_devices_df(device_type, cols, include_archived)

    key = {
        "device_type": device_type,
        "columns": cols,
        "include_archived": include_archived,
    }
    return cache.cached("devices", build, key=key, refresh=refresh)


def _projection_keys(columns: Optional[Sequence[str]]) -> Optional[list[str]]:
    """Top-level param keys to request for ``columns`` (the pushed-down projection)."""
    if columns is None:
        return None
    return sorted({c.split(".", 1)[0] for c in columns})


def _devices_to_frame(devices, columns):
    """Build the devices frame from already-fetched device objects.

    Shared by every acquisition path (paged ``search_devices``, the
    ``cached_devices_query`` cache read, and the ``cached_devices`` index read):
    identity columns first, then either full flattening (``columns=None``) or the
    requested dotted projection, with ``fabrication_date`` coerced to UTC
    ``datetime64``. Chunked assembly keeps the memory peak bounded on a whole-space
    frame (hundreds of thousands of rows over the device cache).
    """
    pd = _pd()
    identity_rows: list[dict] = []
    params_list: list[dict] = []
    for device in devices:
        identity_rows.append(
            {
                "id": str(getattr(device, "id", "") or ""),
                "name": getattr(device, "name", ""),
                "type": getattr(device, "type", ""),
                "fabrication_date": getattr(device, "fabrication_date", None),
                "tags": list(getattr(device, "tags", []) or []),
            }
        )
        params_list.append(_as_dict(getattr(device, "params", {})))

    if columns is None:
        extra_cols, flat_rows = _flatten_many(params_list)
        rows = [{**ident, **flat} for ident, flat in zip(identity_rows, flat_rows)]
    else:
        extra_cols = list(columns)
        rows = []
        for ident, params in zip(identity_rows, params_list):
            row = dict(ident)
            for col in columns:
                if col in _DEVICE_IDENTITY:
                    continue
                value = _extract(params, col)
                if value is not _MISSING:
                    row[col] = value
            rows.append(row)

    df = _assemble(rows, _DEVICE_IDENTITY, extra_cols, chunked=True)
    if "fabrication_date" in df.columns:
        df["fabrication_date"] = pd.to_datetime(
            df["fabrication_date"], errors="coerce", utc=True
        )
    return df


def _build_devices_df(device_type, columns, include_archived):
    blt = _blt.get_blt()

    kwargs: dict[str, Any] = {}
    if device_type is not None:
        kwargs["type"] = device_type
    if not include_archived:
        kwargs["archived"] = False  # only unarchived; None would include archived too
    if _blt.is_tunnel() and columns is not None:
        # Push a projection down: only the top-level param keys we will read.
        kwargs["keys"] = _projection_keys(columns)

    devices = list(blt.search_devices(**kwargs))
    return _devices_to_frame(devices, columns)


def _devices_df_from_server_cache(device_type, columns, refresh):
    """Whole-space (or type-filtered) devices from the server cache, or ``None``.

    Returns a frame read through ``cached_devices_query`` when (a) the located
    module is the tunnel shim, and (b) the shim exposes the device-cache ops, and
    (c) the cache reports ``state == "ready"``. Otherwise returns ``None`` so
    :func:`devices_df` falls through to its unchanged paging path. An older shim
    without ``device_cache_status`` / ``cached_devices_query`` raises
    ``AttributeError``, which we treat as "no device cache" and swallow. This path
    deliberately does not read or write the on-disk frame cache (``refresh`` is a
    no-op here — reload server-side with ``blt.refresh_device_cache()``).
    """
    try:
        if not _blt.is_tunnel():
            return None
    except Exception:  # noqa: BLE001 - no module located -> paging path
        return None

    blt = _blt.get_blt()
    try:
        status = blt.device_cache_status()
    except AttributeError:
        return None  # shim predates the device cache (SPEC §6)
    except Exception:  # noqa: BLE001 - status unreachable -> page instead
        return None
    if not isinstance(status, dict) or status.get("state") != "ready":
        return None

    try:
        devices = list(
            blt.tunnel_cached_devices_query(
                device_type=device_type, keys=_projection_keys(columns)
            )
        )
    except AttributeError:
        return None  # has status but not the query op; fall back to paging
    return _devices_to_frame(devices, columns)


def _devices_df_by_index(device_type, columns, index, value, refresh):
    """Devices for one configured index value, via ``blt.cached_devices`` (tunnel)."""
    try:
        tunnel = _blt.is_tunnel()
    except Exception:  # noqa: BLE001
        tunnel = False
    if not tunnel:
        raise RuntimeError(
            "devices_df(index=, value=) needs the session tunnel's server-side "
            "device cache, but the located balthazar module is a real Runner, which "
            "has no such cache. Use devices_df(device_type=..., columns=[...]) "
            "instead, or run against the tunnel."
        )

    blt = _blt.get_blt()
    fetch = getattr(blt, "cached_devices", None)
    if fetch is None:
        raise RuntimeError(
            "devices_df(index=, value=): this tunnel shim has no cached_devices(); "
            "it predates the server-side device cache (SPEC §6). Update the tunnel "
            "flow and the shim, or use devices_df(device_type=..., columns=[...])."
        )

    devices = list(fetch(index, value, refresh=refresh))
    if device_type is not None:
        devices = [d for d in devices if getattr(d, "type", None) == device_type]
    return _devices_to_frame(devices, columns)


# ---------------------------------------------------------------------------
# runs_df and its pager
# ---------------------------------------------------------------------------


def _normalize_status(status) -> Optional[list[str]]:
    if status is None:
        return None
    # A single scalar (a "FINISHED" string or a FlowRunStatus enum, neither of which
    # is iterable in the useful sense) wraps to one item; only real sequences iterate.
    if isinstance(status, (list, tuple, set)):
        items = list(status)
    else:
        items = [status]
    # Compare on the bare enum-member name, case-insensitively: a caller may pass a
    # string ("finished") or a real-Runner FlowRunStatus enum (str() of which is
    # "FlowRunStatus.FINISHED"); enum_name reduces both to "FINISHED".
    return [(_compat.enum_name(s) or "").lower() for s in items]


def _to_ts(value):
    pd = _pd()
    if value is None:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:  # noqa: BLE001
        return None
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def _device_ids(run) -> list[str]:
    """Device ids from either shape: the shim's ``.device_ids`` or real ``.devices``."""
    dids = getattr(run, "device_ids", None)
    if dids is not None:
        return [str(x) for x in dids]
    out: list[str] = []
    for device in getattr(run, "devices", None) or []:
        ident = getattr(device, "id", None)
        out.append(str(ident if ident is not None else device))
    return out


def _run_identity(run) -> dict:
    return {
        "run_id": str(getattr(run, "id", "") or ""),
        "flow_id": (lambda v: str(v) if v is not None else None)(getattr(run, "flow_id", None)),
        "flow_name": getattr(run, "flow_name", None),
        "status": _compat.enum_name(getattr(run, "status", None)) or "",
        "created_time": getattr(run, "created_time", None),
        "started_time": getattr(run, "started_time", None),
        "finished_time": getattr(run, "finished_time", None),
        "username": getattr(run, "username", None),
        "tags": list(getattr(run, "tags", []) or []),
        "device_ids": _device_ids(run),
        "visualization_ids": [str(v) for v in (getattr(run, "visualization_ids", []) or [])],
    }


def _run_matches(run, statuses, since) -> bool:
    if statuses is not None and (
        _compat.enum_name(getattr(run, "status", None)) or ""
    ).lower() not in statuses:
        return False
    if since is not None:
        ts = _to_ts(getattr(run, "created_time", None))
        if ts is None or ts < since:
            return False
    return True


def runs_df(
    flow: Optional[str] = None,
    *,
    columns: Optional[Sequence[str]] = None,
    status=None,
    since=None,
    max_runs: int = 100_000,
    refresh: bool = False,
):
    """Flow runs as one wide row each, with real param and output values.

    ``flow`` is a flow **name or id** (like ``flow_schema``); ``flow=None`` loads
    every flow, paged per flow with a global fallback. Columns: the identity set in
    ``_RUN_IDENTITY`` plus ``param.<path>`` (inputs) and ``output.<path>`` (outputs),
    flattened by the same rules as :func:`devices_df`. ``status`` (a string or list)
    and ``since`` (a datetime / parseable timestamp bounding ``created_time``) filter
    the runs; ``max_runs`` caps the pull.
    """
    cols = list(columns) if columns is not None else None
    statuses = _normalize_status(status)

    def build():
        return _build_runs_df(flow, cols, statuses, since, max_runs)

    key = {
        "flow": flow,
        "columns": cols,
        "status": statuses,
        "since": since,
        "max_runs": max_runs,
    }
    return cache.cached("runs", build, key=key, refresh=refresh)


def _resolve_flow_ids(blt, flow) -> list[str]:
    """Flow ids matching ``flow`` by id or name; falls back to treating it as a raw id."""
    try:
        flows = list(blt.search_flows())
    except Exception:  # noqa: BLE001 - enumeration failed; use the value verbatim
        flows = []
    ids = [str(getattr(f, "id", "")) for f in flows if str(getattr(f, "id", "")) == str(flow)]
    if not ids:
        ids = [str(getattr(f, "id", "")) for f in flows if getattr(f, "name", None) == flow]
    return ids or [str(flow)]


def _page_flow(blt, flow_id, runs_by_id, max_runs, statuses, since) -> bool:
    """Page one flow (or the whole space when ``flow_id is None``) into ``runs_by_id``.

    Returns True if the global ``max_runs`` cap was hit. The robust paging (shrink-
    and-retry, poison skip, abandon-after-3, dedupe by id, offset by raw page
    length) lives in :func:`blt_analytics.paging.page_flow_runs`; here we fetch,
    then apply ``status``/``since`` filtering and the shared dedupe dict with its
    global cap. The pager's own ``max_runs`` is the *remaining* budget so the cap
    stays global across flows.
    """
    remaining = max_runs - len(runs_by_id)
    if remaining <= 0:
        return True

    def fetch(offset, limit):
        if flow_id is None:
            return blt.search_flow_run_history(limit=limit, offset=offset)
        return blt.search_flow_run_history(flow_id=flow_id, limit=limit, offset=offset)

    def on_skip(event):
        if event.kind == "shrink":
            _info(
                blt,
                f"runs_df: page too large at offset {event.offset}; retrying with "
                f"smaller page ({event.size}).",
            )
        elif event.kind == "skip":
            _warn(
                blt,
                f"runs_df: skipping poison run in flow {flow_id} at offset "
                f"{event.offset} ({event.error!r}).",
            )
        else:  # "abandon"
            _warn(
                blt,
                f"runs_df: 3 consecutive size-1 failures in flow {flow_id} near "
                f"offset {event.offset} ({event.error!r}); abandoning this flow.",
            )

    runs, truncated = paging.page_flow_runs(
        fetch, page_size=_PAGE_SIZE, max_runs=remaining, on_skip=on_skip
    )
    for run in runs:
        if _run_matches(run, statuses, since):
            runs_by_id[str(getattr(run, "id", "") or "")] = run
    return truncated


def _build_runs_df(flow, columns, statuses, since, max_runs):
    pd = _pd()
    blt = _blt.get_blt()
    since_ts = _to_ts(since) if since is not None else None

    runs_by_id: dict[str, Any] = {}
    hit_cap = False

    if flow is not None:
        for flow_id in _resolve_flow_ids(blt, flow):
            if _page_flow(blt, flow_id, runs_by_id, max_runs, statuses, since_ts):
                hit_cap = True
                break
    else:
        try:
            flows = list(blt.search_flows())
        except Exception as exc:  # noqa: BLE001
            _warn(blt, f"runs_df: search_flows failed ({exc!r}); falling back to global paging.")
            flows = []
        if flows:
            for flow_obj in flows:
                flow_id = str(getattr(flow_obj, "id", ""))
                if _page_flow(blt, flow_id, runs_by_id, max_runs, statuses, since_ts):
                    hit_cap = True
                    break
        else:
            hit_cap = _page_flow(blt, None, runs_by_id, max_runs, statuses, since_ts)

    if hit_cap:
        _warn(blt, f"runs_df: stopped at max_runs={max_runs}; more runs exist but were not loaded.")

    runs = list(runs_by_id.values())
    identity_rows = [_run_identity(run) for run in runs]
    params_list = [_as_dict(getattr(run, "params", {})) for run in runs]
    output_list = [_as_dict(getattr(run, "output", {})) for run in runs]

    if columns is None:
        pcols, prows = _flatten_many(params_list)
        ocols, orows = _flatten_many(output_list)
        extra_cols = [f"param.{c}" for c in pcols] + [f"output.{c}" for c in ocols]
        rows = []
        for ident, prow, orow in zip(identity_rows, prows, orows):
            row = dict(ident)
            for path, value in prow.items():
                row[f"param.{path}"] = value
            for path, value in orow.items():
                row[f"output.{path}"] = value
            rows.append(row)
    else:
        extra_cols = list(columns)
        rows = []
        for ident, params, output in zip(identity_rows, params_list, output_list):
            row = dict(ident)
            for col in columns:
                if col in _RUN_IDENTITY:
                    continue
                if col.startswith("param."):
                    value = _extract(params, col[len("param.") :])
                elif col.startswith("output."):
                    value = _extract(output, col[len("output.") :])
                else:
                    value = _MISSING
                if value is not _MISSING:
                    row[col] = value
            rows.append(row)

    df = _assemble(rows, _RUN_IDENTITY, extra_cols, chunked=True)

    for tcol in ("created_time", "started_time", "finished_time"):
        if tcol in df.columns:
            df[tcol] = pd.to_datetime(df[tcol], errors="coerce", utc=True)
    if "started_time" in df.columns and "finished_time" in df.columns and len(df):
        df["duration_s"] = (df["finished_time"] - df["started_time"]).dt.total_seconds()
    return df


# ---------------------------------------------------------------------------
# Reshaping helpers
# ---------------------------------------------------------------------------


def explode_devices(runs):
    """One row per ``(run, device_id)`` from a ``runs_df`` frame — the join bridge.

    The ``device_ids`` list column is exploded into a scalar ``device_id`` column;
    runs with no devices contribute no rows. Merge onto ``devices_df(...)`` with
    ``left_on="device_id", right_on="id"``.
    """
    pd = _pd()
    if "device_ids" not in runs.columns:
        out = runs.iloc[0:0].copy()
        out["device_id"] = pd.Series(dtype="object")
        return out.reset_index(drop=True)
    out = runs.explode("device_ids", ignore_index=True).rename(
        columns={"device_ids": "device_id"}
    )
    out = out[out["device_id"].notna()].reset_index(drop=True)
    return out


def series_to_df(df, column, *, index_name="i"):
    """Explode a list-valued ``column`` into a long frame: one row per element.

    Each source row's other columns are carried alongside an ``index_name`` position
    (0-based) and the scalar element under ``column``. Rows whose value is not a list
    are dropped.
    """
    pd = _pd()
    if column not in df.columns:
        raise KeyError(f"series_to_df: column {column!r} not in frame")
    keep = [c for c in df.columns if c != column]
    out_cols = keep + [index_name, column]
    rows = []
    for _, record in df.iterrows():
        value = record[column]
        if not isinstance(value, (list, tuple)):
            continue
        base = {c: record[c] for c in keep}
        for position, element in enumerate(value):
            rows.append({**base, index_name: position, column: element})
    if not rows:
        return pd.DataFrame(columns=out_cols)
    return pd.DataFrame(rows, columns=out_cols)


def matrix_to_df(df, column):
    """Explode a matrix-valued ``column`` (list of equal-length numeric lists).

    Produces a long ``(row, col, value)`` frame, one row per cell, carrying each
    source row's other columns. Ready to ``pivot(index="row", columns="col",
    values="value")`` for a heatmap.
    """
    pd = _pd()
    if column not in df.columns:
        raise KeyError(f"matrix_to_df: column {column!r} not in frame")
    keep = [c for c in df.columns if c != column]
    out_cols = keep + ["row", "col", "value"]
    rows = []
    for _, record in df.iterrows():
        matrix = record[column]
        if not isinstance(matrix, (list, tuple)) or not matrix:
            continue
        base = {c: record[c] for c in keep}
        for row_index, matrix_row in enumerate(matrix):
            if not isinstance(matrix_row, (list, tuple)):
                continue
            for col_index, cell in enumerate(matrix_row):
                rows.append({**base, "row": row_index, "col": col_index, "value": cell})
    if not rows:
        return pd.DataFrame(columns=out_cols)
    return pd.DataFrame(rows, columns=out_cols)
