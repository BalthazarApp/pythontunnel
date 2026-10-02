"""Schema-only tool functions over the space digest.

These are the functions a coding agent calls — through the ``balthazar-schema`` MCP
server or the ``blt-schema`` CLI — to learn *what exists* in a Balthazar space
before pulling any real data with :mod:`blt_analytics.frames`. Every function here
returns a **JSON-serializable dict of schema only**: path and type names, kinds,
shapes, coverage percentages, distinct-value *counts* and date ranges. No
measurement value ever appears (the one exception the digest sanctions —
timestamps in date ``first``/``last`` — is a timestamp, not a measurement). That
boundary is what lets the tools be called freely.

They all read one :func:`get_digest`. The digest is either served whole by the
bridge (``blt.tunnel.space_schema``) or, against a real/fake Runner module, built
locally here from fetched records via :func:`blt_analytics.digest.build_digest`.
The local path needs record conversion, so this module carries small private
serializers (``_device_to_record`` / ``_flow_to_record`` / ``_run_to_record``)
that match §1 of the spec — the fixture's ``to_records()`` is their reference.

An unknown name never raises: it comes back as ``{"error": ..., "suggestions":
[...]}`` (closest names via :mod:`difflib`). Listings over 100 items are truncated
and report the overflow. The digest is injectable for tests via
:func:`set_digest` / :func:`reset`.
"""

from __future__ import annotations

import copy
import difflib
import sys
import time
from typing import Any, Iterable

from blt_analytics import _blt
from blt_analytics import _compat
from blt_analytics import digest as _digest_mod
from blt_analytics import paging as _paging

__all__ = [
    "get_digest",
    "overview",
    "device_schema",
    "describe_param",
    "flow_schema",
    "describe_output",
    "find",
    "load_snippet",
    "set_digest",
    "reset",
]

# How many entries a listing may carry before it is truncated (spec §4).
_MAX_LISTING = 100
# Scalar leaf kinds, for default column selection in load_snippet.
_SCALAR_KINDS = ("number", "integer", "string", "bool", "date")

# ``blt.tunnel.space_schema()`` builds the digest server-side and reports
# ``{state, progress, digest?}``; while it is ``building``/``empty`` we re-poll.
# (The Remote may also poll internally; this loop is harmless either way and makes
# the schema layer correct against a bridge that returns the raw state.) Both are
# module-level so tests can shrink the interval.
_SCHEMA_POLL_INTERVAL_S = 2.0
_SCHEMA_POLL_TIMEOUT_S = 600.0

# Module state. ``_injected`` is a test override that short-circuits every build;
# ``_memo`` is the memoized real digest. Both cleared by ``reset()``.
_injected: dict | None = None
_memo: dict | None = None


# ---------------------------------------------------------------------------
# Digest acquisition (tunnel op, or local build), memoized and injectable
# ---------------------------------------------------------------------------


def set_digest(d: dict) -> None:
    """Inject a digest, overriding all acquisition (tests). ``reset()`` undoes it."""
    global _injected
    _injected = d


def reset() -> None:
    """Drop the injected and memoized digests, so the next call rebuilds."""
    global _injected, _memo
    _injected = None
    _memo = None


def get_digest(refresh: bool = False) -> dict:
    """Return the space digest, memoized.

    Call order: an injected digest wins (tests); otherwise the memoized one unless
    ``refresh``; otherwise it is built. When the located module is the bridge its
    ``blt.tunnel.space_schema(refresh)`` serves the digest directly (polled until
    ready); if that op is unavailable (a bridge too old — ``AttributeError``) or the
    module is a real/fake Runner, the digest is built locally from fetched records.

    This is the whole space digest (spec §3). The narrower tools (``overview``,
    ``device_schema``, …) are views over it; call one of those first unless you
    truly need the raw digest.
    """
    global _memo
    if _injected is not None:
        return _injected
    if _memo is not None and not refresh:
        return _memo
    _memo = _acquire_digest(refresh)
    return _memo


def _acquire_digest(refresh: bool) -> dict:
    blt = _blt.get_blt()
    ns = _blt.tunnel_ns()  # the tunnel namespace, or None (real Runner)
    if ns is not None:
        try:
            return _space_schema_via_tunnel(ns, refresh)
        except AttributeError:
            # Bridge predates ``tunnel.space_schema``: build locally from the read
            # surface. A server-side build *error* is surfaced (RuntimeError), not
            # masked by a slow, projection-less local rebuild.
            pass
    devices, flows, runs = _fetch_records(blt)
    return _digest_mod.build_digest(devices, flows, runs)


def _space_schema_via_tunnel(ns: Any, refresh: bool) -> dict:
    """The space digest via ``blt.tunnel.space_schema()``, polling until ``ready``.

    ``space_schema(refresh=False)`` returns ``{state, progress, digest?}``. The
    server builds in the background, so while ``state`` is ``building``/``empty`` we
    re-poll every ``_SCHEMA_POLL_INTERVAL_S`` (``refresh`` only on the first call, to
    force a rebuild without restarting the build each loop). ``state == "error"``
    raises ``RuntimeError`` with the server's message; ``ready`` returns the digest.
    """
    deadline = time.monotonic() + _SCHEMA_POLL_TIMEOUT_S
    want_refresh = bool(refresh)
    notified = False
    while True:
        result = ns.space_schema(refresh=want_refresh)
        want_refresh = False  # never re-trigger the build on subsequent polls
        if not isinstance(result, dict):
            raise RuntimeError(f"space_schema returned {type(result).__name__}, expected a dict")
        state = result.get("state")
        if state == "ready":
            return result.get("digest") or {}
        if state == "error":
            raise RuntimeError(result.get("error") or "space_schema failed on the bridge")
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"space_schema did not become ready within {_SCHEMA_POLL_TIMEOUT_S:.0f}s "
                f"(last state={state!r})"
            )
        if not notified:
            print("blt_analytics: building the space schema on the bridge…", file=sys.stderr)
            notified = True
        time.sleep(_SCHEMA_POLL_INTERVAL_S)


def _fetch_records(blt: Any) -> tuple[list[dict], list[dict], list[dict]]:
    """Fetch devices/flows/runs and convert to §1 records for ``build_digest``.

    The local-build path, used against a real/fake Runner (never the tunnel, so no
    projection kwargs are passed). Runs are paged per flow with shrink-and-retry.
    """
    devices = [_device_to_record(d) for d in (blt.search_devices() or [])]
    flows = [_flow_to_record(f) for f in (blt.search_flows() or [])]
    runs: list[dict] = []
    for flow in flows:
        runs.extend(_page_runs(blt, flow["id"]))
    return devices, flows, runs


def _page_runs(blt: Any, flow_id: str, *, page_size: int = 250, max_runs: int = 2000) -> list[dict]:
    """Page one flow's run history into §1 records, deduped by id.

    A thin wrapper over the shared :func:`blt_analytics.paging.page_flow_runs`
    (shrink-and-retry, poison skip, abandon-after-3, dedupe by id, offset by raw
    page length); the local-build fallback logs nothing, so no ``on_skip``.
    """
    def fetch(offset, limit):
        return blt.search_flow_run_history(flow_id=flow_id, limit=limit, offset=offset)

    runs, _truncated = _paging.page_flow_runs(fetch, page_size=page_size, max_runs=max_runs)
    return [_run_to_record(run) for run in runs]


# ---------------------------------------------------------------------------
# Record serializers (spec §1). Match fixture_space.to_records() byte-for-byte.
# ---------------------------------------------------------------------------


def _iso(value: Any) -> Any:
    return value.isoformat() if value is not None and hasattr(value, "isoformat") else value


def _param_meta(meta: Any) -> dict:
    """A declared-parameter record ``{type, default, description}`` (§1)."""
    if isinstance(meta, dict):
        get = meta.get
    else:
        get = lambda key, default=None: getattr(meta, key, default)  # noqa: E731
    return {"type": get("type"), "default": get("default"), "description": get("description")}


def _device_to_record(device: Any) -> dict:
    return {
        "id": device.id,
        "name": getattr(device, "name", "") or "",
        "type": getattr(device, "type", "device"),
        "description": getattr(device, "description", None),
        "fabrication_date": _iso(getattr(device, "fabrication_date", None)),
        "tags": list(getattr(device, "tags", None) or []),
        "params": copy.deepcopy(dict(getattr(device, "params", None) or {})),
    }


def _flow_to_record(flow: Any) -> dict:
    parameters = {
        name: _param_meta(meta) for name, meta in (getattr(flow, "parameters", None) or {}).items()
    }
    return {
        "id": flow.id,
        "name": flow.name,
        "description": getattr(flow, "description", None),
        "branch": getattr(flow, "branch", None),
        "script_filename": getattr(flow, "script_filename", None),
        "tags": list(getattr(flow, "tags", None) or []),
        "created_time": _iso(getattr(flow, "created_time", None)),
        "username": getattr(flow, "username", None),
        "parameters": parameters,
    }


def _run_to_record(run: Any) -> dict:
    # A reflected run may expose device_ids; the real/fake module exposes .devices.
    device_ids = list(getattr(run, "device_ids", None) or [])
    if not device_ids:
        device_ids = [getattr(d, "id", d) for d in (getattr(run, "devices", None) or [])]
    status = getattr(run, "status", None)
    return {
        "id": run.id,
        "flow_id": getattr(run, "flow_id", None),
        "flow_name": getattr(run, "flow_name", None),
        # A real-Runner status is a PyO3 enum whose str() is "FlowRunStatus.FINISHED";
        # the wire format (and the digest) want the bare name. enum_name is None-safe
        # and a no-op on the bare strings a tunnel would already hand back.
        "status": _compat.enum_name(status),
        "created_time": _iso(getattr(run, "created_time", None)),
        "started_time": _iso(getattr(run, "started_time", None)),
        "finished_time": _iso(getattr(run, "finished_time", None)),
        "username": getattr(run, "username", None),
        "tags": list(getattr(run, "tags", None) or []),
        "comment": getattr(run, "comment", None),
        "device_ids": device_ids,
        "params": copy.deepcopy(dict(getattr(run, "params", None) or {})),
        "output": copy.deepcopy(dict(getattr(run, "output", None) or {})),
        "visualization_ids": list(getattr(run, "visualization_ids", None) or []),
    }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _suggestions(name: Any, candidates: Iterable[Any]) -> list[str]:
    names = [str(c) for c in candidates]
    close = difflib.get_close_matches(str(name), names, n=5, cutoff=0.3)
    return close if close else sorted(names)[:5]


def _unknown(name: Any, candidates: Iterable[Any], *, what: str = "name") -> dict:
    return {"error": f"unknown {what}: {name!r}", "suggestions": _suggestions(name, candidates)}


def _order_paths(fieldmap: dict) -> dict:
    """Order a path->field map so top-level (undotted) paths come first."""
    return {k: fieldmap[k] for k in sorted(fieldmap, key=lambda p: (p.count("."), p))}


def _cap(mapping: dict, limit: int = _MAX_LISTING) -> tuple[dict, int]:
    """(capped dict, number dropped). Keeps the first ``limit`` entries in order."""
    items = list(mapping.items())
    if len(items) <= limit:
        return dict(items), 0
    return dict(items[:limit]), len(items) - limit


def _resolve_flow(flows: dict, flow: Any) -> str | None:
    """Resolve a flow by name or id to its digest key (name)."""
    if flow in flows:
        return flow
    for name, entry in flows.items():
        if entry.get("id") == flow:
            return name
    return None


def _flow_lookup_names(flows: dict) -> list[str]:
    names = list(flows)
    names.extend(entry.get("id") for entry in flows.values() if entry.get("id"))
    return names


def _describe_one(fieldmap: dict, path: Any, *, owner_key: str, owner_val: str) -> dict:
    """One field at ``path`` plus its (dotted) child paths, from a path->field map.

    ``path`` may name a flattened leaf (``resistance.value``), a map/collapsed
    field (``measurements``), or an intermediate dict whose children exist only as
    dotted paths (``hierarchy`` -> ``hierarchy.lot``). An empty path lists the
    top-level paths.
    """
    path = path or ""
    result: dict[str, Any] = {owner_key: owner_val, "path": path}

    if path == "":
        children = {k: v for k, v in fieldmap.items() if "." not in k}
        if not children:
            children = dict(fieldmap)
    else:
        field = fieldmap.get(path)
        prefix = path + "."
        children = {k: v for k, v in fieldmap.items() if k.startswith(prefix)}
        if field is None and not children:
            return _unknown(path, fieldmap.keys(), what="path")
        if field is not None:
            result["field"] = field

    ordered, dropped = _cap(_order_paths(children))
    result["children"] = ordered
    if dropped:
        result["children_truncated"] = dropped
    return result


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


def overview() -> dict:
    """The lay of the land — call this first.

    Totals, every device type with its device count and param-path count, and every
    flow with its run count and date range. Schema only; take exact device-type and
    flow names from here, then narrow with the other tools.

    On the bridge it also reports ``device_indexes`` (``{name: dotted path}``)
    when the flow configures a server-side device cache (SPEC §6) — the index names
    to pass to ``devices_df(index=..., value=...)``. Names and paths only, never
    index values.
    """
    d = get_digest()
    device_types = {
        name: {"count": entry.get("count", 0), "param_count": len(entry.get("params", {}))}
        for name, entry in d.get("device_types", {}).items()
    }
    flows = {}
    for name, entry in d.get("flows", {}).items():
        item: dict[str, Any] = {"id": entry.get("id"), "run_count": entry.get("run_count", 0)}
        if "first_run" in entry:
            item["first_run"] = entry["first_run"]
        if "last_run" in entry:
            item["last_run"] = entry["last_run"]
        flows[name] = item

    capped_types, dropped_types = _cap(device_types)
    capped_flows, dropped_flows = _cap(flows)
    out: dict[str, Any] = {
        "version": d.get("version"),
        "built_at": d.get("built_at"),
        "totals": d.get("totals", {}),
        "device_types": capped_types,
        "flows": capped_flows,
    }
    if dropped_types:
        out["device_types_truncated"] = dropped_types
    if dropped_flows:
        out["flows_truncated"] = dropped_flows

    # Configured server-side device indexes (names + dotted paths only — schema, no
    # values), from the ``describe`` reply (SPEC §6). Skipped when a digest is
    # injected (tests), so the pure-digest tools stay offline and deterministic.
    if _injected is None:
        indexes = _device_indexes_from_describe()
        if indexes:
            out["device_indexes"] = indexes
    return out


def _device_indexes_from_describe() -> dict:
    """``{index_name: dotted_path}`` for the configured device indexes, or ``{}``.

    On the bridge, the index names/paths come from the ``describe`` reply's
    ``device_indexes`` (``_blt.describe_info()``). Anything that goes wrong (not a
    bridge, no connection, a malformed reply) collapses to ``{}`` so ``overview``
    never fails over it. Only names and paths surface — never index *values*, which
    are device data.
    """
    try:
        if not _blt.is_tunnel():
            return {}
        info = _blt.describe_info() or {}
    except Exception:  # noqa: BLE001 - offline / no bridge -> nothing to add
        return {}
    raw = info.get("device_indexes") if isinstance(info, dict) else None
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for name, spec in raw.items():
        if isinstance(spec, str):
            out[str(name)] = spec
        elif isinstance(spec, dict) and isinstance(spec.get("path"), str):
            out[str(name)] = spec["path"]  # object form {"path": ..., "device_type": ...}
    return out


def device_schema(device_type: str) -> dict:
    """Everything one device type declares: its count, fabrication-date and tag
    coverage, and its param paths (top-level first) with kinds and coverage.

    Pass an exact type name from ``overview``/``find``. Schema only — use the dotted
    param paths it lists to project columns in ``devices_df``. For a nested or
    matrix param, follow up with ``describe_param``.
    """
    d = get_digest()
    types = d.get("device_types", {})
    if device_type not in types:
        return _unknown(device_type, types.keys(), what="device_type")
    entry = types[device_type]
    params, dropped = _cap(_order_paths(entry.get("params", {})))
    out: dict[str, Any] = {
        "device_type": device_type,
        "count": entry.get("count", 0),
        "fabrication_date": entry.get("fabrication_date", {}),
        "tags": entry.get("tags", {}),
        "params": params,
    }
    if dropped:
        out["params_truncated"] = dropped
    return out


def describe_param(device_type: str, path: str = "") -> dict:
    """One device param path in full, plus its child paths.

    Call before indexing a nested param or plotting a matrix/list: it tells you the
    field's ``kind`` (number, string, list, matrix, map, dict, mixed, …), its
    coverage, and — for a dict — the dotted child paths beneath it. An empty path
    lists the type's top-level params. Schema only, no values.
    """
    d = get_digest()
    types = d.get("device_types", {})
    if device_type not in types:
        return _unknown(device_type, types.keys(), what="device_type")
    params = types[device_type].get("params", {})
    return _describe_one(params, path, owner_key="device_type", owner_val=device_type)


def flow_schema(flow: str) -> dict:
    """Everything one flow declares: its declared parameters, input and output
    paths, status breakdown, run count, date range and how many runs have plots.

    Accepts a flow **name or id** (from ``overview``/``find``). Schema only — use
    the ``param.<path>`` / ``output.<path>`` names to project columns in
    ``runs_df``; follow up on an output with ``describe_output``.
    """
    d = get_digest()
    flows = d.get("flows", {})
    name = _resolve_flow(flows, flow)
    if name is None:
        return _unknown(flow, _flow_lookup_names(flows), what="flow")
    entry = flows[name]

    out: dict[str, Any] = {"flow": name}
    for key in (
        "id",
        "run_count",
        "runs_sampled",
        "truncated",
        "status",
        "first_run",
        "last_run",
        "runs_with_plots",
        "device_types",
        "declared_parameters",
    ):
        if key in entry:
            out[key] = entry[key]

    inputs, in_dropped = _cap(_order_paths(entry.get("inputs", {})))
    outputs, out_dropped = _cap(_order_paths(entry.get("outputs", {})))
    out["inputs"] = inputs
    out["outputs"] = outputs
    if in_dropped:
        out["inputs_truncated"] = in_dropped
    if out_dropped:
        out["outputs_truncated"] = out_dropped
    return out


def describe_output(flow: str, path: str = "") -> dict:
    """One flow output path in full, plus its child paths.

    Like ``describe_param`` but for a flow's outputs. Resolve ``flow`` by name or
    id. Call it to learn an output's ``kind`` and shape (e.g. a ``matrix`` to feed
    ``matrix_to_df`` or a ``list`` for ``series_to_df``). Schema only, no values.
    """
    d = get_digest()
    flows = d.get("flows", {})
    name = _resolve_flow(flows, flow)
    if name is None:
        return _unknown(flow, _flow_lookup_names(flows), what="flow")
    outputs = flows[name].get("outputs", {})
    return _describe_one(outputs, path, owner_key="flow", owner_val=name)


def _match_score(query: str, text: str) -> float:
    q = query.strip().lower()
    t = str(text).lower()
    if not q or not t:
        return 0.0
    if q == t:
        return 1.0
    if q in t:
        # Substring: strong, and stronger the more of the target it covers.
        return round(min(0.99, 0.8 + 0.2 * (len(q) / len(t))), 4)
    return round(difflib.SequenceMatcher(None, q, t).ratio(), 4)


def find(query: str, limit: int = 20) -> dict:
    """Fuzzy-match the user's words across every name in the space.

    Your first move when a request is vague. Searches device-type names, device
    param paths, flow names and flow input/output paths, and returns ranked matches
    ``{"matches": [{"kind", "device_type"|"flow", "path", "score"}]}`` where ``kind``
    is one of ``device_type``, ``device_param``, ``flow``, ``flow_input``,
    ``flow_output``. Use the exact names it returns in the other tools. Schema only.
    """
    d = get_digest()
    candidates: list[tuple[str, dict, str]] = []  # (text_to_match, base_match, kind)

    for dtype, entry in d.get("device_types", {}).items():
        candidates.append((dtype, {"kind": "device_type", "device_type": dtype}, "name"))
        for path in entry.get("params", {}):
            candidates.append(
                (path, {"kind": "device_param", "device_type": dtype, "path": path}, "path")
            )

    for fname, entry in d.get("flows", {}).items():
        candidates.append((fname, {"kind": "flow", "flow": fname}, "name"))
        for path in entry.get("inputs", {}):
            candidates.append(
                (path, {"kind": "flow_input", "flow": fname, "path": path}, "path")
            )
        for path in entry.get("outputs", {}):
            candidates.append(
                (path, {"kind": "flow_output", "flow": fname, "path": path}, "path")
            )

    scored: list[dict] = []
    for text, base, _role in candidates:
        score = _match_score(query, text)
        if score < 0.34:
            continue
        match = dict(base)
        match["score"] = score
        scored.append(match)

    # Rank by score, then kind then name for a stable, deterministic order.
    scored.sort(key=lambda m: (-m["score"], m["kind"], m.get("device_type") or m.get("flow") or "", m.get("path") or ""))
    capped = scored[: max(0, int(limit))]
    out: dict[str, Any] = {"query": query, "matches": capped}
    if len(scored) > len(capped):
        out["truncated"] = len(scored) - len(capped)
    return out


def _normalize_device_columns(params: dict, columns: list[str] | None) -> list[str]:
    if columns:
        chosen = [c for c in columns if c in params]
        if chosen:
            return chosen
    # Default: the first few scalar leaves (top-level paths first).
    return [p for p, f in _order_paths(params).items() if f.get("kind") in _SCALAR_KINDS][:6]


def _normalize_flow_columns(inputs: dict, outputs: dict, columns: list[str] | None) -> list[str]:
    valid = {f"param.{p}" for p in inputs} | {f"output.{p}" for p in outputs}
    if columns:
        chosen: list[str] = []
        for c in columns:
            if c in valid:
                chosen.append(c)
            elif c in inputs:
                chosen.append(f"param.{c}")
            elif c in outputs:
                chosen.append(f"output.{c}")
        if chosen:
            return chosen
    return [f"param.{p}" for p in list(_order_paths(inputs))[:3]] + [
        f"output.{p}" for p in list(_order_paths(outputs))[:3]
    ]


def load_snippet(
    device_type: str | None = None, flow: str | None = None, columns: list[str] | None = None
) -> dict:
    """Starter ``blt_analytics`` code for the slice you want: ``{"code": "..."}``.

    Pass a device type and/or a flow (name or id), optionally the dotted
    ``columns`` to project. Returns runnable Python that pulls the data into pandas
    — only ever referencing names that exist in the digest. A good base to adapt;
    it does not fetch anything itself.
    """
    d = get_digest()
    dt_entry = None
    if device_type is not None:
        types = d.get("device_types", {})
        if device_type not in types:
            return _unknown(device_type, types.keys(), what="device_type")
        dt_entry = types[device_type]

    flow_name = None
    flow_entry = None
    if flow is not None:
        flows = d.get("flows", {})
        flow_name = _resolve_flow(flows, flow)
        if flow_name is None:
            return _unknown(flow, _flow_lookup_names(flows), what="flow")
        flow_entry = flows[flow_name]

    lines = ["import blt_analytics as ba"]

    if dt_entry is not None:
        cols = _normalize_device_columns(dt_entry.get("params", {}), columns if flow is None else None)
        col_arg = f", columns={cols!r}" if cols else ""
        lines.append(f"devices = ba.devices_df({device_type!r}{col_arg})")

    if flow_entry is not None:
        cols = _normalize_flow_columns(
            flow_entry.get("inputs", {}), flow_entry.get("outputs", {}), columns
        )
        col_arg = f", columns={cols!r}" if cols else ""
        lines.append(f"runs = ba.runs_df({flow_name!r}{col_arg})")

    if dt_entry is not None and flow_entry is not None:
        lines.append("pairs = ba.explode_devices(runs)")
        lines.append(
            'merged = pairs.merge(devices, left_on="device_id", right_on="id", '
            'suffixes=("_run", "_dev"))'
        )
        lines.append("print(merged.head())")
    elif flow_entry is not None:
        lines.append("print(runs.head())")
    elif dt_entry is not None:
        lines.append("print(devices.head())")
    else:
        # Nothing specified: a two-line tour of the whole space.
        lines.append("devices = ba.devices_df()")
        lines.append("runs = ba.runs_df()")
        lines.append("print(devices.head())")
        lines.append("print(runs.head())")

    return {"code": "\n".join(lines)}
