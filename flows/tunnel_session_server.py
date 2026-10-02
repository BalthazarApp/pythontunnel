"""Balthazar Session Tunnel — host side, v2. UTILITY FLOW (long-running).

The v1 tunnel (``tunnel_server.py``) treats every request as self-contained. This
one holds a *flow-run context stack* open across requests, so a local script can
write:

    with blt.enter_new_flow_run(name="I-V sweep", devices=[d]):
        plt.plot(...); plt.show()          # visualization lands on the child run
        blt.output["kpi"] = 1.23           # output lands on the child run
        d.params.update({"measured": ...}) # device write, attributed to the child

Everything inside that block executes against a real child flow run on the Runner
while your code runs locally, in your debugger.

Flow parameters
---------------
port             : int (default 8766)  Local port to listen on.
idle_timeout     : int (default 1800)  Seconds of client silence before open contexts
                                       are unwound. Generous by default because a
                                       breakpoint inside a ``with`` block stops the
                                       client from sending anything.
device_indexes   : str (default "{}")  JSON mapping an index name to a dotted param
                                       path, e.g. ``{"wafer": "hierarchy.wafer"}`` or
                                       ``{"wafer": {"path": "hierarchy.wafer",
                                       "device_type": "Die"}}``. Configures the
                                       server-side device cache (spec §6). Bad JSON
                                       logs a ``blt.error`` and the tunnel still starts
                                       (just without indexes).
warm_device_cache: bool (default: on   Load the device cache in the background at
                   when any index is   start.
                   configured)
device_cache_dir : str (default        Where the cache is persisted on the Runner, at
                   ~/.balthazar_       ``<dir>/<space-or-root-flow key>/devices.pkl``
                   tunnel_cache)       (+ a JSON meta sidecar), so a restart does not
                                       reload 250k devices. Written atomically, dir
                                       0700 / files 0600 (it holds space data).

WHY THIS IS HARDER THAN v1
1. ``enter_new_flow_run`` / its exit assert ``is_main_thread`` in the Runner, so
   all ``blt.*`` work happens on the main thread via a job queue. Requests arrive
   on HTTP worker threads and hand jobs over.
2. A context spans *many* requests. The Runner's context is process-global
   (``blt.params``, ``blt.output``, ``blt.devices``, ``blt.flow_run`` are module
   attributes), so two clients interleaving would silently write into each
   other's runs. Hence single-owner locking: while a stack is open, only the
   owning client may act.
3. A client that dies inside a ``with`` block leaves a run stuck in RUNNING.
   Hence the idle watchdog, which unwinds abandoned contexts and marks them
   failed.
4. Marking a child FAILED requires an exception *in flight* at exit: the Runner's
   ``__exit__`` derives status from ``exc_value``. We call ``__exit__`` directly
   with a synthesized exception rather than raising, so the error message is ours.

SECURITY. Binds 127.0.0.1 only, per-session bearer token, non-loopback Host
headers rejected, explicit operation allowlist. ``blt.secrets`` is not exposed.

ANALYTICS READ OPS. Alongside the context ops, this server hosts a set of
read-only operations the ``blt_analytics`` tools drive — ``search_flows``,
``search_flow_runs``, ``fetch_visualizations`` and ``space_schema`` (a
measurement-free digest built with ``blt_analytics.digest``), plus projection
kwargs on ``search_devices``. These never touch the context stack: they call
``_last_seen_touch`` rather than ``_claim``, so a client may read while another
client owns an open context. All ``blt.*`` work still runs on the main-thread
executor via the job queue.

SERVER-SIDE DEVICE CACHE (spec §6). A space with 250k+ devices is expensive to
page on every call, and the data changes rarely. When the ``device_indexes`` flow
parameter is configured, the server loads every device record once (full §1
records, held in memory — a few GB is acceptable) and keeps, per configured index,
a ``str(value) -> [device ids]`` map. The load is orchestrated exactly like
``space_schema`` — many small main-thread jobs off a worker thread — so it does not
monopolize the executor, and it is persisted on the Runner so a restart does not
reload 250k devices. The cache is read by ``cached_devices`` / ``cached_devices_query``
(and by ``space_schema`` when ready), kept fresh write-through on
``update_device_params``, and reloadable with ``refresh_device_cache``. These are
read ops too (``_last_seen_touch``, never ``_claim``); the ones that may trigger a
load are orchestrated on the worker thread rather than run as a capped executor job.
"""

import base64
import hmac
import json
import os
import pickle
import queue
import secrets as _secrets
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import balthazar as blt

if getattr(blt, "__balthazar_tunnel__", False):
    raise RuntimeError(
        "Imported the tunnel shim, not the real balthazar module. This flow must "
        "run on a Balthazar Runner."
    )

CONNECTION_FILE = os.path.expanduser("~/.balthazar_session_tunnel.json")
_MAX_BODY_BYTES = 64 * 1024 * 1024
_JOB_TIMEOUT_S = 300.0

_TOKEN = _secrets.token_urlsafe(32)
_JOBS: "queue.Queue[_Job]" = queue.Queue()
_stop = threading.Event()
# Set while ``_run_executor`` is draining ``_JOBS``. ``_call_on_main`` reads it to
# decide whether a main-thread fetch must be handed over as a job (server is live)
# or can just run inline (a unit test driving an op directly, no executor thread).
_executor_live = threading.Event()

# --- context state, touched only from the main thread ------------------------
_stack = []          # innermost last: {"ctx", "flow_run_id", "name"}
_owner = None        # client_id currently holding _stack
_last_seen = 0.0     # monotonic timestamp of the owner's last request
_idle_timeout = 1800.0

_stats = {
    "requests_served": 0,
    "flow_runs_entered": 0,
    "visualizations_stored": 0,
    "device_writes": 0,
    "reads_served": 0,
    "contexts_reclaimed": 0,
    "errors": 0,
}

# The space digest is expensive (it pages every flow's run history), so the
# server memoizes it until a client asks for a refresh. The build runs on an HTTP
# worker thread (handing its blt.* reads to the main thread one job at a time), so
# unlike the context state this cache may be read/written off the main thread; the
# lock serializes concurrent builds so a second caller waits and returns the cache
# rather than building it twice.
_schema_cache = {"digest": None}
_schema_lock = threading.Lock()

# Page size for ``space_schema``'s per-flow run paging. A module global (not a
# default arg) so it reads live — handy for tests, and a single knob here.
_RUN_PAGE_SIZE = 250

# Page size for ``space_schema``'s device fetch: a bounded read handed over one
# page per job, so a large space's device pull does not monopolize the executor.
_DEVICE_PAGE_SIZE = 500

# The whole ``space_schema`` build gets its own generous budget, separate from the
# per-job ``_JOB_TIMEOUT_S``: a build legitimately spans many pages and may outlast
# a single op's timeout, but it must not run unbounded. Checked between jobs.
_SCHEMA_BUILD_BUDGET_S = 3600.0


# ----------------------------------------------------------------------------
# Server-side device cache (spec §6)
# ----------------------------------------------------------------------------

# Configuration, parsed from the flow params at start (see ``_init_device_cache``).
#   _device_indexes: {name: {"path": "hierarchy.wafer", "device_type": "Die" | None}}
_device_indexes: dict = {}
_warm_device_cache = False
_DEFAULT_CACHE_DIR = "~/.balthazar_tunnel_cache"
_device_cache_dir = os.path.expanduser(_DEFAULT_CACHE_DIR)

# Full-load page size (spec: limit 1000) and refresh-by-value chunk (spec: 500).
_DEVICE_CACHE_PAGE_SIZE = 1000
_DEVICE_REFRESH_CHUNK = 500
# The whole device-cache (re)load gets its own budget, like the schema build.
_DEVICE_CACHE_BUDGET_S = 3600.0

# The *published, ready* store. Swapped atomically (a single reference assignment,
# which is GIL-atomic) on a successful load/refresh so a reader that grabbed the old
# reference keeps seeing a consistent snapshot while a reload builds a fresh one.
#   records: {id: §1 device record}
#   index:   {index_name: {str(value): [ids]}}
_device_store = {"records": {}, "index": {}, "built_at": None}

# Live status, mutated in place during a load so ``device_cache_status`` can report
# progress without taking the build lock. Field reads/writes are GIL-atomic, which is
# all a status snapshot needs.
_cache_meta = {
    "state": "empty",     # "empty" | "loading" | "ready" | "error"
    "count": 0,
    "loaded": 0,
    "built_at": None,
    "started_at": None,
    "error": None,
}

# Serializes full (re)loads: only one runs at a time and concurrent callers wait on
# it (spec §6). Deliberately NOT held by write-through or by status/reads, so a
# main-thread op can never block waiting for a worker-thread load that is itself
# waiting for the main thread (which would deadlock the executor).
_device_cache_lock = threading.Lock()

_MISSING = object()


class _Job:
    """A unit of main-thread work.

    Either a client *op* (``op`` + ``kwargs``, dispatched through ``_DISPATCH``) or
    a bare *callable* (``fn``), which ``space_schema``'s worker-thread orchestration
    uses to run one small ``blt.*`` read on the main thread and get its result back.
    """

    __slots__ = ("op", "kwargs", "fn", "reply")

    def __init__(self, op=None, kwargs=None, fn=None):
        self.op = op
        self.kwargs = kwargs if kwargs is not None else {}
        self.fn = fn
        self.reply: "queue.Queue[tuple[bool, object]]" = queue.Queue(maxsize=1)


class _TunnelRunFailed(Exception):
    """Synthesized at exit so the Runner records the client's error message."""


class _MainThreadJobError(Exception):
    """Raised on the worker thread when a main-thread job (``fn``) failed.

    Carries the original exception's type name and message so the orchestration (and
    the shrink-and-retry pager it drives) sees a real exception, exactly as it would
    if the ``blt.*`` call had run inline.
    """

    def __init__(self, type_name, message):
        super().__init__(f"{type_name}: {message}")
        self.type_name = type_name


# ----------------------------------------------------------------------------
# Serialization
# ----------------------------------------------------------------------------


def _params_to_dict(params):
    for attempt in (
        lambda: dict(params),
        lambda: {k: params[k] for k in params.keys()},
        lambda: {k: v for k, v in params.items()},
    ):
        try:
            return attempt()
        except Exception:  # noqa: BLE001 - probing the proxy's mapping protocol
            continue
    blt.warn("Could not serialize device params; returning empty dict")
    return {}


def _iso(value):
    """ISO-8601 for a date/datetime, else the value unchanged (incl. ``None``)."""
    return value.isoformat() if hasattr(value, "isoformat") else value


def _enum_name(value):
    """The bare NAME of an enum-like value, robust to real Runner PyO3 enums.

    A real Runner's ``FlowRunStatus`` / ``VisualizationDataType`` are PyO3 enums that
    implement only ``__repr__`` ("FlowRunStatus.FINISHED") and expose no ``.name`` —
    so both ``str(v)`` and ``getattr(v, "name", ...)`` mislead. Take the last
    dotted segment of ``str(value)``: "FlowRunStatus.FINISHED" -> "FINISHED", and a
    plain "FINISHED" (or "SVG") passes through unchanged. ``None`` -> ``None``.
    """
    if value is None:
        return None
    return str(value).rsplit(".", 1)[-1]


def _project_params(params, keys=None, scalars_only=False):
    """Apply the shim-only projections to a flat param dict.

    ``keys`` keeps only those top-level keys; ``scalars_only`` drops dict/list
    values. Both narrow what crosses the wire; neither invents data.
    """
    if keys is not None:
        wanted = set(keys)
        params = {k: v for k, v in params.items() if k in wanted}
    if scalars_only:
        params = {
            k: v for k, v in params.items() if not isinstance(v, (dict, list, tuple))
        }
    return params


def _device_to_dict(device, *, keys=None, scalars_only=False, include_params=True):
    fab = getattr(device, "fabrication_date", None)
    record = {
        "id": getattr(device, "id", None),
        "name": getattr(device, "name", ""),
        "type": getattr(device, "type", "device"),
        "description": getattr(device, "description", None),
        "fabrication_date": _iso(fab),
        "tags": list(getattr(device, "tags", []) or []),
    }
    if include_params:
        params = _params_to_dict(getattr(device, "params", {}) or {})
        record["params"] = _project_params(params, keys, scalars_only)
    else:
        record["params"] = {}
    return record


def _flow_to_dict(flow):
    """Serialize a ``Flow`` (spec §1). Declared params keep type/default/desc."""
    declared = {}
    for name, meta in (getattr(flow, "parameters", {}) or {}).items():
        declared[name] = {
            "type": getattr(meta, "type", None),
            "default": getattr(meta, "default", None),
            "description": getattr(meta, "description", None),
        }
    return {
        "id": getattr(flow, "id", None),
        "name": getattr(flow, "name", None),
        "description": getattr(flow, "description", None),
        "branch": getattr(flow, "branch", None),
        "script_filename": getattr(flow, "script_filename", None),
        "tags": list(getattr(flow, "tags", []) or []),
        "created_time": _iso(getattr(flow, "created_time", None)),
        "username": getattr(flow, "username", None),
        "parameters": declared,
    }


def _run_to_dict(run, *, include_params=True, include_output=True):
    """Serialize a flow run (spec §1). ``.devices`` collapses to ``device_ids``."""
    devices = getattr(run, "devices", None)
    if devices is not None:
        device_ids = [getattr(d, "id", d) for d in devices]
    else:
        device_ids = list(getattr(run, "device_ids", []) or [])
    return {
        "id": getattr(run, "id", None),
        "flow_id": getattr(run, "flow_id", None),
        "flow_name": getattr(run, "flow_name", None),
        "status": _enum_name(getattr(run, "status", None)),
        "created_time": _iso(getattr(run, "created_time", None)),
        "started_time": _iso(getattr(run, "started_time", None)),
        "finished_time": _iso(getattr(run, "finished_time", None)),
        "username": getattr(run, "username", None),
        "tags": list(getattr(run, "tags", []) or []),
        "comment": getattr(run, "comment", None),
        "device_ids": device_ids,
        "params": _params_to_dict(getattr(run, "params", {}) or {}) if include_params else {},
        "output": _params_to_dict(getattr(run, "output", {}) or {}) if include_output else {},
        "visualization_ids": list(getattr(run, "visualization_ids", []) or []),
    }


def _viz_type_str(value):
    """A stable string for a visualization data type (its bare enum name).

    Real Runner ``VisualizationDataType`` members are PyO3 enums with no ``.name``,
    so this goes through :func:`_enum_name` rather than reaching for ``.name``.
    """
    return _enum_name(value)


def _resolve_devices(device_ids):
    if not device_ids:
        return []
    by_id = {d.id: d for d in blt.search_devices(id=list(device_ids))}
    missing = [i for i in device_ids if i not in by_id]
    if missing:
        raise ValueError(f"Unknown device id(s): {', '.join(missing)}")
    return [by_id[i] for i in device_ids]


def _decode_visualizations(items):
    """Turn the client's payload into ``blt.VisualizationBuilder`` objects.

    Since Runner 1.35.1 ``blt.store_visualizations`` takes builders, not the
    ``(figure_id, filename, bytes)`` tuples the removed ``blt.api`` accepted.

    The client's ``id`` is its local matplotlib figure number, which maps onto
    ``figure_id``: storing again under the same id *replaces* the previous
    visualization. That matters most here — a session client redraws the same
    figure across many requests, and replace-semantics keep the run holding the
    latest frame rather than every intermediate one. The batch is rejected if two
    items share an id, so check it here where the offending file can be named.
    """
    decoded = []
    seen = set()
    for index, item in enumerate(items or [], start=1):
        filename = item.get("filename") or f"figure_{index}.svg"
        if not filename.endswith(".svg"):
            filename += ".svg"
        try:
            data = base64.b64decode(item["svg_base64"], validate=True)
        except (KeyError, ValueError) as exc:
            raise ValueError(f"Bad visualization payload for {filename!r}: {exc}") from exc

        figure_id = item.get("figure_id", item.get("id"))
        if figure_id is not None:
            figure_id = int(figure_id)
            if figure_id in seen:
                raise ValueError(
                    f"Duplicate figure_id {figure_id} for {filename!r}; the store is "
                    f"transactional and rejects a batch whose items replace each other."
                )
            seen.add(figure_id)

        decoded.append(
            blt.VisualizationBuilder(
                filename,
                data,
                type=blt.VisualizationDataType.SVG,
                figure_id=figure_id,
            )
        )
    return decoded


_MAX_CELL_CHARS = 8000


def _log_cell_source(source):
    """Log the notebook cell that opened this run, into the run's own logs.

    Called just after entering, so the run reads top-down: the code first, then
    whatever it produced. The Runner propagates a context log up into every
    ancestor, so this also surfaces in the tunnel's own run without a second call.

    Truncated server-side as well as client-side: the body limit is 64 MB and an
    arbitrary client could otherwise bury a run's log under one paste.
    """
    text = str(source).strip()
    if len(text) > _MAX_CELL_CHARS:
        text = f"{text[:_MAX_CELL_CHARS]}\n... [truncated, {len(text)} chars]"
    blt.info(f"[tunnel] cell source:\n{text}")


# ----------------------------------------------------------------------------
# Ownership and context unwinding
# ----------------------------------------------------------------------------


def _claim(client_id):
    """Assert this client may touch the context stack, then record activity."""
    global _owner, _last_seen
    if not client_id:
        raise ValueError("client_id is required")
    if _stack and _owner not in (None, client_id):
        idle = time.monotonic() - _last_seen
        raise PermissionError(
            f"Tunnel is mid-context for another client (depth {len(_stack)}, idle "
            f"{idle:.0f}s). Wait for it to finish, or call reset_contexts to force it."
        )
    if not _stack:
        _owner = client_id
    _last_seen = time.monotonic()


def _exit_top(error_message=None):
    """Pop one context, marking the run FAILED when an error message is given."""
    frame = _stack.pop()
    ctx = frame["ctx"]
    if error_message:
        # The Runner derives status from the exception in flight at __exit__, so
        # hand it one directly instead of raising through our own call stack.
        exc = _TunnelRunFailed(error_message)
        ctx.__exit__(type(exc), exc, None)
    else:
        ctx.__exit__(None, None, None)
    return frame


def _unwind_all(reason):
    global _owner
    while _stack:
        frame = _stack[-1]
        try:
            _exit_top(reason)
            blt.warn(f"[tunnel] force-closed run {frame['flow_run_id']}: {reason}")
        except Exception as exc:  # noqa: BLE001 - never let cleanup wedge the loop
            blt.error(f"[tunnel] failed to close {frame['flow_run_id']}: {exc}")
            _stack.pop() if _stack and _stack[-1] is frame else None
    _owner = None


# ----------------------------------------------------------------------------
# Operations — main thread only
# ----------------------------------------------------------------------------


def _context_snapshot():
    return {
        "depth": len(_stack),
        "flow_run_id": blt.flow_run.id,
        "session_id": blt.session.id,
        "flow_name": blt.flow.name,
        "flow_id": blt.flow.id,
        "owner": _owner,
        "stack": [{"flow_run_id": f["flow_run_id"], "name": f["name"]} for f in _stack],
    }


def _op_ping(kwargs):
    _last_seen_touch(kwargs)
    return {
        **_context_snapshot(),
        "idle_timeout_s": _idle_timeout,
        # name -> path, so the shim can advertise dynamic get_<index>_devices().
        "device_indexes": {n: s["path"] for n, s in _device_indexes.items()},
    }


def _last_seen_touch(kwargs):
    global _last_seen
    if kwargs.get("client_id") and kwargs["client_id"] == _owner:
        _last_seen = time.monotonic()


def _op_heartbeat(kwargs):
    _last_seen_touch(kwargs)
    return {"depth": len(_stack), "owner": _owner}


def _op_search_devices(kwargs):
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    call = {}
    for key in ("id", "type", "name", "tags", "archived"):
        if kwargs.get(key) is not None:
            call[key] = kwargs[key]
    for key in ("limit", "offset"):
        if kwargs.get(key):
            call[key] = int(kwargs[key])
    # Projection kwargs are schema-only plumbing and default to the full record,
    # so a caller that omits them gets exactly the pre-analytics behaviour.
    keys = kwargs.get("keys")
    scalars_only = bool(kwargs.get("scalars_only", False))
    include_params = kwargs.get("include_params", True)
    return [
        _device_to_dict(d, keys=keys, scalars_only=scalars_only, include_params=include_params)
        for d in blt.search_devices(**call)
    ]


def _op_search_flows(kwargs):
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    call = {}
    for key in ("name", "flow_ids", "tags"):
        if kwargs.get(key) is not None:
            call[key] = kwargs[key]
    call["limit"] = int(kwargs["limit"]) if kwargs.get("limit") is not None else 1000
    call["offset"] = int(kwargs["offset"]) if kwargs.get("offset") is not None else 0
    return [_flow_to_dict(f) for f in blt.search_flows(**call)]


def _op_search_flow_runs(kwargs):
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    call = {}
    for key in ("flow_id", "device_id", "flow_run_ids"):
        if kwargs.get(key) is not None:
            call[key] = kwargs[key]
    call["limit"] = int(kwargs["limit"]) if kwargs.get("limit") is not None else 250
    call["offset"] = int(kwargs["offset"]) if kwargs.get("offset") is not None else 0
    include_params = kwargs.get("include_params", True)
    include_output = kwargs.get("include_output", True)
    runs = blt.search_flow_run_history(**call)
    return [
        _run_to_dict(r, include_params=include_params, include_output=include_output)
        for r in runs
    ]


def _op_fetch_visualizations(kwargs):
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    ids = list(kwargs.get("ids") or [])
    if len(ids) > 20:
        raise ValueError(
            f"fetch_visualizations accepts at most 20 ids per call, got {len(ids)}; "
            "the client batches larger requests into chunks of 20."
        )
    found = blt.fetch_visualizations(ids)
    out = []
    for vid, viz in (found or {}).items():
        data = getattr(viz, "data", b"") or b""
        if isinstance(data, str):
            data = data.encode("utf-8")
        out.append({
            "id": getattr(viz, "id", vid),
            "type": _viz_type_str(getattr(viz, "type", None)),
            "filename": getattr(viz, "filename", None),
            "flow_run_id": getattr(viz, "flow_run_id", None),
            "timestamp": _iso(getattr(viz, "timestamp", None)),
            "data_b64": base64.b64encode(data).decode("ascii"),
        })
    return out


def _ensure_blt_analytics_on_path():
    """Put the repo root on ``sys.path`` so ``blt_analytics`` is importable.

    The flow lives in ``flows/`` and the package sits at the repo root, so the
    parent of this file's directory is added. Only the ``space_schema`` path needs
    the package; the basic read ops stay standalone, so this is called from there
    (not at module import) — an old Runner without the package still serves
    ``search_flows`` / ``search_flow_runs`` etc.

    Appended (not inserted at ``sys.path[0]``): the repo root must never take
    precedence over the Runner's own modules. A permanent ``insert(0, ...)`` could
    shadow a Runner stdlib/site module that happens to share a name with a top-level
    file here; appending only ever adds ``blt_analytics`` as a fallback import root.
    """
    flow_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(flow_dir)
    if repo_root not in sys.path:
        sys.path.append(repo_root)


def _import_digest():
    """Import ``blt_analytics.digest``. Raises on failure; the caller wraps that
    into the RuntimeError the client falls back on."""
    _ensure_blt_analytics_on_path()
    from blt_analytics import digest
    return digest


def _import_paging():
    """Import ``blt_analytics.paging`` (the shared flow-run pager)."""
    _ensure_blt_analytics_on_path()
    from blt_analytics import paging
    return paging


# ----------------------------------------------------------------------------
# space_schema orchestration — runs on the HTTP worker thread, hands each blt.*
# read to the main thread one small job at a time
# ----------------------------------------------------------------------------


def _call_on_main(fn, *, timeout=_JOB_TIMEOUT_S):
    """Run ``fn()`` on the main-thread executor and return its result.

    While the executor loop is live this hands ``fn`` over as a *small* job on
    ``_JOBS`` and blocks for the reply, so between our fetches the executor returns
    to ``_JOBS.get`` and other clients' ops interleave instead of waiting out the
    whole build. With no executor running — a unit test driving ``_op_space_schema``
    directly on the main thread — ``fn()`` runs inline on the caller. Tests observe
    the hand-over by replacing this function with a recording synchronous runner.

    A failed main-thread job is re-raised here as :class:`_MainThreadJobError`, so
    the orchestration (and the shrink-and-retry pager) sees a real exception — the
    same shape it would if the ``blt.*`` call had run inline and raised.
    """
    if not _executor_live.is_set():
        return fn()
    job = _Job(fn=fn)
    _JOBS.put(job)
    try:
        ok, payload = job.reply.get(timeout=timeout)
    except queue.Empty as exc:
        raise TimeoutError(f"main-thread job timed out after {timeout:.0f}s") from exc
    if ok:
        return payload
    raise _MainThreadJobError(payload.get("type", "Error"), payload.get("message", ""))


def _check_deadline(deadline, what="space_schema build"):
    """Raise ``TimeoutError`` if the overall build budget has been exceeded."""
    if deadline is not None and time.monotonic() > deadline:
        raise TimeoutError(f"{what} exceeded its budget")


def _fetch_devices_paged(deadline=None):
    """Fetch every device as a §1 record, a page per main-thread job (dedupe by id).

    Each page is a discrete job, so the executor returns to ``_JOBS.get`` between
    pages and a large space's device pull cannot monopolize it. Serialization runs
    inside the job (on the main thread), so no ``blt`` attribute access happens off
    the main thread.
    """
    by_id = {}
    offset = 0
    while True:
        _check_deadline(deadline)
        page = _call_on_main(
            lambda o=offset: [
                _device_to_dict(d, include_params=True)
                for d in blt.search_devices(limit=_DEVICE_PAGE_SIZE, offset=o)
            ]
        )
        if not page:
            break
        for record in page:
            by_id[record["id"]] = record
        offset += len(page)
        if len(page) < _DEVICE_PAGE_SIZE:
            break
    return list(by_id.values())


def _fetch_flows(deadline=None):
    """Fetch every flow as a §1 record, as one small main-thread job."""
    _check_deadline(deadline)
    return _call_on_main(lambda: [_flow_to_dict(f) for f in blt.search_flows()])


def _page_flow_runs(flow_id, max_runs, deadline=None):
    """Page one flow's run history via the shared pager. Returns ``(records,
    hit_cap)`` — §1 run records, and whether ``max_runs`` capped the pull.

    The robustness (shrink-and-retry, poison skip, abandon-after-3, dedupe by id,
    offset by raw page length) lives in :func:`blt_analytics.paging.page_flow_runs`;
    here we wire the fetch and log each recovery step in the tunnel's voice. Each
    page fetch — and each recovery log — is a small main-thread job (see
    :func:`_call_on_main`), so other clients' ops interleave between pages. The run
    objects are serialized to records *inside* the fetch job, on the main thread.
    """
    paging = _import_paging()

    def fetch(offset, limit):
        _check_deadline(deadline)
        return _call_on_main(
            lambda: [
                _run_to_dict(run)
                for run in blt.search_flow_run_history(
                    flow_id=flow_id, limit=limit, offset=offset
                )
            ]
        )

    def _log(level, message):
        _call_on_main(lambda: {"info": blt.info, "warn": blt.warn}[level](message))

    def on_skip(event):
        if event.kind == "shrink":
            _log(
                "info",
                f"[tunnel] space_schema: page too large in flow {flow_id} at "
                f"offset {event.offset}; retrying with a smaller page ({event.size})",
            )
        elif event.kind == "skip":
            _log(
                "warn",
                f"[tunnel] space_schema: skipping poison run in flow {flow_id} at "
                f"offset {event.offset} ({event.error!r})",
            )
        else:  # "abandon"
            _log(
                "warn",
                f"[tunnel] space_schema: 3 consecutive size-1 failures in flow "
                f"{flow_id} near offset {event.offset} ({event.error!r}); "
                f"abandoning this flow",
            )

    # fetch already yields §1 records; the pager dedupes them by their ``id`` key.
    records, hit_cap = paging.page_flow_runs(
        fetch, page_size=_RUN_PAGE_SIZE, max_runs=max_runs, on_skip=on_skip
    )
    return records, hit_cap


def _op_space_schema(kwargs):
    """Build (or return the cached) measurement-free digest of the whole space.

    Orchestrated on the calling (HTTP worker) thread: the expensive ``blt.*`` reads
    are handed to the main-thread executor as many small jobs — one device page, the
    flows, one run page at a time — so the executor keeps returning to ``_JOBS.get``
    and other clients' ops interleave rather than 504-ing behind the whole build. The
    digest itself is pure and is built here, off the main thread. ``_schema_lock``
    serializes concurrent builds: a second caller waits, then returns the same cache.

    The build has its own generous budget (``_SCHEMA_BUILD_BUDGET_S``), separate from
    the per-job ``_JOB_TIMEOUT_S``; the HTTP request waits for the whole build (it is
    not run as a single capped ``_JOBS`` job — see :meth:`_Handler.do_POST`).
    """
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    refresh = bool(kwargs.get("refresh", False))
    max_runs_per_flow = int(kwargs.get("max_runs_per_flow", 2000))

    with _schema_lock:
        if not refresh and _schema_cache.get("digest") is not None:
            return _schema_cache["digest"]

        try:
            digest_mod = _import_digest()
        except Exception as exc:  # noqa: BLE001 - old Runner without the package
            raise RuntimeError(
                f"space_schema unavailable on this Runner: {exc}"
            ) from exc

        deadline = time.monotonic() + _SCHEMA_BUILD_BUDGET_S

        # Build from the device cache when it is ready, instead of re-paging every
        # device (spec §6). The store is swapped atomically, so one reference read
        # gives a consistent snapshot.
        if _cache_meta["state"] == "ready":
            device_records = list(_device_store["records"].values())
        else:
            device_records = _fetch_devices_paged(deadline)
        flow_records = _fetch_flows(deadline)

        run_records = []
        truncated_flow_ids = set()
        for flow in flow_records:
            records, hit_cap = _page_flow_runs(flow["id"], max_runs_per_flow, deadline)
            run_records.extend(records)
            if hit_cap:
                truncated_flow_ids.add(flow["id"])

        digest = digest_mod.build_digest(device_records, flow_records, run_records)

        # The digest builder has no notion of capping; stamp the truncation flags it
        # left at their defaults once we know which flows we stopped short on. Resolve
        # each flow id to its digest key via the digest's own public keying rule, so
        # the stamp lands on the right entry even when flow names clash.
        if truncated_flow_ids:
            digest["totals"]["runs_truncated"] = True
        key_by_id = digest_mod.flow_key(flow_records)
        for flow_id in truncated_flow_ids:
            entry = digest["flows"].get(key_by_id.get(flow_id))
            if entry is not None:
                entry["truncated"] = True
                entry["runs_sampled"] = entry.get("run_count", 0)

        _schema_cache["digest"] = digest
        return digest


# ----------------------------------------------------------------------------
# Device cache (spec §6) — configuration, store, indexing, persistence, loading
# ----------------------------------------------------------------------------


def _parse_device_indexes(raw):
    """Parse the ``device_indexes`` flow param (a JSON string) into a spec dict.

    Each entry maps an index name to a dotted param path, as either a bare string
    (``"hierarchy.wafer"``) or an object (``{"path": ..., "device_type": "Die"}``).
    Bad JSON or a malformed entry logs a clear ``blt.error`` and is skipped — the
    tunnel still starts, just without that index.
    """
    if not raw:
        return {}
    spec = raw
    if isinstance(spec, str):
        try:
            spec = json.loads(spec)
        except (ValueError, TypeError) as exc:
            blt.error(
                f"[tunnel] device_indexes is not valid JSON ({exc}); starting with "
                f"no device indexes"
            )
            return {}
    if not isinstance(spec, dict):
        blt.error(
            "[tunnel] device_indexes must be a JSON object {name: path | {path, "
            "device_type}}; starting with no device indexes"
        )
        return {}
    out = {}
    for name, value in spec.items():
        if isinstance(value, str) and value:
            out[str(name)] = {"path": value, "device_type": None}
        elif isinstance(value, dict) and value.get("path"):
            out[str(name)] = {
                "path": value["path"],
                "device_type": value.get("device_type"),
            }
        else:
            blt.error(
                f"[tunnel] device_indexes[{name!r}] must be a dotted path string or an "
                f"object with a 'path'; skipping it"
            )
    return out


def _dig(container, dotted_path):
    """Return the value at ``dotted_path`` in nested dicts, or ``_MISSING``."""
    current = container
    for segment in dotted_path.split("."):
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        else:
            return _MISSING
    return current


def _index_value(record, device_type, path):
    """The ``str(value)`` this record indexes under, or ``None`` if it is not indexed.

    A device of the wrong type (when the index restricts one), one missing the path,
    or one whose value at the path is ``None`` is not indexed.
    """
    if device_type is not None and record.get("type") != device_type:
        return None
    value = _dig(record.get("params") or {}, path)
    if value is _MISSING or value is None:
        return None
    return str(value)


def _build_index(records_by_id):
    """Build every configured index ``{name: {str(value): [ids]}}`` from records."""
    index = {name: {} for name in _device_indexes}
    for name, spec in _device_indexes.items():
        device_type = spec.get("device_type")
        path = spec["path"]
        bucket = index[name]
        for rid, record in records_by_id.items():
            key = _index_value(record, device_type, path)
            if key is None:
                continue
            bucket.setdefault(key, []).append(rid)
    return index


def _iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _space_cache_key():
    """Stable per-space directory key: the space id if the Runner exposes one, else
    the root flow id. A real Runner has no ``blt.space``, so this falls back cleanly."""
    space = getattr(blt, "space", None)
    if space is not None and getattr(space, "id", None):
        return str(space.id)
    return str(getattr(getattr(blt, "flow", None), "id", None) or "unknown")


def _device_cache_space_dir():
    return os.path.join(_device_cache_dir, _space_cache_key())


def _device_cache_path():
    return os.path.join(_device_cache_space_dir(), "devices.pkl")


def _device_cache_meta_path():
    return os.path.join(_device_cache_space_dir(), "meta.json")


def _publish_device_store(records_by_id, index, built_at):
    """Atomically swap in a freshly built store (single reference assignment)."""
    global _device_store
    _device_store = {"records": records_by_id, "index": index, "built_at": built_at}


def _persist_device_cache(records_by_id, index, built_at):
    """Write the store to disk atomically (tmp + ``os.replace``). The file holds
    space data, so the dir is 0700 and the files 0600."""
    space_dir = _device_cache_space_dir()
    os.makedirs(space_dir, mode=0o700, exist_ok=True)
    try:
        os.chmod(space_dir, 0o700)
    except OSError:
        pass

    def _atomic_write(path, binary, writer):
        tmp = f"{path}.tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            opener = os.fdopen(fd, "wb") if binary else os.fdopen(fd, "w", encoding="utf-8")
            with opener as fh:
                writer(fh)
        except BaseException:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise
        os.replace(tmp, path)

    meta = {
        "built_at": built_at,
        "count": len(records_by_id),
        "indexes": _device_indexes,
        "space_key": _space_cache_key(),
    }
    _atomic_write(
        _device_cache_path(), True,
        lambda fh: pickle.dump(
            {"records": records_by_id, "index": index}, fh,
            protocol=pickle.HIGHEST_PROTOCOL,
        ),
    )
    _atomic_write(_device_cache_meta_path(), False, lambda fh: json.dump(meta, fh))


def _load_device_cache_from_disk():
    """Load a persisted store if present and valid; publish it and mark ready.

    Returns True on success. If the configured indexes differ from the persisted
    ones, the records are kept but the index is rebuilt (no re-fetch needed).
    """
    pkl_path = _device_cache_path()
    meta_path = _device_cache_meta_path()
    if not (os.path.exists(pkl_path) and os.path.exists(meta_path)):
        return False
    try:
        with open(meta_path, encoding="utf-8") as fh:
            meta = json.load(fh)
        with open(pkl_path, "rb") as fh:
            payload = pickle.load(fh)
    except Exception as exc:  # noqa: BLE001 - a corrupt cache must not wedge start
        blt.warn(f"[tunnel] could not load persisted device cache: {exc}")
        return False

    records_by_id = payload.get("records") or {}
    index = payload.get("index") or {}
    if (meta.get("indexes") or {}) != _device_indexes:
        index = _build_index(records_by_id)

    built_at = meta.get("built_at")
    _publish_device_store(records_by_id, index, built_at)
    _cache_meta.update(
        state="ready", count=len(records_by_id), loaded=len(records_by_id),
        built_at=built_at, error=None,
    )
    return True


def _fetch_device_cache_pages(deadline):
    """Full paged device load, a page per main-thread job (dedupe by id).

    Each page is a discrete ``_call_on_main`` job (limit 1000), so the executor keeps
    returning to ``_JOBS.get`` and other clients' ops interleave rather than waiting
    out the whole 250k pull. Serialization happens inside the job, on the main thread.
    """
    by_id = {}
    offset = 0
    while True:
        _check_deadline(deadline, "device cache load")
        page = _call_on_main(
            lambda o=offset: [
                _device_to_dict(d, include_params=True)
                for d in blt.search_devices(limit=_DEVICE_CACHE_PAGE_SIZE, offset=o)
            ]
        )
        if not page:
            break
        for record in page:
            by_id[record["id"]] = record
        offset += len(page)
        _cache_meta["loaded"] = len(by_id)  # live progress for status
        if len(page) < _DEVICE_CACHE_PAGE_SIZE:
            break
    return by_id


def _reload_device_cache(force):
    """Run (or skip) a full load under the build lock; swap on success, keep old on
    failure. Concurrent callers serialize here and a second one returns immediately
    once the first has made the cache ready.

    Raises on a failed load when there is no previous store to fall back on; when a
    previous store exists it is kept and the failure is recorded in the status.
    """
    with _device_cache_lock:
        if not force and _cache_meta["state"] == "ready":
            return
        had_data = bool(_device_store["records"])
        _cache_meta.update(
            state="loading", started_at=_iso_now(), loaded=0, error=None
        )
        deadline = time.monotonic() + _DEVICE_CACHE_BUDGET_S
        try:
            records_by_id = _fetch_device_cache_pages(deadline)
            index = _build_index(records_by_id)
        except Exception as exc:  # noqa: BLE001 - keep the old cache on any failure
            _cache_meta["error"] = f"{type(exc).__name__}: {exc}"
            _cache_meta["state"] = "ready" if had_data else "error"
            blt.error(
                f"[tunnel] device cache load failed: {type(exc).__name__}: {exc}; "
                f"{'kept the previous cache' if had_data else 'cache is empty'}"
            )
            raise

        built_at = _iso_now()
        _publish_device_store(records_by_id, index, built_at)
        try:
            _persist_device_cache(records_by_id, index, built_at)
        except Exception as exc:  # noqa: BLE001 - a failed persist is not fatal
            blt.warn(f"[tunnel] device cache persist failed: {type(exc).__name__}: {exc}")
        _cache_meta.update(
            state="ready", count=len(records_by_id), loaded=len(records_by_id),
            built_at=built_at, error=None,
        )
        blt.info(f"[tunnel] device cache ready: {len(records_by_id)} device(s)")


def _reindex_ids(records_by_id, index, ids):
    """Re-place ``ids`` in every index: drop them from all buckets, then re-add from
    ``records_by_id`` wherever they are still present. Handles a value that changed
    bucket, a device that dropped out of its type filter, and a vanished id."""
    id_set = set(ids)
    for buckets in index.values():
        for key in list(buckets):
            remaining = [i for i in buckets[key] if i not in id_set]
            if remaining:
                buckets[key] = remaining
            else:
                del buckets[key]
    for name, spec in _device_indexes.items():
        device_type = spec.get("device_type")
        path = spec["path"]
        for rid in ids:
            record = records_by_id.get(rid)
            if record is None:
                continue
            key = _index_value(record, device_type, path)
            if key is None:
                continue
            index[name].setdefault(key, []).append(rid)


def _refresh_index_value(index_name, value):
    """Re-fetch one index value's known device ids via ``search_devices(id=[…])`` in
    chunks of 500, update their records, re-index them, and drop ids that vanished.

    Cannot discover *new* devices for the value (that needs a full
    ``refresh_device_cache``) — it only revisits the ids already in the bucket.
    """
    with _device_cache_lock:
        store = _device_store
        known_ids = list(store["index"].get(index_name, {}).get(str(value), []))
        if not known_ids:
            return
        found = {}
        for start in range(0, len(known_ids), _DEVICE_REFRESH_CHUNK):
            chunk = known_ids[start:start + _DEVICE_REFRESH_CHUNK]
            page = _call_on_main(
                lambda c=chunk: [
                    _device_to_dict(d, include_params=True)
                    for d in blt.search_devices(id=c)
                ]
            )
            for record in page:
                found[record["id"]] = record

        # Build a fresh store so readers never see a half-updated index.
        records_by_id = dict(store["records"])
        for rid in known_ids:
            if rid in found:
                records_by_id[rid] = found[rid]
            else:
                records_by_id.pop(rid, None)  # vanished
        index = {name: {k: list(v) for k, v in buckets.items()}
                 for name, buckets in store["index"].items()}
        _reindex_ids(records_by_id, index, known_ids)
        _publish_device_store(records_by_id, index, store["built_at"])
        _cache_meta["count"] = len(records_by_id)
        try:
            _persist_device_cache(records_by_id, index, store["built_at"])
        except Exception as exc:  # noqa: BLE001
            blt.warn(f"[tunnel] device cache persist failed: {type(exc).__name__}: {exc}")


def _write_through_device(record):
    """Keep the cache in step with a tunnel write: replace the record and fix only the
    index entries whose value actually changed. Lock-free (so it never deadlocks a
    main-thread op against a worker-thread load); a concurrent full reload wins."""
    if _cache_meta["state"] != "ready":
        return
    store = _device_store
    records = store["records"]
    rid = record["id"]
    if rid not in records:
        return  # not cached; a full reload would pick it up
    old = records[rid]
    records[rid] = record
    for name, spec in _device_indexes.items():
        device_type = spec.get("device_type")
        path = spec["path"]
        old_key = _index_value(old, device_type, path)
        new_key = _index_value(record, device_type, path)
        if old_key == new_key:
            continue
        buckets = store["index"].setdefault(name, {})
        if old_key is not None:
            lst = buckets.get(old_key)
            if lst and rid in lst:
                lst = [i for i in lst if i != rid]
                if lst:
                    buckets[old_key] = lst
                else:
                    del buckets[old_key]
        if new_key is not None:
            buckets.setdefault(new_key, [])
            if rid not in buckets[new_key]:
                buckets[new_key].append(rid)


def _device_cache_status():
    store = _device_store
    indexes = {}
    for name, spec in _device_indexes.items():
        indexes[name] = {
            "path": spec["path"],
            "device_type": spec.get("device_type"),
            "values": len(store["index"].get(name, {})),
        }
    return {
        "state": _cache_meta["state"],
        "count": _cache_meta["count"],
        "loaded": _cache_meta["loaded"],
        "built_at": _cache_meta["built_at"],
        "indexes": indexes,
        "persisted_path": _device_cache_path(),
        "error": _cache_meta["error"],
    }


def _op_device_cache_status(kwargs):
    """Report the cache state, counts, indexes and persisted path. Never loads."""
    _last_seen_touch(kwargs)
    return _device_cache_status()


def _op_refresh_device_cache(kwargs):
    """Full reload: replace the cache atomically on success, keep the old one on
    failure. ``wait=True`` (default) blocks and returns the post-reload status;
    ``wait=False`` kicks it off in the background and returns the current status."""
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    wait = kwargs.get("wait", True)
    if wait:
        try:
            _reload_device_cache(force=True)
        except Exception:  # noqa: BLE001 - the failure is recorded in the status
            pass
        return _device_cache_status()

    def _background():
        try:
            _reload_device_cache(force=True)
        except Exception:  # noqa: BLE001
            pass

    threading.Thread(target=_background, name="device-cache-refresh", daemon=True).start()
    return _device_cache_status()


def _require_index(index_name):
    if index_name not in _device_indexes:
        configured = ", ".join(sorted(_device_indexes)) or "(none configured)"
        raise ValueError(
            f"Unknown device index {index_name!r}. Configured indexes: {configured}."
        )


def _op_cached_devices(kwargs):
    """Return the cached device records for one index value. Loads first if cold.

    ``refresh=True`` re-fetches that value's known ids (spec §6) before serving.
    """
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    index_name = kwargs.get("index")
    value = kwargs.get("value")
    _require_index(index_name)
    if _cache_meta["state"] != "ready":
        _reload_device_cache(force=False)
    if kwargs.get("refresh", False):
        _refresh_index_value(index_name, value)
    store = _device_store
    ids = store["index"].get(index_name, {}).get(str(value), [])
    records = store["records"]
    return [records[i] for i in ids if i in records]


def _op_cached_devices_query(kwargs):
    """Return cached device records, optionally type-filtered and key-projected, paged.

    Loads first if cold. ``device_type`` keeps one type (or a list of types); ``keys``
    projects to those top-level param keys. ``offset``/``limit`` page the result — a
    whole-space pull would otherwise make a response far larger than the request cap,
    so the shim pages transparently. Returns ``{devices, total, offset, limit}``.
    """
    _last_seen_touch(kwargs)
    _stats["reads_served"] += 1
    if _cache_meta["state"] != "ready":
        _reload_device_cache(force=False)

    device_type = kwargs.get("device_type")
    keys = kwargs.get("keys")
    offset = int(kwargs.get("offset") or 0)
    limit = int(kwargs.get("limit") or 0)

    store = _device_store
    records = sorted(store["records"].values(), key=lambda r: r["id"])
    if device_type is not None:
        wanted = set(device_type if isinstance(device_type, (list, tuple, set)) else [device_type])
        records = [r for r in records if r.get("type") in wanted]
    total = len(records)
    if offset:
        records = records[offset:]
    if limit:
        records = records[:limit]

    if keys is None:
        out = records
    else:
        out = [{**r, "params": _project_params(r.get("params") or {}, keys)} for r in records]
    return {"devices": out, "total": total, "offset": offset, "limit": limit}


def _warm_device_cache_async():
    """Background warm at start: wait for the executor, then do a full load.

    Waits for ``_executor_live`` so the load hands its device pages to the main
    thread (as ``space_schema`` does) rather than running ``blt.*`` off it.
    """
    _executor_live.wait(timeout=30.0)
    try:
        _reload_device_cache(force=False)
    except Exception:  # noqa: BLE001 - the failure is recorded in the status
        pass


def _init_device_cache():
    """Parse config and load a persisted cache if present. Call at flow start, after
    the flow params are available and before the executor loop."""
    global _device_indexes, _warm_device_cache, _device_cache_dir
    _device_indexes = _parse_device_indexes(blt.params.get("device_indexes", "{}"))
    configured_dir = blt.params.get("device_cache_dir") or _DEFAULT_CACHE_DIR
    _device_cache_dir = os.path.expanduser(configured_dir)
    # Warm defaults on when any index is configured.
    _warm_device_cache = bool(blt.params.get("warm_device_cache", bool(_device_indexes)))

    if not _device_indexes:
        return
    try:
        if _load_device_cache_from_disk():
            blt.info(
                f"[tunnel] loaded {_cache_meta['count']} device(s) from the persisted "
                f"cache at {_device_cache_path()} (built {_cache_meta['built_at']})"
            )
    except Exception as exc:  # noqa: BLE001 - never let cache init stop the tunnel
        blt.warn(f"[tunnel] device cache init failed: {type(exc).__name__}: {exc}")


def _op_get_device_params(kwargs):
    _last_seen_touch(kwargs)
    matches = blt.search_devices(id=kwargs.get("id") or "")
    if not matches:
        raise FileNotFoundError(f"No device with id {kwargs.get('id')}")
    return _params_to_dict(matches[0].params)


def _op_enter_flow_run(kwargs):
    """Enter a child flow-run context and leave it open for later requests."""
    _claim(kwargs.get("client_id"))

    # devices/parameters are always explicit: omitting them makes the child
    # inherit the *tunnel's* devices and params, which is never what we want.
    ctx = blt.enter_new_flow_run(
        name=kwargs.get("name"),
        flow_id=kwargs.get("flow_id") or blt.flow.id,
        devices=_resolve_devices(kwargs.get("device_ids")),
        parameters=kwargs.get("parameters") or {},
    )
    _stack.append({
        "ctx": ctx,
        "flow_run_id": blt.flow_run.id,
        "name": kwargs.get("name"),
    })
    _stats["flow_runs_entered"] += 1
    blt.info(f"[tunnel] entered run {blt.flow_run.id} (depth {len(_stack)})")

    # Guarded: a malformed cell payload must not redden a run that opened fine,
    # and the client is mid-`with` here so it cannot handle the failure anyway.
    cell_source = kwargs.get("cell_source")
    if cell_source:
        try:
            _log_cell_source(cell_source)
        except Exception as exc:  # noqa: BLE001 - logging is never worth the run
            blt.warn(f"[tunnel] could not log cell source: {type(exc).__name__}: {exc}")

    return _context_snapshot()


def _op_exit_flow_run(kwargs):
    _claim(kwargs.get("client_id"))
    if not _stack:
        raise ValueError("No open flow run context to exit")

    expected = kwargs.get("flow_run_id")
    if expected and _stack[-1]["flow_run_id"] != expected:
        raise ValueError(
            f"Context mismatch: innermost run is {_stack[-1]['flow_run_id']}, "
            f"client tried to close {expected}. Contexts must exit in LIFO order."
        )

    frame = _exit_top(kwargs.get("error_message"))
    blt.info(f"[tunnel] exited run {frame['flow_run_id']} (depth {len(_stack)})")
    return _context_snapshot()


def _op_store_visualizations(kwargs):
    _claim(kwargs.get("client_id"))
    if not _stack:
        raise ValueError(
            "No open flow run context. Visualizations attach to the current run, so "
            "enter a run first (or use the v1 tunnel's figures= argument)."
        )
    visualizations = _decode_visualizations(kwargs.get("visualizations"))
    # Module-level names are bound methods of blt.context, which the Runner
    # rewrites in place on enter, so this targets the innermost open run.
    metas = blt.store_visualizations(visualizations)
    _stats["visualizations_stored"] += len(metas)
    return {
        "stored": len(metas),
        "visualization_ids": [m.id for m in metas],
        "flow_run_id": blt.flow_run.id,
    }


def _op_set_output(kwargs):
    _claim(kwargs.get("client_id"))
    values = kwargs.get("values") or {}
    if not values:
        return {"written": 0}
    blt.output.update(values)
    return {"written": len(values), "flow_run_id": blt.flow_run.id}


def _op_update_device_params(kwargs):
    """Write device params via ``params.update`` — the only reliable write path."""
    _claim(kwargs.get("client_id"))
    device_id = kwargs.get("id")
    values = kwargs.get("values") or {}
    matches = blt.search_devices(id=device_id or "")
    if not matches:
        raise FileNotFoundError(f"No device with id {device_id}")
    device = matches[0]

    for key in kwargs.get("delete") or []:
        # Neither assignment nor update removes a key; a guarded del does.
        if key in device.params:
            del device.params[key]
    if values:
        device.params.update(values)

    # Write-through: keep the cache in step with a write made through the tunnel, so
    # it never serves data staler than what this very tunnel just changed (spec §6).
    try:
        _write_through_device(_device_to_dict(device, include_params=True))
    except Exception as exc:  # noqa: BLE001 - a cache hiccup must not fail the write
        blt.warn(f"[tunnel] device cache write-through failed: {type(exc).__name__}: {exc}")

    _stats["device_writes"] += 1
    return {"updated": sorted(values), "deleted": kwargs.get("delete") or [],
            "params": _params_to_dict(device.params)}


def _op_new_flow_run(kwargs):
    """One-shot completed history entry, for callers that need no open context."""
    _last_seen_touch(kwargs)
    status_name = (kwargs.get("status") or "FINISHED").upper()
    if status_name not in ("FINISHED", "FAILED"):
        raise ValueError("status must be 'FINISHED' or 'FAILED'")
    call = {
        "name": kwargs.get("name"),
        "devices": _resolve_devices(kwargs.get("device_ids")),
        "output": kwargs.get("output"),
        "parameters": kwargs.get("parameters"),
        "error_message": kwargs.get("error_message"),
        "status": getattr(blt.FlowRunStatus, status_name),
        "flow_id": kwargs.get("flow_id") or blt.flow.id,
    }
    return blt.new_flow_run(**{k: v for k, v in call.items() if v is not None})


def _op_reset_contexts(kwargs):
    """Escape hatch: force-close every open context, whoever owns it."""
    depth = len(_stack)
    _unwind_all(kwargs.get("reason") or "reset_contexts called by a client")
    _stats["contexts_reclaimed"] += depth
    return {"closed": depth}


def _op_log(kwargs):
    _last_seen_touch(kwargs)
    level = (kwargs.get("level") or "info").lower()
    message = f"[tunnel] {kwargs.get('message', '')}"
    {"info": blt.info, "warn": blt.warn, "error": blt.error}.get(level, blt.info)(message)
    return True


_DISPATCH = {
    "ping": _op_ping,
    "heartbeat": _op_heartbeat,
    "search_devices": _op_search_devices,
    "search_flows": _op_search_flows,
    "search_flow_runs": _op_search_flow_runs,
    "fetch_visualizations": _op_fetch_visualizations,
    "get_device_params": _op_get_device_params,
    "enter_flow_run": _op_enter_flow_run,
    "exit_flow_run": _op_exit_flow_run,
    "store_visualizations": _op_store_visualizations,
    "set_output": _op_set_output,
    "update_device_params": _op_update_device_params,
    "new_flow_run": _op_new_flow_run,
    "reset_contexts": _op_reset_contexts,
    "log": _op_log,
    # Device cache: status never loads, so it is a fast executor job.
    "device_cache_status": _op_device_cache_status,
}

# Ops orchestrated on the HTTP worker thread rather than run as a single main-thread
# job. They submit their own small jobs to ``_JOBS`` (keeping ``blt.*`` on the main
# thread) and so may legitimately outlast ``_JOB_TIMEOUT_S``; the handler does not
# cap them, their own budget does. Kept out of ``_DISPATCH`` so the executor never
# runs one inline (which is exactly the blockage this split fixes).
_WORKER_DISPATCH = {
    "space_schema": _op_space_schema,
    # These may trigger a full device-cache load (which hands its own small jobs to
    # the executor), so they are orchestrated on the worker thread rather than run as
    # a single capped executor job — running one inline would deadlock the executor.
    "refresh_device_cache": _op_refresh_device_cache,
    "cached_devices": _op_cached_devices,
    "cached_devices_query": _op_cached_devices_query,
}

# The allowlist is the union of both tables.
_ALL_OPS = set(_DISPATCH) | set(_WORKER_DISPATCH)


# ----------------------------------------------------------------------------
# HTTP layer — worker threads; never calls blt.* directly
# ----------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # noqa: A003 - BaseHTTPRequestHandler hook
        pass

    def _reply(self, code, payload):
        body = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, code, kind, message):
        self._reply(code, {"ok": False, "error": {"type": kind, "message": message}})

    def _authorized(self):
        if (self.headers.get("Host") or "").split(":")[0] not in ("127.0.0.1", "localhost", ""):
            return False
        header = self.headers.get("Authorization") or ""
        prefix = "Bearer "
        if not header.startswith(prefix):
            return False
        return hmac.compare_digest(header[len(prefix) :], _TOKEN)

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler hook
        if self.path.rstrip("/") != "/rpc":
            self._error(404, "ValueError", "not found")
            return
        if not self._authorized():
            self._error(401, "PermissionError", "unauthorized")
            return

        length = int(self.headers.get("Content-Length") or 0)
        if length > _MAX_BODY_BYTES:
            self._error(413, "ValueError", "body too large")
            return
        try:
            request = json.loads(self.rfile.read(length).decode("utf-8"))
            op = request["op"]
        except (ValueError, KeyError, UnicodeDecodeError) as exc:
            self._error(400, "ValueError", f"bad request: {exc}")
            return
        if op not in _ALL_OPS:
            self._error(400, "ValueError", f"unknown op {op!r}")
            return

        kwargs = request.get("kwargs") or {}

        if op in _WORKER_DISPATCH:
            # Orchestrated on this worker thread: it submits its own small jobs to
            # the executor, so other clients interleave, and it is not bounded by the
            # per-job timeout — the operation's own budget bounds it. The HTTP request
            # waits here for the whole build.
            _stats["requests_served"] += 1
            try:
                result = _WORKER_DISPATCH[op](kwargs)
            except Exception as exc:  # noqa: BLE001 - marshal every failure to the client
                _stats["errors"] += 1
                blt.warn(f"tunnel op {op!r} failed: {type(exc).__name__}: {exc}")
                self._reply(200, {"ok": False, "error": {
                    "type": type(exc).__name__, "message": str(exc)}})
            else:
                self._reply(200, {"ok": True, "result": result})
            return

        job = _Job(op, kwargs)
        _JOBS.put(job)
        try:
            ok, payload = job.reply.get(timeout=_JOB_TIMEOUT_S)
        except queue.Empty:
            self._error(504, "TimeoutError", f"op {op!r} timed out after {_JOB_TIMEOUT_S}s")
            return
        self._reply(200, {"ok": True, "result": payload} if ok else {"ok": False, "error": payload})


# ----------------------------------------------------------------------------
# Lifecycle
# ----------------------------------------------------------------------------


def _write_connection_file(url):
    payload = {"url": url, "token": _TOKEN, "flow_run_id": blt.flow_run.id}
    fd = os.open(CONNECTION_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)


def _raise_keyboard_interrupt(_signum, _frame):
    raise KeyboardInterrupt


def _maybe_reclaim_idle():
    """Unwind abandoned contexts once the owner has gone silent past the timeout.

    Called both on an empty queue *and* after every job: a space_schema build (or a
    second busy client) keeps ``_JOBS`` non-empty for long stretches, so gating this
    on ``queue.Empty`` alone would starve the watchdog and let an abandoned run hang
    in RUNNING. Driving it off elapsed idle time instead makes it fire regardless.
    """
    if _stack and (time.monotonic() - _last_seen) > _idle_timeout:
        idle = time.monotonic() - _last_seen
        blt.warn(
            f"[tunnel] client silent for {idle:.0f}s with {len(_stack)} open "
            f"context(s); unwinding so the runs do not hang in RUNNING"
        )
        reclaimed = len(_stack)
        _unwind_all(f"context abandoned: client silent for {idle:.0f}s")
        _stats["contexts_reclaimed"] += reclaimed


def _run_executor():
    """Drain jobs on the main thread; police idle contexts between every job.

    A job is either a client op (``job.op``, dispatched through ``_DISPATCH``) or a
    bare callable (``job.fn``) submitted by ``space_schema``'s worker-thread
    orchestration. Only client ops count toward ``requests_served``/``errors`` and
    the periodic stats flush — the orchestration's sub-jobs are internal plumbing.
    """
    _executor_live.set()
    try:
        while not _stop.is_set():
            try:
                job = _JOBS.get(timeout=0.25)
            except queue.Empty:
                _maybe_reclaim_idle()
                continue

            is_client_op = job.op is not None
            if is_client_op:
                _stats["requests_served"] += 1
            try:
                result = job.fn() if job.fn is not None else _DISPATCH[job.op](job.kwargs)
                job.reply.put((True, result))
            except Exception as exc:  # noqa: BLE001 - marshal every failure to the caller
                # A sub-job failure (e.g. a page the pager will shrink past) is not a
                # tunnel error: the orchestration handles and logs it. Only count and
                # announce real client-op failures.
                if is_client_op:
                    _stats["errors"] += 1
                    blt.warn(f"tunnel op {job.op!r} failed: {type(exc).__name__}: {exc}")
                job.reply.put((False, {"type": type(exc).__name__, "message": str(exc)}))

            # Run the watchdog after every job too, not only on an empty queue.
            _maybe_reclaim_idle()

            if is_client_op and _stats["requests_served"] % 20 == 0 and not _stack:
                blt.output.update(dict(_stats))
    finally:
        _executor_live.clear()


def tunnel_session_server_flow():
    global _idle_timeout
    port = int(blt.params.get("port", 8766))
    _idle_timeout = float(blt.params.get("idle_timeout", 1800))

    # Parse device_indexes/warm_device_cache/device_cache_dir and load any persisted
    # cache (spec §6). Bad JSON logs a blt.error and the tunnel still starts.
    _init_device_cache()

    try:
        httpd = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    except OSError as exc:
        blt.error(
            f"Cannot bind 127.0.0.1:{port} ({exc}). Another tunnel run is probably "
            f"still alive — kill it, or start this flow with a different 'port'."
        )
        blt.output.update({"status": "failed", "error": f"port {port} unavailable"})
        raise
    httpd.daemon_threads = True
    url = f"http://127.0.0.1:{port}"

    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)
    threading.Thread(target=httpd.serve_forever, name="tunnel-http", daemon=True).start()

    _write_connection_file(url)
    blt.info(f"Session tunnel listening on {url}, credentials in {CONNECTION_FILE}")
    blt.info(f"Idle timeout {_idle_timeout:.0f}s · operations: {', '.join(sorted(_ALL_OPS))}")
    if _device_indexes:
        blt.info(
            f"Device cache indexes: {', '.join(sorted(_device_indexes))} · "
            f"state {_cache_meta['state']} · warm={_warm_device_cache}"
        )
    blt.output.update({
        "tunnel_url": url, "port": port, "status": "running",
        "idle_timeout_s": _idle_timeout, **_stats,
    })

    # Background warm at start when indexes are configured and the persisted cache did
    # not already make us ready. The thread waits for the executor before loading, so
    # device pages are fetched on the main thread (like space_schema).
    if _warm_device_cache and _device_indexes and _cache_meta["state"] != "ready":
        threading.Thread(
            target=_warm_device_cache_async, name="device-cache-warm", daemon=True
        ).start()

    try:
        _run_executor()
    except KeyboardInterrupt:
        blt.info("Session tunnel interrupted, shutting down")
    finally:
        _stop.set()
        if _stack:
            _unwind_all("tunnel flow stopped with contexts still open")
        httpd.shutdown()
        httpd.server_close()
        try:
            os.remove(CONNECTION_FILE)
        except OSError:
            pass
        blt.output.update({"status": "stopped", **_stats})
        blt.info(
            f"Session tunnel stopped — {_stats['requests_served']} request(s), "
            f"{_stats['flow_runs_entered']} run(s) entered, "
            f"{_stats['visualizations_stored']} plot(s), "
            f"{_stats['device_writes']} device write(s), "
            f"{_stats['contexts_reclaimed']} reclaimed, {_stats['errors']} error(s)"
        )


if __name__ == "__main__":
    tunnel_session_server_flow()
