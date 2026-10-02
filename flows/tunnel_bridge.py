"""Remote bridge server: a generic reflection bridge over the app tunnel.

This is the server flow. It serves the whole ``balthazar`` Python API to a remote
client through the Balthazar app tunnel, by reflecting attribute access, calls,
item access and context-manager use onto the real module running on the Runner.

The wire format, the value tags (``$obj`` / ``$ref`` / ``$attr`` / ``$new`` / …) and
the *parts protocol* (2 MiB zlib-compressed chunks, split and stitched in both
directions) give the client a stable wire contract. Per ``docs/SPEC.md`` the server:

* ``describe`` reports protocol 3, bridge version, user/owner/shared, the tunnel
  function names, the device indexes and the idle/call timeouts;
* long calls run in a worker thread and turn into pending jobs the client polls;
* refs are scoped per caller, with an LRU cap and an explicit ``release`` op;
* a heartbeat op and a watchdog that unwinds a silent caller's entered contexts;
* a ``tunnel`` namespace (server-side device cache + ``space_schema`` digest), run
  directly from worker threads (no main-thread job queue);
* hardening: no module-root ``set``, no ``ModuleType`` ever returned or traversed,
  secrets/context hidden unless opted in, an unpatchable audit log, and tracebacks
  only for the owner.

Stdlib only, except ``blt_analytics.digest`` which is imported lazily (and only by
``space_schema``).
"""

import base64
import collections
import collections.abc
import datetime
import inspect
import itertools
import json
import os
import pathlib
import pickle
import secrets
import sys
import threading
import time
import traceback
import types
import zlib
from datetime import timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import balthazar as blt

# The audit log holds a reference to ``blt.info`` captured here, at import, so a
# caller cannot silence the audit trail by patching ``blt.info`` through the bridge.
_audit_info = blt.info

BRIDGE_VERSION = "3.0.0"
MAX_DEPTH = 12
MAX_PARTS = 512
PART_TTL = 300
JOB_TTL = 600.0                      # finished, unpolled jobs expire after 10 min
DEFAULT_PART_BYTES = 2 * 1024 * 1024

# Device-cache paging constants.
DEVICE_PAGE_SIZE = 1000
DEVICE_REFRESH_CHUNK = 500
RUN_PAGE_SIZE = 250
DEFAULT_CACHE_MAX_BYTES = 2_000_000
SCHEMA_BUILD_BUDGET_S = 3600.0
DEVICE_CACHE_BUDGET_S = 3600.0
_DEFAULT_CACHE_DIR = "~/.balthazar_tunnel_cache"
_MISSING = object()

# ----------------------------------------------------------------------------
# Configuration (flow params), applied by ``configure`` at start and in tests.
# ----------------------------------------------------------------------------
SHARED = False
ALLOWED_USERS = frozenset()
IDLE_TIMEOUT = 900.0
CALL_TIMEOUT = 45.0
MAX_REFS = 50_000
PART_BYTES = DEFAULT_PART_BYTES
EXPOSE_SECRETS = False
WARM_DEVICE_CACHE = False
DEVICE_CACHE_DIR = os.path.expanduser(_DEFAULT_CACHE_DIR)
DEVICE_INDEXES: dict = {}
HIDDEN = frozenset(
    ("serve_app", "prompt_input", "enter_new_flow_run", "context", "secrets")
)

_BASE_HIDDEN = ("serve_app", "prompt_input", "enter_new_flow_run", "context")


def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _parse_allowed_users(raw):
    if not raw:
        return frozenset()
    if isinstance(raw, (list, tuple, set)):
        parts = raw
    else:
        parts = str(raw).replace(";", ",").split(",")
    return frozenset(p.strip().lower() for p in parts if p and p.strip())


def configure(params=None):
    """Apply the flow params and reset all runtime state. Idempotent.

    ``params`` defaults to ``blt.params`` (a real Runner) but any mapping works, which
    is how the tests drive the different modes without a live space.
    """
    global SHARED, ALLOWED_USERS, IDLE_TIMEOUT, CALL_TIMEOUT, MAX_REFS, PART_BYTES
    global EXPOSE_SECRETS, WARM_DEVICE_CACHE, DEVICE_CACHE_DIR, DEVICE_INDEXES, HIDDEN
    if params is None:
        params = dict(getattr(blt, "params", {}) or {})
    SHARED = _as_bool(params.get("shared", False))
    ALLOWED_USERS = _parse_allowed_users(params.get("allowed_users", ""))
    IDLE_TIMEOUT = float(params.get("idle_timeout", 900) or 900)
    CALL_TIMEOUT = float(params.get("call_timeout", 45) or 45)
    MAX_REFS = int(params.get("max_refs", 50_000) or 50_000)
    PART_BYTES = int(params.get("part_bytes", DEFAULT_PART_BYTES) or DEFAULT_PART_BYTES)
    EXPOSE_SECRETS = _as_bool(params.get("expose_secrets", False))
    HIDDEN = frozenset(_BASE_HIDDEN if EXPOSE_SECRETS else _BASE_HIDDEN + ("secrets",))
    DEVICE_INDEXES = _parse_device_indexes(params.get("device_indexes", "{}"))
    DEVICE_CACHE_DIR = os.path.expanduser(
        params.get("device_cache_dir") or _DEFAULT_CACHE_DIR
    )
    WARM_DEVICE_CACHE = _as_bool(
        params.get("warm_device_cache", bool(DEVICE_INDEXES)), bool(DEVICE_INDEXES)
    )
    _reset_runtime_state()


def _reset_runtime_state():
    global _callers, _transfers, _device_store, _cache_meta, _schema_cache, _schema_build
    _callers = {}
    _transfers = {}
    _device_store = {"records": {}, "index": {}, "built_at": None}
    _cache_meta = {
        "state": "empty", "count": 0, "loaded": 0,
        "built_at": None, "started_at": None, "error": None,
    }
    _schema_cache = {"digest": None}
    _schema_build = {
        "state": "empty", "started_at": None,
        "loaded_flows": 0, "total_flows": None, "error": None,
    }


def owner_id():
    return str(getattr(blt, "user", "") or "").lower()


# ----------------------------------------------------------------------------
# Per-caller state: refs (LRU), entered contexts, pending jobs, last-seen.
# ----------------------------------------------------------------------------
_callers: dict = {}
_callers_lock = threading.Lock()
_ref_ids = itertools.count(1)
_local = threading.local()


class _Caller:
    __slots__ = ("lock", "refs", "entered", "jobs", "last_seen")

    def __init__(self):
        self.lock = threading.Lock()
        self.refs = collections.OrderedDict()   # ref id -> value
        self.entered = []                        # entered-context ref ids, innermost last
        self.jobs = {}                           # job id -> _Job
        self.last_seen = time.monotonic()


def caller_state(caller):
    """Return (creating if needed) the caller's state, recording the activity.

    ``last_seen`` is stamped *under* ``_callers_lock`` so the watchdog's reclaim
    decision (which re-reads it under the same lock) can never race an arriving
    request: the two are serialized.
    """
    with _callers_lock:
        state = _callers.get(caller)
        if state is None:
            state = _Caller()
            _callers[caller] = state
        state.last_seen = time.monotonic()
    return state


def remember(value):
    state = _local.state
    with state.lock:
        ref = next(_ref_ids)
        state.refs[ref] = value
        while len(state.refs) > MAX_REFS:
            state.refs.popitem(last=False)
    return ref


def recall(ref):
    state = _local.state
    with state.lock:
        if ref not in state.refs:
            raise LookupError("remote object expired, fetch it again")
        state.refs.move_to_end(ref)
        return state.refs[ref]


def release_refs(state, refs):
    with state.lock:
        for ref in refs or []:
            state.refs.pop(ref, None)
            if ref in state.entered:
                state.entered.remove(ref)


# ----------------------------------------------------------------------------
# Parts protocol (download + upload), per caller.
# ----------------------------------------------------------------------------
_transfers: dict = {}
_transfers_lock = threading.Lock()


def _forget_stale(now):
    for token in [t for t, e in _transfers.items() if e["expires"] < now]:
        del _transfers[token]


def _drop_transfers(caller):
    with _transfers_lock:
        for token in [t for t, e in _transfers.items() if e["caller"] == caller]:
            del _transfers[token]


def offer_parts(caller, body):
    packed = zlib.compress(body)
    parts = [packed[s : s + PART_BYTES] for s in range(0, len(packed), PART_BYTES)]
    token = secrets.token_hex(16)
    now = time.time()
    with _transfers_lock:
        _forget_stale(now)
        _transfers[token] = {"expires": now + PART_TTL, "caller": caller, "parts": parts}
    return {"ok": True, "parts": {"id": token, "count": len(parts)}}


def take_part(caller, token, index):
    with _transfers_lock:
        entry = _transfers.get(token)
        parts = entry.get("parts") if entry and entry["caller"] == caller else None
        if parts is None or not isinstance(index, int) or not 0 <= index < len(parts):
            return None
        if index == len(parts) - 1:
            del _transfers[token]
        return parts[index]


def add_part(caller, token, index, count, data):
    now = time.time()
    with _transfers_lock:
        _forget_stale(now)
        fresh = {"expires": now + PART_TTL, "caller": caller, "count": count, "received": {}}
        entry = _transfers.setdefault(token, fresh)
        known = entry["caller"] == caller and entry.get("count") == count
        if not known or not 0 <= index < count <= MAX_PARTS:
            raise ValueError("unexpected part of a request")
        entry["received"][index] = data
        if len(entry["received"]) < count:
            return None
        del _transfers[token]
    return zlib.decompress(b"".join(entry["received"][p] for p in range(count)))


# ----------------------------------------------------------------------------
# Encoding / decoding (ModuleType refused).
# ----------------------------------------------------------------------------
def describe(value):
    try:
        return str(value)
    except Exception:
        return "<%s>" % type(value).__name__


def snapshot_fields(value, depth):
    if callable(value) or isinstance(value, types.ModuleType):
        return {}
    cls = type(value)
    own = getattr(value, "__dict__", None)
    fields = {}
    for name in dir(value):
        if name.startswith("_"):
            continue
        if not (isinstance(own, dict) and name in own):
            try:
                if not inspect.isdatadescriptor(inspect.getattr_static(cls, name)):
                    continue
            except AttributeError:
                continue
        try:
            item = getattr(value, name)
        except Exception:
            continue
        if callable(item) or isinstance(item, types.ModuleType):
            continue
        fields[name] = encode(item, depth + 1)
    return fields


def encode_object(value, depth):
    cls = type(value)
    out = {"$": "obj", "cls": cls.__name__, "ref": remember(value)}
    if depth >= MAX_DEPTH:
        out.update(kind="object", fields={}, text=describe(value))
    elif isinstance(value, collections.abc.Mapping):
        out["kind"] = "mapping"
        out["items"] = [[encode(k, depth + 1), encode(value[k], depth + 1)] for k in list(value)]
    elif isinstance(value, collections.abc.Sequence):
        out["kind"] = "sequence"
        out["items"] = [encode(item, depth + 1) for item in list(value)]
    else:
        out["kind"] = "object"
        out["fields"] = snapshot_fields(value, depth)
        if not out["fields"] or cls.__str__ is not object.__str__:
            out["text"] = describe(value)
    return out


def encode(value, depth=0):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, types.ModuleType):
        raise AttributeError("modules are not reachable through the bridge")
    if isinstance(value, complex):
        return {"$": "complex", "v": [value.real, value.imag]}
    if isinstance(value, (bytes, bytearray)):
        return {"$": "bytes", "v": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, datetime.datetime):
        return {"$": "datetime", "v": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"$": "date", "v": value.isoformat()}
    if isinstance(value, datetime.time):
        return {"$": "time", "v": value.isoformat()}
    if isinstance(value, datetime.timedelta):
        return {"$": "timedelta", "v": [value.days, value.seconds, value.microseconds]}
    if isinstance(value, pathlib.PurePath):
        return {"$": "path", "v": str(value)}
    if type(value) is list:
        return [encode(item, depth + 1) for item in value]
    if type(value) is tuple:
        return {"$": "tuple", "v": [encode(item, depth + 1) for item in value]}
    if type(value) in (set, frozenset):
        return {"$": "set", "v": [encode(item, depth + 1) for item in value]}
    if type(value) is dict:
        if "$" not in value and all(isinstance(key, str) for key in value):
            return {key: encode(item, depth + 1) for key, item in value.items()}
        pairs = [[encode(k, depth + 1), encode(v, depth + 1)] for k, v in value.items()]
        return {"$": "dict", "v": pairs}
    return encode_object(value, depth)


def public(name):
    if not isinstance(name, str) or name.startswith("_") or name in HIDDEN:
        raise AttributeError("%r is not reachable through the bridge" % (name,))
    return name


def resolve(root, path):
    target = root
    for name in path:
        target = getattr(target, public(name))
        if isinstance(target, types.ModuleType):
            raise AttributeError("modules are not reachable through the bridge")
    return target


def decode(value):
    if isinstance(value, list):
        return [decode(item) for item in value]
    if not isinstance(value, dict):
        return value
    tag = value.get("$")
    if tag is None:
        return {key: decode(item) for key, item in value.items()}
    if tag == "ref":
        return recall(value["id"])
    if tag == "attr":
        return resolve(blt, value["path"])
    if tag == "new":
        factory = resolve(blt, value["path"])
        return factory(*decode(value.get("args", [])), **decode(value.get("kwargs", {})))
    payload = value["v"]
    if tag == "complex":
        return complex(payload[0], payload[1])
    if tag == "bytes":
        return base64.b64decode(payload)
    if tag == "datetime":
        return datetime.datetime.fromisoformat(payload)
    if tag == "date":
        return datetime.date.fromisoformat(payload)
    if tag == "time":
        return datetime.time.fromisoformat(payload)
    if tag == "timedelta":
        return datetime.timedelta(days=payload[0], seconds=payload[1], microseconds=payload[2])
    if tag == "path":
        return pathlib.Path(payload)
    if tag == "tuple":
        return tuple(decode(item) for item in payload)
    if tag == "set":
        return {decode(item) for item in payload}
    if tag == "dict":
        return {decode(key): decode(item) for key, item in payload}
    raise ValueError("unknown value tag %r" % (tag,))


def describe_module(caller):
    functions, classes = [], []
    for name in dir(blt):
        if name.startswith("_") or name in HIDDEN:
            continue
        try:
            item = getattr(blt, name)
        except Exception:
            continue
        if isinstance(item, types.ModuleType):
            continue
        if isinstance(item, type):
            classes.append(name)
        elif callable(item):
            functions.append(name)
    return {
        "functions": functions,
        "classes": classes,
        "parts": PART_BYTES,
        "protocol": 3,
        "bridge_version": BRIDGE_VERSION,
        "user": caller,
        "owner": owner_id(),
        "shared": SHARED,
        "tunnel": sorted(TUNNEL),
        "device_indexes": {name: spec["path"] for name, spec in DEVICE_INDEXES.items()},
        "idle_timeout_s": IDLE_TIMEOUT,
        "call_timeout_s": CALL_TIMEOUT,
    }


# ----------------------------------------------------------------------------
# Audit (shared mode only), through the import-time ``blt.info`` reference.
# ----------------------------------------------------------------------------
def audit(caller, state, request):
    if not SHARED:
        return
    op = request.get("op")
    if op == "tunnel":
        _audit_info("remote call by %s: tunnel %s" % (caller, request.get("name")))
        return
    parts = [str(name) for name in request.get("path", [])]
    ref = request.get("ref")
    if ref is not None:
        with state.lock:
            parts.insert(0, type(state.refs.get(ref)).__name__)
    target = ".".join(parts)
    if "key" in request:
        target += "[%s]" % json.dumps(request["key"])[:80]
    _audit_info("remote call by %s: %s %s" % (caller, op, target))


# ----------------------------------------------------------------------------
# Request handling (the op dispatch) + write-through to the device cache.
# ----------------------------------------------------------------------------
def handle(request):
    op = request.get("op")
    if op == "tunnel":
        name = request.get("name")
        fn = TUNNEL.get(name)
        if fn is None:
            raise ValueError("unknown tunnel function %r" % (name,))
        result = fn(*decode(request.get("args", [])), **decode(request.get("kwargs", {})))
        return {"ok": True, "result": encode(result)}

    ref = request.get("ref")
    root = blt if ref is None else recall(ref)
    path = request.get("path", [])
    result = None
    if op == "call":
        target = resolve(root, path)
        result = target(*decode(request.get("args", [])), **decode(request.get("kwargs", {})))
    elif op == "get":
        result = resolve(root, path)
    elif op == "set":
        if ref is None:
            raise AttributeError("assigning on the balthazar module is forbidden")
        setattr(resolve(root, path[:-1]), public(path[-1]), decode(request["value"]))
    elif op == "getitem":
        result = resolve(root, path)[decode(request["key"])]
    elif op == "contains":
        result = decode(request["key"]) in resolve(root, path)
    elif op == "setitem":
        resolve(root, path)[decode(request["key"])] = decode(request["value"])
    elif op == "delitem":
        del resolve(root, path)[decode(request["key"])]
    elif op == "enter":
        resolve(root, path).__enter__()
        if ref is not None:
            state = _local.state
            with state.lock:
                if ref not in state.entered:
                    state.entered.append(ref)
    elif op == "exit":
        failure = None
        if request.get("interrupted"):
            failure = KeyboardInterrupt()
        elif request.get("error"):
            failure = RuntimeError(request["error"])
        resolve(root, path).__exit__(type(failure) if failure else None, failure, None)
        if ref is not None:
            state = _local.state
            with state.lock:
                if ref in state.entered:
                    state.entered.remove(ref)
    else:
        raise ValueError("unknown operation %r" % (op,))

    if ref is not None and op in ("call", "set", "setitem", "delitem"):
        _write_through(root)
    response = {"ok": True, "result": encode(result)}
    if ref is not None:
        response["self"] = encode(root)
    return response


# ----------------------------------------------------------------------------
# Jobs: run every non-control request in a worker thread; pending/poll.
# ----------------------------------------------------------------------------
class _Job:
    __slots__ = ("caller", "request", "done", "response", "finished_at")

    def __init__(self, caller, request):
        self.caller = caller
        self.request = request
        self.done = threading.Event()
        self.response = None
        self.finished_at = None


def _run_job(state, job):
    _local.caller = job.caller
    _local.state = state
    try:
        job.response = handle(job.request)
    except Exception as error:
        job.response = error_response(error, job.caller)
    job.finished_at = time.time()
    job.done.set()


def submit(state, caller, request):
    """Run the request in a worker thread; reply at once if quick, else as a job."""
    job = _Job(caller, request)
    threading.Thread(target=_run_job, args=(state, job), daemon=True).start()
    if job.done.wait(CALL_TIMEOUT):
        return job.response
    job_id = secrets.token_hex(8)
    with state.lock:
        state.jobs[job_id] = job
    return {"ok": True, "pending": job_id}


def poll(state, job_id):
    with state.lock:
        job = state.jobs.get(job_id)
    if job is None:
        raise LookupError("no pending job %r for this caller" % (job_id,))
    if job.done.wait(CALL_TIMEOUT):
        with state.lock:
            state.jobs.pop(job_id, None)
        return job.response
    return {"ok": True, "pending": job_id}


def error_response(error, caller):
    info = {"type": type(error).__name__, "message": str(error)}
    if caller == owner_id():
        info["traceback"] = traceback.format_exc()
    return {"ok": False, "error": info}


def expire_jobs(now=None):
    """Drop finished, unpolled jobs older than ``JOB_TTL`` (called by the watchdog)."""
    now = time.time() if now is None else now
    for state in list(_callers.values()):
        with state.lock:
            for job_id in [
                j for j, job in state.jobs.items()
                if job.finished_at is not None and now - job.finished_at > JOB_TTL
            ]:
                del state.jobs[job_id]


# ----------------------------------------------------------------------------
# Watchdog: unwind a silent caller's entered contexts, then reclaim it.
# ----------------------------------------------------------------------------
def _has_running_job(state):
    """True while any of the caller's jobs is still executing (not yet finished)."""
    return any(not job.done.is_set() for job in state.jobs.values())


def reclaim_silent(now=None):
    """Reclaim every caller that has been silent for longer than ``IDLE_TIMEOUT``.

    The decision is made atomically under ``_callers_lock``: a caller is detached
    only while it is *still* idle, *still* the registered state and has *no* job
    running. Because an arriving request bumps ``last_seen`` under the same lock
    (and may register a job), re-checking here right before the delete closes the
    time-of-check/time-of-use gap — an active caller is never unwound.
    """
    now = time.monotonic() if now is None else now
    for caller, state in list(_callers.items()):
        with _callers_lock:
            if _callers.get(caller) is not state:
                continue
            with state.lock:
                running = _has_running_job(state)
            idle = now - state.last_seen
            if running or idle <= IDLE_TIMEOUT:
                continue
            del _callers[caller]
        _reclaim(caller, state, "bridge client went silent for %ds" % int(idle))


def _reclaim(caller, state, reason):
    """Unwind a *detached* caller's contexts and drop its refs/jobs/transfers.

    The caller has already been removed from ``_callers`` under ``_callers_lock`` by
    :func:`reclaim_silent`, so no arriving request can attach to this state.
    """
    with state.lock:
        entered = list(state.entered)
        state.entered = []
    for ref in reversed(entered):
        with state.lock:
            ctx = state.refs.get(ref)
        if ctx is None:
            continue
        try:
            ctx.__exit__(RuntimeError, RuntimeError(reason), None)
            _audit_info("remote watchdog closed a context for %s: %s" % (caller, reason))
        except Exception as exc:
            _audit_info("remote watchdog could not close a context for %s: %s" % (caller, exc))
    with state.lock:
        state.refs.clear()
        state.jobs.clear()
    _drop_transfers(caller)


def _watchdog_loop(stop):
    while not stop.wait(1.0):
        try:
            reclaim_silent()
            expire_jobs()
        except Exception as exc:
            _audit_info("remote watchdog error: %s" % (exc,))


# ----------------------------------------------------------------------------
# Serialization helpers.
# ----------------------------------------------------------------------------
def _params_to_dict(params):
    for attempt in (lambda: dict(params),
                    lambda: {k: params[k] for k in params.keys()},
                    lambda: {k: v for k, v in params.items()}):
        try:
            return attempt()
        except Exception:
            continue
    return {}


def _iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def _enum_name(value):
    if value is None:
        return None
    return str(value).rsplit(".", 1)[-1]


def _project_params(params, keys=None):
    if keys is None:
        return params
    wanted = set(keys)
    return {k: v for k, v in params.items() if k in wanted}


def _device_to_dict(device):
    fab = getattr(device, "fabrication_date", None)
    return {
        "id": getattr(device, "id", None),
        "name": getattr(device, "name", ""),
        "type": getattr(device, "type", "device"),
        "description": getattr(device, "description", None),
        "fabrication_date": _iso(fab),
        "tags": list(getattr(device, "tags", []) or []),
        "params": _params_to_dict(getattr(device, "params", {}) or {}),
    }


def _flow_to_dict(flow):
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


def _run_to_dict(run):
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
        "params": _params_to_dict(getattr(run, "params", {}) or {}),
        "output": _params_to_dict(getattr(run, "output", {}) or {}),
        "visualization_ids": list(getattr(run, "visualization_ids", []) or []),
    }


# ----------------------------------------------------------------------------
# Device cache (no main-thread queue, so blt.* is called direct).
# ----------------------------------------------------------------------------
_device_store = {"records": {}, "index": {}, "built_at": None}
_cache_meta = {
    "state": "empty", "count": 0, "loaded": 0,
    "built_at": None, "started_at": None, "error": None,
}
_device_cache_lock = threading.Lock()


def _parse_device_indexes(raw):
    if not raw:
        return {}
    spec = raw
    if isinstance(spec, str):
        try:
            spec = json.loads(spec)
        except (ValueError, TypeError) as exc:
            blt.error("[bridge] device_indexes is not valid JSON (%s); none configured" % exc)
            return {}
    if not isinstance(spec, dict):
        blt.error("[bridge] device_indexes must be a JSON object; none configured")
        return {}
    out = {}
    for name, value in spec.items():
        if isinstance(value, str) and value:
            out[str(name)] = {"path": value, "device_type": None}
        elif isinstance(value, dict) and value.get("path"):
            out[str(name)] = {"path": value["path"], "device_type": value.get("device_type")}
        else:
            blt.error("[bridge] device_indexes[%r] is malformed; skipping it" % name)
    return out


def _dig(container, dotted_path):
    current = container
    for segment in dotted_path.split("."):
        if isinstance(current, dict) and segment in current:
            current = current[segment]
        else:
            return _MISSING
    return current


def _index_value(record, device_type, path):
    if device_type is not None and record.get("type") != device_type:
        return None
    value = _dig(record.get("params") or {}, path)
    if value is _MISSING or value is None:
        return None
    return str(value)


def _build_index(records_by_id):
    index = {name: {} for name in DEVICE_INDEXES}
    for name, spec in DEVICE_INDEXES.items():
        device_type, path, bucket = spec.get("device_type"), spec["path"], index[name]
        for rid, record in records_by_id.items():
            key = _index_value(record, device_type, path)
            if key is not None:
                bucket.setdefault(key, []).append(rid)
    return index


def _iso_now():
    return datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _space_cache_key():
    space = getattr(blt, "space", None)
    if space is not None and getattr(space, "id", None):
        return str(space.id)
    return str(getattr(getattr(blt, "flow", None), "id", None) or "unknown")


def _device_cache_space_dir():
    return os.path.join(DEVICE_CACHE_DIR, _space_cache_key())


def _device_cache_path():
    return os.path.join(_device_cache_space_dir(), "devices.pkl")


def _device_cache_meta_path():
    return os.path.join(_device_cache_space_dir(), "meta.json")


def _publish_device_store(records_by_id, index, built_at):
    global _device_store
    _device_store = {"records": records_by_id, "index": index, "built_at": built_at}


def _persist_device_cache(records_by_id, index, built_at):
    space_dir = _device_cache_space_dir()
    os.makedirs(space_dir, mode=0o700, exist_ok=True)
    try:
        os.chmod(space_dir, 0o700)
    except OSError:
        pass

    def _atomic_write(path, binary, writer):
        tmp = "%s.tmp" % path
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
        "built_at": built_at, "count": len(records_by_id),
        "indexes": DEVICE_INDEXES, "space_key": _space_cache_key(),
    }
    _atomic_write(
        _device_cache_path(), True,
        lambda fh: pickle.dump({"records": records_by_id, "index": index}, fh,
                               protocol=pickle.HIGHEST_PROTOCOL),
    )
    _atomic_write(_device_cache_meta_path(), False, lambda fh: json.dump(meta, fh))


def _load_device_cache_from_disk():
    pkl_path, meta_path = _device_cache_path(), _device_cache_meta_path()
    if not (os.path.exists(pkl_path) and os.path.exists(meta_path)):
        return False
    try:
        with open(meta_path, encoding="utf-8") as fh:
            meta = json.load(fh)
        with open(pkl_path, "rb") as fh:
            payload = pickle.load(fh)
    except Exception as exc:
        blt.warn("[bridge] could not load persisted device cache: %s" % exc)
        return False
    records_by_id = payload.get("records") or {}
    index = payload.get("index") or {}
    if (meta.get("indexes") or {}) != DEVICE_INDEXES:
        index = _build_index(records_by_id)
    built_at = meta.get("built_at")
    _publish_device_store(records_by_id, index, built_at)
    _cache_meta.update(state="ready", count=len(records_by_id), loaded=len(records_by_id),
                       built_at=built_at, error=None)
    return True


def _page_all_devices(track_progress=False):
    """Fetch every device as a record, a page at a time, deduped by id."""
    by_id = {}
    offset = 0
    while True:
        page = [_device_to_dict(d) for d in blt.search_devices(limit=DEVICE_PAGE_SIZE, offset=offset)]
        if not page:
            break
        for record in page:
            by_id[record["id"]] = record
        offset += len(page)
        if track_progress:
            _cache_meta["loaded"] = len(by_id)
        if len(page) < DEVICE_PAGE_SIZE:
            break
    return by_id


def _reload_device_cache(force):
    with _device_cache_lock:
        if not force and _cache_meta["state"] == "ready":
            return
        had_data = bool(_device_store["records"])
        _cache_meta.update(state="loading", started_at=_iso_now(), loaded=0, error=None)
        try:
            records_by_id = _page_all_devices(track_progress=True)
            index = _build_index(records_by_id)
        except Exception as exc:
            _cache_meta["error"] = "%s: %s" % (type(exc).__name__, exc)
            _cache_meta["state"] = "ready" if had_data else "error"
            blt.error("[bridge] device cache load failed: %s" % _cache_meta["error"])
            raise
        built_at = _iso_now()
        _publish_device_store(records_by_id, index, built_at)
        try:
            _persist_device_cache(records_by_id, index, built_at)
        except Exception as exc:
            blt.warn("[bridge] device cache persist failed: %s: %s" % (type(exc).__name__, exc))
        _cache_meta.update(state="ready", count=len(records_by_id), loaded=len(records_by_id),
                           built_at=built_at, error=None)
        blt.info("[bridge] device cache ready: %d device(s)" % len(records_by_id))


def _reindex_ids(records_by_id, index, ids):
    id_set = set(ids)
    for buckets in index.values():
        for key in list(buckets):
            remaining = [i for i in buckets[key] if i not in id_set]
            if remaining:
                buckets[key] = remaining
            else:
                del buckets[key]
    for name, spec in DEVICE_INDEXES.items():
        device_type, path = spec.get("device_type"), spec["path"]
        for rid in ids:
            record = records_by_id.get(rid)
            if record is None:
                continue
            key = _index_value(record, device_type, path)
            if key is not None:
                index[name].setdefault(key, []).append(rid)


def _refresh_index_value(index_name, value):
    # Read the ids to refresh under the lock, then fetch them from the Runner
    # *outside* the lock so concurrent write-throughs are not blocked for the whole
    # network round-trip. Merge the fresh records into the *current* store under the
    # lock (re-reading ``_device_store``), so any write-through that landed during the
    # fetch is preserved instead of being clobbered by a stale snapshot.
    with _device_cache_lock:
        known_ids = list(_device_store["index"].get(index_name, {}).get(str(value), []))
    if not known_ids:
        return
    found = {}
    for start in range(0, len(known_ids), DEVICE_REFRESH_CHUNK):
        chunk = known_ids[start:start + DEVICE_REFRESH_CHUNK]
        for d in blt.search_devices(id=chunk):
            record = _device_to_dict(d)
            found[record["id"]] = record
    with _device_cache_lock:
        store = _device_store
        records_by_id = dict(store["records"])
        for rid in known_ids:
            if rid in found:
                records_by_id[rid] = found[rid]
            else:
                records_by_id.pop(rid, None)
        index = {name: {k: list(v) for k, v in buckets.items()}
                 for name, buckets in store["index"].items()}
        _reindex_ids(records_by_id, index, known_ids)
        _publish_device_store(records_by_id, index, store["built_at"])
        _cache_meta["count"] = len(records_by_id)
        try:
            _persist_device_cache(records_by_id, index, store["built_at"])
        except Exception as exc:
            blt.warn("[bridge] device cache persist failed: %s: %s" % (type(exc).__name__, exc))


def _write_through(root):
    """Keep the cache in step with a write whose root object is a cached device."""
    rid = getattr(root, "id", None)
    if rid is None or not hasattr(root, "params"):
        return
    try:
        record = _device_to_dict(root)
    except Exception:
        return
    # Mutate the cache under ``_device_cache_lock`` and re-read ``_device_store`` under
    # it, so a concurrent reload/refresh that swapped the store object never causes the
    # write to land on a stale, detached store (and never races the index mutation).
    with _device_cache_lock:
        if _cache_meta["state"] != "ready":
            return
        store = _device_store
        records = store["records"]
        if rid not in records:
            return
        old = records[rid]
        records[rid] = record
        for name, spec in DEVICE_INDEXES.items():
            device_type, path = spec.get("device_type"), spec["path"]
            old_key = _index_value(old, device_type, path)
            new_key = _index_value(record, device_type, path)
            if old_key == new_key:
                continue
            buckets = store["index"].setdefault(name, {})
            if old_key is not None and buckets.get(old_key):
                lst = [i for i in buckets[old_key] if i != rid]
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
    for name, spec in DEVICE_INDEXES.items():
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


def _budget_page(records, max_bytes):
    page, size = [], 0
    for record in records:
        record_size = len(json.dumps(record, default=str).encode("utf-8"))
        if page and size + record_size > max_bytes:
            break
        page.append(record)
        size += record_size
    return page


def _require_index(index_name):
    if index_name not in DEVICE_INDEXES:
        configured = ", ".join(sorted(DEVICE_INDEXES)) or "(none configured)"
        raise ValueError("unknown device index %r. Configured: %s." % (index_name, configured))


def _start_device_cache_load_async():
    if _cache_meta["state"] == "loading":
        return

    def _bg():
        try:
            _reload_device_cache(force=False)
        except Exception:
            pass

    threading.Thread(target=_bg, name="bridge-device-cache-load", daemon=True).start()


# ----------------------------------------------------------------------------
# space_schema (builds the digest in the background).
# ----------------------------------------------------------------------------
_schema_cache = {"digest": None}
_schema_build = {
    "state": "empty", "started_at": None,
    "loaded_flows": 0, "total_flows": None, "error": None,
}
_schema_lock = threading.Lock()


def _ensure_blt_analytics_on_path():
    flow_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(flow_dir)
    if repo_root not in sys.path:
        sys.path.append(repo_root)


def _import_digest():
    _ensure_blt_analytics_on_path()
    from blt_analytics import digest
    return digest


def _page_flow_runs(flow_id, max_runs):
    by_id = {}
    offset = 0
    while True:
        page = [_run_to_dict(r) for r in
                blt.search_flow_run_history(flow_id=flow_id, limit=RUN_PAGE_SIZE, offset=offset)]
        if not page:
            break
        for record in page:
            by_id[record["id"]] = record
        offset += len(page)
        if len(page) < RUN_PAGE_SIZE or (max_runs and len(by_id) >= max_runs):
            break
    records = list(by_id.values())
    hit_cap = bool(max_runs and len(records) >= max_runs)
    return (records[:max_runs] if max_runs else records), hit_cap


def _build_space_schema(refresh, max_runs_per_flow):
    with _schema_lock:
        if not refresh and _schema_cache.get("digest") is not None:
            _schema_build["state"] = "ready"
            return _schema_cache["digest"]
        _schema_build.update(state="building", started_at=_iso_now(),
                             loaded_flows=0, total_flows=None, error=None)
        try:
            digest_mod = _import_digest()
        except Exception as exc:
            _schema_build.update(state="error", error="%s: %s" % (type(exc).__name__, exc))
            raise RuntimeError("space_schema unavailable on this Runner: %s" % exc) from exc
        try:
            if _cache_meta["state"] == "ready":
                device_records = list(_device_store["records"].values())
            else:
                device_records = list(_page_all_devices().values())
            flow_records = [_flow_to_dict(f) for f in blt.search_flows()]
            _schema_build["total_flows"] = len(flow_records)
            run_records, truncated = [], set()
            for flow in flow_records:
                records, hit_cap = _page_flow_runs(flow["id"], max_runs_per_flow)
                run_records.extend(records)
                if hit_cap:
                    truncated.add(flow["id"])
                _schema_build["loaded_flows"] += 1
            digest = digest_mod.build_digest(device_records, flow_records, run_records)
            if truncated:
                digest["totals"]["runs_truncated"] = True
                key_by_id = digest_mod.flow_key(flow_records)
                for flow_id in truncated:
                    entry = digest["flows"].get(key_by_id.get(flow_id))
                    if entry is not None:
                        entry["truncated"] = True
                        entry["runs_sampled"] = entry.get("run_count", 0)
        except Exception as exc:
            _schema_build.update(state="error", error="%s: %s" % (type(exc).__name__, exc))
            raise
        _schema_cache["digest"] = digest
        _schema_build.update(state="ready", error=None)
        return digest


def _space_schema_status_dict():
    state = _schema_build["state"]
    if state != "building" and _schema_cache.get("digest") is not None:
        state = "ready"
    out = {
        "state": state,
        "progress": {
            "loaded_flows": _schema_build["loaded_flows"],
            "total_flows": _schema_build["total_flows"],
            "started_at": _schema_build["started_at"],
        },
        "error": _schema_build["error"],
    }
    if state == "ready":
        out["digest"] = _schema_cache["digest"]
    return out


def _start_schema_build_async(refresh, max_runs_per_flow):
    if _schema_build["state"] == "building":
        return
    _schema_build["state"] = "building"

    def _bg():
        try:
            _build_space_schema(refresh, max_runs_per_flow)
        except Exception:
            pass

    threading.Thread(target=_bg, name="bridge-space-schema-build", daemon=True).start()


# ----------------------------------------------------------------------------
# Tunnel functions (the ``tunnel`` op namespace).
# ----------------------------------------------------------------------------
def tn_refresh_device_cache(wait=False):
    if wait:
        try:
            _reload_device_cache(force=True)
        except Exception:
            pass
        return _device_cache_status()

    def _bg():
        try:
            _reload_device_cache(force=True)
        except Exception:
            pass

    threading.Thread(target=_bg, name="bridge-device-cache-refresh", daemon=True).start()
    return _device_cache_status()


def tn_cached_devices(index=None, value=None, *, refresh=False, offset=0,
                      max_bytes=DEFAULT_CACHE_MAX_BYTES):
    _require_index(index)
    if _cache_meta["state"] != "ready":
        _start_device_cache_load_async()
        return {"state": "loading", "devices": [], "next_offset": None, "total": 0,
                "offset": int(offset or 0), "loaded": _cache_meta["loaded"],
                "count": _cache_meta["count"], "error": _cache_meta["error"]}
    if refresh:
        _refresh_index_value(index, value)
    store = _device_store
    ids = store["index"].get(index, {}).get(str(value), [])
    recs = store["records"]
    records = [recs[i] for i in ids if i in recs]
    offset = int(offset or 0)
    page = _budget_page(records[offset:], int(max_bytes or DEFAULT_CACHE_MAX_BYTES))
    next_start = offset + len(page)
    return {"state": "ready", "devices": page,
            "next_offset": next_start if next_start < len(records) else None,
            "total": len(records), "offset": offset}


def tn_cached_devices_query(device_type=None, keys=None, offset=0,
                            max_bytes=DEFAULT_CACHE_MAX_BYTES, limit=0):
    if _cache_meta["state"] != "ready":
        _start_device_cache_load_async()
        return {"state": "loading", "devices": [], "total": 0,
                "next_offset": None, "offset": int(offset or 0)}
    store = _device_store
    records = sorted(store["records"].values(), key=lambda r: r["id"])
    if device_type is not None:
        wanted = set(device_type if isinstance(device_type, (list, tuple, set)) else [device_type])
        records = [r for r in records if r.get("type") in wanted]
    total = len(records)
    offset = int(offset or 0)
    # ``limit`` is a TOTAL cap counted from offset 0, not a per-page size: the client
    # re-sends the same limit with each next_offset page, so the window must stop at
    # ``cap`` and next_offset must be None once the cap is reached.
    cap = min(int(limit), total) if limit else total
    window = records[offset:cap]
    if keys is not None:
        window = [{**r, "params": _project_params(r.get("params") or {}, keys)} for r in window]
    page = _budget_page(window, int(max_bytes or DEFAULT_CACHE_MAX_BYTES))
    next_start = offset + len(page)
    return {"devices": page, "total": total, "offset": offset,
            "next_offset": next_start if next_start < cap else None}


def tn_space_schema(refresh=False, max_runs_per_flow=2000):
    if not refresh and _schema_cache.get("digest") is not None:
        return _space_schema_status_dict()
    _start_schema_build_async(bool(refresh), int(max_runs_per_flow))
    return _space_schema_status_dict()


TUNNEL = {
    "device_cache_status": _device_cache_status,
    "refresh_device_cache": tn_refresh_device_cache,
    "cached_devices": tn_cached_devices,
    "cached_devices_query": tn_cached_devices_query,
    "space_schema": tn_space_schema,
}


# ----------------------------------------------------------------------------
# HTTP handler.
# ----------------------------------------------------------------------------
INDEX_PAGE = b"""<!doctype html>
<meta charset="utf-8">
<title>Balthazar remote bridge</title>
<body style="font-family: system-ui, sans-serif; max-width: 820px; margin: 48px auto; padding: 0 16px">
<h2>Remote bridge is running</h2>
{note}
<p>Call the Balthazar Python API from a script on your computer while this flow run is alive:</p>
<pre id="snippet" style="background: #f4f4f5; padding: 16px; border-radius: 8px; overflow-x: auto"></pre>
<script>
document.getElementById("snippet").textContent = [
  "import balthazar_remote",
  "",
  'blt = balthazar_remote.connect("' + window.location.href + '")',
  "",
  "for device in blt.search_devices():",
  "    print(device.name, device.params)",
].join("\\n");
</script>
"""
SHARED_NOTE = (
    b"<p><b>This bridge is shared.</b> Every call runs as the user who started it "
    b"and is written to the log of this flow run with the id of the caller.</p>"
)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        return

    def reply(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def caller(self):
        return (self.headers.get("X-Blt-User-Id") or "").lower()

    def is_allowed(self):
        caller = self.caller()
        if not caller:
            return False
        if SHARED:
            return True
        return caller == owner_id() or caller in ALLOWED_USERS

    def from_browser(self):
        return bool(self.headers.get("Origin") or self.headers.get("Sec-Fetch-Site"))

    def read_body(self):
        if "chunked" not in (self.headers.get("Transfer-Encoding") or "").lower():
            return self.rfile.read(int(self.headers.get("Content-Length") or 0))
        chunks = []
        while True:
            size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
            if size == 0:
                while self.rfile.readline().strip():
                    pass
                return b"".join(chunks)
            chunks.append(self.rfile.read(size))
            self.rfile.readline()

    def do_GET(self):
        if self.path.split("?")[0] != "/":
            self.reply(404, b"Not found", "text/plain")
        elif not self.is_allowed():
            self.reply(403, b"This remote bridge belongs to another user", "text/plain")
        else:
            page = INDEX_PAGE.replace(b"{note}", SHARED_NOTE if SHARED else b"")
            self.reply(200, page, "text/html; charset=utf-8")

    def do_POST(self):
        body = self.read_body()
        if self.path.split("?")[0] != "/call":
            self.reply(404, b"Not found", "text/plain")
            return
        if not self.is_allowed():
            self.reply(403, b"This remote bridge belongs to another user", "text/plain")
            return
        if self.from_browser():
            self.reply(403, b"The remote bridge does not accept calls from a browser", "text/plain")
            return
        caller = self.caller()
        state = caller_state(caller)
        try:
            upload = self.headers.get("X-Bridge-Upload")
            if upload:
                index = int(self.headers.get("X-Bridge-Index"))
                count = int(self.headers.get("X-Bridge-Count"))
                body = add_part(caller, upload, index, count, body)
            if body is None:
                response = {"ok": True, "result": None}
            else:
                request = json.loads(body)
                op = request.get("op")
                if op == "part":
                    part = take_part(caller, request.get("id"), request.get("index"))
                    if part is None:
                        self.reply(410, b"This part is no longer kept, repeat the call", "text/plain")
                    else:
                        self.reply(200, part, "application/octet-stream")
                    return
                if op == "describe":
                    response = {"ok": True, "result": describe_module(caller)}
                elif op == "heartbeat":
                    response = {"ok": True, "result": None}
                elif op == "release":
                    release_refs(state, request.get("refs", []))
                    response = {"ok": True, "result": None}
                elif op == "poll":
                    response = poll(state, request.get("job"))
                else:
                    audit(caller, state, request)
                    response = submit(state, caller, request)
        except Exception as error:
            response = error_response(error, caller)
        answer = json.dumps(response).encode()
        if len(answer) > PART_BYTES and self.headers.get("X-Bridge-Parts"):
            answer = json.dumps(offer_parts(caller, answer)).encode()
        self.reply(200, answer)


# ----------------------------------------------------------------------------
# Startup.
# ----------------------------------------------------------------------------
def _warm_cache_async():
    def _bg():
        try:
            if not _load_device_cache_from_disk():
                _reload_device_cache(force=False)
        except Exception as exc:
            blt.warn("[bridge] device cache warm failed: %s: %s" % (type(exc).__name__, exc))

    threading.Thread(target=_bg, name="bridge-warm", daemon=True).start()


def start():
    """Configure from the flow params, start the background threads and serve."""
    configure()
    if DEVICE_INDEXES:
        try:
            if _load_device_cache_from_disk():
                blt.info("[bridge] loaded %d device(s) from %s"
                         % (_cache_meta["count"], _device_cache_path()))
            elif WARM_DEVICE_CACHE:
                _warm_cache_async()
        except Exception as exc:
            blt.warn("[bridge] device cache init failed: %s: %s" % (type(exc).__name__, exc))
    stop = threading.Event()
    threading.Thread(target=_watchdog_loop, args=(stop,), name="bridge-watchdog", daemon=True).start()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server._bridge_stop = stop
    blt.serve_app(server.server_address[1])
    return server


if __name__ == "__main__":
    server = start()
    blt.info("Remote bridge is running, open the app to get the connection snippet")
    try:
        server.serve_forever()
    finally:
        server._bridge_stop.set()
        server.server_close()
