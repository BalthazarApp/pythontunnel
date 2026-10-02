"""In-process fakes for the client tests.

Two things live here, both owned by the client test suite:

* :class:`FakeBridge` — a tiny protocol-3 bridge server (the parts protocol,
  pending/poll, release, heartbeat and the ``tunnel`` namespace) backed by a
  reflective :class:`Backend`. It exists so the client tests do not depend on
  ``flows/tunnel_bridge.py`` and can inject faults the real server would not.
* loaders — import the client ``balthazar_remote`` and the ``bridge/balthazar.py``
  drop-in by path.

This is test code, not part of the shipped client.
"""

import base64
import collections
import collections.abc
import copy
import datetime
import importlib.util
import inspect
import json
import os
import secrets
import sys
import threading
import time
import traceback
import types
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))

PART_BYTES = 2 * 1024 * 1024
MAX_DEPTH = 12
HIDDEN = frozenset(("serve_app", "prompt_input", "enter_new_flow_run", "context", "secrets"))
BRIDGE_VERSION = "3.0.0"


# ---------------------------------------------------------------------------
# Loaders (import by path so tests need no sys.path surgery of their own).
# ---------------------------------------------------------------------------
def load_balthazar_remote():
    path = os.path.join(REPO, "bridge", "balthazar_remote.py")
    spec = importlib.util.spec_from_file_location("bridge_balthazar_remote", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_dropin():
    path = os.path.join(REPO, "bridge", "balthazar.py")
    spec = importlib.util.spec_from_file_location("bridge_balthazar_dropin", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# PyO3-style enums (narrow, like the Runner's).
# ---------------------------------------------------------------------------
class _EnumMember:
    __slots__ = ("_qualname",)

    def __init__(self, qualname):
        self._qualname = qualname

    def __repr__(self):
        return self._qualname

    __str__ = __repr__

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return id(self)


def _build_enum(name, members):
    cls = type(name, (_EnumMember,), {"__slots__": ()})
    for member in members:
        setattr(cls, member, cls("%s.%s" % (name, member)))
    return cls


FlowRunStatus = _build_enum("FlowRunStatus", ("PREPARING", "RUNNING", "FINISHED", "FAILED", "KILLED"))
VisualizationDataType = _build_enum("VisualizationDataType", ("SVG", "PNG", "JPG"))


# ---------------------------------------------------------------------------
# Value types (dict/list subclasses so they encode as referenced proxies).
# ---------------------------------------------------------------------------
class ParamDict(dict):
    """A dict subclass; encodes as a ``mapping`` object (with a ref) not plain JSON."""


class TagList(list):
    """A list subclass; encodes as a ``sequence`` object (with a ref)."""


class Ident:
    def __init__(self, id_, name=None):
        self.id = id_
        self.name = name
        self.tags = TagList()

    def __repr__(self):
        return "<Ident id=%s name=%r>" % (self.id, self.name)


class Device:
    def __init__(self, record):
        self.id = record["id"]
        self.type = record.get("type", "device")
        self.name = record.get("name", "")
        self.description = record.get("description")
        fab = record.get("fabrication_date")
        self.fabrication_date = fab
        self.tags = TagList(record.get("tags") or [])
        self.params = ParamDict(record.get("params") or {})

    def __repr__(self):
        return "<Device %r type=%r id=%s>" % (self.name, self.type, self.id)


class FlowRun:
    def __init__(self, record):
        self.id = record["id"]
        self.name = record.get("name")
        self.flow_id = record.get("flow_id")
        self.flow_name = record.get("flow_name")
        self.status = getattr(FlowRunStatus, str(record.get("status") or "FINISHED"), FlowRunStatus.FINISHED)
        self.params = ParamDict(record.get("params") or {})
        self.output = ParamDict(record.get("output") or {})

    def __repr__(self):
        return "<FlowRun %s status=%s>" % (self.id, self.status)


class DeviceBuilder:
    def __init__(self, name, type="device", **params):
        self.name = name
        self.type = type
        self.params = params

    def __repr__(self):
        return "<DeviceBuilder %r>" % (self.name,)


class VisualizationBuilder:
    def __init__(self, filename, data, type=None, figure_id=None):
        self.filename = filename
        self.data = data
        self.type = type
        self.figure_id = figure_id

    def __repr__(self):
        return "<VisualizationBuilder %r figure_id=%r>" % (self.filename, self.figure_id)


# ---------------------------------------------------------------------------
# The flow-run context served by new_flow_run_context.
# ---------------------------------------------------------------------------
class FlowRunContext:
    def __init__(self, backend, name, devices, parameters):
        self._backend = backend
        self.name = name
        self._id = backend.new_id("run")
        self.flow_run = Ident(self._id, name)
        self.flow = backend.flow
        self.session = backend.session
        self.devices = TagList(devices if devices is not None else [])
        self.params = ParamDict(parameters or {})
        self.output = ParamDict()
        self.status = "RUNNING"
        self.finished = False
        self.search_calls = 0
        self.stored_visualizations = []
        self.logs = []

    # lifetime ---------------------------------------------------------------
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.status = "FAILED" if exc_value is not None else "FINISHED"
        self.finished = True
        self._backend.closed_contexts.append((self._id, self.status, str(exc_value) if exc_value else None))
        return False

    def exit(self):
        self.status = "FINISHED"
        self.finished = True
        self._backend.closed_contexts.append((self._id, "FINISHED", None))

    # logging ----------------------------------------------------------------
    def info(self, message):
        self.logs.append(("info", str(message)))
        self._backend.log("info", message, self._id)

    def warn(self, message):
        self.logs.append(("warn", str(message)))
        self._backend.log("warn", message, self._id)

    def error(self, message):
        self.logs.append(("error", str(message)))
        self._backend.log("error", message, self._id)

    def debug(self, message):
        self.logs.append(("debug", str(message)))

    def print(self, message):
        self.logs.append(("print", str(message)))

    # work -------------------------------------------------------------------
    def search_devices(self, **kw):
        self.search_calls += 1
        return self._backend.search_devices(**kw)

    def new_devices(self, builders):
        return self._backend.new_devices(builders)

    def store_visualizations(self, visualizations):
        self.stored_visualizations.extend(visualizations)
        return [Ident(self._backend.new_id("viz")) for _ in visualizations]

    def store_visualization(self, filename, data, type=None, figure_id=None):
        builder = VisualizationBuilder(filename, data, type=type, figure_id=figure_id)
        return self.store_visualizations([builder])[0]

    def upload_visualizations(self, files):
        return [Ident(self._backend.new_id("viz")) for _ in files]

    def upload_visualization(self, filename, type=None, figure_id=None):
        return Ident(self._backend.new_id("viz"))

    def new_flow_run(self, name=None, **kw):
        return self._backend.new_id("run")

    # sibling-only nesting: parents are never reported
    def parents(self):
        return []

    def parent(self):
        return None

    def __repr__(self):
        return "<FlowRunContext %r id=%s>" % (self.name, self._id)


# ---------------------------------------------------------------------------
# The reflective backend (stands in for the balthazar module).
# ---------------------------------------------------------------------------
class Backend:
    # classes reachable through the bridge
    Device = Device
    FlowRun = FlowRun
    DeviceBuilder = DeviceBuilder
    VisualizationBuilder = VisualizationBuilder
    FlowRunStatus = FlowRunStatus
    VisualizationDataType = VisualizationDataType

    def __init__(self, user="owner-user", device_records=None):
        self.user = user
        self.flow = Ident("flow-bridge", "Remote bridge")
        self.session = Ident("session-bridge")
        self.flow_run = Ident("run-bridge-root", "Remote bridge")
        self.params = ParamDict({"shared": True})
        self.devices = TagList()
        self.output = ParamDict()
        self._id_counter = 0
        self.messages = []            # (level, message, run_id)
        self.closed_contexts = []     # (run_id, status, error)
        self.contexts = []            # FlowRunContext instances, in creation order
        self.serve_app_ports = []
        self.module_visualizations = []
        records = device_records if device_records is not None else _default_device_records()
        self._device_records = [copy.deepcopy(r) for r in records]
        # device cache state (tunnel)
        self.device_index_name = "wafer"
        self.device_index_path = "wafer_id"
        self.cache_state = "empty"
        self.cache_ready_after = 1     # cached_* becomes ready on the Nth call
        self._cache_poll = 0
        self.page_size = 1000          # records per cached_* page (small -> more pages)
        self.cache_refresh_calls = []  # refresh flag seen per tn_cached_devices call
        self.schema_ready_after = 1    # space_schema becomes ready on the Nth call
        self._schema_poll = 0

    # identity / logging -----------------------------------------------------
    def new_id(self, prefix):
        self._id_counter += 1
        return "%s-%04d" % (prefix, self._id_counter)

    def log(self, level, message, run_id=None):
        self.messages.append((level, str(message), run_id))

    def info(self, message):
        self.log("info", message)

    def warn(self, message):
        self.log("warn", message)

    def error(self, message):
        self.log("error", message)

    def serve_app(self, port):
        self.serve_app_ports.append(int(port))

    # reads ------------------------------------------------------------------
    def search_devices(self, *, id=None, type=None, name=None, tags=None,
                        offset=0, limit=0, **_ignored):
        ids = _as_list(id)
        types = _as_list(type)
        names = _as_list(name)
        result = []
        for record in sorted(self._device_records, key=lambda r: r["id"]):
            if ids is not None and record["id"] not in ids:
                continue
            if types is not None and record.get("type") not in types:
                continue
            if names is not None and record.get("name") not in names:
                continue
            result.append(Device(record))
        if offset:
            result = result[offset:]
        if limit:
            result = result[:limit]
        return result

    search_objects = search_devices

    def new_devices(self, builders):
        created = []
        for builder in builders:
            rid = self.new_id("device")
            record = {"id": rid, "name": builder.name, "type": builder.type,
                      "params": dict(getattr(builder, "params", {}) or {}), "tags": []}
            self._device_records.append(record)
            created.append(Device(record))
        return created

    def new_flow_run_context(self, name=None, script_name=None, flow_id=None,
                             devices=None, parameters=None, started_time=None):
        ctx = FlowRunContext(self, name, devices, parameters)
        self.contexts.append(ctx)
        return ctx

    def new_flow_run(self, name=None, **kw):
        return self.new_id("run")

    def demo_run(self, status="FINISHED"):
        return FlowRun({"id": self.new_id("run"), "name": "demo", "status": status})

    def store_visualizations(self, visualizations):
        self.module_visualizations.extend(visualizations)
        return [Ident(self.new_id("viz")) for _ in visualizations]

    def store_visualization(self, filename, data, type=None, figure_id=None):
        return self.store_visualizations([VisualizationBuilder(filename, data, type=type, figure_id=figure_id)])[0]

    def boom(self):
        """Always raises, to exercise the remote-error mapping."""
        raise KeyError("boom key is gone")

    # test helpers that double as bridge-reachable functions -----------------
    def echo(self, value):
        return value

    def sink(self, value):
        try:
            return len(value)
        except TypeError:
            return 0

    def blob(self, nbytes):
        return b"\x00" * int(nbytes)

    def noise(self, nbytes):
        """Incompressible bytes, so a large reply really splits into several parts."""
        return os.urandom(int(nbytes))

    def text(self, nchars):
        return "x" * int(nchars)

    def slow(self, value):
        """Marked pending by the server; returns ``value`` once polled."""
        return value

    # tunnel functions -------------------------------------------------------
    def _matched_cache_records(self, value):
        wanted = str(value)
        out = []
        for record in sorted(self._device_records, key=lambda r: r["id"]):
            params = record.get("params") or {}
            if str(params.get(self.device_index_path)) == wanted:
                out.append(_cache_record(record))
        return out

    def tn_device_cache_status(self):
        return {"state": self.cache_state, "count": len(self._device_records),
                "loaded": len(self._device_records), "built_at": None,
                "indexes": [self.device_index_name], "error": None}

    def tn_refresh_device_cache(self, wait=False):
        self.cache_state = "ready"
        return self.tn_device_cache_status()

    def tn_cached_devices(self, index=None, value=None, *, refresh=False, offset=0,
                          max_bytes=PART_BYTES):
        self.cache_refresh_calls.append(bool(refresh))
        if self.cache_state != "ready":
            self._cache_poll += 1
            if self._cache_poll >= self.cache_ready_after:
                self.cache_state = "ready"
            else:
                return {"state": "loading", "devices": [], "next_offset": None,
                        "total": 0, "offset": int(offset or 0)}
        records = self._matched_cache_records(value)
        offset = int(offset or 0)
        page = records[offset:offset + self.page_size]
        nxt = offset + len(page)
        return {"state": "ready", "devices": page, "total": len(records),
                "offset": offset, "next_offset": nxt if nxt < len(records) else None}

    def tn_cached_devices_query(self, device_type=None, keys=None, offset=0,
                                max_bytes=PART_BYTES, limit=0):
        if self.cache_state != "ready":
            self._cache_poll += 1
            if self._cache_poll >= self.cache_ready_after:
                self.cache_state = "ready"
            else:
                return {"state": "loading", "devices": [], "total": 0,
                        "next_offset": None, "offset": int(offset or 0)}
        records = [_cache_record(r) for r in sorted(self._device_records, key=lambda r: r["id"])]
        if device_type is not None:
            wanted = set(_as_list(device_type) or [])
            records = [r for r in records if r.get("type") in wanted]
        total = len(records)
        offset = int(offset or 0)
        page = records[offset:offset + self.page_size]
        nxt = offset + len(page)
        return {"devices": page, "total": total, "offset": offset,
                "next_offset": nxt if nxt < total else None}

    def tn_space_schema(self, refresh=False):
        self._schema_poll += 1
        if self._schema_poll < self.schema_ready_after:
            return {"state": "building",
                    "progress": {"loaded_flows": self._schema_poll,
                                 "total_flows": self.schema_ready_after}}
        return {"state": "ready", "progress": {"loaded_flows": self.schema_ready_after,
                                               "total_flows": self.schema_ready_after},
                "digest": {"device_count": len(self._device_records)}}

    def tunnel_functions(self):
        return {
            "device_cache_status": self.tn_device_cache_status,
            "refresh_device_cache": self.tn_refresh_device_cache,
            "cached_devices": self.tn_cached_devices,
            "cached_devices_query": self.tn_cached_devices_query,
            "space_schema": self.tn_space_schema,
        }

    def device_indexes(self):
        return {self.device_index_name: self.device_index_path}


def _as_list(value):
    if value is None:
        return None
    return list(value) if isinstance(value, (list, tuple, set)) else [value]


def _cache_record(record):
    return {
        "id": record.get("id"),
        "name": record.get("name", ""),
        "type": record.get("type", "device"),
        "description": record.get("description"),
        "fabrication_date": record.get("fabrication_date"),
        "tags": list(record.get("tags") or []),
        "params": dict(record.get("params") or {}),
    }


def _default_device_records():
    records = []
    for n in range(5):
        records.append({
            "id": "dev-%03d" % n,
            "name": "sample-%03d" % n,
            "type": "sample",
            "description": "unit %d" % n,
            "fabrication_date": "2024-01-0%dT00:00:00Z" % ((n % 9) + 1),
            "tags": ["batch-a"] if n % 2 else ["batch-b"],
            "params": {"wafer_id": "W1" if n < 3 else "W2", "power_mw": float(n)},
        })
    return records


# ---------------------------------------------------------------------------
# Encode / decode (reflection), modelled on the server.
# ---------------------------------------------------------------------------
def _describe(value):
    try:
        return str(value)
    except Exception:
        return "<%s>" % type(value).__name__


def _snapshot_fields(value, remember, depth):
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
        if callable(item):
            continue
        fields[name] = _encode(item, remember, depth + 1)
    return fields


def _encode_object(value, remember, depth):
    cls = type(value)
    out = {"$": "obj", "cls": cls.__name__, "ref": remember(value)}
    if depth >= MAX_DEPTH:
        out.update(kind="object", fields={}, text=_describe(value))
    elif isinstance(value, collections.abc.Mapping):
        out["kind"] = "mapping"
        out["items"] = [[_encode(k, remember, depth + 1), _encode(value[k], remember, depth + 1)]
                        for k in list(value)]
    elif isinstance(value, collections.abc.Sequence) and not isinstance(value, (str, bytes, bytearray)):
        out["kind"] = "sequence"
        out["items"] = [_encode(item, remember, depth + 1) for item in list(value)]
    else:
        out["kind"] = "object"
        out["fields"] = _snapshot_fields(value, remember, depth)
        if not out["fields"] or cls.__str__ is not object.__str__:
            out["text"] = _describe(value)
    return out


def _encode(value, remember, depth=0):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
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
    if type(value) is list:
        return [_encode(item, remember, depth + 1) for item in value]
    if type(value) is tuple:
        return {"$": "tuple", "v": [_encode(item, remember, depth + 1) for item in value]}
    if type(value) in (set, frozenset):
        return {"$": "set", "v": [_encode(item, remember, depth + 1) for item in value]}
    if type(value) is dict:
        if "$" not in value and all(isinstance(k, str) for k in value):
            return {k: _encode(v, remember, depth + 1) for k, v in value.items()}
        return {"$": "dict", "v": [[_encode(k, remember, depth + 1), _encode(v, remember, depth + 1)]
                                   for k, v in value.items()]}
    return _encode_object(value, remember, depth)


def _public(name):
    if not isinstance(name, str) or name.startswith("_") or name in HIDDEN:
        raise AttributeError("%r is not reachable through the bridge" % (name,))
    return name


def _resolve(root, path):
    target = root
    for name in path:
        target = getattr(target, _public(name))
    return target


def _decode(value, recall, backend):
    if isinstance(value, list):
        return [_decode(item, recall, backend) for item in value]
    if not isinstance(value, dict):
        return value
    tag = value.get("$")
    if tag is None:
        return {k: _decode(v, recall, backend) for k, v in value.items()}
    if tag == "ref":
        return recall(value["id"])
    if tag == "attr":
        return _resolve(backend, value["path"])
    if tag == "new":
        factory = _resolve(backend, value["path"])
        return factory(*_decode(value.get("args", []), recall, backend),
                       **_decode(value.get("kwargs", {}), recall, backend))
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
        import pathlib
        return pathlib.Path(payload)
    if tag == "tuple":
        return tuple(_decode(item, recall, backend) for item in payload)
    if tag == "set":
        return {_decode(item, recall, backend) for item in payload}
    if tag == "dict":
        return {_decode(k, recall, backend): _decode(v, recall, backend) for k, v in payload}
    raise ValueError("unknown value tag %r" % (tag,))


# ---------------------------------------------------------------------------
# Per-caller state.
# ---------------------------------------------------------------------------
class _CallerState:
    def __init__(self):
        self.lock = threading.Lock()
        self.refs = collections.OrderedDict()
        self.ref_ids = iter(_counter())
        self.entered = []
        self.jobs = {}
        self.released = []
        self.transfers = {}


def _counter():
    n = 0
    while True:
        n += 1
        yield n


# ---------------------------------------------------------------------------
# The HTTP handler + server.
# ---------------------------------------------------------------------------
class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    @property
    def bridge(self):
        return self.server.bridge

    def reply(self, status, body, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def caller(self):
        return (self.headers.get("X-Blt-User-Id") or "").lower()

    def read_body(self):
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def do_POST(self):
        bridge = self.bridge
        body = self.read_body()
        if self.path.split("?")[0] != "/call":
            self.reply(404, b"Not found", "text/plain")
            return
        caller = self.caller()
        if not caller:
            self.reply(403, b"missing user id", "text/plain")
            return
        state = bridge.caller_state(caller)
        try:
            upload = self.headers.get("X-Bridge-Upload")
            if upload:
                index = int(self.headers.get("X-Bridge-Index"))
                count = int(self.headers.get("X-Bridge-Count"))
                body = bridge.add_part(state, upload, index, count, body)
            if body is None:
                response = {"ok": True, "result": None}
            else:
                request = json.loads(body)
                op = request.get("op")
                if op == "part":
                    part = bridge.take_part(state, request.get("id"), request.get("index"))
                    if part is None:
                        self.reply(410, b"expired part", "text/plain")
                    else:
                        self.reply(200, part, "application/octet-stream")
                    return
                if op == "describe":
                    response = {"ok": True, "result": bridge.describe(caller)}
                elif op == "heartbeat":
                    bridge.heartbeats += 1
                    response = {"ok": True, "result": None}
                elif op == "release":
                    bridge.release(state, request.get("refs", []))
                    response = {"ok": True, "result": None}
                elif op == "poll":
                    response = bridge.poll(state, request.get("job"))
                else:
                    response = bridge.submit(state, caller, request)
        except Exception as error:
            response = bridge.error_response(error, caller)
        answer = json.dumps(response).encode()
        if len(answer) > PART_BYTES and self.headers.get("X-Bridge-Parts"):
            answer = json.dumps(bridge.offer_parts(state, answer)).encode()
        self.reply(200, answer)


class FakeBridge:
    """A protocol-3 bridge server over a :class:`Backend`, on 127.0.0.1:0."""

    def __init__(self, backend=None, *, protocol=3, pending_funcs=("slow",),
                 extra_polls=0, expire_downloads=False):
        self.backend = backend if backend is not None else Backend()
        self.protocol = protocol
        self.pending_funcs = set(pending_funcs)
        self.extra_polls = extra_polls
        self.expire_downloads = expire_downloads
        self.heartbeats = 0
        self._callers = {}
        self._callers_lock = threading.Lock()
        self._tunnel = self.backend.tunnel_functions()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.server.bridge = self
        self.port = self.server.server_address[1]
        self.base_url = "http://127.0.0.1:%d/" % self.port
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    @property
    def owner(self):
        return (self.backend.user or "").lower()

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # caller state -----------------------------------------------------------
    def caller_state(self, caller):
        with self._callers_lock:
            state = self._callers.get(caller)
            if state is None:
                state = _CallerState()
                self._callers[caller] = state
            return state

    def _remember(self, state, value):
        with state.lock:
            ref = next(state.ref_ids)
            state.refs[ref] = value
            return ref

    def _recall(self, state, ref):
        with state.lock:
            if ref not in state.refs:
                raise LookupError("remote object expired, fetch it again")
            return state.refs[ref]

    def release(self, state, refs):
        with state.lock:
            for ref in refs:
                state.released.append(ref)
                state.refs.pop(ref, None)

    def released_refs(self, caller=None):
        caller = (caller or self.owner).lower()
        state = self.caller_state(caller)
        with state.lock:
            return list(state.released)

    # describe ---------------------------------------------------------------
    def describe(self, caller):
        backend = self.backend
        functions, classes = [], []
        for name in dir(backend):
            if name.startswith("_") or name in HIDDEN or name.startswith("tn_"):
                continue
            if name in ("tunnel_functions", "device_indexes", "caller_state"):
                continue
            try:
                item = getattr(backend, name)
            except Exception:
                continue
            if isinstance(item, types.ModuleType):
                continue
            if isinstance(item, type):
                classes.append(name)
            elif callable(item):
                functions.append(name)
        return {
            "functions": sorted(functions),
            "classes": sorted(classes),
            "parts": PART_BYTES,
            "protocol": self.protocol,
            "bridge_version": BRIDGE_VERSION,
            "user": caller,
            "owner": self.owner,
            "shared": True,
            "tunnel": sorted(self._tunnel),
            "device_indexes": backend.device_indexes(),
            "idle_timeout_s": 900.0,
            "call_timeout_s": 45.0,
        }

    # op handling ------------------------------------------------------------
    def handle(self, state, request):
        backend = self.backend
        remember = lambda v: self._remember(state, v)
        recall = lambda r: self._recall(state, r)
        decode = lambda v: _decode(v, recall, backend)
        op = request.get("op")
        if op == "tunnel":
            fn = self._tunnel.get(request.get("name"))
            if fn is None:
                raise ValueError("unknown tunnel function %r" % (request.get("name"),))
            result = fn(*decode(request.get("args", [])), **decode(request.get("kwargs", {})))
            return {"ok": True, "result": _encode(result, remember)}

        ref = request.get("ref")
        root = backend if ref is None else recall(ref)
        path = request.get("path", [])
        result = None
        if op == "call":
            target = _resolve(root, path)
            result = target(*decode(request.get("args", [])), **decode(request.get("kwargs", {})))
        elif op == "get":
            result = _resolve(root, path)
        elif op == "set":
            if ref is None:
                raise AttributeError("assigning on the balthazar module is forbidden")
            setattr(_resolve(root, path[:-1]), _public(path[-1]), decode(request["value"]))
        elif op == "getitem":
            result = _resolve(root, path)[decode(request["key"])]
        elif op == "contains":
            result = decode(request["key"]) in _resolve(root, path)
        elif op == "setitem":
            _resolve(root, path)[decode(request["key"])] = decode(request["value"])
        elif op == "delitem":
            del _resolve(root, path)[decode(request["key"])]
        elif op == "enter":
            _resolve(root, path).__enter__()
            if ref is not None:
                with state.lock:
                    if ref not in state.entered:
                        state.entered.append(ref)
        elif op == "exit":
            failure = None
            if request.get("interrupted"):
                failure = KeyboardInterrupt()
            elif request.get("error"):
                failure = RuntimeError(request["error"])
            _resolve(root, path).__exit__(type(failure) if failure else None, failure, None)
            if ref is not None:
                with state.lock:
                    if ref in state.entered:
                        state.entered.remove(ref)
        else:
            raise ValueError("unknown operation %r" % (op,))
        response = {"ok": True, "result": _encode(result, remember)}
        if ref is not None:
            response["self"] = _encode(root, remember)
        return response

    # pending / poll ---------------------------------------------------------
    def _is_pending(self, request):
        if request.get("op") != "call" or request.get("ref") is not None:
            return False
        path = request.get("path", [])
        return bool(path) and path[-1] in self.pending_funcs

    def submit(self, state, caller, request):
        if not self._is_pending(request):
            return self.handle(state, request)
        try:
            response = self.handle(state, request)
        except Exception as error:
            response = self.error_response(error, caller)
        job_id = secrets.token_hex(8)
        with state.lock:
            state.jobs[job_id] = {"response": response, "remaining": int(self.extra_polls)}
        return {"ok": True, "pending": job_id}

    def poll(self, state, job_id):
        with state.lock:
            job = state.jobs.get(job_id)
            if job is None:
                raise LookupError("no pending job %r for this caller" % (job_id,))
            if job["remaining"] > 0:
                job["remaining"] -= 1
                return {"ok": True, "pending": job_id}
            state.jobs.pop(job_id, None)
            return job["response"]

    def error_response(self, error, caller):
        info = {"type": type(error).__name__, "message": str(error)}
        if caller == self.owner:
            info["traceback"] = traceback.format_exc()
        return {"ok": False, "error": info}

    # parts protocol ---------------------------------------------------------
    def offer_parts(self, state, body):
        packed = zlib.compress(body)
        parts = [packed[i:i + PART_BYTES] for i in range(0, len(packed), PART_BYTES)]
        token = secrets.token_hex(16)
        if not self.expire_downloads:
            with state.lock:
                state.transfers[token] = {"parts": parts}
        return {"ok": True, "parts": {"id": token, "count": len(parts)}}

    def take_part(self, state, token, index):
        with state.lock:
            entry = state.transfers.get(token)
            parts = entry.get("parts") if entry else None
            if parts is None or not isinstance(index, int) or not 0 <= index < len(parts):
                return None
            if index == len(parts) - 1:
                del state.transfers[token]
            return parts[index]

    def add_part(self, state, token, index, count, data):
        with state.lock:
            entry = state.transfers.setdefault(token, {"count": count, "received": {}})
            if entry.get("count") != count or not 0 <= index < count:
                raise ValueError("unexpected part of a request")
            entry["received"][index] = data
            if len(entry["received"]) < count:
                return None
            del state.transfers[token]
        return zlib.decompress(b"".join(entry["received"][i] for i in range(count)))


# ---------------------------------------------------------------------------
# Convenience: a connected client against a FakeBridge.
# ---------------------------------------------------------------------------
_REMOTE_MODULE = None


def remote_module():
    global _REMOTE_MODULE
    if _REMOTE_MODULE is None:
        _REMOTE_MODULE = load_balthazar_remote()
    return _REMOTE_MODULE


def connect(bridge, user=None, *, skip_protocol_check=False, **kw):
    """Connect a client to ``bridge`` (test transport, no login)."""
    mod = remote_module()
    return mod.connect(
        _test_base_url=bridge.base_url,
        _test_user_id=user or bridge.owner,
        _skip_protocol_check=skip_protocol_check,
        **kw,
    )
