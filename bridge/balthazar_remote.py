"""Balthazar remote bridge — client side.

Call the ``balthazar`` Python API from a script on your own computer, over the
Balthazar app tunnel, against a flow run that serves ``flows/tunnel_bridge.py``.

This client speaks **protocol 3**: a protocol check, pending/poll for long calls,
a heartbeat thread, ref-release batching, flow-run-context emulation
(``enter_new_flow_run``), ``plt.show`` capture, notebook cell-source logging, an
output-primitive guard, ``isinstance`` support, the ``tunnel`` namespace with
``CachedDevice`` results, and a saved connection profile. See ``docs/SPEC.md``
§Client for the itemised contract.

Everything here is standard library only (Python 3.8+), so the file can be copied
next to a script with no install.
"""

import base64
import datetime
import hashlib
import http.client
import io
import json
import os
import pathlib
import re
import secrets
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import weakref
import webbrowser
import zlib
from http.server import BaseHTTPRequestHandler, HTTPServer

CLIENT_ID = "blt-frontend2"
BROWSER_REDIRECT = "http://localhost:8000/callback"
TOKEN_CACHE = pathlib.Path.home() / ".config" / "balthazar" / "remote.json"
BRIDGE_PROFILE = pathlib.Path.home() / ".balthazar_bridge.json"
USER_AGENT = "balthazar-remote/3.0"
FORM = {"Content-Type": "application/x-www-form-urlencoded"}
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
NONINTERACTIVE_ENV = "BALTHAZAR_TUNNEL_NONINTERACTIVE"
BRIDGE_URL_ENV = "BALTHAZAR_BRIDGE_URL"

# Tunables (module-level so tests can lower them).
POLL_NOTICE_SECONDS = 10.0       # stderr notice once a pending call waits this long
HEARTBEAT_INTERVAL_S = 30.0      # watchdog keep-alive cadence
RELEASE_BATCH = 200              # release refs once this many are pending
RELEASE_INTERVAL_S = 5.0         # ... or at least this often
TUNNEL_POLL_SECONDS = 2.0        # re-poll a loading/building tunnel call this often
TUNNEL_POLL_TIMEOUT = 1800.0     # give up on a never-ready tunnel call after this
_LOGIN_HINT = (
    "not signed in to Balthazar and running non-interactively; "
    "run `blt-tunnel connect <app url>` in a terminal first"
)
# A tunnel reply whose state is one of these is still working; the client re-polls.
_TUNNEL_LOADING = ("empty", "building", "loading", "pending")
# Only these tunnel fns are polled until ready: the schema build and the paged data
# reads. Status calls (device_cache_status, refresh_device_cache) report the cache
# state as their answer, so their reply is returned as-is via the raw path.
_POLLED_TUNNEL_FNS = frozenset({"space_schema", "cached_devices", "cached_devices_query"})

MAPPED_ERRORS = {
    error.__name__: error
    for error in (
        KeyError,
        IndexError,
        LookupError,
        ValueError,
        TypeError,
        AttributeError,
        RuntimeError,
        NotImplementedError,
    )
}

# Module names routed to the innermost open flow-run context (SPEC Client §6).
# ``output`` is handled on its own (the primitive-guarded proxy) and so is left out.
_CONTEXT_NAMES = frozenset((
    "params", "devices", "flow_run", "flow", "session",
    "info", "warn", "error", "debug", "print",
    "search_devices", "new_devices",
    "store_visualization", "store_visualizations",
    "upload_visualization", "upload_visualizations",
    "new_flow_run", "parent", "parents",
))

_GET_INDEX_DEVICES_RE = re.compile(r"^get_(?P<index>.+)_devices$")


class BridgeError(Exception):
    pass


class LoginRequired(BridgeError):
    """A token was missing or invalid and this process must not prompt for a login.

    Raised instead of starting a device-code or browser login when the client is
    non-interactive (``interactive=False`` or ``$BALTHAZAR_TUNNEL_NONINTERACTIVE=1``,
    as the MCP server and ``blt-tunnel doctor`` set). Fix it by running
    ``blt-tunnel connect`` in a terminal.
    """


class RemoteError(BridgeError):
    def __init__(self, message, remote_type=None, remote_traceback=None):
        super().__init__(message)
        self.remote_type = remote_type
        self.remote_traceback = remote_traceback


def _remote_error(info):
    name = info.get("type") or "Exception"
    base = MAPPED_ERRORS.get(name)
    cls = type(name, (RemoteError, base), {}) if base else RemoteError
    return cls(info.get("message") or name, name, info.get("traceback"))


def _failure(status, body, what):
    text = body.decode("utf-8", "replace").strip()[:300] if isinstance(body, (bytes, bytearray)) else str(body)
    return BridgeError("%s: HTTP %s %s" % (what, status, text))


def _tunnel_failure(status, body, what):
    if status == 404:
        return BridgeError("the bridge app is not running, start the bridge flow on your runner")
    if status == 410:
        return BridgeError("a part of the result expired before it was fetched, repeat the call")
    if status == 504:
        return BridgeError("the call took longer than the 60 s an app tunnel request may take")
    return _failure(status, body, what)


def _origin_key(address):
    parsed = urllib.parse.urlsplit(address or "")
    try:
        port = parsed.port
    except ValueError:
        port = None
    return parsed.scheme, (parsed.hostname or "").lower(), port or {"http": 80, "https": 443}.get(parsed.scheme)


def _site_candidates(origin):
    parsed = urllib.parse.urlsplit(origin)
    host = parsed.netloc
    names = []
    if "app-tunnel" in host:
        names.append(host.replace("app-tunnel", "core", 1))
    if "-app-tunnel" in host:
        names.append(host.replace("-app-tunnel", "", 1))
    if host.startswith("app-tunnel."):
        names.append(host[len("app-tunnel.") :])
    names.append(host)
    return ["%s://%s" % (parsed.scheme, name) for name in dict.fromkeys(names)]


def _parse_dt(value):
    """Parse an ISO-8601 string to datetime/date; pass anything else through."""
    if not isinstance(value, str) or not value:
        return value
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return datetime.datetime.fromisoformat(text)
    except ValueError:
        try:
            return datetime.date.fromisoformat(value)
        except ValueError:
            return value


def _resolve_interactive(interactive):
    """Interactive unless the caller asked otherwise or the env var forces it off."""
    if os.environ.get(NONINTERACTIVE_ENV) == "1":
        return False
    return bool(interactive)


# ---------------------------------------------------------------------------
# Value encoding (client -> server).
# ---------------------------------------------------------------------------
def _encode(value):
    if isinstance(value, (RemoteObject, RemoteDict, RemoteList)):
        return {"$": "ref", "id": value._ref}
    if isinstance(value, _Deferred):
        if value._resolved:
            return _encode(value._object)
        return {
            "$": "new",
            "path": list(value._parts),
            "args": _encode(list(value._args)),
            "kwargs": _encode(dict(value._kwargs)),
        }
    if isinstance(value, _Symbol):
        return {"$": "attr", "path": list(value._parts)}
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
    if isinstance(value, pathlib.PurePath):
        return {"$": "path", "v": str(value)}
    if isinstance(value, list):
        return [_encode(item) for item in value]
    if isinstance(value, tuple):
        return {"$": "tuple", "v": [_encode(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        return {"$": "set", "v": [_encode(item) for item in value]}
    if isinstance(value, dict):
        if "$" not in value and all(isinstance(key, str) for key in value):
            return {key: _encode(item) for key, item in value.items()}
        return {"$": "dict", "v": [[_encode(key), _encode(item)] for key, item in value.items()]}
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _encode(tolist())
    raise TypeError("cannot send %s to Balthazar" % type(value).__name__)


def _arguments(args, kwargs):
    return {"args": _encode(list(args)), "kwargs": _encode(kwargs)}


# ---------------------------------------------------------------------------
# Reflection proxies (server -> client). A weakref finalizer is registered so the
# ref can be released server-side.
# ---------------------------------------------------------------------------
class RemoteObject:
    def __init__(self, session, data):
        self.__dict__["_session"] = session
        self._refresh(data)

    def _refresh(self, data):
        decode = self._session.decode
        self.__dict__.update(
            _ref=data["ref"],
            _cls=data["cls"],
            _text=data.get("text"),
            _fields={name: decode(value) for name, value in data.get("fields", {}).items()},
        )
        self._session._on_ref(self, self.__dict__["_ref"])

    def __getattr__(self, name):
        fields = self.__dict__.get("_fields", {})
        if name in fields:
            return fields[name]
        if name.startswith("_"):
            raise AttributeError(name)
        return _Method(self, name)

    def __setattr__(self, name, value):
        payload = {"op": "set", "ref": self._ref, "path": [name], "value": _encode(value)}
        self._session.invoke(payload, target=self)

    def __dir__(self):
        return list(self._fields)

    def __getitem__(self, key):
        return self._session.invoke({"op": "getitem", "ref": self._ref, "key": _encode(key)})

    def __setitem__(self, key, value):
        payload = {"op": "setitem", "ref": self._ref, "key": _encode(key), "value": _encode(value)}
        self._session.invoke(payload, target=self)

    def __delitem__(self, key):
        self._session.invoke({"op": "delitem", "ref": self._ref, "key": _encode(key)}, target=self)

    def __contains__(self, key):
        return self._session.invoke({"op": "contains", "ref": self._ref, "key": _encode(key)})

    def __enter__(self):
        self._session.invoke({"op": "enter", "ref": self._ref}, target=self)
        return self

    def __exit__(self, kind, error, traceback):
        payload = {"op": "exit", "ref": self._ref}
        if error is not None:
            payload["error"] = "%s: %s" % (kind.__name__, error)
            payload["interrupted"] = isinstance(error, KeyboardInterrupt)
        self._session.invoke(payload, target=self)
        return False

    def __repr__(self):
        if self._text is not None:
            return self._text
        fields = ", ".join("%s=%r" % item for item in self._fields.items())
        return "%s(%s)" % (self._cls, fields)

    def __eq__(self, other):
        if isinstance(other, _Symbol):
            other = other._value()
        if isinstance(other, _Deferred):
            other = other._resolve()
        if not isinstance(other, RemoteObject):
            return NotImplemented
        return (self._cls, self._text, self._fields) == (other._cls, other._text, other._fields)

    def __hash__(self):
        return hash((self._cls, self._text, str(self._fields.get("id"))))


class _Method:
    def __init__(self, owner, name):
        self._owner = owner
        self._name = name

    def __call__(self, *args, **kwargs):
        owner = self._owner
        payload = {"op": "call", "ref": owner._ref, "path": [self._name]}
        payload.update(_arguments(args, kwargs))
        return owner._session.invoke(payload, target=owner)

    def __repr__(self):
        return "<remote method %s.%s>" % (self._owner._cls, self._name)


class RemoteDict(dict):
    def __init__(self, session, data):
        super().__init__()
        self._session = session
        self._refresh(data)

    def _refresh(self, data):
        decode = self._session.decode
        self._ref = data["ref"]
        self._cls = data["cls"]
        dict.clear(self)
        for key, value in data["items"]:
            dict.__setitem__(self, decode(key), decode(value))
        self._session._on_ref(self, self._ref)

    def _call(self, name, *args, **kwargs):
        payload = {"op": "call", "ref": self._ref, "path": [name]}
        payload.update(_arguments(args, kwargs))
        return self._session.invoke(payload, target=self)

    def __setitem__(self, key, value):
        payload = {"op": "setitem", "ref": self._ref, "key": _encode(key), "value": _encode(value)}
        self._session.invoke(payload, target=self)

    def __delitem__(self, key):
        self._session.invoke({"op": "delitem", "ref": self._ref, "key": _encode(key)}, target=self)

    def update(self, *args, **kwargs):
        self._call("update", dict(*args, **kwargs))

    def pop(self, *args):
        return self._call("pop", *args)

    def popitem(self):
        return self._call("popitem")

    def setdefault(self, *args):
        return self._call("setdefault", *args)

    def clear(self):
        self._call("clear")

    def __ior__(self, other):
        self.update(other)
        return self


class RemoteList(list):
    def __init__(self, session, data):
        super().__init__()
        self._session = session
        self._refresh(data)

    def _refresh(self, data):
        decode = self._session.decode
        self._ref = data["ref"]
        self._cls = data["cls"]
        list.clear(self)
        list.extend(self, [decode(item) for item in data["items"]])
        self._session._on_ref(self, self._ref)

    def _call(self, name, *args, **kwargs):
        payload = {"op": "call", "ref": self._ref, "path": [name]}
        payload.update(_arguments(args, kwargs))
        return self._session.invoke(payload, target=self)

    def __setitem__(self, index, value):
        payload = {"op": "setitem", "ref": self._ref, "key": _encode(index), "value": _encode(value)}
        self._session.invoke(payload, target=self)

    def __delitem__(self, index):
        self._session.invoke({"op": "delitem", "ref": self._ref, "key": _encode(index)}, target=self)

    def append(self, value):
        self._call("append", value)

    def extend(self, values):
        self._call("extend", list(values))

    def insert(self, index, value):
        self._call("insert", index, value)

    def remove(self, value):
        self._call("remove", value)

    def pop(self, *args):
        return self._call("pop", *args)

    def clear(self):
        self._call("clear")

    def reverse(self):
        self._call("reverse")

    def __iadd__(self, values):
        self.extend(values)
        return self


class _Function:
    def __init__(self, session, name):
        self._session = session
        self._name = name

    def __call__(self, *args, **kwargs):
        payload = {"op": "call", "path": [self._name]}
        payload.update(_arguments(args, kwargs))
        return self._session.invoke(payload)

    def __repr__(self):
        return "<remote function balthazar.%s>" % self._name


class _Symbol:
    def __init__(self, session, parts):
        self._session = session
        self._parts = parts

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return _Symbol(self._session, self._parts + (name,))

    def __call__(self, *args, **kwargs):
        return _Deferred(self._session, self._parts, args, kwargs)

    def _value(self):
        constants = self._session.constants
        if self._parts not in constants:
            constants[self._parts] = self._session.invoke({"op": "get", "path": list(self._parts)})
        return constants[self._parts]

    def __eq__(self, other):
        if isinstance(other, _Symbol):
            other = other._value()
        return self._value() == other

    def __hash__(self):
        return hash(self._parts)

    def __repr__(self):
        return "balthazar.%s" % ".".join(self._parts)


class _Deferred:
    def __init__(self, session, parts, args, kwargs):
        self.__dict__.update(
            _session=session,
            _parts=parts,
            _args=args,
            _kwargs=kwargs,
            _resolved=False,
            _object=None,
        )

    def _resolve(self):
        if not self._resolved:
            payload = {"op": "call", "path": list(self._parts)}
            payload.update(_arguments(self._args, self._kwargs))
            self.__dict__.update(_object=self._session.invoke(payload), _resolved=True)
        return self._object

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._resolve(), name)

    def __setattr__(self, name, value):
        setattr(self._resolve(), name, value)

    def __repr__(self):
        if self._resolved:
            return repr(self._object)
        return "balthazar.%s(...)" % ".".join(self._parts)


# ---------------------------------------------------------------------------
# Class symbols as real classes, so ``isinstance`` works (SPEC Client §10).
# ---------------------------------------------------------------------------
def _remote_cls_name(instance):
    return getattr(instance, "_cls", None)


class _RemoteClassMeta(type):
    """Metaclass for ``blt.Device`` & friends.

    ``isinstance(obj, blt.Device)`` is true when the proxy's ``_cls`` matches the
    class name. Calling the class (``blt.DeviceBuilder(...)``) still yields a
    ``_Deferred`` built on the runner, and attribute access
    (``blt.FlowRunStatus.FINISHED``) yields a constant ``_Symbol``.
    """

    def __instancecheck__(cls, instance):
        return _remote_cls_name(instance) == cls._blt_name

    def __call__(cls, *args, **kwargs):
        return _Deferred(cls._blt_session, (cls._blt_name,), args, kwargs)

    def __getattr__(cls, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return _Symbol(cls._blt_session, (cls._blt_name, name))

    def __repr__(cls):
        return "<balthazar class %s>" % cls._blt_name


def _make_class_symbol(session, name):
    return _RemoteClassMeta(name, (object,), {"_blt_name": name, "_blt_session": session})


# ---------------------------------------------------------------------------
# blt.output: the primitive-guarded proxy (SPEC Client §9).
# ---------------------------------------------------------------------------
def _is_output_primitive(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return True
    if isinstance(value, (list, tuple)):
        return all(item is None or isinstance(item, (str, int, float, bool)) for item in value)
    return False


def _validate_output(values):
    for key, value in values.items():
        if not _is_output_primitive(value):
            raise TypeError(
                "blt.output[%r]: %s is not a flow-run output primitive "
                "(use str / int / float / bool / None or a flat list of those)"
                % (key, type(value).__name__)
            )


class _Output:
    """``blt.output`` — writes to the innermost open context, else the bridge run.

    Primitives only, as on the real platform: a dict, date or DataFrame raises
    ``TypeError`` here instead of being mangled in transport; flat lists of
    primitives are allowed.
    """

    def __init__(self, remote):
        self._remote = remote

    def _ref(self):
        ctx = self._remote._current_context()
        return ctx._ref if ctx is not None else None

    def _dict(self):
        payload = {"op": "get", "path": ["output"]}
        ref = self._ref()
        if ref is not None:
            payload["ref"] = ref
        return self._remote._session.invoke(payload)

    def update(self, values):
        values = dict(values)
        _validate_output(values)
        if not values:
            return
        payload = {"op": "call", "path": ["output", "update"], "args": _encode([values]), "kwargs": {}}
        ref = self._ref()
        if ref is not None:
            payload["ref"] = ref
        self._remote._session.invoke(payload)

    def setdefault(self, key, default=None):
        current = self._dict()
        if key not in current:
            self.update({key: default})
            return default
        return current[key]

    def __setitem__(self, key, value):
        self.update({key: value})

    def __getitem__(self, key):
        return self._dict()[key]

    def get(self, key, default=None):
        return self._dict().get(key, default)

    def keys(self):
        return self._dict().keys()

    def values(self):
        return self._dict().values()

    def items(self):
        return self._dict().items()

    def __iter__(self):
        return iter(self._dict())

    def __len__(self):
        return len(self._dict())

    def __contains__(self, key):
        return key in self._dict()

    def __repr__(self):
        return repr(self._dict())


# ---------------------------------------------------------------------------
# tunnel namespace + CachedDevice (SPEC Client §11).
# ---------------------------------------------------------------------------
class CachedDevice:
    """A read-only device record served from the bridge's device cache.

    Call ``.live()`` to fetch the writable remote ``Device`` (via
    ``search_devices(id=[…])``) when you need to change it. These objects do not
    exist on a real Runner.
    """

    _FIELDS = ("id", "name", "type", "description", "fabrication_date", "tags", "params")

    def __init__(self, remote, record):
        fab = record.get("fabrication_date")
        self.__dict__["_remote"] = remote
        self.__dict__["_data"] = {
            "id": record.get("id"),
            "name": record.get("name"),
            "type": record.get("type"),
            "description": record.get("description"),
            "fabrication_date": _parse_dt(fab) if fab else None,
            "tags": list(record.get("tags") or []),
            "params": dict(record.get("params") or {}),
        }

    def __getattr__(self, name):
        try:
            return self.__dict__["_data"][name]
        except KeyError:
            raise AttributeError(name) from None

    def __setattr__(self, name, value):
        raise AttributeError("CachedDevice is read-only; use .live() for a writable Device")

    def live(self):
        found = self._remote.search_devices(id=[self.id])
        if not found:
            raise LookupError("device %s is no longer present" % (self.id,))
        return found[0]

    def __repr__(self):
        return "<CachedDevice %r type=%r id=%s>" % (self.name, self.type, self.id)


class _TunnelFunction:
    def __init__(self, tunnel, name):
        self._tunnel = tunnel
        self._name = name

    def __call__(self, *args, **kwargs):
        return self._tunnel._dispatch(self._name, args, kwargs)

    def __repr__(self):
        return "<tunnel function %s>" % self._name


class _Tunnel:
    """``blt.tunnel`` — the server's tunnel-only functions.

    ``cached_devices`` / ``cached_devices_query`` are paged transparently and
    returned as plain lists; ``cached_devices`` yields ``CachedDevice`` objects.
    A reply that is still ``loading`` / ``building`` is re-polled until ready.
    """

    def __init__(self, remote):
        self._remote = remote
        self._names = frozenset(remote._tunnel_names)

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return _TunnelFunction(self, name)

    def __dir__(self):
        return sorted(self._names)

    def _raw(self, name, args, kwargs):
        payload = {
            "op": "tunnel",
            "name": name,
            "args": _encode(list(args)),
            "kwargs": _encode(dict(kwargs)),
        }
        return self._remote._session.invoke(payload)

    def _await_ready(self, name, args, kwargs):
        result = self._raw(name, args, kwargs)
        deadline = time.time() + TUNNEL_POLL_TIMEOUT
        noticed = False
        while isinstance(result, dict) and result.get("state") in _TUNNEL_LOADING:
            if not noticed:
                print("tunnel: %s %s …" % (name, result.get("state")), file=sys.stderr)
                noticed = True
            if time.time() > deadline:
                raise BridgeError("tunnel call %s did not become ready in time" % (name,))
            time.sleep(TUNNEL_POLL_SECONDS)
            result = self._raw(name, args, kwargs)
        return result

    def _paged_records(self, name, kwargs):
        first = self._await_ready(name, (), kwargs)
        if not isinstance(first, dict):
            return first
        records = list(first.get("devices") or [])
        nxt = first.get("next_offset")
        # ``refresh`` rebuilds the cache; only the first page should request it, so
        # later pages read the freshly built cache rather than rebuilding each time.
        rest = {k: v for k, v in kwargs.items() if k != "refresh"}
        while nxt is not None:
            page = self._await_ready(name, (), {**rest, "offset": nxt})
            if not isinstance(page, dict):
                break
            records.extend(page.get("devices") or [])
            nxt = page.get("next_offset")
        return records

    def _dispatch(self, name, args, kwargs):
        if name == "cached_devices":
            kwargs = _bind_cached_devices(args, kwargs)
            records = self._paged_records(name, kwargs)
            return [CachedDevice(self._remote, r) for r in records]
        if name == "cached_devices_query":
            return self._paged_records(name, dict(kwargs))
        if name in _POLLED_TUNNEL_FNS:
            return self._await_ready(name, args, kwargs)
        # Status calls (device_cache_status, refresh_device_cache) and anything else:
        # the reply's state is the answer, so return it as-is without polling.
        return self._raw(name, args, kwargs)


def _bind_cached_devices(args, kwargs):
    """Map ``cached_devices(index, value, ...)`` positionals onto keywords."""
    kwargs = dict(kwargs)
    if args:
        kwargs.setdefault("index", args[0])
    if len(args) > 1:
        kwargs.setdefault("value", args[1])
    return kwargs


# ---------------------------------------------------------------------------
# Client-side flow-run context (SPEC Client §6).
# ---------------------------------------------------------------------------
class _ClientContext:
    """Wraps a remote ``FlowRunContext`` and mirrors the Runner's exit semantics.

    **Known Runner limitation:** ``new_flow_run_context`` can only create children
    of the bridge's own run, so nested ``enter_new_flow_run`` blocks become siblings
    (not grandchildren) and ``parent()`` is always the bridge run. This is not faked.
    """

    def __init__(self, remote, ctx_obj, name):
        self._remote = remote
        self._ctx = ctx_obj
        self.name = name
        self.uploaded = set()
        self.finished = False

    @property
    def flow_run_id(self):
        try:
            return self._ctx.flow_run.id
        except Exception:
            return None

    def __enter__(self):
        if self.finished:
            raise ValueError("Attempt to reenter a closed flow run context")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._remote._exit_context(self, exc_value)
        return False  # re-raise, exactly as the Runner's __exit__ does

    def exit(self):
        self._remote._exit_context(self, None)

    def __repr__(self):
        return "<flow run context %r>" % (self.name,)


# ---------------------------------------------------------------------------
# Ref-release batching (SPEC Client §3/§4).
# ---------------------------------------------------------------------------
class _Releaser:
    def __init__(self, session):
        self._session = session
        self._pending = []
        self._lock = threading.Lock()
        self._event = threading.Event()
        self._stopped = False
        self._thread = threading.Thread(target=self._run, name="bridge-release", daemon=True)
        self._thread.start()

    def add(self, ref):
        with self._lock:
            self._pending.append(ref)
            full = len(self._pending) >= RELEASE_BATCH
        if full:
            self._event.set()

    def _run(self):
        while not self._stopped:
            self._event.wait(RELEASE_INTERVAL_S)
            self._event.clear()
            self.flush()

    def flush(self):
        while True:
            with self._lock:
                if not self._pending:
                    return
                batch = self._pending[:RELEASE_BATCH]
                self._pending = self._pending[RELEASE_BATCH:]
            try:
                self._session.invoke({"op": "release", "refs": list(batch)})
            except Exception:
                pass

    def stop(self):
        self._stopped = True
        self._event.set()
        try:
            self.flush()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Transport base: parts protocol, pending/poll, heartbeat, release, decode.
# ---------------------------------------------------------------------------
class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _Http:
    def __init__(self, ca_file=None):
        context = ssl.create_default_context(cafile=ca_file)
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(context=context), _NoRedirect()
        )

    def request(self, method, url, headers=None, data=None, timeout=90):
        merged = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        merged.update(headers or {})
        request = urllib.request.Request(url, data=data, method=method, headers=merged)
        try:
            with self._opener.open(request, timeout=timeout) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.headers, error.read()
        except (OSError, http.client.HTTPException) as error:
            reason = getattr(error, "reason", None) or error
            raise BridgeError("cannot reach %s: %s" % (url, reason)) from None


class _Protocol:
    location = "a bridge"
    part_bytes = None

    def __init__(self):
        self.constants = {}
        self.part_bytes = None
        self._hb_thread = None
        self._hb_stop = None
        self._releaser = _Releaser(self)
        self._closed = False

    # -- transport (subclasses implement exchange) --------------------------
    def exchange(self, body, headers):
        raise NotImplementedError

    def _send(self, body):
        size = self.part_bytes
        if not size or len(body) <= size:
            return self.exchange(body, {"X-Bridge-Parts": "1"})
        packed = zlib.compress(body)
        token = secrets.token_hex(16)
        count = -(-len(packed) // size)
        status, answer = 0, b""
        for index in range(count):
            headers = {
                "X-Bridge-Parts": "1",
                "X-Bridge-Upload": token,
                "X-Bridge-Index": str(index),
                "X-Bridge-Count": str(count),
            }
            status, answer = self.exchange(packed[index * size : (index + 1) * size], headers)
            if status != 200 or b'"ok": true' not in answer[:64]:
                break
        return status, answer

    def _receive(self, parts):
        chunks = []
        for index in range(parts["count"]):
            request = json.dumps({"op": "part", "id": parts["id"], "index": index}).encode()
            status, chunk = self.exchange(request, {"X-Bridge-Parts": "1"})
            if status != 200:
                raise _tunnel_failure(status, chunk, "the bridge call failed")
            chunks.append(chunk)
        return zlib.decompress(b"".join(chunks))

    def _exchange_reply(self, body):
        status, raw = self._send(body)
        if status != 200:
            raise _tunnel_failure(status, raw, "the bridge call failed")
        try:
            reply = json.loads(raw)
        except ValueError:
            raise _failure(status, raw, "the bridge returned something unexpected") from None
        if isinstance(reply, dict) and reply.get("parts"):
            reply = json.loads(self._receive(reply["parts"]))
        return reply

    def _poll_until_done(self, reply):
        if not (isinstance(reply, dict) and reply.get("pending")):
            return reply
        job = reply["pending"]
        started = time.time()
        noticed = False
        while True:
            reply = self._exchange_reply(json.dumps({"op": "poll", "job": job}).encode())
            if not (isinstance(reply, dict) and reply.get("pending")):
                return reply
            if not noticed and time.time() - started > POLL_NOTICE_SECONDS:
                print("tunnel: still waiting for a long call to finish …", file=sys.stderr)
                noticed = True

    def invoke(self, payload, target=None):
        reply = self._exchange_reply(json.dumps(payload).encode())
        reply = self._poll_until_done(reply)
        if not reply.get("ok"):
            raise _remote_error(reply.get("error") or {})
        if target is not None and "self" in reply:
            target._refresh(reply["self"])
        return self.decode(reply.get("result"))

    # -- heartbeat & release ------------------------------------------------
    def _start_heartbeat(self):
        if self._hb_thread is not None:
            return
        self._hb_stop = threading.Event()
        stop = self._hb_stop

        def beat():
            while not stop.wait(HEARTBEAT_INTERVAL_S):
                try:
                    self.invoke({"op": "heartbeat"})
                except Exception:
                    pass

        self._hb_thread = threading.Thread(target=beat, name="bridge-heartbeat", daemon=True)
        self._hb_thread.start()

    def _on_ref(self, obj, ref):
        if ref is None:
            return
        try:
            weakref.finalize(obj, self._releaser.add, ref)
        except TypeError:
            pass

    def close(self):
        self._closed = True
        if self._hb_stop is not None:
            self._hb_stop.set()
        if self._releaser is not None:
            self._releaser.stop()

    # -- decode (server -> client) -----------------------------------------
    def decode(self, value):
        if isinstance(value, list):
            return [self.decode(item) for item in value]
        if not isinstance(value, dict):
            return value
        tag = value.get("$")
        if tag is None:
            return {key: self.decode(item) for key, item in value.items()}
        if tag == "obj":
            kind = value["kind"]
            if kind == "mapping":
                return RemoteDict(self, value)
            if kind == "sequence":
                return RemoteList(self, value)
            return RemoteObject(self, value)
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
            return tuple(self.decode(item) for item in payload)
        if tag == "set":
            return {self.decode(item) for item in payload}
        if tag == "dict":
            return {self.decode(key): self.decode(item) for key, item in payload}
        raise BridgeError("the bridge returned an unknown value tag %r" % (tag,))


class _TestSession(_Protocol):
    """Login-free transport for tests: POST straight to ``base_url`` + ``call``.

    Carries ``X-BLT-User-Id`` and no auth cookie. A fresh HTTP connection per
    request keeps the heartbeat/release threads from colliding on the socket. Used
    only via ``connect(_test_base_url=..., _test_user_id=...)``.
    """

    def __init__(self, base_url, user_id):
        super().__init__()
        parsed = urllib.parse.urlsplit(base_url)
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._path = (parsed.path.rstrip("/") + "/call") or "/call"
        self._user = user_id
        self.location = base_url

    def exchange(self, body, headers):
        conn = http.client.HTTPConnection(self._host, self._port, timeout=90)
        sent = {"Content-Type": "application/json"}
        if self._user is not None:
            sent["X-BLT-User-Id"] = self._user
        sent.update(headers)
        try:
            conn.request("POST", self._path, body=body, headers=sent)
            response = conn.getresponse()
            return response.status, response.read()
        except (OSError, http.client.HTTPException) as error:
            raise BridgeError("cannot reach %s: %s" % (self.location, error)) from None
        finally:
            conn.close()


class _Session(_Protocol):
    def __init__(self, app_url, site, login, username, password, ca_file, remember, client_id, interactive):
        super().__init__()
        if login not in ("device", "password", "browser"):
            raise BridgeError("login must be 'device', 'password' or 'browser'")
        if login == "password" and not (username and password):
            raise BridgeError("login='password' needs username and password")
        parsed = urllib.parse.urlsplit(app_url)
        match = re.match(r"^/app-tunnel/([0-9a-fA-F-]{36})/([0-9a-fA-F-]{36})(/|$)", parsed.path)
        if not match:
            raise BridgeError("app_url must look like https://<host>/app-tunnel/<runner id>/<flow id>/?space_id=...")
        query = urllib.parse.parse_qs(parsed.query)
        self._space_id = (query.get("space_id") or query.get("spaceId") or [None])[0]
        self._context_id = (query.get("context_id") or query.get("contextId") or [None])[0]
        if not self._space_id:
            raise BridgeError("app_url must carry ?space_id=..., copy it from the address bar of the opened app")
        self._ids = "%s/%s" % (match.group(1), match.group(2))
        self._origin = "%s://%s" % (parsed.scheme, parsed.netloc)
        self._tunnel = "%s/app-tunnel/%s/" % (self._origin, self._ids)
        self.location = self._tunnel
        self._login_mode = login
        self._username = username
        self._password = password
        self._client_id = client_id
        self._interactive = interactive
        self._remember = remember and login != "password"
        self._http = _Http(ca_file)
        self._lock = threading.RLock()
        self._access_token = None
        self._access_exp = 0.0
        self._access_lifetime = 300.0
        self._refresh_token = None
        self._cookie = None
        self._cookie_exp = 0.0
        self._site, self._authority = self._find_site(site)
        self._cache_key = "%s|%s" % (self._authority, client_id)
        if self._remember:
            self._refresh_token = self._read_cache().get(self._cache_key)

    def _find_site(self, site):
        if site:
            site = site.rstrip("/")
            status, _, body = self._http.request("GET", site + "/api/frontend/config")
            if status != 200:
                raise _failure(status, body, "%s does not answer like Balthazar" % site)
            return site, json.loads(body)["oidcAuthority"].rstrip("/")
        tried = []
        for candidate in _site_candidates(self._origin):
            try:
                status, _, body = self._http.request("GET", candidate + "/api/frontend/config", timeout=15)
                config = json.loads(body) if status == 200 else None
            except BridgeError as error:
                tried.append(str(error))
                continue
            except ValueError:
                config = None
            if not isinstance(config, dict) or not config.get("oidcAuthority"):
                tried.append("%s does not answer like Balthazar" % candidate)
            elif _origin_key(config.get("appTunnelOrigin")) != _origin_key(self._origin):
                tried.append("%s serves another app tunnel" % candidate)
            else:
                return candidate, config["oidcAuthority"].rstrip("/")
        raise BridgeError(
            'cannot work out the Balthazar address for %s, pass site="https://..." (%s)'
            % (self._origin, "; ".join(tried))
        )

    def _read_cache(self):
        try:
            cached = json.loads(TOKEN_CACHE.read_text())
        except (OSError, ValueError):
            return {}
        return cached if isinstance(cached, dict) else {}

    def _write_cache(self):
        cached = self._read_cache()
        if cached.get(self._cache_key) == self._refresh_token:
            return
        if self._refresh_token:
            cached[self._cache_key] = self._refresh_token
        else:
            cached.pop(self._cache_key, None)
        TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(str(TOKEN_CACHE), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(descriptor, "w") as file:
            json.dump(cached, file)

    def _token_request(self, params):
        form = {"client_id": self._client_id}
        form.update(params)
        status, _, body = self._http.request(
            "POST",
            self._authority + "/protocol/openid-connect/token",
            headers=FORM,
            data=urllib.parse.urlencode(form).encode(),
        )
        try:
            payload = json.loads(body)
        except ValueError:
            payload = {"error": "HTTP %s" % status}
        return status, payload

    def _store_tokens(self, payload):
        self._access_token = payload["access_token"]
        self._access_lifetime = float(payload.get("expires_in") or 300)
        self._access_exp = time.time() + self._access_lifetime
        self._refresh_token = payload.get("refresh_token")
        if self._remember:
            self._write_cache()

    def _access(self, min_left):
        if self._access_token and self._access_exp - time.time() > min_left:
            return self._access_token
        if self._refresh_token:
            status, payload = self._token_request(
                {"grant_type": "refresh_token", "refresh_token": self._refresh_token}
            )
            if status == 200:
                self._store_tokens(payload)
                return self._access_token
        if self._login_mode == "password":
            status, payload = self._token_request(
                {
                    "grant_type": "password",
                    "username": self._username,
                    "password": self._password,
                    "scope": "openid",
                }
            )
        elif self._login_mode == "device":
            if not self._interactive:
                raise LoginRequired(_LOGIN_HINT)
            status, payload = self._device_login()
        else:
            if not self._interactive:
                raise LoginRequired(_LOGIN_HINT)
            status, payload = self._browser_login()
        if status != 200:
            raise BridgeError("login failed: %s" % (payload.get("error_description") or payload.get("error")))
        self._store_tokens(payload)
        return self._access_token

    def _device_login(self):
        status, _, body = self._http.request(
            "POST",
            self._authority + "/protocol/openid-connect/auth/device",
            headers=FORM,
            data=urllib.parse.urlencode({"client_id": self._client_id, "scope": "openid"}).encode(),
        )
        if status != 200:
            raise _failure(status, body, "this Balthazar does not allow the device login")
        device = json.loads(body)
        address = device.get("verification_uri_complete") or device["verification_uri"]
        print("To sign in, open %s and confirm the code %s" % (address, device["user_code"]), file=sys.stderr)
        interval = device.get("interval") or 5
        deadline = time.time() + (device.get("expires_in") or 600)
        while time.time() < deadline:
            time.sleep(interval)
            status, payload = self._token_request(
                {"grant_type": DEVICE_GRANT, "device_code": device["device_code"]}
            )
            error = payload.get("error")
            if error == "slow_down":
                interval += 5
            elif error != "authorization_pending":
                return status, payload
        return 408, {"error": "the code was not confirmed in time"}

    def _browser_login(self):
        verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(verifier.encode()).digest()
        state = secrets.token_urlsafe(16)
        received = {}

        class Callback(BaseHTTPRequestHandler):
            def do_GET(self):
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                if query.get("state") == [state]:
                    received.update({key: values[0] for key, values in query.items()})
                body = b"You can close this tab and return to the script."
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return

        redirect = urllib.parse.urlsplit(BROWSER_REDIRECT)
        try:
            server = HTTPServer((redirect.hostname, redirect.port), Callback)
        except OSError as error:
            raise BridgeError("the browser login needs port %s on this computer: %s" % (redirect.port, error)) from None
        address = "%s/protocol/openid-connect/auth?%s" % (
            self._authority,
            urllib.parse.urlencode(
                {
                    "client_id": self._client_id,
                    "response_type": "code",
                    "redirect_uri": BROWSER_REDIRECT,
                    "scope": "openid",
                    "state": state,
                    "code_challenge": base64.urlsafe_b64encode(digest).decode().rstrip("="),
                    "code_challenge_method": "S256",
                }
            ),
        )
        print("To sign in, open %s" % address, file=sys.stderr)
        webbrowser.open(address)
        server.timeout = 1
        deadline = time.time() + 300
        try:
            while not received and time.time() < deadline:
                server.handle_request()
        finally:
            server.server_close()
        if "code" not in received:
            return 408, {"error": received.get("error_description") or received.get("error") or "no answer from the browser"}
        return self._token_request(
            {
                "grant_type": "authorization_code",
                "code": received["code"],
                "redirect_uri": BROWSER_REDIRECT,
                "code_verifier": verifier,
            }
        )

    def _grant(self):
        status, body = 0, b""
        for attempt in range(2):
            token = self._access(min(120.0, self._access_lifetime / 2))
            headers = {"Authorization": "Bearer %s" % token, "X-BLT-Space-Id": self._space_id}
            if self._context_id:
                headers["X-BLT-Context-Id"] = self._context_id
            status, reply_headers, body = self._http.request(
                "POST",
                "%s/api/app-tunnel/grant-access/%s" % (self._site, self._ids),
                headers=headers,
                data=b"",
            )
            if status not in (400, 401) or attempt:
                break
            self._access_token = None
        if status != 200:
            raise _tunnel_failure(status, body, "Balthazar refused access to the bridge app")
        for cookie in reply_headers.get_all("Set-Cookie") or []:
            pair = cookie.split(";", 1)[0].strip()
            if pair.startswith("blt_tunnel_"):
                self._cookie = pair
                self._cookie_exp = self._access_exp
                return
        raise BridgeError("Balthazar did not return an app tunnel cookie")

    def _tunnel_cookie(self, force):
        with self._lock:
            if force or not self._cookie or time.time() > self._cookie_exp - 5:
                self._grant()
            return self._cookie

    def exchange(self, body, headers):
        status, reply = 0, b""
        for attempt in range(2):
            sent = {"Cookie": self._tunnel_cookie(attempt == 1), "Content-Type": "application/json"}
            sent.update(headers)
            status, _, reply = self._http.request("POST", self._tunnel + "call", headers=sent, data=body)
            if status != 401:
                break
        return status, reply


# ---------------------------------------------------------------------------
# The module proxy.
# ---------------------------------------------------------------------------
class Remote:
    def __init__(self, session, description):
        session.part_bytes = description.get("parts")
        self.__dict__.update(
            _session=session,
            _description=description,
            _functions=frozenset(description.get("functions", ())),
            _classes=frozenset(description.get("classes", ())),
            _tunnel_names=frozenset(description.get("tunnel") or ()),
            _device_indexes=dict(description.get("device_indexes") or {}),
            user=description.get("user"),
            owner=description.get("owner"),
            shared=bool(description.get("shared")),
            protocol=description.get("protocol"),
            bridge_version=description.get("bridge_version"),
            _contexts=[],
            _class_cache={},
            _output=None,
            _tunnel=None,
            _orig_show=None,
            _module_uploaded=set(),
            _cell_source=None,
            log_cell_source=True,
        )
        self.__dict__["_output"] = _Output(self)
        self.__dict__["_tunnel"] = _Tunnel(self)
        self._install_cell_hook()
        self._maybe_install_show_hook()
        session._start_heartbeat()

    # -- attribute routing --------------------------------------------------
    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        if name == "output":
            return self._output
        if name == "tunnel":
            return self._tunnel
        if name == "enter_new_flow_run":
            return self.enter_new_flow_run
        ctx = self._current_context()
        if ctx is not None and name in _CONTEXT_NAMES:
            return getattr(ctx, name)
        match = _GET_INDEX_DEVICES_RE.match(name)
        if match and match.group("index") in self._device_indexes:
            return self._make_index_accessor(match.group("index"))
        if name in self._classes:
            return self._class_symbol(name)
        if name in self._functions:
            return _Function(self._session, name)
        return self._session.invoke({"op": "get", "path": [name]})

    def __setattr__(self, name, value):
        if name == "log_cell_source":
            self.__dict__["log_cell_source"] = bool(value)
            return
        raise AttributeError("balthazar.%s cannot be assigned through the bridge" % name)

    def __dir__(self):
        names = set(self._functions) | set(self._classes)
        names.update({"tunnel", "output", "enter_new_flow_run"})
        names.update("get_%s_devices" % index for index in self._device_indexes)
        return sorted(names)

    def __repr__(self):
        return "<balthazar via %s>" % self._session.location

    def close(self):
        """Best-effort teardown: close contexts, restore ``plt.show``, stop threads."""
        for frame in reversed(list(self._contexts)):
            try:
                self._exit_context(frame, RuntimeError("client closed with the context still open"))
            except Exception:
                pass
        self._remove_show_hook()
        self._session.close()

    # -- classes & indexes --------------------------------------------------
    def _class_symbol(self, name):
        cls = self._class_cache.get(name)
        if cls is None:
            cls = _make_class_symbol(self._session, name)
            self._class_cache[name] = cls
        return cls

    def _make_index_accessor(self, index):
        tunnel = self._tunnel

        def accessor(value, *, refresh=False):
            return tunnel.cached_devices(index, value, refresh=refresh)

        accessor.__name__ = "get_%s_devices" % index
        accessor.__qualname__ = accessor.__name__
        accessor.__doc__ = (
            "Tunnel-only: devices whose %r index matches ``value``, as read-only "
            "CachedDevice objects from the bridge's device cache. Generated from the "
            "bridge's configured indexes; does not exist on a real Runner." % index
        )
        return accessor

    # -- contexts -----------------------------------------------------------
    def _current_context(self):
        return self._contexts[-1]._ctx if self._contexts else None

    def _current_frame(self):
        return self._contexts[-1] if self._contexts else None

    def enter_new_flow_run(self, name=None, devices=None, parameters=None, **kw):
        """Create a child flow run, enter it, and route the module names to it.

        Returns a context manager; the child run starts immediately (as on the
        Runner) and closes when the ``with`` block leaves. An exception leaving the
        block fails the run and is re-raised. See ``_ClientContext`` for the Runner's
        sibling-only nesting limitation.
        """
        factory = _Function(self._session, "new_flow_run_context")
        ctx_obj = factory(name=name, devices=devices, parameters=parameters, **kw)
        ctx_obj.__enter__()  # register the context with the server watchdog
        frame = _ClientContext(self, ctx_obj, name)
        self._contexts.append(frame)
        self._maybe_install_show_hook()
        self._session._start_heartbeat()
        source = self._take_cell_source()
        if source:
            try:
                getattr(ctx_obj, "info")(source)
            except Exception:
                pass
        return frame

    def _exit_context(self, frame, error):
        if frame.finished:
            return
        payload = {"op": "exit", "ref": frame._ctx._ref}
        if error is not None:
            payload["error"] = "%s: %s" % (type(error).__name__, error)
            payload["interrupted"] = isinstance(error, KeyboardInterrupt)
        try:
            self._session.invoke(payload, target=frame._ctx)
        finally:
            frame.finished = True
            try:
                self._contexts.remove(frame)
            except ValueError:
                pass
            # Keep the plt.show hook installed after the stack empties so module-level
            # plots are still captured; only Remote.close() tears the hook down.

    # -- notebook cell source (SPEC Client §8) ------------------------------
    def _install_cell_hook(self):
        try:
            from IPython import get_ipython
        except ImportError:
            return
        ip = get_ipython()
        if ip is None:
            return
        try:
            ip.events.register("pre_run_cell", self._capture_cell)
        except Exception:
            pass

    def _capture_cell(self, info=None):
        self.__dict__["_cell_source"] = getattr(info, "raw_cell", None)

    def _take_cell_source(self):
        if not self.log_cell_source or not self._cell_source:
            return None
        text = self._cell_source.strip()
        if not text:
            return None
        if len(text) > 8000:
            text = "%s\n... [truncated, %d chars]" % (text[:8000], len(text))
        return text

    # -- plt.show capture (SPEC Client §7) ----------------------------------
    def _maybe_install_show_hook(self):
        if self._orig_show is not None:
            return
        if "matplotlib.pyplot" not in sys.modules and "matplotlib" not in sys.modules:
            return
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            return
        remote = self
        self.__dict__["_orig_show"] = plt.show

        def _tunnel_show(*args, **kwargs):
            try:
                remote._upload_open_figures()
            except Exception as exc:  # never break local plotting
                print("tunnel warning: figure upload failed: %s" % exc, file=sys.stderr)
            return remote._orig_show(*args, **kwargs) if remote._orig_show else None

        plt.show = _tunnel_show

    def _remove_show_hook(self):
        if self._orig_show is None:
            return
        try:
            import matplotlib.pyplot as plt
            plt.show = self._orig_show
        except ImportError:
            pass
        self.__dict__["_orig_show"] = None

    def _render_open_figures(self):
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            return []
        figures = []
        for num in plt.get_fignums():
            fig = plt.figure(num)
            buf = io.BytesIO()
            # A fixed hashsalt and no embedded date make the SVG byte-stable, so an
            # unchanged figure hashes the same and is not re-uploaded on the next show.
            with plt.rc_context({"svg.hashsalt": "balthazar-bridge"}):
                fig.savefig(buf, format="svg", bbox_inches="tight", metadata={"Date": None})
            data = buf.getvalue()
            label = fig.get_label() or ("figure_%d" % num)
            figures.append({
                "num": num,
                "filename": "%s.svg" % label,
                "data": data,
                "hash": hashlib.sha256(data).hexdigest(),
            })
        return figures

    def _upload_open_figures(self):
        frame = self._current_frame()
        seen = frame.uploaded if frame is not None else self._module_uploaded
        fresh = [f for f in self._render_open_figures() if f["hash"] not in seen]
        if not fresh:
            return 0
        svg = _Symbol(self._session, ("VisualizationDataType", "SVG"))
        builders = [
            _Deferred(
                self._session,
                ("VisualizationBuilder",),
                (item["filename"], item["data"]),
                {"type": svg, "figure_id": item["num"]},
            )
            for item in fresh
        ]
        if frame is not None:
            getattr(frame._ctx, "store_visualizations")(builders)
        else:
            _Function(self._session, "store_visualizations")(builders)
        for item in fresh:
            seen.add(item["hash"])
        return len(fresh)


def _check_protocol(description):
    protocol = description.get("protocol")
    if protocol != 3:
        seen = protocol if protocol is not None else "an older one without a protocol field"
        raise BridgeError(
            "this bridge speaks protocol %s but this client needs protocol 3; "
            "update the bridge flow (flows/tunnel_bridge.py) or use a matching client"
            % (seen,)
        )


# ---------------------------------------------------------------------------
# Public API: connect, profiles.
# ---------------------------------------------------------------------------
def connect(
    app_url=None,
    site=None,
    login="device",
    username=None,
    password=None,
    ca_file=None,
    remember=True,
    client_id=CLIENT_ID,
    interactive=True,
    *,
    _test_base_url=None,
    _test_user_id=None,
    _skip_protocol_check=False,
):
    """Connect to a bridge and return a :class:`Remote`.

    ``interactive=False`` (or ``$BALTHAZAR_TUNNEL_NONINTERACTIVE=1``) turns a
    missing/invalid token into :class:`LoginRequired` instead of prompting.

    Test hooks (documented, underscore-prefixed, never used in production):

    * ``_test_base_url`` / ``_test_user_id`` — skip the whole login/site dance and
      POST straight to ``<base>/call`` with ``X-BLT-User-Id: <user>``. ``base`` is
      e.g. ``"http://127.0.0.1:<port>/"``.
    * ``_skip_protocol_check`` — connect even when ``describe`` does not report
      protocol 3.
    """
    if _test_base_url is not None:
        session = _TestSession(_test_base_url, _test_user_id)
    else:
        session = _Session(
            app_url, site, login, username, password, ca_file, remember, client_id,
            _resolve_interactive(interactive),
        )
    try:
        description = session.invoke({"op": "describe"})
        if not _skip_protocol_check:
            _check_protocol(description)
    except BaseException:
        session.close()
        raise
    return Remote(session, description)


def save_profile(app_url, login, site=None, ca_file=None):
    """Write the connection profile to ``~/.balthazar_bridge.json`` (0600).

    It never holds a token — those stay in the OAuth token cache
    (``~/.config/balthazar/remote.json``). Returns the profile path.
    """
    profile = {"app_url": app_url, "login": login}
    if site:
        profile["site"] = site
    if ca_file:
        profile["ca_file"] = ca_file
    BRIDGE_PROFILE.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(str(BRIDGE_PROFILE), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as file:
        json.dump(profile, file)
    os.chmod(str(BRIDGE_PROFILE), 0o600)
    return BRIDGE_PROFILE


def load_profile():
    try:
        data = json.loads(BRIDGE_PROFILE.read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _profile_app_url(profile):
    """The app_url to connect to: ``$BALTHAZAR_BRIDGE_URL`` overrides the profile."""
    return os.environ.get(BRIDGE_URL_ENV) or (profile.get("app_url") if profile else None)


def connect_from_profile(interactive=True, **kw):
    """Connect using the saved profile (``$BALTHAZAR_BRIDGE_URL`` overrides app_url).

    Extra keyword arguments are forwarded to :func:`connect` (this is how tests pass
    ``_test_base_url`` / ``_test_user_id``).
    """
    profile = load_profile() or {}
    app_url = _profile_app_url(profile)
    if app_url is None and "_test_base_url" not in kw:
        raise BridgeError(
            "no bridge profile; run `blt-tunnel connect <app url>` or set $%s" % BRIDGE_URL_ENV
        )
    return connect(
        app_url,
        site=profile.get("site"),
        login=profile.get("login", "device"),
        ca_file=profile.get("ca_file"),
        interactive=interactive,
        **kw,
    )
