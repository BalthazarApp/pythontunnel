"""Balthazar Session Tunnel — client side, v2.

Higher-fidelity stand-in for the Runner-injected ``balthazar`` module. Unlike the
v1 shim (one self-contained call per request), this one supports **open flow-run
contexts**, so local code reads exactly like a real flow:

    import balthazar as blt

    with blt.enter_new_flow_run(name="I-V sweep", devices=[device]):
        plt.plot(bias, current)
        plt.show()                      # visualization lands on THIS run
        blt.output["r_zero_ohm"] = 12.3 # output lands on THIS run
        device.params.update({...})     # device write, attributed to THIS run

Contexts nest, and an exception inside the block marks that run FAILED — the same
semantics the Runner gives you, because it is the Runner doing the work.

Run scripts from *this* directory so ``import balthazar`` resolves here rather than
to the v1 shim one level up. Naming the file ``balthazar`` is safe: the Runner
registers its module via ``pyo3::append_to_inittab!``, making it a builtin, and
CPython consults ``BuiltinImporter`` before ``PathFinder`` — so the real module
always wins on the Runner.

Requires ``flows/tunnel_session_server.py`` to be running as a flow in Balthazar.
"""

from __future__ import annotations

import atexit
import base64
import datetime
import hashlib
import importlib.util
import io
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Mapping
from typing import Any, Callable, Optional

__all__ = [
    "enter_new_flow_run",
    "new_flow_run",
    "search_devices",
    "search_objects",
    "search_flows",
    "search_flow_run_history",
    "fetch_visualizations",
    "tunnel_space_schema",
    "cached_devices",
    "device_cache_status",
    "refresh_device_cache",
    "tunnel_cached_devices_query",
    "Device",
    "Flow",
    "FlowRun",
    "Visualization",
    "output",
    "parent",
    "parents",
    "log_cell_source",
    "tunnel_state",
    "reset_contexts",
    "info",
    "warn",
    "error",
    "ping",
    "tunnel_connect",
    "tunnel_disconnect",
    "tunnel_transport",
    "TunnelError",
    "TunnelLoginRequired",
]

__balthazar_tunnel__ = True

CONNECTION_FILE = os.path.expanduser("~/.balthazar_session_tunnel.json")
_TIMEOUT_S = 300.0
# Calls that may trigger a full device-cache load (250k+ devices) use a long timeout.
_LOAD_TIMEOUT_S = 1800.0
# Page size for transparently paging a whole-space cached_devices_query. Keeps each
# response well under the request-body cap even for full §1 records.
_QUERY_PAGE_SIZE = 10000
_HEARTBEAT_INTERVAL_S = 30.0

# App-transport (SPEC §7) knobs.
_APP_URL_ENV = "BALTHAZAR_SESSION_TUNNEL_APP_URL"
# One request may not exceed the platform's 60 s app-tunnel cap, so each POST is
# clamped to just above it; a long op is a quick wait=False call plus polling, not
# one blocking request.
_APP_REQUEST_TIMEOUT_S = 65.0
# Poll a long op's status op this often, printing a one-line progress notice.
_POLL_INTERVAL_S = 2.0
# Default byte budget for cache-backed paged ops (server cuts at whole records and
# returns next_offset; the transport follows it until None).
_APP_MAX_BYTES = 2_000_000
# Long ops: op -> the status op the app transport polls while it loads.
_POLL_OPS = {
    "space_schema": "space_schema_status",
    "cached_devices": "device_cache_status",
    "refresh_device_cache": "device_cache_status",
}
# Cache-backed ops whose byte-budgeted pages the app transport follows transparently.
_PAGED_OPS = ("cached_devices", "cached_devices_query")

_CLIENT_ID = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"


class TunnelError(RuntimeError):
    """The tunnel itself failed (unreachable, bad token, malformed reply)."""


class TunnelLoginRequired(TunnelError):
    """App transport needs an interactive login that this process must not run.

    Raised (instead of blocking on a device-code/browser prompt) when the app
    transport is used non-interactively — ``$BALTHAZAR_TUNNEL_NONINTERACTIVE=1``,
    as an MCP stdio server or ``blt-tunnel doctor`` sets — and no cached token
    works. The fix is to run ``blt-tunnel connect`` in a terminal.
    """


_ERROR_TYPES: dict[str, type[BaseException]] = {
    "KeyError": KeyError,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "FileNotFoundError": FileNotFoundError,
    "PermissionError": PermissionError,
    "NotImplementedError": NotImplementedError,
    "TimeoutError": TimeoutError,
}


# ----------------------------------------------------------------------------
# Transport
# ----------------------------------------------------------------------------
#
# ``_call`` is transport-agnostic: it pops ``_timeout``, stamps ``client_id`` and
# hands the (op, kwargs, timeout) to whichever transport is active. Two transports
# answer the same JSON envelope ``{op, kwargs}`` -> ``{ok, result|error}``:
#
# * loopback (unchanged) — POST ``{url}/rpc`` with a bearer token;
# * app (SPEC §7) — POST ``{tunnel}/rpc`` with only the grant cookie, through the
#   platform's app tunnel, with login handled by the sibling ``_app_auth`` module.
#
# Selection precedence: ``$BALTHAZAR_SESSION_TUNNEL_APP_URL`` > a profile whose
# ``transport`` is ``"app"`` > the existing loopback profile/env. The resolved
# transport is cached and rebuilt by ``tunnel_connect``/``tunnel_disconnect``.


def _raise_remote(err: Mapping) -> None:
    """Re-raise a remote error as its mapped builtin type, carrying the traceback.

    Unknown types become :class:`TunnelError`. The remote traceback (present on the
    app transport, SPEC §7) is attached as ``exc.remote_traceback`` so a caller can
    see where the Runner failed; it is ``None`` on the loopback transport.
    """
    exc_type = _ERROR_TYPES.get(err.get("type", ""), TunnelError)
    exc = exc_type(err.get("message") or "unknown remote error")
    try:
        exc.remote_traceback = err.get("traceback")
    except (AttributeError, TypeError):  # pragma: no cover - builtins allow it
        pass
    raise exc


def _unwrap(payload: Mapping) -> Any:
    """Turn a decoded ``{ok, result|error}`` reply into a result or raised error."""
    if payload.get("ok"):
        return payload.get("result")
    _raise_remote(payload.get("error") or {})


def _to_jsonable(value: Any) -> Any:
    """Convert numpy-ish values (anything with ``tolist()``) to builtins, recursively.

    The app transport serializes with plain ``json``; a numpy array/scalar passed as
    a kwarg would otherwise fail to encode (SPEC §7: "values with tolist() are
    converted before sending").
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        return {key: _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return _to_jsonable(tolist())
    return value


class _LoopbackTransport:
    """The original transport: bearer-token POST to a local ``{url}/rpc``."""

    kind = "loopback"

    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token

    def call(self, op: str, kwargs: dict, timeout: float) -> Any:
        body = json.dumps({"op": op, "kwargs": kwargs}).encode("utf-8")
        req = urllib.request.Request(
            f"{self.url}/rpc", data=body, method="POST",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {self.token}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")[:400]
            raise TunnelError(f"Tunnel returned HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise TunnelError(
                f"Cannot reach the tunnel at {self.url} ({exc.reason}). Is the flow still running?"
            ) from exc
        except (ValueError, OSError) as exc:
            raise TunnelError(f"Malformed tunnel reply: {exc}") from exc
        return _unwrap(payload)


class _AppTransport:
    """Talks to the platform app tunnel: cookie-only POST, polling, page following.

    The 60 s per-request cap forces long ops into start-then-poll (SPEC §7). For
    ``space_schema`` / ``cached_devices`` / ``refresh_device_cache`` the shim always
    sends ``wait=False`` and, while the reply is a ``{state: loading|empty}``
    sentinel, polls the matching status op every 2 s with a one-line progress notice.
    Cache-backed ops (``cached_devices`` / ``cached_devices_query``) send ``max_bytes``
    and have their ``next_offset`` pages followed transparently.
    """

    kind = "app"

    def __init__(self, session: Any):
        self.session = session

    def _rpc(self, op: str, kwargs: dict, timeout: float) -> Any:
        body = json.dumps({"op": op, "kwargs": kwargs}).encode("utf-8")
        per_request = min(timeout, _APP_REQUEST_TIMEOUT_S)
        try:
            status, raw = self.session.post_json("/rpc", body, per_request)
        except Exception as exc:  # noqa: BLE001 - translate the one auth case, re-raise the rest
            login_required = getattr(_app_auth_module, "LoginRequired", None)
            if login_required is not None and isinstance(exc, login_required):
                raise TunnelLoginRequired(str(exc)) from exc
            raise
        if status == 404:
            raise TunnelError("tunnel flow not running / app tunnel not active (HTTP 404)")
        if status == 504:
            raise TunnelError("call exceeded the 60 s app-tunnel limit (HTTP 504)")
        if status != 200:
            detail = raw.decode("utf-8", "replace")[:400] if isinstance(raw, (bytes, bytearray)) else str(raw)
            raise TunnelError(f"App tunnel returned HTTP {status}: {detail}")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, AttributeError) as exc:
            raise TunnelError(f"Malformed tunnel reply: {exc}") from exc
        return _unwrap(payload)

    def call(self, op: str, kwargs: dict, timeout: float) -> Any:
        kwargs = _to_jsonable(dict(kwargs))
        if op in _PAGED_OPS:
            kwargs.setdefault("max_bytes", _APP_MAX_BYTES)
        status_op = _POLL_OPS.get(op)
        if status_op is None:
            return self._follow_pages(op, kwargs, self._rpc(op, kwargs, timeout), timeout)

        # Long op: the shim always sends wait=False (a blocking build would hit the
        # 60 s cap) and polls the status op while the server reports progress.
        intended_wait = kwargs.get("wait", True)
        kwargs = {**kwargs, "wait": False}
        result = self._rpc(op, kwargs, timeout)
        if intended_wait and _is_loading(result):
            final = self._poll(status_op, kwargs.get("client_id"), timeout)
            if op == "refresh_device_cache":
                return final  # post-reload status, as wait=True would have returned
            if _is_error(final):
                raise TunnelError(_state_error(op, final))
            if op == "space_schema":
                return final.get("digest")  # the status carries the built digest
            result = self._rpc(op, kwargs, timeout)  # cached_devices: fetch now ready

        if op == "space_schema":
            if _is_error(result):
                raise TunnelError(_state_error(op, result))
            return result.get("digest") if isinstance(result, dict) else result
        if op == "refresh_device_cache":
            return result
        if _is_error(result):
            raise TunnelError(_state_error(op, result))
        return self._follow_pages(op, kwargs, result, timeout)

    def _poll(self, status_op: str, client_id: Any, timeout: float) -> dict:
        deadline = time.time() + timeout
        kwargs = {"client_id": client_id} if client_id else {}
        while time.time() < deadline:
            status = self._rpc(status_op, dict(kwargs), timeout)
            status = status if isinstance(status, dict) else {}
            _print_progress(status_op, status)
            if status.get("state") in ("ready", "error"):
                return status
            time.sleep(_POLL_INTERVAL_S)
        raise TunnelError(f"timed out waiting for {status_op} to finish")

    def _follow_pages(self, op: str, kwargs: dict, first: Any, timeout: float) -> Any:
        """Follow byte-budgeted ``next_offset`` pages (SPEC §7) for cache-backed ops.

        ``cached_devices`` is returned as a bare list (its old shape); the paged
        ``cached_devices_query`` reply is returned whole with its pages concatenated
        and ``next_offset`` cleared.
        """
        if op not in _PAGED_OPS or not isinstance(first, dict) or "next_offset" not in first:
            return first  # unpaged reply (e.g. a legacy bare list) — nothing to follow
        devices = list(first.get("devices") or [])
        nxt = first.get("next_offset")
        while nxt is not None:
            page = self._rpc(op, {**kwargs, "offset": nxt}, timeout)
            if not isinstance(page, dict):
                devices.extend(page or [])
                break
            devices.extend(page.get("devices") or [])
            nxt = page.get("next_offset")
        if op == "cached_devices":
            return devices
        merged = dict(first)
        merged["devices"] = devices
        merged["next_offset"] = None
        return merged


# A long op is "still loading" while its state is any non-terminal one. The server
# uses "building" for the schema and "loading" for the device cache, plus "empty"
# before either starts; "ready"/"error" are the terminal states.
_LOADING_STATES = ("empty", "building", "loading", "pending")


def _is_loading(result: Any) -> bool:
    return isinstance(result, dict) and result.get("state") in _LOADING_STATES


def _is_error(result: Any) -> bool:
    return isinstance(result, dict) and result.get("state") == "error"


def _state_error(op: str, status: dict) -> str:
    return status.get("error") or f"{op} failed on the server (state=error)"


def _print_progress(status_op: str, status: dict) -> None:
    """One-line stderr progress notice while a long op loads (app transport only)."""
    label = "space schema" if status_op == "space_schema_status" else "device cache"
    detail = ""
    progress = status.get("progress")
    if isinstance(progress, Mapping):
        loaded, total = progress.get("loaded_flows"), progress.get("total_flows")
        if loaded is not None or total is not None:
            detail = f" ({loaded}/{total} flows)"
    else:
        bits = [f"{key}={status[key]}" for key in ("loaded", "count") if status.get(key) is not None]
        if bits:
            detail = f" ({', '.join(bits)})"
    print(f"tunnel: {label} {status.get('state', 'loading')}…{detail}", file=sys.stderr)


_app_auth_module: Any = None


def _load_app_auth() -> Any:
    """Load the sibling ``_app_auth`` module by file path and return its ``AppSession``.

    Loaded by path (not imported as a package) so the stdlib-only shim keeps working
    when copied out next to its ``_app_auth.py`` sibling, with no package on the path.
    """
    global _app_auth_module
    if _app_auth_module is None:
        here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(here, "_app_auth.py")
        if not os.path.exists(path):
            raise TunnelError(
                f"No app-tunnel auth module at {path}; the app tunnel needs "
                "session_tunnel/_app_auth.py alongside this shim."
            )
        spec = importlib.util.spec_from_file_location("blt_session_app_auth", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _app_auth_module = module
    return _app_auth_module.AppSession


_transport: Any = None
_transport_lock = threading.RLock()


def _load_profile() -> Optional[dict]:
    try:
        with open(CONNECTION_FILE, encoding="utf-8") as fh:
            conf = json.load(fh)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise TunnelError(f"Unreadable connection file {CONNECTION_FILE}: {exc}") from exc
    return conf if isinstance(conf, dict) else None


def _noninteractive() -> bool:
    """Whether the app transport must never start an interactive login (SPEC §7).

    Set ``$BALTHAZAR_TUNNEL_NONINTERACTIVE=1`` in an MCP stdio server or
    ``blt-tunnel doctor``: a device-code/browser prompt would block it for minutes.
    """
    return os.environ.get("BALTHAZAR_TUNNEL_NONINTERACTIVE") == "1"


def _app_session_from_profile(app_url: str, profile: dict, *, interactive: Optional[bool] = None) -> Any:
    AppSession = _load_app_auth()
    if interactive is None:
        interactive = not _noninteractive()
    return AppSession(
        app_url,
        site=profile.get("site"),
        login=profile.get("login", "device"),
        username=profile.get("username"),
        ca_file=profile.get("ca_file"),
        client_id=profile.get("client_id", "blt-frontend2"),
        interactive=interactive,
    )


def _build_transport() -> Any:
    """Resolve the active transport by SPEC §7 precedence, or ``None`` if unconfigured."""
    profile = _load_profile()
    app_profile = profile if (isinstance(profile, dict) and profile.get("transport") == "app") else {}

    app_url = os.environ.get(_APP_URL_ENV) or app_profile.get("app_url")
    if app_url:
        return _AppTransport(_app_session_from_profile(app_url, app_profile))

    env_url = os.environ.get("BALTHAZAR_SESSION_TUNNEL_URL")
    env_token = os.environ.get("BALTHAZAR_SESSION_TUNNEL_TOKEN")
    url = env_url or (profile.get("url") if profile else None)
    token = env_token or (profile.get("token") if profile else None)
    if url and token:
        return _LoopbackTransport(url, token)
    return None


def _active_transport() -> Any:
    global _transport
    with _transport_lock:
        if _transport is None:
            _transport = _build_transport()
        if _transport is None:
            raise TunnelError(
                "No tunnel connection. Start the 'flows/tunnel_session_server.py' flow "
                f"(loopback writes {CONNECTION_FILE}), run 'blt-tunnel connect <app url>' "
                f"for a remote app tunnel, or set ${_APP_URL_ENV}."
            )
        return _transport


def _set_transport(transport: Any) -> None:
    """Install ``transport`` as active and clear caches that key off the old server."""
    global _transport, _advertised_indexes_cache
    with _transport_lock:
        _transport = transport
    _advertised_indexes_cache = None
    _root.clear()


def _reset_transport() -> None:
    """Drop the cached transport so the next call re-resolves env/profile afresh."""
    _set_transport(None)


def _call(op: str, **kwargs: Any) -> Any:
    # ``_timeout`` is popped before serialization, so it never reaches the wire; it
    # lets calls that may trigger a device-cache load wait far longer than a read.
    timeout = kwargs.pop("_timeout", None) or _TIMEOUT_S
    kwargs.setdefault("client_id", _CLIENT_ID)
    return _active_transport().call(op, kwargs, timeout)


# ----------------------------------------------------------------------------
# Local mirror of the server's context stack
# ----------------------------------------------------------------------------


class _Frame:
    """Local reflection of one open flow-run context."""

    def __init__(self, flow_run_id, name, params, devices):
        self.flow_run_id = flow_run_id
        self.name = name
        self.params = dict(params or {})
        self.devices = list(devices or [])
        self.output: dict[str, Any] = {}
        self.uploaded: set[str] = set()   # SVG hashes, to avoid re-sending figures


_frames: list[_Frame] = []
_root: dict[str, Any] = {}               # identity of the tunnel's own run
_orig_show = None
_heartbeat_stop: Optional[threading.Event] = None


def _current() -> Optional[_Frame]:
    return _frames[-1] if _frames else None


def _root_info() -> dict[str, Any]:
    if not _root:
        _root.update(_call("ping"))
    return _root


class _Ident:
    """Stands in for blt.flow / blt.session / blt.flow_run."""

    def __init__(self, id_, name=None):
        self.id = id_
        self.name = name
        self.tags: list[str] = []

    def __repr__(self):
        return f"<{type(self).__name__} id={self.id} name={self.name!r}>"


def __getattr__(name: str) -> Any:
    """Make the context-dependent globals dynamic (PEP 562).

    ``blt.params``, ``blt.devices``, ``blt.flow_run`` and friends must reflect the
    *innermost* open context, exactly as the Runner rebinds its module attributes
    on entering a child run. A plain module-level assignment could not do that.
    """
    frame = _current()
    if name == "params":
        return _ReadOnlyMapping(frame.params if frame else _root_info().get("params", {}))
    if name in ("devices", "objects"):
        return list(frame.devices) if frame else []
    if name in ("device", "object"):
        items = list(frame.devices) if frame else []
        return items[0] if items else None
    if name == "flow_run":
        return _Ident(frame.flow_run_id if frame else _root_info()["flow_run_id"],
                      frame.name if frame else None)
    if name == "session":
        return _Ident(frame.flow_run_id if frame else _root_info()["session_id"])
    if name == "flow":
        info = _root_info()
        return _Ident(info["flow_id"], info["flow_name"])
    if name == "secrets":
        raise NotImplementedError(
            "blt.secrets is deliberately not tunnelled: it would make any process "
            "that can reach the port able to read your credentials."
        )
    if name == "context":
        raise NotImplementedError(
            "blt.context (the Runner's FlowRunContext for the current run, added in "
            "1.35.1) is not emulated. Use the module-level names — blt.output, "
            "blt.devices, blt.params — which this shim already rebinds to the "
            "innermost open context, or blt.tunnel_state() for the server's own view "
            "of the context stack."
        )
    # Dynamic per-index accessors: blt.get_wafer_devices("W123"). Generated only for
    # the indexes the server advertises in ping, so an unknown index still raises
    # AttributeError rather than silently returning a broken accessor.
    match = _GET_INDEX_DEVICES_RE.match(name)
    if match:
        index_name = match.group("index")
        if index_name in _advertised_indexes():
            return _make_index_accessor(index_name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Include the dynamic ``get_<index>_devices`` accessors for tab-completion."""
    names = set(globals()) | set(__all__)
    names.update(f"get_{index}_devices" for index in _advertised_indexes())
    return sorted(names)


class _ReadOnlyMapping(Mapping):
    def __init__(self, data):
        self._data = dict(data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return repr(self._data)


# ----------------------------------------------------------------------------
# Output proxy
# ----------------------------------------------------------------------------


class _Output:
    """``blt.output`` — writes through to whichever run is currently open.

    Primitives only, as on the real platform: str / int / float / bool / small
    list. Dicts, dates and DataFrames are rejected here rather than silently
    mangled in transport.
    """

    _ALLOWED = (str, int, float, bool, list, tuple, type(None))

    def _target(self) -> dict[str, Any]:
        frame = _current()
        return frame.output if frame else _root.setdefault("_output", {})

    def _validate(self, values):
        for key, value in values.items():
            if not isinstance(value, self._ALLOWED):
                raise TypeError(
                    f"blt.output[{key!r}]: {type(value).__name__} is not a flow-run "
                    "output primitive (use str / int / float / bool / small list)"
                )

    def update(self, values: Mapping) -> None:
        values = dict(values)
        self._validate(values)
        _call("set_output", values=values)
        self._target().update(values)

    def setdefault(self, key: str, default: Any = None) -> Any:
        target = self._target()
        if key not in target:
            self.update({key: default})
        return target[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.update({key: value})

    def __getitem__(self, key: str) -> Any:
        return self._target()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._target().get(key, default)

    def keys(self):
        return self._target().keys()

    def items(self):
        return self._target().items()

    def __contains__(self, key):
        return key in self._target()

    def __repr__(self):
        return repr(self._target())


output = _Output()


# ----------------------------------------------------------------------------
# Devices
# ----------------------------------------------------------------------------


class _DeviceParams(Mapping):
    """``device.params`` — read-through cache with a write-through ``update``.

    Fidelity choices, so code written here behaves the same once deployed:

    * ``update({...})`` is the only write path, because on the real proxy a
      subscript assignment does not reliably sync (notably for dict values).
      Subscript raises here rather than silently losing the write — the real
      failure is silent, which is worse to develop against.
    * ``pop(key, default)`` raises ``KeyError`` on a missing key, matching the
      real proxy, where the default is not honoured.
    * ``del params[key]`` works: removal is the one thing ``update`` cannot do.
    """

    def __init__(self, device_id: str, initial: Optional[dict] = None):
        self._device_id = device_id
        self._cache = dict(initial) if initial is not None else None

    def _data(self) -> dict[str, Any]:
        if self._cache is None:
            self._cache = _call("get_device_params", id=self._device_id)
        return self._cache

    def refresh(self) -> "_DeviceParams":
        self._cache = None
        return self

    def __getitem__(self, key):
        return self._data()[key]

    def __iter__(self):
        return iter(self._data())

    def __len__(self):
        return len(self._data())

    def __repr__(self):
        return repr(self._data())

    def update(self, values: Mapping) -> None:
        values = dict(values)
        if not values:
            return
        result = _call("update_device_params", id=self._device_id, values=values)
        self._cache = result.get("params", {})

    def __setitem__(self, key, value):
        raise NotImplementedError(
            f"params[{key!r}] = ... does not reliably sync on the real Balthazar "
            f"proxy. Use params.update({{{key!r}: ...}}) instead — including for "
            "dict values, and for nested changes rebuild the top-level key."
        )

    def __delitem__(self, key):
        if key not in self._data():
            raise KeyError(key)
        result = _call("update_device_params", id=self._device_id, delete=[key])
        self._cache = result.get("params", {})

    def pop(self, key, *default):
        # Bug-compatible: the real proxy ignores the default and raises.
        if key not in self._data():
            raise KeyError(
                f"{key!r} — note the real params proxy also raises here even when "
                "you pass a default. Guard a del instead."
            )
        value = self._data()[key]
        del self[key]
        return value

    def clear(self):
        for key in list(self._data()):
            del self[key]


class Device:
    """Local stand-in for a remote ``balthazar.Device``."""

    def __init__(self, payload: dict[str, Any]):
        self.id: str = payload["id"]
        self.name: str = payload.get("name", "")
        self.type: str = payload.get("type", "device")
        self.description: Optional[str] = payload.get("description")
        self.fabrication_date: Optional[str] = payload.get("fabrication_date")
        self.tags: list[str] = payload.get("tags") or []
        self.params = _DeviceParams(self.id, payload.get("params"))

    def __repr__(self):
        return f"<Device {self.name!r} type={self.type!r} id={self.id}>"


def search_devices(
    *,
    id: Optional[str | list[str]] = None,
    type: Optional[str | list[str]] = None,
    name: Optional[str | list[str]] = None,
    tags: Optional[str | list[str]] = None,
    limit: int = 0,
    offset: int = 0,
    archived: Optional[bool] = None,
    keys: Optional[list[str]] = None,
    scalars_only: bool = False,
    include_params: bool = True,
) -> list[Device]:
    """Search devices in the tunnel's context. Filters are ANDed; ``name`` globs.

    ``keys``, ``scalars_only`` and ``include_params`` are **shim-only** projection
    kwargs — they trim what crosses the wire and have no counterpart on a real
    Runner, so ``blt_analytics`` passes them only when it is talking to the tunnel
    (``is_tunnel()``). ``keys`` keeps only those top-level param keys,
    ``scalars_only`` drops dict/list param values, and ``include_params=False``
    omits params entirely. Omitting all three returns the full device record,
    exactly as before.
    """
    payload = _call("search_devices", id=id, type=type, name=name, tags=tags,
                    limit=limit, offset=offset, archived=archived,
                    keys=keys, scalars_only=scalars_only, include_params=include_params)
    return [Device(item) for item in payload]


search_objects = search_devices


# ----------------------------------------------------------------------------
# Read-only schema/history types and functions
# ----------------------------------------------------------------------------


def _parse_dt(value: Any) -> Any:
    """Parse an ISO-8601 string back to a ``datetime``/``date``; pass else through.

    A trailing ``Z`` is normalized to ``+00:00`` because ``fromisoformat`` rejects
    it before Python 3.11. Anything that does not parse is returned unchanged.
    """
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


class Flow:
    """Read-only local view of a ``balthazar.Flow`` (stub attribute names)."""

    def __init__(self, record: dict[str, Any]):
        self.id: str = record.get("id")
        self.name: Optional[str] = record.get("name")
        self.description: Optional[str] = record.get("description")
        self.branch: Optional[str] = record.get("branch")
        self.script_filename: Optional[str] = record.get("script_filename")
        self.tags: list[str] = list(record.get("tags") or [])
        self.created_time = _parse_dt(record.get("created_time"))
        self.username: Optional[str] = record.get("username")
        # {name: {type, default, description}}, straight from the wire.
        self.parameters: dict[str, Any] = dict(record.get("parameters") or {})

    def __repr__(self):
        return f"<Flow {self.name!r} id={self.id}>"


class FlowRun:
    """Read-only local view of a flow run.

    Mirrors the stub's attribute names, with two deliberate differences the tunnel
    forces: ``device_ids`` (the run's devices are not fetched, only their ids) and
    ``status`` as a plain string (the bare status name, e.g. ``"FINISHED"``).
    """

    def __init__(self, record: dict[str, Any]):
        self.id: str = record.get("id")
        self.flow_id: Optional[str] = record.get("flow_id")
        self.flow_name: Optional[str] = record.get("flow_name")
        self.name: Optional[str] = record.get("flow_name")
        self.status: Optional[str] = record.get("status")
        self.created_time = _parse_dt(record.get("created_time"))
        self.started_time = _parse_dt(record.get("started_time"))
        self.finished_time = _parse_dt(record.get("finished_time"))
        self.username: Optional[str] = record.get("username")
        self.tags: list[str] = list(record.get("tags") or [])
        self.comment: Optional[str] = record.get("comment")
        self.device_ids: list[str] = list(record.get("device_ids") or [])
        self.params: dict[str, Any] = dict(record.get("params") or {})
        self.output: dict[str, Any] = dict(record.get("output") or {})
        self.visualization_ids: list[str] = list(record.get("visualization_ids") or [])

    def __repr__(self):
        return f"<FlowRun {self.id} flow={self.flow_name!r} status={self.status}>"


class Visualization:
    """Read-only local view of a visualization; ``.data`` is the decoded bytes."""

    def __init__(self, record: dict[str, Any]):
        self.id: str = record.get("id")
        self.type = record.get("type")
        self.filename: Optional[str] = record.get("filename")
        self.flow_run_id: Optional[str] = record.get("flow_run_id")
        self.timestamp = _parse_dt(record.get("timestamp"))
        data_b64 = record.get("data_b64")
        self.data: bytes = base64.b64decode(data_b64) if data_b64 else b""

    def __repr__(self):
        return f"<Visualization {self.id} {self.filename!r}>"


def search_flows(
    *,
    name: Optional[str | list[str]] = None,
    flow_ids: Optional[list[str]] = None,
    tags: Optional[str | list[str]] = None,
    limit: int = 1000,
    offset: int = 0,
) -> list[Flow]:
    """Search flows in the tunnel's space. Filters are ANDed; ``name`` globs."""
    payload = _call("search_flows", name=name, flow_ids=flow_ids, tags=tags,
                    limit=limit, offset=offset)
    return [Flow(item) for item in payload]


def search_flow_run_history(
    *,
    flow_id: Optional[str] = None,
    device_id: Optional[str] = None,
    flow_run_ids: Optional[list[str]] = None,
    limit: int = 250,
    offset: int = 0,
) -> list[FlowRun]:
    """Search flow run history. Mirrors the real API name; returns ``FlowRun``s."""
    payload = _call("search_flow_runs", flow_id=flow_id, device_id=device_id,
                    flow_run_ids=flow_run_ids, limit=limit, offset=offset)
    return [FlowRun(item) for item in payload]


def fetch_visualizations(ids: Any) -> dict[str, Visualization]:
    """Fetch visualizations by id as ``{id: Visualization}`` with decoded bytes.

    The server caps a single call at 20 ids, so larger requests are batched into
    chunks of 20 here and merged. A bare string id is accepted as a one-element
    list.
    """
    if isinstance(ids, str):
        ids = [ids]
    ids = list(ids or [])
    out: dict[str, Visualization] = {}
    for start in range(0, len(ids), 20):
        chunk = ids[start:start + 20]
        for record in _call("fetch_visualizations", ids=chunk) or []:
            viz = Visualization(record)
            out[viz.id] = viz
    return out


def tunnel_space_schema(refresh: bool = False) -> dict[str, Any]:
    """The server's measurement-free space digest (shim-only, hence the prefix).

    Named ``tunnel_space_schema`` rather than ``space_schema`` because it has no
    counterpart on a real Runner — it is the tunnel asking its host flow to build
    the digest. ``refresh=True`` rebuilds it server-side rather than returning the
    memoized copy.
    """
    return _call("space_schema", refresh=refresh)


# ----------------------------------------------------------------------------
# Server-side device cache (spec §6) — tunnel-only accessors
# ----------------------------------------------------------------------------

_GET_INDEX_DEVICES_RE = re.compile(r"^get_(?P<index>.+)_devices$")
_advertised_indexes_cache: Optional[dict[str, str]] = None


def _advertised_indexes() -> dict[str, str]:
    """The server's configured device indexes ``{name: path}`` from ``ping``, cached.

    Cached because it drives ``get_<index>_devices`` attribute resolution, which must
    not hit the network on every miss. Call ``ping()`` again to refresh it.
    """
    global _advertised_indexes_cache
    if _advertised_indexes_cache is None:
        try:
            info = _call("ping")
            _root.update(info)
            _advertised_indexes_cache = dict(info.get("device_indexes") or {})
        except Exception:  # noqa: BLE001 - treat an unreachable tunnel as no indexes
            _advertised_indexes_cache = {}
    return _advertised_indexes_cache


def _notice_if_loading() -> None:
    """Print a one-line notice when the device cache is still loading or empty, so a
    call that is about to block for a full load is not a silent hang."""
    try:
        status = _call("device_cache_status")
    except Exception:  # noqa: BLE001 - the real call that follows will report it
        return
    if status.get("state") in ("loading", "empty"):
        print("tunnel: loading device cache …", file=sys.stderr)


def cached_devices(index: str, value: Any, *, refresh: bool = False) -> list[Device]:
    """Tunnel-only: devices for one index value, served from the server's device cache.

    Returns the shim's ``Device`` objects, so ``device.params.update(...)`` keeps
    working. If the cache is cold this blocks while the server loads it (hence the long
    timeout). ``refresh=True`` re-fetches that value's known device ids before serving
    (picking up changes and dropping vanished devices); it cannot discover *newly
    added* devices — use :func:`refresh_device_cache` for that. Does not exist on a
    real Runner.
    """
    _notice_if_loading()
    payload = _call("cached_devices", _timeout=_LOAD_TIMEOUT_S,
                    index=index, value=value, refresh=refresh)
    return [Device(item) for item in payload]


def device_cache_status() -> dict[str, Any]:
    """Tunnel-only: the server's device-cache status (state, counts, indexes, path)."""
    return _call("device_cache_status")


def refresh_device_cache(wait: bool = True) -> dict[str, Any]:
    """Tunnel-only: trigger a full device-cache reload on the server.

    ``wait=True`` (default) blocks until the reload finishes and returns the resulting
    status; ``wait=False`` kicks it off in the background and returns immediately. The
    reload replaces the cache atomically on success and keeps the old one on failure.
    """
    _notice_if_loading()
    return _call("refresh_device_cache", _timeout=_LOAD_TIMEOUT_S, wait=wait)


def tunnel_cached_devices_query(
    device_type: Optional[str | list[str]] = None,
    keys: Optional[list[str]] = None,
) -> list[Device]:
    """Tunnel-only: every cached device record, optionally type-filtered and projected.

    Lets whole-space frames avoid paging ``search_devices``. A full pull would make a
    response far larger than the request cap, so this pages the query transparently
    (the server takes ``offset``/``limit``) and returns the concatenated ``Device``
    list. ``keys`` keeps only those top-level param keys. Does not exist on a real
    Runner, hence the ``tunnel_`` prefix.
    """
    _notice_if_loading()
    out: list[Device] = []
    offset = 0
    while True:
        payload = _call("cached_devices_query", _timeout=_LOAD_TIMEOUT_S,
                        device_type=device_type, keys=keys,
                        offset=offset, limit=_QUERY_PAGE_SIZE)
        records = payload["devices"]
        out.extend(Device(item) for item in records)
        total = payload.get("total", len(out))
        offset += len(records)
        if not records or offset >= total or len(records) < _QUERY_PAGE_SIZE:
            break
    return out


def _make_index_accessor(index_name: str) -> Callable[..., list[Device]]:
    """Build a ``get_<index>_devices(value, *, refresh=False)`` accessor."""

    def accessor(value: Any, *, refresh: bool = False) -> list[Device]:
        return cached_devices(index_name, value, refresh=refresh)

    accessor.__name__ = f"get_{index_name}_devices"
    accessor.__qualname__ = accessor.__name__
    accessor.__doc__ = (
        f"Tunnel-only: devices whose '{index_name}' index matches ``value``, from the "
        f"server's device cache (equivalent to ``blt.cached_devices({index_name!r}, "
        f"value)``). This accessor is generated from the tunnel's configured indexes "
        f"and does NOT exist on a real Runner."
    )
    return accessor


# ----------------------------------------------------------------------------
# Notebook cell capture
# ----------------------------------------------------------------------------

log_cell_source = True
"""Send the notebook cell that opened a run into that run's Balthazar logs.

Set ``blt.log_cell_source = False`` to stop shipping your source to the server.
"""

_MAX_CELL_CHARS = 8000
_cell_source: Optional[str] = None


def _capture_cell(info: Any = None) -> None:
    """``pre_run_cell`` hook — remember the cell that is about to execute."""
    global _cell_source
    # Older IPython called this with no argument; tolerate both.
    _cell_source = getattr(info, "raw_cell", None)


def _install_cell_hook() -> bool:
    """Register the cell hook when running under IPython/Jupyter.

    A plain interpreter has no cells, so this is a no-op there and
    ``_take_cell_source`` keeps returning None — demo_sessions.py is unaffected.
    """
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    ip = get_ipython()
    if ip is None:  # imported inside IPython's process but not from a shell
        return False
    ip.events.register("pre_run_cell", _capture_cell)
    return True


_cell_hook_installed = _install_cell_hook()


def _take_cell_source() -> Optional[str]:
    """The current cell's code, trimmed for transport, or None outside a notebook.

    Sent on *every* context the cell opens, not just the first. The innermost run
    is the one that carries the plots, output and device writes, so that is the
    run you open when something looks wrong — it has to be able to show the code
    that produced it.

    The cost is that the Runner propagates a child's log up into every ancestor,
    so an outer run in a nested cell lists the same block once per context below
    it. Redundant, but the alternative loses the code exactly where it is most
    wanted.
    """
    if not log_cell_source or not _cell_source:
        return None
    text = _cell_source.strip()
    if not text:
        return None
    if len(text) > _MAX_CELL_CHARS:
        text = f"{text[:_MAX_CELL_CHARS]}\n... [truncated, {len(text)} chars]"
    return text


# ----------------------------------------------------------------------------
# plt.show() capture
# ----------------------------------------------------------------------------


def _render_open_figures() -> list[dict[str, Any]]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return []
    payload = []
    for index, num in enumerate(plt.get_fignums(), start=1):
        fig = plt.figure(num)
        buf = io.BytesIO()
        fig.savefig(buf, format="svg", bbox_inches="tight")
        data = buf.getvalue()
        label = getattr(fig, "label", "") or f"figure_{index}"
        payload.append({
            # The matplotlib figure number becomes the server-side figure_id, which
            # since Runner 1.35.1 is a *replace* key. That is what we want in a
            # session: redrawing figure 1 across several plt.show() calls leaves the
            # run holding the latest frame instead of a pile of intermediate ones.
            # Open figure numbers are unique, so a batch never self-collides.
            "id": num,
            "filename": f"{label}.svg",
            "svg_base64": base64.b64encode(data).decode("ascii"),
            "_hash": hashlib.sha256(data).hexdigest(),
        })
    return payload


def _upload_open_figures() -> int:
    """Send every open figure not already sent with identical content."""
    frame = _current()
    if frame is None:
        return 0
    fresh = [f for f in _render_open_figures() if f["_hash"] not in frame.uploaded]
    if not fresh:
        return 0
    for item in fresh:
        frame.uploaded.add(item.pop("_hash"))
    _call("store_visualizations", visualizations=fresh)
    return len(fresh)


def _tunnel_show(*args, **kwargs):
    """Patched ``plt.show``: ship figures to the open run, then display normally.

    Wrapping rather than replacing the backend keeps inline display working in
    notebooks. The Runner does this with a real matplotlib backend
    (``MPLBACKEND=module://balthazar.matplotlib.backend``), which would displace
    the inline backend and cost you local rendering.
    """
    try:
        _upload_open_figures()
    except Exception as exc:  # noqa: BLE001 - never break local plotting
        print(f"tunnel warning: figure upload failed: {exc}", file=sys.stderr)
    return _orig_show(*args, **kwargs) if _orig_show else None


def _install_show_hook():
    global _orig_show
    if _orig_show is not None:
        return
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return
    _orig_show = plt.show
    plt.show = _tunnel_show


def _remove_show_hook():
    global _orig_show
    if _orig_show is None:
        return
    try:
        import matplotlib.pyplot as plt
        plt.show = _orig_show
    except ImportError:
        pass
    _orig_show = None


# ----------------------------------------------------------------------------
# Heartbeat — keeps the server from reclaiming an open context
# ----------------------------------------------------------------------------


def _start_heartbeat():
    global _heartbeat_stop
    if _heartbeat_stop is not None:
        return
    _heartbeat_stop = threading.Event()
    stop = _heartbeat_stop

    def beat():
        while not stop.wait(_HEARTBEAT_INTERVAL_S):
            try:
                _call("heartbeat")
            except Exception:  # noqa: BLE001 - the next real call will report it
                pass

    threading.Thread(target=beat, name="tunnel-heartbeat", daemon=True).start()


def _stop_heartbeat():
    global _heartbeat_stop
    if _heartbeat_stop is not None:
        _heartbeat_stop.set()
        _heartbeat_stop = None


# ----------------------------------------------------------------------------
# Flow-run contexts
# ----------------------------------------------------------------------------


class FlowRunContextManager:
    """Mirrors the Runner's context manager, including its exit semantics."""

    def __init__(self, frame: _Frame):
        self._frame = frame
        self.finished = False

    @property
    def flow_run_id(self) -> str:
        return self._frame.flow_run_id

    def __enter__(self) -> "FlowRunContextManager":
        if self.finished:
            raise ValueError("Attempt to reenter a closed flow run context")
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if self.finished:
            raise ValueError("Attempt to exit a closed flow run context")
        error_message = None
        if exc_value is not None:
            error_message = f"{type(exc_value).__name__}: {exc_value}"
        self._close(error_message)
        return False  # re-raise, exactly as the Runner's __exit__ does

    def exit(self) -> None:
        """Close the context explicitly (successful)."""
        if self.finished:
            raise ValueError("Attempt to exit a closed flow run context")
        self._close(None)

    def fail(self, message: str) -> None:
        """Close the context, marking the run FAILED with ``message``."""
        if self.finished:
            raise ValueError("Attempt to exit a closed flow run context")
        self._close(message)

    def _close(self, error_message):
        try:
            _call("exit_flow_run", flow_run_id=self._frame.flow_run_id,
                  error_message=error_message)
        finally:
            self.finished = True
            if _frames and _frames[-1] is self._frame:
                _frames.pop()
            if not _frames:
                _remove_show_hook()
                _stop_heartbeat()


def enter_new_flow_run(
    name: Optional[str] = None,
    *,
    flow_id: Optional[str] = None,
    devices: Optional[list[Device]] = None,
    parameters: Optional[dict[str, Any]] = None,
) -> FlowRunContextManager:
    """Create a child flow run on the Runner and enter its context.

    Like the real API, the context is entered as soon as this returns — the
    ``with`` statement only governs when it closes. Inside the block,
    ``blt.output``, ``plt.show()`` and device writes all target this run, and
    ``blt.params`` / ``blt.devices`` reflect what you passed here.

    An exception escaping the block marks the run FAILED and is re-raised.
    Contexts nest and must close in LIFO order.
    """
    payload = _call(
        "enter_flow_run",
        name=name,
        flow_id=flow_id,
        device_ids=[d.id for d in devices] if devices else None,
        parameters=parameters or {},
        cell_source=_take_cell_source(),
    )
    frame = _Frame(payload["flow_run_id"], name, parameters, devices)
    _frames.append(frame)
    _install_show_hook()
    _start_heartbeat()
    return FlowRunContextManager(frame)


def new_flow_run(
    name: Optional[str] = None,
    *,
    flow_id: Optional[str] = None,
    devices: Optional[list[Device]] = None,
    output: Optional[dict[str, Any]] = None,
    parameters: Optional[dict[str, Any]] = None,
    error_message: Optional[str] = None,
    status: str = "FINISHED",
) -> str:
    """Create a completed run in one call, without opening a context.

    Use when there is nothing to attach — no plots, no incremental output.
    """
    return _call("new_flow_run", name=name, flow_id=flow_id,
                 device_ids=[d.id for d in devices] if devices else None,
                 output=output, parameters=parameters,
                 error_message=error_message, status=status)


def parents() -> list[dict[str, Any]]:
    """Open contexts, outermost first — the analogue of ``blt.parents()``."""
    return [{"flow_run_id": f.flow_run_id, "name": f.name} for f in _frames[:-1]]


def parent() -> Optional[dict[str, Any]]:
    """The immediately enclosing context, or None at the top level."""
    items = parents()
    return items[-1] if items else None


def tunnel_state() -> dict[str, Any]:
    """Ask the server what it thinks the context stack is (a consistency check).

    Named ``tunnel_state`` rather than ``context`` because Runner 1.35.1 added a
    real ``balthazar.context`` — the `FlowRunContext` bound to the current run,
    an object rather than a callable. Keeping this helper under that name would
    make shim-tested code fail on a real Runner with ``'FlowRunContext' object is
    not callable``, which is exactly the class of surprise the shim exists to
    prevent. This is tunnel bookkeeping, not part of the emulated API.
    """
    return _call("ping")


def reset_contexts(reason: str = "manual reset from the client") -> int:
    """Force-close every context on the server, including another client's.

    The escape hatch for a crashed session that left a run stuck in RUNNING.
    """
    closed = _call("reset_contexts", reason=reason)["closed"]
    _frames.clear()
    _remove_show_hook()
    _stop_heartbeat()
    return closed


def ping() -> dict[str, Any]:
    info = _call("ping")
    _root.update(info)
    return info


def info(message: str) -> None:
    _call("log", level="info", message=str(message))


def warn(message: str) -> None:
    _call("log", level="warn", message=str(message))


def error(message: str) -> None:
    _call("log", level="error", message=str(message))


# ----------------------------------------------------------------------------
# Remote app tunnel: connect / disconnect / which transport (SPEC §7)
# ----------------------------------------------------------------------------


def tunnel_connect(
    app_url: str,
    *,
    login: str = "device",
    username: Optional[str] = None,
    password: Optional[str] = None,
    site: Optional[str] = None,
    ca_file: Optional[str] = None,
) -> dict[str, Any]:
    """Log in to a remote Balthazar **app tunnel**, ping it, and save the profile.

    ``app_url`` is the address of the opened tunnel app (``.../app-tunnel/<runner>/
    <flow>/?space_id=...``). ``login`` is ``device`` (default), ``browser`` or
    ``password`` (then ``username``/``password`` are required). Logs in (caching only
    the refresh token, under ``~/.config/balthazar/remote.json``), pings through the
    tunnel to confirm access, and writes the connection profile (0600, **no tokens**)
    so later processes reconnect without this call. Returns the ping info, including
    the ``transport``, the connected ``user`` id and the tunnel's ``flow_run_id``.
    """
    AppSession = _load_app_auth()
    session = AppSession(
        app_url,
        site=site,
        login=login,
        username=username,
        password=password,
        ca_file=ca_file,
        client_id="blt-frontend2",
    )
    transport = _AppTransport(session)
    info_reply = transport.call("ping", {"client_id": _CLIENT_ID}, _TIMEOUT_S)
    info: dict[str, Any] = dict(info_reply) if isinstance(info_reply, dict) else {}
    # Guarantee the keys the CLI reads, mapping the server's ``user`` <-> ``user_id``.
    info.setdefault("transport", "app")
    if not info.get("user_id") and info.get("user"):
        info["user_id"] = info["user"]
    if not info.get("user") and info.get("user_id"):
        info["user"] = info["user_id"]
    info.setdefault("user_id", None)
    info.setdefault("flow_run_id", None)
    info.setdefault("flow_name", None)

    profile: dict[str, Any] = {
        "transport": "app",
        "app_url": app_url,
        "login": login,
        "client_id": "blt-frontend2",
    }
    if site:
        profile["site"] = site
    if ca_file:
        profile["ca_file"] = ca_file
    if username:
        profile["username"] = username
    _write_profile(profile)

    _set_transport(transport)
    _root.update(info)
    return info


def tunnel_disconnect(*, forget: bool = False) -> None:
    """Remove the saved connection profile; with ``forget`` also drop the cached token.

    ``forget=True`` deletes the cached refresh token for an app profile, so the next
    connect logs in from scratch. Removing the profile falls back to the loopback
    env/profile (if any) or to "not connected".
    """
    profile = _load_profile()
    if forget and isinstance(profile, dict) and profile.get("transport") == "app" and profile.get("app_url"):
        try:
            _app_session_from_profile(profile["app_url"], profile).forget()
        except Exception:  # noqa: BLE001 - forgetting a token must never block disconnect
            pass
    try:
        os.remove(CONNECTION_FILE)
    except FileNotFoundError:
        pass
    _reset_transport()


def tunnel_transport() -> str:
    """Which transport is active: ``"loopback"``, ``"app"`` or ``"none"``."""
    try:
        return _active_transport().kind
    except TunnelError:
        return "none"


def _write_profile(profile: Mapping) -> None:
    """Write the connection profile atomically with 0600 permissions (no tokens)."""
    descriptor = os.open(CONNECTION_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as fh:
        json.dump(dict(profile), fh)
    os.chmod(CONNECTION_FILE, 0o600)


@atexit.register
def _close_dangling_contexts():
    """Best-effort close on interpreter exit, so runs do not hang in RUNNING."""
    while _frames:
        frame = _frames[-1]
        try:
            _call("exit_flow_run", flow_run_id=frame.flow_run_id,
                  error_message="client exited with the context still open")
        except Exception:  # noqa: BLE001 - the server's watchdog is the backstop
            pass
        _frames.pop()
