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
import hashlib
import io
import json
import os
import sys
import threading
import urllib.error
import urllib.request
import uuid
from collections.abc import Mapping
from typing import Any, Optional

__all__ = [
    "enter_new_flow_run",
    "new_flow_run",
    "search_devices",
    "search_objects",
    "Device",
    "output",
    "parent",
    "parents",
    "context",
    "reset_contexts",
    "info",
    "warn",
    "error",
    "ping",
    "TunnelError",
]

__balthazar_tunnel__ = True

CONNECTION_FILE = os.path.expanduser("~/.balthazar_session_tunnel.json")
_TIMEOUT_S = 300.0
_HEARTBEAT_INTERVAL_S = 30.0

_CLIENT_ID = f"{os.getpid()}-{uuid.uuid4().hex[:8]}"


class TunnelError(RuntimeError):
    """The tunnel itself failed (unreachable, bad token, malformed reply)."""


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


def _connection() -> tuple[str, str]:
    url = os.environ.get("BALTHAZAR_SESSION_TUNNEL_URL")
    token = os.environ.get("BALTHAZAR_SESSION_TUNNEL_TOKEN")
    if url and token:
        return url, token
    try:
        with open(CONNECTION_FILE, encoding="utf-8") as fh:
            conf = json.load(fh)
    except FileNotFoundError:
        raise TunnelError(
            f"No tunnel connection file at {CONNECTION_FILE}. Start the "
            "'flows/tunnel_session_server.py' flow in Balthazar first."
        ) from None
    except (OSError, ValueError) as exc:
        raise TunnelError(f"Unreadable connection file {CONNECTION_FILE}: {exc}") from exc
    return url or conf["url"], token or conf["token"]


def _call(op: str, **kwargs: Any) -> Any:
    url, token = _connection()
    kwargs.setdefault("client_id", _CLIENT_ID)
    body = json.dumps({"op": op, "kwargs": kwargs}).encode("utf-8")
    req = urllib.request.Request(
        f"{url}/rpc", data=body, method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:400]
        raise TunnelError(f"Tunnel returned HTTP {exc.code}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise TunnelError(
            f"Cannot reach the tunnel at {url} ({exc.reason}). Is the flow still running?"
        ) from exc
    except (ValueError, OSError) as exc:
        raise TunnelError(f"Malformed tunnel reply: {exc}") from exc

    if payload.get("ok"):
        return payload.get("result")
    err = payload.get("error") or {}
    raise _ERROR_TYPES.get(err.get("type", ""), TunnelError)(
        err.get("message") or "unknown remote error"
    )


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
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


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
) -> list[Device]:
    """Search devices in the tunnel's context. Filters are ANDed; ``name`` globs."""
    payload = _call("search_devices", id=id, type=type, name=name, tags=tags,
                    limit=limit, offset=offset, archived=archived)
    return [Device(item) for item in payload]


search_objects = search_devices


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


def context() -> dict[str, Any]:
    """Ask the server what it thinks the context stack is (a consistency check)."""
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
