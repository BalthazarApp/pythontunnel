"""Balthazar Tunnel — client side.

Drop-in local stand-in for the Runner-injected ``balthazar`` module. Mimics the
``blt.*`` API surface, but every call is forwarded over HTTP to a tunnel server
running inside a live Balthazar flow run (see ``flows/tunnel_server.py``), which
executes it against the real module.

Usage from any local script (VS Code, debugger, pytest)::

    import balthazar as blt
    device = blt.search_devices()[0]
    print(dict(device.params))

Why naming this file ``balthazar`` is safe: the Runner registers its module via
``pyo3::append_to_inittab!``, making it a *builtin*. CPython consults
``BuiltinImporter`` before ``PathFinder`` in ``sys.meta_path``, so on the Runner
the real module always wins over this file, even if it sits in the cwd or venv.
Locally, where no builtin exists, this file is found instead.

Scope:
  * read a device (``search_devices`` + ``device.params``)
  * create a flow run, optionally carrying matplotlib figures (``new_flow_run``)
Device-param *writes* are deliberately not implemented; see ``_ReadOnlyParams``.
"""

from __future__ import annotations

import base64
import io
import json
import os
import sys
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any, Optional

__all__ = [
    "search_devices",
    "search_objects",
    "new_flow_run",
    "log_cell_source",
    "Device",
    "info",
    "warn",
    "error",
    "ping",
    "TunnelError",
]

# Marker so the host-side flow can detect it accidentally imported the shim
# instead of the real module (which would make the tunnel call itself).
__balthazar_tunnel__ = True

CONNECTION_FILE = os.path.expanduser("~/.balthazar_tunnel.json")
_TIMEOUT_S = 120.0  # generous: plot-bearing runs ship SVG payloads


class TunnelError(RuntimeError):
    """The tunnel itself failed (not reachable, bad token, malformed reply)."""


# Map marshalled error type names back onto real Python exceptions, so local
# code can catch what it would catch when running for real on the Runner.
_ERROR_TYPES: dict[str, type[BaseException]] = {
    "KeyError": KeyError,
    "ValueError": ValueError,
    "TypeError": TypeError,
    "FileNotFoundError": FileNotFoundError,
    "PermissionError": PermissionError,
    "NotImplementedError": NotImplementedError,
}


def _connection() -> tuple[str, str]:
    """Resolve the tunnel URL and token, env vars taking priority."""
    url = os.environ.get("BALTHAZAR_TUNNEL_URL")
    token = os.environ.get("BALTHAZAR_TUNNEL_TOKEN")
    if url and token:
        return url, token

    try:
        with open(CONNECTION_FILE, encoding="utf-8") as fh:
            conf = json.load(fh)
    except FileNotFoundError:
        raise TunnelError(
            f"No tunnel connection file at {CONNECTION_FILE}. Start the "
            "'Balthazar Tunnel' flow (flows/tunnel_server.py) in Balthazar first."
        ) from None
    except (OSError, ValueError) as exc:
        raise TunnelError(f"Unreadable connection file {CONNECTION_FILE}: {exc}") from exc

    return url or conf["url"], token or conf["token"]


def _call(op: str, **kwargs: Any) -> Any:
    """Send one RPC to the tunnel and return its result, re-raising remote errors."""
    url, token = _connection()
    body = json.dumps({"op": op, "kwargs": kwargs}).encode("utf-8")
    req = urllib.request.Request(
        f"{url}/rpc",
        data=body,
        method="POST",
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
    exc_type = _ERROR_TYPES.get(err.get("type", ""), TunnelError)
    raise exc_type(err.get("message") or "unknown remote error")


class _ReadOnlyParams(Mapping):
    """A snapshot of ``device.params``, fetched on first access.

    Read-only on purpose. Writing device params through the tunnel needs a
    decision this PoC hasn't made: whether to faithfully reproduce the real
    proxy's quirks (subscript assignment not syncing, ``pop(k, default)``
    raising ``KeyError``) or paper over them. Reproducing them is the safer
    choice, since code written against a friendlier shim would break once
    deployed as a real flow.
    """

    def __init__(self, device_id: str, initial: Optional[dict[str, Any]] = None):
        self._device_id = device_id
        self._cache = initial

    def _data(self) -> dict[str, Any]:
        if self._cache is None:
            self._cache = _call("get_device_params", id=self._device_id)
        return self._cache

    def refresh(self) -> "_ReadOnlyParams":
        """Discard the cached snapshot and re-fetch on next access."""
        self._cache = None
        return self

    def __getitem__(self, key: str) -> Any:
        return self._data()[key]

    def __iter__(self):
        return iter(self._data())

    def __len__(self) -> int:
        return len(self._data())

    def __repr__(self) -> str:
        return f"{self._data()!r}"

    def _readonly(self, *_args: Any, **_kwargs: Any):
        raise NotImplementedError(
            "Writing device.params through the tunnel is not implemented in this "
            "PoC. Add a 'update_device_params' op to flows/tunnel_server.py, and "
            "decide first whether the shim should reproduce the real proxy's "
            "sync quirks."
        )

    __setitem__ = _readonly
    __delitem__ = _readonly
    update = _readonly
    clear = _readonly
    pop = _readonly


class Device:
    """Local stand-in for a remote ``balthazar.Device``."""

    def __init__(self, payload: dict[str, Any]):
        self.id: str = payload["id"]
        self.name: str = payload.get("name", "")
        self.type: str = payload.get("type", "device")
        self.description: Optional[str] = payload.get("description")
        self.fabrication_date: Optional[str] = payload.get("fabrication_date")
        self.tags: list[str] = payload.get("tags") or []
        self.params = _ReadOnlyParams(self.id, payload.get("params"))

    def __repr__(self) -> str:
        return f"<Device {self.name!r} type={self.type!r} id={self.id}>"


def ping() -> dict[str, Any]:
    """Return the tunnel's identity: flow, session and flow-run IDs."""
    return _call("ping")


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
    payload = _call(
        "search_devices",
        id=id,
        type=type,
        name=name,
        tags=tags,
        limit=limit,
        offset=offset,
        archived=archived,
    )
    return [Device(item) for item in payload]


search_objects = search_devices


log_cell_source = True
"""Send the notebook cell that created a run into that run's Balthazar logs.

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
    ``_take_cell_source`` simply keeps returning None — demo.py behaves exactly
    as it did before.
    """
    try:
        from IPython import get_ipython
    except ImportError:
        return False
    ip = get_ipython()
    if ip is None:  # imported inside IPython's own process but not a shell
        return False
    ip.events.register("pre_run_cell", _capture_cell)
    return True


_cell_hook_installed = _install_cell_hook()


def _take_cell_source() -> Optional[str]:
    """The current cell's code, trimmed for transport, or None outside a notebook.

    Sent on every run rather than once per cell: v1 runs are independent siblings
    under the tunnel run, so each one should be able to explain itself.
    """
    if not log_cell_source or not _cell_source:
        return None
    text = _cell_source.strip()
    if len(text) > _MAX_CELL_CHARS:
        text = f"{text[:_MAX_CELL_CHARS]}\n... [truncated, {len(text)} chars]"
    return text or None


def _render_figures(figures: Any) -> list[dict[str, Any]]:
    """Render matplotlib figures to SVG and base64-encode them for transport.

    Shipping *rendered SVG* rather than a pickled figure is deliberate: it keeps
    the payload independent of the matplotlib version on the Runner, and it is
    exactly the format the Runner's own backend produces on ``plt.show()``.

    ``figures="all"`` grabs every currently-open figure, mirroring what
    ``plt.show()`` does. Otherwise pass a Figure or a list of them.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError as exc:  # pragma: no cover - depends on the local env
        raise TunnelError(
            "matplotlib is required to send figures through the tunnel"
        ) from exc

    if figures == "all":
        figures = [plt.figure(num) for num in plt.get_fignums()]
    elif not isinstance(figures, (list, tuple)):
        figures = [figures]

    payload = []
    for index, fig in enumerate(figures, start=1):
        buf = io.BytesIO()
        # savefig (not plt.show) is correct here: this is the client, not a flow.
        # There is no Runner backend to hook, so we render the bytes ourselves.
        fig.savefig(buf, format="svg", bbox_inches="tight")
        label = getattr(fig, "label", "") or f"figure_{index}"
        payload.append({
            # No figure_id. Since Runner 1.35.1 it is a *replace* key: a second
            # store under the same id overwrites the first and the batch is
            # rejected outright if two items share one. Every new_flow_run(figures=)
            # call here creates a fresh run, so there is nothing to redraw over,
            # and manufacturing ids from fig.number would collide the moment a
            # bare Figure() (no .number) sat next to a pyplot figure numbered the
            # same as its list position. Omitting it stores each plot on its own.
            "filename": f"{label}.svg",
            "svg_base64": base64.b64encode(buf.getvalue()).decode("ascii"),
        })
    return payload


def new_flow_run(
    name: Optional[str] = None,
    *,
    script_name: Optional[str] = None,
    flow_id: Optional[str] = None,
    devices: Optional[list[Device]] = None,
    output: Optional[dict[str, Any]] = None,
    parameters: Optional[dict[str, Any]] = None,
    error_message: Optional[str] = None,
    status: str = "FINISHED",
    figures: Any = None,
) -> str:
    """Create a flow run in Balthazar and return its ID.

    ``figures`` attaches plots to the new run: a matplotlib Figure, a list of
    them, or the string ``"all"`` for every open figure. They are rendered to SVG
    locally and stored against the new run by the tunnel.

    ``status`` must be ``"FINISHED"`` or ``"FAILED"``. ``devices`` are sent as IDs
    and re-resolved server-side.

    Two server-side paths, because plots and logs constrain how the run must be
    made. A bare run is one ``blt.new_flow_run`` call, which writes a completed
    history entry. But a history entry has no log stream and is not current, so
    anything that must be *written into* the run — visualizations, or the cell
    source when running under Jupyter — forces the tunnel to actually enter the
    new run's context via ``enter_new_flow_run``. Same visible result; different
    call underneath.
    """
    device_ids = [d.id for d in devices] if devices else None
    cell_source = _take_cell_source()

    if figures is None and cell_source is None:
        return _call(
            "new_flow_run",
            name=name,
            script_name=script_name,
            flow_id=flow_id,
            device_ids=device_ids,
            output=output,
            parameters=parameters,
            error_message=error_message,
            status=status,
        )

    result = _call(
        "create_flow_run",
        name=name,
        script_name=script_name,
        flow_id=flow_id,
        device_ids=device_ids,
        output=output,
        parameters=parameters,
        error_message=error_message,
        status=status,
        visualizations=_render_figures(figures) if figures is not None else None,
        cell_source=cell_source,
    )
    # The run is created either way; surface partial failures rather than letting
    # a plot or output silently go missing.
    for problem in result.get("problems") or []:
        print(f"tunnel warning: {problem}", file=sys.stderr)
    return result["flow_run_id"]


def info(message: str) -> None:
    """Log at info level into the tunnel's flow run."""
    _call("log", level="info", message=str(message))


def warn(message: str) -> None:
    """Log at warn level into the tunnel's flow run."""
    _call("log", level="warn", message=str(message))


def error(message: str) -> None:
    """Log at error level into the tunnel's flow run."""
    _call("log", level="error", message=str(message))
