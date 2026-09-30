"""Balthazar Tunnel — host side. UTILITY FLOW (long-running; exclude from pipelines).

Serves JSON-RPC on 127.0.0.1 from inside a live Balthazar flow run and translates
each request into a real ``blt.*`` call. Together with the local ``balthazar.py``
shim this lets a script in VS Code — with breakpoints, in a debugger — read data
out of Balthazar and write flow runs, plots and output back into it.

Run this flow in Balthazar and leave it running. It writes an 0600 connection
file to ~/.balthazar_tunnel.json that the local shim reads for URL and token.

Flow parameters
---------------
port : int (default 8765)   Local port to listen on.

ARCHITECTURE — why the job queue exists.
``enter_new_flow_run`` and its matching exit assert ``is_main_thread`` in the
Runner's Rust source, and raise ValueError otherwise. HTTP requests are served on
worker threads, so those threads must never touch ``blt.*`` directly. Instead:

    worker thread (HTTP)  --job-->  queue  -->  main thread executes blt.*
                          <-reply--  queue  <--

The main thread runs the executor loop; ``serve_forever`` runs on a background
thread. Every operation is therefore executed serially on the main thread. That
also protects the module-level context globals (``blt.params``, ``blt.output``,
``blt.devices``), which a concurrent child-run context would corrupt.

HOW PLOTS REACH A NEW FLOW RUN.
``blt.store_visualizations([VisualizationBuilder(...)])`` — the same call the
Runner's own matplotlib backend makes on ``plt.show()`` — attaches to whatever
flow run is current and returns the stored ``VisualizationMeta`` list. So a plot
can only land on a new run while that run is current, which means entering its
context. The client renders figures to SVG locally and ships the bytes; this flow
stores them inside an ``enter_new_flow_run`` block.

Module-level names are bound methods of ``blt.context``, which the Runner rewrites
in place when a run is entered, so ``blt.store_visualizations`` inside the block
still targets the child run. Before Runner 1.35.1 this call lived on ``blt.api``,
which no longer exists.

SECURITY. The tunnel grants full read access to the space's devices and can
create flow runs, so it binds 127.0.0.1 only, requires a per-session bearer
token, and rejects non-loopback Host headers to blunt DNS rebinding. Secrets are
deliberately not exposed — do not add a ``blt.secrets`` operation.
"""

import base64
import hmac
import json
import os
import queue
import secrets as _secrets
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import balthazar as blt

# Fail loudly if we picked up the local shim instead of the Runner's builtin
# module — otherwise the tunnel would forward requests to itself.
if getattr(blt, "__balthazar_tunnel__", False):
    raise RuntimeError(
        "Imported the tunnel shim, not the real balthazar module. This flow must "
        "run on a Balthazar Runner."
    )

CONNECTION_FILE = os.path.expanduser("~/.balthazar_tunnel.json")
_MAX_BODY_BYTES = 32 * 1024 * 1024  # SVG payloads for plot-bearing runs
_JOB_TIMEOUT_S = 120.0

_TOKEN = _secrets.token_urlsafe(32)
_JOBS: "queue.Queue[_Job]" = queue.Queue()
_stop = threading.Event()


class _Job:
    """One RPC awaiting execution on the main thread."""

    __slots__ = ("op", "kwargs", "reply")

    def __init__(self, op, kwargs):
        self.op = op
        self.kwargs = kwargs
        self.reply: "queue.Queue[tuple[bool, object]]" = queue.Queue(maxsize=1)


class _FailedRun(Exception):
    """Raised inside a child flow-run context to mark that run FAILED."""


# ----------------------------------------------------------------------------
# Serialization helpers
# ----------------------------------------------------------------------------


def _params_to_dict(params):
    """Copy a device's param proxy into a plain dict.

    ``device.params`` is a Rust-backed proxy (``DeviceParameterDict``), not a
    dict, so fall back through a few mapping protocols rather than assuming one.
    """
    for attempt in (
        lambda: dict(params),
        lambda: {k: params[k] for k in params.keys()},
        lambda: {k: v for k, v in params.items()},
    ):
        try:
            return attempt()
        except Exception:  # noqa: BLE001 - probing which protocol the proxy supports
            continue
    blt.warn("Could not serialize device params; returning empty dict")
    return {}


def _device_to_dict(device):
    fab = getattr(device, "fabrication_date", None)
    return {
        "id": device.id,
        "name": device.name,
        "type": device.type,
        "description": getattr(device, "description", None),
        "fabrication_date": fab.isoformat() if hasattr(fab, "isoformat") else fab,
        "tags": list(getattr(device, "tags", []) or []),
        "params": _params_to_dict(device.params),
    }


def _resolve_devices(device_ids):
    """Turn client-supplied device IDs back into real Device objects."""
    if not device_ids:
        return []
    found = blt.search_devices(id=list(device_ids))
    by_id = {d.id: d for d in found}
    missing = [i for i in device_ids if i not in by_id]
    if missing:
        raise ValueError(f"Unknown device id(s): {', '.join(missing)}")
    return [by_id[i] for i in device_ids]


def _decode_visualizations(visualizations):
    """Turn the client's payload into ``blt.VisualizationBuilder`` objects.

    Since Runner 1.35.1 ``blt.store_visualizations`` takes builders, not the
    ``(figure_id, filename, bytes)`` tuples the removed ``blt.api`` accepted.

    The client's ``id`` is its local matplotlib figure number, which maps onto
    ``figure_id``: storing again under the same id *replaces* the previous
    visualization, so a redrawn figure leaves no intermediate frames behind. That
    is what we want, but it also means the batch is rejected if two items share an
    id — checked here, where the offending file can still be named.
    """
    decoded = []
    seen = set()
    for index, item in enumerate(visualizations or [], start=1):
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
    """Log the notebook cell that produced this run, into that run's own logs.

    Only meaningful inside an entered context — a history entry made by
    ``blt.new_flow_run`` has no log stream, which is why the client routes runs
    that carry a cell source through ``create_flow_run`` instead.

    Truncated server-side as well as client-side: the body limit is 32 MB and an
    arbitrary client could otherwise bury a run's log under one paste.
    """
    text = str(source).strip()
    if len(text) > _MAX_CELL_CHARS:
        text = f"{text[:_MAX_CELL_CHARS]}\n... [truncated, {len(text)} chars]"
    blt.info(f"[tunnel] cell source:\n{text}")


# ----------------------------------------------------------------------------
# Operations — all executed on the main thread
# ----------------------------------------------------------------------------


def _op_ping(_kwargs):
    return {
        "flow_name": blt.flow.name,
        "flow_id": blt.flow.id,
        "session_id": blt.session.id,
        "flow_run_id": blt.flow_run.id,
        "device_count": len(blt.devices),
    }


def _op_search_devices(kwargs):
    call = {}
    for key in ("id", "type", "name", "tags", "archived"):
        if kwargs.get(key) is not None:
            call[key] = kwargs[key]
    for key in ("limit", "offset"):
        if kwargs.get(key):
            call[key] = int(kwargs[key])
    return [_device_to_dict(d) for d in blt.search_devices(**call)]


def _op_get_device_params(kwargs):
    device_id = kwargs.get("id")
    if not device_id:
        raise ValueError("'id' is required")
    matches = blt.search_devices(id=device_id)
    if not matches:
        raise FileNotFoundError(f"No device with id {device_id}")
    return _params_to_dict(matches[0].params)


def _op_new_flow_run(kwargs):
    """Create a completed flow-run history entry. No plots (see _op_create_flow_run)."""
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
    if kwargs.get("script_name") is not None:
        call["script_name"] = kwargs["script_name"]
    return blt.new_flow_run(**{k: v for k, v in call.items() if v is not None})


def _op_create_flow_run(kwargs):
    """Create a flow run by entering its context, so plots and output attach to it.

    Main-thread only — which is why every op is funnelled through the executor.
    ``devices`` and ``parameters`` are always passed explicitly: omitting them
    makes the child inherit the tunnel flow's own devices and params, which is
    almost never what the caller wants.

    Every step inside the context catches its own errors. The Runner's context
    ``__exit__`` reports FAILED for *any* exception in flight, so an unguarded
    failure in one sub-step would turn the whole run red with no explanation
    beyond the exception string. Instead each problem is logged into the run and
    returned to the client, and the run stays FINISHED unless the caller asked
    for FAILED.
    """
    visualizations = _decode_visualizations(kwargs.get("visualizations"))
    cell_source = kwargs.get("cell_source")
    output = kwargs.get("output") or {}
    parameters = kwargs.get("parameters") or {}
    want_failed = (kwargs.get("status") or "FINISHED").upper() == "FAILED"
    error_message = kwargs.get("error_message") or "marked FAILED by tunnel client"

    child_id = None
    stored = 0
    stored_ids = []
    problems = []

    def _guarded(label, fn):
        """Run fn, absorbing any error so it never reaches the context __exit__."""
        try:
            fn()
            return True
        except Exception as exc:  # noqa: BLE001 - must not escape into __exit__
            detail = f"{label}: {type(exc).__name__}: {exc}"
            problems.append(detail)
            blt.error(f"[tunnel] {detail}")
            return False

    try:
        with blt.enter_new_flow_run(
            name=kwargs.get("name"),
            script_name=kwargs.get("script_name"),
            flow_id=kwargs.get("flow_id") or blt.flow.id,
            devices=_resolve_devices(kwargs.get("device_ids")),
            parameters=parameters,
        ):
            child_id = blt.flow_run.id

            # First, so the run reads top-down: the code, then what it produced.
            if cell_source:
                _guarded("log_cell_source", lambda: _log_cell_source(cell_source))

            def _store():
                # store_visualizations returns the stored VisualizationMeta list,
                # so the client gets real IDs back instead of just a count.
                stored_ids.extend(v.id for v in blt.store_visualizations(visualizations))

            if visualizations and _guarded("store_visualizations", _store):
                stored = len(stored_ids)

            if output:
                _guarded("output.update", lambda: blt.output.update(output))

            if want_failed:
                # The documented way to land a child run in FAILED: raise inside
                # the context and swallow it outside.
                raise _FailedRun(error_message)
    except _FailedRun:
        pass

    return {
        "flow_run_id": child_id,
        "visualizations_stored": stored,
        "visualization_ids": stored_ids,
        "status": "FAILED" if want_failed else "FINISHED",
        "problems": problems,
    }


def _op_log(kwargs):
    level = (kwargs.get("level") or "info").lower()
    message = f"[tunnel] {kwargs.get('message', '')}"
    {"info": blt.info, "warn": blt.warn, "error": blt.error}.get(level, blt.info)(message)
    return True


_DISPATCH = {
    "ping": _op_ping,
    "search_devices": _op_search_devices,
    "get_device_params": _op_get_device_params,
    "new_flow_run": _op_new_flow_run,
    "create_flow_run": _op_create_flow_run,
    "log": _op_log,
}


# ----------------------------------------------------------------------------
# HTTP layer — worker threads only; never calls blt.* directly
# ----------------------------------------------------------------------------


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):  # noqa: A003 - BaseHTTPRequestHandler hook
        pass  # blt.* is main-thread-only here; HTTP noise is not worth a job

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
        host = (self.headers.get("Host") or "").split(":")[0]
        if host not in ("127.0.0.1", "localhost", ""):
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

        if op not in _DISPATCH:
            self._error(400, "ValueError", f"unknown op {op!r}")
            return

        job = _Job(op, request.get("kwargs") or {})
        _JOBS.put(job)
        try:
            ok, payload = job.reply.get(timeout=_JOB_TIMEOUT_S)
        except queue.Empty:
            self._error(504, "TimeoutError", f"op {op!r} did not complete in {_JOB_TIMEOUT_S}s")
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
    """Route SIGTERM through the same path as an interrupt so cleanup runs.

    Without this, SIGTERM kills the process outright, the ``finally`` block never
    runs, and a stale connection file is left behind pointing at a dead port.
    """
    raise KeyboardInterrupt


def _run_executor(stats):
    """Drain the job queue on the main thread until stopped."""
    while not _stop.is_set():
        try:
            job = _JOBS.get(timeout=0.25)
        except queue.Empty:
            continue

        stats["requests_served"] += 1
        try:
            result = _DISPATCH[job.op](job.kwargs)
            job.reply.put((True, result))
            if job.op in ("new_flow_run", "create_flow_run"):
                stats["flow_runs_created"] += 1
            if isinstance(result, dict):
                stats["visualizations_stored"] += result.get("visualizations_stored", 0)
        except Exception as exc:  # noqa: BLE001 - marshal any failure to the client
            stats["errors"] += 1
            blt.warn(f"tunnel op {job.op!r} failed: {type(exc).__name__}: {exc}")
            job.reply.put((False, {"type": type(exc).__name__, "message": str(exc)}))

        # Publish progress periodically rather than per request — each output
        # write is a round-trip to the server.
        if stats["requests_served"] % 10 == 0:
            blt.output.update(dict(stats))


def tunnel_server_flow():
    port = int(blt.params.get("port", 8765))
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
    blt.info(f"Tunnel listening on {url}, credentials in {CONNECTION_FILE}")
    blt.info(f"Operations: {', '.join(sorted(_DISPATCH))}")

    stats = {
        "requests_served": 0,
        "flow_runs_created": 0,
        "visualizations_stored": 0,
        "errors": 0,
    }
    blt.output.update({"tunnel_url": url, "port": port, "status": "running", **stats})

    try:
        # Blocks until the flow run is killed. Flow interrupts arrive as
        # KeyboardInterrupt; catching it here is safe because this is the top
        # level of the flow, not inside a child flow-run context.
        _run_executor(stats)
    except KeyboardInterrupt:
        blt.info("Tunnel interrupted, shutting down")
    finally:
        _stop.set()
        httpd.shutdown()
        httpd.server_close()
        try:
            os.remove(CONNECTION_FILE)
        except OSError:
            pass
        blt.output.update({"status": "stopped", **stats})
        blt.info(
            f"Tunnel stopped — {stats['requests_served']} request(s), "
            f"{stats['flow_runs_created']} flow run(s), "
            f"{stats['visualizations_stored']} plot(s), {stats['errors']} error(s)"
        )


if __name__ == "__main__":
    tunnel_server_flow()
