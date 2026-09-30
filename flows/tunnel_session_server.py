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
port         : int (default 8766)   Local port to listen on.
idle_timeout : int (default 1800)   Seconds of client silence before open contexts
                                    are unwound. Generous by default because a
                                    breakpoint inside a ``with`` block stops the
                                    client from sending anything.

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
"""

import base64
import hmac
import json
import os
import queue
import secrets as _secrets
import signal
import threading
import time
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
    "contexts_reclaimed": 0,
    "errors": 0,
}


class _Job:
    __slots__ = ("op", "kwargs", "reply")

    def __init__(self, op, kwargs):
        self.op = op
        self.kwargs = kwargs
        self.reply: "queue.Queue[tuple[bool, object]]" = queue.Queue(maxsize=1)


class _TunnelRunFailed(Exception):
    """Synthesized at exit so the Runner records the client's error message."""


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
    return {**_context_snapshot(), "idle_timeout_s": _idle_timeout}


def _last_seen_touch(kwargs):
    global _last_seen
    if kwargs.get("client_id") and kwargs["client_id"] == _owner:
        _last_seen = time.monotonic()


def _op_heartbeat(kwargs):
    _last_seen_touch(kwargs)
    return {"depth": len(_stack), "owner": _owner}


def _op_search_devices(kwargs):
    _last_seen_touch(kwargs)
    call = {}
    for key in ("id", "type", "name", "tags", "archived"):
        if kwargs.get(key) is not None:
            call[key] = kwargs[key]
    for key in ("limit", "offset"):
        if kwargs.get(key):
            call[key] = int(kwargs[key])
    return [_device_to_dict(d) for d in blt.search_devices(**call)]


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
    "get_device_params": _op_get_device_params,
    "enter_flow_run": _op_enter_flow_run,
    "exit_flow_run": _op_exit_flow_run,
    "store_visualizations": _op_store_visualizations,
    "set_output": _op_set_output,
    "update_device_params": _op_update_device_params,
    "new_flow_run": _op_new_flow_run,
    "reset_contexts": _op_reset_contexts,
    "log": _op_log,
}


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
        if op not in _DISPATCH:
            self._error(400, "ValueError", f"unknown op {op!r}")
            return

        job = _Job(op, request.get("kwargs") or {})
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


def _run_executor():
    """Drain jobs on the main thread; police idle contexts between jobs."""
    while not _stop.is_set():
        try:
            job = _JOBS.get(timeout=0.25)
        except queue.Empty:
            if _stack and (time.monotonic() - _last_seen) > _idle_timeout:
                idle = time.monotonic() - _last_seen
                blt.warn(
                    f"[tunnel] client silent for {idle:.0f}s with {len(_stack)} open "
                    f"context(s); unwinding so the runs do not hang in RUNNING"
                )
                reclaimed = len(_stack)
                _unwind_all(f"context abandoned: client silent for {idle:.0f}s")
                _stats["contexts_reclaimed"] += reclaimed
            continue

        _stats["requests_served"] += 1
        try:
            result = _DISPATCH[job.op](job.kwargs)
            job.reply.put((True, result))
        except Exception as exc:  # noqa: BLE001 - marshal every failure to the client
            _stats["errors"] += 1
            blt.warn(f"tunnel op {job.op!r} failed: {type(exc).__name__}: {exc}")
            job.reply.put((False, {"type": type(exc).__name__, "message": str(exc)}))

        if _stats["requests_served"] % 20 == 0:
            blt.output.update(dict(_stats)) if not _stack else None


def tunnel_session_server_flow():
    global _idle_timeout
    port = int(blt.params.get("port", 8766))
    _idle_timeout = float(blt.params.get("idle_timeout", 1800))

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
    blt.info(f"Idle timeout {_idle_timeout:.0f}s · operations: {', '.join(sorted(_DISPATCH))}")
    blt.output.update({
        "tunnel_url": url, "port": port, "status": "running",
        "idle_timeout_s": _idle_timeout, **_stats,
    })

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
