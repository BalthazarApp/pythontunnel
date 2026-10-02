# Bridge: interface spec

The transport is a **generic reflection bridge** over the Balthazar app tunnel. The bridge
uses `new_flow_run_context` (Runner >= 1.35.1). It has no main-thread job queue, no
single-owner lock, and one transport, the Balthazar app tunnel.

## Layout

```
flows/tunnel_bridge.py         the server flow: one file, stdlib-only; imports blt_analytics.digest
                               only for space_schema
bridge/balthazar_remote.py     the client (Remote): one file, stdlib-only
bridge/balthazar.py            drop-in module: `import balthazar as blt` connects from the profile
                               and delegates to the Remote
blt_analytics/…                gets the blt module from the bridge first, using the tunnel namespace
tests/bridge/…                 bridge tests (fakes: reuse tests/fakes/fake_blt.py, extended)
```

## Protocol (POST `/call`, JSON; GET `/` serves the snippet page)

The protocol has these ops: `describe`, `call`, `get`, `set`, `getitem`, `contains`,
`setitem`, `delitem`, `enter`, `exit`, plus the value tags `$obj` / `$ref` / `$attr` / `$new` /
… . The details below are the contract the server and client both follow exactly.

1. **`describe`** returns:
   - `protocol: 3`
   - `bridge_version: "3.0.0"`
   - `user`: the caller id
   - `owner`: `blt.user`
   - `shared: bool`
   - `tunnel: [names of tunnel functions]`
   - `device_indexes: {name: path}`
   - `idle_timeout_s`, `call_timeout_s`

   The client refuses to connect if `protocol != 3`, with a one-sentence error naming both
   protocol numbers.
2. **Long calls.** The server runs every non-`describe` request in a worker thread and waits
   up to `call_timeout_s` (default 45). If the request isn't done by then, it replies
   `{"ok": true, "pending": "<job id>"}`. The client then sends `{"op": "poll", "job": id}`
   until it gets the normal reply; a poll waits up to `call_timeout_s` itself. Jobs belong to
   their caller (another caller gets 404-style `LookupError`), and finished, unpolled jobs
   expire after 10 min.
3. **Refs are scoped per caller.** Every caller has its own LRU store (`max_refs` per caller,
   default 50_000). `recall` checks the caller; another caller's ref raises `LookupError`.
   New op `{"op": "release", "refs": [ids]}` drops refs. The client batches releases from
   weakref finalizers and sends them every 5 s or when 200 are pending.
4. **Heartbeat:** `{"op": "heartbeat"}` updates the caller's `last_seen`. Every request
   counts as activity.
5. **Watchdog.** The server remembers the contexts each caller entered via `enter`, by ref.
   If a caller is silent for `idle_timeout_s` (default 900), the server calls
   `__exit__(RuntimeError, RuntimeError("bridge client went silent for Ns"), None)` on each of
   them (innermost first), logs it, and drops that caller's refs. The client sends a heartbeat
   every 30 s from a daemon thread.
   - Known limitation, documented: a debugger pause freezes the heartbeat thread. A detached
     heartbeat sidecar process (as in balthazar-tunnel) is a later improvement.
6. **Tunnel namespace:** `{"op": "tunnel", "name": "<fn>", "args": [...], "kwargs": {...}}`.
   Results are encoded with the normal encoder. The functions are listed below.
7. **Hardening:**
   - `set` with `ref == null` (on the module itself) is forbidden.
   - Values that are `types.ModuleType` are never returned or traversed; `resolve` refuses
     them.
   - `HIDDEN = {"serve_app", "prompt_input", "enter_new_flow_run", "secrets", "context"}`.
     `secrets` is reachable only with flow param `expose_secrets=true`.
   - The audit log uses a reference to `blt.info` captured at import, so it can't be patched.
   - Error replies carry `traceback` only for the owner. Other callers in shared mode get
     type and message only.
8. **Snapshots** re-encode `"self"` after ref-targeted ops (keep that; optimizing it is out
   of scope).

## Flow parameters (`blt.params`, all optional)

| param | default | meaning |
|---|---|---|
| `shared` | `false` | allow every caller the platform lets through; otherwise owner + `allowed_users` |
| `allowed_users` | `""` | comma-separated user ids, in addition to the owner |
| `idle_timeout` | `900` | watchdog seconds |
| `call_timeout` | `45` | seconds before a call turns into a pending job |
| `max_refs` | `50000` | per caller |
| `expose_secrets` | `false` | make `blt.secrets` reachable |
| `device_indexes` | `"{}"` | JSON `{name: "dotted.path"}` or `{name: {"path": …, "device_type": …}}` |
| `warm_device_cache` | `true` if indexes are configured | start loading the cache at start |
| `device_cache_dir` | `~/.balthazar_tunnel_cache` | on the Runner; files 0600, dir 0700 |

Auth:
- `X-BLT-User-Id` is required;
- the owner is `blt.user` (case-insensitive);
- browser POSTs (`Origin` or `Sec-Fetch-Site` present) are rejected;
- `GET /` is visible to allowed users.

In shared mode, an audit line is written per call.

## Tunnel functions (server side, in `flows/tunnel_bridge.py`)

Without a main-thread queue everything just calls `blt.*` from threads.

| name | signature | returns |
|---|---|---|
| `device_cache_status` | `()` | `{state: empty\|loading\|ready\|error, count, loaded, built_at, indexes, error}` |
| `refresh_device_cache` | `(wait=False)` | status. Loads in a background thread; the cache is swapped atomically on success, and the old one is kept on failure. |
| `cached_devices` | `(index, value, *, refresh=False, offset=0, max_bytes=2_000_000)` | `{state, devices: [record], next_offset, total}`. While the cold cache loads: `{state: "loading", …}`. `refresh=True` re-fetches that value's known ids in chunks of 500. |
| `cached_devices_query` | `(device_type=None, keys=None, offset=0, max_bytes=2_000_000)` | `{devices: [record], total, next_offset}` |
| `space_schema` | `(refresh=False)` | `{state: empty\|building\|ready\|error, progress, digest?}`. Builds in the background via `blt_analytics.digest` (repo root appended to `sys.path`; on import failure, `state: "error"` with a clear message). Uses the device cache when it is ready. |

- A record is plain JSON: `{id, name, type, description, fabrication_date, tags, params}`.
- **Persistence:** pickle of the records plus a JSON meta file, keyed by `blt.flow.id`.
- **Write-through:** after any successful `set`/`setitem`/`delitem`/`call` whose root object has an `id`
  in the cache and a `params` attribute, refresh that cache record from the live object.

## Client (`bridge/balthazar_remote.py`)

The client exposes `connect(app_url, site=None, login="device", …)` with `_find_site` and the
OAuth login code, plus:

1. **`interactive=True` parameter.** When `False`, a missing or invalid token raises
   `LoginRequired(BridgeError)` ("run `blt-tunnel connect` in a terminal") instead of
   prompting. The environment variable `BALTHAZAR_TUNNEL_NONINTERACTIVE=1` forces `False`.
2. **Protocol check** on `describe`.
3. **Polling** of `pending` replies, with a one-line stderr notice after 10 s.
4. **Heartbeat** daemon thread (30 s) and **release** batching via `weakref.finalize` on
   RemoteObject, RemoteDict and RemoteList.
5. **Profile:**
   - `save_profile(app_url, login, site=None, ca_file=None)` writes `~/.balthazar_bridge.json`
     (0600). It never contains tokens; those stay in the existing token cache.
   - `connect_from_profile(interactive=True)`.
   - The environment variable `BALTHAZAR_BRIDGE_URL` overrides the profile's `app_url`.
6. **Run emulation, so flow code runs unchanged.**
   - `enter_new_flow_run(name=None, devices=None, parameters=None, **kw)` returns a context
     manager that calls the remote `new_flow_run_context(...)`, `enter`s it, and pushes it on
     a client-side stack.
   - While the stack is non-empty, these names on the Remote resolve to the innermost
     context's own attribute or method instead of the module's:
     `output`, `params`, `devices`, `flow_run`, `flow`, `session`, `info`, `warn`, `error`,
     `debug`, `print`, `search_devices`, `new_devices`, `store_visualization(s)`,
     `upload_visualization(s)`, `new_flow_run`, `parent`, `parents`.
     Routing `search_devices` through the context is what keeps device writes attributed to
     the child run.
   - An exception leaving the block exits the context with that error (run FAILED) and
     re-raises.
   - **Known Runner limitation:** contexts can only be created as children of the bridge run,
     so nested blocks become siblings, and `parent()` is the bridge run. Document this; don't
     fake it.
7. **`plt.show()` capture.** When matplotlib is already imported or gets imported, wrap
   `plt.show`. Render each open figure whose SVG hash changed to SVG and upload it via the
   innermost context's `store_visualizations([VisualizationBuilder(filename, data,
   type=SVG, figure_id=num)])` (an int, as the Runner stub types it), built with `$new` deferreds in one request, falling back
   to the module (the bridge run) when no context is open. Then call the real `show`, so
   inline notebook rendering keeps working.
8. **Notebook cell source.** An IPython `pre_run_cell` hook stores the source (capped at
   8000 chars); `enter_new_flow_run` logs it via the context's `info` right after entering.
   It can be switched off with `remote.log_cell_source = False`.
9. **`blt.output` primitives check:**
   - assigning a dict, date or DataFrame to output raises `TypeError` client-side;
   - flat lists of primitives are allowed.
10. **`isinstance` support.** Class symbols (`blt.Device`, `blt.Flow`, …) are real Python
    classes, made with a metaclass whose `__instancecheck__` matches the
    RemoteObject/RemoteDict/RemoteList `_cls` name. Calling them still produces a
    `_Deferred`, and the `_Symbol` behaviour for constants (`blt.FlowRunStatus.FINISHED`) is
    unchanged.
11. **`tunnel` attribute.** `blt.tunnel.<fn>(...)` sends the tunnel op and follows
    `next_offset` pages transparently for `cached_devices` / `cached_devices_query`, returning
    plain lists.
    - `cached_devices` and `get_<index>_devices` return `CachedDevice` read-only objects
      (`id`, `name`, `type`, `description`, `fabrication_date` as datetime, `tags`, `params`
      as dict) with `.live()`, which fetches the real remote Device
      (`search_devices(id=[…])`) for writes.
    - `blt.get_<index>_devices(value, *, refresh=False)` is generated from `device_indexes`
      in `describe`.
    - Long tunnel calls (`state` loading or building) are polled every 2 s until ready, with
      a notice.
12. Remote exceptions use the mapping `RemoteError` + builtin subclass + `remote_traceback`.

## `bridge/balthazar.py` (drop-in)

This is a module whose `__getattr__` (PEP 562) lazily calls `connect_from_profile()` once
and delegates every attribute to that Remote. It exposes `__balthazar_tunnel__ = 3` as a
marker. Running from `bridge/`, or with `bridge/` on `sys.path`, gives
`import balthazar as blt` with the real module's names. On a Runner the builtin wins.

## blt_analytics integration

- **`_blt.get_blt()` order:** the real Runner module; else the bridge drop-in when
  `~/.balthazar_bridge.json` or `BALTHAZAR_BRIDGE_URL` exists, loaded by path from
  `<repo>/bridge/balthazar.py` or `$BLT_BRIDGE_DIR`. `is_tunnel()` reports whether the located
  module is the bridge.
- **frames:** the server-cache fast path uses `blt.tunnel.cached_devices_query` and
  `blt.tunnel.cached_devices`. There is no `search_devices` projection pushdown; `search_devices`
  pages plainly.
- **schema:** `get_digest()` calls `blt.tunnel.space_schema()` and polls until `ready`.
- **CLI:**
  - `blt-tunnel connect APP_URL` writes the profile after a successful `describe` (via
    `balthazar_remote.save_profile`) and prints the owner, caller, `shared` and the flow run.
  - `doctor` reports the bridge.
  - The MCP server sets `BALTHAZAR_TUNNEL_NONINTERACTIVE=1` before connecting.

## Testing

- No live space.
- A fake bridge runs in-process: start `flows/tunnel_bridge.py`'s handler on 127.0.0.1:0 with
  `tests/fakes/fake_blt.py` injected as `balthazar`. Extend the fake additively with `user`,
  `serve_app`, `new_flow_run_context` (FlowRunContext fake with output, info, search_devices
  and store_visualizations), `VisualizationBuilder` and `DeviceBuilder`/`new_devices`.
- Integration tests have the client talk to that server with the auth layer bypassed. The
  client gets a test hook that skips login, sends `X-BLT-User-Id` directly, and posts to
  `http://127.0.0.1:port/`.

## Large payloads

Large results and uploads are split and stitched in both directions, so transfers work past
the platform's 5 MB app-tunnel cap. The wire format:
- `PART_BYTES = 2 MiB`, `PART_TTL = 300` s, `MAX_PARTS = 512`, with zlib compression of the
  whole body. `describe` returns `"parts": PART_BYTES`.
- **Downloads (runner → client).** When the client sends the `X-Bridge-Parts: 1` header and the
  JSON answer exceeds `PART_BYTES`, the server answers `{"ok": true, "parts": {"id", "count"}}`.
  - The client fetches each part with `{"op": "part", "id", "index"}`. The reply is the raw
    bytes, with `application/octet-stream` and status 200.
  - An expired part gets 410.
  - Fetching the last index deletes the transfer, so parts are fetched **sequentially, in
    order**.
  - The client joins the parts, runs `zlib.decompress`, then `json.loads`, and handles the
    result as the original reply.
- **Uploads (client → runner).** When the body exceeds `part_bytes`, the client compresses it
  and posts the parts with the headers `X-Bridge-Upload: <token>`, `X-Bridge-Index` and
  `X-Bridge-Count`.
  - The server collects them per caller and runs the request once all have arrived.
  - The reply to each earlier part is `{"ok": true, "result": null}`.
- This applies to **every** reply, including `poll` and `tunnel` replies. `part` requests
  aren't audited, aren't turned into pending jobs, and aren't split themselves.
- Transfers are scoped per caller:
  - the flow param `part_bytes`, defaulting to 2 MiB;
  - transfers are dropped by the watchdog when it reclaims a caller.
