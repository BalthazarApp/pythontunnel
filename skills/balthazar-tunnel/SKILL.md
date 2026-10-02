---
name: balthazar-tunnel
description: Run local Python against live Balthazar space data through the v3 reflection bridge (bridge/balthazar.py, blt-tunnel connect, app tunnel only, behind your Balthazar login). Open flow-run contexts with enter_new_flow_run, capture plt.show() figures onto the open run, write run outputs (blt.output) and device params (params.update). Covers connecting (blt-tunnel connect/disconnect/doctor, balthazar_remote.connect/connect_from_profile, the bridge profile ~/.balthazar_bridge.json), the blt.tunnel namespace (cached_devices, cached_devices_query, device_cache_status, refresh_device_cache, space_schema), v3 limits (long calls polled, large payloads as parts, sibling-only nesting, heartbeat vs debugger pauses, shared-mode runs as owner, secrets hidden), what is not tunnelled (blt.secrets, blt.context), and portability to the real Runner. Triggers include: reflection bridge, v3 bridge, remote tunnel, app tunnel, blt-tunnel connect, balthazar_remote, connect from a laptop, device-code login, enter_new_flow_run, nested flow runs, plt.show not saving to a run, device.params.update, run stuck in RUNNING, balthazar.py drop-in, blt.tunnel, "debug a flow locally against real data".
---

# Balthazar tunnel — v3 reflection bridge

Debug local Python against live Balthazar data. A flow running on the Runner serves the
whole `balthazar` API over the Balthazar app tunnel; a local drop-in `balthazar` module
reflects `blt.*` calls to it. A script in your editor — breakpoints and all — reads real
devices and writes real flow runs, with flow-run **contexts kept open across calls**, so
local code reads exactly like a real flow.

Use this skill when the task is to *act like a flow* locally: open a run, attach plots,
write outputs, update device params. To *answer data questions* (load devices/runs into
pandas and plot), use the **balthazar-analytics** skill instead — it builds on this tunnel.

## The reflection bridge

v3 is a **generic reflection bridge** (ported from the `remoteblt4/` developer prototype).
It has no main-thread job queue and no single-owner lock — every call just runs `blt.*` from
a worker thread — and its only transport is the **Balthazar app tunnel**, so it always works
behind your login, from any laptop.

```
flows/tunnel_bridge.py           the v3 server flow (stdlib-only)
bridge/balthazar.py              the drop-in: `import balthazar as blt` connects from the profile
bridge/balthazar_remote.py       the v3 client (Remote)
```

## Connect

```bash
# a) Start flows/tunnel_bridge.py in your space. Optional flow params:
#       shared = true                      # else owner + allowed_users only
#       allowed_users = "alice-id,bob-id"
#       device_indexes = {"wafer": "hierarchy.wafer"}
#       expose_secrets = false
# b) In Balthazar, click "Open app" on the running flow — the page shows the snippet.
# c) Copy the URL and connect (device-code login by default):
blt-tunnel connect "https://<host>/app-tunnel/<runner>/<flow>/?space_id=…"
#    …prints the owner, your caller id, whether the bridge is shared, and the flow run.
# d) Verify, then use blt / blt_analytics / the MCP tools:
blt-tunnel doctor
```

Or connect directly in Python: `import balthazar_remote; remote =
balthazar_remote.connect("<app url>")` — or `balthazar_remote.connect_from_profile()` after
one `blt-tunnel connect`.

**`balthazar_remote.connect` parameters:**
`connect(app_url, *, site=None, login="device", username=None, password=None, ca_file=None, interactive=True)`

| param | meaning |
|---|---|
| `app_url` | the app-tunnel URL from the opened flow |
| `login` | `device` (default), `browser` (PKCE), or `password` |
| `username` / `password` | password login only; a password is never a CLI argument |
| `site` / `ca_file` | a non-discoverable site, or a custom CA bundle (PEM) |
| `interactive` | `False` raises `LoginRequired` instead of prompting; `$BALTHAZAR_TUNNEL_NONINTERACTIVE=1` forces it (the MCP server sets this) |

`save_profile(...)` + `connect_from_profile(...)` persist and reuse the profile at
`~/.balthazar_bridge.json` (0600, **no tokens**; tokens stay in
`~/.config/balthazar/remote.json`). `$BALTHAZAR_BRIDGE_URL` overrides the URL.
`blt-tunnel disconnect [--forget]` removes the profile (and, with `--forget`, the token).
Once connected, `blt_analytics` loads `bridge/balthazar.py` automatically.

## The `blt.tunnel` namespace

The device-cache and schema helpers are grouped under `blt.tunnel`:
`cached_devices(index, value)`, `cached_devices_query(device_type=…, keys=[…])`,
`device_cache_status()`, `refresh_device_cache()`, `space_schema()`. The per-index accessor
`blt.get_<index>_devices(value)` is generated from the flow's `device_indexes`.
`blt_analytics` uses all of these automatically.

## Limits (real; don't work around them blindly)

- **Long calls are polled.** A call that exceeds `call_timeout` (first device-cache load,
  `space_schema`) returns a `pending` job the client polls transparently (with a notice).
- **Large payloads.** Anything over the app-tunnel's ~5 MB cap is sent as **zlib-compressed
  2 MiB parts** in both directions; the part size is the `part_bytes` flow parameter.
  Transparent to you.
- **Sibling-only nesting.** Runner 1.35.1 can only create contexts as children of the bridge
  run, so nested `enter_new_flow_run` blocks become **siblings** and `parent()` is the bridge
  run. Documented, not faked.
- **Heartbeat vs. debugger pauses.** A 30 s daemon heartbeat keeps the session alive; if the
  client is silent for `idle_timeout` the server closes its open contexts. A debugger pause
  freezes the heartbeat thread (known limitation).
- **Shared mode runs as the owner.** With `shared=true`, every allowed caller's work is
  attributed to the flow owner (`blt.user`), with a per-call audit line.
- **Secrets hidden by default.** `blt.secrets` (and `serve_app`, `prompt_input`,
  `enter_new_flow_run`, `context`) are unreachable unless the flow sets `expose_secrets=true`.

## Prerequisites

1. **The bridge flow is running in the space.** Register and start `flows/tunnel_bridge.py`
   as a flow; "Open app" to get the connection URL.
2. **A saved connection.** `blt-tunnel connect "<app url>"` writes `~/.balthazar_bridge.json`
   (0600) and caches the login token at `~/.config/balthazar/remote.json`. Anything that can
   read those gets your access to the space — treat them as credentials.
3. **Verify before you code:**

   ```bash
   blt-tunnel doctor
   ```

   It reports the bridge, then checks the connection/profile, `describe`, `space_schema`,
   pandas, the `mcp` package, and the agent registrations, printing pass/fail for each. Fix
   any failing line first. If `blt-tunnel` is not on PATH, install the package:
   `uv pip install -e ".[all]"`.

## Importing the drop-in

`import balthazar as blt` resolves to `bridge/balthazar.py`, which connects from the saved
profile on first use. Run your script with `bridge/` on `sys.path` (e.g. `cd bridge`, or put
it on `PYTHONPATH`); when you use `blt_analytics` you don't need to — it loads
`bridge/balthazar.py` for you (via `$BLT_BRIDGE_DIR`, else `<repo>/bridge/balthazar.py`).

Naming the file `balthazar` is safe: on the real Runner the injected module is a builtin and
always wins, so the *same* `import balthazar as blt` works unchanged in both places — no
`try/except` import, and code you debug locally is code you deploy.

## Run contexts

A context is a live flow run. Open one, and output, plots and device writes inside the block
attribute to it:

```python
import matplotlib.pyplot as plt
import balthazar as blt

device = blt.search_devices(type="Wafer", limit=1)[0]

with blt.enter_new_flow_run(name="IV sweep", devices=[device],
                            parameters={"bias_max_v": 1.0}):
    plt.plot(bias, current)
    plt.show()                                   # figure lands on THIS run
    blt.output["r_zero_ohm"] = 12.3              # output lands on THIS run
    device.params.update({"measurements": {...}})# device write attributed to THIS run
```

- The context is **entered as soon as `enter_new_flow_run` returns**; the `with` only governs
  when it closes.
- Inside the block `blt.params`, `blt.devices`, `blt.flow_run`, `blt.parent()` and
  `blt.parents()` all reflect the innermost open run.
- **Nested blocks become siblings** of the bridge run (the Runner limit above), so
  `parent()` is always the bridge run. Cap any per-device loop so you don't open one run per
  device on a large space.
- An **exception** escaping the block marks the run FAILED (with the message) and re-raises.

`new_flow_run(...)` creates a completed run in one call when there is nothing to attach
incrementally.

## plt.show() capture

While a context is open the client **wraps** `plt.show` (it does not replace the backend, so
inline rendering in notebooks keeps working). Each `plt.show()` uploads every open figure
whose SVG it has not already sent, then displays normally.

- **You must call `plt.show()`** for a figure to reach the run. Building a figure is not
  enough.
- Figures are keyed by matplotlib **figure number**, which is a *replace* key: redrawing
  figure 1 across several `plt.show()` calls leaves the run holding the latest frame.
- **Notebook gotcha:** the inline backend closes figures at the end of each cell, so build
  the figures you want on one run within a single cell.
- Sending plots needs `matplotlib` installed locally (imported lazily; reads and plain runs
  work without it).

## Outputs

`blt.output` writes through to the open run (or the bridge's own run at the top level):

```python
blt.output["yield_pct"] = 92.5
blt.output.update({"status": "success", "n": 40})
```

**Primitives only** — str / int / float / bool / small list. Dicts, dates and DataFrames are
rejected at the client rather than silently mangled. Convert first
(`float(df["x"].mean())`, `ts.isoformat()`).

## Device writes — `update()` only

`device.params.update({...})` is the **only** write path.

- Subscript assignment (`params["k"] = v`) **raises** — on the real proxy it does not
  reliably sync (notably for dict values), and a silent failure is worse to debug against.
  For nested changes, rebuild the top-level key and `update` it in one pass.
- `del params[key]` works (removal is the one thing `update` cannot express).
- `params.pop(key, default)` **raises `KeyError`** on a missing key even with a default —
  bug-compatible with the real proxy. Guard a `del` instead.

## Recovery

A client that dies mid-block leaves a run stuck in `RUNNING`. The server's **watchdog**
closes a caller's open contexts (innermost first) once it has been silent for `idle_timeout`.
The client sends a heartbeat every 30 s to keep the session alive — but a debugger pause
freezes that thread (the known limitation above), so a long breakpoint inside a `with` block
can trip the watchdog.

## What is not supported

- `blt.secrets` — hidden unless the flow sets `expose_secrets=true`. Not exposed by default
  because any caller the bridge admits could otherwise read your credentials.
- `blt.context`, `serve_app`, `prompt_input`, `enter_new_flow_run` as a reflected attribute —
  hidden on the server. Use `enter_new_flow_run(...)` as a context manager (the client
  emulates it), and the run globals the client rebinds (`blt.output`, `blt.devices`,
  `blt.params`).
- The schema digest comes through `blt.tunnel.space_schema()` (used mainly via
  `blt_analytics`). See the **balthazar-analytics** skill.

## Server-side device cache (large spaces)

On a space with hundreds of thousands of devices, paging `search_devices` on every call is
slow. The bridge flow can keep a **server-side device cache** — every device record by id,
plus one lookup table per configured *index* (an index maps a value to the ids that have it).
It is persisted on the Runner, so a bridge restart does not reload everything, and it is
write-through: a `device.params.update(...)` through the bridge updates the cached record too.

**Configuring indexes** (flow parameters on `flows/tunnel_bridge.py`):

- `device_indexes` — a JSON string mapping an index name to a dotted param path,
  e.g. `{"wafer": "hierarchy.wafer"}`. The value may instead be an object
  `{"path": "hierarchy.wafer", "device_type": "Die"}` to restrict the index to one device
  type. Default `"{}"` (no cache).
- `warm_device_cache` — start loading at bridge start (default `true` once any index is
  configured).
- `device_cache_dir` — where the cache is persisted (default `~/.balthazar_tunnel_cache`).

**Accessors (tunnel-only — these do not exist on the real Runner):**

```python
import balthazar as blt

blt.tunnel.device_cache_status()        # {"state": "empty"|"loading"|"ready"|"error", count, indexes: {...}}
wafer_devs = blt.get_wafer_devices("W123")       # every device with hierarchy.wafer == "W123"
same       = blt.tunnel.cached_devices("wafer", "W123")   # the generic form
```

- `blt.get_<index>_devices(value, *, refresh=False)` is **generated per configured index**
  (the `describe` reply advertises them). They return read-only `CachedDevice` objects; call
  `.live()` for the real remote `Device` when you need to write.
- **First load can take minutes** on a 250k-device space. While it runs,
  `device_cache_status()["state"]` is `"loading"` and the cache ops are polled until it is
  `"ready"`. Call `device_cache_status()` to check progress.
- **`refresh=True` re-fetches only the *already-known* device ids for that value** and
  re-indexes them (dropping ids that vanished). It does **not** discover brand-new devices of
  that value — for that, reload the whole cache first with `blt.tunnel.refresh_device_cache()`.

## Portability to the real Runner

Code you debug here deploys to the Runner unchanged. One caveat: the device-cache ops
(`blt.tunnel.*`, `get_<index>_devices`) and `blt.tunnel.space_schema()` are **tunnel-only** —
the real Runner has none of them. There is **no `search_devices` projection** on v3 either:
the bridge forwards straight to the real Runner, which has no `keys=`/`scalars_only=` kwargs.
For portable code:

- Use `blt_analytics.devices_df(device_type=..., columns=[...])`. On the bridge it reads the
  device cache when it is `"ready"` (no paging); on the Runner it pages `search_devices` —
  same code, same frame.
- `blt_analytics.devices_df(index=..., value=...)` is the uniform wrapper over the index
  accessor. It is tunnel-only but fails with a **clear, actionable error** on the Runner
  (pointing you at `device_type=`/`columns=`) instead of an `AttributeError`.

Everything else — `enter_new_flow_run`, `new_flow_run`, `blt.output`, `plt.show()` capture,
`device.params.update`, and FAILED semantics — mirrors the Runner.
