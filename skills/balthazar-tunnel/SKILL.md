---
name: balthazar-tunnel
description: Run local Python against live Balthazar space data through the v2 session tunnel. Open flow-run contexts with enter_new_flow_run, capture plt.show() figures onto the open run, write run outputs (blt.output) and device params (params.update), and recover stuck contexts with reset_contexts. Covers setup (blt-tunnel doctor, the connection file), importing the shim (import balthazar as blt, from session_tunnel/ or via blt_analytics), what is not tunnelled (blt.secrets, blt.context), and portability to the real Runner. Triggers include: session tunnel, enter_new_flow_run, nested flow runs, plt.show not saving to a run, device.params.update, run stuck in RUNNING, reset_contexts, balthazar.py shim, port 8766, "debug a flow locally against real data".
---

# Balthazar session tunnel (v2)

Debug local Python against live Balthazar data. A flow running on the Runner hosts a
loopback JSON-RPC server; a local drop-in `balthazar` module forwards `blt.*` calls to it.
A script in your editor — breakpoints and all — reads real devices and writes real flow
runs. **v2** keeps flow-run **contexts open across calls**, so local code reads exactly
like a real flow.

Use this skill when the task is to *act like a flow* locally: open a run, attach plots,
write outputs, update device params. To *answer data questions* (load devices/runs into
pandas and plot), use the **balthazar-analytics** skill instead — it builds on this tunnel.

## Prerequisites

1. **The tunnel flow is running in the space.** Register and start
   `flows/tunnel_session_server.py` as a flow (port 8766, idle timeout 1800s). Leave it
   running; it writes credentials to `~/.balthazar_session_tunnel.json` (mode 0600).
2. **A connection the client can find.** Either the connection file above, or the env
   vars `BALTHAZAR_SESSION_TUNNEL_URL` and `BALTHAZAR_SESSION_TUNNEL_TOKEN` (which win over
   the file). Anything that can read the file gets full read access to the space and can
   create runs — treat the token as a credential.
3. **Verify before you code:**

   ```bash
   blt-tunnel doctor
   ```

   It checks the connection file, `ping`, `space_schema`, pandas, the `mcp` package, and
   the agent registrations, printing pass/fail for each. Fix any failing line first. If
   `blt-tunnel` is not on PATH, install the package: `uv pip install -e ".[all]"`.

## Importing the shim

`import balthazar as blt` must resolve to the **v2** shim at
`session_tunnel/balthazar.py`, not the v1 one at the repo root.

- **Writing a plain script/notebook that calls `blt.*` directly:** run it from the
  `session_tunnel/` directory (`cd session_tunnel && python your_script.py`), or put that
  directory on `PYTHONPATH`. In VS Code / JupyterLab, a notebook stored in
  `session_tunnel/` resolves there by default.
- **Using `blt_analytics`:** you don't need to `cd` anywhere. `blt_analytics` locates the
  v2 shim for you (via `$BLT_TUNNEL_SHIM`, else `<repo>/session_tunnel/balthazar.py`) and
  raises a clear error if it accidentally picks up the v1 shim.

Naming the file `balthazar` is safe: on the real Runner the injected module is a builtin
and always wins, so the *same* `import balthazar as blt` works unchanged in both places —
no `try/except` shim import, and code you debug locally is code you deploy.

## Run contexts

A context is a live flow run. Open one, and output, plots and device writes inside the
block attribute to it:

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

- The context is **entered as soon as `enter_new_flow_run` returns**; the `with` only
  governs when it closes.
- Inside the block `blt.params`, `blt.devices`, `blt.device`, `blt.flow_run`,
  `blt.parent()` and `blt.parents()` all reflect the innermost open run (implemented with a
  module-level `__getattr__`, PEP 562).
- **Contexts nest and must close LIFO.** A parent run with one child per device is the
  standard nested shape; cap the loop (the demos use `MAX_BATCH_DEVICES`) so you don't open
  one run per device on a 65-device space.
- An **exception** escaping the block marks the run FAILED (with the message) and
  re-raises. To fail without raising: `cm = blt.enter_new_flow_run(...); cm.fail("why")`.
  `cm.exit()` closes it successfully without a `with`.

`new_flow_run(...)` creates a completed run in one call when there is nothing to attach
incrementally.

## plt.show() capture

While a context is open the shim **wraps** `plt.show` (it does not replace the backend, so
inline rendering in notebooks keeps working). Each `plt.show()` uploads every open figure
whose SVG it has not already sent, then displays normally.

- **You must call `plt.show()`** for a figure to reach the run. Building a figure is not
  enough.
- Figures are keyed by matplotlib **figure number**, which is a *replace* key: redrawing
  figure 1 across several `plt.show()` calls leaves the run holding the latest frame, not a
  pile of intermediates.
- **Notebook gotcha:** the inline backend closes figures at the end of each cell, so build
  the figures you want on one run within a single cell.
- Sending plots needs `matplotlib` installed locally (imported lazily; reads and plain
  runs work without it).

## Outputs

`blt.output` writes through to the open run (or the tunnel's own root run at the top
level):

```python
blt.output["yield_pct"] = 92.5
blt.output.update({"status": "success", "n": 40})
```

**Primitives only** — str / int / float / bool / small list. Dicts, dates and DataFrames
are rejected at the client rather than silently mangled. Convert first
(`float(df["x"].mean())`, `ts.isoformat()`).

## Device writes — `update()` only

`device.params.update({...})` is the **only** write path.

- Subscript assignment (`params["k"] = v`) **raises** — on the real proxy it does not
  reliably sync (notably for dict values), and a silent failure is worse to debug against.
  For nested changes, rebuild the top-level key and `update` it in one pass.
- `del params[key]` works (removal is the one thing `update` cannot express).
- `params.pop(key, default)` **raises `KeyError`** on a missing key even with a default —
  bug-compatible with the real proxy. Guard a `del` instead.
- `params.refresh()` drops the read cache so the next read re-fetches.

## Recovery

A client that dies mid-block leaves a run stuck in `RUNNING`. Three backstops:

1. The shim closes open contexts on interpreter exit (`atexit`).
2. The server's idle watchdog unwinds abandoned contexts after `idle_timeout` (default
   ~30 min — generous because a breakpoint inside a `with` block stops the client, and the
   heartbeat, from sending anything).
3. **`blt.reset_contexts()`** force-closes every context on the server, including another
   client's. This is the escape hatch for a stuck session; it returns the count closed.

`blt.tunnel_state()` asks the server what it thinks the context stack is — a consistency
check when something looks wrong. A **second client acting while a stack is open** gets a
`PermissionError` (the Runner's context is process-global, so only the owner may act);
`reset_contexts()` clears it.

## What is not supported

- `blt.secrets` — raises `NotImplementedError`. Deliberately not tunnelled: any process
  that can reach the port could otherwise read your credentials.
- `blt.context` — raises `NotImplementedError`. The Runner's 1.35.1 `FlowRunContext`
  object is not emulated. Use the module globals the shim already rebinds (`blt.output`,
  `blt.devices`, `blt.params`), or `blt.tunnel_state()` for the server's view.
- The shim also exposes read ops used mainly through `blt_analytics` — `search_flows`,
  `search_flow_run_history`, `fetch_visualizations`, and `tunnel_space_schema`. See the
  **balthazar-analytics** skill.

## Server-side device cache (large spaces)

On a space with hundreds of thousands of devices, paging `search_devices` on every
call is slow. The tunnel flow can keep a **server-side device cache** — every device
record by id, plus one lookup table per configured *index* (an index maps a value to
the ids that have it). It is persisted on the Runner, so a tunnel restart does not
reload everything, and it is write-through: a `device.params.update(...)` through the
tunnel updates the cached record too.

**Configuring indexes** (flow parameters on `tunnel_session_server_flow`):

- `device_indexes` — a JSON string mapping an index name to a dotted param path,
  e.g. `{"wafer": "hierarchy.wafer"}`. The value may instead be an object
  `{"path": "hierarchy.wafer", "device_type": "Die"}` to restrict the index to one
  device type. Default `"{}"` (no cache).
- `warm_device_cache` — start loading at tunnel start (default `true` once any index
  is configured).
- `device_cache_dir` — where the cache is persisted (default `~/.balthazar_tunnel_cache`).

**Accessors (tunnel-only — these do not exist on the real Runner):**

```python
import balthazar as blt

blt.device_cache_status()          # {"state": "empty"|"loading"|"ready"|"error", count, indexes: {...}}
wafer_devs = blt.get_wafer_devices("W123")          # every device with hierarchy.wafer == "W123"
same      = blt.cached_devices("wafer", "W123")     # the generic form
```

- `blt.get_<index>_devices(value, *, refresh=False)` is **generated per configured
  index** (the `ping` response advertises them). They return normal `Device` objects,
  so `device.params.update(...)` keeps working.
- **First load can take minutes** on a 250k-device space. While it runs,
  `device_cache_status()["state"]` is `"loading"` and accessors block until it is
  `"ready"` (the shim prints `tunnel: loading device cache …` and uses a long HTTP
  timeout). Call `device_cache_status()` to check progress without blocking.
- **`refresh=True` re-fetches only the *already-known* device ids for that value**
  and re-indexes them (dropping ids that vanished). It does **not** discover
  brand-new devices of that value — for that, reload the whole cache first with
  `blt.refresh_device_cache()` (a full reload; `wait=True` blocks until done).

**Portability.** `get_<index>_devices` / `cached_devices` / `device_cache_status` /
`refresh_device_cache` are shim-only; the real Runner has none of them (a bare
`blt.get_wafer_devices(...)` is an `AttributeError` there). The index cache is a
tunnel optimisation, not a flow API — so:

- For **deployable flow code**, use the portable `blt_analytics.devices_df(
  device_type=..., columns=[...])`. On the tunnel it transparently reads the device
  cache when it is `"ready"` (no paging); on the Runner it pages `search_devices` —
  same code, same frame.
- `blt_analytics.devices_df(index=..., value=...)` is the uniform wrapper over the
  index accessor. It is still tunnel-only, but fails with a **clear, actionable
  error** on the Runner (pointing you at `device_type=`/`columns=`) instead of an
  `AttributeError` — prefer it over the raw `blt.get_<index>_devices` accessor, and
  gate it behind `blt_analytics.is_tunnel()` if the code must also run on the Runner.

See the **balthazar-analytics** skill for the pandas side.

## Portability to the real Runner

Code you debug here deploys to the Runner unchanged, with two caveats:

- The extra `search_devices` projection kwargs (`keys=`, `scalars_only=`,
  `include_params=`) and `tunnel_space_schema()` are **shim-only**. The real Runner's
  `search_devices` has no projection. For portable projection, go through `blt_analytics`
  (which passes those kwargs only when talking to the shim).
- `blt.tunnel_state()` is tunnel bookkeeping, not part of the emulated flow API; it is not
  present on the Runner.

Everything else — `enter_new_flow_run`, `new_flow_run`, `blt.output`, `plt.show()`
capture, `device.params.update`, nesting and FAILED semantics — mirrors the Runner.
