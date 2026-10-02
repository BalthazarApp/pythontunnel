# pythontunnel

Debug local Python against live Balthazar data. A flow running on the Runner serves the
whole `balthazar` API over the Balthazar app tunnel; a local drop-in `balthazar` module
reflects `blt.*` calls to it, so a script in your editor — breakpoints and all — reads real
devices and writes real flow runs, behind your Balthazar login, from any laptop.

The transport is the **reflection bridge**.

```
flows/tunnel_bridge.py           the server flow (stdlib-only)
bridge/
├── balthazar.py                 the drop-in: `import balthazar as blt` connects from the profile
└── balthazar_remote.py          the client (Remote)
```

The bridge **reflects** attribute access, calls, item access and context-manager use onto
the real module on the Runner. There is no main-thread job queue and no single-owner lock;
every call just runs `blt.*` from a worker thread. The one transport is the **Balthazar app
tunnel**. The full interface is in [`docs/SPEC.md`](docs/SPEC.md).

## Connect

```bash
# 1. Start flows/tunnel_bridge.py in your space.
#    Optional flow params: shared=true (+ allowed_users="alice-id,bob-id"),
#    device_indexes='{"wafer": "hierarchy.wafer"}', expose_secrets=false, …
# 2. In Balthazar, click "Open app" on the running flow — the page shows the snippet.
# 3. Copy the URL and connect (device-code login by default):
blt-tunnel connect "https://<host>/app-tunnel/<runner>/<flow>/?space_id=…"
#    …which prints the owner, your caller id, whether the bridge is shared, and the flow run.
# 4. Verify, then use blt / blt_analytics / the MCP tools:
blt-tunnel doctor
```

Or connect directly in Python, without the CLI:

```python
import balthazar_remote
remote = balthazar_remote.connect("https://<host>/app-tunnel/<runner>/<flow>/?space_id=…")
# or, after `blt-tunnel connect` once:  remote = balthazar_remote.connect_from_profile()
```

`blt-tunnel connect` writes `~/.balthazar_bridge.json` (0600, **no tokens** — those stay in
the `~/.config/balthazar/remote.json` cache). `blt_analytics._blt.get_blt()` then loads
`bridge/balthazar.py` automatically, so `devices_df`, `runs_df`, `overview`, the
`blt-schema` CLI and the MCP tools all run over the bridge with no code change. `$BALTHAZAR_BRIDGE_URL`
overrides the profile's URL; `blt-tunnel disconnect [--forget]` removes the profile (and,
with `--forget`, the cached token).

## Client parameters (`balthazar_remote.connect`)

`connect(app_url, *, site=None, login="device", username=None, password=None, ca_file=None, interactive=True)`

| param | meaning |
|---|---|
| `app_url` | the app-tunnel URL copied from the opened flow |
| `login` | `device` (default — confirm a code in a browser), `browser` (PKCE), or `password` |
| `username` / `password` | password login only; a password is never a CLI argument |
| `site` / `ca_file` | a non-discoverable Balthazar site, or a custom CA bundle (PEM) |
| `interactive` | `False` raises `LoginRequired` instead of prompting; `$BALTHAZAR_TUNNEL_NONINTERACTIVE=1` forces it (the MCP server sets this) |

## Run contexts, plots, outputs, device writes

Local code reads like a real flow — open a run, and output, plots and device writes inside
the block attribute to it:

```python
import matplotlib.pyplot as plt
import balthazar as blt

device = blt.search_devices(type="Wafer", limit=1)[0]

with blt.enter_new_flow_run(name="IV sweep", devices=[device],
                            parameters={"bias_max_v": 1.0}):
    plt.plot(bias, current)
    plt.show()                                     # figure lands on THIS run
    blt.output["r_zero_ohm"] = 12.3                # output lands on THIS run
    device.params.update({"measurements": {...}})  # device write attributed to THIS run
```

`blt.output` takes **primitives only** (str / int / float / bool / small list). An
exception leaving the block marks the run FAILED and re-raises.

## Flow parameters (`flows/tunnel_bridge.py`)

`shared` (default `false` — owner + `allowed_users` only), `allowed_users`, `idle_timeout`
(`900` s), `call_timeout` (`45` s), `max_refs` (`50000` per caller), `expose_secrets`
(`false`), `device_indexes`, `warm_device_cache`, `device_cache_dir`, and `part_bytes`.

## Limits (documented; not faked)

- **Long calls are polled.** A call that exceeds `call_timeout` becomes a `pending` job the
  client polls transparently (with a one-line notice); `blt_analytics` schema acquisition
  polls `blt.tunnel.space_schema()` until it is `ready`.
- **Large payloads.** Anything over the app-tunnel's ~5 MB cap is sent as **zlib-compressed
  2 MiB parts** in both directions; the part size is the `part_bytes` flow parameter.
  Transparent to your code.
- **Sibling-only nesting.** Runner 1.35.1 can only create contexts as children of the bridge
  run, so `enter_new_flow_run` blocks nested inside one another become **siblings**, and
  `parent()` is always the bridge run.
- **Heartbeat vs. debugger pauses.** A daemon thread sends a heartbeat every 30 s; if the
  client goes silent for `idle_timeout` the server closes its open contexts. A debugger pause
  freezes that thread — a known limitation (a detached heartbeat sidecar is a later
  improvement).
- **Shared mode runs as the owner.** With `shared=true`, every allowed caller's work is
  attributed to the flow's owner (`blt.user`), and an audit line is written per call.
- **Secrets hidden by default.** `blt.secrets` (and `serve_app`, `prompt_input`,
  `enter_new_flow_run`, `context`) are not reachable through the bridge unless the flow sets
  `expose_secrets=true`.

## tunnel namespace

The device-cache and schema helpers live under `blt.tunnel`:
`blt.tunnel.cached_devices(index, value)`, `blt.tunnel.cached_devices_query(device_type=…,
keys=[…])`, `blt.tunnel.device_cache_status()`, `blt.tunnel.refresh_device_cache()`, and
`blt.tunnel.space_schema()`. `blt.get_<index>_devices(value)` is generated from the flow's
`device_indexes`. `blt_analytics` uses these automatically.

## Security

The bridge is served over the Balthazar app tunnel, behind your login: every call carries
`X-BLT-User-Id`; browser POSTs (`Origin` / `Sec-Fetch-Site` present) are rejected; by default
only the owner (`blt.user`, case-insensitive) and `allowed_users` may connect, and
`shared=true` opens it to every caller the platform lets through, with a per-call audit line.

## Why naming the drop-in `balthazar` is safe

The Runner registers its module with `pyo3::append_to_inittab!`, which makes it a
**builtin**. CPython consults `BuiltinImporter` before `PathFinder`, so on the Runner the
real module always wins. Locally, where no builtin exists, `bridge/balthazar.py` is found
instead. The same `import balthazar as blt` works in both places — no `try/except` import,
and code you debug locally is code you deploy unchanged.

---

# Analytics with your AI agent

A coding agent (Claude Code, Cursor, Copilot) can answer data questions about your Balthazar
space: it learns the space's **structure** from schema tools, then writes Python that pulls
the **real data** through the bridge and plots it.

## Install and set up

```bash
uv pip install -e ".[all]"     # package `blt_analytics` + CLI + MCP server
blt-tunnel setup               # register the MCP server, copy the skills, update AGENTS.md
blt-tunnel connect "<app url>" # start flows/tunnel_bridge.py, "Open app", copy the URL
blt-tunnel doctor              # bridge, connection, describe, schema, pandas, mcp
```

`blt-tunnel setup` is idempotent and prints what it changed: it writes the `balthazar-schema`
MCP server entry into `.mcp.json` / `.cursor/mcp.json` / `.vscode/mcp.json`, copies the
`skills/*` into `.claude/skills/` and `.agents/skills/` (add `--global` for the user-level
dirs), and maintains a block in `AGENTS.md`. The bridge flow (`flows/tunnel_bridge.py`) must
be running in the space first.

## Three layers, three jobs

| Layer | What it is | What it does |
|---|---|---|
| **Tools** | `balthazar-schema` MCP server (or the `blt-schema` CLI) | Tell the agent **what exists** — device types, params, flows, runs, inputs/outputs, with dtypes, shapes and coverage. **Schema only, never values.** |
| **Skills** | `balthazar-tunnel`, `balthazar-analytics` | Tell the agent **how to work** — the setup, the schema-first workflow, the gotchas. |
| **`blt` / `blt_analytics`** | the drop-in + the pandas layer | Fetch the **actual data** into the user's process (`devices_df`, `runs_df`, …) and plot it. |

The split is the point: the agent reads *schema* to decide what to pull, then pulls and plots
the *values* locally. Figures attach to a Balthazar run with `publish(...)` only when you ask
to save or share. See `skills/balthazar-analytics/` for the full workflow and worked examples.

## Large spaces: the server-side device cache

On a space with hundreds of thousands of devices, paging every `search_devices` call is slow.
The bridge flow can keep a **server-side device cache** — all device records by id, plus a
lookup table per configured *index*. Configure it with the flow parameters `device_indexes`
(e.g. `{"wafer": "hierarchy.wafer"}`), `warm_device_cache` and `device_cache_dir`.

- `blt_analytics.devices_df(...)` uses the cache automatically when it is ready (no API
  change), and `devices_df(index="wafer", value="W123")` pulls one index value straight from
  it. `overview()` lists the configured indexes under `device_indexes`.
- Directly on the bridge, these live under `blt.tunnel`:
  `blt.tunnel.cached_devices("wafer", "W123")`, `blt.tunnel.device_cache_status()`,
  `blt.tunnel.refresh_device_cache()`, `blt.tunnel.cached_devices_query(...)` — plus the
  generated `blt.get_wafer_devices("W123")`. They are **tunnel-only** (not on the real
  Runner), and the first load can take minutes. See `skills/balthazar-tunnel/` and
  `skills/balthazar-analytics/`.
