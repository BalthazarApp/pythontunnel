# pythontunnel

Debug local Python against live Balthazar data. A flow running on the Runner hosts a
loopback JSON-RPC server; a local drop-in `balthazar` module forwards `blt.*` calls to
it, so a script in VS Code — breakpoints and all — reads real devices and writes real
flow runs.

Two generations live here:

| | v1 — one-shot | v2 — session |
|---|---|---|
| Flow | `flows/tunnel_server.py` | `flows/tunnel_session_server.py` |
| Client | `balthazar.py` | `session_tunnel/balthazar.py` |
| Model | each call self-contained; a run is assembled client-side and posted in one shot | flow-run **contexts stay open** across calls |
| `enter_new_flow_run` | not supported | yes, nested |
| Plots | `new_flow_run(..., figures=fig)` | `plt.show()` inside a context |
| Device writes | no | `device.params.update({...})` |
| Port | 8765 | 8766 |

v1 is the smaller, safer thing: no shared server state, so no ownership or recovery
concerns. v2 is what you want for code that should read like a real flow. They can run
side by side on different ports. **[Jump to v2 →](#v2--session-tunnel)**

## The three pieces (v1)

| File | Runs where | Role |
|---|---|---|
| `flows/tunnel_server.py` | On the Runner, as a Balthazar flow | Hosts the RPC server, translates requests into real `blt.*` calls |
| `balthazar.py` | Your machine | Drop-in stand-in for the injected module; forwards over HTTP |
| `demo.ipynb` | Your machine | Notebook demo — one example per cell, inline plots and printed results |
| `demo.py` | Your machine | Same examples as a plain script, for headless runs and CI |

## Run it

1. Register `flows/tunnel_server.py` as a flow in your space and start it. Leave it
   running — it writes credentials to `~/.balthazar_tunnel.json` (mode 0600).
   Optional flow parameter: `port` (default `8765`).
2. Open `demo.ipynb` from this directory and run the cells, or run `python demo.py`. Set a
   breakpoint anywhere — the calls execute on the Runner, the code executes locally.

Open the notebook with this directory as the working directory, so `import balthazar`
resolves to the shim. In VS Code and JupyterLab that is the default for a notebook stored
here.

### One notebook gotcha

The inline backend closes figures at the end of each cell
(`InlineBackend.close_figures` defaults to `True`), so `figures="all"` only sees figures
created in the *current* cell — a figure from an earlier cell is already gone. Build the
set you want to ship together in one cell, or pass the figures explicitly. In a plain
script this doesn't arise.

Ordering works out on its own: the inline backend renders at the *end* of the cell, which
is after `new_flow_run` has already shipped the figure, so one `fig` both uploads and
displays.

The shim is stdlib-only and needs no install — `balthazar.py` is picked up from the
working directory. To use it from elsewhere, put this directory on `PYTHONPATH`. Sending
plots additionally needs `matplotlib` locally (imported lazily, so reads and plain runs
work without it); `demo.py` also uses `numpy`.

## Attaching plots to a flow run

```python
fig, ax = plt.subplots()
ax.plot(bias, current)

blt.new_flow_run("I–V sweep", devices=[device], figures=fig)   # or figures="all"
```

`figures` takes a Figure, a list of them, or `"all"` for every open figure (which is
what `plt.show()` would sweep up). They are rendered to SVG locally and stored against
the new run.

## Inputs and outputs on a run

```python
blt.new_flow_run(
    "I-V sweep",
    devices=[device],
    parameters={"bias_min_v": -1.0, "bias_max_v": 1.0, "points": 201},   # inputs
    output={"i_at_1v_ma": 0.87, "status": "success"},                     # outputs
    figures=fig,
)
```

`parameters` are the run's inputs and must be **flat scalars** — they go through the
platform's parameter type inference, and a `None` has no inferable type. `output` takes
**primitives only**: str / int / float / bool / small list. No dicts, no dates, no
DataFrames.

The tunnel flow reports its own outputs too — `tunnel_url`, `port`, `status`, and live
counters for requests served, flow runs created, plots stored and errors.

## Run status

A run is `FINISHED` unless you pass `status="FAILED"`. That is enforced rather than
incidental: the Runner's context `__exit__` reports FAILED for **any** exception in
flight inside the run's context, so an unguarded failure while storing a plot or writing
output would turn the whole run red with no explanation beyond the exception string. Each
step inside the context therefore catches its own errors, which are

- logged into the run via `blt.error`,
- returned to the client in `problems`,
- printed locally as `tunnel warning: …`.

Two things make this work, both found by reading the Runner source:

- Plots are stored via `blt.api.store_visualizations([(id, filename, svg_bytes)])` — the
  same call the Runner's own matplotlib backend makes on `plt.show()`. It attaches to
  whichever flow run is *current* and returns no IDs, so a plot cannot be linked to a
  run after the fact.
- Therefore the new run must be **entered** before storing, via `enter_new_flow_run`,
  which the Runner restricts to the main thread. Hence the job queue: HTTP handlers run
  on worker threads and hand work to the main thread, which is the only caller of
  `blt.*`. Every operation is executed serially.

Sending rendered SVG rather than a pickled figure is deliberate — the payload stays
independent of the matplotlib version on the Runner.

Without `figures`, `new_flow_run` takes the simpler path (`blt.new_flow_run`, a
completed history entry). Same visible result, different call underneath.

## Why naming the shim `balthazar` is safe

The Runner registers its module with `pyo3::append_to_inittab!`, which makes it a
**builtin**. CPython consults `BuiltinImporter` before `PathFinder` in `sys.meta_path`,
so on the Runner the real module always wins — even if this file sits in the cwd or the
venv. Locally, where no builtin exists, this file is found instead. That means
`import balthazar as blt` is identical in both places: no `try/except` shim import, and
code you debug locally is code you can deploy unchanged.

`flows/tunnel_server.py` still asserts it got the real module (via a
`__balthazar_tunnel__` marker), so a misconfigured path fails loudly instead of making
the tunnel call itself.

## Scope (v1)

Operations: `ping`, `search_devices`, `get_device_params`, `new_flow_run`,
`create_flow_run` (the plot-bearing path), `log`. Device-param writes, open contexts and
a context-aware `plt.show()` are v2's job. `blt.secrets` is exposed by neither.

---

# v2 — session tunnel

```
flows/tunnel_session_server.py   the flow (port 8766, idle_timeout 1800s)
session_tunnel/
├── balthazar.py                 the v2 client
├── demo_sessions.py             script demo
└── demo_sessions.ipynb          notebook demo
```

Start the flow, then run the demos **from `session_tunnel/`** so `import balthazar`
resolves to the v2 shim rather than the v1 one a level up:

```bash
cd session_tunnel && python demo_sessions.py
```

```python
with blt.enter_new_flow_run(name="I-V sweep", devices=[device],
                            parameters={"bias_max_v": 1.0}):
    plt.plot(bias, current)
    plt.show()                                   # figure lands on THIS run
    blt.output["r_zero_ohm"] = 12.3              # output lands on THIS run
    device.params.update({"measurements": ...})  # attributed to THIS run
```

Inside the block, `blt.params`, `blt.devices`, `blt.flow_run`, `blt.parent()` and
`blt.parents()` all reflect the open run — implemented with a module-level
`__getattr__` (PEP 562), since a plain assignment could not track the innermost
context. Contexts nest and must close LIFO. An exception marks the run FAILED and
re-raises; `run.fail("why")` does it without an exception.

### What makes v2 harder

- **A context spans many requests**, and the Runner's context is process-global
  (`blt.params`, `blt.output`, `blt.devices`, `blt.flow_run` are module attributes). Two
  clients interleaving would silently write into each other's runs, so while a stack is
  open only the owning client may act.
- **A client that dies mid-block** leaves a run stuck in `RUNNING`. Three backstops: the
  shim closes contexts on interpreter exit, the server's idle watchdog unwinds abandoned
  contexts after `idle_timeout`, and `blt.reset_contexts()` forces it. The watchdog
  default is deliberately generous (30 min) because a breakpoint inside a `with` block
  stops the client from sending anything — and with VS Code's debugger suspending all
  threads, even the heartbeat stops.
- **Marking a child FAILED** needs an exception in flight at exit, since the Runner
  derives status from `exc_value`. The server calls `__exit__` directly with a
  synthesized exception rather than raising, so the recorded message is the client's.

### `plt.show()` capture

The shim wraps `plt.show` while a context is open — uploading every open figure whose
SVG hash it has not already sent, then delegating to the real `show`. Wrapping rather
than replacing the backend keeps inline rendering working in notebooks; the Runner uses a
real backend (`MPLBACKEND=module://balthazar.matplotlib.backend`), which would displace
the inline backend and cost you local plots.

### Device-param writes

`update()` is the only write path. Subscript assignment **raises**, because on the real
proxy it does not reliably sync (notably for dict values) — the real failure is silent,
which is worse to develop against. `pop(key, default)` raises `KeyError` to match the
real proxy, which ignores the default. `del params[key]` works, since removal is the one
thing `update` cannot express.

### Demo configuration

Both v2 demos start with `DEVICE_TYPE`, `DEVICE_NAME` and `MAX_BATCH_DEVICES`. The last
one matters: the nested example opens one child run per device, so on a space with 65
devices an uncapped loop would create 65 runs.

## Security (both)

Binds `127.0.0.1` only, per-session bearer token, non-loopback `Host` headers rejected,
and dispatch only through an explicit operation allowlist. Anything that can read the
connection file gets full read access to the space and can create flow runs, so treat
the token as a credential. `blt.secrets` is deliberately not tunnelled — v2 raises
`NotImplementedError` on it rather than forwarding.
