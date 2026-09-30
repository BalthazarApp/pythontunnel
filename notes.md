v2 is in place as a separate set of scripts, with `enter_new_flow_run` examples in both the script and the notebook. v1 is untouched and still passing.

## The new files

```
flows/tunnel_session_server.py   the v2 flow (port 8766)
session_tunnel/
├── balthazar.py                 v2 client — contexts, context-aware globals, plt.show capture
├── demo_sessions.py             6 examples, script form
└── demo_sessions.ipynb          same, one example per cell, inline plots
```

Run the demos from `session_tunnel/` so `import balthazar` picks up the v2 shim rather than the v1 one a level up. Both tunnels can run side by side on different ports.

## The examples

1. **One context** — `plt.show()` and `blt.output[...]` land on the open run; prints `blt.params`/`blt.devices` from inside to show they reflect *that* run
2. **Nested contexts** — a parent run with one child per device, using `blt.parent()`/`blt.parents()`
3. **Device writes from inside a run** — the measurement pattern, attributed to the run that produced them
4. **Exception inside the block** → run FAILED with the message attached, then re-raised to you
5. **`run.fail("why")`** — red run without an exception, plus manual `enter`/`exit`
6. **Recovery** — `reset_contexts()` for a stuck run

## What v2 forced

An open context spanning many requests changes the problem. The Runner's context is process-global (`blt.params`, `blt.output`, `blt.devices`, `blt.flow_run` are module attributes it rebinds), so two clients interleaving would write into each other's runs — hence single-owner locking. And a client dying mid-block leaves a run stuck in `RUNNING`, hence three backstops.

Client-side, the context-aware globals needed a module-level `__getattr__` (PEP 562); a plain assignment can't track the innermost context.

For FAILED status the server calls `__exit__(type(exc), exc, None)` directly with a synthesized exception rather than raising, so the recorded message is the client's rather than a leaked internal traceback.

## Verified

Script and notebook both run clean against the tunnel (notebook: 15 cells, 3 inline images, 0 errors), nesting reaches depth 2 and unwinds correctly, and every safety path fires:

| Path                                                       | Result                                     |
| ---------------------------------------------------------- | ------------------------------------------ |
| LIFO violation                                             | rejected with both run IDs named           |
| Second client during open context                          | `PermissionError` with depth and idle time |
| `reset_contexts()`                                         | closed 1, depth → 0                        |
| `atexit` on clean exit                                     | context closed, depth → 0                  |
| **Idle watchdog** (client `SIGKILL`ed, no atexit possible) | reclaimed after timeout, run marked FAILED |
| Subscript param write                                      | raises with the `update()` fix             |

I also re-ran all v1 tests after rewriting the shared test double — script demo, status regression, and notebook all still pass.

## Two things from your `demo.ipynb`

You'd run v1 against the live space, which is good news — it works there. Your extra cell is preserved untouched (16 cells, outputs intact); I didn't regenerate that file.

It also caught a real problem before you hit it: **your space has 65 devices, and the v2 nested example loops over all of them** — that would have created 65 child runs. Both v2 demos now start with `DEVICE_TYPE`, `DEVICE_NAME` and `MAX_BATCH_DEVICES` (default 2). Related: `devices[0]` on your space is a device named `dummy` of type `dummy`, so the demos now print which device they picked instead of silently using it.

One note on the earlier failed-run question — it's still unresolved, and the fix I made was to the mechanism (any sub-step failure can no longer redden a run) rather than to a specific trigger I could reproduce. If it recurs, the run detail page now carries the exact sub-step name.
---

# Runner 1.35.1 API migration (2026-09-30)

## What broke

`blt.api` no longer exists. Commit `b8def1b4a` ("[#4054] Parallel flow run context
Python API") dropped `add_class::<BalthazarApi>()` from
`crates/worker/src/python/mod.rs`. It shipped in **v1.35.1**; the platform monorepo is
now on 1.38.1. Both tunnels called `blt.api.store_visualizations(...)` and would have
died with `AttributeError: module 'balthazar' has no attribute 'api'`.

Two things changed, not one:

| | before | now |
| --- | --- | --- |
| entry point | `blt.api.store_visualizations(...)` | `blt.store_visualizations(...)` |
| payload | `(figure_id, filename, bytes)` tuples | `blt.VisualizationBuilder(filename, data, type=, figure_id=)` |
| returns | nothing | `list[VisualizationMeta]` (has `.id`) |

The Rust signature is `Vec<Py<PyVisualizationBuilder>>`
(`flow_run_context.rs:784`), strictly typed — tuples would have raised `TypeError`
even if `blt.api` had survived.

What did *not* change: module-level names are now bound methods of `blt.context`,
which the Runner rewrites in place when a run is entered (`mod.rs:262-300`). So
`blt.store_visualizations` inside an `enter_new_flow_run` block still lands on the
child run, and all the surrounding context logic in both tunnels held unchanged.

## `figure_id` is now a replace key

The old first tuple element became `figure_id`, and its meaning is load-bearing:
storing again under the same id **replaces** the previous visualization, and a batch
containing two items with the same id is **rejected outright** (the store is
transactional). The two tunnels want opposite defaults here:

- **v1** (`balthazar.py`) sends no `figure_id`. Every `new_flow_run(figures=)` makes a
  fresh run, so there is nothing to redraw over, and the old
  `getattr(fig, "number", index)` would collide the moment a bare `Figure()` (no
  `.number`) sat next to a pyplot figure numbered the same as its list position.
- **v2** (`session_tunnel/balthazar.py`) keeps sending the matplotlib figure number.
  Redrawing figure 1 across several `plt.show()` calls now leaves the run holding the
  latest frame instead of every intermediate one — which is what the session case
  wants. Open figure numbers are unique, so a batch never self-collides.

Both servers still guard the duplicate case themselves, so the error names the
offending filename rather than surfacing a bare transaction rejection.

## Name collision: `blt.context`

1.35.1 added a real `balthazar.context` — the `FlowRunContext` bound to the current
run, an **object**, not a callable. The v2 shim already exported a `context()`
helper returning tunnel bookkeeping. Keeping that name would make shim-tested code
fail on a real Runner with `'FlowRunContext' object is not callable`, which is exactly
the class of surprise the shim exists to prevent.

Renamed to `blt.tunnel_state()`. Accessing `blt.context` on the shim now raises
`NotImplementedError` pointing at the alternatives rather than a bare `AttributeError`.
Call sites updated in `demo_sessions.py` and `demo_sessions.ipynb` (3 cells).

## Unchanged after checking

`search_devices` in both shims already matches the current signature exactly
(`id, type, name, tags, offset, limit, archived`; `limit=0` means no limit). The
servers pass `limit`/`offset` only when truthy, so `0` correctly falls through to "no
limit". `FlowRunStatus`, `new_flow_run`, `enter_new_flow_run` and the module globals
are all unchanged.

`new_flow_run` *gained* a `visualization_ids` parameter, so the v1 no-context path
could finally carry plots (store first, then link by id) — the docstring claim that it
cannot is now only true of how we call it, not of the API. Not taken up; noted.

---

# Sketch: v3 session tunnel on `FlowRunContext`

**Not implemented.** This is the design note for it.

## Why it is now possible

`is_main_thread` appears in exactly two places in the Runner
(`flow_run_context.rs:404` and `:1745`) — both on `enter_new_flow_run` and on the
*entered*-context exit. `blt.new_flow_run_context()` has no such gate. Per its
docstring: *"Since nothing global is modified, contexts can be created and closed from
any thread, and any number of them can be open at the same time. Every call releases
the GIL while waiting for the server."*

That invalidates the two premises the current v2 design is built on:

1. The `ARCHITECTURE — why the job queue exists` block in both flow files. The queue
   exists solely because `enter_new_flow_run` asserts main-thread. A `FlowRunContext`
   can be created, written to and closed directly on the HTTP worker thread.
2. The single-owner locking (`_claim`, `_owner`, the `PermissionError` path). That
   exists solely because the Runner's context was process-global and two clients would
   have written into each other's runs. Contexts are now objects, so each client can
   hold its own independent stack and work concurrently.

## Shape

Replace the global `_stack` / `_owner` / `_last_seen` triple with a per-client record:

```python
_clients = {}   # client_id -> {"stack": [FlowRunContext], "last_seen": float}
_lock = threading.Lock()   # guards _clients only, not blt.* calls
```

- `_op_enter_flow_run` → `blt.new_flow_run_context(name=, flow_id=, devices=, parameters=)`,
  pushed onto that client's stack. Note it does **not** need `__enter__`; the run starts
  when the context is created.
- `_op_store_visualizations` / `_op_set_output` → `ctx.store_visualizations(...)`,
  `ctx.output.update(...)` on the client's innermost context instead of the globals.
- `_op_exit_flow_run` → `ctx.exit()` for success. The FAILED path is unchanged:
  `ctx.__exit__(type(exc), exc, None)` with a synthesized exception is still how you get
  the client's own message recorded.
- `_run_executor` and the whole `_Job` queue go away. Keep a small worker pool or just
  let `ThreadingHTTPServer` handle concurrency.

## What stays

- **The idle watchdog.** Still required, and arguably more so: *"Nothing closes a
  context automatically. A flow run whose context was never closed is left
  unfinished."* It just needs to sweep per-client rather than one global stack.
- **`reset_contexts`**, now naturally scoped per client with an "all clients" override.
- **LIFO enforcement** within a single client's stack.
- The auth/loopback/`blt.secrets` posture, unchanged.

## Caveats found while checking

- `print()`, `open()` and `matplotlib.pyplot` are captured from the *process*, so they
  always land on the root run no matter which contexts are open. Irrelevant here (the
  client renders SVG itself and ships bytes) but it would bite anyone expecting
  `ctx.print()` and `print()` to behave alike. Logs are the exception — a context log
  reaches its own run *and* all ancestors.
- **Device provenance matters, and this is the trap.** `commit` routes a param write
  through the `session_ref` the `Device` object carries (`object.rs:760`), and a Device
  carries the refs of the context it was obtained from. The name is misleading: for a
  child run, `session_ref` and `flow_run_ref` are created with the *same* run id
  (`flow_run_context.rs:290-291`), so the write does land on the current run.

  In v2 this is automatic — `enter_new_flow_run` *resets* the shared refs in place
  (`:340-341`), so devices fetched anywhere follow the entered run. A detached context
  instead creates **fresh** refs and only rebinds the devices handed to it
  (`rebind_devices`, `:292`). So in v3, `update_device_params` must resolve devices via
  `ctx.search_devices(...)` or from `ctx.devices`; a device fetched through the global
  `blt.search_devices` would silently attribute its write to the tunnel's own root run.
  This is the one place where the v3 rewrite is not a mechanical translation.
- The client shim keeps its module-level `__getattr__`; with per-client contexts the
  server just resolves "innermost" per `client_id` instead of globally.

---

# Cell source → Balthazar logs (2026-09-30)

Each run created from a notebook cell now carries that cell's code in its logs, so a
run in Balthazar shows what produced it.

## How the source is captured

Both shims register an IPython `pre_run_cell` hook at import
(`_install_cell_hook`), stashing `info.raw_cell`. Outside IPython —
`demo.py`, `demo_sessions.py`, any plain interpreter — `get_ipython()` returns None,
the hook is never registered, and `_take_cell_source()` keeps returning None. The
script demos are byte-for-byte unaffected.

The hook handler tolerates being called with no argument, which is how IPython
invoked `pre_run_cell` before 7.x.

Opt out with `blt.log_cell_source = False` (module-level, exported in `__all__`).
Truncated at 8000 chars client-side *and* server-side — the request body limit is
32/64 MB, so without the server-side cap an arbitrary client could bury a run's log
under a single paste.

## Where it lands, and why the two tunnels differ

**v2 (session):** sent on `enter_flow_run` and logged immediately after entering, so
each run reads top-down — code first, then what it produced. Sent on **every** context
the cell opens, including the innermost.

This was briefly "once per cell, on the outermost context" to avoid duplication. That
was the wrong trade: the innermost run is the one holding the plots, output and device
writes, so it is the run you open when something looks wrong, and it was the one run
with no code attached. The Runner propagates a child's log up into every ancestor, so
the cost of the fix is that an outer run in a nested cell now lists the same block once
per context below it — redundant, but redundancy in the parent beats absence in the
leaf.

If that noise becomes a problem, the precise fix is to defer: stash the source on the
frame at enter, clear the parent's copy when a child enters with the same source, and
flush it just before `_exit_top` (the context is still current there, so the log still
lands on the right run). That yields exactly one copy per leaf and none on interior
runs, at the price of the code appearing at the *end* of each run's log rather than the
start.

**v1:** sent on every run, because v1 runs are independent siblings under the tunnel
run rather than a tree — each should be able to explain itself. Each `create_flow_run`
is already the only child of its cell, so v1 needed no change.

## The v1 path change worth knowing about

A history entry made by `blt.new_flow_run` has no log stream and is never current, so
there is nowhere to write the source. The v1 shim therefore routes any run carrying a
cell source through `create_flow_run` (`enter_new_flow_run`) instead — the same switch
it already made for figures, now on either condition:

```python
if figures is None and cell_source is None:
    ...  # plain history entry
```

**So under Jupyter, v1 cells that previously produced a history entry now produce a
real entered run.** Visible result is the same shape (a child of the tunnel flow with
inputs, outputs and now logs); it differs in having a real duration rather than instant
timestamps. `blt.log_cell_source = False` restores the old path exactly.

`_op_create_flow_run` also now forwards `script_name`, which it silently dropped
before — previously invisible, but it would have become a real regression the moment
the plain path stopped being used.

## Verified

Driven through a real `InteractiveShell`, not just unit-called:

| Check | Result |
| --- | --- |
| Hook registers under IPython / not outside | `True` / `False` |
| v1 cell → op chosen | `create_flow_run`, exact cell source sent |
| v1 second cell | sends its own source, not the previous cell's |
| v2 one cell, 3 nested contexts | source on all three, innermost included |
| Toggle off | `_take_cell_source()` → None |
| 9000-char cell | truncated to 8050 with a `[truncated, 9000 chars]` marker |
| No-arg hook call (pre-7.x IPython) | no exception |
