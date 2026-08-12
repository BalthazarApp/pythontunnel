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