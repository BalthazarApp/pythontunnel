# Working with Balthazar data in this folder

The user is a scientist, not a programmer. Do the work for them: write and run Python
scripts in this folder, show results as tables and plots, and explain them in plain
language. Don't ask them to install tools, configure anything, or read code.

## Connecting

Every script starts with:

```python
from connect import blt, ba
```

- `blt` is the live Balthazar API for the user's space. It behaves exactly like
  `import balthazar as blt` inside a Balthazar flow.
- `ba` turns devices and flow runs into pandas DataFrames.
- The first run opens a browser to sign in, so tell the user to expect that.
- If `connect` fails with "Put the bridge address…" or an HTTP 404 / "not running" error,
  the bridge is not reachable. Tell the user to ask their Balthazar contact for a current
  address and paste it into `bridge_url.txt`. Don't try to fix it yourself.
- A 403 error means the user isn't allowed on this bridge. The same contact has to add
  them.

## The other skills in this folder

`balthazar-tunnel` and `balthazar-analytics` go deeper: run contexts, plots, device writes,
DataFrame conventions and worked examples. Here they work with two differences:

- Connect with `from connect import blt, ba` (above), never with `blt-tunnel connect`.
- The schema tools (`overview`, `device_schema`, `find`, …), the MCP server and the
  `blt-schema` / `blt-tunnel` commands are **not available** in this folder. Explore with
  `blt.search_devices`, `blt.search_flows` and `ba.devices_df` instead, as shown below.

## Exploring the space

```python
devices = blt.search_devices(limit=20)              # a sample of devices
for d in devices:
    print(d.type, d.name, list(d.params)[:10])     # params is a dict, often nested

wafers = blt.search_devices(type="Wafer")           # filter on type, name (wildcards * ?), tags
flows = blt.search_flows()                          # the flows (procedures) in the space
runs = blt.search_flow_run_history(flow_id=flows[0].id, limit=50)
for r in runs:
    print(r.status, r.created_time, dict(r.output))   # run inputs: r.params, results: r.output
```

Start by finding out what exists: device types, a few devices of each type and their
param keys, the flows and the outputs of their runs. Use the exact names you find. Never
guess param names.

## DataFrames

```python
df = ba.devices_df("Wafer")                          # one row per device; nested params become
                                                     # columns like "hierarchy.lot"
df = ba.devices_df("Wafer", columns=["hierarchy.lot", "yield_pct"])   # only what you need
runs = ba.runs_df("IV sweep", max_runs=1000)         # one row per run: run_id, status,
                                                     # created_time, "param.<x>", "output.<y>"
runs = runs[runs["status"] == "FINISHED"]            # usually drop failed runs
pairs = ba.explode_devices(runs)                     # one row per (run, device), to join on devices
```

Check how many values a column actually has (`df[col].notna().mean()`) before relying on
it. Spaces are often messy.

## Plotting

Use matplotlib. Save every figure as a PNG in this folder, and show or describe it to the
user:

```python
import matplotlib.pyplot as plt
ax = df.groupby("hierarchy.lot")["yield_pct"].mean().plot.bar()
ax.set_ylabel("yield (%)")
plt.tight_layout(); plt.savefig("yield_per_lot.png")
```

Only when the user asks to save results in Balthazar, attach them to a new flow run:

```python
import matplotlib.pyplot as plt          # import before the run starts
with blt.enter_new_flow_run(name="Yield per lot"):
    df.groupby("hierarchy.lot")["yield_pct"].mean().plot.bar()
    plt.show()                           # the figure is stored on this run
    blt.output["n_wafers"] = len(df)     # outputs: numbers, text, booleans
```

## Rules

- Read-only by default. Never change device params, tags or anything else in Balthazar
  unless the user explicitly asks for it, and confirm with them first.
- Large spaces can have many devices. Filter with `type=`, `name=` or `limit=` instead of
  fetching everything.
- A single call can take up to a minute; that is normal.
