# API reference — schema tools and `blt_analytics`

Everything here is defined in the analytics interface spec. Schema tools return
**JSON-serializable dicts, schema only** (no measurement values). `blt_analytics`
functions return **pandas objects with real values**.

---

## Schema tools

Available two ways, same names and arguments:

- **MCP** — server `balthazar-schema` (stdio). One tool per function below. Run it with
  `blt-schema-mcp`.
- **CLI** — `blt-schema <tool> [args…] [--json]`. Pretty text by default; add `--json` for
  the raw dict (what the MCP tool returns).

An unknown name never raises — it returns `{"error": "...", "suggestions": [closest names]}`.
Listings over 100 items are truncated and report `"truncated": n`.

| Tool | Signature | Returns |
|---|---|---|
| `overview` | `overview()` | Totals; device types with counts and param counts; flows with run counts and date ranges. On the tunnel, also `device_indexes` (`{name: dotted path}`) when a server-side device cache is configured — the index names for `devices_df(index=…, value=…)`. The first call. |
| `find` | `find(query, limit=20)` | Fuzzy matches across device types, param paths, flow names, input/output paths: `{"query": …, "matches": [{"kind": "device_type"\|"device_param"\|"flow"\|"flow_input"\|"flow_output", "device_type"\|"flow": …, "path": …, "score": …}]}`. Echoes the `query`, and adds `"truncated": n` when more than `limit` matched. |
| `device_schema` | `device_schema(device_type)` | The device-type digest entry: count, `fabrication_date`/`tags` coverage, and `params` (top-level paths first). |
| `describe_param` | `describe_param(device_type, path)` | One param field plus its child paths. `path` is the **bare** dotted param path (e.g. `measurements`, `hierarchy.lot`). Call before indexing a nested param or plotting a matrix. |
| `flow_schema` | `flow_schema(flow)` | Flow by **name or id**: declared parameters, `inputs`, `outputs`, status breakdown, run count, date range, runs-with-plots. |
| `describe_output` | `describe_output(flow, path)` | One flow output field plus its child paths. `path` is the **bare** output path (e.g. `iv_curve`) — the `output.`/`param.` prefixes belong only to `runs_df` columns, not here. |
| `load_snippet` | `load_snippet(device_type=None, flow=None, columns=None)` | `{"code": "<python using blt_analytics>"}` — starter code to adapt. |
| `get_digest` | `get_digest(refresh=False)` | The whole space digest (see `digest-format.md`). Memoized; `refresh=True` rebuilds. |

CLI examples:

CLI subcommands are **hyphenated** (the Python functions keep underscores):

```bash
blt-schema overview --json
blt-schema find "yield" --limit 10 --json
blt-schema device-schema Wafer --json
blt-schema describe-param Wafer measurements --json      # bare param path
blt-schema flow-schema "IV sweep" --json
blt-schema describe-output "IV sweep" iv_curve --json     # bare output path, no "output." prefix
blt-schema load-snippet --device-type Wafer --flow "IV sweep" --columns yield_pct --json
```

---

## `blt_analytics` — the data path (pandas)

Import from the package. It uses only attributes common to the shim and the real Runner.
The only projection pushed down the tunnel is `devices_df`'s set of **top-level** param
keys (derived from `columns=`); everything else — nested dotted projection, run
`status`/`since`, `max_runs` — is applied **client-side** in your process after the data
arrives.

```python
from blt_analytics import (
    devices_df, runs_df, explode_devices, series_to_df, matrix_to_df, publish,
)
```

### `devices_df(device_type=None, columns=None, *, include_archived=False, refresh=False, index=None, value=None) -> DataFrame`

One row per device.

- Identity columns always present: `id`, `name`, `type`, `fabrication_date`
  (`datetime64`), `tags`.
- `columns=None`: every scalar leaf is flattened to a dotted column (e.g.
  `hierarchy.lot`). Anything under a dict with more than 50 keys (a **map**) is skipped;
  lists and leftover dicts stay as object columns named by their path.
- `columns=[…]`: only those dotted paths. A path that is missing becomes an all-NaN
  column (so a typo looks like "no data" — re-check names with `find`/`device_schema`).
- `device_type=None`: all device types.
- **`index=`, `value=`** (tunnel-only, SPEC §6): pass both to fetch exactly the devices
  whose configured index equals `value` — e.g. `devices_df(index="wafer", value="W123")`.
  Index names come from `overview()`'s `device_indexes`. Raises a clear error on a real
  Runner. Passing only one of the two raises `ValueError`.

**Server-side device cache.** On the bridge, when the flow configures a device
cache that is `"ready"`, `devices_df` reads from it (whole-frame via `cached_devices_query`,
or one index value via `cached_devices`) instead of paging `search_devices`; otherwise the
behaviour is unchanged. These cache-backed paths **bypass the on-disk frame cache** (the
server holds the authoritative, write-through copy), so `refresh=` differs there: it is a
no-op on the whole-frame path, and on the `index=`/`value=` path `refresh=True` re-fetches
only the **already-known** ids of that value (not newly-added devices — reload server-side
for those with `blt.tunnel.refresh_device_cache()`). The first load can take minutes. See
the **balthazar-tunnel** skill for configuring and inspecting the cache.

### `runs_df(flow=None, *, columns=None, status=None, since=None, max_runs=100_000, refresh=False) -> DataFrame`

One row per flow run (wide).

- Columns: `run_id`, `flow_id`, `flow_name`, `status`, `created_time`, `started_time`,
  `finished_time`, `duration_s`, `username`, `tags`, `device_ids`, `visualization_ids`,
  plus `param.<path>` (inputs) and `output.<path>` (outputs), flattened by the same rules
  as `devices_df`.
- `flow=`: a flow **name or id** (like `flow_schema`). `flow=None`: every flow, paged per
  flow with shrink-and-retry, built in chunks. Scope to one `flow=` while iterating — it is
  much cheaper.
- `status=` (a string or list) and `since=` (a datetime / parseable timestamp bounding
  `created_time`) filter the runs **client-side**, after they are fetched; `max_runs=` caps
  the pull. The tunnel pages the raw run history — it does not filter server-side.

### `explode_devices(runs) -> DataFrame`

One row per `(run, device_id)` from a `runs_df` frame — the join bridge to devices. Merge
onto `devices_df(...)` with `left_on="device_id", right_on="id"`.

### `series_to_df(df, column, *, index_name="i") -> DataFrame`

Explode a list-valued column into a long frame (one row per element, with `index_name` as
the position). For swept/series values.

### `matrix_to_df(df, column) -> DataFrame`

Explode a matrix-valued column (list of equal-length numeric lists) into a long
`(row, col, value)` frame — ready for a heatmap.

### `publish(figures, name, *, devices=None, parameters=None, output=None) -> str`

Open a new flow run named `name`, make the given `figures` the ones captured, call
`plt.show()`, set `blt.output` from `output`, and return the new `flow_run_id`. It does
**not** close or destroy your figures: for the duration of the `plt.show()` call it
temporarily restricts what pyplot sees to just `figures`, then restores the rest — any
other figures you still hold survive untouched. Works on the shim and the real Runner.
**Call only when the user asks to save or share.**

```python
fig, ax = plt.subplots(); ax.plot(x, y)
run_id = publish(fig, "Yield by lot", devices=[dev], output={"median_yield": 0.91})
```

---

## Caching

Frames are cached on disk per space under `~/.cache/blt_analytics/<space_key>/<name>.pkl`
(parquet if pyarrow is installed). Default TTL 1 h. `refresh=True` on any frame call
bypasses and rewrites the entry.

---

## Notes

- The schema tools' digest is injectable for tests (`schema.set_digest` / `schema.reset`),
  irrelevant to normal use.
- `runs_df(flow=)` accepts **either a flow name or a flow id** (confirmed in `frames.py`,
  matching `flow_schema(flow)`); an unresolved value is treated as a raw id.
