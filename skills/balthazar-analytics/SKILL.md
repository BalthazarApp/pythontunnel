---
name: balthazar-analytics
description: Answer data questions about a Balthazar space as a coding agent. First call schema tools (overview, find, device_schema, flow_schema, describe_param, describe_output — from the balthazar-schema MCP server, or the `blt-schema` CLI) to learn exactly what devices, params, flows, runs, inputs and outputs exist, with dtypes, shapes and coverage. Then write Python with blt_analytics (devices_df, runs_df, explode_devices, series_to_df, matrix_to_df) to pull the real values into pandas and plot them with matplotlib, and call publish() to attach figures to a run only when the user asks to save or share. Covers coverage-aware columns, maps/matrices/mixed kinds, joining runs to devices, filtering failed runs, timestamps, caching and performance. Triggers include: analyze/plot/compare my devices or runs, yield, drift, KPI distribution, correlation, "what is in this space", schema tools, blt_analytics, devices_df, runs_df, prompt-to-plot over a Balthazar space.
---

# Answering data questions over a Balthazar space

You answer questions about a Balthazar space by learning its **structure** from schema
tools, then pulling the **real data** with `blt_analytics` and plotting it. Two layers,
clear division of labour:

- **Schema tools** (`overview`, `find`, `device_schema`, `flow_schema`, `describe_param`,
  `describe_output`, `load_snippet`) tell you *what exists* — names, dtypes, shapes,
  coverage. They return **schema only, never measurement values**, so you call them freely.
  Available as MCP tools from server **`balthazar-schema`**, or as the CLI
  `blt-schema <tool> [args…] [--json]` when MCP isn't wired up.
- **`blt_analytics`** (Python) fetches the *actual values* through the session tunnel into
  your process and gives you pandas frames. What you do with that data is your business.

The tunnel must be up. If anything fails to connect, run `blt-tunnel doctor` and see the
**balthazar-tunnel** skill.

## Core workflow

1. **`overview()`** — the lay of the land: totals, device types with counts and param
   counts, flows with run counts and date ranges. Read it first.
2. **Narrow down** with the other schema tools, taking **exact names and dotted paths from
   their output — never guess a name**:
   - `find(query)` — fuzzy match a user's words across device types, param paths, flow
     names and input/output paths. Your first move when the user is vague.
   - `device_schema(device_type)` — a type's param paths with kinds and coverage.
   - `flow_schema(flow)` — a flow's declared params, inputs, outputs, status breakdown.
   - `describe_param(device_type, path)` / `describe_output(flow, path)` — one path in full,
     with its child paths.
3. **`load_snippet(device_type=…, flow=…, columns=…)`** — starter `blt_analytics` code for
   the slice you're after. A good base to adapt.
4. **Write a script / notebook cell** with `blt_analytics` to pull the data:

   ```python
   from blt_analytics import devices_df, runs_df, explode_devices, series_to_df, matrix_to_df
   df = devices_df("Wafer", columns=["hierarchy.lot", "fwhm_arcsec"])   # projection
   ```

   Pass `columns=` to project only what you need, reuse the on-disk cache across calls, and
   pass `refresh=True` only when you must bypass it.
5. **Plot with matplotlib.** Label axes with the unit the digest reports. Put the sample
   size in the title.
6. **`publish(figs, name, devices=…)`** — attach the figures to a Balthazar run **only when
   the user asks to save or share**. Not for throwaway exploration.

Full signatures are in `references/api.md`. How to read tool output is in
`references/digest-format.md`. Worked end-to-end examples are in `examples/`.

## Reading schema output

Every path comes back as a small **field** describing one dotted path — its `kind`
(`number`, `integer`, `string`, `bool`, `date`, `list`, `matrix`, `dict`, `map`, `mixed`,
`null`), its **`coverage`** (integer % of records where the path is present and non-null),
and kind-specific extras (`distinct` count for strings/bools, `length`+`item_kind` for
lists, `shape` for matrices, `keys` for dicts, `key_count`+`value` for maps, `unit` when a
sibling says so). **Values never appear** except date first/last and run timestamps. Full
rules in `references/digest-format.md`.

## Rules that keep answers correct

- **Use exact names from the tools.** Device types, param paths, flow names and
  input/output paths are all reported verbatim — filter and project on those strings, never
  on a guess. If a slice comes back empty, re-check the name with `find` before concluding
  there is no data.
- **Be coverage-aware.** A column at 30% coverage has data in ~30% of rows; expect
  sparsity and NaNs. Check a path's coverage with `describe_param` / `device_schema` before
  you rely on a column, and drop NaNs for the specific columns you plot (don't filter
  `!= 0` — `0` can be a real measurement).
- **Mixed kinds.** A path whose kind varies across records comes back as `mixed` with a
  `kinds` histogram. Decide which kind you mean and coerce (e.g.
  `pd.to_numeric(col, errors="coerce")`) rather than assuming one type.
- **Maps vs dicts.** A dict with a stable, small key set is **flattened** into dotted
  columns (`hierarchy.lot`). A dict with many or data-like keys is collapsed to a **map**
  (`measurements.{run_key}`): its keys are *not* flattened, and the digest describes the
  merged shape of its **values** under `value`. Reach map values in Python by indexing the
  object column, not by expecting a column per key.
- **Matrices and series.** A `matrix` path is a list of equal-length numeric lists; a
  `list` path may be a swept series. Use `matrix_to_df(df, column)` → long `(row, col,
  value)` for a heatmap, and `series_to_df(df, column)` → long frame for a line.
- **Timestamps.** `created_time`, `started_time`, `finished_time` are datetimes;
  `runs_df` also gives `duration_s`. Parse with `pd.to_datetime(..., errors="coerce")` for
  a time axis and sort before plotting a trend.
- **Filter failed runs.** Drop failures before a physics/quality plot:
  `runs = runs[~runs["status"].astype(str).str.contains("fail", case=False, na=False)]`,
  or let `runs_df(flow=…, status=…)` do it for you (`status=`/`since=` are applied
  client-side after the runs are fetched, not pushed to the server).
- **Join runs ↔ devices.** `explode_devices(runs)` gives one row per `(run, device_id)`;
  merge it onto `devices_df(...)` with `left_on="device_id", right_on="id"` to correlate a
  run output against a device param.

## Performance

- **Project with `columns=`.** `devices_df` / `runs_df` default to every scalar leaf; pass
  the handful of dotted paths you actually plot. For `devices_df` over the tunnel the
  top-level param keys are pushed down (fewer bytes); run columns and nested paths are
  projected in your process after fetch.
- **Never loop per device.** One `devices_df(...)` or `runs_df(...)` fetches the whole set;
  N per-device searches are N round trips.
- **Lean on the cache.** Frames are cached on disk per space (default 1 h TTL). Re-running
  a cell is cheap. Use `refresh=True` only when the space changed under you.
- `runs_df(flow=None)` pages across *every* flow — scope to one `flow=` (and `max_runs=`)
  while iterating, widen once the plot is right.

### Big spaces: the server-side device cache

On a space with hundreds of thousands of devices the tunnel flow can keep a **server-side
device cache** (SPEC §6), which `devices_df` uses automatically — no API change:

- When the cache is `"ready"`, `devices_df(...)` reads the whole (optionally
  type-filtered) set from it in one shot instead of paging `search_devices`. If it is cold
  or the Runner is too old, behaviour is unchanged. `overview()` lists the configured
  indexes under `device_indexes` (`{name: dotted path}`).
- **`devices_df(index="wafer", value="W123")`** is a shortcut for "every device whose
  `wafer` index equals `W123`", served straight from the cache's index — far cheaper than
  pulling the whole frame and filtering. The index names come from `overview()`'s
  `device_indexes`. This path is **tunnel-only** (a clear error on a real Runner).
- **These paths bypass the on-disk frame cache** (the server already holds the
  authoritative, write-through copy). So `refresh=` behaves differently here: on the
  whole-frame cache path it is a no-op (the server cache is managed server-side); on the
  `index=`/`value=` path `refresh=True` re-fetches only the **already-known** device ids
  for that value (it will not surface brand-new devices). The **first** cache load can take
  minutes; a call that triggers it blocks. To pick up newly-added devices, ask the tunnel
  to reload (`import balthazar as blt; blt.refresh_device_cache()`), then call `devices_df`
  again. See the **balthazar-tunnel** skill for cache configuration and status.

## Presenting results

Prefer **plots and small aggregates**. Dumping a huge DataFrame into the chat is expensive
(it burns context) and rarely what the user wants — show the chart, or a `head()` /
`groupby(...).agg(...)` summary, and describe what it shows. This is good practice, not a
prohibition: the data in your process is the user's to use.

Attach figures to Balthazar with `publish(...)` **only when asked** to save or share.

## Reference files

- `references/api.md` — full `blt_analytics` + schema-tool API.
- `references/digest-format.md` — how to read the digest / field format (kinds, coverage,
  map, matrix, mixed).
- `examples/` — worked question → tool-calls → code examples.
