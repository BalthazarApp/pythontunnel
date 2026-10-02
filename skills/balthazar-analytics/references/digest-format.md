# How to read schema-tool output (the digest format)

The schema tools are views over one **space digest** — a pure, deterministic summary built
from device, flow and run records. It describes **structure and coverage only**; the single
place any value appears is date `first`/`last` and run timestamps. Knowing the shape lets
you turn tool output straight into correct pandas code.

## Top-level digest shape

```jsonc
{
  "version": 1,
  "built_at": "2026-10-02T12:00:00Z",
  "totals": {"devices": 65, "device_types": 4, "flows": 9, "runs": 1830,
             "runs_truncated": false},
  "device_types": {
    "Wafer": {
      "count": 40,
      "fabrication_date": {"coverage": 100, "first": "2025-01-03", "last": "2026-09-12"},
      "tags": {"count_distinct": 3, "coverage": 25},
      "params": { "<dotted.path>": <field>, ... }
    }
  },
  "flows": {
    "IV sweep": {
      "id": "…", "run_count": 400, "runs_sampled": 400, "truncated": false,
      "status": {"FINISHED": 390, "FAILED": 10},
      "first_run": "…iso", "last_run": "…iso",
      "runs_with_plots": 380,
      "device_types": {"Wafer": 400},
      "declared_parameters": {"bias_max_v": {"type": "float", "description": "…"}},
      "inputs":  { "<dotted.path>": <field> },
      "outputs": { "<dotted.path>": <field> }
    }
  }
}
```

`overview()` is the condensed top (`totals`, type counts + param counts, flow run counts +
date ranges). `device_schema` / `flow_schema` return one `device_types` / `flows` entry.
`describe_param` / `describe_output` return one `<field>` plus its children.

## The `<field>` object — one dotted path

```jsonc
{"kind": "number" | "integer" | "string" | "bool" | "date" | "null" | "mixed"
         | "list" | "matrix" | "dict" | "map" | "other",
 "coverage": 82,                         // integer %: records where present and not None
 // kind-specific, all optional:
 "distinct": 12,                         // strings & bools only: COUNT of distinct values
 "length": {"min": 1, "max": 401},       // list
 "item_kind": "number",                  // list
 "shape": {"rows": [1, 8], "cols": [101, 101]},  // matrix
 "keys": ["a", "b"],                     // dict: child key names (<= 50); below a map, only structural ones
 "other_keys": 58,                       // dict below a map: data-like keys dropped from "keys"
 "key_count": 230,                       // map: number of distinct keys seen
 "value": <field>,                       // map: merged shape of the values (absent on a nested map)
 "unit": "us"}                           // if a sibling key unit/units holds a string
```

### What each kind means for your code

| `kind` | What it is | How to pull it |
|---|---|---|
| `number` / `integer` | scalar numeric leaf | a float/int column; plot directly |
| `string` | scalar text; `distinct` is a **count**, never the values | category column; group/filter by it |
| `bool` | boolean | boolean column |
| `date` | ISO-8601 datetime; may carry `first`/`last` | parse with `pd.to_datetime(..., errors="coerce")` |
| `list` | a flat list; `length` + `item_kind` describe it | object column → `series_to_df(df, col)` |
| `matrix` | list of equal-length numeric lists; `shape` has row/col ranges | object column → `matrix_to_df(df, col)` for a heatmap |
| `dict` | small, stable key set; `keys` lists them | **flattened** into dotted columns (`path.child`) |
| `map` | many or data-like keys; `key_count` + `value` | **not flattened**; index the object column; `value` tells you the per-entry shape |
| `mixed` | kind varies across records; carries `kinds: {kind: count}` | decide one kind and coerce (`pd.to_numeric(..., errors="coerce")`) |
| `null` | always absent/None in the sample | no data — ignore the column |
| `other` | value matched no kind above (opaque object, `Decimal`, bytes, `np.bool_`) | rare; inspect the raw value before relying on it |

> Numeric kinds come from the `numbers` ABCs, so numpy scalars (`np.int64`, `np.float64`) are
> classified as `integer` / `number`, not `other`.

## Coverage, every time

`coverage` is the integer percentage of records where the path is present and non-null. A
path at `coverage: 30` has data in ~30% of rows — sparse by design (different device types
and flows populate largely disjoint paths). Before you rely on a column: check its
coverage, then drop NaNs for *that* column only. Do not filter `!= 0`; `0` can be a valid
measurement.

## Dict vs map — the flattening rule

- A **dict** with a small, stable key set (≤ 50 and consistent across records) is flattened
  into its own dotted paths, up to depth 4. These become columns in `devices_df` /
  `runs_df`: `hierarchy.lot`, `hierarchy.wafer`.
- A dict whose key set **exceeds 50**, or whose keys **differ so much across records that
  their union exceeds 50**, collapses to a **map**. Its children are summarized once under
  `value` and are *not* flattened. The classic case is `measurements.{run_key}`: many
  run-keyed sub-records, described by their merged `value` shape. In pandas it stays an
  object column — index into it, don't expect a column per key.
- **Keys below a map.** A map's own keys are data and are only counted (`key_count`). But its
  *entries* are often uniform records: their field names appear in `value.keys` when each
  occurs in **≥ 50% of the map's entries** and at most 50 survive — so `measurements.*` yields
  `value: {"kind": "dict", "keys": ["peak", "ts"]}`. Keys below that threshold are data-like
  and are dropped, counted as `other_keys`. If none qualify (or too many do), `value` is a
  **nested `map`** with only `key_count`. Use `value.keys` to know an entry's fields; never
  expect the map's own keys.

## Units

A `<field>` carries `unit` only when a sibling key named `unit`/`units` held a string. Use
it to label axes (`ylabel=f"T1 ({field['unit']})"`). When there is no `unit`, plot the raw
value and label with the path name; don't invent a conversion.

## What you will never see

No string value, no number value, no min/max/mean of numbers, and no *data-like* key below a
`map` — a map's own keys are only counted, and of its entries' keys only the structural ones
(shared by ≥ 50% of entries, within `max_keys`) are named; the rest are counted as
`other_keys`. The only values in the whole digest are date `first`/`last` and the run
timestamps. To see an actual value, pull it with `blt_analytics` and plot it.
