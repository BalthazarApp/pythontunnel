# Analytics tools: interface spec

The shared contract for the analytics work on branch `feature/analytics-tools`. Several
agents build against it in parallel, and each owns a disjoint set of files (see
"Ownership"). If you need to deviate from a contract here, say so in your report rather
than silently changing it.

## Goal in one paragraph

A coding agent (Claude Code, Cursor, Copilot) answers data questions about a Balthazar
space. It calls **schema tools**, which return names, types, shapes and coverage, never
measurement values, to learn what exists. Then it writes Python against
**`blt_analytics`**, which pulls real data through the v2 session tunnel into the user's
process. **Skills** document that workflow. What the user does with data in their own
process is their business. The tools are schema-only by construction.

## Decisions already made

- Builds on the **v2 session tunnel**. v1 stays untouched.
- Tools return **schema only**: type and path names, dtypes, shapes, counts, coverage %,
  date ranges, distinct-value *counts*. No measurement values, and no identity listings
  (device names, run keys, string values).
- Tools are offered **both** as an MCP server (stdio) and as a CLI, both thin wrappers
  over the same Python functions.
- Install is an **editable install from the repo clone** (`uv pip install -e ".[all]"`).
- Plots use **matplotlib** (they attach to runs through the existing `plt.show()` hook).
- Existing behaviour of the shim and server is unchanged. All additions are additive.
  Read ops already don't call `_claim`, so reads are already allowed while another client
  owns the context stack. Keep it that way.

## Layout

```
pyproject.toml                       package `blt_analytics`; extras [analytics] [mcp] [dev] [all]
flows/tunnel_session_server.py       + new read ops (below)
session_tunnel/balthazar.py          + matching client functions (below)
blt_analytics/
  __init__.py                        re-exports the public API
  _blt.py                            locate the blt module (shim or real), detect tunnel mode
  digest.py                          STDLIB-ONLY: build the space digest from plain records
  schema.py                          tool functions over the digest
  frames.py                          devices_df, runs_df, series/matrix helpers, paging
  cache.py                           on-disk cache per space
  publish.py                         figures -> flow run
  mcp_server.py                      stdio MCP server exposing schema.* as tools
  cli.py                             `blt-schema` and `blt-tunnel` entry points
skills/
  balthazar-tunnel/SKILL.md
  balthazar-analytics/SKILL.md (+ references/, examples/)
tests/
  fakes/fake_blt.py                  fake *real* balthazar module (Runner side)
  fakes/fixture_space.py             deterministic fixture space (devices, flows, runs)
  conftest.py
  test_*.py
docs/analytics/SPEC.md               this file
```

## 1. Records (wire format)

The server turns `blt` objects into plain JSON records. `digest.py` consumes these
records, so the digest is computed the same way on the Runner and locally.

**Device record** (existing `_device_to_dict`, unchanged):
`{id, name, type, description, fabrication_date (iso|None), tags: [str], params: dict}`

**Flow record**:
`{id, name, description, branch, script_filename, tags: [str], created_time (iso),
username, parameters: {name: {type, default, description}}}`. Take the fields from the
stub's `Flow` / `FlowParameterMetadata`. Omit any field the object lacks.

**Run record**:
`{id, flow_id, flow_name, status (str), created_time, started_time, finished_time (iso|None),
username, tags: [str], comment, device_ids: [str], params: dict, output: dict,
visualization_ids: [str]}`

Canonical API stub (real attribute names):
`/Users/oscar.hesselberth/Balthazar/proto-fe/crates/worker/resources/balthazar.py`
- `search_flows`, around line 1680
- `search_flow_run_history`, around line 2240
- `FlowRunBase` / `OneshotFlowRun`, around lines 2068 and 2161
- `fetch_visualizations`, around line 2594

## 2. New server ops (`flows/tunnel_session_server.py`)

All ops are read-only. They call `_last_seen_touch(kwargs)` and never `_claim`, and they
are added to `_DISPATCH`.

| op | kwargs | returns |
|---|---|---|
| `search_devices` (extended) | existing + `keys: [str]\|None` (keep only these top-level param keys), `scalars_only: bool=False` (drop dict/list param values), `include_params: bool=True` | `[device record]` |
| `search_flows` | `name`, `flow_ids`, `tags`, `limit=1000`, `offset=0` | `[flow record]` |
| `search_flow_runs` | `flow_id`, `device_id`, `flow_run_ids`, `limit=250`, `offset=0`, `include_params=True`, `include_output=True` | `[run record]` |
| `fetch_visualizations` | `ids: [str]` (at most 20 per call, else `ValueError`) | `[{id, type, filename, flow_run_id, timestamp, data_b64}]` |
| `space_schema` | `refresh: bool=False`, `max_runs_per_flow: int=2000` | digest (section 3), cached in server memory until `refresh` |

`space_schema` imports `blt_analytics.digest`. The flow file is in `flows/` and the
package sits at the repo root, so insert the parent of the flow file's directory into
`sys.path` before importing. If the import fails, raise `RuntimeError("space_schema
unavailable on this Runner: …")`. The client then falls back to building the digest
locally (section 4). Runs are fetched per flow, with `limit=250` pages, and are capped by
`max_runs_per_flow`.

Paging robustness, ported from prompt_plotting `_page_flow_runs`: on a failed page,
retry the same offset with a quarter of the page size. At page size 1, skip the run. After
3 consecutive skips, abandon that flow. Dedupe by id, and advance the offset by the raw
page length.

## 3. Space digest (`blt_analytics/digest.py`, stdlib only)

```python
def build_digest(devices: list[dict], flows: list[dict], runs: list[dict], *,
                 built_at: str | None = None, max_keys: int = 50, max_depth: int = 4) -> dict
```

Pure and deterministic: sorted keys, and the same input gives byte-identical
`json.dumps(sort_keys=True)`. Shape:

```jsonc
{
  "version": 1,
  "built_at": "2026-10-02T12:00:00Z",
  "totals": {"devices": 65, "device_types": 4, "flows": 9, "runs": 1830, "runs_truncated": false},
  "device_types": {
    "Chip": {
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
      "device_types": {"Chip": 400},
      "declared_parameters": {"bias_max_v": {"type": "float", "description": "…"}},
      "inputs":  { "<dotted.path>": <field> },
      "outputs": { "<dotted.path>": <field> }
    }
  }
}
```

`<field>` describes one path. Coverage is the integer percentage of records where the
path is present and not None:

```jsonc
{"kind": "number" | "integer" | "string" | "bool" | "date" | "null" | "mixed"
         | "list" | "matrix" | "dict" | "map" | "other",
 "coverage": 82,
 // kind-specific, all optional:
 "distinct": 12,                        // strings and bools only: COUNT of distinct values, never the values
 "length": {"min": 1, "max": 401},      // list
 "item_kind": "number",                 // list
 "shape": {"rows": [1, 8], "cols": [101, 101]},   // matrix: list of equal-length numeric lists
 "keys": ["a", "b"],                    // dict: child key names (<= max_keys); below a map, only the structural ones
 "other_keys": 58,                      // dict below a map: count of data-like keys dropped from "keys"
 "key_count": 230,                      // map: dict with too many or data-like keys, collapsed
 "value": <field>,                      // map: merged shape of its values (absent on a nested map, see below)
 "unit": "us"}                          // only if a sibling key named unit/units holds a string
```

Rules:
- Dict children are flattened into their own dotted paths, up to `max_depth`.
- A dict whose key set exceeds `max_keys` becomes `"map"`, and so does a dict whose keys
  differ across records so much that their union exceeds `max_keys`. Its children are
  summarized under `value` and are not flattened.
- A path whose kind varies across records gets `"mixed"` plus `"kinds": {kind: count}`.
- Numbers are classified through the `numbers` ABCs, so a numpy scalar (`np.int64`,
  `np.float64`) is `integer`/`number`, not a string. Any value that is none of the kinds
  above — an opaque object, `Decimal`, bytes — is `"other"` (its kind is reported; the value
  never is), rather than being silently mislabelled a string.
- Date-like strings (ISO 8601) count as `date`. Date fields and the run timestamps may
  report first/last. That is the only place where values appear, and those values are
  timestamps.
- Never emit a string value, a number value, or min/max/mean of numbers.
- **Keys below a collapsed `map`.** A map's own keys are data (run ids, wafer ids) and are
  never emitted — only counted as `key_count`. But a map's *entries* are often uniform
  records whose field names are genuine structure worth reporting. So, below a map, an inner
  dict's keys are emitted (under `value.keys`) only when each emitted key occurs in **at
  least 50% of the map's entries** *and* the surviving set is `<= max_keys`; keys that fall
  below the threshold are data (a value smuggled in as a key) and are dropped, their number
  reported as `other_keys`. If no key passes, or too many do, the entries are themselves
  map-like and the `value` collapses to a **nested `map`** carrying only `key_count` (no
  `keys`, no further `value`). At the depth budget (`depth >= max_depth`) keys are never
  listed below a map either — only `key_count`; outside a map at the depth budget the child
  key names *are* structural paths and remain allowed. The rule recurses at each level. This
  keeps structural field names (`peak`, `ts`) useful while provably hiding data-like keys.

## 4. Client functions

### Shim (`session_tunnel/balthazar.py`)

These mirror the names on the real Runner module, so code is portable:
- `search_devices(..., keys=None, scalars_only=False, include_params=True)`. The new
  kwargs are **shim-only**; document that.
- `search_flows(*, name=None, flow_ids=None, tags=None, limit=1000, offset=0) -> list[Flow]`
- `search_flow_run_history(*, flow_id=None, device_id=None, flow_run_ids=None, limit=250, offset=0) -> list[FlowRun]`
- `fetch_visualizations(ids) -> dict[str, Visualization]`. Batch into chunks of 20, and
  set `.data` to the decoded bytes.
- `tunnel_space_schema(refresh=False) -> dict` (shim-only, hence the prefix)

`Flow`, `FlowRun` and `Visualization` are light read-only classes whose attributes match
the stub (`id`, `name`, `flow_id`, `flow_name`, `status`, `created_time` as `datetime`,
`params`, `output`, `visualization_ids`, …). `FlowRun.devices` is not fetched; expose
`device_ids` instead. Add them to `__all__`.

### `blt_analytics._blt`

- `get_blt()` returns the module to call. It is the importable `balthazar` if that is the
  real one or the v2 shim (`__balthazar_tunnel__` plus `tunnel_state`). Otherwise it loads
  `session_tunnel/balthazar.py` by file path: either `$BLT_TUNNEL_SHIM` or
  `<repo>/session_tunnel/balthazar.py`, found relative to the package. Raise a clear error
  if the v1 shim was picked up.
- `is_tunnel()` reports whether that module is the shim, which is where projection kwargs
  are allowed.

### `blt_analytics.schema` (the tool functions)

Every function returns a JSON-serializable dict. An unknown name returns
`{"error": "...", "suggestions": [closest names via difflib]}` and does not raise.
Listings over 100 items are truncated and report `"truncated": n`.

```python
get_digest(refresh=False) -> dict      # tunnel space_schema; on RuntimeError falls back to fetching
                                       # records via shim and digest.build_digest locally; memoized
overview() -> dict                     # totals, device types with counts and param counts, flows with run counts and date ranges
device_schema(device_type) -> dict     # the device-type entry, top-level paths first
describe_param(device_type, path) -> dict   # one field plus its child paths
flow_schema(flow) -> dict              # flow by name or id: declared params, inputs, outputs, status...
describe_output(flow, path) -> dict
find(query, limit=20) -> dict          # fuzzy match on device types, param paths, flow names, input/output paths
                                       # -> {"matches": [{"kind": ..., "device_type"|"flow": ..., "path": ..., "score": ...}]}
                                       # kind is exactly one of: "device_type", "device_param", "flow", "flow_input", "flow_output"
load_snippet(device_type=None, flow=None, columns=None) -> dict   # {"code": "<python using blt_analytics>"}
```

The digest is injectable for tests: `schema.set_digest(d)` / `schema.reset()`.

### `blt_analytics.frames`

The data path, built on pandas. It uses only attributes common to the real module and the
shim, and passes projection kwargs only when `is_tunnel()`.

```python
devices_df(device_type=None, columns=None, *, include_archived=False, refresh=False) -> pd.DataFrame
#   identity cols: id, name, type, fabrication_date (datetime64), tags
#   columns=None: every scalar leaf flattened to a dotted column ("hierarchy.lot"),
#                 skipping anything under a dict with more than 50 keys; lists and leftover
#                 dicts stay as object columns named by their path
#   columns=[...]: only those dotted paths (a missing path becomes an all-NaN column)
runs_df(flow=None, *, columns=None, status=None, since=None, max_runs=100_000, refresh=False) -> pd.DataFrame
#   flow: name or id, like flow_schema
#   cols: run_id, flow_id, flow_name, status, created_time, started_time, finished_time,
#         duration_s, username, tags, device_ids, visualization_ids,
#         "param.<path>", "output.<path>"  (same flattening rules)
#   flow=None: every flow, paged per flow (search_flows, then search_flow_run_history per
#   flow_id), with shrink-and-retry; builds the frame in chunks of 2000 records
explode_devices(runs) -> pd.DataFrame   # one row per (run, device_id)
series_to_df(df, column, *, index_name="i") -> pd.DataFrame   # long frame of list-valued column
matrix_to_df(df, column) -> pd.DataFrame                       # long frame (row, col, value)
```

### `blt_analytics.cache`

Pickle files under `~/.cache/blt_analytics/<space_key>/<name>.pkl`. Use parquet if pyarrow
is installed, but don't require it. `space_key` comes from the tunnel `ping()` (root
`flow_id` + session id are unstable, so prefer the space id from `blt` if available;
otherwise hash the tunnel URL plus the root flow id). `refresh=True` bypasses the cache.
Each entry has a configurable TTL (default 1 h).

### `blt_analytics.publish`

```python
publish(figures, name, *, devices=None, parameters=None, output=None) -> str  # returns flow_run_id
```
Opens `blt.enter_new_flow_run(name=..., devices=..., parameters=...)`, makes sure the given
figures are the ones shown (close the others, or show them one by one), calls
`plt.show()`, sets `blt.output`, and returns the run id. It works on the shim and on the
real Runner.

### MCP server and CLI (`mcp_server.py`, `cli.py`)

- MCP: official `mcp` SDK (`FastMCP`), server name `balthazar-schema`, stdio. One tool per
  schema function, with the same names. The docstrings are the tool descriptions, so write
  them for a model: say when to call each tool. Return JSON.
- `blt-schema <tool> [args…] [--json]` prints pretty text by default and JSON with `--json`.
- `blt-schema-mcp` runs the MCP server.
- `blt-tunnel setup [--agents claude,cursor,copilot] [--project DIR] [--global]` does three
  things, idempotently, and prints what it changed:
  1. writes or merges the MCP server entry (absolute path to the venv's `blt-schema-mcp`)
     into `.mcp.json` (Claude Code), `.cursor/mcp.json` (Cursor) and `.vscode/mcp.json`
     (Copilot; this file uses a `"servers"` key, not `"mcpServers"`)
  2. copies `skills/*` to `.claude/skills/` and `.agents/skills/` (and to `~/.claude/skills`
     etc. with `--global`)
  3. adds or updates a block in `AGENTS.md` between `<!-- blt-analytics:start -->` and
     `<!-- blt-analytics:end -->`
- `blt-tunnel doctor` checks the connection file, `ping`, `space_schema`, pandas,
  mcp, and the registrations, and prints pass/fail for each.

## 5. Tests

- **Use `pytest`, and no live space.** Run with `uv run pytest` from the repo root.
- `tests/fakes/fake_blt.py` is a fake *real* module (no `__balthazar_tunnel__`) with
  `search_devices`, `search_flows`, `search_flow_run_history`, `fetch_visualizations`,
  `info`/`warn`/`error`, `flow_run`/`session`/`flow` objects, and fault injection: make
  page N fail, make the run with id X always fail. It serves data from
  `fixture_space.py`.
- `fixture_space.py` is deterministic. It has about 4 device types, including one with a
  `measurements` map with many run keys, one with list and matrix params, and one with mixed
  kinds. It has 3 flows with runs that have params, outputs, a series output, a matrix
  output, failed runs and visualizations. Every numeric value is distinctive (for example
  `123.456789`) and every string value has a `SECRET_` prefix, so the leak tests can grep
  for them.
- Server ops are tested by inserting `fake_blt` as `sys.modules["balthazar"]`, importing
  the flow module, and calling `_op_*` directly.
- **Leak test (required):** for every schema tool called on every name in the fixture,
  `json.dumps(result)` contains no fixture numeric value and no `SECRET_` string.

## Ownership (parallel phase)

| Agent | Owns |
|---|---|
| Foundation (runs first) | `pyproject.toml`, `.gitignore`, `tests/fakes/*`, `tests/conftest.py`, `blt_analytics/__init__.py`, `blt_analytics/_blt.py`, `blt_analytics/digest.py` + its tests |
| Tunnel | `flows/tunnel_session_server.py`, `session_tunnel/balthazar.py`, `tests/test_server_ops.py`, `tests/test_shim_reads.py` |
| Tools | `blt_analytics/schema.py`, `mcp_server.py`, `cli.py`, `tests/test_schema*.py`, `tests/test_cli*.py` |
| Data | `blt_analytics/frames.py`, `cache.py`, `publish.py`, `tests/test_frames*.py`, `tests/test_cache*.py`, `tests/test_publish*.py` |
| Skills | `skills/**`, the README section on analytics |

No agent commits. The orchestrator reviews and commits.

## As-built deviations

Things the shipped code does differently from, or more specifically than, the contract
above. Recorded here so the spec and the code agree.

- **No `default` in `declared_parameters`.** The run/flow *record* still carries a declared
  parameter's `default` (§1), but `digest.build_digest` deliberately drops it: a default can
  be a measurement-shaped value, which has no place in a schema-only digest. A flow's
  `declared_parameters` entry is `{type?, description?}` only (see the §3 example, which
  shows no `default`).
- **`integer` and `number` are distinct kinds.** The digest classifies a Python `int` as
  `integer` and a `float` as `number` (and `bool` as `bool`), and does not coalesce them. A
  path that holds ints in some records and floats in others therefore reports `mixed` with a
  `kinds` histogram, not `number`. Consumers that want one numeric column should coerce
  (`pd.to_numeric(col, errors="coerce")`). `distinct` counts remain strings/bools only.
- **Numpy scalars classify as numbers; the `other` kind.** `digest._full_kind` classifies
  numerics through `numbers.Integral` / `numbers.Real` (ruling out `bool` first), so a numpy
  scalar (`np.int64` → `integer`, `np.float64` → `number`) is classified correctly without the
  stdlib-only module importing numpy. Anything that matches no kind — an opaque object,
  `Decimal`, bytes, `complex`, `np.bool_` — is the new kind `"other"` (its kind is reported,
  never the value), replacing the old silent fall-through to `string`.
- **Structural keys survive below a `map`; data-like keys do not.** The §3 absolute "no key
  below a map" is refined (see §3 "Keys below a collapsed `map`"): a map's own keys are only
  counted, but its entries' field names are emitted under `value.keys` when each occurs in
  ≥ 50% of the map's entries and at most `max_keys` survive; the rest are counted as
  `other_keys`; otherwise the `value` is a nested `map` with only `key_count`. The depth-cap
  branch lists no keys below a map. Implemented in `digest._map_value_dict`, threaded via an
  `under_map` flag.
- **`digest.flow_key(flows) -> {id: key}` is public, and the digest keys flows through it.**
  Flows are keyed by name, falling back to the flow `id` when the name is falsy (`None`/`""`),
  and when several flows share a name *every* colliding flow is keyed `"{name} [{id}]"` so none
  silently overwrites another (the old `out[flow.get("name", flow["id"])]` dropped collisions
  and mis-handled a present-but-`None` name). It is exported from `digest` so the Runner's
  `space_schema` op can map a flow id back to its digest key: `flows/tunnel_session_server.py`
  currently derives `name_by_id = {id: name}` itself to stamp `truncated`, which misses
  disambiguated keys — it should call `digest.flow_key(flow_records)` instead (Tunnel agent).
- **`status` / `since` are filtered client-side.** `runs_df`'s `status=` and `since=` are
  applied in the user's process *after* the runs are paged in; the tunnel and server page the
  raw run history and do not filter it. The only projection pushed over the tunnel is
  `devices_df`'s set of top-level param `keys=` (via `search_devices(keys=…)`); nested
  projection and run-column selection also happen client-side. The skills say so explicitly.
- **MCP SDK 2.x `MCPServer` fallback.** §4 names `mcp.server.fastmcp.FastMCP`. That class
  exists in mcp v1; v2 renamed it to `mcp.server.mcpserver.MCPServer`. `mcp_server._server_class`
  imports `FastMCP` and falls back to `MCPServer`, so the server runs on either SDK major.
- **Shared pager in `blt_analytics/paging.py`.** The shrink-and-retry run-history pager
  (§2 "Paging robustness") is a single stdlib-only helper, `paging.page_flow_runs(fetch_page,
  *, page_size=250, max_runs, on_skip=None) -> (runs, truncated)`. All three call sites use it:
  the server's `space_schema` (`flows/tunnel_session_server.py:_page_flow_runs`), `schema`'s
  local-build fallback (`_page_runs`), and `frames.runs_df` (`_page_flow`). The server imports
  it the same way it imports `blt_analytics.digest` (repo root on `sys.path`), and only from the
  `space_schema` path, so the basic read ops still work on a Runner without the package.

## 6. Server-side device cache and index accessors (added 2026-10-02)

Motivation: a space with 250k+ devices. Paging `search_devices` on every call is slow,
and `space_schema` fetches every device too. The data changes rarely.

### Configuration (the only space-specific part)

New flow parameter on `tunnel_session_server_flow`:
- `device_indexes`: JSON string, default `"{}"`. It maps an index name to a dotted param
  path, e.g. `{"wafer": "hierarchy.wafer"}`. Optionally the value is an object
  `{"path": "hierarchy.wafer", "device_type": "Die"}` to restrict the index to one type.
- `warm_device_cache`: bool, default `true` when any index is configured. Starts a
  background load at tunnel start.
- `device_cache_dir`: default `~/.balthazar_tunnel_cache`. The cache is persisted on the
  Runner at `<dir>/<space id or root flow id>/devices.pkl` (with a JSON sidecar for
  metadata) so a tunnel restart doesn't reload 250k devices. Write atomically
  (tmp + `os.replace`).

### Server behaviour

- **Store:** full device records (§1 format) by id, plus, for each configured index,
  `value -> [ids]`. The value is the param at the dotted path, compared as `str(value)`.
  Devices missing the path aren't indexed.
- **Full load:** paged `search_devices(limit=1000, offset=…)`, orchestrated like
  `space_schema`: many small executor jobs, so other clients' ops interleave and the
  watchdog keeps running. Each page is a job. Progress is tracked (`loaded`, `started_at`).
  Only one load runs at a time; concurrent callers wait for it.
- **Write-through:** `update_device_params` updates the cached record (and its index
  entries) after a successful write, so the cache never serves stale data written through
  the tunnel itself.
- `space_schema` builds from the device cache when it is loaded, instead of re-fetching.

### New ops (read ops: `_last_seen_touch`, never `_claim`)

| op | kwargs | returns |
|---|---|---|
| `device_cache_status` | none | `{state: "empty"\|"loading"\|"ready"\|"error", count, loaded, built_at, indexes: {name: {path, device_type, values}}, persisted_path, error}` |
| `refresh_device_cache` | `wait: bool=True` | status after the reload (a full reload; replaces the cache atomically on success, keeps the old one on failure) |
| `cached_devices` | `index`, `value`, `refresh=False` | `[device record]` for that index value. Unknown index → `ValueError` naming the configured indexes. If the cache is cold, it loads first (blocking). `refresh=True` re-fetches **that value's known device ids** via `search_devices(id=[…])` in chunks of 500, updates and re-indexes them, and drops ids that no longer exist. It **cannot discover newly added devices**; that needs `refresh_device_cache`. |
| `ping` (extended) | — | adds `device_indexes: {name: path}` |

### Shim

- `blt.cached_devices(index, value, *, refresh=False) -> list[Device]`: generic; returns the
  shim's existing `Device` objects, so `device.params.update(...)` keeps working.
- `blt.get_<index>_devices(value, *, refresh=False)`: generated dynamically through the
  module-level `__getattr__` for every index the server advertises in `ping`, e.g.
  `blt.get_wafer_devices("W123")`. The docstring says it is **tunnel-only** and doesn't
  exist on the real Runner.
- `blt.device_cache_status()` and `blt.refresh_device_cache(wait=True)`.
- HTTP timeout: calls that may trigger a full load use a long timeout (e.g. 1800s). Print
  a one-line `tunnel: loading device cache …` notice when the status is `loading` or
  `empty`.

### blt_analytics

- `devices_df(...)`: when `is_tunnel()` and the device cache is `ready`, read from it
  through a new server op `cached_devices_query(device_type=None, keys=None)`. It returns
  all cached records (optionally type-filtered and key-projected), so whole-space frames
  avoid paging. Otherwise behaviour is unchanged.
- A new `devices_df(..., index=None, value=None)` shortcut uses `cached_devices`.
- Schema tool `overview` reports the configured indexes (names and paths only).

### As-built (`blt_analytics` side, 2026-10-02)

Recorded so the spec and the shipped `frames.py` / `schema.py` agree (Data/Tools files for
this task; server + shim are the Tunnel agent's).

- **Assumed shim function names** (the server/shim agent must match these):
  `blt.cached_devices(index, value, *, refresh=False) -> list[Device]`, the dynamic
  `blt.get_<index>_devices(value, *, refresh=False)`, `blt.device_cache_status() -> dict`
  (with a top-level `"state"` of `empty`/`loading`/`ready`/`error`),
  `blt.refresh_device_cache(wait=True)`, and — for the `cached_devices_query` op — a
  **shim-only** wrapper named **`blt.tunnel_cached_devices_query(device_type=None,
  keys=None) -> list[Device]`** (the `tunnel_` prefix mirrors `tunnel_space_schema`, since
  the op has no real-Runner counterpart). `ping()` is extended with
  `device_indexes: {name: path}`.
- **`devices_df(index=, value=)` requires the tunnel; it does not degrade.** Per §6/§4 it
  raises a clear `RuntimeError` on a real Runner (and on a shim too old to expose
  `cached_devices`), pointing the caller at `device_type=`/`columns=`. Passing only one of
  `index`/`value` raises `ValueError`. `device_type=` and `columns=` are applied
  client-side to the index result.
- **Both server-cache paths bypass the on-disk frame cache** (the decision §6 left to the
  implementer). The server already holds the authoritative, persisted, write-through copy,
  so a second local pickle would only risk serving a pre-`refresh_device_cache` (or
  pre-write-through) snapshot. Consequence: `refresh=` is a **no-op on the whole-frame
  `cached_devices_query` path** (reload server-side instead), and is **forwarded to
  `cached_devices` on the `index=` path** with the per-value known-id meaning. The paging
  path (real Runner, or a cold/absent cache) keeps the on-disk cache unchanged.
- **The `keys=` projection is pushed to `cached_devices_query` too**, computed exactly as
  for `search_devices` (`sorted({c.split(".",1)[0] for c in columns})`); nested dotted
  projection still happens client-side.
- **Older shims degrade gracefully on the whole-frame path.** A missing
  `device_cache_status` / `cached_devices_query` (`AttributeError`), a non-`ready` state, or
  any status-call failure all fall through to the unchanged paging path.
- **`overview` reads `device_indexes` from the extended `ping`**, best-effort: any failure
  (not a tunnel, no connection, old shim, malformed reply) yields no `device_indexes` key,
  and it is **skipped entirely when a digest is injected** (`schema.set_digest`, i.e. tests)
  so the pure-digest tools stay offline and deterministic. Only index names and dotted
  paths surface — never index values. The object index form
  (`{"path": ..., "device_type": ...}`) is flattened to its `path`.
