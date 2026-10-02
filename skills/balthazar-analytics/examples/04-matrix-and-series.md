# Example 4 — "Show the crosstalk matrix and the sweep from the latest run"

Structured run outputs: a `matrix` and a `list`/series. Pull the latest run, expand with
`matrix_to_df` / `series_to_df`, and plot. Optionally save with `publish`.

## Tool calls

```
flow_schema("Crosstalk")                       # which outputs are structured?
describe_output("Crosstalk", "xtalk_matrix")   # kind: matrix, shape rows/cols
describe_output("Crosstalk", "bias_sweep")     # kind: list (a swept series)
```

## Code — matrix heatmap

```python
import matplotlib.pyplot as plt
from blt_analytics import runs_df, matrix_to_df

runs = runs_df("Crosstalk", columns=["output.xtalk_matrix"]).sort_values("created_time")
runs = runs.dropna(subset=["output.xtalk_matrix"])
latest = runs.tail(1)                    # keep it a 1-row frame

long = matrix_to_df(latest, "output.xtalk_matrix")   # long (row, col, value)
grid = long.pivot(index="row", columns="col", values="value")

fig, ax = plt.subplots(figsize=(6, 5.5))
im = ax.imshow(grid, cmap="RdBu_r")
fig.colorbar(im, ax=ax)
ax.set_title("Crosstalk matrix (latest run)")
fig.tight_layout()
plt.show()
```

## Code — series line

```python
from blt_analytics import series_to_df

long = series_to_df(latest, "output.bias_sweep")   # one row per element
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.plot(long["i"], long["output.bias_sweep"], marker=".")
ax.set_title("Bias sweep (latest run)")
ax.set_xlabel("point"); ax.set_ylabel("signal")
fig.tight_layout()
plt.show()
```

## Saving (only when asked)

```python
from blt_analytics import publish
run_id = publish([fig], "Crosstalk review")   # attaches the figure to a new Balthazar run
```

## Notes

- A `matrix` is a list of equal-length numeric lists; `matrix_to_df` reads the shape for
  you. A 2-column matrix can look like a series — select by the known output path, not by
  guessing the kind.
- Checked `describe_output` for `kind` first, so the right helper was used for each.
- `publish` ran only because the user asked to save/share; exploration stays local.
