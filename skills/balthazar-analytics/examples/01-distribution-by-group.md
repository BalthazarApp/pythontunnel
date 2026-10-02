# Example 1 — "How does wafer yield vary by lot?"

A current-state question about devices: one KPI, grouped by a categorical param.

## Tool calls (learn the structure first)

```
overview()                              # which device types exist, and their counts
find("yield")                           # where does "yield" live? -> a device_param path
device_schema("Wafer")                  # confirm the exact param paths + coverage
describe_param("Wafer", "hierarchy")    # confirm the grouping key, e.g. hierarchy.lot
```

Say `find` returns a match `{"kind": "device_param", "device_type": "Wafer",
"path": "yield_pct"}` at good coverage, and `hierarchy.lot` is the lot key.

## Code (pull real data, then plot)

```python
import matplotlib.pyplot as plt
from blt_analytics import devices_df

# Project only what we plot — cheaper over the tunnel.
df = devices_df("Wafer", columns=["hierarchy.lot", "yield_pct"])

# yield_pct is sparse? drop NaNs for THIS column only (0 is a valid yield).
df = df.dropna(subset=["yield_pct"])

agg = (df.groupby("hierarchy.lot")["yield_pct"]
         .median().sort_values().reset_index())

fig, ax = plt.subplots(figsize=(9, 4.5))
ax.bar(agg["hierarchy.lot"], agg["yield_pct"])
ax.set_title(f"Median yield by lot (n={len(df)} wafers)")
ax.set_xlabel("lot"); ax.set_ylabel("yield (%)")
ax.tick_params(axis="x", rotation=90)
fig.tight_layout()
plt.show()
```

## Notes

- Took the exact strings `Wafer`, `yield_pct`, `hierarchy.lot` from the tools — no guessing.
- Grouped on a flattened dict path (`hierarchy.lot`), which is a real column because
  `hierarchy` is a small, stable dict.
- Reported the sample size in the title; showed an aggregate, not the raw table.
