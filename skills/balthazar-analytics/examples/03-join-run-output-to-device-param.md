# Example 3 — "Does zero-bias resistance correlate with a wafer's design width?"

A cross-frame question: join a **run output** to a **device param**. This is what
`explode_devices` is for.

## Tool calls

```
flow_schema("IV sweep")                   # output path: r_zero_ohm
device_schema("Wafer")                    # device param: design.width_um
describe_param("Wafer", "design")         # confirm width_um lives under design
```

## Code

```python
import matplotlib.pyplot as plt
from blt_analytics import runs_df, devices_df, explode_devices

runs = runs_df("IV sweep", columns=["output.r_zero_ohm"])
runs = runs[~runs["status"].astype(str).str.contains("fail", case=False, na=False)]

# One row per (run, device_id) — the bridge to devices.
rd = explode_devices(runs)

devs = devices_df("Wafer", columns=["design.width_um"])

merged = rd.merge(devs, left_on="device_id", right_on="id", how="inner")
merged = merged.dropna(subset=["output.r_zero_ohm", "design.width_um"])

fig, ax = plt.subplots(figsize=(7, 6))
ax.scatter(merged["design.width_um"], merged["output.r_zero_ohm"], alpha=0.6)
ax.set_title(f"R(0) vs design width (n={len(merged)})")
ax.set_xlabel("design width (um)"); ax.set_ylabel("r_zero_ohm (ohm)")
fig.tight_layout()
plt.show()
```

## Notes

- `explode_devices` turns the run's `device_ids` list into one row per device so the merge
  is clean — a run that targeted several devices contributes one row each.
- Merge on `device_id` (runs side) ↔ `id` (devices side).
- Dropped NaNs on both plotted columns together; reported the joined sample size.
