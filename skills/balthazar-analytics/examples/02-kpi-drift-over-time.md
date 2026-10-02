# Example 2 — "Plot how the IV-sweep zero-bias resistance drifted over time"

A history question: a run output on a time axis, failures excluded.

## Tool calls

```
overview()                                  # find the flow name + its run count/date range
flow_schema("IV sweep")                     # its outputs, inputs, status breakdown
describe_output("IV sweep", "r_zero_ohm")   # confirm the exact output path + coverage + unit
```

`flow_schema` shows `status: {"FINISHED": 390, "FAILED": 10}` and an output
`r_zero_ohm` (kind `number`, unit `ohm`).

## Code

```python
import pandas as pd
import matplotlib.pyplot as plt
from blt_analytics import runs_df

# Scope to the one flow; pull only the output column we need.
runs = runs_df("IV sweep", columns=["output.r_zero_ohm"])

# Drop failed runs before a quality plot.
runs = runs[~runs["status"].astype(str).str.contains("fail", case=False, na=False)]

runs = runs.dropna(subset=["output.r_zero_ohm"])
runs["when"] = pd.to_datetime(runs["created_time"], errors="coerce")
runs = runs.dropna(subset=["when"]).sort_values("when")

fig, ax = plt.subplots(figsize=(9, 4.5))
ax.plot(runs["when"], runs["output.r_zero_ohm"], marker="o", linestyle="-", alpha=0.7)
ax.set_title(f"Zero-bias resistance drift (ohm, n={len(runs)})")
ax.set_xlabel("measured at"); ax.set_ylabel("r_zero_ohm (ohm)")
fig.autofmt_xdate(); fig.tight_layout()
plt.show()
```

## Notes

- `runs_df(flow="IV sweep", …)` scopes to one flow — far cheaper than `runs_df()` over all.
- Parsed the timestamp and sorted before plotting a trend.
- Took the unit `ohm` from `describe_output` and put it on the axis.
- If the slice came back empty, the first check would be `find("resistance")` to confirm the
  path name, not an assumption that there's no data.
