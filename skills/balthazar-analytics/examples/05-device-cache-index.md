# Example 5 — "Pull every die on wafer W123" (big space, device index)

A targeted lookup on a space with hundreds of thousands of devices. Paging the whole
set to filter one wafer would be wasteful — use the **server-side device cache** index
(SPEC §6) instead.

## Tool calls (find the index first)

```
overview()                         # totals + device_indexes: {"wafer": "hierarchy.wafer", ...}
device_schema("Die")               # the param paths on a die, with coverage
```

`overview()` returns `device_indexes` only when the tunnel flow configures a device
cache. Say it lists `{"wafer": "hierarchy.wafer"}` — then `wafer` is a usable index.

## Code (fetch one index value, straight from the cache)

```python
import matplotlib.pyplot as plt
from blt_analytics import devices_df

# Served from the cache's "wafer" index — no whole-space paging.
dies = devices_df(index="wafer", value="W123", columns=["vth_v", "idsat_a"])
dies = dies.dropna(subset=["vth_v"])

fig, ax = plt.subplots(figsize=(7, 4.5))
ax.hist(dies["vth_v"], bins=30)
ax.set_title(f"Vth across wafer W123 (n={len(dies)} dies)")
ax.set_xlabel("Vth (V)"); ax.set_ylabel("count")
fig.tight_layout()
plt.show()
```

## Notes

- Took the index name `wafer` and its path `hierarchy.wafer` from `overview()` /
  `device_schema` — no guessing.
- `index=`/`value=` is **tunnel-only**. For code you will deploy as a flow, use the
  portable `devices_df(device_type="Die", columns=[...])` and filter the frame instead;
  on the tunnel that *also* reads the cache when it is ready.
- The **first** `devices_df` call after the tunnel starts may block for minutes while the
  cache loads (watch `blt.tunnel.device_cache_status()`). Later calls are fast.
- `refresh=True` here re-fetches only the dies already known for `W123` (re-reading their
  current params); it will not surface dies added to the wafer since the cache was built.
  For new devices, reload the cache first: `import balthazar as blt;
  blt.tunnel.refresh_device_cache()`.
