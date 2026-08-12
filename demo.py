"""Local proof of concept — run this in VS Code, breakpoints and all.

Every `blt.*` call below executes inside a live Balthazar flow run on the Runner,
while this file runs in your local interpreter and debugger.

Start `flows/tunnel_server.py` as a flow in Balthazar first, then run this.

Each run below carries both inputs (`parameters=`) and outputs (`output=`), so the
run pages show what went in and what came out. Inputs must be flat scalars — they
go through the platform's parameter type inference. Outputs are primitives only:
str / int / float / bool / small list. No dicts, no dates, no DataFrames.

Every run created here finishes green except the last, which is FAILED on purpose
to show what that looks like. Set INCLUDE_FAILURE_EXAMPLE = False to skip it.
"""

import matplotlib.pyplot as plt
import numpy as np

import balthazar as blt  # resolves to the local tunnel shim, not the Runner's module

INCLUDE_FAILURE_EXAMPLE = True

# --- 1. Read: pull real devices out of the space --------------------------------
print(f"connected to flow {blt.ping()['flow_name']!r}")

devices = blt.search_devices()
print(f"{len(devices)} device(s) in the space")
for device in devices[:5]:
    print(f"  {device.name:<16} type={device.type:<10} params={dict(device.params)}")

device = devices[0]
device_params = dict(device.params)
baseline_yield = float(device_params.get("yield_pct", 50.0))

# --- 2. A plain run: inputs and outputs, no plot --------------------------------
run_id = blt.new_flow_run(
    "tunnel: plain run",
    devices=[device],
    parameters={
        "operator": "dejan",
        "source": "vscode-tunnel",
        "baseline_yield_pct": baseline_yield,
        "dry_run": False,
    },
    output={
        "device_name": device.name,
        "baseline_yield_pct": baseline_yield,
        "status": "success",
    },
)
print(f"\nplain run          -> {run_id}")

# --- 3. A run carrying one plot -------------------------------------------------
# Build the figure from real device data, locally, with matplotlib you control.
bias_min, bias_max, n_points = -1.0, 1.0, 201
bias = np.linspace(bias_min, bias_max, n_points)
current = np.sinh(bias * 4.0) * baseline_yield / 100.0

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot(bias, current, color="#2563eb")
ax.axhline(0, color="0.7", lw=0.8)
ax.axvline(0, color="0.7", lw=0.8)
ax.set_xlabel("Bias (V)")
ax.set_ylabel("Current (mA)")
ax.set_title(f"I-V sweep - {device.name}")
fig.tight_layout()

resistance = float(np.gradient(bias, current)[n_points // 2])

run_id = blt.new_flow_run(
    "tunnel: I-V sweep with plot",
    devices=[device],
    parameters={
        "bias_min_v": bias_min,
        "bias_max_v": bias_max,
        "points": n_points,
        "sweep_mode": "linear",
    },
    output={
        "i_at_1v_ma": round(float(current[-1]), 4),
        "i_at_minus_1v_ma": round(float(current[0]), 4),
        "zero_bias_resistance_ohm": round(resistance, 4),
        "symmetric": bool(abs(current[0] + current[-1]) < 1e-9),
        "status": "success",
    },
    figures=fig,
)
print(f"run with 1 plot    -> {run_id}")

# --- 4. A run carrying every open figure ----------------------------------------
rng = np.random.default_rng(0)
yields = rng.normal(baseline_yield, 4.0, 500)

fig2, ax2 = plt.subplots(figsize=(6, 4))
ax2.hist(yields, bins=30, color="#7c3aed")
ax2.axvline(float(yields.mean()), color="#1f2937", ls="--", lw=1.2, label="mean")
ax2.set_xlabel("Yield (%)")
ax2.set_ylabel("Count")
ax2.set_title("Yield distribution")
ax2.legend()
fig2.tight_layout()

run_id = blt.new_flow_run(
    "tunnel: batch summary with 2 plots",
    devices=[device],
    parameters={
        "sample_count": int(yields.size),
        "rng_seed": 0,
        "distribution": "normal",
    },
    output={
        "yield_mean_pct": round(float(yields.mean()), 3),
        "yield_std_pct": round(float(yields.std()), 3),
        "yield_min_pct": round(float(yields.min()), 3),
        "yield_max_pct": round(float(yields.max()), 3),
        "figure_count": len(plt.get_fignums()),
        "status": "success",
    },
    figures="all",  # mirrors what plt.show() would sweep up
)
print(f"run with 2 plots   -> {run_id}")

# --- 5. A run marked FAILED (red on purpose) ------------------------------------
if INCLUDE_FAILURE_EXAMPLE:
    run_id = blt.new_flow_run(
        "tunnel: FAILED on purpose (demo)",
        devices=[device],
        parameters={"bias_max_v": 5.0, "compliance_ma": 10.0},
        output={"aborted_at_v": 3.2, "status": "failed"},
        status="FAILED",
        error_message="sweep aborted: compliance limit hit at 3.2 V",
        figures=fig,
    )
    print(f"failed run (demo)  -> {run_id}")

blt.info("demo finished from the local debugger")
plt.close("all")
