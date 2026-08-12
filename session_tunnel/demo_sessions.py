"""Session-tunnel demo — `enter_new_flow_run` from your local debugger.

Run this from *this* directory so `import balthazar` resolves to the v2 shim here
rather than the v1 shim one level up:

    cd session_tunnel && python demo_sessions.py

Start `flows/tunnel_session_server.py` as a flow in Balthazar first.

Each example below opens a real child flow run on the Runner and writes into it
from local code. The point of comparison with v1: there, a run was assembled
client-side and posted in one shot. Here the run is *open* while your code runs,
so plots, output and device writes land as they happen — and an exception marks
the run FAILED without you saying so.
"""

import warnings

import matplotlib
matplotlib.use("Agg")  # headless script; the notebook uses the inline backend

# plt.show() uploads the figure *before* delegating to the real show, so the
# upload works fine under Agg — only the display half has nothing to draw on.
warnings.filterwarnings("ignore", message="FigureCanvasAgg is non-interactive")

import matplotlib.pyplot as plt
import numpy as np

import balthazar as blt

# --- Configuration --------------------------------------------------------------
# A real space has plenty of devices, and `devices[0]` is whatever happens to sort
# first. Narrow the selection here, and cap how many child runs the batch example
# creates — otherwise it opens one per device and floods the run list.
DEVICE_TYPE = None      # e.g. "Instrument" or "Wafer"; None = any type
DEVICE_NAME = None      # e.g. "Instrument_QCCS-*" (glob); None = any name
MAX_BATCH_DEVICES = 2   # child runs created by the nested example


# --- 0. Connect -----------------------------------------------------------------
info = blt.ping()
print(f"connected to flow {info['flow_name']!r} (context depth {info['depth']})")

all_devices = blt.search_devices(type=DEVICE_TYPE, name=DEVICE_NAME)
if not all_devices:
    raise SystemExit(f"no devices match type={DEVICE_TYPE!r} name={DEVICE_NAME!r}")

devices = all_devices[:MAX_BATCH_DEVICES]
print(f"{len(all_devices)} device(s) matched; using {len(devices)}: "
      f"{', '.join(d.name for d in devices)}")

device = devices[0]
baseline_yield = float(dict(device.params).get("yield_pct", 50.0))
print(f"primary device: {device.name} (type {device.type}, "
      f"baseline yield {baseline_yield}%)")


# --- 1. A single context: plot and output land on the open run -------------------
print("\n1. single context")
bias = np.linspace(-1.0, 1.0, 201)
current = np.sinh(bias * 4.0) * baseline_yield / 100.0

with blt.enter_new_flow_run(
    name="sweep: single context",
    devices=[device],
    parameters={"bias_min_v": -1.0, "bias_max_v": 1.0, "points": bias.size},
) as run:
    # Inside the block, these globals reflect THIS run, not the tunnel's.
    print(f"   run {run.flow_run_id}")
    print(f"   blt.params      -> {dict(blt.params)}")
    print(f"   blt.devices     -> {[d.name for d in blt.devices]}")

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(bias, current, color="#2563eb")
    ax.set_xlabel("Bias (V)")
    ax.set_ylabel("Current (mA)")
    ax.set_title(f"I-V sweep - {device.name}")
    fig.tight_layout()
    plt.show()  # ships the figure to THIS run, then displays normally

    blt.output["i_at_1v_ma"] = round(float(current[-1]), 4)
    blt.output.update({
        "zero_bias_resistance_ohm": round(float(np.gradient(bias, current)[100]), 4),
        "status": "success",
    })
    print(f"   blt.output      -> {dict(blt.output.items())}")

plt.close("all")


# --- 2. Nested contexts: one parent, a child per device -------------------------
# This is the orchestrator shape — a parent run that spawns a child run per item,
# each with its own inputs, plot and output, all attributable in the UI.
print("\n2. nested contexts")
with blt.enter_new_flow_run(
    name="batch: all devices",
    devices=devices,
    parameters={"device_count": len(devices), "mode": "batch"},
) as batch:
    print(f"   parent run {batch.flow_run_id}")
    peaks = []

    for d in devices:
        d_yield = float(dict(d.params).get("yield_pct", 50.0))
        with blt.enter_new_flow_run(
            name=f"sweep: {d.name}",
            devices=[d],
            parameters={"device": d.name, "yield_pct": d_yield},
        ) as child:
            print(f"     child {child.flow_run_id} for {d.name} "
                  f"(depth {len(blt.parents()) + 1}, parent {blt.parent()['name']!r})")

            i = np.sinh(bias * 4.0) * d_yield / 100.0
            fig, ax = plt.subplots(figsize=(5, 3))
            ax.plot(bias, i, color="#059669")
            ax.set_title(f"{d.name} I-V")
            fig.tight_layout()
            plt.show()

            blt.output.update({"peak_ma": round(float(i.max()), 4), "status": "success"})
            peaks.append(float(i.max()))
            plt.close("all")

    # Back in the parent context: output here lands on the parent run.
    blt.output.update({
        "children": len(devices),
        "peak_max_ma": round(max(peaks), 4),
        "status": "success",
    })

plt.close("all")


# --- 3. Writing device params from inside a run ---------------------------------
# The measurement-flow pattern: results are fields on the measured device, and the
# write is attributed to the run that produced them.
print("\n3. device params written from inside a run")
with blt.enter_new_flow_run(
    name="measure: write back to device",
    devices=[device],
    parameters={"probe": "4-point", "temperature_k": 297.0},
) as run:
    measurements = dict(dict(device.params).get("measurements", {}))
    measurements["iv_sweep"] = {
        "i_at_1v_ma": round(float(current[-1]), 4),
        "probe": "4-point",
    }
    # update() is the only reliable write path, and nested changes rebuild the
    # whole top-level key rather than mutating in place.
    device.params.update({"measurements": measurements})
    blt.output.update({"keys_written": 1, "status": "success"})
    print(f"   device.params['measurements'] -> {dict(device.params)['measurements']}")


# --- 4. An exception marks the run FAILED, no bookkeeping required ---------------
print("\n4. exception inside a context")
try:
    with blt.enter_new_flow_run(
        name="sweep: aborted by exception",
        devices=[device],
        parameters={"bias_max_v": 5.0, "compliance_ma": 10.0},
    ) as run:
        blt.output.update({"aborted_at_v": 3.2})
        raise RuntimeError("compliance limit hit at 3.2 V")
except RuntimeError as exc:
    # The run is already FAILED on the server; the exception is re-raised to you,
    # exactly as the Runner's own context manager behaves.
    print(f"   caught locally: {exc}")
    print("   the run is FAILED in Balthazar, with that message attached")


# --- 5. Explicit failure without raising ----------------------------------------
print("\n5. explicit fail() without an exception")
run = blt.enter_new_flow_run(name="sweep: explicitly failed", devices=[device])
blt.output.update({"reason": "instrument offline"})
run.fail("instrument offline: no GPIB response")
print(f"   run {run.flow_run_id} closed as FAILED")


# --- 6. Back at the top level ---------------------------------------------------
print("\n6. wrap up")
print(f"   open contexts: {len(blt.parents())} (expect 0)")
print(f"   server agrees: depth {blt.context()['depth']}")
blt.info("session-tunnel demo finished from the local debugger")
