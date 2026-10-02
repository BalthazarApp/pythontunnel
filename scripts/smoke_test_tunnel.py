#!/usr/bin/env python
"""Live smoke test for the Balthazar v3 reflection bridge.

Exercises the real bridge end-to-end from the client side: it locates the balthazar
module exactly the way ``blt_analytics`` does (via ``blt_analytics._blt.get_blt()``),
confirms it is the **v3 bridge** (``bridge_version() == 3``), and runs a sequence of
numbered checks against the running flow. Tunnel ops live under ``blt.tunnel`` and
identity comes from ``describe``.

Setup: start ``flows/tunnel_bridge.py`` in Balthazar, click "Open app", copy the
snippet URL, and ``blt-tunnel connect "<url>"`` (writes ``~/.balthazar_bridge.json``).

    uv run python scripts/smoke_test_tunnel.py              # read-only (default)
    uv run python scripts/smoke_test_tunnel.py --write      # also create flow runs
    uv run python scripts/smoke_test_tunnel.py --skip-schema
    uv run python scripts/smoke_test_tunnel.py --device-type Chip --flow "IV sweep" --limit 10

Every check prints ``PASS`` / ``FAIL`` / ``SKIP`` with its duration and a one-line
summary (counts and names only — never a dump of params or values). Failures do not
stop the run; the script exits non-zero if any check FAILed.

**Read-only by default.** Without ``--write`` nothing is created or modified: the
checks only read (describe, searches, schema, data frames). ``--write`` creates small
throwaway flow runs (a tiny plot via ``plt.show``, one ``blt.output`` primitive, and
one intentionally-FAILED nested run); it never writes device params.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any, Callable, Optional

# Force a non-interactive matplotlib backend before anything imports pyplot, so
# --write never tries to pop a window on a headless Runner.
os.environ.setdefault("MPLBACKEND", "Agg")

# Make ``import blt_analytics`` resolve to this repo's package even when the script
# is launched as ``scripts/smoke_test_tunnel.py`` (whose own dir, not the repo root,
# lands on sys.path). Appending — not inserting — keeps an installed copy preferred.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.append(_REPO_ROOT)

_MISSING = object()


# ---------------------------------------------------------------------------
# Tiny check harness
# ---------------------------------------------------------------------------


class SkipCheck(Exception):
    """Raise from a check body to report SKIP (with a reason) rather than FAIL."""


class Harness:
    def __init__(self) -> None:
        self.n = 0
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def run(self, title: str, fn: Callable[[], str]) -> None:
        self.n += 1
        num = self.n
        t0 = time.perf_counter()
        try:
            summary = fn() or ""
            status = "PASS"
            self.passed += 1
        except SkipCheck as exc:
            summary = str(exc)
            status = "SKIP"
            self.skipped += 1
        except Exception as exc:  # noqa: BLE001 - a smoke test must never traceback
            summary = f"{type(exc).__name__}: {exc}"
            status = "FAIL"
            self.failed += 1
        dt_ms = (time.perf_counter() - t0) * 1000.0
        print(f"[{status}] {num:>2}. {title}  ({dt_ms:7.0f} ms)  {summary}", flush=True)


class State:
    """Shared data threaded between checks."""

    def __init__(self) -> None:
        self.blt: Any = None
        self.version: Optional[int] = None  # 3 (bridge) or None (runner)
        self.describe: Optional[dict] = None
        self.device_indexes: dict[str, str] = {}
        self.sample_devices: list = []
        self.flows: list = []
        self.target_flow: Any = None
        self.runs: list = []
        self.cache_status: Optional[dict] = None
        self.digest_first_type: Optional[str] = None
        self.created_run_ids: list[str] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require_describe(state: State) -> None:
    if state.describe is None:
        raise SkipCheck("bridge not reachable (see the describe check above)")


def _describe(state: State) -> dict:
    """The v3 ``describe`` payload (``blt._session.description``), or ``{}``."""
    from blt_analytics import _blt

    return _blt.describe_info() or {}


def _dotted_get(container: Any, dotted: str) -> Any:
    cur = container
    for part in dotted.split("."):
        if isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return _MISSING
    return _MISSING if cur is None else cur


def _resolve_device_type(state: State, args: argparse.Namespace) -> Optional[str]:
    if args.device_type:
        return args.device_type
    if state.digest_first_type:
        return state.digest_first_type
    for dev in state.sample_devices:
        dtype = getattr(dev, "type", None)
        if dtype:
            return dtype
    return None


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_connection(state: State) -> str:
    app_url = os.environ.get("BALTHAZAR_BRIDGE_URL")
    path = os.path.expanduser("~/.balthazar_bridge.json")
    if app_url:
        return "using BALTHAZAR_BRIDGE_URL"
    if os.path.exists(path):
        return f"bridge profile present ({path})"
    raise RuntimeError(
        f"no bridge profile at {path} and no BALTHAZAR_BRIDGE_URL — start "
        'flows/tunnel_bridge.py in Balthazar, "Open app", and run '
        '`blt-tunnel connect "<app url>"`'
    )


def check_describe(state: State) -> str:
    info = _describe(state)
    state.describe = info
    state.device_indexes = dict(info.get("device_indexes") or {})
    idx = ", ".join(sorted(state.device_indexes)) or "none"
    return (
        f"describe: protocol={info.get('protocol')} "
        f"caller={info.get('user')} owner={info.get('owner')} "
        f"shared={info.get('shared')} device_indexes=[{idx}]"
    )


def check_search_devices(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    devs = list(state.blt.search_devices(limit=args.limit))
    state.sample_devices = devs
    types = sorted({getattr(d, "type", "?") for d in devs})
    shown = ", ".join(types[:5]) + (" …" if len(types) > 5 else "")
    return f"{len(devs)} devices (limit={args.limit}); types: {shown or '—'}"


def check_search_flows(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    flows = list(state.blt.search_flows(limit=20))
    state.flows = flows
    # Resolve which flow drives the later flow-scoped checks.
    if args.flow:
        match = next(
            (f for f in flows
             if getattr(f, "name", None) == args.flow or getattr(f, "id", None) == args.flow),
            None,
        )
        if match is None:
            found = list(state.blt.search_flows(name=args.flow)) or \
                list(state.blt.search_flows(flow_ids=[args.flow]))
            match = found[0] if found else None
        state.target_flow = match
        if match is None:
            return f"{len(flows)} flows; requested --flow {args.flow!r} NOT found"
    else:
        state.target_flow = flows[0] if flows else None
    first = getattr(state.target_flow, "name", None)
    return f"{len(flows)} flows (limit=20); using: {first!r}"


def check_run_history(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    if state.target_flow is None:
        raise SkipCheck("no target flow available")
    flow = state.target_flow
    runs = list(state.blt.search_flow_run_history(flow_id=flow.id, limit=args.limit))
    state.runs = runs
    statuses = sorted({(getattr(r, "status", None) or "?") for r in runs})
    return (
        f"{len(runs)} runs for {getattr(flow, 'name', None)!r} (limit={args.limit}); "
        f"statuses: {', '.join(statuses) or '—'}"
    )


def check_fetch_visualizations(state: State) -> str:
    _require_describe(state)
    viz_id = None
    for run in state.runs:
        ids = getattr(run, "visualization_ids", None) or []
        if ids:
            viz_id = ids[0]
            break
    if viz_id is None:
        raise SkipCheck("no run in the sample has a visualization")
    vizmap = state.blt.fetch_visualizations(viz_id)
    viz = vizmap.get(viz_id)
    if viz is None:
        raise RuntimeError(f"visualization {viz_id} not returned by fetch_visualizations")
    return f"fetched 1 visualization ({getattr(viz, 'filename', '?')}): {len(viz.data)} bytes"


def check_device_cache_status(state: State) -> str:
    _require_describe(state)
    status = state.blt.tunnel.device_cache_status()
    state.cache_status = status
    indexes = list((status.get("indexes") or {}).keys())
    return (
        f"state={status.get('state')} count={status.get('count')} "
        f"loaded={status.get('loaded')} indexes={indexes or '[]'}"
    )


def check_device_cache_index(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    if not state.device_indexes:
        raise SkipCheck("no device_indexes configured on this bridge")
    status = state.cache_status or state.blt.tunnel.device_cache_status()
    cache_state = status.get("state")
    if cache_state != "ready":
        if cache_state in ("loading", "empty"):
            raise SkipCheck(
                f"device cache not ready (state={cache_state}, "
                f"loaded={status.get('loaded')}/{status.get('count')})"
            )
        raise SkipCheck(f"device cache state={cache_state} ({status.get('error')})")

    # Pick an index and locate a sample value at its param path, without printing it.
    index_name = sorted(state.device_indexes)[0]
    spec = (status.get("indexes") or {}).get(index_name) or {}
    path = spec.get("path") or state.device_indexes[index_name]
    dtype = spec.get("device_type")

    kwargs: dict[str, Any] = {"limit": 50}
    if dtype:
        kwargs["type"] = dtype
    value = _MISSING
    for dev in state.blt.search_devices(**kwargs):
        candidate = _dotted_get(dict(getattr(dev, "params", {}) or {}), path)
        if candidate is not _MISSING:
            value = candidate
            break
    if value is _MISSING:
        raise SkipCheck(f"no sampled device had a value at index path {path!r}")

    # ``get_<index>_devices`` is generated on the v3 drop-in; ``cached_devices`` lives
    # under ``blt.tunnel``.
    accessor = getattr(state.blt, f"get_{index_name}_devices")
    t0 = time.perf_counter()
    via_accessor = list(accessor(value))
    t_acc = (time.perf_counter() - t0) * 1000.0
    t0 = time.perf_counter()
    via_cached = list(state.blt.tunnel.cached_devices(index_name, value))
    t_cached = (time.perf_counter() - t0) * 1000.0

    match = "match" if len(via_accessor) == len(via_cached) else "MISMATCH"
    return (
        f"index {index_name!r}: get_{index_name}_devices -> {len(via_accessor)} "
        f"({t_acc:.0f} ms), cached_devices -> {len(via_cached)} ({t_cached:.0f} ms) [{match}]"
    )


def _schema():
    from blt_analytics import schema
    return schema


def check_schema_overview(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    if args.skip_schema:
        raise SkipCheck("--skip-schema")
    schema = _schema()
    t0 = time.perf_counter()
    ov = schema.overview()
    elapsed = time.perf_counter() - t0
    if isinstance(ov, dict) and ov.get("error"):
        raise RuntimeError(ov["error"])
    dtypes = list((ov.get("device_types") or {}).keys())
    state.digest_first_type = dtypes[0] if dtypes else None
    totals = ov.get("totals") or {}
    return (
        f"devices={totals.get('devices')} flows={totals.get('flows')} "
        f"device_types={len(dtypes)} (digest built in {elapsed:.1f} s)"
    )


def check_device_schema(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    if args.skip_schema:
        raise SkipCheck("--skip-schema")
    dtype = _resolve_device_type(state, args)
    if not dtype:
        raise SkipCheck("no device type available")
    res = _schema().device_schema(dtype)
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(res["error"])
    return f"{dtype!r}: count={res.get('count')} param_paths={len(res.get('params') or {})}"


def check_flow_schema(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    if args.skip_schema:
        raise SkipCheck("--skip-schema")
    flow_ref = args.flow or getattr(state.target_flow, "name", None)
    if not flow_ref:
        raise SkipCheck("no flow available")
    res = _schema().flow_schema(flow_ref)
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(res["error"])
    return (
        f"{flow_ref!r}: runs={res.get('run_count')} "
        f"inputs={len(res.get('inputs') or {})} outputs={len(res.get('outputs') or {})}"
    )


def check_find(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    if args.skip_schema:
        raise SkipCheck("--skip-schema")
    res = _schema().find("a")
    matches = res.get("matches") or []
    return f'find("a"): {len(matches)} matches'


def check_devices_df(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    try:
        import pandas  # noqa: F401
    except ImportError:
        raise SkipCheck("pandas not installed")
    import blt_analytics as ba

    dtype = _resolve_device_type(state, args)
    if not dtype:
        raise SkipCheck("no device type available")
    df = ba.devices_df(dtype)
    return f"devices_df({dtype!r}) shape={df.shape[0]}x{df.shape[1]}"


def check_runs_df(state: State, args: argparse.Namespace) -> str:
    _require_describe(state)
    try:
        import pandas  # noqa: F401
    except ImportError:
        raise SkipCheck("pandas not installed")
    import blt_analytics as ba

    flow_ref = args.flow or getattr(state.target_flow, "name", None)
    if not flow_ref:
        raise SkipCheck("no flow available")
    df = ba.runs_df(flow_ref, max_runs=50)
    return f"runs_df({flow_ref!r}, max_runs=50) shape={df.shape[0]}x{df.shape[1]}"


def check_write_run(state: State) -> str:
    _require_describe(state)
    import warnings

    import matplotlib.pyplot as plt

    blt = state.blt
    context = blt.enter_new_flow_run(name="tunnel smoke test", devices=None)
    run_id = context.flow_run_id
    with context:
        fig, ax = plt.subplots()
        ax.plot([0, 1, 2, 3], [0, 1, 4, 9], marker="o")
        ax.set_title("tunnel smoke test")
        with warnings.catch_warnings():
            # The Agg backend warns that it cannot "show"; we only want the bridge's
            # upload side effect, so silence that one cosmetic warning.
            warnings.simplefilter("ignore", UserWarning)
            plt.show()  # inside the context -> figure lands on this run
        plt.close(fig)
        blt.output["smoke_ok"] = True
    state.created_run_ids.append(run_id)
    return f"created run {run_id} (1 figure, output smoke_ok=True)"


def check_write_nested_fail(state: State) -> str:
    _require_describe(state)
    blt = state.blt

    outer = blt.enter_new_flow_run(name="tunnel smoke test (outer)")
    outer_id = outer.flow_run_id
    inner_id = None
    caught = False
    with outer:
        try:
            with blt.enter_new_flow_run(name="tunnel smoke test (nested, expected-fail)") as inner:
                inner_id = inner.flow_run_id
                raise RuntimeError("intentional smoke-test failure (expected)")
        except RuntimeError:
            caught = True  # catch locally so the smoke test itself keeps running
    state.created_run_ids.extend([rid for rid in (outer_id, inner_id) if rid])
    if not caught or inner_id is None:
        raise RuntimeError("nested context did not raise as expected")
    return f"outer {outer_id} FINISHED; nested {inner_id} marked FAILED (exception caught locally)"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="smoke_test_tunnel.py",
        description="Live smoke test for the Balthazar v3 reflection bridge "
                    "(read-only unless --write).",
    )
    p.add_argument("--write", action="store_true",
                   help="opt-in: also create throwaway flow runs (enter_new_flow_run + "
                        "plt.show + output, then a nested exception -> FAILED). Never "
                        "writes device params.")
    p.add_argument("--skip-schema", action="store_true",
                   help="skip the schema tools (space_schema can be slow on big spaces)")
    p.add_argument("--device-type", default=None,
                   help="device type to use for device_schema / devices_df "
                        "(default: first type found)")
    p.add_argument("--flow", default=None,
                   help="flow name or id to use for run history / flow_schema / runs_df "
                        "(default: first flow found)")
    p.add_argument("--limit", type=int, default=5,
                   help="sample size for device and run-history reads (default: 5)")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    state = State()

    mode = "WRITE (creates flow runs)" if args.write else "read-only"
    print(f"Balthazar bridge smoke test — mode: {mode}", flush=True)

    # Locate the balthazar module exactly as blt_analytics does. get_blt() prefers an
    # importable real module / v3 drop-in, otherwise loads the v3 bridge by profile.
    try:
        from blt_analytics import _blt
        state.blt = _blt.get_blt()
        state.version = _blt.bridge_version()
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL]  0. locate balthazar module  {type(exc).__name__}: {exc}", flush=True)
        print("\n0 passed, 1 failed, 0 skipped", flush=True)
        return 1
    label = "v3 reflection bridge" if state.version == 3 else "real runner module"
    print(
        f"        located balthazar: {label} "
        f"({getattr(state.blt, '__file__', '?')})",
        flush=True,
    )

    if state.version != 3:
        print(
            "[FAIL]  0. this smoke test needs the v3 bridge, but located "
            f"{label}. Run `blt-tunnel connect \"<url>\"` first.",
            flush=True,
        )
        print("\n0 passed, 1 failed, 0 skipped", flush=True)
        return 1

    h = Harness()
    h.run("bridge profile / env present", lambda: check_connection(state))
    h.run("describe", lambda: check_describe(state))
    h.run("search_devices(limit)", lambda: check_search_devices(state, args))
    h.run("search_flows(limit=20)", lambda: check_search_flows(state, args))
    h.run("search_flow_run_history(first flow)", lambda: check_run_history(state, args))
    h.run("fetch_visualizations(first viz)", lambda: check_fetch_visualizations(state))
    h.run("device_cache_status", lambda: check_device_cache_status(state))
    h.run("device cache index accessor", lambda: check_device_cache_index(state, args))
    h.run("schema.overview", lambda: check_schema_overview(state, args))
    h.run("schema.device_schema(first type)", lambda: check_device_schema(state, args))
    h.run("schema.flow_schema(first flow)", lambda: check_flow_schema(state, args))
    h.run("schema.find('a')", lambda: check_find(state, args))
    h.run("blt_analytics.devices_df(first type)", lambda: check_devices_df(state, args))
    h.run("blt_analytics.runs_df(first flow, max_runs=50)", lambda: check_runs_df(state, args))

    if args.write:
        h.run("[write] enter_new_flow_run + plt.show + output", lambda: check_write_run(state))
        h.run("[write] exception in nested context -> FAILED", lambda: check_write_nested_fail(state))
        if state.created_run_ids:
            print(f"        created run ids: {', '.join(state.created_run_ids)}", flush=True)

    print(f"\n{h.passed} passed, {h.failed} failed, {h.skipped} skipped", flush=True)
    return 1 if h.failed else 0


if __name__ == "__main__":
    sys.exit(main())
