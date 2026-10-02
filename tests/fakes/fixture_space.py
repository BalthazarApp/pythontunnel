"""A deterministic fixture space: devices, flows, runs and visualizations.

This is the single source of truth the tests build on. ``fake_blt`` serves rich
objects built from it (stub attribute names, datetime timestamps, a dict-like
``params``), and :func:`to_records` serializes the *same* data into the §1 wire
format that the Tunnel agent's server serializers must reproduce byte-for-byte.
Keeping both views in one module is what keeps them honest.

The leak convention (spec §5)
-----------------------------
Two disjoint classes of literal, so the leak tests can grep for data that must
never reach the digest:

* **Measurement values** — every numeric leaf is a distinctive float (e.g.
  ``123.456789``) and every opaque / measurement string is ``SECRET_``-prefixed.
  These are what the digest must withhold. :func:`fixture_numeric_values` and
  :func:`fixture_secret_strings` enumerate them (by walking the data, so they
  cannot drift from it).
* **Schema values** — path names, device types, flow names/ids, units, declared
  parameter descriptions and timestamps are plain. The digest *does* surface
  these by design, so they are deliberately not ``SECRET_`` and not distinctive
  numerics.

A ``SECRET_`` string is safe in three places that the digest provably does not
echo: a measurement *value*, a *tag* (reported only as a count), and a *map key*
(a dict large enough to collapse, whose keys become a count). The fixture puts
them in exactly those places on purpose, to prove the collapse hides them.

The structural coverage, by design:

* **Chip** — scalars, nested dicts that flatten (``hierarchy.lot``,
  ``resistance.value`` with a sibling ``unit``), partial coverage.
* **Wafer** — a ``measurements`` dict with 60 run keys that must collapse to a
  ``map``, hiding its ``SECRET_`` keys.
* **Sensor** — a numeric ``sweep`` list and an ``iv_matrix`` matrix.
* **Mixed** — a ``flaky`` param whose kind varies across devices (number /
  string / bool), i.e. ``mixed``.
"""

from __future__ import annotations

import copy
import datetime
from typing import Any

# --- distinctive measurement numbers (never integers, never round) -----------
N_RESISTANCE = 123.456789
N_RESISTANCE_2 = 123.987654
N_RESISTANCE_3 = 124.135790
N_YIELD = 234.567891
N_YIELD_2 = 235.678902
N_DIAMETER = 345.678912
N_DIAMETER_2 = 346.789023
N_SWEEP = (11.111111, 22.222222, 33.333333, 44.444444, 55.555555, 66.666666, 77.777777)
N_MATRIX = (
    (0.101010, 0.202020, 0.303030),
    (0.404040, 0.505050, 0.606060),
)
N_FLAKY = 919.181716
N_BIAS_MAX = 401.202122
N_BIAS_MIN = -402.232425
N_RZERO = 512.262728
N_RZERO_2 = 513.293031
N_TEMP = 77.123456
N_SPECTRUM = (9.010203, 8.040506, 7.070809, 6.101112)
N_CAP_MATRIX = (
    (1.234501, 1.234502),
    (1.234503, 1.234504),
    (1.234505, 1.234506),
)
N_THRESHOLD = 628.313233
N_SCORE = 731.343536
N_SCORE_2 = 732.373839

# --- schema literals that the digest is allowed to surface (NOT secret) ------
UNIT_OHM = "ohm"
UNIT_MV = "mV"
UNIT_KELVIN = "K"


def _d(year: int, month: int, day: int) -> datetime.date:
    return datetime.date(year, month, day)


def _ts(year: int, month: int, day: int, hour: int = 12) -> datetime.datetime:
    return datetime.datetime(year, month, day, hour, 0, 0, tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Raw data (the source of truth). Fresh copies each call so a test that mutates
# an object can never bleed into the next test.
# ---------------------------------------------------------------------------


def raw_devices() -> list[dict]:
    chips = [
        {
            "id": "dev-chip-1",
            "type": "Chip",
            "name": "SECRET_chip_one",
            "fabrication_date": _d(2025, 1, 3),
            "description": "SECRET_desc_chip_one",
            "tags": ["SECRET_tag_alpha"],
            "params": {
                "hierarchy": {"lot": "SECRET_lot_A9", "wafer": "SECRET_wafer_17"},
                "resistance": {"value": N_RESISTANCE, "unit": UNIT_OHM},
                "serial": "SECRET_serial_0001",
                "yield_pct": N_YIELD,
                "active": True,
            },
        },
        {
            "id": "dev-chip-2",
            "type": "Chip",
            "name": "SECRET_chip_two",
            "fabrication_date": _d(2026, 9, 12),
            "description": None,
            "tags": [],
            "params": {
                "hierarchy": {"lot": "SECRET_lot_B3", "wafer": "SECRET_wafer_42"},
                "resistance": {"value": N_RESISTANCE_2, "unit": UNIT_OHM},
                "serial": "SECRET_serial_0002",
                "yield_pct": N_YIELD_2,
                "active": False,
            },
        },
        {
            "id": "dev-chip-3",
            "type": "Chip",
            "name": "SECRET_chip_three",
            # No fabrication_date -> partial coverage.
            "fabrication_date": None,
            "description": None,
            "tags": [],
            "params": {
                # Missing 'wafer' and 'yield_pct' -> partial coverage of those paths.
                "hierarchy": {"lot": "SECRET_lot_C7"},
                "resistance": {"value": N_RESISTANCE_3, "unit": UNIT_OHM},
                "serial": "SECRET_serial_0003",
                "active": True,
            },
        },
    ]

    # A measurements dict keyed by 60 distinct run keys -> collapses to a map,
    # hiding the SECRET_ keys. Each value is a small record {peak, ts}.
    def measurements(seed: int) -> dict:
        return {
            f"SECRET_run_{i:04d}": {
                # Non-round distinctive floats so they can never coincide with a
                # count in the digest — though the map collapse hides them anyway.
                "peak": round(100.111 + seed * 0.07 + i * 0.010101, 6),
                "ts": _ts(2025, 6, 1 + (i % 27)).isoformat(),
            }
            for i in range(60)
        }

    wafers = [
        {
            "id": "dev-wafer-1",
            "type": "Wafer",
            "name": "SECRET_wafer_alpha",
            "fabrication_date": _d(2025, 2, 2),
            "description": "SECRET_desc_wafer",
            "tags": ["SECRET_tag_beta", "SECRET_tag_gamma"],
            "params": {
                "measurements": measurements(0),
                "diameter_mm": N_DIAMETER,
            },
        },
        {
            "id": "dev-wafer-2",
            "type": "Wafer",
            "name": "SECRET_wafer_beta",
            "fabrication_date": _d(2025, 3, 3),
            "description": None,
            "tags": [],
            "params": {
                "measurements": measurements(1),
                "diameter_mm": N_DIAMETER_2,
            },
        },
    ]

    sensors = [
        {
            "id": "dev-sensor-1",
            "type": "Sensor",
            "name": "SECRET_sensor_one",
            "fabrication_date": _d(2025, 4, 4),
            "description": None,
            "tags": ["SECRET_tag_delta"],
            "params": {
                "sweep": list(N_SWEEP[:5]),
                "iv_matrix": [list(row) for row in N_MATRIX],
                "label": "SECRET_sensor_label_1",
            },
        },
        {
            "id": "dev-sensor-2",
            "type": "Sensor",
            "name": "SECRET_sensor_two",
            "fabrication_date": _d(2025, 5, 5),
            "description": None,
            "tags": [],
            "params": {
                # Different length -> list length range is exercised.
                "sweep": list(N_SWEEP[:7]),
                "iv_matrix": [list(row) for row in N_MATRIX],
                "label": "SECRET_sensor_label_2",
            },
        },
    ]

    mixed = [
        {
            "id": "dev-mixed-1",
            "type": "Mixed",
            "name": "SECRET_mixed_one",
            "fabrication_date": _d(2025, 6, 6),
            "description": None,
            "tags": [],
            "params": {"flaky": N_FLAKY, "grade": "SECRET_grade_A"},
        },
        {
            "id": "dev-mixed-2",
            "type": "Mixed",
            "name": "SECRET_mixed_two",
            "fabrication_date": None,
            "description": None,
            "tags": [],
            "params": {"flaky": "SECRET_flaky_text", "grade": "SECRET_grade_B"},
        },
        {
            "id": "dev-mixed-3",
            "type": "Mixed",
            "name": "SECRET_mixed_three",
            "fabrication_date": None,
            "description": None,
            "tags": [],
            "params": {"flaky": True, "grade": "SECRET_grade_C"},
        },
    ]

    return chips + wafers + sensors + mixed


def raw_flows() -> list[dict]:
    return [
        {
            "id": "flow-1",
            "name": "IV sweep",
            "description": "SECRET_flow_desc_iv",
            "branch": "SECRET_branch_main",
            "script_filename": "iv_sweep.py",
            "tags": ["SECRET_flowtag_1"],
            "created_time": _ts(2025, 1, 1, 9),
            "username": "SECRET_user_ava",
            # Declared parameters: type + description are schema (plain); default is
            # present in the record but the digest drops it.
            "parameters": {
                "bias_max_v": {
                    "type": "float",
                    "default": 1.0,
                    "description": "Maximum bias voltage applied in the sweep",
                },
                "points": {
                    "type": "int",
                    "default": 201,
                    "description": "Number of samples across the sweep",
                },
            },
        },
        {
            "id": "flow-2",
            "name": "Transport map",
            "description": None,
            "branch": "SECRET_branch_dev",
            "script_filename": "transport_map.py",
            "tags": [],
            "created_time": _ts(2025, 1, 2, 9),
            "username": "SECRET_user_bo",
            "parameters": {
                "temperature_k": {
                    "type": "float",
                    "default": 4.2,
                    "description": "Sample temperature",
                }
            },
        },
        {
            "id": "flow-3",
            "name": "Yield audit",
            "description": None,
            "branch": "SECRET_branch_main",
            "script_filename": "yield_audit.py",
            "tags": [],
            "created_time": _ts(2025, 1, 3, 9),
            "username": "SECRET_user_cy",
            "parameters": {},
        },
    ]


def raw_runs() -> list[dict]:
    runs: list[dict] = []

    # Flow 1 — IV sweep: 4 runs on a Chip, 3 FINISHED + 1 FAILED, 3 with plots.
    iv_specs = [
        ("run-f1-001", "FINISHED", N_BIAS_MAX, N_RZERO, ["viz-1"]),
        ("run-f1-002", "FINISHED", N_BIAS_MAX, N_RZERO_2, ["viz-2"]),
        ("run-f1-003", "FINISHED", N_BIAS_MAX, N_RZERO, ["viz-3"]),
        ("run-f1-004", "FAILED", N_BIAS_MAX, N_RZERO_2, []),
    ]
    for idx, (rid, status, bias, rzero, viz) in enumerate(iv_specs):
        runs.append(
            {
                "id": rid,
                "flow_id": "flow-1",
                "flow_name": "IV sweep",
                "status": status,
                "created_time": _ts(2025, 7, 1 + idx, 10),
                "started_time": _ts(2025, 7, 1 + idx, 10),
                "finished_time": _ts(2025, 7, 1 + idx, 11) if status == "FINISHED" else None,
                "username": "SECRET_user_ava",
                "tags": ["SECRET_runtag_iv"] if idx == 0 else [],
                "comment": "SECRET_comment_iv" if idx == 0 else None,
                "device_ids": ["dev-chip-1"],
                "params": {"bias_max_v": bias, "bias_min_v": N_BIAS_MIN},
                "output": {"r_zero_ohm": rzero, "quality": "SECRET_quality_good"},
                "visualization_ids": viz,
            }
        )

    # Flow 2 — Transport map: 3 FINISHED runs on a Sensor, with a series output
    # (spectrum) and a matrix output (cap_matrix).
    for idx in range(3):
        runs.append(
            {
                "id": f"run-f2-{idx + 1:03d}",
                "flow_id": "flow-2",
                "flow_name": "Transport map",
                "status": "FINISHED",
                "created_time": _ts(2025, 8, 1 + idx, 10),
                "started_time": _ts(2025, 8, 1 + idx, 10),
                "finished_time": _ts(2025, 8, 1 + idx, 11),
                "username": "SECRET_user_bo",
                "tags": [],
                "comment": None,
                "device_ids": ["dev-sensor-1"],
                "params": {"temperature_k": N_TEMP},
                "output": {
                    "spectrum": list(N_SPECTRUM),
                    "cap_matrix": [list(row) for row in N_CAP_MATRIX],
                },
                "visualization_ids": ["viz-f2-1"] if idx == 0 else [],
            }
        )

    # Flow 3 — Yield audit: 3 runs on a Wafer, 2 FINISHED + 1 FAILED, mixed output.
    audit_specs = [
        ("run-f3-001", "FINISHED", N_SCORE, True),
        ("run-f3-002", "FINISHED", N_SCORE_2, False),
        ("run-f3-003", "FAILED", N_SCORE, False),
    ]
    for idx, (rid, status, score, passed) in enumerate(audit_specs):
        runs.append(
            {
                "id": rid,
                "flow_id": "flow-3",
                "flow_name": "Yield audit",
                "status": status,
                "created_time": _ts(2025, 9, 1 + idx, 10),
                "started_time": _ts(2025, 9, 1 + idx, 10),
                "finished_time": _ts(2025, 9, 1 + idx, 11) if status == "FINISHED" else None,
                "username": "SECRET_user_cy",
                "tags": [],
                "comment": None,
                "device_ids": ["dev-wafer-1"],
                "params": {"threshold": N_THRESHOLD},
                "output": {"score": score, "passed": passed, "note": "SECRET_note_audit"},
                "visualization_ids": [],
            }
        )

    return runs


def raw_visualizations() -> list[dict]:
    """Visualizations referenced by run ``visualization_ids``; data is raw bytes."""
    specs = [
        ("viz-1", "run-f1-001"),
        ("viz-2", "run-f1-002"),
        ("viz-3", "run-f1-003"),
        ("viz-f2-1", "run-f2-001"),
    ]
    return [
        {
            "id": vid,
            "type": "SVG",
            "filename": f"{vid}.svg",
            "flow_run_id": run_id,
            "timestamp": _ts(2025, 7, 1, 11),
            "data": b"<svg>SECRET_plot_bytes</svg>",
        }
        for vid, run_id in specs
    ]


# ---------------------------------------------------------------------------
# Wire-format serialization (spec §1). The Tunnel agent's server serializers
# must produce records equal to these.
# ---------------------------------------------------------------------------


def _iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _device_record(device: dict) -> dict:
    return {
        "id": device["id"],
        "name": device.get("name", ""),
        "type": device.get("type", "device"),
        "description": device.get("description"),
        "fabrication_date": _iso(device.get("fabrication_date")),
        "tags": list(device.get("tags") or []),
        "params": copy.deepcopy(device.get("params") or {}),
    }


def _flow_record(flow: dict) -> dict:
    return {
        "id": flow["id"],
        "name": flow["name"],
        "description": flow.get("description"),
        "branch": flow.get("branch"),
        "script_filename": flow.get("script_filename"),
        "tags": list(flow.get("tags") or []),
        "created_time": _iso(flow.get("created_time")),
        "username": flow.get("username"),
        "parameters": copy.deepcopy(flow.get("parameters") or {}),
    }


def _run_record(run: dict) -> dict:
    return {
        "id": run["id"],
        "flow_id": run.get("flow_id"),
        "flow_name": run.get("flow_name"),
        "status": str(run.get("status")),
        "created_time": _iso(run.get("created_time")),
        "started_time": _iso(run.get("started_time")),
        "finished_time": _iso(run.get("finished_time")),
        "username": run.get("username"),
        "tags": list(run.get("tags") or []),
        "comment": run.get("comment"),
        "device_ids": list(run.get("device_ids") or []),
        "params": copy.deepcopy(run.get("params") or {}),
        "output": copy.deepcopy(run.get("output") or {}),
        "visualization_ids": list(run.get("visualization_ids") or []),
    }


def to_records() -> dict[str, list[dict]]:
    """The fixture as §1 wire-format records: ``{devices, flows, runs}``.

    This is the contract the server serializers match. ``digest.build_digest``
    consumes exactly this.
    """
    return {
        "devices": [_device_record(d) for d in raw_devices()],
        "flows": [_flow_record(f) for f in raw_flows()],
        "runs": [_run_record(r) for r in raw_runs()],
    }


# ---------------------------------------------------------------------------
# Leak-test helpers
# ---------------------------------------------------------------------------


def _walk(value: Any, numbers: set, secrets: set, *, keys_too: bool) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        numbers.add(value)
    elif isinstance(value, str):
        if value.startswith("SECRET_"):
            secrets.add(value)
    elif isinstance(value, dict):
        for key, child in value.items():
            if keys_too and isinstance(key, str) and key.startswith("SECRET_"):
                secrets.add(key)
            _walk(child, numbers, secrets, keys_too=keys_too)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _walk(item, numbers, secrets, keys_too=keys_too)


def _collect() -> tuple[set, set]:
    numbers: set = set()
    secrets: set = set()
    for device in raw_devices():
        _walk(device.get("params") or {}, numbers, secrets, keys_too=True)
        for tag in device.get("tags") or []:
            _walk(tag, numbers, secrets, keys_too=True)
        _walk(device.get("name"), numbers, secrets, keys_too=True)
        _walk(device.get("description"), numbers, secrets, keys_too=True)
    for run in raw_runs():
        _walk(run.get("params") or {}, numbers, secrets, keys_too=True)
        _walk(run.get("output") or {}, numbers, secrets, keys_too=True)
        for tag in run.get("tags") or []:
            _walk(tag, numbers, secrets, keys_too=True)
        _walk(run.get("comment"), numbers, secrets, keys_too=True)
    for flow in raw_flows():
        _walk(flow.get("username"), numbers, secrets, keys_too=True)
        _walk(flow.get("description"), numbers, secrets, keys_too=True)
        _walk(flow.get("branch"), numbers, secrets, keys_too=True)
    return numbers, secrets


def fixture_numeric_values() -> list[float]:
    """Every distinctive measurement number in the fixture (for leak tests)."""
    numbers, _ = _collect()
    return sorted(numbers)


def fixture_secret_strings() -> list[str]:
    """Every ``SECRET_`` string in the fixture, including data-like keys/tags."""
    _, secrets = _collect()
    return sorted(secrets)
