"""Tests for ``blt_analytics.digest.build_digest``.

Covers the kinds, coverage arithmetic, map collapse, matrix/list detection, mixed
kinds, unit detection, date first/last, strict determinism, and — the one that
guards the whole design — that no measurement value leaks into the digest.
"""

from __future__ import annotations

import json

import pytest

import numpy as np

from blt_analytics.digest import build_digest, flow_key
from blt_analytics.digest import _full_kind
from fakes import fixture_space

BUILT_AT = "2026-10-02T12:00:00Z"


@pytest.fixture
def digest(fixture_records):
    return build_digest(
        fixture_records["devices"],
        fixture_records["flows"],
        fixture_records["runs"],
        built_at=BUILT_AT,
    )


# ---------------------------------------------------------------------------
# Totals and envelope
# ---------------------------------------------------------------------------


def test_envelope_and_totals(digest):
    assert digest["version"] == 1
    assert digest["built_at"] == BUILT_AT
    totals = digest["totals"]
    assert totals["devices"] == 10
    assert totals["device_types"] == 4
    assert totals["flows"] == 3
    assert totals["runs"] == 10
    assert totals["runs_truncated"] is False


def test_default_built_at_is_filled(fixture_records):
    d = build_digest(fixture_records["devices"], fixture_records["flows"], fixture_records["runs"])
    # Not asserting the value (it is "now"), only that it is a non-empty string.
    assert isinstance(d["built_at"], str) and d["built_at"]


# ---------------------------------------------------------------------------
# Device-type params: kinds, coverage, units, flattening
# ---------------------------------------------------------------------------


def test_chip_scalar_kinds_and_flattening(digest):
    params = digest["device_types"]["Chip"]["params"]
    assert params["hierarchy.lot"]["kind"] == "string"
    assert params["hierarchy.lot"]["distinct"] == 3
    assert params["resistance.value"]["kind"] == "number"
    assert params["serial"]["kind"] == "string"
    assert params["active"]["kind"] == "bool"
    # bool distinct is a count (True/False), never the booleans themselves.
    assert params["active"]["distinct"] == 2


def test_chip_partial_coverage(digest):
    params = digest["device_types"]["Chip"]["params"]
    # Present on all three chips.
    assert params["hierarchy.lot"]["coverage"] == 100
    # 'wafer' and 'yield_pct' only on two of three chips -> 67%.
    assert params["hierarchy.wafer"]["coverage"] == 67
    assert params["yield_pct"]["coverage"] == 67


def test_unit_detection_from_sibling(digest):
    value = digest["device_types"]["Chip"]["params"]["resistance.value"]
    assert value["unit"] == "ohm"
    # The sibling 'unit' key is still flattened as its own string path, by count.
    unit_path = digest["device_types"]["Chip"]["params"]["resistance.unit"]
    assert unit_path["kind"] == "string"


def test_fabrication_date_first_last(digest):
    fab = digest["device_types"]["Chip"]["fabrication_date"]
    assert fab["coverage"] == 67  # two of three chips dated
    assert fab["first"] == "2025-01-03"
    assert fab["last"] == "2026-09-12"


def test_tags_reported_as_counts_only(digest):
    tags = digest["device_types"]["Chip"]["tags"]
    assert tags["count_distinct"] == 1
    assert tags["coverage"] == 33


# ---------------------------------------------------------------------------
# Map collapse
# ---------------------------------------------------------------------------


def test_measurements_collapse_to_map(digest):
    measurements = digest["device_types"]["Wafer"]["params"]["measurements"]
    assert measurements["kind"] == "map"
    assert measurements["key_count"] == 60
    # The 60 SECRET_ run keys must NOT be flattened into their own paths.
    assert all(
        not path.startswith("measurements.")
        for path in digest["device_types"]["Wafer"]["params"]
    )
    # The merged value shape is reported by its key names only.
    value = measurements["value"]
    assert value["kind"] == "dict"
    assert value["keys"] == ["peak", "ts"]


# ---------------------------------------------------------------------------
# List and matrix detection
# ---------------------------------------------------------------------------


def test_list_detection_with_length_range(digest):
    sweep = digest["device_types"]["Sensor"]["params"]["sweep"]
    assert sweep["kind"] == "list"
    assert sweep["item_kind"] == "number"
    assert sweep["length"] == {"min": 5, "max": 7}


def test_matrix_detection_shape(digest):
    matrix = digest["device_types"]["Sensor"]["params"]["iv_matrix"]
    assert matrix["kind"] == "matrix"
    assert matrix["shape"] == {"rows": [2, 2], "cols": [3, 3]}


# ---------------------------------------------------------------------------
# Mixed kinds
# ---------------------------------------------------------------------------


def test_mixed_kinds(digest):
    flaky = digest["device_types"]["Mixed"]["params"]["flaky"]
    assert flaky["kind"] == "mixed"
    assert flaky["kinds"] == {"number": 1, "string": 1, "bool": 1}


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


def test_flow_overview(digest):
    flow = digest["flows"]["IV sweep"]
    assert flow["id"] == "flow-1"
    assert flow["run_count"] == 4
    assert flow["runs_sampled"] == 4
    assert flow["truncated"] is False
    assert flow["status"] == {"FINISHED": 3, "FAILED": 1}
    assert flow["runs_with_plots"] == 3
    assert flow["device_types"] == {"Chip": 4}


def test_flow_run_date_range(digest):
    flow = digest["flows"]["IV sweep"]
    assert flow["first_run"] == "2025-07-01T10:00:00+00:00"
    assert flow["last_run"] == "2025-07-04T10:00:00+00:00"


def test_declared_parameters_type_and_description_no_default(digest):
    declared = digest["flows"]["IV sweep"]["declared_parameters"]
    assert declared["bias_max_v"]["type"] == "float"
    assert "description" in declared["bias_max_v"]
    # Defaults are deliberately dropped from the digest.
    assert "default" not in declared["bias_max_v"]
    assert declared["points"]["type"] == "int"


def test_flow_inputs_and_outputs(digest):
    flow = digest["flows"]["IV sweep"]
    assert flow["inputs"]["bias_max_v"]["kind"] == "number"
    assert flow["inputs"]["bias_min_v"]["kind"] == "number"
    assert flow["outputs"]["r_zero_ohm"]["kind"] == "number"
    assert flow["outputs"]["quality"]["kind"] == "string"


def test_series_and_matrix_outputs(digest):
    outputs = digest["flows"]["Transport map"]["outputs"]
    assert outputs["spectrum"]["kind"] == "list"
    assert outputs["spectrum"]["item_kind"] == "number"
    assert outputs["cap_matrix"]["kind"] == "matrix"
    assert outputs["cap_matrix"]["shape"] == {"rows": [3, 3], "cols": [2, 2]}


def test_failed_runs_and_bool_output(digest):
    flow = digest["flows"]["Yield audit"]
    assert flow["status"] == {"FINISHED": 2, "FAILED": 1}
    passed = flow["outputs"]["passed"]
    assert passed["kind"] == "bool"
    assert passed["distinct"] == 2


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_determinism_byte_identical():
    r1 = fixture_space.to_records()
    r2 = fixture_space.to_records()
    d1 = build_digest(r1["devices"], r1["flows"], r1["runs"], built_at=BUILT_AT)
    d2 = build_digest(r2["devices"], r2["flows"], r2["runs"], built_at=BUILT_AT)
    assert json.dumps(d1, sort_keys=True) == json.dumps(d2, sort_keys=True)


def test_digest_is_json_serializable(digest):
    # Must round-trip without a custom encoder — no datetimes, sets or bytes.
    assert json.loads(json.dumps(digest)) == digest


# ---------------------------------------------------------------------------
# Leak test (required): no measurement value reaches the digest
# ---------------------------------------------------------------------------


def test_no_measurement_values_leak(digest):
    blob = json.dumps(digest)
    for secret in fixture_space.fixture_secret_strings():
        assert secret not in blob, f"secret string leaked into digest: {secret}"
    for number in fixture_space.fixture_numeric_values():
        assert str(number) not in blob, f"numeric value leaked into digest: {number}"


# ---------------------------------------------------------------------------
# Structural keys below a collapsed map (fixture data kept inline here)
# ---------------------------------------------------------------------------


def _device(dtype, params):
    return {"id": f"dev-{dtype}", "type": dtype, "name": "n", "fabrication_date": None,
            "tags": [], "params": params}


def _digest_of(devices, *, max_keys=3, max_depth=4):
    return build_digest(devices, [], [], built_at=BUILT_AT, max_keys=max_keys, max_depth=max_depth)


def test_map_entries_keep_structural_keys_and_count_data_keys():
    # 5 run-keyed entries (> max_keys=3 -> the outer dict collapses to a map).
    # Each entry shares the structural keys peak/ts (100%), plus a unique,
    # data-like SECRET_ key that appears in only one entry (20% < 50%).
    measurements = {
        f"run_{i}": {
            "peak": 100.111 + i,
            "ts": "2025-06-01T00:00:00+00:00",
            f"SECRET_data_{i}": 900.0 + i,
        }
        for i in range(5)
    }
    d = _digest_of([_device("W", {"measurements": measurements})])
    m = d["device_types"]["W"]["params"]["measurements"]
    assert m["kind"] == "map"
    assert m["key_count"] == 5
    value = m["value"]
    assert value["kind"] == "dict"
    assert value["keys"] == ["peak", "ts"]          # structural names survive
    assert value["other_keys"] == 5                  # the 5 data-like keys, counted not named

    blob = json.dumps(d)
    assert "SECRET_data_0" not in blob               # data-like keys never surface
    assert "900.0" not in blob and "100.111" not in blob  # nor the leaf values


def test_map_of_maps_collapses_to_nested_map():
    # Every entry is itself a dict whose single key is unique -> no inner key
    # reaches 50% -> the value collapses to a NESTED map carrying only key_count.
    blob_param = {f"outer_{i}": {f"SECRET_inner_{i}": 1.23 + i} for i in range(5)}
    d = _digest_of([_device("B", {"blob": blob_param})])
    field = d["device_types"]["B"]["params"]["blob"]
    assert field["kind"] == "map"
    assert field["key_count"] == 5
    nested = field["value"]
    assert nested["kind"] == "map"
    assert nested["key_count"] == 5
    assert "keys" not in nested and "value" not in nested   # only key_count below a map
    assert "SECRET_inner_0" not in json.dumps(d)


def test_depth_cap_below_a_map_lists_no_keys():
    # At the depth budget, a dict below a map reports only key_count, never keys,
    # even when its entries share clean structural names.
    measurements = {f"run_{i}": {"peak": 1.0 + i, "ts": "2025-06-01"} for i in range(5)}
    d = _digest_of([_device("W", {"measurements": measurements})], max_depth=1)
    value = d["device_types"]["W"]["params"]["measurements"]["value"]
    assert value["kind"] == "map"
    assert "keys" not in value


def test_depth_cap_outside_a_map_still_lists_keys():
    # The same depth budget, but NOT below a map: child key names are structural
    # paths and remain allowed.
    d = _digest_of([_device("C", {"hierarchy": {"lot": "x", "wafer": "y"}})], max_depth=1)
    field = d["device_types"]["C"]["params"]["hierarchy"]
    assert field["kind"] == "dict"
    assert field["keys"] == ["lot", "wafer"]


# ---------------------------------------------------------------------------
# flow_key: falsy names and name collisions
# ---------------------------------------------------------------------------


def test_flow_key_falsy_and_collisions():
    flows = [
        {"id": "a", "name": "Solo"},
        {"id": "b", "name": "Dup"},
        {"id": "c", "name": "Dup"},
        {"id": "d", "name": None},
        {"id": "e", "name": ""},
    ]
    assert flow_key(flows) == {
        "a": "Solo",
        "b": "Dup [b]",
        "c": "Dup [c]",
        "d": "d",
        "e": "e",
    }


def test_build_digest_uses_flow_key_for_collisions_and_none():
    flows = [
        {"id": "f1", "name": "Dup", "parameters": {}},
        {"id": "f2", "name": "Dup", "parameters": {}},
        {"id": "f3", "name": None, "parameters": {}},
    ]
    d = build_digest([], flows, [], built_at=BUILT_AT)
    assert set(d["flows"]) == {"Dup [f1]", "Dup [f2]", "f3"}
    # Both colliding flows survive (neither overwrote the other).
    assert d["flows"]["Dup [f1]"]["id"] == "f1"
    assert d["flows"]["Dup [f2]"]["id"] == "f2"


# ---------------------------------------------------------------------------
# _full_kind: numpy scalars and the "other" kind
# ---------------------------------------------------------------------------


def test_full_kind_numpy_scalars():
    assert _full_kind(np.int64(5)) == "integer"
    assert _full_kind(np.int32(5)) == "integer"
    assert _full_kind(np.float64(5.0)) == "number"
    assert _full_kind(np.float32(5.0)) == "number"
    # Plain Python still classifies as before (bool is not an integer).
    assert _full_kind(True) == "bool"
    assert _full_kind(5) == "integer"
    assert _full_kind(5.0) == "number"


def test_full_kind_other_for_unknown_objects():
    assert _full_kind(object()) == "other"
    assert _full_kind(b"bytes") == "other"
    assert _full_kind(complex(1, 2)) == "other"


def test_numpy_int_param_is_integer_end_to_end():
    d = _digest_of([
        _device("N", {"count": np.int64(7)}),
        _device("N2", {"count": np.int64(8)}),
    ], max_keys=50)
    # numpy ints must not be mislabelled "string" (the old fall-through bug).
    assert d["device_types"]["N"]["params"]["count"]["kind"] == "integer"
    # And the digest stays JSON-serializable with no numpy types leaking through.
    assert json.loads(json.dumps(d)) == d
