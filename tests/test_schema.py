"""Tests for ``blt_analytics.schema`` — the schema-only tool functions.

Three concerns: that the tools are faithful views over the digest (overview,
device/flow schema, describe_param/output, find, load_snippet), that
``get_digest`` acquires and memoizes the digest correctly (tunnel op, local-build
fallback, and the private §1 record serializers that feed the local path), and —
the one that guards the whole design — that no measurement value and no secret
string ever leaks into any tool's JSON output.
"""

from __future__ import annotations

import json

import pytest

from blt_analytics import digest as digest_mod
from blt_analytics import schema
from fakes import fixture_space

BUILT_AT = "2026-10-02T12:00:00Z"


@pytest.fixture(autouse=True)
def _reset_schema():
    """Clear injected/memoized digest around every test (no state bleed)."""
    schema.reset()
    yield
    schema.reset()


@pytest.fixture
def digest():
    recs = fixture_space.to_records()
    return digest_mod.build_digest(
        recs["devices"], recs["flows"], recs["runs"], built_at=BUILT_AT
    )


@pytest.fixture
def injected(digest):
    schema.set_digest(digest)
    return digest


# ---------------------------------------------------------------------------
# overview
# ---------------------------------------------------------------------------


def test_overview_totals_and_counts(injected):
    out = schema.overview()
    assert out["totals"]["devices"] == 10
    assert out["totals"]["device_types"] == 4
    assert out["device_types"]["Chip"] == {"count": 3, "param_count": 7}
    assert out["flows"]["IV sweep"]["run_count"] == 4
    assert out["flows"]["IV sweep"]["id"] == "flow-1"
    assert out["flows"]["IV sweep"]["first_run"].startswith("2025-07-01")


def test_overview_is_json_serializable(injected):
    json.dumps(schema.overview())  # must not raise


# ---------------------------------------------------------------------------
# device_schema / describe_param
# ---------------------------------------------------------------------------


def test_device_schema_params_top_level_first(injected):
    out = schema.device_schema("Chip")
    assert out["count"] == 3
    paths = list(out["params"])
    # Undotted paths precede dotted ones.
    first_dotted = next(i for i, p in enumerate(paths) if "." in p)
    assert all("." not in p for p in paths[:first_dotted])
    assert out["params"]["resistance.value"]["unit"] == "ohm"


def test_device_schema_unknown_type_suggests(injected):
    out = schema.device_schema("Chop")
    assert "error" in out
    assert "Chip" in out["suggestions"]
    assert "error" not in schema.device_schema("Chip")


def test_describe_param_intermediate_dict_lists_children(injected):
    out = schema.describe_param("Chip", "hierarchy")
    # 'hierarchy' is flattened, so it is not itself a field — only its children.
    assert "field" not in out
    assert set(out["children"]) == {"hierarchy.lot", "hierarchy.wafer"}


def test_describe_param_map_has_field_no_children(injected):
    out = schema.describe_param("Wafer", "measurements")
    assert out["field"]["kind"] == "map"
    assert out["field"]["key_count"] == 60
    assert out["children"] == {}


def test_describe_param_empty_path_lists_top_level(injected):
    out = schema.describe_param("Chip", "")
    assert "hierarchy.lot" not in out["children"]  # not a top-level path
    assert "yield_pct" in out["children"]


def test_describe_param_unknown_path_errors(injected):
    out = schema.describe_param("Chip", "nope")
    assert "error" in out


# ---------------------------------------------------------------------------
# flow_schema / describe_output
# ---------------------------------------------------------------------------


def test_flow_schema_by_name_and_id_agree(injected):
    by_name = schema.flow_schema("Transport map")
    by_id = schema.flow_schema("flow-2")
    assert by_name == by_id
    assert by_name["flow"] == "Transport map"
    assert by_name["status"] == {"FINISHED": 3}
    assert by_name["declared_parameters"]["temperature_k"]["type"] == "float"
    # Declared parameters never carry a default (schema-only).
    assert "default" not in by_name["declared_parameters"]["temperature_k"]


def test_flow_schema_unknown_suggests(injected):
    out = schema.flow_schema("IV sweeep")
    assert "error" in out
    assert "IV sweep" in out["suggestions"]


def test_describe_output_matrix(injected):
    out = schema.describe_output("Transport map", "cap_matrix")
    assert out["field"]["kind"] == "matrix"
    assert out["field"]["shape"]["rows"] == [3, 3]


# ---------------------------------------------------------------------------
# find
# ---------------------------------------------------------------------------


def test_find_ranks_param_and_flow(injected):
    out = schema.find("yield")
    kinds = {(m["kind"], m.get("device_type") or m.get("flow"), m.get("path")) for m in out["matches"]}
    assert ("device_param", "Chip", "yield_pct") in kinds
    assert ("flow", "Yield audit", None) in kinds
    # Top match is the strongest, sorted descending.
    scores = [m["score"] for m in out["matches"]]
    assert scores == sorted(scores, reverse=True)


def test_find_kinds_are_the_spec_set(injected):
    allowed = {"device_type", "device_param", "flow", "flow_input", "flow_output"}
    for query in ("bias", "resistance", "matrix", "temperature", "chip", "score"):
        for match in schema.find(query)["matches"]:
            assert match["kind"] in allowed


def test_find_respects_limit(injected):
    out = schema.find("e", limit=2)  # 'e' matches broadly
    assert len(out["matches"]) <= 2


# ---------------------------------------------------------------------------
# load_snippet
# ---------------------------------------------------------------------------


def test_load_snippet_device_only(injected):
    code = schema.load_snippet(device_type="Chip")["code"]
    assert "ba.devices_df('Chip'" in code
    assert "import blt_analytics as ba" in code


def test_load_snippet_flow_uses_param_output_prefixes(injected):
    code = schema.load_snippet(flow="IV sweep")["code"]
    assert "ba.runs_df('IV sweep'" in code
    assert "param.bias_max_v" in code
    assert "output.r_zero_ohm" in code


def test_load_snippet_join_when_both(injected):
    code = schema.load_snippet(device_type="Chip", flow="IV sweep")["code"]
    assert "ba.explode_devices(runs)" in code
    assert 'left_on="device_id"' in code


def test_load_snippet_only_references_existing_columns(injected):
    code = schema.load_snippet(device_type="Chip", columns=["nonexistent", "yield_pct"])["code"]
    assert "nonexistent" not in code
    assert "yield_pct" in code


def test_load_snippet_unknown_flow_errors(injected):
    assert "error" in schema.load_snippet(flow="ghost")


# ---------------------------------------------------------------------------
# Truncation (listings over 100)
# ---------------------------------------------------------------------------


def test_overview_truncates_large_listings():
    many_types = {f"T{i:03d}": {"count": 1, "params": {}} for i in range(150)}
    schema.set_digest({"version": 1, "built_at": BUILT_AT, "totals": {}, "device_types": many_types, "flows": {}})
    out = schema.overview()
    assert len(out["device_types"]) == 100
    assert out["device_types_truncated"] == 50


def test_device_schema_truncates_params():
    params = {f"p{i:03d}": {"kind": "number", "coverage": 100} for i in range(130)}
    schema.set_digest(
        {"version": 1, "built_at": BUILT_AT, "totals": {},
         "device_types": {"Big": {"count": 1, "params": params}}, "flows": {}}
    )
    out = schema.device_schema("Big")
    assert len(out["params"]) == 100
    assert out["params_truncated"] == 30


# ---------------------------------------------------------------------------
# get_digest: local build, serializers, memoization, tunnel path, fallback
# ---------------------------------------------------------------------------


def test_record_serializers_match_fixture(fake_blt):
    """The private §1 serializers reproduce fixture_space.to_records() exactly."""
    devices = [schema._device_to_record(d) for d in fake_blt.search_devices()]
    flows = [schema._flow_to_record(f) for f in fake_blt.search_flows()]
    runs = []
    for frec in flows:
        runs.extend(schema._page_runs(fake_blt, frec["id"]))

    expected = fixture_space.to_records()
    by_id = lambda records: sorted(records, key=lambda r: r["id"])  # noqa: E731
    assert by_id(devices) == by_id(expected["devices"])
    assert by_id(flows) == by_id(expected["flows"])
    assert by_id(runs) == by_id(expected["runs"])


def test_run_record_status_is_bare_name_from_pyo3_enum(fake_blt):
    """``_run_to_record`` must reduce a real-Runner PyO3 status enum to its bare name.

    The fake's ``run.status`` is a PyO3-style enum whose ``str()`` is the qualified
    ``"FlowRunStatus.FINISHED"``; without ``enum_name`` the record (and every digest
    status key built from it) would carry that prefix.
    """
    [run] = fake_blt.search_flow_run_history(flow_run_ids=["run-f1-001"])
    assert str(run.status) == "FlowRunStatus.FINISHED"  # the trap the fix guards against
    assert schema._run_to_record(run)["status"] == "FINISHED"


def test_local_digest_status_keys_are_bare_names(fake_blt):
    """Status counts in the locally built digest key on bare names, not the enum repr."""
    d = schema.get_digest()
    assert d["flows"]["IV sweep"]["status"] == {"FINISHED": 3, "FAILED": 1}


def test_get_digest_local_build_against_runner(fake_blt):
    """A real/fake Runner (not a tunnel) triggers the local digest build."""
    d = schema.get_digest()
    expected = digest_mod.build_digest(**fixture_space.to_records())
    # built_at differs (it is 'now'); everything else must match.
    d_cmp = {k: v for k, v in d.items() if k != "built_at"}
    e_cmp = {k: v for k, v in expected.items() if k != "built_at"}
    assert d_cmp == e_cmp


def test_get_digest_memoized_and_refresh(fake_blt, monkeypatch):
    first = schema.get_digest()
    assert schema.get_digest() is first  # memoized, no rebuild
    calls = {"n": 0}
    real_build = digest_mod.build_digest

    def counting_build(*a, **k):
        calls["n"] += 1
        return real_build(*a, **k)

    monkeypatch.setattr(schema._digest_mod, "build_digest", counting_build)
    refreshed = schema.get_digest(refresh=True)
    assert calls["n"] == 1
    assert refreshed is not first


# ---------------------------------------------------------------------------
# Leak test (spec §5, REQUIRED): no measurement value or secret in any output
# ---------------------------------------------------------------------------


def _all_tool_outputs(digest):
    """Every tool, called on every name in the fixture, as a list of result dicts."""
    schema.set_digest(digest)
    outputs = [schema.overview(), schema.get_digest()]

    for dtype, entry in digest["device_types"].items():
        outputs.append(schema.device_schema(dtype))
        outputs.append(schema.describe_param(dtype, ""))
        seen_prefixes = set()
        for path in entry["params"]:
            outputs.append(schema.describe_param(dtype, path))
            if "." in path:
                prefix = path.split(".", 1)[0]
                if prefix not in seen_prefixes:
                    seen_prefixes.add(prefix)
                    outputs.append(schema.describe_param(dtype, prefix))
        outputs.append(schema.load_snippet(device_type=dtype))

    for fname, entry in digest["flows"].items():
        outputs.append(schema.flow_schema(fname))
        outputs.append(schema.flow_schema(entry["id"]))
        outputs.append(schema.describe_output(fname, ""))
        for path in entry["outputs"]:
            outputs.append(schema.describe_output(fname, path))
        outputs.append(schema.load_snippet(flow=fname))
        # a join snippet for good measure
        first_dtype = next(iter(digest["device_types"]))
        outputs.append(schema.load_snippet(device_type=first_dtype, flow=fname))

    for query in (
        "yield", "resistance", "bias", "matrix", "temperature", "score",
        "measurements", "flaky", "chip", "sweep", "SECRET", "lot",
    ):
        outputs.append(schema.find(query))

    return outputs


def test_no_measurement_or_secret_leaks(digest):
    numbers = fixture_space.fixture_numeric_values()
    secrets = fixture_space.fixture_secret_strings()
    assert numbers and secrets  # guard: the fixture actually has values to leak

    for result in _all_tool_outputs(digest):
        blob = json.dumps(result, default=str)
        for secret in secrets:
            assert secret not in blob, f"secret {secret!r} leaked into {result!r}"
        for number in numbers:
            for form in {repr(number), str(number)}:
                assert form not in blob, f"number {form} leaked into {result!r}"
