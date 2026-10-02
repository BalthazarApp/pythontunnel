"""Tests for the data path (``blt_analytics.frames``) against the fixture space.

These use the ``fake_blt`` fixture (a fake *real* module, so no projection kwargs)
and assert on the real values the frames carry — this is the layer that *does* hand
over measurements. The cache is redirected to a ``tmp_path`` and bypassed with
``refresh=True`` where a test injects faults, so one case never reads another's
cached frame.
"""

from __future__ import annotations

import datetime

import pandas as pd
import pytest

from blt_analytics import _compat, frames
from fakes import fake_blt as _fake_blt_module
from fakes import fixture_space


@pytest.fixture(autouse=True)
def _tmp_cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("BLT_ANALYTICS_CACHE_DIR", str(tmp_path))
    yield tmp_path


def _new_log_messages(fake, before):
    return fake.logged_messages()[before:]


# ---------------------------------------------------------------------------
# devices_df
# ---------------------------------------------------------------------------


def test_devices_df_identity_columns_always_present(fake_blt):
    df = frames.devices_df("Chip")
    for col in ("id", "name", "type", "fabrication_date", "tags"):
        assert col in df.columns
    assert str(df["fabrication_date"].dtype).startswith("datetime64")
    assert isinstance(df["tags"].iloc[0], list)


def test_devices_df_flattens_small_dicts_to_dotted_columns(fake_blt):
    df = frames.devices_df("Chip").set_index("id")
    assert "hierarchy.lot" in df.columns
    assert "resistance.value" in df.columns
    assert df.loc["dev-chip-1", "resistance.value"] == fixture_space.N_RESISTANCE
    assert df.loc["dev-chip-1", "hierarchy.lot"] == "SECRET_lot_A9"
    # Partial coverage: chip-3 has no yield_pct -> NaN, no wafer -> NaN.
    assert pd.isna(df.loc["dev-chip-3", "yield_pct"])
    assert pd.isna(df.loc["dev-chip-3", "hierarchy.wafer"])


def test_devices_df_map_stays_whole_object_column(fake_blt):
    df = frames.devices_df("Wafer")
    # The 60-key measurements dict is a map: one object column, not 60 columns.
    assert "measurements" in df.columns
    assert not any(c.startswith("measurements.") for c in df.columns)
    value = df["measurements"].iloc[0]
    assert isinstance(value, dict) and len(value) == 60
    assert df["diameter_mm"].iloc[0] == fixture_space.N_DIAMETER


def test_devices_df_lists_and_matrices_stay_object_columns(fake_blt):
    df = frames.devices_df("Sensor").set_index("id")
    assert isinstance(df.loc["dev-sensor-1", "sweep"], list)
    assert isinstance(df.loc["dev-sensor-1", "iv_matrix"], list)
    assert df.loc["dev-sensor-1", "label"] == "SECRET_sensor_label_1"


def test_devices_df_mixed_kind_column_keeps_raw_values(fake_blt):
    df = frames.devices_df("Mixed").set_index("id")
    assert df.loc["dev-mixed-1", "flaky"] == fixture_space.N_FLAKY
    assert df.loc["dev-mixed-2", "flaky"] == "SECRET_flaky_text"
    assert df.loc["dev-mixed-3", "flaky"] is True


def test_devices_df_explicit_columns_projection(fake_blt):
    df = frames.devices_df("Chip", columns=["hierarchy.lot", "resistance.value"])
    # Identity + exactly the requested projection, nothing else flattened.
    assert set(df.columns) == {
        "id", "name", "type", "fabrication_date", "tags",
        "hierarchy.lot", "resistance.value",
    }


def test_devices_df_missing_path_becomes_all_nan_column(fake_blt):
    df = frames.devices_df("Chip", columns=["does.not.exist"])
    assert "does.not.exist" in df.columns
    assert df["does.not.exist"].isna().all()


def test_devices_df_all_types(fake_blt):
    df = frames.devices_df()
    assert len(df) == len(fixture_space.raw_devices())
    assert set(df["type"]) == {"Chip", "Wafer", "Sensor", "Mixed"}


# ---------------------------------------------------------------------------
# runs_df
# ---------------------------------------------------------------------------


def test_runs_df_columns_and_identity(fake_blt):
    df = frames.runs_df("IV sweep")
    for col in frames._RUN_IDENTITY:
        assert col in df.columns
    assert "param.bias_max_v" in df.columns
    assert "output.r_zero_ohm" in df.columns
    indexed = df.set_index("run_id")
    assert indexed.loc["run-f1-001", "output.r_zero_ohm"] == fixture_space.N_RZERO
    assert indexed.loc["run-f1-001", "device_ids"] == ["dev-chip-1"]


def test_runs_df_duration_and_datetimes(fake_blt):
    df = frames.runs_df("IV sweep").set_index("run_id")
    assert str(df["created_time"].dtype).startswith("datetime64")
    assert df.loc["run-f1-001", "duration_s"] == 3600.0
    assert pd.isna(df.loc["run-f1-004", "duration_s"])  # FAILED run has no finish


def test_runs_df_all_flows(fake_blt):
    df = frames.runs_df()
    assert len(df) == len(fixture_space.raw_runs())
    assert set(df["flow_name"]) == {"IV sweep", "Transport map", "Yield audit"}


def test_runs_df_by_flow_id(fake_blt):
    df = frames.runs_df("flow-1")
    assert set(df["run_id"]) == {"run-f1-001", "run-f1-002", "run-f1-003", "run-f1-004"}


def test_runs_df_status_filter_str_and_list(fake_blt):
    assert len(frames.runs_df("IV sweep", status="FINISHED")) == 3
    assert len(frames.runs_df("IV sweep", status=["FINISHED", "FAILED"])) == 4
    assert len(frames.runs_df("IV sweep", status="finished")) == 3  # case-insensitive


def test_runs_df_status_column_is_the_bare_member_name(fake_blt):
    # The fake's run.status is a PyO3-style enum (str() -> "FlowRunStatus.FINISHED");
    # the frame must carry the bare name, else status filtering and display both break.
    df = frames.runs_df("IV sweep").set_index("run_id")
    assert df.loc["run-f1-001", "status"] == "FINISHED"
    assert df.loc["run-f1-004", "status"] == "FAILED"


def test_runs_df_status_filter_accepts_a_real_enum(fake_blt):
    # A caller may hand a FlowRunStatus enum straight through, not just a string.
    only = frames.runs_df("IV sweep", status=fake_blt.FlowRunStatus.FAILED)
    assert set(only["status"]) == {"FAILED"}


def test_runs_df_since_filter(fake_blt):
    df = frames.runs_df(since=datetime.datetime(2025, 8, 15, tzinfo=datetime.timezone.utc))
    assert set(df["flow_name"]) == {"Yield audit"}  # only September runs survive


def test_runs_df_explicit_output_projection(fake_blt):
    df = frames.runs_df("Transport map", columns=["output.spectrum", "output.cap_matrix"])
    expected = set(frames._RUN_IDENTITY) | {"output.spectrum", "output.cap_matrix"}
    assert set(df.columns) == expected


# ---------------------------------------------------------------------------
# reshaping helpers
# ---------------------------------------------------------------------------


def test_explode_devices_one_row_per_device_and_merge(fake_blt):
    runs = frames.runs_df("IV sweep")
    exploded = frames.explode_devices(runs)
    assert len(exploded) == 4
    assert set(exploded["device_id"]) == {"dev-chip-1"}

    devs = frames.devices_df("Chip", columns=["resistance.value"])
    merged = exploded.merge(devs, left_on="device_id", right_on="id", how="inner")
    assert len(merged) == 4
    assert (merged["resistance.value"] == fixture_space.N_RESISTANCE).all()


def test_series_to_df_explodes_a_list_column(fake_blt):
    runs = frames.runs_df("Transport map", columns=["output.spectrum"])
    one = runs.head(1)
    long = frames.series_to_df(one, "output.spectrum")
    assert list(long["output.spectrum"]) == list(fixture_space.N_SPECTRUM)
    assert list(long["i"]) == list(range(len(fixture_space.N_SPECTRUM)))


def test_matrix_to_df_explodes_a_matrix_column(fake_blt):
    runs = frames.runs_df("Transport map", columns=["output.cap_matrix"])
    one = runs.head(1)
    long = frames.matrix_to_df(one, "output.cap_matrix")
    rows, cols = len(fixture_space.N_CAP_MATRIX), len(fixture_space.N_CAP_MATRIX[0])
    assert len(long) == rows * cols
    grid = long.pivot(index="row", columns="col", values="value")
    assert grid.loc[0, 0] == fixture_space.N_CAP_MATRIX[0][0]
    assert grid.loc[rows - 1, cols - 1] == fixture_space.N_CAP_MATRIX[-1][-1]


# ---------------------------------------------------------------------------
# paging robustness via the fixture's fault injection
# ---------------------------------------------------------------------------


def test_runs_df_shrink_and_retry_on_too_large_page(fake_blt):
    before = len(fake_blt.logged_messages())
    fake_blt.fail_page(1)  # first history call for every flow raises -> shrink + retry
    df = frames.runs_df("IV sweep", refresh=True)
    assert set(df["run_id"]) == {"run-f1-001", "run-f1-002", "run-f1-003", "run-f1-004"}
    msgs = _new_log_messages(fake_blt, before)
    assert any("retrying with smaller page" in m for _, m in msgs)


def test_runs_df_skips_a_poison_run(fake_blt):
    before = len(fake_blt.logged_messages())
    fake_blt.fail_run("run-f1-002")  # any page containing it raises
    df = frames.runs_df("IV sweep", refresh=True)
    assert set(df["run_id"]) == {"run-f1-001", "run-f1-003", "run-f1-004"}  # 002 skipped
    msgs = _new_log_messages(fake_blt, before)
    assert any("skipping poison run" in m for _, m in msgs)


# ---------------------------------------------------------------------------
# PyO3 enum compatibility (_compat.enum_name) and the fake's realism
# ---------------------------------------------------------------------------


def test_enum_name_recovers_bare_member():
    assert _compat.enum_name(_fake_blt_module.FlowRunStatus.FINISHED) == "FINISHED"
    assert _compat.enum_name(_fake_blt_module.VisualizationDataType.SVG) == "SVG"
    assert _compat.enum_name("FINISHED") == "FINISHED"  # bare string passes through
    assert _compat.enum_name("FlowRunStatus.FINISHED") == "FINISHED"  # already qualified
    assert _compat.enum_name(None) is None


def test_fake_flow_run_status_behaves_like_a_pyo3_enum():
    finished = _fake_blt_module.FlowRunStatus.FINISHED
    assert str(finished) == "FlowRunStatus.FINISHED"  # qualified, not bare
    assert repr(finished) == "FlowRunStatus.FINISHED"
    assert not hasattr(finished, "name")  # PyO3 enums expose no .name
    assert finished is _fake_blt_module.FlowRunStatus.FINISHED  # singleton
    assert finished != _fake_blt_module.FlowRunStatus.FAILED
    assert finished != "FINISHED"  # never equals a plain string


def test_fake_visualization_type_behaves_like_a_pyo3_enum():
    svg = _fake_blt_module.VisualizationDataType.SVG
    assert str(svg) == "VisualizationDataType.SVG"
    assert not hasattr(svg, "name")
    [viz] = _fake_blt_module.fetch_visualizations(["viz-1"]).values()
    assert viz.type is _fake_blt_module.VisualizationDataType.SVG
