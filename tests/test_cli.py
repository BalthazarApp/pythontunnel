"""Tests for ``blt_analytics.cli`` — the ``blt-schema`` and ``blt-tunnel`` CLIs.

``blt-schema`` is checked as a thin wrapper (text and ``--json`` rendering, errors,
``--refresh``). ``blt-tunnel setup`` is checked for idempotent merges of the three
MCP config files, the skills copy, the AGENTS.md marker block, ``--dry-run``,
``--global`` and ``--agents`` — all against a tmp project with an overridden home,
never the real one. ``blt-tunnel doctor`` is checked to run, report, and never
crash when the tunnel is down.
"""

from __future__ import annotations

import json

import pytest

from blt_analytics import cli, digest as digest_mod, schema
from fakes import fixture_space

BUILT_AT = "2026-10-02T12:00:00Z"
FAKE_CMD = "/opt/venv/bin/blt-schema-mcp"


@pytest.fixture(autouse=True)
def _reset_schema():
    schema.reset()
    yield
    schema.reset()


@pytest.fixture
def injected():
    recs = fixture_space.to_records()
    schema.set_digest(
        digest_mod.build_digest(recs["devices"], recs["flows"], recs["runs"], built_at=BUILT_AT)
    )


# ---------------------------------------------------------------------------
# blt-schema
# ---------------------------------------------------------------------------


def test_schema_overview_text(injected, capsys):
    rc = cli.schema_main(["overview"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "devices: 10" in out
    assert "Chip" in out


def test_schema_overview_json(injected, capsys):
    cli.schema_main(["overview", "--json"])
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed["totals"]["devices"] == 10


def test_schema_find(injected, capsys):
    cli.schema_main(["find", "yield", "--json"])
    parsed = json.loads(capsys.readouterr().out)
    assert any(m.get("path") == "yield_pct" for m in parsed["matches"])


def test_schema_load_snippet_prints_code(injected, capsys):
    cli.schema_main(["load-snippet", "--device-type", "Chip", "--flow", "IV sweep"])
    out = capsys.readouterr().out
    assert "import blt_analytics as ba" in out
    assert "ba.explode_devices(runs)" in out


def test_schema_describe_param_positional(injected, capsys):
    cli.schema_main(["describe-param", "Wafer", "measurements", "--json"])
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["field"]["kind"] == "map"


def test_schema_unknown_prints_error(injected, capsys):
    cli.schema_main(["device-schema", "Chop"])
    out = capsys.readouterr().out
    assert "error:" in out and "did you mean:" in out


def test_schema_refresh_builds_from_runner(fake_blt, capsys):
    # No injected digest: --refresh forces a local build from the fake Runner.
    rc = cli.schema_main(["overview", "--refresh", "--json"])
    parsed = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert parsed["totals"]["devices"] == 10


# ---------------------------------------------------------------------------
# blt-tunnel setup
# ---------------------------------------------------------------------------


def _home(tmp_path):
    h = tmp_path / "home"
    h.mkdir(exist_ok=True)
    return str(h)


def test_setup_writes_three_configs(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    cli.run_setup(project=str(project), home=_home(tmp_path), mcp_command=FAKE_CMD)

    mcp = json.loads((project / ".mcp.json").read_text())
    assert mcp["mcpServers"]["balthazar-schema"] == {"command": FAKE_CMD, "args": []}

    cursor = json.loads((project / ".cursor" / "mcp.json").read_text())
    assert cursor["mcpServers"]["balthazar-schema"]["command"] == FAKE_CMD

    vscode = json.loads((project / ".vscode" / "mcp.json").read_text())
    assert vscode["servers"]["balthazar-schema"]["type"] == "stdio"
    assert vscode["servers"]["balthazar-schema"]["command"] == FAKE_CMD


def test_setup_copies_skills_and_agents_md(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    cli.run_setup(project=str(project), home=_home(tmp_path), mcp_command=FAKE_CMD)

    assert (project / ".claude" / "skills" / "balthazar-analytics" / "SKILL.md").exists()
    assert (project / ".agents" / "skills" / "balthazar-analytics" / "SKILL.md").exists()

    agents_md = (project / "AGENTS.md").read_text()
    assert cli._AGENTS_START in agents_md and cli._AGENTS_END in agents_md
    assert "balthazar-schema" in agents_md


def test_setup_is_idempotent(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    home = _home(tmp_path)
    cli.run_setup(project=str(project), home=home, mcp_command=FAKE_CMD)
    second = cli.run_setup(project=str(project), home=home, mcp_command=FAKE_CMD)
    assert all(changed is False for _target, changed in second)


def test_setup_preserves_existing_mcp_entries(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    (project / ".mcp.json").write_text(json.dumps({"mcpServers": {"other": {"command": "x"}}}))
    cli.run_setup(project=str(project), home=_home(tmp_path), mcp_command=FAKE_CMD)
    mcp = json.loads((project / ".mcp.json").read_text())
    assert "other" in mcp["mcpServers"]
    assert "balthazar-schema" in mcp["mcpServers"]


def test_setup_dry_run_writes_nothing(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    changes = cli.run_setup(
        project=str(project), home=_home(tmp_path), mcp_command=FAKE_CMD, dry_run=True
    )
    assert any(changed for _t, changed in changes)  # it reports what it would do
    assert not (project / ".mcp.json").exists()
    assert not (project / "AGENTS.md").exists()


def test_setup_agents_filter(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    cli.run_setup(
        project=str(project), agents=["claude"], home=_home(tmp_path), mcp_command=FAKE_CMD
    )
    assert (project / ".mcp.json").exists()
    assert not (project / ".cursor" / "mcp.json").exists()
    assert not (project / ".vscode" / "mcp.json").exists()


def test_setup_global_installs_home_skills(tmp_path):
    project = tmp_path / "proj"
    project.mkdir()
    home = _home(tmp_path)
    cli.run_setup(project=str(project), global_=True, home=home, mcp_command=FAKE_CMD)
    from pathlib import Path

    assert (Path(home) / ".claude" / "skills" / "balthazar-analytics" / "SKILL.md").exists()


def test_tunnel_main_setup_end_to_end(tmp_path, capsys):
    project = tmp_path / "proj"
    project.mkdir()
    rc = cli.tunnel_main(
        ["setup", "--project", str(project), "--home", _home(tmp_path), "--agents", "claude"]
    )
    out = capsys.readouterr().out
    assert rc == 0
    assert "updated" in out
    assert (project / ".mcp.json").exists()


# ---------------------------------------------------------------------------
# blt-tunnel doctor
# ---------------------------------------------------------------------------


def test_doctor_runs_and_reports_with_runner(fake_blt, tmp_path, capsys):
    project = tmp_path / "proj"
    project.mkdir()
    rc = cli.run_doctor(project=str(project), home=_home(tmp_path))
    out = capsys.readouterr().out
    assert "[PASS]" in out
    assert "space_schema" in out
    assert "pandas" in out and "mcp" in out
    # Not set up and no live tunnel -> non-zero, but it must not have crashed.
    assert rc == 1


def test_doctor_does_not_crash_when_tunnel_down(tmp_path, capsys):
    project = tmp_path / "proj"
    project.mkdir()
    # No fake_blt installed and no bridge profile: get_blt cannot locate a module, so
    # the module/connection/describe/space_schema checks fail (but doctor never crashes).
    rc = cli.run_doctor(project=str(project), home=_home(tmp_path))
    out = capsys.readouterr().out
    assert rc in (0, 1)
    assert "[FAIL]" in out  # at least the connection file is absent


def test_doctor_passes_registration_after_setup(fake_blt, tmp_path, capsys):
    project = tmp_path / "proj"
    project.mkdir()
    cli.run_setup(project=str(project), home=_home(tmp_path), mcp_command=FAKE_CMD)
    cli.run_doctor(project=str(project), home=_home(tmp_path))
    out = capsys.readouterr().out
    # The registration line reports a pass once configs + skills exist.
    reg_line = next(line for line in out.splitlines() if "registration" in line)
    assert reg_line.startswith("[PASS]")
