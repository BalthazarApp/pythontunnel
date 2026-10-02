"""Tests for ``blt_analytics.mcp_server`` — the ``balthazar-schema`` MCP server.

Driven in-process against the installed mcp SDK: build the server, list its tools,
and call them, asserting each is registered with a model-facing description and
returns the same JSON its schema function does. No stdio, no subprocess.
"""

from __future__ import annotations

import asyncio
import inspect
import json

import pytest

from blt_analytics import digest as digest_mod
from blt_analytics import mcp_server, schema
from fakes import fixture_space

BUILT_AT = "2026-10-02T12:00:00Z"

EXPECTED_TOOLS = {
    "overview",
    "device_schema",
    "describe_param",
    "flow_schema",
    "describe_output",
    "find",
    "load_snippet",
    "get_digest",
}


@pytest.fixture(autouse=True)
def _inject_digest():
    recs = fixture_space.to_records()
    schema.set_digest(
        digest_mod.build_digest(recs["devices"], recs["flows"], recs["runs"], built_at=BUILT_AT)
    )
    yield
    schema.reset()


@pytest.fixture
def server():
    return mcp_server.build_server()


def _await(value):
    """Resolve a value that may be a coroutine (mcp 2.x methods are async)."""
    if inspect.isawaitable(value):
        return asyncio.run(value)
    return value


def _list_tools(server):
    return _await(server.list_tools())


def _call(server, name, arguments):
    result = _await(server.call_tool(name, arguments))
    # Both SDK majors return a result whose .content is a list of text blocks.
    text = result.content[0].text
    return json.loads(text)


def test_all_tools_registered(server):
    names = {t.name for t in _list_tools(server)}
    assert names == EXPECTED_TOOLS


def test_tools_have_model_facing_descriptions(server):
    for tool in _list_tools(server):
        assert tool.description and tool.description.strip()
        # input_schema is a JSON-schema object (snake_case in mcp 2.x, camelCase in mcp 1.x).
        schema_obj = getattr(tool, "input_schema", None) or getattr(tool, "inputSchema", None)
        assert isinstance(schema_obj, dict)
    overview = next(t for t in _list_tools(server) if t.name == "overview")
    assert "first" in overview.description.lower()


def test_call_overview(server):
    out = _call(server, "overview", {})
    assert out["totals"]["devices"] == 10
    assert "Chip" in out["device_types"]


def test_call_device_schema(server):
    out = _call(server, "device_schema", {"device_type": "Chip"})
    assert out["count"] == 3


def test_call_find(server):
    out = _call(server, "find", {"query": "yield"})
    assert any(m["kind"] == "device_param" and m.get("path") == "yield_pct" for m in out["matches"])


def test_call_load_snippet(server):
    out = _call(server, "load_snippet", {"device_type": "Chip", "flow": "IV sweep"})
    assert "ba.explode_devices(runs)" in out["code"]


def test_unknown_name_returns_error_not_raises(server):
    out = _call(server, "device_schema", {"device_type": "Nope"})
    assert "error" in out and "suggestions" in out


def test_mcp_output_does_not_leak(server):
    """The MCP surface is subject to the same leak guarantee as schema.*."""
    numbers = fixture_space.fixture_numeric_values()
    secrets = fixture_space.fixture_secret_strings()
    for name, args in [
        ("overview", {}),
        ("device_schema", {"device_type": "Wafer"}),
        ("describe_param", {"device_type": "Wafer", "path": "measurements"}),
        ("flow_schema", {"flow": "IV sweep"}),
        ("describe_output", {"flow": "Transport map", "path": "cap_matrix"}),
        ("find", {"query": "SECRET"}),
        ("get_digest", {}),
    ]:
        blob = json.dumps(_call(server, name, args), default=str)
        assert all(s not in blob for s in secrets)
        assert all(repr(n) not in blob and str(n) not in blob for n in numbers)
