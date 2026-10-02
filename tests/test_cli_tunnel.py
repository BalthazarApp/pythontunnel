"""Transport-agnostic ``blt-tunnel`` / ``mcp_server`` guard tests.

The v3 connect / disconnect / doctor and the v3-specific MCP guard behaviour live in
``tests/test_analytics_v3.py``. What remains here is independent of the transport: the
parser never accepts a password on the command line, and ``_guard`` converts a raised
error into a JSON error dict.
"""

from __future__ import annotations

import pytest

from blt_analytics import cli, mcp_server, schema


@pytest.fixture(autouse=True)
def _reset_schema():
    schema.reset()
    yield
    schema.reset()


def test_connect_never_accepts_password_as_argument():
    parser = cli._tunnel_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["connect", "https://x/", "--password", "secret"])
    # but --password-stdin is accepted
    args = parser.parse_args(["connect", "https://x/", "--password-stdin"])
    assert args.password_stdin is True


def test_mcp_guard_converts_exception_to_error():
    # With a digest injected there is no tunnel acquisition to guard, so _guard runs
    # the call and turns any raised error into a JSON error dict (never a crash).
    schema.set_digest({"version": 1, "totals": {}, "device_types": {}, "flows": {}})

    def boom():
        raise RuntimeError("tunnel is down")

    result = mcp_server._guard(boom)
    assert "error" in result and "tunnel is down" in result["error"]
    assert result.get("hint") == "run blt-tunnel doctor"
