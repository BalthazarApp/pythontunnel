"""The ``balthazar-schema`` MCP server (stdio).

One tool per :mod:`blt_analytics.schema` function, with the same names and
signatures. The docstrings are the tool descriptions the model reads, so they say
what each tool returns, when to call it, that the output is **schema only**, and
the recommended order (``overview`` first, then narrow). Every tool returns a
JSON-serializable dict.

The spec names ``mcp.server.fastmcp.FastMCP``; that lives there in mcp v1 but was
renamed to ``mcp.server.mcpserver.MCPServer`` in mcp v2. Both expose the same
surface the server needs — construct with a name, a ``.tool()`` decorator, and a
``.run()`` that defaults to stdio — so :func:`build_server` picks whichever the
installed SDK provides and the rest of the module is version-agnostic.
"""

from __future__ import annotations

from typing import Any, List, Optional

from blt_analytics import schema


def _server_class():
    """The FastMCP/MCPServer class from whichever mcp SDK major is installed."""
    try:  # mcp v1
        from mcp.server.fastmcp import FastMCP

        return FastMCP
    except Exception:  # noqa: BLE001 - mcp v2 renamed it; try that next
        from mcp.server.mcpserver import MCPServer

        return MCPServer


def build_server():
    """Construct the ``balthazar-schema`` server with every schema tool registered.

    Returned rather than run so tests can drive it in-process (``list_tools`` /
    ``call_tool``). :func:`main` builds it and serves over stdio.
    """
    server = _server_class()("balthazar-schema")

    @server.tool()
    def overview() -> dict:
        """The lay of the land — CALL THIS FIRST. Returns totals, every device type
        with its device count and param-path count, and every flow with its run
        count and date range. Schema only (names, counts, dates — never a measured
        value). Take exact device-type and flow names from here, then narrow with
        the other tools before writing any blt_analytics code."""
        return schema.overview()

    @server.tool()
    def device_schema(device_type: str) -> dict:
        """Everything one device type declares: its count, fabrication-date and tag
        coverage, and its param paths (top-level first) with kinds and coverage.
        Pass an exact type name from overview/find. Schema only — the dotted param
        paths are what you project as columns in devices_df. For a nested or matrix
        param, follow up with describe_param."""
        return schema.device_schema(device_type)

    @server.tool()
    def describe_param(device_type: str, path: str = "") -> dict:
        """One device param path in full, plus its child paths. Call before indexing
        a nested param or plotting a matrix/list: it reports the field's kind
        (number, string, list, matrix, map, dict, mixed, …), coverage, and the
        dotted children beneath a dict. An empty path lists the type's top-level
        params. Schema only, never a value."""
        return schema.describe_param(device_type, path)

    @server.tool()
    def flow_schema(flow: str) -> dict:
        """Everything one flow declares: declared parameters, input and output
        paths, status breakdown, run count, date range, and how many runs have
        plots. Accepts a flow name OR id (from overview/find). Schema only — use the
        param.<path> / output.<path> names as runs_df columns; follow up on an
        output with describe_output."""
        return schema.flow_schema(flow)

    @server.tool()
    def describe_output(flow: str, path: str = "") -> dict:
        """One flow output path in full, plus its child paths. Like describe_param
        but for a flow's outputs; resolve flow by name or id. Call it to learn an
        output's kind and shape — e.g. a matrix to feed matrix_to_df or a list for
        series_to_df. Schema only, never a value."""
        return schema.describe_output(flow, path)

    @server.tool()
    def find(query: str, limit: int = 20) -> dict:
        """Fuzzy-match the user's words across every name in the space — your first
        move when a request is vague. Searches device-type names, device param
        paths, flow names and flow input/output paths; returns ranked matches, each
        with its kind (device_type, device_param, flow, flow_input, flow_output),
        owner and path. Use the exact names it returns in the other tools. Schema
        only."""
        return schema.find(query, limit)

    @server.tool()
    def load_snippet(
        device_type: Optional[str] = None,
        flow: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> dict:
        """Starter blt_analytics code for a slice: {"code": "..."}. Pass a device
        type and/or a flow (name or id), optionally the dotted columns to project.
        Returns runnable Python that pulls the data into pandas, referencing only
        names that exist in the space. A base to adapt; it fetches nothing itself."""
        return schema.load_snippet(device_type, flow, columns)

    @server.tool()
    def get_digest(refresh: bool = False) -> dict:
        """The whole space digest (schema §3) in one dict. Prefer overview and the
        narrower tools; reach for this only when you genuinely need the raw digest.
        Memoized — pass refresh=True to rebuild after the space changed. Schema
        only."""
        return schema.get_digest(refresh)

    # Keep references so linters don't flag the nested defs as unused; the decorator
    # has already registered them on the server.
    _ = (overview, device_schema, describe_param, flow_schema, describe_output, find, load_snippet, get_digest)
    return server


def main(argv: Any = None) -> None:
    """Console-script entry point (``blt-schema-mcp``): serve schema tools on stdio."""
    build_server().run()


if __name__ == "__main__":
    main()
