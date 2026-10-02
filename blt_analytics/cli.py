"""Command-line entry points: ``blt-schema`` and ``blt-tunnel``.

``blt-schema`` is a thin wrapper over :mod:`blt_analytics.schema`: one subcommand
per tool, pretty text by default and raw JSON with ``--json`` (the exact dict the
MCP tool returns). ``--refresh`` rebuilds the digest first.

``blt-tunnel setup`` wires a project for the analytics workflow — idempotently
merging the ``balthazar-schema`` MCP server into the per-agent config files,
copying the bundled skills, and maintaining a marked block in ``AGENTS.md`` — and
``blt-tunnel doctor`` prints a pass/fail diagnostic (transport, connection/profile,
ping, space_schema, pandas, mcp, and the project registrations) and exits non-zero
if anything is wrong, without ever crashing when the tunnel is down.

``blt-tunnel connect`` logs in to a **remote** app tunnel (SPEC §7), pings it, and
saves the connection profile; ``blt-tunnel disconnect`` removes it. Both drive the
shim's ``tunnel_connect`` / ``tunnel_disconnect`` / ``tunnel_transport`` functions.
A password is never taken as a command-line argument — only prompted (``getpass``)
or read with ``--password-stdin``.

The home directory is overridable (``--home`` / ``$BLT_ANALYTICS_HOME``) so tests
never touch the real one.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from typing import Any, Callable

from blt_analytics import _blt
from blt_analytics import schema

# ---------------------------------------------------------------------------
# blt-schema
# ---------------------------------------------------------------------------


def _scalar_str(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def _format(obj: Any, indent: int = 0) -> str:
    """A compact, readable YAML-ish rendering of a schema-tool result dict."""
    pad = "  " * indent
    lines: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, (dict, list)) and value:
                lines.append(f"{pad}{key}:")
                lines.append(_format(value, indent + 1))
            else:
                rendered = "" if isinstance(value, (dict, list)) else _scalar_str(value)
                lines.append(f"{pad}{key}: {rendered}".rstrip())
    elif isinstance(obj, list):
        for item in obj:
            if isinstance(item, (dict, list)):
                lines.append(f"{pad}-")
                lines.append(_format(item, indent + 1))
            else:
                lines.append(f"{pad}- {_scalar_str(item)}")
    else:
        lines.append(f"{pad}{_scalar_str(obj)}")
    return "\n".join(lines)


def _emit(result: Any, as_json: bool, out=None) -> None:
    out = out or sys.stdout
    if as_json:
        print(json.dumps(result, indent=2, default=str), file=out)
        return
    if isinstance(result, dict) and "error" in result:
        print(f"error: {result['error']}", file=out)
        suggestions = result.get("suggestions")
        if suggestions:
            print("did you mean: " + ", ".join(str(s) for s in suggestions), file=out)
        return
    if isinstance(result, dict) and set(result) == {"code"}:
        print(result["code"], file=out)
        return
    print(_format(result), file=out)


def _schema_parser() -> argparse.ArgumentParser:
    # --json/--refresh live on a shared parent so they are accepted both before and
    # after the subcommand (e.g. `blt-schema overview --json`, the documented form).
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", help="emit raw JSON instead of text")
    common.add_argument("--refresh", action="store_true", help="rebuild the digest first")

    parser = argparse.ArgumentParser(
        prog="blt-schema",
        parents=[common],
        description="Schema-only analytics tools over a Balthazar space.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("overview", parents=[common], help="totals, device types and flows (call first)")

    p = sub.add_parser("device-schema", parents=[common], help="one device type's params")
    p.add_argument("device_type")

    p = sub.add_parser("describe-param", parents=[common], help="one device param path in full")
    p.add_argument("device_type")
    p.add_argument("path", nargs="?", default="")

    p = sub.add_parser("flow-schema", parents=[common], help="one flow's params, inputs and outputs")
    p.add_argument("flow")

    p = sub.add_parser("describe-output", parents=[common], help="one flow output path in full")
    p.add_argument("flow")
    p.add_argument("path", nargs="?", default="")

    p = sub.add_parser("find", parents=[common], help="fuzzy-match names across the space")
    p.add_argument("query")
    p.add_argument("--limit", type=int, default=20)

    p = sub.add_parser("load-snippet", parents=[common], help="starter blt_analytics code")
    p.add_argument("--device-type", default=None)
    p.add_argument("--flow", default=None)
    p.add_argument("--columns", nargs="*", default=None)

    return parser


def schema_main(argv: list[str] | None = None) -> int:
    """Entry point for ``blt-schema``."""
    args = _schema_parser().parse_args(argv)

    if args.refresh:
        # Prime (and memoize) a fresh digest so the tool below reuses it.
        schema.get_digest(refresh=True)

    if args.command == "overview":
        result: Any = schema.overview()
    elif args.command == "device-schema":
        result = schema.device_schema(args.device_type)
    elif args.command == "describe-param":
        result = schema.describe_param(args.device_type, args.path)
    elif args.command == "flow-schema":
        result = schema.flow_schema(args.flow)
    elif args.command == "describe-output":
        result = schema.describe_output(args.flow, args.path)
    elif args.command == "find":
        result = schema.find(args.query, args.limit)
    elif args.command == "load-snippet":
        result = schema.load_snippet(
            device_type=args.device_type, flow=args.flow, columns=args.columns
        )
    else:  # pragma: no cover - argparse requires a subcommand
        _schema_parser().error("a subcommand is required")
        return 2

    _emit(result, args.json)
    return 0


# ---------------------------------------------------------------------------
# blt-tunnel — shared pieces
# ---------------------------------------------------------------------------

_SERVER_NAME = "balthazar-schema"
_AGENTS_START = "<!-- blt-analytics:start -->"
_AGENTS_END = "<!-- blt-analytics:end -->"
_ALL_AGENTS = ("claude", "cursor", "copilot")


def _package_root() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _repo_skills_dir() -> str:
    """The bundled skills directory, at the repo root next to the package."""
    return os.path.join(os.path.dirname(_package_root()), "skills")


def _mcp_command() -> str:
    """Absolute path to the venv's ``blt-schema-mcp`` console script."""
    found = shutil.which("blt-schema-mcp")
    if found:
        return os.path.abspath(found)
    candidate = os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "blt-schema-mcp")
    return os.path.abspath(candidate)


def _resolve_home(home: str | None) -> str:
    return home or os.environ.get("BLT_ANALYTICS_HOME") or os.path.expanduser("~")


# The loopback connection file and the remote app-tunnel profile share one path
# (``~/.balthazar_session_tunnel.json``); the ``transport`` key inside tells them
# apart (SPEC §7). The refresh-token cache lives separately, under ``~/.config``.
_CONNECTION_FILE = ".balthazar_session_tunnel.json"
_TOKEN_CACHE = (".config", "balthazar", "remote.json")


def _connection_profile_path(home: str) -> str:
    return os.path.join(home, _CONNECTION_FILE)


def _token_cache_path(home: str) -> str:
    return os.path.join(home, *_TOKEN_CACHE)


def _has_cached_token(home: str) -> bool:
    """Whether the refresh-token cache holds any entry — checked *without* logging in.

    Doctor uses this to decide whether pinging the app tunnel could prompt: a token
    in the cache means a refresh (not an interactive login) is the expected path, so
    pinging is safe; an empty/absent cache means ``blt-tunnel connect`` is needed.
    """
    try:
        with open(_token_cache_path(home), encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return False
    return isinstance(data, dict) and bool(data)


def _detect_transport() -> str:
    """Best-effort current tunnel transport, never raising.

    Returns the shim's ``tunnel_transport()`` (``"loopback"``/``"app"``/``"none"``);
    ``"runner"`` when the located module is the real Runner module (no tunnel), and
    ``"unknown"`` when a shim is present but too old to report it.
    """
    try:
        module = _blt.get_blt()
    except Exception:  # noqa: BLE001 - no module/shim reachable at all
        return "none"
    fn = getattr(module, "tunnel_transport", None)
    if fn is None:
        return "unknown" if getattr(module, "__balthazar_tunnel__", False) else "runner"
    try:
        return fn() or "none"
    except Exception:  # noqa: BLE001 - a diagnostic must never crash
        return "unknown"


def _agents_block() -> str:
    return (
        "## Balthazar analytics\n"
        "\n"
        "This project is wired for schema-only analytics over a Balthazar space.\n"
        "\n"
        f"- Schema tools (MCP server `{_SERVER_NAME}`, or the `blt-schema` CLI) report what\n"
        "  devices, params, flows, runs, inputs and outputs exist — names, kinds, shapes and\n"
        "  coverage. They never return measurement values. Call `overview` first, then narrow\n"
        "  with `find`, `device_schema`, `flow_schema`, `describe_param`, `describe_output`.\n"
        "- Then pull the real data with the `blt_analytics` Python package (`devices_df`,\n"
        "  `runs_df`, `explode_devices`, …) and plot with matplotlib.\n"
        "- The full workflow is in the `balthazar-analytics` skill. Run `blt-tunnel doctor`\n"
        "  if anything fails to connect."
    )


# ---------------------------------------------------------------------------
# blt-tunnel setup
# ---------------------------------------------------------------------------


def _read_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def _merge_json_config(
    path: str, top_key: str, entry: dict, *, dry_run: bool
) -> bool:
    """Idempotently ensure ``{top_key: {server_name: entry}}`` lives in ``path``.

    Returns True if the file needed a change. Existing unrelated keys are preserved.
    """
    data = _read_json(path)
    section = data.get(top_key)
    if not isinstance(section, dict):
        section = {}
    if section.get(_SERVER_NAME) == entry:
        return False
    section[_SERVER_NAME] = entry
    data[top_key] = section
    if not dry_run:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2)
            fh.write("\n")
    return True


def _needs_copy(src: str, dest: str) -> bool:
    if not os.path.exists(dest):
        return True
    try:
        with open(src, "rb") as a, open(dest, "rb") as b:
            return a.read() != b.read()
    except OSError:
        return True


def _copy_skills(src: str, dest_root: str, *, dry_run: bool) -> bool:
    """Copy ``src/*`` into ``dest_root`` (preserving structure). True if changed."""
    if not os.path.isdir(src):
        return False
    changed = False
    for root, _dirs, files in os.walk(src):
        rel = os.path.relpath(root, src)
        for fname in files:
            source = os.path.join(root, fname)
            dest = os.path.join(dest_root, os.path.normpath(os.path.join(rel, fname)))
            if _needs_copy(source, dest):
                changed = True
                if not dry_run:
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    shutil.copy2(source, dest)
    return changed


def _update_agents_md(path: str, block: str, *, dry_run: bool) -> bool:
    """Insert or refresh the marked analytics block in ``AGENTS.md``. True if changed."""
    marker_block = f"{_AGENTS_START}\n{block}\n{_AGENTS_END}"
    existing = ""
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as fh:
                existing = fh.read()
        except OSError:
            existing = ""

    if _AGENTS_START in existing and _AGENTS_END in existing:
        pre = existing[: existing.index(_AGENTS_START)]
        post = existing[existing.index(_AGENTS_END) + len(_AGENTS_END) :]
        new = pre + marker_block + post
    elif existing.strip() == "":
        new = marker_block + "\n"
    else:
        sep = "\n" if existing.endswith("\n") else "\n\n"
        new = existing + sep + marker_block + "\n"

    if new == existing:
        return False
    if not dry_run:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(new)
    return True


def run_setup(
    *,
    project: str,
    agents: list[str] | None = None,
    global_: bool = False,
    dry_run: bool = False,
    home: str | None = None,
    mcp_command: str | None = None,
) -> list[tuple[str, bool]]:
    """Wire ``project`` for the analytics workflow. Returns ``[(target, changed)]``.

    Idempotent: a second identical run reports ``changed=False`` for every target.
    """
    project = os.path.abspath(project)
    home = _resolve_home(home)
    command = mcp_command or _mcp_command()
    selected = [a for a in (agents or list(_ALL_AGENTS)) if a in _ALL_AGENTS]

    stdio = {"command": command, "args": []}
    config_targets = {
        "claude": (os.path.join(project, ".mcp.json"), "mcpServers", dict(stdio)),
        "cursor": (os.path.join(project, ".cursor", "mcp.json"), "mcpServers", dict(stdio)),
        "copilot": (
            os.path.join(project, ".vscode", "mcp.json"),
            "servers",
            {"type": "stdio", **stdio},
        ),
    }

    changes: list[tuple[str, bool]] = []

    # 1. per-agent MCP server registration
    for agent in selected:
        path, top_key, entry = config_targets[agent]
        changes.append((path, _merge_json_config(path, top_key, entry, dry_run=dry_run)))

    # 2. skills (project-local always; home dirs with --global)
    skills_src = _repo_skills_dir()
    skill_dests = [
        os.path.join(project, ".claude", "skills"),
        os.path.join(project, ".agents", "skills"),
    ]
    if global_:
        skill_dests += [
            os.path.join(home, ".claude", "skills"),
            os.path.join(home, ".agents", "skills"),
        ]
    for dest in skill_dests:
        changes.append((dest, _copy_skills(skills_src, dest, dry_run=dry_run)))

    # 3. AGENTS.md marker block
    agents_md = os.path.join(project, "AGENTS.md")
    changes.append((agents_md, _update_agents_md(agents_md, _agents_block(), dry_run=dry_run)))

    return changes


# ---------------------------------------------------------------------------
# blt-tunnel doctor
# ---------------------------------------------------------------------------


def _check(name: str, fn: Callable[[], tuple[bool, str]]) -> tuple[str, bool, str]:
    try:
        ok, detail = fn()
    except Exception as exc:  # noqa: BLE001 - a diagnostic must never crash
        return name, False, f"error: {exc!r}"
    return name, bool(ok), detail


def _check_module() -> tuple[bool, str]:
    from blt_analytics import _blt

    module = _blt.get_blt()
    kind = "tunnel shim" if _blt.is_tunnel() else "runner module"
    return True, f"located balthazar ({kind})"


_TRANSPORT_LABELS = {
    "loopback": "loopback (local — same host as the Runner)",
    "app": "app tunnel (remote — Balthazar login)",
    "none": "not connected",
    "runner": "not using the tunnel (real Runner module)",
    "unknown": "could not determine (shim too old to report it?)",
}


def _check_transport(transport: str) -> tuple[bool, str]:
    """Report the transport. Informational — the connection check judges 'none'."""
    return True, _TRANSPORT_LABELS.get(transport, transport)


def _check_connection(home: str, transport: str) -> tuple[bool, str]:
    """Connection/profile presence, by transport.

    The app transport additionally needs a cached login token to work without an
    interactive prompt; without one it points the user at ``blt-tunnel connect``.
    """
    try:
        module = _blt.get_blt()
    except Exception:  # noqa: BLE001
        module = None
    path = getattr(module, "CONNECTION_FILE", None) or _connection_profile_path(home)

    if transport == "runner":
        return True, "running on a Runner; no connection file needed"

    if transport == "app":
        have_profile = os.path.exists(path) or bool(
            os.environ.get("BALTHAZAR_SESSION_TUNNEL_APP_URL")
        )
        if not have_profile:
            return False, f"no app-tunnel profile at {path} (run: blt-tunnel connect)"
        if _has_cached_token(home):
            return True, f"app-tunnel profile present ({path}); cached login token found"
        return False, "app-tunnel profile present but no cached login token (run: blt-tunnel connect)"

    if os.path.exists(path):
        return True, f"connection file present ({path})"
    return False, f"no connection file at {path}"


def _check_ping(transport: str, home: str) -> tuple[bool, str]:
    # Never trigger an interactive login from a diagnostic: on the app transport with
    # no cached token, pinging would prompt (device code / browser), so skip it.
    if transport == "app" and not _has_cached_token(home):
        return False, "skipped: no cached login token — run blt-tunnel connect in a terminal"

    module = _blt.get_blt()
    ping = getattr(module, "ping", None)
    if ping is None:
        return False, "module has no ping()"
    info = ping() or {}
    who = info.get("user") or info.get("user_id") if isinstance(info, dict) else None
    detail = "ping ok"
    if who:
        detail += f" (user {who})"
    return True, detail


def _check_space_schema() -> tuple[bool, str]:
    d = schema.get_digest()
    if isinstance(d, dict) and d.get("version"):
        totals = d.get("totals", {})
        return True, f"digest built (devices={totals.get('devices')}, flows={totals.get('flows')})"
    return False, "space_schema returned no digest"


def _check_import(module_name: str) -> tuple[bool, str]:
    __import__(module_name)
    return True, f"{module_name} importable"


def _check_mcp() -> tuple[bool, str]:
    from blt_analytics.mcp_server import _server_class

    _server_class()
    return True, "mcp server SDK importable"


def _check_registration(project: str) -> tuple[bool, str]:
    present = []
    for rel, top_key in (
        (".mcp.json", "mcpServers"),
        (os.path.join(".cursor", "mcp.json"), "mcpServers"),
        (os.path.join(".vscode", "mcp.json"), "servers"),
    ):
        data = _read_json(os.path.join(project, rel))
        if _SERVER_NAME in (data.get(top_key) or {}):
            present.append(rel)
    skills = os.path.join(project, ".claude", "skills")
    has_skills = os.path.isdir(skills) and bool(os.listdir(skills))
    if present and has_skills:
        return True, f"registered in {', '.join(present)}; skills installed"
    missing = []
    if not present:
        missing.append("no MCP config lists the server")
    if not has_skills:
        missing.append("skills not installed")
    return False, "; ".join(missing) + " (run: blt-tunnel setup)"


def run_doctor(*, project: str | None = None, home: str | None = None, out=None) -> int:
    """Run the diagnostics, print pass/fail, return 0 if all pass else 1."""
    out = out or sys.stdout
    project = os.path.abspath(project or os.getcwd())
    home = _resolve_home(home)
    transport = _detect_transport()

    checks = [
        _check("balthazar module", _check_module),
        _check("transport", lambda: _check_transport(transport)),
        _check("connection", lambda: _check_connection(home, transport)),
        _check("ping", lambda: _check_ping(transport, home)),
        _check("space_schema", _check_space_schema),
        _check("pandas", lambda: _check_import("pandas")),
        _check("mcp", _check_mcp),
        _check("registration", lambda: _check_registration(project)),
    ]

    for name, ok, detail in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}", file=out)

    all_ok = all(ok for _name, ok, _detail in checks)
    print(("all checks passed" if all_ok else "some checks failed"), file=out)
    return 0 if all_ok else 1


# ---------------------------------------------------------------------------
# blt-tunnel connect / disconnect (remote app tunnel, SPEC §7)
# ---------------------------------------------------------------------------


def _read_password(login: str, *, password_stdin: bool, out=None) -> str | None:
    """Obtain the password for ``password`` login — never from the command line.

    Read from stdin with ``--password-stdin`` (one line, trailing newline stripped),
    otherwise prompt with :func:`getpass.getpass`. ``None`` for the other logins.
    """
    if login != "password":
        return None
    if password_stdin:
        return sys.stdin.readline().rstrip("\n")
    import getpass

    return getpass.getpass("Balthazar password: ")


def run_connect(
    app_url: str,
    *,
    login: str = "device",
    username: str | None = None,
    password_stdin: bool = False,
    site: str | None = None,
    ca_file: str | None = None,
    out=None,
) -> int:
    """Log in to a remote app tunnel and save the profile. Returns an exit code.

    Drives the shim's ``tunnel_connect`` (login + ping + profile write) and prints
    who connected and which flow run. 0 on success, 1 on any failure.
    """
    out = out or sys.stdout

    if login == "password" and not username:
        username = input("Balthazar username: ").strip()
    try:
        password = _read_password(login, password_stdin=password_stdin)
    except (EOFError, KeyboardInterrupt):
        print("error: no password supplied", file=sys.stderr)
        return 1

    try:
        module = _blt.get_blt()
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not load the balthazar shim: {exc}", file=sys.stderr)
        return 1

    connect = getattr(module, "tunnel_connect", None)
    if connect is None:
        print(
            "error: this balthazar shim has no tunnel_connect(...) — the app tunnel "
            "needs the v2 session shim (session_tunnel/balthazar.py).",
            file=sys.stderr,
        )
        return 1

    try:
        info = connect(
            app_url,
            login=login,
            username=username,
            password=password,
            site=site,
            ca_file=ca_file,
        )
    except Exception as exc:  # noqa: BLE001 - surface any login/connection failure cleanly
        print(f"error: could not connect: {exc}", file=sys.stderr)
        return 1

    info = info if isinstance(info, dict) else {}
    who = info.get("user") or info.get("user_id") or "?"
    transport = info.get("transport") or "app"
    run_id = info.get("flow_run_id") or "?"
    flow_name = info.get("flow_name")
    print(f"connected over the {transport} tunnel as user {who}", file=out)
    tail = f" (flow {flow_name!r})" if flow_name else ""
    print(f"flow run: {run_id}{tail}", file=out)
    return 0


def run_disconnect(*, forget: bool = False, out=None) -> int:
    """Remove the saved tunnel profile (and, with ``forget``, the cached token)."""
    out = out or sys.stdout
    try:
        module = _blt.get_blt()
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not load the balthazar shim: {exc}", file=sys.stderr)
        return 1

    disconnect = getattr(module, "tunnel_disconnect", None)
    if disconnect is None:
        print("error: this balthazar shim has no tunnel_disconnect(...)", file=sys.stderr)
        return 1

    try:
        disconnect(forget=forget)
    except Exception as exc:  # noqa: BLE001
        print(f"error: could not disconnect: {exc}", file=sys.stderr)
        return 1

    extra = " and forgot the cached login token" if forget else ""
    print(f"disconnected{extra}", file=out)
    return 0


# ---------------------------------------------------------------------------
# blt-tunnel entry point
# ---------------------------------------------------------------------------


def _tunnel_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="blt-tunnel", description="Set up and diagnose the Balthazar analytics tunnel."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    s = sub.add_parser("setup", help="wire a project for the analytics workflow")
    s.add_argument(
        "--agents",
        default=",".join(_ALL_AGENTS),
        help="comma-separated: claude,cursor,copilot (default: all)",
    )
    s.add_argument("--project", default=".", help="project directory (default: cwd)")
    s.add_argument("--global", dest="global_", action="store_true", help="also install skills to home")
    s.add_argument("--dry-run", action="store_true", help="report changes without writing")
    s.add_argument("--home", default=None, help=argparse.SUPPRESS)

    d = sub.add_parser("doctor", help="print a pass/fail connection diagnostic")
    d.add_argument("--project", default=".", help="project directory (default: cwd)")
    d.add_argument("--home", default=None, help=argparse.SUPPRESS)

    c = sub.add_parser(
        "connect", help="connect to a remote tunnel through the Balthazar app tunnel"
    )
    c.add_argument("app_url", help="the app-tunnel URL (copy it from the opened app in Balthazar)")
    c.add_argument(
        "--login",
        choices=("device", "browser", "password"),
        default="device",
        help="login method (default: device — a device code you confirm in a browser)",
    )
    c.add_argument("--username", default=None, help="username (password login; prompted if omitted)")
    c.add_argument(
        "--password-stdin",
        dest="password_stdin",
        action="store_true",
        help="read the password from stdin (password login). A password is never taken "
        "as a command-line argument; without this flag it is prompted via getpass.",
    )
    c.add_argument("--site", default=None, help="Balthazar site URL, if it can't be auto-discovered")
    c.add_argument("--ca-file", dest="ca_file", default=None, help="custom CA bundle (PEM file)")

    x = sub.add_parser("disconnect", help="remove the saved app-tunnel profile")
    x.add_argument(
        "--forget", action="store_true", help="also delete the cached login/refresh token"
    )

    return parser


def tunnel_main(argv: list[str] | None = None) -> int:
    """Entry point for ``blt-tunnel``."""
    args = _tunnel_parser().parse_args(argv)

    if args.command == "setup":
        agents = [a.strip() for a in args.agents.split(",") if a.strip()]
        changes = run_setup(
            project=args.project,
            agents=agents,
            global_=args.global_,
            dry_run=args.dry_run,
            home=args.home,
        )
        prefix = "would update" if args.dry_run else "updated"
        any_change = False
        for target, changed in changes:
            if changed:
                any_change = True
                print(f"{prefix}: {target}")
            else:
                print(f"unchanged: {target}")
        if not any_change:
            print("everything already up to date")
        return 0

    if args.command == "doctor":
        return run_doctor(project=args.project, home=args.home)

    if args.command == "connect":
        return run_connect(
            args.app_url,
            login=args.login,
            username=args.username,
            password_stdin=args.password_stdin,
            site=args.site,
            ca_file=args.ca_file,
        )

    if args.command == "disconnect":
        return run_disconnect(forget=args.forget)

    _tunnel_parser().error("a subcommand is required")  # pragma: no cover
    return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(tunnel_main())
