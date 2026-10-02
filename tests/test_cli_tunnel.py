"""Tests for the remote app-tunnel CLI (SPEC §7): ``blt-tunnel connect`` /
``disconnect``, the transport-aware ``blt-tunnel doctor``, and the ``mcp_server``
guard that refuses to prompt for a login inside a stdio server.

Everything is driven against a monkeypatched shim (``_blt.get_blt`` returns a fake
module that records its calls); nothing touches the network, the real home, or a
real login. A password is only ever obtained from stdin or getpass — never from a
command-line argument — and these tests prove the parser rejects a ``--password``
flag outright.
"""

from __future__ import annotations

import io
import json

import pytest

import blt_analytics._blt as blt_locator
from blt_analytics import cli, mcp_server, schema


# ---------------------------------------------------------------------------
# A fake v2 shim that records how the CLI drives it
# ---------------------------------------------------------------------------


class FakeShim:
    __balthazar_tunnel__ = True
    tunnel_state = {}  # marks it as the v2 shim (not the v1 one-shot)

    def __init__(self, *, transport="app", ping_info=None, has_connect=True, has_disconnect=True):
        self._transport = transport
        self._ping_info = ping_info or {}
        self.connect_calls: list[dict] = []
        self.disconnect_calls: list[dict] = []
        self.ping_calls = 0
        self.connect_error: Exception | None = None
        self._has_connect = has_connect
        self._has_disconnect = has_disconnect

    # tunnel_connect / tunnel_disconnect are looked up with getattr(module, name, None),
    # so expose them conditionally through __getattr__ rather than as hard attributes.
    def __getattr__(self, name):  # only called for missing attributes
        if name == "tunnel_connect" and self.__dict__.get("_has_connect", True):
            return self._tunnel_connect
        if name == "tunnel_disconnect" and self.__dict__.get("_has_disconnect", True):
            return self._tunnel_disconnect
        raise AttributeError(name)

    def tunnel_transport(self):
        return self._transport

    def ping(self):
        self.ping_calls += 1
        return dict(self._ping_info)

    def tunnel_space_schema(self, refresh=False):
        return {"version": 1, "built_at": "x", "totals": {"devices": 1, "flows": 1}}

    def _tunnel_connect(self, app_url, *, login="device", username=None,
                        password=None, site=None, ca_file=None):
        self.connect_calls.append(
            {"app_url": app_url, "login": login, "username": username,
             "password": password, "site": site, "ca_file": ca_file}
        )
        if self.connect_error is not None:
            raise self.connect_error
        return dict(self._ping_info)

    def _tunnel_disconnect(self, *, forget=False):
        self.disconnect_calls.append({"forget": forget})


@pytest.fixture(autouse=True)
def _reset_schema():
    schema.reset()
    yield
    schema.reset()


@pytest.fixture
def use_shim(monkeypatch):
    """Install a FakeShim as the module ``get_blt()`` returns; return a setter."""
    def install(shim):
        monkeypatch.setattr(blt_locator, "get_blt", lambda: shim)
        return shim
    return install


def _home(tmp_path):
    h = tmp_path / "home"
    h.mkdir(exist_ok=True)
    return str(h)


def _write_app_profile(home):
    import os
    path = cli._connection_profile_path(home)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"transport": "app", "app_url": "https://x/app-tunnel/a/b/", "login": "device"}, fh)
    return path


def _write_token_cache(home):
    import os
    path = cli._token_cache_path(home)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"authority|blt-frontend2": "refresh-token"}, fh)
    return path


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------


def test_connect_device_login_prints_who_and_run(use_shim, capsys):
    shim = use_shim(FakeShim(ping_info={
        "transport": "app", "user": "u-123", "flow_run_id": "run-9", "flow_name": "Session tunnel",
    }))
    rc = cli.run_connect("https://host/app-tunnel/a/b/?space_id=s", login="device")
    out = capsys.readouterr().out
    assert rc == 0
    assert "connected over the app tunnel as user u-123" in out
    assert "run-9" in out and "Session tunnel" in out
    # device login carries no password/username
    call = shim.connect_calls[0]
    assert call["login"] == "device" and call["password"] is None and call["username"] is None


def test_connect_never_accepts_password_as_argument():
    parser = cli._tunnel_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["connect", "https://x/", "--password", "secret"])
    # but --password-stdin is accepted
    args = parser.parse_args(["connect", "https://x/", "--password-stdin"])
    assert args.password_stdin is True


def test_connect_password_from_stdin(use_shim, monkeypatch):
    shim = use_shim(FakeShim(ping_info={"user": "alice", "flow_run_id": "r1"}))
    monkeypatch.setattr("sys.stdin", io.StringIO("s3cr3t\n"))
    rc = cli.run_connect("https://x/app-tunnel/a/b/", login="password",
                         username="alice", password_stdin=True)
    assert rc == 0
    call = shim.connect_calls[0]
    assert call["login"] == "password" and call["username"] == "alice"
    assert call["password"] == "s3cr3t"  # newline stripped


def test_connect_password_from_getpass(use_shim, monkeypatch):
    shim = use_shim(FakeShim(ping_info={"user": "alice", "flow_run_id": "r1"}))
    monkeypatch.setattr("getpass.getpass", lambda *a, **k: "pw-from-getpass")
    rc = cli.run_connect("https://x/app-tunnel/a/b/", login="password", username="alice")
    assert rc == 0
    assert shim.connect_calls[0]["password"] == "pw-from-getpass"


def test_connect_passes_site_and_ca_file(use_shim):
    shim = use_shim(FakeShim(ping_info={"user": "u", "flow_run_id": "r"}))
    cli.run_connect("https://x/app-tunnel/a/b/", login="device",
                    site="https://core.example", ca_file="/tmp/ca.pem")
    call = shim.connect_calls[0]
    assert call["site"] == "https://core.example" and call["ca_file"] == "/tmp/ca.pem"


def test_connect_missing_tunnel_connect_errors(use_shim, capsys):
    use_shim(FakeShim(has_connect=False))
    rc = cli.run_connect("https://x/app-tunnel/a/b/", login="device")
    err = capsys.readouterr().err
    assert rc == 1
    assert "tunnel_connect" in err


def test_connect_failure_returns_nonzero(use_shim, capsys):
    shim = use_shim(FakeShim())
    shim.connect_error = RuntimeError("login failed: bad code")
    rc = cli.run_connect("https://x/app-tunnel/a/b/", login="device")
    err = capsys.readouterr().err
    assert rc == 1
    assert "could not connect" in err and "login failed" in err


def test_tunnel_main_connect_dispatch(use_shim, capsys):
    shim = use_shim(FakeShim(ping_info={"user": "u", "flow_run_id": "r", "transport": "app"}))
    rc = cli.tunnel_main(["connect", "https://x/app-tunnel/a/b/?space_id=s"])
    assert rc == 0
    assert shim.connect_calls and shim.connect_calls[0]["login"] == "device"


# ---------------------------------------------------------------------------
# disconnect
# ---------------------------------------------------------------------------


def test_disconnect_calls_shim(use_shim, capsys):
    shim = use_shim(FakeShim())
    rc = cli.run_disconnect(forget=False)
    out = capsys.readouterr().out
    assert rc == 0 and "disconnected" in out
    assert shim.disconnect_calls == [{"forget": False}]
    assert "forgot" not in out


def test_disconnect_forget(use_shim, capsys):
    shim = use_shim(FakeShim())
    rc = cli.run_disconnect(forget=True)
    out = capsys.readouterr().out
    assert rc == 0 and "forgot the cached login token" in out
    assert shim.disconnect_calls == [{"forget": True}]


def test_disconnect_missing_shim_errors(use_shim, capsys):
    use_shim(FakeShim(has_disconnect=False))
    rc = cli.run_disconnect()
    assert rc == 1
    assert "tunnel_disconnect" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# doctor — transport aware (SPEC §7)
# ---------------------------------------------------------------------------


def _doctor_lines(out):
    return {line.split(":", 1)[0]: line for line in out.splitlines() if line.startswith("[")}


def test_doctor_app_transport_with_token(use_shim, tmp_path, capsys):
    home = _home(tmp_path)
    _write_app_profile(home)
    _write_token_cache(home)
    shim = use_shim(FakeShim(transport="app", ping_info={"user": "u-1", "flow_run_id": "r"}))
    project = tmp_path / "proj"
    project.mkdir()

    cli.run_doctor(project=str(project), home=home)
    out = capsys.readouterr().out
    assert "app tunnel" in out  # transport line reports it
    assert "[PASS] connection:" in out and "cached login token found" in out
    assert "[PASS] ping:" in out and "user u-1" in out
    assert shim.ping_calls == 1


def test_doctor_app_transport_without_token_does_not_ping(use_shim, tmp_path, capsys):
    home = _home(tmp_path)
    _write_app_profile(home)  # profile present, but NO token cache
    shim = use_shim(FakeShim(transport="app"))
    project = tmp_path / "proj"
    project.mkdir()

    cli.run_doctor(project=str(project), home=home)
    out = capsys.readouterr().out
    assert "[FAIL] connection:" in out and "no cached login token" in out
    assert "[FAIL] ping:" in out and "blt-tunnel connect" in out
    # The crucial invariant: doctor never triggered an (interactive) login.
    assert shim.ping_calls == 0


def test_doctor_loopback_transport(use_shim, tmp_path, capsys):
    home = _home(tmp_path)
    # a loopback connection file at the default path
    with open(cli._connection_profile_path(home), "w", encoding="utf-8") as fh:
        json.dump({"url": "http://127.0.0.1:8766", "token": "t", "flow_run_id": "r"}, fh)
    shim = use_shim(FakeShim(transport="loopback", ping_info={"flow_run_id": "r"}))
    project = tmp_path / "proj"
    project.mkdir()

    cli.run_doctor(project=str(project), home=home)
    out = capsys.readouterr().out
    assert "loopback" in out
    assert "[PASS] connection:" in out and "connection file present" in out
    assert shim.ping_calls == 1


# ---------------------------------------------------------------------------
# mcp_server guard — no interactive login over stdio
# ---------------------------------------------------------------------------


def test_mcp_guard_app_without_token_returns_error(use_shim, monkeypatch):
    use_shim(FakeShim(transport="app"))
    monkeypatch.setattr(mcp_server, "_has_cached_token", lambda: False)
    err = mcp_server._tunnel_unavailable()
    assert err is not None and "blt-tunnel connect" in err["error"]
    # _guard returns it without ever running the call
    sentinel = {"ran": True}
    ran = []
    result = mcp_server._guard(lambda: ran.append(1) or sentinel)
    assert result == err and ran == []


def test_mcp_guard_none_transport_returns_error(use_shim):
    use_shim(FakeShim(transport="none"))
    err = mcp_server._tunnel_unavailable()
    assert err is not None and "not connected" in err["error"]


def test_mcp_guard_loopback_runs_call(use_shim):
    use_shim(FakeShim(transport="loopback"))
    assert mcp_server._tunnel_unavailable() is None
    assert mcp_server._guard(lambda: {"ok": 1}) == {"ok": 1}


def test_mcp_guard_app_with_token_runs_call(use_shim, monkeypatch):
    use_shim(FakeShim(transport="app"))
    monkeypatch.setattr(mcp_server, "_has_cached_token", lambda: True)
    assert mcp_server._tunnel_unavailable() is None


def test_mcp_guard_converts_exception_to_error(use_shim):
    use_shim(FakeShim(transport="loopback"))

    def boom():
        raise RuntimeError("tunnel is down")

    result = mcp_server._guard(boom)
    assert "error" in result and "tunnel is down" in result["error"]
    assert result.get("hint") == "run blt-tunnel doctor"


def test_mcp_guard_injected_digest_short_circuits(use_shim):
    # With a digest injected (tests / already-built), there is no tunnel acquisition to
    # guard, so even an 'app' transport must not block the pure-digest tools.
    use_shim(FakeShim(transport="app"))
    schema.set_digest({"version": 1, "totals": {}, "device_types": {}, "flows": {}})
    assert mcp_server._tunnel_unavailable() is None
