"""Bridge server tests: auth, protocol, ops, hardening, refs, jobs, watchdog,
audit, parts, and wire compatibility with the client.

The handler runs for real on a loopback socket; the fake *real* ``balthazar`` is
injected, and a :class:`Client` drives the wire protocol directly.
"""

from __future__ import annotations

import base64
import json
import os
import secrets
import threading
import time
import urllib.error
import urllib.request
import zlib

from bridge._helpers import OWNER, load_remote_client


def _incompressible(nbytes: int) -> str:
    """A base64 string of random bytes, so zlib cannot shrink it below the part cap."""
    return base64.b64encode(os.urandom(nbytes)).decode("ascii")

CHIP_CALL = {"op": "call", "path": ["search_devices"], "args": [], "kwargs": {"type": "Chip"}}
ALL_CALL = {"op": "call", "path": ["search_devices"], "args": [], "kwargs": {}}


# ---------------------------------------------------------------------------
# Auth matrix
# ---------------------------------------------------------------------------


def test_owner_is_allowed(bridge_server, client_factory):
    url, _ = bridge_server()
    assert client_factory(url, OWNER).op({"op": "describe"})["ok"]


def test_other_user_denied_when_not_shared(bridge_server, client_factory):
    url, _ = bridge_server()
    status, _, _ = client_factory(url, "stranger").post({"op": "describe"})
    assert status == 403


def test_allowed_users_can_call(bridge_server, client_factory):
    url, _ = bridge_server(allowed_users="Friend-1, friend-2")
    assert client_factory(url, "friend-1").op({"op": "describe"})["ok"]
    status, _, _ = client_factory(url, "nobody").post({"op": "describe"})
    assert status == 403


def test_shared_allows_any_authenticated_caller(bridge_server, client_factory):
    url, _ = bridge_server(shared=True)
    assert client_factory(url, "anyone-at-all").op({"op": "describe"})["ok"]


def test_missing_user_header_denied(bridge_server, client_factory):
    url, _ = bridge_server(shared=True)
    status, _, _ = client_factory(url, None).post({"op": "describe"}, user=None)
    assert status == 403


def test_browser_origin_rejected(bridge_server, client_factory):
    url, _ = bridge_server()
    status, _, _ = client_factory(url, OWNER).post({"op": "describe"}, headers={"Origin": "https://x"})
    assert status == 403


def test_browser_sec_fetch_rejected(bridge_server, client_factory):
    url, _ = bridge_server()
    status, _, _ = client_factory(url, OWNER).post(
        {"op": "describe"}, headers={"Sec-Fetch-Site": "cross-site"}
    )
    assert status == 403


def test_get_index_page_visible_to_allowed_denied_to_others(bridge_server, client_factory):
    url, _ = bridge_server()
    status, body = client_factory(url, OWNER).get("/")
    assert status == 200 and b"Remote bridge is running" in body
    status, _ = client_factory(url, "stranger").get("/")
    assert status == 403


# ---------------------------------------------------------------------------
# describe
# ---------------------------------------------------------------------------


def test_describe_reports_all_fields(bridge_server, client_factory):
    indexes = json.dumps({"by_wafer": {"path": "hierarchy.wafer", "device_type": "Chip"}})
    url, mod = bridge_server(
        shared=True, device_indexes=indexes, idle_timeout=123, call_timeout=7, part_bytes=4096
    )
    d = client_factory(url, "caller-7").op({"op": "describe"})["result"]
    assert d["protocol"] == 3
    assert d["bridge_version"] == "3.0.0"
    assert d["user"] == "caller-7"
    assert d["owner"] == OWNER
    assert d["shared"] is True
    assert d["parts"] == 4096
    assert d["idle_timeout_s"] == 123 and d["call_timeout_s"] == 7
    assert sorted(d["tunnel"]) == [
        "cached_devices", "cached_devices_query", "device_cache_status",
        "refresh_device_cache", "space_schema",
    ]
    assert d["device_indexes"] == {"by_wafer": "hierarchy.wafer"}
    assert "search_devices" in d["functions"] and "DeviceBuilder" in d["classes"]


# ---------------------------------------------------------------------------
# Basic ops: call / get / set / getitem / setitem / delitem / contains / enter / exit
# ---------------------------------------------------------------------------


def test_call_returns_encoded_devices(bridge_server, client_factory):
    url, _ = bridge_server()
    reply = client_factory(url, OWNER).op(CHIP_CALL)
    assert reply["ok"]
    assert {d["cls"] for d in reply["result"]} == {"Device"}
    assert len(reply["result"]) == 3


def test_get_module_attribute(bridge_server, client_factory):
    url, _ = bridge_server()
    reply = client_factory(url, OWNER).op({"op": "get", "path": ["user"]})
    assert reply["ok"] and reply["result"] == OWNER


def test_set_on_ref_reencodes_self(bridge_server, client_factory):
    url, _ = bridge_server()
    c = client_factory(url, OWNER)
    ref = c.op(CHIP_CALL)["result"][0]["ref"]
    reply = c.op({"op": "set", "ref": ref, "path": ["name"], "value": "renamed"})
    assert reply["ok"]
    assert reply["self"]["fields"]["name"] == "renamed"


def test_getitem_setitem_delitem_contains(bridge_server, client_factory, fake_blt, monkeypatch):
    url, _ = bridge_server(expose_secrets=True)
    monkeypatch.setattr(fake_blt, "secrets", {"API_KEY": "shh"}, raising=False)
    c = client_factory(url, OWNER)
    assert c.op({"op": "getitem", "path": ["secrets"], "key": "API_KEY"})["result"] == "shh"
    assert c.op({"op": "contains", "path": ["secrets"], "key": "API_KEY"})["result"] is True
    assert c.op({"op": "setitem", "path": ["secrets"], "key": "NEW", "value": 7})["ok"]
    assert fake_blt.secrets["NEW"] == 7
    assert c.op({"op": "delitem", "path": ["secrets"], "key": "NEW"})["ok"]
    assert "NEW" not in fake_blt.secrets


def test_enter_exit_records_how_closed(bridge_server, client_factory, fake_blt):
    url, mod = bridge_server()
    c = client_factory(url, OWNER)
    ref = c.op({"op": "call", "path": ["new_flow_run_context"], "args": [], "kwargs": {"name": "ok"}})["result"]["ref"]
    assert c.op({"op": "enter", "ref": ref})["ok"]
    ctx = fake_blt.flow_run_contexts()[-1]
    assert ctx.entered is True
    assert ref in mod._callers[OWNER].entered
    assert c.op({"op": "exit", "ref": ref})["ok"]
    assert ctx.exited == "success"
    assert ref not in mod._callers[OWNER].entered


def test_exit_with_error_fails_the_run(bridge_server, client_factory, fake_blt):
    url, _ = bridge_server()
    c = client_factory(url, OWNER)
    ref = c.op({"op": "call", "path": ["new_flow_run_context"], "args": [], "kwargs": {"name": "bad"}})["result"]["ref"]
    c.op({"op": "enter", "ref": ref})
    c.op({"op": "exit", "ref": ref, "error": "boom"})
    assert fake_blt.flow_run_contexts()[-1].exited == ("error", "RuntimeError", "boom")


# ---------------------------------------------------------------------------
# Hardening
# ---------------------------------------------------------------------------


def test_module_root_set_forbidden(bridge_server, client_factory):
    url, _ = bridge_server()
    reply = client_factory(url, OWNER).op({"op": "set", "path": ["user"], "value": "x"})
    assert reply["ok"] is False and reply["error"]["type"] == "AttributeError"


def test_secrets_hidden_by_default(bridge_server, client_factory):
    url, _ = bridge_server()
    reply = client_factory(url, OWNER).op({"op": "get", "path": ["secrets"]})
    assert reply["ok"] is False and reply["error"]["type"] == "AttributeError"
    d = client_factory(url, OWNER).op({"op": "describe"})["result"]
    assert "secrets" not in d["functions"] and "secrets" not in d["classes"]


def test_secrets_reachable_when_exposed(bridge_server, client_factory, fake_blt, monkeypatch):
    url, _ = bridge_server(expose_secrets=True)
    monkeypatch.setattr(fake_blt, "secrets", {"API_KEY": "visible"}, raising=False)
    reply = client_factory(url, OWNER).op({"op": "getitem", "path": ["secrets"], "key": "API_KEY"})
    assert reply["ok"] and reply["result"] == "visible"


def test_context_always_hidden(bridge_server, client_factory):
    url, _ = bridge_server(expose_secrets=True)
    reply = client_factory(url, OWNER).op({"op": "get", "path": ["context"]})
    assert reply["ok"] is False and reply["error"]["type"] == "AttributeError"


def test_moduletype_never_returned_or_listed(bridge_server, client_factory):
    url, _ = bridge_server()
    c = client_factory(url, OWNER)
    reply = c.op({"op": "get", "path": ["datetime"]})
    assert reply["ok"] is False and reply["error"]["type"] == "AttributeError"
    d = c.op({"op": "describe"})["result"]
    assert "datetime" not in d["functions"] and "datetime" not in d["classes"]


# ---------------------------------------------------------------------------
# Per-caller refs + release
# ---------------------------------------------------------------------------


def test_refs_are_isolated_per_caller(bridge_server, client_factory):
    url, _ = bridge_server(shared=True)
    a, b = client_factory(url, "user-a"), client_factory(url, "user-b")
    ref = a.op(CHIP_CALL)["result"][0]["ref"]
    assert a.op({"op": "get", "ref": ref, "path": ["name"]})["ok"]
    stolen = b.op({"op": "get", "ref": ref, "path": ["name"]})
    assert stolen["ok"] is False and stolen["error"]["type"] == "LookupError"


def test_release_drops_a_ref(bridge_server, client_factory):
    url, _ = bridge_server()
    c = client_factory(url, OWNER)
    ref = c.op(CHIP_CALL)["result"][0]["ref"]
    assert c.op({"op": "get", "ref": ref, "path": ["name"]})["ok"]
    assert c.op({"op": "release", "refs": [ref]})["ok"]
    gone = c.op({"op": "get", "ref": ref, "path": ["name"]})
    assert gone["ok"] is False and gone["error"]["type"] == "LookupError"


# ---------------------------------------------------------------------------
# Pending / poll jobs
# ---------------------------------------------------------------------------


def _blocking(monkeypatch, fake_blt):
    """Make ``search_devices`` block on a gate the test releases."""
    gate = threading.Event()
    original = fake_blt.search_devices

    def slow(**kwargs):
        gate.wait(5)
        return original(**kwargs)

    monkeypatch.setattr(fake_blt, "search_devices", slow)
    return gate


def test_long_call_becomes_pending_then_polls_to_result(bridge_server, client_factory, fake_blt, monkeypatch):
    url, _ = bridge_server(call_timeout=0.2)
    gate = _blocking(monkeypatch, fake_blt)
    c = client_factory(url, OWNER)
    pending = c.op(ALL_CALL)
    assert pending["ok"] and "pending" in pending
    job = pending["pending"]
    gate.set()
    for _ in range(50):
        reply = c.op({"op": "poll", "job": job})
        if "pending" not in reply:
            break
        time.sleep(0.02)
    assert reply["ok"] and len(reply["result"]) == 10


def test_job_belongs_to_its_caller(bridge_server, client_factory, fake_blt, monkeypatch):
    url, _ = bridge_server(shared=True, call_timeout=0.2)
    gate = _blocking(monkeypatch, fake_blt)
    a, b = client_factory(url, "owner-a"), client_factory(url, "other-b")
    job = a.op(ALL_CALL)["pending"]
    stolen = b.op({"op": "poll", "job": job})
    assert stolen["ok"] is False and stolen["error"]["type"] == "LookupError"
    gate.set()
    mine = a.op({"op": "poll", "job": job})
    for _ in range(50):
        if "pending" not in mine:
            break
        time.sleep(0.02)
        mine = a.op({"op": "poll", "job": job})
    assert mine["ok"] and len(mine["result"]) == 10


# ---------------------------------------------------------------------------
# Watchdog
# ---------------------------------------------------------------------------


def test_watchdog_exits_contexts_and_drops_refs(bridge_server, client_factory, fake_blt):
    url, mod = bridge_server(idle_timeout=100)
    c = client_factory(url, OWNER)
    ref = c.op({"op": "call", "path": ["new_flow_run_context"], "args": [], "kwargs": {"name": "live"}})["result"]["ref"]
    c.op({"op": "enter", "ref": ref})
    ctx = fake_blt.flow_run_contexts()[-1]

    mod._callers[OWNER].last_seen -= 10_000
    mod.reclaim_silent()

    assert ctx.exited[0] == "error" and ctx.exited[1] == "RuntimeError"
    assert "silent" in ctx.exited[2]
    assert OWNER not in mod._callers
    orphan = c.op({"op": "get", "ref": ref, "path": ["name"]})
    assert orphan["ok"] is False and orphan["error"]["type"] == "LookupError"


# ---------------------------------------------------------------------------
# Audit (shared) + traceback scoping
# ---------------------------------------------------------------------------


def test_audit_line_written_per_call_in_shared(bridge_server, client_factory, fake_blt):
    url, _ = bridge_server(shared=True)
    before = len(fake_blt.logged_messages())
    client_factory(url, "caller-x").op(ALL_CALL)
    new = fake_blt.logged_messages()[before:]
    assert any("remote call by caller-x" in message for _, message in new)


def test_audit_uses_unpatchable_info_reference(bridge_server, client_factory, fake_blt, monkeypatch):
    url, _ = bridge_server(shared=True)
    before = len(fake_blt.logged_messages())
    monkeypatch.setattr(fake_blt, "info", lambda message: None)  # patch the public logger
    client_factory(url, "caller-y").op(ALL_CALL)
    new = fake_blt.logged_messages()[before:]
    assert any("remote call by caller-y" in message for _, message in new)


def test_traceback_only_for_owner(bridge_server, client_factory):
    url, _ = bridge_server(shared=True)
    bad = {"op": "call", "path": ["does_not_exist"], "args": [], "kwargs": {}}
    owner_error = client_factory(url, OWNER).op(bad)["error"]
    other_error = client_factory(url, "someone").op(bad)["error"]
    assert owner_error["type"] == "AttributeError" and "traceback" in owner_error
    assert other_error["type"] == "AttributeError" and "traceback" not in other_error


# ---------------------------------------------------------------------------
# Parts protocol (both directions)
# ---------------------------------------------------------------------------


def _big_device(fake_blt, nbytes=16_384):
    blob = _incompressible(nbytes)
    fake_blt.set_synthetic_devices([{"id": "big", "type": "Chip", "name": "B", "params": {"blob": blob}}])
    return blob


def test_download_is_split_into_parts(bridge_server, client_factory, fake_blt):
    url, _ = bridge_server(part_bytes=4096)
    blob = _big_device(fake_blt)
    c = client_factory(url, OWNER)
    _, _, body = c.post(ALL_CALL)
    first = json.loads(body)
    assert "parts" in first and first["parts"]["count"] > 1
    full = json.loads(c._receive(first["parts"]))
    assert full["result"][0]["fields"]["params"]["blob"] == blob


def test_upload_is_stitched_from_parts(bridge_server, client_factory, fake_blt):
    url, _ = bridge_server(part_bytes=4096)
    c = client_factory(url, OWNER)
    huge = _incompressible(16_384)
    packed = zlib.compress(json.dumps({"op": "call", "path": ["info"], "args": [huge], "kwargs": {}}).encode())
    token = secrets.token_hex(8)
    size, count = 4096, -(-len(packed) // 4096)
    before = len(fake_blt.logged_messages())
    reply = None
    for index in range(count):
        headers = {
            "X-Bridge-Parts": "1", "X-Bridge-Upload": token,
            "X-Bridge-Index": str(index), "X-Bridge-Count": str(count),
        }
        status, _, body = c.send(packed[index * size:(index + 1) * size], headers)
        reply = json.loads(body)
        assert status == 200 and reply["ok"]
    assert count > 1 and reply["result"] is None
    assert fake_blt.logged_messages()[before:][-1][1] == huge


def test_expired_part_returns_410(bridge_server, client_factory, fake_blt):
    url, mod = bridge_server(part_bytes=4096)
    _big_device(fake_blt)
    c = client_factory(url, OWNER)
    _, _, body = c.post(ALL_CALL)
    token = json.loads(body)["parts"]["id"]
    mod._transfers.clear()  # the transfer has expired / been reclaimed
    status, _, _ = c.send(json.dumps({"op": "part", "id": token, "index": 0}).encode(), {"X-Bridge-Parts": "1"})
    assert status == 410


def test_parts_are_isolated_per_caller(bridge_server, client_factory, fake_blt):
    url, _ = bridge_server(part_bytes=4096, shared=True)
    _big_device(fake_blt)
    a, b = client_factory(url, "owner-a"), client_factory(url, "other-b")
    _, _, body = a.post(ALL_CALL)
    token = json.loads(body)["parts"]["id"]
    part = json.dumps({"op": "part", "id": token, "index": 0}).encode()
    status_b, _, _ = b.send(part, {"X-Bridge-Parts": "1"})
    status_a, _, _ = a.send(part, {"X-Bridge-Parts": "1"})
    assert status_b == 410 and status_a == 200


def test_poll_reply_is_itself_split(bridge_server, client_factory, fake_blt, monkeypatch):
    url, _ = bridge_server(part_bytes=4096, call_timeout=0.2)
    blob = _big_device(fake_blt)
    gate = _blocking(monkeypatch, fake_blt)
    c = client_factory(url, OWNER)
    job = c.op(ALL_CALL)["pending"]
    gate.set()
    polled = None
    for _ in range(50):
        _, _, body = c.post({"op": "poll", "job": job})
        polled = json.loads(body)
        if "parts" in polled or "pending" not in polled:
            break
        time.sleep(0.02)
    assert "parts" in polled
    full = json.loads(c._receive(polled["parts"]))
    assert full["result"][0]["fields"]["params"]["blob"] == blob


# ---------------------------------------------------------------------------
# Wire compatibility: the client's _Protocol driven directly
# ---------------------------------------------------------------------------


def _direct_remote(url, user=OWNER):
    br = load_remote_client()

    class Direct(br._Protocol):
        def __init__(self):
            super().__init__()
            self.location = url

        def exchange(self, body, headers):
            req = urllib.request.Request(url + "/call", data=body, method="POST")
            req.add_header("X-BLT-User-Id", user)
            req.add_header("Content-Type", "application/json")
            for key, value in headers.items():
                req.add_header(key, value)
            try:
                with urllib.request.urlopen(req) as resp:
                    return resp.status, resp.read()
            except urllib.error.HTTPError as error:
                return error.code, error.read()

    proto = Direct()
    return br, br.Remote(proto, proto.invoke({"op": "describe"}))


def test_wire_compat_basic_call(bridge_server):
    url, _ = bridge_server()
    _, remote = _direct_remote(url)
    assert len(remote.search_devices()) == 10


def test_wire_compat_large_result(bridge_server, fake_blt):
    url, _ = bridge_server()
    blob = _incompressible(int(2.6 * 1024 * 1024))  # > 2 MiB even after zlib
    fake_blt.set_synthetic_devices([{"id": "big", "type": "Chip", "name": "B", "params": {"blob": blob}}])
    _, remote = _direct_remote(url)
    devices = remote.search_devices()
    assert devices[0].params["blob"] == blob


def test_wire_compat_large_upload(bridge_server, fake_blt):
    url, _ = bridge_server()
    huge = _incompressible(int(2.6 * 1024 * 1024))  # > 2 MiB even after zlib
    _, remote = _direct_remote(url)
    before = len(fake_blt.logged_messages())
    remote.info(huge)
    assert fake_blt.logged_messages()[before:][-1][1] == huge
