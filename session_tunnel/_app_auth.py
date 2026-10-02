"""Balthazar app-tunnel login client — stdlib only.

This is a trimmed port of the login/transport half of the developer prototype
``remoteblt/balthazar_remote.py`` (its ``_Http`` / ``_Session``). Credit to that
prototype: the Keycloak device/browser/password flows, the refresh-token cache,
the ``grant-access`` cookie exchange and the site-discovery logic are all its
design. What is **not** ported is the generic reflection bridge (the remote object
/ deferred / symbol machinery and the ``decode``/``invoke`` value protocol): this
module does transport only, returning ``(status, body)`` for the shim to interpret.

Why a standalone, stdlib-only module living next to the shim: the v2 session shim
(``session_tunnel/balthazar.py``) is stdlib-only and may be copied out on its own.
It loads this file by path (``importlib``) rather than importing a package, so the
pair keeps working wherever the shim lands. ``blt_analytics`` reuses it too.

The public surface is :class:`AppSession`:

    session = AppSession("https://host/app-tunnel/<runner>/<flow>/?space_id=...")
    status, body = session.post_json("/rpc", b'{"op": "ping", ...}', timeout=65)
    session.forget()          # drop the cached refresh token

``post_json`` grants the ``blt_tunnel_<runner>_<flow>`` cookie on demand and, on a
401, grants again once before returning. No ``Authorization`` header ever reaches
the app listener — the platform proxy strips it, so the cookie is the only auth.
"""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import os
import pathlib
import re
import secrets
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

CLIENT_ID = "blt-frontend2"
BROWSER_REDIRECT = "http://localhost:8000/callback"
TOKEN_CACHE = pathlib.Path.home() / ".config" / "balthazar" / "remote.json"
USER_AGENT = "balthazar-app-tunnel/0.1"
FORM = {"Content-Type": "application/x-www-form-urlencoded"}
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"

# Plaintext HTTP is refused by default (tokens would travel in the clear). Tests
# that stand up a fake Keycloak + tunnel over ``http://127.0.0.1`` flip this on —
# never set it against a real site. The env var is read live so a test can set it
# after import; the module global is the monkeypatch seam.
_ALLOW_INSECURE_HTTP = False


def _insecure_http_allowed() -> bool:
    return _ALLOW_INSECURE_HTTP or os.environ.get("BALTHAZAR_TUNNEL_ALLOW_HTTP") == "1"


class AppAuthError(RuntimeError):
    """A login, grant or transport step failed (not a remote app exception)."""


class LoginRequired(AppAuthError):
    """Raised in non-interactive mode when no cached access/refresh token works.

    An MCP stdio server or ``blt-tunnel doctor`` must never block on a device-code
    or browser prompt, so ``AppSession(interactive=False)`` refuses to start one and
    raises this instead — the caller tells the user to log in from a terminal.
    """


def _failure(status, body, what):
    text = body.decode("utf-8", "replace").strip()[:300] if isinstance(body, (bytes, bytearray)) else str(body)
    return AppAuthError("%s: HTTP %s %s" % (what, status, text))


def _tunnel_failure(status, body, what):
    if status == 404:
        return AppAuthError("tunnel flow not running / app tunnel not active (HTTP 404)")
    if status == 504:
        return AppAuthError("call exceeded the 60 s app-tunnel limit (HTTP 504)")
    return _failure(status, body, what)


def _origin_key(address):
    parsed = urllib.parse.urlsplit(address or "")
    try:
        port = parsed.port
    except ValueError:
        port = None
    return parsed.scheme, (parsed.hostname or "").lower(), port or {"http": 80, "https": 443}.get(parsed.scheme)


def _site_candidates(origin):
    parsed = urllib.parse.urlsplit(origin)
    host = parsed.netloc
    names = []
    if "app-tunnel" in host:
        names.append(host.replace("app-tunnel", "core", 1))
    if host.startswith("app-tunnel."):
        names.append(host[len("app-tunnel.") :])
    names.append(host)
    return ["%s://%s" % (parsed.scheme, name) for name in dict.fromkeys(names)]


# ---------------------------------------------------------------------------
# Token cache (refresh tokens only, 0600, keyed by ``authority|client_id``)
# ---------------------------------------------------------------------------


def _read_cache() -> dict:
    try:
        cached = json.loads(TOKEN_CACHE.read_text())
    except (OSError, ValueError):
        return {}
    return cached if isinstance(cached, dict) else {}


def _write_cache(cache: dict) -> None:
    TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(str(TOKEN_CACHE), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as file:
        json.dump(cache, file)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _Http:
    def __init__(self, ca_file=None):
        context = ssl.create_default_context(cafile=ca_file)
        handlers = [urllib.request.HTTPSHandler(context=context), _NoRedirect()]
        if _insecure_http_allowed():
            handlers.append(urllib.request.HTTPHandler())
        self._opener = urllib.request.build_opener(*handlers)

    def request(self, method, url, headers=None, data=None, timeout=90):
        merged = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        merged.update(headers or {})
        request = urllib.request.Request(url, data=data, method=method, headers=merged)
        try:
            with self._opener.open(request, timeout=timeout) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.headers, error.read()
        except (OSError, http.client.HTTPException) as error:
            reason = getattr(error, "reason", None) or error
            raise AppAuthError("cannot reach %s: %s" % (url, reason)) from None


# ---------------------------------------------------------------------------
# AppSession
# ---------------------------------------------------------------------------


class AppSession:
    """Logs in to Balthazar and talks to one app tunnel over its grant-access cookie.

    ``app_url`` is the address copied from the opened app's address bar, e.g.
    ``https://<host>/app-tunnel/<runner id>/<flow id>/?space_id=...&context_id=...``.
    The trailing slash matters; the proxy strips the prefix, so the app itself sees
    ``/rpc``. Login is ``device`` (default, no browser needed), ``browser`` (PKCE on
    ``http://localhost:8000/callback``) or ``password`` (needs ``username`` and
    ``password``; never remembered). ``site`` short-circuits the ``/api/frontend/config``
    discovery of the OIDC authority. ``ca_file`` adds a private CA.

    Site discovery is lazy — nothing hits the network until the first ``post_json``
    (or an explicit ``forget``, which never does), so building a session is cheap.
    """

    def __init__(
        self,
        app_url,
        *,
        site=None,
        login="device",
        username=None,
        password=None,
        ca_file=None,
        remember=True,
        client_id=CLIENT_ID,
        interactive=True,
    ):
        if login not in ("device", "password", "browser"):
            raise AppAuthError("login must be 'device', 'password' or 'browser'")
        if login == "password" and not (username and password):
            raise AppAuthError("login='password' needs username and password")
        parsed = urllib.parse.urlsplit(app_url)
        match = re.match(r"^/app-tunnel/([0-9a-fA-F-]{36})/([0-9a-fA-F-]{36})(/|$)", parsed.path)
        if not match:
            raise AppAuthError(
                "app_url must look like https://<host>/app-tunnel/<runner id>/<flow id>/?space_id=..."
            )
        query = urllib.parse.parse_qs(parsed.query)
        self._space_id = (query.get("space_id") or query.get("spaceId") or [None])[0]
        self._context_id = (query.get("context_id") or query.get("contextId") or [None])[0]
        if not self._space_id:
            raise AppAuthError(
                "app_url must carry ?space_id=..., copy it from the address bar of the opened app"
            )
        self._ids = "%s/%s" % (match.group(1), match.group(2))
        self._origin = "%s://%s" % (parsed.scheme, parsed.netloc)
        self._tunnel = "%s/app-tunnel/%s/" % (self._origin, self._ids)
        self.location = self._tunnel
        self._login_mode = login
        self._username = username
        self._password = password
        self._client_id = client_id
        self._remember = remember and login != "password"
        self._interactive = interactive
        self._ca_file = ca_file
        self._http = _Http(ca_file)
        self._lock = threading.RLock()
        self._access_token = None
        self._access_exp = 0.0
        self._access_lifetime = 300.0
        self._refresh_token = None
        self._cookie = None
        self._cookie_exp = 0.0
        self._site_arg = site
        self._site = None
        self._authority = None
        self._cache_key = None

    # -- site discovery (lazy) ------------------------------------------------

    def _ensure_site(self):
        if self._authority is not None:
            return
        self._site, self._authority = self._find_site(self._site_arg)
        self._cache_key = "%s|%s" % (self._authority, self._client_id)
        if self._remember and self._refresh_token is None:
            self._refresh_token = _read_cache().get(self._cache_key)

    def _find_site(self, site):
        if site:
            site = site.rstrip("/")
            status, _, body = self._http.request("GET", site + "/api/frontend/config")
            if status != 200:
                raise _failure(status, body, "%s does not answer like Balthazar" % site)
            return site, json.loads(body)["oidcAuthority"].rstrip("/")
        for candidate in _site_candidates(self._origin):
            try:
                status, _, body = self._http.request("GET", candidate + "/api/frontend/config", timeout=15)
                config = json.loads(body) if status == 200 else None
            except (AppAuthError, ValueError):
                continue
            if not isinstance(config, dict) or not config.get("oidcAuthority"):
                continue
            if _origin_key(config.get("appTunnelOrigin")) == _origin_key(self._origin):
                return candidate, config["oidcAuthority"].rstrip("/")
        raise AppAuthError(
            'cannot work out the Balthazar address for %s, pass site="https://..."' % self._origin
        )

    # -- tokens ---------------------------------------------------------------

    def _write_refresh_token(self):
        if not self._remember or self._cache_key is None:
            return
        cache = _read_cache()
        if cache.get(self._cache_key) == self._refresh_token:
            return
        if self._refresh_token:
            cache[self._cache_key] = self._refresh_token
        else:
            cache.pop(self._cache_key, None)
        _write_cache(cache)

    def _token_request(self, params):
        form = {"client_id": self._client_id}
        form.update(params)
        status, _, body = self._http.request(
            "POST",
            self._authority + "/protocol/openid-connect/token",
            headers=FORM,
            data=urllib.parse.urlencode(form).encode(),
        )
        try:
            payload = json.loads(body)
        except ValueError:
            payload = {"error": "HTTP %s" % status}
        return status, payload

    def _store_tokens(self, payload):
        self._access_token = payload["access_token"]
        self._access_lifetime = float(payload.get("expires_in") or 300)
        self._access_exp = time.time() + self._access_lifetime
        self._refresh_token = payload.get("refresh_token")
        self._write_refresh_token()

    def _access(self, min_left):
        self._ensure_site()
        if self._access_token and self._access_exp - time.time() > min_left:
            return self._access_token
        if self._refresh_token:
            status, payload = self._token_request(
                {"grant_type": "refresh_token", "refresh_token": self._refresh_token}
            )
            if status == 200:
                self._store_tokens(payload)
                return self._access_token
        if not self._interactive:
            # No valid access/refresh token, and we are forbidden from prompting.
            raise LoginRequired("run `blt-tunnel connect` in a terminal")
        if self._login_mode == "password":
            status, payload = self._token_request(
                {
                    "grant_type": "password",
                    "username": self._username,
                    "password": self._password,
                    "scope": "openid",
                }
            )
        elif self._login_mode == "device":
            status, payload = self._device_login()
        else:
            status, payload = self._browser_login()
        if status != 200:
            raise AppAuthError("login failed: %s" % (payload.get("error_description") or payload.get("error")))
        self._store_tokens(payload)
        return self._access_token

    def _device_login(self):
        status, _, body = self._http.request(
            "POST",
            self._authority + "/protocol/openid-connect/auth/device",
            headers=FORM,
            data=urllib.parse.urlencode({"client_id": self._client_id, "scope": "openid"}).encode(),
        )
        if status != 200:
            raise _failure(status, body, "this Balthazar does not allow the device login")
        device = json.loads(body)
        address = device.get("verification_uri_complete") or device["verification_uri"]
        print("To sign in, open %s and confirm the code %s" % (address, device["user_code"]), file=sys.stderr)
        interval = device.get("interval") or 5
        deadline = time.time() + (device.get("expires_in") or 600)
        while time.time() < deadline:
            time.sleep(interval)
            status, payload = self._token_request(
                {"grant_type": DEVICE_GRANT, "device_code": device["device_code"]}
            )
            error = payload.get("error")
            if error == "slow_down":
                interval += 5
            elif error != "authorization_pending":
                return status, payload
        return 408, {"error": "the code was not confirmed in time"}

    def _browser_login(self):
        verifier = secrets.token_urlsafe(64)
        digest = hashlib.sha256(verifier.encode()).digest()
        state = secrets.token_urlsafe(16)
        received = {}

        class Callback(BaseHTTPRequestHandler):
            def do_GET(self):
                query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
                if query.get("state") == [state]:
                    received.update({key: values[0] for key, values in query.items()})
                body = b"You can close this tab and return to the script."
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                return

        redirect = urllib.parse.urlsplit(BROWSER_REDIRECT)
        try:
            server = HTTPServer((redirect.hostname, redirect.port), Callback)
        except OSError as error:
            raise AppAuthError(
                "the browser login needs port %s on this computer: %s" % (redirect.port, error)
            ) from None
        address = "%s/protocol/openid-connect/auth?%s" % (
            self._authority,
            urllib.parse.urlencode(
                {
                    "client_id": self._client_id,
                    "response_type": "code",
                    "redirect_uri": BROWSER_REDIRECT,
                    "scope": "openid",
                    "state": state,
                    "code_challenge": base64.urlsafe_b64encode(digest).decode().rstrip("="),
                    "code_challenge_method": "S256",
                }
            ),
        )
        print("To sign in, open %s" % address, file=sys.stderr)
        webbrowser.open(address)
        server.timeout = 1
        deadline = time.time() + 300
        try:
            while not received and time.time() < deadline:
                server.handle_request()
        finally:
            server.server_close()
        if "code" not in received:
            return 408, {
                "error": received.get("error_description") or received.get("error") or "no answer from the browser"
            }
        return self._token_request(
            {
                "grant_type": "authorization_code",
                "code": received["code"],
                "redirect_uri": BROWSER_REDIRECT,
                "code_verifier": verifier,
            }
        )

    # -- grant-access cookie --------------------------------------------------

    def _grant(self):
        status, body = None, b""
        for attempt in range(2):
            token = self._access(min(120.0, self._access_lifetime / 2))
            headers = {"Authorization": "Bearer %s" % token, "X-BLT-Space-Id": self._space_id}
            if self._context_id:
                headers["X-BLT-Context-Id"] = self._context_id
            status, reply_headers, body = self._http.request(
                "POST",
                "%s/api/app-tunnel/grant-access/%s" % (self._site, self._ids),
                headers=headers,
                data=b"",
            )
            if status not in (400, 401) or attempt:
                break
            self._access_token = None
        if status != 200:
            raise _tunnel_failure(status, body, "Balthazar refused access to the app tunnel")
        for cookie in reply_headers.get_all("Set-Cookie") or []:
            pair = cookie.split(";", 1)[0].strip()
            if pair.startswith("blt_tunnel_"):
                self._cookie = pair
                self._cookie_exp = self._access_exp
                return
        raise AppAuthError("Balthazar did not return an app tunnel cookie")

    def _tunnel_cookie(self, force):
        with self._lock:
            if force or not self._cookie or time.time() > self._cookie_exp - 5:
                self._grant()
            return self._cookie

    # -- public ---------------------------------------------------------------

    def post_json(self, path, body, timeout=90):
        """POST ``body`` to ``{tunnel}{path}`` with the grant cookie; return (status, bytes).

        Sends only ``Cookie`` and ``Content-Type`` — never ``Authorization`` (the
        proxy strips it). On a 401 the cookie is regranted once and the call retried.
        """
        url = self._tunnel + str(path).lstrip("/")
        status, reply = None, b""
        for attempt in range(2):
            headers = {"Cookie": self._tunnel_cookie(attempt == 1), "Content-Type": "application/json"}
            status, _, reply = self._http.request("POST", url, headers=headers, data=body, timeout=timeout)
            if status != 401:
                break
        return status, reply

    def forget(self):
        """Delete this session's cached refresh token (no network).

        When the OIDC authority is already known the exact ``authority|client_id``
        entry is dropped; otherwise every entry for this ``client_id`` is removed,
        so disconnecting works even offline (before any site discovery).
        """
        self._access_token = None
        self._refresh_token = None
        self._cookie = None
        cache = _read_cache()
        if not cache:
            return
        if self._cache_key is not None:
            cache.pop(self._cache_key, None)
        else:
            suffix = "|" + self._client_id
            for key in [k for k in cache if k.endswith(suffix)]:
                cache.pop(key, None)
        _write_cache(cache)
