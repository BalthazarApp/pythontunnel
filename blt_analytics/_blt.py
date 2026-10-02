"""Locate the ``balthazar`` module to call, and tell the bridge apart from a Runner.

Two modules can answer to the name ``balthazar`` in this repo:

* the **real** Runner module — injected as a builtin on a Balthazar Runner, with
  no ``__balthazar_tunnel__`` marker;
* the **v3 reflection bridge drop-in** (``bridge/balthazar.py``) — marked
  ``__balthazar_tunnel__ = 3``; a PEP 562 module that lazily connects from the
  saved profile and delegates to a ``Remote``. Its tunnel helpers live under a
  single ``blt.tunnel`` namespace (``cached_devices``, ``cached_devices_query``,
  ``device_cache_status``, ``refresh_device_cache``, ``space_schema``). This is the
  only transport.

``get_blt`` returns, in order: an importable ``balthazar`` that is the real module
or the v3 drop-in; else — when a v3 profile exists (``~/.balthazar_bridge.json`` or
``$BALTHAZAR_BRIDGE_URL``) — the v3 drop-in loaded by path from ``$BLT_BRIDGE_DIR``
or ``<repo>/bridge/balthazar.py``. If nothing can be located it raises, so a
misconfiguration fails fast instead of silently losing the API.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
import sys
from types import ModuleType

__all__ = ["get_blt", "is_tunnel", "bridge_version", "tunnel_ns", "describe_info"]

# v3 drop-ins loaded off disk are cached by resolved path. The drop-in holds live
# state (the open-context stack, the patched ``plt.show``), so re-executing the file
# on every call would quietly reset it — load once, reuse. An importable real/v3
# module is *not* cached here: it already lives in ``sys.modules``, and skipping the
# cache lets a test swap it in and out via ``sys.modules`` and see the change.
_PATH_SHIMS: dict[str, ModuleType] = {}


def _classify(module: ModuleType) -> str:
    """One of ``"v3"`` or ``"real"`` for a candidate module.

    The v3 marker is the *integer* ``3``; anything else (including no marker) is a
    real Runner module.
    """
    return "v3" if getattr(module, "__balthazar_tunnel__", False) == 3 else "real"


def _try_import_balthazar() -> ModuleType | None:
    """Import ``balthazar`` if anything answers to the name, else ``None``.

    Goes through ``importlib`` so a test that injects ``sys.modules["balthazar"]``
    is honoured. Any failure (nothing importable, a broken module) is swallowed —
    the caller falls back to loading the v3 drop-in by path.
    """
    try:
        return importlib.import_module("balthazar")
    except Exception:  # noqa: BLE001 - absence or breakage both mean "fall back"
        return None


# ---------------------------------------------------------------------------
# v3 reflection bridge drop-in (SPEC "blt_analytics integration")
# ---------------------------------------------------------------------------


def _bridge_dir() -> str:
    """Directory holding the v3 drop-in: ``$BLT_BRIDGE_DIR`` or ``<repo>/bridge``."""
    override = os.environ.get("BLT_BRIDGE_DIR")
    if override:
        return os.path.abspath(os.path.expanduser(override))
    package_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(package_dir)
    return os.path.join(repo_root, "bridge")


def _bridge_profile_path() -> str:
    """The v3 connection profile written by ``blt-tunnel connect`` / ``save_profile``."""
    return os.path.join(os.path.expanduser("~"), ".balthazar_bridge.json")


def _v3_profile_present() -> bool:
    """Whether a v3 bridge is configured (profile file or ``$BALTHAZAR_BRIDGE_URL``)."""
    if os.environ.get("BALTHAZAR_BRIDGE_URL"):
        return True
    return os.path.exists(_bridge_profile_path())


def _try_v3_bridge() -> ModuleType | None:
    """Load the v3 drop-in by path when a bridge profile/env is configured.

    Returns the module only when (a) a profile or ``$BALTHAZAR_BRIDGE_URL`` exists,
    (b) ``<bridge_dir>/balthazar.py`` is present and imports, and (c) it classifies
    as v3. Otherwise ``None``. The drop-in's own sibling imports (``balthazar_remote``)
    need ``bridge_dir`` on ``sys.path``, so it is prepended before the module is
    executed.
    """
    if not _v3_profile_present():
        return None
    bridge_dir = _bridge_dir()
    path = os.path.join(bridge_dir, "balthazar.py")
    if path in _PATH_SHIMS:
        return _PATH_SHIMS[path]
    if not os.path.exists(path):
        return None
    if bridge_dir not in sys.path:
        sys.path.insert(0, bridge_dir)
    spec = importlib.util.spec_from_file_location("blt_analytics._v3_bridge", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:  # noqa: BLE001 - a broken drop-in must not kill analytics
        return None
    if _classify(module) != "v3":
        return None
    _PATH_SHIMS[path] = module
    return module


# ---------------------------------------------------------------------------
# Test-only override: a v3 drop-in pre-connected to a local bridge over the
# client's documented ``_test_base_url`` hook. Active only when the environment
# variable below is set (the E2E tests set it); a no-op in production.
# ---------------------------------------------------------------------------
_BRIDGE_TEST_BASE_URL_ENV = "BLT_BRIDGE_TEST_BASE_URL"
_BRIDGE_TEST_USER_ENV = "BLT_BRIDGE_TEST_USER"
_test_bridges: dict[tuple, ModuleType] = {}


def _bridge_from_test_env() -> ModuleType | None:
    """The v3 drop-in connected to ``$BLT_BRIDGE_TEST_BASE_URL`` (tests), else ``None``.

    When the env var is set, load ``<bridge_dir>/balthazar.py`` by path and inject a
    ``Remote`` connected through the client's login-free test transport
    (``connect(_test_base_url=..., _test_user_id=...)``), so ``get_blt()`` hands the
    analytics code the real drop-in talking to an in-process bridge — no profile,
    login or network. Returns ``None`` when the env var is unset (production) or the
    drop-in is missing. Connections are cached per ``(base_url, user)`` and closed by
    :func:`_reset_test_bridges`.
    """
    base = os.environ.get(_BRIDGE_TEST_BASE_URL_ENV)
    if not base:
        return None
    user = os.environ.get(_BRIDGE_TEST_USER_ENV)
    key = (base, user)
    cached = _test_bridges.get(key)
    if cached is not None:
        return cached
    bridge_dir = _bridge_dir()
    path = os.path.join(bridge_dir, "balthazar.py")
    if not os.path.exists(path):
        return None
    if bridge_dir not in sys.path:
        sys.path.insert(0, bridge_dir)
    spec = importlib.util.spec_from_file_location("blt_analytics._v3_bridge_test", path)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if _classify(module) != "v3":
        return None
    remote_mod = module._load_remote_module()
    module._remote = remote_mod.connect(_test_base_url=base, _test_user_id=user)
    _test_bridges[key] = module
    return module


def _reset_test_bridges() -> None:
    """Close and forget every test-bridge connection (called from E2E teardown)."""
    for module in _test_bridges.values():
        try:
            module._remote.close()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass
    _test_bridges.clear()


def get_blt() -> ModuleType:
    """Return the balthazar module the analytics code should call.

    Order (SPEC "blt_analytics integration"): an importable real module or v3
    drop-in; else the v3 drop-in loaded by path when a bridge profile exists. Raises
    ``RuntimeError`` if no balthazar module can be located at all.
    """
    test_bridge = _bridge_from_test_env()
    if test_bridge is not None:
        return test_bridge
    imported = _try_import_balthazar()
    if imported is not None:
        return imported
    v3 = _try_v3_bridge()
    if v3 is not None:
        return v3
    raise RuntimeError(
        "No balthazar module is reachable: neither an importable 'balthazar' (a real "
        "Runner module) nor the v3 reflection bridge. Run `blt-tunnel connect "
        '"<app url>"` to configure the bridge (writes ~/.balthazar_bridge.json), or '
        "run on a Balthazar Runner."
    )


def is_tunnel() -> bool:
    """Whether the located module is the v3 bridge drop-in (not a real Runner module).

    On a real Runner this is ``False``, and callers must not use bridge-only features
    (the ``blt.tunnel`` namespace).
    """
    try:
        return _classify(get_blt()) == "v3"
    except Exception:  # noqa: BLE001 - no module located -> not a tunnel
        return False


def bridge_version() -> int | None:
    """``3`` for the v3 drop-in, else ``None`` (real Runner, or nothing located).

    Never raises: if no balthazar module can be located at all it returns ``None``.
    """
    try:
        module = get_blt()
    except Exception:  # noqa: BLE001 - "no bridge" reads as "no version"
        return None
    return 3 if _classify(module) == "v3" else None


def _v3_tunnel_namespace(module: ModuleType) -> object | None:
    """The v3 ``blt.tunnel`` namespace if ``module`` exposes one, else ``None``.

    Duck-typed rather than keyed off :func:`bridge_version` so the analytics tests
    can drive it with a plain stub object (no file loading). A v3 tunnel namespace
    resolves its functions dynamically, so probing one attribute is enough.
    """
    try:
        ns = getattr(module, "tunnel", None)
    except Exception:  # noqa: BLE001 - a delegating drop-in may raise on connect failure
        return None
    if ns is None:
        return None
    if callable(getattr(ns, "cached_devices_query", None)) or callable(
        getattr(ns, "space_schema", None)
    ):
        return ns
    return None


def tunnel_ns():
    """Return the located module's ``blt.tunnel`` namespace, or ``None`` (real Runner)."""
    try:
        return _v3_tunnel_namespace(get_blt())
    except Exception:  # noqa: BLE001
        return None


def describe_from_object(obj: object) -> dict:
    """Best-effort v3 ``describe`` payload (dict) from a Remote or drop-in, or ``{}``.

    The real client (``bridge/balthazar_remote.py``) stores its ``describe`` reply on
    the ``Remote`` as ``remote._description`` (see ``Remote.__init__``). The v3 drop-in
    delegates attribute access to that Remote, so ``blt._description`` reaches it. A few
    other shapes are tried defensively afterwards (an older ``_session.description``,
    a ``.describe()`` method, ``_describe``); every access is guarded so a caller never
    breaks over missing describe info.
    """
    getters = (
        lambda: obj._description,  # type: ignore[attr-defined]
        lambda: obj._session.description,  # type: ignore[attr-defined]
        lambda: obj._remote._session.description,  # type: ignore[attr-defined]
        lambda: obj.describe(),  # type: ignore[attr-defined]
        lambda: obj._describe,  # type: ignore[attr-defined]
    )
    for getter in getters:
        try:
            value = getter()
        except Exception:  # noqa: BLE001 - try the next shape
            continue
        if callable(value):
            try:
                value = value()
            except Exception:  # noqa: BLE001
                continue
        if isinstance(value, dict):
            return value
    return {}


def describe_info() -> dict:
    """The v3 ``describe`` payload for the located drop-in, or ``{}`` (real Runner)."""
    try:
        module = get_blt()
    except Exception:  # noqa: BLE001
        return {}
    if _classify(module) != "v3":
        return {}
    return describe_from_object(module)
