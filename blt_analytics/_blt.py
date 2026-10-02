"""Locate the ``balthazar`` module to call, and tell the tunnel apart from a Runner.

Three modules answer to the name ``balthazar`` in this repo:

* the **real** Runner module — injected as a builtin on a Balthazar Runner, with
  no ``__balthazar_tunnel__`` marker;
* the **v2 session shim** (``session_tunnel/balthazar.py``) — marked
  ``__balthazar_tunnel__`` *and* carrying ``tunnel_state``; this is the one the
  analytics data path is built for, because its projection kwargs
  (``keys=``/``scalars_only=``) and open contexts exist only here;
* the **v1 one-shot shim** (``balthazar.py`` at the repo root) — also marked
  ``__balthazar_tunnel__`` but with no ``tunnel_state``; it predates the read ops
  and would answer ``search_flows``/``fetch_visualizations`` with ``AttributeError``.
  Picking it up is a misconfiguration we refuse loudly.

``get_blt`` returns the first of: an importable ``balthazar`` that is the real
module or the v2 shim; otherwise the v2 shim loaded by file path (``$BLT_TUNNEL_SHIM``
or ``<repo>/session_tunnel/balthazar.py``, found relative to this package). If the
path resolves to the v1 shim it raises, so a stray ``PYTHONPATH`` fails fast
instead of silently losing half the API.
"""

from __future__ import annotations

import importlib
import importlib.util
import os
from types import ModuleType

__all__ = ["get_blt", "is_tunnel"]

# v2 shims loaded off disk are cached by resolved path. The shim holds live state
# (the open-context stack, the patched ``plt.show``), so re-executing the file on
# every call would quietly reset it — load once, reuse. An importable real/v2
# module is *not* cached here: it already lives in ``sys.modules``, and skipping
# the cache lets a test swap it in and out via ``sys.modules`` and see the change.
_PATH_SHIMS: dict[str, ModuleType] = {}


def _classify(module: ModuleType) -> str:
    """One of ``"real"``, ``"v2"`` or ``"v1"`` for a candidate balthazar module."""
    if not getattr(module, "__balthazar_tunnel__", False):
        return "real"
    return "v2" if hasattr(module, "tunnel_state") else "v1"


def _try_import_balthazar() -> ModuleType | None:
    """Import ``balthazar`` if anything answers to the name, else ``None``.

    Goes through ``importlib`` so a test that injects ``sys.modules["balthazar"]``
    is honoured. Any failure (nothing importable, a broken module) is swallowed —
    the caller falls back to loading the shim by path.
    """
    try:
        return importlib.import_module("balthazar")
    except Exception:  # noqa: BLE001 - absence or breakage both mean "fall back"
        return None


def _shim_path() -> str:
    """The file path of the v2 shim: ``$BLT_TUNNEL_SHIM`` or the repo default."""
    override = os.environ.get("BLT_TUNNEL_SHIM")
    if override:
        return os.path.abspath(os.path.expanduser(override))
    # blt_analytics/_blt.py -> blt_analytics/ -> repo root -> session_tunnel/balthazar.py
    package_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(package_dir)
    return os.path.join(repo_root, "session_tunnel", "balthazar.py")


def _load_shim_from_path(path: str) -> ModuleType:
    """Load (and cache) the balthazar module at ``path``, rejecting the v1 shim."""
    if path in _PATH_SHIMS:
        return _PATH_SHIMS[path]
    if not os.path.exists(path):
        raise RuntimeError(
            f"No balthazar tunnel shim at {path}. Start the v2 session tunnel and "
            "run from a place where 'session_tunnel/balthazar.py' is reachable, or "
            "point $BLT_TUNNEL_SHIM at the v2 shim."
        )
    # Load under a private name, never "balthazar": we do not want to shadow the
    # real builtin on a Runner, and the shim keys nothing off its own module name.
    spec = importlib.util.spec_from_file_location("blt_analytics._tunnel_shim", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load a balthazar shim from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if _classify(module) == "v1":
        raise RuntimeError(
            f"The balthazar module at {path} is the v1 one-shot tunnel shim, which "
            "lacks the read ops (search_flows, fetch_visualizations, open contexts) "
            "the analytics tools need. Point $BLT_TUNNEL_SHIM at the v2 shim "
            "(session_tunnel/balthazar.py)."
        )
    _PATH_SHIMS[path] = module
    return module


def get_blt() -> ModuleType:
    """Return the balthazar module the analytics code should call.

    Prefers an importable real module or v2 shim; otherwise loads the v2 shim by
    path. Raises ``RuntimeError`` if the only balthazar reachable is the v1 shim.
    """
    imported = _try_import_balthazar()
    if imported is not None and _classify(imported) in ("real", "v2"):
        return imported
    # Either nothing importable, or the importable one is the v1 shim (common when
    # running from the repo root, where balthazar.py is v1). Load v2 by path.
    return _load_shim_from_path(_shim_path())


def is_tunnel() -> bool:
    """Whether the located module is a tunnel shim (where projection kwargs apply).

    ``get_blt`` never returns the v1 shim, so a truthy ``__balthazar_tunnel__`` can
    only be the v2 shim. On a real Runner this is ``False`` and callers must not
    pass the shim-only ``keys=``/``scalars_only=`` kwargs.
    """
    return bool(getattr(get_blt(), "__balthazar_tunnel__", False))
