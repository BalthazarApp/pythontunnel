"""Drop-in ``balthazar`` module backed by the remote bridge.

With this directory on ``sys.path`` (or while running from it), ``import balthazar
as blt`` connects to the bridge described by ``~/.balthazar_bridge.json`` (or
``$BALTHAZAR_BRIDGE_URL``) the first time any attribute is read, and every
``blt.*`` name then delegates to that :class:`balthazar_remote.Remote`. Local code
reads exactly as a flow script would.

On a Runner this file never wins: the real module is registered as a *builtin* via
``pyo3::append_to_inittab!`` and CPython's ``BuiltinImporter`` is consulted before
the path finder. ``__balthazar_tunnel__ = 3`` marks this as the bridge drop-in so
``blt_analytics`` can tell it apart from the real Runner module.
"""

import importlib.util
import os
import threading

__balthazar_tunnel__ = 3

_remote = None
_remote_module = None
_lock = threading.RLock()


def _load_remote_module():
    """Load the sibling ``balthazar_remote.py`` by path (no package required)."""
    global _remote_module
    if _remote_module is None:
        here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(here, "balthazar_remote.py")
        spec = importlib.util.spec_from_file_location("balthazar_remote", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _remote_module = module
    return _remote_module


def _get_remote():
    global _remote
    with _lock:
        if _remote is None:
            _remote = _load_remote_module().connect_from_profile()
        return _remote


def __getattr__(name):
    # Never connect just to answer dunder/introspection lookups during import.
    if name.startswith("__") and name.endswith("__"):
        raise AttributeError(name)
    return getattr(_get_remote(), name)


def __dir__():
    try:
        return dir(_get_remote())
    except Exception:
        return ["__balthazar_tunnel__"]
