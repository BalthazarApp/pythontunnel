"""On-disk cache for analytics frames, scoped per Balthazar space.

The data path (:mod:`blt_analytics.frames`) pulls real values over the tunnel,
which is the expensive part — a full ``runs_df()`` pages every flow. So each frame
is memoized to disk and keyed by the arguments that produced it, so re-running a
notebook cell (or a second tool call for the same slice) is a file read, not a
round trip.

Layout
------
``<base>/<space_key>/<name>__<arghash>.<ext>`` where

* ``base`` is ``$BLT_ANALYTICS_CACHE_DIR`` if set, else ``~/.cache/blt_analytics``
  (the env override is what the tests point at a ``tmp_path``);
* ``space_key`` isolates one space's frames from another's (see :func:`space_key`);
* ``name`` is the logical frame (``"devices"`` / ``"runs"``);
* ``arghash`` is a short digest of the call's keyword arguments, so different
  projections / filters never collide.

Format: a :class:`pandas.DataFrame` is written as **parquet** when ``pyarrow`` is
importable (columnar, typed, smaller), and as **pickle** otherwise — and any value
that is not a DataFrame, or that parquet cannot represent (ragged object columns),
falls back to pickle too. Reads try whichever file is present. ``pyarrow`` is never
required: the package declares no hard dependency on it.

Freshness is the data file's mtime against a TTL (default one hour). ``refresh=True``
on a frame call bypasses the lookup and rewrites the entry.
"""

from __future__ import annotations

import hashlib
import json
import os
import pickle
import re
import time
from pathlib import Path
from typing import Any, Callable, Optional

from . import _blt

__all__ = ["cached", "space_key", "cache_dir", "clear", "DEFAULT_TTL"]

DEFAULT_TTL = 3600  # one hour
_ENV_DIR = "BLT_ANALYTICS_CACHE_DIR"
_BRIDGE_PROFILE = os.path.expanduser("~/.balthazar_bridge.json")

_MISS = object()
_SLUG_RE = re.compile(r"[^A-Za-z0-9._-]+")

# Memoized bridge root id per located-module + bridge-URL. ``space_key`` runs on
# *every* ``cached()`` lookup — cache hits included — so without this a hit still
# paid for a ``describe`` round trip. Keyed by ``(id(module), bridge_url)`` so that
# reconnecting to a different bridge (a freshly loaded drop-in and/or a new URL)
# re-queries and yields a fresh namespace. Cleared only by process exit (and tests).
_ROOT_CACHE: dict[tuple[int, str], str] = {}


# ---------------------------------------------------------------------------
# Keys and locations
# ---------------------------------------------------------------------------


def _slug(text: str) -> str:
    """A filesystem-safe fragment of ``text`` (collapsing anything exotic to ``-``)."""
    cleaned = _SLUG_RE.sub("-", str(text)).strip("-")
    return cleaned or "x"


def _short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _base_dir() -> Path:
    override = os.environ.get(_ENV_DIR)
    if override:
        return Path(os.path.expanduser(override))
    return Path(os.path.expanduser("~/.cache/blt_analytics"))


def _attr_id(blt: Any, name: str) -> Optional[str]:
    obj = getattr(blt, name, None)
    if obj is None:
        return None
    ident = getattr(obj, "id", None)
    return str(ident) if ident else None


def _bridge_url() -> str:
    url = os.environ.get("BALTHAZAR_BRIDGE_URL")
    if url:
        return url
    try:
        with open(_BRIDGE_PROFILE, encoding="utf-8") as fh:
            return str(json.load(fh).get("app_url", ""))
    except Exception:  # noqa: BLE001 - absence just means "no url component"
        return ""


def _root_id(blt: Any, url: str) -> str:
    """The bridge's root id from ``describe``, memoized per process.

    The memo (see :data:`_ROOT_CACHE`) turns a per-``cached()``-call round trip into a
    single ``describe`` for the life of the connection. Only a *successful* lookup is
    cached: a failure returns ``""`` (the url-alone fallback) without memoizing it, so
    a transient outage does not pin the key to the fallback for the whole process.
    """
    memo_key = (id(blt), url)
    if memo_key in _ROOT_CACHE:
        return _ROOT_CACHE[memo_key]
    try:
        info = _blt.describe_info() or {}
    except Exception:  # noqa: BLE001 - offline / no describe -> url alone, not memoized
        return ""
    root = str(
        info.get("flow_run")
        or info.get("flow_run_id")
        or info.get("owner")
        or ""
    )
    _ROOT_CACHE[memo_key] = root
    return root


def space_key(blt: Any = None) -> str:
    """A stable, per-space cache namespace derived from the located ``blt`` module.

    The choice, documented because the spec left it open (§4 cache):

    1. on the **bridge**, a hash of the bridge URL plus a root id from
       ``describe`` (flow run, else the owner). The endpoint + root pins the space a
       bridge is attached to without a measurement value ever entering the key. The
       bridge is checked first because a reflected ``blt.space`` would otherwise
       resolve to a method proxy, not a real id;
    2. on a **real Runner** module, an explicit ``blt.space.id`` when it exposes one
       (the cleanest, space-scoped and stable), else the flow id — the most stable
       space-scoped handle available there.

    Any failure collapses to ``"default"`` so caching never breaks the data path.
    """
    try:
        blt = blt if blt is not None else _blt.get_blt()
    except Exception:  # noqa: BLE001 - no module located -> shared default bucket
        return "default"

    try:
        tunnel = _blt.is_tunnel()
    except Exception:  # noqa: BLE001
        tunnel = getattr(blt, "__balthazar_tunnel__", False) == 3

    if tunnel:
        url = _bridge_url()
        root = _root_id(blt, url)
        return "tunnel-" + _short_hash(f"{url}|{root}")

    sid = _attr_id(blt, "space")
    if sid:
        return "space-" + _slug(sid)

    sid = _attr_id(blt, "flow") or _attr_id(blt, "session") or _attr_id(blt, "flow_run")
    if sid:
        return _slug(sid)
    return "default"


def cache_dir(blt: Any = None) -> Path:
    """The directory holding one space's cached frames."""
    return _base_dir() / space_key(blt)


def _arg_hash(key: Any) -> str:
    return _short_hash(json.dumps(key or {}, sort_keys=True, default=str))


def _paths(name: str, key: Any) -> tuple[Path, Path]:
    directory = cache_dir()
    stem = f"{_slug(name)}__{_arg_hash(key)}"
    return directory / f"{stem}.parquet", directory / f"{stem}.pkl"


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------


def _is_dataframe(value: Any) -> bool:
    try:
        import pandas as pd
    except Exception:  # noqa: BLE001
        return False
    return isinstance(value, pd.DataFrame)


def _has_pyarrow() -> bool:
    try:
        import pyarrow  # noqa: F401
    except Exception:  # noqa: BLE001
        return False
    return True


def _fresh(path: Path, ttl: float) -> bool:
    try:
        age = time.time() - path.stat().st_mtime
    except OSError:
        return False
    return age < ttl


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _load(parquet: Path, pickle_path: Path, ttl: float) -> Any:
    if parquet.exists() and _fresh(parquet, ttl):
        try:
            import pandas as pd

            return pd.read_parquet(parquet)
        except Exception:  # noqa: BLE001 - corrupt / unreadable -> treat as a miss
            pass
    if pickle_path.exists() and _fresh(pickle_path, ttl):
        try:
            with open(pickle_path, "rb") as fh:
                return pickle.load(fh)
        except Exception:  # noqa: BLE001
            pass
    return _MISS


def _store(value: Any, parquet: Path, pickle_path: Path) -> None:
    parquet.parent.mkdir(parents=True, exist_ok=True)

    if _is_dataframe(value) and _has_pyarrow():
        try:
            value.to_parquet(parquet)
            _safe_unlink(pickle_path)
            return
        except Exception:  # noqa: BLE001 - ragged object columns -> pickle instead
            _safe_unlink(parquet)

    tmp = pickle_path.with_suffix(".pkl.tmp")
    with open(tmp, "wb") as fh:
        pickle.dump(value, fh, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, pickle_path)  # atomic publish
    _safe_unlink(parquet)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def cached(
    name: str,
    builder: Callable[[], Any],
    *,
    key: Any = None,
    refresh: bool = False,
    ttl: float = DEFAULT_TTL,
) -> Any:
    """Return ``name``'s cached value for ``key``, or build, store and return it.

    Parameters
    ----------
    name
        Logical frame name — the file stem (``"devices"`` / ``"runs"``).
    builder
        Zero-argument callable that produces the value on a miss. Called at most
        once per invocation.
    key
        JSON-able description of the call's arguments; different keys never share a
        file. ``default=str`` in the digest lets datetimes and the like through.
    refresh
        Skip the lookup, rebuild, and overwrite the entry.
    ttl
        Seconds an entry stays fresh (by file mtime). Default one hour.
    """
    parquet, pickle_path = _paths(name, key)

    if not refresh:
        hit = _load(parquet, pickle_path, ttl)
        if hit is not _MISS:
            return hit

    value = builder()
    try:
        _store(value, parquet, pickle_path)
    except Exception:  # noqa: BLE001 - a cache write must never fail the data path
        pass
    return value


def clear(*, name: Optional[str] = None, blt: Any = None) -> int:
    """Delete cached entries for the current space; return how many files were removed.

    ``name=None`` clears the whole space directory; a ``name`` clears just that
    frame's entries. Intended for tests and manual cleanup.
    """
    directory = cache_dir(blt)
    if not directory.exists():
        return 0
    pattern = f"{_slug(name)}__*" if name else "*"
    removed = 0
    for path in directory.glob(pattern):
        if path.is_file():
            _safe_unlink(path)
            removed += 1
    return removed
