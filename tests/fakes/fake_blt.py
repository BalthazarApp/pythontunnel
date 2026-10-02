"""A fake *real* ``balthazar`` module, serving the fixture space.

This stands in for the module the Runner injects — a fake **real** module, so it
carries no ``__balthazar_tunnel__`` marker and ``blt_analytics._blt.get_blt``
treats it as the genuine article. Install it with the ``fake_blt`` fixture, which
sets ``sys.modules["balthazar"]`` and restores it afterwards; never put it on the
import path.

It exposes the read surface the server's new ops drive — ``search_devices``,
``search_flows``, ``search_flow_run_history``, ``fetch_visualizations`` — plus
``info``/``warn``/``error`` and ``flow``/``flow_run``/``session`` identity objects,
and the stub's ``FlowRunStatus`` / ``VisualizationDataType`` constants. Objects use
the real stub attribute names (``Device.params``, ``Flow.parameters``,
``OneshotFlowRun.devices``/``.status``/``.params``/``.output``, datetime
timestamps) so the server serializers exercise the same access paths they will on
a Runner.

Fault injection (spec §5), for the paging-robustness tests:

* :func:`fail_page` makes the N-th ``search_flow_run_history`` call *for a given
  flow* raise. A shrink-and-retry re-queries the same offset with a smaller page,
  which is a *different* call number, so it can succeed — exactly the recovery the
  server's pager must show.
* :func:`fail_run` makes any page whose slice would include that run id raise, so
  the pager narrows the window until it isolates (and skips) the one bad run.
"""

from __future__ import annotations

import copy
import datetime
import fnmatch
from collections import defaultdict
from typing import Any

from fakes import fixture_space

# Intentionally NO __balthazar_tunnel__: this is a fake of the *real* module.

__all__ = [
    "search_devices",
    "search_objects",
    "search_flows",
    "search_flow_run_history",
    "fetch_visualizations",
    "info",
    "warn",
    "error",
    "Device",
    "Flow",
    "FlowRun",
    "Visualization",
    "FlowRunStatus",
    "VisualizationDataType",
    "fail_page",
    "fail_run",
    "reset_faults",
    "logged_messages",
    "set_synthetic_devices",
    "user",
    "serve_app",
    "serve_app_calls",
]


# ---------------------------------------------------------------------------
# Stub-shaped value types
# ---------------------------------------------------------------------------


class _PyO3Enum:
    """A PyO3-style *unit enum member*, matching the Runner's ``FlowRunStatus`` and
    ``VisualizationDataType``.

    The real objects are deliberately narrow, and the fake must be just as narrow so
    client code cannot quietly rely on more:

    * ``str()`` / ``repr()`` are the **qualified** name (``"FlowRunStatus.FINISHED"``,
      ``"VisualizationDataType.SVG"``) — *not* the bare member name;
    * there is **no** ``.name`` attribute (``__slots__`` with no ``name`` slot means
      accessing or setting one raises ``AttributeError``);
    * members are class-attribute singletons and compare / hash by **identity**
      (``FlowRunStatus.FINISHED is FlowRunStatus.FINISHED``), so — like the real
      enum — a member never equals a plain string.
    """

    __slots__ = ("_qualname",)

    def __init__(self, qualname: str) -> None:
        self._qualname = qualname

    def __repr__(self) -> str:
        return self._qualname

    __str__ = __repr__

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)


def _build_enum(name: str, members: tuple[str, ...]) -> type:
    """Build a PyO3-style enum class with singleton members, like the real stub."""
    cls = type(name, (_PyO3Enum,), {"__slots__": ()})
    for member in members:
        setattr(cls, member, cls(f"{name}.{member}"))
    return cls


# Member sets taken from the stub (FlowRunStatus ~line 1297, VisualizationDataType
# ~line 2519 of resources/balthazar.py).
FlowRunStatus = _build_enum(
    "FlowRunStatus",
    ("PREPARING", "READY", "RUNNING", "FINISHED", "FAILED", "KILLED", "ABORTED",
     "FAILED_TO_START"),
)
VisualizationDataType = _build_enum("VisualizationDataType", ("SVG", "PNG", "JPG"))


class _Ident:
    """Stands in for blt.flow / blt.flow_run / blt.session."""

    def __init__(self, id_: str, name: str | None = None):
        self.id = id_
        self.name = name
        self.tags: list[str] = []

    def __repr__(self) -> str:
        return f"<{type(self).__name__} id={self.id} name={self.name!r}>"


class Device:
    def __init__(self, record: dict):
        self.id: str = record["id"]
        self.type: str = record.get("type", "device")
        self.name: str = record.get("name", "")
        self.fabrication_date = record.get("fabrication_date")  # datetime.date | None
        self.description = record.get("description")
        self.image_id = None
        self.tags: list[str] = list(record.get("tags") or [])
        # Plain dict: dict-like, and what the real proxy behaves as for reads.
        self.params: dict[str, Any] = dict(record.get("params") or {})

    def __repr__(self) -> str:
        return f"<Device {self.name!r} type={self.type!r} id={self.id}>"


class FlowParameterMetadata:
    """Flow parameter metadata, carrying the record fields directly.

    The real stub's ``FlowParameterMetadata`` names its fields ``type_info`` /
    ``unit`` / ``comment``; the server maps those to the record's
    ``type``/``default``/``description``. The fake carries the record fields as
    attributes so the server can read them straight off (how the production
    serializer derives them from ``type_info`` is its own concern).
    """

    def __init__(self, meta: dict):
        self.type = meta.get("type")
        self.default = meta.get("default")
        self.description = meta.get("description")

    def __repr__(self) -> str:
        return f"<FlowParameterMetadata type={self.type!r}>"


class Flow:
    def __init__(self, record: dict):
        self.id: str = record["id"]
        self.name: str = record["name"]
        self.description = record.get("description")
        self.repository = None
        self.branch = record.get("branch")
        self.script_filename = record.get("script_filename")
        self.tags: list[str] = list(record.get("tags") or [])
        self.created_time = record.get("created_time")  # datetime
        self.username = record.get("username")
        self.email = None
        self.parameters = {
            name: FlowParameterMetadata(meta)
            for name, meta in (record.get("parameters") or {}).items()
        }

    def __repr__(self) -> str:
        return f"<Flow {self.name!r} id={self.id}>"


class FlowRun:
    """An oneshot flow run, with the stub's ``.devices`` (not device_ids)."""

    def __init__(self, record: dict, device_index: dict[str, Device]):
        self.id: str = record["id"]
        self.type = "ONESHOT"
        self.name = record.get("flow_name")
        _status_name = str(record.get("status"))
        # Resolve to the real singleton member so identity comparison works; fall
        # back to a fresh PyO3-style member for an unknown status name.
        self.status = getattr(
            FlowRunStatus, _status_name, FlowRunStatus(f"FlowRunStatus.{_status_name}")
        )
        self.created_time = record.get("created_time")
        self.started_time = record.get("started_time")
        self.finished_time = record.get("finished_time")
        self.flow_id = record.get("flow_id")
        self.flow_name = record.get("flow_name")
        self.username = record.get("username")
        self.tags: list[str] = list(record.get("tags") or [])
        self.comment = record.get("comment")
        self.params: dict[str, Any] = dict(record.get("params") or {})
        self.output: dict[str, Any] = dict(record.get("output") or {})
        self.visualization_ids: list[str] = list(record.get("visualization_ids") or [])
        self.devices: list[Device] = [
            device_index[did] for did in (record.get("device_ids") or []) if did in device_index
        ]

    def __repr__(self) -> str:
        return f"<FlowRun {self.id} flow={self.flow_name!r} status={self.status}>"


class Visualization:
    def __init__(self, record: dict):
        self.id: str = record["id"]
        # The real .type is a PyO3 VisualizationDataType enum (str() ->
        # "VisualizationDataType.SVG", no .name), so carry the singleton member
        # rather than the raw wire string the record stores.
        self.type = getattr(
            VisualizationDataType, str(record.get("type") or "SVG"),
            VisualizationDataType.SVG,
        )
        self.flow_run_id = record.get("flow_run_id")
        self.filename = record.get("filename")
        self.timestamp = record.get("timestamp")
        self.data: bytes = record.get("data", b"")

    def __repr__(self) -> str:
        return f"<Visualization {self.id} {self.filename!r}>"


# ---------------------------------------------------------------------------
# Module state: identities, logs, faults
# ---------------------------------------------------------------------------

flow = _Ident("flow-tunnel", "Session tunnel")
flow_run = _Ident("run-tunnel-root", "Session tunnel")
session = _Ident("session-tunnel-root")
# NB: deliberately NO module-level ``space`` — a real Runner has none, and the device
# cache falls back to the root flow id ("flow-tunnel") when it is absent. A test that
# wants to exercise the space-id branch sets ``blt.space`` with monkeypatch.

_LOG: list[tuple[str, str]] = []

FAULTS: dict[str, set] = {"fail_pages": set(), "fail_runs": set()}
_history_calls: dict[str, int] = defaultdict(int)

# App-tunnel support (spec §7). A real Runner exposes ``blt.user`` (the starting
# user's id) and ``blt.serve_app(port)``. Tests set ``user`` (via monkeypatch) and read
# back the recorded ``serve_app`` port(s) to assert the second listener bound correctly.
user: str | None = None
_serve_app_calls: list[int] = []

# Test hook (spec §6 perf sanity check): when set, ``search_devices`` serves these raw
# device records instead of the fixture space, so a test can drive a large synthetic
# space through the server's device cache without perturbing the fixture counts.
_synthetic_records: list[dict] | None = None


def set_synthetic_devices(records: list[dict] | None) -> None:
    """Serve ``records`` from ``search_devices`` instead of the fixture (None resets).

    Records are plain §1-style device dicts (``id``/``type``/``name``/``params``/…).
    Pass ``None`` to restore the fixture space.
    """
    global _synthetic_records
    _synthetic_records = [copy.deepcopy(r) for r in records] if records is not None else None


def reset_faults() -> None:
    """Clear injected faults, per-flow call counters, the synthetic device override and
    the app-tunnel state (call between tests)."""
    global _synthetic_records, user
    FAULTS["fail_pages"] = set()
    FAULTS["fail_runs"] = set()
    _history_calls.clear()
    _synthetic_records = None
    _serve_app_calls.clear()
    user = None


def serve_app(port: int) -> None:
    """Record the port a listener was published on (the real Runner publishes the app
    tunnel here). Tests read it back via :func:`serve_app_calls`."""
    _serve_app_calls.append(int(port))


def serve_app_calls() -> list[int]:
    """The ports passed to :func:`serve_app`, in order (for assertions)."""
    return list(_serve_app_calls)


def fail_page(n: int) -> None:
    """Make the N-th ``search_flow_run_history`` call (per flow, 1-based) raise."""
    FAULTS["fail_pages"].add(int(n))


def fail_run(run_id: str) -> None:
    """Make any history page whose slice would include ``run_id`` raise."""
    FAULTS["fail_runs"].add(run_id)


def logged_messages() -> list[tuple[str, str]]:
    """The ``(level, message)`` log calls, for assertions."""
    return list(_LOG)


def info(message: str) -> None:
    _LOG.append(("info", str(message)))


def warn(message: str) -> None:
    _LOG.append(("warn", str(message)))


def error(message: str) -> None:
    _LOG.append(("error", str(message)))


# ---------------------------------------------------------------------------
# Read API
# ---------------------------------------------------------------------------


def _as_list(value: Any) -> list | None:
    if value is None:
        return None
    return list(value) if isinstance(value, (list, tuple, set)) else [value]


def _devices() -> list[Device]:
    source = _synthetic_records if _synthetic_records is not None else fixture_space.raw_devices()
    return [Device(r) for r in source]


def _device_index() -> dict[str, Device]:
    return {d.id: d for d in _devices()}


def search_devices(
    *,
    id: Any = None,
    type: Any = None,
    name: Any = None,
    tags: Any = None,
    offset: int = 0,
    limit: int = 0,
    archived: Any = None,
    **_ignored: Any,
) -> list[Device]:
    """Search fixture devices; filters are ANDed, names glob, ordered by id."""
    devices = sorted(_devices(), key=lambda d: d.id)

    ids = _as_list(id)
    types = _as_list(type)
    names = _as_list(name)
    tag_filter = _as_list(tags)

    def matches(d: Device) -> bool:
        if ids is not None and d.id not in ids:
            return False
        if types is not None and d.type not in types:
            return False
        if names is not None and not any(fnmatch.fnmatchcase(d.name, pat) for pat in names):
            return False
        if tag_filter is not None and not all(t in d.tags for t in tag_filter):
            return False
        return True

    result = [d for d in devices if matches(d)]
    if offset:
        result = result[offset:]
    if limit:
        result = result[:limit]
    return result


search_objects = search_devices


def search_flows(
    *,
    name: Any = None,
    flow_ids: Any = None,
    tags: Any = None,
    limit: int = 1000,
    offset: int = 0,
    **_ignored: Any,
) -> list[Flow]:
    """Search fixture flows; filters are ANDed, names glob, ordered by id."""
    flows = sorted((Flow(r) for r in fixture_space.raw_flows()), key=lambda f: f.id)

    ids = _as_list(flow_ids)
    names = _as_list(name)
    tag_filter = _as_list(tags)

    def matches(f: Flow) -> bool:
        if ids is not None and f.id not in ids:
            return False
        if names is not None and not any(fnmatch.fnmatchcase(f.name, pat) for pat in names):
            return False
        if tag_filter is not None and not all(t in f.tags for t in tag_filter):
            return False
        return True

    result = [f for f in flows if matches(f)]
    if offset:
        result = result[offset:]
    if limit:
        result = result[:limit]
    return result


def search_flow_run_history(
    *,
    flow_id: Any = None,
    device_id: Any = None,
    flow_run_ids: Any = None,
    limit: int = 1000,
    offset: int = 0,
    runner_id: Any = None,
    **_ignored: Any,
) -> list[FlowRun]:
    """Search fixture runs, with page/run fault injection for the pager tests.

    ``limit <= 0`` means no limit. Faults are checked against the *matched* slice:
    a page-number fault is per ``flow_id`` and increments on every call (so a
    shrink-retry is a fresh number that may pass); a run fault trips whenever the
    slice would surface that run.
    """
    index = _device_index()
    runs = sorted((FlowRun(r, index) for r in fixture_space.raw_runs()), key=lambda r: r.id)

    ids = _as_list(flow_run_ids)

    def matches(run: FlowRun) -> bool:
        if flow_id is not None and run.flow_id != flow_id:
            return False
        if device_id is not None and device_id not in [d.id for d in run.devices]:
            return False
        if ids is not None and run.id not in ids:
            return False
        return True

    matched = [r for r in runs if matches(r)]

    # Page-number fault: count calls per flow (None is a valid key for "all flows").
    call_key = flow_id if flow_id is not None else "__all__"
    _history_calls[call_key] += 1
    if _history_calls[call_key] in FAULTS["fail_pages"]:
        raise RuntimeError(f"injected page fault on call {_history_calls[call_key]} for {call_key}")

    page = matched[offset:] if limit in (0, None) else matched[offset : offset + limit]

    if FAULTS["fail_runs"] and any(r.id in FAULTS["fail_runs"] for r in page):
        raise RuntimeError("injected run fault: page contains a failing run id")

    return page


def fetch_visualizations(visualization_ids: Any) -> dict[str, Visualization]:
    """Return ``{id: Visualization}`` for known ids (unknown ids are omitted)."""
    wanted = set(_as_list(visualization_ids) or [])
    return {
        v["id"]: Visualization(v)
        for v in fixture_space.raw_visualizations()
        if v["id"] in wanted
    }
