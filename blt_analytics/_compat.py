"""Client-side shims for the narrow Python surface of real-Runner PyO3 objects.

The one quirk that bites the analytics *client* is the flow-run **status** (and,
symmetrically, a visualization's data **type**). On a real Runner these are PyO3
unit enums whose only string form is the *qualified* name — ``str(status)`` is
``"FlowRunStatus.FINISHED"``, not ``"FINISHED"`` — and which expose **no** ``.name``
attribute. Code that did ``str(status)`` therefore produced digest status keys like
``"FlowRunStatus.FINISHED"`` and made ``runs_df(status="finished")`` match nothing.

:func:`enum_name` is the single, ``None``-safe way to recover the bare member name
everywhere the client reads one of these values (``schema._run_to_record``,
``frames._run_identity`` / ``_run_matches`` / ``_normalize_status``). It is also a
no-op on data that already crossed the tunnel as a bare-name string, so it is safe
to apply uniformly regardless of which module ``_blt.get_blt`` located.
"""

from __future__ import annotations

from typing import Any, Optional

__all__ = ["enum_name"]


def enum_name(value: Any) -> Optional[str]:
    """Return the bare member name of a PyO3-style enum value.

    ``enum_name(FlowRunStatus.FINISHED) -> "FINISHED"`` (whose ``str`` is the
    qualified ``"FlowRunStatus.FINISHED"``). A plain string is returned as its own
    last dotted segment, so a bare ``"FINISHED"`` stays ``"FINISHED"`` and an
    already-qualified ``"FlowRunStatus.FINISHED"`` string collapses the same way.
    ``None`` maps to ``None``.
    """
    if value is None:
        return None
    return str(value).rsplit(".", 1)[-1]
