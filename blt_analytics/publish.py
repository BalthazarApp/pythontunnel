"""Attach matplotlib figures to a new Balthazar flow run.

``publish(figures, name, ...)`` opens a fresh child run, makes sure **only** the
given figures are the ones captured, calls ``plt.show()``, writes ``blt.output``,
and returns the new ``flow_run_id``. It leans on the environment's existing
``plt.show`` capture rather than uploading anything itself, so the same call works
both ways:

* on the **v3 bridge**, ``plt.show()`` inside an open context uploads the open
  figures it has not already sent;
* on a **real Runner**, the Balthazar matplotlib backend captures ``plt.show()``.

Showing only the requested figures
----------------------------------
Both capture paths render whatever pyplot currently manages (``plt.get_fignums()``
== the figures registered with ``matplotlib._pylab_helpers.Gcf``). So, for the
duration of the ``plt.show()`` call, this module temporarily restricts that registry
to the given figures and restores it afterwards. Nothing is closed or destroyed:
unrelated figures the user still holds survive untouched, they are merely hidden
from pyplot while the run is captured. (A figure built straight from
``matplotlib.figure.Figure`` with no pyplot manager is given a temporary one so it,
too, can be shown; that manager is dropped again on the way out.)
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from . import _blt

__all__ = ["publish"]


def _as_figure_list(figures: Any) -> list:
    """Accept a single Figure or an iterable of Figures; return a list of Figures."""
    try:
        from matplotlib.figure import Figure
    except Exception:  # noqa: BLE001 - matplotlib absent; treat input opaquely
        Figure = None  # type: ignore[assignment]

    if Figure is not None and isinstance(figures, Figure):
        return [figures]
    if isinstance(figures, (list, tuple, set)):
        return list(figures)
    return [figures]


def _manager_for(plt, fig):
    """A pyplot figure manager for ``fig``, creating a temporary one if it has none."""
    manager = getattr(getattr(fig, "canvas", None), "manager", None)
    if manager is not None:
        return manager
    try:
        from matplotlib import _pylab_helpers

        num = getattr(fig, "number", None)
        if num is None:
            existing = _pylab_helpers.Gcf.figs
            num = (max(existing) + 1) if existing else 1
            fig.number = num
        return plt._backend_mod.new_figure_manager_given_figure(num, fig)
    except Exception:  # noqa: BLE001 - backend without this hook: best effort
        return None


def _show_only(plt, figures: list) -> None:
    """Call ``plt.show()`` with only ``figures`` registered with pyplot, then restore."""
    try:
        from matplotlib import _pylab_helpers

        gcf = _pylab_helpers.Gcf
    except Exception:  # noqa: BLE001 - cannot introspect; just show everything open
        plt.show()
        return

    saved = dict(gcf.figs)  # number -> manager
    target_ids = {id(f) for f in figures}
    keep = {
        num: mgr
        for num, mgr in saved.items()
        if id(getattr(getattr(mgr, "canvas", None), "figure", None)) in target_ids
    }
    already = {id(mgr.canvas.figure) for mgr in keep.values()}
    orphans = [f for f in figures if id(f) not in already]

    created_nums: list = []
    try:
        gcf.figs.clear()
        gcf.figs.update(keep)
        for fig in orphans:
            manager = _manager_for(plt, fig)
            if manager is not None:
                gcf.figs[manager.num] = manager
                created_nums.append(manager.num)
        plt.show()
    finally:
        for num in created_nums:
            gcf.figs.pop(num, None)
        gcf.figs.clear()
        gcf.figs.update(saved)


def _run_id(blt: Any, cm: Any) -> str:
    """The new run's id, from the context handle or the rebound module name."""
    rid = getattr(cm, "flow_run_id", None)
    if rid:
        return str(rid)
    flow_run = getattr(blt, "flow_run", None)
    rid = getattr(flow_run, "id", None)
    if rid:
        return str(rid)
    raise RuntimeError("publish: could not determine the new flow_run_id")


def _set_output(blt: Any, output) -> None:
    target = blt.output
    values = dict(output)
    update = getattr(target, "update", None)
    if callable(update):
        update(values)
    else:  # pragma: no cover - every known blt.output is a dict / proxy
        for key, value in values.items():
            target[key] = value


def publish(
    figures: Any,
    name: str,
    *,
    devices: Optional[Sequence] = None,
    parameters: Optional[dict] = None,
    output: Optional[dict] = None,
) -> str:
    """Open a run named ``name``, attach ``figures``, set ``output``, return its id.

    Parameters
    ----------
    figures
        A single matplotlib ``Figure`` or a list of them — the figures to attach.
    name
        The new flow run's name.
    devices, parameters
        Passed straight to ``blt.enter_new_flow_run`` (run device list / params).
    output
        Optional mapping written to ``blt.output`` for the run (primitives, per the
        platform's output rules).
    """
    import matplotlib.pyplot as plt

    figs = _as_figure_list(figures)
    if not figs:
        raise ValueError("publish: no figures given")

    blt = _blt.get_blt()
    context = blt.enter_new_flow_run(name=name, devices=devices, parameters=parameters)
    with context:
        run_id = _run_id(blt, context)
        _show_only(plt, figs)
        if output:
            _set_output(blt, output)
    return run_id
