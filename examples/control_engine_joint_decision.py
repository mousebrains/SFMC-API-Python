"""Two gliders forced to surface together, and one joint decision.

Normally a formation's gliders surface at different times, and each
decision can be made on its own using whatever the others last reported.
That case needs nothing special: the engine banks each glider's state as
it arrives and reads it later.

This example is the other case.  The gliders have been forced to surface
together so the controller can look at both and modify both behaviours
as one decision -- comparing positions, currents, CTD.

Three things make that harder than it sounds, and all three are the
point of this file::

    sfmc-control --glider osu684 --glider osu685 \\
                 --engine examples/control_engine_joint_decision.py \\
                 --tick 5 --allow-writes --dry-run

**A call-in can be missed.**  A glider may not surface, may surface late,
or may surface while the stream has a gap -- that last one is not
hypothetical: a run of this software sat through a real surfacing at
23:45Z because SFMC delivers a surfacing as one burst and a reconnect
had eaten it.  So the barrier must **degrade to deciding with whoever
arrived**, never block waiting for a glider that is not coming.

**The wait has a hard deadline set by the vehicle.**  She announces
``Time until diving is: N secs`` and then stops listening.  Waiting past
that means the glider that *did* surface gets nothing -- the worst
outcome, because it looks like a decision was made.

**A partial decision must be a decision.**  Acting on one glider is
usually better than acting on none, but it is a different decision from
the joint one, and the engine should know which it made.
"""

from __future__ import annotations

import time

from sfmc_api import Event
from sfmc_api.dialog_parser import SurfacingStream
from sfmc_api.engine import BaseControlEngine

#: How long to hold the barrier open for a glider that has not arrived,
#: when the vehicle has not told us its own deadline.  Deliberately
#: short: a surfacing lasts a few minutes, and a wait that outlives it
#: has spent the whole window achieving nothing.
DEFAULT_MAX_WAIT = 120.0

#: Leave this much of the surfacing window for the decision and the
#: commands themselves.  Waiting until the last second means the
#: commands arrive after she has stopped listening.
COMMAND_BUDGET = 45.0


class JointDecision(BaseControlEngine):
    """Wait for the formation, then decide once for all of it.

    State is ordinary attributes with no locking, because every event
    for every glider arrives on one thread.  That is what makes a joint
    decision straightforward to write: there is no moment at which two
    gliders' data is being updated concurrently.
    """

    sources = ("dialog",)

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self.max_wait: float = float(self.config.get("max_wait", DEFAULT_MAX_WAIT))
        self.streams: dict[str, SurfacingStream] = {}
        #: glider -> its surfacing this round
        self.arrived: dict[str, object] = {}
        #: host clock when the first glider of this round surfaced
        self.round_opened: float | None = None
        self.rounds = 0

    # ── Events ───────────────────────────────────────────────────────

    def on_event(self, event: Event) -> None:
        match event.source:
            case "dialog":
                self._on_dialog(event)
            case "tick":
                # Silence is a signal here: the barrier closes on a
                # deadline, and a glider that never arrives sends
                # nothing to notice.
                self._maybe_decide()
            case "dropped":
                # Events were lost, so this round may be built on a
                # partial surfacing.  Say so rather than deciding
                # quietly on incomplete data.
                self.log(
                    "%s: lost %d %s events; this round may be incomplete",
                    event.glider,
                    event.body.count,
                    event.body.source,
                )
            case "error":
                self.log("%s: %s", event.glider, event.body)

    def _on_dialog(self, event: Event) -> None:
        stream = self.streams.setdefault(event.glider, SurfacingStream())
        surfacing = stream.feed(event.body)
        if surfacing is None:
            return
        self.arrived[event.glider] = surfacing
        if self.round_opened is None:
            self.round_opened = event.received_at
        self.log(
            "%s surfaced; still waiting for %s",
            event.glider,
            ", ".join(self._missing()) or "nobody",
        )
        # Deliberately no decision here.  A surfacing event completes at
        # the sensor block, and the glider prints "Time until diving is:
        # N secs" *after* it -- so deciding the moment the last glider
        # arrives means deciding without knowing that glider's deadline.
        # The tick that follows has it.  Costing one tick to know how
        # long we have is the right trade inside a window of minutes.

    # ── The barrier ──────────────────────────────────────────────────

    def _missing(self) -> list[str]:
        return [g for g in self.gliders if g not in self.arrived]

    def _deadline(self) -> float:
        """When the barrier must close, on the host clock.

        The earliest of: what any arrived glider says her own dive
        deadline is, minus a budget for deciding and sending; and a
        plain timeout for the case where nobody has told us anything.
        """
        assert self.round_opened is not None
        limits = [self.round_opened + self.max_wait]
        for glider in self.arrived:
            deadline = self.streams[glider].dive_deadline
            if deadline is not None:
                limits.append(deadline - COMMAND_BUDGET)
        return min(limits)

    def _maybe_decide(self) -> None:
        if self.round_opened is None:
            return
        missing = self._missing()
        if not missing:
            self._decide(complete=True)
            return
        remaining = self._deadline() - time.time()
        if remaining <= 0:
            # A missed call-in is normal, not an error.  Deciding for
            # the glider that did surface beats waiting for one that is
            # not coming and giving neither of them anything.
            self.log("barrier closed without %s; deciding partially", ", ".join(missing))
            self._decide(complete=False)

    def _decide(self, *, complete: bool) -> None:
        """The joint decision.  Replace this with real control law."""
        self.rounds += 1
        present = sorted(self.arrived)
        self.log(
            "round %d: %s decision across %s",
            self.rounds,
            "joint" if complete else "PARTIAL",
            ", ".join(present),
        )

        # Here is where a formation controller compares the fleet: this
        # is the one moment the states are genuinely contemporaneous, so
        # it is the only time a snapshot is honest rather than a set of
        # last-known values.
        for glider in present:
            surfacing = self.arrived[glider]
            left = self.streams[glider].time_left()
            self.log(
                "  %s: %s, %s",
                glider,
                getattr(surfacing, "vehicle_name", "?"),
                "no dive deadline reported" if left is None else f"{left:.0f}s before diving",
            )
            # A real engine would send steering here, e.g.:
            #     self.request("upload_glider_file_contents", glider,
            #                  "to-glider", {"goto_l10.ma": plan},
            #                  glider=glider, tag="retask")

        self.arrived.clear()
        self.round_opened = None
