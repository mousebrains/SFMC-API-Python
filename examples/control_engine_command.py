"""A single-glider engine that waits for a quiet link before commanding.

The worked example phase 4 calls for.  It exists because sending a
command at the wrong moment is a real operational mistake, not a
hypothetical one: while a Zmodem transfer is running the glider is not
reading its terminal, so a command submitted then is accepted by SFMC
and simply goes unanswered.  See ``docs/script_control.md``.

So this engine does the patient thing.  It waits for a surfacing, waits
for the link to go quiet, and only then sends.

Nothing is sent without ``--allow-writes``::

    # Watch it decide, send nothing
    sfmc-control --glider osu685 --engine examples/control_engine_command.py --tick 5

    # Full logic, writes simulated
    sfmc-control --glider osu685 --engine examples/control_engine_command.py \\
                 --allow-writes --dry-run

    # For real
    sfmc-control --glider osu685 --engine examples/control_engine_command.py \\
                 --allow-writes --config command.yaml

``command.yaml``::

    command: "sensor m_battery"
    quiet_seconds: 20
"""

from __future__ import annotations

from sfmc_api import Event
from sfmc_api.engine import BaseControlEngine

#: Dialog that means a transfer is in progress and the glider is busy.
BUSY_MARKERS = (
    "Starting zModem transfer of",
    "Total Bytes sent/received:",
    "About to send",
)

#: Dialog that means the glider is at the surface and listening.
SURFACED_MARKERS = (
    "Hit Control-R to RESUME",
    "Glider osusim at surface.",
    "Vehicle Name:",
)


class QuietLinkCommander(BaseControlEngine):
    """Send one command per surfacing, once the link goes quiet.

    State is per glider even though this is meant for one, because
    writing it any other way would have to be undone the moment somebody
    points it at two.
    """

    sources = ("dialog",)

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self.command: str = self.config.get("command", "sensor m_battery")
        self.quiet_seconds: float = float(self.config.get("quiet_seconds", 20))
        # glider -> when we last heard anything from it
        self.last_heard: dict[str, float] = {}
        # glider -> whether it is at the surface and not transferring
        self.surfaced: dict[str, bool] = {}
        self.busy: dict[str, bool] = {}
        self.sent_this_surfacing: set[str] = set()

    def on_start(self) -> None:
        self.log("will send %r after %.0fs of quiet", self.command, self.quiet_seconds)
        self.log("run with --tick: this engine reacts to silence, which sends no events")

    def on_event(self, event: Event) -> None:
        match event.source:
            case "dialog":
                self._on_dialog(event)
            case "tick":
                # Silence is the signal, and silence delivers no dialog
                # -- so the periodic tick is what lets this engine
                # notice the link has gone quiet.  Run with --tick.
                self._maybe_send(event.glider, event.received_at)
            case "stream" if event.body.state == "disconnected":
                # She dove, or the link dropped.  Either way the next
                # surfacing is a fresh opportunity.
                self.surfaced[event.glider] = False
                self.sent_this_surfacing.discard(event.glider)
            case "result":
                self.log("%s replied: %s", event.glider, event.body)
            case "error":
                # A blocked write lands here too, naming the flag it
                # needs -- which is why this engine does not have to
                # know whether writes are enabled.
                self.log("%s: %s", event.glider, event.body)
            case "dropped":
                self.log(
                    "%s: lost %d %s events; a trigger may have been missed",
                    event.glider,
                    event.body.count,
                    event.body.source,
                )

    def _on_dialog(self, event: Event) -> None:
        glider, text = event.glider, event.body
        self.last_heard[glider] = event.received_at

        if any(marker in text for marker in BUSY_MARKERS):
            self.busy[glider] = True
            return
        if any(marker in text for marker in SURFACED_MARKERS):
            self.surfaced[glider] = True
            self.busy[glider] = False

    def _maybe_send(self, glider: str, now: float) -> None:
        """Send once the glider is up, idle, and has been quiet a while.

        Note what decides "quiet": the gap since the last dialog line,
        measured on the *host* clock via ``received_at``.  Not the
        glider's clock, which can be 48 minutes off.
        """
        if glider in self.sent_this_surfacing:
            return
        if not self.surfaced.get(glider) or self.busy.get(glider):
            return
        quiet_for = now - self.last_heard.get(glider, now)
        if quiet_for < self.quiet_seconds:
            return
        self.sent_this_surfacing.add(glider)
        self.log("%s quiet for %.0fs; sending %r", glider, quiet_for, self.command)
        self.request("send_command", glider, self.command, glider=glider, tag="command")
