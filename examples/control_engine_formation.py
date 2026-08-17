"""Two control engines: a monitor, and a formation watcher.

Phase 2 is read-only, so neither of these commands a glider.  They
show the shape an engine takes and, in the formation case, the point of
the whole design: cross-glider state held in ordinary attributes with
no locking anywhere.

Run against a live server::

    python examples/control_engine_formation.py osu684 osu685

Or offline against a recorded dialog log, with no network at all::

    python examples/control_engine_formation.py --replay dialog.log osusim
"""

from __future__ import annotations

import argparse
import sys
import time

from sfmc_api import Event, SFMCClient
from sfmc_api.engine import BaseControlEngine, EngineRunner


class Monitor(BaseControlEngine):
    """What ``sfmc-monitor-glider`` does, as an engine.

    The acceptance case for a single glider: print every line, and say
    when the stream comes and goes.
    """

    sources = ("dialog",)

    def on_start(self) -> None:
        self.log("watching %s", ", ".join(self.gliders) or "nothing yet")

    def on_event(self, event: Event) -> None:
        match event.source:
            case "dialog":
                print(f"{event.glider}  {event.body}", flush=True)
            case "stream":
                print(f"{event.glider}  [stream {event.body.state}]", flush=True)
            case "dropped":
                # Never silent: bounded queues drop the oldest, and the
                # count says how much was lost.
                print(
                    f"{event.glider}  [LOST {event.body.count} "
                    f"{event.body.source} events: {event.body.reason}]",
                    flush=True,
                )


class Formation(BaseControlEngine):
    """Tracks every glider's last known position, and compares them.

    Note what is *absent*: no lock, no queue of its own, no thread.
    ``on_event`` runs on one thread for the whole fleet, so
    ``self.last_fix`` is ordinary mutable state that happens to span
    gliders.  That is the payoff the design exists for.
    """

    sources = ("dialog",)

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # glider -> (text, host clock when it arrived)
        self.last_fix: dict[str, tuple[str, float]] = {}

    def on_event(self, event: Event) -> None:
        if event.source != "dialog" or "GPS Location:" not in event.body:
            return
        self.last_fix[event.glider] = (event.body.strip(), event.received_at)
        self.report()

    def report(self) -> None:
        """Print the fleet as what it is: last-known values, with ages.

        Never as a snapshot.  Each glider is observed independently, so
        one may be minutes stale while another is current — a formation
        engine that forgets this will steer on old data.
        """
        now = time.time()
        print(f"--- fleet, {len(self.last_fix)} glider(s) ---", flush=True)
        for glider in sorted(self.last_fix):
            text, at = self.last_fix[glider]
            print(f"  {glider:10s} {now - at:6.0f}s old  {text}", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gliders", nargs="+")
    parser.add_argument("--host", help="SFMC host")
    parser.add_argument("--replay", metavar="DIALOG.log", help="Offline, no network")
    parser.add_argument(
        "--engine",
        choices=("monitor", "formation"),
        default="formation",
        help="Which example engine to run",
    )
    parser.add_argument("--max-runtime", type=float, help="Stop after this many seconds")
    args = parser.parse_args(argv)

    engine = Monitor() if args.engine == "monitor" else Formation()

    if args.replay:
        # No client is contacted: a replay engine that never calls
        # request() needs no server at all.
        with open(args.replay, encoding="utf-8", errors="replace") as handle:
            runner = EngineRunner(engine, client=None, watchdog=None)  # type: ignore[arg-type]
            try:
                runner.replay(handle, glider=args.gliders[0])
            finally:
                runner.close()
        return 0

    with SFMCClient(host=args.host) as client:
        runner = EngineRunner(engine, client, gliders=args.gliders)
        if args.max_runtime:
            import threading

            threading.Timer(args.max_runtime, runner.stop).start()
        try:
            runner.run()
        except KeyboardInterrupt:
            runner.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
