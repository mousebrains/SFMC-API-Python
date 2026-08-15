#!/usr/bin/env python3
"""Stream several topics, to several consumers, across reconnects — the
session parallel to ``stream_glider_events.py``.

Usage::

    python stream_glider_events_session.py <glider-name> [seconds]

Press Ctrl-C to stop.  Loads credentials from
``~/.config/sfmc/credentials.json`` by default.

What this shows
---------------

``stream_glider_events.py`` opens a stream, subscribes to one topic, and
iterates it.  That is the right tool for a single consumer watching a
single topic for a short while, and nothing here replaces it.  It has
two limits worth knowing:

* **One subscription feeds one consumer.**  A ``StompSubscription`` is a
  queue; whoever calls ``get()`` takes the message, and nobody else sees
  it.  Watching the same dialog from two places means subscribing twice,
  which duplicates the server traffic *and* runs the sequence-reordering
  buffer twice.
* **It dies with the connection.**  When the WebSocket drops — and over
  a multi-week deployment it will — the iterator ends.  Reconnecting is
  the caller's problem.

A :class:`~sfmc_api.session.GliderSession` subscribes once per topic,
runs ordering and line reassembly once, and fans the result out to as
many listeners and callbacks as you register, reconnecting underneath
with backoff.  This example registers **four** consumers of two topics
over a single connection:

1. a callback that prints dialog lines as they arrive;
2. a callback that counts them;
3. a queue listener that a slow consumer reads at its own pace;
4. a connection-event listener watching the glider surface and dive.

See ``docs/streaming.md``.
"""

import json
import sys
import threading
import time
from queue import Empty

from sfmc_api import DialogLine, SFMCClient, SFMCError


def main() -> None:
    if not 2 <= len(sys.argv) <= 3:
        print(f"Usage: {sys.argv[0]} <glider-name> [seconds]")
        sys.exit(1)

    glider_name = sys.argv[1]
    run_for = float(sys.argv[2]) if len(sys.argv) == 3 else 300.0

    counts = {"lines": 0}

    try:
        with SFMCClient() as client:
            print(f"Opening session for {glider_name}...")

            # Subscribe only to what is consumed: each topic costs a
            # subscription, and zmodem/deployment cost an extra lookup.
            with client.session(glider_name, topics=("dialog", "connections")) as session:
                print(f"Subscribed (epoch={session.epoch}).  Ctrl-C to stop.\n")

                # ── Consumer 1: print every dialog line ──────────────
                # Callbacks run on the pump thread, so keep them cheap.
                # Anything slow belongs on a listener (consumer 3).
                def show(line: DialogLine) -> None:
                    stamp = time.strftime("%H:%M:%S", time.localtime(line.first_seen))
                    print(f"  {stamp} | {line.text}")

                session.on_line(show)

                # ── Consumer 2: count them ──────────────────────────
                # A second callback on the same stream, entirely
                # independent of the first.
                def tally(_line: DialogLine) -> None:
                    counts["lines"] += 1

                session.on_line(tally)

                # ── Consumer 3: a queue for slower work ─────────────
                # Listener queues are bounded and drop their *oldest*
                # entry when a consumer falls behind, counting the loss
                # in .dropped — so a gap is detectable rather than a
                # silently short transcript.
                listener = session.dialog_listener(maxsize=512)

                def slow_consumer() -> None:
                    while True:
                        try:
                            line = listener.get(timeout=1.0)
                        except Empty:
                            continue
                        if line is None:  # session closed
                            return
                        del line  # pretend this took real work
                        time.sleep(0.01)

                threading.Thread(target=slow_consumer, daemon=True).start()

                # ── Consumer 4: connection events ───────────────────
                # The same information stream_glider_events.py shows,
                # here as one consumer among several.
                connections = session.listen("connections")

                def watch_connections() -> None:
                    while True:
                        try:
                            event = connections.get(timeout=1.0)
                        except Empty:
                            continue
                        if event is None:
                            return
                        for item in event if isinstance(event, list) else [event]:
                            if isinstance(item, dict):
                                status = "CONNECTED" if item.get("active") else "DISCONNECTED"
                                print(f"  *** {status}: {json.dumps(item)}")

                threading.Thread(target=watch_connections, daemon=True).start()

                # ── Run ─────────────────────────────────────────────
                # The session reconnects on its own, so a drop shows up
                # as a change of epoch rather than as the end of the
                # stream.  Registered consumers stay valid across it.
                deadline = time.monotonic() + run_for
                epoch = session.epoch
                try:
                    while time.monotonic() < deadline:
                        time.sleep(1.0)
                        if session.epoch != epoch:
                            print(
                                f"  --- stream reconnected "
                                f"(epoch {epoch} -> {session.epoch}); "
                                f"data may be missing across the gap"
                            )
                            epoch = session.epoch
                except KeyboardInterrupt:
                    print("\nStopping.")

                print(
                    f"\n{counts['lines']} dialog line(s); "
                    f"{listener.dropped} dropped by the slow consumer"
                )
            # Leaving the `with` closes the session and releases every
            # listener, so the consumer threads above exit on their own.

    except SFMCError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
