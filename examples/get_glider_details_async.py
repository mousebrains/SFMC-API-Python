#!/usr/bin/env python3
"""Retrieve glider details without blocking — the async parallel to
``get_glider_details.py``.

Usage::

    python get_glider_details_async.py <glider-name> [<glider-name> ...]

Loads credentials from ``~/.config/sfmc/credentials.json`` by default.

What this shows
---------------

``get_glider_details.py`` calls the client directly and blocks until the
server answers.  That is the right default, and nothing here replaces
it.  This version submits the same call to an executor instead, which
hands back a :class:`concurrent.futures.Future`.

Three things follow from that, and each is shown below:

1. **Several gliders at once.**  Latency is per-request, so asking about
   four gliders takes about as long as asking about one.
2. **Do other work while it runs.**  The future is a handle; you decide
   when to wait.
3. **Be told instead of waiting.**  ``add_done_callback`` turns the call
   event-driven.

The executor runs *existing* client methods — there is no separate async
API to learn, and no per-endpoint wrapper, so anything on ``SFMCClient``
works the same way.  See ``docs/async_operations.md``.
"""

import json
import sys
from concurrent.futures import Future
from typing import Any

from sfmc_api import SFMCClient, SFMCError


def main() -> None:
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <glider-name> [<glider-name> ...]")
        sys.exit(1)

    glider_names = sys.argv[1:]

    try:
        # Keep the pool small: SFMC rate-limits, and these calls are
        # latency-bound rather than throughput-bound.
        with SFMCClient() as client, client.operations(max_workers=4) as ops:
            # ── 1. All gliders at once ───────────────────────────────
            # Submitting returns immediately; the requests are already
            # in flight by the time this loop ends.
            futures: dict[str, Future[dict[str, Any]]] = {
                name: ops.submit(client.get_glider_details, name) for name in glider_names
            }

            # ── 2. Other work happens here ───────────────────────────
            print(f"submitted {len(futures)} request(s); collecting...\n")

            for name, future in futures.items():
                try:
                    # A failed request raises here, from result() —
                    # not at submit() time.
                    details = future.result(timeout=60)
                except SFMCError as exc:
                    # One glider failing must not lose the others.
                    print(f"{name}: error: {exc}\n")
                    continue
                print(f"{name}:")
                print(json.dumps(details, indent=2))
                print()

            # ── 3. Event-driven instead of waiting ───────────────────
            # The callback runs on the worker thread as soon as the
            # response lands, so nothing here blocks on it.
            def report(future: Future[dict[str, Any]]) -> None:
                if future.exception() is not None:
                    print(f"callback: request failed: {future.exception()}")
                    return
                state = future.result().get("data", {}).get("state", "?")
                print(f"callback: {glider_names[0]} is {state}")

            ops.submit(client.get_glider_details, glider_names[0]).add_done_callback(report)

            # Leaving the `with` block shuts the pool down, which waits
            # for that callback to run.

        # From asyncio, the same future is awaited directly — no async
        # client and no second protocol stack are involved:
        #
        #     import asyncio
        #     details = await asyncio.wrap_future(
        #         ops.submit(client.get_glider_details, "osu685")
        #     )

    except SFMCError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
