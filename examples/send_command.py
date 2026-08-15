#!/usr/bin/env python3
"""Send commands to a glider and read the replies.

Run with a glider name::

    python examples/send_command.py osu685

What this demonstrates
----------------------

1. A blocking send that captures the reply.
2. The same send asynchronously, as a ``Future``.
3. Running any other API call off-thread through the same executor.

The important thing this example shows is how to *read the result
honestly*.  SFMC accepting a command is not the glider running it: a
Slocum is underwater most of the time, and a command submitted then
sits queued until the next surfacing.  So every reply says whether it
completed, why it stopped, and whether it could be correlated to the
command at all.  Check ``reply.complete`` before trusting the text.
"""

from __future__ import annotations

import sys

from sfmc_api import SFMCClient


def describe(reply: object) -> str:
    """Render a CommandReply for a human, including the caveats."""
    from sfmc_api import CommandReply

    assert isinstance(reply, CommandReply)
    if reply.complete:
        header = f"reply ({reply.reason}, {len(reply.lines)} line(s))"
        if not reply.correlated:
            header += " [uncorrelated: shared terminal]"
        if reply.dropped_lines:
            header += f" [WARNING: {reply.dropped_lines} line(s) dropped]"
        return f"{header}\n" + "\n".join(f"    {line}" for line in reply.lines)

    link = {True: "up", False: "down", None: "unchecked"}[reply.glider_connected]
    return f"no complete reply ({reply.reason}); glider link is {link}"


def main() -> int:
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} GLIDER_NAME", file=sys.stderr)
        return 2
    glider = sys.argv[1]

    # The channel subscribes to the dialog stream first, so a fast
    # reply cannot arrive before we are listening.
    with SFMCClient() as client, client.command_channel(glider) as chan:
        # ── 1. Blocking send with reply capture ──────────────────────
        print("$ sensor m_battery")
        print(describe(chan.send("sensor m_battery", timeout=45, quiet=5)))

        # ── 2. The same thing, asynchronously ────────────────────────
        # send_async returns a concurrent.futures.Future, so it
        # composes with callbacks, with other futures, and — via
        # asyncio.wrap_future — with async code.
        future = chan.send_async("sensor m_depth")
        print("\n(submitted m_depth; doing other work meanwhile)")

        # ── 3. Any client method, off-thread ─────────────────────────
        # No per-endpoint async wrapper exists or is needed.
        with client.operations() as ops:
            details = ops.submit(client.get_glider_details, glider)
            print(f"glider state: {details.result(timeout=30)['data']['state']}")

        print("\n$ sensor m_depth")
        print(describe(future.result(timeout=60)))

        # ── 4. Fire and forget ───────────────────────────────────────
        # No reply capture: use when the command has no output worth
        # waiting for.  Still serialized against captures on this
        # glider, so it cannot pollute one in progress.
        # chan.send_nowait("put c_science_on 0")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
