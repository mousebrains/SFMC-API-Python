#!/usr/bin/env python3
"""Stream real-time connection events for an SFMC glider.

Usage::

    python stream_glider_events.py <glider-name>

Press Ctrl-C to stop.

Loads credentials from ``~/.config/sfmc/credentials.json`` by default.
"""

import json
import sys

from sfmc_api import SFMCClient, SFMCError


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <glider-name>")
        sys.exit(1)

    glider_name = sys.argv[1]

    try:
        with SFMCClient() as client:
            print(f"Connecting to STOMP for {glider_name}...")
            with client.open_stream() as stomp:
                sub = client.subscribe_connection_events(glider_name, stomp)
                print("Subscribed to connection events. Waiting for events...\n")

                try:
                    for event in sub:
                        print(json.dumps(event, indent=2))
                        print()
                except KeyboardInterrupt:
                    print("\nStopping.")
    except SFMCError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
