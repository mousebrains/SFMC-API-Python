#!/usr/bin/env python3
"""Retrieve and display details for an SFMC-registered glider.

Usage::

    python get_glider_details.py <glider-name>

Loads credentials from ``~/.config/sfmc/credentials.json`` by default.
"""

import json
import sys

from sfmc_api import SFMCClient


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <glider-name>")
        sys.exit(1)

    glider_name = sys.argv[1]

    with SFMCClient() as client:
        details = client.get_glider_details(glider_name)
        print(json.dumps(details, indent=2))


if __name__ == "__main__":
    main()
