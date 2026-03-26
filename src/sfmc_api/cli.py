"""Command-line interface for the SFMC REST API.

Provides the ``sfmc`` command with subcommands for every API operation::

    sfmc get-glider-details osusim
    sfmc get-waypoint-plan osusim
    sfmc subscribe-connection-events osusim
    sfmc --compact get-glider-details osusim
    sfmc --config /path/to/creds.json auth

Run ``sfmc --help`` for the full list of subcommands.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from . import __version__
from .client import SFMCClient
from .exceptions import SFMCError

__all__ = ["build_parser", "main"]

# ── Command categories ───────────────────────────────────────────────

# Commands that take only GLIDER_NAME and return JSON.
_GLIDER_ONLY: dict[str, str] = {
    "get-glider-details": "Retrieve glider details",
    "get-active-deployment-details": "Get active deployment details",
    "get-newest-mission-status": "Get newest mission status",
    "get-available-scripts": "List available scripts for a glider",
    "get-mission-plan": "Get the assigned mission plan",
    "get-waypoint-plan": "Get the assigned waypoint plan",
    "get-yo-plan": "Get the assigned yo (dive/climb) plan",
    "get-surface-plan": "Get the assigned surface plan",
    "get-sampling-plan": "Get the assigned sampling plan",
    "get-data-transmission-plan": "Get the assigned data transmission plan",
    "get-mission-sensor-plan": "Get the assigned mission sensor plan",
    "get-abort-plan": "Get the assigned abort plan",
    "delete-hit-waypoint-surface-plan-rule": "Delete hit-waypoint surface plan rule",
    "delete-every-secs-surface-plan-rules": "Delete every-N-secs surface plan rules",
    "delete-at-utc-time-surface-plan-rules": "Delete at-UTC-time surface plan rules",
    "delete-sampling-plan-rules": "Delete sampling plan rules",
    "obtain-or-create-active-deployment": "Get or create the active deployment",
    "clear-assigned-script": "Clear the assigned script",
    "pause-assigned-script": "Pause the assigned script",
    "resume-assigned-script": "Resume the assigned script",
    "rewind-assigned-script": "Rewind the assigned script",
    "deploy-goto-file": "Generate and deploy goto file",
    "deploy-yo-file": "Generate and deploy yo file",
    "deploy-surface-files": "Generate and deploy surface files",
    "deploy-sample-files": "Generate and deploy sample files",
    "deploy-sbd-list-file": "Generate and deploy SBD list file",
    "deploy-tbd-list-file": "Generate and deploy TBD list file",
}

# Commands that take GLIDER_NAME + FILE and return JSON.
_PLAN_UPLOAD: dict[str, str] = {
    "update-waypoint-plan": "Upload a new waypoint plan file",
    "update-yo-plan": "Upload a new yo plan file",
    "update-surface-plan": "Upload a new surface plan file",
    "update-sampling-plan": "Upload a new sampling plan file",
    "update-flight-data-transmission-plan": "Upload a new flight data transmission plan",
    "update-science-data-transmission-plan": "Upload a new science data transmission plan",
}

# Streaming commands that take GLIDER_NAME and run until Ctrl-C.
_STREAM: dict[str, str] = {
    "subscribe-connection-events": "Stream connection events",
    "subscribe-glider-output": "Stream glider dialog output",
    "subscribe-script-events": "Stream script assignment events",
    "subscribe-zmodem-transfer-events": "Stream Zmodem transfer events",
    "subscribe-deployment-events": "Stream deployment update events",
}


# ── Output helpers ───────────────────────────────────────────────────


def _print_json(data: object, compact: bool) -> None:
    """Print data as JSON to stdout."""
    if compact:
        print(json.dumps(data, separators=(",", ":")))
    else:
        print(json.dumps(data, indent=2))


# ── Parser builder ───────────────────────────────────────────────────


def _add_glider_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("glider_name", metavar="GLIDER_NAME", help="Registered glider name")


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="sfmc",
        description="CLI for the Slocum Fleet Management Center REST API",
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        default=None,
        metavar="PATH",
        help="Path to credentials JSON file (default: ~/.config/sfmc/credentials.json)",
    )
    parser.add_argument(
        "--host",
        default=None,
        metavar="HOSTNAME",
        help="SFMC server hostname (selects entry from multi-host credentials file)",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        default=False,
        help="Single-line JSON output instead of pretty-printed",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    # ── auth ─────────────────────────────────────────────────────
    sub.add_parser("auth", help="Test authentication credentials")

    # ── Category A: glider-name-only ─────────────────────────────
    for name, help_text in _GLIDER_ONLY.items():
        p = sub.add_parser(name, help=help_text)
        _add_glider_arg(p)

    # ── Category B: plan-upload ──────────────────────────────────
    for name, help_text in _PLAN_UPLOAD.items():
        p = sub.add_parser(name, help=help_text)
        _add_glider_arg(p)
        p.add_argument("file", metavar="FILE", type=Path, help="Plan file to upload")

    # ── Category D: streaming ────────────────────────────────────
    for name, help_text in _STREAM.items():
        p = sub.add_parser(name, help=help_text)
        _add_glider_arg(p)

    # ── Custom-arg commands ──────────────────────────────────────

    p = sub.add_parser("get-surface-sensor-samples", help="Get surface sensor samples")
    _add_glider_arg(p)
    p.add_argument("sensor_type", metavar="SENSOR_TYPE", help="Sensor type name")
    p.add_argument("--start", required=True, metavar="DATETIME", help="Start (yyyyMMddHHmm)")
    p.add_argument("--end", required=True, metavar="DATETIME", help="End (yyyyMMddHHmm)")

    p = sub.add_parser("get-folder-file-listing", help="List files in a glider folder")
    _add_glider_arg(p)
    p.add_argument("folder", metavar="FOLDER", help="Folder name (e.g. from-glider)")
    p.add_argument("--page", type=int, default=0, help="Page number (default: 0)")
    p.add_argument(
        "--filter",
        default=None,
        metavar="PATTERN",
        help="Wildcard filter",
    )
    p.add_argument(
        "--last-modified-after",
        default=None,
        metavar="DATETIME",
        help="Filter by date (yyyyMMddHHmm)",
    )

    p = sub.add_parser("get-zmodem-transfers", help="Get Zmodem transfers")
    p.add_argument("connection_id", metavar="CONNECTION_ID", help="Connection ID")

    p = sub.add_parser("register-glider", help="Register a glider")
    _add_glider_arg(p)
    p.add_argument(
        "--group",
        default="default",
        metavar="GROUP",
        help="Group name",
    )

    p = sub.add_parser(
        "update-active-deployment-start",
        help="Update deployment start",
    )
    _add_glider_arg(p)
    p.add_argument("start_datetime", metavar="DATETIME", help="yyyyMMddHHmm")

    p = sub.add_parser("set-assigned-script", help="Assign a script")
    _add_glider_arg(p)
    p.add_argument("script_type", metavar="TYPE", help="Script type")
    p.add_argument("script_name", metavar="NAME", help="Script name")

    p = sub.add_parser("send-command", help="Send a command to a glider")
    _add_glider_arg(p)
    p.add_argument("command_str", metavar="COMMAND", help="Command string")

    p = sub.add_parser("upload-glider-files", help="Upload files to a folder")
    _add_glider_arg(p)
    p.add_argument("folder", metavar="FOLDER", help="Target folder")
    p.add_argument(
        "files",
        metavar="FILE",
        nargs="+",
        type=Path,
        help="Files to upload",
    )

    p = sub.add_parser("upload-cache-files", help="Upload cache files")
    p.add_argument("group_name", metavar="GROUP_NAME", help="Group name")
    p.add_argument(
        "files",
        metavar="FILE",
        nargs="+",
        type=Path,
        help="Files to upload",
    )

    p = sub.add_parser("download-glider-file", help="Download a single file")
    _add_glider_arg(p)
    p.add_argument("folder", metavar="FOLDER", help="Source folder")
    p.add_argument("file_name", metavar="FILE_NAME", help="File name")
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help="Output path (default: ./<FILE_NAME>)",
    )

    p = sub.add_parser("download-glider-files", help="Download files as zip")
    _add_glider_arg(p)
    p.add_argument("folder", metavar="FOLDER", help="Source folder")
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        required=True,
        metavar="PATH",
        help="Output zip path",
    )
    p.add_argument(
        "--filter",
        default=None,
        metavar="PATTERN",
        help="Wildcard filter",
    )
    p.add_argument(
        "--last-modified-after",
        default=None,
        metavar="DATETIME",
        help="Filter by date (yyyyMMddHHmm)",
    )

    p = sub.add_parser("delete-glider-file", help="Delete a file")
    _add_glider_arg(p)
    p.add_argument("folder", metavar="FOLDER", help="Target folder")
    p.add_argument("file_name", metavar="FILE_NAME", help="File to delete")

    return parser


# ── Dispatch ─────────────────────────────────────────────────────────


def _run(client: SFMCClient, args: argparse.Namespace) -> int:
    """Dispatch the parsed command to the appropriate handler."""
    cmd: str = args.command
    compact: bool = args.compact

    # Auth test
    if cmd == "auth":
        client.authenticate()
        _print_json({"status": "ok", "host": client._config.host}, compact)
        return 0

    # Derive method name from command name
    method_name = cmd.replace("-", "_")

    # Streaming commands
    if cmd in _STREAM:
        return _handle_stream(client, args, method_name, compact)

    # Download commands
    if cmd == "download-glider-file":
        output = args.output or Path(args.file_name)
        path = client.download_glider_file(args.glider_name, args.folder, args.file_name, output)
        _print_json({"downloaded": str(path)}, compact)
        return 0

    if cmd == "download-glider-files":
        path = client.download_glider_files(
            args.glider_name,
            args.folder,
            args.output,
            filter=args.filter,
            last_modified_after=getattr(args, "last_modified_after", None),
        )
        _print_json({"downloaded": str(path)}, compact)
        return 0

    # All remaining commands return dict → JSON
    result = _call_method(client, cmd, method_name, args)
    _print_json(result, compact)
    return 0


def _call_method(
    client: SFMCClient,
    cmd: str,
    method_name: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    """Call the appropriate SFMCClient method and return the result."""
    method: Any = getattr(client, method_name)

    # Category A: glider-name-only
    if cmd in _GLIDER_ONLY:
        return method(args.glider_name)  # type: ignore[no-any-return]

    # Category B: plan-upload
    if cmd in _PLAN_UPLOAD:
        return method(args.glider_name, args.file)  # type: ignore[no-any-return]

    # Custom commands
    if cmd == "get-surface-sensor-samples":
        return method(  # type: ignore[no-any-return]
            args.glider_name,
            args.sensor_type,
            start_datetime=args.start,
            end_datetime=args.end,
        )

    if cmd == "get-folder-file-listing":
        return method(  # type: ignore[no-any-return]
            args.glider_name,
            args.folder,
            page=args.page,
            filter=args.filter,
            last_modified_after=getattr(args, "last_modified_after", None),
        )

    if cmd == "get-zmodem-transfers":
        return method(args.connection_id)  # type: ignore[no-any-return]

    if cmd == "register-glider":
        return method(args.glider_name, args.group)  # type: ignore[no-any-return]

    if cmd == "update-active-deployment-start":
        return method(args.glider_name, args.start_datetime)  # type: ignore[no-any-return]

    if cmd == "set-assigned-script":
        return method(  # type: ignore[no-any-return]
            args.glider_name, args.script_type, args.script_name
        )

    if cmd == "send-command":
        return method(args.glider_name, args.command_str)  # type: ignore[no-any-return]

    if cmd == "upload-glider-files":
        return method(  # type: ignore[no-any-return]
            args.glider_name, args.folder, args.files
        )

    if cmd == "upload-cache-files":
        return method(args.group_name, args.files)  # type: ignore[no-any-return]

    if cmd == "delete-glider-file":
        return method(  # type: ignore[no-any-return]
            args.glider_name, args.folder, args.file_name
        )

    # Should not reach here if build_parser is complete
    raise SystemExit(f"Unknown command: {cmd}")


def _handle_stream(
    client: SFMCClient,
    args: argparse.Namespace,
    method_name: str,
    compact: bool,
) -> int:
    """Handle a streaming subscription command."""
    subscribe_method: Any = getattr(client, method_name)
    with client.open_stream() as stomp:
        sub = subscribe_method(args.glider_name, stomp)
        sys.stderr.write(
            f"Streaming {args.command} for {args.glider_name}. Press Ctrl-C to stop.\n"
        )
        try:
            for event in sub:
                _print_json(event, compact)
                sys.stdout.flush()
        except KeyboardInterrupt:
            sys.stderr.write("\nStopped.\n")
    return 0


# ── Entry point ──────────────────────────────────────────────────────


def main() -> None:
    """Entry point for the ``sfmc`` console script."""
    parser = build_parser()
    args = parser.parse_args()

    try:
        with SFMCClient(config_path=args.credentials, host=args.host) as client:
            code = _run(client, args)
    except SFMCError as exc:
        sys.stderr.write(f"Error: {exc}\n")
        code = 1
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted.\n")
        code = 130

    sys.exit(code)
