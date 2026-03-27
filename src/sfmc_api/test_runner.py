"""Integration test runner for the SFMC REST API via the ``sfmc-api`` CLI.

Installed as the ``sfmc-api-test`` console script.  Exercises as many
API endpoints as possible against a live SFMC server *without*
registering a new glider.

Usage::

    sfmc-api-test --host gliderfmc1.ceoas.oregonstate.edu --glider shoebox

The script creates temporary files for upload/plan tests, cleans them
up from the server when done, and prints a pass/fail/warn summary.

Results:
  PASS — command succeeded (exit 0)
  WARN — command reached the server but got HTTP 412 (precondition
         failed) or similar expected server rejection
  FAIL — client crash, unexpected error, or persistent rate limiting
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# ── Colour helpers ────────────────────────────────────────────────────

_USE_COLOUR = sys.stdout.isatty()


def _green(text: str) -> str:
    return f"\033[32m{text}\033[0m" if _USE_COLOUR else text


def _red(text: str) -> str:
    return f"\033[31m{text}\033[0m" if _USE_COLOUR else text


def _yellow(text: str) -> str:
    return f"\033[33m{text}\033[0m" if _USE_COLOUR else text


def _bold(text: str) -> str:
    return f"\033[1m{text}\033[0m" if _USE_COLOUR else text


# ── Result tracking ──────────────────────────────────────────────────

# status is one of "pass", "warn", "fail"
_results: list[tuple[str, str, str]] = []  # (label, status, detail)

_MAX_RETRIES = 5
_MAX_RETRY_WAIT = 60  # seconds — give up if server says wait longer

# HTTP status codes that count as WARN (server reached, precondition not met)
_WARN_CODES = {412}


def _sfmc(
    args: list[str],
    *,
    host: str,
    credentials: str | None = None,
    label: str | None = None,
    expect_fail: bool = False,
    warn_on_codes: set[int] | None = None,
) -> tuple[str, str, str]:
    """Run an ``sfmc-api`` CLI command and return (status, stdout, stderr).

    Automatically retries on rate-limit (429) errors, sleeping for the
    duration indicated by the server.

    Args:
        args: Sub-command and arguments (e.g. ``["auth"]``).
        host: SFMC hostname.
        credentials: Optional path to credentials file.
        label: Human-readable label for progress output.
        expect_fail: If True, a non-zero exit code counts as pass.
        warn_on_codes: HTTP status codes to treat as WARN instead of
            FAIL.  Defaults to ``{412}``.

    Returns:
        ``(status, stdout, stderr)`` where status is "pass", "warn",
        or "fail".
    """
    if warn_on_codes is None:
        warn_on_codes = _WARN_CODES

    cmd = ["sfmc-api", "--host", host]
    if credentials:
        cmd.extend(["--credentials", credentials])
    cmd.extend(args)

    tag = label or " ".join(args[:2])
    sys.stdout.write(f"  {tag} ... ")
    sys.stdout.flush()

    for attempt in range(_MAX_RETRIES):
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        # Check for rate-limit error and retry
        if result.returncode != 0 and "Rate limited" in result.stderr:
            delay = _parse_retry_delay(result.stderr)
            if delay > _MAX_RETRY_WAIT:
                break  # give up — server is throttling hard
            if attempt < _MAX_RETRIES - 1:
                sys.stdout.write(_yellow(f"rate-limited, retry in {delay:.0f}s ... "))
                sys.stdout.flush()
                time.sleep(delay)
                continue
        break

    # Determine outcome
    stderr = result.stderr.strip()

    if expect_fail:
        status = "pass" if result.returncode != 0 else "fail"
    elif result.returncode == 0:
        status = "pass"
    elif _is_http_error(stderr, warn_on_codes):
        status = "warn"
    else:
        status = "fail"

    # Print result
    if status == "pass":
        sys.stdout.write(_green("PASS") + "\n")
    elif status == "warn":
        reason = _extract_error_line(stderr)
        sys.stdout.write(_yellow("WARN") + f"  {reason}\n")
    else:
        sys.stdout.write(_red("FAIL") + "\n")
        if stderr:
            # Show concise output — just the last meaningful line for tracebacks
            if "Traceback" in stderr:
                lines = stderr.splitlines()
                # Show the final exception line
                for line in reversed(lines):
                    if line and not line.startswith(" "):
                        sys.stdout.write(f"    {_red(line)}\n")
                        break
            else:
                for line in stderr.splitlines():
                    sys.stdout.write(f"    {_red(line)}\n")

    detail = result.stdout.strip() or stderr
    _results.append((tag, status, detail))
    return status, result.stdout, result.stderr


def _parse_retry_delay(stderr: str) -> float:
    """Extract retry delay in seconds from rate-limit error message."""
    m = re.search(r"Retry after ([\d.]+)s", stderr)
    if m:
        return max(float(m.group(1)), 1.0)
    return 3.0


def _is_http_error(stderr: str, codes: set[int]) -> bool:
    """Check if stderr contains an SFMC API HTTP error with a given code."""
    m = re.search(r"HTTP (\d+)", stderr)
    return m is not None and int(m.group(1)) in codes


def _extract_error_line(stderr: str) -> str:
    """Pull out the one-line error message."""
    for line in stderr.splitlines():
        if line.startswith("Error:"):
            return line
    return stderr.splitlines()[-1] if stderr else ""


# ── Test groups ───────────────────────────────────────────────────────


def _test_auth(host: str, creds: str | None) -> bool:
    """Test authentication."""
    status, stdout, _ = _sfmc(["auth"], host=host, credentials=creds, label="auth")
    if status == "pass" and stdout.strip():
        data = json.loads(stdout)
        if data.get("status") != "ok":
            return False
    return status == "pass"


def _test_glider_queries(host: str, glider: str, creds: str | None) -> None:
    """Test all read-only glider queries."""
    commands = [
        "get-glider-details",
        "get-active-deployment-details",
        "get-newest-mission-status",
        "get-available-scripts",
        "get-mission-plan",
        "get-waypoint-plan",
        "get-yo-plan",
        "get-surface-plan",
        "get-sampling-plan",
        "get-data-transmission-plan",
        "get-mission-sensor-plan",
        "get-abort-plan",
    ]
    for cmd in commands:
        _sfmc([cmd, glider], host=host, credentials=creds, label=cmd)


def _test_deployment(host: str, glider: str, creds: str | None) -> None:
    """Test obtain-or-create-active-deployment."""
    _sfmc(
        ["obtain-or-create-active-deployment", glider],
        host=host,
        credentials=creds,
        label="obtain-or-create-active-deployment",
    )


def _test_folder_file_listing(host: str, glider: str, creds: str | None) -> None:
    """Test folder file listing with various options."""
    folders = ["to-glider", "to-science", "from-glider", "configuration"]
    for folder in folders:
        _sfmc(
            ["get-folder-file-listing", glider, folder],
            host=host,
            credentials=creds,
            label=f"get-folder-file-listing {folder}",
        )

    # With filter
    _sfmc(
        ["get-folder-file-listing", glider, "from-glider", "--filter", "*.log"],
        host=host,
        credentials=creds,
        label="get-folder-file-listing --filter",
    )

    # With page
    _sfmc(
        ["get-folder-file-listing", glider, "from-glider", "--page", "0"],
        host=host,
        credentials=creds,
        label="get-folder-file-listing --page",
    )


def _test_file_upload_download_delete(
    host: str, glider: str, creds: str | None, tmpdir: Path
) -> None:
    """Test file upload, listing, download, and delete cycle."""
    # Create test files
    test_file = tmpdir / "sfmc-api-test-file.txt"
    test_file.write_text("sfmc-api integration test file\n")

    test_file2 = tmpdir / "sfmc-api-test-file2.txt"
    test_file2.write_text("sfmc-api integration test file 2\n")

    # Upload to to-glider
    _sfmc(
        ["upload-glider-files", glider, "to-glider", str(test_file), str(test_file2)],
        host=host,
        credentials=creds,
        label="upload-glider-files to-glider (2 files)",
    )

    # List to-glider to verify upload
    _sfmc(
        ["get-folder-file-listing", glider, "to-glider", "--filter", "sfmc-api-test*"],
        host=host,
        credentials=creds,
        label="verify upload via listing",
    )

    # Download single file
    download_dest = tmpdir / "downloaded"
    download_dest.mkdir(exist_ok=True)
    _sfmc(
        [
            "download-glider-file",
            glider,
            "to-glider",
            "sfmc-api-test-file.txt",
            "-o",
            str(download_dest / "sfmc-api-test-file.txt"),
        ],
        host=host,
        credentials=creds,
        label="download-glider-file (single)",
    )

    # Download multiple as zip
    _sfmc(
        [
            "download-glider-files",
            glider,
            "to-glider",
            "--filter",
            "sfmc-api-test*",
            "-o",
            str(download_dest / "test-files.zip"),
        ],
        host=host,
        credentials=creds,
        label="download-glider-files (zip)",
    )

    # Delete the uploaded files
    _sfmc(
        ["delete-glider-file", glider, "to-glider", "sfmc-api-test-file.txt"],
        host=host,
        credentials=creds,
        label="delete-glider-file (file 1)",
    )
    _sfmc(
        ["delete-glider-file", glider, "to-glider", "sfmc-api-test-file2.txt"],
        host=host,
        credentials=creds,
        label="delete-glider-file (file 2)",
    )


def _test_upload_to_science(host: str, glider: str, creds: str | None, tmpdir: Path) -> None:
    """Test upload and delete in the to-science folder."""
    test_file = tmpdir / "sfmc-api-test-sci.txt"
    test_file.write_text("science test file\n")

    _sfmc(
        ["upload-glider-files", glider, "to-science", str(test_file)],
        host=host,
        credentials=creds,
        label="upload-glider-files to-science",
    )

    _sfmc(
        ["delete-glider-file", glider, "to-science", "sfmc-api-test-sci.txt"],
        host=host,
        credentials=creds,
        label="delete-glider-file to-science",
    )


def _find_ma_file(ma_dir: Path, prefix: str) -> Path | None:
    """Find the highest-numbered .ma file matching a prefix.

    When multiple files match (e.g. ``yo30.ma``, ``yo90.ma``), the last
    in sorted order is returned — typically the newest/highest variant.
    """
    candidates = sorted(ma_dir.glob(f"{prefix}*.ma"))
    return candidates[-1] if candidates else None


def _test_plan_updates(
    host: str, glider: str, creds: str | None, tmpdir: Path, ma_dir: Path | None
) -> None:
    """Test plan upload commands with real Slocum .ma plan files.

    If *ma_dir* is provided and contains the expected files, those are
    used directly.  Otherwise the tests are skipped with a warning.
    """
    if ma_dir is None or not ma_dir.is_dir():
        sys.stdout.write(_yellow("    (no --ma-files directory — skipping plan updates)\n"))
        for label in [
            "update-waypoint-plan",
            "update-yo-plan",
            "update-surface-plan",
            "update-sampling-plan",
        ]:
            _results.append((label, "warn", "skipped: no .ma files provided"))
        return

    # Map: (test label, file prefix, cli command)
    plan_tests = [
        ("update-waypoint-plan", "goto_l", "update-waypoint-plan"),
        ("update-yo-plan", "yo", "update-yo-plan"),
        ("update-surface-plan", "surfac", "update-surface-plan"),
        ("update-sampling-plan", "sample", "update-sampling-plan"),
    ]

    for label, prefix, cmd in plan_tests:
        ma_file = _find_ma_file(ma_dir, prefix)
        if ma_file:
            _sfmc(
                [cmd, glider, str(ma_file)],
                host=host,
                credentials=creds,
                label=f"{label} ({ma_file.name})",
            )
        else:
            sys.stdout.write(_yellow(f"  {label} ... WARN  no {prefix}*.ma file found\n"))
            _results.append((label, "warn", f"skipped: no {prefix}*.ma in {ma_dir}"))

    # SBD list (flight data transmission plan) — simple text format
    sbd_file = tmpdir / "sbdlist.dat"
    sbd_file.write_text("# SBD list for testing\nm_depth\nm_gps_lat\nm_gps_lon\n")
    _sfmc(
        ["update-flight-data-transmission-plan", glider, str(sbd_file)],
        host=host,
        credentials=creds,
        label="update-flight-data-transmission-plan",
    )

    # TBD list (science data transmission plan) — simple text format
    tbd_file = tmpdir / "tbdlist.dat"
    tbd_file.write_text("# TBD list for testing\nsci_water_temp\nsci_water_cond\n")
    _sfmc(
        ["update-science-data-transmission-plan", glider, str(tbd_file)],
        host=host,
        credentials=creds,
        label="update-science-data-transmission-plan",
    )


def _test_deploy_files(host: str, glider: str, creds: str | None) -> None:
    """Test generate-and-deploy commands."""
    deploy_commands = [
        "deploy-goto-file",
        "deploy-yo-file",
        "deploy-surface-files",
        "deploy-sample-files",
        "deploy-sbd-list-file",
        "deploy-tbd-list-file",
    ]
    for cmd in deploy_commands:
        _sfmc([cmd, glider], host=host, credentials=creds, label=cmd)


def _test_script_control(host: str, glider: str, creds: str | None) -> None:
    """Test script control commands.

    First gets available scripts to find a real script name, then
    exercises the set/pause/resume/rewind/clear cycle.
    """
    # Get available scripts
    status, stdout, _ = _sfmc(
        ["get-available-scripts", glider],
        host=host,
        credentials=creds,
        label="get-available-scripts (for script control)",
    )

    script_type = None
    script_name = None
    if status == "pass" and stdout.strip():
        try:
            data = json.loads(stdout)
            # Try to find any script we can use
            if isinstance(data, dict):
                for stype, scripts in data.items():
                    if isinstance(scripts, list) and scripts:
                        script_type = stype
                        script_name = scripts[0] if isinstance(scripts[0], str) else None
                        break
                    elif isinstance(scripts, dict):
                        for sname in scripts:
                            script_type = stype
                            script_name = sname
                            break
                        if script_name:
                            break
            elif isinstance(data, list) and data:
                # Flat list of script objects
                for entry in data:
                    if isinstance(entry, dict):
                        script_type = entry.get("scriptType") or entry.get("type")
                        script_name = entry.get("scriptName") or entry.get("name")
                        if script_type and script_name:
                            break
        except json.JSONDecodeError:
            pass

    if script_type and script_name:
        sys.stdout.write(f"    (using script: type={script_type!r}, name={script_name!r})\n")

        _sfmc(
            ["set-assigned-script", glider, script_type, script_name],
            host=host,
            credentials=creds,
            label="set-assigned-script",
        )

        # Small delay to let the server process the assignment
        time.sleep(0.5)

        _sfmc(
            ["pause-assigned-script", glider],
            host=host,
            credentials=creds,
            label="pause-assigned-script",
        )

        _sfmc(
            ["resume-assigned-script", glider],
            host=host,
            credentials=creds,
            label="resume-assigned-script",
        )

        _sfmc(
            ["rewind-assigned-script", glider],
            host=host,
            credentials=creds,
            label="rewind-assigned-script",
        )

        _sfmc(
            ["clear-assigned-script", glider],
            host=host,
            credentials=creds,
            label="clear-assigned-script",
        )
    else:
        sys.stdout.write(
            _yellow("    (no scripts found — skipping set/pause/resume/rewind/clear)\n")
        )
        for cmd in [
            "set-assigned-script",
            "pause-assigned-script",
            "resume-assigned-script",
            "rewind-assigned-script",
            "clear-assigned-script",
        ]:
            _results.append((cmd, "warn", "skipped: no scripts available"))


def _test_send_command(host: str, glider: str, creds: str | None) -> None:
    """Test sending a command to the glider."""
    _sfmc(
        ["send-command", glider, "status"],
        host=host,
        credentials=creds,
        label="send-command",
    )


def _test_surface_sensor_samples(host: str, glider: str, creds: str | None) -> None:
    """Test surface sensor samples query."""
    # Use a broad time range — even if no data, a 200 response is a pass
    _sfmc(
        [
            "get-surface-sensor-samples",
            glider,
            "m_gps_lat",
            "--start",
            "202501010000",
            "--end",
            "202512312359",
        ],
        host=host,
        credentials=creds,
        label="get-surface-sensor-samples",
    )


def _test_delete_plan_rules(host: str, glider: str, creds: str | None) -> None:
    """Test plan rule deletion commands.

    HTTP 412 is expected when no rules exist to delete — reported as
    WARN, not FAIL.
    """
    commands = [
        "delete-hit-waypoint-surface-plan-rule",
        "delete-every-secs-surface-plan-rules",
        "delete-at-utc-time-surface-plan-rules",
        "delete-sampling-plan-rules",
    ]
    for cmd in commands:
        _sfmc([cmd, glider], host=host, credentials=creds, label=cmd)


# ── Main ──────────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sfmc-api-test",
        description="Integration test runner for the SFMC REST API via the sfmc-api CLI.",
        epilog=(
            "Requires a credentials file at ~/.config/sfmc/credentials.json.\n"
            "Create one with: sfmc-api init\n"
            "Add a host with:  sfmc-api add-host"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--host",
        required=True,
        metavar="HOSTNAME",
        help="SFMC server hostname (must be in credentials file)",
    )
    parser.add_argument(
        "--glider",
        required=True,
        metavar="NAME",
        help="Glider name to test with (must already be registered)",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        metavar="PATH",
        help="Path to credentials file (default: ~/.config/sfmc/credentials.json)",
    )
    parser.add_argument(
        "--ma-files",
        default=None,
        type=Path,
        metavar="DIR",
        help="Directory containing Slocum .ma plan files for plan update tests "
        "(goto_l*.ma, yo*.ma, surfac*.ma, sample*.ma)",
    )
    parser.add_argument(
        "--skip",
        nargs="*",
        default=[],
        metavar="GROUP",
        help="Test groups to skip (auth, queries, deployment, listing, files, "
        "plans, deploy, scripts, command, sensors, delete-rules)",
    )
    return parser


def main() -> None:
    """Entry point for the ``sfmc-api-test`` console script."""
    parser = _build_parser()
    args = parser.parse_args()

    host: str = args.host
    glider: str = args.glider
    creds: str | None = args.credentials
    ma_dir: Path | None = args.ma_files
    skip: set[str] = set(args.skip or [])

    # Check that sfmc-api CLI is available
    try:
        subprocess.run(
            ["sfmc-api", "--version"], capture_output=True, text=True, check=True, timeout=10
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        sys.stderr.write("Error: 'sfmc-api' command not found.\nInstall with: pip install -e .\n")
        sys.exit(1)

    # Check credentials file exists
    creds_path = Path(creds) if creds else Path.home() / ".config" / "sfmc" / "credentials.json"
    if not creds_path.exists():
        sys.stderr.write(
            f"Error: Credentials file not found at {creds_path}\n\n"
            "Create one with:\n"
            "  sfmc-api init\n\n"
        )
        sys.exit(1)

    # Check host is in credentials
    try:
        data = json.loads(creds_path.read_text(encoding="utf-8"))
        if host not in data:
            sys.stderr.write(
                f"Error: Host '{host}' not found in {creds_path}\n\n"
                f"Available hosts: {', '.join(data.keys())}\n\n"
                "Add it with:\n"
                "  sfmc-api add-host\n\n"
            )
            sys.exit(1)
    except (json.JSONDecodeError, OSError) as exc:
        sys.stderr.write(f"Error reading {creds_path}: {exc}\n")
        sys.exit(1)

    print(_bold("\nSFMC API Integration Tests"))
    print(f"  Host:   {host}")
    print(f"  Glider: {glider}")
    if ma_dir:
        print(f"  MA dir: {ma_dir}")
    if skip:
        print(f"  Skip:   {', '.join(sorted(skip))}")
    print()

    with tempfile.TemporaryDirectory(prefix="sfmc-api-test-") as tmpdir:
        tmp = Path(tmpdir)

        # ── 1. Authentication ──
        if "auth" not in skip:
            print(_bold("Authentication"))
            if not _test_auth(host, creds):
                sys.stderr.write("\nAuthentication failed — cannot continue.\n")
                sys.exit(1)
            print()

        # ── 2. Read-only glider queries ──
        if "queries" not in skip:
            print(_bold("Glider Queries (read-only)"))
            _test_glider_queries(host, glider, creds)
            print()

        # ── 3. Deployment ──
        if "deployment" not in skip:
            print(_bold("Deployment"))
            _test_deployment(host, glider, creds)
            print()

        # ── 4. Folder file listing ──
        if "listing" not in skip:
            print(_bold("Folder File Listing"))
            _test_folder_file_listing(host, glider, creds)
            print()

        # ── 5. Surface sensor samples ──
        if "sensors" not in skip:
            print(_bold("Surface Sensor Samples"))
            _test_surface_sensor_samples(host, glider, creds)
            print()

        # ── 6. File upload/download/delete cycle ──
        if "files" not in skip:
            print(_bold("File Upload / Download / Delete"))
            _test_file_upload_download_delete(host, glider, creds, tmp)
            _test_upload_to_science(host, glider, creds, tmp)
            print()

        # ── 7. Plan updates ──
        if "plans" not in skip:
            print(_bold("Plan Updates"))
            _test_plan_updates(host, glider, creds, tmp, ma_dir)
            print()

        # ── 8. Generate and deploy files ──
        if "deploy" not in skip:
            print(_bold("Generate & Deploy Files"))
            _test_deploy_files(host, glider, creds)
            print()

        # ── 9. Delete plan rules ──
        if "delete-rules" not in skip:
            print(_bold("Delete Plan Rules"))
            _test_delete_plan_rules(host, glider, creds)
            print()

        # ── 10. Script control ──
        if "scripts" not in skip:
            print(_bold("Script Control"))
            _test_script_control(host, glider, creds)
            print()

        # ── 11. Send command ──
        if "command" not in skip:
            print(_bold("Send Command"))
            _test_send_command(host, glider, creds)
            print()

    # ── Summary ───────────────────────────────────────────────────
    passed = sum(1 for _, s, _ in _results if s == "pass")
    warned = sum(1 for _, s, _ in _results if s == "warn")
    failed = sum(1 for _, s, _ in _results if s == "fail")
    total = len(_results)

    print(_bold("=" * 60))
    print(_bold("Summary"))
    print(f"  Total:  {total}")
    print(f"  Passed: {_green(str(passed))}")
    if warned:
        print(f"  Warned: {_yellow(str(warned))}  (server precondition / expected rejection)")
    if failed:
        print(f"  Failed: {_red(str(failed))}")
        print()
        print(_bold("Failed tests:"))
        for label, status, detail in _results:
            if status == "fail":
                msg = detail[:120] if detail else ""
                print(f"  {_red('FAIL')}  {label}")
                if msg:
                    print(f"         {msg}")
    else:
        print(f"  Failed: {_green('0')}")

    if warned and not failed:
        print()
        print(_bold("Warned tests (not failures):"))
        for label, status, detail in _results:
            if status == "warn":
                msg = detail[:120] if detail else ""
                print(f"  {_yellow('WARN')}  {label}")
                if msg:
                    print(f"         {msg}")

    print(_bold("=" * 60))

    sys.exit(1 if failed else 0)
