"""Monitor a glider and run a follower plugin that generates files.

``sfmc-follow`` connects to a glider's real-time dialog stream,
parses telemetry from each surfacing (GPS, sensors, timestamps),
passes the data to a user-supplied *follower* class, and uploads
any files the follower generates back to SFMC.

Simulation modes
----------------

Two orthogonal flags control simulation behaviour:

``--replay LOGFILE``
    Read dialog lines from a log file (produced by ``sfmc-monitor-glider``)
    instead of connecting to a live STOMP stream.  Events are replayed
    with a configurable delay (``--replay-interval``, default 10 s).

``--dry-run``
    Print the files the follower would generate instead of uploading
    them to SFMC.

Combine them for four modes:

=========================  =============  ==========
Mode                       ``--replay``   ``--dry-run``
=========================  =============  ==========
Live + upload (default)    no             no
Live + print only          no             yes
Replay + upload            yes            no
Replay + print (offline)   yes            yes
=========================  =============  ==========

CLI usage::

    # Full live mode (default)
    sfmc-follow --glider osu685 --follower drifter_follower.py \\
                --config drifter.yaml

    # Offline replay: no SFMC connection needed
    sfmc-follow --glider osu685 --follower drifter_follower.py \\
                --config drifter.yaml --replay dialog.log --dry-run

    # Replay and upload to SFMC
    sfmc-follow --glider osu685 --follower drifter_follower.py \\
                --config drifter.yaml --replay dialog.log

    # Live monitor, print only (no upload)
    sfmc-follow --glider osu685 --follower drifter_follower.py \\
                --config drifter.yaml --dry-run

Programmatic usage::

    from sfmc_api import SFMCClient
    from sfmc_api.follow_glider import follow_glider
    from my_follower import MyFollower

    with SFMCClient() as client:
        follow_glider(client, "osu685", MyFollower, config={"key": "val"})

    # Offline replay
    follow_glider(
        client=None, glider_name="osu685",
        follower_class=MyFollower,
        replay="dialog.log", dry_run=True,
    )

Press Ctrl-C to stop.
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import re
import sys
import threading
import time
from pathlib import Path
from queue import Empty, Queue
from typing import Any

from sfmc_api.dialog_parser import DialogParser, SurfacingEvent
from sfmc_api.follower import BaseFollower, load_follower_class
from sfmc_api.monitor_glider import _LINE_SEP, _log_with_time, ordered_dialog
from sfmc_api.stomp import StompSubscription

logger = logging.getLogger(__name__)


# ── Log line regex ──────────────────────────────────────────────────

#: Matches a sfmc-monitor-glider log line and captures the dialog text.
#: Format: ``2026-03-26T19:14:41.123456 sfmc.NAME.DIALOG  dialog text``
#: Also accepts lines without the logger prefix (raw dialog).
_LOG_LINE_RE = re.compile(
    r"^(?:\d{4}-\d{2}-\d{2}T[\d:.]+\s+\S+\s{2})?(.*)",
)


def _parse_log_line(line: str) -> str | None:
    """Extract the dialog text from a log line.

    Handles both prefixed lines (from sfmc-monitor-glider logs) and
    raw dialog lines.  Returns ``None`` for blank lines or lines
    from non-DIALOG loggers (e.g. SCRIPT, INFO, FOLLOW).

    Args:
        line: A single line from the log file (no trailing newline).

    Returns:
        The dialog text, or ``None`` if the line should be skipped.
    """
    stripped = line.rstrip()
    if not stripped:
        return None

    # If the line has a logger prefix, only keep DIALOG lines.
    # Pattern: timestamp + space + logger_name + double-space + message
    # e.g.: "2026-03-26T19:14:41.123456 sfmc.osu685.DIALOG  Vehicle Name: osu685"
    ts_logger_match = re.match(
        r"^\d{4}-\d{2}-\d{2}T[\d:.]+\s+(\S+)\s{2}(.*)",
        stripped,
    )
    if ts_logger_match:
        logger_name = ts_logger_match.group(1)
        if ".DIALOG" not in logger_name:
            return None
        return ts_logger_match.group(2)

    # Raw dialog line (no prefix).
    return stripped


# ── Dialog reader thread (live STOMP) ───────────────────────────────


def _read_dialog(
    sub: StompSubscription,
    parser: DialogParser,
    queue_in: Queue[SurfacingEvent | None],
    dialog_log: logging.Logger | None,
    stop: threading.Event,
) -> None:
    """Read STOMP dialog output, parse surfacings, and feed the follower.

    This function runs in its own thread.  It reassembles fragmented
    dialog lines (same logic as :func:`monitor_glider.monitor_dialog`),
    logs each line, and feeds them to *parser*.  When *parser* produces
    a :class:`SurfacingEvent`, it is put on *queue_in* for the follower.
    """
    buf = ""
    line_start: float = 0.0

    for data in ordered_dialog(sub):
        if stop.is_set():
            break

        if not buf:
            line_start = time.time()
        buf += data
        parts = _LINE_SEP.split(buf)
        buf = parts[-1]

        for line in parts[:-1]:
            if line:
                if dialog_log:
                    _log_with_time(dialog_log, line, line_start)
                event = parser.feed_line(line)
                if event is not None:
                    queue_in.put(event)
            line_start = time.time()

    # Flush remaining buffer.
    if buf.strip():
        if dialog_log:
            _log_with_time(dialog_log, buf, line_start)
        event = parser.feed_line(buf)
        if event is not None:
            queue_in.put(event)

    # Flush any partially collected surfacing.
    event = parser.flush()
    if event is not None:
        queue_in.put(event)


# ── Replay reader thread (log file) ────────────────────────────────


def _replay_dialog(
    replay_path: str | Path,
    parser: DialogParser,
    queue_in: Queue[SurfacingEvent | None],
    info_log: logging.Logger,
    interval: float,
    stop: threading.Event,
) -> None:
    """Read dialog lines from a log file and feed them to the parser.

    Surfacing events are delivered with *interval* seconds between
    each one, simulating real-time surfacings.

    The log file can be a ``sfmc-monitor-glider`` output file (with
    timestamp + logger prefix) or a raw dialog text file.
    """
    event_count = 0
    with open(replay_path, encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            if stop.is_set():
                break
            dialog_text = _parse_log_line(raw_line)
            if dialog_text is None:
                continue
            event = parser.feed_line(dialog_text)
            if event is not None:
                event_count += 1
                info_log.info(
                    "Replay event %d: %s at %.4f, %.4f",
                    event_count,
                    event.vehicle_name or "?",
                    event.gps_lat or 0.0,
                    event.gps_lon or 0.0,
                )
                queue_in.put(event)
                if not stop.is_set():
                    stop.wait(timeout=interval)

    # Flush any remaining partial surfacing.
    event = parser.flush()
    if event is not None:
        event_count += 1
        info_log.info(
            "Replay event %d (final flush): %s",
            event_count,
            event.vehicle_name or "?",
        )
        queue_in.put(event)

    info_log.info("Replay complete: %d surfacing events", event_count)


# ── Upload thread ───────────────────────────────────────────────────


def _upload_files(
    client: Any,  # SFMCClient, but Any to avoid import at module level
    glider_name: str,
    queue_out: Queue[dict[str, dict[str, str | bytes]] | None],
    upload_log: logging.Logger,
    stop: threading.Event,
) -> None:
    """Read file dicts from queue_out and upload them to SFMC.

    Runs in its own thread.  Each item from the queue is a dict like
    ``{"to-glider": {"filename": "content"}, "to-science": {...}}``.
    """
    while not stop.is_set():
        try:
            output = queue_out.get(timeout=1.0)
        except Empty:
            continue

        if output is None:
            break

        for folder, files in output.items():
            if not files:
                continue
            filenames = ", ".join(files.keys())
            try:
                client.upload_glider_file_contents(glider_name, folder, files)
                upload_log.info(
                    "Uploaded to %s: %s",
                    folder,
                    filenames,
                )
            except Exception:
                upload_log.exception(
                    "Failed to upload to %s: %s",
                    folder,
                    filenames,
                )


# ── Dry-run output thread ──────────────────────────────────────────


def _print_files(
    queue_out: Queue[dict[str, dict[str, str | bytes]] | None],
    output_log: logging.Logger,
    stop: threading.Event,
) -> None:
    """Print generated files instead of uploading them.

    Used in ``--dry-run`` mode.  For each file the follower produces,
    logs the folder, filename, and content.
    """
    while not stop.is_set():
        try:
            output = queue_out.get(timeout=1.0)
        except Empty:
            continue

        if output is None:
            break

        for folder, files in output.items():
            if not files:
                continue
            for filename, content in files.items():
                if isinstance(content, bytes):
                    display = content.decode("utf-8", errors="replace")
                else:
                    display = content
                output_log.info(
                    "[dry-run] %s/%s (%d bytes):\n%s",
                    folder,
                    filename,
                    len(display),
                    display,
                )


# ── Logging setup ───────────────────────────────────────────────────


def setup_logging(
    glider_name: str,
    log_file: str | None = None,
    log_level: str = "INFO",
    log_max_bytes: int = 10 * 1024 * 1024,
    log_backup_count: int = 5,
) -> tuple[logging.Logger, logging.Logger, logging.Logger]:
    """Create loggers for dialog, upload events, and info messages.

    Args:
        glider_name: Used in logger names.
        log_file: Path to log file.  If ``None``, logs to stderr only.
        log_level: Logging level name (e.g. ``"DEBUG"``, ``"INFO"``).
        log_max_bytes: Max log file size before rotation (default 10 MB).
        log_backup_count: Number of rotated backup files (default 5).

    Returns:
        A tuple of ``(dialog_logger, upload_logger, info_logger)``.
    """
    level = getattr(logging, log_level.upper(), logging.INFO)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    def format_time_usec(record: logging.LogRecord, datefmt: str | None = None) -> str:
        import datetime

        dt = datetime.datetime.fromtimestamp(record.created)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    fmt.formatTime = format_time_usec  # type: ignore[method-assign]

    handlers: list[logging.Handler] = []

    if log_file:
        fh = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=log_max_bytes,
            backupCount=log_backup_count,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        handlers.append(fh)

        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        handlers.append(sh)
    else:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        handlers.append(sh)

    def _make_logger(suffix: str) -> logging.Logger:
        log = logging.getLogger(f"sfmc.{glider_name}.{suffix}")
        log.setLevel(level)
        log.propagate = False
        for h in handlers:
            log.addHandler(h)
        return log

    dialog_log = _make_logger("DIALOG")
    upload_log = _make_logger("UPLOAD")
    info_log = _make_logger("FOLLOW")

    return dialog_log, upload_log, info_log


# ── Main API ────────────────────────────────────────────────────────


def follow_glider(
    client: Any | None,
    glider_name: str,
    follower_class: type[BaseFollower],
    follower_config: dict[str, Any] | None = None,
    log_file: str | None = None,
    log_level: str = "INFO",
    log_max_bytes: int = 10 * 1024 * 1024,
    log_backup_count: int = 5,
    replay: str | None = None,
    replay_interval: float = 10.0,
    dry_run: bool = False,
    stop: threading.Event | None = None,
) -> None:
    """Monitor a glider and run a follower that generates files.

    Connects to the glider's STOMP dialog stream (or replays from a
    log file), parses telemetry from each surfacing, feeds it to
    *follower_class*, and uploads (or prints) any files the follower
    generates.

    Blocks until *stop* is set, the replay finishes, or the user
    presses Ctrl-C.

    Args:
        client: An authenticated :class:`~sfmc_api.SFMCClient`, or
            ``None`` when running in fully offline mode
            (``replay`` + ``dry_run``).
        glider_name: The registered glider name.
        follower_class: A :class:`BaseFollower` subclass to instantiate.
        follower_config: Configuration dict passed to the follower.
        log_file: Path to log file (or ``None`` for stderr only).
        log_level: Logging level (``"DEBUG"``, ``"INFO"``, etc.).
        log_max_bytes: Max log file size before rotation.
        log_backup_count: Number of rotated backup files.
        replay: Path to a dialog log file to replay instead of
            connecting to a live STOMP stream.  The file can be output
            from ``sfmc-monitor-glider`` or raw dialog text.
        replay_interval: Seconds to wait between surfacing events
            during replay (default 10).
        dry_run: If ``True``, print generated files to the log instead
            of uploading them to SFMC.
        stop: Optional event to signal shutdown externally.
    """
    if stop is None:
        stop = threading.Event()

    dialog_log, upload_log, info_log = setup_logging(
        glider_name,
        log_file,
        log_level,
        log_max_bytes,
        log_backup_count,
    )

    # ── Validate arguments ──────────────────────────────────────
    if not replay and not dry_run and client is None:
        info_log.error("client is required for live mode without --dry-run")
        return

    if replay and not dry_run and client is None:
        info_log.error("client is required for replay + upload mode")
        return

    # ── Verify glider exists (skip in offline replay) ───────────
    if client is not None and not replay:
        details = client.get_glider_details(glider_name)
        try:
            glider_state = details["data"]["state"]
        except (KeyError, TypeError) as exc:
            info_log.error("Unexpected API response: %s", exc)
            return
        info_log.info(
            "Following %s (state=%s)",
            glider_name,
            glider_state,
        )
    else:
        mode_label = []
        if replay:
            mode_label.append(f"replay={replay}")
        if dry_run:
            mode_label.append("dry-run")
        info_log.info(
            "Following %s (%s)",
            glider_name,
            ", ".join(mode_label),
        )

    # ── Set up queues and follower ──────────────────────────────
    queue_in: Queue[SurfacingEvent | None] = Queue()
    queue_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()

    follower = follower_class(
        config=follower_config or {},
        queue_in=queue_in,
        queue_out=queue_out,
    )
    info_log.info("Follower: %s", type(follower).__name__)

    parser = DialogParser()

    # ── Replay mode ─────────────────────────────────────────────
    if replay:
        replay_thread = threading.Thread(
            target=_replay_dialog,
            args=(replay, parser, queue_in, info_log, replay_interval, stop),
            daemon=True,
            name="dialog-replay",
        )

        if dry_run:
            output_thread = threading.Thread(
                target=_print_files,
                args=(queue_out, upload_log, stop),
                daemon=True,
                name="dry-run-printer",
            )
        else:
            output_thread = threading.Thread(
                target=_upload_files,
                args=(client, glider_name, queue_out, upload_log, stop),
                daemon=True,
                name="file-uploader",
            )

        replay_thread.start()
        follower.start()
        output_thread.start()

        info_log.info("Replay started. Press Ctrl-C to stop.")

        while not stop.is_set():
            try:
                stop.wait(timeout=5)
            except KeyboardInterrupt:
                info_log.info("Stopping...")
                stop.set()
                break
            if not replay_thread.is_alive():
                # Replay finished naturally -- let pipeline drain.
                info_log.info("Replay file exhausted, draining pipeline...")
                follower.shutdown()
                follower.join(timeout=10)
                queue_out.put(None)
                output_thread.join(timeout=10)
                break
            if not follower.is_alive():
                info_log.warning("Follower thread died")
                stop.set()
                break

        if stop.is_set():
            follower.shutdown()
            queue_out.put(None)
            replay_thread.join(timeout=5)
            follower.join(timeout=5)
            output_thread.join(timeout=5)

        info_log.info("Done.")
        return

    # ── Live mode ───────────────────────────────────────────────
    assert client is not None
    info_log.info("Opening STOMP stream...")
    with client.open_stream() as stomp:
        dialog_sub = client.subscribe_glider_output(glider_name, stomp)
        info_log.info("Subscribed to dialog output")

        dialog_thread = threading.Thread(
            target=_read_dialog,
            args=(dialog_sub, parser, queue_in, dialog_log, stop),
            daemon=True,
            name="dialog-reader",
        )

        if dry_run:
            output_thread = threading.Thread(
                target=_print_files,
                args=(queue_out, upload_log, stop),
                daemon=True,
                name="dry-run-printer",
            )
        else:
            output_thread = threading.Thread(
                target=_upload_files,
                args=(client, glider_name, queue_out, upload_log, stop),
                daemon=True,
                name="file-uploader",
            )

        dialog_thread.start()
        follower.start()
        output_thread.start()

        info_log.info(
            "All threads started%s. Press Ctrl-C to stop.",
            " (dry-run)" if dry_run else "",
        )

        # ── Wait loop ───────────────────────────────────────────
        while not stop.is_set():
            try:
                stop.wait(timeout=5)
            except KeyboardInterrupt:
                info_log.info("Stopping...")
                stop.set()
                break
            if not dialog_thread.is_alive():
                info_log.warning("Dialog stream disconnected")
                stop.set()
                break
            if not follower.is_alive():
                info_log.warning("Follower thread died")
                stop.set()
                break

        # ── Cleanup ─────────────────────────────────────────────
        dialog_sub.close()
        follower.shutdown()
        queue_out.put(None)

        dialog_thread.join(timeout=5)
        follower.join(timeout=5)
        output_thread.join(timeout=5)

    info_log.info("Disconnected.")


# ── CLI ─────────────────────────────────────────────────────────────


def _load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML config file."""
    try:
        import yaml
    except ImportError:
        sys.stderr.write(
            "Error: PyYAML is required for --config.\n"
            "Install with: pip install 'sfmc-api[follow]'\n"
        )
        sys.exit(1)
    with open(path) as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        sys.stderr.write(f"Error: {path} must contain a YAML mapping\n")
        sys.exit(1)
    return data


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for sfmc-follow."""
    parser = argparse.ArgumentParser(
        prog="sfmc-follow",
        description=(
            "Monitor a glider's dialog output and run a follower plugin "
            "that generates navigation files."
        ),
        epilog=(
            "Simulation modes:\n"
            "  --replay LOG --dry-run   Offline: replay log, print output\n"
            "  --replay LOG             Replay log, upload to SFMC\n"
            "  --dry-run                Live monitor, print output (no upload)\n"
            "  (neither)                Live monitor + upload (default)\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--glider",
        required=True,
        metavar="NAME",
        help="Registered glider name (e.g. osu685)",
    )
    parser.add_argument(
        "--follower",
        required=True,
        metavar="FILE",
        help="Path to Python file containing the follower class",
    )
    parser.add_argument(
        "--class",
        dest="class_name",
        default=None,
        metavar="NAME",
        help=("Follower class name within the file (auto-detected if the file has exactly one)"),
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="FILE",
        help="YAML configuration file passed to the follower",
    )
    parser.add_argument(
        "--hostname",
        default=None,
        help="SFMC server hostname (selects entry from multi-host credentials file)",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        metavar="PATH",
        help="Path to credentials JSON file (default: ~/.config/sfmc/credentials.json)",
    )

    # ── Simulation ──────────────────────────────────────────────
    sim = parser.add_argument_group("simulation modes")
    sim.add_argument(
        "--replay",
        default=None,
        metavar="LOGFILE",
        help=("Replay dialog lines from a log file instead of connecting to a live STOMP stream"),
    )
    sim.add_argument(
        "--replay-interval",
        type=float,
        default=10.0,
        metavar="SECS",
        help="Seconds between surfacing events during replay (default: 10)",
    )
    sim.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print generated files instead of uploading to SFMC",
    )

    # ── Logging ─────────────────────────────────────────────────
    log_group = parser.add_argument_group("logging")
    log_group.add_argument(
        "--logfile",
        default=None,
        metavar="FILE",
        help="Log file path (default: stderr only)",
    )
    log_group.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    log_group.add_argument(
        "--log-max-size",
        type=int,
        default=10 * 1024 * 1024,
        metavar="BYTES",
        help="Max log file size in bytes before rotation (default: 10485760 = 10 MB)",
    )
    log_group.add_argument(
        "--log-backup-count",
        type=int,
        default=5,
        metavar="N",
        help="Number of rotated backup log files to keep (default: 5)",
    )
    return parser


def main() -> None:
    """CLI entry point for sfmc-follow."""
    ap = build_parser()
    args = ap.parse_args()

    # Load follower class.
    try:
        follower_class = load_follower_class(args.follower, args.class_name)
    except (FileNotFoundError, ValueError, ImportError) as exc:
        sys.stderr.write(f"Error loading follower: {exc}\n")
        sys.exit(1)

    # Load YAML config.
    follower_config: dict[str, Any] = {}
    if args.config:
        follower_config = _load_yaml(args.config)

    # Decide whether we need an SFMC client.
    need_client = not (args.replay and args.dry_run)

    try:
        if need_client:
            from sfmc_api import SFMCClient

            with SFMCClient(
                host=args.hostname,
                config_path=args.credentials,
            ) as client:
                follow_glider(
                    client=client,
                    glider_name=args.glider,
                    follower_class=follower_class,
                    follower_config=follower_config,
                    log_file=args.logfile,
                    log_level=args.log_level,
                    log_max_bytes=args.log_max_size,
                    log_backup_count=args.log_backup_count,
                    replay=args.replay,
                    replay_interval=args.replay_interval,
                    dry_run=args.dry_run,
                )
        else:
            # Fully offline: replay + dry-run, no client needed.
            follow_glider(
                client=None,
                glider_name=args.glider,
                follower_class=follower_class,
                follower_config=follower_config,
                log_file=args.logfile,
                log_level=args.log_level,
                log_max_bytes=args.log_max_size,
                log_backup_count=args.log_backup_count,
                replay=args.replay,
                replay_interval=args.replay_interval,
                dry_run=args.dry_run,
            )
    except KeyboardInterrupt:
        sys.stderr.write("\nStopped.\n")
    except Exception as exc:
        sys.stderr.write(f"Error: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
