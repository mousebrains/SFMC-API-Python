#!/usr/bin/env python3
"""Monitor a glider's dialog output and script state transitions.

Subscribes to real-time STOMP streams for dialog data and script
assignment events, logging each line with a high-resolution timestamp.

Usage::

    sfmc-monitor-glider <glider-name> [logfile]

    # Log to file (also prints to stderr)
    sfmc-monitor-glider osu685 osu685.log

    # Log to stdout only
    sfmc-monitor-glider osu685

Press Ctrl-C to stop.

Loads credentials from ``~/.config/sfmc/credentials.json`` by default.
"""

import argparse
import logging
import re
import sys
import threading
import time
from collections.abc import Generator

from sfmc_api import SFMCClient
from sfmc_api.stomp import MAX_SEQUENCE, StompSubscription

logger = logging.getLogger(__name__)

# ── Sequence-ordered dialog output ───────────────────────────────────

#: Maximum number of out-of-order messages we buffer before giving up
#: and yielding what we have.  100 covers typical Iridium reordering
#: while still bounding memory if a sequence number is permanently
#: lost.
_ORDER_BUFFER_MAX = 100


def ordered_dialog(
    sub: StompSubscription,
) -> Generator[str, None, None]:
    """Yield dialog data strings in sequence order.

    The SFMC server sends dialog output with ``sequenceNumber`` fields.
    Messages may arrive out of order.  This generator buffers
    out-of-order messages and yields them in correct sequence,
    matching the Node.js reference implementation's reordering logic.

    Recovery: if the out-of-order buffer grows past
    ``_ORDER_BUFFER_MAX``, we assume a sequence number is permanently
    lost (e.g. a dropped Iridium frame) and flush every buffered
    message in sequence-number order, then resume from whatever
    arrives next.  Messages are never silently discarded — they may
    just be yielded out of natural order across a flush boundary.
    A WARNING is logged when this happens so operators can see it.

    Yields:
        Each dialog data string, in sequence order.
    """
    next_expected: int | None = None
    pending: dict[int, str] = {}

    for msg in sub:
        seq = msg.get("sequenceNumber")
        data = msg.get("data", "")

        if seq is None:
            # No sequence info — yield immediately
            yield data
            continue

        if next_expected is None or seq == next_expected:
            # In order (or first message) — yield and advance
            yield data
            if next_expected is None:
                next_expected = seq
            next_expected = (next_expected + 1) if next_expected < MAX_SEQUENCE else 0

            # Drain any buffered messages that are now in order
            while next_expected in pending:
                yield pending.pop(next_expected)
                next_expected = (next_expected + 1) if next_expected < MAX_SEQUENCE else 0
        else:
            # Out of order — buffer it
            pending[seq] = data

            # If the gap is too large, the buffer is stale — flush and reset.
            if len(pending) > _ORDER_BUFFER_MAX:
                lowest = min(pending)
                highest = max(pending)
                logger.warning(
                    "ordered_dialog: sequence gap exceeded buffer (%d msgs, "
                    "expected=%s, buffered range [%d, %d]). Flushing in "
                    "sequence-number order and resuming.",
                    len(pending),
                    next_expected,
                    lowest,
                    highest,
                )
                for seq_key in sorted(pending):
                    yield pending[seq_key]
                pending.clear()
                next_expected = None


# ── Logging setup ────────────────────────────────────────────────────


def setup_logging(
    glider_name: str,
    log_file: str | None,
) -> tuple[logging.Logger, logging.Logger]:
    """Create two loggers: one for dialog output, one for script events.

    Both use the same format with high-resolution sortable timestamps::

        2026-03-26T19:14:41.123456 DIALOG  line of dialog text
        2026-03-26T19:14:42.654321 SCRIPT  state=running name=sfmc.xml type=factory paused=False

    Args:
        glider_name: Used in the log filename and logger names.
        log_file: Path to the log file.  If ``None``, logs to stderr.

    Returns:
        A tuple of ``(dialog_logger, script_logger)``.
    """
    fmt = logging.Formatter(
        fmt="%(asctime)s %(name)s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Override formatTime to include microseconds
    def format_time_usec(record: logging.LogRecord, datefmt: str | None = None) -> str:
        import datetime

        dt = datetime.datetime.fromtimestamp(record.created)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    fmt.formatTime = format_time_usec  # type: ignore[method-assign]

    handlers: list[logging.Handler] = []

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        handlers.append(fh)

        # Also log to stderr for visibility
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        handlers.append(sh)
    else:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        handlers.append(sh)

    dialog_logger = logging.getLogger(f"sfmc.{glider_name}.DIALOG")
    dialog_logger.setLevel(logging.INFO)
    dialog_logger.propagate = False
    for h in handlers:
        dialog_logger.addHandler(h)

    script_logger = logging.getLogger(f"sfmc.{glider_name}.SCRIPT")
    script_logger.setLevel(logging.INFO)
    script_logger.propagate = False
    for h in handlers:
        script_logger.addHandler(h)

    return dialog_logger, script_logger


# ── Monitoring threads ───────────────────────────────────────────────


_LINE_SEP = re.compile(r"\r\n|\r|\n")


def _log_with_time(log: logging.Logger, msg: str, created: float) -> None:
    """Emit a log record with an explicit creation timestamp."""
    record = log.makeRecord(
        log.name,
        logging.INFO,
        "(monitor)",
        0,
        msg,
        (),
        None,
    )
    record.created = created
    log.handle(record)


def monitor_dialog(
    sub: StompSubscription,
    log: logging.Logger,
    stop: threading.Event,
) -> None:
    """Read dialog output and log each reassembled line."""
    buf = ""
    line_start: float = 0.0
    for data in ordered_dialog(sub):
        if stop.is_set():
            break
        if not buf:
            line_start = time.time()
        buf += data
        parts = _LINE_SEP.split(buf)
        # Last element is the unterminated fragment — keep buffering it.
        buf = parts[-1]
        for line in parts[:-1]:
            if line:
                _log_with_time(log, line, line_start)
            line_start = time.time()
    # Flush remaining buffer when stream ends.
    if buf.strip():
        _log_with_time(log, buf, line_start)


def monitor_scripts(
    sub: StompSubscription,
    log: logging.Logger,
    stop: threading.Event,
) -> None:
    """Read script events and log each state transition."""
    for event in sub:
        if stop.is_set():
            break
        script_name = event.get("scriptName", "?")
        script_type = event.get("scriptType", "?")
        script_state = event.get("scriptState", "?")
        paused = event.get("paused", False)
        log.info(
            "state=%s name=%s type=%s paused=%s",
            script_state,
            script_name,
            script_type,
            paused,
        )


# ── Main ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Monitor a glider's dialog output and script state transitions.",
    )
    parser.add_argument("glider_name", help="Registered glider name (e.g. osu685)")
    parser.add_argument(
        "logfile",
        nargs="?",
        default=None,
        help="Log file path (default: stderr only)",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="SFMC server hostname (selects entry from multi-host credentials file)",
    )
    parser.add_argument(
        "--credentials",
        default=None,
        metavar="PATH",
        help="Path to credentials JSON file (default: ~/.config/sfmc/credentials.json)",
    )
    args = parser.parse_args()

    dialog_log, script_log = setup_logging(args.glider_name, args.logfile)
    stop = threading.Event()

    info_log = logging.getLogger(f"sfmc.{args.glider_name}.INFO")
    info_log.setLevel(logging.INFO)
    info_log.propagate = False
    for h in dialog_log.handlers:
        info_log.addHandler(h)

    with SFMCClient(host=args.host, config_path=args.credentials) as client:
        details = client.get_glider_details(args.glider_name)
        try:
            glider_state = details["data"]["state"]
            glider_id = details["data"]["id"]
        except (KeyError, TypeError) as exc:
            info_log.error("Unexpected API response: %s", exc)
            sys.exit(1)
        info_log.info(
            "Monitoring %s (id=%s, state=%s)",
            args.glider_name,
            glider_id,
            glider_state,
        )

        deploy = client.get_active_deployment_details(args.glider_name)
        try:
            d = deploy["data"]
        except (KeyError, TypeError) as exc:
            info_log.error("Unexpected API response: %s", exc)
            sys.exit(1)
        if d.get("currentScriptName"):
            info_log.info(
                "Active script: %s (%s), running=%s",
                d["currentScriptName"],
                d["currentScriptType"],
                d["isCurrentScriptRunning"],
            )
        else:
            info_log.info("No script currently assigned")

        info_log.info("Opening STOMP stream...")

        with client.open_stream() as stomp:
            dialog_sub = client.subscribe_glider_output(args.glider_name, stomp)
            script_sub = client.subscribe_script_events(args.glider_name, stomp)

            info_log.info(
                "Subscribed. Logging to %s",
                args.logfile or "stderr",
            )

            dialog_thread = threading.Thread(
                target=monitor_dialog,
                args=(dialog_sub, dialog_log, stop),
                daemon=True,
                name="dialog-monitor",
            )
            script_thread = threading.Thread(
                target=monitor_scripts,
                args=(script_sub, script_log, stop),
                daemon=True,
                name="script-monitor",
            )

            dialog_thread.start()
            script_thread.start()

            while not stop.is_set():
                try:
                    stop.wait(timeout=5)
                except KeyboardInterrupt:
                    info_log.info("Stopping...")
                    stop.set()
                    dialog_sub.close()
                    script_sub.close()
                    break
                if not dialog_thread.is_alive() or not script_thread.is_alive():
                    info_log.warning("Stream disconnected")
                    break

        info_log.info("Disconnected.")


if __name__ == "__main__":
    main()
