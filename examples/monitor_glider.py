#!/usr/bin/env python3
"""Monitor a glider's dialog output and script state transitions.

Subscribes to real-time STOMP streams for dialog data and script
assignment events, logging each line with a high-resolution timestamp.

Usage::

    python monitor_glider.py <glider-name> [logfile]

    # Log to file (also prints to stderr)
    python monitor_glider.py osusim osusim.log

    # Log to stdout only
    python monitor_glider.py osusim

Press Ctrl-C to stop.

Loads credentials from ``~/.config/sfmc/credentials.json`` by default.
"""

import argparse
import logging
import sys
import threading
from collections.abc import Generator

from sfmc_api import SFMCClient
from sfmc_api.stomp import _MAX_SEQUENCE, StompSubscription

# ── Sequence-ordered dialog output ───────────────────────────────────


def ordered_dialog(
    sub: StompSubscription,
) -> Generator[str, None, None]:
    """Yield dialog data strings in sequence order.

    The SFMC server sends dialog output with ``sequenceNumber`` fields.
    Messages may arrive out of order.  This generator buffers
    out-of-order messages and yields them in correct sequence,
    matching the Node.js reference implementation's reordering logic.

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
            next_expected = (next_expected + 1) if next_expected < _MAX_SEQUENCE else 0

            # Drain any buffered messages that are now in order
            while next_expected in pending:
                yield pending.pop(next_expected)
                next_expected = (next_expected + 1) if next_expected < _MAX_SEQUENCE else 0
        else:
            # Out of order — buffer it
            pending[seq] = data

            # If the gap is too large, the buffer is stale — flush and reset
            if len(pending) > 100:
                pending.clear()


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

    fmt.formatTime = format_time_usec  # type: ignore[assignment]

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


def monitor_dialog(
    sub: StompSubscription,
    log: logging.Logger,
    stop: threading.Event,
) -> None:
    """Read dialog output and log each line."""
    for data in ordered_dialog(sub):
        if stop.is_set():
            break
        # Dialog data may contain multiple lines or partial lines.
        # Log each non-empty line separately for clean output.
        for line in data.splitlines():
            if line:
                log.info(line)


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
    parser.add_argument("glider_name", help="Registered glider name (e.g. osusim)")
    parser.add_argument(
        "logfile",
        nargs="?",
        default=None,
        help="Log file path (default: stderr only)",
    )
    args = parser.parse_args()

    dialog_log, script_log = setup_logging(args.glider_name, args.logfile)
    stop = threading.Event()

    info_log = logging.getLogger(f"sfmc.{args.glider_name}.INFO")
    info_log.setLevel(logging.INFO)
    info_log.propagate = False
    for h in dialog_log.handlers:
        info_log.addHandler(h)

    with SFMCClient() as client:
        details = client.get_glider_details(args.glider_name)
        glider_state = details["data"]["state"]
        info_log.info(
            "Monitoring %s (id=%s, state=%s)",
            args.glider_name,
            details["data"]["id"],
            glider_state,
        )

        deploy = client.get_active_deployment_details(args.glider_name)
        d = deploy["data"]
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

            try:
                # Block until Ctrl-C
                stop.wait()
            except KeyboardInterrupt:
                info_log.info("Stopping...")
                stop.set()
                dialog_sub.close()
                script_sub.close()

        info_log.info("Disconnected.")


if __name__ == "__main__":
    main()
