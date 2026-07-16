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
import queue
import re
import signal
import sys
import threading
import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

from sfmc_api import SFMCClient
from sfmc_api.exceptions import SFMCError
from sfmc_api.stomp import MAX_SEQUENCE, StompError, StompSubscription
from sfmc_api.stream_reconnect import ReconnectBackoff, safe_stream_error

logger = logging.getLogger(__name__)

STREAM_BOUNDARY_PREFIX = "STREAM_BOUNDARY"

# ── Sequence-ordered dialog output ───────────────────────────────────

#: Maximum number of out-of-order messages we buffer before giving up
#: and yielding what we have.  100 covers typical Iridium reordering
#: while still bounding memory if a sequence number is permanently
#: lost.
_ORDER_BUFFER_MAX = 100


def _flush_order(pending: dict[int, str], next_expected: int | None) -> list[int]:
    """Order buffered sequence numbers for a flush.

    Sorts by modular distance from *next_expected* so a buffer that
    straddles the ``MAX_SEQUENCE -> 0`` wraparound flushes in stream
    order (e.g. expected ``MAX_SEQUENCE``, buffered ``{MAX_SEQUENCE,
    0, 1}`` flushes in that order, not ``0, 1, MAX_SEQUENCE``).
    """
    if next_expected is None:
        return sorted(pending)
    span = MAX_SEQUENCE + 1
    return sorted(pending, key=lambda seq: (seq - next_expected) % span)


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
    message in stream order, then resume from whatever arrives next.
    When the subscription ends, anything still buffered is flushed the
    same way.  Messages are never silently discarded — they may just
    be yielded out of natural order across a flush boundary.  A
    WARNING is logged when this happens so operators can see it.

    Yields:
        Each dialog data string, in sequence order.
    """
    next_expected: int | None = None
    pending: dict[int, str] = {}

    try:
        for msg in sub:
            # Server-data variance (a bare array, a null field) must
            # cost one skipped message, not the whole service: these
            # workers run under a supervisor that treats unexpected
            # exceptions as fatal code bugs.
            if not isinstance(msg, dict):
                logger.warning("ordered_dialog: skipping non-object message: %.200r", msg)
                continue
            seq = msg.get("sequenceNumber")
            data = msg.get("data", "")
            if not isinstance(data, str):
                logger.warning("ordered_dialog: skipping non-string data: %.200r", msg)
                continue
            if not isinstance(seq, int):
                seq = None

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
                    logger.warning(
                        "ordered_dialog: sequence gap exceeded buffer (%d msgs, "
                        "expected=%s, buffered range [%d, %d]). Flushing in "
                        "stream order and resuming.",
                        len(pending),
                        next_expected,
                        min(pending),
                        max(pending),
                    )
                    for seq_key in _flush_order(pending, next_expected):
                        yield pending[seq_key]
                    pending.clear()
                    next_expected = None
    except StompError:
        # A queued STOMP ERROR terminates iteration by raising rather than by
        # normal EOF. Preserve the same no-loss tail behavior before the
        # session supervisor replaces the connection.
        if pending:
            logger.warning(
                "ordered_dialog: STOMP error with %d message(s) buffered "
                "(expected=%s); flushing in stream order.",
                len(pending),
                next_expected,
            )
            for seq_key in _flush_order(pending, next_expected):
                yield pending[seq_key]
            pending.clear()
        raise

    # End of stream — a gap that never filled must not swallow the
    # buffered tail (often the last lines of a surfacing).
    if pending:
        logger.warning(
            "ordered_dialog: stream ended with %d message(s) buffered "
            "(expected=%s); flushing in stream order.",
            len(pending),
            next_expected,
        )
        for seq_key in _flush_order(pending, next_expected):
            yield pending[seq_key]


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
    dialog_logger.handlers.clear()
    dialog_logger.setLevel(logging.INFO)
    dialog_logger.propagate = False
    for h in handlers:
        dialog_logger.addHandler(h)

    script_logger = logging.getLogger(f"sfmc.{glider_name}.SCRIPT")
    script_logger.handlers.clear()
    script_logger.setLevel(logging.INFO)
    script_logger.propagate = False
    for h in handlers:
        script_logger.addHandler(h)

    return dialog_logger, script_logger


# ── Monitoring threads ───────────────────────────────────────────────


_LINE_SEP = re.compile(r"\r\n|\r|\n")

#: Cap on the line-reassembly buffer.  Dialog lines are short; data
#: that accumulates this much without a line break is binary chatter,
#: and buffering it forever is unbounded memory growth on a service
#: that runs for weeks.
_MAX_LINE_BUFFER_BYTES = 256 * 1024


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
    info_log: logging.Logger | None = None,
) -> None:
    """Read dialog output and log each reassembled line."""
    buf = ""
    line_start: float = 0.0
    try:
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
            if len(buf) > _MAX_LINE_BUFFER_BYTES:
                logger.warning(
                    "discarding %d bytes of line-break-free dialog data (buffer cap)",
                    len(buf),
                )
                buf = ""
    finally:
        if buf.strip():
            if stop.is_set():
                _log_with_time(log, buf, line_start)
            elif info_log is not None:
                info_log.warning(
                    "stream boundary discarded %d-byte unterminated fragment",
                    len(buf.encode("utf-8")),
                )


def monitor_scripts(
    sub: StompSubscription,
    log: logging.Logger,
    stop: threading.Event,
) -> None:
    """Read script events and log each state transition."""
    for event in sub:
        if stop.is_set():
            break
        if not isinstance(event, dict):
            logger.warning("monitor_scripts: skipping non-object event: %.200r", event)
            continue
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


# ── Session supervision ─────────────────────────────────────────────


@dataclass(frozen=True)
class _WorkerResult:
    name: str
    error: Exception | None


def _run_monitor_worker(
    name: str,
    target: Any,
    args: tuple[Any, ...],
    results: queue.Queue[_WorkerResult],
) -> None:
    try:
        target(*args)
    except Exception as exc:
        results.put(_WorkerResult(name, exc))
    else:
        results.put(_WorkerResult(name, None))


def _initial_status(
    client: SFMCClient,
    glider_name: str,
    info_log: logging.Logger,
) -> None:
    details = client.get_glider_details(glider_name)
    try:
        glider_state = details["data"]["state"]
        glider_id = details["data"]["id"]
    except (KeyError, TypeError) as exc:
        raise SFMCError(f"Unexpected glider-details response: {exc}") from exc
    info_log.info(
        "Monitoring %s (id=%s, state=%s)",
        glider_name,
        glider_id,
        glider_state,
    )
    _log_active_script(client, glider_name, info_log, resync=False)


def _log_active_script(
    client: SFMCClient,
    glider_name: str,
    info_log: logging.Logger,
    *,
    resync: bool,
) -> None:
    deploy = client.get_active_deployment_details(glider_name)
    try:
        data = deploy["data"]
        if not isinstance(data, dict):
            raise TypeError("'data' is not an object")
        current_script = data.get("currentScriptName")
        if current_script:
            script_type = data["currentScriptType"]
            is_running = data["isCurrentScriptRunning"]
    except (KeyError, TypeError) as exc:
        raise SFMCError(f"Unexpected deployment response: {exc}") from exc
    prefix = "Resync: " if resync else ""
    if current_script:
        info_log.info(
            "%sactive script: %s (%s), running=%s",
            prefix,
            current_script,
            script_type,
            is_running,
        )
    else:
        info_log.info("%sno script currently assigned", prefix)


def monitor_glider(
    client: SFMCClient,
    glider_name: str,
    dialog_log: logging.Logger,
    script_log: logging.Logger,
    info_log: logging.Logger,
    *,
    stop: threading.Event | None = None,
    reconnect: bool = True,
    reconnect_initial_delay: float = 15.0,
    reconnect_max_delay: float = 300.0,
    reconnect_stable_after: float = 60.0,
    reconnect_jitter: float = 0.2,
    worker_join_timeout: float = 5.0,
) -> None:
    """Monitor live streams until stopped, reconnecting after session loss."""
    if stop is None:
        stop = threading.Event()
    _initial_status(client, glider_name, info_log)

    backoff = ReconnectBackoff(
        initial_delay=reconnect_initial_delay,
        max_delay=reconnect_max_delay,
        stable_after=reconnect_stable_after,
        jitter=reconnect_jitter,
    )
    attempt_number = 0
    session_number = 0
    offline_since: float | None = None

    while not stop.is_set():
        attempt_number += 1
        subscribed_at: float | None = None
        failure: Exception | None = None
        reason = "closed"
        try:
            if attempt_number > 1:
                client.refresh_auth()
            with client.open_stream() as stomp:
                dialog_sub = client.subscribe_glider_output(glider_name, stomp)
                script_sub = client.subscribe_script_events(glider_name, stomp)
                results: queue.Queue[_WorkerResult] = queue.Queue()
                dialog_thread = threading.Thread(
                    target=_run_monitor_worker,
                    args=(
                        "dialog",
                        monitor_dialog,
                        (dialog_sub, dialog_log, stop, info_log),
                        results,
                    ),
                    daemon=True,
                    name="dialog-monitor",
                )
                script_thread = threading.Thread(
                    target=_run_monitor_worker,
                    args=(
                        "script",
                        monitor_scripts,
                        (script_sub, script_log, stop),
                        results,
                    ),
                    daemon=True,
                    name="script-monitor",
                )
                dialog_started = False
                script_started = False
                first_result: _WorkerResult | None = None
                try:
                    dialog_thread.start()
                    dialog_started = True
                    script_thread.start()
                    script_started = True
                    subscribed_at = time.monotonic()
                    session_number += 1
                    info_log.info("stream session %d subscribed", session_number)
                    if offline_since is not None:
                        info_log.info(
                            "stream session %d reconnected after %.1fs offline",
                            session_number,
                            subscribed_at - offline_since,
                        )
                        try:
                            _log_active_script(client, glider_name, info_log, resync=True)
                        except SFMCError as exc:
                            info_log.warning("script resync failed: %s", safe_stream_error(exc))
                        offline_since = None

                    while not stop.is_set():
                        try:
                            first_result = results.get(timeout=0.5)
                            break
                        except queue.Empty:
                            continue
                finally:
                    dialog_sub.close()
                    script_sub.close()
                    if dialog_started:
                        dialog_thread.join(timeout=worker_join_timeout)
                    if script_started:
                        script_thread.join(timeout=worker_join_timeout)
                    if dialog_thread.is_alive() or script_thread.is_alive():
                        raise RuntimeError("monitor worker did not stop after subscription close")

                worker_results = [] if first_result is None else [first_result]
                while True:
                    try:
                        worker_results.append(results.get_nowait())
                    except queue.Empty:
                        break
                for result in worker_results:
                    if result.error is None:
                        continue
                    if isinstance(result.error, StompError):
                        failure = result.error
                        reason = "stomp-error"
                    else:
                        raise RuntimeError(
                            f"{result.name} monitor failed: {safe_stream_error(result.error)}"
                        ) from result.error
        except SFMCError as exc:
            failure = exc
            reason = "session-error"

        if stop.is_set():
            break

        subscribed_uptime = (
            None if subscribed_at is None else max(0.0, time.monotonic() - subscribed_at)
        )
        if subscribed_at is not None:
            info_log.warning(
                "%s session=%d reason=%s",
                STREAM_BOUNDARY_PREFIX,
                session_number,
                reason,
            )
        if offline_since is None:
            offline_since = time.monotonic()
        detail = "normal subscription close" if failure is None else safe_stream_error(failure)
        if subscribed_at is None:
            info_log.warning(
                "stream setup attempt %d ended: %s: %s",
                attempt_number,
                reason,
                detail,
            )
        else:
            info_log.warning(
                "stream session %d ended: %s: %s",
                session_number,
                reason,
                detail,
            )
        if not reconnect:
            raise StompError(f"stream session ended: {reason}: {detail}") from failure

        delay = backoff.next_delay(subscribed_uptime=subscribed_uptime)
        info_log.info("reconnect attempt %d in %.1fs", delay.attempt, delay.actual)
        if stop.wait(delay.actual):
            break

    info_log.info("Disconnected.")


# ── Main ─────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    """Build the ``sfmc-monitor-glider`` argument parser."""
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
    parser.add_argument(
        "--no-reconnect",
        action="store_true",
        help="Exit non-zero if the live stream disconnects",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()

    dialog_log, script_log = setup_logging(args.glider_name, args.logfile)
    stop = threading.Event()

    info_log = logging.getLogger(f"sfmc.{args.glider_name}.INFO")
    info_log.handlers.clear()
    info_log.setLevel(logging.INFO)
    info_log.propagate = False
    for h in dialog_log.handlers:
        info_log.addHandler(h)

    previous_handlers: dict[signal.Signals, Any] = {}

    def request_stop(signum: int, frame: Any) -> None:
        del signum, frame
        info_log.info("Stopping...")
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)

    try:
        with SFMCClient(host=args.host, config_path=args.credentials) as client:
            monitor_glider(
                client,
                args.glider_name,
                dialog_log,
                script_log,
                info_log,
                stop=stop,
                reconnect=not args.no_reconnect,
            )
    except Exception as exc:
        info_log.error("Error: %s", safe_stream_error(exc))
        sys.exit(1)
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    main()
