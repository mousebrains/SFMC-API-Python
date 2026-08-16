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
import logging.handlers
import signal
import sys
import threading
from typing import Any

from sfmc_api import SFMCClient
from sfmc_api.dialog_stream import (
    MAX_LINE_BUFFER_BYTES,
    LineAssembler,
    ordered_dialog,
)
from sfmc_api.disconnect_notify import (
    DisconnectNotifier,
    add_notification_cli_args,
    build_notifier,
)
from sfmc_api.exceptions import SFMCError
from sfmc_api.stomp import StompConnection, StompSubscription
from sfmc_api.stream_reconnect import (
    STREAM_BOUNDARY_PREFIX,
    ReconnectBackoff,
    StreamSession,
    StreamSupervisor,
    retry_transient,
    safe_stream_error,
)

logger = logging.getLogger(__name__)

__all__ = [
    "MAX_LINE_BUFFER_BYTES",
    "STREAM_BOUNDARY_PREFIX",
    "monitor_dialog",
    "monitor_glider",
    "monitor_scripts",
    "ordered_dialog",
]


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

    # Override formatTime to include microseconds.  Timestamps are UTC:
    # the dialog log is correlated with glider-clock (UTC) data, and
    # naive local time goes non-monotonic at DST fall-back.
    def format_time_usec(record: logging.LogRecord, datefmt: str | None = None) -> str:
        import datetime

        dt = datetime.datetime.fromtimestamp(record.created, tz=datetime.UTC)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")

    fmt.formatTime = format_time_usec  # type: ignore[method-assign]

    handlers: list[logging.Handler] = []

    if log_file:
        # WatchedFileHandler reopens the file when logrotate renames
        # it.  A plain FileHandler keeps the rotated inode open
        # forever, silently sending all new dialog — the primary data
        # record — into the file logrotate is about to delete.
        fh = logging.handlers.WatchedFileHandler(log_file, encoding="utf-8")
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

    def _make_logger(suffix: str) -> logging.Logger:
        log = logging.getLogger(f"sfmc.{glider_name}.{suffix}")
        for stale in log.handlers:
            stale.close()  # repeated setup must not leak file handles
        log.handlers.clear()
        log.setLevel(logging.INFO)
        log.propagate = False
        for h in handlers:
            log.addHandler(h)
        return log

    return _make_logger("DIALOG"), _make_logger("SCRIPT")


# ── Monitoring threads ───────────────────────────────────────────────


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
    assembler = LineAssembler()
    try:
        for data in ordered_dialog(sub):
            if stop.is_set():
                break
            for line in assembler.feed(data):
                if line.text:
                    _log_with_time(log, line.text, line.first_seen)
    finally:
        # A shutdown is the one time the unterminated tail is worth
        # keeping: there is no next session to re-send it.  At a stream
        # boundary it is dropped, because a half line logged as a whole
        # one would corrupt the record.
        pending_bytes = len(assembler.pending.encode("utf-8"))
        tail = assembler.take_pending()
        if tail is not None:
            if stop.is_set():
                _log_with_time(log, tail.text, tail.first_seen)
            elif info_log is not None:
                info_log.warning(
                    "stream boundary discarded %d-byte unterminated fragment",
                    pending_bytes,
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
    notifier: DisconnectNotifier | None = None,
) -> None:
    """Monitor live streams until stopped, reconnecting after session loss."""
    stop_event = stop if stop is not None else threading.Event()

    # The startup status check retries like the stream loop does: a
    # service started at boot, before DNS/WAN is up, must not exit on
    # a transient failure that the steady-state loop would have ridden
    # out.  A separate backoff keeps startup failures from inflating
    # the stream loop's delays.
    started = retry_transient(
        lambda: _initial_status(client, glider_name, info_log),
        stop=stop_event,
        backoff=ReconnectBackoff(
            initial_delay=reconnect_initial_delay,
            max_delay=reconnect_max_delay,
            stable_after=reconnect_stable_after,
            jitter=reconnect_jitter,
        ),
        what="startup status check",
        log=info_log,
        notifier=notifier,
        reconnect=reconnect,
    )
    if not started:
        info_log.info("Disconnected.")
        return

    def setup(stomp: StompConnection) -> StreamSession:
        dialog_sub = client.subscribe_glider_output(glider_name, stomp)
        script_sub = client.subscribe_script_events(glider_name, stomp)
        return StreamSession(
            subscriptions=(dialog_sub, script_sub),
            workers=(
                ("dialog", lambda: monitor_dialog(dialog_sub, dialog_log, stop_event, info_log)),
                ("script", lambda: monitor_scripts(script_sub, script_log, stop_event)),
            ),
        )

    def on_subscribed(*, reconnected: bool = False) -> None:
        if not reconnected:
            return
        # State may have moved while we were offline; the log must not
        # imply the script is still whatever it was before the gap.
        try:
            _log_active_script(client, glider_name, info_log, resync=True)
        except SFMCError as exc:
            info_log.warning("script resync failed: %s", safe_stream_error(exc))

    StreamSupervisor(
        client,
        setup=setup,
        stop=stop_event,
        log=info_log,
        notifier=notifier,
        on_subscribed=on_subscribed,
        reconnect=reconnect,
        reconnect_initial_delay=reconnect_initial_delay,
        reconnect_max_delay=reconnect_max_delay,
        reconnect_stable_after=reconnect_stable_after,
        reconnect_jitter=reconnect_jitter,
        worker_join_timeout=worker_join_timeout,
    ).run()

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
    add_notification_cli_args(parser)
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
        # Only set the event: logging from a signal handler can garble
        # a log line the interrupted main thread is mid-way through
        # emitting.  The supervisor loop logs the shutdown itself.
        del signum, frame
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)

    notifier = build_notifier(
        args,
        program="sfmc-monitor-glider",
        glider_name=args.glider_name,
        log=info_log,
    )
    if notifier is not None and args.no_reconnect:
        # The process exits on the first stream loss, long before
        # --notify-after can elapse, so the threshold alert never fires
        # (only a post-alert exit notice could, and it needs an alert
        # first).  Say so loudly instead of silently mailing nothing.
        info_log.warning(
            "--no-reconnect exits on the first stream loss, before "
            "--notify-after can elapse; disconnect emails will "
            "effectively never be sent.  Use your service manager's "
            "failure alerting (e.g. systemd OnFailure=) instead."
        )
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
                notifier=notifier,
            )
    except Exception as exc:
        info_log.error("Error: %s", safe_stream_error(exc))
        if notifier is not None:
            # If a DOWN alert went out, the operator must hear that the
            # watcher itself is quitting (no RECOVERED will follow).
            notifier.record_exit(reason=safe_stream_error(exc))
        sys.exit(1)
    finally:
        if notifier is not None:
            notifier.close()
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    main()
