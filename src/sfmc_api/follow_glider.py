"""Monitor a glider and run a follower plugin that generates files.

``sfmc-follow`` connects to a glider's real-time dialog stream,
parses telemetry from each surfacing (GPS, sensors, timestamps),
passes the data to a user-supplied *follower* class, and uploads
any files the follower generates back to SFMC.

Simulation modes
----------------

Two orthogonal flags control simulation behaviour.  They can be
combined to produce four distinct modes, each useful in a different
stage of development and operations.

``--replay LOGFILE``
    Read dialog lines from a log file (produced by
    ``sfmc-monitor-glider``) instead of connecting to a live STOMP
    stream.  Events are replayed with a configurable delay
    (``--replay-interval``, default 10 s).

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

When to use each mode
~~~~~~~~~~~~~~~~~~~~~

**Replay + print (offline development)**
    Use this when you are *writing* your follower and want to iterate
    quickly.  No SFMC connection or credentials are needed.  Feed a
    recorded dialog log and inspect the files your code generates::

        sfmc-follow --glider osu685 --follower my_follower.py \\
                    --config my_config.yaml \\
                    --replay dialog.log --dry-run

    This is the fastest feedback loop: edit your follower, re-run,
    see the output.  You can capture a dialog log from a live glider
    using ``sfmc-monitor-glider --logfile dialog.log``.

**Replay + upload (integration testing)**
    Use this to verify that your follower correctly uploads files to
    SFMC, without waiting for a real glider to surface.  The dialog
    is read from a log file, but the generated files are actually
    pushed to the SFMC server.  This requires valid SFMC credentials::

        sfmc-follow --glider osu685 --follower my_follower.py \\
                    --config my_config.yaml --replay dialog.log

    Check the SFMC web interface or the glider's ``to-glider`` folder
    listing to confirm the files arrived.

**Live + print only (monitoring without interfering)**
    Use this during a real deployment when you want to *watch* what
    your follower would do, but you are not ready to let it send
    files to the glider.  The follower receives real surfacing data
    from the live STOMP stream, runs your algorithm, and prints the
    files it would generate -- but nothing is uploaded::

        sfmc-follow --glider osu685 --follower my_follower.py \\
                    --config my_config.yaml --dry-run

    This is a good "shadow mode" for the first day of a deployment,
    so you can verify the output before going fully live.

**Live + upload (production)**
    The default mode for operational use.  The follower is connected
    to the live dialog stream and uploads files to SFMC for real::

        sfmc-follow --glider osu685 --follower my_follower.py \\
                    --config my_config.yaml

    Press Ctrl-C to stop.

Programmatic usage
------------------

You can also call the :func:`follow_glider` function directly from
Python, which is useful for integrating into larger scripts or
running multiple followers in one process::

    from sfmc_api import SFMCClient
    from sfmc_api.follow_glider import follow_glider
    from my_follower import MyFollower

    # Live mode with upload.
    with SFMCClient() as client:
        follow_glider(
            client, "osu685", MyFollower,
            follower_config={"key": "val"},
        )

    # Offline replay + dry-run (no client needed).
    follow_glider(
        client=None,
        glider_name="osu685",
        follower_class=MyFollower,
        follower_config={"key": "val"},
        replay="dialog.log",
        dry_run=True,
    )

    # Replay + upload (needs a client for SFMC access).
    with SFMCClient() as client:
        follow_glider(
            client, "osu685", MyFollower,
            follower_config={"key": "val"},
            replay="dialog.log",
        )

    # Stop externally using a threading.Event:
    import threading
    stop = threading.Event()
    # ... in another thread: stop.set()
    follow_glider(client, "osu685", MyFollower, stop=stop)

Press Ctrl-C to stop in any mode.
"""

from __future__ import annotations

import argparse
import logging
import logging.handlers
import queue
import re
import signal
import sys
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue
from typing import Any

from sfmc_api.client import SFMCClient
from sfmc_api.dialog_parser import DialogParser, SurfacingEvent
from sfmc_api.exceptions import SFMCError
from sfmc_api.follower import BaseFollower, load_follower_class
from sfmc_api.monitor_glider import (
    _LINE_SEP,
    _MAX_LINE_BUFFER_BYTES,
    STREAM_BOUNDARY_PREFIX,
    _log_with_time,
    ordered_dialog,
)
from sfmc_api.stomp import StompError, StompSubscription
from sfmc_api.stream_reconnect import ReconnectBackoff, safe_stream_error

logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    """Thread-safe counters describing one ``follow_glider`` run.

    Used to print an end-of-run summary so a non-expert running the
    follower in a terminal can see at a glance whether anything went
    wrong, instead of having to scroll back through the log.
    """

    surfacings: int = 0
    files_emitted: int = 0
    upload_errors: int = 0
    reconnects: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def incr_surfacings(self) -> None:
        with self._lock:
            self.surfacings += 1

    def incr_files(self, n: int = 1) -> None:
        with self._lock:
            self.files_emitted += n

    def incr_upload_errors(self, n: int = 1) -> None:
        with self._lock:
            self.upload_errors += n

    def incr_reconnects(self) -> None:
        with self._lock:
            self.reconnects += 1

    def had_errors(self) -> bool:
        with self._lock:
            return self.upload_errors > 0

    def format(self) -> str:
        with self._lock:
            return (
                f"surfacings={self.surfacings}, "
                f"files_emitted={self.files_emitted}, "
                f"upload_errors={self.upload_errors}, "
                f"reconnects={self.reconnects}"
            )


# ── Log line regex ──────────────────────────────────────────────────

_TS_LOGGER_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T[\d:.]+\s+(\S+)\s{2}(.*)")
_REPLAY_STREAM_BOUNDARY = "\x1eSFMC_STREAM_BOUNDARY\x1e"
_STREAM_BOUNDARY_RE = re.compile(
    rf"^{re.escape(STREAM_BOUNDARY_PREFIX)} session=\d+ reason=[a-z][a-z0-9-]*$"
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
    ts_logger_match = _TS_LOGGER_RE.match(stripped)
    if ts_logger_match:
        logger_name = ts_logger_match.group(1)
        message = ts_logger_match.group(2)
        if (".INFO" in logger_name or ".FOLLOW" in logger_name) and _STREAM_BOUNDARY_RE.fullmatch(
            message
        ):
            return _REPLAY_STREAM_BOUNDARY
        if ".DIALOG" not in logger_name:
            return None
        return message

    if _STREAM_BOUNDARY_RE.fullmatch(stripped):
        return _REPLAY_STREAM_BOUNDARY

    # Raw dialog line (no prefix).
    return stripped


# ── File replay subscription ────────────────────────────────────────


def _file_reader(
    replay_path: str | Path,
    out_queue: Queue[dict[str, Any] | list[Any] | StompError | None],
    stop: threading.Event,
) -> None:
    """Background thread: read a log file and feed lines into a queue.

    Each dialog line is wrapped as ``{"data": "line\\r\\n"}`` — the
    same format that the STOMP subscription produces — so the
    downstream pipeline (:func:`ordered_dialog`, :func:`_read_dialog`)
    works identically for both live and replay modes.

    Non-DIALOG lines (SCRIPT, INFO, etc.) are filtered out here.
    """
    try:
        with open(replay_path, encoding="utf-8", errors="replace") as f:
            for raw_line in f:
                if stop.is_set():
                    break
                dialog_text = _parse_log_line(raw_line)
                if dialog_text is None:
                    continue
                out_queue.put({"data": dialog_text + "\r\n"})
    except Exception as exc:
        out_queue.put(StompError(f"Replay reader failed: {exc}"))
    finally:
        out_queue.put(None)  # Sentinel — signals end of stream.


def _open_replay(
    replay_path: str | Path,
    stop: threading.Event,
) -> tuple[StompSubscription, threading.Thread]:
    """Create a StompSubscription backed by a log-file reader thread.

    Returns the subscription (which can be iterated exactly like a
    live STOMP subscription) and the reader thread (already started).
    """
    q: Queue[dict[str, Any] | list[Any] | StompError | None] = Queue()
    thread = threading.Thread(
        target=_file_reader,
        args=(replay_path, q, stop),
        daemon=True,
        name="file-reader",
    )
    thread.start()
    return StompSubscription("replay-0", "/replay", q), thread


# ── Dialog reader thread (shared by live and replay) ────────────────


class _RecentSurfacingIds:
    """Bounded de-duplication cache for strong surfacing identities."""

    def __init__(self, maxsize: int = 128) -> None:
        self._maxsize = maxsize
        self._ordered: deque[tuple[Any, ...]] = deque()
        self._known: set[tuple[Any, ...]] = set()

    def duplicate_identity(self, event: SurfacingEvent) -> tuple[Any, ...] | None:
        if event.timestamp is not None and event.mission_time is not None:
            identity: tuple[Any, ...] = (
                event.vehicle_name,
                event.timestamp,
                event.mission_time,
            )
        else:
            # Weak fallback: a surfacing whose Curr Time line was
            # dropped or garbled still reproduces the identical raw
            # dialog block when SFMC's resubscribe replay re-delivers
            # it, so its content identifies it.  Treating "no identity"
            # as "not a duplicate" delivered the same surfacing to the
            # follower twice across a reconnect.
            identity = (event.vehicle_name, "raw-lines", hash(tuple(event.raw_lines)))
        if identity in self._known:
            return identity
        if len(self._ordered) >= self._maxsize:
            self._known.remove(self._ordered.popleft())
        self._ordered.append(identity)
        self._known.add(identity)
        return None


def _deliver_surfacing(
    event: SurfacingEvent | None,
    queue_in: Queue[SurfacingEvent | None],
    stats: RunStats | None,
    recent_ids: _RecentSurfacingIds | None,
    info_log: logging.Logger | None,
) -> bool:
    if event is None:
        return False
    duplicate = None if recent_ids is None else recent_ids.duplicate_identity(event)
    if duplicate is not None:
        if info_log is not None:
            info_log.warning("duplicate surfacing suppressed: %s", duplicate)
        return False
    if stats is not None:
        stats.incr_surfacings()
    queue_in.put(event)
    return True


def _finish_dialog_session(
    *,
    buf: str,
    line_start: float,
    parser: DialogParser,
    queue_in: Queue[SurfacingEvent | None],
    dialog_log: logging.Logger | None,
    flush_unterminated: bool,
    stats: RunStats | None,
    recent_ids: _RecentSurfacingIds | None,
    info_log: logging.Logger | None,
) -> None:
    if buf.strip():
        if flush_unterminated:
            if dialog_log:
                _log_with_time(dialog_log, buf, line_start)
            _deliver_surfacing(
                parser.feed_line(buf),
                queue_in,
                stats,
                recent_ids,
                info_log,
            )
        elif info_log is not None:
            info_log.warning(
                "stream boundary discarded %d-byte unterminated fragment",
                len(buf.encode("utf-8")),
            )
    _deliver_surfacing(parser.flush(), queue_in, stats, recent_ids, info_log)
    parser.reset()


def _read_dialog(
    sub: StompSubscription,
    parser: DialogParser,
    queue_in: Queue[SurfacingEvent | None],
    dialog_log: logging.Logger | None,
    stop: threading.Event,
    event_interval: float = 0.0,
    stats: RunStats | None = None,
    recent_ids: _RecentSurfacingIds | None = None,
    info_log: logging.Logger | None = None,
    flush_unterminated: bool = True,
) -> None:
    """Read dialog output, parse surfacings, and feed the follower.

    This function runs in its own thread.  It reassembles fragmented
    dialog lines (same logic as :func:`monitor_glider.monitor_dialog`),
    logs each line, and feeds them to *parser*.  When *parser* produces
    a :class:`SurfacingEvent`, it is put on *queue_in* for the follower.

    The *sub* parameter can be either a live STOMP subscription or a
    replay subscription from :func:`_open_replay` — the downstream
    logic is identical.

    Args:
        sub: A :class:`StompSubscription` (live or replay).
        parser: The :class:`DialogParser` instance.
        queue_in: Queue to deliver :class:`SurfacingEvent` objects.
        dialog_log: Logger for raw dialog lines (or ``None``).
        stop: Event to signal shutdown.
        event_interval: Seconds to wait after each surfacing event
            (used in replay mode to simulate real-time pacing).
    """
    buf = ""
    line_start: float = 0.0

    def process() -> None:
        nonlocal buf, line_start
        for data in ordered_dialog(sub):
            if stop.is_set():
                break

            if not buf:
                line_start = time.time()
            buf += data
            parts = _LINE_SEP.split(buf)
            buf = parts[-1]
            if len(buf) > _MAX_LINE_BUFFER_BYTES:
                logger.warning(
                    "discarding %d bytes of line-break-free dialog data (buffer cap)",
                    len(buf),
                )
                buf = ""

            for line in parts[:-1]:
                delivered = False
                if line == _REPLAY_STREAM_BOUNDARY:
                    delivered = _deliver_surfacing(
                        parser.flush(),
                        queue_in,
                        stats,
                        recent_ids,
                        info_log,
                    )
                    parser.reset()
                elif line:
                    if dialog_log:
                        _log_with_time(dialog_log, line, line_start)
                    delivered = _deliver_surfacing(
                        parser.feed_line(line),
                        queue_in,
                        stats,
                        recent_ids,
                        info_log,
                    )
                if delivered and event_interval > 0 and not stop.is_set():
                    stop.wait(timeout=event_interval)
                line_start = time.time()

    try:
        process()
    except StompError:
        _finish_dialog_session(
            buf=buf,
            line_start=line_start,
            parser=parser,
            queue_in=queue_in,
            dialog_log=dialog_log,
            flush_unterminated=flush_unterminated or stop.is_set(),
            stats=stats,
            recent_ids=recent_ids,
            info_log=info_log,
        )
        raise
    else:
        _finish_dialog_session(
            buf=buf,
            line_start=line_start,
            parser=parser,
            queue_in=queue_in,
            dialog_log=dialog_log,
            flush_unterminated=flush_unterminated or stop.is_set(),
            stats=stats,
            recent_ids=recent_ids,
            info_log=info_log,
        )


# ── Upload thread ───────────────────────────────────────────────────


#: Backoff waits between upload attempts (attempts = len + 1).  Short
#: enough to fit within a surfacing window, long enough to ride out a
#: network blip.
_UPLOAD_RETRY_DELAYS = (10.0, 30.0)


def _upload_files(
    client: SFMCClient,
    glider_name: str,
    queue_out: Queue[dict[str, dict[str, str | bytes]] | None],
    upload_log: logging.Logger,
    stats: RunStats | None = None,
    abort: threading.Event | None = None,
) -> None:
    """Read file dicts from queue_out and upload them to SFMC.

    Runs in its own thread.  Each item from the queue is a dict like
    ``{"to-glider": {"filename": "content"}, "to-science": {...}}``.

    Terminates only on the ``None`` sentinel, not on the shared stop
    event: files queued just before a disconnect or Ctrl-C must still
    be uploaded, so shutdown enqueues the sentinel after the producers
    have drained and this loop works through the backlog first.

    Each upload is retried with short backoff: re-uploading the same
    named file is idempotent, and a network blip while the glider
    waits at the surface must not silently discard the cycle's
    steering files (the glider would fly stale waypoints for the
    whole next dive).  *abort* — set during shutdown — cuts the
    backoff waits short so the drain stays bounded; remaining
    attempts still run, just without the delay.
    """
    while True:
        output = queue_out.get()

        if output is None:
            break

        for folder, files in output.items():
            if not files:
                continue
            filenames = ", ".join(files.keys())
            attempts = 1 + len(_UPLOAD_RETRY_DELAYS)
            for attempt in range(1, attempts + 1):
                try:
                    client.upload_glider_file_contents(glider_name, folder, files)
                except Exception as exc:
                    if attempt >= attempts:
                        upload_log.exception(
                            "Failed to upload to %s after %d attempts: %s",
                            folder,
                            attempt,
                            filenames,
                        )
                        if stats is not None:
                            stats.incr_upload_errors()
                        break
                    delay = _UPLOAD_RETRY_DELAYS[attempt - 1]
                    upload_log.warning(
                        "Upload to %s failed (attempt %d/%d, %s: %s), retrying in %.0fs: %s",
                        folder,
                        attempt,
                        attempts,
                        type(exc).__name__,
                        exc,
                        delay,
                        filenames,
                    )
                    if abort is not None:
                        abort.wait(timeout=delay)
                    else:
                        time.sleep(delay)
                else:
                    upload_log.info(
                        "Uploaded to %s: %s",
                        folder,
                        filenames,
                    )
                    if stats is not None:
                        stats.incr_files(len(files))
                    break


# ── Dry-run output thread ──────────────────────────────────────────


def _print_files(
    queue_out: Queue[dict[str, dict[str, str | bytes]] | None],
    output_log: logging.Logger,
    stats: RunStats | None = None,
) -> None:
    """Print generated files instead of uploading them.

    Used in ``--dry-run`` mode.  For each file the follower produces,
    logs the folder, filename, and content.

    Terminates only on the ``None`` sentinel, like
    :func:`_upload_files`, so queued output is never discarded.
    """
    while True:
        output = queue_out.get()

        if output is None:
            break

        for folder, files in output.items():
            if not files:
                continue
            for filename, content in files.items():
                if isinstance(content, bytes):
                    byte_count = len(content)
                    display = content.decode("utf-8", errors="replace")
                else:
                    byte_count = len(content.encode("utf-8"))
                    display = content
                output_log.info(
                    "[dry-run] %s/%s (%d bytes):\n%s",
                    folder,
                    filename,
                    byte_count,
                    display,
                )
                if stats is not None:
                    stats.incr_files()


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

        dt = datetime.datetime.fromtimestamp(record.created, tz=datetime.UTC)
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
        for stale in log.handlers:
            # The documented multiple-followers-in-one-process usage
            # calls setup repeatedly; clearing without closing leaks a
            # file handle per call.
            stale.close()
        log.handlers.clear()
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


@dataclass(frozen=True)
class _ThreadResult:
    name: str
    error: Exception | None


def _run_thread_target(
    name: str,
    target: Callable[..., None],
    args: tuple[Any, ...],
    results: queue.Queue[_ThreadResult],
) -> None:
    try:
        target(*args)
    except Exception as exc:
        results.put(_ThreadResult(name, exc))
    else:
        results.put(_ThreadResult(name, None))


def _pipeline_health(
    follower: BaseFollower,
    output_thread: threading.Thread,
    output_results: queue.Queue[_ThreadResult],
) -> None:
    if not follower.is_alive():
        raise RuntimeError("Follower thread died")
    if output_thread.is_alive():
        return
    try:
        result = output_results.get_nowait()
    except queue.Empty as exc:
        raise RuntimeError("Output worker died without reporting a result") from exc
    output_results.put(result)
    if result.error is not None:
        raise RuntimeError(
            f"Output worker failed: {safe_stream_error(result.error)}"
        ) from result.error
    raise RuntimeError("Output worker exited unexpectedly")


def _wait_for_reconnect(
    stop: threading.Event,
    delay: float,
    follower: BaseFollower,
    output_thread: threading.Thread,
    output_results: queue.Queue[_ThreadResult],
) -> bool:
    deadline = time.monotonic() + delay
    while not stop.is_set():
        _pipeline_health(follower, output_thread, output_results)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        if stop.wait(min(0.5, remaining)):
            return True
    return True


def _run_live_dialog_sessions(
    *,
    client: SFMCClient,
    glider_name: str,
    queue_in: Queue[SurfacingEvent | None],
    dialog_log: logging.Logger,
    info_log: logging.Logger,
    stop: threading.Event,
    stats: RunStats,
    recent_ids: _RecentSurfacingIds,
    follower: BaseFollower,
    output_thread: threading.Thread,
    output_results: queue.Queue[_ThreadResult],
    reconnect: bool,
    reconnect_initial_delay: float,
    reconnect_max_delay: float,
    reconnect_stable_after: float,
    reconnect_jitter: float,
    worker_join_timeout: float,
) -> None:
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
                parser = DialogParser()
                results: queue.Queue[_ThreadResult] = queue.Queue()
                dialog_thread = threading.Thread(
                    target=_run_thread_target,
                    args=(
                        "dialog",
                        _read_dialog,
                        (
                            dialog_sub,
                            parser,
                            queue_in,
                            dialog_log,
                            stop,
                            0.0,
                            stats,
                            recent_ids,
                            info_log,
                            False,
                        ),
                        results,
                    ),
                    daemon=True,
                    name="dialog-reader",
                )
                dialog_thread.start()
                subscribed_at = time.monotonic()
                session_number += 1
                info_log.info("stream session %d subscribed", session_number)
                if attempt_number > 1:
                    stats.incr_reconnects()
                if offline_since is not None:
                    info_log.info(
                        "stream session %d reconnected after %.1fs offline",
                        session_number,
                        subscribed_at - offline_since,
                    )
                    offline_since = None

                first_result: _ThreadResult | None = None
                try:
                    while not stop.is_set():
                        _pipeline_health(follower, output_thread, output_results)
                        try:
                            first_result = results.get(timeout=0.5)
                            break
                        except queue.Empty:
                            continue
                finally:
                    dialog_sub.close()
                    dialog_thread.join(timeout=worker_join_timeout)
                    if dialog_thread.is_alive():
                        raise RuntimeError("dialog reader did not stop after subscription close")

                if first_result is None:
                    try:
                        first_result = results.get_nowait()
                    except queue.Empty:
                        first_result = None
                if first_result is not None and first_result.error is not None:
                    if isinstance(first_result.error, StompError):
                        failure = first_result.error
                        reason = "stomp-error"
                    else:
                        raise RuntimeError(
                            f"dialog reader failed: {safe_stream_error(first_result.error)}"
                        ) from first_result.error
        except SFMCError as exc:
            failure = exc
            reason = "session-error"

        if stop.is_set():
            return

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
        if _wait_for_reconnect(
            stop,
            delay.actual,
            follower,
            output_thread,
            output_results,
        ):
            return


def _run_replay_dialog(
    *,
    replay: str,
    replay_interval: float,
    queue_in: Queue[SurfacingEvent | None],
    dialog_log: logging.Logger,
    info_log: logging.Logger,
    stop: threading.Event,
    stats: RunStats,
    recent_ids: _RecentSurfacingIds,
    follower: BaseFollower,
    output_thread: threading.Thread,
    output_results: queue.Queue[_ThreadResult],
    worker_join_timeout: float,
) -> None:
    dialog_sub, file_reader_thread = _open_replay(replay, stop)
    results: queue.Queue[_ThreadResult] = queue.Queue()
    dialog_thread = threading.Thread(
        target=_run_thread_target,
        args=(
            "dialog",
            _read_dialog,
            (
                dialog_sub,
                DialogParser(),
                queue_in,
                dialog_log,
                stop,
                replay_interval,
                stats,
                recent_ids,
                info_log,
                True,
            ),
            results,
        ),
        daemon=True,
        name="dialog-reader",
    )
    dialog_thread.start()
    try:
        while not stop.is_set() and dialog_thread.is_alive():
            _pipeline_health(follower, output_thread, output_results)
            stop.wait(0.5)
    finally:
        dialog_sub.close()
        dialog_thread.join(timeout=worker_join_timeout)
        file_reader_thread.join(timeout=worker_join_timeout)
    if dialog_thread.is_alive() or file_reader_thread.is_alive():
        raise RuntimeError("replay reader did not stop")
    try:
        result = results.get_nowait()
    except queue.Empty as exc:
        raise RuntimeError("dialog reader ended without reporting a result") from exc
    if result.error is not None:
        raise RuntimeError(f"Replay failed: {safe_stream_error(result.error)}") from result.error
    if not stop.is_set():
        info_log.info("Replay exhausted, draining pipeline...")


def _shutdown_follow_pipeline(
    *,
    follower: BaseFollower,
    queue_out: Queue[dict[str, dict[str, str | bytes]] | None],
    output_thread: threading.Thread,
    output_results: queue.Queue[_ThreadResult],
    info_log: logging.Logger,
    upload_abort: threading.Event | None = None,
    drain_timeout: float = 60.0,
) -> None:
    follower.shutdown()
    deadline = time.monotonic() + drain_timeout
    warning_at = time.monotonic() + 10.0
    while follower.is_alive():
        follower.join(timeout=1.0)
        now = time.monotonic()
        if not follower.is_alive():
            break
        if now >= deadline:
            # A follower stuck in user code (hung HTTP call in
            # on_surfacing) must not hang shutdown forever — signals
            # only set the already-set stop event, so without this
            # bound only SIGKILL ends the process.  Follower threads
            # are daemons; abandoning the drain loses at most output
            # it would have produced after this point, and says so.
            info_log.error(
                "follower still running after %.0fs drain timeout; "
                "abandoning it — output produced after this point is lost",
                drain_timeout,
            )
            break
        if now >= warning_at:
            info_log.warning("still waiting for follower to finish before closing output")
            warning_at = now + 10.0

    if upload_abort is not None:
        # Cut retry backoffs short; queued files still get attempted.
        upload_abort.set()
    queue_out.put(None)
    output_thread.join(timeout=45.0)
    if output_thread.is_alive():
        raise RuntimeError("output worker did not stop after drain sentinel")
    try:
        result = output_results.get_nowait()
    except queue.Empty as exc:
        raise RuntimeError("output worker ended without reporting a result") from exc
    if result.error is not None:
        raise RuntimeError(
            f"Output worker failed: {safe_stream_error(result.error)}"
        ) from result.error


def follow_glider(
    client: SFMCClient | None,
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
    stats: RunStats | None = None,
    *,
    reconnect: bool = True,
    reconnect_initial_delay: float = 15.0,
    reconnect_max_delay: float = 300.0,
    reconnect_stable_after: float = 60.0,
    reconnect_jitter: float = 0.2,
    worker_join_timeout: float = 5.0,
) -> RunStats:
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
        stats: Optional :class:`RunStats` to accumulate into.  A new
            one is created if not supplied.  Either way the populated
            instance is returned for inspection.
        reconnect: Reconnect live streams after an expected session failure.
        reconnect_initial_delay: Initial nominal retry delay in seconds.
        reconnect_max_delay: Maximum nominal and jittered retry delay.
        reconnect_stable_after: Subscribed uptime required to reset backoff.
        reconnect_jitter: Symmetric jitter fraction applied to retry delays.
        worker_join_timeout: Maximum time to join a session-scoped input worker.

    Returns:
        A :class:`RunStats` summarising the run (surfacings,
        files emitted, upload errors, and successful reconnections).
    """
    if stop is None:
        stop = threading.Event()
    if stats is None:
        stats = RunStats()

    dialog_log, upload_log, info_log = setup_logging(
        glider_name,
        log_file,
        log_level,
        log_max_bytes,
        log_backup_count,
    )

    # ── Validate arguments ──────────────────────────────────────
    if not replay and client is None:
        raise ValueError("client is required for live mode")

    if replay and not dry_run and client is None:
        raise ValueError("client is required for replay + upload mode")

    # ── Verify glider exists (skip in offline replay) ───────────
    if client is not None and not replay:
        # Retried like the session loop: a service started at boot,
        # before DNS/WAN is up, must not exit on a transient failure
        # the steady-state supervisor would have ridden out.
        startup_backoff = ReconnectBackoff(
            initial_delay=reconnect_initial_delay,
            max_delay=reconnect_max_delay,
            stable_after=reconnect_stable_after,
            jitter=reconnect_jitter,
        )
        while True:
            try:
                details = client.get_glider_details(glider_name)
                break
            except SFMCError as exc:
                if not reconnect:
                    raise
                delay = startup_backoff.next_delay(subscribed_uptime=None)
                info_log.warning(
                    "startup glider check failed (%s); retrying in %.1fs",
                    safe_stream_error(exc),
                    delay.actual,
                )
                if stop.wait(delay.actual):
                    return stats
        try:
            glider_state = details["data"]["state"]
        except (KeyError, TypeError) as exc:
            raise SFMCError(f"Unexpected glider-details response: {exc}") from exc
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

    if replay and not Path(replay).is_file():
        raise FileNotFoundError(f"Replay file not found: {replay}")

    # ── Set up persistent queues and workers ───────────────────
    queue_in: Queue[SurfacingEvent | None] = Queue()
    queue_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()

    follower = follower_class(
        config=follower_config or {},
        queue_in=queue_in,
        queue_out=queue_out,
    )
    info_log.info("Follower: %s", type(follower).__name__)
    recent_ids = _RecentSurfacingIds()
    output_results: queue.Queue[_ThreadResult] = queue.Queue()
    upload_abort = threading.Event()
    if dry_run:
        output_thread = threading.Thread(
            target=_run_thread_target,
            args=("output", _print_files, (queue_out, upload_log, stats), output_results),
            daemon=True,
            name="dry-run-printer",
        )
    else:
        assert client is not None
        output_thread = threading.Thread(
            target=_run_thread_target,
            args=(
                "output",
                _upload_files,
                (client, glider_name, queue_out, upload_log, stats, upload_abort),
                output_results,
            ),
            daemon=True,
            name="file-uploader",
        )

    follower_started = False
    output_started = False
    try:
        follower.start()
        follower_started = True
        output_thread.start()
        output_started = True

        mode_desc = "replay" if replay else "live"
        if dry_run:
            mode_desc += " + dry-run"
        info_log.info("Pipeline started (%s). Press Ctrl-C to stop.", mode_desc)
        try:
            if replay:
                info_log.info("Replaying from %s", replay)
                _run_replay_dialog(
                    replay=replay,
                    replay_interval=replay_interval,
                    queue_in=queue_in,
                    dialog_log=dialog_log,
                    info_log=info_log,
                    stop=stop,
                    stats=stats,
                    recent_ids=recent_ids,
                    follower=follower,
                    output_thread=output_thread,
                    output_results=output_results,
                    worker_join_timeout=worker_join_timeout,
                )
            else:
                assert client is not None
                _run_live_dialog_sessions(
                    client=client,
                    glider_name=glider_name,
                    queue_in=queue_in,
                    dialog_log=dialog_log,
                    info_log=info_log,
                    stop=stop,
                    stats=stats,
                    recent_ids=recent_ids,
                    follower=follower,
                    output_thread=output_thread,
                    output_results=output_results,
                    reconnect=reconnect,
                    reconnect_initial_delay=reconnect_initial_delay,
                    reconnect_max_delay=reconnect_max_delay,
                    reconnect_stable_after=reconnect_stable_after,
                    reconnect_jitter=reconnect_jitter,
                    worker_join_timeout=worker_join_timeout,
                )
        except KeyboardInterrupt:
            info_log.info("Stopping...")
            stop.set()
    finally:
        if follower_started and output_started:
            info_log.info("stopping; draining follower/output queues")
            _shutdown_follow_pipeline(
                follower=follower,
                queue_out=queue_out,
                output_thread=output_thread,
                output_results=output_results,
                info_log=info_log,
                upload_abort=upload_abort,
            )
        elif follower_started:
            follower.shutdown()
            follower.join(timeout=5.0)

    info_log.info("Done. %s", stats.format())
    return stats


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
    sim.add_argument(
        "--strict",
        action="store_true",
        default=False,
        help="Exit with non-zero status if any upload error occurred",
    )
    sim.add_argument(
        "--no-reconnect",
        action="store_true",
        default=False,
        help="Exit non-zero if the live stream disconnects",
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

    stats: RunStats | None = None
    stop = threading.Event()
    previous_handlers: dict[signal.Signals, Any] = {}

    def request_stop(signum: int, frame: Any) -> None:
        del signum, frame
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[signum] = signal.getsignal(signum)
        signal.signal(signum, request_stop)

    try:
        try:
            if need_client:
                with SFMCClient(
                    host=args.hostname,
                    config_path=args.credentials,
                ) as client:
                    stats = follow_glider(
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
                        stop=stop,
                        reconnect=not args.no_reconnect,
                    )
            else:
                # Fully offline: replay + dry-run, no client needed.
                stats = follow_glider(
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
                    stop=stop,
                    reconnect=not args.no_reconnect,
                )
        except KeyboardInterrupt:
            stop.set()
            sys.stderr.write("\nStopped.\n")
        except Exception as exc:
            sys.stderr.write(f"Error: {safe_stream_error(exc)}\n")
            sys.exit(1)
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)

    if args.strict and stats is not None and stats.had_errors():
        sys.stderr.write(f"--strict: exiting non-zero ({stats.format()})\n")
        sys.exit(2)


if __name__ == "__main__":
    main()
