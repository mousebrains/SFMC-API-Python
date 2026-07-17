#!/usr/bin/env python3
"""Email alerting for sustained loss of the SFMC connection.

The long-running stream commands (``sfmc-follow``,
``sfmc-monitor-glider``, ``sfmc-pull-new-downloads``) each ride out a
dropped STOMP connection to the SFMC server by reconnecting with
backoff — most drops self-heal within seconds and are not worth
waking anyone.  :class:`DisconnectNotifier` turns a *sustained* outage
into an email: it starts a timer when the connection goes down, and if
the connection has not come back before the timer expires, it sends an
alert to one or more recipients.  While the outage persists it can
re-send reminders on a fixed cadence, and when the connection returns
it sends a single all-clear.

Design notes:

* The supervisor threads only ever make cheap, lock-guarded edge
  calls — :meth:`record_disconnect`, :meth:`record_connect`, and
  :meth:`record_exit` — so a slow or dead mail server can never stall
  a reconnect loop.
* All timing lives in an injectable timer (``threading.Timer`` by
  default), so the threshold fires precisely rather than at
  backoff-loop granularity, and unit tests can drive it deterministically.
* All email sending happens on a dedicated daemon thread draining a
  queue, so :func:`smtplib.SMTP` latency is off the caller's path.
  A failed send is retried a few times before the message is dropped.
* A drop that recovers before the threshold (a *flap*) sends nothing.
* A reconnect only ends an outage after the new session survives
  ``stable_seconds`` — a stream that subscribes and then dies seconds
  later, over and over, keeps counting as one continuous outage
  instead of resetting the clock on every short-lived session.
* Timer callbacks race with state transitions (a reconnect can land
  between a timer expiring and its callback acquiring the lock), so
  every armed timer captures a generation number and a stale callback
  is ignored.

Beyond connection outages, application logic can email the operator at
its own discretion via :meth:`DisconnectNotifier.notify_event` — e.g. a
follower whose external float-position feed has gone quiet, or that
cannot generate an ``.ma`` file.  Events share the same delivery
machinery and are rate-limited per condition key.
"""

from __future__ import annotations

import argparse
import logging
import queue
import smtplib
import socket
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Protocol

__all__ = [
    "DisconnectNotifier",
    "add_notification_cli_args",
    "build_notifier",
    "make_smtp_send",
]

logger = logging.getLogger(__name__)

#: Uptime a new session must survive before it ends the outage episode.
#: Matches the reconnect supervisors' ``stable_after`` policy.
DEFAULT_STABLE_SECONDS = 60.0

#: Smallest allowed nonzero ``--notify-repeat``.  Anything shorter is a
#: mail storm during a long outage, never a deliberate choice.
_MIN_REPEAT_SECONDS = 60.0

#: Cap on distinct :meth:`DisconnectNotifier.notify_event` keys tracked
#: for rate limiting.  Far above any sane static condition set.
_MAX_EVENT_KEYS = 256


class _TimerLike(Protocol):
    """The slice of ``threading.Timer`` the notifier relies on."""

    def start(self) -> None: ...

    def cancel(self) -> None: ...


#: ``(subject, body)`` -> deliver it.  Injected so tests never touch SMTP.
SendFn = Callable[[str, str], None]
#: ``(interval_seconds, callback)`` -> an armed-but-not-started timer.
TimerFactory = Callable[[float, Callable[[], None]], _TimerLike]

_SENTINEL = ("", "")  # queue marker: sender thread should exit


class DisconnectNotifier:
    """Send email when the SFMC connection stays down past a threshold.

    Thread-safe.  The owning program calls :meth:`record_connect` each
    time a stream session subscribes and :meth:`record_disconnect` each
    time a session ends; the notifier decides when (and whether) to
    email.  Call :meth:`close` at shutdown to stop the timers and flush
    any queued mail.
    """

    def __init__(
        self,
        *,
        send_fn: SendFn,
        threshold_seconds: float,
        repeat_seconds: float,
        subject_prefix: str,
        program: str,
        glider_name: str,
        host: str | None = None,
        log: logging.Logger | None = None,
        stable_seconds: float = DEFAULT_STABLE_SECONDS,
        send_attempts: int = 3,
        send_retry_delay: float = 30.0,
        timer_factory: TimerFactory = threading.Timer,
        monotonic: Callable[[], float] = time.monotonic,
        now_utc: Callable[[], datetime] = lambda: datetime.now(UTC),
        close_join_timeout: float = 15.0,
    ) -> None:
        """Create a notifier and start its background sender thread.

        Args:
            send_fn: Delivers one ``(subject, body)`` message.  Called
                only from the sender thread.
            threshold_seconds: How long the connection must stay down
                before the first alert.  ``0`` alerts immediately.
            repeat_seconds: Re-send a reminder every this many seconds
                while still down.  ``0`` sends a single alert per outage.
            subject_prefix: Leading tag for every subject line.
            program: Program label (e.g. ``"sfmc-follow"``) for the body.
            glider_name: Glider this notifier is watching.
            host: Hostname shown in the message; defaults to this host.
            log: Logger for send failures; defaults to the module logger.
            stable_seconds: Uptime a reconnected session must survive
                before the outage episode ends.  ``0`` ends it on the
                subscribe itself (the pre-flap-hardening behaviour).
            send_attempts: Delivery attempts per message before it is
                dropped with a logged warning.
            send_retry_delay: Seconds between delivery attempts.
            timer_factory: Builds the timers (injectable for tests).
            monotonic: Monotonic clock (injectable for tests).
            now_utc: Wall-clock source for message timestamps.
            close_join_timeout: Seconds :meth:`close` waits for the
                sender thread to flush queued mail.
        """
        if threshold_seconds < 0:
            raise ValueError("threshold_seconds must be >= 0")
        if repeat_seconds < 0:
            raise ValueError("repeat_seconds must be >= 0")
        if stable_seconds < 0:
            raise ValueError("stable_seconds must be >= 0")
        if send_attempts < 1:
            raise ValueError("send_attempts must be >= 1")
        if send_retry_delay < 0:
            raise ValueError("send_retry_delay must be >= 0")
        self._send_fn = send_fn
        self._threshold = threshold_seconds
        self._repeat = repeat_seconds
        self._stable = stable_seconds
        self._send_attempts = send_attempts
        self._send_retry_delay = send_retry_delay
        self._prefix = subject_prefix
        self._program = program
        self._glider = glider_name
        self._host = host if host is not None else socket.gethostname()
        self._log = log if log is not None else logger
        self._timer_factory = timer_factory
        self._monotonic = monotonic
        self._now_utc = now_utc
        self._close_join_timeout = close_join_timeout

        self._lock = threading.Lock()
        self._down_since: float | None = None  # monotonic; None => connected
        self._down_wall: datetime | None = None  # wall-clock of the drop
        self._up_since: float | None = None  # monotonic; reconnect pending stability
        self._last_reason: str | None = None
        self._alerts_sent = 0  # alerts emitted this outage episode
        self._alert_timer: _TimerLike | None = None
        self._alert_gen = 0  # bumped when outstanding alert timers go stale
        self._pending_up: _TimerLike | None = None
        self._pending_gen = 0  # bumped when outstanding stability timers go stale
        self._event_last_sent: dict[str, float] = {}  # notify_event rate limiting
        self._closed = False

        self._stop_sending = threading.Event()
        self._queue: queue.Queue[tuple[str, str]] = queue.Queue()
        self._sender = threading.Thread(
            target=self._sender_loop,
            name=f"disconnect-notifier-{glider_name}",
            daemon=True,
        )
        self._sender.start()

    # ── Edge calls from the supervisor ───────────────────────────────

    def record_disconnect(self, reason: str | None = None) -> None:
        """Note that the SFMC connection is down.

        The first call of an outage episode starts the alert timer;
        later calls only refresh the reason string.  A call that lands
        while a reconnect is still inside its stability window cancels
        the window: the outage episode continues uninterrupted, so
        flapping sessions cannot reset the clock.  Safe to call on
        every failed reconnect attempt.
        """
        with self._lock:
            if self._closed:
                return
            self._last_reason = reason
            if self._pending_up is not None:
                # The reconnect died before proving stable: same outage.
                self._cancel_pending_locked()
                return
            if self._down_since is not None:
                return  # already tracking this outage
            self._down_since = self._monotonic()
            self._down_wall = self._now_utc()
            self._alerts_sent = 0
            self._arm_alert_locked(self._threshold)

    def record_connect(self) -> None:
        """Note that a stream session subscribed (connection is up).

        A no-op if the connection was not considered down.  The outage
        episode only ends once the session survives ``stable_seconds``;
        until then alerts keep firing on schedule.  If an alert was
        sent during the outage, a single all-clear follows the
        confirmation.
        """
        with self._lock:
            if self._closed or self._down_since is None:
                return  # was already up (or shut down); nothing to clear
            if self._pending_up is not None:
                return  # already waiting for this reconnect to prove stable
            self._up_since = self._monotonic()
            if self._stable <= 0:
                self._confirm_up_locked()
                return
            self._pending_gen += 1
            gen = self._pending_gen
            self._pending_up = self._timer_factory(
                self._stable,
                lambda: self._confirm_up(gen),
            )
            self._pending_up.start()

    def record_exit(self, reason: str | None = None) -> None:
        """Queue a final notice: the process is exiting while still down.

        Only sends when a DOWN alert was already emitted for the
        current outage — the operator was told the connection is down,
        so they must also be told the process stopped watching it (the
        all-clear will never come).  Short-lived processes that exit
        before the threshold stay silent, so a crash loop under a
        service manager cannot storm the mailbox.
        """
        with self._lock:
            if self._closed or self._down_since is None or self._alerts_sent == 0:
                return
            if reason is not None:
                self._last_reason = reason
            downtime = self._monotonic() - self._down_since
            self._queue.put(self._exit_message(downtime))

    # ── Discretionary events (followers / application logic) ─────────

    def notify_event(
        self,
        key: str,
        summary: str,
        detail: str = "",
        *,
        min_gap_seconds: float = 900.0,
    ) -> bool:
        """Queue an operator notification at the caller's discretion.

        For conditions only the application logic can see — an external
        float-position feed going quiet, a follower unable to generate
        an ``.ma`` file, a value out of physical range.  Delivery uses
        the same background sender (non-blocking, retried) as the
        disconnect alerts.

        Repeats of the same *key* within *min_gap_seconds* are dropped,
        so a condition rechecked every surfacing (or every loop
        iteration) costs one email per window, not one per check.  Keys
        should be a small, static set of condition names — they are
        remembered for rate limiting.

        Args:
            key: Stable identifier of the condition (e.g.
                ``"float-feed-down"``).  Rate limiting is per key.
            summary: One line for the subject.
            detail: Optional body text; the subject line is used if empty.
            min_gap_seconds: Minimum spacing between emails for this
                key (default 900 = 15 min).

        Returns:
            ``True`` if the message was queued, ``False`` if it was
            rate-limited or the notifier is closed.
        """
        with self._lock:
            if self._closed:
                return False
            now = self._monotonic()
            last = self._event_last_sent.get(key)
            if last is not None and now - last < min_gap_seconds:
                return False
            self._event_last_sent[key] = now
            # Bound the rate-limit table: dynamic keys are a caller bug,
            # but they must cost dropped dedup state, not memory growth.
            if len(self._event_last_sent) > _MAX_EVENT_KEYS:
                oldest = min(self._event_last_sent, key=self._event_last_sent.__getitem__)
                del self._event_last_sent[oldest]
                self._log.warning(
                    "notify_event key table full (%d); dropping dedup state for %r",
                    _MAX_EVENT_KEYS,
                    oldest,
                )
            self._queue.put(self._event_message(key, summary, detail))
            return True

    def close(self) -> None:
        """Stop the timers and flush queued mail, then join the sender."""
        with self._lock:
            self._closed = True
            self._cancel_alert_locked()
            self._cancel_pending_locked()
        self._stop_sending.set()
        self._queue.put(_SENTINEL)
        self._sender.join(timeout=self._close_join_timeout)

    # ── Internals ────────────────────────────────────────────────────

    def _arm_alert_locked(self, interval: float) -> None:
        """Start a fresh alert timer.  Caller holds ``self._lock``."""
        gen = self._alert_gen
        self._alert_timer = self._timer_factory(interval, lambda: self._fire(gen))
        self._alert_timer.start()

    def _cancel_alert_locked(self) -> None:
        """Cancel/invalidate any alert timer.  Caller holds ``self._lock``."""
        self._alert_gen += 1  # a fired-but-not-yet-run callback must not act
        if self._alert_timer is not None:
            self._alert_timer.cancel()
            self._alert_timer = None

    def _cancel_pending_locked(self) -> None:
        """Cancel/invalidate any stability timer.  Caller holds ``self._lock``."""
        self._pending_gen += 1
        if self._pending_up is not None:
            self._pending_up.cancel()
            self._pending_up = None
        self._up_since = None

    def _fire(self, gen: int) -> None:
        """Alert-timer callback: emit an alert if still down, then re-arm."""
        with self._lock:
            # A confirmation or close that landed between the timer
            # expiring and this callback acquiring the lock invalidates
            # the generation; a stale callback must not alert a new
            # (or no) episode.
            if self._closed or gen != self._alert_gen or self._down_since is None:
                return
            self._alerts_sent += 1
            first = self._alerts_sent == 1
            downtime = self._monotonic() - self._down_since
            self._queue.put(self._alert_message(first=first, downtime=downtime))
            if self._repeat > 0:
                self._arm_alert_locked(self._repeat)
            else:
                self._alert_timer = None

    def _confirm_up(self, gen: int) -> None:
        """Stability-timer callback: the reconnect survived long enough."""
        with self._lock:
            if self._closed or gen != self._pending_gen or self._down_since is None:
                return
            self._confirm_up_locked()

    def _confirm_up_locked(self) -> None:
        """End the outage episode.  Caller holds ``self._lock``.

        The outage is credited as ending at the reconnect itself
        (``_up_since``), not at confirmation — the stability window is
        observation delay, not downtime.
        """
        up_at = self._up_since if self._up_since is not None else self._monotonic()
        downtime = up_at - (self._down_since or up_at)
        alerted = self._alerts_sent
        # Build the all-clear before clearing state it reads.
        message = self._recovered_message(downtime) if alerted else None
        self._cancel_alert_locked()
        self._cancel_pending_locked()
        self._down_since = None
        self._down_wall = None
        self._last_reason = None
        self._alerts_sent = 0
        if message is not None:
            self._queue.put(message)

    def _sender_loop(self) -> None:
        """Drain the queue, delivering each message via ``send_fn``.

        Each message gets ``send_attempts`` tries ``send_retry_delay``
        apart — a briefly-restarting local MTA must not eat the one
        alert this whole feature exists to deliver.  A message is only
        dropped (with a logged warning) once its attempts are spent or
        shutdown interrupts the retry wait; a failure never kills the
        thread, so later messages still go out.
        """
        while True:
            subject, body = self._queue.get()
            if not subject and not body:  # sentinel
                return
            for attempt in range(1, self._send_attempts + 1):
                try:
                    self._send_fn(subject, body)
                    break
                except Exception as exc:
                    if attempt == self._send_attempts:
                        self._log.warning(
                            "disconnect email dropped after %d attempt(s): %s",
                            attempt,
                            exc,
                        )
                        break
                    self._log.warning(
                        "disconnect email send failed (attempt %d/%d): %s; retrying in %.0fs",
                        attempt,
                        self._send_attempts,
                        exc,
                        self._send_retry_delay,
                    )
                    if self._stop_sending.wait(self._send_retry_delay):
                        self._log.warning("shutdown during email retry; message dropped")
                        break

    # ── Message bodies ───────────────────────────────────────────────

    def _where(self) -> str:
        return f"{self._program} on {self._host}"

    def _body_facts(self, downtime: float) -> str:
        return (
            f"Program:    {self._program}\n"
            f"Host:       {self._host}\n"
            f"Glider:     {self._glider}\n"
            f"Down since: {_fmt(self._down_wall)} UTC\n"
            f"Downtime:   {downtime / 60.0:.1f} min\n"
            f"Last error: {self._last_reason or 'normal session close'}\n"
        )

    def _alert_message(self, *, first: bool, downtime: float) -> tuple[str, str]:
        mins = downtime / 60.0
        if first:
            subject = f"{self._prefix} {self._glider}: SFMC connection DOWN ({self._where()})"
            headline = f"The SFMC connection for {self._glider} has been down for {mins:.1f} min."
        else:
            subject = (
                f"{self._prefix} {self._glider}: SFMC connection STILL DOWN "
                f"after {mins:.0f} min ({self._where()})"
            )
            headline = (
                f"The SFMC connection for {self._glider} is still down after {mins:.1f} min."
            )
        return subject, f"{headline}\n\n{self._body_facts(downtime)}"

    def _recovered_message(self, downtime: float) -> tuple[str, str]:
        mins = downtime / 60.0
        subject = f"{self._prefix} {self._glider}: SFMC connection RECOVERED ({self._where()})"
        body = (
            f"The SFMC connection for {self._glider} recovered after {mins:.1f} min down.\n\n"
            f"Program:      {self._program}\n"
            f"Host:         {self._host}\n"
            f"Glider:       {self._glider}\n"
            f"Was down:     {_fmt(self._down_wall)} UTC\n"
            f"Total downtime: {mins:.1f} min\n"
        )
        return subject, body

    def _event_message(self, key: str, summary: str, detail: str) -> tuple[str, str]:
        subject = f"{self._prefix} {self._glider}: {summary} ({self._where()})"
        body = (
            f"{detail or summary}\n\n"
            f"Condition:  {key}\n"
            f"Program:    {self._program}\n"
            f"Host:       {self._host}\n"
            f"Glider:     {self._glider}\n"
            f"Time:       {_fmt(self._now_utc())} UTC\n"
        )
        return subject, body

    def _exit_message(self, downtime: float) -> tuple[str, str]:
        subject = (
            f"{self._prefix} {self._glider}: {self._program} EXITING — "
            f"SFMC connection still DOWN ({self._host})"
        )
        body = (
            f"{self._program} is exiting while the SFMC connection for "
            f"{self._glider} is still down ({downtime / 60.0:.1f} min).\n\n"
            f"{self._body_facts(downtime)}"
            "\n"
            "No further email will come from this process — in particular,\n"
            "no RECOVERED notice.  Check the service manager for restart\n"
            "status.\n"
        )
        return subject, body


def _fmt(when: datetime | None) -> str:
    """Format a wall-clock timestamp, tolerating ``None``."""
    if when is None:
        return "unknown"
    return when.strftime("%Y-%m-%d %H:%M:%S")


# ── SMTP delivery ─────────────────────────────────────────────────────


def make_smtp_send(
    *,
    host: str,
    port: int,
    sender: str | None,
    recipients: list[str],
    timeout: float,
    program: str = "sfmc",
) -> SendFn:
    """Return a ``send_fn`` that delivers one message over SMTP.

    Targets a local relay (``localhost:25`` by default, no auth/TLS),
    the standard setup on a Debian/Ubuntu host forwarding to a campus
    mail server.  Each call opens and closes its own connection —
    disconnect emails are rare, so pooling would only add state.

    When *sender* is ``None`` the From address defaults to
    ``<program>@<fqdn>``, computed lazily on the sender thread at send
    time: ``socket.getfqdn()`` can block for seconds on misconfigured
    DNS, and that stall must not land on the program's startup path.
    """

    def send(subject: str, body: str) -> None:
        msg = EmailMessage()
        msg["From"] = sender or f"{program}@{socket.getfqdn()}"
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        msg["Message-ID"] = make_msgid()
        msg.set_content(body)
        with smtplib.SMTP(host, port, timeout=timeout) as smtp:
            smtp.send_message(msg)

    return send


# ── CLI wiring ────────────────────────────────────────────────────────


def _nonneg_float(value: str) -> float:
    f = float(value)
    if not (f >= 0 and f != float("inf")):
        raise argparse.ArgumentTypeError("must be a finite number of seconds >= 0")
    return f


def _positive_float(value: str) -> float:
    f = float(value)
    if not (f > 0 and f != float("inf")):
        raise argparse.ArgumentTypeError("must be a positive number of seconds")
    return f


def _repeat_secs(value: str) -> float:
    """argparse type for ``--notify-repeat``: 0, or >= the storm floor."""
    f = _nonneg_float(value)
    if 0 < f < _MIN_REPEAT_SECONDS:
        raise argparse.ArgumentTypeError(
            f"must be 0 (single alert per outage) or >= {_MIN_REPEAT_SECONDS:.0f} seconds"
        )
    return f


def _email_addr(value: str) -> str:
    # A light check to catch obvious typos; full RFC validation is the
    # mail server's job, not this argument parser's.
    if "@" not in value or value.startswith("@") or value.endswith("@"):
        raise argparse.ArgumentTypeError(f"not a valid email address: {value!r}")
    return value


def add_notification_cli_args(parser: argparse.ArgumentParser) -> None:
    """Add the shared ``--notify-*`` / ``--smtp-*`` options to *parser*.

    Every stream command uses this so the flags and their defaults stay
    identical.  Email alerting is off unless at least one
    ``--notify-email`` is given.
    """
    group = parser.add_argument_group("disconnect email notifications")
    group.add_argument(
        "--notify-email",
        action="append",
        metavar="ADDR",
        type=_email_addr,
        default=None,
        help=(
            "Email address to alert when the SFMC connection stays down "
            "(repeatable; omit to disable email alerts)"
        ),
    )
    group.add_argument(
        "--notify-after",
        type=_nonneg_float,
        default=300.0,
        metavar="SECS",
        help="Seconds the SFMC connection must stay down before alerting (default: 300)",
    )
    group.add_argument(
        "--notify-repeat",
        type=_repeat_secs,
        default=3600.0,
        metavar="SECS",
        help=(
            "Re-send a reminder every SECS while still down; 0 sends a "
            "single alert per outage; minimum 60 (default: 3600)"
        ),
    )
    group.add_argument(
        "--smtp-host",
        default="localhost",
        metavar="HOST",
        help="SMTP relay for alert email (default: localhost)",
    )
    group.add_argument(
        "--smtp-port",
        type=int,
        default=25,
        metavar="PORT",
        help="SMTP relay port (default: 25)",
    )
    group.add_argument(
        "--smtp-timeout",
        type=_positive_float,
        default=10.0,
        metavar="SECS",
        help="SMTP connection timeout in seconds (default: 10)",
    )
    group.add_argument(
        "--notify-from",
        default=None,
        metavar="ADDR",
        help="From address for alert email (default: <program>@<fqdn>)",
    )


def build_notifier(
    args: argparse.Namespace,
    *,
    program: str,
    glider_name: str,
    subject_prefix: str = "[SFMC]",
    log: logging.Logger | None = None,
) -> DisconnectNotifier | None:
    """Build a notifier from parsed CLI args, or ``None`` if disabled.

    Returns ``None`` (email alerting off) when no ``--notify-email`` was
    given.  Otherwise wires an SMTP :data:`SendFn` from the ``--smtp-*``
    options and starts the notifier.  Missing attributes fall back to
    the CLI defaults so a hand-built ``Namespace`` only needs
    ``notify_email``.
    """
    recipients: list[str] | None = getattr(args, "notify_email", None)
    if not recipients:
        return None
    send_fn = make_smtp_send(
        host=getattr(args, "smtp_host", "localhost"),
        port=getattr(args, "smtp_port", 25),
        sender=getattr(args, "notify_from", None),
        recipients=recipients,
        timeout=getattr(args, "smtp_timeout", 10.0),
        program=program,
    )
    return DisconnectNotifier(
        send_fn=send_fn,
        threshold_seconds=getattr(args, "notify_after", 300.0),
        repeat_seconds=getattr(args, "notify_repeat", 3600.0),
        subject_prefix=subject_prefix,
        program=program,
        glider_name=glider_name,
        log=log,
    )
