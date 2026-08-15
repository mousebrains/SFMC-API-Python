"""Shared reconnect policy and session supervision for stream commands.

Every long-running command in this package — ``sfmc-monitor-glider``,
``sfmc-follow``, ``sfmc-pull-new-downloads``, and the command channel —
runs the same session lifecycle: open a STOMP stream, subscribe, work
until it drops, log the boundary, notify, back off, reconnect.
:class:`StreamSupervisor` owns that loop so there is one implementation
of it; callers supply only what differs, via hooks.
"""

from __future__ import annotations

import logging
import queue
import random
import re
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from math import isfinite
from typing import TYPE_CHECKING

from .exceptions import APIError, RateLimitError, SFMCError
from .stomp import StompConnection, StompError, StompSubscription

if TYPE_CHECKING:  # pragma: no cover - import cycle avoidance
    from .client import SFMCClient
    from .disconnect_notify import DisconnectNotifier

__all__ = [
    "ReconnectBackoff",
    "ReconnectDelay",
    "StreamSession",
    "StreamSupervisor",
    "Worker",
    "is_transient_error",
    "retry_transient",
    "safe_stream_error",
]

logger = logging.getLogger(__name__)

#: Prefix for the log line marking the end of a stream session.  Log
#: readers grep for it to find gaps in a dialog capture.
STREAM_BOUNDARY_PREFIX = "STREAM_BOUNDARY"


def is_transient_error(exc: BaseException) -> bool:
    """True if a failure is the kind that retrying can fix.

    Transport failures (no HTTP status), rate limiting, and
    server-side 5xx responses are transient.  Permanent client errors
    — 404 from a misspelled glider name, 401/403 from bad credentials,
    unexpected response shapes — must fail fast instead: retrying them
    forever hides a misconfigured deployment from operators and from
    systemd's exit-status/restart accounting.
    """
    if isinstance(exc, RateLimitError):
        return True
    if isinstance(exc, APIError):
        return exc.status_code == 0 or exc.status_code >= 500
    return False


_TOKEN_QUERY_RE = re.compile(r"(access_token=)[^&\s]+", re.IGNORECASE)
_BEARER_RE = re.compile(r"(Bearer\s+)[^\s,;]+", re.IGNORECASE)


def safe_stream_error(exc: BaseException) -> str:
    """Format an exception without exposing a token-bearing URL."""
    detail = _TOKEN_QUERY_RE.sub(r"\1<redacted>", f"{type(exc).__name__}: {exc}")
    return _BEARER_RE.sub(r"\1<redacted>", detail)


@dataclass(frozen=True)
class ReconnectDelay:
    """One reconnect decision returned by :class:`ReconnectBackoff`."""

    attempt: int
    nominal: float
    actual: float


@dataclass
class ReconnectBackoff:
    """Calculate capped exponential reconnect delays with bounded jitter.

    The object only owns policy state. Callers remain responsible for
    classifying failures and waiting in a stop-aware manner.
    """

    initial_delay: float = 15.0
    max_delay: float = 300.0
    stable_after: float = 60.0
    jitter: float = 0.2
    random_uniform: Callable[[float, float], float] = field(
        default=random.uniform,
        repr=False,
    )
    _nominal: float = field(init=False, repr=False)
    _attempt: int = field(init=False, default=0, repr=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("initial_delay", self.initial_delay),
            ("max_delay", self.max_delay),
            ("stable_after", self.stable_after),
            ("jitter", self.jitter),
        ):
            if not isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be >= 0")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        if self.stable_after < 0:
            raise ValueError("stable_after must be >= 0")
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError("jitter must be between 0 and 1")
        self._nominal = self.initial_delay

    def next_delay(self, *, subscribed_uptime: float | None) -> ReconnectDelay:
        """Return the next delay and advance the failure sequence.

        Only time spent in a successfully subscribed session is eligible to
        reset backoff. Failed handshakes pass ``None``.
        """
        if subscribed_uptime is not None and subscribed_uptime >= self.stable_after:
            self._nominal = self.initial_delay
            self._attempt = 0

        nominal = self._nominal
        self._attempt += 1
        if self.jitter:
            low = max(0.0, nominal * (1.0 - self.jitter))
            high = min(self.max_delay, nominal * (1.0 + self.jitter))
            actual = self.random_uniform(low, high)
        else:
            actual = nominal

        self._nominal = min(nominal * 2.0, self.max_delay)
        return ReconnectDelay(
            attempt=self._attempt,
            nominal=nominal,
            actual=min(max(actual, 0.0), self.max_delay),
        )


# ── Startup retry ────────────────────────────────────────────────────


def retry_transient(
    action: Callable[[], None],
    *,
    stop: threading.Event,
    backoff: ReconnectBackoff,
    what: str,
    log: logging.Logger = logger,
    notifier: DisconnectNotifier | None = None,
    reconnect: bool = True,
) -> bool:
    """Run *action*, retrying transient failures with backoff.

    A service started at boot — before DNS/WAN is up, or during an SFMC
    outage — must not exit on a transient failure that the steady-state
    loop would have ridden out.  Permanent client errors (a misspelled
    glider name, bad credentials) still fail fast, so a misconfigured
    deployment is visible to the operator and to systemd's restart
    accounting instead of retrying forever.

    Args:
        action: The startup step to run.  Retried until it returns.
        stop: Shutdown signal; checked before each wait.
        backoff: Delay policy.  Give startup its own instance so its
            failures do not inflate the steady-state loop's delays.
        what: Short description used in log messages.
        log: Where to report retries.
        notifier: Told about each failed attempt, so an outage present
            from the first boot still raises the sustained-outage alert.
        reconnect: ``False`` re-raises instead of retrying.

    Returns:
        ``True`` if *action* succeeded, ``False`` if *stop* was set
        first.

    Raises:
        SFMCError: On a permanent failure, or any failure when
            *reconnect* is ``False``.
    """
    while not stop.is_set():
        try:
            action()
            return True
        except SFMCError as exc:
            if not reconnect or not is_transient_error(exc):
                raise
            delay = backoff.next_delay(subscribed_uptime=None)
            reason = safe_stream_error(exc)
            log.warning("%s failed (%s); retrying in %.1fs", what, reason, delay.actual)
            if notifier is not None:
                notifier.record_disconnect(reason=reason)
            if stop.wait(delay.actual):
                return False
    return False


# ── Session supervision ──────────────────────────────────────────────

#: One unit of per-session work: a name (for error messages) and a
#: callable that runs until its subscription closes.
Worker = tuple[str, Callable[[], None]]


@dataclass
class StreamSession:
    """What a supervised session set up, returned by the setup hook.

    Attributes:
        subscriptions: Subscriptions to close when the session ends.
            Closing them is what unblocks the workers.
        workers: Threads to run for the life of the session.  A worker
            returning normally ends the session; a worker raising
            :class:`~sfmc_api.stomp.StompError` ends it for a
            reconnect; any other exception is treated as a code fault
            and propagates out of :meth:`StreamSupervisor.run`.
    """

    subscriptions: Sequence[StompSubscription] = ()
    workers: Sequence[Worker] = ()


@dataclass(frozen=True)
class _WorkerResult:
    name: str
    error: Exception | None


def _run_worker(
    name: str,
    target: Callable[[], None],
    results: queue.Queue[_WorkerResult],
) -> None:
    try:
        target()
    except Exception as exc:
        results.put(_WorkerResult(name, exc))
    else:
        results.put(_WorkerResult(name, None))


class StreamSupervisor:
    """Run supervised STOMP sessions, reconnecting after each loss.

    The supervisor owns everything the long-running commands used to
    each implement themselves: token refresh before a retry, session
    numbering, stream-boundary logging, connect/disconnect
    notification, offline accounting, worker-failure classification,
    and backed-off reconnect.  Callers supply the parts that differ:

    ``setup``
        Subscribe and build the session's workers.  Runs inside the
        open :class:`~sfmc_api.stomp.StompConnection`.
    ``on_subscribed``
        Called once per session, just after the workers start, with
        ``reconnected=True`` when this session follows an outage.  Use
        it to re-sync state that may have changed while offline.
    ``on_idle``
        Called about twice a second while the session runs *and* while
        waiting to reconnect.  Raise from it to fail the whole
        supervisor — that is how a caller reports that a thread it
        owns outside the session has died.
    ``on_session_end``
        Called after each session ends, before the reconnect wait.

    Failure policy: a worker raising :class:`StompError` (or the
    session raising any :class:`~sfmc_api.exceptions.SFMCError`) is a
    transient session loss and reconnects.  Any other worker exception
    is a code fault and propagates — running on would look healthy
    while silently doing nothing.
    """

    def __init__(
        self,
        client: SFMCClient,
        *,
        setup: Callable[[StompConnection], StreamSession],
        stop: threading.Event | None = None,
        log: logging.Logger = logger,
        notifier: DisconnectNotifier | None = None,
        on_subscribed: Callable[..., None] | None = None,
        on_idle: Callable[[], None] | None = None,
        on_session_end: Callable[[], None] | None = None,
        reconnect: bool = True,
        reconnect_initial_delay: float = 15.0,
        reconnect_max_delay: float = 300.0,
        reconnect_stable_after: float = 60.0,
        reconnect_jitter: float = 0.2,
        worker_join_timeout: float = 5.0,
    ) -> None:
        self._client = client
        self._setup = setup
        self._stop = stop if stop is not None else threading.Event()
        self._log = log
        self._notifier = notifier
        self._on_subscribed = on_subscribed
        self._on_idle = on_idle
        self._on_session_end = on_session_end
        self._reconnect = reconnect
        self._worker_join_timeout = worker_join_timeout
        self._backoff = ReconnectBackoff(
            initial_delay=reconnect_initial_delay,
            max_delay=reconnect_max_delay,
            stable_after=reconnect_stable_after,
            jitter=reconnect_jitter,
        )
        self._session_number = 0

    @property
    def stop(self) -> threading.Event:
        """The shutdown signal this supervisor watches."""
        return self._stop

    @property
    def session_number(self) -> int:
        """How many sessions have been successfully subscribed."""
        return self._session_number

    def run(self) -> None:
        """Supervise sessions until *stop* is set.

        Raises:
            StompError: If ``reconnect=False`` and the session ends.
            RuntimeError: If a worker fails for a non-transient reason,
                or does not stop after its subscription is closed.
        """
        attempt_number = 0
        offline_since: float | None = None

        while not self._stop.is_set():
            attempt_number += 1
            subscribed_at: float | None = None
            failure: Exception | None = None
            reason = "closed"

            try:
                if attempt_number > 1:
                    self._client.refresh_auth()
                with self._client.open_stream() as stomp:
                    subscribed_at, failure, reason = self._run_session(
                        stomp,
                        offline_since=offline_since,
                    )
                    if subscribed_at is not None:
                        offline_since = None
            except SFMCError as exc:
                failure = exc
                reason = "session-error"

            if self._stop.is_set():
                break

            subscribed_uptime = (
                None if subscribed_at is None else max(0.0, time.monotonic() - subscribed_at)
            )
            if subscribed_at is not None:
                self._log.warning(
                    "%s session=%d reason=%s",
                    STREAM_BOUNDARY_PREFIX,
                    self._session_number,
                    reason,
                )
            if offline_since is None:
                offline_since = time.monotonic()
            detail = "normal subscription close" if failure is None else safe_stream_error(failure)
            if subscribed_at is None:
                self._log.warning(
                    "stream setup attempt %d ended: %s: %s", attempt_number, reason, detail
                )
            else:
                self._log.warning(
                    "stream session %d ended: %s: %s", self._session_number, reason, detail
                )
            if self._notifier is not None:
                self._notifier.record_disconnect(reason=detail)
            if self._on_session_end is not None:
                self._on_session_end()
            if not self._reconnect:
                raise StompError(f"stream session ended: {reason}: {detail}") from failure

            delay = self._backoff.next_delay(subscribed_uptime=subscribed_uptime)
            self._log.info("reconnect attempt %d in %.1fs", delay.attempt, delay.actual)
            if self._wait(delay.actual):
                break

    def _run_session(
        self,
        stomp: StompConnection,
        *,
        offline_since: float | None,
    ) -> tuple[float | None, Exception | None, str]:
        """Run one session to completion.

        Returns:
            ``(subscribed_at, failure, reason)``.  *subscribed_at* is
            ``None`` if the session never got as far as starting its
            workers.
        """
        session = self._setup(stomp)
        results: queue.Queue[_WorkerResult] = queue.Queue()
        threads: list[threading.Thread] = []
        first_result: _WorkerResult | None = None

        try:
            # Everything that marks the session live happens *before*
            # the workers start.  The subscriptions already exist, so
            # messages queue up meanwhile and nothing is lost — but a
            # consumer can now never observe data from a session that
            # has not yet been counted.  With the order reversed, a
            # reconnect could deliver its first line before the epoch
            # advanced, so a reader comparing epochs would attribute
            # new-session data to the old one.
            subscribed_at = time.monotonic()
            self._session_number += 1
            self._log.info("stream session %d subscribed", self._session_number)
            if self._notifier is not None:
                self._notifier.record_connect()
            reconnected = offline_since is not None
            if reconnected and offline_since is not None:
                self._log.info(
                    "stream session %d reconnected after %.1fs offline",
                    self._session_number,
                    subscribed_at - offline_since,
                )
            if self._on_subscribed is not None:
                self._on_subscribed(reconnected=reconnected)

            for name, target in session.workers:
                thread = threading.Thread(
                    target=_run_worker,
                    args=(name, target, results),
                    daemon=True,
                    name=f"sfmc-{name}",
                )
                thread.start()
                threads.append(thread)

            while not self._stop.is_set():
                if self._on_idle is not None:
                    self._on_idle()
                try:
                    first_result = results.get(timeout=0.5)
                    break
                except queue.Empty:
                    continue
        finally:
            for sub in session.subscriptions:
                sub.close()
            for thread in threads:
                thread.join(timeout=self._worker_join_timeout)
            if any(thread.is_alive() for thread in threads):
                # A lingering worker would keep consuming the old
                # session's queue while the next session runs.
                raise RuntimeError("stream worker did not stop after subscription close")

        return (subscribed_at, *self._classify(first_result, results))

    def _classify(
        self,
        first_result: _WorkerResult | None,
        results: queue.Queue[_WorkerResult],
    ) -> tuple[Exception | None, str]:
        """Reduce the workers' outcomes to a session failure and reason."""
        collected = [] if first_result is None else [first_result]
        while True:
            try:
                collected.append(results.get_nowait())
            except queue.Empty:
                break

        failure: Exception | None = None
        reason = "closed"
        for result in collected:
            if result.error is None:
                continue
            if isinstance(result.error, StompError):
                failure = result.error
                reason = "stomp-error"
            else:
                raise RuntimeError(
                    f"{result.name} worker failed: {safe_stream_error(result.error)}"
                ) from result.error
        return failure, reason

    def _wait(self, delay: float) -> bool:
        """Wait out the reconnect delay.

        Returns ``True`` if the supervisor should stop.  ``on_idle``
        keeps running here: a caller-owned thread that dies during a
        five-minute backoff must not go unnoticed until the next
        session.
        """
        deadline = time.monotonic() + delay
        while not self._stop.is_set():
            if self._on_idle is not None:
                self._on_idle()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if self._stop.wait(min(0.5, remaining)):
                return True
        return True
