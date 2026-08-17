"""Control engines: react to a fleet's events on one thread.

Phase 2 of ``docs/design/control_engine.md`` — the engine and its
runner, **read operations only**.  Writes are phase 3, and asking for
one here is refused rather than quietly allowed.

An engine subclasses :class:`BaseControlEngine`, implements
:meth:`~BaseControlEngine.on_event`, and acts through
:meth:`~BaseControlEngine.request`::

    class WatchBattery(ControlEngine):
        sources = ("dialog",)

        def on_event(self, event):
            match event.source:
                case "dialog" if "Vehicle Name:" in event.body:
                    self.request("get_glider_details", event.glider,
                                 glider=event.glider, tag="details")
                case "result" if event.tag == "details":
                    self.seen[event.glider] = event.body   # no locks
                case "error" if event.tag == "details":
                    self.log("%s: %s", event.glider, event.body)

What the framework guarantees, and an engine may rely on:

1. :meth:`~BaseControlEngine.on_start`, every
   :meth:`~BaseControlEngine.on_event`, and
   :meth:`~BaseControlEngine.on_stop` run on **one** thread, never
   concurrently, across the whole fleet.  Engine state — including
   cross-glider state — needs no locking.
2. Events from one ``(glider, source)`` pair arrive in order.
3. A ``result`` or ``error`` always follows the ``request`` that caused
   it, and carries that request's ``request_id`` and ``tag``.

What it does **not** guarantee, stated because pretending otherwise
would bite later:

* Ordering *between* sources or *between* gliders.  ``osu685``'s dialog
  may land between two of ``osu684``'s lines, and a ``result`` may land
  between two dialog lines.  That is correct actor behaviour.
* That a result arrives before shutdown; :meth:`on_stop` may run with
  requests outstanding.
* That every event was delivered — queues are bounded, and a
  ``dropped`` event says so.
* That the fleet is consistent at any instant.  Each glider is observed
  independently, so treat fleet state as last-known values with
  per-glider timestamps, never a snapshot.
"""

from __future__ import annotations

import logging
import threading
import time
import traceback
from collections.abc import Callable, Iterable
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any

from .events import Event, EventMerge, FleetStream
from .ops import OperationExecutor

if TYPE_CHECKING:  # pragma: no cover
    from .client import SFMCClient

__all__ = [
    "READ_OPERATIONS",
    "BaseControlEngine",
    "EngineRunner",
    "WriteRefused",
]

logger = logging.getLogger(__name__)

#: Client methods an engine may call in phase 2.
#:
#: Named explicitly rather than inferred.  There is no marker on the
#: client saying which methods change state, and guessing from the verb
#: would classify ``download_glider_file`` (a GET) with ``delete_*``.
#: An explicit list is reviewable, and every name is checked against
#: :class:`~sfmc_api.client.SFMCClient` at engine start — so a typo
#: fails at startup rather than at 3 a.m. on a surfacing.
#:
#: Downloads are included: they write to local disk but change nothing
#: on the server or the vehicle, which is the line that matters here.
READ_OPERATIONS = frozenset(
    {
        "get_glider_details",
        "get_active_deployment_details",
        "get_newest_mission_status",
        "get_available_scripts",
        "get_surface_sensor_samples",
        "get_folder_file_listing",
        "get_zmodem_transfers",
        "get_mission_plan",
        "get_waypoint_plan",
        "get_yo_plan",
        "get_surface_plan",
        "get_sampling_plan",
        "get_data_transmission_plan",
        "get_mission_sensor_plan",
        "get_abort_plan",
        "download_glider_file",
        "download_glider_files",
    }
)

#: Seconds an ``on_event`` may run before the watchdog complains.
DEFAULT_WATCHDOG_SECONDS = 30.0

#: Consecutive ``on_event`` failures before the runner gives up.
DEFAULT_MAX_FAILURES = 5


class WriteRefused(Exception):
    """A state-changing operation was requested from a read-only engine."""


class BaseControlEngine:
    """Subclass this.  Implement :meth:`on_event`; call :meth:`request`.

    Attributes:
        sources: Which sources to subscribe per glider.  ``dialog``
            (assembled lines) is the default and what almost every
            engine wants; ``dialog.raw`` gives chunks as received.
        config: The ``dict`` handed in at construction, usually from a
            YAML file.  The framework does not inspect it — the same
            convention :class:`~sfmc_api.follower.BaseFollower` uses,
            so a follower author has one less thing to relearn.
    """

    sources: tuple[str, ...] = ("dialog",)

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config: dict[str, Any] = config or {}
        self._runner: EngineRunner | None = None

    # ── Implement these ──────────────────────────────────────────────

    def on_start(self) -> None:
        """Called once, on the engine thread, before any event."""

    def on_event(self, event: Event) -> None:
        """Called for every event, on the engine thread, in order."""

    def on_stop(self) -> None:
        """Called once, on the engine thread, after the last event.

        May run with requests still outstanding.
        """

    # ── Call these ───────────────────────────────────────────────────

    def request(
        self,
        op: str,
        *args: Any,
        glider: str,
        tag: str | None = None,
        **kwargs: Any,
    ) -> int:
        """Run a client operation off-thread; get the answer as an event.

        Returns immediately with a request id.  The outcome arrives
        later as a ``result`` or ``error`` event carrying that id and
        *tag*.

        Args:
            op: A client method name, e.g. ``"get_mission_plan"``.  A
                string rather than the bound method, because that gives
                the framework somewhere to stand — dry-run, write
                gating, rate limiting and audit all need to know what is
                being asked *before* it happens.  Validated against the
                client at engine start.
            glider: **Keyword, separate from the positional args, on
                purpose.**  It names the serialisation key, not an
                argument.  Inferring it from ``args[0]`` would be right
                for most endpoints and quietly wrong for
                ``get_zmodem_transfers(connection_id)`` and
                ``upload_cache_files(group_name)`` — and "quietly wrong
                about which glider we locked" is not worth saving a
                keyword.  It also gives every result an event to be
                tagged with.
            tag: Your label, echoed back on the result.

        Raises:
            WriteRefused: If *op* changes state.  Phase 2 is read-only.
        """
        return self._require_runner()._request(op, args, kwargs, glider=glider, tag=tag)

    def add_glider(self, name: str) -> None:
        """Start streaming another glider.  A formation changes."""
        self._require_runner().add_glider(name)

    def remove_glider(self, name: str) -> None:
        """Stop streaming a glider and discard its queues."""
        self._require_runner().remove_glider(name)

    def log(self, message: str, *args: Any) -> None:
        """Log against the engine's logger, lazily formatted."""
        logger.info(f"{type(self).__name__}: {message}", *args)

    def notify(self, key: str, summary: str, detail: str) -> None:
        """Raise an operator-visible notification.

        Phase 2 logs it.  Wiring this to the existing disconnect
        notifier is deliberately left for the phase that also has to
        decide de-duplication policy.
        """
        logger.warning("%s [%s] %s: %s", type(self).__name__, key, summary, detail)

    @property
    def gliders(self) -> tuple[str, ...]:
        """Gliders currently streaming."""
        return self._require_runner().gliders

    @property
    def client(self) -> SFMCClient:
        """Escape hatch for the genuinely synchronous case.

        **This blocks the event loop.**  Nothing else is processed while
        it runs — no other glider's dialog, no results — and none of the
        framework's safety rails apply to what you do with it.  Prefer
        :meth:`request`.
        """
        return self._require_runner().client

    def _require_runner(self) -> EngineRunner:
        if self._runner is None:
            raise RuntimeError(
                "engine is not attached to a runner; "
                "construct an EngineRunner(engine, client) first"
            )
        return self._runner


class EngineRunner:
    """Owns the thread, the fleet, and the operation pool.

    Args:
        engine: The engine to drive.
        client: An :class:`~sfmc_api.client.SFMCClient`.
        gliders: Names to start streaming immediately.
        max_workers: Operation pool size.  One pool for the whole
            fleet: SFMC rate-limits the *account*, not the glider, so a
            per-glider cap would multiply by fleet size and produce the
            429 storm the cap exists to prevent.
        watchdog: Seconds an ``on_event`` may run before a warning names
            it.  ``None`` disables.  A slow ``on_event`` is the single
            most common way to break this system, so it is
            self-diagnosing.
        max_failures: Consecutive ``on_event`` failures before stopping.
            Continuing after one bad event is right for a long mission;
            continuing forever with a wedged engine is not.
    """

    def __init__(
        self,
        engine: BaseControlEngine,
        client: SFMCClient | None = None,
        *,
        gliders: Iterable[str] = (),
        max_workers: int = 4,
        watchdog: float | None = DEFAULT_WATCHDOG_SECONDS,
        max_failures: int = DEFAULT_MAX_FAILURES,
        fleet: FleetStream | None = None,
        executor: OperationExecutor | None = None,
    ) -> None:
        self._engine = engine
        self._client = client
        self._watchdog = watchdog
        self._max_failures = max_failures
        self._validate(engine, client)
        if fleet is not None:
            self._fleet: FleetStream | None = fleet
        elif client is not None:
            self._fleet = FleetStream(client, sources=tuple(engine.sources))
        else:
            # Replay-only: no sessions, no sockets, no server.  "A
            # scientist should be able to test a control algorithm
            # without a glider, a server, or a network" is only true if
            # constructing the runner does not need one either.
            self._fleet = None
        self._merge: EventMerge = self._fleet.merge if self._fleet is not None else EventMerge()
        self._ops = (
            executor if executor is not None else OperationExecutor(max_workers=max_workers)
        )
        self._owns_executor = executor is None
        self._request_seq = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._in_flight: tuple[Event, float] | None = None
        self._watchdog_thread: threading.Thread | None = None
        self.failures = 0
        engine._runner = self
        for name in gliders:
            self.add_glider(name)

    def _validate(self, engine: BaseControlEngine, client: SFMCClient | None) -> None:
        """Fail at construction, not at 3 a.m. on a surfacing."""
        if not engine.sources:
            raise ValueError("engine.sources is empty; nothing would ever arrive")
        if client is None:
            return
        missing = sorted(op for op in READ_OPERATIONS if not hasattr(client, op))
        if missing:
            raise ValueError(f"client has no such operation(s): {missing}")

    # ── Fleet ────────────────────────────────────────────────────────

    @property
    def gliders(self) -> tuple[str, ...]:
        return self._fleet.gliders if self._fleet is not None else ()

    @property
    def client(self) -> SFMCClient:
        if self._client is None:
            raise RuntimeError("this runner has no client; it is replay-only")
        return self._client

    def add_glider(self, name: str) -> None:
        self._require_fleet().add_glider(name)

    def remove_glider(self, name: str) -> None:
        self._require_fleet().remove_glider(name)

    def _require_fleet(self) -> FleetStream:
        if self._fleet is None:
            raise RuntimeError("this runner has no fleet; it is replay-only")
        return self._fleet

    # ── Requests ─────────────────────────────────────────────────────

    def _request(
        self,
        op: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        glider: str,
        tag: str | None,
    ) -> int:
        if self._client is None:
            raise RuntimeError(f"cannot {op!r}: this runner has no client; it is replay-only")
        if op not in READ_OPERATIONS:
            kind = (
                "a state-changing operation"
                if hasattr(self._client, op)
                else "not a client operation"
            )
            raise WriteRefused(
                f"{op!r} is {kind}; this engine is read-only "
                f"(writes are phase 3).  Read operations: {sorted(READ_OPERATIONS)}"
            )
        method = getattr(self._client, op)
        with self._lock:
            self._request_seq += 1
            request_id = self._request_seq
        # Serialized per glider: two operations on one glider must not
        # interleave, while the same two on different gliders may run
        # concurrently.
        future = self._ops.serialized(glider, method, *args, **kwargs)
        future.add_done_callback(
            self._make_completion(request_id=request_id, glider=glider, tag=tag)
        )
        return request_id

    def _make_completion(
        self, *, request_id: int, glider: str, tag: str | None
    ) -> Callable[[Future[Any]], None]:
        def completed(future: Future[Any]) -> None:
            try:
                value: Any = future.result()
                source = "result"
            except Exception as exc:
                value, source = exc, "error"
            self._merge.publish(glider, source, value, request_id=request_id, tag=tag)

        return completed

    # ── Running ──────────────────────────────────────────────────────

    def run(self) -> None:
        """Drive the engine until :meth:`stop`.  Blocks; owns this thread."""
        self._start_watchdog()
        try:
            self._engine.on_start()
            for event in self._merge:
                if self._stop.is_set():
                    break
                self._deliver(event)
        finally:
            self._stop.set()
            try:
                self._engine.on_stop()
            except Exception:
                logger.exception("%s.on_stop failed", type(self._engine).__name__)
            self.close()

    def _deliver(self, event: Event) -> None:
        """One event through on_event, with the failure policy applied."""
        with self._lock:
            self._in_flight = (event, time.monotonic())
        try:
            self._engine.on_event(event)
        except Exception as exc:
            self.failures += 1
            logger.error(
                "%s.on_event failed on %s/%s (failure %d of %d)\n%s",
                type(self._engine).__name__,
                event.glider,
                event.source,
                self.failures,
                self._max_failures,
                traceback.format_exc(),
            )
            if self.failures >= self._max_failures:
                self._engine.notify(
                    "engine-failed",
                    f"{type(self._engine).__name__} stopped",
                    f"{self.failures} consecutive on_event failures; last: {exc!r}",
                )
                self.stop()
                return
            # Tell the engine about its own failure, once, rather than
            # silently swallowing it.  A handler that also raises just
            # advances the strike counter, which is bounded.
            self._merge.publish(event.glider, "error", exc)
        else:
            self.failures = 0
        finally:
            with self._lock:
                self._in_flight = None

    def replay(self, dialog: Iterable[str], glider: str) -> None:
        """Drive the engine from recorded dialog, with no network.

        A scientist should be able to test a control algorithm without
        a glider, a server, or a network.  Each line becomes a
        ``dialog`` event for *glider*; requests are still refused unless
        the client can serve them, so pass a stub client for a fully
        offline run.
        """
        self._engine.on_start()
        try:
            for line in dialog:
                if self._stop.is_set():
                    break
                event = self._merge.publish(glider, "dialog", line.rstrip("\r\n"))
                if event is None:
                    break
                # Drain, so results provoked by this line are seen in
                # order rather than piling up behind the next one.
                while (queued := self._merge.get(timeout=0)) is not None:
                    self._deliver(queued)
        finally:
            self._engine.on_stop()

    # ── Watchdog ─────────────────────────────────────────────────────

    def _start_watchdog(self) -> None:
        if self._watchdog is None:
            return

        def watch() -> None:
            while not self._stop.wait(min(self._watchdog or 1.0, 5.0)):
                with self._lock:
                    in_flight = self._in_flight
                if in_flight is None:
                    continue
                event, started = in_flight
                elapsed = time.monotonic() - started
                if elapsed >= (self._watchdog or 0):
                    logger.warning(
                        "%s.on_event has been running %.0fs on %s/%s "
                        "(seq=%d); the whole fleet is stalled behind it",
                        type(self._engine).__name__,
                        elapsed,
                        event.glider,
                        event.source,
                        event.seq,
                    )

        self._watchdog_thread = threading.Thread(
            target=watch, daemon=True, name="sfmc-engine-watchdog"
        )
        self._watchdog_thread.start()

    # ── Lifecycle ────────────────────────────────────────────────────

    def stop(self) -> None:
        """Ask the runner to finish.  Safe from any thread."""
        self._stop.set()
        self._merge.close()

    def close(self) -> None:
        """Release the fleet and the pool."""
        if self._fleet is not None:
            self._fleet.close()
        if self._owns_executor:
            self._ops.shutdown(wait=False, cancel_pending=True)

    def __enter__(self) -> EngineRunner:
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
        self.close()
