"""Control engines: react to a fleet's events on one thread.

Phases 2 and 3 of ``docs/design/control_engine.md`` — the engine, its
runner, and the safety rails around anything that can move a glider.

**Writes are off by default.**  A state-changing operation is refused
unless ``allow_writes=True``, and the refusal arrives as an ``error``
event naming the flag, so an engine handles it like any other failed
operation.  ``dry_run=True`` runs the engine's whole logic and answers
each write with a synthetic :class:`DryRun` result instead of sending
it — reads still happen, so the engine sees real data and makes real
decisions, and only the consequences are withheld.  Every request and
outcome is written to the ``sfmc_api.engine.audit`` logger: when a
glider does something surprising, that is the artefact that explains
why.

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

import inspect
import logging
import threading
import time
import traceback
from collections.abc import Callable, Iterable
from concurrent.futures import Future
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .client import SFMCClient as _SFMCClient
from .events import Event, EventMerge, FleetStream
from .ops import OperationExecutor

if TYPE_CHECKING:  # pragma: no cover
    from .client import SFMCClient

__all__ = [
    "DEFAULT_MAX_OUTSTANDING",
    "ENGINE_FAILURE_TAG",
    "OPERATIONS",
    "READ_OPERATIONS",
    "WRITE_OPERATIONS",
    "BaseControlEngine",
    "DryRun",
    "EngineRunner",
    "RateLimited",
    "WriteRefused",
]

logger = logging.getLogger(__name__)


#: Operations classified at their definition, on the client itself.
#:
#: :func:`~sfmc_api.client.reads` and :func:`~sfmc_api.client.mutates`
#: mark each endpoint where it is written, so adding an endpoint and
#: classifying it are the same act.  A list kept here instead would
#: drift the first time somebody added a method in a hurry — and the
#: drift would be silent, and on the dangerous side.
#:
#: An **unmarked** method is not requestable at all.  That is the
#: fail-safe direction: a new mutating endpoint nobody classified
#: cannot be called by an engine, rather than defaulting to allowed.
def _classify(client: object) -> tuple[frozenset[str], frozenset[str]]:
    reads: set[str] = set()
    writes: set[str] = set()
    for name in dir(client):
        if name.startswith("_"):
            continue
        # getattr_static, because plain getattr *invokes* descriptors:
        # SFMCClient.download_dir is a property whose getter mkdir()s,
        # so classifying with getattr made merely constructing a runner
        # create a directory -- and fail outright when the download path
        # was unwritable, for an engine that never downloads anything.
        try:
            attribute = inspect.getattr_static(client, name)
        except AttributeError:  # pragma: no cover - dir() lied
            continue
        marker = getattr(attribute, "sfmc_mutates", None)
        if marker is True:
            writes.add(name)
        elif marker is False:
            reads.add(name)
    return frozenset(reads), frozenset(writes)


READ_OPERATIONS, WRITE_OPERATIONS = _classify(_SFMCClient)

#: Everything an engine may name in :meth:`BaseControlEngine.request`.
OPERATIONS = READ_OPERATIONS | WRITE_OPERATIONS

#: Operation pool size, shared by the whole fleet.
#:
#: Sized against the server, not the fleet.  SFMC's concurrency limit is
#: soft, around 20, and depends on the server's own resources -- so this
#: sits well under it.  It also bounds the starvation
#: :meth:`~sfmc_api.ops.OperationExecutor.serialized` can cause: that
#: waits for its per-glider lock *inside* a worker, so a burst aimed at
#: one surfacing glider occupies workers the rest of the fleet needs.
DEFAULT_MAX_WORKERS = 8

#: Outstanding requests allowed at once, fleet-wide.
#:
#: Fleet-wide rather than per glider: SFMC rate-limits the *account*, so
#: a per-glider cap would multiply by fleet size and produce exactly the
#: 429 storm the cap exists to prevent.  Its job is to stop an engine
#: that fires a request per dialog line -- and a surfacing delivers
#: hundreds of lines in milliseconds -- from melting the server.
DEFAULT_MAX_OUTSTANDING = 12

#: One line per request and per outcome.  When a glider does something
#: surprising, this is the artefact that explains why, so it is a
#: separate logger that a deployment can route to its own file.
audit_log = logging.getLogger("sfmc_api.engine.audit")

#: Seconds an ``on_event`` may run before the watchdog complains.
DEFAULT_WATCHDOG_SECONDS = 30.0

#: Tag on the ``error`` event the framework publishes when ``on_event``
#: itself raises.
#:
#: Distinguishable on purpose.  Handling that notice used to count as a
#: success and reset the strike counter, so an engine failing on *every*
#: dialog line oscillated 1 -> 0 forever and the cap never tripped: the
#: engine was totally dead on its primary source and nobody was told.
#: It tripped only when events were already queued in a burst -- which
#: is how a test gets written, so it passed the test and failed in
#: production.
ENGINE_FAILURE_TAG = "engine-failure"

#: Consecutive ``on_event`` failures before the runner gives up.
DEFAULT_MAX_FAILURES = 5


def _summarise(args: tuple[Any, ...], limit: int = 80) -> str:
    """Short, single-line rendering of call arguments for the audit log.

    Bounded because an upload's arguments can be a whole file, and an
    audit line that wraps for a page is one nobody reads.
    """
    text = ", ".join(repr(a) for a in args)
    return text if len(text) <= limit else text[: limit - 1] + "…"


class WriteRefused(Exception):
    """A state-changing operation was attempted without ``allow_writes``.

    Delivered as the body of an ``error`` event rather than raised, so
    an engine handles a blocked write the same way it handles any other
    failed operation.
    """


class RateLimited(Exception):
    """Too many requests outstanding; this one was not submitted."""


@dataclass(frozen=True)
class DryRun:
    """Body of the synthetic ``result`` a dry run answers a write with.

    A distinct type on purpose: an engine that treats it as real server
    data is making a mistake, and should be able to notice.
    """

    op: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


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

        Because nothing downstream can tell a read from a write once the
        client is in hand, taking it requires ``allow_writes`` and is
        refused under a dry run.  Every access is logged at ``WARNING``
        to the audit trail, since it is the point where that trail stops
        being a complete account of what the run did.

        Raises:
            WriteRefused: If writes are not enabled, or this is a dry
                run.
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
        final_drain: Seconds to let work queued during shutdown finish.
            ``sfmc-follow`` waits 45 s for the same reason: a steering
            file queued just before a disconnect must still be uploaded,
            or the glider flies stale waypoints for the whole next dive.
        allow_writes: Permit state-changing operations.  **Off by
            default**, matching ``sfmc-api-test``'s posture so the
            project has one rule rather than two.  A blocked write
            produces an ``error`` event naming this flag, not an
            exception -- an engine then handles it like any other failed
            operation.
        dry_run: Run the engine's full logic, but answer every write
            with a synthetic :class:`DryRun` result instead of sending
            it.  Reads still happen, so the engine sees real data and
            makes real decisions; only the consequences are withheld.
        max_outstanding: Cap on requests in flight, fleet-wide.
            Exceeding it produces an ``error`` event rather than
            queueing silently -- a loop that fires a request per dialog
            line must fail loudly, not melt the server.
        tick: Seconds between ``tick`` events, or ``None`` for none.
            An engine that must notice the *absence* of dialog -- "she
            has been quiet for twenty seconds, so the transfer is over
            and she is listening" -- cannot do it from dialog events
            alone, because silence delivers nothing to react to.  One
            tick is emitted per glider, so per-glider timing needs no
            bookkeeping in the engine.
    """

    def __init__(
        self,
        engine: BaseControlEngine,
        client: SFMCClient | None = None,
        *,
        gliders: Iterable[str] = (),
        max_workers: int = DEFAULT_MAX_WORKERS,
        watchdog: float | None = DEFAULT_WATCHDOG_SECONDS,
        max_failures: int = DEFAULT_MAX_FAILURES,
        final_drain: float = 45.0,
        allow_writes: bool = False,
        dry_run: bool = False,
        max_outstanding: int = DEFAULT_MAX_OUTSTANDING,
        tick: float | None = None,
        fleet: FleetStream | None = None,
        executor: OperationExecutor | None = None,
    ) -> None:
        self._engine = engine
        self._client = client
        # Default to the base class's classification; _validate narrows
        # these to the actual client instance when there is one.
        self.reads: frozenset[str] = READ_OPERATIONS
        self.writes: frozenset[str] = WRITE_OPERATIONS
        self._watchdog = watchdog
        self._max_failures = max_failures
        self._final_drain = final_drain
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
        self._owns_fleet = fleet is None
        self._owns_executor = executor is None
        self._tick = tick
        self._tick_thread: threading.Thread | None = None
        self.allow_writes = allow_writes
        self.dry_run = dry_run
        self._max_outstanding = max_outstanding
        self._outstanding = 0
        self._request_seq = 0
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._in_flight: tuple[Event, float] | None = None
        self._watchdog_thread: threading.Thread | None = None
        self.failures = 0
        self._ran = False
        engine._runner = self
        # Stated once at startup: "was this run allowed to touch the
        # glider?" must be answerable from the log alone.
        audit_log.info(
            "engine=%s writes=%s dry_run=%s max_outstanding=%d",
            type(engine).__name__,
            "ALLOWED" if allow_writes else "blocked",
            dry_run,
            max_outstanding,
        )
        try:
            for name in gliders:
                self.add_glider(name)
        except Exception:
            # The caller never receives this object, so nothing else can
            # ever close what we already started.
            self.close()
            raise

    def _validate(self, engine: BaseControlEngine, client: SFMCClient | None) -> None:
        """Fail at construction, not at 3 a.m. on a surfacing."""
        if not engine.sources:
            raise ValueError("engine.sources is empty; nothing would ever arrive")
        if client is None:
            return
        missing = sorted(op for op in OPERATIONS if not hasattr(client, op))
        if missing:
            raise ValueError(f"client has no such operation(s): {missing}")
        # Classification follows the *client we were given*, not
        # SFMCClient.  A subclass that overrides a read to issue a PUT
        # loses the marker with the method it replaced, so it becomes
        # unmarked -- and unmarked means not requestable, which is the
        # fail-safe direction.  Classifying the base class instead would
        # have kept calling it a read and let the PUT through.
        self.reads, self.writes = _classify(client)

    @property
    def operations(self) -> frozenset[str]:
        """Everything this runner's client may be asked to do."""
        return self.reads | self.writes

    # ── Fleet ────────────────────────────────────────────────────────

    @property
    def gliders(self) -> tuple[str, ...]:
        return self._fleet.gliders if self._fleet is not None else ()

    @property
    def client(self) -> SFMCClient:
        """The raw client, gated and audited.  See the engine docstring.

        Refused unless writes are enabled, because nothing downstream of
        here can tell a read from a write: handing out the client hands
        out every endpoint on it.  Refused under a dry run for the same
        reason -- that mode promises nothing reaches the glider, and a
        raw client cannot honour a promise it does not know about.
        """
        if self._client is None:
            raise RuntimeError("this runner has no client; it is replay-only")
        if not self.allow_writes:
            raise WriteRefused(
                "the raw client bypasses the write gate, the rate cap and the "
                "audit log, so it is refused unless writes are enabled; "
                "pass allow_writes=True (--allow-writes), or use request()"
            )
        if self.dry_run:
            raise WriteRefused(
                "a dry run promises nothing reaches the glider, and the raw "
                "client cannot honour that; use request(), which is simulated"
            )
        # Audited at WARNING, and loudly, because from here on the trail
        # goes cold: the banner at startup says what the run was allowed
        # to do, and this is the moment that stops being the whole story.
        audit_log.warning(
            "engine=%s took the raw client ESCAPE HATCH: "
            "operations from here are unaudited and ungated",
            type(self._engine).__name__,
        )
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
        # Naming something that is not an operation at all is a
        # programming error, so it raises here.  Everything below this
        # line is an *operational* condition -- blocked, rate limited,
        # dry run -- and those are reported as events, so an engine
        # handles them the same way it handles any other failure rather
        # than wrapping every request in a try block.
        if op not in self.operations:
            raise ValueError(
                f"{op!r} is not a requestable operation.  "
                f"Reads: {sorted(READ_OPERATIONS)}.  Writes: {sorted(WRITE_OPERATIONS)}"
            )
        if self._client is None:
            raise RuntimeError(f"cannot {op!r}: this runner has no client; it is replay-only")

        with self._lock:
            self._request_seq += 1
            request_id = self._request_seq
        is_write = op in self.writes
        self._audit(request_id, glider, op, args, tag, "requested", kwargs=kwargs)

        if is_write and self._fleet is not None and glider not in self.gliders:
            return self._refuse(
                request_id,
                glider,
                op,
                args,
                tag,
                WriteRefused(
                    f"{glider!r} is not in this run's fleet "
                    f"({', '.join(self.gliders) or 'none'}); --glider is a scope, "
                    "and a write to a glider outside it is refused"
                ),
            )
        if is_write and not self.allow_writes:
            return self._refuse(
                request_id,
                glider,
                op,
                args,
                tag,
                WriteRefused(
                    f"{op!r} changes state and writes are not enabled; "
                    "pass allow_writes=True (--allow-writes) to permit it"
                ),
            )

        with self._lock:
            outstanding = self._outstanding
            if outstanding >= self._max_outstanding:
                over_cap = True
            else:
                over_cap = False
                self._outstanding += 1
        if over_cap:
            return self._refuse(
                request_id,
                glider,
                op,
                args,
                tag,
                RateLimited(
                    f"{outstanding} requests already outstanding "
                    f"(cap {self._max_outstanding}); {op!r} was not submitted"
                ),
            )

        if is_write and self.dry_run:
            with self._lock:
                self._outstanding -= 1
            self._audit(request_id, glider, op, args, tag, "dry-run", kwargs=kwargs)
            self._merge.publish(
                glider,
                "result",
                DryRun(op=op, args=args, kwargs=dict(kwargs)),
                request_id=request_id,
                tag=tag,
            )
            return request_id

        try:
            method = getattr(self._client, op)
            # Serialized per glider: two operations on one glider must
            # not interleave, while the same two on different gliders
            # may run concurrently.
            future = self._ops.serialized(glider, method, *args, **kwargs)
        except Exception as exc:
            # The slot must come back.  Without this, a client missing a
            # method, or a pool shut down under us, leaks one slot per
            # attempt until the cap is full -- and then every request,
            # including reads, is refused forever.  A watching engine
            # goes silently blind, which is the worst failure this
            # system has.
            with self._lock:
                self._outstanding -= 1
            return self._refuse(request_id, glider, op, args, tag, exc)
        future.add_done_callback(
            self._make_completion(request_id=request_id, glider=glider, op=op, args=args, tag=tag)
        )
        return request_id

    def _refuse(
        self,
        request_id: int,
        glider: str,
        op: str,
        args: tuple[Any, ...],
        tag: str | None,
        error: Exception,
    ) -> int:
        """Report a request that was never submitted, as an error event."""
        self._audit(request_id, glider, op, args, tag, f"refused: {type(error).__name__}")
        self._merge.publish(glider, "error", error, request_id=request_id, tag=tag)
        return request_id

    def _audit(
        self,
        request_id: int,
        glider: str,
        op: str,
        args: tuple[Any, ...],
        tag: str | None,
        outcome: str,
        elapsed: float | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> None:
        # kwargs are logged because the calling convention puts real
        # payloads there: request("send_command", command="abort") used
        # to audit as args= -- empty -- while an abort went to a glider.
        audit_log.info(
            "req=%d glider=%s op=%s args=%s%s tag=%s %s%s",
            request_id,
            glider,
            op,
            _summarise(args),
            "" if not kwargs else " " + _summarise(tuple(f"{k}={v!r}" for k, v in kwargs.items())),
            tag,
            outcome,
            "" if elapsed is None else f" in {elapsed:.2f}s",
        )

    def _make_completion(
        self,
        *,
        request_id: int,
        glider: str,
        op: str,
        args: tuple[Any, ...],
        tag: str | None,
    ) -> Callable[[Future[Any]], None]:
        started = time.monotonic()

        def completed(future: Future[Any]) -> None:
            try:
                value: Any = future.result()
                source, outcome = "result", "ok"
            except Exception as exc:
                value, source = exc, "error"
                outcome = f"failed: {exc!r}"
            with self._lock:
                self._outstanding -= 1
            self._audit(request_id, glider, op, args, tag, outcome, time.monotonic() - started)
            self._merge.publish(glider, source, value, request_id=request_id, tag=tag)

        return completed

    @property
    def outstanding(self) -> int:
        """Requests submitted and not yet answered."""
        with self._lock:
            return self._outstanding

    # ── Running ──────────────────────────────────────────────────────

    def run(self) -> None:
        """Drive the engine until :meth:`stop`.  Blocks; owns this thread.

        Once only.  Guarantee 1 says ``on_start`` and ``on_stop`` are
        called once, and a second ``run()`` would call both again and
        start a second watchdog and tick thread.
        """
        if self._ran:
            raise RuntimeError("this runner has already run; construct another")
        self._ran = True
        self._start_watchdog()
        self._start_ticks()
        try:
            self._engine.on_start()
            for event in self._merge:
                # Delivered before the stop check: this event is already
                # off the queue, and dropping it here loses it with no
                # accounting -- the one thing backpressure exists to
                # prevent.
                self._deliver(event)
                if self._stop.is_set():
                    break
        finally:
            self._stop.set()
            abandoned = self._merge.pending()
            if abandoned:
                logger.warning(
                    "stopping with %d event(s) undelivered; they are discarded", abandoned
                )
            try:
                self._engine.on_stop()
            except Exception:
                logger.exception("%s.on_stop failed", type(self._engine).__name__)
            # on_stop may queue work -- a follower flushes a part-built
            # surfacing and its files land here.  Closing immediately
            # submitted those and then cancelled them, which reverses
            # what the deployed pipeline promises out loud: files queued
            # just before a disconnect must still be uploaded, or the
            # glider flies stale waypoints for the whole next dive.
            self._drain_final(self._final_drain)
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
            self._merge.publish(event.glider, "error", exc, tag=ENGINE_FAILURE_TAG)
        else:
            # Surviving the framework's own crash notice is not evidence
            # the engine has recovered -- it is the one event such an
            # engine is most likely to handle.  Counting it as success
            # is what pinned the counter below the cap forever.
            if not (event.source == "error" and event.tag == ENGINE_FAILURE_TAG):
                self.failures = 0
        finally:
            with self._lock:
                self._in_flight = None

    def _settle(self, timeout: float) -> None:
        """Deliver whatever outstanding requests still owe us.

        Without this a replay answered nothing: the drain uses a zero
        timeout, so it always ran before any operation could finish, and
        an engine's whole request/response half looked like it simply
        never replied.  Replay is meant to be the path of least
        resistance for developing a control algorithm, so it has to
        exercise the half that acts.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            event = self._merge.get(timeout=0.02)
            if event is not None:
                self._deliver(event)
                continue
            if not self.outstanding:
                return
        if self.outstanding:
            logger.warning(
                "replay finished with %d request(s) still outstanding after %.0fs",
                self.outstanding,
                timeout,
            )

    def replay(self, dialog: Iterable[str], glider: str, *, settle: float = 5.0) -> None:
        """Drive the engine from recorded dialog, with no network.

        A scientist should be able to test a control algorithm without
        a glider, a server, or a network.  Each line becomes a
        ``dialog`` event for *glider*; requests are still refused unless
        the client can serve them, so pass a stub client for a fully
        offline run.
        """
        try:
            self._engine.on_start()
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
            self._settle(settle)
        finally:
            self._engine.on_stop()

    # ── Ticks ────────────────────────────────────────────────────────

    def _start_ticks(self) -> None:
        """Publish a periodic wake-up per glider.

        Silence delivers no events, so an engine waiting for a link to
        go quiet has nothing to react to.  A tick is that something.
        """
        if not self._tick:
            return

        def beat() -> None:
            while not self._stop.wait(self._tick or 1.0):
                for glider in self.gliders:
                    self._merge.publish(glider, "tick", None)

        self._tick_thread = threading.Thread(target=beat, daemon=True, name="sfmc-engine-tick")
        self._tick_thread.start()

    def _drain_final(self, timeout: float) -> None:
        """Let work queued during shutdown finish before the pool closes."""
        if timeout <= 0 or not self.outstanding:
            return
        deadline = time.monotonic() + timeout
        while self.outstanding and time.monotonic() < deadline:
            time.sleep(0.05)
        if self.outstanding:
            logger.warning(
                "shutting down with %d request(s) unfinished after %.0fs; they are cancelled",
                self.outstanding,
                timeout,
            )

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
        """Release the fleet and the pool.  Every step, even after a failure.

        Sequenced with ``finally`` rather than in a row: a raising
        ``fleet.close()`` used to skip the pool shutdown, leaving
        in-flight operations -- including writes to a glider -- running
        after the runner had ostensibly closed.
        """
        try:
            if self._fleet is not None and self._owns_fleet:
                self._fleet.close()
        finally:
            try:
                self._merge.close()
            finally:
                if self._owns_executor:
                    self._ops.shutdown(wait=False, cancel_pending=True)

    def __enter__(self) -> EngineRunner:
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()
        self.close()
