"""Merge many gliders' streams into one tagged, ordered event queue.

Phase 1 of the control engine design (``docs/design/control_engine.md``):
the substrate a control engine sits on, with no engine yet.

A formation controller exists to make decisions *across* gliders — "osu685
is 400 m behind, slow osu684" — so every event carries the glider it
concerns and they all arrive in one sequence on one thread.  That is what
lets an engine hold fleet state in ordinary attributes with no locking,
and it is why this is multi-glider from the first commit: the merge is
where that is either easy or impossible.

Split in two, so the interesting half needs no I/O to test:

* :class:`EventMerge` is pure.  It knows nothing about SFMC or sessions;
  producers call :meth:`~EventMerge.publish` and a consumer calls
  :meth:`~EventMerge.get`.  All the ordering and drop accounting lives
  here, and so do most of the tests.
* :class:`FleetStream` wires N :class:`~sfmc_api.session.GliderSession`
  objects into one, and is the only part that touches the network.

Usage::

    with FleetStream(client) as fleet:
        fleet.add_glider("osu684")
        fleet.add_glider("osu685")
        for event in fleet:
            match event.source:
                case "dialog":
                    print(event.glider, event.body)
                case "dropped":
                    print("fell behind:", event.body)
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from .client import SFMCClient
    from .dialog_stream import DialogLine
    from .session import GliderSession

__all__ = [
    "DEFAULT_PAIR_MAXSIZE",
    "SOURCES",
    "DroppedNotice",
    "Event",
    "EventMerge",
    "FleetStream",
    "StreamNotice",
]

logger = logging.getLogger(__name__)

#: Per-(glider, source) queue bound.
#:
#: The bound is per *pair*, not global, and that is the point: one glider
#: surfacing and dumping a mission's worth of dialog must not evict the
#: connection events of the five gliders still in the water.
#:
#: A surfacing measured on osusim delivered 437 dialog lines in about ten
#: milliseconds, so a bound below that would drop routinely during normal
#: operation rather than only when an engine is genuinely too slow.
DEFAULT_PAIR_MAXSIZE = 2048

#: Every source tag an engine may see, for validation at start rather
#: than a typo surfacing as an arm that never fires.
SOURCES = frozenset(
    {
        "dialog",
        "dialog.raw",
        "connections",
        "scripts",
        "zmodem",
        "deployment",
        "result",
        "error",
        "dropped",
        "stream",
        "tick",
    }
)

#: Sources that :class:`FleetStream` can subscribe to on a session.
_SUBSCRIBABLE = {
    "dialog": "dialog",
    "dialog.raw": "dialog",
    "connections": "connections",
    "scripts": "scripts",
    "zmodem": "zmodem",
    "deployment": "deployment",
}


@dataclass(frozen=True)
class Event:
    """One thing that happened, tagged with the glider it concerns.

    Attributes:
        glider: Which glider this concerns.  **Required, never None.**
            A single-glider engine can ignore it; a formation engine
            cannot function without it, and making it optional would
            mean every multi-glider engine starts with a ``None`` check
            that can only ever be dead code.
        source: One of :data:`SOURCES`.  Engines switch on this.
        body: Payload — ``str`` for dialog, ``dict`` for STOMP topics,
            a notice object for ``dropped`` and ``stream``.
        received_at: ``time.time()`` when this arrived **on this host**.
            Named explicitly because the confusion is otherwise
            guaranteed: glider time appears *inside* dialog text
            (``Curr Time: ...``) and the two can differ by an hour on a
            simulator — 48 minutes, measured.
        seq: Monotonic counter across the whole merge, assigned at
            publish.  Gives a total order over every glider and source.
        request_id: Set on ``result`` / ``error``.
        tag: Caller's label, echoed back on results.
    """

    glider: str
    source: str
    body: Any
    received_at: float
    seq: int
    request_id: int | None = None
    tag: str | None = None


@dataclass(frozen=True)
class DroppedNotice:
    """Body of a ``dropped`` event: the engine fell behind.

    Attributes:
        source: Which source was dropped for this glider.
        count: How many events were lost since the last notice.
        reason: ``"drained"`` when reported after the burst passed,
            ``"saturating"`` when reported mid-burst because a queue's
            worth had already been lost.
    """

    source: str
    count: int
    reason: str = "drained"


@dataclass(frozen=True)
class StreamNotice:
    """Body of a ``stream`` event: a glider's stream changed state.

    Attributes:
        state: ``"connected"``, ``"reconnected"``, or ``"disconnected"``.
        epoch: The session's epoch, which increments once per subscribed
            session.  A consumer that sees the epoch change knows the
            stream dropped and reconnected, and therefore that events
            published during the gap are gone — SFMC's live topics offer
            no cursor, and a surfacing arrives as one burst, so a gap
            loses all of it rather than degrading it.
    """

    state: str
    epoch: int


@dataclass
class _Pair:
    """One (glider, source) queue, bounded, drop-oldest."""

    items: deque[Event]
    dropped: int = 0
    reported_at: int = 0


class EventMerge:
    """N (glider, source) streams into one ordered queue.

    Pure: no I/O, no sessions, no SFMC.  Producers call :meth:`publish`
    from any thread; one consumer calls :meth:`get`.

    Ordering is by arrival: every event is stamped with a monotonic
    ``seq`` at publish, and :meth:`get` always returns the lowest
    outstanding one.  So order *within* a (glider, source) pair is
    strictly preserved, and across pairs it is arrival order.

    Args:
        maxsize: Per-pair queue bound.  See :data:`DEFAULT_PAIR_MAXSIZE`.
        now: Clock for ``received_at``, injectable for tests.
    """

    def __init__(
        self,
        maxsize: int = DEFAULT_PAIR_MAXSIZE,
        now: Callable[[], float] = time.time,
    ) -> None:
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1")
        self._maxsize = maxsize
        self._now = now
        self._pairs: dict[tuple[str, str], _Pair] = {}
        self._seq = itertools.count()
        self._lock = threading.Lock()
        self._ready = threading.Condition(self._lock)
        self._closed = False

    # ── Producing ────────────────────────────────────────────────────

    def publish(
        self,
        glider: str,
        source: str,
        body: Any,
        *,
        request_id: int | None = None,
        tag: str | None = None,
    ) -> Event | None:
        """Add an event.  Returns it, or ``None`` if the merge is closed.

        Never blocks and never raises on a full queue: the oldest event
        for that pair is dropped and counted.  Unbounded queues turn a
        slow engine into an OOM kill hours later, which is far harder to
        diagnose than a logged drop.
        """
        with self._lock:
            if self._closed:
                return None
            event = Event(
                glider=glider,
                source=source,
                body=body,
                received_at=self._now(),
                seq=next(self._seq),
                request_id=request_id,
                tag=tag,
            )
            pair = self._pairs.get((glider, source))
            if pair is None:
                pair = _Pair(items=deque())
                self._pairs[(glider, source)] = pair
            if len(pair.items) >= self._maxsize:
                pair.items.popleft()
                pair.dropped += 1
            pair.items.append(event)
            self._ready.notify()
            return event

    # ── Consuming ────────────────────────────────────────────────────

    def get(self, timeout: float | None = None) -> Event | None:
        """Return the next event, or ``None`` on timeout or close."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while True:
                event = self._take_locked()
                if event is not None:
                    return event
                if self._closed:
                    return None
                if deadline is None:
                    self._ready.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    self._ready.wait(remaining)

    def __iter__(self) -> Iterator[Event]:
        """Yield events until the merge is closed."""
        while True:
            event = self.get()
            if event is None:
                return
            yield event

    def _take_locked(self) -> Event | None:
        notice = self._drop_notice_locked()
        if notice is not None:
            return notice
        best: _Pair | None = None
        for pair in self._pairs.values():
            if not pair.items:
                continue
            if best is None or pair.items[0].seq < best.items[0].seq:
                best = pair
        if best is None:
            return None
        return best.items.popleft()

    def _drop_notice_locked(self) -> Event | None:
        """Report drops once the consumer has caught up on that pair.

        Reported when the pair drains, so a burst yields one notice
        carrying the whole count rather than one per lost event.  A pair
        that never drains would otherwise never report, so a full
        queue's worth of further loss also forces a notice — silent loss
        is the one outcome not on offer.
        """
        for (glider, source), pair in self._pairs.items():
            outstanding = pair.dropped - pair.reported_at
            if outstanding <= 0:
                continue
            if pair.items and outstanding < self._maxsize:
                continue
            pair.reported_at = pair.dropped
            return Event(
                glider=glider,
                source="dropped",
                body=DroppedNotice(
                    source=source,
                    count=outstanding,
                    reason="drained" if not pair.items else "saturating",
                ),
                received_at=self._now(),
                seq=next(self._seq),
            )
        return None

    # ── Introspection ────────────────────────────────────────────────

    def pending(self) -> int:
        """Events queued across every pair."""
        with self._lock:
            return sum(len(pair.items) for pair in self._pairs.values())

    def dropped(self) -> dict[tuple[str, str], int]:
        """Total drops per (glider, source), including already reported."""
        with self._lock:
            return {key: pair.dropped for key, pair in self._pairs.items() if pair.dropped}

    # ── Lifecycle ────────────────────────────────────────────────────

    def forget(self, glider: str) -> None:
        """Discard every queue for *glider*, e.g. once it has left."""
        with self._lock:
            for key in [k for k in self._pairs if k[0] == glider]:
                del self._pairs[key]

    def close(self) -> None:
        """Stop accepting events and release every waiter."""
        with self._lock:
            self._closed = True
            self._ready.notify_all()


class FleetStream:
    """Wires N :class:`~sfmc_api.session.GliderSession` into one merge.

    The only part of this module that touches the network.  Each glider
    gets its own session, and therefore its own STOMP connection,
    supervisor, and reconnect timer — so one glider's stream dropping
    does not blind the engine to the rest of the fleet.

    Events are published from each session's own pump thread, so this
    adds no threads of its own; the consumer's thread is the one that
    calls :meth:`get`.

    Args:
        client: An :class:`~sfmc_api.client.SFMCClient`.
        sources: Which sources to subscribe.  ``dialog`` (assembled
            lines) is the default and what almost every engine wants;
            ``dialog.raw`` gives chunks exactly as received.
        maxsize: Per-pair queue bound.
        merge: An existing :class:`EventMerge`, if you have one.
    """

    def __init__(
        self,
        client: SFMCClient,
        *,
        sources: tuple[str, ...] = ("dialog",),
        maxsize: int = DEFAULT_PAIR_MAXSIZE,
        merge: EventMerge | None = None,
    ) -> None:
        unknown = set(sources) - set(_SUBSCRIBABLE)
        if unknown:
            # Validated here rather than surfacing as a match arm that
            # silently never fires.
            raise ValueError(
                f"unsubscribable source(s): {sorted(unknown)}; choose from {sorted(_SUBSCRIBABLE)}"
            )
        if not sources:
            raise ValueError("at least one source is required")
        self._client = client
        self._sources = tuple(dict.fromkeys(sources))
        self._merge = merge if merge is not None else EventMerge(maxsize=maxsize)
        self._sessions: dict[str, GliderSession] = {}
        self._lock = threading.Lock()

    @property
    def merge(self) -> EventMerge:
        """The underlying queue."""
        return self._merge

    @property
    def gliders(self) -> tuple[str, ...]:
        """Names currently streaming, in the order they were added."""
        with self._lock:
            return tuple(self._sessions)

    def add_glider(self, name: str, session: GliderSession | None = None) -> GliderSession:
        """Start streaming *name* into the merge.

        A formation changes, so this is an ordinary call rather than
        startup-only configuration.

        Args:
            name: Registered glider name.
            session: An existing session to adopt, mainly for tests.
                When ``None`` one is created and started.
        """
        with self._lock:
            if name in self._sessions:
                raise ValueError(f"{name} is already streaming")
        topics = tuple(dict.fromkeys(_SUBSCRIBABLE[s] for s in self._sources))
        if session is None:
            session = self._client.session(name, topics=topics, start=False)
        self._wire(name, session)
        with self._lock:
            self._sessions[name] = session
        # Started after wiring so nothing can arrive before it can be
        # delivered, and with no deadline so every retry -- including the
        # first connection -- belongs to the session's own supervisor.
        session.start(timeout=None)
        return session

    def remove_glider(self, name: str) -> None:
        """Stop streaming *name* and discard its queues."""
        with self._lock:
            session = self._sessions.pop(name, None)
        if session is None:
            raise ValueError(f"{name} is not streaming")
        session.close()
        self._merge.forget(name)

    def _wire(self, name: str, session: GliderSession) -> None:
        """Attach callbacks that publish this glider's events.

        Explicit closures rather than lambdas with default arguments:
        the late-binding trap is real here (every callback would capture
        the last glider in the loop), and naming the closures makes the
        capture obvious instead of clever.
        """
        publish = self._merge.publish

        def on_line(line: DialogLine, glider: str = name) -> None:
            publish(glider, "dialog", line.text)

        def on_raw(chunk: str, glider: str = name) -> None:
            publish(glider, "dialog.raw", chunk)

        def make_topic_callback(source: str) -> Callable[[Any], None]:
            def on_topic(message: Any, glider: str = name, tag: str = source) -> None:
                publish(glider, tag, message)

            return on_topic

        def on_connect(reconnected: bool, glider: str = name) -> None:
            publish(
                glider,
                "stream",
                StreamNotice(
                    state="reconnected" if reconnected else "connected",
                    epoch=session.epoch,
                ),
            )

        def on_disconnect(glider: str = name) -> None:
            publish(glider, "stream", StreamNotice(state="disconnected", epoch=session.epoch))

        for source in self._sources:
            if source == "dialog":
                session.on_line(on_line)
            elif source == "dialog.raw":
                session.on_raw_dialog(on_raw)
            else:
                session.on_event(_SUBSCRIBABLE[source], make_topic_callback(source))
        session.on_connect(on_connect)
        session.on_disconnect(on_disconnect)

    # ── Consuming ────────────────────────────────────────────────────

    def get(self, timeout: float | None = None) -> Event | None:
        """Next event from any glider.  See :meth:`EventMerge.get`."""
        return self._merge.get(timeout=timeout)

    def __iter__(self) -> Iterator[Event]:
        return iter(self._merge)

    # ── Lifecycle ────────────────────────────────────────────────────

    def close(self) -> None:
        """Close every session and the merge."""
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            session.close()
        self._merge.close()

    def __enter__(self) -> FleetStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
