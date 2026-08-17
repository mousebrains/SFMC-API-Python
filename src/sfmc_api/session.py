"""Supervised, multi-consumer event session for one glider.

A :class:`~sfmc_api.stomp.StompSubscription` feeds exactly one queue,
so exactly one consumer.  That is enough when a program does one thing
with a topic, but not when several parts of a program need the same
stream at once — logging the dialog *while* a command waits for its
reply, say.  Subscribing twice would work but would run the sequence
reordering twice over duplicate server traffic.

:class:`GliderSession` subscribes once per topic, runs the ordering and
line-reassembly pipeline once, and fans the result out to any number of
listeners and callbacks.  It reconnects through
:class:`~sfmc_api.stream_reconnect.StreamSupervisor`, so it behaves
like the long-running commands do.

Typical usage::

    with client.session("osu685", topics=["dialog"]) as session:
        session.on_line(lambda line: print(line.text))
        with session.dialog_listener() as listener:
            for line in listener:
                ...

Listener queues are bounded and drop their **oldest** entry when a slow
consumer falls behind, counting what was lost in
:attr:`Listener.dropped`.  A consumer that must not miss data — the
command channel — checks that count rather than trusting a silent
stream.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable, Iterable, Iterator
from queue import Empty, Full, Queue
from types import TracebackType
from typing import TYPE_CHECKING, Any

from .dialog_stream import DialogLine, LineAssembler, ordered_dialog
from .exceptions import APIError, SFMCError
from .stomp import StompConnection, StompSubscription
from .stream_reconnect import StreamSession, StreamSupervisor

if TYPE_CHECKING:  # pragma: no cover - import cycle avoidance
    from .client import SFMCClient
    from .disconnect_notify import DisconnectNotifier

__all__ = ["DEFAULT_LISTENER_MAXSIZE", "GliderSession", "Listener", "Topic"]

logger = logging.getLogger(__name__)


#: Default listener queue depth.  Deep enough that a consumer pausing
#: for a second of work does not lose a surfacing's worth of dialog
#: (a busy surfacing is a few hundred lines), shallow enough that a
#: consumer that has stopped reading entirely cannot grow without
#: bound.
DEFAULT_LISTENER_MAXSIZE = 2048

#: Topic keys accepted by :class:`GliderSession`.
Topic = str

_TOPICS: tuple[str, ...] = ("dialog", "connections", "scripts", "zmodem", "deployment")


class _Closed:
    """Sentinel published to listeners when a session shuts down."""


_CLOSED = _Closed()


class Listener[T]:
    """A bounded, drop-oldest view of one broadcast topic.

    Obtained from :meth:`GliderSession.listen` (or
    :meth:`GliderSession.dialog_listener`).  Iterating yields items
    until the session closes.  Detach with :meth:`close`, or use it as
    a context manager.
    """

    def __init__(self, maxsize: int = DEFAULT_LISTENER_MAXSIZE) -> None:
        self._queue: Queue[T | _Closed] = Queue(maxsize=maxsize)
        self._dropped = 0
        self._lock = threading.Lock()
        self._closed = threading.Event()
        self._detach: Callable[[Listener[T]], None] | None = None

    # ── Producer side ────────────────────────────────────────────────

    def _publish(self, item: T | _Closed) -> None:
        """Enqueue *item*, dropping the oldest entry if full."""
        while True:
            try:
                self._queue.put_nowait(item)
                return
            except Full:
                try:
                    self._queue.get_nowait()
                except Empty:  # pragma: no cover - raced with a consumer
                    continue
                with self._lock:
                    self._dropped += 1

    # ── Consumer side ────────────────────────────────────────────────

    @property
    def dropped(self) -> int:
        """How many items were discarded because this listener lagged.

        Non-zero means the data this listener saw has gaps.
        """
        with self._lock:
            return self._dropped

    def get(self, timeout: float | None = None) -> T | None:
        """Return the next item, or ``None`` once the session closed.

        Raises:
            queue.Empty: If *timeout* expires with nothing available.
        """
        item = self._queue.get(timeout=timeout)
        if isinstance(item, _Closed):
            # Put it back so every waiter sees the close, not just the
            # first one to notice it.
            with contextlib.suppress(Full):
                self._queue.put_nowait(item)
            return None
        return item

    def __iter__(self) -> Iterator[T]:
        while True:
            try:
                item = self.get(timeout=0.5)
            except Empty:
                if self._closed.is_set():
                    return
                continue
            if item is None:
                return
            yield item

    def close(self) -> None:
        """Detach from the session and stop iteration."""
        if self._closed.is_set():
            return
        self._closed.set()
        if self._detach is not None:
            self._detach(self)
        self._publish(_CLOSED)

    def __enter__(self) -> Listener[T]:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class _Broadcaster[T]:
    """Fans one stream out to N listeners and N callbacks."""

    def __init__(self) -> None:
        self._listeners: list[Listener[T]] = []
        self._callbacks: list[Callable[[T], None]] = []
        self._lock = threading.Lock()

    def attach(self, maxsize: int = DEFAULT_LISTENER_MAXSIZE) -> Listener[T]:
        listener: Listener[T] = Listener(maxsize=maxsize)
        listener._detach = self.detach
        with self._lock:
            self._listeners.append(listener)
        return listener

    def detach(self, listener: Listener[T]) -> None:
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def subscribe(self, callback: Callable[[T], None]) -> None:
        with self._lock:
            self._callbacks.append(callback)

    def publish(self, item: T) -> None:
        with self._lock:
            listeners = list(self._listeners)
            callbacks = list(self._callbacks)
        for listener in listeners:
            listener._publish(item)
        for callback in callbacks:
            # A caller's callback must not be able to kill the pump
            # thread and with it every other consumer of this topic.
            try:
                callback(item)
            except Exception:
                logger.exception("event callback failed; continuing")

    def close(self) -> None:
        with self._lock:
            listeners = list(self._listeners)
            self._listeners.clear()
        for listener in listeners:
            listener._publish(_CLOSED)


class GliderSession:
    """One supervised, fanned-out event stream for a single glider.

    Create with :meth:`SFMCClient.session`.  The session runs its
    STOMP connection on a background thread and reconnects on loss, so
    listeners and callbacks registered once stay valid across
    reconnects.

    Args:
        client: The API client to stream through.
        glider_name: Registered glider name.
        topics: Which topics to subscribe.  Any of ``dialog``,
            ``connections``, ``scripts``, ``zmodem``, ``deployment``.
            Subscribe only to what you consume — each topic costs a
            subscription and, for ``zmodem``/``deployment``, an extra
            HTTP lookup of the deployment id.
        stop: Shared shutdown signal.  One is created if omitted.
        notifier: Optional connect/disconnect notifier.
        log: Where session lifecycle messages go.
        reconnect: ``False`` makes a session loss fatal.
        **supervisor_kwargs: Backoff overrides forwarded to
            :class:`~sfmc_api.stream_reconnect.StreamSupervisor`.
    """

    def __init__(
        self,
        client: SFMCClient,
        glider_name: str,
        *,
        topics: Iterable[Topic] = ("dialog",),
        stop: threading.Event | None = None,
        notifier: DisconnectNotifier | None = None,
        log: logging.Logger = logger,
        reconnect: bool = True,
        **supervisor_kwargs: Any,
    ) -> None:
        requested = tuple(dict.fromkeys(topics))
        unknown = [topic for topic in requested if topic not in _TOPICS]
        if unknown:
            raise ValueError(f"Unknown topic(s): {', '.join(unknown)}; expected {_TOPICS}")
        if not requested:
            raise ValueError("At least one topic is required")

        self._client = client
        self._glider_name = glider_name
        self._topics = requested
        self._log = log
        self._stop = stop if stop is not None else threading.Event()

        self._dialog: _Broadcaster[DialogLine] = _Broadcaster()
        # Raw, pre-assembly dialog text.  Kept as a separate fan-out
        # because a consumer that matches against the stream needs the
        # unterminated tail the line assembler is still buffering — see
        # raw_dialog_listener().
        self._raw_dialog: _Broadcaster[str] = _Broadcaster()
        self._events: dict[str, _Broadcaster[Any]] = {
            topic: _Broadcaster() for topic in requested if topic != "dialog"
        }

        self._connect_callbacks: list[Callable[[bool], None]] = []
        self._disconnect_callbacks: list[Callable[[], None]] = []

        self._ready = threading.Event()
        self._epoch = 0
        self._epoch_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

        self._supervisor = StreamSupervisor(
            client,
            setup=self._setup,
            stop=self._stop,
            log=log,
            notifier=notifier,
            on_subscribed=self._on_subscribed,
            on_session_end=self._on_session_end,
            reconnect=reconnect,
            **supervisor_kwargs,
        )

    # ── Properties ───────────────────────────────────────────────────

    @property
    def glider_name(self) -> str:
        """The glider this session streams."""
        return self._glider_name

    @property
    def epoch(self) -> int:
        """Increments once per successfully subscribed session.

        A consumer that captured an epoch and later sees a different
        one knows the stream dropped and reconnected in between — and
        therefore that it may have missed data.
        """
        with self._epoch_lock:
            return self._epoch

    @property
    def connected(self) -> bool:
        """True while a session is subscribed."""
        return self._ready.is_set()

    @property
    def stop(self) -> threading.Event:
        """The shutdown signal; set it to end the session."""
        return self._stop

    # ── Lifecycle ────────────────────────────────────────────────────

    def start(self, timeout: float | None = 30.0) -> GliderSession:
        """Start streaming and wait for the first subscription.

        Args:
            timeout: Seconds to wait for the first session to
                subscribe.  ``None`` returns immediately, leaving the
                connection to come up in the background.

        Raises:
            APIError: If the first connection attempt failed
                permanently (bad credentials, unknown glider).
            TimeoutError: If no session subscribed within *timeout*.
        """
        if self._thread is not None:
            raise RuntimeError("Session already started")
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"sfmc-session-{self._glider_name}",
        )
        self._thread.start()
        if timeout is not None and not self.wait_ready(timeout):
            self.close()
            if self._error is not None:
                raise self._error
            raise TimeoutError(f"Session for {self._glider_name} did not subscribe in {timeout}s")
        return self

    def wait_ready(self, timeout: float | None = None) -> bool:
        """Block until a session is subscribed (or *timeout* expires)."""
        deadline_step = 0.1 if timeout is None else min(0.1, timeout)
        waited = 0.0
        while True:
            if self._ready.wait(timeout=deadline_step):
                return True
            if self._error is not None or self._stop.is_set():
                return False
            waited += deadline_step
            if timeout is not None and waited >= timeout:
                return False

    def close(self) -> None:
        """Stop streaming and release every listener."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10.0)
            self._thread = None
        self._dialog.close()
        self._raw_dialog.close()
        for broadcaster in self._events.values():
            broadcaster.close()
        self._ready.clear()

    def __enter__(self) -> GliderSession:
        if self._thread is None:
            self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ── Consumer registration ────────────────────────────────────────

    def dialog_listener(self, maxsize: int = DEFAULT_LISTENER_MAXSIZE) -> Listener[DialogLine]:
        """Attach a listener to the reassembled dialog line stream."""
        self._require_topic("dialog")
        return self._dialog.attach(maxsize=maxsize)

    def raw_dialog_listener(self, maxsize: int = DEFAULT_LISTENER_MAXSIZE) -> Listener[str]:
        """Attach a listener to the raw, pre-assembly dialog text.

        Yields each sequence-ordered chunk exactly as it arrived, with
        no line reassembly: chunks may hold half a line, or three lines
        and a bit.

        Use this instead of :meth:`dialog_listener` when you match
        against the stream rather than against lines.  A GliderDos
        prompt carries no trailing newline, so it never becomes a
        complete line — it sits in the assembler's buffer and is
        discarded at the session boundary, meaning a line consumer
        never sees an idle prompt at all.  Nine of the twenty known
        SFMC scripts trigger on that prompt, so for them the difference
        is between working and hanging forever.

        Costs the caller its own reassembly if it also wants lines.
        """
        self._require_topic("dialog")
        return self._raw_dialog.attach(maxsize=maxsize)

    def on_raw_dialog(self, callback: Callable[[str], None]) -> None:
        """Call *callback* for every raw dialog chunk.

        Same thread rules as :meth:`on_line` — the callback runs on the
        pump thread, so slow work there delays every other consumer.
        """
        self._require_topic("dialog")
        self._raw_dialog.subscribe(callback)

    def listen(
        self, topic: Topic, maxsize: int = DEFAULT_LISTENER_MAXSIZE
    ) -> Listener[Any] | Listener[DialogLine]:
        """Attach a listener to any subscribed topic."""
        self._require_topic(topic)
        if topic == "dialog":
            return self._dialog.attach(maxsize=maxsize)
        return self._events[topic].attach(maxsize=maxsize)

    def on_line(self, callback: Callable[[DialogLine], None]) -> None:
        """Call *callback* for every reassembled dialog line.

        Callbacks run on the pump thread, so a slow one delays every
        other consumer of that topic.  Do real work on your own thread
        — attach a :class:`Listener` for that.  A callback that raises
        is logged and skipped, never fatal.
        """
        self._require_topic("dialog")
        self._dialog.subscribe(callback)

    def on_event(self, topic: Topic, callback: Callable[[Any], None]) -> None:
        """Call *callback* for every message on a non-dialog topic."""
        self._require_topic(topic)
        if topic == "dialog":
            raise ValueError("Use on_line() for the dialog topic")
        self._events[topic].subscribe(callback)

    def on_connect(self, callback: Callable[[bool], None]) -> None:
        """Call *callback(reconnected)* each time a session subscribes."""
        self._connect_callbacks.append(callback)

    def on_disconnect(self, callback: Callable[[], None]) -> None:
        """Call *callback* each time a session ends."""
        self._disconnect_callbacks.append(callback)

    def _require_topic(self, topic: Topic) -> None:
        if topic not in self._topics:
            raise ValueError(
                f"Topic {topic!r} is not subscribed by this session "
                f"(subscribed: {', '.join(self._topics)})"
            )

    # ── Supervisor plumbing ──────────────────────────────────────────

    def _run(self) -> None:
        try:
            self._supervisor.run()
        except BaseException as exc:
            self._error = exc
            self._log.error(
                "session for %s ended: %s: %s",
                self._glider_name,
                type(exc).__name__,
                exc,
            )
        finally:
            self._ready.clear()
            self._dialog.close()
            self._raw_dialog.close()
            for broadcaster in self._events.values():
                broadcaster.close()

    def _setup(self, stomp: StompConnection) -> StreamSession:
        subscriptions: list[StompSubscription] = []
        workers: list[tuple[str, Callable[[], None]]] = []

        for topic in self._topics:
            sub = self._subscribe(topic, stomp)
            subscriptions.append(sub)
            if topic == "dialog":
                workers.append(("dialog", self._make_dialog_pump(sub)))
            else:
                workers.append((topic, self._make_event_pump(topic, sub)))

        return StreamSession(subscriptions=subscriptions, workers=workers)

    def _subscribe(self, topic: Topic, stomp: StompConnection) -> StompSubscription:
        client = self._client
        name = self._glider_name
        if topic == "dialog":
            return client.subscribe_glider_output(name, stomp)
        if topic == "connections":
            return client.subscribe_connection_events(name, stomp)
        if topic == "scripts":
            return client.subscribe_script_events(name, stomp)
        if topic == "zmodem":
            return client.subscribe_zmodem_transfer_events(name, stomp)
        if topic == "deployment":
            return client.subscribe_deployment_events(name, stomp)
        raise APIError(0, f"Unknown topic {topic!r}")  # pragma: no cover - guarded in __init__

    def _make_dialog_pump(self, sub: StompSubscription) -> Callable[[], None]:
        def pump() -> None:
            assembler = LineAssembler()
            for data in ordered_dialog(sub):
                if self._stop.is_set():
                    break
                # Raw first: a stream matcher must see a prompt at the
                # moment it arrives, not once something later terminates
                # the line it sits on.
                self._raw_dialog.publish(data)
                for line in assembler.feed(data):
                    self._dialog.publish(line)
            # An unterminated tail at a session boundary is dropped:
            # the glider does not re-send it, but a half line delivered
            # as if it were whole would corrupt a parse downstream.
            if assembler.pending.strip():
                self._log.warning(
                    "stream boundary discarded %d-byte unterminated fragment",
                    len(assembler.pending.encode("utf-8")),
                )

        return pump

    def _make_event_pump(self, topic: str, sub: StompSubscription) -> Callable[[], None]:
        broadcaster = self._events[topic]

        def pump() -> None:
            for message in sub:
                if self._stop.is_set():
                    break
                broadcaster.publish(message)

        return pump

    def _on_subscribed(self, *, reconnected: bool = False) -> None:
        with self._epoch_lock:
            self._epoch += 1
        self._ready.set()
        for callback in list(self._connect_callbacks):
            try:
                callback(reconnected)
            except Exception:
                logger.exception("on_connect callback failed; continuing")

    def _on_session_end(self) -> None:
        self._ready.clear()
        for callback in list(self._disconnect_callbacks):
            try:
                callback()
            except Exception:
                logger.exception("on_disconnect callback failed; continuing")

    # ── Convenience ──────────────────────────────────────────────────

    def glider_is_connected(self) -> bool:
        """True unless SFMC reports the glider's state as disconnected.

        Unknown or missing states count as connected, the conservative
        choice for callers that defer work while a glider is down.
        """
        return _glider_is_connected(self._client, self._glider_name)


def _glider_is_connected(client: SFMCClient, glider_name: str) -> bool:
    """True unless the glider's state is reported as ``disconnected``.

    Unknown or missing states count as connected, the conservative
    choice: it defers possibly in-flight work rather than acting on a
    guess.
    """
    try:
        details = client.get_glider_details(glider_name)
        state = details["data"]["state"]
    except (SFMCError, KeyError, TypeError) as exc:
        logger.warning("could not determine glider state (%s); assuming connected", exc)
        return True
    return str(state) != "disconnected"
