"""STOMP-over-SockJS client for real-time SFMC event streaming.

The SFMC server exposes a SockJS endpoint for STOMP messaging, used to
stream real-time events such as glider connections, dialog output,
script changes, Zmodem transfers, and deployment updates.

This module handles the SockJS WebSocket transport framing and the
STOMP protocol (CONNECT, SUBSCRIBE, MESSAGE, DISCONNECT).

Typical usage::

    from sfmc_api import SFMCClient

    with SFMCClient() as client:
        with client.open_stream() as stomp:
            sub = client.subscribe_connection_events("osu684", stomp)
            for event in sub:
                print(event)

See :doc:`/docs/streaming` for detailed data-flow documentation.
"""

from __future__ import annotations

import contextlib
import json
import logging
import random
import ssl
import string
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from queue import Empty, Full, Queue
from typing import Any

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import ClientConnection
from websockets.sync.client import connect as ws_connect

from .config import SFMCConfig
from .exceptions import SFMCError

__all__ = ["MAX_SEQUENCE", "StompConnection", "StompError", "StompSubscription"]

logger = logging.getLogger(__name__)

#: Maximum STOMP sequence number used by the SFMC server.
#: After this value the sequence wraps back to 0.
MAX_SEQUENCE = 9007199254740991


class StompError(SFMCError):
    """Error during STOMP communication."""


# ── STOMP frame helpers ──────────────────────────────────────────────


def _encode_frame(command: str, headers: dict[str, str] | None = None, body: str = "") -> str:
    """Build a STOMP frame string."""
    if headers is None:
        headers = {}
    lines = [command]
    for key, value in headers.items():
        lines.append(f"{key}:{value}")
    lines.append("")
    lines.append(body)
    return "\n".join(lines) + "\0"


@dataclass
class StompFrame:
    """A parsed STOMP frame."""

    command: str
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""


def _parse_frame(raw: str) -> StompFrame:
    """Parse a STOMP frame string into a :class:`StompFrame`."""
    # Strip any leading/trailing whitespace and null bytes
    raw = raw.strip().rstrip("\0")
    if not raw:
        return StompFrame(command="HEARTBEAT")

    parts = raw.split("\n\n", 1)
    header_section = parts[0]
    body = parts[1] if len(parts) > 1 else ""

    lines = header_section.split("\n")
    command = lines[0]
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key] = value

    return StompFrame(command=command, headers=headers, body=body)


# ── SockJS helpers ───────────────────────────────────────────────────


def _sockjs_url(config: SFMCConfig, token: str) -> str:
    """Build the SockJS WebSocket transport URL.

    SockJS WebSocket URLs follow the pattern::

        wss://{host}/path/{server_id}/{session_id}/websocket

    The ``server_id`` is a random 3-digit number and ``session_id``
    is a random string.  The access token is passed as a query
    parameter.
    """
    server_id = str(random.randint(100, 999))
    session_id = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    scheme = "wss"
    return (
        f"{scheme}://{config.host}/sfmc/api/sfmc-stomp"
        f"/{server_id}/{session_id}/websocket"
        f"?access_token={token}"
    )


def _sockjs_decode(data: str) -> list[str] | None:
    """Decode a SockJS message frame.

    SockJS wraps STOMP frames in JSON arrays preceded by a type
    character:

    * ``o`` — open frame (connection established)
    * ``h`` — heartbeat
    * ``c`` — close frame (returns ``None`` to signal closure)
    * ``a[...]`` — array of message strings

    Returns a list of STOMP frame strings (may be empty), or
    ``None`` when a SockJS close frame is received.
    """
    if not data:
        return []

    frame_type = data[0]

    if frame_type == "o":
        return []
    if frame_type == "h":
        return []
    if frame_type == "c":
        logger.debug("SockJS close frame: %s", data)
        return None
    if frame_type == "a":
        try:
            messages = json.loads(data[1:])
            return messages if isinstance(messages, list) else []
        except json.JSONDecodeError:
            logger.warning("Failed to decode SockJS message: %s", data[:200])
            return []

    logger.debug("Unknown SockJS frame type %r: %s", frame_type, data[:200])
    return []


# ── STOMP subscription ───────────────────────────────────────────────


class StompSubscription:
    """An active STOMP topic subscription.

    Provides an iterator interface to receive messages.  Call
    :meth:`close` to unsubscribe and stop iteration.
    """

    def __init__(
        self,
        sub_id: str,
        topic: str,
        queue: Queue[dict[str, Any] | StompError | None],
        connection: StompConnection | None = None,
    ) -> None:
        self._id = sub_id
        self._topic = topic
        self._queue: Queue[dict[str, Any] | StompError | None] = queue
        self._connection = connection
        self._closed = threading.Event()

    @property
    def topic(self) -> str:
        """The STOMP topic this subscription is listening to."""
        return self._topic

    def __iter__(self) -> Iterator[dict[str, Any]]:
        """Yield parsed JSON messages until the subscription is closed."""
        while True:
            try:
                msg = self.get(timeout=1.0)
            except Empty:
                continue
            if msg is None:
                break
            yield msg

    def get(self, timeout: float | None = None) -> dict[str, Any] | None:
        """Get the next message, or ``None`` if the subscription closed.

        After :meth:`close`, messages still buffered in the queue are
        drained first; once the queue is empty, ``None`` is returned
        immediately instead of blocking.

        Args:
            timeout: Seconds to wait.  ``None`` blocks indefinitely.
                Raises :class:`queue.Empty` if *timeout* expires with
                no message.

        Raises:
            StompError: If a STOMP ERROR frame was received.
        """
        if self._closed.is_set():
            try:
                msg = self._queue.get_nowait()
            except Empty:
                return None
        else:
            try:
                msg = self._queue.get(timeout=timeout)
            except Empty:
                if self._closed.is_set():
                    return None
                raise
        if isinstance(msg, StompError):
            raise msg
        return msg

    def close(self) -> None:
        """Unsubscribe from the topic and stop iteration.

        Sends a STOMP ``UNSUBSCRIBE`` frame to the server, removes
        this subscription from the connection registry, and signals
        the iterator to stop.

        Never blocks: on a bounded queue that is already full, the
        ``None`` sentinel is skipped and the closed flag alone ends
        iteration — :meth:`get` checks it once the backlog drains.
        """
        if self._closed.is_set():
            return
        self._closed.set()
        if self._connection is not None:
            self._connection._unsubscribe(self._id)
        with contextlib.suppress(Full):
            self._queue.put_nowait(None)


# ── STOMP connection ─────────────────────────────────────────────────


class StompConnection:
    """A STOMP-over-SockJS connection to the SFMC server.

    Manages the WebSocket connection, STOMP handshake, and
    subscriptions.  Runs a background thread to receive messages
    and dispatch them to subscription queues.

    Use as a context manager::

        with StompConnection(config, token) as stomp:
            sub = stomp.subscribe("/topic/glider-connections-8")
            for event in sub:
                print(event)
    """

    def __init__(self, config: SFMCConfig, token: str, heartbeat_interval: int = 0) -> None:
        self._config = config
        self._token = token
        self._heartbeat_interval = heartbeat_interval
        self._ws: ClientConnection | None = None
        self._lock = threading.Lock()
        self._subscriptions: dict[str, StompSubscription] = {}
        self._sub_topics: dict[str, str] = {}  # sub_id → topic
        self._next_sub_id = 0
        self._receiver_thread: threading.Thread | None = None
        self._connected = False
        self._closing = threading.Event()
        self._disconnect_event = threading.Event()

    def __enter__(self) -> StompConnection:
        with self._lock:
            connected = self._connected
        if not connected:
            self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.disconnect()

    def connect(self) -> None:
        """Open the WebSocket and perform the STOMP handshake.

        Raises:
            StompError: If already connected, or if the connection or
                handshake fails.  Connecting twice without an
                intervening :meth:`disconnect` would leak the previous
                WebSocket and receiver thread.
        """
        with self._lock:
            if self._connected:
                raise StompError("Already connected — call disconnect() first")
        if self._receiver_thread is not None and self._receiver_thread.is_alive():
            # A lingering receiver (disconnect() join timed out) would
            # clobber the new connection's state from its teardown.
            raise StompError("Previous receiver thread has not exited yet")
        # Reset lifecycle events so the object can be reused after a
        # disconnect; a stale _closing flag would make the new
        # receiver thread exit immediately.
        self._closing.clear()
        self._disconnect_event.clear()

        # Honor stomp_debug from config — enable DEBUG logging for this module.
        if self._config.stomp_debug:
            logger.setLevel(logging.DEBUG)

        url = _sockjs_url(self._config, self._token)
        logger.debug("Connecting to wss://%s/sfmc/api/sfmc-stomp/...", self._config.host)

        # Honor tls_verify from config, matching the HTTP client behavior.
        ssl_context: ssl.SSLContext | None = None
        if not self._config.tls_verify:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        try:
            self._ws = ws_connect(
                url,
                additional_headers={},
                open_timeout=10,
                close_timeout=5,
                ssl=ssl_context,
            )
        except Exception as exc:
            raise StompError(f"WebSocket connection failed: {exc}") from exc

        try:
            # Wait for SockJS open frame
            open_frame = self._ws.recv(timeout=10)
            logger.debug("SockJS open: %s", open_frame)

            # Send STOMP CONNECT
            connect_frame = _encode_frame(
                "CONNECT",
                {"accept-version": "1.2", "heart-beat": f"0,{self._heartbeat_interval}"},
            )
            self._ws.send(json.dumps([connect_frame]))
            logger.debug("Sent STOMP CONNECT")

            # Wait for STOMP CONNECTED
            resp = str(self._ws.recv(timeout=10))
            messages = _sockjs_decode(resp) or []
            for msg in messages:
                frame = _parse_frame(msg)
                if frame.command == "CONNECTED":
                    with self._lock:
                        self._connected = True
                    logger.debug("STOMP CONNECTED: %s", frame.headers)
                    break
                elif frame.command == "ERROR":
                    raise StompError(f"STOMP connection refused: {frame.body}")

            with self._lock:
                connected = self._connected
            if not connected:
                raise StompError("Did not receive STOMP CONNECTED frame")
        except StompError:
            self._close_ws()
            raise
        except Exception as exc:
            self._close_ws()
            raise StompError(f"STOMP handshake failed: {exc}") from exc

        # Start receiver thread
        self._receiver_thread = threading.Thread(
            target=self._receive_loop,
            daemon=True,
            name="sfmc-stomp-receiver",
        )
        self._receiver_thread.start()

    def disconnect(self) -> None:
        """Send STOMP DISCONNECT and close the WebSocket."""
        self._closing.set()

        # Close all subscriptions
        with self._lock:
            subs = list(self._subscriptions.values())
            self._subscriptions.clear()
            self._sub_topics.clear()
        for sub in subs:
            sub.close()

        with self._lock:
            connected = self._connected
        if self._ws is not None and connected:
            try:
                disconnect_frame = _encode_frame("DISCONNECT", {"receipt": "disc-1"})
                self._ws.send(json.dumps([disconnect_frame]))
            except Exception:
                pass  # best-effort disconnect

        self._close_ws()
        with self._lock:
            self._connected = False

        if self._receiver_thread is not None:
            self._receiver_thread.join(timeout=5)

    def _close_ws(self) -> None:
        """Close the WebSocket connection, ignoring errors."""
        if self._ws is not None:
            with contextlib.suppress(Exception):
                self._ws.close()

    def wait_disconnected(self, timeout: float | None = None) -> bool:
        """Block until the connection drops.

        Args:
            timeout: Seconds to wait.  ``None`` blocks indefinitely.

        Returns:
            ``True`` if disconnected, ``False`` if the timeout expired.
        """
        return self._disconnect_event.wait(timeout=timeout)

    @property
    def disconnected(self) -> bool:
        """True if the connection has been lost or closed."""
        return self._disconnect_event.is_set()

    def subscribe(self, topic: str, maxsize: int = 0) -> StompSubscription:
        """Subscribe to a STOMP topic.

        Args:
            topic: The STOMP destination
                (e.g. ``"/topic/glider-connections-8"``).
            maxsize: Maximum number of messages to buffer in the
                subscription queue.  ``0`` (default) means unlimited.

        Returns:
            A :class:`StompSubscription` that yields parsed JSON
            messages.

        Raises:
            StompError: If not connected, or if sending the
                ``SUBSCRIBE`` frame fails (the subscription is then
                not registered).
        """
        with self._lock:
            if not self._connected:
                raise StompError("Not connected — call connect() first")

            sub_id = f"sub-{self._next_sub_id}"
            self._next_sub_id += 1

            queue: Queue[dict[str, Any] | StompError | None] = Queue(maxsize=maxsize)
            sub = StompSubscription(sub_id, topic, queue, connection=self)
            self._subscriptions[sub_id] = sub
            self._sub_topics[sub_id] = topic

        subscribe_frame = _encode_frame(
            "SUBSCRIBE",
            {"id": sub_id, "destination": topic},
        )
        assert self._ws is not None  # guaranteed after connect()
        try:
            self._ws.send(json.dumps([subscribe_frame]))
        except Exception as exc:
            # Roll back the registration: a subscription the server
            # never saw must not linger in the registry, holding its
            # queue and receiving dispatches if its ID is ever reused.
            with self._lock:
                self._subscriptions.pop(sub_id, None)
                self._sub_topics.pop(sub_id, None)
            raise StompError(f"SUBSCRIBE for {topic} failed: {exc}") from exc
        logger.debug("Subscribed %s to %s", sub_id, topic)

        return sub

    def _unsubscribe(self, sub_id: str) -> None:
        """Send UNSUBSCRIBE frame and remove from registry.

        Called by :meth:`StompSubscription.close`.
        """
        with self._lock:
            self._subscriptions.pop(sub_id, None)
            self._sub_topics.pop(sub_id, None)
            connected = self._connected

        if connected and self._ws is not None:
            try:
                frame = _encode_frame("UNSUBSCRIBE", {"id": sub_id})
                self._ws.send(json.dumps([frame]))
                logger.debug("Unsubscribed %s", sub_id)
            except Exception:
                pass  # best-effort during teardown

    def _receive_loop(self) -> None:
        """Background thread: receive WebSocket messages and dispatch."""
        assert self._ws is not None  # guaranteed: thread starts after connect()
        try:
            while not self._closing.is_set():
                try:
                    raw = str(self._ws.recv(timeout=1))
                except TimeoutError:
                    continue
                except ConnectionClosed:
                    logger.debug("WebSocket connection closed")
                    break
                except Exception as exc:
                    if not self._closing.is_set():
                        logger.warning("WebSocket recv error: %s", exc)
                    break

                decoded = _sockjs_decode(raw)
                if decoded is None:
                    logger.info("SockJS close frame received")
                    break
                for msg_str in decoded:
                    frame = _parse_frame(msg_str)

                    if frame.command == "MESSAGE":
                        sub_id = frame.headers.get("subscription", "")
                        with self._lock:
                            sub = self._subscriptions.get(sub_id)
                        if sub is not None:
                            try:
                                payload: dict[str, Any] = json.loads(frame.body)
                            except json.JSONDecodeError:
                                payload = {"_raw": frame.body}
                            try:
                                sub._queue.put_nowait(payload)
                            except Full:
                                logger.warning(
                                    "Subscription %s queue full, dropping message",
                                    sub._id,
                                )
                    elif frame.command == "ERROR":
                        logger.error("STOMP ERROR: %s", frame.body)
                        err = StompError(f"STOMP server error: {frame.body}")
                        with self._lock:
                            subs = list(self._subscriptions.values())
                        for sub in subs:
                            with contextlib.suppress(Full):
                                sub._queue.put_nowait(err)
                    elif frame.command == "HEARTBEAT":
                        pass
                    else:
                        logger.debug("STOMP frame: %s", frame.command)
        finally:
            # The connection is unusable once this thread exits: clear
            # the connected flag first so subscribe() fails fast
            # instead of writing to a dead socket, then signal waiters
            # and close the subscriptions.
            with self._lock:
                self._connected = False
            self._disconnect_event.set()
            with self._lock:
                remaining = list(self._subscriptions.values())
            for sub in remaining:
                sub.close()
