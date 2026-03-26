"""STOMP-over-SockJS client for real-time SFMC event streaming.

The SFMC server exposes a SockJS endpoint for STOMP messaging, used to
stream real-time events such as glider connections, dialog output,
script changes, Zmodem transfers, and deployment updates.

This module handles the SockJS WebSocket transport framing and the
STOMP protocol (CONNECT, SUBSCRIBE, MESSAGE, DISCONNECT).

Typical usage::

    from sfmc_api import SFMCClient

    with SFMCClient() as client:
        with client.stream("osu684") as stream:
            for event in stream.connection_events():
                print(event)

See :doc:`/docs/streaming` for detailed data-flow documentation.
"""

from __future__ import annotations

import json
import logging
import random
import string
import threading
from collections.abc import Generator
from dataclasses import dataclass, field
from queue import Queue
from typing import Any

from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect as ws_connect

from .config import SFMCConfig
from .exceptions import SFMCError

logger = logging.getLogger(__name__)

# Maximum STOMP sequence number used by the SFMC server.  After this
# value the sequence wraps back to 0.
_MAX_SEQUENCE = 9007199254740991


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


def _sockjs_decode(data: str) -> list[str]:
    """Decode a SockJS message frame.

    SockJS wraps STOMP frames in JSON arrays preceded by a type
    character:

    * ``o`` — open frame (connection established)
    * ``h`` — heartbeat
    * ``c`` — close frame
    * ``a[...]`` — array of message strings

    Returns a list of STOMP frame strings (may be empty).
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
        return []
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

    Provides an iterator interface to receive messages.  Use
    :meth:`close` or the context manager protocol to unsubscribe.
    """

    def __init__(self, sub_id: str, topic: str, queue: Queue[dict[str, Any] | None]) -> None:
        self._id = sub_id
        self._topic = topic
        self._queue: Queue[dict[str, Any] | None] = queue
        self._closed = False

    @property
    def topic(self) -> str:
        """The STOMP topic this subscription is listening to."""
        return self._topic

    def __iter__(self) -> Generator[dict[str, Any], None, None]:
        """Yield parsed JSON messages until the subscription is closed."""
        while True:
            msg = self._queue.get()
            if msg is None:
                break
            yield msg

    def get(self, timeout: float | None = None) -> dict[str, Any] | None:
        """Get the next message, or ``None`` if the subscription closed.

        Args:
            timeout: Seconds to wait.  ``None`` blocks indefinitely.
                Raises :class:`queue.Empty` if *timeout* expires with
                no message.
        """
        return self._queue.get(timeout=timeout)

    def close(self) -> None:
        """Signal this subscription to stop iterating."""
        if not self._closed:
            self._closed = True
            self._queue.put(None)


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

    def __init__(self, config: SFMCConfig, token: str) -> None:
        self._config = config
        self._token = token
        self._ws: Any = None
        self._subscriptions: dict[str, StompSubscription] = {}
        self._sub_topics: dict[str, str] = {}  # sub_id → topic
        self._next_sub_id = 0
        self._receiver_thread: threading.Thread | None = None
        self._connected = False
        self._closing = False

    def __enter__(self) -> StompConnection:
        if not self._connected:
            self.connect()
        return self

    def __exit__(self, *exc: object) -> None:
        self.disconnect()

    def connect(self) -> None:
        """Open the WebSocket and perform the STOMP handshake.

        Raises:
            StompError: If the connection or handshake fails.
        """
        url = _sockjs_url(self._config, self._token)
        logger.debug("Connecting to %s", url)

        try:
            self._ws = ws_connect(
                url,
                additional_headers={},
                open_timeout=10,
                close_timeout=5,
            )
        except Exception as exc:
            raise StompError(f"WebSocket connection failed: {exc}") from exc

        # Wait for SockJS open frame
        try:
            open_frame = self._ws.recv(timeout=10)
            logger.debug("SockJS open: %s", open_frame)
        except Exception as exc:
            raise StompError(f"No SockJS open frame: {exc}") from exc

        # Send STOMP CONNECT
        connect_frame = _encode_frame(
            "CONNECT",
            {"accept-version": "1.2", "heart-beat": "0,0"},
        )
        self._ws.send(json.dumps([connect_frame]))
        logger.debug("Sent STOMP CONNECT")

        # Wait for STOMP CONNECTED
        try:
            resp = self._ws.recv(timeout=10)
            messages = _sockjs_decode(resp)
            for msg in messages:
                frame = _parse_frame(msg)
                if frame.command == "CONNECTED":
                    self._connected = True
                    logger.debug("STOMP CONNECTED: %s", frame.headers)
                    break
                elif frame.command == "ERROR":
                    raise StompError(f"STOMP connection refused: {frame.body}")
        except StompError:
            raise
        except Exception as exc:
            raise StompError(f"STOMP handshake failed: {exc}") from exc

        if not self._connected:
            raise StompError("Did not receive STOMP CONNECTED frame")

        # Start receiver thread
        self._receiver_thread = threading.Thread(
            target=self._receive_loop,
            daemon=True,
            name="sfmc-stomp-receiver",
        )
        self._receiver_thread.start()

    def disconnect(self) -> None:
        """Send STOMP DISCONNECT and close the WebSocket."""
        self._closing = True

        # Close all subscriptions
        for sub in self._subscriptions.values():
            sub.close()
        self._subscriptions.clear()
        self._sub_topics.clear()

        if self._ws is not None and self._connected:
            try:
                disconnect_frame = _encode_frame("DISCONNECT", {"receipt": "disc-1"})
                self._ws.send(json.dumps([disconnect_frame]))
            except Exception:
                pass  # best-effort disconnect

        if self._ws is not None:
            import contextlib

            with contextlib.suppress(Exception):
                self._ws.close()

        self._connected = False

        if self._receiver_thread is not None:
            self._receiver_thread.join(timeout=5)

    def subscribe(self, topic: str) -> StompSubscription:
        """Subscribe to a STOMP topic.

        Args:
            topic: The STOMP destination
                (e.g. ``"/topic/glider-connections-8"``).

        Returns:
            A :class:`StompSubscription` that yields parsed JSON
            messages.

        Raises:
            StompError: If not connected.
        """
        if not self._connected:
            raise StompError("Not connected — call connect() first")

        sub_id = f"sub-{self._next_sub_id}"
        self._next_sub_id += 1

        queue: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription(sub_id, topic, queue)
        self._subscriptions[sub_id] = sub
        self._sub_topics[sub_id] = topic

        subscribe_frame = _encode_frame(
            "SUBSCRIBE",
            {"id": sub_id, "destination": topic},
        )
        self._ws.send(json.dumps([subscribe_frame]))
        logger.debug("Subscribed %s to %s", sub_id, topic)

        return sub

    def _receive_loop(self) -> None:
        """Background thread: receive WebSocket messages and dispatch."""
        while not self._closing:
            try:
                raw = self._ws.recv(timeout=1)
            except TimeoutError:
                continue
            except ConnectionClosed:
                logger.debug("WebSocket connection closed")
                break
            except Exception as exc:
                if not self._closing:
                    logger.warning("WebSocket recv error: %s", exc)
                break

            for msg_str in _sockjs_decode(raw):
                frame = _parse_frame(msg_str)

                if frame.command == "MESSAGE":
                    sub_id = frame.headers.get("subscription", "")
                    sub = self._subscriptions.get(sub_id)
                    if sub is not None:
                        try:
                            payload: dict[str, Any] = json.loads(frame.body)
                        except json.JSONDecodeError:
                            payload = {"_raw": frame.body}
                        sub._queue.put(payload)
                elif frame.command == "ERROR":
                    logger.error("STOMP ERROR: %s", frame.body)
                elif frame.command == "HEARTBEAT":
                    pass
                else:
                    logger.debug("STOMP frame: %s", frame.command)

        # Signal all subscriptions that the connection is gone
        for sub in self._subscriptions.values():
            sub.close()
