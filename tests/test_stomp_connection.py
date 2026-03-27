"""Tests for STOMP connection, subscription, and lifecycle."""

from __future__ import annotations

import json
import logging
import threading
import time
from queue import Empty
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from websockets.exceptions import ConnectionClosed

from sfmc_api.config import SFMCConfig
from sfmc_api.stomp import (
    StompConnection,
    StompError,
    StompSubscription,
    _sockjs_url,
)


class TestSockJSUrl:
    def test_format(self, config: SFMCConfig) -> None:
        url = _sockjs_url(config, "tok123")
        assert url.startswith("wss://sfmc.test/sfmc/api/sfmc-stomp/")
        assert "access_token=tok123" in url
        assert "/websocket?" in url

    def test_random_ids(self, config: SFMCConfig) -> None:
        """Each call should generate different server/session IDs."""
        urls = {_sockjs_url(config, "tok") for _ in range(10)}
        assert len(urls) > 1


class TestStompSubscription:
    def test_iterate_messages(self) -> None:
        from queue import Queue

        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/test", q)

        q.put({"event": "a"})
        q.put({"event": "b"})
        q.put(None)  # close signal

        msgs = list(sub)
        assert len(msgs) == 2
        assert msgs[0]["event"] == "a"
        assert msgs[1]["event"] == "b"

    def test_get_with_timeout(self) -> None:
        from queue import Queue

        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/test", q)

        with pytest.raises(Empty):
            sub.get(timeout=0.01)

    def test_get_returns_message(self) -> None:
        from queue import Queue

        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/test", q)
        q.put({"data": "hello"})

        msg = sub.get(timeout=1)
        assert msg is not None
        assert msg["data"] == "hello"

    def test_close_signals_none(self) -> None:
        from queue import Queue

        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/test", q)

        sub.close()
        msg = sub.get(timeout=1)
        assert msg is None

    def test_close_idempotent(self) -> None:
        from queue import Queue

        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/test", q)

        sub.close()
        sub.close()  # should not add second None
        assert q.qsize() == 1

    def test_topic_property(self) -> None:
        from queue import Queue

        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/glider-connections-8", q)
        assert sub.topic == "/topic/glider-connections-8"

    def test_close_sends_unsubscribe(self) -> None:
        """close() calls _unsubscribe on the connection."""
        from queue import Queue

        conn = MagicMock()
        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-7", "/topic/test", q, connection=conn)
        sub.close()
        conn._unsubscribe.assert_called_once_with("sub-7")

    def test_close_without_connection(self) -> None:
        """close() works when no connection is set (e.g. during disconnect)."""
        from queue import Queue

        q: Queue[dict[str, Any] | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/test", q, connection=None)
        sub.close()
        assert q.get_nowait() is None


class TestStompTlsConfig:
    @patch("sfmc_api.stomp.ws_connect")
    def test_tls_verify_false_passes_ssl_context(self, mock_ws_connect: MagicMock) -> None:
        """When tls_verify=False, ws_connect gets an ssl context with CERT_NONE."""
        config = SFMCConfig(host="test.com", client_id="c", secret="s", tls_verify=False)
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",
            'a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]',
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        call_kwargs = mock_ws_connect.call_args
        ssl_ctx = call_kwargs.kwargs.get("ssl") or call_kwargs[1].get("ssl")
        assert ssl_ctx is not None
        import ssl

        assert ssl_ctx.verify_mode == ssl.CERT_NONE

        conn.disconnect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_tls_verify_true_no_ssl_context(self, mock_ws_connect: MagicMock) -> None:
        """When tls_verify=True, ws_connect gets ssl=None (default verification)."""
        config = SFMCConfig(host="test.com", client_id="c", secret="s", tls_verify=True)
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",
            'a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]',
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        call_kwargs = mock_ws_connect.call_args
        ssl_ctx = call_kwargs.kwargs.get("ssl") or call_kwargs[1].get("ssl")
        assert ssl_ctx is None

        conn.disconnect()


class TestStompConnectionLifecycle:
    @patch("sfmc_api.stomp.ws_connect")
    def test_connect_success(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",  # SockJS open
            'a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]',  # STOMP CONNECTED
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        assert conn._connected
        assert conn._receiver_thread is not None

        conn.disconnect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_connect_no_open_frame(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = Exception("timeout")
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        with pytest.raises(StompError, match="handshake failed"):
            conn.connect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_connect_stomp_error(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",
            'a["ERROR\\nmessage:bad creds\\n\\nAccess denied\\u0000"]',
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        with pytest.raises(StompError, match="connection refused"):
            conn.connect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_subscribe_without_connect_raises(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        conn = StompConnection(config, "tok")
        with pytest.raises(StompError, match="Not connected"):
            conn.subscribe("/topic/test")

    @patch("sfmc_api.stomp.ws_connect")
    def test_subscribe_sends_frame(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",
            'a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]',
        ]

        # After connect, recv in _receive_loop should block
        def recv_loop_block(timeout: float = 0) -> str:
            import time

            time.sleep(0.1)
            raise TimeoutError

        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        # Reset recv to simulate blocking for the receiver thread
        mock_ws.recv.side_effect = recv_loop_block

        sub = conn.subscribe("/topic/test-topic")
        assert sub.topic == "/topic/test-topic"

        # Verify SUBSCRIBE frame was sent
        calls = mock_ws.send.call_args_list
        subscribe_call = calls[-1]
        sent = json.loads(subscribe_call[0][0])
        assert "SUBSCRIBE" in sent[0]
        assert "destination:/topic/test-topic" in sent[0]

        conn.disconnect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_context_manager(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",
            'a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]',
            TimeoutError,
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        with conn:
            assert conn._connected

        # After exit, should be disconnected
        assert not conn._connected

    @patch("sfmc_api.stomp.ws_connect")
    def test_context_manager_auto_connects(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",
            'a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]',
            TimeoutError,
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        with conn:
            assert conn._connected


# ── Helpers for SockJS-encoded STOMP frames ──────────────────────────


def _sockjs_message_frame(
    command: str, headers: dict[str, str] | None = None, body: str = ""
) -> str:
    """Build a SockJS 'a[...]' frame wrapping a single STOMP frame."""
    hdr = headers or {}
    lines = [command]
    for k, v in hdr.items():
        lines.append(f"{k}:{v}")
    lines.append("")
    lines.append(body)
    stomp_str = "\\n".join(lines) + "\\u0000"
    return f'a["{stomp_str}"]'


_OPEN = "o"
_CONNECTED = 'a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]'


def _make_connected_ws(mock_ws_connect: MagicMock) -> MagicMock:
    """Create a mock ws that completes the STOMP handshake, then blocks."""
    mock_ws = MagicMock()
    mock_ws.recv.side_effect = [
        _OPEN,
        _CONNECTED,
    ]
    mock_ws_connect.return_value = mock_ws
    return mock_ws


# ── TestStompDebugConfig ─────────────────────────────────────────────


class TestStompDebugConfig:
    @patch("sfmc_api.stomp.ws_connect")
    def test_stomp_debug_enables_logging(self, mock_ws_connect: MagicMock) -> None:
        """When stomp_debug=True, the module logger level is set to DEBUG."""
        config = SFMCConfig(
            host="sfmc.test",
            client_id="cid",
            secret="sec",
            tls_verify=False,
            stomp_debug=True,
        )
        _make_connected_ws(mock_ws_connect)

        conn = StompConnection(config, "tok")
        conn.connect()

        stomp_logger = logging.getLogger("sfmc_api.stomp")
        assert stomp_logger.level == logging.DEBUG

        conn.disconnect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_stomp_debug_false_does_not_change_level(self, mock_ws_connect: MagicMock) -> None:
        """When stomp_debug=False (default), logger level is not forced to DEBUG."""
        config = SFMCConfig(
            host="sfmc.test",
            client_id="cid",
            secret="sec",
            tls_verify=False,
            stomp_debug=False,
        )
        stomp_logger = logging.getLogger("sfmc_api.stomp")
        original_level = stomp_logger.level
        _make_connected_ws(mock_ws_connect)

        conn = StompConnection(config, "tok")
        conn.connect()

        assert stomp_logger.level == original_level

        conn.disconnect()


# ── TestConnectFailures ──────────────────────────────────────────────


class TestConnectFailures:
    @patch("sfmc_api.stomp.ws_connect")
    def test_ws_connect_raises_stomp_error(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        """When ws_connect raises, it is wrapped in StompError."""
        mock_ws_connect.side_effect = OSError("connection refused")

        conn = StompConnection(config, "tok")
        with pytest.raises(StompError, match="WebSocket connection failed"):
            conn.connect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_no_connected_frame(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        """If the server sends a non-CONNECTED, non-ERROR frame, raise StompError."""
        mock_ws = MagicMock()
        # Return SockJS open, then a HEARTBEAT-only frame (no CONNECTED)
        mock_ws.recv.side_effect = [
            "o",
            'a["RECEIPT\\nreceipt-id:0\\n\\n\\u0000"]',
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        with pytest.raises(StompError, match="Did not receive STOMP CONNECTED frame"):
            conn.connect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_generic_exception_wraps_stomp_error(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        """A generic exception during handshake is wrapped in StompError."""
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            "o",
            RuntimeError("unexpected parse failure"),
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        with pytest.raises(StompError, match="handshake failed"):
            conn.connect()


# ── TestDisconnectEdgeCases ──────────────────────────────────────────


class TestDisconnectEdgeCases:
    @patch("sfmc_api.stomp.ws_connect")
    def test_disconnect_send_failure(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        """If sending the DISCONNECT frame fails, disconnect still completes."""
        mock_ws = _make_connected_ws(mock_ws_connect)

        conn = StompConnection(config, "tok")
        conn.connect()

        # Make the next send (DISCONNECT) raise
        mock_ws.send.side_effect = OSError("broken pipe")

        # Should NOT raise
        conn.disconnect()
        assert not conn._connected


# ── TestUnsubscribeEdgeCases ─────────────────────────────────────────


class TestUnsubscribeEdgeCases:
    @patch("sfmc_api.stomp.ws_connect")
    def test_unsubscribe_send_failure(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        """If sending UNSUBSCRIBE frame fails, _unsubscribe still completes."""
        mock_ws = _make_connected_ws(mock_ws_connect)

        conn = StompConnection(config, "tok")
        conn.connect()

        # Subscribe first (this send works fine since side_effect not set yet)
        mock_ws.recv.side_effect = [TimeoutError] * 100  # keep receiver loop alive
        conn.subscribe("/topic/test")

        # Now make send raise for the UNSUBSCRIBE
        mock_ws.send.side_effect = OSError("broken pipe")

        # Should NOT raise
        conn._unsubscribe("sub-0")

        # Verify the subscription was removed from the registry
        assert "sub-0" not in conn._subscriptions

        conn._closing = True  # stop receiver thread
        conn._connected = False


# ── TestReceiveLoop ──────────────────────────────────────────────────


class TestReceiveLoop:
    """Tests for _receive_loop MESSAGE/ERROR/HEARTBEAT/unknown dispatch."""

    @patch("sfmc_api.stomp.ws_connect")
    def test_message_dispatched_to_subscription(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        """A MESSAGE frame with valid JSON is dispatched to the subscription queue."""
        mock_ws = MagicMock()
        message_frame = 'a["MESSAGE\\nsubscription:sub-0\\n\\n{\\"key\\":\\"val\\"}\\u0000"]'

        gate = threading.Event()
        call_count = 0

        def gated_recv(timeout: float = 0) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _OPEN
            if call_count == 2:
                return _CONNECTED
            # Block until subscription is registered
            if call_count == 3:
                gate.wait(timeout=5)
                return message_frame
            raise TimeoutError

        mock_ws.recv.side_effect = gated_recv
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        sub = conn.subscribe("/topic/test")
        gate.set()  # release the message

        msg = sub.get(timeout=2)
        assert msg == {"key": "val"}

        conn.disconnect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_message_invalid_json_raw_fallback(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        """A MESSAGE with non-JSON body produces {"_raw": body}."""
        mock_ws = MagicMock()
        message_frame = 'a["MESSAGE\\nsubscription:sub-0\\n\\nnot-json-data\\u0000"]'

        gate = threading.Event()
        call_count = 0

        def gated_recv(timeout: float = 0) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _OPEN
            if call_count == 2:
                return _CONNECTED
            if call_count == 3:
                gate.wait(timeout=5)
                return message_frame
            raise TimeoutError

        mock_ws.recv.side_effect = gated_recv
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        sub = conn.subscribe("/topic/test")
        gate.set()

        msg = sub.get(timeout=2)
        assert msg == {"_raw": "not-json-data"}

        conn.disconnect()

    @patch("sfmc_api.stomp.logger")
    @patch("sfmc_api.stomp.ws_connect")
    def test_error_frame_logged(
        self, mock_ws_connect: MagicMock, mock_logger: MagicMock, config: SFMCConfig
    ) -> None:
        """An ERROR frame in the receive loop triggers logger.error."""
        mock_ws = MagicMock()
        error_frame = 'a["ERROR\\nmessage:server error\\n\\nSome error body\\u0000"]'
        mock_ws.recv.side_effect = [
            _OPEN,
            _CONNECTED,
            error_frame,
            TimeoutError,
            TimeoutError,
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()
        time.sleep(0.3)

        mock_logger.error.assert_any_call("STOMP ERROR: %s", "Some error body")

        conn.disconnect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_heartbeat_ignored(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        """A heartbeat SockJS frame is silently ignored."""
        mock_ws = MagicMock()
        mock_ws.recv.side_effect = [
            _OPEN,
            _CONNECTED,
            "h",  # SockJS heartbeat
            TimeoutError,
            TimeoutError,
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()
        time.sleep(0.2)

        # No crash, connection still valid
        assert conn._connected

        conn.disconnect()

    @patch("sfmc_api.stomp.logger")
    @patch("sfmc_api.stomp.ws_connect")
    def test_unknown_frame_logged(
        self, mock_ws_connect: MagicMock, mock_logger: MagicMock, config: SFMCConfig
    ) -> None:
        """An unknown STOMP command triggers a debug log."""
        mock_ws = MagicMock()
        unknown_frame = 'a["WEIRD_COMMAND\\n\\nsome body\\u0000"]'
        mock_ws.recv.side_effect = [
            _OPEN,
            _CONNECTED,
            unknown_frame,
            TimeoutError,
            TimeoutError,
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()
        time.sleep(0.3)

        mock_logger.debug.assert_any_call("STOMP frame: %s", "WEIRD_COMMAND")

        conn.disconnect()

    @patch("sfmc_api.stomp.ws_connect")
    def test_connection_closed_closes_subscriptions(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        """When recv raises ConnectionClosed, the loop exits and remaining subs are closed."""
        mock_ws = MagicMock()

        gate = threading.Event()
        call_count = 0

        def gated_recv(timeout: float = 0) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _OPEN
            if call_count == 2:
                return _CONNECTED
            if call_count == 3:
                gate.wait(timeout=5)
                raise ConnectionClosed(None, None)
            raise TimeoutError

        mock_ws.recv.side_effect = gated_recv
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        sub = conn.subscribe("/topic/test")
        gate.set()  # let the ConnectionClosed fire

        # The subscription should have been closed (None sentinel in queue)
        msg = sub.get(timeout=2)
        assert msg is None

        conn._closing = True
        conn._connected = False

    @patch("sfmc_api.stomp.ws_connect")
    def test_recv_error_breaks_loop(self, mock_ws_connect: MagicMock, config: SFMCConfig) -> None:
        """A generic recv exception breaks the receive loop and closes subs."""
        mock_ws = MagicMock()

        gate = threading.Event()
        call_count = 0

        def gated_recv(timeout: float = 0) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _OPEN
            if call_count == 2:
                return _CONNECTED
            if call_count == 3:
                gate.wait(timeout=5)
                raise RuntimeError("unexpected recv failure")
            raise TimeoutError

        mock_ws.recv.side_effect = gated_recv
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()

        sub = conn.subscribe("/topic/test")
        gate.set()  # let the error fire

        # The subscription should have been closed
        msg = sub.get(timeout=2)
        assert msg is None

        conn._closing = True
        conn._connected = False

    @patch("sfmc_api.stomp.ws_connect")
    def test_message_to_unknown_subscription_ignored(
        self, mock_ws_connect: MagicMock, config: SFMCConfig
    ) -> None:
        """A MESSAGE for a subscription ID not in the registry is silently ignored."""
        mock_ws = MagicMock()
        # Message for sub-99 which doesn't exist
        message_frame = 'a["MESSAGE\\nsubscription:sub-99\\n\\n{\\"x\\":1}\\u0000"]'
        mock_ws.recv.side_effect = [
            _OPEN,
            _CONNECTED,
            message_frame,
            TimeoutError,
            TimeoutError,
        ]
        mock_ws_connect.return_value = mock_ws

        conn = StompConnection(config, "tok")
        conn.connect()
        time.sleep(0.3)

        # No crash, connection is still up
        assert conn._connected

        conn.disconnect()
