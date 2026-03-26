"""Tests for STOMP connection, subscription, and lifecycle."""

from __future__ import annotations

import json
from queue import Empty
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

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
