"""Tests for STOMP frame encoding/decoding and SockJS helpers."""

from __future__ import annotations

from sfmc_api.stomp import _encode_frame, _parse_frame, _sockjs_decode


class TestEncodeFrame:
    def test_minimal(self) -> None:
        frame = _encode_frame("CONNECT")
        assert frame.startswith("CONNECT\n")
        assert frame.endswith("\0")

    def test_with_headers(self) -> None:
        frame = _encode_frame("SUBSCRIBE", {"id": "sub-0", "destination": "/topic/test"})
        assert "id:sub-0" in frame
        assert "destination:/topic/test" in frame

    def test_with_body(self) -> None:
        frame = _encode_frame("SEND", {"destination": "/topic/x"}, body="hello")
        assert frame.endswith("hello\0")


class TestParseFrame:
    def test_connected(self) -> None:
        raw = "CONNECTED\nversion:1.2\nheart-beat:0,0\n\n\0"
        frame = _parse_frame(raw)
        assert frame.command == "CONNECTED"
        assert frame.headers["version"] == "1.2"

    def test_message_with_body(self) -> None:
        raw = 'MESSAGE\nsubscription:sub-0\n\n{"key":"value"}\0'
        frame = _parse_frame(raw)
        assert frame.command == "MESSAGE"
        assert frame.headers["subscription"] == "sub-0"
        assert '"key"' in frame.body

    def test_empty(self) -> None:
        frame = _parse_frame("")
        assert frame.command == "HEARTBEAT"

    def test_whitespace(self) -> None:
        frame = _parse_frame("  \n  ")
        assert frame.command == "HEARTBEAT"


class TestSockJSDecode:
    def test_open_frame(self) -> None:
        assert _sockjs_decode("o") == []

    def test_heartbeat(self) -> None:
        assert _sockjs_decode("h") == []

    def test_close_frame(self) -> None:
        assert _sockjs_decode('c[1000,"normal"]') is None

    def test_array_frame(self) -> None:
        msgs = _sockjs_decode('a["CONNECTED\\nversion:1.2\\n\\n\\u0000"]')
        assert len(msgs) == 1
        assert "CONNECTED" in msgs[0]

    def test_empty(self) -> None:
        assert _sockjs_decode("") == []

    def test_unknown_type(self) -> None:
        assert _sockjs_decode("x_unknown") == []
