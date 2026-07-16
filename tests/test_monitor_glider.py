"""Tests for the monitor_glider module."""

from __future__ import annotations

import logging
import logging.handlers
import signal
import threading
from queue import Queue
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sfmc_api.exceptions import APIError, SFMCError
from sfmc_api.monitor_glider import _log_with_time, build_parser, monitor_dialog, ordered_dialog
from sfmc_api.stomp import MAX_SEQUENCE, StompError, StompSubscription


def _make_sub(messages: list[dict[str, Any]]) -> StompSubscription:
    """Create a StompSubscription with pre-loaded messages."""
    q: Queue[dict[str, Any] | StompError | None] = Queue()
    for msg in messages:
        q.put(msg)
    q.put(None)  # sentinel
    return StompSubscription("sub-0", "/topic/test", q)


class TestOrderedDialogInOrder:
    def test_yields_in_order(self) -> None:
        sub = _make_sub(
            [
                {"sequenceNumber": 0, "data": "a"},
                {"sequenceNumber": 1, "data": "b"},
                {"sequenceNumber": 2, "data": "c"},
            ]
        )
        result = list(ordered_dialog(sub))
        assert result == ["a", "b", "c"]


class TestOrderedDialogOutOfOrder:
    def test_stomp_error_flushes_buffered_tail_before_raising(self) -> None:
        q: Queue[dict[str, Any] | StompError | None] = Queue()
        q.put({"sequenceNumber": 0, "data": "a"})
        q.put({"sequenceNumber": 2, "data": "c"})
        q.put(StompError("connection lost"))
        stream = iter(ordered_dialog(StompSubscription("sub", "/test", q)))

        assert next(stream) == "a"
        assert next(stream) == "c"
        with pytest.raises(StompError, match="connection lost"):
            next(stream)

    def test_eof_flushes_buffered_tail(self) -> None:
        """A gap that never fills must not swallow the buffered tail:
        it is flushed in stream order when the subscription ends."""
        sub = _make_sub(
            [
                {"sequenceNumber": 2, "data": "c"},
                {"sequenceNumber": 0, "data": "a"},
                {"sequenceNumber": 1, "data": "b"},
            ]
        )
        result = list(ordered_dialog(sub))
        # seq=2 sets next_expected=2, yields "c", advances to 3.
        # seq=0 and seq=1 are out of order (expected 3) and buffered.
        # Stream ends: the buffer flushes by modular distance from 3,
        # so 0 comes before 1.
        assert result == ["c", "a", "b"]

    def test_eof_flush_gap_never_filled(self) -> None:
        """Sequence 0, 2, EOF: 2 must still be delivered."""
        sub = _make_sub(
            [
                {"sequenceNumber": 0, "data": "a"},
                {"sequenceNumber": 2, "data": "c"},
            ]
        )
        result = list(ordered_dialog(sub))
        assert result == ["a", "c"]

    def test_in_order_drain_wraps_around_max_sequence(self) -> None:
        """The buffered drain follows the MAX_SEQUENCE -> 0 wraparound."""
        sub = _make_sub(
            [
                {"sequenceNumber": MAX_SEQUENCE - 1, "data": "w"},
                {"sequenceNumber": 0, "data": "y"},
                {"sequenceNumber": MAX_SEQUENCE, "data": "x"},
                {"sequenceNumber": 1, "data": "z"},
            ]
        )
        result = list(ordered_dialog(sub))
        assert result == ["w", "x", "y", "z"]

    def test_eof_flush_wraparound_gap(self) -> None:
        """Wraparound with the boundary message lost: expected
        MAX_SEQUENCE, buffered {0, 1} — flushed as 0 then 1."""
        sub = _make_sub(
            [
                {"sequenceNumber": MAX_SEQUENCE - 1, "data": "w"},
                {"sequenceNumber": 1, "data": "z"},
                {"sequenceNumber": 0, "data": "y"},
            ]
        )
        result = list(ordered_dialog(sub))
        assert result == ["w", "y", "z"]


class TestOrderedDialogReorder:
    def test_first_message_sets_baseline(self) -> None:
        """When first msg is seq=0, subsequent out-of-order msgs are buffered
        and yielded when the gap is filled."""
        sub = _make_sub(
            [
                {"sequenceNumber": 0, "data": "a"},
                {"sequenceNumber": 2, "data": "c"},
                {"sequenceNumber": 1, "data": "b"},
            ]
        )
        result = list(ordered_dialog(sub))
        # seq=0: yields "a", next_expected=1
        # seq=2: out of order (expected 1), buffered
        # seq=1: in order, yields "b", next_expected=2, drains 2 -> yields "c"
        assert result == ["a", "b", "c"]


class TestOrderedDialogNoSequence:
    def test_yields_immediately(self) -> None:
        sub = _make_sub(
            [
                {"data": "no-seq-1"},
                {"data": "no-seq-2"},
            ]
        )
        result = list(ordered_dialog(sub))
        assert result == ["no-seq-1", "no-seq-2"]


class TestOrderedDialogFlushOnGap:
    def test_flushes_buffered_messages_in_sorted_order(self) -> None:
        """When >100 messages are buffered, they are flushed (not dropped)."""
        # Start with seq=0 to set next_expected=1
        messages: list[dict[str, Any]] = [{"sequenceNumber": 0, "data": "start"}]
        # Then send seq=2..102 (101 messages), skipping seq=1
        # After adding the 101st out-of-order message, len(pending)=101 > 100
        for i in range(2, 103):
            messages.append({"sequenceNumber": i, "data": f"msg-{i}"})
        sub = _make_sub(messages)
        result = list(ordered_dialog(sub))
        # "start" is yielded first (seq=0, in order)
        # seq=2..102 are all buffered (expected=1)
        # When len(pending) hits 101 (> 100), flush in sorted order
        assert result[0] == "start"
        # The flushed messages should be in sorted seq order
        flushed = result[1:]
        expected_flushed = [f"msg-{i}" for i in range(2, 103)]
        assert flushed == expected_flushed


class TestLogWithTime:
    def test_sets_record_created(self) -> None:
        log = logging.getLogger("test._log_with_time")
        log.setLevel(logging.DEBUG)
        log.propagate = False
        # Remove any existing handlers from previous test runs
        log.handlers.clear()

        handler = logging.handlers.MemoryHandler(capacity=100)
        log.addHandler(handler)

        custom_time = 1700000000.123456
        _log_with_time(log, "hello", custom_time)

        handler.flush()
        assert len(handler.buffer) == 1
        assert handler.buffer[0].created == custom_time
        assert handler.buffer[0].getMessage() == "hello"

        log.removeHandler(handler)


class TestMonitorDialogReassembly:
    def test_reassembles_lines(self) -> None:
        q: Queue[dict[str, Any] | StompError | None] = Queue()
        sub = StompSubscription("sub-0", "/topic/test", q)

        # Simulate fragmented data: "hello world\r\n" split across chunks
        q.put({"sequenceNumber": 0, "data": "hello "})
        q.put({"sequenceNumber": 1, "data": "world\r\n"})
        q.put(None)

        log = logging.getLogger("test.monitor_dialog")
        log.setLevel(logging.DEBUG)
        log.propagate = False
        log.handlers.clear()

        handler = logging.handlers.MemoryHandler(capacity=100)
        log.addHandler(handler)

        stop = threading.Event()
        monitor_dialog(sub, log, stop)

        handler.flush()
        # Check that one complete line "hello world" was logged
        messages = [r.getMessage() for r in handler.buffer]
        assert "hello world" in messages

        log.removeHandler(handler)


class TestMainArgparse:
    def test_host_and_credentials_flags(self) -> None:
        """Verify --host and --credentials flags are accepted by the parser."""
        args = build_parser().parse_args(
            [
                "osusim",
                "--host",
                "sfmc.example.com",
                "--credentials",
                "/tmp/creds.json",
            ]
        )
        assert args.glider_name == "osusim"
        assert args.host == "sfmc.example.com"
        assert args.credentials == "/tmp/creds.json"
        assert args.logfile is None
        assert args.no_reconnect is False

    def test_defaults(self) -> None:
        """Without optional flags, defaults are None."""
        args = build_parser().parse_args(["myglider"])
        assert args.host is None
        assert args.credentials is None
        assert args.logfile is None
        assert args.no_reconnect is False

    def test_no_reconnect(self) -> None:
        args = build_parser().parse_args(["myglider", "--no-reconnect"])
        assert args.no_reconnect is True

    @patch("sfmc_api.monitor_glider.monitor_glider", side_effect=StompError("lost"))
    @patch("sfmc_api.monitor_glider.SFMCClient")
    @patch("sfmc_api.monitor_glider.setup_logging")
    def test_no_reconnect_stream_loss_exits_1_and_restores_signals(
        self,
        mock_setup: MagicMock,
        mock_client_class: MagicMock,
        mock_monitor: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        dialog_log = MagicMock(spec=logging.Logger)
        dialog_log.handlers = []
        mock_setup.return_value = (dialog_log, MagicMock(spec=logging.Logger))
        mock_client_class.return_value.__enter__.return_value = MagicMock()
        monkeypatch.setattr("sys.argv", ["sfmc-monitor-glider", "g1", "--no-reconnect"])
        previous_int = signal.getsignal(signal.SIGINT)
        previous_term = signal.getsignal(signal.SIGTERM)

        from sfmc_api.monitor_glider import main

        with pytest.raises(SystemExit) as exc_info:
            main()

        assert exc_info.value.code == 1
        assert mock_monitor.call_args.kwargs["reconnect"] is False
        assert isinstance(mock_monitor.call_args.kwargs["stop"], threading.Event)
        assert signal.getsignal(signal.SIGINT) == previous_int
        assert signal.getsignal(signal.SIGTERM) == previous_term


class TestOrderedDialogMalformedMessages:
    """Server-data variance must cost one skipped message, not the
    service (finding 3 of the robustness review)."""

    def test_non_dict_messages_skipped(self) -> None:
        q: Queue[Any] = Queue()
        q.put([40633])  # bare array, verifiably real on the zmodem topic
        q.put({"sequenceNumber": 0, "data": "a"})
        q.put("junk")
        q.put({"sequenceNumber": 1, "data": "b"})
        q.put(None)
        result = list(ordered_dialog(StompSubscription("sub", "/test", q)))
        assert result == ["a", "b"]

    def test_null_data_field_skipped(self) -> None:
        q: Queue[Any] = Queue()
        q.put({"sequenceNumber": 0, "data": None})
        q.put({"sequenceNumber": 1, "data": "b"})
        q.put(None)
        result = list(ordered_dialog(StompSubscription("sub", "/test", q)))
        assert result == ["b"]

    def test_non_int_sequence_yields_immediately(self) -> None:
        q: Queue[Any] = Queue()
        q.put({"sequenceNumber": "seven", "data": "x"})
        q.put(None)
        result = list(ordered_dialog(StompSubscription("sub", "/test", q)))
        assert result == ["x"]


class TestMonitorScriptsMalformedEvents:
    def test_non_dict_event_skipped(self) -> None:
        from sfmc_api.monitor_glider import monitor_scripts

        q: Queue[Any] = Queue()
        q.put([1, 2, 3])
        q.put(
            {
                "scriptName": "s.xml",
                "scriptType": "factory",
                "scriptState": "running",
                "paused": False,
            }
        )
        q.put(None)
        log = MagicMock()
        monitor_scripts(StompSubscription("sub", "/test", q), log, threading.Event())
        log.info.assert_called_once()


class TestLineBufferCap:
    """Finding 29: line-break-free binary chatter must not grow the
    reassembly buffer without bound on a weeks-long service."""

    def test_oversized_fragment_discarded(self, caplog: pytest.LogCaptureFixture) -> None:
        from sfmc_api.monitor_glider import monitor_dialog

        q: Queue[Any] = Queue()
        q.put({"sequenceNumber": 0, "data": "x" * 300_000})
        q.put(None)
        with caplog.at_level("WARNING", logger="sfmc_api.monitor_glider"):
            monitor_dialog(StompSubscription("sub", "/test", q), MagicMock(), threading.Event())
        assert any("buffer cap" in r.message for r in caplog.records)


class TestSequenceReset:
    """Finding 23: a server-side sequence reset must re-anchor after a
    short streak instead of stalling dialog in the reorder buffer."""

    def test_sustained_regression_reanchors(self) -> None:
        msgs: list[dict[str, Any]] = [
            {"sequenceNumber": 5_000_000, "data": "a"},
            {"sequenceNumber": 5_000_001, "data": "b"},
            # Server restart: counter resets to 0.
            {"sequenceNumber": 0, "data": "r0"},
            {"sequenceNumber": 1, "data": "r1"},
            {"sequenceNumber": 2, "data": "r2"},
            {"sequenceNumber": 3, "data": "r3"},
            {"sequenceNumber": 4, "data": "r4"},
        ]
        result = list(ordered_dialog(_make_sub(msgs)))
        # Nothing is lost, nothing stalls; after the streak the cursor
        # follows the new numbering so r3/r4 flow in order.
        assert result == ["a", "b", "r0", "r1", "r2", "r3", "r4"]

    def test_isolated_stale_redelivery_yields_immediately(self) -> None:
        msgs: list[dict[str, Any]] = [
            {"sequenceNumber": 10, "data": "a"},
            {"sequenceNumber": 5, "data": "stale"},  # behind: yielded, not parked
            {"sequenceNumber": 11, "data": "b"},
        ]
        result = list(ordered_dialog(_make_sub(msgs)))
        assert result == ["a", "stale", "b"]

    def test_forward_out_of_order_still_buffers(self) -> None:
        msgs: list[dict[str, Any]] = [
            {"sequenceNumber": 10, "data": "a"},
            {"sequenceNumber": 12, "data": "c"},  # ahead: buffered
            {"sequenceNumber": 11, "data": "b"},
        ]
        result = list(ordered_dialog(_make_sub(msgs)))
        assert result == ["a", "b", "c"]


class TestMonitorLogging:
    """Findings 15 and 26: the primary data record must survive
    logrotate and carry UTC timestamps."""

    def test_file_handler_is_watched(self, tmp_path: Any) -> None:
        from sfmc_api.monitor_glider import setup_logging

        dialog_log, _ = setup_logging("logtest", str(tmp_path / "d.log"))
        assert any(isinstance(h, logging.handlers.WatchedFileHandler) for h in dialog_log.handlers)
        for h in dialog_log.handlers:
            h.close()

    def test_timestamps_are_utc(self, tmp_path: Any) -> None:
        from sfmc_api.monitor_glider import setup_logging

        dialog_log, _ = setup_logging("utctest", str(tmp_path / "d.log"))
        fmt = dialog_log.handlers[0].formatter
        assert fmt is not None
        record = logging.LogRecord("n", logging.INFO, "p", 0, "m", (), None)
        record.created = 0.0  # epoch
        assert fmt.formatTime(record).startswith("1970-01-01T00:00:00")
        for h in dialog_log.handlers:
            h.close()


class TestStartupRetry:
    """Finding 14: a transient boot-time failure must be retried like
    any steady-state failure, not exit before the loop starts."""

    def test_initial_status_retried(self) -> None:
        from sfmc_api.monitor_glider import monitor_glider

        client = MagicMock()
        client.get_glider_details.side_effect = [
            APIError(0, "DNS not up yet"),
            {"data": {"state": "connected", "id": 8}},
        ]
        # Succeed startup, then make the stream session end fatally to
        # exit the test quickly via the no-reconnect path... instead,
        # use stop: set it during the first session wait.
        stop = threading.Event()

        def stop_soon(*args: Any, **kwargs: Any) -> MagicMock:
            stop.set()
            raise SFMCError("no stream in this test")

        client.open_stream.side_effect = stop_soon
        client.get_active_deployment_details.return_value = {"data": {}}

        monitor_glider(
            client,
            "g1",
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            stop=stop,
            reconnect_initial_delay=0.0,
            reconnect_max_delay=0.0,
            reconnect_jitter=0.0,
        )

        assert client.get_glider_details.call_count == 2

    def test_no_reconnect_startup_failure_still_raises(self) -> None:
        from sfmc_api.monitor_glider import monitor_glider

        client = MagicMock()
        client.get_glider_details.side_effect = SFMCError("down")

        with pytest.raises(SFMCError, match="down"):
            monitor_glider(
                client,
                "g1",
                MagicMock(spec=logging.Logger),
                MagicMock(spec=logging.Logger),
                MagicMock(spec=logging.Logger),
                reconnect=False,
            )


class TestStartupErrorClassification:
    """Permanent client errors must fail fast at startup; only
    transient failures retry (review follow-up on finding 14)."""

    def _monitor(self, client: MagicMock, **kwargs: Any) -> None:
        from sfmc_api.monitor_glider import monitor_glider

        monitor_glider(
            client,
            "g1",
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            reconnect_initial_delay=0.0,
            reconnect_max_delay=0.0,
            reconnect_jitter=0.0,
            **kwargs,
        )

    def test_404_fails_fast_even_with_reconnect(self) -> None:
        client = MagicMock()
        client.get_glider_details.side_effect = APIError(404, "no such glider")

        with pytest.raises(APIError):
            self._monitor(client)

        assert client.get_glider_details.call_count == 1

    def test_bad_credentials_fail_fast(self) -> None:
        from sfmc_api.exceptions import AuthenticationError

        client = MagicMock()
        client.get_glider_details.side_effect = AuthenticationError("bad secret")

        with pytest.raises(AuthenticationError):
            self._monitor(client)

    def test_server_error_is_retried(self) -> None:
        stop = threading.Event()
        client = MagicMock()
        client.get_glider_details.side_effect = [
            APIError(503, "maintenance"),
            {"data": {"state": "connected", "id": 8}},
        ]
        client.get_active_deployment_details.return_value = {"data": {}}

        def stop_soon(*args: Any, **kwargs: Any) -> MagicMock:
            stop.set()
            raise SFMCError("no stream in this test")

        client.open_stream.side_effect = stop_soon

        self._monitor(client, stop=stop)

        assert client.get_glider_details.call_count == 2


class TestIsTransientError:
    def test_classification(self) -> None:
        from sfmc_api.exceptions import (
            APIError,
            AuthenticationError,
            RateLimitError,
            SFMCError,
        )
        from sfmc_api.stream_reconnect import is_transient_error

        assert is_transient_error(APIError(0, "transport"))
        assert is_transient_error(APIError(500, "boom"))
        assert is_transient_error(APIError(503, "maintenance"))
        assert is_transient_error(RateLimitError(retry_after_seconds=1.0))
        assert not is_transient_error(APIError(401, "expired"))
        assert not is_transient_error(APIError(404, "no such glider"))
        assert not is_transient_error(AuthenticationError("bad secret"))
        assert not is_transient_error(SFMCError("unexpected shape"))
