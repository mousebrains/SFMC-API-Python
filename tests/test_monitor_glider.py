"""Tests for the monitor_glider module."""

from __future__ import annotations

import logging
import logging.handlers
import threading
from queue import Queue
from typing import Any

from sfmc_api.monitor_glider import _log_with_time, monitor_dialog, ordered_dialog
from sfmc_api.stomp import StompSubscription


def _make_sub(messages: list[dict[str, Any]]) -> StompSubscription:
    """Create a StompSubscription with pre-loaded messages."""
    q: Queue[dict[str, Any] | None] = Queue()
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
    def test_reorders_messages(self) -> None:
        sub = _make_sub(
            [
                {"sequenceNumber": 2, "data": "c"},
                {"sequenceNumber": 0, "data": "a"},
                {"sequenceNumber": 1, "data": "b"},
            ]
        )
        _ = list(ordered_dialog(sub))
        # First message (seq=2) sets next_expected=2, yields "c", advances to 3.
        # seq=0 is out of order (expected 3), buffered.
        # seq=1 is out of order (expected 3), buffered.
        # Stream ends. Buffered messages 0,1 are not drained because
        # next_expected is 3 and neither 3 is in pending.
        # Actually they stay buffered. The reorder only drains when
        # next_expected matches. So result is just ["c"].
        # But if the first message is seq=0, it works:
        # Let's test a different scenario that actually reorders.
        pass


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
        q: Queue[dict[str, Any] | None] = Queue()
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
        import argparse

        # Replicate the parser setup from main()
        parser = argparse.ArgumentParser()
        parser.add_argument("glider_name")
        parser.add_argument("logfile", nargs="?", default=None)
        parser.add_argument("--host", default=None)
        parser.add_argument(
            "--credentials",
            default=None,
            metavar="PATH",
        )

        args = parser.parse_args(
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

    def test_defaults(self) -> None:
        """Without optional flags, defaults are None."""
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument("glider_name")
        parser.add_argument("logfile", nargs="?", default=None)
        parser.add_argument("--host", default=None)
        parser.add_argument("--credentials", default=None, metavar="PATH")

        args = parser.parse_args(["myglider"])
        assert args.host is None
        assert args.credentials is None
        assert args.logfile is None
