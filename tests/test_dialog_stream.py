"""Tests for the shared dialog pipeline (ordering + line reassembly).

Sequence-ordering behaviour is covered in ``test_monitor_glider.py``,
which exercises ``ordered_dialog`` through its long-standing public
import path.  These tests cover the line-reassembly half, which used
to be inlined in two different commands.
"""

from __future__ import annotations

import queue as queue_mod
from typing import Any

from sfmc_api.dialog_stream import (
    DialogLine,
    LineAssembler,
    dialog_lines,
    ordered_dialog,
)
from sfmc_api.stomp import StompSubscription


def _sub(messages: list[dict[str, Any]]) -> StompSubscription:
    q: queue_mod.Queue[Any] = queue_mod.Queue()
    for message in messages:
        q.put(message)
    q.put(None)
    return StompSubscription("sub", "/topic/test", q)


class TestLineAssembler:
    def test_splits_on_every_line_ending(self) -> None:
        assembler = LineAssembler()
        lines = assembler.feed("a\r\nb\nc\rd")
        assert [line.text for line in lines] == ["a", "b", "c"]
        assert assembler.pending == "d"

    def test_reassembles_across_fragments(self) -> None:
        assembler = LineAssembler()
        assert assembler.feed("Vehicle ") == []
        assert assembler.feed("Name: os") == []
        lines = assembler.feed("u685\r\n")
        assert [line.text for line in lines] == ["Vehicle Name: osu685"]

    def test_first_seen_is_when_the_line_started(self) -> None:
        assembler = LineAssembler()
        assembler.feed("half ")
        started = assembler._line_start
        lines = assembler.feed("a line\r\n")
        # Timestamped by when the glider began the line, not when the
        # final fragment closed it.
        assert lines[0].first_seen == started

    def test_blank_lines_are_preserved(self) -> None:
        assembler = LineAssembler()
        lines = assembler.feed("a\r\n\r\nb\r\n")
        assert [line.text for line in lines] == ["a", "", "b"]

    def test_take_pending_returns_partial_and_clears(self) -> None:
        assembler = LineAssembler()
        assembler.feed("unterminated tail")
        tail = assembler.take_pending()
        assert tail is not None
        assert tail.text == "unterminated tail"
        assert tail.partial is True
        assert assembler.pending == ""
        assert assembler.take_pending() is None

    def test_take_pending_ignores_whitespace_only_tail(self) -> None:
        assembler = LineAssembler()
        assembler.feed("done\r\n   ")
        assert assembler.take_pending() is None
        assert assembler.pending == ""

    def test_buffer_cap_discards_line_break_free_chatter(self) -> None:
        # A binary stream with no line breaks must not grow forever on
        # a service that runs for weeks.
        assembler = LineAssembler(max_buffer_bytes=64)
        assert assembler.feed("x" * 100) == []
        assert assembler.pending == ""

    def test_cap_does_not_discard_completed_lines(self) -> None:
        assembler = LineAssembler(max_buffer_bytes=8)
        lines = assembler.feed("keep me\r\n" + "y" * 40)
        assert [line.text for line in lines] == ["keep me"]
        assert assembler.pending == ""

    def test_reset_drops_the_tail(self) -> None:
        assembler = LineAssembler()
        assembler.feed("partial")
        assembler.reset()
        assert assembler.pending == ""


class TestDialogLines:
    def test_orders_then_reassembles(self) -> None:
        # The stream anchors on the first message it sees, so seq 0
        # arrives first; seq 2 then overtakes seq 1 and must be held
        # back until seq 1 lands.
        sub = _sub(
            [
                {"sequenceNumber": 0, "data": "hello "},
                {"sequenceNumber": 2, "data": "!\r\n"},
                {"sequenceNumber": 1, "data": "world"},
            ]
        )
        assert [line.text for line in dialog_lines(sub)] == ["hello world!"]

    def test_flushes_partial_tail_by_default(self) -> None:
        sub = _sub([{"sequenceNumber": 0, "data": "complete\r\npartial"}])
        lines = list(dialog_lines(sub))
        assert [line.text for line in lines] == ["complete", "partial"]
        assert lines[-1].partial is True

    def test_partial_tail_suppressed_when_asked(self) -> None:
        sub = _sub([{"sequenceNumber": 0, "data": "complete\r\npartial"}])
        lines = list(dialog_lines(sub, flush_partial=False))
        assert [line.text for line in lines] == ["complete"]

    def test_ordered_dialog_is_re_exported_by_monitor_glider(self) -> None:
        # Existing user code and docs import it from there.
        from sfmc_api.monitor_glider import ordered_dialog as via_monitor

        assert via_monitor is ordered_dialog

    def test_dialog_line_is_immutable(self) -> None:
        line = DialogLine(text="a", first_seen=1.0)
        try:
            line.text = "b"  # type: ignore[misc]
        except AttributeError:
            return
        raise AssertionError("DialogLine should be frozen")
