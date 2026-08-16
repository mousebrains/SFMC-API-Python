"""Shared dialog-stream pipeline: sequence ordering and line reassembly.

A glider's dialog arrives on ``/topic/glider-link-output/{gliderId}`` as
a stream of small fragments that are *not* aligned to line boundaries
and may arrive out of order.  Turning that into usable text takes two
steps, both implemented here:

1. :func:`ordered_dialog` — reorder messages by ``sequenceNumber``.
2. :class:`LineAssembler` — reassemble fragments into complete lines.

Every consumer of glider dialog runs the same two steps:
:mod:`sfmc_api.monitor_glider` logs the lines, :mod:`sfmc_api.follow_glider`
feeds them to a :class:`~sfmc_api.dialog_parser.DialogParser`, and
:mod:`sfmc_api.commands` matches them against a submitted command.  They
share this module so there is one implementation to reason about.

Typical usage::

    from sfmc_api.dialog_stream import dialog_lines

    with client.open_stream() as stomp:
        sub = client.subscribe_glider_output("osu685", stomp)
        for line in dialog_lines(sub):
            print(line.text)

Consumers that need control over the unterminated tail (whether a
partial final line is kept or discarded at a stream boundary) drive
:class:`LineAssembler` directly instead.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Generator, Iterator
from dataclasses import dataclass

from .stomp import MAX_SEQUENCE, StompError, StompSubscription

__all__ = [
    "MAX_LINE_BUFFER_BYTES",
    "DialogLine",
    "LineAssembler",
    "dialog_lines",
    "ordered_dialog",
]

logger = logging.getLogger(__name__)

# ── Sequence-ordered dialog output ───────────────────────────────────

#: Maximum number of out-of-order messages we buffer before giving up
#: and yielding what we have.  100 covers typical Iridium reordering
#: while still bounding memory if a sequence number is permanently
#: lost.
_ORDER_BUFFER_MAX = 100

#: Consecutive behind-the-cursor sequence numbers before we conclude
#: the server restarted and reset its sequence counter (rather than a
#: stray stale re-delivery) and re-anchor to the new numbering.
_SEQ_RESET_STREAK = 3


def _flush_order(pending: dict[int, str], next_expected: int | None) -> list[int]:
    """Order buffered sequence numbers for a flush.

    Sorts by modular distance from *next_expected* so a buffer that
    straddles the ``MAX_SEQUENCE -> 0`` wraparound flushes in stream
    order (e.g. expected ``MAX_SEQUENCE``, buffered ``{MAX_SEQUENCE,
    0, 1}`` flushes in that order, not ``0, 1, MAX_SEQUENCE``).
    """
    if next_expected is None:
        return sorted(pending)
    span = MAX_SEQUENCE + 1
    return sorted(pending, key=lambda seq: (seq - next_expected) % span)


def ordered_dialog(
    sub: StompSubscription,
) -> Generator[str, None, None]:
    """Yield dialog data strings in sequence order.

    The SFMC server sends dialog output with ``sequenceNumber`` fields.
    Messages may arrive out of order.  This generator buffers
    out-of-order messages and yields them in correct sequence,
    matching the Node.js reference implementation's reordering logic.

    Recovery: if the out-of-order buffer grows past
    ``_ORDER_BUFFER_MAX``, we assume a sequence number is permanently
    lost (e.g. a dropped Iridium frame) and flush every buffered
    message in stream order, then resume from whatever arrives next.
    When the subscription ends, anything still buffered is flushed the
    same way.  Messages are never silently discarded — they may just
    be yielded out of natural order across a flush boundary.  A
    WARNING is logged when this happens so operators can see it.

    Yields:
        Each dialog data string, in sequence order.
    """
    next_expected: int | None = None
    pending: dict[int, str] = {}
    span = MAX_SEQUENCE + 1
    behind_streak = 0

    try:
        for msg in sub:
            # Server-data variance (a bare array, a null field) must
            # cost one skipped message, not the whole service: these
            # workers run under a supervisor that treats unexpected
            # exceptions as fatal code bugs.
            if not isinstance(msg, dict):
                logger.warning("ordered_dialog: skipping non-object message: %.200r", msg)
                continue
            seq = msg.get("sequenceNumber")
            data = msg.get("data", "")
            if not isinstance(data, str):
                logger.warning("ordered_dialog: skipping non-string data: %.200r", msg)
                continue
            if not isinstance(seq, int):
                seq = None

            if seq is None:
                # No sequence info — yield immediately
                yield data
                continue

            if next_expected is None or seq == next_expected:
                # In order (or first message) — yield and advance
                behind_streak = 0
                yield data
                if next_expected is None:
                    next_expected = seq
                next_expected = (next_expected + 1) if next_expected < MAX_SEQUENCE else 0

                # Drain any buffered messages that are now in order
                while next_expected in pending:
                    yield pending.pop(next_expected)
                    next_expected = (next_expected + 1) if next_expected < MAX_SEQUENCE else 0
            elif (seq - next_expected) % span > span // 2:
                # Behind the cursor: a stale re-delivery, or the server
                # restarted and reset its sequence counter.  Never park
                # these (they can never drain) — yield immediately, and
                # after a sustained streak re-anchor to the new
                # numbering instead of stalling every fresh message in
                # the out-of-order buffer until the overflow flush.
                behind_streak += 1
                if behind_streak >= _SEQ_RESET_STREAK:
                    logger.warning(
                        "ordered_dialog: %d consecutive sequence numbers behind "
                        "expected=%s (last=%d); assuming server sequence reset "
                        "and re-anchoring.",
                        behind_streak,
                        next_expected,
                        seq,
                    )
                    for seq_key in _flush_order(pending, next_expected):
                        yield pending[seq_key]
                    pending.clear()
                    next_expected = (seq + 1) if seq < MAX_SEQUENCE else 0
                    behind_streak = 0
                yield data
            else:
                # Out of order — buffer it
                behind_streak = 0
                pending[seq] = data

                # If the gap is too large, the buffer is stale — flush and reset.
                if len(pending) > _ORDER_BUFFER_MAX:
                    logger.warning(
                        "ordered_dialog: sequence gap exceeded buffer (%d msgs, "
                        "expected=%s, buffered range [%d, %d]). Flushing in "
                        "stream order and resuming.",
                        len(pending),
                        next_expected,
                        min(pending),
                        max(pending),
                    )
                    for seq_key in _flush_order(pending, next_expected):
                        yield pending[seq_key]
                    pending.clear()
                    next_expected = None
    except StompError:
        # A queued STOMP ERROR terminates iteration by raising rather than by
        # normal EOF. Preserve the same no-loss tail behavior before the
        # session supervisor replaces the connection.
        if pending:
            logger.warning(
                "ordered_dialog: STOMP error with %d message(s) buffered "
                "(expected=%s); flushing in stream order.",
                len(pending),
                next_expected,
            )
            for seq_key in _flush_order(pending, next_expected):
                yield pending[seq_key]
            pending.clear()
        raise

    # End of stream — a gap that never filled must not swallow the
    # buffered tail (often the last lines of a surfacing).
    if pending:
        logger.warning(
            "ordered_dialog: stream ended with %d message(s) buffered "
            "(expected=%s); flushing in stream order.",
            len(pending),
            next_expected,
        )
        for seq_key in _flush_order(pending, next_expected):
            yield pending[seq_key]


# ── Line reassembly ──────────────────────────────────────────────────

#: Dialog fragments are not aligned to line boundaries and mix line
#: endings (CRLF from the glider, bare CR/LF from some scripts).
_LINE_SEP = re.compile(r"\r\n|\r|\n")

#: Cap on the line-reassembly buffer.  Dialog lines are short; data
#: that accumulates this much without a line break is binary chatter,
#: and buffering it forever is unbounded memory growth on a service
#: that runs for weeks.
MAX_LINE_BUFFER_BYTES = 256 * 1024


@dataclass(frozen=True)
class DialogLine:
    """One reassembled line of glider dialog.

    Attributes:
        text: The line, without its terminator.  May be empty for a
            blank line.
        first_seen: ``time.time()`` when the first fragment of this
            line arrived.  Logged rather than the emit time so a line
            that took several Iridium frames to arrive is timestamped
            by when the glider started sending it.
        partial: ``True`` for an unterminated tail released by
            :meth:`LineAssembler.take_pending` — the glider may have
            been mid-line when the stream ended.
    """

    text: str
    first_seen: float
    partial: bool = False


class LineAssembler:
    """Reassemble dialog fragments into complete :class:`DialogLine` s.

    Fragments arrive without regard for line boundaries: one message
    may carry half a line, or three lines and a bit.  Feed each
    fragment to :meth:`feed` and it returns whatever complete lines
    that fragment finished.

    The unterminated remainder stays buffered.  At a stream boundary
    the caller decides what to do with it via :meth:`take_pending` —
    keeping it (a clean shutdown, where the text is real) or
    discarding it (a dropped session, where the next session will
    re-send).  This class does not make that choice.
    """

    def __init__(self, max_buffer_bytes: int = MAX_LINE_BUFFER_BYTES) -> None:
        self._buf = ""
        self._line_start: float = 0.0
        self._max_buffer_bytes = max_buffer_bytes

    def feed(self, data: str) -> list[DialogLine]:
        """Add a fragment and return the lines it completed."""
        if not self._buf:
            self._line_start = time.time()
        self._buf += data
        parts = _LINE_SEP.split(self._buf)
        # The last element is the unterminated fragment — keep buffering it.
        self._buf = parts[-1]

        lines: list[DialogLine] = []
        for text in parts[:-1]:
            lines.append(DialogLine(text=text, first_seen=self._line_start))
            # Each subsequent line in this fragment started now, not
            # when the first byte of the first line arrived.
            self._line_start = time.time()

        if len(self._buf) > self._max_buffer_bytes:
            logger.warning(
                "discarding %d bytes of line-break-free dialog data (buffer cap)",
                len(self._buf),
            )
            self._buf = ""
        return lines

    @property
    def pending(self) -> str:
        """The buffered, not-yet-terminated tail (``""`` if none)."""
        return self._buf

    def take_pending(self) -> DialogLine | None:
        """Release the unterminated tail as a partial line, clearing it.

        Returns ``None`` when the tail is empty or whitespace-only —
        matching the long-standing behaviour of only flushing a tail
        that carries actual text.
        """
        if not self._buf.strip():
            self._buf = ""
            return None
        line = DialogLine(text=self._buf, first_seen=self._line_start, partial=True)
        self._buf = ""
        return line

    def reset(self) -> None:
        """Discard any buffered tail."""
        self._buf = ""


def dialog_lines(
    sub: StompSubscription,
    *,
    flush_partial: bool = True,
) -> Iterator[DialogLine]:
    """Yield reassembled, sequence-ordered dialog lines from *sub*.

    Combines :func:`ordered_dialog` and :class:`LineAssembler` for
    consumers that do not need to distinguish a clean shutdown from a
    dropped session.

    Args:
        sub: A dialog-topic subscription (or a replay stand-in).
        flush_partial: Yield the unterminated tail as a partial
            :class:`DialogLine` when the stream ends normally.  Has no
            effect when the stream ends with a :class:`StompError` —
            the tail is dropped there, because the next session
            re-sends it.

    Yields:
        Each :class:`DialogLine`, in order.
    """
    assembler = LineAssembler()
    for data in ordered_dialog(sub):
        yield from assembler.feed(data)
    if flush_partial:
        tail = assembler.take_pending()
        if tail is not None:
            yield tail
