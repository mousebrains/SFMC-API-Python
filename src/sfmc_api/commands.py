"""Send commands to a glider and capture what the glider says back.

What SFMC actually guarantees
-----------------------------

``PUT /v1/submit-command/{glider}`` returning 200 means **SFMC accepted
the command**, not that the glider ran it.  A Slocum spends most of a
mission underwater; a command submitted while it is down sits queued
until the next surfacing, which may be hours away.
:meth:`~sfmc_api.client.SFMCClient.send_command` returns that
acceptance and nothing more.

Anything the glider says in response arrives separately, on the dialog
topic ``/topic/glider-link-output/{gliderId}`` — and that topic carries
**no correlation handle**.  There is no request id, no framing, and no
per-caller channel: it is one shared terminal that other pilots, the
SFMC script engine, and the glider's own unprompted chatter all write
to.  Matching a reply to a command is therefore a heuristic, and this
module keeps that fact in the types rather than hiding it:

* :attr:`CommandReply.correlated` says whether the reply was anchored
  to an echo of the command, or is merely everything that appeared on
  the shared terminal during the capture window.
* :attr:`CommandReply.complete` says whether capture reached a defined
  stopping point, and :attr:`CommandReply.reason` says which one.
* :attr:`CommandReply.dropped_lines` is non-zero if the capture lagged
  and lost lines, so a truncated reply is detectable instead of
  quietly wrong.

**A missing reply is not an error.**  Silence is the normal case for a
submerged glider, so :meth:`CommandChannel.send` returns a reply with
``complete=False`` rather than raising.  Exceptions are reserved for
failures to *submit* the command.

Usage
-----

::

    with client.command_channel("osu685") as chan:
        reply = chan.send("sensor m_battery")
        if reply.complete:
            print(reply.text)
        else:
            print(f"no reply: {reply.reason}")

Asynchronously — the same :class:`~concurrent.futures.Future` the rest
of the package uses::

        future = chan.send_async("sensor m_battery")
        future.add_done_callback(lambda f: print(f.result().text))
        reply = await asyncio.wrap_future(future)   # from asyncio

Ordering rules this module enforces
-----------------------------------

1. The dialog listener is attached **before** the command is submitted.
   Subscribing afterwards races the reply and loses fast ones.
2. One reply-capturing command runs at a time per glider.  Two
   overlapping captures would attribute each other's output.
3. A stream drop mid-capture ends the capture with
   ``reason="disconnected"``.  The command is **never resubmitted** —
   a silently repeated ``put`` on a live glider is a real hazard, and
   SFMC may well have delivered the first one.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from dataclasses import dataclass, replace
from queue import Empty
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal

from .dialog_stream import DialogLine
from .ops import KeyedLock
from .session import GliderSession, Listener

if TYPE_CHECKING:  # pragma: no cover - import cycle avoidance
    from .client import SFMCClient

__all__ = ["CommandChannel", "CommandReply", "ReplyPolicy", "StopReason"]

logger = logging.getLogger(__name__)

#: Why reply capture stopped.
#:
#: ``terminator``   a line matched :attr:`ReplyPolicy.until`
#: ``quiet``        output arrived, then stopped for
#:                  :attr:`ReplyPolicy.quiet` seconds
#: ``max_lines``    the line cap was reached; output may be truncated
#: ``silent``       the quiet window elapsed without a single line —
#:                  nothing was heard at all
#: ``timeout``      :attr:`ReplyPolicy.timeout` expired with the reply
#:                  still open — usual when the glider is submerged
#: ``disconnected`` the event stream dropped during capture
#: ``no_echo``      echo anchoring was on and no echo ever appeared
StopReason = Literal[
    "terminator", "quiet", "max_lines", "silent", "timeout", "disconnected", "no_echo"
]

#: Reasons that represent a capture which ran to a defined end.
#:
#: ``silent`` is deliberately absent.  A quiet window that elapsed
#: without a single line means nothing was heard — reporting that as a
#: completed reply is exactly the false reassurance this class exists
#: to avoid.  Observed live: commands submitted while a glider was busy
#: transmitting produced no output whatever, and an earlier version
#: reported them ``complete=True`` with an empty ``lines``.
_COMPLETE_REASONS: frozenset[str] = frozenset({"terminator", "quiet", "max_lines"})

#: Serializes reply-capturing commands per glider across every channel
#: in this process.
_COMMAND_LOCKS = KeyedLock()


@dataclass(frozen=True)
class ReplyPolicy:
    """When to stop collecting a command's reply.

    The defaults are sized for an Iridium link: round trips are
    seconds, and a chatty command (``sensors``) can take tens of
    seconds to finish printing.

    Attributes:
        timeout: Hard ceiling on the whole capture, in seconds.  Always
            enforced; nothing below can extend it.
        quiet: Stop once this many seconds pass with no new output.
            The usual end-of-reply signal, since Slocum dialog has no
            dependable prompt sentinel over a fragmented link.
        until: Stop at the first line matching this pattern.  Use it
            when you know the command's last line — far more precise
            than a quiet window.
        echo_anchor: Ignore output until a line echoing the submitted
            command appears, and capture from there.  The strongest
            correlation available *if* the dockserver echoes; when it
            does not, every capture ends as ``no_echo``.  Verify with
            ``sfmc-api probe-command`` before enabling.  Default
            ``False`` — the honest setting, which reports
            ``correlated=False``.
        echo_timeout: How long to wait for that echo before giving up.
        include_echo: Keep the echoed command as the first captured
            line instead of discarding it.
        max_lines: Cap on captured lines, so a glider stuck printing
            cannot grow the reply without bound.
    """

    timeout: float = 45.0
    quiet: float = 5.0
    until: re.Pattern[str] | None = None
    echo_anchor: bool = False
    echo_timeout: float = 15.0
    include_echo: bool = False
    max_lines: int = 2000

    def __post_init__(self) -> None:
        if self.timeout <= 0:
            raise ValueError("timeout must be > 0")
        if self.quiet <= 0:
            raise ValueError("quiet must be > 0")
        if self.echo_timeout <= 0:
            raise ValueError("echo_timeout must be > 0")
        if self.max_lines < 1:
            raise ValueError("max_lines must be >= 1")


@dataclass(frozen=True)
class CommandReply:
    """What came back after submitting one command.

    Attributes:
        command: The command as submitted.
        submitted_at: ``time.time()`` when SFMC accepted the command.
        lines: Captured dialog lines, in order.
        complete: ``True`` if capture reached a defined stopping point
            *and* actually captured something (terminator, quiet
            window, or line cap).  ``False`` means the reply may be
            partial or absent — check *reason*.  A capture that heard
            nothing at all is never ``complete``; it is ``silent``.
        reason: Why capture stopped; see :data:`StopReason`.
        correlated: ``True`` only when the capture was anchored to an
            echo of this command.  ``False`` means these lines are
            simply what appeared on a shared terminal during the
            window, and may include other sources' output.
        dropped_lines: Lines lost because the capture queue overflowed.
            Non-zero means *lines* has gaps.
        glider_connected: SFMC's view of the glider's link state,
            sampled only when capture ended without a reply.  ``None``
            when it was not checked.
        raw_response: The parsed body of the ``submit-command``
            response — SFMC's acceptance, not the glider's answer.
    """

    command: str
    submitted_at: float
    lines: tuple[str, ...]
    complete: bool
    reason: StopReason
    correlated: bool
    dropped_lines: int = 0
    glider_connected: bool | None = None
    raw_response: dict[str, Any] | None = None

    @property
    def text(self) -> str:
        """The captured lines joined with newlines."""
        return "\n".join(self.lines)

    def __bool__(self) -> bool:
        """True when capture completed with at least one line."""
        return self.complete and bool(self.lines)


def _echo_matches(line: str, command: str) -> bool:
    """True if *line* looks like the terminal echoing *command*.

    Deliberately loose: the dockserver may wrap the echo in a prompt
    or trailing whitespace.  Anchoring is still confirmed by the
    per-glider lock, which guarantees only one of our commands is
    outstanding at a time.
    """
    needle = " ".join(command.split())
    haystack = " ".join(line.split())
    return bool(needle) and needle in haystack


class CommandChannel:
    """Submit commands to one glider and capture the replies.

    Build one with :meth:`~sfmc_api.client.SFMCClient.command_channel`,
    which also opens the event session the channel needs.  To share an
    existing session — a monitor that is already streaming dialog, say
    — construct it directly with that :class:`~sfmc_api.session.GliderSession`.

    The channel is safe to use from several threads: reply-capturing
    sends are serialized per glider.

    Args:
        client: The API client used to submit commands.
        session: A started :class:`~sfmc_api.session.GliderSession`
            subscribed to the ``dialog`` topic.
        policy: Default stop conditions; override per call.
        lock_timeout: Seconds to wait for another send on the same
            glider to finish before giving up.
        owns_session: Close *session* when the channel closes.
    """

    def __init__(
        self,
        client: SFMCClient,
        session: GliderSession,
        *,
        policy: ReplyPolicy | None = None,
        lock_timeout: float = 300.0,
        owns_session: bool = False,
    ) -> None:
        self._client = client
        self._session = session
        self._policy = policy if policy is not None else ReplyPolicy()
        self._lock_timeout = lock_timeout
        self._owns_session = owns_session
        self._closed = False

    # ── Properties ───────────────────────────────────────────────────

    @property
    def glider_name(self) -> str:
        """The glider this channel talks to."""
        return self._session.glider_name

    @property
    def session(self) -> GliderSession:
        """The underlying event session."""
        return self._session

    # ── Sending ──────────────────────────────────────────────────────

    def send(self, command: str, **overrides: Any) -> CommandReply:
        """Submit *command* and capture the reply, blocking.

        Keyword overrides are applied to this channel's
        :class:`ReplyPolicy` for this call only — e.g.
        ``send("sensors", timeout=120, quiet=10)``.

        Returns:
            A :class:`CommandReply`.  Check
            :attr:`~CommandReply.complete` before trusting the text:
            a submerged glider legitimately answers nothing.

        Raises:
            APIError: If SFMC rejected the submission.
            RateLimitError: If SFMC rate-limited the submission.
            TimeoutError: If another send on this glider held the lock
                for longer than ``lock_timeout``.
        """
        return self._execute(command, self._policy_for(overrides))

    def send_async(self, command: str, **overrides: Any) -> Future[CommandReply]:
        """Submit *command* on a worker thread; return its future.

        The future's *exception* is a submission failure only.  A
        reply that never arrives resolves normally, as a
        :class:`CommandReply` with ``complete=False``.

        ``asyncio`` callers await it directly::

            reply = await asyncio.wrap_future(chan.send_async("sensor m_battery"))
        """
        policy = self._policy_for(overrides)
        future: Future[CommandReply] = Future()

        def run() -> None:
            if not future.set_running_or_notify_cancel():
                return
            try:
                future.set_result(self._execute(command, policy))
            except BaseException as exc:
                future.set_exception(exc)

        threading.Thread(
            target=run,
            daemon=True,
            name=f"sfmc-command-{self.glider_name}",
        ).start()
        return future

    def send_nowait(self, command: str) -> dict[str, Any]:
        """Submit *command* without capturing a reply.

        Still serialized against reply-capturing sends: firing a
        command into the middle of another command's capture window
        would pollute that capture.

        Returns:
            SFMC's acceptance response — again, not the glider's answer.
        """
        self._check_open()
        with _COMMAND_LOCKS.hold(self.glider_name, timeout=self._lock_timeout):
            return self._client.send_command(self.glider_name, command)

    # ── Observation ──────────────────────────────────────────────────

    def on_line(self, callback: Callable[[DialogLine], None]) -> None:
        """Call *callback* for every dialog line, command-related or not."""
        self._session.on_line(callback)

    # ── Lifecycle ────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the channel, and its session if the channel owns it."""
        if self._closed:
            return
        self._closed = True
        if self._owns_session:
            self._session.close()

    def __enter__(self) -> CommandChannel:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # ── Internals ────────────────────────────────────────────────────

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("CommandChannel is closed")

    def _policy_for(self, overrides: dict[str, Any]) -> ReplyPolicy:
        if not overrides:
            return self._policy
        unknown = set(overrides) - set(ReplyPolicy.__dataclass_fields__)
        if unknown:
            raise TypeError(f"Unknown reply policy option(s): {', '.join(sorted(unknown))}")
        return replace(self._policy, **overrides)

    def _execute(self, command: str, policy: ReplyPolicy) -> CommandReply:
        """Attach, submit, capture, detach — in that order."""
        self._check_open()
        with _COMMAND_LOCKS.hold(self.glider_name, timeout=self._lock_timeout):
            epoch = self._session.epoch
            # Attach BEFORE submitting: a reply that beats the
            # subscription would otherwise be lost.
            listener = self._session.dialog_listener()
            try:
                response = self._client.send_command(self.glider_name, command)
                submitted_at = time.time()
                return self._capture(
                    command=command,
                    policy=policy,
                    listener=listener,
                    epoch=epoch,
                    submitted_at=submitted_at,
                    response=response,
                )
            finally:
                listener.close()

    def _capture(
        self,
        *,
        command: str,
        policy: ReplyPolicy,
        listener: Listener[DialogLine],
        epoch: int,
        submitted_at: float,
        response: dict[str, Any],
    ) -> CommandReply:
        lines: list[str] = []
        deadline = time.monotonic() + policy.timeout
        echo_deadline = time.monotonic() + min(policy.echo_timeout, policy.timeout)
        anchored = not policy.echo_anchor
        last_event = time.monotonic()
        reason: StopReason = "timeout"

        while True:
            if self._session.epoch != epoch:
                # The stream dropped and came back; anything the glider
                # said in the gap is gone.  Do not resubmit.
                reason = "disconnected"
                break

            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                reason = "timeout"
                break
            if not anchored and now >= echo_deadline:
                reason = "no_echo"
                break

            try:
                line = listener.get(timeout=min(0.5, remaining))
            except Empty:
                if anchored and (time.monotonic() - last_event) >= policy.quiet:
                    # Silence after output is an ended reply; silence
                    # from the start means we never heard anything, and
                    # must not be dressed up as a completed one.
                    reason = "quiet" if lines else "silent"
                    break
                continue

            if line is None:
                reason = "disconnected"
                break

            last_event = time.monotonic()

            if not anchored:
                if _echo_matches(line.text, command):
                    anchored = True
                    if policy.include_echo:
                        lines.append(line.text)
                continue

            lines.append(line.text)
            if policy.until is not None and policy.until.search(line.text):
                reason = "terminator"
                break
            if len(lines) >= policy.max_lines:
                logger.warning(
                    "reply to %r hit the %d-line cap; output may be truncated",
                    command,
                    policy.max_lines,
                )
                reason = "max_lines"
                break

        complete = reason in _COMPLETE_REASONS
        glider_connected: bool | None = None
        if not complete:
            # Only in the failure path: one extra request buys the
            # operator the difference between "SFMC is broken" and
            # "the glider is simply underwater".
            glider_connected = self._session.glider_is_connected()
            logger.info(
                "no complete reply to %r (%s); glider link is %s",
                command,
                reason,
                "up" if glider_connected else "down",
            )

        return CommandReply(
            command=command,
            submitted_at=submitted_at,
            lines=tuple(lines),
            complete=complete,
            reason=reason,
            correlated=policy.echo_anchor and anchored,
            dropped_lines=listener.dropped,
            glider_connected=glider_connected,
            raw_response=response,
        )
