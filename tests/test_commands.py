"""Tests for CommandChannel: ordering, correlation, and honest failure.

The behaviours these lock down are the ones that make the difference
between a reply you can trust and one you cannot:

* the dialog listener is attached before the command is submitted;
* two senders on one glider never interleave;
* every way capture can stop is reported, and a missing reply is not
  an exception;
* a stream drop mid-capture never resubmits the command.
"""

from __future__ import annotations

import re
import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from sfmc_api.commands import CommandChannel, CommandReply, ReplyPolicy, _echo_matches
from sfmc_api.dialog_stream import DialogLine
from sfmc_api.exceptions import APIError
from sfmc_api.session import GliderSession, Listener, _Broadcaster


class FakeSession:
    """A GliderSession stand-in driven directly by the test.

    Real reconnect machinery is covered in ``test_session.py``; here we
    need precise control over what the dialog stream emits and when.
    """

    def __init__(self, glider_name: str = "osu-test", connected: bool = True) -> None:
        self.glider_name = glider_name
        self._dialog: _Broadcaster[DialogLine] = _Broadcaster()
        self.epoch = 1
        self.connected = connected
        self.listeners_at_send: list[int] = []
        self._attached = 0

    def dialog_listener(self, maxsize: int = 2048) -> Listener[DialogLine]:
        self._attached += 1
        return self._dialog.attach(maxsize=maxsize)

    def on_line(self, callback: Any) -> None:
        self._dialog.subscribe(callback)

    def glider_is_connected(self) -> bool:
        return self.connected

    def close(self) -> None:
        self._dialog.close()

    # ── Test controls ────────────────────────────────────────────────

    @property
    def attached(self) -> int:
        return self._attached

    def emit(self, *lines: str) -> None:
        for text in lines:
            self._dialog.publish(DialogLine(text=text, first_seen=time.time()))

    def drop(self) -> None:
        """Simulate a stream drop and reconnect."""
        self.epoch += 1


def _channel(
    session: FakeSession,
    client: MagicMock | None = None,
    **policy_kwargs: Any,
) -> tuple[CommandChannel, MagicMock]:
    if client is None:
        client = MagicMock()
        client.send_command.return_value = {"status": "accepted"}
    policy = ReplyPolicy(**policy_kwargs) if policy_kwargs else None
    return CommandChannel(client, session, policy=policy), client  # type: ignore[arg-type]


def _emit_after(session: FakeSession, delay: float, *lines: str) -> threading.Thread:
    def run() -> None:
        time.sleep(delay)
        session.emit(*lines)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


class TestOrdering:
    def test_listener_is_attached_before_the_command_is_submitted(self) -> None:
        """A reply that beats the subscription would otherwise be lost."""
        session = FakeSession()
        client = MagicMock()
        attached_at_send: list[int] = []

        def send_command(_name: str, _command: str) -> dict[str, Any]:
            attached_at_send.append(session.attached)
            return {}

        client.send_command.side_effect = send_command
        channel, _ = _channel(session, client, timeout=1.0, quiet=0.05)
        channel.send("sensor m_battery")

        assert attached_at_send == [1], "listener must exist before the PUT"

    def test_two_senders_do_not_interleave(self) -> None:
        """Overlapping captures would attribute each other's output."""
        session = FakeSession()
        client = MagicMock()
        overlapping = []
        in_flight = threading.Lock()

        def send_command(_name: str, command: str) -> dict[str, Any]:
            acquired = in_flight.acquire(blocking=False)
            overlapping.append(acquired)
            time.sleep(0.05)
            if acquired:
                in_flight.release()
            return {}

        client.send_command.side_effect = send_command
        channel, _ = _channel(session, client, timeout=1.0, quiet=0.05)

        def send(index: int) -> None:
            channel.send(f"cmd {index}")

        threads = [threading.Thread(target=send, args=(i,), daemon=True) for i in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert all(overlapping), "sends on one glider must be serialized"

    def test_send_nowait_is_serialized_too(self) -> None:
        """Firing into another command's window would pollute its capture."""
        session = FakeSession()
        channel, client = _channel(session, timeout=1.0, quiet=0.05)

        started = threading.Event()
        result: list[CommandReply] = []

        def capture() -> None:
            started.set()
            result.append(channel.send("slow", timeout=0.5, quiet=0.4))

        thread = threading.Thread(target=capture, daemon=True)
        thread.start()
        started.wait(timeout=2)
        channel.send_nowait("quick")
        thread.join(timeout=5)

        # The capture finished before send_nowait's PUT went out.
        assert [call.args[1] for call in client.send_command.call_args_list] == [
            "slow",
            "quick",
        ]


class TestStopReasons:
    def test_quiet_window_ends_a_reply(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=0.2)
        _emit_after(session, 0.05, "m_battery(volts)=15.1")
        reply = channel.send("sensor m_battery")

        assert reply.complete is True
        assert reply.reason == "quiet"
        assert reply.lines == ("m_battery(volts)=15.1",)
        assert reply.text == "m_battery(volts)=15.1"

    def test_terminator_ends_a_reply_immediately(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=10.0)
        _emit_after(session, 0.05, "line one", "DONE", "line three")
        reply = channel.send("run", until=re.compile(r"^DONE$"))

        assert reply.reason == "terminator"
        assert reply.lines == ("line one", "DONE")

    def test_max_lines_caps_a_runaway_reply(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=10.0)
        _emit_after(session, 0.05, *[f"line {i}" for i in range(20)])
        reply = channel.send("sensors", max_lines=5)

        assert reply.reason == "max_lines"
        assert len(reply.lines) == 5
        assert reply.complete is True

    def test_timeout_is_not_an_error(self) -> None:
        """A submerged glider answering nothing is normal, not a failure."""
        session = FakeSession(connected=False)
        channel, _ = _channel(session, timeout=0.2, quiet=10.0)
        reply = channel.send("sensor m_battery")

        assert reply.complete is False
        assert reply.reason == "timeout"
        assert reply.lines == ()
        # The failure path checks the link so the operator can tell
        # "SFMC is broken" from "the glider is underwater".
        assert reply.glider_connected is False
        assert bool(reply) is False

    def test_disconnect_mid_capture_never_resubmits(self) -> None:
        session = FakeSession()
        channel, client = _channel(session, timeout=5.0, quiet=10.0)

        def drop() -> None:
            time.sleep(0.05)
            session.drop()

        threading.Thread(target=drop, daemon=True).start()
        reply = channel.send("put c_science_on 0")

        assert reply.reason == "disconnected"
        assert reply.complete is False
        # The command may already have reached the glider; sending it
        # twice is the hazard this guards against.
        assert client.send_command.call_count == 1

    def test_listener_close_ends_capture_as_disconnected(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=10.0)

        def close() -> None:
            time.sleep(0.05)
            session.close()

        threading.Thread(target=close, daemon=True).start()
        reply = channel.send("sensor m_battery")
        assert reply.reason == "disconnected"


class TestCorrelation:
    def test_uncorrelated_by_default(self) -> None:
        """Without echo anchoring the lines are merely 'what appeared'."""
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=0.2)
        _emit_after(session, 0.05, "someone else's output")
        reply = channel.send("sensor m_battery")

        assert reply.correlated is False
        assert reply.lines == ("someone else's output",)

    def test_echo_anchor_discards_output_before_the_echo(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=0.2, echo_anchor=True)
        _emit_after(
            session,
            0.05,
            "unrelated chatter",
            "sensor m_battery",
            "m_battery(volts)=15.1",
        )
        reply = channel.send("sensor m_battery")

        assert reply.correlated is True
        assert reply.lines == ("m_battery(volts)=15.1",)

    def test_echo_anchor_can_keep_the_echo(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=0.2, echo_anchor=True, include_echo=True)
        _emit_after(session, 0.05, "sensor m_battery", "m_battery(volts)=15.1")
        reply = channel.send("sensor m_battery")

        assert reply.lines == ("sensor m_battery", "m_battery(volts)=15.1")

    def test_missing_echo_is_reported_not_guessed(self) -> None:
        """A server that does not echo must not yield a false reply."""
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=10.0, echo_anchor=True, echo_timeout=0.2)
        _emit_after(session, 0.05, "output with no echo")
        reply = channel.send("sensor m_battery")

        assert reply.reason == "no_echo"
        assert reply.complete is False
        assert reply.correlated is False
        assert reply.lines == ()

    def test_echo_matching_tolerates_whitespace_and_prompts(self) -> None:
        assert _echo_matches("sensor m_battery", "sensor m_battery")
        assert _echo_matches("  sensor   m_battery  ", "sensor m_battery")
        assert _echo_matches("GliderDos> sensor m_battery", "sensor m_battery")
        assert not _echo_matches("sensor m_depth", "sensor m_battery")
        assert not _echo_matches("anything", "")


class TestFailureSurfacing:
    def test_submission_failure_raises(self) -> None:
        """Failing to submit is a real error, unlike a missing reply."""
        session = FakeSession()
        client = MagicMock()
        client.send_command.side_effect = APIError(500, "server exploded")
        channel, _ = _channel(session, client)

        with pytest.raises(APIError):
            channel.send("sensor m_battery")

    def test_async_submission_failure_surfaces_via_the_future(self) -> None:
        session = FakeSession()
        client = MagicMock()
        client.send_command.side_effect = APIError(500, "server exploded")
        channel, _ = _channel(session, client)

        future = channel.send_async("sensor m_battery")
        with pytest.raises(APIError):
            future.result(timeout=5)

    def test_dropped_lines_are_reported(self) -> None:
        """A truncated capture must be detectable, not silently wrong."""
        session = FakeSession()
        client = MagicMock()
        client.send_command.return_value = {}
        channel = CommandChannel(
            client,  # type: ignore[arg-type]
            session,  # type: ignore[arg-type]
            policy=ReplyPolicy(timeout=5.0, quiet=0.3),
        )
        # Overflow a deliberately tiny listener queue.
        original = session.dialog_listener

        def small_listener(maxsize: int = 2048) -> Listener[DialogLine]:
            del maxsize
            return original(maxsize=2)

        session.dialog_listener = small_listener  # type: ignore[assignment]
        _emit_after(session, 0.05, *[f"line {i}" for i in range(10)])
        reply = channel.send("sensors")

        assert reply.dropped_lines > 0
        assert len(reply.lines) < 10

    def test_unknown_policy_option_is_rejected(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session)
        with pytest.raises(TypeError, match="Unknown reply policy option"):
            channel.send("cmd", nonsense=1)

    def test_closed_channel_refuses_sends(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session)
        channel.close()
        with pytest.raises(RuntimeError, match="closed"):
            channel.send("cmd")


class TestAsyncSurface:
    def test_send_async_returns_a_future(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=0.2)
        _emit_after(session, 0.05, "m_battery(volts)=15.1")

        future = channel.send_async("sensor m_battery")
        reply = future.result(timeout=10)
        assert reply.lines == ("m_battery(volts)=15.1",)

    def test_done_callback_fires(self) -> None:
        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=0.2)
        _emit_after(session, 0.05, "ok")

        seen: list[CommandReply] = []
        done = threading.Event()
        future = channel.send_async("cmd")
        future.add_done_callback(lambda f: (seen.append(f.result()), done.set()))
        assert done.wait(timeout=10)
        assert seen[0].lines == ("ok",)

    def test_awaitable_from_asyncio(self) -> None:
        import asyncio

        session = FakeSession()
        channel, _ = _channel(session, timeout=5.0, quiet=0.2)
        _emit_after(session, 0.05, "ok")

        async def main() -> CommandReply:
            return await asyncio.wrap_future(channel.send_async("cmd"))

        reply = asyncio.run(main())
        assert reply.lines == ("ok",)


class TestReplyPolicy:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"timeout": 0},
            {"timeout": -1},
            {"quiet": 0},
            {"echo_timeout": 0},
            {"max_lines": 0},
        ],
    )
    def test_rejects_nonsense_values(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ValueError):
            ReplyPolicy(**kwargs)

    def test_defaults_are_honest_about_correlation(self) -> None:
        # Echo anchoring is opt-in until a probe confirms the server
        # echoes; the default reports correlated=False rather than
        # implying a correlation it cannot make.
        assert ReplyPolicy().echo_anchor is False


class TestAgainstARealSession:
    """End-to-end over a real GliderSession, not the fake.

    ``FakeSession`` above duplicates the session contract for precise
    timing control, which means it could drift from the real class
    without any test noticing.  This exercises the whole stack —
    supervisor, subscription, ordering, line reassembly, fan-out,
    capture — against a mocked transport only.
    """

    def test_command_reply_over_the_full_stack(self) -> None:
        import queue as queue_mod

        from sfmc_api.stomp import StompSubscription

        dialog: queue_mod.Queue[Any] = queue_mod.Queue()
        client = MagicMock()
        client.get_glider_details.return_value = {"data": {"id": 8, "state": "connected"}}
        client.subscribe_glider_output.return_value = StompSubscription(
            "sub", "/topic/glider-link-output/8", dialog
        )

        def send_command(_name: str, command: str) -> dict[str, Any]:
            # The glider "answers" in fragments that straddle line
            # boundaries and arrive out of sequence order.
            dialog.put({"sequenceNumber": 0, "data": f"{command}\r\nm_bat"})
            dialog.put({"sequenceNumber": 2, "data": "\r\n"})
            dialog.put({"sequenceNumber": 1, "data": "tery(volts)=15.1"})
            return {"status": "accepted"}

        client.send_command.side_effect = send_command

        session = GliderSession(client, "osu685", reconnect=False)
        session.start(timeout=5.0)
        try:
            channel = CommandChannel(
                client,
                session,
                policy=ReplyPolicy(timeout=5.0, quiet=0.3, echo_anchor=True),
            )
            reply = channel.send("sensor m_battery")
        finally:
            session.close()

        assert reply.complete is True
        assert reply.correlated is True
        assert reply.reason == "quiet"
        # Reordered, reassembled, and anchored past the echo.
        assert reply.lines == ("m_battery(volts)=15.1",)
        assert reply.dropped_lines == 0

    def test_channel_and_logger_share_one_subscription(self) -> None:
        """A monitor can keep logging while a command captures."""
        import queue as queue_mod

        from sfmc_api.stomp import StompSubscription

        dialog: queue_mod.Queue[Any] = queue_mod.Queue()
        client = MagicMock()
        client.get_glider_details.return_value = {"data": {"id": 8, "state": "connected"}}
        client.subscribe_glider_output.return_value = StompSubscription(
            "sub", "/topic/glider-link-output/8", dialog
        )

        def send_command(_name: str, _command: str) -> dict[str, Any]:
            dialog.put({"sequenceNumber": 0, "data": "m_battery(volts)=15.1\r\n"})
            return {}

        client.send_command.side_effect = send_command

        session = GliderSession(client, "osu685", reconnect=False)
        logged: list[str] = []
        session.on_line(lambda line: logged.append(line.text))
        session.start(timeout=5.0)
        try:
            channel = CommandChannel(client, session, policy=ReplyPolicy(timeout=5.0, quiet=0.3))
            reply = channel.send("sensor m_battery")
        finally:
            session.close()

        assert reply.lines == ("m_battery(volts)=15.1",)
        assert logged == ["m_battery(volts)=15.1"]
        # One subscription served both consumers.
        assert client.subscribe_glider_output.call_count == 1
