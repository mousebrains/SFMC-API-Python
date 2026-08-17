"""Tests for GliderSession: fan-out, drop accounting, and lifecycle."""

from __future__ import annotations

import queue as queue_mod
import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest

from sfmc_api.session import GliderSession, Listener, _Broadcaster
from sfmc_api.stomp import StompSubscription


def _sub(messages: list[Any], *, keep_open: bool = False) -> StompSubscription:
    q: queue_mod.Queue[Any] = queue_mod.Queue()
    for message in messages:
        q.put(message)
    if not keep_open:
        q.put(None)
    return StompSubscription("sub", "/topic/test", q)


def _client(dialog: list[Any] | None = None, **kwargs: Any) -> MagicMock:
    client = MagicMock()
    client.subscribe_glider_output.return_value = _sub(dialog or [], **kwargs)
    client.get_glider_details.return_value = {"data": {"id": 8, "state": "connected"}}
    return client


class TestListener:
    def test_drops_oldest_and_counts_the_loss(self) -> None:
        listener: Listener[int] = Listener(maxsize=2)
        for value in (1, 2, 3, 4):
            listener._publish(value)
        # The newest data survives; the loss is reported rather than
        # silently swallowed.
        assert listener.get(timeout=0.1) == 3
        assert listener.get(timeout=0.1) == 4
        assert listener.dropped == 2

    def test_close_ends_iteration(self) -> None:
        listener: Listener[int] = Listener()
        listener._publish(1)
        listener.close()
        assert list(listener) == [1]

    def test_close_is_visible_to_every_waiter(self) -> None:
        listener: Listener[int] = Listener()
        listener.close()
        assert listener.get(timeout=0.1) is None
        assert listener.get(timeout=0.1) is None


class TestBroadcaster:
    def test_every_listener_gets_every_item(self) -> None:
        broadcaster: _Broadcaster[str] = _Broadcaster()
        first = broadcaster.attach()
        second = broadcaster.attach()
        broadcaster.publish("a")
        assert first.get(timeout=0.1) == "a"
        assert second.get(timeout=0.1) == "a"

    def test_detached_listener_stops_receiving(self) -> None:
        broadcaster: _Broadcaster[str] = _Broadcaster()
        listener = broadcaster.attach()
        listener.close()
        broadcaster.publish("a")
        # Only the close sentinel is queued.
        assert listener.get(timeout=0.1) is None

    def test_failing_callback_cannot_kill_the_pump(self) -> None:
        broadcaster: _Broadcaster[str] = _Broadcaster()
        seen: list[str] = []
        broadcaster.subscribe(lambda _: (_ for _ in ()).throw(RuntimeError("boom")))
        broadcaster.subscribe(seen.append)
        broadcaster.publish("a")
        assert seen == ["a"]


class TestGliderSession:
    def test_rejects_unknown_topics(self) -> None:
        with pytest.raises(ValueError, match="Unknown topic"):
            GliderSession(MagicMock(), "osu685", topics=["nope"])

    def test_requires_at_least_one_topic(self) -> None:
        with pytest.raises(ValueError, match="At least one topic"):
            GliderSession(MagicMock(), "osu685", topics=[])

    def test_listening_to_an_unsubscribed_topic_fails_loudly(self) -> None:
        session = GliderSession(MagicMock(), "osu685", topics=["dialog"])
        with pytest.raises(ValueError, match="not subscribed"):
            session.listen("scripts")

    def test_fans_dialog_out_to_two_listeners(self) -> None:
        client = _client(
            [{"sequenceNumber": 0, "data": "hello\r\n"}],
            keep_open=True,
        )
        session = GliderSession(client, "osu685", reconnect=False)
        first = session.dialog_listener()
        second = session.dialog_listener()
        session.start(timeout=5.0)
        try:
            line_one = first.get(timeout=5.0)
            line_two = second.get(timeout=5.0)
        finally:
            session.close()

        # Both consumers see the same line, from a single subscription
        # and a single pass through the reordering buffer.
        assert line_one is not None and line_one.text == "hello"
        assert line_two is not None and line_two.text == "hello"
        assert client.subscribe_glider_output.call_count == 1

    def test_raw_listener_sees_an_unterminated_prompt(self) -> None:
        """The line stream cannot deliver a GliderDos prompt.

        A prompt carries no trailing newline, so it never completes a
        line: it sits in the assembler's buffer and is discarded at the
        session boundary.  Nine of the twenty known SFMC scripts trigger
        on that prompt, so a stream matcher needs the raw chunks.
        """
        client = _client(
            [{"sequenceNumber": 0, "data": "done\r\nGliderDos N -1 > "}],
            keep_open=True,
        )
        session = GliderSession(client, "osu685", reconnect=False)
        raw = session.raw_dialog_listener()
        lines = session.dialog_listener()
        session.start(timeout=5.0)
        try:
            chunk = raw.get(timeout=5.0)
            line = lines.get(timeout=5.0)
        finally:
            session.close()

        assert chunk == "done\r\nGliderDos N -1 > "
        assert "GliderDos" in (chunk or "")
        # The line consumer gets only the terminated part; the prompt is
        # still buffered and will be dropped at the boundary.
        assert line is not None and line.text == "done"
        assert lines.get(timeout=0.2) is None

    def test_raw_chunks_are_not_reassembled(self) -> None:
        """Chunks arrive as sent — half lines and all."""
        client = _client(
            [
                {"sequenceNumber": 0, "data": "abc"},
                {"sequenceNumber": 1, "data": "def\r\n"},
            ],
            keep_open=True,
        )
        session = GliderSession(client, "osu685", reconnect=False)
        raw = session.raw_dialog_listener()
        session.start(timeout=5.0)
        try:
            first = raw.get(timeout=5.0)
            second = raw.get(timeout=5.0)
        finally:
            session.close()
        assert [first, second] == ["abc", "def\r\n"]

    def test_raw_callbacks_receive_chunks(self) -> None:
        client = _client([{"sequenceNumber": 0, "data": "xy"}], keep_open=True)
        session = GliderSession(client, "osu685", reconnect=False)
        seen: list[str] = []
        session.on_raw_dialog(seen.append)
        session.start(timeout=5.0)
        deadline = time.monotonic() + 5.0
        while not seen and time.monotonic() < deadline:
            time.sleep(0.01)
        session.close()
        assert seen == ["xy"]

    def test_raw_listener_needs_the_dialog_topic(self) -> None:
        session = GliderSession(MagicMock(), "osu685", topics=["scripts"])
        with pytest.raises(ValueError, match="not subscribed"):
            session.raw_dialog_listener()

    def test_callbacks_receive_lines(self) -> None:
        client = _client([{"sequenceNumber": 0, "data": "abc\r\n"}], keep_open=True)
        session = GliderSession(client, "osu685", reconnect=False)
        seen: list[str] = []
        session.on_line(lambda line: seen.append(line.text))
        session.start(timeout=5.0)
        deadline = time.monotonic() + 5.0
        while not seen and time.monotonic() < deadline:
            time.sleep(0.01)
        session.close()
        assert seen == ["abc"]

    def test_epoch_advances_once_per_subscribed_session(self) -> None:
        client = _client([], keep_open=True)
        session = GliderSession(client, "osu685", reconnect=False)
        assert session.epoch == 0
        session.start(timeout=5.0)
        assert session.epoch == 1
        session.close()

    def test_on_connect_reports_first_session_as_not_reconnected(self) -> None:
        client = _client([], keep_open=True)
        session = GliderSession(client, "osu685", reconnect=False)
        seen: list[bool] = []
        session.on_connect(seen.append)
        session.start(timeout=5.0)
        session.close()
        assert seen == [False]

    def test_start_twice_is_refused(self) -> None:
        client = _client([], keep_open=True)
        session = GliderSession(client, "osu685", reconnect=False)
        session.start(timeout=5.0)
        try:
            with pytest.raises(RuntimeError, match="already started"):
                session.start()
        finally:
            session.close()

    def test_close_releases_listeners(self) -> None:
        client = _client([], keep_open=True)
        session = GliderSession(client, "osu685", reconnect=False)
        listener = session.dialog_listener()
        session.start(timeout=5.0)
        session.close()
        assert listener.get(timeout=1.0) is None

    def test_glider_is_connected_reads_state(self) -> None:
        client = _client([], keep_open=True)
        session = GliderSession(client, "osu685", reconnect=False)
        assert session.glider_is_connected() is True
        client.get_glider_details.return_value = {"data": {"state": "disconnected"}}
        assert session.glider_is_connected() is False

    def test_glider_is_connected_assumes_connected_on_error(self) -> None:
        from sfmc_api.exceptions import APIError

        client = _client([], keep_open=True)
        client.get_glider_details.side_effect = APIError(500, "boom")
        session = GliderSession(client, "osu685", reconnect=False)
        assert session.glider_is_connected() is True


class TestGliderIdCaching:
    def test_glider_id_is_looked_up_once(self) -> None:
        from sfmc_api import SFMCClient
        from sfmc_api.config import SFMCConfig

        client = SFMCClient(
            config=SFMCConfig(host="h", client_id="c", secret="s", tls_verify=False)
        )
        calls = 0

        def details(_name: str) -> dict[str, Any]:
            nonlocal calls
            calls += 1
            return {"data": {"id": 42}}

        client.get_glider_details = details  # type: ignore[method-assign]
        assert client._get_glider_id("osu685") == 42
        assert client._get_glider_id("osu685") == 42
        assert calls == 1

        client.clear_glider_id_cache()
        assert client._get_glider_id("osu685") == 42
        assert calls == 2

    def test_cache_is_per_glider(self) -> None:
        from sfmc_api import SFMCClient
        from sfmc_api.config import SFMCConfig

        client = SFMCClient(
            config=SFMCConfig(host="h", client_id="c", secret="s", tls_verify=False)
        )
        ids = {"osu684": 1, "osu685": 2}
        client.get_glider_details = lambda name: {  # type: ignore[method-assign]
            "data": {"id": ids[name]}
        }
        assert client._get_glider_id("osu684") == 1
        assert client._get_glider_id("osu685") == 2


class TestSupervisedReconnect:
    def test_session_survives_a_dropped_stream(self) -> None:
        """A dropped subscription must be replaced, not end the session."""
        client = MagicMock()
        client.get_glider_details.return_value = {"data": {"id": 8, "state": "connected"}}
        subs = [
            _sub([{"sequenceNumber": 0, "data": "first\r\n"}]),  # closes immediately
            _sub([{"sequenceNumber": 0, "data": "second\r\n"}], keep_open=True),
        ]
        client.subscribe_glider_output.side_effect = subs

        session = GliderSession(
            client,
            "osu685",
            reconnect=True,
            reconnect_initial_delay=0.01,
            reconnect_max_delay=0.01,
            reconnect_jitter=0.0,
        )
        seen: list[str] = []
        session.on_line(lambda line: seen.append(line.text))
        session.start(timeout=5.0)
        deadline = time.monotonic() + 5.0
        while len(seen) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        epoch = session.epoch
        session.close()

        assert seen == ["first", "second"]
        assert epoch >= 2, "a reconnect must advance the epoch"

    def test_raw_listener_survives_a_reconnect(self) -> None:
        """A raw consumer must keep receiving after the stream rebuilds.

        run_live() attaches one raw listener for the life of a run that
        may last hours across many dives, during which the stream drops
        every time the glider submerges.  A listener that went deaf
        after the first drop would look exactly like a glider that
        never surfaced.
        """
        client = MagicMock()
        client.get_glider_details.return_value = {"data": {"id": 8, "state": "connected"}}
        client.subscribe_glider_output.side_effect = [
            _sub([{"sequenceNumber": 0, "data": "before"}]),  # closes immediately
            _sub([{"sequenceNumber": 0, "data": "after"}], keep_open=True),
        ]

        session = GliderSession(
            client,
            "osu685",
            reconnect=True,
            reconnect_initial_delay=0.01,
            reconnect_max_delay=0.01,
            reconnect_jitter=0.0,
        )
        raw = session.raw_dialog_listener()
        session.start(timeout=5.0)
        seen: list[str] = []
        deadline = time.monotonic() + 5.0
        while len(seen) < 2 and time.monotonic() < deadline:
            chunk = raw.get(timeout=0.1)
            if chunk is not None:
                seen.append(chunk)
        session.close()

        assert seen == ["before", "after"], "the raw listener went deaf across the reconnect"

    def test_epoch_advances_before_that_session_delivers_data(self) -> None:
        """No consumer may see data from a session that is not yet counted.

        CI caught the reverse order: the second session's first line
        arrived before its epoch bump, so a reader comparing epochs
        would have attributed new-session data to the old session —
        which is exactly what CommandChannel uses the epoch to rule out.
        """
        client = MagicMock()
        client.get_glider_details.return_value = {"data": {"id": 8, "state": "connected"}}
        client.subscribe_glider_output.side_effect = [
            _sub([{"sequenceNumber": 0, "data": "first\r\n"}]),  # closes immediately
            _sub([{"sequenceNumber": 0, "data": "second\r\n"}], keep_open=True),
        ]

        session = GliderSession(
            client,
            "osu685",
            reconnect=True,
            reconnect_initial_delay=0.01,
            reconnect_max_delay=0.01,
            reconnect_jitter=0.0,
        )
        # Record the epoch observed at the moment each line is seen.
        observed: list[tuple[str, int]] = []
        session.on_line(lambda line: observed.append((line.text, session.epoch)))
        session.start(timeout=5.0)
        deadline = time.monotonic() + 5.0
        while len(observed) < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        session.close()

        assert [text for text, _ in observed] == ["first", "second"]
        assert observed[0][1] == 1, "first session's data must carry epoch 1"
        assert observed[1][1] == 2, "second session's data must carry epoch 2"


class TestThreadSafety:
    def test_concurrent_attach_and_publish(self) -> None:
        broadcaster: _Broadcaster[int] = _Broadcaster()
        stop = threading.Event()

        def publisher() -> None:
            value = 0
            while not stop.is_set():
                broadcaster.publish(value)
                value += 1

        thread = threading.Thread(target=publisher, daemon=True)
        thread.start()
        try:
            for _ in range(50):
                listener = broadcaster.attach(maxsize=4)
                listener.close()
        finally:
            stop.set()
            thread.join(timeout=2)
        assert not thread.is_alive()
