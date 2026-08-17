"""Tests for the fleet event merge: tagging, ordering, drop accounting."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from sfmc_api.events import (
    DEFAULT_PAIR_MAXSIZE,
    SOURCES,
    DroppedNotice,
    Event,
    EventMerge,
    FleetStream,
    StreamNotice,
)


class TestTagging:
    def test_every_event_carries_its_glider(self) -> None:
        """The whole reason this is multi-glider from the first commit."""
        merge = EventMerge()
        merge.publish("osu684", "dialog", "one")
        merge.publish("osu685", "dialog", "two")

        first = merge.get(timeout=1)
        second = merge.get(timeout=1)
        assert first is not None and second is not None
        assert (first.glider, first.body) == ("osu684", "one")
        assert (second.glider, second.body) == ("osu685", "two")

    def test_seq_is_monotonic_across_gliders_and_sources(self) -> None:
        merge = EventMerge()
        merge.publish("osu684", "dialog", "a")
        merge.publish("osu685", "connections", {"x": 1})
        merge.publish("osu684", "dialog", "b")

        seqs = [e.seq for e in (merge.get(timeout=1), merge.get(timeout=1), merge.get(timeout=1))]  # type: ignore[union-attr]
        assert seqs == sorted(seqs)
        assert len(set(seqs)) == 3

    def test_received_at_is_the_host_clock(self) -> None:
        """Not the glider's -- they differ by 48 minutes on osusim."""
        merge = EventMerge(now=lambda: 1234.5)
        event = merge.publish("osu684", "dialog", "Curr Time: Mon Aug 17 03:19:31 2026")
        assert event is not None
        assert event.received_at == 1234.5

    def test_documented_sources_are_all_known(self) -> None:
        for source in ("dialog", "dialog.raw", "connections", "dropped", "stream"):
            assert source in SOURCES


class TestOrdering:
    def test_order_within_a_pair_is_strict(self) -> None:
        merge = EventMerge()
        for i in range(50):
            merge.publish("osu684", "dialog", i)
        seen = [merge.get(timeout=1).body for _ in range(50)]  # type: ignore[union-attr]
        assert seen == list(range(50))

    def test_order_across_pairs_is_arrival_order(self) -> None:
        merge = EventMerge()
        merge.publish("osu684", "dialog", "first")
        merge.publish("osu685", "dialog", "second")
        merge.publish("osu684", "connections", "third")
        merge.publish("osu685", "dialog", "fourth")

        bodies = [merge.get(timeout=1).body for _ in range(4)]  # type: ignore[union-attr]
        assert bodies == ["first", "second", "third", "fourth"]

    def test_one_glider_flooding_does_not_starve_another(self) -> None:
        """The reason the bound is per pair rather than global.

        A surfacing dumps hundreds of lines in milliseconds.  That must
        not evict the connection events of gliders still in the water.
        """
        merge = EventMerge(maxsize=4)
        for i in range(100):
            merge.publish("noisy", "dialog", i)
        merge.publish("quiet", "connections", "still here")

        bodies = []
        while (event := merge.get(timeout=0.2)) is not None:
            bodies.append((event.glider, event.body))
        assert ("quiet", "still here") in bodies


class TestDropAccounting:
    def test_oldest_is_dropped_and_newest_survives(self) -> None:
        merge = EventMerge(maxsize=3)
        for i in range(10):
            merge.publish("osu684", "dialog", i)

        kept = []
        while (event := merge.get(timeout=0.2)) is not None:
            if event.source == "dialog":
                kept.append(event.body)
        assert kept == [7, 8, 9], "drop-oldest keeps the newest data"

    def test_a_drop_is_reported_not_silent(self) -> None:
        merge = EventMerge(maxsize=2)
        for i in range(5):
            merge.publish("osu684", "dialog", i)

        events = []
        while (event := merge.get(timeout=0.2)) is not None:
            events.append(event)

        notices = [e for e in events if e.source == "dropped"]
        assert len(notices) == 1
        notice = notices[0].body
        assert isinstance(notice, DroppedNotice)
        assert notice.source == "dialog"
        assert notice.count == 3
        assert notices[0].glider == "osu684"

    def test_a_burst_yields_one_notice_carrying_the_whole_count(self) -> None:
        merge = EventMerge(maxsize=2)
        for i in range(20):
            merge.publish("osu684", "dialog", i)
        notices = []
        while (event := merge.get(timeout=0.2)) is not None:
            if event.source == "dropped":
                notices.append(event.body.count)
        assert sum(notices) == 18
        assert len(notices) == 1, "one notice per burst, not one per lost event"

    def test_a_saturated_pair_still_reports(self) -> None:
        """Silent loss is the one outcome not on offer.

        Reporting only on drain would mean a pair that never drains
        never reports, so a full queue's worth of further loss forces a
        notice mid-burst.
        """
        merge = EventMerge(maxsize=2)
        for i in range(10):
            merge.publish("osu684", "dialog", i)
        # Consume without ever draining: keep publishing as we read.
        seen_notice = False
        for i in range(10, 20):
            event = merge.get(timeout=0.2)
            if event is not None and event.source == "dropped":
                seen_notice = True
                assert event.body.reason in {"drained", "saturating"}
            merge.publish("osu684", "dialog", i)
        assert seen_notice

    def test_drops_are_counted_per_pair(self) -> None:
        merge = EventMerge(maxsize=2)
        for i in range(6):
            merge.publish("osu684", "dialog", i)
        merge.publish("osu685", "dialog", "fine")
        counts = merge.dropped()
        assert counts[("osu684", "dialog")] == 4
        assert ("osu685", "dialog") not in counts

    def test_no_drops_means_no_notices(self) -> None:
        merge = EventMerge(maxsize=10)
        merge.publish("osu684", "dialog", "a")
        event = merge.get(timeout=1)
        assert event is not None and event.source == "dialog"
        assert merge.get(timeout=0.1) is None


class TestLifecycle:
    def test_get_times_out_when_idle(self) -> None:
        assert EventMerge().get(timeout=0.05) is None

    def test_close_releases_a_blocked_consumer(self) -> None:
        merge = EventMerge()
        result: list[Any] = []

        def consume() -> None:
            result.append(merge.get())

        thread = threading.Thread(target=consume)
        thread.start()
        time.sleep(0.05)
        merge.close()
        thread.join(timeout=5)
        assert result == [None]

    def test_publish_after_close_is_refused(self) -> None:
        merge = EventMerge()
        merge.close()
        assert merge.publish("osu684", "dialog", "late") is None

    def test_iteration_ends_at_close(self) -> None:
        merge = EventMerge()
        merge.publish("osu684", "dialog", "a")
        merge.publish("osu684", "dialog", "b")
        merge.close()
        assert [e.body for e in merge] == ["a", "b"]

    def test_forget_discards_a_departed_glider(self) -> None:
        merge = EventMerge()
        merge.publish("osu684", "dialog", "a")
        merge.publish("osu685", "dialog", "b")
        merge.forget("osu684")
        remaining = [e.glider for e in iter(lambda: merge.get(timeout=0.1), None)]
        assert remaining == ["osu685"]

    def test_maxsize_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            EventMerge(maxsize=0)

    def test_publish_is_thread_safe(self) -> None:
        """Producers are session pump threads, one per glider."""
        merge = EventMerge(maxsize=10_000)

        def produce(glider: str) -> None:
            for i in range(200):
                merge.publish(glider, "dialog", i)

        threads = [threading.Thread(target=produce, args=(f"osu{n}",)) for n in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        assert merge.pending() == 800
        seqs = [e.seq for e in iter(lambda: merge.get(timeout=0.2), None)]
        assert len(set(seqs)) == 800, "every event got a unique seq"
        assert seqs == sorted(seqs)


class _FakeSession:
    """The GliderSession surface FleetStream actually uses."""

    def __init__(self, epoch: int = 1) -> None:
        self.epoch = epoch
        self.closed = False
        self.started = False
        self._lines: list[Any] = []
        self._raw: list[Any] = []
        self._events: dict[str, list[Any]] = {}
        self._connect: list[Any] = []
        self._disconnect: list[Any] = []

    def on_line(self, callback: Any) -> None:
        self._lines.append(callback)

    def on_raw_dialog(self, callback: Any) -> None:
        self._raw.append(callback)

    def on_event(self, topic: str, callback: Any) -> None:
        self._events.setdefault(topic, []).append(callback)

    def on_connect(self, callback: Any) -> None:
        self._connect.append(callback)

    def on_disconnect(self, callback: Any) -> None:
        self._disconnect.append(callback)

    def start(self, timeout: float | None = 30.0) -> _FakeSession:
        self.started = True
        return self

    def close(self) -> None:
        self.closed = True

    # ── Drive it from a test ─────────────────────────────────────────

    def emit_line(self, text: str) -> None:
        for callback in self._lines:
            callback(type("L", (), {"text": text})())

    def emit_raw(self, chunk: str) -> None:
        for callback in self._raw:
            callback(chunk)

    def emit_event(self, topic: str, message: Any) -> None:
        for callback in self._events.get(topic, []):
            callback(message)

    def emit_connect(self, reconnected: bool) -> None:
        for callback in self._connect:
            callback(reconnected)

    def emit_disconnect(self) -> None:
        for callback in self._disconnect:
            callback()


class TestFleetStream:
    """Two fake sessions, as the phasing plan calls for."""

    def _fleet(self, **kwargs: Any) -> tuple[FleetStream, _FakeSession, _FakeSession]:
        fleet = FleetStream(object(), **kwargs)  # type: ignore[arg-type]
        first, second = _FakeSession(), _FakeSession()
        fleet.add_glider("osu684", first)
        fleet.add_glider("osu685", second)
        return fleet, first, second

    def test_two_gliders_interleave_in_one_queue(self) -> None:
        fleet, first, second = self._fleet()
        first.emit_line("684 says hello")
        second.emit_line("685 says hello")
        first.emit_line("684 again")

        got = [(e.glider, e.body) for e in iter(lambda: fleet.get(timeout=0.2), None)]
        assert got == [
            ("osu684", "684 says hello"),
            ("osu685", "685 says hello"),
            ("osu684", "684 again"),
        ]

    def test_sessions_are_started_after_wiring(self) -> None:
        """Nothing may arrive before there is somewhere to put it."""
        fleet, first, _ = self._fleet()
        assert first.started
        fleet.close()

    def test_stream_events_carry_state_and_epoch(self) -> None:
        fleet, first, _ = self._fleet()
        first.emit_connect(reconnected=False)
        first.emit_disconnect()
        first.emit_connect(reconnected=True)

        notices = [
            e.body for e in iter(lambda: fleet.get(timeout=0.2), None) if e.source == "stream"
        ]
        assert [n.state for n in notices] == ["connected", "disconnected", "reconnected"]
        assert all(isinstance(n, StreamNotice) for n in notices)
        assert notices[0].epoch == 1

    def test_raw_source_is_opt_in(self) -> None:
        fleet, first, _ = self._fleet(sources=("dialog.raw",))
        first.emit_raw("GliderDos N -1 > ")
        event = fleet.get(timeout=0.2)
        assert event is not None
        assert event.source == "dialog.raw"
        assert event.body == "GliderDos N -1 > "

    def test_both_dialog_forms_at_once(self) -> None:
        fleet, first, _ = self._fleet(sources=("dialog", "dialog.raw"))
        first.emit_raw("partial")
        first.emit_line("complete")
        sources = [e.source for e in iter(lambda: fleet.get(timeout=0.2), None)]
        assert sources == ["dialog.raw", "dialog"]

    def test_topic_events_are_tagged(self) -> None:
        fleet, first, _ = self._fleet(sources=("connections",))
        first.emit_event("connections", {"state": "connected"})
        event = fleet.get(timeout=0.2)
        assert event is not None
        assert (event.glider, event.source) == ("osu684", "connections")
        assert event.body == {"state": "connected"}

    def test_removing_a_glider_closes_and_forgets_it(self) -> None:
        fleet, first, _ = self._fleet()
        first.emit_line("before removal")
        fleet.remove_glider("osu684")
        assert first.closed
        assert fleet.gliders == ("osu685",)
        # Its queued events go with it.
        assert fleet.get(timeout=0.1) is None

    def test_a_glider_may_join_at_runtime(self) -> None:
        fleet, _, _ = self._fleet()
        third = _FakeSession()
        fleet.add_glider("osu686", third)
        third.emit_line("late arrival")
        event = fleet.get(timeout=0.2)
        assert event is not None and event.glider == "osu686"
        assert fleet.gliders == ("osu684", "osu685", "osu686")

    def test_adding_the_same_glider_twice_is_refused(self) -> None:
        fleet, _, _ = self._fleet()
        with pytest.raises(ValueError, match="already streaming"):
            fleet.add_glider("osu684", _FakeSession())

    def test_removing_an_unknown_glider_is_refused(self) -> None:
        fleet, _, _ = self._fleet()
        with pytest.raises(ValueError, match="not streaming"):
            fleet.remove_glider("nobody")

    def test_unsubscribable_source_fails_at_construction(self) -> None:
        """Not as a match arm that silently never fires."""
        with pytest.raises(ValueError, match="unsubscribable"):
            FleetStream(object(), sources=("result",))  # type: ignore[arg-type]

    def test_close_closes_every_session(self) -> None:
        fleet, first, second = self._fleet()
        fleet.close()
        assert first.closed and second.closed
        assert fleet.get(timeout=0.1) is None

    def test_context_manager_closes(self) -> None:
        first = _FakeSession()
        with FleetStream(object()) as fleet:  # type: ignore[arg-type]
            fleet.add_glider("osu684", first)
        assert first.closed


class TestEventShape:
    def test_event_is_frozen(self) -> None:
        import dataclasses

        event = Event(glider="osu684", source="dialog", body="x", received_at=0.0, seq=0)
        with pytest.raises(dataclasses.FrozenInstanceError):
            event.glider = "osu685"  # type: ignore[misc]

    def test_default_bound_exceeds_a_real_surfacing_burst(self) -> None:
        """437 lines in ~10ms, measured on osusim 2026-08-17."""
        assert DEFAULT_PAIR_MAXSIZE > 437
