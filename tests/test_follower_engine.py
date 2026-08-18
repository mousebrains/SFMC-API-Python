"""Followers on the control engine: unchanged code, plus formations."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from sfmc_api.dialog_parser import SurfacingEvent
from sfmc_api.engine import READ_OPERATIONS, WRITE_OPERATIONS, EngineRunner
from sfmc_api.events import FleetStream
from sfmc_api.follower import BaseFollower, FollowerEngine, UploadBatch

# A real surfacing, trimmed to what DialogParser needs.  The carrier
# line matters: it is what moves the parser out of IDLE, and leaving it
# out made every one of these tests silently observe nothing.
SURFACING = [
    "Connection Event: Carrier Detect found.",
    "Vehicle Name: osu684",
    "Curr Time: Mon Aug 17 03:19:31 2026 MT: 18142",
    "GPS Location:  3346.435 N -12009.669 E measured     3.831 secs ago",
    "   sensor:m_battery(volts)=15.44                    39.301 secs ago",
    "Waypoint: (3346.4370,-12009.6440) Range: 39m, Bearing: 84deg, Age: 0:0h:m",
]


class _FakeSession:
    def __init__(self) -> None:
        self.epoch = 1
        self.closed = False
        self._lines: list[Any] = []
        self._connect: list[Any] = []
        self._disconnect: list[Any] = []

    def on_line(self, cb: Any) -> None:
        self._lines.append(cb)

    def on_raw_dialog(self, cb: Any) -> None:
        pass

    def on_event(self, topic: str, cb: Any) -> None:
        pass

    def on_connect(self, cb: Any) -> None:
        self._connect.append(cb)

    def on_disconnect(self, cb: Any) -> None:
        self._disconnect.append(cb)

    def start(self, timeout: float | None = 30.0) -> _FakeSession:
        return self

    def close(self) -> None:
        self.closed = True

    def emit_line(self, text: str) -> None:
        for cb in self._lines:
            cb(type("L", (), {"text": text})())

    def emit_disconnect(self) -> None:
        for cb in self._disconnect:
            cb()


class _FakeClient:
    def __init__(self) -> None:
        self.uploads: list[tuple[str, str, dict[str, Any]]] = []
        for op in READ_OPERATIONS | WRITE_OPERATIONS:
            call = self._make(op)
            # A double must be classified like the real thing: unmarked
            # means not requestable, which is the fail-safe rule.
            call.sfmc_mutates = op in WRITE_OPERATIONS  # type: ignore[attr-defined]
            setattr(self, op, call)

    def _make(self, op: str) -> Any:
        def call(*args: Any, **kwargs: Any) -> Any:
            if op == "upload_glider_file_contents":
                self.uploads.append((args[0], args[1], args[2]))
            return {}

        return call


class LegacyFollower(BaseFollower):
    """Written before formations existed.  Not modified for this test."""

    def __init__(self, config: Any, queue_in: Any, queue_out: Any) -> None:
        super().__init__(config, queue_in, queue_out)
        self.seen: list[str] = []

    def on_surfacing(self, event: SurfacingEvent) -> None:
        self.seen.append(event.vehicle_name)
        self.send_files(to_glider={"goto_l10.ma": "waypoints"})


class TestSendFilesCompatibility:
    """The one surface the design says must change, changed compatibly."""

    def test_send_files_still_works_with_no_glider(self) -> None:
        from queue import Queue

        follower = LegacyFollower({}, Queue(), Queue())
        follower.send_files(to_glider={"a.ma": "x"})
        batch = follower.queue_out.get_nowait()
        assert isinstance(batch, UploadBatch)
        assert batch.folders == {"to-glider": {"a.ma": "x"}}
        assert batch.glider is None, "unspecified means 'this pipeline's glider'"

    def test_send_files_defaults_to_the_current_surfacing(self) -> None:
        from queue import Queue

        follower = LegacyFollower({}, Queue(), Queue())
        follower.current_glider = "osu685"
        follower.send_files(to_glider={"a.ma": "x"})
        assert follower.queue_out.get_nowait().glider == "osu685"

    def test_a_formation_follower_may_name_another_glider(self) -> None:
        from queue import Queue

        follower = LegacyFollower({}, Queue(), Queue())
        follower.current_glider = "osu684"
        follower.send_files(to_glider={"a.ma": "x"}, glider="osu686")
        assert follower.queue_out.get_nowait().glider == "osu686"

    def test_the_run_loop_does_not_trust_the_parsed_name(self) -> None:
        """The legacy loop must not stamp a glider read from dialog.

        ``vehicle_name`` comes from an unanchored regex over glider
        output.  Using it as an upload target let untrusted firmware
        text choose which vehicle received steering files; the operator
        supplies the target instead, once, at startup.
        """
        from queue import Queue

        queue_in: Queue[Any] = Queue()
        follower = LegacyFollower({}, queue_in, Queue())
        queue_in.put(SurfacingEvent(vehicle_name="osu685-FROM-DIALOG"))
        queue_in.put(None)
        follower.run()
        assert follower.queue_out.get_nowait().glider is None

    def test_the_engine_stamps_the_trusted_glider(self) -> None:
        """FollowerEngine may, because its identity is the fleet tag.

        ``event.glider`` is the name the operator gave FleetStream, not
        anything parsed out of the dialog, so it is safe as a target --
        and it is what the follower sees, whatever the dialog claims.
        """
        seen: list[str | None] = []

        class Observer(BaseFollower):
            def on_surfacing(self, event: SurfacingEvent) -> None:
                seen.append(self.current_glider)

        engine = FollowerEngine(Observer)
        engine._deliver(SurfacingEvent(vehicle_name="LIES-FROM-DIALOG"), "osu685")
        assert seen == ["osu685"]
        assert engine.follower.current_glider is None, "cleared after delivery"


class TestFollowerEngine:
    def _fleet(self, sessions: dict[str, _FakeSession]) -> tuple[Any, Any, Any]:
        engine = FollowerEngine(LegacyFollower)
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        for name, session in sessions.items():
            fleet.add_glider(name, session)
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None, allow_writes=True)
        return engine, client, runner

    def test_an_unmodified_follower_receives_surfacings(self) -> None:
        session = _FakeSession()
        engine, _client, runner = self._fleet({"osu684": session})
        for line in SURFACING:
            session.emit_line(line)
        session.emit_disconnect()
        threading.Timer(0.5, runner.stop).start()
        runner.run()

        assert engine.follower.seen == ["osu684"]

    def test_files_are_uploaded_through_the_engine(self) -> None:
        session = _FakeSession()
        _engine, client, runner = self._fleet({"osu684": session})
        for line in SURFACING:
            session.emit_line(line)
        session.emit_disconnect()
        threading.Timer(0.5, runner.stop).start()
        runner.run()
        time.sleep(0.2)

        assert client.uploads, "the follower's files reached the client"
        glider, folder, files = client.uploads[0]
        assert glider == "osu684"
        assert folder == "to-glider"
        assert files == {"goto_l10.ma": "waypoints"}

    def test_uploads_are_refused_without_allow_writes(self) -> None:
        """A follower inherits every rail the engine has."""
        engine = FollowerEngine(LegacyFollower)
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        session = _FakeSession()
        fleet.add_glider("osu684", session)
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None)
        for line in SURFACING:
            session.emit_line(line)
        session.emit_disconnect()
        threading.Timer(0.5, runner.stop).start()
        runner.run()
        time.sleep(0.2)

        assert engine.follower.seen == ["osu684"], "the follower still ran"
        assert client.uploads == [], "but nothing was uploaded"

    def test_two_gliders_reach_one_follower_instance(self) -> None:
        """The formation case, with no change to on_surfacing."""
        first, second = _FakeSession(), _FakeSession()
        engine, client, runner = self._fleet({"osu684": first, "osu685": second})

        for session, name in ((first, "osu684"), (second, "osu685")):
            for line in SURFACING:
                session.emit_line(line.replace("osu684", name))
            session.emit_disconnect()
        threading.Timer(0.6, runner.stop).start()
        runner.run()
        time.sleep(0.2)

        assert sorted(engine.follower.seen) == ["osu684", "osu685"]
        assert sorted({g for g, _, _ in client.uploads}) == ["osu684", "osu685"]

    def test_each_glider_gets_its_own_parser(self) -> None:
        """Two gliders surfacing at once must not braid into one event."""
        first, second = _FakeSession(), _FakeSession()
        engine, _client, runner = self._fleet({"osu684": first, "osu685": second})

        # Interleave the two surfacings line by line.
        for line in SURFACING:
            first.emit_line(line)
            second.emit_line(line.replace("osu684", "osu685"))
        first.emit_disconnect()
        second.emit_disconnect()
        threading.Timer(0.6, runner.stop).start()
        runner.run()

        assert sorted(engine.follower.seen) == ["osu684", "osu685"]

    def test_a_raising_follower_does_not_end_the_deployment(self) -> None:
        class Fragile(BaseFollower):
            def on_surfacing(self, event: SurfacingEvent) -> None:
                raise ValueError("bad maths")

        engine = FollowerEngine(Fragile)
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        session = _FakeSession()
        fleet.add_glider("osu684", session)
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None)
        for line in SURFACING:
            session.emit_line(line)
        session.emit_disconnect()
        threading.Timer(0.5, runner.stop).start()
        runner.run()

        assert runner.failures == 0, "a follower's error is not an engine failure"

    def test_the_follower_thread_is_never_started(self) -> None:
        """Two schedulers disagreeing about who is in charge is a bug."""
        engine = FollowerEngine(LegacyFollower)
        assert not engine.follower.is_alive()

    def test_config_reaches_the_follower(self) -> None:
        engine = FollowerEngine(LegacyFollower, {"speed": 0.4})
        assert engine.follower.config == {"speed": 0.4}


class TestUploadBatch:
    def test_legacy_dict_payloads_are_still_understood(self) -> None:
        """An exotic follower writing the old shape must not break."""
        engine = FollowerEngine(LegacyFollower)
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        fleet.add_glider("osu684", _FakeSession())
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None, allow_writes=True)
        try:
            engine._queue_out.put({"to-glider": {"legacy.ma": "x"}})
            engine._drain_uploads("osu684")
            time.sleep(0.3)
            assert client.uploads == [("osu684", "to-glider", {"legacy.ma": "x"})]
        finally:
            runner.stop()
            runner.close()

    @pytest.mark.parametrize("empty", [{}, {"to-glider": {}}])
    def test_empty_batches_upload_nothing(self, empty: dict[str, Any]) -> None:
        engine = FollowerEngine(LegacyFollower)
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        fleet.add_glider("osu684", _FakeSession())
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None, allow_writes=True)
        try:
            engine._queue_out.put(UploadBatch(folders=empty))
            engine._drain_uploads("osu684")
            time.sleep(0.2)
            assert client.uploads == []
        finally:
            runner.stop()
            runner.close()


class TestDeduplicationIsShared:
    """The drift that already happened, now closed.

    sfmc-follow has always suppressed a surfacing re-delivered after a
    reconnect -- SFMC replays dialog when a subscription is replaced.
    FollowerEngine re-implemented the parser feeding and omitted it, so
    a follower on the engine acted on the same surfacing twice: it
    re-ran its decision and re-sent its files.  Both now use
    SurfacingStream.
    """

    def test_a_replayed_surfacing_reaches_the_follower_once(self) -> None:
        engine = FollowerEngine(LegacyFollower)
        # SFMC's resubscribe replay: the identical block, twice.
        for _ in range(2):
            for line in SURFACING:
                surfacing = engine._stream("osu684").feed(line)
                if surfacing is not None:
                    engine.follower.on_surfacing(surfacing)
        assert engine.follower.seen == ["osu684"], "the replay must be suppressed"

    def test_the_parser_resets_but_the_dedup_cache_does_not(self) -> None:
        """Their lifetimes differ, and that difference is the point.

        Resetting the cache at a stream boundary would defeat it
        entirely: a boundary is exactly when the replay arrives.
        """
        from sfmc_api.dialog_parser import SurfacingStream

        stream = SurfacingStream()
        for line in SURFACING:
            stream.feed(line)
        stream.reset()
        again = [stream.feed(line) for line in SURFACING]
        assert all(event is None for event in again)

    def test_both_consumers_use_the_same_implementation(self) -> None:
        # import_module, because sfmc_api.follow_glider the *function*
        # is exported from the package and shadows the module name.
        import importlib

        from sfmc_api.dialog_parser import SurfacingDeduplicator

        legacy = importlib.import_module("sfmc_api.follow_glider")
        assert legacy.SurfacingDeduplicator is SurfacingDeduplicator


class TestShutdownDrains:
    """Files queued during shutdown must still be uploaded.

    sfmc-follow says so explicitly: "files queued just before a
    disconnect or Ctrl-C must still be uploaded... the glider would fly
    stale waypoints for the whole next dive."  On the engine path
    on_stop's flush queued uploads and close() then cancelled them.
    """

    def test_files_flushed_in_on_stop_are_uploaded(self) -> None:
        engine = FollowerEngine(LegacyFollower)
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        session = _FakeSession()
        fleet.add_glider("osu684", session)
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None, allow_writes=True)
        # A surfacing the parser is still holding when we stop: no
        # disconnect arrives, so only on_stop's flush will emit it.
        for line in SURFACING[:-1]:
            session.emit_line(line)
        session.emit_line(SURFACING[-1])
        threading.Timer(0.4, runner.stop).start()
        runner.run()

        assert client.uploads, "the shutdown flush reached the client"
        assert client.uploads[0][0] == "osu684"
