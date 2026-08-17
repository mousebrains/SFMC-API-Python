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
            setattr(self, op, self._make(op))

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

    def test_the_run_loop_sets_the_current_glider(self) -> None:
        """So an unmodified follower's send_files targets correctly."""
        from queue import Queue

        queue_in: Queue[Any] = Queue()
        follower = LegacyFollower({}, queue_in, Queue())
        queue_in.put(SurfacingEvent(vehicle_name="osu685"))
        queue_in.put(None)
        follower.run()
        assert follower.queue_out.get_nowait().glider == "osu685"


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
