"""Tests for BaseControlEngine and its runner (phase 2, read-only)."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from sfmc_api.engine import (
    READ_OPERATIONS,
    BaseControlEngine,
    EngineRunner,
    WriteRefused,
)
from sfmc_api.events import Event, FleetStream


class _FakeSession:
    """The GliderSession surface FleetStream uses."""

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


class _FakeClient:
    """Only the read operations, plus one write to prove it is refused."""

    def __init__(self, **returns: Any) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._returns = returns
        for op in READ_OPERATIONS:
            setattr(self, op, self._make(op))

    def _make(self, op: str) -> Any:
        def call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((op, args))
            value = self._returns.get(op, {"op": op, "args": args})
            if isinstance(value, Exception):
                raise value
            return value

        return call

    def send_command(self, glider: str, command: str) -> dict[str, Any]:
        raise AssertionError("a read-only engine must never reach a write")


def _runner(engine: BaseControlEngine, client: Any = None, **kwargs: Any) -> EngineRunner:
    client = client if client is not None else _FakeClient()
    fleet = FleetStream(client, sources=tuple(engine.sources))
    return EngineRunner(engine, client, fleet=fleet, watchdog=None, **kwargs)


class TestEngineSurface:
    def test_engine_without_a_runner_says_so(self) -> None:
        engine = BaseControlEngine()
        with pytest.raises(RuntimeError, match="not attached to a runner"):
            engine.request("get_glider_details", "osu684", glider="osu684")

    def test_empty_sources_is_refused(self) -> None:
        """Validated at construction: an engine that hears nothing is a bug."""

        class Silent(BaseControlEngine):
            sources = ()

        with pytest.raises(ValueError, match="nothing would ever arrive"):
            EngineRunner(Silent(), _FakeClient(), watchdog=None)

    def test_config_is_passed_through_uninspected(self) -> None:
        engine = BaseControlEngine(config={"anything": [1, 2, 3]})
        assert engine.config == {"anything": [1, 2, 3]}

    def test_default_config_is_a_dict(self) -> None:
        assert BaseControlEngine().config == {}


class TestReadOnly:
    def test_a_write_is_refused_by_name(self) -> None:
        engine = BaseControlEngine()
        runner = _runner(engine)
        try:
            with pytest.raises(WriteRefused, match="state-changing"):
                engine.request("send_command", "osu684", "s", glider="osu684")
        finally:
            runner.close()

    def test_an_unknown_operation_is_refused(self) -> None:
        engine = BaseControlEngine()
        runner = _runner(engine)
        try:
            with pytest.raises(WriteRefused, match="not a client operation"):
                engine.request("summon_kraken", glider="osu684")
        finally:
            runner.close()

    def test_every_read_operation_exists_on_the_real_client(self) -> None:
        """Validated at start so a typo fails then, not on a surfacing."""
        from sfmc_api import SFMCClient

        missing = [op for op in READ_OPERATIONS if not hasattr(SFMCClient, op)]
        assert missing == []

    def test_no_write_verb_slipped_into_the_read_list(self) -> None:
        forbidden = ("send_", "update_", "delete_", "deploy_", "upload_", "set_", "clear_")
        assert [op for op in READ_OPERATIONS if op.startswith(forbidden)] == []


class TestRequests:
    def test_a_result_comes_back_as_an_event(self) -> None:
        seen: list[Event] = []

        class Asker(BaseControlEngine):
            def on_start(self) -> None:
                self.request("get_glider_details", "osu684", glider="osu684", tag="d")

            def on_event(self, event: Event) -> None:
                seen.append(event)
                if event.source == "result":
                    self.request  # noqa: B018 - touch nothing else
                    runner.stop()

        client = _FakeClient(get_glider_details={"state": "connected"})
        engine = Asker()
        runner = _runner(engine, client)
        runner.run()

        results = [e for e in seen if e.source == "result"]
        assert len(results) == 1
        assert results[0].body == {"state": "connected"}
        assert results[0].tag == "d"
        assert results[0].glider == "osu684"
        assert results[0].request_id == 1

    def test_a_failure_comes_back_as_an_error_event(self) -> None:
        seen: list[Event] = []
        boom = RuntimeError("no such glider")

        class Asker(BaseControlEngine):
            def on_start(self) -> None:
                self.request("get_glider_details", "nope", glider="nope", tag="d")

            def on_event(self, event: Event) -> None:
                seen.append(event)
                runner.stop()

        engine = Asker()
        runner = _runner(engine, _FakeClient(get_glider_details=boom))
        runner.run()

        assert [e.source for e in seen] == ["error"]
        assert seen[0].body is boom
        assert seen[0].tag == "d"

    def test_request_ids_increase(self) -> None:
        engine = BaseControlEngine()
        runner = _runner(engine)
        try:
            ids = [
                engine.request("get_glider_details", "osu684", glider="osu684") for _ in range(3)
            ]
            assert ids == [1, 2, 3]
        finally:
            runner.stop()
            runner.close()

    def test_glider_is_a_keyword_not_inferred_from_args(self) -> None:
        """It names the serialisation key, not an argument.

        get_zmodem_transfers takes a connection id, so inferring the
        glider from args[0] would be quietly wrong exactly where it
        matters.
        """
        engine = BaseControlEngine()
        client = _FakeClient()
        runner = _runner(engine, client)
        try:
            engine.request("get_zmodem_transfers", 4321, glider="osu684", tag="z")
            time.sleep(0.2)
            assert ("get_zmodem_transfers", (4321,)) in client.calls
        finally:
            runner.stop()
            runner.close()


class TestFailurePolicy:
    def test_one_bad_event_does_not_stop_the_engine(self) -> None:
        handled: list[str] = []

        class Fragile(BaseControlEngine):
            def on_event(self, event: Event) -> None:
                if event.source == "dialog" and event.body == "bad":
                    raise ValueError("boom")
                handled.append(str(event.source))

        engine = Fragile()
        runner = _runner(engine)
        merge = runner._merge
        merge.publish("osu684", "dialog", "bad")
        merge.publish("osu684", "dialog", "fine")
        threading.Timer(0.4, runner.stop).start()
        runner.run()

        assert "dialog" in handled, "the engine kept going after a failure"

    def test_repeated_failures_stop_the_engine(self) -> None:
        class Broken(BaseControlEngine):
            def on_event(self, event: Event) -> None:
                raise ValueError("always")

        engine = Broken()
        runner = _runner(engine, max_failures=3)
        for i in range(10):
            runner._merge.publish("osu684", "dialog", i)
        runner.run()

        assert runner.failures >= 3
        assert runner._stop.is_set()

    def test_a_good_event_resets_the_strike_counter(self) -> None:
        class Occasional(BaseControlEngine):
            def on_event(self, event: Event) -> None:
                if event.body == "bad":
                    raise ValueError("boom")

        engine = Occasional()
        runner = _runner(engine, max_failures=3)
        runner._merge.publish("osu684", "dialog", "bad")
        runner._merge.publish("osu684", "dialog", "good")
        threading.Timer(0.4, runner.stop).start()
        runner.run()
        assert runner.failures == 0


class TestReplay:
    def test_replay_needs_no_network(self) -> None:
        """A scientist tests an algorithm without a glider or a server."""
        seen: list[str] = []

        class Reader(BaseControlEngine):
            def on_event(self, event: Event) -> None:
                if event.source == "dialog":
                    seen.append(event.body)

        engine = Reader()
        runner = _runner(engine)
        try:
            runner.replay(
                ["Vehicle Name: osusim\r\n", "GPS Location: 3346.4 N\r\n"],
                glider="osusim",
            )
        finally:
            runner.close()

        assert seen == ["Vehicle Name: osusim", "GPS Location: 3346.4 N"]

    def test_replay_runs_start_and_stop(self) -> None:
        order: list[str] = []

        class Noted(BaseControlEngine):
            def on_start(self) -> None:
                order.append("start")

            def on_event(self, event: Event) -> None:
                order.append("event")

            def on_stop(self) -> None:
                order.append("stop")

        engine = Noted()
        runner = _runner(engine)
        try:
            runner.replay(["one"], glider="osusim")
        finally:
            runner.close()
        assert order == ["start", "event", "stop"]


class TestAcceptanceMonitor:
    """A monitor-equivalent engine on one glider."""

    def test_it_logs_every_line_like_sfmc_monitor_glider(self) -> None:
        class Monitor(BaseControlEngine):
            sources = ("dialog",)

            def __init__(self) -> None:
                super().__init__()
                self.lines: list[str] = []
                self.streams: list[str] = []

            def on_event(self, event: Event) -> None:
                match event.source:
                    case "dialog":
                        self.lines.append(event.body)
                    case "stream":
                        self.streams.append(event.body.state)

        engine = Monitor()
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        session = _FakeSession()
        fleet.add_glider("osusim", session)
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None)

        session.emit_line("Vehicle Name: osusim")
        session.emit_line("Curr Time: Mon Aug 17 03:19:31 2026")
        session.emit_connect = None  # type: ignore[assignment]
        for cb in session._connect:
            cb(False)
        threading.Timer(0.4, runner.stop).start()
        runner.run()

        assert engine.lines == ["Vehicle Name: osusim", "Curr Time: Mon Aug 17 03:19:31 2026"]
        assert engine.streams == ["connected"]


class TestAcceptanceFormation:
    """Two gliders, proving cross-glider state needs no locking."""

    def test_cross_glider_state_needs_no_locking(self) -> None:
        class Formation(BaseControlEngine):
            sources = ("dialog",)

            def __init__(self) -> None:
                super().__init__()
                self.positions: dict[str, str] = {}
                self.threads: set[int] = set()
                self.comparisons: list[tuple[str, str]] = []

            def on_event(self, event: Event) -> None:
                # No lock anywhere in this method, deliberately.
                self.threads.add(threading.get_ident())
                if event.source != "dialog":
                    return
                self.positions[event.glider] = event.body
                if len(self.positions) == 2:
                    a, b = (self.positions[g] for g in sorted(self.positions))
                    self.comparisons.append((a, b))

        engine = Formation()
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        first, second = _FakeSession(), _FakeSession()
        fleet.add_glider("osu684", first)
        fleet.add_glider("osu685", second)
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None)

        # Two independent producer threads, as two sessions would be.
        def feed(session: _FakeSession, label: str) -> None:
            for i in range(25):
                session.emit_line(f"{label}-{i}")

        producers = [
            threading.Thread(target=feed, args=(first, "684")),
            threading.Thread(target=feed, args=(second, "685")),
        ]
        for thread in producers:
            thread.start()
        for thread in producers:
            thread.join(timeout=5)

        threading.Timer(0.6, runner.stop).start()
        runner.run()

        assert len(engine.threads) == 1, "on_event must run on exactly one thread"
        assert set(engine.positions) == {"osu684", "osu685"}
        assert engine.comparisons, "the engine compared across gliders"

    def test_a_glider_may_be_added_and_removed_by_the_engine(self) -> None:
        engine = BaseControlEngine()
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        fleet.add_glider("osu684", _FakeSession())
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=None)
        try:
            assert engine.gliders == ("osu684",)
            fleet.add_glider("osu685", _FakeSession())
            assert set(engine.gliders) == {"osu684", "osu685"}
            engine.remove_glider("osu684")
            assert engine.gliders == ("osu685",)
        finally:
            runner.stop()
            runner.close()


class TestWatchdog:
    def test_a_slow_on_event_is_named(self, caplog: pytest.LogCaptureFixture) -> None:
        """The most common way a novice breaks this, self-diagnosing."""

        class Slow(BaseControlEngine):
            def on_event(self, event: Event) -> None:
                time.sleep(0.5)

        engine = Slow()
        client = _FakeClient()
        fleet = FleetStream(client, sources=("dialog",))
        runner = EngineRunner(engine, client, fleet=fleet, watchdog=0.05)
        runner._merge.publish("osu684", "dialog", "slow one")
        threading.Timer(0.9, runner.stop).start()
        with caplog.at_level("WARNING"):
            runner.run()

        assert any("has been running" in r.message for r in caplog.records)
        assert any("osu684" in str(r.args) or "osu684" in r.getMessage() for r in caplog.records)


class TestGuarantees:
    def test_order_within_a_pair_is_preserved_through_the_engine(self) -> None:
        seen: list[Any] = []

        class Recorder(BaseControlEngine):
            def on_event(self, event: Event) -> None:
                if event.source == "dialog":
                    seen.append(event.body)

        engine = Recorder()
        runner = _runner(engine)
        for i in range(40):
            runner._merge.publish("osu684", "dialog", i)
        threading.Timer(0.5, runner.stop).start()
        runner.run()
        assert seen == sorted(seen)

    def test_on_stop_runs_even_when_the_engine_is_stopped_early(self) -> None:
        stopped: list[bool] = []

        class Noted(BaseControlEngine):
            def on_stop(self) -> None:
                stopped.append(True)

        engine = Noted()
        runner = _runner(engine)
        threading.Timer(0.1, runner.stop).start()
        runner.run()
        assert stopped == [True]


class TestReplayNeedsNothing:
    """ "Without a glider, a server, or a network" has to be literally true."""

    def test_a_runner_can_be_built_with_no_client_at_all(self) -> None:
        seen: list[str] = []

        class Reader(BaseControlEngine):
            def on_event(self, event: Event) -> None:
                seen.append(event.body)

        runner = EngineRunner(Reader(), client=None, watchdog=None)
        try:
            runner.replay(["Vehicle Name: osusim"], glider="osusim")
        finally:
            runner.close()
        assert seen == ["Vehicle Name: osusim"]

    def test_requesting_without_a_client_says_why(self) -> None:
        engine = BaseControlEngine()
        runner = EngineRunner(engine, client=None, watchdog=None)
        try:
            with pytest.raises(RuntimeError, match="replay-only"):
                engine.request("get_glider_details", "osusim", glider="osusim")
        finally:
            runner.close()

    def test_a_replay_only_runner_has_no_gliders_and_says_so(self) -> None:
        engine = BaseControlEngine()
        runner = EngineRunner(engine, client=None, watchdog=None)
        try:
            assert engine.gliders == ()
            with pytest.raises(RuntimeError, match="replay-only"):
                engine.add_glider("osusim")
        finally:
            runner.close()
