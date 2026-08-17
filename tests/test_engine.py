"""Tests for BaseControlEngine and its runner (phase 2, read-only)."""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from sfmc_api.engine import (
    READ_OPERATIONS,
    WRITE_OPERATIONS,
    BaseControlEngine,
    DryRun,
    EngineRunner,
    RateLimited,
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
    """Records every call; never does anything.

    Implements both operation lists, so "a blocked write never reached
    the client" is asserted by inspecting ``calls`` rather than by an
    exploding stub — which would also fire on the legitimate
    ``allow_writes`` path.
    """

    def __init__(self, **returns: Any) -> None:
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._returns = returns
        for op in READ_OPERATIONS | WRITE_OPERATIONS:
            setattr(self, op, self._make(op))

    def _make(self, op: str) -> Any:
        def call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append((op, args))
            value = self._returns.get(op, {"op": op, "args": args})
            if isinstance(value, Exception):
                raise value
            return value

        return call


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
    def test_a_write_is_refused_as_an_event_not_an_exception(self) -> None:
        """Rail 2: a blocked write yields an error event naming the flag.

        An event rather than a raise so an engine handles a blocked
        write exactly like any other failed operation, instead of
        wrapping every request in a try block.
        """
        engine = BaseControlEngine()
        runner = _runner(engine)
        try:
            request_id = engine.request("send_command", "osu684", "s", glider="osu684")
            event = runner._merge.get(timeout=1)
            assert event is not None
            assert event.source == "error"
            assert isinstance(event.body, WriteRefused)
            assert "allow_writes" in str(event.body)
            assert event.request_id == request_id
        finally:
            runner.close()

    def test_an_unknown_operation_raises_because_it_is_a_bug(self) -> None:
        """Not an operational condition -- a typo in the engine's code."""
        engine = BaseControlEngine()
        runner = _runner(engine)
        try:
            with pytest.raises(ValueError, match="not a requestable operation"):
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


class TestWriteGating:
    """Rail 2: writes off by default, matching sfmc-api-test's posture."""

    def test_writes_are_off_by_default(self) -> None:
        runner = _runner(BaseControlEngine())
        try:
            assert runner.allow_writes is False
            assert runner.dry_run is False
        finally:
            runner.close()

    def test_an_allowed_write_actually_runs(self) -> None:
        engine = BaseControlEngine()
        client = _FakeClient()
        runner = _runner(engine, client, allow_writes=True)
        try:
            engine.request("send_command", "osu684", "Ctrl-M", glider="osu684", tag="c")
            event = runner._merge.get(timeout=2)
            assert event is not None and event.source == "result"
            assert ("send_command", ("osu684", "Ctrl-M")) in client.calls
        finally:
            runner.stop()
            runner.close()

    def test_a_blocked_write_never_reaches_the_client(self) -> None:
        engine = BaseControlEngine()
        client = _FakeClient()
        runner = _runner(engine, client)
        try:
            engine.request("send_command", "osu684", "Ctrl-C", glider="osu684")
            time.sleep(0.2)
            assert client.calls == [], "a blocked write must not touch the glider"
        finally:
            runner.close()

    def test_reads_still_work_when_writes_are_blocked(self) -> None:
        engine = BaseControlEngine()
        client = _FakeClient()
        runner = _runner(engine, client)
        try:
            engine.request("get_glider_details", "osu684", glider="osu684")
            event = runner._merge.get(timeout=2)
            assert event is not None and event.source == "result"
        finally:
            runner.stop()
            runner.close()

    def test_the_two_operation_lists_do_not_overlap(self) -> None:
        assert frozenset() == READ_OPERATIONS & WRITE_OPERATIONS

    def test_every_write_operation_exists_on_the_real_client(self) -> None:
        from sfmc_api import SFMCClient

        assert [op for op in WRITE_OPERATIONS if not hasattr(SFMCClient, op)] == []

    def test_every_dangerous_verb_is_classified_as_a_write(self) -> None:
        """Nothing that mutates may sit outside the gate."""
        from sfmc_api import SFMCClient

        dangerous = ("send_", "update_", "delete_", "deploy_", "upload_", "set_", "clear_assigned")
        for name in dir(SFMCClient):
            if name.startswith(dangerous):
                assert name in WRITE_OPERATIONS, f"{name} is ungated"


class TestDryRun:
    """Rail 1: full logic, synthetic results, nothing sent."""

    def test_a_dry_run_write_is_answered_but_not_sent(self) -> None:
        engine = BaseControlEngine()
        client = _FakeClient()
        runner = _runner(engine, client, allow_writes=True, dry_run=True)
        try:
            request_id = engine.request(
                "send_command", "osu684", "Ctrl-C", glider="osu684", tag="c"
            )
            event = runner._merge.get(timeout=2)
            assert event is not None
            assert event.source == "result", "the engine's logic still gets an answer"
            assert isinstance(event.body, DryRun)
            assert event.body.op == "send_command"
            assert event.body.args == ("osu684", "Ctrl-C")
            assert event.request_id == request_id
            assert event.tag == "c"
            assert client.calls == [], "nothing was sent"
        finally:
            runner.close()

    def test_a_dry_run_still_performs_reads(self) -> None:
        """Only the consequences are withheld, not the observations."""
        engine = BaseControlEngine()
        client = _FakeClient()
        runner = _runner(engine, client, allow_writes=True, dry_run=True)
        try:
            engine.request("get_glider_details", "osu684", glider="osu684")
            event = runner._merge.get(timeout=2)
            assert event is not None and event.source == "result"
            assert not isinstance(event.body, DryRun)
            assert ("get_glider_details", ("osu684",)) in client.calls
        finally:
            runner.stop()
            runner.close()

    def test_dry_run_result_is_a_distinct_type(self) -> None:
        """An engine treating it as server data should be able to notice."""
        assert not isinstance(DryRun(op="x", args=(), kwargs={}), dict)


class TestRateLimit:
    """Rail 4: exceeding the cap errors rather than queueing silently."""

    def test_exceeding_the_cap_produces_an_error_not_a_queue(self) -> None:
        engine = BaseControlEngine()
        blocker = threading.Event()

        class SlowClient(_FakeClient):
            def _make(self, op: str) -> Any:
                def call(*args: Any, **kwargs: Any) -> Any:
                    self.calls.append((op, args))
                    blocker.wait(timeout=5)
                    return {}

                return call

        client = SlowClient()
        runner = _runner(engine, client, max_outstanding=2)
        try:
            ids = [engine.request("get_glider_details", f"g{i}", glider=f"g{i}") for i in range(5)]
            assert len(ids) == 5, "every request gets an id, even a refused one"
            errors = []
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline and len(errors) < 3:
                event = runner._merge.get(timeout=0.2)
                if event is not None and event.source == "error":
                    errors.append(event)
            assert len(errors) == 3, "3 of 5 exceeded a cap of 2"
            assert all(isinstance(e.body, RateLimited) for e in errors)
            assert "not submitted" in str(errors[0].body)
        finally:
            blocker.set()
            runner.stop()
            runner.close()

    def test_outstanding_returns_to_zero(self) -> None:
        engine = BaseControlEngine()
        runner = _runner(engine, _FakeClient())
        try:
            engine.request("get_glider_details", "osu684", glider="osu684")
            deadline = time.monotonic() + 2
            while runner.outstanding and time.monotonic() < deadline:
                time.sleep(0.02)
            assert runner.outstanding == 0
        finally:
            runner.stop()
            runner.close()

    def test_a_refused_request_does_not_consume_a_slot(self) -> None:
        engine = BaseControlEngine()
        runner = _runner(engine, _FakeClient(), max_outstanding=1)
        try:
            for _ in range(5):
                engine.request("send_command", "osu684", "x", glider="osu684")
            time.sleep(0.1)
            assert runner.outstanding == 0
        finally:
            runner.stop()
            runner.close()


class TestAudit:
    """Rail 5: the artefact that explains a surprising glider."""

    def test_every_request_and_outcome_is_logged_with_its_glider(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        engine = BaseControlEngine()
        with caplog.at_level("INFO", logger="sfmc_api.engine.audit"):
            runner = _runner(engine, _FakeClient())
            try:
                engine.request("get_glider_details", "osu684", glider="osu684", tag="d")
                deadline = time.monotonic() + 2
                while runner.outstanding and time.monotonic() < deadline:
                    time.sleep(0.02)
            finally:
                runner.stop()
                runner.close()

        lines = [r.getMessage() for r in caplog.records]
        assert any("requested" in line and "osu684" in line for line in lines)
        assert any("ok" in line and "get_glider_details" in line for line in lines)
        assert any("tag=d" in line for line in lines)

    def test_the_run_posture_is_stated_at_startup(self, caplog: pytest.LogCaptureFixture) -> None:
        """ "Was this run allowed to touch the glider?" from the log alone."""
        with caplog.at_level("INFO", logger="sfmc_api.engine.audit"):
            runner = _runner(BaseControlEngine(), _FakeClient(), allow_writes=True)
            runner.close()
        assert any("writes=ALLOWED" in r.getMessage() for r in caplog.records)

    def test_a_refusal_is_audited(self, caplog: pytest.LogCaptureFixture) -> None:
        engine = BaseControlEngine()
        with caplog.at_level("INFO", logger="sfmc_api.engine.audit"):
            runner = _runner(engine, _FakeClient())
            try:
                engine.request("send_command", "osu684", "x", glider="osu684")
            finally:
                runner.close()
        assert any("refused: WriteRefused" in r.getMessage() for r in caplog.records)

    def test_long_arguments_are_truncated(self) -> None:
        """An upload's arguments can be a whole file."""
        from sfmc_api.engine import _summarise

        assert len(_summarise(("x" * 500,))) <= 81
