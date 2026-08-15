"""Tests for the generic operation executor."""

from __future__ import annotations

import threading
import time

import pytest

from sfmc_api.exceptions import APIError
from sfmc_api.ops import KeyedLock, OperationExecutor, OperationResult


class TestKeyedLock:
    def test_same_key_serializes(self) -> None:
        locks = KeyedLock()
        overlapping: list[bool] = []
        probe = threading.Lock()

        def work() -> None:
            with locks.hold("osu685"):
                acquired = probe.acquire(blocking=False)
                overlapping.append(acquired)
                time.sleep(0.02)
                if acquired:
                    probe.release()

        threads = [threading.Thread(target=work) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
        assert all(overlapping)

    def test_different_keys_run_concurrently(self) -> None:
        locks = KeyedLock()
        assert locks.get("a") is not locks.get("b")
        assert locks.get("a") is locks.get("a")

    def test_timeout_raises(self) -> None:
        locks = KeyedLock()
        locks.get("busy").acquire()
        try:
            with pytest.raises(TimeoutError, match="busy"), locks.hold("busy", timeout=0.01):
                pass
        finally:
            locks.get("busy").release()


class TestSubmit:
    def test_runs_any_callable_and_returns_a_future(self) -> None:
        with OperationExecutor(max_workers=2) as ops:
            future = ops.submit(lambda a, b: a + b, 2, 3)
            assert future.result(timeout=5) == 5

    def test_exceptions_surface_from_the_future(self) -> None:
        with OperationExecutor(max_workers=1) as ops:

            def boom() -> None:
                raise APIError(500, "nope")

            future = ops.submit(boom)
            with pytest.raises(APIError):
                future.result(timeout=5)

    def test_keyword_arguments_are_passed_through(self) -> None:
        with OperationExecutor(max_workers=1) as ops:
            future = ops.submit(lambda *, name: f"hi {name}", name="osu685")
            assert future.result(timeout=5) == "hi osu685"

    def test_awaitable_from_asyncio(self) -> None:
        import asyncio

        with OperationExecutor(max_workers=1) as ops:

            async def main() -> int:
                return await asyncio.wrap_future(ops.submit(lambda: 7))

            assert asyncio.run(main()) == 7

    def test_rejects_zero_workers(self) -> None:
        with pytest.raises(ValueError, match="max_workers"):
            OperationExecutor(max_workers=0)


class TestSerialization:
    def test_serialized_operations_never_overlap(self) -> None:
        """Two plan updates on one glider racing is a real hazard."""
        order: list[str] = []
        probe = threading.Lock()
        overlapping: list[bool] = []

        def work(tag: str) -> str:
            acquired = probe.acquire(blocking=False)
            overlapping.append(acquired)
            time.sleep(0.02)
            order.append(tag)
            if acquired:
                probe.release()
            return tag

        with OperationExecutor(max_workers=4) as ops:
            futures = [ops.serialized("osu685", work, f"op{i}") for i in range(4)]
            results = [f.result(timeout=10) for f in futures]

        assert all(overlapping)
        assert sorted(results) == ["op0", "op1", "op2", "op3"]

    def test_different_gliders_are_not_serialized_against_each_other(self) -> None:
        started = threading.Event()
        released = threading.Event()

        def blocker() -> str:
            started.set()
            released.wait(timeout=5)
            return "blocked"

        with OperationExecutor(max_workers=2) as ops:
            slow = ops.serialized("osu684", blocker)
            assert started.wait(timeout=5)
            fast = ops.serialized("osu685", lambda: "free")
            # The second glider's work completes while the first is held.
            assert fast.result(timeout=5) == "free"
            released.set()
            assert slow.result(timeout=5) == "blocked"

    def test_sequence_runs_in_order(self) -> None:
        order: list[str] = []

        def step(tag: str) -> str:
            order.append(tag)
            return tag

        with OperationExecutor(max_workers=4) as ops:
            future = ops.sequence(
                "osu685",
                (step, "upload"),
                (step, "deploy"),
            )
            assert future.result(timeout=10) == ["upload", "deploy"]
        assert order == ["upload", "deploy"]

    def test_sequence_stops_at_the_first_failure(self) -> None:
        ran: list[str] = []

        def ok() -> str:
            ran.append("ok")
            return "ok"

        def fail() -> str:
            raise APIError(500, "nope")

        def never() -> str:  # pragma: no cover - must not run
            ran.append("never")
            return "never"

        with OperationExecutor(max_workers=2) as ops:
            future = ops.sequence("osu685", (ok,), (fail,), (never,))
            with pytest.raises(APIError):
                future.result(timeout=10)
        assert ran == ["ok"]

    def test_empty_sequence_is_rejected(self) -> None:
        with (
            OperationExecutor(max_workers=1) as ops,
            pytest.raises(ValueError, match="at least one call"),
        ):
            ops.sequence("osu685")


class TestMap:
    def test_fans_one_operation_across_items(self) -> None:
        with OperationExecutor(max_workers=3) as ops:
            futures = ops.map(lambda name: f"details:{name}", ["osu684", "osu685"])
            assert [f.result(timeout=5) for f in futures] == [
                "details:osu684",
                "details:osu685",
            ]


class TestObservers:
    def test_observer_sees_success_and_failure(self) -> None:
        seen: list[OperationResult] = []

        def boom() -> None:
            raise APIError(500, "nope")

        with OperationExecutor(max_workers=1) as ops:
            ops.on_result(seen.append)
            ops.submit(lambda: "fine").result(timeout=5)
            with pytest.raises(APIError):
                ops.submit(boom).result(timeout=5)

        assert [r.ok for r in seen] == [True, False]
        assert seen[0].value == "fine"
        assert isinstance(seen[1].error, APIError)
        assert all(r.elapsed >= 0 for r in seen)

    def test_failing_observer_does_not_fail_the_operation(self) -> None:
        def bad_observer(_: OperationResult) -> None:
            raise RuntimeError("observer bug")

        with OperationExecutor(max_workers=1) as ops:
            ops.on_result(bad_observer)
            assert ops.submit(lambda: 1).result(timeout=5) == 1


class TestLifecycle:
    def test_shutdown_waits_for_running_work(self) -> None:
        done: list[str] = []
        ops = OperationExecutor(max_workers=1)
        ops.submit(lambda: (time.sleep(0.05), done.append("finished")))
        ops.shutdown()
        assert done == ["finished"]

    def test_context_manager_shuts_down(self) -> None:
        with OperationExecutor(max_workers=1) as ops:
            assert ops.submit(lambda: 1).result(timeout=5) == 1
        with pytest.raises(RuntimeError):
            ops.submit(lambda: 2)


class TestClientIntegration:
    def test_client_operations_runs_client_methods(self) -> None:
        from unittest.mock import MagicMock

        from sfmc_api import SFMCClient
        from sfmc_api.config import SFMCConfig

        client = SFMCClient(
            config=SFMCConfig(host="h", client_id="c", secret="s", tls_verify=False)
        )
        client.get_glider_details = MagicMock(  # type: ignore[method-assign]
            return_value={"data": {"id": 8}}
        )
        with client.operations(max_workers=2) as ops:
            future = ops.submit(client.get_glider_details, "osu685")
            assert future.result(timeout=5) == {"data": {"id": 8}}

        # No per-endpoint wrapper was needed for this to work.
        client.get_glider_details.assert_called_once_with("osu685")

    def test_operation_name_is_recorded_for_observers(self) -> None:
        from sfmc_api import SFMCClient
        from sfmc_api.config import SFMCConfig

        client = SFMCClient(
            config=SFMCConfig(host="h", client_id="c", secret="s", tls_verify=False)
        )
        seen: list[str] = []
        with client.operations(max_workers=1) as ops:
            ops.on_result(lambda r: seen.append(r.name))
            ops.submit(_named_operation).result(timeout=5)
        assert seen == ["_named_operation"]


def _named_operation() -> str:
    return "ok"
