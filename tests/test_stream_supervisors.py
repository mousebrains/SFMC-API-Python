"""Fault-oriented tests for the live stream reconnect supervisors."""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from queue import Queue
from typing import Any, ClassVar
from unittest.mock import MagicMock, patch

import pytest

from sfmc_api import SFMCClient
from sfmc_api.dialog_parser import SurfacingEvent
from sfmc_api.disconnect_notify import DisconnectNotifier
from sfmc_api.exceptions import RateLimitError
from sfmc_api.follow_glider import (
    RunStats,
    _open_replay,
    _read_dialog,
    _RecentSurfacingIds,
    follow_glider,
)
from sfmc_api.follower import BaseFollower
from sfmc_api.monitor_glider import STREAM_BOUNDARY_PREFIX, monitor_glider
from sfmc_api.stomp import StompError, StompSubscription

SAMPLE_DIALOG_MESSAGES: list[dict[str, Any]] = [
    {"sequenceNumber": 0, "data": "Connection Event: Carrier Detect found.123\r\n"},
    {"sequenceNumber": 1, "data": "Vehicle Name: testbot\r\n"},
    {
        "sequenceNumber": 2,
        "data": "Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339\r\n",
    },
    {
        "sequenceNumber": 3,
        "data": "GPS Location:  3310.021 N -11741.800 E measured  64.746 secs ago\r\n",
    },
    {
        "sequenceNumber": 4,
        "data": "   sensor:m_battery(volts)=15.0  50.0 secs ago\r\n",
    },
    {
        "sequenceNumber": 5,
        "data": "   sensor:m_water_vx(m/s)=0.04  68.0 secs ago\r\n",
    },
    {"sequenceNumber": 6, "data": "ABORT HISTORY: total since reset: 1\r\n"},
]


def _subscription(
    messages: list[dict[str, Any]],
    *,
    terminal: StompError | None = None,
    blocking: bool = False,
) -> StompSubscription:
    queue: Queue[dict[str, Any] | StompError | None] = Queue()
    for message in messages:
        queue.put(message)
    if terminal is not None:
        queue.put(terminal)
    elif not blocking:
        queue.put(None)
    return StompSubscription("test", "/topic/test", queue)


def _dialog_messages(*, second: int, mission_time: int) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for original in SAMPLE_DIALOG_MESSAGES:
        message = dict(original)
        if "Curr Time:" in message["data"]:
            message["data"] = message["data"].replace(
                "20:40:38 2026 MT:  169339",
                f"20:40:{second:02d} 2026 MT:  {mission_time}",
            )
        messages.append(message)
    return messages


def _client() -> MagicMock:
    client = MagicMock(spec=SFMCClient)
    client.get_glider_details.return_value = {
        "data": {"state": "deployed", "id": 42},
    }
    client.get_active_deployment_details.return_value = {
        "data": {"currentScriptName": None},
    }
    client.open_stream.return_value.__enter__.return_value = MagicMock()
    client.open_stream.return_value.__exit__.return_value = False
    return client


def _mock_logs() -> tuple[MagicMock, MagicMock, MagicMock]:
    dialog = MagicMock(spec=logging.Logger)
    dialog.name = "test.dialog"
    dialog.makeRecord.return_value = MagicMock()
    output = MagicMock(spec=logging.Logger)
    output.name = "test.output"
    info = MagicMock(spec=logging.Logger)
    info.name = "test.info"
    return dialog, output, info


def _stream_context() -> MagicMock:
    context = MagicMock()
    context.__enter__.return_value = MagicMock()
    context.__exit__.return_value = False
    return context


class PersistentFollower(BaseFollower):
    """Follower whose class state exposes instance and delivery counts."""

    instances: ClassVar[list[PersistentFollower]] = []
    received: ClassVar[list[SurfacingEvent]] = []
    stop_after: ClassVar[int | None] = None
    stop: ClassVar[threading.Event | None] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        type(self).instances.append(self)

    def on_surfacing(self, event: SurfacingEvent) -> None:
        type(self).received.append(event)
        self.send_files(to_glider={f"event-{len(type(self).received)}.ma": "content"})
        terminal_stop = type(self).stop
        stop_after = type(self).stop_after
        if (
            terminal_stop is not None
            and stop_after is not None
            and len(type(self).received) >= stop_after
        ):
            terminal_stop.set()


def _reset_follower(stop: threading.Event, *, stop_after: int | None) -> None:
    PersistentFollower.instances = []
    PersistentFollower.received = []
    PersistentFollower.stop = stop
    PersistentFollower.stop_after = stop_after


@patch("sfmc_api.follow_glider.setup_logging")
def test_follow_reconnect_keeps_one_pipeline(mock_setup_logging: MagicMock) -> None:
    dialog_log, output_log, info_log = _mock_logs()
    mock_setup_logging.return_value = (dialog_log, output_log, info_log)
    stop = threading.Event()
    _reset_follower(stop, stop_after=3)
    client = _client()
    client.subscribe_glider_output.side_effect = [
        _subscription(_dialog_messages(second=38, mission_time=169339)),
        _subscription(_dialog_messages(second=39, mission_time=169340)),
        _subscription(
            _dialog_messages(second=40, mission_time=169341),
            blocking=True,
        ),
    ]

    stats = follow_glider(
        client=client,
        glider_name="testbot",
        follower_class=PersistentFollower,
        dry_run=True,
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
    )

    assert len(PersistentFollower.instances) == 1
    assert len(PersistentFollower.received) == 3
    assert stats.reconnects == 2
    assert stats.surfacings == 3
    assert client.open_stream.call_count == 3
    assert client.refresh_auth.call_count == 2


@patch("sfmc_api.follow_glider.setup_logging")
def test_follow_recovers_initial_session_setup_failure(
    mock_setup_logging: MagicMock,
) -> None:
    mock_setup_logging.return_value = _mock_logs()
    stop = threading.Event()
    _reset_follower(stop, stop_after=1)
    client = _client()
    client.open_stream.side_effect = [StompError("handshake failed"), _stream_context()]
    client.subscribe_glider_output.return_value = _subscription(
        _dialog_messages(second=38, mission_time=169339),
        blocking=True,
    )

    stats = follow_glider(
        client=client,
        glider_name="testbot",
        follower_class=PersistentFollower,
        dry_run=True,
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
    )

    assert stats.reconnects == 1
    assert client.open_stream.call_count == 2
    client.refresh_auth.assert_called_once_with()


@patch("sfmc_api.follow_glider.setup_logging")
def test_follow_suppresses_strong_duplicate_after_reconnect(
    mock_setup_logging: MagicMock,
) -> None:
    dialog_log, output_log, info_log = _mock_logs()
    mock_setup_logging.return_value = (dialog_log, output_log, info_log)
    stop = threading.Event()
    _reset_follower(stop, stop_after=None)

    def stop_on_duplicate(message: str, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        if message.startswith("duplicate surfacing suppressed"):
            stop.set()

    info_log.warning.side_effect = stop_on_duplicate
    client = _client()
    client.subscribe_glider_output.side_effect = [
        _subscription(_dialog_messages(second=38, mission_time=169339)),
        _subscription(
            _dialog_messages(second=38, mission_time=169339),
            blocking=True,
        ),
    ]

    stats = follow_glider(
        client=client,
        glider_name="testbot",
        follower_class=PersistentFollower,
        dry_run=True,
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
    )

    assert len(PersistentFollower.received) == 1
    assert stats.surfacings == 1
    assert stats.reconnects == 1
    assert any(
        call.args[0].startswith("duplicate surfacing suppressed")
        for call in info_log.warning.call_args_list
    )


@patch("sfmc_api.follow_glider.setup_logging")
def test_follow_no_reconnect_raises_after_ordered_drain(
    mock_setup_logging: MagicMock,
) -> None:
    dialog_log, output_log, info_log = _mock_logs()
    mock_setup_logging.return_value = (dialog_log, output_log, info_log)
    stop = threading.Event()
    _reset_follower(stop, stop_after=None)
    client = _client()
    client.subscribe_glider_output.return_value = _subscription(
        _dialog_messages(second=38, mission_time=169339)
    )

    with pytest.raises(StompError, match="stream session ended"):
        follow_glider(
            client=client,
            glider_name="testbot",
            follower_class=PersistentFollower,
            dry_run=True,
            stop=stop,
            reconnect=False,
        )

    assert len(PersistentFollower.instances) == 1
    assert not PersistentFollower.instances[0].is_alive()
    assert len(PersistentFollower.received) == 1
    assert client.open_stream.call_count == 1


@patch("sfmc_api.follow_glider._print_files", side_effect=RuntimeError("printer failed"))
@patch("sfmc_api.follow_glider.setup_logging")
def test_follow_output_worker_failure_remains_observable_during_cleanup(
    mock_setup_logging: MagicMock,
    mock_print: MagicMock,
) -> None:
    del mock_print
    mock_setup_logging.return_value = _mock_logs()
    stop = threading.Event()
    _reset_follower(stop, stop_after=None)
    client = _client()
    client.subscribe_glider_output.return_value = _subscription([], blocking=True)

    with pytest.raises(RuntimeError, match="printer failed") as exc_info:
        follow_glider(
            client=client,
            glider_name="testbot",
            follower_class=PersistentFollower,
            dry_run=True,
            stop=stop,
        )

    assert "without reporting" not in str(exc_info.value)


@patch("sfmc_api.follow_glider.setup_logging")
def test_follow_unexpected_dialog_processing_error_is_fatal(
    mock_setup_logging: MagicMock,
) -> None:
    mock_setup_logging.return_value = _mock_logs()
    stop = threading.Event()
    _reset_follower(stop, stop_after=None)
    client = _client()
    client.subscribe_glider_output.return_value = _subscription(
        [{"sequenceNumber": 0, "data": "line\n"}]
    )

    # Malformed server data is skipped (not fatal), so a genuine code
    # bug is injected at the dialog-processing boundary to verify the
    # fatal-by-design policy for non-StompError worker failures.
    with (
        patch(
            "sfmc_api.follow_glider.ordered_dialog",
            side_effect=RuntimeError("injected bug"),
        ),
        pytest.raises(RuntimeError, match="dialog reader failed"),
    ):
        follow_glider(
            client=client,
            glider_name="testbot",
            follower_class=PersistentFollower,
            dry_run=True,
            stop=stop,
            reconnect_initial_delay=0.0,
            reconnect_max_delay=0.0,
            reconnect_jitter=0.0,
        )

    assert client.open_stream.call_count == 1
    client.refresh_auth.assert_not_called()


def test_replay_boundary_flushes_and_resets_parser(tmp_path: Path) -> None:
    replay = tmp_path / "boundary.log"
    pre_boundary = [message["data"].rstrip("\r\n") for message in SAMPLE_DIALOG_MESSAGES[:-1]]
    replay.write_text(
        "\n".join(
            [
                *pre_boundary,
                (
                    "2026-03-28T20:40:39.000000 sfmc.test.FOLLOW  "
                    f"{STREAM_BOUNDARY_PREFIX} session=1 reason=closed"
                ),
                "   sensor:m_vacuum(inHg)=7.8  2.0 secs ago",
                "ABORT HISTORY: post-boundary data must stay separate",
            ]
        )
        + "\n"
    )
    stop = threading.Event()
    sub, file_reader = _open_replay(replay, stop)
    queue_in: Queue[SurfacingEvent | None] = Queue()
    stats = RunStats()

    from sfmc_api.dialog_parser import DialogParser

    _read_dialog(
        sub,
        DialogParser(),
        queue_in,
        None,
        stop,
        stats=stats,
        recent_ids=_RecentSurfacingIds(),
    )
    file_reader.join(timeout=2)

    event = queue_in.get_nowait()
    assert event is not None
    assert queue_in.empty()
    assert stats.surfacings == 1
    assert all(STREAM_BOUNDARY_PREFIX not in line for line in event.raw_lines)
    assert "m_vacuum" not in event.sensors


class _CollectingHandler(logging.Handler):
    def __init__(self, stop_on: str | None = None, stop: threading.Event | None = None) -> None:
        super().__init__()
        self.messages: list[str] = []
        self.stop_on = stop_on
        self.stop = stop

    def emit(self, record: logging.LogRecord) -> None:
        message = record.getMessage()
        self.messages.append(message)
        if self.stop is not None and message == self.stop_on:
            self.stop.set()


def test_monitor_reconnects_both_subscriptions_and_resyncs() -> None:
    stop = threading.Event()
    handler = _CollectingHandler(stop_on="third", stop=stop)
    dialog_log = logging.getLogger("test.monitor.reconnect.dialog")
    dialog_log.handlers.clear()
    dialog_log.propagate = False
    dialog_log.setLevel(logging.INFO)
    dialog_log.addHandler(handler)
    script_log = MagicMock(spec=logging.Logger)
    info_log = MagicMock(spec=logging.Logger)
    client = _client()
    client.subscribe_glider_output.side_effect = [
        _subscription([{"sequenceNumber": 0, "data": "first\r\n"}]),
        _subscription([{"sequenceNumber": 0, "data": "second\r\n"}]),
        _subscription(
            [{"sequenceNumber": 0, "data": "third\r\n"}],
            blocking=True,
        ),
    ]
    client.subscribe_script_events.side_effect = [
        _subscription([], blocking=True),
        _subscription([], blocking=True),
        _subscription([], blocking=True),
    ]

    monitor_glider(
        client,
        "testbot",
        dialog_log,
        script_log,
        info_log,
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
    )

    assert handler.messages == ["first", "second", "third"]
    assert client.open_stream.call_count == 3
    assert client.refresh_auth.call_count == 2
    assert client.get_active_deployment_details.call_count == 3
    assert any(
        call.args[:2] == ("%s session=%d reason=%s", STREAM_BOUNDARY_PREFIX)
        for call in info_log.warning.call_args_list
    )


def test_monitor_recovers_initial_session_setup_failure() -> None:
    stop = threading.Event()
    handler = _CollectingHandler(stop_on="connected", stop=stop)
    dialog_log = logging.getLogger("test.monitor.setup-retry.dialog")
    dialog_log.handlers.clear()
    dialog_log.propagate = False
    dialog_log.setLevel(logging.INFO)
    dialog_log.addHandler(handler)
    info_log = MagicMock(spec=logging.Logger)
    client = _client()
    client.get_active_deployment_details.side_effect = [
        {"data": {"currentScriptName": None}},
        {"data": {"currentScriptName": "incomplete"}},
    ]
    client.open_stream.side_effect = [StompError("handshake failed"), _stream_context()]
    client.subscribe_glider_output.return_value = _subscription(
        [{"sequenceNumber": 0, "data": "connected\r\n"}],
        blocking=True,
    )
    client.subscribe_script_events.return_value = _subscription([], blocking=True)

    monitor_glider(
        client,
        "testbot",
        dialog_log,
        MagicMock(spec=logging.Logger),
        info_log,
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
    )

    assert client.open_stream.call_count == 2
    client.refresh_auth.assert_called_once_with()
    assert any(
        call.args[0] == "stream session %d reconnected after %.1fs offline"
        for call in info_log.info.call_args_list
    )
    assert any(
        call.args[0] == "script resync failed: %s" for call in info_log.warning.call_args_list
    )


def test_monitor_stop_interrupts_reconnect_wait() -> None:
    stop = threading.Event()
    client = _client()
    client.subscribe_glider_output.return_value = _subscription([])
    client.subscribe_script_events.return_value = _subscription([], blocking=True)
    info_log = MagicMock(spec=logging.Logger)

    def stop_before_wait(message: str, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        if message.startswith("reconnect attempt"):
            stop.set()

    info_log.info.side_effect = stop_before_wait
    started = time.monotonic()
    monitor_glider(
        client,
        "testbot",
        MagicMock(spec=logging.Logger),
        MagicMock(spec=logging.Logger),
        info_log,
        stop=stop,
        reconnect_initial_delay=300.0,
        reconnect_max_delay=300.0,
        reconnect_jitter=0.0,
    )

    assert time.monotonic() - started < 1.0
    assert client.open_stream.call_count == 1


def test_monitor_unexpected_worker_error_is_fatal() -> None:
    client = _client()
    client.subscribe_glider_output.return_value = _subscription([], blocking=True)
    client.subscribe_script_events.return_value = _subscription([{"scriptName": "s"}])

    # Malformed server data is skipped (not fatal), so a genuine code
    # bug is injected into the script worker to verify the
    # fatal-by-design policy for non-StompError worker failures.
    with (
        patch(
            "sfmc_api.monitor_glider.monitor_scripts",
            side_effect=RuntimeError("injected bug"),
        ),
        pytest.raises(RuntimeError, match="script monitor failed"),
    ):
        monitor_glider(
            client,
            "testbot",
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            reconnect_initial_delay=0.0,
            reconnect_max_delay=0.0,
            reconnect_jitter=0.0,
        )

    assert client.open_stream.call_count == 1
    client.refresh_auth.assert_not_called()


def test_monitor_worker_join_timeout_prevents_overlapping_session() -> None:
    release = threading.Event()

    def stuck_dialog(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        release.wait(timeout=2)

    client = _client()
    client.subscribe_glider_output.return_value = _subscription([], blocking=True)
    client.subscribe_script_events.return_value = _subscription([])
    try:
        with (
            patch("sfmc_api.monitor_glider.monitor_dialog", side_effect=stuck_dialog),
            pytest.raises(RuntimeError, match="monitor worker did not stop"),
        ):
            monitor_glider(
                client,
                "testbot",
                MagicMock(spec=logging.Logger),
                MagicMock(spec=logging.Logger),
                MagicMock(spec=logging.Logger),
                reconnect_initial_delay=0.0,
                reconnect_max_delay=0.0,
                reconnect_jitter=0.0,
                worker_join_timeout=0.01,
            )
    finally:
        release.set()

    assert client.open_stream.call_count == 1


# ── Disconnect notifier wiring ───────────────────────────────────────


def test_monitor_drives_notifier_across_sessions() -> None:
    """The monitor tells the notifier each subscribe and each drop."""
    stop = threading.Event()
    handler = _CollectingHandler(stop_on="third", stop=stop)
    dialog_log = logging.getLogger("test.monitor.notifier.dialog")
    dialog_log.handlers.clear()
    dialog_log.propagate = False
    dialog_log.setLevel(logging.INFO)
    dialog_log.addHandler(handler)
    notifier = MagicMock(spec=DisconnectNotifier)
    client = _client()
    client.subscribe_glider_output.side_effect = [
        _subscription([{"sequenceNumber": 0, "data": "first\r\n"}]),
        _subscription([{"sequenceNumber": 0, "data": "second\r\n"}]),
        _subscription([{"sequenceNumber": 0, "data": "third\r\n"}], blocking=True),
    ]
    client.subscribe_script_events.side_effect = [
        _subscription([], blocking=True),
        _subscription([], blocking=True),
        _subscription([], blocking=True),
    ]

    monitor_glider(
        client,
        "testbot",
        dialog_log,
        MagicMock(spec=logging.Logger),
        MagicMock(spec=logging.Logger),
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
        notifier=notifier,
    )

    # Three subscribes -> three connect edges; the two mid-run drops ->
    # two disconnect edges (the third session ends via stop, not a drop).
    assert notifier.record_connect.call_count == 3
    assert notifier.record_disconnect.call_count == 2
    # The supervisor never closes the notifier; main() owns that.
    notifier.close.assert_not_called()


def test_monitor_notifier_records_startup_failure() -> None:
    """A transient failure of the startup status check counts as a
    disconnect, and the later subscribe clears it."""
    stop = threading.Event()
    handler = _CollectingHandler(stop_on="connected", stop=stop)
    dialog_log = logging.getLogger("test.monitor.notifier.startup")
    dialog_log.handlers.clear()
    dialog_log.propagate = False
    dialog_log.setLevel(logging.INFO)
    dialog_log.addHandler(handler)
    notifier = MagicMock(spec=DisconnectNotifier)
    client = _client()
    # The startup status check reads glider details: fail transiently
    # once (rate limited), then succeed so the stream loop can start.
    client.get_glider_details.side_effect = [
        RateLimitError(0.0, "slow down"),
        {"data": {"state": "deployed", "id": 42}},
    ]
    client.subscribe_glider_output.return_value = _subscription(
        [{"sequenceNumber": 0, "data": "connected\r\n"}], blocking=True
    )
    client.subscribe_script_events.return_value = _subscription([], blocking=True)

    monitor_glider(
        client,
        "testbot",
        dialog_log,
        MagicMock(spec=logging.Logger),
        MagicMock(spec=logging.Logger),
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
        notifier=notifier,
    )

    assert notifier.record_disconnect.call_count >= 1
    assert notifier.record_connect.call_count >= 1


@patch("sfmc_api.follow_glider.setup_logging")
def test_follow_drives_notifier_across_sessions(mock_setup_logging: MagicMock) -> None:
    dialog_log, output_log, info_log = _mock_logs()
    mock_setup_logging.return_value = (dialog_log, output_log, info_log)
    stop = threading.Event()
    _reset_follower(stop, stop_after=3)
    notifier = MagicMock(spec=DisconnectNotifier)
    client = _client()
    client.subscribe_glider_output.side_effect = [
        _subscription(_dialog_messages(second=38, mission_time=169339)),
        _subscription(_dialog_messages(second=39, mission_time=169340)),
        _subscription(_dialog_messages(second=40, mission_time=169341), blocking=True),
    ]

    follow_glider(
        client=client,
        glider_name="testbot",
        follower_class=PersistentFollower,
        dry_run=True,
        stop=stop,
        reconnect_initial_delay=0.0,
        reconnect_max_delay=0.0,
        reconnect_jitter=0.0,
        notifier=notifier,
    )

    assert notifier.record_connect.call_count == 3
    assert notifier.record_disconnect.call_count == 2
    notifier.close.assert_not_called()
