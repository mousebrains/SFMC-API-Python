"""Tests for sfmc_api.follow_glider — orchestrator, CLI, and simulation modes."""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from queue import Queue
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sfmc_api import SFMCClient
from sfmc_api.dialog_parser import DialogParser, SurfacingEvent
from sfmc_api.follow_glider import (
    RunStats,
    _open_replay,
    _parse_log_line,
    _print_files,
    _read_dialog,
    _upload_files,
    build_parser,
    follow_glider,
    setup_logging,
)
from sfmc_api.follower import BaseFollower
from sfmc_api.stomp import StompError, StompSubscription

# ── Helpers ─────────────────────────────────────────────────────────


def _make_sub(messages: list[dict[str, Any]]) -> StompSubscription:
    """Create a StompSubscription with pre-loaded messages."""
    q: Queue[dict[str, Any] | StompError | None] = Queue()
    for msg in messages:
        q.put(msg)
    q.put(None)  # sentinel
    return StompSubscription("sub-0", "/topic/test", q)


class RecordingFollower(BaseFollower):
    """Follower that records received events and echoes a file."""

    # Class-level mutable shared state; safe here because tests run sequentially.
    received: list[SurfacingEvent]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        RecordingFollower.received = []

    def on_surfacing(self, event: SurfacingEvent) -> None:
        RecordingFollower.received.append(event)
        self.send_files(
            to_glider={"goto_l30.ma": f"wpt for {event.vehicle_name}"},
        )


# Sample dialog as STOMP messages (live mode).
SAMPLE_DIALOG_MESSAGES = [
    {"sequenceNumber": 0, "data": "Connection Event: Carrier Detect found.123\r\n"},
    {"sequenceNumber": 1, "data": "Vehicle Name: testbot\r\n"},
    {"sequenceNumber": 2, "data": "Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339\r\n"},
    {
        "sequenceNumber": 3,
        "data": "GPS Location:  3310.021 N -11741.800 E measured     64.746 secs ago\r\n",
    },
    {"sequenceNumber": 4, "data": "   sensor:m_battery(volts)=15.0    50.0 secs ago\r\n"},
    {"sequenceNumber": 5, "data": "   sensor:m_water_vx(m/s)=0.04    68.0 secs ago\r\n"},
    {"sequenceNumber": 6, "data": "ABORT HISTORY: total since reset: 1\r\n"},
]

# Same dialog as a log file (from sfmc-monitor-glider).
# Uses shorter logger name "sfmc.t" to stay within line-length limits.
SAMPLE_LOG_LINES = """\
2026-03-28T20:40:38.00 sfmc.t.FOLLOW  Monitoring testbot
2026-03-28T20:40:38.10 sfmc.t.DIALOG  Connection Event: Carrier Detect found.123
2026-03-28T20:40:38.20 sfmc.t.DIALOG  Vehicle Name: testbot
2026-03-28T20:40:38.30 sfmc.t.DIALOG  Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339
2026-03-28T20:40:38.40 sfmc.t.DIALOG  GPS Location:  3310.0 N -11741.8 E measured  64.0 secs ago
2026-03-28T20:40:38.50 sfmc.t.DIALOG     sensor:m_battery(volts)=15.0  50.0 secs ago
2026-03-28T20:40:38.60 sfmc.t.DIALOG     sensor:m_water_vx(m/s)=0.04  68.0 secs ago
2026-03-28T20:40:38.70 sfmc.t.DIALOG  ABORT HISTORY: total since reset: 1
2026-03-28T20:40:38.80 sfmc.t.SCRIPT  state=running name=sfmc.xml
"""


# ── _parse_log_line tests ──────────────────────────────────────────


class TestParseLogLine:
    """Test log line parsing for replay."""

    def test_dialog_line(self) -> None:
        line = "2026-03-28T20:40:38.200000 sfmc.testbot.DIALOG  Vehicle Name: testbot"
        assert _parse_log_line(line) == "Vehicle Name: testbot"

    def test_script_line_skipped(self) -> None:
        line = "2026-03-28T20:40:38.800000 sfmc.testbot.SCRIPT  state=running"
        assert _parse_log_line(line) is None

    def test_follow_line_skipped(self) -> None:
        line = "2026-03-28T20:40:38.000000 sfmc.testbot.FOLLOW  Monitoring testbot"
        assert _parse_log_line(line) is None

    def test_info_line_skipped(self) -> None:
        line = "2026-03-28T20:40:38.000000 sfmc.testbot.INFO  Connected"
        assert _parse_log_line(line) is None

    def test_blank_line(self) -> None:
        assert _parse_log_line("") is None
        assert _parse_log_line("   ") is None

    def test_raw_dialog_line(self) -> None:
        """Lines without a timestamp prefix are treated as raw dialog."""
        assert _parse_log_line("Vehicle Name: testbot") == "Vehicle Name: testbot"

    def test_sensor_line_with_prefix(self) -> None:
        line = (
            "2026-03-28T20:40:38.500000 sfmc.osu685.DIALOG  "
            "   sensor:m_battery(volts)=15.0  50.0 secs ago"
        )
        result = _parse_log_line(line)
        assert result is not None
        assert "sensor:m_battery" in result


# ── Dialog reader tests (live STOMP) ────────────────────────────────


class TestReadDialog:
    """Test the _read_dialog thread function."""

    def test_produces_surfacing_event(self) -> None:
        sub = _make_sub(SAMPLE_DIALOG_MESSAGES)
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        stop = threading.Event()

        _read_dialog(sub, parser, q_in, None, stop)

        event = q_in.get(timeout=2)
        assert event is not None
        assert event.vehicle_name == "testbot"
        assert event.gps_lat is not None
        assert len(event.sensors) == 2

    def test_logs_dialog_lines(self) -> None:
        sub = _make_sub(SAMPLE_DIALOG_MESSAGES)
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        stop = threading.Event()

        mock_log = MagicMock(spec=logging.Logger)
        mock_log.name = "test.dialog"
        mock_log.makeRecord.return_value = MagicMock()

        _read_dialog(sub, parser, q_in, mock_log, stop)

        assert mock_log.handle.call_count > 0

    def test_respects_stop_event(self) -> None:
        q: Queue[dict[str, Any] | StompError | None] = Queue()
        for msg in SAMPLE_DIALOG_MESSAGES:
            q.put(msg)
        sub = StompSubscription("sub-0", "/topic/test", q)

        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        stop = threading.Event()
        stop.set()

        thread = threading.Thread(
            target=_read_dialog,
            args=(sub, parser, q_in, None, stop),
        )
        thread.start()
        sub.close()
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_fragmented_lines_reassembled(self) -> None:
        messages = [
            {"sequenceNumber": 0, "data": "Connection Event: Carrier Detect found.123    Iridium"},
            {"sequenceNumber": 1, "data": " console active and ready...\r\n"},
            {"sequenceNumber": 2, "data": "Vehicle Name: frag\r\n"},
            {
                "sequenceNumber": 3,
                "data": "GPS Location:  3310.021 N -11741.800 E measured     64.0 secs ago\r\n",
            },
            {"sequenceNumber": 4, "data": "   sensor:m_battery(volts)=15.0    50.0 secs ago\r\n"},
            {"sequenceNumber": 5, "data": "ABORT HISTORY: done\r\n"},
        ]
        sub = _make_sub(messages)
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        stop = threading.Event()

        _read_dialog(sub, parser, q_in, None, stop)

        event = q_in.get(timeout=2)
        assert event is not None
        assert event.vehicle_name == "frag"


# ── Replay subscription tests ──────────────────────────────────────


class TestOpenReplay:
    """Test _open_replay: log file → StompSubscription via shared pipeline."""

    def test_replay_through_full_pipeline(self, tmp_path: Path) -> None:
        """Log file → _open_replay → _read_dialog → SurfacingEvent."""
        log_file = tmp_path / "dialog.log"
        log_file.write_text(SAMPLE_LOG_LINES)

        stop = threading.Event()
        sub, reader_thread = _open_replay(log_file, stop)

        # Feed through the same _read_dialog used by live mode.
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        _read_dialog(sub, parser, q_in, None, stop)

        reader_thread.join(timeout=5)
        event = q_in.get(timeout=2)
        assert event is not None
        assert event.vehicle_name == "testbot"
        assert event.gps_lat is not None
        assert len(event.sensors) == 2

    def test_raw_dialog_file(self, tmp_path: Path) -> None:
        """Works with raw dialog text (no timestamp prefix)."""
        raw = "\n".join(
            [
                "Connection Event: Carrier Detect found.123",
                "Vehicle Name: rawbot",
                "Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339",
                "GPS Location:  3310.021 N -11741.800 E measured  64.0 secs ago",
                "   sensor:m_battery(volts)=15.0  50.0 secs ago",
                "ABORT HISTORY: total since reset: 1",
            ]
        )
        log_file = tmp_path / "raw.log"
        log_file.write_text(raw)

        stop = threading.Event()
        sub, reader_thread = _open_replay(log_file, stop)
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        _read_dialog(sub, parser, q_in, None, stop)

        reader_thread.join(timeout=5)
        event = q_in.get(timeout=2)
        assert event is not None
        assert event.vehicle_name == "rawbot"

    def test_skips_non_dialog_lines(self, tmp_path: Path) -> None:
        """SCRIPT, INFO, FOLLOW lines are filtered out by _file_reader."""
        log = (
            "2026-03-28T20:40:38 sfmc.g.FOLLOW  info\n"
            "2026-03-28T20:40:38 sfmc.g.SCRIPT  script\n"
            "2026-03-28T20:40:39 sfmc.g.DIALOG  Connection Event: Carrier Detect found.1\n"
            "2026-03-28T20:40:40 sfmc.g.DIALOG  Vehicle Name: filtered\n"
            "2026-03-28T20:40:41 sfmc.g.DIALOG  "
            "GPS Location:  3310.0 N -117.8 E measured  64.0 secs ago\n"
            "2026-03-28T20:40:42 sfmc.g.DIALOG     sensor:m_battery(volts)=15.0  50.0 secs ago\n"
            "2026-03-28T20:40:43 sfmc.g.DIALOG  ABORT HISTORY: done\n"
        )
        log_file = tmp_path / "mixed.log"
        log_file.write_text(log)

        stop = threading.Event()
        sub, reader_thread = _open_replay(log_file, stop)
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        _read_dialog(sub, parser, q_in, None, stop)

        reader_thread.join(timeout=5)
        event = q_in.get(timeout=2)
        assert event is not None
        assert event.vehicle_name == "filtered"

    def test_empty_log_file(self, tmp_path: Path) -> None:
        log_file = tmp_path / "empty.log"
        log_file.write_text("")

        stop = threading.Event()
        sub, reader_thread = _open_replay(log_file, stop)
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()
        _read_dialog(sub, parser, q_in, None, stop)

        reader_thread.join(timeout=5)
        assert q_in.empty()

    def test_event_interval_delays_events(self, tmp_path: Path) -> None:
        """event_interval causes a pause after each SurfacingEvent."""
        # Two surfacings in the log.
        two_events = SAMPLE_LOG_LINES + SAMPLE_LOG_LINES
        log_file = tmp_path / "two.log"
        log_file.write_text(two_events)

        stop = threading.Event()
        sub, reader_thread = _open_replay(log_file, stop)
        parser = DialogParser()
        q_in: Queue[SurfacingEvent | None] = Queue()

        start = time.monotonic()
        _read_dialog(sub, parser, q_in, None, stop, event_interval=0.5)
        elapsed = time.monotonic() - start

        reader_thread.join(timeout=5)
        # Should have waited ~0.5s after the first event.
        assert elapsed >= 0.4


# ── Dry-run printer tests ──────────────────────────────────────────


class TestPrintFiles:
    """Test the _print_files dry-run output handler."""

    def test_prints_file_content(self) -> None:
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        stop = threading.Event()
        mock_log = MagicMock(spec=logging.Logger)
        mock_log.name = "test.dryrun"

        q_out.put({"to-glider": {"goto_l30.ma": "waypoint data"}})
        q_out.put(None)

        _print_files(q_out, mock_log, stop)

        mock_log.info.assert_called()
        call_args_str = str(mock_log.info.call_args)
        assert "dry-run" in call_args_str
        assert "goto_l30.ma" in call_args_str

    def test_handles_bytes_content(self) -> None:
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        stop = threading.Event()
        mock_log = MagicMock(spec=logging.Logger)
        mock_log.name = "test.dryrun.bytes"

        q_out.put({"to-science": {"data.bin": b"binary content"}})
        q_out.put(None)

        _print_files(q_out, mock_log, stop)

        mock_log.info.assert_called()

    def test_respects_stop_event(self) -> None:
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        stop = threading.Event()
        stop.set()
        mock_log = MagicMock(spec=logging.Logger)
        mock_log.name = "test.dryrun.stop"

        thread = threading.Thread(
            target=_print_files,
            args=(q_out, mock_log, stop),
        )
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive()


# ── Upload thread tests ─────────────────────────────────────────────


class TestUploadFiles:
    """Test the _upload_files thread function."""

    def test_uploads_files(self) -> None:
        mock_client = MagicMock()
        mock_client.upload_glider_file_contents.return_value = {"ok": True}
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        stop = threading.Event()
        log = logging.getLogger("test.upload")

        q_out.put({"to-glider": {"goto_l30.ma": "content"}})
        q_out.put(None)

        _upload_files(mock_client, "g1", q_out, log, stop)

        mock_client.upload_glider_file_contents.assert_called_once_with(
            "g1",
            "to-glider",
            {"goto_l30.ma": "content"},
        )

    def test_handles_upload_error(self) -> None:
        mock_client = MagicMock()
        mock_client.upload_glider_file_contents.side_effect = RuntimeError("network")
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        stop = threading.Event()
        log = logging.getLogger("test.upload.err")

        q_out.put({"to-glider": {"f.ma": "data"}})
        q_out.put(None)

        _upload_files(mock_client, "g1", q_out, log, stop)

    def test_respects_stop_event(self) -> None:
        mock_client = MagicMock()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        stop = threading.Event()
        stop.set()
        log = logging.getLogger("test.upload.stop")

        thread = threading.Thread(
            target=_upload_files,
            args=(mock_client, "g1", q_out, log, stop),
        )
        thread.start()
        thread.join(timeout=5)
        assert not thread.is_alive()

    def test_multiple_folders(self) -> None:
        mock_client = MagicMock()
        mock_client.upload_glider_file_contents.return_value = {}
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        stop = threading.Event()
        log = logging.getLogger("test.upload.multi")

        q_out.put(
            {
                "to-glider": {"goto_l30.ma": "wpts"},
                "to-science": {"log.txt": "data"},
            }
        )
        q_out.put(None)

        _upload_files(mock_client, "g1", q_out, log, stop)

        assert mock_client.upload_glider_file_contents.call_count == 2


# ── Logging setup tests ─────────────────────────────────────────────


class TestSetupLogging:
    """Test the logging configuration."""

    def test_returns_three_loggers(self) -> None:
        d, u, i = setup_logging("testglider")
        assert d.name == "sfmc.testglider.DIALOG"
        assert u.name == "sfmc.testglider.UPLOAD"
        assert i.name == "sfmc.testglider.FOLLOW"

    def test_logfile_creates_rotating_handler(self, tmp_path: pytest.TempPathFactory) -> None:
        logfile = str(tmp_path) + "/test.log"  # type: ignore[operator]
        d, u, i = setup_logging("testglider", log_file=logfile)
        handler_types = [type(h).__name__ for h in d.handlers]
        assert "RotatingFileHandler" in handler_types
        assert "StreamHandler" in handler_types
        for log in (d, u, i):
            for h in log.handlers[:]:
                log.removeHandler(h)
                h.close()

    def test_no_logfile_stderr_only(self) -> None:
        d, u, i = setup_logging("testglider")
        handler_types = [type(h).__name__ for h in d.handlers]
        assert "StreamHandler" in handler_types
        assert "RotatingFileHandler" not in handler_types
        for log in (d, u, i):
            for h in log.handlers[:]:
                log.removeHandler(h)

    def test_log_level(self) -> None:
        d, u, i = setup_logging("testglider", log_level="DEBUG")
        assert d.level == logging.DEBUG
        for log in (d, u, i):
            for h in log.handlers[:]:
                log.removeHandler(h)


# ── CLI argument parser tests ───────────────────────────────────────


class TestBuildParser:
    """Test the CLI argument parser."""

    def test_required_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--glider",
                "osu685",
                "--follower",
                "my_follower.py",
            ]
        )
        assert args.glider == "osu685"
        assert args.follower == "my_follower.py"

    def test_all_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--glider",
                "osu685",
                "--follower",
                "my_follower.py",
                "--class",
                "DrifterFollower",
                "--config",
                "config.yaml",
                "--hostname",
                "sfmc.example.com",
                "--credentials",
                "/path/to/creds.json",
                "--logfile",
                "follow.log",
                "--log-level",
                "DEBUG",
                "--log-max-size",
                "5242880",
                "--log-backup-count",
                "3",
            ]
        )
        assert args.glider == "osu685"
        assert args.follower == "my_follower.py"
        assert args.class_name == "DrifterFollower"
        assert args.config == "config.yaml"
        assert args.hostname == "sfmc.example.com"
        assert args.credentials == "/path/to/creds.json"
        assert args.logfile == "follow.log"
        assert args.log_level == "DEBUG"
        assert args.log_max_size == 5242880
        assert args.log_backup_count == 3

    def test_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--glider",
                "g1",
                "--follower",
                "f.py",
            ]
        )
        assert args.class_name is None
        assert args.config is None
        assert args.hostname is None
        assert args.credentials is None
        assert args.logfile is None
        assert args.log_level == "INFO"
        assert args.log_max_size == 10 * 1024 * 1024
        assert args.log_backup_count == 5

    def test_simulation_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--glider",
                "g1",
                "--follower",
                "f.py",
                "--replay",
                "dialog.log",
                "--replay-interval",
                "5.0",
                "--dry-run",
            ]
        )
        assert args.replay == "dialog.log"
        assert args.replay_interval == 5.0
        assert args.dry_run is True

    def test_simulation_defaults(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "--glider",
                "g1",
                "--follower",
                "f.py",
            ]
        )
        assert args.replay is None
        assert args.replay_interval == 10.0
        assert args.dry_run is False

    def test_missing_glider_fails(self) -> None:
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--follower", "f.py"])

    def test_missing_follower_fails(self) -> None:
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--glider", "g1"])


# ── Integration tests ──────────────────────────────────────────────


class TestFollowGliderIntegration:
    """Integration test: wire up a mock STOMP stream with a follower."""

    @patch("sfmc_api.follow_glider.setup_logging")
    def test_end_to_end_flow(self, mock_setup_logging: MagicMock) -> None:
        """Full flow: dialog -> parser -> follower -> upload."""
        mock_dialog_log = MagicMock(spec=logging.Logger)
        mock_dialog_log.name = "test.dialog"
        mock_dialog_log.makeRecord.return_value = MagicMock()
        mock_upload_log = MagicMock(spec=logging.Logger)
        mock_upload_log.name = "test.upload"
        mock_info_log = MagicMock(spec=logging.Logger)
        mock_info_log.name = "test.info"
        mock_setup_logging.return_value = (mock_dialog_log, mock_upload_log, mock_info_log)

        mock_client = MagicMock(spec=SFMCClient)
        mock_client.get_glider_details.return_value = {
            "data": {"state": "deployed", "id": 42},
        }
        mock_client.upload_glider_file_contents.return_value = {"ok": True}

        sub = _make_sub(SAMPLE_DIALOG_MESSAGES)
        mock_stomp = MagicMock()
        mock_client.open_stream.return_value.__enter__ = MagicMock(return_value=mock_stomp)
        mock_client.open_stream.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.subscribe_glider_output.return_value = sub

        stop = threading.Event()

        def _stop_after_delay() -> None:
            time.sleep(2)
            stop.set()

        stopper = threading.Thread(target=_stop_after_delay, daemon=True)
        stopper.start()

        follow_glider(
            client=mock_client,
            glider_name="testbot",
            follower_class=RecordingFollower,
            follower_config={"test": True},
            stop=stop,
        )

        assert len(RecordingFollower.received) >= 1
        event = RecordingFollower.received[0]
        assert event.vehicle_name == "testbot"

        assert mock_client.upload_glider_file_contents.call_count >= 1
        call_args = mock_client.upload_glider_file_contents.call_args
        assert call_args[0][0] == "testbot"
        assert call_args[0][1] == "to-glider"


class TestFollowGliderReplayDryRun:
    """Integration: replay + dry-run (fully offline, no client)."""

    @patch("sfmc_api.follow_glider.setup_logging")
    def test_offline_replay(
        self,
        mock_setup_logging: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_dialog_log = MagicMock(spec=logging.Logger)
        mock_dialog_log.name = "test.dialog"
        mock_upload_log = MagicMock(spec=logging.Logger)
        mock_upload_log.name = "test.upload"
        mock_info_log = MagicMock(spec=logging.Logger)
        mock_info_log.name = "test.info"
        mock_setup_logging.return_value = (mock_dialog_log, mock_upload_log, mock_info_log)

        log_file = tmp_path / "dialog.log"
        log_file.write_text(SAMPLE_LOG_LINES)

        follow_glider(
            client=None,
            glider_name="testbot",
            follower_class=RecordingFollower,
            follower_config={},
            replay=str(log_file),
            replay_interval=0.0,
            dry_run=True,
        )

        # Follower should have processed at least one event.
        assert len(RecordingFollower.received) >= 1
        assert RecordingFollower.received[0].vehicle_name == "testbot"

        # Dry-run printer should have been called with the file.
        mock_upload_log.info.assert_called()
        logged = str(mock_upload_log.info.call_args_list)
        assert "dry-run" in logged
        assert "goto_l30.ma" in logged


class TestFollowGliderLiveDryRun:
    """Integration: live STOMP + dry-run (no upload)."""

    @patch("sfmc_api.follow_glider.setup_logging")
    def test_live_dry_run(self, mock_setup_logging: MagicMock) -> None:
        mock_dialog_log = MagicMock(spec=logging.Logger)
        mock_dialog_log.name = "test.dialog"
        mock_dialog_log.makeRecord.return_value = MagicMock()
        mock_upload_log = MagicMock(spec=logging.Logger)
        mock_upload_log.name = "test.upload"
        mock_info_log = MagicMock(spec=logging.Logger)
        mock_info_log.name = "test.info"
        mock_setup_logging.return_value = (mock_dialog_log, mock_upload_log, mock_info_log)

        mock_client = MagicMock(spec=SFMCClient)
        mock_client.get_glider_details.return_value = {
            "data": {"state": "deployed", "id": 42},
        }

        sub = _make_sub(SAMPLE_DIALOG_MESSAGES)
        mock_stomp = MagicMock()
        mock_client.open_stream.return_value.__enter__ = MagicMock(return_value=mock_stomp)
        mock_client.open_stream.return_value.__exit__ = MagicMock(return_value=False)
        mock_client.subscribe_glider_output.return_value = sub

        stop = threading.Event()

        def _stop_after_delay() -> None:
            time.sleep(2)
            stop.set()

        stopper = threading.Thread(target=_stop_after_delay, daemon=True)
        stopper.start()

        follow_glider(
            client=mock_client,
            glider_name="testbot",
            follower_class=RecordingFollower,
            follower_config={},
            dry_run=True,
            stop=stop,
        )

        assert len(RecordingFollower.received) >= 1

        # upload_glider_file_contents should NOT have been called.
        mock_client.upload_glider_file_contents.assert_not_called()

        # Dry-run printer should have logged the output.
        mock_upload_log.info.assert_called()
        logged = str(mock_upload_log.info.call_args_list)
        assert "dry-run" in logged


class TestFollowGliderErrorPaths:
    """Test error paths where client is None but required."""

    @patch("sfmc_api.follow_glider.setup_logging")
    def test_live_mode_without_client_returns(
        self,
        mock_setup_logging: MagicMock,
    ) -> None:
        """Live mode (no replay, no dry-run) with client=None should return."""
        mock_info_log = MagicMock(spec=logging.Logger)
        mock_info_log.name = "test.info"
        mock_setup_logging.return_value = (
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            mock_info_log,
        )

        follow_glider(
            client=None,
            glider_name="g1",
            follower_class=RecordingFollower,
            follower_config={},
        )

        mock_info_log.error.assert_called_once()
        assert "client is required" in str(mock_info_log.error.call_args)

    @patch("sfmc_api.follow_glider.setup_logging")
    def test_replay_upload_without_client_returns(
        self,
        mock_setup_logging: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Replay + upload (no dry-run) with client=None should return."""
        mock_info_log = MagicMock(spec=logging.Logger)
        mock_info_log.name = "test.info"
        mock_setup_logging.return_value = (
            MagicMock(spec=logging.Logger),
            MagicMock(spec=logging.Logger),
            mock_info_log,
        )

        log_file = tmp_path / "dialog.log"
        log_file.write_text("some data\n")

        follow_glider(
            client=None,
            glider_name="g1",
            follower_class=RecordingFollower,
            follower_config={},
            replay=str(log_file),
            dry_run=False,
        )

        mock_info_log.error.assert_called_once()
        assert "client is required" in str(mock_info_log.error.call_args)


class TestRunStats:
    """RunStats counters and end-of-run summary."""

    def test_starts_at_zero(self) -> None:
        s = RunStats()
        assert s.surfacings == 0
        assert s.files_emitted == 0
        assert s.upload_errors == 0
        assert not s.had_errors()

    def test_incr_methods(self) -> None:
        s = RunStats()
        s.incr_surfacings()
        s.incr_files(3)
        s.incr_upload_errors()
        assert s.surfacings == 1
        assert s.files_emitted == 3
        assert s.upload_errors == 1
        assert s.had_errors()

    def test_format_summary(self) -> None:
        s = RunStats()
        s.incr_surfacings()
        s.incr_files(2)
        assert "surfacings=1" in s.format()
        assert "files_emitted=2" in s.format()
        assert "upload_errors=0" in s.format()

    def test_concurrent_increments(self) -> None:
        """Counters survive concurrent increments without losing updates."""
        s = RunStats()
        threads = [
            threading.Thread(target=lambda: [s.incr_surfacings() for _ in range(1000)])
            for _ in range(8)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert s.surfacings == 8000

    @patch("sfmc_api.follow_glider.setup_logging")
    def test_replay_returns_stats(
        self,
        mock_setup_logging: MagicMock,
        tmp_path: Path,
    ) -> None:
        mock_dialog_log = MagicMock(spec=logging.Logger)
        mock_dialog_log.name = "test.dialog"
        mock_upload_log = MagicMock(spec=logging.Logger)
        mock_upload_log.name = "test.upload"
        mock_info_log = MagicMock(spec=logging.Logger)
        mock_info_log.name = "test.info"
        mock_setup_logging.return_value = (mock_dialog_log, mock_upload_log, mock_info_log)

        log_file = tmp_path / "dialog.log"
        log_file.write_text(SAMPLE_LOG_LINES)

        stats = follow_glider(
            client=None,
            glider_name="testbot",
            follower_class=RecordingFollower,
            follower_config={},
            replay=str(log_file),
            replay_interval=0.0,
            dry_run=True,
        )

        assert isinstance(stats, RunStats)
        assert stats.surfacings >= 1
        assert stats.files_emitted >= 1
        assert stats.upload_errors == 0


class TestStrictFlagExit:
    """``--strict`` exits non-zero only when the run had upload errors."""

    @patch("sfmc_api.follow_glider.follow_glider")
    @patch("sfmc_api.follow_glider.load_follower_class")
    def test_strict_exits_2_when_errors(
        self,
        mock_load: MagicMock,
        mock_follow: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Pretend the follower module loads fine.
        mock_load.return_value = RecordingFollower
        # follow_glider returns a RunStats with one upload error.
        stats = RunStats()
        stats.incr_upload_errors()
        mock_follow.return_value = stats

        monkeypatch.setattr(
            "sys.argv",
            [
                "sfmc-follow",
                "--glider",
                "g1",
                "--follower",
                "fake.py",
                "--replay",
                "fake.log",
                "--dry-run",
                "--strict",
            ],
        )
        from sfmc_api.follow_glider import main

        with pytest.raises(SystemExit) as exc:
            main()
        assert exc.value.code == 2

    @patch("sfmc_api.follow_glider.follow_glider")
    @patch("sfmc_api.follow_glider.load_follower_class")
    def test_strict_returns_normally_when_no_errors(
        self,
        mock_load: MagicMock,
        mock_follow: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_load.return_value = RecordingFollower
        mock_follow.return_value = RunStats()  # no errors

        monkeypatch.setattr(
            "sys.argv",
            [
                "sfmc-follow",
                "--glider",
                "g1",
                "--follower",
                "fake.py",
                "--replay",
                "fake.log",
                "--dry-run",
                "--strict",
            ],
        )
        from sfmc_api.follow_glider import main

        # No SystemExit means clean return.
        main()

    @patch("sfmc_api.follow_glider.follow_glider")
    @patch("sfmc_api.follow_glider.load_follower_class")
    def test_no_strict_returns_normally_even_with_errors(
        self,
        mock_load: MagicMock,
        mock_follow: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        mock_load.return_value = RecordingFollower
        stats = RunStats()
        stats.incr_upload_errors()
        mock_follow.return_value = stats

        monkeypatch.setattr(
            "sys.argv",
            [
                "sfmc-follow",
                "--glider",
                "g1",
                "--follower",
                "fake.py",
                "--replay",
                "fake.log",
                "--dry-run",
            ],
        )
        from sfmc_api.follow_glider import main

        # Without --strict, errors do not force a non-zero exit.
        main()
