"""Tests for sfmc_api.follower — BaseFollower and dynamic loader."""

from __future__ import annotations

import textwrap
from pathlib import Path
from queue import Queue

import pytest

from sfmc_api.dialog_parser import SurfacingEvent
from sfmc_api.follower import BaseFollower, load_follower_class

# ── Test follower subclass ──────────────────────────────────────────


class EchoFollower(BaseFollower):
    """Minimal follower that echoes the vehicle name as a file."""

    def on_surfacing(self, event: SurfacingEvent) -> None:
        name = event.vehicle_name or "unknown"
        self.send_files(to_glider={f"{name}.txt": f"surfaced: {name}"})


class CrashFollower(BaseFollower):
    """Follower that raises on every surfacing (for error handling tests)."""

    def on_surfacing(self, event: SurfacingEvent) -> None:
        raise RuntimeError("intentional crash")


# ── BaseFollower run loop ───────────────────────────────────────────


class TestBaseFollowerRunLoop:
    """Test the default run() event loop."""

    def test_processes_surfacing_event(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()

        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)
        event = SurfacingEvent(vehicle_name="osu685")
        q_in.put(event)
        q_in.put(None)  # Shutdown sentinel.

        follower.start()
        follower.join(timeout=5)
        assert not follower.is_alive()

        output = q_out.get(timeout=1)
        assert output is not None
        assert "to-glider" in output
        assert "osu685.txt" in output["to-glider"]

    def test_shutdown_sentinel_stops_loop(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()

        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)
        q_in.put(None)

        follower.start()
        follower.join(timeout=5)
        assert not follower.is_alive()
        assert q_out.empty()

    def test_error_in_on_surfacing_continues(self) -> None:
        """A crash in on_surfacing should not kill the follower thread."""
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()

        follower = CrashFollower(config={}, queue_in=q_in, queue_out=q_out)
        q_in.put(SurfacingEvent(vehicle_name="test1"))  # Will crash.
        q_in.put(SurfacingEvent(vehicle_name="test2"))  # Will also crash.
        q_in.put(None)  # Shutdown.

        follower.start()
        follower.join(timeout=5)
        assert not follower.is_alive()
        # No output because CrashFollower always raises.
        assert q_out.empty()

    def test_multiple_events(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()

        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)
        q_in.put(SurfacingEvent(vehicle_name="g1"))
        q_in.put(SurfacingEvent(vehicle_name="g2"))
        q_in.put(None)

        follower.start()
        follower.join(timeout=5)

        results = []
        while not q_out.empty():
            results.append(q_out.get_nowait())
        assert len(results) == 2


# ── send_files ──────────────────────────────────────────────────────


class TestSendFiles:
    """Test the send_files convenience method."""

    def test_to_glider_only(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)

        follower.send_files(to_glider={"a.ma": "content"})
        output = q_out.get_nowait()
        assert output == {"to-glider": {"a.ma": "content"}}

    def test_to_science_only(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)

        follower.send_files(to_science={"data.dat": b"binary"})
        output = q_out.get_nowait()
        assert output == {"to-science": {"data.dat": b"binary"}}

    def test_both_folders(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)

        follower.send_files(
            to_glider={"goto_l30.ma": "waypoints"},
            to_science={"log.txt": "data"},
        )
        output = q_out.get_nowait()
        assert "to-glider" in output
        assert "to-science" in output

    def test_empty_dicts_not_sent(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)

        follower.send_files()  # No files.
        assert q_out.empty()


# ── shutdown ────────────────────────────────────────────────────────


class TestShutdown:
    """Test the shutdown method."""

    def test_puts_none_sentinel(self) -> None:
        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        follower = EchoFollower(config={}, queue_in=q_in, queue_out=q_out)

        follower.shutdown()
        assert q_in.get_nowait() is None


# ── Dynamic loader ──────────────────────────────────────────────────


class TestLoadFollowerClass:
    """Test load_follower_class dynamic loading."""

    def test_load_with_class_name(self, tmp_path: Path) -> None:
        src = textwrap.dedent("""\
            from sfmc_api.follower import BaseFollower
            from sfmc_api.dialog_parser import SurfacingEvent

            class MyFollower(BaseFollower):
                def on_surfacing(self, event: SurfacingEvent) -> None:
                    pass
        """)
        f = tmp_path / "my_follower.py"
        f.write_text(src)

        cls = load_follower_class(f, "MyFollower")
        assert cls.__name__ == "MyFollower"
        assert issubclass(cls, BaseFollower)

    def test_auto_detect_single_class(self, tmp_path: Path) -> None:
        src = textwrap.dedent("""\
            from sfmc_api.follower import BaseFollower
            from sfmc_api.dialog_parser import SurfacingEvent

            class OnlyFollower(BaseFollower):
                def on_surfacing(self, event: SurfacingEvent) -> None:
                    pass
        """)
        f = tmp_path / "only.py"
        f.write_text(src)

        cls = load_follower_class(f)
        assert cls.__name__ == "OnlyFollower"

    def test_auto_detect_multiple_raises(self, tmp_path: Path) -> None:
        src = textwrap.dedent("""\
            from sfmc_api.follower import BaseFollower
            from sfmc_api.dialog_parser import SurfacingEvent

            class A(BaseFollower):
                def on_surfacing(self, event: SurfacingEvent) -> None:
                    pass

            class B(BaseFollower):
                def on_surfacing(self, event: SurfacingEvent) -> None:
                    pass
        """)
        f = tmp_path / "multi.py"
        f.write_text(src)

        with pytest.raises(ValueError, match="Multiple"):
            load_follower_class(f)

    def test_no_subclass_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.py"
        f.write_text("x = 42\n")

        with pytest.raises(ValueError, match="No BaseFollower"):
            load_follower_class(f)

    def test_class_name_not_found_raises(self, tmp_path: Path) -> None:
        src = textwrap.dedent("""\
            from sfmc_api.follower import BaseFollower
            from sfmc_api.dialog_parser import SurfacingEvent

            class MyFollower(BaseFollower):
                def on_surfacing(self, event: SurfacingEvent) -> None:
                    pass
        """)
        f = tmp_path / "mf.py"
        f.write_text(src)

        with pytest.raises(ValueError, match="not found"):
            load_follower_class(f, "NonExistent")

    def test_class_name_not_subclass_raises(self, tmp_path: Path) -> None:
        src = "class NotAFollower:\n    pass\n"
        f = tmp_path / "bad.py"
        f.write_text(src)

        with pytest.raises(ValueError, match="not a BaseFollower"):
            load_follower_class(f, "NotAFollower")

    def test_file_not_found_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_follower_class("/nonexistent/path/follower.py")

    def test_config_passed_to_follower(self, tmp_path: Path) -> None:
        src = textwrap.dedent("""\
            from sfmc_api.follower import BaseFollower
            from sfmc_api.dialog_parser import SurfacingEvent

            class CfgFollower(BaseFollower):
                def on_surfacing(self, event: SurfacingEvent) -> None:
                    pass
        """)
        f = tmp_path / "cfg.py"
        f.write_text(src)

        cls = load_follower_class(f)
        from queue import Queue

        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        config = {"key": "value"}
        follower = cls(config=config, queue_in=q_in, queue_out=q_out)
        assert follower.config == {"key": "value"}


class TestFollowerNotify:
    """BaseFollower.notify — discretionary operator email from engine code."""

    def _follower(self):
        from queue import Queue

        from sfmc_api.follower import BaseFollower

        class Minimal(BaseFollower):
            def on_surfacing(self, event) -> None:
                pass

        return Minimal(config={}, queue_in=Queue(), queue_out=Queue())

    def test_notify_without_notifier_is_silent_noop(self) -> None:
        follower = self._follower()
        assert follower.notify("float-feed-down", "feed unavailable") is False

    def test_notify_delegates_to_notifier(self) -> None:
        from unittest.mock import MagicMock

        from sfmc_api.disconnect_notify import DisconnectNotifier

        follower = self._follower()
        notifier = MagicMock(spec=DisconnectNotifier)
        notifier.notify_event.return_value = True
        follower.set_notifier(notifier)

        assert follower.notify(
            "ma-write-failed",
            "could not generate goto.ma",
            "solver returned no waypoint",
            min_gap_seconds=600.0,
        )
        notifier.notify_event.assert_called_once_with(
            "ma-write-failed",
            "could not generate goto.ma",
            "solver returned no waypoint",
            min_gap_seconds=600.0,
        )
