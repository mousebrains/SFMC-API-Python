"""Tests for examples/drifter_follower.py — drifter tracking algorithm.

These tests require ``netCDF4`` and ``numpy`` (the ``[drifter]`` extra).
They are automatically skipped when those packages are not installed,
so CI does not need to install heavy scientific dependencies.
"""

from __future__ import annotations

import sys
from pathlib import Path
from queue import Queue
from unittest.mock import MagicMock, patch

import pytest

from sfmc_api.dialog_parser import SensorReading, SurfacingEvent

# Skip the entire module if netCDF4 is not installed.
pytest.importorskip("netCDF4", reason="netCDF4 not installed (pip install 'sfmc-api[drifter]')")

# Add examples/ to sys.path so we can import drifter_follower.
_examples_dir = str(Path(__file__).resolve().parent.parent / "examples")
if _examples_dir not in sys.path:
    sys.path.insert(0, _examples_dir)


# ── _estimate_transit_time tests ────────────────────────────────────


class TestEstimateTransitTime:
    """Test transit time estimation between waypoints."""

    def _import(self):
        from drifter_follower import _estimate_transit_time

        return _estimate_transit_time

    def test_stationary_current(self) -> None:
        est = self._import()
        # 1 km north, glider at 0.5 m/s, no current.
        lat1, lon1 = 33.0, -117.0
        lat2 = 33.0 + 1.0 / 111.32  # ~1 km north
        lon2 = -117.0
        t = est(lat1, lon1, lat2, lon2, 0.5, 0.0, 0.0)
        # ~1000 m / 0.5 m/s = ~2000 s
        assert 1800 < t < 2200

    def test_following_current_reduces_time(self) -> None:
        est = self._import()
        lat1, lon1 = 33.0, -117.0
        lat2 = 33.0 + 1.0 / 111.32
        lon2 = -117.0
        t_no_current = est(lat1, lon1, lat2, lon2, 0.5, 0.0, 0.0)
        # Northward current helps the glider.
        t_with_current = est(lat1, lon1, lat2, lon2, 0.5, 0.0, 0.2)
        assert t_with_current < t_no_current

    def test_opposing_current_increases_time(self) -> None:
        est = self._import()
        lat1, lon1 = 33.0, -117.0
        lat2 = 33.0 + 1.0 / 111.32
        lon2 = -117.0
        t_no_current = est(lat1, lon1, lat2, lon2, 0.5, 0.0, 0.0)
        t_opposing = est(lat1, lon1, lat2, lon2, 0.5, 0.0, -0.3)
        assert t_opposing > t_no_current

    def test_zero_distance_returns_minimum(self) -> None:
        est = self._import()
        t = est(33.0, -117.0, 33.0, -117.0, 0.5, 0.0, 0.0)
        assert t == 1.0

    def test_effective_speed_clamped(self) -> None:
        est = self._import()
        # Strong opposing current, effective speed would be negative.
        lat1, lon1 = 33.0, -117.0
        lat2 = 33.0 + 1.0 / 111.32
        lon2 = -117.0
        t = est(lat1, lon1, lat2, lon2, 0.5, 0.0, -10.0)
        # Should be clamped, not negative or infinite.
        assert t > 0
        # Capped at 24 hours max.
        assert t <= 86400.0


# ── DrifterFollower.on_surfacing tests ──────────────────────────────


class TestDrifterFollowerOnSurfacing:
    """Test the DrifterFollower with mocked NetCDF data."""

    def _make_event(
        self,
        lat: float = 33.167,
        lon: float = -117.697,
        vx: float = 0.04,
        vy: float = 0.04,
    ) -> SurfacingEvent:
        return SurfacingEvent(
            vehicle_name="testbot",
            gps_lat=lat,
            gps_lon=lon,
            sensors={
                "m_water_vx": SensorReading("m_water_vx", "m/s", vx, 60.0),
                "m_water_vy": SensorReading("m_water_vy", "m/s", vy, 60.0),
            },
        )

    @patch("drifter_follower._get_drifter_state")
    def test_generates_goto_file(self, mock_drifter: MagicMock) -> None:
        from drifter_follower import DrifterFollower

        mock_drifter.return_value = (33.17, -117.70, 0.01, 0.01)

        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        config = {
            "input": "drifter.nc",
            "sequence_number": 30,
            "geometry": [[1, 0], [0, 1], [-1, 0], [0, -1]],
            "glider": {"speed_horizontal": 0.5},
            "list_when_wpt_dist": 100.0,
        }

        follower = DrifterFollower(
            config=config,
            queue_in=q_in,
            queue_out=q_out,
        )
        follower.on_surfacing(self._make_event())

        output = q_out.get(timeout=2)
        assert output is not None
        assert "to-glider" in output
        filenames = list(output["to-glider"].keys())
        assert len(filenames) == 1
        assert filenames[0] == "goto_l30.ma"

        content = output["to-glider"]["goto_l30.ma"]
        assert isinstance(content, str)
        assert "behavior_name=goto_list" in content
        assert "num_waypoints(nodim) 4" in content

    @patch("drifter_follower._get_drifter_state")
    def test_skips_when_no_gps(self, mock_drifter: MagicMock) -> None:
        from drifter_follower import DrifterFollower

        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        config = {
            "input": "drifter.nc",
            "sequence_number": 30,
            "geometry": [[1, 0]],
            "glider": {"speed_horizontal": 0.5},
        }
        follower = DrifterFollower(
            config=config,
            queue_in=q_in,
            queue_out=q_out,
        )
        # Event with no GPS fix.
        event = SurfacingEvent(vehicle_name="testbot")
        follower.on_surfacing(event)
        assert q_out.empty()

    @patch("drifter_follower._get_drifter_state")
    def test_skips_when_drifter_unavailable(
        self,
        mock_drifter: MagicMock,
    ) -> None:
        from drifter_follower import DrifterFollower

        mock_drifter.return_value = None

        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        config = {
            "input": "drifter.nc",
            "sequence_number": 30,
            "geometry": [[1, 0]],
            "glider": {"speed_horizontal": 0.5},
        }
        follower = DrifterFollower(
            config=config,
            queue_in=q_in,
            queue_out=q_out,
        )
        follower.on_surfacing(self._make_event())
        assert q_out.empty()

    @patch("drifter_follower._get_drifter_state")
    def test_missing_config_keys(self, mock_drifter: MagicMock) -> None:
        from drifter_follower import DrifterFollower

        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()

        # Missing 'input' key.
        follower = DrifterFollower(
            config={},
            queue_in=q_in,
            queue_out=q_out,
        )
        follower.on_surfacing(self._make_event())
        assert q_out.empty()

    @patch("drifter_follower._get_drifter_state")
    def test_no_current_sensors_uses_zero(
        self,
        mock_drifter: MagicMock,
    ) -> None:
        from drifter_follower import DrifterFollower

        mock_drifter.return_value = (33.17, -117.70, 0.01, 0.01)

        q_in: Queue[SurfacingEvent | None] = Queue()
        q_out: Queue[dict[str, dict[str, str | bytes]] | None] = Queue()
        config = {
            "input": "drifter.nc",
            "sequence_number": 30,
            "geometry": [[1, 0]],
            "glider": {"speed_horizontal": 0.5},
        }
        follower = DrifterFollower(
            config=config,
            queue_in=q_in,
            queue_out=q_out,
        )
        # Event with GPS but no current sensors.
        event = SurfacingEvent(
            vehicle_name="testbot",
            gps_lat=33.167,
            gps_lon=-117.697,
            sensors={},
        )
        follower.on_surfacing(event)
        output = q_out.get(timeout=2)
        assert output is not None
        assert "to-glider" in output
