"""Drifter-following follower for sfmc-follow.

This follower reads drifter positions from a NetCDF file, predicts
where the drifter will be in the near future, and generates a
``goto_l{N}.ma`` waypoint file that keeps the glider flying a
geometric pattern (e.g. a diamond) around the drifter.

The waypoints are placed in the drifter's reference frame — as the
drifter moves, the pattern moves with it.  Ocean currents measured
by the glider (``m_water_vx``, ``m_water_vy``) are used to improve
transit time estimates.

Requirements::

    pip install 'sfmc-api[drifter]'   # installs netCDF4, numpy, pyyaml

Configuration (YAML)::

    input: drifter.nc         # NetCDF with time, latitude, longitude
    sequence_number: 30       # slot number → goto_l30.ma
    geometry:                 # pattern offsets in km [east, north]
      - [1, 0]
      - [0, 1]
      - [-1, 0]
      - [0, -1]
    glider:
      speed_horizontal: 0.5  # m/s through-water
    list_when_wpt_dist: 100.0 # metres

Usage::

    sfmc-follow --glider osu685 \\
                --follower examples/drifter_follower.py \\
                --config examples/drifter_config.yaml
"""

from __future__ import annotations

import logging
import math

try:
    import netCDF4  # type: ignore[import-untyped]
except ImportError:
    raise ImportError(
        "netCDF4 is required for the drifter follower. "
        "Install with: pip install 'sfmc-api[drifter]'"
    ) from None

try:
    import numpy as np  # type: ignore[import-untyped]
except ImportError:
    raise ImportError(
        "numpy is required for the drifter follower. Install with: pip install 'sfmc-api[drifter]'"
    ) from None

from sfmc_api.coordinates import km_to_degrees
from sfmc_api.dialog_parser import SurfacingEvent
from sfmc_api.follower import BaseFollower
from sfmc_api.ma_writer import generate_goto_ma

logger = logging.getLogger(__name__)


def _get_drifter_state(
    nc_path: str,
) -> tuple[float, float, float, float] | None:
    """Read the latest drifter position and velocity from a NetCDF file.

    The file must contain variables ``time``, ``latitude``, and
    ``longitude``.  Time is converted to hours via ``netCDF4.num2date``.

    Returns:
        ``(lat, lon, vel_east_m_s, vel_north_m_s)`` or ``None`` if the
        file has fewer than 2 positions (velocity cannot be estimated).
    """
    with netCDF4.Dataset(nc_path, "r") as ds:
        time_var = ds.variables["time"]
        lat = np.array(ds.variables["latitude"][:])
        lon = np.array(ds.variables["longitude"][:])

        # Convert time to datetime objects, then to hours since first.
        times = netCDF4.num2date(time_var[:], time_var.units, time_var.calendar)
        if len(times) < 2:
            return None

        # Use the last two points to estimate velocity.
        dt_sec = (times[-1] - times[-2]).total_seconds()
        if dt_sec <= 0:
            return None

        # Velocity in degrees/s, then convert to m/s.
        dlat = float(lat[-1] - lat[-2])
        dlon = float(lon[-1] - lon[-2])

        # Approximate conversion: 1 degree lat ≈ 111320 m
        mean_lat = float((lat[-1] + lat[-2]) / 2.0)
        m_per_deg_lat = 111320.0
        m_per_deg_lon = 111320.0 * math.cos(math.radians(mean_lat))

        vel_north = dlat * m_per_deg_lat / dt_sec  # m/s
        vel_east = dlon * m_per_deg_lon / dt_sec  # m/s

        return float(lat[-1]), float(lon[-1]), vel_east, vel_north


def _estimate_transit_time(
    from_lat: float,
    from_lon: float,
    to_lat: float,
    to_lon: float,
    glider_speed: float,
    current_vx: float,
    current_vy: float,
) -> float:
    """Estimate transit time in seconds between two points.

    Accounts for ocean currents by projecting them onto the transit
    direction to get effective ground speed.

    Args:
        from_lat, from_lon: Start position in decimal degrees.
        to_lat, to_lon: End position in decimal degrees.
        glider_speed: Through-water horizontal speed in m/s.
        current_vx: Eastward ocean current in m/s.
        current_vy: Northward ocean current in m/s.

    Returns:
        Estimated transit time in seconds (minimum 1.0).
    """
    mean_lat = (from_lat + to_lat) / 2.0
    m_per_deg_lat = 111320.0
    m_per_deg_lon = 111320.0 * math.cos(math.radians(mean_lat))

    dx = (to_lon - from_lon) * m_per_deg_lon  # metres east
    dy = (to_lat - from_lat) * m_per_deg_lat  # metres north
    distance = math.sqrt(dx * dx + dy * dy)

    if distance < 1.0:
        return 1.0

    # Unit vector in transit direction.
    ux = dx / distance
    uy = dy / distance

    # Component of current along transit direction.
    current_along = current_vx * ux + current_vy * uy

    # Effective ground speed toward waypoint.
    effective_speed = glider_speed + current_along
    if effective_speed < 0.05:
        # Avoid division by near-zero; glider can barely make headway.
        effective_speed = 0.05

    return distance / effective_speed


class DrifterFollower(BaseFollower):
    """Follow a drifter by generating waypoints around its predicted position.

    On each glider surfacing, this follower:

    1. Reads the latest drifter position and velocity from a NetCDF file.
    2. Gets the glider's current position and ocean currents from telemetry.
    3. For each waypoint in the configured geometry pattern, estimates
       the transit time and advances the drifter position prediction.
    4. Generates a ``goto_l{N}.ma`` file and queues it for upload.

    Configuration keys (from the YAML --config file):

    - ``input`` (str): Path to drifter NetCDF file.
    - ``sequence_number`` (int): Slot number for the .ma filename.
    - ``geometry`` (list of [east_km, north_km]): Pattern around drifter.
    - ``glider.speed_horizontal`` (float): Through-water speed in m/s.
    - ``list_when_wpt_dist`` (float, optional): Waypoint distance in metres.
    """

    def on_surfacing(self, event: SurfacingEvent) -> None:
        """Process one surfacing: predict drifter, build waypoints, upload."""
        nc_path = self.config.get("input")
        if not nc_path:
            logger.error("No 'input' NetCDF path in config")
            return

        seq = int(self.config.get("sequence_number", 30))
        geometry = self.config.get("geometry", [])
        if not geometry:
            logger.error("No 'geometry' defined in config")
            return

        glider_cfg = self.config.get("glider", {})
        glider_speed = float(glider_cfg.get("speed_horizontal", 0.5))
        wpt_dist = float(self.config.get("list_when_wpt_dist", 100.0))

        # ── Get glider state from telemetry ─────────────────────
        if event.gps_lat is None or event.gps_lon is None:
            logger.warning("No GPS fix in surfacing event, skipping")
            return

        glider_lat = event.gps_lat
        glider_lon = event.gps_lon

        # Ocean currents (default to zero if not available).
        vx_sensor = event.sensors.get("m_water_vx")
        vy_sensor = event.sensors.get("m_water_vy")
        current_vx = vx_sensor.value if vx_sensor else 0.0
        current_vy = vy_sensor.value if vy_sensor else 0.0

        # ── Get drifter state ───────────────────────────────────
        drifter_state = _get_drifter_state(nc_path)
        if drifter_state is None:
            logger.warning("Cannot read drifter state from %s", nc_path)
            return

        drifter_lat, drifter_lon, drift_vx, drift_vy = drifter_state
        logger.info(
            "Drifter at %.4f, %.4f  vel=(%.4f, %.4f) m/s",
            drifter_lat,
            drifter_lon,
            drift_vx,
            drift_vy,
        )
        logger.info(
            "Glider at %.4f, %.4f  current=(%.4f, %.4f) m/s",
            glider_lat,
            glider_lon,
            current_vx,
            current_vy,
        )

        # ── Build waypoints ─────────────────────────────────────
        waypoints: list[tuple[float, float]] = []
        cumulative_time = 0.0
        prev_lat = glider_lat
        prev_lon = glider_lon

        for offset in geometry:
            east_km = float(offset[0])
            north_km = float(offset[1])

            # Estimate transit time from previous position to this waypoint.
            # First, predict where the drifter will be after transit.
            m_per_deg_lat = 111320.0
            m_per_deg_lon = 111320.0 * math.cos(math.radians(drifter_lat))

            # Advance drifter position by cumulative time so far.
            pred_lat = drifter_lat + (drift_vy * cumulative_time) / m_per_deg_lat
            pred_lon = drifter_lon + (drift_vx * cumulative_time) / m_per_deg_lon

            # Apply geometry offset.
            dlon_offset, dlat_offset = km_to_degrees(east_km, north_km, pred_lat)
            wpt_lon = pred_lon + dlon_offset
            wpt_lat = pred_lat + dlat_offset

            # Estimate transit time to this waypoint.
            transit = _estimate_transit_time(
                prev_lat,
                prev_lon,
                wpt_lat,
                wpt_lon,
                glider_speed,
                current_vx,
                current_vy,
            )
            cumulative_time += transit

            # Re-predict drifter position at arrival and re-place waypoint.
            pred_lat = drifter_lat + (drift_vy * cumulative_time) / m_per_deg_lat
            pred_lon = drifter_lon + (drift_vx * cumulative_time) / m_per_deg_lon
            wpt_lon = pred_lon + dlon_offset
            wpt_lat = pred_lat + dlat_offset

            waypoints.append((wpt_lon, wpt_lat))
            prev_lat = wpt_lat
            prev_lon = wpt_lon

            logger.debug(
                "  WPT %d: (%.4f, %.4f) transit=%.0fs cumulative=%.0fs",
                len(waypoints),
                wpt_lon,
                wpt_lat,
                transit,
                cumulative_time,
            )

        if not waypoints:
            logger.warning("No waypoints generated")
            return

        # ── Generate .ma file and send ──────────────────────────
        filename, content = generate_goto_ma(
            waypoints=waypoints,
            sequence_number=seq,
            num_legs_to_run=-2,
            list_when_wpt_dist=wpt_dist,
        )

        logger.info(
            "Generated %s with %d waypoints (cumulative transit %.0fs)",
            filename,
            len(waypoints),
            cumulative_time,
        )
        self.send_files(to_glider={filename: content})
