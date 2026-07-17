"""Drifter-following follower for sfmc-follow.

What this follower does
-----------------------

Imagine you have deployed a drifting instrument (a surface drifter,
a sediment trap, a float) in the ocean, and you want your Slocum
glider to keep flying near it.  The drifter moves with the currents,
so you cannot just give the glider a fixed set of waypoints -- the
target is always moving.

This follower solves that problem.  Every time the glider surfaces,
it:

1. Reads the drifter's latest known position and velocity from a
   NetCDF file that you update externally (e.g. from satellite
   tracking, a shore-side script, or an operational model).
2. Reads the glider's GPS position and depth-average current from
   the surfacing telemetry.
3. Predicts where the drifter will be by the time the glider reaches
   each waypoint, accounting for both drifter drift and ocean
   currents affecting the glider's ground speed.
4. Places waypoints in a geometric pattern (diamond, triangle, line,
   etc.) around the *predicted* drifter position -- not where the
   drifter is now, but where it will be when the glider arrives.
5. Writes a ``goto_l{N}.ma`` file and queues it for upload to SFMC.

The glider picks up the new ``.ma`` file the next time it calls in,
and starts flying the updated pattern.

The algorithm step by step
--------------------------

For each waypoint in the configured geometry pattern:

a. Start from the glider's current position (for the first waypoint)
   or from the previous waypoint (for subsequent ones).
b. Add the geometry offset (e.g. "1 km east of the drifter") to the
   drifter's position extrapolated forward by the cumulative transit
   time so far.  This gives a candidate waypoint.
c. Estimate how long the glider will take to swim from its previous
   position to this candidate waypoint, using the configured
   through-water speed adjusted by the ocean current projected along
   the transit direction.
d. Re-extrapolate the drifter position using the updated cumulative
   time and re-place the waypoint.  This second pass corrects for
   the drifter motion that occurs during this specific transit leg.

The result is a set of waypoints that "lead" the drifter, so the
glider arrives at the right place at the right time.

How ocean currents are compensated
----------------------------------

The glider reports depth-average current (``m_water_vx`` and
``m_water_vy``, in m/s east and north) at each surfacing.  These
values describe the net current the glider experienced over its last
dive cycle.

When estimating transit time to a waypoint, the algorithm projects
this current vector onto the direction of travel.  If the current
is favorable (pushing the glider toward the waypoint), the effective
ground speed is higher and transit time is shorter.  If the current
is adverse, transit time increases.  This makes the drifter-position
prediction more accurate.

If the glider has no current data (e.g. first dive, or the sensor
is not configured), the algorithm assumes zero current.

Configuration parameters
-------------------------

The YAML config file (see ``examples/drifter_config.yaml``) contains:

``input`` (str)
    Path to a NetCDF file with at least three variables: ``time``,
    ``latitude``, and ``longitude``.  This file is re-read on every
    surfacing, so an external process can update it while the
    follower is running.

``sequence_number`` (int, default 30)
    The slot number *N* in the glider's mission file.  The generated
    file will be named ``goto_l{N}.ma``.  This must match the
    ``args_from_file`` slot in the ``goto_list`` behavior of the
    glider's active ``.mi`` mission.

``geometry`` (list of [east_km, north_km])
    The pattern of waypoints to fly around the drifter.  Each entry
    is an offset in kilometres: ``[east_km, north_km]``.  A diamond
    pattern might be ``[[1,0], [0,1], [-1,0], [0,-1]]``.  See the
    config file for more examples.

``glider.speed_horizontal`` (float, default 0.5)
    The glider's horizontal through-water speed in m/s.  Used to
    estimate transit times.  Typical Slocum values: 0.25-0.50 m/s.

``list_when_wpt_dist`` (float, default 100.0)
    The distance in metres at which the glider considers a waypoint
    "reached" and moves on to the next one.

What the generated file does on the glider
------------------------------------------

The output is a ``goto_l{N}.ma`` file -- a mission-argument file for
the ``goto_list`` behavior.  It contains a list of waypoints in the
glider's native DDDMM.MMMM coordinate format plus parameters like
"traverse the list once" and "consider a waypoint reached at 100 m
distance."  When SFMC delivers this file to the glider, the
``goto_list`` behavior reads it and starts navigating to the first
(closest) waypoint, then the second, and so on.

Setting up the NetCDF file
--------------------------

The NetCDF file must have these variables:

- ``time`` -- a 1-D array with a ``units`` attribute that
  ``netCDF4.num2date`` can parse (e.g. ``"seconds since 1970-01-01"``
  or any CF-convention calendar string), and a ``calendar`` attribute
  (e.g. ``"standard"``).
- ``latitude`` -- a 1-D array of latitudes in decimal degrees.
- ``longitude`` -- a 1-D array of longitudes in decimal degrees.

The file must contain at least 2 time points so that velocity can be
estimated from the last two positions.

Example: creating a minimal drifter NetCDF with Python::

    import netCDF4, numpy as np
    ds = netCDF4.Dataset("drifter.nc", "w")
    ds.createDimension("time", None)  # unlimited
    t = ds.createVariable("time", "f8", ("time",))
    t.units = "hours since 2026-01-01"
    t.calendar = "standard"
    lat = ds.createVariable("latitude", "f8", ("time",))
    lon = ds.createVariable("longitude", "f8", ("time",))
    t[:] = [0.0, 1.0, 2.0]
    lat[:] = [44.60, 44.61, 44.62]
    lon[:] = [-124.50, -124.51, -124.52]
    ds.close()

Running it
----------

Install the optional dependencies and run::

    pip install 'sfmc-api[drifter]'   # installs netCDF4, numpy

    sfmc-follow --glider osu685 \\
                --follower examples/drifter_follower.py \\
                --config examples/drifter_config.yaml

To test offline with a recorded dialog log::

    sfmc-follow --glider osu685 \\
                --follower examples/drifter_follower.py \\
                --config examples/drifter_config.yaml \\
                --replay dialog.log --dry-run
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

    Opens the file at *nc_path*, reads the ``time``, ``latitude``, and
    ``longitude`` variables, and estimates the drifter's current
    velocity from the last two recorded positions.  The velocity is
    returned in metres per second (east and north components).

    The function needs at least two time points to compute a velocity.
    If the file has only one point, or if the time difference between
    the last two points is zero or negative, the function returns
    ``None`` (meaning "I cannot determine where the drifter is
    heading").

    This function is called once per surfacing, so the NetCDF file
    can be updated by an external process between surfacings.

    Args:
        nc_path: Filesystem path to the drifter NetCDF file.

    Returns:
        A tuple ``(lat, lon, vel_east_m_s, vel_north_m_s)`` where
        lat/lon are the most recent position in decimal degrees and
        vel_east/vel_north are the estimated drift velocity in m/s.
        Returns ``None`` if velocity cannot be estimated.
    """
    try:
        ds = netCDF4.Dataset(nc_path, "r")
    except (OSError, FileNotFoundError) as exc:
        logger.warning("Cannot open NetCDF file %s: %s", nc_path, exc)
        return None

    with ds:
        for var_name in ("time", "latitude", "longitude"):
            if var_name not in ds.variables:
                logger.warning("Missing variable %r in %s", var_name, nc_path)
                return None

        time_var = ds.variables["time"]
        lat = np.array(ds.variables["latitude"][:])
        lon = np.array(ds.variables["longitude"][:])

        # Convert time to datetime objects, then to hours since first.
        calendar = getattr(time_var, "calendar", "standard")
        times = netCDF4.num2date(time_var[:], time_var.units, calendar)
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
    """Estimate how long the glider needs to swim between two points.

    The glider moves through the water at a known speed, but ocean
    currents push it sideways and along its track.  This function
    projects the current vector onto the direction of travel to get
    the effective speed over ground toward the waypoint.

    For example, if the glider swims at 0.5 m/s and there is a
    0.1 m/s current pushing it toward the waypoint, the effective
    ground speed is 0.6 m/s.  If the current opposes the glider,
    effective speed drops.  A minimum effective speed of 0.05 m/s
    is enforced to avoid division by near-zero when the glider can
    barely make headway.

    Args:
        from_lat: Start latitude in decimal degrees.
        from_lon: Start longitude in decimal degrees.
        to_lat: End latitude in decimal degrees.
        to_lon: End longitude in decimal degrees.
        glider_speed: Through-water horizontal speed in m/s
            (typically 0.25--0.50 for a Slocum).
        current_vx: Eastward component of ocean current in m/s
            (from ``m_water_vx``).
        current_vy: Northward component of ocean current in m/s
            (from ``m_water_vy``).

    Returns:
        Estimated transit time in seconds.  Never less than 1.0.
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

    max_transit = 86400.0  # 24-hour ceiling
    return min(distance / effective_speed, max_transit)


class DrifterFollower(BaseFollower):
    """Follow a drifting object by generating waypoints around it.

    This is the main class you use.  It subclasses
    :class:`~sfmc_api.follower.BaseFollower` and implements the
    :meth:`on_surfacing` method.

    On each glider surfacing the follower performs these steps:

    1. Opens the NetCDF file and reads the drifter's latest position
       and estimated velocity (see :func:`_get_drifter_state`).
    2. Reads the glider's GPS fix and depth-average current from the
       :class:`~sfmc_api.dialog_parser.SurfacingEvent`.
    3. Iterates over the geometry pattern from the config.  For each
       offset (e.g. "1 km east"), it predicts the drifter's future
       position at the estimated arrival time, places the waypoint
       relative to that predicted position, and estimates the transit
       time to the waypoint using :func:`_estimate_transit_time`.
    4. Calls :func:`~sfmc_api.ma_writer.generate_goto_ma` to build a
       ``goto_l{N}.ma`` file with all the waypoints.
    5. Calls :meth:`send_files` to queue the file for upload.

    Trouble only this follower can see — the drifter feed unreadable,
    the ``.ma`` file failing to generate — is emailed to the operator
    via :meth:`~sfmc_api.follower.BaseFollower.notify` (active when
    ``sfmc-follow`` runs with ``--notify-email``; otherwise a no-op).

    Configuration keys (loaded from your ``--config`` YAML file):

    ``input`` (str)
        Path to the drifter NetCDF file.
    ``sequence_number`` (int, default 30)
        Slot number for the ``.ma`` filename (e.g. 30 produces
        ``goto_l30.ma``).
    ``geometry`` (list of [east_km, north_km])
        Waypoint offsets around the drifter in kilometres.
    ``glider.speed_horizontal`` (float, default 0.5)
        Through-water speed in m/s.
    ``list_when_wpt_dist`` (float, default 100.0)
        Waypoint-reached distance threshold in metres.

    Example usage from the command line::

        sfmc-follow --glider osu685 \\
                    --follower examples/drifter_follower.py \\
                    --config examples/drifter_config.yaml
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
            # Only this follower knows the drifter feed matters — the
            # framework sees a healthy SFMC connection.  Email the
            # operator directly.  Rate-limited per key (one email per
            # 15 min while the condition persists) and a silent no-op
            # unless sfmc-follow ran with --notify-email.
            self.notify(
                "drifter-feed-down",
                "drifter position feed unavailable",
                f"Could not read a drifter position from {nc_path}; "
                "no new waypoints were generated for this surfacing. "
                "The glider continues on its previous goto file.",
            )
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
        #
        # The drifter is moving, so naive "drifter position + offset"
        # waypoints would be stale by the time the glider arrived.
        # We compensate in two passes per waypoint:
        #
        #   Pass 1: assume the drifter has advected to "now + cumulative
        #           time so far", place a candidate waypoint, estimate
        #           the transit time to reach it, and update the
        #           cumulative time.
        #   Pass 2: redo the drifter prediction using that updated
        #           cumulative time and re-place the waypoint at the
        #           drifter's actual expected position when the glider
        #           is expected to arrive.
        #
        # A single pass is enough when transits are short relative to
        # the drifter's velocity; the second pass matters most for
        # waypoints far from the glider or when currents are strong.
        waypoints: list[tuple[float, float]] = []
        cumulative_time = 0.0
        prev_lat = glider_lat
        prev_lon = glider_lon

        # Mean Earth metres per degree latitude.  Longitude scales with
        # cos(latitude); we use the drifter's reference latitude.  Good
        # to ~0.5 % within a few hundred km, which is plenty for these
        # short transits.
        m_per_deg_lat = 111320.0
        m_per_deg_lon = 111320.0 * math.cos(math.radians(drifter_lat))

        for offset in geometry:
            east_km = float(offset[0])
            north_km = float(offset[1])

            # Pass 1: where will the drifter be after the transits we
            # have already scheduled?  Place a candidate waypoint there.
            pred_lat = drifter_lat + (drift_vy * cumulative_time) / m_per_deg_lat
            pred_lon = drifter_lon + (drift_vx * cumulative_time) / m_per_deg_lon

            # Apply the geometry pattern offset around the predicted
            # drifter position.  km_to_degrees handles the cos(lat)
            # correction for the east-km → degrees conversion.
            dlon_offset, dlat_offset = km_to_degrees(east_km, north_km, pred_lat)
            wpt_lon = pred_lon + dlon_offset
            wpt_lat = pred_lat + dlat_offset

            # How long will the glider take to fly from the previous
            # waypoint to this candidate, accounting for currents
            # pushing it sideways?
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

            # Pass 2: redo the drifter prediction now that we know how
            # long the transit takes.  This is the refinement step; we
            # re-place the waypoint at the drifter's actual expected
            # position on arrival.
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
        try:
            filename, content = generate_goto_ma(
                waypoints=waypoints,
                sequence_number=seq,
                num_legs_to_run=-2,
                list_when_wpt_dist=wpt_dist,
            )
        except Exception as exc:
            # A logic/validation failure here means the glider gets no
            # new steering this surfacing — worth an operator email, not
            # just a log line.  Re-raise so the framework also logs the
            # traceback and counts the error.
            self.notify(
                "ma-generation-failed",
                "could not generate goto .ma file",
                f"generate_goto_ma failed: {exc}\nwaypoints={waypoints!r}",
            )
            raise

        logger.info(
            "Generated %s with %d waypoints (cumulative transit %.0fs)",
            filename,
            len(waypoints),
            cumulative_time,
        )
        self.send_files(to_glider={filename: content})
