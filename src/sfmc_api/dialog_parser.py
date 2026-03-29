"""Parse Slocum glider dialog output into structured surfacing events.

When a glider surfaces and connects via Iridium, it transmits a block of
text that includes its vehicle name, current time, GPS fix, and sensor
readings.  This module provides a state-machine parser that converts raw
dialog lines into :class:`SurfacingEvent` objects.

Typical usage::

    parser = DialogParser()
    for line in dialog_lines:
        event = parser.feed_line(line)
        if event is not None:
            print(event.vehicle_name, event.gps_lat, event.gps_lon)

The parser detects the start of a surfacing from the ``Carrier Detect
found`` connection event and emits a :class:`SurfacingEvent` once a GPS
fix and sensor readings have been collected.
"""

from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sfmc_api.coordinates import dddmm_to_decimal

# ── Data classes ────────────────────────────────────────────────────


@dataclass
class SensorReading:
    """A single sensor value from the glider's dialog output.

    Attributes:
        name: Sensor name (e.g. ``"m_water_vx"``).
        unit: Unit string from parentheses (e.g. ``"m/s"``, ``"lat"``).
        value: Numeric sensor value.
        age_secs: How many seconds ago the reading was taken.
    """

    name: str
    unit: str
    value: float
    age_secs: float


@dataclass
class SurfacingEvent:
    """Parsed telemetry from one glider surfacing.

    All fields default to ``None`` or empty so that partial surfacings
    (e.g. a connection that drops before the GPS fix) can still be
    represented.

    Attributes:
        vehicle_name: The glider's self-reported name.
        timestamp: UTC time parsed from the ``Curr Time:`` line.
        mission_time: Mission-elapsed time in seconds from ``MT:``.
        gps_lat: GPS latitude in decimal degrees (positive = north).
        gps_lon: GPS longitude in decimal degrees (positive = east).
        gps_lat_ddmm: Raw latitude in DDMM.MMMM format from dialog.
        gps_lon_ddmm: Raw longitude in DDDMM.MMMM format from dialog.
        gps_age_secs: Age of the GPS fix in seconds.
        sensors: Mapping of sensor name → :class:`SensorReading`.
        raw_lines: All dialog lines collected during this surfacing.
    """

    vehicle_name: str | None = None
    timestamp: datetime | None = None
    mission_time: float | None = None
    gps_lat: float | None = None
    gps_lon: float | None = None
    gps_lat_ddmm: float | None = None
    gps_lon_ddmm: float | None = None
    gps_age_secs: float | None = None
    sensors: dict[str, SensorReading] = field(default_factory=dict)
    raw_lines: list[str] = field(default_factory=list)


# ── Compiled regex patterns ─────────────────────────────────────────

#: Trigger: a new Iridium connection has started.
CARRIER_DETECT_RE = re.compile(r"Carrier Detect found")

#: ``Vehicle Name: osu685``
VEHICLE_NAME_RE = re.compile(r"Vehicle Name:\s*(\S+)")

#: ``Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339``
CURR_TIME_RE = re.compile(
    r"Curr Time:\s*(.+?)\s+MT:\s*([\d.]+)",
)

#: ``GPS Location:  3310.021 N -11741.800 E measured     64.746 secs ago``
#: The N/E labels are axis identifiers; the sign of the numeric value
#: carries the hemisphere (negative longitude = west).
GPS_LOCATION_RE = re.compile(
    r"GPS Location:\s*"
    r"([\-\d.]+)\s+[NS]\s+"
    r"([\-\d.]+)\s+[EW]\s+"
    r"measured\s+([\d.eE+]+)\s+secs ago",
)

#: ``   sensor:m_water_vx(m/s)=0.040895035455703        68.915 secs ago``
SENSOR_RE = re.compile(
    r"sensor:(\w+)\(([^)]+)\)=([\-\d.eE+]+)\s+"
    r"([\d.eE+]+)\s+secs ago",
)


# ── Parser ──────────────────────────────────────────────────────────


class DialogParser:
    """State-machine parser for Slocum glider dialog output.

    Feed reassembled dialog lines one at a time via :meth:`feed_line`.
    When a complete surfacing block has been collected (GPS fix plus at
    least one sensor reading), the method returns a
    :class:`SurfacingEvent`.  Otherwise it returns ``None``.

    The parser transitions through two states:

    ``IDLE``
        Waiting for a ``Carrier Detect found`` line.

    ``SURFACING``
        Collecting telemetry.  A :class:`SurfacingEvent` is emitted
        once a GPS fix and at least one sensor line have been seen
        and the next line is *not* a sensor line (e.g. the
        ``ABORT HISTORY`` line, a blank, or any other non-sensor text).
    """

    def __init__(self) -> None:
        self._state: str = "IDLE"
        self._current: SurfacingEvent | None = None
        self._has_gps: bool = False
        self._has_sensors: bool = False
        self._in_sensor_block: bool = False

    def reset(self) -> None:
        """Discard any partially collected surfacing data."""
        self._state = "IDLE"
        self._current = None
        self._has_gps = False
        self._has_sensors = False
        self._in_sensor_block = False

    def feed_line(self, line: str) -> SurfacingEvent | None:
        """Process one dialog line.

        Args:
            line: A single reassembled line of dialog output (no
                trailing newline).

        Returns:
            A :class:`SurfacingEvent` when a complete surfacing block
            has been parsed, otherwise ``None``.
        """
        if self._state == "IDLE":
            return self._handle_idle(line)
        else:
            return self._handle_surfacing(line)

    # ── State handlers ──────────────────────────────────────────

    def _handle_idle(self, line: str) -> SurfacingEvent | None:
        if CARRIER_DETECT_RE.search(line):
            self._state = "SURFACING"
            self._current = SurfacingEvent()
            self._current.raw_lines.append(line)
            self._has_gps = False
            self._has_sensors = False
            self._in_sensor_block = False
        return None

    def _handle_surfacing(self, line: str) -> SurfacingEvent | None:
        assert self._current is not None

        # Check if a new surfacing starts (emit current if ready).
        if CARRIER_DETECT_RE.search(line):
            result = self._try_emit()
            # Start fresh surfacing.
            self._current = SurfacingEvent()
            self._current.raw_lines.append(line)
            self._has_gps = False
            self._has_sensors = False
            self._in_sensor_block = False
            return result

        self._current.raw_lines.append(line)

        # Try to match known patterns.
        if self._try_vehicle_name(line):
            return None
        if self._try_curr_time(line):
            return None
        if self._try_gps_location(line):
            return None
        if self._try_sensor(line):
            return None

        # Non-sensor line after we were in the sensor block:
        # emit if we have enough data.
        if self._in_sensor_block and self._has_gps and self._has_sensors:
            self._in_sensor_block = False
            return self._try_emit()

        return None

    # ── Pattern matchers ────────────────────────────────────────

    def _try_vehicle_name(self, line: str) -> bool:
        m = VEHICLE_NAME_RE.search(line)
        if m and self._current is not None:
            self._current.vehicle_name = m.group(1)
            return True
        return False

    def _try_curr_time(self, line: str) -> bool:
        m = CURR_TIME_RE.search(line)
        if m and self._current is not None:
            time_str = m.group(1).strip()
            try:
                dt = datetime.strptime(time_str, "%a %b %d %H:%M:%S %Y")
                self._current.timestamp = dt.replace(tzinfo=UTC)
            except ValueError:
                pass  # Unparseable time -- keep going.
            with contextlib.suppress(ValueError):
                self._current.mission_time = float(m.group(2))
            return True
        return False

    def _try_gps_location(self, line: str) -> bool:
        m = GPS_LOCATION_RE.search(line)
        if m and self._current is not None:
            lat_ddmm = float(m.group(1))
            lon_ddmm = float(m.group(2))
            age = float(m.group(3))
            self._current.gps_lat_ddmm = lat_ddmm
            self._current.gps_lon_ddmm = lon_ddmm
            self._current.gps_lat = dddmm_to_decimal(lat_ddmm)
            self._current.gps_lon = dddmm_to_decimal(lon_ddmm)
            self._current.gps_age_secs = age
            self._has_gps = True
            return True
        return False

    def _try_sensor(self, line: str) -> bool:
        m = SENSOR_RE.search(line)
        if m and self._current is not None:
            name = m.group(1)
            unit = m.group(2)
            value = float(m.group(3))
            age = float(m.group(4))
            self._current.sensors[name] = SensorReading(
                name=name,
                unit=unit,
                value=value,
                age_secs=age,
            )
            self._has_sensors = True
            self._in_sensor_block = True
            return True
        return False

    # ── Helpers ─────────────────────────────────────────────────

    def _try_emit(self) -> SurfacingEvent | None:
        """Return the current event if it has GPS + sensors, else None."""
        if self._has_gps and self._has_sensors and self._current is not None:
            event = self._current
            self._current = None
            self._state = "IDLE"
            self._has_gps = False
            self._has_sensors = False
            self._in_sensor_block = False
            return event
        return None

    def flush(self) -> SurfacingEvent | None:
        """Emit any buffered surfacing data.

        Call this when the dialog stream ends to avoid losing a
        surfacing that was never followed by a non-sensor line.

        Returns:
            A :class:`SurfacingEvent` if sufficient data was collected,
            otherwise ``None``.
        """
        if self._state == "SURFACING":
            result = self._try_emit()
            self.reset()
            return result
        return None
