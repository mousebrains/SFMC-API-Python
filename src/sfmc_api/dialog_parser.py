"""Parse Slocum glider dialog output into structured surfacing events.

When a Slocum glider surfaces and connects to shore via Iridium
satellite, it transmits a block of plain text called "dialog output."
This text contains the glider's name, current time, GPS fix, and a
list of sensor readings.  This module provides a state-machine parser
that converts that raw text into :class:`SurfacingEvent` objects you
can work with in Python.

What glider dialog output looks like
-------------------------------------

Here is a simplified sample of what the glider transmits each time
it surfaces (the real output has more lines, but these are the key
ones the parser extracts)::

    Carrier Detect found
    Vehicle Name: osu685
    Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339
    GPS Location:  4439.300 N -12406.120 E measured     64.746 secs ago
    sensor:m_water_vx(m/s)=0.040895           68.915 secs ago
    sensor:m_water_vy(m/s)=-0.018732          68.915 secs ago
    sensor:m_battery(volts)=10.5117           10.000 secs ago
    sensor:m_vacuum(inHg)=7.89100              2.000 secs ago
    ABORT HISTORY: ...

The parser watches for these patterns:

``Carrier Detect found``
    Signals the start of a new surfacing.  The parser begins
    collecting data.

``Vehicle Name: osu685``
    The glider's self-reported name.  Stored in
    ``event.vehicle_name``.

``Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339``
    The glider's onboard clock (UTC) and mission elapsed time in
    seconds.  Stored in ``event.timestamp`` (a ``datetime``) and
    ``event.mission_time`` (a float).

``GPS Location:  4439.300 N -12406.120 E measured  64.746 secs ago``
    The GPS fix in DDMM.MMMM format.  The parser converts this to
    decimal degrees and stores it in ``event.gps_lat`` and
    ``event.gps_lon``.  The raw DDMM values are also available as
    ``event.gps_lat_ddmm`` and ``event.gps_lon_ddmm``.  The fix
    age (how many seconds ago the GPS was acquired) is stored in
    ``event.gps_age_secs``.

``sensor:m_water_vx(m/s)=0.040895  68.915 secs ago``
    A sensor reading.  The parser extracts the sensor name
    (``m_water_vx``), unit (``m/s``), numeric value (``0.040895``),
    and age (``68.915``).  All sensor readings are stored in
    ``event.sensors`` as a dict mapping sensor name to
    :class:`SensorReading`.

The parser emits a :class:`SurfacingEvent` once it has collected a
GPS fix and at least one sensor reading, and then encounters a
non-sensor line (such as ``ABORT HISTORY``).  Partial surfacings
(e.g. if the Iridium connection drops before the GPS line) are
silently discarded.

Iridium corruption can garble any line, so a line that matches a
pattern but yields unparseable or physically impossible numbers
(DDMM minutes ≥ 60, |latitude| > 90°, |longitude| > 180°, absurd fix
age, non-finite sensor value) is logged and dropped rather than
raised or stored — a corrupt fix must neither kill the service nor
steer the glider.

Typical usage::

    parser = DialogParser()
    for line in dialog_lines:
        event = parser.feed_line(line)
        if event is not None:
            print(event.vehicle_name, event.gps_lat, event.gps_lon)
"""

from __future__ import annotations

import contextlib
import logging
import math
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sfmc_api.coordinates import dddmm_to_decimal

logger = logging.getLogger(__name__)

#: Upper bound on a believable ``secs ago`` GPS fix age.  The glider
#: emits ~1.8e308 as a no-fix sentinel, and Iridium corruption can
#: produce arbitrary in-regex garbage; anything this old is not a fix.
_GPS_AGE_MAX_SECS = 1e8


def _try_float(text: str) -> float | None:
    """Convert regex-captured numeric text, or ``None`` if degenerate.

    The permissive character classes in the dialog patterns can match
    strings ``float`` rejects (``4439..300``, ``-e+``) when Iridium
    corrupts a line, and those must never raise out of the parser.
    """
    try:
        return float(text)
    except ValueError:
        return None


def _valid_ddmm(ddmm: float, max_degrees: int) -> bool:
    """True if a DDMM.MMMM value is a physically possible coordinate.

    Requires finite input, minutes < 60, and degrees within
    ``±max_degrees`` (the pole/antimeridian itself only with zero
    minutes).  A single corrupted digit usually produces a value that
    still matches the GPS regex but fails one of these bounds.
    """
    if not math.isfinite(ddmm):
        return False
    degrees, minutes = divmod(abs(ddmm), 100.0)
    if minutes >= 60.0:
        return False
    return degrees < max_degrees or (degrees == max_degrees and minutes == 0.0)


# The glider firmware emits English month abbreviations regardless of
# the host's locale.  Mapping them explicitly avoids ``strptime``'s
# locale-dependent behaviour, which silently fails on non-English
# systems (German, Japanese, etc.).
_MONTH_ABBR_TO_NUM = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}  # fmt: skip

#: ``Sat Mar 28 20:40:38 2026`` — weekday + month + day + time + year.
_CURR_TIME_VALUE_RE = re.compile(
    r"^\S+\s+(?P<mon>[A-Za-z]{3})\s+(?P<day>\d{1,2})\s+"
    r"(?P<hour>\d{1,2}):(?P<min>\d{1,2}):(?P<sec>\d{1,2})\s+"
    r"(?P<year>\d{4})$"
)


def _parse_glider_timestamp(value: str) -> datetime | None:
    """Parse a ``Curr Time:`` value, locale-independent.

    Returns ``None`` for any malformed input rather than raising — the
    caller continues collecting other fields when the timestamp is
    unreadable.
    """
    m = _CURR_TIME_VALUE_RE.match(value.strip())
    if not m:
        return None
    month = _MONTH_ABBR_TO_NUM.get(m.group("mon"))
    if month is None:
        return None
    try:
        return datetime(
            year=int(m.group("year")),
            month=month,
            day=int(m.group("day")),
            hour=int(m.group("hour")),
            minute=int(m.group("min")),
            second=int(m.group("sec")),
            tzinfo=UTC,
        )
    except ValueError:
        return None


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
            parsed = _parse_glider_timestamp(time_str)
            if parsed is not None:
                self._current.timestamp = parsed
            with contextlib.suppress(ValueError):
                self._current.mission_time = float(m.group(2))
            return True
        return False

    def _try_gps_location(self, line: str) -> bool:
        m = GPS_LOCATION_RE.search(line)
        if not m or self._current is None:
            return False
        lat_ddmm = _try_float(m.group(1))
        lon_ddmm = _try_float(m.group(2))
        age = _try_float(m.group(3))
        if (
            lat_ddmm is None
            or lon_ddmm is None
            or age is None
            or not _valid_ddmm(lat_ddmm, 90)
            or not _valid_ddmm(lon_ddmm, 180)
            or not 0.0 <= age <= _GPS_AGE_MAX_SECS
        ):
            # Consumed but not stored: a garbled fix must neither raise
            # nor steer, and must not terminate the sensor block.
            logger.warning("Rejected implausible GPS line: %r", line)
            return True
        self._current.gps_lat_ddmm = lat_ddmm
        self._current.gps_lon_ddmm = lon_ddmm
        self._current.gps_lat = dddmm_to_decimal(lat_ddmm)
        self._current.gps_lon = dddmm_to_decimal(lon_ddmm)
        self._current.gps_age_secs = age
        self._has_gps = True
        return True

    def _try_sensor(self, line: str) -> bool:
        m = SENSOR_RE.search(line)
        if not m or self._current is None:
            return False
        value = _try_float(m.group(3))
        age = _try_float(m.group(4))
        if value is None or age is None or not math.isfinite(value):
            logger.warning("Rejected unparseable sensor line: %r", line)
            return True
        self._current.sensors[m.group(1)] = SensorReading(
            name=m.group(1),
            unit=m.group(2),
            value=value,
            age_secs=age,
        )
        self._has_sensors = True
        self._in_sensor_block = True
        return True

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
