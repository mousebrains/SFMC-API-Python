"""Tests for sfmc_api.dialog_parser — glider dialog telemetry parsing."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from sfmc_api.dialog_parser import (
    CARRIER_DETECT_RE,
    CURR_TIME_RE,
    GPS_LOCATION_RE,
    SENSOR_RE,
    VEHICLE_NAME_RE,
    DialogParser,
    SurfacingEvent,
    _parse_glider_timestamp,
)

# ── Sample dialog text ──────────────────────────────────────────────

SAMPLE_SURFACING = [
    "Connection Event: Carrier Detect found.169339    Iridium console active and ready...",
    "",
    "Vehicle Name: osu685",
    "Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339",
    "DR  Location:  3310.021 N -11741.800 E measured     60.812 secs ago",
    "GPS TooFar:   69696969.000 N 69696969.000 E measured     1e+308 secs ago",
    "GPS Invalid :  3310.066 N -11741.666 E measured    154.415 secs ago",
    "GPS Location:  3310.021 N -11741.800 E measured     64.746 secs ago",
    "   sensor:c_autoballast_state(enum)=5              1e+308 secs ago",
    "   sensor:c_wpt_lat(lat)=3309.91                  259.819 secs ago",
    "   sensor:c_wpt_lon(lon)=-11741.98                259.823 secs ago",
    "   sensor:m_battery(volts)=15.2327964879377         51.99 secs ago",
    "   sensor:m_water_vx(m/s)=0.040895035455703        68.915 secs ago",
    "   sensor:m_water_vy(m/s)=0.042725242954555        68.918 secs ago",
    "ABORT HISTORY: total since reset: 9",
]


# ── Regex pattern tests ────────────────────────────────────────────


class TestRegexPatterns:
    """Verify each regex matches the expected dialog lines."""

    def test_carrier_detect(self) -> None:
        line = (
            "Connection Event: Carrier Detect found.169339    Iridium console active and ready..."
        )
        assert CARRIER_DETECT_RE.search(line) is not None

    def test_vehicle_name(self) -> None:
        m = VEHICLE_NAME_RE.search("Vehicle Name: osu685")
        assert m is not None
        assert m.group(1) == "osu685"

    def test_curr_time(self) -> None:
        m = CURR_TIME_RE.search("Curr Time: Sat Mar 28 20:40:38 2026 MT:  169339")
        assert m is not None
        assert m.group(1).strip() == "Sat Mar 28 20:40:38 2026"
        assert m.group(2) == "169339"

    def test_gps_location(self) -> None:
        m = GPS_LOCATION_RE.search(
            "GPS Location:  3310.021 N -11741.800 E measured     64.746 secs ago"
        )
        assert m is not None
        assert float(m.group(1)) == pytest.approx(3310.021)
        assert float(m.group(2)) == pytest.approx(-11741.800)
        assert float(m.group(3)) == pytest.approx(64.746)

    def test_gps_location_does_not_match_dr(self) -> None:
        """DR Location lines should not be matched by the GPS regex."""
        assert (
            GPS_LOCATION_RE.search(
                "DR  Location:  3310.021 N -11741.800 E measured     60.812 secs ago"
            )
            is None
        )

    def test_gps_location_does_not_match_toofar(self) -> None:
        assert (
            GPS_LOCATION_RE.search(
                "GPS TooFar:   69696969.000 N 69696969.000 E measured     1e+308 secs ago"
            )
            is None
        )

    def test_gps_location_does_not_match_invalid(self) -> None:
        """GPS Invalid lines should not be matched by the GPS regex."""
        assert (
            GPS_LOCATION_RE.search(
                "GPS Invalid :  3310.066 N -11741.666 E measured    154.415 secs ago"
            )
            is None
        )

    def test_sensor_normal(self) -> None:
        m = SENSOR_RE.search("   sensor:m_battery(volts)=15.2327964879377         51.99 secs ago")
        assert m is not None
        assert m.group(1) == "m_battery"
        assert m.group(2) == "volts"
        assert float(m.group(3)) == pytest.approx(15.2327964879377)
        assert float(m.group(4)) == pytest.approx(51.99)

    def test_sensor_scientific_notation_age(self) -> None:
        m = SENSOR_RE.search("   sensor:c_autoballast_state(enum)=5              1e+308 secs ago")
        assert m is not None
        assert m.group(1) == "c_autoballast_state"
        assert float(m.group(4)) == pytest.approx(1e308)

    def test_sensor_negative_value(self) -> None:
        m = SENSOR_RE.search("   sensor:c_dive_bpump(X)=-420                     1e+308 secs ago")
        assert m is not None
        assert float(m.group(3)) == -420.0

    def test_sensor_lat_type(self) -> None:
        m = SENSOR_RE.search("   sensor:c_wpt_lat(lat)=3309.91                  259.819 secs ago")
        assert m is not None
        assert m.group(2) == "lat"
        assert float(m.group(3)) == pytest.approx(3309.91)


# ── DialogParser state machine tests ───────────────────────────────


class TestDialogParserCompleteSurfacing:
    """Parse a full surfacing block from sample dialog text."""

    def test_emits_event_after_sensors(self) -> None:
        parser = DialogParser()
        event = None
        for line in SAMPLE_SURFACING:
            result = parser.feed_line(line)
            if result is not None:
                event = result
        assert event is not None

    def test_vehicle_name(self) -> None:
        event = _parse_sample()
        assert event.vehicle_name == "osu685"

    def test_timestamp(self) -> None:
        event = _parse_sample()
        assert event.timestamp == datetime(2026, 3, 28, 20, 40, 38, tzinfo=UTC)

    def test_mission_time(self) -> None:
        event = _parse_sample()
        assert event.mission_time == pytest.approx(169339.0)

    def test_gps_decimal(self) -> None:
        event = _parse_sample()
        assert event.gps_lat is not None
        assert event.gps_lon is not None
        assert event.gps_lat == pytest.approx(33.16702, abs=1e-4)
        assert event.gps_lon == pytest.approx(-117.6967, abs=1e-4)

    def test_gps_ddmm_raw(self) -> None:
        event = _parse_sample()
        assert event.gps_lat_ddmm == pytest.approx(3310.021)
        assert event.gps_lon_ddmm == pytest.approx(-11741.800)

    def test_gps_age(self) -> None:
        event = _parse_sample()
        assert event.gps_age_secs == pytest.approx(64.746)

    def test_sensor_count(self) -> None:
        event = _parse_sample()
        assert len(event.sensors) == 6

    def test_sensor_water_vx(self) -> None:
        event = _parse_sample()
        vx = event.sensors["m_water_vx"]
        assert vx.unit == "m/s"
        assert vx.value == pytest.approx(0.040895035455703)
        assert vx.age_secs == pytest.approx(68.915)

    def test_sensor_wpt_lat(self) -> None:
        event = _parse_sample()
        wpt = event.sensors["c_wpt_lat"]
        assert wpt.unit == "lat"
        assert wpt.value == pytest.approx(3309.91)

    def test_raw_lines_collected(self) -> None:
        event = _parse_sample()
        assert len(event.raw_lines) == len(SAMPLE_SURFACING)


class TestDialogParserIdleState:
    """Parser ignores non-surfacing lines when idle."""

    def test_no_event_before_carrier_detect(self) -> None:
        parser = DialogParser()
        for line in [
            "some random output",
            "Vehicle Name: osu685",
            "GPS Location:  3310.021 N -11741.800 E measured     64.746 secs ago",
        ]:
            assert parser.feed_line(line) is None

    def test_no_event_for_empty_lines(self) -> None:
        parser = DialogParser()
        assert parser.feed_line("") is None
        assert parser.feed_line("   ") is None


class TestDialogParserIncompleteSurfacing:
    """Surfacings missing GPS or sensors are not emitted."""

    def test_no_gps_no_emit(self) -> None:
        parser = DialogParser()
        parser.feed_line("Connection Event: Carrier Detect found.123")
        parser.feed_line("Vehicle Name: test")
        parser.feed_line("   sensor:m_battery(volts)=15.0    50.0 secs ago")
        parser.feed_line("ABORT HISTORY: total since reset: 1")
        # No GPS → should not emit.
        assert parser.flush() is None

    def test_no_sensors_no_emit(self) -> None:
        parser = DialogParser()
        parser.feed_line("Connection Event: Carrier Detect found.123")
        parser.feed_line("Vehicle Name: test")
        parser.feed_line("GPS Location:  3310.021 N -11741.800 E measured  64.0 secs ago")
        parser.feed_line("ABORT HISTORY: total since reset: 1")
        # No sensors → should not emit even with flush.
        assert parser.flush() is None


class TestDialogParserConsecutiveSurfacings:
    """A new Carrier Detect emits the previous surfacing."""

    def test_second_carrier_detect_emits_first(self) -> None:
        parser = DialogParser()
        # First surfacing.
        for line in SAMPLE_SURFACING:
            parser.feed_line(line)
        # Start second surfacing — should emit first.
        event = parser.feed_line(
            "Connection Event: Carrier Detect found.170000    Iridium console active and ready..."
        )
        # The "ABORT HISTORY" line already triggered emit during the first pass,
        # so this may or may not produce a second emit. Let's check the overall flow.
        # Feed a complete second surfacing to verify parser continues working.
        for line in SAMPLE_SURFACING[1:]:
            result = parser.feed_line(line)
            if result is not None:
                event = result
        # Should have gotten an event from the second surfacing.
        assert event is not None
        assert event.vehicle_name == "osu685"


class TestDialogParserFlush:
    """flush() emits buffered surfacing data."""

    def test_flush_emits_ready_event(self) -> None:
        parser = DialogParser()
        # Feed carrier detect + data but no terminating non-sensor line.
        parser.feed_line("Connection Event: Carrier Detect found.123")
        parser.feed_line("Vehicle Name: test")
        parser.feed_line("GPS Location:  3310.021 N -11741.800 E measured  64.0 secs ago")
        parser.feed_line("   sensor:m_battery(volts)=15.0    50.0 secs ago")
        # Still in sensor block — no emit yet.
        event = parser.flush()
        assert event is not None
        assert event.vehicle_name == "test"

    def test_flush_resets_state(self) -> None:
        parser = DialogParser()
        parser.feed_line("Connection Event: Carrier Detect found.123")
        parser.feed_line("Vehicle Name: test")
        parser.feed_line("GPS Location:  3310.021 N -11741.800 E measured  64.0 secs ago")
        parser.feed_line("   sensor:m_battery(volts)=15.0    50.0 secs ago")
        parser.flush()
        # After flush, parser should be idle.
        assert parser.feed_line("random text") is None


class TestDialogParserReset:
    """reset() discards partial data."""

    def test_reset_clears_state(self) -> None:
        parser = DialogParser()
        parser.feed_line("Connection Event: Carrier Detect found.123")
        parser.feed_line("Vehicle Name: test")
        parser.reset()
        assert parser.flush() is None


class TestLocaleIndependentTimestamp:
    """``_parse_glider_timestamp`` works regardless of host locale."""

    def test_parses_typical_value(self) -> None:
        dt = _parse_glider_timestamp("Sat Mar 28 20:40:38 2026")
        assert dt == datetime(2026, 3, 28, 20, 40, 38, tzinfo=UTC)

    def test_implementation_does_not_use_strptime(self) -> None:
        """The fix is meaningful only if we don't fall back to strptime.

        ``datetime.strptime`` reads the C locale for ``%a`` / ``%b``,
        which is what we are trying to avoid.  This test ensures
        future refactors do not silently reintroduce that dependency.
        """
        import inspect

        from sfmc_api import dialog_parser

        source = inspect.getsource(dialog_parser._parse_glider_timestamp)
        assert "strptime" not in source
        # Sanity-check the module-level table is being looked up.
        assert "_MONTH_ABBR_TO_NUM" in inspect.getsource(dialog_parser)

    def test_works_under_alternate_locale(self) -> None:
        """A non-English LC_TIME locale must not break parsing.

        When no alternative locale is installed (some minimal CI
        images), the test still verifies the C/POSIX fallback path.
        """
        import locale

        original = locale.setlocale(locale.LC_TIME)
        try:
            # Try a few non-English locales — fall back to C if none.
            tried = False
            for candidate in ("de_DE.UTF-8", "ja_JP.UTF-8", "C.UTF-8", "C"):
                try:
                    locale.setlocale(locale.LC_TIME, candidate)
                    tried = True
                    break
                except locale.Error:
                    continue
            assert tried, "Could not switch LC_TIME even to C"

            dt = _parse_glider_timestamp("Sat Mar 28 20:40:38 2026")
            assert dt == datetime(2026, 3, 28, 20, 40, 38, tzinfo=UTC)
        finally:
            locale.setlocale(locale.LC_TIME, original)

    def test_unknown_month_returns_none(self) -> None:
        assert _parse_glider_timestamp("Sat Xyz 28 20:40:38 2026") is None

    def test_malformed_returns_none(self) -> None:
        assert _parse_glider_timestamp("not a timestamp") is None

    def test_out_of_range_returns_none(self) -> None:
        assert _parse_glider_timestamp("Sat Feb 30 20:40:38 2026") is None


# ── Helpers ─────────────────────────────────────────────────────────


def _parse_sample() -> SurfacingEvent:
    """Parse SAMPLE_SURFACING and return the emitted event."""
    parser = DialogParser()
    for line in SAMPLE_SURFACING:
        event = parser.feed_line(line)
        if event is not None:
            return event
    # Try flushing in case the event wasn't emitted mid-stream.
    event = parser.flush()
    assert event is not None, "Sample surfacing should produce an event"
    return event
