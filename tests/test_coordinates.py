"""Tests for sfmc_api.coordinates — DDDMM ↔ decimal degree conversions."""

from __future__ import annotations

import math

import pytest

from sfmc_api.coordinates import (
    _DEG_PER_KM_LAT,
    dddmm_to_decimal,
    decimal_to_dddmm,
    km_to_degrees,
)

# ── dddmm_to_decimal ───────────────────────────────────────────────


class TestDddmmToDecimal:
    """Convert DDDMM.MMMM → decimal degrees."""

    def test_positive_latitude(self) -> None:
        # 33° 10.021'  →  33 + 10.021/60
        assert dddmm_to_decimal(3310.021) == pytest.approx(33.167016667, abs=1e-7)

    def test_negative_longitude(self) -> None:
        # -117° 41.800'  →  -(117 + 41.800/60)
        assert dddmm_to_decimal(-11741.800) == pytest.approx(-117.696667, abs=1e-5)

    def test_zero(self) -> None:
        assert dddmm_to_decimal(0.0) == 0.0

    def test_exact_degree(self) -> None:
        # 4500.000 = 45° 00.000' = 45.0
        assert dddmm_to_decimal(4500.0) == 45.0

    def test_southern_latitude(self) -> None:
        # -3310.021 = 33° 10.021' S
        assert dddmm_to_decimal(-3310.021) == pytest.approx(-33.167016667, abs=1e-7)

    def test_small_longitude(self) -> None:
        # 130.5 = 1° 30.5' = 1.508333...
        assert dddmm_to_decimal(130.5) == pytest.approx(1.508333, abs=1e-5)


# ── decimal_to_dddmm ───────────────────────────────────────────────


class TestDecimalToDddmm:
    """Convert decimal degrees → DDDMM.MMMM."""

    def test_positive_latitude(self) -> None:
        result = decimal_to_dddmm(33.167017)
        assert result == pytest.approx(3310.021, abs=0.001)

    def test_negative_longitude(self) -> None:
        result = decimal_to_dddmm(-117.696667)
        assert result == pytest.approx(-11741.800, abs=0.001)

    def test_zero(self) -> None:
        assert decimal_to_dddmm(0.0) == 0.0

    def test_exact_degree(self) -> None:
        assert decimal_to_dddmm(45.0) == 4500.0

    def test_round_trip_positive(self) -> None:
        original = 3310.021
        assert decimal_to_dddmm(dddmm_to_decimal(original)) == pytest.approx(original, abs=1e-6)

    def test_round_trip_negative(self) -> None:
        original = -11741.800
        assert decimal_to_dddmm(dddmm_to_decimal(original)) == pytest.approx(original, abs=1e-6)


# ── km_to_degrees ───────────────────────────────────────────────────


class TestKmToDegrees:
    """Convert km offsets to degree offsets."""

    def test_north_only(self) -> None:
        dlon, dlat = km_to_degrees(0.0, 1.0, 33.0)
        assert dlon == 0.0
        assert dlat == pytest.approx(_DEG_PER_KM_LAT, abs=1e-8)

    def test_east_only_equator(self) -> None:
        # At the equator, cos(0) = 1, so east offset equals north offset.
        dlon, dlat = km_to_degrees(1.0, 0.0, 0.0)
        assert dlat == 0.0
        assert dlon == pytest.approx(_DEG_PER_KM_LAT, abs=1e-8)

    def test_east_scales_with_latitude(self) -> None:
        # At 60 deg lat, cos(60) = 0.5, so 1 km east = 2x the degree offset.
        dlon_eq, _ = km_to_degrees(1.0, 0.0, 0.0)
        dlon_60, _ = km_to_degrees(1.0, 0.0, 60.0)
        assert dlon_60 == pytest.approx(dlon_eq / math.cos(math.radians(60.0)), abs=1e-8)

    def test_near_pole(self) -> None:
        # At the pole, east offset should be clamped to zero.
        dlon, dlat = km_to_degrees(1.0, 1.0, 90.0)
        assert dlon == 0.0
        assert dlat == pytest.approx(_DEG_PER_KM_LAT, abs=1e-8)

    def test_negative_offsets(self) -> None:
        dlon, dlat = km_to_degrees(-2.0, -3.0, 45.0)
        assert dlon < 0
        assert dlat < 0

    def test_symmetry(self) -> None:
        dlon_pos, dlat_pos = km_to_degrees(1.0, 1.0, 33.0)
        dlon_neg, dlat_neg = km_to_degrees(-1.0, -1.0, 33.0)
        assert dlon_neg == pytest.approx(-dlon_pos, abs=1e-10)
        assert dlat_neg == pytest.approx(-dlat_pos, abs=1e-10)
