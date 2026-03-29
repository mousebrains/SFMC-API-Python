"""Coordinate conversion utilities for Slocum glider navigation.

Slocum gliders use a degrees-and-decimal-minutes format (DDMM.MMMM for
latitude, DDDMM.MMMM for longitude) in their dialog output and mission
plan files.  This module converts between that format and standard
decimal degrees.

Quick reference::

    # DDMM → decimal
    >>> dddmm_to_decimal(3310.021)   #  33° 10.021'
    33.167016666...

    >>> dddmm_to_decimal(-11741.800)  # -117° 41.800'
    -117.696666...

    # decimal → DDMM
    >>> decimal_to_dddmm(33.167017)
    3310.021...

    >>> decimal_to_dddmm(-117.696667)
    -11741.800...

    # km offsets → degree offsets at a given latitude
    >>> km_to_degrees(1.0, 1.0, 33.0)
    (0.01074..., 0.00899...)   # (delta_lon, delta_lat)
"""

from __future__ import annotations

import math

# Earth radius in km (WGS-84 mean radius)
_EARTH_RADIUS_KM = 6371.0

# Degrees per km of latitude (constant, independent of position)
_DEG_PER_KM_LAT = 1.0 / (math.pi / 180.0 * _EARTH_RADIUS_KM)


def dddmm_to_decimal(dddmm: float) -> float:
    """Convert a DDDMM.MMMM coordinate to decimal degrees.

    The sign of the input is preserved in the output:
    positive values remain positive, negative values remain negative.

    Args:
        dddmm: Coordinate in degrees-and-decimal-minutes format.
            For latitude this is DDMM.MMMM (e.g. ``3310.021`` for
            33° 10.021' N).  For longitude this is DDDMM.MMMM
            (e.g. ``-11741.800`` for 117° 41.800' W).

    Returns:
        The coordinate in decimal degrees.

    Examples:
        >>> dddmm_to_decimal(3310.021)
        33.167016666666666
        >>> dddmm_to_decimal(-11741.800)
        -117.69666666666667
    """
    sign = -1.0 if dddmm < 0 else 1.0
    absolute = abs(dddmm)
    degrees = int(absolute / 100.0)
    minutes = absolute - degrees * 100.0
    return sign * (degrees + minutes / 60.0)


def decimal_to_dddmm(decimal_deg: float) -> float:
    """Convert decimal degrees to DDDMM.MMMM format.

    The sign of the input is preserved: negative decimal degrees
    produce a negative DDDMM value.

    Args:
        decimal_deg: Coordinate in decimal degrees (e.g. ``33.167017``
            or ``-117.696667``).

    Returns:
        The coordinate in DDDMM.MMMM format.

    Examples:
        >>> decimal_to_dddmm(33.167017)
        3310.021...
        >>> decimal_to_dddmm(-117.696667)
        -11741.800...
    """
    sign = -1.0 if decimal_deg < 0 else 1.0
    absolute = abs(decimal_deg)
    degrees = int(absolute)
    minutes = (absolute - degrees) * 60.0
    return sign * (degrees * 100.0 + minutes)


def km_to_degrees(
    east_km: float,
    north_km: float,
    latitude_deg: float,
) -> tuple[float, float]:
    """Convert kilometre offsets to degree offsets at a given latitude.

    Uses a simple spherical-Earth approximation, which is accurate to
    better than 0.5 % for offsets under ~100 km.

    Args:
        east_km: Eastward offset in kilometres (positive = east).
        north_km: Northward offset in kilometres (positive = north).
        latitude_deg: Reference latitude in decimal degrees, used to
            scale the east-west offset.

    Returns:
        A ``(delta_lon_deg, delta_lat_deg)`` tuple in decimal degrees.

    Examples:
        >>> dlon, dlat = km_to_degrees(1.0, 1.0, 33.0)
        >>> round(dlat, 6)
        0.008993
    """
    delta_lat = north_km * _DEG_PER_KM_LAT
    cos_lat = math.cos(math.radians(latitude_deg))
    # Near the poles, east-west distance is effectively zero.
    delta_lon = 0.0 if cos_lat < 1e-10 else east_km * _DEG_PER_KM_LAT / cos_lat
    return delta_lon, delta_lat
