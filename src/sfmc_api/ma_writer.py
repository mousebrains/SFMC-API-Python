"""Generate Slocum glider mission-argument (``.ma``) files.

What are .ma files?
-------------------

A ``.ma`` (mission-argument) file is a small text file that supplies
parameters to one of the glider's built-in behaviors.  Think of it
as a configuration file that tells the glider *where to go* or *how
to dive*.

The most common .ma file is a **goto list**: a file named
``goto_l{N}.ma`` that contains a list of waypoints for the
``goto_list`` behavior.  When SFMC delivers this file to the glider,
the ``goto_list`` behavior reads it and starts navigating to the
waypoints in sequence.

How goto_list works on the glider
---------------------------------

The ``goto_list`` behavior is one of several built-in navigation
behaviors in the Slocum firmware.  Here is what happens when the
glider receives a new ``goto_l{N}.ma`` file:

1. On the next dive cycle, the glider's mission file (``.mi``)
   triggers the ``goto_list`` behavior with an ``args_from_file``
   directive that points to slot *N*.
2. The behavior reads the ``.ma`` file and gets the list of
   waypoints plus parameters like ``num_legs_to_run`` and
   ``initial_wpt``.
3. By default (``initial_wpt = -2``), the glider heads for the
   *closest* waypoint first.
4. Once the glider comes within ``list_when_wpt_dist`` metres of a
   waypoint, it considers that waypoint "reached" and moves on to
   the next one.
5. After traversing all waypoints (with ``num_legs_to_run = -2``,
   meaning "once through"), the behavior completes and the glider
   follows whatever the mission says to do next (often: surface and
   check for new files).

This means your follower can update the waypoints every surfacing,
and the glider will always fly the freshest set of waypoints.

Example::

    from sfmc_api.ma_writer import generate_goto_ma

    filename, content = generate_goto_ma(
        waypoints=[(-117.6967, 33.1670), (-117.6900, 33.1750)],
        sequence_number=30,
    )
    # filename == "goto_l30.ma"
    # content is a string ready to upload to the "to-glider" folder.

File format reference
---------------------

See ``maFiles/goto_l90.ma`` in this repository for a real example
written by SFMC.  The structure is::

    behavior_name=goto_list
    # Written by ... on UTC: <timestamp>
    # goto_l<N>.ma

    <start:b_arg>
        b_arg: num_legs_to_run(nodim) -2
        ...
    <end:b_arg>
    <start:waypoints>
    <lon_dddmm>\\t<lat_ddmm>
    ...
    <end:waypoints>

Waypoints are tab-separated ``longitude  latitude`` in DDDMM.MMMM /
DDMM.MMMM format (the same format used by sensor values ``c_wpt_lon``
and ``c_wpt_lat``).  This module handles the conversion from decimal
degrees to DDDMM format automatically.
"""

from __future__ import annotations

import datetime
import math

from sfmc_api.coordinates import decimal_to_dddmm

#: Maximum number of waypoints the ``goto_list`` behavior supports.
#: The firmware allows up to 8 (indices 0-7), but slot 7 is sometimes
#: reserved for special use, so we cap at 7 by default.
MAX_WAYPOINTS = 7


def _validate_waypoints(waypoints: list[tuple[float, float]]) -> None:
    """Validate every waypoint is a finite (lon, lat) pair in range.

    Catches the most common follower bugs before they reach the glider:
    swapped lat/lon, NaN/inf from a divide-by-zero, or out-of-range
    values from a unit-conversion mistake.
    """
    for i, wpt in enumerate(waypoints):
        try:
            lon_deg, lat_deg = wpt
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Waypoint {i} must be a (longitude, latitude) pair, got {wpt!r}"
            ) from exc
        if not (math.isfinite(lon_deg) and math.isfinite(lat_deg)):
            raise ValueError(f"Waypoint {i} contains NaN or infinity: ({lon_deg}, {lat_deg})")
        if not -180.0 <= lon_deg <= 180.0:
            raise ValueError(
                f"Waypoint {i} longitude {lon_deg} is outside [-180, 180]. "
                "Tuples are (longitude, latitude) — check for swapped values."
            )
        if not -90.0 <= lat_deg <= 90.0:
            raise ValueError(
                f"Waypoint {i} latitude {lat_deg} is outside [-90, 90]. "
                "Tuples are (longitude, latitude) — check for swapped values."
            )


def generate_goto_ma(
    waypoints: list[tuple[float, float]],
    sequence_number: int,
    *,
    num_legs_to_run: int = -2,
    initial_wpt: int = -2,
    list_stop_when: int = 7,
    list_when_wpt_dist: float = 100.0,
) -> tuple[str, str]:
    """Generate a ``goto_l{N}.ma`` file for the ``goto_list`` behavior.

    Args:
        waypoints: List of ``(longitude, latitude)`` tuples in
            **decimal degrees** (e.g. ``(-117.697, 33.167)``).
            Maximum :data:`MAX_WAYPOINTS` entries.
        sequence_number: The sequence number *N* that determines the
            filename (``goto_l{N}.ma``) and which ``args_from_file``
            slot the glider reads.
        num_legs_to_run: How to sequence through waypoints.
            ``-2`` traverses the list once (default).
            ``-1`` loops forever.
        initial_wpt: Which waypoint to head for first.
            ``-2`` starts at the closest waypoint (default).
            ``-1`` starts one after the last achieved.
        list_stop_when: Waypoint-reached condition.
            ``7`` means stop when within *list_when_wpt_dist* metres
            of the waypoint (default).
        list_when_wpt_dist: Distance threshold in metres used when
            *list_stop_when* is ``7``.  Default is ``100.0`` m.

    Returns:
        A ``(filename, content)`` tuple where *filename* is
        ``"goto_l{N}.ma"`` and *content* is the complete file text.

    Raises:
        ValueError: If *waypoints* is empty, exceeds
            :data:`MAX_WAYPOINTS`, or contains a value with an
            out-of-range latitude/longitude or NaN/inf.
    """
    if not waypoints:
        raise ValueError("At least one waypoint is required")
    if len(waypoints) > MAX_WAYPOINTS:
        raise ValueError(f"Too many waypoints: {len(waypoints)} (maximum is {MAX_WAYPOINTS})")
    _validate_waypoints(waypoints)

    filename = f"goto_l{sequence_number}.ma"
    now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")

    lines: list[str] = [
        "behavior_name=goto_list",
        f"# Written by sfmc-follow on UTC: {now}",
        f"# {filename}",
        "",
        "<start:b_arg>",
        f"\tb_arg: num_legs_to_run(nodim) {num_legs_to_run}",
        "\tb_arg: start_when(enum) 0 # BAW_IMMEDIATELY",
        f"\tb_arg: list_stop_when(enum) {list_stop_when} # BAW_WHEN_WPT_DIST",
        f"\tb_arg: list_when_wpt_dist(m) {list_when_wpt_dist}",
        f"\tb_arg: initial_wpt(enum) {initial_wpt} # Closest",
        f"\tb_arg: num_waypoints(nodim) {len(waypoints)}",
        "<end:b_arg>",
        "<start:waypoints>",
    ]

    for lon_deg, lat_deg in waypoints:
        lon_ddmm = decimal_to_dddmm(lon_deg)
        lat_ddmm = decimal_to_dddmm(lat_deg)
        lines.append(f"{lon_ddmm:.4f}\t{lat_ddmm:.4f}")

    lines.append("<end:waypoints>")
    lines.append("")  # Trailing newline.

    return filename, "\n".join(lines)
