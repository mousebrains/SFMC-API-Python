"""Generate Slocum glider mission-argument (``.ma``) files.

This module creates ``goto_l{N}.ma`` files that tell a glider's
``goto_list`` behavior where to navigate.  The output matches the
format produced by SFMC and consumed by the Slocum glider firmware.

Example::

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
and ``c_wpt_lat``).
"""

from __future__ import annotations

import datetime

from sfmc_api.coordinates import decimal_to_dddmm

#: Maximum number of waypoints the ``goto_list`` behavior supports.
#: The firmware allows up to 8 (indices 0-7), but slot 7 is sometimes
#: reserved for special use, so we cap at 7 by default.
MAX_WAYPOINTS = 7


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
        ValueError: If *waypoints* is empty or exceeds
            :data:`MAX_WAYPOINTS`.
    """
    if not waypoints:
        raise ValueError("At least one waypoint is required")
    if len(waypoints) > MAX_WAYPOINTS:
        raise ValueError(f"Too many waypoints: {len(waypoints)} (maximum is {MAX_WAYPOINTS})")

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
