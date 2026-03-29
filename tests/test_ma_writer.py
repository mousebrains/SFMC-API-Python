"""Tests for sfmc_api.ma_writer — goto_l*.ma file generation."""

from __future__ import annotations

import re

import pytest

from sfmc_api.coordinates import decimal_to_dddmm
from sfmc_api.ma_writer import MAX_WAYPOINTS, generate_goto_ma


class TestGenerateGotoMa:
    """Verify the structure and content of generated .ma files."""

    def test_filename(self) -> None:
        filename, _ = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        assert filename == "goto_l30.ma"

    def test_filename_sequence_90(self) -> None:
        filename, _ = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=90,
        )
        assert filename == "goto_l90.ma"

    def test_behavior_name_header(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        assert content.startswith("behavior_name=goto_list\n")

    def test_written_by_comment(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        lines = content.split("\n")
        assert lines[1].startswith("# Written by sfmc-follow on UTC:")

    def test_filename_comment(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        lines = content.split("\n")
        assert lines[2] == "# goto_l30.ma"

    def test_b_arg_block_structure(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        assert "<start:b_arg>" in content
        assert "<end:b_arg>" in content

    def test_waypoints_block_structure(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        assert "<start:waypoints>" in content
        assert "<end:waypoints>" in content

    def test_num_legs_default(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        assert "\tb_arg: num_legs_to_run(nodim) -2\n" in content

    def test_num_legs_custom(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
            num_legs_to_run=-1,
        )
        assert "\tb_arg: num_legs_to_run(nodim) -1\n" in content

    def test_num_waypoints_matches(self) -> None:
        wpts = [(-117.697, 33.167), (-117.690, 33.175), (-117.700, 33.160)]
        _, content = generate_goto_ma(waypoints=wpts, sequence_number=30)
        assert "\tb_arg: num_waypoints(nodim) 3\n" in content

    def test_list_when_wpt_dist_default(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
        )
        assert "\tb_arg: list_when_wpt_dist(m) 100.0\n" in content

    def test_list_when_wpt_dist_custom(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.697, 33.167)],
            sequence_number=30,
            list_when_wpt_dist=250.0,
        )
        assert "\tb_arg: list_when_wpt_dist(m) 250.0\n" in content


class TestWaypointFormatting:
    """Verify waypoints are in tab-separated lon\\tlat DDDMM.MMMM format."""

    def test_waypoint_format(self) -> None:
        lon_deg, lat_deg = -117.696667, 33.167017
        _, content = generate_goto_ma(
            waypoints=[(lon_deg, lat_deg)],
            sequence_number=30,
        )
        # Extract the waypoint line between the markers.
        wpt_lines = _extract_waypoint_lines(content)
        assert len(wpt_lines) == 1

        parts = wpt_lines[0].split("\t")
        assert len(parts) == 2

        lon_ddmm = float(parts[0])
        lat_ddmm = float(parts[1])
        assert lon_ddmm == pytest.approx(decimal_to_dddmm(lon_deg), abs=0.001)
        assert lat_ddmm == pytest.approx(decimal_to_dddmm(lat_deg), abs=0.001)

    def test_multiple_waypoints(self) -> None:
        wpts = [
            (-117.697, 33.167),
            (-117.690, 33.175),
            (-117.700, 33.160),
            (-117.695, 33.170),
        ]
        _, content = generate_goto_ma(waypoints=wpts, sequence_number=30)
        wpt_lines = _extract_waypoint_lines(content)
        assert len(wpt_lines) == 4

    def test_waypoint_lon_before_lat(self) -> None:
        """Longitude comes first (x), latitude second (y)."""
        lon_deg, lat_deg = -124.0, 44.5
        _, content = generate_goto_ma(
            waypoints=[(lon_deg, lat_deg)],
            sequence_number=30,
        )
        wpt_lines = _extract_waypoint_lines(content)
        parts = wpt_lines[0].split("\t")
        # Longitude is large negative, latitude is moderate positive.
        assert float(parts[0]) < 0  # lon (west)
        assert float(parts[1]) > 0  # lat (north)

    def test_four_decimal_places(self) -> None:
        _, content = generate_goto_ma(
            waypoints=[(-117.696667, 33.167017)],
            sequence_number=30,
        )
        wpt_lines = _extract_waypoint_lines(content)
        # Each value should have exactly 4 decimal places.
        for part in wpt_lines[0].split("\t"):
            assert re.match(r"-?\d+\.\d{4}$", part), f"Bad format: {part!r}"


class TestValidation:
    """Edge cases and error handling."""

    def test_empty_waypoints_raises(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            generate_goto_ma(waypoints=[], sequence_number=30)

    def test_too_many_waypoints_raises(self) -> None:
        wpts = [(0.0, 0.0)] * (MAX_WAYPOINTS + 1)
        with pytest.raises(ValueError, match="Too many"):
            generate_goto_ma(waypoints=wpts, sequence_number=30)

    def test_max_waypoints_ok(self) -> None:
        wpts = [(float(i), float(i)) for i in range(MAX_WAYPOINTS)]
        filename, content = generate_goto_ma(waypoints=wpts, sequence_number=30)
        assert filename == "goto_l30.ma"
        wpt_lines = _extract_waypoint_lines(content)
        assert len(wpt_lines) == MAX_WAYPOINTS

    def test_matches_goto_l90_structure(self) -> None:
        """Output structure should match the real goto_l90.ma from maFiles/."""
        _, content = generate_goto_ma(
            waypoints=[(-124.102, 44.655), (-125.129, 44.656)],
            sequence_number=90,
            num_legs_to_run=-1,
            initial_wpt=-1,
        )
        lines = content.strip().split("\n")
        # Line 0: behavior_name
        assert lines[0] == "behavior_name=goto_list"
        # Find b_arg block.
        assert any("<start:b_arg>" in ln for ln in lines)
        assert any("<end:b_arg>" in ln for ln in lines)
        # Find waypoints block.
        assert any("<start:waypoints>" in ln for ln in lines)
        assert any("<end:waypoints>" in ln for ln in lines)


# ── Helpers ─────────────────────────────────────────────────────────


def _extract_waypoint_lines(content: str) -> list[str]:
    """Return the lines between <start:waypoints> and <end:waypoints>."""
    lines = content.split("\n")
    start = lines.index("<start:waypoints>")
    end = lines.index("<end:waypoints>")
    return [ln for ln in lines[start + 1 : end] if ln.strip()]
