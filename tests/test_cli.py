"""Tests for sfmc_api.cli."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sfmc_api.cli import (
    _GLIDER_ONLY,
    _PLAN_UPLOAD,
    _STREAM,
    _call_method,
    _print_json,
    build_parser,
)


class TestBuildParser:
    def test_version(self) -> None:
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--version"])

    def test_auth_subcommand(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["auth"])
        assert args.command == "auth"

    def test_glider_details(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["get-glider-details", "myglider"])
        assert args.command == "get-glider-details"
        assert args.glider_name == "myglider"

    def test_compact_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--compact", "auth"])
        assert args.compact is True

    def test_config_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--config", "/tmp/c.json", "auth"])
        assert str(args.config) == "/tmp/c.json"

    def test_no_command_fails(self) -> None:
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_folder_file_listing_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "get-folder-file-listing",
                "g1",
                "from-glider",
                "--page",
                "3",
                "--filter",
                "*.sbd",
                "--last-modified-after",
                "202601010000",
            ]
        )
        assert args.glider_name == "g1"
        assert args.folder == "from-glider"
        assert args.page == 3
        assert args.filter == "*.sbd"
        assert args.last_modified_after == "202601010000"

    def test_sensor_samples_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "get-surface-sensor-samples",
                "g1",
                "m_gps_lat",
                "--start",
                "202601010000",
                "--end",
                "202601020000",
            ]
        )
        assert args.sensor_type == "m_gps_lat"
        assert args.start == "202601010000"
        assert args.end == "202601020000"

    def test_register_glider_default_group(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["register-glider", "newglider"])
        assert args.glider_name == "newglider"
        assert args.group == "default"

    def test_register_glider_custom_group(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["register-glider", "g1", "--group", "mygrp"])
        assert args.group == "mygrp"

    def test_set_assigned_script_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["set-assigned-script", "g1", "factory", "sfmc.xml"])
        assert args.script_type == "factory"
        assert args.script_name == "sfmc.xml"

    def test_send_command_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["send-command", "g1", "put c_science_on 0"])
        assert args.command_str == "put c_science_on 0"

    def test_download_file_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "download-glider-file",
                "g1",
                "from-glider",
                "data.sbd",
                "-o",
                "/tmp/out.sbd",
            ]
        )
        assert args.file_name == "data.sbd"

    def test_upload_files_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(
            [
                "upload-glider-files",
                "g1",
                "to-glider",
                "f1.mi",
                "f2.ma",
            ]
        )
        assert len(args.files) == 2

    def test_plan_upload_args(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["update-waypoint-plan", "g1", "plan.goto"])
        assert args.glider_name == "g1"

    def test_all_glider_only_commands_parseable(self) -> None:
        parser = build_parser()
        for cmd in _GLIDER_ONLY:
            args = parser.parse_args([cmd, "testglider"])
            assert args.command == cmd
            assert args.glider_name == "testglider"

    def test_all_plan_upload_commands_parseable(self) -> None:
        parser = build_parser()
        for cmd in _PLAN_UPLOAD:
            args = parser.parse_args([cmd, "testglider", "file.dat"])
            assert args.command == cmd

    def test_all_stream_commands_parseable(self) -> None:
        parser = build_parser()
        for cmd in _STREAM:
            args = parser.parse_args([cmd, "testglider"])
            assert args.command == cmd


class TestPrintJson:
    def test_pretty(self, capsys: pytest.CaptureFixture[str]) -> None:
        _print_json({"key": "val"}, compact=False)
        out = capsys.readouterr().out
        assert '"key": "val"' in out
        assert "\n" in out

    def test_compact(self, capsys: pytest.CaptureFixture[str]) -> None:
        _print_json({"key": "val"}, compact=True)
        out = capsys.readouterr().out.strip()
        assert out == '{"key":"val"}'


class TestCallMethod:
    def _make_args(self, **kwargs: object) -> MagicMock:
        args = MagicMock()
        for k, v in kwargs.items():
            setattr(args, k, v)
        return args

    def test_glider_only(self) -> None:
        client = MagicMock()
        client.get_glider_details.return_value = {"data": {}}
        args = self._make_args(glider_name="g1")
        result = _call_method(client, "get-glider-details", "get_glider_details", args)
        client.get_glider_details.assert_called_once_with("g1")
        assert result == {"data": {}}

    def test_plan_upload(self) -> None:
        client = MagicMock()
        client.update_waypoint_plan.return_value = {"ok": True}
        args = self._make_args(glider_name="g1", file="plan.goto")
        _call_method(client, "update-waypoint-plan", "update_waypoint_plan", args)
        client.update_waypoint_plan.assert_called_once_with("g1", "plan.goto")

    def test_unknown_command_raises(self) -> None:
        client = MagicMock()
        args = self._make_args()
        with pytest.raises(SystemExit, match="Unknown command"):
            _call_method(client, "not-a-real-command", "not_a_real_command", args)

    def test_register_glider(self) -> None:
        client = MagicMock()
        client.register_glider.return_value = {}
        args = self._make_args(glider_name="g1", group="mygrp")
        _call_method(client, "register-glider", "register_glider", args)
        client.register_glider.assert_called_once_with("g1", "mygrp")

    def test_send_command(self) -> None:
        client = MagicMock()
        client.send_command.return_value = {}
        args = self._make_args(glider_name="g1", command_str="help")
        _call_method(client, "send-command", "send_command", args)
        client.send_command.assert_called_once_with("g1", "help")

    def test_get_zmodem_transfers(self) -> None:
        client = MagicMock()
        client.get_zmodem_transfers.return_value = {}
        args = self._make_args(connection_id="42")
        _call_method(client, "get-zmodem-transfers", "get_zmodem_transfers", args)
        client.get_zmodem_transfers.assert_called_once_with("42")

    def test_delete_glider_file(self) -> None:
        client = MagicMock()
        client.delete_glider_file.return_value = {}
        args = self._make_args(glider_name="g1", folder="to-glider", file_name="old.mi")
        _call_method(client, "delete-glider-file", "delete_glider_file", args)
        client.delete_glider_file.assert_called_once_with("g1", "to-glider", "old.mi")
