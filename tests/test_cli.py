"""Tests for sfmc_api.cli."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sfmc_api.cli import (
    _GLIDER_ONLY,
    _PLAN_UPLOAD,
    _STREAM,
    _call_method,
    _handle_add_host,
    _handle_init,
    _handle_stream,
    _print_json,
    _prompt,
    _prompt_host_entry,
    _run,
    build_parser,
    main,
)
from sfmc_api.exceptions import SFMCError


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

    def test_credentials_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--credentials", "/tmp/c.json", "auth"])
        assert str(args.credentials) == "/tmp/c.json"

    def test_host_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["--host", "sfmc.test.com", "auth"])
        assert args.host == "sfmc.test.com"

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


# ── TestRun ──────────────────────────────────────────────────────────


class TestRun:
    """Tests for the _run() dispatch function."""

    def _make_args(self, **kwargs: object) -> MagicMock:
        args = MagicMock()
        args.compact = False
        for k, v in kwargs.items():
            setattr(args, k, v)
        return args

    def test_auth_command(self, capsys: pytest.CaptureFixture[str]) -> None:
        client = MagicMock()
        client._config.host = "sfmc.example.com"
        args = self._make_args(command="auth")
        rc = _run(client, args)
        assert rc == 0
        client.authenticate.assert_called_once()
        out = json.loads(capsys.readouterr().out)
        assert out == {"status": "ok", "host": "sfmc.example.com"}

    def test_download_glider_file(self, capsys: pytest.CaptureFixture[str]) -> None:
        client = MagicMock()
        client.download_glider_file.return_value = Path("/tmp/data.sbd")
        args = self._make_args(
            command="download-glider-file",
            glider_name="g1",
            folder="from-glider",
            file_name="data.sbd",
            output=None,
        )
        rc = _run(client, args)
        assert rc == 0
        client.download_glider_file.assert_called_once_with("g1", "from-glider", "data.sbd", None)
        out = json.loads(capsys.readouterr().out)
        assert out == {"downloaded": "/tmp/data.sbd"}

    def test_download_glider_files(self, capsys: pytest.CaptureFixture[str]) -> None:
        client = MagicMock()
        client.download_glider_files.return_value = Path("/tmp/archive.zip")
        args = self._make_args(
            command="download-glider-files",
            glider_name="g1",
            folder="from-glider",
            output=None,
            filter="*.sbd",
            last_modified_after=None,
        )
        rc = _run(client, args)
        assert rc == 0
        client.download_glider_files.assert_called_once_with(
            "g1", "from-glider", None, filter="*.sbd", last_modified_after=None
        )
        out = json.loads(capsys.readouterr().out)
        assert out == {"downloaded": "/tmp/archive.zip"}

    def test_dispatches_to_call_method(self, capsys: pytest.CaptureFixture[str]) -> None:
        client = MagicMock()
        client.get_glider_details.return_value = {"name": "g1"}
        args = self._make_args(command="get-glider-details", glider_name="g1")
        rc = _run(client, args)
        assert rc == 0
        client.get_glider_details.assert_called_once_with("g1")
        out = json.loads(capsys.readouterr().out)
        assert out == {"name": "g1"}

    @patch("sfmc_api.cli._handle_stream", return_value=0)
    def test_dispatches_stream_command(self, mock_stream: MagicMock) -> None:
        client = MagicMock()
        args = self._make_args(command="subscribe-connection-events", glider_name="g1")
        rc = _run(client, args)
        assert rc == 0
        mock_stream.assert_called_once_with(client, args, "subscribe_connection_events", False)


# ── TestCallMethodCustom ─────────────────────────────────────────────


class TestCallMethodCustom:
    """Tests for _call_method branches not covered by TestCallMethod."""

    def _make_args(self, **kwargs: object) -> MagicMock:
        args = MagicMock()
        for k, v in kwargs.items():
            setattr(args, k, v)
        return args

    def test_surface_sensor_samples(self) -> None:
        client = MagicMock()
        client.get_surface_sensor_samples.return_value = {"samples": []}
        args = self._make_args(
            glider_name="g1",
            sensor_type="m_gps_lat",
            start="202601010000",
            end="202601020000",
        )
        result = _call_method(
            client, "get-surface-sensor-samples", "get_surface_sensor_samples", args
        )
        client.get_surface_sensor_samples.assert_called_once_with(
            "g1", "m_gps_lat", start_datetime="202601010000", end_datetime="202601020000"
        )
        assert result == {"samples": []}

    def test_folder_file_listing(self) -> None:
        client = MagicMock()
        client.get_folder_file_listing.return_value = {"files": []}
        args = self._make_args(
            glider_name="g1",
            folder="from-glider",
            page=2,
            filter="*.sbd",
            last_modified_after="202601010000",
        )
        result = _call_method(client, "get-folder-file-listing", "get_folder_file_listing", args)
        client.get_folder_file_listing.assert_called_once_with(
            "g1",
            "from-glider",
            page=2,
            filter="*.sbd",
            last_modified_after="202601010000",
        )
        assert result == {"files": []}

    def test_update_active_deployment_start(self) -> None:
        client = MagicMock()
        client.update_active_deployment_start.return_value = {"ok": True}
        args = self._make_args(glider_name="g1", start_datetime="202601010000")
        result = _call_method(
            client,
            "update-active-deployment-start",
            "update_active_deployment_start",
            args,
        )
        client.update_active_deployment_start.assert_called_once_with("g1", "202601010000")
        assert result == {"ok": True}

    def test_set_assigned_script(self) -> None:
        client = MagicMock()
        client.set_assigned_script.return_value = {"ok": True}
        args = self._make_args(glider_name="g1", script_type="factory", script_name="sfmc.xml")
        result = _call_method(client, "set-assigned-script", "set_assigned_script", args)
        client.set_assigned_script.assert_called_once_with("g1", "factory", "sfmc.xml")
        assert result == {"ok": True}

    def test_upload_glider_files(self) -> None:
        client = MagicMock()
        client.upload_glider_files.return_value = {"uploaded": 2}
        files = [Path("f1.mi"), Path("f2.ma")]
        args = self._make_args(glider_name="g1", folder="to-glider", files=files)
        result = _call_method(client, "upload-glider-files", "upload_glider_files", args)
        client.upload_glider_files.assert_called_once_with("g1", "to-glider", files)
        assert result == {"uploaded": 2}

    def test_upload_cache_files(self) -> None:
        client = MagicMock()
        client.upload_cache_files.return_value = {"uploaded": 1}
        files = [Path("cache.dat")]
        args = self._make_args(group_name="mygrp", files=files)
        result = _call_method(client, "upload-cache-files", "upload_cache_files", args)
        client.upload_cache_files.assert_called_once_with("mygrp", files)
        assert result == {"uploaded": 1}


# ── TestHandleStream ─────────────────────────────────────────────────


class TestHandleStream:
    """Tests for _handle_stream()."""

    def test_streams_events(self, capsys: pytest.CaptureFixture[str]) -> None:
        client = MagicMock()
        stomp = MagicMock()
        client.open_stream.return_value.__enter__ = MagicMock(return_value=stomp)
        client.open_stream.return_value.__exit__ = MagicMock(return_value=False)

        events = [{"event": "connected"}, {"event": "data"}]
        sub = MagicMock()
        sub.__iter__ = MagicMock(return_value=iter(events))
        client.subscribe_connection_events.return_value = sub

        args = MagicMock()
        args.command = "subscribe-connection-events"
        args.glider_name = "g1"

        rc = _handle_stream(client, args, "subscribe_connection_events", compact=True)

        assert rc == 0
        client.subscribe_connection_events.assert_called_once_with("g1", stomp)
        out = capsys.readouterr().out.strip().split("\n")
        assert len(out) == 2
        assert json.loads(out[0]) == {"event": "connected"}
        assert json.loads(out[1]) == {"event": "data"}

    def test_keyboard_interrupt(self, capsys: pytest.CaptureFixture[str]) -> None:
        client = MagicMock()
        stomp = MagicMock()
        client.open_stream.return_value.__enter__ = MagicMock(return_value=stomp)
        client.open_stream.return_value.__exit__ = MagicMock(return_value=False)

        def raise_interrupt() -> None:
            raise KeyboardInterrupt

        sub = MagicMock()
        sub.__iter__ = MagicMock(side_effect=raise_interrupt)
        client.subscribe_connection_events.return_value = sub

        args = MagicMock()
        args.command = "subscribe-connection-events"
        args.glider_name = "g1"

        rc = _handle_stream(client, args, "subscribe_connection_events", compact=False)

        assert rc == 0
        err = capsys.readouterr().err
        assert "Stopped" in err


# ── TestPrompt ───────────────────────────────────────────────────────


class TestPrompt:
    """Tests for the _prompt() helper."""

    def test_with_default_uses_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "custom_val")
        assert _prompt("Label", default="fallback") == "custom_val"

    def test_with_default_empty_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "   ")
        assert _prompt("Label", default="fallback") == "fallback"

    def test_required_repeats_until_input(self, monkeypatch: pytest.MonkeyPatch) -> None:
        responses = iter(["", "  ", "finally"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        assert _prompt("Label", required=True) == "finally"

    def test_optional_accepts_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert _prompt("Label", required=False) == ""


# ── TestPromptHostEntry ──────────────────────────────────────────────


class TestPromptHostEntry:
    """Tests for _prompt_host_entry()."""

    def test_returns_hostname_and_entry(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        inputs = iter(["sfmc.example.com", "myid", "", ""])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        import getpass as _gp

        monkeypatch.setattr(_gp, "getpass", lambda _prompt, stream=None: "mysecret")
        hostname, entry = _prompt_host_entry()
        assert hostname == "sfmc.example.com"
        assert entry["apiCredentials"]["clientId"] == "myid"
        assert entry["apiCredentials"]["secret"] == "mysecret"
        assert entry["tlsRejectUnauthorized"] == 1  # default is now "yes"
        assert "rootDownloadPath" not in entry

    @patch("getpass.getpass", return_value="mysecret")
    def test_tls_yes(self, mock_gp: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        inputs = iter(["sfmc.example.com", "myid", "yes", ""])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        _hostname, entry = _prompt_host_entry()
        assert entry["tlsRejectUnauthorized"] == 1

    @patch("getpass.getpass", return_value="mysecret")
    def test_tls_no(self, mock_gp: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
        inputs = iter(["sfmc.example.com", "myid", "no", ""])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        _hostname, entry = _prompt_host_entry()
        assert entry["tlsRejectUnauthorized"] == 0

    @patch("getpass.getpass", return_value="mysecret")
    def test_with_download_dir(
        self,
        mock_gp: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        inputs = iter(["sfmc.example.com", "myid", "", "/tmp/downloads"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        _hostname, entry = _prompt_host_entry()
        assert entry["rootDownloadPath"] == "/tmp/downloads"


# ── TestHandleInit ───────────────────────────────────────────────────


class TestHandleInit:
    """Tests for _handle_init()."""

    @patch("sfmc_api.cli._prompt_host_entry")
    def test_creates_file(self, mock_prompt: MagicMock, tmp_path: Path) -> None:
        creds = tmp_path / "creds.json"
        args = MagicMock()
        args.credentials = creds
        mock_prompt.return_value = (
            "sfmc.example.com",
            {"apiCredentials": {"clientId": "id", "secret": "sec"}, "tlsRejectUnauthorized": 0},
        )
        rc = _handle_init(args)
        assert rc == 0
        assert creds.exists()
        data = json.loads(creds.read_text())
        assert "sfmc.example.com" in data
        assert data["sfmc.example.com"]["apiCredentials"]["clientId"] == "id"

    def test_file_exists_returns_1(self, tmp_path: Path) -> None:
        creds = tmp_path / "creds.json"
        creds.write_text("{}")
        args = MagicMock()
        args.credentials = creds
        rc = _handle_init(args)
        assert rc == 1

    @patch("sfmc_api.cli._prompt_host_entry")
    def test_sets_permissions(self, mock_prompt: MagicMock, tmp_path: Path) -> None:
        creds = tmp_path / "creds.json"
        args = MagicMock()
        args.credentials = creds
        mock_prompt.return_value = (
            "host.example.com",
            {"apiCredentials": {"clientId": "id", "secret": "sec"}, "tlsRejectUnauthorized": 0},
        )
        _handle_init(args)
        mode = creds.stat().st_mode & 0o777
        assert mode == 0o600


# ── TestHandleAddHost ────────────────────────────────────────────────


class TestHandleAddHost:
    """Tests for _handle_add_host()."""

    def test_no_file_returns_1(self, tmp_path: Path) -> None:
        creds = tmp_path / "nonexistent.json"
        args = MagicMock()
        args.credentials = creds
        rc = _handle_add_host(args)
        assert rc == 1

    @patch("sfmc_api.cli._prompt_host_entry")
    def test_adds_new_host(self, mock_prompt: MagicMock, tmp_path: Path) -> None:
        creds = tmp_path / "creds.json"
        creds.write_text(
            json.dumps(
                {
                    "existing.com": {
                        "apiCredentials": {"clientId": "a", "secret": "b"},
                        "tlsRejectUnauthorized": 0,
                    }
                }
            )
        )
        args = MagicMock()
        args.credentials = creds
        mock_prompt.return_value = (
            "newhost.com",
            {"apiCredentials": {"clientId": "c", "secret": "d"}, "tlsRejectUnauthorized": 0},
        )
        rc = _handle_add_host(args)
        assert rc == 0
        data = json.loads(creds.read_text())
        assert "existing.com" in data
        assert "newhost.com" in data

    @patch("sfmc_api.cli._prompt")
    @patch("sfmc_api.cli._prompt_host_entry")
    def test_duplicate_host_cancelled(
        self, mock_phe: MagicMock, mock_prompt: MagicMock, tmp_path: Path
    ) -> None:
        creds = tmp_path / "creds.json"
        creds.write_text(
            json.dumps(
                {
                    "dup.com": {
                        "apiCredentials": {"clientId": "a", "secret": "b"},
                        "tlsRejectUnauthorized": 0,
                    }
                }
            )
        )
        args = MagicMock()
        args.credentials = creds
        mock_phe.return_value = (
            "dup.com",
            {"apiCredentials": {"clientId": "new", "secret": "new"}, "tlsRejectUnauthorized": 0},
        )
        mock_prompt.return_value = "no"
        rc = _handle_add_host(args)
        assert rc == 1
        # Original data unchanged
        data = json.loads(creds.read_text())
        assert data["dup.com"]["apiCredentials"]["clientId"] == "a"

    @patch("sfmc_api.cli._prompt")
    @patch("sfmc_api.cli._prompt_host_entry")
    def test_duplicate_host_overwrite(
        self, mock_phe: MagicMock, mock_prompt: MagicMock, tmp_path: Path
    ) -> None:
        creds = tmp_path / "creds.json"
        creds.write_text(
            json.dumps(
                {
                    "dup.com": {
                        "apiCredentials": {"clientId": "a", "secret": "b"},
                        "tlsRejectUnauthorized": 0,
                    }
                }
            )
        )
        args = MagicMock()
        args.credentials = creds
        mock_phe.return_value = (
            "dup.com",
            {"apiCredentials": {"clientId": "new", "secret": "new"}, "tlsRejectUnauthorized": 0},
        )
        mock_prompt.return_value = "yes"
        rc = _handle_add_host(args)
        assert rc == 0
        data = json.loads(creds.read_text())
        assert data["dup.com"]["apiCredentials"]["clientId"] == "new"

    def test_corrupt_json_returns_1(self, tmp_path: Path) -> None:
        creds = tmp_path / "creds.json"
        creds.write_text("NOT VALID JSON{{{")
        args = MagicMock()
        args.credentials = creds
        rc = _handle_add_host(args)
        assert rc == 1


# ── TestOSErrorHandling ──────────────────────────────────────────────


class TestOSErrorHandling:
    @patch("sfmc_api.cli._run")
    @patch("sfmc_api.cli.SFMCClient")
    @patch("sfmc_api.cli.build_parser")
    def test_os_error_caught(
        self,
        mock_parser_fn: MagicMock,
        mock_client_cls: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        args = MagicMock()
        args.command = "auth"
        args.credentials = None
        args.host = None
        args.download_path = None
        mock_parser_fn.return_value.parse_args.return_value = args

        mock_client_cls.return_value = MagicMock()
        mock_run.side_effect = FileNotFoundError("No such file: test.json")
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1


# ── TestMain ─────────────────────────────────────────────────────────


class TestMain:
    """Tests for main()."""

    @patch("sfmc_api.cli._run", return_value=0)
    @patch("sfmc_api.cli.SFMCClient")
    @patch("sfmc_api.cli.build_parser")
    def test_main_auth(
        self,
        mock_parser_fn: MagicMock,
        mock_client_cls: MagicMock,
        mock_run: MagicMock,
    ) -> None:
        args = MagicMock()
        args.command = "auth"
        args.credentials = None
        args.host = None
        args.download_path = None
        mock_parser_fn.return_value.parse_args.return_value = args

        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
        mock_run.assert_called_once_with(mock_client, args)

    @patch("sfmc_api.cli.SFMCClient")
    @patch("sfmc_api.cli.build_parser")
    def test_main_sfmc_error(
        self,
        mock_parser_fn: MagicMock,
        mock_client_cls: MagicMock,
    ) -> None:
        args = MagicMock()
        args.command = "auth"
        args.credentials = None
        args.host = None
        args.download_path = None
        mock_parser_fn.return_value.parse_args.return_value = args

        mock_client_cls.return_value.__enter__ = MagicMock(side_effect=SFMCError("fail"))

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    @patch("sfmc_api.cli.SFMCClient")
    @patch("sfmc_api.cli.build_parser")
    def test_main_keyboard_interrupt(
        self,
        mock_parser_fn: MagicMock,
        mock_client_cls: MagicMock,
    ) -> None:
        args = MagicMock()
        args.command = "auth"
        args.credentials = None
        args.host = None
        args.download_path = None
        mock_parser_fn.return_value.parse_args.return_value = args

        mock_client_cls.return_value.__enter__ = MagicMock(side_effect=KeyboardInterrupt)

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 130

    @patch("sfmc_api.cli._handle_init", return_value=0)
    @patch("sfmc_api.cli.build_parser")
    def test_main_init(
        self,
        mock_parser_fn: MagicMock,
        mock_handle_init: MagicMock,
    ) -> None:
        args = MagicMock()
        args.command = "init"
        mock_parser_fn.return_value.parse_args.return_value = args

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
        mock_handle_init.assert_called_once_with(args)

    @patch("sfmc_api.cli._handle_add_host", return_value=0)
    @patch("sfmc_api.cli.build_parser")
    def test_main_add_host(
        self,
        mock_parser_fn: MagicMock,
        mock_handle_add: MagicMock,
    ) -> None:
        args = MagicMock()
        args.command = "add-host"
        mock_parser_fn.return_value.parse_args.return_value = args

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
        mock_handle_add.assert_called_once_with(args)


# ── TestDestructiveConfirmation ──────────────────────────────────────


class TestDestructiveConfirmation:
    """Destructive commands prompt unless --yes or SFMC_ASSUME_YES is set."""

    def _make_args(self, **kwargs: object) -> MagicMock:
        args = MagicMock()
        args.compact = False
        args.yes = False
        for k, v in kwargs.items():
            setattr(args, k, v)
        return args

    def test_yes_flag_skips_prompt(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SFMC_ASSUME_YES", raising=False)
        client = MagicMock()
        client.delete_glider_file.return_value = {}
        args = self._make_args(
            command="delete-glider-file",
            yes=True,
            glider_name="g1",
            folder="to-glider",
            file_name="old.mi",
        )
        rc = _run(client, args)
        assert rc == 0
        client.delete_glider_file.assert_called_once_with("g1", "to-glider", "old.mi")

    def test_env_var_skips_prompt(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SFMC_ASSUME_YES", "1")
        client = MagicMock()
        client.delete_glider_file.return_value = {}
        args = self._make_args(
            command="delete-glider-file",
            glider_name="g1",
            folder="to-glider",
            file_name="old.mi",
        )
        rc = _run(client, args)
        assert rc == 0
        client.delete_glider_file.assert_called_once()

    def test_non_tty_without_yes_refuses(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SFMC_ASSUME_YES", raising=False)
        monkeypatch.setattr("sys.stdin.isatty", lambda: False)
        client = MagicMock()
        args = self._make_args(
            command="delete-glider-file",
            glider_name="g1",
            folder="to-glider",
            file_name="old.mi",
        )
        rc = _run(client, args)
        assert rc == 1
        client.delete_glider_file.assert_not_called()
        err = capsys.readouterr().err
        assert "SFMC_ASSUME_YES" in err or "--yes" in err

    def test_tty_user_says_no(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SFMC_ASSUME_YES", raising=False)
        monkeypatch.setattr("sys.stdin.isatty", lambda: True)
        monkeypatch.setattr("builtins.input", lambda _prompt: "n")
        client = MagicMock()
        args = self._make_args(command="clear-assigned-script", glider_name="g1")
        rc = _run(client, args)
        assert rc == 1
        client.clear_assigned_script.assert_not_called()

    def test_tty_user_says_yes(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("SFMC_ASSUME_YES", raising=False)
        monkeypatch.setattr("sys.stdin.isatty", lambda: True)
        monkeypatch.setattr("builtins.input", lambda _prompt: "yes")
        client = MagicMock()
        client.clear_assigned_script.return_value = {}
        args = self._make_args(command="clear-assigned-script", glider_name="g1")
        rc = _run(client, args)
        assert rc == 0
        client.clear_assigned_script.assert_called_once_with("g1")

    def test_non_destructive_unaffected(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # No prompt should appear; no input read.
        monkeypatch.setattr("sys.stdin.isatty", lambda: False)
        monkeypatch.delenv("SFMC_ASSUME_YES", raising=False)
        client = MagicMock()
        client.get_glider_details.return_value = {"name": "g1"}
        args = self._make_args(command="get-glider-details", glider_name="g1")
        rc = _run(client, args)
        assert rc == 0
        client.get_glider_details.assert_called_once()
