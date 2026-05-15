"""Tests for sfmc_api.client.SFMCClient."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from sfmc_api.client import SFMCClient
from sfmc_api.config import SFMCConfig
from sfmc_api.exceptions import APIError, AuthenticationError, RateLimitError
from tests.conftest import make_mock_response


class TestConstruction:
    def test_from_config_object(self, config: SFMCConfig) -> None:
        client = SFMCClient(config=config)
        assert client._config is config
        client.close()

    def test_from_config_file(self, tmp_path: Path) -> None:
        p = tmp_path / "creds.json"
        p.write_text('{"h.test":{"apiCredentials":{"clientId":"c","secret":"s"}}}')
        client = SFMCClient(config_path=p)
        assert client._config.host == "h.test"
        client.close()

    def test_with_host_param(self, tmp_path: Path) -> None:
        p = tmp_path / "creds.json"
        p.write_text(
            '{"a.test":{"apiCredentials":{"clientId":"c","secret":"s"}},'
            '"b.test":{"apiCredentials":{"clientId":"d","secret":"t"}}}'
        )
        client = SFMCClient(config_path=p, host="b.test")
        assert client._config.host == "b.test"
        assert client._config.client_id == "d"
        client.close()

    def test_context_manager(self, config: SFMCConfig) -> None:
        with SFMCClient(config=config) as client:
            assert client._config is config

    def test_default_no_token(self, config: SFMCConfig) -> None:
        client = SFMCClient(config=config)
        assert client._token is None
        client.close()


class TestLazyAuth:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_first_request_authenticates(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {"data": {"id": 1, "name": "g1"}})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            result = client.get_glider_details("g1")

        mock_auth.assert_called_once()
        assert result["data"]["name"] == "g1"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_auth_cached_across_calls(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {"data": {}})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.get_glider_details("g1")
            client.get_glider_details("g2")

        mock_auth.assert_called_once()

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_explicit_authenticate(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)

        with SFMCClient(config=config) as client:
            client.authenticate()
            assert client._token == "tok"


class TestRequest:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_auth_header_attached(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {"ok": True})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.get_glider_details("g1")

        call_kwargs = mock_http.request.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers", {})
        assert headers["Authorization"] == "Bearer tok"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_api_error_raised(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(404, text="not found")
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client, pytest.raises(APIError) as exc_info:
            client.get_glider_details("nope")
        assert exc_info.value.status_code == 404

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_transport_error_wrapped(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        """Raw httpx transport errors are wrapped as APIError with type info."""
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = httpx.ConnectError("connection refused")
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client, pytest.raises(APIError) as exc_info:
            client.get_glider_details("g1")
        assert exc_info.value.status_code == 0
        # The new message surfaces the exception class and attempt count
        # so a non-expert can tell network failures from server failures.
        body = exc_info.value.response_body
        assert "connection refused" in body
        assert "ConnectError" in body
        assert "after 3 attempts" in body


class TestGliderOnlyMethods:
    """Test all glider-name-only methods build the correct request path."""

    @pytest.mark.parametrize(
        ("method_name", "expected_path"),
        [
            ("get_glider_details", "/v1/gliders/tg"),
            ("get_active_deployment_details", "/v1/active-deployment/tg"),
            ("get_newest_mission_status", "/v1/newest-mission-details/tg"),
            ("get_mission_plan", "/v1/glider-assigned-mission-plan/tg"),
            ("get_waypoint_plan", "/v1/glider-assigned-waypoint-plan/tg"),
            ("get_yo_plan", "/v1/glider-assigned-yo-plan/tg"),
            ("get_surface_plan", "/v1/glider-assigned-surface-plan/tg"),
            ("get_sampling_plan", "/v1/glider-assigned-sampling-plan/tg"),
            ("get_data_transmission_plan", "/v1/glider-assigned-data-transmission-plan/tg"),
            ("get_mission_sensor_plan", "/v1/glider-assigned-mission-sensor-plan/tg"),
            ("get_abort_plan", "/v1/glider-assigned-abort-plan/tg"),
            ("get_available_scripts", "/v1/scripts-for-glider/tg"),
            ("obtain_or_create_active_deployment", "/v1/obtain-or-create-active-deployment/tg"),
            ("clear_assigned_script", "/v1/clear-assigned-script/tg"),
            ("pause_assigned_script", "/v1/pause-assigned-script/tg"),
            ("resume_assigned_script", "/v1/resume-assigned-script/tg"),
            ("rewind_assigned_script", "/v1/rewind-assigned-script/tg"),
            ("deploy_goto_file", "/v1/gen-and-deploy-glider-goto-file/tg"),
            ("deploy_yo_file", "/v1/gen-and-deploy-glider-yo-file/tg"),
            ("deploy_surface_files", "/v1/gen-and-deploy-glider-surface-files/tg"),
            ("deploy_sample_files", "/v1/gen-and-deploy-glider-sample-files/tg"),
            ("deploy_sbd_list_file", "/v1/gen-and-deploy-glider-sbd-list-file/tg"),
            ("deploy_tbd_list_file", "/v1/gen-and-deploy-glider-tbd-list-file/tg"),
            (
                "delete_hit_waypoint_surface_plan_rule",
                "/v1/delete-glider-hit-waypoint-surface-plan-rule/tg",
            ),
            (
                "delete_every_secs_surface_plan_rules",
                "/v1/delete-glider-every-secs-surface-plan-rules/tg",
            ),
            (
                "delete_at_utc_time_surface_plan_rules",
                "/v1/delete-glider-at-utc-time-surface-plan-rules/tg",
            ),
            ("delete_sampling_plan_rules", "/v1/delete-glider-sampling-plan-rules/tg"),
        ],
    )
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_correct_path(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
        method_name: str,
        expected_path: str,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {"data": {}})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            getattr(client, method_name)("tg")

        assert mock_http.request.call_args[0][1] == expected_path


class TestScriptControl:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_set_assigned_script(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.set_assigned_script("g1", "factory", "sfmc.xml")

        assert mock_http.request.call_args[0][1] == "/v1/set-assigned-script/g1/factory/sfmc.xml"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_send_command(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.send_command("g1", "put c_science_on 0")

        assert mock_http.request.call_args[0][1] == "/v1/submit-command/g1"
        assert mock_http.request.call_args.kwargs["content"] == "put c_science_on 0"


class TestFileOperations:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_invalid_folder(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)
        with SFMCClient(config=config) as client:
            client._token = "tok"
            with pytest.raises(ValueError, match="must be one of"):
                client.upload_glider_files("g1", "bad-folder", [])

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_delete_invalid_folder(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)
        with SFMCClient(config=config) as client:
            client._token = "tok"
            with pytest.raises(ValueError, match="must be one of"):
                client.delete_glider_file("g1", "bad-folder", "f.txt")


class TestFolderFileListing:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_params_passed(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {"results": []})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.get_folder_file_listing(
                "g1",
                "from-glider",
                page=2,
                filter="*.sbd",
                last_modified_after="202601010000",
            )

        call_kwargs = mock_http.request.call_args.kwargs
        assert call_kwargs["params"]["page"] == 2
        assert call_kwargs["params"]["filter"] == "*.sbd"
        assert call_kwargs["params"]["lastModifiedAfter"] == "202601010000"


class TestSurfaceSensorSamples:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_params_passed(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(200, {"data": []})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.get_surface_sensor_samples("g1", "m_gps_lat", "202601010000", "202601020000")

        call_args = mock_http.request.call_args
        assert "/v1/surface-sensor-samples/g1/m_gps_lat" in call_args[0][1]
        params = call_args.kwargs["params"]
        assert params["startDateTime"] == "202601010000"
        assert params["endDateTime"] == "202601020000"


class TestOpenStream:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    @patch("sfmc_api.client.StompConnection")
    def test_creates_stomp_connection(
        self,
        mock_stomp_cls: MagicMock,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)
        mock_conn = MagicMock()
        mock_stomp_cls.return_value = mock_conn

        with SFMCClient(config=config) as client:
            result = client.open_stream()

        mock_stomp_cls.assert_called_once_with(config, "tok")
        mock_conn.connect.assert_called_once()
        assert result is mock_conn

    @patch("sfmc_api.client.authenticate", return_value=None)
    @patch("sfmc_api.client.build_http_client")
    def test_raises_if_no_token(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)
        with SFMCClient(config=config) as client, pytest.raises(AuthenticationError):
            client.open_stream()


class TestGetGliderId:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_extracts_id(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(
            200, {"data": {"id": 42, "name": "g1"}}
        )
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            gid = client._get_glider_id("g1")

        assert gid == 42
        assert isinstance(gid, int)


# ── download_dir property ────────────────────────────────────────


class TestDownloadDir:
    @patch("sfmc_api.client.build_http_client")
    def test_uses_constructor_path(
        self, mock_build: MagicMock, tmp_path: Path, config: SFMCConfig
    ) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)
        dl_dir = tmp_path / "my-downloads"

        with SFMCClient(config=config, download_path=dl_dir) as client:
            result = client.download_dir

        assert result == dl_dir
        assert dl_dir.is_dir()

    @patch("sfmc_api.client.build_http_client")
    def test_uses_config_download_path(self, mock_build: MagicMock, tmp_path: Path) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)
        cfg_dir = tmp_path / "cfg-downloads"
        cfg = SFMCConfig(
            host="sfmc.test",
            client_id="cid",
            secret="sec",
            tls_verify=False,
            root_download_path=cfg_dir,
        )

        with SFMCClient(config=cfg) as client:
            result = client.download_dir

        assert result == cfg_dir
        assert cfg_dir.is_dir()

    @patch("sfmc_api.client.build_http_client")
    def test_creates_directory(
        self, mock_build: MagicMock, tmp_path: Path, config: SFMCConfig
    ) -> None:
        mock_build.return_value = MagicMock(spec=httpx.Client)
        nested = tmp_path / "a" / "b" / "c"

        with SFMCClient(config=config, download_path=nested) as client:
            result = client.download_dir

        assert result == nested
        assert nested.is_dir()


# ── _json_or_empty with non-empty body ───────────────────────────


class TestJsonOrEmpty:
    def test_returns_parsed_json_for_non_empty_body(self) -> None:
        response = MagicMock(spec=httpx.Response)
        response.content = b'{"status": "ok"}'
        response.json.return_value = {"status": "ok"}

        result = SFMCClient._json_or_empty(response)

        assert result == {"status": "ok"}
        response.json.assert_called_once()

    def test_returns_empty_dict_for_empty_body(self) -> None:
        response = MagicMock(spec=httpx.Response)
        response.content = b""

        result = SFMCClient._json_or_empty(response)

        assert result == {}
        response.json.assert_not_called()

    def test_returns_empty_dict_for_whitespace_only_body(self) -> None:
        response = MagicMock(spec=httpx.Response)
        response.content = b"   \n  "

        result = SFMCClient._json_or_empty(response)

        assert result == {}
        response.json.assert_not_called()


# ── Upload files ─────────────────────────────────────────────────


class TestUploadFiles:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_glider_files_valid_folder(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(200, {"uploaded": 2})
        resp.content = b'{"uploaded": 2}'
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        f1 = tmp_path / "a.ma"
        f2 = tmp_path / "b.mi"
        f1.write_bytes(b"file-a-content")
        f2.write_bytes(b"file-b-content")

        with SFMCClient(config=config) as client:
            result = client.upload_glider_files("g1", "to-glider", [f1, f2])

        assert result == {"uploaded": 2}
        call_args = mock_http.request.call_args
        assert call_args[0][0] == "PUT"
        assert call_args[0][1] == "/v1/upload-glider-files/g1/to-glider"
        # Verify files were passed in the request
        files_kwarg = call_args.kwargs["files"]
        assert len(files_kwarg) == 2
        assert files_kwarg[0][0] == "files"
        assert files_kwarg[0][1][0] == "a.ma"
        assert files_kwarg[1][0] == "files"
        assert files_kwarg[1][1][0] == "b.mi"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_cache_files(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(200, {"uploaded": 1})
        resp.content = b'{"uploaded": 1}'
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        f1 = tmp_path / "cache.dat"
        f1.write_bytes(b"cache-data")

        with SFMCClient(config=config) as client:
            result = client.upload_cache_files("mygroup", [f1])

        assert result == {"uploaded": 1}
        call_args = mock_http.request.call_args
        assert call_args[0][0] == "PUT"
        assert call_args[0][1] == "/v1/upload-cache-files/mygroup"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_correct_path_to_science(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(200)
        resp.content = b""
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        f1 = tmp_path / "science.dat"
        f1.write_bytes(b"data")

        with SFMCClient(config=config) as client:
            client.upload_glider_files("osu680", "to-science", [f1])

        assert mock_http.request.call_args[0][1] == "/v1/upload-glider-files/osu680/to-science"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_glider_file_contents_str(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(200, {"uploaded": 1})
        resp.content = b'{"uploaded": 1}'
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            result = client.upload_glider_file_contents(
                "g1",
                "to-glider",
                {"goto_l30.ma": "behavior_name=goto_list\n"},
            )

        assert result == {"uploaded": 1}
        call_args = mock_http.request.call_args
        assert call_args[0][0] == "PUT"
        assert call_args[0][1] == "/v1/upload-glider-files/g1/to-glider"
        files_kwarg = call_args.kwargs["files"]
        assert len(files_kwarg) == 1
        assert files_kwarg[0][0] == "files"
        assert files_kwarg[0][1][0] == "goto_l30.ma"
        # BytesIO is properly closed after the request completes.
        bio = files_kwarg[0][1][1]
        assert bio.closed

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_glider_file_contents_bytes(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(200, {"uploaded": 1})
        resp.content = b'{"uploaded": 1}'
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            result = client.upload_glider_file_contents(
                "g1",
                "to-science",
                {"data.bin": b"\x00\x01\x02"},
            )

        assert result == {"uploaded": 1}
        call_args = mock_http.request.call_args
        assert call_args[0][1] == "/v1/upload-glider-files/g1/to-science"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_glider_file_contents_bad_folder(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client, pytest.raises(ValueError, match="Upload folder"):
            client.upload_glider_file_contents("g1", "from-glider", {"f": "x"})

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_upload_glider_file_contents_empty_raises(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http

        with (
            SFMCClient(config=config) as client,
            pytest.raises(ValueError, match="must not be empty"),
        ):
            client.upload_glider_file_contents("g1", "to-glider", {})


# ── Download single file ─────────────────────────────────────────


def _make_stream_context(
    chunks: list[bytes] | None = None,
    raise_on_iter: bool = False,
) -> MagicMock:
    """Build a mock stream context manager for self._http.stream()."""
    mock_stream_response = MagicMock()
    mock_stream_response.is_success = True
    mock_stream_response.status_code = 200
    mock_stream_response.headers = {}
    mock_stream_response.text = ""
    if raise_on_iter:
        mock_stream_response.iter_bytes.side_effect = OSError("disk full")
    else:
        mock_stream_response.iter_bytes.return_value = chunks or [b"file content"]

    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=mock_stream_response)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


class TestDownloadGliderFile:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_download_single_file(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.stream.return_value = _make_stream_context([b"hello ", b"world"])
        mock_build.return_value = mock_http

        dest = tmp_path / "test.sbd"
        with SFMCClient(config=config) as client:
            result = client.download_glider_file(
                "g1", "from-glider", "test.sbd", download_path=dest
            )

        assert result == dest
        assert dest.read_bytes() == b"hello world"
        # .part file should not remain
        assert not dest.with_suffix(".sbd.part").exists()

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_download_with_explicit_path(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.stream.return_value = _make_stream_context([b"data"])
        mock_build.return_value = mock_http

        custom_path = tmp_path / "custom" / "output.bin"
        custom_path.parent.mkdir(parents=True)
        with SFMCClient(config=config) as client:
            result = client.download_glider_file(
                "g1", "from-glider", "f.bin", download_path=custom_path
            )

        assert result == custom_path
        assert custom_path.read_bytes() == b"data"
        # Verify the correct API path was called
        mock_http.stream.assert_called_once()
        call_args = mock_http.stream.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == "/v1/download-glider-file/g1/from-glider/f.bin"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_download_cleanup_on_error(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.stream.return_value = _make_stream_context(raise_on_iter=True)
        mock_build.return_value = mock_http

        dest = tmp_path / "fail.sbd"
        with SFMCClient(config=config) as client, pytest.raises(OSError, match="disk full"):
            client.download_glider_file("g1", "from-glider", "fail.sbd", download_path=dest)

        # Neither the final file nor the .part temp file should remain
        assert not dest.exists()
        assert not dest.with_suffix(".sbd.part").exists()


# ── Download multiple files (zip) ────────────────────────────────


class TestDownloadGliderFiles:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_download_zip_default_path(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.stream.return_value = _make_stream_context([b"PK\x03\x04zipdata"])
        mock_build.return_value = mock_http

        with SFMCClient(config=config, download_path=tmp_path) as client:
            result = client.download_glider_files("g1", "from-glider")

        expected = tmp_path / "g1-from-glider.zip"
        assert result == expected
        assert expected.read_bytes() == b"PK\x03\x04zipdata"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_download_zip_with_filters(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.stream.return_value = _make_stream_context([b"zip"])
        mock_build.return_value = mock_http

        with SFMCClient(config=config, download_path=tmp_path) as client:
            client.download_glider_files(
                "g1",
                "from-glider",
                filter="*.sbd",
                last_modified_after="202601010000",
            )

        call_kwargs = mock_http.stream.call_args.kwargs
        assert call_kwargs["params"]["filter"] == "*.sbd"
        assert call_kwargs["params"]["lastModifiedAfter"] == "202601010000"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_download_zip_explicit_path(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        tmp_path: Path,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.stream.return_value = _make_stream_context([b"zipdata"])
        mock_build.return_value = mock_http

        explicit = tmp_path / "my-archive.zip"
        with SFMCClient(config=config) as client:
            result = client.download_glider_files("g1", "from-glider", download_path=explicit)

        assert result == explicit
        assert explicit.read_bytes() == b"zipdata"


# ── Delete file (valid folder) ───────────────────────────────────


class TestDeleteGliderFileValid:
    @pytest.mark.parametrize("folder", ["to-glider", "to-science", "configuration"])
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_delete_valid_folder(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
        folder: str,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(200)
        resp.content = b""
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            result = client.delete_glider_file("g1", folder, "old.ma")

        assert result == {}
        call_args = mock_http.request.call_args
        assert call_args[0][0] == "DELETE"
        assert call_args[0][1] == f"/v1/delete-glider-file/g1/{folder}/old.ma"


# ── Subscription methods ─────────────────────────────────────────


class TestSubscriptionMethods:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_subscribe_connection_events(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(
            200, {"data": {"id": 42, "name": "g1"}}
        )
        mock_build.return_value = mock_http

        mock_stomp = MagicMock()
        mock_sub = MagicMock()
        mock_stomp.subscribe.return_value = mock_sub

        with SFMCClient(config=config) as client:
            result = client.subscribe_connection_events("g1", mock_stomp)

        mock_stomp.subscribe.assert_called_once_with("/topic/glider-connections-42")
        assert result is mock_sub

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_subscribe_glider_output(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(
            200, {"data": {"id": 42, "name": "g1"}}
        )
        mock_build.return_value = mock_http

        mock_stomp = MagicMock()
        mock_sub = MagicMock()
        mock_stomp.subscribe.return_value = mock_sub

        with SFMCClient(config=config) as client:
            result = client.subscribe_glider_output("g1", mock_stomp)

        mock_stomp.subscribe.assert_called_once_with("/topic/glider-link-output/42")
        assert result is mock_sub

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_subscribe_script_events(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(
            200, {"data": {"id": 42, "name": "g1"}}
        )
        mock_build.return_value = mock_http

        mock_stomp = MagicMock()
        mock_sub = MagicMock()
        mock_stomp.subscribe.return_value = mock_sub

        with SFMCClient(config=config) as client:
            result = client.subscribe_script_events("g1", mock_stomp)

        mock_stomp.subscribe.assert_called_once_with("/topic/glider-script-assignment-updates-42")
        assert result is mock_sub

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_subscribe_zmodem_transfer_events(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(
            200, {"data": {"id": 99, "gliderName": "g1"}}
        )
        mock_build.return_value = mock_http

        mock_stomp = MagicMock()
        mock_sub = MagicMock()
        mock_stomp.subscribe.return_value = mock_sub

        with SFMCClient(config=config) as client:
            result = client.subscribe_zmodem_transfer_events("g1", mock_stomp)

        mock_stomp.subscribe.assert_called_once_with("/topic/new-and-updated-zmodem-transfers-99")
        assert result is mock_sub

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_subscribe_deployment_events(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = make_mock_response(
            200, {"data": {"id": 99, "gliderName": "g1"}}
        )
        mock_build.return_value = mock_http

        mock_stomp = MagicMock()
        mock_sub = MagicMock()
        mock_stomp.subscribe.return_value = mock_sub

        with SFMCClient(config=config) as client:
            result = client.subscribe_deployment_events("g1", mock_stomp)

        mock_stomp.subscribe.assert_called_once_with(
            "/topic/low-freq-glider-deployment-updates-99"
        )
        assert result is mock_sub


class TestRetryBehavior:
    """Tests for _request() retry logic."""

    @patch("sfmc_api.client.time")
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_retries_on_429(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        mock_time: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        rate_resp = make_mock_response(
            429,
            {},
            headers={"x-rate-limit-retry-after-milliseconds": "100"},
        )
        ok_resp = make_mock_response(200, {"data": {}})
        mock_http.request.side_effect = [rate_resp, ok_resp]
        client = SFMCClient(config=config)
        result = client.get_glider_details("g1")
        assert result == {"data": {}}
        assert mock_http.request.call_count == 2
        mock_time.sleep.assert_called_once_with(0.1)

    @patch("sfmc_api.client.authenticate")
    @patch("sfmc_api.client.build_http_client")
    def test_refreshes_token_on_401(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_auth.return_value = "tok"
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        unauth_resp = make_mock_response(401, {})
        ok_resp = make_mock_response(200, {"data": {}})
        mock_http.request.side_effect = [unauth_resp, ok_resp]
        client = SFMCClient(config=config)
        result = client.get_glider_details("g1")
        assert result == {"data": {}}
        assert mock_auth.call_count == 2

    @patch("sfmc_api.client.authenticate")
    @patch("sfmc_api.client.build_http_client")
    def test_401_only_retried_once(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_auth.return_value = "tok"
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        unauth_resp = make_mock_response(401, {})
        mock_http.request.return_value = unauth_resp
        client = SFMCClient(config=config)
        with pytest.raises(APIError):
            client.get_glider_details("g1")
        assert mock_http.request.call_count == 2

    @patch("sfmc_api.client.time")
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_retries_on_transport_error(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        mock_time: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        ok_resp = make_mock_response(200, {"data": {}})
        mock_http.request.side_effect = [httpx.ConnectError("fail"), ok_resp]
        client = SFMCClient(config=config)
        result = client.get_glider_details("g1")
        assert result == {"data": {}}
        mock_time.sleep.assert_called_once_with(1)

    @patch("sfmc_api.client.time")
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_gives_up_after_max_retries(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        mock_time: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        rate_resp = make_mock_response(
            429,
            {},
            headers={"x-rate-limit-retry-after-milliseconds": "100"},
        )
        mock_http.request.return_value = rate_resp
        client = SFMCClient(config=config)
        with pytest.raises(RateLimitError):
            client.get_glider_details("g1")
        assert mock_http.request.call_count == 3


class TestPlanUpdateMethods:
    """Tests for plan update methods (multipart file upload)."""

    @pytest.mark.parametrize(
        ("method_name", "expected_path"),
        [
            ("update_waypoint_plan", "/v1/update-glider-waypoint-plan/tg"),
            ("update_yo_plan", "/v1/update-glider-yo-plan/tg"),
            ("update_surface_plan", "/v1/update-glider-surface-plan/tg"),
            ("update_sampling_plan", "/v1/update-glider-sampling-plan/tg"),
            (
                "update_flight_data_transmission_plan",
                "/v1/update-glider-flight-data-transmission-plan/tg",
            ),
            (
                "update_science_data_transmission_plan",
                "/v1/update-glider-science-data-transmission-plan/tg",
            ),
        ],
    )
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_plan_upload(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        method_name: str,
        expected_path: str,
        config: SFMCConfig,
        tmp_path: Path,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        mock_http.request.return_value = make_mock_response(200, {})

        plan_file = tmp_path / "plan.json"
        plan_file.write_text('{"waypoints": []}')

        with SFMCClient(config=config) as client:
            method = getattr(client, method_name)
            method("tg", str(plan_file))

        call_args = mock_http.request.call_args
        assert call_args[0][0] == "PUT"
        assert call_args[0][1] == expected_path
        # Verify files were passed (multipart upload)
        assert "files" in call_args.kwargs


class TestRegistrationAndDeployment:
    """Tests for register_glider and update_active_deployment_start."""

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_register_glider_default_group(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        mock_http.request.return_value = make_mock_response(200, {"data": {}})

        with SFMCClient(config=config) as client:
            client.register_glider("myglider")

        call_args = mock_http.request.call_args
        assert call_args[0][0] == "POST"
        assert call_args[0][1] == "/v1/register-glider/default"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_register_glider_custom_group(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        mock_http.request.return_value = make_mock_response(200, {"data": {}})

        with SFMCClient(config=config) as client:
            client.register_glider("myglider", group_name="mygroup")

        call_args = mock_http.request.call_args
        assert call_args[0][1] == "/v1/register-glider/mygroup"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_update_active_deployment_start(
        self,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_build.return_value = mock_http
        mock_http.request.return_value = make_mock_response(200, {"data": {}})

        with SFMCClient(config=config) as client:
            client.update_active_deployment_start("tg", "2026-01-01T00:00:00")

        call_args = mock_http.request.call_args
        assert call_args[0][0] == "PUT"
        assert call_args[0][1] == "/v1/update-active-deployment-start/tg"
        # Check startDateTime param
        assert "params" in call_args.kwargs


class TestPathValidation:
    """Tests for _validate_path_segment."""

    @pytest.mark.parametrize(
        "bad_value",
        ["../../etc", "foo/bar", "", "ok\x00bad", "../passwd"],
    )
    def test_rejects_bad_path_segments(self, bad_value: str) -> None:
        from sfmc_api.client import _validate_path_segment

        with pytest.raises(ValueError):
            _validate_path_segment(bad_value, "test")

    def test_accepts_valid_segment(self) -> None:
        from sfmc_api.client import _validate_path_segment

        assert _validate_path_segment("osusim", "glider") == "osusim"
        assert _validate_path_segment("from-glider", "folder") == "from-glider"
        assert _validate_path_segment("data.sbd", "file") == "data.sbd"
