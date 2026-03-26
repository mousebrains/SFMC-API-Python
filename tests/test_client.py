"""Tests for sfmc_api.client.SFMCClient."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from sfmc_api.client import SFMCClient
from sfmc_api.config import SFMCConfig
from sfmc_api.exceptions import APIError, AuthenticationError
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
        """Raw httpx transport errors are wrapped as APIError."""
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.side_effect = httpx.ConnectError("connection refused")
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client, pytest.raises(APIError) as exc_info:
            client.get_glider_details("g1")
        assert exc_info.value.status_code == 0
        assert "connection refused" in exc_info.value.response_body


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
