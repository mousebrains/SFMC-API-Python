"""Tests for sfmc_api.client.SFMCClient."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from sfmc_api.client import SFMCClient
from sfmc_api.config import SFMCConfig
from sfmc_api.exceptions import APIError, AuthenticationError


@pytest.fixture()
def config() -> SFMCConfig:
    return SFMCConfig(host="sfmc.test", client_id="cid", secret="sec", tls_verify=False)


def _mock_response(status: int = 200, json_data: dict[str, Any] | None = None) -> MagicMock:
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.is_success = 200 <= status < 300
    r.json.return_value = json_data or {}
    r.text = ""
    r.headers = {}
    return r


class TestConstruction:
    def test_from_config_object(self, config: SFMCConfig) -> None:
        client = SFMCClient(config=config)
        assert client._config is config
        client.close()

    def test_from_config_file(self, tmp_path: Path) -> None:
        p = tmp_path / "creds.json"
        p.write_text('{"host":"h","apiCredentials":{"clientId":"c","secret":"s"}}')
        client = SFMCClient(config_path=p)
        assert client._config.host == "h"
        client.close()

    def test_context_manager(self, config: SFMCConfig) -> None:
        with SFMCClient(config=config) as client:
            assert client._config is config
        # After exiting, the httpx client should be closed

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
        mock_http.request.return_value = _mock_response(200, {"data": {"id": 1, "name": "g1"}})
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
        mock_http.request.return_value = _mock_response(200, {"data": {}})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.get_glider_details("g1")
            client.get_glider_details("g2")

        # authenticate() should only be called once
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
        mock_http.request.return_value = _mock_response(200, {"ok": True})
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
        resp = _mock_response(404)
        resp.text = "not found"
        mock_http.request.return_value = resp
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client, pytest.raises(APIError) as exc_info:
            client.get_glider_details("nope")
        assert exc_info.value.status_code == 404


class TestGliderOnlyMethods:
    """Test that glider-name-only methods build the correct request path."""

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def _call_method(
        self,
        method_name: str,
        expected_path: str,
        mock_build: MagicMock,
        mock_auth: MagicMock,
        config: SFMCConfig,
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = _mock_response(200, {"data": {}})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            method = getattr(client, method_name)
            method("testglider")

        call_args = mock_http.request.call_args
        assert call_args[0][0] == "GET"
        assert call_args[0][1] == expected_path

    def test_get_glider_details(self, config: SFMCConfig) -> None:
        self._call_method("get_glider_details", "/v1/gliders/testglider", config=config)

    def test_get_active_deployment(self, config: SFMCConfig) -> None:
        self._call_method(
            "get_active_deployment_details",
            "/v1/active-deployment/testglider",
            config=config,
        )

    def test_get_mission_plan(self, config: SFMCConfig) -> None:
        self._call_method(
            "get_mission_plan",
            "/v1/glider-assigned-mission-plan/testglider",
            config=config,
        )

    def test_get_waypoint_plan(self, config: SFMCConfig) -> None:
        self._call_method(
            "get_waypoint_plan",
            "/v1/glider-assigned-waypoint-plan/testglider",
            config=config,
        )

    def test_get_abort_plan(self, config: SFMCConfig) -> None:
        self._call_method(
            "get_abort_plan",
            "/v1/glider-assigned-abort-plan/testglider",
            config=config,
        )

    def test_get_available_scripts(self, config: SFMCConfig) -> None:
        self._call_method(
            "get_available_scripts",
            "/v1/scripts-for-glider/testglider",
            config=config,
        )


class TestScriptControl:
    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_set_assigned_script_path(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = _mock_response(200, {})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.set_assigned_script("g1", "factory", "sfmc.xml")

        call_args = mock_http.request.call_args
        assert call_args[0][0] == "PUT"
        assert call_args[0][1] == "/v1/set-assigned-script/g1/factory/sfmc.xml"

    @patch("sfmc_api.client.authenticate", return_value="tok")
    @patch("sfmc_api.client.build_http_client")
    def test_send_command(
        self, mock_build: MagicMock, mock_auth: MagicMock, config: SFMCConfig
    ) -> None:
        mock_http = MagicMock(spec=httpx.Client)
        mock_http.request.return_value = _mock_response(200, {})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.send_command("g1", "put c_science_on 0")

        call_args = mock_http.request.call_args
        assert call_args[0][1] == "/v1/submit-command/g1"
        assert call_args.kwargs["content"] == "put c_science_on 0"


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
        mock_http.request.return_value = _mock_response(200, {"results": []})
        mock_build.return_value = mock_http

        with SFMCClient(config=config) as client:
            client.get_folder_file_listing(
                "g1", "from-glider", page=2, filter="*.sbd", last_modified_after="202601010000"
            )

        call_kwargs = mock_http.request.call_args.kwargs
        assert call_kwargs["params"]["page"] == 2
        assert call_kwargs["params"]["filter"] == "*.sbd"
        assert call_kwargs["params"]["lastModifiedAfter"] == "202601010000"


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
