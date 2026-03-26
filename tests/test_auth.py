"""Tests for sfmc_api.auth."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from sfmc_api.auth import authenticate
from sfmc_api.config import SFMCConfig
from sfmc_api.exceptions import AuthenticationError


@pytest.fixture()
def config() -> SFMCConfig:
    return SFMCConfig(host="sfmc.test", client_id="cid", secret="sec")


def _mock_response(status: int, json_data: dict | None = None) -> MagicMock:  # type: ignore[type-arg]
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.is_success = 200 <= status < 300
    r.json.return_value = json_data or {}
    r.text = ""
    r.headers = {}
    return r


class TestAuthenticate:
    def test_success(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = _mock_response(200, {"token": "tok123"})

        token = authenticate(http, config)

        assert token == "tok123"
        http.post.assert_called_once_with(
            "/signin",
            json={"clientId": "cid", "secret": "sec"},
        )

    def test_bad_credentials(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = _mock_response(401)

        with pytest.raises(AuthenticationError, match="Authentication failed"):
            authenticate(http, config)

    def test_missing_token_key(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = _mock_response(200, {"not_token": "x"})

        with pytest.raises(AuthenticationError, match="missing 'token'"):
            authenticate(http, config)

    def test_network_error(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.side_effect = httpx.ConnectError("connection refused")

        with pytest.raises(AuthenticationError, match="Authentication failed"):
            authenticate(http, config)

    def test_token_is_str(self, config: SFMCConfig) -> None:
        """Token is coerced to str even if server returns non-string."""
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = _mock_response(200, {"token": 12345})

        token = authenticate(http, config)
        assert token == "12345"
        assert isinstance(token, str)
