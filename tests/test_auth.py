"""Tests for sfmc_api.auth."""

from __future__ import annotations

from unittest.mock import MagicMock

import httpx
import pytest

from sfmc_api.auth import authenticate
from sfmc_api.config import SFMCConfig
from sfmc_api.exceptions import AuthenticationError
from tests.conftest import make_mock_response


class TestAuthenticate:
    def test_success(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = make_mock_response(200, {"token": "tok123"})

        token = authenticate(http, config)

        assert token == "tok123"
        http.post.assert_called_once_with(
            "/signin",
            json={"clientId": "cid", "secret": "sec"},
        )

    def test_bad_credentials(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = make_mock_response(401)

        with pytest.raises(AuthenticationError, match="Authentication failed"):
            authenticate(http, config)

    def test_missing_token_key(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = make_mock_response(200, {"not_token": "x"})

        with pytest.raises(AuthenticationError, match="missing 'token'"):
            authenticate(http, config)

    def test_network_error(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.side_effect = httpx.ConnectError("connection refused")

        with pytest.raises(AuthenticationError, match="Authentication failed"):
            authenticate(http, config)

    def test_token_coerced_to_str(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = make_mock_response(200, {"token": 12345})

        token = authenticate(http, config)
        assert token == "12345"
        assert isinstance(token, str)

    def test_rate_limit_wrapped(self, config: SFMCConfig) -> None:
        http = MagicMock(spec=httpx.Client)
        http.post.return_value = make_mock_response(
            429, headers={"x-rate-limit-retry-after-milliseconds": "5000"}
        )

        with pytest.raises(AuthenticationError, match="Authentication failed"):
            authenticate(http, config)

    def test_malformed_json_wrapped(self, config: SFMCConfig) -> None:
        """ValueError from response.json() is wrapped as AuthenticationError."""
        http = MagicMock(spec=httpx.Client)
        resp = make_mock_response(200)
        resp.json.side_effect = ValueError("malformed JSON")
        http.post.return_value = resp

        with pytest.raises(AuthenticationError, match="Authentication failed"):
            authenticate(http, config)
