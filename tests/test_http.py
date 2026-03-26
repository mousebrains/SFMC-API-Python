"""Tests for sfmc_api._http."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from sfmc_api._http import check_response
from sfmc_api.exceptions import APIError, RateLimitError


def _mock_response(
    status: int, headers: dict[str, str] | None = None, text: str = ""
) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.is_success = 200 <= status < 300
    r.headers = headers or {}
    r.text = text
    return r


def test_success() -> None:
    check_response(_mock_response(200))


def test_rate_limit() -> None:
    r = _mock_response(429, headers={"x-rate-limit-retry-after-milliseconds": "3000"})
    with pytest.raises(RateLimitError) as exc_info:
        check_response(r)
    assert exc_info.value.retry_after_seconds == 3.0


def test_rate_limit_missing_header() -> None:
    r = _mock_response(429)
    with pytest.raises(RateLimitError) as exc_info:
        check_response(r)
    assert exc_info.value.retry_after_seconds == 0.0


def test_api_error() -> None:
    r = _mock_response(404, text="not found")
    with pytest.raises(APIError) as exc_info:
        check_response(r)
    assert exc_info.value.status_code == 404
    assert exc_info.value.response_body == "not found"


def test_server_error() -> None:
    r = _mock_response(500, text="internal")
    with pytest.raises(APIError) as exc_info:
        check_response(r)
    assert exc_info.value.status_code == 500
