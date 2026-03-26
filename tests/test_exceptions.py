"""Tests for sfmc_api.exceptions."""

from sfmc_api.exceptions import (
    APIError,
    AuthenticationError,
    ConfigError,
    RateLimitError,
    SFMCError,
)


def test_hierarchy() -> None:
    assert issubclass(AuthenticationError, SFMCError)
    assert issubclass(RateLimitError, SFMCError)
    assert issubclass(APIError, SFMCError)
    assert issubclass(ConfigError, SFMCError)


def test_rate_limit_error() -> None:
    e = RateLimitError(retry_after_seconds=2.5)
    assert e.retry_after_seconds == 2.5
    assert "2.5" in str(e)


def test_rate_limit_error_custom_message() -> None:
    e = RateLimitError(retry_after_seconds=1.0, message="custom")
    assert str(e) == "custom"


def test_api_error() -> None:
    e = APIError(status_code=404, response_body='{"error":"not found"}')
    assert e.status_code == 404
    assert e.response_body == '{"error":"not found"}'
    assert "404" in str(e)
