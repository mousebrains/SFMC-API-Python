"""Exception hierarchy for the SFMC API client.

All exceptions inherit from :class:`SFMCError`, so callers can catch
that single base class to handle any SFMC-related failure.
"""

__all__ = ["APIError", "AuthenticationError", "ConfigError", "RateLimitError", "SFMCError"]


class SFMCError(Exception):
    """Base exception for all SFMC client errors."""


class ConfigError(SFMCError):
    """Configuration is missing, unreadable, or malformed."""


class AuthenticationError(SFMCError):
    """Sign-in failed (bad credentials, network error, unexpected response)."""


class RateLimitError(SFMCError):
    """Server returned HTTP 429 — too many requests.

    Attributes:
        retry_after_seconds: How long the server asks us to wait before retrying.
    """

    def __init__(self, retry_after_seconds: float, message: str = "") -> None:
        self.retry_after_seconds = retry_after_seconds
        super().__init__(message or f"Rate limited. Retry after {retry_after_seconds:.1f}s")


class APIError(SFMCError):
    """Non-success HTTP response from the SFMC API.

    Attributes:
        status_code: The HTTP status code returned by the server.
        response_body: The raw response body, if available.
    """

    def __init__(self, status_code: int, response_body: str = "") -> None:
        self.status_code = status_code
        self.response_body = response_body
        if status_code == 0 and response_body:
            # Transport failure — there is no HTTP status to show, and
            # the description (exception type, attempt count, whether a
            # retry was withheld) is the only useful information.
            message = response_body
        elif response_body:
            # Operators diagnosing a failed service from its logs need
            # the server's words, not just the status number.
            message = f"SFMC API error: HTTP {status_code}: {response_body[:200]}"
        else:
            message = f"SFMC API error: HTTP {status_code}"
        super().__init__(message)
