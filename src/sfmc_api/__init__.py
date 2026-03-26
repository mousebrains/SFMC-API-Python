"""Python client for the Slocum Fleet Management Center (SFMC) REST API.

Quick start::

    from sfmc_api import SFMCClient

    with SFMCClient() as client:
        details = client.get_glider_details("my-glider")
        print(details)

Configuration is loaded from ``~/.config/sfmc/credentials.json`` by
default.  See :class:`SFMCConfig` for alternative ways to supply
credentials.
"""

from .client import SFMCClient
from .config import SFMCConfig
from .exceptions import (
    APIError,
    AuthenticationError,
    ConfigError,
    RateLimitError,
    SFMCError,
)
from .stomp import MAX_SEQUENCE, StompConnection, StompError, StompSubscription

__all__ = [
    "MAX_SEQUENCE",
    "APIError",
    "AuthenticationError",
    "ConfigError",
    "RateLimitError",
    "SFMCClient",
    "SFMCConfig",
    "SFMCError",
    "StompConnection",
    "StompError",
    "StompSubscription",
]

__version__ = "0.1.0"
