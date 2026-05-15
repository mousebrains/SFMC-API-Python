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
from .coordinates import dddmm_to_decimal, decimal_to_dddmm, km_to_degrees
from .dialog_parser import DialogParser, SensorReading, SurfacingEvent
from .exceptions import (
    APIError,
    AuthenticationError,
    ConfigError,
    RateLimitError,
    SFMCError,
)
from .follow_glider import RunStats, follow_glider
from .follower import BaseFollower, load_follower_class
from .ma_writer import MAX_WAYPOINTS, generate_goto_ma
from .stomp import MAX_SEQUENCE, StompConnection, StompError, StompSubscription

__all__ = [
    "MAX_SEQUENCE",
    "MAX_WAYPOINTS",
    "APIError",
    "AuthenticationError",
    "BaseFollower",
    "ConfigError",
    "DialogParser",
    "RateLimitError",
    "RunStats",
    "SFMCClient",
    "SFMCConfig",
    "SFMCError",
    "SensorReading",
    "StompConnection",
    "StompError",
    "StompSubscription",
    "SurfacingEvent",
    "dddmm_to_decimal",
    "decimal_to_dddmm",
    "follow_glider",
    "generate_goto_ma",
    "km_to_degrees",
    "load_follower_class",
]

__version__ = "0.2.0"
