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
from .commands import CommandChannel, CommandReply, ReplyPolicy
from .config import SFMCConfig
from .coordinates import dddmm_to_decimal, decimal_to_dddmm, km_to_degrees
from .dialog_parser import DialogParser, SensorReading, SurfacingEvent
from .dialog_stream import DialogLine, LineAssembler, dialog_lines, ordered_dialog
from .engine import BaseControlEngine, EngineRunner, WriteRefused
from .events import (
    DroppedNotice,
    Event,
    EventMerge,
    FleetStream,
    StreamNotice,
)
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
from .ops import OperationExecutor, OperationResult
from .session import GliderSession, Listener
from .stomp import MAX_SEQUENCE, StompConnection, StompError, StompSubscription
from .xml_engine import (
    Action,
    Script,
    ScriptError,
    State,
    Transition,
    XmlStateMachine,
    describe,
    parse_script,
    replay,
    run_live,
)

__all__ = [
    "MAX_SEQUENCE",
    "MAX_WAYPOINTS",
    "APIError",
    "Action",
    "AuthenticationError",
    "BaseControlEngine",
    "BaseFollower",
    "CommandChannel",
    "CommandReply",
    "ConfigError",
    "DialogLine",
    "DialogParser",
    "DroppedNotice",
    "EngineRunner",
    "Event",
    "EventMerge",
    "FleetStream",
    "GliderSession",
    "LineAssembler",
    "Listener",
    "OperationExecutor",
    "OperationResult",
    "RateLimitError",
    "ReplyPolicy",
    "RunStats",
    "SFMCClient",
    "SFMCConfig",
    "SFMCError",
    "Script",
    "ScriptError",
    "SensorReading",
    "State",
    "StompConnection",
    "StompError",
    "StompSubscription",
    "StreamNotice",
    "SurfacingEvent",
    "Transition",
    "WriteRefused",
    "XmlStateMachine",
    "dddmm_to_decimal",
    "decimal_to_dddmm",
    "describe",
    "dialog_lines",
    "follow_glider",
    "generate_goto_ma",
    "km_to_degrees",
    "load_follower_class",
    "ordered_dialog",
    "parse_script",
    "replay",
    "run_live",
]

__version__ = "0.2.0"
