"""Shared reconnect backoff policy for long-running stream commands."""

from __future__ import annotations

import random
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from math import isfinite

__all__ = ["ReconnectBackoff", "ReconnectDelay", "safe_stream_error"]

_TOKEN_QUERY_RE = re.compile(r"(access_token=)[^&\s]+", re.IGNORECASE)
_BEARER_RE = re.compile(r"(Bearer\s+)[^\s,;]+", re.IGNORECASE)


def safe_stream_error(exc: BaseException) -> str:
    """Format an exception without exposing a token-bearing URL."""
    detail = _TOKEN_QUERY_RE.sub(r"\1<redacted>", f"{type(exc).__name__}: {exc}")
    return _BEARER_RE.sub(r"\1<redacted>", detail)


@dataclass(frozen=True)
class ReconnectDelay:
    """One reconnect decision returned by :class:`ReconnectBackoff`."""

    attempt: int
    nominal: float
    actual: float


@dataclass
class ReconnectBackoff:
    """Calculate capped exponential reconnect delays with bounded jitter.

    The object only owns policy state. Callers remain responsible for
    classifying failures and waiting in a stop-aware manner.
    """

    initial_delay: float = 15.0
    max_delay: float = 300.0
    stable_after: float = 60.0
    jitter: float = 0.2
    random_uniform: Callable[[float, float], float] = field(
        default=random.uniform,
        repr=False,
    )
    _nominal: float = field(init=False, repr=False)
    _attempt: int = field(init=False, default=0, repr=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("initial_delay", self.initial_delay),
            ("max_delay", self.max_delay),
            ("stable_after", self.stable_after),
            ("jitter", self.jitter),
        ):
            if not isfinite(value):
                raise ValueError(f"{name} must be finite")
        if self.initial_delay < 0:
            raise ValueError("initial_delay must be >= 0")
        if self.max_delay < self.initial_delay:
            raise ValueError("max_delay must be >= initial_delay")
        if self.stable_after < 0:
            raise ValueError("stable_after must be >= 0")
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError("jitter must be between 0 and 1")
        self._nominal = self.initial_delay

    def next_delay(self, *, subscribed_uptime: float | None) -> ReconnectDelay:
        """Return the next delay and advance the failure sequence.

        Only time spent in a successfully subscribed session is eligible to
        reset backoff. Failed handshakes pass ``None``.
        """
        if subscribed_uptime is not None and subscribed_uptime >= self.stable_after:
            self._nominal = self.initial_delay
            self._attempt = 0

        nominal = self._nominal
        self._attempt += 1
        if self.jitter:
            low = max(0.0, nominal * (1.0 - self.jitter))
            high = min(self.max_delay, nominal * (1.0 + self.jitter))
            actual = self.random_uniform(low, high)
        else:
            actual = nominal

        self._nominal = min(nominal * 2.0, self.max_delay)
        return ReconnectDelay(
            attempt=self._attempt,
            nominal=nominal,
            actual=min(max(actual, 0.0), self.max_delay),
        )
