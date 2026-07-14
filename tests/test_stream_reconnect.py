"""Tests for the shared stream reconnect policy."""

from __future__ import annotations

import pytest

from sfmc_api.stream_reconnect import ReconnectBackoff, safe_stream_error


def test_exponential_sequence_caps() -> None:
    backoff = ReconnectBackoff(jitter=0.0)

    delays = [backoff.next_delay(subscribed_uptime=None) for _ in range(8)]

    assert [delay.nominal for delay in delays] == [15, 30, 60, 120, 240, 300, 300, 300]
    assert [delay.attempt for delay in delays] == list(range(1, 9))


def test_stable_subscribed_session_resets_sequence() -> None:
    backoff = ReconnectBackoff(jitter=0.0)
    backoff.next_delay(subscribed_uptime=None)
    backoff.next_delay(subscribed_uptime=10)

    reset = backoff.next_delay(subscribed_uptime=60)

    assert reset.nominal == 15
    assert reset.attempt == 1


def test_slow_failed_handshake_does_not_reset() -> None:
    backoff = ReconnectBackoff(jitter=0.0)
    first = backoff.next_delay(subscribed_uptime=None)
    second = backoff.next_delay(subscribed_uptime=None)

    assert first.nominal == 15
    assert second.nominal == 30


def test_jitter_is_bounded_and_capped() -> None:
    calls: list[tuple[float, float]] = []

    def choose_high(low: float, high: float) -> float:
        calls.append((low, high))
        return high

    backoff = ReconnectBackoff(
        initial_delay=300,
        max_delay=300,
        jitter=0.2,
        random_uniform=choose_high,
    )

    delay = backoff.next_delay(subscribed_uptime=None)

    assert calls == [(240, 300)]
    assert delay.actual == 300


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"initial_delay": -1}, "initial_delay"),
        ({"initial_delay": 10, "max_delay": 5}, "max_delay"),
        ({"stable_after": -1}, "stable_after"),
        ({"jitter": -0.1}, "jitter"),
        ({"jitter": 1.1}, "jitter"),
        ({"initial_delay": float("nan")}, "initial_delay"),
        ({"max_delay": float("inf")}, "max_delay"),
    ],
)
def test_invalid_policy_rejected(kwargs: dict[str, float], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        ReconnectBackoff(**kwargs)


def test_error_formatter_redacts_query_and_bearer_tokens() -> None:
    error = RuntimeError(
        "wss://example.test/ws?access_token=secret&x=1 Authorization: Bearer abc.def"
    )

    detail = safe_stream_error(error)

    assert "secret" not in detail
    assert "abc.def" not in detail
    assert "access_token=<redacted>&x=1" in detail
    assert "Bearer <redacted>" in detail
