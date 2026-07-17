"""Tests for the SFMC-disconnect email notifier."""

from __future__ import annotations

import argparse
import threading
import time
from datetime import UTC, datetime

import pytest

from sfmc_api.disconnect_notify import (
    DisconnectNotifier,
    add_notification_cli_args,
    build_notifier,
    make_smtp_send,
)

# ── Test doubles ──────────────────────────────────────────────────────


class _FakeTimerHandle:
    """A ``threading.Timer`` stand-in the test fires by hand."""

    def __init__(self, interval: float, callback):
        self.interval = interval
        self.callback = callback
        self.started = False
        self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True

    def fire(self) -> None:
        """Invoke the callback as the real timer would on expiry."""
        self.callback()


class _TimerRegistry:
    """Factory that records every armed timer for inspection."""

    def __init__(self) -> None:
        self.timers: list[_FakeTimerHandle] = []

    def __call__(self, interval: float, callback) -> _FakeTimerHandle:
        handle = _FakeTimerHandle(interval, callback)
        self.timers.append(handle)
        return handle

    @property
    def latest(self) -> _FakeTimerHandle:
        return self.timers[-1]


class _Recorder:
    """Thread-safe capture of delivered ``(subject, body)`` messages."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.messages: list[tuple[str, str]] = []

    def __call__(self, subject: str, body: str) -> None:
        with self._lock:
            self.messages.append((subject, body))

    def wait_for(self, count: int, timeout: float = 2.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.messages) >= count:
                    return
            time.sleep(0.005)
        raise AssertionError(f"only {len(self.messages)} message(s) delivered, wanted {count}")

    def snapshot(self) -> list[tuple[str, str]]:
        with self._lock:
            return list(self.messages)


class _FakeClock:
    """A manually advanced monotonic clock."""

    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def _make_notifier(
    recorder: _Recorder,
    registry: _TimerRegistry,
    clock: _FakeClock,
    *,
    threshold: float = 300.0,
    repeat: float = 3600.0,
    stable: float = 0.0,
    send_attempts: int = 3,
    send_retry_delay: float = 0.01,
) -> DisconnectNotifier:
    return DisconnectNotifier(
        send_fn=recorder,
        threshold_seconds=threshold,
        repeat_seconds=repeat,
        stable_seconds=stable,
        send_attempts=send_attempts,
        send_retry_delay=send_retry_delay,
        subject_prefix="[SFMC]",
        program="sfmc-test",
        glider_name="osu685",
        host="testhost",
        timer_factory=registry,
        monotonic=clock,
        now_utc=lambda: datetime(2026, 7, 16, 12, 0, 0, tzinfo=UTC),
    )


# ── Outage lifecycle ──────────────────────────────────────────────────


def test_flap_under_threshold_sends_nothing() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_disconnect(reason="dropped")
        assert registry.latest.interval == 300.0
        # Reconnect before the timer fires: a flap, not an outage.
        notifier.record_connect()
        assert registry.latest.cancelled is True
    finally:
        notifier.close()
    assert recorder.snapshot() == []


def test_alert_fires_after_threshold() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_disconnect(reason="ConnectionClosed: gone")
        clock.advance(300.0)
        registry.latest.fire()  # threshold reached, still down
        recorder.wait_for(1)
    finally:
        notifier.close()
    subject, body = recorder.snapshot()[0]
    assert "DOWN" in subject
    assert "osu685" in subject
    assert "sfmc-test on testhost" in subject
    assert "ConnectionClosed: gone" in body
    assert "5.0 min" in body


def test_repeat_reminder_rearms_and_wording() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, threshold=300.0, repeat=600.0)
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(300.0)
        registry.latest.fire()  # first alert
        recorder.wait_for(1)
        # Reminder timer armed at the repeat interval.
        assert registry.latest.interval == 600.0
        clock.advance(600.0)
        registry.latest.fire()  # reminder
        recorder.wait_for(2)
    finally:
        notifier.close()
    first_subject = recorder.snapshot()[0][0]
    second_subject = recorder.snapshot()[1][0]
    assert "DOWN" in first_subject and "STILL DOWN" not in first_subject
    assert "STILL DOWN" in second_subject


def test_repeat_zero_is_one_shot() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, repeat=0.0)
    try:
        notifier.record_disconnect(reason="down")
        n_timers_before = len(registry.timers)
        clock.advance(300.0)
        registry.latest.fire()
        recorder.wait_for(1)
        # No new timer armed after a one-shot alert.
        assert len(registry.timers) == n_timers_before
    finally:
        notifier.close()
    assert len(recorder.snapshot()) == 1


def test_recovered_email_after_alert() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, repeat=0.0)
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(300.0)
        registry.latest.fire()
        recorder.wait_for(1)
        clock.advance(120.0)
        notifier.record_connect()  # back after a total of 7 min
        recorder.wait_for(2)
    finally:
        notifier.close()
    recovered_subject, recovered_body = recorder.snapshot()[1]
    assert "RECOVERED" in recovered_subject
    assert "7.0 min" in recovered_body
    # The "was down since" wall-clock must survive the state reset.
    assert "2026-07-16 12:00:00 UTC" in recovered_body
    assert "unknown" not in recovered_body


def test_no_recovered_email_when_no_alert_sent() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_disconnect(reason="down")
        # Reconnect before threshold -> no down alert -> no all-clear.
        notifier.record_connect()
    finally:
        notifier.close()
    assert recorder.snapshot() == []


def test_repeated_disconnect_keeps_original_timer() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_disconnect(reason="first")
        first_timer = registry.latest
        # Further disconnect notices during the same outage must not
        # re-arm the timer (that would push the alert out forever).
        notifier.record_disconnect(reason="second")
        assert registry.latest is first_timer
        assert len(registry.timers) == 1
    finally:
        notifier.close()


def test_connect_when_never_down_is_noop() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_connect()
        notifier.record_connect()
    finally:
        notifier.close()
    assert registry.timers == []
    assert recorder.snapshot() == []


def test_fire_after_reconnect_race_sends_nothing() -> None:
    """A timer that expires just after a reconnect must not alert."""
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_disconnect(reason="down")
        armed = registry.latest
        notifier.record_connect()  # clears the outage first
        armed.fire()  # stale timer fires afterward
    finally:
        notifier.close()
    assert recorder.snapshot() == []


def test_second_outage_after_recovery_alerts_again() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, repeat=0.0)
    try:
        notifier.record_disconnect(reason="first outage")
        clock.advance(300.0)
        registry.latest.fire()
        recorder.wait_for(1)
        notifier.record_connect()
        recorder.wait_for(2)  # recovered
        # A brand-new outage should alert again.
        notifier.record_disconnect(reason="second outage")
        clock.advance(300.0)
        registry.latest.fire()
        recorder.wait_for(3)
    finally:
        notifier.close()
    assert "second outage" in recorder.snapshot()[2][1]


# ── Validation ────────────────────────────────────────────────────────


def test_negative_threshold_rejected() -> None:
    with pytest.raises(ValueError, match="threshold_seconds"):
        DisconnectNotifier(
            send_fn=lambda s, b: None,
            threshold_seconds=-1.0,
            repeat_seconds=0.0,
            subject_prefix="[SFMC]",
            program="p",
            glider_name="g",
        )


def test_negative_repeat_rejected() -> None:
    with pytest.raises(ValueError, match="repeat_seconds"):
        DisconnectNotifier(
            send_fn=lambda s, b: None,
            threshold_seconds=1.0,
            repeat_seconds=-1.0,
            subject_prefix="[SFMC]",
            program="p",
            glider_name="g",
        )


def test_send_failure_retries_and_delivers() -> None:
    """A transient SMTP failure is retried; the alert is not lost."""
    registry, clock = _TimerRegistry(), _FakeClock()
    calls: list[str] = []
    delivered = threading.Event()

    def flaky_send(subject: str, body: str) -> None:
        calls.append(subject)
        if len(calls) == 1:
            raise OSError("smtp down")
        delivered.set()

    notifier = DisconnectNotifier(
        send_fn=flaky_send,
        threshold_seconds=300.0,
        repeat_seconds=0.0,
        stable_seconds=0.0,
        send_attempts=3,
        send_retry_delay=0.01,
        subject_prefix="[SFMC]",
        program="p",
        glider_name="g",
        timer_factory=registry,
        monotonic=clock,
    )
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(300.0)
        registry.latest.fire()
        assert delivered.wait(timeout=2.0), "retry never delivered the alert"
    finally:
        notifier.close()
    assert len(calls) == 2  # attempt 1 failed, attempt 2 delivered


def test_send_exhaustion_does_not_kill_later_messages() -> None:
    """A message dropped after its attempts must not stop the next one."""
    registry, clock = _TimerRegistry(), _FakeClock()
    calls: list[str] = []

    def flaky_send(subject: str, body: str) -> None:
        calls.append(subject)
        if len(calls) == 1:
            raise OSError("smtp down")

    notifier = DisconnectNotifier(
        send_fn=flaky_send,
        threshold_seconds=300.0,
        repeat_seconds=600.0,
        stable_seconds=0.0,
        send_attempts=1,  # no retries: first message is dropped
        send_retry_delay=0.01,
        subject_prefix="[SFMC]",
        program="p",
        glider_name="g",
        timer_factory=registry,
        monotonic=clock,
    )
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(300.0)
        registry.latest.fire()  # dropped after 1 attempt
        clock.advance(600.0)
        registry.latest.fire()  # reminder must still be attempted
        deadline = time.monotonic() + 2.0
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.005)
    finally:
        notifier.close()
    assert len(calls) == 2


# ── CLI wiring ────────────────────────────────────────────────────────


def _parse(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    add_notification_cli_args(parser)
    return parser.parse_args(argv)


def test_cli_defaults() -> None:
    args = _parse([])
    assert args.notify_email is None
    assert args.notify_after == 300.0
    assert args.notify_repeat == 3600.0
    assert args.smtp_host == "localhost"
    assert args.smtp_port == 25
    assert args.notify_from is None


def test_cli_repeatable_email() -> None:
    args = _parse(["--notify-email", "a@x.org", "--notify-email", "b@x.org"])
    assert args.notify_email == ["a@x.org", "b@x.org"]


def test_cli_rejects_bad_email() -> None:
    with pytest.raises(SystemExit):
        _parse(["--notify-email", "not-an-email"])


def test_build_notifier_none_without_recipients() -> None:
    args = _parse([])
    assert build_notifier(args, program="sfmc-test", glider_name="g") is None


def test_build_notifier_active_with_recipients(monkeypatch: pytest.MonkeyPatch) -> None:
    sent: list[tuple[str, list[str], str]] = []

    class FakeSMTP:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            self.host = host

        def __enter__(self) -> FakeSMTP:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def send_message(self, msg) -> None:
            sent.append((msg["Subject"], msg["To"], msg.get_content()))

    monkeypatch.setattr("sfmc_api.disconnect_notify.smtplib.SMTP", FakeSMTP)
    args = _parse(["--notify-email", "ops@x.org", "--notify-after", "0"])
    notifier = build_notifier(args, program="sfmc-test", glider_name="osu685")
    assert notifier is not None
    try:
        # threshold 0 -> the real timer fires almost immediately.
        notifier.record_disconnect(reason="down")
        deadline = time.monotonic() + 2.0
        while not sent and time.monotonic() < deadline:
            time.sleep(0.005)
    finally:
        notifier.close()
    assert sent, "expected an SMTP send"
    subject, to, _ = sent[0]
    assert "DOWN" in subject
    assert to == "ops@x.org"


def test_make_smtp_send_builds_message(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeSMTP:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            captured["host"] = host
            captured["port"] = port
            captured["timeout"] = timeout

        def __enter__(self) -> FakeSMTP:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def send_message(self, msg) -> None:
            captured["from"] = msg["From"]
            captured["to"] = msg["To"]
            captured["subject"] = msg["Subject"]
            captured["has_date"] = msg["Date"] is not None
            captured["has_msgid"] = msg["Message-ID"] is not None

    monkeypatch.setattr("sfmc_api.disconnect_notify.smtplib.SMTP", FakeSMTP)
    send = make_smtp_send(
        host="mail.local",
        port=2525,
        sender="sfmc@host",
        recipients=["a@x.org", "b@x.org"],
        timeout=7.0,
    )
    send("hi", "body text")
    assert captured["host"] == "mail.local"
    assert captured["port"] == 2525
    assert captured["timeout"] == 7.0
    assert captured["from"] == "sfmc@host"
    assert captured["to"] == "a@x.org, b@x.org"
    assert captured["subject"] == "hi"
    assert captured["has_date"] is True
    assert captured["has_msgid"] is True


# ── Flap hardening (stability window) ─────────────────────────────────


def test_flap_before_stable_keeps_episode() -> None:
    """A reconnect that dies inside the stability window must not
    reset the outage clock — the alert still fires with the episode's
    full downtime."""
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, stable=60.0, repeat=0.0)
    try:
        notifier.record_disconnect(reason="first drop")
        alert_timer = registry.latest
        clock.advance(200.0)
        notifier.record_connect()  # subscribe — pending stability
        pending = registry.latest
        assert pending is not alert_timer
        assert pending.interval == 60.0
        clock.advance(30.0)
        notifier.record_disconnect(reason="died again")  # flap!
        assert pending.cancelled is True
        assert len(registry.timers) == 2  # no new alert timer: same episode
        clock.advance(70.0)  # total downtime 300s
        alert_timer.fire()
        recorder.wait_for(1)
    finally:
        notifier.close()
    subject, body = recorder.snapshot()[0]
    assert "DOWN" in subject
    assert "5.0 min" in body  # full episode, not reset by the flap
    assert "died again" in body


def test_stable_reconnect_confirms_and_credits_downtime_to_reconnect() -> None:
    """The recovered email reports downtime up to the reconnect, not up
    to the end of the stability window."""
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, stable=60.0, repeat=0.0)
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(300.0)
        registry.latest.fire()  # DOWN alert
        recorder.wait_for(1)
        clock.advance(60.0)  # reconnect at 360s down
        notifier.record_connect()
        pending = registry.latest
        clock.advance(60.0)  # stability window elapses
        pending.fire()
        recorder.wait_for(2)
    finally:
        notifier.close()
    subject, body = recorder.snapshot()[1]
    assert "RECOVERED" in subject
    assert "6.0 min" in body  # 360s to the reconnect; the window is not downtime


def test_stale_stability_timer_cannot_end_the_episode() -> None:
    """A cancelled stability timer whose callback runs late must not
    confirm an episode a newer pending window owns."""
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, stable=60.0, repeat=0.0)
    try:
        notifier.record_disconnect(reason="down")
        alert_timer = registry.latest
        notifier.record_connect()
        pending_a = registry.latest
        notifier.record_disconnect(reason="flap")  # cancels A
        notifier.record_connect()
        pending_b = registry.latest
        assert pending_b is not pending_a
        pending_a.fire()  # stale callback — must be ignored
        clock.advance(300.0)
        alert_timer.fire()  # episode must still be alive
        recorder.wait_for(1)
        pending_b.fire()  # the *current* window may confirm
        recorder.wait_for(2)  # recovered follows the alert
    finally:
        notifier.close()
    assert "DOWN" in recorder.snapshot()[0][0]
    assert "RECOVERED" in recorder.snapshot()[1][0]


def test_second_connect_during_pending_window_is_noop() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, stable=60.0)
    try:
        notifier.record_disconnect(reason="down")
        notifier.record_connect()
        n_timers = len(registry.timers)
        notifier.record_connect()  # must not arm a second window
        assert len(registry.timers) == n_timers
    finally:
        notifier.close()


# ── Exit notice ───────────────────────────────────────────────────────


def test_record_exit_after_alert_sends_final_notice() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, repeat=0.0)
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(300.0)
        registry.latest.fire()
        recorder.wait_for(1)
        clock.advance(60.0)
        notifier.record_exit(reason="fatal worker error")
        recorder.wait_for(2)
    finally:
        notifier.close()
    subject, body = recorder.snapshot()[1]
    assert "EXITING" in subject
    assert "6.0 min" in body
    assert "fatal worker error" in body
    assert "no RECOVERED" in body


def test_record_exit_without_alert_is_silent() -> None:
    """Crash loops under a service manager must not storm the mailbox:
    no exit notice unless a DOWN alert already went out."""
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(10.0)  # well under the threshold
        notifier.record_exit(reason="process dying")
    finally:
        notifier.close()
    assert recorder.snapshot() == []


def test_record_exit_when_connected_is_silent() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        notifier.record_exit(reason="dying while healthy")
    finally:
        notifier.close()
    assert recorder.snapshot() == []


# ── Reminder / recovery ordering ─────────────────────────────────────


def test_reminder_then_recovered_ordering() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock, repeat=600.0)
    try:
        notifier.record_disconnect(reason="down")
        clock.advance(300.0)
        registry.latest.fire()  # DOWN
        clock.advance(600.0)
        registry.latest.fire()  # STILL DOWN
        clock.advance(60.0)
        notifier.record_connect()  # stable=0 -> immediate RECOVERED
        recorder.wait_for(3)
    finally:
        notifier.close()
    subjects = [s for s, _ in recorder.snapshot()]
    assert "DOWN" in subjects[0] and "STILL DOWN" not in subjects[0]
    assert "STILL DOWN" in subjects[1]
    assert "RECOVERED" in subjects[2]
    assert "16.0 min" in recorder.snapshot()[2][1]  # 960s total


# ── Discretionary events (engine/follower notifications) ─────────────


def test_notify_event_delivers_and_rate_limits() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    try:
        assert notifier.notify_event(
            "float-feed-down",
            "float position feed unavailable",
            "No position for 45 min; holding previous waypoint.",
        )
        # Same key inside the window: dropped.
        assert not notifier.notify_event("float-feed-down", "still down")
        # A different key is independent.
        assert notifier.notify_event("ma-write-failed", "could not generate goto.ma")
        clock.advance(900.0)
        assert notifier.notify_event("float-feed-down", "float feed still unavailable")
        recorder.wait_for(3)
    finally:
        notifier.close()
    subjects = [s for s, _ in recorder.snapshot()]
    assert "float position feed unavailable" in subjects[0]
    assert "osu685" in subjects[0]
    body = recorder.snapshot()[0][1]
    assert "Condition:  float-feed-down" in body
    assert "No position for 45 min" in body


def test_notify_event_after_close_is_refused() -> None:
    recorder, registry, clock = _Recorder(), _TimerRegistry(), _FakeClock()
    notifier = _make_notifier(recorder, registry, clock)
    notifier.close()
    assert not notifier.notify_event("k", "s")
    assert recorder.snapshot() == []


# ── Real-timer integration ───────────────────────────────────────────


def test_close_cancels_pending_real_timers() -> None:
    """close() with live threading.Timer objects armed must cancel them
    and return promptly, sending nothing."""
    recorder = _Recorder()
    notifier = DisconnectNotifier(
        send_fn=recorder,
        threshold_seconds=30.0,
        repeat_seconds=0.0,
        subject_prefix="[SFMC]",
        program="p",
        glider_name="g",
    )
    notifier.record_disconnect(reason="down")
    started = time.monotonic()
    notifier.close()
    assert time.monotonic() - started < 5.0
    assert recorder.snapshot() == []


# ── CLI floor and lazy sender default ────────────────────────────────


def test_cli_repeat_floor() -> None:
    assert _parse(["--notify-repeat", "0"]).notify_repeat == 0.0
    assert _parse(["--notify-repeat", "60"]).notify_repeat == 60.0
    with pytest.raises(SystemExit):
        _parse(["--notify-repeat", "30"])  # storm floor


def test_build_notifier_accepts_partial_namespace() -> None:
    """A hand-built Namespace with only notify_email gets CLI defaults."""
    args = argparse.Namespace(notify_email=["ops@x.org"])
    notifier = build_notifier(args, program="sfmc-test", glider_name="g")
    assert notifier is not None
    notifier.close()


def test_make_smtp_send_lazy_from_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no --notify-from, the From address is <program>@<fqdn>,
    resolved at send time (off the startup path)."""
    captured: dict[str, object] = {}

    class FakeSMTP:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            pass

        def __enter__(self) -> FakeSMTP:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def send_message(self, msg) -> None:
            captured["from"] = msg["From"]

    monkeypatch.setattr("sfmc_api.disconnect_notify.smtplib.SMTP", FakeSMTP)
    monkeypatch.setattr("sfmc_api.disconnect_notify.socket.getfqdn", lambda: "buoy.campus.example")
    send = make_smtp_send(
        host="localhost",
        port=25,
        sender=None,
        recipients=["a@x.org"],
        timeout=5.0,
        program="sfmc-follow",
    )
    send("subj", "body")
    assert captured["from"] == "sfmc-follow@buoy.campus.example"
