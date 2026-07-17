# Monitor Glider

`sfmc-monitor-glider` streams a glider's real-time dialog output and
script state transitions to the console and/or a log file.

If terms like "dialog output," "script," or "deployment" are
unfamiliar, see the [glossary](glossary.md).

## Usage

```bash
# Log to file and stderr
sfmc-monitor-glider osu685 dialog.log

# Stderr only
sfmc-monitor-glider osu685

# Specify host (for multi-host credentials files)
sfmc-monitor-glider --host gliderfmc1.ceoas.oregonstate.edu osu685

# Custom credentials file
sfmc-monitor-glider --credentials /path/to/creds.json osu685

# Let systemd restart the process after a stream loss instead
sfmc-monitor-glider --no-reconnect osu685
```

Press **Ctrl-C** to stop.

## What It Does

The tool connects to the SFMC STOMP streaming interface and subscribes
to two topics:

1. **Dialog output** (`/topic/glider-link-output/{gliderId}`) --
   everything the glider sends during an Iridium surfacing: GPS
   fixes, sensor readings, file transfers, abort history, etc.

2. **Script events** (`/topic/glider-script-assignment-updates/{gliderId}`)
   -- state transitions of the assigned script (running, paused, etc.).

Each line is logged with a high-resolution UTC timestamp:

```
2026-03-28T20:40:38.123456 sfmc.osu685.DIALOG  Vehicle Name: osu685
2026-03-28T20:40:38.234567 sfmc.osu685.DIALOG  GPS Location:  3310.021 N -11741.800 E measured     64.746 secs ago
2026-03-28T20:40:39.345678 sfmc.osu685.SCRIPT  state=running name=sfmc.xml type=factory paused=False
```

## Reconnection and delivery gaps

The command reconnects automatically when the WebSocket/STOMP session
closes, reports an error, or fails during replacement-session setup. It opens
a new connection and both subscriptions, refreshing authentication first.
Retry delays start at 15 seconds, double to a 300-second cap, include up to
20% jitter, and reset after a session stays subscribed for 60 seconds.
Ctrl-C, SIGTERM, and a programmatic stop interrupt the wait immediately.

A reconnect restores future delivery only. These live topics provide no
cursor or history operation, so dialog lines or script transitions published
while the process is offline cannot be recovered. Every lost subscribed
session therefore writes a stable marker such as:

```
STREAM_BOUNDARY session=2 reason=stomp-error
```

Treat that marker as evidence of a possible gap. `sfmc-follow --replay`
recognizes it and resets parser state, preventing telemetry on opposite sides
of the outage from being combined. An unterminated dialog fragment is also
discarded at a transient boundary.

Use `--no-reconnect` to make an unexpected stream loss exit nonzero. This is
useful with `Restart=on-failure`; intentional Ctrl-C or SIGTERM still performs
clean shutdown.

## Disconnect email alerts

For an unattended service, an email can be sent when the SFMC connection stays
down. Most drops reconnect within seconds and are not worth an alert, so
nothing is sent until the connection has been down continuously for
`--notify-after` seconds (default 300). While it stays down, a reminder repeats
every `--notify-repeat` seconds (default 3600; `0` sends a single alert per
outage; minimum 60), and a single all-clear is sent when the stream comes
back. A drop that recovers before the threshold sends nothing.

Two hardening details worth knowing:

- A reconnect only ends the outage after the new session survives 60
  seconds. A stream that subscribes and dies over and over — SFMC
  half-alive — therefore counts as one continuous outage and still
  alerts, instead of resetting the clock on every short-lived session.
- If the process exits (fatal error) while an alerted outage is still
  open, a final "exiting — no all-clear will follow" notice is sent, so
  a sent DOWN alert is never left dangling.

```bash
# Alert two addresses if SFMC is unreachable for 5 minutes, hourly reminders
sfmc-monitor-glider --notify-email ops@example.org --notify-email pat@example.org \
    osu685 /var/log/sfmc/osu685.log
```

Email is off unless at least one `--notify-email` is given. Delivery uses a
local SMTP relay by default (`--smtp-host`, default `localhost`; `--smtp-port`,
default 25; `--smtp-timeout`, default 10 s; no authentication or TLS) — the
usual Debian/Ubuntu setup where a local MTA forwards to a campus mail server. The From address defaults to
`sfmc-monitor-glider@<fqdn>` (override with `--notify-from`). Sending happens
on a background thread with a few retries per message, so a slow or
briefly-restarting mail server neither stalls reconnection nor eats the alert.

Do not combine email alerting with `--no-reconnect`: the process exits on the
first stream loss, before `--notify-after` can elapse, so the alert never
fires (a startup warning says so). With `--no-reconnect`, use your service
manager's failure alerting (e.g. systemd `OnFailure=`) instead.

## Log Files

When a log file path is provided, output goes to both the file and
stderr.  The log file can later be replayed through `sfmc-follow
--replay` for offline testing of follower plugins.

## Running as a service

A tuned, hardened example systemd unit for Debian/Ubuntu — dedicated
service user, sandboxing, log directory, restart policy, and email
alerting wired in — lives in `examples/systemd/`, with an install
walkthrough in its README (service user, virtualenv, credentials in
`/etc/sfmc`, local mail relay, logrotate).

## Programmatic Use

```python
from sfmc_api import SFMCClient
from sfmc_api.monitor_glider import ordered_dialog

with SFMCClient() as client:
    with client.open_stream() as stomp:
        sub = client.subscribe_glider_output("osu685", stomp)
        for data in ordered_dialog(sub):
            print(data, end="")
```

This low-level example owns only one connection. Application code using
`open_stream()` directly must recreate its connection and subscriptions after
closure; transparent reconnect belongs above the transport because only the
application knows which subscriptions and state to restore.
