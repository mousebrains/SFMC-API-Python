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

## Log Files

When a log file path is provided, output goes to both the file and
stderr.  The log file can later be replayed through `sfmc-follow
--replay` for offline testing of follower plugins.

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
