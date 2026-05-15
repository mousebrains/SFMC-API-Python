# Follow Glider

`sfmc-follow` monitors a glider's real-time dialog output, parses
telemetry from each surfacing, feeds it to a user-supplied **follower
plugin**, and uploads any files the follower generates back to SFMC.

## Quick Start

```bash
# Install with follower support
pip install -e '.[follow]'

# Offline test with the drifter follower example
sfmc-follow --glider osu685 \
    --follower examples/drifter_follower.py \
    --config examples/drifter_config.yaml \
    --replay dialog.log --dry-run

# Live mode
sfmc-follow --glider osu685 \
    --follower examples/drifter_follower.py \
    --config examples/drifter_config.yaml
```

## How It Works

The data flow is shown in [`docs/follow_dataflow.svg`](follow_dataflow.svg):

```
Source (STOMP or log file)
  → StompSubscription queue
  → ordered_dialog()        -- reorder by sequence number
  → _read_dialog()          -- reassemble line fragments
  → DialogParser            -- state machine: GPS + sensors → SurfacingEvent
  → queue_in
  → Your Follower           -- on_surfacing() → send_files()
  → queue_out
  → Upload to SFMC  or  Print (--dry-run)
```

Both live and replay modes produce a `StompSubscription` and feed
through the exact same pipeline.  The only difference is the data
source (WebSocket vs. log file).

## Simulation Modes

Two flags combine into four modes:

| Mode | `--replay` | `--dry-run` | Needs SFMC? |
|------|-----------|-------------|-------------|
| Live + upload (default) | no | no | yes |
| Live + print only | no | yes | yes |
| Replay + upload | yes | no | yes |
| Replay + print (offline) | yes | yes | **no** |

**Recommended workflow:**

1. Develop your follower with `--replay --dry-run` (no server needed)
2. Test against SFMC with `--replay` (uploads real files from log data)
3. Shadow a live glider with `--dry-run` (verify output without affecting the glider)
4. Go live (default mode)

## Writing a Follower

A follower is a Python class that extends `BaseFollower`.  You only
need to implement one method: `on_surfacing()`.

```python
from sfmc_api.follower import BaseFollower
from sfmc_api.dialog_parser import SurfacingEvent
from sfmc_api.ma_writer import generate_goto_ma

class MyFollower(BaseFollower):
    def on_surfacing(self, event: SurfacingEvent) -> None:
        # event.gps_lat, event.gps_lon  -- decimal degrees
        # event.sensors["m_water_vx"]   -- SensorReading with .value, .unit
        # event.vehicle_name, event.timestamp, etc.

        waypoints = [(-117.70, 33.17), (-117.69, 33.18)]
        filename, content = generate_goto_ma(
            waypoints, self.config["sequence_number"],
        )
        self.send_files(to_glider={filename: content})
```

Save it as `my_follower.py` and run:

```bash
sfmc-follow --glider osu685 --follower my_follower.py \
    --config my_config.yaml --replay dialog.log --dry-run
```

The `--class` flag is only needed if your file contains more than one
`BaseFollower` subclass.

See [`src/sfmc_api/follower.py`](../src/sfmc_api/follower.py) for the
full `BaseFollower` tutorial docstring, and
[`examples/drifter_follower.py`](../examples/drifter_follower.py) for a
complete real-world example.

## Drifter Follower Example

The included drifter follower tracks a drifting ocean target (e.g. a
surface drifter) by reading its position from a NetCDF file and
generating `goto_l*.ma` waypoint plans that keep the glider flying a
geometric pattern around the drifter.

```bash
pip install -e '.[drifter]'   # adds netCDF4, numpy, pyyaml

sfmc-follow --glider osu685 \
    --follower examples/drifter_follower.py \
    --config examples/drifter_config.yaml \
    --replay dialog.log --dry-run
```

See [`examples/drifter_config.yaml`](../examples/drifter_config.yaml)
for the configuration reference with geometry pattern examples.

## CLI Reference

```
sfmc-follow --glider NAME --follower FILE [options]

Required:
  --glider NAME           Glider name
  --follower FILE         Python file with follower class

Optional:
  --class NAME            Class name (auto-detected if only one)
  --config FILE           YAML config passed to follower
  --hostname HOST         SFMC server hostname
  --credentials PATH      Credentials JSON file

Simulation:
  --replay LOGFILE        Replay from log file instead of live STOMP
  --replay-interval SECS  Delay between events during replay (default: 10)
  --dry-run               Print output instead of uploading
  --strict                Exit non-zero if any upload error occurred

Logging:
  --logfile FILE          Log file path
  --log-level LEVEL       DEBUG, INFO, WARNING, ERROR (default: INFO)
  --log-max-size BYTES    Max log size before rotation (default: 10 MB)
  --log-backup-count N    Rotated backup files to keep (default: 5)
```

## End-of-run summary

Every run prints a one-line summary just before exiting:

```
2026-05-15T12:00:00 sfmc.osu685.FOLLOW  Done. surfacings=12, files_emitted=12, upload_errors=0
```

- `surfacings` — number of `SurfacingEvent`s the parser produced and
  delivered to your follower.
- `files_emitted` — number of files actually uploaded (or printed in
  `--dry-run`).
- `upload_errors` — number of upload attempts that failed.  A non-zero
  count is logged with full tracebacks at the time of failure; the
  count is recapped here so you cannot miss it.

When `upload_errors > 0` and you passed `--strict`, the process exits
with status 2 instead of 0.  This is intended for unattended
deployments (cron, systemd) where you want a non-zero exit to trigger
alerting.

The summary is also returned programmatically as a
`sfmc_api.RunStats` instance — see the
[Programmatic API](#programmatic-api) section.

## Programmatic API

```python
from sfmc_api import RunStats, SFMCClient
from sfmc_api.follow_glider import follow_glider
from my_follower import MyFollower

# Live mode — returns a RunStats summarising the run.
with SFMCClient() as client:
    stats: RunStats = follow_glider(
        client, "osu685", MyFollower,
        follower_config={"sequence_number": 30},
    )
    print(stats.format())  # e.g. "surfacings=12, files_emitted=12, upload_errors=0"

# Offline replay (no client needed)
stats = follow_glider(
    client=None,
    glider_name="osu685",
    follower_class=MyFollower,
    replay="dialog.log",
    dry_run=True,
)
if stats.had_errors():
    raise SystemExit(2)
```
