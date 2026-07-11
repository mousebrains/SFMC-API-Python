# sfmc-api

[![CI](https://github.com/mousebrains/SFMC-API-Python/actions/workflows/ci.yml/badge.svg)](https://github.com/mousebrains/SFMC-API-Python/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mousebrains/SFMC-API-Python/graph/badge.svg)](https://codecov.io/gh/mousebrains/SFMC-API-Python)
[![PyPI](https://img.shields.io/pypi/v/sfmc-api)](https://pypi.org/project/sfmc-api/)
[![Python](https://img.shields.io/pypi/pyversions/sfmc-api)](https://pypi.org/project/sfmc-api/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

Python client for the Slocum Fleet Management Center (SFMC) REST API.

SFMC is the web service Slocum glider pilots use to monitor and
command autonomous underwater gliders.  This library lets you script
against it from Python, query glider state, upload mission plans, and
stream real-time dialog events.  If terms like "glider," "deployment,"
or "yo" are unfamiliar, start with the [glossary](docs/glossary.md).

## Installation

We recommend a Python virtual environment for any new project.  See
[Getting Started](docs/getting_started.md) for a step-by-step setup,
including how to obtain SFMC API credentials.

```bash
python3 -m venv venv
source venv/bin/activate
pip install -e .
sfmc-api init      # interactive credentials setup
sfmc-api auth      # verify your credentials work
```

## Quick Start

### Python API

```python
from sfmc_api import SFMCClient

with SFMCClient() as client:
    details = client.get_glider_details("osu685")
    print(details)
```

### Command-Line Interface

```bash
sfmc-api init                                    # one-time: set up credentials
sfmc-api auth                                    # test credentials
sfmc-api get-glider-details osu685               # query a glider
sfmc-api get-waypoint-plan osu685                # get navigation plan
sfmc-api get-folder-file-listing osu685 from-glider  # list files
sfmc-api subscribe-connection-events osu685      # stream events (Ctrl-C to stop)
sfmc-api --compact get-glider-details osu685     # single-line JSON
sfmc-api --help                                  # see all subcommands
```

Destructive commands (`delete-*`, `clear-assigned-script`) prompt for
confirmation by default; pass `-y` / `--yes` or set
`SFMC_ASSUME_YES=1` to skip the prompt in scripts and services.

### [Monitor a Glider](docs/monitor_glider.md)

Stream a glider's real-time dialog output and script state transitions
to the console and/or a log file:

```bash
sfmc-monitor-glider osu685 dialog.log
sfmc-monitor-glider --host gliderfmc1.ceoas.oregonstate.edu osu685
```

See [docs/monitor_glider.md](docs/monitor_glider.md) for details.

### [Pull New Downloads](docs/pull_new_downloads.md)

Mirror new `from-glider` files into a local directory as the glider
sends them, driven by real-time events (no polling while the glider
is underwater):

```bash
sfmc-pull-new-downloads --host gliderfmc1.ceoas.oregonstate.edu osu685 /data/osu685
sfmc-pull-new-downloads --once osu685 /data/osu685   # cron-friendly catch-up pass
```

See [docs/pull_new_downloads.md](docs/pull_new_downloads.md) for how
batches, rename delays, and glider-clock timestamps are handled.

### [Follow a Glider (Autonomous Navigation)](docs/follow_glider.md)

Run a follower plugin that watches each surfacing, generates new
navigation files (e.g. waypoint plans), and uploads them to SFMC:

```bash
# Offline test: replay a log file, print what the follower generates
sfmc-follow --glider osu685 --follower examples/drifter_follower.py \
            --config examples/drifter_config.yaml \
            --replay dialog.log --dry-run

# Live: monitor the glider and upload generated files
sfmc-follow --glider osu685 --follower examples/drifter_follower.py \
            --config examples/drifter_config.yaml
```

The included drifter follower example tracks a drifting target using
a NetCDF position file and generates `goto_l*.ma` waypoint plans.
See [docs/follow_glider.md](docs/follow_glider.md) for the full guide.

### Integration Tests

```bash
# Run live integration tests against an SFMC server
sfmc-api-test --host gliderfmc1.ceoas.oregonstate.edu --glider osu685
```

## Configuration

Credentials are loaded from `~/.config/sfmc/credentials.json` by default.

### Multi-Host Format (Recommended)

```json
{
    "gliderfmc1.ceoas.oregonstate.edu": {
        "apiCredentials": {
            "clientId": "YOUR_CLIENT_ID",
            "secret": "YOUR_SECRET"
        }
    },
    "sfmc-backup.example.com": {
        "apiCredentials": {
            "clientId": "OTHER_ID",
            "secret": "OTHER_SECRET"
        }
    }
}
```

Select a host with `--host`:

```bash
sfmc-api --host sfmc-backup.example.com get-glider-details osu685
```

If the file has only one host, it is selected automatically.

See [docs/configuration.md](docs/configuration.md) for full details.

## Documentation

- [Getting Started](docs/getting_started.md) -- installation and configuration
- [Glossary](docs/glossary.md) -- term definitions for non-experts
- [Troubleshooting](docs/troubleshooting.md) -- common errors and fixes
- [Authentication](docs/authentication.md) -- auth data flow
- [Configuration](docs/configuration.md) -- config resolution and JSON schema
- [CLI Reference](docs/cli.md) -- command-line interface
- [Glider Management](docs/glider_management.md) -- queries, registration, deployment
- [Plans](docs/plans.md) -- mission plan queries, updates, and deployments
- [Script Control](docs/script_control.md) -- script assignment and commands
- [File Operations](docs/file_operations.md) -- upload, download, and delete
- [Real-Time Streaming](docs/streaming.md) -- STOMP over SockJS event streaming
- [Monitor Glider](docs/monitor_glider.md) -- real-time dialog and script monitoring
- [Follow Glider](docs/follow_glider.md) -- autonomous follower plugins and simulation modes

## Getting Help

- For usage questions, start with [Troubleshooting](docs/troubleshooting.md).
- Report bugs or request features at
  <https://github.com/mousebrains/SFMC-API-Python/issues>.

## License

GPL-3.0-or-later
