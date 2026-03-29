# sfmc-api

[![CI](https://github.com/mousebrains/SFMC-API-Python/actions/workflows/ci.yml/badge.svg)](https://github.com/mousebrains/SFMC-API-Python/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mousebrains/SFMC-API-Python/graph/badge.svg)](https://codecov.io/gh/mousebrains/SFMC-API-Python)
[![PyPI](https://img.shields.io/pypi/v/sfmc-api)](https://pypi.org/project/sfmc-api/)
[![Python](https://img.shields.io/pypi/pyversions/sfmc-api)](https://pypi.org/project/sfmc-api/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

Python client for the Slocum Fleet Management Center (SFMC) REST API.

## Installation

```bash
pip install -e .
```

## Quick Start

### Python API

```python
from sfmc_api import SFMCClient

with SFMCClient() as client:
    details = client.get_glider_details("osusim")
    print(details)
```

### Command-Line Interface

```bash
sfmc-api auth                                    # test credentials
sfmc-api get-glider-details osusim               # query a glider
sfmc-api get-waypoint-plan osusim                # get navigation plan
sfmc-api get-folder-file-listing osusim from-glider  # list files
sfmc-api subscribe-connection-events osusim      # stream events (Ctrl-C to stop)
sfmc-api --compact get-glider-details osusim     # single-line JSON
sfmc-api --help                                  # see all subcommands
```

### Monitor a Glider

Stream a glider's real-time dialog output and script state transitions
to the console and/or a log file:

```bash
sfmc-monitor-glider osusim dialog.log
sfmc-monitor-glider --host gliderfmc1.ceoas.oregonstate.edu osusim
```

See [docs/monitor_glider.md](docs/monitor_glider.md) for details.

### Follow a Glider (Autonomous Navigation)

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
sfmc-api-test --host gliderfmc1.ceoas.oregonstate.edu --glider osusim
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
sfmc-api --host sfmc-backup.example.com get-glider-details osusim
```

If the file has only one host, it is selected automatically.

See [docs/configuration.md](docs/configuration.md) for full details.

## Documentation

- [Getting Started](docs/getting_started.md) -- installation and configuration
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

## License

GPL-3.0-or-later
