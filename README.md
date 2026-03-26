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
sfmc auth                                    # test credentials
sfmc get-glider-details osusim               # query a glider
sfmc get-waypoint-plan osusim                # get navigation plan
sfmc get-folder-file-listing osusim from-glider  # list files
sfmc subscribe-connection-events osusim      # stream events (Ctrl-C to stop)
sfmc --compact get-glider-details osusim     # single-line JSON
sfmc --help                                  # see all subcommands
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
        },
        "tlsRejectUnauthorized": 0
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
sfmc --host sfmc-backup.example.com get-glider-details osusim
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

## License

GPL-3.0-or-later
