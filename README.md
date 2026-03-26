# sfmc-api

[![CI](https://github.com/mousebrains/SFMC-API-Python/actions/workflows/ci.yml/badge.svg)](https://github.com/mousebrains/SFMC-API-Python/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mousebrains/SFMC-API-Python/graph/badge.svg)](https://codecov.io/gh/mousebrains/SFMC-API-Python)
[![PyPI](https://img.shields.io/pypi/v/sfmc-api)](https://pypi.org/project/sfmc-api/)
[![Python](https://img.shields.io/pypi/pyversions/sfmc-api)](https://pypi.org/project/sfmc-api/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

Python client for the Slocum Fleet Management Center (SFMC) REST API.

## Installation

```bash
pip install sfmc-api
```

Or for development:

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from sfmc_api import SFMCClient

with SFMCClient() as client:
    details = client.get_glider_details("osu684")
    print(details)
```

Credentials are loaded from `~/.config/sfmc/credentials.json` by default.
See [docs/getting_started.md](docs/getting_started.md) for setup instructions.

## Documentation

- [Getting Started](docs/getting_started.md) — installation and configuration
- [Authentication](docs/authentication.md) — auth data flow
- [Configuration](docs/configuration.md) — config resolution and JSON schema
- [Glider Management](docs/glider_management.md) — queries, registration, deployment
- [Plans](docs/plans.md) — mission plan queries, updates, and deployments
- [Script Control](docs/script_control.md) — script assignment and commands
- [File Operations](docs/file_operations.md) — upload, download, and delete
- [Real-Time Streaming](docs/streaming.md) — STOMP over SockJS event streaming

## License

GPL-3.0-or-later
