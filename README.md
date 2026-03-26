# sfmc-api

Python client for the Slocum Fleet Management Center (SFMC) REST API.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from sfmc_api import SFMCClient

with SFMCClient() as client:
    details = client.get_glider_details("osu680")
    print(details)
```

Credentials are loaded from `~/.config/sfmc/credentials.json` by default.
See [docs/getting_started.md](docs/getting_started.md) for setup instructions.

## License

GPL-3.0-or-later
