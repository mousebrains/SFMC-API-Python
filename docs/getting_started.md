# Getting Started

## Installation

From the repository root, install in editable (development) mode:

```bash
pip install -e .
```

Or with development dependencies (pytest, ruff):

```bash
pip install -e ".[dev]"
```

## Configuration

Create a credentials file at `~/.config/sfmc/credentials.json`:

```json
{
    "host": "gliderfmc1.ceoas.oregonstate.edu",
    "apiCredentials": {
        "clientId": "YOUR_CLIENT_ID",
        "secret": "YOUR_SECRET"
    },
    "tlsRejectUnauthorized": 0,
    "rootDownloadPath": "/tmp/sfmc-downloads",
    "stompDebug": false
}
```

See [configuration.md](configuration.md) for details on each field and
alternative ways to provide credentials.

## First API Call

```python
from sfmc_api import SFMCClient

with SFMCClient() as client:
    details = client.get_glider_details("osu680")
    print(details)
```

Or run the included example script:

```bash
python examples/get_glider_details.py osu680
```

## What Happens Under the Hood

1. `SFMCClient()` loads `~/.config/sfmc/credentials.json` and creates an
   HTTP connection pool — **no network calls yet**.
2. `get_glider_details("osu680")` triggers lazy authentication:
   the client signs in via `POST /sfmc/api/signin` and caches the bearer
   token.
3. The actual API call `GET /sfmc/api/v1/gliders/osu680` is made with
   the cached token in the `Authorization` header.
4. The JSON response is returned as a Python dictionary.

See [authentication.md](authentication.md) for the detailed auth flow.
