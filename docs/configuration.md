# Configuration Data Flow

## Overview

`SFMCConfig` holds all settings needed to connect to an SFMC server.
It is an immutable (frozen) dataclass — once created, its values cannot
be changed.

## Resolution Order

When `SFMCClient` is constructed, configuration is resolved in this
priority order:

```
SFMCClient(config=..., config_path=...)

  1. config argument     ──► used directly if provided
  2. config_path argument ──► loaded via SFMCConfig.from_file(path)
  3. default path         ──► ~/.config/sfmc/credentials.json
```

```
┌──────────────────────────────────────────────────────┐
│                  SFMCClient.__init__                  │
│                                                      │
│   config provided? ──yes──► use it directly          │
│        │                                             │
│        no                                            │
│        │                                             │
│        ▼                                             │
│   config_path provided? ──yes──► from_file(path)     │
│        │                                             │
│        no                                            │
│        │                                             │
│        ▼                                             │
│   from_file(~/.config/sfmc/credentials.json)         │
└──────────────────────────────────────────────────────┘
```

## JSON File Schema

```json
{
    "host": "gliderfmc1.ceoas.oregonstate.edu",
    "apiCredentials": {
        "clientId": "your-client-id",
        "secret": "your-secret"
    },
    "tlsRejectUnauthorized": 0,
    "rootDownloadPath": "/tmp/sfmc-downloads",
    "stompDebug": false
}
```

### Field Reference

| JSON Key | Python Attribute | Required | Default | Description |
|----------|-----------------|----------|---------|-------------|
| `host` | `host` | yes | — | SFMC server hostname or IP (no `https://`) |
| `apiCredentials.clientId` | `client_id` | yes | — | API credential identifier |
| `apiCredentials.secret` | `secret` | yes | — | API credential secret |
| `tlsRejectUnauthorized` | `tls_verify` | no | `1` (verify) | `0` = skip TLS verification, `1` = verify. **Note:** the Python attribute uses the *inverted* convention — `tls_verify=False` when the JSON value is `0`. |
| `rootDownloadPath` | `root_download_path` | no | `None` | Local directory for file downloads. Converted to `pathlib.Path`. |
| `stompDebug` | `stomp_debug` | no | `false` | Enable verbose STOMP protocol logging. |

## Translation Logic

The JSON schema matches the Node.js reference implementation
(`local.json`).  Key translations:

```
JSON                         Python
────                         ──────
tlsRejectUnauthorized: 0  →  tls_verify = False
tlsRejectUnauthorized: 1  →  tls_verify = True   (default)
rootDownloadPath: "/tmp"  →  root_download_path = Path("/tmp")
rootDownloadPath: null    →  root_download_path = None
```

## Programmatic Construction

You can bypass the JSON file entirely:

```python
from sfmc_api import SFMCConfig, SFMCClient

config = SFMCConfig(
    host="sfmc.example.com",
    client_id="my-id",
    secret="my-secret",
    tls_verify=False,
)

with SFMCClient(config=config) as client:
    ...
```

Or build from a dictionary (same schema as the JSON file):

```python
config = SFMCConfig.from_dict({
    "host": "sfmc.example.com",
    "apiCredentials": {"clientId": "my-id", "secret": "s3cret"},
})
```

## Error Handling

| Problem | Exception |
|---------|-----------|
| File not found | `ConfigError("Config file not found: ...")` |
| File unreadable (permissions) | `ConfigError("Cannot read config file ...: ...")` |
| Invalid JSON | `ConfigError("Invalid JSON in ...: ...")` |
| Missing required key | `ConfigError("Missing required config key: ...")` |

## Derived Property

`SFMCConfig.base_url` assembles the full API root from the host:

```python
config.base_url  # → "https://gliderfmc1.ceoas.oregonstate.edu/sfmc/api"
```

This is used internally by `build_http_client()` to set the `httpx`
client's `base_url`.
