# Configuration Data Flow

## Overview

`SFMCConfig` holds all settings needed to connect to an SFMC server.
It is an immutable (frozen) dataclass — once created, its values cannot
be changed.

## Resolution Order

When `SFMCClient` is constructed, configuration is resolved in this
priority order:

```
SFMCClient(config=..., config_path=..., host=...)

  1. config argument     ──► used directly if provided
  2. config_path + host  ──► loaded via SFMCConfig.from_file(path, host)
  3. default path + host ──► ~/.config/sfmc/credentials.json
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
│   config_path provided? ──yes──► from_file(path,host)│
│        │                                             │
│        no                                            │
│        │                                             │
│        ▼                                             │
│   from_file(~/.config/sfmc/credentials.json, host)   │
└──────────────────────────────────────────────────────┘
```

## Multi-Host Format (Recommended)

The credentials file supports multiple SFMC servers, keyed by hostname:

```json
{
    "gliderfmc1.ceoas.oregonstate.edu": {
        "apiCredentials": {
            "clientId": "your-client-id",
            "secret": "your-secret"
        }
    },
    "sfmc-backup.example.com": {
        "apiCredentials": {
            "clientId": "other-id",
            "secret": "other-secret"
        }
    }
}
```

**Host selection rules:**

- **One host in file** -- auto-selected, no `--host` needed.
- **Multiple hosts** -- specify with `--host`:
  - CLI: `sfmc-api --host sfmc-backup.example.com ...`
  - Python: `SFMCClient(host="sfmc-backup.example.com")`
- **Unknown host** -- error with list of available hosts.

## Per-Host Entry Fields

### Field Reference

| JSON Key | Python Attribute | Required | Default | Description |
|----------|-----------------|----------|---------|-------------|
| `host` | `host` | yes | — | SFMC server hostname or IP (no `https://`) |
| `apiCredentials.clientId` | `client_id` | yes | — | API credential identifier |
| `apiCredentials.secret` | `secret` | yes | — | API credential secret |
| `tlsRejectUnauthorized` | `tls_verify` | no | `1` (verify) | Only `0` disables TLS verification (`tls_verify=False`). All other values — including `false`, `true`, `1` — keep verification on. Follows the Node.js `NODE_TLS_REJECT_UNAUTHORIZED` convention. |
| `rootDownloadPath` | `root_download_path` | no | `None` | Local directory for file downloads. Converted to `pathlib.Path`. |
| `stompDebug` | `stomp_debug` | no | `false` | Enable verbose STOMP protocol logging. |

## Translation Logic

The JSON schema matches the Node.js reference implementation
(`local.json`).  Key translations:

```
JSON                         Python
────                         ──────
tlsRejectUnauthorized: 0      →  tls_verify = False  (only 0 disables)
tlsRejectUnauthorized: 1      →  tls_verify = True
tlsRejectUnauthorized: false  →  tls_verify = True   (not 0, so verify)
tlsRejectUnauthorized: true   →  tls_verify = True
(absent)                      →  tls_verify = True   (default)
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

## Download Directory

File downloads use a default directory resolved in this order:

1. `download_path=` passed to ``SFMCClient()``
2. ``rootDownloadPath`` from the credentials file
3. The current working directory

```python
# Override at the client level
client = SFMCClient(download_path="/data/gliders")

# Downloads default to /data/gliders/<filename>
client.download_glider_file("osusim", "from-glider", "data.sbd")

# CLI equivalent
# sfmc-api --download-path /data/gliders download-glider-file osusim from-glider data.sbd
```

The directory is created automatically if it does not exist.

## Derived Property

`SFMCConfig.base_url` assembles the full API root from the host:

```python
config.base_url  # → "https://gliderfmc1.ceoas.oregonstate.edu/sfmc/api"
```

This is used internally by `build_http_client()` to set the `httpx`
client's `base_url`.
