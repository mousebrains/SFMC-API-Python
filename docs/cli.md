# CLI Reference

The `sfmc` command provides access to all SFMC REST API operations
from the terminal.

## Installation

The CLI is installed automatically with the package:

```bash
pip install -e .
sfmc --help
```

Or run via Python module:

```bash
python -m sfmc_api --help
```

## Global Options

| Option | Description |
|--------|-------------|
| `--credentials PATH` | Path to credentials JSON file (default: `~/.config/sfmc/credentials.json`) |
| `--host HOSTNAME` | Select host from a multi-host credentials file |
| `--download-path DIR` | Default directory for downloads (overrides config `rootDownloadPath`) |
| `--compact` | Single-line JSON output (default: pretty-printed) |
| `--version` | Show version and exit |

## Setup

```bash
# Create a new credentials file (interactive prompts)
sfmc init

# Add another SFMC server to an existing credentials file
sfmc add-host

# Use a custom credentials path
sfmc --credentials /path/to/creds.json init
```

The `init` command prompts for hostname, client ID, secret, TLS
verification, and an optional download directory.  It provides the
URL to the API credentials page for each server:

    https://<hostname>/sfmc/api-access-pages/api-access

## Examples

```bash
# Test authentication
sfmc auth

# Query a glider
sfmc get-glider-details osusim

# Select a specific host
sfmc --host gliderfmc1.ceoas.oregonstate.edu get-glider-details osusim

# Use a different credentials file
sfmc --credentials /path/to/creds.json auth

# List files with filtering
sfmc get-folder-file-listing osusim from-glider --filter "*.sbd" --page 0

# Stream events (Ctrl-C to stop)
sfmc subscribe-connection-events osusim

# Compact output for piping
sfmc --compact get-glider-details osusim | jq .data.state
```

## Commands

### Setup & Authentication

| Command | Description |
|---------|-------------|
| `init` | Create a new credentials file (interactive prompts) |
| `add-host` | Add another host to an existing credentials file |
| `auth` | Test credentials, prints `{"status": "ok", "host": "..."}` |

### Glider Queries

| Command | Args | Description |
|---------|------|-------------|
| `get-glider-details` | `GLIDER` | Glider details (id, name, state) |
| `get-active-deployment-details` | `GLIDER` | Active deployment info |
| `get-newest-mission-status` | `GLIDER` | Current mission status |
| `get-available-scripts` | `GLIDER` | List available scripts |
| `get-surface-sensor-samples` | `GLIDER SENSOR --start DT --end DT` | Sensor data in time range |
| `get-folder-file-listing` | `GLIDER FOLDER [--page N] [--filter PAT]` | List files in folder |
| `get-zmodem-transfers` | `CONNECTION_ID` | Zmodem transfer details |

### Plan Queries

| Command | Args | Description |
|---------|------|-------------|
| `get-mission-plan` | `GLIDER` | Assigned mission plan |
| `get-waypoint-plan` | `GLIDER` | Assigned waypoint plan |
| `get-yo-plan` | `GLIDER` | Assigned yo (dive/climb) plan |
| `get-surface-plan` | `GLIDER` | Assigned surface plan |
| `get-sampling-plan` | `GLIDER` | Assigned sampling plan |
| `get-data-transmission-plan` | `GLIDER` | Data transmission plan |
| `get-mission-sensor-plan` | `GLIDER` | Mission sensor plan |
| `get-abort-plan` | `GLIDER` | Abort plan |

### Plan Updates

| Command | Args | Description |
|---------|------|-------------|
| `update-waypoint-plan` | `GLIDER FILE` | Upload waypoint plan file |
| `update-yo-plan` | `GLIDER FILE` | Upload yo plan file |
| `update-surface-plan` | `GLIDER FILE` | Upload surface plan file |
| `update-sampling-plan` | `GLIDER FILE` | Upload sampling plan file |
| `update-flight-data-transmission-plan` | `GLIDER FILE` | Upload flight DT plan |
| `update-science-data-transmission-plan` | `GLIDER FILE` | Upload science DT plan |

### Plan Rule Deletions

| Command | Args | Description |
|---------|------|-------------|
| `delete-hit-waypoint-surface-plan-rule` | `GLIDER` | Delete hit-waypoint rule |
| `delete-every-secs-surface-plan-rules` | `GLIDER` | Delete every-N-secs rules |
| `delete-at-utc-time-surface-plan-rules` | `GLIDER` | Delete at-UTC-time rules |
| `delete-sampling-plan-rules` | `GLIDER` | Delete sampling rules |

### Registration & Deployment

| Command | Args | Description |
|---------|------|-------------|
| `register-glider` | `GLIDER [--group GRP]` | Register a glider |
| `obtain-or-create-active-deployment` | `GLIDER` | Get/create deployment |
| `update-active-deployment-start` | `GLIDER DATETIME` | Update start time |

### Script Control

| Command | Args | Description |
|---------|------|-------------|
| `set-assigned-script` | `GLIDER TYPE NAME` | Assign a script |
| `clear-assigned-script` | `GLIDER` | Clear assigned script |
| `pause-assigned-script` | `GLIDER` | Pause assigned script |
| `resume-assigned-script` | `GLIDER` | Resume assigned script |
| `rewind-assigned-script` | `GLIDER` | Rewind assigned script |
| `send-command` | `GLIDER COMMAND` | Send command to glider |

### Deploy Files

| Command | Args | Description |
|---------|------|-------------|
| `deploy-goto-file` | `GLIDER` | Generate & deploy goto file |
| `deploy-yo-file` | `GLIDER` | Generate & deploy yo file |
| `deploy-surface-files` | `GLIDER` | Generate & deploy surface files |
| `deploy-sample-files` | `GLIDER` | Generate & deploy sample files |
| `deploy-sbd-list-file` | `GLIDER` | Generate & deploy SBD list |
| `deploy-tbd-list-file` | `GLIDER` | Generate & deploy TBD list |

### File Operations

| Command | Args | Description |
|---------|------|-------------|
| `upload-glider-files` | `GLIDER FOLDER FILE...` | Upload files to folder |
| `upload-cache-files` | `GROUP FILE...` | Upload cache files |
| `download-glider-file` | `GLIDER FOLDER FILE [-o PATH]` | Download single file |
| `download-glider-files` | `GLIDER FOLDER [-o PATH] [--filter]` | Download as zip |
| `delete-glider-file` | `GLIDER FOLDER FILE` | Delete a file |

### Real-Time Streaming

These commands run until Ctrl-C, printing each event as JSON:

| Command | Args | Description |
|---------|------|-------------|
| `subscribe-connection-events` | `GLIDER` | Connection events |
| `subscribe-glider-output` | `GLIDER` | Dialog output |
| `subscribe-script-events` | `GLIDER` | Script state changes |
| `subscribe-zmodem-transfer-events` | `GLIDER` | Zmodem transfers |
| `subscribe-deployment-events` | `GLIDER` | Deployment updates |
