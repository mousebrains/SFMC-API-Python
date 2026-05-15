# Getting Started

This guide walks a brand-new user from a clean machine to a working
SFMC connection.  If you already have Python and a virtual
environment set up, jump to [Configuration](#configuration).

## Prerequisites

- **Python 3.12 or newer.**  Check with `python3 --version`.  If you
  need to install or upgrade, use the official installer from
  [python.org](https://www.python.org/downloads/) or your system's
  package manager.
- **`pip`.**  Usually bundled with Python; if missing, run
  `python3 -m ensurepip`.
- **An SFMC server you have credentials for.**  Ask your glider pilot
  or system administrator for the hostname.  Credentials come from
  the SFMC web UI at `https://<your-host>/sfmc/api-access-pages/api-access`.

If any of this is unfamiliar, also read [glossary.md](glossary.md)
for a primer on the terms used here.

## 1. Create a virtual environment (recommended)

A virtual environment ("venv") is a self-contained Python
installation that keeps this project's dependencies separate from
everything else on your machine.  Without one you risk version
conflicts that are hard to debug.

```bash
cd /path/to/SFMC-API-Python
python3 -m venv venv
source venv/bin/activate    # on Linux/macOS
# venv\Scripts\activate     # on Windows PowerShell
```

You will know it worked when your shell prompt is prefixed with
`(venv)`.  **You need to re-run `source venv/bin/activate` every
time you open a new terminal.**

## 2. Install the library

```bash
pip install -e .
```

The `-e` flag installs in *editable* mode: pip points your
installation at the source files in this directory so any code change
takes effect immediately, without re-installing.

If you plan to develop a follower plugin, add the follower extras:

```bash
pip install -e '.[follow]'        # YAML support for follower configs
pip install -e '.[drifter]'       # adds netCDF4 + numpy for the drifter example
pip install -e '.[dev]'           # plus pytest, ruff, mypy for development
```

## 3. Configuration

Use the built-in `init` command to create your credentials file:

```bash
sfmc-api init
```

This will:

1. Prompt for your SFMC hostname, client ID, and secret.
2. Show you the SFMC URL where these credentials live.
3. Write `~/.config/sfmc/credentials.json` with secure permissions
   (`0600`, readable only by you).

The resulting file looks like:

```json
{
    "gliderfmc1.ceoas.oregonstate.edu": {
        "apiCredentials": {
            "clientId": "YOUR_CLIENT_ID",
            "secret": "YOUR_SECRET"
        }
    }
}
```

Top-level keys are hostnames, so you can have several SFMC servers in
one file.  Use `sfmc-api add-host` to add another.  See
[configuration.md](configuration.md) for the full field reference and
[troubleshooting.md](troubleshooting.md) if `init` fails.

## 4. Verify the connection

```bash
sfmc-api auth
```

Expected output:

```json
{
  "status": "ok",
  "host": "gliderfmc1.ceoas.oregonstate.edu"
}
```

If you see an error here, fix it before going further.  See the
[Authentication failures](troubleshooting.md#authentication-failures)
section of the troubleshooting guide.

## 5. First real API call

From the CLI:

```bash
sfmc-api get-glider-details osu685
```

From Python:

```python
from sfmc_api import SFMCClient

with SFMCClient() as client:
    details = client.get_glider_details("osu685")
    print(details)
```

Replace `osu685` with one of *your* glider names.  See
[cli.md](cli.md) for the full list of CLI commands, or
[glider_management.md](glider_management.md) for the Python API.

## What happens under the hood

1. `SFMCClient()` reads `~/.config/sfmc/credentials.json` and builds
   an HTTP connection pool — **no network call yet**.
2. The first method call triggers lazy authentication: the client
   signs in via `POST /sfmc/api/signin` and caches the bearer token.
3. The actual API call goes out with that token in the
   `Authorization` header.
4. The JSON response comes back as a Python dictionary.

See [authentication.md](authentication.md) for the detailed auth
flow.

## Where to go next

- **List available commands:** [cli.md](cli.md)
- **Real-time monitoring:** [monitor_glider.md](monitor_glider.md)
- **Autonomous following:** [follow_glider.md](follow_glider.md)
- **When things break:** [troubleshooting.md](troubleshooting.md)
- **Term definitions:** [glossary.md](glossary.md)
