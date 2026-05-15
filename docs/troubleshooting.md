# Troubleshooting

If a term in an error message is unfamiliar, the
[glossary](glossary.md) is the right place to look first.

When something goes wrong, the first three things to check are:

1. **Is the credentials file in the right place?**
   `ls ~/.config/sfmc/credentials.json` — if missing, run `sfmc-api init`.
2. **Does authentication work?**
   `sfmc-api auth` — if this fails, every other command will fail too.
3. **Is the glider name spelled correctly?**
   Glider names are case-sensitive. Check the SFMC web UI for the exact name.

If those are fine, look up your error message below.

## Installation problems

### `command not found: sfmc-api`

The console script is installed into your Python environment, but the shell
cannot find it.  Three usual causes:

- **You forgot to activate your virtual environment.**
  Run `source venv/bin/activate` (or whatever path you used) and try again.
- **You ran `pip install` for a different Python than the one on your `PATH`.**
  Check with `which python3` and `which pip3`.  If they point to different
  installations, use `python3 -m pip install -e .` instead of bare `pip`.
- **The install ran but the scripts directory is not on `PATH`.**
  After `pip install`, pip usually prints `Installing collected packages …`
  followed by a warning if the scripts location is outside `PATH`.  Add
  the printed directory to your `PATH` in `~/.bashrc` or `~/.zshrc`.

### `pip install -e .` fails with a build error

Try `python3 -m pip install -e .` to make sure you are using the same
Python that has `pip` configured.  On macOS, ensure Xcode Command Line
Tools are installed (`xcode-select --install`).  On Linux, ensure
`python3-dev` (or `python3-devel`) is installed.

### `ModuleNotFoundError: No module named 'netCDF4'`

You installed the base package but the drifter follower needs extra
dependencies.  Run:

```bash
pip install -e '.[drifter]'
```

## Configuration / credentials

### `ConfigError: Config file not found: /Users/.../credentials.json`

You have not created a credentials file yet.  Run:

```bash
sfmc-api init
```

This will walk you through the credential entry and create
`~/.config/sfmc/credentials.json` with secure file permissions (0600).

### `ConfigError: Multiple hosts in … — specify one with --host`

Your credentials file contains entries for more than one SFMC server, so
the client cannot guess which one to use.  Pick one explicitly:

```bash
sfmc-api --host gliderfmc1.ceoas.oregonstate.edu get-glider-details osu685
```

In Python:

```python
SFMCClient(host="gliderfmc1.ceoas.oregonstate.edu")
```

### `ConfigError: Host 'foo' not found in …`

The hostname you passed to `--host` does not appear in the credentials
file.  The error message lists the available hosts; copy one of those
exactly.

### `ConfigError: Missing required config key: 'apiCredentials.clientId' …`

The credentials file is malformed.  See
[configuration.md](configuration.md) for the expected shape, or just
delete the file and re-run `sfmc-api init`.

## Authentication failures

### `AuthenticationError: HTTP 401`

The server rejected your credentials.

- Double-check `clientId` and `secret` in your credentials file.  Copy
  them again from `https://<host>/sfmc/api-access-pages/api-access` —
  it is easy to lose a character to clipboard issues.
- Confirm you are pointing at the right host.  Different SFMC servers
  have different credential pairs.

### `AuthenticationError: ... CERTIFICATE_VERIFY_FAILED`

The SFMC server is presenting a TLS certificate your machine does not
trust.  This is common on test servers with self-signed certificates.

If you trust the server (it is yours, or you are on a private network):

```json
{
    "yourhost.example.com": {
        "apiCredentials": { "...": "..." },
        "tlsRejectUnauthorized": 0
    }
}
```

The value `0` disables TLS verification.  **Do not use this on a
production server you do not control.**

### `AuthenticationError: Connection refused / timed out`

Network issue.  Try in a browser: `https://<hostname>/sfmc/`.  If that
fails, the server is down or unreachable from your machine (firewall,
VPN, DNS).  If it works in the browser, double-check the hostname in
your credentials file — no `https://` prefix.

## API errors

### `APIError: HTTP 404`

The resource does not exist.  For glider operations, double-check the
glider name spelling.  For files, run
`sfmc-api get-folder-file-listing <glider> from-glider` to see what is
actually there.

### `APIError: HTTP 429` (rate limited)

The client retries rate-limit responses automatically using the delay
the server suggests.  If you still see this in your output, it means
three retries were not enough.  Wait a minute and try again, or batch
fewer operations.

### `APIError: HTTP 500` or other server error

Something went wrong on the server side.  The response body, included
in the error message, sometimes has details.  If the same operation
fails repeatedly, contact your SFMC server administrator.

## Follower (`sfmc-follow`) problems

### `Failed to import follower module …`

Python could not import your follower file.  Common causes:

- Syntax error in the follower file.  Run
  `python3 -c "import my_follower"` (after `cd` into the file's
  directory) to see the real error.
- Missing dependency.  If the follower imports `netCDF4` or `numpy`,
  install with `pip install -e '.[drifter]'`.

### `No BaseFollower subclass found in …`

Your follower file does not define a class that extends
`sfmc_api.follower.BaseFollower`.  The minimal shape is:

```python
from sfmc_api.follower import BaseFollower

class MyFollower(BaseFollower):
    def on_surfacing(self, event):
        ...
```

If you have more than one `BaseFollower` subclass in the file, pass
`--class MyFollower` on the command line.

### Waypoint validation: `ValueError: Waypoint 0 latitude … is outside [-90, 90]`

Your follower generated a waypoint outside the valid range.  Most often
the cause is **swapped latitude and longitude**.  `generate_goto_ma`
expects `(longitude, latitude)` tuples — longitude first.

If the values look right but still out of range, check any
`km_to_degrees` math: a divide-by-zero or off-by-1000 unit error can
produce huge numbers.

### Drifter follower: `OSError` or `RuntimeError: NetCDF: HDF error`

The drifter NetCDF file is missing, corrupt, or being written by
another process at the same time the follower is trying to read it.
Make sure the shore-side updater closes the file before the follower
re-reads it on each surfacing.

### Follower runs but no files are being uploaded

Are you running with `--dry-run`?  Dry-run prints files but never
uploads.  Remove `--dry-run` to go live.

Also check that your `on_surfacing()` actually calls
`self.send_files(to_glider={...})`.  If it does not, the framework has
nothing to upload.

## Streaming / monitor

### `sfmc-monitor-glider` exits immediately

The most likely cause is a network or authentication problem when
opening the STOMP connection.  Re-run with the `--log-level DEBUG`
flag (if available) or run `sfmc-api auth` first to verify
credentials.

### Streaming hangs with no output

Some gliders only emit dialog output while they are actively
communicating with SFMC.  If the glider is on the ocean floor or in
between Iridium sessions, there is nothing to stream.  Watch
connection events instead:

```bash
sfmc-api subscribe-connection-events <glider>
```

## Running from a service / cron / unattended

Destructive commands (`delete-glider-file`, `clear-assigned-script`,
`delete-*-rules`) prompt for confirmation when run interactively, and
refuse to run when stdin is not a terminal (so a runaway script
cannot delete things by accident).

To allow destructive commands from a service, cron job, or pipeline,
either:

- Pass `--yes` on each invocation:

  ```bash
  sfmc-api --yes delete-glider-file g1 to-glider old.mi
  ```

- Or set the env var once in your service environment:

  ```bash
  export SFMC_ASSUME_YES=1
  ```

  In a systemd unit:

  ```ini
  [Service]
  Environment=SFMC_ASSUME_YES=1
  ```

Either path opts you into unconfirmed destructive operations.  Use
deliberately; this is a foot-gun by design.

## When to ask for help

If your problem is not listed here:

1. Re-run the failing command with `--log-level DEBUG` if it is
   `sfmc-follow` or `sfmc-monitor-glider`, or wrap the failing Python
   in `logging.basicConfig(level=logging.DEBUG)`.
2. Open an issue at <https://github.com/mousebrains/SFMC-API-Python/issues>
   with: the exact command you ran, the full error message
   (including the traceback), and your Python and SFMC versions.
