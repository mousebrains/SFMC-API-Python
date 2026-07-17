# Running the stream commands as systemd services

`sfmc-monitor-glider-foo.service` is a tuned, hardened example unit for
a modern Debian/Ubuntu system (systemd ≥ 247; Debian 11+/Ubuntu 22.04+).
As written it monitors glider **foo** on **sfmc.alpha.com** and emails
**spam@spam.com** when the SFMC connection stays down — edit those
three things (plus paths) for your deployment.

## Install walkthrough

### 1. Service user

A dedicated unprivileged user with no shell and no home directory:

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin sfmc
```

### 2. Install the package

Into a dedicated virtualenv the service user can read but not write:

```bash
sudo python3 -m venv /opt/sfmc/venv
sudo /opt/sfmc/venv/bin/pip install "git+https://github.com/mousebrains/SFMC-API-Python"
```

(Any location works — update `ExecStart=` to match.)

### 3. Credentials

The unit's `ConfigurationDirectory=sfmc` provides `/etc/sfmc`.  Create
the credentials file there, readable by the service user only (see
`docs/configuration.md` for the multi-host format):

```bash
sudo install -m 0640 -o root -g sfmc /dev/null /etc/sfmc/credentials.json
sudo editor /etc/sfmc/credentials.json
```

```json
{
    "hosts": {
        "sfmc.alpha.com": {
            "clientId": "your-client-id",
            "secret": "your-secret"
        }
    }
}
```

### 4. Local mail relay

Alert email goes to `localhost:25` by default, so the host needs an MTA
that forwards to your mail server — on Debian/Ubuntu typically postfix
in "Satellite system" mode:

```bash
sudo apt install postfix   # choose "Satellite system", relay = campus mail host
echo test | mail -s "relay test" spam@spam.com   # verify before relying on it
```

### 5. Enable

```bash
sudo cp sfmc-monitor-glider-foo.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sfmc-monitor-glider-foo
systemctl status sfmc-monitor-glider-foo
journalctl -u sfmc-monitor-glider-foo -f
```

The dialog log (the primary data record) lands in
`/var/log/sfmc/foo.log`; the journal carries the same lines.

## Log rotation

The monitor uses a `WatchedFileHandler`, so plain logrotate works — it
reopens the file after rotation.  `/etc/logrotate.d/sfmc`:

```
/var/log/sfmc/*.log {
    weekly
    rotate 8
    compress
    delaycompress
    missingok
    notifempty
    create 0640 sfmc sfmc
}
```

## Notes on the unit's choices

- **Reconnect belongs to the program, not systemd.**  The monitor
  survives stream loss with capped, jittered backoff and emails after a
  sustained outage; `Restart=on-failure` is only the backstop for fatal
  errors.  Do **not** add `--no-reconnect` alongside `--notify-email` —
  the process would exit before the alert threshold could ever elapse
  (the program warns about this combination at startup).
- **`StartLimitIntervalSec`/`StartLimitBurst`** stop a permanently-bad
  config (wrong secret, misspelled glider name) from restart-flapping
  forever; `systemctl reset-failed sfmc-monitor-glider-foo` clears the
  latch after you fix it.
- **Sandboxing** follows `systemd-analyze security` guidance: read-only
  filesystem (`ProtectSystem=strict`) except `/var/log/sfmc`, no
  capabilities, no privilege escalation, private /tmp and devices,
  kernel interfaces masked, syscalls limited to `@system-service`, and
  address families limited to what a TLS/SMTP client needs.  If you
  later add a native extension that needs runtime code generation, drop
  `MemoryDenyWriteExecute=true`.
- **Memory/task caps** (`MemoryMax=512M`, `TasksMax=32`) are generous
  for this small process but protect the host from a leak.

## Adapting for other commands or a fleet

`sfmc-follow` and `sfmc-pull-new-downloads` run the same way — swap the
`ExecStart=` line (both take the same `--notify-*` options).  Two extra
notes for `sfmc-pull-new-downloads`: give it a writable download
directory (add e.g. `StateDirectory=sfmc` and download into
`/var/lib/sfmc/…`), and its state file lives in that directory too.

For several gliders, copy the unit per glider
(`sfmc-monitor-glider-bar.service`, …) — or convert it into a systemd
template unit (`sfmc-monitor-glider@.service` with `%i` as the glider
name and per-glider settings in `/etc/sfmc/%i.env` via
`EnvironmentFile=`), which is worth it once the fleet outgrows
copy-and-edit.
