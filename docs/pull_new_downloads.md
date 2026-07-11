# Pull New Downloads

`sfmc-pull-new-downloads` mirrors new files from a glider's
`from-glider` folder into a local directory, driven by SFMC's
real-time event streams.  It is designed to run unattended for the
length of a deployment and to place as little load on the SFMC server
as possible.

```bash
# Stream events and download new files as they arrive
sfmc-pull-new-downloads --host sfmc.example.com osu685 /data/osu685

# Single catch-up pass (for cron), no streaming
sfmc-pull-new-downloads --host sfmc.example.com --once osu685 /data/osu685
```

The first run records a **baseline** of the current folder contents
and downloads nothing.  From then on — whether streaming or via
repeated `--once` runs — only files that appeared after the baseline
are downloaded.

## How it works

1. **Wait on events, not polls.**  One STOMP connection carries
   subscriptions to connection events and Zmodem transfer events.
   While the glider is underwater the process makes no requests.
2. **Surfacing summary.**  When a connection closes (`active: false`
   event), one `get-zmodem-transfers` request logs what the surfacing
   transferred (downloads, uploads, byte counts).
3. **Settle window.**  SFMC receives files under their 8.3 DOS names
   (e.g. `48280001.sbd`) and renames most of them to full Dinkum
   names (`osu685-2026-191-0-1.sbd`) on a variable delay — observed
   from under a minute to ~25 minutes.  Both copies are downloaded
   when both get listed: compressed `*.?cd` files may never be
   renamed, and modem files (`*.mri`/`*.mrd`) never are.  Because a
   non-Dinkum-named file can be *partially transferred* while a
   connection is open, such entries are deferred until the glider
   disconnects; Dinkum-named entries only ever appear complete (the
   rename is atomic) and download immediately.  After each surfacing
   the listing is polled (default: every 60 s, backing off) until
   several consecutive polls find nothing new, or a hard timeout
   passes.  An idle reconcile (default: every 15 min) backstops
   missed events, and a catch-up pass runs at every startup.
4. **One zip per batch.**  New files are fetched with a single
   `download-glider-files` request using the `lastModifiedAfter`
   prefilter, extracted into `OUTPUT_DIR`, and recorded in a state
   file (`OUTPUT_DIR/.sfmc-pull-state.json`).

## Why the cutoff logic looks the way it does

- Renamed files carry `dateTimeModified` from the **glider's own
  clock** (via the Dinkum file header); un-renamed files carry
  dockserver-clock times.  The two can disagree — on a simulator by
  an hour or more.  All cutoff arithmetic therefore stays in the
  glider-clock domain: only Dinkum-named entries advance the
  high-water mark, and queries subtract a safety margin from it.
- The margin (`--margin-minutes`, default 2880 = 48 h) must cover how
  *old* a new file's glider-clock timestamp can be: a segment file
  from a long dive closes hours before it is transmitted, and backlog
  files can be several dives older still.  Size it comfortably above
  the longest expected dive.  Filename comparison against the state
  file removes the window overlap, so a wide margin costs only
  listing pages, not repeat downloads.
- Gliders re-transmit files that were already delivered on earlier
  surfacings.  SFMC keeps the original listing timestamp for these,
  so the high-water-mark filter excludes them automatically.
- `lastModifiedAfter` has minute resolution and includes files from
  the named minute onward, so flooring the high-water mark to the
  minute never skips a same-minute file.
- While a file is still arriving it can appear in the listing under
  its transfer-time name with a partial, growing size.  Deferring
  non-Dinkum names until the glider disconnects avoids downloading
  partial files without giving up the never-renamed file classes.

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `--credentials PATH` | `~/.config/sfmc/credentials.json` | Credentials file |
| `--host HOSTNAME` | sole host in credentials | SFMC server |
| `--once` | off | One catch-up pass, then exit |
| `--state-file PATH` | `OUTPUT_DIR/.sfmc-pull-state.json` | State location |
| `--margin-minutes N` | 2880 | Cutoff safety margin; must exceed the longest dive |
| `--settle-poll SECS` | 60 | Initial poll interval after a surfacing (backs off 1.5x, capped at 5 min) |
| `--settle-quiet N` | 3 | Quiet polls that end a settle window once it has downloaded something |
| `--settle-timeout SECS` | 1800 | Hard limit on a settle window (the only exit when a surfacing produced no new files) |
| `--reconcile-interval SECS` | 900 | Idle backstop check |
| `-v, --verbose` | off | Debug logging (includes per-request HTTP lines) |

## Operational notes

- Extracted files get their listing `dateTimeModified` as the local
  file mtime (interpreted as UTC).  Remember this is glider-clock
  time.
- The state file records every file name ever seen with its size and
  listing timestamp.  Deleting it causes the next run to re-baseline
  (nothing is re-downloaded, but files that arrive while the state is
  missing are skipped).
- If a file is re-listed with a **changed** `dateTimeModified`, it is
  treated as modified and downloaded again, overwriting the local
  copy.
- The STOMP connection reconnects automatically with backoff
  (15 s doubling to 5 min); a reconcile pass runs after each
  reconnect so nothing is lost across the gap.
- Stop with Ctrl-C or SIGTERM; state is saved after every batch, so
  restarting is always safe.
