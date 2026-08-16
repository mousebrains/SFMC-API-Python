# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- Service robustness hardening across all three long-running commands,
  addressing the 29 findings of the adversarial review
  ([#8](https://github.com/mousebrains/SFMC-API-Python/issues/8)).
  Highlights: corrupt Iridium dialog lines and physically impossible
  GPS fixes are rejected instead of killing `sfmc-follow` or silently
  steering it; a liveness watchdog detects half-open TCP connections
  that previously hung `sfmc-monitor-glider` and `sfmc-follow` forever;
  ill-typed STOMP message bodies cost one skipped message instead of
  the whole service; follower file uploads retry with backoff instead
  of discarding steering files; `sfmc-pull-new-downloads` quarantines
  malformed listing entries, bounds high-water-mark advancement against
  corrupt glider clocks, and fsyncs its state file; 429/401/non-JSON
  HTTP responses are handled on every path; startup checks retry like
  steady-state failures; the monitor's dialog log survives logrotate
  and uses UTC timestamps; and `.ma` waypoint formatting can no longer
  emit an invalid 60-minutes DDMM value.

### Added

- **XML script engine.**  `sfmc-xml-engine` parses and executes the
  XML state machine SFMC runs beside the dockserver, so the same
  behaviour can run from Python — to understand a script
  (`--describe`), to replay one offline against a recorded dialog log
  (`--replay`), or to drive a glider (`--glider`).  Split in two on
  purpose: `XmlStateMachine` is pure — no I/O, no clock of its own,
  driven by `feed()` and `check_timeout()`, returning the actions a
  caller may then perform — and `run_live()` wires it to a glider and
  is the only part that can transmit.  Validated against the real
  corpus rather than invented examples: all 20 reference scripts parse,
  and replaying `riot.xml` against dialog captured from osusim
  reproduces the seven-action sequence documented in that script's own
  header, matching what the live SFMC engine did.  **Nothing is sent
  without `--send`** — the default reports what it would do, because
  the other default is a program that steers a glider the first time
  somebody runs it to see what it does.  Unknown action types, dangling
  `toState`, invalid regexes, malformed XML, and immediate-transition
  cycles are all refused rather than assumed benign.  See
  [docs/xml_engine.md](docs/xml_engine.md).
  - The XML `timeout` attribute is **minutes**, not seconds.  Every
    author in the corpus documents it that way — all 22 of `riot.xml`'s
    timers carry "If nothing within 10 minutes", and
    `vacuum_test_send_data_2hrs.xml` pairs `timeout="120"` with "a 120
    minute (2 hours) timeout" — and the SFMC operator confirmed it.
    Reading it as seconds is a 60x error in the dangerous direction: a
    script meant to wait ten minutes for a glider to answer would give
    up after ten seconds and act on the silence.  `Transition` names
    the unit (`timeout_seconds`, `timeout_minutes`) so the file's
    number cannot be mistaken for the running one, and a test pins it.
  - Control characters are sent as literal text (`Ctrl-C`, `Ctrl-R`,
    `Ctrl-W`), confirmed live against osusim: the dockserver echoes
    `^C` inline at the head of a glider output line, which also means a
    control character's reply is not correlated by echo anchoring.
  - `run_live()` sends a bare return after 60 seconds of dialog silence
    (`--keepalive`, `0` disables), because SFMC drops the connection
    after roughly 90 seconds of inactivity.  A mission in progress
    produces dialog at least every minute so this never fires; a glider
    sitting at a GliderDos prompt says nothing at all, and that is
    where the drop happens.  It only ever sends with `--send`, so a dry
    run waiting at a quiet prompt will still be dropped.
  - `--replay` now strips `sfmc-monitor-glider`'s log prefix.  Its
    format is `%(asctime)s %(name)s  %(message)s` with a dotted name
    (`sfmc.osusim.DIALOG`), which the original stripper — written for a
    bare-uppercase capture format — did not match.  A stripper that
    fails to match does not skip the line: it falls through to the
    raw-capture path, feeding the timestamp, the logger name, and the
    tool's own `INFO` bookkeeping straight to the matcher.
  - Known limitation: `run_live()` matches line-by-line whatever
    `--match-mode` says, because the dialog listener publishes only
    newline-terminated lines and drops the unterminated tail at a
    session boundary.  A GliderDos prompt followed by more output still
    matches; a trailing idle prompt can be dropped before it arrives.
    Documented rather than papered over.
- **Command replies.** `client.command_channel(glider)` submits a
  command and captures what the glider says back, returning a
  `CommandReply` instead of only SFMC's acceptance. The reply is
  explicit about what it can and cannot promise: `complete` and
  `reason` say whether capture reached a defined stop (terminator,
  quiet window, line cap) or ran out (timeout, disconnect, missing
  echo); `correlated` says whether the lines were anchored to an echo
  of *this* command or are merely what appeared on a shared terminal
  during the window; `dropped_lines` exposes a lagging capture instead
  of silently truncating. A missing reply is **not** an exception —
  silence is normal for a submerged glider — so exceptions are
  reserved for failures to submit. The dialog listener is attached
  before the command is submitted (a fast reply cannot be missed),
  reply-capturing sends are serialized per glider (no interleaved
  attribution), and a stream drop mid-capture never resubmits the
  command. Available from the CLI as
  `sfmc-api send-command GLIDER CMD --wait` (exit `2` when no complete
  reply arrived), with `--timeout`, `--quiet-for`, `--until`, and
  `--echo-anchor`. See [docs/script_control.md](docs/script_control.md).
  A capture that hears nothing at all reports `reason="silent"` and
  `complete=False`.  Live testing against a glider that was busy
  transmitting produced exactly this case, and an earlier revision
  reported it as `complete=True` with an empty `lines` — the false
  reassurance the type exists to prevent.
- **`sfmc-api probe-command GLIDER CMD`** — a diagnostic that dumps
  raw dialog frames with arrival offsets and sequence numbers around a
  submitted command, uninterpreted. It answers the one question this
  library cannot answer on its own: whether your dockserver echoes
  submitted commands, and therefore whether `--echo-anchor` is
  trustworthy on your server.
- **Asynchronous execution for every operation.**
  `client.operations()` returns an `OperationExecutor` that runs any
  bound client method on a worker thread and hands back a
  `concurrent.futures.Future` — the same type
  `CommandChannel.send_async()` returns, and directly awaitable via
  `asyncio.wrap_future()`. It wraps existing methods generically
  rather than describing the API a second time, so endpoints added
  later are asynchronously callable with no new wrapper code.
  `serialized(key, ...)` and `sequence(key, ...)` hold a per-glider
  lock for operations that must not interleave (two plan updates, an
  upload and the deploy that consumes it). See
  [docs/async_operations.md](docs/async_operations.md).
- **`client.session(glider, topics=[...])`** — a supervised event
  session that subscribes once per topic, runs sequence ordering and
  line reassembly once, and fans the result out to any number of
  listeners and callbacks, reconnecting on its own. Previously a
  subscription fed exactly one consumer and died with its connection,
  so watching a glider's dialog from two places meant two
  subscriptions and two reordering buffers. Listener queues are
  bounded and drop oldest-first, counting the loss so a gap is
  detectable.
- `sfmc_api.dialog_stream` — the dialog pipeline (`ordered_dialog`,
  `LineAssembler`, `dialog_lines`) as a first-class module. It was
  previously split between `sfmc_api.monitor_glider` and a private
  copy of the line-reassembly logic in `sfmc_api.follow_glider`, with
  the client's own docstrings pointing library users at an application
  script. `ordered_dialog` remains importable from `monitor_glider`.
- Glider IDs are cached per client, so a session that reconnects
  hourly no longer spends an HTTP round trip per topic per reconnect.
  `client.clear_glider_id_cache()` forces a fresh lookup.
- Email alerts on a sustained loss of the SFMC connection for all three
  long-running commands (`sfmc-follow`, `sfmc-monitor-glider`,
  `sfmc-pull-new-downloads`). When the STOMP stream stays down past
  `--notify-after` seconds (default 300), an alert is emailed to each
  `--notify-email` recipient; reminders repeat every `--notify-repeat`
  seconds while still down (default 3600, `0` for a single alert,
  minimum 60 as a storm floor), and a single all-clear follows recovery.
  Drops that recover before the threshold (flaps) stay silent, and a
  reconnect only ends an outage after the new session survives 60
  seconds — a stream that subscribes and dies over and over counts as
  one continuous outage instead of resetting the clock. If the process
  exits while an alerted outage is still open, a final "exiting, no
  all-clear will follow" notice is sent. Delivery is via a local SMTP
  relay by default (`--smtp-host`/`--smtp-port`, default `localhost:25`,
  no auth/TLS) on a background thread with per-message retries, so a
  slow or briefly-restarting mail server neither stalls a reconnect
  loop nor eats the alert. Email alerting is off unless at least one
  `--notify-email` is given; combining it with `--no-reconnect` logs a
  warning, since exiting on the first stream loss means the threshold
  can never elapse.
- Follower plugins can email the operator at their own discretion via
  `self.notify(key, summary, detail)` — for conditions only the
  follower's logic can see (an external float-position feed gone quiet,
  an `.ma` file that cannot be generated). Notifications are
  rate-limited per condition key (default 15 min) and share the
  disconnect-alert delivery machinery; without `--notify-email` the
  call is a silent no-op.
- `sfmc-pull-new-downloads` service mode now retries a transient
  first-run baseline failure with backoff instead of exiting, matching
  the boot-time policy of the other startup paths (`--once` still fails
  loudly for cron).
- A tuned, hardened example systemd unit for running
  `sfmc-monitor-glider` as a Debian/Ubuntu service (dedicated user,
  sandboxing, restart policy, email alerting), with an install
  walkthrough covering credentials, the local mail relay, and logrotate
  (`examples/systemd/`).
- `sfmc-monitor-glider` and live `sfmc-follow` now reconnect expected
  WebSocket/STOMP failures with capped, jittered exponential backoff and
  synchronized authentication refresh. Both support `--no-reconnect` for
  supervisors using `Restart=on-failure`, interrupt retry waits on SIGTERM,
  and record replay-safe `STREAM_BOUNDARY` markers. `sfmc-follow` preserves
  one follower/output pipeline across sessions, resets partial parser state at
  gaps, suppresses strong-identity overlap duplicates, and reports successful
  reconnects in `RunStats`. Reconnection restores future delivery only; SFMC
  provides no stream-history catch-up for the offline interval.

- `sfmc-pull-new-downloads` — event-driven mirroring of new
  `from-glider` files into a local directory.  Subscribes to
  connection and Zmodem transfer events, waits out SFMC's variable
  rename delay after each surfacing, then fetches all new files in a
  single filtered zip request.  Downloads both the 8.3-named and the
  renamed copies (compressed `*.?cd` may never be renamed;
  `*.mri`/`*.mrd` never are), deferring non-Dinkum names while the
  glider is connected so partially transferred files are never
  fetched.  Keeps a state file for safe restarts and offers a
  `--once` mode for cron.  Timestamp cutoffs stay in the glider-clock
  domain with a dive-scale safety margin (default 48 h) and local
  filename dedup.  (`docs/pull_new_downloads.md`)

### Changed

- All three long-running commands now share one reconnect supervisor
  (`sfmc_api.stream_reconnect.StreamSupervisor`) instead of carrying
  three near-identical copies of the session lifecycle. Behaviour is
  unchanged for `sfmc-monitor-glider` and `sfmc-follow`.
- **`sfmc-pull-new-downloads` now fails fast on permanent errors.**
  It previously caught every exception and retried forever, so a
  misspelled glider name or bad credentials produced an endless
  reconnect loop behind a service that looked healthy. It now matches
  `sfmc-monitor-glider` and `sfmc-follow`: transient failures (5xx,
  rate limits, transport errors) still retry with backoff; permanent
  client errors exit, making the misconfiguration visible to the
  operator and to systemd's restart accounting. Its hand-rolled
  backoff is replaced by the shared `ReconnectBackoff`, which also
  adds jitter.
- The dev-extra mypy floor moves from `>=1.10` to `>=2.3` (also in
  `.pre-commit-config.yaml`, which the comment there asks to keep in
  sync).  The source is clean under mypy 2.3.1 in `--strict`; the old
  floor let a contributor's environment resolve to a 1.x that checks
  materially less than CI does.
- `sfmc-api-test` is **read-only by default**.  Pass `--allow-writes`
  to run the state-changing groups (upload/deploy/delete files,
  deployment creation, script-assignment cycling, send-command); the
  runner then forwards consent to the child CLI via
  `SFMC_ASSUME_YES`, so its cleanup deletions no longer hang or fail,
  and steps that depend on a failed upload are skipped instead of
  cascading (#4)
- State-changing requests (POST/PUT/DELETE) are no longer retried
  automatically after ambiguous transport failures such as read
  timeouts — the server may already have applied them.  Failures that
  occur before transmission (connect/pool errors) still retry, and
  GET requests keep full retry behavior (#4)
- `APIError` for a transport failure (status 0) now shows the failure
  description in its message instead of the meaningless `HTTP 0`

### Fixed

- Streaming downloads (`download_glider_file`,
  `download_glider_files`) refresh an expired auth token once on
  HTTP 401, like every other request — long-lived processes no longer
  fail downloads after token expiry (#4)
- `sfmc-pull-new-downloads`: file listings paginate to exhaustion
  instead of silently stopping at 50 pages (which could permanently
  strand files past the cut); each zip member is verified
  byte-for-byte against the listing's `fileSize` before being
  installed or checkpointed, and is streamed to disk in chunks
  instead of read whole into memory; malformed state files are
  rejected with a clear `SFMCError` instead of crashing mid-run (#4)
- STOMP: `StompSubscription.close()` no longer blocks when a bounded
  queue is full; receive-loop teardown clears the connected flag so
  `subscribe()` on a dead connection fails fast; calling `connect()`
  twice raises instead of leaking the previous WebSocket and receiver
  thread; a failed SUBSCRIBE send unregisters the subscription (#4)
- `sfmc-follow`: shutdown drains queued uploads before exiting, so
  files generated just before a disconnect or Ctrl-C are uploaded
  rather than discarded; `ordered_dialog()` flushes buffered
  out-of-order messages at end of stream (in wraparound-correct
  order) instead of dropping them (#4)
- `sfmc-api init` / `add-host`: credentials are written atomically
  via a temp file created with mode 0600, so an interrupted write can
  no longer truncate the store or briefly expose the secret with
  permissive permissions; write failures and Ctrl-C at prompts exit
  cleanly instead of printing a traceback (#4)
- The sdist now ships `docs/` and `examples/`, which the README links
  throughout; the follow-glider quick start installs the `[drifter]`
  extra its example actually needs; CI runs the drifter example
  tests, builds both artifacts, checks sdist contents, and
  smoke-tests a clean wheel install (#4)

## [0.2.0] - 2026-05-15

Improvements focused on making the toolkit safer and easier to learn
for non-expert oceanographers.

### Added

- `docs/troubleshooting.md` — common error messages mapped to fixes
  (auth, SSL, multi-host, command-not-found, follower failures)
- `docs/glossary.md` — plain-language definitions of SFMC, deployment,
  yo, waypoint, `.ma`/`.mi`/`.sbd`/`.tbd`, Iridium, STOMP, etc.
- `docs/getting_started.md` — venv walkthrough, `sfmc-api init` flow,
  credential sourcing
- Waypoint sanity validation in `generate_goto_ma()`: rejects NaN/inf
  and lat/lon outside `[-90, 90]` / `[-180, 180]`, catching the most
  common follower bugs (swapped lat/lon, off-by-1000 unit errors)
  before they reach the glider
- Confirmation prompts on destructive CLI commands
  (`delete-glider-file`, `delete-*-rules`, `clear-assigned-script`).
  Bypassed by `-y` / `--yes` or `SFMC_ASSUME_YES=1` env var
- `RunStats` class returned from `follow_glider()`, with end-of-run
  summary line ("surfacings=N, files_emitted=M, upload_errors=K")
- `sfmc-follow --strict` exits with status 2 when any upload error
  occurred (intended for cron / systemd alerting)
- Inline algorithm comments in `examples/drifter_follower.py`
  explaining the two-pass drifter extrapolation

### Changed

- `dialog_parser._parse_glider_timestamp()` no longer depends on the
  host locale's month-name table; uses an explicit English table so
  non-English `LC_TIME` does not silently break timestamp parsing
- Retry-exhaustion errors in `_request` now include the underlying
  exception class and attempt count in the message
- `ordered_dialog` warning when the buffer overflows now reports the
  expected sequence and buffered range, not just the count
- Examples in README and all docs standardized on `osu685`

### Fixed

- `.gitignore` now covers `htmlcov/`, `.benchmarks/`, and `*.log`

## [0.1.1] - 2026-03-28

### Added

- `sfmc-follow` CLI command and `follow_glider()` API for autonomous
  glider navigation using pluggable follower classes
- `BaseFollower` abstract class and `load_follower_class()` for writing
  and dynamically loading follower plugins from Python files
- `DialogParser` state-machine that parses glider dialog output into
  structured `SurfacingEvent` objects (vehicle name, GPS, sensors,
  timestamps)
- `generate_goto_ma()` for generating `goto_l*.ma` waypoint files
  matching the Slocum glider firmware format
- Coordinate conversion utilities: `dddmm_to_decimal()`,
  `decimal_to_dddmm()`, `km_to_degrees()`
- `SFMCClient.upload_glider_file_contents()` for uploading
  programmatically generated files (in-memory content via `io.BytesIO`)
- Simulation modes for `sfmc-follow`:
  - `--replay LOGFILE` replays dialog from `sfmc-monitor-glider` logs
  - `--dry-run` prints generated files instead of uploading
  - Combined `--replay --dry-run` for fully offline development
- Unified pipeline: both live STOMP and replay feed through the same
  `StompSubscription` -> `ordered_dialog` -> `DialogParser` path
- Drifter follower example (`examples/drifter_follower.py`) with
  current-compensated waypoint generation from NetCDF drifter positions
- Rotating log file support (`--logfile`, `--log-max-size`,
  `--log-backup-count`, `--log-level`)
- Data-flow SVG diagram (`docs/follow_dataflow.svg`)
- Optional dependency groups: `[follow]` (pyyaml), `[drifter]`
  (pyyaml + netCDF4 + numpy)
- 120+ new tests (415 total), 87% coverage

## [0.1.0] - 2026-03-26

### Added

- `SFMCClient` with lazy authentication and 50+ REST API methods
- Full coverage of glider management, plans, file operations, script
  control, deployment, and Zmodem transfers
- Real-time STOMP-over-SockJS event streaming (connection events,
  dialog output, script events, Zmodem transfers, deployment updates)
- `sfmc-api` CLI with subcommands for all API operations plus `init`
  and `add-host` for credential management
- Multi-host credentials file support (`--host` selector)
- `--download-path` CLI option and `download_dir` property for
  configurable default download directory
- `SFMCConfig` with `from_file()` / `from_dict()` for flexible
  configuration loading
- Custom exception hierarchy: `SFMCError`, `AuthenticationError`,
  `RateLimitError`, `APIError`, `ConfigError`, `StompError`
- PEP 561 `py.typed` marker for type checker compatibility
- Example scripts: `get_glider_details.py`, `stream_glider_events.py`,
  `monitor_glider.py`
- Documentation with data-flow diagrams for every API category
- Pre-commit hooks (ruff, mypy strict)
- CI with lint, test, coverage, and install validation (Python 3.12-3.14)
- PyPI/TestPyPI publish workflow (trusted publishing)
