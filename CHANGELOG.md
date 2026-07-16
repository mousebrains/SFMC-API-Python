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
