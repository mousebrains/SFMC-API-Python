# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
