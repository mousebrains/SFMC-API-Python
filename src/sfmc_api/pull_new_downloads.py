#!/usr/bin/env python3
"""Watch a glider and download new from-glider files as they arrive.

Combines the real-time event streams with the file-listing and bulk
zip download endpoints to mirror new files sent from a glider into a
local directory, with minimal load on the SFMC server:

1. Subscribe to connection events and Zmodem transfer events (one
   STOMP connection, no polling while the glider is underwater).
2. When a surfacing ends (connection event with ``active: false``),
   fetch that connection's Zmodem transfer records — one request —
   to log what was transferred.
3. SFMC renames most arriving files (e.g. ``48280001.sbd``) to their
   full Dinkum-header names (``osusim-2026-191-0-1.sbd``) on a
   variable delay (observed: under a minute to ~25 minutes).  Both
   copies are downloaded when both are listed: compressed ``*.?cd``
   files may never be renamed, and modem files (``*.mri``/``*.mrd``)
   never are, so non-Dinkum names cannot simply be skipped.  They
   can, however, be *partially transferred* while a connection is
   open — so non-Dinkum-named entries are deferred while the glider
   is connected and picked up right after it disconnects.  After each
   surfacing, the listing is polled until nothing new has appeared
   for a few polls (or a hard timeout).
4. Download all new files in a single zip request using the
   ``lastModifiedAfter`` prefilter, extract into the output
   directory, and record them in a state file so nothing is fetched
   twice.  Re-transmissions of already-delivered files keep their
   original listing timestamp, so they are filtered out server-side.

Renamed files carry ``dateTimeModified`` values from the glider's own
clock (via the Dinkum file header), which may disagree with the
dockserver clock; un-renamed files carry dockserver-clock times.  All
cutoff arithmetic therefore stays in the glider-clock domain: only
Dinkum-named entries advance the high-water mark, and the server-side
prefilter is that mark minus a safety margin.  The margin must cover
how *old* a genuinely new file's glider-clock timestamp can be —
segment files from a long dive close hours before they are sent, and
backlog can be several dives older — hence the generous 48-hour
default.  Local filename dedup makes the wide overlap harmless.

Usage::

    sfmc-pull-new-downloads [options] GLIDER_NAME OUTPUT_DIR

    # Stream events and download new files as they arrive
    sfmc-pull-new-downloads --host sfmc.example.com osu685 /data/osu685

    # Single catch-up pass (for cron), no streaming
    sfmc-pull-new-downloads --host sfmc.example.com --once osu685 /data/osu685

The first run records a baseline of the current folder contents and
downloads nothing; subsequent runs (or the running stream) download
only files that appeared after the baseline.

Press Ctrl-C to stop.  State is saved after every downloaded batch,
so the process can be killed and restarted at any time; a catch-up
pass runs automatically at startup.

Loads credentials from ``~/.config/sfmc/credentials.json`` by default.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import re
import signal
import sys
import tempfile
import time
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from . import __version__
from .client import SFMCClient
from .exceptions import SFMCError
from .stomp import StompError, StompSubscription

__all__ = ["main"]

logger = logging.getLogger("sfmc.pull")

#: Listing timestamp format (server responses).
_MTIME_FMT = "%Y-%m-%d %H:%M:%S"
#: Query cutoff format (lastModifiedAfter parameter).  Minute
#: resolution; the server includes files from the named minute
#: onward, so flooring a high-water mark to the minute never skips
#: same-minute files.
_CUTOFF_FMT = "%Y%m%d%H%M"

#: Never paginate the filtered listing past this many pages (20
#: files/page).  A safety valve, not a tuning knob.
_MAX_LISTING_PAGES = 50

#: Full Dinkum-header name SFMC renames files to, e.g.
#: ``osusim-2026-191-0-8.sbd`` (glider-year-yearday-mission-segment).
#: These names appear in the listing only after the file has fully
#: arrived (the rename is atomic), and their timestamps come from the
#: glider's clock.  Everything else — 8.3 DOS names like
#: ``48280006.sbd``, compressed ``*.?cd`` forms that may never be
#: renamed, modem files (``*.mri``/``*.mrd``) that are never renamed —
#: can be observed mid-transfer with a partial, growing size, and
#: carries dockserver-clock timestamps.
_DINKUM_NAME_RE = re.compile(r"^.+-\d{4}-\d{1,3}-\d+-\d+\.\w+$")

_STATE_VERSION = 1


# ── State file ───────────────────────────────────────────────────────


@dataclass
class PullState:
    """Persistent record of which files have been seen or downloaded.

    Attributes:
        hwm: Newest listing ``dateTimeModified`` seen so far, in the
            listing's own clock domain (``YYYY-MM-DD HH:MM:SS``), or
            ``None`` before the first baseline.
        files: Map of file name to ``{"size": int, "mtime": str}``
            for every file ever seen (baseline) or downloaded.
    """

    hwm: str | None = None
    files: dict[str, dict[str, Any]] = field(default_factory=dict)

    # The high-water mark must stay in the glider-clock domain, so
    # only Dinkum-named entries may advance it: 8.3/unrenamed names
    # carry dockserver-clock mtimes, and mixing the domains (they can
    # differ by tens of minutes or more) would push the cutoff past
    # genuinely new files.

    @classmethod
    def load(cls, path: Path) -> PullState:
        """Load state from *path*, returning empty state if absent.

        Raises:
            SFMCError: If the file exists but is not a valid state
                file (corrupt JSON, wrong shape, or unknown version),
                so cron/systemd logs show a clear message instead of
                a traceback.
        """
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return cls()
        except ValueError as exc:
            raise SFMCError(f"Corrupt state file {path}: {exc}") from exc
        if not isinstance(data, dict):
            raise SFMCError(f"Corrupt state file {path}: expected a JSON object")
        if data.get("version") != _STATE_VERSION:
            raise SFMCError(f"Unsupported state file version {data.get('version')!r} in {path}")
        return cls(hwm=data.get("hwm"), files=data.get("files", {}))

    def save(self, path: Path) -> None:
        """Atomically write state to *path*."""
        payload = json.dumps(
            {"version": _STATE_VERSION, "hwm": self.hwm, "files": self.files},
            indent=1,
        )
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(path)

    def observe(self, name: str, mtime: str, size: int) -> None:
        """Record a file; Dinkum-named entries advance the high-water mark."""
        self.files[name] = {"size": size, "mtime": mtime}
        if _DINKUM_NAME_RE.match(name) and (self.hwm is None or mtime > self.hwm):
            self.hwm = mtime

    def is_new(self, name: str, mtime: str) -> bool:
        """True if *name* is unseen, or seen with a different mtime."""
        known = self.files.get(name)
        return known is None or known.get("mtime") != mtime


# ── Timestamp helpers ────────────────────────────────────────────────


def cutoff_before(mtime: str, margin_minutes: int) -> str:
    """Convert a listing mtime to a ``lastModifiedAfter`` cutoff.

    Floors to the minute and subtracts *margin_minutes*.  Both values
    are in the listing's clock domain, so no cross-clock comparison
    happens.

    Args:
        mtime: Listing timestamp (``YYYY-MM-DD HH:MM:SS``).
        margin_minutes: Safety margin to subtract.

    Returns:
        A cutoff string in ``yyyyMMddHHmm`` format.
    """
    dt = datetime.strptime(mtime, _MTIME_FMT) - timedelta(minutes=margin_minutes)
    return dt.strftime(_CUTOFF_FMT)


# ── Server interaction ───────────────────────────────────────────────


def _iter_listing(
    client: SFMCClient,
    glider_name: str,
    cutoff: str | None,
) -> Iterator[dict[str, Any]]:
    """Yield from-glider listing entries page by page.

    Stops at the first short page, or at ``_MAX_LISTING_PAGES`` with
    a warning (a safety valve, not a tuning knob).
    """
    for page in range(_MAX_LISTING_PAGES):
        result = client.get_folder_file_listing(
            glider_name,
            "from-glider",
            page=page,
            last_modified_after=cutoff,
        )
        entries = result.get("results", [])
        yield from entries
        if len(entries) < result.get("limit", 20):
            return
    logger.warning(
        "Listing exceeded %d pages; processing what was fetched",
        _MAX_LISTING_PAGES,
    )


def list_new_files(
    client: SFMCClient,
    glider_name: str,
    state: PullState,
    margin_minutes: int,
    *,
    connected: bool = False,
) -> list[dict[str, Any]]:
    """Return listing entries not yet recorded in *state*.

    Uses the ``lastModifiedAfter`` prefilter (high-water mark minus
    margin) so the server only returns the recent window; local
    filename comparison removes the overlap.

    Args:
        connected: Whether the glider currently has an open dockserver
            connection.  Files transfer only over open connections, so
            while one is open a non-Dinkum-named entry (8.3 name,
            ``*.?cd``, ``*.mri``/``*.mrd``, …) may still be arriving
            and is deferred; once the connection closes every listed
            entry is complete and downloadable.  Dinkum-named entries
            appear only via SFMC's atomic rename and are safe anytime.

    Returns:
        Listing entries (``fileName`` / ``dateTimeModified`` /
        ``fileSize``) that are new or have changed mtimes.
    """
    cutoff = None
    if state.hwm is not None:
        cutoff = cutoff_before(state.hwm, margin_minutes)

    new: list[dict[str, Any]] = []
    for entry in _iter_listing(client, glider_name, cutoff):
        name = entry["fileName"]
        if connected and not _DINKUM_NAME_RE.match(name):
            logger.debug("deferring possibly in-flight entry %s", name)
            continue
        if state.is_new(name, entry["dateTimeModified"]):
            new.append(entry)
    return new


def download_new_files(
    client: SFMCClient,
    glider_name: str,
    new_entries: list[dict[str, Any]],
    output_dir: Path,
    state: PullState,
    state_path: Path,
) -> int:
    """Download *new_entries* in one zip request and extract them.

    The zip request uses a cutoff derived from the oldest new entry,
    so the archive stays as small as the server allows.  Only members
    named in *new_entries* are kept; margin-overlap duplicates are
    discarded.  Extracted files get their listing mtime (interpreted
    as UTC — glider clock domain) so downstream tools see a
    meaningful timestamp.

    Returns:
        The number of files written to *output_dir*.
    """
    wanted = {e["fileName"]: e for e in new_entries}
    oldest = min(e["dateTimeModified"] for e in new_entries)
    # The wide listing margin exists to *find* old-stamped files; the
    # entries' mtimes are exact once found, so the zip request only
    # needs a minute of slack, not the full margin — otherwise every
    # batch re-downloads (and discards) up to margin-worth of files.
    cutoff = cutoff_before(oldest, 1)

    written: set[str] = set()
    with tempfile.TemporaryDirectory(prefix="sfmc-pull-") as tmp:
        zip_path = Path(tmp) / "batch.zip"
        client.download_glider_files(
            glider_name,
            "from-glider",
            zip_path,
            last_modified_after=cutoff,
        )
        with zipfile.ZipFile(zip_path) as zf:
            for member in zf.infolist():
                name = os.path.basename(member.filename)
                entry = wanted.get(name)
                if entry is None or member.is_dir():
                    continue
                target = output_dir / name
                part = target.with_suffix(target.suffix + ".part")
                with zf.open(member) as src, open(part, "wb") as dst:
                    dst.write(src.read())
                mtime_dt = datetime.strptime(entry["dateTimeModified"], _MTIME_FMT)
                epoch = mtime_dt.replace(tzinfo=UTC).timestamp()
                os.utime(part, (epoch, epoch))
                part.replace(target)
                state.observe(name, entry["dateTimeModified"], entry["fileSize"])
                written.add(name)
                logger.info(
                    "downloaded %s (%d bytes, modified %s)",
                    name,
                    entry["fileSize"],
                    entry["dateTimeModified"],
                )

    missing = set(wanted) - written
    if missing:
        logger.warning(
            "%d expected file(s) absent from zip, will retry next pass: %s",
            len(missing),
            ", ".join(sorted(missing)),
        )
    if written:
        state.save(state_path)
    return len(written)


def reconcile(
    client: SFMCClient,
    glider_name: str,
    output_dir: Path,
    state: PullState,
    state_path: Path,
    margin_minutes: int,
    *,
    connected: bool = False,
) -> int:
    """One listing-vs-state pass: download whatever is new.

    Returns:
        The number of files downloaded.
    """
    new_entries = list_new_files(client, glider_name, state, margin_minutes, connected=connected)
    if not new_entries:
        return 0
    return download_new_files(
        client,
        glider_name,
        new_entries,
        output_dir,
        state,
        state_path,
    )


def baseline(
    client: SFMCClient,
    glider_name: str,
    state: PullState,
    state_path: Path,
    margin_minutes: int,
) -> None:
    """Record the current folder contents without downloading.

    Every file inside the future query window (high-water mark minus
    margin) must be recorded, or the first reconcile would see it as
    new and download deployment history.  Files older than the window
    stay invisible to all future passes, so their names are never
    needed.

    The high-water mark can only come from a Dinkum-named entry, and
    the newest page may hold none (e.g. right after a surfacing burst
    whose renames are still pending) — so the unfiltered walk
    continues, recording everything it passes, until it finds one.
    If the folder has no Dinkum-named files at all, the mark stays
    unset and every entry walked is recorded; reconciles then run
    unfiltered until the first rename appears.
    """
    for entry in _iter_listing(client, glider_name, None):
        state.observe(entry["fileName"], entry["dateTimeModified"], entry["fileSize"])
        if state.hwm is not None:
            break

    if state.hwm is not None:
        cutoff = cutoff_before(state.hwm, margin_minutes)
        for entry in _iter_listing(client, glider_name, cutoff):
            state.observe(entry["fileName"], entry["dateTimeModified"], entry["fileSize"])
    else:
        logger.warning(
            "no Dinkum-named files found; high-water mark unset — "
            "reconciles will scan unfiltered until the first rename appears"
        )

    state.save(state_path)
    logger.info(
        "baseline recorded: %d file(s), high-water mark %s — nothing downloaded",
        len(state.files),
        state.hwm,
    )


def glider_is_connected(client: SFMCClient, glider_name: str) -> bool:
    """True unless the glider's state is reported as ``disconnected``.

    Unknown or missing states count as connected, the conservative
    choice: it defers possibly in-flight files rather than risking a
    partial download.
    """
    try:
        details = client.get_glider_details(glider_name)
        state = details["data"]["state"]
    except (SFMCError, KeyError, TypeError) as exc:
        logger.warning("could not determine glider state (%s); assuming connected", exc)
        return True
    return str(state) != "disconnected"


def log_transfers(client: SFMCClient, connection_id: int) -> None:
    """Log a one-line summary of a connection's Zmodem transfers."""
    try:
        data = client.get_zmodem_transfers(connection_id)["data"]
    except (SFMCError, KeyError, TypeError) as exc:
        logger.info("connection %d: no transfer records (%s)", connection_id, exc)
        return
    downloads = [t for t in data.get("downloads", []) if t.get("transferStatus") == "Completed"]
    logger.info(
        "connection %d: %d download(s) (%d bytes), %d upload(s) — "
        "waiting for files to appear in listing",
        connection_id,
        len(downloads),
        data.get("totalDownloadBytes", 0),
        len(data.get("uploads", [])),
    )


def try_reconcile(
    client: SFMCClient,
    args: argparse.Namespace,
    state: PullState,
    state_path: Path,
    *,
    connected: bool,
) -> int:
    """Reconcile, degrading any failure to a logged warning.

    The daemon must survive transient server outages, rate limiting,
    and malformed responses: a failed pass costs one poll cycle — the
    settle window, idle reconcile, or reconnect catch-up retries
    naturally.  KeyboardInterrupt/SystemExit still propagate.
    """
    try:
        return reconcile(
            client,
            args.glider_name,
            args.output_dir,
            state,
            state_path,
            args.margin_minutes,
            connected=connected,
        )
    except Exception as exc:
        logger.warning("reconcile pass failed (%s: %s); will retry", type(exc).__name__, exc)
        return 0


# ── Event-driven main loop ───────────────────────────────────────────


def _drain(sub: StompSubscription) -> tuple[list[Any], bool]:
    """Return ``(queued messages, closed)`` from *sub* without blocking.

    The subscription's close sentinel (a single ``None``) must be
    reported, not swallowed: it is enqueued exactly once, and losing
    it here would leave the caller looping forever on a dead stream.
    """
    messages: list[Any] = []
    while True:
        try:
            msg = sub.get(timeout=0.05)
        except queue.Empty:
            return messages, False
        if msg is None:
            return messages, True
        messages.append(msg)


def stream_once(
    client: SFMCClient,
    args: argparse.Namespace,
    state: PullState,
    state_path: Path,
) -> None:
    """Run one STOMP session until the connection drops.

    Waits for connection-close events; after each, polls the listing
    for new files.  SFMC's rename delay is unpredictable (seconds to
    tens of minutes), so polls back off (``--settle-poll`` initial
    interval, growing 1.5x to a 5-minute cap) and the window only
    ends early — after ``--settle-quiet`` consecutive quiet polls —
    once at least one file has been downloaded in it.  A window with
    no downloads runs to ``--settle-timeout``; surfacings whose
    transfers were all re-sends produce nothing to download, and the
    timeout is what stops the polling in that case.  While idle,
    reconciles every ``--reconcile-interval`` seconds as a
    missed-event backstop.

    Raises:
        StompError: If the server sends a STOMP error frame.
    """
    with client.open_stream() as stomp:
        conn_sub = client.subscribe_connection_events(args.glider_name, stomp)
        zmodem_sub = client.subscribe_zmodem_transfer_events(args.glider_name, stomp)
        connected = glider_is_connected(client, args.glider_name)
        logger.info(
            "subscribed; %s is %s",
            args.glider_name,
            "connected" if connected else "disconnected — waiting for a surfacing",
        )

        settle_until: float | None = None  # deadline of current settle window
        quiet_polls = 0
        window_downloads = 0
        poll_interval = args.settle_poll
        next_poll = 0.0
        last_activity = time.monotonic()

        while True:
            try:
                msg = conn_sub.get(timeout=poll_interval if settle_until else 30.0)
            except queue.Empty:
                msg = None
            else:
                if msg is None:
                    logger.warning("event stream closed")
                    return

            now = time.monotonic()
            drained, conn_closed = _drain(conn_sub)
            events = ([msg] if msg else []) + drained
            for event_list in events:
                for event in event_list if isinstance(event_list, list) else [event_list]:
                    if event.get("active") is False:
                        logger.info(
                            "connection %s closed (%s — %s)",
                            event.get("id"),
                            event.get("startDateTime"),
                            event.get("endDateTime"),
                        )
                        connected = False
                        log_transfers(client, event["id"])
                        settle_until = now + args.settle_timeout
                        quiet_polls = 0
                        window_downloads = 0
                        poll_interval = args.settle_poll
                        next_poll = now  # poll immediately
                    elif event.get("active") is True:
                        connected = True
                        logger.info(
                            "connection %s opened at %s",
                            event.get("id"),
                            event.get("startDateTime"),
                        )

            zmodem_bodies, zmodem_closed = _drain(zmodem_sub)
            for body in zmodem_bodies:
                logger.debug("zmodem transfer activity: %s", body)

            if conn_closed or zmodem_closed:
                logger.warning("event stream closed")
                return

            if settle_until is not None and now >= next_poll:
                downloaded = try_reconcile(client, args, state, state_path, connected=connected)
                if downloaded:
                    quiet_polls = 0
                    window_downloads += downloaded
                    poll_interval = args.settle_poll
                    last_activity = now
                else:
                    quiet_polls += 1
                    poll_interval = min(poll_interval * 1.5, 300.0)
                next_poll = now + poll_interval
                if window_downloads and quiet_polls >= args.settle_quiet:
                    logger.info("settle window done: %d file(s) downloaded", window_downloads)
                    settle_until = None
                elif now >= settle_until:
                    logger.info(
                        "settle window ended after %ds with %d file(s); any "
                        "pending renames will be caught by the idle reconcile",
                        args.settle_timeout,
                        window_downloads,
                    )
                    settle_until = None

            if settle_until is None and now - last_activity >= args.reconcile_interval:
                last_activity = now
                downloaded = try_reconcile(client, args, state, state_path, connected=connected)
                if downloaded:
                    logger.info("idle reconcile caught %d file(s)", downloaded)


def run_stream(
    client: SFMCClient,
    args: argparse.Namespace,
    state: PullState,
    state_path: Path,
) -> None:
    """Stream events forever, reconnecting with backoff on drops.

    Survives anything short of KeyboardInterrupt/SystemExit: server
    outages, auth hiccups, and malformed responses all reduce to a
    logged warning and a backed-off reconnect.  Backoff only resets
    after a session that lived a while, so a flapping stream cannot
    reconnect-storm every 15 seconds forever.
    """
    backoff = 15.0
    while True:
        session_start = time.monotonic()
        try:
            stream_once(client, args, state, state_path)
        except StompError as exc:
            logger.warning("STOMP error: %s", exc)
        except Exception as exc:
            logger.warning("stream session failed (%s: %s)", type(exc).__name__, exc)
        if time.monotonic() - session_start > 60.0:
            backoff = 15.0
        logger.info("reconnecting in %.0fs", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 300.0)
        # Catch anything that arrived while the stream was down.
        try_reconcile(
            client,
            args,
            state,
            state_path,
            connected=glider_is_connected(client, args.glider_name),
        )


# ── Entry point ──────────────────────────────────────────────────────


def _nonnegative_int(value: str) -> int:
    """argparse type: an integer >= 0.

    A negative margin would place the cutoff *after* the high-water
    mark and guarantee missed files.
    """
    n = int(value)
    if n < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return n


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for ``sfmc-pull-new-downloads``."""
    parser = argparse.ArgumentParser(
        prog="sfmc-pull-new-downloads",
        description=(
            "Mirror new from-glider files into a local directory, driven by real-time SFMC events."
        ),
    )
    parser.add_argument("glider_name", metavar="GLIDER_NAME", help="Registered glider name")
    parser.add_argument(
        "output_dir",
        metavar="OUTPUT_DIR",
        type=Path,
        help="Directory to download new files into (created if needed)",
    )
    parser.add_argument(
        "--credentials",
        metavar="PATH",
        default=None,
        help="Credentials JSON file (default: ~/.config/sfmc/credentials.json)",
    )
    parser.add_argument(
        "--host",
        metavar="HOSTNAME",
        default=None,
        help="SFMC server hostname (default: sole host in credentials file)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Do a single catch-up pass and exit (no event streaming)",
    )
    parser.add_argument(
        "--state-file",
        metavar="PATH",
        type=Path,
        default=None,
        help="State file path (default: OUTPUT_DIR/.sfmc-pull-state.json)",
    )
    parser.add_argument(
        "--margin-minutes",
        type=_nonnegative_int,
        default=2880,
        metavar="N",
        help=(
            "Safety margin subtracted from the high-water mark.  New files "
            "carry glider-clock timestamps that can be a full dive old (or "
            "several dives, for backlog), so the margin must comfortably "
            "exceed the longest dive (default: 2880 = 48 h)"
        ),
    )
    parser.add_argument(
        "--settle-poll",
        type=float,
        default=60.0,
        metavar="SECS",
        help=(
            "Initial listing poll interval after a surfacing; "
            "backs off 1.5x to a 5 min cap (default: 60)"
        ),
    )
    parser.add_argument(
        "--settle-quiet",
        type=int,
        default=3,
        metavar="N",
        help="Quiet polls before a settle window ends (default: 3)",
    )
    parser.add_argument(
        "--settle-timeout",
        type=float,
        default=1800.0,
        metavar="SECS",
        help="Hard limit on a settle window (default: 1800)",
    )
    parser.add_argument(
        "--reconcile-interval",
        type=float,
        default=900.0,
        metavar="SECS",
        help="Idle listing check as missed-event backstop (default: 900)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main() -> None:
    """Entry point for the ``sfmc-pull-new-downloads`` console script."""
    args = build_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )
    if not args.verbose:
        logging.getLogger("httpx").setLevel(logging.WARNING)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    state_path = args.state_file or args.output_dir / ".sfmc-pull-state.json"

    signal.signal(signal.SIGTERM, lambda *_: sys.exit(143))

    code = 0
    try:
        state = PullState.load(state_path)
        with SFMCClient(config_path=args.credentials, host=args.host) as client:
            # Baseline only when no state file exists: an existing file
            # with empty contents means "baselined an empty folder", and
            # re-baselining then would mark everything that arrived since
            # as seen without downloading it.
            if not state_path.exists():
                baseline(client, args.glider_name, state, state_path, args.margin_minutes)
            else:
                caught_up = reconcile(
                    client,
                    args.glider_name,
                    args.output_dir,
                    state,
                    state_path,
                    args.margin_minutes,
                    connected=glider_is_connected(client, args.glider_name),
                )
                if caught_up:
                    logger.info("startup catch-up downloaded %d file(s)", caught_up)
            if not args.once:
                run_stream(client, args, state, state_path)
    except (SFMCError, OSError) as exc:
        sys.stderr.write(f"Error: {exc}\n")
        code = 1
    except KeyboardInterrupt:
        sys.stderr.write("\nStopped.\n")
        code = 130

    sys.exit(code)


if __name__ == "__main__":
    main()
