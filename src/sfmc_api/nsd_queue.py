"""SQLite-backed queue shared by the web tier and the NSD worker.

A "next surfacing do" request flows through two independent processes:

* The **web endpoint** (producer) validates commands and calls
  :func:`enqueue`, then returns immediately.  It never waits for a
  glider — the request just becomes a durable row.
* The **worker daemon** (consumer, see :mod:`sfmc_api.nsd_worker`)
  claims queued rows, waits for the glider to surface, injects, and
  writes the outcome back.

They communicate *only* through this table — no direct calls.  The web
page shows status by reading rows (polling); because the store is
durable, the browser can be closed the whole time and the worker still
does its job.

Status lifecycle
----------------

``queued``     Just created by the web tier; not yet claimed.
``waiting``    Claimed by a worker; waiting for the glider to surface.
``injecting``  Glider surfaced, script paused, commands being sent.
``done``       All commands confirmed and the script resumed.
``failed``     A command failed to verify, or no quiescent prompt was
               reached (the script is still resumed).
``cancelled``  Withdrawn by the web tier before a worker started it.

SQLite is opened in WAL mode with a busy timeout so the worker's writes
and the web tier's reads do not block each other.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

#: Statuses a request can be withdrawn from by the web tier.
CANCELLABLE = ("queued", "waiting")
#: Statuses considered finished (won't be processed again).
TERMINAL = ("done", "failed", "cancelled")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS nsd_requests (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    glider      TEXT NOT NULL,
    commands    TEXT NOT NULL,              -- JSON array of command strings
    status      TEXT NOT NULL DEFAULT 'queued',
    worker      TEXT,                        -- id of the claiming worker
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    result      TEXT,                        -- JSON of InjectResult (when done)
    error       TEXT
);
CREATE INDEX IF NOT EXISTS ix_nsd_status ON nsd_requests (status);
CREATE INDEX IF NOT EXISTS ix_nsd_glider ON nsd_requests (glider);
"""


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class Request:
    """One NSD request row."""

    id: int
    glider: str
    commands: list[str]
    status: str
    worker: str | None
    created_at: str
    updated_at: str
    result: dict | None
    error: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Request:
        return cls(
            id=row["id"],
            glider=row["glider"],
            commands=json.loads(row["commands"]),
            status=row["status"],
            worker=row["worker"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            result=json.loads(row["result"]) if row["result"] else None,
            error=row["error"],
        )


# ── Connection ───────────────────────────────────────────────────────


def connect(db_path: Path | str) -> sqlite3.Connection:
    """Open (and initialise) the queue database.

    Safe to call from both the web tier and the worker.  WAL mode plus a
    busy timeout lets concurrent readers and the single writer coexist.
    """
    conn = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(_SCHEMA)
    return conn


# ── Producer (web tier) ──────────────────────────────────────────────


def enqueue(conn: sqlite3.Connection, glider: str, commands: list[str]) -> int:
    """Create a queued request and return its id.

    Called by the web endpoint.  *commands* should already be validated
    / allow-listed by the caller.
    """
    if not commands:
        raise ValueError("commands must not be empty")
    now = _now()
    cur = conn.execute(
        "INSERT INTO nsd_requests (glider, commands, status, created_at, updated_at) "
        "VALUES (?, ?, 'queued', ?, ?)",
        (glider, json.dumps(commands), now, now),
    )
    return int(cur.lastrowid)


def cancel(conn: sqlite3.Connection, req_id: int) -> bool:
    """Withdraw a request that a worker has not begun injecting.

    Returns True if it was cancelled.  Fails (returns False) once the
    status is ``injecting`` or terminal.
    """
    placeholders = ",".join("?" for _ in CANCELLABLE)
    cur = conn.execute(
        f"UPDATE nsd_requests SET status='cancelled', updated_at=? "
        f"WHERE id=? AND status IN ({placeholders})",
        (_now(), req_id, *CANCELLABLE),
    )
    return cur.rowcount > 0


# ── Reads (web tier + worker) ────────────────────────────────────────


def get(conn: sqlite3.Connection, req_id: int) -> Request | None:
    row = conn.execute("SELECT * FROM nsd_requests WHERE id=?", (req_id,)).fetchone()
    return Request.from_row(row) if row else None


def list_recent(
    conn: sqlite3.Connection,
    glider: str | None = None,
    limit: int = 50,
) -> list[Request]:
    """Most-recent requests first — what the web page renders."""
    if glider:
        rows = conn.execute(
            "SELECT * FROM nsd_requests WHERE glider=? ORDER BY id DESC LIMIT ?",
            (glider, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM nsd_requests ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [Request.from_row(r) for r in rows]


def queued_gliders(conn: sqlite3.Connection) -> list[str]:
    """Distinct gliders that currently have queued work."""
    rows = conn.execute(
        "SELECT DISTINCT glider FROM nsd_requests WHERE status='queued' ORDER BY glider"
    ).fetchall()
    return [r["glider"] for r in rows]


# ── Consumer (worker) ────────────────────────────────────────────────


def claim_next(conn: sqlite3.Connection, glider: str, worker: str) -> Request | None:
    """Atomically claim the oldest queued request for *glider*.

    Moves it ``queued`` → ``waiting`` and stamps the worker id, so no
    other worker can take it.  Returns the claimed request, or ``None``
    if there is nothing queued for that glider.
    """
    row = conn.execute(
        "SELECT id FROM nsd_requests WHERE glider=? AND status='queued' "
        "ORDER BY id ASC LIMIT 1",
        (glider,),
    ).fetchone()
    if row is None:
        return None
    cur = conn.execute(
        "UPDATE nsd_requests SET status='waiting', worker=?, updated_at=? "
        "WHERE id=? AND status='queued'",
        (worker, _now(), row["id"]),
    )
    if cur.rowcount == 0:
        # Lost the race to another worker — let the caller retry.
        return None
    return get(conn, row["id"])


def set_status(
    conn: sqlite3.Connection,
    req_id: int,
    status: str,
    *,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    """Update a request's status (and optionally its result/error)."""
    conn.execute(
        "UPDATE nsd_requests SET status=?, "
        "result=COALESCE(?, result), error=COALESCE(?, error), updated_at=? "
        "WHERE id=?",
        (
            status,
            json.dumps(result) if result is not None else None,
            error,
            _now(),
            req_id,
        ),
    )


def reset_stale(conn: sqlite3.Connection, worker: str) -> int:
    """Requeue this worker's in-flight rows after a restart/crash.

    Any ``waiting``/``injecting`` rows owned by *worker* are returned to
    ``queued`` so they are retried on the next surfacing.  Returns the
    number reset.  Call once at worker startup.
    """
    cur = conn.execute(
        "UPDATE nsd_requests SET status='queued', worker=NULL, updated_at=? "
        "WHERE worker=? AND status IN ('waiting','injecting')",
        (_now(), worker),
    )
    return cur.rowcount
