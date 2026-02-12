"""SQLite database for task history, stage runs, usage snapshots, and event logs."""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS tasks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id       TEXT NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    project         TEXT NOT NULL,
    slot            INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    started_at      TEXT,
    completed_at    TEXT,
    cost_usd        REAL NOT NULL DEFAULT 0.0,
    turns           INTEGER NOT NULL DEFAULT 0,
    review_iterations INTEGER NOT NULL DEFAULT 0,
    comments        TEXT NOT NULL DEFAULT '',
    limit_interruptions INTEGER NOT NULL DEFAULT 0,
    failure_reason  TEXT
);

CREATE TABLE IF NOT EXISTS stage_runs (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id             INTEGER NOT NULL REFERENCES tasks(id),
    stage               TEXT NOT NULL,
    iteration           INTEGER NOT NULL DEFAULT 1,
    session_id          TEXT,
    turns               INTEGER NOT NULL DEFAULT 0,
    duration_seconds    REAL,
    cost_usd            REAL NOT NULL DEFAULT 0.0,
    exit_subtype        TEXT,
    was_limit_restart   INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS usage_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    utilization_5h      REAL,
    utilization_7d      REAL,
    resets_at           TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER REFERENCES tasks(id),
    event_type  TEXT NOT NULL,
    detail      TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_stage_runs_task_id ON stage_runs(task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_type ON task_events(event_type);
CREATE INDEX IF NOT EXISTS idx_tasks_ticket_id ON tasks(ticket_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def init_db(db_path: str | Path) -> sqlite3.Connection:
    """Open (or create) the database and ensure the schema exists.

    Returns a sqlite3.Connection with WAL mode and foreign keys enabled.
    """
    db_path = Path(db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    # executescript() implicitly commits and can reset pragmas — re-apply FK enforcement
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
    )
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
    elif row[0] != SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version mismatch: expected {SCHEMA_VERSION}, got {row[0]}. "
            "Manual migration required."
        )
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Task helpers
# ---------------------------------------------------------------------------

def insert_task(
    conn: sqlite3.Connection,
    *,
    ticket_id: str,
    title: str,
    project: str,
    slot: int,
    status: str = "pending",
) -> int:
    """Insert a new task (or reset an existing one for retries) and return its id.

    If a task with the same ticket_id already exists (e.g. a retry after
    failure), the existing row is updated with fresh values instead of
    raising an IntegrityError.
    """
    cur = conn.execute(
        """
        INSERT INTO tasks (ticket_id, title, project, slot, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(ticket_id) DO UPDATE SET
            title = excluded.title,
            project = excluded.project,
            slot = excluded.slot,
            status = excluded.status,
            created_at = excluded.created_at,
            started_at = NULL,
            completed_at = NULL,
            cost_usd = 0.0,
            turns = 0,
            review_iterations = 0,
            comments = '',
            limit_interruptions = 0,
            failure_reason = NULL
        """,
        (ticket_id, title, project, slot, status, _now_iso()),
    )
    # ON CONFLICT ... DO UPDATE sets lastrowid; for the update case we need
    # to look up the existing id.
    if cur.lastrowid:
        return cur.lastrowid
    row = conn.execute(
        "SELECT id FROM tasks WHERE ticket_id = ?", (ticket_id,)
    ).fetchone()
    return row[0]


def update_task(
    conn: sqlite3.Connection,
    task_id: int,
    **fields: object,
) -> None:
    """Update one or more columns on a task row.

    Only the columns passed as keyword arguments are updated.
    Allowed columns: status, started_at, completed_at, cost_usd, turns,
    review_iterations, comments, limit_interruptions, failure_reason.
    """
    allowed = {
        "status",
        "started_at",
        "completed_at",
        "cost_usd",
        "turns",
        "review_iterations",
        "comments",
        "limit_interruptions",
        "failure_reason",
    }
    bad = set(fields) - allowed
    if bad:
        raise ValueError(f"Cannot update disallowed columns: {sorted(bad)}")
    if not fields:
        return

    set_clause = ", ".join(f"{col} = ?" for col in fields)
    values = list(fields.values())
    values.append(task_id)
    conn.execute(f"UPDATE tasks SET {set_clause} WHERE id = ?", values)  # noqa: S608


def increment_limit_interruptions(conn: sqlite3.Connection, task_id: int) -> None:
    """Atomically increment the limit_interruptions counter on a task."""
    conn.execute(
        "UPDATE tasks SET limit_interruptions = limit_interruptions + 1 WHERE id = ?",
        (task_id,),
    )


def get_task(conn: sqlite3.Connection, task_id: int) -> sqlite3.Row | None:
    """Fetch a single task by id."""
    return conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()


def get_task_by_ticket(conn: sqlite3.Connection, ticket_id: str) -> sqlite3.Row | None:
    """Fetch a single task by Linear ticket identifier."""
    return conn.execute(
        "SELECT * FROM tasks WHERE ticket_id = ?", (ticket_id,)
    ).fetchone()


def get_task_history(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    offset: int = 0,
    project: str | None = None,
    status: str | None = None,
    search: str | None = None,
    sort_by: str = "created_at",
    sort_dir: str = "DESC",
) -> list[sqlite3.Row]:
    """Return recent tasks, newest first. Optionally filter by project/status/search."""
    allowed_sort_cols = {
        "ticket_id", "title", "project", "status", "cost_usd", "turns",
        "review_iterations", "limit_interruptions", "created_at",
        "started_at", "completed_at",
    }
    if sort_by not in allowed_sort_cols:
        sort_by = "created_at"
    if sort_dir.upper() not in ("ASC", "DESC"):
        sort_dir = "DESC"

    query = "SELECT * FROM tasks WHERE 1=1"
    params: list[object] = []
    if project is not None:
        query += " AND project = ?"
        params.append(project)
    if status is not None:
        query += " AND status = ?"
        params.append(status)
    if search is not None:
        query += " AND (ticket_id LIKE ? OR title LIKE ?)"
        like = f"%{search}%"
        params.extend([like, like])
    query += f" ORDER BY {sort_by} {sort_dir.upper()} LIMIT ? OFFSET ?"  # noqa: S608
    params.extend([limit, offset])
    return conn.execute(query, params).fetchall()


def count_tasks(
    conn: sqlite3.Connection,
    *,
    project: str | None = None,
    status: str | None = None,
    search: str | None = None,
) -> int:
    """Count tasks matching the given filters."""
    query = "SELECT COUNT(*) FROM tasks WHERE 1=1"
    params: list[object] = []
    if project is not None:
        query += " AND project = ?"
        params.append(project)
    if status is not None:
        query += " AND status = ?"
        params.append(status)
    if search is not None:
        query += " AND (ticket_id LIKE ? OR title LIKE ?)"
        like = f"%{search}%"
        params.extend([like, like])
    return conn.execute(query, params).fetchone()[0]


def get_distinct_projects(conn: sqlite3.Connection) -> list[str]:
    """Return distinct project names from tasks table."""
    rows = conn.execute("SELECT DISTINCT project FROM tasks ORDER BY project").fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Stage run helpers
# ---------------------------------------------------------------------------

def insert_stage_run(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    stage: str,
    iteration: int = 1,
    session_id: str | None = None,
    turns: int = 0,
    duration_seconds: float | None = None,
    cost_usd: float = 0.0,
    exit_subtype: str | None = None,
    was_limit_restart: bool = False,
) -> int:
    """Insert a stage run record and return its id."""
    cur = conn.execute(
        """
        INSERT INTO stage_runs
            (task_id, stage, iteration, session_id, turns,
             duration_seconds, cost_usd, exit_subtype, was_limit_restart, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            stage,
            iteration,
            session_id,
            turns,
            duration_seconds,
            cost_usd,
            exit_subtype,
            int(was_limit_restart),
            _now_iso(),
        ),
    )
    return cur.lastrowid


def get_stage_runs(conn: sqlite3.Connection, task_id: int) -> list[sqlite3.Row]:
    """Return all stage runs for a task, ordered by creation time."""
    return conn.execute(
        "SELECT * FROM stage_runs WHERE task_id = ? ORDER BY created_at",
        (task_id,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Usage snapshot helpers
# ---------------------------------------------------------------------------

def insert_usage_snapshot(
    conn: sqlite3.Connection,
    *,
    utilization_5h: float | None = None,
    utilization_7d: float | None = None,
    resets_at: str | None = None,
) -> int:
    """Insert a usage snapshot and return its id."""
    cur = conn.execute(
        """
        INSERT INTO usage_snapshots (utilization_5h, utilization_7d, resets_at, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (utilization_5h, utilization_7d, resets_at, _now_iso()),
    )
    return cur.lastrowid


def get_usage_snapshots(
    conn: sqlite3.Connection, *, limit: int = 100
) -> list[sqlite3.Row]:
    """Return recent usage snapshots, newest first."""
    return conn.execute(
        "SELECT * FROM usage_snapshots ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()


# ---------------------------------------------------------------------------
# Event helpers
# ---------------------------------------------------------------------------

def insert_event(
    conn: sqlite3.Connection,
    *,
    task_id: int | None = None,
    event_type: str,
    detail: str | None = None,
) -> int:
    """Insert a task event and return its id."""
    cur = conn.execute(
        """
        INSERT INTO task_events (task_id, event_type, detail, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (task_id, event_type, detail, _now_iso()),
    )
    return cur.lastrowid


def get_events(
    conn: sqlite3.Connection,
    *,
    task_id: int | None = None,
    event_type: str | None = None,
    limit: int = 100,
) -> list[sqlite3.Row]:
    """Return events, newest first. Optionally filter by task or type."""
    query = "SELECT * FROM task_events WHERE 1=1"
    params: list[object] = []
    if task_id is not None:
        query += " AND task_id = ?"
        params.append(task_id)
    if event_type is not None:
        query += " AND event_type = ?"
        params.append(event_type)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    return conn.execute(query, params).fetchall()
