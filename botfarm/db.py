"""SQLite database for task history, stage runs, usage snapshots, event logs, and slot runtime state."""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 5

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
    failure_reason  TEXT,
    pr_url          TEXT,
    pipeline_stage  TEXT,
    review_state    TEXT
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
    resets_at_7d        TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS task_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER REFERENCES tasks(id),
    event_type  TEXT NOT NULL,
    detail      TEXT,
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS slots (
    project             TEXT NOT NULL,
    slot_id             INTEGER NOT NULL,
    status              TEXT NOT NULL DEFAULT 'free',
    ticket_id           TEXT,
    ticket_title        TEXT,
    branch              TEXT,
    pr_url              TEXT,
    stage               TEXT,
    stage_iteration     INTEGER NOT NULL DEFAULT 0,
    current_session_id  TEXT,
    started_at          TEXT,
    stage_started_at    TEXT,
    sigterm_sent_at     TEXT,
    pid                 INTEGER,
    interrupted_by_limit INTEGER NOT NULL DEFAULT 0,
    resume_after        TEXT,
    stages_completed    TEXT,
    updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY(project, slot_id)
);

CREATE TABLE IF NOT EXISTS dispatch_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    paused          INTEGER NOT NULL DEFAULT 0,
    pause_reason    TEXT,
    supervisor_heartbeat TEXT,
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_stage_runs_task_id ON stage_runs(task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_task_id ON task_events(task_id);
CREATE INDEX IF NOT EXISTS idx_task_events_type ON task_events(event_type);
CREATE INDEX IF NOT EXISTS idx_tasks_ticket_id ON tasks(ticket_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _migrate(conn: sqlite3.Connection, from_version: int) -> None:
    """Run schema migrations from *from_version* up to SCHEMA_VERSION."""
    if from_version < 2:
        # v1→v2: add pr_url, pipeline_stage, review_state to tasks
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(tasks)").fetchall()
        }
        for col in ("pr_url", "pipeline_stage", "review_state"):
            if col not in existing:
                conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} TEXT")  # noqa: S608
    if from_version < 3:
        # v2→v3: add slots and dispatch_state tables for runtime state
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS slots (
                project             TEXT NOT NULL,
                slot_id             INTEGER NOT NULL,
                status              TEXT NOT NULL DEFAULT 'free',
                ticket_id           TEXT,
                ticket_title        TEXT,
                branch              TEXT,
                pr_url              TEXT,
                stage               TEXT,
                stage_iteration     INTEGER NOT NULL DEFAULT 0,
                current_session_id  TEXT,
                started_at          TEXT,
                stage_started_at    TEXT,
                sigterm_sent_at     TEXT,
                pid                 INTEGER,
                interrupted_by_limit INTEGER NOT NULL DEFAULT 0,
                resume_after        TEXT,
                stages_completed    TEXT,
                updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY(project, slot_id)
            );
            CREATE TABLE IF NOT EXISTS dispatch_state (
                id              INTEGER PRIMARY KEY CHECK (id = 1),
                paused          INTEGER NOT NULL DEFAULT 0,
                pause_reason    TEXT,
                updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
        """)
    if from_version < 4:
        # v3→v4: add supervisor_heartbeat to dispatch_state
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(dispatch_state)").fetchall()
        }
        if "supervisor_heartbeat" not in existing:
            conn.execute("ALTER TABLE dispatch_state ADD COLUMN supervisor_heartbeat TEXT")
    if from_version < 5:
        # v4→v5: add resets_at_7d to usage_snapshots
        existing = {
            row[1]
            for row in conn.execute("PRAGMA table_info(usage_snapshots)").fetchall()
        }
        if "resets_at_7d" not in existing:
            conn.execute("ALTER TABLE usage_snapshots ADD COLUMN resets_at_7d TEXT")
    conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))


def init_db(db_path: str | Path) -> sqlite3.Connection:
    """Open (or create) the database and ensure the schema exists.

    If the ``BOTFARM_DB_PATH`` environment variable is set, it overrides
    *db_path*.  This allows worker-spawned Claude subprocesses to use a
    per-slot sandboxed database so that schema migrations never touch the
    shared production DB.

    Returns a sqlite3.Connection with WAL mode and foreign keys enabled.
    """
    # SAFETY: This env var is only set in the Claude subprocess's environment
    # (via subprocess.run(env=...)) — never in the supervisor/worker process.
    # If it were set in the parent process, all init_db() calls would silently
    # redirect to the wrong database.
    env_override = os.environ.get("BOTFARM_DB_PATH")
    if env_override:
        db_path = env_override
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
    elif row[0] < SCHEMA_VERSION:
        _migrate(conn, row[0])
    elif row[0] > SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version mismatch: expected {SCHEMA_VERSION}, got {row[0]}. "
            "Cannot downgrade."
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
    conn.execute(
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
            failure_reason = NULL,
            pr_url = NULL,
            pipeline_stage = NULL,
            review_state = NULL
        """,
        (ticket_id, title, project, slot, status, _now_iso()),
    )
    # cursor.lastrowid is unreliable for ON CONFLICT DO UPDATE — when
    # the UPDATE branch fires it may return a stale value from a prior
    # INSERT.  Always look up the canonical id by ticket_id instead.
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
    review_iterations, comments, limit_interruptions, failure_reason,
    pr_url, pipeline_stage, review_state.
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
        "pr_url",
        "pipeline_stage",
        "review_state",
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
    rows = conn.execute("SELECT DISTINCT project FROM tasks WHERE project IS NOT NULL ORDER BY project").fetchall()
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
    resets_at_7d: str | None = None,
) -> int:
    """Insert a usage snapshot and return its id."""
    cur = conn.execute(
        """
        INSERT INTO usage_snapshots (utilization_5h, utilization_7d, resets_at, resets_at_7d, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (utilization_5h, utilization_7d, resets_at, resets_at_7d, _now_iso()),
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


# ---------------------------------------------------------------------------
# Slot runtime state helpers
# ---------------------------------------------------------------------------

def upsert_slot(conn: sqlite3.Connection, slot_data: dict) -> None:
    """Insert or update a slot runtime state row.

    Accepts a dict matching SlotState.to_dict() format.
    """
    stages = json.dumps(slot_data.get("stages_completed") or [])
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO slots (
            project, slot_id, status, ticket_id, ticket_title, branch,
            pr_url, stage, stage_iteration, current_session_id, started_at,
            stage_started_at, sigterm_sent_at, pid, interrupted_by_limit,
            resume_after, stages_completed, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(project, slot_id) DO UPDATE SET
            status=excluded.status,
            ticket_id=excluded.ticket_id,
            ticket_title=excluded.ticket_title,
            branch=excluded.branch,
            pr_url=excluded.pr_url,
            stage=excluded.stage,
            stage_iteration=excluded.stage_iteration,
            current_session_id=excluded.current_session_id,
            started_at=excluded.started_at,
            stage_started_at=excluded.stage_started_at,
            sigterm_sent_at=excluded.sigterm_sent_at,
            pid=excluded.pid,
            interrupted_by_limit=excluded.interrupted_by_limit,
            resume_after=excluded.resume_after,
            stages_completed=excluded.stages_completed,
            updated_at=excluded.updated_at
        """,
        (
            slot_data["project"],
            slot_data["slot_id"],
            slot_data.get("status", "free"),
            slot_data.get("ticket_id"),
            slot_data.get("ticket_title"),
            slot_data.get("branch"),
            slot_data.get("pr_url"),
            slot_data.get("stage"),
            slot_data.get("stage_iteration", 0),
            slot_data.get("current_session_id"),
            slot_data.get("started_at"),
            slot_data.get("stage_started_at"),
            slot_data.get("sigterm_sent_at"),
            slot_data.get("pid"),
            int(slot_data.get("interrupted_by_limit", False)),
            slot_data.get("resume_after"),
            stages,
            now,
        ),
    )


def load_all_slots(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Return all slot runtime state rows."""
    return conn.execute(
        "SELECT * FROM slots ORDER BY project, slot_id"
    ).fetchall()


def load_dispatch_state(conn: sqlite3.Connection) -> tuple[bool, str | None, str | None]:
    """Load dispatch pause state. Returns (paused, reason, supervisor_heartbeat)."""
    row = conn.execute(
        "SELECT paused, pause_reason, supervisor_heartbeat FROM dispatch_state WHERE id = 1"
    ).fetchone()
    if row is None:
        return False, None, None
    return bool(row["paused"]), row["pause_reason"], row["supervisor_heartbeat"]


def save_dispatch_state(
    conn: sqlite3.Connection,
    *,
    paused: bool,
    reason: str | None = None,
    supervisor_heartbeat: str | None = None,
) -> None:
    """Save dispatch pause state and supervisor heartbeat (single-row table)."""
    conn.execute(
        """
        INSERT INTO dispatch_state (id, paused, pause_reason, supervisor_heartbeat, updated_at)
        VALUES (1, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            paused=excluded.paused,
            pause_reason=excluded.pause_reason,
            supervisor_heartbeat=excluded.supervisor_heartbeat,
            updated_at=excluded.updated_at
        """,
        (int(paused), reason, supervisor_heartbeat, _now_iso()),
    )
