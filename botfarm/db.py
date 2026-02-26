"""SQLite database for task history, stage runs, usage snapshots, event logs, and slot runtime state."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _discover_migrations() -> dict[int, Path]:
    """Glob migration SQL files and return {version: path} sorted by version."""
    migrations: dict[int, Path] = {}
    for sql_file in sorted(MIGRATIONS_DIR.glob("*.sql")):
        match = re.match(r"^(\d+)", sql_file.name)
        if match:
            migrations[int(match.group(1))] = sql_file
    return dict(sorted(migrations.items()))


def get_schema_version() -> int:
    """Return the highest migration number found on disk."""
    migrations = _discover_migrations()
    if not migrations:
        raise RuntimeError("No migration files found in %s" % MIGRATIONS_DIR)
    return max(migrations.keys())


SCHEMA_VERSION = get_schema_version()


class SchemaVersionError(RuntimeError):
    """Raised when the database schema version does not match the expected version."""


def resolve_db_path() -> Path:
    """Resolve the database path from the ``BOTFARM_DB_PATH`` environment variable.

    Every process (supervisor, worker, CLI) must call this to obtain the
    authoritative database path.  The variable is typically loaded from a
    ``.env`` file in the working directory via ``python-dotenv``.

    Raises ``RuntimeError`` if the variable is not set or is empty.
    """
    db_path = os.environ.get("BOTFARM_DB_PATH")
    if not db_path:
        raise RuntimeError(
            "BOTFARM_DB_PATH environment variable is not set. "
            "Add it to your .env file (e.g. BOTFARM_DB_PATH=~/.botfarm/botfarm.db)."
        )
    return Path(db_path).expanduser()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _run_migrations(conn: sqlite3.Connection, from_version: int) -> None:
    """Run migration SQL files from *from_version* up to SCHEMA_VERSION.

    Each migration is executed and committed individually so that a crash
    mid-upgrade leaves the database at the last successful migration.
    """
    migrations = _discover_migrations()
    for version, sql_path in migrations.items():
        if version <= from_version:
            continue
        sql = sql_path.read_text()
        conn.executescript(sql)
        conn.execute("UPDATE schema_version SET version = ?", (version,))
        conn.commit()


def init_db(
    db_path: str | Path,
    *,
    allow_migration: bool = False,
) -> sqlite3.Connection:
    """Open (or create) the database and ensure the schema exists.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.
    allow_migration:
        When ``True``, automatically migrate an older schema to the
        current version.  When ``False`` (default), a version mismatch
        raises ``SchemaVersionError`` so that migrations are never
        applied silently.  The supervisor passes ``True``; CLI commands
        and worker subprocesses should leave it at the default.

    Returns a sqlite3.Connection with WAL mode and foreign keys enabled.
    """
    db_path = Path(db_path).expanduser()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
    )
    row = conn.execute("SELECT version FROM schema_version").fetchone()

    if row is None:
        # Fresh database — run all migrations from scratch
        conn.execute("INSERT INTO schema_version (version) VALUES (0)")
        conn.commit()
        _run_migrations(conn, 0)
    elif row[0] < SCHEMA_VERSION:
        if not allow_migration:
            raise SchemaVersionError(
                f"Database schema version ({row[0]}) is older than expected "
                f"({SCHEMA_VERSION}). Run `botfarm run` "
                f"(the supervisor will migrate automatically)."
            )
        _run_migrations(conn, row[0])
    elif row[0] > SCHEMA_VERSION:
        raise SchemaVersionError(
            f"Database schema version mismatch: expected {SCHEMA_VERSION}, got {row[0]}. "
            "Cannot downgrade."
        )

    # executescript() implicitly commits and can reset pragmas — re-apply
    conn.execute("PRAGMA foreign_keys=ON")
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
    Allowed columns: status, started_at, completed_at, turns,
    review_iterations, comments, limit_interruptions, failure_reason,
    pr_url, pipeline_stage, review_state.
    """
    allowed = {
        "status",
        "started_at",
        "completed_at",
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
        "ticket_id", "title", "project", "status", "turns",
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
    exit_subtype: str | None = None,
    was_limit_restart: bool = False,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cache_read_input_tokens: int = 0,
    cache_creation_input_tokens: int = 0,
    total_cost_usd: float = 0.0,
    context_fill_pct: float | None = None,
    model_usage_json: str | None = None,
) -> int:
    """Insert a stage run record and return its id."""
    cur = conn.execute(
        """
        INSERT INTO stage_runs
            (task_id, stage, iteration, session_id, turns,
             duration_seconds, exit_subtype, was_limit_restart,
             input_tokens, output_tokens, cache_read_input_tokens,
             cache_creation_input_tokens, total_cost_usd,
             context_fill_pct, model_usage_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            stage,
            iteration,
            session_id,
            turns,
            duration_seconds,
            exit_subtype,
            int(was_limit_restart),
            input_tokens,
            output_tokens,
            cache_read_input_tokens,
            cache_creation_input_tokens,
            total_cost_usd,
            context_fill_pct,
            model_usage_json,
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


def get_latest_context_fill_by_ticket(
    conn: sqlite3.Connection, ticket_ids: list[str],
) -> dict[str, float | None]:
    """Return the most recent context_fill_pct for each ticket.

    Used to display context fill on the live status page for busy slots.
    """
    if not ticket_ids:
        return {}
    placeholders = ",".join("?" for _ in ticket_ids)
    rows = conn.execute(
        "SELECT t.ticket_id, sr.context_fill_pct "
        "FROM stage_runs sr "
        "JOIN tasks t ON sr.task_id = t.id "
        f"WHERE t.ticket_id IN ({placeholders}) "
        "AND sr.context_fill_pct IS NOT NULL "
        "ORDER BY sr.created_at DESC",
        ticket_ids,
    ).fetchall()
    # Take the first (most recent) per ticket
    result: dict[str, float | None] = {}
    for r in rows:
        tid = r["ticket_id"]
        if tid not in result:
            result[tid] = r["context_fill_pct"]
    return result


def get_stage_run_aggregates(
    conn: sqlite3.Connection, task_ids: list[int],
) -> dict[int, dict]:
    """Return aggregated token usage per task for a batch of task IDs.

    Returns a dict mapping task_id -> {total_input_tokens, total_output_tokens,
    total_cost_usd, max_context_fill_pct}.
    """
    if not task_ids:
        return {}
    placeholders = ",".join("?" for _ in task_ids)
    rows = conn.execute(
        "SELECT task_id, "
        "SUM(input_tokens) as total_input_tokens, "
        "SUM(output_tokens) as total_output_tokens, "
        "SUM(total_cost_usd) as total_cost_usd, "
        "MAX(context_fill_pct) as max_context_fill_pct "
        f"FROM stage_runs WHERE task_id IN ({placeholders}) "
        "GROUP BY task_id",
        task_ids,
    ).fetchall()
    return {
        r["task_id"]: {
            "total_input_tokens": r["total_input_tokens"] or 0,
            "total_output_tokens": r["total_output_tokens"] or 0,
            "total_cost_usd": r["total_cost_usd"] or 0.0,
            "max_context_fill_pct": r["max_context_fill_pct"],
        }
        for r in rows
    }


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


def save_queue_entries(conn: sqlite3.Connection, project: str, candidates: list, snapshot_at: str | None = None) -> None:
    """Replace queue entries for a project with fresh poll results."""
    if snapshot_at is None:
        snapshot_at = _now_iso()
    conn.execute("DELETE FROM queue_entries WHERE project = ?", (project,))
    for i, issue in enumerate(candidates):
        conn.execute(
            "INSERT INTO queue_entries (project, position, ticket_id, ticket_title, priority, sort_order, url, snapshot_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (project, i, issue.identifier, issue.title, issue.priority, issue.sort_order, issue.url, snapshot_at),
        )
    conn.commit()


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
