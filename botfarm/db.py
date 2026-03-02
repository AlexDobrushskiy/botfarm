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
        "started_on_extra_usage",
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
    log_file_path: str | None = None,
    on_extra_usage: bool = False,
) -> int:
    """Insert a stage run record and return its id."""
    cur = conn.execute(
        """
        INSERT INTO stage_runs
            (task_id, stage, iteration, session_id, turns,
             duration_seconds, exit_subtype, was_limit_restart,
             input_tokens, output_tokens, cache_read_input_tokens,
             cache_creation_input_tokens, total_cost_usd,
             context_fill_pct, model_usage_json, log_file_path,
             on_extra_usage, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            log_file_path,
            int(on_extra_usage),
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


def update_stage_run_context_fill(
    conn: sqlite3.Connection,
    stage_run_id: int,
    context_fill_pct: float,
) -> None:
    """Update the context_fill_pct for a stage run in-place.

    Called per-turn during streaming execution to provide real-time
    context fill data to the dashboard.
    """
    conn.execute(
        "UPDATE stage_runs SET context_fill_pct = ? WHERE id = ?",
        (context_fill_pct, stage_run_id),
    )
    conn.commit()


def delete_stage_run(conn: sqlite3.Connection, stage_run_id: int) -> None:
    """Delete a stage_run row by id.

    Used to clean up placeholder rows when a stage fails before
    producing final metrics, preventing orphaned rows with NULL data.
    """
    conn.execute("DELETE FROM stage_runs WHERE id = ?", (stage_run_id,))
    conn.commit()


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
    total_cost_usd, max_context_fill_pct, extra_usage_cost_usd}.
    """
    if not task_ids:
        return {}
    placeholders = ",".join("?" for _ in task_ids)
    rows = conn.execute(
        "SELECT task_id, "
        "SUM(input_tokens) as total_input_tokens, "
        "SUM(output_tokens) as total_output_tokens, "
        "SUM(total_cost_usd) as total_cost_usd, "
        "MAX(context_fill_pct) as max_context_fill_pct, "
        "SUM(CASE WHEN on_extra_usage THEN total_cost_usd ELSE 0 END) as extra_usage_cost_usd "
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
            "extra_usage_cost_usd": r["extra_usage_cost_usd"] or 0.0,
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
    extra_usage_enabled: bool = False,
    extra_usage_monthly_limit: float | None = None,
    extra_usage_used_credits: float | None = None,
    extra_usage_utilization: float | None = None,
) -> int:
    """Insert a usage snapshot and return its id."""
    cur = conn.execute(
        """
        INSERT INTO usage_snapshots
            (utilization_5h, utilization_7d, resets_at, resets_at_7d,
             extra_usage_enabled, extra_usage_monthly_limit,
             extra_usage_used_credits, extra_usage_utilization,
             created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            utilization_5h, utilization_7d, resets_at, resets_at_7d,
            int(extra_usage_enabled), extra_usage_monthly_limit,
            extra_usage_used_credits, extra_usage_utilization,
            _now_iso(),
        ),
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


def is_extra_usage_active(conn: sqlite3.Connection) -> bool:
    """Check whether extra usage is currently active based on the latest snapshot.

    Returns True if extra usage is enabled and either 5h or 7d utilization
    is at 100% (meaning included usage is exhausted).
    """
    row = conn.execute(
        "SELECT extra_usage_enabled, utilization_5h, utilization_7d "
        "FROM usage_snapshots ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return False
    if not row["extra_usage_enabled"]:
        return False
    u5h = row["utilization_5h"]
    u7d = row["utilization_7d"]
    return (u5h is not None and u5h >= 1.0) or (u7d is not None and u7d >= 1.0)


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
    labels = json.dumps(slot_data.get("ticket_labels") or [])
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO slots (
            project, slot_id, status, ticket_id, ticket_title, branch,
            pr_url, stage, stage_iteration, current_session_id, started_at,
            stage_started_at, sigterm_sent_at, pid, interrupted_by_limit,
            resume_after, stages_completed, ticket_labels, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(project, slot_id) DO UPDATE SET
            status=excluded.status,
            ticket_id=excluded.ticket_id,
            ticket_title=excluded.ticket_title,
            branch=excluded.branch,
            pr_url=excluded.pr_url,
            stage=COALESCE(excluded.stage, slots.stage),
            stage_iteration=excluded.stage_iteration,
            current_session_id=COALESCE(excluded.current_session_id, slots.current_session_id),
            started_at=excluded.started_at,
            stage_started_at=COALESCE(excluded.stage_started_at, slots.stage_started_at),
            sigterm_sent_at=excluded.sigterm_sent_at,
            pid=excluded.pid,
            interrupted_by_limit=excluded.interrupted_by_limit,
            resume_after=excluded.resume_after,
            stages_completed=excluded.stages_completed,
            ticket_labels=excluded.ticket_labels,
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
            labels,
            now,
        ),
    )


def clear_slot_stage(conn: sqlite3.Connection, project: str, slot_id: int) -> None:
    """Explicitly NULL out stage-related fields for a slot.

    Because upsert_slot() uses COALESCE to protect worker-written stage
    values from being overwritten, callers that legitimately need to clear
    these fields (e.g. free_slot, assign_ticket) must use this helper.
    """
    conn.execute(
        """UPDATE slots SET
            stage = NULL,
            stage_iteration = 0,
            stage_started_at = NULL,
            current_session_id = NULL,
            updated_at = ?
           WHERE project = ? AND slot_id = ?""",
        (_now_iso(), project, slot_id),
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
    """Replace queue entries for a project with fresh poll results.

    Each item in *candidates* must have identifier, title, priority,
    sort_order, and url attributes.  An optional ``blocked_by`` attribute
    (list of blocker identifiers or None) is persisted as a JSON string.
    """
    if snapshot_at is None:
        snapshot_at = _now_iso()
    conn.execute("DELETE FROM queue_entries WHERE project = ?", (project,))
    for i, issue in enumerate(candidates):
        blocked_by = getattr(issue, "blocked_by", None)
        blocked_by_json = json.dumps(blocked_by) if blocked_by else None
        conn.execute(
            "INSERT INTO queue_entries (project, position, ticket_id, ticket_title, priority, sort_order, url, snapshot_at, blocked_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (project, i, issue.identifier, issue.title, issue.priority, issue.sort_order, issue.url, snapshot_at, blocked_by_json),
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


def save_capacity_state(
    conn: sqlite3.Connection,
    *,
    issue_count: int,
    limit: int,
    by_project: dict[str, int],
    checked_at: str,
) -> None:
    """Persist Linear capacity snapshot into the dispatch_state singleton row."""
    by_project_json = json.dumps(by_project)
    conn.execute(
        """
        INSERT INTO dispatch_state (id, paused, linear_issue_count, linear_issue_limit,
                                    linear_capacity_by_project, linear_capacity_checked_at, updated_at)
        VALUES (1, 0, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            linear_issue_count=excluded.linear_issue_count,
            linear_issue_limit=excluded.linear_issue_limit,
            linear_capacity_by_project=excluded.linear_capacity_by_project,
            linear_capacity_checked_at=excluded.linear_capacity_checked_at,
            updated_at=excluded.updated_at
        """,
        (issue_count, limit, by_project_json, checked_at, _now_iso()),
    )


def load_capacity_state(
    conn: sqlite3.Connection,
) -> dict | None:
    """Load cached Linear capacity data from dispatch_state.

    Returns a dict with keys ``issue_count``, ``limit``, ``by_project``,
    and ``checked_at``, or ``None`` if no capacity data has been saved yet.
    """
    row = conn.execute(
        "SELECT linear_issue_count, linear_issue_limit, "
        "linear_capacity_by_project, linear_capacity_checked_at "
        "FROM dispatch_state WHERE id = 1"
    ).fetchone()
    if row is None or row["linear_issue_count"] is None:
        return None
    by_project_raw = row["linear_capacity_by_project"]
    return {
        "issue_count": row["linear_issue_count"],
        "limit": row["linear_issue_limit"],
        "by_project": json.loads(by_project_raw) if by_project_raw else {},
        "checked_at": row["linear_capacity_checked_at"],
    }


# ---------------------------------------------------------------------------
# Per-project pause state helpers
# ---------------------------------------------------------------------------

def load_all_project_pause_states(
    conn: sqlite3.Connection,
) -> dict[str, tuple[bool, str | None]]:
    """Load all per-project pause states.

    Returns a dict mapping project name -> (paused, reason).
    """
    rows = conn.execute(
        "SELECT project, paused, pause_reason FROM project_pause_state"
    ).fetchall()
    return {
        row["project"]: (bool(row["paused"]), row["pause_reason"])
        for row in rows
    }


def save_project_pause_state(
    conn: sqlite3.Connection,
    *,
    project: str,
    paused: bool,
    reason: str | None = None,
) -> None:
    """Save per-project pause state (upsert)."""
    conn.execute(
        """
        INSERT INTO project_pause_state (project, paused, pause_reason, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(project) DO UPDATE SET
            paused=excluded.paused,
            pause_reason=excluded.pause_reason,
            updated_at=excluded.updated_at
        """,
        (project, int(paused), reason if paused else None, _now_iso()),
    )


# ---------------------------------------------------------------------------
# Ticket history helpers
# ---------------------------------------------------------------------------

def upsert_ticket_history(conn: sqlite3.Connection, **fields: object) -> None:
    """Insert or update a ticket history entry keyed on ``ticket_id``.

    Accepts keyword arguments matching the ``ticket_history`` columns.
    The ``ticket_id`` and ``capture_source`` fields are required.
    """
    ticket_id = fields.get("ticket_id")
    if not ticket_id:
        raise ValueError("ticket_id is required for upsert_ticket_history")
    if not fields.get("capture_source"):
        raise ValueError("capture_source is required for upsert_ticket_history")

    # JSON-encode list fields
    for list_field in ("children_ids", "blocked_by", "blocks", "labels", "comments_json"):
        val = fields.get(list_field)
        if val is not None and not isinstance(val, str):
            fields[list_field] = json.dumps(val)

    allowed_columns = {
        "ticket_id", "linear_uuid", "title", "description", "status",
        "priority", "url", "assignee_name", "assignee_email", "creator_name",
        "project_name", "team_name", "estimate", "due_date", "parent_id",
        "children_ids", "blocked_by", "blocks", "labels", "comments_json",
        "pr_url", "branch_name", "linear_created_at", "linear_updated_at",
        "linear_completed_at", "captured_at", "capture_source", "raw_json",
        "deleted_from_linear",
    }
    filtered = {k: v for k, v in fields.items() if k in allowed_columns}

    if "captured_at" not in filtered:
        filtered["captured_at"] = _now_iso()

    columns = list(filtered.keys())
    placeholders = ", ".join("?" for _ in columns)
    col_names = ", ".join(columns)

    # On conflict, update all columns except ticket_id
    update_cols = [c for c in columns if c != "ticket_id"]
    update_clause = ", ".join(f"{c}=excluded.{c}" for c in update_cols)

    conn.execute(
        f"INSERT INTO ticket_history ({col_names}) VALUES ({placeholders}) "  # noqa: S608
        f"ON CONFLICT(ticket_id) DO UPDATE SET {update_clause}",
        [filtered[c] for c in columns],
    )


def get_ticket_history_entry(
    conn: sqlite3.Connection, ticket_id: str,
) -> sqlite3.Row | None:
    """Fetch a single ticket history entry by ticket identifier."""
    return conn.execute(
        "SELECT * FROM ticket_history WHERE ticket_id = ?", (ticket_id,)
    ).fetchone()


def get_ticket_history_list(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
    offset: int = 0,
    project: str | None = None,
    status: str | None = None,
    search: str | None = None,
    sort_by: str = "captured_at",
    sort_dir: str = "DESC",
) -> list[sqlite3.Row]:
    """Return ticket history entries with optional filters."""
    allowed_sort_cols = {
        "ticket_id", "title", "project_name", "status", "priority",
        "captured_at", "linear_created_at", "linear_updated_at",
    }
    if sort_by not in allowed_sort_cols:
        sort_by = "captured_at"
    if sort_dir.upper() not in ("ASC", "DESC"):
        sort_dir = "DESC"

    query = "SELECT * FROM ticket_history WHERE 1=1"
    params: list[object] = []
    if project is not None:
        query += " AND project_name = ?"
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


def count_ticket_history(
    conn: sqlite3.Connection,
    *,
    project: str | None = None,
    status: str | None = None,
    search: str | None = None,
) -> int:
    """Count ticket history entries matching the given filters."""
    query = "SELECT COUNT(*) FROM ticket_history WHERE 1=1"
    params: list[object] = []
    if project is not None:
        query += " AND project_name = ?"
        params.append(project)
    if status is not None:
        query += " AND status = ?"
        params.append(status)
    if search is not None:
        query += " AND (ticket_id LIKE ? OR title LIKE ?)"
        like = f"%{search}%"
        params.extend([like, like])
    return conn.execute(query, params).fetchone()[0]


def mark_deleted_from_linear(conn: sqlite3.Connection, ticket_id: str) -> None:
    """Mark a ticket as deleted from Linear."""
    conn.execute(
        "UPDATE ticket_history SET deleted_from_linear = 1 WHERE ticket_id = ?",
        (ticket_id,),
    )


# ---------------------------------------------------------------------------
# Cleanup batch helpers
# ---------------------------------------------------------------------------

def insert_cleanup_batch(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    action: str,
    team_key: str | None = None,
    project_name: str | None = None,
    total: int = 0,
    succeeded: int = 0,
    failed: int = 0,
    skipped: int = 0,
) -> None:
    """Insert a cleanup batch record."""
    conn.execute(
        """
        INSERT INTO cleanup_batches
            (batch_id, action, team_key, project_name, total, succeeded, failed, skipped, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (batch_id, action, team_key, project_name, total, succeeded, failed, skipped, _now_iso()),
    )


def update_cleanup_batch(
    conn: sqlite3.Connection,
    batch_id: str,
    *,
    succeeded: int,
    failed: int,
    skipped: int,
) -> None:
    """Update the result counts on a cleanup batch."""
    conn.execute(
        "UPDATE cleanup_batches SET succeeded = ?, failed = ?, skipped = ? WHERE batch_id = ?",
        (succeeded, failed, skipped, batch_id),
    )


def insert_cleanup_batch_item(
    conn: sqlite3.Connection,
    *,
    batch_id: str,
    linear_uuid: str,
    identifier: str,
    action: str,
    success: bool,
    error: str | None = None,
) -> None:
    """Insert a cleanup batch item record."""
    conn.execute(
        """
        INSERT INTO cleanup_batch_items
            (batch_id, linear_uuid, identifier, action, success, error, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (batch_id, linear_uuid, identifier, action, int(success), error, _now_iso()),
    )


def get_cleanup_batch(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
    """Fetch a cleanup batch by batch_id."""
    return conn.execute(
        "SELECT * FROM cleanup_batches WHERE batch_id = ?", (batch_id,)
    ).fetchone()


def get_cleanup_batch_items(
    conn: sqlite3.Connection, batch_id: str, *, success_only: bool = False,
) -> list[sqlite3.Row]:
    """Fetch items for a cleanup batch."""
    query = "SELECT * FROM cleanup_batch_items WHERE batch_id = ?"
    params: list[object] = [batch_id]
    if success_only:
        query += " AND success = 1"
    return conn.execute(query, params).fetchall()


def get_last_cleanup_batch_time(conn: sqlite3.Connection) -> str | None:
    """Return the created_at of the most recent cleanup batch, or None."""
    row = conn.execute(
        "SELECT created_at FROM cleanup_batches ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    return row["created_at"] if row else None


def list_cleanup_batches(
    conn: sqlite3.Connection, *, limit: int = 10,
) -> list[sqlite3.Row]:
    """Return the most recent cleanup batches, newest first."""
    return conn.execute(
        "SELECT * FROM cleanup_batches ORDER BY created_at DESC LIMIT ?",
        (limit,),
    ).fetchall()
