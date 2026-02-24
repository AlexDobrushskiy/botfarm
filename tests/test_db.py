"""Tests for botfarm.db — schema creation and helper functions."""

from __future__ import annotations

import sqlite3

import pytest

from botfarm.db import (
    SCHEMA_VERSION,
    count_tasks,
    get_distinct_projects,
    get_events,
    get_stage_runs,
    get_task,
    get_task_by_ticket,
    get_task_history,
    get_usage_snapshots,
    init_db,
    insert_event,
    insert_stage_run,
    insert_task,
    insert_usage_snapshot,
    load_all_slots,
    load_dispatch_state,
    save_dispatch_state,
    update_task,
    upsert_slot,
)


@pytest.fixture()
def conn(tmp_path):
    """Yield a fresh in-memory-style DB connection using a temp file."""
    db_file = tmp_path / "test.db"
    connection = init_db(db_file)
    yield connection
    connection.close()


# ---------------------------------------------------------------------------
# Schema / init
# ---------------------------------------------------------------------------


class TestInitDb:
    def test_creates_file_and_tables(self, tmp_path):
        db_file = tmp_path / "sub" / "botfarm.db"
        c = init_db(db_file)
        assert db_file.exists()

        tables = {
            row[0]
            for row in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert tables >= {"tasks", "stage_runs", "usage_snapshots", "task_events", "schema_version", "slots", "dispatch_state"}
        c.close()

    def test_wal_mode_enabled(self, conn):
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_foreign_keys_enabled(self, conn):
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1

    def test_schema_version_recorded(self, conn):
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

    def test_idempotent_init(self, tmp_path):
        db_file = tmp_path / "botfarm.db"
        c1 = init_db(db_file)
        c1.close()
        c2 = init_db(db_file)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION
        c2.close()

    def test_schema_version_future_raises(self, tmp_path):
        db_file = tmp_path / "botfarm.db"
        c = init_db(db_file)
        c.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 99,))
        c.commit()
        c.close()
        with pytest.raises(RuntimeError, match="Cannot downgrade"):
            init_db(db_file)

    def test_botfarm_db_path_env_overrides(self, tmp_path, monkeypatch):
        """When BOTFARM_DB_PATH is set, init_db uses that path instead of the provided one."""
        real_db = tmp_path / "real.db"
        override_db = tmp_path / "override" / "botfarm.db"
        monkeypatch.setenv("BOTFARM_DB_PATH", str(override_db))

        c = init_db(real_db)
        c.close()

        assert override_db.exists()
        assert not real_db.exists()

    def test_botfarm_db_path_env_not_set(self, tmp_path, monkeypatch):
        """Without BOTFARM_DB_PATH, init_db uses the provided path."""
        monkeypatch.delenv("BOTFARM_DB_PATH", raising=False)
        db_file = tmp_path / "normal.db"
        c = init_db(db_file)
        c.close()
        assert db_file.exists()

    def test_migration_v1_to_v2(self, tmp_path):
        """Simulate a v1 database and verify migration adds new columns."""
        db_file = tmp_path / "botfarm.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        # Create v1 schema (without pr_url, pipeline_stage, review_state)
        conn.executescript("""
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                stage TEXT NOT NULL,
                iteration INTEGER NOT NULL DEFAULT 1,
                session_id TEXT,
                turns INTEGER NOT NULL DEFAULT 0,
                duration_seconds REAL,
                cost_usd REAL NOT NULL DEFAULT 0.0,
                exit_subtype TEXT,
                was_limit_restart INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS usage_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utilization_5h REAL,
                utilization_7d REAL,
                resets_at TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER REFERENCES tasks(id),
                event_type TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
        """)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        conn.execute("INSERT INTO schema_version (version) VALUES (1)")
        # Insert a task to verify data survives migration
        conn.execute(
            "INSERT INTO tasks (ticket_id, title, project, slot) VALUES (?, ?, ?, ?)",
            ("SMA-1", "Old task", "proj", 1),
        )
        conn.commit()
        conn.close()

        # Re-open via init_db — should migrate
        c2 = init_db(db_file)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

        # New columns should exist and be NULL for pre-existing rows
        task = c2.execute("SELECT pr_url, pipeline_stage, review_state FROM tasks WHERE ticket_id = 'SMA-1'").fetchone()
        assert task[0] is None
        assert task[1] is None
        assert task[2] is None

        # Should be able to update new columns
        c2.execute("UPDATE tasks SET pr_url = 'https://example.com/pr/1' WHERE ticket_id = 'SMA-1'")
        c2.commit()
        task = c2.execute("SELECT pr_url FROM tasks WHERE ticket_id = 'SMA-1'").fetchone()
        assert task[0] == "https://example.com/pr/1"
        c2.close()

    def test_migration_v2_to_v3(self, tmp_path):
        """Simulate a v2 database and verify migration adds slots and dispatch_state tables."""
        db_file = tmp_path / "botfarm.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        # Create v2 schema (without slots and dispatch_state)
        conn.executescript("""
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                stage TEXT NOT NULL,
                iteration INTEGER NOT NULL DEFAULT 1,
                session_id TEXT,
                turns INTEGER NOT NULL DEFAULT 0,
                duration_seconds REAL,
                cost_usd REAL NOT NULL DEFAULT 0.0,
                exit_subtype TEXT,
                was_limit_restart INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS usage_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utilization_5h REAL,
                utilization_7d REAL,
                resets_at TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER REFERENCES tasks(id),
                event_type TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
        """)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        conn.execute("INSERT INTO schema_version (version) VALUES (2)")
        conn.commit()
        conn.close()

        # Re-open via init_db — should migrate to v3
        c2 = init_db(db_file)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

        # New tables should exist
        tables = {
            r[0]
            for r in c2.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "slots" in tables
        assert "dispatch_state" in tables

        # Should be able to insert into new tables
        c2.execute(
            "INSERT INTO slots (project, slot_id, status) VALUES (?, ?, ?)",
            ("proj", 1, "free"),
        )
        c2.execute(
            "INSERT INTO dispatch_state (id, paused) VALUES (1, 0)"
        )
        c2.commit()
        c2.close()


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


class TestTasks:
    def test_insert_and_get(self, conn):
        tid = insert_task(
            conn, ticket_id="SMA-1", title="Test task", project="proj", slot=1
        )
        assert tid is not None
        row = get_task(conn, tid)
        assert row["ticket_id"] == "SMA-1"
        assert row["title"] == "Test task"
        assert row["project"] == "proj"
        assert row["slot"] == 1
        assert row["status"] == "pending"

    def test_get_task_by_ticket(self, conn):
        insert_task(
            conn, ticket_id="SMA-2", title="Another", project="proj", slot=2
        )
        row = get_task_by_ticket(conn, "SMA-2")
        assert row is not None
        assert row["title"] == "Another"

    def test_get_task_by_ticket_missing(self, conn):
        assert get_task_by_ticket(conn, "NOPE-999") is None

    def test_duplicate_ticket_id_upserts(self, conn):
        tid1 = insert_task(conn, ticket_id="SMA-3", title="First", project="p", slot=1)
        update_task(conn, tid1, status="failed", failure_reason="old failure")
        conn.commit()

        # Re-inserting same ticket_id resets the row for retry
        tid2 = insert_task(conn, ticket_id="SMA-3", title="Retry", project="p", slot=2)
        assert tid2 == tid1  # Same row reused

        row = get_task(conn, tid2)
        assert row["title"] == "Retry"
        assert row["slot"] == 2
        assert row["status"] == "pending"
        assert row["failure_reason"] is None

    def test_insert_task_upsert_then_insert_event(self, conn):
        """Regression: insert_event after upsert must not cause FK violation.

        When insert_task hits ON CONFLICT DO UPDATE, cursor.lastrowid may
        return a stale value from a prior INSERT.  The returned task_id must
        still be valid for foreign-key references in task_events.
        """
        # First insert — creates row with id=N
        tid1 = insert_task(conn, ticket_id="SMA-FK", title="First", project="p", slot=1)
        insert_event(conn, task_id=tid1, event_type="worker_dispatched", detail="first")
        update_task(conn, tid1, status="failed", failure_reason="boom")
        conn.commit()

        # Upsert — should return the same id
        tid2 = insert_task(conn, ticket_id="SMA-FK", title="Retry", project="p", slot=1)
        conn.commit()

        assert tid2 == tid1

        # This must not raise IntegrityError
        insert_event(conn, task_id=tid2, event_type="worker_dispatched", detail="retry")
        conn.commit()

        events = get_events(conn, task_id=tid2, event_type="worker_dispatched")
        assert len(events) == 2

    def test_update_task(self, conn):
        tid = insert_task(
            conn, ticket_id="SMA-4", title="Upd", project="p", slot=1
        )
        update_task(conn, tid, status="in_progress", turns=5)
        row = get_task(conn, tid)
        assert row["status"] == "in_progress"
        assert row["turns"] == 5

    def test_update_task_new_columns(self, conn):
        tid = insert_task(
            conn, ticket_id="SMA-NC", title="New cols", project="p", slot=1
        )
        update_task(conn, tid, pr_url="https://github.com/o/r/pull/1")
        update_task(conn, tid, pipeline_stage="review")
        update_task(conn, tid, review_state="approved")
        row = get_task(conn, tid)
        assert row["pr_url"] == "https://github.com/o/r/pull/1"
        assert row["pipeline_stage"] == "review"
        assert row["review_state"] == "approved"

    def test_new_columns_null_by_default(self, conn):
        tid = insert_task(
            conn, ticket_id="SMA-NULL", title="Null cols", project="p", slot=1
        )
        row = get_task(conn, tid)
        assert row["pr_url"] is None
        assert row["pipeline_stage"] is None
        assert row["review_state"] is None

    def test_update_task_disallowed_column(self, conn):
        tid = insert_task(
            conn, ticket_id="SMA-5", title="Bad", project="p", slot=1
        )
        with pytest.raises(ValueError, match="disallowed"):
            update_task(conn, tid, ticket_id="SMA-HACK")

    def test_update_task_no_fields(self, conn):
        tid = insert_task(
            conn, ticket_id="SMA-6", title="Noop", project="p", slot=1
        )
        update_task(conn, tid)  # no-op, should not raise

    def test_get_task_history(self, conn):
        for i in range(5):
            insert_task(
                conn,
                ticket_id=f"H-{i}",
                title=f"Task {i}",
                project="alpha" if i % 2 == 0 else "beta",
                slot=i,
            )

        all_tasks = get_task_history(conn)
        assert len(all_tasks) == 5

        alpha = get_task_history(conn, project="alpha")
        assert len(alpha) == 3
        assert all(r["project"] == "alpha" for r in alpha)

    def test_get_task_history_with_status_filter(self, conn):
        tid = insert_task(
            conn, ticket_id="S-1", title="Done", project="p", slot=1, status="completed"
        )
        insert_task(
            conn, ticket_id="S-2", title="Pend", project="p", slot=2
        )
        completed = get_task_history(conn, status="completed")
        assert len(completed) == 1
        assert completed[0]["id"] == tid

    def test_get_task_history_limit(self, conn):
        for i in range(10):
            insert_task(
                conn, ticket_id=f"L-{i}", title=f"T{i}", project="p", slot=i
            )
        limited = get_task_history(conn, limit=3)
        assert len(limited) == 3

    def test_get_task_history_offset(self, conn):
        for i in range(5):
            insert_task(
                conn, ticket_id=f"O-{i}", title=f"Task {i}", project="p", slot=i
            )
        page1 = get_task_history(conn, limit=2, offset=0)
        page2 = get_task_history(conn, limit=2, offset=2)
        assert len(page1) == 2
        assert len(page2) == 2
        assert page1[0]["ticket_id"] != page2[0]["ticket_id"]

    def test_get_task_history_search(self, conn):
        insert_task(conn, ticket_id="SRCH-1", title="Fix login bug", project="p", slot=1)
        insert_task(conn, ticket_id="SRCH-2", title="Add feature", project="p", slot=2)
        results = get_task_history(conn, search="login")
        assert len(results) == 1
        assert results[0]["ticket_id"] == "SRCH-1"

    def test_get_task_history_search_by_ticket(self, conn):
        insert_task(conn, ticket_id="FIND-42", title="Something", project="p", slot=1)
        insert_task(conn, ticket_id="OTHER-1", title="Other", project="p", slot=2)
        results = get_task_history(conn, search="FIND-42")
        assert len(results) == 1
        assert results[0]["ticket_id"] == "FIND-42"

    def test_get_task_history_sort(self, conn):
        insert_task(conn, ticket_id="SORT-A", title="A", project="p", slot=1, status="completed")
        update_task(conn, 1, cost_usd=10.0)
        insert_task(conn, ticket_id="SORT-B", title="B", project="p", slot=2, status="completed")
        update_task(conn, 2, cost_usd=1.0)
        asc = get_task_history(conn, sort_by="cost_usd", sort_dir="ASC")
        assert asc[0]["cost_usd"] <= asc[1]["cost_usd"]

    def test_get_task_history_invalid_sort_defaults(self, conn):
        insert_task(conn, ticket_id="DEF-1", title="T", project="p", slot=1)
        # Should not raise on invalid sort column
        results = get_task_history(conn, sort_by="DROP TABLE tasks")
        assert len(results) == 1

    def test_count_tasks(self, conn):
        for i in range(5):
            insert_task(
                conn, ticket_id=f"C-{i}", title=f"T{i}",
                project="a" if i < 3 else "b", slot=i,
            )
        assert count_tasks(conn) == 5
        assert count_tasks(conn, project="a") == 3
        assert count_tasks(conn, project="b") == 2

    def test_count_tasks_with_search(self, conn):
        insert_task(conn, ticket_id="CS-1", title="Fix bug", project="p", slot=1)
        insert_task(conn, ticket_id="CS-2", title="Add test", project="p", slot=2)
        assert count_tasks(conn, search="bug") == 1

    def test_get_distinct_projects(self, conn):
        insert_task(conn, ticket_id="DP-1", title="T1", project="alpha", slot=1)
        insert_task(conn, ticket_id="DP-2", title="T2", project="beta", slot=2)
        insert_task(conn, ticket_id="DP-3", title="T3", project="alpha", slot=3)
        projects = get_distinct_projects(conn)
        assert projects == ["alpha", "beta"]


# ---------------------------------------------------------------------------
# Stage runs
# ---------------------------------------------------------------------------


class TestStageRuns:
    def test_insert_and_get(self, conn):
        tid = insert_task(
            conn, ticket_id="SR-1", title="SR test", project="p", slot=1
        )
        sr_id = insert_stage_run(
            conn,
            task_id=tid,
            stage="implement",
            session_id="sess-abc",
            turns=10,
            duration_seconds=120.5,
            cost_usd=0.42,
        )
        runs = get_stage_runs(conn, tid)
        assert len(runs) == 1
        assert runs[0]["id"] == sr_id
        assert runs[0]["stage"] == "implement"
        assert runs[0]["turns"] == 10
        assert runs[0]["duration_seconds"] == pytest.approx(120.5)
        assert runs[0]["was_limit_restart"] == 0

    def test_was_limit_restart_flag(self, conn):
        tid = insert_task(
            conn, ticket_id="SR-2", title="LR", project="p", slot=1
        )
        insert_stage_run(
            conn, task_id=tid, stage="review", was_limit_restart=True
        )
        runs = get_stage_runs(conn, tid)
        assert runs[0]["was_limit_restart"] == 1

    def test_foreign_key_enforcement(self, conn):
        with pytest.raises(sqlite3.IntegrityError):
            insert_stage_run(conn, task_id=99999, stage="implement")


# ---------------------------------------------------------------------------
# Usage snapshots
# ---------------------------------------------------------------------------


class TestUsageSnapshots:
    def test_insert_and_get(self, conn):
        sid = insert_usage_snapshot(
            conn, utilization_5h=0.73, utilization_7d=0.45, resets_at="2026-02-13T00:00:00Z"
        )
        rows = get_usage_snapshots(conn)
        assert len(rows) == 1
        assert rows[0]["id"] == sid
        assert rows[0]["utilization_5h"] == pytest.approx(0.73)

    def test_nullable_fields(self, conn):
        insert_usage_snapshot(conn)
        rows = get_usage_snapshots(conn)
        assert rows[0]["utilization_5h"] is None
        assert rows[0]["utilization_7d"] is None
        assert rows[0]["resets_at"] is None

    def test_limit(self, conn):
        for _ in range(5):
            insert_usage_snapshot(conn, utilization_5h=0.5)
        rows = get_usage_snapshots(conn, limit=2)
        assert len(rows) == 2


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


class TestEvents:
    def test_insert_and_get(self, conn):
        tid = insert_task(
            conn, ticket_id="EV-1", title="Ev test", project="p", slot=1
        )
        eid = insert_event(
            conn, task_id=tid, event_type="stage_started", detail="implement"
        )
        events = get_events(conn, task_id=tid)
        assert len(events) == 1
        assert events[0]["id"] == eid
        assert events[0]["event_type"] == "stage_started"

    def test_event_without_task(self, conn):
        eid = insert_event(conn, event_type="limit_hit", detail="5h at 95%")
        events = get_events(conn)
        assert len(events) == 1
        assert events[0]["id"] == eid
        assert events[0]["task_id"] is None

    def test_filter_by_event_type(self, conn):
        tid = insert_task(
            conn, ticket_id="EV-2", title="Filt", project="p", slot=1
        )
        insert_event(conn, task_id=tid, event_type="stage_started")
        insert_event(conn, task_id=tid, event_type="stage_completed")
        insert_event(conn, task_id=tid, event_type="stage_started")

        started = get_events(conn, event_type="stage_started")
        assert len(started) == 2

    def test_event_limit(self, conn):
        for i in range(10):
            insert_event(conn, event_type="tick", detail=str(i))
        events = get_events(conn, limit=3)
        assert len(events) == 3


# ---------------------------------------------------------------------------
# Slot runtime state
# ---------------------------------------------------------------------------


class TestSlotRuntimeState:
    def test_upsert_and_load(self, conn):
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "ticket_id": "SMA-1", "stages_completed": ["implement"],
        })
        conn.commit()
        rows = load_all_slots(conn)
        assert len(rows) == 1
        assert rows[0]["project"] == "proj"
        assert rows[0]["slot_id"] == 1
        assert rows[0]["status"] == "busy"
        assert rows[0]["ticket_id"] == "SMA-1"

    def test_upsert_updates_existing(self, conn):
        upsert_slot(conn, {"project": "proj", "slot_id": 1, "status": "free"})
        conn.commit()
        upsert_slot(conn, {"project": "proj", "slot_id": 1, "status": "busy", "ticket_id": "T-1"})
        conn.commit()
        rows = load_all_slots(conn)
        assert len(rows) == 1
        assert rows[0]["status"] == "busy"
        assert rows[0]["ticket_id"] == "T-1"

    def test_stages_completed_serialized(self, conn):
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1,
            "stages_completed": ["implement", "review"],
        })
        conn.commit()
        rows = load_all_slots(conn)
        import json
        stages = json.loads(rows[0]["stages_completed"])
        assert stages == ["implement", "review"]

    def test_interrupted_by_limit_stored_as_int(self, conn):
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "interrupted_by_limit": True,
        })
        conn.commit()
        rows = load_all_slots(conn)
        assert rows[0]["interrupted_by_limit"] == 1

    def test_multiple_slots(self, conn):
        upsert_slot(conn, {"project": "alpha", "slot_id": 1})
        upsert_slot(conn, {"project": "alpha", "slot_id": 2})
        upsert_slot(conn, {"project": "beta", "slot_id": 1})
        conn.commit()
        rows = load_all_slots(conn)
        assert len(rows) == 3


# ---------------------------------------------------------------------------
# Dispatch state
# ---------------------------------------------------------------------------


class TestDispatchState:
    def test_save_and_load(self, conn):
        save_dispatch_state(conn, paused=True, reason="limit hit")
        conn.commit()
        paused, reason, _heartbeat = load_dispatch_state(conn)
        assert paused is True
        assert reason == "limit hit"

    def test_update_existing(self, conn):
        save_dispatch_state(conn, paused=True, reason="first")
        conn.commit()
        save_dispatch_state(conn, paused=False)
        conn.commit()
        paused, reason, _heartbeat = load_dispatch_state(conn)
        assert paused is False
        assert reason is None

    def test_defaults_when_empty(self, conn):
        paused, reason, _heartbeat = load_dispatch_state(conn)
        assert paused is False
        assert reason is None
