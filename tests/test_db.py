"""Tests for botfarm.db — schema creation and helper functions."""

from __future__ import annotations

import sqlite3

import pytest

from botfarm.db import (
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
    update_task,
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
        assert tables >= {"tasks", "stage_runs", "usage_snapshots", "task_events", "schema_version"}
        c.close()

    def test_wal_mode_enabled(self, conn):
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"

    def test_foreign_keys_enabled(self, conn):
        fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        assert fk == 1

    def test_schema_version_recorded(self, conn):
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == 1

    def test_idempotent_init(self, tmp_path):
        db_file = tmp_path / "botfarm.db"
        c1 = init_db(db_file)
        c1.close()
        c2 = init_db(db_file)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == 1
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

    def test_unique_ticket_id(self, conn):
        insert_task(conn, ticket_id="SMA-3", title="First", project="p", slot=1)
        with pytest.raises(sqlite3.IntegrityError):
            insert_task(conn, ticket_id="SMA-3", title="Dup", project="p", slot=2)

    def test_update_task(self, conn):
        tid = insert_task(
            conn, ticket_id="SMA-4", title="Upd", project="p", slot=1
        )
        update_task(conn, tid, status="in_progress", turns=5)
        row = get_task(conn, tid)
        assert row["status"] == "in_progress"
        assert row["turns"] == 5

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
