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

    def test_schema_version_mismatch_raises(self, tmp_path):
        db_file = tmp_path / "botfarm.db"
        c = init_db(db_file)
        c.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 99,))
        c.commit()
        c.close()
        with pytest.raises(RuntimeError, match="schema version mismatch"):
            init_db(db_file)


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
