"""Tests for tests/seed_test_db.py — comprehensive DB seed script."""

from __future__ import annotations

import json
import sqlite3

import pytest

from botfarm.db import init_db
from tests.seed_test_db import seed_comprehensive_db


@pytest.fixture()
def seeded_db(tmp_path):
    """Create and seed a comprehensive test database, return (path, connection)."""
    path = tmp_path / "test.db"
    seed_comprehensive_db(path)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    yield path, conn
    conn.close()


# ---------------------------------------------------------------------------
# Schema & idempotency
# ---------------------------------------------------------------------------


class TestSeedBasics:
    def test_schema_version_matches(self, seeded_db):
        _, conn = seeded_db
        from botfarm.db import SCHEMA_VERSION
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        assert version == SCHEMA_VERSION

    def test_idempotent_rerun(self, tmp_path):
        """Seeding twice on the same path produces identical row counts."""
        path = tmp_path / "test.db"
        seed_comprehensive_db(path)
        conn = sqlite3.connect(str(path))
        counts_before = {
            t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]  # noqa: S608
            for t in ["slots", "tasks", "stage_runs", "task_events", "usage_snapshots", "queue_entries"]
        }
        conn.close()

        seed_comprehensive_db(path)
        conn = sqlite3.connect(str(path))
        for table, expected in counts_before.items():
            actual = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            assert actual == expected, f"{table}: expected {expected}, got {actual}"
        conn.close()


# ---------------------------------------------------------------------------
# Slots — all statuses across multiple projects
# ---------------------------------------------------------------------------


class TestSlots:
    def test_slot_count(self, seeded_db):
        _, conn = seeded_db
        count = conn.execute("SELECT COUNT(*) FROM slots").fetchone()[0]
        assert count == 8

    def test_all_statuses_present(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute("SELECT DISTINCT status FROM slots ORDER BY status").fetchall()
        statuses = {r[0] for r in rows}
        assert statuses == {
            "free", "busy", "failed", "paused_manual",
            "paused_limit", "completed_pending_cleanup",
        }

    def test_multiple_projects(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute("SELECT DISTINCT project FROM slots ORDER BY project").fetchall()
        projects = [r[0] for r in rows]
        assert len(projects) >= 3

    def test_busy_slot_has_ticket(self, seeded_db):
        _, conn = seeded_db
        busy = conn.execute(
            "SELECT * FROM slots WHERE status = 'busy' LIMIT 1"
        ).fetchone()
        assert busy["ticket_id"] is not None
        assert busy["stage"] is not None
        assert busy["pid"] is not None

    def test_paused_manual_slot_has_resume_after(self, seeded_db):
        _, conn = seeded_db
        paused = conn.execute(
            "SELECT * FROM slots WHERE status = 'paused_manual' LIMIT 1"
        ).fetchone()
        assert paused["resume_after"] is not None

    def test_paused_limit_slot_has_interrupted_by_limit(self, seeded_db):
        _, conn = seeded_db
        paused = conn.execute(
            "SELECT * FROM slots WHERE status = 'paused_limit' LIMIT 1"
        ).fetchone()
        assert paused is not None
        assert paused["interrupted_by_limit"]

    def test_completed_pending_cleanup_slot(self, seeded_db):
        _, conn = seeded_db
        slot = conn.execute(
            "SELECT * FROM slots WHERE status = 'completed_pending_cleanup' LIMIT 1"
        ).fetchone()
        assert slot is not None
        assert slot["ticket_id"] is not None
        assert slot["pr_url"] is not None

    def test_slot_has_ticket_labels(self, seeded_db):
        _, conn = seeded_db
        busy = conn.execute(
            "SELECT ticket_labels FROM slots WHERE status = 'busy' LIMIT 1"
        ).fetchone()
        labels = json.loads(busy["ticket_labels"])
        assert isinstance(labels, list)
        assert len(labels) > 0


# ---------------------------------------------------------------------------
# Tasks — completed, failed, pending with varied attributes
# ---------------------------------------------------------------------------


class TestTasks:
    def test_task_count(self, seeded_db):
        _, conn = seeded_db
        count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        assert count == 10

    def test_all_statuses_present(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute("SELECT DISTINCT status FROM tasks ORDER BY status").fetchall()
        statuses = {r[0] for r in rows}
        assert statuses == {"completed", "failed", "pending"}

    def test_multiple_projects(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute("SELECT DISTINCT project FROM tasks ORDER BY project").fetchall()
        projects = [r[0] for r in rows]
        assert len(projects) >= 3

    def test_completed_task_has_pr_url(self, seeded_db):
        _, conn = seeded_db
        completed = conn.execute(
            "SELECT * FROM tasks WHERE status = 'completed' AND pr_url IS NOT NULL"
        ).fetchall()
        assert len(completed) >= 1

    def test_failed_task_has_failure_reason(self, seeded_db):
        _, conn = seeded_db
        failed = conn.execute(
            "SELECT * FROM tasks WHERE status = 'failed' AND failure_reason IS NOT NULL"
        ).fetchall()
        assert len(failed) >= 1

    def test_varied_turn_counts(self, seeded_db):
        _, conn = seeded_db
        turns = conn.execute("SELECT DISTINCT turns FROM tasks WHERE turns > 0").fetchall()
        assert len(turns) >= 3

    def test_varied_review_iterations(self, seeded_db):
        _, conn = seeded_db
        iters = conn.execute(
            "SELECT DISTINCT review_iterations FROM tasks"
        ).fetchall()
        values = {r[0] for r in iters}
        assert len(values) >= 3  # 0, 1, 2, 3

    def test_task_with_limit_interruptions(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT * FROM tasks WHERE limit_interruptions > 0"
        ).fetchone()
        assert row is not None
        assert row["limit_interruptions"] >= 1

    def test_task_started_on_extra_usage(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT * FROM tasks WHERE started_on_extra_usage = 1"
        ).fetchone()
        assert row is not None


# ---------------------------------------------------------------------------
# Stage runs — all 5 stages, varied durations/tokens/costs/exit subtypes
# ---------------------------------------------------------------------------


class TestStageRuns:
    def test_stage_run_count(self, seeded_db):
        _, conn = seeded_db
        count = conn.execute("SELECT COUNT(*) FROM stage_runs").fetchone()[0]
        assert count == 37

    def test_all_five_stages_present(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute("SELECT DISTINCT stage FROM stage_runs ORDER BY stage").fetchall()
        stages = {r[0] for r in rows}
        assert stages == {"implement", "review", "fix", "pr_checks", "merge"}

    def test_varied_exit_subtypes(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute(
            "SELECT DISTINCT exit_subtype FROM stage_runs WHERE exit_subtype IS NOT NULL"
        ).fetchall()
        subtypes = {r[0] for r in rows}
        assert len(subtypes) >= 5

    def test_varied_durations(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute(
            "SELECT MIN(duration_seconds), MAX(duration_seconds) FROM stage_runs"
        ).fetchone()
        assert rows[1] > rows[0] * 10  # At least 10x range

    def test_token_counts_populated(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT SUM(input_tokens), SUM(output_tokens) FROM stage_runs"
        ).fetchone()
        assert row[0] > 0
        assert row[1] > 0

    def test_costs_populated(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT SUM(total_cost_usd) FROM stage_runs"
        ).fetchone()
        assert row[0] > 0

    def test_context_fill_varied(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute(
            "SELECT MIN(context_fill_pct), MAX(context_fill_pct) FROM stage_runs "
            "WHERE context_fill_pct IS NOT NULL"
        ).fetchone()
        assert rows[0] < 20  # Low end
        assert rows[1] > 90  # High end

    def test_limit_restart_present(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT COUNT(*) FROM stage_runs WHERE was_limit_restart = 1"
        ).fetchone()
        assert row[0] >= 1

    def test_extra_usage_stages(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT COUNT(*) FROM stage_runs WHERE on_extra_usage = 1"
        ).fetchone()
        assert row[0] >= 1

    def test_multiple_iterations(self, seeded_db):
        """Verify some stages have iteration > 1 (review cycles)."""
        _, conn = seeded_db
        row = conn.execute(
            "SELECT COUNT(*) FROM stage_runs WHERE iteration > 1"
        ).fetchone()
        assert row[0] >= 1


# ---------------------------------------------------------------------------
# Usage snapshots — time series with varying utilization
# ---------------------------------------------------------------------------


class TestUsageSnapshots:
    def test_snapshot_count(self, seeded_db):
        _, conn = seeded_db
        count = conn.execute("SELECT COUNT(*) FROM usage_snapshots").fetchone()[0]
        assert count >= 8

    def test_utilization_range(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT MIN(utilization_5h), MAX(utilization_5h) FROM usage_snapshots"
        ).fetchone()
        assert row[0] < 0.2  # Low
        assert row[1] >= 1.0  # At limit

    def test_extra_usage_scenarios(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT COUNT(*) FROM usage_snapshots WHERE extra_usage_enabled = 1"
        ).fetchone()
        assert row[0] >= 2

    def test_extra_usage_fields_populated(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT * FROM usage_snapshots WHERE extra_usage_enabled = 1 LIMIT 1"
        ).fetchone()
        assert row["extra_usage_monthly_limit"] is not None
        assert row["extra_usage_used_credits"] is not None
        assert row["extra_usage_utilization"] is not None


# ---------------------------------------------------------------------------
# Task events — full audit trails
# ---------------------------------------------------------------------------


class TestTaskEvents:
    def test_event_count(self, seeded_db):
        _, conn = seeded_db
        count = conn.execute("SELECT COUNT(*) FROM task_events").fetchone()[0]
        assert count >= 50

    def test_varied_event_types(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute(
            "SELECT DISTINCT event_type FROM task_events ORDER BY event_type"
        ).fetchall()
        types = {r[0] for r in rows}
        assert "task_dispatched" in types
        assert "stage_started" in types
        assert "stage_completed" in types
        assert "pr_created" in types
        assert "task_completed" in types
        assert "task_failed" in types

    def test_limit_interruption_events(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT COUNT(*) FROM task_events WHERE event_type = 'limit_interruption'"
        ).fetchone()
        assert row[0] >= 1

    def test_events_linked_to_tasks(self, seeded_db):
        _, conn = seeded_db
        orphans = conn.execute(
            "SELECT COUNT(*) FROM task_events WHERE task_id IS NOT NULL "
            "AND task_id NOT IN (SELECT id FROM tasks)"
        ).fetchone()
        assert orphans[0] == 0


# ---------------------------------------------------------------------------
# Queue entries — multi-project with priorities and blockers
# ---------------------------------------------------------------------------


class TestQueueEntries:
    def test_queue_count(self, seeded_db):
        _, conn = seeded_db
        count = conn.execute("SELECT COUNT(*) FROM queue_entries").fetchone()[0]
        assert count >= 5

    def test_multiple_project_queues(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute(
            "SELECT DISTINCT project FROM queue_entries ORDER BY project"
        ).fetchall()
        projects = [r[0] for r in rows]
        assert len(projects) >= 2

    def test_varied_priorities(self, seeded_db):
        _, conn = seeded_db
        rows = conn.execute(
            "SELECT DISTINCT priority FROM queue_entries ORDER BY priority"
        ).fetchall()
        priorities = [r[0] for r in rows]
        assert len(priorities) >= 3

    def test_blocking_relationships(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute(
            "SELECT blocked_by FROM queue_entries WHERE blocked_by IS NOT NULL LIMIT 1"
        ).fetchone()
        assert row is not None
        blockers = json.loads(row["blocked_by"])
        assert isinstance(blockers, list)
        assert len(blockers) > 0


# ---------------------------------------------------------------------------
# Dispatch state
# ---------------------------------------------------------------------------


class TestDispatchState:
    def test_dispatch_state_exists(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute("SELECT * FROM dispatch_state WHERE id = 1").fetchone()
        assert row is not None

    def test_supervisor_heartbeat_set(self, seeded_db):
        _, conn = seeded_db
        row = conn.execute("SELECT supervisor_heartbeat FROM dispatch_state WHERE id = 1").fetchone()
        assert row["supervisor_heartbeat"] is not None


# ---------------------------------------------------------------------------
# Data determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_deterministic_row_counts(self, tmp_path):
        """Seeding twice produces identical row counts."""
        path1 = tmp_path / "db1.db"
        path2 = tmp_path / "db2.db"
        seed_comprehensive_db(path1)
        seed_comprehensive_db(path2)

        conn1 = sqlite3.connect(str(path1))
        conn2 = sqlite3.connect(str(path2))

        for table in ["tasks", "stage_runs", "usage_snapshots", "queue_entries", "task_events", "slots"]:
            count1 = conn1.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            count2 = conn2.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608
            assert count1 == count2, f"Row count differs for {table}"

        conn1.close()
        conn2.close()

    def test_deterministic_task_data(self, tmp_path):
        """Task content fields (excluding auto-generated timestamps) are identical."""
        path1 = tmp_path / "db1.db"
        path2 = tmp_path / "db2.db"
        seed_comprehensive_db(path1)
        seed_comprehensive_db(path2)

        conn1 = sqlite3.connect(str(path1))
        conn2 = sqlite3.connect(str(path2))

        # Compare task data excluding created_at (which uses _now_iso())
        cols = "ticket_id, title, project, slot, status, turns, review_iterations, failure_reason, pr_url"
        rows1 = conn1.execute(f"SELECT {cols} FROM tasks ORDER BY ticket_id").fetchall()
        rows2 = conn2.execute(f"SELECT {cols} FROM tasks ORDER BY ticket_id").fetchall()
        assert len(rows1) == len(rows2)
        for r1, r2 in zip(rows1, rows2):
            assert list(r1) == list(r2), "Task data differs"

        conn1.close()
        conn2.close()

    def test_no_random_values(self, seeded_db):
        """All numeric values should be explicitly set (no random data)."""
        _, conn = seeded_db
        # Every stage run should have explicit token counts
        row = conn.execute(
            "SELECT COUNT(*) FROM stage_runs WHERE input_tokens = 0 AND output_tokens = 0"
        ).fetchone()
        assert row[0] == 0, "All stage runs should have explicit token counts"


# ---------------------------------------------------------------------------
# Standalone script entry point
# ---------------------------------------------------------------------------


class TestStandaloneEntryPoint:
    def test_module_has_main_guard(self):
        """The module should be importable without side effects."""
        import tests.seed_test_db as mod
        assert hasattr(mod, "seed_comprehensive_db")
        assert callable(mod.seed_comprehensive_db)
