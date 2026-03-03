"""Tests for botfarm.db — schema creation and helper functions."""

from __future__ import annotations

import sqlite3

import pytest

from botfarm.db import (
    SCHEMA_VERSION,
    SchemaVersionError,
    _discover_migrations,
    clear_slot_stage,
    count_tasks,
    count_ticket_history,
    get_codex_review_stats,
    get_distinct_projects,
    get_events,
    get_latest_context_fill_by_ticket,
    get_schema_version,
    get_stage_run_aggregates,
    get_stage_runs,
    get_task,
    get_task_by_ticket,
    get_task_history,
    get_ticket_history_entry,
    get_ticket_history_list,
    get_usage_snapshots,
    init_db,
    insert_event,
    insert_stage_run,
    insert_task,
    insert_usage_snapshot,
    is_extra_usage_active,
    load_all_project_pause_states,
    load_all_slots,
    load_capacity_state,
    load_dispatch_state,
    mark_deleted_from_linear,
    resolve_db_path,
    save_capacity_state,
    save_dispatch_state,
    save_project_pause_state,
    save_queue_entries,
    update_stage_run_context_fill,
    update_task,
    upsert_slot,
    upsert_ticket_history,
)


# conn fixture provided by tests/conftest.py


# ---------------------------------------------------------------------------
# Schema / init
# ---------------------------------------------------------------------------


class TestInitDb:
    def test_creates_file_and_tables(self, tmp_path):
        db_file = tmp_path / "sub" / "botfarm.db"
        c = init_db(db_file, allow_migration=True)
        assert db_file.exists()

        tables = {
            row[0]
            for row in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert tables >= {"tasks", "stage_runs", "usage_snapshots", "task_events", "schema_version", "slots", "dispatch_state", "queue_entries", "pipeline_templates", "stage_templates", "stage_loops", "ticket_history"}
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
        c1 = init_db(db_file, allow_migration=True)
        c1.close()
        c2 = init_db(db_file, allow_migration=True)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION
        c2.close()

    def test_schema_version_future_raises(self, tmp_path):
        db_file = tmp_path / "botfarm.db"
        c = init_db(db_file, allow_migration=True)
        c.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION + 99,))
        c.commit()
        c.close()
        with pytest.raises(SchemaVersionError, match="Cannot downgrade"):
            init_db(db_file, allow_migration=True)

    def test_resolve_db_path_from_env(self, tmp_path, monkeypatch):
        """resolve_db_path returns the path from BOTFARM_DB_PATH env var."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        result = resolve_db_path()
        assert result == tmp_path / "test.db"

    def test_resolve_db_path_expands_tilde(self, monkeypatch):
        """resolve_db_path expands ~ in the path."""
        monkeypatch.setenv("BOTFARM_DB_PATH", "~/.botfarm/botfarm.db")
        from pathlib import Path
        result = resolve_db_path()
        assert "~" not in str(result)
        assert result == Path.home() / ".botfarm" / "botfarm.db"

    def test_resolve_db_path_raises_when_unset(self, monkeypatch):
        """resolve_db_path raises RuntimeError when env var is missing."""
        monkeypatch.delenv("BOTFARM_DB_PATH", raising=False)
        with pytest.raises(RuntimeError, match="BOTFARM_DB_PATH"):
            resolve_db_path()

    def test_resolve_db_path_raises_when_empty(self, monkeypatch):
        """resolve_db_path raises RuntimeError when env var is empty."""
        monkeypatch.setenv("BOTFARM_DB_PATH", "")
        with pytest.raises(RuntimeError, match="BOTFARM_DB_PATH"):
            resolve_db_path()

    def test_init_db_uses_provided_path(self, tmp_path, monkeypatch):
        """init_db uses the explicitly provided path, not BOTFARM_DB_PATH."""
        monkeypatch.delenv("BOTFARM_DB_PATH", raising=False)
        db_file = tmp_path / "normal.db"
        c = init_db(db_file, allow_migration=True)
        c.close()
        assert db_file.exists()

    def test_schema_version_older_without_migration_raises(self, tmp_path):
        """init_db raises SchemaVersionError for older schema without allow_migration."""
        db_file = tmp_path / "botfarm.db"
        c = init_db(db_file, allow_migration=True)
        c.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION - 1,))
        c.commit()
        c.close()
        with pytest.raises(SchemaVersionError, match="older than expected"):
            init_db(db_file, allow_migration=False)

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
        c2 = init_db(db_file, allow_migration=True)
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
        c2 = init_db(db_file, allow_migration=True)
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

    def test_migration_v4_to_v5(self, tmp_path):
        """Simulate a v4 database and verify migration adds resets_at_7d column."""
        db_file = tmp_path / "botfarm.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        # Create v4 schema (usage_snapshots without resets_at_7d)
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
            CREATE TABLE IF NOT EXISTS slots (
                project TEXT NOT NULL,
                slot_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'free',
                ticket_id TEXT,
                ticket_title TEXT,
                branch TEXT,
                pr_url TEXT,
                stage TEXT,
                stage_iteration INTEGER NOT NULL DEFAULT 0,
                current_session_id TEXT,
                started_at TEXT,
                stage_started_at TEXT,
                sigterm_sent_at TEXT,
                pid INTEGER,
                interrupted_by_limit INTEGER NOT NULL DEFAULT 0,
                resume_after TEXT,
                stages_completed TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY(project, slot_id)
            );
            CREATE TABLE IF NOT EXISTS dispatch_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                paused INTEGER NOT NULL DEFAULT 0,
                pause_reason TEXT,
                supervisor_heartbeat TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
        """)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        conn.execute("INSERT INTO schema_version (version) VALUES (4)")
        # Insert a snapshot without resets_at_7d to verify data survives migration
        conn.execute(
            "INSERT INTO usage_snapshots (utilization_5h, utilization_7d, resets_at) VALUES (?, ?, ?)",
            (0.5, 0.3, "2026-02-13T00:00:00Z"),
        )
        conn.commit()
        conn.close()

        # Re-open via init_db — should migrate to v5
        c2 = init_db(db_file, allow_migration=True)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

        # resets_at_7d column should exist and be NULL for pre-existing rows
        snap = c2.execute("SELECT resets_at, resets_at_7d FROM usage_snapshots").fetchone()
        assert snap[0] == "2026-02-13T00:00:00Z"
        assert snap[1] is None

        # Should be able to insert with the new column
        c2.execute(
            "INSERT INTO usage_snapshots (utilization_5h, utilization_7d, resets_at, resets_at_7d) VALUES (?, ?, ?, ?)",
            (0.6, 0.4, "2026-02-14T00:00:00Z", "2026-02-21T00:00:00Z"),
        )
        c2.commit()
        snap2 = c2.execute(
            "SELECT resets_at_7d FROM usage_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert snap2[0] == "2026-02-21T00:00:00Z"
        c2.close()

    def test_migration_v5_to_v6(self, tmp_path):
        """Simulate a v5 database and verify migration drops cost_usd columns."""
        db_file = tmp_path / "botfarm.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
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
                resets_at_7d TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER REFERENCES tasks(id),
                event_type TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS slots (
                project TEXT NOT NULL,
                slot_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'free',
                ticket_id TEXT,
                ticket_title TEXT,
                branch TEXT,
                pr_url TEXT,
                stage TEXT,
                stage_iteration INTEGER NOT NULL DEFAULT 0,
                current_session_id TEXT,
                started_at TEXT,
                stage_started_at TEXT,
                sigterm_sent_at TEXT,
                pid INTEGER,
                interrupted_by_limit INTEGER NOT NULL DEFAULT 0,
                resume_after TEXT,
                stages_completed TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY(project, slot_id)
            );
            CREATE TABLE IF NOT EXISTS dispatch_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                paused INTEGER NOT NULL DEFAULT 0,
                pause_reason TEXT,
                supervisor_heartbeat TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
        """)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        conn.execute("INSERT INTO schema_version (version) VALUES (5)")
        # Insert data with cost_usd to verify it survives (column dropped)
        conn.execute(
            "INSERT INTO tasks (ticket_id, title, project, slot, cost_usd) VALUES (?, ?, ?, ?, ?)",
            ("SMA-1", "Old task", "proj", 1, 1.25),
        )
        conn.execute(
            "INSERT INTO stage_runs (task_id, stage, cost_usd) VALUES (?, ?, ?)",
            (1, "implement", 0.75),
        )
        conn.commit()
        conn.close()

        # Re-open via init_db — should migrate to v6
        c2 = init_db(db_file, allow_migration=True)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

        # cost_usd columns should be gone
        task_cols = {r[1] for r in c2.execute("PRAGMA table_info(tasks)").fetchall()}
        assert "cost_usd" not in task_cols

        stage_cols = {r[1] for r in c2.execute("PRAGMA table_info(stage_runs)").fetchall()}
        assert "cost_usd" not in stage_cols

        # Data should survive (other columns intact)
        task = c2.execute("SELECT ticket_id, turns FROM tasks WHERE ticket_id = 'SMA-1'").fetchone()
        assert task[0] == "SMA-1"
        assert task[1] == 0

        c2.close()

    def test_migration_v6_to_v7(self, tmp_path):
        """Simulate a v6 database and verify migration creates queue_entries table."""
        db_file = tmp_path / "botfarm.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
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
                exit_subtype TEXT,
                was_limit_restart INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS usage_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utilization_5h REAL,
                utilization_7d REAL,
                resets_at TEXT,
                resets_at_7d TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER REFERENCES tasks(id),
                event_type TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
            CREATE TABLE IF NOT EXISTS slots (
                project TEXT NOT NULL,
                slot_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'free',
                ticket_id TEXT,
                ticket_title TEXT,
                branch TEXT,
                pr_url TEXT,
                stage TEXT,
                stage_iteration INTEGER NOT NULL DEFAULT 0,
                current_session_id TEXT,
                started_at TEXT,
                stage_started_at TEXT,
                sigterm_sent_at TEXT,
                pid INTEGER,
                interrupted_by_limit INTEGER NOT NULL DEFAULT 0,
                resume_after TEXT,
                stages_completed TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                PRIMARY KEY(project, slot_id)
            );
            CREATE TABLE IF NOT EXISTS dispatch_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                paused INTEGER NOT NULL DEFAULT 0,
                pause_reason TEXT,
                supervisor_heartbeat TEXT,
                updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
            );
        """)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        conn.execute("INSERT INTO schema_version (version) VALUES (6)")
        conn.commit()
        conn.close()

        # Re-open via init_db — should migrate to v7
        c2 = init_db(db_file, allow_migration=True)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

        # queue_entries table should exist
        tables = {
            r[0]
            for r in c2.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "queue_entries" in tables

        # Index should exist
        indexes = {
            r[0]
            for r in c2.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "idx_queue_entries_project" in indexes

        # Verify columns
        cols = {r[1] for r in c2.execute("PRAGMA table_info(queue_entries)").fetchall()}
        assert cols == {"id", "project", "position", "ticket_id", "ticket_title", "priority", "sort_order", "url", "snapshot_at", "blocked_by"}

        c2.close()

    def test_migration_008_adds_token_columns(self, tmp_path):
        """Migration 008 adds token usage columns to stage_runs."""
        db_file = tmp_path / "v7.db"
        conn = sqlite3.connect(str(db_file))
        conn.execute("PRAGMA foreign_keys=ON")
        # Build a v7 schema (all tables through migration 007)
        conn.executescript("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                project TEXT NOT NULL,
                slot INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                turns INTEGER NOT NULL DEFAULT 0,
                review_iterations INTEGER NOT NULL DEFAULT 0,
                comments TEXT NOT NULL DEFAULT '',
                limit_interruptions INTEGER NOT NULL DEFAULT 0,
                failure_reason TEXT,
                pr_url TEXT,
                pipeline_stage TEXT,
                review_state TEXT
            );
            CREATE TABLE stage_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER NOT NULL REFERENCES tasks(id),
                stage TEXT NOT NULL,
                iteration INTEGER NOT NULL DEFAULT 1,
                session_id TEXT,
                turns INTEGER NOT NULL DEFAULT 0,
                duration_seconds REAL,
                exit_subtype TEXT,
                was_limit_restart INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE TABLE usage_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                utilization_5h REAL,
                utilization_7d REAL,
                resets_at TEXT,
                resets_at_7d TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER REFERENCES tasks(id),
                event_type TEXT NOT NULL,
                detail TEXT,
                created_at TEXT NOT NULL
            );
            CREATE TABLE slots (
                project TEXT NOT NULL,
                slot_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'free',
                ticket_id TEXT,
                ticket_title TEXT,
                branch TEXT,
                pr_url TEXT,
                stage TEXT,
                stage_iteration INTEGER NOT NULL DEFAULT 0,
                current_session_id TEXT,
                started_at TEXT,
                stage_started_at TEXT,
                sigterm_sent_at TEXT,
                pid INTEGER,
                interrupted_by_limit INTEGER NOT NULL DEFAULT 0,
                resume_after TEXT,
                stages_completed TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(project, slot_id)
            );
            CREATE TABLE dispatch_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                paused INTEGER NOT NULL DEFAULT 0,
                pause_reason TEXT,
                supervisor_heartbeat TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE queue_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project TEXT NOT NULL,
                position INTEGER NOT NULL,
                ticket_id TEXT NOT NULL,
                ticket_title TEXT NOT NULL,
                priority INTEGER NOT NULL,
                sort_order REAL NOT NULL,
                url TEXT NOT NULL,
                snapshot_at TEXT NOT NULL
            );
        """)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
        )
        conn.execute("INSERT INTO schema_version (version) VALUES (7)")
        # Insert a pre-existing stage run
        conn.execute(
            "INSERT INTO tasks (ticket_id, title, project, slot, created_at) VALUES ('T-1', 'Old', 'p', 1, '2025-01-01')"
        )
        conn.execute(
            "INSERT INTO stage_runs (task_id, stage, created_at) VALUES (1, 'implement', '2025-01-01')"
        )
        conn.commit()
        conn.close()

        # Re-open via init_db — should migrate to v8
        c2 = init_db(db_file, allow_migration=True)
        row = c2.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

        # New columns should exist
        stage_cols = {r[1] for r in c2.execute("PRAGMA table_info(stage_runs)").fetchall()}
        for col in ("input_tokens", "output_tokens", "cache_read_input_tokens",
                     "cache_creation_input_tokens", "total_cost_usd",
                     "context_fill_pct", "model_usage_json"):
            assert col in stage_cols

        # Pre-existing row should have defaults
        old = c2.execute("SELECT * FROM stage_runs WHERE id = 1").fetchone()
        assert old["input_tokens"] == 0
        assert old["output_tokens"] == 0
        assert old["cache_read_input_tokens"] == 0
        assert old["cache_creation_input_tokens"] == 0
        assert old["total_cost_usd"] == pytest.approx(0.0)
        assert old["context_fill_pct"] is None
        assert old["model_usage_json"] is None

        c2.close()


# ---------------------------------------------------------------------------
# Migration infrastructure
# ---------------------------------------------------------------------------


class TestMigrationInfrastructure:
    def test_discover_migrations(self):
        """All 10 SQL migration files are discovered with correct numbering."""
        migrations = _discover_migrations()
        assert len(migrations) >= 10
        for version in range(1, 11):
            assert version in migrations
            assert migrations[version].suffix == ".sql"

    def test_migration_numbers_contiguous(self):
        """Migration numbers form a contiguous sequence with no gaps."""
        migrations = _discover_migrations()
        versions = sorted(migrations.keys())
        expected = list(range(versions[0], versions[-1] + 1))
        assert versions == expected

    def test_schema_version_matches_highest_migration(self):
        """SCHEMA_VERSION equals the highest migration file number."""
        migrations = _discover_migrations()
        assert SCHEMA_VERSION == max(migrations.keys())
        assert SCHEMA_VERSION == get_schema_version()

    def test_fresh_db_runs_all_migrations(self, tmp_path):
        """A fresh database reaches SCHEMA_VERSION with all tables and cost_usd dropped."""
        db_file = tmp_path / "fresh.db"
        c = init_db(db_file, allow_migration=True)

        # Version should be current
        row = c.execute("SELECT version FROM schema_version").fetchone()
        assert row[0] == SCHEMA_VERSION

        # All tables should exist
        tables = {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert tables >= {
            "tasks", "stage_runs", "usage_snapshots", "task_events",
            "schema_version", "slots", "dispatch_state", "queue_entries",
        }

        # cost_usd columns should NOT exist (dropped by migration 006)
        task_cols = {r[1] for r in c.execute("PRAGMA table_info(tasks)").fetchall()}
        assert "cost_usd" not in task_cols
        stage_cols = {r[1] for r in c.execute("PRAGMA table_info(stage_runs)").fetchall()}
        assert "cost_usd" not in stage_cols

        # Token usage columns from migration 008 should exist
        for col in ("input_tokens", "output_tokens", "cache_read_input_tokens",
                     "cache_creation_input_tokens", "total_cost_usd",
                     "context_fill_pct", "model_usage_json"):
            assert col in stage_cols, f"Missing column {col} in stage_runs"

        # Columns added in later migrations should exist
        assert "pr_url" in task_cols
        assert "pipeline_stage" in task_cols
        assert "review_state" in task_cols

        snap_cols = {r[1] for r in c.execute("PRAGMA table_info(usage_snapshots)").fetchall()}
        assert "resets_at_7d" in snap_cols

        dispatch_cols = {r[1] for r in c.execute("PRAGMA table_info(dispatch_state)").fetchall()}
        assert "supervisor_heartbeat" in dispatch_cols

        c.close()


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
        update_task(conn, 1, turns=10)
        insert_task(conn, ticket_id="SORT-B", title="B", project="p", slot=2, status="completed")
        update_task(conn, 2, turns=1)
        asc = get_task_history(conn, sort_by="turns", sort_dir="ASC")
        assert asc[0]["turns"] <= asc[1]["turns"]

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

    def test_token_usage_columns(self, conn):
        tid = insert_task(
            conn, ticket_id="SR-TOK", title="Token test", project="p", slot=1
        )
        model_json = '{"claude-opus-4-6": {"costUSD": 0.5}}'
        sr_id = insert_stage_run(
            conn,
            task_id=tid,
            stage="implement",
            input_tokens=1000,
            output_tokens=500,
            cache_read_input_tokens=200,
            cache_creation_input_tokens=300,
            total_cost_usd=0.42,
            context_fill_pct=0.9,
            model_usage_json=model_json,
        )
        runs = get_stage_runs(conn, tid)
        assert len(runs) == 1
        row = runs[0]
        assert row["input_tokens"] == 1000
        assert row["output_tokens"] == 500
        assert row["cache_read_input_tokens"] == 200
        assert row["cache_creation_input_tokens"] == 300
        assert row["total_cost_usd"] == pytest.approx(0.42)
        assert row["context_fill_pct"] == pytest.approx(0.9)
        assert row["model_usage_json"] == model_json

    def test_token_usage_defaults(self, conn):
        """Existing callers that omit token fields should still work (defaults)."""
        tid = insert_task(
            conn, ticket_id="SR-DEF", title="Default test", project="p", slot=1
        )
        insert_stage_run(conn, task_id=tid, stage="review")
        runs = get_stage_runs(conn, tid)
        row = runs[0]
        assert row["input_tokens"] == 0
        assert row["output_tokens"] == 0
        assert row["cache_read_input_tokens"] == 0
        assert row["cache_creation_input_tokens"] == 0
        assert row["total_cost_usd"] == pytest.approx(0.0)
        assert row["context_fill_pct"] is None
        assert row["model_usage_json"] is None
        assert row["log_file_path"] is None

    def test_log_file_path_stored(self, conn):
        """log_file_path is persisted when provided."""
        tid = insert_task(
            conn, ticket_id="SR-LOG", title="Log path test", project="p", slot=1
        )
        insert_stage_run(
            conn,
            task_id=tid,
            stage="implement",
            log_file_path="/home/user/.botfarm/logs/SMA-1/implement-20260226.log",
        )
        runs = get_stage_runs(conn, tid)
        assert len(runs) == 1
        assert runs[0]["log_file_path"] == "/home/user/.botfarm/logs/SMA-1/implement-20260226.log"

    def test_log_file_path_defaults_to_none(self, conn):
        """Omitting log_file_path defaults to None for backward compatibility."""
        tid = insert_task(
            conn, ticket_id="SR-LOGNONE", title="No log", project="p", slot=1
        )
        insert_stage_run(conn, task_id=tid, stage="review")
        runs = get_stage_runs(conn, tid)
        assert runs[0]["log_file_path"] is None


class TestUpdateStageRunContextFill:
    def test_updates_context_fill_in_place(self, conn):
        tid = insert_task(
            conn, ticket_id="CF-1", title="Fill test", project="p", slot=1
        )
        sr_id = insert_stage_run(conn, task_id=tid, stage="implement")
        conn.commit()

        # Initially None
        runs = get_stage_runs(conn, tid)
        assert runs[0]["context_fill_pct"] is None

        # Update to 25.5%
        update_stage_run_context_fill(conn, sr_id, 25.5)
        runs = get_stage_runs(conn, tid)
        assert runs[0]["context_fill_pct"] == pytest.approx(25.5)

        # Update again (overwrite) to 60.0%
        update_stage_run_context_fill(conn, sr_id, 60.0)
        runs = get_stage_runs(conn, tid)
        assert runs[0]["context_fill_pct"] == pytest.approx(60.0)

    def test_does_not_affect_other_rows(self, conn):
        tid = insert_task(
            conn, ticket_id="CF-2", title="Multi", project="p", slot=1
        )
        sr_id_1 = insert_stage_run(
            conn, task_id=tid, stage="implement", context_fill_pct=10.0,
        )
        sr_id_2 = insert_stage_run(conn, task_id=tid, stage="review")
        conn.commit()

        update_stage_run_context_fill(conn, sr_id_2, 55.0)
        runs = get_stage_runs(conn, tid)
        assert runs[0]["context_fill_pct"] == pytest.approx(10.0)  # unchanged
        assert runs[1]["context_fill_pct"] == pytest.approx(55.0)  # updated


class TestStageRunAggregates:
    def test_aggregates_single_task(self, conn):
        tid = insert_task(conn, ticket_id="AGG-1", title="Agg", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement",
                         input_tokens=100, output_tokens=50,
                         total_cost_usd=0.01, context_fill_pct=30.0)
        insert_stage_run(conn, task_id=tid, stage="review",
                         input_tokens=200, output_tokens=80,
                         total_cost_usd=0.02, context_fill_pct=55.0)
        result = get_stage_run_aggregates(conn, [tid])
        assert tid in result
        assert result[tid]["total_input_tokens"] == 300
        assert result[tid]["total_output_tokens"] == 130
        assert result[tid]["total_cost_usd"] == pytest.approx(0.03)
        assert result[tid]["max_context_fill_pct"] == pytest.approx(55.0)

    def test_aggregates_multiple_tasks(self, conn):
        t1 = insert_task(conn, ticket_id="AGG-2", title="T1", project="p", slot=1)
        t2 = insert_task(conn, ticket_id="AGG-3", title="T2", project="p", slot=2)
        insert_stage_run(conn, task_id=t1, stage="implement",
                         input_tokens=100, output_tokens=50, total_cost_usd=0.01)
        insert_stage_run(conn, task_id=t2, stage="implement",
                         input_tokens=200, output_tokens=80, total_cost_usd=0.02)
        result = get_stage_run_aggregates(conn, [t1, t2])
        assert t1 in result
        assert t2 in result
        assert result[t1]["total_input_tokens"] == 100
        assert result[t2]["total_input_tokens"] == 200

    def test_empty_task_ids(self, conn):
        result = get_stage_run_aggregates(conn, [])
        assert result == {}

    def test_no_matching_tasks(self, conn):
        result = get_stage_run_aggregates(conn, [9999])
        assert result == {}

    def test_null_context_fill(self, conn):
        tid = insert_task(conn, ticket_id="AGG-4", title="NullFill", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement",
                         input_tokens=100, output_tokens=50)
        result = get_stage_run_aggregates(conn, [tid])
        assert result[tid]["max_context_fill_pct"] is None


class TestCodexReviewStats:
    def test_codex_stats_single_task(self, conn):
        tid = insert_task(conn, ticket_id="CDX-1", title="Codex", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="codex_review", iteration=1,
                         input_tokens=500_000, output_tokens=10_000,
                         cache_read_input_tokens=200_000,
                         duration_seconds=45.0, exit_subtype="approved")
        insert_stage_run(conn, task_id=tid, stage="codex_review", iteration=2,
                         input_tokens=600_000, output_tokens=12_000,
                         cache_read_input_tokens=250_000,
                         duration_seconds=50.0, exit_subtype="changes_requested")
        # Non-codex stage should be excluded
        insert_stage_run(conn, task_id=tid, stage="implement",
                         input_tokens=100, output_tokens=50)
        result = get_codex_review_stats(conn, [tid])
        assert tid in result
        assert result[tid]["codex_input_tokens"] == 1_100_000
        assert result[tid]["codex_output_tokens"] == 22_000
        assert result[tid]["codex_cache_read_tokens"] == 450_000
        assert result[tid]["codex_runs"] == 2
        assert result[tid]["codex_duration_seconds"] == pytest.approx(95.0)
        assert result[tid]["codex_approved"] == 1
        assert result[tid]["codex_changes_requested"] == 1
        assert result[tid]["codex_failed"] == 0

    def test_codex_stats_with_failures(self, conn):
        tid = insert_task(conn, ticket_id="CDX-2", title="CodexFail", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="codex_review",
                         input_tokens=100, output_tokens=10,
                         exit_subtype="failed")
        insert_stage_run(conn, task_id=tid, stage="codex_review",
                         input_tokens=100, output_tokens=10,
                         exit_subtype="timeout")
        insert_stage_run(conn, task_id=tid, stage="codex_review",
                         input_tokens=100, output_tokens=10,
                         exit_subtype="approved")
        result = get_codex_review_stats(conn, [tid])
        assert result[tid]["codex_failed"] == 2
        assert result[tid]["codex_approved"] == 1
        assert result[tid]["codex_runs"] == 3

    def test_codex_stats_no_codex_runs(self, conn):
        tid = insert_task(conn, ticket_id="CDX-3", title="NoCdx", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement",
                         input_tokens=100, output_tokens=50)
        result = get_codex_review_stats(conn, [tid])
        assert tid not in result

    def test_codex_stats_empty_ids(self, conn):
        result = get_codex_review_stats(conn, [])
        assert result == {}

    def test_codex_stats_multiple_tasks(self, conn):
        t1 = insert_task(conn, ticket_id="CDX-4", title="T1", project="p", slot=1)
        t2 = insert_task(conn, ticket_id="CDX-5", title="T2", project="p", slot=2)
        insert_stage_run(conn, task_id=t1, stage="codex_review",
                         input_tokens=500, output_tokens=50,
                         exit_subtype="approved")
        insert_stage_run(conn, task_id=t2, stage="codex_review",
                         input_tokens=800, output_tokens=80,
                         exit_subtype="changes_requested")
        result = get_codex_review_stats(conn, [t1, t2])
        assert result[t1]["codex_input_tokens"] == 500
        assert result[t2]["codex_input_tokens"] == 800
        assert result[t1]["codex_approved"] == 1
        assert result[t2]["codex_changes_requested"] == 1


class TestLatestContextFillByTicket:
    def test_returns_latest_fill(self, conn):
        tid = insert_task(conn, ticket_id="CTX-1", title="Ctx", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement", context_fill_pct=40.0)
        insert_stage_run(conn, task_id=tid, stage="review", context_fill_pct=65.0)
        result = get_latest_context_fill_by_ticket(conn, ["CTX-1"])
        assert "CTX-1" in result
        assert result["CTX-1"] == pytest.approx(65.0)

    def test_empty_ticket_ids(self, conn):
        result = get_latest_context_fill_by_ticket(conn, [])
        assert result == {}

    def test_no_matching_tickets(self, conn):
        result = get_latest_context_fill_by_ticket(conn, ["NOPE-1"])
        assert result == {}

    def test_null_fills_excluded(self, conn):
        tid = insert_task(conn, ticket_id="CTX-2", title="NullCtx", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement")  # no context_fill_pct
        result = get_latest_context_fill_by_ticket(conn, ["CTX-2"])
        assert "CTX-2" not in result


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

    def test_insert_with_resets_at_7d(self, conn):
        sid = insert_usage_snapshot(
            conn,
            utilization_5h=0.73,
            utilization_7d=0.45,
            resets_at="2026-02-13T00:00:00Z",
            resets_at_7d="2026-02-20T00:00:00Z",
        )
        rows = get_usage_snapshots(conn)
        assert len(rows) == 1
        assert rows[0]["id"] == sid
        assert rows[0]["resets_at"] == "2026-02-13T00:00:00Z"
        assert rows[0]["resets_at_7d"] == "2026-02-20T00:00:00Z"

    def test_nullable_fields(self, conn):
        insert_usage_snapshot(conn)
        rows = get_usage_snapshots(conn)
        assert rows[0]["utilization_5h"] is None
        assert rows[0]["utilization_7d"] is None
        assert rows[0]["resets_at"] is None
        assert rows[0]["resets_at_7d"] is None

    def test_limit(self, conn):
        for _ in range(5):
            insert_usage_snapshot(conn, utilization_5h=0.5)
        rows = get_usage_snapshots(conn, limit=2)
        assert len(rows) == 2

    def test_extra_usage_columns(self, conn):
        sid = insert_usage_snapshot(
            conn,
            utilization_5h=1.0,
            utilization_7d=0.58,
            extra_usage_enabled=True,
            extra_usage_monthly_limit=5000.0,
            extra_usage_used_credits=2344.0,
            extra_usage_utilization=46.88,
        )
        rows = get_usage_snapshots(conn)
        assert len(rows) == 1
        row = rows[0]
        assert row["extra_usage_enabled"] == 1
        assert row["extra_usage_monthly_limit"] == pytest.approx(5000.0)
        assert row["extra_usage_used_credits"] == pytest.approx(2344.0)
        assert row["extra_usage_utilization"] == pytest.approx(46.88)

    def test_extra_usage_columns_default(self, conn):
        insert_usage_snapshot(conn, utilization_5h=0.5)
        rows = get_usage_snapshots(conn)
        row = rows[0]
        assert row["extra_usage_enabled"] == 0
        assert row["extra_usage_monthly_limit"] is None
        assert row["extra_usage_used_credits"] is None
        assert row["extra_usage_utilization"] is None


# ---------------------------------------------------------------------------
# Extra usage active check
# ---------------------------------------------------------------------------


class TestIsExtraUsageActive:
    def test_no_snapshots(self, conn):
        assert is_extra_usage_active(conn) is False

    def test_extra_usage_disabled(self, conn):
        insert_usage_snapshot(
            conn, utilization_5h=1.0, extra_usage_enabled=False,
        )
        assert is_extra_usage_active(conn) is False

    def test_extra_usage_enabled_below_100(self, conn):
        insert_usage_snapshot(
            conn, utilization_5h=0.85, extra_usage_enabled=True,
        )
        assert is_extra_usage_active(conn) is False

    def test_extra_usage_enabled_5h_at_100(self, conn):
        insert_usage_snapshot(
            conn, utilization_5h=1.0, utilization_7d=0.5,
            extra_usage_enabled=True,
        )
        assert is_extra_usage_active(conn) is True

    def test_extra_usage_enabled_7d_at_100(self, conn):
        insert_usage_snapshot(
            conn, utilization_5h=0.5, utilization_7d=1.0,
            extra_usage_enabled=True,
        )
        assert is_extra_usage_active(conn) is True

    def test_uses_latest_snapshot(self, conn):
        # Insert older snapshot with extra usage active
        insert_usage_snapshot(
            conn, utilization_5h=1.0, extra_usage_enabled=True,
        )
        # Insert newer snapshot with extra usage inactive
        insert_usage_snapshot(
            conn, utilization_5h=0.3, extra_usage_enabled=True,
        )
        assert is_extra_usage_active(conn) is False


# ---------------------------------------------------------------------------
# Stage run on_extra_usage
# ---------------------------------------------------------------------------


class TestStageRunOnExtraUsage:
    def test_on_extra_usage_flag(self, conn):
        tid = insert_task(conn, ticket_id="EU-1", title="Extra", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement", on_extra_usage=True,
                         total_cost_usd=0.50)
        runs = get_stage_runs(conn, tid)
        assert runs[0]["on_extra_usage"] == 1

    def test_on_extra_usage_defaults_false(self, conn):
        tid = insert_task(conn, ticket_id="EU-2", title="NoExtra", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement")
        runs = get_stage_runs(conn, tid)
        assert runs[0]["on_extra_usage"] == 0

    def test_aggregates_include_extra_usage_cost(self, conn):
        tid = insert_task(conn, ticket_id="EU-3", title="MixedExtra", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement",
                         total_cost_usd=0.10, on_extra_usage=True)
        insert_stage_run(conn, task_id=tid, stage="review",
                         total_cost_usd=0.05, on_extra_usage=False)
        insert_stage_run(conn, task_id=tid, stage="fix",
                         total_cost_usd=0.08, on_extra_usage=True)
        result = get_stage_run_aggregates(conn, [tid])
        assert result[tid]["total_cost_usd"] == pytest.approx(0.23)
        assert result[tid]["extra_usage_cost_usd"] == pytest.approx(0.18)

    def test_aggregates_no_extra_usage(self, conn):
        tid = insert_task(conn, ticket_id="EU-4", title="NoExtra", project="p", slot=1)
        insert_stage_run(conn, task_id=tid, stage="implement",
                         total_cost_usd=0.10, on_extra_usage=False)
        result = get_stage_run_aggregates(conn, [tid])
        assert result[tid]["extra_usage_cost_usd"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Task started_on_extra_usage
# ---------------------------------------------------------------------------


class TestTaskStartedOnExtraUsage:
    def test_update_started_on_extra_usage(self, conn):
        tid = insert_task(conn, ticket_id="TEU-1", title="Extra", project="p", slot=1)
        update_task(conn, tid, started_on_extra_usage=1)
        task = get_task(conn, tid)
        assert task["started_on_extra_usage"] == 1

    def test_started_on_extra_usage_defaults_false(self, conn):
        tid = insert_task(conn, ticket_id="TEU-2", title="Normal", project="p", slot=1)
        task = get_task(conn, tid)
        assert task["started_on_extra_usage"] == 0


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
# Stage COALESCE protection and clear_slot_stage
# ---------------------------------------------------------------------------


class TestUpsertSlotCoalesce:
    """upsert_slot uses COALESCE so NULL stage values don't overwrite
    non-NULL values already in the database."""

    def test_null_stage_does_not_overwrite_existing(self, conn):
        # First insert with a real stage value
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "stage": "implement", "stage_iteration": 1,
            "stage_started_at": "2026-01-01T00:00:00Z",
            "current_session_id": "sess-1",
        })
        conn.commit()

        # Second upsert with stage=None (as _save() would do when
        # the supervisor's in-memory slot has stage=None)
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "stage": None, "stage_iteration": 0,
            "stage_started_at": None, "current_session_id": None,
        })
        conn.commit()

        rows = load_all_slots(conn)
        assert rows[0]["stage"] == "implement"
        assert rows[0]["stage_started_at"] == "2026-01-01T00:00:00Z"
        assert rows[0]["current_session_id"] == "sess-1"

    def test_non_null_stage_does_overwrite(self, conn):
        """When supervisor has a real stage value, it should overwrite."""
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "stage": "implement",
        })
        conn.commit()

        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "stage": "review", "stage_iteration": 2,
        })
        conn.commit()

        rows = load_all_slots(conn)
        assert rows[0]["stage"] == "review"
        assert rows[0]["stage_iteration"] == 2


class TestClearSlotStage:
    def test_clears_stage_fields(self, conn):
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "stage": "implement", "stage_iteration": 2,
            "stage_started_at": "2026-01-01T00:00:00Z",
            "current_session_id": "sess-1",
        })
        conn.commit()

        clear_slot_stage(conn, "proj", 1)
        conn.commit()

        rows = load_all_slots(conn)
        assert rows[0]["stage"] is None
        assert rows[0]["stage_iteration"] == 0
        assert rows[0]["stage_started_at"] is None
        assert rows[0]["current_session_id"] is None

    def test_clear_then_upsert_stays_null(self, conn):
        """After clear_slot_stage, a subsequent upsert with NULL stage
        should keep the fields NULL (not revive old values)."""
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "stage": "implement",
        })
        conn.commit()

        clear_slot_stage(conn, "proj", 1)
        conn.commit()

        # Another upsert with stage=None
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
        })
        conn.commit()

        rows = load_all_slots(conn)
        assert rows[0]["stage"] is None

    def test_only_affects_target_slot(self, conn):
        upsert_slot(conn, {
            "project": "proj", "slot_id": 1, "status": "busy",
            "stage": "implement",
        })
        upsert_slot(conn, {
            "project": "proj", "slot_id": 2, "status": "busy",
            "stage": "review",
        })
        conn.commit()

        clear_slot_stage(conn, "proj", 1)
        conn.commit()

        rows = load_all_slots(conn)
        slot1 = [r for r in rows if r["slot_id"] == 1][0]
        slot2 = [r for r in rows if r["slot_id"] == 2][0]
        assert slot1["stage"] is None
        assert slot2["stage"] == "review"


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


# ---------------------------------------------------------------------------
# Queue entries
# ---------------------------------------------------------------------------


class _FakeIssue:
    """Minimal stand-in for LinearIssue with the fields save_queue_entries needs."""

    def __init__(self, identifier, title, priority, sort_order, url, blocked_by=None):
        self.identifier = identifier
        self.title = title
        self.priority = priority
        self.sort_order = sort_order
        self.url = url
        self.blocked_by = blocked_by


class TestQueueEntries:
    def test_save_writes_entries(self, conn):
        candidates = [
            _FakeIssue("SMA-1", "First", 2, 1.0, "https://linear.app/1"),
            _FakeIssue("SMA-2", "Second", 3, 2.5, "https://linear.app/2"),
        ]
        save_queue_entries(conn, "proj", candidates, "2026-02-25T00:00:00Z")

        rows = conn.execute(
            "SELECT * FROM queue_entries WHERE project = ? ORDER BY position", ("proj",)
        ).fetchall()
        assert len(rows) == 2
        assert rows[0]["position"] == 0
        assert rows[0]["ticket_id"] == "SMA-1"
        assert rows[0]["ticket_title"] == "First"
        assert rows[0]["priority"] == 2
        assert rows[0]["sort_order"] == 1.0
        assert rows[0]["url"] == "https://linear.app/1"
        assert rows[0]["snapshot_at"] == "2026-02-25T00:00:00Z"
        assert rows[1]["position"] == 1
        assert rows[1]["ticket_id"] == "SMA-2"

    def test_save_replaces_entries(self, conn):
        old = [_FakeIssue("SMA-1", "Old", 2, 1.0, "https://linear.app/1")]
        save_queue_entries(conn, "proj", old, "2026-02-25T00:00:00Z")

        new = [_FakeIssue("SMA-3", "New", 1, 0.5, "https://linear.app/3")]
        save_queue_entries(conn, "proj", new, "2026-02-25T01:00:00Z")

        rows = conn.execute(
            "SELECT * FROM queue_entries WHERE project = ?", ("proj",)
        ).fetchall()
        assert len(rows) == 1
        assert rows[0]["ticket_id"] == "SMA-3"
        assert rows[0]["snapshot_at"] == "2026-02-25T01:00:00Z"

    def test_empty_candidates_clears_entries(self, conn):
        candidates = [_FakeIssue("SMA-1", "T", 2, 1.0, "https://linear.app/1")]
        save_queue_entries(conn, "proj", candidates, "2026-02-25T00:00:00Z")

        save_queue_entries(conn, "proj", [], "2026-02-25T01:00:00Z")

        rows = conn.execute(
            "SELECT * FROM queue_entries WHERE project = ?", ("proj",)
        ).fetchall()
        assert len(rows) == 0

    def test_other_projects_not_affected(self, conn):
        alpha = [_FakeIssue("SMA-1", "Alpha", 2, 1.0, "https://linear.app/1")]
        beta = [_FakeIssue("SMA-2", "Beta", 3, 2.0, "https://linear.app/2")]
        save_queue_entries(conn, "alpha", alpha, "2026-02-25T00:00:00Z")
        save_queue_entries(conn, "beta", beta, "2026-02-25T00:00:00Z")

        # Replace alpha with empty — beta should be unaffected
        save_queue_entries(conn, "alpha", [], "2026-02-25T01:00:00Z")

        alpha_rows = conn.execute(
            "SELECT * FROM queue_entries WHERE project = ?", ("alpha",)
        ).fetchall()
        beta_rows = conn.execute(
            "SELECT * FROM queue_entries WHERE project = ?", ("beta",)
        ).fetchall()
        assert len(alpha_rows) == 0
        assert len(beta_rows) == 1
        assert beta_rows[0]["ticket_id"] == "SMA-2"

    def test_save_blocked_by_persisted(self, conn):
        """blocked_by list should be stored as JSON."""
        candidates = [
            _FakeIssue("SMA-1", "Blocked", 2, 1.0, "https://linear.app/1", blocked_by=["SMA-10", "SMA-11"]),
        ]
        save_queue_entries(conn, "proj", candidates, "2026-02-25T00:00:00Z")
        rows = conn.execute("SELECT * FROM queue_entries WHERE project = ?", ("proj",)).fetchall()
        assert len(rows) == 1
        assert rows[0]["blocked_by"] == '["SMA-10", "SMA-11"]'

    def test_save_unblocked_has_null_blocked_by(self, conn):
        """Unblocked issues should have NULL blocked_by."""
        candidates = [
            _FakeIssue("SMA-1", "Normal", 2, 1.0, "https://linear.app/1", blocked_by=None),
        ]
        save_queue_entries(conn, "proj", candidates, "2026-02-25T00:00:00Z")
        rows = conn.execute("SELECT * FROM queue_entries WHERE project = ?", ("proj",)).fetchall()
        assert rows[0]["blocked_by"] is None

    def test_save_empty_blocked_by_is_null(self, conn):
        """Empty blocked_by list should be stored as NULL."""
        candidates = [
            _FakeIssue("SMA-1", "Normal", 2, 1.0, "https://linear.app/1", blocked_by=[]),
        ]
        save_queue_entries(conn, "proj", candidates, "2026-02-25T00:00:00Z")
        rows = conn.execute("SELECT * FROM queue_entries WHERE project = ?", ("proj",)).fetchall()
        assert rows[0]["blocked_by"] is None


# ---------------------------------------------------------------------------
# Per-project pause state
# ---------------------------------------------------------------------------


class TestProjectPauseState:
    def test_load_empty(self, conn):
        """No project pause states initially."""
        result = load_all_project_pause_states(conn)
        assert result == {}

    def test_save_and_load(self, conn):
        """Save a paused project and load it back."""
        save_project_pause_state(conn, project="proj-a", paused=True, reason="manual")
        conn.commit()
        result = load_all_project_pause_states(conn)
        assert result == {"proj-a": (True, "manual")}

    def test_save_multiple_projects(self, conn):
        """Multiple projects can have independent pause states."""
        save_project_pause_state(conn, project="proj-a", paused=True, reason="testing")
        save_project_pause_state(conn, project="proj-b", paused=False)
        conn.commit()
        result = load_all_project_pause_states(conn)
        assert result["proj-a"] == (True, "testing")
        assert result["proj-b"] == (False, None)

    def test_upsert_updates_existing(self, conn):
        """Saving again for the same project updates the row."""
        save_project_pause_state(conn, project="proj-a", paused=True, reason="first")
        conn.commit()
        save_project_pause_state(conn, project="proj-a", paused=False)
        conn.commit()
        result = load_all_project_pause_states(conn)
        assert result["proj-a"] == (False, None)

    def test_reason_cleared_on_unpause(self, conn):
        """Reason is cleared when project is unpaused."""
        save_project_pause_state(conn, project="proj-a", paused=True, reason="manual")
        conn.commit()
        save_project_pause_state(conn, project="proj-a", paused=False)
        conn.commit()
        result = load_all_project_pause_states(conn)
        assert result["proj-a"][1] is None


# ---------------------------------------------------------------------------
# Workflow definition tables (pipeline_templates, stage_templates, stage_loops)
# ---------------------------------------------------------------------------


class TestWorkflowDefinitionTables:
    """Verify migration 015 creates tables, seeds data, and enforces constraints."""

    def test_tables_exist(self, conn):
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "pipeline_templates" in tables
        assert "stage_templates" in tables
        assert "stage_loops" in tables

    # -- pipeline_templates seed data --

    def test_implementation_pipeline_seeded(self, conn):
        row = conn.execute(
            "SELECT * FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()
        assert row is not None
        assert row["is_default"] == 1
        assert row["ticket_label"] is None

    def test_investigation_pipeline_seeded(self, conn):
        row = conn.execute(
            "SELECT * FROM pipeline_templates WHERE name = 'investigation'"
        ).fetchone()
        assert row is not None
        assert row["is_default"] == 0
        assert row["ticket_label"] == "Investigation"

    def test_pipeline_count(self, conn):
        count = conn.execute("SELECT COUNT(*) FROM pipeline_templates").fetchone()[0]
        assert count == 2

    # -- stage_templates seed data --

    def test_implementation_stages_count(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        count = conn.execute(
            "SELECT COUNT(*) FROM stage_templates WHERE pipeline_id = ?",
            (impl_id,),
        ).fetchone()[0]
        assert count == 7

    def test_implementation_stage_order(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        stages = conn.execute(
            "SELECT name, stage_order FROM stage_templates WHERE pipeline_id = ? ORDER BY stage_order",
            (impl_id,),
        ).fetchall()
        names = [s["name"] for s in stages]
        assert names == ["implement", "review", "fix", "pr_checks", "ci_fix", "merge", "resolve_conflict"]

    def test_investigation_stages_count(self, conn):
        inv_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'investigation'"
        ).fetchone()["id"]
        count = conn.execute(
            "SELECT COUNT(*) FROM stage_templates WHERE pipeline_id = ?",
            (inv_id,),
        ).fetchone()[0]
        assert count == 1

    def test_implement_stage_properties(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? AND name = 'implement'",
            (impl_id,),
        ).fetchone()
        assert row["executor_type"] == "claude"
        assert row["identity"] == "coder"
        assert row["max_turns"] == 200
        assert row["timeout_minutes"] == 120
        assert row["result_parser"] == "pr_url"
        assert "{ticket_id}" in row["prompt_template"]

    def test_review_stage_properties(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? AND name = 'review'",
            (impl_id,),
        ).fetchone()
        assert row["executor_type"] == "claude"
        assert row["identity"] == "reviewer"
        assert row["max_turns"] == 100
        assert row["result_parser"] == "review_verdict"
        assert "{pr_url}" in row["prompt_template"]

    def test_pr_checks_stage_properties(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? AND name = 'pr_checks'",
            (impl_id,),
        ).fetchone()
        assert row["executor_type"] == "shell"
        assert row["shell_command"] == "gh pr checks {pr_url} --watch"
        assert row["prompt_template"] is None
        assert row["identity"] is None

    def test_merge_stage_properties(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? AND name = 'merge'",
            (impl_id,),
        ).fetchone()
        assert row["executor_type"] == "internal"
        assert row["prompt_template"] is None

    def test_ci_fix_stage_properties(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? AND name = 'ci_fix'",
            (impl_id,),
        ).fetchone()
        assert row["executor_type"] == "claude"
        assert row["identity"] == "coder"
        assert "{ci_failure_output}" in row["prompt_template"]
        assert "{pr_url}" in row["prompt_template"]

    def test_investigation_implement_stage(self, conn):
        inv_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'investigation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? AND name = 'implement'",
            (inv_id,),
        ).fetchone()
        assert row["executor_type"] == "claude"
        assert row["identity"] == "coder"
        assert row["max_turns"] == 200
        assert row["timeout_minutes"] == 30
        assert "{ticket_id}" in row["prompt_template"]
        assert "investigation" in row["prompt_template"].lower()

    def test_investigation_prompt_includes_dependency_instruction(self, conn):
        inv_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'investigation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? AND name = 'implement'",
            (inv_id,),
        ).fetchone()
        prompt = row["prompt_template"]
        assert "blockedBy" in prompt
        assert "blocks" in prompt
        assert "save_issue" in prompt

    def test_migration_019_preserves_custom_investigation_prompt(self, conn):
        """Migration 019 must not overwrite a user-customized prompt."""
        from pathlib import Path

        inv_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'investigation'"
        ).fetchone()["id"]
        custom = "My custom investigation prompt for {ticket_id}."
        conn.execute(
            "UPDATE stage_templates SET prompt_template = ? "
            "WHERE pipeline_id = ? AND name = 'implement'",
            (custom, inv_id),
        )
        # Re-run migration 019 SQL — should be a no-op on custom prompts
        migration_path = (
            Path(__file__).resolve().parent.parent
            / "botfarm" / "migrations" / "019_investigation_dependency_instruction.sql"
        )
        sql = migration_path.read_text()
        # Strip comment lines for execution
        stmts = [l for l in sql.splitlines() if not l.startswith("--")]
        conn.executescript("\n".join(stmts))
        row = conn.execute(
            "SELECT prompt_template FROM stage_templates "
            "WHERE pipeline_id = ? AND name = 'implement'",
            (inv_id,),
        ).fetchone()
        assert row["prompt_template"] == custom

    # -- stage_loops seed data --

    def test_implementation_loops_count(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        count = conn.execute(
            "SELECT COUNT(*) FROM stage_loops WHERE pipeline_id = ?",
            (impl_id,),
        ).fetchone()[0]
        assert count == 3

    def test_review_loop(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_loops WHERE pipeline_id = ? AND name = 'review_loop'",
            (impl_id,),
        ).fetchone()
        assert row["start_stage"] == "review"
        assert row["end_stage"] == "fix"
        assert row["max_iterations"] == 3
        assert row["config_key"] == "max_review_iterations"
        assert row["exit_condition"] == "review_approved"
        assert row["on_failure_stage"] is None

    def test_ci_retry_loop(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        row = conn.execute(
            "SELECT * FROM stage_loops WHERE pipeline_id = ? AND name = 'ci_retry_loop'",
            (impl_id,),
        ).fetchone()
        assert row["start_stage"] == "ci_fix"
        assert row["end_stage"] == "pr_checks"
        assert row["max_iterations"] == 2
        assert row["config_key"] == "max_ci_retries"
        assert row["exit_condition"] == "ci_passed"
        assert row["on_failure_stage"] == "ci_fix"

    def test_investigation_no_loops(self, conn):
        inv_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'investigation'"
        ).fetchone()["id"]
        count = conn.execute(
            "SELECT COUNT(*) FROM stage_loops WHERE pipeline_id = ?",
            (inv_id,),
        ).fetchone()[0]
        assert count == 0

    # -- schema constraints --

    def test_pipeline_name_unique(self, conn):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO pipeline_templates (name, is_default) VALUES ('implementation', 0)"
            )

    def test_stage_name_unique_per_pipeline(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type) "
                "VALUES (?, 'implement', 99, 'claude')",
                (impl_id,),
            )

    def test_stage_order_unique_per_pipeline(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type) "
                "VALUES (?, 'new_stage', 1, 'claude')",
                (impl_id,),
            )

    def test_loop_name_unique_per_pipeline(self, conn):
        impl_id = conn.execute(
            "SELECT id FROM pipeline_templates WHERE name = 'implementation'"
        ).fetchone()["id"]
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO stage_loops (pipeline_id, name, start_stage, end_stage, max_iterations) "
                "VALUES (?, 'review_loop', 'review', 'fix', 5)",
                (impl_id,),
            )

    def test_stage_foreign_key(self, conn):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type) "
                "VALUES (9999, 'orphan', 1, 'claude')"
            )

    def test_loop_foreign_key(self, conn):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO stage_loops (pipeline_id, name, start_stage, end_stage, max_iterations) "
                "VALUES (9999, 'orphan', 'a', 'b', 1)"
            )

    # -- migration on existing DB --

    def test_migration_applies_on_existing_db(self, tmp_path):
        """Simulate migrating from version 14 to 15."""
        db_file = tmp_path / "existing.db"
        c = init_db(db_file, allow_migration=True)
        # Verify tables exist and seed data is present
        count = c.execute("SELECT COUNT(*) FROM pipeline_templates").fetchone()[0]
        assert count == 2
        count = c.execute("SELECT COUNT(*) FROM stage_templates").fetchone()[0]
        assert count == 8  # 7 impl + 1 investigation
        count = c.execute("SELECT COUNT(*) FROM stage_loops").fetchone()[0]
        assert count == 3
        c.close()


# ---------------------------------------------------------------------------
# Capacity tracking (migration 016)
# ---------------------------------------------------------------------------


class TestCapacityTracking:
    """Verify migration 016 columns and save/load helpers."""

    def test_columns_exist(self, conn):
        cols = {r[1] for r in conn.execute("PRAGMA table_info(dispatch_state)").fetchall()}
        assert "linear_issue_count" in cols
        assert "linear_issue_limit" in cols
        assert "linear_capacity_checked_at" in cols
        assert "linear_capacity_by_project" in cols

    def test_default_limit(self, conn):
        conn.execute(
            "INSERT INTO dispatch_state (id, paused) VALUES (1, 0) "
            "ON CONFLICT(id) DO NOTHING"
        )
        conn.commit()
        row = conn.execute(
            "SELECT linear_issue_limit FROM dispatch_state WHERE id = 1"
        ).fetchone()
        assert row["linear_issue_limit"] == 250

    def test_load_returns_none_when_empty(self, conn):
        result = load_capacity_state(conn)
        assert result is None

    def test_load_returns_none_when_no_count(self, conn):
        save_dispatch_state(conn, paused=False)
        conn.commit()
        result = load_capacity_state(conn)
        assert result is None

    def test_save_and_load(self, conn):
        by_project = {"Project A": 50, "Project B": 30}
        save_capacity_state(
            conn,
            issue_count=80,
            limit=250,
            by_project=by_project,
            checked_at="2026-03-01T10:00:00Z",
        )
        conn.commit()
        result = load_capacity_state(conn)
        assert result is not None
        assert result["issue_count"] == 80
        assert result["limit"] == 250
        assert result["by_project"] == {"Project A": 50, "Project B": 30}
        assert result["checked_at"] == "2026-03-01T10:00:00Z"

    def test_save_updates_existing(self, conn):
        save_capacity_state(
            conn,
            issue_count=80,
            limit=250,
            by_project={"A": 80},
            checked_at="2026-03-01T10:00:00Z",
        )
        conn.commit()
        save_capacity_state(
            conn,
            issue_count=100,
            limit=250,
            by_project={"A": 60, "B": 40},
            checked_at="2026-03-01T11:00:00Z",
        )
        conn.commit()
        result = load_capacity_state(conn)
        assert result["issue_count"] == 100
        assert result["by_project"] == {"A": 60, "B": 40}
        assert result["checked_at"] == "2026-03-01T11:00:00Z"

    def test_save_preserves_dispatch_pause_state(self, conn):
        save_dispatch_state(conn, paused=True, reason="limit hit")
        conn.commit()
        save_capacity_state(
            conn,
            issue_count=200,
            limit=250,
            by_project={},
            checked_at="2026-03-01T10:00:00Z",
        )
        conn.commit()
        paused, reason, _ = load_dispatch_state(conn)
        assert paused is True
        assert reason == "limit hit"

    def test_dispatch_save_preserves_capacity(self, conn):
        save_capacity_state(
            conn,
            issue_count=200,
            limit=250,
            by_project={"X": 200},
            checked_at="2026-03-01T10:00:00Z",
        )
        conn.commit()
        save_dispatch_state(conn, paused=False)
        conn.commit()
        result = load_capacity_state(conn)
        assert result is not None
        assert result["issue_count"] == 200


# ---------------------------------------------------------------------------
# Ticket history helpers
# ---------------------------------------------------------------------------


class TestTicketHistoryTable:
    def test_table_exists(self, conn):
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "ticket_history" in tables

    def test_indexes_exist(self, conn):
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "idx_ticket_history_project" in indexes
        assert "idx_ticket_history_status" in indexes
        assert "idx_ticket_history_captured_at" in indexes


class TestUpsertTicketHistory:
    def test_insert_new_entry(self, conn):
        upsert_ticket_history(
            conn,
            ticket_id="SMA-100",
            title="Test ticket",
            capture_source="dispatch",
        )
        conn.commit()
        row = get_ticket_history_entry(conn, "SMA-100")
        assert row is not None
        assert row["title"] == "Test ticket"
        assert row["capture_source"] == "dispatch"

    def test_update_existing_entry(self, conn):
        upsert_ticket_history(
            conn,
            ticket_id="SMA-100",
            title="Original title",
            capture_source="dispatch",
        )
        conn.commit()
        upsert_ticket_history(
            conn,
            ticket_id="SMA-100",
            title="Updated title",
            capture_source="completion",
            pr_url="https://github.com/test/pr/1",
        )
        conn.commit()
        row = get_ticket_history_entry(conn, "SMA-100")
        assert row["title"] == "Updated title"
        assert row["capture_source"] == "completion"
        assert row["pr_url"] == "https://github.com/test/pr/1"

    def test_all_fields(self, conn):
        upsert_ticket_history(
            conn,
            ticket_id="SMA-200",
            linear_uuid="uuid-123",
            title="Full ticket",
            description="# Description\nSome markdown",
            status="In Progress",
            priority=2,
            url="https://linear.app/test/SMA-200",
            assignee_name="Bot",
            assignee_email="bot@test.com",
            creator_name="User",
            project_name="Test Project",
            team_name="Team A",
            estimate=3.0,
            due_date="2026-03-15",
            parent_id="SMA-199",
            children_ids=["SMA-201", "SMA-202"],
            blocked_by=["SMA-198"],
            blocks=["SMA-203"],
            labels=["bug", "urgent"],
            comments_json=[{"body": "comment", "author": "user", "created_at": "2026-01-01"}],
            pr_url="https://github.com/test/pr/1",
            branch_name="feature-branch",
            linear_created_at="2026-01-01T00:00:00Z",
            linear_updated_at="2026-02-01T00:00:00Z",
            linear_completed_at=None,
            capture_source="dispatch",
            raw_json='{"id": "uuid-123"}',
        )
        conn.commit()
        row = get_ticket_history_entry(conn, "SMA-200")
        assert row["linear_uuid"] == "uuid-123"
        assert row["description"] == "# Description\nSome markdown"
        assert row["priority"] == 2
        assert row["project_name"] == "Test Project"
        assert row["branch_name"] == "feature-branch"

    def test_json_list_fields_serialized(self, conn):
        upsert_ticket_history(
            conn,
            ticket_id="SMA-300",
            title="JSON test",
            capture_source="dispatch",
            labels=["bug", "p1"],
            children_ids=["SMA-301"],
        )
        conn.commit()
        row = get_ticket_history_entry(conn, "SMA-300")
        import json
        assert json.loads(row["labels"]) == ["bug", "p1"]
        assert json.loads(row["children_ids"]) == ["SMA-301"]

    def test_requires_ticket_id(self, conn):
        with pytest.raises(ValueError, match="ticket_id"):
            upsert_ticket_history(conn, title="No ID", capture_source="dispatch")

    def test_requires_capture_source(self, conn):
        with pytest.raises(ValueError, match="capture_source"):
            upsert_ticket_history(conn, ticket_id="SMA-1", title="No source")


class TestGetTicketHistoryList:
    def _seed(self, conn):
        for i in range(5):
            upsert_ticket_history(
                conn,
                ticket_id=f"SMA-{i + 1}",
                title=f"Ticket {i + 1}",
                capture_source="dispatch",
                project_name="Project A" if i < 3 else "Project B",
                status="Done" if i < 2 else "In Progress",
            )
        conn.commit()

    def test_list_all(self, conn):
        self._seed(conn)
        rows = get_ticket_history_list(conn)
        assert len(rows) == 5

    def test_filter_by_project(self, conn):
        self._seed(conn)
        rows = get_ticket_history_list(conn, project="Project A")
        assert len(rows) == 3

    def test_filter_by_status(self, conn):
        self._seed(conn)
        rows = get_ticket_history_list(conn, status="Done")
        assert len(rows) == 2

    def test_search_by_title(self, conn):
        self._seed(conn)
        rows = get_ticket_history_list(conn, search="Ticket 3")
        assert len(rows) == 1
        assert rows[0]["ticket_id"] == "SMA-3"

    def test_search_by_ticket_id(self, conn):
        self._seed(conn)
        rows = get_ticket_history_list(conn, search="SMA-4")
        assert len(rows) == 1

    def test_pagination(self, conn):
        self._seed(conn)
        rows = get_ticket_history_list(conn, limit=2, offset=0)
        assert len(rows) == 2
        rows2 = get_ticket_history_list(conn, limit=2, offset=2)
        assert len(rows2) == 2

    def test_count(self, conn):
        self._seed(conn)
        assert count_ticket_history(conn) == 5
        assert count_ticket_history(conn, project="Project A") == 3
        assert count_ticket_history(conn, status="Done") == 2
        assert count_ticket_history(conn, search="SMA-5") == 1


class TestMarkDeletedFromLinear:
    def test_marks_as_deleted(self, conn):
        upsert_ticket_history(
            conn,
            ticket_id="SMA-99",
            title="Will be deleted",
            capture_source="dispatch",
        )
        conn.commit()
        assert get_ticket_history_entry(conn, "SMA-99")["deleted_from_linear"] == 0
        mark_deleted_from_linear(conn, "SMA-99")
        conn.commit()
        assert get_ticket_history_entry(conn, "SMA-99")["deleted_from_linear"] == 1
