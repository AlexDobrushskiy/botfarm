"""Shared pytest fixtures for botfarm unit tests."""

from __future__ import annotations

import json as _json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from botfarm.dashboard import create_app
from botfarm.db import (
    init_db,
    insert_event,
    insert_stage_run,
    insert_task,
    insert_usage_snapshot,
    save_dispatch_state,
    update_task,
    upsert_slot,
)
from botfarm.linear import PollResult
from botfarm.supervisor import Supervisor
from tests.helpers import make_config, seed_slot as _seed_slot


@pytest.fixture()
def conn(tmp_path):
    """Yield a fresh DB connection with all migrations applied."""
    db_file = tmp_path / "test.db"
    connection = init_db(db_file, allow_migration=True)
    yield connection
    connection.close()


@pytest.fixture()
def tmp_config(tmp_path):
    """Return a BotfarmConfig and ensure directories exist."""
    config = make_config(tmp_path)
    (tmp_path / "repo").mkdir()
    return config


@pytest.fixture()
def supervisor(tmp_config, tmp_path, monkeypatch):
    """Create a Supervisor with mocked pollers (no real Linear calls)."""
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
    mock_poller.is_issue_terminal.return_value = False

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

    return sup


# ---------------------------------------------------------------------------
# Dashboard shared helpers & fixtures
# ---------------------------------------------------------------------------

def _seed_queue_entry(conn, project, position, ticket_id, ticket_title, priority=3, url="", snapshot_at="2026-02-25T12:00:00+00:00", blocked_by=None):
    """Helper to seed a queue entry row in the DB."""
    blocked_by_json = _json.dumps(blocked_by) if blocked_by else None
    conn.execute(
        "INSERT INTO queue_entries (project, position, ticket_id, ticket_title, priority, sort_order, url, snapshot_at, blocked_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (project, position, ticket_id, ticket_title, priority, 0.0, url or f"https://linear.app/issue/{ticket_id}", snapshot_at, blocked_by_json),
    )
    conn.commit()


@pytest.fixture()
def db_file(tmp_path):
    """Create a database with sample task data, slot state, and usage snapshots."""
    path = tmp_path / "botfarm.db"
    conn = init_db(path)

    _seed_slot(
        conn, "my-project", 1, status="busy",
        ticket_id="TST-1", ticket_title="Fix bug", branch="fix-bug",
        stage="implement", stage_iteration=1,
        started_at="2026-02-12T10:00:00+00:00", pid=1234,
    )
    _seed_slot(conn, "my-project", 2, status="free")

    save_dispatch_state(conn, paused=False)

    insert_usage_snapshot(
        conn,
        utilization_5h=0.45,
        utilization_7d=0.72,
        resets_at="2026-02-12T15:00:00+00:00",
    )

    task_id = insert_task(
        conn,
        ticket_id="TST-1",
        title="Fix bug",
        project="my-project",
        slot=1,
        status="completed",
    )
    update_task(
        conn,
        task_id,
        started_at="2026-02-12T10:00:00+00:00",
        completed_at="2026-02-12T11:30:00+00:00",
        turns=42,
        review_iterations=2,
    )
    insert_stage_run(
        conn,
        task_id=task_id,
        stage="implement",
        iteration=1,
        session_id="sess-abc123def456",
        turns=30,
        duration_seconds=3600.0,
        input_tokens=50000,
        output_tokens=15000,
        total_cost_usd=0.1234,
        context_fill_pct=72.5,
    )
    insert_stage_run(
        conn,
        task_id=task_id,
        stage="review",
        iteration=1,
        turns=12,
        duration_seconds=1800.0,
        exit_subtype="approved",
        input_tokens=20000,
        output_tokens=5000,
        total_cost_usd=0.0456,
        context_fill_pct=45.3,
    )
    insert_event(conn, task_id=task_id, event_type="stage_started", detail="implement")
    insert_event(conn, task_id=task_id, event_type="stage_completed", detail="implement")
    insert_event(conn, task_id=task_id, event_type="pr_created", detail="https://github.com/org/repo/pull/1")

    task_id2 = insert_task(
        conn,
        ticket_id="TST-2",
        title="Add feature",
        project="other-project",
        slot=2,
        status="failed",
    )
    update_task(conn, task_id2, failure_reason="Tests failed", turns=10)

    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def client(db_file):
    """FastAPI test client."""
    app = create_app(db_path=db_file)
    return TestClient(app)
