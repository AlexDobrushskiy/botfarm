"""Tests for botfarm.dashboard module."""

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import (
    BotfarmConfig,
    CoderIdentity,
    DashboardConfig,
    IdentitiesConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
    ReviewerIdentity,
)
from botfarm.dashboard import build_pipeline_state, create_app, format_codex_ndjson_line, format_ndjson_line, start_dashboard
from botfarm.db import (
    count_tasks,
    get_distinct_projects,
    get_task_history,
    init_db,
    insert_event,
    insert_stage_run,
    insert_task,
    insert_usage_snapshot,
    update_task,
    upsert_slot,
    save_dispatch_state,
    load_all_slots,
    load_dispatch_state,
    save_queue_entries,
)


def _seed_queue_entry(conn, project, position, ticket_id, ticket_title, priority=3, url="", snapshot_at="2026-02-25T12:00:00+00:00", blocked_by=None):
    """Helper to seed a queue entry row in the DB."""
    import json as _json
    blocked_by_json = _json.dumps(blocked_by) if blocked_by else None
    conn.execute(
        "INSERT INTO queue_entries (project, position, ticket_id, ticket_title, priority, sort_order, url, snapshot_at, blocked_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (project, position, ticket_id, ticket_title, priority, 0.0, url or f"https://linear.app/issue/{ticket_id}", snapshot_at, blocked_by_json),
    )
    conn.commit()


def _seed_slot(conn, project, slot_id, status="free", **overrides):
    """Helper to seed a slot row in the DB."""
    slot = {
        "project": project, "slot_id": slot_id, "status": status,
        "ticket_id": None, "ticket_title": None, "branch": None,
        "pr_url": None, "stage": None, "stage_iteration": 0,
        "current_session_id": None, "started_at": None,
        "stage_started_at": None, "sigterm_sent_at": None,
        "pid": None, "interrupted_by_limit": False,
        "resume_after": None, "stages_completed": [],
    }
    slot.update(overrides)
    upsert_slot(conn, slot)


@pytest.fixture()
def db_file(tmp_path):
    """Create a database with sample task data, slot state, and usage snapshots."""
    path = tmp_path / "botfarm.db"
    conn = init_db(path)

    # --- Slot data (was in state.json) ---
    _seed_slot(
        conn, "my-project", 1, status="busy",
        ticket_id="TST-1", ticket_title="Fix bug", branch="fix-bug",
        stage="implement", stage_iteration=1,
        started_at="2026-02-12T10:00:00+00:00", pid=1234,
    )
    _seed_slot(conn, "my-project", 2, status="free")

    # --- Dispatch state (was in state.json) ---
    save_dispatch_state(conn, paused=False)

    # --- Usage snapshot (was in state.json "usage" + "last_usage_check") ---
    insert_usage_snapshot(
        conn,
        utilization_5h=0.45,
        utilization_7d=0.72,
        resets_at="2026-02-12T15:00:00+00:00",
    )

    # --- Task history data ---
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


# --- create_app ---


class TestCreateApp:
    def test_app_created(self, db_file):
        app = create_app(db_path=db_file)
        assert app.title == "Botfarm Dashboard"

    def test_no_docs_endpoints(self, client):
        resp = client.get("/docs")
        assert resp.status_code == 404

        resp = client.get("/redoc")
        assert resp.status_code == 404


# --- Index / Live Status ---


class TestIndexPage:
    def test_index_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_index_contains_slot_data(self, client):
        resp = client.get("/")
        body = resp.text
        assert "TST-1" in body
        assert "my-project" in body
        assert "busy" in body
        assert "free" in body

    def test_index_contains_usage_data(self, client):
        resp = client.get("/")
        body = resp.text
        assert "45.0%" in body
        assert "72.0%" in body

    def test_index_contains_htmx_triggers(self, client):
        resp = client.get("/")
        body = resp.text
        assert "hx-get" in body
        assert "hx-trigger" in body

    def test_index_update_banner_loads_immediately(self, client):
        """Update banner must fetch on page load, not just on the poll interval."""
        resp = client.get("/")
        body = resp.text
        assert 'id="update-banner"' in body
        assert 'hx-trigger="load, every 60s"' in body

    def test_index_contains_navigation(self, client):
        resp = client.get("/")
        body = resp.text
        assert "Live Status" in body
        assert "Task History" in body
        assert "Usage Trends" in body
        assert "Metrics" in body


class TestIndexNoState:
    def test_index_no_state_file(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "No slots configured" in resp.text


# --- Partials ---


class TestPartialSlots:
    def test_returns_200(self, client):
        resp = client.get("/partials/slots")
        assert resp.status_code == 200

    def test_contains_slot_table(self, client):
        resp = client.get("/partials/slots")
        assert "TST-1" in resp.text
        assert "implement" in resp.text

    def test_context_fill_column_present(self, client):
        resp = client.get("/partials/slots")
        assert "Context Fill" in resp.text

    def test_busy_slot_shows_context_fill(self, client):
        """Busy slot with stage run data should show context fill %."""
        resp = client.get("/partials/slots")
        body = resp.text
        # TST-1 is busy, and has stage runs with context_fill_pct
        # The most recent is review with 45.3%
        assert "45.3%" in body or "72.5%" in body

    def test_dispatch_paused_banner(self, tmp_path):
        db_path = tmp_path / "paused.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=True, reason="5-hour limit exceeded",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "DISPATCH PAUSED" in resp.text
        assert "5-hour limit exceeded" in resp.text


class TestPartialUsage:
    @pytest.fixture(autouse=True)
    def _mock_refresh(self, monkeypatch):
        """Prevent real API calls during usage partial tests."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None
        )

    def test_returns_200(self, client):
        resp = client.get("/partials/usage")
        assert resp.status_code == 200

    def test_contains_usage_percentages(self, client):
        resp = client.get("/partials/usage")
        assert "45.0%" in resp.text
        assert "72.0%" in resp.text


# --- Partial Queue ---


class TestPartialQueue:
    def test_returns_200(self, client):
        resp = client.get("/partials/queue")
        assert resp.status_code == 200

    def test_contains_no_work_available(self, client):
        """Queue data is no longer available from DB; expect 'No work available'."""
        resp = client.get("/partials/queue")
        body = resp.text
        assert "No work available" in body

    def test_queue_no_data(self, tmp_path):
        """With an empty DB, queue should show 'No work available'."""
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/queue")
        assert resp.status_code == 200
        assert "No work available" in resp.text

    def test_no_queue_data(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/partials/queue")
        assert resp.status_code == 200
        assert "No work available" in resp.text

    def test_empty_queue_projects(self, tmp_path):
        """With no queue source in DB, should show 'No work available'."""
        db_path = tmp_path / "nq.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/queue")
        assert resp.status_code == 200
        assert "No work available" in resp.text

    def test_queue_displays_entries(self, tmp_path):
        """Queue entries from DB should render per-project tables."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "alpha", 0, "ALP-1", "First ticket", priority=1)
        _seed_queue_entry(conn, "alpha", 1, "ALP-2", "Second ticket", priority=3)
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        assert resp.status_code == 200
        body = resp.text
        assert "alpha" in body
        assert "ALP-1" in body
        assert "ALP-2" in body
        assert "First ticket" in body
        assert "Second ticket" in body

    def test_queue_next_up_highlighted(self, tmp_path):
        """Position 0 should show 'Next' marker."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "P-1", "Top ticket")
        _seed_queue_entry(conn, "proj", 1, "P-2", "Other ticket")
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "<mark>Next</mark>" in body

    def test_queue_priority_labels(self, tmp_path):
        """Priority integers should render as text labels."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "P-1", "Urgent task", priority=1)
        _seed_queue_entry(conn, "proj", 1, "P-2", "High task", priority=2)
        _seed_queue_entry(conn, "proj", 2, "P-3", "Low task", priority=4)
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "Urgent" in body
        assert "High" in body
        assert "Low" in body

    def test_queue_multiple_projects(self, tmp_path):
        """Multiple projects should each get their own section."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "alpha", 0, "A-1", "Alpha task")
        _seed_queue_entry(conn, "beta", 0, "B-1", "Beta task")
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "alpha" in body
        assert "beta" in body
        assert "A-1" in body
        assert "B-1" in body

    def test_queue_ticket_links(self, tmp_path):
        """Ticket IDs should be linked to Linear."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "TST-99", "Linked ticket")
        conn.close()
        app = create_app(db_path=db_path, linear_workspace="myws")
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "https://linear.app/myws/issue/TST-99" in body

    def test_queue_empty_project_from_config(self, tmp_path):
        """Configured projects with no queue entries should show empty state."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "alpha", 0, "A-1", "Alpha task")
        conn.close()
        cfg = BotfarmConfig(projects=[
            ProjectConfig(name="alpha", linear_team="T", base_dir="/tmp", worktree_prefix="w", slots=[1]),
            ProjectConfig(name="beta", linear_team="T", base_dir="/tmp", worktree_prefix="w", slots=[2]),
        ])
        app = create_app(db_path=db_path, botfarm_config=cfg)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "alpha" in body
        assert "beta" in body
        assert "No tickets in queue" in body

    def test_queue_snapshot_timestamp(self, tmp_path):
        """Snapshot timestamp should appear as 'Last polled' text."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "P-1", "Task", snapshot_at="2026-02-25T12:00:00+00:00")
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "Last polled:" in body

    def test_queue_blocked_entry_greyed_out(self, tmp_path):
        """Blocked entries should render with reduced opacity."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "P-1", "Blocked task", blocked_by=["P-0"])
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "opacity: 0.45" in body
        assert "blocked by P-0" in body

    def test_queue_blocked_entry_no_next_marker(self, tmp_path):
        """Blocked entry at position 0 should NOT get the 'Next' marker."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "P-1", "Blocked first", blocked_by=["P-0"])
        _seed_queue_entry(conn, "proj", 1, "P-2", "Unblocked second")
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        # P-2 (unblocked) should be marked Next, not P-1 (blocked)
        assert "<mark>Next</mark>" in body
        # The lock icon should appear for the blocked entry (rendered as HTML entity)
        assert "&#x1F512;" in body

    def test_queue_blocked_shows_multiple_blockers(self, tmp_path):
        """Blocked entry should list all blocking ticket IDs."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "P-3", "Multi-blocked", blocked_by=["P-1", "P-2"])
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "P-1" in body
        assert "P-2" in body
        assert "blocked by P-1, P-2" in body

    def test_queue_unblocked_entry_no_blocked_styling(self, tmp_path):
        """Unblocked entries should not have blocked styling."""
        db_path = tmp_path / "q.db"
        conn = init_db(db_path)
        _seed_queue_entry(conn, "proj", 0, "P-1", "Normal task")
        conn.close()
        app = create_app(db_path=db_path)
        c = TestClient(app)
        resp = c.get("/partials/queue")
        body = resp.text
        assert "opacity: 0.45" not in body
        assert "blocked by" not in body


# --- Slot Panel enhancements ---


class TestSlotPanelEnhancements:
    def test_links_header_present(self, client):
        resp = client.get("/partials/slots")
        assert "<th>Links</th>" in resp.text

    def test_progress_header_present(self, client):
        resp = client.get("/partials/slots")
        assert "<th>Progress</th>" in resp.text

    def test_ticket_link(self, client):
        resp = client.get("/partials/slots")
        assert "linear.app/issue/TST-1" in resp.text

    def test_ticket_link_with_workspace(self, db_file):
        app = create_app(
            db_path=db_file,
            linear_workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_paused_slot_resume_countdown(self, tmp_path):
        db_path = tmp_path / "paused_slot.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "my-project", 1, status="paused_limit",
            ticket_id="TST-1", ticket_title="Fix bug",
            resume_after="2099-12-31T23:59:00+00:00",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "resume-countdown" in resp.text
        assert "2099-12-31T23:59:00" in resp.text

    def test_task_detail_link(self, client):
        """Busy slot with ticket_id shows task detail link."""
        resp = client.get("/partials/slots")
        assert '/task/TST-1' in resp.text
        assert ">detail</a>" in resp.text

    def test_pr_link_shown(self, tmp_path):
        """Slot with pr_url shows PR link."""
        db_path = tmp_path / "pr_link.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="pr_checks", pr_url="https://github.com/org/repo/pull/42",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "https://github.com/org/repo/pull/42" in resp.text
        assert ">PR</a>" in resp.text

    def test_no_pr_link_when_absent(self, client):
        """Slot without pr_url does not show PR link."""
        resp = client.get("/partials/slots")
        assert ">PR</a>" not in resp.text


class TestSlotPipeline:
    """Tests for the visual stage pipeline progress in the slots table."""

    def test_busy_slot_shows_pipeline(self, tmp_path):
        """Busy slot displays pipeline dots."""
        db_path = tmp_path / "pipeline.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="review", stages_completed=["implement"],
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        body = resp.text
        assert "slot-pipeline" in body
        assert "slot-pipe-dot-completed" in body
        assert "slot-pipe-dot-active" in body
        assert "slot-pipe-dot-pending" in body

    def test_pipeline_completed_stages(self, tmp_path):
        """Completed stages get checkmark class, active gets active class."""
        db_path = tmp_path / "pipeline2.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="fix",
            stages_completed=["implement", "review"],
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        body = resp.text
        # 2 completed dots, 1 active, 2 pending
        assert body.count("slot-pipe-dot-completed") == 2
        assert body.count("slot-pipe-dot-active") == 1
        assert body.count("slot-pipe-dot-pending") == 2

    def test_pipeline_connectors(self, tmp_path):
        """Connectors between stages are colored based on completion."""
        db_path = tmp_path / "connectors.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="fix",
            stages_completed=["implement", "review"],
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        body = resp.text
        # After implement(completed) and review(completed): 2 completed connectors
        # After fix(active) and pr_checks(pending): 2 pending connectors
        assert body.count("slot-pipe-line-completed") == 2
        assert body.count("slot-pipe-line-pending") == 2

    def test_free_slot_no_pipeline(self, tmp_path):
        """Free slot shows dash instead of pipeline."""
        db_path = tmp_path / "free.db"
        conn = init_db(db_path)
        _seed_slot(conn, "proj", 1, status="free")
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "slot-pipeline" not in resp.text

    def test_failed_slot_shows_failed_dot(self, tmp_path):
        """Failed slot shows failed state on current stage."""
        db_path = tmp_path / "failed.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="failed",
            ticket_id="T-1", ticket_title="Test",
            stage="review",
            stages_completed=["implement"],
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "slot-pipe-dot-failed" in resp.text


class TestStageElapsed:
    """Tests for per-stage elapsed time display."""

    def test_stage_elapsed_shown(self, tmp_path):
        """Busy slot with stage_started_at shows stage elapsed time."""
        db_path = tmp_path / "stage_elapsed.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="implement", stage_started_at="2026-02-26T10:00:00+00:00",
            started_at="2026-02-26T09:00:00+00:00",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "stage-elapsed" in resp.text
        assert "stage:" in resp.text

    def test_no_stage_elapsed_for_free_slot(self, tmp_path):
        """Free slot does not show stage elapsed."""
        db_path = tmp_path / "free_elapsed.db"
        conn = init_db(db_path)
        _seed_slot(conn, "proj", 1, status="free")
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "stage-elapsed" not in resp.text


class TestIterationContext:
    """Tests for iteration context display."""

    def test_fix_loop_label(self, tmp_path):
        """Fix stage with iteration > 1 shows 'loop N' badge."""
        db_path = tmp_path / "fix_loop.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="fix", stage_iteration=2,
            stages_completed=["implement", "review"],
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "loop 2" in resp.text
        assert "slot-iter-badge" in resp.text

    def test_pr_checks_retry_label(self, tmp_path):
        """PR checks stage with iteration > 1 shows 'retry N' badge."""
        db_path = tmp_path / "retry.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="pr_checks", stage_iteration=3,
            stages_completed=["implement", "review", "fix"],
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "retry 3" in resp.text

    def test_generic_iter_label(self, tmp_path):
        """Non-fix/pr_checks stage with iteration > 1 shows 'iter N'."""
        db_path = tmp_path / "iter.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="implement", stage_iteration=2,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "iter 2" in resp.text

    def test_no_iter_badge_for_first_iteration(self, tmp_path):
        """First iteration does not show iteration badge."""
        db_path = tmp_path / "no_iter.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="implement", stage_iteration=1,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "slot-iter-badge" not in resp.text

    def test_paused_limit_shows_interrupted_label(self, tmp_path):
        """Paused-limit slot with interrupted_by_limit shows label."""
        db_path = tmp_path / "interrupted.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="paused_limit",
            ticket_id="T-1", ticket_title="Test",
            stage="implement",
            interrupted_by_limit=True,
            resume_after="2099-12-31T23:59:00+00:00",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "limit interrupted" in resp.text
        assert "slot-limit-label" in resp.text


# --- Usage Panel enhancements ---


class TestUsagePanelEnhancements:
    @pytest.fixture(autouse=True)
    def _mock_refresh(self, monkeypatch):
        """Prevent real API calls during usage panel tests."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None
        )

    def test_progress_bars(self, client):
        resp = client.get("/partials/usage")
        assert "<progress" in resp.text

    def test_reset_countdowns(self, client):
        """Reset countdowns appear on the index page (which falls back to DB data)."""
        resp = client.get("/")
        body = resp.text
        # DB stores resets_at as a single field; the index page renders usage
        # from the full-page context which includes DB-sourced data
        assert "45.0%" in body
        assert "72.0%" in body

    def test_last_usage_check(self, client):
        """Last checked appears on the index page (which falls back to DB data)."""
        resp = client.get("/")
        body = resp.text
        assert "Last checked:" in body
        assert "ago" in body

    def test_no_dispatch_pause_banner_in_usage(self, tmp_path, monkeypatch):
        """Dispatch pause banner lives in slots panel only, not usage."""
        db_path = tmp_path / "paused_usage.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=True, reason="5-hour limit exceeded",
        )
        insert_usage_snapshot(
            conn, utilization_5h=0.45, utilization_7d=0.72,
            resets_at="2026-02-12T15:00:00+00:00",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "DISPATCH PAUSED" not in resp.text
        assert "Dispatch paused" not in resp.text


# --- Usage freshness & staleness ---


class TestUsageFreshness:
    """Tests for SMA-111: dashboard usage data freshness fixes."""

    def test_rate_limit_slot_not_claimed_on_failure(
        self, db_file, monkeypatch,
    ):
        """Rate-limit slot should not be consumed when the API call fails.

        After a failed refresh, the next call should retry immediately rather
        than returning stale cached data for up to 60 seconds.
        """
        call_count = 0

        def _failing_refresh(conn):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("API down")

        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", _failing_refresh,
        )
        app = create_app(db_path=db_file)
        client = TestClient(app)

        # First call -- triggers a real refresh attempt which fails
        resp1 = client.get("/partials/usage")
        assert resp1.status_code == 200
        first_call_count = call_count

        # Second call -- should also attempt a refresh (slot was not claimed)
        resp2 = client.get("/partials/usage")
        assert resp2.status_code == 200
        assert call_count > first_call_count, (
            "Expected retry after failure, but rate-limit slot blocked it"
        )

    def test_dashboard_tracks_own_refresh_timestamp(
        self, db_file, monkeypatch,
    ):
        """After a successful refresh, 'Last checked' should reflect the
        dashboard's own timestamp, not the supervisor's."""
        class FakeState:
            def to_dict(self):
                return {"utilization_5h": 0.30, "utilization_7d": 0.60}

        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot",
            lambda conn: FakeState(),
        )

        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        body = resp.text
        # Should show fresh data from our mock, not the DB snapshot usage
        assert "30.0%" in body
        # The "Last checked" should be a recent dashboard timestamp
        assert "Last checked:" in body

    def test_staleness_warning_shown_when_data_old(
        self, tmp_path, monkeypatch,
    ):
        """A visual warning should appear when usage data is older than
        2x the refresh interval (>120 seconds with default 60s interval).

        The index page falls back to DB's last_usage_check (from
        usage_snapshot.created_at), so staleness is testable there.
        """
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None,
        )
        # Create a DB with a usage snapshot that has an old created_at
        from datetime import datetime, timedelta, timezone
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).isoformat()
        db_path = tmp_path / "stale.db"
        conn = init_db(db_path)
        # Insert usage snapshot with old timestamp
        conn.execute(
            "INSERT INTO usage_snapshots (utilization_5h, utilization_7d, resets_at, created_at)"
            " VALUES (?, ?, ?, ?)",
            (0.45, 0.72, "2026-02-12T15:00:00+00:00", old_time),
        )
        conn.commit()
        conn.close()

        app = create_app(db_path=db_path)
        client = TestClient(app)
        # Use index page which falls back to DB's last_usage_check
        resp = client.get("/")
        assert "usage data may be stale" in resp.text

    def test_staleness_warning_on_usage_partial_cold_start(
        self, tmp_path, monkeypatch,
    ):
        """The /partials/usage endpoint should show a staleness warning on
        cold start (before the dashboard refreshes) when the DB snapshot is old.
        """
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None,
        )
        from datetime import datetime, timedelta, timezone
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).isoformat()
        db_path = tmp_path / "stale_partial.db"
        conn = init_db(db_path)
        conn.execute(
            "INSERT INTO usage_snapshots (utilization_5h, utilization_7d, resets_at, created_at)"
            " VALUES (?, ?, ?, ?)",
            (0.45, 0.72, "2026-02-12T15:00:00+00:00", old_time),
        )
        conn.commit()
        conn.close()

        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "usage data may be stale" in resp.text

    def test_no_staleness_warning_when_data_fresh(
        self, db_file, monkeypatch,
    ):
        """No warning when the usage data was refreshed recently."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None,
        )
        # db_file already has a recent usage snapshot (just created)
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "usage data may be stale" not in resp.text

    def test_warning_level_log_on_refresh_failure(
        self, db_file, monkeypatch, caplog,
    ):
        """Refresh failures should log at WARNING level, not DEBUG."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot",
            lambda conn: (_ for _ in ()).throw(RuntimeError("API error")),
        )
        app = create_app(db_path=db_file)
        client = TestClient(app)

        import logging
        with caplog.at_level(logging.WARNING, logger="botfarm.dashboard"):
            client.get("/partials/usage")
        assert any(
            "Dashboard usage refresh failed" in r.message for r in caplog.records
        )
        assert all(
            r.levelno >= logging.WARNING
            for r in caplog.records
            if "Dashboard usage refresh failed" in r.message
        )

    def test_successful_refresh_updates_cached_data(
        self, db_file, monkeypatch,
    ):
        """After a successful API refresh, subsequent rate-limited calls
        should return the fresh data, not None."""
        class FakeState:
            def to_dict(self):
                return {"utilization_5h": 0.55, "utilization_7d": 0.80}

        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot",
            lambda conn: FakeState(),
        )
        app = create_app(db_path=db_file)
        client = TestClient(app)

        # First call -- refreshes from API
        resp1 = client.get("/partials/usage")
        assert "55.0%" in resp1.text

        # Second call within rate-limit window -- should return cached data
        resp2 = client.get("/partials/usage")
        assert "55.0%" in resp2.text


# --- Index Queue panel ---


class TestIndexQueuePanel:
    def test_index_contains_queue_section(self, client):
        resp = client.get("/")
        body = resp.text
        assert "Queue" in body
        assert "queue-panel" in body
        assert "/partials/queue" in body

    def test_index_queue_shows_no_work(self, client):
        """Queue data is no longer in DB; index should show 'No work available'."""
        resp = client.get("/")
        body = resp.text
        assert "No work available" in body


# --- History ---


class TestHistoryPage:
    def test_returns_200(self, client):
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_contains_task_data(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "TST-1" in body
        assert "my-project" in body
        assert "completed" in body
        assert "1.25" in body

    def test_contains_filter_form(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "history-filters" in body
        assert "Search" in body
        assert "Project" in body
        assert "Status" in body

    def test_contains_all_columns(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "Ticket" in body
        assert "Title" in body
        assert "Wall Time" in body
        assert "Reviews" in body
        assert "Limit Hits" in body
        assert "Cost" in body
        assert "Max Ctx Fill" in body

    def test_history_shows_cost_and_context_fill(self, client):
        """History rows should display aggregated cost and max context fill."""
        resp = client.get("/history")
        body = resp.text
        # TST-1: total cost = 0.1234 + 0.0456 = 0.1690
        assert "$0.1690" in body
        # TST-1: max context fill = 72.5%
        assert "72.5%" in body

    def test_project_dropdown_populated(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "my-project" in body
        assert "other-project" in body

    def test_task_rows_are_clickable(self, client):
        resp = client.get("/history")
        assert "/task/" in resp.text

    def test_task_rows_use_ticket_id_urls(self, client):
        resp = client.get("/history")
        assert "/task/TST-1" in resp.text
        assert "/task/TST-2" in resp.text

    def test_ticket_links_to_linear(self, db_file):
        app = create_app(
            db_path=db_file,
            linear_workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/history")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_history_no_db(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "No tasks found" in resp.text


class TestHistoryFilters:
    def test_filter_by_project(self, client):
        resp = client.get("/history?project=my-project")
        body = resp.text
        assert "TST-1" in body
        assert "TST-2" not in body

    def test_filter_by_status(self, client):
        resp = client.get("/history?status=failed")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body

    def test_search_by_ticket_id(self, client):
        resp = client.get("/history?search=TST-2")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body

    def test_search_by_title(self, client):
        resp = client.get("/history?search=Add feature")
        body = resp.text
        assert "TST-2" in body

    def test_sort_by_turns_asc(self, client):
        resp = client.get("/history?sort_by=turns&sort_dir=ASC")
        assert resp.status_code == 200
        body = resp.text
        # TST-2 (10 turns) should appear before TST-1 (42 turns)
        assert body.index("TST-2") < body.index("TST-1")

    def test_sort_by_turns_desc(self, client):
        resp = client.get("/history?sort_by=turns&sort_dir=DESC")
        assert resp.status_code == 200
        body = resp.text
        assert body.index("TST-1") < body.index("TST-2")

    def test_invalid_sort_column_defaults_gracefully(self, client):
        resp = client.get("/history?sort_by=DROP TABLE&sort_dir=ASC")
        assert resp.status_code == 200

    def test_combined_filters(self, client):
        resp = client.get("/history?project=other-project&status=failed")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body


class TestHistoryPagination:
    def test_pagination_info_shown(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "page 1 of 1" in body
        assert "2 tasks found" in body

    def test_page_param(self, client):
        resp = client.get("/history?page=1")
        assert resp.status_code == 200

    def test_invalid_page_defaults_to_1(self, client):
        resp = client.get("/history?page=abc")
        assert resp.status_code == 200

    def test_pagination_with_many_tasks(self, tmp_path):
        """When there are more than 25 tasks, pagination links appear."""
        path = tmp_path / "big.db"
        conn = init_db(path)
        for i in range(30):
            insert_task(
                conn,
                ticket_id=f"BIG-{i}",
                title=f"Task {i}",
                project="bulk",
                slot=1,
            )
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/history")
        body = resp.text
        assert "page 1 of 2" in body
        assert "Next" in body

        resp2 = client.get("/history?page=2")
        body2 = resp2.text
        assert "page 2 of 2" in body2
        assert "Prev" in body2


class TestPartialHistory:
    def test_returns_200(self, client):
        resp = client.get("/partials/history")
        assert resp.status_code == 200

    def test_contains_task_data(self, client):
        resp = client.get("/partials/history")
        assert "TST-1" in resp.text

    def test_partial_respects_filters(self, client):
        resp = client.get("/partials/history?project=other-project")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body


class TestTaskDetailPage:
    def test_returns_200(self, client):
        resp = client.get("/task/1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_contains_task_summary(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "TST-1" in body
        assert "Fix bug" in body
        assert "completed" in body
        assert "1.25" in body
        assert "my-project" in body

    def test_contains_metric_cards(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "Wall Time" in body
        assert "Turns" in body
        assert "Review Iterations" in body

    def test_contains_stage_runs(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "Stage Runs" in body
        assert "implement" in body
        assert "review" in body
        assert "sess-abc123d" in body  # truncated session id
        assert "approved" in body

    def test_contains_event_log(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "Event Log" in body
        assert "stage_started" in body
        assert "stage_completed" in body
        assert "pr_created" in body

    def test_back_link(self, client):
        resp = client.get("/task/1")
        assert "Back to Task History" in resp.text

    def test_ticket_links_to_linear(self, db_file):
        app = create_app(
            db_path=db_file,
            linear_workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/task/1")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_task_not_found(self, client):
        resp = client.get("/task/9999")
        assert resp.status_code == 200
        assert "Task not found" in resp.text

    def test_task_detail_no_db(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/task/1")
        assert resp.status_code == 200
        assert "Task not found" in resp.text

    def test_failed_task_shows_failure_reason(self, client):
        resp = client.get("/task/2")
        body = resp.text
        assert "TST-2" in body
        assert "Tests failed" in body

    def test_task_with_no_stages_or_events(self, tmp_path):
        path = tmp_path / "sparse.db"
        conn = init_db(path)
        insert_task(conn, ticket_id="BARE-1", title="Bare task", project="p", slot=1)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/task/1")
        body = resp.text
        assert "BARE-1" in body
        assert "No stage runs recorded" in body
        assert "No events recorded" in body

    def test_ticket_id_url_resolves(self, client):
        """GET /task/TST-1 resolves via ticket_id lookup."""
        resp = client.get("/task/TST-1")
        assert resp.status_code == 200
        body = resp.text
        assert "TST-1" in body
        assert "Fix bug" in body

    def test_integer_id_still_works(self, client):
        """GET /task/1 still resolves via integer ID (backward compat)."""
        resp = client.get("/task/1")
        assert resp.status_code == 200
        body = resp.text
        assert "TST-1" in body

    def test_nonexistent_ticket_id_shows_not_found(self, client):
        """GET /task/NOPE-999 shows not-found state gracefully."""
        resp = client.get("/task/NOPE-999")
        assert resp.status_code == 200
        assert "Task not found" in resp.text

    def test_second_task_by_ticket_id(self, client):
        """GET /task/TST-2 resolves the failed task."""
        resp = client.get("/task/TST-2")
        assert resp.status_code == 200
        body = resp.text
        assert "TST-2" in body
        assert "Add feature" in body

    def test_contains_token_usage_columns(self, client):
        """Stage runs table should show token usage and cost columns."""
        resp = client.get("/task/1")
        body = resp.text
        assert "Context Fill" in body
        assert "Cost" in body
        assert "Input Tokens" in body
        assert "Output Tokens" in body

    def test_stage_run_token_values(self, client):
        """Stage runs should display actual token/cost values from DB."""
        resp = client.get("/task/1")
        body = resp.text
        assert "50,000" in body  # input_tokens formatted
        assert "15,000" in body  # output_tokens formatted
        assert "$0.1234" in body  # cost
        assert "72.5%" in body  # context_fill_pct

    def test_context_fill_color_coding(self, client):
        """Context fill should be color-coded based on percentage."""
        resp = client.get("/task/1")
        body = resp.text
        # 72.5% should be yellow (50-75%)
        assert "ctx-fill-yellow" in body
        # 45.3% should be green (<50%)
        assert "ctx-fill-green" in body

    def test_task_totals_summary(self, client):
        """Task detail should show total cost and max context fill."""
        resp = client.get("/task/1")
        body = resp.text
        assert "Total Cost" in body
        assert "Max Context Fill" in body
        # Total cost = 0.1234 + 0.0456 = 0.1690
        assert "$0.1690" in body
        # Max context fill = 72.5%
        assert "72.5%" in body


# --- Usage Trends ---


class TestUsagePage:
    def test_returns_200(self, client):
        resp = client.get("/usage")
        assert resp.status_code == 200

    def test_contains_snapshot_data(self, client):
        resp = client.get("/usage")
        body = resp.text
        assert "45.0" in body
        assert "72.0" in body

    def test_contains_chart(self, client):
        resp = client.get("/usage")
        assert "usage-chart" in resp.text
        assert "chart.js" in resp.text.lower()

    def test_time_range_selector(self, client):
        resp = client.get("/usage")
        body = resp.text
        assert "Last 24h" in body
        assert "Last 7d" in body
        assert "Last 30d" in body

    def test_default_range_is_7d(self, client):
        resp = client.get("/usage")
        body = resp.text
        # 7d button should be active (aria-current)
        assert 'range=7d" role="button" aria-current="true"' in body

    def test_range_24h(self, client):
        resp = client.get("/usage?range=24h")
        assert resp.status_code == 200
        body = resp.text
        assert 'range=24h" role="button" aria-current="true"' in body

    def test_range_30d(self, client):
        resp = client.get("/usage?range=30d")
        assert resp.status_code == 200

    def test_invalid_range_defaults_to_7d(self, client):
        resp = client.get("/usage?range=invalid")
        assert resp.status_code == 200
        assert 'range=7d" role="button" aria-current="true"' in resp.text

    def test_raw_data_in_details(self, client):
        resp = client.get("/usage")
        assert "<details>" in resp.text
        assert "Raw snapshot data" in resp.text


# --- Metrics ---


class TestMetricsPage:
    def test_returns_200(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200

    def test_contains_aggregate_data(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Total Tasks" in body
        assert "Completed" in body
        assert "Failed" in body

    def test_contains_success_rate(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Success Rate" in body
        # 1 completed / 2 total = 50%
        assert "50.0%" in body

    def test_contains_averages(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Avg Wall Time" in body
        assert "Avg Turns / Task" in body
        assert "Avg Review Iterations" in body

    def test_contains_time_buckets(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Tasks Completed" in body
        assert "Today" in body
        assert "Last 7 Days" in body
        assert "Last 30 Days" in body

    def test_failure_reasons_displayed(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Common Failure Reasons" in body
        assert "Tests failed" in body

    def test_project_filter(self, client):
        resp = client.get("/metrics?project=my-project")
        body = resp.text
        assert resp.status_code == 200
        # Only 1 task in my-project (TST-1, completed)
        assert "100.0%" in body  # 1/1 success rate

    def test_project_filter_dropdown(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "my-project" in body
        assert "other-project" in body

    def test_metrics_no_db(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "Total Tasks" in resp.text

    def test_avg_wall_time_formatted(self, client):
        resp = client.get("/metrics")
        body = resp.text
        # TST-1 has 1.5h wall time => 1h30m
        assert "1h30m" in body

    def test_contains_token_usage_section(self, client):
        """Metrics page should show token usage & cost aggregates."""
        resp = client.get("/metrics")
        body = resp.text
        assert "Token Usage" in body
        assert "Total Input Tokens" in body
        assert "Total Output Tokens" in body
        assert "Total Cost" in body
        assert "Avg Context Fill" in body

    def test_token_aggregate_values(self, client):
        """Token totals should aggregate from all stage runs."""
        resp = client.get("/metrics")
        body = resp.text
        # 50000 + 20000 = 70000 total input tokens
        assert "70,000" in body
        # 15000 + 5000 = 20000 total output tokens
        assert "20,000" in body
        # 0.1234 + 0.0456 = 0.17 total cost
        assert "$0.17" in body


# --- start_dashboard ---


class TestStartDashboard:
    def test_disabled_returns_none(self, db_file):
        config = DashboardConfig(enabled=False)
        result = start_dashboard(
            config, db_path=db_file,
        )
        assert result is None

    def test_enabled_returns_thread(self, db_file):
        config = DashboardConfig(enabled=True, host="127.0.0.1", port=0)
        thread = start_dashboard(
            config, db_path=db_file,
        )
        assert thread is not None
        assert thread.is_alive()
        assert thread.daemon is True


# --- Config integration ---


class TestDashboardConfig:
    def test_default_values(self):
        cfg = DashboardConfig()
        assert cfg.enabled is False
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 8420

    def test_custom_values(self):
        cfg = DashboardConfig(enabled=True, host="127.0.0.1", port=9000)
        assert cfg.enabled is True
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 9000


# --- Edge cases ---


class TestEdgeCases:
    @pytest.fixture(autouse=True)
    def _mock_refresh(self, monkeypatch):
        """Prevent real API calls during edge case tests."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None
        )

    def test_empty_db_index(self, tmp_path):
        """With an empty DB (no slots, no state), index renders gracefully."""
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200

    def test_empty_database(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "No tasks found" in resp.text

    def test_usage_no_data(self, tmp_path):
        """With no usage snapshots in DB, usage partial renders without error."""
        db_path = tmp_path / "no_usage.db"
        conn = init_db(db_path)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert resp.status_code == 200


# --- Config page ---


def _make_botfarm_config(tmp_path):
    """Create a BotfarmConfig with a real YAML source file for testing."""
    config_data = {
        "projects": [
            {
                "name": "test-project",
                "linear_team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1],
            }
        ],
        "linear": {
            "api_key": "${LINEAR_API_KEY}",
            "poll_interval_seconds": 120,
            "comment_on_failure": True,
            "comment_on_completion": False,
            "comment_on_limit_pause": False,
        },
        "usage_limits": {
            "pause_five_hour_threshold": 0.85,
            "pause_seven_day_threshold": 0.90,
        },
        "agents": {
            "max_review_iterations": 3,
            "max_ci_retries": 2,
            "timeout_minutes": {"implement": 120, "review": 30, "fix": 60},
            "timeout_grace_seconds": 10,
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_data, sort_keys=False))

    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project", linear_team="TST",
                base_dir="~/test", worktree_prefix="test-slot-", slots=[1],
            ),
        ],
        linear=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=120,
            comment_on_failure=True,
            comment_on_completion=False,
            comment_on_limit_pause=False,
        ),
    )
    config.source_path = str(config_path)
    return config, config_path


class TestConfigPage:
    @pytest.fixture()
    def config_client(self, db_file, tmp_path):
        config, _ = _make_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        return TestClient(app)

    def test_config_page_returns_200(self, config_client):
        resp = config_client.get("/config")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_config_page_contains_sections(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "Linear" in body
        assert "Usage Limits" in body
        assert "Agents" in body

    def test_config_page_contains_current_values(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "120" in body  # poll_interval_seconds
        assert "Save" in body

    def test_config_page_contains_nav_link(self, config_client):
        resp = config_client.get("/")
        assert "Config" in resp.text

    def test_config_page_disabled_without_config(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "not available" in resp.text


class TestConfigUpdate:
    @pytest.fixture()
    def setup(self, db_file, tmp_path):
        config, config_path = _make_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        return client, config, config_path

    def test_update_linear_setting(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "linear": {"poll_interval_seconds": 60},
        })
        assert resp.status_code == 200
        assert "successfully" in resp.text
        assert config.linear.poll_interval_seconds == 60

    def test_update_bool_setting(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "linear": {"comment_on_completion": True},
        })
        assert resp.status_code == 200
        assert config.linear.comment_on_completion is True

    def test_update_usage_limits(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "usage_limits": {"pause_five_hour_threshold": 0.75},
        })
        assert resp.status_code == 200
        assert config.usage_limits.pause_five_hour_threshold == 0.75

    def test_update_agents(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "agents": {"max_review_iterations": 5},
        })
        assert resp.status_code == 200
        assert config.agents.max_review_iterations == 5

    def test_update_timeout_minutes(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "agents": {"timeout_minutes": {"implement": 90}},
        })
        assert resp.status_code == 200
        assert config.agents.timeout_minutes["implement"] == 90
        # Other stages preserved
        assert config.agents.timeout_minutes["review"] == 30

    def test_update_writes_to_file(self, setup):
        client, _, config_path = setup
        client.post("/config", json={
            "linear": {"poll_interval_seconds": 60},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["linear"]["poll_interval_seconds"] == 60

    def test_update_preserves_env_vars_in_file(self, setup):
        client, _, config_path = setup
        client.post("/config", json={
            "linear": {"poll_interval_seconds": 60},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["linear"]["api_key"] == "${LINEAR_API_KEY}"

    def test_validation_error_returns_422(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "linear": {"poll_interval_seconds": 0},
        })
        assert resp.status_code == 422
        assert "at least 1" in resp.text
        # Config unchanged
        assert config.linear.poll_interval_seconds == 120

    def test_non_editable_field_rejected(self, setup):
        client, _, _ = setup
        resp = client.post("/config", json={
            "linear": {"api_key": "hacked"},
        })
        assert resp.status_code == 422
        assert "not an editable field" in resp.text

    def test_invalid_json_returns_400(self, setup):
        client, _, _ = setup
        resp = client.post(
            "/config",
            content=b"not json",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400

    def test_non_dict_body_returns_400(self, setup):
        client, _, _ = setup
        resp = client.post("/config", json=["not", "a", "dict"])
        assert resp.status_code == 400
        assert "JSON object" in resp.text

    def test_update_without_config_returns_400(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/config", json={"linear": {"poll_interval_seconds": 60}})
        assert resp.status_code == 400
        assert "not available" in resp.text

    def test_multiple_sections_in_one_update(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "linear": {"poll_interval_seconds": 60},
            "agents": {"max_ci_retries": 5},
        })
        assert resp.status_code == 200
        assert config.linear.poll_interval_seconds == 60
        assert config.agents.max_ci_retries == 5


# --- Structural config editing ---


def _make_structural_botfarm_config(tmp_path):
    """Create a BotfarmConfig with structural fields for testing."""
    config_data = {
        "projects": [
            {
                "name": "test-project",
                "linear_team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1, 2],
                "linear_project": "My Filter",
            },
        ],
        "linear": {
            "api_key": "${LINEAR_API_KEY}",
            "poll_interval_seconds": 120,
        },
        "notifications": {
            "webhook_url": "https://hooks.example.com/old",
            "webhook_format": "slack",
            "rate_limit_seconds": 300,
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_data, sort_keys=False))

    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project", linear_team="TST",
                base_dir="~/test", worktree_prefix="test-slot-",
                slots=[1, 2], linear_project="My Filter",
            ),
        ],
        linear=LinearConfig(api_key="test-key", poll_interval_seconds=120),
        notifications=NotificationsConfig(
            webhook_url="https://hooks.example.com/old",
            webhook_format="slack",
            rate_limit_seconds=300,
        ),
    )
    config.source_path = str(config_path)
    return config, config_path


class TestStructuralConfigPage:
    @pytest.fixture()
    def structural_client(self, db_file, tmp_path):
        config, _ = _make_structural_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        return TestClient(app)

    def test_config_page_contains_projects_section(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        assert "Projects" in body
        assert "test-project" in body
        assert "restart" in body.lower()

    def test_config_page_contains_notifications_section(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        assert "Notifications" in body
        assert "Webhook URL" in body
        assert "webhook_format" in body.lower() or "Webhook format" in body

    def test_config_page_shows_project_slots(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        # Slot chips for slots [1, 2]
        assert 'data-slot="1"' in body
        assert 'data-slot="2"' in body

    def test_config_page_shows_linear_project_filter(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        assert "My Filter" in body

    def test_no_restart_banner_initially(self, structural_client):
        resp = structural_client.get("/config")
        assert 'id="restart-banner"' not in resp.text


class TestStructuralConfigUpdate:
    @pytest.fixture()
    def setup(self, db_file, tmp_path):
        config, config_path = _make_structural_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        return client, config, config_path, app

    def test_update_notifications_writes_yaml_only(self, setup):
        client, config, config_path, _ = setup
        resp = client.post("/config", json={
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        assert resp.status_code == 200
        assert "Restart required" in resp.text
        # YAML file updated
        data = yaml.safe_load(config_path.read_text())
        assert data["notifications"]["webhook_url"] == "https://new.example.com"
        # In-memory config NOT updated
        assert config.notifications.webhook_url == "https://hooks.example.com/old"

    def test_update_notifications_sets_restart_flag(self, setup):
        client, _, _, app = setup
        assert app.state.restart_required is False
        client.post("/config", json={
            "notifications": {"rate_limit_seconds": 60},
        })
        assert app.state.restart_required is True

    def test_restart_banner_shown_after_structural_update(self, setup):
        client, _, _, _ = setup
        client.post("/config", json={
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        resp = client.get("/config")
        assert 'id="restart-banner"' in resp.text
        assert "Restart required" in resp.text

    def test_update_project_slots_writes_yaml_only(self, setup):
        client, config, config_path, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "slots": [1, 2, 3]},
            ],
        })
        assert resp.status_code == 200
        # YAML file updated
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["slots"] == [1, 2, 3]
        # In-memory config NOT updated
        assert config.projects[0].slots == [1, 2]

    def test_update_project_linear_project_writes_yaml_only(self, setup):
        client, config, config_path, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "linear_project": "New Filter"},
            ],
        })
        assert resp.status_code == 200
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["linear_project"] == "New Filter"
        assert config.projects[0].linear_project == "My Filter"

    def test_structural_validation_error_returns_422(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "slots": [1, 1]},
            ],
        })
        assert resp.status_code == 422
        assert "duplicate" in resp.text

    def test_structural_unknown_project_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "nonexistent", "slots": [1]},
            ],
        })
        assert resp.status_code == 422
        assert "does not exist" in resp.text

    def test_structural_non_editable_project_field_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "base_dir": "/tmp/hacked"},
            ],
        })
        assert resp.status_code == 422
        assert "cannot edit" in resp.text

    def test_notifications_invalid_format_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "notifications": {"webhook_format": "teams"},
        })
        assert resp.status_code == 422
        assert "must be one of" in resp.text

    def test_mixed_runtime_and_structural_update(self, setup):
        client, config, config_path, app = setup
        resp = client.post("/config", json={
            "linear": {"poll_interval_seconds": 60},
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        assert resp.status_code == 200
        # Runtime applied in-memory
        assert config.linear.poll_interval_seconds == 60
        # Structural written to file only
        data = yaml.safe_load(config_path.read_text())
        assert data["notifications"]["webhook_url"] == "https://new.example.com"
        assert config.notifications.webhook_url == "https://hooks.example.com/old"
        # Restart required because of structural update
        assert app.state.restart_required is True

    def test_structural_update_no_config_path(self, db_file):
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test", linear_team="T", base_dir="~/t",
                    worktree_prefix="t-", slots=[1],
                ),
            ],
        )
        # No source_path set
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        resp = client.post("/config", json={
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        assert resp.status_code == 400
        assert "config file path" in resp.text.lower() or "Cannot save" in resp.text


class TestConfigViewPage:
    @pytest.fixture()
    def config_client(self, db_file, tmp_path):
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test-project", linear_team="TST",
                    base_dir="~/test", worktree_prefix="test-slot-",
                    slots=[1, 2], linear_project="My Project",
                ),
            ],
            linear=LinearConfig(
                api_key="lin_api_1234567890abcdef",
                workspace="my-workspace",
                poll_interval_seconds=60,
                exclude_tags=["Human", "Manual"],
                comment_on_failure=True,
            ),
            notifications=NotificationsConfig(
                webhook_url="https://hooks.slack.com/services/T00/B00/xxx",
                webhook_format="slack",
                rate_limit_seconds=300,
            ),
        )
        config.source_path = str(tmp_path / "config.yaml")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        return TestClient(app)

    def test_config_view_returns_200(self, config_client):
        resp = config_client.get("/config")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_config_page_has_view_and_edit_tabs(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "tab-view" in body
        assert "tab-edit" in body
        assert "switchTab" in body

    def test_config_view_contains_all_sections(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        for section in [
            "Projects", "Linear", "Agents", "Usage Limits",
            "Notifications", "Dashboard", "Database",
        ]:
            assert section in body

    def test_config_view_shows_project_details(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "test-project" in body
        assert "TST" in body
        assert "~/test" in body
        assert "test-slot-" in body
        assert "My Project" in body

    def test_config_view_masks_api_key(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        # View tab should show masked key (first 4 + **** + last 4)
        assert "lin_****cdef" in body
        # Full key must not appear (API key is not editable, so not in edit tab either)
        assert "lin_api_1234567890abcdef" not in body

    def test_config_view_masks_webhook_url(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        # View tab should show masked version
        assert "http****/xxx" in body

    def test_config_view_shows_linear_settings(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "my-workspace" in body
        assert "60" in body  # poll_interval_seconds
        assert "Human" in body
        assert "Manual" in body

    def test_config_view_shows_boolean_values(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "Yes" in body  # comment_on_failure = True
        assert "No" in body   # comment_on_completion = False

    def test_config_view_nav_link(self, config_client):
        resp = config_client.get("/")
        assert "Configuration" in resp.text
        assert "/config" in resp.text

    def test_config_view_disabled_without_config(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "not available" in resp.text

    def test_config_view_masks_short_api_key(self, tmp_path):
        # Use a db in a separate temp dir so the db path displayed on
        # the config page doesn't contain the test name substring.
        import tempfile
        with tempfile.TemporaryDirectory(prefix="botfarm_mask_") as td:
            db = Path(td) / "botfarm.db"
            init_db(db)
            config = BotfarmConfig(
                projects=[
                    ProjectConfig(
                        name="p", linear_team="T",
                        base_dir="~/p", worktree_prefix="p-", slots=[1],
                    ),
                ],
                linear=LinearConfig(api_key="short"),
            )
            config.source_path = str(tmp_path / "config.yaml")
            app = create_app(
                db_path=db,
                botfarm_config=config,
            )
            client = TestClient(app)
            resp = client.get("/config")
            body = resp.text
            assert "short" not in body
            assert "****" in body

    def test_config_view_empty_webhook_shows_dash(self, db_file, tmp_path):
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="p", linear_team="T",
                    base_dir="~/p", worktree_prefix="p-", slots=[1],
                ),
            ],
        )
        config.source_path = str(tmp_path / "config.yaml")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        resp = client.get("/config")
        body = resp.text
        # Empty webhook_url should show "-"
        assert "Webhook URL" in body


# --- Pipeline stepper ---


class TestBuildPipelineState:
    def test_no_stage_runs(self):
        result = build_pipeline_state([], None)
        assert len(result) == 5
        assert all(s["status"] == "pending" for s in result)
        assert all(s["iteration_count"] == 0 for s in result)

    def test_completed_task_all_stages(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "fix", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": "passed", "was_limit_restart": 0},
            {"stage": "merge", "exit_subtype": "merged", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        assert all(s["status"] == "completed" for s in result)

    def test_failed_at_review(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "rejected", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "failed")
        assert result[0]["status"] == "completed"  # implement
        assert result[1]["status"] == "failed"  # review
        assert result[2]["status"] == "pending"  # fix
        assert result[3]["status"] == "pending"  # pr_checks
        assert result[4]["status"] == "pending"  # merge

    def test_active_in_progress(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        assert result[0]["status"] == "completed"  # implement
        assert result[1]["status"] == "active"  # review
        assert result[2]["status"] == "pending"  # fix

    def test_iteration_count(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "changes_requested", "was_limit_restart": 0},
            {"stage": "fix", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "fix", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        assert result[0]["iteration_count"] == 1  # implement
        assert result[1]["iteration_count"] == 2  # review
        assert result[2]["iteration_count"] == 2  # fix
        assert result[3]["iteration_count"] == 1  # pr_checks

    def test_limit_restart_flag(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 1},
            {"stage": "review", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        assert result[0]["has_limit_restart"] is True
        assert result[1]["has_limit_restart"] is False

    def test_skipped_stage_shows_completed(self):
        """fix is skipped when review approves on first try."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": "passed", "was_limit_restart": 0},
            {"stage": "merge", "exit_subtype": "merged", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        assert result[2]["name"] == "fix"
        assert result[2]["status"] == "completed"  # skipped but should show completed
        assert result[2]["iteration_count"] == 0

    def test_stage_names_match_canonical_order(self):
        result = build_pipeline_state([], None)
        names = [s["name"] for s in result]
        assert names == ["implement", "review", "fix", "pr_checks", "merge"]

    def test_codex_review_not_a_pipeline_stage(self):
        """codex_review runs should NOT create a separate pipeline stage."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        names = [s["name"] for s in result]
        assert "codex_review" not in names
        assert len(result) == 5  # only canonical stages

    def test_codex_review_attached_to_review_stage(self):
        """codex_review info should be attached to the review stage entry."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        review = next(s for s in result if s["name"] == "review")
        assert "codex_review" in review
        assert review["codex_review"]["status"] == "APPROVED"
        assert review["codex_review"]["count"] == 1

    def test_codex_review_changes_requested(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "changes_requested", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        review = next(s for s in result if s["name"] == "review")
        assert review["codex_review"]["status"] == "CHANGES_REQUESTED"

    def test_codex_review_skipped(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "skipped", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        review = next(s for s in result if s["name"] == "review")
        assert review["codex_review"]["status"] == "Skipped"

    def test_codex_review_failed(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "failed", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        review = next(s for s in result if s["name"] == "review")
        assert review["codex_review"]["status"] == "Failed"

    def test_no_codex_review_no_key(self):
        """When there are no codex_review runs, review stage has no codex_review key."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        review = next(s for s in result if s["name"] == "review")
        assert "codex_review" not in review


class TestTaskDetailPipeline:
    def test_stepper_renders_on_task_page(self, client):
        resp = client.get("/task/1")
        assert resp.status_code == 200
        body = resp.text
        assert "pipeline-stepper" in body
        assert "pipeline-node" in body

    def test_stepper_shows_stage_labels(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "implement" in body
        assert "review" in body
        assert "pr checks" in body
        assert "merge" in body

    def test_stepper_not_shown_for_missing_task(self, client):
        resp = client.get("/task/9999")
        assert resp.status_code == 200
        body = resp.text
        # The CSS class exists in <style>, but the actual stepper div should not render
        assert 'class="pipeline-stepper"' not in body


# --- Supervisor status badge ---


class TestSupervisorBadge:
    def test_index_shows_stopped_when_no_heartbeat(self, tmp_path):
        """Without supervisor_heartbeat, badge should show Stopped."""
        db_path = tmp_path / "no_hb.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Stopped" in resp.text

    def test_index_shows_running_with_fresh_heartbeat(self, tmp_path):
        """A recent heartbeat should show Supervisor Running."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "fresh_hb.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_index_shows_stopped_with_stale_heartbeat(self, tmp_path):
        """A heartbeat older than poll_interval + grace should show Stopped.

        The default poll_interval is 120s and grace is 60s, so a heartbeat
        older than 180s should be considered stale.
        """
        from datetime import datetime, timedelta, timezone
        stale = datetime.now(timezone.utc) - timedelta(seconds=200)
        db_path = tmp_path / "stale_hb.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False,
            supervisor_heartbeat=stale.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Stopped" in resp.text

    def test_index_shows_running_during_poll_sleep(self, tmp_path):
        """A heartbeat within the poll interval should show Running.

        With default poll_interval=120s, a heartbeat 60s old is well
        within the expected range and must NOT show Stopped.
        """
        from datetime import datetime, timedelta, timezone
        recent = datetime.now(timezone.utc) - timedelta(seconds=60)
        db_path = tmp_path / "poll_sleep.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False,
            supervisor_heartbeat=recent.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_supervisor_badge_partial_endpoint(self, tmp_path):
        """The /partials/supervisor-badge endpoint returns the badge."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "badge_partial.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-badge")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_slots_partial_no_duplicate_badge(self, tmp_path):
        """The /partials/slots response should NOT contain the supervisor badge."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "no_dup_badge.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert 'hx-swap-oob' not in resp.text

    def test_badge_on_history_page(self, tmp_path):
        """History page should also show the supervisor badge."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "hist_badge.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_backward_compat_no_heartbeat_field(self, tmp_path):
        """Dashboard should not crash when no dispatch_state row exists."""
        db_path = tmp_path / "compat.db"
        conn = init_db(db_path)
        _seed_slot(conn, "p", 1, status="free")
        # No save_dispatch_state call -- no heartbeat row
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Stopped" in resp.text


# --- Relative timestamps (timeago) ---


class TestRelativeTimestamps:
    """Verify that timestamps use <time data-timestamp=...> elements for JS-based
    relative time display, with raw ISO fallback text and title attributes."""

    def test_timeago_js_in_base(self, client):
        """The timeago() function and updateTimeagos() must be in the base template."""
        resp = client.get("/")
        body = resp.text
        assert "function timeago(" in body
        assert "function updateTimeagos(" in body
        assert "data-timestamp" in body or "updateTimeagos" in body

    def test_task_detail_timestamps_have_data_attr(self, client):
        """Task detail page timestamps should use <time data-timestamp=...>."""
        resp = client.get("/task/1")
        body = resp.text
        # created_at, started_at, completed_at should have data-timestamp
        assert 'data-timestamp="2026-02-12T10:00:00+00:00"' in body  # started_at
        assert 'data-timestamp="2026-02-12T11:30:00+00:00"' in body  # completed_at

    def test_task_detail_timestamps_have_title(self, client):
        """Task detail timestamps should have title attributes with full datetime."""
        resp = client.get("/task/1")
        body = resp.text
        assert 'title="2026-02-12T10:00:00"' in body
        assert 'title="2026-02-12T11:30:00"' in body

    def test_task_detail_event_log_timestamps(self, client):
        """Event log timestamps should use <time data-timestamp=...>."""
        resp = client.get("/task/1")
        body = resp.text
        # Event log entries should have data-timestamp attributes
        assert body.count("data-timestamp") >= 3  # at least task info + events

    def test_history_timestamps_have_data_attr(self, client):
        """History table started_at/completed_at should use <time data-timestamp=...>."""
        resp = client.get("/partials/history")
        body = resp.text
        assert "data-timestamp" in body
        # started_at and completed_at for TST-1
        assert 'data-timestamp="2026-02-12T10:00:00+00:00"' in body
        assert 'data-timestamp="2026-02-12T11:30:00+00:00"' in body

    def test_history_timestamps_have_title(self, client):
        """History timestamps should have title attributes."""
        resp = client.get("/partials/history")
        body = resp.text
        assert 'title="2026-02-12T10:00:00"' in body

    def test_usage_snapshot_timestamps(self, client):
        """Usage page snapshot timestamps should use <time data-timestamp=...>."""
        resp = client.get("/usage")
        body = resp.text
        assert "data-timestamp" in body

    def test_usage_resets_at_has_data_attr(self, client):
        """Usage page resets_at should use <time data-timestamp=...>."""
        resp = client.get("/usage")
        body = resp.text
        assert 'data-timestamp="2026-02-12T15:00:00+00:00"' in body

    def test_null_timestamps_show_dash(self, tmp_path):
        """Null timestamps should show '-' without a <time> wrapper."""
        path = tmp_path / "null_ts.db"
        conn = init_db(path)
        insert_task(conn, ticket_id="NULL-1", title="No dates", project="p", slot=1)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/task/1")
        body = resp.text
        assert "NULL-1" in body
        # Null started_at and completed_at should show "-" not wrapped in <time>
        assert "<td>-</td>" in body

    def test_timeago_updates_on_htmx_swap(self, client):
        """The updateTimeagos function should be registered for htmx:afterSwap."""
        resp = client.get("/")
        body = resp.text
        assert 'addEventListener("htmx:afterSwap", updateTimeagos)' in body

    def test_timeago_periodic_refresh(self, client):
        """updateTimeagos should run on a 60-second interval."""
        resp = client.get("/")
        body = resp.text
        assert "setInterval(updateTimeagos, 60000)" in body


# ---------------------------------------------------------------------------
# Manual pause / resume API
# ---------------------------------------------------------------------------


class TestManualPauseState:
    def test_running_state(self, db_file):
        """No manual pause → state is 'running'."""
        app = create_app(db_path=db_file, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        # Running state should show the Pause button
        assert "Pause" in resp.text

    def test_paused_state(self, tmp_path):
        """When dispatch_paused=manual_pause and no busy slots → state is 'paused'."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="paused_manual", ticket_id="T-1")
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        assert "Resume" in resp.text

    def test_pausing_state(self, tmp_path):
        """When dispatch_paused=manual_pause and busy slots exist → pausing."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="busy", ticket_id="T-1", pid=12345)
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert resp.status_code == 200
        assert "Pausing" in resp.text
        assert "Cancel" in resp.text


class TestPauseHints:
    """Verify informational hints shown in each pause state."""

    def test_running_hint(self, db_file):
        """Running state shows hint about graceful pause behavior."""
        app = create_app(db_path=db_file, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        assert "Stops new dispatches" in resp.text
        assert "running stages finish gracefully" in resp.text

    def test_pausing_hint_and_worker_details(self, tmp_path):
        """Pausing state shows dispatch-stopped message and busy worker stages."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "proj", 1, status="busy", ticket_id="SMA-42",
            stage="implement", pid=12345,
        )
        _seed_slot(
            conn, "proj", 2, status="busy", ticket_id="SMA-43",
            stage="review", pid=12346,
        )
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        body = resp.text
        assert "New dispatches stopped" in body
        assert "2 workers" in body
        assert "Slot 1: implement SMA-42" in body
        assert "Slot 2: review SMA-43" in body

    def test_paused_hint(self, tmp_path):
        """Paused state shows resume hint about partially-completed tickets."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="paused_manual", ticket_id="T-1")
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path, on_pause=lambda: None, on_resume=lambda: None)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-controls")
        body = resp.text
        assert "All work paused" in body
        assert "resume from where they left off" in body


class TestPauseResumeAPI:
    def test_pause_calls_callback(self, db_file):
        """POST /api/pause calls the on_pause callback."""
        called = []
        app = create_app(
            db_path=db_file,
            on_pause=lambda: called.append("pause"),
            on_resume=lambda: None,
        )
        client = TestClient(app)
        resp = client.post("/api/pause")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
        assert called == ["pause"]

    def test_resume_calls_callback(self, db_file):
        """POST /api/resume calls the on_resume callback."""
        called = []
        app = create_app(
            db_path=db_file,
            on_pause=lambda: None,
            on_resume=lambda: called.append("resume"),
        )
        client = TestClient(app)
        resp = client.post("/api/resume")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
        assert called == ["resume"]

    def test_pause_without_callback_returns_503(self, db_file):
        """POST /api/pause returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/pause")
        assert resp.status_code == 503

    def test_resume_without_callback_returns_503(self, db_file):
        """POST /api/resume returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/resume")
        assert resp.status_code == 503


class TestPausedManualSlotDisplay:
    def test_paused_manual_shown_in_slots_table(self, tmp_path):
        """A slot with status paused_manual should display correctly."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "proj", 1, status="paused_manual",
            ticket_id="T-1", ticket_title="Test", branch="b1",
            stages_completed=["implement"],
        )
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()

        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert "paused manual" in resp.text


# --- Log Viewer ---


class TestLogViewer:
    """Tests for the live log viewer feature."""

    @pytest.fixture()
    def logs_setup(self, tmp_path):
        """Create DB + log files for testing the log viewer."""
        db_path = tmp_path / "logtest.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn,
            ticket_id="LOG-1",
            title="Log test",
            project="my-project",
            slot=1,
            status="completed",
        )
        update_task(
            conn,
            task_id,
            started_at="2026-02-26T10:00:00+00:00",
            completed_at="2026-02-26T11:00:00+00:00",
        )
        insert_stage_run(
            conn,
            task_id=task_id,
            stage="implement",
            iteration=1,
            turns=10,
            duration_seconds=1800.0,
        )
        insert_stage_run(
            conn,
            task_id=task_id,
            stage="review",
            iteration=1,
            turns=5,
            duration_seconds=600.0,
        )
        conn.commit()
        conn.close()

        # Create log files
        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "LOG-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text(
            "Starting implement stage\nDoing work...\nDone.\n"
        )
        (ticket_log_dir / "review-20260226-103000.log").write_text(
            "Reviewing PR\nLooks good.\n"
        )

        return db_path, logs_dir

    def test_log_viewer_page_returns_200(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_log_viewer_shows_stage_tabs(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs")
        body = resp.text
        assert "implement" in body
        assert "review" in body

    def test_log_viewer_stage_page_returns_200(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/implement")
        assert resp.status_code == 200

    def test_log_viewer_shows_log_content(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/implement")
        body = resp.text
        assert "Starting implement stage" in body
        assert "Doing work..." in body

    def test_log_viewer_no_logs_dir(self, tmp_path):
        """Without logs_dir, log viewer shows no logs available."""
        db_path = tmp_path / "nolog.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="NL-1", title="No logs",
            project="proj", slot=1, status="completed",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/task/NL-1/logs")
        assert resp.status_code == 200
        assert "No log files available" in resp.text

    def test_log_viewer_no_log_files(self, tmp_path):
        """With logs_dir but no files for ticket, shows no logs available."""
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="EMP-1", title="Empty",
            project="proj", slot=1, status="completed",
        )
        conn.commit()
        conn.close()
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/EMP-1/logs")
        assert resp.status_code == 200
        assert "No log files available" in resp.text

    def test_log_content_api_returns_text(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/LOG-1/implement/content")
        assert resp.status_code == 200
        assert "text/plain" in resp.headers["content-type"]
        assert "Starting implement stage" in resp.text

    def test_log_content_api_404_no_file(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/LOG-1/merge/content")
        assert resp.status_code == 404

    def test_log_content_api_404_no_ticket(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/NONEXIST-1/implement/content")
        assert resp.status_code == 404

    def test_stream_endpoint_404_no_file(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/LOG-1/merge/stream")
        assert resp.status_code == 404

    def test_log_viewer_back_link_to_task_detail(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs")
        assert "Back to Task Detail" in resp.text

    def test_log_viewer_active_stage_tab(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/implement")
        assert "log-stage-tab-active" in resp.text

    def test_log_viewer_review_content(self, logs_setup):
        db_path, logs_dir = logs_setup
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LOG-1/logs/review")
        assert "Reviewing PR" in resp.text
        assert "Looks good" in resp.text


class TestLogViewerBusySlot:
    """Tests for log viewer on busy (live) slots."""

    def test_busy_slot_shows_logs_link(self, tmp_path):
        """Busy slot should show a 'logs' link."""
        db_path = tmp_path / "busy.db"
        conn = init_db(db_path)
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="T-1", ticket_title="Test",
            stage="implement",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "/task/T-1/logs" in resp.text
        assert ">logs</a>" in resp.text

    def test_free_slot_no_logs_link(self, tmp_path):
        """Free slot should not show a 'logs' link."""
        db_path = tmp_path / "free.db"
        conn = init_db(db_path)
        _seed_slot(conn, "proj", 1, status="free")
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert ">logs</a>" not in resp.text

    def test_live_badge_shown_for_busy_stage(self, tmp_path):
        """When a stage is actively running, show LIVE badge."""
        db_path = tmp_path / "live.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="LIVE-1", title="Live test",
            project="proj", slot=1, status="in_progress",
        )
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="LIVE-1", ticket_title="Live test",
            stage="implement",
        )
        conn.commit()
        conn.close()

        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "LIVE-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text(
            "Working...\n"
        )

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/LIVE-1/logs/implement")
        assert resp.status_code == 200
        assert "LIVE" in resp.text
        assert "log-live-badge" in resp.text


class TestTaskDetailLogLinks:
    """Tests for log links on the task detail page."""

    def test_stage_runs_table_has_logs_column(self, client):
        resp = client.get("/task/TST-1")
        body = resp.text
        assert "<th>Logs</th>" in body

    def test_stage_runs_have_log_view_links(self, client):
        resp = client.get("/task/TST-1")
        body = resp.text
        assert "/task/TST-1/logs/implement" in body
        assert "/task/TST-1/logs/review" in body


class TestFormatNdjsonLine:
    """Tests for the NDJSON line formatting function."""

    def test_empty_line(self):
        event_type, text = format_ndjson_line("")
        assert event_type == "log"
        assert text == ""

    def test_non_json_line(self):
        event_type, text = format_ndjson_line("plain text log line")
        assert event_type == "log"
        assert text == "plain text log line"

    def test_assistant_text_content(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{"type": "text", "text": "Hello, I will help you."}],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "assistant"
        assert "Hello, I will help you." in text

    def test_assistant_tool_use(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Let me read that file."},
                    {"type": "tool_use", "name": "Read", "input": {"file_path": "/foo/bar.py"}},
                ],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "assistant"
        assert "Let me read that file." in text
        assert "Read" in text
        assert "/foo/bar.py" in text

    def test_assistant_empty_content(self):
        line = json.dumps({
            "type": "assistant",
            "message": {"content": []},
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "assistant"
        assert "[assistant turn]" in text

    def test_user_tool_result_ok(self):
        line = json.dumps({
            "type": "user",
            "message": {
                "content": [{
                    "type": "tool_result",
                    "is_error": False,
                    "content": "File contents here...",
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_result"
        assert "[ok]" in text
        assert "File contents" in text

    def test_user_tool_result_error(self):
        line = json.dumps({
            "type": "user",
            "message": {
                "content": [{
                    "type": "tool_result",
                    "is_error": True,
                    "content": "Permission denied",
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_result"
        assert "[ERROR]" in text

    def test_result_event(self):
        line = json.dumps({
            "type": "result",
            "num_turns": 15,
            "duration_ms": 45000,
            "subtype": "end_turn",
            "is_error": False,
            "result": "Task completed successfully",
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "result"
        assert "15 turns" in text
        assert "45.0s" in text
        assert "end_turn" in text
        assert "Task completed successfully" in text

    def test_result_event_error(self):
        line = json.dumps({
            "type": "result",
            "num_turns": 5,
            "duration_ms": 10000,
            "is_error": True,
            "result": "Something went wrong",
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "result"
        assert "[ERROR]" in text

    def test_unknown_json_type(self):
        line = json.dumps({"type": "ping"})
        event_type, text = format_ndjson_line(line)
        assert event_type == "system"
        assert "[ping]" in text

    def test_json_without_type(self):
        line = json.dumps({"foo": "bar"})
        event_type, text = format_ndjson_line(line)
        assert event_type == "log"

    def test_invalid_json(self):
        event_type, text = format_ndjson_line("{bad json")
        assert event_type == "log"
        assert "{bad json" in text

    def test_bash_tool_summarized(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{
                    "type": "tool_use",
                    "name": "Bash",
                    "input": {"command": "git status"},
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_use"
        assert "Bash" in text
        assert "git status" in text

    def test_edit_tool_summarized(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{
                    "type": "tool_use",
                    "name": "Edit",
                    "input": {"file_path": "/src/main.py"},
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_use"
        assert "Edit" in text
        assert "/src/main.py" in text

    def test_grep_tool_summarized(self):
        line = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{
                    "type": "tool_use",
                    "name": "Grep",
                    "input": {"pattern": "def foo"},
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_use"
        assert "Grep" in text
        assert "def foo" in text

    def test_tool_result_content_as_list(self):
        line = json.dumps({
            "type": "user",
            "message": {
                "content": [{
                    "type": "tool_result",
                    "is_error": False,
                    "content": [{"type": "text", "text": "Result text here"}],
                }],
            },
        })
        event_type, text = format_ndjson_line(line)
        assert event_type == "tool_result"
        assert "Result text here" in text

    def test_whitespace_stripped(self):
        event_type, text = format_ndjson_line("   \n  ")
        assert event_type == "log"
        assert text == ""

    def test_stderr_marker(self):
        event_type, text = format_ndjson_line("--- STDERR ---")
        assert event_type == "log"
        assert "STDERR" in text


class TestTaskDetailLiveOutput:
    """Tests for the 'View Live Output' link on the task detail page."""

    def test_in_progress_task_shows_live_link(self, tmp_path):
        db_path = tmp_path / "live.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="LIVE-2", title="In progress task",
            project="proj", slot=1, status="in_progress",
        )
        insert_stage_run(conn, task_id=task_id, stage="implement",
                         iteration=1, turns=5, duration_seconds=300.0)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/task/LIVE-2")
        body = resp.text
        assert "View Live Output" in body
        assert "/task/LIVE-2/logs" in body
        assert "log-live-badge" in body

    def test_completed_task_no_live_link(self, client):
        # TST-1 is a completed task in the default fixture
        resp = client.get("/task/TST-1")
        body = resp.text
        assert "View Live Output" not in body

    def test_failed_task_no_live_link(self, tmp_path):
        db_path = tmp_path / "fail.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="FAIL-1", title="Failed task",
            project="proj", slot=1, status="failed",
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/task/FAIL-1")
        assert "View Live Output" not in resp.text


class TestSSEStreamFormatting:
    """Tests for the SSE stream endpoint with NDJSON formatting."""

    def test_stream_returns_formatted_events(self, tmp_path):
        """SSE stream should transform NDJSON into formatted event types."""
        db_path = tmp_path / "stream.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="STR-1", title="Stream test",
            project="proj", slot=1, status="in_progress",
        )
        conn.commit()
        conn.close()

        # Create a log file with NDJSON content
        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "STR-1"
        ticket_log_dir.mkdir(parents=True)
        ndjson_content = (
            json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "Working on it"}]}}) + "\n"
            + json.dumps({"type": "user", "message": {"content": [{"type": "tool_result", "is_error": False, "content": "ok"}]}}) + "\n"
            + json.dumps({"type": "result", "num_turns": 3, "duration_ms": 5000, "result": "Done"}) + "\n"
        )
        (ticket_log_dir / "implement-20260226-100000.log").write_text(ndjson_content)

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/STR-1/implement/stream")
        assert resp.status_code == 200

    def test_stream_404_no_log(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        insert_task(
            conn, ticket_id="NF-1", title="No file",
            project="proj", slot=1, status="in_progress",
        )
        conn.commit()
        conn.close()
        logs_dir = tmp_path / "logs"
        logs_dir.mkdir()
        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/api/logs/NF-1/implement/stream")
        assert resp.status_code == 404


class TestLogViewerFormattedEvents:
    """Tests for the updated log viewer template with event type styling."""

    def test_live_viewer_includes_event_type_handlers(self, tmp_path):
        """Live log viewer should register handlers for all event types."""
        db_path = tmp_path / "live.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="EVT-1", title="Event test",
            project="proj", slot=1, status="in_progress",
        )
        _seed_slot(
            conn, "proj", 1, status="busy",
            ticket_id="EVT-1", ticket_title="Event test",
            stage="implement",
        )
        conn.commit()
        conn.close()

        logs_dir = tmp_path / "logs"
        ticket_log_dir = logs_dir / "EVT-1"
        ticket_log_dir.mkdir(parents=True)
        (ticket_log_dir / "implement-20260226-100000.log").write_text("test\n")

        app = create_app(db_path=db_path, logs_dir=logs_dir)
        client = TestClient(app)
        resp = client.get("/task/EVT-1/logs/implement")
        body = resp.text
        assert resp.status_code == 200
        assert "LIVE" in body
        # Check that the JS registers handlers for formatted event types
        assert "assistant" in body
        assert "tool_use" in body
        assert "tool_result" in body
        assert "appendLine" in body


# ---------------------------------------------------------------------------
# Identity credentials management
# ---------------------------------------------------------------------------


def _make_identity_config(tmp_path, *, coder=None, reviewer=None):
    """Create a BotfarmConfig with identity fields and a YAML source file."""
    coder_identity = coder or CoderIdentity(
        github_token="ghp_testabc123def456",
        ssh_key_path="~/.botfarm/coder_id_ed25519",
        git_author_name="Coder Bot",
        git_author_email="coder@example.com",
        linear_api_key="lin_api_test123",
    )
    reviewer_identity = reviewer or ReviewerIdentity(
        github_token="ghp_reviewer789xyz",
        linear_api_key="lin_api_reviewer456",
    )

    config_data = {
        "projects": [
            {
                "name": "test-project",
                "linear_team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1],
            }
        ],
        "linear": {
            "api_key": "${LINEAR_API_KEY}",
            "poll_interval_seconds": 30,
        },
        "identities": {
            "coder": {
                "github_token": "${CODER_GITHUB_TOKEN}",
                "ssh_key_path": coder_identity.ssh_key_path,
                "git_author_name": coder_identity.git_author_name,
                "git_author_email": coder_identity.git_author_email,
                "linear_api_key": "${CODER_LINEAR_API_KEY}",
            },
            "reviewer": {
                "github_token": "${REVIEWER_GITHUB_TOKEN}",
                "linear_api_key": "${REVIEWER_LINEAR_API_KEY}",
            },
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_data, sort_keys=False))

    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project", linear_team="TST",
                base_dir="~/test", worktree_prefix="test-slot-", slots=[1],
            ),
        ],
        linear=LinearConfig(api_key="test-key", poll_interval_seconds=30),
        identities=IdentitiesConfig(
            coder=coder_identity,
            reviewer=reviewer_identity,
        ),
    )
    config.source_path = str(config_path)
    return config, config_path


class TestIdentitiesPage:
    @pytest.fixture()
    def identity_client(self, db_file, tmp_path):
        config, _ = _make_identity_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=config)
        return TestClient(app)

    def test_identities_page_returns_200(self, identity_client):
        resp = identity_client.get("/identities")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_identities_page_shows_coder_section(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        assert "Coder Identity" in body
        assert "Reviewer Identity" in body

    def test_identities_page_shows_masked_tokens(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        # Masked: first 4 + **** + last 4
        assert "ghp_****f456" in body
        assert "ghp_****9xyz" in body

    def test_identities_page_shows_plain_fields(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        assert "Coder Bot" in body
        assert "coder@example.com" in body
        assert "coder_id_ed25519" in body

    def test_identities_page_shows_set_badges(self, identity_client):
        resp = identity_client.get("/identities")
        body = resp.text
        assert "identity-badge-set" in body

    def test_identities_page_disabled_without_config(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/identities")
        assert resp.status_code == 200
        assert "not available" in resp.text

    def test_identities_page_shows_unset_badges_when_empty(self, db_file, tmp_path):
        config, _ = _make_identity_config(
            tmp_path,
            coder=CoderIdentity(),
            reviewer=ReviewerIdentity(),
        )
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.get("/identities")
        body = resp.text
        assert "identity-badge-unset" in body

    def test_identities_nav_link_present(self, identity_client):
        resp = identity_client.get("/")
        assert "Identities" in resp.text

    def test_identities_page_ssh_key_exists_check(self, db_file, tmp_path):
        ssh_key = tmp_path / "test_key"
        ssh_key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\ntest\n")
        config, _ = _make_identity_config(
            tmp_path,
            coder=CoderIdentity(ssh_key_path=str(ssh_key)),
        )
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.get("/identities")
        assert "File exists" in resp.text

    def test_identities_page_ssh_key_not_found(self, db_file, tmp_path):
        config, _ = _make_identity_config(
            tmp_path,
            coder=CoderIdentity(ssh_key_path="/nonexistent/key"),
        )
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.get("/identities")
        assert "File not found" in resp.text


class TestIdentitiesUpdate:
    @pytest.fixture()
    def setup(self, db_file, tmp_path):
        config, config_path = _make_identity_config(tmp_path)
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        return client, config, config_path, tmp_path

    def test_update_coder_plain_fields(self, setup):
        client, _, config_path, _ = setup
        resp = client.post("/identities", json={
            "coder": {
                "git_author_name": "New Bot Name",
                "git_author_email": "new@example.com",
                "ssh_key_path": "/new/path/key",
            },
        })
        assert resp.status_code == 200
        assert "saved" in resp.text.lower()
        # Check YAML was updated
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["git_author_name"] == "New Bot Name"
        assert data["identities"]["coder"]["git_author_email"] == "new@example.com"
        assert data["identities"]["coder"]["ssh_key_path"] == "/new/path/key"

    def test_update_coder_secret_writes_env_file(self, setup):
        client, _, config_path, tmp_path = setup
        resp = client.post("/identities", json={
            "coder": {"github_token": "ghp_newsecret123"},
        })
        assert resp.status_code == 200
        # .env should have the token
        env_path = config_path.parent / ".env"
        env_content = env_path.read_text()
        assert 'CODER_GITHUB_TOKEN="ghp_newsecret123"' in env_content

    def test_update_coder_secret_writes_env_ref_to_yaml(self, setup):
        client, _, config_path, _ = setup
        client.post("/identities", json={
            "coder": {"github_token": "ghp_newsecret123"},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["github_token"] == "${CODER_GITHUB_TOKEN}"

    def test_update_reviewer_secret(self, setup):
        client, _, config_path, _ = setup
        resp = client.post("/identities", json={
            "reviewer": {
                "github_token": "ghp_reviewernew",
                "linear_api_key": "lin_api_new",
            },
        })
        assert resp.status_code == 200
        env_path = config_path.parent / ".env"
        env_content = env_path.read_text()
        assert 'REVIEWER_GITHUB_TOKEN="ghp_reviewernew"' in env_content
        assert 'REVIEWER_LINEAR_API_KEY="lin_api_new"' in env_content

    def test_update_sets_restart_required(self, setup):
        client, _, _, _ = setup
        client.post("/identities", json={
            "coder": {"git_author_name": "Changed"},
        })
        resp = client.get("/config")
        # The config page should show restart banner if restart_required is set
        # We can check via the app state indirectly
        # Just verify the response was successful
        assert resp.status_code == 200

    def test_update_mixed_secret_and_plain(self, setup):
        client, _, config_path, _ = setup
        resp = client.post("/identities", json={
            "coder": {
                "github_token": "ghp_mixed123",
                "git_author_name": "Mixed Bot",
            },
        })
        assert resp.status_code == 200
        # .env has secret
        env_path = config_path.parent / ".env"
        assert 'CODER_GITHUB_TOKEN="ghp_mixed123"' in env_path.read_text()
        # YAML has env ref for secret, plain value for non-secret
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["github_token"] == "${CODER_GITHUB_TOKEN}"
        assert data["identities"]["coder"]["git_author_name"] == "Mixed Bot"

    def test_update_unknown_role_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "admin": {"github_token": "nope"},
        })
        assert resp.status_code == 422
        assert "Unknown role" in resp.text

    def test_update_unknown_field_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "coder": {"password": "nope"},
        })
        assert resp.status_code == 422
        assert "not an editable" in resp.text

    def test_update_non_string_value_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "coder": {"github_token": 12345},
        })
        assert resp.status_code == 422
        assert "must be a string" in resp.text

    def test_update_invalid_json_returns_400(self, setup):
        client, _, _, _ = setup
        resp = client.post(
            "/identities",
            content=b"not json",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400

    def test_update_non_dict_body_returns_400(self, setup):
        client, _, _, _ = setup
        resp = client.post("/identities", json=["not", "a", "dict"])
        assert resp.status_code == 400

    def test_update_without_config_returns_400(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/identities", json={"coder": {"git_author_name": "x"}})
        assert resp.status_code == 400
        assert "not available" in resp.text

    def test_env_file_preserves_existing_keys(self, setup):
        client, _, config_path, _ = setup
        env_path = config_path.parent / ".env"
        env_path.write_text("EXISTING_KEY=keepme\n")
        client.post("/identities", json={
            "coder": {"github_token": "ghp_new"},
        })
        env_content = env_path.read_text()
        assert "EXISTING_KEY=keepme" in env_content
        assert 'CODER_GITHUB_TOKEN="ghp_new"' in env_content

    def test_env_file_updates_existing_secret(self, setup):
        client, _, config_path, _ = setup
        env_path = config_path.parent / ".env"
        env_path.write_text("CODER_GITHUB_TOKEN=old_value\n")
        client.post("/identities", json={
            "coder": {"github_token": "ghp_updated"},
        })
        env_content = env_path.read_text()
        assert 'CODER_GITHUB_TOKEN="ghp_updated"' in env_content
        assert "old_value" not in env_content

    def test_yaml_creates_identities_section_if_missing(self, db_file, tmp_path):
        """When config.yaml has no identities section, it should be created."""
        config_data = {
            "projects": [
                {
                    "name": "test-project",
                    "linear_team": "TST",
                    "base_dir": "~/test",
                    "worktree_prefix": "test-slot-",
                    "slots": [1],
                }
            ],
            "linear": {"api_key": "${LINEAR_API_KEY}", "poll_interval_seconds": 30},
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config_data, sort_keys=False))

        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test-project", linear_team="TST",
                    base_dir="~/test", worktree_prefix="test-slot-", slots=[1],
                ),
            ],
            linear=LinearConfig(api_key="test-key", poll_interval_seconds=30),
        )
        config.source_path = str(config_path)

        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        resp = client.post("/identities", json={
            "coder": {"git_author_name": "New Bot"},
        })
        assert resp.status_code == 200
        data = yaml.safe_load(config_path.read_text())
        assert data["identities"]["coder"]["git_author_name"] == "New Bot"

    def test_reviewer_plain_field_rejected(self, setup):
        """Reviewer identity has no plain-text editable fields like ssh_key_path."""
        client, _, _, _ = setup
        resp = client.post("/identities", json={
            "reviewer": {"ssh_key_path": "/path"},
        })
        assert resp.status_code == 422
        assert "not an editable" in resp.text


# --- Preflight / System Health ---


class _FakeCheckResult:
    """Lightweight stand-in for preflight.CheckResult (avoids importing the module)."""
    def __init__(self, name, passed, message, critical=True):
        self.name = name
        self.passed = passed
        self.message = message
        self.critical = critical


class TestPreflightBanner:
    def test_banner_hidden_when_not_degraded(self, db_file):
        """Banner partial should be empty when supervisor is not degraded."""
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: [],
            get_degraded=lambda: False,
        )
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert resp.status_code == 200
        assert "Degraded Mode" not in resp.text

    def test_banner_visible_when_degraded(self, db_file):
        """Banner partial should show degraded-mode alert when checks fail."""
        results = [
            _FakeCheckResult("git_repo:proj", False, "Remote unreachable"),
            _FakeCheckResult("database", True, "OK"),
        ]
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: results,
            get_degraded=lambda: True,
        )
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert resp.status_code == 200
        body = resp.text
        assert "Degraded Mode" in body
        assert "1 preflight check failed" in body
        assert "Re-run Checks" in body

    def test_banner_shows_plural_failures(self, db_file):
        """Banner should pluralise correctly for multiple failures."""
        results = [
            _FakeCheckResult("git_repo:a", False, "fail", critical=True),
            _FakeCheckResult("linear_api", False, "fail", critical=True),
        ]
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: results,
            get_degraded=lambda: True,
        )
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert "2 preflight checks failed" in resp.text

    def test_banner_without_callbacks(self, db_file):
        """Banner should render gracefully when no supervisor is connected."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/preflight-banner")
        assert resp.status_code == 200
        assert "Degraded Mode" not in resp.text

    def test_base_template_includes_banner_div(self, db_file):
        """Every page should include the preflight-banner htmx container."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        assert 'id="preflight-banner"' in resp.text
        assert 'hx-get="/partials/preflight-banner"' in resp.text


class TestHealthPage:
    def _make_client(self, db_file, results=None, degraded=False):
        app = create_app(
            db_path=db_file,
            get_preflight_results=lambda: (results or []),
            get_degraded=lambda: degraded,
        )
        return TestClient(app)

    def test_health_page_returns_200(self, db_file):
        client = self._make_client(db_file)
        resp = client.get("/health")
        assert resp.status_code == 200
        assert "System Health" in resp.text

    def test_health_page_shows_all_green(self, db_file):
        results = [
            _FakeCheckResult("git_repo", True, "OK"),
            _FakeCheckResult("database", True, "Schema v5 OK"),
        ]
        client = self._make_client(db_file, results=results)
        resp = client.get("/health")
        body = resp.text
        assert "All Checks Passed" in body
        assert "git_repo" in body
        assert "database" in body

    def test_health_page_shows_degraded(self, db_file):
        results = [
            _FakeCheckResult("linear_api", False, "401 Unauthorized", critical=True),
            _FakeCheckResult("database", True, "OK"),
        ]
        client = self._make_client(db_file, results=results, degraded=True)
        resp = client.get("/health")
        body = resp.text
        assert "Degraded Mode" in body
        assert "linear_api" in body
        assert "401 Unauthorized" in body

    def test_health_page_shows_guidance_for_failures(self, db_file):
        results = [
            _FakeCheckResult("git_repo:myproject", False, "Remote unreachable"),
        ]
        client = self._make_client(db_file, results=results, degraded=True)
        resp = client.get("/health")
        body = resp.text
        assert "Verify base_dir path" in body

    def test_health_page_shows_warning_checks(self, db_file):
        results = [
            _FakeCheckResult("notifications_webhook", False, "Unreachable", critical=False),
        ]
        client = self._make_client(db_file, results=results)
        resp = client.get("/health")
        body = resp.text
        assert "Warning" in body
        assert "notifications_webhook" in body

    def test_health_page_navigation_link(self, db_file):
        """Health page should be accessible from navigation."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        assert 'href="/health"' in resp.text

    def test_health_checks_partial(self, db_file):
        results = [
            _FakeCheckResult("database", True, "OK"),
            _FakeCheckResult("git_repo", False, "Not found", critical=True),
        ]
        client = self._make_client(db_file, results=results, degraded=True)
        resp = client.get("/partials/health-checks")
        assert resp.status_code == 200
        body = resp.text
        assert "database" in body
        assert "git_repo" in body
        assert "Blocking" in body

    def test_health_badge_partial_degraded(self, db_file):
        client = self._make_client(db_file, degraded=True)
        resp = client.get("/partials/health-badge")
        assert resp.status_code == 200
        assert "Degraded Mode" in resp.text

    def test_health_badge_partial_healthy(self, db_file):
        results = [_FakeCheckResult("database", True, "OK")]
        client = self._make_client(db_file, results=results, degraded=False)
        resp = client.get("/partials/health-badge")
        assert resp.status_code == 200
        assert "All Checks Passed" in resp.text

    def test_no_results_shows_empty_message(self, db_file):
        client = self._make_client(db_file)
        resp = client.get("/partials/health-checks")
        assert "No preflight results available" in resp.text


class TestRerunPreflightAPI:
    def test_rerun_calls_callback(self, db_file):
        """POST /api/rerun-preflight calls the on_rerun_preflight callback."""
        called = []
        app = create_app(
            db_path=db_file,
            on_rerun_preflight=lambda: called.append("rerun"),
        )
        client = TestClient(app)
        resp = client.post("/api/rerun-preflight")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}
        assert called == ["rerun"]

    def test_rerun_without_callback_returns_503(self, db_file):
        """POST /api/rerun-preflight returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/rerun-preflight")
        assert resp.status_code == 503


# --- Codex JSONL formatter ---


class TestFormatCodexNdjsonLine:
    def test_empty_line(self):
        event_type, text = format_codex_ndjson_line("")
        assert event_type == "log"
        assert text == ""

    def test_non_json_passthrough(self):
        event_type, text = format_codex_ndjson_line("just a plain line")
        assert event_type == "log"
        assert text == "just a plain line"

    def test_thread_started(self):
        line = json.dumps({"type": "thread.started"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "thread started" in text.lower()

    def test_turn_started(self):
        line = json.dumps({"type": "turn.started"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "turn started" in text.lower()

    def test_turn_completed(self):
        line = json.dumps({"type": "turn.completed", "status": "completed"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "result"
        assert "completed" in text.lower()

    def test_item_completed_agent_message(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "content": [{"type": "text", "text": "Review looks good"}],
            },
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "assistant"
        assert "Review looks good" in text

    def test_item_completed_reasoning(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {
                "type": "reasoning",
                "content": [{"type": "text", "text": "Thinking about the code"}],
            },
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "Thinking about the code" in text

    def test_item_completed_shell_command(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {"type": "shell_command", "command": "ls -la"},
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "tool_use"
        assert "ls -la" in text

    def test_item_completed_shell_result(self):
        line = json.dumps({
            "type": "item.completed",
            "item": {"type": "shell_result", "output": "file1.py\nfile2.py"},
        })
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "tool_result"
        assert "file1.py" in text

    def test_unknown_event_type(self):
        line = json.dumps({"type": "some.unknown"})
        event_type, text = format_codex_ndjson_line(line)
        assert event_type == "system"
        assert "some.unknown" in text


# --- Codex review log viewer ---


class TestLogViewerCodexStage:
    def test_codex_review_stage_in_log_tabs(self, tmp_path):
        """codex_review logs should be discoverable and selectable in the log viewer."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="TST-CX1", title="Test Codex",
            project="proj", slot=1, status="completed",
        )
        insert_stage_run(
            conn, task_id=task_id, stage="implement", iteration=1,
            turns=10, duration_seconds=60.0,
        )
        insert_stage_run(
            conn, task_id=task_id, stage="review", iteration=1,
            turns=5, duration_seconds=30.0,
        )
        insert_stage_run(
            conn, task_id=task_id, stage="codex_review", iteration=1,
            turns=3, duration_seconds=20.0, exit_subtype="approved",
        )
        conn.commit()
        conn.close()

        # Create log files
        logs_dir = tmp_path / "logs"
        ticket_dir = logs_dir / "TST-CX1"
        ticket_dir.mkdir(parents=True)
        (ticket_dir / "implement-20260228-120000.log").write_text("impl log")
        (ticket_dir / "review-20260228-120100.log").write_text("review log")
        (ticket_dir / "codex_review-20260228-120200.log").write_text(
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "content": [{"type": "text", "text": "LGTM"}]}}) + "\n"
        )

        app = create_app(db_path=db_path, logs_dir=str(logs_dir))
        client = TestClient(app)

        resp = client.get("/task/TST-CX1/logs")
        assert resp.status_code == 200
        body = resp.text
        assert "codex review" in body.lower() or "codex_review" in body.lower()

    def test_codex_review_stage_detail_shows_label(self, tmp_path):
        """codex_review stage runs should show 'Codex Review' label in task detail."""
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        task_id = insert_task(
            conn, ticket_id="TST-CX2", title="Test Codex Label",
            project="proj", slot=1, status="completed",
        )
        insert_stage_run(
            conn, task_id=task_id, stage="review", iteration=1,
            turns=5, duration_seconds=30.0,
        )
        insert_stage_run(
            conn, task_id=task_id, stage="codex_review", iteration=1,
            turns=3, duration_seconds=20.0, exit_subtype="approved",
        )
        conn.commit()
        conn.close()

        app = create_app(db_path=db_path)
        client = TestClient(app)

        resp = client.get(f"/task/{task_id}")
        assert resp.status_code == 200
        assert "Codex Review" in resp.text


# --- Codex config toggles ---


class TestCodexConfigToggles:
    @pytest.fixture()
    def config_client(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        cfg = BotfarmConfig(
            projects=[ProjectConfig(
                name="proj", linear_team="team",
                base_dir="/tmp/test", worktree_prefix="/tmp/test-wt",
                slots=[1],
            )],
            linear=LinearConfig(api_key="test-key", workspace="test"),
            source_path=str(tmp_path / "config.yaml"),
        )
        # Write a minimal config.yaml for edit tests
        yaml_data = {"agents": {}}
        (tmp_path / "config.yaml").write_text(yaml.dump(yaml_data))
        app = create_app(db_path=db_path, botfarm_config=cfg)
        return TestClient(app)

    def test_config_view_shows_codex_fields(self, config_client):
        """Config view tab should show Codex reviewer status."""
        resp = config_client.get("/config")
        assert resp.status_code == 200
        assert "Codex Reviewer" in resp.text

    def test_config_edit_shows_codex_fields(self, config_client):
        """Config edit tab should have Codex reviewer form fields."""
        resp = config_client.get("/config")
        assert resp.status_code == 200
        body = resp.text
        assert "codex_reviewer_enabled" in body
        assert "codex_reviewer_model" in body
        assert "codex_reviewer_timeout_minutes" in body


# --- Auto-restart banner ---


class TestAutoRestartBanner:
    """Tests for auto-restart behaviour in the update banner and API."""

    @pytest.fixture()
    def client_auto_restart(self, db_file):
        app = create_app(db_path=db_file, auto_restart=True)
        return TestClient(app)

    @pytest.fixture()
    def client_no_auto_restart(self, db_file):
        app = create_app(db_path=db_file, auto_restart=False)
        return TestClient(app)

    def test_api_update_allowed_with_auto_restart(self, db_file):
        called = []
        app = create_app(db_path=db_file, auto_restart=True, on_update=lambda: called.append(1))
        client = TestClient(app)
        resp = client.post("/api/update")
        assert resp.status_code == 200
        assert called  # callback was invoked

    def test_api_update_blocked_without_auto_restart(self, client_no_auto_restart):
        resp = client_no_auto_restart.post("/api/update")
        assert resp.status_code == 409
        assert "Auto-restart is disabled" in resp.json()["error"]

    @patch("botfarm.dashboard.commits_behind", return_value=3)
    def test_banner_shows_button_with_auto_restart(self, _mock_cb, client_auto_restart):
        resp = client_auto_restart.get("/partials/update-banner")
        assert resp.status_code == 200
        body = resp.text
        assert "Update &amp; Restart" in body
        assert "disabled" not in body

    @patch("botfarm.dashboard.commits_behind", return_value=3)
    def test_banner_disables_button_without_auto_restart(self, _mock_cb, client_no_auto_restart):
        resp = client_no_auto_restart.get("/partials/update-banner")
        assert resp.status_code == 200
        body = resp.text
        assert "disabled" in body
        assert "Auto-restart is disabled" in body

    def test_auto_restart_defaults_to_true(self, db_file):
        app = create_app(db_path=db_file)
        assert app.state.auto_restart is True


# --- Workflow page ---


class TestWorkflowPage:
    def test_workflow_returns_200(self, client):
        resp = client.get("/workflow")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_workflow_shows_implementation_pipeline(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "implementation" in body.lower()
        assert "implement" in body
        assert "review" in body
        assert "pr_checks" in body
        assert "merge" in body

    def test_workflow_shows_investigation_pipeline(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "investigation" in body.lower()
        assert "Investigation" in body or "investigation" in body

    def test_workflow_shows_decision_points(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "Approved?" in body
        assert "CI passed?" in body

    def test_workflow_shows_loop_iterations(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "max 3 iterations" in body
        assert "max 2 retries" in body

    def test_workflow_shows_executor_types(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "workflow-node-claude" in body
        assert "workflow-node-shell" in body
        assert "workflow-node-internal" in body

    def test_workflow_shows_identity_badges(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "coder" in body
        assert "reviewer" in body

    def test_workflow_shows_loop_fix_stages(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "fix" in body
        assert "ci_fix" in body

    def test_workflow_nav_link(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'href="/workflow"' in body
        assert 'aria-current="page"' in body

    def test_workflow_pipeline_tabs(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "workflow-tab" in body
        assert "switchPipeline" in body

    def test_workflow_no_db(self, tmp_path):
        app = create_app(db_path=tmp_path / "nonexistent.db")
        client = TestClient(app)
        resp = client.get("/workflow")
        assert resp.status_code == 200
        assert "No pipeline definitions found" in resp.text

    def test_workflow_shows_stage_timeouts(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "min" in body

    def test_workflow_shows_investigation_note(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "Research only" in body

    def test_workflow_config_overrides_loop_iterations(self, db_file):
        """When botfarm_config is provided, loop max iterations should reflect config values."""
        cfg = BotfarmConfig(
            projects=[ProjectConfig(name="test", linear_team="T", base_dir="/tmp", worktree_prefix="w", slots=[1])],
            linear=LinearConfig(api_key="test"),
        )
        cfg.agents.max_review_iterations = 5
        cfg.agents.max_ci_retries = 4
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/workflow")
        body = resp.text
        assert "max 5 iterations" in body
        assert "max 4 retries" in body

    def test_workflow_shows_prompt_edit_buttons(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-edit-btn" in body
        assert "enterEditMode" in body

    def test_workflow_shows_prompt_preview(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-preview" in body
        assert "prompt-textarea" in body

    def test_workflow_shows_variable_chips(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-chip" in body
        assert "{ticket_id}" in body
        assert "{pr_url}" in body
        assert "{pr_number}" in body
        assert "{owner}" in body
        assert "{repo}" in body
        assert "{ci_failure_output}" in body

    def test_workflow_hides_prompt_for_non_claude_stages(self, client):
        """Shell/internal stages have prompt section hidden."""
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-field-claude" in body

    def test_workflow_prompt_save_cancel_buttons(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-save-btn" in body
        assert "prompt-cancel-btn" in body
        assert "savePrompt" in body
        assert "cancelEdit" in body

    def test_workflow_unknown_var_detection(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "checkUnknownVars" in body
        assert "KNOWN_VARS" in body

    def test_workflow_shows_stage_properties_form(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-props-form" in body
        assert "prop-executor_type-" in body
        assert "prop-identity-" in body
        assert "prop-max_turns-" in body
        assert "prop-timeout_minutes-" in body
        assert "prop-result_parser-" in body

    def test_workflow_shows_executor_type_dropdown(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'value="claude"' in body
        assert 'value="shell"' in body
        assert 'value="internal"' in body
        assert "onExecutorTypeChange" in body

    def test_workflow_shows_identity_dropdown(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'value="coder"' in body
        assert 'value="reviewer"' in body

    def test_workflow_shows_result_parser_dropdown(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'value="pr_url"' in body
        assert 'value="review_verdict"' in body

    def test_workflow_shows_shell_command_field(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prop-shell_command-" in body
        assert "stage-field-shell" in body

    def test_workflow_shows_save_properties_button(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "saveStageProperties" in body
        assert "Save Properties" in body

    def test_workflow_conditional_visibility_classes(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-field-claude" in body
        assert "stage-field-shell" in body
        assert "stage-field-not-internal" in body


# --- Workflow API ---


class TestApiListPipelines:
    def test_list_returns_all_pipelines(self, client):
        resp = client.get("/api/workflow/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert isinstance(data["data"], list)
        assert len(data["data"]) >= 2  # implementation + investigation from seed

    def test_list_includes_stages_and_loops(self, client):
        resp = client.get("/api/workflow/pipelines")
        pipelines = resp.json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        assert len(impl["stages"]) >= 1
        assert len(impl["loops"]) >= 1
        # Stages should have IDs
        assert "id" in impl["stages"][0]

    def test_list_pipeline_fields(self, client):
        resp = client.get("/api/workflow/pipelines")
        pipeline = resp.json()["data"][0]
        for key in ("id", "name", "description", "ticket_label", "is_default", "stages", "loops"):
            assert key in pipeline


class TestApiGetPipeline:
    def test_get_existing_pipeline(self, client):
        # Get list first to find an ID
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.get(f"/api/workflow/pipelines/{pid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["id"] == pid

    def test_get_nonexistent_pipeline(self, client):
        resp = client.get("/api/workflow/pipelines/99999")
        assert resp.status_code == 404
        data = resp.json()
        assert data["ok"] is False
        assert any("not found" in e for e in data["errors"])


class TestApiCreatePipeline:
    def test_create_pipeline(self, client):
        resp = client.post(
            "/api/workflow/pipelines",
            json={"name": "test_pipeline", "description": "A test"},
        )
        # Validation is skipped on create (no stages yet), pipeline is created
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "test_pipeline"
        assert data["data"]["stages"] == []

    def test_create_pipeline_then_add_stage(self, db_file):
        """Create pipeline, then add a stage — full happy path."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        # Create succeeds even with no stages (validation deferred)
        resp = client.post(
            "/api/workflow/pipelines",
            json={"name": "custom_pipe", "description": "Custom"},
        )
        assert resp.status_code == 200
        pid = resp.json()["data"]["id"]
        # Add a stage
        stage_resp = client.post(
            f"/api/workflow/pipelines/{pid}/stages",
            json={
                "name": "build",
                "stage_order": 1,
                "executor_type": "claude",
                "identity": "coder",
                "max_turns": 50,
                "timeout_minutes": 30,
            },
        )
        assert stage_resp.status_code == 200
        assert stage_resp.json()["data"]["name"] == "build"


class TestApiUpdatePipeline:
    def test_update_pipeline_metadata(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        original_name = pipelines[0]["name"]
        resp = client.patch(
            f"/api/workflow/pipelines/{pid}",
            json={"description": "Updated description"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["description"] == "Updated description"
        assert data["data"]["name"] == original_name

    def test_update_nonexistent_pipeline(self, client):
        resp = client.patch(
            "/api/workflow/pipelines/99999",
            json={"description": "Nope"},
        )
        assert resp.status_code == 404

    def test_update_unknown_field(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.patch(
            f"/api/workflow/pipelines/{pid}",
            json={"bogus_field": "value"},
        )
        assert resp.status_code == 400
        assert resp.json()["ok"] is False


class TestApiDeletePipeline:
    def test_delete_non_default_pipeline(self, client):
        # Investigation pipeline is not default
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        non_default = next(p for p in pipelines if not p["is_default"])
        resp = client.delete(f"/api/workflow/pipelines/{non_default['id']}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        # Verify it's gone
        resp2 = client.get(f"/api/workflow/pipelines/{non_default['id']}")
        assert resp2.status_code == 404

    def test_delete_sole_default_pipeline_rejected(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        default_p = next(p for p in pipelines if p["is_default"])
        resp = client.delete(f"/api/workflow/pipelines/{default_p['id']}")
        assert resp.status_code == 400
        assert resp.json()["ok"] is False
        assert any("default" in e.lower() for e in resp.json()["errors"])

    def test_delete_nonexistent_pipeline(self, client):
        resp = client.delete("/api/workflow/pipelines/99999")
        assert resp.status_code == 404


class TestApiDuplicatePipeline:
    def test_duplicate_pipeline(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/duplicate",
            json={"name": "implementation_copy"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "implementation_copy"
        # Should have same number of stages
        assert len(data["data"]["stages"]) == len(pipelines[0]["stages"])

    def test_duplicate_requires_name(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/duplicate",
            json={},
        )
        assert resp.status_code == 400
        assert resp.json()["ok"] is False

    def test_duplicate_nonexistent(self, client):
        resp = client.post(
            "/api/workflow/pipelines/99999/duplicate",
            json={"name": "copy"},
        )
        assert resp.status_code == 404


class TestApiCreateStage:
    def test_create_stage(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        num_stages = len(pipelines[0]["stages"])
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/stages",
            json={
                "name": "new_stage",
                "stage_order": num_stages + 1,
                "executor_type": "claude",
                "identity": "coder",
                "max_turns": 50,
                "timeout_minutes": 30,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "new_stage"
        assert data["data"]["executor_type"] == "claude"

    def test_create_stage_nonexistent_pipeline(self, client):
        resp = client.post(
            "/api/workflow/pipelines/99999/stages",
            json={"name": "s", "stage_order": 1, "executor_type": "claude"},
        )
        assert resp.status_code == 404


class TestApiUpdateStage:
    def test_update_stage(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"max_turns": 999},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["max_turns"] == 999

    def test_update_prompt_template(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        new_prompt = "Updated prompt for {ticket_id} with {pr_url}"
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"prompt_template": new_prompt},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["prompt_template"] == new_prompt

    def test_update_nonexistent_stage(self, client):
        resp = client.patch(
            "/api/workflow/stages/99999",
            json={"max_turns": 10},
        )
        assert resp.status_code == 404

    def test_update_unknown_field(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"bogus": "value"},
        )
        assert resp.status_code == 400

    def test_update_executor_type(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"executor_type": "shell", "shell_command": "echo hello", "identity": None, "max_turns": None},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["executor_type"] == "shell"
        assert data["data"]["shell_command"] == "echo hello"
        assert data["data"]["identity"] is None
        assert data["data"]["max_turns"] is None

    def test_update_multiple_properties(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={
                "identity": "reviewer",
                "max_turns": 50,
                "timeout_minutes": 30,
                "result_parser": "review_verdict",
            },
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["identity"] == "reviewer"
        assert data["max_turns"] == 50
        assert data["timeout_minutes"] == 30
        assert data["result_parser"] == "review_verdict"

    def test_update_null_out_optional_fields(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"identity": None, "max_turns": None, "timeout_minutes": None, "result_parser": None},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["identity"] is None
        assert data["max_turns"] is None
        assert data["timeout_minutes"] is None
        assert data["result_parser"] is None

    def test_update_shell_command(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        # Find the shell stage (pr_checks)
        shell_stage = None
        for s in pipelines[0]["stages"]:
            if s["executor_type"] == "shell":
                shell_stage = s
                break
        assert shell_stage is not None
        new_cmd = "gh pr checks {pr_url} --watch --fail-fast"
        resp = client.patch(
            f"/api/workflow/stages/{shell_stage['id']}",
            json={"shell_command": new_cmd},
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["shell_command"] == new_cmd

    def test_update_response_includes_shell_command(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"timeout_minutes": 60},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert "shell_command" in data
        assert "executor_type" in data
        assert "identity" in data
        assert "max_turns" in data
        assert "timeout_minutes" in data
        assert "result_parser" in data


class TestApiDeleteStage:
    def test_delete_stage(self, client):
        # Add a stage first so we can delete it without breaking the pipeline
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        num_stages = len(pipelines[0]["stages"])
        create_resp = client.post(
            f"/api/workflow/pipelines/{pid}/stages",
            json={
                "name": "disposable",
                "stage_order": num_stages + 1,
                "executor_type": "shell",
            },
        )
        stage_id = create_resp.json()["data"]["id"]
        resp = client.delete(f"/api/workflow/stages/{stage_id}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_delete_nonexistent_stage(self, client):
        resp = client.delete("/api/workflow/stages/99999")
        assert resp.status_code == 404


class TestApiReorderStages:
    def test_reorder_stages(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        stage_ids = [s["id"] for s in pipelines[0]["stages"]]
        reversed_ids = list(reversed(stage_ids))
        resp = client.put(
            f"/api/workflow/pipelines/{pid}/stages/order",
            json={"stage_ids": reversed_ids},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        new_order = [s["id"] for s in data["data"]["stages"]]
        assert new_order == reversed_ids

    def test_reorder_nonexistent_pipeline(self, client):
        resp = client.put(
            "/api/workflow/pipelines/99999/stages/order",
            json={"stage_ids": [1, 2]},
        )
        assert resp.status_code == 404

    def test_reorder_invalid_ids(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.put(
            f"/api/workflow/pipelines/{pid}/stages/order",
            json={"stage_ids": [99998, 99999]},
        )
        assert resp.status_code == 400

    def test_reorder_missing_stage_ids(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.put(
            f"/api/workflow/pipelines/{pid}/stages/order",
            json={"not_stage_ids": [1]},
        )
        assert resp.status_code == 400


class TestApiCreateLoop:
    def test_create_loop(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        stages = pipelines[0]["stages"]
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/loops",
            json={
                "name": "test_loop",
                "start_stage": stages[0]["name"],
                "end_stage": stages[1]["name"],
                "max_iterations": 2,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "test_loop"
        assert data["data"]["max_iterations"] == 2

    def test_create_loop_nonexistent_pipeline(self, client):
        resp = client.post(
            "/api/workflow/pipelines/99999/loops",
            json={
                "name": "l",
                "start_stage": "a",
                "end_stage": "b",
                "max_iterations": 1,
            },
        )
        assert resp.status_code == 404

    def test_create_loop_invalid_stages(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        loops_before = len(pipelines[0]["loops"])
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/loops",
            json={
                "name": "bad_loop",
                "start_stage": "nonexistent_stage",
                "end_stage": "also_nonexistent",
                "max_iterations": 1,
            },
        )
        assert resp.status_code == 400
        assert resp.json()["ok"] is False
        # Verify the invalid loop was rolled back from the DB
        updated = client.get(f"/api/workflow/pipelines/{pid}").json()["data"]
        assert len(updated["loops"]) == loops_before


class TestApiUpdateLoop:
    def test_update_loop(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        loop = impl["loops"][0]
        resp = client.patch(
            f"/api/workflow/loops/{loop['id']}",
            json={"max_iterations": 10},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["max_iterations"] == 10

    def test_update_nonexistent_loop(self, client):
        resp = client.patch(
            "/api/workflow/loops/99999",
            json={"max_iterations": 5},
        )
        assert resp.status_code == 404

    def test_update_unknown_field(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        loop = impl["loops"][0]
        resp = client.patch(
            f"/api/workflow/loops/{loop['id']}",
            json={"bogus_field": "value"},
        )
        assert resp.status_code == 400


class TestApiDeleteLoop:
    def test_delete_loop(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        loop = impl["loops"][0]
        resp = client.delete(f"/api/workflow/loops/{loop['id']}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_delete_nonexistent_loop(self, client):
        resp = client.delete("/api/workflow/loops/99999")
        assert resp.status_code == 404
        assert resp.json()["ok"] is False
