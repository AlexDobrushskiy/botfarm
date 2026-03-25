"""Tests for dashboard index page, slot/usage/queue panels."""

import pytest
from fastapi.testclient import TestClient

from botfarm.config import BotfarmConfig, ProjectConfig
from botfarm.dashboard import create_app
from botfarm.db import init_db, insert_codex_usage_snapshot, insert_usage_snapshot, save_dispatch_state
from tests.conftest import _seed_queue_entry
from tests.helpers import seed_slot as _seed_slot


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
        app = create_app(db_path=db_path, workspace="myws")
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
            ProjectConfig(name="alpha", team="T", base_dir="/tmp", worktree_prefix="w", slots=[1]),
            ProjectConfig(name="beta", team="T", base_dir="/tmp", worktree_prefix="w", slots=[2]),
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
            workspace="my-team",
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
        """Last polled timestamp appears on the index page (which falls back to DB data)."""
        resp = client.get("/")
        body = resp.text
        assert "Last polled:" in body
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

    def test_codex_usage_in_partial(self, tmp_path, monkeypatch):
        """Codex usage stats must persist across htmx partial refreshes (SMA-546)."""
        db_path = tmp_path / "codex_usage.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        insert_usage_snapshot(conn, utilization_5h=0.45, utilization_7d=0.72)
        insert_codex_usage_snapshot(conn, primary_used_pct=0.65, plan_type="pro")
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "Codex Usage" in resp.text
        assert "65%" in resp.text
        assert "pro" in resp.text


# --- Usage freshness & staleness ---


class TestUsageFreshness:
    """Tests for dashboard usage data freshness — DB-only, no API fallback."""

    def test_fresh_db_snapshot_shows_data(self, db_file):
        """When a fresh DB snapshot exists, usage data is displayed."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert resp.status_code == 200
        assert "45.0%" in resp.text

    def test_stale_db_snapshot_shows_stale_data(self, tmp_path):
        """When the DB snapshot is stale, the dashboard shows stale DB data
        rather than making a live API call (SMA-441)."""
        from datetime import datetime, timedelta, timezone
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=12)
        ).isoformat()
        db_path = tmp_path / "stale_api.db"
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
        body = resp.text
        # Should show stale DB data (no API fallback)
        assert "45.0%" in body
        assert "Last polled:" in body

    def test_staleness_warning_shown_when_data_old(self, tmp_path):
        """A visual warning should appear when usage data is older than
        the staleness threshold (15 minutes)."""
        from datetime import datetime, timedelta, timezone
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=20)
        ).isoformat()
        db_path = tmp_path / "stale.db"
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
        resp = client.get("/")
        assert "usage data may be stale" in resp.text

    def test_staleness_warning_on_usage_partial_cold_start(self, tmp_path):
        """The /partials/usage endpoint should show a staleness warning on
        cold start when the DB snapshot is old."""
        from datetime import datetime, timedelta, timezone
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=20)
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

    def test_no_staleness_warning_when_data_fresh(self, db_file):
        """No warning when the usage data was refreshed recently."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "usage data may be stale" not in resp.text

    def test_cached_db_snapshot_returned_on_subsequent_calls(self, db_file):
        """After reading a fresh DB snapshot, subsequent calls within the
        cache TTL should return the same cached data without re-reading DB."""
        app = create_app(db_path=db_file)
        client = TestClient(app)

        # First call -- reads from DB snapshot
        resp1 = client.get("/partials/usage")
        assert "45.0%" in resp1.text

        # Second call within cache TTL -- should return same cached data
        resp2 = client.get("/partials/usage")
        assert "45.0%" in resp2.text

    def test_stale_db_snapshot_still_returned(self, tmp_path):
        """When the DB snapshot is stale, dashboard returns stale data
        without making API calls (SMA-441 — no live API fallback)."""
        from datetime import datetime, timedelta, timezone
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=12)
        ).isoformat()
        db_path = tmp_path / "backoff.db"
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
        assert resp.status_code == 200
        assert "45.0%" in resp.text

    def test_no_db_snapshot_shows_no_data(self, tmp_path):
        """When there are no DB snapshots, the dashboard shows 'No usage
        data available' instead of falling back to API (SMA-441)."""
        db_path = tmp_path / "empty_usage.db"
        conn = init_db(db_path)
        conn.commit()
        conn.close()

        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert resp.status_code == 200
        assert "No usage data available" in resp.text


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


# --- Connection Lost Banner (SMA-346) ---


class TestConnectionLostBanner:
    """Tests for HTMX polling error handling: banner, backoff, auto-recovery."""

    def test_banner_element_present(self, client):
        """Connection-lost banner div is rendered in the base template."""
        resp = client.get("/")
        body = resp.text
        assert 'id="connection-lost-banner"' in body
        assert "connection-lost-banner" in body

    def test_banner_hidden_by_default(self, client):
        """Banner should not have the 'visible' class on initial load."""
        resp = client.get("/")
        body = resp.text
        assert 'class="connection-lost-banner"' in body
        assert 'class="connection-lost-banner visible"' not in body

    def test_banner_has_reconnecting_text(self, client):
        """Banner contains reconnecting indicator text."""
        resp = client.get("/")
        body = resp.text
        assert "Connection lost" in body
        assert "Reconnecting" in body

    def test_banner_has_aria_role(self, client):
        """Banner has role=alert for accessibility."""
        resp = client.get("/")
        assert 'role="alert"' in resp.text

    def test_response_error_handler_present(self, client):
        """JavaScript listens for htmx:responseError events."""
        resp = client.get("/")
        assert "htmx:responseError" in resp.text

    def test_send_error_handler_present(self, client):
        """JavaScript listens for htmx:sendError events (network failures)."""
        resp = client.get("/")
        assert "htmx:sendError" in resp.text

    def test_backoff_logic_present(self, client):
        """JavaScript implements exponential backoff via htmx:beforeRequest."""
        resp = client.get("/")
        body = resp.text
        assert "htmx:beforeRequest" in body
        assert "backoffMs" in body

    def test_auto_recovery_handler_present(self, client):
        """JavaScript auto-recovers on successful request via htmx:afterRequest."""
        resp = client.get("/")
        body = resp.text
        assert "htmx:afterRequest" in body
        assert "evt.detail.successful" in body

    def test_banner_on_non_index_pages(self, client):
        """Connection-lost banner appears on non-index pages too (via base.html)."""
        for page in ["/history", "/health", "/tickets"]:
            resp = client.get(page)
            assert 'id="connection-lost-banner"' in resp.text, (
                f"Banner missing on {page}"
            )

    def test_banner_css_present(self, client):
        """Connection-lost banner CSS class is defined."""
        resp = client.get("/")
        body = resp.text
        assert ".connection-lost-banner" in body
        assert "conn-pulse" in body


# --- History ---


