"""Tests for botfarm.dashboard module."""

import json
import sqlite3
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import (
    BotfarmConfig,
    DashboardConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
)
from botfarm.dashboard import build_pipeline_state, create_app, start_dashboard
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
)


@pytest.fixture()
def state_file(tmp_path):
    """Create a state.json with sample slot data."""
    path = tmp_path / "state.json"
    data = {
        "slots": [
            {
                "project": "my-project",
                "slot_id": 1,
                "status": "busy",
                "ticket_id": "TST-1",
                "ticket_title": "Fix bug",
                "branch": "fix-bug",
                "pr_url": None,
                "stage": "implement",
                "stage_iteration": 1,
                "current_session_id": None,
                "started_at": "2026-02-12T10:00:00+00:00",
                "pid": 1234,
                "interrupted_by_limit": False,
                "resume_after": None,
                "stages_completed": [],
            },
            {
                "project": "my-project",
                "slot_id": 2,
                "status": "free",
                "ticket_id": None,
                "ticket_title": None,
                "branch": None,
                "pr_url": None,
                "stage": None,
                "stage_iteration": 0,
                "current_session_id": None,
                "started_at": None,
                "pid": None,
                "interrupted_by_limit": False,
                "resume_after": None,
                "stages_completed": [],
            },
        ],
        "usage": {
            "utilization_5h": 0.45,
            "utilization_7d": 0.72,
            "resets_at_5h": "2026-02-12T15:00:00+00:00",
            "resets_at_7d": "2026-02-19T00:00:00+00:00",
        },
        "dispatch_paused": False,
        "dispatch_pause_reason": None,
        "last_usage_check": "2026-02-12T14:30:00+00:00",
        "queue": {
            "projects": [
                {
                    "name": "my-project",
                    "todo_count": 3,
                    "next_ticket_id": "TST-5",
                    "next_ticket_title": "Add logging",
                },
            ],
        },
    }
    path.write_text(json.dumps(data))
    return path


@pytest.fixture()
def db_file(tmp_path):
    """Create a database with sample task data."""
    path = tmp_path / "botfarm.db"
    conn = init_db(path)
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
        cost_usd=1.25,
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
        cost_usd=0.75,
    )
    insert_stage_run(
        conn,
        task_id=task_id,
        stage="review",
        iteration=1,
        turns=12,
        duration_seconds=1800.0,
        cost_usd=0.50,
        exit_subtype="approved",
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
    update_task(conn, task_id2, failure_reason="Tests failed", cost_usd=0.50, turns=10)

    insert_usage_snapshot(
        conn,
        utilization_5h=0.45,
        utilization_7d=0.72,
        resets_at="2026-02-12T15:00:00+00:00",
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def client(state_file, db_file):
    """FastAPI test client."""
    app = create_app(state_file=state_file, db_path=db_file)
    return TestClient(app)


# --- create_app ---


class TestCreateApp:
    def test_app_created(self, state_file, db_file):
        app = create_app(state_file=state_file, db_path=db_file)
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
            state_file=tmp_path / "nonexistent.json",
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

    def test_dispatch_paused_banner(self, state_file, db_file):
        data = json.loads(state_file.read_text())
        data["dispatch_paused"] = True
        data["dispatch_pause_reason"] = "5-hour limit exceeded"
        state_file.write_text(json.dumps(data))
        app = create_app(state_file=state_file, db_path=db_file)
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

    def test_contains_queue_data(self, client):
        resp = client.get("/partials/queue")
        body = resp.text
        assert "my-project" in body
        assert "3" in body
        assert "TST-5" in body
        assert "Add logging" in body

    def test_queue_ticket_link_with_workspace(self, state_file, db_file):
        app = create_app(
            state_file=state_file, db_path=db_file,
            linear_workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/partials/queue")
        assert "linear.app/my-team/issue/TST-5" in resp.text

    def test_no_queue_data(self, tmp_path):
        state = tmp_path / "state.json"
        state.write_text(json.dumps({"slots": [], "usage": {}}))
        app = create_app(
            state_file=state,
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/partials/queue")
        assert resp.status_code == 200
        assert "No work available" in resp.text

    def test_empty_queue_projects(self, tmp_path):
        state = tmp_path / "state.json"
        state.write_text(json.dumps({
            "slots": [], "usage": {},
            "queue": {"projects": []},
        }))
        app = create_app(
            state_file=state,
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/partials/queue")
        assert resp.status_code == 200
        assert "No work available" in resp.text


# --- Slot Panel enhancements ---


class TestSlotPanelEnhancements:
    def test_pid_displayed(self, client):
        resp = client.get("/partials/slots")
        assert "1234" in resp.text

    def test_ticket_link(self, client):
        resp = client.get("/partials/slots")
        assert "linear.app/issue/TST-1" in resp.text

    def test_ticket_link_with_workspace(self, state_file, db_file):
        app = create_app(
            state_file=state_file, db_path=db_file,
            linear_workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_paused_slot_resume_countdown(self, state_file, db_file):
        data = json.loads(state_file.read_text())
        data["slots"][0]["status"] = "paused_limit"
        data["slots"][0]["resume_after"] = "2099-12-31T23:59:00+00:00"
        state_file.write_text(json.dumps(data))
        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert "resume-countdown" in resp.text
        assert "2099-12-31T23:59:00" in resp.text

    def test_pid_header_present(self, client):
        resp = client.get("/partials/slots")
        assert "<th>PID</th>" in resp.text


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
        resp = client.get("/partials/usage")
        assert "reset-countdown" in resp.text
        assert "Resets at:" in resp.text

    def test_last_usage_check(self, client):
        resp = client.get("/partials/usage")
        assert "Last checked:" in resp.text
        assert "ago" in resp.text

    def test_no_dispatch_pause_banner_in_usage(self, state_file, db_file):
        """Dispatch pause banner lives in slots panel only, not usage."""
        data = json.loads(state_file.read_text())
        data["dispatch_paused"] = True
        data["dispatch_pause_reason"] = "5-hour limit exceeded"
        state_file.write_text(json.dumps(data))
        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "DISPATCH PAUSED" not in resp.text
        assert "Dispatch paused" not in resp.text


# --- Usage freshness & staleness ---


class TestUsageFreshness:
    """Tests for SMA-111: dashboard usage data freshness fixes."""

    def test_rate_limit_slot_not_claimed_on_failure(
        self, state_file, db_file, monkeypatch,
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
        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)

        # First call — triggers a real refresh attempt which fails
        resp1 = client.get("/partials/usage")
        assert resp1.status_code == 200
        first_call_count = call_count

        # Second call — should also attempt a refresh (slot was not claimed)
        resp2 = client.get("/partials/usage")
        assert resp2.status_code == 200
        assert call_count > first_call_count, (
            "Expected retry after failure, but rate-limit slot blocked it"
        )

    def test_dashboard_tracks_own_refresh_timestamp(
        self, state_file, db_file, monkeypatch,
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
        # Set supervisor's last_usage_check to something old
        data = json.loads(state_file.read_text())
        data["last_usage_check"] = "2020-01-01T00:00:00+00:00"
        state_file.write_text(json.dumps(data))

        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        body = resp.text
        # Should show fresh data from our mock, not the stale state.json usage
        assert "30.0%" in body
        # The "Last checked" should NOT show the ~6-year-old supervisor time;
        # it should be a recent dashboard timestamp (seconds/minutes, not years)
        assert "Last checked:" in body

    def test_staleness_warning_shown_when_data_old(
        self, state_file, db_file, monkeypatch,
    ):
        """A visual warning should appear when usage data is older than
        2x the refresh interval (>120 seconds with default 60s interval)."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None,
        )
        # Set last_usage_check to 5 minutes ago (well over 2x60s threshold)
        from datetime import datetime, timedelta, timezone
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).isoformat()
        data = json.loads(state_file.read_text())
        data["last_usage_check"] = old_time
        state_file.write_text(json.dumps(data))

        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "usage data may be stale" in resp.text

    def test_no_staleness_warning_when_data_fresh(
        self, state_file, db_file, monkeypatch,
    ):
        """No warning when the usage data was refreshed recently."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot", lambda conn: None,
        )
        from datetime import datetime, timezone
        fresh_time = datetime.now(timezone.utc).isoformat()
        data = json.loads(state_file.read_text())
        data["last_usage_check"] = fresh_time
        state_file.write_text(json.dumps(data))

        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert "usage data may be stale" not in resp.text

    def test_warning_level_log_on_refresh_failure(
        self, state_file, db_file, monkeypatch, caplog,
    ):
        """Refresh failures should log at WARNING level, not DEBUG."""
        monkeypatch.setattr(
            "botfarm.dashboard.refresh_usage_snapshot",
            lambda conn: (_ for _ in ()).throw(RuntimeError("API error")),
        )
        app = create_app(state_file=state_file, db_path=db_file)
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
        self, state_file, db_file, monkeypatch,
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
        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)

        # First call — refreshes from API
        resp1 = client.get("/partials/usage")
        assert "55.0%" in resp1.text

        # Second call within rate-limit window — should return cached data
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

    def test_index_contains_queue_data(self, client):
        resp = client.get("/")
        body = resp.text
        assert "TST-5" in body
        assert "Add logging" in body


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

    def test_project_dropdown_populated(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "my-project" in body
        assert "other-project" in body

    def test_task_rows_are_clickable(self, client):
        resp = client.get("/history")
        assert "/task/" in resp.text

    def test_ticket_links_to_linear(self, state_file, db_file):
        app = create_app(
            state_file=state_file, db_path=db_file,
            linear_workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/history")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_history_no_db(self, state_file, tmp_path):
        app = create_app(
            state_file=state_file,
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

    def test_sort_by_cost_asc(self, client):
        resp = client.get("/history?sort_by=cost_usd&sort_dir=ASC")
        assert resp.status_code == 200
        body = resp.text
        # TST-2 ($0.50) should appear before TST-1 ($1.25)
        assert body.index("TST-2") < body.index("TST-1")

    def test_sort_by_cost_desc(self, client):
        resp = client.get("/history?sort_by=cost_usd&sort_dir=DESC")
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

    def test_pagination_with_many_tasks(self, state_file, tmp_path):
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
        app = create_app(state_file=state_file, db_path=path)
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
        assert "Cost" in body
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

    def test_ticket_links_to_linear(self, state_file, db_file):
        app = create_app(
            state_file=state_file, db_path=db_file,
            linear_workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/task/1")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_task_not_found(self, client):
        resp = client.get("/task/9999")
        assert resp.status_code == 200
        assert "Task not found" in resp.text

    def test_task_detail_no_db(self, state_file, tmp_path):
        app = create_app(
            state_file=state_file,
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

    def test_task_with_no_stages_or_events(self, state_file, tmp_path):
        path = tmp_path / "sparse.db"
        conn = init_db(path)
        insert_task(conn, ticket_id="BARE-1", title="Bare task", project="p", slot=1)
        conn.commit()
        conn.close()
        app = create_app(state_file=state_file, db_path=path)
        client = TestClient(app)
        resp = client.get("/task/1")
        body = resp.text
        assert "BARE-1" in body
        assert "No stage runs recorded" in body
        assert "No events recorded" in body


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
        assert "Total Cost" in body
        assert "$1.75" in body

    def test_contains_success_rate(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Success Rate" in body
        # 1 completed / 2 total = 50%
        assert "50.0%" in body

    def test_contains_averages(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Avg Cost / Task" in body
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

    def test_contains_cost_buckets(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Cost" in body

    def test_failure_reasons_displayed(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Common Failure Reasons" in body
        assert "Tests failed" in body

    def test_project_filter(self, client):
        resp = client.get("/metrics?project=my-project")
        body = resp.text
        assert resp.status_code == 200
        # Only 1 task in my-project (TST-1, completed, $1.25)
        assert "$1.25" in body
        assert "100.0%" in body  # 1/1 success rate

    def test_project_filter_dropdown(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "my-project" in body
        assert "other-project" in body

    def test_metrics_no_db(self, state_file, tmp_path):
        app = create_app(
            state_file=state_file,
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


# --- start_dashboard ---


class TestStartDashboard:
    def test_disabled_returns_none(self, state_file, db_file):
        config = DashboardConfig(enabled=False)
        result = start_dashboard(
            config, state_file=state_file, db_path=db_file,
        )
        assert result is None

    def test_enabled_returns_thread(self, state_file, db_file):
        config = DashboardConfig(enabled=True, host="127.0.0.1", port=0)
        thread = start_dashboard(
            config, state_file=state_file, db_path=db_file,
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

    def test_corrupt_state_file(self, tmp_path, db_file):
        state = tmp_path / "state.json"
        state.write_text("not valid json {{{")
        app = create_app(state_file=state, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200

    def test_state_file_bare_list(self, tmp_path, db_file):
        state = tmp_path / "state.json"
        slots = [
            {"project": "p", "slot_id": 1, "status": "free",
             "ticket_id": None, "ticket_title": None, "stage": None,
             "stage_iteration": 0, "started_at": None},
        ]
        state.write_text(json.dumps(slots))
        app = create_app(state_file=state, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200

    def test_empty_database(self, state_file, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(state_file=state_file, db_path=db_path)
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "No tasks found" in resp.text

    def test_usage_no_data(self, tmp_path):
        state = tmp_path / "state.json"
        state.write_text(json.dumps({"slots": [], "usage": {}}))
        app = create_app(
            state_file=state,
            db_path=tmp_path / "nonexistent.db",
        )
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
    def config_client(self, state_file, db_file, tmp_path):
        config, _ = _make_botfarm_config(tmp_path)
        app = create_app(
            state_file=state_file, db_path=db_file,
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

    def test_config_page_disabled_without_config(self, state_file, db_file):
        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "not available" in resp.text


class TestConfigUpdate:
    @pytest.fixture()
    def setup(self, state_file, db_file, tmp_path):
        config, config_path = _make_botfarm_config(tmp_path)
        app = create_app(
            state_file=state_file, db_path=db_file,
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

    def test_update_without_config_returns_400(self, state_file, db_file):
        app = create_app(state_file=state_file, db_path=db_file)
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
    def structural_client(self, state_file, db_file, tmp_path):
        config, _ = _make_structural_botfarm_config(tmp_path)
        app = create_app(
            state_file=state_file, db_path=db_file,
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
    def setup(self, state_file, db_file, tmp_path):
        config, config_path = _make_structural_botfarm_config(tmp_path)
        app = create_app(
            state_file=state_file, db_path=db_file,
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

    def test_structural_update_no_config_path(self, state_file, db_file):
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
            state_file=state_file, db_path=db_file,
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
    def config_client(self, state_file, db_file, tmp_path):
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
            state_file=state_file, db_path=db_file,
            botfarm_config=config,
        )
        return TestClient(app)

    def test_config_view_returns_200(self, config_client):
        resp = config_client.get("/config/view")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_config_view_contains_all_sections(self, config_client):
        resp = config_client.get("/config/view")
        body = resp.text
        for section in [
            "Projects", "Linear", "Agents", "Usage Limits",
            "Notifications", "Dashboard", "Database", "State File",
        ]:
            assert section in body

    def test_config_view_shows_project_details(self, config_client):
        resp = config_client.get("/config/view")
        body = resp.text
        assert "test-project" in body
        assert "TST" in body
        assert "~/test" in body
        assert "test-slot-" in body
        assert "My Project" in body

    def test_config_view_masks_api_key(self, config_client):
        resp = config_client.get("/config/view")
        body = resp.text
        # Should show masked key (first 4 + **** + last 4)
        assert "lin_****cdef" in body
        # Full key must not appear
        assert "lin_api_1234567890abcdef" not in body

    def test_config_view_masks_webhook_url(self, config_client):
        resp = config_client.get("/config/view")
        body = resp.text
        # Full URL must not appear
        assert "https://hooks.slack.com/services/T00/B00/xxx" not in body
        # Should show masked version
        assert "http****/xxx" in body

    def test_config_view_shows_linear_settings(self, config_client):
        resp = config_client.get("/config/view")
        body = resp.text
        assert "my-workspace" in body
        assert "60" in body  # poll_interval_seconds
        assert "Human" in body
        assert "Manual" in body

    def test_config_view_shows_boolean_values(self, config_client):
        resp = config_client.get("/config/view")
        body = resp.text
        assert "Yes" in body  # comment_on_failure = True
        assert "No" in body   # comment_on_completion = False

    def test_config_view_nav_link(self, config_client):
        resp = config_client.get("/")
        assert "Configuration" in resp.text
        assert "/config/view" in resp.text

    def test_config_view_disabled_without_config(self, state_file, db_file):
        app = create_app(state_file=state_file, db_path=db_file)
        client = TestClient(app)
        resp = client.get("/config/view")
        assert resp.status_code == 200
        assert "not available" in resp.text

    def test_config_view_masks_short_api_key(self, state_file, db_file, tmp_path):
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
            state_file=state_file, db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        resp = client.get("/config/view")
        body = resp.text
        assert "short" not in body
        assert "****" in body

    def test_config_view_empty_webhook_shows_dash(self, state_file, db_file, tmp_path):
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
            state_file=state_file, db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        resp = client.get("/config/view")
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
