"""Tests for botfarm.dashboard module."""

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from botfarm.config import DashboardConfig
from botfarm.dashboard import create_app, start_dashboard
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

    def test_contains_snapshot_table(self, client):
        resp = client.get("/usage")
        body = resp.text
        assert "45.0" in body
        assert "72.0" in body

    def test_contains_chart(self, client):
        resp = client.get("/usage")
        assert "usage-chart" in resp.text
        assert "chart.js" in resp.text.lower()


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
        assert "$1.75" in body

    def test_metrics_no_db(self, state_file, tmp_path):
        app = create_app(
            state_file=state_file,
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "Total Tasks" in resp.text


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
