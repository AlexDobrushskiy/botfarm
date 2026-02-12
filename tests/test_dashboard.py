"""Tests for botfarm.dashboard module."""

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from botfarm.config import DashboardConfig
from botfarm.dashboard import create_app, start_dashboard
from botfarm.db import init_db, insert_task, insert_usage_snapshot, update_task


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
    )
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

    def test_history_no_db(self, state_file, tmp_path):
        app = create_app(
            state_file=state_file,
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "No tasks found" in resp.text


class TestPartialHistory:
    def test_returns_200(self, client):
        resp = client.get("/partials/history")
        assert resp.status_code == 200

    def test_contains_task_data(self, client):
        resp = client.get("/partials/history")
        assert "TST-1" in resp.text


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
        assert "$1.25" in body

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
