"""Tests for POST /api/dispatch endpoint (manual ticket dispatch)."""

from __future__ import annotations

import json
import sqlite3

import pytest
from fastapi.testclient import TestClient

from botfarm.dashboard import create_app
from botfarm.db import init_db
from botfarm.bugtracker import Issue
from tests.helpers import seed_slot as _seed_slot


@pytest.fixture()
def db_file(tmp_path):
    """Create a minimal database for dispatch tests."""
    path = tmp_path / "test.db"
    conn = init_db(path)
    _seed_slot(conn, "my-project", 1, status="free")
    conn.commit()
    conn.close()
    return path


def _seed_queue(db_path, project, ticket_id, *, blocked_by=None):
    """Insert a single queue_entries row for testing."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    blocked_json = json.dumps(blocked_by) if blocked_by else None
    conn.execute(
        "INSERT INTO queue_entries "
        "(project, position, ticket_id, ticket_title, priority, sort_order, url, snapshot_at, blocked_by) "
        "VALUES (?, 0, ?, 'Test ticket', 2, 0.0, 'https://example.com', '2026-01-01T00:00:00Z', ?)",
        (project, ticket_id, blocked_json),
    )
    conn.commit()
    conn.close()


class TestDispatchEndpoint:
    """Tests for POST /api/dispatch endpoint."""

    def test_dispatch_calls_callback(self, db_file):
        """POST /api/dispatch calls on_dispatch_ticket with project and ticket_id."""
        called = []

        def fake_cb(project, ticket_id, pipeline_id=None):
            called.append((project, ticket_id, pipeline_id))
            return {"success": True, "slot_id": 1}

        app = create_app(db_path=db_file, on_dispatch_ticket=fake_cb)
        client = TestClient(app)
        resp = client.post(
            "/api/dispatch",
            json={"project": "my-project", "ticket_id": "TST-1"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["slot_id"] == 1
        assert called == [("my-project", "TST-1", None)]

    def test_dispatch_passes_pipeline_id(self, db_file):
        """POST /api/dispatch forwards pipeline_id to callback."""
        called = []

        def fake_cb(project, ticket_id, pipeline_id=None):
            called.append((project, ticket_id, pipeline_id))
            return {"success": True, "slot_id": 1}

        app = create_app(db_path=db_file, on_dispatch_ticket=fake_cb)
        client = TestClient(app)
        resp = client.post(
            "/api/dispatch",
            json={"project": "my-project", "ticket_id": "TST-1", "pipeline_id": 2},
        )
        assert resp.status_code == 200
        assert called == [("my-project", "TST-1", 2)]

    def test_dispatch_invalid_pipeline_id_returns_400(self, db_file):
        """POST /api/dispatch returns 400 for non-integer pipeline_id."""
        app = create_app(
            db_path=db_file,
            on_dispatch_ticket=lambda p, t, pid=None: {"success": True, "slot_id": 1},
        )
        client = TestClient(app)
        for bad_val in ["abc", True, [1], {"x": 1}]:
            resp = client.post(
                "/api/dispatch",
                json={"project": "proj", "ticket_id": "TST-1", "pipeline_id": bad_val},
            )
            assert resp.status_code == 400, f"Expected 400 for pipeline_id={bad_val!r}"
            assert "pipeline_id" in resp.json()["error"]

    def test_dispatch_error_returns_409(self, db_file):
        """POST /api/dispatch returns 409 when callback returns an error."""
        def fake_cb(project, ticket_id, pipeline_id=None):
            return {"error": "No free slot available"}

        app = create_app(db_path=db_file, on_dispatch_ticket=fake_cb)
        client = TestClient(app)
        resp = client.post(
            "/api/dispatch",
            json={"project": "my-project", "ticket_id": "TST-1"},
        )
        assert resp.status_code == 409
        assert "No free slot" in resp.json()["error"]

    def test_dispatch_without_callback_returns_503(self, db_file):
        """POST /api/dispatch returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post(
            "/api/dispatch",
            json={"project": "proj", "ticket_id": "TST-1"},
        )
        assert resp.status_code == 503

    def test_dispatch_missing_project_returns_400(self, db_file):
        """POST /api/dispatch returns 400 when project is missing."""
        app = create_app(
            db_path=db_file,
            on_dispatch_ticket=lambda p, t, pid=None: {"success": True, "slot_id": 1},
        )
        client = TestClient(app)
        resp = client.post("/api/dispatch", json={"ticket_id": "TST-1"})
        assert resp.status_code == 400
        assert "project" in resp.json()["error"]

    def test_dispatch_missing_ticket_id_returns_400(self, db_file):
        """POST /api/dispatch returns 400 when ticket_id is missing."""
        app = create_app(
            db_path=db_file,
            on_dispatch_ticket=lambda p, t, pid=None: {"success": True, "slot_id": 1},
        )
        client = TestClient(app)
        resp = client.post("/api/dispatch", json={"project": "proj"})
        assert resp.status_code == 400
        assert "ticket_id" in resp.json()["error"]

    def test_dispatch_non_string_project_returns_400(self, db_file):
        """POST /api/dispatch rejects non-string project values."""
        app = create_app(
            db_path=db_file,
            on_dispatch_ticket=lambda p, t, pid=None: {"success": True, "slot_id": 1},
        )
        client = TestClient(app)
        for payload in [1, ["a"], {"x": 1}, True, 0, False]:
            resp = client.post(
                "/api/dispatch",
                json={"project": payload, "ticket_id": "TST-1"},
            )
            assert resp.status_code == 400, f"Expected 400 for project={payload!r}"

    def test_dispatch_non_string_ticket_id_returns_400(self, db_file):
        """POST /api/dispatch rejects non-string ticket_id values."""
        app = create_app(
            db_path=db_file,
            on_dispatch_ticket=lambda p, t, pid=None: {"success": True, "slot_id": 1},
        )
        client = TestClient(app)
        for payload in [1, ["a"], {"x": 1}, True, 0]:
            resp = client.post(
                "/api/dispatch",
                json={"project": "proj", "ticket_id": payload},
            )
            assert resp.status_code == 400, f"Expected 400 for ticket_id={payload!r}"

    def test_dispatch_non_dict_body_returns_400(self, db_file):
        """POST /api/dispatch returns 400 when body is not a JSON object."""
        app = create_app(
            db_path=db_file,
            on_dispatch_ticket=lambda p, t, pid=None: {"success": True, "slot_id": 1},
        )
        client = TestClient(app)
        for payload in ["[]", '"string"', "123", "null"]:
            resp = client.post(
                "/api/dispatch",
                content=payload,
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 400, f"Expected 400 for body={payload!r}"

    def test_dispatch_invalid_json_returns_400(self, db_file):
        """POST /api/dispatch returns 400 for invalid JSON."""
        app = create_app(
            db_path=db_file,
            on_dispatch_ticket=lambda p, t, pid=None: {"success": True, "slot_id": 1},
        )
        client = TestClient(app)
        resp = client.post(
            "/api/dispatch",
            content="not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400


class TestExecuteDispatchTicket:
    """Tests for _execute_dispatch_ticket validation logic in supervisor_ops."""

    @pytest.fixture()
    def _mock_supervisor(self, tmp_path, monkeypatch):
        """Build a minimal mock of the supervisor OperationsMixin for testing."""
        from unittest.mock import MagicMock, PropertyMock
        from botfarm.config import ProjectConfig, BotfarmConfig, LinearConfig, DatabaseConfig, UsageLimitsConfig
        from botfarm.db import init_db, save_dispatch_state
        from botfarm.slots import SlotManager

        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(conn, paused=False)
        conn.commit()

        # Create a minimal "self" object that has the attributes
        # _execute_dispatch_ticket needs
        sup = MagicMock()
        sup._conn = conn
        sup._projects = {
            "my-project": ProjectConfig(
                name="my-project",
                team="TST",
                base_dir=str(tmp_path),
                worktree_prefix="my-project-slot-",
                slots=[1],
                dispatch_mode="semi-auto",
            ),
        }
        sup._config = BotfarmConfig(
            projects=list(sup._projects.values()),
            bugtracker=LinearConfig(api_key="test-key"),
            database=DatabaseConfig(),
            usage_limits=UsageLimitsConfig(),
        )

        slot_mgr = SlotManager(db_path, conn=conn)
        slot_mgr.register_slot("my-project", 1)
        slot_mgr.load()
        sup._slot_manager = slot_mgr

        usage_state = MagicMock()
        usage_state.should_pause_with_thresholds.return_value = (False, None)
        usage_poller = MagicMock()
        usage_poller.state = usage_state
        sup._usage_poller = usage_poller

        codex_usage_state = MagicMock()
        codex_usage_state.should_pause.return_value = (False, None)
        codex_usage_poller = MagicMock()
        codex_usage_poller.state = codex_usage_state
        sup._codex_usage_poller = codex_usage_poller

        poller = MagicMock()
        poll_result = MagicMock()
        issue = Issue(
            id="uuid-1",
            identifier="TST-1",
            title="Test ticket",
            priority=2,
            url="https://example.com/TST-1",
            sort_order=0.0,
        )
        poll_result.candidates = [issue]
        poll_result.blocked = []
        poller.poll.return_value = poll_result
        sup._pollers = {"my-project": poller}

        sup._dispatch_worker = MagicMock()

        self.sup = sup
        self.conn = conn
        self.db_path = db_path
        self.tmp_path = tmp_path

    def _execute(self, project, ticket_id, pipeline_id=None):
        """Call _execute_dispatch_ticket on the mock supervisor."""
        from botfarm.supervisor_ops import OperationsMixin
        return OperationsMixin._execute_dispatch_ticket(
            self.sup, project, ticket_id, pipeline_id=pipeline_id,
        )

    def test_project_not_found(self, _mock_supervisor):
        result = self._execute("nonexistent", "TST-1")
        assert "error" in result
        assert "not found" in result["error"]

    def test_project_not_semi_auto(self, _mock_supervisor):
        self.sup._projects["my-project"].dispatch_mode = "auto"
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "semi-auto" in result["error"]

    def test_ticket_not_in_queue(self, _mock_supervisor):
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "not found in queue" in result["error"]

    def test_ticket_blocked(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1", blocked_by=["TST-2"])
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "blocked" in result["error"]
        assert "TST-2" in result["error"]

    def test_no_free_slot(self, _mock_supervisor):
        from botfarm.slots import SlotManager
        _seed_queue(self.db_path, "my-project", "TST-1")
        # Mark the only slot as busy
        _seed_slot(self.conn, "my-project", 1, status="busy", ticket_id="TST-99")
        self.conn.commit()
        slot_mgr = SlotManager(self.db_path, conn=self.conn)
        slot_mgr.register_slot("my-project", 1)
        slot_mgr.load()
        self.sup._slot_manager = slot_mgr

        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "No free slot" in result["error"]

    def test_global_dispatch_paused(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        self.sup._slot_manager.set_dispatch_paused(True, "manual_pause")
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "Global dispatch is paused" in result["error"]

    def test_project_paused(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        self.sup._slot_manager.set_project_paused("my-project", True, "testing")
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "paused" in result["error"]

    def test_usage_limits_exceeded(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        self.sup._usage_poller.state.should_pause_with_thresholds.return_value = (
            True, "5-hour utilization 90% >= 85% threshold",
        )
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "Usage limits exceeded" in result["error"]

    def test_codex_usage_limits_exceeded(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        self.sup._codex_usage_poller.state.should_pause.return_value = (
            True, "primary budget 90% >= 85% threshold",
        )
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "Usage limits exceeded" in result["error"]

    def test_ticket_not_in_poll_results(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        # Make poller return empty results
        self.sup._pollers["my-project"].poll.return_value.candidates = []
        self.sup._pollers["my-project"].poll.return_value.blocked = []
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "not found in current poll results" in result["error"]

    def test_successful_dispatch(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        result = self._execute("my-project", "TST-1")
        assert result["success"] is True
        assert result["slot_id"] == 1
        self.sup._dispatch_worker.assert_called_once()
        # Default: no pipeline_id override
        call_kwargs = self.sup._dispatch_worker.call_args
        assert call_kwargs.kwargs.get("pipeline_id") is None

    def test_successful_dispatch_with_pipeline_id(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        result = self._execute("my-project", "TST-1", pipeline_id=42)
        assert result["success"] is True
        assert result["slot_id"] == 1
        call_kwargs = self.sup._dispatch_worker.call_args
        assert call_kwargs.kwargs.get("pipeline_id") == 42

    def test_no_poller_for_project(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        self.sup._pollers = {}
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "No poller" in result["error"]

    def test_poller_failure(self, _mock_supervisor):
        _seed_queue(self.db_path, "my-project", "TST-1")
        self.sup._pollers["my-project"].poll.side_effect = RuntimeError("API down")
        result = self._execute("my-project", "TST-1")
        assert "error" in result
        assert "Failed to poll" in result["error"]
