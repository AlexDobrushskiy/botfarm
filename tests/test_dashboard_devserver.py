"""Tests for dashboard dev server API endpoints and SSE streaming."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from botfarm.config import ProjectConfig
from botfarm.dashboard import create_app
from botfarm.db import init_db
from botfarm.devserver import DevServerManager


@pytest.fixture()
def db_file(tmp_path):
    path = tmp_path / "botfarm.db"
    conn = init_db(path)
    conn.close()
    return path


@pytest.fixture()
def devserver_mgr(tmp_path):
    """Create a DevServerManager with a registered test project."""
    log_dir = tmp_path / "logs" / "devserver"
    mgr = DevServerManager(log_dir=log_dir)
    project = ProjectConfig(
        name="myapp",
        base_dir=str(tmp_path),
        run_command="echo hello",
    )
    mgr.register_project(project)
    return mgr


@pytest.fixture()
def client(db_file, devserver_mgr):
    app = create_app(db_path=db_file, devserver_manager=devserver_mgr)
    return TestClient(app)


@pytest.fixture()
def client_no_mgr(db_file):
    """Client without dev server manager."""
    app = create_app(db_path=db_file)
    return TestClient(app)


class TestDevserverStart:
    def test_start_success(self, client, devserver_mgr):
        with patch.object(devserver_mgr, "start") as mock_start:
            with patch.object(devserver_mgr, "status", return_value={"status": "stopped"}):
                resp = client.post("/api/devserver/myapp/start")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        mock_start.assert_called_once_with("myapp")

    def test_start_no_run_command(self, client):
        resp = client.post("/api/devserver/unknown_project/start")
        assert resp.status_code == 400
        assert "run_command" in resp.json()["error"]

    def test_start_already_running(self, client, devserver_mgr):
        with patch.object(devserver_mgr, "status", return_value={"status": "running"}):
            resp = client.post("/api/devserver/myapp/start")
        assert resp.status_code == 409
        assert "already running" in resp.json()["error"]

    def test_start_no_manager(self, client_no_mgr):
        resp = client_no_mgr.post("/api/devserver/myapp/start")
        assert resp.status_code == 503


class TestDevserverStop:
    def test_stop_success(self, client, devserver_mgr):
        with patch.object(devserver_mgr, "status", return_value={"status": "running"}):
            with patch.object(devserver_mgr, "stop") as mock_stop:
                resp = client.post("/api/devserver/myapp/stop")
        assert resp.status_code == 200
        mock_stop.assert_called_once_with("myapp")

    def test_stop_not_running(self, client, devserver_mgr):
        with patch.object(devserver_mgr, "status", return_value={"status": "stopped"}):
            resp = client.post("/api/devserver/myapp/stop")
        assert resp.status_code == 409
        assert "not running" in resp.json()["error"]

    def test_stop_no_manager(self, client_no_mgr):
        resp = client_no_mgr.post("/api/devserver/myapp/stop")
        assert resp.status_code == 503


class TestDevserverRestart:
    def test_restart_success(self, client, devserver_mgr):
        with patch.object(devserver_mgr, "restart") as mock_restart:
            resp = client.post("/api/devserver/myapp/restart")
        assert resp.status_code == 200
        mock_restart.assert_called_once_with("myapp")

    def test_restart_no_run_command(self, client):
        resp = client.post("/api/devserver/unknown_project/restart")
        assert resp.status_code == 400

    def test_restart_no_manager(self, client_no_mgr):
        resp = client_no_mgr.post("/api/devserver/myapp/restart")
        assert resp.status_code == 503


class TestDevserverStatus:
    def test_status_with_servers(self, client, devserver_mgr):
        with patch.object(
            devserver_mgr,
            "status",
            return_value={
                "project": "myapp",
                "status": "running",
                "pid": 1234,
                "uptime": 3665.0,
                "restart_count": 0,
            },
        ):
            resp = client.get("/api/devserver/status")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["servers"]) == 1
        srv = data["servers"][0]
        assert srv["project"] == "myapp"
        assert srv["status"] == "running"
        assert srv["uptime_display"] == "1h01m"

    def test_status_stopped(self, client, devserver_mgr):
        with patch.object(
            devserver_mgr,
            "status",
            return_value={
                "project": "myapp",
                "status": "stopped",
                "pid": None,
                "uptime": None,
                "restart_count": 0,
            },
        ):
            resp = client.get("/api/devserver/status")
        data = resp.json()
        assert data["servers"][0]["uptime_display"] is None

    def test_status_no_manager(self, client_no_mgr):
        resp = client_no_mgr.get("/api/devserver/status")
        assert resp.status_code == 200
        assert resp.json()["servers"] == []

    def test_status_uptime_minutes(self, client, devserver_mgr):
        with patch.object(
            devserver_mgr,
            "status",
            return_value={
                "project": "myapp",
                "status": "running",
                "pid": 1234,
                "uptime": 125.0,
                "restart_count": 0,
            },
        ):
            resp = client.get("/api/devserver/status")
        assert resp.json()["servers"][0]["uptime_display"] == "2m05s"

    def test_status_uptime_seconds(self, client, devserver_mgr):
        with patch.object(
            devserver_mgr,
            "status",
            return_value={
                "project": "myapp",
                "status": "running",
                "pid": 1234,
                "uptime": 42.0,
                "restart_count": 0,
            },
        ):
            resp = client.get("/api/devserver/status")
        assert resp.json()["servers"][0]["uptime_display"] == "42s"


class TestDevserverStream:
    def test_stream_no_log_file(self, client):
        resp = client.get("/api/devserver/myapp/stream")
        assert resp.status_code == 404

    def test_stream_reads_log(self, client, devserver_mgr, tmp_path):
        """SSE stream reads log file and emits stdout events."""
        log_dir = devserver_mgr._log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "myapp.log"
        log_file.write_text("line one\nline two\n")

        with patch.object(
            devserver_mgr, "status", return_value={"status": "stopped"}
        ):
            with client.stream("GET", "/api/devserver/myapp/stream") as resp:
                assert resp.status_code == 200
                events = []
                for line in resp.iter_lines():
                    if line.startswith("event:"):
                        events.append(line.split(":", 1)[1].strip())
                    if len(events) >= 3:  # stdout, stdout, done
                        break
        # Should have stdout events for each line, then system+done
        assert "stdout" in events

    def test_stream_strips_ansi(self, client, devserver_mgr, tmp_path):
        """ANSI escape codes should be stripped from log output."""
        log_dir = devserver_mgr._log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file = log_dir / "myapp.log"
        log_file.write_text("\x1b[32mgreen text\x1b[0m\n")

        with patch.object(
            devserver_mgr, "status", return_value={"status": "stopped"}
        ):
            with client.stream("GET", "/api/devserver/myapp/stream") as resp:
                data_lines = []
                for line in resp.iter_lines():
                    if line.startswith("data:"):
                        data_lines.append(line.split(":", 1)[1].strip())
                    if len(data_lines) >= 1:
                        break
        assert data_lines
        assert "\x1b" not in data_lines[0]
        assert "green text" in data_lines[0]


class TestDevserverPartial:
    def test_partial_renders(self, client, devserver_mgr):
        with patch.object(
            devserver_mgr,
            "status",
            return_value={
                "project": "myapp",
                "status": "running",
                "pid": 1234,
                "uptime": 60.0,
                "restart_count": 0,
            },
        ):
            resp = client.get("/partials/devserver-status")
        assert resp.status_code == 200
        assert "myapp" in resp.text
        assert "Stop" in resp.text

    def test_partial_stopped_shows_run(self, client, devserver_mgr):
        with patch.object(
            devserver_mgr,
            "status",
            return_value={
                "project": "myapp",
                "status": "stopped",
                "pid": None,
                "uptime": None,
                "restart_count": 0,
            },
        ):
            resp = client.get("/partials/devserver-status")
        assert resp.status_code == 200
        assert "Run" in resp.text

    def test_partial_no_manager(self, client_no_mgr):
        resp = client_no_mgr.get("/partials/devserver-status")
        assert resp.status_code == 200
        assert "run_command" in resp.text  # shows the "no projects" message


class TestDevserverIndexSection:
    def test_index_has_devserver_section(self, client, devserver_mgr):
        with patch.object(
            devserver_mgr,
            "status",
            return_value={
                "project": "myapp",
                "status": "stopped",
                "pid": None,
                "uptime": None,
                "restart_count": 0,
            },
        ):
            resp = client.get("/")
        assert resp.status_code == 200
        assert "Dev Servers" in resp.text
        assert "devserver-panel" in resp.text
