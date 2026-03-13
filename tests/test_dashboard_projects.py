"""Tests for dashboard add-project routes: Linear lookups, project creation, SSE progress."""

import json
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from botfarm.config import BotfarmConfig, LinearConfig, ProjectConfig
from botfarm.dashboard import create_app
from botfarm.db import init_db


def _make_config(*, projects=None, api_key="test-key"):
    return BotfarmConfig(
        projects=projects or [],
        linear=LinearConfig(api_key=api_key),
    )


def _make_app(tmp_path, *, config=None, on_add_project=None):
    db_path = tmp_path / "test.db"
    conn = init_db(db_path)
    conn.close()
    return create_app(
        db_path=db_path,
        botfarm_config=config or _make_config(),
        on_add_project=on_add_project,
    )


class TestLinearTeamsEndpoint:
    def test_returns_teams(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        mock_teams = [{"id": "1", "name": "Engineering", "key": "ENG"}]
        with patch("botfarm.dashboard.routes_projects.LinearClient") as MockClient:
            MockClient.return_value.list_teams.return_value = mock_teams
            resp = client.get("/api/linear/teams")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0] == {"key": "ENG", "name": "Engineering"}

    def test_no_api_key(self, tmp_path):
        app = _make_app(tmp_path, config=_make_config(api_key=""))
        client = TestClient(app)
        resp = client.get("/api/linear/teams")
        assert resp.status_code == 503


class TestLinearProjectsEndpoint:
    def test_returns_projects(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        mock_projects = [{"id": "p1", "name": "Bot farm"}]
        with patch("botfarm.dashboard.routes_projects.LinearClient") as MockClient:
            MockClient.return_value.get_team_id.return_value = "team-uuid"
            MockClient.return_value.list_team_projects.return_value = mock_projects
            resp = client.get("/api/linear/projects?team=ENG")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0] == {"id": "p1", "name": "Bot farm"}

    def test_missing_team_param(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/linear/projects")
        assert resp.status_code == 400


class TestProjectCreateEndpoint:
    def test_validation_errors(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/create",
            json={"repo_url": "", "name": "", "linear_team": "", "slots": 0},
        )
        assert resp.status_code == 400
        data = resp.json()
        assert len(data["errors"]) >= 3

    def test_duplicate_project_name(self, tmp_path):
        existing = ProjectConfig(
            name="my-proj", linear_team="ENG",
            base_dir="/tmp/x", worktree_prefix="/tmp/x-slot-", slots=[1],
        )
        cfg = _make_config(projects=[existing])
        app = _make_app(tmp_path, config=cfg)
        client = TestClient(app)
        resp = client.post(
            "/api/project/create",
            json={
                "repo_url": "git@github.com:user/repo.git",
                "name": "my-proj",
                "linear_team": "ENG",
                "slots": 1,
            },
        )
        assert resp.status_code == 400
        assert "already exists" in resp.json()["errors"][0]

    def test_duplicate_linear_project(self, tmp_path):
        existing = ProjectConfig(
            name="other-proj", linear_team="ENG", linear_project="Bot farm",
            base_dir="/tmp/x", worktree_prefix="/tmp/x-slot-", slots=[1],
        )
        cfg = _make_config(projects=[existing])
        app = _make_app(tmp_path, config=cfg)
        client = TestClient(app)
        resp = client.post(
            "/api/project/create",
            json={
                "repo_url": "git@github.com:user/repo.git",
                "name": "new-proj",
                "linear_team": "ENG",
                "linear_project": "Bot farm",
                "slots": 1,
            },
        )
        assert resp.status_code == 400
        assert any("already used" in e for e in resp.json()["errors"])

    def test_invalid_repo_url(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/create",
            json={
                "repo_url": "not-a-url",
                "name": "test",
                "linear_team": "ENG",
                "slots": 1,
            },
        )
        assert resp.status_code == 400
        assert any("URL" in e for e in resp.json()["errors"])

    def test_slots_out_of_range(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/create",
            json={
                "repo_url": "git@github.com:user/repo.git",
                "name": "test",
                "linear_team": "ENG",
                "slots": 25,
            },
        )
        assert resp.status_code == 400
        assert any("between 1 and 20" in e for e in resp.json()["errors"])

    def test_successful_start(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup:
            mock_setup.return_value = {"name": "repo"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "repo",
                    "linear_team": "ENG",
                    "slots": 2,
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "started"
            assert "task_id" in data
            # Wait for the background thread to call the mocked function
            time.sleep(0.1)

    def test_no_config(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/create",
            json={
                "repo_url": "git@github.com:user/repo.git",
                "name": "test",
                "linear_team": "ENG",
                "slots": 1,
            },
        )
        assert resp.status_code == 503


class TestProgressEndpoint:
    def test_unknown_task_id(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/project/create/progress?task_id=nonexistent")
        assert resp.status_code == 404

    def test_missing_task_id(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/project/create/progress")
        assert resp.status_code == 400


class TestSuggestNameEndpoint:
    def test_ssh_url(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/project/suggest-name?repo_url=git@github.com:user/my-repo.git")
        assert resp.status_code == 200
        assert resp.json()["name"] == "my-repo"

    def test_https_url(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/project/suggest-name?repo_url=https://github.com/user/cool-project.git")
        assert resp.status_code == 200
        assert resp.json()["name"] == "cool-project"

    def test_empty_url(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/project/suggest-name")
        assert resp.status_code == 200
        assert resp.json()["name"] == ""


class TestAddProjectPage:
    def test_renders(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/projects/add")
        assert resp.status_code == 200
        assert "Add Project" in resp.text
        assert "repo_url" in resp.text

    def test_add_project_button_on_status_page(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Add Project" in resp.text
        assert "/projects/add" in resp.text
