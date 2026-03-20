"""Tests for setup status API and partial endpoints."""

import json
import platform

import pytest
from fastapi.testclient import TestClient

from botfarm.config import BotfarmConfig, LinearBugtrackerConfig, ProjectConfig
from botfarm.dashboard import create_app
from botfarm.dashboard.routes_setup import SetupStep, get_setup_steps
from botfarm.db import init_db


@pytest.fixture()
def db_file(tmp_path):
    path = tmp_path / "botfarm.db"
    conn = init_db(path)
    conn.close()
    return path


def _make_config(*, api_key="", bt_type="linear", projects=None):
    bt = LinearBugtrackerConfig(type=bt_type, api_key=api_key)
    return BotfarmConfig(projects=projects or [], bugtracker=bt)


# ---------------------------------------------------------------------------
# Unit tests for individual check functions
# ---------------------------------------------------------------------------

class TestGetSetupSteps:
    def test_empty_config_all_incomplete(self):
        config = _make_config()
        steps = get_setup_steps(config)
        ids = {s.id for s in steps}
        assert "bugtracker_type" in ids
        assert "bugtracker_api_key" in ids
        assert "github_auth" in ids
        assert "claude_auth" in ids
        assert "project_configured" in ids
        assert "repos_cloned" in ids

    def test_bugtracker_type_detected(self):
        config = _make_config(bt_type="linear")
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["bugtracker_type"].done is True

    def test_bugtracker_type_empty(self):
        config = _make_config(bt_type="")
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["bugtracker_type"].done is False

    def test_bugtracker_api_key_set(self):
        config = _make_config(api_key="lin_api_abc123")
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["bugtracker_api_key"].done is True

    def test_bugtracker_api_key_missing(self):
        config = _make_config(api_key="")
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["bugtracker_api_key"].done is False

    def test_project_configured(self):
        proj = ProjectConfig(name="myproj", base_dir="/tmp/myproj")
        config = _make_config(projects=[proj])
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["project_configured"].done is True

    def test_no_project_configured(self):
        config = _make_config(projects=[])
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["project_configured"].done is False

    def test_repos_cloned_when_git_dir_exists(self, tmp_path):
        repo = tmp_path / "myrepo"
        repo.mkdir()
        (repo / ".git").mkdir()
        proj = ProjectConfig(name="myproj", base_dir=str(repo))
        config = _make_config(projects=[proj])
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["repos_cloned"].done is True

    def test_repos_not_cloned_when_git_dir_missing(self, tmp_path):
        repo = tmp_path / "myrepo"
        repo.mkdir()
        # No .git directory
        proj = ProjectConfig(name="myproj", base_dir=str(repo))
        config = _make_config(projects=[proj])
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["repos_cloned"].done is False

    def test_repos_not_cloned_when_no_projects(self):
        config = _make_config(projects=[])
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["repos_cloned"].done is False

    def test_github_auth_detected(self, tmp_path, monkeypatch):
        # Create fake gh hosts.yml
        gh_dir = tmp_path / ".config" / "gh"
        gh_dir.mkdir(parents=True)
        hosts = gh_dir / "hosts.yml"
        hosts.write_text("github.com:\n  oauth_token: abc\n")
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        monkeypatch.setattr("shutil.which", lambda cmd: "/usr/bin/gh" if cmd == "gh" else None)

        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["github_auth"].done is True

    def test_github_auth_missing_no_gh(self, monkeypatch):
        monkeypatch.setattr("shutil.which", lambda cmd: None)
        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["github_auth"].done is False

    def test_claude_auth_linux(self, tmp_path, monkeypatch):
        monkeypatch.setattr("botfarm.dashboard.routes_setup.platform.system", lambda: "Linux")
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        claude_dir = tmp_path / ".claude"
        claude_dir.mkdir()
        creds = claude_dir / ".credentials.json"
        creds.write_text('{"access_token": "abc"}')

        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["claude_auth"].done is True

    def test_claude_auth_linux_missing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("botfarm.dashboard.routes_setup.platform.system", lambda: "Linux")
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        # No .credentials.json

        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["claude_auth"].done is False

    def test_claude_auth_macos_with_binary(self, monkeypatch):
        monkeypatch.setattr("botfarm.dashboard.routes_setup.platform.system", lambda: "Darwin")
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.shutil.which",
            lambda cmd: "/usr/local/bin/claude" if cmd == "claude" else None,
        )

        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["claude_auth"].done is True


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------

class TestSetupStatusAPI:
    def test_returns_json_checklist(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/api/setup/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "setup_complete" in data
        assert "steps" in data
        assert isinstance(data["steps"], list)
        assert len(data["steps"]) == 6

        step_ids = [s["id"] for s in data["steps"]]
        assert "bugtracker_type" in step_ids
        assert "bugtracker_api_key" in step_ids

    def test_setup_complete_when_all_done(self, db_file, tmp_path, monkeypatch):
        # Set up a fully configured environment
        repo = tmp_path / "myrepo"
        repo.mkdir()
        (repo / ".git").mkdir()
        proj = ProjectConfig(name="myproj", base_dir=str(repo))
        config = _make_config(api_key="key123", bt_type="linear", projects=[proj])

        # Mock GitHub and Claude auth
        gh_dir = tmp_path / ".config" / "gh"
        gh_dir.mkdir(parents=True)
        (gh_dir / "hosts.yml").write_text("github.com:\n  oauth_token: abc\n")
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.shutil.which",
            lambda cmd: "/usr/bin/" + cmd,
        )
        monkeypatch.setattr("botfarm.dashboard.routes_setup.platform.system", lambda: "Linux")
        claude_dir = tmp_path / ".claude"
        claude_dir.mkdir()
        (claude_dir / ".credentials.json").write_text('{"access_token": "x"}')

        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/api/setup/status")
        data = resp.json()
        assert data["setup_complete"] is True
        assert all(s["done"] for s in data["steps"])

    def test_no_config_returns_503(self, db_file):
        app = create_app(db_path=db_file, botfarm_config=None)
        client = TestClient(app)

        resp = client.get("/api/setup/status")
        assert resp.status_code == 503
        assert "error" in resp.json()


# ---------------------------------------------------------------------------
# Partial endpoint tests
# ---------------------------------------------------------------------------

class TestSetupStatusPartial:
    def test_partial_renders_html(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/partials/setup-status")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Bugtracker type selected" in resp.text
        assert "Bugtracker API key configured" in resp.text

    def test_partial_shows_checkmarks_for_done_steps(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/partials/setup-status")
        # bugtracker_type=linear → done, so checkmark should be present
        assert "&#10003;" in resp.text
        # No projects → repos_cloned is not done, cross should appear
        assert "&#10007;" in resp.text

    def test_partial_without_config(self, db_file):
        app = create_app(db_path=db_file, botfarm_config=None)
        client = TestClient(app)

        resp = client.get("/partials/setup-status")
        assert resp.status_code == 200
        assert "No configuration loaded" in resp.text

    def test_partial_shows_complete_message(self, db_file, tmp_path, monkeypatch):
        repo = tmp_path / "myrepo"
        repo.mkdir()
        (repo / ".git").mkdir()
        proj = ProjectConfig(name="myproj", base_dir=str(repo))
        config = _make_config(api_key="key123", bt_type="linear", projects=[proj])

        gh_dir = tmp_path / ".config" / "gh"
        gh_dir.mkdir(parents=True)
        (gh_dir / "hosts.yml").write_text("github.com:\n  oauth_token: abc\n")
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.shutil.which",
            lambda cmd: "/usr/bin/" + cmd,
        )
        monkeypatch.setattr("botfarm.dashboard.routes_setup.platform.system", lambda: "Linux")
        claude_dir = tmp_path / ".claude"
        claude_dir.mkdir()
        (claude_dir / ".credentials.json").write_text('{"access_token": "x"}')

        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/partials/setup-status")
        assert "Setup complete" in resp.text
