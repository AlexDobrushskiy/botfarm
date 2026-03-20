"""Tests for setup status API, partial endpoints, and setup form endpoints."""

import os

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import BotfarmConfig, LinearBugtrackerConfig, ProjectConfig
from botfarm.dashboard import create_app
from botfarm.dashboard.routes_setup import (
    SetupStep,
    _validate_bugtracker_api_key,
    _validate_github_token,
    get_setup_steps,
)
from botfarm.db import init_db


@pytest.fixture()
def db_file(tmp_path):
    path = tmp_path / "botfarm.db"
    conn = init_db(path)
    conn.close()
    return path


def _make_config(*, api_key="", bt_type="linear", projects=None, source_path=""):
    bt = LinearBugtrackerConfig(type=bt_type, api_key=api_key)
    return BotfarmConfig(
        projects=projects or [], bugtracker=bt, source_path=source_path,
    )


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
        # Ensure env vars don't short-circuit the hosts.yml detection path
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
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
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.setattr("shutil.which", lambda cmd: None)
        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["github_auth"].done is False

    def test_claude_auth_available(self, monkeypatch):
        from botfarm.credentials import OAuthToken

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: OAuthToken(access_token="abc"),
        )
        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["claude_auth"].done is True

    def test_claude_auth_unavailable(self, monkeypatch):
        from botfarm.credentials import CredentialError

        def _raise():
            raise CredentialError("no credentials")

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            _raise,
        )
        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["claude_auth"].done is False

    def test_github_auth_via_env_var(self, monkeypatch):
        monkeypatch.setenv("GH_TOKEN", "ghp_abc123")
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.setattr("shutil.which", lambda cmd: None)

        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["github_auth"].done is True

    def test_github_auth_via_github_token_env(self, monkeypatch):
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_abc123")
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setattr("shutil.which", lambda cmd: None)

        config = _make_config()
        steps = {s.id: s for s in get_setup_steps(config)}
        assert steps["github_auth"].done is True


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
        from botfarm.credentials import OAuthToken

        # Set up a fully configured environment
        repo = tmp_path / "myrepo"
        repo.mkdir()
        (repo / ".git").mkdir()
        proj = ProjectConfig(name="myproj", base_dir=str(repo))
        config = _make_config(api_key="key123", bt_type="linear", projects=[proj])

        # Mock GitHub auth via hosts.yml
        gh_dir = tmp_path / ".config" / "gh"
        gh_dir.mkdir(parents=True)
        (gh_dir / "hosts.yml").write_text("github.com:\n  oauth_token: abc\n")
        monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.shutil.which",
            lambda cmd: "/usr/bin/" + cmd,
        )
        # Mock Claude auth via _load_token
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: OAuthToken(access_token="x"),
        )

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
        from botfarm.credentials import OAuthToken

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
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: OAuthToken(access_token="x"),
        )

        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/partials/setup-status")
        assert "Setup complete" in resp.text


# ---------------------------------------------------------------------------
# Bugtracker API key validation unit tests
# ---------------------------------------------------------------------------

class TestValidateBugtrackerApiKey:
    def test_valid_linear_key(self, monkeypatch):
        class FakeClient:
            def get_viewer_id(self):
                return "user-123"

        monkeypatch.setattr(
            "botfarm.bugtracker.create_client",
            lambda **kwargs: FakeClient(),
        )
        result = _validate_bugtracker_api_key("linear", "lin_api_test")
        assert result is None

    def test_invalid_linear_key(self, monkeypatch):
        from botfarm.bugtracker import BugtrackerError

        class FailClient:
            def get_viewer_id(self):
                raise BugtrackerError("Invalid API key")

        monkeypatch.setattr(
            "botfarm.bugtracker.create_client",
            lambda **kwargs: FailClient(),
        )
        result = _validate_bugtracker_api_key("linear", "bad_key")
        assert result is not None
        assert "Invalid API key" in result

    def test_unknown_type(self):
        result = _validate_bugtracker_api_key("unknown", "key")
        assert result is not None
        assert "Unknown bugtracker type" in result


# ---------------------------------------------------------------------------
# GitHub token validation unit tests
# ---------------------------------------------------------------------------

class TestValidateGithubToken:
    def test_valid_token(self, monkeypatch):
        import subprocess

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.subprocess.run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout='{"login":"user"}', stderr="",
            ),
        )
        result = _validate_github_token("ghp_valid")
        assert result is None

    def test_invalid_token(self, monkeypatch):
        import subprocess

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.subprocess.run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=1, stdout="", stderr="Bad credentials",
            ),
        )
        result = _validate_github_token("ghp_bad")
        assert result is not None
        assert "Bad credentials" in result

    def test_gh_not_installed(self, monkeypatch):
        def _raise(*a, **kw):
            raise FileNotFoundError("gh not found")

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.subprocess.run", _raise,
        )
        result = _validate_github_token("ghp_test")
        assert result is not None
        assert "not installed" in result

    def test_timeout(self, monkeypatch):
        import subprocess

        def _timeout(*a, **kw):
            raise subprocess.TimeoutExpired(cmd="gh", timeout=15)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.subprocess.run", _timeout,
        )
        result = _validate_github_token("ghp_test")
        assert result is not None
        assert "timed out" in result


# ---------------------------------------------------------------------------
# Bugtracker setup endpoint tests
# ---------------------------------------------------------------------------

class TestSetupBugtrackerEndpoint:
    def test_no_config_returns_400(self, db_file):
        app = create_app(db_path=db_file, botfarm_config=None)
        client = TestClient(app)
        resp = client.post("/api/setup/bugtracker", json={"type": "linear"})
        assert resp.status_code == 400

    def test_missing_fields_returns_422(self, db_file, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("bugtracker:\n  type: linear\n")
        config = _make_config(source_path=str(config_file))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/bugtracker", json={
            "type": "linear",
            "workspace": "",
            "api_key": "",
        })
        assert resp.status_code == 422
        assert "Workspace name is required" in resp.text
        assert "API key is required" in resp.text

    def test_invalid_type_returns_422(self, db_file, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("bugtracker:\n  type: linear\n")
        config = _make_config(source_path=str(config_file))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/bugtracker", json={
            "type": "github",
            "workspace": "ws",
            "api_key": "key",
        })
        assert resp.status_code == 422
        assert "linear" in resp.text or "jira" in resp.text

    def test_jira_missing_fields_returns_422(self, db_file, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("bugtracker:\n  type: jira\n")
        config = _make_config(source_path=str(config_file))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/bugtracker", json={
            "type": "jira",
            "workspace": "ws",
            "api_key": "key",
        })
        assert resp.status_code == 422
        assert "Jira URL" in resp.text
        assert "Jira email" in resp.text

    def test_api_key_validation_failure_returns_422(self, db_file, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("bugtracker:\n  type: linear\n")
        config = _make_config(source_path=str(config_file))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        from botfarm.bugtracker import BugtrackerError

        class FailClient:
            def get_viewer_id(self):
                raise BugtrackerError("Invalid key")

        monkeypatch.setattr(
            "botfarm.bugtracker.create_client",
            lambda **kwargs: FailClient(),
        )

        resp = client.post("/api/setup/bugtracker", json={
            "type": "linear",
            "workspace": "my-ws",
            "api_key": "bad_key",
        })
        assert resp.status_code == 422
        assert "validation failed" in resp.text

    def test_success_writes_env_and_yaml(self, db_file, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("bugtracker:\n  type: linear\n")
        env_file = tmp_path / ".env"
        config = _make_config(source_path=str(config_file))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        class FakeClient:
            def get_viewer_id(self):
                return "user-123"

        monkeypatch.setattr(
            "botfarm.bugtracker.create_client",
            lambda **kwargs: FakeClient(),
        )

        resp = client.post("/api/setup/bugtracker", json={
            "type": "linear",
            "workspace": "test-ws",
            "api_key": "lin_api_test123",
        })
        assert resp.status_code == 200
        assert "success" in resp.text

        # Check .env was written
        assert env_file.exists()
        env_content = env_file.read_text()
        assert "LINEAR_API_KEY" in env_content
        assert "lin_api_test123" in env_content

        # Check config.yaml was updated
        data = yaml.safe_load(config_file.read_text())
        assert data["bugtracker"]["type"] == "linear"
        assert data["bugtracker"]["workspace"] == "test-ws"
        assert data["bugtracker"]["api_key"] == "${LINEAR_API_KEY}"

        # Check in-memory config updated
        assert config.bugtracker.type == "linear"
        assert config.bugtracker.workspace == "test-ws"
        assert config.bugtracker.api_key == "lin_api_test123"

    def test_invalid_json_returns_400(self, db_file, tmp_path):
        config = _make_config(source_path=str(tmp_path / "config.yaml"))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post(
            "/api/setup/bugtracker",
            content="not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400

    def test_non_dict_body_returns_400(self, db_file, tmp_path):
        config = _make_config(source_path=str(tmp_path / "config.yaml"))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/bugtracker", json=["not", "a", "dict"])
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# GitHub setup endpoint tests
# ---------------------------------------------------------------------------

class TestSetupGithubEndpoint:
    def test_no_config_returns_400(self, db_file):
        app = create_app(db_path=db_file, botfarm_config=None)
        client = TestClient(app)
        resp = client.post("/api/setup/github", json={"github_token": "ghp_test"})
        assert resp.status_code == 400

    def test_missing_token_returns_422(self, db_file, tmp_path):
        config = _make_config(source_path=str(tmp_path / "config.yaml"))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/github", json={"github_token": ""})
        assert resp.status_code == 422
        assert "required" in resp.text

    def test_token_validation_failure_returns_422(self, db_file, tmp_path, monkeypatch):
        import subprocess

        config = _make_config(source_path=str(tmp_path / "config.yaml"))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.subprocess.run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=1, stdout="", stderr="Bad credentials",
            ),
        )

        resp = client.post("/api/setup/github", json={"github_token": "ghp_bad"})
        assert resp.status_code == 422
        assert "Bad credentials" in resp.text

    def test_success_writes_env_and_sets_env_var(
        self, db_file, tmp_path, monkeypatch,
    ):
        import subprocess

        config_file = tmp_path / "config.yaml"
        config_file.write_text("bugtracker:\n  type: linear\n")
        env_file = tmp_path / ".env"
        config = _make_config(source_path=str(config_file))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.subprocess.run",
            lambda *a, **kw: subprocess.CompletedProcess(
                args=a[0], returncode=0, stdout='{"login":"bot"}', stderr="",
            ),
        )
        # Ensure GH_TOKEN starts unset
        monkeypatch.delenv("GH_TOKEN", raising=False)

        resp = client.post("/api/setup/github", json={"github_token": "ghp_valid123"})
        assert resp.status_code == 200
        assert "success" in resp.text

        # Check .env was written
        assert env_file.exists()
        env_content = env_file.read_text()
        assert "GH_TOKEN" in env_content
        assert "ghp_valid123" in env_content

        # Check os.environ was updated
        assert os.environ.get("GH_TOKEN") == "ghp_valid123"

    def test_invalid_json_returns_400(self, db_file, tmp_path):
        config = _make_config(source_path=str(tmp_path / "config.yaml"))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post(
            "/api/setup/github",
            content="not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Setup credentials partial tests
# ---------------------------------------------------------------------------

class TestSetupCredentialsPartial:
    def test_renders_html(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        # Mock Claude auth as unavailable
        from botfarm.credentials import CredentialError

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: (_ for _ in ()).throw(CredentialError("no creds")),
        )
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.shutil.which",
            lambda cmd: None,
        )

        resp = client.get("/partials/setup-credentials")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Bugtracker" in resp.text
        assert "GitHub" in resp.text
        assert "Claude Code" in resp.text
        assert "SSH Key" in resp.text

    def test_shows_configured_status(self, db_file, monkeypatch):
        from botfarm.credentials import OAuthToken

        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setenv("GH_TOKEN", "ghp_test")
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: OAuthToken(access_token="x"),
        )

        resp = client.get("/partials/setup-credentials")
        assert resp.status_code == 200
        assert "Configured" in resp.text
        assert "Authenticated" in resp.text

    def test_shows_claude_instructions_when_not_authenticated(
        self, db_file, monkeypatch,
    ):
        from botfarm.credentials import CredentialError

        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: (_ for _ in ()).throw(CredentialError("no creds")),
        )
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.shutil.which",
            lambda cmd: None,
        )

        resp = client.get("/partials/setup-credentials")
        assert "SSH into the server" in resp.text
        assert "claude" in resp.text

    def test_without_config(self, db_file, monkeypatch):
        from botfarm.credentials import CredentialError

        app = create_app(db_path=db_file, botfarm_config=None)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: (_ for _ in ()).throw(CredentialError("no creds")),
        )
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.shutil.which",
            lambda cmd: None,
        )

        resp = client.get("/partials/setup-credentials")
        assert resp.status_code == 200
