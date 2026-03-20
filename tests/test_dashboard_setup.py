"""Tests for setup status API, partial endpoints, and setup form endpoints."""

import os

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import BotfarmConfig, LinearBugtrackerConfig, ProjectConfig
from botfarm.dashboard import create_app
from botfarm.dashboard.routes_setup import (
    SetupStep,
    _build_credentials_context,
    _section_done_map,
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


# ---------------------------------------------------------------------------
# Section done map unit tests
# ---------------------------------------------------------------------------

class TestSectionDoneMap:
    def test_all_done(self):
        steps = [
            SetupStep(id="bugtracker_type", label="", done=True),
            SetupStep(id="bugtracker_api_key", label="", done=True),
            SetupStep(id="github_auth", label="", done=True),
            SetupStep(id="claude_auth", label="", done=True),
            SetupStep(id="project_configured", label="", done=True),
            SetupStep(id="repos_cloned", label="", done=True),
        ]
        result = _section_done_map(steps)
        assert result["bugtracker"] is True
        assert result["credentials"] is True
        assert result["project"] is True
        assert result["verification"] is True

    def test_none_done(self):
        steps = [
            SetupStep(id="bugtracker_type", label="", done=False),
            SetupStep(id="bugtracker_api_key", label="", done=False),
            SetupStep(id="github_auth", label="", done=False),
            SetupStep(id="claude_auth", label="", done=False),
            SetupStep(id="project_configured", label="", done=False),
            SetupStep(id="repos_cloned", label="", done=False),
        ]
        result = _section_done_map(steps)
        assert result["bugtracker"] is False
        assert result["credentials"] is False
        assert result["project"] is False
        assert result["verification"] is False

    def test_partial_bugtracker(self):
        steps = [
            SetupStep(id="bugtracker_type", label="", done=True),
            SetupStep(id="bugtracker_api_key", label="", done=False),
            SetupStep(id="github_auth", label="", done=True),
            SetupStep(id="claude_auth", label="", done=True),
            SetupStep(id="project_configured", label="", done=True),
            SetupStep(id="repos_cloned", label="", done=True),
        ]
        result = _section_done_map(steps)
        assert result["bugtracker"] is False
        assert result["credentials"] is True
        assert result["project"] is True
        # Verification requires ALL steps done
        assert result["verification"] is False

    def test_partial_credentials(self):
        steps = [
            SetupStep(id="bugtracker_type", label="", done=True),
            SetupStep(id="bugtracker_api_key", label="", done=True),
            SetupStep(id="github_auth", label="", done=True),
            SetupStep(id="claude_auth", label="", done=False),
            SetupStep(id="project_configured", label="", done=True),
            SetupStep(id="repos_cloned", label="", done=True),
        ]
        result = _section_done_map(steps)
        assert result["bugtracker"] is True
        assert result["credentials"] is False
        assert result["project"] is True
        assert result["verification"] is False


# ---------------------------------------------------------------------------
# Setup wizard page tests
# ---------------------------------------------------------------------------

def _mock_auth_unavailable(monkeypatch):
    """Helper to mock both GitHub and Claude auth as unavailable."""
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


class TestBuildCredentialsContext:
    def test_returns_expected_keys(self, monkeypatch):
        _mock_auth_unavailable(monkeypatch)
        config = _make_config(api_key="key123", bt_type="linear")
        ctx = _build_credentials_context(config)
        expected_keys = {
            "github_done", "claude_done", "ssh_key_path", "ssh_key_exists",
            "bt_type", "bt_workspace", "bt_api_key_set", "jira_url", "jira_email",
        }
        assert set(ctx.keys()) == expected_keys

    def test_none_config(self, monkeypatch):
        _mock_auth_unavailable(monkeypatch)
        ctx = _build_credentials_context(None)
        assert ctx["bt_type"] == ""
        assert ctx["bt_api_key_set"] is False

    def test_bugtracker_values(self, monkeypatch):
        _mock_auth_unavailable(monkeypatch)
        config = _make_config(api_key="key123", bt_type="linear")
        ctx = _build_credentials_context(config)
        assert ctx["bt_type"] == "linear"
        assert ctx["bt_api_key_set"] is True


class TestSetupWizardPage:
    def test_renders_setup_page(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]
        assert "Setup Wizard" in resp.text

    def test_shows_all_sections(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "Bugtracker Configuration" in resp.text
        assert "Credentials" in resp.text
        assert "Project Setup" in resp.text
        assert "Verification" in resp.text

    def test_shows_sidebar_checklist(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "Setup Progress" in resp.text
        assert "setup-steps-nav" in resp.text

    def test_shows_step_navigation(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert 'data-section="bugtracker"' in resp.text
        assert 'data-section="credentials"' in resp.text
        assert 'data-section="project"' in resp.text
        assert 'data-section="verification"' in resp.text

    def test_no_config_renders_empty(self, db_file, monkeypatch):
        app = create_app(db_path=db_file, botfarm_config=None)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert resp.status_code == 200
        assert "Setup Wizard" in resp.text

    def test_shows_complete_banner_when_all_done(
        self, db_file, tmp_path, monkeypatch,
    ):
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

        resp = client.get("/setup")
        assert "Setup Complete" in resp.text
        assert "Go to Dashboard" in resp.text

    def test_no_complete_banner_when_degraded(
        self, db_file, tmp_path, monkeypatch,
    ):
        """All steps done but supervisor still degraded → no banner."""
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

        app = create_app(
            db_path=db_file, botfarm_config=config, get_degraded=lambda: True,
        )
        client = TestClient(app)

        resp = client.get("/setup")
        assert "Setup Complete" not in resp.text
        assert "Go to Dashboard" not in resp.text

    def test_shows_project_count(self, db_file, tmp_path, monkeypatch):
        repo = tmp_path / "myrepo"
        repo.mkdir()
        (repo / ".git").mkdir()
        proj = ProjectConfig(name="myproj", base_dir=str(repo))
        config = _make_config(api_key="key123", bt_type="linear", projects=[proj])
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "1 project configured" in resp.text

    def test_shows_add_project_link(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "/projects/add" in resp.text
        assert "Add Project" in resp.text

    def test_includes_credentials_forms(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "saveBugtracker" in resp.text
        assert "saveGithub" in resp.text

    def test_has_preflight_button(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "Run Preflight Checks" in resp.text

    def test_mobile_responsive(self, db_file, monkeypatch):
        """Verify the template contains mobile responsive styles."""
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "@media" in resp.text
        assert "768px" in resp.text


# ---------------------------------------------------------------------------
# Index redirect tests
# ---------------------------------------------------------------------------

class TestIndexSetupRedirect:
    def test_redirects_to_setup_when_degraded(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
            get_degraded=lambda: True,
        )
        client = TestClient(app, follow_redirects=False)

        resp = client.get("/")
        assert resp.status_code == 302
        assert resp.headers["location"] == "/setup"

    def test_no_redirect_when_not_degraded(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
            get_degraded=lambda: False,
        )
        client = TestClient(app, follow_redirects=False)

        resp = client.get("/")
        assert resp.status_code == 200

    def test_no_redirect_when_no_degraded_getter(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app, follow_redirects=False)

        resp = client.get("/")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Setup preflight partial tests
# ---------------------------------------------------------------------------

class TestSetupPreflightPartial:
    def test_renders_no_results(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/partials/setup-preflight")
        assert resp.status_code == 200
        assert "No preflight results yet" in resp.text

    def test_renders_with_results(self, db_file, monkeypatch):
        from dataclasses import dataclass

        @dataclass
        class FakeCheck:
            name: str
            passed: bool
            message: str
            critical: bool

        checks = [
            FakeCheck("git_repo", True, "OK", True),
            FakeCheck("database", False, "Schema mismatch", True),
        ]

        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
            get_preflight_results=lambda: checks,
            get_degraded=lambda: True,
        )
        client = TestClient(app)

        resp = client.get("/partials/setup-preflight")
        assert resp.status_code == 200
        assert "git_repo" in resp.text
        assert "database" in resp.text
        assert "Schema mismatch" in resp.text
        assert "critical" in resp.text

    def test_shows_all_passed(self, db_file, monkeypatch):
        from dataclasses import dataclass

        @dataclass
        class FakeCheck:
            name: str
            passed: bool
            message: str
            critical: bool

        checks = [FakeCheck("git_repo", True, "OK", True)]

        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
            get_preflight_results=lambda: checks,
        )
        client = TestClient(app)

        resp = client.get("/partials/setup-preflight")
        assert "All preflight checks passed" in resp.text
