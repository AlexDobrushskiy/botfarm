"""Tests for setup status API, partial endpoints, and setup form endpoints."""

import os

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import (
    BotfarmConfig,
    DashboardConfig,
    LinearBugtrackerConfig,
    ProjectConfig,
)
from botfarm.dashboard import create_app
from botfarm.dashboard.routes_setup import (
    SetupStep,
    _build_credentials_context,
    _extract_auth_url,
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
# GitHub device code flow tests
# ---------------------------------------------------------------------------


def _fake_httpx_client(response):
    """Build a mock ``httpx.AsyncClient`` context manager returning *response*."""

    class FakeResponse:
        def __init__(self, status_code, data):
            self.status_code = status_code
            self._data = data
            self.text = str(data)

        def json(self):
            return self._data

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            pass

        async def post(self, *a, **kw):
            return FakeResponse(*response)

    return lambda **kw: FakeClient()


def _fake_httpx_client_error():
    """Build a mock ``httpx.AsyncClient`` that raises on post."""
    import httpx

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            pass

        async def post(self, *a, **kw):
            raise httpx.ConnectError("connection refused")

    return lambda **kw: FakeClient()


class TestDeviceCodeFlowStart:
    def test_returns_user_code_and_url(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((200, {
                "device_code": "dc_abc123",
                "user_code": "ABCD-1234",
                "verification_uri": "https://github.com/login/device",
                "interval": 5,
                "expires_in": 900,
            })),
        )

        resp = client.post("/api/setup/github/device-code", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_code"] == "ABCD-1234"
        assert data["verification_uri"] == "https://github.com/login/device"
        assert data["device_code"] == "dc_abc123"
        assert data["interval"] == 5
        assert data["expires_in"] == 900

    def test_github_error_returns_502(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((403, {"error": "forbidden"})),
        )

        resp = client.post("/api/setup/github/device-code", json={})
        assert resp.status_code == 502
        assert "error" in resp.json()

    def test_network_error_returns_502(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client_error(),
        )

        resp = client.post("/api/setup/github/device-code", json={})
        assert resp.status_code == 502

    def test_missing_user_code_returns_502(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((200, {"error": "bad_client_id", "error_description": "bad id"})),
        )

        resp = client.post("/api/setup/github/device-code", json={})
        assert resp.status_code == 502
        assert "bad id" in resp.json()["error"]


class TestDeviceCodeFlowPoll:
    def test_pending_status(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((200, {"error": "authorization_pending"})),
        )

        resp = client.post(
            "/api/setup/github/device-code/poll",
            json={"device_code": "dc_abc"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "pending"

    def test_complete_saves_token(self, db_file, tmp_path, monkeypatch):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("bugtracker:\n  type: linear\n")
        env_file = tmp_path / ".env"
        config = _make_config(source_path=str(config_file))
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((200, {
                "access_token": "gho_device_token_xyz",
                "token_type": "bearer",
                "scope": "repo,read:org",
            })),
        )
        monkeypatch.delenv("GH_TOKEN", raising=False)

        resp = client.post(
            "/api/setup/github/device-code/poll",
            json={"device_code": "dc_abc"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "complete"

        # Token saved to .env
        assert env_file.exists()
        env_content = env_file.read_text()
        assert "GH_TOKEN" in env_content
        assert "gho_device_token_xyz" in env_content

        # Process environment updated
        assert os.environ.get("GH_TOKEN") == "gho_device_token_xyz"

    def test_slow_down_returns_new_interval(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((200, {"error": "slow_down", "interval": 10})),
        )

        resp = client.post(
            "/api/setup/github/device-code/poll",
            json={"device_code": "dc_abc"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "slow_down"
        assert data["interval"] == 10

    def test_expired_token(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((200, {"error": "expired_token"})),
        )

        resp = client.post(
            "/api/setup/github/device-code/poll",
            json={"device_code": "dc_abc"},
        )
        assert resp.json()["status"] == "expired"

    def test_access_denied(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client((200, {"error": "access_denied"})),
        )

        resp = client.post(
            "/api/setup/github/device-code/poll",
            json={"device_code": "dc_abc"},
        )
        assert resp.json()["status"] == "denied"

    def test_missing_device_code_returns_400(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post(
            "/api/setup/github/device-code/poll",
            json={"device_code": ""},
        )
        assert resp.status_code == 400

    def test_invalid_json_returns_400(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post(
            "/api/setup/github/device-code/poll",
            content="not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400

    def test_network_error_during_poll(self, db_file, monkeypatch):
        config = _make_config(source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup.httpx.AsyncClient",
            _fake_httpx_client_error(),
        )

        resp = client.post(
            "/api/setup/github/device-code/poll",
            json={"device_code": "dc_abc"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "error"


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
        assert "Start Authentication" in resp.text
        assert "startClaudeAuth" in resp.text

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

    def test_terminal_panel_shown_for_claude_when_enabled(
        self, db_file, monkeypatch,
    ):
        from botfarm.credentials import CredentialError

        config = _make_config(api_key="key123", bt_type="linear")
        config.dashboard = DashboardConfig(terminal_enabled=True)
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
        assert resp.status_code == 200
        assert "Open Terminal" in resp.text
        assert "claude-terminal-panel" in resp.text
        assert "SSH into the server" not in resp.text

    def test_terminal_panel_shown_for_github_when_enabled(
        self, db_file, monkeypatch,
    ):
        from botfarm.credentials import CredentialError

        config = _make_config(api_key="key123", bt_type="linear")
        config.dashboard = DashboardConfig(terminal_enabled=True)
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
        assert resp.status_code == 200
        assert "Or use terminal" in resp.text
        assert "github-terminal-panel" in resp.text

    def test_terminal_panels_hidden_when_authenticated(
        self, db_file, monkeypatch,
    ):
        from botfarm.credentials import OAuthToken

        config = _make_config(api_key="key123", bt_type="linear")
        config.dashboard = DashboardConfig(terminal_enabled=True)
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        monkeypatch.setenv("GH_TOKEN", "ghp_test")
        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: OAuthToken(access_token="x"),
        )

        resp = client.get("/partials/setup-credentials")
        assert resp.status_code == 200
        assert "Authenticated" in resp.text
        # Terminal button/panel HTML elements should not be rendered when
        # authenticated (JS references to IDs may still exist).
        assert 'id="claude-terminal-toggle"' not in resp.text
        assert 'id="github-terminal-details"' not in resp.text
        # Device code flow button should also not appear when authenticated.
        assert 'id="github-device-flow-btn"' not in resp.text

    def test_device_code_button_visible_without_terminal(
        self, db_file, monkeypatch,
    ):
        """Device code flow button appears even when terminal is disabled."""
        from botfarm.credentials import CredentialError

        config = _make_config(api_key="key123", bt_type="linear")
        # terminal_enabled defaults to False
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
        assert resp.status_code == 200
        assert "Authenticate with GitHub" in resp.text
        assert 'id="github-device-flow-btn"' in resp.text
        # Terminal should NOT appear since terminal_enabled is false
        assert 'id="github-terminal-details"' not in resp.text


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
        # The server-rendered banner (outside <script>) should not appear.
        # Split on <script> to isolate the HTML body from JS code that may
        # contain "Setup Complete" as a string literal.
        html_before_script = resp.text.split("<script>")[0]
        assert "Setup Complete" not in html_before_script
        assert "Go to Dashboard" not in html_before_script

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

    def test_shows_inline_project_form(self, db_file, monkeypatch):
        """When no projects exist, setup wizard shows inline form (not a link)."""
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "setup-project-form" in resp.text
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


# ---------------------------------------------------------------------------
# Setup complete endpoint tests
# ---------------------------------------------------------------------------

class TestSetupCompleteEndpoint:
    def test_returns_preflight_not_triggered_without_callback(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/complete")
        assert resp.status_code == 200
        data = resp.json()
        assert data["preflight_triggered"] is False

    def test_triggers_preflight_callback(self, db_file):
        called = []
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
            on_rerun_preflight=lambda: called.append(True),
        )
        client = TestClient(app)

        resp = client.post("/api/setup/complete")
        assert resp.status_code == 200
        assert len(called) == 1

    def test_no_config_still_triggers(self, db_file):
        called = []
        app = create_app(
            db_path=db_file,
            botfarm_config=None,
            on_rerun_preflight=lambda: called.append(True),
        )
        client = TestClient(app)

        resp = client.post("/api/setup/complete")
        assert resp.status_code == 200
        data = resp.json()
        assert data["preflight_triggered"] is True
        assert len(called) == 1

    def test_no_preflight_callback_still_works(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/complete")
        assert resp.status_code == 200
        assert resp.json()["preflight_triggered"] is False

    def test_callback_exception_returns_not_triggered(self, db_file):
        def _boom():
            raise RuntimeError("preflight exploded")

        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
            on_rerun_preflight=_boom,
        )
        client = TestClient(app)

        resp = client.post("/api/setup/complete")
        assert resp.status_code == 200
        assert resp.json()["preflight_triggered"] is False


# ---------------------------------------------------------------------------
# Bugtracker-agnostic team/project endpoint tests
# ---------------------------------------------------------------------------

class TestBugtrackerEndpoints:
    def test_teams_no_config(self, db_file):
        app = create_app(db_path=db_file, botfarm_config=None)
        client = TestClient(app)

        resp = client.get("/api/bugtracker/teams")
        assert resp.status_code == 503

    def test_teams_returns_list(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        class FakeClient:
            def list_teams(self):
                return [{"key": "ENG", "name": "Engineering"}]

        monkeypatch.setattr(
            "botfarm.dashboard.routes_projects.create_client",
            lambda *a, **kw: FakeClient(),
        )

        resp = client.get("/api/bugtracker/teams")
        assert resp.status_code == 200
        teams = resp.json()
        assert len(teams) == 1
        assert teams[0]["key"] == "ENG"

    def test_projects_missing_team(self, db_file):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/api/bugtracker/projects")
        assert resp.status_code == 400

    def test_projects_returns_list(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        class FakeClient:
            def get_team_id(self, key):
                return "team-123"

            def list_team_projects(self, team_id):
                return [{"id": "p1", "name": "My Project"}]

        monkeypatch.setattr(
            "botfarm.dashboard.routes_projects.create_client",
            lambda *a, **kw: FakeClient(),
        )

        resp = client.get("/api/bugtracker/projects?team=ENG")
        assert resp.status_code == 200
        projects = resp.json()
        assert len(projects) == 1
        assert projects[0]["name"] == "My Project"


# ---------------------------------------------------------------------------
# Setup wizard project form tests
# ---------------------------------------------------------------------------

class TestSetupProjectForm:
    def test_shows_inline_form_when_no_projects(self, db_file, monkeypatch):
        config = _make_config(api_key="key123", bt_type="linear")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        assert "setup-project-form" in resp.text
        assert "submitSetupProject" in resp.text
        assert "setup-repo-url" in resp.text

    def test_shows_project_count_when_has_projects(
        self, db_file, tmp_path, monkeypatch,
    ):
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
        # Should link to add-project page instead of inline form
        assert "Add Another Project" in resp.text
        # The actual form element (not JS references) should not be present
        text = resp.text
        proj_start = text.find('id="section-project"')
        proj_end = text.find('id="section-verification"')
        project_section = text[proj_start:proj_end]
        assert 'id="setup-project-form"' not in project_section

    def test_form_disabled_without_bugtracker(self, db_file, monkeypatch):
        config = _make_config(api_key="", bt_type="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        # Non-linear bt_type → shows link instead of inline form
        # Extract the project section (between section-project and section-verification)
        text = resp.text
        proj_start = text.find('id="section-project"')
        proj_end = text.find('id="section-verification"')
        project_section = text[proj_start:proj_end]
        assert 'id="setup-project-form"' not in project_section
        assert "/projects/add" in project_section

    def test_jira_shows_link_instead_of_inline_form(self, db_file, monkeypatch):
        from botfarm.config import JiraBugtrackerConfig

        bt = JiraBugtrackerConfig(
            type="jira", api_key="tok", url="https://x.atlassian.net", email="a@b.c",
        )
        config = BotfarmConfig(projects=[], bugtracker=bt, source_path="")
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)
        _mock_auth_unavailable(monkeypatch)

        resp = client.get("/setup")
        text = resp.text
        proj_start = text.find('id="section-project"')
        proj_end = text.find('id="section-verification"')
        project_section = text[proj_start:proj_end]
        assert 'id="setup-project-form"' not in project_section
        assert "/projects/add" in project_section


# ---------------------------------------------------------------------------
# _extract_auth_url unit tests
# ---------------------------------------------------------------------------

class TestExtractAuthUrl:
    def test_extracts_https_url(self):
        text = "Open this URL: https://platform.claude.ai/oauth/authorize?client_id=abc"
        url = _extract_auth_url(text)
        assert url == "https://platform.claude.ai/oauth/authorize?client_id=abc"

    def test_extracts_url_with_trailing_punctuation(self):
        text = "Visit https://example.com/auth."
        url = _extract_auth_url(text)
        assert url == "https://example.com/auth"

    def test_extracts_url_with_trailing_paren(self):
        text = "(https://example.com/auth)"
        url = _extract_auth_url(text)
        assert url == "https://example.com/auth"

    def test_extracts_url_with_query_params(self):
        text = "URL: https://auth.example.com/login?code=abc&state=xyz"
        url = _extract_auth_url(text)
        assert url == "https://auth.example.com/login?code=abc&state=xyz"

    def test_returns_none_for_no_url(self):
        text = "No URL here, just plain text."
        assert _extract_auth_url(text) is None

    def test_returns_none_for_empty_string(self):
        assert _extract_auth_url("") is None

    def test_extracts_first_url_from_multiline(self):
        text = "Starting auth...\nhttps://first.example.com/auth\nhttps://second.example.com"
        url = _extract_auth_url(text)
        assert url == "https://first.example.com/auth"

    def test_ignores_http_urls(self):
        text = "http://insecure.example.com then https://secure.example.com/auth"
        url = _extract_auth_url(text)
        assert url == "https://secure.example.com/auth"

    def test_handles_ansi_escape_codes_in_url(self):
        # claude may output ANSI codes — URL should stop at them
        text = "Visit https://example.com/auth\x1b[0m to continue"
        url = _extract_auth_url(text)
        assert url == "https://example.com/auth"


# ---------------------------------------------------------------------------
# Claude auth API endpoint tests
# ---------------------------------------------------------------------------

class TestClaudeAuthEndpoint:
    def test_returns_already_authenticated(self, db_file, monkeypatch):
        from botfarm.credentials import OAuthToken

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: OAuthToken(access_token="test"),
        )
        config = _make_config()
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/claude/auth", json={})
        assert resp.status_code == 200
        assert resp.json()["status"] == "already_authenticated"

    def test_returns_error_when_claude_not_found(self, db_file, monkeypatch):
        _mock_auth_unavailable(monkeypatch)
        config = _make_config()
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.post("/api/setup/claude/auth", json={})
        assert resp.status_code == 400
        data = resp.json()
        assert "error" in data
        assert "not installed" in data["error"] or "not on PATH" in data["error"]


class TestClaudeAuthStatusEndpoint:
    def test_returns_not_authenticated_and_expired_when_no_process(self, db_file, monkeypatch):
        _mock_auth_unavailable(monkeypatch)
        # Ensure no active auth state
        monkeypatch.setattr("botfarm.dashboard.routes_setup._claude_auth_state", None)
        config = _make_config()
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/api/setup/claude/auth/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["authenticated"] is False
        assert data["expired"] is True

    def test_returns_authenticated(self, db_file, monkeypatch):
        from botfarm.credentials import OAuthToken

        monkeypatch.setattr(
            "botfarm.dashboard.routes_setup._load_token",
            lambda: OAuthToken(access_token="test"),
        )
        config = _make_config()
        app = create_app(db_path=db_file, botfarm_config=config)
        client = TestClient(app)

        resp = client.get("/api/setup/claude/auth/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["authenticated"] is True
        assert data["expired"] is False
