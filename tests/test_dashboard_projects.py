"""Tests for dashboard project routes: Linear lookups, project creation, SSE progress, and removal."""

import time
from unittest.mock import MagicMock, patch

import yaml
from fastapi.testclient import TestClient

from botfarm.config import BotfarmConfig, LinearConfig, ProjectConfig
from botfarm.dashboard import create_app
from botfarm.db import init_db, upsert_slot


def _make_config(*, projects=None, api_key="test-key", source_path=""):
    cfg = BotfarmConfig(
        projects=projects or [],
        bugtracker=LinearConfig(api_key=api_key),
    )
    if source_path:
        cfg.source_path = source_path
    return cfg


def _make_app(tmp_path, *, config=None, on_add_project=None, on_remove_project=None):
    db_path = tmp_path / "test.db"
    conn = init_db(db_path)
    conn.close()
    return create_app(
        db_path=db_path,
        botfarm_config=config or _make_config(),
        on_add_project=on_add_project,
        on_remove_project=on_remove_project,
    )


class TestLinearTeamsEndpoint:
    def test_returns_teams(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        mock_teams = [{"id": "1", "name": "Engineering", "key": "ENG"}]
        with patch("botfarm.dashboard.routes_projects.create_client") as MockClient:
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
        with patch("botfarm.dashboard.routes_projects.create_client") as MockClient:
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
            json={"repo_url": "", "name": "", "team": "", "slots": 0},
        )
        assert resp.status_code == 400
        data = resp.json()
        assert len(data["errors"]) >= 3

    def test_duplicate_project_name(self, tmp_path):
        existing = ProjectConfig(
            name="my-proj", team="ENG",
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
                "team": "ENG",
                "slots": 1,
            },
        )
        assert resp.status_code == 400
        assert "already exists" in resp.json()["errors"][0]

    def test_duplicate_linear_project(self, tmp_path):
        existing = ProjectConfig(
            name="other-proj", team="ENG", tracker_project="Bot farm",
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
                "team": "ENG",
                "tracker_project": "Bot farm",
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
                "team": "ENG",
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
                "team": "ENG",
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
                    "team": "ENG",
                    "slots": 2,
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "started"
            assert "task_id" in data
            # Wait for the background thread to call the mocked function
            time.sleep(0.1)

    def test_passes_project_type_and_setup_commands(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup:
            mock_setup.return_value = {"name": "typed"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "typed",
                    "team": "ENG",
                    "slots": 1,
                    "project_type": "python",
                    "setup_commands": "pip install -r requirements.txt\npip install -e .",
                },
            )
            assert resp.status_code == 200
            time.sleep(0.1)
            call_kwargs = mock_setup.call_args
            assert call_kwargs[1]["project_type"] == "python"
            assert call_kwargs[1]["setup_commands"] == [
                "pip install -r requirements.txt",
                "pip install -e .",
            ]

    def test_setup_commands_as_list(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup:
            mock_setup.return_value = {"name": "listed"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "listed",
                    "team": "ENG",
                    "slots": 1,
                    "setup_commands": ["npm install"],
                },
            )
            assert resp.status_code == 200
            time.sleep(0.1)
            call_kwargs = mock_setup.call_args
            assert call_kwargs[1]["setup_commands"] == ["npm install"]

    def test_empty_setup_commands(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup:
            mock_setup.return_value = {"name": "empty"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "empty",
                    "team": "ENG",
                    "slots": 1,
                },
            )
            assert resp.status_code == 200
            time.sleep(0.1)
            call_kwargs = mock_setup.call_args
            assert call_kwargs[1]["setup_commands"] is None

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
                "team": "ENG",
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


class TestProjectCreateNoRepoUrl:
    """Test creating projects without a repo URL (init path)."""

    def test_empty_repo_url_accepted(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup:
            mock_setup.return_value = {"name": "new-proj"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "",
                    "name": "new-proj",
                    "team": "ENG",
                    "slots": 2,
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "started"
            time.sleep(0.1)

    def test_invalid_repo_url_still_rejected(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/create",
            json={
                "repo_url": "not-a-url",
                "name": "test",
                "team": "ENG",
                "slots": 1,
            },
        )
        assert resp.status_code == 400
        assert any("URL" in e for e in resp.json()["errors"])

    def test_create_github_passed_through(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup:
            mock_setup.return_value = {"name": "gh-proj"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "",
                    "name": "gh-proj",
                    "team": "ENG",
                    "slots": 1,
                    "create_github": True,
                },
            )
            assert resp.status_code == 200
            time.sleep(0.1)
            # Verify create_github was passed
            assert mock_setup.call_args.kwargs.get("create_github") is True


class TestSetupGitEndpoint:
    def test_missing_name(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/setup-git",
            json={"name": ""},
        )
        assert resp.status_code == 400

    def test_unknown_project(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/setup-git",
            json={"name": "nonexistent"},
        )
        assert resp.status_code == 404

    def test_successful_start(self, tmp_path):
        existing = ProjectConfig(
            name="my-proj", team="ENG",
            base_dir="/tmp/x", worktree_prefix="/tmp/x-slot-", slots=[1],
        )
        cfg = _make_config(projects=[existing])
        app = _make_app(tmp_path, config=cfg)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project_git") as mock_setup:
            resp = client.post(
                "/api/project/setup-git",
                json={"name": "my-proj"},
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["status"] == "started"
            assert "task_id" in data
            time.sleep(0.1)

    def test_no_config(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.post(
            "/api/project/setup-git",
            json={"name": "test"},
        )
        assert resp.status_code == 503


class TestAddProjectPage:
    def test_renders(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/projects/add")
        assert resp.status_code == 200
        assert "Add Project" in resp.text
        assert "repo_url" in resp.text

    def test_renders_create_linear_project_checkbox(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/projects/add")
        assert resp.status_code == 200
        assert "create_linear_project" in resp.text
        assert "Create new Linear project" in resp.text

    def test_add_project_button_on_status_page(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Add Project" in resp.text
        assert "/projects/add" in resp.text


class TestLinearProjectAutoCreate:
    def test_auto_creates_linear_project_on_setup(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        mock_linear = MagicMock()
        mock_linear.get_or_create_project.return_value = {
            "id": "proj-new", "name": "my-proj",
        }
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup, \
             patch("botfarm.dashboard.routes_projects._get_bugtracker_client", return_value=mock_linear):
            mock_setup.return_value = {"name": "my-proj"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "my-proj",
                    "team": "ENG",
                    "create_linear_project": True,
                    "tracker_project": "my-proj",
                    "slots": 1,
                },
            )
            assert resp.status_code == 200
            time.sleep(0.2)
            mock_linear.get_or_create_project.assert_called_once_with("ENG", "my-proj")

    def test_skips_linear_creation_when_flag_false(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        mock_linear = MagicMock()
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup, \
             patch("botfarm.dashboard.routes_projects._get_bugtracker_client", return_value=mock_linear):
            mock_setup.return_value = {"name": "my-proj"}
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "my-proj",
                    "team": "ENG",
                    "create_linear_project": False,
                    "tracker_project": "Existing Project",
                    "slots": 1,
                },
            )
            assert resp.status_code == 200
            time.sleep(0.2)
            mock_linear.get_or_create_project.assert_not_called()

    def test_linear_creation_failure_reports_error(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        mock_linear = MagicMock()
        mock_linear.get_or_create_project.side_effect = Exception("API down")
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup, \
             patch("botfarm.dashboard.routes_projects._get_bugtracker_client", return_value=mock_linear):
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "my-proj",
                    "team": "ENG",
                    "create_linear_project": True,
                    "tracker_project": "my-proj",
                    "slots": 1,
                },
            )
            assert resp.status_code == 200
            time.sleep(0.2)
            # setup_project should NOT be called since linear creation failed
            mock_setup.assert_not_called()

    def test_no_linear_client_reports_error(self, tmp_path):
        app = _make_app(tmp_path)
        client = TestClient(app)
        with patch("botfarm.dashboard.routes_projects.setup_project") as mock_setup, \
             patch("botfarm.dashboard.routes_projects._get_bugtracker_client", return_value=None):
            resp = client.post(
                "/api/project/create",
                json={
                    "repo_url": "git@github.com:user/repo.git",
                    "name": "my-proj",
                    "team": "ENG",
                    "create_linear_project": True,
                    "tracker_project": "my-proj",
                    "slots": 1,
                },
            )
            assert resp.status_code == 200
            time.sleep(0.2)
            # setup_project should NOT be called since linear client is unavailable
            mock_setup.assert_not_called()


# ---------------------------------------------------------------------------
# Remove-project endpoints
# ---------------------------------------------------------------------------


def _write_config_yaml(tmp_path, projects):
    """Write a config.yaml with the given project dicts and return the path."""
    config_path = tmp_path / "config.yaml"
    data = {"projects": projects}
    config_path.write_text(yaml.dump(data, default_flow_style=False))
    return config_path


def _make_remove_app(tmp_path, *, project_dicts=None, project_configs=None,
                     on_remove_project=None):
    """Create an app wired for remove-project tests.

    *project_dicts* are the raw YAML dicts written to config.yaml.
    *project_configs* are the ProjectConfig objects on BotfarmConfig.
    """
    if project_dicts is None:
        project_dicts = [{"name": "demo", "team": "ENG", "base_dir": "/tmp/demo/repo",
                          "worktree_prefix": "/tmp/demo/slot-", "slots": [1, 2]}]
    if project_configs is None:
        project_configs = [
            ProjectConfig(name=p["name"], team=p["team"],
                          base_dir=p.get("base_dir", ""),
                          worktree_prefix=p.get("worktree_prefix", ""),
                          slots=p.get("slots", [1]))
            for p in project_dicts
        ]
    config_path = _write_config_yaml(tmp_path, project_dicts)
    cfg = _make_config(projects=project_configs, source_path=str(config_path))
    db_path = tmp_path / "test.db"
    conn = init_db(db_path)
    conn.close()
    return create_app(
        db_path=db_path,
        botfarm_config=cfg,
        on_remove_project=on_remove_project,
    )


class TestRemoveInfoEndpoint:
    def test_project_not_found(self, tmp_path):
        app = _make_remove_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/project/nonexistent/remove-info")
        assert resp.status_code == 404

    def test_returns_project_details(self, tmp_path):
        app = _make_remove_app(tmp_path)
        client = TestClient(app)
        resp = client.get("/api/project/demo/remove-info")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "demo"
        assert data["team"] == "ENG"
        assert data["slots"] == [1, 2]
        assert data["active_slots"] == []

    def test_shows_active_slots(self, tmp_path):
        project_dicts = [{"name": "busy-proj", "team": "ENG",
                          "base_dir": "/tmp/bp/repo",
                          "worktree_prefix": "/tmp/bp/slot-", "slots": [1]}]
        project_configs = [
            ProjectConfig(name="busy-proj", team="ENG",
                          base_dir="/tmp/bp/repo",
                          worktree_prefix="/tmp/bp/slot-", slots=[1]),
        ]
        config_path = _write_config_yaml(tmp_path, project_dicts)
        cfg = _make_config(projects=project_configs, source_path=str(config_path))
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        upsert_slot(conn, {
            "project": "busy-proj", "slot_id": 1, "status": "busy",
            "ticket_id": "SMA-100", "ticket_title": "test", "branch": "",
            "pr_url": "", "stage": "implement", "stage_iteration": 1,
            "current_session_id": "", "started_at": "", "stage_started_at": "",
            "sigterm_sent_at": None, "pid": None, "interrupted_by_limit": 0,
            "resume_after": None, "stages_completed": [], "ticket_labels": [],
        })
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/api/project/busy-proj/remove-info")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["active_slots"]) == 1
        assert data["active_slots"][0]["ticket_id"] == "SMA-100"

    def test_no_config(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/api/project/demo/remove-info")
        assert resp.status_code == 503


class TestRemoveEndpoint:
    def test_missing_name(self, tmp_path):
        app = _make_remove_app(tmp_path)
        client = TestClient(app)
        resp = client.post("/api/project/remove", json={"name": ""})
        assert resp.status_code == 400

    def test_project_not_found_in_yaml(self, tmp_path):
        app = _make_remove_app(tmp_path)
        client = TestClient(app)
        resp = client.post("/api/project/remove", json={"name": "nonexistent"})
        assert resp.status_code == 404

    def test_successful_removal(self, tmp_path):
        on_remove = MagicMock()
        app = _make_remove_app(tmp_path, on_remove_project=on_remove)
        client = TestClient(app)
        resp = client.post("/api/project/remove", json={"name": "demo"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True

        # Config YAML should no longer contain the project
        config_path = tmp_path / "config.yaml"
        reloaded = yaml.safe_load(config_path.read_text())
        assert len(reloaded["projects"]) == 0

        # Supervisor callback should have been invoked
        on_remove.assert_called_once_with("demo")

    def test_active_slots_block_removal(self, tmp_path):
        project_dicts = [{"name": "active", "team": "ENG",
                          "base_dir": "/tmp/a/repo",
                          "worktree_prefix": "/tmp/a/slot-", "slots": [1]}]
        project_configs = [
            ProjectConfig(name="active", team="ENG",
                          base_dir="/tmp/a/repo",
                          worktree_prefix="/tmp/a/slot-", slots=[1]),
        ]
        config_path = _write_config_yaml(tmp_path, project_dicts)
        cfg = _make_config(projects=project_configs, source_path=str(config_path))
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        upsert_slot(conn, {
            "project": "active", "slot_id": 1, "status": "busy",
            "ticket_id": "SMA-99", "ticket_title": "test", "branch": "",
            "pr_url": "", "stage": "implement", "stage_iteration": 1,
            "current_session_id": "", "started_at": "", "stage_started_at": "",
            "sigterm_sent_at": None, "pid": None, "interrupted_by_limit": 0,
            "resume_after": None, "stages_completed": [], "ticket_labels": [],
        })
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.post("/api/project/remove", json={"name": "active"})
        assert resp.status_code == 409
        assert "active slot" in resp.json()["errors"][0].lower()

    def test_force_bypasses_active_check(self, tmp_path):
        project_dicts = [{"name": "forced", "team": "ENG",
                          "base_dir": "/tmp/f/repo",
                          "worktree_prefix": "/tmp/f/slot-", "slots": [1]}]
        project_configs = [
            ProjectConfig(name="forced", team="ENG",
                          base_dir="/tmp/f/repo",
                          worktree_prefix="/tmp/f/slot-", slots=[1]),
        ]
        config_path = _write_config_yaml(tmp_path, project_dicts)
        cfg = _make_config(projects=project_configs, source_path=str(config_path))
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        upsert_slot(conn, {
            "project": "forced", "slot_id": 1, "status": "busy",
            "ticket_id": "SMA-88", "ticket_title": "test", "branch": "",
            "pr_url": "", "stage": "implement", "stage_iteration": 1,
            "current_session_id": "", "started_at": "", "stage_started_at": "",
            "sigterm_sent_at": None, "pid": None, "interrupted_by_limit": 0,
            "resume_after": None, "stages_completed": [], "ticket_labels": [],
        })
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.post("/api/project/remove",
                           json={"name": "forced", "force": True})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_clean_removes_repo_dir(self, tmp_path):
        # Set up a managed project directory
        managed_root = tmp_path / "projects"
        proj_dir = managed_root / "demo"
        repo_dir = proj_dir / "repo"
        repo_dir.mkdir(parents=True)
        (repo_dir / "README.md").write_text("hello")

        project_dicts = [{"name": "demo", "team": "ENG",
                          "base_dir": str(repo_dir),
                          "worktree_prefix": str(proj_dir / "slot-"),
                          "slots": [1]}]
        project_configs = [
            ProjectConfig(name="demo", team="ENG",
                          base_dir=str(repo_dir),
                          worktree_prefix=str(proj_dir / "slot-"), slots=[1]),
        ]
        config_path = _write_config_yaml(tmp_path, project_dicts)
        cfg = _make_config(projects=project_configs, source_path=str(config_path))
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path, botfarm_config=cfg)
        client = TestClient(app)

        # Patch DEFAULT_CONFIG_DIR so managed_root check works with tmp_path
        with patch("botfarm.dashboard.routes_projects.DEFAULT_CONFIG_DIR", tmp_path):
            resp = client.post("/api/project/remove",
                               json={"name": "demo", "clean": True})
        assert resp.status_code == 200
        assert resp.json()["data"]["cleaned_dir"] is True
        assert not proj_dir.exists()

    def test_no_config(self, tmp_path):
        db_path = tmp_path / "test.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.post("/api/project/remove", json={"name": "test"})
        assert resp.status_code == 503
