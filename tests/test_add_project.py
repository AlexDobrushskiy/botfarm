"""Tests for botfarm add-project CLI command."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from botfarm.cli import (
    _append_project_to_config,
    _extract_repo_name,
    _run_readiness_checks,
    main,
)
from botfarm.linear import LinearAPIError


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def config_dir(tmp_path):
    """Create a tmp config dir with a minimal valid config.yaml."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "projects:\n"
        "  - name: existing-proj\n"
        "    linear_team: SMA\n"
        "    base_dir: /tmp/existing\n"
        "    worktree_prefix: existing-slot-\n"
        "    slots: [1]\n"
        "linear:\n"
        "  api_key: test-key\n"
    )
    return tmp_path, config_path


@pytest.fixture()
def git_repo(tmp_path):
    """Create a directory that looks like a git repo (for tests that mock validation)."""
    repo = tmp_path / "my-repo"
    repo.mkdir()
    (repo / ".git").mkdir()
    return repo


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch, tmp_path):
    """Prevent load_dotenv from loading real env files."""
    monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", tmp_path / "nonexistent.env")


def _make_mock_run(tmp_path):
    """Create a subprocess.run mock that simulates git clone and worktree add."""

    def mock_run(cmd, **kwargs):
        result = MagicMock()
        result.returncode = 0
        result.stderr = ""
        result.stdout = ""
        if cmd[0] == "git" and "clone" in cmd:
            target = Path(cmd[-1])
            target.mkdir(parents=True, exist_ok=True)
            (target / ".git").mkdir(exist_ok=True)
        elif cmd[0] == "git" and "worktree" in cmd and "add" in cmd:
            for i, arg in enumerate(cmd):
                if arg == "add":
                    path_idx = i + 3  # add -b <branch> <path>
                    if path_idx < len(cmd):
                        Path(cmd[path_idx]).mkdir(parents=True, exist_ok=True)
                    break
        return result

    return mock_run


# ---------------------------------------------------------------------------
# _extract_repo_name
# ---------------------------------------------------------------------------


class TestExtractRepoName:
    def test_ssh_url(self):
        assert _extract_repo_name("git@github.com:user/my-app.git") == "my-app"

    def test_https_url(self):
        assert _extract_repo_name("https://github.com/user/my-app.git") == "my-app"

    def test_no_git_suffix(self):
        assert _extract_repo_name("https://github.com/user/my-app") == "my-app"

    def test_trailing_slash(self):
        assert _extract_repo_name("https://github.com/user/my-app.git/") == "my-app"

    def test_ssh_no_git_suffix(self):
        assert _extract_repo_name("git@github.com:org/repo-name") == "repo-name"

    def test_plain_name(self):
        assert _extract_repo_name("my-repo") == "my-repo"


# ---------------------------------------------------------------------------
# _run_readiness_checks
# ---------------------------------------------------------------------------


class TestRunReadinessChecks:
    def test_claude_md_exists(self, tmp_path):
        base = tmp_path / "proj"
        base.mkdir()
        (base / "CLAUDE.md").write_text("# Project")
        project = {
            "name": "proj",
            "linear_team": "SMA",
            "base_dir": str(base),
            "worktree_prefix": "proj-slot-",
            "slots": [1],
            "linear_project": "",
        }
        results = _run_readiness_checks(project)
        ok_results = [r for r in results if r[0] == "ok"]
        assert any("CLAUDE.md found" in msg for _, msg in ok_results)

    def test_claude_md_missing(self, tmp_path):
        base = tmp_path / "proj"
        base.mkdir()
        project = {
            "name": "proj",
            "linear_team": "SMA",
            "base_dir": str(base),
            "worktree_prefix": "proj-slot-",
            "slots": [1],
            "linear_project": "",
        }
        results = _run_readiness_checks(project)
        warnings = [r for r in results if r[0] == "warning"]
        assert any("No CLAUDE.md" in msg for _, msg in warnings)

    def test_python_runtime_detected(self, tmp_path):
        base = tmp_path / "proj"
        base.mkdir()
        (base / "requirements.txt").write_text("click\n")
        project = {
            "name": "proj",
            "linear_team": "SMA",
            "base_dir": str(base),
            "worktree_prefix": "proj-slot-",
            "slots": [1],
            "linear_project": "",
        }
        results = _run_readiness_checks(project)
        ok_results = [r for r in results if r[0] == "ok"]
        assert any("Python runtime" in msg for _, msg in ok_results)


# ---------------------------------------------------------------------------
# _append_project_to_config
# ---------------------------------------------------------------------------


class TestAppendProjectToConfig:
    def test_appends_project(self, config_dir):
        _, config_path = config_dir
        project = {
            "name": "new-proj",
            "linear_team": "TEAM",
            "base_dir": "/tmp/new",
            "worktree_prefix": "new-slot-",
            "slots": [1, 2],
            "linear_project": "",
        }
        _append_project_to_config(config_path, project)

        data = yaml.safe_load(config_path.read_text())
        assert len(data["projects"]) == 2
        added = data["projects"][1]
        assert added["name"] == "new-proj"
        assert added["slots"] == [1, 2]
        # linear_project should be omitted when empty
        assert "linear_project" not in added

    def test_includes_linear_project_when_set(self, config_dir):
        _, config_path = config_dir
        project = {
            "name": "new-proj",
            "linear_team": "TEAM",
            "base_dir": "/tmp/new",
            "worktree_prefix": "new-slot-",
            "slots": [1],
            "linear_project": "My Project",
        }
        _append_project_to_config(config_path, project)

        data = yaml.safe_load(config_path.read_text())
        added = data["projects"][1]
        assert added["linear_project"] == "My Project"

    def test_creates_projects_list_if_missing(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("linear:\n  api_key: test\n")
        project = {
            "name": "proj",
            "linear_team": "SMA",
            "base_dir": "/tmp/proj",
            "worktree_prefix": "proj-slot-",
            "slots": [1],
            "linear_project": "",
        }
        _append_project_to_config(config_path, project)

        data = yaml.safe_load(config_path.read_text())
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "proj"

    def test_raises_on_non_list_projects(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("projects: not-a-list\nlinear:\n  api_key: test\n")
        project = {
            "name": "proj",
            "linear_team": "SMA",
            "base_dir": "/tmp/proj",
            "worktree_prefix": "proj-slot-",
            "slots": [1],
            "linear_project": "",
        }
        with pytest.raises(Exception, match="not a list"):
            _append_project_to_config(config_path, project)


# ---------------------------------------------------------------------------
# add-project CLI command (integration tests)
# ---------------------------------------------------------------------------


class TestAddProjectCommand:
    def test_config_not_found(self, runner, tmp_path):
        result = runner.invoke(
            main,
            ["add-project", "--config", str(tmp_path / "nonexistent.yaml")],
        )
        assert result.exit_code != 0
        assert "Config file not found" in result.output

    def test_duplicate_project_name(self, runner, config_dir, monkeypatch):
        _, config_path = config_dir
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)
        result = runner.invoke(
            main,
            ["add-project", "--config", str(config_path)],
            input="git@github.com:user/existing-proj.git\nexisting-proj\n",
        )
        assert result.exit_code != 0
        assert "already exists" in result.output

    def test_full_flow_with_linear_api(self, runner, config_dir, tmp_path, monkeypatch):
        """Test the full interactive flow with Linear API team/project selection."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.setenv("LINEAR_API_KEY", "test-key")

        mock_client = MagicMock()
        mock_client.list_teams.return_value = [
            {"id": "t1", "name": "Engineering", "key": "ENG"},
            {"id": "t2", "name": "Smart AI Coach", "key": "SMA"},
        ]
        mock_client.list_team_projects.return_value = [
            {"id": "p1", "name": "Bot farm"},
            {"id": "p2", "name": "Other project"},
        ]

        with patch("botfarm.cli.subprocess.run", side_effect=_make_mock_run(tmp_path)), \
             patch("botfarm.cli.LinearClient", return_value=mock_client):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                # repo URL, name, team choice, project choice, slots, confirm
                input="git@github.com:user/my-app.git\nmy-app\n2\n1\n2\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert "added successfully" in result.output

        config = yaml.safe_load(config_path.read_text())
        added = next(p for p in config["projects"] if p["name"] == "my-app")
        assert added["linear_team"] == "SMA"
        assert added["linear_project"] == "Bot farm"
        assert added["slots"] == [1, 2]

    def test_flow_without_linear_key(self, runner, config_dir, tmp_path, monkeypatch):
        """Test add-project when LINEAR_API_KEY is not set."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        with patch("botfarm.cli.subprocess.run", side_effect=_make_mock_run(tmp_path)):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                # repo URL, name, team key (manual), project filter (manual), slots, confirm
                input="git@github.com:user/my-app.git\nmy-app\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert "added successfully" in result.output

        config = yaml.safe_load(config_path.read_text())
        added = next(p for p in config["projects"] if p["name"] == "my-app")
        assert added["linear_team"] == "SMA"
        assert added["slots"] == [1]

    def test_aborted_by_user(self, runner, config_dir, tmp_path, monkeypatch):
        """Test that the user can abort before proceeding."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        result = runner.invoke(
            main,
            ["add-project", "--config", str(config_path)],
            input="git@github.com:user/my-app.git\nmy-app\nSMA\n\n1\nn\n",
        )

        assert result.exit_code == 0
        assert "Aborted" in result.output

    def test_git_clone_failure(self, runner, config_dir, tmp_path, monkeypatch):
        """Test error handling when git clone fails."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        def fail_clone(cmd, **kwargs):
            result = MagicMock()
            if "clone" in cmd:
                result.returncode = 128
                result.stderr = "fatal: repository not found"
            else:
                result.returncode = 0
                result.stderr = ""
            return result

        with patch("botfarm.cli.subprocess.run", side_effect=fail_clone):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/nonexistent.git\nmy-app\nSMA\n\n1\ny\n",
            )

        assert result.exit_code != 0
        assert "git clone failed" in result.output

    def test_auto_suggests_name_from_url(self, runner, config_dir, tmp_path, monkeypatch):
        """Test that project name is auto-suggested from repo URL."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        with patch("botfarm.cli.subprocess.run", side_effect=_make_mock_run(tmp_path)):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                # Accept the default name by pressing enter
                input="git@github.com:user/cool-repo.git\n\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        config = yaml.safe_load(config_path.read_text())
        project_names = [p["name"] for p in config["projects"]]
        assert "cool-repo" in project_names

    def test_multiple_slots_creates_worktrees(self, runner, config_dir, tmp_path, monkeypatch):
        """Test that multiple worktrees are created for multiple slots."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        worktree_cmds = []
        original_mock = _make_mock_run(tmp_path)

        def tracking_run(cmd, **kwargs):
            if isinstance(cmd, list) and "worktree" in cmd:
                worktree_cmds.append(cmd)
            return original_mock(cmd, **kwargs)

        with patch("botfarm.cli.subprocess.run", side_effect=tracking_run):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/multi.git\nmulti\nSMA\n\n3\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert len(worktree_cmds) == 3

        config = yaml.safe_load(config_path.read_text())
        added = next(p for p in config["projects"] if p["name"] == "multi")
        assert added["slots"] == [1, 2, 3]

    def test_linear_api_failure_falls_back_to_manual(
        self, runner, config_dir, tmp_path, monkeypatch
    ):
        """Test that Linear API failure falls back to manual input."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.setenv("LINEAR_API_KEY", "test-key")

        mock_client = MagicMock()
        mock_client.list_teams.side_effect = LinearAPIError("connection failed")

        with patch("botfarm.cli.subprocess.run", side_effect=_make_mock_run(tmp_path)), \
             patch("botfarm.cli.LinearClient", return_value=mock_client):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/my-app.git\nmy-app\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert "added successfully" in result.output

        config = yaml.safe_load(config_path.read_text())
        added = next(p for p in config["projects"] if p["name"] == "my-app")
        assert added["linear_team"] == "SMA"

    def test_readiness_checks_run_after_clone(
        self, runner, config_dir, tmp_path, monkeypatch
    ):
        """Test that readiness checks run after cloning."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        with patch("botfarm.cli.subprocess.run", side_effect=_make_mock_run(tmp_path)), \
             patch("botfarm.cli._run_readiness_checks", return_value=[
                 ("warning", "No CLAUDE.md"),
             ]) as mock_readiness:
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/my-app.git\nmy-app\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert mock_readiness.called
        assert "WARN" in result.output

    def test_suggests_init_claude_md(self, runner, config_dir, tmp_path, monkeypatch):
        """Test that init-claude-md is suggested when CLAUDE.md is missing."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        with patch("botfarm.cli.subprocess.run", side_effect=_make_mock_run(tmp_path)):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/my-app.git\nmy-app\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert "init-claude-md" in result.output
