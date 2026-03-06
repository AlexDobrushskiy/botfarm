"""Tests for botfarm add-project CLI command."""

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from botfarm.cli import (
    _append_project_to_config,
    _run_readiness_checks,
    _validate_project_dir,
    _validate_worktree_parent,
    add_project,
    main,
)


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


# ---------------------------------------------------------------------------
# _validate_project_dir
# ---------------------------------------------------------------------------


class TestValidateProjectDir:
    def test_nonexistent_dir(self, tmp_path):
        errors = _validate_project_dir(str(tmp_path / "nonexistent"))
        assert len(errors) == 1
        assert "does not exist" in errors[0]

    def test_not_a_git_repo(self, tmp_path):
        d = tmp_path / "plain"
        d.mkdir()
        errors = _validate_project_dir(str(d))
        assert len(errors) == 1
        assert "Not a git repository" in errors[0]

    def test_git_repo_remote_unreachable(self, tmp_path):
        # Create a repo with a .git dir and mock ls-remote to fail
        repo = tmp_path / "unreachable-repo"
        repo.mkdir()
        (repo / ".git").mkdir()
        with patch("botfarm.cli.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=128, stdout="",
                stderr="fatal: could not read from remote repository",
            )
            errors = _validate_project_dir(str(repo))
        assert len(errors) == 1
        assert "not reachable" in errors[0]

    def test_valid_git_repo(self, tmp_path):
        """A valid repo with reachable remote returns no errors."""
        with patch("botfarm.cli.subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0, stdout="", stderr=""
            )
            repo = tmp_path / "good-repo"
            repo.mkdir()
            (repo / ".git").mkdir()
            errors = _validate_project_dir(str(repo))
            assert errors == []


# ---------------------------------------------------------------------------
# _validate_worktree_parent
# ---------------------------------------------------------------------------


class TestValidateWorktreeParent:
    def test_parent_exists_and_writable(self, tmp_path):
        repo = tmp_path / "my-repo"
        repo.mkdir()
        errors = _validate_worktree_parent(str(repo))
        assert errors == []

    def test_parent_not_writable(self, tmp_path):
        repo = tmp_path / "subdir" / "my-repo"
        repo.mkdir(parents=True)
        parent = repo.parent
        parent.chmod(0o555)
        try:
            errors = _validate_worktree_parent(str(repo))
            assert len(errors) == 1
            assert "not writable" in errors[0]
        finally:
            parent.chmod(0o755)


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

    def test_duplicate_project_name(self, runner, config_dir):
        _, config_path = config_dir
        # Input: base_dir, name (existing-proj), linear_team, etc.
        result = runner.invoke(
            main,
            ["add-project", "--config", str(config_path)],
            input="/tmp/somedir\nexisting-proj\n",
        )
        assert result.exit_code != 0
        assert "already exists" in result.output

    def test_successful_add(self, runner, config_dir, git_repo):
        _, config_path = config_dir

        # Mock git validation to pass
        with patch("botfarm.cli._validate_project_dir", return_value=[]), \
             patch("botfarm.cli._validate_worktree_parent", return_value=[]), \
             patch("botfarm.cli._run_readiness_checks", return_value=[
                 ("ok", "CLAUDE.md found"),
             ]):
            # Input: base_dir, name, linear_team, worktree_prefix, slots, linear_project
            input_text = (
                f"{git_repo}\n"  # base_dir
                "my-repo\n"       # name (accept default)
                "SMA\n"          # linear_team
                "\n"             # worktree_prefix (accept default)
                "1,2\n"          # slots
                "\n"             # linear_project (skip)
            )
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input=input_text,
            )

        assert result.exit_code == 0, f"Output: {result.output}"
        assert "added to" in result.output

        # Verify config was updated
        data = yaml.safe_load(config_path.read_text())
        assert len(data["projects"]) == 2
        new_proj = data["projects"][1]
        assert new_proj["name"] == "my-repo"
        assert new_proj["linear_team"] == "SMA"
        assert new_proj["slots"] == [1, 2]

    def test_invalid_slots_format(self, runner, config_dir):
        _, config_path = config_dir
        input_text = (
            "/tmp/somedir\n"
            "new-proj\n"
            "SMA\n"
            "\n"
            "a,b,c\n"  # invalid slots
            "\n"
        )
        result = runner.invoke(
            main,
            ["add-project", "--config", str(config_path)],
            input=input_text,
        )
        assert result.exit_code != 0
        assert "Invalid slot IDs" in result.output

    def test_duplicate_slot_ids(self, runner, config_dir):
        _, config_path = config_dir
        input_text = (
            "/tmp/somedir\n"
            "new-proj\n"
            "SMA\n"
            "\n"
            "1,1,2\n"  # duplicate slot 1
            "\n"
        )
        result = runner.invoke(
            main,
            ["add-project", "--config", str(config_path)],
            input=input_text,
        )
        assert result.exit_code != 0
        assert "unique" in result.output

    def test_validation_fails_user_aborts(self, runner, config_dir):
        _, config_path = config_dir
        with patch("botfarm.cli._validate_project_dir",
                    return_value=["Not a git repo"]), \
             patch("botfarm.cli._validate_worktree_parent", return_value=[]):
            input_text = (
                "/tmp/somedir\n"
                "new-proj\n"
                "SMA\n"
                "\n"
                "1\n"
                "\n"
                "n\n"  # abort on validation failure
            )
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input=input_text,
            )

        assert result.exit_code == 0
        assert "Aborted" in result.output
        # Config should not be modified
        data = yaml.safe_load(config_path.read_text())
        assert len(data["projects"]) == 1

    def test_validation_fails_user_continues(self, runner, config_dir):
        _, config_path = config_dir
        with patch("botfarm.cli._validate_project_dir",
                    return_value=["Not a git repo"]), \
             patch("botfarm.cli._validate_worktree_parent", return_value=[]), \
             patch("botfarm.cli._run_readiness_checks", return_value=[]):
            input_text = (
                "/tmp/somedir\n"
                "new-proj\n"
                "SMA\n"
                "\n"
                "1\n"
                "\n"
                "y\n"  # continue despite validation failure
            )
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input=input_text,
            )

        assert result.exit_code == 0
        assert "added to" in result.output
        data = yaml.safe_load(config_path.read_text())
        assert len(data["projects"]) == 2

    def test_shows_warnings(self, runner, config_dir):
        _, config_path = config_dir
        with patch("botfarm.cli._validate_project_dir", return_value=[]), \
             patch("botfarm.cli._validate_worktree_parent", return_value=[]), \
             patch("botfarm.cli._run_readiness_checks", return_value=[
                 ("warning", "No CLAUDE.md"),
             ]):
            input_text = (
                "/tmp/somedir\n"
                "new-proj\n"
                "SMA\n"
                "\n"
                "1\n"
                "\n"
            )
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input=input_text,
            )

        assert result.exit_code == 0
        assert "WARN" in result.output
        assert "1 warning" in result.output

    def test_suggests_init_claude_md(self, runner, config_dir, tmp_path):
        _, config_path = config_dir
        base = tmp_path / "no-claude-md"
        base.mkdir()
        with patch("botfarm.cli._validate_project_dir", return_value=[]), \
             patch("botfarm.cli._validate_worktree_parent", return_value=[]), \
             patch("botfarm.cli._run_readiness_checks", return_value=[
                 ("warning", "No CLAUDE.md"),
             ]):
            input_text = (
                f"{base}\n"
                "no-claude-md\n"
                "SMA\n"
                "\n"
                "1\n"
                "\n"
            )
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input=input_text,
            )

        assert result.exit_code == 0
        assert "init-claude-md" in result.output
