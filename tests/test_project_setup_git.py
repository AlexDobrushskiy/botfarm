"""Tests for git repo initialization and worktree setup in project_setup.py."""

import os
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml

from botfarm.project_setup import (
    is_git_repo,
    init_repo,
    create_github_repo,
    setup_project,
    setup_project_git,
    ProjectSetupError,
)


# Repo-binding GIT_* vars that must be stripped when targeting a different repo.
_GIT_REPO_VARS = {
    "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE",
    "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES",
    "GIT_CEILING_DIRECTORIES",
}


def _clean_git_env():
    """Return env dict with repo-binding GIT_* vars removed."""
    return {k: v for k, v in os.environ.items() if k not in _GIT_REPO_VARS}


def _git_init(path):
    """Run git init with a clean env so pre-commit hook GIT_* vars don't interfere."""
    subprocess.run(
        ["git", "init", str(path)],
        capture_output=True, env=_clean_git_env(),
    )


class TestIsGitRepo:
    def test_returns_true_for_git_repo(self, tmp_path):
        _git_init(tmp_path)
        assert is_git_repo(tmp_path) is True

    def test_returns_false_for_plain_dir(self, tmp_path):
        assert is_git_repo(tmp_path) is False

    def test_returns_false_for_nonexistent(self, tmp_path):
        assert is_git_repo(tmp_path / "nope") is False


class TestInitRepo:
    def test_creates_repo_with_initial_commit(self, tmp_path):
        repo = tmp_path / "myrepo"
        init_repo(repo, "myrepo")

        assert repo.is_dir()
        assert (repo / ".gitignore").exists()
        assert (repo / "README.md").exists()
        assert (repo / "CLAUDE.md").exists()
        assert is_git_repo(repo)

        # Verify initial commit exists
        result = subprocess.run(
            ["git", "-C", str(repo), "log", "--oneline"],
            capture_output=True, text=True, env=_clean_git_env(),
        )
        assert result.returncode == 0
        assert "Initial commit" in result.stdout

    def test_readme_contains_project_name(self, tmp_path):
        repo = tmp_path / "cool-project"
        init_repo(repo, "cool-project")
        readme = (repo / "README.md").read_text()
        assert "cool-project" in readme

    def test_raises_on_no_git(self, tmp_path):
        with patch("botfarm.project_setup.subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(ProjectSetupError, match="git is not installed"):
                init_repo(tmp_path / "repo", "test")

    def test_raises_on_unwritable_parent(self, tmp_path):
        bad_path = tmp_path / "readonly" / "repo"
        (tmp_path / "readonly").mkdir()
        (tmp_path / "readonly").chmod(0o444)
        try:
            with pytest.raises(ProjectSetupError, match="Cannot create"):
                init_repo(bad_path, "test")
        finally:
            (tmp_path / "readonly").chmod(0o755)


class TestCreateGithubRepo:
    def test_calls_gh_cli(self, tmp_path):
        # Create a real git repo for the test
        subprocess.run(["git", "init", str(tmp_path)], capture_output=True)

        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "https://github.com/user/my-proj\n"

        with patch("botfarm.project_setup.subprocess.run", return_value=mock_result) as mock_run:
            url = create_github_repo(tmp_path, "my-proj")

        assert url == "https://github.com/user/my-proj"
        call_args = mock_run.call_args[0][0]
        assert "gh" in call_args
        assert "repo" in call_args
        assert "create" in call_args
        assert "--private" in call_args
        assert "--push" in call_args

    def test_public_flag(self, tmp_path):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "https://github.com/user/my-proj\n"

        with patch("botfarm.project_setup.subprocess.run", return_value=mock_result) as mock_run:
            create_github_repo(tmp_path, "my-proj", private=False)

        call_args = mock_run.call_args[0][0]
        assert "--public" in call_args
        assert "--private" not in call_args

    def test_raises_on_no_gh(self, tmp_path):
        with patch("botfarm.project_setup.subprocess.run", side_effect=FileNotFoundError):
            with pytest.raises(ProjectSetupError, match="GitHub CLI"):
                create_github_repo(tmp_path, "test")

    def test_raises_on_failure(self, tmp_path):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stderr = "auth required"

        with patch("botfarm.project_setup.subprocess.run", return_value=mock_result):
            with pytest.raises(ProjectSetupError, match="gh repo create failed"):
                create_github_repo(tmp_path, "test")


class TestSetupProjectInitPath:
    """Test setup_project with empty repo_url (init path)."""

    def _write_config(self, path, projects=None):
        data = {"projects": projects or []}
        path.write_text(yaml.dump(data))

    def test_init_creates_repo_and_worktrees(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        projects_dir = tmp_path / "projects" / "test-proj"
        self._write_config(config_path)

        result = setup_project(
            repo_url="",
            name="test-proj",
            team="ENG",
            tracker_project="",
            slots=[1, 2],
            config_path=config_path,
            projects_dir=projects_dir,
        )

        repo_dir = projects_dir / "repo"
        assert repo_dir.is_dir()
        assert is_git_repo(repo_dir)
        assert (repo_dir / ".gitignore").exists()
        assert (repo_dir / "CLAUDE.md").exists()

        # Worktrees created
        assert (projects_dir / "test-proj-slot-1").is_dir()
        assert (projects_dir / "test-proj-slot-2").is_dir()

        # Config updated
        data = yaml.safe_load(config_path.read_text())
        assert any(p["name"] == "test-proj" for p in data["projects"])

        # Result dict
        assert result["name"] == "test-proj"
        assert result["slots"] == [1, 2]

    def test_reuses_existing_git_repo(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        projects_dir = tmp_path / "projects" / "existing"
        self._write_config(config_path)

        # Pre-create the repo (use clean env to avoid GIT_* interference)
        repo_dir = projects_dir / "repo"
        repo_dir.mkdir(parents=True)
        env = _clean_git_env()
        subprocess.run(
            ["git", "init", str(repo_dir)], capture_output=True, env=env,
        )
        # Need at least one commit for worktree creation
        (repo_dir / "dummy").write_text("x")
        subprocess.run(
            ["git", "-C", str(repo_dir), "add", "."],
            capture_output=True, env=env,
        )
        subprocess.run(
            ["git", "-C", str(repo_dir), "commit", "-m", "seed"],
            capture_output=True, env=env,
        )

        messages = []
        setup_project(
            repo_url="",
            name="existing",
            team="ENG",
            tracker_project="",
            slots=[1],
            config_path=config_path,
            projects_dir=projects_dir,
            progress_callback=messages.append,
        )

        assert any("existing git repo" in m.lower() for m in messages)
        # No .gitignore added (repo reused as-is)
        assert not (repo_dir / ".gitignore").exists()

    def test_error_when_dir_exists_not_git(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        projects_dir = tmp_path / "projects" / "bad"
        self._write_config(config_path)

        (projects_dir / "repo").mkdir(parents=True)

        with pytest.raises(ProjectSetupError, match="not a git repo"):
            setup_project(
                repo_url="",
                name="bad",
                team="ENG",
                tracker_project="",
                slots=[1],
                config_path=config_path,
                projects_dir=projects_dir,
            )

    def test_create_github_flag(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        projects_dir = tmp_path / "projects" / "gh-proj"
        self._write_config(config_path)

        with patch("botfarm.project_setup.create_github_repo") as mock_gh:
            mock_gh.return_value = "https://github.com/user/gh-proj"
            setup_project(
                repo_url="",
                name="gh-proj",
                team="ENG",
                tracker_project="",
                slots=[1],
                config_path=config_path,
                projects_dir=projects_dir,
                create_github=True,
            )

        mock_gh.assert_called_once()
        assert mock_gh.call_args[0][1] == "gh-proj"

    def test_create_github_ignored_with_repo_url(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        projects_dir = tmp_path / "projects" / "cloned"
        self._write_config(config_path)

        with patch("botfarm.project_setup.clone_repo") as mock_clone, \
             patch("botfarm.project_setup.create_github_repo") as mock_gh, \
             patch("botfarm.project_setup.create_worktree"):
            setup_project(
                repo_url="git@github.com:user/repo.git",
                name="cloned",
                team="ENG",
                tracker_project="",
                slots=[1],
                config_path=config_path,
                projects_dir=projects_dir,
                create_github=True,
            )

        mock_gh.assert_not_called()

    def test_parent_not_writable(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        self._write_config(config_path)

        readonly = tmp_path / "readonly"
        readonly.mkdir()
        readonly.chmod(0o444)
        try:
            with pytest.raises(ProjectSetupError, match="not writable"):
                setup_project(
                    repo_url="",
                    name="test",
                    team="ENG",
                    tracker_project="",
                    slots=[1],
                    config_path=config_path,
                    projects_dir=readonly / "test",
                )
        finally:
            readonly.chmod(0o755)

    def test_cleanup_on_failure_init_path(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        projects_dir = tmp_path / "projects" / "fail"
        self._write_config(config_path)

        with patch("botfarm.project_setup.create_worktree", side_effect=ProjectSetupError("boom")):
            with pytest.raises(ProjectSetupError, match="boom"):
                setup_project(
                    repo_url="",
                    name="fail",
                    team="ENG",
                    tracker_project="",
                    slots=[1],
                    config_path=config_path,
                    projects_dir=projects_dir,
                )

        # Should be cleaned up
        assert not (projects_dir / "repo").exists()


class TestSetupProjectGit:
    """Test the standalone setup_project_git function."""

    def _write_config_with_project(self, config_path, name, base_dir, slots, wt_prefix):
        data = {
            "projects": [{
                "name": name,
                "team": "ENG",
                "base_dir": str(base_dir),
                "worktree_prefix": str(wt_prefix),
                "slots": slots,
            }]
        }
        config_path.write_text(yaml.dump(data))

    def test_inits_missing_repo(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        repo_dir = tmp_path / "projects" / "new" / "repo"
        wt_prefix = tmp_path / "projects" / "new" / "new-slot-"
        self._write_config_with_project(
            config_path, "new", repo_dir, [1, 2], wt_prefix,
        )

        setup_project_git(name="new", config_path=config_path)

        assert repo_dir.is_dir()
        assert is_git_repo(repo_dir)
        assert (tmp_path / "projects" / "new" / "new-slot-1").is_dir()
        assert (tmp_path / "projects" / "new" / "new-slot-2").is_dir()

    def test_skips_existing_worktrees(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        repo_dir = tmp_path / "projects" / "ex" / "repo"
        wt_prefix = tmp_path / "projects" / "ex" / "ex-slot-"
        self._write_config_with_project(
            config_path, "ex", repo_dir, [1, 2], wt_prefix,
        )

        # Pre-create repo and slot 1 worktree
        init_repo(repo_dir, "ex")
        from botfarm.project_setup import create_worktree
        wt1 = tmp_path / "projects" / "ex" / "ex-slot-1"
        create_worktree(repo_dir, wt1, "slot-1-placeholder")

        messages = []
        setup_project_git(
            name="ex", config_path=config_path,
            progress_callback=messages.append,
        )

        # Slot 1 skipped, slot 2 created
        assert any("already exists" in m for m in messages)
        assert (tmp_path / "projects" / "ex" / "ex-slot-2").is_dir()

    def test_reuses_existing_repo(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        repo_dir = tmp_path / "projects" / "exist" / "repo"
        wt_prefix = tmp_path / "projects" / "exist" / "exist-slot-"
        self._write_config_with_project(
            config_path, "exist", repo_dir, [1], wt_prefix,
        )

        init_repo(repo_dir, "exist")

        messages = []
        setup_project_git(
            name="exist", config_path=config_path,
            progress_callback=messages.append,
        )

        assert any("already exists" in m for m in messages)

    def test_raises_for_unknown_project(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump({"projects": []}))

        with pytest.raises(ProjectSetupError, match="not found"):
            setup_project_git(name="ghost", config_path=config_path)

    def test_raises_for_non_git_dir(self, tmp_path):
        config_path = tmp_path / "config.yaml"
        repo_dir = tmp_path / "notgit"
        repo_dir.mkdir()
        wt_prefix = tmp_path / "x-slot-"
        self._write_config_with_project(
            config_path, "notgit", repo_dir, [1], wt_prefix,
        )

        with pytest.raises(ProjectSetupError, match="not a git repo"):
            setup_project_git(name="notgit", config_path=config_path)
