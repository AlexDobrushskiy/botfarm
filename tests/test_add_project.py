"""Tests for botfarm add-project CLI command."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner

from botfarm.cli import main
from botfarm.bugtracker import BugtrackerError as LinearAPIError
from botfarm.project_setup import (
    append_project_to_config,
    detect_project_indent,
    extract_repo_name,
    find_projects_insert_point,
    format_project_entry,
    is_placeholder_project,
    remove_project_entry_text,
    run_readiness_checks,
    yaml_scalar,
)


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def config_dir(tmp_path):
    """Create a tmp config dir with a minimal valid config.yaml."""
    base_dir = tmp_path / "existing-proj-repo"
    base_dir.mkdir()
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "projects:\n"
        "  - name: existing-proj\n"
        "    linear_team: SMA\n"
        f"    base_dir: {base_dir}\n"
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


def _make_mock_run():
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
        assert extract_repo_name("git@github.com:user/my-app.git") == "my-app"

    def test_https_url(self):
        assert extract_repo_name("https://github.com/user/my-app.git") == "my-app"

    def test_no_git_suffix(self):
        assert extract_repo_name("https://github.com/user/my-app") == "my-app"

    def test_trailing_slash(self):
        assert extract_repo_name("https://github.com/user/my-app.git/") == "my-app"

    def test_ssh_no_git_suffix(self):
        assert extract_repo_name("git@github.com:org/repo-name") == "repo-name"

    def test_plain_name(self):
        assert extract_repo_name("my-repo") == "my-repo"


# ---------------------------------------------------------------------------
# _is_placeholder_project
# ---------------------------------------------------------------------------


class TestIsPlaceholderProject:
    def test_known_name_nonexistent_base_dir(self):
        entry = {"name": "my-project", "base_dir": "/tmp/nonexistent-dir-xyz-9999"}
        assert is_placeholder_project(entry) is True

    def test_known_name_existing_base_dir(self, tmp_path):
        """Known init name but directory exists — not a placeholder."""
        entry = {"name": "my-project", "base_dir": str(tmp_path)}
        assert is_placeholder_project(entry) is False

    def test_known_name_empty_base_dir(self):
        entry = {"name": "my-project", "base_dir": ""}
        assert is_placeholder_project(entry) is True

    def test_known_name_missing_base_dir(self):
        entry = {"name": "project"}
        assert is_placeholder_project(entry) is True

    def test_unknown_name_not_placeholder(self):
        """Non-default name is never a placeholder, even if base_dir is missing."""
        entry = {"name": "production-api", "base_dir": "/tmp/nonexistent-dir-xyz-9999"}
        assert is_placeholder_project(entry) is False

    def test_tilde_expanded(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        (tmp_path / "my-project").mkdir()
        entry = {"name": "my-project", "base_dir": "~/my-project"}
        assert is_placeholder_project(entry) is False

    def test_deleted_repo_under_botfarm_dir_not_placeholder(self, tmp_path, monkeypatch):
        """Known init name under ~/.botfarm/projects/ is not a placeholder."""
        botfarm_dir = tmp_path / ".botfarm"
        botfarm_dir.mkdir()
        monkeypatch.setattr("botfarm.project_setup.DEFAULT_CONFIG_DIR", botfarm_dir)
        entry = {
            "name": "my-project",
            "base_dir": str(botfarm_dir / "projects" / "my-project" / "repo"),
        }
        assert is_placeholder_project(entry) is False


# ---------------------------------------------------------------------------
# _remove_project_entry_text
# ---------------------------------------------------------------------------


class TestRemoveProjectEntryText:
    def test_removes_single_entry(self):
        raw = (
            "projects:\n"
            "  - name: my-project\n"
            "    linear_team: TEAM\n"
            "    base_dir: ~/my-project\n"
            "    worktree_prefix: my-project-slot-\n"
            "    slots: [1, 2]\n"
            "\n"
            "linear:\n"
            "  api_key: test\n"
        )
        result = remove_project_entry_text(raw, "my-project")
        data = yaml.safe_load(result)
        # projects key should remain but be empty/null
        assert "linear" in data
        assert data.get("projects") is None or data.get("projects") == []

    def test_removes_first_of_two_entries(self):
        raw = (
            "projects:\n"
            "  - name: placeholder\n"
            "    linear_team: TEAM\n"
            "    slots: [1]\n"
            "  - name: real-project\n"
            "    linear_team: SMA\n"
            "    slots: [1, 2]\n"
        )
        result = remove_project_entry_text(raw, "placeholder")
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "real-project"

    def test_removes_second_of_two_entries(self):
        raw = (
            "projects:\n"
            "  - name: real-project\n"
            "    linear_team: SMA\n"
            "    slots: [1]\n"
            "  - name: placeholder\n"
            "    linear_team: TEAM\n"
            "    slots: [1, 2]\n"
        )
        result = remove_project_entry_text(raw, "placeholder")
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "real-project"

    def test_preserves_comments(self):
        raw = (
            "# Header comment\n"
            "projects:\n"
            "  - name: my-project\n"
            "    linear_team: TEAM  # inline comment\n"
            "    slots: [1]\n"
            "\n"
            "# Section comment\n"
            "linear:\n"
            "  api_key: test\n"
        )
        result = remove_project_entry_text(raw, "my-project")
        assert "# Header comment" in result
        assert "# Section comment" in result

    def test_no_match_returns_unchanged(self):
        raw = "projects:\n  - name: real\n    slots: [1]\n"
        assert remove_project_entry_text(raw, "nonexistent") == raw

    def test_handles_quoted_name(self):
        raw = (
            "projects:\n"
            '  - name: "my-project"\n'
            "    slots: [1]\n"
        )
        result = remove_project_entry_text(raw, "my-project")
        data = yaml.safe_load(result)
        assert data.get("projects") is None or data.get("projects") == []

    def test_handles_inline_comment_on_name(self):
        raw = (
            "projects:\n"
            "  - name: my-project  # placeholder\n"
            "    slots: [1]\n"
        )
        result = remove_project_entry_text(raw, "my-project")
        data = yaml.safe_load(result)
        assert data.get("projects") is None or data.get("projects") == []

    def test_removes_entry_zero_indent(self):
        raw = (
            "projects:\n"
            "- name: my-project\n"
            "  linear_team: TEAM\n"
            "  base_dir: ~/my-project\n"
            "  slots: [1, 2]\n"
            "\n"
            "linear:\n"
            "  api_key: test\n"
        )
        result = remove_project_entry_text(raw, "my-project")
        data = yaml.safe_load(result)
        assert "linear" in data
        assert data.get("projects") is None or data.get("projects") == []

    def test_removes_first_of_two_zero_indent(self):
        raw = (
            "projects:\n"
            "- name: placeholder\n"
            "  linear_team: TEAM\n"
            "  slots: [1]\n"
            "- name: real-project\n"
            "  linear_team: SMA\n"
            "  slots: [1, 2]\n"
        )
        result = remove_project_entry_text(raw, "placeholder")
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "real-project"


# ---------------------------------------------------------------------------
# _detect_project_indent
# ---------------------------------------------------------------------------


class TestDetectProjectIndent:
    def test_two_space_indent(self):
        raw = "projects:\n  - name: foo\n    slots: [1]\n"
        assert detect_project_indent(raw) == 2

    def test_zero_indent(self):
        raw = "projects:\n- name: foo\n  slots: [1]\n"
        assert detect_project_indent(raw) == 0

    def test_no_entries_defaults_to_two(self):
        raw = "projects: []\nlinear:\n  api_key: test\n"
        assert detect_project_indent(raw) == 2

    def test_no_projects_key_defaults_to_two(self):
        raw = "linear:\n  api_key: test\n"
        assert detect_project_indent(raw) == 2

    def test_ignores_nested_name_keys(self):
        raw = (
            "linear:\n"
            "  - name: not-a-project\n"
            "projects:\n"
            "- name: real\n"
            "  slots: [1]\n"
        )
        assert detect_project_indent(raw) == 0


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
        results = run_readiness_checks(project)
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
        results = run_readiness_checks(project)
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
        results = run_readiness_checks(project)
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
        append_project_to_config(config_path, project)

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
        append_project_to_config(config_path, project)

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
        append_project_to_config(config_path, project)

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
            append_project_to_config(config_path, project)

    def test_preserves_yaml_comments(self, tmp_path):
        """Verify that add-project preserves all YAML comments in config."""
        config_path = tmp_path / "config.yaml"
        config_text = (
            "# Botfarm configuration\n"
            "projects:\n"
            "  - name: existing-proj\n"
            "    linear_team: SMA  # Smart AI Coach\n"
            "    base_dir: /tmp/existing\n"
            "    worktree_prefix: existing-slot-\n"
            "    slots: [1]\n"
            "\n"
            "linear:\n"
            "  api_key: ${LINEAR_API_KEY}\n"
            "\n"
            "# identities:\n"
            "#   coder:\n"
            "#     github_token: ${CODER_GITHUB_TOKEN}\n"
            "\n"
            "# notifications:\n"
            "#   webhook_url: https://hooks.slack.com/services/...\n"
        )
        config_path.write_text(config_text)
        project = {
            "name": "new-proj",
            "linear_team": "TEAM",
            "base_dir": "/tmp/new",
            "worktree_prefix": "new-slot-",
            "slots": [1, 2],
            "linear_project": "",
        }
        append_project_to_config(config_path, project)

        result = config_path.read_text()
        # All comments must be preserved
        assert "# Botfarm configuration" in result
        assert "# Smart AI Coach" in result
        assert "# identities:" in result
        assert "#   coder:" in result
        assert "# notifications:" in result
        assert "#   webhook_url:" in result
        # Data must still be valid YAML
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 2
        assert data["projects"][1]["name"] == "new-proj"
        assert data["projects"][1]["slots"] == [1, 2]

    def test_preserves_full_init_template(self, tmp_path):
        """Verify comment preservation with a realistic init-generated config."""
        config_path = tmp_path / "config.yaml"
        # Simulate the config generated by `botfarm init`
        config_text = (
            "# Botfarm configuration\n"
            "# See documentation for full reference.\n"
            "\n"
            "projects:\n"
            "  - name: my-project\n"
            "    linear_team: SMA  # Smart AI Coach\n"
            "    base_dir: ~/my-project\n"
            "    worktree_prefix: my-project-slot-\n"
            "    slots: [1, 2]\n"
            "\n"
            "linear:\n"
            "  api_key: ${LINEAR_API_KEY}\n"
            "  workspace: my-workspace\n"
            "\n"
            "# Separate coder/reviewer GitHub identities.\n"
            "# identities:\n"
            "#   coder:\n"
            "#     github_token: ${CODER_GITHUB_TOKEN}\n"
            "\n"
            "# Periodic refactoring analysis\n"
            "# refactoring_analysis:\n"
            "#   enabled: true\n"
            "#   cadence_days: 14\n"
            "\n"
            "start_paused: true\n"
            "\n"
            "# notifications:\n"
            "#   webhook_url: https://hooks.slack.com/services/...\n"
            "#   webhook_format: slack\n"
        )
        config_path.write_text(config_text)
        project = {
            "name": "another-project",
            "linear_team": "ENG",
            "base_dir": "~/another-project",
            "worktree_prefix": "another-project-slot-",
            "slots": [1],
            "linear_project": "Engineering",
        }
        append_project_to_config(config_path, project)

        result = config_path.read_text()
        # Every commented-out section must survive
        assert "# Separate coder/reviewer" in result
        assert "# identities:" in result
        assert "# Periodic refactoring analysis" in result
        assert "# refactoring_analysis:" in result
        assert "# notifications:" in result
        assert "#   webhook_format: slack" in result
        # Data round-trips correctly
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 2
        added = data["projects"][1]
        assert added["name"] == "another-project"
        assert added["linear_team"] == "ENG"
        assert added["linear_project"] == "Engineering"

    def test_replaces_placeholder_entry(self, tmp_path):
        """Verify that replace_names removes placeholder and adds new entry."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            "# Botfarm configuration\n"
            "projects:\n"
            "  - name: my-project\n"
            "    linear_team: TEAM\n"
            "    base_dir: ~/my-project\n"
            "    worktree_prefix: my-project-slot-\n"
            "    slots: [1, 2]\n"
            "\n"
            "linear:\n"
            "  api_key: test\n"
        )
        project = {
            "name": "real-project",
            "linear_team": "SMA",
            "base_dir": "/tmp/real",
            "worktree_prefix": "real-slot-",
            "slots": [1],
            "linear_project": "Bot farm",
        }
        append_project_to_config(
            config_path, project, replace_names=frozenset({"my-project"}),
        )

        result = config_path.read_text()
        assert "# Botfarm configuration" in result
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "real-project"
        assert data["projects"][0]["linear_project"] == "Bot farm"
        # Placeholder must be gone
        names = [p["name"] for p in data["projects"]]
        assert "my-project" not in names

    def test_replaces_placeholder_preserves_other_projects(self, tmp_path):
        """Verify replace_names only removes specified entries."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            "projects:\n"
            "  - name: my-project\n"
            "    linear_team: TEAM\n"
            "    base_dir: ~/my-project\n"
            "    worktree_prefix: my-project-slot-\n"
            "    slots: [1]\n"
            "  - name: existing\n"
            "    linear_team: SMA\n"
            "    base_dir: /tmp/existing\n"
            "    worktree_prefix: existing-slot-\n"
            "    slots: [1]\n"
            "\n"
            "linear:\n"
            "  api_key: test\n"
        )
        project = {
            "name": "new-proj",
            "linear_team": "ENG",
            "base_dir": "/tmp/new",
            "worktree_prefix": "new-slot-",
            "slots": [1, 2],
            "linear_project": "",
        }
        append_project_to_config(
            config_path, project, replace_names=frozenset({"my-project"}),
        )

        data = yaml.safe_load(config_path.read_text())
        names = [p["name"] for p in data["projects"]]
        assert "my-project" not in names
        assert "existing" in names
        assert "new-proj" in names
        assert len(data["projects"]) == 2

    def test_appends_project_zero_indent(self, tmp_path):
        """Appending to a 0-indent config matches the existing style."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            "projects:\n"
            "- name: existing-proj\n"
            "  linear_team: SMA\n"
            "  base_dir: /tmp/existing\n"
            "  worktree_prefix: existing-slot-\n"
            "  slots: [1]\n"
            "\n"
            "linear:\n"
            "  api_key: test-key\n"
        )
        project = {
            "name": "new-proj",
            "linear_team": "TEAM",
            "base_dir": "/tmp/new",
            "worktree_prefix": "new-slot-",
            "slots": [1, 2],
            "linear_project": "",
        }
        append_project_to_config(config_path, project)

        result = config_path.read_text()
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 2
        assert data["projects"][1]["name"] == "new-proj"
        # New entry should use 0-indent to match existing style
        assert "\n- name: new-proj\n" in result

    def test_replaces_placeholder_zero_indent(self, tmp_path):
        """Replacing a placeholder in 0-indent config preserves style."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            "projects:\n"
            "- name: my-project\n"
            "  linear_team: TEAM\n"
            "  base_dir: ~/my-project\n"
            "  worktree_prefix: my-project-slot-\n"
            "  slots: [1, 2]\n"
            "\n"
            "linear:\n"
            "  api_key: test\n"
        )
        project = {
            "name": "real-project",
            "linear_team": "SMA",
            "base_dir": "/tmp/real",
            "worktree_prefix": "real-slot-",
            "slots": [1],
            "linear_project": "Bot farm",
        }
        append_project_to_config(
            config_path, project, replace_names=frozenset({"my-project"}),
        )

        result = config_path.read_text()
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "real-project"
        assert "my-project" not in result
        # Should use 0-indent style
        assert "\n- name: real-project\n" in result

    def test_handles_empty_projects_list(self, tmp_path):
        """Verify handling when projects key exists but list is empty."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            "# Config\n"
            "projects: []\n"
            "\n"
            "# Keep this comment\n"
            "linear:\n"
            "  api_key: test\n"
        )
        project = {
            "name": "proj",
            "linear_team": "SMA",
            "base_dir": "/tmp/proj",
            "worktree_prefix": "proj-slot-",
            "slots": [1],
            "linear_project": "",
        }
        append_project_to_config(config_path, project)

        result = config_path.read_text()
        assert "# Config" in result
        assert "# Keep this comment" in result
        data = yaml.safe_load(result)
        assert len(data["projects"]) == 1
        assert data["projects"][0]["name"] == "proj"


# ---------------------------------------------------------------------------
# _yaml_scalar
# ---------------------------------------------------------------------------


class TestYamlScalar:
    def test_simple_string(self):
        assert yaml_scalar("hello") == "hello"

    def test_string_with_colon_space(self):
        # "key: value" would be parsed as a mapping
        result = yaml_scalar("key: value")
        assert result.startswith('"')
        assert yaml.safe_load(result) == "key: value"

    def test_boolean_like_string(self):
        result = yaml_scalar("true")
        assert result.startswith('"')
        assert yaml.safe_load(result) == "true"

    def test_empty_string(self):
        assert yaml_scalar("") == '""'

    def test_list_of_ints(self):
        assert yaml_scalar([1, 2, 3]) == "[1, 2, 3]"

    def test_path_with_tilde(self):
        assert yaml_scalar("~/my-project") == "~/my-project"

    def test_string_with_quotes(self):
        result = yaml_scalar('say "hello"')
        assert yaml.safe_load(result) == 'say "hello"'


# ---------------------------------------------------------------------------
# _format_project_entry
# ---------------------------------------------------------------------------


class TestFormatProjectEntry:
    def test_basic_entry(self):
        project = {
            "name": "my-app",
            "linear_team": "SMA",
            "base_dir": "~/my-app",
            "worktree_prefix": "my-app-slot-",
            "slots": [1, 2],
            "linear_project": "",
        }
        result = format_project_entry(project)
        # Should be valid YAML when combined with "projects:\n"
        data = yaml.safe_load("projects:\n" + result)
        assert data["projects"][0]["name"] == "my-app"
        assert data["projects"][0]["slots"] == [1, 2]
        assert "linear_project" not in data["projects"][0]

    def test_entry_with_linear_project(self):
        project = {
            "name": "my-app",
            "linear_team": "SMA",
            "base_dir": "~/my-app",
            "worktree_prefix": "my-app-slot-",
            "slots": [1],
            "linear_project": "Bot farm",
        }
        result = format_project_entry(project)
        data = yaml.safe_load("projects:\n" + result)
        assert data["projects"][0]["linear_project"] == "Bot farm"

    def test_zero_indent(self):
        project = {
            "name": "my-app",
            "linear_team": "SMA",
            "base_dir": "~/my-app",
            "worktree_prefix": "my-app-slot-",
            "slots": [1, 2],
            "linear_project": "",
        }
        result = format_project_entry(project, indent=0)
        assert result.startswith("- name:")
        data = yaml.safe_load("projects:\n" + result)
        assert data["projects"][0]["name"] == "my-app"
        assert data["projects"][0]["slots"] == [1, 2]


# ---------------------------------------------------------------------------
# _find_projects_insert_point
# ---------------------------------------------------------------------------


class TestFindProjectsInsertPoint:
    def test_projects_followed_by_section(self):
        lines = [
            "projects:",
            "  - name: proj1",
            "    slots: [1]",
            "",
            "linear:",
            "  api_key: test",
        ]
        assert find_projects_insert_point(lines) == 3

    def test_projects_at_end_of_file(self):
        lines = [
            "linear:",
            "  api_key: test",
            "projects:",
            "  - name: proj1",
            "    slots: [1]",
        ]
        assert find_projects_insert_point(lines) == 5

    def test_empty_projects_inline(self):
        lines = [
            "projects: []",
            "",
            "linear:",
            "  api_key: test",
        ]
        assert find_projects_insert_point(lines) == 1

    def test_projects_with_comment_between_sections(self):
        lines = [
            "projects:",
            "  - name: proj1",
            "    slots: [1]",
            "",
            "# This is a top-level comment",
            "linear:",
            "  api_key: test",
        ]
        # Comment is between sections — insert point is after project content
        assert find_projects_insert_point(lines) == 3

    def test_zero_indent_followed_by_section(self):
        lines = [
            "projects:",
            "- name: proj1",
            "  slots: [1]",
            "",
            "linear:",
            "  api_key: test",
        ]
        assert find_projects_insert_point(lines) == 3

    def test_zero_indent_multiple_entries(self):
        lines = [
            "projects:",
            "- name: proj1",
            "  slots: [1]",
            "- name: proj2",
            "  slots: [2]",
            "",
            "linear:",
            "  api_key: test",
        ]
        assert find_projects_insert_point(lines) == 5

    def test_zero_indent_at_end_of_file(self):
        lines = [
            "linear:",
            "  api_key: test",
            "projects:",
            "- name: proj1",
            "  slots: [1]",
        ]
        assert find_projects_insert_point(lines) == 5


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

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()), \
             patch("botfarm.cli.create_client", return_value=mock_client):
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

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()):
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

        with patch("botfarm.project_setup.subprocess.run", side_effect=fail_clone):
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

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()):
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
        original_mock = _make_mock_run()

        def tracking_run(cmd, **kwargs):
            if isinstance(cmd, list) and "worktree" in cmd:
                worktree_cmds.append(cmd)
            return original_mock(cmd, **kwargs)

        with patch("botfarm.project_setup.subprocess.run", side_effect=tracking_run):
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

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()), \
             patch("botfarm.cli.create_client", return_value=mock_client):
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

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()), \
             patch("botfarm.project_setup.run_readiness_checks", return_value=[
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

    def test_partial_worktree_failure_cleanup(
        self, runner, config_dir, tmp_path, monkeypatch
    ):
        """Test cleanup when worktree #2 of 3 fails — repo and worktree #1 are removed."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        worktree_call_count = 0
        base_mock = _make_mock_run()

        def fail_second_worktree(cmd, **kwargs):
            nonlocal worktree_call_count
            if isinstance(cmd, list) and "worktree" in cmd and "add" in cmd:
                worktree_call_count += 1
                if worktree_call_count == 2:
                    result = MagicMock()
                    result.returncode = 1
                    result.stderr = "fatal: worktree error"
                    result.stdout = ""
                    return result
            return base_mock(cmd, **kwargs)

        with patch("botfarm.project_setup.subprocess.run", side_effect=fail_second_worktree):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/cleanup-test.git\ncleanup-test\nSMA\n\n3\ny\n",
            )

        assert result.exit_code != 0
        # The projects dir was freshly created, so the entire dir should be removed
        projects_dir = tmp_path / ".botfarm" / "projects" / "cleanup-test"
        assert not projects_dir.exists(), (
            f"Expected {projects_dir} to be cleaned up after partial worktree failure"
        )
        # Config should NOT have the failed project
        config = yaml.safe_load(config_path.read_text())
        project_names = [p["name"] for p in config["projects"]]
        assert "cleanup-test" not in project_names

    def test_suggests_init_claude_md(self, runner, config_dir, tmp_path, monkeypatch):
        """Test that init-claude-md is suggested when CLAUDE.md is missing."""
        _, config_path = config_dir
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/my-app.git\nmy-app\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert "init-claude-md" in result.output

    def test_placeholder_replaced_on_add(self, runner, tmp_path, monkeypatch):
        """Test that placeholder projects are detected and replaced."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            "projects:\n"
            "  - name: my-project\n"
            "    linear_team: TEAM\n"
            "    base_dir: ~/my-project\n"
            "    worktree_prefix: my-project-slot-\n"
            "    slots: [1, 2]\n"
            "\n"
            "linear:\n"
            "  api_key: test-key\n"
        )
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()):
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/real-app.git\nreal-app\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        assert "placeholder" in result.output.lower()

        config = yaml.safe_load(config_path.read_text())
        names = [p["name"] for p in config["projects"]]
        assert "my-project" not in names
        assert "real-app" in names

    def test_placeholder_name_allowed_for_new_project(self, runner, tmp_path, monkeypatch):
        """Test that user can reuse the placeholder name for the real project."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            "projects:\n"
            "  - name: my-project\n"
            "    linear_team: TEAM\n"
            "    base_dir: ~/my-project\n"
            "    worktree_prefix: my-project-slot-\n"
            "    slots: [1, 2]\n"
            "\n"
            "linear:\n"
            "  api_key: test-key\n"
        )
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_DIR", tmp_path / ".botfarm")
        monkeypatch.delenv("LINEAR_API_KEY", raising=False)

        with patch("botfarm.project_setup.subprocess.run", side_effect=_make_mock_run()):
            # User enters "my-project" as the name — should be allowed
            result = runner.invoke(
                main,
                ["add-project", "--config", str(config_path)],
                input="git@github.com:user/my-project.git\nmy-project\nSMA\n\n1\ny\n",
            )

        assert result.exit_code == 0, result.output
        config = yaml.safe_load(config_path.read_text())
        # Should have exactly one project — the real one
        assert len(config["projects"]) == 1
        assert config["projects"][0]["name"] == "my-project"
        assert config["projects"][0]["linear_team"] == "SMA"
