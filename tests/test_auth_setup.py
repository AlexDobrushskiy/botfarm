"""Tests for botfarm.auth_setup module."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from botfarm.auth_setup import (
    AuthCheck,
    _write_env_var,
    check_bugtracker_auth,
    check_claude_auth,
    check_github_auth,
    display_auth_summary,
    run_auth_checks,
    run_interactive_auth,
)
from botfarm.config import (
    AgentsConfig,
    BotfarmConfig,
    BugtrackerConfig,
    CoderIdentity,
    DatabaseConfig,
    IdentitiesConfig,
    JiraBugtrackerConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
    ReviewerIdentity,
)
from botfarm.credentials import CredentialError, OAuthToken


def _make_config(
    *,
    bugtracker: BugtrackerConfig | LinearConfig | JiraBugtrackerConfig | None = None,
) -> BotfarmConfig:
    """Build a minimal BotfarmConfig for auth tests."""
    return BotfarmConfig(
        projects=[],
        bugtracker=bugtracker or LinearConfig(api_key="lin_test_key"),
        database=DatabaseConfig(),
        notifications=NotificationsConfig(),
        identities=IdentitiesConfig(),
        agents=AgentsConfig(),
    )


# ---------------------------------------------------------------------------
# check_claude_auth
# ---------------------------------------------------------------------------


class TestCheckClaudeAuth:
    def test_passes_when_authenticated(self):
        token = OAuthToken(access_token="test-token")
        with patch("botfarm.auth_setup.shutil.which", return_value="/usr/bin/claude"), \
             patch("botfarm.auth_setup._load_token", return_value=token):
            result = check_claude_auth()
        assert result.passed is True
        assert result.name == "claude"

    def test_fails_when_no_credentials(self):
        with patch("botfarm.auth_setup.shutil.which", return_value="/usr/bin/claude"), \
             patch("botfarm.auth_setup._load_token", side_effect=CredentialError("no creds")):
            result = check_claude_auth()
        assert result.passed is False
        assert result.fixable is True
        assert "Not authenticated" in result.message

    def test_fails_when_binary_missing(self):
        with patch("botfarm.auth_setup.shutil.which", return_value=None), \
             patch("botfarm.auth_setup.Path") as mock_path_cls:
            mock_path_cls.return_value.expanduser.return_value.exists.return_value = False
            result = check_claude_auth()
        assert result.passed is False
        assert result.fixable is False
        assert "not found" in result.message

    def test_fails_when_binary_in_local_bin_but_not_on_path(self, tmp_path):
        with patch("botfarm.auth_setup.shutil.which", return_value=None), \
             patch("botfarm.auth_setup.Path") as mock_path_cls:
            mock_path_cls.return_value.expanduser.return_value.exists.return_value = True
            result = check_claude_auth()
        assert result.passed is False
        assert result.fixable is False
        assert "not on PATH" in result.message


# ---------------------------------------------------------------------------
# check_github_auth
# ---------------------------------------------------------------------------


class TestCheckGitHubAuth:
    def test_passes_with_gh_token(self, monkeypatch):
        monkeypatch.setenv("GH_TOKEN", "ghp_test")
        result = check_github_auth()
        assert result.passed is True

    def test_passes_with_github_token(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setenv("GITHUB_TOKEN", "ghp_test")
        result = check_github_auth()
        assert result.passed is True

    def test_passes_with_hosts_yml(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        hosts = tmp_path / ".config" / "gh" / "hosts.yml"
        hosts.parent.mkdir(parents=True)
        hosts.write_text("github.com:\n  oauth_token: test\n")

        with patch("botfarm.auth_setup.shutil.which", return_value="/usr/bin/gh"), \
             patch("botfarm.auth_setup.Path.home", return_value=tmp_path):
            result = check_github_auth()
        assert result.passed is True

    def test_fails_when_gh_not_installed(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        with patch("botfarm.auth_setup.shutil.which", return_value=None):
            result = check_github_auth()
        assert result.passed is False
        assert result.fixable is False
        assert "not found" in result.message

    def test_fails_when_not_authenticated(self, tmp_path, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GITHUB_TOKEN", raising=False)
        with patch("botfarm.auth_setup.shutil.which", return_value="/usr/bin/gh"), \
             patch("botfarm.auth_setup.Path.home", return_value=tmp_path):
            result = check_github_auth()
        assert result.passed is False
        assert result.fixable is True
        assert "Not authenticated" in result.message


# ---------------------------------------------------------------------------
# check_bugtracker_auth
# ---------------------------------------------------------------------------


class TestCheckBugtrackerAuth:
    def test_passes_with_valid_linear_key(self):
        config = _make_config(bugtracker=LinearConfig(api_key="lin_test_key"))
        mock_client = MagicMock()
        mock_client.get_viewer_id.return_value = "user-123"

        with patch("botfarm.bugtracker.create_client", return_value=mock_client):
            result = check_bugtracker_auth(config)
        assert result.passed is True
        assert "Linear" in result.label

    def test_fails_when_no_config(self):
        result = check_bugtracker_auth(None)
        assert result.passed is False
        assert result.fixable is False
        assert "No config" in result.message

    def test_fails_when_no_api_key(self):
        config = _make_config(bugtracker=LinearConfig(api_key=""))
        result = check_bugtracker_auth(config)
        assert result.passed is False
        assert "not configured" in result.message

    def test_fails_with_invalid_key(self):
        config = _make_config(bugtracker=LinearConfig(api_key="bad_key"))
        mock_client = MagicMock()
        mock_client.get_viewer_id.side_effect = Exception("401 Unauthorized")

        with patch("botfarm.bugtracker.create_client", return_value=mock_client):
            result = check_bugtracker_auth(config)
        assert result.passed is False
        assert "invalid" in result.message

    def test_jira_tracker_label(self):
        config = _make_config(bugtracker=JiraBugtrackerConfig(
            api_key="jira_key", url="https://jira.example.com", email="a@b.com",
        ))
        mock_client = MagicMock()
        mock_client.get_viewer_id.return_value = "user-123"

        with patch("botfarm.bugtracker.create_client", return_value=mock_client):
            result = check_bugtracker_auth(config)
        assert result.passed is True
        assert "Jira" in result.label


# ---------------------------------------------------------------------------
# run_auth_checks
# ---------------------------------------------------------------------------


class TestRunAuthChecks:
    def test_returns_three_checks(self):
        config = _make_config()
        token = OAuthToken(access_token="test")
        mock_client = MagicMock()
        mock_client.get_viewer_id.return_value = "user-123"

        with patch("botfarm.auth_setup.shutil.which", return_value="/usr/bin/claude"), \
             patch("botfarm.auth_setup._load_token", return_value=token), \
             patch("botfarm.auth_setup.os.environ", {"GH_TOKEN": "ghp_test"}), \
             patch("botfarm.bugtracker.create_client", return_value=mock_client):
            results = run_auth_checks(config)

        assert len(results) == 3
        names = [r.name for r in results]
        assert "claude" in names
        assert "github" in names
        assert "bugtracker" in names


# ---------------------------------------------------------------------------
# display_auth_summary
# ---------------------------------------------------------------------------


class TestDisplayAuthSummary:
    def test_renders_without_error(self):
        from rich.console import Console
        from io import StringIO

        output = StringIO()
        console = Console(file=output, force_terminal=True)
        checks = [
            AuthCheck(name="claude", label="Claude Code", passed=True, message="OK"),
            AuthCheck(name="github", label="GitHub CLI", passed=False, message="Missing"),
        ]
        display_auth_summary(console, checks)
        rendered = output.getvalue()
        assert "Claude Code" in rendered
        assert "GitHub CLI" in rendered


# ---------------------------------------------------------------------------
# _write_env_var
# ---------------------------------------------------------------------------


class TestWriteEnvVar:
    def test_creates_new_file(self, tmp_path):
        env_path = tmp_path / ".env"
        _write_env_var(env_path, "LINEAR_API_KEY", "test-key")
        content = env_path.read_text()
        assert "LINEAR_API_KEY=test-key\n" in content

    def test_updates_existing_var(self, tmp_path):
        env_path = tmp_path / ".env"
        env_path.write_text("OTHER=foo\nLINEAR_API_KEY=old\nANOTHER=bar\n")
        _write_env_var(env_path, "LINEAR_API_KEY", "new-key")
        content = env_path.read_text()
        assert "LINEAR_API_KEY=new-key" in content
        assert "old" not in content
        assert "OTHER=foo" in content
        assert "ANOTHER=bar" in content

    def test_appends_when_var_not_present(self, tmp_path):
        env_path = tmp_path / ".env"
        env_path.write_text("OTHER=foo\n")
        _write_env_var(env_path, "NEW_VAR", "value")
        content = env_path.read_text()
        assert "OTHER=foo" in content
        assert "NEW_VAR=value" in content

    def test_creates_parent_dirs(self, tmp_path):
        env_path = tmp_path / "sub" / "dir" / ".env"
        _write_env_var(env_path, "KEY", "val")
        assert env_path.exists()
        assert "KEY=val" in env_path.read_text()


# ---------------------------------------------------------------------------
# CLI command integration
# ---------------------------------------------------------------------------


class TestAuthCommand:
    def test_auth_command_exists(self):
        from botfarm.cli import main
        runner = CliRunner()
        result = runner.invoke(main, ["auth", "--help"])
        assert result.exit_code == 0
        assert "authentication" in result.output.lower()

    def test_auth_runs_without_config(self, tmp_path, monkeypatch):
        from botfarm.cli import main

        monkeypatch.setenv("GH_TOKEN", "ghp_test")
        # Point config to non-existent path
        runner = CliRunner()
        result = runner.invoke(main, [
            "auth", "--config", str(tmp_path / "nonexistent.yaml"),
        ])
        # Should still run (config=None case is handled)
        assert result.exit_code == 0

    def test_auth_shows_status_table(self, tmp_path, monkeypatch):
        from botfarm.cli import main

        monkeypatch.setenv("GH_TOKEN", "ghp_test")
        token = OAuthToken(access_token="test")

        runner = CliRunner()
        with patch("botfarm.auth_setup.shutil.which", return_value="/usr/bin/claude"), \
             patch("botfarm.auth_setup._load_token", return_value=token):
            result = runner.invoke(main, [
                "auth", "--config", str(tmp_path / "nonexistent.yaml"),
            ])

        assert result.exit_code == 0
        assert "Authentication" in result.output
