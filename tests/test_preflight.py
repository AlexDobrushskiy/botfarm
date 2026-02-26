"""Tests for botfarm.preflight module."""

import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from botfarm.config import (
    BotfarmConfig,
    DatabaseConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
)
from botfarm.credentials import CredentialError, OAuthToken
from botfarm.db import SCHEMA_VERSION
from botfarm.linear import LinearAPIError
from botfarm.preflight import (
    CheckResult,
    check_config_consistency,
    check_credentials,
    check_database,
    check_git_repos,
    check_linear_api,
    check_notifications_webhook,
    check_worktree_dirs,
    log_preflight_summary,
    run_preflight_checks,
)


def _make_config(
    tmp_path: Path,
    *,
    projects: list[ProjectConfig] | None = None,
    linear: LinearConfig | None = None,
    notifications: NotificationsConfig | None = None,
) -> BotfarmConfig:
    """Build a BotfarmConfig with sensible defaults for testing."""
    if projects is None:
        base = tmp_path / "repo"
        base.mkdir()
        (base / ".git").mkdir()
        projects = [ProjectConfig(
            name="test-project",
            linear_team="TST",
            base_dir=str(base),
            worktree_prefix="test-slot-",
            slots=[1, 2],
        )]
    return BotfarmConfig(
        projects=projects,
        linear=linear or LinearConfig(api_key="lin_test_key"),
        database=DatabaseConfig(),
        notifications=notifications or NotificationsConfig(),
    )


# ---------------------------------------------------------------------------
# check_git_repos
# ---------------------------------------------------------------------------


class TestCheckGitRepos:
    def test_pass_valid_git_repo(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            results = check_git_repos(config)
        assert len(results) == 1
        assert results[0].passed
        assert "OK" in results[0].message

    def test_fail_missing_base_dir(self, tmp_path):
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(tmp_path / "nonexistent"),
            worktree_prefix="p-", slots=[1],
        )])
        results = check_git_repos(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "does not exist" in results[0].message

    def test_fail_not_git_repo(self, tmp_path):
        base = tmp_path / "notgit"
        base.mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        results = check_git_repos(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "not a git repository" in results[0].message

    def test_fail_remote_unreachable(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 128
            mock_run.return_value.stderr = "fatal: could not read from remote"
            results = check_git_repos(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "not reachable" in results[0].message

    def test_fail_git_timeout(self, tmp_path):
        import subprocess

        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run", side_effect=subprocess.TimeoutExpired("git", 15)):
            results = check_git_repos(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "timed out" in results[0].message

    def test_passes_env_to_subprocess(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        custom_env = {"GIT_SSH_COMMAND": "ssh -i /my/key"}
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            results = check_git_repos(config, env=custom_env)
        assert len(results) == 1
        assert results[0].passed
        call_env = mock_run.call_args.kwargs.get("env")
        assert call_env is not None
        assert call_env["GIT_SSH_COMMAND"] == "ssh -i /my/key"

    def test_no_env_when_none(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            check_git_repos(config, env=None)
        assert mock_run.call_args.kwargs.get("env") is None


# ---------------------------------------------------------------------------
# check_worktree_dirs
# ---------------------------------------------------------------------------


class TestCheckWorktreeDirs:
    def test_pass_writable_parent(self, tmp_path):
        base = tmp_path / "repo"
        base.mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        results = check_worktree_dirs(config)
        assert len(results) == 1
        assert results[0].passed

    def test_fail_parent_not_exists(self, tmp_path):
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T",
            base_dir=str(tmp_path / "nonexistent" / "deep" / "repo"),
            worktree_prefix="p-", slots=[1],
        )])
        results = check_worktree_dirs(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "does not exist" in results[0].message

    def test_fail_parent_not_writable(self, tmp_path):
        base = tmp_path / "repo"
        base.mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.os.access", return_value=False):
            results = check_worktree_dirs(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "not writable" in results[0].message


# ---------------------------------------------------------------------------
# check_linear_api
# ---------------------------------------------------------------------------


class TestCheckLinearApi:
    def test_fail_no_api_key(self, tmp_path):
        config = _make_config(tmp_path, linear=LinearConfig(api_key=""))
        results = check_linear_api(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "not set" in results[0].message

    def test_pass_team_and_statuses(self, tmp_path):
        config = _make_config(tmp_path)
        team_states = {
            "Todo": "s1", "In Progress": "s2",
            "Done": "s3", "In Review": "s4",
        }
        with patch.object(
            __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
            "get_team_states",
            return_value=team_states,
        ):
            results = check_linear_api(config)

        # Should have: 1 team check + 5 status checks = 6
        assert len(results) == 6
        # Team check passes
        assert results[0].passed
        assert "TST" in results[0].message
        # All 5 statuses pass (failed_status defaults to "Todo" which also exists)
        passed_statuses = [r for r in results[1:] if r.passed]
        assert len(passed_statuses) == 5
        failed_statuses = [r for r in results[1:] if not r.passed]
        assert len(failed_statuses) == 0

    def test_fail_team_not_found(self, tmp_path):
        config = _make_config(tmp_path)
        with patch.object(
            __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
            "get_team_states",
            side_effect=LinearAPIError("Team not found"),
        ):
            results = check_linear_api(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "Cannot reach team" in results[0].message

    def test_fail_missing_status(self, tmp_path):
        config = _make_config(tmp_path, linear=LinearConfig(
            api_key="key", in_review_status="Code Review",
        ))
        team_states = {
            "Todo": "s1", "In Progress": "s2", "Done": "s3",
        }
        with patch.object(
            __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
            "get_team_states",
            return_value=team_states,
        ):
            results = check_linear_api(config)

        failed = [r for r in results if not r.passed]
        assert len(failed) >= 1
        # "Code Review" is not in team_states
        review_fail = [r for r in failed if "Code Review" in r.message]
        assert len(review_fail) == 1


# ---------------------------------------------------------------------------
# check_credentials
# ---------------------------------------------------------------------------


class TestCheckCredentials:
    def test_pass_token_loaded(self):
        token = OAuthToken(access_token="test-token", expires_at="2099-01-01T00:00:00Z")
        with patch("botfarm.preflight._load_token", return_value=token):
            results = check_credentials()
        assert len(results) == 1
        assert results[0].passed
        assert "expires_at" in results[0].message

    def test_warn_no_credentials(self):
        with patch("botfarm.preflight._load_token", side_effect=CredentialError("not found")):
            results = check_credentials()
        assert len(results) == 1
        assert not results[0].passed
        assert not results[0].critical  # Warning, not failure

    def test_pass_token_without_expiry(self):
        token = OAuthToken(access_token="test-token", expires_at=None)
        with patch("botfarm.preflight._load_token", return_value=token):
            results = check_credentials()
        assert len(results) == 1
        assert results[0].passed
        assert "expires_at" not in results[0].message


# ---------------------------------------------------------------------------
# check_database
# ---------------------------------------------------------------------------


class TestCheckDatabase:
    def test_fail_env_not_set(self, tmp_path, monkeypatch):
        monkeypatch.delenv("BOTFARM_DB_PATH", raising=False)
        config = _make_config(tmp_path)
        results = check_database(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "BOTFARM_DB_PATH" in results[0].message

    def test_pass_new_db(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "new.db"))
        config = _make_config(tmp_path)
        results = check_database(config)
        assert len(results) == 1
        assert results[0].passed

    def test_pass_existing_db_matching_version(self, tmp_path, monkeypatch):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION,))
        conn.commit()
        conn.close()

        monkeypatch.setenv("BOTFARM_DB_PATH", str(db_path))
        config = _make_config(tmp_path)
        results = check_database(config)
        assert len(results) == 1
        assert results[0].passed

    def test_pass_older_version_will_migrate(self, tmp_path, monkeypatch):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION - 1,))
        conn.commit()
        conn.close()

        monkeypatch.setenv("BOTFARM_DB_PATH", str(db_path))
        config = _make_config(tmp_path)
        results = check_database(config)
        assert len(results) == 1
        assert results[0].passed
        assert "will migrate" in results[0].message

    def test_fail_newer_version(self, tmp_path, monkeypatch):
        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(str(db_path))
        conn.execute("CREATE TABLE schema_version (version INTEGER NOT NULL)")
        conn.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION + 1,))
        conn.commit()
        conn.close()

        monkeypatch.setenv("BOTFARM_DB_PATH", str(db_path))
        config = _make_config(tmp_path)
        results = check_database(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "cannot downgrade" in results[0].message

    def test_fail_dir_not_writable(self, tmp_path, monkeypatch):
        db_dir = tmp_path / "dbdir"
        db_dir.mkdir()
        monkeypatch.setenv("BOTFARM_DB_PATH", str(db_dir / "test.db"))
        config = _make_config(tmp_path)
        with patch("botfarm.preflight.os.access", return_value=False):
            results = check_database(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "not writable" in results[0].message


# ---------------------------------------------------------------------------
# check_config_consistency
# ---------------------------------------------------------------------------


class TestCheckConfigConsistency:
    def test_pass_unique_slots(self, tmp_path):
        config = _make_config(tmp_path, projects=[
            ProjectConfig(name="p1", linear_team="T", base_dir="/a",
                          worktree_prefix="p1-", slots=[1, 2]),
            ProjectConfig(name="p2", linear_team="T", base_dir="/b",
                          worktree_prefix="p2-", slots=[1, 2]),
        ])
        results = check_config_consistency(config)
        assert len(results) == 1
        assert results[0].passed
        assert "2 project(s)" in results[0].message

    def test_fail_duplicate_slots_within_project(self, tmp_path):
        # This is already caught by config validation, but let's test the
        # preflight check handles it gracefully if config was built programmatically
        config = _make_config(tmp_path, projects=[
            ProjectConfig(name="p1", linear_team="T", base_dir="/a",
                          worktree_prefix="p1-", slots=[1, 1]),
        ])
        results = check_config_consistency(config)
        failed = [r for r in results if not r.passed]
        assert len(failed) == 1
        assert "Duplicate slot" in failed[0].message


# ---------------------------------------------------------------------------
# check_notifications_webhook
# ---------------------------------------------------------------------------


class TestCheckNotificationsWebhook:
    def test_skip_when_not_configured(self, tmp_path):
        config = _make_config(tmp_path)
        results = check_notifications_webhook(config)
        assert len(results) == 0

    def test_pass_valid_url(self, tmp_path):
        config = _make_config(tmp_path, notifications=NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/T0/B0/xxx",
        ))
        results = check_notifications_webhook(config)
        assert len(results) == 1
        assert results[0].passed
        assert not results[0].critical  # Webhook is always non-critical

    def test_warn_invalid_url(self, tmp_path):
        config = _make_config(tmp_path, notifications=NotificationsConfig(
            webhook_url="not-a-url",
        ))
        results = check_notifications_webhook(config)
        assert len(results) == 1
        assert not results[0].passed
        assert not results[0].critical


# ---------------------------------------------------------------------------
# log_preflight_summary
# ---------------------------------------------------------------------------


class TestLogPreflightSummary:
    def test_returns_true_all_pass(self):
        results = [
            CheckResult(name="a", passed=True, message="ok"),
            CheckResult(name="b", passed=True, message="ok"),
        ]
        assert log_preflight_summary(results) is True

    def test_returns_false_critical_failure(self):
        results = [
            CheckResult(name="a", passed=True, message="ok"),
            CheckResult(name="b", passed=False, message="bad", critical=True),
        ]
        assert log_preflight_summary(results) is False

    def test_returns_true_with_warnings_only(self):
        results = [
            CheckResult(name="a", passed=True, message="ok"),
            CheckResult(name="b", passed=False, message="warn", critical=False),
        ]
        assert log_preflight_summary(results) is True


# ---------------------------------------------------------------------------
# run_preflight_checks integration
# ---------------------------------------------------------------------------


class TestRunPreflightChecks:
    def test_runs_all_checks(self, tmp_path, monkeypatch):
        base = tmp_path / "repo"
        base.mkdir()
        (base / ".git").mkdir()
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="TST", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])

        team_states = {
            "Todo": "s1", "In Progress": "s2",
            "Done": "s3", "In Review": "s4",
        }
        token = OAuthToken(access_token="tok")

        with patch("botfarm.preflight.subprocess.run") as mock_run, \
             patch.object(
                 __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
                 "get_team_states",
                 return_value=team_states,
             ), \
             patch("botfarm.preflight._load_token", return_value=token):
            mock_run.return_value.returncode = 0
            results = run_preflight_checks(config)

        # Should have results from all check categories
        names = {r.name.split(":")[0] for r in results}
        assert "config_consistency" in names
        assert "database" in names
        assert "git_repo" in names
        assert "worktree_dir" in names
        assert "linear_team" in names
        assert "claude_credentials" in names

        # All should pass
        assert all(r.passed for r in results)
