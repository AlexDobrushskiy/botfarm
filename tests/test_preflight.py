"""Tests for botfarm.preflight module."""

import os
import sqlite3
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from botfarm.config import (
    AgentsConfig,
    BotfarmConfig,
    CoderIdentity,
    DatabaseConfig,
    IdentitiesConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
    ReviewerIdentity,
)
from botfarm.credentials import CredentialError, OAuthToken
from botfarm.db import SCHEMA_VERSION
from botfarm.linear import LinearAPIError
from botfarm.preflight import (
    CheckResult,
    _describe_identity,
    _resolve_remote_url,
    check_codex_reviewer,
    check_config_consistency,
    check_credentials,
    check_database,
    check_git_repos,
    check_identity_cross_validation,
    check_identity_github_tokens,
    check_identity_linear_api_key,
    check_identity_ssh_key,
    check_linear_api,
    check_notifications_webhook,
    check_systemd_unit,
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
    identities: IdentitiesConfig | None = None,
    agents: AgentsConfig | None = None,
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
        identities=identities or IdentitiesConfig(),
        agents=agents or AgentsConfig(),
    )


# ---------------------------------------------------------------------------
# check_git_repos
# ---------------------------------------------------------------------------


class TestResolveRemoteUrl:
    def test_returns_url_on_success(self, tmp_path):
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            mock_run.return_value.stdout = "git@github.com:org/repo.git\n"
            result = _resolve_remote_url(tmp_path)
        assert result == "git@github.com:org/repo.git"

    def test_returns_none_on_failure(self, tmp_path):
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 2
            mock_run.return_value.stdout = ""
            result = _resolve_remote_url(tmp_path)
        assert result is None

    def test_returns_none_on_timeout(self, tmp_path):
        with patch("botfarm.preflight.subprocess.run",
                    side_effect=subprocess.TimeoutExpired("git", 5)):
            result = _resolve_remote_url(tmp_path)
        assert result is None

    def test_returns_none_on_file_not_found(self, tmp_path):
        with patch("botfarm.preflight.subprocess.run",
                    side_effect=FileNotFoundError("git")):
            result = _resolve_remote_url(tmp_path)
        assert result is None


class TestDescribeIdentity:
    def test_coder_ssh_key_configured(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path="~/.botfarm/coder_id_ed25519"),
        ))
        desc = _describe_identity(config, env=None)
        assert "coder SSH key" in desc
        assert "coder_id_ed25519" in desc

    def test_custom_git_ssh_command(self, tmp_path):
        config = _make_config(tmp_path)
        desc = _describe_identity(config, env={"GIT_SSH_COMMAND": "ssh -i /my/key"})
        assert desc == "custom GIT_SSH_COMMAND"

    def test_default_ssh(self, tmp_path):
        config = _make_config(tmp_path)
        desc = _describe_identity(config, env=None)
        assert desc == "default SSH"

    def test_coder_ssh_key_takes_priority_over_env(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path="~/.botfarm/coder_id_ed25519"),
        ))
        desc = _describe_identity(config, env={"GIT_SSH_COMMAND": "ssh -i /other"})
        assert "coder SSH key" in desc


class TestCheckGitRepos:
    def _mock_git_calls(self, mock_run, *, ls_remote_rc=0, ls_remote_stderr="",
                         remote_url="git@github.com:org/repo.git"):
        """Configure mock_run to handle get-url then ls-remote calls."""
        url_result = subprocess.CompletedProcess(
            args=["git", "remote", "get-url", "origin"],
            returncode=0 if remote_url else 2,
            stdout=f"{remote_url}\n" if remote_url else "",
            stderr="",
        )
        ls_result = subprocess.CompletedProcess(
            args=["git", "ls-remote", "--exit-code", "origin"],
            returncode=ls_remote_rc,
            stdout="",
            stderr=ls_remote_stderr,
        )
        mock_run.side_effect = [url_result, ls_result]

    def test_pass_valid_git_repo(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            self._mock_git_calls(mock_run)
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

    def test_fail_remote_unreachable_includes_url_and_identity(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            self._mock_git_calls(
                mock_run,
                ls_remote_rc=128,
                ls_remote_stderr="fatal: Repository not found.",
                remote_url="git@github.com:org/training-plan-proto.git",
            )
            results = check_git_repos(config)
        assert len(results) == 1
        assert not results[0].passed
        msg = results[0].message
        assert "not reachable" in msg
        assert "git@github.com:org/training-plan-proto.git" in msg
        assert "default SSH" in msg
        assert "Ensure the associated GitHub account has access" in msg
        assert "Repository not found" in msg

    def test_fail_remote_unreachable_with_coder_ssh_key(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path="~/.botfarm/coder_id_ed25519"),
        ), projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            self._mock_git_calls(
                mock_run,
                ls_remote_rc=128,
                ls_remote_stderr="ERROR: Repository not found.",
            )
            results = check_git_repos(config)
        assert len(results) == 1
        msg = results[0].message
        assert "coder SSH key" in msg
        assert "coder_id_ed25519" in msg

    def test_fail_remote_unreachable_without_url(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            self._mock_git_calls(
                mock_run,
                ls_remote_rc=128,
                ls_remote_stderr="fatal: could not read from remote",
                remote_url=None,
            )
            results = check_git_repos(config)
        assert len(results) == 1
        msg = results[0].message
        assert "not reachable" in msg
        # No URL in parentheses when resolution fails
        assert "(git@" not in msg

    def test_fail_git_timeout(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        # First call (get-url) succeeds, second (ls-remote) times out
        url_result = subprocess.CompletedProcess(
            args=[], returncode=0,
            stdout="git@github.com:org/repo.git\n", stderr="",
        )
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.side_effect = [
                url_result,
                subprocess.TimeoutExpired("git", 15),
            ]
            results = check_git_repos(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "timed out" in results[0].message

    def test_passes_env_to_ls_remote_only(self, tmp_path):
        base = tmp_path / "myrepo"
        base.mkdir()
        (base / ".git").mkdir()
        config = _make_config(tmp_path, projects=[ProjectConfig(
            name="p1", linear_team="T", base_dir=str(base),
            worktree_prefix="p-", slots=[1],
        )])
        custom_env = {"GIT_SSH_COMMAND": "ssh -i /my/key"}
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            self._mock_git_calls(mock_run)
            results = check_git_repos(config, env=custom_env)
        assert len(results) == 1
        assert results[0].passed
        # First call (get-url) should NOT get custom env
        assert mock_run.call_args_list[0].kwargs.get("env") is None
        # Second call (ls-remote) should get custom env
        call_env = mock_run.call_args_list[1].kwargs.get("env")
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
            self._mock_git_calls(mock_run)
            check_git_repos(config, env=None)
        # ls-remote call (second) should have env=None
        assert mock_run.call_args_list[1].kwargs.get("env") is None


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
# check_identity_ssh_key
# ---------------------------------------------------------------------------


class TestCheckIdentitySshKey:
    def test_skip_when_not_configured(self, tmp_path):
        config = _make_config(tmp_path)
        results = check_identity_ssh_key(config)
        assert len(results) == 0

    def test_fail_file_missing(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(tmp_path / "nonexistent_key")),
        ))
        results = check_identity_ssh_key(config)
        assert len(results) == 1
        assert not results[0].passed
        assert results[0].critical
        assert "does not exist" in results[0].message

    def test_warn_bad_permissions(self, tmp_path):
        key = tmp_path / "id_ed25519"
        key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\nfakedata\n")
        key.chmod(0o644)  # Group/other readable
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(key)),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 1
            mock_run.return_value.stderr = "successfully authenticated"
            results = check_identity_ssh_key(config)
        perm_results = [r for r in results if "permissions" in r.name]
        assert len(perm_results) == 1
        assert not perm_results[0].passed
        assert not perm_results[0].critical  # Warning only

    def test_pass_good_permissions(self, tmp_path):
        key = tmp_path / "id_ed25519"
        key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\nfakedata\n")
        key.chmod(0o600)
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(key)),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 1
            mock_run.return_value.stderr = "successfully authenticated"
            results = check_identity_ssh_key(config)
        perm_results = [r for r in results if "permissions" in r.name]
        assert len(perm_results) == 0

    def test_fail_not_a_private_key(self, tmp_path):
        key = tmp_path / "id_ed25519"
        key.write_text("ssh-ed25519 AAAA... public key\n")
        key.chmod(0o600)
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(key)),
        ))
        results = check_identity_ssh_key(config)
        failed = [r for r in results if r.name == "identity_ssh_key" and not r.passed]
        assert len(failed) == 1
        assert "does not look like a private key" in failed[0].message

    def test_fail_binary_key_file(self, tmp_path):
        key = tmp_path / "id_ed25519"
        key.write_bytes(b"\x80\x81\x82\xff\xfe")  # Invalid UTF-8
        key.chmod(0o600)
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(key)),
        ))
        results = check_identity_ssh_key(config)
        failed = [r for r in results if r.name == "identity_ssh_key" and not r.passed]
        assert len(failed) == 1
        assert "Cannot read SSH key file" in failed[0].message

    def test_warn_github_ssh_fails(self, tmp_path):
        key = tmp_path / "id_ed25519"
        key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\nfakedata\n")
        key.chmod(0o600)
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(key)),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 255
            mock_run.return_value.stderr = "Permission denied"
            results = check_identity_ssh_key(config)
        ssh_results = [r for r in results if "github_ssh" in r.name]
        assert len(ssh_results) == 1
        assert not ssh_results[0].passed
        assert not ssh_results[0].critical

    def test_pass_github_ssh_success(self, tmp_path):
        key = tmp_path / "id_ed25519"
        key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\nfakedata\n")
        key.chmod(0o600)
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(key)),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 1
            mock_run.return_value.stderr = "Hi user! You've successfully authenticated"
            results = check_identity_ssh_key(config)
        ssh_results = [r for r in results if "github_ssh" in r.name]
        assert len(ssh_results) == 1
        assert ssh_results[0].passed

    def test_warn_ssh_timeout(self, tmp_path):
        key = tmp_path / "id_ed25519"
        key.write_text("-----BEGIN OPENSSH PRIVATE KEY-----\nfakedata\n")
        key.chmod(0o600)
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path=str(key)),
        ))
        with patch("botfarm.preflight.subprocess.run",
                    side_effect=subprocess.TimeoutExpired("ssh", 15)):
            results = check_identity_ssh_key(config)
        ssh_results = [r for r in results if "github_ssh" in r.name]
        assert len(ssh_results) == 1
        assert not ssh_results[0].passed
        assert not ssh_results[0].critical


# ---------------------------------------------------------------------------
# check_identity_github_tokens
# ---------------------------------------------------------------------------


class TestCheckIdentityGithubTokens:
    def test_skip_when_not_configured(self, tmp_path):
        config = _make_config(tmp_path)
        results = check_identity_github_tokens(config)
        assert len(results) == 0

    def test_pass_coder_token(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(github_token="ghp_coder123"),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            mock_run.return_value.stdout = '{"login":"bot"}'
            results = check_identity_github_tokens(config)
        assert len(results) == 1
        assert results[0].passed
        assert "coder" in results[0].name

    def test_fail_coder_token(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(github_token="ghp_bad"),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 1
            mock_run.return_value.stderr = "Bad credentials"
            results = check_identity_github_tokens(config)
        assert len(results) == 1
        assert not results[0].passed
        assert results[0].critical

    def test_pass_both_tokens(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(github_token="ghp_coder"),
            reviewer=ReviewerIdentity(github_token="ghp_reviewer"),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            results = check_identity_github_tokens(config)
        assert len(results) == 2
        assert all(r.passed for r in results)

    def test_fail_timeout(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(github_token="ghp_tok"),
        ))
        with patch("botfarm.preflight.subprocess.run",
                    side_effect=subprocess.TimeoutExpired("gh", 15)):
            results = check_identity_github_tokens(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "timed out" in results[0].message

    def test_fail_gh_not_found_includes_role(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(github_token="ghp_tok"),
        ))
        with patch("botfarm.preflight.subprocess.run",
                    side_effect=FileNotFoundError("gh")):
            results = check_identity_github_tokens(config)
        assert len(results) == 1
        assert not results[0].passed
        assert "coder" in results[0].message
        assert "gh command not found" in results[0].message

    def test_passes_token_as_env(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(github_token="ghp_mytoken"),
        ))
        with patch("botfarm.preflight.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            check_identity_github_tokens(config)
        call_env = mock_run.call_args.kwargs.get("env")
        assert call_env is not None
        assert call_env["GH_TOKEN"] == "ghp_mytoken"


# ---------------------------------------------------------------------------
# check_identity_linear_api_key
# ---------------------------------------------------------------------------


class TestCheckIdentityLinearApiKey:
    def test_skip_when_not_configured(self, tmp_path):
        config = _make_config(tmp_path)
        results = check_identity_linear_api_key(config)
        assert len(results) == 0

    def test_pass_valid_key(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(linear_api_key="lin_api_abc"),
        ))
        with patch.object(
            __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
            "get_viewer_id",
            return_value="user-123",
        ):
            results = check_identity_linear_api_key(config)
        assert len(results) == 1
        assert results[0].passed

    def test_fail_invalid_key(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(linear_api_key="lin_api_bad"),
        ))
        with patch.object(
            __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
            "get_viewer_id",
            side_effect=LinearAPIError("Unauthorized"),
        ):
            results = check_identity_linear_api_key(config)
        assert len(results) == 1
        assert not results[0].passed
        assert results[0].critical

    def test_pass_reviewer_key(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            reviewer=ReviewerIdentity(linear_api_key="lin_api_rev"),
        ))
        with patch.object(
            __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
            "get_viewer_id",
            return_value="user-456",
        ):
            results = check_identity_linear_api_key(config)
        assert len(results) == 1
        assert results[0].passed
        assert "reviewer" in results[0].name

    def test_pass_both_keys(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(linear_api_key="lin_api_coder"),
            reviewer=ReviewerIdentity(linear_api_key="lin_api_rev"),
        ))
        with patch.object(
            __import__("botfarm.linear", fromlist=["LinearClient"]).LinearClient,
            "get_viewer_id",
            return_value="user-123",
        ):
            results = check_identity_linear_api_key(config)
        assert len(results) == 2
        assert all(r.passed for r in results)
        roles = {r.name.split(":")[-1] for r in results}
        assert roles == {"coder", "reviewer"}


# ---------------------------------------------------------------------------
# check_identity_cross_validation
# ---------------------------------------------------------------------------


class TestCheckIdentityCrossValidation:
    def test_no_warnings_when_empty(self, tmp_path):
        config = _make_config(tmp_path)
        results = check_identity_cross_validation(config)
        assert len(results) == 0

    def test_no_warnings_when_both_set(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(
                github_token="ghp_coder",
                ssh_key_path="/some/key",
            ),
        ))
        results = check_identity_cross_validation(config)
        assert len(results) == 0

    def test_warn_ssh_without_github_token(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path="/some/key"),
        ))
        results = check_identity_cross_validation(config)
        assert len(results) == 1
        assert not results[0].passed
        assert not results[0].critical
        assert "SSH key is set but GitHub token is not" in results[0].message

    def test_warn_github_token_without_ssh(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(github_token="ghp_tok"),
        ))
        results = check_identity_cross_validation(config)
        assert len(results) == 1
        assert not results[0].passed
        assert not results[0].critical
        assert "GitHub token is set but SSH key is not" in results[0].message

    def test_warn_same_github_tokens(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(
                github_token="ghp_same",
                ssh_key_path="/some/key",
            ),
            reviewer=ReviewerIdentity(github_token="ghp_same"),
        ))
        results = check_identity_cross_validation(config)
        assert len(results) == 1
        assert not results[0].passed
        assert not results[0].critical
        assert "identical" in results[0].message

    def test_no_warning_different_tokens(self, tmp_path):
        config = _make_config(tmp_path, identities=IdentitiesConfig(
            coder=CoderIdentity(
                github_token="ghp_coder",
                ssh_key_path="/some/key",
            ),
            reviewer=ReviewerIdentity(github_token="ghp_reviewer"),
        ))
        results = check_identity_cross_validation(config)
        assert len(results) == 0


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
             patch("botfarm.preflight._load_token", return_value=token), \
             patch("botfarm.preflight.check_installed_unit_stale", return_value=(False, "OK")):
            mock_run.return_value.returncode = 0
            mock_run.return_value.stdout = "git@github.com:org/repo.git\n"
            mock_run.return_value.stderr = ""
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


# --- check_codex_reviewer ---


class TestCheckCodexReviewer:
    def _make_config_with_codex(self, tmp_path, enabled=True):
        return _make_config(
            tmp_path,
            agents=AgentsConfig(codex_reviewer_enabled=enabled),
        )

    def test_disabled_returns_empty(self, tmp_path):
        config = _make_config(tmp_path)
        results = check_codex_reviewer(config)
        assert results == []

    def test_missing_binary(self, tmp_path):
        config = self._make_config_with_codex(tmp_path)
        with patch("botfarm.preflight.check_codex_available", return_value=(False, "codex binary not found on PATH")):
            results = check_codex_reviewer(config)
        binary_results = [r for r in results if r.name == "codex_reviewer:binary"]
        assert len(binary_results) == 1
        assert not binary_results[0].passed
        assert "not found on PATH" in binary_results[0].message

    def test_binary_found(self, tmp_path, monkeypatch):
        config = self._make_config_with_codex(tmp_path)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        with patch("botfarm.preflight.check_codex_available", return_value=(True, "codex 0.106.0")):
            results = check_codex_reviewer(config)
        binary_results = [r for r in results if r.name == "codex_reviewer:binary"]
        assert len(binary_results) == 1
        assert binary_results[0].passed

    def test_missing_auth(self, tmp_path, monkeypatch):
        config = self._make_config_with_codex(tmp_path)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        with patch("botfarm.preflight.check_codex_available", return_value=(True, "codex 0.106.0")), \
             patch("botfarm.preflight.Path.expanduser") as mock_expand:
            mock_expand.return_value.exists.return_value = False
            results = check_codex_reviewer(config)
        auth_results = [r for r in results if r.name == "codex_reviewer:auth"]
        assert len(auth_results) == 1
        assert not auth_results[0].passed
        assert "OPENAI_API_KEY" in auth_results[0].message

    def test_auth_with_api_key(self, tmp_path, monkeypatch):
        config = self._make_config_with_codex(tmp_path)
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        with patch("botfarm.preflight.check_codex_available", return_value=(True, "codex 0.106.0")):
            results = check_codex_reviewer(config)
        auth_results = [r for r in results if r.name == "codex_reviewer:auth"]
        assert len(auth_results) == 1
        assert auth_results[0].passed

    def test_auth_with_auth_file_only(self, tmp_path, monkeypatch):
        config = self._make_config_with_codex(tmp_path)
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        with patch("botfarm.preflight.check_codex_available", return_value=(True, "codex 0.106.0")), \
             patch("botfarm.preflight.Path.expanduser") as mock_expand:
            mock_expand.return_value.exists.return_value = True
            results = check_codex_reviewer(config)
        auth_results = [r for r in results if r.name == "codex_reviewer:auth"]
        assert len(auth_results) == 1
        assert auth_results[0].passed


class TestCheckSystemdUnit:
    def test_no_unit_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.preflight.check_installed_unit_stale",
            lambda: (False, "no installed unit file"),
        )
        results = check_systemd_unit()
        assert results == []

    def test_stale_unit_warns(self, monkeypatch):
        monkeypatch.setattr(
            "botfarm.preflight.check_installed_unit_stale",
            lambda: (True, "unit contains --no-auto-restart"),
        )
        results = check_systemd_unit()
        assert len(results) == 1
        assert not results[0].passed
        assert not results[0].critical
        assert results[0].name == "systemd_unit"

    def test_current_unit_returns_empty(self, monkeypatch):
        monkeypatch.setattr(
            "botfarm.preflight.check_installed_unit_stale",
            lambda: (False, "OK"),
        )
        results = check_systemd_unit()
        assert results == []
