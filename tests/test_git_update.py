"""Tests for botfarm.git_update module."""

import subprocess
import sys
import threading
from unittest.mock import MagicMock, patch

import pytest

from botfarm.git_update import (
    UPDATE_EXIT_CODE,
    _ensure_not_bare,
    _subprocess_env,
    commits_behind,
    pull_and_install,
)


class TestUpdateExitCode:
    def test_exit_code_is_42(self):
        assert UPDATE_EXIT_CODE == 42


class TestSubprocessEnv:
    def test_always_sets_git_terminal_prompt(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        result = _subprocess_env(None)
        assert result["GIT_TERMINAL_PROMPT"] == "0"

    def test_merges_caller_env(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        result = _subprocess_env({"MY_VAR": "hello"})
        assert result["MY_VAR"] == "hello"
        assert result["GIT_TERMINAL_PROMPT"] == "0"

    def test_gh_token_sets_credential_helper(self, monkeypatch):
        monkeypatch.setenv("GH_TOKEN", "ghp_test123")
        monkeypatch.delenv("GIT_CONFIG_COUNT", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        result = _subprocess_env(None)
        assert result["GIT_CONFIG_COUNT"] == "2"
        assert result["GIT_CONFIG_KEY_0"] == "credential.helper"
        assert result["GIT_CONFIG_VALUE_0"] == ""
        assert result["GIT_CONFIG_KEY_1"] == "credential.helper"
        assert "x-access-token" in result["GIT_CONFIG_VALUE_1"]
        assert "GH_TOKEN" in result["GIT_CONFIG_VALUE_1"]

    def test_gh_token_appends_to_existing_git_config(self, monkeypatch):
        monkeypatch.setenv("GH_TOKEN", "ghp_test")
        monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        result = _subprocess_env(None)
        assert result["GIT_CONFIG_COUNT"] == "3"
        assert result["GIT_CONFIG_KEY_1"] == "credential.helper"
        assert result["GIT_CONFIG_KEY_2"] == "credential.helper"

    def test_no_credential_helper_without_gh_token(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_CONFIG_COUNT", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        result = _subprocess_env(None)
        assert "GIT_CONFIG_COUNT" not in result

    def test_ssh_batch_mode_appended(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setenv("GIT_SSH_COMMAND", "ssh -i /my/key")
        result = _subprocess_env(None)
        assert result["GIT_SSH_COMMAND"] == "ssh -i /my/key -o BatchMode=yes"

    def test_ssh_batch_mode_not_duplicated(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.setenv("GIT_SSH_COMMAND", "ssh -o BatchMode=yes -i /my/key")
        result = _subprocess_env(None)
        assert result["GIT_SSH_COMMAND"] == "ssh -o BatchMode=yes -i /my/key"

    def test_no_ssh_modification_without_ssh_command(self, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        result = _subprocess_env(None)
        assert "GIT_SSH_COMMAND" not in result


class TestCommitsBehind:
    @patch("botfarm.git_update.subprocess.run")
    def test_returns_count(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git fetch
            MagicMock(returncode=0, stdout="5\n"),  # git rev-list
        ]
        assert commits_behind() == 5

    @patch("botfarm.git_update.subprocess.run")
    def test_returns_zero_when_up_to_date(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0, stdout="0\n"),
        ]
        assert commits_behind() == 0

    @patch("botfarm.git_update.subprocess.run")
    def test_returns_zero_on_fetch_failure(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, "git fetch")
        assert commits_behind() == 0

    @patch("botfarm.git_update.subprocess.run")
    def test_returns_zero_on_rev_list_failure(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git fetch succeeds
            subprocess.CalledProcessError(1, "git rev-list"),
        ]
        assert commits_behind() == 0

    @patch("botfarm.git_update.subprocess.run")
    def test_returns_zero_on_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired("git fetch", 30)
        assert commits_behind() == 0

    @patch("botfarm.git_update.subprocess.run")
    def test_returns_zero_on_invalid_output(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0, stdout="not-a-number\n"),
        ]
        assert commits_behind() == 0

    @patch("botfarm.git_update.subprocess.run")
    def test_passes_repo_dir(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0, stdout="3\n"),
        ]
        result = commits_behind(repo_dir="/some/path")
        assert result == 3
        for call in mock_run.call_args_list:
            assert call.kwargs.get("cwd") == "/some/path"

    @patch("botfarm.git_update.subprocess.run")
    def test_no_cwd_when_repo_dir_none(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0, stdout="0\n"),
        ]
        commits_behind(repo_dir=None)
        for call in mock_run.call_args_list:
            assert "cwd" not in call.kwargs

    @patch("botfarm.git_update.subprocess.run")
    def test_returns_zero_on_file_not_found(self, mock_run):
        mock_run.side_effect = FileNotFoundError("git")
        assert commits_behind() == 0


    @patch("botfarm.git_update.subprocess.run")
    def test_passes_env_to_subprocess(self, mock_run, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git fetch
            MagicMock(returncode=0, stdout="2\n"),  # git rev-list
        ]
        custom_env = {"GIT_SSH_COMMAND": "ssh -i /my/key"}
        result = commits_behind(env=custom_env)
        assert result == 2
        for call in mock_run.call_args_list:
            call_env = call.kwargs.get("env")
            assert call_env is not None
            assert "ssh -i /my/key" in call_env["GIT_SSH_COMMAND"]
            assert call_env["GIT_TERMINAL_PROMPT"] == "0"

    @patch("botfarm.git_update.subprocess.run")
    def test_always_sets_env_even_when_none(self, mock_run, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0, stdout="0\n"),
        ]
        commits_behind(env=None)
        for call in mock_run.call_args_list:
            call_env = call.kwargs.get("env")
            assert call_env is not None
            assert call_env["GIT_TERMINAL_PROMPT"] == "0"


class TestEnsureNotBare:
    @patch("botfarm.git_update.subprocess.run")
    def test_repairs_bare_true(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="true\n"),  # git config --get
            MagicMock(returncode=0),  # git config --local set
        ]
        _ensure_not_bare("/some/repo")
        assert mock_run.call_count == 2
        set_call = mock_run.call_args_list[1]
        assert set_call.args[0] == ["git", "config", "--local", "core.bare", "false"]

    @patch("botfarm.git_update.subprocess.run")
    def test_no_repair_when_not_bare(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        _ensure_not_bare("/some/repo")
        assert mock_run.call_count == 1

    @patch("botfarm.git_update.subprocess.run")
    def test_no_repair_when_bare_false(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="false\n")
        _ensure_not_bare("/some/repo")
        assert mock_run.call_count == 1

    @patch("botfarm.git_update.subprocess.run")
    def test_passes_repo_dir_as_cwd(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        _ensure_not_bare("/my/repo")
        assert mock_run.call_args.kwargs.get("cwd") == "/my/repo"

    @patch("botfarm.git_update.subprocess.run")
    def test_no_cwd_when_repo_dir_none(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1, stdout="")
        _ensure_not_bare(None)
        assert "cwd" not in mock_run.call_args.kwargs

    @patch("botfarm.git_update.subprocess.run")
    def test_tolerates_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired("git", 5)
        _ensure_not_bare()  # should not raise

    @patch("botfarm.git_update.subprocess.run")
    def test_tolerates_repair_failure(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0, stdout="true\n"),
            subprocess.CalledProcessError(1, "git config"),
        ]
        _ensure_not_bare()  # should not raise


@patch("botfarm.git_update._ensure_not_bare")
class TestPullAndInstall:
    @patch("botfarm.git_update.subprocess.run")
    def test_success(self, mock_run, _mock_bare):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull
            MagicMock(returncode=0),  # pip install
        ]
        assert pull_and_install() == ""

    @patch("botfarm.git_update.subprocess.run")
    def test_git_pull_failure(self, mock_run, _mock_bare):
        mock_run.side_effect = subprocess.CalledProcessError(1, "git pull")
        result = pull_and_install()
        assert result  # non-empty error string
        assert "git pull" in result

    @patch("botfarm.git_update.subprocess.run")
    def test_pip_install_failure(self, mock_run, _mock_bare):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull ok
            subprocess.CalledProcessError(1, "pip install"),
        ]
        result = pull_and_install()
        assert result
        assert "pip install" in result

    @patch("botfarm.git_update.subprocess.run")
    def test_passes_repo_dir(self, mock_run, _mock_bare):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0),
        ]
        pull_and_install(repo_dir="/my/repo")
        for call in mock_run.call_args_list:
            assert call.kwargs.get("cwd") == "/my/repo"

    @patch("botfarm.git_update.subprocess.run")
    def test_timeout(self, mock_run, _mock_bare):
        mock_run.side_effect = subprocess.TimeoutExpired("git pull", 120)
        assert pull_and_install() != ""

    @patch("botfarm.git_update.subprocess.run")
    def test_file_not_found(self, mock_run, _mock_bare):
        mock_run.side_effect = FileNotFoundError("git")
        assert pull_and_install() != ""

    @patch("botfarm.git_update.subprocess.run")
    def test_passes_env_to_subprocess(self, mock_run, _mock_bare, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull
            MagicMock(returncode=0),  # pip install
        ]
        custom_env = {"GIT_SSH_COMMAND": "ssh -i /my/key"}
        assert pull_and_install(env=custom_env) == ""
        for call in mock_run.call_args_list:
            call_env = call.kwargs.get("env")
            assert call_env is not None
            assert "ssh -i /my/key" in call_env["GIT_SSH_COMMAND"]
            assert call_env["GIT_TERMINAL_PROMPT"] == "0"

    @patch("botfarm.git_update.subprocess.run")
    def test_always_sets_env_even_when_none(self, mock_run, _mock_bare, monkeypatch):
        monkeypatch.delenv("GH_TOKEN", raising=False)
        monkeypatch.delenv("GIT_SSH_COMMAND", raising=False)
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0),
        ]
        pull_and_install(env=None)
        for call in mock_run.call_args_list:
            call_env = call.kwargs.get("env")
            assert call_env is not None
            assert call_env["GIT_TERMINAL_PROMPT"] == "0"

    @patch("botfarm.git_update.subprocess.run")
    def test_uses_sys_executable_for_pip(self, mock_run, _mock_bare):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull
            MagicMock(returncode=0),  # pip install
        ]
        pull_and_install()
        pip_call = mock_run.call_args_list[1]
        assert pip_call.args[0][:3] == [sys.executable, "-m", "pip"]

    def test_calls_ensure_not_bare(self, mock_bare):
        with patch("botfarm.git_update.subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(returncode=0),
                MagicMock(returncode=0),
            ]
            pull_and_install(repo_dir="/my/repo")
        mock_bare.assert_called_once_with("/my/repo")


class TestDashboardUpdateBanner:
    """Test the dashboard update banner endpoint."""

    @pytest.fixture()
    def db_file(self, tmp_path):
        from botfarm.db import init_db, save_dispatch_state
        path = tmp_path / "botfarm.db"
        conn = init_db(path)
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        return path

    @pytest.fixture()
    def client(self, db_file):
        from botfarm.dashboard import create_app
        from fastapi.testclient import TestClient
        app = create_app(db_path=db_file)
        return TestClient(app)

    @patch("botfarm.dashboard.commits_behind", return_value=3)
    def test_banner_shows_commits_behind(self, mock_cb, client):
        resp = client.get("/partials/update-banner")
        assert resp.status_code == 200
        assert "3 commits behind" in resp.text
        assert "Update" in resp.text

    @patch("botfarm.dashboard.commits_behind", return_value=1)
    def test_banner_singular_commit(self, mock_cb, client):
        resp = client.get("/partials/update-banner")
        assert resp.status_code == 200
        assert "1 commit behind" in resp.text

    @patch("botfarm.dashboard.commits_behind", return_value=0)
    def test_banner_hidden_when_up_to_date(self, mock_cb, client):
        resp = client.get("/partials/update-banner")
        assert resp.status_code == 200
        assert "Update" not in resp.text
        assert "commit" not in resp.text

    def test_banner_shows_updating_state(self, db_file):
        from botfarm.dashboard import create_app
        from fastapi.testclient import TestClient
        app = create_app(db_path=db_file)
        app.state.update_in_progress = True
        client = TestClient(app)
        resp = client.get("/partials/update-banner")
        assert resp.status_code == 200
        assert "Updating" in resp.text

    @patch("botfarm.dashboard.commits_behind", return_value=3)
    def test_banner_resets_on_update_failed_event(self, mock_cb, db_file):
        from botfarm.dashboard import create_app
        from fastapi.testclient import TestClient
        failed_event = threading.Event()
        app = create_app(db_path=db_file, update_failed_event=failed_event)
        app.state.update_in_progress = True
        failed_event.set()
        client = TestClient(app)
        resp = client.get("/partials/update-banner")
        assert resp.status_code == 200
        # Should have reset to idle and show commits behind, not "Updating..."
        assert "Updating" not in resp.text
        assert app.state.update_in_progress is False
        assert not failed_event.is_set()

    @patch("botfarm.dashboard.commits_behind", return_value=3)
    def test_banner_shows_error_on_update_failure(self, mock_cb, db_file):
        from botfarm.dashboard import create_app
        from fastapi.testclient import TestClient
        failed_event = threading.Event()
        app = create_app(
            db_path=db_file,
            update_failed_event=failed_event,
            get_update_failed_message=lambda: "git pull failed: conflict in main",
        )
        app.state.update_in_progress = True
        failed_event.set()
        client = TestClient(app)
        resp = client.get("/partials/update-banner")
        assert resp.status_code == 200
        assert "Update failed" in resp.text
        assert "git pull failed: conflict in main" in resp.text
        assert "Retry" in resp.text
        assert app.state.update_in_progress is False


class TestDashboardUpdateAPI:
    """Test the POST /api/update endpoint."""

    @pytest.fixture()
    def db_file(self, tmp_path):
        from botfarm.db import init_db, save_dispatch_state
        path = tmp_path / "botfarm.db"
        conn = init_db(path)
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        return path

    def test_update_without_callback(self, db_file):
        from botfarm.dashboard import create_app
        from fastapi.testclient import TestClient
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/update")
        assert resp.status_code == 503

    def test_update_with_callback(self, db_file):
        from botfarm.dashboard import create_app
        from fastapi.testclient import TestClient
        callback = MagicMock()
        app = create_app(db_path=db_file, on_update=callback)
        client = TestClient(app)
        resp = client.post("/api/update")
        assert resp.status_code == 200
        callback.assert_called_once()
        assert app.state.update_in_progress is True
