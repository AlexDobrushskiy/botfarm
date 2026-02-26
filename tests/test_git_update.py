"""Tests for botfarm.git_update module."""

import subprocess
import sys
import threading
from unittest.mock import MagicMock, patch

import pytest

from botfarm.git_update import UPDATE_EXIT_CODE, commits_behind, pull_and_install


class TestUpdateExitCode:
    def test_exit_code_is_42(self):
        assert UPDATE_EXIT_CODE == 42


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
    def test_passes_env_to_subprocess(self, mock_run):
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
            assert call_env["GIT_SSH_COMMAND"] == "ssh -i /my/key"

    @patch("botfarm.git_update.subprocess.run")
    def test_no_env_when_none(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0, stdout="0\n"),
        ]
        commits_behind(env=None)
        for call in mock_run.call_args_list:
            assert "env" not in call.kwargs


class TestPullAndInstall:
    @patch("botfarm.git_update.subprocess.run")
    def test_success(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull
            MagicMock(returncode=0),  # pip install
        ]
        assert pull_and_install() is True

    @patch("botfarm.git_update.subprocess.run")
    def test_git_pull_failure(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, "git pull")
        assert pull_and_install() is False

    @patch("botfarm.git_update.subprocess.run")
    def test_pip_install_failure(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull ok
            subprocess.CalledProcessError(1, "pip install"),
        ]
        assert pull_and_install() is False

    @patch("botfarm.git_update.subprocess.run")
    def test_passes_repo_dir(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0),
        ]
        pull_and_install(repo_dir="/my/repo")
        for call in mock_run.call_args_list:
            assert call.kwargs.get("cwd") == "/my/repo"

    @patch("botfarm.git_update.subprocess.run")
    def test_timeout(self, mock_run):
        mock_run.side_effect = subprocess.TimeoutExpired("git pull", 120)
        assert pull_and_install() is False

    @patch("botfarm.git_update.subprocess.run")
    def test_file_not_found(self, mock_run):
        mock_run.side_effect = FileNotFoundError("git")
        assert pull_and_install() is False

    @patch("botfarm.git_update.subprocess.run")
    def test_passes_env_to_subprocess(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull
            MagicMock(returncode=0),  # pip install
        ]
        custom_env = {"GIT_SSH_COMMAND": "ssh -i /my/key"}
        assert pull_and_install(env=custom_env) is True
        for call in mock_run.call_args_list:
            call_env = call.kwargs.get("env")
            assert call_env is not None
            assert call_env["GIT_SSH_COMMAND"] == "ssh -i /my/key"

    @patch("botfarm.git_update.subprocess.run")
    def test_no_env_when_none(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),
            MagicMock(returncode=0),
        ]
        pull_and_install(env=None)
        for call in mock_run.call_args_list:
            assert "env" not in call.kwargs

    @patch("botfarm.git_update.subprocess.run")
    def test_uses_sys_executable_for_pip(self, mock_run):
        mock_run.side_effect = [
            MagicMock(returncode=0),  # git pull
            MagicMock(returncode=0),  # pip install
        ]
        pull_and_install()
        pip_call = mock_run.call_args_list[1]
        assert pip_call.args[0][:3] == [sys.executable, "-m", "pip"]


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
        assert "3 commits behind" in resp.text
        assert app.state.update_in_progress is False
        assert not failed_event.is_set()


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
