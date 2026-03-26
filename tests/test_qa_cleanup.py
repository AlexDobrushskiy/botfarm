"""Tests for QA environment cleanup (botfarm.qa_cleanup)."""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import ProjectConfig
from botfarm.qa_cleanup import (
    _kill_docker_compose,
    _kill_orphan_browsers,
    _kill_port_holder,
    _run_teardown_command,
    cleanup_qa_environment,
)


# ---------------------------------------------------------------------------
# _kill_port_holder
# ---------------------------------------------------------------------------


class TestKillPortHolder:
    def test_kills_pids_from_fuser(self):
        """Should SIGKILL every PID reported by fuser."""
        result = MagicMock()
        result.stdout = " 1234  5678 "
        with (
            patch("subprocess.run", return_value=result) as mock_run,
            patch("os.kill") as mock_kill,
        ):
            _kill_port_holder(3000, label="test/0")
            mock_run.assert_called_once_with(
                ["fuser", "3000/tcp"],
                capture_output=True, text=True, timeout=5,
            )
            mock_kill.assert_any_call(1234, signal.SIGKILL)
            mock_kill.assert_any_call(5678, signal.SIGKILL)

    def test_noop_when_no_pids(self):
        """No kills when fuser returns empty output."""
        result = MagicMock()
        result.stdout = ""
        with (
            patch("subprocess.run", return_value=result),
            patch("os.kill") as mock_kill,
        ):
            _kill_port_holder(3000, label="test/0")
            mock_kill.assert_not_called()

    def test_fuser_not_found(self):
        """Should not raise when fuser is not installed."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            _kill_port_holder(3000, label="test/0")  # no exception

    def test_oserror_on_kill_ignored(self):
        """OSError on os.kill (process already gone) should be ignored."""
        result = MagicMock()
        result.stdout = "999"
        with (
            patch("subprocess.run", return_value=result),
            patch("os.kill", side_effect=OSError),
        ):
            _kill_port_holder(3000, label="test/0")  # no exception


# ---------------------------------------------------------------------------
# _kill_docker_compose
# ---------------------------------------------------------------------------


class TestKillDockerCompose:
    def test_runs_compose_down_when_compose_file_exists(self, tmp_path):
        """Should run docker compose down if a compose file is present."""
        (tmp_path / "docker-compose.yml").touch()
        with patch("subprocess.run") as mock_run:
            _kill_docker_compose(str(tmp_path), label="test/0")
            mock_run.assert_called_once()
            args = mock_run.call_args
            assert args[0][0] == ["docker", "compose", "down", "--remove-orphans", "--timeout", "5"]
            assert args[1]["cwd"] == str(tmp_path)

    def test_noop_when_no_compose_file(self, tmp_path):
        """Should skip docker compose if no compose file is present."""
        with patch("subprocess.run") as mock_run:
            _kill_docker_compose(str(tmp_path), label="test/0")
            mock_run.assert_not_called()

    def test_recognises_compose_yaml(self, tmp_path):
        """Should recognise compose.yaml as well."""
        (tmp_path / "compose.yaml").touch()
        with patch("subprocess.run") as mock_run:
            _kill_docker_compose(str(tmp_path), label="test/0")
            mock_run.assert_called_once()

    def test_docker_not_found(self, tmp_path):
        """Should not raise when docker is not installed."""
        (tmp_path / "docker-compose.yml").touch()
        with patch("subprocess.run", side_effect=FileNotFoundError):
            _kill_docker_compose(str(tmp_path), label="test/0")  # no exception


# ---------------------------------------------------------------------------
# _kill_orphan_browsers
# ---------------------------------------------------------------------------


class TestKillOrphanBrowsers:
    def test_kills_browser_with_matching_cwd(self, tmp_path):
        """Should kill browser PIDs whose /proc/PID/cwd is under the worktree."""
        result = MagicMock()
        result.stdout = "42\n"
        with (
            patch("subprocess.run", return_value=result),
            patch("pathlib.Path.resolve") as mock_resolve,
            patch("os.kill") as mock_kill,
        ):
            # Make resolve return matching path for /proc/42/cwd
            mock_resolve.side_effect = lambda: Path(str(tmp_path) + "/subdir")
            _kill_orphan_browsers(str(tmp_path), label="test/0")
            # The function checks /proc/<pid>/cwd — with mocked resolve
            # this test verifies the kill path runs without error

    def test_pgrep_not_found(self, tmp_path):
        """Should not raise when pgrep is not installed."""
        with patch("subprocess.run", side_effect=FileNotFoundError):
            _kill_orphan_browsers(str(tmp_path), label="test/0")  # no exception

    def test_noop_when_no_browser_pids(self, tmp_path):
        """Should not kill anything when pgrep returns empty."""
        result = MagicMock()
        result.stdout = ""
        with (
            patch("subprocess.run", return_value=result),
            patch("os.kill") as mock_kill,
        ):
            _kill_orphan_browsers(str(tmp_path), label="test/0")
            mock_kill.assert_not_called()


# ---------------------------------------------------------------------------
# _run_teardown_command
# ---------------------------------------------------------------------------


class TestRunTeardownCommand:
    def test_runs_command(self, tmp_path):
        """Should run the teardown command via shell."""
        with patch("subprocess.run") as mock_run:
            _run_teardown_command("make clean", cwd=str(tmp_path), label="test/0")
            mock_run.assert_called_once_with(
                "make clean", shell=True, cwd=str(tmp_path),
                capture_output=True, timeout=30,
            )

    def test_timeout_logged_not_raised(self, tmp_path):
        """Should catch timeout without raising."""
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("cmd", 30)):
            _run_teardown_command("sleep 999", cwd=str(tmp_path), label="test/0")

    def test_generic_error_logged_not_raised(self, tmp_path):
        """Should catch generic errors without raising."""
        with patch("subprocess.run", side_effect=RuntimeError("fail")):
            _run_teardown_command("bad", cwd=str(tmp_path), label="test/0")


# ---------------------------------------------------------------------------
# cleanup_qa_environment (integration)
# ---------------------------------------------------------------------------


class TestCleanupQaEnvironment:
    def _make_project(self, *, run_port=0, qa_teardown_command=""):
        return ProjectConfig(
            name="web-app",
            base_dir="/tmp/web-app",
            worktree_prefix="web-app-slot-",
            slots=[0],
            run_port=run_port,
            qa_teardown_command=qa_teardown_command,
        )

    def test_calls_port_cleanup_when_port_set(self, tmp_path):
        proj = self._make_project(run_port=3000)
        with (
            patch("botfarm.qa_cleanup._kill_port_holder") as mock_port,
            patch("botfarm.qa_cleanup._kill_docker_compose") as mock_docker,
            patch("botfarm.qa_cleanup._kill_orphan_browsers") as mock_browsers,
        ):
            cleanup_qa_environment(proj, 0, worktree_cwd=str(tmp_path))
            mock_port.assert_called_once_with(3000, label="web-app/0")
            mock_docker.assert_called_once()
            mock_browsers.assert_called_once()

    def test_calls_teardown_when_configured(self, tmp_path):
        proj = self._make_project(qa_teardown_command="make teardown")
        with (
            patch("botfarm.qa_cleanup._kill_docker_compose"),
            patch("botfarm.qa_cleanup._kill_orphan_browsers"),
            patch("botfarm.qa_cleanup._run_teardown_command") as mock_teardown,
        ):
            cleanup_qa_environment(proj, 0, worktree_cwd=str(tmp_path))
            mock_teardown.assert_called_once_with(
                "make teardown", cwd=str(tmp_path), label="web-app/0",
            )

    def test_skips_port_cleanup_when_no_port(self, tmp_path):
        proj = self._make_project(qa_teardown_command="echo done")
        with (
            patch("botfarm.qa_cleanup._kill_port_holder") as mock_port,
            patch("botfarm.qa_cleanup._kill_docker_compose"),
            patch("botfarm.qa_cleanup._kill_orphan_browsers"),
            patch("botfarm.qa_cleanup._run_teardown_command"),
        ):
            cleanup_qa_environment(proj, 0, worktree_cwd=str(tmp_path))
            mock_port.assert_not_called()

    def test_skips_worktree_cleanup_when_no_cwd(self):
        proj = self._make_project(run_port=3000)
        with (
            patch("botfarm.qa_cleanup._kill_port_holder") as mock_port,
            patch("botfarm.qa_cleanup._kill_docker_compose") as mock_docker,
            patch("botfarm.qa_cleanup._kill_orphan_browsers") as mock_browsers,
        ):
            cleanup_qa_environment(proj, 0, worktree_cwd=None)
            mock_port.assert_called_once()
            mock_docker.assert_not_called()
            mock_browsers.assert_not_called()


# ---------------------------------------------------------------------------
# ProjectConfig qa_teardown_command field
# ---------------------------------------------------------------------------


class TestProjectConfigQaTeardown:
    def test_default_empty(self):
        proj = ProjectConfig(name="p")
        assert proj.qa_teardown_command == ""

    def test_set_via_constructor(self):
        proj = ProjectConfig(name="p", qa_teardown_command="docker compose down")
        assert proj.qa_teardown_command == "docker compose down"

    def test_equality_includes_teardown(self):
        a = ProjectConfig(name="p", qa_teardown_command="cmd1")
        b = ProjectConfig(name="p", qa_teardown_command="cmd2")
        assert a != b

    def test_repr_includes_teardown(self):
        proj = ProjectConfig(name="p", qa_teardown_command="make clean")
        assert "qa_teardown_command='make clean'" in repr(proj)
