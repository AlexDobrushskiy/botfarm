"""Tests for botfarm.devserver — DevServerManager."""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import ProjectConfig
from botfarm.devserver import DevServerManager, _ServerInfo


def _make_project(tmp_path: Path, *, name: str = "web-app",
                  run_command: str = "npm run dev",
                  run_env: dict | None = None,
                  run_port: int = 3000) -> ProjectConfig:
    base = tmp_path / name
    base.mkdir(exist_ok=True)
    return ProjectConfig(
        name=name,
        base_dir=str(base),
        run_command=run_command,
        run_env=run_env,
        run_port=run_port,
    )


class TestDevServerManagerStart:
    def test_start_launches_subprocess(self, tmp_path):
        """start() launches a subprocess with correct args."""
        project = _make_project(tmp_path)
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.register_project(project)

        with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_proc.pid = 1234
            mock_proc.poll.return_value = None
            mock_popen.return_value = mock_proc

            mgr.start("web-app")

            mock_popen.assert_called_once()
            call_kwargs = mock_popen.call_args
            assert call_kwargs[0][0] == "npm run dev"
            assert call_kwargs[1]["shell"] is True
            assert call_kwargs[1]["start_new_session"] is True
            assert call_kwargs[1]["cwd"] == str(tmp_path / "web-app")

    def test_start_sets_port_env(self, tmp_path):
        """start() sets PORT env var from run_port config."""
        project = _make_project(tmp_path, run_port=8080)
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.register_project(project)

        with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_proc.pid = 1234
            mock_popen.return_value = mock_proc

            mgr.start("web-app")

            env = mock_popen.call_args[1]["env"]
            assert env["PORT"] == "8080"

    def test_start_merges_run_env(self, tmp_path):
        """start() merges run_env from project config into env."""
        project = _make_project(tmp_path, run_env={"NODE_ENV": "development"})
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.register_project(project)

        with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_proc.pid = 1234
            mock_popen.return_value = mock_proc

            mgr.start("web-app")

            env = mock_popen.call_args[1]["env"]
            assert env["NODE_ENV"] == "development"

    def test_start_noop_when_already_running(self, tmp_path):
        """start() is a no-op if the server is already running."""
        project = _make_project(tmp_path)
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.register_project(project)

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = None  # still running
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            mgr.start("web-app")
            mock_popen.assert_not_called()

    def test_start_raises_for_unregistered_project(self, tmp_path):
        """start() raises ValueError for unknown project."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        with pytest.raises(ValueError, match="not-a-project"):
            mgr.start("not-a-project")

    def test_start_creates_log_dir(self, tmp_path):
        """start() creates the log directory if it doesn't exist."""
        log_dir = tmp_path / "logs" / "devserver"
        project = _make_project(tmp_path)
        mgr = DevServerManager(log_dir=log_dir)
        mgr.register_project(project)

        with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_proc.pid = 1234
            mock_popen.return_value = mock_proc

            mgr.start("web-app")

            assert log_dir.exists()

    def test_start_rotates_existing_log(self, tmp_path):
        """start() rotates existing log to .log.1."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "web-app.log"
        log_file.write_text("old output")

        project = _make_project(tmp_path)
        mgr = DevServerManager(log_dir=log_dir)
        mgr.register_project(project)

        with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            mock_proc = MagicMock()
            mock_proc.pid = 1234
            mock_popen.return_value = mock_proc

            mgr.start("web-app")

            rotated = log_dir / "web-app.log.1"
            assert rotated.exists()
            assert rotated.read_text() == "old output"

    def test_start_no_port_env_when_zero(self, tmp_path):
        """start() does not set PORT if run_port is 0."""
        project = _make_project(tmp_path, run_port=0)
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.register_project(project)

        with patch.dict(os.environ, {}, clear=True):
            with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
                mock_proc = MagicMock()
                mock_proc.pid = 1234
                mock_popen.return_value = mock_proc

                mgr.start("web-app")

                env = mock_popen.call_args[1]["env"]
                assert "PORT" not in env


class TestDevServerManagerStop:
    def test_stop_sends_sigterm_to_process_group(self, tmp_path):
        """stop() sends SIGTERM to the process group."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = None  # still running
        mock_proc.wait.return_value = 0
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        with patch("botfarm.devserver.os.getpgid", return_value=1234) as mock_getpgid, \
             patch("botfarm.devserver.os.killpg") as mock_killpg:
            mgr.stop("web-app")

            mock_killpg.assert_called_once_with(1234, signal.SIGTERM)
            mock_proc.wait.assert_called_once()

        assert "web-app" not in mgr._servers

    def test_stop_escalates_to_sigkill(self, tmp_path):
        """stop() escalates to SIGKILL if SIGTERM doesn't work and reaps zombie."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = None
        # First wait() raises TimeoutExpired (SIGTERM grace period);
        # second wait() succeeds (reap after SIGKILL).
        mock_proc.wait.side_effect = [
            subprocess.TimeoutExpired(cmd="npm", timeout=5),
            0,
        ]
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        with patch("botfarm.devserver.os.getpgid", return_value=1234), \
             patch("botfarm.devserver.os.killpg") as mock_killpg:
            mgr.stop("web-app")

            assert mock_killpg.call_count == 2
            mock_killpg.assert_any_call(1234, signal.SIGTERM)
            mock_killpg.assert_any_call(1234, signal.SIGKILL)
            # proc.wait() called twice: once for SIGTERM timeout, once to reap after SIGKILL
            assert mock_proc.wait.call_count == 2

    def test_stop_noop_when_not_running(self, tmp_path):
        """stop() is a no-op for unknown project."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.stop("nonexistent")  # should not raise

    def test_stop_already_exited(self, tmp_path):
        """stop() handles process that already exited."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = 0  # already exited
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        with patch("botfarm.devserver.os.getpgid"), \
             patch("botfarm.devserver.os.killpg") as mock_killpg:
            mgr.stop("web-app")
            mock_killpg.assert_not_called()


class TestDevServerManagerStatus:
    def test_status_stopped(self, tmp_path):
        """status() returns 'stopped' for unknown project."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        result = mgr.status("web-app")
        assert result["status"] == "stopped"
        assert result["pid"] is None

    def test_status_running(self, tmp_path):
        """status() returns 'running' with uptime for live process."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = None
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        result = mgr.status("web-app")
        assert result["status"] == "running"
        assert result["pid"] == 1234
        assert result["uptime"] is not None
        assert result["uptime"] >= 0

    def test_status_crashed(self, tmp_path):
        """status() returns 'crashed' with exit code for dead process."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = 1
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        result = mgr.status("web-app")
        assert result["status"] == "crashed"
        assert result["exit_code"] == 1


class TestDevServerManagerHealthCheck:
    def test_health_check_detects_crash(self, tmp_path):
        """health_check() logs warning and removes crashed entry."""
        mgr = DevServerManager(log_dir=tmp_path / "logs", auto_restart=False)

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = 1  # crashed
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        mgr.health_check()
        # Crashed entry is removed to avoid repeated warnings
        assert "web-app" not in mgr._servers

    def test_health_check_closes_log_on_crash(self, tmp_path):
        """health_check() closes log file when removing crashed entry."""
        mgr = DevServerManager(log_dir=tmp_path / "logs", auto_restart=False)

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = 1
        info = _ServerInfo(mock_proc)
        mock_log = MagicMock()
        info.log_file = mock_log
        mgr._servers["web-app"] = info

        mgr.health_check()
        mock_log.close.assert_called_once()

    def test_health_check_auto_restarts(self, tmp_path):
        """health_check() auto-restarts crashed process when enabled."""
        project = _make_project(tmp_path)
        mgr = DevServerManager(log_dir=tmp_path / "logs", auto_restart=True)
        mgr.register_project(project)

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = 1  # crashed
        info = _ServerInfo(mock_proc, restart_count=2)
        mock_log = MagicMock()
        info.log_file = mock_log
        mgr._servers["web-app"] = info

        with patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            new_proc = MagicMock()
            new_proc.pid = 5678
            mock_popen.return_value = new_proc

            mgr.health_check()

            mock_popen.assert_called_once()
            assert mgr._servers["web-app"].restart_count == 3
            # Old log file was closed before restart
            mock_log.close.assert_called_once()
            assert mgr._servers["web-app"].pid == 5678

    def test_health_check_skips_running(self, tmp_path):
        """health_check() does nothing for running processes."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = None  # still running
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        mgr.health_check()
        assert mgr._servers["web-app"].pid == 1234


class TestDevServerManagerStopAll:
    def test_stop_all_stops_everything(self, tmp_path):
        """stop_all() stops all running dev servers."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        for name in ("app1", "app2"):
            mock_proc = MagicMock()
            mock_proc.pid = hash(name) % 10000
            mock_proc.poll.return_value = None
            mock_proc.wait.return_value = 0
            mgr._servers[name] = _ServerInfo(mock_proc)

        with patch("botfarm.devserver.os.getpgid", side_effect=lambda p: p), \
             patch("botfarm.devserver.os.killpg"):
            mgr.stop_all()

        assert len(mgr._servers) == 0


class TestDevServerManagerRestart:
    def test_restart_stops_then_starts(self, tmp_path):
        """restart() calls stop then start."""
        project = _make_project(tmp_path)
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.register_project(project)

        mock_proc = MagicMock()
        mock_proc.pid = 1234
        mock_proc.poll.return_value = None
        mock_proc.wait.return_value = 0
        mgr._servers["web-app"] = _ServerInfo(mock_proc)

        with patch("botfarm.devserver.os.getpgid", return_value=1234), \
             patch("botfarm.devserver.os.killpg"), \
             patch("botfarm.devserver.subprocess.Popen") as mock_popen:
            new_proc = MagicMock()
            new_proc.pid = 5678
            mock_popen.return_value = new_proc

            mgr.restart("web-app")

            assert mgr._servers["web-app"].pid == 5678


class TestDevServerManagerRegister:
    def test_register_skips_no_run_command(self, tmp_path):
        """register_project() skips projects without run_command."""
        project = ProjectConfig(
            name="no-cmd",
            base_dir=str(tmp_path),
            run_command="",
        )
        mgr = DevServerManager(log_dir=tmp_path / "logs")
        mgr.register_project(project)

        assert "no-cmd" not in mgr._projects


class TestRunningProjects:
    def test_running_projects_filters_correctly(self, tmp_path):
        """running_projects() returns only projects with live processes."""
        mgr = DevServerManager(log_dir=tmp_path / "logs")

        running = MagicMock()
        running.pid = 1
        running.poll.return_value = None

        crashed = MagicMock()
        crashed.pid = 2
        crashed.poll.return_value = 1

        mgr._servers["alive"] = _ServerInfo(running)
        mgr._servers["dead"] = _ServerInfo(crashed)

        assert mgr.running_projects() == ["alive"]


class TestSupervisorIntegration:
    """Verify supervisor creates / shuts down DevServerManager."""

    def test_shutdown_stops_devservers(self, tmp_path, monkeypatch):
        """Supervisor._shutdown() calls devserver_mgr.stop_all()."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setenv("LINEAR_API_KEY", "test-key")

        from tests.helpers import make_config
        config = make_config(tmp_path)

        with patch("botfarm.supervisor.create_pollers", return_value=[]), \
             patch("botfarm.supervisor.create_client"), \
             patch("botfarm.supervisor.UsagePoller"), \
             patch("botfarm.supervisor.CodexUsagePoller"), \
             patch("botfarm.supervisor.Notifier"), \
             patch("botfarm.supervisor.SlotManager"), \
             patch("botfarm.supervisor.sync_agent_config_to_db"), \
             patch("botfarm.supervisor.build_git_env", return_value={}), \
             patch("botfarm.supervisor.init_db") as mock_init_db:

            mock_conn = MagicMock()
            mock_init_db.return_value = mock_conn

            from botfarm.supervisor import Supervisor
            sup = Supervisor(config, log_dir=tmp_path / "logs")

            # Verify devserver manager exists
            assert hasattr(sup, '_devserver_mgr')
            assert isinstance(sup.devserver_manager, DevServerManager)

            # Mock the devserver manager to verify stop_all is called
            sup._devserver_mgr = MagicMock()

            # Suppress notification / event writes
            sup._notifier = MagicMock()
            sup._slot_manager = MagicMock()
            sup._usage_poller = MagicMock()
            sup._codex_usage_poller = MagicMock()

            sup._shutdown()

            sup._devserver_mgr.stop_all.assert_called_once()
