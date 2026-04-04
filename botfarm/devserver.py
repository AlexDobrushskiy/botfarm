"""Dev server process manager for web projects.

Manages dev server subprocesses (e.g. ``npm run dev``, ``python manage.py
runserver``) for projects that have a ``run_command`` configured.  Each
project gets at most one dev server process.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from pathlib import Path

from botfarm.config import ProjectConfig

logger = logging.getLogger(__name__)

DEFAULT_DEVSERVER_LOG_DIR = Path.home() / ".botfarm" / "logs" / "devserver"

# Grace period (seconds) before escalating SIGTERM → SIGKILL
_STOP_GRACE_SECONDS = 5.0


class _ServerInfo:
    """Mutable record for a running (or recently-crashed) dev server."""

    __slots__ = ("proc", "pid", "start_time", "restart_count", "log_file")

    def __init__(self, proc: subprocess.Popen, restart_count: int = 0) -> None:
        self.proc = proc
        self.pid = proc.pid
        self.start_time = time.monotonic()
        self.restart_count = restart_count
        self.log_file: object | None = None


class DevServerManager:
    """Manage dev server processes for web projects.

    Parameters
    ----------
    log_dir:
        Directory for per-project dev server log files.  Defaults to
        ``~/.botfarm/logs/devserver/``.
    auto_restart:
        If *True*, ``health_check`` will automatically restart crashed
        servers.  Off by default.
    """

    def __init__(
        self,
        *,
        log_dir: str | Path | None = None,
        auto_restart: bool = False,
    ) -> None:
        self._log_dir = Path(log_dir or DEFAULT_DEVSERVER_LOG_DIR)
        self._auto_restart = auto_restart
        # project_name -> _ServerInfo (only while running or just-crashed)
        self._servers: dict[str, _ServerInfo] = {}
        # project_name -> ProjectConfig (registered projects with run_command)
        self._projects: dict[str, ProjectConfig] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_project(self, project: ProjectConfig) -> None:
        """Register a project so it can be started later."""
        if project.run_command:
            self._projects[project.name] = project

    def unregister_project(self, project_name: str) -> None:
        """Remove a project registration (e.g. during rollback)."""
        self._projects.pop(project_name, None)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _close_log_file(info: _ServerInfo) -> None:
        """Close the log file handle on a _ServerInfo if present."""
        if info.log_file is not None:
            try:
                info.log_file.close()
            except Exception:
                pass

    def _do_start(self, project: ProjectConfig, restart_count: int) -> None:
        """Launch the dev server subprocess."""
        self._log_dir.mkdir(parents=True, exist_ok=True)

        log_path = self._log_dir / f"{project.name}.log"
        # Rotate: rename current log to .log.1
        if log_path.exists():
            rotated = self._log_dir / f"{project.name}.log.1"
            try:
                log_path.rename(rotated)
            except OSError:
                logger.warning("Failed to rotate dev server log for %s", project.name)

        # Build environment: inherit current env + run_env + PORT
        env = dict(os.environ)
        if project.run_env:
            env.update(project.run_env)
        if project.run_port:
            env["PORT"] = str(project.run_port)

        cwd = str(Path(project.base_dir).expanduser())

        log_file = open(log_path, "w")  # noqa: SIM115
        try:
            proc = subprocess.Popen(
                project.run_command,
                shell=True,
                cwd=cwd,
                env=env,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        except Exception:
            log_file.close()
            raise

        info = _ServerInfo(proc, restart_count=restart_count)
        info.log_file = log_file
        self._servers[project.name] = info
        logger.info(
            "Started dev server for %s (PID %d, cmd=%r, cwd=%s)",
            project.name, proc.pid, project.run_command, cwd,
        )

    def start(self, project_name: str) -> None:
        """Start the dev server for *project_name*.

        No-op if the server is already running.  Raises ``ValueError`` if
        the project has no ``run_command`` configured.
        """
        existing = self._servers.get(project_name)
        if existing is not None and existing.proc.poll() is None:
            logger.info("Dev server for %s already running (PID %d)", project_name, existing.pid)
            return

        project = self._projects.get(project_name)
        if project is None:
            raise ValueError(f"Project {project_name!r} not registered or has no run_command")

        restart_count = existing.restart_count if existing else 0
        if existing is not None:
            self._close_log_file(existing)
        self._do_start(project, restart_count)

    def _do_stop(self, info: _ServerInfo, project_name: str) -> None:
        """Terminate a dev server process group."""
        self._close_log_file(info)

        if info.proc.poll() is not None:
            logger.info("Dev server for %s already exited (PID %d)", project_name, info.pid)
            return

        logger.info("Stopping dev server for %s (PID %d)", project_name, info.pid)
        try:
            pgid = os.getpgid(info.proc.pid)
            os.killpg(pgid, signal.SIGTERM)
        except OSError:
            return
        try:
            info.proc.wait(timeout=_STOP_GRACE_SECONDS)
        except subprocess.TimeoutExpired:
            logger.warning("Dev server for %s did not stop gracefully — sending SIGKILL", project_name)
            try:
                os.killpg(pgid, signal.SIGKILL)
                info.proc.wait()  # reap the zombie
            except OSError:
                pass

    def stop(self, project_name: str) -> None:
        """Stop the dev server for *project_name*.

        Sends SIGTERM to the process group, waits a grace period, then
        SIGKILL.  No-op if not running.
        """
        info = self._servers.pop(project_name, None)
        if info is None:
            return
        self._do_stop(info, project_name)

    def restart(self, project_name: str) -> None:
        """Stop then start the dev server for *project_name*."""
        self.stop(project_name)
        self.start(project_name)

    def status(self, project_name: str) -> dict:
        """Return status dict for *project_name*."""
        info = self._servers.get(project_name)
        if info is None:
            return {"project": project_name, "status": "stopped", "pid": None,
                    "uptime": None, "restart_count": 0}

        poll = info.proc.poll()
        if poll is None:
            uptime = time.monotonic() - info.start_time
            return {"project": project_name, "status": "running", "pid": info.pid,
                    "uptime": uptime, "restart_count": info.restart_count}
        else:
            return {"project": project_name, "status": "crashed",
                    "pid": info.pid, "exit_code": poll,
                    "uptime": None, "restart_count": info.restart_count}

    def stop_all(self) -> None:
        """Stop all running dev servers (used during supervisor shutdown)."""
        names = list(self._servers.keys())
        for name in names:
            try:
                self.stop(name)
            except Exception:
                logger.exception("Error stopping dev server for %s", name)

    def health_check(self) -> None:
        """Check liveness of all tracked servers.

        Detects crashed processes and optionally restarts them if
        ``auto_restart`` is enabled.
        """
        for name in list(self._servers.keys()):
            info = self._servers.get(name)
            if info is None:
                continue
            poll = info.proc.poll()
            if poll is not None:
                logger.warning(
                    "Dev server for %s crashed (PID %d, exit code %s)",
                    name, info.pid, poll,
                )
                if self._auto_restart:
                    project = self._projects.get(name)
                    if project is not None:
                        logger.info("Auto-restarting dev server for %s", name)
                        restart_count = info.restart_count + 1
                        self._close_log_file(info)
                        self._servers.pop(name, None)
                        self._do_start(project, restart_count)
                    else:
                        self._close_log_file(info)
                        self._servers.pop(name, None)
                else:
                    # Remove crashed entry to avoid repeated warnings;
                    # close log file to prevent fd leak.
                    self._close_log_file(info)
                    self._servers.pop(name, None)

    @property
    def project_names(self) -> list[str]:
        """Return names of all registered projects."""
        return list(self._projects)

    def log_path(self, project_name: str) -> Path:
        """Return the log file path for a project's dev server."""
        return self._log_dir / f"{project_name}.log"

    def running_projects(self) -> list[str]:
        """Return names of projects with running dev servers."""
        return [
            name for name, info in self._servers.items()
            if info.proc.poll() is None
        ]
