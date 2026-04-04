"""QA environment cleanup after pipeline completion, failure, or timeout.

Cleans up resources that a QA agent may have started:
- Processes bound to the project's ``run_port``
- Docker containers started by the agent
- Orphaned Playwright / Chromium browser processes
- Optional user-configured teardown command
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
from pathlib import Path

from botfarm.config import ProjectConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _kill_port_holder(port: int, *, label: str, worktree_cwd: str | None = None) -> None:
    """Kill any process bound to *port* via ``fuser``, if it belongs to the worktree."""
    if worktree_cwd is None:
        logger.debug("[%s] No worktree cwd — skipping port %d cleanup", label, port)
        return
    try:
        result = subprocess.run(
            ["fuser", f"{port}/tcp"],
            capture_output=True, text=True, timeout=5,
        )
        pids_str = result.stdout.strip()
        if not pids_str:
            return

        pids = []
        for tok in pids_str.split():
            try:
                pids.append(int(tok))
            except ValueError:
                continue

        cwd_path = Path(worktree_cwd).resolve()
        for pid in pids:
            try:
                proc_cwd = Path(f"/proc/{pid}/cwd").resolve()
            except (OSError, ValueError):
                continue
            if proc_cwd == cwd_path or str(proc_cwd).startswith(str(cwd_path) + os.sep):
                try:
                    os.kill(pid, signal.SIGKILL)
                    logger.info("[%s] Killed PID %d holding port %d", label, pid, port)
                except OSError:
                    pass
            else:
                logger.debug(
                    "[%s] PID %d on port %d not owned by worktree — skipped",
                    label, pid, port,
                )
    except FileNotFoundError:
        logger.debug("[%s] fuser not available — skipping port cleanup", label)
    except Exception:
        logger.debug("[%s] Port cleanup for %d failed", label, port, exc_info=True)


def _kill_docker_compose(cwd: str, *, label: str) -> None:
    """Run ``docker compose down`` if a compose file exists in *cwd*."""
    compose_names = ("docker-compose.yml", "docker-compose.yaml", "compose.yml", "compose.yaml")
    has_compose = any((Path(cwd) / name).exists() for name in compose_names)
    if not has_compose:
        return
    try:
        result = subprocess.run(
            ["docker", "compose", "down", "--remove-orphans", "--timeout", "5"],
            cwd=cwd, capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            logger.warning(
                "[%s] docker compose down failed (exit %d) in %s: %s",
                label, result.returncode, cwd, result.stderr.strip(),
            )
        else:
            logger.info("[%s] Ran docker compose down in %s", label, cwd)
    except FileNotFoundError:
        logger.debug("[%s] docker not available — skipping compose cleanup", label)
    except Exception:
        logger.debug("[%s] docker compose down failed in %s", label, cwd, exc_info=True)


def _kill_orphan_browsers(cwd: str, *, label: str) -> None:
    """Kill orphaned Playwright/Chromium processes whose cwd is under *cwd*."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", "chromium|playwright"],
            capture_output=True, text=True, timeout=5,
        )
        pids_str = result.stdout.strip()
        if not pids_str:
            return

        cwd_path = Path(cwd).resolve()
        for line in pids_str.splitlines():
            try:
                pid = int(line.strip())
            except ValueError:
                continue
            try:
                proc_cwd = Path(f"/proc/{pid}/cwd").resolve()
            except (OSError, ValueError):
                continue
            if proc_cwd == cwd_path or str(proc_cwd).startswith(str(cwd_path) + os.sep):
                try:
                    os.kill(pid, signal.SIGKILL)
                    logger.info("[%s] Killed orphan browser PID %d", label, pid)
                except OSError:
                    pass
    except FileNotFoundError:
        logger.debug("[%s] pgrep not available — skipping browser cleanup", label)
    except Exception:
        logger.debug("[%s] Browser cleanup failed", label, exc_info=True)


def _run_teardown_command(command: str, *, cwd: str | None, label: str) -> None:
    """Run the user-configured teardown command."""
    try:
        result = subprocess.run(
            command, shell=True, cwd=cwd,
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            logger.warning(
                "[%s] qa_teardown_command failed (exit %d): %s — %s",
                label, result.returncode, command, result.stderr.strip(),
            )
        else:
            logger.info("[%s] Ran qa_teardown_command: %s", label, command)
    except subprocess.TimeoutExpired:
        logger.warning("[%s] qa_teardown_command timed out after 30s: %s", label, command)
    except Exception:
        logger.debug("[%s] qa_teardown_command failed: %s", label, command, exc_info=True)


def cleanup_qa_environment(
    project_cfg: ProjectConfig,
    slot_id: int,
    *,
    worktree_cwd: str | None = None,
) -> None:
    """Run all QA cleanup steps for a slot. Failures are logged, never raised.

    Parameters
    ----------
    project_cfg:
        The project configuration (used for ``run_port``, ``qa_teardown_command``).
    slot_id:
        Slot ID (for logging).
    worktree_cwd:
        Working directory of the slot's worktree.  Used as ``cwd`` for the
        teardown command and for docker-compose down.
    """
    label = f"{project_cfg.name}/{slot_id}"

    if project_cfg.run_port:
        _kill_port_holder(project_cfg.run_port, label=label, worktree_cwd=worktree_cwd)

    if worktree_cwd:
        _kill_docker_compose(worktree_cwd, label=label)
        _kill_orphan_browsers(worktree_cwd, label=label)

    if project_cfg.qa_teardown_command:
        _run_teardown_command(
            project_cfg.qa_teardown_command,
            cwd=worktree_cwd,
            label=label,
        )
