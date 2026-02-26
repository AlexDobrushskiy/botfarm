"""Git-based update helpers for the botfarm auto-update feature.

Provides functions to check how many commits the local HEAD is behind
``origin/main`` and to pull + reinstall the package.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Exit code the supervisor uses to signal "restart after update"
UPDATE_EXIT_CODE = 42


def commits_behind(
    repo_dir: str | Path | None = None,
    *,
    env: dict[str, str] | None = None,
) -> int:
    """Return the number of commits HEAD is behind ``origin/main``.

    Runs ``git fetch origin`` followed by
    ``git rev-list HEAD..origin/main --count``.

    When *env* is provided (e.g. ``GIT_SSH_COMMAND``), it is merged into
    the current environment for the subprocess calls.

    Returns 0 when already up-to-date or on any git error.
    """
    import os as _os

    kwargs: dict = {"capture_output": True, "text": True, "timeout": 30}
    if repo_dir is not None:
        kwargs["cwd"] = str(repo_dir)
    if env:
        kwargs["env"] = {**_os.environ, **env}

    try:
        subprocess.run(
            ["git", "fetch", "origin"],
            check=True,
            **kwargs,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        logger.warning("git fetch origin failed", exc_info=True)
        return 0

    try:
        result = subprocess.run(
            ["git", "rev-list", "HEAD..origin/main", "--count"],
            check=True,
            **kwargs,
        )
        return int(result.stdout.strip())
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError, ValueError):
        logger.warning("git rev-list count failed", exc_info=True)
        return 0


def pull_and_install(
    repo_dir: str | Path | None = None,
    *,
    env: dict[str, str] | None = None,
) -> bool:
    """Pull latest ``origin/main`` and reinstall the package.

    Runs ``git pull origin main`` then ``sys.executable -m pip install -e .``.

    When *env* is provided (e.g. ``GIT_SSH_COMMAND``), it is merged into
    the current environment for the subprocess calls.

    Returns ``True`` on success, ``False`` on any error.
    """
    import os as _os

    kwargs: dict = {"capture_output": True, "text": True, "timeout": 120}
    if repo_dir is not None:
        kwargs["cwd"] = str(repo_dir)
    if env:
        kwargs["env"] = {**_os.environ, **env}

    try:
        subprocess.run(
            ["git", "pull", "origin", "main"],
            check=True,
            **kwargs,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError) as exc:
        logger.error("git pull failed: %s", exc)
        return False

    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", "."],
            check=True,
            **kwargs,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError) as exc:
        logger.error("pip install -e . failed: %s", exc)
        return False

    logger.info("Update complete: pulled origin/main and reinstalled package")
    return True
