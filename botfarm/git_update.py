"""Git-based update helpers for the botfarm auto-update feature.

Provides functions to check how many commits the local HEAD is behind
``origin/main`` and to pull + reinstall the package.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# Exit code the supervisor uses to signal "restart after update"
UPDATE_EXIT_CODE = 42


def _subprocess_env(env: dict[str, str] | None) -> dict[str, str]:
    """Build subprocess environment for git commands in non-interactive contexts.

    Always sets ``GIT_TERMINAL_PROMPT=0`` to prevent git from prompting for
    HTTPS credentials (which would hang under systemd/cron/nohup).

    When ``GH_TOKEN`` is present, configures a credential helper via
    ``GIT_CONFIG_*`` env vars that bridges the token to git's HTTPS auth —
    fixing ``git fetch``/``git pull`` failures when system credential helpers
    (gnome-keyring, libsecret) are unavailable.

    When ``GIT_SSH_COMMAND`` is present, appends ``-o BatchMode=yes`` to
    prevent SSH from prompting for a passphrase.
    """
    merged = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    if env:
        merged.update(env)

    # Bridge GH_TOKEN → git credential helper for HTTPS remotes.
    if merged.get("GH_TOKEN"):
        base = int(merged.get("GIT_CONFIG_COUNT", "0"))
        # First: clear system credential helpers that may not work headless.
        merged[f"GIT_CONFIG_KEY_{base}"] = "credential.helper"
        merged[f"GIT_CONFIG_VALUE_{base}"] = ""
        # Second: set a helper that reads GH_TOKEN from the environment.
        merged[f"GIT_CONFIG_KEY_{base + 1}"] = "credential.helper"
        merged[f"GIT_CONFIG_VALUE_{base + 1}"] = (
            '!f() { echo "username=x-access-token"; '
            'echo "password=${GH_TOKEN}"; }; f'
        )
        merged["GIT_CONFIG_COUNT"] = str(base + 2)

    # Prevent SSH passphrase prompts in non-interactive environments.
    ssh_cmd = merged.get("GIT_SSH_COMMAND")
    if ssh_cmd and "-o BatchMode=" not in ssh_cmd:
        merged["GIT_SSH_COMMAND"] = f"{ssh_cmd} -o BatchMode=yes"

    return merged


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
    kwargs: dict = {"capture_output": True, "text": True, "timeout": 30}
    if repo_dir is not None:
        kwargs["cwd"] = str(repo_dir)
    kwargs["env"] = _subprocess_env(env)

    try:
        subprocess.run(
            ["git", "fetch", "origin"],
            check=True,
            **kwargs,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError):
        logger.debug("git fetch origin failed (update check is best-effort)", exc_info=True)
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
        logger.debug("git rev-list count failed", exc_info=True)
        return 0


def _ensure_not_bare(
    repo_dir: str | Path | None = None,
) -> None:
    """Check ``core.bare`` and set it to ``false`` if needed.

    Worktrees sharing a ``.git/config`` with the base repo can end up with
    ``core.bare = true`` after certain git operations, which breaks
    ``git pull``.  This helper detects and auto-repairs that condition.
    """
    kwargs: dict = {"capture_output": True, "text": True, "timeout": 5}
    if repo_dir is not None:
        kwargs["cwd"] = str(repo_dir)

    try:
        proc = subprocess.run(
            ["git", "config", "--get", "core.bare"],
            **kwargs,
        )
        if proc.returncode != 0 or proc.stdout.strip().lower() != "true":
            return
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return

    logger.warning("core.bare=true detected — setting to false before pull")
    try:
        subprocess.run(
            ["git", "config", "--local", "core.bare", "false"],
            check=True,
            **kwargs,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError) as exc:
        logger.error("Failed to unset core.bare: %s", exc)


def pull_and_install(
    repo_dir: str | Path | None = None,
    *,
    env: dict[str, str] | None = None,
) -> str:
    """Pull latest ``origin/main`` and reinstall the package.

    Runs ``git pull origin main`` then ``sys.executable -m pip install -e .``.
    Before pulling, ensures ``core.bare`` is not set to ``true``.

    When *env* is provided (e.g. ``GIT_SSH_COMMAND``), it is merged into
    the current environment for the subprocess calls.

    Returns an empty string on success, or an error description on failure.
    """
    _ensure_not_bare(repo_dir)

    kwargs: dict = {"capture_output": True, "text": True, "timeout": 120}
    if repo_dir is not None:
        kwargs["cwd"] = str(repo_dir)
    kwargs["env"] = _subprocess_env(env)

    try:
        subprocess.run(
            ["git", "pull", "origin", "main"],
            check=True,
            **kwargs,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError) as exc:
        logger.error("git pull failed: %s", exc)
        return f"git pull failed: {exc}"

    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-e", "."],
            check=True,
            **kwargs,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError) as exc:
        logger.error("pip install -e . failed: %s", exc)
        return f"pip install failed: {exc}"

    logger.info("Update complete: pulled origin/main and reinstalled package")
    return ""
