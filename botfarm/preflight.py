"""Pre-flight health checks run before the supervisor main loop.

Validates that the environment is correctly configured before entering
the main loop. All checks run and collect results so the operator gets
a single, actionable summary rather than one-error-at-a-time debugging.
"""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

from botfarm.codex import check_codex_available
from botfarm.worker_claude import check_claude_available
from botfarm.config import BotfarmConfig, JiraBugtrackerConfig
from botfarm.credentials import CredentialError, _load_token
from botfarm.db import SCHEMA_VERSION, resolve_db_path
from botfarm.bugtracker import BugtrackerError, create_client
from botfarm.systemd_service import check_installed_unit_stale

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Outcome of a single pre-flight check."""

    name: str
    passed: bool
    message: str
    critical: bool = True  # False = warning only, won't block startup


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def _resolve_remote_url(base: Path) -> str | None:
    """Return the URL for the 'origin' remote, or *None* on failure."""
    try:
        proc = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=str(base),
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def _describe_identity(config: BotfarmConfig, env: dict[str, str] | None) -> str:
    """Return a human-readable description of the SSH identity being used."""
    ssh_key_path = config.identities.coder.ssh_key_path
    if ssh_key_path:
        return f"coder SSH key ({Path(ssh_key_path).expanduser()})"
    if env and "GIT_SSH_COMMAND" in env:
        return "custom GIT_SSH_COMMAND"
    return "default SSH"


def check_git_repos(
    config: BotfarmConfig,
    *,
    env: dict[str, str] | None = None,
) -> list[CheckResult]:
    """Verify each project's base_dir exists and is a git repository.

    When *env* is provided (e.g. ``GIT_SSH_COMMAND``), it is merged into
    the current environment for the ``git ls-remote`` call.
    """
    subprocess_env = {**os.environ, **env} if env else None
    identity_desc = _describe_identity(config, env)
    results: list[CheckResult] = []
    for project in config.projects:
        base = Path(project.base_dir).expanduser()
        if not base.exists():
            results.append(CheckResult(
                name=f"git_repo:{project.name}",
                passed=False,
                message=f"base_dir does not exist: {base}",
            ))
            continue
        if not (base / ".git").exists():
            results.append(CheckResult(
                name=f"git_repo:{project.name}",
                passed=False,
                message=f"base_dir is not a git repository: {base}",
            ))
            continue
        # Resolve remote URL for richer error messages
        remote_url = _resolve_remote_url(base)
        url_display = f" ({remote_url})" if remote_url else ""
        # Check that git remote is reachable
        try:
            proc = subprocess.run(
                ["git", "ls-remote", "--exit-code", "origin"],
                cwd=str(base),
                capture_output=True,
                text=True,
                timeout=15,
                env=subprocess_env,
            )
            if proc.returncode != 0:
                raw_err = proc.stderr.strip()[:200]
                results.append(CheckResult(
                    name=f"git_repo:{project.name}",
                    passed=False,
                    message=(
                        f"git remote 'origin'{url_display} not reachable "
                        f"using {identity_desc}. "
                        f"Ensure the associated GitHub account has access "
                        f"to this repository. Raw error: {raw_err}"
                    ),
                ))
                continue
        except subprocess.TimeoutExpired:
            results.append(CheckResult(
                name=f"git_repo:{project.name}",
                passed=False,
                message=f"git ls-remote{url_display} timed out after 15s",
            ))
            continue
        except FileNotFoundError:
            results.append(CheckResult(
                name=f"git_repo:{project.name}",
                passed=False,
                message="git command not found",
            ))
            continue
        results.append(CheckResult(
            name=f"git_repo:{project.name}",
            passed=True,
            message=f"OK — {base}",
        ))
    return results


def check_worktree_dirs(config: BotfarmConfig) -> list[CheckResult]:
    """Verify worktree parent directories exist and are writable."""
    results: list[CheckResult] = []
    for project in config.projects:
        base = Path(project.base_dir).expanduser()
        # Worktrees are created as siblings of base_dir:
        # e.g. base_dir=~/project → worktrees at ~/project-slot-1, ~/project-slot-2
        # The parent dir of worktrees is the parent of base_dir.
        parent = base.parent
        if not parent.exists():
            results.append(CheckResult(
                name=f"worktree_dir:{project.name}",
                passed=False,
                message=f"worktree parent dir does not exist: {parent}",
            ))
            continue
        if not os.access(str(parent), os.W_OK):
            results.append(CheckResult(
                name=f"worktree_dir:{project.name}",
                passed=False,
                message=f"worktree parent dir is not writable: {parent}",
            ))
            continue
        results.append(CheckResult(
            name=f"worktree_dir:{project.name}",
            passed=True,
            message=f"OK — {parent}",
        ))
    return results


def check_linear_api(config: BotfarmConfig) -> list[CheckResult]:
    """Validate bugtracker API key, team existence, and configured status names.

    Supports per-project bugtracker overrides — each project is validated
    against its effective bugtracker config (project-level merged with global).
    """
    from botfarm.config import resolve_project_bugtracker

    results: list[CheckResult] = []
    if not config.bugtracker.api_key and not any(
        p.bugtracker and p.bugtracker.get("api_key") for p in config.projects
    ):
        results.append(CheckResult(
            name="linear_api",
            passed=False,
            message="Bugtracker API key is not set",
        ))
        return results

    # Check each project's team and statuses using its effective bugtracker config.
    # Cache clients and team states to avoid redundant API calls.
    client_cache: dict[tuple[str, str], BugtrackerClient] = {}  # (api_key, type) -> client
    checked_teams: dict[tuple[str, str], dict[str, str]] = {}  # (api_key, team) -> states
    for project in config.projects:
        bt = resolve_project_bugtracker(config.bugtracker, project)
        if not bt.api_key:
            results.append(CheckResult(
                name=f"bugtracker:{project.name}",
                passed=False,
                message=f"No API key for project '{project.name}'",
            ))
            continue

        team_key = project.team
        cache_key = (bt.api_key, team_key)
        if cache_key in checked_teams:
            team_states = checked_teams[cache_key]
        else:
            client_key = (bt.api_key, bt.type)
            if client_key not in client_cache:
                try:
                    client_cache[client_key] = create_client(bt_config=bt)
                except Exception as exc:
                    results.append(CheckResult(
                        name=f"bugtracker:{project.name}",
                        passed=False,
                        message=f"Cannot create client for project '{project.name}': {exc}",
                    ))
                    continue
            client = client_cache[client_key]
            try:
                team_states = client.get_team_states(team_key)
                checked_teams[cache_key] = team_states
            except BugtrackerError as exc:
                results.append(CheckResult(
                    name=f"linear_team:{team_key}",
                    passed=False,
                    message=f"Cannot reach team '{team_key}': {exc}",
                ))
                continue

            results.append(CheckResult(
                name=f"linear_team:{team_key}",
                passed=True,
                message=f"OK — team '{team_key}' found with {len(team_states)} states",
            ))

        # Verify configured status names exist in the team's workflow
        configured_statuses = {
            "todo_status": bt.todo_status,
            "in_progress_status": bt.in_progress_status,
            "done_status": bt.done_status,
            "in_review_status": bt.in_review_status,
        }
        for field, status_name in configured_statuses.items():
            if status_name not in team_states:
                results.append(CheckResult(
                    name=f"linear_status:{team_key}/{field}",
                    passed=False,
                    message=(
                        f"Status '{status_name}' (from bugtracker.{field}) "
                        f"not found in team '{team_key}'. "
                        f"Available: {sorted(team_states.keys())}"
                    ),
                ))
            else:
                results.append(CheckResult(
                    name=f"linear_status:{team_key}/{field}",
                    passed=True,
                    message=f"OK — '{status_name}'",
                ))

    return results


def check_credentials() -> list[CheckResult]:
    """Check that Claude OAuth credentials exist and can be loaded."""
    try:
        token = _load_token()
    except CredentialError as exc:
        return [CheckResult(
            name="claude_credentials",
            passed=False,
            message=f"Cannot load OAuth credentials: {exc}",
            critical=False,  # Supervisor can run without — limit checking disabled
        )]

    msg = "OK — token loaded"
    if token.expires_at:
        msg += f" (expires_at: {token.expires_at})"
    return [CheckResult(
        name="claude_credentials",
        passed=True,
        message=msg,
    )]


def check_claude_binary() -> list[CheckResult]:
    """Check that the ``claude`` binary is available on PATH.

    When ``claude`` is not found, probes ``~/.local/bin/claude`` (the
    default install location) to distinguish "not installed" from
    "installed but PATH not configured".
    """
    ok, msg = check_claude_available()
    if not ok:
        # Augment the message with recovery guidance when binary is missing.
        # Note: string coupling — relies on "not found" wording from check_claude_available().
        if "not found" in msg:
            local_bin = Path("~/.local/bin").expanduser()
            if (local_bin / "claude").exists():
                msg = (
                    "'claude' found at ~/.local/bin/claude but ~/.local/bin "
                    "is not in PATH. "
                    "Add it permanently: "
                    "echo 'export PATH=\"$HOME/.local/bin:$PATH\"' >> ~/.bashrc && source ~/.bashrc — "
                    "Or for nohup: PATH=$HOME/.local/bin:$PATH nohup botfarm run &"
                )
            else:
                msg = (
                    "'claude' not found on PATH and not present at "
                    "~/.local/bin/claude. "
                    "Install Claude Code: curl -fsSL https://claude.ai/install.sh | bash — "
                    "Then add ~/.local/bin to PATH if needed."
                )
        return [CheckResult(name="claude_binary", passed=False, message=msg)]
    return [CheckResult(name="claude_binary", passed=True, message=f"OK — {msg}")]


def check_database(config: BotfarmConfig) -> list[CheckResult]:
    """Verify DB file path is writable and schema version matches.

    The database path is resolved via :func:`resolve_db_path`, which
    defaults to ``~/.botfarm/botfarm.db`` when ``BOTFARM_DB_PATH`` is unset.
    """
    results: list[CheckResult] = []
    db_path = resolve_db_path()
    db_dir = db_path.parent

    if not db_dir.exists():
        # init_db will create it, just check the parent is writable
        grandparent = db_dir.parent
        if grandparent.exists() and not os.access(str(grandparent), os.W_OK):
            results.append(CheckResult(
                name="database",
                passed=False,
                message=f"Cannot create DB directory — parent not writable: {grandparent}",
            ))
            return results

    if db_dir.exists() and not os.access(str(db_dir), os.W_OK):
        results.append(CheckResult(
            name="database",
            passed=False,
            message=f"DB directory is not writable: {db_dir}",
        ))
        return results

    # If the DB file exists, check schema version
    if db_path.exists():
        try:
            with sqlite3.connect(str(db_path)) as conn:
                row = conn.execute(
                    "SELECT version FROM schema_version"
                ).fetchone()
        except sqlite3.Error:
            # New DB, missing table, or corrupt file — init_db will handle it
            row = None

        if row is not None:
            version = row[0]
            if version > SCHEMA_VERSION:
                results.append(CheckResult(
                    name="database",
                    passed=False,
                    message=(
                        f"DB schema version ({version}) is newer than "
                        f"expected ({SCHEMA_VERSION}) — cannot downgrade"
                    ),
                ))
                return results
            elif version < SCHEMA_VERSION:
                results.append(CheckResult(
                    name="database",
                    passed=True,
                    message=f"OK — DB at v{version}, will migrate to v{SCHEMA_VERSION}",
                ))
                return results

    results.append(CheckResult(
        name="database",
        passed=True,
        message=f"OK — {db_path}",
    ))
    return results


def check_config_consistency(config: BotfarmConfig) -> list[CheckResult]:
    """Verify slot IDs are globally unique per project."""
    results: list[CheckResult] = []

    # Check for duplicate slot IDs across projects
    seen_slots: dict[str, str] = {}  # "project:slot_id" -> project name
    for project in config.projects:
        for sid in project.slots:
            key = f"{project.name}:{sid}"
            if key in seen_slots:
                results.append(CheckResult(
                    name="config_consistency",
                    passed=False,
                    message=(
                        f"Duplicate slot {sid} in project '{project.name}'"
                    ),
                ))
            else:
                seen_slots[key] = project.name

    if not results:
        total_slots = sum(len(p.slots) for p in config.projects)
        results.append(CheckResult(
            name="config_consistency",
            passed=True,
            message=f"OK — {len(config.projects)} project(s), {total_slots} slot(s)",
        ))

    return results


def check_notifications_webhook(config: BotfarmConfig) -> list[CheckResult]:
    """Warn if notifications are configured but the webhook URL looks invalid."""
    url = config.notifications.webhook_url
    if not url:
        return []  # Notifications not configured — nothing to check

    if not url.startswith(("http://", "https://")):
        return [CheckResult(
            name="notifications_webhook",
            passed=False,
            message=f"Webhook URL does not look valid: {url[:80]}",
            critical=False,
        )]

    return [CheckResult(
        name="notifications_webhook",
        passed=True,
        message="OK — webhook URL configured",
        critical=False,
    )]


def check_identity_ssh_key(config: BotfarmConfig) -> list[CheckResult]:
    """Validate coder SSH key if configured."""
    results: list[CheckResult] = []
    ssh_key_path = config.identities.coder.ssh_key_path
    if not ssh_key_path:
        return results  # Not configured — nothing to check

    key_path = Path(ssh_key_path).expanduser()

    # File must exist (critical)
    if not key_path.exists():
        results.append(CheckResult(
            name="identity_ssh_key",
            passed=False,
            message=f"SSH key file does not exist: {key_path}",
        ))
        return results

    # File permissions should be 600 (warn only)
    try:
        mode = key_path.stat().st_mode
        if mode & (stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH):
            results.append(CheckResult(
                name="identity_ssh_key:permissions",
                passed=False,
                message=(
                    f"SSH key file has overly permissive mode "
                    f"{oct(stat.S_IMODE(mode))} — expected 0o600: {key_path}"
                ),
                critical=False,
            ))
    except OSError as exc:
        results.append(CheckResult(
            name="identity_ssh_key:permissions",
            passed=False,
            message=f"Cannot stat SSH key file: {exc}",
            critical=False,
        ))

    # File should look like a valid SSH private key (critical)
    try:
        with open(key_path) as f:
            first_line = f.readline()
        if not first_line.startswith("-----BEGIN"):
            results.append(CheckResult(
                name="identity_ssh_key",
                passed=False,
                message=f"SSH key file does not look like a private key: {key_path}",
            ))
            return results
    except (OSError, UnicodeDecodeError) as exc:
        results.append(CheckResult(
            name="identity_ssh_key",
            passed=False,
            message=f"Cannot read SSH key file: {exc}",
        ))
        return results

    # Test SSH connectivity to GitHub (warn only)
    try:
        proc = subprocess.run(
            ["ssh", "-T", "-o", "StrictHostKeyChecking=accept-new",
             "-o", "BatchMode=yes", "-i", str(key_path), "git@github.com"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        # ssh -T git@github.com returns exit code 1 on success with a greeting
        if proc.returncode not in (0, 1) or "successfully authenticated" not in proc.stderr.lower():
            results.append(CheckResult(
                name="identity_ssh_key:github_ssh",
                passed=False,
                message=f"SSH to GitHub failed: {proc.stderr.strip()[:200]}",
                critical=False,
            ))
        else:
            results.append(CheckResult(
                name="identity_ssh_key:github_ssh",
                passed=True,
                message="OK — SSH connectivity to GitHub verified",
                critical=False,
            ))
    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        results.append(CheckResult(
            name="identity_ssh_key:github_ssh",
            passed=False,
            message=f"SSH connectivity test failed: {exc}",
            critical=False,
        ))

    if not any(r.name == "identity_ssh_key" and not r.passed for r in results):
        results.append(CheckResult(
            name="identity_ssh_key",
            passed=True,
            message=f"OK — {key_path}",
        ))

    return results


def check_identity_github_tokens(config: BotfarmConfig) -> list[CheckResult]:
    """Validate GitHub tokens for coder and reviewer identities."""
    results: list[CheckResult] = []

    tokens = [
        ("coder", config.identities.coder.github_token),
        ("reviewer", config.identities.reviewer.github_token),
    ]

    for role, token in tokens:
        if not token:
            continue  # Not configured — skip

        # Test API access with gh api user
        try:
            proc = subprocess.run(
                ["gh", "api", "user"],
                capture_output=True,
                text=True,
                timeout=15,
                env={**os.environ, "GH_TOKEN": token},
            )
            if proc.returncode != 0:
                results.append(CheckResult(
                    name=f"identity_github_token:{role}",
                    passed=False,
                    message=f"GitHub token for {role} failed API check: {proc.stderr.strip()[:200]}",
                ))
            else:
                results.append(CheckResult(
                    name=f"identity_github_token:{role}",
                    passed=True,
                    message=f"OK — {role} GitHub token verified",
                ))
        except subprocess.TimeoutExpired:
            results.append(CheckResult(
                name=f"identity_github_token:{role}",
                passed=False,
                message=f"GitHub API check for {role} timed out after 15s",
            ))
        except FileNotFoundError:
            results.append(CheckResult(
                name=f"identity_github_token:{role}",
                passed=False,
                message=f"gh command not found — cannot check {role} GitHub token",
            ))

    return results


def check_identity_linear_api_key(config: BotfarmConfig) -> list[CheckResult]:
    """Validate coder and reviewer tracker API keys if configured."""
    results: list[CheckResult] = []
    is_jira = isinstance(config.bugtracker, JiraBugtrackerConfig)
    tracker_name = "Jira" if is_jira else "Linear"

    if is_jira:
        keys: list[tuple[str, str, str | None]] = [
            ("coder", config.identities.coder.jira_api_token,
             config.identities.coder.jira_email or config.bugtracker.email),
            ("reviewer", config.identities.reviewer.tracker_api_key, None),
        ]
    else:
        keys = [
            ("coder", config.identities.coder.tracker_api_key, None),
            ("reviewer", config.identities.reviewer.tracker_api_key, None),
        ]

    for role, api_key, email_override in keys:
        if not api_key:
            continue  # Not configured — skip

        client = create_client(config, api_key=api_key, email=email_override)
        try:
            client.get_viewer_id()
            results.append(CheckResult(
                name=f"identity_tracker_key:{role}",
                passed=True,
                message=f"OK — {role} {tracker_name} API key verified",
            ))
        except BugtrackerError as exc:
            results.append(CheckResult(
                name=f"identity_tracker_key:{role}",
                passed=False,
                message=f"{role.capitalize()} {tracker_name} API key failed: {exc}",
            ))

    return results


def check_core_bare(config: BotfarmConfig) -> list[CheckResult]:
    """Verify core.bare is not set to true in each project's base repo.

    The shared .git/config can get corrupted by worktree edge cases,
    causing ``git status`` to fail with "this operation must be run in
    a work tree".  Severity: error (blocks startup).
    """
    results: list[CheckResult] = []
    for project in config.projects:
        base = Path(project.base_dir).expanduser()
        if not base.exists() or not (base / ".git").exists():
            continue  # check_git_repos will report this
        try:
            proc = subprocess.run(
                ["git", "config", "--get", "core.bare"],
                cwd=str(base),
                capture_output=True,
                text=True,
                timeout=5,
            )
            value = proc.stdout.strip().lower()
            if value == "true":
                results.append(CheckResult(
                    name=f"core_bare:{project.name}",
                    passed=False,
                    message=(
                        f"core.bare=true in {base} — git operations will fail. "
                        f"Fix manually: git -C {base} config --local core.bare false"
                    ),
                ))
            else:
                results.append(CheckResult(
                    name=f"core_bare:{project.name}",
                    passed=True,
                    message=f"OK — core.bare is not true in {base}",
                ))
        except (subprocess.TimeoutExpired, FileNotFoundError):
            results.append(CheckResult(
                name=f"core_bare:{project.name}",
                passed=False,
                message=f"Failed to check core.bare in {base}",
            ))
    return results


def repair_core_bare(config: BotfarmConfig) -> list[str]:
    """Detect and auto-fix core.bare=true in each project's base repo.

    Returns a list of project names that were repaired.
    Called on supervisor startup before preflight checks.
    """
    repaired: list[str] = []
    for project in config.projects:
        base = Path(project.base_dir).expanduser()
        if not base.exists() or not (base / ".git").exists():
            continue
        try:
            proc = subprocess.run(
                ["git", "config", "--get", "core.bare"],
                cwd=str(base),
                capture_output=True,
                text=True,
                timeout=5,
            )
            if proc.stdout.strip().lower() != "true":
                continue
        except (subprocess.TimeoutExpired, FileNotFoundError):
            continue

        logger.warning(
            "core.bare=true detected in %s — auto-repairing", base,
        )
        try:
            subprocess.run(
                ["git", "config", "--local", "core.bare", "false"],
                cwd=str(base),
                capture_output=True,
                text=True,
                timeout=5,
                check=True,
            )
            repaired.append(project.name)
            logger.warning(
                "Repaired core.bare for project %s (%s)", project.name, base,
            )
        except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError) as exc:
            logger.error(
                "Failed to repair core.bare for project %s: %s",
                project.name, exc,
            )
    return repaired


def check_codex_reviewer(config: BotfarmConfig) -> list[CheckResult]:
    """Validate Codex reviewer prerequisites when enabled."""
    if not config.agents.codex_reviewer_enabled:
        return []

    results: list[CheckResult] = []

    codex_ok, codex_msg = check_codex_available()
    if not codex_ok:
        results.append(CheckResult(
            name="codex_reviewer:binary",
            passed=False,
            message=f"{codex_msg} — install Codex or disable codex_reviewer_enabled",
        ))
    else:
        results.append(CheckResult(
            name="codex_reviewer:binary",
            passed=True,
            message=f"OK — {codex_msg}",
        ))

    has_api_key = bool(os.environ.get("OPENAI_API_KEY"))
    has_auth_file = Path("~/.codex/auth.json").expanduser().exists()
    if not has_api_key and not has_auth_file:
        results.append(CheckResult(
            name="codex_reviewer:auth",
            passed=False,
            message=(
                "OPENAI_API_KEY is not set and ~/.codex/auth.json not found — "
                "Codex requires one of these for authentication"
            ),
        ))
    else:
        results.append(CheckResult(
            name="codex_reviewer:auth",
            passed=True,
            message="OK — Codex authentication available",
        ))

    return results


def check_jira_mcp_server(config: BotfarmConfig) -> list[CheckResult]:
    """Warn if Jira is configured but ``uvx`` is not available.

    The Jira MCP server (``mcp-atlassian``) is launched via ``uvx``.
    """
    from botfarm.config import resolve_project_bugtracker
    has_jira = isinstance(config.bugtracker, JiraBugtrackerConfig) or any(
        isinstance(resolve_project_bugtracker(config.bugtracker, p), JiraBugtrackerConfig)
        for p in config.projects
    )
    if not has_jira:
        return []

    if shutil.which("uvx"):
        return [CheckResult(
            name="jira_mcp_server",
            passed=True,
            message="OK — uvx is available for mcp-atlassian",
            critical=False,
        )]

    return [CheckResult(
        name="jira_mcp_server",
        passed=False,
        message=(
            "'uvx' not found on PATH — Jira MCP tools will not be available to agents. "
            "Install uv (https://docs.astral.sh/uv/getting-started/installation/) "
            "to enable Jira MCP support via mcp-atlassian."
        ),
        critical=False,
    )]


def check_systemd_unit() -> list[CheckResult]:
    """Warn if the installed systemd unit file has stale flags."""
    is_stale, message = check_installed_unit_stale()
    if is_stale:
        return [CheckResult(
            name="systemd_unit",
            passed=False,
            message=message,
            critical=False,
        )]
    return []


def check_project_claude_md(config: BotfarmConfig) -> list[CheckResult]:
    """Warn if any project is missing a CLAUDE.md file."""
    results: list[CheckResult] = []
    for project in config.projects:
        base = Path(project.base_dir).expanduser()
        claude_md = base / "CLAUDE.md"
        if claude_md.exists():
            results.append(CheckResult(
                name=f"project_claude_md:{project.name}",
                passed=True,
                message=f"OK — {claude_md}",
                critical=False,
            ))
        else:
            results.append(CheckResult(
                name=f"project_claude_md:{project.name}",
                passed=False,
                message=(
                    f"Project '{project.name}' has no CLAUDE.md — agent will "
                    "lack project-specific instructions (test commands, "
                    "architecture, conventions)"
                ),
                critical=False,
            ))
    return results


# Language marker files → (language name, version command)
_LANGUAGE_MARKERS: list[tuple[str, str, list[str]]] = [
    ("requirements.txt", "Python", ["python3", "--version"]),
    ("pyproject.toml", "Python", ["python3", "--version"]),
    ("setup.py", "Python", ["python3", "--version"]),
    ("package.json", "Node.js", ["node", "--version"]),
    ("go.mod", "Go", ["go", "version"]),
    ("Cargo.toml", "Rust", ["cargo", "--version"]),
    ("Gemfile", "Ruby", ["ruby", "--version"]),
]


def detect_runtimes(base_dir: Path) -> list[tuple[str, str, str]]:
    """Detect project languages and check if runtimes are available.

    Returns a list of ``(level, language, message)`` tuples where *level*
    is ``"ok"`` or ``"warning"``.  This helper is shared by
    :func:`check_project_runtimes` (preflight) and the ``add-project``
    CLI command.
    """
    results: list[tuple[str, str, str]] = []
    checked_languages: set[str] = set()
    for marker_file, language, version_cmd in _LANGUAGE_MARKERS:
        if language in checked_languages:
            continue
        if not (base_dir / marker_file).exists():
            continue
        checked_languages.add(language)
        try:
            proc = subprocess.run(
                version_cmd,
                capture_output=True,
                text=True,
                timeout=10,
            )
            if proc.returncode == 0:
                version = proc.stdout.strip() or proc.stderr.strip()
                results.append(("ok", language, version))
            else:
                results.append((
                    "warning",
                    language,
                    f"{language} project detected (found {marker_file}) but "
                    f"`{version_cmd[0]}` returned exit code {proc.returncode}",
                ))
        except FileNotFoundError:
            results.append((
                "warning",
                language,
                f"{language} project detected (found {marker_file}) but "
                f"`{version_cmd[0]}` not found in PATH",
            ))
        except subprocess.TimeoutExpired:
            results.append((
                "warning",
                language,
                f"`{version_cmd[0]}` timed out checking {language} runtime",
            ))
    return results


def check_project_runtimes(config: BotfarmConfig) -> list[CheckResult]:
    """Detect project languages and warn if the runtime is not installed."""
    results: list[CheckResult] = []
    for project in config.projects:
        base = Path(project.base_dir).expanduser()
        for level, language, message in detect_runtimes(base):
            results.append(CheckResult(
                name=f"project_runtime:{project.name}/{language}",
                passed=(level == "ok"),
                message=f"OK — {message}" if level == "ok" else message,
                critical=False,
            ))
    return results


def check_identity_cross_validation(config: BotfarmConfig) -> list[CheckResult]:
    """Warn about potentially inconsistent identity configuration."""
    results: list[CheckResult] = []
    coder = config.identities.coder
    reviewer = config.identities.reviewer

    # SSH key set but no GitHub token (or vice versa)
    if coder.ssh_key_path and not coder.github_token:
        results.append(CheckResult(
            name="identity_cross:coder_partial",
            passed=False,
            message="Coder SSH key is set but GitHub token is not — partial config may cause issues",
            critical=False,
        ))
    if coder.github_token and not coder.ssh_key_path:
        results.append(CheckResult(
            name="identity_cross:coder_partial",
            passed=False,
            message="Coder GitHub token is set but SSH key is not — partial config may cause issues",
            critical=False,
        ))

    # Reviewer token equals coder token
    if (reviewer.github_token and coder.github_token
            and reviewer.github_token == coder.github_token):
        results.append(CheckResult(
            name="identity_cross:same_github_token",
            passed=False,
            message="Reviewer and coder GitHub tokens are identical — should be different accounts",
            critical=False,
        ))

    return results


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def run_preflight_checks(
    config: BotfarmConfig,
    *,
    env: dict[str, str] | None = None,
) -> list[CheckResult]:
    """Run all pre-flight checks and return collected results.

    When *env* is provided, it is forwarded to checks that run git
    subprocesses (e.g. ``check_git_repos``).
    """
    results: list[CheckResult] = []
    results.extend(check_config_consistency(config))
    results.extend(check_database(config))
    results.extend(check_git_repos(config, env=env))
    results.extend(check_core_bare(config))
    results.extend(check_worktree_dirs(config))
    results.extend(check_linear_api(config))
    results.extend(check_credentials())
    results.extend(check_claude_binary())
    results.extend(check_notifications_webhook(config))
    results.extend(check_identity_ssh_key(config))
    results.extend(check_identity_github_tokens(config))
    results.extend(check_identity_linear_api_key(config))
    results.extend(check_identity_cross_validation(config))
    results.extend(check_project_claude_md(config))
    results.extend(check_project_runtimes(config))
    results.extend(check_codex_reviewer(config))
    results.extend(check_jira_mcp_server(config))
    results.extend(check_systemd_unit())
    return results


def log_preflight_summary(results: list[CheckResult]) -> bool:
    """Log a summary of pre-flight check results.

    Returns True if all critical checks passed (startup can proceed).
    """
    critical_failures = [r for r in results if not r.passed and r.critical]
    warnings = [r for r in results if not r.passed and not r.critical]
    passed = [r for r in results if r.passed]

    logger.info("Pre-flight checks: %d passed, %d failed, %d warnings",
                len(passed), len(critical_failures), len(warnings))

    for r in results:
        if r.passed:
            logger.info("  [PASS] %-40s %s", r.name, r.message)
        elif r.critical:
            logger.error("  [FAIL] %-40s %s", r.name, r.message)
        else:
            logger.warning("  [WARN] %-40s %s", r.name, r.message)

    if critical_failures:
        logger.error(
            "Pre-flight failed: %d critical check(s) did not pass. "
            "Fix the issues above and restart.",
            len(critical_failures),
        )
        return False

    if warnings:
        logger.warning(
            "Pre-flight passed with %d warning(s) — proceeding.",
            len(warnings),
        )
    else:
        logger.info("Pre-flight passed — all checks OK.")

    return True
