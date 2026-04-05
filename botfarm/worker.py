"""Worker pipeline: implement → review → fix → pr_checks → merge.

Each stage that requires Claude Code spawns a subprocess via
``claude -p --output-format json --dangerously-skip-permissions --max-turns N``.
The JSON output is parsed for metrics and stored in the ``stage_runs`` table.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import re
import shutil
import sqlite3
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from botfarm.agent import AdapterRegistry, AgentAdapter, build_adapter_registry
from botfarm.codex import run_codex_streaming as run_codex_streaming
from botfarm.config import CODEX_ADAPTER_DEFAULTS, AdapterConfig, IdentitiesConfig
from botfarm.db import delete_stage_run, get_task, insert_event, insert_stage_run, is_extra_usage_active, read_runtime_config, update_stage_run_context_fill, update_task
from botfarm.slots import update_slot_stage
from botfarm.worker_claude import (
    ClaudeResult,
    ContextFillCallback,
    StageResult,
    _detect_no_pr_needed,
    _make_stage_log_path,
    _parse_review_approved,
    _recover_pr_url,
    run_claude_streaming,
    # Re-exported for backward compatibility (external modules import from botfarm.worker)
    DEFAULT_CONTEXT_WINDOW as DEFAULT_CONTEXT_WINDOW,
    DETAIL_TRUNCATE_CHARS as DETAIL_TRUNCATE_CHARS,
    RESULT_TRUNCATE_CHARS as RESULT_TRUNCATE_CHARS,
    _check_pr_merged as _check_pr_merged,
    _compute_context_fill as _compute_context_fill,
    _compute_turn_context_fill as _compute_turn_context_fill,
    CI_OUTPUT_TRUNCATE_CHARS as CI_OUTPUT_TRUNCATE_CHARS,
    _extract_pr_url as _extract_pr_url,
    _parse_pr_url as _parse_pr_url,
    _write_subprocess_log as _write_subprocess_log,
    parse_claude_output as parse_claude_output,
    parse_stream_json_result as parse_stream_json_result,
)
from botfarm.worker_stages import (
    _check_pr_has_merge_conflict,
    _execute_stage,
    _is_investigation,
    _is_merge_conflict,
    _run_ci_fix,
    # Re-exported for backward compatibility
    DEFAULT_PR_CHECKS_TIMEOUT as DEFAULT_PR_CHECKS_TIMEOUT,
    _NO_PR_LABELS as _NO_PR_LABELS,
    _build_claude_review_prompt as _build_claude_review_prompt,
    _build_codex_review_prompt as _build_codex_review_prompt,
    _build_implement_prompt as _build_implement_prompt,
    _merge_review_verdicts as _merge_review_verdicts,
    _run_codex_review as _run_codex_review,
    _run_fix as _run_fix,
    _run_implement as _run_implement,
    _run_merge as _run_merge,
    _run_pr_checks as _run_pr_checks,
    _run_resolve_conflict as _run_resolve_conflict,
    _run_review as _run_review,
    _submit_aggregate_review as _submit_aggregate_review,
    is_protected_branch as is_protected_branch,
    _run_claude_stage as _run_claude_stage,
)
from botfarm.workflow import (
    PipelineTemplate,
    StageLoop,
    StageTemplate,
    get_loop_for_stage,
    get_stage,
    load_pipeline,
    load_pipeline_by_id,
    render_prompt as render_prompt,
    resolve_max_iterations,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module constants — extracted magic numbers
# ---------------------------------------------------------------------------

# Max turns for Claude stages (default when not overridden by pipeline template)
DEFAULT_IMPLEMENT_MAX_TURNS = 200
DEFAULT_REVIEW_MAX_TURNS = 100
DEFAULT_FIX_MAX_TURNS = 100

# Truncation limits and other constants now live in worker_claude.py
# and worker_stages.py; imported above for backward compatibility.

# ---------------------------------------------------------------------------
# Stage names — backward-compatible constant
# ---------------------------------------------------------------------------

# STAGES is kept as a module-level constant for backward compatibility
# (supervisor.py imports it for stage-aware recovery).  It reflects the
# *main* stages of the default implementation pipeline.  The actual stage
# ordering is now driven by pipeline_templates / stage_templates in the DB.
STAGES = ("implement", "review", "fix", "pr_checks", "merge")


# Backward-compatible identity sets — kept for tests and external callers.
# New code should use StageTemplate.identity from the pipeline template.
_CODER_STAGES = frozenset({"implement", "fix", "ci_fix", "pr_checks", "merge", "resolve_conflict"})
_REVIEWER_STAGES = frozenset({"review"})

# Default max-turns per stage — kept for backward compatibility.
# New code should use StageTemplate.max_turns from the pipeline template.
DEFAULT_MAX_TURNS: dict[str, int] = {
    "implement": DEFAULT_IMPLEMENT_MAX_TURNS,
    "review": DEFAULT_REVIEW_MAX_TURNS,
    "fix": DEFAULT_FIX_MAX_TURNS,
}


def _derive_stages(pipeline: PipelineTemplate) -> tuple[str, ...]:
    """Derive the ordered stage-name tuple from a loaded pipeline."""
    return tuple(s.name for s in pipeline.stages)


def _build_turns_cfg(pipeline: PipelineTemplate, overrides: dict[str, int] | None = None) -> dict[str, int]:
    """Build a max-turns dict from pipeline stage templates + caller overrides."""
    cfg: dict[str, int] = {}
    for s in pipeline.stages:
        if s.max_turns is not None:
            cfg[s.name] = s.max_turns
    if overrides:
        cfg.update(overrides)
    return cfg


def _build_ssh_command(key_path_str: str) -> str:
    """Return a ``GIT_SSH_COMMAND`` value for the given SSH key path."""
    key_path = Path(key_path_str).expanduser()
    return (
        f"ssh -i {key_path} "
        "-o IdentitiesOnly=yes "
        "-o StrictHostKeyChecking=accept-new "
        "-o ControlMaster=no "
        "-o ControlPath=none"
    )


def build_git_env(identities: IdentitiesConfig) -> dict[str, str] | None:
    """Build env dict with coder SSH key and GH_TOKEN for supervisor-level git operations.

    Returns ``None`` if neither SSH key nor GitHub token is configured.
    This is for supervisor operations (git fetch, git ls-remote, gh pr view)
    — not for worker Claude subprocesses (use :func:`build_coder_env` for those).
    """
    coder = identities.coder
    env: dict[str, str] = {}
    if coder.ssh_key_path:
        env["GIT_SSH_COMMAND"] = _build_ssh_command(coder.ssh_key_path)
    if coder.github_token:
        env["GH_TOKEN"] = coder.github_token
    return env if env else None


def build_coder_env(
    identities: IdentitiesConfig,
    slot_db_path: str | Path | None = None,
) -> dict[str, str]:
    """Build environment overrides for coder stages (implement, fix, pr_checks, merge)."""
    env: dict[str, str] = {}
    if slot_db_path:
        env["BOTFARM_DB_PATH"] = str(slot_db_path)

    identity = identities.coder
    if identity.ssh_key_path:
        env["GIT_SSH_COMMAND"] = _build_ssh_command(identity.ssh_key_path)
    if identity.github_token:
        env["GH_TOKEN"] = identity.github_token
    if identity.git_author_name:
        env["GIT_AUTHOR_NAME"] = identity.git_author_name
        env["GIT_COMMITTER_NAME"] = identity.git_author_name
    if identity.git_author_email:
        env["GIT_AUTHOR_EMAIL"] = identity.git_author_email
        env["GIT_COMMITTER_EMAIL"] = identity.git_author_email
    return env


def build_reviewer_env(
    identities: IdentitiesConfig,
    slot_db_path: str | Path | None = None,
) -> dict[str, str]:
    """Build environment overrides for reviewer stages (review)."""
    env: dict[str, str] = {}
    if slot_db_path:
        env["BOTFARM_DB_PATH"] = str(slot_db_path)

    reviewer_identity = identities.reviewer
    if reviewer_identity.github_token:
        env["GH_TOKEN"] = reviewer_identity.github_token
    return env


def build_bugtracker_mcp_config(
    bugtracker_type: str,
    api_key: str,
    *,
    bugtracker_url: str = "",
    jira_username: str = "",
) -> str:
    """Generate MCP config JSON for the bugtracker.

    Returns inline JSON string suitable for ``--mcp-config``.
    Supports Linear and Jira; other trackers return an empty string.

    For Jira, *bugtracker_url* is the full Jira instance URL (e.g.
    ``"https://acme.atlassian.net"``) and *jira_username* is the email
    used for API authentication.
    """

    bt = bugtracker_type.lower()
    if bt == "linear":
        config = {
            "mcpServers": {
                "linear": {
                    "command": "npx",
                    "args": ["-y", "@tacticlaunch/mcp-linear"],
                    "env": {"LINEAR_API_TOKEN": api_key},
                }
            }
        }
        return json.dumps(config)
    if bt == "jira":
        if not bugtracker_url or not jira_username:
            logger.warning(
                "Jira MCP config skipped: missing %s",
                "bugtracker_url" if not bugtracker_url else "jira_username",
            )
            return ""
        config = {
            "mcpServers": {
                "jira": {
                    "command": "uvx",
                    "args": ["mcp-atlassian"],
                    "env": {
                        "JIRA_URL": bugtracker_url,
                        "JIRA_USERNAME": jira_username,
                        "JIRA_API_TOKEN": api_key,
                    },
                }
            }
        }
        return json.dumps(config)
    return ""


def _merge_mcp_configs(base_config: str, extra_servers: dict) -> str:
    """Merge additional MCP servers into an existing MCP config JSON string.

    *base_config* is a JSON string (as returned by
    ``build_bugtracker_mcp_config``).  *extra_servers* is a dict of
    ``{server_name: server_config}`` to add under ``mcpServers``.
    If *base_config* is empty or invalid JSON, returns a new config
    containing only *extra_servers*.
    """
    if base_config:
        try:
            merged = json.loads(base_config)
        except json.JSONDecodeError:
            merged = {"mcpServers": {}}
    else:
        merged = {"mcpServers": {}}
    merged.setdefault("mcpServers", {}).update(extra_servers)
    return json.dumps(merged)


_PLAYWRIGHT_MCP_SERVER = {
    "playwright": {
        "command": "npx",
        "args": ["-y", "@anthropic/mcp-playwright"],
    }
}
"""Playwright MCP server config.  Requires Node.js to be installed at runtime."""


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """Aggregated outcome of a full worker pipeline run."""

    ticket_id: str
    success: bool
    stages_completed: list[str]
    pr_url: str | None = None
    total_turns: int = 0
    total_duration_seconds: float = 0.0
    failure_stage: str | None = None
    failure_reason: str | None = None
    paused: bool = False
    no_pr_reason: str | None = None
    result_text: str | None = None


def _merge_main_before_retry_resume(
    cwd: str | Path,
    pr_url: str,
    *,
    env: dict[str, str] | None = None,
) -> bool:
    """Check out the PR branch, merge main into it, and push.

    Called before resuming a retry pipeline so the branch is up to date.
    Returns True on success, False if the merge had conflicts (branch is
    checked out at the PR ref so the resumed pipeline stage can handle it).

    Raises RuntimeError if the PR branch cannot be checked out at all —
    the caller should abort the pipeline rather than continue on the
    wrong branch.
    """
    subprocess_env = {**os.environ, **(env or {})}
    cwd_str = str(cwd)

    # Get the branch name from the PR
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", pr_url, "--json", "headRefName", "--jq", ".headRefName"],
            capture_output=True, text=True, cwd=cwd_str, env=subprocess_env, timeout=30,
        )
        if proc.returncode != 0 or not proc.stdout.strip():
            raise RuntimeError(f"Could not determine PR branch from {pr_url}")
        branch = proc.stdout.strip()
    except (subprocess.TimeoutExpired, OSError) as e:
        raise RuntimeError(f"Failed to get PR branch from {pr_url}: {e}") from e

    try:
        # Fetch all refs
        subprocess.run(
            ["git", "fetch", "origin"],
            cwd=cwd_str, env=subprocess_env, check=True,
            capture_output=True, timeout=60,
        )

        # Check out the PR branch (reset to remote state)
        subprocess.run(
            ["git", "checkout", "-B", branch, f"origin/{branch}"],
            cwd=cwd_str, env=subprocess_env, check=True,
            capture_output=True, timeout=30,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        raise RuntimeError(f"Failed to check out PR branch {branch}: {e}") from e

    # -- Branch is now checked out. Merge/push failures below are
    #    non-fatal: the pipeline can proceed on the checked-out branch. --

    try:
        result = subprocess.run(
            ["git", "merge", "origin/main", "--no-edit"],
            cwd=cwd_str, env=subprocess_env,
            capture_output=True, text=True, timeout=120,
        )
    except subprocess.TimeoutExpired:
        logger.warning("Merge of main into %s timed out", branch)
        subprocess.run(
            ["git", "merge", "--abort"],
            cwd=cwd_str, env=subprocess_env, capture_output=True, timeout=30,
        )
        return False

    if result.returncode != 0:
        subprocess.run(
            ["git", "merge", "--abort"],
            cwd=cwd_str, env=subprocess_env, capture_output=True, timeout=30,
        )
        logger.warning(
            "Merge conflict when merging main into %s — pipeline will handle it",
            branch,
        )
        return False

    # Push the merged result
    try:
        subprocess.run(
            ["git", "push", "origin", branch],
            cwd=cwd_str, env=subprocess_env, check=True,
            capture_output=True, timeout=60,
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        logger.warning("Failed to push merged branch %s — continuing anyway", branch)
        return False

    logger.info("Successfully merged main into %s and pushed", branch)
    return True


def run_pipeline(
    *,
    ticket_id: str,
    ticket_labels: list[str] | None = None,
    task_id: int,
    cwd: str | Path,
    conn,
    slot_manager=None,
    project: str | None = None,
    slot_id: int | None = None,
    db_path: str | Path | None = None,
    log_dir: str | Path | None = None,
    placeholder_branch: str | None = None,
    max_turns: dict[str, int] | None = None,
    pr_checks_timeout: int = DEFAULT_PR_CHECKS_TIMEOUT,
    max_review_iterations: int = 3,
    max_ci_retries: int = 2,
    max_merge_conflict_retries: int = 2,
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,  # TODO: wire through to run_claude_streaming --resume
    slot_db_path: str | Path | None = None,
    pause_event: multiprocessing.Event | None = None,
    identities: IdentitiesConfig | None = None,
    agent_adapters_config: dict[str, AdapterConfig] | None = None,
    prior_context: str = "",
    merge_main_before_resume: bool = False,
    bugtracker_type: str = "Linear",
    bugtracker_api_key: str = "",
    bugtracker_url: str = "",
    bugtracker_email: str = "",
    oauth_token: str = "",
    auth_mode: str = "oauth",
    pipeline_id: int | None = None,
    project_default_pipeline: str = "",
) -> PipelineResult:
    """Execute the full implement→review→fix→pr_checks→merge pipeline.

    After the first review→fix cycle, the pipeline loops back to review
    to verify the fixes.  This continues until the reviewer approves or
    ``max_review_iterations`` is reached, after which the pipeline
    proceeds to pr_checks regardless.

    If CI checks fail and ``max_ci_retries`` > 0, the pipeline loops
    back to the fix stage (with CI failure context) and re-runs
    pr_checks.  This continues until CI passes or retries are exhausted.

    Args:
        ticket_id: Linear ticket identifier (e.g. "SMA-42").
        task_id: Database task row ID for recording stage runs.
        cwd: Working directory (git worktree) for all commands.
        conn: sqlite3 connection for recording metrics.
        slot_manager: Optional SlotManager for updating slot state.
        project: Project name (required if slot_manager is provided).
        slot_id: Slot ID (required if slot_manager is provided).
        log_dir: Per-ticket log directory. When set, each stage writes
            raw subprocess output to ``<log_dir>/<stage>-<timestamp>.log``.
        max_turns: Per-stage max_turns overrides.
        pr_checks_timeout: Seconds to wait for CI checks.
        max_review_iterations: Maximum review→fix iterations before
            proceeding to pr_checks (default 3).
        max_ci_retries: Maximum fix→pr_checks retries when CI fails
            (default 2). Set to 0 to disable CI retry loop.
        resume_from_stage: If set, skip stages before this one (for limit
            resumption). The pipeline continues from this stage onward.
        resume_session_id: If set, attempt to use ``--resume`` with this
            session ID for the first stage invocation.

    Returns:
        PipelineResult with aggregated metrics.
    """
    pipeline = PipelineResult(ticket_id=ticket_id, success=False, stages_completed=[])

    pr_url: str | None = None

    # Build per-role env overrides for Claude subprocesses.
    # When identities are configured, coder stages get SSH/git/GH_TOKEN
    # and reviewer stages get their own GH_TOKEN.
    ident = identities or IdentitiesConfig()
    coder_env = build_coder_env(ident, slot_db_path) or None
    reviewer_env = build_reviewer_env(ident, slot_db_path) or None

    # Inject the long-lived token so Claude Code uses it directly.
    # Only for long_lived_token mode — in oauth mode, Claude Code reads
    # credentials from disk (short-lived tokens go stale mid-session).
    if oauth_token and auth_mode == "long_lived_token":
        if coder_env is None:
            coder_env = {}
        coder_env["CLAUDE_CODE_OAUTH_TOKEN"] = oauth_token
        if reviewer_env is None:
            reviewer_env = {}
        reviewer_env["CLAUDE_CODE_OAUTH_TOKEN"] = oauth_token

    # Build MCP config for bugtracker tools.
    # Prefer the coder identity's tracker key so operations appear under the
    # coder bot; fall back to the shared bugtracker key.
    # For Jira, the coder identity has separate jira_api_token / jira_email fields.
    # Pick credentials as a pair to avoid mismatched email/token combinations.
    if bugtracker_type.lower() == "jira":
        if ident.coder.jira_api_token:
            effective_tracker_key = ident.coder.jira_api_token
            effective_email = ident.coder.jira_email or bugtracker_email
        else:
            effective_tracker_key = bugtracker_api_key
            effective_email = bugtracker_email
    else:
        effective_tracker_key = ident.coder.tracker_api_key or bugtracker_api_key
        effective_email = ""
    mcp_config = build_bugtracker_mcp_config(
        bugtracker_type, effective_tracker_key,
        bugtracker_url=bugtracker_url, jira_username=effective_email,
    ) if effective_tracker_key else ""

    # If the supervisor pre-assigned a pipeline_id (e.g. re-dispatch for
    # A/B comparison), honour it instead of resolving from ticket labels.
    task_row = get_task(conn, task_id)
    pre_assigned_pipeline_id = (
        task_row["pipeline_id"] if task_row and task_row["pipeline_id"] is not None else None
    )

    # Load pipeline template and derive configuration
    (stages, turns_cfg, eff_max_review_iterations, eff_max_ci_retries,
     eff_max_merge_conflict_retries, loop_managed_stages,
     pipeline_tpl) = _load_pipeline_config(
        conn, ticket_id, ticket_labels or [], max_turns,
        max_review_iterations, max_ci_retries, max_merge_conflict_retries,
        pipeline_id=pre_assigned_pipeline_id if pre_assigned_pipeline_id is not None else pipeline_id,
        project_default_pipeline=project_default_pipeline,
    )

    # Persist the resolved pipeline_id on the task for later querying.
    # Skip if already pre-assigned (re-dispatch) to avoid overwriting.
    if pipeline_tpl is not None and not pre_assigned_pipeline_id:
        update_task(conn, task_id, pipeline_id=pipeline_tpl.id)
        conn.commit()

    # Extend MCP config with any extra servers declared on the pipeline template.
    if pipeline_tpl is not None and pipeline_tpl.mcp_servers:
        mcp_config = _merge_mcp_configs(mcp_config, pipeline_tpl.mcp_servers)

    # Validate resume_from_stage upfront — check against all pipeline stages
    # (including loop-managed ones), not just main stages.
    if resume_from_stage and resume_from_stage not in stages:
        pipeline.failure_stage = resume_from_stage
        pipeline.failure_reason = f"Unknown resume stage: {resume_from_stage}"
        _record_failure(conn, task_id, pipeline)
        return pipeline

    # When resuming, recover PR URL — check task DB first (retry resume
    # preserves it), then fall back to gh pr view (limit-pause resume).
    if resume_from_stage:
        task_row = get_task(conn, task_id)
        pr_url = (task_row["pr_url"] if task_row else None) or _recover_pr_url(
            cwd, env=coder_env,
        )
        if pr_url:
            pipeline.pr_url = pr_url

        # On retry resume, check out the PR branch and merge main.
        # RuntimeError means the branch couldn't be checked out — hard-fail
        # rather than running mid-pipeline stages on the wrong branch.
        if merge_main_before_resume and pr_url:
            try:
                _merge_main_before_retry_resume(cwd, pr_url, env=coder_env)
            except RuntimeError as e:
                logger.warning("Pre-resume branch setup failed: %s", e)
                pipeline.failure_stage = resume_from_stage
                pipeline.failure_reason = f"Pre-resume branch setup failed: {e}"
                _record_failure(conn, task_id, pipeline)
                return pipeline

    # Determine which stages to skip when resuming
    skipping = resume_from_stage is not None

    # Create shared-mem directory for inter-stage context passing.
    # The implementer writes a summary here; the fixer reads it for a warm start.
    shared_mem_dir = Path.home() / ".botfarm" / "shared-mem" / ticket_id
    if resume_from_stage is None and shared_mem_dir.exists():
        # Fresh run — clear stale notes from any previous attempt.
        shutil.rmtree(shared_mem_dir, ignore_errors=True)
    shared_mem_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Created shared-mem directory: %s", shared_mem_dir)

    # Resolve adapter configs — use provided config or defaults.
    adapters = agent_adapters_config or {}
    codex_ac = adapters.get("codex", CODEX_ADAPTER_DEFAULTS)

    # Build adapter registry via entry-point discovery, passing per-adapter
    # config generically so worker doesn't need to know adapter types.
    adapter_cfg_dicts: dict[str, dict] = {
        name: asdict(ac) for name, ac in adapters.items()
    }
    registry = build_adapter_registry(
        adapter_configs=adapter_cfg_dicts,
        auth_mode=auth_mode,
    )

    # Shared context for _run_and_record helper
    ctx = _PipelineContext(
        ticket_id=ticket_id,
        ticket_labels=ticket_labels or [],
        task_id=task_id,
        cwd=cwd,
        conn=conn,
        turns_cfg=turns_cfg,
        pr_checks_timeout=pr_checks_timeout,
        pipeline=pipeline,
        pipeline_tpl=pipeline_tpl,
        slot_manager=slot_manager,
        project=project,
        slot_id=slot_id,
        db_path=db_path,
        log_dir=Path(log_dir) if log_dir else None,
        placeholder_branch=placeholder_branch,
        coder_env=coder_env,
        reviewer_env=reviewer_env,
        shared_mem_path=shared_mem_dir,
        mcp_config=mcp_config or "",
        codex_config=_CodexReviewerConfig(
            enabled=codex_ac.enabled,
            model=codex_ac.model,
            reasoning_effort=codex_ac.reasoning_effort,
            timeout_minutes=codex_ac.timeout_minutes if codex_ac.timeout_minutes is not None else CODEX_ADAPTER_DEFAULTS.timeout_minutes,
            skip_on_reiteration=codex_ac.skip_on_reiteration,
        ),
        registry=registry,
        max_review_iterations=eff_max_review_iterations,
        max_ci_retries=eff_max_ci_retries,
        max_merge_conflict_retries=eff_max_merge_conflict_retries,
        _refreshable_limits=_compute_refreshable_limits(pipeline_tpl),
        prior_context=prior_context,
        bugtracker_type=bugtracker_type,
        auth_mode=auth_mode,
    )

    # Build main-stage list: skip loop-managed stages (they're handled
    # inside their respective loops), unless we're resuming from one.
    resume_include = {resume_from_stage} if resume_from_stage and resume_from_stage in loop_managed_stages else set()
    main_stages = [s for s in stages if s not in loop_managed_stages or s in resume_include]

    try:
        for stage in main_stages:
            if skipping:
                if stage == resume_from_stage:
                    skipping = False
                    logger.info(
                        "Resuming pipeline at stage '%s' for %s", stage, ticket_id,
                    )
                else:
                    pipeline.stages_completed.append(stage)
                    logger.info("Skipping already-completed stage '%s' for %s", stage, ticket_id)
                    continue
            else:
                logger.info("Starting stage '%s' for %s", stage, ticket_id)

            # Check for manual pause request between stages
            if pause_event is not None and pause_event.is_set():
                logger.info(
                    "Manual pause requested — exiting pipeline after stage '%s' for %s",
                    pipeline.stages_completed[-1] if pipeline.stages_completed else "(none)",
                    ticket_id,
                )
                pipeline.paused = True
                return pipeline

            # Re-read runtime config so dashboard changes propagate mid-pipeline
            ctx._refresh_runtime_config()

            should_stop, pr_url = _dispatch_pipeline_stage(
                ctx, stage,
                pr_url=pr_url,
            )
            if should_stop:
                return pipeline

        # All stages passed
        pipeline.success = True
        update_task(
            conn,
            task_id,
            status="completed",
            turns=pipeline.total_turns,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        conn.commit()

        return pipeline
    finally:
        # Clean up shared-mem directory (but preserve on pause so a
        # resumed pipeline can still use the implementer's notes).
        if not pipeline.paused and shared_mem_dir.exists():
            shutil.rmtree(shared_mem_dir, ignore_errors=True)
            logger.info("Cleaned up shared-mem directory: %s", shared_mem_dir)


# ---------------------------------------------------------------------------
# Pipeline setup and stage dispatch
# ---------------------------------------------------------------------------


def _load_pipeline_config(
    conn,
    ticket_id: str,
    ticket_labels: list[str],
    max_turns: dict[str, int] | None,
    max_review_iterations: int,
    max_ci_retries: int,
    max_merge_conflict_retries: int = 2,
    pipeline_id: int | None = None,
    project_default_pipeline: str = "",
) -> tuple[
    tuple[str, ...],
    dict[str, int],
    int,
    int,
    int,
    set[str],
    PipelineTemplate | None,
]:
    """Load pipeline template from DB and derive all configuration.

    When *pipeline_id* is provided (manual dispatch or re-dispatch override),
    it takes precedence over label-based selection.

    Returns ``(stages, turns_cfg, eff_max_review_iterations,
    eff_max_ci_retries, eff_max_merge_conflict_retries,
    loop_managed_stages, pipeline_tpl)``.
    """
    pipeline_tpl: PipelineTemplate | None = None
    try:
        if pipeline_id is not None:
            pipeline_tpl = load_pipeline_by_id(conn, pipeline_id)
            logger.info(
                "Using manually selected pipeline '%s' (id=%d) for %s (stages: %s)",
                pipeline_tpl.name, pipeline_id, ticket_id,
                ", ".join(s.name for s in pipeline_tpl.stages),
            )
        else:
            pipeline_tpl = load_pipeline(conn, ticket_labels, project_default_pipeline)
            logger.info(
                "Loaded pipeline '%s' for %s (stages: %s)",
                pipeline_tpl.name, ticket_id,
                ", ".join(s.name for s in pipeline_tpl.stages),
            )
    except Exception:
        if pipeline_id is not None:
            raise  # User explicitly chose this pipeline — don't silently fall back
        logger.warning(
            "Could not load pipeline from DB for %s — using legacy constants",
            ticket_id, exc_info=True,
        )

    # Derive stage ordering and max-turns from pipeline template
    if pipeline_tpl is not None:
        stages = _derive_stages(pipeline_tpl)
        turns_cfg = _build_turns_cfg(pipeline_tpl, max_turns)
    else:
        stages = STAGES
        turns_cfg = {
            "implement": DEFAULT_IMPLEMENT_MAX_TURNS,
            "review": DEFAULT_REVIEW_MAX_TURNS,
            "fix": DEFAULT_FIX_MAX_TURNS,
            **(max_turns or {}),
        }

    # Resolve loop configurations from DB
    review_loop: StageLoop | None = None
    ci_retry_loop: StageLoop | None = None
    if pipeline_tpl is not None:
        review_loop = get_loop_for_stage(pipeline_tpl, "review")
        ci_retry_loop = get_loop_for_stage(pipeline_tpl, "ci_fix")

    # Resolve effective iteration counts
    eff_max_review_iterations = max_review_iterations
    if review_loop is not None:
        from botfarm.config import AgentsConfig
        _agents_cfg = AgentsConfig(
            max_review_iterations=max_review_iterations,
            max_ci_retries=max_ci_retries,
        )
        eff_max_review_iterations = resolve_max_iterations(review_loop, _agents_cfg)

    eff_max_ci_retries = max_ci_retries
    if ci_retry_loop is not None:
        from botfarm.config import AgentsConfig
        _agents_cfg = AgentsConfig(
            max_review_iterations=max_review_iterations,
            max_ci_retries=max_ci_retries,
        )
        eff_max_ci_retries = resolve_max_iterations(ci_retry_loop, _agents_cfg)

    merge_conflict_loop: StageLoop | None = None
    if pipeline_tpl is not None:
        merge_conflict_loop = get_loop_for_stage(pipeline_tpl, "resolve_conflict")

    eff_max_merge_conflict_retries = max_merge_conflict_retries
    if merge_conflict_loop is not None:
        from botfarm.config import AgentsConfig
        _agents_cfg = AgentsConfig(
            max_review_iterations=max_review_iterations,
            max_ci_retries=max_ci_retries,
            max_merge_conflict_retries=max_merge_conflict_retries,
        )
        eff_max_merge_conflict_retries = resolve_max_iterations(
            merge_conflict_loop, _agents_cfg,
        )

    # Determine loop-managed stages: skipped in main iteration because
    # they're handled inside their respective loops.
    loop_managed_stages: set[str] = set()
    if pipeline_tpl is not None:
        for loop in pipeline_tpl.loops:
            if loop.on_failure_stage:
                loop_managed_stages.add(loop.start_stage)
            else:
                loop_managed_stages.add(loop.end_stage)

    return (stages, turns_cfg, eff_max_review_iterations, eff_max_ci_retries,
            eff_max_merge_conflict_retries, loop_managed_stages, pipeline_tpl)


# Mapping from loop start-stage to the _PipelineContext field it controls.
_LOOP_STAGE_TO_LIMIT_FIELD: dict[str, str] = {
    "review": "max_review_iterations",
    "ci_fix": "max_ci_retries",
    "resolve_conflict": "max_merge_conflict_retries",
}


def _to_bool(v: object) -> bool:
    """Convert a value to bool, handling string representations safely."""
    if isinstance(v, str):
        return v.lower() in ("true", "1", "yes")
    return bool(v)


def _compute_refreshable_limits(
    pipeline_tpl: PipelineTemplate | None,
) -> frozenset[str]:
    """Determine which limit fields should be refreshed from runtime config.

    A field is refreshable only when the pipeline template's loop for that
    stage has a ``config_key`` that matches the corresponding runtime-config
    field name.  Loops with ``config_key=None`` (fixed iteration count) or
    a custom/unknown ``config_key`` are not refreshable.
    """
    all_fields = {"max_review_iterations", "max_ci_retries", "max_merge_conflict_retries"}
    if pipeline_tpl is None:
        return frozenset(all_fields)

    for stage_name, field_name in _LOOP_STAGE_TO_LIMIT_FIELD.items():
        loop = get_loop_for_stage(pipeline_tpl, stage_name)
        if loop is not None and loop.config_key != field_name:
            all_fields.discard(field_name)

    return frozenset(all_fields)


def _dispatch_pipeline_stage(
    ctx: "_PipelineContext",
    stage: str,
    *,
    pr_url: str | None,
) -> tuple[bool, str | None]:
    """Dispatch a single stage in the pipeline main loop.

    Returns ``(should_stop, updated_pr_url)``.  When *should_stop* is
    ``True``, the pipeline should return immediately (failure or early
    success already recorded in ``ctx.pipeline``).
    """
    # ----- Review iteration loop -----
    if stage == "review":
        success = _run_review_fix_loop(
            ctx, pr_url=pr_url, max_iterations=ctx.max_review_iterations,
        )
        if not success:
            return True, pr_url
        if "review" not in ctx.pipeline.stages_completed:
            ctx.pipeline.stages_completed.append("review")
        if "fix" not in ctx.pipeline.stages_completed:
            ctx.pipeline.stages_completed.append("fix")
        return False, pr_url

    if stage == "fix" and "fix" in ctx.pipeline.stages_completed:
        return False, pr_url

    if stage == "fix":
        should_stop = _handle_fix_resume(
            ctx, pr_url=pr_url, max_iterations=ctx.max_review_iterations,
        )
        return should_stop, pr_url

    # ----- CI retry loop -----
    if stage == "pr_checks":
        return _handle_pr_checks_stage(
            ctx, pr_url=pr_url, max_ci_retries=ctx.max_ci_retries,
        )

    # ----- Resolve conflict (resume mid-merge-conflict-loop) -----
    if stage == "resolve_conflict":
        return _handle_resolve_conflict_resume(
            ctx,
            pr_url=pr_url,
        )

    # ----- Merge with conflict retry loop -----
    if stage == "merge" and "merge" in ctx.pipeline.stages_completed:
        return False, pr_url

    if stage == "merge":
        return _handle_merge_stage(
            ctx,
            pr_url=pr_url,
            max_merge_conflict_retries=ctx.max_merge_conflict_retries,
        )

    # ----- Normal stages (implement, etc.) -----
    result = ctx.run_and_record(stage, pr_url=pr_url)
    if result is None:
        return True, pr_url

    # Capture PR URL from any stage with pr_url result_parser
    if result.pr_url:
        pr_url = result.pr_url
        ctx.pipeline.pr_url = pr_url
        update_task(ctx.conn, ctx.task_id, pr_url=pr_url)
        ctx.conn.commit()
        if ctx.slot_manager and ctx.project and ctx.slot_id is not None:
            ctx.slot_manager.set_pr_url(ctx.project, ctx.slot_id, pr_url)

    # Post-implement special handling
    if stage == "implement":
        should_stop = _handle_implement_result(ctx, result, pr_url)
        if should_stop:
            return True, pr_url

    return False, pr_url


def _handle_fix_resume(
    ctx: "_PipelineContext",
    *,
    pr_url: str | None,
    max_iterations: int,
) -> bool:
    """Handle resuming from the fix stage.

    Runs the initial fix, then enters the review iteration loop with
    remaining iterations so fixes get verified.

    Returns ``True`` if the pipeline should stop (failure), ``False``
    to continue.
    """
    result = ctx.run_and_record("fix", pr_url=pr_url)
    if result is None:
        return True
    update_task(ctx.conn, ctx.task_id, review_iterations=1)
    ctx.conn.commit()
    if max_iterations > 1:
        success = _run_review_fix_loop(
            ctx, pr_url=pr_url, max_iterations=max_iterations,
            start_iteration=2,
        )
        if not success:
            return True
    if "review" not in ctx.pipeline.stages_completed:
        ctx.pipeline.stages_completed.append("review")
    if "fix" not in ctx.pipeline.stages_completed:
        ctx.pipeline.stages_completed.append("fix")
    return False


def _try_conflict_resolution(
    ctx: "_PipelineContext",
    pr_url: str,
    log_msg: str,
) -> tuple[bool, str | None] | None:
    """Check for merge conflict and attempt resolution if found.

    Returns ``(should_stop, pr_url)`` when a conflict was detected,
    or ``None`` to let the caller fall through to other logic.
    """
    if not _check_pr_has_merge_conflict(
        pr_url, ctx.cwd,
        env={**os.environ, **(ctx.coder_env or {})},
    ):
        return None

    ctx._refresh_runtime_config()
    if ctx.max_merge_conflict_retries <= 0:
        return True, pr_url  # conflict retries disabled — fail

    logger.info(log_msg, ctx.ticket_id)
    _reset_for_retry(ctx)
    success = _run_merge_conflict_loop(
        ctx,
        pr_url=pr_url,
        max_retries=ctx.max_merge_conflict_retries,
    )
    if not success:
        return True, pr_url
    if "pr_checks" not in ctx.pipeline.stages_completed:
        ctx.pipeline.stages_completed.append("pr_checks")
    return False, pr_url


def _handle_pr_checks_stage(
    ctx: "_PipelineContext",
    *,
    pr_url: str | None,
    max_ci_retries: int,
) -> tuple[bool, str | None]:
    """Handle the pr_checks stage with CI retry loop.

    Returns ``(should_stop, pr_url)``.
    """
    result = ctx.run_and_record("pr_checks", pr_url=pr_url)
    if result is not None:
        return False, pr_url  # CI passed on first try

    # Check if the failure is due to merge conflicts.
    # When pr_checks detects a merge conflict (pre-check or "no checks reported"
    # because GitHub won't run CI on conflicting PRs), route to the conflict
    # resolution flow instead of wasting ci_fix retries.
    if pr_url:
        conflict_result = _try_conflict_resolution(
            ctx, pr_url,
            "pr_checks failed due to merge conflict for %s — entering conflict resolution",
        )
        if conflict_result is not None:
            return conflict_result

    # Re-read in case the budget was changed while pr_checks was running
    ctx._refresh_runtime_config()
    eff_max_ci_retries = ctx.max_ci_retries

    if eff_max_ci_retries > 0:
        ci_failure_output = ctx.pipeline.failure_reason or "CI checks failed"
        _reset_for_retry(ctx)
        success = _run_ci_retry_loop(
            ctx,
            pr_url=pr_url,
            ci_failure_output=ci_failure_output,
            max_retries=eff_max_ci_retries,
        )
        if success:
            if "pr_checks" not in ctx.pipeline.stages_completed:
                ctx.pipeline.stages_completed.append("pr_checks")
            return False, pr_url

        # CI retries exhausted — check if a merge conflict developed
        # during the ci_fix loop (e.g. another PR merged to main).
        if pr_url:
            conflict_result = _try_conflict_resolution(
                ctx, pr_url,
                "Merge conflict detected after CI retries for %s — entering conflict resolution",
            )
            if conflict_result is not None:
                return conflict_result

        return True, pr_url

    # No retries configured — fail
    return True, pr_url


def _handle_resolve_conflict_resume(
    ctx: "_PipelineContext",
    *,
    pr_url: str | None,
) -> tuple[bool, str | None]:
    """Handle resuming from the resolve_conflict stage.

    When a worker is resumed mid-merge-conflict-loop (e.g. after a
    usage-limit pause during resolve_conflict), we must re-run the
    remaining loop steps — review, pr_checks, and merge — to ensure
    the conflict-resolved code is reviewed and passes CI before merging.

    Returns ``(should_stop, pr_url)``.
    """
    result = ctx.run_and_record("resolve_conflict", pr_url=pr_url)
    if result is None:
        return True, pr_url

    # Re-run review→fix loop on the conflict-resolved code
    success = _run_review_fix_loop(
        ctx, pr_url=pr_url, max_iterations=ctx.max_review_iterations,
    )
    if not success:
        return True, pr_url

    # Re-run pr_checks (with CI retry support)
    checks_stop, pr_url = _handle_pr_checks_stage(
        ctx, pr_url=pr_url, max_ci_retries=ctx.max_ci_retries,
    )
    if checks_stop:
        return True, pr_url

    # Attempt merge (with full conflict retry support)
    return _handle_merge_stage(
        ctx,
        pr_url=pr_url,
        max_merge_conflict_retries=ctx.max_merge_conflict_retries,
    )


def _handle_merge_stage(
    ctx: "_PipelineContext",
    *,
    pr_url: str | None,
    max_merge_conflict_retries: int,
) -> tuple[bool, str | None]:
    """Handle the merge stage with conflict retry loop.

    Returns ``(should_stop, pr_url)``.
    """
    result = ctx.run_and_record("merge", pr_url=pr_url)
    if result is not None:
        return False, pr_url  # merge succeeded

    # Re-read in case the budget was changed while merge was running
    ctx._refresh_runtime_config()
    eff_max_merge_conflict_retries = ctx.max_merge_conflict_retries

    merge_error = ctx.pipeline.failure_reason or "merge failed"
    if eff_max_merge_conflict_retries > 0 and _is_merge_conflict(
        merge_error, pr_url or "", ctx.cwd,
        env={**os.environ, **(ctx.coder_env or {})},
    ):
        _reset_for_retry(ctx)
        success = _run_merge_conflict_loop(
            ctx,
            pr_url=pr_url,
            max_retries=eff_max_merge_conflict_retries,
        )
        if not success:
            return True, pr_url
        if "merge" not in ctx.pipeline.stages_completed:
            ctx.pipeline.stages_completed.append("merge")
        return False, pr_url

    # Non-conflict failure — bail
    return True, pr_url


def _run_merge_conflict_loop(
    ctx: "_PipelineContext",
    *,
    pr_url: str | None,
    max_retries: int,
) -> bool:
    """Resolve merge conflicts and re-run review→fix→pr_checks→merge.

    Returns ``True`` on success (merge completed) and ``False`` if
    retries are exhausted or a non-conflict error occurs.
    """
    # Seed from persisted count so crash-recovered / paused workers
    # don't restart the budget from scratch.
    task_row = get_task(ctx.conn, ctx.task_id)
    already_used = (task_row["merge_conflict_retries"] if task_row else 0) or 0

    actual_retries = already_used
    for retry in range(already_used + 1, max_retries + 1):
        ctx._refresh_runtime_config()
        # Note: only limit reductions take effect mid-loop; increases
        # require a new loop entry because the range upper bound is fixed.
        if retry > ctx.max_merge_conflict_retries:
            logger.info(
                "max_merge_conflict_retries reduced to %d — stopping merge conflict loop at retry %d",
                ctx.max_merge_conflict_retries, retry,
            )
            break

        actual_retries = retry

        insert_event(
            ctx.conn,
            task_id=ctx.task_id,
            event_type="merge_conflict_retry_started",
            detail=f"retry={retry}",
        )
        update_task(ctx.conn, ctx.task_id, merge_conflict_retries=retry)
        ctx.conn.commit()

        # --- RESOLVE CONFLICT (Claude merges main into feature branch) ---
        resolve_result = ctx.run_and_record(
            "resolve_conflict", pr_url=pr_url, iteration=retry,
        )
        if resolve_result is None:
            return False  # failure recorded by run_and_record

        # --- RE-RUN REVIEW→FIX LOOP (code diff changed) ---
        success = _run_review_fix_loop(
            ctx, pr_url=pr_url, max_iterations=ctx.max_review_iterations,
        )
        if not success:
            return False

        # --- RE-RUN PR_CHECKS ---
        checks_result = ctx.run_and_record("pr_checks", pr_url=pr_url, iteration=retry)
        if checks_result is None:
            # Check if this pr_checks failure is due to a merge conflict
            # (e.g. another PR merged to main during review). If so,
            # continue to the next conflict retry instead of entering
            # the CI fix path (which can't resolve conflicts).
            if pr_url and _check_pr_has_merge_conflict(
                pr_url, ctx.cwd,
                env={**os.environ, **(ctx.coder_env or {})},
            ):
                _reset_for_retry(ctx)
                continue

            # CI failed — attempt CI retries within this conflict retry
            if ctx.max_ci_retries > 0:
                ci_failure_output = ctx.pipeline.failure_reason or "CI checks failed"
                _reset_for_retry(ctx)
                ci_success = _run_ci_retry_loop(
                    ctx,
                    pr_url=pr_url,
                    ci_failure_output=ci_failure_output,
                    max_retries=ctx.max_ci_retries,
                )
                if not ci_success:
                    # CI retries exhausted — check if a merge conflict
                    # developed during CI fixes.
                    if pr_url and _check_pr_has_merge_conflict(
                        pr_url, ctx.cwd,
                        env={**os.environ, **(ctx.coder_env or {})},
                    ):
                        _reset_for_retry(ctx)
                        continue
                    return False
            else:
                return False

        # --- RE-ATTEMPT MERGE ---
        merge_result = ctx.run_and_record("merge", pr_url=pr_url, iteration=retry)
        if merge_result is not None:
            insert_event(
                ctx.conn,
                task_id=ctx.task_id,
                event_type="merge_succeeded_after_conflict_retry",
                detail=f"retry={retry}",
            )
            ctx.conn.commit()
            logger.info(
                "Merge succeeded on conflict retry %d for %s",
                retry, ctx.ticket_id,
            )
            return True

        # Merge failed again — check if it's still a conflict
        merge_error = ctx.pipeline.failure_reason or "merge failed"
        if not _is_merge_conflict(
            merge_error, pr_url or "", ctx.cwd,
            env={**os.environ, **(ctx.coder_env or {})},
        ):
            # Non-conflict failure — bail out
            return False

        _reset_for_retry(ctx)

    # Max retries exhausted (or limit reduced mid-loop)
    effective_limit = min(ctx.max_merge_conflict_retries, max_retries)
    insert_event(
        ctx.conn,
        task_id=ctx.task_id,
        event_type="max_merge_conflict_retries_reached",
        detail=f"retries={actual_retries}, limit={effective_limit}",
    )
    ctx.conn.commit()
    logger.warning(
        "Max merge conflict retries (%d) reached for %s — marking as failed",
        effective_limit, ctx.ticket_id,
    )
    ctx.pipeline.failure_stage = "merge"
    ctx.pipeline.failure_reason = (
        f"Merge conflicts could not be resolved after {actual_retries} retries (limit={effective_limit})"
    )
    _record_failure(ctx.conn, ctx.task_id, ctx.pipeline)
    return False


def _handle_implement_result(
    ctx: "_PipelineContext",
    result: "StageResult",
    pr_url: str | None,
) -> bool:
    """Handle post-implement special cases.

    Returns ``True`` if the pipeline should stop (success or failure
    already recorded).
    """
    # Investigation: short-circuit success
    if _is_investigation(ctx.ticket_labels):
        ctx.pipeline.success = True
        ctx.pipeline.no_pr_reason = "No-PR ticket — no PR created"
        update_task(
            ctx.conn,
            ctx.task_id,
            status="completed",
            turns=ctx.pipeline.total_turns,
            completed_at=datetime.now(timezone.utc).isoformat(),
            comments=f"NO_PR_NEEDED: {ctx.pipeline.no_pr_reason}",
        )
        ctx.conn.commit()
        return True

    # No PR URL: check for explicit no-PR signal
    if not pr_url:
        no_pr_reason = _detect_no_pr_needed(
            result.agent_result.result_text if result.agent_result else ""
        )
        if no_pr_reason:
            logger.info(
                "Implement stage reported no PR needed for %s: %s",
                ctx.ticket_id, no_pr_reason[:RESULT_TRUNCATE_CHARS],
            )
            ctx.pipeline.success = True
            ctx.pipeline.no_pr_reason = no_pr_reason
            update_task(
                ctx.conn, ctx.task_id,
                status="completed",
                turns=ctx.pipeline.total_turns,
                completed_at=datetime.now(timezone.utc).isoformat(),
                comments=f"NO_PR_NEEDED: {no_pr_reason}",
            )
            ctx.conn.commit()
            return True
        # Genuine failure
        ctx.pipeline.failure_stage = "implement"
        ctx.pipeline.failure_reason = "Implement stage did not produce a PR URL"
        _record_failure(ctx.conn, ctx.task_id, ctx.pipeline)
        return True

    return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


@dataclass
class _CodexReviewerConfig:
    """Configuration for the Codex dual-reviewer feature."""

    enabled: bool = False
    model: str = ""
    reasoning_effort: str = "medium"
    timeout_minutes: int = 15
    skip_on_reiteration: bool = True


def _env_for_stage(
    stage: str,
    pipeline_tpl: PipelineTemplate | None,
    coder_env: dict[str, str] | None,
    reviewer_env: dict[str, str] | None,
) -> dict[str, str] | None:
    """Return the appropriate env dict for a given stage.

    Uses ``StageTemplate.identity`` when a pipeline template is loaded,
    falling back to the stage name for backward compatibility.
    """
    if pipeline_tpl is not None:
        try:
            tpl = get_stage(pipeline_tpl, stage)
            if tpl.identity == "reviewer":
                return reviewer_env
            return coder_env
        except ValueError:
            pass
    # Legacy fallback
    if stage == "review":
        return reviewer_env
    return coder_env


@dataclass
class _PipelineContext:
    """Shared state passed to pipeline helpers to avoid long argument lists."""

    ticket_id: str
    ticket_labels: list[str]
    task_id: int
    cwd: str | Path
    conn: sqlite3.Connection
    turns_cfg: dict[str, int]
    pr_checks_timeout: int
    pipeline: PipelineResult
    pipeline_tpl: PipelineTemplate | None = None
    slot_manager: object | None = None
    project: str | None = None
    slot_id: int | None = None
    db_path: str | Path | None = None
    log_dir: Path | None = None
    placeholder_branch: str | None = None
    coder_env: dict[str, str] | None = None
    reviewer_env: dict[str, str] | None = None
    shared_mem_path: Path | None = None
    mcp_config: str = ""
    codex_config: _CodexReviewerConfig = field(default_factory=_CodexReviewerConfig)
    registry: AdapterRegistry | None = None
    max_review_iterations: int = 3
    max_ci_retries: int = 2
    max_merge_conflict_retries: int = 2
    # Which limit fields are driven by runtime config (vs fixed by pipeline template).
    # Fields NOT in this set were set by a pipeline loop whose config_key does
    # not match the runtime-config field and should not be overwritten.
    _refreshable_limits: frozenset[str] = field(
        default_factory=lambda: frozenset({"max_review_iterations", "max_ci_retries", "max_merge_conflict_retries"}),
    )
    prior_context: str = ""
    bugtracker_type: str = "Linear"
    auth_mode: str = "oauth"

    @property
    def _pipeline_id(self) -> int | None:
        """Return the pipeline template id, or None if no template loaded."""
        return self.pipeline_tpl.id if self.pipeline_tpl is not None else None

    def _refresh_runtime_config(self) -> None:
        """Re-read runtime config from DB and update mutable fields.

        Called before each pipeline stage so dashboard config changes
        propagate to running workers.  Emits a ``runtime_config_refreshed``
        task event when any value actually changes.
        """
        try:
            rt = read_runtime_config(self.conn)
        except Exception:
            logger.debug("Could not read runtime_config — keeping current values", exc_info=True)
            return
        if not rt:
            return  # empty table → keep frozen defaults

        changes: list[str] = []

        for attr in ("max_review_iterations", "max_ci_retries", "max_merge_conflict_retries"):
            if attr in rt and attr in self._refreshable_limits:
                try:
                    new_val = int(rt[attr])
                except (ValueError, TypeError):
                    logger.warning("Invalid runtime config value for %s: %r", attr, rt[attr])
                    continue
                if getattr(self, attr) != new_val:
                    changes.append(f"{attr}: {getattr(self, attr)} -> {new_val}")
                    setattr(self, attr, new_val)

        codex_fields = {
            "codex_reviewer_enabled": ("enabled", _to_bool),
            "codex_reviewer_model": ("model", str),
            "codex_reviewer_reasoning_effort": ("reasoning_effort", str),
            "codex_reviewer_timeout_minutes": ("timeout_minutes", int),
            "codex_reviewer_skip_on_reiteration": ("skip_on_reiteration", _to_bool),
        }
        for db_key, (codex_attr, cast) in codex_fields.items():
            if db_key in rt:
                try:
                    new_val = cast(rt[db_key])
                except (ValueError, TypeError):
                    logger.warning("Invalid runtime config value for %s: %r", db_key, rt[db_key])
                    continue
                if getattr(self.codex_config, codex_attr) != new_val:
                    changes.append(f"{db_key}: {getattr(self.codex_config, codex_attr)} -> {new_val}")
                    setattr(self.codex_config, codex_attr, new_val)

        if changes:
            detail = "; ".join(changes)
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="runtime_config_refreshed",
                detail=detail,
            )
            self.conn.commit()
            logger.info("Runtime config refreshed for %s: %s", self.ticket_id, detail)

    def _env_for_stage(self, stage: str) -> dict[str, str] | None:
        """Return the appropriate env dict for a given stage."""
        return _env_for_stage(
            stage, self.pipeline_tpl, self.coder_env, self.reviewer_env,
        )

    def _get_stage_tpl(self, stage: str) -> StageTemplate | None:
        """Get the StageTemplate for a stage, or None if no pipeline loaded."""
        if self.pipeline_tpl is None:
            return None
        try:
            return get_stage(self.pipeline_tpl, stage)
        except ValueError:
            return None

    def _record_codex_review(
        self,
        result: StageResult,
        *,
        iteration: int,
        codex_log_file: Path | None,
        on_extra_usage: bool = False,
    ) -> None:
        """Record Codex review events and a separate ``codex_review`` stage_runs row."""
        if not result.success:
            # Claude failed → Codex never ran
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="codex_review_skipped",
                detail="Claude review failed",
            )
            self.conn.commit()
            return

        # Record Claude's individual review verdict
        if result.claude_review_approved is not None:
            claude_verdict = "approved" if result.claude_review_approved else "changes_requested"
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="claude_review_completed",
                detail=f"verdict={claude_verdict}",
            )
            self.conn.commit()

        codex_ar = result.secondary_agent_result
        if codex_ar is None:
            # Codex was enabled but never ran (e.g. exception before
            # invocation or template path that didn't invoke Codex).
            # Record as skipped rather than misleading failed.
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="codex_review_skipped",
                detail="no result (Codex not invoked)",
            )
            self.conn.commit()
            return

        # Codex ran and produced a result
        insert_event(
            self.conn,
            task_id=self.task_id,
            event_type="codex_review_started",
        )
        self.conn.commit()

        if codex_ar.is_error:
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="codex_review_failed",
                detail=codex_ar.result_text[:RESULT_TRUNCATE_CHARS] if codex_ar.result_text else "unknown error",
            )
        else:
            codex_approved = _parse_review_approved(codex_ar.result_text)
            verdict_str = "approved" if codex_approved else "changes_requested"
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="codex_review_completed",
                detail=f"verdict={verdict_str}",
            )
        self.conn.commit()

        # Record separate codex_review stage_runs row.
        # Resolve codex adapter for accurate cost calculation — codex_ar.cost_usd
        # is 0.0 by design (cost lives in adapter.calculate_cost()).
        codex_adapter = self.registry.get("codex") if self.registry else None
        if codex_adapter is not None:
            codex_cost = codex_adapter.calculate_cost(codex_ar)
        else:
            codex_cost = codex_ar.cost_usd
        insert_stage_run(
            self.conn,
            task_id=self.task_id,
            stage="codex_review",
            iteration=iteration,
            session_id=codex_ar.session_id,
            turns=codex_ar.num_turns,
            duration_seconds=codex_ar.duration_seconds,
            input_tokens=codex_ar.input_tokens,
            output_tokens=codex_ar.output_tokens,
            cache_read_input_tokens=codex_ar.extra.get("cache_read_input_tokens", 0),
            cache_creation_input_tokens=0,
            total_cost_usd=codex_cost,
            log_file_path=str(codex_log_file) if codex_log_file else None,
            on_extra_usage=on_extra_usage,
            pipeline_id=self._pipeline_id,
        )
        self.conn.commit()

    def run_and_record(
        self,
        stage: str,
        *,
        pr_url: str | None,
        iteration: int = 1,
    ) -> StageResult | None:
        """Execute a stage, record it, accumulate metrics.

        For Claude-based stages (implement, review, fix), inserts a
        placeholder ``stage_runs`` row before execution and updates its
        ``context_fill_pct`` in-place on every assistant turn via the
        streaming runner.  This enables real-time dashboard monitoring.

        Returns the ``StageResult`` on success, or ``None`` if the stage
        failed (in which case ``pipeline`` is updated with the failure).
        """
        if self.db_path and self.project and self.slot_id is not None:
            update_slot_stage(
                self.db_path,
                self.project,
                self.slot_id,
                stage=stage,
                iteration=iteration,
            )
        elif self.slot_manager and self.project and self.slot_id is not None:
            self.slot_manager.update_stage(
                self.project, self.slot_id, stage=stage, iteration=iteration,
            )

        update_task(self.conn, self.task_id, pipeline_stage=stage)
        self.conn.commit()

        log_file = _make_stage_log_path(self.log_dir, stage, iteration)
        wall_start = time.monotonic()

        # For agent-based stages, insert a placeholder stage_run row
        # so per-turn context fill updates have a target row to update.
        on_context_fill: ContextFillCallback | None = None
        stage_run_id: int | None = None
        stage_tpl = self._get_stage_tpl(stage)
        uses_agent = (
            stage_tpl.executor_type not in ("shell", "internal")
            if stage_tpl is not None
            else stage in ("implement", "review", "fix")
        )

        extra_usage = is_extra_usage_active(self.conn)

        if uses_agent:
            stage_run_id = insert_stage_run(
                self.conn,
                task_id=self.task_id,
                stage=stage,
                iteration=iteration,
                log_file_path=str(log_file) if log_file else None,
                on_extra_usage=extra_usage,
                pipeline_id=self._pipeline_id,
            )
            self.conn.commit()

            _conn = self.conn
            _sr_id = stage_run_id

            def on_context_fill(turn: int, fill_pct: float) -> None:
                update_stage_run_context_fill(_conn, _sr_id, fill_pct)

        # Codex reviewer params — when skip_on_reiteration is set (default),
        # only run codex on the first review iteration.  This is an explicit
        # policy: Claude's review covers subsequent iterations and codex adds
        # diminishing value after the first pass (~5 min saved per iteration).
        codex_kwargs: dict = {}
        skip_codex = (
            self.codex_config.skip_on_reiteration and iteration > 1
        )
        if stage == "review" and self.codex_config.enabled and not skip_codex:
            codex_kwargs = {
                "codex_enabled": True,
                "codex_model": self.codex_config.model or None,
                "codex_reasoning_effort": self.codex_config.reasoning_effort or None,
                "codex_timeout": self.codex_config.timeout_minutes * 60.0,
                "codex_log_file": _make_stage_log_path(
                    self.log_dir, "codex_review", iteration,
                ),
                "review_iteration": iteration,
            }
        elif stage == "review" and self.codex_config.enabled and skip_codex:
            logger.info(
                "Skipping Codex review on iteration %d (only runs on first iteration)",
                iteration,
            )
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="codex_review_skipped",
                detail=f"iteration {iteration} (only runs on first)",
            )
            self.conn.commit()

        # Pre-stage token freshness check — proactively refresh expired
        # OAuth tokens in the credential file before spawning a Claude
        # subprocess so it reads a valid token from disk.
        # Only applies to oauth mode — api_key uses ANTHROPIC_API_KEY,
        # and long_lived_token uses a static token that doesn't expire.
        if uses_agent and self.auth_mode == "oauth":
            try:
                from botfarm.credentials import CredentialManager

                cm = CredentialManager()
                if cm.is_token_expired():
                    logger.info("OAuth token expired/near-expiry — refreshing before stage '%s'", stage)
                    cm.refresh_token()
            except Exception:
                logger.debug("Token freshness check failed — continuing anyway", exc_info=True)

        try:
            result = _execute_stage(
                stage,
                ticket_id=self.ticket_id,
                ticket_labels=self.ticket_labels,
                pr_url=pr_url,
                cwd=self.cwd,
                max_turns=self.turns_cfg.get(stage, DEFAULT_MAX_TURNS.get(stage, DEFAULT_FIX_MAX_TURNS)),
                pr_checks_timeout=self.pr_checks_timeout,
                log_file=log_file,
                placeholder_branch=self.placeholder_branch,
                env=self._env_for_stage(stage),
                on_context_fill=on_context_fill,
                stage_tpl=stage_tpl,
                registry=self.registry,
                shared_mem_path=self.shared_mem_path,
                prior_context=self.prior_context,
                bugtracker_type=self.bugtracker_type,
                mcp_config=self.mcp_config,
                **codex_kwargs,
            )
        except Exception as exc:
            logger.error("Stage '%s' raised: %s", stage, exc)
            if stage_run_id is not None:
                delete_stage_run(self.conn, stage_run_id)
            if codex_kwargs:
                insert_event(
                    self.conn,
                    task_id=self.task_id,
                    event_type="codex_review_skipped",
                    detail=f"review stage raised: {str(exc)[:RESULT_TRUNCATE_CHARS]}",
                )
                self.conn.commit()
            self.pipeline.failure_stage = stage
            self.pipeline.failure_reason = str(exc)[:DETAIL_TRUNCATE_CHARS]
            _record_failure(self.conn, self.task_id, self.pipeline)
            return None

        # Auth-failure retry: if the stage failed with an authentication
        # error, refresh the OAuth token and re-run the stage exactly once.
        # Only applies to oauth mode — api_key uses static ANTHROPIC_API_KEY,
        # and long_lived_token uses a static long-lived token.
        if not result.success and uses_agent and self.auth_mode == "oauth":
            result, stage_run_id = self._maybe_retry_on_auth_failure(
                result,
                stage=stage,
                pr_url=pr_url,
                iteration=iteration,
                log_file=log_file,
                stage_run_id=stage_run_id,
                stage_tpl=stage_tpl,
                on_context_fill=on_context_fill,
                extra_usage=extra_usage,
                codex_kwargs=codex_kwargs,
                wall_start=wall_start,
            )

        # Record Codex review as a separate stage run + events
        if codex_kwargs:
            self._record_codex_review(
                result,
                iteration=iteration,
                codex_log_file=codex_kwargs.get("codex_log_file"),
                on_extra_usage=extra_usage,
            )
        elif stage == "review" and self.codex_config.enabled and result.review_approved is not None:
            # Codex was skipped this iteration (e.g. skip_on_reiteration)
            # but we still need to record Claude's verdict so that
            # _build_review_summary picks up the latest result.
            verdict = "approved" if result.review_approved else "changes_requested"
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="claude_review_completed",
                detail=f"verdict={verdict}",
            )
            self.conn.commit()

        return self._finish_stage(
            stage, result, wall_start, iteration,
            stage_run_id=stage_run_id,
            log_file=log_file,
            on_extra_usage=extra_usage,
        )

    def run_and_record_result(
        self,
        stage: str,
        result: StageResult,
        *,
        wall_start: float,
        iteration: int = 1,
        stage_run_id: int | None = None,
        log_file: Path | None = None,
        on_extra_usage: bool = False,
    ) -> StageResult | None:
        """Record a pre-computed StageResult, accumulate metrics.

        Like ``run_and_record`` but skips execution — the caller already
        ran the stage directly (e.g. ``_run_ci_fix``).

        Returns the ``StageResult`` on success, or ``None`` on failure.
        """
        return self._finish_stage(
            stage, result, wall_start, iteration,
            stage_run_id=stage_run_id,
            log_file=log_file,
            on_extra_usage=on_extra_usage,
        )

    def _maybe_retry_on_auth_failure(
        self,
        result: StageResult,
        *,
        stage: str,
        pr_url: str | None,
        iteration: int,
        log_file: Path | None,
        stage_run_id: int | None,
        stage_tpl,
        on_context_fill: ContextFillCallback | None,
        extra_usage: bool,
        codex_kwargs: dict,
        wall_start: float,
    ) -> tuple[StageResult, int | None]:
        """If *result* is an auth failure, refresh the token and retry once.

        Returns ``(result, stage_run_id)`` — either the original values
        (if no retry was needed/possible) or the retry result with a new
        stage_run_id.
        """
        from botfarm.supervisor_workers import _classify_failure

        category = _classify_failure(result.error)
        if category != "auth_failure":
            return result, stage_run_id

        logger.warning(
            "Stage '%s' failed with auth_failure for %s — attempting token refresh and retry",
            stage, self.ticket_id,
        )
        insert_event(
            self.conn,
            task_id=self.task_id,
            event_type="auth_retry",
            detail=f"stage={stage} iteration={iteration} error={result.error[:200] if result.error else ''}",
        )
        self.conn.commit()

        from botfarm.credentials import CredentialManager

        cm = CredentialManager()
        new_token = cm.refresh_token()
        if not new_token:
            logger.warning("Token refresh returned None — skipping retry")
            return result, stage_run_id

        logger.info("Token refreshed — retrying stage '%s' for %s", stage, self.ticket_id)

        # Finalize the original stage_run with the auth failure result
        # so that any turns/tokens/cost from the failed attempt are preserved.
        if stage_run_id is not None:
            retry_adapter: AgentAdapter | None = None
            if stage_tpl and self.registry:
                retry_adapter = self.registry.get(stage_tpl.executor_type)
            _record_stage_run(
                self.conn,
                task_id=self.task_id,
                stage=stage,
                result=result,
                wall_elapsed=time.monotonic() - wall_start,
                iteration=iteration,
                stage_run_id=stage_run_id,
                log_file_path=str(log_file) if log_file else None,
                on_extra_usage=extra_usage,
                pipeline_id=self._pipeline_id,
                adapter=retry_adapter,
            )

        # Create a fresh placeholder for the retry (always an agent stage
        # since _maybe_retry_on_auth_failure is only called when uses_agent).
        retry_log_file = _make_stage_log_path(self.log_dir, f"{stage}_auth_retry", iteration)
        new_stage_run_id = insert_stage_run(
            self.conn,
            task_id=self.task_id,
            stage=stage,
            iteration=iteration,
            log_file_path=str(retry_log_file) if retry_log_file else None,
            on_extra_usage=extra_usage,
            pipeline_id=self._pipeline_id,
        )
        self.conn.commit()

        _conn = self.conn
        _sr_id = new_stage_run_id

        def retry_on_context_fill(turn: int, fill_pct: float) -> None:
            update_stage_run_context_fill(_conn, _sr_id, fill_pct)

        try:
            retry_result = _execute_stage(
                stage,
                ticket_id=self.ticket_id,
                ticket_labels=self.ticket_labels,
                pr_url=pr_url,
                cwd=self.cwd,
                max_turns=self.turns_cfg.get(stage, DEFAULT_MAX_TURNS.get(stage, DEFAULT_FIX_MAX_TURNS)),
                pr_checks_timeout=self.pr_checks_timeout,
                log_file=retry_log_file,
                placeholder_branch=self.placeholder_branch,
                env=self._env_for_stage(stage),
                on_context_fill=retry_on_context_fill,
                stage_tpl=stage_tpl,
                registry=self.registry,
                shared_mem_path=self.shared_mem_path,
                prior_context=self.prior_context,
                bugtracker_type=self.bugtracker_type,
                mcp_config=self.mcp_config,
                **codex_kwargs,
            )
        except Exception as exc:
            logger.error("Auth retry for stage '%s' raised: %s", stage, exc)
            if new_stage_run_id is not None:
                delete_stage_run(self.conn, new_stage_run_id)
            # Original stage_run already finalized above; return it so
            # _finish_stage updates it (harmless no-op) and records failure.
            return result, stage_run_id

        # Accumulate the failed attempt's metrics into pipeline totals so
        # _finish_stage (which only sees the retry result) doesn't drop them.
        if result.agent_result:
            self.pipeline.total_turns += result.agent_result.num_turns
            self.pipeline.total_duration_seconds += result.agent_result.duration_seconds

        return retry_result, new_stage_run_id

    def _finish_stage(
        self,
        stage: str,
        result: StageResult,
        wall_start: float,
        iteration: int,
        *,
        stage_run_id: int | None = None,
        log_file: Path | None = None,
        on_extra_usage: bool = False,
    ) -> StageResult | None:
        """Shared logic for recording a stage result and updating metrics.

        If *stage_run_id* is provided, the placeholder row inserted
        before streaming execution is updated in-place instead of
        inserting a new row.
        """
        wall_elapsed = time.monotonic() - wall_start

        # Resolve adapter for cost calculation.
        adapter: AgentAdapter | None = None
        stage_tpl = self._get_stage_tpl(stage)
        if stage_tpl and self.registry:
            adapter = self.registry.get(stage_tpl.executor_type)

        _record_stage_run(
            self.conn,
            task_id=self.task_id,
            stage=stage,
            result=result,
            wall_elapsed=wall_elapsed,
            iteration=iteration,
            stage_run_id=stage_run_id,
            log_file_path=str(log_file) if log_file else None,
            on_extra_usage=on_extra_usage,
            pipeline_id=self._pipeline_id,
            adapter=adapter,
        )

        if result.agent_result:
            self.pipeline.total_turns += result.agent_result.num_turns
            self.pipeline.total_duration_seconds += result.agent_result.duration_seconds
            # Track the last stage's result text for terminal comment posting.
            if result.agent_result.result_text:
                self.pipeline.result_text = result.agent_result.result_text

        if not result.success:
            logger.warning("Stage '%s' failed for %s", stage, self.ticket_id)
            self.pipeline.failure_stage = stage
            self.pipeline.failure_reason = result.error
            _record_failure(self.conn, self.task_id, self.pipeline)
            return None

        if stage not in self.pipeline.stages_completed:
            self.pipeline.stages_completed.append(stage)
        if self.slot_manager and self.project and self.slot_id is not None:
            self.slot_manager.complete_stage(self.project, self.slot_id, stage)

        logger.info(
            "Stage '%s' completed for %s (%.1fs wall)",
            stage, self.ticket_id, wall_elapsed,
        )

        return result


def _run_review_fix_loop(
    ctx: _PipelineContext,
    *,
    pr_url: str | None,
    max_iterations: int,
    start_iteration: int = 1,
) -> bool:
    """Run the review→fix loop.

    Returns ``True`` on success (review approved or max iterations
    reached — pipeline should proceed to pr_checks) and ``False`` if a
    stage failed (failure already recorded by ``run_and_record``).
    """
    actual_iterations = start_iteration - 1
    for iteration in range(start_iteration, max_iterations + 1):
        ctx._refresh_runtime_config()
        # Note: only limit reductions take effect mid-loop; increases
        # require a new loop entry because the range upper bound is fixed.
        if iteration > ctx.max_review_iterations:
            logger.info(
                "max_review_iterations reduced to %d — stopping review loop at iteration %d",
                ctx.max_review_iterations, iteration,
            )
            break

        actual_iterations = iteration

        insert_event(
            ctx.conn,
            task_id=ctx.task_id,
            event_type="review_iteration_started",
            detail=f"iteration={iteration}",
        )
        ctx.conn.commit()

        # --- REVIEW ---
        review_result = ctx.run_and_record("review", pr_url=pr_url, iteration=iteration)
        if review_result is None:
            return False  # failure recorded by run_and_record

        review_state = "approved" if review_result.review_approved else "changes_requested"
        update_task(ctx.conn, ctx.task_id, review_state=review_state)
        ctx.conn.commit()

        if review_result.review_approved:
            insert_event(
                ctx.conn,
                task_id=ctx.task_id,
                event_type="review_approved",
                detail=f"iteration={iteration}",
            )
            ctx.conn.commit()
            update_task(ctx.conn, ctx.task_id, review_iterations=iteration)
            ctx.conn.commit()
            logger.info(
                "Review approved on iteration %d for %s — skipping fix",
                iteration, ctx.ticket_id,
            )
            return True  # success — proceed to pr_checks

        # --- FIX ---
        fix_result = ctx.run_and_record("fix", pr_url=pr_url, iteration=iteration)
        if fix_result is None:
            return False  # failure recorded by run_and_record

        update_task(ctx.conn, ctx.task_id, review_iterations=iteration)
        ctx.conn.commit()

    # Max iterations reached (or limit reduced mid-loop) — proceed to pr_checks regardless
    effective_limit = min(ctx.max_review_iterations, max_iterations)
    insert_event(
        ctx.conn,
        task_id=ctx.task_id,
        event_type="max_iterations_reached",
        detail=f"iterations={actual_iterations}, limit={effective_limit}",
    )
    ctx.conn.commit()
    logger.warning(
        "Max review iterations (%d) reached for %s — proceeding to pr_checks",
        effective_limit, ctx.ticket_id,
    )
    return True


def _reset_for_retry(ctx: _PipelineContext) -> None:
    """Reset pipeline and task state so a retry can proceed.

    After ``run_and_record`` records a failure, this undoes the failure
    side-effects (pipeline flags + task DB status) so the next retry
    iteration starts from a clean state.
    """
    ctx.pipeline.failure_stage = None
    ctx.pipeline.failure_reason = None
    update_task(ctx.conn, ctx.task_id, status="in_progress", failure_reason=None)
    ctx.conn.commit()


def _run_ci_retry_loop(
    ctx: _PipelineContext,
    *,
    pr_url: str | None,
    ci_failure_output: str,
    max_retries: int,
) -> bool:
    """Re-run fix→pr_checks when CI checks fail.

    Returns ``True`` on success (CI checks pass) and ``False`` if the
    fix stage fails or max retries are exhausted (failure already
    recorded by ``run_and_record`` or recorded here).
    """
    actual_retries = 0
    for retry in range(1, max_retries + 1):
        ctx._refresh_runtime_config()
        # Note: only limit reductions take effect mid-loop; increases
        # require a new loop entry because the range upper bound is fixed.
        if retry > ctx.max_ci_retries:
            logger.info(
                "max_ci_retries reduced to %d — stopping CI retry loop at retry %d",
                ctx.max_ci_retries, retry,
            )
            break

        actual_retries = retry

        insert_event(
            ctx.conn,
            task_id=ctx.task_id,
            event_type="ci_retry_started",
            detail=f"retry={retry}, ci_output={ci_failure_output[:RESULT_TRUNCATE_CHARS]}",
        )
        ctx.conn.commit()

        # --- FIX (with CI context) — call _run_ci_fix directly ---
        log_file = _make_stage_log_path(ctx.log_dir, "ci_fix", retry)
        wall_start = time.monotonic()

        # Insert placeholder for live context fill updates
        ci_fix_extra_usage = is_extra_usage_active(ctx.conn)
        ci_fix_stage_run_id = insert_stage_run(
            ctx.conn,
            task_id=ctx.task_id,
            stage="fix",
            iteration=retry,
            log_file_path=str(log_file) if log_file else None,
            on_extra_usage=ci_fix_extra_usage,
            pipeline_id=ctx._pipeline_id,
        )
        ctx.conn.commit()

        _sr_id = ci_fix_stage_run_id

        def _ci_fix_on_fill(turn: int, fill_pct: float) -> None:
            update_stage_run_context_fill(ctx.conn, _sr_id, fill_pct)

        ci_fix_tpl = ctx._get_stage_tpl("ci_fix")
        try:
            fix_result = _run_ci_fix(
                pr_url, ci_failure_output=ci_failure_output,
                cwd=ctx.cwd, max_turns=ctx.turns_cfg.get("ci_fix", ctx.turns_cfg.get("fix", DEFAULT_FIX_MAX_TURNS)),
                log_file=log_file, env=ctx._env_for_stage("ci_fix") or ctx._env_for_stage("fix"),
                on_context_fill=_ci_fix_on_fill,
                stage_tpl=ci_fix_tpl,
                shared_mem_path=ctx.shared_mem_path,
                mcp_config=ctx.mcp_config,
            )
        except Exception as exc:
            logger.error("CI fix stage raised: %s", exc)
            delete_stage_run(ctx.conn, ci_fix_stage_run_id)
            ctx.pipeline.failure_stage = "fix"
            ctx.pipeline.failure_reason = str(exc)[:DETAIL_TRUNCATE_CHARS]
            _record_failure(ctx.conn, ctx.task_id, ctx.pipeline)
            return False

        fix_result = ctx.run_and_record_result(
            "fix", fix_result, wall_start=wall_start, iteration=retry,
            stage_run_id=ci_fix_stage_run_id,
            log_file=log_file,
            on_extra_usage=ci_fix_extra_usage,
        )
        if fix_result is None:
            return False  # failure recorded by run_and_record_result

        # --- PR_CHECKS ---
        checks_result = ctx.run_and_record(
            "pr_checks", pr_url=pr_url, iteration=retry,
        )
        if checks_result is not None:
            # CI passed
            insert_event(
                ctx.conn,
                task_id=ctx.task_id,
                event_type="ci_passed_after_retry",
                detail=f"retry={retry}",
            )
            ctx.conn.commit()
            logger.info(
                "CI checks passed on retry %d for %s",
                retry, ctx.ticket_id,
            )
            return True

        # CI still failing — capture new failure output for next retry
        ci_failure_output = ctx.pipeline.failure_reason or ci_failure_output
        _reset_for_retry(ctx)

    # Max retries exhausted (or limit reduced mid-loop)
    effective_limit = min(ctx.max_ci_retries, max_retries)
    insert_event(
        ctx.conn,
        task_id=ctx.task_id,
        event_type="max_ci_retries_reached",
        detail=f"retries={actual_retries}, limit={effective_limit}",
    )
    ctx.conn.commit()
    logger.warning(
        "Max CI retries (%d) reached for %s — marking as failed",
        effective_limit, ctx.ticket_id,
    )
    ctx.pipeline.failure_stage = "pr_checks"
    ctx.pipeline.failure_reason = f"CI checks failed after {actual_retries} retries (limit={effective_limit})"
    _record_failure(ctx.conn, ctx.task_id, ctx.pipeline)
    return False

def _record_stage_run(
    conn,
    *,
    task_id: int,
    stage: str,
    result: StageResult,
    wall_elapsed: float,
    iteration: int = 1,
    stage_run_id: int | None = None,
    log_file_path: str | None = None,
    on_extra_usage: bool = False,
    pipeline_id: int | None = None,
    adapter: AgentAdapter | None = None,
) -> None:
    """Insert or update a stage_run row from the result.

    Extracts metrics from ``result.agent_result`` (an :class:`AgentResult`).
    When *stage_run_id* is provided (streaming path), the existing
    placeholder row is updated with final metrics.  Otherwise a new
    row is inserted (non-streaming path).

    When *adapter* is provided, cost is calculated via
    ``adapter.calculate_cost()`` instead of using the pre-computed
    ``cost_usd`` on the agent result.
    """
    ar = result.agent_result
    if ar and adapter is not None:
        cost = adapter.calculate_cost(ar)
    else:
        cost = ar.cost_usd if ar else 0.0
    if stage_run_id is not None:
        # Update the placeholder row created before streaming execution
        conn.execute(
            """
            UPDATE stage_runs SET
                session_id = ?, turns = ?, duration_seconds = ?,
                exit_subtype = ?,
                input_tokens = ?, output_tokens = ?,
                cache_read_input_tokens = ?, cache_creation_input_tokens = ?,
                total_cost_usd = ?, context_fill_pct = ?,
                model_usage_json = ?
            WHERE id = ?
            """,
            (
                ar.session_id if ar else None,
                ar.num_turns if ar else 0,
                ar.duration_seconds if ar else wall_elapsed,
                ar.extra.get("exit_subtype") if ar else None,
                ar.input_tokens if ar else 0,
                ar.output_tokens if ar else 0,
                ar.extra.get("cache_read_input_tokens", 0) if ar else 0,
                ar.extra.get("cache_creation_input_tokens", 0) if ar else 0,
                cost,
                ar.context_fill_pct if ar else None,
                ar.extra.get("model_usage_json") if ar else None,
                stage_run_id,
            ),
        )
    else:
        insert_stage_run(
            conn,
            task_id=task_id,
            stage=stage,
            iteration=iteration,
            session_id=ar.session_id if ar else None,
            turns=ar.num_turns if ar else 0,
            duration_seconds=ar.duration_seconds if ar else wall_elapsed,
            exit_subtype=ar.extra.get("exit_subtype") if ar else None,
            input_tokens=ar.input_tokens if ar else 0,
            output_tokens=ar.output_tokens if ar else 0,
            cache_read_input_tokens=ar.extra.get("cache_read_input_tokens", 0) if ar else 0,
            cache_creation_input_tokens=ar.extra.get("cache_creation_input_tokens", 0) if ar else 0,
            total_cost_usd=cost,
            context_fill_pct=ar.context_fill_pct if ar else None,
            model_usage_json=ar.extra.get("model_usage_json") if ar else None,
            log_file_path=log_file_path,
            on_extra_usage=on_extra_usage,
            pipeline_id=pipeline_id,
        )
    conn.commit()


def _record_failure(conn, task_id: int, pipeline: PipelineResult) -> None:
    """Update the task record on pipeline failure."""
    update_task(
        conn,
        task_id,
        status="failed",
        turns=pipeline.total_turns,
        failure_reason=f"{pipeline.failure_stage}: {pipeline.failure_reason}"[:DETAIL_TRUNCATE_CHARS],
        completed_at=datetime.now(timezone.utc).isoformat(),
    )
    conn.commit()
