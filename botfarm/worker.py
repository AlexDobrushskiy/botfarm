"""Worker pipeline: implement → review → fix → pr_checks → merge.

Each stage that requires Claude Code spawns a subprocess via
``claude -p --output-format json --dangerously-skip-permissions --max-turns N``.
The JSON output is parsed for metrics and stored in the ``stage_runs`` table.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from botfarm.db import insert_stage_run, update_task

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stage names — canonical ordering
# ---------------------------------------------------------------------------

STAGES = ("implement", "review", "fix", "pr_checks", "merge")

# Default max-turns per stage (can be overridden per invocation)
DEFAULT_MAX_TURNS: dict[str, int] = {
    "implement": 200,
    "review": 100,
    "fix": 100,
}

# ---------------------------------------------------------------------------
# Claude subprocess result
# ---------------------------------------------------------------------------


@dataclass
class ClaudeResult:
    """Parsed result from a ``claude -p --output-format json`` invocation."""

    session_id: str
    num_turns: int
    duration_seconds: float
    cost_usd: float
    exit_subtype: str
    result_text: str
    is_error: bool = False


def parse_claude_output(raw: str) -> ClaudeResult:
    """Parse the JSON output produced by ``claude -p --output-format json``.

    The expected shape is a JSON object with at least:
    ``session_id``, ``num_turns``, ``duration_ms``, ``cost_usd``,
    ``is_error``, ``result``, and ``subtype``.
    """
    data = json.loads(raw)
    duration_ms = data.get("duration_ms", 0)
    return ClaudeResult(
        session_id=data.get("session_id", ""),
        num_turns=data.get("num_turns", 0),
        duration_seconds=duration_ms / 1000.0,
        cost_usd=data.get("cost_usd", 0.0),
        exit_subtype=data.get("subtype", ""),
        result_text=data.get("result", ""),
        is_error=bool(data.get("is_error", False)),
    )


def run_claude(
    prompt: str,
    *,
    cwd: str | Path,
    max_turns: int,
) -> ClaudeResult:
    """Run ``claude`` as a subprocess and return the parsed result.

    Raises ``subprocess.CalledProcessError`` if the process exits non-zero,
    or ``json.JSONDecodeError`` / ``KeyError`` if the output is unexpected.
    """
    cmd = [
        "claude",
        "-p",
        "--output-format", "json",
        "--dangerously-skip-permissions",
        "--max-turns", str(max_turns),
    ]
    logger.info("Running claude with max_turns=%d in %s", max_turns, cwd)

    proc = subprocess.run(
        cmd,
        input=prompt,
        capture_output=True,
        text=True,
        cwd=str(cwd),
    )
    if proc.returncode != 0:
        logger.error(
            "claude exited with code %d\nstderr: %s",
            proc.returncode,
            proc.stderr[:500] if proc.stderr else "(empty)",
        )
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr
        )

    return parse_claude_output(proc.stdout)


# ---------------------------------------------------------------------------
# Stage result
# ---------------------------------------------------------------------------


@dataclass
class StageResult:
    """Outcome of a single pipeline stage."""

    stage: str
    success: bool
    claude_result: ClaudeResult | None = None
    pr_url: str | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Individual stage implementations
# ---------------------------------------------------------------------------


def _run_implement(ticket_id: str, *, cwd: str | Path, max_turns: int) -> StageResult:
    """IMPLEMENT stage — Claude Code implements the ticket and creates a PR."""
    prompt = (
        f"Work on Linear ticket {ticket_id}. "
        "Follow the Linear Tickets workflow in CLAUDE.md. "
        "Complete all steps through PR creation. Do not stop until the PR is created."
    )
    result = run_claude(prompt, cwd=cwd, max_turns=max_turns)

    if result.is_error:
        return StageResult(
            stage="implement",
            success=False,
            claude_result=result,
            error=f"Claude reported error: {result.result_text[:200]}",
        )

    # Try to extract PR URL from the result text
    pr_url = _extract_pr_url(result.result_text)

    return StageResult(
        stage="implement",
        success=True,
        claude_result=result,
        pr_url=pr_url,
    )


def _run_review(pr_url: str, *, cwd: str | Path, max_turns: int) -> StageResult:
    """REVIEW stage — Fresh Claude Code reviews the PR and posts comments."""
    prompt = (
        f"Review the pull request at {pr_url}. "
        "Read the PR diff carefully. Post your review comments directly on the PR "
        "using 'gh pr review'. Be thorough but constructive."
    )
    result = run_claude(prompt, cwd=cwd, max_turns=max_turns)

    if result.is_error:
        return StageResult(
            stage="review",
            success=False,
            claude_result=result,
            error=f"Claude reported error: {result.result_text[:200]}",
        )

    return StageResult(
        stage="review",
        success=True,
        claude_result=result,
    )


def _run_fix(pr_url: str, *, cwd: str | Path, max_turns: int) -> StageResult:
    """FIX stage — Fresh Claude Code addresses review comments and pushes fixes."""
    prompt = (
        f"Address the review comments on PR {pr_url}. "
        "Read each comment, make the necessary code changes, run tests, "
        "commit and push the fixes."
    )
    result = run_claude(prompt, cwd=cwd, max_turns=max_turns)

    if result.is_error:
        return StageResult(
            stage="fix",
            success=False,
            claude_result=result,
            error=f"Claude reported error: {result.result_text[:200]}",
        )

    return StageResult(
        stage="fix",
        success=True,
        claude_result=result,
    )


def _run_pr_checks(pr_url: str, *, cwd: str | Path, timeout: int = 600) -> StageResult:
    """PR_CHECKS stage — Wait for CI checks to pass. No Claude Code invocation."""
    start = time.monotonic()
    try:
        proc = subprocess.run(
            ["gh", "pr", "checks", pr_url, "--watch"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            timeout=timeout,
        )
        success = proc.returncode == 0
        if success:
            logger.info("CI checks passed in %.1fs", time.monotonic() - start)
        return StageResult(
            stage="pr_checks",
            success=success,
            error=None if success else f"CI checks failed:\n{proc.stdout[:500]}",
        )
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        return StageResult(
            stage="pr_checks",
            success=False,
            error=f"CI checks timed out after {elapsed:.0f}s",
        )


def _run_merge(pr_url: str, *, cwd: str | Path) -> StageResult:
    """MERGE stage — Squash merge the PR and delete the remote branch."""
    proc = subprocess.run(
        ["gh", "pr", "merge", pr_url, "--squash", "--delete-branch"],
        capture_output=True,
        text=True,
        cwd=str(cwd),
    )
    success = proc.returncode == 0
    error = None if success else proc.stderr[:500] if proc.stderr else proc.stdout[:500]
    return StageResult(
        stage="merge",
        success=success,
        error=error,
    )


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
    total_cost_usd: float = 0.0
    total_turns: int = 0
    total_duration_seconds: float = 0.0
    failure_stage: str | None = None
    failure_reason: str | None = None


def run_pipeline(
    *,
    ticket_id: str,
    task_id: int,
    cwd: str | Path,
    conn,
    slot_manager=None,
    project: str | None = None,
    slot_id: int | None = None,
    max_turns: dict[str, int] | None = None,
    pr_checks_timeout: int = 600,
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,  # TODO: wire through to run_claude --resume
) -> PipelineResult:
    """Execute the full implement→review→fix→pr_checks→merge pipeline.

    Args:
        ticket_id: Linear ticket identifier (e.g. "SMA-42").
        task_id: Database task row ID for recording stage runs.
        cwd: Working directory (git worktree) for all commands.
        conn: sqlite3 connection for recording metrics.
        slot_manager: Optional SlotManager for updating slot state.
        project: Project name (required if slot_manager is provided).
        slot_id: Slot ID (required if slot_manager is provided).
        max_turns: Per-stage max_turns overrides.
        pr_checks_timeout: Seconds to wait for CI checks.
        resume_from_stage: If set, skip stages before this one (for limit
            resumption). The pipeline continues from this stage onward.
        resume_session_id: If set, attempt to use ``--resume`` with this
            session ID for the first stage invocation.

    Returns:
        PipelineResult with aggregated metrics.
    """
    turns_cfg = {**DEFAULT_MAX_TURNS, **(max_turns or {})}
    pipeline = PipelineResult(ticket_id=ticket_id, success=False, stages_completed=[])

    pr_url: str | None = None

    # Validate resume_from_stage upfront
    if resume_from_stage and resume_from_stage not in STAGES:
        pipeline.failure_stage = resume_from_stage
        pipeline.failure_reason = f"Unknown resume stage: {resume_from_stage}"
        _record_failure(conn, task_id, pipeline)
        return pipeline

    # When resuming, recover PR URL from existing stage_runs
    if resume_from_stage:
        pr_url = _recover_pr_url(conn, task_id, cwd)
        if pr_url:
            pipeline.pr_url = pr_url

    # Determine which stages to skip when resuming
    skipping = resume_from_stage is not None

    for stage in STAGES:
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

        # Update slot state if manager provided
        if slot_manager and project and slot_id is not None:
            slot_manager.update_stage(project, slot_id, stage=stage)

        wall_start = time.monotonic()

        try:
            result = _execute_stage(
                stage,
                ticket_id=ticket_id,
                pr_url=pr_url,
                cwd=cwd,
                max_turns=turns_cfg.get(stage, 100),
                pr_checks_timeout=pr_checks_timeout,
            )
        except Exception as exc:
            logger.error("Stage '%s' raised: %s", stage, exc)
            pipeline.failure_stage = stage
            pipeline.failure_reason = str(exc)[:500]
            _record_failure(conn, task_id, pipeline)
            return pipeline

        wall_elapsed = time.monotonic() - wall_start

        # Record stage run in DB
        _record_stage_run(
            conn,
            task_id=task_id,
            stage=stage,
            result=result,
            wall_elapsed=wall_elapsed,
        )

        # Accumulate metrics from claude invocations
        if result.claude_result:
            pipeline.total_cost_usd += result.claude_result.cost_usd
            pipeline.total_turns += result.claude_result.num_turns
            pipeline.total_duration_seconds += result.claude_result.duration_seconds

        if not result.success:
            logger.warning("Stage '%s' failed for %s", stage, ticket_id)
            pipeline.failure_stage = stage
            pipeline.failure_reason = result.error
            _record_failure(conn, task_id, pipeline)
            return pipeline

        # Track completed stages
        pipeline.stages_completed.append(stage)
        if slot_manager and project and slot_id is not None:
            slot_manager.complete_stage(project, slot_id, stage)

        # Capture PR URL from implement stage
        if stage == "implement" and result.pr_url:
            pr_url = result.pr_url
            pipeline.pr_url = pr_url
            if slot_manager and project and slot_id is not None:
                slot_manager.set_pr_url(project, slot_id, pr_url)

        # If implement didn't produce a PR URL, we can't continue
        if stage == "implement" and not pr_url:
            pipeline.failure_stage = "implement"
            pipeline.failure_reason = "Implement stage did not produce a PR URL"
            _record_failure(conn, task_id, pipeline)
            return pipeline

        logger.info(
            "Stage '%s' completed for %s (%.1fs wall)",
            stage, ticket_id, wall_elapsed,
        )

    # All stages passed
    pipeline.success = True
    update_task(
        conn,
        task_id,
        status="completed",
        cost_usd=pipeline.total_cost_usd,
        turns=pipeline.total_turns,
        completed_at=datetime.now(timezone.utc).isoformat(),
    )
    conn.commit()

    return pipeline


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _execute_stage(
    stage: str,
    *,
    ticket_id: str,
    pr_url: str | None,
    cwd: str | Path,
    max_turns: int,
    pr_checks_timeout: int,
) -> StageResult:
    """Dispatch to the appropriate stage runner."""
    if stage == "implement":
        return _run_implement(ticket_id, cwd=cwd, max_turns=max_turns)
    elif stage == "review":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_review(pr_url, cwd=cwd, max_turns=max_turns)
    elif stage == "fix":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_fix(pr_url, cwd=cwd, max_turns=max_turns)
    elif stage == "pr_checks":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_pr_checks(pr_url, cwd=cwd, timeout=pr_checks_timeout)
    elif stage == "merge":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_merge(pr_url, cwd=cwd)
    else:
        raise ValueError(f"Unknown stage: {stage}")


def _record_stage_run(
    conn,
    *,
    task_id: int,
    stage: str,
    result: StageResult,
    wall_elapsed: float,
) -> None:
    """Insert a stage_run row from the result."""
    cr = result.claude_result
    insert_stage_run(
        conn,
        task_id=task_id,
        stage=stage,
        session_id=cr.session_id if cr else None,
        turns=cr.num_turns if cr else 0,
        duration_seconds=cr.duration_seconds if cr else wall_elapsed,
        cost_usd=cr.cost_usd if cr else 0.0,
        exit_subtype=cr.exit_subtype if cr else None,
    )
    conn.commit()


def _record_failure(conn, task_id: int, pipeline: PipelineResult) -> None:
    """Update the task record on pipeline failure."""
    update_task(
        conn,
        task_id,
        status="failed",
        cost_usd=pipeline.total_cost_usd,
        turns=pipeline.total_turns,
        failure_reason=f"{pipeline.failure_stage}: {pipeline.failure_reason}"[:500],
        completed_at=datetime.now(timezone.utc).isoformat(),
    )
    conn.commit()


def _extract_pr_url(text: str) -> str | None:
    """Try to extract a GitHub PR URL from text.

    Looks for patterns like https://github.com/<owner>/<repo>/pull/<number>.
    """
    match = re.search(r"https://github\.com/[\w.-]+/[\w.-]+/pull/\d+", text)
    return match.group(0) if match else None


def _recover_pr_url(conn, task_id: int, cwd: str | Path) -> str | None:
    """Recover the PR URL for a resumed pipeline.

    First checks ``gh pr view`` in the working directory, then falls back
    to scanning previous stage_run records in the database.
    """
    # Try gh pr view to get current branch's PR URL
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", "--json", "url", "--jq", ".url"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
        )
        if proc.returncode == 0 and proc.stdout.strip():
            url = proc.stdout.strip()
            if "github.com" in url and "/pull/" in url:
                logger.info("Recovered PR URL from gh: %s", url)
                return url
    except Exception:
        pass

    return None


