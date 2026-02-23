"""Worker pipeline: implement → review → fix → pr_checks → merge.

Each stage that requires Claude Code spawns a subprocess via
``claude -p --output-format json --dangerously-skip-permissions --max-turns N``.
The JSON output is parsed for metrics and stored in the ``stage_runs`` table.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from botfarm.db import insert_event, insert_stage_run, update_task
from botfarm.slots import update_slot_stage

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
    review_approved: bool | None = None


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
    owner, repo, number = _parse_pr_url(pr_url)
    prompt = (
        f"Review the pull request at {pr_url}. "
        "Read the PR diff carefully. Be thorough but constructive.\n\n"
        "For file-specific feedback, post inline review comments on the exact "
        "lines where changes are needed. First get the head SHA with:\n"
        f"  gh pr view {number} --json headRefOid --jq .headRefOid\n"
        "Then post each inline comment using:\n"
        f"  gh api repos/{owner}/{repo}/pulls/{number}/comments "
        "-f body='comment' -f commit_id='HEAD_SHA' -f path='file.py' "
        "-F line=42 -f side='RIGHT'\n\n"
        "After posting all inline comments, submit your overall assessment "
        "using 'gh pr review' with either --approve or --request-changes "
        "and a summary body.\n\n"
        "At the very end of your response, output exactly one of these verdict "
        "markers on its own line:\n"
        "  VERDICT: APPROVED\n"
        "  VERDICT: CHANGES_REQUESTED"
    )
    result = run_claude(prompt, cwd=cwd, max_turns=max_turns)

    if result.is_error:
        return StageResult(
            stage="review",
            success=False,
            claude_result=result,
            error=f"Claude reported error: {result.result_text[:200]}",
        )

    approved = _parse_review_approved(result.result_text)

    return StageResult(
        stage="review",
        success=True,
        claude_result=result,
        review_approved=approved,
    )


def _run_fix(pr_url: str, *, cwd: str | Path, max_turns: int) -> StageResult:
    """FIX stage — Fresh Claude Code addresses review comments and pushes fixes."""
    owner, repo, number = _parse_pr_url(pr_url)
    prompt = (
        f"Address the review comments on PR {pr_url}. "
        "Read both the top-level review comment and any inline review comments "
        f"on specific files/lines. Use 'gh api repos/{owner}/{repo}/pulls/{number}/comments' "
        "to list inline comments. "
        "Make the necessary code changes for each comment, run tests, "
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
            error=None if success else f"CI checks failed:\n{proc.stdout[:2000]}",
        )
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        return StageResult(
            stage="pr_checks",
            success=False,
            error=f"CI checks timed out after {elapsed:.0f}s",
        )


def _run_ci_fix(
    pr_url: str, *, ci_failure_output: str, cwd: str | Path, max_turns: int,
) -> StageResult:
    """FIX stage variant — Claude Code fixes CI failures using CI output context."""
    prompt = (
        f"The CI checks on PR {pr_url} have failed. "
        "Diagnose and fix the CI failures based on the output below, "
        "then run tests locally, commit and push the fixes.\n\n"
        f"CI failure output:\n{ci_failure_output[:2000]}"
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
    state_path: str | Path | None = None,
    max_turns: dict[str, int] | None = None,
    pr_checks_timeout: int = 600,
    max_review_iterations: int = 3,
    max_ci_retries: int = 2,
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,  # TODO: wire through to run_claude --resume
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

    # Shared context for _run_and_record helper
    ctx = _PipelineContext(
        ticket_id=ticket_id,
        task_id=task_id,
        cwd=cwd,
        conn=conn,
        turns_cfg=turns_cfg,
        pr_checks_timeout=pr_checks_timeout,
        pipeline=pipeline,
        slot_manager=slot_manager,
        project=project,
        slot_id=slot_id,
        state_path=state_path,
    )

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

        # ----- Review iteration loop -----
        if stage == "review":
            success = _run_review_fix_loop(ctx, pr_url=pr_url, max_iterations=max_review_iterations)
            if not success:
                return pipeline
            # Ensure both review and fix appear in stages_completed.
            # "review" is already added by run_and_record, but "fix" may
            # not be if the reviewer approved on the first iteration
            # (fix never ran).  We mark it complete so the outer loop's
            # skip-fix guard works and stages_completed matches STAGES.
            if "review" not in pipeline.stages_completed:
                pipeline.stages_completed.append("review")
            if "fix" not in pipeline.stages_completed:
                pipeline.stages_completed.append("fix")
            continue

        if stage == "fix" and "fix" in pipeline.stages_completed:
            # Already handled by _run_review_fix_loop above
            continue

        if stage == "fix":
            # Resuming from fix — run the initial fix, then enter the
            # review iteration loop with remaining iterations so fixes
            # get verified.
            result = ctx.run_and_record("fix", pr_url=pr_url)
            if result is None:
                return pipeline
            update_task(conn, task_id, review_iterations=1)
            conn.commit()
            # Verify fixes via remaining review iterations (iteration 1
            # fix already completed; start review loop from iteration 2).
            if max_review_iterations > 1:
                success = _run_review_fix_loop(
                    ctx, pr_url=pr_url, max_iterations=max_review_iterations,
                    start_iteration=2,
                )
                if not success:
                    return pipeline
            if "review" not in pipeline.stages_completed:
                pipeline.stages_completed.append("review")
            if "fix" not in pipeline.stages_completed:
                pipeline.stages_completed.append("fix")
            continue

        # ----- CI retry loop -----
        if stage == "pr_checks":
            result = ctx.run_and_record("pr_checks", pr_url=pr_url)
            if result is not None:
                # CI passed on first try
                continue
            # CI failed — attempt retries if configured
            if max_ci_retries > 0:
                ci_failure_output = pipeline.failure_reason or "CI checks failed"
                _reset_for_retry(ctx)
                success = _run_ci_retry_loop(
                    ctx,
                    pr_url=pr_url,
                    ci_failure_output=ci_failure_output,
                    max_retries=max_ci_retries,
                )
                if not success:
                    return pipeline
                # Ensure pr_checks is marked completed
                if "pr_checks" not in pipeline.stages_completed:
                    pipeline.stages_completed.append("pr_checks")
                continue
            # No retries configured — fail
            return pipeline

        # ----- Normal stages (implement, merge) -----
        result = ctx.run_and_record(stage, pr_url=pr_url)
        if result is None:
            return pipeline

        # Capture PR URL from implement stage
        if stage == "implement" and result.pr_url:
            pr_url = result.pr_url
            pipeline.pr_url = pr_url
            update_task(conn, task_id, pr_url=pr_url)
            conn.commit()
            if slot_manager and project and slot_id is not None:
                slot_manager.set_pr_url(project, slot_id, pr_url)

        # If implement didn't produce a PR URL, we can't continue
        if stage == "implement" and not pr_url:
            pipeline.failure_stage = "implement"
            pipeline.failure_reason = "Implement stage did not produce a PR URL"
            _record_failure(conn, task_id, pipeline)
            return pipeline

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


@dataclass
class _PipelineContext:
    """Shared state passed to pipeline helpers to avoid long argument lists."""

    ticket_id: str
    task_id: int
    cwd: str | Path
    conn: sqlite3.Connection
    turns_cfg: dict[str, int]
    pr_checks_timeout: int
    pipeline: PipelineResult
    slot_manager: object | None = None
    project: str | None = None
    slot_id: int | None = None
    state_path: str | Path | None = None

    def run_and_record(
        self,
        stage: str,
        *,
        pr_url: str | None,
        iteration: int = 1,
    ) -> StageResult | None:
        """Execute a stage, record it, accumulate metrics.

        Returns the ``StageResult`` on success, or ``None`` if the stage
        failed (in which case ``pipeline`` is updated with the failure).
        """
        if self.state_path and self.project and self.slot_id is not None:
            update_slot_stage(
                self.state_path,
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

        wall_start = time.monotonic()

        try:
            result = _execute_stage(
                stage,
                ticket_id=self.ticket_id,
                pr_url=pr_url,
                cwd=self.cwd,
                max_turns=self.turns_cfg.get(stage, 100),
                pr_checks_timeout=self.pr_checks_timeout,
            )
        except Exception as exc:
            logger.error("Stage '%s' raised: %s", stage, exc)
            self.pipeline.failure_stage = stage
            self.pipeline.failure_reason = str(exc)[:500]
            _record_failure(self.conn, self.task_id, self.pipeline)
            return None

        return self._finish_stage(stage, result, wall_start, iteration)

    def run_and_record_result(
        self,
        stage: str,
        result: StageResult,
        *,
        wall_start: float,
        iteration: int = 1,
    ) -> StageResult | None:
        """Record a pre-computed StageResult, accumulate metrics.

        Like ``run_and_record`` but skips execution — the caller already
        ran the stage directly (e.g. ``_run_ci_fix``).

        Returns the ``StageResult`` on success, or ``None`` on failure.
        """
        return self._finish_stage(stage, result, wall_start, iteration)

    def _finish_stage(
        self,
        stage: str,
        result: StageResult,
        wall_start: float,
        iteration: int,
    ) -> StageResult | None:
        """Shared logic for recording a stage result and updating metrics."""
        wall_elapsed = time.monotonic() - wall_start

        _record_stage_run(
            self.conn,
            task_id=self.task_id,
            stage=stage,
            result=result,
            wall_elapsed=wall_elapsed,
            iteration=iteration,
        )

        if result.claude_result:
            self.pipeline.total_cost_usd += result.claude_result.cost_usd
            self.pipeline.total_turns += result.claude_result.num_turns
            self.pipeline.total_duration_seconds += result.claude_result.duration_seconds

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
    for iteration in range(start_iteration, max_iterations + 1):
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

    # Max iterations reached — proceed to pr_checks regardless
    insert_event(
        ctx.conn,
        task_id=ctx.task_id,
        event_type="max_iterations_reached",
        detail=f"iterations={max_iterations}",
    )
    ctx.conn.commit()
    logger.warning(
        "Max review iterations (%d) reached for %s — proceeding to pr_checks",
        max_iterations, ctx.ticket_id,
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
    for retry in range(1, max_retries + 1):
        insert_event(
            ctx.conn,
            task_id=ctx.task_id,
            event_type="ci_retry_started",
            detail=f"retry={retry}, ci_output={ci_failure_output[:200]}",
        )
        ctx.conn.commit()

        # --- FIX (with CI context) — call _run_ci_fix directly ---
        wall_start = time.monotonic()
        try:
            fix_result = _run_ci_fix(
                pr_url, ci_failure_output=ci_failure_output,
                cwd=ctx.cwd, max_turns=ctx.turns_cfg.get("fix", 100),
            )
        except Exception as exc:
            logger.error("CI fix stage raised: %s", exc)
            ctx.pipeline.failure_stage = "fix"
            ctx.pipeline.failure_reason = str(exc)[:500]
            _record_failure(ctx.conn, ctx.task_id, ctx.pipeline)
            return False

        fix_result = ctx.run_and_record_result(
            "fix", fix_result, wall_start=wall_start, iteration=retry,
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

    # Max retries exhausted
    insert_event(
        ctx.conn,
        task_id=ctx.task_id,
        event_type="max_ci_retries_reached",
        detail=f"retries={max_retries}",
    )
    ctx.conn.commit()
    logger.warning(
        "Max CI retries (%d) reached for %s — marking as failed",
        max_retries, ctx.ticket_id,
    )
    ctx.pipeline.failure_stage = "pr_checks"
    ctx.pipeline.failure_reason = f"CI checks failed after {max_retries} retries"
    _record_failure(ctx.conn, ctx.task_id, ctx.pipeline)
    return False


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
    iteration: int = 1,
) -> None:
    """Insert a stage_run row from the result."""
    cr = result.claude_result
    insert_stage_run(
        conn,
        task_id=task_id,
        stage=stage,
        iteration=iteration,
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


_PR_URL_RE = re.compile(
    r"https://github\.com/([\w.-]+)/([\w.-]+)/pull/(\d+)",
)


def _extract_pr_url(text: str) -> str | None:
    """Try to extract a GitHub PR URL from text.

    Looks for patterns like https://github.com/<owner>/<repo>/pull/<number>.
    """
    match = _PR_URL_RE.search(text)
    return match.group(0) if match else None


def _parse_pr_url(pr_url: str) -> tuple[str, str, str]:
    """Extract (owner, repo, number) from a GitHub PR URL.

    Raises ``ValueError`` if the URL doesn't match the expected pattern.
    """
    match = _PR_URL_RE.match(pr_url)
    if not match:
        raise ValueError(f"Cannot parse PR URL: {pr_url}")
    return match.group(1), match.group(2), match.group(3)


def _parse_review_approved(text: str) -> bool:
    """Determine whether a review result indicates approval or changes requested.

    First checks for structured verdict markers (``VERDICT: APPROVED`` or
    ``VERDICT: CHANGES_REQUESTED``) anywhere in the last 500 characters.
    Falls back to heuristic keyword matching if no marker is found.

    Returns ``True`` if the review text signals approval, ``False`` if
    changes are requested, and ``False`` as a conservative default when
    the signal is ambiguous.
    """
    tail = text[-500:]
    # Structured verdict marker — strongest signal
    verdict_match = re.search(r"VERDICT:\s*(APPROVED|CHANGES_REQUESTED)", tail)
    if verdict_match:
        return verdict_match.group(1) == "APPROVED"

    lower = tail.lower()
    # gh pr review --approve is a strong signal
    if "--approve" in lower:
        return True
    # Explicit approval phrases
    approve_patterns = [
        "approve the pr",
        "i approve",
        "lgtm",
        "looks good to me",
        "approved the pr",
        "approved this pr",
        "pr is approved",
    ]
    changes_patterns = [
        "request changes",
        "changes requested",
        "--request-changes",
        "requesting changes",
        "needs changes",
    ]
    for pattern in changes_patterns:
        if pattern in lower:
            return False
    for pattern in approve_patterns:
        if pattern in lower:
            return True
    # Conservative default: assume changes are needed
    return False


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


