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
import sqlite3
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from botfarm.db import delete_stage_run, insert_event, insert_stage_run, update_stage_run_context_fill, update_task
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
    exit_subtype: str
    result_text: str
    is_error: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    total_cost_usd: float = 0.0
    context_fill_pct: float | None = None
    model_usage_json: str | None = None


def parse_claude_output(raw: str) -> ClaudeResult:
    """Parse the JSON output produced by ``claude -p --output-format json``.

    The expected shape is a JSON object with at least:
    ``session_id``, ``num_turns``, ``duration_ms``,
    ``is_error``, ``result``, and ``subtype``.

    Also extracts token usage data from the ``usage`` and ``modelUsage``
    objects when present, and computes ``context_fill_pct`` from the
    primary model's context window.
    """
    data = json.loads(raw)
    duration_ms = data.get("duration_ms", 0)

    # Token usage from top-level "usage" object
    usage = data.get("usage") or {}
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    cache_creation = usage.get("cache_creation_input_tokens", 0)

    total_cost_usd = data.get("total_cost_usd", 0.0) or 0.0

    # Per-model breakdown
    model_usage = data.get("modelUsage")
    model_usage_json = json.dumps(model_usage) if model_usage else None

    # Compute context fill % from the primary model's contextWindow
    context_fill_pct = _compute_context_fill(input_tokens, cache_creation, output_tokens, model_usage)

    return ClaudeResult(
        session_id=data.get("session_id", ""),
        num_turns=data.get("num_turns", 0),
        duration_seconds=duration_ms / 1000.0,
        exit_subtype=data.get("subtype", ""),
        result_text=data.get("result", ""),
        is_error=bool(data.get("is_error", False)),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_input_tokens=cache_read,
        cache_creation_input_tokens=cache_creation,
        total_cost_usd=total_cost_usd,
        context_fill_pct=context_fill_pct,
        model_usage_json=model_usage_json,
    )


def _compute_context_fill(
    input_tokens: int,
    cache_creation_input_tokens: int,
    output_tokens: int,
    model_usage: dict | None,
) -> float | None:
    """Compute context fill percentage from the primary model's context window.

    Returns ``None`` when ``modelUsage`` is absent or has no ``contextWindow``.
    """
    if not model_usage:
        return None
    # Pick the first model entry that has a contextWindow
    for _model, info in model_usage.items():
        context_window = info.get("contextWindow")
        if context_window and context_window > 0:
            unique_tokens = input_tokens + cache_creation_input_tokens + output_tokens
            return round(unique_tokens / context_window * 100, 2)
    return None


def run_claude(
    prompt: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
) -> ClaudeResult:
    """Run ``claude`` as a subprocess and return the parsed result.

    If *log_file* is provided, the raw stdout and stderr from the claude
    subprocess are written to that file for later inspection.

    If *env* is provided, those entries are merged into the current
    process environment for the subprocess (e.g. ``BOTFARM_DB_PATH``
    for DB isolation).

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

    subprocess_env = None
    if env:
        subprocess_env = {**os.environ, **env}

    proc = subprocess.run(
        cmd,
        input=prompt,
        capture_output=True,
        text=True,
        cwd=str(cwd),
        env=subprocess_env,
    )

    # Persist raw subprocess output to disk when a log file is configured
    if log_file is not None:
        _write_subprocess_log(log_file, proc.stdout, proc.stderr)

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


# Default context window size for per-turn fill calculation when the final
# result hasn't been received yet.  200k is the current window for Claude
# Opus/Sonnet models.
_DEFAULT_CONTEXT_WINDOW = 200_000

# Type alias for the per-turn context fill callback.
ContextFillCallback = Callable[[int, float], None]


def _compute_turn_context_fill(
    usage: dict,
    context_window: int,
) -> float | None:
    """Compute context fill % from a single assistant turn's usage data.

    Unlike ``_compute_context_fill()`` (which uses cumulative data and
    excludes cache_read to avoid double-counting), this per-turn formula
    **includes** cache_read_input_tokens because per-API-call usage
    reports the full cached input read at each turn.
    """
    input_tokens = usage.get("input_tokens", 0)
    cache_creation = usage.get("cache_creation_input_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)

    total = input_tokens + cache_creation + cache_read + output_tokens
    if context_window <= 0 or total == 0:
        return None
    return round(total / context_window * 100, 2)


def parse_stream_json_result(result_data: dict) -> ClaudeResult:
    """Parse a stream-json ``result`` message into a ``ClaudeResult``.

    The ``result`` message in stream-json contains the same fields as the
    single JSON blob from ``--output-format json``.  Finds and parses the
    ``result`` line from NDJSON stream output.
    """
    duration_ms = result_data.get("duration_ms", 0)

    usage = result_data.get("usage") or {}
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    cache_read = usage.get("cache_read_input_tokens", 0)
    cache_creation = usage.get("cache_creation_input_tokens", 0)

    total_cost_usd = result_data.get("total_cost_usd", 0.0) or 0.0

    model_usage = result_data.get("modelUsage")
    model_usage_json = json.dumps(model_usage) if model_usage else None

    context_fill_pct = _compute_context_fill(
        input_tokens, cache_creation, output_tokens, model_usage,
    )

    return ClaudeResult(
        session_id=result_data.get("session_id", ""),
        num_turns=result_data.get("num_turns", 0),
        duration_seconds=duration_ms / 1000.0,
        exit_subtype=result_data.get("subtype", ""),
        result_text=result_data.get("result", ""),
        is_error=bool(result_data.get("is_error", False)),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_input_tokens=cache_read,
        cache_creation_input_tokens=cache_creation,
        total_cost_usd=total_cost_usd,
        context_fill_pct=context_fill_pct,
        model_usage_json=model_usage_json,
    )


def run_claude_streaming(
    prompt: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    timeout: float | None = None,
) -> ClaudeResult:
    """Run ``claude`` with streaming output and per-turn context fill callbacks.

    Uses ``--output-format stream-json --verbose`` to receive NDJSON
    output line-by-line.  On each ``assistant`` message, computes the
    current context fill % and invokes *on_context_fill(turn_number,
    context_fill_pct)* if provided.

    The final ``result`` message is parsed into a ``ClaudeResult``
    identical to what ``run_claude()`` returns.

    Stderr is drained on a separate thread to prevent pipe deadlocks.

    If *timeout* is given (in seconds), a watchdog thread kills the
    process when the deadline is exceeded and ``TimeoutError`` is raised.
    If *log_file* is given, each NDJSON line is flushed to disk in
    real-time so that external tools can tail the log during execution.
    """
    cmd = [
        "claude",
        "-p",
        "--output-format", "stream-json",
        "--verbose",
        "--dangerously-skip-permissions",
        "--max-turns", str(max_turns),
    ]
    logger.info("Running claude (streaming) with max_turns=%d in %s", max_turns, cwd)

    subprocess_env = None
    if env:
        subprocess_env = {**os.environ, **env}

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(cwd),
        env=subprocess_env,
    )

    # Write prompt and close stdin
    proc.stdin.write(prompt)
    proc.stdin.close()

    # Drain stderr on a background thread to prevent deadlock
    stderr_lines: list[str] = []

    def _drain_stderr():
        for line in proc.stderr:
            stderr_lines.append(line)

    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    # Watchdog: kill the process if it exceeds the timeout.
    # Uses two events: ``cancel_watchdog`` to stop the timer early (normal
    # completion) and ``timed_out`` so the main thread can detect a timeout.
    cancel_watchdog = threading.Event()
    timed_out = threading.Event()

    def _watchdog():
        if not cancel_watchdog.wait(timeout):
            # Timeout elapsed before cancellation — kill the process
            timed_out.set()
            try:
                proc.kill()
            except OSError:
                pass

    watchdog_thread: threading.Thread | None = None
    if timeout is not None and timeout > 0:
        watchdog_thread = threading.Thread(target=_watchdog, daemon=True)
        watchdog_thread.start()

    # Open log file for real-time writing (if configured)
    log_fh = None
    if log_file is not None:
        try:
            log_file.parent.mkdir(parents=True, exist_ok=True)
            log_fh = log_file.open("w")
        except OSError:
            logger.warning("Failed to open log file %s for streaming", log_file, exc_info=True)

    # Read stdout line-by-line (NDJSON)
    stdout_lines: list[str] = []
    turn_number = 0
    claude_result: ClaudeResult | None = None

    try:
        for line in proc.stdout:
            stdout_lines.append(line)

            # Write to log file in real-time
            if log_fh is not None:
                try:
                    log_fh.write(line)
                    log_fh.flush()
                except OSError:
                    pass

            stripped = line.strip()
            if not stripped:
                continue

            try:
                event = json.loads(stripped)
            except json.JSONDecodeError:
                logger.debug("Skipping non-JSON stream line: %s", stripped[:100])
                continue

            msg_type = event.get("type")

            if msg_type == "assistant":
                turn_number += 1
                usage = (event.get("message") or {}).get("usage")
                if usage and on_context_fill is not None:
                    fill_pct = _compute_turn_context_fill(usage, _DEFAULT_CONTEXT_WINDOW)
                    if fill_pct is not None:
                        try:
                            on_context_fill(turn_number, fill_pct)
                        except Exception:
                            logger.debug(
                                "on_context_fill callback failed", exc_info=True,
                            )

            elif msg_type == "result":
                claude_result = parse_stream_json_result(event)
    finally:
        # Cancel watchdog
        cancel_watchdog.set()
        if watchdog_thread is not None:
            watchdog_thread.join(timeout=2)

    proc.wait()
    stderr_thread.join(timeout=5)

    stdout_text = "".join(stdout_lines)
    stderr_text = "".join(stderr_lines)

    # Append stderr to log file and close
    if log_fh is not None:
        try:
            if stderr_text:
                log_fh.write("\n--- STDERR ---\n")
                log_fh.write(stderr_text)
        except OSError:
            pass
        finally:
            log_fh.close()

    if timed_out.is_set() and proc.returncode in (-9, 137, None):
        raise TimeoutError(
            f"claude streaming process timed out after {timeout}s"
        )

    if proc.returncode != 0:
        logger.error(
            "claude (streaming) exited with code %d\nstderr: %s",
            proc.returncode,
            stderr_text[:500] if stderr_text else "(empty)",
        )
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=stdout_text, stderr=stderr_text,
        )

    if claude_result is None:
        raise RuntimeError(
            "claude stream-json produced no result message; "
            f"stdout length={len(stdout_text)}"
        )

    return claude_result


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
# Claude invocation helper
# ---------------------------------------------------------------------------


def _invoke_claude(
    prompt: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    timeout: float | None = None,
) -> ClaudeResult:
    """Run Claude via streaming or non-streaming path depending on callback."""
    if on_context_fill is not None:
        return run_claude_streaming(
            prompt, cwd=cwd, max_turns=max_turns,
            log_file=log_file, env=env, on_context_fill=on_context_fill,
            timeout=timeout,
        )
    return run_claude(prompt, cwd=cwd, max_turns=max_turns, log_file=log_file, env=env)


# ---------------------------------------------------------------------------
# Individual stage implementations
# ---------------------------------------------------------------------------


def _run_implement(
    ticket_id: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
) -> StageResult:
    """IMPLEMENT stage — Claude Code implements the ticket and creates a PR."""
    prompt = (
        f"Work on Linear ticket {ticket_id}. "
        "Follow the Linear Tickets workflow in CLAUDE.md. "
        "Complete all steps through PR creation. Do not stop until the PR is created."
    )
    result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )

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


def _run_review(
    pr_url: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
) -> StageResult:
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
        "IMPORTANT: If you posted ANY inline comments with suggestions, issues, "
        "or actionable feedback, you MUST use --request-changes and output "
        "VERDICT: CHANGES_REQUESTED. Only use --approve and VERDICT: APPROVED "
        "when there are ZERO actionable inline comments.\n\n"
        "At the very end of your response, output exactly one of these verdict "
        "markers on its own line:\n"
        "  VERDICT: APPROVED\n"
        "  VERDICT: CHANGES_REQUESTED"
    )
    result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )

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


def _run_fix(
    pr_url: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
) -> StageResult:
    """FIX stage — Fresh Claude Code addresses review comments and pushes fixes."""
    owner, repo, number = _parse_pr_url(pr_url)
    prompt = (
        f"Address the review comments on PR {pr_url}. "
        "Read both the top-level review comment and any inline review comments "
        f"on specific files/lines. Use 'gh api repos/{owner}/{repo}/pulls/{number}/comments' "
        "to list inline comments. Note each comment's `id` field from the API response.\n\n"
        "Make the necessary code changes for each comment, run tests, "
        "commit and push the fixes.\n\n"
        "After addressing (or deciding to skip) each inline comment, reply to it using:\n"
        f"  gh api repos/{owner}/{repo}/pulls/{number}/comments/COMMENT_ID/replies -f body='...'\n"
        "Replace COMMENT_ID with the comment's `id` from the earlier API response.\n\n"
        "Reply guidelines:\n"
        "- If fixed and obvious from context: reply \"Fixed\"\n"
        "- If fixed but clarification helps: reply \"Fixed — [what changed]\"\n"
        "- If intentionally not fixed: reply with a brief explanation why"
    )
    result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )

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


def _run_pr_checks(
    pr_url: str,
    *,
    cwd: str | Path,
    timeout: int = 600,
    log_file: Path | None = None,
) -> StageResult:
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
        if log_file is not None:
            _write_subprocess_log(log_file, proc.stdout, proc.stderr)
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
    pr_url: str,
    *,
    ci_failure_output: str,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
) -> StageResult:
    """FIX stage variant — Claude Code fixes CI failures using CI output context."""
    prompt = (
        f"The CI checks on PR {pr_url} have failed. "
        "Diagnose and fix the CI failures based on the output below, "
        "then run tests locally, commit and push the fixes.\n\n"
        f"CI failure output:\n{ci_failure_output[:2000]}"
    )
    result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )

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


def _is_protected_branch(branch: str) -> bool:
    """Return True if the branch must never be deleted."""
    if branch == "main":
        return True
    if re.match(r"^slot-\d+-placeholder$", branch):
        return True
    return False


def _run_merge(
    pr_url: str,
    *,
    cwd: str | Path,
    log_file: Path | None = None,
    placeholder_branch: str | None = None,
) -> StageResult:
    """MERGE stage — Squash merge the PR, then checkout placeholder branch."""
    # Capture the current branch name before merge/checkout so we can
    # delete the actual feature branch afterwards (not a stale name
    # threaded through the pipeline).
    feature_branch: str | None = None
    rev_parse = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        cwd=str(cwd),
    )
    if rev_parse.returncode == 0:
        feature_branch = rev_parse.stdout.strip() or None
    else:
        logger.warning(
            "Could not determine current branch: %s",
            rev_parse.stderr[:300],
        )

    proc = subprocess.run(
        ["gh", "pr", "merge", pr_url, "--squash"],
        capture_output=True,
        text=True,
        cwd=str(cwd),
    )
    if log_file is not None:
        _write_subprocess_log(log_file, proc.stdout, proc.stderr)
    if proc.returncode != 0:
        # The merge command failed, but the PR may have actually been merged
        # (e.g. post-merge git checkout fails in worktree environments).
        # Check the actual PR state before reporting failure.
        if _check_pr_merged(pr_url, cwd):
            logger.info(
                "gh pr merge returned non-zero but PR is actually merged: %s",
                pr_url,
            )
        else:
            error = proc.stderr[:500] if proc.stderr else proc.stdout[:500]
            return StageResult(stage="merge", success=False, error=error)

    # Switch worktree back to its placeholder branch so it doesn't hold
    # the now-deleted feature branch (which causes issues on next dispatch).
    if placeholder_branch:
        checkout = subprocess.run(
            ["git", "checkout", placeholder_branch],
            capture_output=True,
            text=True,
            cwd=str(cwd),
        )
        if checkout.returncode != 0:
            logger.warning(
                "Post-merge checkout of '%s' failed: %s",
                placeholder_branch,
                checkout.stderr[:300],
            )

    # Delete the local feature branch to prevent stale branch accumulation.
    if feature_branch:
        if _is_protected_branch(feature_branch):
            logger.warning(
                "Refusing to delete protected branch '%s'", feature_branch,
            )
        else:
            delete = subprocess.run(
                ["git", "branch", "-D", feature_branch],
                capture_output=True,
                text=True,
                cwd=str(cwd),
            )
            if delete.returncode != 0:
                logger.warning(
                    "Failed to delete local feature branch '%s': %s",
                    feature_branch,
                    delete.stderr[:300],
                )
            else:
                logger.info(
                    "Deleted local feature branch '%s'", feature_branch,
                )

    return StageResult(stage="merge", success=True)


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


def run_pipeline(
    *,
    ticket_id: str,
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
    pr_checks_timeout: int = 600,
    max_review_iterations: int = 3,
    max_ci_retries: int = 2,
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,  # TODO: wire through to run_claude --resume
    slot_db_path: str | Path | None = None,
    pause_event: multiprocessing.Event | None = None,
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

    # Build env overrides for Claude subprocesses (DB isolation)
    subprocess_env = None
    if slot_db_path:
        subprocess_env = {"BOTFARM_DB_PATH": str(slot_db_path)}

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
        db_path=db_path,
        log_dir=Path(log_dir) if log_dir else None,
        placeholder_branch=placeholder_branch,
        subprocess_env=subprocess_env,
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

        # Check for manual pause request between stages
        if pause_event is not None and pause_event.is_set():
            logger.info(
                "Manual pause requested — exiting pipeline after stage '%s' for %s",
                pipeline.stages_completed[-1] if pipeline.stages_completed else "(none)",
                ticket_id,
            )
            pipeline.paused = True
            return pipeline

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
    db_path: str | Path | None = None
    log_dir: Path | None = None
    placeholder_branch: str | None = None
    subprocess_env: dict[str, str] | None = None

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

        # For Claude-based stages, insert a placeholder stage_run row
        # so per-turn context fill updates have a target row to update.
        on_context_fill: ContextFillCallback | None = None
        stage_run_id: int | None = None
        uses_claude = stage in ("implement", "review", "fix")

        if uses_claude:
            stage_run_id = insert_stage_run(
                self.conn,
                task_id=self.task_id,
                stage=stage,
                iteration=iteration,
                log_file_path=str(log_file) if log_file else None,
            )
            self.conn.commit()

            _conn = self.conn
            _sr_id = stage_run_id

            def on_context_fill(turn: int, fill_pct: float) -> None:
                update_stage_run_context_fill(_conn, _sr_id, fill_pct)

        try:
            result = _execute_stage(
                stage,
                ticket_id=self.ticket_id,
                pr_url=pr_url,
                cwd=self.cwd,
                max_turns=self.turns_cfg.get(stage, 100),
                pr_checks_timeout=self.pr_checks_timeout,
                log_file=log_file,
                placeholder_branch=self.placeholder_branch,
                env=self.subprocess_env,
                on_context_fill=on_context_fill,
            )
        except Exception as exc:
            logger.error("Stage '%s' raised: %s", stage, exc)
            if stage_run_id is not None:
                delete_stage_run(self.conn, stage_run_id)
            self.pipeline.failure_stage = stage
            self.pipeline.failure_reason = str(exc)[:500]
            _record_failure(self.conn, self.task_id, self.pipeline)
            return None

        return self._finish_stage(
            stage, result, wall_start, iteration,
            stage_run_id=stage_run_id,
            log_file=log_file,
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
        )

    def _finish_stage(
        self,
        stage: str,
        result: StageResult,
        wall_start: float,
        iteration: int,
        *,
        stage_run_id: int | None = None,
        log_file: Path | None = None,
    ) -> StageResult | None:
        """Shared logic for recording a stage result and updating metrics.

        If *stage_run_id* is provided, the placeholder row inserted
        before streaming execution is updated in-place instead of
        inserting a new row.
        """
        wall_elapsed = time.monotonic() - wall_start

        _record_stage_run(
            self.conn,
            task_id=self.task_id,
            stage=stage,
            result=result,
            wall_elapsed=wall_elapsed,
            iteration=iteration,
            stage_run_id=stage_run_id,
            log_file_path=str(log_file) if log_file else None,
        )

        if result.claude_result:
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
        log_file = _make_stage_log_path(ctx.log_dir, "ci_fix", retry)
        wall_start = time.monotonic()

        # Insert placeholder for live context fill updates
        ci_fix_stage_run_id = insert_stage_run(
            ctx.conn,
            task_id=ctx.task_id,
            stage="fix",
            iteration=retry,
            log_file_path=str(log_file) if log_file else None,
        )
        ctx.conn.commit()

        _sr_id = ci_fix_stage_run_id

        def _ci_fix_on_fill(turn: int, fill_pct: float) -> None:
            update_stage_run_context_fill(ctx.conn, _sr_id, fill_pct)

        try:
            fix_result = _run_ci_fix(
                pr_url, ci_failure_output=ci_failure_output,
                cwd=ctx.cwd, max_turns=ctx.turns_cfg.get("fix", 100),
                log_file=log_file, env=ctx.subprocess_env,
                on_context_fill=_ci_fix_on_fill,
            )
        except Exception as exc:
            logger.error("CI fix stage raised: %s", exc)
            delete_stage_run(ctx.conn, ci_fix_stage_run_id)
            ctx.pipeline.failure_stage = "fix"
            ctx.pipeline.failure_reason = str(exc)[:500]
            _record_failure(ctx.conn, ctx.task_id, ctx.pipeline)
            return False

        fix_result = ctx.run_and_record_result(
            "fix", fix_result, wall_start=wall_start, iteration=retry,
            stage_run_id=ci_fix_stage_run_id,
            log_file=log_file,
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
    log_file: Path | None = None,
    placeholder_branch: str | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
) -> StageResult:
    """Dispatch to the appropriate stage runner."""
    if stage == "implement":
        return _run_implement(
            ticket_id, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
        )
    elif stage == "review":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_review(
            pr_url, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
        )
    elif stage == "fix":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_fix(
            pr_url, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
        )
    elif stage == "pr_checks":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_pr_checks(pr_url, cwd=cwd, timeout=pr_checks_timeout, log_file=log_file)
    elif stage == "merge":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_merge(
            pr_url,
            cwd=cwd,
            log_file=log_file,
            placeholder_branch=placeholder_branch,
        )
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
    stage_run_id: int | None = None,
    log_file_path: str | None = None,
) -> None:
    """Insert or update a stage_run row from the result.

    When *stage_run_id* is provided (streaming path), the existing
    placeholder row is updated with final metrics.  Otherwise a new
    row is inserted (non-streaming path).
    """
    cr = result.claude_result
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
                cr.session_id if cr else None,
                cr.num_turns if cr else 0,
                cr.duration_seconds if cr else wall_elapsed,
                cr.exit_subtype if cr else None,
                cr.input_tokens if cr else 0,
                cr.output_tokens if cr else 0,
                cr.cache_read_input_tokens if cr else 0,
                cr.cache_creation_input_tokens if cr else 0,
                cr.total_cost_usd if cr else 0.0,
                cr.context_fill_pct if cr else None,
                cr.model_usage_json if cr else None,
                stage_run_id,
            ),
        )
    else:
        insert_stage_run(
            conn,
            task_id=task_id,
            stage=stage,
            iteration=iteration,
            session_id=cr.session_id if cr else None,
            turns=cr.num_turns if cr else 0,
            duration_seconds=cr.duration_seconds if cr else wall_elapsed,
            exit_subtype=cr.exit_subtype if cr else None,
            input_tokens=cr.input_tokens if cr else 0,
            output_tokens=cr.output_tokens if cr else 0,
            cache_read_input_tokens=cr.cache_read_input_tokens if cr else 0,
            cache_creation_input_tokens=cr.cache_creation_input_tokens if cr else 0,
            total_cost_usd=cr.total_cost_usd if cr else 0.0,
            context_fill_pct=cr.context_fill_pct if cr else None,
            model_usage_json=cr.model_usage_json if cr else None,
            log_file_path=log_file_path,
        )
    conn.commit()


def _record_failure(conn, task_id: int, pipeline: PipelineResult) -> None:
    """Update the task record on pipeline failure."""
    update_task(
        conn,
        task_id,
        status="failed",
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

    Searches the **entire** ``text`` for structured verdict markers
    (``VERDICT: APPROVED`` or ``VERDICT: CHANGES_REQUESTED``) using a
    case-insensitive regex.  Falls back to ``gh pr review`` command
    detection, then keyword heuristics on the last 500 characters.

    Returns ``True`` if the review text signals approval, ``False`` if
    changes are requested, and ``False`` as a conservative default when
    the signal is ambiguous.
    """
    # 1. Structured verdict marker — strongest signal, full text, case-insensitive.
    #    Use findall + last match so early occurrences (e.g. quoted from test code
    #    the reviewer is discussing) don't shadow the real verdict at the end.
    verdict_matches = re.findall(
        r"VERDICT:\s*(APPROVED|CHANGES_REQUESTED)", text, re.IGNORECASE
    )
    if verdict_matches:
        return verdict_matches[-1].upper() == "APPROVED"

    # 2. gh pr review command — search full text, check reject first (conservative)
    if re.search(r"gh\s+pr\s+review\s+.*--request-changes", text):
        return False
    if re.search(r"gh\s+pr\s+review\s+.*--approve", text):
        return True

    logger.warning(
        "No VERDICT marker or gh pr review command found; "
        "falling back to keyword heuristics"
    )

    # 3. Keyword heuristics on the tail to avoid false positives from
    #    quoted content earlier in the output.
    tail = text[-500:]
    lower = tail.lower()
    # gh --approve/--request-changes without the full command prefix
    if "--approve" in lower:
        return True
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


def _make_stage_log_path(
    log_dir: Path | None,
    stage: str,
    iteration: int = 1,
) -> Path | None:
    """Build a timestamped log file path for a pipeline stage.

    Returns ``None`` when *log_dir* is not configured, which disables
    per-stage log file writing.
    """
    if log_dir is None:
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    suffix = f"-iter{iteration}" if iteration > 1 else ""
    return log_dir / f"{stage}{suffix}-{ts}.log"


def _write_subprocess_log(
    log_file: Path,
    stdout: str | None,
    stderr: str | None,
) -> None:
    """Write captured subprocess stdout/stderr to *log_file*."""
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("w") as fh:
            if stdout:
                fh.write(stdout)
            if stderr:
                fh.write("\n--- STDERR ---\n")
                fh.write(stderr)
        logger.info("Wrote subprocess log to %s", log_file)
    except OSError:
        logger.warning("Failed to write subprocess log to %s", log_file, exc_info=True)


def _check_pr_merged(pr_url: str, cwd: str | Path) -> bool:
    """Check whether a PR has been merged by querying its state.

    Returns ``True`` if the PR state is ``MERGED``, ``False`` otherwise.
    """
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", pr_url, "--json", "state", "--jq", ".state"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            timeout=15,
        )
        if proc.returncode == 0:
            return proc.stdout.strip().upper() == "MERGED"
    except Exception:
        logger.debug("Failed to check PR state for %s", pr_url, exc_info=True)
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


