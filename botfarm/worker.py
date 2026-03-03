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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from botfarm.codex import CodexResult, run_codex_streaming
from botfarm.config import IdentitiesConfig
from botfarm.db import delete_stage_run, insert_event, insert_stage_run, is_extra_usage_active, update_stage_run_context_fill, update_task
from botfarm.process import terminate_process_group as _terminate_process_group
from botfarm.slots import update_slot_stage
from botfarm.workflow import (
    PipelineTemplate,
    StageLoop,
    StageTemplate,
    get_loop_for_stage,
    get_stage,
    load_pipeline,
    render_prompt,
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

# Truncation limits (characters) for error messages and subprocess output
RESULT_TRUNCATE_CHARS = 200
DETAIL_TRUNCATE_CHARS = 500
CI_OUTPUT_TRUNCATE_CHARS = 2000

# PR checks timeout (seconds)
DEFAULT_PR_CHECKS_TIMEOUT = 600

# Default context window size for per-turn fill calculation when the final
# result hasn't been received yet.  200k is the current window for Claude
# Opus/Sonnet models.
DEFAULT_CONTEXT_WINDOW = 200_000

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
_CODER_STAGES = frozenset({"implement", "fix", "ci_fix", "pr_checks", "merge"})
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


def _parse_ndjson_stream(
    proc_stdout,
    *,
    on_context_fill: ContextFillCallback | None = None,
    log_fh=None,
) -> tuple[list[str], ClaudeResult | None]:
    """Read NDJSON lines from Claude's stdout stream.

    Parses each line as JSON, tracks assistant turns for context fill
    callbacks, and returns when a ``result`` message is received.

    Returns ``(stdout_lines, claude_result)``.  *claude_result* is
    ``None`` if no result message was received before the stream ended.
    """
    stdout_lines: list[str] = []
    turn_number = 0
    claude_result: ClaudeResult | None = None

    for line in proc_stdout:
        stdout_lines.append(line)

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
                fill_pct = _compute_turn_context_fill(usage, DEFAULT_CONTEXT_WINDOW)
                if fill_pct is not None:
                    try:
                        on_context_fill(turn_number, fill_pct)
                    except Exception:
                        logger.debug(
                            "on_context_fill callback failed", exc_info=True,
                        )

        elif msg_type == "result":
            claude_result = parse_stream_json_result(event)
            break

    return stdout_lines, claude_result


def _finalize_streaming_result(
    proc: subprocess.Popen,
    cmd: list[str],
    *,
    stdout_lines: list[str],
    stderr_lines: list[str],
    claude_result: ClaudeResult | None,
    timed_out: threading.Event,
    log_fh,
    timeout: float | None,
) -> ClaudeResult:
    """Finalize a streaming Claude process: write logs, check errors, return result.

    Appends stderr to the log file (if open), checks for timeout and
    non-zero exit codes, and returns the parsed ``ClaudeResult``.

    Raises ``TimeoutError``, ``subprocess.CalledProcessError``, or
    ``RuntimeError`` when the process did not produce a valid result.
    """
    stdout_text = "".join(stdout_lines)
    stderr_text = "".join(stderr_lines)

    if log_fh is not None:
        try:
            if stderr_text:
                log_fh.write("\n--- STDERR ---\n")
                log_fh.write(stderr_text)
        except OSError:
            pass
        finally:
            log_fh.close()

    if timed_out.is_set() and proc.returncode in (-15, -9, 137, 143, None):
        raise TimeoutError(
            f"claude streaming process timed out after {timeout}s"
        )

    if proc.returncode != 0 and claude_result is None:
        logger.error(
            "claude (streaming) exited with code %d\nstderr: %s",
            proc.returncode,
            stderr_text[:DETAIL_TRUNCATE_CHARS] if stderr_text else "(empty)",
        )
        raise subprocess.CalledProcessError(
            proc.returncode, cmd, output=stdout_text, stderr=stderr_text,
        )

    if proc.returncode != 0 and claude_result is not None:
        logger.warning(
            "claude (streaming) exited with code %d after result was parsed "
            "(likely killed during MCP server cleanup)",
            proc.returncode,
        )

    if claude_result is None:
        raise RuntimeError(
            "claude stream-json produced no result message; "
            f"stdout length={len(stdout_text)}"
        )

    return claude_result


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
    identical to what ``parse_claude_output()`` returns.

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
        start_new_session=True,
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
    cancel_watchdog = threading.Event()
    timed_out = threading.Event()

    def _watchdog():
        if not cancel_watchdog.wait(timeout):
            timed_out.set()
            _terminate_process_group(proc)

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

    # Parse NDJSON stream with context fill tracking
    try:
        stdout_lines, claude_result = _parse_ndjson_stream(
            proc.stdout, on_context_fill=on_context_fill, log_fh=log_fh,
        )
    finally:
        cancel_watchdog.set()
        if watchdog_thread is not None:
            watchdog_thread.join(timeout=2)

    # Terminate the process group (Claude + MCP server children) if still alive
    _terminate_process_group(proc)
    proc.wait()
    stderr_thread.join(timeout=5)

    return _finalize_streaming_result(
        proc, cmd,
        stdout_lines=stdout_lines,
        stderr_lines=stderr_lines,
        claude_result=claude_result,
        timed_out=timed_out,
        log_fh=log_fh,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Stage result
# ---------------------------------------------------------------------------


@dataclass
class StageResult:
    """Outcome of a single pipeline stage."""

    stage: str
    success: bool
    claude_result: ClaudeResult | None = None
    codex_result: CodexResult | None = None
    pr_url: str | None = None
    error: str | None = None
    review_approved: bool | None = None
    claude_review_approved: bool | None = None


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
    """Run Claude via the streaming runner.

    Always uses ``run_claude_streaming()`` which writes logs incrementally
    and supports watchdog-based timeouts.  The *on_context_fill* callback
    is optional — when ``None``, streaming still works but per-turn
    context fill updates are simply skipped.
    """
    return run_claude_streaming(
        prompt, cwd=cwd, max_turns=max_turns,
        log_file=log_file, env=env, on_context_fill=on_context_fill,
        timeout=timeout,
    )


# ---------------------------------------------------------------------------
# Individual stage implementations
# ---------------------------------------------------------------------------


_INVESTIGATION_LABEL = "investigation"


def _is_investigation(labels: list[str] | None) -> bool:
    """Return True if the ticket has an Investigation label (case-insensitive)."""
    if not labels:
        return False
    return any(lbl.lower() == _INVESTIGATION_LABEL for lbl in labels)


def _build_implement_prompt(ticket_id: str, labels: list[str] | None) -> str:
    """Build the implement-stage prompt, varying by ticket labels.

    .. deprecated::
        Kept for backward compatibility.  New code should use
        ``render_prompt(stage_template, ticket_id=...)`` instead.
    """
    if _is_investigation(labels):
        return (
            f"Work on Linear ticket {ticket_id}. "
            "This is an investigation ticket. Produce a summary of findings "
            "as a Linear comment on the ticket. If you identify implementation "
            "work, create follow-up Linear tickets. Do not create a PR.\n\n"
            "When creating multiple follow-up tickets where one depends on another, "
            "set blockedBy / blocks relationships between them using the Linear MCP "
            "save_issue tool. This ensures the supervisor dispatches them in the correct "
            "order. Do not just mention dependencies in the description \u2014 set them as "
            "actual Linear relations."
        )
    return (
        f"Work on Linear ticket {ticket_id}. "
        "Follow the Linear Tickets workflow in CLAUDE.md. "
        "Complete all steps through PR creation. Do not stop until the PR is created. "
        "If the work described in the ticket is already fully implemented on main "
        "(e.g. delivered by another PR), verify all acceptance criteria are met, "
        "then output NO_PR_NEEDED: <explanation> as your final message. "
        "Do not create a branch or PR in that case."
    )


def _run_claude_stage(
    stage_tpl: StageTemplate,
    *,
    cwd: str | Path,
    max_turns: int,
    prompt_vars: dict[str, str],
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
) -> StageResult:
    """Generic runner for any claude-executor stage using its DB template."""
    prompt = render_prompt(stage_tpl, **prompt_vars)
    result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )

    if result.is_error:
        return StageResult(
            stage=stage_tpl.name,
            success=False,
            claude_result=result,
            error=f"Claude reported error: {result.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    # Apply result_parser
    pr_url: str | None = None
    review_approved: bool | None = None
    if stage_tpl.result_parser == "pr_url":
        pr_url = _extract_pr_url(result.result_text)
    elif stage_tpl.result_parser == "review_verdict":
        review_approved = _parse_review_approved(result.result_text)

    return StageResult(
        stage=stage_tpl.name,
        success=True,
        claude_result=result,
        pr_url=pr_url,
        review_approved=review_approved,
    )


def _run_implement(
    ticket_id: str,
    *,
    ticket_labels: list[str] | None = None,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    stage_tpl: StageTemplate | None = None,
) -> StageResult:
    """IMPLEMENT stage — Claude Code implements the ticket and creates a PR."""
    if stage_tpl is not None:
        return _run_claude_stage(
            stage_tpl, cwd=cwd, max_turns=max_turns,
            prompt_vars={"ticket_id": ticket_id},
            log_file=log_file, env=env, on_context_fill=on_context_fill,
        )
    # Legacy fallback
    prompt = _build_implement_prompt(ticket_id, ticket_labels)
    result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )

    if result.is_error:
        return StageResult(
            stage="implement",
            success=False,
            claude_result=result,
            error=f"Claude reported error: {result.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    pr_url = _extract_pr_url(result.result_text)

    return StageResult(
        stage="implement",
        success=True,
        claude_result=result,
        pr_url=pr_url,
    )


def _review_inline_comment_instructions(
    owner: str,
    repo: str,
    number: str,
    *,
    body_example: str = "comment",
) -> str:
    """Return the shared inline-comment API instructions used by both reviewers."""
    return (
        "For file-specific feedback, post inline review comments on the exact "
        "lines where changes are needed. First get the head SHA with:\n"
        f"  gh pr view {number} --json headRefOid --jq .headRefOid\n"
        "Then post each inline comment using:\n"
        f"  gh api repos/{owner}/{repo}/pulls/{number}/comments "
        f"-f body='{body_example}' -f commit_id='HEAD_SHA' -f path='file.py' "
        "-F line=42 -f side='RIGHT'\n\n"
    )


def _review_verdict_instructions(*, submit_review: bool) -> str:
    """Return the shared verdict marker instructions used by both reviewers."""
    return (
        "IMPORTANT: If you posted ANY inline comments with suggestions, issues, "
        "or actionable feedback, you MUST "
        + ("output " if not submit_review else "use --request-changes and output ")
        + "VERDICT: CHANGES_REQUESTED. Only "
        + ("output " if not submit_review else "use --approve and output ")
        + "VERDICT: APPROVED "
        "when there are ZERO actionable inline comments.\n\n"
        "At the very end of your response, output exactly one of these verdict "
        "markers on its own line:\n"
        "  VERDICT: APPROVED\n"
        "  VERDICT: CHANGES_REQUESTED"
    )


def _build_claude_review_prompt(
    pr_url: str,
    owner: str,
    repo: str,
    number: str,
    *,
    codex_enabled: bool = False,
) -> str:
    """Build the Claude review prompt, adjusting for multi-review mode."""
    prompt = (
        f"Review the pull request at {pr_url}. "
        "Read the PR diff carefully. Be thorough but constructive.\n\n"
    )
    prompt += _review_inline_comment_instructions(owner, repo, number)
    if codex_enabled:
        # Multi-review mode: Claude must NOT submit gh pr review
        prompt += (
            "Do NOT submit a final review via 'gh pr review'. Only post inline "
            "comments and output your verdict marker.\n\n"
        )
    else:
        # Single-reviewer mode: Claude submits gh pr review directly
        prompt += (
            "After posting all inline comments, submit your overall assessment "
            "using 'gh pr review' with either --approve or --request-changes "
            "and a summary body.\n\n"
        )
    prompt += _review_verdict_instructions(submit_review=not codex_enabled)
    return prompt


def _build_codex_review_prompt(
    pr_url: str,
    owner: str,
    repo: str,
    number: str,
) -> str:
    """Build the Codex review prompt."""
    prompt = (
        f"Review the pull request at {pr_url}. "
        "Read the PR diff carefully. Be thorough but constructive.\n\n"
        "IMPORTANT: First fetch existing review comments to avoid posting duplicate feedback:\n"
        f"  gh api repos/{owner}/{repo}/pulls/{number}/comments\n\n"
    )
    prompt += _review_inline_comment_instructions(
        owner, repo, number, body_example="CODEX: <your comment>",
    )
    prompt += (
        "ALL your inline comments MUST start with the prefix 'CODEX: ' in the body.\n\n"
        "Do NOT submit a final review via 'gh pr review'. Only post inline comments "
        "and output your verdict marker.\n\n"
    )
    prompt += _review_verdict_instructions(submit_review=False)
    return prompt


def _merge_review_verdicts(
    claude_approved: bool,
    codex_approved: bool | None,
) -> bool:
    """Merge Claude and Codex verdicts.

    Either reviewer blocking results in CHANGES_REQUESTED.
    If Codex is ``None`` (error/timeout/disabled), Claude's verdict stands.
    """
    if codex_approved is None:
        return claude_approved
    return claude_approved and codex_approved


def _submit_aggregate_review(
    pr_url: str,
    *,
    cwd: str | Path,
    claude_verdict: str,
    codex_verdict: str,
    approved: bool,
    iteration: int = 1,
    env: dict[str, str] | None = None,
) -> bool:
    """Submit a single ``gh pr review`` with the aggregate verdict.

    This is used in multi-review mode where neither Claude nor Codex
    submits their own ``gh pr review``.

    Returns ``True`` if the review was posted successfully, ``False``
    otherwise (the error is logged as a warning).
    """
    owner, repo, number = _parse_pr_url(pr_url)
    action = "--approve" if approved else "--request-changes"
    overall = "approved" if approved else "changes_requested"
    body = (
        f"Review iteration {iteration}\n"
        f"Claude: {claude_verdict}\n"
        f"Codex: {codex_verdict}\n"
        f"Overall: {overall}"
    )

    subprocess_env = None
    if env:
        subprocess_env = {**os.environ, **env}

    try:
        subprocess.run(
            ["gh", "pr", "review", number, action, "--body", body,
             "--repo", f"{owner}/{repo}"],
            cwd=str(cwd),
            env=subprocess_env,
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        logger.warning("Failed to submit aggregate review: %s", exc)
        return False


def _run_review(
    pr_url: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    stage_tpl: StageTemplate | None = None,
    codex_enabled: bool = False,
    codex_log_file: Path | None = None,
    codex_timeout: float | None = None,
    codex_model: str | None = None,
    review_iteration: int = 1,
) -> StageResult:
    """REVIEW stage — Claude Code reviews the PR and posts comments.

    When *codex_enabled* is ``True``, runs a sequential dual-reviewer
    flow: Claude first, then Codex.  Neither reviewer submits
    ``gh pr review`` — Botfarm aggregates verdicts and submits one
    final review.
    """
    owner, repo, number = _parse_pr_url(pr_url)

    # --- DB-driven template path ---
    if stage_tpl is not None:
        # When codex is enabled, append multi-reviewer instructions to the
        # rendered template prompt so Claude does NOT submit gh pr review.
        prompt_vars = {
            "pr_url": pr_url,
            "pr_number": number,
            "owner": owner,
            "repo": repo,
        }
        if codex_enabled:
            # Run Claude via the DB template with the multi-reviewer note
            prompt = render_prompt(stage_tpl, **prompt_vars)
            prompt += (
                "\n\nDo NOT submit a final review via 'gh pr review'. Only post inline "
                "comments and output your verdict marker."
            )
            claude_result = _invoke_claude(
                prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
                env=env, on_context_fill=on_context_fill,
            )
            if claude_result.is_error:
                return StageResult(
                    stage=stage_tpl.name,
                    success=False,
                    claude_result=claude_result,
                    error=f"Claude reported error: {claude_result.result_text[:RESULT_TRUNCATE_CHARS]}",
                )
            claude_approved = _parse_review_approved(claude_result.result_text)
            # Continue to Codex section below
        else:
            # Single-reviewer mode with DB template — unchanged behaviour
            return _run_claude_stage(
                stage_tpl, cwd=cwd, max_turns=max_turns,
                prompt_vars=prompt_vars,
                log_file=log_file, env=env, on_context_fill=on_context_fill,
            )
    else:
        # --- Legacy fallback ---
        prompt = _build_claude_review_prompt(
            pr_url, owner, repo, number, codex_enabled=codex_enabled,
        )
        claude_result = _invoke_claude(
            prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
        )

        if claude_result.is_error:
            return StageResult(
                stage="review",
                success=False,
                claude_result=claude_result,
                error=f"Claude reported error: {claude_result.result_text[:RESULT_TRUNCATE_CHARS]}",
            )

        claude_approved = _parse_review_approved(claude_result.result_text)

    # --- Single-reviewer mode: return immediately ---
    if not codex_enabled:
        return StageResult(
            stage="review",
            success=True,
            claude_result=claude_result,
            review_approved=claude_approved,
            claude_review_approved=claude_approved,
        )

    # --- Multi-review mode: run Codex sequentially ---
    codex_result_obj: CodexResult | None = None
    codex_approved: bool | None = None
    codex_verdict_str = "skipped"

    try:
        codex_stage_result = _run_codex_review(
            pr_url,
            cwd=cwd,
            log_file=codex_log_file,
            env=env,
            timeout=codex_timeout,
            codex_model=codex_model,
        )
        codex_result_obj = codex_stage_result.codex_result

        if codex_stage_result.success:
            codex_approved = codex_stage_result.review_approved
            codex_verdict_str = "approved" if codex_approved else "changes_requested"
        else:
            # Codex errored — fail-open, use Claude-only verdict
            logger.warning(
                "Codex review failed (fail-open): %s",
                codex_stage_result.error or "unknown error",
            )
            codex_verdict_str = "error"
    except Exception:
        logger.warning("Codex review raised exception (fail-open)", exc_info=True)
        codex_verdict_str = "error"

    # Merge verdicts
    claude_verdict_str = "approved" if claude_approved else "changes_requested"
    merged_approved = _merge_review_verdicts(claude_approved, codex_approved)

    # Submit ONE aggregate gh pr review (failure is logged internally;
    # pipeline proceeds regardless since the merged verdict drives the
    # review-fix loop).
    _submit_aggregate_review(
        pr_url,
        cwd=cwd,
        claude_verdict=claude_verdict_str,
        codex_verdict=codex_verdict_str,
        approved=merged_approved,
        iteration=review_iteration,
        env=env,
    )

    return StageResult(
        stage="review",
        success=True,
        claude_result=claude_result,
        codex_result=codex_result_obj,
        review_approved=merged_approved,
        claude_review_approved=claude_approved,
    )


def _run_codex_review(
    pr_url: str,
    *,
    cwd: str | Path,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
    codex_model: str | None = None,
) -> StageResult:
    """CODEX_REVIEW stage — Codex reviews the PR and posts inline comments.

    Uses ``run_codex_streaming()`` from ``botfarm/codex.py`` for subprocess
    management.  Codex does NOT submit ``gh pr review`` — Botfarm handles
    the final review submission in multi-review mode.
    """
    owner, repo, number = _parse_pr_url(pr_url)

    prompt = _build_codex_review_prompt(pr_url, owner, repo, number)

    result = run_codex_streaming(
        prompt,
        cwd=cwd,
        model=codex_model,
        log_file=log_file,
        env=env,
        timeout=timeout,
    )

    if result.is_error:
        return StageResult(
            stage="codex_review",
            success=False,
            codex_result=result,
            error=f"Codex reported error: {result.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    approved = _parse_review_approved(result.result_text)

    return StageResult(
        stage="codex_review",
        success=True,
        codex_result=result,
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
    stage_tpl: StageTemplate | None = None,
    codex_enabled: bool = False,
) -> StageResult:
    """FIX stage — Fresh Claude Code addresses review comments and pushes fixes."""
    owner, repo, number = _parse_pr_url(pr_url)
    if stage_tpl is not None:
        return _run_claude_stage(
            stage_tpl, cwd=cwd, max_turns=max_turns,
            prompt_vars={
                "pr_url": pr_url,
                "pr_number": number,
                "owner": owner,
                "repo": repo,
            },
            log_file=log_file, env=env, on_context_fill=on_context_fill,
        )
    # Legacy fallback
    prompt = (
        f"Address the review comments on PR {pr_url}. "
        "Read both the top-level review comment and any inline review comments "
        f"on specific files/lines. Use 'gh api repos/{owner}/{repo}/pulls/{number}/comments' "
        "to list inline comments. Note each comment's `id` field from the API response.\n\n"
        "Make the necessary code changes for each comment, run tests, "
        "commit and push the fixes.\n\n"
    )
    if codex_enabled:
        prompt += (
            "Note: Comments may come from multiple reviewers. Comments prefixed with "
            "\"CODEX: \" are from the Codex reviewer. Address all comments regardless "
            "of source.\n\n"
        )
    prompt += (
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
            error=f"Claude reported error: {result.result_text[:RESULT_TRUNCATE_CHARS]}",
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
    timeout: int = DEFAULT_PR_CHECKS_TIMEOUT,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
) -> StageResult:
    """PR_CHECKS stage — Wait for CI checks to pass. No Claude Code invocation."""
    subprocess_env = {**os.environ, **env} if env else None
    start = time.monotonic()
    try:
        proc = subprocess.run(
            ["gh", "pr", "checks", pr_url, "--watch"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            timeout=timeout,
            env=subprocess_env,
        )
        if log_file is not None:
            _write_subprocess_log(log_file, proc.stdout, proc.stderr)
        success = proc.returncode == 0
        if success:
            logger.info("CI checks passed in %.1fs", time.monotonic() - start)
        return StageResult(
            stage="pr_checks",
            success=success,
            error=None if success else f"CI checks failed:\n{proc.stdout[:CI_OUTPUT_TRUNCATE_CHARS]}",
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
    stage_tpl: StageTemplate | None = None,
) -> StageResult:
    """CI FIX stage — Claude Code fixes CI failures using CI output context."""
    if stage_tpl is not None:
        return _run_claude_stage(
            stage_tpl, cwd=cwd, max_turns=max_turns,
            prompt_vars={
                "pr_url": pr_url,
                "ci_failure_output": ci_failure_output[:CI_OUTPUT_TRUNCATE_CHARS],
            },
            log_file=log_file, env=env, on_context_fill=on_context_fill,
        )
    # Legacy fallback
    prompt = (
        f"The CI checks on PR {pr_url} have failed. "
        "Diagnose and fix the CI failures based on the output below, "
        "then run tests locally, commit and push the fixes.\n\n"
        f"CI failure output:\n{ci_failure_output[:CI_OUTPUT_TRUNCATE_CHARS]}"
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
            error=f"Claude reported error: {result.result_text[:RESULT_TRUNCATE_CHARS]}",
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
    env: dict[str, str] | None = None,
) -> StageResult:
    """MERGE stage — Squash merge the PR, then checkout placeholder branch."""
    subprocess_env = {**os.environ, **env} if env else None

    # Capture the current branch name before merge/checkout so we can
    # delete the actual feature branch afterwards (not a stale name
    # threaded through the pipeline).
    feature_branch: str | None = None
    rev_parse = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        capture_output=True,
        text=True,
        cwd=str(cwd),
        env=subprocess_env,
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
        env=subprocess_env,
    )
    if log_file is not None:
        _write_subprocess_log(log_file, proc.stdout, proc.stderr)
    if proc.returncode != 0:
        # The merge command failed, but the PR may have actually been merged
        # (e.g. post-merge git checkout fails in worktree environments).
        # Check the actual PR state before reporting failure.
        if _check_pr_merged(pr_url, cwd, env=subprocess_env):
            logger.info(
                "gh pr merge returned non-zero but PR is actually merged: %s",
                pr_url,
            )
        else:
            error = proc.stderr[:DETAIL_TRUNCATE_CHARS] if proc.stderr else proc.stdout[:DETAIL_TRUNCATE_CHARS]
            return StageResult(stage="merge", success=False, error=error)

    # Switch worktree back to its placeholder branch so it doesn't hold
    # the now-deleted feature branch (which causes issues on next dispatch).
    if placeholder_branch:
        checkout = subprocess.run(
            ["git", "checkout", placeholder_branch],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            env=subprocess_env,
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
                env=subprocess_env,
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
    no_pr_reason: str | None = None
    result_text: str | None = None


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
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,  # TODO: wire through to run_claude_streaming --resume
    slot_db_path: str | Path | None = None,
    pause_event: multiprocessing.Event | None = None,
    identities: IdentitiesConfig | None = None,
    codex_reviewer_enabled: bool = False,
    codex_reviewer_model: str = "",
    codex_reviewer_timeout_minutes: int = 15,
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

    # Load pipeline template and derive configuration
    (stages, turns_cfg, eff_max_review_iterations, eff_max_ci_retries,
     loop_managed_stages, pipeline_tpl) = _load_pipeline_config(
        conn, ticket_id, ticket_labels or [], max_turns,
        max_review_iterations, max_ci_retries,
    )

    # Validate resume_from_stage upfront — check against all pipeline stages
    # (including loop-managed ones), not just main stages.
    if resume_from_stage and resume_from_stage not in stages:
        pipeline.failure_stage = resume_from_stage
        pipeline.failure_reason = f"Unknown resume stage: {resume_from_stage}"
        _record_failure(conn, task_id, pipeline)
        return pipeline

    # When resuming, recover PR URL from existing stage_runs
    if resume_from_stage:
        pr_url = _recover_pr_url(conn, task_id, cwd, env=coder_env)
        if pr_url:
            pipeline.pr_url = pr_url

    # Determine which stages to skip when resuming
    skipping = resume_from_stage is not None

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
        codex_config=_CodexReviewerConfig(
            enabled=codex_reviewer_enabled,
            model=codex_reviewer_model,
            timeout_minutes=codex_reviewer_timeout_minutes,
        ),
    )

    # Build main-stage list: skip loop-managed stages (they're handled
    # inside their respective loops), unless we're resuming from one.
    resume_include = {resume_from_stage} if resume_from_stage and resume_from_stage in loop_managed_stages else set()
    main_stages = [s for s in stages if s not in loop_managed_stages or s in resume_include]

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

        should_stop, pr_url = _dispatch_pipeline_stage(
            ctx, stage,
            pr_url=pr_url,
            eff_max_review_iterations=eff_max_review_iterations,
            eff_max_ci_retries=eff_max_ci_retries,
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
) -> tuple[
    tuple[str, ...],
    dict[str, int],
    int,
    int,
    set[str],
    PipelineTemplate | None,
]:
    """Load pipeline template from DB and derive all configuration.

    Returns ``(stages, turns_cfg, eff_max_review_iterations,
    eff_max_ci_retries, loop_managed_stages, pipeline_tpl)``.
    """
    pipeline_tpl: PipelineTemplate | None = None
    try:
        pipeline_tpl = load_pipeline(conn, ticket_labels)
        logger.info(
            "Loaded pipeline '%s' for %s (stages: %s)",
            pipeline_tpl.name, ticket_id,
            ", ".join(s.name for s in pipeline_tpl.stages),
        )
    except Exception:
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

    # Determine loop-managed stages: skipped in main iteration because
    # they're handled inside their respective loops.
    loop_managed_stages: set[str] = set()
    if pipeline_tpl is not None:
        for loop in pipeline_tpl.loops:
            if loop.on_failure_stage:
                loop_managed_stages.add(loop.start_stage)
            else:
                loop_managed_stages.add(loop.end_stage)

    return stages, turns_cfg, eff_max_review_iterations, eff_max_ci_retries, loop_managed_stages, pipeline_tpl


def _dispatch_pipeline_stage(
    ctx: "_PipelineContext",
    stage: str,
    *,
    pr_url: str | None,
    eff_max_review_iterations: int,
    eff_max_ci_retries: int,
) -> tuple[bool, str | None]:
    """Dispatch a single stage in the pipeline main loop.

    Returns ``(should_stop, updated_pr_url)``.  When *should_stop* is
    ``True``, the pipeline should return immediately (failure or early
    success already recorded in ``ctx.pipeline``).
    """
    # ----- Review iteration loop -----
    if stage == "review":
        success = _run_review_fix_loop(
            ctx, pr_url=pr_url, max_iterations=eff_max_review_iterations,
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
            ctx, pr_url=pr_url, max_iterations=eff_max_review_iterations,
        )
        return should_stop, pr_url

    # ----- CI retry loop -----
    if stage == "pr_checks":
        return _handle_pr_checks_stage(
            ctx, pr_url=pr_url, max_ci_retries=eff_max_ci_retries,
        )

    # ----- Normal stages (implement, merge, etc.) -----
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

    if max_ci_retries > 0:
        ci_failure_output = ctx.pipeline.failure_reason or "CI checks failed"
        _reset_for_retry(ctx)
        success = _run_ci_retry_loop(
            ctx,
            pr_url=pr_url,
            ci_failure_output=ci_failure_output,
            max_retries=max_ci_retries,
        )
        if not success:
            return True, pr_url
        if "pr_checks" not in ctx.pipeline.stages_completed:
            ctx.pipeline.stages_completed.append("pr_checks")
        return False, pr_url

    # No retries configured — fail
    return True, pr_url


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
        update_task(
            ctx.conn,
            ctx.task_id,
            status="completed",
            turns=ctx.pipeline.total_turns,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        ctx.conn.commit()
        return True

    # No PR URL: check for explicit no-PR signal
    if not pr_url:
        no_pr_reason = _detect_no_pr_needed(
            result.claude_result.result_text if result.claude_result else ""
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
    timeout_minutes: int = 15


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
    codex_config: _CodexReviewerConfig = field(default_factory=_CodexReviewerConfig)

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

        codex = result.codex_result
        if codex is None:
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

        if codex.is_error:
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="codex_review_failed",
                detail=codex.result_text[:RESULT_TRUNCATE_CHARS] if codex.result_text else "unknown error",
            )
        else:
            codex_approved = _parse_review_approved(codex.result_text)
            verdict_str = "approved" if codex_approved else "changes_requested"
            insert_event(
                self.conn,
                task_id=self.task_id,
                event_type="codex_review_completed",
                detail=f"verdict={verdict_str}",
            )
        self.conn.commit()

        # Record separate codex_review stage_runs row
        insert_stage_run(
            self.conn,
            task_id=self.task_id,
            stage="codex_review",
            iteration=iteration,
            session_id=codex.thread_id,
            turns=codex.num_turns,
            duration_seconds=codex.duration_seconds,
            input_tokens=codex.input_tokens,
            output_tokens=codex.output_tokens,
            cache_read_input_tokens=codex.cached_input_tokens,
            cache_creation_input_tokens=0,
            total_cost_usd=0.0,
            log_file_path=str(codex_log_file) if codex_log_file else None,
            on_extra_usage=on_extra_usage,
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

        # For Claude-based stages, insert a placeholder stage_run row
        # so per-turn context fill updates have a target row to update.
        on_context_fill: ContextFillCallback | None = None
        stage_run_id: int | None = None
        stage_tpl = self._get_stage_tpl(stage)
        uses_claude = (
            stage_tpl.executor_type == "claude"
            if stage_tpl is not None
            else stage in ("implement", "review", "fix")
        )

        extra_usage = is_extra_usage_active(self.conn)

        if uses_claude:
            stage_run_id = insert_stage_run(
                self.conn,
                task_id=self.task_id,
                stage=stage,
                iteration=iteration,
                log_file_path=str(log_file) if log_file else None,
                on_extra_usage=extra_usage,
            )
            self.conn.commit()

            _conn = self.conn
            _sr_id = stage_run_id

            def on_context_fill(turn: int, fill_pct: float) -> None:
                update_stage_run_context_fill(_conn, _sr_id, fill_pct)

        # Codex reviewer params (only relevant for review stage)
        codex_kwargs: dict = {}
        if stage == "review" and self.codex_config.enabled:
            codex_kwargs = {
                "codex_enabled": True,
                "codex_model": self.codex_config.model or None,
                "codex_timeout": self.codex_config.timeout_minutes * 60.0,
                "codex_log_file": _make_stage_log_path(
                    self.log_dir, "codex_review", iteration,
                ),
                "review_iteration": iteration,
            }

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

        # Record Codex review as a separate stage run + events
        if codex_kwargs:
            self._record_codex_review(
                result,
                iteration=iteration,
                codex_log_file=codex_kwargs.get("codex_log_file"),
                on_extra_usage=extra_usage,
            )

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
        )

        if result.claude_result:
            self.pipeline.total_turns += result.claude_result.num_turns
            self.pipeline.total_duration_seconds += result.claude_result.duration_seconds
            # Track the last stage's result text for terminal comment posting.
            if result.claude_result.result_text:
                self.pipeline.result_text = result.claude_result.result_text

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
    ticket_labels: list[str] | None = None,
    pr_url: str | None,
    cwd: str | Path,
    max_turns: int,
    pr_checks_timeout: int,
    log_file: Path | None = None,
    placeholder_branch: str | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    stage_tpl: StageTemplate | None = None,
    codex_enabled: bool = False,
    codex_model: str | None = None,
    codex_timeout: float | None = None,
    codex_log_file: Path | None = None,
    review_iteration: int = 1,
) -> StageResult:
    """Dispatch to the appropriate stage runner.

    When *stage_tpl* is provided, routing uses ``executor_type`` from the
    DB template.  Otherwise falls back to name-based dispatch for backward
    compatibility.
    """
    # --- DB-driven routing via executor_type ---
    if stage_tpl is not None:
        if stage_tpl.executor_type == "claude":
            # Review stage routes through _run_review() so it can
            # integrate the Codex dual-reviewer flow when enabled.
            if stage == "review":
                if not pr_url:
                    raise ValueError(f"{stage} stage requires pr_url")
                return _run_review(
                    pr_url, cwd=cwd, max_turns=max_turns, log_file=log_file,
                    env=env, on_context_fill=on_context_fill,
                    stage_tpl=stage_tpl,
                    codex_enabled=codex_enabled,
                    codex_log_file=codex_log_file,
                    codex_timeout=codex_timeout,
                    codex_model=codex_model,
                    review_iteration=review_iteration,
                )
            # Other claude stages use the generic runner
            prompt_vars: dict[str, str] = {}
            if stage == "implement":
                prompt_vars["ticket_id"] = ticket_id
            elif pr_url:
                owner, repo, number = _parse_pr_url(pr_url)
                prompt_vars.update(
                    pr_url=pr_url,
                    pr_number=number,
                    owner=owner,
                    repo=repo,
                )
            return _run_claude_stage(
                stage_tpl, cwd=cwd, max_turns=max_turns,
                prompt_vars=prompt_vars,
                log_file=log_file, env=env, on_context_fill=on_context_fill,
            )
        elif stage_tpl.executor_type == "shell":
            if not pr_url:
                raise ValueError(f"{stage} stage requires pr_url")
            return _run_pr_checks(pr_url, cwd=cwd, timeout=pr_checks_timeout, log_file=log_file, env=env)
        elif stage_tpl.executor_type == "internal":
            if not pr_url:
                raise ValueError(f"{stage} stage requires pr_url")
            return _run_merge(
                pr_url, cwd=cwd, log_file=log_file,
                placeholder_branch=placeholder_branch, env=env,
            )
        else:
            raise ValueError(f"Unknown executor_type: {stage_tpl.executor_type!r} for stage {stage!r}")

    # --- Legacy name-based routing ---
    if stage == "implement":
        return _run_implement(
            ticket_id, ticket_labels=ticket_labels,
            cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
        )
    elif stage == "review":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_review(
            pr_url, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
            codex_enabled=codex_enabled,
            codex_log_file=codex_log_file,
            codex_timeout=codex_timeout,
            codex_model=codex_model,
            review_iteration=review_iteration,
        )
    elif stage == "fix":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_fix(
            pr_url, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
            codex_enabled=codex_enabled,
        )
    elif stage == "pr_checks":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_pr_checks(pr_url, cwd=cwd, timeout=pr_checks_timeout, log_file=log_file, env=env)
    elif stage == "merge":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_merge(
            pr_url, cwd=cwd, log_file=log_file,
            placeholder_branch=placeholder_branch, env=env,
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
    on_extra_usage: bool = False,
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
            on_extra_usage=on_extra_usage,
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


_NO_PR_NEEDED_RE = re.compile(r"NO_PR_NEEDED:\s*(.+)", re.IGNORECASE | re.DOTALL)


def _detect_no_pr_needed(text: str) -> str | None:
    """Check if the agent signalled that no PR is needed.

    Looks for ``NO_PR_NEEDED: <explanation>`` in *text* (case-insensitive).
    Returns the explanation string (stripped, capped at 500 chars) or ``None``.
    """
    if not text:
        return None
    match = _NO_PR_NEEDED_RE.search(text)
    if not match:
        return None
    return match.group(1).strip()[:DETAIL_TRUNCATE_CHARS] or None


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
    tail = text[-DETAIL_TRUNCATE_CHARS:]
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


def _check_pr_merged(pr_url: str, cwd: str | Path, *, env: dict[str, str] | None = None) -> bool:
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
            env=env,
        )
        if proc.returncode == 0:
            return proc.stdout.strip().upper() == "MERGED"
    except Exception:
        logger.debug("Failed to check PR state for %s", pr_url, exc_info=True)
    return False


def _recover_pr_url(
    conn, task_id: int, cwd: str | Path,
    *, env: dict[str, str] | None = None,
) -> str | None:
    """Recover the PR URL for a resumed pipeline.

    First checks ``gh pr view`` in the working directory, then falls back
    to scanning previous stage_run records in the database.
    """
    subprocess_env = {**os.environ, **env} if env else None
    # Try gh pr view to get current branch's PR URL
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", "--json", "url", "--jq", ".url"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            env=subprocess_env,
        )
        if proc.returncode == 0 and proc.stdout.strip():
            url = proc.stdout.strip()
            if "github.com" in url and "/pull/" in url:
                logger.info("Recovered PR URL from gh: %s", url)
                return url
    except Exception:
        pass

    return None


