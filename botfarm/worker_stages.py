"""Individual pipeline stage implementations.

Each ``_run_*`` function executes a single stage (implement, review,
fix, pr_checks, ci_fix, merge, resolve_conflict) and returns a
:class:`~botfarm.worker_claude.StageResult`.

Extracted from ``worker.py`` to keep the main pipeline module focused
on orchestration logic.
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import re
import subprocess
import time
from pathlib import Path

from botfarm.agent import AdapterRegistry, AgentAdapter, AgentResult
from botfarm.agent_claude import _claude_result_to_agent_result
from botfarm.worker_claude import (
    CI_OUTPUT_TRUNCATE_CHARS,
    DETAIL_TRUNCATE_CHARS,
    RESULT_TRUNCATE_CHARS,
    ContextFillCallback,
    StageResult,
    _check_pr_merged,
    _extract_pr_url,
    _extract_pr_url_from_log,
    _gh_pr_view_url,
    _invoke_claude,
    _parse_pr_url,
    _parse_review_approved,
    _write_subprocess_log,
)
from botfarm.workflow import StageTemplate, render_prompt

logger = logging.getLogger(__name__)

# PR checks timeout (seconds)
DEFAULT_PR_CHECKS_TIMEOUT = 600

_NO_PR_LABELS = {"investigation", "refactoring analysis"}


def _is_investigation(labels: list[str] | None) -> bool:
    """Return True if the ticket has a no-PR label (case-insensitive).

    Recognized labels: ``Investigation``, ``Refactoring Analysis``.
    """
    if not labels:
        return False
    return any(lbl.lower() in _NO_PR_LABELS for lbl in labels)


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
        "Follow the Linear Tickets workflow in AGENTS.md. "
        "Complete all steps through PR creation. Do not stop until the PR is created. "
        "If the work described in the ticket is already fully implemented on main "
        "(e.g. delivered by another PR), verify all acceptance criteria are met, "
        "then output NO_PR_NEEDED: <explanation> as your final message. "
        "Do not create a branch or PR in that case."
    )


def _run_agent_stage(
    stage_tpl: StageTemplate,
    adapter: AgentAdapter,
    *,
    cwd: str | Path,
    max_turns: int,
    prompt_vars: dict[str, str],
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: float | None = None,
    on_context_fill: ContextFillCallback | None = None,
) -> StageResult:
    """Generic runner for any agent-executor stage using its DB template.

    Dispatches to the given *adapter* (resolved from the adapter registry).
    """
    # Always inject worktree_path so prompt templates can anchor the agent
    # to the correct working directory (prevents git ops in the base repo).
    prompt_vars.setdefault("worktree_path", str(cwd))
    prompt = render_prompt(stage_tpl, **prompt_vars)
    result = adapter.run(
        prompt,
        cwd=Path(cwd),
        max_turns=max_turns if adapter.supports_max_turns else None,
        log_file=log_file,
        env=env,
        timeout=timeout,
        on_context_fill=on_context_fill if adapter.supports_context_fill else None,
    )

    if result.is_error:
        return StageResult(
            stage=stage_tpl.name,
            success=False,
            agent_result=result,
            error=f"{adapter.name} reported error: {result.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    # Apply result_parser
    pr_url: str | None = None
    review_approved: bool | None = None
    if stage_tpl.result_parser == "pr_url":
        pr_url = _extract_pr_url(result.result_text)
        if pr_url is None:
            logger.info("PR URL not found in result_text, trying gh pr view")
            pr_url = _gh_pr_view_url(cwd, env=env)
            if pr_url is None:
                logger.info("PR URL not found via gh pr view, scanning log file")
                pr_url = _extract_pr_url_from_log(log_file)
            if pr_url:
                logger.info("Recovered PR URL via fallback: %s", pr_url)
    elif stage_tpl.result_parser == "review_verdict":
        review_approved = _parse_review_approved(result.result_text)

    return StageResult(
        stage=stage_tpl.name,
        success=True,
        agent_result=result,
        pr_url=pr_url,
        review_approved=review_approved,
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
    """Backward-compat wrapper — runs a Claude stage via :func:`_invoke_claude`.

    Used when no adapter registry is available (legacy code paths, tests).
    """
    prompt_vars.setdefault("worktree_path", str(cwd))
    prompt = render_prompt(stage_tpl, **prompt_vars)
    result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )
    ar = _claude_result_to_agent_result(result)

    if ar.is_error:
        return StageResult(
            stage=stage_tpl.name,
            success=False,
            agent_result=ar,
            error=f"Claude reported error: {ar.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    pr_url: str | None = None
    review_approved: bool | None = None
    if stage_tpl.result_parser == "pr_url":
        pr_url = _extract_pr_url(ar.result_text)
        if pr_url is None:
            logger.info("PR URL not found in result_text, trying gh pr view")
            pr_url = _gh_pr_view_url(cwd, env=env)
            if pr_url is None:
                logger.info("PR URL not found via gh pr view, scanning log file")
                pr_url = _extract_pr_url_from_log(log_file)
            if pr_url:
                logger.info("Recovered PR URL via fallback: %s", pr_url)
    elif stage_tpl.result_parser == "review_verdict":
        review_approved = _parse_review_approved(ar.result_text)

    return StageResult(
        stage=stage_tpl.name,
        success=True,
        agent_result=ar,
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
    shared_mem_path: Path | None = None,
    prior_context: str = "",
) -> StageResult:
    """IMPLEMENT stage — Claude Code implements the ticket and creates a PR."""
    if stage_tpl is not None:
        prompt_vars: dict[str, str] = {"ticket_id": ticket_id}
        if shared_mem_path:
            prompt_vars["shared_mem_path"] = str(shared_mem_path)
        prompt_vars["prior_context"] = prior_context
        return _run_claude_stage(
            stage_tpl, cwd=cwd, max_turns=max_turns,
            prompt_vars=prompt_vars,
            log_file=log_file, env=env, on_context_fill=on_context_fill,
        )
    # Legacy fallback
    prompt = _build_implement_prompt(ticket_id, ticket_labels)
    if prior_context:
        prompt = prior_context + prompt
    if shared_mem_path:
        prompt += (
            f"\n\nAfter completing your implementation, write a brief summary to "
            f"{shared_mem_path}/implementer.md containing:\n"
            "- Files created or modified (with brief description of changes)\n"
            "- Key architectural decisions made\n"
            "- Areas of the codebase that are relevant to the change\n"
            "- Any gotchas or non-obvious implementation details"
        )
    claude_result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )
    ar = _claude_result_to_agent_result(claude_result)

    if ar.is_error:
        return StageResult(
            stage="implement",
            success=False,
            agent_result=ar,
            error=f"Claude reported error: {ar.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    pr_url = _extract_pr_url(ar.result_text)
    if pr_url is None:
        logger.info("PR URL not found in result_text, trying gh pr view")
        pr_url = _gh_pr_view_url(cwd, env=env)
        if pr_url is None:
            logger.info("PR URL not found via gh pr view, scanning log file")
            pr_url = _extract_pr_url_from_log(log_file)
        if pr_url:
            logger.info("Recovered PR URL via fallback: %s", pr_url)

    return StageResult(
        stage="implement",
        success=True,
        agent_result=ar,
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
        "REVIEW SCOPE AND SEVERITY GUIDELINES:\n"
        "Focus ONLY on issues that materially affect the correctness, safety, "
        "or reliability of the code:\n"
        "- Bugs, logic errors, or runtime failures\n"
        "- Security vulnerabilities\n"
        "- Data loss or corruption risks\n"
        "- Broken builds or test failures\n"
        "- Clear violations of project conventions stated in AGENTS.md\n\n"
        "Do NOT flag or request changes for:\n"
        "- Style preferences or cosmetic suggestions\n"
        "- Accessibility, performance, or UX improvements not required by the ticket\n"
        "- Theoretical edge cases unlikely to occur in practice\n"
        "- Missing features or enhancements beyond the ticket scope\n"
        "- Minor naming, formatting, or documentation nits\n\n"
        "Use CHANGES_REQUESTED only when a comment identifies a real defect that "
        "would break functionality or violate explicit project requirements. "
        "If all your comments are suggestions or nice-to-haves, use APPROVED.\n\n"
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
    prompt += (
        "VERDICT RULES:\n"
        "Output VERDICT: CHANGES_REQUESTED only if you identified a critical defect "
        "(bug, security vulnerability, data loss risk, broken build, or violation of "
        "explicit project requirements). If your comments are limited to suggestions, "
        "nice-to-haves, or non-critical observations, output VERDICT: APPROVED.\n\n"
        "At the very end of your response, output exactly one of these verdict "
        "markers on its own line:\n"
        "  VERDICT: APPROVED\n"
        "  VERDICT: CHANGES_REQUESTED"
    )
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
    registry: AdapterRegistry | None = None,
    codex_enabled: bool = False,
    codex_log_file: Path | None = None,
    codex_timeout: float | None = None,
    codex_model: str | None = None,
    codex_reasoning_effort: str | None = None,
    review_iteration: int = 1,
) -> StageResult:
    """REVIEW stage — reviews the PR and posts comments.

    When *registry* is provided, adapters are resolved from it. Otherwise
    falls back to direct invocation for backward compatibility.

    When *codex_enabled* is ``True``, runs both reviewers in parallel
    using a thread pool.  Neither reviewer submits ``gh pr review`` —
    Botfarm aggregates verdicts and submits one final review.
    """
    owner, repo, number = _parse_pr_url(pr_url)

    # --- Single-reviewer mode (no Codex) ---
    if not codex_enabled:
        if stage_tpl is not None and registry is not None:
            adapter = registry[stage_tpl.executor_type]
            prompt_vars = {
                "pr_url": pr_url,
                "pr_number": number,
                "owner": owner,
                "repo": repo,
                "worktree_path": str(cwd),
            }
            return _run_agent_stage(
                stage_tpl, adapter, cwd=cwd, max_turns=max_turns,
                prompt_vars=prompt_vars,
                log_file=log_file, env=env, on_context_fill=on_context_fill,
            )
        if stage_tpl is not None:
            prompt_vars = {
                "pr_url": pr_url,
                "pr_number": number,
                "owner": owner,
                "repo": repo,
                "worktree_path": str(cwd),
            }
            return _run_claude_stage(
                stage_tpl, cwd=cwd, max_turns=max_turns,
                prompt_vars=prompt_vars,
                log_file=log_file, env=env, on_context_fill=on_context_fill,
            )
        # Legacy fallback
        prompt = _build_claude_review_prompt(
            pr_url, owner, repo, number, codex_enabled=False,
        )
        claude_result = _invoke_claude(
            prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
        )
        ar = _claude_result_to_agent_result(claude_result)
        if ar.is_error:
            return StageResult(
                stage="review",
                success=False,
                agent_result=ar,
                error=f"Claude reported error: {ar.result_text[:RESULT_TRUNCATE_CHARS]}",
            )
        claude_approved = _parse_review_approved(ar.result_text)
        return StageResult(
            stage="review",
            success=True,
            agent_result=ar,
            review_approved=claude_approved,
            claude_review_approved=claude_approved,
        )

    # --- Multi-review mode: run Claude and Codex in parallel ---

    # Build the Claude review callable
    def _claude_review() -> AgentResult:
        if stage_tpl is not None:
            prompt_vars = {
                "pr_url": pr_url,
                "pr_number": number,
                "owner": owner,
                "repo": repo,
                "worktree_path": str(cwd),
            }
            prompt = render_prompt(stage_tpl, **prompt_vars)
            prompt += (
                "\n\nDo NOT submit a final review via 'gh pr review'. Only post inline "
                "comments and output your verdict marker."
            )
        else:
            prompt = _build_claude_review_prompt(
                pr_url, owner, repo, number, codex_enabled=True,
            )
        if registry is not None:
            executor = stage_tpl.executor_type if stage_tpl else "claude"
            if executor not in registry:
                raise ValueError(
                    f"No adapter registered for executor_type={executor!r}"
                )
            adapter = registry[executor]
            return adapter.run(
                prompt,
                cwd=Path(cwd),
                max_turns=max_turns if adapter.supports_max_turns else None,
                log_file=log_file,
                env=env,
                on_context_fill=on_context_fill if adapter.supports_context_fill else None,
            )
        cr = _invoke_claude(
            prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
        )
        return _claude_result_to_agent_result(cr)

    def _codex_review() -> StageResult:
        return _run_codex_review(
            pr_url,
            cwd=cwd,
            log_file=codex_log_file,
            env=env,
            timeout=codex_timeout,
            registry=registry,
            codex_model=codex_model,
            codex_reasoning_effort=codex_reasoning_effort,
        )

    # Submit both reviews concurrently — both are subprocess calls so
    # threading works well (GIL is not an issue).
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    try:
        claude_future = executor.submit(_claude_review)
        codex_future = executor.submit(_codex_review)

        # Wait for Claude (required)
        claude_ar = claude_future.result()

        # Short-circuit on Claude error — don't block on Codex
        stage_name = stage_tpl.name if stage_tpl is not None else "review"
        if claude_ar.is_error:
            # Best-effort cancel — won't interrupt an already-running future
            codex_future.cancel()
            return StageResult(
                stage=stage_name,
                success=False,
                agent_result=claude_ar,
                error=f"Claude reported error: {claude_ar.result_text[:RESULT_TRUNCATE_CHARS]}",
            )

        # Process Codex result (fail-open)
        codex_ar: AgentResult | None = None
        codex_approved: bool | None = None
        codex_verdict_str = "skipped"

        try:
            codex_stage_result = codex_future.result()
            codex_ar = codex_stage_result.agent_result

            if codex_stage_result.success:
                codex_approved = codex_stage_result.review_approved
                codex_verdict_str = "approved" if codex_approved else "changes_requested"
            else:
                logger.warning(
                    "Codex review failed (fail-open): %s",
                    codex_stage_result.error or "unknown error",
                )
                codex_verdict_str = "error"
        except Exception:
            logger.warning("Codex review raised exception (fail-open)", exc_info=True)
            codex_verdict_str = "error"
    finally:
        executor.shutdown(wait=False)

    claude_approved = _parse_review_approved(claude_ar.result_text)

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
        stage=stage_name,
        success=True,
        agent_result=claude_ar,
        secondary_agent_result=codex_ar,
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
    registry: AdapterRegistry | None = None,
    codex_model: str | None = None,
    codex_reasoning_effort: str | None = None,
) -> StageResult:
    """CODEX_REVIEW stage — Codex reviews the PR and posts inline comments.

    When *registry* is provided, resolves the Codex adapter from it;
    in that case *codex_reasoning_effort* is ignored because reasoning
    effort is baked into the adapter at construction time.
    Otherwise falls back to direct ``run_codex_streaming()`` invocation.
    """
    owner, repo, number = _parse_pr_url(pr_url)
    prompt = _build_codex_review_prompt(pr_url, owner, repo, number)

    if registry is not None and "codex" in registry:
        adapter = registry["codex"]
        ar = adapter.run(
            prompt,
            cwd=Path(cwd),
            model=codex_model,
            log_file=log_file,
            env=env,
            timeout=timeout,
        )
    else:
        from botfarm.codex import run_codex_streaming
        from botfarm.agent_codex import _codex_result_to_agent_result
        codex_result = run_codex_streaming(
            prompt,
            cwd=cwd,
            model=codex_model,
            reasoning_effort=codex_reasoning_effort,
            log_file=log_file,
            env=env,
            timeout=timeout,
        )
        ar = _codex_result_to_agent_result(codex_result, codex_model)

    if ar.is_error:
        return StageResult(
            stage="codex_review",
            success=False,
            agent_result=ar,
            error=f"Codex reported error: {ar.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    approved = _parse_review_approved(ar.result_text)

    return StageResult(
        stage="codex_review",
        success=True,
        agent_result=ar,
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
    shared_mem_path: Path | None = None,
) -> StageResult:
    """FIX stage — Fresh Claude Code addresses review comments and pushes fixes."""
    owner, repo, number = _parse_pr_url(pr_url)
    if stage_tpl is not None:
        prompt_vars: dict[str, str] = {
            "pr_url": pr_url,
            "pr_number": number,
            "owner": owner,
            "repo": repo,
        }
        if shared_mem_path:
            prompt_vars["shared_mem_path"] = str(shared_mem_path)
        return _run_claude_stage(
            stage_tpl, cwd=cwd, max_turns=max_turns,
            prompt_vars=prompt_vars,
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
    if shared_mem_path:
        prompt += (
            f"\n\nBefore starting, read {shared_mem_path}/implementer.md "
            "(if it exists) for context from the implementer about what was "
            "changed and why."
        )
    claude_result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )
    ar = _claude_result_to_agent_result(claude_result)

    if ar.is_error:
        return StageResult(
            stage="fix",
            success=False,
            agent_result=ar,
            error=f"Claude reported error: {ar.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    return StageResult(
        stage="fix",
        success=True,
        agent_result=ar,
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

    # Pre-check: detect merge conflicts before waiting for CI.
    # When a PR has conflicts, GitHub won't run CI checks, so
    # `gh pr checks --watch` would return "no checks reported"
    # which wastes ci_fix retries on the wrong problem.
    if _check_pr_has_merge_conflict(pr_url, cwd, env=subprocess_env):
        logger.info("PR has merge conflicts — skipping CI check wait")
        return StageResult(
            stage="pr_checks",
            success=False,
            error="PR has merge conflicts — CI checks will not run",
        )

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
    shared_mem_path: Path | None = None,
) -> StageResult:
    """CI FIX stage — Claude Code fixes CI failures using CI output context."""
    if stage_tpl is not None:
        prompt_vars: dict[str, str] = {
            "pr_url": pr_url,
            "ci_failure_output": ci_failure_output[:CI_OUTPUT_TRUNCATE_CHARS],
        }
        if shared_mem_path:
            prompt_vars["shared_mem_path"] = str(shared_mem_path)
        return _run_claude_stage(
            stage_tpl, cwd=cwd, max_turns=max_turns,
            prompt_vars=prompt_vars,
            log_file=log_file, env=env, on_context_fill=on_context_fill,
        )
    # Legacy fallback
    prompt = (
        f"The CI checks on PR {pr_url} have failed. "
        "Diagnose and fix the CI failures based on the output below, "
        "then run tests locally, commit and push the fixes.\n\n"
        f"CI failure output:\n{ci_failure_output[:CI_OUTPUT_TRUNCATE_CHARS]}"
    )
    if shared_mem_path:
        prompt += (
            f"\n\nBefore starting, read {shared_mem_path}/implementer.md "
            "(if it exists) for context from the implementer about what was "
            "changed and why."
        )
    claude_result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )
    ar = _claude_result_to_agent_result(claude_result)

    if ar.is_error:
        return StageResult(
            stage="fix",
            success=False,
            agent_result=ar,
            error=f"Claude reported error: {ar.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    return StageResult(
        stage="fix",
        success=True,
        agent_result=ar,
    )


# ---------------------------------------------------------------------------
# Merge conflict detection + resolution
# ---------------------------------------------------------------------------

_CONFLICT_RE = re.compile(r"conflict|isn't mergeable|not mergeable", re.IGNORECASE)


def _check_pr_has_merge_conflict(
    pr_url: str,
    cwd: str | Path,
    *,
    env: dict[str, str] | None = None,
) -> bool:
    """Check whether a PR has merge conflicts via the GitHub API.

    Queries ``gh pr view --json mergeable`` and returns ``True`` when the
    PR state is ``CONFLICTING``.  Returns ``False`` on any other state or
    on errors (conservative — avoids false positives).
    """
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", pr_url, "--json", "mergeable", "--jq", ".mergeable"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            timeout=30,
            env=env,
        )
        if proc.returncode == 0:
            return proc.stdout.strip().upper() == "CONFLICTING"
    except (subprocess.TimeoutExpired, OSError):
        logger.debug("Could not check PR mergeable state for %s", pr_url, exc_info=True)
    return False


def _is_merge_conflict(
    error_text: str,
    pr_url: str,
    cwd: str | Path,
    *,
    env: dict[str, str] | None = None,
) -> bool:
    """Return True if *error_text* indicates a merge conflict.

    Two-tier approach:
    1. Fast regex check on the ``gh pr merge`` stderr.
    2. Authoritative confirmation via ``gh pr view --json mergeable``.

    Both must agree to avoid false positives from unrelated failures.
    """
    if not _CONFLICT_RE.search(error_text):
        return False

    # Authoritative check via GitHub API
    try:
        proc = subprocess.run(
            ["gh", "pr", "view", pr_url, "--json", "mergeable", "--jq", ".mergeable"],
            capture_output=True,
            text=True,
            cwd=str(cwd),
            timeout=30,
            env=env,
        )
        if proc.returncode != 0:
            logger.warning(
                "gh pr view failed (rc=%d): %s", proc.returncode, proc.stderr[:200],
            )
            return True  # fall back to regex match
        mergeable = proc.stdout.strip().upper()
        return mergeable == "CONFLICTING"
    except (subprocess.TimeoutExpired, OSError):
        # If we can't confirm, fall back to the regex match alone
        logger.warning("Could not confirm merge conflict via gh pr view")
        return True


def _run_resolve_conflict(
    pr_url: str,
    *,
    cwd: str | Path,
    max_turns: int,
    log_file: Path | None = None,
    env: dict[str, str] | None = None,
    on_context_fill: ContextFillCallback | None = None,
    stage_tpl: StageTemplate | None = None,
) -> StageResult:
    """RESOLVE_CONFLICT stage — Claude merges main into feature branch."""
    if stage_tpl is not None:
        return _run_claude_stage(
            stage_tpl, cwd=cwd, max_turns=max_turns,
            log_file=log_file, env=env, on_context_fill=on_context_fill,
        )
    # Legacy fallback
    prompt = (
        "The feature branch has merge conflicts with main. Resolve them by running:\n\n"
        "1. git fetch origin main\n"
        "2. git merge origin/main\n"
        "3. Resolve any merge conflicts — keep the intent of both the feature "
        "branch and main changes\n"
        "4. Run the full test suite and fix any test failures\n"
        "5. git add the resolved files and commit\n"
        "6. git push\n\n"
        "Do NOT force push. Do NOT rebase. Use a merge commit."
    )
    claude_result = _invoke_claude(
        prompt, cwd=cwd, max_turns=max_turns, log_file=log_file,
        env=env, on_context_fill=on_context_fill,
    )
    ar = _claude_result_to_agent_result(claude_result)

    if ar.is_error:
        return StageResult(
            stage="resolve_conflict",
            success=False,
            agent_result=ar,
            error=f"Claude reported error: {ar.result_text[:RESULT_TRUNCATE_CHARS]}",
        )

    return StageResult(
        stage="resolve_conflict",
        success=True,
        agent_result=ar,
    )


def is_protected_branch(branch: str) -> bool:
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
        if is_protected_branch(feature_branch):
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
    registry: AdapterRegistry | None = None,
    shared_mem_path: Path | None = None,
    prior_context: str = "",
    codex_enabled: bool = False,
    codex_model: str | None = None,
    codex_reasoning_effort: str | None = None,
    codex_timeout: float | None = None,
    codex_log_file: Path | None = None,
    review_iteration: int = 1,
) -> StageResult:
    """Dispatch to the appropriate stage runner.

    When *stage_tpl* is provided, routing uses ``executor_type`` from the
    DB template.  ``"shell"`` and ``"internal"`` executor types are handled
    directly; all other types are resolved through *registry*.

    When *stage_tpl* is ``None``, falls back to legacy name-based dispatch.
    """
    # --- DB-driven routing via executor_type ---
    if stage_tpl is not None:
        if stage_tpl.executor_type in ("shell", "internal"):
            # Non-agent executors — keep existing logic unchanged.
            if stage_tpl.executor_type == "shell":
                if not pr_url:
                    raise ValueError(f"{stage} stage requires pr_url")
                return _run_pr_checks(pr_url, cwd=cwd, timeout=pr_checks_timeout, log_file=log_file, env=env)
            else:  # internal
                if not pr_url:
                    raise ValueError(f"{stage} stage requires pr_url")
                return _run_merge(
                    pr_url, cwd=cwd, log_file=log_file,
                    placeholder_branch=placeholder_branch, env=env,
                )

        # --- Agent executor: resolve adapter from registry ---
        if registry is None:
            raise ValueError(
                f"Adapter registry required for executor_type={stage_tpl.executor_type!r} "
                f"on stage {stage!r}"
            )
        adapter = registry.get(stage_tpl.executor_type)
        if adapter is None:
            raise ValueError(
                f"No adapter registered for executor_type={stage_tpl.executor_type!r} "
                f"(stage {stage!r}). Available: {sorted(registry)}"
            )

        # Review stage routes through _run_review() for dual-reviewer flow.
        if stage == "review":
            if not pr_url:
                raise ValueError(f"{stage} stage requires pr_url")
            return _run_review(
                pr_url, cwd=cwd, max_turns=max_turns, log_file=log_file,
                env=env, on_context_fill=on_context_fill,
                stage_tpl=stage_tpl,
                registry=registry,
                codex_enabled=codex_enabled,
                codex_log_file=codex_log_file,
                codex_timeout=codex_timeout,
                codex_model=codex_model,
                codex_reasoning_effort=codex_reasoning_effort,
                review_iteration=review_iteration,
            )

        # All other agent stages use the generic runner.
        prompt_vars: dict[str, str] = {}
        if stage == "implement":
            prompt_vars["ticket_id"] = ticket_id
            prompt_vars["prior_context"] = prior_context
        elif pr_url:
            owner, repo, number = _parse_pr_url(pr_url)
            prompt_vars.update(
                pr_url=pr_url,
                pr_number=number,
                owner=owner,
                repo=repo,
            )
        # Pass shared-mem path so stages can share context.
        if shared_mem_path and stage in ("implement", "fix", "ci_fix"):
            prompt_vars["shared_mem_path"] = str(shared_mem_path)
        return _run_agent_stage(
            stage_tpl, adapter, cwd=cwd, max_turns=max_turns,
            prompt_vars=prompt_vars,
            log_file=log_file, env=env, on_context_fill=on_context_fill,
        )

    # --- Legacy name-based routing (no DB template) ---
    if stage == "implement":
        return _run_implement(
            ticket_id, ticket_labels=ticket_labels,
            cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
            shared_mem_path=shared_mem_path,
            prior_context=prior_context,
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
            codex_reasoning_effort=codex_reasoning_effort,
            review_iteration=review_iteration,
        )
    elif stage == "fix":
        if not pr_url:
            raise ValueError(f"{stage} stage requires pr_url")
        return _run_fix(
            pr_url, cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill,
            codex_enabled=codex_enabled,
            shared_mem_path=shared_mem_path,
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
    elif stage == "resolve_conflict":
        return _run_resolve_conflict(
            pr_url or "", cwd=cwd, max_turns=max_turns, log_file=log_file,
            env=env, on_context_fill=on_context_fill, stage_tpl=stage_tpl,
        )
    else:
        raise ValueError(f"Unknown stage: {stage}")
