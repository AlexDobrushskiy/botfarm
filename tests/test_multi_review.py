"""Tests for multi-review orchestration (SMA-212).

Tests cover:
- Verdict merging (all truth table combinations)
- Codex failure/timeout fallback to Claude-only
- Claude prompt modifications for multi-review mode
- Aggregate review submission
- Codex disabled skips invocation
- Full review stage with mocked Claude + Codex
- Fix loop triggered on Codex rejection
- Parallel execution of Claude and Codex reviews (SMA-361)
"""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from botfarm.codex import CodexResult
from botfarm.workflow import StageTemplate
from botfarm.worker import (
    ClaudeResult,
    StageResult,
    _build_claude_review_prompt,
    _build_codex_review_prompt,
    _merge_review_verdicts,
    _run_codex_review,
    _run_review,
    _submit_aggregate_review,
)
from tests.helpers import (
    codex_result_to_agent,
    make_claude_result as _make_claude_result,
    make_codex_result as _make_codex_result,
)


PR_URL = "https://github.com/owner/repo/pull/42"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_codex_stage_result(
    codex_result: CodexResult,
    success: bool = True,
    review_approved: bool | None = None,
    error: str | None = None,
) -> StageResult:
    """Build a StageResult for a codex_review stage using agent_result."""
    return StageResult(
        stage="codex_review",
        success=success,
        agent_result=codex_result_to_agent(codex_result),
        review_approved=review_approved,
        error=error,
    )


def _make_review_stage_tpl() -> StageTemplate:
    """Create a minimal review StageTemplate for testing."""
    return StageTemplate(
        id=2,
        name="review",
        stage_order=2,
        executor_type="claude",
        identity="reviewer",
        prompt_template="Review the pull request at {pr_url}. VERDICT: APPROVED or CHANGES_REQUESTED",
        max_turns=10,
        timeout_minutes=30,
        shell_command=None,
        result_parser="review_verdict",
    )


# ---------------------------------------------------------------------------
# _merge_review_verdicts — truth table
# ---------------------------------------------------------------------------


class TestVerdictMerge:
    """Test all 7 rows from the verdict merge truth table."""

    def test_both_approved(self):
        assert _merge_review_verdicts(True, True) is True

    def test_claude_approved_codex_changes(self):
        assert _merge_review_verdicts(True, False) is False

    def test_claude_changes_codex_approved(self):
        assert _merge_review_verdicts(False, True) is False

    def test_both_changes_requested(self):
        assert _merge_review_verdicts(False, False) is False

    def test_claude_approved_codex_error(self):
        """Fail-open: Claude's APPROVED verdict stands when Codex errors."""
        assert _merge_review_verdicts(True, None) is True

    def test_claude_changes_codex_error(self):
        """Fail-open: Claude's CHANGES_REQUESTED verdict stands when Codex errors."""
        assert _merge_review_verdicts(False, None) is False

    def test_codex_disabled(self):
        """Single-reviewer fallback: Claude's verdict stands."""
        assert _merge_review_verdicts(True, None) is True
        assert _merge_review_verdicts(False, None) is False


# ---------------------------------------------------------------------------
# Codex failure fallback
# ---------------------------------------------------------------------------


class TestCodexFailureFallback:
    """Codex error degrades gracefully to Claude-only verdict."""

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_error_fallback(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """When Codex returns an error, Claude's verdict stands (fail-open)."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("error", is_error=True),
            success=False,
            error="Codex reported error",
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is True  # Claude's verdict stands
        mock_submit.assert_called_once()
        submit_kwargs = mock_submit.call_args.kwargs
        assert submit_kwargs["codex_verdict"] == "error"
        assert submit_kwargs["approved"] is True


class TestCodexTimeoutFallback:
    """Codex timeout degrades gracefully to Claude-only verdict."""

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_timeout_fallback(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """When Codex raises an exception (timeout), Claude's verdict stands."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.side_effect = Exception("Codex timed out")

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is True  # Claude's verdict stands
        assert result.codex_result is None
        mock_submit.assert_called_once()
        submit_kwargs = mock_submit.call_args.kwargs
        assert submit_kwargs["codex_verdict"] == "error"


# ---------------------------------------------------------------------------
# Claude prompt content tests
# ---------------------------------------------------------------------------


class TestClaudePromptMultiReviewMode:
    """Verify prompt modifications when codex_reviewer_enabled=True."""

    def test_no_submit_gh_pr_review_instruction(self):
        prompt = _build_claude_review_prompt(
            PR_URL, "owner", "repo", "42", codex_enabled=True,
        )
        # Should NOT instruct to submit via gh pr review
        assert "submit your overall assessment" not in prompt
        assert "using 'gh pr review' with either --approve or --request-changes" not in prompt
        # The "Do NOT submit" instruction IS expected
        assert "Do NOT submit a final review via 'gh pr review'" in prompt

    def test_has_verdict_markers(self):
        prompt = _build_claude_review_prompt(
            PR_URL, "owner", "repo", "42", codex_enabled=True,
        )
        assert "VERDICT: APPROVED" in prompt
        assert "VERDICT: CHANGES_REQUESTED" in prompt

    def test_has_no_submit_instruction(self):
        prompt = _build_claude_review_prompt(
            PR_URL, "owner", "repo", "42", codex_enabled=True,
        )
        assert "Do NOT submit a final review" in prompt

    def test_has_inline_comment_api(self):
        prompt = _build_claude_review_prompt(
            PR_URL, "owner", "repo", "42", codex_enabled=True,
        )
        assert "gh api repos/owner/repo/pulls/42/comments" in prompt


class TestClaudePromptSingleReviewMode:
    """Verify prompt is unchanged when codex_reviewer_enabled=False."""

    def test_has_gh_pr_review_instruction(self):
        prompt = _build_claude_review_prompt(
            PR_URL, "owner", "repo", "42", codex_enabled=False,
        )
        assert "gh pr review" in prompt
        assert "--approve" in prompt
        assert "--request-changes" in prompt

    def test_has_verdict_markers(self):
        prompt = _build_claude_review_prompt(
            PR_URL, "owner", "repo", "42", codex_enabled=False,
        )
        assert "VERDICT: APPROVED" in prompt
        assert "VERDICT: CHANGES_REQUESTED" in prompt

    def test_no_do_not_submit(self):
        prompt = _build_claude_review_prompt(
            PR_URL, "owner", "repo", "42", codex_enabled=False,
        )
        assert "Do NOT submit a final review" not in prompt


# ---------------------------------------------------------------------------
# Aggregate review submission
# ---------------------------------------------------------------------------


class TestAggregateReviewSubmission:
    """Verify Botfarm submits correct ``gh pr review``."""

    @patch("botfarm.worker_stages.subprocess.run")
    def test_approved_review(self, mock_run, tmp_path):
        _submit_aggregate_review(
            PR_URL,
            cwd=tmp_path,
            claude_verdict="approved",
            codex_verdict="approved",
            approved=True,
            iteration=1,
        )

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "--approve" in cmd
        assert "42" in cmd
        body_idx = cmd.index("--body")
        body = cmd[body_idx + 1]
        assert "Claude: approved" in body
        assert "Codex: approved" in body
        assert "Overall: approved" in body

    @patch("botfarm.worker_stages.subprocess.run")
    def test_changes_requested_review(self, mock_run, tmp_path):
        _submit_aggregate_review(
            PR_URL,
            cwd=tmp_path,
            claude_verdict="approved",
            codex_verdict="changes_requested",
            approved=False,
            iteration=2,
        )

        cmd = mock_run.call_args[0][0]
        assert "--request-changes" in cmd
        body_idx = cmd.index("--body")
        body = cmd[body_idx + 1]
        assert "Review iteration 2" in body
        assert "Overall: changes_requested" in body

    @patch("botfarm.worker_stages.subprocess.run")
    def test_submission_failure_returns_false(self, mock_run, tmp_path):
        """Failure to submit review returns False (does not raise)."""
        mock_run.side_effect = subprocess.CalledProcessError(1, "gh")

        result = _submit_aggregate_review(
            PR_URL,
            cwd=tmp_path,
            claude_verdict="approved",
            codex_verdict="approved",
            approved=True,
        )
        assert result is False

    @patch("botfarm.worker_stages.subprocess.run")
    def test_submission_success_returns_true(self, mock_run, tmp_path):
        """Successful submission returns True."""
        result = _submit_aggregate_review(
            PR_URL,
            cwd=tmp_path,
            claude_verdict="approved",
            codex_verdict="approved",
            approved=True,
        )
        assert result is True


# ---------------------------------------------------------------------------
# Codex disabled skips invocation
# ---------------------------------------------------------------------------


class TestCodexDisabledSkipsInvocation:
    """No Codex subprocess when disabled."""

    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_not_called_when_disabled(self, mock_claude, mock_codex, tmp_path):
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=False,
        )

        mock_codex.assert_not_called()
        assert result.success is True
        assert result.review_approved is True
        assert result.codex_result is None

    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_not_called_by_default(self, mock_claude, mock_codex, tmp_path):
        """Default codex_enabled=False means no Codex invocation."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")

        result = _run_review(PR_URL, cwd=tmp_path, max_turns=10)

        mock_codex.assert_not_called()


# ---------------------------------------------------------------------------
# Integration: full review stage with mocked Claude + Codex
# ---------------------------------------------------------------------------


class TestReviewStageDualReviewer:
    """Full review stage with mocked Claude + Codex subprocess."""

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_both_approve(self, mock_claude, mock_codex, mock_submit, tmp_path):
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: APPROVED"),
            review_approved=True,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is True
        assert result.claude_result is not None
        assert result.codex_result is not None
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["approved"] is True

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_blocks(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """Codex requests changes while Claude approves — result is blocked."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: CHANGES_REQUESTED"),
            review_approved=False,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is False
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["approved"] is False

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_claude_error_stops_pipeline(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """Claude error stops pipeline — result is failure, no aggregate review."""
        mock_claude.return_value = _make_claude_result("Error", is_error=True)
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: APPROVED"),
            review_approved=True,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is False
        # Claude error short-circuits — we don't wait for Codex.
        mock_submit.assert_not_called()

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_claude_changes_codex_approved(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """Claude requests changes, Codex approves — result is still blocked."""
        mock_claude.return_value = _make_claude_result("VERDICT: CHANGES_REQUESTED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: APPROVED"),
            review_approved=True,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is False


# ---------------------------------------------------------------------------
# Integration: fix loop triggers on Codex rejection
# ---------------------------------------------------------------------------


class TestReviewFixLoopCodexRequestsChanges:
    """Verify fix loop triggers when Codex rejects."""

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_rejection_triggers_changes_requested(
        self, mock_claude, mock_codex, mock_submit, tmp_path,
    ):
        """When Codex requests changes, review_approved is False, triggering fix."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: CHANGES_REQUESTED"),
            review_approved=False,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        # review_approved=False means the review-fix loop will trigger a fix stage
        assert result.review_approved is False
        assert result.success is True


# ---------------------------------------------------------------------------
# _run_codex_review unit tests
# ---------------------------------------------------------------------------


class TestRunCodexReview:
    """Test the _run_codex_review function."""

    @patch("botfarm.codex.run_codex_streaming")
    def test_success_approved(self, mock_codex, tmp_path):
        mock_codex.return_value = _make_codex_result("VERDICT: APPROVED")

        result = _run_codex_review(PR_URL, cwd=tmp_path)

        assert result.success is True
        assert result.stage == "codex_review"
        assert result.review_approved is True
        assert result.agent_result is not None

    @patch("botfarm.codex.run_codex_streaming")
    def test_success_changes_requested(self, mock_codex, tmp_path):
        mock_codex.return_value = _make_codex_result("VERDICT: CHANGES_REQUESTED")

        result = _run_codex_review(PR_URL, cwd=tmp_path)

        assert result.success is True
        assert result.review_approved is False

    @patch("botfarm.codex.run_codex_streaming")
    def test_error_returns_failure(self, mock_codex, tmp_path):
        mock_codex.return_value = _make_codex_result("Error", is_error=True)

        result = _run_codex_review(PR_URL, cwd=tmp_path)

        assert result.success is False
        assert "Codex reported error" in result.error

    @patch("botfarm.codex.run_codex_streaming")
    def test_prompt_contains_codex_prefix(self, mock_codex, tmp_path):
        mock_codex.return_value = _make_codex_result("VERDICT: APPROVED")

        _run_codex_review(PR_URL, cwd=tmp_path)

        prompt = mock_codex.call_args.kwargs.get("prompt") or mock_codex.call_args[0][0]
        assert "CODEX: " in prompt

    @patch("botfarm.codex.run_codex_streaming")
    def test_prompt_no_gh_pr_review(self, mock_codex, tmp_path):
        mock_codex.return_value = _make_codex_result("VERDICT: APPROVED")

        _run_codex_review(PR_URL, cwd=tmp_path)

        prompt = mock_codex.call_args.kwargs.get("prompt") or mock_codex.call_args[0][0]
        assert "Do NOT submit a final review" in prompt

    @patch("botfarm.codex.run_codex_streaming")
    def test_prompt_fetches_existing_comments(self, mock_codex, tmp_path):
        mock_codex.return_value = _make_codex_result("VERDICT: APPROVED")

        _run_codex_review(PR_URL, cwd=tmp_path)

        prompt = mock_codex.call_args.kwargs.get("prompt") or mock_codex.call_args[0][0]
        assert "gh api repos/owner/repo/pulls/42/comments" in prompt

    @patch("botfarm.codex.run_codex_streaming")
    def test_model_and_timeout_passed(self, mock_codex, tmp_path):
        mock_codex.return_value = _make_codex_result("VERDICT: APPROVED")

        _run_codex_review(
            PR_URL, cwd=tmp_path,
            codex_model="o3", timeout=300.0,
        )

        kwargs = mock_codex.call_args.kwargs
        assert kwargs["model"] == "o3"
        assert kwargs["timeout"] == 300.0


# ---------------------------------------------------------------------------
# StageResult extension
# ---------------------------------------------------------------------------


class TestStageResultCodexField:
    """Verify StageResult codex_result backward-compat property."""

    def test_default_none(self):
        sr = StageResult(stage="review", success=True)
        assert sr.codex_result is None

    def test_with_secondary_agent_result(self):
        cr = _make_codex_result("VERDICT: APPROVED")
        ar = codex_result_to_agent(cr)
        sr = StageResult(stage="review", success=True, secondary_agent_result=ar)
        assert sr.codex_result is not None
        assert sr.codex_result.thread_id == "t-test"


# ---------------------------------------------------------------------------
# DB template + Codex integration
# ---------------------------------------------------------------------------


class TestDBTemplateWithCodex:
    """Verify _run_review() works with DB templates and Codex enabled."""

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_both_approve(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """DB template + Codex enabled: both approve → merged approved."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: APPROVED"),
            review_approved=True,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            stage_tpl=_make_review_stage_tpl(),
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is True
        assert result.codex_result is not None
        assert result.claude_result is not None
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["approved"] is True

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_blocks(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """DB template + Codex requests changes → merged blocked."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: CHANGES_REQUESTED"),
            review_approved=False,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            stage_tpl=_make_review_stage_tpl(),
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is False
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["approved"] is False

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_error_fallback(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """DB template + Codex error → Claude verdict stands (fail-open)."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("error", is_error=True),
            success=False,
            error="Codex reported error",
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            stage_tpl=_make_review_stage_tpl(),
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is True  # Claude's verdict stands
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["codex_verdict"] == "error"

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_claude_error_stops(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """DB template + Claude error → result is failure, no aggregate review."""
        mock_claude.return_value = _make_claude_result("Error", is_error=True)
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: APPROVED"),
            review_approved=True,
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            stage_tpl=_make_review_stage_tpl(),
            codex_enabled=True,
        )

        assert result.success is False
        # Claude error short-circuits — we don't wait for Codex.
        mock_submit.assert_not_called()

    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_disabled_uses_claude_stage(self, mock_claude, mock_codex, tmp_path):
        """DB template + Codex disabled → single-reviewer, no Codex."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            stage_tpl=_make_review_stage_tpl(),
            codex_enabled=False,
        )

        assert result.success is True
        assert result.review_approved is True
        mock_codex.assert_not_called()
        assert result.codex_result is None

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_prompt_has_no_submit_instruction(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """DB template + Codex: prompt includes 'Do NOT submit' instruction."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("VERDICT: APPROVED"),
            review_approved=True,
        )

        _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            stage_tpl=_make_review_stage_tpl(),
            codex_enabled=True,
        )

        prompt = mock_claude.call_args[0][0]
        assert "Do NOT submit a final review via 'gh pr review'" in prompt

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_exception_fallback(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """DB template + Codex raises exception → Claude verdict stands."""
        mock_claude.return_value = _make_claude_result("VERDICT: APPROVED")
        mock_codex.side_effect = Exception("Codex timed out")

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            stage_tpl=_make_review_stage_tpl(),
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is True
        assert result.codex_result is None
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["codex_verdict"] == "error"


# ---------------------------------------------------------------------------
# Parallel execution tests (SMA-361)
# ---------------------------------------------------------------------------


class TestParallelReviewExecution:
    """Verify Claude and Codex reviews run concurrently."""

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_reviews_run_concurrently(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """Both reviews should overlap in time (run in parallel threads)."""
        barrier = threading.Barrier(2, timeout=5)

        def slow_claude(*args, **kwargs):
            barrier.wait()  # wait until codex also starts
            return _make_claude_result("VERDICT: APPROVED")

        def slow_codex(*args, **kwargs):
            barrier.wait()  # wait until claude also starts
            return _make_codex_stage_result(
                _make_codex_result("VERDICT: APPROVED"),
                review_approved=True,
            )

        mock_claude.side_effect = slow_claude
        mock_codex.side_effect = slow_codex

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        # If execution were sequential, the barrier would timeout
        assert result.success is True
        assert result.review_approved is True
        mock_claude.assert_called_once()
        mock_codex.assert_called_once()

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_both_fail_returns_claude_error(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """When both reviewers fail, Claude's error is reported."""
        mock_claude.return_value = _make_claude_result("Error", is_error=True)
        mock_codex.return_value = _make_codex_stage_result(
            _make_codex_result("Error", is_error=True),
            success=False,
            error="Codex error",
        )

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is False
        assert result.claude_result is not None
        assert result.claude_result.is_error is True
        mock_submit.assert_not_called()

    @patch("botfarm.worker_stages._submit_aggregate_review")
    @patch("botfarm.worker_stages._run_codex_review")
    @patch("botfarm.worker_stages._invoke_claude")
    def test_codex_timeout_claude_succeeds(self, mock_claude, mock_codex, mock_submit, tmp_path):
        """Codex timeout during parallel execution: Claude verdict stands."""
        mock_claude.return_value = _make_claude_result("VERDICT: CHANGES_REQUESTED")
        mock_codex.side_effect = TimeoutError("Codex timed out")

        result = _run_review(
            PR_URL, cwd=tmp_path, max_turns=10,
            codex_enabled=True,
        )

        assert result.success is True
        assert result.review_approved is False  # Claude's verdict
        assert result.codex_result is None
        mock_submit.assert_called_once()
        assert mock_submit.call_args.kwargs["codex_verdict"] == "error"
