"""Tests for Codex reviewer pipeline wiring (SMA-213).

Tests cover:
- _PipelineContext populated with Codex fields from config
- Separate codex_review stage_runs row recorded after review
- Distinct Codex log file path generated
- Codex task events recorded (started, completed, failed, skipped)
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from botfarm.codex import CodexResult
from botfarm.db import get_events, get_stage_runs, insert_task
from botfarm.worker import (
    ClaudeResult,
    PipelineResult,
    StageResult,
    _CodexReviewerConfig,
    _PipelineContext,
    _make_stage_log_path,
    _parse_review_approved,
    run_pipeline,
)
from tests.helpers import (
    claude_result_to_agent,
    codex_result_to_agent,
    make_agent_result,
    make_claude_result,
    make_codex_result,
)


PR_URL = "https://github.com/owner/repo/pull/42"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def task_id(conn):
    return insert_task(
        conn,
        ticket_id="SMA-99",
        title="Test task",
        project="test-project",
        slot=1,
        status="in_progress",
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_stage_result(
    stage,
    success=True,
    pr_url=None,
    turns=3,
    review_approved=None,
    codex_result=None,
):
    ar = None
    secondary = None
    if stage in ("implement", "review", "fix"):
        ar = claude_result_to_agent(make_claude_result())
    if codex_result is not None:
        secondary = codex_result_to_agent(codex_result)
    return StageResult(
        stage=stage,
        success=success,
        agent_result=ar,
        secondary_agent_result=secondary,
        pr_url=pr_url,
        error=None if success else f"{stage} failed",
        review_approved=review_approved,
    )


def _make_ctx(conn, task_id, tmp_path, **overrides):
    defaults = dict(
        ticket_id="SMA-99",
        ticket_labels=[],
        task_id=task_id,
        cwd=str(tmp_path),
        conn=conn,
        turns_cfg={"review": 10},
        pr_checks_timeout=600,
        pipeline=PipelineResult(ticket_id="SMA-99", success=False, stages_completed=[]),
        log_dir=tmp_path / "logs",
    )
    defaults.update(overrides)
    return _PipelineContext(**defaults)


# ---------------------------------------------------------------------------
# test_pipeline_context_codex_fields
# ---------------------------------------------------------------------------


class TestPipelineContextCodexFields:
    """Verify _PipelineContext codex fields populated from config."""

    def test_defaults(self):
        ctx = _PipelineContext(
            ticket_id="SMA-1",
            ticket_labels=[],
            task_id=1,
            cwd="/tmp",
            conn=None,
            turns_cfg={},
            pr_checks_timeout=600,
            pipeline=PipelineResult(ticket_id="SMA-1", success=False, stages_completed=[]),
        )
        assert ctx.codex_config.enabled is False
        assert ctx.codex_config.model == ""
        assert ctx.codex_config.timeout_minutes == 15

    def test_populated_from_kwargs(self):
        ctx = _PipelineContext(
            ticket_id="SMA-1",
            ticket_labels=[],
            task_id=1,
            cwd="/tmp",
            conn=None,
            turns_cfg={},
            pr_checks_timeout=600,
            pipeline=PipelineResult(ticket_id="SMA-1", success=False, stages_completed=[]),
            codex_config=_CodexReviewerConfig(
                enabled=True,
                model="o3",
                timeout_minutes=30,
            ),
        )
        assert ctx.codex_config.enabled is True
        assert ctx.codex_config.model == "o3"
        assert ctx.codex_config.timeout_minutes == 30

    @patch("botfarm.worker._execute_stage")
    def test_run_pipeline_passes_codex_to_context(self, mock_exec, conn, task_id, tmp_path):
        """run_pipeline() threads codex config into _PipelineContext."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
            codex_reviewer_model="o3",
            codex_reviewer_reasoning_effort="low",
            codex_reviewer_timeout_minutes=20,
        )
        # If it ran without error, the params were accepted.
        # Verify codex kwargs were passed to _execute_stage for the review stage.
        review_call = mock_exec.call_args_list[1]
        assert review_call.kwargs.get("codex_enabled") is True
        assert review_call.kwargs.get("codex_model") == "o3"
        assert review_call.kwargs.get("codex_reasoning_effort") == "low"
        assert review_call.kwargs.get("codex_timeout") == 20 * 60.0


# ---------------------------------------------------------------------------
# test_codex_stage_run_recorded
# ---------------------------------------------------------------------------


class TestCodexStageRunRecorded:
    """Verify codex_review rows appear in stage_runs."""

    @patch("botfarm.worker._execute_stage")
    def test_codex_review_row_recorded(self, mock_exec, conn, task_id, tmp_path):
        """When Codex succeeds, a separate codex_review stage_runs row is inserted."""
        codex_res = make_codex_result("VERDICT: APPROVED", thread_id="t-codex")
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=True,
                codex_result=codex_res,
            ),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
            codex_reviewer_model="o3",
            codex_reviewer_timeout_minutes=15,
        )

        runs = get_stage_runs(conn, task_id)
        stages = [r["stage"] for r in runs]
        assert "codex_review" in stages

        codex_run = [r for r in runs if r["stage"] == "codex_review"][0]
        assert codex_run["session_id"] == "t-codex"
        assert codex_run["turns"] == 3
        assert codex_run["input_tokens"] == 1000
        assert codex_run["output_tokens"] == 500
        assert codex_run["cache_read_input_tokens"] == 200
        assert codex_run["iteration"] == 1

    @patch("botfarm.worker._execute_stage")
    def test_codex_review_not_recorded_when_disabled(self, mock_exec, conn, task_id, tmp_path):
        """When Codex is disabled, no codex_review stage_runs row appears."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=False,
        )

        runs = get_stage_runs(conn, task_id)
        stages = [r["stage"] for r in runs]
        assert "codex_review" not in stages

    @patch("botfarm.worker._execute_stage")
    def test_codex_error_still_records_stage_run(self, mock_exec, conn, task_id, tmp_path):
        """When Codex returns is_error, a stage_run row is still recorded."""
        codex_res = make_codex_result("Error output", is_error=True, thread_id="t-codex")
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=False,
                codex_result=codex_res,
            ),
            _mock_stage_result("fix"),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        runs = get_stage_runs(conn, task_id)
        codex_runs = [r for r in runs if r["stage"] == "codex_review"]
        assert len(codex_runs) == 1
        assert codex_runs[0]["session_id"] == "t-codex"


# ---------------------------------------------------------------------------
# test_codex_log_file_separate
# ---------------------------------------------------------------------------


class TestCodexLogFileSeparate:
    """Verify distinct log file created for codex_review."""

    def test_codex_log_path_distinct_from_review(self, tmp_path):
        """_make_stage_log_path produces different paths for review vs codex_review."""
        log_dir = tmp_path / "logs"
        review_path = _make_stage_log_path(log_dir, "review", 1)
        codex_path = _make_stage_log_path(log_dir, "codex_review", 1)
        assert review_path != codex_path
        assert "review" in review_path.name
        assert "codex_review" in codex_path.name

    def test_codex_log_path_iteration_suffix(self, tmp_path):
        log_dir = tmp_path / "logs"
        path_iter1 = _make_stage_log_path(log_dir, "codex_review", 1)
        path_iter2 = _make_stage_log_path(log_dir, "codex_review", 2)
        assert "iter" not in path_iter1.name  # iteration 1 has no suffix
        assert "iter2" in path_iter2.name

    @patch("botfarm.worker._execute_stage")
    def test_codex_log_file_in_stage_run(self, mock_exec, conn, task_id, tmp_path):
        """Codex stage_run row has its own log_file_path (not the Claude one)."""
        codex_res = make_codex_result("VERDICT: APPROVED")
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=True,
                codex_result=codex_res,
            ),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        log_dir = tmp_path / "logs"
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            log_dir=str(log_dir),
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        runs = get_stage_runs(conn, task_id)
        review_run = [r for r in runs if r["stage"] == "review"][0]
        codex_run = [r for r in runs if r["stage"] == "codex_review"][0]

        assert review_run["log_file_path"] is not None
        assert codex_run["log_file_path"] is not None
        assert review_run["log_file_path"] != codex_run["log_file_path"]
        assert "codex_review" in codex_run["log_file_path"]


# ---------------------------------------------------------------------------
# test_codex_events_recorded
# ---------------------------------------------------------------------------


class TestCodexEventsRecorded:
    """Verify new event types in task_events."""

    @patch("botfarm.worker._execute_stage")
    def test_codex_completed_events(self, mock_exec, conn, task_id, tmp_path):
        """Successful Codex review records started + completed events."""
        codex_res = make_codex_result("VERDICT: APPROVED")
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=True,
                codex_result=codex_res,
            ),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "codex_review_started" in event_types
        assert "codex_review_completed" in event_types
        assert "codex_review_failed" not in event_types
        assert "codex_review_skipped" not in event_types

        completed = [e for e in events if e["event_type"] == "codex_review_completed"][0]
        assert "verdict=approved" in completed["detail"]

    @patch("botfarm.worker._execute_stage")
    def test_codex_completed_changes_requested(self, mock_exec, conn, task_id, tmp_path):
        """Codex requests changes → completed event with changes_requested verdict."""
        codex_res = make_codex_result("VERDICT: CHANGES_REQUESTED")
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=False,
                codex_result=codex_res,
            ),
            _mock_stage_result("fix"),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        events = get_events(conn, task_id=task_id)
        completed = [e for e in events if e["event_type"] == "codex_review_completed"]
        assert len(completed) == 1
        assert "verdict=changes_requested" in completed[0]["detail"]

    @patch("botfarm.worker._execute_stage")
    def test_codex_failed_events(self, mock_exec, conn, task_id, tmp_path):
        """Codex error records started + failed events."""
        codex_res = make_codex_result("Error", is_error=True)
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=False,
                codex_result=codex_res,
            ),
            _mock_stage_result("fix"),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "codex_review_started" in event_types
        assert "codex_review_failed" in event_types
        assert "codex_review_completed" not in event_types

    @patch("botfarm.worker._execute_stage")
    def test_codex_skipped_on_claude_failure(self, mock_exec, conn, task_id, tmp_path):
        """Claude review failure records codex_review_skipped."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", success=False),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "codex_review_skipped" in event_types
        skipped = [e for e in events if e["event_type"] == "codex_review_skipped"][0]
        assert "Claude review failed" in skipped["detail"]

    @patch("botfarm.worker._execute_stage")
    def test_codex_skipped_on_exception(self, mock_exec, conn, task_id, tmp_path):
        """Review stage exception records codex_review_skipped."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            Exception("review exploded"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "codex_review_skipped" in event_types
        skipped = [e for e in events if e["event_type"] == "codex_review_skipped"][0]
        assert "review stage raised" in skipped["detail"]

    @patch("botfarm.worker._execute_stage")
    def test_codex_exception_no_result(self, mock_exec, conn, task_id, tmp_path):
        """Codex not invoked (codex_result=None) records codex_review_skipped."""
        # Review succeeds but codex_result is None (Codex never ran)
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=True,
                codex_result=None,  # Codex not invoked
            ),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "codex_review_skipped" in event_types
        skipped = [e for e in events if e["event_type"] == "codex_review_skipped"][0]
        assert "no result" in skipped["detail"]

    @patch("botfarm.worker._execute_stage")
    def test_no_codex_events_when_disabled(self, mock_exec, conn, task_id, tmp_path):
        """No codex events when codex_reviewer_enabled=False."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=False,
        )

        events = get_events(conn, task_id=task_id)
        codex_events = [e for e in events if "codex" in e["event_type"]]
        assert codex_events == []

    @patch("botfarm.worker._execute_stage")
    def test_codex_skipped_on_iteration_2(self, mock_exec, conn, task_id, tmp_path):
        """Codex review skipped on iteration 2+ with appropriate event."""
        codex_res = make_codex_result("VERDICT: CHANGES_REQUESTED")
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result(
                "review",
                review_approved=False,
                codex_result=codex_res,
            ),
            _mock_stage_result("fix"),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            codex_reviewer_enabled=True,
        )

        # Iteration 1 review should have codex kwargs
        review_call_1 = mock_exec.call_args_list[1]
        assert review_call_1.kwargs.get("codex_enabled") is True

        # Iteration 2 review should NOT have codex kwargs
        review_call_2 = mock_exec.call_args_list[3]
        assert "codex_enabled" not in review_call_2.kwargs

        # Verify codex_review_skipped event for iteration 2
        events = get_events(conn, task_id=task_id)
        skipped = [e for e in events if e["event_type"] == "codex_review_skipped"
                   and "iteration" in (e["detail"] or "")]
        assert len(skipped) == 1
        assert "iteration 2" in skipped[0]["detail"]

        # Verify claude_review_completed is recorded for iteration 2
        # even though Codex was skipped (SMA-356 fix).
        claude_reviews = [e for e in events
                          if e["event_type"] == "claude_review_completed"]
        assert len(claude_reviews) >= 1, (
            "claude_review_completed must be recorded when Codex is skipped"
        )
        # Most recent (events are DESC) should be the iteration 2 approval
        assert "verdict=approved" in claude_reviews[0]["detail"]
