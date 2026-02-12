"""Tests for botfarm.worker — pipeline stages, metrics capture, and orchestration."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.db import get_stage_runs, get_task, init_db, insert_task
from botfarm.slots import SlotManager
from botfarm.worker import (
    STAGES,
    ClaudeResult,
    PipelineResult,
    StageResult,
    parse_claude_output,
    run_claude,
    run_pipeline,
    _extract_pr_url,
    _run_implement,
    _run_review,
    _run_fix,
    _run_pr_checks,
    _run_merge,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def conn(tmp_path):
    db_file = tmp_path / "test.db"
    connection = init_db(db_file)
    yield connection
    connection.close()


@pytest.fixture()
def task_id(conn):
    """Insert a test task and return its id."""
    return insert_task(
        conn,
        ticket_id="SMA-99",
        title="Test task",
        project="test-project",
        slot=1,
        status="in_progress",
    )


@pytest.fixture()
def slot_manager(tmp_path):
    mgr = SlotManager(state_path=tmp_path / "state.json")
    mgr.register_slot("test-project", 1)
    mgr.assign_ticket(
        "test-project",
        1,
        ticket_id="SMA-99",
        ticket_title="Test task",
        branch="test-branch",
    )
    return mgr


def _make_claude_json(
    session_id="sess-abc",
    num_turns=5,
    duration_ms=12000,
    cost_usd=0.25,
    subtype="tool_use",
    result="Done",
    is_error=False,
) -> str:
    return json.dumps(
        {
            "session_id": session_id,
            "num_turns": num_turns,
            "duration_ms": duration_ms,
            "cost_usd": cost_usd,
            "subtype": subtype,
            "result": result,
            "is_error": is_error,
        }
    )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_stages_tuple(self):
        assert STAGES == ("implement", "review", "fix", "pr_checks", "merge")

    def test_stages_ordering(self):
        assert STAGES[0] == "implement"
        assert STAGES[-1] == "merge"


# ---------------------------------------------------------------------------
# parse_claude_output
# ---------------------------------------------------------------------------


class TestParseClaudeOutput:
    def test_basic_parse(self):
        raw = _make_claude_json()
        result = parse_claude_output(raw)
        assert result.session_id == "sess-abc"
        assert result.num_turns == 5
        assert result.duration_seconds == 12.0
        assert result.cost_usd == 0.25
        assert result.exit_subtype == "tool_use"
        assert result.result_text == "Done"
        assert result.is_error is False

    def test_is_error_true(self):
        raw = _make_claude_json(is_error=True)
        result = parse_claude_output(raw)
        assert result.is_error is True

    def test_duration_conversion(self):
        raw = _make_claude_json(duration_ms=60000)
        result = parse_claude_output(raw)
        assert result.duration_seconds == 60.0

    def test_missing_fields_use_defaults(self):
        raw = json.dumps({"result": "ok"})
        result = parse_claude_output(raw)
        assert result.session_id == ""
        assert result.num_turns == 0
        assert result.duration_seconds == 0.0
        assert result.cost_usd == 0.0
        assert result.exit_subtype == ""
        assert result.result_text == "ok"

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            parse_claude_output("not json")


# ---------------------------------------------------------------------------
# run_claude
# ---------------------------------------------------------------------------


class TestRunClaude:
    @patch("botfarm.worker.subprocess.run")
    def test_success(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["claude"],
            returncode=0,
            stdout=_make_claude_json(),
            stderr="",
        )
        result = run_claude("do stuff", cwd=tmp_path, max_turns=10)
        assert result.session_id == "sess-abc"
        assert result.num_turns == 5

        call_args = mock_run.call_args
        cmd = call_args[0][0]
        assert "claude" in cmd
        assert "--max-turns" in cmd
        assert "10" in cmd
        assert "--dangerously-skip-permissions" in cmd
        assert call_args.kwargs["input"] == "do stuff"

    @patch("botfarm.worker.subprocess.run")
    def test_nonzero_exit_raises(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["claude"],
            returncode=1,
            stdout="",
            stderr="error occurred",
        )
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            run_claude("do stuff", cwd=tmp_path, max_turns=10)
        assert exc_info.value.returncode == 1


# ---------------------------------------------------------------------------
# _extract_pr_url
# ---------------------------------------------------------------------------


class TestExtractPrUrl:
    def test_extracts_url(self):
        text = "Created PR: https://github.com/owner/repo/pull/42 done"
        assert _extract_pr_url(text) == "https://github.com/owner/repo/pull/42"

    def test_no_url_returns_none(self):
        assert _extract_pr_url("no pr here") is None

    def test_url_at_end(self):
        text = "see https://github.com/org/project/pull/100"
        assert _extract_pr_url(text) == "https://github.com/org/project/pull/100"

    def test_trailing_punctuation_not_captured(self):
        text = "see https://github.com/owner/repo/pull/42."
        assert _extract_pr_url(text) == "https://github.com/owner/repo/pull/42"

    def test_trailing_fragment_not_captured(self):
        text = "link: https://github.com/owner/repo/pull/42#issuecomment-123"
        assert _extract_pr_url(text) == "https://github.com/owner/repo/pull/42"

    def test_empty_string(self):
        assert _extract_pr_url("") is None


# ---------------------------------------------------------------------------
# Individual stage runners
# ---------------------------------------------------------------------------

PR_URL = "https://github.com/owner/repo/pull/42"


class TestRunImplement:
    @patch("botfarm.worker.run_claude")
    def test_success_with_pr_url(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1",
            num_turns=10,
            duration_seconds=30.0,
            cost_usd=0.5,
            exit_subtype="tool_use",
            result_text=f"Created PR: {PR_URL}",
        )
        result = _run_implement("SMA-1", cwd=tmp_path, max_turns=100)
        assert result.success is True
        assert result.stage == "implement"
        assert result.pr_url == PR_URL
        assert result.claude_result.num_turns == 10

    @patch("botfarm.worker.run_claude")
    def test_success_without_pr_url(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1",
            num_turns=10,
            duration_seconds=30.0,
            cost_usd=0.5,
            exit_subtype="tool_use",
            result_text="Done but no PR link",
        )
        result = _run_implement("SMA-1", cwd=tmp_path, max_turns=100)
        assert result.success is True
        assert result.pr_url is None

    @patch("botfarm.worker.run_claude")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1",
            num_turns=10,
            duration_seconds=30.0,
            cost_usd=0.5,
            exit_subtype="tool_use",
            result_text="Max turns reached",
            is_error=True,
        )
        result = _run_implement("SMA-1", cwd=tmp_path, max_turns=100)
        assert result.success is False
        assert "Claude reported error" in result.error


class TestRunReview:
    @patch("botfarm.worker.run_claude")
    def test_success(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s2",
            num_turns=5,
            duration_seconds=15.0,
            cost_usd=0.2,
            exit_subtype="tool_use",
            result_text="Review posted",
        )
        result = _run_review(PR_URL, cwd=tmp_path, max_turns=50)
        assert result.success is True
        assert result.stage == "review"

    @patch("botfarm.worker.run_claude")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s2",
            num_turns=5,
            duration_seconds=15.0,
            cost_usd=0.2,
            exit_subtype="tool_use",
            result_text="Error occurred",
            is_error=True,
        )
        result = _run_review(PR_URL, cwd=tmp_path, max_turns=50)
        assert result.success is False
        assert "Claude reported error" in result.error


class TestRunFix:
    @patch("botfarm.worker.run_claude")
    def test_success(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s3",
            num_turns=8,
            duration_seconds=20.0,
            cost_usd=0.3,
            exit_subtype="tool_use",
            result_text="Fixes pushed",
        )
        result = _run_fix(PR_URL, cwd=tmp_path, max_turns=50)
        assert result.success is True
        assert result.stage == "fix"

    @patch("botfarm.worker.run_claude")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s3",
            num_turns=8,
            duration_seconds=20.0,
            cost_usd=0.3,
            exit_subtype="tool_use",
            result_text="Error occurred",
            is_error=True,
        )
        result = _run_fix(PR_URL, cwd=tmp_path, max_turns=50)
        assert result.success is False
        assert "Claude reported error" in result.error


class TestRunPrChecks:
    @patch("botfarm.worker.subprocess.run")
    def test_checks_pass(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="All checks passed", stderr=""
        )
        result = _run_pr_checks(PR_URL, cwd=tmp_path)
        assert result.success is True
        assert result.stage == "pr_checks"
        assert result.claude_result is None

    @patch("botfarm.worker.subprocess.run")
    def test_checks_fail(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=1, stdout="Check X failed", stderr=""
        )
        result = _run_pr_checks(PR_URL, cwd=tmp_path)
        assert result.success is False
        assert "CI checks failed" in result.error

    @patch("botfarm.worker.subprocess.run")
    def test_timeout(self, mock_run, tmp_path):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="gh", timeout=5)
        result = _run_pr_checks(PR_URL, cwd=tmp_path, timeout=5)
        assert result.success is False
        assert "timed out" in result.error


class TestRunMerge:
    @patch("botfarm.worker.subprocess.run")
    def test_merge_success(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="Merged", stderr=""
        )
        result = _run_merge(PR_URL, cwd=tmp_path)
        assert result.success is True
        assert result.stage == "merge"

    @patch("botfarm.worker.subprocess.run")
    def test_merge_failure(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=1, stdout="", stderr="merge conflict"
        )
        result = _run_merge(PR_URL, cwd=tmp_path)
        assert result.success is False
        assert "merge conflict" in result.error


# ---------------------------------------------------------------------------
# run_pipeline — full pipeline tests
# ---------------------------------------------------------------------------


def _mock_stage_result(stage, success=True, pr_url=None, cost=0.1, turns=3):
    """Build a StageResult with optional ClaudeResult."""
    cr = None
    if stage in ("implement", "review", "fix"):
        cr = ClaudeResult(
            session_id=f"sess-{stage}",
            num_turns=turns,
            duration_seconds=10.0,
            cost_usd=cost,
            exit_subtype="tool_use",
            result_text="done",
        )
    return StageResult(
        stage=stage,
        success=success,
        claude_result=cr,
        pr_url=pr_url,
        error=None if success else f"{stage} failed",
    )


class TestRunPipeline:
    @patch("botfarm.worker._execute_stage")
    def test_full_success(self, mock_exec, conn, task_id, tmp_path):
        """All 5 stages succeed → pipeline success, metrics recorded."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, cost=0.5, turns=10),
            _mock_stage_result("review", cost=0.2, turns=5),
            _mock_stage_result("fix", cost=0.3, turns=8),
            _mock_stage_result("pr_checks"),  # no claude result
            _mock_stage_result("merge"),  # no claude result
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is True
        assert result.stages_completed == list(STAGES)
        assert result.pr_url == PR_URL
        assert result.total_cost_usd == pytest.approx(1.0)
        assert result.total_turns == 23
        assert result.failure_stage is None

        # Check DB: 5 stage runs recorded
        runs = get_stage_runs(conn, task_id)
        assert len(runs) == 5
        assert [r["stage"] for r in runs] == list(STAGES)

        # Task should be marked completed with timestamp
        task = get_task(conn, task_id)
        assert task["status"] == "completed"
        assert task["cost_usd"] == pytest.approx(1.0)
        assert task["turns"] == 23
        assert task["completed_at"] is not None

    @patch("botfarm.worker._execute_stage")
    def test_implement_no_pr_url_fails(self, mock_exec, conn, task_id, tmp_path):
        """Implement succeeds but produces no PR URL → pipeline fails."""
        mock_exec.return_value = _mock_stage_result("implement", pr_url=None)
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is False
        assert result.failure_stage == "implement"
        assert "PR URL" in result.failure_reason

        task = get_task(conn, task_id)
        assert task["status"] == "failed"

    @patch("botfarm.worker._execute_stage")
    def test_review_failure_stops_pipeline(self, mock_exec, conn, task_id, tmp_path):
        """Review stage fails → pipeline stops, only implement+review recorded."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is False
        assert result.stages_completed == ["implement"]
        assert result.failure_stage == "review"

        runs = get_stage_runs(conn, task_id)
        assert len(runs) == 2

    @patch("botfarm.worker._execute_stage")
    def test_stage_exception_is_caught(self, mock_exec, conn, task_id, tmp_path):
        """An exception during a stage is caught and recorded as failure."""
        mock_exec.side_effect = RuntimeError("subprocess died")
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is False
        assert result.failure_stage == "implement"
        assert "subprocess died" in result.failure_reason

        task = get_task(conn, task_id)
        assert task["status"] == "failed"
        assert "subprocess died" in task["failure_reason"]
        assert task["completed_at"] is not None

    @patch("botfarm.worker._execute_stage")
    def test_merge_failure(self, mock_exec, conn, task_id, tmp_path):
        """Merge fails → pipeline records failure at merge stage."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review"),
            _mock_stage_result("fix"),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is False
        assert result.failure_stage == "merge"
        assert result.stages_completed == ["implement", "review", "fix", "pr_checks"]

    @patch("botfarm.worker._execute_stage")
    def test_slot_manager_integration(self, mock_exec, conn, task_id, slot_manager, tmp_path):
        """SlotManager is updated at each stage when provided."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review"),
            _mock_stage_result("fix"),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            slot_manager=slot_manager,
            project="test-project",
            slot_id=1,
        )
        assert result.success is True

        slot = slot_manager.get_slot("test-project", 1)
        assert set(slot.stages_completed) == set(STAGES)
        assert slot.pr_url == PR_URL

    @patch("botfarm.worker._execute_stage")
    def test_max_turns_override(self, mock_exec, conn, task_id, tmp_path):
        """Custom max_turns are passed through to _execute_stage."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review"),
            _mock_stage_result("fix"),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        custom_turns = {"implement": 50, "review": 25}
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_turns=custom_turns,
        )
        # Verify first call (implement) got max_turns=50
        first_call = mock_exec.call_args_list[0]
        assert first_call.kwargs["max_turns"] == 50
        # Second call (review) got max_turns=25
        second_call = mock_exec.call_args_list[1]
        assert second_call.kwargs["max_turns"] == 25

    @patch("botfarm.worker._execute_stage")
    def test_metrics_accumulation(self, mock_exec, conn, task_id, tmp_path):
        """Costs, turns, and duration accumulate across stages."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, cost=1.0, turns=20),
            _mock_stage_result("review", cost=0.5, turns=10),
            _mock_stage_result("fix", cost=0.3, turns=5),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.total_cost_usd == pytest.approx(1.8)
        assert result.total_turns == 35
        # 3 claude stages × 10s each
        assert result.total_duration_seconds == pytest.approx(30.0)

    @patch("botfarm.worker._execute_stage")
    def test_pr_checks_timeout_override(self, mock_exec, conn, task_id, tmp_path):
        """pr_checks_timeout is passed through."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review"),
            _mock_stage_result("fix"),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            pr_checks_timeout=300,
        )
        # pr_checks is the 4th call (index 3)
        pr_checks_call = mock_exec.call_args_list[3]
        assert pr_checks_call.kwargs["pr_checks_timeout"] == 300


# ---------------------------------------------------------------------------
# StageResult / ClaudeResult dataclasses
# ---------------------------------------------------------------------------


class TestDataclasses:
    def test_claude_result_fields(self):
        cr = ClaudeResult(
            session_id="s1",
            num_turns=10,
            duration_seconds=30.0,
            cost_usd=0.5,
            exit_subtype="tool_use",
            result_text="done",
        )
        assert cr.session_id == "s1"

    def test_stage_result_defaults(self):
        sr = StageResult(stage="implement", success=True)
        assert sr.claude_result is None
        assert sr.pr_url is None
        assert sr.error is None

    def test_pipeline_result_defaults(self):
        pr = PipelineResult(ticket_id="SMA-1", success=False, stages_completed=[])
        assert pr.total_cost_usd == 0.0
        assert pr.total_turns == 0
        assert pr.failure_stage is None
