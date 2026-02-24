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
    _check_pr_merged,
    _extract_pr_url,
    _make_stage_log_path,
    _parse_pr_url,
    _parse_review_approved,
    _recover_pr_url,
    _run_implement,
    _run_review,
    _run_fix,
    _run_ci_fix,
    _run_pr_checks,
    _run_merge,
    _write_subprocess_log,
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
    mgr = SlotManager(db_path=tmp_path / "slots.db")
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
            "total_cost_usd": cost_usd,
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

    @patch("botfarm.worker.subprocess.run")
    def test_writes_log_file_on_success(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["claude"],
            returncode=0,
            stdout=_make_claude_json(),
            stderr="some warning",
        )
        log_file = tmp_path / "logs" / "test.log"
        run_claude("do stuff", cwd=tmp_path, max_turns=10, log_file=log_file)
        assert log_file.exists()
        content = log_file.read_text()
        assert "sess-abc" in content  # stdout (JSON) was written
        assert "some warning" in content  # stderr was written

    @patch("botfarm.worker.subprocess.run")
    def test_writes_log_file_on_failure(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["claude"],
            returncode=1,
            stdout="partial output",
            stderr="error occurred",
        )
        log_file = tmp_path / "logs" / "test.log"
        with pytest.raises(subprocess.CalledProcessError):
            run_claude("do stuff", cwd=tmp_path, max_turns=10, log_file=log_file)
        # Log file should still be written even when the process fails
        assert log_file.exists()
        content = log_file.read_text()
        assert "partial output" in content
        assert "error occurred" in content

    @patch("botfarm.worker.subprocess.run")
    def test_no_log_file_when_none(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["claude"],
            returncode=0,
            stdout=_make_claude_json(),
            stderr="",
        )
        # Should work without log_file (default None)
        result = run_claude("do stuff", cwd=tmp_path, max_turns=10)
        assert result.session_id == "sess-abc"

    @patch("botfarm.worker.subprocess.run")
    def test_env_passed_to_subprocess(self, mock_run, tmp_path):
        """When env is provided, it is merged into os.environ for the subprocess."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["claude"],
            returncode=0,
            stdout=_make_claude_json(),
            stderr="",
        )
        env = {"BOTFARM_DB_PATH": "/tmp/slot.db"}
        run_claude("do stuff", cwd=tmp_path, max_turns=10, env=env)

        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["env"] is not None
        assert call_kwargs["env"]["BOTFARM_DB_PATH"] == "/tmp/slot.db"
        # Should also contain inherited env vars
        assert "PATH" in call_kwargs["env"]

    @patch("botfarm.worker.subprocess.run")
    def test_env_none_no_env_override(self, mock_run, tmp_path):
        """When env is None, subprocess inherits the default environment."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["claude"],
            returncode=0,
            stdout=_make_claude_json(),
            stderr="",
        )
        run_claude("do stuff", cwd=tmp_path, max_turns=10, env=None)

        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["env"] is None


# ---------------------------------------------------------------------------
# _write_subprocess_log / _make_stage_log_path
# ---------------------------------------------------------------------------


class TestWriteSubprocessLog:
    def test_writes_stdout_and_stderr(self, tmp_path):
        log_file = tmp_path / "test.log"
        _write_subprocess_log(log_file, "hello stdout", "hello stderr")
        content = log_file.read_text()
        assert "hello stdout" in content
        assert "hello stderr" in content
        assert "--- STDERR ---" in content

    def test_creates_parent_dirs(self, tmp_path):
        log_file = tmp_path / "a" / "b" / "test.log"
        _write_subprocess_log(log_file, "output", None)
        assert log_file.exists()
        content = log_file.read_text()
        assert "output" in content

    def test_empty_stderr_omits_marker(self, tmp_path):
        log_file = tmp_path / "test.log"
        _write_subprocess_log(log_file, "output", None)
        content = log_file.read_text()
        assert "output" in content
        # No stderr section when stderr is None
        assert "STDERR" not in content

    def test_empty_stdout(self, tmp_path):
        log_file = tmp_path / "test.log"
        _write_subprocess_log(log_file, None, "error only")
        content = log_file.read_text()
        assert "error only" in content


class TestMakeStageLogPath:
    def test_returns_none_when_no_log_dir(self):
        assert _make_stage_log_path(None, "implement") is None

    def test_returns_path_with_stage_and_timestamp(self, tmp_path):
        result = _make_stage_log_path(tmp_path, "implement")
        assert result is not None
        assert result.parent == tmp_path
        assert result.name.startswith("implement-")
        assert result.suffix == ".log"

    def test_includes_iteration_suffix(self, tmp_path):
        result = _make_stage_log_path(tmp_path, "review", iteration=2)
        assert result is not None
        assert "-iter2-" in result.name

    def test_no_iteration_suffix_for_first(self, tmp_path):
        result = _make_stage_log_path(tmp_path, "review", iteration=1)
        assert result is not None
        assert "iter" not in result.name


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

    @patch("botfarm.worker._check_pr_merged", return_value=True)
    @patch("botfarm.worker.subprocess.run")
    def test_merge_failure_but_pr_actually_merged(self, mock_run, mock_check, tmp_path):
        """When gh pr merge fails but PR is actually merged, return success."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=1, stdout="",
            stderr="fatal: 'main' is already used by worktree",
        )
        result = _run_merge(PR_URL, cwd=tmp_path)
        assert result.success is True
        assert result.stage == "merge"
        mock_check.assert_called_once_with(PR_URL, tmp_path)

    @patch("botfarm.worker._check_pr_merged", return_value=False)
    @patch("botfarm.worker.subprocess.run")
    def test_merge_failure_pr_not_merged(self, mock_run, mock_check, tmp_path):
        """When gh pr merge fails and PR is not merged, return failure."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=1, stdout="", stderr="merge conflict"
        )
        result = _run_merge(PR_URL, cwd=tmp_path)
        assert result.success is False
        assert "merge conflict" in result.error

    @patch("botfarm.worker.subprocess.run")
    def test_merge_uses_delete_branch_flag(self, mock_run, tmp_path):
        """Merge command includes --delete-branch flag."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="Merged", stderr=""
        )
        _run_merge(PR_URL, cwd=tmp_path)
        call_args = mock_run.call_args[0][0]
        assert "--delete-branch" in call_args


class TestCheckPrMerged:
    @patch("botfarm.worker.subprocess.run")
    def test_pr_merged(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="MERGED\n", stderr=""
        )
        assert _check_pr_merged(PR_URL, tmp_path) is True

    @patch("botfarm.worker.subprocess.run")
    def test_pr_open(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="OPEN\n", stderr=""
        )
        assert _check_pr_merged(PR_URL, tmp_path) is False

    @patch("botfarm.worker.subprocess.run")
    def test_pr_check_fails(self, mock_run, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=1, stdout="", stderr="error"
        )
        assert _check_pr_merged(PR_URL, tmp_path) is False

    @patch("botfarm.worker.subprocess.run")
    def test_pr_check_timeout(self, mock_run, tmp_path):
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="gh", timeout=15)
        assert _check_pr_merged(PR_URL, tmp_path) is False


# ---------------------------------------------------------------------------
# run_pipeline — full pipeline tests
# ---------------------------------------------------------------------------


def _mock_stage_result(stage, success=True, pr_url=None, cost=0.1, turns=3, review_approved=None):
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
        review_approved=review_approved,
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
            max_review_iterations=1,
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
    def test_pr_url_persisted_to_db(self, mock_exec, conn, task_id, tmp_path):
        """PR URL from implement stage is written to tasks DB."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, cost=0.5, turns=10),
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
        )
        task = get_task(conn, task_id)
        assert task["pr_url"] == PR_URL

    @patch("botfarm.worker._execute_stage")
    def test_pipeline_stage_persisted_to_db(self, mock_exec, conn, task_id, tmp_path):
        """pipeline_stage is written to DB at each stage transition."""
        stages_in_db = []

        def capture_stage(stage, **kwargs):
            task = get_task(conn, task_id)
            stages_in_db.append(task["pipeline_stage"])
            return _mock_stage_result(stage, pr_url=PR_URL if stage == "implement" else None)

        mock_exec.side_effect = capture_stage
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
        )
        # Each stage should have been written before execution
        assert "implement" in stages_in_db
        assert "review" in stages_in_db
        assert "merge" in stages_in_db

    @patch("botfarm.worker._execute_stage")
    def test_review_state_persisted_to_db(self, mock_exec, conn, task_id, tmp_path):
        """review_state is written to DB after review stage."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=False),
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
        )
        task = get_task(conn, task_id)
        assert task["review_state"] == "approved"

    @patch("botfarm.worker._execute_stage")
    def test_slot_db_path_threaded_as_env(self, mock_exec, conn, task_id, tmp_path):
        """slot_db_path is passed as BOTFARM_DB_PATH env to _execute_stage."""
        envs_seen = []

        def capture_env(stage, **kwargs):
            envs_seen.append(kwargs.get("env"))
            return _mock_stage_result(stage, pr_url=PR_URL if stage == "implement" else None)

        mock_exec.side_effect = capture_env
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
            slot_db_path="/tmp/slot-db/botfarm.db",
        )
        # Every stage should receive the env with BOTFARM_DB_PATH
        assert all(e == {"BOTFARM_DB_PATH": "/tmp/slot-db/botfarm.db"} for e in envs_seen)

    @patch("botfarm.worker._execute_stage")
    def test_no_slot_db_path_no_env(self, mock_exec, conn, task_id, tmp_path):
        """Without slot_db_path, no env override is passed."""
        envs_seen = []

        def capture_env(stage, **kwargs):
            envs_seen.append(kwargs.get("env"))
            return _mock_stage_result(stage, pr_url=PR_URL if stage == "implement" else None)

        mock_exec.side_effect = capture_env
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
        )
        assert all(e is None for e in envs_seen)

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
        """Review stage fails → pipeline stops, only implement recorded."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
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
            max_review_iterations=1,
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
            max_review_iterations=1,
        )
        assert result.success is True

        slot = slot_manager.get_slot("test-project", 1)
        assert set(slot.stages_completed) == set(STAGES)
        assert slot.pr_url == PR_URL

    @patch("botfarm.worker._execute_stage")
    def test_db_path_updates_stage_in_db(self, mock_exec, conn, task_id, tmp_path):
        """When db_path is provided, pipeline writes stage to the database."""
        from botfarm.slots import SlotManager

        db_path = tmp_path / "slots.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("test-project", 1)
        mgr.assign_ticket(
            "test-project", 1,
            ticket_id="SMA-99", ticket_title="Test", branch="b1",
        )

        stages_seen = []

        def capture_stage(stage, **kwargs):
            import sqlite3 as _sqlite3
            _conn = _sqlite3.connect(str(db_path))
            _conn.row_factory = _sqlite3.Row
            row = _conn.execute(
                "SELECT stage FROM slots WHERE project='test-project' AND slot_id=1"
            ).fetchone()
            stages_seen.append(row["stage"] if row else None)
            _conn.close()
            return _mock_stage_result(stage, pr_url=PR_URL if stage == "implement" else None)

        mock_exec.side_effect = capture_stage

        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            db_path=str(db_path),
            project="test-project",
            slot_id=1,
            max_review_iterations=1,
        )
        assert result.success is True
        # Each stage should have been written to the DB before execution
        assert "implement" in stages_seen
        assert "review" in stages_seen

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
            max_review_iterations=1,
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
            max_review_iterations=1,
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
            max_review_iterations=1,
        )
        # pr_checks is the 4th call (index 3)
        pr_checks_call = mock_exec.call_args_list[3]
        assert pr_checks_call.kwargs["pr_checks_timeout"] == 300

    @patch("botfarm.worker._execute_stage")
    def test_log_dir_passed_to_execute_stage(self, mock_exec, conn, task_id, tmp_path):
        """When log_dir is set, _execute_stage receives a log_file path."""
        log_dir = tmp_path / "logs" / "SMA-99"
        log_dir.mkdir(parents=True)

        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, cost=0.5, turns=10),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            log_dir=str(log_dir),
            max_review_iterations=3,
        )
        # Every call to _execute_stage should have a log_file kwarg
        for call in mock_exec.call_args_list:
            log_file = call.kwargs.get("log_file")
            assert log_file is not None
            assert str(log_dir) in str(log_file)
            assert log_file.suffix == ".log"

    @patch("botfarm.worker._execute_stage")
    def test_no_log_dir_passes_none(self, mock_exec, conn, task_id, tmp_path):
        """When log_dir is not set, _execute_stage receives log_file=None."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, cost=0.5, turns=10),
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
        )
        for call in mock_exec.call_args_list:
            assert call.kwargs.get("log_file") is None


# ---------------------------------------------------------------------------
# run_pipeline — resume from stage tests
# ---------------------------------------------------------------------------


class TestRunPipelineResume:
    @patch("botfarm.worker._recover_pr_url")
    @patch("botfarm.worker._execute_stage")
    def test_resume_from_review_skips_implement(
        self, mock_exec, mock_recover, conn, task_id, tmp_path,
    ):
        """Resuming from 'review' should skip 'implement' and start at review."""
        mock_recover.return_value = PR_URL
        mock_exec.side_effect = [
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
            resume_from_stage="review",
            max_review_iterations=1,
        )
        assert result.success is True
        assert result.stages_completed == list(STAGES)
        assert result.pr_url == PR_URL

        # Only 4 stage executions (review through merge)
        assert mock_exec.call_count == 4
        called_stages = [c[0][0] for c in mock_exec.call_args_list]
        assert called_stages == ["review", "fix", "pr_checks", "merge"]

    @patch("botfarm.worker._recover_pr_url")
    @patch("botfarm.worker._execute_stage")
    def test_resume_from_fix_skips_implement_and_review(
        self, mock_exec, mock_recover, conn, task_id, tmp_path,
    ):
        """Resuming from 'fix' should skip implement and review."""
        mock_recover.return_value = PR_URL
        mock_exec.side_effect = [
            _mock_stage_result("fix"),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            resume_from_stage="fix",
            max_review_iterations=1,
        )
        assert result.success is True
        assert mock_exec.call_count == 3

    @patch("botfarm.worker._recover_pr_url")
    @patch("botfarm.worker._execute_stage")
    def test_resume_from_fix_enters_review_loop(
        self, mock_exec, mock_recover, conn, task_id, tmp_path,
    ):
        """Resuming from 'fix' with max_review_iterations>1 re-reviews after fix."""
        mock_recover.return_value = PR_URL
        mock_exec.side_effect = [
            _mock_stage_result("fix"),           # resumed fix
            _mock_stage_result("review", review_approved=True),  # verify fix
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            resume_from_stage="fix",
            max_review_iterations=3,
        )
        assert result.success is True
        assert mock_exec.call_count == 4
        called_stages = [c[0][0] for c in mock_exec.call_args_list]
        assert called_stages == ["fix", "review", "pr_checks", "merge"]

    @patch("botfarm.worker._recover_pr_url")
    @patch("botfarm.worker._execute_stage")
    def test_resume_failure_at_resumed_stage(
        self, mock_exec, mock_recover, conn, task_id, tmp_path,
    ):
        """If the resumed stage fails, pipeline should fail."""
        mock_recover.return_value = PR_URL
        mock_exec.return_value = _mock_stage_result("review", success=False)

        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            resume_from_stage="review",
        )
        assert result.success is False
        assert result.failure_stage == "review"

    @patch("botfarm.worker._execute_stage")
    def test_resume_from_implement_runs_all_stages(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """Resuming from 'implement' should run all stages normally."""
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
            resume_from_stage="implement",
            max_review_iterations=1,
        )
        assert result.success is True
        assert mock_exec.call_count == 5

    def test_resume_invalid_stage_fails(self, conn, task_id, tmp_path):
        """Resuming from an unknown stage should fail immediately."""
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            resume_from_stage="implment",
        )
        assert result.success is False
        assert result.failure_stage == "implment"
        assert "Unknown resume stage" in result.failure_reason


# ---------------------------------------------------------------------------
# _recover_pr_url
# ---------------------------------------------------------------------------


class TestRecoverPrUrl:
    @patch("botfarm.worker.subprocess.run")
    def test_recovers_url_from_gh(self, mock_run, conn, task_id, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"],
            returncode=0,
            stdout="https://github.com/owner/repo/pull/42\n",
            stderr="",
        )
        url = _recover_pr_url(conn, task_id, tmp_path)
        assert url == "https://github.com/owner/repo/pull/42"

    @patch("botfarm.worker.subprocess.run")
    def test_returns_none_on_gh_failure(self, mock_run, conn, task_id, tmp_path):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"],
            returncode=1,
            stdout="",
            stderr="no PR found",
        )
        url = _recover_pr_url(conn, task_id, tmp_path)
        assert url is None

    @patch("botfarm.worker.subprocess.run")
    def test_returns_none_on_exception(self, mock_run, conn, task_id, tmp_path):
        mock_run.side_effect = FileNotFoundError("gh not found")
        url = _recover_pr_url(conn, task_id, tmp_path)
        assert url is None


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

    def test_stage_result_review_approved_default(self):
        sr = StageResult(stage="review", success=True)
        assert sr.review_approved is None


# ---------------------------------------------------------------------------
# _parse_review_approved
# ---------------------------------------------------------------------------


class TestParseReviewApproved:
    def test_approve_flag(self):
        assert _parse_review_approved("I ran gh pr review --approve") is True

    def test_request_changes_flag(self):
        assert _parse_review_approved("gh pr review --request-changes") is False

    def test_lgtm(self):
        assert _parse_review_approved("LGTM, ship it!") is True

    def test_looks_good_to_me(self):
        assert _parse_review_approved("This looks good to me overall.") is True

    def test_approve_the_pr(self):
        assert _parse_review_approved("I approve the PR.") is True

    def test_changes_requested(self):
        assert _parse_review_approved("Changes requested: fix the typo") is False

    def test_needs_changes(self):
        assert _parse_review_approved("This needs changes before merging.") is False

    def test_ambiguous_defaults_to_false(self):
        assert _parse_review_approved("I reviewed the code.") is False

    def test_empty_string(self):
        assert _parse_review_approved("") is False

    def test_changes_pattern_takes_priority(self):
        # If both approve and changes patterns are present, changes wins
        assert _parse_review_approved("LGTM but request changes on the tests") is False

    def test_ignores_lgtm_early_in_output(self):
        # "LGTM" in quoted PR content early in the output should not cause false positive
        early_lgtm = "The PR description says LGTM. " + ("x" * 600) + "I request changes."
        assert _parse_review_approved(early_lgtm) is False

    def test_detects_approval_in_tail(self):
        # Approval signal in the last 500 chars is detected
        long_prefix = "Reviewing code... " + ("x" * 600)
        assert _parse_review_approved(long_prefix + " I ran gh pr review --approve") is True

    def test_verdict_marker_approved(self):
        assert _parse_review_approved("Review complete.\nVERDICT: APPROVED") is True

    def test_verdict_marker_changes_requested(self):
        assert _parse_review_approved("Issues found.\nVERDICT: CHANGES_REQUESTED") is False

    def test_verdict_marker_takes_priority_over_heuristics(self):
        # Even if heuristic keywords suggest approval, the marker wins
        text = "LGTM overall but VERDICT: CHANGES_REQUESTED"
        assert _parse_review_approved(text) is False

    def test_verdict_marker_approved_with_whitespace(self):
        assert _parse_review_approved("VERDICT:  APPROVED") is True

    def test_verdict_case_insensitive_approved(self):
        assert _parse_review_approved("Verdict: Approved") is True

    def test_verdict_case_insensitive_changes_requested(self):
        assert _parse_review_approved("verdict: changes_requested") is False

    def test_verdict_mixed_case(self):
        assert _parse_review_approved("Verdict: APPROVED") is True

    def test_verdict_beyond_500_char_tail(self):
        # VERDICT marker outside the last 500 chars should still be found
        text = "VERDICT: APPROVED\n" + ("x" * 600)
        assert _parse_review_approved(text) is True

    def test_verdict_changes_requested_beyond_tail(self):
        text = "VERDICT: CHANGES_REQUESTED\n" + ("x" * 600)
        assert _parse_review_approved(text) is False

    def test_gh_pr_review_approve_full_text(self):
        text = "I ran gh pr review 42 --approve\n" + ("x" * 600)
        assert _parse_review_approved(text) is True

    def test_gh_pr_review_request_changes_full_text(self):
        text = "I ran gh pr review 42 --request-changes\n" + ("x" * 600)
        assert _parse_review_approved(text) is False

    def test_verdict_takes_priority_over_gh_command(self):
        # VERDICT marker wins over gh pr review command
        text = "gh pr review --approve\nVERDICT: CHANGES_REQUESTED"
        assert _parse_review_approved(text) is False

    def test_multiple_verdicts_last_wins(self):
        # Early VERDICT (e.g. quoted from test code) should not shadow the real one
        text = "The test checks VERDICT: APPROVED\n...later...\nVERDICT: CHANGES_REQUESTED"
        assert _parse_review_approved(text) is False

    def test_multiple_verdicts_last_wins_approved(self):
        text = "Quoted VERDICT: CHANGES_REQUESTED from diff\n...later...\nVERDICT: APPROVED"
        assert _parse_review_approved(text) is True

    def test_fallback_warning_logged(self, caplog):
        # When no VERDICT or gh command found, warning is logged
        import logging

        with caplog.at_level(logging.WARNING, logger="botfarm.worker"):
            _parse_review_approved("LGTM, ship it!")
        assert "falling back to keyword heuristics" in caplog.text


# ---------------------------------------------------------------------------
# run_pipeline — review iteration loop tests
# ---------------------------------------------------------------------------


class TestReviewIterationLoop:
    @patch("botfarm.worker._execute_stage")
    def test_review_approved_first_iteration_skips_fix(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """When review approves on first iteration, fix is skipped."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
        )
        assert result.success is True
        assert result.stages_completed == list(STAGES)
        assert mock_exec.call_count == 4

        task = get_task(conn, task_id)
        assert task["review_iterations"] == 1

    @patch("botfarm.worker._execute_stage")
    def test_review_changes_then_approved_second_iteration(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """Review requests changes, fix runs, second review approves."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            # Iteration 1: review requests changes
            _mock_stage_result("review", review_approved=False, cost=0.2, turns=5),
            _mock_stage_result("fix", cost=0.3, turns=8),
            # Iteration 2: review approves
            _mock_stage_result("review", review_approved=True, cost=0.1, turns=3),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
        )
        assert result.success is True
        assert mock_exec.call_count == 6

        task = get_task(conn, task_id)
        assert task["review_iterations"] == 2

        # Check stage runs include iteration tracking
        runs = get_stage_runs(conn, task_id)
        review_runs = [r for r in runs if r["stage"] == "review"]
        assert len(review_runs) == 2
        assert review_runs[0]["iteration"] == 1
        assert review_runs[1]["iteration"] == 2

    @patch("botfarm.worker._execute_stage")
    def test_max_iterations_reached(self, mock_exec, conn, task_id, tmp_path):
        """After max iterations, proceed to pr_checks regardless."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            # Iteration 1
            _mock_stage_result("review", review_approved=False),
            _mock_stage_result("fix"),
            # Iteration 2
            _mock_stage_result("review", review_approved=False),
            _mock_stage_result("fix"),
            # pr_checks and merge
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=2,
        )
        assert result.success is True
        assert result.stages_completed == list(STAGES)
        assert mock_exec.call_count == 7

        task = get_task(conn, task_id)
        assert task["review_iterations"] == 2

    @patch("botfarm.worker._execute_stage")
    def test_fix_failure_in_iteration_stops_pipeline(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """Fix failure during iteration loop stops the pipeline."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=False),
            _mock_stage_result("fix", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
        )
        assert result.success is False
        assert result.failure_stage == "fix"

    @patch("botfarm.worker._execute_stage")
    def test_review_failure_in_second_iteration_stops_pipeline(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """Review failure on second iteration stops the pipeline."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=False),
            _mock_stage_result("fix"),
            _mock_stage_result("review", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
        )
        assert result.success is False
        assert result.failure_stage == "review"

    @patch("botfarm.worker._execute_stage")
    def test_metrics_accumulate_across_iterations(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """Costs and turns from all review/fix iterations are accumulated."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, cost=1.0, turns=20),
            # Iteration 1
            _mock_stage_result("review", review_approved=False, cost=0.2, turns=5),
            _mock_stage_result("fix", cost=0.3, turns=8),
            # Iteration 2 — approved
            _mock_stage_result("review", review_approved=True, cost=0.1, turns=3),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
        )
        assert result.success is True
        # 1.0 + 0.2 + 0.3 + 0.1 = 1.6
        assert result.total_cost_usd == pytest.approx(1.6)
        # 20 + 5 + 8 + 3 = 36
        assert result.total_turns == 36

    @patch("botfarm.worker._execute_stage")
    def test_events_logged_for_iterations(self, mock_exec, conn, task_id, tmp_path):
        """Review iteration events are logged in task_events."""
        from botfarm.db import get_events

        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=False),
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
        )
        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "review_iteration_started" in event_types
        assert "review_approved" in event_types

    @patch("botfarm.worker._execute_stage")
    def test_max_iterations_reached_event(self, mock_exec, conn, task_id, tmp_path):
        """max_iterations_reached event is logged when limit is hit."""
        from botfarm.db import get_events

        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=False),
            _mock_stage_result("fix"),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
        )
        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "max_iterations_reached" in event_types


# ---------------------------------------------------------------------------
# _run_ci_fix
# ---------------------------------------------------------------------------


class TestRunCiFix:
    @patch("botfarm.worker.run_claude")
    def test_success(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s-ci",
            num_turns=6,
            duration_seconds=18.0,
            cost_usd=0.35,
            exit_subtype="tool_use",
            result_text="Fixed CI failures",
        )
        result = _run_ci_fix(
            PR_URL, ci_failure_output="test_foo FAILED", cwd=tmp_path, max_turns=100,
        )
        assert result.success is True
        assert result.stage == "fix"
        assert result.claude_result.session_id == "s-ci"

        # Verify prompt includes CI failure output
        prompt = mock_claude.call_args.args[0]
        assert "CI checks" in prompt
        assert "test_foo FAILED" in prompt

    @patch("botfarm.worker.run_claude")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s-ci",
            num_turns=6,
            duration_seconds=18.0,
            cost_usd=0.35,
            exit_subtype="tool_use",
            result_text="Error occurred",
            is_error=True,
        )
        result = _run_ci_fix(
            PR_URL, ci_failure_output="build failed", cwd=tmp_path, max_turns=100,
        )
        assert result.success is False
        assert "Claude reported error" in result.error

    @patch("botfarm.worker.run_claude")
    def test_ci_output_truncated_at_2000_chars(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s-ci",
            num_turns=3,
            duration_seconds=10.0,
            cost_usd=0.1,
            exit_subtype="tool_use",
            result_text="Fixed",
        )
        long_output = "x" * 5000
        _run_ci_fix(PR_URL, ci_failure_output=long_output, cwd=tmp_path, max_turns=100)
        prompt = mock_claude.call_args.args[0]
        # The prompt should contain at most 2000 chars of the CI output
        assert "x" * 2000 in prompt
        assert "x" * 2001 not in prompt


# ---------------------------------------------------------------------------
# run_pipeline — CI retry loop tests
# ---------------------------------------------------------------------------


class TestCiRetryLoop:
    @patch("botfarm.worker._execute_stage")
    def test_ci_passes_first_try_no_retry(self, mock_exec, conn, task_id, tmp_path):
        """CI passes on first try — no retries needed."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        assert result.success is True
        assert result.stages_completed == list(STAGES)
        assert mock_exec.call_count == 4

    @patch("botfarm.worker._run_ci_fix")
    @patch("botfarm.worker._execute_stage")
    def test_ci_fails_then_passes_after_retry(self, mock_exec, mock_ci_fix, conn, task_id, tmp_path):
        """CI fails, fix runs with CI context, CI passes on retry."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            # First pr_checks: fails
            _mock_stage_result("pr_checks", success=False),
            # Second pr_checks (after CI fix): passes
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix", cost=0.3, turns=8)
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        assert result.success is True
        assert result.stages_completed == list(STAGES)

        # Verify _run_ci_fix was called with ci_failure_output
        assert mock_ci_fix.call_count == 1
        assert mock_ci_fix.call_args.kwargs.get("ci_failure_output") is not None

    @patch("botfarm.worker._run_ci_fix")
    @patch("botfarm.worker._execute_stage")
    def test_ci_fails_all_retries_exhausted(self, mock_exec, mock_ci_fix, conn, task_id, tmp_path):
        """CI fails and all retries exhausted — pipeline fails."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            # First pr_checks: fails
            _mock_stage_result("pr_checks", success=False),
            # Retry 1: pr_checks fails again
            _mock_stage_result("pr_checks", success=False),
            # Retry 2: pr_checks fails again
            _mock_stage_result("pr_checks", success=False),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix")
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        assert result.success is False
        assert result.failure_stage == "pr_checks"
        assert "2 retries" in result.failure_reason
        assert mock_ci_fix.call_count == 2

        task = get_task(conn, task_id)
        assert task["status"] == "failed"

    @patch("botfarm.worker._run_ci_fix")
    @patch("botfarm.worker._execute_stage")
    def test_ci_fix_failure_stops_pipeline(self, mock_exec, mock_ci_fix, conn, task_id, tmp_path):
        """If the CI fix stage fails, pipeline stops immediately."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            # First pr_checks: fails
            _mock_stage_result("pr_checks", success=False),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix", success=False)
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        assert result.success is False
        assert result.failure_stage == "fix"

    @patch("botfarm.worker._execute_stage")
    def test_ci_retries_zero_disables_retry(self, mock_exec, conn, task_id, tmp_path):
        """With max_ci_retries=0, CI failure is immediate pipeline failure."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=0,
        )
        assert result.success is False
        assert result.failure_stage == "pr_checks"
        assert mock_exec.call_count == 3

    @patch("botfarm.worker._run_ci_fix")
    @patch("botfarm.worker._execute_stage")
    def test_ci_retry_events_logged(self, mock_exec, mock_ci_fix, conn, task_id, tmp_path):
        """CI retry events are logged in task_events."""
        from botfarm.db import get_events

        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks", success=False),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix")
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "ci_retry_started" in event_types
        assert "ci_passed_after_retry" in event_types

    @patch("botfarm.worker._run_ci_fix")
    @patch("botfarm.worker._execute_stage")
    def test_ci_max_retries_event_logged(self, mock_exec, mock_ci_fix, conn, task_id, tmp_path):
        """max_ci_retries_reached event is logged when retries exhausted."""
        from botfarm.db import get_events

        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks", success=False),
            _mock_stage_result("pr_checks", success=False),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix")
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=1,
        )
        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "ci_retry_started" in event_types
        assert "max_ci_retries_reached" in event_types

    @patch("botfarm.worker._run_ci_fix")
    @patch("botfarm.worker._execute_stage")
    def test_ci_retry_metrics_accumulate(self, mock_exec, mock_ci_fix, conn, task_id, tmp_path):
        """Costs and turns from CI fix retries accumulate in pipeline metrics."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, cost=1.0, turns=20),
            _mock_stage_result("review", review_approved=True, cost=0.2, turns=5),
            _mock_stage_result("pr_checks", success=False),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix", cost=0.4, turns=10)
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        assert result.success is True
        # 1.0 + 0.2 + 0.4 = 1.6
        assert result.total_cost_usd == pytest.approx(1.6)
        # 20 + 5 + 10 = 35
        assert result.total_turns == 35

    @patch("botfarm.worker._run_ci_fix")
    @patch("botfarm.worker._execute_stage")
    def test_ci_retry_second_attempt_passes(self, mock_exec, mock_ci_fix, conn, task_id, tmp_path):
        """CI fails twice, passes on second retry."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            # Initial pr_checks: fails
            _mock_stage_result("pr_checks", success=False),
            # Retry 1: pr_checks fails
            _mock_stage_result("pr_checks", success=False),
            # Retry 2: pr_checks passes
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix")
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        assert result.success is True
        assert mock_ci_fix.call_count == 2


# ---------------------------------------------------------------------------
# _parse_pr_url
# ---------------------------------------------------------------------------


class TestParsePrUrl:
    def test_valid_url(self):
        owner, repo, number = _parse_pr_url("https://github.com/owner/repo/pull/42")
        assert owner == "owner"
        assert repo == "repo"
        assert number == "42"

    def test_complex_names(self):
        owner, repo, number = _parse_pr_url(
            "https://github.com/my-org/my.repo-name/pull/123"
        )
        assert owner == "my-org"
        assert repo == "my.repo-name"
        assert number == "123"

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError, match="Cannot parse PR URL"):
            _parse_pr_url("https://example.com/not-a-pr")

    def test_missing_number_raises(self):
        with pytest.raises(ValueError):
            _parse_pr_url("https://github.com/owner/repo/pull/")


# ---------------------------------------------------------------------------
# Prompt content smoke tests
# ---------------------------------------------------------------------------


class TestPromptContent:
    """Verify that review and fix prompts contain expected substrings."""

    @patch("botfarm.worker.run_claude")
    def test_review_prompt_contains_inline_comment_api(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            cost_usd=0.0, exit_subtype="", result_text="done",
        )
        _run_review(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "gh api repos/owner/repo/pulls/42/comments" in prompt
        assert "inline" in prompt
        assert "{{" not in prompt

    @patch("botfarm.worker.run_claude")
    def test_review_prompt_contains_verdict_instruction(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            cost_usd=0.0, exit_subtype="", result_text="done",
        )
        _run_review(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "VERDICT: APPROVED" in prompt
        assert "VERDICT: CHANGES_REQUESTED" in prompt

    @patch("botfarm.worker.run_claude")
    def test_review_prompt_requires_changes_requested_for_inline_comments(
        self, mock_claude, tmp_path,
    ):
        """Review prompt instructs agent to use CHANGES_REQUESTED when inline comments exist."""
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            cost_usd=0.0, exit_subtype="", result_text="done",
        )
        _run_review(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "ZERO actionable inline comments" in prompt
        assert "MUST use --request-changes" in prompt

    @patch("botfarm.worker.run_claude")
    def test_fix_prompt_contains_inline_comment_api(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            cost_usd=0.0, exit_subtype="", result_text="done",
        )
        _run_fix(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "gh api repos/owner/repo/pulls/42/comments" in prompt
        assert "{{" not in prompt

    @patch("botfarm.worker.run_claude")
    def test_fix_prompt_instructs_reply_to_comments(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            cost_usd=0.0, exit_subtype="", result_text="done",
        )
        _run_fix(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "comments/COMMENT_ID/replies" in prompt
        assert "Fixed" in prompt
