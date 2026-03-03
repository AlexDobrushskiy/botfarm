"""Tests for botfarm.worker — pipeline stages, metrics capture, and orchestration."""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.db import get_stage_runs, get_task, insert_stage_run, insert_task
from botfarm.slots import SlotManager
from botfarm.config import CoderIdentity, IdentitiesConfig, ReviewerIdentity
from botfarm.worker import (
    STAGES,
    ClaudeResult,
    PipelineResult,
    StageResult,
    _CODER_STAGES,
    DEFAULT_CONTEXT_WINDOW,
    _PipelineContext,
    _REVIEWER_STAGES,
    _build_implement_prompt,
    _compute_turn_context_fill,
    _detect_no_pr_needed,
    _is_investigation,
    build_coder_env,
    build_git_env,
    build_reviewer_env,
    parse_stream_json_result,
    parse_claude_output,
    run_claude_streaming,
    run_pipeline,
    _check_pr_merged,
    _compute_context_fill,
    _extract_pr_url,
    is_protected_branch,
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
    _is_merge_conflict,
    _write_subprocess_log,
)
from tests.helpers import make_claude_json


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
        raw = make_claude_json()
        result = parse_claude_output(raw)
        assert result.session_id == "sess-abc"
        assert result.num_turns == 5
        assert result.duration_seconds == 12.0
        assert result.exit_subtype == "tool_use"
        assert result.result_text == "Done"
        assert result.is_error is False

    def test_is_error_true(self):
        raw = make_claude_json(is_error=True)
        result = parse_claude_output(raw)
        assert result.is_error is True

    def test_duration_conversion(self):
        raw = make_claude_json(duration_ms=60000)
        result = parse_claude_output(raw)
        assert result.duration_seconds == 60.0

    def test_missing_fields_use_defaults(self):
        raw = json.dumps({"result": "ok"})
        result = parse_claude_output(raw)
        assert result.session_id == ""
        assert result.num_turns == 0
        assert result.duration_seconds == 0.0
        assert result.exit_subtype == ""
        assert result.result_text == "ok"

    def test_invalid_json_raises(self):
        with pytest.raises(json.JSONDecodeError):
            parse_claude_output("not json")

    def test_token_usage_fields(self):
        raw = json.dumps({
            "session_id": "sess-tok",
            "num_turns": 3,
            "duration_ms": 5000,
            "subtype": "tool_use",
            "result": "ok",
            "is_error": False,
            "total_cost_usd": 0.42,
            "usage": {
                "input_tokens": 1000,
                "output_tokens": 500,
                "cache_read_input_tokens": 200,
                "cache_creation_input_tokens": 300,
            },
            "modelUsage": {
                "claude-sonnet-4-6": {
                    "inputTokens": 1000,
                    "outputTokens": 500,
                    "cacheReadInputTokens": 200,
                    "cacheCreationInputTokens": 300,
                    "contextWindow": 200000,
                    "maxOutputTokens": 32000,
                    "costUSD": 0.42,
                }
            },
        })
        result = parse_claude_output(raw)
        assert result.input_tokens == 1000
        assert result.output_tokens == 500
        assert result.cache_read_input_tokens == 200
        assert result.cache_creation_input_tokens == 300
        assert result.total_cost_usd == pytest.approx(0.42)
        # context_fill = (1000 + 300 + 500) / 200000 * 100 = 0.9%
        assert result.context_fill_pct == pytest.approx(0.9)
        assert result.model_usage_json is not None
        parsed = json.loads(result.model_usage_json)
        assert "claude-sonnet-4-6" in parsed

    def test_token_usage_defaults_when_absent(self):
        raw = make_claude_json()
        result = parse_claude_output(raw)
        assert result.input_tokens == 0
        assert result.output_tokens == 0
        assert result.cache_read_input_tokens == 0
        assert result.cache_creation_input_tokens == 0
        assert result.total_cost_usd == 0.0
        assert result.context_fill_pct is None
        assert result.model_usage_json is None

    def test_context_fill_ignores_cache_reads(self):
        """cache_read_input_tokens should NOT count toward context fill."""
        raw = json.dumps({
            "result": "ok",
            "usage": {
                "input_tokens": 50000,
                "output_tokens": 10000,
                "cache_read_input_tokens": 100000,
                "cache_creation_input_tokens": 20000,
            },
            "modelUsage": {
                "claude-opus-4-6": {
                    "contextWindow": 200000,
                }
            },
        })
        result = parse_claude_output(raw)
        # unique = 50000 + 20000 + 10000 = 80000
        # fill = 80000 / 200000 * 100 = 40.0
        assert result.context_fill_pct == pytest.approx(40.0)

    def test_context_fill_none_without_model_usage(self):
        raw = json.dumps({
            "result": "ok",
            "usage": {"input_tokens": 100, "output_tokens": 50},
        })
        result = parse_claude_output(raw)
        assert result.context_fill_pct is None


class TestComputeContextFill:
    def test_basic_calculation(self):
        model_usage = {"model-a": {"contextWindow": 100000}}
        pct = _compute_context_fill(5000, 2000, 3000, model_usage)
        # (5000 + 2000 + 3000) / 100000 * 100 = 10.0
        assert pct == pytest.approx(10.0)

    def test_returns_none_without_model_usage(self):
        assert _compute_context_fill(100, 50, 50, None) is None
        assert _compute_context_fill(100, 50, 50, {}) is None

    def test_returns_none_without_context_window(self):
        model_usage = {"model-a": {"maxOutputTokens": 32000}}
        assert _compute_context_fill(100, 50, 50, model_usage) is None

    def test_uses_first_model_with_context_window(self):
        model_usage = {
            "model-a": {"maxOutputTokens": 32000},
            "model-b": {"contextWindow": 200000},
        }
        pct = _compute_context_fill(10000, 5000, 5000, model_usage)
        # (10000 + 5000 + 5000) / 200000 * 100 = 10.0
        assert pct == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# _compute_turn_context_fill / parse_stream_json_result / run_claude_streaming
# ---------------------------------------------------------------------------


class TestComputeTurnContextFill:
    def test_basic_calculation(self):
        usage = {
            "input_tokens": 1000,
            "cache_creation_input_tokens": 500,
            "cache_read_input_tokens": 8000,
            "output_tokens": 500,
        }
        pct = _compute_turn_context_fill(usage, 200_000)
        # (1000 + 500 + 8000 + 500) / 200000 * 100 = 5.0
        assert pct == pytest.approx(5.0)

    def test_includes_cache_read(self):
        """Per-turn formula includes cache_read unlike cumulative formula."""
        usage = {
            "input_tokens": 100,
            "cache_creation_input_tokens": 0,
            "cache_read_input_tokens": 50000,
            "output_tokens": 100,
        }
        pct = _compute_turn_context_fill(usage, 200_000)
        # (100 + 0 + 50000 + 100) / 200000 * 100 = 25.1
        assert pct == pytest.approx(25.1)

    def test_zero_context_window_returns_none(self):
        usage = {"input_tokens": 100, "output_tokens": 50}
        assert _compute_turn_context_fill(usage, 0) is None

    def test_empty_usage_returns_none(self):
        assert _compute_turn_context_fill({}, 200_000) is None

    def test_missing_keys_treated_as_zero(self):
        usage = {"input_tokens": 10000, "output_tokens": 5000}
        pct = _compute_turn_context_fill(usage, 200_000)
        # (10000 + 0 + 0 + 5000) / 200000 * 100 = 7.5
        assert pct == pytest.approx(7.5)


class TestParseStreamResult:
    def test_basic_parse(self):
        data = {
            "session_id": "sess-stream",
            "num_turns": 10,
            "duration_ms": 30000,
            "subtype": "end_turn",
            "result": "All done",
            "is_error": False,
            "usage": {
                "input_tokens": 5000,
                "output_tokens": 2000,
                "cache_read_input_tokens": 1000,
                "cache_creation_input_tokens": 500,
            },
            "total_cost_usd": 0.15,
            "modelUsage": {
                "claude-opus-4-6": {
                    "contextWindow": 200000,
                    "inputTokens": 5000,
                    "outputTokens": 2000,
                }
            },
        }
        result = parse_stream_json_result(data)
        assert result.session_id == "sess-stream"
        assert result.num_turns == 10
        assert result.duration_seconds == 30.0
        assert result.result_text == "All done"
        assert result.input_tokens == 5000
        assert result.output_tokens == 2000
        assert result.cache_read_input_tokens == 1000
        assert result.cache_creation_input_tokens == 500
        assert result.total_cost_usd == 0.15
        assert result.context_fill_pct is not None
        assert result.model_usage_json is not None

    def test_missing_usage_defaults_to_zero(self):
        data = {
            "session_id": "sess-1",
            "result": "ok",
        }
        result = parse_stream_json_result(data)
        assert result.input_tokens == 0
        assert result.output_tokens == 0
        assert result.context_fill_pct is None


def _make_stream_ndjson(
    *,
    session_id="sess-stream",
    num_turns=3,
    result_text="Done streaming",
    turns_usage=None,
) -> str:
    """Build NDJSON output simulating stream-json mode."""
    lines = []
    # init message
    lines.append(json.dumps({
        "type": "system",
        "subtype": "init",
        "session_id": session_id,
    }))

    # assistant turns with usage
    if turns_usage is None:
        turns_usage = [
            {"input_tokens": 1000, "cache_creation_input_tokens": 500,
             "cache_read_input_tokens": 30000, "output_tokens": 200},
            {"input_tokens": 1, "cache_creation_input_tokens": 100,
             "cache_read_input_tokens": 31700, "output_tokens": 300},
            {"input_tokens": 1, "cache_creation_input_tokens": 50,
             "cache_read_input_tokens": 32100, "output_tokens": 150},
        ]
    for usage in turns_usage:
        lines.append(json.dumps({
            "type": "assistant",
            "message": {"usage": usage},
        }))

    # result message
    lines.append(json.dumps({
        "type": "result",
        "session_id": session_id,
        "num_turns": num_turns,
        "duration_ms": 15000,
        "subtype": "end_turn",
        "result": result_text,
        "is_error": False,
        "usage": {
            "input_tokens": 1002,
            "output_tokens": 650,
            "cache_read_input_tokens": 93800,
            "cache_creation_input_tokens": 650,
        },
        "total_cost_usd": 0.10,
        "modelUsage": {
            "claude-opus-4-6": {
                "contextWindow": 200000,
                "inputTokens": 1002,
                "outputTokens": 650,
                "cacheReadInputTokens": 93800,
                "cacheCreationInputTokens": 650,
            }
        },
    }))
    return "\n".join(lines) + "\n"


class TestRunClaudeStreaming:
    @patch("botfarm.worker.subprocess.Popen")
    def test_success_parses_result(self, mock_popen, tmp_path):
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        result = run_claude_streaming("do stuff", cwd=tmp_path, max_turns=10)
        assert result.session_id == "sess-stream"
        assert result.num_turns == 3
        assert result.result_text == "Done streaming"
        assert result.context_fill_pct is not None

        # Verify command flags
        cmd = mock_popen.call_args[0][0]
        assert "stream-json" in cmd
        assert "--verbose" in cmd

    @patch("botfarm.worker.subprocess.Popen")
    def test_start_new_session_enabled(self, mock_popen, tmp_path):
        """Popen is called with start_new_session=True for process group control."""
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        run_claude_streaming("do stuff", cwd=tmp_path, max_turns=10)
        assert mock_popen.call_args.kwargs["start_new_session"] is True

    @patch("botfarm.worker.subprocess.Popen")
    def test_invokes_context_fill_callback(self, mock_popen, tmp_path):
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        callback_calls = []

        def on_fill(turn, pct):
            callback_calls.append((turn, pct))

        run_claude_streaming(
            "do stuff", cwd=tmp_path, max_turns=10,
            on_context_fill=on_fill,
        )
        # 3 assistant turns → 3 callback invocations
        assert len(callback_calls) == 3
        assert callback_calls[0][0] == 1  # turn 1
        assert callback_calls[1][0] == 2  # turn 2
        assert callback_calls[2][0] == 3  # turn 3
        # All fill percentages should be positive
        for turn, pct in callback_calls:
            assert pct > 0

    @patch("botfarm.worker.subprocess.Popen")
    def test_nonzero_exit_raises(self, mock_popen, tmp_path):
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter([])
        mock_proc.stderr = iter(["error\n"])
        mock_proc.returncode = 1
        mock_proc.wait.return_value = 1
        mock_popen.return_value = mock_proc

        with pytest.raises(subprocess.CalledProcessError):
            run_claude_streaming("do stuff", cwd=tmp_path, max_turns=10)

    @patch("botfarm.worker.subprocess.Popen")
    def test_no_result_message_raises(self, mock_popen, tmp_path):
        """If stream ends without a result message, raises RuntimeError."""
        ndjson = json.dumps({"type": "system", "subtype": "init"}) + "\n"
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        with pytest.raises(RuntimeError, match="no result message"):
            run_claude_streaming("do stuff", cwd=tmp_path, max_turns=10)

    @patch("botfarm.worker.subprocess.Popen")
    def test_writes_log_file(self, mock_popen, tmp_path):
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter(["some warning\n"])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        log_file = tmp_path / "logs" / "stream.log"
        run_claude_streaming(
            "do stuff", cwd=tmp_path, max_turns=10, log_file=log_file,
        )
        assert log_file.exists()
        content = log_file.read_text()
        assert "sess-stream" in content
        assert "some warning" in content

    @patch("botfarm.worker.subprocess.Popen")
    def test_callback_exception_does_not_crash(self, mock_popen, tmp_path):
        """Callback errors are logged but don't stop the stream."""
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        def bad_callback(turn, pct):
            raise RuntimeError("DB write failed")

        # Should not raise
        result = run_claude_streaming(
            "do stuff", cwd=tmp_path, max_turns=10,
            on_context_fill=bad_callback,
        )
        assert result.session_id == "sess-stream"

    @patch("botfarm.worker.subprocess.Popen")
    def test_env_passed_to_subprocess(self, mock_popen, tmp_path):
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        env = {"BOTFARM_DB_PATH": "/tmp/slot.db"}
        run_claude_streaming("do stuff", cwd=tmp_path, max_turns=10, env=env)

        call_kwargs = mock_popen.call_args.kwargs
        assert call_kwargs["env"]["BOTFARM_DB_PATH"] == "/tmp/slot.db"

    @patch("botfarm.worker.os.killpg")
    @patch("botfarm.worker.os.getpgid", return_value=12345)
    @patch("botfarm.worker.subprocess.Popen")
    def test_timeout_kills_process(self, mock_popen, mock_getpgid, mock_killpg, tmp_path):
        """When the process exceeds the timeout, the process group is killed and TimeoutError is raised."""
        import threading

        # Simulate a process that blocks forever on stdout
        block = threading.Event()
        ndjson = _make_stream_ndjson()
        lines = ndjson.splitlines(keepends=True)

        def slow_iter():
            yield lines[0]  # emit the init line
            block.wait(timeout=10)  # block until killed

        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = slow_iter()
        mock_proc.stderr = iter([])
        mock_proc.pid = 12345
        mock_proc.returncode = -15  # killed by SIGTERM
        mock_proc.wait.return_value = -15
        mock_proc.poll.return_value = None  # process still running
        # When killpg is called, unblock the iterator
        mock_killpg.side_effect = lambda *_args: block.set()
        mock_popen.return_value = mock_proc

        with pytest.raises(TimeoutError, match="timed out after 0.1s"):
            run_claude_streaming(
                "do stuff", cwd=tmp_path, max_turns=10, timeout=0.1,
            )
        mock_killpg.assert_called()

    @patch("botfarm.worker.subprocess.Popen")
    def test_no_timeout_when_process_completes(self, mock_popen, tmp_path):
        """Normal completion with a timeout set should not raise."""
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        result = run_claude_streaming(
            "do stuff", cwd=tmp_path, max_turns=10, timeout=30,
        )
        assert result.session_id == "sess-stream"
        mock_proc.kill.assert_not_called()

    @patch("botfarm.worker.subprocess.Popen")
    def test_realtime_log_writing(self, mock_popen, tmp_path):
        """Log file is written line-by-line during streaming, not post-hoc."""
        ndjson = _make_stream_ndjson()
        lines_written = []

        # Track lines as they arrive by intercepting stdout iteration
        real_lines = ndjson.splitlines(keepends=True)

        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(real_lines)
        mock_proc.stderr = iter(["some warning\n"])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        log_file = tmp_path / "logs" / "stream.log"
        run_claude_streaming(
            "do stuff", cwd=tmp_path, max_turns=10, log_file=log_file,
        )

        assert log_file.exists()
        content = log_file.read_text()
        # All NDJSON lines should be present
        assert "sess-stream" in content
        # Stderr should be appended
        assert "--- STDERR ---" in content
        assert "some warning" in content

    @patch("botfarm.worker.subprocess.Popen")
    def test_realtime_log_no_stderr_section_when_empty(self, mock_popen, tmp_path):
        """No STDERR section in log when stderr is empty."""
        ndjson = _make_stream_ndjson()
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter(ndjson.splitlines(keepends=True))
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        log_file = tmp_path / "stream.log"
        run_claude_streaming(
            "do stuff", cwd=tmp_path, max_turns=10, log_file=log_file,
        )

        content = log_file.read_text()
        assert "sess-stream" in content
        assert "STDERR" not in content

    @patch("botfarm.worker.os.killpg")
    @patch("botfarm.worker.os.getpgid", return_value=12345)
    @patch("botfarm.worker.subprocess.Popen")
    def test_timeout_with_log_file(self, mock_popen, mock_getpgid, mock_killpg, tmp_path):
        """Timeout still writes partial log and closes the file handle."""
        import threading

        block = threading.Event()
        ndjson = _make_stream_ndjson()
        lines = ndjson.splitlines(keepends=True)

        def slow_iter():
            yield lines[0]  # emit init line
            block.wait(timeout=10)

        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = slow_iter()
        mock_proc.stderr = iter([])
        mock_proc.pid = 12345
        mock_proc.returncode = -15
        mock_proc.wait.return_value = -15
        mock_proc.poll.return_value = None
        mock_killpg.side_effect = lambda *_args: block.set()
        mock_popen.return_value = mock_proc

        log_file = tmp_path / "timeout.log"
        with pytest.raises(TimeoutError):
            run_claude_streaming(
                "do stuff", cwd=tmp_path, max_turns=10,
                log_file=log_file, timeout=0.1,
            )

        # Partial log should exist with at least the init line
        assert log_file.exists()
        content = log_file.read_text()
        assert "system" in content or "init" in content

    @patch("botfarm.worker.subprocess.Popen")
    def test_stdout_loop_exits_after_result(self, mock_popen, tmp_path):
        """After receiving a result message, the stdout loop breaks immediately
        without waiting for EOF — the fix for MCP server children holding the pipe."""
        import threading

        ndjson = _make_stream_ndjson()
        lines = ndjson.splitlines(keepends=True)
        lines_read = []

        # Append a line that blocks forever *after* the result message.
        # If the loop doesn't break on result, this will hang.
        block = threading.Event()

        def iter_with_trailing_block():
            for line in lines:
                lines_read.append(line)
                yield line
            # Simulate MCP server children holding stdout open after result
            block.wait(timeout=5)
            yield '{"type": "trailing_garbage"}\n'

        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.stdout = iter_with_trailing_block()
        mock_proc.stderr = iter([])
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        mock_popen.return_value = mock_proc

        result = run_claude_streaming("do stuff", cwd=tmp_path, max_turns=10)

        assert result.session_id == "sess-stream"
        assert result.result_text == "Done streaming"
        # The trailing block line should NOT have been read
        block.set()  # unblock to allow cleanup


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
    @patch("botfarm.worker.run_claude_streaming")
    def test_success_with_pr_url(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1",
            num_turns=10,
            duration_seconds=30.0,
            exit_subtype="tool_use",
            result_text=f"Created PR: {PR_URL}",
        )
        result = _run_implement("SMA-1", cwd=tmp_path, max_turns=100)
        assert result.success is True
        assert result.stage == "implement"
        assert result.pr_url == PR_URL
        assert result.claude_result.num_turns == 10

    @patch("botfarm.worker.run_claude_streaming")
    def test_success_without_pr_url(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1",
            num_turns=10,
            duration_seconds=30.0,
            exit_subtype="tool_use",
            result_text="Done but no PR link",
        )
        result = _run_implement("SMA-1", cwd=tmp_path, max_turns=100)
        assert result.success is True
        assert result.pr_url is None

    @patch("botfarm.worker.run_claude_streaming")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1",
            num_turns=10,
            duration_seconds=30.0,
            exit_subtype="tool_use",
            result_text="Max turns reached",
            is_error=True,
        )
        result = _run_implement("SMA-1", cwd=tmp_path, max_turns=100)
        assert result.success is False
        assert "Claude reported error" in result.error


class TestRunReview:
    @patch("botfarm.worker.run_claude_streaming")
    def test_success(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s2",
            num_turns=5,
            duration_seconds=15.0,
            exit_subtype="tool_use",
            result_text="Review posted",
        )
        result = _run_review(PR_URL, cwd=tmp_path, max_turns=50)
        assert result.success is True
        assert result.stage == "review"

    @patch("botfarm.worker.run_claude_streaming")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s2",
            num_turns=5,
            duration_seconds=15.0,
            exit_subtype="tool_use",
            result_text="Error occurred",
            is_error=True,
        )
        result = _run_review(PR_URL, cwd=tmp_path, max_turns=50)
        assert result.success is False
        assert "Claude reported error" in result.error


class TestRunFix:
    @patch("botfarm.worker.run_claude_streaming")
    def test_success(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s3",
            num_turns=8,
            duration_seconds=20.0,
            exit_subtype="tool_use",
            result_text="Fixes pushed",
        )
        result = _run_fix(PR_URL, cwd=tmp_path, max_turns=50)
        assert result.success is True
        assert result.stage == "fix"

    @patch("botfarm.worker.run_claude_streaming")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s3",
            num_turns=8,
            duration_seconds=20.0,
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
        mock_check.assert_called_once_with(PR_URL, tmp_path, env=None)

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
    def test_merge_does_not_use_delete_branch_flag(self, mock_run, tmp_path):
        """Merge command must not include --delete-branch (handled by GitHub repo setting)."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="Merged", stderr=""
        )
        _run_merge(PR_URL, cwd=tmp_path)
        # Second call is gh pr merge (first is git rev-parse)
        merge_call_args = mock_run.call_args_list[1][0][0]
        assert "--delete-branch" not in merge_call_args

    @patch("botfarm.worker.subprocess.run")
    def test_merge_deletes_local_feature_branch(self, mock_run, tmp_path):
        """After merge and checkout, the local feature branch is deleted."""
        def side_effect(cmd, **kwargs):
            if cmd[:2] == ["git", "rev-parse"]:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="feat/my-feature\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok", stderr=""
            )
        mock_run.side_effect = side_effect
        _run_merge(PR_URL, cwd=tmp_path, placeholder_branch="slot-1-placeholder")
        # 4 calls: git rev-parse, gh pr merge, git checkout, git branch -D
        assert mock_run.call_count == 4
        delete_call = mock_run.call_args_list[3][0][0]
        assert delete_call == ["git", "branch", "-D", "feat/my-feature"]

    @patch("botfarm.worker.subprocess.run")
    def test_merge_skips_branch_delete_when_rev_parse_fails(self, mock_run, tmp_path):
        """When current branch cannot be determined, no branch deletion is attempted."""
        def side_effect(cmd, **kwargs):
            if cmd[:2] == ["git", "rev-parse"]:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=1, stdout="", stderr="not a git repo"
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok", stderr=""
            )
        mock_run.side_effect = side_effect
        result = _run_merge(PR_URL, cwd=tmp_path, placeholder_branch="slot-1-placeholder")
        assert result.success is True
        # 3 calls: git rev-parse (fail), gh pr merge, git checkout (no branch -D)
        assert mock_run.call_count == 3

    @patch("botfarm.worker.subprocess.run")
    def test_merge_branch_delete_failure_does_not_block_pipeline(self, mock_run, tmp_path):
        """Branch deletion failure logs a warning but returns success."""
        def side_effect(cmd, **kwargs):
            if cmd[:2] == ["git", "rev-parse"]:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="feat/my-feature\n", stderr=""
                )
            if cmd[0] == "git" and "branch" in cmd and "-D" in cmd:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=1, stdout="", stderr="error: branch not found"
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok", stderr=""
            )
        mock_run.side_effect = side_effect
        result = _run_merge(
            PR_URL, cwd=tmp_path,
            placeholder_branch="slot-1-placeholder",
        )
        assert result.success is True

    @patch("botfarm.worker.subprocess.run")
    def test_merge_refuses_to_delete_protected_branch(self, mock_run, tmp_path):
        """Protected branches (main, slot-*-placeholder) must never be deleted."""
        def side_effect(cmd, **kwargs):
            if cmd[:2] == ["git", "rev-parse"]:
                return subprocess.CompletedProcess(
                    args=cmd, returncode=0, stdout="main\n", stderr=""
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="ok", stderr=""
            )
        mock_run.side_effect = side_effect
        _run_merge(PR_URL, cwd=tmp_path, placeholder_branch="slot-1-placeholder")
        # 3 calls: git rev-parse, gh pr merge, git checkout (no branch -D for protected)
        assert mock_run.call_count == 3


class TestIsProtectedBranch:
    def test_main_is_protected(self):
        assert is_protected_branch("main") is True

    def test_slot_placeholder_is_protected(self):
        assert is_protected_branch("slot-1-placeholder") is True
        assert is_protected_branch("slot-42-placeholder") is True

    def test_feature_branch_is_not_protected(self):
        assert is_protected_branch("feat/my-feature") is False
        assert is_protected_branch("adobrushskiy/sma-141-fix") is False

    def test_similar_but_not_matching_patterns(self):
        assert is_protected_branch("slot-placeholder") is False
        assert is_protected_branch("slot-1-placeholders") is False
        assert is_protected_branch("main-backup") is False


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


def _mock_stage_result(stage, success=True, pr_url=None, turns=3, review_approved=None):
    """Build a StageResult with optional ClaudeResult."""
    cr = None
    if stage in ("implement", "review", "fix", "resolve_conflict"):
        cr = ClaudeResult(
            session_id=f"sess-{stage}",
            num_turns=turns,
            duration_seconds=10.0,
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
            _mock_stage_result("implement", pr_url=PR_URL, turns=10),
            _mock_stage_result("review", turns=5),
            _mock_stage_result("fix", turns=8),
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
        assert result.total_turns == 23
        assert result.failure_stage is None

        # Check DB: 5 stage runs recorded
        runs = get_stage_runs(conn, task_id)
        assert len(runs) == 5
        assert [r["stage"] for r in runs] == list(STAGES)

        # Task should be marked completed with timestamp
        task = get_task(conn, task_id)
        assert task["status"] == "completed"
        assert task["turns"] == 23
        assert task["completed_at"] is not None

    @patch("botfarm.worker._execute_stage")
    def test_pr_url_persisted_to_db(self, mock_exec, conn, task_id, tmp_path):
        """PR URL from implement stage is written to tasks DB."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, turns=10),
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
    def test_investigation_short_circuits_after_implement(self, mock_exec, conn, task_id, tmp_path):
        """Investigation tickets succeed after implement with no PR URL."""
        mock_exec.return_value = _mock_stage_result("implement", pr_url=None)
        result = run_pipeline(
            ticket_id="SMA-99",
            ticket_labels=["Investigation"],
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is True
        assert result.stages_completed == ["implement"]
        assert result.pr_url is None

        task = get_task(conn, task_id)
        assert task["status"] == "completed"
        assert task["completed_at"] is not None

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
        """Turns and duration accumulate across stages."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, turns=20),
            _mock_stage_result("review", turns=10),
            _mock_stage_result("fix", turns=5),
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
            _mock_stage_result("implement", pr_url=PR_URL, turns=10),
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
            _mock_stage_result("implement", pr_url=PR_URL, turns=10),
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
# run_pipeline — streaming context fill integration
# ---------------------------------------------------------------------------


class TestRunPipelineStreamingContextFill:
    @patch("botfarm.worker._execute_stage")
    def test_claude_stages_create_placeholder_and_update(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """Claude-based stages insert a placeholder stage_run before execution,
        then update it with final metrics."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, turns=10),
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

        runs = get_stage_runs(conn, task_id)
        # implement + review (approved, no fix) + pr_checks + merge = 4
        assert len(runs) == 4
        stages = [r["stage"] for r in runs]
        assert "implement" in stages
        assert "review" in stages

        # Claude stages should have their data updated from the ClaudeResult
        implement_run = [r for r in runs if r["stage"] == "implement"][0]
        assert implement_run["turns"] == 10
        assert implement_run["session_id"] == "sess-implement"

    @patch("botfarm.worker._execute_stage")
    def test_on_context_fill_passed_to_execute_stage(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """_execute_stage receives on_context_fill for Claude-based stages."""
        received_callbacks = []

        def capture_execute(stage, **kwargs):
            cb = kwargs.get("on_context_fill")
            received_callbacks.append((stage, cb is not None))
            return _mock_stage_result(
                stage,
                pr_url=PR_URL if stage == "implement" else None,
                review_approved=True if stage == "review" else None,
            )

        mock_exec.side_effect = capture_execute
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
        )

        # Claude stages should have a callback, others should not
        for stage, has_cb in received_callbacks:
            if stage in ("implement", "review", "fix"):
                assert has_cb, f"{stage} should have on_context_fill"
            else:
                assert not has_cb, f"{stage} should NOT have on_context_fill"

    @patch("botfarm.worker._execute_stage")
    def test_log_file_path_recorded_in_stage_runs(
        self, mock_exec, conn, task_id, tmp_path,
    ):
        """Stage runs record the log file path in the database."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, turns=5),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            log_dir=log_dir,
        )
        assert result.success is True

        runs = get_stage_runs(conn, task_id)
        # Claude-based stages (implement, review) should have log_file_path set
        # in both placeholder insert and final record
        for run in runs:
            if run["stage"] in ("implement", "review"):
                assert run["log_file_path"] is not None, (
                    f"{run['stage']} should have log_file_path"
                )
                assert str(log_dir) in run["log_file_path"]
            elif run["stage"] in ("pr_checks", "merge"):
                # Non-Claude stages record log_file_path in _record_stage_run
                assert run["log_file_path"] is not None, (
                    f"{run['stage']} should have log_file_path"
                )


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
# Pipeline pause_event
# ---------------------------------------------------------------------------


PR_URL_PAUSE = "https://github.com/owner/repo/pull/42"


class TestRunPipelinePauseEvent:
    @patch("botfarm.worker._execute_stage")
    def test_pause_event_stops_pipeline_between_stages(self, mock_exec, conn, task_id, tmp_path):
        """When pause_event is set, pipeline exits with paused=True between stages."""
        import multiprocessing as mp

        pause_evt = mp.Event()

        call_count = 0

        def _side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            stage = args[0] if args else kwargs.get("stage", "implement")
            # After implement completes, set the pause event
            if call_count == 1:
                pause_evt.set()
            return _mock_stage_result(stage, pr_url=PR_URL_PAUSE if call_count == 1 else None)

        mock_exec.side_effect = _side_effect

        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            pause_event=pause_evt,
        )
        assert result.paused is True
        assert result.success is False
        assert "implement" in result.stages_completed
        # Should have stopped before running review
        assert call_count == 1

    @patch("botfarm.worker._execute_stage")
    def test_unset_pause_event_does_not_stop(self, mock_exec, conn, task_id, tmp_path):
        """When pause_event exists but is NOT set, pipeline proceeds normally."""
        import multiprocessing as mp

        pause_evt = mp.Event()  # not set

        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL_PAUSE, turns=10),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]

        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            pause_event=pause_evt,
        )
        assert result.success is True
        assert result.paused is False

    @patch("botfarm.worker._execute_stage")
    def test_no_pause_event_pipeline_proceeds(self, mock_exec, conn, task_id, tmp_path):
        """When pause_event is None, pipeline proceeds normally."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL_PAUSE, turns=10),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]

        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            pause_event=None,
        )
        assert result.success is True
        assert result.paused is False


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
        assert pr.total_turns == 0
        assert pr.failure_stage is None
        assert pr.paused is False

    def test_pipeline_result_paused(self):
        pr = PipelineResult(
            ticket_id="SMA-1", success=False, stages_completed=["implement"],
            paused=True,
        )
        assert pr.paused is True

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
            _mock_stage_result("review", review_approved=False, turns=5),
            _mock_stage_result("fix", turns=8),
            # Iteration 2: review approves
            _mock_stage_result("review", review_approved=True, turns=3),
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
        """Turns from all review/fix iterations are accumulated."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, turns=20),
            # Iteration 1
            _mock_stage_result("review", review_approved=False, turns=5),
            _mock_stage_result("fix", turns=8),
            # Iteration 2 — approved
            _mock_stage_result("review", review_approved=True, turns=3),
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
    @patch("botfarm.worker.run_claude_streaming")
    def test_success(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s-ci",
            num_turns=6,
            duration_seconds=18.0,
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

    @patch("botfarm.worker.run_claude_streaming")
    def test_is_error_returns_failure(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s-ci",
            num_turns=6,
            duration_seconds=18.0,
            exit_subtype="tool_use",
            result_text="Error occurred",
            is_error=True,
        )
        result = _run_ci_fix(
            PR_URL, ci_failure_output="build failed", cwd=tmp_path, max_turns=100,
        )
        assert result.success is False
        assert "Claude reported error" in result.error

    @patch("botfarm.worker.run_claude_streaming")
    def test_ci_output_truncated_at_2000_chars(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s-ci",
            num_turns=3,
            duration_seconds=10.0,
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
        mock_ci_fix.return_value = _mock_stage_result("fix", turns=8)
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
        """Turns from CI fix retries accumulate in pipeline metrics."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, turns=20),
            _mock_stage_result("review", review_approved=True, turns=5),
            _mock_stage_result("pr_checks", success=False),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        mock_ci_fix.return_value = _mock_stage_result("fix", turns=10)
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=3,
            max_ci_retries=2,
        )
        assert result.success is True
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
# Merge conflict detection + retry loop
# ---------------------------------------------------------------------------


class TestIsMergeConflict:
    """Unit tests for _is_merge_conflict()."""

    @patch("botfarm.worker.subprocess.run")
    def test_conflict_detected(self, mock_run):
        """Regex match + gh pr view confirms CONFLICTING → True."""
        mock_run.return_value = MagicMock(
            stdout="CONFLICTING\n", returncode=0,
        )
        assert _is_merge_conflict(
            "Pull request isn't mergeable", PR_URL, "/tmp",
        ) is True

    @patch("botfarm.worker.subprocess.run")
    def test_no_conflict_keyword(self, mock_run):
        """Error without conflict keywords → False (no gh call)."""
        assert _is_merge_conflict(
            "permission denied", PR_URL, "/tmp",
        ) is False
        mock_run.assert_not_called()

    @patch("botfarm.worker.subprocess.run")
    def test_conflict_keyword_but_mergeable(self, mock_run):
        """Regex matches but gh pr view says MERGEABLE → False."""
        mock_run.return_value = MagicMock(
            stdout="MERGEABLE\n", returncode=0,
        )
        assert _is_merge_conflict(
            "not mergeable right now", PR_URL, "/tmp",
        ) is False

    @patch("botfarm.worker.subprocess.run")
    def test_conflict_keyword_gh_timeout(self, mock_run):
        """Regex matches but gh pr view times out → True (fallback)."""
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="gh", timeout=30)
        assert _is_merge_conflict(
            "conflict detected", PR_URL, "/tmp",
        ) is True

    @patch("botfarm.worker.subprocess.run")
    def test_case_insensitive(self, mock_run):
        """Conflict keyword matching is case-insensitive."""
        mock_run.return_value = MagicMock(
            stdout="CONFLICTING\n", returncode=0,
        )
        assert _is_merge_conflict(
            "CONFLICT in file.py", PR_URL, "/tmp",
        ) is True


class TestMergeConflictLoop:
    """Integration tests for the merge conflict retry loop."""

    @patch("botfarm.worker._is_merge_conflict", return_value=True)
    @patch("botfarm.worker._execute_stage")
    def test_merge_conflict_resolved_first_retry(
        self, mock_exec, mock_conflict, conn, task_id, tmp_path,
    ):
        """Merge fails with conflict, resolve_conflict runs, re-review + merge succeeds."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            # Initial merge: fails with conflict
            _mock_stage_result("merge", success=False),
            # Conflict loop retry 1:
            _mock_stage_result("resolve_conflict"),  # resolve
            _mock_stage_result("review", review_approved=True),  # re-review
            _mock_stage_result("pr_checks"),  # re-check CI
            _mock_stage_result("merge"),  # re-merge succeeds
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
            max_ci_retries=0,
            max_merge_conflict_retries=2,
        )
        assert result.success is True
        assert "merge" in result.stages_completed

    @patch("botfarm.worker._is_merge_conflict", return_value=True)
    @patch("botfarm.worker._execute_stage")
    def test_merge_conflict_retries_exhausted(
        self, mock_exec, mock_conflict, conn, task_id, tmp_path,
    ):
        """Max merge conflict retries exhausted → pipeline fails."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            # Initial merge: fails
            _mock_stage_result("merge", success=False),
            # Retry 1: resolve, review, checks, merge fails again
            _mock_stage_result("resolve_conflict"),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
            max_ci_retries=0,
            max_merge_conflict_retries=1,
        )
        assert result.success is False
        assert result.failure_stage == "merge"
        assert "retries" in (result.failure_reason or "")

    @patch("botfarm.worker._is_merge_conflict", return_value=False)
    @patch("botfarm.worker._execute_stage")
    def test_non_conflict_merge_failure_bails(
        self, mock_exec, mock_conflict, conn, task_id, tmp_path,
    ):
        """Merge fails with non-conflict error → immediate failure, no retry."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
            max_ci_retries=0,
            max_merge_conflict_retries=2,
        )
        assert result.success is False
        assert result.failure_stage == "merge"
        # Should not have called resolve_conflict
        stages_called = [c.kwargs.get("stage", c.args[0] if c.args else None)
                         for c in mock_exec.call_args_list]
        assert "resolve_conflict" not in stages_called

    @patch("botfarm.worker._is_merge_conflict", return_value=True)
    @patch("botfarm.worker._execute_stage")
    def test_merge_conflict_retries_zero_disables(
        self, mock_exec, mock_conflict, conn, task_id, tmp_path,
    ):
        """With max_merge_conflict_retries=0, conflict is immediate failure."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge", success=False),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
            max_ci_retries=0,
            max_merge_conflict_retries=0,
        )
        assert result.success is False
        assert result.failure_stage == "merge"

    @patch("botfarm.worker._is_merge_conflict", return_value=True)
    @patch("botfarm.worker._execute_stage")
    def test_merge_conflict_events_logged(
        self, mock_exec, mock_conflict, conn, task_id, tmp_path,
    ):
        """Merge conflict retry events are logged."""
        from botfarm.db import get_events

        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge", success=False),
            # Retry 1 succeeds
            _mock_stage_result("resolve_conflict"),
            _mock_stage_result("review", review_approved=True),
            _mock_stage_result("pr_checks"),
            _mock_stage_result("merge"),
        ]
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
            max_ci_retries=0,
            max_merge_conflict_retries=2,
        )
        assert result.success is True
        events = get_events(conn, task_id=task_id)
        event_types = [e["event_type"] for e in events]
        assert "merge_conflict_retry_started" in event_types
        assert "merge_succeeded_after_conflict_retry" in event_types


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

    @patch("botfarm.worker.run_claude_streaming")
    def test_review_prompt_contains_inline_comment_api(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="", result_text="done",
        )
        _run_review(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "gh api repos/owner/repo/pulls/42/comments" in prompt
        assert "inline" in prompt
        assert "{{" not in prompt

    @patch("botfarm.worker.run_claude_streaming")
    def test_review_prompt_contains_verdict_instruction(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="", result_text="done",
        )
        _run_review(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "VERDICT: APPROVED" in prompt
        assert "VERDICT: CHANGES_REQUESTED" in prompt

    @patch("botfarm.worker.run_claude_streaming")
    def test_review_prompt_requires_changes_requested_for_inline_comments(
        self, mock_claude, tmp_path,
    ):
        """Review prompt instructs agent to use CHANGES_REQUESTED when inline comments exist."""
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="", result_text="done",
        )
        _run_review(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "ZERO actionable inline comments" in prompt
        assert "MUST use --request-changes" in prompt

    @patch("botfarm.worker.run_claude_streaming")
    def test_fix_prompt_contains_inline_comment_api(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="", result_text="done",
        )
        _run_fix(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "gh api repos/owner/repo/pulls/42/comments" in prompt
        assert "{{" not in prompt

    @patch("botfarm.worker.run_claude_streaming")
    def test_fix_prompt_instructs_reply_to_comments(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="", result_text="done",
        )
        _run_fix(PR_URL, cwd=tmp_path, max_turns=10)
        prompt = mock_claude.call_args.args[0]
        assert "comments/COMMENT_ID/replies" in prompt
        assert "Fixed" in prompt

    @patch("botfarm.worker.run_claude_streaming")
    def test_fix_prompt_multi_reviewer(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="", result_text="done",
        )
        _run_fix(PR_URL, cwd=tmp_path, max_turns=10, codex_enabled=True)
        prompt = mock_claude.call_args.args[0]
        assert "multiple reviewers" in prompt
        assert "CODEX: " in prompt
        assert "Address all comments regardless of source" in prompt

    @patch("botfarm.worker.run_claude_streaming")
    def test_fix_prompt_single_reviewer(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="", result_text="done",
        )
        _run_fix(PR_URL, cwd=tmp_path, max_turns=10, codex_enabled=False)
        prompt = mock_claude.call_args.args[0]
        assert "multiple reviewers" not in prompt
        assert "CODEX: " not in prompt


# ---------------------------------------------------------------------------
# Investigation label detection and prompt selection
# ---------------------------------------------------------------------------


class TestIsInvestigation:
    def test_none_labels(self):
        assert _is_investigation(None) is False

    def test_empty_labels(self):
        assert _is_investigation([]) is False

    def test_no_investigation_label(self):
        assert _is_investigation(["Bug", "Feature"]) is False

    def test_investigation_label_exact(self):
        assert _is_investigation(["Investigation"]) is True

    def test_investigation_label_case_insensitive(self):
        assert _is_investigation(["investigation"]) is True
        assert _is_investigation(["INVESTIGATION"]) is True

    def test_investigation_among_others(self):
        assert _is_investigation(["Bug", "Investigation", "Urgent"]) is True


class TestBuildImplementPrompt:
    def test_standard_prompt(self):
        prompt = _build_implement_prompt("SMA-42", None)
        assert "SMA-42" in prompt
        assert "PR creation" in prompt
        assert "Do not stop" in prompt

    def test_standard_prompt_includes_no_pr_needed_instruction(self):
        prompt = _build_implement_prompt("SMA-42", None)
        assert "NO_PR_NEEDED:" in prompt

    def test_standard_prompt_with_non_investigation_labels(self):
        prompt = _build_implement_prompt("SMA-42", ["Bug", "Feature"])
        assert "PR creation" in prompt

    def test_investigation_prompt(self):
        prompt = _build_implement_prompt("SMA-42", ["Investigation"])
        assert "SMA-42" in prompt
        assert "investigation ticket" in prompt
        assert "Linear comment" in prompt
        assert "follow-up" in prompt.lower()
        assert "Do not create a PR" in prompt
        assert "PR creation" not in prompt

    def test_investigation_prompt_includes_dependency_instruction(self):
        prompt = _build_implement_prompt("SMA-42", ["Investigation"])
        assert "blockedBy" in prompt
        assert "blocks" in prompt
        assert "save_issue" in prompt

    def test_investigation_prompt_case_insensitive(self):
        prompt = _build_implement_prompt("SMA-42", ["investigation"])
        assert "investigation ticket" in prompt
        assert "Do not create a PR" in prompt


class TestRunImplementWithLabels:
    @patch("botfarm.worker.run_claude_streaming")
    def test_standard_ticket_uses_pr_prompt(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1", num_turns=10, duration_seconds=30.0,
            exit_subtype="tool_use", result_text=f"Created PR: {PR_URL}",
        )
        _run_implement("SMA-1", cwd=tmp_path, max_turns=100)
        prompt = mock_claude.call_args.args[0]
        assert "PR creation" in prompt

    @patch("botfarm.worker.run_claude_streaming")
    def test_investigation_ticket_uses_investigation_prompt(self, mock_claude, tmp_path):
        mock_claude.return_value = ClaudeResult(
            session_id="s1", num_turns=10, duration_seconds=30.0,
            exit_subtype="tool_use", result_text="Investigation complete.",
        )
        _run_implement(
            "SMA-1", ticket_labels=["Investigation"],
            cwd=tmp_path, max_turns=100,
        )
        prompt = mock_claude.call_args.args[0]
        assert "investigation ticket" in prompt
        assert "Do not create a PR" in prompt


# ---------------------------------------------------------------------------
# Identity env building tests
# ---------------------------------------------------------------------------


class TestBuildGitEnv:
    """Tests for build_git_env — supervisor-level SSH/GH_TOKEN env."""

    def test_full_config(self):
        ident = IdentitiesConfig(
            coder=CoderIdentity(
                github_token="gh-tok-123",
                ssh_key_path="~/.ssh/coder_key",
            ),
        )
        env = build_git_env(ident)
        assert env is not None
        assert env["GH_TOKEN"] == "gh-tok-123"
        assert "ssh -i" in env["GIT_SSH_COMMAND"]
        assert "coder_key" in env["GIT_SSH_COMMAND"]
        assert "-o IdentitiesOnly=yes" in env["GIT_SSH_COMMAND"]

    def test_returns_none_when_empty(self):
        """Empty identities should return None."""
        ident = IdentitiesConfig()
        assert build_git_env(ident) is None

    def test_ssh_only(self):
        ident = IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path="~/.ssh/my_key"),
        )
        env = build_git_env(ident)
        assert env is not None
        assert "GIT_SSH_COMMAND" in env
        assert "GH_TOKEN" not in env

    def test_gh_token_only(self):
        ident = IdentitiesConfig(
            coder=CoderIdentity(github_token="tok"),
        )
        env = build_git_env(ident)
        assert env == {"GH_TOKEN": "tok"}

    def test_ssh_key_path_expanded(self):
        ident = IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path="~/my_key"),
        )
        env = build_git_env(ident)
        assert env is not None
        assert "~" not in env["GIT_SSH_COMMAND"]
        assert "my_key" in env["GIT_SSH_COMMAND"]

    def test_does_not_include_git_author_or_db_path(self):
        """build_git_env should NOT include author or DB path (unlike build_coder_env)."""
        ident = IdentitiesConfig(
            coder=CoderIdentity(
                github_token="tok",
                ssh_key_path="~/.ssh/key",
                git_author_name="Bot",
                git_author_email="bot@example.com",
            ),
        )
        env = build_git_env(ident)
        assert env is not None
        assert "GIT_AUTHOR_NAME" not in env
        assert "GIT_COMMITTER_NAME" not in env
        assert "GIT_AUTHOR_EMAIL" not in env
        assert "GIT_COMMITTER_EMAIL" not in env
        assert "BOTFARM_DB_PATH" not in env


class TestBuildCoderEnv:
    def test_full_config(self):
        ident = IdentitiesConfig(
            coder=CoderIdentity(
                github_token="gh-tok-123",
                ssh_key_path="~/.ssh/coder_key",
                git_author_name="Coder Bot",
                git_author_email="coder@example.com",
            ),
        )
        env = build_coder_env(ident, slot_db_path="/tmp/slot.db")

        assert env["BOTFARM_DB_PATH"] == "/tmp/slot.db"
        assert env["GH_TOKEN"] == "gh-tok-123"
        assert "ssh -i" in env["GIT_SSH_COMMAND"]
        assert "coder_key" in env["GIT_SSH_COMMAND"]
        assert "-o IdentitiesOnly=yes" in env["GIT_SSH_COMMAND"]
        assert env["GIT_AUTHOR_NAME"] == "Coder Bot"
        assert env["GIT_COMMITTER_NAME"] == "Coder Bot"
        assert env["GIT_AUTHOR_EMAIL"] == "coder@example.com"
        assert env["GIT_COMMITTER_EMAIL"] == "coder@example.com"

    def test_empty_config(self):
        """Empty identities should produce empty env (backward compat)."""
        ident = IdentitiesConfig()
        env = build_coder_env(ident)
        assert env == {}

    def test_partial_config(self):
        """Only populated fields should appear in env."""
        ident = IdentitiesConfig(
            coder=CoderIdentity(github_token="tok"),
        )
        env = build_coder_env(ident)
        assert env == {"GH_TOKEN": "tok"}
        assert "GIT_SSH_COMMAND" not in env
        assert "GIT_AUTHOR_NAME" not in env

    def test_slot_db_path_only(self):
        ident = IdentitiesConfig()
        env = build_coder_env(ident, slot_db_path="/tmp/db.sqlite")
        assert env == {"BOTFARM_DB_PATH": "/tmp/db.sqlite"}

    def test_ssh_key_path_expanded(self):
        ident = IdentitiesConfig(
            coder=CoderIdentity(ssh_key_path="~/my_key"),
        )
        env = build_coder_env(ident)
        assert "~" not in env["GIT_SSH_COMMAND"]
        assert "my_key" in env["GIT_SSH_COMMAND"]


class TestBuildReviewerEnv:
    def test_full_config(self):
        ident = IdentitiesConfig(
            reviewer=ReviewerIdentity(github_token="rev-tok-456"),
        )
        env = build_reviewer_env(ident, slot_db_path="/tmp/slot.db")
        assert env == {
            "BOTFARM_DB_PATH": "/tmp/slot.db",
            "GH_TOKEN": "rev-tok-456",
        }

    def test_empty_config(self):
        ident = IdentitiesConfig()
        env = build_reviewer_env(ident)
        assert env == {}

    def test_slot_db_path_only(self):
        ident = IdentitiesConfig()
        env = build_reviewer_env(ident, slot_db_path="/tmp/db.sqlite")
        assert env == {"BOTFARM_DB_PATH": "/tmp/db.sqlite"}


class TestPipelineContextEnvForStage:
    def _make_ctx(self, coder_env=None, reviewer_env=None):
        return _PipelineContext(
            ticket_id="SMA-1",
            ticket_labels=[],
            task_id=1,
            cwd="/tmp",
            conn=MagicMock(),
            turns_cfg={},
            pr_checks_timeout=600,
            pipeline=PipelineResult(ticket_id="SMA-1", success=False, stages_completed=[]),
            coder_env=coder_env,
            reviewer_env=reviewer_env,
        )

    def test_coder_stages_get_coder_env(self):
        coder = {"GH_TOKEN": "coder-tok"}
        reviewer = {"GH_TOKEN": "reviewer-tok"}
        ctx = self._make_ctx(coder_env=coder, reviewer_env=reviewer)

        for stage in _CODER_STAGES:
            assert ctx._env_for_stage(stage) is coder, f"stage={stage}"

    def test_reviewer_stages_get_reviewer_env(self):
        coder = {"GH_TOKEN": "coder-tok"}
        reviewer = {"GH_TOKEN": "reviewer-tok"}
        ctx = self._make_ctx(coder_env=coder, reviewer_env=reviewer)

        for stage in _REVIEWER_STAGES:
            assert ctx._env_for_stage(stage) is reviewer, f"stage={stage}"

    def test_none_envs_return_none(self):
        ctx = self._make_ctx()
        for stage in STAGES:
            assert ctx._env_for_stage(stage) is None


class TestPrChecksEnvPropagation:
    @patch("botfarm.worker.subprocess.run")
    def test_env_propagated_to_subprocess(self, mock_run, tmp_path):
        """_run_pr_checks should pass merged env to subprocess.run."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="OK", stderr=""
        )
        env = {"GH_TOKEN": "test-token"}
        _run_pr_checks(PR_URL, cwd=tmp_path, env=env)

        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["env"] is not None
        assert call_kwargs["env"]["GH_TOKEN"] == "test-token"

    @patch("botfarm.worker.subprocess.run")
    def test_no_env_passes_none(self, mock_run, tmp_path):
        """When env is None, subprocess_env should be None."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="OK", stderr=""
        )
        _run_pr_checks(PR_URL, cwd=tmp_path)

        call_kwargs = mock_run.call_args[1]
        assert call_kwargs["env"] is None


class TestMergeEnvPropagation:
    @patch("botfarm.worker.subprocess.run")
    def test_env_propagated_to_all_subprocess_calls(self, mock_run, tmp_path):
        """_run_merge should pass env to all subprocess.run calls."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="Merged", stderr=""
        )
        env = {"GH_TOKEN": "merge-token"}
        _run_merge(PR_URL, cwd=tmp_path, env=env)

        for call in mock_run.call_args_list:
            call_kwargs = call[1]
            assert call_kwargs["env"] is not None
            assert call_kwargs["env"]["GH_TOKEN"] == "merge-token"

    @patch("botfarm.worker.subprocess.run")
    def test_no_env_passes_none(self, mock_run, tmp_path):
        """When env is None, subprocess calls get env=None."""
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="Merged", stderr=""
        )
        _run_merge(PR_URL, cwd=tmp_path)

        for call in mock_run.call_args_list:
            call_kwargs = call[1]
            assert call_kwargs["env"] is None


# ---------------------------------------------------------------------------
# DB-driven pipeline tests
# ---------------------------------------------------------------------------


from botfarm.worker import (
    _build_turns_cfg,
    _derive_stages,
    _run_claude_stage,
)
from botfarm.workflow import (
    PipelineTemplate,
    StageTemplate,
    StageLoop,
    get_stage,
    load_pipeline,
    render_prompt,
)


class TestDeriveStages:
    def test_derives_from_pipeline(self, conn):
        pipeline = load_pipeline(conn, [])
        stages = _derive_stages(pipeline)
        assert stages == ("implement", "review", "fix", "pr_checks", "ci_fix", "merge", "resolve_conflict")

    def test_investigation_pipeline_single_stage(self, conn):
        pipeline = load_pipeline(conn, ["Investigation"])
        stages = _derive_stages(pipeline)
        assert stages == ("implement",)


class TestBuildTurnsCfg:
    def test_from_pipeline(self, conn):
        pipeline = load_pipeline(conn, [])
        cfg = _build_turns_cfg(pipeline)
        assert cfg["implement"] == 200
        assert cfg["review"] == 100
        assert cfg["fix"] == 100

    def test_overrides_take_precedence(self, conn):
        pipeline = load_pipeline(conn, [])
        cfg = _build_turns_cfg(pipeline, {"implement": 50})
        assert cfg["implement"] == 50
        assert cfg["review"] == 100


class TestRunClaudeStage:
    @patch("botfarm.worker._invoke_claude")
    def test_uses_render_prompt(self, mock_invoke, conn):
        pipeline = load_pipeline(conn, [])
        stage_tpl = get_stage(pipeline, "implement")

        mock_invoke.return_value = ClaudeResult(
            session_id="sess-1", num_turns=5, duration_seconds=10.0,
            exit_subtype="tool_use", result_text="Created PR https://github.com/org/repo/pull/42",
        )
        result = _run_claude_stage(
            stage_tpl, cwd="/tmp", max_turns=100,
            prompt_vars={"ticket_id": "SMA-42"},
        )
        # Verify prompt was rendered with ticket_id
        called_prompt = mock_invoke.call_args[0][0]
        assert "SMA-42" in called_prompt
        assert "{ticket_id}" not in called_prompt

    @patch("botfarm.worker._invoke_claude")
    def test_pr_url_result_parser(self, mock_invoke, conn):
        pipeline = load_pipeline(conn, [])
        stage_tpl = get_stage(pipeline, "implement")

        mock_invoke.return_value = ClaudeResult(
            session_id="sess-1", num_turns=5, duration_seconds=10.0,
            exit_subtype="tool_use",
            result_text="Created PR https://github.com/org/repo/pull/42",
        )
        result = _run_claude_stage(
            stage_tpl, cwd="/tmp", max_turns=100,
            prompt_vars={"ticket_id": "SMA-42"},
        )
        assert result.pr_url == "https://github.com/org/repo/pull/42"
        assert result.success is True

    @patch("botfarm.worker._invoke_claude")
    def test_review_verdict_result_parser(self, mock_invoke, conn):
        pipeline = load_pipeline(conn, [])
        stage_tpl = get_stage(pipeline, "review")

        mock_invoke.return_value = ClaudeResult(
            session_id="sess-1", num_turns=3, duration_seconds=10.0,
            exit_subtype="tool_use",
            result_text="VERDICT: APPROVED",
        )
        result = _run_claude_stage(
            stage_tpl, cwd="/tmp", max_turns=100,
            prompt_vars={
                "pr_url": PR_URL,
                "pr_number": "42",
                "owner": "org",
                "repo": "repo",
            },
        )
        assert result.review_approved is True
        assert result.success is True

    @patch("botfarm.worker._invoke_claude")
    def test_error_result(self, mock_invoke, conn):
        pipeline = load_pipeline(conn, [])
        stage_tpl = get_stage(pipeline, "implement")

        mock_invoke.return_value = ClaudeResult(
            session_id="sess-1", num_turns=1, duration_seconds=5.0,
            exit_subtype="error", result_text="Something went wrong",
            is_error=True,
        )
        result = _run_claude_stage(
            stage_tpl, cwd="/tmp", max_turns=100,
            prompt_vars={"ticket_id": "SMA-42"},
        )
        assert result.success is False
        assert "Claude reported error" in result.error


class TestExecuteStageWithTemplate:
    @patch("botfarm.worker._invoke_claude")
    def test_claude_executor_routes_correctly(self, mock_invoke, conn):
        """executor_type='claude' uses _run_claude_stage."""
        pipeline = load_pipeline(conn, [])
        stage_tpl = get_stage(pipeline, "implement")
        mock_invoke.return_value = ClaudeResult(
            session_id="sess-1", num_turns=5, duration_seconds=10.0,
            exit_subtype="tool_use", result_text="done",
        )
        from botfarm.worker import _execute_stage
        result = _execute_stage(
            "implement", ticket_id="SMA-42", pr_url=None, cwd="/tmp",
            max_turns=100, pr_checks_timeout=600, stage_tpl=stage_tpl,
        )
        assert result.success is True
        assert mock_invoke.called

    @patch("botfarm.worker.subprocess.run")
    def test_shell_executor_routes_to_pr_checks(self, mock_run, conn, tmp_path):
        """executor_type='shell' routes to _run_pr_checks."""
        pipeline = load_pipeline(conn, [])
        stage_tpl = get_stage(pipeline, "pr_checks")
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="OK", stderr=""
        )
        from botfarm.worker import _execute_stage
        result = _execute_stage(
            "pr_checks", ticket_id="SMA-42", pr_url=PR_URL, cwd=tmp_path,
            max_turns=100, pr_checks_timeout=600, stage_tpl=stage_tpl,
        )
        assert result.success is True

    @patch("botfarm.worker.subprocess.run")
    def test_internal_executor_routes_to_merge(self, mock_run, conn, tmp_path):
        """executor_type='internal' routes to _run_merge."""
        pipeline = load_pipeline(conn, [])
        stage_tpl = get_stage(pipeline, "merge")
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="Merged", stderr=""
        )
        from botfarm.worker import _execute_stage
        result = _execute_stage(
            "merge", ticket_id="SMA-42", pr_url=PR_URL, cwd=tmp_path,
            max_turns=100, pr_checks_timeout=600, stage_tpl=stage_tpl,
        )
        assert result.success is True


class TestPipelineDbDriven:
    @patch("botfarm.worker._execute_stage")
    def test_pipeline_loads_from_db(self, mock_exec, conn, task_id, tmp_path):
        """Pipeline loads template from DB and uses its stages."""
        mock_exec.side_effect = [
            _mock_stage_result("implement", pr_url=PR_URL, turns=10),
            _mock_stage_result("review", turns=5),
            _mock_stage_result("fix", turns=8),
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
        assert result.success is True
        assert "implement" in result.stages_completed
        assert "merge" in result.stages_completed

    @patch("botfarm.worker._execute_stage")
    def test_investigation_pipeline_from_db(self, mock_exec, conn, task_id, tmp_path):
        """Investigation label selects investigation pipeline (single stage)."""
        mock_exec.return_value = _mock_stage_result("implement", pr_url=None)
        result = run_pipeline(
            ticket_id="SMA-99",
            ticket_labels=["Investigation"],
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is True
        assert result.stages_completed == ["implement"]
        # Only 1 call — investigation pipeline has no review/fix/merge
        assert mock_exec.call_count == 1

    @patch("botfarm.worker._execute_stage")
    def test_pipeline_context_has_template(self, mock_exec, conn, task_id, tmp_path):
        """_PipelineContext.pipeline_tpl is set when pipeline loads from DB."""
        captured_tpl = []

        def capture(stage, **kwargs):
            # The stage_tpl kwarg tells us the template was passed through
            captured_tpl.append(kwargs.get("stage_tpl"))
            return _mock_stage_result(stage, pr_url=PR_URL if stage == "implement" else None)

        mock_exec.side_effect = capture
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
        )
        # All stages should have received a stage_tpl
        assert all(tpl is not None for tpl in captured_tpl)

    @patch("botfarm.worker._execute_stage")
    def test_env_routing_uses_identity(self, mock_exec, conn, task_id, tmp_path):
        """Stage env is chosen by StageTemplate.identity, not hardcoded sets."""
        envs = []

        def capture(stage, **kwargs):
            envs.append((stage, kwargs.get("env")))
            return _mock_stage_result(stage, pr_url=PR_URL if stage == "implement" else None)

        mock_exec.side_effect = capture
        run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
            max_review_iterations=1,
            slot_db_path="/tmp/slot.db",
        )
        env_by_stage = dict(envs)
        # Coder stages should have BOTFARM_DB_PATH
        assert env_by_stage["implement"]["BOTFARM_DB_PATH"] == "/tmp/slot.db"
        # Review should also have BOTFARM_DB_PATH (reviewer env)
        assert env_by_stage["review"]["BOTFARM_DB_PATH"] == "/tmp/slot.db"


# ---------------------------------------------------------------------------
# _detect_no_pr_needed
# ---------------------------------------------------------------------------


class TestDetectNoPrNeeded:
    def test_matching_text(self):
        text = "NO_PR_NEEDED: All acceptance criteria already met on main."
        assert _detect_no_pr_needed(text) == "All acceptance criteria already met on main."

    def test_case_insensitive(self):
        text = "no_pr_needed: work already done"
        assert _detect_no_pr_needed(text) == "work already done"

    def test_mixed_case(self):
        text = "No_Pr_Needed: delivered by SMA-200"
        assert _detect_no_pr_needed(text) == "delivered by SMA-200"

    def test_embedded_in_longer_text(self):
        text = "I verified all criteria.\nNO_PR_NEEDED: Everything is already on main."
        assert _detect_no_pr_needed(text) == "Everything is already on main."

    def test_no_match(self):
        assert _detect_no_pr_needed("Created PR: https://github.com/o/r/pull/1") is None

    def test_empty_string(self):
        assert _detect_no_pr_needed("") is None

    def test_none_input(self):
        assert _detect_no_pr_needed("") is None

    def test_prefix_only_no_explanation(self):
        # "NO_PR_NEEDED:" with only whitespace after → None (empty explanation)
        assert _detect_no_pr_needed("NO_PR_NEEDED:   ") is None

    def test_long_explanation_truncated(self):
        explanation = "x" * 600
        text = f"NO_PR_NEEDED: {explanation}"
        result = _detect_no_pr_needed(text)
        assert result is not None
        assert len(result) == 500


# ---------------------------------------------------------------------------
# run_pipeline — no_pr_needed signal tests
# ---------------------------------------------------------------------------


class TestRunPipelineNoPrNeeded:
    @patch("botfarm.worker._execute_stage")
    def test_no_pr_needed_signal_succeeds(self, mock_exec, conn, task_id, tmp_path):
        """Implement with NO_PR_NEEDED signal → pipeline success, no failure."""
        cr = ClaudeResult(
            session_id="sess-impl",
            num_turns=5,
            duration_seconds=10.0,
            exit_subtype="tool_use",
            result_text="NO_PR_NEEDED: All criteria met on main via SMA-200.",
        )
        mock_exec.return_value = StageResult(
            stage="implement", success=True, claude_result=cr, pr_url=None,
        )
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is True
        assert result.no_pr_reason == "All criteria met on main via SMA-200."
        assert result.pr_url is None
        assert result.failure_stage is None

        task = get_task(conn, task_id)
        assert task["status"] == "completed"
        assert task["completed_at"] is not None
        assert "criteria met" in task["comments"]

    @patch("botfarm.worker._execute_stage")
    def test_no_pr_needed_without_signal_still_fails(self, mock_exec, conn, task_id, tmp_path):
        """Implement with no PR URL and no signal → pipeline failure (legacy behavior)."""
        cr = ClaudeResult(
            session_id="sess-impl",
            num_turns=5,
            duration_seconds=10.0,
            exit_subtype="tool_use",
            result_text="I verified everything looks good.",
        )
        mock_exec.return_value = StageResult(
            stage="implement", success=True, claude_result=cr, pr_url=None,
        )
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
    def test_no_pr_needed_with_no_claude_result(self, mock_exec, conn, task_id, tmp_path):
        """Implement with no claude_result and no PR URL → failure (no crash)."""
        mock_exec.return_value = StageResult(
            stage="implement", success=True, claude_result=None, pr_url=None,
        )
        result = run_pipeline(
            ticket_id="SMA-99",
            task_id=task_id,
            cwd=tmp_path,
            conn=conn,
        )
        assert result.success is False
        assert result.failure_stage == "implement"
