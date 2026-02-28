"""Tests for botfarm.codex — Codex CLI invocation and JSONL parsing."""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.codex import (
    CodexResult,
    check_codex_available,
    parse_codex_jsonl,
    run_codex_streaming,
)


# ---------------------------------------------------------------------------
# Sample JSONL data
# ---------------------------------------------------------------------------

SAMPLE_JSONL_LINES = [
    '{"type":"thread.started","thread_id":"019ca047-abcd-1234-5678-abcdef012345"}\n',
    '{"type":"turn.started"}\n',
    '{"type":"item.completed","item":{"id":"item_0","type":"reasoning","text":"Let me think about this..."}}\n',
    '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"I have reviewed the code and found no issues."}}\n',
    '{"type":"turn.completed","usage":{"input_tokens":13268,"cached_input_tokens":4480,"output_tokens":34}}\n',
]


# ---------------------------------------------------------------------------
# parse_codex_jsonl tests
# ---------------------------------------------------------------------------


class TestParseCodexJsonl:
    def test_parse_full_session(self):
        result = parse_codex_jsonl(SAMPLE_JSONL_LINES)

        assert result.thread_id == "019ca047-abcd-1234-5678-abcdef012345"
        assert result.num_turns == 1
        assert result.result_text == "I have reviewed the code and found no issues."
        assert result.is_error is False
        assert result.input_tokens == 13268
        assert result.output_tokens == 34
        assert result.cached_input_tokens == 4480

    def test_multi_turn_session(self):
        lines = [
            '{"type":"thread.started","thread_id":"thread-multi"}\n',
            '{"type":"turn.started"}\n',
            '{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"First response"}}\n',
            '{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":20}}\n',
            '{"type":"turn.started"}\n',
            '{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"Second response"}}\n',
            '{"type":"turn.completed","usage":{"input_tokens":200,"cached_input_tokens":50,"output_tokens":30}}\n',
        ]
        result = parse_codex_jsonl(lines)

        assert result.num_turns == 2
        assert result.result_text == "Second response"  # last agent_message
        assert result.input_tokens == 300  # accumulated
        assert result.output_tokens == 50
        assert result.cached_input_tokens == 50

    def test_error_event(self):
        lines = [
            '{"type":"thread.started","thread_id":"thread-err"}\n',
            '{"type":"turn.started"}\n',
            '{"type":"error","message":"something went wrong"}\n',
        ]
        result = parse_codex_jsonl(lines)

        assert result.is_error is True
        assert result.thread_id == "thread-err"

    def test_turn_failed_event(self):
        lines = [
            '{"type":"thread.started","thread_id":"thread-fail"}\n',
            '{"type":"turn.started"}\n',
            '{"type":"turn.failed","reason":"timeout"}\n',
        ]
        result = parse_codex_jsonl(lines)

        assert result.is_error is True

    def test_empty_lines_skipped(self):
        lines = ["", "\n", "  \n"]
        result = parse_codex_jsonl(lines)

        assert result.thread_id == ""
        assert result.num_turns == 0

    def test_reasoning_not_included_in_result(self):
        lines = [
            '{"type":"thread.started","thread_id":"thread-reason"}\n',
            '{"type":"turn.started"}\n',
            '{"type":"item.completed","item":{"id":"item_0","type":"reasoning","text":"Deep thoughts here"}}\n',
            '{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":10}}\n',
        ]
        result = parse_codex_jsonl(lines)

        assert result.result_text == ""  # no agent_message => empty


class TestCodexResultDefaults:
    def test_minimal_output(self):
        """Verify defaults when only thread.started is present."""
        lines = ['{"type":"thread.started","thread_id":"thread-minimal"}\n']
        result = parse_codex_jsonl(lines)

        assert result.thread_id == "thread-minimal"
        assert result.num_turns == 0
        assert result.duration_seconds == 0.0
        assert result.result_text == ""
        assert result.is_error is False
        assert result.input_tokens == 0
        assert result.output_tokens == 0
        assert result.cached_input_tokens == 0

    def test_dataclass_defaults(self):
        result = CodexResult(
            thread_id="t1",
            num_turns=1,
            duration_seconds=5.0,
            result_text="hello",
        )
        assert result.is_error is False
        assert result.input_tokens == 0
        assert result.output_tokens == 0
        assert result.cached_input_tokens == 0


class TestCodexMalformedJsonl:
    def test_malformed_lines_skipped(self):
        lines = [
            '{"type":"thread.started","thread_id":"thread-ok"}\n',
            "this is not json at all\n",
            '{"type":"turn.started"}\n',
            "{bad json\n",
            '{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"All good"}}\n',
            '{"type":"turn.completed","usage":{"input_tokens":100,"cached_input_tokens":0,"output_tokens":10}}\n',
        ]
        result = parse_codex_jsonl(lines)

        assert result.thread_id == "thread-ok"
        assert result.num_turns == 1
        assert result.result_text == "All good"
        assert result.is_error is False

    def test_all_malformed(self):
        lines = ["not json\n", "also not json\n"]
        result = parse_codex_jsonl(lines)

        assert result.thread_id == ""
        assert result.num_turns == 0
        assert result.is_error is False


# ---------------------------------------------------------------------------
# run_codex_streaming tests
# ---------------------------------------------------------------------------


def _make_fake_popen(stdout_lines, returncode=0):
    """Create a mock Popen that yields the given stdout lines."""
    proc = MagicMock()
    proc.pid = 99999
    proc.returncode = returncode

    # stdin mock
    proc.stdin = MagicMock()

    # stdout as iterable
    proc.stdout = iter(stdout_lines)

    # stderr as iterable (empty)
    proc.stderr = iter([])

    def fake_poll():
        return returncode

    proc.poll = fake_poll
    proc.wait = MagicMock(return_value=returncode)

    return proc


class TestRunCodexStreaming:
    def test_successful_run(self, tmp_path):
        proc = _make_fake_popen(SAMPLE_JSONL_LINES, returncode=0)

        with patch("botfarm.codex.subprocess.Popen", return_value=proc):
            result = run_codex_streaming(
                "Review this code",
                cwd=tmp_path,
            )

        assert result.thread_id == "019ca047-abcd-1234-5678-abcdef012345"
        assert result.num_turns == 1
        assert result.result_text == "I have reviewed the code and found no issues."
        assert result.is_error is False
        assert result.duration_seconds > 0

    def test_nonzero_exit_sets_error(self, tmp_path):
        proc = _make_fake_popen(SAMPLE_JSONL_LINES, returncode=1)

        with patch("botfarm.codex.subprocess.Popen", return_value=proc):
            result = run_codex_streaming(
                "Review this code",
                cwd=tmp_path,
            )

        assert result.is_error is True
        # Result text should still be parsed
        assert result.result_text == "I have reviewed the code and found no issues."

    def test_log_file_written(self, tmp_path):
        log_path = tmp_path / "logs" / "codex.jsonl"
        proc = _make_fake_popen(SAMPLE_JSONL_LINES, returncode=0)

        with patch("botfarm.codex.subprocess.Popen", return_value=proc):
            run_codex_streaming(
                "Review this code",
                cwd=tmp_path,
                log_file=log_path,
            )

        assert log_path.exists()
        content = log_path.read_text()
        for line in SAMPLE_JSONL_LINES:
            assert line in content

    def test_model_flag_passed(self, tmp_path):
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.codex.subprocess.Popen", return_value=proc) as mock_popen:
            run_codex_streaming(
                "Review this code",
                cwd=tmp_path,
                model="o3",
            )

        call_args = mock_popen.call_args[0][0]
        assert "-m" in call_args
        model_idx = call_args.index("-m")
        assert call_args[model_idx + 1] == "o3"

    def test_env_passed_to_subprocess(self, tmp_path):
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.codex.subprocess.Popen", return_value=proc) as mock_popen:
            run_codex_streaming(
                "Review this code",
                cwd=tmp_path,
                env={"OPENAI_API_KEY": "sk-test"},
            )

        passed_env = mock_popen.call_args[1]["env"]
        assert passed_env["OPENAI_API_KEY"] == "sk-test"

    def test_command_structure(self, tmp_path):
        """Verify the command has the correct structure per the ticket spec."""
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.codex.subprocess.Popen", return_value=proc) as mock_popen:
            run_codex_streaming(
                "Review this code",
                cwd=tmp_path,
            )

        cmd = mock_popen.call_args[0][0]
        assert cmd[0] == "codex"
        assert cmd[1:3] == ["-a", "never"]
        assert cmd[3:5] == ["-s", "workspace-write"]
        # -C <cwd> should come before exec
        c_idx = cmd.index("-C")
        exec_idx = cmd.index("exec")
        assert c_idx < exec_idx
        # --ephemeral and --json should be after exec
        assert "--ephemeral" in cmd[exec_idx:]
        assert "--json" in cmd[exec_idx:]
        assert cmd[-1] == "-"


class TestCodexTimeout:
    def test_timeout_kills_process(self, tmp_path):
        """Mock a subprocess that hangs, verify timeout sets is_error."""
        proc = MagicMock()
        proc.pid = 99999
        proc.stdin = MagicMock()
        proc.stderr = iter([])
        proc.returncode = -9

        # stdout blocks forever then stops when process is killed
        stop_event = threading.Event()

        def slow_stdout():
            # Yield one line, then block
            yield '{"type":"thread.started","thread_id":"thread-hang"}\n'
            stop_event.wait()  # block until told to stop

        proc.stdout = slow_stdout()
        proc.poll = lambda: None

        def fake_wait(timeout=None):
            if timeout is not None:
                raise subprocess.TimeoutExpired(cmd="codex", timeout=timeout)

        proc.wait = fake_wait

        def fake_terminate(*args):
            stop_event.set()
            proc.poll = lambda: -9
            proc.wait = MagicMock(return_value=-9)

        with patch("botfarm.codex.subprocess.Popen", return_value=proc), \
             patch("botfarm.codex.terminate_process_group", side_effect=fake_terminate):
            result = run_codex_streaming(
                "Review this code",
                cwd=tmp_path,
                timeout=0.5,
            )

        assert result.is_error is True


# ---------------------------------------------------------------------------
# check_codex_available tests
# ---------------------------------------------------------------------------


class TestCheckCodexAvailable:
    def test_codex_found(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "0.106.0\n"

        with patch("botfarm.codex.subprocess.run", return_value=mock_result):
            ok, msg = check_codex_available()

        assert ok is True
        assert "0.106.0" in msg

    def test_codex_not_found(self):
        with patch("botfarm.codex.subprocess.run", side_effect=FileNotFoundError):
            ok, msg = check_codex_available()

        assert ok is False
        assert "not found" in msg

    def test_codex_nonzero_exit(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("botfarm.codex.subprocess.run", return_value=mock_result):
            ok, msg = check_codex_available()

        assert ok is False
        assert "exited with code 1" in msg

    def test_codex_timeout(self):
        with patch("botfarm.codex.subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="codex", timeout=10)):
            ok, msg = check_codex_available()

        assert ok is False
        assert "timed out" in msg

    def test_codex_oserror(self):
        with patch("botfarm.codex.subprocess.run", side_effect=OSError("permission denied")):
            ok, msg = check_codex_available()

        assert ok is False
        assert "permission denied" in msg
