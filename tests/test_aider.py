"""Tests for botfarm.aider -- Aider CLI invocation and output parsing."""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.aider import (
    AiderResult,
    _parse_token_count,
    check_aider_available,
    parse_aider_output,
    run_aider,
)


# ---------------------------------------------------------------------------
# Sample output data
# ---------------------------------------------------------------------------

SAMPLE_AIDER_OUTPUT = [
    "Aider v0.82.1\n",
    "Model: claude-3-5-sonnet with diff edit format\n",
    "Git repo: . with 42 files\n",
    "Repo-map: using 1024 tokens, auto refresh\n",
    "\n",
    "I'll implement the requested changes.\n",
    "\n",
    "Applied edit to src/main.py\n",
    "Commit abc1234 feat: add new feature\n",
    "\n",
    "Tokens: 4.2k sent, 892 received. Cost: $0.02 message, $0.05 session.\n",
]


# ---------------------------------------------------------------------------
# _parse_token_count tests
# ---------------------------------------------------------------------------


class TestParseTokenCount:
    def test_plain_number(self):
        assert _parse_token_count("892") == 892

    def test_comma_separated(self):
        assert _parse_token_count("4,225") == 4225

    def test_k_suffix(self):
        assert _parse_token_count("4.2k") == 4200

    def test_k_suffix_integer(self):
        assert _parse_token_count("10k") == 10000

    def test_whitespace(self):
        assert _parse_token_count("  892  ") == 892


# ---------------------------------------------------------------------------
# parse_aider_output tests
# ---------------------------------------------------------------------------


class TestParseAiderOutput:
    def test_parse_full_output(self):
        result = parse_aider_output(SAMPLE_AIDER_OUTPUT)

        assert result.input_tokens == 4200
        assert result.output_tokens == 892
        assert result.cost_usd == pytest.approx(0.05)
        assert "I'll implement the requested changes." in result.result_text

    def test_multiple_token_lines_uses_last(self):
        """When Aider prints multiple summaries, use the last one."""
        lines = [
            "First batch\n",
            "Tokens: 1k sent, 200 received. Cost: $0.01 message, $0.02 session.\n",
            "Second batch\n",
            "Tokens: 2k sent, 400 received. Cost: $0.03 message, $0.08 session.\n",
        ]
        result = parse_aider_output(lines)

        assert result.input_tokens == 2000
        assert result.output_tokens == 400
        assert result.cost_usd == pytest.approx(0.08)

    def test_no_token_line(self):
        lines = ["Just some output\n", "No token info here\n"]
        result = parse_aider_output(lines)

        assert result.input_tokens == 0
        assert result.output_tokens == 0
        assert result.cost_usd == 0.0

    def test_tokens_without_cost(self):
        lines = ["Tokens: 500 sent, 100 received.\n"]
        result = parse_aider_output(lines)

        assert result.input_tokens == 500
        assert result.output_tokens == 100
        assert result.cost_usd == 0.0

    def test_empty_output(self):
        result = parse_aider_output([])

        assert result.input_tokens == 0
        assert result.output_tokens == 0
        assert result.cost_usd == 0.0
        assert result.result_text == ""
        assert result.duration_seconds == 0.0

    def test_result_text_preserves_full_output(self):
        result = parse_aider_output(SAMPLE_AIDER_OUTPUT)
        assert "Applied edit to src/main.py" in result.result_text
        assert "Commit abc1234" in result.result_text


class TestAiderResultDefaults:
    def test_dataclass_defaults(self):
        result = AiderResult(
            duration_seconds=5.0,
            result_text="hello",
        )
        assert result.is_error is False
        assert result.input_tokens == 0
        assert result.output_tokens == 0
        assert result.cost_usd == 0.0
        assert result.model == ""


# ---------------------------------------------------------------------------
# run_aider tests
# ---------------------------------------------------------------------------


def _make_fake_popen(stdout_lines, returncode=0):
    """Create a mock Popen that yields the given stdout lines."""
    proc = MagicMock()
    proc.pid = 99999
    proc.returncode = returncode
    proc.stdout = iter(stdout_lines)
    proc.stderr = iter([])
    proc.poll = lambda: returncode
    proc.wait = MagicMock(return_value=returncode)
    return proc


class TestRunAider:
    @pytest.fixture(autouse=True)
    def _mock_aider_which(self):
        """Mock shutil.which so tests don't depend on aider being installed."""
        with patch("botfarm.aider.shutil.which", return_value="/usr/local/bin/aider"):
            yield

    def test_successful_run(self, tmp_path):
        proc = _make_fake_popen(SAMPLE_AIDER_OUTPUT, returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc):
            result = run_aider("Implement the feature", cwd=tmp_path)

        assert result.input_tokens == 4200
        assert result.output_tokens == 892
        assert result.cost_usd == pytest.approx(0.05)
        assert result.is_error is False
        assert result.duration_seconds > 0

    def test_nonzero_exit_sets_error(self, tmp_path):
        proc = _make_fake_popen(SAMPLE_AIDER_OUTPUT, returncode=1)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc):
            result = run_aider("Implement the feature", cwd=tmp_path)

        assert result.is_error is True
        # Result text should still be parsed
        assert "I'll implement the requested changes." in result.result_text

    def test_log_file_written(self, tmp_path):
        log_path = tmp_path / "logs" / "aider.log"
        proc = _make_fake_popen(SAMPLE_AIDER_OUTPUT, returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc):
            run_aider("Implement the feature", cwd=tmp_path, log_file=log_path)

        assert log_path.exists()
        content = log_path.read_text()
        for line in SAMPLE_AIDER_OUTPUT:
            assert line in content

    def test_model_flag_passed(self, tmp_path):
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc) as mock_popen:
            run_aider("Implement the feature", cwd=tmp_path, model="gpt-4o")

        call_args = mock_popen.call_args[0][0]
        assert "--model" in call_args
        model_idx = call_args.index("--model")
        assert call_args[model_idx + 1] == "gpt-4o"

    def test_no_model_flag_when_none(self, tmp_path):
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc) as mock_popen:
            run_aider("Implement the feature", cwd=tmp_path)

        call_args = mock_popen.call_args[0][0]
        assert "--model" not in call_args

    def test_env_passed_to_subprocess(self, tmp_path):
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc) as mock_popen:
            run_aider(
                "Implement the feature",
                cwd=tmp_path,
                env={"OPENAI_API_KEY": "sk-test"},
            )

        passed_env = mock_popen.call_args[1]["env"]
        assert passed_env["OPENAI_API_KEY"] == "sk-test"

    def test_no_env_when_none(self, tmp_path):
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc) as mock_popen:
            run_aider("Implement the feature", cwd=tmp_path)

        passed_env = mock_popen.call_args[1]["env"]
        assert passed_env is None

    def test_command_structure(self, tmp_path):
        """Verify the command has the correct flags."""
        proc = _make_fake_popen([], returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc) as mock_popen:
            run_aider("Do the thing", cwd=tmp_path)

        cmd = mock_popen.call_args[0][0]
        assert cmd[0] == "/usr/local/bin/aider"
        assert "--yes-always" in cmd
        assert "--no-suggest-shell-commands" in cmd
        assert "--no-pretty" in cmd
        assert "--no-auto-lint" in cmd
        assert "--no-auto-test" in cmd
        assert "--message" in cmd
        msg_idx = cmd.index("--message")
        assert cmd[msg_idx + 1] == "Do the thing"

    def test_aider_not_found_raises(self, tmp_path):
        with patch("botfarm.aider.shutil.which", return_value=None):
            with pytest.raises(FileNotFoundError, match="Cannot find 'aider'"):
                run_aider("Implement the feature", cwd=tmp_path)

    def test_model_stored_in_result(self, tmp_path):
        proc = _make_fake_popen(SAMPLE_AIDER_OUTPUT, returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc):
            result = run_aider("Do it", cwd=tmp_path, model="gpt-4o")

        assert result.model == "gpt-4o"

    def test_no_model_stored_empty(self, tmp_path):
        proc = _make_fake_popen(SAMPLE_AIDER_OUTPUT, returncode=0)

        with patch("botfarm.aider.subprocess.Popen", return_value=proc):
            result = run_aider("Do it", cwd=tmp_path)

        assert result.model == ""


class TestAiderTimeout:
    @pytest.fixture(autouse=True)
    def _mock_aider_which(self):
        with patch("botfarm.aider.shutil.which", return_value="/usr/local/bin/aider"):
            yield

    def test_timeout_kills_process(self, tmp_path):
        """Mock a subprocess that hangs, verify timeout sets is_error."""
        proc = MagicMock()
        proc.pid = 99999
        proc.stderr = iter([])
        proc.returncode = -9

        stop_event = threading.Event()

        def slow_stdout():
            yield "Starting aider...\n"
            stop_event.wait()

        proc.stdout = slow_stdout()
        proc.poll = lambda: None
        proc.wait = MagicMock(return_value=-9)

        def fake_terminate(*args):
            stop_event.set()
            proc.poll = lambda: -9

        with patch("botfarm.aider.subprocess.Popen", return_value=proc), \
             patch("botfarm.aider.terminate_process_group", side_effect=fake_terminate):
            result = run_aider("Do it", cwd=tmp_path, timeout=0.5)

        assert result.is_error is True


# ---------------------------------------------------------------------------
# check_aider_available tests
# ---------------------------------------------------------------------------


class TestCheckAiderAvailable:
    def test_aider_found(self):
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "aider v0.82.1\n"

        with patch("botfarm.aider.subprocess.run", return_value=mock_result):
            ok, msg = check_aider_available()

        assert ok is True
        assert "0.82.1" in msg

    def test_aider_not_found(self):
        with patch("botfarm.aider.subprocess.run", side_effect=FileNotFoundError):
            ok, msg = check_aider_available()

        assert ok is False
        assert "not found" in msg

    def test_aider_nonzero_exit(self):
        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""

        with patch("botfarm.aider.subprocess.run", return_value=mock_result):
            ok, msg = check_aider_available()

        assert ok is False
        assert "exited with code 1" in msg

    def test_aider_timeout(self):
        with patch(
            "botfarm.aider.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="aider", timeout=10),
        ):
            ok, msg = check_aider_available()

        assert ok is False
        assert "timed out" in msg

    def test_aider_oserror(self):
        with patch(
            "botfarm.aider.subprocess.run",
            side_effect=OSError("permission denied"),
        ):
            ok, msg = check_aider_available()

        assert ok is False
        assert "permission denied" in msg
