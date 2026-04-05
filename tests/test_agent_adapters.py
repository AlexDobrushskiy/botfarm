"""Tests for ClaudeAdapter and CodexAdapter."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from botfarm.agent import AgentAdapter, AgentResult
from botfarm.agent_claude import ClaudeAdapter, _claude_result_to_agent_result
from botfarm.agent_codex import CodexAdapter, _codex_result_to_agent_result
from botfarm.codex import CodexResult
from botfarm.worker_claude import ClaudeResult


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_claude_adapter_is_agent_adapter(self):
        assert isinstance(ClaudeAdapter(), AgentAdapter)

    def test_codex_adapter_is_agent_adapter(self):
        assert isinstance(CodexAdapter(), AgentAdapter)


# ---------------------------------------------------------------------------
# ClaudeAdapter
# ---------------------------------------------------------------------------


class TestClaudeAdapter:
    def test_properties(self):
        adapter = ClaudeAdapter()
        assert adapter.name == "claude"
        assert adapter.supports_context_fill is True
        assert adapter.supports_max_turns is True
        assert adapter.supports_model_override is True

    @patch("botfarm.agent_claude.run_claude_streaming")
    def test_run_delegates_to_run_claude_streaming(self, mock_run, tmp_path: Path):
        mock_run.return_value = ClaudeResult(
            session_id="sess-abc",
            num_turns=5,
            duration_seconds=30.0,
            exit_subtype="end_turn",
            result_text="all done",
            input_tokens=1000,
            output_tokens=500,
            cache_read_input_tokens=200,
            cache_creation_input_tokens=100,
            total_cost_usd=0.05,
            context_fill_pct=42.0,
            model_usage_json='{"claude-sonnet": {}}',
        )

        adapter = ClaudeAdapter()
        result = adapter.run(
            "test prompt",
            cwd=tmp_path,
            max_turns=50,
            log_file=tmp_path / "log.txt",
            env={"KEY": "val"},
            timeout=300.0,
        )

        mock_run.assert_called_once_with(
            "test prompt",
            cwd=tmp_path,
            max_turns=50,
            log_file=tmp_path / "log.txt",
            env={"KEY": "val"},
            on_context_fill=None,
            timeout=300.0,
            mcp_config=None,
            auth_mode="oauth",
            model=None,
            effort=None,
        )

        assert isinstance(result, AgentResult)
        assert result.session_id == "sess-abc"
        assert result.num_turns == 5
        assert result.duration_seconds == 30.0
        assert result.result_text == "all done"
        assert result.is_error is False
        assert result.input_tokens == 1000
        assert result.output_tokens == 500
        assert result.cost_usd == 0.05
        assert result.context_fill_pct == 42.0
        assert result.extra["exit_subtype"] == "end_turn"
        assert result.extra["model_usage_json"] == '{"claude-sonnet": {}}'
        assert result.extra["cache_creation_input_tokens"] == 100
        assert result.extra["cache_read_input_tokens"] == 200

    @patch("botfarm.agent_claude.run_claude_streaming")
    def test_run_defaults_max_turns(self, mock_run, tmp_path: Path):
        mock_run.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="end_turn", result_text="",
        )

        adapter = ClaudeAdapter()
        adapter.run("prompt", cwd=tmp_path)

        _, kwargs = mock_run.call_args
        assert kwargs["max_turns"] == 200

    @patch("botfarm.agent_claude.run_claude_streaming")
    def test_run_passes_context_fill_callback(self, mock_run, tmp_path: Path):
        mock_run.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="end_turn", result_text="",
        )

        def my_cb(turn: int, fill: float) -> None:
            pass

        adapter = ClaudeAdapter()
        adapter.run("prompt", cwd=tmp_path, on_context_fill=my_cb)

        _, kwargs = mock_run.call_args
        assert kwargs["on_context_fill"] is my_cb

    @patch("botfarm.agent_claude.check_claude_available")
    def test_check_available_delegates(self, mock_check):
        mock_check.return_value = (True, "claude 1.0.30")
        ok, msg = ClaudeAdapter().check_available()
        assert ok is True
        assert "1.0.30" in msg
        mock_check.assert_called_once()

    @patch("botfarm.agent_claude.check_claude_available")
    def test_check_available_not_found(self, mock_check):
        mock_check.return_value = (False, "claude binary not found on PATH")
        ok, msg = ClaudeAdapter().check_available()
        assert ok is False
        assert "not found" in msg


# ---------------------------------------------------------------------------
# CodexAdapter
# ---------------------------------------------------------------------------


class TestCodexAdapter:
    def test_properties(self):
        adapter = CodexAdapter()
        assert adapter.name == "codex"
        assert adapter.supports_context_fill is False
        assert adapter.supports_max_turns is False
        assert adapter.supports_model_override is True

    @patch("botfarm.agent_codex.run_codex_streaming")
    def test_run_delegates_to_run_codex_streaming(self, mock_run, tmp_path: Path):
        mock_run.return_value = CodexResult(
            thread_id="thread-xyz",
            num_turns=3,
            duration_seconds=20.0,
            result_text="codex done",
            input_tokens=800,
            output_tokens=400,
            cached_input_tokens=150,
            model="o4-mini",
        )

        adapter = CodexAdapter(model="o3")
        result = adapter.run(
            "test prompt",
            cwd=tmp_path,
            log_file=tmp_path / "log.txt",
            env={"KEY": "val"},
            timeout=300.0,
        )

        mock_run.assert_called_once_with(
            "test prompt",
            cwd=tmp_path,
            model="o3",
            reasoning_effort=None,
            log_file=tmp_path / "log.txt",
            env={"KEY": "val"},
            timeout=300.0,
        )

        assert isinstance(result, AgentResult)
        assert result.session_id == "thread-xyz"
        assert result.num_turns == 3
        assert result.duration_seconds == 20.0
        assert result.result_text == "codex done"
        assert result.is_error is False
        assert result.input_tokens == 800
        assert result.output_tokens == 400
        assert result.context_fill_pct is None
        assert result.extra["thread_id"] == "thread-xyz"
        assert result.extra["cache_read_input_tokens"] == 150

    @patch("botfarm.agent_codex.run_codex_streaming")
    def test_run_model_override(self, mock_run, tmp_path: Path):
        """Runtime model param overrides constructor model."""
        mock_run.return_value = CodexResult(
            thread_id="t", num_turns=1, duration_seconds=1.0,
            result_text="", model="o3",
        )

        adapter = CodexAdapter(model="o4-mini")
        adapter.run("prompt", cwd=tmp_path, model="o3")

        _, kwargs = mock_run.call_args
        assert kwargs["model"] == "o3"

    @patch("botfarm.agent_codex.run_codex_streaming")
    def test_run_uses_constructor_model_when_no_override(self, mock_run, tmp_path: Path):
        mock_run.return_value = CodexResult(
            thread_id="t", num_turns=1, duration_seconds=1.0,
            result_text="", model="o4-mini",
        )

        adapter = CodexAdapter(model="o4-mini")
        adapter.run("prompt", cwd=tmp_path)

        _, kwargs = mock_run.call_args
        assert kwargs["model"] == "o4-mini"

    @patch("botfarm.agent_codex.check_codex_available")
    def test_check_available_delegates(self, mock_check):
        mock_check.return_value = (True, "codex 0.1.0")
        ok, msg = CodexAdapter().check_available()
        assert ok is True
        assert "codex" in msg
        mock_check.assert_called_once()

    @patch("botfarm.agent_codex.run_codex_streaming")
    def test_run_reasoning_effort(self, mock_run, tmp_path: Path):
        mock_run.return_value = CodexResult(
            thread_id="t", num_turns=1, duration_seconds=1.0,
            result_text="", model="o3",
        )

        adapter = CodexAdapter(model="o3", reasoning_effort="low")
        adapter.run("prompt", cwd=tmp_path)

        _, kwargs = mock_run.call_args
        assert kwargs["reasoning_effort"] == "low"


# ---------------------------------------------------------------------------
# AgentResult normalization
# ---------------------------------------------------------------------------


class TestClaudeResultNormalization:
    def test_full_normalization(self):
        cr = ClaudeResult(
            session_id="sess-1",
            num_turns=10,
            duration_seconds=45.0,
            exit_subtype="end_turn",
            result_text="result text",
            is_error=False,
            input_tokens=5000,
            output_tokens=2000,
            cache_read_input_tokens=300,
            cache_creation_input_tokens=150,
            total_cost_usd=0.10,
            context_fill_pct=65.0,
            model_usage_json='{"model": {}}',
        )
        ar = _claude_result_to_agent_result(cr)
        assert ar.session_id == "sess-1"
        assert ar.cost_usd == 0.10
        assert ar.context_fill_pct == 65.0
        assert ar.extra["exit_subtype"] == "end_turn"
        assert ar.extra["cache_read_input_tokens"] == 300

    def test_error_normalization(self):
        cr = ClaudeResult(
            session_id="sess-err",
            num_turns=1,
            duration_seconds=2.0,
            exit_subtype="error",
            result_text="boom",
            is_error=True,
        )
        ar = _claude_result_to_agent_result(cr)
        assert ar.is_error is True
        assert ar.cost_usd == 0.0
        assert ar.context_fill_pct is None


class TestCodexResultNormalization:
    def test_full_normalization(self):
        cr = CodexResult(
            thread_id="thread-1",
            num_turns=4,
            duration_seconds=25.0,
            result_text="output",
            input_tokens=1000,
            output_tokens=500,
            cached_input_tokens=200,
            model="o4-mini",
        )
        ar = _codex_result_to_agent_result(cr, "o4-mini")
        assert ar.session_id == "thread-1"
        assert ar.context_fill_pct is None
        assert ar.extra["thread_id"] == "thread-1"
        assert ar.extra["cache_read_input_tokens"] == 200
        # Cost should be calculated from pricing table
        assert ar.cost_usd > 0.0

    def test_unknown_model_cost_zero(self):
        cr = CodexResult(
            thread_id="t",
            num_turns=1,
            duration_seconds=1.0,
            result_text="",
            input_tokens=100,
            output_tokens=50,
            cached_input_tokens=0,
            model="unknown-model",
        )
        ar = _codex_result_to_agent_result(cr, "unknown-model")
        assert ar.cost_usd == 0.0

    def test_no_model_fallback(self):
        cr = CodexResult(
            thread_id="t",
            num_turns=1,
            duration_seconds=1.0,
            result_text="",
            input_tokens=1_000_000,
            output_tokens=0,
            cached_input_tokens=0,
            model="",
        )
        ar = _codex_result_to_agent_result(cr, None)
        # Falls back to DEFAULT_CODEX_MODEL ("o4-mini") → $1.10/M input
        assert ar.cost_usd == pytest.approx(1.10)


# ---------------------------------------------------------------------------
# calculate_cost() method tests
# ---------------------------------------------------------------------------


class TestClaudeCalculateCost:
    def test_returns_result_cost_usd(self):
        adapter = ClaudeAdapter()
        result = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="", cost_usd=0.42,
        )
        assert adapter.calculate_cost(result) == 0.42

    def test_zero_cost(self):
        adapter = ClaudeAdapter()
        result = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="", cost_usd=0.0,
        )
        assert adapter.calculate_cost(result) == 0.0


class TestCodexCalculateCost:
    def test_uses_model_from_extra(self):
        adapter = CodexAdapter()
        result = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="",
            input_tokens=1_000_000, output_tokens=1_000_000,
            extra={"model": "o4-mini", "cache_read_input_tokens": 0},
        )
        # 1M input @ $1.10 + 1M output @ $4.40 = $5.50
        assert adapter.calculate_cost(result) == pytest.approx(5.50)

    def test_falls_back_to_constructor_model(self):
        adapter = CodexAdapter(model="o3")
        result = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="",
            input_tokens=1_000_000, output_tokens=0,
            extra={"model": "", "cache_read_input_tokens": 0},
        )
        # 1M input @ $2.00/M = $2.00
        assert adapter.calculate_cost(result) == pytest.approx(2.00)

    def test_falls_back_to_default_model(self):
        adapter = CodexAdapter()
        result = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="",
            input_tokens=1_000_000, output_tokens=0,
            extra={"model": "", "cache_read_input_tokens": 0},
        )
        # Falls back to DEFAULT_CODEX_MODEL ("o4-mini") → $1.10/M input
        assert adapter.calculate_cost(result) == pytest.approx(1.10)

    def test_unknown_model_returns_zero(self):
        adapter = CodexAdapter()
        result = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="",
            input_tokens=1_000, output_tokens=500,
            extra={"model": "unknown-xyz", "cache_read_input_tokens": 0},
        )
        assert adapter.calculate_cost(result) == 0.0

    def test_cached_tokens_reduce_cost(self):
        adapter = CodexAdapter()
        full = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="",
            input_tokens=100_000, output_tokens=10_000,
            extra={"model": "o4-mini", "cache_read_input_tokens": 0},
        )
        cached = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="",
            input_tokens=100_000, output_tokens=10_000,
            extra={"model": "o4-mini", "cache_read_input_tokens": 50_000},
        )
        assert adapter.calculate_cost(cached) < adapter.calculate_cost(full)
