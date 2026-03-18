"""Tests for the adapter registry and registry-based stage dispatch (SMA-460).

Tests cover:
- build_adapter_registry() returns valid registry with claude/codex entries
- _execute_stage() resolves adapters from the registry
- _execute_stage() raises clear errors for missing adapters / missing registry
- _record_stage_run() works with AgentResult (via StageResult.agent_result)
- StageResult backward-compat properties (claude_result, codex_result)
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.agent import (
    AdapterRegistry,
    AgentAdapter,
    AgentResult,
    build_adapter_registry,
)
from botfarm.agent_claude import ClaudeAdapter
from botfarm.agent_codex import CodexAdapter
from botfarm.codex import CodexResult
from botfarm.db import get_stage_runs, insert_task
from botfarm.worker import (
    StageResult,
    _record_stage_run,
)
from botfarm.worker_claude import ClaudeResult
from botfarm.workflow import StageTemplate
from tests.helpers import (
    claude_result_to_agent,
    codex_result_to_agent,
    make_agent_result,
    make_claude_result,
    make_codex_result,
)


# ---------------------------------------------------------------------------
# build_adapter_registry
# ---------------------------------------------------------------------------


class TestBuildAdapterRegistry:
    def test_returns_dict(self):
        reg = build_adapter_registry()
        assert isinstance(reg, dict)

    def test_has_claude_and_codex(self):
        reg = build_adapter_registry()
        assert "claude" in reg
        assert "codex" in reg

    def test_claude_adapter_type(self):
        reg = build_adapter_registry()
        assert isinstance(reg["claude"], ClaudeAdapter)
        assert isinstance(reg["claude"], AgentAdapter)

    def test_codex_adapter_type(self):
        reg = build_adapter_registry()
        assert isinstance(reg["codex"], CodexAdapter)
        assert isinstance(reg["codex"], AgentAdapter)

    def test_codex_model_passed(self):
        reg = build_adapter_registry(codex_model="o3")
        adapter = reg["codex"]
        assert adapter._model == "o3"

    def test_codex_reasoning_effort_passed(self):
        reg = build_adapter_registry(codex_reasoning_effort="low")
        adapter = reg["codex"]
        assert adapter._reasoning_effort == "low"


# ---------------------------------------------------------------------------
# _execute_stage registry dispatch
# ---------------------------------------------------------------------------


class TestExecuteStageRegistryDispatch:
    def _make_stage_tpl(self, executor_type="claude", name="implement"):
        return StageTemplate(
            id=1,
            name=name,
            stage_order=1,
            executor_type=executor_type,
            identity="coder",
            prompt_template="Do {ticket_id}",
            max_turns=100,
            timeout_minutes=60,
            shell_command=None,
            result_parser="pr_url",
        )

    @patch("botfarm.worker_stages._run_agent_stage")
    def test_dispatches_to_correct_adapter(self, mock_run, tmp_path):
        from botfarm.worker_stages import _execute_stage

        registry = build_adapter_registry()
        mock_run.return_value = StageResult(
            stage="implement", success=True,
            agent_result=make_agent_result(),
        )

        _execute_stage(
            "implement",
            ticket_id="SMA-1",
            pr_url=None,
            cwd=tmp_path,
            max_turns=100,
            pr_checks_timeout=600,
            stage_tpl=self._make_stage_tpl("claude"),
            registry=registry,
        )

        assert mock_run.called
        # Second positional arg is the adapter
        adapter = mock_run.call_args[0][1]
        assert adapter.name == "claude"

    def test_missing_registry_raises(self, tmp_path):
        from botfarm.worker_stages import _execute_stage

        with pytest.raises(ValueError, match="Adapter registry required"):
            _execute_stage(
                "implement",
                ticket_id="SMA-1",
                pr_url=None,
                cwd=tmp_path,
                max_turns=100,
                pr_checks_timeout=600,
                stage_tpl=self._make_stage_tpl("claude"),
                # registry intentionally omitted
            )

    def test_missing_adapter_raises(self, tmp_path):
        from botfarm.worker_stages import _execute_stage

        registry: AdapterRegistry = {"claude": ClaudeAdapter()}
        with pytest.raises(ValueError, match="No adapter registered"):
            _execute_stage(
                "implement",
                ticket_id="SMA-1",
                pr_url=None,
                cwd=tmp_path,
                max_turns=100,
                pr_checks_timeout=600,
                stage_tpl=self._make_stage_tpl("unknown_agent"),
                registry=registry,
            )

    @patch("botfarm.worker_stages.subprocess.run")
    def test_shell_executor_no_registry_needed(self, mock_run, tmp_path):
        """shell executor doesn't require a registry."""
        from botfarm.worker_stages import _execute_stage
        import subprocess

        tpl = self._make_stage_tpl("shell", name="pr_checks")
        mock_run.return_value = subprocess.CompletedProcess(
            args=["gh"], returncode=0, stdout="OK", stderr="",
        )
        result = _execute_stage(
            "pr_checks",
            ticket_id="SMA-1",
            pr_url="https://github.com/o/r/pull/1",
            cwd=tmp_path,
            max_turns=100,
            pr_checks_timeout=600,
            stage_tpl=tpl,
            # registry intentionally omitted
        )
        assert result.success is True


# ---------------------------------------------------------------------------
# _record_stage_run with AgentResult
# ---------------------------------------------------------------------------


class TestRecordStageRunWithAgentResult:
    def test_insert_with_agent_result(self, conn, task_id):
        """_record_stage_run inserts a row from AgentResult fields."""
        ar = AgentResult(
            session_id="sess-test",
            num_turns=7,
            duration_seconds=25.0,
            result_text="done",
            input_tokens=3000,
            output_tokens=1500,
            cost_usd=0.08,
            context_fill_pct=55.0,
            extra={
                "exit_subtype": "end_turn",
                "cache_read_input_tokens": 400,
                "cache_creation_input_tokens": 200,
                "model_usage_json": '{"opus": {}}',
            },
        )
        sr = StageResult(stage="implement", success=True, agent_result=ar)
        _record_stage_run(
            conn,
            task_id=task_id,
            stage="implement",
            result=sr,
            wall_elapsed=30.0,
        )

        rows = get_stage_runs(conn, task_id)
        assert len(rows) == 1
        row = rows[0]
        assert row["session_id"] == "sess-test"
        assert row["turns"] == 7
        assert row["input_tokens"] == 3000
        assert row["output_tokens"] == 1500
        assert row["total_cost_usd"] == pytest.approx(0.08)
        assert row["context_fill_pct"] == pytest.approx(55.0)
        assert row["exit_subtype"] == "end_turn"
        assert row["cache_read_input_tokens"] == 400
        assert row["cache_creation_input_tokens"] == 200
        assert row["model_usage_json"] == '{"opus": {}}'

    def test_insert_with_no_agent_result(self, conn, task_id):
        """_record_stage_run handles None agent_result (shell/internal stages)."""
        sr = StageResult(stage="pr_checks", success=True, agent_result=None)
        _record_stage_run(
            conn,
            task_id=task_id,
            stage="pr_checks",
            result=sr,
            wall_elapsed=45.0,
        )

        rows = get_stage_runs(conn, task_id)
        assert len(rows) == 1
        row = rows[0]
        assert row["session_id"] is None
        assert row["turns"] == 0
        assert row["duration_seconds"] == pytest.approx(45.0)
        assert row["input_tokens"] == 0

    def test_insert_codex_agent_result(self, conn, task_id):
        """_record_stage_run correctly extracts Codex-specific extra fields."""
        codex_ar = codex_result_to_agent(make_codex_result("done"))
        sr = StageResult(stage="implement", success=True, agent_result=codex_ar)
        _record_stage_run(
            conn,
            task_id=task_id,
            stage="implement",
            result=sr,
            wall_elapsed=20.0,
        )

        rows = get_stage_runs(conn, task_id)
        assert len(rows) == 1
        row = rows[0]
        assert row["session_id"] == "t-test"
        assert row["turns"] == 3
        # Codex results have cost_usd set during normalization
        assert row["total_cost_usd"] >= 0


# ---------------------------------------------------------------------------
# StageResult backward-compat properties
# ---------------------------------------------------------------------------


class TestStageResultBackwardCompat:
    def test_claude_result_property_roundtrip(self):
        """agent_result → claude_result property preserves key fields."""
        ar = AgentResult(
            session_id="sess-1",
            num_turns=10,
            duration_seconds=30.0,
            result_text="all done",
            input_tokens=5000,
            output_tokens=2000,
            cost_usd=0.10,
            context_fill_pct=65.0,
            extra={
                "exit_subtype": "end_turn",
                "cache_read_input_tokens": 300,
                "cache_creation_input_tokens": 150,
                "model_usage_json": '{"model": {}}',
            },
        )
        sr = StageResult(stage="implement", success=True, agent_result=ar)
        cr = sr.claude_result

        assert cr is not None
        assert cr.session_id == "sess-1"
        assert cr.num_turns == 10
        assert cr.duration_seconds == 30.0
        assert cr.result_text == "all done"
        assert cr.input_tokens == 5000
        assert cr.output_tokens == 2000
        assert cr.total_cost_usd == 0.10
        assert cr.context_fill_pct == 65.0
        assert cr.exit_subtype == "end_turn"
        assert cr.cache_read_input_tokens == 300
        assert cr.cache_creation_input_tokens == 150
        assert cr.model_usage_json == '{"model": {}}'

    def test_claude_result_none_when_no_agent_result(self):
        sr = StageResult(stage="pr_checks", success=True)
        assert sr.claude_result is None

    def test_codex_result_property_roundtrip(self):
        """secondary_agent_result → codex_result property preserves key fields."""
        codex_cr = make_codex_result("output text", thread_id="thread-xyz")
        ar = codex_result_to_agent(codex_cr)
        sr = StageResult(
            stage="review", success=True,
            agent_result=make_agent_result(),
            secondary_agent_result=ar,
        )
        cr = sr.codex_result

        assert cr is not None
        assert cr.thread_id == "thread-xyz"
        assert cr.num_turns == 3
        assert cr.result_text == "output text"

    def test_codex_result_none_when_no_secondary(self):
        sr = StageResult(stage="review", success=True, agent_result=make_agent_result())
        assert sr.codex_result is None


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
