"""Tests for model/effort columns, Claude adapter changes, and model list caching."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.agent import AgentResult
from botfarm.agent_claude import ClaudeAdapter
from botfarm.models import (
    SEED_MODELS,
    CachedModel,
    _upsert_models,
    ensure_seed_data,
    get_cached_models,
    refresh_models,
)
from botfarm.worker_claude import ClaudeResult
from botfarm.workflow import (
    StageTemplate,
    create_pipeline,
    create_stage,
    duplicate_pipeline,
    load_pipeline_by_name,
    update_stage,
)


@pytest.fixture
def conn(tmp_path: Path) -> sqlite3.Connection:
    """Create an in-memory DB with all migrations applied."""
    from botfarm.db import init_db

    db_path = tmp_path / "test.db"
    c = init_db(str(db_path), allow_migration=True)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# StageTemplate model/effort round-trip through DB
# ---------------------------------------------------------------------------


class TestStageTemplateModelEffort:
    def test_create_stage_with_model_and_effort(self, conn):
        pid = create_pipeline(conn, "model_test")
        create_stage(
            conn, pid, "implement", 1, "claude",
            model="claude-opus-4-6",
            effort="high",
        )
        pipeline = load_pipeline_by_name(conn, "model_test")
        stage = pipeline.stages[0]
        assert stage.model == "claude-opus-4-6"
        assert stage.effort == "high"

    def test_create_stage_model_effort_default_none(self, conn):
        pid = create_pipeline(conn, "default_test")
        create_stage(conn, pid, "step", 1, "claude")
        pipeline = load_pipeline_by_name(conn, "default_test")
        stage = pipeline.stages[0]
        assert stage.model is None
        assert stage.effort is None

    def test_update_stage_model(self, conn):
        pid = create_pipeline(conn, "update_model_test")
        sid = create_stage(conn, pid, "step", 1, "claude")
        update_stage(conn, sid, model="claude-sonnet-4-6")
        pipeline = load_pipeline_by_name(conn, "update_model_test")
        assert pipeline.stages[0].model == "claude-sonnet-4-6"

    def test_update_stage_effort(self, conn):
        pid = create_pipeline(conn, "update_effort_test")
        sid = create_stage(conn, pid, "step", 1, "claude")
        update_stage(conn, sid, effort="low")
        pipeline = load_pipeline_by_name(conn, "update_effort_test")
        assert pipeline.stages[0].effort == "low"

    def test_update_stage_clear_model(self, conn):
        pid = create_pipeline(conn, "clear_model_test")
        sid = create_stage(conn, pid, "step", 1, "claude", model="claude-opus-4-6")
        update_stage(conn, sid, model=None)
        pipeline = load_pipeline_by_name(conn, "clear_model_test")
        assert pipeline.stages[0].model is None

    def test_duplicate_pipeline_copies_model_effort(self, conn):
        pid = create_pipeline(conn, "dup_src")
        create_stage(
            conn, pid, "step", 1, "claude",
            model="claude-opus-4-6", effort="max",
        )
        new_id = duplicate_pipeline(conn, pid, "dup_dst")
        copy = load_pipeline_by_name(conn, "dup_dst")
        assert copy.stages[0].model == "claude-opus-4-6"
        assert copy.stages[0].effort == "max"


# ---------------------------------------------------------------------------
# run_claude_streaming builds correct command with --model/--effort flags
# ---------------------------------------------------------------------------


class TestClaudeStreamingModelEffort:
    @patch("botfarm.worker_claude.subprocess.Popen")
    @patch("botfarm.worker_claude.shutil.which", return_value="/usr/bin/claude")
    def test_model_flag_in_cmd(self, mock_which, mock_popen, tmp_path):
        """When model is set, --model <model> appears in the command."""
        from botfarm.worker_claude import run_claude_streaming

        # Set up mock process
        proc = MagicMock()
        proc.stdin = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter([])
        proc.wait.return_value = 0
        proc.returncode = 0
        mock_popen.return_value = proc

        with pytest.raises(RuntimeError):
            # Will fail because no result message, but we only care about cmd
            run_claude_streaming(
                "test", cwd=tmp_path, max_turns=10,
                model="claude-sonnet-4-6",
            )

        cmd = mock_popen.call_args[0][0]
        assert "--model" in cmd
        idx = cmd.index("--model")
        assert cmd[idx + 1] == "claude-sonnet-4-6"

    @patch("botfarm.worker_claude.subprocess.Popen")
    @patch("botfarm.worker_claude.shutil.which", return_value="/usr/bin/claude")
    def test_effort_flag_in_cmd(self, mock_which, mock_popen, tmp_path):
        """When effort is set, --effort <effort> appears in the command."""
        from botfarm.worker_claude import run_claude_streaming

        proc = MagicMock()
        proc.stdin = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter([])
        proc.wait.return_value = 0
        proc.returncode = 0
        mock_popen.return_value = proc

        with pytest.raises(RuntimeError):
            run_claude_streaming(
                "test", cwd=tmp_path, max_turns=10,
                effort="low",
            )

        cmd = mock_popen.call_args[0][0]
        assert "--effort" in cmd
        idx = cmd.index("--effort")
        assert cmd[idx + 1] == "low"

    @patch("botfarm.worker_claude.subprocess.Popen")
    @patch("botfarm.worker_claude.shutil.which", return_value="/usr/bin/claude")
    def test_no_flags_when_none(self, mock_which, mock_popen, tmp_path):
        """When model/effort are None, neither flag appears in the command."""
        from botfarm.worker_claude import run_claude_streaming

        proc = MagicMock()
        proc.stdin = MagicMock()
        proc.stdout = iter([])
        proc.stderr = iter([])
        proc.wait.return_value = 0
        proc.returncode = 0
        mock_popen.return_value = proc

        with pytest.raises(RuntimeError):
            run_claude_streaming("test", cwd=tmp_path, max_turns=10)

        cmd = mock_popen.call_args[0][0]
        assert "--model" not in cmd
        assert "--effort" not in cmd


# ---------------------------------------------------------------------------
# ClaudeAdapter passes model/effort through
# ---------------------------------------------------------------------------


class TestClaudeAdapterModelEffort:
    def test_supports_model_override_is_true(self):
        adapter = ClaudeAdapter()
        assert adapter.supports_model_override is True

    @patch("botfarm.agent_claude.run_claude_streaming")
    def test_run_passes_model_and_effort(self, mock_run, tmp_path):
        mock_run.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="end_turn", result_text="done",
        )

        adapter = ClaudeAdapter()
        adapter.run(
            "test", cwd=tmp_path, max_turns=10,
            model="claude-sonnet-4-6", effort="high",
        )

        _, kwargs = mock_run.call_args
        assert kwargs["model"] == "claude-sonnet-4-6"
        assert kwargs["effort"] == "high"

    @patch("botfarm.agent_claude.run_claude_streaming")
    def test_run_passes_none_by_default(self, mock_run, tmp_path):
        mock_run.return_value = ClaudeResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            exit_subtype="end_turn", result_text="done",
        )

        adapter = ClaudeAdapter()
        adapter.run("test", cwd=tmp_path)

        _, kwargs = mock_run.call_args
        assert kwargs["model"] is None
        assert kwargs["effort"] is None


# ---------------------------------------------------------------------------
# _run_agent_stage passes model/effort to adapter
# ---------------------------------------------------------------------------


class TestRunAgentStageModelEffort:
    def _make_stage_tpl(self, model=None, effort=None):
        return StageTemplate(
            id=1, name="implement", stage_order=1,
            executor_type="claude", identity="coder",
            prompt_template="Do {ticket_id}",
            max_turns=100, timeout_minutes=60,
            shell_command=None, result_parser="pr_url",
            model=model, effort=effort,
        )

    def test_passes_model_effort_to_adapter(self, tmp_path):
        from botfarm.worker_stages import _run_agent_stage

        mock_adapter = MagicMock()
        mock_adapter.name = "claude"
        mock_adapter.supports_max_turns = True
        mock_adapter.supports_context_fill = False
        mock_adapter.supports_model_override = True
        mock_adapter.run.return_value = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="PR: https://github.com/o/r/pull/1",
        )

        stage_tpl = self._make_stage_tpl(model="claude-sonnet-4-6", effort="low")
        _run_agent_stage(
            stage_tpl, mock_adapter,
            cwd=tmp_path, max_turns=100,
            prompt_vars={"ticket_id": "SMA-1"},
        )

        _, kwargs = mock_adapter.run.call_args
        assert kwargs["model"] == "claude-sonnet-4-6"
        assert kwargs["effort"] == "low"

    def test_model_none_when_adapter_does_not_support(self, tmp_path):
        from botfarm.worker_stages import _run_agent_stage

        mock_adapter = MagicMock()
        mock_adapter.name = "codex"
        mock_adapter.supports_max_turns = False
        mock_adapter.supports_context_fill = False
        mock_adapter.supports_model_override = False
        mock_adapter.run.return_value = AgentResult(
            session_id="s", num_turns=1, duration_seconds=1.0,
            result_text="done",
        )

        stage_tpl = self._make_stage_tpl(model="claude-opus-4-6", effort="max")
        _run_agent_stage(
            stage_tpl, mock_adapter,
            cwd=tmp_path, max_turns=100,
            prompt_vars={"ticket_id": "SMA-1"},
        )

        _, kwargs = mock_adapter.run.call_args
        assert kwargs["model"] is None  # model suppressed when not supported
        assert kwargs["effort"] is None  # effort suppressed when not supported


# ---------------------------------------------------------------------------
# Model list caching (botfarm/models.py)
# ---------------------------------------------------------------------------


class TestModelCaching:
    def test_ensure_seed_data_populates_empty_table(self, conn):
        ensure_seed_data(conn)
        models = get_cached_models(conn)
        assert len(models) == len(SEED_MODELS)
        ids = {m.id for m in models}
        assert "claude-opus-4-6" in ids

    def test_ensure_seed_data_noop_when_populated(self, conn):
        ensure_seed_data(conn)
        # Insert another model manually
        conn.execute(
            "INSERT INTO available_models (id, display_name, max_input_tokens, "
            "max_output_tokens, executor_type) VALUES (?, ?, ?, ?, ?)",
            ("custom-model", "Custom", 100000, 4096, "claude"),
        )
        conn.commit()
        count_before = conn.execute("SELECT COUNT(*) AS cnt FROM available_models").fetchone()["cnt"]
        ensure_seed_data(conn)
        count_after = conn.execute("SELECT COUNT(*) AS cnt FROM available_models").fetchone()["cnt"]
        assert count_before == count_after

    def test_get_cached_models_filters_by_executor_type(self, conn):
        ensure_seed_data(conn)
        # All seed models are executor_type=claude
        models = get_cached_models(conn, executor_type="codex")
        assert len(models) == 0
        models = get_cached_models(conn, executor_type="claude")
        assert len(models) > 0

    def test_cached_model_has_correct_fields(self, conn):
        ensure_seed_data(conn)
        models = get_cached_models(conn)
        opus = next(m for m in models if m.id == "claude-opus-4-6")
        assert opus.display_name == "Claude Opus 4.6"
        assert opus.max_input_tokens == 1_000_000
        assert opus.max_output_tokens == 32_000
        assert opus.supported_efforts == ["low", "medium", "high", "max"]
        assert opus.executor_type == "claude"
        assert opus.is_alias is False

    @patch("botfarm.models.httpx.get")
    def test_refresh_models_from_api(self, mock_get, conn):
        mock_get.return_value = MagicMock(
            status_code=200,
            json=lambda: {
                "data": [
                    {
                        "id": "claude-test-model",
                        "display_name": "Test Model",
                        "max_input_tokens": 50000,
                        "max_output_tokens": 4096,
                        "capabilities": {
                            "extended_thinking": True,
                        },
                    },
                ],
                "has_more": False,
            },
        )
        mock_get.return_value.raise_for_status = MagicMock()

        models = refresh_models(conn, "test-api-key")
        assert any(m.id == "claude-test-model" for m in models)
        test_model = next(m for m in models if m.id == "claude-test-model")
        assert test_model.display_name == "Test Model"
        assert test_model.supported_efforts == ["low", "medium", "high", "max"]

    @patch("botfarm.models.httpx.get")
    def test_refresh_models_pagination(self, mock_get, conn):
        """Verify pagination follows has_more/after_id."""
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if call_count == 1:
                resp.json.return_value = {
                    "data": [{"id": "model-a", "display_name": "A",
                              "max_input_tokens": 100, "max_output_tokens": 50}],
                    "has_more": True,
                }
            else:
                resp.json.return_value = {
                    "data": [{"id": "model-b", "display_name": "B",
                              "max_input_tokens": 200, "max_output_tokens": 100}],
                    "has_more": False,
                }
            return resp

        mock_get.side_effect = side_effect
        models = refresh_models(conn, "key")
        assert call_count == 2
        ids = {m.id for m in models}
        assert "model-a" in ids
        assert "model-b" in ids

    def test_upsert_models_replaces_existing(self, conn):
        """Upsert should update existing rows rather than fail on conflict."""
        _upsert_models(conn, [
            {"id": "m1", "display_name": "V1", "max_input_tokens": 100,
             "max_output_tokens": 50, "supported_efforts": None,
             "executor_type": "claude", "is_alias": 0},
        ])
        _upsert_models(conn, [
            {"id": "m1", "display_name": "V2", "max_input_tokens": 200,
             "max_output_tokens": 100, "supported_efforts": None,
             "executor_type": "claude", "is_alias": 0},
        ])
        models = get_cached_models(conn)
        m1 = next(m for m in models if m.id == "m1")
        assert m1.display_name == "V2"
        assert m1.max_input_tokens == 200
