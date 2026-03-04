"""Tests for runtime_config DB table and sync helpers."""

from __future__ import annotations

from botfarm.config import AgentsConfig, sync_agent_config_to_db
from botfarm.db import (
    RUNTIME_CONFIG_KEYS,
    get_events,
    insert_task,
    read_runtime_config,
    write_runtime_config,
    write_runtime_config_batch,
)
from botfarm.worker import PipelineResult, _CodexReviewerConfig, _PipelineContext


class TestRuntimeConfigRoundTrip:
    """Round-trip for each type (bool, str, int)."""

    def test_bool_round_trip(self, conn):
        write_runtime_config(conn, "codex_reviewer_enabled", True)
        conn.commit()
        result = read_runtime_config(conn)
        assert result["codex_reviewer_enabled"] is True

    def test_str_round_trip(self, conn):
        write_runtime_config(conn, "codex_reviewer_model", "o3")
        conn.commit()
        result = read_runtime_config(conn)
        assert result["codex_reviewer_model"] == "o3"

    def test_int_round_trip(self, conn):
        write_runtime_config(conn, "max_review_iterations", 5)
        conn.commit()
        result = read_runtime_config(conn)
        assert result["max_review_iterations"] == 5


class TestRuntimeConfigUpsert:
    """Upsert behavior (write same key twice)."""

    def test_upsert_overwrites(self, conn):
        write_runtime_config(conn, "max_ci_retries", 2)
        conn.commit()
        write_runtime_config(conn, "max_ci_retries", 4)
        conn.commit()
        result = read_runtime_config(conn)
        assert result["max_ci_retries"] == 4


class TestRuntimeConfigBatch:
    """Batch write + read back."""

    def test_batch_write_and_read(self, conn):
        settings = {
            "max_review_iterations": 3,
            "max_ci_retries": 2,
            "max_merge_conflict_retries": 2,
            "codex_reviewer_enabled": False,
            "codex_reviewer_model": "o4-mini",
            "codex_reviewer_timeout_minutes": 15,
        }
        write_runtime_config_batch(conn, settings)
        result = read_runtime_config(conn)
        assert result == settings


class TestSyncAgentConfigToDb:
    """sync_agent_config_to_db verifies all 5 keys written."""

    def test_all_keys_written(self, conn):
        agents = AgentsConfig(
            max_review_iterations=4,
            max_ci_retries=3,
            max_merge_conflict_retries=5,
            codex_reviewer_enabled=True,
            codex_reviewer_model="o3",
            codex_reviewer_timeout_minutes=20,
        )
        sync_agent_config_to_db(conn, agents)
        result = read_runtime_config(conn)
        assert set(result.keys()) == RUNTIME_CONFIG_KEYS
        assert result["max_review_iterations"] == 4
        assert result["max_ci_retries"] == 3
        assert result["max_merge_conflict_retries"] == 5
        assert result["codex_reviewer_enabled"] is True
        assert result["codex_reviewer_model"] == "o3"
        assert result["codex_reviewer_timeout_minutes"] == 20


class TestRuntimeConfigEmpty:
    """Empty table returns empty dict."""

    def test_empty_table(self, conn):
        result = read_runtime_config(conn)
        assert result == {}


# ---------------------------------------------------------------------------
# _PipelineContext._refresh_runtime_config tests
# ---------------------------------------------------------------------------


def _make_ctx(conn, **overrides):
    """Build a minimal _PipelineContext for testing."""
    task_id = insert_task(
        conn,
        ticket_id="SMA-TEST",
        title="test",
        project="test",
        slot=1,
    )
    conn.commit()
    defaults = dict(
        ticket_id="SMA-TEST",
        ticket_labels=[],
        task_id=task_id,
        cwd="/tmp",
        conn=conn,
        turns_cfg={},
        pr_checks_timeout=600,
        pipeline=PipelineResult(ticket_id="SMA-TEST", success=False, stages_completed=[]),
    )
    defaults.update(overrides)
    return _PipelineContext(**defaults)


class TestRefreshRuntimeConfig:
    """_PipelineContext._refresh_runtime_config updates fields from DB."""

    def test_fields_update_when_db_changes(self, conn):
        ctx = _make_ctx(conn, max_review_iterations=3, max_ci_retries=2)

        write_runtime_config_batch(conn, {
            "max_review_iterations": 5,
            "max_ci_retries": 4,
            "codex_reviewer_enabled": True,
            "codex_reviewer_model": "o3",
            "codex_reviewer_timeout_minutes": 30,
        })

        ctx._refresh_runtime_config()

        assert ctx.max_review_iterations == 5
        assert ctx.max_ci_retries == 4
        assert ctx.codex_config.enabled is True
        assert ctx.codex_config.model == "o3"
        assert ctx.codex_config.timeout_minutes == 30

    def test_noop_when_table_empty(self, conn):
        ctx = _make_ctx(
            conn,
            max_review_iterations=3,
            max_ci_retries=2,
            codex_config=_CodexReviewerConfig(enabled=False, model="", timeout_minutes=15),
        )

        ctx._refresh_runtime_config()

        # All fields unchanged
        assert ctx.max_review_iterations == 3
        assert ctx.max_ci_retries == 2
        assert ctx.codex_config.enabled is False
        assert ctx.codex_config.model == ""
        assert ctx.codex_config.timeout_minutes == 15

    def test_partial_override(self, conn):
        ctx = _make_ctx(
            conn,
            max_review_iterations=3,
            max_ci_retries=2,
            codex_config=_CodexReviewerConfig(enabled=False, model="", timeout_minutes=15),
        )

        # Only write some keys
        write_runtime_config(conn, "max_review_iterations", 7)
        conn.commit()

        ctx._refresh_runtime_config()

        assert ctx.max_review_iterations == 7
        # Other fields stay at their original values
        assert ctx.max_ci_retries == 2
        assert ctx.codex_config.enabled is False

    def test_event_emitted_on_change(self, conn):
        ctx = _make_ctx(conn, max_review_iterations=3, max_ci_retries=2)

        write_runtime_config(conn, "max_review_iterations", 5)
        conn.commit()

        ctx._refresh_runtime_config()

        events = get_events(conn, task_id=ctx.task_id, event_type="runtime_config_refreshed")
        assert len(events) == 1
        assert "max_review_iterations: 3 -> 5" in events[0]["detail"]

    def test_no_event_when_unchanged(self, conn):
        ctx = _make_ctx(conn, max_review_iterations=3, max_ci_retries=2)

        # Write same values as defaults
        write_runtime_config_batch(conn, {
            "max_review_iterations": 3,
            "max_ci_retries": 2,
        })

        ctx._refresh_runtime_config()

        events = get_events(conn, task_id=ctx.task_id, event_type="runtime_config_refreshed")
        assert len(events) == 0
