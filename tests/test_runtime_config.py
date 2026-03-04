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
    """sync_agent_config_to_db verifies all keys written."""

    def test_all_keys_written(self, conn):
        agents = AgentsConfig(
            max_review_iterations=4,
            max_ci_retries=3,
            max_merge_conflict_retries=5,
            codex_reviewer_enabled=True,
            codex_reviewer_model="o3",
            codex_reviewer_timeout_minutes=20,
            codex_reviewer_skip_on_reiteration=False,
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
        assert result["codex_reviewer_skip_on_reiteration"] is False


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
        ctx = _make_ctx(conn, max_review_iterations=3, max_ci_retries=2, max_merge_conflict_retries=2)

        write_runtime_config_batch(conn, {
            "max_review_iterations": 5,
            "max_ci_retries": 4,
            "max_merge_conflict_retries": 5,
            "codex_reviewer_enabled": True,
            "codex_reviewer_model": "o3",
            "codex_reviewer_timeout_minutes": 30,
            "codex_reviewer_skip_on_reiteration": False,
        })

        ctx._refresh_runtime_config()

        assert ctx.max_review_iterations == 5
        assert ctx.max_ci_retries == 4
        assert ctx.max_merge_conflict_retries == 5
        assert ctx.codex_config.enabled is True
        assert ctx.codex_config.model == "o3"
        assert ctx.codex_config.timeout_minutes == 30
        assert ctx.codex_config.skip_on_reiteration is False

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

    def test_non_refreshable_field_not_overwritten(self, conn):
        """Fields not in _refreshable_limits are preserved (pipeline-fixed loops)."""
        ctx = _make_ctx(
            conn,
            max_review_iterations=7,
            max_ci_retries=2,
            _refreshable_limits=frozenset({"max_ci_retries", "max_merge_conflict_retries"}),
        )

        write_runtime_config_batch(conn, {
            "max_review_iterations": 3,
            "max_ci_retries": 5,
        })

        ctx._refresh_runtime_config()

        # max_review_iterations is NOT refreshable → stays at pipeline value
        assert ctx.max_review_iterations == 7
        # max_ci_retries IS refreshable → updated
        assert ctx.max_ci_retries == 5

    def test_merge_conflict_retries_refreshed(self, conn):
        ctx = _make_ctx(conn, max_merge_conflict_retries=2)

        write_runtime_config(conn, "max_merge_conflict_retries", 4)
        conn.commit()

        ctx._refresh_runtime_config()

        assert ctx.max_merge_conflict_retries == 4

    def test_malformed_int_value_skipped(self, conn):
        """Malformed DB values (e.g. 'abc' for an int field) are skipped without crashing."""
        ctx = _make_ctx(conn, max_review_iterations=3, max_ci_retries=2)

        write_runtime_config(conn, "max_review_iterations", "abc")
        write_runtime_config(conn, "max_ci_retries", 5)
        conn.commit()

        ctx._refresh_runtime_config()

        # Malformed value skipped — stays at original
        assert ctx.max_review_iterations == 3
        # Valid value still applied
        assert ctx.max_ci_retries == 5

    def test_malformed_codex_value_skipped(self, conn):
        """Malformed codex config values are skipped without crashing."""
        ctx = _make_ctx(
            conn,
            codex_config=_CodexReviewerConfig(enabled=False, model="", timeout_minutes=15),
        )

        write_runtime_config(conn, "codex_reviewer_timeout_minutes", "not_a_number")
        conn.commit()

        ctx._refresh_runtime_config()

        # Malformed value skipped — stays at original
        assert ctx.codex_config.timeout_minutes == 15


class TestComputeRefreshableLimits:
    """_compute_refreshable_limits respects pipeline loop config_key."""

    def test_no_pipeline_all_refreshable(self):
        from botfarm.worker import _compute_refreshable_limits

        result = _compute_refreshable_limits(None)
        assert result == frozenset({
            "max_review_iterations", "max_ci_retries", "max_merge_conflict_retries",
        })

    def test_loop_with_config_key_stays_refreshable(self):
        from botfarm.worker import _compute_refreshable_limits
        from botfarm.workflow import PipelineTemplate, StageLoop, StageTemplate

        tpl = PipelineTemplate(
            id=1, name="test", description=None, ticket_label=None, is_default=False,
            stages=[StageTemplate(
                id=1, name="review", stage_order=0, executor_type="claude",
                identity=None, prompt_template="", max_turns=5,
                timeout_minutes=None, shell_command=None, result_parser=None,
            )],
            loops=[StageLoop(
                id=1, name="review_loop", start_stage="review", end_stage="fix",
                max_iterations=3, config_key="max_review_iterations",
                exit_condition=None, on_failure_stage=None,
            )],
        )
        result = _compute_refreshable_limits(tpl)
        assert "max_review_iterations" in result

    def test_loop_with_no_config_key_not_refreshable(self):
        from botfarm.worker import _compute_refreshable_limits
        from botfarm.workflow import PipelineTemplate, StageLoop, StageTemplate

        tpl = PipelineTemplate(
            id=1, name="test", description=None, ticket_label=None, is_default=False,
            stages=[StageTemplate(
                id=1, name="review", stage_order=0, executor_type="claude",
                identity=None, prompt_template="", max_turns=5,
                timeout_minutes=None, shell_command=None, result_parser=None,
            )],
            loops=[StageLoop(
                id=1, name="review_loop", start_stage="review", end_stage="fix",
                max_iterations=7, config_key=None,
                exit_condition=None, on_failure_stage=None,
            )],
        )
        result = _compute_refreshable_limits(tpl)
        assert "max_review_iterations" not in result
        # Other fields still refreshable
        assert "max_ci_retries" in result
        assert "max_merge_conflict_retries" in result

    def test_loop_with_custom_config_key_not_refreshable(self):
        """A loop with an arbitrary config_key that doesn't match the runtime field is not refreshable."""
        from botfarm.worker import _compute_refreshable_limits
        from botfarm.workflow import PipelineTemplate, StageLoop, StageTemplate

        tpl = PipelineTemplate(
            id=1, name="test", description=None, ticket_label=None, is_default=False,
            stages=[StageTemplate(
                id=1, name="review", stage_order=0, executor_type="claude",
                identity=None, prompt_template="", max_turns=5,
                timeout_minutes=None, shell_command=None, result_parser=None,
            )],
            loops=[StageLoop(
                id=1, name="review_loop", start_stage="review", end_stage="fix",
                max_iterations=5, config_key="custom_review_budget",
                exit_condition=None, on_failure_stage=None,
            )],
        )
        result = _compute_refreshable_limits(tpl)
        # custom_review_budget != max_review_iterations → not refreshable
        assert "max_review_iterations" not in result
        # Other fields still refreshable
        assert "max_ci_retries" in result
        assert "max_merge_conflict_retries" in result
