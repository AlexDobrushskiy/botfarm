"""Tests for botfarm.workflow — pipeline loading and prompt rendering."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from botfarm.db import init_db
from botfarm.workflow import (
    PipelineTemplate,
    StageLoop,
    StageTemplate,
    get_loop_for_stage,
    get_stage,
    load_pipeline,
    load_pipeline_by_name,
    render_prompt,
    resolve_max_iterations,
)


@pytest.fixture()
def conn(tmp_path):
    """Yield a fresh DB with seed data."""
    db_file = tmp_path / "test.db"
    connection = init_db(db_file, allow_migration=True)
    yield connection
    connection.close()


# ---------------------------------------------------------------------------
# load_pipeline — label matching
# ---------------------------------------------------------------------------


class TestLoadPipeline:
    def test_default_pipeline_with_no_labels(self, conn):
        pipeline = load_pipeline(conn, [])
        assert pipeline.name == "implementation"
        assert pipeline.is_default is True

    def test_default_pipeline_with_empty_labels(self, conn):
        pipeline = load_pipeline(conn, [])
        assert pipeline.name == "implementation"

    def test_investigation_label_selects_investigation_pipeline(self, conn):
        pipeline = load_pipeline(conn, ["Investigation"])
        assert pipeline.name == "investigation"
        assert pipeline.ticket_label == "Investigation"

    def test_investigation_label_case_insensitive(self, conn):
        pipeline = load_pipeline(conn, ["investigation"])
        assert pipeline.name == "investigation"

    def test_unrecognized_label_falls_back_to_default(self, conn):
        pipeline = load_pipeline(conn, ["SomeRandomLabel"])
        assert pipeline.name == "implementation"
        assert pipeline.is_default is True

    def test_multiple_labels_with_investigation(self, conn):
        pipeline = load_pipeline(conn, ["Bug", "Investigation", "Priority"])
        assert pipeline.name == "investigation"

    def test_loads_stages_in_order(self, conn):
        pipeline = load_pipeline(conn, [])
        stage_names = [s.name for s in pipeline.stages]
        assert stage_names == ["implement", "review", "fix", "pr_checks", "ci_fix", "merge"]

    def test_loads_loops(self, conn):
        pipeline = load_pipeline(conn, [])
        loop_names = [lp.name for lp in pipeline.loops]
        assert "review_loop" in loop_names
        assert "ci_retry_loop" in loop_names

    def test_no_default_pipeline_raises(self, conn):
        conn.execute("UPDATE pipeline_templates SET is_default = 0")
        conn.commit()
        with pytest.raises(RuntimeError, match="No matching pipeline found"):
            load_pipeline(conn, [])

    def test_investigation_pipeline_has_single_stage(self, conn):
        pipeline = load_pipeline(conn, ["Investigation"])
        assert len(pipeline.stages) == 1
        assert pipeline.stages[0].name == "implement"

    def test_investigation_pipeline_has_no_loops(self, conn):
        pipeline = load_pipeline(conn, ["Investigation"])
        assert pipeline.loops == []


# ---------------------------------------------------------------------------
# load_pipeline_by_name
# ---------------------------------------------------------------------------


class TestLoadPipelineByName:
    def test_load_implementation(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        assert pipeline.name == "implementation"
        assert pipeline.is_default is True

    def test_load_investigation(self, conn):
        pipeline = load_pipeline_by_name(conn, "investigation")
        assert pipeline.name == "investigation"
        assert pipeline.ticket_label == "Investigation"

    def test_nonexistent_pipeline_raises(self, conn):
        with pytest.raises(ValueError, match="not found"):
            load_pipeline_by_name(conn, "nonexistent")


# ---------------------------------------------------------------------------
# Stage template fields
# ---------------------------------------------------------------------------


class TestStageTemplateFields:
    def test_implement_stage_fields(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "implement")
        assert stage.executor_type == "claude"
        assert stage.identity == "coder"
        assert stage.max_turns == 200
        assert stage.timeout_minutes == 120
        assert stage.result_parser == "pr_url"
        assert "{ticket_id}" in stage.prompt_template

    def test_review_stage_fields(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "review")
        assert stage.executor_type == "claude"
        assert stage.identity == "reviewer"
        assert stage.result_parser == "review_verdict"
        assert "{pr_url}" in stage.prompt_template

    def test_pr_checks_stage_fields(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "pr_checks")
        assert stage.executor_type == "shell"
        assert stage.identity is None
        assert stage.shell_command is not None
        assert "gh pr checks" in stage.shell_command
        assert stage.prompt_template is None

    def test_merge_stage_fields(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "merge")
        assert stage.executor_type == "internal"
        assert stage.identity is None
        assert stage.prompt_template is None


# ---------------------------------------------------------------------------
# render_prompt
# ---------------------------------------------------------------------------


class TestRenderPrompt:
    def test_render_with_ticket_id(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "implement")
        result = render_prompt(stage, ticket_id="SMA-100")
        assert "SMA-100" in result
        assert "{ticket_id}" not in result

    def test_render_with_pr_url(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "review")
        result = render_prompt(
            stage,
            pr_url="https://github.com/org/repo/pull/42",
            pr_number="42",
            owner="org",
            repo="repo",
        )
        assert "https://github.com/org/repo/pull/42" in result
        assert "{pr_url}" not in result

    def test_unknown_placeholders_left_unchanged(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "implement")
        result = render_prompt(stage, ticket_id="SMA-1")
        # The template doesn't have {unknown}, but other placeholders are fine
        assert "SMA-1" in result

    def test_ci_fix_with_failure_output(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "ci_fix")
        result = render_prompt(
            stage,
            pr_url="https://github.com/org/repo/pull/10",
            ci_failure_output="Error: tests failed",
        )
        assert "Error: tests failed" in result
        assert "{ci_failure_output}" not in result

    def test_no_prompt_template_raises(self):
        stage = StageTemplate(
            name="merge",
            stage_order=6,
            executor_type="internal",
            identity=None,
            prompt_template=None,
            max_turns=None,
            timeout_minutes=None,
            shell_command=None,
            result_parser=None,
        )
        with pytest.raises(ValueError, match="no prompt template"):
            render_prompt(stage)

    def test_missing_variables_left_as_placeholders(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "review")
        # Don't pass pr_url — it should be left as {pr_url}
        result = render_prompt(stage)
        assert "{pr_url}" in result


# ---------------------------------------------------------------------------
# get_stage
# ---------------------------------------------------------------------------


class TestGetStage:
    def test_get_existing_stage(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        stage = get_stage(pipeline, "review")
        assert stage.name == "review"

    def test_get_nonexistent_stage_raises(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        with pytest.raises(ValueError, match="not found"):
            get_stage(pipeline, "nonexistent")


# ---------------------------------------------------------------------------
# get_loop_for_stage
# ---------------------------------------------------------------------------


class TestGetLoopForStage:
    def test_review_is_in_review_loop(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        loop = get_loop_for_stage(pipeline, "review")
        assert loop is not None
        assert loop.name == "review_loop"
        assert loop.start_stage == "review"
        assert loop.end_stage == "fix"

    def test_fix_is_in_review_loop(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        loop = get_loop_for_stage(pipeline, "fix")
        assert loop is not None
        assert loop.name == "review_loop"

    def test_ci_fix_is_in_ci_retry_loop(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        loop = get_loop_for_stage(pipeline, "ci_fix")
        assert loop is not None
        assert loop.name == "ci_retry_loop"

    def test_implement_has_no_loop(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        loop = get_loop_for_stage(pipeline, "implement")
        assert loop is None

    def test_merge_has_no_loop(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        loop = get_loop_for_stage(pipeline, "merge")
        assert loop is None


# ---------------------------------------------------------------------------
# resolve_max_iterations
# ---------------------------------------------------------------------------


class TestResolveMaxIterations:
    def test_uses_config_key_override(self):
        @dataclass
        class FakeAgentsCfg:
            max_review_iterations: int = 5

        loop = StageLoop(
            name="review_loop",
            start_stage="review",
            end_stage="fix",
            max_iterations=3,
            config_key="max_review_iterations",
            exit_condition="review_approved",
            on_failure_stage=None,
        )
        assert resolve_max_iterations(loop, FakeAgentsCfg()) == 5

    def test_falls_back_to_loop_default(self):
        @dataclass
        class FakeAgentsCfg:
            pass  # no matching attribute

        loop = StageLoop(
            name="review_loop",
            start_stage="review",
            end_stage="fix",
            max_iterations=3,
            config_key="max_review_iterations",
            exit_condition="review_approved",
            on_failure_stage=None,
        )
        assert resolve_max_iterations(loop, FakeAgentsCfg()) == 3

    def test_no_config_key_uses_loop_default(self):
        @dataclass
        class FakeAgentsCfg:
            max_review_iterations: int = 10

        loop = StageLoop(
            name="some_loop",
            start_stage="a",
            end_stage="b",
            max_iterations=7,
            config_key=None,
            exit_condition=None,
            on_failure_stage=None,
        )
        assert resolve_max_iterations(loop, FakeAgentsCfg()) == 7

    def test_config_value_converted_to_int(self):
        class FakeAgentsCfg:
            max_ci_retries = "4"

        loop = StageLoop(
            name="ci_loop",
            start_stage="ci_fix",
            end_stage="pr_checks",
            max_iterations=2,
            config_key="max_ci_retries",
            exit_condition=None,
            on_failure_stage=None,
        )
        assert resolve_max_iterations(loop, FakeAgentsCfg()) == 4


# ---------------------------------------------------------------------------
# Loop seed data fields
# ---------------------------------------------------------------------------


class TestLoopSeedData:
    def test_review_loop_fields(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        loop = get_loop_for_stage(pipeline, "review")
        assert loop.max_iterations == 3
        assert loop.config_key == "max_review_iterations"
        assert loop.exit_condition == "review_approved"
        assert loop.on_failure_stage is None

    def test_ci_retry_loop_fields(self, conn):
        pipeline = load_pipeline_by_name(conn, "implementation")
        loop = get_loop_for_stage(pipeline, "ci_fix")
        assert loop.max_iterations == 2
        assert loop.config_key == "max_ci_retries"
        assert loop.exit_condition == "ci_passed"
        assert loop.on_failure_stage == "ci_fix"
