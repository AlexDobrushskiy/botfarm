"""Tests for botfarm.workflow — pipeline loading, prompt rendering, and CRUD operations."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from botfarm.db import init_db
from botfarm.workflow import (
    PipelineTemplate,
    StageLoop,
    StageTemplate,
    create_loop,
    create_pipeline,
    create_stage,
    delete_loop,
    delete_pipeline,
    delete_stage,
    duplicate_pipeline,
    get_loop_for_stage,
    get_stage,
    load_all_pipelines,
    load_pipeline,
    load_pipeline_by_name,
    render_prompt,
    reorder_stages,
    resolve_max_iterations,
    update_loop,
    update_pipeline,
    update_stage,
    validate_pipeline,
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


# ---------------------------------------------------------------------------
# Pipeline CRUD — create_pipeline
# ---------------------------------------------------------------------------


class TestCreatePipeline:
    def test_create_basic_pipeline(self, conn):
        pid = create_pipeline(conn, "testing")
        pipeline = load_pipeline_by_name(conn, "testing")
        assert pipeline.id == pid
        assert pipeline.name == "testing"
        assert pipeline.is_default is False
        assert pipeline.description is None
        assert pipeline.ticket_label is None

    def test_create_pipeline_with_all_fields(self, conn):
        pid = create_pipeline(
            conn, "custom", description="A custom pipeline",
            ticket_label="Custom", is_default=False,
        )
        pipeline = load_pipeline_by_name(conn, "custom")
        assert pipeline.description == "A custom pipeline"
        assert pipeline.ticket_label == "Custom"

    def test_create_default_pipeline_unsets_existing_default(self, conn):
        # "implementation" is currently default
        pid = create_pipeline(conn, "new_default", is_default=True)
        new_pipe = load_pipeline_by_name(conn, "new_default")
        assert new_pipe.is_default is True
        old_pipe = load_pipeline_by_name(conn, "implementation")
        assert old_pipe.is_default is False

    def test_create_pipeline_returns_id(self, conn):
        pid = create_pipeline(conn, "test_id")
        assert isinstance(pid, int)
        assert pid > 0


# ---------------------------------------------------------------------------
# Pipeline CRUD — update_pipeline
# ---------------------------------------------------------------------------


class TestUpdatePipeline:
    def test_update_name(self, conn):
        pid = create_pipeline(conn, "orig")
        update_pipeline(conn, pid, name="renamed")
        pipeline = load_pipeline_by_name(conn, "renamed")
        assert pipeline.id == pid

    def test_update_description(self, conn):
        pid = create_pipeline(conn, "desc_test")
        update_pipeline(conn, pid, description="Updated desc")
        pipeline = load_pipeline_by_name(conn, "desc_test")
        assert pipeline.description == "Updated desc"

    def test_update_ticket_label(self, conn):
        pid = create_pipeline(conn, "label_test")
        update_pipeline(conn, pid, ticket_label="NewLabel")
        pipeline = load_pipeline_by_name(conn, "label_test")
        assert pipeline.ticket_label == "NewLabel"

    def test_update_is_default_true_unsets_others(self, conn):
        pid = create_pipeline(conn, "new_default_update")
        update_pipeline(conn, pid, is_default=True)
        pipeline = load_pipeline_by_name(conn, "new_default_update")
        assert pipeline.is_default is True
        old = load_pipeline_by_name(conn, "implementation")
        assert old.is_default is False

    def test_update_is_default_false(self, conn):
        # Get the implementation pipeline ID
        impl = load_pipeline_by_name(conn, "implementation")
        # First make another pipeline the default
        pid = create_pipeline(conn, "alt_default", is_default=True)
        update_pipeline(conn, impl.id, is_default=False)
        pipe = load_pipeline_by_name(conn, "implementation")
        assert pipe.is_default is False

    def test_update_with_no_valid_kwargs_is_noop(self, conn):
        pid = create_pipeline(conn, "noop_test")
        update_pipeline(conn, pid, invalid_field="ignored")
        pipeline = load_pipeline_by_name(conn, "noop_test")
        assert pipeline.name == "noop_test"

    def test_update_multiple_fields(self, conn):
        pid = create_pipeline(conn, "multi")
        update_pipeline(conn, pid, name="multi_updated", description="New desc")
        pipeline = load_pipeline_by_name(conn, "multi_updated")
        assert pipeline.description == "New desc"


# ---------------------------------------------------------------------------
# Pipeline CRUD — delete_pipeline
# ---------------------------------------------------------------------------


class TestDeletePipeline:
    def test_delete_non_default_pipeline(self, conn):
        pid = create_pipeline(conn, "to_delete")
        delete_pipeline(conn, pid)
        with pytest.raises(ValueError, match="not found"):
            load_pipeline_by_name(conn, "to_delete")

    def test_delete_cascades_stages_and_loops(self, conn):
        pid = create_pipeline(conn, "cascade_test")
        create_stage(conn, pid, "step1", 1, "claude")
        create_stage(conn, pid, "step2", 2, "claude")
        create_loop(conn, pid, "test_loop", "step1", "step2", 3)
        delete_pipeline(conn, pid)
        # Verify stages and loops are gone
        rows = conn.execute(
            "SELECT COUNT(*) AS cnt FROM stage_templates WHERE pipeline_id = ?",
            (pid,),
        ).fetchone()
        assert rows["cnt"] == 0
        rows = conn.execute(
            "SELECT COUNT(*) AS cnt FROM stage_loops WHERE pipeline_id = ?",
            (pid,),
        ).fetchone()
        assert rows["cnt"] == 0

    def test_delete_only_default_raises(self, conn):
        impl = load_pipeline_by_name(conn, "implementation")
        with pytest.raises(ValueError, match="Cannot delete the only default"):
            delete_pipeline(conn, impl.id)

    def test_delete_default_when_another_default_exists(self, conn):
        impl = load_pipeline_by_name(conn, "implementation")
        # Create another default
        pid2 = create_pipeline(conn, "alt_default", is_default=True)
        # Now the old implementation is no longer default, but pid2 is
        # Make implementation default again too by direct SQL
        conn.execute(
            "UPDATE pipeline_templates SET is_default = 1 WHERE id = ?",
            (impl.id,),
        )
        conn.commit()
        # Now both are default — deleting one should work
        delete_pipeline(conn, impl.id)
        with pytest.raises(ValueError, match="not found"):
            load_pipeline_by_name(conn, "implementation")

    def test_delete_nonexistent_raises(self, conn):
        with pytest.raises(ValueError, match="not found"):
            delete_pipeline(conn, 99999)


# ---------------------------------------------------------------------------
# Pipeline CRUD — duplicate_pipeline
# ---------------------------------------------------------------------------


class TestDuplicatePipeline:
    def test_duplicate_creates_copy(self, conn):
        impl = load_pipeline_by_name(conn, "implementation")
        new_id = duplicate_pipeline(conn, impl.id, "impl_copy")
        copy = load_pipeline_by_name(conn, "impl_copy")
        assert copy.id == new_id
        assert copy.name == "impl_copy"
        assert copy.is_default is False  # Never copies default flag

    def test_duplicate_copies_stages(self, conn):
        impl = load_pipeline_by_name(conn, "implementation")
        duplicate_pipeline(conn, impl.id, "impl_stages_copy")
        copy = load_pipeline_by_name(conn, "impl_stages_copy")
        assert len(copy.stages) == len(impl.stages)
        for orig, dup in zip(impl.stages, copy.stages):
            assert orig.name == dup.name
            assert orig.stage_order == dup.stage_order
            assert orig.executor_type == dup.executor_type

    def test_duplicate_copies_loops(self, conn):
        impl = load_pipeline_by_name(conn, "implementation")
        duplicate_pipeline(conn, impl.id, "impl_loops_copy")
        copy = load_pipeline_by_name(conn, "impl_loops_copy")
        assert len(copy.loops) == len(impl.loops)
        orig_loop_names = {lp.name for lp in impl.loops}
        copy_loop_names = {lp.name for lp in copy.loops}
        assert orig_loop_names == copy_loop_names

    def test_duplicate_nonexistent_raises(self, conn):
        with pytest.raises(ValueError, match="not found"):
            duplicate_pipeline(conn, 99999, "bad_copy")

    def test_duplicate_preserves_description_and_label(self, conn):
        inv = load_pipeline_by_name(conn, "investigation")
        duplicate_pipeline(conn, inv.id, "inv_copy")
        copy = load_pipeline_by_name(conn, "inv_copy")
        assert copy.description == inv.description
        # ticket_label is copied but is_default is always False
        assert copy.ticket_label == inv.ticket_label


# ---------------------------------------------------------------------------
# Stage CRUD — create_stage
# ---------------------------------------------------------------------------


class TestCreateStage:
    def test_create_stage_at_end(self, conn):
        pid = create_pipeline(conn, "stage_test")
        sid = create_stage(conn, pid, "step1", 1, "claude")
        assert isinstance(sid, int)
        pipeline = load_pipeline_by_name(conn, "stage_test")
        assert len(pipeline.stages) == 1
        assert pipeline.stages[0].name == "step1"

    def test_create_stage_with_all_fields(self, conn):
        pid = create_pipeline(conn, "full_stage")
        create_stage(
            conn, pid, "review", 1, "claude",
            identity="reviewer",
            prompt_template="Review {pr_url}",
            max_turns=50,
            timeout_minutes=30,
            result_parser="review_verdict",
        )
        pipeline = load_pipeline_by_name(conn, "full_stage")
        stage = pipeline.stages[0]
        assert stage.identity == "reviewer"
        assert stage.prompt_template == "Review {pr_url}"
        assert stage.max_turns == 50
        assert stage.timeout_minutes == 30
        assert stage.result_parser == "review_verdict"

    def test_create_stage_shifts_existing_orders(self, conn):
        pid = create_pipeline(conn, "shift_test")
        create_stage(conn, pid, "step1", 1, "claude")
        create_stage(conn, pid, "step3", 2, "claude")
        # Insert in the middle — should shift step3 to order 3
        create_stage(conn, pid, "step2", 2, "claude")
        pipeline = load_pipeline_by_name(conn, "shift_test")
        names = [s.name for s in pipeline.stages]
        assert names == ["step1", "step2", "step3"]
        orders = [s.stage_order for s in pipeline.stages]
        assert orders == [1, 2, 3]

    def test_create_stage_at_beginning(self, conn):
        pid = create_pipeline(conn, "begin_test")
        create_stage(conn, pid, "step2", 1, "claude")
        create_stage(conn, pid, "step1", 1, "claude")
        pipeline = load_pipeline_by_name(conn, "begin_test")
        names = [s.name for s in pipeline.stages]
        assert names == ["step1", "step2"]


# ---------------------------------------------------------------------------
# Stage CRUD — update_stage
# ---------------------------------------------------------------------------


class TestUpdateStage:
    def test_update_stage_name(self, conn):
        pid = create_pipeline(conn, "update_stage_test")
        sid = create_stage(conn, pid, "old_name", 1, "claude")
        update_stage(conn, sid, name="new_name")
        pipeline = load_pipeline_by_name(conn, "update_stage_test")
        assert pipeline.stages[0].name == "new_name"

    def test_update_stage_executor_type(self, conn):
        pid = create_pipeline(conn, "exec_test")
        sid = create_stage(conn, pid, "step", 1, "claude")
        update_stage(conn, sid, executor_type="shell")
        pipeline = load_pipeline_by_name(conn, "exec_test")
        assert pipeline.stages[0].executor_type == "shell"

    def test_update_stage_prompt_template(self, conn):
        pid = create_pipeline(conn, "prompt_test")
        sid = create_stage(conn, pid, "step", 1, "claude", prompt_template="old")
        update_stage(conn, sid, prompt_template="new template")
        pipeline = load_pipeline_by_name(conn, "prompt_test")
        assert pipeline.stages[0].prompt_template == "new template"

    def test_update_stage_multiple_fields(self, conn):
        pid = create_pipeline(conn, "multi_stage")
        sid = create_stage(conn, pid, "step", 1, "claude")
        update_stage(conn, sid, max_turns=100, timeout_minutes=60, identity="coder")
        pipeline = load_pipeline_by_name(conn, "multi_stage")
        stage = pipeline.stages[0]
        assert stage.max_turns == 100
        assert stage.timeout_minutes == 60
        assert stage.identity == "coder"

    def test_update_stage_noop_with_invalid_kwargs(self, conn):
        pid = create_pipeline(conn, "noop_stage")
        sid = create_stage(conn, pid, "step", 1, "claude")
        update_stage(conn, sid, invalid="value")
        pipeline = load_pipeline_by_name(conn, "noop_stage")
        assert pipeline.stages[0].name == "step"


# ---------------------------------------------------------------------------
# Stage CRUD — delete_stage
# ---------------------------------------------------------------------------


class TestDeleteStage:
    def test_delete_stage_basic(self, conn):
        pid = create_pipeline(conn, "del_stage")
        sid = create_stage(conn, pid, "step1", 1, "claude")
        delete_stage(conn, sid)
        pipeline = load_pipeline_by_name(conn, "del_stage")
        assert len(pipeline.stages) == 0

    def test_delete_stage_recompacts_order(self, conn):
        pid = create_pipeline(conn, "recompact_test")
        s1 = create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        s3 = create_stage(conn, pid, "step3", 3, "claude")
        delete_stage(conn, s2)
        pipeline = load_pipeline_by_name(conn, "recompact_test")
        assert len(pipeline.stages) == 2
        assert pipeline.stages[0].name == "step1"
        assert pipeline.stages[0].stage_order == 1
        assert pipeline.stages[1].name == "step3"
        assert pipeline.stages[1].stage_order == 2

    def test_delete_first_stage_recompacts(self, conn):
        pid = create_pipeline(conn, "del_first")
        s1 = create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        s3 = create_stage(conn, pid, "step3", 3, "claude")
        delete_stage(conn, s1)
        pipeline = load_pipeline_by_name(conn, "del_first")
        orders = [s.stage_order for s in pipeline.stages]
        assert orders == [1, 2]

    def test_delete_last_stage_no_recompact_needed(self, conn):
        pid = create_pipeline(conn, "del_last")
        s1 = create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        s3 = create_stage(conn, pid, "step3", 3, "claude")
        delete_stage(conn, s3)
        pipeline = load_pipeline_by_name(conn, "del_last")
        orders = [s.stage_order for s in pipeline.stages]
        assert orders == [1, 2]

    def test_delete_stage_cleans_up_referencing_loops(self, conn):
        pid = create_pipeline(conn, "loop_cleanup")
        create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        create_stage(conn, pid, "step3", 3, "claude")
        create_loop(conn, pid, "test_loop", "step1", "step2", 3)
        delete_stage(conn, s2)
        pipeline = load_pipeline_by_name(conn, "loop_cleanup")
        assert len(pipeline.loops) == 0

    def test_delete_nonexistent_stage_raises(self, conn):
        with pytest.raises(ValueError, match="not found"):
            delete_stage(conn, 99999)


# ---------------------------------------------------------------------------
# Stage CRUD — reorder_stages
# ---------------------------------------------------------------------------


class TestReorderStages:
    def test_reorder_reverses_stages(self, conn):
        pid = create_pipeline(conn, "reorder_test")
        s1 = create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        s3 = create_stage(conn, pid, "step3", 3, "claude")
        reorder_stages(conn, pid, [s3, s2, s1])
        pipeline = load_pipeline_by_name(conn, "reorder_test")
        names = [s.name for s in pipeline.stages]
        assert names == ["step3", "step2", "step1"]
        orders = [s.stage_order for s in pipeline.stages]
        assert orders == [1, 2, 3]

    def test_reorder_moves_last_to_first(self, conn):
        pid = create_pipeline(conn, "reorder_first")
        s1 = create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        s3 = create_stage(conn, pid, "step3", 3, "claude")
        reorder_stages(conn, pid, [s3, s1, s2])
        pipeline = load_pipeline_by_name(conn, "reorder_first")
        names = [s.name for s in pipeline.stages]
        assert names == ["step3", "step1", "step2"]

    def test_reorder_same_order_is_noop(self, conn):
        pid = create_pipeline(conn, "reorder_noop")
        s1 = create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        reorder_stages(conn, pid, [s1, s2])
        pipeline = load_pipeline_by_name(conn, "reorder_noop")
        names = [s.name for s in pipeline.stages]
        assert names == ["step1", "step2"]


# ---------------------------------------------------------------------------
# Loop CRUD — create_loop
# ---------------------------------------------------------------------------


class TestCreateLoop:
    def test_create_loop_basic(self, conn):
        pid = create_pipeline(conn, "loop_test")
        create_stage(conn, pid, "step1", 1, "claude")
        create_stage(conn, pid, "step2", 2, "claude")
        lid = create_loop(conn, pid, "my_loop", "step1", "step2", 5)
        assert isinstance(lid, int)
        pipeline = load_pipeline_by_name(conn, "loop_test")
        assert len(pipeline.loops) == 1
        loop = pipeline.loops[0]
        assert loop.name == "my_loop"
        assert loop.start_stage == "step1"
        assert loop.end_stage == "step2"
        assert loop.max_iterations == 5

    def test_create_loop_with_all_fields(self, conn):
        pid = create_pipeline(conn, "full_loop")
        create_stage(conn, pid, "review", 1, "claude")
        create_stage(conn, pid, "fix", 2, "claude")
        create_loop(
            conn, pid, "review_loop", "review", "fix", 3,
            config_key="max_review_iterations",
            exit_condition="review_approved",
            on_failure_stage="fix",
        )
        pipeline = load_pipeline_by_name(conn, "full_loop")
        loop = pipeline.loops[0]
        assert loop.config_key == "max_review_iterations"
        assert loop.exit_condition == "review_approved"
        assert loop.on_failure_stage == "fix"


# ---------------------------------------------------------------------------
# Loop CRUD — update_loop
# ---------------------------------------------------------------------------


class TestUpdateLoop:
    def test_update_loop_name(self, conn):
        pid = create_pipeline(conn, "update_loop_test")
        create_stage(conn, pid, "s1", 1, "claude")
        create_stage(conn, pid, "s2", 2, "claude")
        lid = create_loop(conn, pid, "old_loop", "s1", "s2", 3)
        update_loop(conn, lid, name="new_loop")
        pipeline = load_pipeline_by_name(conn, "update_loop_test")
        assert pipeline.loops[0].name == "new_loop"

    def test_update_loop_max_iterations(self, conn):
        pid = create_pipeline(conn, "iter_test")
        create_stage(conn, pid, "s1", 1, "claude")
        create_stage(conn, pid, "s2", 2, "claude")
        lid = create_loop(conn, pid, "loop", "s1", "s2", 3)
        update_loop(conn, lid, max_iterations=10)
        pipeline = load_pipeline_by_name(conn, "iter_test")
        assert pipeline.loops[0].max_iterations == 10

    def test_update_loop_stages(self, conn):
        pid = create_pipeline(conn, "stages_test")
        create_stage(conn, pid, "s1", 1, "claude")
        create_stage(conn, pid, "s2", 2, "claude")
        create_stage(conn, pid, "s3", 3, "claude")
        lid = create_loop(conn, pid, "loop", "s1", "s2", 3)
        update_loop(conn, lid, start_stage="s2", end_stage="s3")
        pipeline = load_pipeline_by_name(conn, "stages_test")
        loop = pipeline.loops[0]
        assert loop.start_stage == "s2"
        assert loop.end_stage == "s3"

    def test_update_loop_noop_with_invalid_kwargs(self, conn):
        pid = create_pipeline(conn, "noop_loop")
        create_stage(conn, pid, "s1", 1, "claude")
        create_stage(conn, pid, "s2", 2, "claude")
        lid = create_loop(conn, pid, "loop", "s1", "s2", 3)
        update_loop(conn, lid, invalid="value")
        pipeline = load_pipeline_by_name(conn, "noop_loop")
        assert pipeline.loops[0].name == "loop"


# ---------------------------------------------------------------------------
# Loop CRUD — delete_loop
# ---------------------------------------------------------------------------


class TestDeleteLoop:
    def test_delete_loop_basic(self, conn):
        pid = create_pipeline(conn, "del_loop")
        create_stage(conn, pid, "s1", 1, "claude")
        create_stage(conn, pid, "s2", 2, "claude")
        lid = create_loop(conn, pid, "loop", "s1", "s2", 3)
        delete_loop(conn, lid)
        pipeline = load_pipeline_by_name(conn, "del_loop")
        assert len(pipeline.loops) == 0

    def test_delete_loop_leaves_stages_intact(self, conn):
        pid = create_pipeline(conn, "del_loop_stages")
        create_stage(conn, pid, "s1", 1, "claude")
        create_stage(conn, pid, "s2", 2, "claude")
        lid = create_loop(conn, pid, "loop", "s1", "s2", 3)
        delete_loop(conn, lid)
        pipeline = load_pipeline_by_name(conn, "del_loop_stages")
        assert len(pipeline.stages) == 2


# ---------------------------------------------------------------------------
# Validation — validate_pipeline
# ---------------------------------------------------------------------------


class TestValidatePipeline:
    def test_valid_pipeline_has_no_errors(self, conn):
        impl = load_pipeline_by_name(conn, "implementation")
        errors = validate_pipeline(conn, impl.id)
        assert errors == []

    def test_valid_investigation_pipeline(self, conn):
        inv = load_pipeline_by_name(conn, "investigation")
        errors = validate_pipeline(conn, inv.id)
        assert errors == []

    def test_nonexistent_pipeline(self, conn):
        errors = validate_pipeline(conn, 99999)
        assert len(errors) == 1
        assert "not found" in errors[0]

    def test_empty_name_detected(self, conn):
        pid = create_pipeline(conn, "temp")
        conn.execute("UPDATE pipeline_templates SET name = '' WHERE id = ?", (pid,))
        conn.commit()
        errors = validate_pipeline(conn, pid)
        assert any("name must not be empty" in e for e in errors)

    def test_no_stages_detected(self, conn):
        pid = create_pipeline(conn, "empty_pipe")
        errors = validate_pipeline(conn, pid)
        assert any("at least one stage" in e for e in errors)

    def test_non_contiguous_stage_orders_detected(self, conn):
        pid = create_pipeline(conn, "gap_stages")
        create_stage(conn, pid, "step1", 1, "claude")
        s2 = create_stage(conn, pid, "step2", 2, "claude")
        # Create a gap by updating order directly
        conn.execute(
            "UPDATE stage_templates SET stage_order = 5 WHERE id = ?", (s2,)
        )
        conn.commit()
        errors = validate_pipeline(conn, pid)
        assert any("not contiguous" in e for e in errors)

    def test_loop_references_nonexistent_start_stage(self, conn):
        pid = create_pipeline(conn, "bad_loop_start")
        create_stage(conn, pid, "step1", 1, "claude")
        create_loop(conn, pid, "bad_loop", "nonexistent", "step1", 3)
        errors = validate_pipeline(conn, pid)
        assert any("start_stage" in e and "nonexistent" in e for e in errors)

    def test_loop_references_nonexistent_end_stage(self, conn):
        pid = create_pipeline(conn, "bad_loop_end")
        create_stage(conn, pid, "step1", 1, "claude")
        create_loop(conn, pid, "bad_loop", "step1", "nonexistent", 3)
        errors = validate_pipeline(conn, pid)
        assert any("end_stage" in e and "nonexistent" in e for e in errors)

    def test_multiple_errors_returned(self, conn):
        pid = create_pipeline(conn, "multi_err")
        # No stages + empty name
        conn.execute("UPDATE pipeline_templates SET name = '' WHERE id = ?", (pid,))
        conn.commit()
        errors = validate_pipeline(conn, pid)
        assert len(errors) >= 2
