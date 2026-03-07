"""Tests for dashboard workflow page, pipeline APIs, cleanup, tickets, stop slot."""

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from botfarm.config import BotfarmConfig, CapacityConfig, LinearConfig, ProjectConfig
from botfarm.dashboard import create_app
from botfarm.db import (
    init_db,
    insert_task,
    save_capacity_state,
    save_dispatch_state,
    upsert_ticket_history,
)
from tests.helpers import seed_slot as _seed_slot


class TestWorkflowPage:
    def test_workflow_returns_200(self, client):
        resp = client.get("/workflow")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_workflow_shows_implementation_pipeline(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "implementation" in body.lower()
        assert "implement" in body
        assert "review" in body
        assert "pr_checks" in body
        assert "merge" in body

    def test_workflow_shows_investigation_pipeline(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "investigation" in body.lower()
        assert "Investigation" in body or "investigation" in body

    def test_workflow_shows_decision_points(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "Approved?" in body
        assert "CI passed?" in body

    def test_workflow_shows_loop_iterations(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "max 3 iterations" in body
        assert "max 2 retries" in body

    def test_workflow_shows_executor_types(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "workflow-node-claude" in body
        assert "workflow-node-shell" in body
        assert "workflow-node-internal" in body

    def test_workflow_shows_identity_badges(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "coder" in body
        assert "reviewer" in body

    def test_workflow_shows_loop_fix_stages(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "fix" in body
        assert "ci_fix" in body

    def test_workflow_nav_link(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'href="/workflow"' in body
        assert 'aria-current="page"' in body

    def test_workflow_pipeline_tabs(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "workflow-tab" in body
        assert "switchPipeline" in body

    def test_workflow_no_db(self, tmp_path):
        app = create_app(db_path=tmp_path / "nonexistent.db")
        client = TestClient(app)
        resp = client.get("/workflow")
        assert resp.status_code == 200
        assert "No pipeline definitions found" in resp.text

    def test_workflow_shows_stage_timeouts(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "min" in body

    def test_workflow_shows_investigation_note(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "Research only" in body

    def test_workflow_config_overrides_loop_iterations(self, db_file):
        """When botfarm_config is provided, loop max iterations should reflect config values."""
        cfg = BotfarmConfig(
            projects=[ProjectConfig(name="test", linear_team="T", base_dir="/tmp", worktree_prefix="w", slots=[1])],
            linear=LinearConfig(api_key="test"),
        )
        cfg.agents.max_review_iterations = 5
        cfg.agents.max_ci_retries = 4
        app = create_app(db_path=db_file, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/workflow")
        body = resp.text
        assert "max 5 iterations" in body
        assert "max 4 retries" in body

    def test_workflow_shows_prompt_edit_buttons(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-edit-btn" in body
        assert "enterEditMode" in body

    def test_workflow_shows_prompt_preview(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-preview" in body
        assert "prompt-textarea" in body

    def test_workflow_shows_variable_chips(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-chip" in body
        assert "{ticket_id}" in body
        assert "{pr_url}" in body
        assert "{pr_number}" in body
        assert "{owner}" in body
        assert "{repo}" in body
        assert "{ci_failure_output}" in body

    def test_workflow_hides_prompt_for_non_claude_stages(self, client):
        """Shell/internal stages have prompt section hidden."""
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-field-claude" in body

    def test_workflow_prompt_save_cancel_buttons(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prompt-save-btn" in body
        assert "prompt-cancel-btn" in body
        assert "savePrompt" in body
        assert "cancelEdit" in body

    def test_workflow_unknown_var_detection(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "checkUnknownVars" in body
        assert "KNOWN_VARS" in body

    def test_workflow_shows_stage_properties_form(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-props-form" in body
        assert "prop-executor_type-" in body
        assert "prop-identity-" in body
        assert "prop-max_turns-" in body
        assert "prop-timeout_minutes-" in body
        assert "prop-result_parser-" in body

    def test_workflow_shows_executor_type_dropdown(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'value="claude"' in body
        assert 'value="shell"' in body
        assert 'value="internal"' in body
        assert "onExecutorTypeChange" in body

    def test_workflow_shows_identity_dropdown(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'value="coder"' in body
        assert 'value="reviewer"' in body

    def test_workflow_shows_result_parser_dropdown(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'value="pr_url"' in body
        assert 'value="review_verdict"' in body

    def test_workflow_shows_shell_command_field(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "prop-shell_command-" in body
        assert "stage-field-shell" in body

    def test_workflow_shows_save_properties_button(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "saveStageProperties" in body
        assert "Save Properties" in body

    def test_workflow_conditional_visibility_classes(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-field-claude" in body
        assert "stage-field-shell" in body
        assert "stage-field-not-internal" in body

    def test_workflow_has_pipeline_id_attribute(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "data-pipeline-id=" in body

    def test_workflow_shows_new_pipeline_button(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "+ New Pipeline" in body
        assert "showCreatePipelineModal" in body

    def test_workflow_shows_pipeline_action_buttons(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "Edit Metadata" in body
        assert "Duplicate" in body
        assert "Delete" in body

    def test_workflow_has_create_pipeline_modal(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'id="create-pipeline-modal"' in body
        assert "Create New Pipeline" in body
        assert 'id="new-pipe-name"' in body

    def test_workflow_has_duplicate_pipeline_modal(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'id="duplicate-pipeline-modal"' in body
        assert "Duplicate Pipeline" in body
        assert 'id="dup-pipe-name"' in body

    def test_workflow_has_pipeline_metadata_editor(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "pipeline-editor" in body
        assert "savePipelineMetadata" in body
        assert "togglePipelineEditor" in body

    def test_workflow_metadata_editor_has_fields(self, client):
        resp = client.get("/workflow")
        body = resp.text
        # Check for name, description, ticket_label, is_default fields
        assert "pipe-name-" in body
        assert "pipe-desc-" in body
        assert "pipe-label-" in body
        assert "pipe-default-" in body

    def test_workflow_has_delete_pipeline_function(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "deletePipeline" in body

    def test_workflow_has_duplicate_pipeline_function(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "duplicatePipeline" in body
        assert "confirmDuplicatePipeline" in body

    def test_workflow_tabs_always_shown(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'class="workflow-tabs"' in body

    def test_workflow_shows_loop_editor_elements(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "loop-editor" in body
        assert "toggleLoopEditor" in body

    def test_workflow_loop_editor_has_all_fields(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "loop-name-" in body
        assert "loop-start_stage-" in body
        assert "loop-end_stage-" in body
        assert "loop-max_iterations-" in body
        assert "loop-config_key-" in body
        assert "loop-exit_condition-" in body
        assert "loop-on_failure_stage-" in body

    def test_workflow_has_add_loop_button(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "Add Loop" in body
        assert "showAddLoopModal" in body

    def test_workflow_has_add_loop_modal(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "add-loop-modal" in body
        assert "add-loop-name" in body
        assert "createLoop" in body

    def test_workflow_has_delete_loop_button(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "deleteLoop" in body
        assert "Remove Loop" in body

    def test_workflow_has_save_loop_function(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "saveLoop" in body

    def test_workflow_loop_editor_has_stage_dropdowns(self, client):
        resp = client.get("/workflow")
        body = resp.text
        # The loop editor should have stage options for start_stage and end_stage
        assert "loop-start_stage-" in body
        assert "loop-end_stage-" in body
        # Stage names should appear as options
        assert "implement" in body
        assert "review" in body

    def test_workflow_loop_shows_config_hint(self, client):
        resp = client.get("/workflow")
        body = resp.text
        # review_loop has config_key set, so it should show the config hint
        assert "overridable via config" in body

    def test_workflow_loop_data_attributes(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "data-loop-id" in body
        assert "data-decision-stage" in body
        assert "data-start-stage" in body
        assert "data-end-stage" in body
        assert "data-failure-stage" in body

    def test_workflow_has_add_stage_button(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "+ Add Stage" in body
        assert "showAddStageModal" in body

    def test_workflow_has_add_stage_modal(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert 'id="add-stage-modal"' in body
        assert 'id="add-stage-name"' in body
        assert 'id="add-stage-executor_type"' in body
        assert 'id="add-stage-identity"' in body

    def test_workflow_has_remove_stage_buttons(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-remove-btn" in body
        assert "removeStage" in body

    def test_workflow_has_reorder_stage_buttons(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "stage-move-up-btn" in body
        assert "stage-move-down-btn" in body
        assert "moveStageUp" in body
        assert "moveStageDown" in body

    def test_workflow_has_stage_ids_data_attribute(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "data-stage-ids" in body

    def test_workflow_stage_action_buttons_have_disabled_state(self, client):
        """First stage should have up disabled, last stage should have down disabled."""
        resp = client.get("/workflow")
        body = resp.text
        # Check that disabled attribute appears on some stage action buttons
        assert "stage-action-btn" in body
        # The first and last stage buttons should have disabled attributes
        assert 'disabled' in body

    def test_workflow_has_create_stage_function(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "function createStage()" in body

    def test_workflow_has_reorder_functions(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "function moveStageUp(" in body
        assert "function moveStageDown(" in body
        assert "_reorderStages" in body

    def test_workflow_add_stage_modal_has_executor_change(self, client):
        resp = client.get("/workflow")
        body = resp.text
        assert "onAddStageExecutorChange" in body

    def test_workflow_investigation_has_add_stage(self, client):
        """Even single-stage (investigation) pipeline should show Add Stage button."""
        resp = client.get("/workflow")
        body = resp.text
        # Count occurrences - should appear for each pipeline
        assert body.count("+ Add Stage") >= 2


# --- Workflow API ---


class TestApiListPipelines:
    def test_list_returns_all_pipelines(self, client):
        resp = client.get("/api/workflow/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert isinstance(data["data"], list)
        assert len(data["data"]) >= 2  # implementation + investigation from seed

    def test_list_includes_stages_and_loops(self, client):
        resp = client.get("/api/workflow/pipelines")
        pipelines = resp.json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        assert len(impl["stages"]) >= 1
        assert len(impl["loops"]) >= 1
        # Stages should have IDs
        assert "id" in impl["stages"][0]

    def test_list_pipeline_fields(self, client):
        resp = client.get("/api/workflow/pipelines")
        pipeline = resp.json()["data"][0]
        for key in ("id", "name", "description", "ticket_label", "is_default", "stages", "loops"):
            assert key in pipeline


class TestApiGetPipeline:
    def test_get_existing_pipeline(self, client):
        # Get list first to find an ID
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.get(f"/api/workflow/pipelines/{pid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["id"] == pid

    def test_get_nonexistent_pipeline(self, client):
        resp = client.get("/api/workflow/pipelines/99999")
        assert resp.status_code == 404
        data = resp.json()
        assert data["ok"] is False
        assert any("not found" in e for e in data["errors"])


class TestApiCreatePipeline:
    def test_create_pipeline(self, client):
        resp = client.post(
            "/api/workflow/pipelines",
            json={"name": "test_pipeline", "description": "A test"},
        )
        # Validation is skipped on create (no stages yet), pipeline is created
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "test_pipeline"
        assert data["data"]["stages"] == []

    def test_create_pipeline_then_add_stage(self, db_file):
        """Create pipeline, then add a stage — full happy path."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        # Create succeeds even with no stages (validation deferred)
        resp = client.post(
            "/api/workflow/pipelines",
            json={"name": "custom_pipe", "description": "Custom"},
        )
        assert resp.status_code == 200
        pid = resp.json()["data"]["id"]
        # Add a stage
        stage_resp = client.post(
            f"/api/workflow/pipelines/{pid}/stages",
            json={
                "name": "build",
                "stage_order": 1,
                "executor_type": "claude",
                "identity": "coder",
                "max_turns": 50,
                "timeout_minutes": 30,
            },
        )
        assert stage_resp.status_code == 200
        assert stage_resp.json()["data"]["name"] == "build"


class TestApiUpdatePipeline:
    def test_update_pipeline_metadata(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        original_name = pipelines[0]["name"]
        resp = client.patch(
            f"/api/workflow/pipelines/{pid}",
            json={"description": "Updated description"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["description"] == "Updated description"
        assert data["data"]["name"] == original_name

    def test_update_nonexistent_pipeline(self, client):
        resp = client.patch(
            "/api/workflow/pipelines/99999",
            json={"description": "Nope"},
        )
        assert resp.status_code == 404

    def test_update_unknown_field(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.patch(
            f"/api/workflow/pipelines/{pid}",
            json={"bogus_field": "value"},
        )
        assert resp.status_code == 400
        assert resp.json()["ok"] is False


class TestApiDeletePipeline:
    def test_delete_non_default_pipeline(self, client):
        # Investigation pipeline is not default
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        non_default = next(p for p in pipelines if not p["is_default"])
        resp = client.delete(f"/api/workflow/pipelines/{non_default['id']}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        # Verify it's gone
        resp2 = client.get(f"/api/workflow/pipelines/{non_default['id']}")
        assert resp2.status_code == 404

    def test_delete_sole_default_pipeline_rejected(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        default_p = next(p for p in pipelines if p["is_default"])
        resp = client.delete(f"/api/workflow/pipelines/{default_p['id']}")
        assert resp.status_code == 400
        assert resp.json()["ok"] is False
        assert any("default" in e.lower() for e in resp.json()["errors"])

    def test_delete_nonexistent_pipeline(self, client):
        resp = client.delete("/api/workflow/pipelines/99999")
        assert resp.status_code == 404


class TestApiDuplicatePipeline:
    def test_duplicate_pipeline(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/duplicate",
            json={"name": "implementation_copy"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "implementation_copy"
        # Should have same number of stages
        assert len(data["data"]["stages"]) == len(pipelines[0]["stages"])

    def test_duplicate_requires_name(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/duplicate",
            json={},
        )
        assert resp.status_code == 400
        assert resp.json()["ok"] is False

    def test_duplicate_nonexistent(self, client):
        resp = client.post(
            "/api/workflow/pipelines/99999/duplicate",
            json={"name": "copy"},
        )
        assert resp.status_code == 404


class TestApiCreateStage:
    def test_create_stage(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        num_stages = len(pipelines[0]["stages"])
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/stages",
            json={
                "name": "new_stage",
                "stage_order": num_stages + 1,
                "executor_type": "claude",
                "identity": "coder",
                "max_turns": 50,
                "timeout_minutes": 30,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "new_stage"
        assert data["data"]["executor_type"] == "claude"

    def test_create_stage_nonexistent_pipeline(self, client):
        resp = client.post(
            "/api/workflow/pipelines/99999/stages",
            json={"name": "s", "stage_order": 1, "executor_type": "claude"},
        )
        assert resp.status_code == 404


class TestApiUpdateStage:
    def test_update_stage(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"max_turns": 999},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["max_turns"] == 999

    def test_update_prompt_template(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        new_prompt = "Updated prompt for {ticket_id} with {pr_url}"
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"prompt_template": new_prompt},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["prompt_template"] == new_prompt

    def test_update_nonexistent_stage(self, client):
        resp = client.patch(
            "/api/workflow/stages/99999",
            json={"max_turns": 10},
        )
        assert resp.status_code == 404

    def test_update_unknown_field(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"bogus": "value"},
        )
        assert resp.status_code == 400

    def test_update_executor_type(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"executor_type": "shell", "shell_command": "echo hello", "identity": None, "max_turns": None},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["executor_type"] == "shell"
        assert data["data"]["shell_command"] == "echo hello"
        assert data["data"]["identity"] is None
        assert data["data"]["max_turns"] is None

    def test_update_multiple_properties(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={
                "identity": "reviewer",
                "max_turns": 50,
                "timeout_minutes": 30,
                "result_parser": "review_verdict",
            },
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["identity"] == "reviewer"
        assert data["max_turns"] == 50
        assert data["timeout_minutes"] == 30
        assert data["result_parser"] == "review_verdict"

    def test_update_null_out_optional_fields(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"identity": None, "max_turns": None, "timeout_minutes": None, "result_parser": None},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert data["identity"] is None
        assert data["max_turns"] is None
        assert data["timeout_minutes"] is None
        assert data["result_parser"] is None

    def test_update_shell_command(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        # Find the shell stage (pr_checks)
        shell_stage = None
        for s in pipelines[0]["stages"]:
            if s["executor_type"] == "shell":
                shell_stage = s
                break
        assert shell_stage is not None
        new_cmd = "gh pr checks {pr_url} --watch --fail-fast"
        resp = client.patch(
            f"/api/workflow/stages/{shell_stage['id']}",
            json={"shell_command": new_cmd},
        )
        assert resp.status_code == 200
        assert resp.json()["data"]["shell_command"] == new_cmd

    def test_update_response_includes_shell_command(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        stage = pipelines[0]["stages"][0]
        resp = client.patch(
            f"/api/workflow/stages/{stage['id']}",
            json={"timeout_minutes": 60},
        )
        assert resp.status_code == 200
        data = resp.json()["data"]
        assert "shell_command" in data
        assert "executor_type" in data
        assert "identity" in data
        assert "max_turns" in data
        assert "timeout_minutes" in data
        assert "result_parser" in data


class TestApiDeleteStage:
    def test_delete_stage(self, client):
        # Add a stage first so we can delete it without breaking the pipeline
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        num_stages = len(pipelines[0]["stages"])
        create_resp = client.post(
            f"/api/workflow/pipelines/{pid}/stages",
            json={
                "name": "disposable",
                "stage_order": num_stages + 1,
                "executor_type": "shell",
            },
        )
        stage_id = create_resp.json()["data"]["id"]
        resp = client.delete(f"/api/workflow/stages/{stage_id}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_delete_nonexistent_stage(self, client):
        resp = client.delete("/api/workflow/stages/99999")
        assert resp.status_code == 404


class TestApiReorderStages:
    def test_reorder_stages(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        stage_ids = [s["id"] for s in pipelines[0]["stages"]]
        reversed_ids = list(reversed(stage_ids))
        resp = client.put(
            f"/api/workflow/pipelines/{pid}/stages/order",
            json={"stage_ids": reversed_ids},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        new_order = [s["id"] for s in data["data"]["stages"]]
        assert new_order == reversed_ids

    def test_reorder_nonexistent_pipeline(self, client):
        resp = client.put(
            "/api/workflow/pipelines/99999/stages/order",
            json={"stage_ids": [1, 2]},
        )
        assert resp.status_code == 404

    def test_reorder_invalid_ids(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.put(
            f"/api/workflow/pipelines/{pid}/stages/order",
            json={"stage_ids": [99998, 99999]},
        )
        assert resp.status_code == 400

    def test_reorder_missing_stage_ids(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        resp = client.put(
            f"/api/workflow/pipelines/{pid}/stages/order",
            json={"not_stage_ids": [1]},
        )
        assert resp.status_code == 400


class TestApiCreateLoop:
    def test_create_loop(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        stages = pipelines[0]["stages"]
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/loops",
            json={
                "name": "test_loop",
                "start_stage": stages[0]["name"],
                "end_stage": stages[1]["name"],
                "max_iterations": 2,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["name"] == "test_loop"
        assert data["data"]["max_iterations"] == 2

    def test_create_loop_nonexistent_pipeline(self, client):
        resp = client.post(
            "/api/workflow/pipelines/99999/loops",
            json={
                "name": "l",
                "start_stage": "a",
                "end_stage": "b",
                "max_iterations": 1,
            },
        )
        assert resp.status_code == 404

    def test_create_loop_invalid_stages(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        pid = pipelines[0]["id"]
        loops_before = len(pipelines[0]["loops"])
        resp = client.post(
            f"/api/workflow/pipelines/{pid}/loops",
            json={
                "name": "bad_loop",
                "start_stage": "nonexistent_stage",
                "end_stage": "also_nonexistent",
                "max_iterations": 1,
            },
        )
        assert resp.status_code == 400
        assert resp.json()["ok"] is False
        # Verify the invalid loop was rolled back from the DB
        updated = client.get(f"/api/workflow/pipelines/{pid}").json()["data"]
        assert len(updated["loops"]) == loops_before


class TestApiUpdateLoop:
    def test_update_loop(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        loop = impl["loops"][0]
        resp = client.patch(
            f"/api/workflow/loops/{loop['id']}",
            json={"max_iterations": 10},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["data"]["max_iterations"] == 10

    def test_update_nonexistent_loop(self, client):
        resp = client.patch(
            "/api/workflow/loops/99999",
            json={"max_iterations": 5},
        )
        assert resp.status_code == 404

    def test_update_unknown_field(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        loop = impl["loops"][0]
        resp = client.patch(
            f"/api/workflow/loops/{loop['id']}",
            json={"bogus_field": "value"},
        )
        assert resp.status_code == 400


class TestApiDeleteLoop:
    def test_delete_loop(self, client):
        pipelines = client.get("/api/workflow/pipelines").json()["data"]
        impl = next(p for p in pipelines if p["name"] == "implementation")
        loop = impl["loops"][0]
        resp = client.delete(f"/api/workflow/loops/{loop['id']}")
        assert resp.status_code == 200
        assert resp.json()["ok"] is True

    def test_delete_nonexistent_loop(self, client):
        resp = client.delete("/api/workflow/loops/99999")
        assert resp.status_code == 404
        assert resp.json()["ok"] is False


# --- Linear Capacity Widget (SMA-275) ---


class TestLinearCapacityPartial:
    """Tests for the /partials/linear-capacity endpoint and index integration."""

    def _make_client(self, tmp_path, *, issue_count=None, limit=250,
                     by_project=None, checked_at="2026-03-01T12:00:00+00:00",
                     botfarm_config=None):
        """Create a test client with optional capacity data seeded."""
        db_path = tmp_path / "capacity.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(conn, paused=False)
        if issue_count is not None:
            save_capacity_state(
                conn,
                issue_count=issue_count,
                limit=limit,
                by_project=by_project or {},
                checked_at=checked_at,
            )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path, botfarm_config=botfarm_config)
        return TestClient(app)

    def test_no_capacity_data(self, tmp_path):
        """Shows fallback message when no capacity data exists."""
        client = self._make_client(tmp_path)
        resp = client.get("/partials/linear-capacity")
        assert resp.status_code == 200
        assert "No capacity data available" in resp.text

    def test_capacity_green(self, tmp_path):
        """Low utilization shows green status."""
        client = self._make_client(tmp_path, issue_count=100, limit=250)
        resp = client.get("/partials/linear-capacity")
        assert resp.status_code == 200
        assert "100 / 250" in resp.text
        assert "status-free" in resp.text
        assert "40%" in resp.text

    def test_capacity_yellow_warning(self, tmp_path):
        """At 70%+ utilization shows yellow/warning status."""
        client = self._make_client(tmp_path, issue_count=180, limit=250)
        resp = client.get("/partials/linear-capacity")
        assert resp.status_code == 200
        assert "status-busy" in resp.text
        assert "180 / 250" in resp.text

    def test_capacity_red_critical(self, tmp_path):
        """At 85%+ utilization shows red/critical status with warning banner."""
        client = self._make_client(tmp_path, issue_count=220, limit=250)
        resp = client.get("/partials/linear-capacity")
        assert resp.status_code == 200
        assert "status-failed" in resp.text
        assert "approaching limit" in resp.text

    def test_capacity_blocked(self, tmp_path):
        """At 95%+ utilization shows blocked banner with dispatch paused notice."""
        client = self._make_client(tmp_path, issue_count=240, limit=250)
        resp = client.get("/partials/linear-capacity")
        assert resp.status_code == 200
        assert "DISPATCH PAUSED" in resp.text
        assert "Archive completed issues" in resp.text

    def test_project_breakdown(self, tmp_path):
        """Per-project breakdown table renders with counts and percentages."""
        by_project = {"Alpha": 50, "Beta": 30, "Gamma": 20}
        client = self._make_client(
            tmp_path, issue_count=100, limit=250, by_project=by_project,
        )
        resp = client.get("/partials/linear-capacity")
        body = resp.text
        assert "Alpha" in body
        assert "Beta" in body
        assert "Gamma" in body
        # Alpha is 50 out of 100 = 50%
        assert "50%" in body

    def test_last_checked_timestamp(self, tmp_path):
        """Last checked timestamp is shown."""
        client = self._make_client(
            tmp_path, issue_count=50, limit=250,
            checked_at="2026-03-01T12:00:00+00:00",
        )
        resp = client.get("/partials/linear-capacity")
        assert "Last checked:" in resp.text
        assert "ago" in resp.text

    def test_index_contains_linear_capacity_section(self, db_file):
        """Index page includes the Linear Capacity section with htmx polling."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Linear Capacity" in resp.text
        assert "linear-capacity-panel" in resp.text
        assert "every 30s" in resp.text

    def test_index_capacity_between_usage_and_queue(self, db_file):
        """Linear Capacity section appears between Usage and Queue."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/")
        body = resp.text
        usage_pos = body.find(">Usage<")
        capacity_pos = body.find(">Linear Capacity<")
        queue_pos = body.find(">Queue<")
        assert usage_pos < capacity_pos < queue_pos

    def test_capacity_with_custom_thresholds(self, tmp_path):
        """Respects custom threshold config for color classes."""
        cfg = BotfarmConfig(
            projects=[],
            linear=LinearConfig(
                capacity_monitoring=CapacityConfig(
                    warning_threshold=0.50,
                    critical_threshold=0.60,
                    pause_threshold=0.70,
                ),
            ),
        )
        # 60% would be green with defaults, but critical with custom thresholds
        client = self._make_client(
            tmp_path, issue_count=160, limit=250, botfarm_config=cfg,
        )
        resp = client.get("/partials/linear-capacity")
        assert "status-failed" in resp.text

    def test_capacity_widget_free_up_space_button(self, tmp_path):
        """Capacity widget should include a 'Free Up Space' link."""
        client = self._make_client(tmp_path, issue_count=200, limit=250)
        resp = client.get("/partials/linear-capacity")
        assert "Free Up Space" in resp.text
        assert 'href="/cleanup"' in resp.text


# ---------------------------------------------------------------------------
# Cleanup page
# ---------------------------------------------------------------------------


class TestCleanupPage:
    def _make_client(self, tmp_path, *, with_config=True):
        db_path = tmp_path / "cleanup.db"
        conn = init_db(db_path)
        conn.close()
        kwargs = {"db_path": db_path}
        if with_config:
            cfg = BotfarmConfig(
                projects=[
                    ProjectConfig(
                        name="proj",
                        linear_team="SMA",
                        base_dir="/tmp",
                        worktree_prefix="w",
                        slots=[1],
                    ),
                ],
                linear=LinearConfig(api_key="test-key"),
            )
            kwargs["botfarm_config"] = cfg
        app = create_app(**kwargs)
        return TestClient(app)

    def test_cleanup_page_returns_200(self, tmp_path):
        client = self._make_client(tmp_path)
        resp = client.get("/cleanup")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_cleanup_page_contains_title(self, tmp_path):
        client = self._make_client(tmp_path)
        resp = client.get("/cleanup")
        assert "Bulk Archive" in resp.text

    def test_cleanup_page_contains_filter_controls(self, tmp_path):
        client = self._make_client(tmp_path)
        resp = client.get("/cleanup")
        assert "filter-min-age" in resp.text
        assert "filter-limit" in resp.text
        assert "Load Preview" in resp.text

    def test_cleanup_page_contains_action_buttons(self, tmp_path):
        client = self._make_client(tmp_path)
        resp = client.get("/cleanup")
        assert "btn-action-archive" in resp.text
        assert "btn-action-delete" in resp.text

    def test_cleanup_page_no_config_shows_warning(self, tmp_path):
        client = self._make_client(tmp_path, with_config=False)
        resp = client.get("/cleanup")
        assert "Linear API key not configured" in resp.text

    def test_cleanup_page_shows_recent_batches(self, tmp_path):
        from botfarm.db import insert_cleanup_batch
        db_path = tmp_path / "cleanup_batches.db"
        conn = init_db(db_path)
        insert_cleanup_batch(
            conn,
            batch_id="test-batch-1",
            action="archive",
            team_key="SMA",
            total=10,
            succeeded=8,
            failed=2,
        )
        conn.commit()
        conn.close()
        cfg = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj", linear_team="SMA",
                    base_dir="/tmp", worktree_prefix="w", slots=[1],
                ),
            ],
            linear=LinearConfig(api_key="test-key"),
        )
        app = create_app(db_path=db_path, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.get("/cleanup")
        assert "Recent Operations" in resp.text
        assert "test-batch-1" in resp.text

    def test_cleanup_page_in_navigation(self, tmp_path):
        client = self._make_client(tmp_path)
        resp = client.get("/cleanup")
        assert 'aria-current="page"' in resp.text
        assert "Cleanup" in resp.text


class TestCleanupPreviewAPI:
    def _make_client(self, tmp_path):
        db_path = tmp_path / "cleanup_api.db"
        conn = init_db(db_path)
        conn.close()
        cfg = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj", linear_team="SMA",
                    base_dir="/tmp", worktree_prefix="w", slots=[1],
                ),
            ],
            linear=LinearConfig(api_key="test-key"),
        )
        app = create_app(db_path=db_path, botfarm_config=cfg)
        return TestClient(app)

    def test_preview_no_config(self, tmp_path):
        db_path = tmp_path / "no_cfg.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/api/cleanup/preview")
        assert resp.status_code == 503

    def test_preview_returns_candidates(self, tmp_path):
        from datetime import datetime, timedelta, timezone
        client = self._make_client(tmp_path)
        old_iso = (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        mock_nodes = [{
            "id": "uuid-1",
            "identifier": "SMA-100",
            "title": "Old issue",
            "updatedAt": old_iso,
            "completedAt": old_iso,
            "labels": {"nodes": []},
            "children": {"nodes": []},
        }]
        import httpx
        from botfarm.linear import LINEAR_API_URL
        resp_data = httpx.Response(
            status_code=200,
            json={"data": {"issues": {"nodes": mock_nodes}}},
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        with patch.object(httpx, "post", return_value=resp_data):
            resp = client.get("/api/cleanup/preview?limit=25&min_age_days=7")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["candidates"][0]["identifier"] == "SMA-100"

    def test_preview_empty_result(self, tmp_path):
        client = self._make_client(tmp_path)
        import httpx
        from botfarm.linear import LINEAR_API_URL
        resp_data = httpx.Response(
            status_code=200,
            json={"data": {"issues": {"nodes": []}}},
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        with patch.object(httpx, "post", return_value=resp_data):
            resp = client.get("/api/cleanup/preview")
        assert resp.status_code == 200
        assert resp.json()["total"] == 0


class TestCleanupExecuteAPI:
    def _make_client(self, tmp_path):
        db_path = tmp_path / "cleanup_exec.db"
        conn = init_db(db_path)
        conn.close()
        cfg = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj", linear_team="SMA",
                    base_dir="/tmp", worktree_prefix="w", slots=[1],
                ),
            ],
            linear=LinearConfig(api_key="test-key"),
        )
        app = create_app(db_path=db_path, botfarm_config=cfg)
        return TestClient(app)

    def test_execute_invalid_action(self, tmp_path):
        client = self._make_client(tmp_path)
        resp = client.post(
            "/api/cleanup/execute",
            json={"action": "purge"},
        )
        assert resp.status_code == 400

    def test_execute_no_config(self, tmp_path):
        db_path = tmp_path / "no_cfg.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.post(
            "/api/cleanup/execute",
            json={"action": "archive"},
        )
        assert resp.status_code == 503

    @patch("botfarm.linear_cleanup.time.sleep")
    def test_execute_archive_success(self, mock_sleep, tmp_path):
        from datetime import datetime, timedelta, timezone
        client = self._make_client(tmp_path)
        old_iso = (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        mock_nodes = [{
            "id": "uuid-1",
            "identifier": "SMA-100",
            "title": "Old issue",
            "updatedAt": old_iso,
            "completedAt": old_iso,
            "labels": {"nodes": []},
            "children": {"nodes": []},
        }]
        import httpx
        from botfarm.linear import LINEAR_API_URL

        fetch_resp = httpx.Response(
            status_code=200,
            json={"data": {"issues": {"nodes": mock_nodes}}},
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        detail_resp = httpx.Response(
            status_code=200,
            json={"data": {"issue": {
                "id": "uuid-1", "identifier": "SMA-100", "title": "Old issue",
                "description": None, "priority": 3,
                "url": "https://linear.app/test/SMA-100",
                "estimate": None, "dueDate": None,
                "createdAt": old_iso, "updatedAt": old_iso,
                "completedAt": old_iso,
                "state": {"name": "Done"},
                "creator": {"name": "User"},
                "assignee": {"name": "Bot", "email": "bot@test.com"},
                "project": {"name": "TestProj"},
                "team": {"name": "SMA", "key": "SMA"},
                "parent": None, "children": {"nodes": []},
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "inverseRelations": {"nodes": []},
                "comments": {"nodes": []},
            }}},
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        archive_resp = httpx.Response(
            status_code=200,
            json={"data": {"issueArchive": {"success": True}}},
            request=httpx.Request("POST", LINEAR_API_URL),
        )

        with patch.object(httpx, "post", side_effect=[fetch_resp, detail_resp, archive_resp]):
            resp = client.post(
                "/api/cleanup/execute",
                json={"action": "archive", "limit": 50},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["succeeded"] == 1
        assert data["batch_id"]

    def test_execute_cooldown_returns_429(self, tmp_path):
        from botfarm.db import insert_cleanup_batch
        db_path = tmp_path / "cooldown.db"
        conn = init_db(db_path)
        insert_cleanup_batch(conn, batch_id="recent", action="archive")
        conn.commit()
        conn.close()
        cfg = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj", linear_team="SMA",
                    base_dir="/tmp", worktree_prefix="w", slots=[1],
                ),
            ],
            linear=LinearConfig(api_key="test-key"),
        )
        app = create_app(db_path=db_path, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.post(
            "/api/cleanup/execute",
            json={"action": "archive"},
        )
        assert resp.status_code == 429
        assert "Cooldown" in resp.json()["error"]


class TestCleanupUndoAPI:
    def test_undo_nonexistent_batch(self, tmp_path):
        db_path = tmp_path / "undo.db"
        conn = init_db(db_path)
        conn.close()
        cfg = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj", linear_team="SMA",
                    base_dir="/tmp", worktree_prefix="w", slots=[1],
                ),
            ],
            linear=LinearConfig(api_key="test-key"),
        )
        app = create_app(db_path=db_path, botfarm_config=cfg)
        client = TestClient(app)
        resp = client.post("/api/cleanup/undo/nonexistent")
        assert resp.status_code == 400
        assert "not found" in resp.json()["error"]

    def test_undo_no_config(self, tmp_path):
        db_path = tmp_path / "undo_no_cfg.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.post("/api/cleanup/undo/some-batch")
        assert resp.status_code == 503


class TestCleanupBatchDetailAPI:
    def test_batch_detail_returns_data(self, tmp_path):
        from botfarm.db import insert_cleanup_batch, insert_cleanup_batch_item
        db_path = tmp_path / "batch_detail.db"
        conn = init_db(db_path)
        insert_cleanup_batch(
            conn, batch_id="detail-batch", action="archive", total=1,
        )
        insert_cleanup_batch_item(
            conn,
            batch_id="detail-batch",
            linear_uuid="uuid-1",
            identifier="SMA-100",
            action="archive",
            success=True,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/api/cleanup/batch/detail-batch")
        assert resp.status_code == 200
        data = resp.json()
        assert data["batch"]["action"] == "archive"
        assert len(data["items"]) == 1

    def test_batch_detail_not_found(self, tmp_path):
        db_path = tmp_path / "batch_404.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/api/cleanup/batch/nonexistent")
        assert resp.status_code == 404


class TestCleanupDBHelpers:
    def test_list_cleanup_batches(self, tmp_path):
        from botfarm.db import insert_cleanup_batch, list_cleanup_batches
        db_path = tmp_path / "list.db"
        conn = init_db(db_path)
        insert_cleanup_batch(conn, batch_id="b1", action="archive", total=5)
        insert_cleanup_batch(conn, batch_id="b2", action="delete", total=3)
        conn.commit()
        batches = list_cleanup_batches(conn)
        assert len(batches) == 2
        # Most recent first
        assert batches[0]["batch_id"] == "b2"
        conn.close()

    def test_list_cleanup_batches_empty(self, tmp_path):
        from botfarm.db import list_cleanup_batches
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        batches = list_cleanup_batches(conn)
        assert batches == []
        conn.close()

    def test_list_cleanup_batches_respects_limit(self, tmp_path):
        from botfarm.db import insert_cleanup_batch, list_cleanup_batches
        db_path = tmp_path / "limit.db"
        conn = init_db(db_path)
        for i in range(5):
            insert_cleanup_batch(conn, batch_id=f"b{i}", action="archive")
        conn.commit()
        batches = list_cleanup_batches(conn, limit=3)
        assert len(batches) == 3
        conn.close()


# ---------------------------------------------------------------------------
# Ticket History Browser (/tickets)
# ---------------------------------------------------------------------------


def _seed_ticket_history(conn, ticket_id, title, project_name="my-project",
                         status="Todo", priority="High", **overrides):
    """Helper to seed a ticket_history entry."""
    fields = {
        "ticket_id": ticket_id,
        "title": title,
        "project_name": project_name,
        "status": status,
        "priority": priority,
        "capture_source": "test",
        "url": f"https://linear.app/issue/{ticket_id}",
    }
    fields.update(overrides)
    upsert_ticket_history(conn, **fields)


@pytest.fixture()
def ticket_db(tmp_path):
    """Create a database with ticket_history data."""
    path = tmp_path / "tickets.db"
    conn = init_db(path)
    save_dispatch_state(conn, paused=False)
    _seed_ticket_history(
        conn, "TIK-1", "Fix login bug", project_name="my-project",
        status="Done", priority="Urgent",
        description="## Description\nFix the login bug",
        assignee_name="Alice", creator_name="Bob",
        team_name="Engineering", labels=json.dumps(["bug", "auth"]),
        branch_name="fix-login", pr_url="https://github.com/org/repo/pull/10",
        blocked_by=json.dumps(["TIK-3"]),
        comments_json=json.dumps([
            {"user": "Alice", "body": "Working on it", "created_at": "2026-02-28T10:00:00Z"},
        ]),
    )
    _seed_ticket_history(
        conn, "TIK-2", "Add dark mode", project_name="other-project",
        status="In Progress", priority="Normal",
        deleted_from_linear=1,
    )
    _seed_ticket_history(
        conn, "TIK-3", "API refactor", project_name="my-project",
        status="Todo", priority="High",
        blocks=json.dumps(["TIK-1"]),
        children_ids=json.dumps(["TIK-4"]),
    )
    # Also insert a task matching TIK-1 so the "View Execution History" link works
    task_id = insert_task(conn, ticket_id="TIK-1", title="Fix login bug",
                          project="my-project", slot=1, status="completed")
    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def ticket_client(ticket_db):
    """FastAPI test client with ticket data."""
    app = create_app(db_path=ticket_db)
    return TestClient(app)


class TestTicketsPage:
    def test_returns_200(self, ticket_client):
        resp = ticket_client.get("/tickets")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_contains_ticket_data(self, ticket_client):
        resp = ticket_client.get("/tickets")
        body = resp.text
        assert "TIK-1" in body
        assert "TIK-2" in body
        assert "TIK-3" in body

    def test_contains_filter_form(self, ticket_client):
        resp = ticket_client.get("/tickets")
        body = resp.text
        assert "ticket-filters" in body
        assert "Search" in body
        assert "Project" in body
        assert "Status" in body
        assert "Deleted" in body

    def test_contains_all_columns(self, ticket_client):
        resp = ticket_client.get("/tickets")
        body = resp.text
        assert "Ticket" in body
        assert "Title" in body
        assert "Project" in body
        assert "Priority" in body
        assert "Labels" in body
        assert "Captured" in body
        assert "In Linear" in body

    def test_deleted_indicator(self, ticket_client):
        resp = ticket_client.get("/tickets")
        body = resp.text
        assert "line-through" in body
        assert "Deleted" in body

    def test_project_dropdown_populated(self, ticket_client):
        resp = ticket_client.get("/tickets")
        body = resp.text
        assert "my-project" in body
        assert "other-project" in body

    def test_ticket_rows_are_clickable(self, ticket_client):
        resp = ticket_client.get("/tickets")
        assert "/tickets/TIK-1" in resp.text

    def test_tickets_no_db(self, tmp_path):
        app = create_app(db_path=tmp_path / "nonexistent.db")
        client = TestClient(app)
        resp = client.get("/tickets")
        assert resp.status_code == 200
        assert "No tickets found" in resp.text

    def test_nav_contains_tickets_link(self, ticket_client):
        resp = ticket_client.get("/tickets")
        body = resp.text
        assert 'href="/tickets"' in body
        assert "Tickets" in body


class TestTicketsFilters:
    def test_filter_by_project(self, ticket_client):
        resp = ticket_client.get("/tickets?project=my-project")
        body = resp.text
        assert "TIK-1" in body
        assert "TIK-3" in body
        assert "TIK-2" not in body

    def test_filter_by_status(self, ticket_client):
        resp = ticket_client.get("/tickets?status=Done")
        body = resp.text
        assert "TIK-1" in body
        assert "TIK-2" not in body
        assert "TIK-3" not in body

    def test_filter_deleted_yes(self, ticket_client):
        resp = ticket_client.get("/tickets?deleted=yes")
        body = resp.text
        assert "TIK-2" in body
        assert "TIK-1" not in body

    def test_filter_deleted_no(self, ticket_client):
        resp = ticket_client.get("/tickets?deleted=no")
        body = resp.text
        assert "TIK-1" in body
        assert "TIK-3" in body
        assert "TIK-2" not in body

    def test_search_by_ticket_id(self, ticket_client):
        resp = ticket_client.get("/tickets?search=TIK-2")
        body = resp.text
        assert "TIK-2" in body
        assert "TIK-1" not in body

    def test_search_by_title(self, ticket_client):
        resp = ticket_client.get("/tickets?search=dark mode")
        body = resp.text
        assert "TIK-2" in body

    def test_sort_by_title_asc(self, ticket_client):
        resp = ticket_client.get("/tickets?sort_by=title&sort_dir=ASC")
        assert resp.status_code == 200
        body = resp.text
        # "API refactor" < "Add dark mode" < "Fix login bug"
        assert body.index("TIK-2") < body.index("TIK-1")

    def test_invalid_sort_column_defaults_gracefully(self, ticket_client):
        resp = ticket_client.get("/tickets?sort_by=DROP TABLE&sort_dir=ASC")
        assert resp.status_code == 200

    def test_combined_filters(self, ticket_client):
        resp = ticket_client.get("/tickets?project=my-project&status=Done")
        body = resp.text
        assert "TIK-1" in body
        assert "TIK-3" not in body


class TestTicketsPagination:
    def test_pagination_info_shown(self, ticket_client):
        resp = ticket_client.get("/tickets")
        body = resp.text
        assert "page 1 of 1" in body
        assert "3 tickets found" in body

    def test_page_param(self, ticket_client):
        resp = ticket_client.get("/tickets?page=1")
        assert resp.status_code == 200

    def test_invalid_page_defaults_to_1(self, ticket_client):
        resp = ticket_client.get("/tickets?page=abc")
        assert resp.status_code == 200

    def test_pagination_with_many_tickets(self, tmp_path):
        path = tmp_path / "big_tickets.db"
        conn = init_db(path)
        save_dispatch_state(conn, paused=False)
        for i in range(30):
            _seed_ticket_history(conn, f"BIG-{i}", f"Ticket {i}")
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/tickets")
        body = resp.text
        assert "page 1 of 2" in body
        assert "Next" in body

        resp2 = client.get("/tickets?page=2")
        body2 = resp2.text
        assert "page 2 of 2" in body2
        assert "Prev" in body2


class TestPartialTickets:
    def test_returns_200(self, ticket_client):
        resp = ticket_client.get("/partials/tickets")
        assert resp.status_code == 200

    def test_contains_ticket_data(self, ticket_client):
        resp = ticket_client.get("/partials/tickets")
        body = resp.text
        assert "TIK-1" in body

    def test_respects_filters(self, ticket_client):
        resp = ticket_client.get("/partials/tickets?project=other-project")
        body = resp.text
        assert "TIK-2" in body
        assert "TIK-1" not in body


class TestTicketDetailPage:
    def test_returns_200(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_contains_ticket_content(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "Fix login bug" in body
        assert "TIK-1" in body
        assert "Done" in body
        assert "Urgent" in body

    def test_contains_metadata(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "Alice" in body
        assert "Bob" in body
        assert "Engineering" in body

    def test_contains_description(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "Fix the login bug" in body

    def test_contains_labels(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "bug" in body
        assert "auth" in body

    def test_contains_dependencies(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "Blocked By" in body
        assert "TIK-3" in body

    def test_contains_comments(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "Working on it" in body

    def test_contains_pr_and_branch(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "github.com/org/repo/pull/10" in body
        assert "fix-login" in body

    def test_contains_execution_history_link(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "View Execution History" in body
        assert "/task/TIK-1" in body

    def test_deleted_ticket_shows_indicator(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-2")
        body = resp.text
        assert "Deleted from Linear" in body
        assert "line-through" in body

    def test_deleted_ticket_no_view_in_linear(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-2")
        body = resp.text
        assert "View in Linear" not in body

    def test_nonexistent_ticket_shows_not_found(self, ticket_client):
        resp = ticket_client.get("/tickets/NOPE-999")
        assert resp.status_code == 200
        body = resp.text
        assert "Ticket not found" in body

    def test_hierarchy_shown(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-3")
        body = resp.text
        assert "Blocks" in body
        assert "Children" in body
        assert "TIK-4" in body

    def test_view_in_linear_for_active_ticket(self, ticket_client):
        resp = ticket_client.get("/tickets/TIK-1")
        body = resp.text
        assert "View in Linear" in body


class TestTaskDetailTicketContent:
    """Test the collapsible Ticket Content section on /task/{id}."""

    def test_task_detail_shows_ticket_content(self, ticket_db):
        app = create_app(db_path=ticket_db)
        client = TestClient(app)
        resp = client.get("/task/TIK-1")
        body = resp.text
        assert "Ticket Content" in body
        assert "View full ticket" in body

    def test_task_detail_no_ticket_content(self, db_file):
        """When no ticket_history exists, the section should not appear."""
        client_obj = TestClient(create_app(db_path=db_file))
        resp = client_obj.get("/task/TST-1")
        body = resp.text
        assert "Ticket Content" not in body


class TestHistoryTicketLink:
    """Test that history rows have a link to /tickets/{id}."""

    def test_history_has_ticket_icon_link(self, ticket_db):
        # Seed a task so it appears in history
        app = create_app(db_path=ticket_db)
        client = TestClient(app)
        resp = client.get("/history")
        body = resp.text
        assert "/tickets/TIK-1" in body


class TestStopSlotAPI:
    """Tests for POST /api/slot/stop endpoint."""

    def test_stop_calls_callback(self, db_file):
        """POST /api/slot/stop calls on_stop_slot with project and slot_id."""
        called = []
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: called.append((p, s)),
        )
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"project": "my-project", "slot_id": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "requested"
        assert "my-project/1" in data["message"]
        assert called == [("my-project", 1)]

    def test_stop_without_callback_returns_503(self, db_file):
        """POST /api/slot/stop returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"project": "proj", "slot_id": 1})
        assert resp.status_code == 503

    def test_stop_missing_project_returns_400(self, db_file):
        """POST /api/slot/stop returns 400 when project is missing."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"slot_id": 1})
        assert resp.status_code == 400

    def test_stop_missing_slot_id_returns_400(self, db_file):
        """POST /api/slot/stop returns 400 when slot_id is missing."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"project": "proj"})
        assert resp.status_code == 400

    def test_stop_invalid_slot_id_returns_400(self, db_file):
        """POST /api/slot/stop returns 400 when slot_id is not an integer."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"project": "proj", "slot_id": "abc"})
        assert resp.status_code == 400

    def test_stop_slot_id_coerced_from_string(self, db_file):
        """POST /api/slot/stop coerces slot_id from string to int."""
        called = []
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: called.append((p, s)),
        )
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"project": "proj", "slot_id": "2"})
        assert resp.status_code == 200
        assert called == [("proj", 2)]

    def test_stop_non_dict_body_returns_400(self, db_file):
        """POST /api/slot/stop returns 400 when body is not a JSON object."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        for payload in ["[]", '"string"', "123", "null"]:
            resp = client.post(
                "/api/slot/stop",
                content=payload,
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 400, f"Expected 400 for body={payload!r}"
            assert "Expected a JSON object" in resp.json()["error"]

    def test_stop_boolean_slot_id_returns_400(self, db_file):
        """POST /api/slot/stop rejects boolean slot_id (true/false)."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"project": "proj", "slot_id": True})
        assert resp.status_code == 400
        assert "slot_id must be an integer" in resp.json()["error"]

    def test_stop_float_slot_id_returns_400(self, db_file):
        """POST /api/slot/stop rejects float slot_id."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        resp = client.post("/api/slot/stop", json={"project": "proj", "slot_id": 1.5})
        assert resp.status_code == 400
        assert "slot_id must be an integer" in resp.json()["error"]

    def test_stop_non_string_project_returns_400(self, db_file):
        """POST /api/slot/stop rejects non-string project values."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        for payload in [1, ["a"], {"x": 1}, True]:
            resp = client.post(
                "/api/slot/stop", json={"project": payload, "slot_id": 1},
            )
            assert resp.status_code == 400, f"Expected 400 for project={payload!r}"
            assert "project must be a string" in resp.json()["error"]

    def test_stop_with_matching_ticket_id_succeeds(self, db_file):
        """POST /api/slot/stop succeeds when ticket_id matches current slot."""
        called = []
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: called.append((p, s)),
        )
        client = TestClient(app)
        resp = client.post(
            "/api/slot/stop",
            json={"project": "my-project", "slot_id": 1, "ticket_id": "TST-1"},
        )
        assert resp.status_code == 200
        assert called == [("my-project", 1)]

    def test_stop_with_mismatched_ticket_id_returns_409(self, db_file):
        """POST /api/slot/stop returns 409 when ticket_id doesn't match."""
        called = []
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: called.append((p, s)),
        )
        client = TestClient(app)
        resp = client.post(
            "/api/slot/stop",
            json={"project": "my-project", "slot_id": 1, "ticket_id": "TST-999"},
        )
        assert resp.status_code == 409
        assert "ticket has changed" in resp.json()["error"]
        assert called == []

    def test_stop_with_ticket_id_rejects_free_slot(self, db_file):
        """POST /api/slot/stop returns 409 when slot is no longer stoppable."""
        called = []
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: called.append((p, s)),
        )
        client = TestClient(app)
        # Slot 2 is free in the db_file fixture
        resp = client.post(
            "/api/slot/stop",
            json={"project": "my-project", "slot_id": 2, "ticket_id": "TST-1"},
        )
        assert resp.status_code == 409
        assert "no longer stoppable" in resp.json()["error"]
        assert called == []

    def test_stop_without_ticket_id_still_works(self, db_file):
        """POST /api/slot/stop still works without ticket_id (backward compat)."""
        called = []
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: called.append((p, s)),
        )
        client = TestClient(app)
        resp = client.post(
            "/api/slot/stop",
            json={"project": "my-project", "slot_id": 1},
        )
        assert resp.status_code == 200
        assert called == [("my-project", 1)]

    def test_stop_non_string_ticket_id_returns_400(self, db_file):
        """POST /api/slot/stop rejects non-string ticket_id."""
        app = create_app(
            db_path=db_file,
            on_stop_slot=lambda p, s: None,
        )
        client = TestClient(app)
        resp = client.post(
            "/api/slot/stop",
            json={"project": "my-project", "slot_id": 1, "ticket_id": 123},
        )
        assert resp.status_code == 400
        assert "ticket_id must be a string" in resp.json()["error"]


class TestStopSlotButton:
    """Tests for the stop button in the slots partial."""

    def test_busy_slot_has_stop_button(self, tmp_path):
        """A busy slot should show a Stop button."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "my-project", 1, status="busy",
            ticket_id="TST-1", ticket_title="Fix bug",
            stage="implement", stage_iteration=1,
            started_at="2026-02-12T10:00:00+00:00", pid=1234,
        )
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert "stop-slot-btn" in resp.text
        assert "Stop" in resp.text

    def test_free_slot_has_no_stop_button(self, tmp_path):
        """A free slot should not show a Stop button."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert "stop-slot-btn" not in resp.text

    def test_paused_limit_slot_has_stop_button(self, tmp_path):
        """A paused_limit slot should show a Stop button."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "my-project", 1, status="paused_limit",
            ticket_id="TST-1", ticket_title="Fix bug",
            stage="review", stage_iteration=1,
            started_at="2026-02-12T10:00:00+00:00",
            interrupted_by_limit=True,
        )
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert "stop-slot-btn" in resp.text

    def test_paused_manual_slot_has_stop_button(self, tmp_path):
        """A paused_manual slot should show a Stop button."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "my-project", 1, status="paused_manual",
            ticket_id="TST-1", ticket_title="Fix bug",
            stage="implement", stage_iteration=1,
            started_at="2026-02-12T10:00:00+00:00",
        )
        save_dispatch_state(conn, paused=True, reason="manual_pause")
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert "stop-slot-btn" in resp.text

    def test_stop_modal_present_on_full_page(self, tmp_path):
        """The stop modal lives in index.html (outside htmx-refreshed partial)."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "my-project", 1, status="busy",
            ticket_id="TST-1", ticket_title="Fix bug",
            stage="implement", stage_iteration=1,
            started_at="2026-02-12T10:00:00+00:00", pid=1234,
        )
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert 'id="stop-slot-modal"' in resp.text
        assert "Kill the running worker process" in resp.text

    def test_stop_modal_not_in_partial(self, tmp_path):
        """The modal should NOT be in the slots partial (to survive htmx refresh)."""
        path = tmp_path / "test.db"
        conn = init_db(path)
        _seed_slot(
            conn, "my-project", 1, status="busy",
            ticket_id="TST-1", ticket_title="Fix bug",
            stage="implement", stage_iteration=1,
            started_at="2026-02-12T10:00:00+00:00", pid=1234,
        )
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert 'id="stop-slot-modal"' not in resp.text


class TestAddSlotAPI:
    """Tests for POST /api/slot/add endpoint."""

    def test_add_calls_callback(self, db_file):
        """POST /api/slot/add calls on_add_slot with project."""
        called = []
        app = create_app(
            db_path=db_file,
            on_add_slot=lambda p: called.append(p),
        )
        client = TestClient(app)
        resp = client.post("/api/slot/add", json={"project": "my-project"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "requested"
        assert "my-project" in data["message"]
        assert called == ["my-project"]

    def test_add_without_callback_returns_503(self, db_file):
        """POST /api/slot/add returns 503 when no callback is registered."""
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/api/slot/add", json={"project": "proj"})
        assert resp.status_code == 503

    def test_add_missing_project_returns_400(self, db_file):
        """POST /api/slot/add returns 400 when project is missing."""
        app = create_app(
            db_path=db_file,
            on_add_slot=lambda p: None,
        )
        client = TestClient(app)
        resp = client.post("/api/slot/add", json={})
        assert resp.status_code == 400

    def test_add_non_string_project_returns_400(self, db_file):
        """POST /api/slot/add rejects non-string project values."""
        app = create_app(
            db_path=db_file,
            on_add_slot=lambda p: None,
        )
        client = TestClient(app)
        for payload in [1, ["a"], {"x": 1}, True]:
            resp = client.post("/api/slot/add", json={"project": payload})
            assert resp.status_code == 400, f"Expected 400 for project={payload!r}"
            assert "project must be a string" in resp.json()["error"]

    def test_add_non_dict_body_returns_400(self, db_file):
        """POST /api/slot/add returns 400 when body is not a JSON object."""
        app = create_app(
            db_path=db_file,
            on_add_slot=lambda p: None,
        )
        client = TestClient(app)
        for payload in ["[]", '"string"', "123", "null"]:
            resp = client.post(
                "/api/slot/add",
                content=payload,
                headers={"Content-Type": "application/json"},
            )
            assert resp.status_code == 400, f"Expected 400 for body={payload!r}"

    def test_add_invalid_json_returns_400(self, db_file):
        """POST /api/slot/add returns 400 for invalid JSON."""
        app = create_app(
            db_path=db_file,
            on_add_slot=lambda p: None,
        )
        client = TestClient(app)
        resp = client.post(
            "/api/slot/add",
            content="not json",
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 400
