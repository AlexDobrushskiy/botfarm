-- Migration 021: Add resolve_conflict stage and merge_conflict_loop for
-- handling merge conflicts when parallel slots cause branch divergence.

-- Shift stages at or after merge's current position up by 1, making room
-- for resolve_conflict before merge.  Uses a negative temp offset to avoid
-- unique (pipeline_id, stage_order) collisions during the shift.
UPDATE stage_templates SET stage_order = -(stage_order + 1)
    WHERE pipeline_id = 1 AND stage_order >= 6;
UPDATE stage_templates SET stage_order = -stage_order
    WHERE pipeline_id = 1 AND stage_order < 0;

-- New stage template: resolve_conflict (Claude merges main into feature branch)
INSERT INTO stage_templates (pipeline_id, name, stage_order, executor_type, identity, prompt_template, max_turns, timeout_minutes, result_parser)
VALUES (
    1, 'resolve_conflict', 6, 'claude', 'coder',
    'The feature branch has merge conflicts with main. Resolve them by running:

1. git fetch origin main
2. git merge origin/main
3. Resolve any merge conflicts — keep the intent of both the feature branch and main changes
4. Run the full test suite and fix any test failures
5. git add the resolved files and commit
6. git push

Do NOT force push. Do NOT rebase. Use a merge commit.',
    100, 60, NULL
);

-- New stage loop: merge_conflict_loop
INSERT INTO stage_loops (pipeline_id, name, start_stage, end_stage, max_iterations, config_key, exit_condition, on_failure_stage)
VALUES (
    1, 'merge_conflict_loop', 'resolve_conflict', 'merge', 2, 'max_merge_conflict_retries', 'merge_succeeded', 'resolve_conflict'
);

-- Track merge conflict retry count per task
ALTER TABLE tasks ADD COLUMN merge_conflict_retries INTEGER DEFAULT 0;
