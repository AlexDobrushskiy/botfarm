# Database Schema

Botfarm uses a single SQLite database (`~/.botfarm/botfarm.db`) in WAL mode with foreign keys enabled. Schema is versioned — migrations run automatically on startup (see `db.py`).

Current schema version: **39**

## Tables

### `tasks`

One row per ticket dispatch. Multiple rows can exist for the same `ticket_id` (e.g. A/B comparison via redispatch).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `ticket_id` | TEXT | Ticket identifier (e.g. `SMA-123`) |
| `title` | TEXT | Ticket title |
| `project` | TEXT | Project name from config |
| `slot` | INTEGER | Slot ID that processed this ticket |
| `status` | TEXT | `pending`, `in_progress`, `completed`, or `failed` |
| `created_at` | TEXT | ISO-8601 timestamp |
| `started_at` | TEXT | When work began |
| `completed_at` | TEXT | When work finished (success or failure) |
| `turns` | INTEGER | Total agentic turns across all stages |
| `review_iterations` | INTEGER | Number of review→fix cycles |
| `comments` | TEXT | Accumulated review comments |
| `limit_interruptions` | INTEGER | Times paused by usage limits |
| `failure_reason` | TEXT | Error message if failed |
| `pr_url` | TEXT | GitHub PR URL |
| `pipeline_stage` | TEXT | Last pipeline stage reached |
| `review_state` | TEXT | Last review verdict |
| `started_on_extra_usage` | INTEGER | 1 if task started during extra usage period |
| `merge_conflict_retries` | INTEGER | Number of merge conflict resolution attempts |
| `result_text` | TEXT | Agent result text (e.g. investigation findings, QA report) |
| `failure_category` | TEXT | Failure classification (see below) |
| `pipeline_id` | INTEGER FK | References `pipeline_templates(id)` — which pipeline ran this task |

**Failure categories:** `env_missing_runtime`, `env_missing_package`, `env_missing_service`, `env_missing_config`, `code_failure`, `limit_hit`, `timeout`

### `stage_runs`

One row per agent subprocess invocation. A single task has multiple stage runs (implement, review, fix, etc.).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `task_id` | INTEGER FK | References `tasks(id)` |
| `stage` | TEXT | Stage name (e.g. `implement`, `review`, `fix`, `pr_checks`, `ci_fix`, `resolve_conflict`, `merge`, `qa`) |
| `iteration` | INTEGER | Iteration number (1-based, relevant for review/fix cycles) |
| `session_id` | TEXT | Agent session ID |
| `turns` | INTEGER | Agentic turns in this stage |
| `duration_seconds` | REAL | Wall-clock duration |
| `exit_subtype` | TEXT | How the agent exited (e.g. `end_turn`, `max_turns`) |
| `was_limit_restart` | INTEGER | 1 if this run was a resume after a limit pause |
| `created_at` | TEXT | ISO-8601 timestamp |
| `input_tokens` | INTEGER | Input tokens consumed |
| `output_tokens` | INTEGER | Output tokens consumed |
| `cache_read_input_tokens` | INTEGER | Tokens served from cache |
| `cache_creation_input_tokens` | INTEGER | Tokens written to cache |
| `total_cost_usd` | REAL | Estimated cost in USD |
| `context_fill_pct` | REAL | Context window utilization (0.0–1.0) |
| `model_usage_json` | TEXT | Per-model token breakdown (JSON) |
| `log_file_path` | TEXT | Path to the agent subprocess log file |
| `on_extra_usage` | INTEGER | 1 if this run occurred during extra usage |
| `pipeline_id` | INTEGER FK | References `pipeline_templates(id)` |

### `usage_snapshots`

Periodic snapshots of Anthropic API utilization, recorded each poll cycle.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `utilization_5h` | REAL | 5-hour rolling utilization (0.0–1.0) |
| `utilization_7d` | REAL | 7-day rolling utilization (0.0–1.0) |
| `resets_at` | TEXT | When 5-hour window resets (ISO-8601) |
| `resets_at_7d` | TEXT | When 7-day window resets (ISO-8601) |
| `extra_usage_enabled` | INTEGER | 1 if extra usage is enabled on the account |
| `extra_usage_monthly_limit` | REAL | Monthly extra usage limit in dollars |
| `extra_usage_used_credits` | REAL | Extra usage credits consumed this month in dollars |
| `extra_usage_utilization` | REAL | Extra usage utilization (0.0–1.0) |
| `created_at` | TEXT | ISO-8601 timestamp |

### `task_events`

Append-only event log for observability. Every significant action is recorded here.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `task_id` | INTEGER FK | References `tasks(id)` — NULL for system-level events |
| `event_type` | TEXT | Event category (see below) |
| `detail` | TEXT | Free-form context (JSON or plain text) |
| `created_at` | TEXT | ISO-8601 timestamp |

**Event types:**

| Event type | Scope | Description |
|------------|-------|-------------|
| `supervisor_start` | system | Supervisor process started |
| `supervisor_stop` | system | Supervisor shutting down |
| `setup_mode_start` | system | Entered setup mode (project add/remove) |
| `setup_mode_exit` | system | Exited setup mode |
| `preflight_failed` | system | Pre-run checks failed |
| `preflight_recovered` | system | Pre-run checks passed after prior failure |
| `start_paused` | system | Supervisor started in paused state |
| `start_paused_cleared` | system | Initial paused state cleared |
| `core_bare_repaired` | system | Bare repo repaired during startup |
| `update_started` | system | Self-update initiated |
| `update_pulling` | system | Pulling updates from remote |
| `update_failed` | system | Self-update failed |
| `update_complete` | system | Self-update completed successfully |
| `capacity_warning` | system | Tracker issue count approaching limit |
| `capacity_critical` | system | Tracker issue count near limit |
| `capacity_blocked` | system | Tracker issue count at/above limit, dispatch blocked |
| `capacity_cleared` | system | Tracker issue count dropped below warning threshold |
| `dispatch_paused` | system | All dispatch paused (usage limits) |
| `dispatch_resumed` | system | Dispatch unpaused |
| `startup_recovery` | system | Recovering slots after supervisor restart |
| `project_added` | system | New project added to configuration |
| `project_removed` | system | Project removed from configuration |
| `slot_added` | system | New slot added to a project |
| `daily_summary_sent` | system | Daily summary notification sent |
| `refactoring_analysis_scheduled` | system | Refactoring analysis ticket created |
| `worker_dispatched` | task | Worker subprocess spawned for a ticket |
| `slot_completed` | task | Slot finished successfully |
| `slot_failed` | task | Slot finished with failure |
| `slot_stopped` | task | Slot stopped manually |
| `slot_reconciled` | task | Slot state corrected during reconciliation |
| `timeout` | task | Stage timed out and was killed |
| `limit_hit` | task | Usage limit threshold reached, slot paused |
| `limit_resumed` | task | Usage dropped below threshold, slot resumed |
| `manual_dispatch` | task | Ticket dispatched manually (dashboard/CLI) |
| `redispatch` | task | Ticket re-dispatched for A/B comparison |
| `manual_pause_requested` | task | Manual pause requested for a slot |
| `manual_pause_complete` | task | Manual pause completed |
| `manual_resumed` | task | Manually resumed a paused slot |
| `parent_auto_closed` | task | Parent issue auto-closed |
| `review_iteration_started` | task | Review→fix cycle started |
| `review_approved` | task | Reviewer approved the implementation |
| `max_iterations_reached` | task | Hit max review iterations |
| `claude_review_completed` | task | Claude review stage completed |
| `codex_review_started` | task | Codex review started |
| `codex_review_completed` | task | Codex review completed |
| `codex_review_failed` | task | Codex review failed |
| `codex_review_skipped` | task | Codex review skipped (not configured or not applicable) |
| `ci_retry_started` | task | CI fix retry started |
| `ci_passed_after_retry` | task | CI passed after a fix attempt |
| `max_ci_retries_reached` | task | Hit max CI retry attempts |
| `merge_conflict_retry_started` | task | Merge conflict resolution attempt started |
| `merge_succeeded_after_conflict_retry` | task | Merge succeeded after conflict resolution |
| `max_merge_conflict_retries_reached` | task | Hit max merge conflict retry attempts |
| `qa_ticket_triggered` | task | QA ticket auto-created after implementation |
| `auth_retry` | task | Agent auth refresh retried |
| `runtime_config_refreshed` | task | Runtime config reloaded during worker execution |
| `base_dir_reset` | task | Base directory reset to clean state |
| `base_dir_reset_failed` | task | Base directory reset failed |
| `worker_reattached` | task | Reattached to a still-running worker process |
| `recovery_completed` | task | Recovered slot completed successfully |
| `recovery_failed` | task | Recovered slot failed |
| `recovery_resumed` | task | Recovered paused slot resumed |
| `recovery_timeout` | task | Recovered slot timed out |

### `slots`

Runtime state for each worker slot. Primary key is `(project, slot_id)`.

| Column | Type | Description |
|--------|------|-------------|
| `project` | TEXT PK | Project name |
| `slot_id` | INTEGER PK | Slot number |
| `status` | TEXT | `free`, `busy`, `paused_limit`, `paused_manual`, `failed`, or `completed_pending_cleanup` |
| `ticket_id` | TEXT | Currently assigned ticket |
| `ticket_title` | TEXT | Ticket title |
| `ticket_labels` | TEXT | JSON array of ticket label names (default `'[]'`) |
| `branch` | TEXT | Git branch name |
| `pr_url` | TEXT | PR URL if created |
| `stage` | TEXT | Current pipeline stage |
| `stage_iteration` | INTEGER | Current iteration within stage |
| `current_session_id` | TEXT | Agent session ID for resume |
| `started_at` | TEXT | When slot became busy |
| `stage_started_at` | TEXT | When current stage started |
| `sigterm_sent_at` | TEXT | When SIGTERM was sent (for timeout) |
| `pid` | INTEGER | Worker process ID |
| `interrupted_by_limit` | INTEGER | 1 if paused by usage limit |
| `resume_after` | TEXT | ISO-8601 time to resume after limit pause |
| `stages_completed` | TEXT | JSON array of completed stage names |
| `updated_at` | TEXT | Last modification timestamp |

### `dispatch_state`

Singleton row (id=1) tracking global dispatch state.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Always 1 |
| `paused` | INTEGER | 1 if dispatch is paused |
| `pause_reason` | TEXT | Why dispatch is paused |
| `supervisor_heartbeat` | TEXT | Last heartbeat timestamp |
| `tracker_issue_count` | INTEGER | Current issue count from bugtracker |
| `tracker_issue_limit` | INTEGER | Issue limit (default 250) |
| `tracker_capacity_checked_at` | TEXT | Last capacity check timestamp |
| `tracker_capacity_by_project` | TEXT | Per-project issue counts (JSON) |
| `updated_at` | TEXT | Last modification timestamp |

### `queue_entries`

Snapshot of the ticket queue at each poll cycle.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `project` | TEXT | Project name |
| `position` | INTEGER | Queue position |
| `ticket_id` | TEXT | Ticket identifier |
| `ticket_title` | TEXT | Ticket title |
| `priority` | INTEGER | Ticket priority |
| `sort_order` | REAL | Sort order value |
| `url` | TEXT | Ticket URL |
| `blocked_by` | TEXT | Blocking ticket IDs |
| `snapshot_at` | TEXT | When this snapshot was taken |

### `project_pause_state`

Per-project pause state for selective dispatch control.

| Column | Type | Description |
|--------|------|-------------|
| `project` | TEXT PK | Project name |
| `paused` | INTEGER | 1 if project is paused (default 0) |
| `pause_reason` | TEXT | Why project is paused |
| `updated_at` | TEXT | Last modification timestamp |

### `pipeline_templates`

Pipeline definitions — each represents a ticket-processing workflow.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `name` | TEXT UNIQUE | Pipeline name (e.g. `implementation`, `investigation`, `manual-qa`) |
| `description` | TEXT | Human-readable description |
| `ticket_label` | TEXT | Ticket label that routes to this pipeline (NULL for default) |
| `is_default` | INTEGER | 1 if this is the default pipeline (default 0) |
| `mcp_servers` | TEXT | Additional MCP server config (JSON), merged into base config at runtime |
| `created_at` | TEXT | ISO-8601 timestamp |

### `stage_templates`

Stage definitions within pipelines — each is one step in a pipeline.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `pipeline_id` | INTEGER FK | References `pipeline_templates(id)` |
| `name` | TEXT | Stage name (e.g. `implement`, `review`, `fix`) |
| `stage_order` | INTEGER | Execution order within the pipeline |
| `executor_type` | TEXT | `claude`, `codex`, `aider`, `shell`, or `internal` |
| `identity` | TEXT | Agent identity profile |
| `prompt_template` | TEXT | Prompt template with `{variable}` placeholders |
| `max_turns` | INTEGER | Maximum agentic turns |
| `timeout_minutes` | INTEGER | Stage timeout in minutes |
| `shell_command` | TEXT | Shell command (for `shell` executor type) |
| `result_parser` | TEXT | Result parser name (e.g. `qa_report`) |
| `model` | TEXT | Model override for this stage (NULL = use default) |
| `effort` | TEXT | Reasoning effort level (NULL = use default) |
| `context_window` | INTEGER | Context window size in tokens (NULL = default 200k) |
| `thinking_mode` | TEXT | Thinking mode: `enabled`, `adaptive`, `disabled` (NULL = inherit default) |
| `created_at` | TEXT | ISO-8601 timestamp |

Unique constraints: `(pipeline_id, name)`, `(pipeline_id, stage_order)`.

### `stage_loops`

Loop definitions — describe which stages can iterate (e.g. review→fix cycle).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `pipeline_id` | INTEGER FK | References `pipeline_templates(id)` |
| `name` | TEXT | Loop name (e.g. `review_loop`, `ci_retry_loop`) |
| `start_stage` | TEXT | First stage in the loop |
| `end_stage` | TEXT | Last stage in the loop |
| `max_iterations` | INTEGER | Maximum iterations allowed |
| `config_key` | TEXT | Config key for runtime override of max iterations |
| `exit_condition` | TEXT | Condition to exit the loop early |
| `on_failure_stage` | TEXT | Stage to jump to if max iterations exhausted |
| `created_at` | TEXT | ISO-8601 timestamp |

Unique constraint: `(pipeline_id, name)`.

### `ticket_history`

Local backup of ticket data from the bugtracker.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `ticket_id` | TEXT UNIQUE | Ticket identifier |
| `tracker_uuid` | TEXT | Bugtracker-internal UUID |
| `title` | TEXT | Ticket title |
| `description` | TEXT | Ticket description |
| `status` | TEXT | Ticket status |
| `priority` | INTEGER | Priority level |
| `url` | TEXT | Ticket URL |
| `assignee_name` | TEXT | Assignee display name |
| `assignee_email` | TEXT | Assignee email |
| `creator_name` | TEXT | Creator display name |
| `project_name` | TEXT | Project name |
| `team_name` | TEXT | Team name |
| `estimate` | REAL | Size estimate |
| `due_date` | TEXT | Due date |
| `parent_id` | TEXT | Parent ticket ID |
| `children_ids` | TEXT | JSON array of child ticket IDs |
| `blocked_by` | TEXT | JSON array of blocking ticket IDs |
| `blocks` | TEXT | JSON array of tickets this blocks |
| `labels` | TEXT | JSON array of label names |
| `comments_json` | TEXT | JSON array of comments |
| `pr_url` | TEXT | Associated PR URL |
| `branch_name` | TEXT | Associated branch name |
| `tracker_created_at` | TEXT | When ticket was created in bugtracker |
| `tracker_updated_at` | TEXT | When ticket was last updated in bugtracker |
| `tracker_completed_at` | TEXT | When ticket was completed in bugtracker |
| `captured_at` | TEXT | When this snapshot was taken |
| `capture_source` | TEXT | What triggered the capture |
| `raw_json` | TEXT | Raw bugtracker API response |
| `deleted_from_linear` | INTEGER | 1 if deleted from the tracker (default 0) |

### `cleanup_batches`

Tracks bulk archive/delete operations on bugtracker issues.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `batch_id` | TEXT UNIQUE | Unique batch identifier |
| `action` | TEXT | `archive` or `delete` |
| `team_key` | TEXT | Team identifier |
| `project_name` | TEXT | Project name |
| `total` | INTEGER | Total items in batch |
| `succeeded` | INTEGER | Items processed successfully |
| `failed` | INTEGER | Items that failed |
| `skipped` | INTEGER | Items skipped |
| `created_at` | TEXT | ISO-8601 timestamp |

### `cleanup_batch_items`

Individual items within a cleanup batch.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `batch_id` | TEXT FK | References `cleanup_batches(batch_id)` |
| `tracker_uuid` | TEXT | Bugtracker-internal UUID |
| `identifier` | TEXT | Ticket identifier |
| `action` | TEXT | `archive`, `delete`, or `unarchive` |
| `success` | INTEGER | 1 if successful (default 0) |
| `error` | TEXT | Error message if failed |
| `created_at` | TEXT | ISO-8601 timestamp |

### `codex_usage_snapshots`

Periodic snapshots of Codex/OpenAI rate-limit state.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `plan_type` | TEXT | OpenAI plan type |
| `primary_used_pct` | REAL | Primary rate limit utilization (0.0–1.0) |
| `primary_reset_at` | TEXT | When primary window resets (ISO-8601) |
| `primary_window_seconds` | INTEGER | Primary window duration |
| `secondary_used_pct` | REAL | Secondary rate limit utilization (0.0–1.0) |
| `secondary_reset_at` | TEXT | When secondary window resets (ISO-8601) |
| `secondary_window_seconds` | INTEGER | Secondary window duration |
| `rate_limit_allowed` | INTEGER | Whether rate limit allows requests |
| `raw_json` | TEXT | Raw API response |
| `created_at` | TEXT | ISO-8601 timestamp |

### `runtime_config`

Key-value store for dynamic configuration that persists across restarts.

| Column | Type | Description |
|--------|------|-------------|
| `key` | TEXT PK | Configuration key |
| `value` | TEXT | Configuration value |
| `updated_at` | TEXT | Last modification timestamp |

### `available_models`

Cached model metadata from the Anthropic Models API.

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT PK | Model ID (e.g. `claude-opus-4-6`) |
| `display_name` | TEXT | Human-readable model name |
| `max_input_tokens` | INTEGER | Maximum input token count |
| `max_output_tokens` | INTEGER | Maximum output token count |
| `supported_efforts` | TEXT | JSON array of supported effort levels |
| `executor_type` | TEXT | Agent executor type (default `claude`) |
| `is_alias` | INTEGER | 1 if this is an alias (e.g. `opus`) vs a dated ID |
| `fetched_at` | TEXT | When this model info was fetched |

### `usage_api_calls`

Append-only audit log — one row per usage API call attempt.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `created_at` | TEXT | ISO-8601 timestamp |
| `token_fingerprint` | TEXT | SHA-256 of last 8 chars of OAuth token (16 hex chars) |
| `status_code` | INTEGER | HTTP status (200, 429, 401, etc.). NULL for connection errors |
| `success` | INTEGER | 1 for 2xx responses, 0 otherwise |
| `error_type` | TEXT | `rate_limit`, `auth_error`, `server_error`, `connection_error`, `timeout`, `other`. NULL on success |
| `error_detail` | TEXT | Error message or response body excerpt |
| `response_time_ms` | REAL | Wall-clock time for the HTTP request |
| `retry_after` | TEXT | Value of `Retry-After` response header |
| `caller` | TEXT | Context: `poll`, `force_poll`, `force_poll_bypass`, `cli_refresh`, `dashboard_refresh`, `manual_refresh` |

### `usage_api_key_sessions`

One row per distinct token fingerprint — tracks the key's lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `token_fingerprint` | TEXT UNIQUE | SHA-256 fingerprint of the token |
| `first_seen_at` | TEXT | When this token was first used |
| `last_success_at` | TEXT | Last successful API call |
| `first_error_at` | TEXT | First error with this token |
| `last_error_at` | TEXT | Most recent error |
| `consecutive_errors` | INTEGER | Current streak of consecutive errors |
| `total_errors` | INTEGER | Lifetime error count |
| `total_successes` | INTEGER | Lifetime success count |
| `status` | TEXT | `active`, `erroring`, `blocked`, `recovered`, `replaced` |
| `blocked_at` | TEXT | When status became `blocked` |
| `unblocked_at` | TEXT | When status became `recovered` |
| `replaced_at` | TEXT | When this token was replaced by a new one |
| `block_duration_seconds` | REAL | Duration of the blocked period |
| `created_at` | TEXT | ISO-8601 timestamp |

### `schema_version`

Single row tracking the current schema version for migrations.

| Column | Type | Description |
|--------|------|-------------|
| `version` | INTEGER | Current schema version |

## Indexes

| Index | Table | Column(s) |
|-------|-------|-----------|
| `idx_tasks_ticket_id` | tasks | ticket_id |
| `idx_tasks_status` | tasks | status |
| `idx_stage_runs_task_id` | stage_runs | task_id |
| `idx_task_events_task_id` | task_events | task_id |
| `idx_task_events_type` | task_events | event_type |
| `idx_queue_entries_project` | queue_entries | project |
| `idx_ticket_history_project` | ticket_history | project_name |
| `idx_ticket_history_status` | ticket_history | status |
| `idx_ticket_history_captured_at` | ticket_history | captured_at |
| `idx_cleanup_batches_created_at` | cleanup_batches | created_at |
| `idx_cleanup_batch_items_batch_id` | cleanup_batch_items | batch_id |
| `idx_usage_api_calls_created_at` | usage_api_calls | created_at |
| `idx_usage_api_calls_token` | usage_api_calls | token_fingerprint |
| `idx_usage_api_calls_success` | usage_api_calls | success |
| `idx_usage_api_key_sessions_status` | usage_api_key_sessions | status |

## Pipeline Stages

Pipelines and their stages are defined in the `pipeline_templates` and `stage_templates` tables. Seeded pipelines:

### Implementation pipeline (default)

1. **implement** — Agent implements the ticket requirements
2. **review** — A separate agent instance reviews the implementation
3. **fix** — Agent addresses review feedback (loops with review, max 3 iterations)
4. **pr_checks** — Wait for CI checks, fix failures if needed
5. **ci_fix** — Fix CI failures (loops with pr_checks, max 2 iterations)
6. **resolve_conflict** — Resolve merge conflicts with main (loops with merge, max 2 iterations)
7. **merge** — Merge the PR

### Investigation pipeline (label: `Investigation`)

1. **implement** — Agent investigates and posts findings as a comment

### QA pipeline (label: `manual-qa`)

1. **qa** — Agent tests the feature via Playwright/shell and produces a structured report

## Migrations

Migrations live as numbered `.sql` files in `botfarm/migrations/`:

```
botfarm/migrations/
  001_initial_schema.sql
  002_add_pr_columns.sql
  ...
  039_add_thinking_mode_to_stages.sql
```

Each file is executed exactly once, in order, with a commit after each step. The `schema_version` table tracks the last successfully applied migration. Fresh databases run all migrations sequentially; existing databases skip already-applied migrations.

`SCHEMA_VERSION` is computed dynamically from the highest migration file number via `get_schema_version()`.

### Adding a new migration

1. Create `botfarm/migrations/NNN_description.sql` where `NNN` is the next number (zero-padded to 3 digits).
2. Write the SQL statements (no `IF NOT EXISTS` guards needed — each migration runs exactly once).
3. Run `python -m pytest tests/test_db.py -v` to verify the migration chain.
4. `SCHEMA_VERSION` updates automatically — no code changes needed in `db.py`.

### Migration history

| Version | File | Change |
|---------|------|--------|
| 1 | `001_initial_schema.sql` | Initial schema (tasks, stage_runs, usage_snapshots, task_events) |
| 2 | `002_add_pr_columns.sql` | Add `pr_url`, `pipeline_stage`, `review_state` to tasks |
| 3 | `003_add_slots_and_dispatch.sql` | Add `slots` and `dispatch_state` tables |
| 4 | `004_add_supervisor_heartbeat.sql` | Add `supervisor_heartbeat` to dispatch_state |
| 5 | `005_add_resets_at_7d.sql` | Add `resets_at_7d` to usage_snapshots |
| 6 | `006_drop_cost_usd.sql` | Drop `cost_usd` from tasks and stage_runs |
| 7 | `007_add_queue_entries.sql` | Add `queue_entries` table for ticket queue snapshots |
| 8 | `008_add_token_usage_columns.sql` | Add token usage columns to stage_runs (input/output/cache tokens, cost, context fill, model usage) |
| 9 | `009_add_log_file_path.sql` | Add `log_file_path` to stage_runs |
| 10 | `010_add_blocked_by_to_queue_entries.sql` | Add `blocked_by` to queue_entries |
| 11 | `011_add_extra_usage_columns.sql` | Add extra usage columns to usage_snapshots, `started_on_extra_usage` to tasks, `on_extra_usage` to stage_runs |
| 12 | `012_fix_extra_usage_cents_to_dollars.sql` | Convert extra usage values from cents to dollars |
| 13 | `013_add_ticket_labels_to_slots.sql` | Add `ticket_labels` to slots |
| 14 | `014_add_project_pause_state.sql` | Add `project_pause_state` table |
| 15 | `015_add_workflow_definitions.sql` | Add `pipeline_templates`, `stage_templates`, `stage_loops` tables with seeded implementation and investigation pipelines |
| 16 | `016_add_capacity_tracking.sql` | Add capacity tracking columns to dispatch_state (`linear_issue_count`, `linear_issue_limit`, etc.) |
| 17 | `017_ticket_history.sql` | Add `ticket_history` table for local ticket backup |
| 18 | `018_add_no_pr_needed_instruction.sql` | Update implement stage prompt to include NO_PR_NEEDED signal |
| 19 | `019_investigation_dependency_instruction.sql` | Update investigation prompt to set blocking relationships on follow-up tickets |
| 20 | `020_cleanup_batches.sql` | Add `cleanup_batches` and `cleanup_batch_items` tables |
| 21 | `021_add_merge_conflict_stage.sql` | Add `resolve_conflict` stage and `merge_conflict_loop`, add `merge_conflict_retries` to tasks |
| 22 | `022_add_result_text.sql` | Add `result_text` to tasks |
| 23 | `023_add_runtime_config.sql` | Add `runtime_config` key-value table |
| 24 | `024_add_shared_mem_to_templates.sql` | Add shared memory instructions to implement/fix/ci_fix stage prompts |
| 25 | `025_worktree_path_prompts.sql` | Add worktree path anchoring instructions to stage prompts |
| 26 | `026_add_failure_category.sql` | Add `failure_category` to tasks |
| 27 | `027_add_codex_usage_snapshots.sql` | Add `codex_usage_snapshots` table (initial version) |
| 28 | `028_retry_context_in_implement_prompt.sql` | Add `{prior_context}` variable to implement stage prompts |
| 29 | `029_codex_usage_rate_limits.sql` | Replace `codex_usage_snapshots` with rate-limit schema |
| 30 | `030_usage_api_audit.sql` | Add `usage_api_calls` and `usage_api_key_sessions` tables |
| 31 | `031_tracker_agnostic_prompts.sql` | Replace hardcoded "Linear" references with tracker-agnostic template variables in stage prompts |
| 32 | `032_rename_linear_to_tracker.sql` | Rename `linear_*` columns to `tracker_*` in ticket_history, dispatch_state, cleanup_batch_items |
| 33 | `033_pipeline_mcp_servers.sql` | Add `mcp_servers` to pipeline_templates |
| 34 | `034_add_manual_qa_pipeline.sql` | Add `manual-qa` pipeline with `qa` stage |
| 35 | `035_add_model_effort_columns.sql` | Add `model`, `effort` to stage_templates; add `available_models` table |
| 36 | `036_add_pipeline_id_columns.sql` | Add `pipeline_id` to tasks and stage_runs |
| 37 | `037_allow_redispatch.sql` | Remove UNIQUE constraint on `tasks.ticket_id` to support A/B comparison |
| 38 | `038_add_context_window_to_stages.sql` | Add `context_window` to stage_templates |
| 39 | `039_add_thinking_mode_to_stages.sql` | Add `thinking_mode` to stage_templates |
