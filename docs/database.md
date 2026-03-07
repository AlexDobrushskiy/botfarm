# Database Schema

Botfarm uses a single SQLite database (`~/.botfarm/botfarm.db`) in WAL mode with foreign keys enabled. Schema is versioned — migrations run automatically on startup (see `db.py`).

Current schema version: **21**

## Tables

### `tasks`

One row per Linear ticket that botfarm has picked up. Retries reuse the same row (upsert on `ticket_id`).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `ticket_id` | TEXT UNIQUE | Linear ticket identifier (e.g. `SMA-123`) |
| `title` | TEXT | Ticket title |
| `project` | TEXT | Project name from config |
| `slot` | INTEGER | Slot ID that processed this ticket |
| `status` | TEXT | `pending`, `running`, `completed`, or `failed` |
| `created_at` | TEXT | ISO-8601 timestamp |
| `started_at` | TEXT | When work began |
| `completed_at` | TEXT | When work finished (success or failure) |
| `turns` | INTEGER | Total agentic turns across all stages |
| `review_iterations` | INTEGER | Number of review→fix cycles |
| `comments` | TEXT | Accumulated review comments |
| `limit_interruptions` | INTEGER | Times paused by usage limits |
| `failure_reason` | TEXT | Error message if failed |
| `pr_url` | TEXT | GitHub PR URL (added in v2) |
| `pipeline_stage` | TEXT | Last pipeline stage reached (added in v2) |
| `review_state` | TEXT | Last review verdict (added in v2) |

### `stage_runs`

One row per Claude subprocess invocation. A single task has multiple stage runs (implement, review, fix, etc.).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `task_id` | INTEGER FK | References `tasks(id)` |
| `stage` | TEXT | `implement`, `review`, `fix`, `pr_checks`, or `merge` |
| `iteration` | INTEGER | Iteration number (1-based, relevant for review/fix cycles) |
| `session_id` | TEXT | Claude session ID |
| `turns` | INTEGER | Agentic turns in this stage |
| `duration_seconds` | REAL | Wall-clock duration |
| `exit_subtype` | TEXT | How Claude exited (e.g. `end_turn`, `max_turns`) |
| `was_limit_restart` | INTEGER | 1 if this run was a resume after a limit pause |
| `created_at` | TEXT | ISO-8601 timestamp |

### `usage_snapshots`

Periodic snapshots of Anthropic API utilization, recorded each poll cycle.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PK | Auto-increment |
| `utilization_5h` | REAL | 5-hour rolling utilization (0.0–1.0) |
| `utilization_7d` | REAL | 7-day rolling utilization (0.0–1.0) |
| `resets_at` | TEXT | When 5-hour window resets (ISO-8601) |
| `resets_at_7d` | TEXT | When 7-day window resets (ISO-8601, added in v5) |
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
| `preflight_failed` | system | Pre-run checks failed |
| `worker_dispatched` | task | Worker subprocess spawned for a ticket |
| `slot_completed` | task | Slot finished successfully |
| `slot_failed` | task | Slot finished with failure |
| `slot_reconciled` | task | Slot state corrected during reconciliation |
| `timeout` | task | Stage timed out and was killed |
| `limit_hit` | task | Usage limit threshold reached, slot paused |
| `limit_resumed` | task | Usage dropped below threshold, slot resumed |
| `dispatch_paused` | system | All dispatch paused (usage limits) |
| `dispatch_resumed` | system | Dispatch unpaused |
| `parent_auto_closed` | task | Parent Linear issue auto-closed |
| `review_iteration_started` | task | Review→fix cycle started |
| `review_approved` | task | Reviewer approved the implementation |
| `max_iterations_reached` | task | Hit max review iterations |
| `ci_retry_started` | task | CI fix retry started |
| `ci_passed_after_retry` | task | CI passed after a fix attempt |
| `max_ci_retries_reached` | task | Hit max CI retry attempts |
| `startup_recovery` | system | Recovering slots after supervisor restart |
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
| `status` | TEXT | `free`, `busy`, `paused_limit`, `failed`, or `completed_pending_cleanup` |
| `ticket_id` | TEXT | Currently assigned ticket |
| `ticket_title` | TEXT | Ticket title |
| `branch` | TEXT | Git branch name |
| `pr_url` | TEXT | PR URL if created |
| `stage` | TEXT | Current pipeline stage |
| `stage_iteration` | INTEGER | Current iteration within stage |
| `current_session_id` | TEXT | Claude session ID for resume |
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
| `supervisor_heartbeat` | TEXT | Last heartbeat timestamp (added in v4) |
| `updated_at` | TEXT | Last modification timestamp |

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
| `caller` | TEXT | Context: `poll`, `force_poll`, `force_poll_bypass`, `cli_refresh`, `dashboard_refresh` |

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
| `idx_stage_runs_task_id` | stage_runs | task_id |
| `idx_task_events_task_id` | task_events | task_id |
| `idx_task_events_type` | task_events | event_type |
| `idx_tasks_ticket_id` | tasks | ticket_id |
| `idx_tasks_status` | tasks | status |
| `idx_usage_api_calls_created_at` | usage_api_calls | created_at |
| `idx_usage_api_calls_token` | usage_api_calls | token_fingerprint |
| `idx_usage_api_calls_success` | usage_api_calls | success |
| `idx_usage_api_key_sessions_status` | usage_api_key_sessions | status |

## Pipeline Stages

The worker runs tickets through these stages in order:

1. **implement** — Claude implements the ticket requirements
2. **review** — A separate Claude instance reviews the implementation
3. **fix** — Claude addresses review feedback (may iterate with review)
4. **pr_checks** — Wait for CI checks, fix failures if needed
5. **merge** — Merge the PR

Stages are defined in `worker.py:STAGES`.

## Migrations

Migrations live as numbered `.sql` files in `botfarm/migrations/`:

```
botfarm/migrations/
  001_initial_schema.sql
  002_add_pr_columns.sql
  003_add_slots_and_dispatch.sql
  004_add_supervisor_heartbeat.sql
  005_add_resets_at_7d.sql
  006_drop_cost_usd.sql
  ...
  021_usage_api_audit.sql
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
| 6 | `006_drop_cost_usd.sql` | Drop `cost_usd` from tasks and stage_runs (always $0.00 on Max subscription) |
| 7–20 | `007_*` – `020_*` | Various additions (queue entries, token usage, workflow definitions, ticket history, cleanup batches, etc.) |
| 21 | `021_usage_api_audit.sql` | Add `usage_api_calls` and `usage_api_key_sessions` tables for usage API audit logging |
