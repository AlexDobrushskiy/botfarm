# Database Schema

Botfarm uses a single SQLite database (`~/.botfarm/botfarm.db`) in WAL mode with foreign keys enabled. Schema is versioned — migrations run automatically on startup (see `db.py`).

Current schema version: **5**

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
| `cost_usd` | REAL | Total API cost for all stages |
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
| `cost_usd` | REAL | API cost for this stage |
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

## Pipeline Stages

The worker runs tickets through these stages in order:

1. **implement** — Claude implements the ticket requirements
2. **review** — A separate Claude instance reviews the implementation
3. **fix** — Claude addresses review feedback (may iterate with review)
4. **pr_checks** — Wait for CI checks, fix failures if needed
5. **merge** — Merge the PR

Stages are defined in `worker.py:STAGES`.

## Migration History

| Version | Change |
|---------|--------|
| 1 | Initial schema (tasks, stage_runs, usage_snapshots, task_events) |
| 2 | Add `pr_url`, `pipeline_stage`, `review_state` to tasks |
| 3 | Add `slots` and `dispatch_state` tables |
| 4 | Add `supervisor_heartbeat` to dispatch_state |
| 5 | Add `resets_at_7d` to usage_snapshots |
