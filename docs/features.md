# Feature Catalog

Comprehensive reference of all botfarm capabilities, organized by user task.
For detailed configuration, see [configuration.md](configuration.md).

## Pipeline Types

- **Implementation** (default) — full PR pipeline: implement → review → fix → PR checks → merge
- **Investigation** — research-only pipeline, posts findings as ticket comment, no PR created (label: `Investigation`)
- **QA** — tests features via Playwright/shell, produces structured report with bugs (label: `manual-qa`)
- **Custom pipelines** — create arbitrary stage sequences via the dashboard pipeline editor or direct DB inserts

## Ticket Dispatch

- **Auto mode** — supervisor polls bugtracker, dispatches Todo tickets to free slots automatically
- **Semi-auto mode** — tickets queued but require manual dispatch via dashboard (`dispatch_mode: "semi-auto"`)
- **Label-based routing** — ticket labels map to pipeline templates (e.g., `Investigation` → investigation pipeline)
- **Board/rank ordering** — tickets dispatched by tracker sort order (Linear board position, Jira rank field)
- **Dependency blocking** — tickets with unresolved blockers are skipped
- **Tag filtering** — `include_tags` / `exclude_tags` control which tickets are eligible
- **Project filtering** — `tracker_project` limits polling to a specific project within a team (Linear only; Jira ignores this setting)
- **Start-paused mode** — supervisor starts with dispatch paused; resume via CLI or dashboard (`start_paused: true`)

## Pipeline Stages

- **implement** — agent implements ticket, creates PR (or posts findings for investigation tickets)
- **review** — separate reviewer identity reviews PR, posts inline comments, outputs APPROVED/CHANGES_REQUESTED verdict
- **fix** — agent addresses review comments
- **pr_checks** — waits for CI checks to pass (`gh pr checks --watch`)
- **ci_fix** — agent fixes CI failures using CI output as context
- **merge** — squash-merges PR, cleans up feature branch
- **resolve_conflict** — agent resolves merge conflicts by merging main into feature branch
- **codex_review** — optional secondary review via Codex adapter
- **qa** — agent tests features using Playwright MCP, shell commands, and DB queries

### Stage Loops

- **Review loop** — iterates review → fix until approved or max iterations reached (`max_review_iterations`, default: 3)
- **CI retry loop** — iterates ci_fix → pr_checks on CI failure (`max_ci_retries`, default: 2)
- **Merge conflict loop** — resolve_conflict → re-review → pr_checks → merge (`max_merge_conflict_retries`, default: 2)

## Pipeline Configuration

- **DB-driven templates** — pipelines, stages, and loops stored in `pipeline_templates`, `stage_templates`, `stage_loops` tables
- **Dashboard-editable** — full CRUD for pipelines, stages, loops, and ordering via the workflow editor
- **Per-stage settings** — `model`, `effort`, `max_turns`, `timeout_minutes`, `context_window`, `thinking_mode`, `prompt_template`
- **Executor types** — `claude` (agent), `shell` (subprocess), `internal` (built-in logic), or custom adapter name
- **Identity assignment** — stages tagged as `coder` or `reviewer` for credential isolation
- **MCP server injection** — per-pipeline `mcp_servers` JSON merged with bugtracker MCP config
- **Result parsers** — `pr_url`, `review_verdict`, `qa_report` extract structured data from agent output
- **Prompt templates** — `{ticket_id}`, `{pr_url}`, `{worktree_path}`, `{shared_mem_path}`, etc.; unknown vars left as literals
- **Pipeline duplication** — clone existing pipelines via dashboard for A/B testing

## Agent Support

- **Claude Code** (primary) — full support: context fill tracking, max turns, model/effort override, streaming output, OAuth and API key auth
- **OpenAI Codex** (secondary) — `codex exec` with reasoning effort, model selection (o3, o4-mini, gpt-4o), `--dangerously-bypass-approvals-and-sandbox`
- **Aider** (reference) — reference adapter implementation for adding new backends
- **Adapter registry** — adapters registered via entry points, configurable per-stage (`agents.adapters.*`)
- **Skip on reiteration** — adapters can skip redundant runs on review loop re-entry (`skip_on_reiteration`)

## Identity Management

- **Separate coder/reviewer GitHub accounts** — PRs and reviews appear from different users
- **Per-identity SSH keys** — `ssh_key_path` for authenticated git operations
- **Per-identity git author** — `git_author_name` / `git_author_email` on coder identity
- **Per-identity bugtracker tokens** — separate Linear/Jira API keys for coder and reviewer
- **Jira identity fields** — `jira_api_token` / `jira_email` for Jira Cloud authentication

## Usage & Rate Limiting

- **Claude monitoring** — polls Anthropic usage API for 5-hour and 7-day utilization
- **Configurable thresholds** — `pause_five_hour_threshold` (default: 0.85), `pause_seven_day_threshold` (default: 0.90)
- **Auto pause/resume** — dispatch pauses when thresholds exceeded, resumes when utilization drops
- **Mid-pipeline pause** — slots enter `paused_limit` state, resume from interrupted stage when limits clear
- **Extra usage tracking** — detects extra usage enabled status, monthly limit, and utilization
- **Codex usage monitoring** — separate polling for OpenAI usage (`codex_usage` config section)
- **401 backoff system** — linear backoff on auth failures, proactive token refresh, recovery notifications
- **429 rate limit handling** — exponential backoff with jitter, respects `Retry-After` header
- **Audit logging** — all usage API calls stored in `usage_api_calls` table with status and timing
- **Manual refresh** — force usage refresh via dashboard button or API endpoint
- **Poll interval** — configurable, default 600s (10 minutes)

## Bugtracker Support

- **Linear** (full) — polling, status transitions, comments, labels, capacity monitoring, issue creation
- **Jira Cloud** (full) — polling, status transitions, comments, labels via `mcp-atlassian`/`uvx`
- **Jira Server/Data Center** — Bearer token auth support
- **Capacity monitoring** — warning (70%), critical (85%), pause (95%), resume (90%) thresholds (Linear)
- **Comment on events** — configurable: `comment_on_failure`, `comment_on_completion`, `comment_on_limit_pause`
- **Rank-based ordering** — Jira adapter discovers `Rank` custom field for dispatch priority
- **Per-project bugtracker overrides** — project-level bugtracker config overrides global settings
- **Issue limit** — configurable max issues to fetch per poll (default: 250)

## Notifications

- **Slack/Discord webhooks** — auto-detected from URL, or explicit `webhook_format` config
- **Events**: task completion, task failure, usage limit hit/cleared, capacity warning/critical/blocked, auth failure/recovery, all slots idle, human blocker, refactoring analysis trigger, supervisor shutdown, daily summary
- **Rate limiting** — global cooldown (`rate_limit_seconds`, default: 300s), per-event-type cooldown
- **Human blocker cooldown** — separate cooldown for blocked-by-human notifications (default: 1 hour)
- **Daily summary** — configurable send hour, minimum task threshold, separate webhook URL option

## Dashboard

- **Live status** — slot states, queue visualization, usage stats, tracker capacity, dev server status
- **Task history** — filterable task list with duration, cost, stage, and status
- **Ticket queue** — pending tickets with priority and labels
- **Ticket detail** — full ticket info with linked task history
- **Task detail** — stage runs, cost breakdown, log file links
- **Manual dispatch** — dispatch tickets to free slots from dashboard (semi-auto or override)
- **Re-dispatch / A/B comparison** — re-dispatch completed tickets with different pipelines, compare results with export
- **Pipeline editor** — full CRUD for pipelines, stages, loops; stage reordering; pipeline duplication
- **Config editor** — edit runtime config with hot-apply for safe fields (no restart needed)
- **Identity editor** — manage coder/reviewer credentials
- **Adapter management** — enable/disable adapters, configure model/timeout/reasoning per adapter
- **Usage stats** — utilization charts, cost tracking, model usage breakdown
- **Log viewer** — per-stage log files with syntax highlighting
- **Live log streaming** — SSE-based tailing of in-progress stage logs
- **Web terminal** — browser-based terminal via WebSocket (configurable, `terminal_enabled: false` by default)
- **Setup wizard** — guided setup: bugtracker config, GitHub device flow, Claude auth, project creation
- **Project management** — add/remove projects with SSE progress streaming
- **Preflight/health page** — environment validation results with fix guidance
- **Dev server management** — start/stop/restart project dev servers with SSE log streaming
- **Metrics page** — analytics and throughput data
- **Supervisor controls** — pause/resume, slot stop, trigger update from dashboard
- **HTMX partials** — auto-refreshing UI components without full page reload

## CLI Commands

| Command | Description |
|---------|-------------|
| `init` | Interactive setup: discovers bugtracker teams, generates config and .env |
| `add-project` | Add project: clone repo, create worktrees, update config |
| `remove-project` | Remove project: clean up DB state, optionally delete repo |
| `run` | Start supervisor in foreground (with optional `--auto-restart`) |
| `status` | Show current slot states across all projects |
| `history` | Show recent completed/failed tasks with metrics |
| `limits` | Show current usage limit utilization |
| `pause` | Pause dispatching for a specific project |
| `resume` | Resume dispatching (project-specific or global start-paused) |
| `stop` | Stop a running slot, kill worker, clean up |
| `reset` | Reset stuck slots to free (with optional `--all` for all projects) |
| `preflight` | Run environment validation checks without starting supervisor |
| `auth` | Check and fix authentication for all services (with `--fix` for auto-repair) |
| `backfill-history` | Backfill ticket_history from bugtracker for existing tasks |
| `install-service` | Install systemd user service for auto-start and crash recovery |
| `uninstall-service` | Remove systemd user service |
| `init-claude-md` | Generate a CLAUDE.md template for a project directory |

## Recovery & Resilience

- **Crash recovery on restart** — detects dead worker PIDs, reconciles DB state, resumes or fails gracefully
- **Slot state persistence** — all slot state in SQLite; survives supervisor crashes
- **Stage-aware resume** — resumes from the next incomplete stage, not from the beginning
- **PR status checking** — on recovery, checks if PR was merged/open/closed to determine correct action
- **Usage limit pause/resume** — mid-pipeline pause with `paused_limit` state, resumes from interrupted stage
- **Dead PID detection** — `os.kill(pid, 0)` signal probe on startup and periodic ticks
- **Timeout enforcement** — per-stage timeouts with SIGTERM → SIGKILL escalation and grace period
- **Legacy state migration** — one-time auto-migration from `state.json` to SQLite

## Auth Modes

- **oauth** (default) — Claude Code OAuth via macOS keychain or `~/.claude/.credentials.json`; proactive token refresh with 5-minute buffer
- **api_key** (`--bare`) — direct Anthropic API key, set via `ANTHROPIC_API_KEY` env var
- **long_lived_token** — long-lived OAuth token without refresh rotation

## Operational

- **systemd service** — `install-service` / `uninstall-service` with auto-restart on crashes, `KillMode=mixed`, 300s stop timeout
- **Log rotation** — configurable `max_bytes` (default: 10 MB) and `backup_count` (default: 5)
- **Per-ticket log directories** — `logs/<TICKET-ID>/worker.log` + `<stage>[-iter<N>]-<timestamp>.log`
- **Refactoring analysis scheduler** — periodic codebase analysis via Investigation tickets (`refactoring_analysis` config section)
- **Git worktree isolation** — each slot runs in its own worktree; no cross-slot interference
- **Start-paused mode** — supervisor starts with dispatch disabled (`start_paused: true`, default)
- **Hot-reload** — config changes, dispatch mode, and pause state apply without supervisor restart
- **Auto-restart on update** — `--auto-restart` flag (default) restarts supervisor on exit code 42
- **Project types** — `project_type` field derives dev server defaults (nextjs, react, django, flask, fastapi, etc.)
- **Setup commands** — per-project `setup_commands` run after clone
- **Dev server** — per-project `run_command` / `run_env` / `run_port` for QA testing
- **Per-project pause** — pause/resume dispatch per project independently
- **Ticket log retention** — configurable cleanup of old per-ticket logs (`ticket_log_retention_days`, default: 30)
