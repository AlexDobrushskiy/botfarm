# Botfarm Configuration Guide

## Config File Location

Botfarm looks for its configuration at `~/.botfarm/config.yaml` by default. You can generate a starter config with:

```bash
botfarm init
```

To use a custom path:

```bash
botfarm init --path /path/to/config.yaml
botfarm run --config /path/to/config.yaml
botfarm status --config /path/to/config.yaml
```

All runtime state lives under `~/.botfarm/` by default:

| File | Purpose |
|---|---|
| `config.yaml` | Main configuration |
| `.env` | Environment variables (loaded automatically) |
| `botfarm.db` | SQLite database (slots, tasks, stage runs, usage snapshots) |
| `logs/` | Supervisor and worker log files |

## Environment Variables

Config values support `${VAR}` syntax for environment variable expansion. If a referenced variable is not set, botfarm raises an error at startup.

You can place variables in `~/.botfarm/.env` — botfarm loads this file automatically via `python-dotenv`.

Required:

| Variable | Purpose |
|---|---|
| `LINEAR_API_KEY` | Linear API key for polling tickets and updating status |

Usage limit monitoring reads your Claude Code OAuth token automatically from `~/.claude/.credentials.json` (Linux) or the system keychain (macOS). No extra env var needed.

## Config File Format

The file is YAML. Below is the full reference with all sections, defaults, and explanations.

---

### `projects` (required)

A list of projects to manage. Each project maps to a Linear team and a local git checkout.

```yaml
projects:
  - name: my-project            # Unique project name
    linear_team: TEAM            # Linear team key (e.g. "SMA")
    base_dir: ~/my-project       # Path to the git repository
    worktree_prefix: my-project-slot-  # Prefix for git worktree directories
    slots: [1, 2]                # Slot IDs assigned to this project
    linear_project: ""           # Optional: filter to a specific Linear project within the team
```

**Rules:**
- `name` must be unique across all projects
- `slots` must be a list of integers with no duplicates within each project
- Slot IDs are per-project — different projects can reuse the same IDs

---

### `linear`

Linear API connection and workflow configuration.

```yaml
linear:
  api_key: ${LINEAR_API_KEY}        # Required. Supports env var expansion
  workspace: my-workspace           # Linear workspace slug
  poll_interval_seconds: 120        # How often to poll for new tickets (default: 120)
  exclude_tags:                     # Tickets with these labels are skipped
    - Human

  # Workflow status names — must match your Linear team's workflow
  todo_status: Todo                 # Status to poll for new work (default: "Todo")
  in_progress_status: In Progress   # Set when a worker picks up a ticket (default: "In Progress")
  done_status: Done                 # Set on successful completion (default: "Done")
  in_review_status: In Review       # Set when a PR is created (default: "In Review")
  failed_status: Todo               # Set when a worker fails (default: "Todo")

  # Comment posting on Linear tickets
  comment_on_failure: true          # Post a comment when a task fails (default: true)
  comment_on_completion: false      # Post a comment on success (default: false)
  comment_on_limit_pause: false     # Post when paused by usage limits (default: false)
```

---

### `database`

SQLite database location for task history, stage runs, and usage snapshots.

```yaml
database:
  path: ~/.botfarm/botfarm.db  # default
```

---

### `usage_limits`

Thresholds for pausing dispatch when Anthropic API usage is high. Values are fractions (0.0–1.0).

```yaml
usage_limits:
  enabled: true                       # Set to false to disable usage-based pausing (default: true)
  pause_five_hour_threshold: 0.85     # Pause if 5-hour utilization >= 85% (default: 0.85)
  pause_seven_day_threshold: 0.90     # Pause if 7-day utilization >= 90% (default: 0.90)
```

Set `enabled: false` to fully disable usage-based dispatch pausing (e.g. if you have extended usage on your Claude account).

When a threshold is hit, dispatch pauses (no new tickets are picked up). Workers already running continue to completion. Dispatch resumes automatically once utilization drops below the threshold.

---

### `agents`

Controls for the Claude Code agent pipeline (implement → review → fix → pr_checks → merge).

```yaml
agents:
  max_review_iterations: 3    # Max review→fix cycles before giving up (default: 3)
  max_ci_retries: 2           # Max CI fix attempts (default: 2)
  timeout_minutes:            # Per-stage timeout
    implement: 120            # default: 120
    review: 30                # default: 30
    fix: 60                   # default: 60
  timeout_grace_seconds: 10   # Grace period after timeout before killing (default: 10)
  codex_reviewer_enabled: false          # Enable Codex as a secondary reviewer (default: false)
  codex_reviewer_model: ""               # Codex model, e.g. "o3", "o4-mini", or empty for default
  codex_reviewer_timeout_minutes: 15     # Timeout for Codex review stage (default: 15)
```

Valid stage names for `timeout_minutes`: `implement`, `review`, `fix`, `pr_checks`, `merge`.

When `codex_reviewer_enabled` is `true`, the supervisor validates at startup that:
- The `codex` binary is on PATH
- `OPENAI_API_KEY` is set or `~/.codex/auth.json` exists

---

### `logging`

Log rotation and cleanup settings.

```yaml
logging:
  max_bytes: 10485760           # Max size per log file before rotation (default: 10 MB)
  backup_count: 5               # Number of rotated files to keep (default: 5)
  ticket_log_retention_days: 30 # Delete ticket log dirs older than N days (default: 30)
```

`max_bytes` and `backup_count` apply to both the supervisor log and per-worker log files via `RotatingFileHandler`. Old per-ticket log directories are cleaned up periodically (hourly) — directories for active tickets are never removed.

---

### `dashboard`

Optional web dashboard (FastAPI + htmx). Runs as a background thread inside the supervisor.

```yaml
dashboard:
  enabled: false       # default: false
  host: 0.0.0.0       # default: 0.0.0.0
  port: 8420           # default: 8420
```

---

### `notifications`

Optional Slack or Discord webhook notifications for key events (task completed, task failed, usage limit hit/cleared, all slots idle).

```yaml
notifications:
  webhook_url: https://hooks.slack.com/services/...  # Leave empty to disable
  webhook_format: slack    # "slack" or "discord" (auto-detected from URL if omitted)
  rate_limit_seconds: 300  # Min interval between repeated limit_hit notifications (default: 300)
```

---

### `identities`

Optional credentials for separate coder and reviewer identities. When empty (the default), the system falls back to system-level git/gh auth. This allows gradual adoption — configure one identity at a time.

```yaml
identities:
  coder:
    github_token: ${CODER_GITHUB_TOKEN}          # PAT for gh CLI
    ssh_key_path: ~/.botfarm/coder_id_ed25519     # Private key for git SSH
    git_author_name: "Coder Bot"                  # Git commit author name
    git_author_email: "coder-bot@example.com"     # Git commit author email
    linear_api_key: ${CODER_LINEAR_API_KEY}       # Coder's own Linear API key
  reviewer:
    github_token: ${REVIEWER_GITHUB_TOKEN}        # PAT for gh CLI (review commands)
    linear_api_key: ${REVIEWER_LINEAR_API_KEY}    # Reviewer's own Linear API key
```

All fields default to empty string. Environment variable expansion (`${VAR}`) works in all string fields.

---

## Minimal Working Example

```yaml
projects:
  - name: my-app
    linear_team: APP
    base_dir: ~/code/my-app
    worktree_prefix: my-app-slot-
    slots: [1]

linear:
  api_key: ${LINEAR_API_KEY}
  workspace: my-workspace
```

Everything else uses defaults. Set `LINEAR_API_KEY` in your environment or in `~/.botfarm/.env`:

```
LINEAR_API_KEY=lin_api_xxxxxxxxxxxxxxxxxxxx
```

Then run:

```bash
botfarm run
```

## Multi-Project Example

```yaml
projects:
  - name: backend
    linear_team: BE
    base_dir: ~/code/backend
    worktree_prefix: backend-slot-
    slots: [1, 2, 3]

  - name: frontend
    linear_team: FE
    base_dir: ~/code/frontend
    worktree_prefix: frontend-slot-
    slots: [1, 2]
    linear_project: "Web App"

linear:
  api_key: ${LINEAR_API_KEY}
  workspace: acme
  poll_interval_seconds: 60
  exclude_tags:
    - Human
    - Manual

agents:
  max_review_iterations: 2
  timeout_minutes:
    implement: 90

notifications:
  webhook_url: https://hooks.slack.com/services/T00/B00/xxx
  webhook_format: slack

dashboard:
  enabled: true
  port: 8420
```

## CLI Commands

| Command | Description |
|---|---|
| `botfarm init` | Generate a starter config file |
| `botfarm run` | Start the supervisor (foreground) |
| `botfarm status` | Show current slot states |
| `botfarm history` | Show recent task history |
| `botfarm limits` | Show current usage limit utilization |

All commands accept `--config <path>` to override the default config location.
