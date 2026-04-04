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

Botfarm loads `~/.botfarm/.env` automatically via `python-dotenv`. This is the **only** `.env` file that botfarm reads — `.env` files in the current working directory are ignored. If a CWD `.env` is detected, botfarm prints a warning to stderr.

Required (depends on bugtracker type):

| Variable | Purpose |
|---|---|
| `LINEAR_API_KEY` | Linear API key — used for both supervisor operations (polling, status updates) and agent MCP tools (auto-configured via `--mcp-config`) |
| `JIRA_API_TOKEN` | Jira Cloud API token (when `type: jira`) |

Usage limit monitoring reads your Claude Code OAuth token automatically from `~/.claude/.credentials.json` (Linux) or the system keychain (macOS). No extra env var needed.

## Config File Format

The file is YAML. Below is the full reference with all sections, defaults, and explanations.

---

### `projects` (required)

A list of projects to manage. Each project maps to a bugtracker team and a local git checkout.

```yaml
projects:
  - name: my-project            # Unique project name
    team: TEAM                   # Bugtracker team key (e.g. "SMA")
    base_dir: ~/my-project       # Path to the git repository
    worktree_prefix: my-project-slot-  # Prefix for git worktree directories
    slots: [1, 2]                # Slot IDs assigned to this project
    tracker_project: ""          # Optional: filter to a specific project within the team
    include_tags:                # Optional: project-level label filter (overrides global bugtracker.include_tags)
      - my-label
    dispatch_mode: auto          # Optional: "auto" (default) or "semi-auto"
```

**Rules:**
- `name` must be unique across all projects
- `tracker_project` must be unique across all projects (when set). If two projects share the same `tracker_project` filter, tickets may be dispatched to the wrong repo.
- `slots` must be a list of integers with no duplicates within each project
- Slot IDs are per-project — different projects can reuse the same IDs
- `include_tags` on a project overrides the global `bugtracker.include_tags` for that project. If a project does not set `include_tags`, the global value is used as a fallback

#### Multi-project routing with `include_tags`

When multiple projects share the same bugtracker team, use per-project `include_tags` to route tickets to the correct project:

```yaml
bugtracker:
  type: jira
  include_tags:
    - botfarm          # global fallback (used if a project doesn't set its own)
projects:
  - name: air-back
    team: AIR
    base_dir: ~/repos/air-back
    worktree_prefix: air-back-slot-
    slots: [1, 2]
    include_tags:       # project-level override
      - botfarm-backend
  - name: air-front
    team: AIR
    base_dir: ~/repos/air-front
    worktree_prefix: air-front-slot-
    slots: [3]
    include_tags:
      - botfarm-frontend
```

With this config:
- A ticket labeled `botfarm-backend` is picked up by `air-back` only
- A ticket labeled `botfarm-frontend` is picked up by `air-front` only
- A ticket labeled `botfarm` only is not picked up by either (doesn't match project-level tags)
- If a project has no `include_tags`, it falls back to the global `bugtracker.include_tags`

#### `dispatch_mode`

Controls how tickets are dispatched for a project. Allowed values:

| Value | Behavior |
|---|---|
| `auto` (default) | Tickets are automatically picked up and dispatched to available slots |
| `semi-auto` | Tickets require manual approval before dispatch |

This is a hot-reloadable setting — changing it in `config.yaml` takes effect at the next supervisor tick without a restart.

---

### `bugtracker`

Bugtracker API connection and workflow configuration. Supports Linear (`type: linear`) and Jira (`type: jira`).

For Linear, the `api_key` serves double duty: the supervisor uses it for polling and status updates, and it is also passed to agent workers via `--mcp-config` so they have Linear MCP tools available. No separate plugin installation is needed. If `identities.coder.tracker_api_key` is set, it takes priority for agent MCP tools.

#### Linear configuration

```yaml
bugtracker:
  type: linear                      # "linear" or "jira"
  api_key: ${LINEAR_API_KEY}        # Required. Supports env var expansion
  workspace: my-workspace           # Workspace slug
  poll_interval_seconds: 120        # How often to poll for new tickets (default: 120)
  include_tags:                     # Only tickets with at least one of these labels are picked up
    - botfarm                       # (default: [] — all tickets eligible)
  exclude_tags:                     # Tickets with these labels are skipped
    - Human

  # Workflow status names — must match your bugtracker's workflow
  todo_status: Todo                 # Status to poll for new work (default: "Todo")
  in_progress_status: In Progress   # Set when a worker picks up a ticket (default: "In Progress")
  done_status: Done                 # Set on successful completion (default: "Done")
  in_review_status: In Review       # Set when a PR is created (default: "In Review")

  # Comment posting on tickets
  comment_on_failure: true          # Post a comment when a task fails (default: true)
  comment_on_completion: false      # Post a comment on success (default: false)
  comment_on_limit_pause: false     # Post when paused by usage limits (default: false)
```

#### Jira configuration

```yaml
bugtracker:
  type: jira
  api_key: ${JIRA_API_TOKEN}        # Jira Cloud API token
  workspace: my-org                  # Jira Cloud site name (e.g. "acme" for acme.atlassian.net)
  poll_interval_seconds: 120
  exclude_tags:
    - Human

  # Workflow status names — must match your Jira project's workflow
  todo_status: To Do
  in_progress_status: In Progress
  done_status: Done
  in_review_status: In Review

  comment_on_failure: true
  comment_on_completion: false
  comment_on_limit_pause: false
```

For Jira, set the API token in `~/.botfarm/.env`:
```
JIRA_API_TOKEN=your-jira-api-token
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
  pr_checks_timeout_seconds: 600  # Max seconds to wait for CI checks (default: 600)
  timeout_minutes:            # Per-stage timeout
    implement: 120            # default: 120
    review: 30                # default: 30
    fix: 60                   # default: 60
  timeout_grace_seconds: 10   # Grace period after timeout before killing (default: 10)
  adapters:                   # Per-agent-adapter configuration
    claude:
      enabled: true           # Claude is always the primary agent
    codex:
      enabled: false          # Enable Codex as a secondary reviewer (default: false)
      model: ""               # Codex model, e.g. "o3", "o4-mini", or empty for default
      timeout_minutes: 15     # Timeout for Codex review stage (default: 15)
      reasoning_effort: "medium"  # none, low, medium, high, xhigh (default: medium)
      skip_on_reiteration: true   # Skip codex on review iterations 2+ (default: true)
```

Valid stage names for `timeout_minutes`: `implement`, `review`, `fix`, `pr_checks`, `merge`.

**Backward compatibility:** Legacy `codex_reviewer_*` flat fields (e.g. `codex_reviewer_enabled: true`)
are still accepted and automatically migrated to `adapters.codex`. A deprecation warning is logged.

When `adapters.codex.enabled` is `true`, the supervisor validates at startup that:
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
    tracker_api_key: ${CODER_LINEAR_API_KEY}      # Coder's own bugtracker API key (overrides bugtracker.api_key for agent MCP tools)
  reviewer:
    github_token: ${REVIEWER_GITHUB_TOKEN}        # PAT for gh CLI (review commands)
    tracker_api_key: ${REVIEWER_LINEAR_API_KEY}   # Reviewer's own bugtracker API key
```

All fields default to empty string. Environment variable expansion (`${VAR}`) works in all string fields.

#### Why use separate identities?

Without identity configuration, all PRs, commits, and review comments are posted under your personal GitHub account. This works fine for solo use, but has drawbacks for teams:

- **PR review credibility** — when the same account creates a PR and approves it, GitHub shows the review as the author approving their own work. With separate coder/reviewer accounts, reviews appear as genuine third-party feedback.
- **Audit trail** — separate accounts make it clear which actions came from the coder agent vs the reviewer agent in GitHub history and notifications.
- **Access control** — you can scope each account's permissions independently (e.g. give the reviewer read-only repo access).

If you only run a single slot with no review stage, you can skip identity setup entirely.

#### How to create separate GitHub accounts

You need two GitHub accounts: one for the coder and one for the reviewer.

1. **Create the accounts.** Use separate email addresses (e.g. `coder-bot@yourcompany.com` and `reviewer-bot@yourcompany.com`). GitHub requires unique emails per account.

2. **Add both accounts as collaborators** on your repository (Settings → Collaborators). The coder needs write access; the reviewer needs at least read access (write if you want it to post review comments via the API).

3. **Generate a Personal Access Token (PAT)** for each account:
   - Go to **Settings → Developer settings → Personal access tokens → Fine-grained tokens**
   - Set the token to only access the specific repository/repositories botfarm manages
   - Required permissions for the **coder** token: `Contents` (read/write), `Pull requests` (read/write), `Metadata` (read)
   - Required permissions for the **reviewer** token: `Contents` (read), `Pull requests` (read/write), `Metadata` (read)

4. **Generate an SSH key** for the coder account (used for git push):
   ```bash
   ssh-keygen -t ed25519 -f ~/.botfarm/coder_id_ed25519 -N "" -C "coder-bot"
   chmod 600 ~/.botfarm/coder_id_ed25519
   ```
   Add the public key (`~/.botfarm/coder_id_ed25519.pub`) to the coder GitHub account under **Settings → SSH and GPG keys**.

5. **Add tokens to your `.env` file** (`~/.botfarm/.env`):
   ```
   CODER_GITHUB_TOKEN=github_pat_xxxx
   REVIEWER_GITHUB_TOKEN=github_pat_yyyy
   ```

#### Configuring identities in config.yaml

Uncomment the `identities` section and fill in the values:

```yaml
identities:
  coder:
    github_token: ${CODER_GITHUB_TOKEN}
    ssh_key_path: ~/.botfarm/coder_id_ed25519
    git_author_name: "Coder Bot"
    git_author_email: "coder-bot@yourcompany.com"
  reviewer:
    github_token: ${REVIEWER_GITHUB_TOKEN}
```

How each field is used:

| Field | Effect |
|---|---|
| `coder.github_token` | Set as `GH_TOKEN` for `gh` CLI during implement/fix/pr_checks/merge stages |
| `coder.ssh_key_path` | Used in `GIT_SSH_COMMAND` for git push (must be an absolute or `~`-prefixed path) |
| `coder.git_author_name` | Set as `GIT_AUTHOR_NAME` and `GIT_COMMITTER_NAME` on commits during implement/fix/pr_checks/merge stages |
| `coder.git_author_email` | Set as `GIT_AUTHOR_EMAIL` and `GIT_COMMITTER_EMAIL` on commits during implement/fix/pr_checks/merge stages |
| `coder.tracker_api_key` | Optional separate bugtracker API key for the coder — when set, overrides `bugtracker.api_key` for agent MCP tools |
| `reviewer.github_token` | Set as `GH_TOKEN` for `gh` CLI during review stages |
| `reviewer.tracker_api_key` | Optional separate bugtracker API key for the reviewer |

You can configure identities incrementally — any field left empty falls back to the system-level default (your `gh auth` login, global git config, etc.).

#### Preflight checks

When identities are configured, `botfarm run` automatically validates them at startup:

- SSH key file exists (critical — blocks startup)
- SSH key has correct permissions, 0600 (warning — does not block startup)
- SSH key can connect to GitHub (warning — does not block startup)
- GitHub tokens are valid (critical — blocks startup)
- Coder and reviewer tokens are not identical (warning — does not block startup)

---

## Minimal Working Example

```yaml
projects:
  - name: my-app
    team: APP
    base_dir: ~/code/my-app
    worktree_prefix: my-app-slot-
    slots: [1]

bugtracker:
  type: linear
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
    team: BE
    base_dir: ~/code/backend
    worktree_prefix: backend-slot-
    slots: [1, 2, 3]

  - name: frontend
    team: FE
    base_dir: ~/code/frontend
    worktree_prefix: frontend-slot-
    slots: [1, 2]
    tracker_project: "Web App"

bugtracker:
  type: linear
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
| `botfarm add-project` | Interactive wizard to add a new project (clone, worktrees, config) |
| `botfarm run` | Start the supervisor (foreground) |
| `botfarm status` | Show current slot states |
| `botfarm history` | Show recent task history |
| `botfarm limits` | Show current usage limit utilization |
| `botfarm preflight` | Run preflight checks against config |

All commands accept `--config <path>` to override the default config location.

See `docs/cli-add-project.md` for detailed add-project implementation reference.
