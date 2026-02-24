# CLAUDE.md

## Project Context
- Botfarm: autonomous Linear ticket dispatcher for Claude Code agents
- Pure Python CLI project — optional web dashboard, no database migrations, no run.sh/stop.sh
- This CLAUDE.md primarily covers the implementer workflow. Reviewer and review-addresser agents receive instructions via prompt.

## Architecture
Modules under `botfarm/`:
- `cli.py` — Click/Rich CLI (status, history, limits, init, run)
- `config.py` — YAML config loading with `${ENV_VAR}` expansion and validation
- `supervisor.py` — Main loop: poll Linear, dispatch workers via multiprocessing, manage timeouts, crash recovery
- `worker.py` — Stage pipeline: implement → review → fix → pr_checks → merge (iterates review/CI fix loops)
- `slots.py` — Slot lifecycle & JSON state persistence (free/busy/paused_limit/failed/completed_pending_cleanup)
- `db.py` — SQLite (sync, WAL mode) for tasks, stage_runs, usage_snapshots, task_events
- `linear.py` — Linear GraphQL client with priority-sorted polling and state caching
- `usage.py` — Anthropic usage API polling, threshold-based dispatch pausing
- `credentials.py` — OAuth token retrieval (macOS keychain / Linux ~/.claude/.credentials.json)
- `notifications.py` — Slack/Discord webhook notifications with rate limiting
- `dashboard.py` — Optional FastAPI + htmx web dashboard (background thread in supervisor)

Key patterns:
- Workers run as subprocesses; communicate results via `multiprocessing.Queue`
- All state persists to `state.json` after every mutation — supervisor survives crashes
- Usage limits pause slots mid-pipeline and resume from interrupted stage
- Claude invoked via `claude -p --output-format json` subprocess

## Workflow: Linear Tickets
When working on a Linear ticket:
1. Fetch ticket details via Linear MCP
2. Move ticket to "In Progress"
3. `git fetch origin && git checkout -b <branch name> origin/main`; for branch name use linear git branch name
4. Delete previous working branch if it exists (NEVER delete: main, slot-1-placeholder)
5. Run baseline tests before starting work
6. Implement changes
7. Add/update tests
8. Run full test suite — fix until green
9. Commit, push
10. Create PR via `gh` — link ticket
11. Move ticket to "In Review"
12. If you identify out-of-scope issues during work — create Linear tickets for them

## Testing
- Run tests in a subagent that returns ONLY: pass/fail summary + failing test details
- Exception: 1-2 individual tests can run in main thread
- Command: `python -m pytest tests/ -v` (use .venv virtualenv)
- No Playwright / no frontend tests
- Run tests: before work, after implementation, after test changes, before push
- Pre-commit hook: `.githooks/pre-commit` — activate with `git config core.hooksPath .githooks`

## Development
- New packages → add to requirements.txt FIRST, then `pip install -r requirements.txt` (in .venv)
- Install editable: `pip install -e .`
- Validate with: `botfarm --help`
- Branch protection: NEVER delete `main` or `slot-1-placeholder`

## Linear
- When creating Linear tickets, always use team "Smart AI Coach" and project "Bot farm"

## PR Process
- Clear description linking Linear ticket
- Concise but meaningful
