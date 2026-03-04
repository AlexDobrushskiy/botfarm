# CLAUDE.md

## Project Context
- Botfarm: autonomous Linear ticket dispatcher for Claude Code agents
- Pure Python CLI project — optional web dashboard, no run.sh/stop.sh
- Database schema has versioned migrations in `db.py` (current: v5)
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

Docs under `docs/`:
- `linear-workflow.md` — Detailed ticket creation, sizing, and workflow guide
- `linear-archiving.md` — How to archive Done tickets (filtering, sorting, GraphQL API)
- `configuration.md` — Full config reference with examples
- `runtime-files.md` — `~/.botfarm/` directory layout, logs, and temporary files
- `database.md` — SQLite schema, tables, event types, migration history
- `philosophy.md` — Project design principles and trade-offs
- `improvements.md` — Planned improvements and ideas
- `competitors.md` — Competitor analysis
- `codex-cli.md` — Codex CLI automation, approval, and non-interactive behavior
- `refactoring-analysis.md` — Periodic refactoring analysis procedure for agents

Key patterns:
- Workers run as subprocesses; communicate results via `multiprocessing.Queue`
- All state persists to SQLite (`~/.botfarm/botfarm.db`) after every mutation — supervisor survives crashes
- Usage limits pause slots mid-pipeline and resume from interrupted stage
- Claude invoked via `claude -p --output-format json` subprocess

## Runtime Files
All runtime data lives under `~/.botfarm/` (see `docs/runtime-files.md` for full details):
- `config.yaml` / `.env` — configuration and environment variables
- `botfarm.db` — SQLite database (slots, tasks, stage runs, events, usage snapshots)
- `logs/supervisor.log` — main supervisor log (rotated)
- `logs/<TICKET-ID>/worker.log` — per-ticket worker log
- `logs/<TICKET-ID>/<stage>[-iter<N>]-<timestamp>.log` — per-stage Claude subprocess output
- `slots/<project>-<slot_id>/` — temporary sandboxed DB copies (auto-cleaned)

## Workflow: Linear Tickets
For full details on ticket creation, sizing, and all ticket types see `docs/linear-workflow.md`.

### Implementation Tickets
The supervisor handles status transitions (→ In Progress before, → In Review/Done after). Agent focuses on code:
1. Fetch ticket details via Linear MCP (`get_issue`) — use the `gitBranchName` field for the branch name
2. `git fetch origin && git checkout -b <gitBranchName> origin/main`
3. Delete previous working branch if it exists (NEVER delete: main, slot-1-placeholder)
4. Run baseline tests before starting work
5. Implement changes
6. Add/update tests
7. Run full test suite — fix until green
8. Commit, push
9. Create PR via `gh` (Linear-GitHub integration auto-links via branch name)
10. If you identify out-of-scope issues — create new Linear tickets for them

### Investigation Tickets (label: `Investigation`)
No PR created. Agent researches and posts findings as a Linear comment, then creates follow-up tickets.
Review/fix loop happens via Linear comments (not GitHub).

### Creating Tickets
- Always use team "Smart AI Coach", project "Bot farm"
- If a task may consume >60% of 200k context — split into smaller tickets
- Use parent tickets to group related work; set dependencies (`blocks`/`is blocked by`)
- Use `Investigation` label for research tasks, `Human` label for tasks requiring human action
- If blocked by a Human ticket, notify the human via a Linear comment on the blocking ticket

## Testing
- Run tests in a subagent that returns ONLY: pass/fail summary + failing test details
- Exception: 1-2 individual tests can run in main thread
- Command: `python -m pytest tests/ -v` (use .venv virtualenv)
  - Tests run in parallel via pytest-xdist (`-n auto` is configured in pyproject.toml addopts)
  - Playwright tests are excluded by default via addopts marker filter
- No Playwright / no frontend tests
- Run tests: before work, after implementation, after test changes, before push
- Pre-commit hook: `.githooks/pre-commit` — activate with `git config core.hooksPath .githooks`
- Test isolation (required for parallel execution):
  - Never use shared global state between tests
  - Always use `tmp_path` for file system operations and `monkeypatch` for environment variables
  - Never use `os.environ` directly in tests — use `monkeypatch.setenv`/`monkeypatch.delenv`

## Development
- New packages → add to requirements.txt FIRST, then `pip install -r requirements.txt` (in .venv)
- Install editable: `pip install -e .`
- Validate with: `botfarm --help`
- Branch protection: NEVER delete `main` or `slot-1-placeholder`

## Codex CLI Quick Reference
- `codex exec` is the Codex equivalent of Claude's non-interactive `claude -p`
- Codex `-p` means `--profile`, not prompt mode
- For autonomous runs (default): `codex --dangerously-bypass-approvals-and-sandbox exec "<prompt>"`
- For stdin-driven runs: `printf '%s\n' "<prompt>" | codex --dangerously-bypass-approvals-and-sandbox exec -`
- For machine-readable output: `codex --dangerously-bypass-approvals-and-sandbox exec --json "<prompt>"`
- `--full-auto` is not fully autonomous; it maps to `-a on-request --sandbox workspace-write`
- See `docs/codex-cli.md` for details and examples

## Refactoring Analysis
Botfarm supports periodic codebase refactoring analysis via Investigation tickets (created manually or by agents).
See `docs/refactoring-analysis.md` for the full procedure, decision framework, and thresholds.

## PR Process
- Clear, concise description
- Linear-GitHub integration handles ticket linking automatically via branch name
