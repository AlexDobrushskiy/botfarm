# CLAUDE.md

## Project Context
- Botfarm: autonomous Linear ticket dispatcher for Claude Code agents
- Pure Python CLI project — no frontend, no database migrations, no run.sh/stop.sh
- This CLAUDE.md primarily covers the implementer workflow. Reviewer and review-addresser agents receive instructions via prompt.

## Architecture
Planned modules (under `botfarm/`):
- `cli.py` — Click CLI entry point (status, history, limits commands)
- `config.py` — YAML config loading and validation
- `supervisor.py` — Main loop: poll Linear, dispatch workers, manage slots
- `worker.py` — Single-ticket agent lifecycle (implement → review → validate → merge)
- `db.py` — SQLite via aiosqlite for task history and state
- `linear.py` — Linear API client (fetch tickets, update status)
- `usage.py` — Token/cost tracking and rate-limit awareness

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

## PR Process
- Clear description linking Linear ticket
- Concise but meaningful
