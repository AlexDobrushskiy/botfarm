# Linear Workflow Guide

Detailed guide for how agents create, size, and work on Linear tickets. For configuration reference, see [configuration.md](configuration.md).

## Ticket Creation

### Defaults

All tickets must be created with:
- **Team:** "Smart AI Coach"
- **Project:** "Bot farm"

### Sizing Tickets

Estimate whether a task will consume more than ~60% of a 200k-token LLM context window. If it likely will, split it into smaller tickets.

Signals a ticket is too large:
- Touches many files across multiple subsystems
- Requires extensive research/reading before implementation begins
- Combines unrelated changes (e.g. refactor + new feature + test overhaul)

When splitting, create a **parent ticket** to group the sub-tickets and set dependencies between them.

### Dependencies

When creating multiple tickets or tickets that depend on existing work:
- Set **"is blocked by"** / **"blocks"** relations properly
- The supervisor skips blocked tickets automatically — incorrect dependencies cause stalls

### Ticket Types

**Implementation tickets** — the default. Agent follows the full pipeline: implement, review, fix, PR checks, merge.

**Investigation tickets** — labeled `Investigation`. The agent researches and produces findings as a Linear comment. No PR is created. The agent should create follow-up tickets (implementation or further investigation) based on findings. After the implement stage, the reviewer reviews findings on Linear (not GitHub) and the review/fix loop happens via Linear comments.

**Human tickets** — labeled `Human`. These are skipped by the supervisor (`exclude_tags` config). Use them for tasks that require human action (e.g. infrastructure changes, credential rotation, design decisions). When working on a ticket queue and you discover a ticket is blocked by a Human-tagged ticket, notify the human via a Linear comment on the blocking ticket.

### Parent Tickets

Use parent tickets to organize groups of related work. The supervisor handles parent tickets automatically:
- Parent tickets with open children are skipped (not dispatched)
- When all children reach completed/canceled status, the parent is auto-closed

## Working on Implementation Tickets

The supervisor handles Linear status transitions automatically. The agent focuses on the code work.

**What the supervisor does before the agent runs:**
1. Polls Linear for "Todo" tickets (filtered by team, project, labels, dependencies)
2. Moves the ticket to "In Progress"
3. Spawns the worker agent

**What the agent does:**
1. Fetch ticket details via Linear MCP tools (auto-configured from `bugtracker.api_key`)
2. Get the branch name from the `gitBranchName` field in the issue response
3. `git fetch origin && git checkout -b <gitBranchName> origin/main`
4. Delete previous working branch if it exists (NEVER delete: `main`, `slot-1-placeholder`)
5. Run baseline tests before starting work
6. Implement changes
7. Add/update tests
8. Run full test suite — fix until green
9. Commit, push
10. Create PR via `gh` (Linear-GitHub integration auto-links the PR to the ticket via the branch name)

**What the supervisor does after the agent finishes:**
- **Success + PR merged** → moves ticket to "Done"
- **Success + PR open** → moves ticket to "In Review"
- **Failure** → moves ticket to failed status (default: "Todo" to re-queue)
- Optionally posts completion/failure comments (see `linear.comment_on_*` config)

**Out-of-scope work:** If you discover issues outside the current ticket's scope, create new Linear tickets for them rather than expanding the current ticket.

## Working on Investigation Tickets

Investigation tickets use a different workflow — no PR, no code changes expected.

**Agent workflow:**
1. Fetch ticket details via Linear MCP tools
2. Research the topic (read code, search docs, analyze architecture)
3. Post findings as a Linear comment on the ticket
4. Create follow-up tickets based on findings (implementation or further investigation)

**Review loop:** The reviewer checks the investigation findings on Linear (not GitHub). If feedback is needed, the review/fix loop happens via Linear comments — similar to how code review works on GitHub, but entirely within Linear.

The pipeline short-circuits after the implement stage — review, PR checks, and merge stages are skipped.

## Ticket Priority and Ordering

The supervisor dispatches tickets based on Linear's **manual sort order** (`sortOrder` field). This respects the drag-and-drop ordering set in the Linear UI. Higher position in the list = dispatched first.

Priority field is fetched but sort order takes precedence for dispatch ordering.

## Blocked Ticket Handling

The supervisor checks two types of blocking:
- **Explicit relations:** `isBlockedBy` / `blocks` relations in Linear
- **Parent-child:** Parent tickets with open children are implicitly blocked

Only unresolved blockers count — completed or canceled blockers are ignored.
