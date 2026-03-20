# Jira Workflow Guide

Detailed guide for how agents create, size, and work on Jira tickets. For configuration reference, see [configuration.md](configuration.md).

## Ticket Creation

### Defaults

All tickets must be created with the team and project configured in your `config.yaml` `projects` entries.

### Sizing Tickets

Estimate whether a task will consume more than ~60% of a 200k-token LLM context window. If it likely will, split it into smaller tickets.

Signals a ticket is too large:
- Touches many files across multiple subsystems
- Requires extensive research/reading before implementation begins
- Combines unrelated changes (e.g. refactor + new feature + test overhaul)

When splitting, create a **parent ticket** (epic or story) to group the sub-tickets and set dependencies between them.

### Dependencies

When creating multiple tickets or tickets that depend on existing work:
- Set **"is blocked by"** / **"blocks"** link types properly
- The supervisor skips blocked tickets automatically — incorrect dependencies cause stalls

### Ticket Types

**Implementation tickets** — the default. Agent follows the full pipeline: implement, review, fix, PR checks, merge.

**Investigation tickets** — labeled `Investigation`. The agent researches and produces findings as a Jira comment. No PR is created. The agent should create follow-up tickets (implementation or further investigation) based on findings. After the implement stage, the reviewer reviews findings on Jira (not GitHub) and the review/fix loop happens via Jira comments.

**Human tickets** — labeled `Human`. These are skipped by the supervisor (`exclude_tags` config). Use them for tasks that require human action (e.g. infrastructure changes, credential rotation, design decisions).

## Working on Implementation Tickets

The supervisor handles Jira status transitions automatically. The agent focuses on the code work.

**What the supervisor does before the agent runs:**
1. Polls Jira for tickets in the "Todo" status (filtered by project, labels, dependencies)
2. Moves the ticket to "In Progress"
3. Spawns the worker agent

**What the agent does:**
1. Fetch ticket details via Jira API or MCP tools
2. Derive the branch name from the ticket key (e.g. `PROJECT-123-ticket-summary`)
3. `git fetch origin && git checkout -b <branchName> origin/main`
4. Delete previous working branch if it exists (NEVER delete: `main`, `slot-1-placeholder`)
5. Run baseline tests before starting work
6. Implement changes
7. Add/update tests
8. Run full test suite — fix until green
9. Commit, push
10. Create PR via `gh`

**What the supervisor does after the agent finishes:**
- **Success + PR merged** → moves ticket to "Done"
- **Success + PR open** → moves ticket to "In Review"
- **Failure** → moves ticket to failed status (default: "Todo" to re-queue)
- Optionally posts completion/failure comments (see `bugtracker.comment_on_*` config)

**Out-of-scope work:** If you discover issues outside the current ticket's scope, create new Jira tickets for them rather than expanding the current ticket.

## Working on Investigation Tickets

Investigation tickets use a different workflow — no PR, no code changes expected.

**Agent workflow:**
1. Fetch ticket details via Jira API or MCP tools
2. Research the topic (read code, search docs, analyze architecture)
3. Post findings as a Jira comment on the ticket
4. Create follow-up tickets based on findings (implementation or further investigation)

**Review loop:** The reviewer checks the investigation findings on Jira (not GitHub). If feedback is needed, the review/fix loop happens via Jira comments.

The pipeline short-circuits after the implement stage — review, PR checks, and merge stages are skipped.

## MCP Tools

When Jira is configured as the bugtracker, agents get access to Jira MCP tools via the [mcp-atlassian](https://github.com/sooperset/mcp-atlassian) community server. This gives agents the ability to search issues, read/write comments, manage labels, and perform other Jira operations directly.

**Prerequisites:** `uvx` must be available on PATH (install via [uv](https://docs.astral.sh/uv/getting-started/installation/)). The preflight check will warn if `uvx` is missing.

The MCP server is launched automatically as a stdio subprocess for each agent invocation. It receives credentials via environment variables derived from your config:

| Config field | MCP env var | Description |
|---|---|---|
| `bugtracker.url` | `JIRA_URL` | Full Jira instance URL (e.g. `https://acme.atlassian.net`) |
| `identities.coder.jira_email` or `bugtracker.email` | `JIRA_USERNAME` | Email for API authentication |
| `identities.coder.jira_api_token` or `bugtracker.api_key` | `JIRA_API_TOKEN` | Jira API token |

## Configuration

```yaml
bugtracker:
  type: jira
  api_key: ${JIRA_API_TOKEN}
  url: https://my-org.atlassian.net  # Jira instance URL
  workspace: my-org                  # Jira Cloud site name (used for ticket poller)
  email: bot@example.com             # Email for Jira API authentication

  # Workflow status names — must match your Jira project's workflow
  todo_status: To Do
  in_progress_status: In Progress
  done_status: Done
  in_review_status: In Review
```

See [configuration.md](configuration.md) for the full config reference.
