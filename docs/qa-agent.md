# QA Agent

The QA agent is a pipeline type that launches an autonomous "QA engineer" — a Claude Code process that reads project docs, understands a feature from its ticket description, figures out how to run the project, designs test scenarios, executes them using Playwright MCP, shell commands, and DB queries, and produces a structured QA report. Unlike scripted E2E tests, this agent reasons about *what* to test from the ticket context and investigates unexpected behavior.

## How It Works

The QA agent reuses existing Botfarm primitives:

- **Slot/worktree** — runs in a regular slot with the branch checked out from `origin/main`
- **Pipeline template** — `qa` pipeline with a single `qa` stage (stored in DB like existing pipelines). Must be created manually via the dashboard or SQL — it is not auto-seeded on fresh installs
- **Label routing** — tickets with `manual-qa` label route to the `qa` pipeline via `pipeline_templates.ticket_label` (requires the `qa` pipeline template to exist in the DB)
- **MCP tools** — Playwright MCP (for browser testing) is automatically added alongside the bugtracker MCP
- **Bugtracker integration** — report posted as a comment, bugs created as new tickets, labels updated (`qa-passed`/`qa-failed`)

### Pipeline structure

The `qa` pipeline has a single stage:

| Field | Value |
|---|---|
| Stage name | `qa` |
| Executor | `claude` |
| Identity | `coder` |
| Max turns | 300 (QA sessions are long — Playwright back-and-forth) |
| Timeout | 120 minutes |
| Result parser | `qa_report` |

No loops — the agent tests and reports in one pass.

> **Note:** This pipeline template is not auto-provisioned. You must create it manually (via the dashboard or direct SQL insert into `pipeline_templates` / `stage_templates`). The values above are recommended defaults.

## Trigger Scenarios

### Scenario A: Standalone QA ticket

Create a ticket with the `manual-qa` label. The poller picks it up and routes it to the `qa` pipeline. The agent checks out `origin/main` at HEAD and runs immediately.

Examples:
- "Test MCP scanning end-to-end after recent sprint changes"
- "Post-deploy sanity check for v2.4.0 — run through critical flows"
- "Regression test: verify login, dashboard, scan creation still work"

## Creating QA Tickets

To trigger the QA agent manually:

1. Create a ticket in your bugtracker (Linear or Jira)
2. Add the `manual-qa` label
3. Set priority and description as you would for any ticket
4. The ticket description should explain **what to test** — the agent uses this to design test scenarios

The description can be as simple as "Test the new user registration flow" or as detailed as a full test plan. The agent adapts its approach based on the level of detail provided.

## Writing `QA_AGENT.md`

Each project that wants QA agent support should add a `QA_AGENT.md` file to its repository root. This file tells the agent how to run the project, access the DB, check logs, and what the critical user flows are.

The agent reads this file at runtime. If it doesn't exist, the agent reasons from `CLAUDE.md` and `AGENTS.md` alone (or reports it can't figure out how to run the project).

### Recommended contents

```markdown
# QA_AGENT.md

## How to Run
- Steps to start the application (e.g. `docker compose up`, `npm start`)
- Required environment variables or setup
- URL where the running app is accessible (e.g. http://localhost:3000)

## Database Access
- How to connect to the database
- Useful queries for verifying data integrity

## Logs
- Where to find application logs
- How to tail or search logs

## Critical User Flows
- List the most important flows to test
- Include typical user journeys and expected outcomes

## Test Data
- How to seed test data if needed
- Default credentials for test accounts

## Known Limitations
- Features that are intentionally broken or incomplete
- Third-party services that won't work in the test environment
```

Think of this as onboarding docs for a new QA hire — if the docs are bad, they can't test effectively. That's useful feedback in itself.

## Report Format

The QA agent produces a structured report using text markers that the `qa_report` result parser extracts.

### Report markers

The agent wraps its report between `QA_REPORT_START` and `QA_REPORT_END` markers:

```
QA_REPORT_START
## Test Summary
Tested 5 scenarios for the user registration feature.

## Results
- Sign up with valid email: PASSED
- Sign up with duplicate email: PASSED
- Password validation: FAILED (see bug below)
- Email confirmation flow: PASSED
- Login after registration: PASSED

Verdict: FAILED
QA_REPORT_END
```

### Bug markers

Each bug found is wrapped in `BUG_START` / `BUG_END` markers with structured fields:

```
BUG_START
Title: Password validation accepts passwords shorter than 8 characters
Severity: high
Description:
The registration form accepts a 3-character password ("abc") without
showing any validation error. The user is registered successfully but
the short password violates the stated policy of minimum 8 characters.

Steps to reproduce:
1. Navigate to /register
2. Enter valid email and password "abc"
3. Click Submit — registration succeeds with no error
BUG_END
```

Supported severity levels: `critical`, `high`, `medium`, `low`.

### JSON format (alternative)

The agent can also output a JSON report:

```json
{
  "type": "qa_report",
  "passed": false,
  "summary": "Tested 5 scenarios, 1 bug found",
  "bugs": [
    {
      "title": "Password validation accepts short passwords",
      "description": "...",
      "severity": "high"
    }
  ],
  "report_text": "Full human-readable report..."
}
```

### Verdict line

The report should include a `Verdict:` line with either `PASSED` or `FAILED`. The `Verdict:` line can appear anywhere inside the `QA_REPORT_START`/`QA_REPORT_END` block (or the full output text as a fallback) — placement doesn't matter, the parser searches with a multiline regex.

If no `Verdict:` line is found, the system infers the result from whether bugs were detected: no bugs = PASSED, any bugs = FAILED. If the entire report cannot be parsed at all (no markers and no JSON), it defaults to FAILED.

## Post-QA Actions

After the QA pipeline completes, the supervisor automatically:

1. **Posts the QA report** as a comment on the trigger ticket
2. **Creates bug tickets** for each bug found:
   - Title: `[QA Bug] <bug title>`
   - Priority mapped from severity: critical=1, high=2, medium=3, low=4
   - Labels: `Bug`, `QA`
   - Referenced from the original ticket (via text in description: "Found during QA of TICKET-ID")
3. **Updates labels** on the trigger ticket:
   - Adds `qa-passed` if no bugs found
   - Adds `qa-failed` if bugs found (or if the report couldn't be parsed)
4. **Status transition**:
   - Standalone QA tickets: moved to Done if passed, left open with `qa-failed` if failed
   - Implementation tickets with QA: bugs tracked separately, ticket stays at Done

## Configuration

The QA pipeline uses the same configuration infrastructure as other pipelines. Stage-level settings (max turns, timeout) are defined in the pipeline template stored in the database.

### Pipeline template settings

| Setting | Recommended | Description |
|---|---|---|
| `max_turns` | 300 | Maximum Claude turns for the QA session |
| `timeout_minutes` | 120 | Maximum duration for the QA stage |

These values are set when you create the pipeline template. They can be customized by editing the `stage_templates` record in the database.

### Timeout overrides

Per-label timeout overrides configured in `config.yaml` apply to QA stages as well:

```yaml
agents:
  timeout_overrides:
    manual-qa:
      qa: 180  # Allow 3 hours for QA on tickets with this label
```

### MCP servers

The `qa` pipeline template has Playwright MCP configured via its `mcp_servers` column. This is merged into the base bugtracker MCP config automatically at pipeline start. No additional configuration is needed.

## Prerequisites

### Node.js

The Playwright MCP server is launched via `npx`, which requires Node.js to be installed on the machine running Botfarm workers.

```bash
# Verify Node.js is available
node --version  # v18+ recommended
npx --version
```

The first QA run may take a moment longer as `npx` downloads the `@anthropic/mcp-playwright` package.

### Playwright browsers

Playwright downloads browser binaries on first use. If running in a headless server environment, ensure the necessary system dependencies are installed:

```bash
npx playwright install --with-deps chromium
```

## Example Report Output

Here's what a typical QA report comment looks like on the ticket:

```
**QA Report — failed**

Tested 5 scenarios for the user registration feature.
3 passed, 1 failed, 1 could not be tested (third-party email service unavailable).

**Bugs found: 1**
- **[high]** Password validation accepts passwords shorter than 8 characters

---
## Test Summary
Tested 5 scenarios for the user registration feature.

## Detailed Results

### 1. Sign up with valid email — PASSED
Navigated to /register, entered valid credentials, confirmed registration succeeded.

### 2. Sign up with duplicate email — PASSED
Attempted registration with existing email, received expected error message.

### 3. Password validation — FAILED
Registration accepted a 3-character password without validation error.

### 4. Email confirmation flow — SKIPPED
Could not test: email service not running in test environment.

### 5. Login after registration — PASSED
Successfully logged in with newly created account.

Verdict: FAILED
```

## Troubleshooting

### Environment cleanup

The QA agent may start services (docker compose, npm start, etc.) and launch a Playwright browser. If the agent times out or fails, these processes may be left running. The supervisor's timeout handler kills the worker process tree, but background processes started by the agent (docker containers, spawned servers) may survive.

To clean up manually:
- Check for orphaned processes on the expected port: `lsof -i :<port>`
- Kill orphaned containers: `docker compose down` in the worktree directory
- Kill orphaned browser processes: `pkill -f chromium` or `pkill -f playwright`

### Port conflicts

If a QA run fails with port-already-in-use errors, a previous run likely left a process behind. Check and kill the process using the port:

```bash
lsof -i :3000  # or whatever port the project uses
kill <PID>
```

### Agent can't figure out how to run the project

If the QA report says the agent couldn't start the application, add or improve your project's `QA_AGENT.md`. The agent needs clear instructions for:
- How to install dependencies
- How to start the application
- What URL to access
- Any required environment variables

### Playwright MCP not available

If the QA agent can't use browser testing tools:
1. Verify Node.js is installed: `node --version`
2. Check that `npx` can download packages (no network restrictions)
3. Verify the `qa` pipeline template has `mcp_servers` configured with the Playwright entry
