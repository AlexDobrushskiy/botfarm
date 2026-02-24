# Botfarm Philosophy

## Core Idea

Botfarm exists because AI coding agents are good enough to handle routine software engineering tickets autonomously, but not good enough to manage their own work queue, recover from failures, or respect API rate limits. Botfarm is the orchestration layer that bridges this gap.

The system is a **supervisor**, not a replacement for developer judgment. It picks up tickets, runs agents, and manages the pipeline. A human still writes the tickets, prioritizes the backlog, and reviews the results. Botfarm automates the repetitive dispatch-and-wait cycle so the human can focus on higher-level work.

## Design Principles

### 1. Wrap, don't reinvent

The existing Claude Code CLI works well. Botfarm wraps it (`claude -p --output-format json`) rather than reimplementing agent logic. The pipeline stages — implement, review, fix, CI checks, merge — mirror what a developer does manually. Each stage is a fresh Claude subprocess with clear inputs and outputs.

### 2. Process isolation is non-negotiable

Every Claude invocation runs as its own OS process in its own git worktree. A hanging agent in slot 1 never affects slot 2 or the supervisor. Timeouts kill the process group. The supervisor's main loop catches all exceptions per-phase so a single failure never cascades.

### 3. Survive everything

The supervisor assumes it will crash. State persists to `state.json` after every mutation (atomic write-then-rename). On restart, it reconciles saved state against reality — checking PIDs, PR status, and database records. The system picks up where it left off without human intervention.

### 4. Configuration over code

Project definitions, slot assignments, timeouts, thresholds, workflow status names — all live in `config.yaml`. Adding a new project or adjusting behavior requires editing YAML, not Python. The person running botfarm should never need to touch the codebase for operational changes.

### 5. Observe everything

Every dispatch, stage transition, failure, timeout, limit hit, and recovery action is logged as a `task_event` in SQLite. Usage snapshots track API consumption over time. The dashboard provides real-time visibility. When something goes wrong at 3 AM, the event log tells you exactly what happened.

### 6. Graceful degradation

Missing OAuth credentials? Usage polling is disabled, dispatch continues without limit checks. Webhook URL not configured? Notifications are silently skipped. Dashboard disabled? The supervisor still runs. Linear API down? The supervisor logs a warning and retries next tick. No feature failure should bring down the core dispatch loop.

### 7. Keep it simple

This is an internal tool for one person (or a small team). Server-rendered HTML with HTMX auto-refresh is sufficient for the dashboard. SQLite is the right database. Python with Click/Rich is the right CLI stack. No React SPA, no Kubernetes, no message queues. The right amount of complexity is the minimum needed for the current task.

## Pipeline Philosophy

The implement-review-fix pipeline embodies a key insight: **AI agents produce better code when their work is reviewed by another AI agent**. The reviewer catches issues that the implementer misses. The fix stage addresses review feedback. This review loop — up to 3 iterations by default — significantly improves output quality before any human sees the PR.

When the reviewer approves (detected via a structured VERDICT marker), the pipeline skips unnecessary fix iterations and proceeds directly to CI checks. When CI fails, a separate retry loop uses failure context to help Claude diagnose and fix the issue.

The pipeline always proceeds forward. After max iterations, it moves to CI checks regardless of review status. The goal is completion, not perfection — a human can always request changes on the PR.

## What Botfarm Is Not

- **Not a CI/CD system.** It triggers CI checks and waits for results, but doesn't replace GitHub Actions or similar.
- **Not a project manager.** It works tickets from a backlog. A human still writes, prioritizes, and tags those tickets.
- **Not a code quality gate.** The self-review loop improves quality but doesn't guarantee it. Human review of merged PRs is still expected.
- **Not multi-tenant.** It runs as a single user's tool. There's no auth, no RBAC, no shared access control.

## Trade-offs Accepted

- **Subprocess over SDK**: More overhead, less real-time visibility, but proven reliability and clean isolation.
- **JSON state file over database for runtime state**: Simpler, atomic, readable with `cat`, but doesn't scale beyond ~10 slots.
- **SQLite over Postgres**: Single-writer, no concurrent access from multiple machines, but zero operational overhead.
- **Poll-based over event-driven**: The supervisor polls Linear every N seconds rather than receiving webhooks. Simpler, no public endpoint needed, but adds latency between ticket creation and dispatch.
- **Heuristic limit detection**: Usage limit hits are detected by string matching in error messages. Not 100% reliable, but combined with pre-dispatch usage checks, it works well enough in practice.
