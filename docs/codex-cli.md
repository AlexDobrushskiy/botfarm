# Codex CLI Quick Reference

This project primarily targets Claude Code, but contributors sometimes run the repository with the OpenAI Codex CLI instead. This document captures the Codex behaviors that matter for automation and orchestration.

Verified against `codex-cli 0.106.0` on February 27, 2026 via local `codex --help` and `codex exec --help`.

## Key Mapping From Claude Code

| Claude Code | Codex CLI | Notes |
| --- | --- | --- |
| `claude -p` | `codex exec` | Codex uses the `exec` subcommand for non-interactive runs. |
| `--dangerously-skip-permissions` | `--dangerously-bypass-approvals-and-sandbox` | Codex bypasses both approvals and sandboxing, not just permission prompts. |
| prompt from stdin | `codex exec -` | If the prompt argument is `-`, Codex reads instructions from stdin. |
| structured output | `codex exec --json` | Emits JSONL events to stdout. |

Important: in Codex, `-p` means `--profile`, not "prompt mode".

## Approval And Sandbox Behavior

### Fully autonomous and unsandboxed (default for Botfarm)

```bash
codex --dangerously-bypass-approvals-and-sandbox exec \
  "implement this task end-to-end"
```

This is the standard way to run autonomous Codex agents. It disables both approval prompts and sandboxing, matching the behavior of Claude Code's `--dangerously-skip-permissions`. Since Botfarm runs on an isolated VM, the sandbox provides no additional security benefit while blocking necessary operations like network access (required for `gh api` calls during code review).

### Fully non-interactive, sandboxed

Use `-a never -s workspace-write` to keep filesystem sandboxing while preventing approval prompts:

```bash
codex -a never -s workspace-write exec "fix the failing tests and commit the result"
```

Note: the `workspace-write` sandbox blocks network access by default, which prevents `gh` CLI commands from working. Enable network access with `-c 'sandbox_workspace_write.network_access=true'` if you need `gh api` calls in this mode.

### Fully non-interactive, prompt from stdin

```bash
printf '%s\n' 'fix the failing tests and commit the result' | \
  codex --dangerously-bypass-approvals-and-sandbox exec -
```

### `--full-auto` is not the same as zero-interaction

`--full-auto` expands to:

```text
-a on-request --sandbox workspace-write
```

That means the agent may still choose to ask for approval. For unattended automation, use `--dangerously-bypass-approvals-and-sandbox`.

## Recommended Invocation Patterns

### Non-interactive run with JSON event stream

```bash
codex --dangerously-bypass-approvals-and-sandbox exec --json \
  "update the docs and run the relevant tests"
```

### Capture the final assistant message to a file

```bash
codex --dangerously-bypass-approvals-and-sandbox exec \
  -o /tmp/codex-last.txt \
  "summarize the changes after tests pass"
```

### Change working directory explicitly

```bash
codex --dangerously-bypass-approvals-and-sandbox -C /path/to/repo exec \
  "fix the regression and run tests"
```

## Practical Notes

- Put global flags before `exec` in scripts. That matches the top-level CLI help and avoids relying on ambiguous parsing.
- Use `--add-dir` when the agent needs write access outside the primary workspace.
- Use `--ephemeral` if you do not want Codex session files persisted to disk.
- Use `--skip-git-repo-check` only when intentionally running outside a Git repository.
