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

### Fully non-interactive, sandboxed

Use `-a never` to prevent approval prompts while keeping sandboxing enabled:

```bash
codex -a never -s workspace-write exec "fix the failing tests and commit the result"
```

This is the safest automation mode when you still want filesystem isolation.

### Fully non-interactive, prompt from stdin

```bash
printf '%s\n' 'fix the failing tests and commit the result' | \
  codex -a never -s workspace-write exec -
```

### Fully autonomous and unsandboxed

```bash
codex --dangerously-bypass-approvals-and-sandbox exec \
  "implement this task end-to-end"
```

This is the closest Codex equivalent to a "run without interactive input and do not stop for permissions" mode. It is broader than Claude's flag because it also disables sandboxing.

### `--full-auto` is not the same as zero-interaction

`--full-auto` expands to:

```text
-a on-request --sandbox workspace-write
```

That means the agent may still choose to ask for approval. For unattended automation, prefer `-a never` or `--dangerously-bypass-approvals-and-sandbox`.

## Recommended Invocation Patterns

### Non-interactive run with JSON event stream

```bash
codex -a never -s workspace-write exec --json \
  "update the docs and run the relevant tests"
```

### Capture the final assistant message to a file

```bash
codex -a never -s workspace-write exec \
  -o /tmp/codex-last.txt \
  "summarize the changes after tests pass"
```

### Change working directory explicitly

```bash
codex -a never -s workspace-write -C /path/to/repo exec \
  "fix the regression and run tests"
```

## Practical Notes

- Put global flags before `exec` in scripts. That matches the top-level CLI help and avoids relying on ambiguous parsing.
- Use `--add-dir` when the agent needs write access outside the primary workspace.
- Use `--ephemeral` if you do not want Codex session files persisted to disk.
- Use `--skip-git-repo-check` only when intentionally running outside a Git repository.
