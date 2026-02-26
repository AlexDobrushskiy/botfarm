# Investigation: Real-time Context Monitoring During Running Stages

**Ticket:** SMA-154
**Date:** 2026-02-26

## Summary

Real-time context fill monitoring is **feasible** using Claude Code's
`--output-format stream-json` mode combined with `subprocess.Popen` for
line-by-line output reading.

---

## 1. `--output-format stream-json`

Claude Code supports a streaming NDJSON output mode activated with:

```
claude -p --output-format stream-json --verbose --dangerously-skip-permissions
```

**Requirements:** The `--verbose` flag is mandatory when using `stream-json`
in `--print` mode (Claude Code will exit with an error otherwise).

### Output Schema

Each line is a standalone JSON object. The message types emitted in order:

| Type | Subtype | When | Contains Token Data |
|------|---------|------|---------------------|
| `system` | `init` | Once at start | No — has session_id, model, tools |
| `assistant` | — | Each API turn | **Yes** — per-turn usage |
| `rate_limit_event` | — | After assistant msgs | No — rate limit status |
| `user` | — | After tool execution | No — tool result content |
| `result` | `success`/`error` | Once at end | **Yes** — cumulative usage + modelUsage |

### Per-Turn Token Data (from `assistant` messages)

Each `assistant` message includes a `message.usage` object:

```json
{
  "input_tokens": 1,
  "cache_creation_input_tokens": 109,
  "cache_read_input_tokens": 35106,
  "output_tokens": 24,
  "service_tier": "standard"
}
```

These are **per-API-call** counts (not cumulative). The total input tokens
for a given turn represent the full conversation history size at that point:

```
context_at_turn = input_tokens + cache_creation_input_tokens + cache_read_input_tokens
```

This grows with each turn as the conversation history expands.

### Final Result Data

The `result` message includes cumulative usage and `modelUsage` with the
context window size:

```json
{
  "modelUsage": {
    "claude-opus-4-6": {
      "inputTokens": 4,
      "outputTokens": 154,
      "cacheReadInputTokens": 100091,
      "cacheCreationInputTokens": 5524,
      "contextWindow": 200000,
      "maxOutputTokens": 32000
    }
  }
}
```

### Real-time Context Fill Calculation

At each `assistant` message, we can compute the current context fill:

```python
context_fill_pct = (
    (input_tokens + cache_creation_input_tokens + cache_read_input_tokens + output_tokens)
    / context_window
    * 100
)
```

The `contextWindow` value (e.g. 200,000) is only available in the final
`result` message. For real-time monitoring, we should use a configured
default (200,000 for current Claude models) and update it from the result
when available.

---

## 2. Per-Turn Token Counts — Verified

Multi-turn test (3 turns with tool use) confirmed:

- **Turn 1:** 35,108 input tokens (2 + 5,336 + 29,770)
- **Turn 2:** 35,216 input tokens (1 + 109 + 35,106) — grew by ~108 tokens
- **Turn 3:** 35,295 input tokens (1 + 79 + 35,215) — grew by ~79 tokens

Context fill grows monotonically as expected. Each turn adds the previous
assistant response + tool results to the conversation.

---

## 3. `subprocess.Popen` Streaming — Impact Assessment

### Current Implementation

`run_claude()` uses `subprocess.run()` with `capture_output=True`, which
blocks until the process completes and returns all output at once.

### Required Changes

Switch to `subprocess.Popen()` to read stdout line-by-line:

```python
proc = subprocess.Popen(
    cmd,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    cwd=str(cwd),
    env=subprocess_env,
)
proc.stdin.write(prompt)
proc.stdin.close()

lines = []
for line in proc.stdout:
    lines.append(line)
    event = json.loads(line)
    if event["type"] == "assistant":
        usage = event["message"]["usage"]
        # emit context fill update
    elif event["type"] == "result":
        # final result
```

### Impact on Existing Code

| Area | Impact | Notes |
|------|--------|-------|
| Output parsing | **Medium** | `parse_claude_output()` currently expects a single JSON blob. With stream-json, stdout is NDJSON (multiple lines). Need a new parser or adapter that extracts the `result` line. |
| Error handling | **Low** | `proc.returncode` still available after `proc.wait()`. stderr can be read after stdout is consumed. |
| Log file writing | **Low** | Can still collect all lines and write them at the end, or stream to log file in real-time. |
| Timeout handling | **Medium** | Currently inherited from subprocess.run. With Popen, need explicit timeout via `proc.wait(timeout=...)` or a watchdog thread. |
| Return value | **None** | `ClaudeResult` dataclass unchanged — populated from the `result` line. |
| Test changes | **Medium** | Tests that mock `subprocess.run` will need updating for `Popen`. |

### Backward Compatibility

The `--output-format json` (current) and `--output-format stream-json` produce
equivalent final data. The `result` line in stream-json contains the same
fields as the single JSON output from `--output-format json`. A phased
migration is possible:

1. Phase 1: Add stream-json support alongside existing json mode
2. Phase 2: Migrate all invocations to stream-json
3. Phase 3: Remove json mode support

---

## 4. Update Frequency

Context fill updates at **every assistant turn** — typically every few
seconds during active work. In a 200-turn implement stage, this means
~200 updates over 10-30 minutes of execution.

This frequency is appropriate: not too noisy (updates are meaningful
turn boundaries), and frequent enough to catch context filling up before
the stage completes.

---

## 5. Alternative Approaches

### 5a. Estimate from Turn Count + Average Tokens

- **Feasibility:** Low accuracy. Token usage varies wildly per turn (a turn
  reading a large file vs. writing a one-line edit).
- **Recommendation:** Not recommended as primary approach. Could serve as
  a rough fallback if streaming isn't available.

### 5b. Poll Claude Code's Session API

- **Feasibility:** No known public session API for querying token counts
  of a running session.
- **Recommendation:** Not feasible.

### 5c. Watch Claude Code Internal Log Files

- **Feasibility:** Claude Code writes session data to
  `~/.claude/projects/*/sessions/`, but the format is internal and subject
  to change without notice.
- **Recommendation:** Not recommended — too fragile.

### 5d. Stream-JSON (Recommended)

- **Feasibility:** High. Fully supported, documented output format.
- **Recommendation:** This is the correct approach.

---

## 6. Recommendations

### Implement: Stream-JSON with Popen

1. **Add `run_claude_streaming()` function** alongside existing `run_claude()`:
   - Uses `subprocess.Popen` with `--output-format stream-json --verbose`
   - Reads stdout line-by-line
   - Emits context fill updates via a callback or writes to DB
   - Returns the same `ClaudeResult` from the `result` line

2. **Add context fill callback to stage execution**:
   - `_PipelineContext.run_and_record()` accepts an optional callback
   - Callback receives `(stage, turn_number, context_fill_pct)` per turn
   - Callback writes to `stage_runs` or a new `context_fill_events` table

3. **Dashboard integration**:
   - Dashboard already polls `context_fill_pct` from DB
   - With per-turn updates, it will show live fill instead of post-hoc fill

### Implementation Tickets Created

- **SMA-160 — Streaming Claude runner**: Add `run_claude_streaming()` using
  Popen + stream-json, with per-turn context fill callback
- **SMA-161 — Live context fill DB writes**: Write per-turn context fill to
  DB during stage execution (update `stage_runs.context_fill_pct` in-place)
- Dashboard live context fill is already partially supported via polling;
  no separate ticket needed since the dashboard already reads
  `stage_runs.context_fill_pct`.
