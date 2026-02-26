# Investigation: Live Claude Code Output Streaming Architecture

**Ticket:** SMA-155
**Date:** 2026-02-26

## Summary

Live streaming of Claude Code output to the dashboard is **feasible** using
`--output-format stream-json --include-partial-messages` combined with
`subprocess.Popen`, real-time log file writing, and Server-Sent Events (SSE)
for the dashboard transport layer.

---

## 1. Claude Code Output Formats

### Available Formats

Claude Code's `--output-format` flag (requires `--print` mode) supports:

| Format | Behavior |
|--------|----------|
| `text` | Plain text output, not structured, cannot distinguish events |
| `json` | Single JSON object after completion — current botfarm approach |
| `stream-json` | NDJSON (one JSON object per line), emitted in real-time |

### `stream-json` Message Types

Each line is a standalone JSON object. Types emitted in order:

| Type | Subtype | When | Key Data |
|------|---------|------|----------|
| `system` | `init` | Once at start | session_id, model, tools list, mcp_servers |
| `assistant` | — | Each API turn | Full message content + per-turn usage |
| `rate_limit_event` | — | After assistant msgs | Rate limit status |
| `user` | — | After tool execution | Tool result content |
| `result` | `success`/`error` | Once at end | Cumulative usage, modelUsage, total_cost_usd |

### `--include-partial-messages` Flag

When combined with `stream-json`, this flag adds `stream_event` messages
that mirror the Anthropic API streaming protocol:

```
system/init
stream_event → message_start
stream_event → content_block_start (type=text)
stream_event → content_block_delta (text chunk: "Lines")
stream_event → content_block_delta (text chunk: " of logic")
stream_event → content_block_delta (text chunk: " flow")
...
assistant (full assembled message with usage)
stream_event → content_block_stop
stream_event → message_delta (stop_reason=end_turn)
stream_event → message_stop
rate_limit_event
result/success
```

For tool use, `content_block_start` has `type=tool_use` and deltas contain
`input_json_delta` with partial JSON fragments. The `user` message after
tool execution contains the tool result.

### Requirements

- `--verbose` is **mandatory** with `stream-json` (Claude Code exits with
  error otherwise)
- `--include-partial-messages` is optional — omit for turn-level granularity,
  include for token-by-token streaming

### Verified via Live Test

Tested `echo "prompt" | claude -p --output-format stream-json --verbose
--include-partial-messages` — confirmed all event types stream in real-time
as documented above.

---

## 2. Subprocess Streaming

### Current Implementation (worker.py)

`run_claude()` uses `subprocess.run()` with `capture_output=True`, which:
- Blocks until the entire process completes
- Returns all stdout/stderr at once as strings
- Log files are written **after** completion via `_write_subprocess_log()`

### Required: Switch to `subprocess.Popen`

```python
proc = subprocess.Popen(
    cmd,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=open(stderr_path, "w"),  # Separate file avoids deadlock
    text=True,
    cwd=str(cwd),
    env=subprocess_env,
)
proc.stdin.write(prompt)
proc.stdin.close()

for line in iter(proc.stdout.readline, ""):
    log_fh.write(line)        # Real-time log file append
    log_fh.flush()
    event = json.loads(line)
    if event["type"] == "assistant":
        # Emit per-turn context fill update
        callback(event["message"]["usage"])
    elif event["type"] == "result":
        final_result = event
```

### Deadlock Prevention

Reading stdout line-by-line while `stderr=subprocess.PIPE` risks deadlock
if stderr fills the OS pipe buffer (~64KB on Linux). Three solutions:

1. **Redirect stderr to file** (recommended): `stderr=open(stderr_path, "w")`
2. Drain stderr on a separate thread
3. Use asyncio subprocess

Option 1 is simplest and aligns with existing log file architecture.

### Impact on Existing Code

| Area | Impact | Notes |
|------|--------|-------|
| `run_claude()` | **High** | New `run_claude_streaming()` function using Popen |
| `parse_claude_output()` | **Medium** | New parser that extracts `result` line from NDJSON stream |
| Log file writing | **Low** | Moves from post-hoc to real-time — simpler, not harder |
| Timeout handling | **Medium** | Need explicit watchdog (Popen has no built-in timeout for line reads) |
| `ClaudeResult` | **None** | Unchanged — populated from `result` line |
| Test mocks | **Medium** | Tests mocking `subprocess.run` need updates for Popen |

### Backward Compatibility

The `result` line in stream-json contains the same fields as `--output-format
json` output. A phased migration is possible (see SMA-154 investigation).

---

## 3. Dashboard Streaming Transport

### Option A: Server-Sent Events (SSE) — Recommended

**How it works:**
- Dashboard opens an SSE connection to `/api/stream/{ticket_id}/{stage}`
- Server reads the log file in real-time (tail -f style) and pushes events
- htmx has native SSE support via `hx-ext="sse"`

**Pros:**
- One-way (server→client) — exactly what we need
- Native htmx support (`hx-ext="sse"`, `sse-connect`, `sse-swap`)
- Auto-reconnection built into EventSource API
- Works through proxies/CDNs without issues
- FastAPI supports SSE via `StreamingResponse` or `sse-starlette`

**Cons:**
- Unidirectional (fine for log viewing)
- Connection limit per domain in HTTP/1.1 (6 connections) — not an issue
  for single-user dashboard

**Implementation sketch:**

```python
from sse_starlette.sse import EventSourceResponse

@app.get("/api/stream/{ticket_id}/{stage}")
async def stream_log(ticket_id: str, stage: str):
    log_path = _find_active_log(ticket_id, stage)

    async def event_generator():
        with open(log_path) as f:
            while True:
                line = f.readline()
                if line:
                    yield {"event": "log", "data": line.rstrip()}
                else:
                    if _stage_completed(ticket_id, stage):
                        yield {"event": "done", "data": ""}
                        break
                    await asyncio.sleep(0.5)

    return EventSourceResponse(event_generator())
```

```html
<div hx-ext="sse" sse-connect="/api/stream/SMA-123/implement">
    <pre sse-swap="log" hx-swap="beforeend"></pre>
</div>
```

### Option B: WebSocket

**Pros:** Bidirectional, lower overhead per message
**Cons:** More complex setup, not natively supported by htmx (requires
extension), overkill for one-way log streaming

**Verdict:** Unnecessary complexity for this use case.

### Option C: Polling with Log Tailing

**How it works:**
- Dashboard polls `/api/logs/{ticket}/{stage}/tail?offset=N`
- Server reads log file from byte offset N, returns new content + new offset
- Client updates display and polls again

**Pros:**
- Simplest implementation — fits existing polling patterns
- No SSE/WebSocket infrastructure needed
- Works with any HTTP client

**Cons:**
- Higher latency (polling interval = delay)
- More HTTP overhead
- Requires offset tracking on client side

**Verdict:** Viable as a fallback or simpler first step, but SSE provides
better UX with similar implementation effort.

### Recommendation: SSE

SSE is the best fit because:
1. Natural match for one-way log streaming
2. Native htmx support minimizes frontend complexity
3. `sse-starlette` integrates cleanly with FastAPI
4. Real-time delivery without polling overhead
5. Graceful degradation — falls back to long-polling automatically

---

## 4. Log File Architecture

### Current State

- Log files created **after** subprocess completion
- Path: `~/.botfarm/logs/<TICKET-ID>/<stage>[-iter<N>]-<timestamp>.log`
- Contains raw stdout + stderr dump
- Dashboard has **no access** to log files (only reads from SQLite)

### Proposed Architecture

#### Create log file at subprocess start

```python
log_file = _make_stage_log_path(log_dir, stage, iteration)
log_file.parent.mkdir(parents=True, exist_ok=True)
log_fh = log_file.open("w")  # File exists immediately
```

#### Append lines as they arrive

```python
for line in iter(proc.stdout.readline, ""):
    log_fh.write(line)
    log_fh.flush()  # Ensure immediate disk write for concurrent readers
```

#### Concurrent read + write safety

File-based concurrent access is safe on Linux for the append-write +
sequential-read pattern:

- **Writer** (worker): Opens file in write mode, appends lines, calls
  `flush()` after each line
- **Reader** (dashboard SSE): Opens file in read mode, reads to EOF,
  sleeps, reads again
- No file locking needed — there is only a single writer, so there is
  no concurrent `write()` contention. The reader only sees data already
  flushed by the writer
- Reader may see partial lines at EOF — mitigated by only emitting
  complete lines (check for trailing newline)

#### File format options

**Option A: Raw NDJSON** (write stream-json output directly)
- Pro: Zero processing overhead in writer
- Con: Dashboard must parse JSON to render meaningfully

**Option B: Processed text** (extract meaningful content from stream events)
- Pro: Human-readable log files, simpler dashboard rendering
- Con: Processing overhead in worker, lossy transformation

**Recommendation:** Write raw NDJSON to log files. The dashboard SSE
endpoint can optionally transform events for display (e.g., extract
assistant text, summarize tool calls).

#### Register active log file path in DB

Add a column or use the existing `task_events` table to record the
active log file path for each running stage:

```sql
-- Option: Add to stage_runs
ALTER TABLE stage_runs ADD COLUMN log_file_path TEXT;

-- Or: Use task_events
INSERT INTO task_events (task_id, event_type, detail)
VALUES (?, 'stage_log_started', '{"path": "...", "stage": "implement"}');
```

This allows the dashboard to discover which log file to tail without
filesystem scanning.

---

## 5. Impact Assessment

### worker.py Changes

| Change | Scope | Complexity |
|--------|-------|------------|
| Add `run_claude_streaming()` with Popen | New function (~60 lines) | Medium |
| Add NDJSON result parser | New function (~30 lines) | Low |
| Switch command to `--output-format stream-json --verbose` | 2-line change | Low |
| Write log file in real-time during streaming | ~10 lines in new function | Low |
| Add per-turn callback for context fill / event emission | ~15 lines | Low |
| Timeout watchdog for Popen | ~20 lines (threading.Timer or select) | Medium |

### dashboard.py Changes

| Change | Scope | Complexity |
|--------|-------|------------|
| Add SSE endpoint `/api/stream/{ticket_id}/{stage}` | New route (~40 lines) | Medium |
| Add `sse-starlette` dependency | requirements.txt | Low |
| Add log file discovery helper | New function (~15 lines) | Low |

### New Templates

| Template | Purpose | Complexity |
|----------|---------|------------|
| `log_viewer.html` | Full-page or modal log viewer with SSE | Medium |
| `partials/log_stream.html` | Embeddable SSE log panel | Low |

### Backwards Compatibility

- Existing `--output-format json` logs remain readable
- `parse_claude_output()` unchanged for existing tests
- New streaming is additive — old code paths remain functional
- Log file path format unchanged

---

## 6. Architecture Recommendation

### Phase 1: Streaming Worker (foundation)

Add `run_claude_streaming()` alongside existing `run_claude()`:
- Uses Popen + `--output-format stream-json --verbose`
- Writes log file in real-time (lines appended on arrival)
- Extracts `result` line → `ClaudeResult` (same as today)
- Per-turn callback for context fill updates to DB
- Records active log file path in DB

### Phase 2: Dashboard Log Viewer

Add SSE-based log viewer to the dashboard:
- New SSE endpoint tails the active log file
- Transforms raw NDJSON into human-readable events for display
- New log viewer page/panel with auto-scrolling
- Shows: assistant text output, tool calls (summarized), errors

### Phase 3: Enhanced Streaming (optional)

- `--include-partial-messages` for token-by-token text display
- Tool call visualization (which files being read/edited)
- Progress indicators based on tool use patterns

### Dependency

New pip dependency: `sse-starlette` (lightweight SSE support for FastAPI).

---

## 7. Implementation Tickets

See Linear tickets created as part of this investigation:

- **SMA-162 — Streaming Claude runner with real-time log writing**: Add
  `run_claude_streaming()` using Popen + stream-json, write log files in
  real-time, extract result for ClaudeResult
- **SMA-163 — SSE log streaming endpoint for dashboard**: Add SSE endpoint
  to tail active log files, render human-readable events, add log viewer
  template with htmx SSE integration
