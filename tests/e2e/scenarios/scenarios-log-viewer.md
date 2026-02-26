# E2E Test Scenarios: Log Viewer Page (`/task/{id}/logs`)

## Scenario: Stage tabs render for task with logs
**Preconditions:** A task with log files for multiple stages (implement, review, fix)
**Steps:**
1. Navigate to `/task/{task_id}/logs`
2. Observe the stage tab bar
**Expected Result:** Horizontal tab bar shows all stages that have log files. First active or latest stage is auto-selected. Active tab is underlined in primary color
**Priority:** P0

## Scenario: Stage tab switching
**Preconditions:** A task with logs for at least two stages
**Steps:**
1. Navigate to `/task/{task_id}/logs`
2. Click on a different stage tab
**Expected Result:** URL updates to `/task/{task_id}/logs/{stage}`. Log terminal content switches to the selected stage's logs. Previously active tab loses underline, new tab gains it
**Priority:** P0

## Scenario: Completed stage shows full log content
**Preconditions:** A task with a completed stage that has log files
**Steps:**
1. Navigate to `/task/{task_id}/logs/{completed_stage}`
2. Observe the log terminal
**Expected Result:** Full log content is pre-loaded (fetched from `/api/logs/{ticket_id}/{stage}/content`). Terminal shows all log lines. "COMPLETED" badge displayed (green). Content is pre-scrolled to bottom
**Priority:** P0

## Scenario: Active stage shows SSE live stream
**Preconditions:** A task currently in progress with an active stage
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Observe the log terminal
**Expected Result:** "LIVE" badge displayed with animated red pulse. EventSource connection opens to `/api/logs/{ticket_id}/{stage}/stream`. Log lines appear in real-time as they arrive
**Priority:** P0

## Scenario: Log lines color-coded by type
**Preconditions:** A stage log with various event types
**Steps:**
1. Navigate to `/task/{task_id}/logs/{stage}`
2. Observe log line colors
**Expected Result:** Lines are color-coded: assistant (blue #79c0ff), tool_use (purple #d2a8ff), tool_result (gray #8b949e), result (green #3fb950 bold), system (gray italic), log (light gray #c9d1d9 fallback)
**Priority:** P1

## Scenario: Tool use lines show invocation info
**Preconditions:** A stage log with tool_use events
**Steps:**
1. Navigate to `/task/{task_id}/logs/{stage}`
2. Locate tool_use log lines
**Expected Result:** Tool use lines show tool invocation details in purple. Only tool invocations are shown (not full payloads)
**Priority:** P1

## Scenario: Tool result lines show status
**Preconditions:** A stage log with tool_result events
**Steps:**
1. Navigate to `/task/{task_id}/logs/{stage}`
2. Locate tool_result log lines
**Expected Result:** Lines show [ok] or [ERROR] prefix followed by a snippet of the result, in gray
**Priority:** P1

## Scenario: Result line shows completion summary
**Preconditions:** A completed stage with a result event in the log
**Steps:**
1. Navigate to `/task/{task_id}/logs/{stage}`
2. Locate the result log line
**Expected Result:** Line shows "Completed in X turns (Ys) [subtype]" in green bold. [ERROR] suffix if applicable
**Priority:** P1

## Scenario: Auto-scroll enabled by default for live stream
**Preconditions:** A task with an active stage streaming logs
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Observe auto-scroll behavior as new lines arrive
**Expected Result:** Terminal automatically scrolls to bottom as new content appears. Auto-scroll toggle button shows "ON" state
**Priority:** P0

## Scenario: Manual scroll disables auto-scroll
**Preconditions:** A task with an active stage streaming logs, auto-scroll is ON
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Manually scroll up in the log terminal (more than 50px from bottom)
**Expected Result:** Auto-scroll is disabled. New lines still appear but terminal stays at current scroll position. Auto-scroll button reflects "OFF" state
**Priority:** P1

## Scenario: Scrolling back to bottom re-enables auto-scroll
**Preconditions:** Auto-scroll was disabled by manual scroll
**Steps:**
1. After disabling auto-scroll, scroll back down to the bottom of the terminal
**Expected Result:** Auto-scroll is re-enabled when within 50px of the bottom
**Priority:** P1

## Scenario: Auto-scroll toggle button
**Preconditions:** A task with an active stage
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Click the auto-scroll toggle button to disable
3. Click it again to re-enable
**Expected Result:** Toggle button switches between ON/OFF states. When re-enabled, terminal scrolls to bottom immediately
**Priority:** P1

## Scenario: SSE stream done event
**Preconditions:** A task with an active stage that completes while viewing
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Wait for the stage to complete
**Expected Result:** "done" SSE event received. EventSource connection closes. "LIVE" badge swaps to "COMPLETED" (green)
**Priority:** P1

## Scenario: SSE stream error event
**Preconditions:** SSE stream encounters an error
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Simulate or wait for a stream error
**Expected Result:** Connection closes silently. No crash or infinite retry loop
**Priority:** P2

## Scenario: Log terminal styling
**Preconditions:** Any stage with log content
**Steps:**
1. Navigate to `/task/{task_id}/logs/{stage}`
2. Observe the terminal appearance
**Expected Result:** Dark background (#0d1117), light text (#c9d1d9), monospace font (SF Mono/Cascadia Code/Fira Code/Consolas), max-height 70vh with scroll, min-height 300px, rounded corners
**Priority:** P2

## Scenario: NDJSON parse error fallback
**Preconditions:** A stage log with lines that are not valid NDJSON
**Steps:**
1. Navigate to `/task/{task_id}/logs/{stage}`
2. Observe lines that fail JSON parsing
**Expected Result:** Non-JSON lines fall back to "log" type styling (light gray) instead of causing errors
**Priority:** P1

## Scenario: No log files for task
**Preconditions:** A task exists but has no log files
**Steps:**
1. Navigate to `/task/{task_id}/logs`
**Expected Result:** Page renders without error. Shows appropriate empty state or message indicating no logs available
**Priority:** P1

## Scenario: Invalid stage name
**Preconditions:** A task exists but the requested stage has no logs
**Steps:**
1. Navigate to `/task/{task_id}/logs/nonexistent_stage`
**Expected Result:** 404 response or appropriate error message
**Priority:** P2

## Scenario: Back link navigation
**Preconditions:** Task exists in database
**Steps:**
1. Navigate to `/task/{task_id}/logs`
2. Click the back link
**Expected Result:** Navigates to `/task/{task_id}` (task detail page). If no task found in DB, navigates to `/history`
**Priority:** P2

## Scenario: Very long log lines wrap correctly
**Preconditions:** A stage log with lines exceeding the terminal width
**Steps:**
1. Navigate to `/task/{task_id}/logs/{stage}`
2. Observe very long lines
**Expected Result:** Long lines wrap within the terminal (pre-wrap enabled). No horizontal overflow issues
**Priority:** P2
