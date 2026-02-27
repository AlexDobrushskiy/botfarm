# E2E Test Scenarios: Real-Time Updates (htmx & SSE)

## htmx Polling

### Scenario: Live Status slots refresh every 5 seconds
**Preconditions:** Application is running with active slots
**Steps:**
1. Navigate to `/`
2. Observe network requests over 15 seconds
**Expected Result:** GET `/partials/slots` fires every 5 seconds. Slot table content updates in-place without full page reload
**Priority:** P0

### Scenario: Live Status usage refresh every 5 seconds
**Preconditions:** Application is running with usage tracking enabled
**Steps:**
1. Navigate to `/`
2. Observe network requests over 15 seconds
**Expected Result:** GET `/partials/usage` fires every 5 seconds. Usage bars update in-place
**Priority:** P0

### Scenario: Live Status queue refresh every 5 seconds
**Preconditions:** Application is running with queue entries
**Steps:**
1. Navigate to `/`
2. Observe network requests over 15 seconds
**Expected Result:** GET `/partials/queue` fires every 5 seconds. Queue content updates in-place
**Priority:** P0

### Scenario: Supervisor badge refresh every 5 seconds
**Preconditions:** Application is running
**Steps:**
1. Navigate to any page
2. Observe network requests over 15 seconds
**Expected Result:** GET `/partials/supervisor-badge` fires every 5 seconds. Badge updates reflect current supervisor state
**Priority:** P1

### Scenario: Task History refresh every 10 seconds
**Preconditions:** Application is running with tasks in the database
**Steps:**
1. Navigate to `/history`
2. Observe network requests over 30 seconds
**Expected Result:** GET `/partials/history` fires every 10 seconds with current filter/sort/page params. Table content refreshes in-place. Filter state is preserved across refreshes
**Priority:** P0

### Scenario: Update banner check every 60 seconds
**Preconditions:** Application is running
**Steps:**
1. Navigate to `/`
2. Observe network requests over 2 minutes
**Expected Result:** GET `/partials/update-banner` fires periodically (~60s). Banner updates if commits-behind changes
**Priority:** P2

### Scenario: htmx refresh does not cause full page reload
**Preconditions:** Application is running on Live Status page
**Steps:**
1. Navigate to `/`
2. Interact with an element (e.g. scroll position, open a dropdown)
3. Wait for htmx polling cycle
**Expected Result:** Page does not fully reload. Only targeted partials update. Scroll position, focus, and other page state are preserved
**Priority:** P0

### Scenario: htmx polling preserves filter state on Task History
**Preconditions:** Filters and sort applied on Task History
**Steps:**
1. Navigate to `/history`
2. Apply a project filter and sort by cost DESC
3. Wait for 10-second htmx refresh
**Expected Result:** After refresh, the same filters and sort are applied. Query params in the htmx request include the current filter/sort state
**Priority:** P1

### Scenario: Pause/Resume via htmx POST
**Preconditions:** Supervisor is running
**Steps:**
1. Navigate to `/`
2. Click Pause button
3. Observe the network request
**Expected Result:** htmx sends POST to `/api/pause`. hx-confirm triggers confirmation dialog before sending. On success, supervisor controls partial updates to show Resume button
**Priority:** P0

## SSE Streaming

### Scenario: Log viewer establishes SSE connection for active stage
**Preconditions:** A task is in progress with an active stage
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Observe network connections
**Expected Result:** EventSource connection opens to `/api/logs/{ticket_id}/{stage}/stream`. Connection is established and kept alive
**Priority:** P0

### Scenario: SSE events render as log lines in real-time
**Preconditions:** SSE connection is established for an active stage
**Steps:**
1. Navigate to active stage log viewer
2. Observe as the agent produces output
**Expected Result:** Each SSE event (assistant, tool_use, tool_result, result, system, log) appends a new `<span class="log-line log-line-{type}">` element to the terminal. Lines appear immediately as events arrive
**Priority:** P0

### Scenario: SSE done event transitions to completed state
**Preconditions:** Watching an active stage that completes
**Steps:**
1. Navigate to `/task/{task_id}/logs/{active_stage}`
2. Wait for the stage to complete
**Expected Result:** "done" SSE event received. EventSource connection closes cleanly. "LIVE" badge transitions to "COMPLETED" (green). No more events expected
**Priority:** P0

### Scenario: SSE error event handles gracefully
**Preconditions:** SSE connection is established
**Steps:**
1. Navigate to active stage log viewer
2. Simulate stream error (e.g. server restart)
**Expected Result:** "error" SSE event or connection drop handled gracefully. Connection closes silently. No infinite retry loop. No JavaScript errors
**Priority:** P1

### Scenario: SSE does not connect for completed stages
**Preconditions:** A task with a completed stage
**Steps:**
1. Navigate to `/task/{task_id}/logs/{completed_stage}`
2. Observe network connections
**Expected Result:** No EventSource/SSE connection is opened. Content is loaded statically from `/api/logs/{ticket_id}/{stage}/content`. "COMPLETED" badge shown (not "LIVE")
**Priority:** P1

### Scenario: Auto-scroll during SSE streaming
**Preconditions:** SSE stream is actively producing content
**Steps:**
1. Navigate to active stage log viewer
2. Let content stream in for several seconds
**Expected Result:** Terminal auto-scrolls to bottom as new lines arrive. User sees the latest output without manual scrolling
**Priority:** P0

### Scenario: Manual scroll pauses auto-scroll during SSE
**Preconditions:** SSE stream is actively producing content, auto-scroll is ON
**Steps:**
1. Navigate to active stage log viewer
2. Manually scroll up more than 50px from the bottom
**Expected Result:** Auto-scroll is disabled. New SSE events still append to the terminal, but viewport stays at user's scroll position. Auto-scroll button shows OFF state
**Priority:** P1

## Countdown Timers

### Scenario: Resume countdown updates in real-time
**Preconditions:** A slot is in paused_limit state with resume time
**Steps:**
1. Navigate to `/`
2. Observe the paused_limit slot's countdown timer
3. Wait 10+ seconds
**Expected Result:** Countdown timer decrements every ~10 seconds. Shows remaining time until auto-resume
**Priority:** P1

### Scenario: Reset countdown updates in real-time
**Preconditions:** Usage data has reset timestamps
**Steps:**
1. Navigate to `/`
2. Observe the usage reset countdown timers
3. Wait 10+ seconds
**Expected Result:** Reset countdown timers decrement every ~10 seconds
**Priority:** P2

### Scenario: Time-ago timestamps update periodically
**Preconditions:** Page has data-timestamp elements
**Steps:**
1. Navigate to any page with timestamps (e.g. `/history`)
2. Note the time-ago values
3. Wait 60+ seconds
**Expected Result:** Time-ago values update every 60 seconds (e.g. "2m ago" becomes "3m ago")
**Priority:** P2
