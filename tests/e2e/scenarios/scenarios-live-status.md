# E2E Test Scenarios: Live Status Page (`/`)

## Scenario: Slots table renders with correct data
**Preconditions:** Database has tasks in various states (busy, free, paused_limit, failed, completed_pending_cleanup)
**Steps:**
1. Navigate to `/`
2. Observe the slots table
3. Verify each row shows: Project, Slot ID, Status, Ticket ID/Title, Pipeline progress, Context fill %, Elapsed time
**Expected Result:** All slots render with correct data; status badges use appropriate color coding (e.g. busy=active, free=neutral, failed=red)
**Priority:** P0

## Scenario: Slot status color coding
**Preconditions:** Slots exist with statuses: busy, free, paused_limit, paused_manual, failed, completed_pending_cleanup
**Steps:**
1. Navigate to `/`
2. Check each slot's status badge color
**Expected Result:** Each status type has a distinct, visually identifiable color/style
**Priority:** P1

## Scenario: Context fill percentage color coding
**Preconditions:** Slots with context fill values at various thresholds: <50%, 50-74%, 75-89%, 90-94%, 95%+
**Steps:**
1. Navigate to `/`
2. Check the context fill column for each slot
**Expected Result:** Colors match thresholds — green (<50%), yellow (<75%), orange (<90%), red (>=90%)
**Priority:** P1

## Scenario: Pipeline progress stepper displays correctly
**Preconditions:** A busy slot with an active task showing completed and pending stages
**Steps:**
1. Navigate to `/`
2. Locate the busy slot row
3. Observe the pipeline progress column
**Expected Result:** Completed stages show checkmark (green), active stage pulses (yellow), pending stages are gray outlines. Connectors between nodes are colored to match state
**Priority:** P1

## Scenario: Usage bars show percentages
**Preconditions:** Usage data is available (5-hour and 7-day utilization tracked)
**Steps:**
1. Navigate to `/`
2. Observe the usage panel
**Expected Result:** Both 5-hour and 7-day utilization show percentage values with progress bars. Reset timers display countdown. Colors reflect thresholds: green (<70%), yellow (<85%), red (>=85%)
**Priority:** P0

## Scenario: Extra usage status displays when enabled
**Preconditions:** Extra usage is enabled in config with available data
**Steps:**
1. Navigate to `/`
2. Observe the usage panel
**Expected Result:** Extra usage section shows dollar amount and percentage of monthly limit
**Priority:** P1

## Scenario: Queue shows grouped entries with priority badges
**Preconditions:** Queue has entries across multiple projects with varying priorities (Urgent, High, Normal, Low)
**Steps:**
1. Navigate to `/`
2. Observe the queue section
**Expected Result:** Entries are grouped by project. Each entry shows priority badge, ticket ID/title as Linear link, and "Next" highlighting for the first unblocked item per project. Last polled timestamp is displayed
**Priority:** P0

## Scenario: Queue shows blocked entries
**Preconditions:** Queue has entries where some are blocked by other tickets
**Steps:**
1. Navigate to `/`
2. Locate a blocked queue entry
**Expected Result:** Blocked entries show a blocked indicator with blocking ticket IDs listed
**Priority:** P1

## Scenario: Supervisor badge shows running state
**Preconditions:** Supervisor is running (heartbeat within poll_interval + 60s grace period)
**Steps:**
1. Navigate to `/`
2. Observe the supervisor badge in the nav bar
**Expected Result:** Badge shows green dot with "running" indicator
**Priority:** P0

## Scenario: Supervisor badge shows stopped state
**Preconditions:** Supervisor heartbeat is stale (>poll_interval + 60s)
**Steps:**
1. Navigate to `/`
2. Observe the supervisor badge
**Expected Result:** Badge shows red dot with "stopped" indicator
**Priority:** P0

## Scenario: Pause button works
**Preconditions:** Supervisor is running, dispatch is active
**Steps:**
1. Navigate to `/`
2. Click the "Pause" button
3. Confirm the action in the confirmation dialog
**Expected Result:** POST to `/api/pause` is sent. Supervisor status updates to paused. Button state changes accordingly
**Priority:** P0

## Scenario: Resume button works
**Preconditions:** Supervisor is paused
**Steps:**
1. Navigate to `/`
2. Click the "Resume" button
3. Confirm the action
**Expected Result:** POST to `/api/resume` is sent. Supervisor resumes dispatching. Button state changes accordingly
**Priority:** P0

## Scenario: Slot row links to task detail
**Preconditions:** A slot has an active or recent task
**Steps:**
1. Navigate to `/`
2. Click on a slot row with a task
**Expected Result:** Navigates to `/task/{task_id}` for the associated task
**Priority:** P1

## Scenario: Resume countdown timer for paused_limit slots
**Preconditions:** A slot is in paused_limit state with a scheduled resume time
**Steps:**
1. Navigate to `/`
2. Observe the paused_limit slot row
**Expected Result:** A countdown timer shows time remaining until auto-resume. Timer updates periodically (every ~10s)
**Priority:** P1

## Scenario: Empty slots table
**Preconditions:** No slots configured in config
**Steps:**
1. Navigate to `/`
**Expected Result:** Page renders without error. Slots area shows empty state or no rows
**Priority:** P2

## Scenario: Empty queue
**Preconditions:** No tickets in the queue
**Steps:**
1. Navigate to `/`
**Expected Result:** Queue section renders without error. Shows empty state or "no items" message
**Priority:** P2

## Scenario: Usage data stale indicator
**Preconditions:** Usage data is older than 120 seconds
**Steps:**
1. Navigate to `/`
2. Observe the usage panel
**Expected Result:** Stale data is indicated (visual cue that data may not be current)
**Priority:** P2

## Scenario: Update banner shows commits behind
**Preconditions:** Local installation is behind main branch by N commits
**Steps:**
1. Navigate to `/`
2. Observe the update banner area
**Expected Result:** Banner displays showing number of commits behind with an "Update & Restart" button
**Priority:** P2

## Scenario: Update & Restart button with confirmation
**Preconditions:** Update banner is visible showing commits behind
**Steps:**
1. Navigate to `/`
2. Click "Update & Restart" button
3. Observe confirmation dialog
4. Confirm the action
**Expected Result:** Confirmation dialog appears before action proceeds. After confirmation, update process initiates
**Priority:** P2
