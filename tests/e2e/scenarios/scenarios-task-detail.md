# E2E Test Scenarios: Task Detail Page (`/task/{id}`)

## Scenario: Page renders with summary cards
**Preconditions:** A completed task exists with full data
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the summary cards grid
**Expected Result:** Cards display: Status, Wall time, Turns, Review iterations, Limit interruptions, Total cost ($, 4 decimals), Max context fill %. Status card is color-coded
**Priority:** P0

## Scenario: Header shows ticket info with Linear link
**Preconditions:** A task exists with a known ticket ID
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the header area
**Expected Result:** Ticket ID is displayed as a clickable link that opens the Linear issue in a new tab. Title is displayed next to it
**Priority:** P0

## Scenario: Pipeline stepper — completed task
**Preconditions:** A completed task with stages: implement (done), review (done), fix (done), pr_checks (done), merge (done)
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the pipeline stepper
**Expected Result:** All 5 stage nodes show green checkmark (✓). Connectors between nodes are green. Stages appear in canonical order: implement → review → fix → pr_checks → merge
**Priority:** P0

## Scenario: Pipeline stepper — failed task
**Preconditions:** A failed task where the review stage failed
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the pipeline stepper
**Expected Result:** Completed stages show green ✓, failed stage shows red ✗, subsequent stages show gray outline (pending)
**Priority:** P0

## Scenario: Pipeline stepper — in-progress task
**Preconditions:** A task currently in progress at the implement stage
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the pipeline stepper
**Expected Result:** Active stage shows pulsing yellow animation. Pending stages are gray outlines. No completed stages yet (or some completed if further along)
**Priority:** P0

## Scenario: Pipeline stepper — iteration badges
**Preconditions:** A task where the review stage ran 3 times (multiple iterations)
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the pipeline stepper at the review node
**Expected Result:** Review node displays an iteration badge (e.g. "×3") indicating it ran multiple times
**Priority:** P1

## Scenario: Pipeline stepper — limit restart badge
**Preconditions:** A task where a stage was interrupted by a usage limit and restarted
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the pipeline stepper
**Expected Result:** The limit-restarted stage shows a "limit" badge
**Priority:** P1

## Scenario: Extra usage cost card highlighted
**Preconditions:** A task with extra usage cost > 0
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the summary cards
**Expected Result:** Extra usage cost card is visible and highlighted in yellow
**Priority:** P1

## Scenario: Task started on extra usage warning
**Preconditions:** A task that was started while extra usage was active
**Steps:**
1. Navigate to `/task/{task_id}`
**Expected Result:** A warning note is displayed indicating the task started on extra usage
**Priority:** P2

## Scenario: Live output link for in-progress tasks
**Preconditions:** A task currently in progress
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the live output section
**Expected Result:** A link with "LIVE" badge is displayed, pointing to `/task/{id}/logs`
**Priority:** P1

## Scenario: No live output link for completed tasks
**Preconditions:** A completed task
**Steps:**
1. Navigate to `/task/{task_id}`
**Expected Result:** No "LIVE" badge or live output link is shown
**Priority:** P2

## Scenario: Task info details section — expandable
**Preconditions:** A task with full metadata
**Steps:**
1. Navigate to `/task/{task_id}`
2. Click to expand the Task Info details section
**Expected Result:** Expanded section shows: Project, Slot, Created/Started/Completed timestamps, Failure reason (if present, in red), Review comments, PR URL link
**Priority:** P1

## Scenario: Stage runs table — expandable
**Preconditions:** A task with multiple stage runs
**Steps:**
1. Navigate to `/task/{task_id}`
2. Click to expand the Stage Runs table
**Expected Result:** Table shows columns: Stage, Iteration, Duration, Turns, Context Fill %, Cost, Extra Usage ($), Input tokens, Output tokens, Session ID (truncated), Exit subtype, Limit restart (Yes/No), Log link
**Priority:** P1

## Scenario: Stage runs — log link navigation
**Preconditions:** A task with stage runs that have log files
**Steps:**
1. Navigate to `/task/{task_id}`
2. Expand Stage Runs table
3. Click a log link
**Expected Result:** Navigates to `/task/{id}/logs/{stage}` for that specific stage
**Priority:** P1

## Scenario: Event log table — chronological
**Preconditions:** A task with multiple events
**Steps:**
1. Navigate to `/task/{task_id}`
2. Click to expand the Event Log table
**Expected Result:** Events are listed chronologically. Each row shows: Timestamp, Event type, Detail message. Maximum 500 events displayed
**Priority:** P1

## Scenario: Context fill percentage color coding
**Preconditions:** A task with max context fill at various thresholds
**Steps:**
1. Navigate to `/task/{task_id}`
2. Observe the context fill card
**Expected Result:** Color matches threshold — green (<50%), yellow (<75%), orange (<90%), red (>=90%)
**Priority:** P1

## Scenario: Back link returns to history
**Preconditions:** Any task exists
**Steps:**
1. Navigate to `/task/{task_id}`
2. Click the back link
**Expected Result:** Navigates to `/history`
**Priority:** P1

## Scenario: Task not found
**Preconditions:** No task with the given ID exists
**Steps:**
1. Navigate to `/task/nonexistent-id`
**Expected Result:** 404 response or appropriate error message displayed
**Priority:** P1

## Scenario: Polymorphic ID lookup
**Preconditions:** A task exists with both a numeric ID and a ticket ID (e.g. SMA-123)
**Steps:**
1. Navigate to `/task/{numeric_id}`
2. Navigate to `/task/{ticket_id}`
**Expected Result:** Both URLs resolve to the same task detail page
**Priority:** P2

## Scenario: Session ID truncation
**Preconditions:** A task with stage runs having session IDs longer than 12 characters
**Steps:**
1. Navigate to `/task/{task_id}`
2. Expand Stage Runs table
3. Observe Session ID column
**Expected Result:** Session IDs are truncated to first 12 characters with "..." appended
**Priority:** P2

## Scenario: Long failure reason collapsible
**Preconditions:** A failed task with a very long failure reason string
**Steps:**
1. Navigate to `/task/{task_id}`
2. Expand Task Info section
**Expected Result:** Long failure reason is wrapped in a collapsible details/summary element
**Priority:** P2

## Scenario: Pipeline stepper responsive on mobile
**Preconditions:** A task with pipeline stages
**Steps:**
1. Navigate to `/task/{task_id}` on a viewport width < 480px
2. Observe the pipeline stepper
**Expected Result:** Pipeline nodes shrink (1.8rem), connectors adjust, font sizes reduce. All stages remain visible and legible
**Priority:** P2
