# E2E Test Scenarios: Task History Page (`/history`)

## Scenario: Task table renders with correct columns
**Preconditions:** Database has multiple completed and failed tasks
**Steps:**
1. Navigate to `/history`
2. Observe the task table
**Expected Result:** Table shows columns: Ticket ID, Title, Project, Status, Wall time, Turns, Reviews, Comments, Limit hits, Cost, Max context fill %, Started, Completed, PR link. Data matches database records
**Priority:** P0

## Scenario: Status column is color-coded
**Preconditions:** Tasks exist with statuses: completed, failed, in_progress, pending, needs_human, interrupted
**Steps:**
1. Navigate to `/history`
2. Observe the Status column
**Expected Result:** Each status type has a distinct color (e.g. completed=green, failed=red, in_progress=yellow)
**Priority:** P1

## Scenario: Search filter by ticket ID
**Preconditions:** Database has tasks with known ticket IDs
**Steps:**
1. Navigate to `/history`
2. Enter a known ticket ID in the search field
3. Submit the filter
**Expected Result:** Table filters to show only tasks matching the ticket ID. Result count updates. Pagination adjusts
**Priority:** P0

## Scenario: Search filter by title
**Preconditions:** Database has tasks with known titles
**Steps:**
1. Navigate to `/history`
2. Enter a partial title string in the search field
3. Submit the filter
**Expected Result:** Table filters to show tasks with matching titles
**Priority:** P0

## Scenario: Project filter dropdown
**Preconditions:** Tasks exist across multiple projects
**Steps:**
1. Navigate to `/history`
2. Select a specific project from the project dropdown
3. Submit the filter
**Expected Result:** Table shows only tasks from the selected project. Result count updates
**Priority:** P0

## Scenario: Status filter dropdown
**Preconditions:** Tasks exist with various statuses
**Steps:**
1. Navigate to `/history`
2. Select a specific status from the status dropdown
3. Submit the filter
**Expected Result:** Table shows only tasks with the selected status
**Priority:** P0

## Scenario: Combined filters
**Preconditions:** Tasks exist across projects with various statuses
**Steps:**
1. Navigate to `/history`
2. Select a project filter
3. Select a status filter
4. Enter a search term
5. Submit
**Expected Result:** Table shows only tasks matching all three filter criteria simultaneously
**Priority:** P1

## Scenario: Column sorting — default sort
**Preconditions:** Database has multiple tasks
**Steps:**
1. Navigate to `/history`
**Expected Result:** Tasks are sorted by created_at DESC (newest first) by default. Sort indicator (↓) shown on the active column
**Priority:** P0

## Scenario: Column sorting — toggle sort direction
**Preconditions:** Database has multiple tasks
**Steps:**
1. Navigate to `/history`
2. Click on a sortable column header (e.g. "Turns")
3. Click the same column header again
**Expected Result:** First click sorts by that column DESC (↓). Second click toggles to ASC (↑). Data reorders accordingly
**Priority:** P0

## Scenario: Column sorting — sortable columns
**Preconditions:** Database has multiple tasks
**Steps:**
1. Navigate to `/history`
2. Click on each sortable column: ticket_id, title, project, status, turns, review_iterations, limit_interruptions, created_at, started_at, completed_at
**Expected Result:** Each column sorts correctly. Sort direction indicator updates
**Priority:** P1

## Scenario: Pagination — navigate pages
**Preconditions:** Database has enough tasks to span multiple pages
**Steps:**
1. Navigate to `/history`
2. Observe the pagination controls
3. Click "Next" to go to page 2
4. Click "Previous" to go back to page 1
5. Click a specific page number
**Expected Result:** Each navigation action loads the correct page. Result count shows "X tasks found - page Y of Z". Previous/Next buttons disabled at boundaries
**Priority:** P0

## Scenario: Pagination preserves filters and sort
**Preconditions:** Database has many tasks, filters are applied
**Steps:**
1. Navigate to `/history`
2. Apply a project filter and sort by turns
3. Navigate to page 2
**Expected Result:** Page 2 still shows the same project filter and sort order. Query params include all filter/sort/page state
**Priority:** P1

## Scenario: Row click navigates to task detail
**Preconditions:** Database has at least one task
**Steps:**
1. Navigate to `/history`
2. Click on a task row (not on a link)
**Expected Result:** Browser navigates to `/task/{ticket_id}` for that task
**Priority:** P0

## Scenario: Linear ticket link opens in new tab
**Preconditions:** Task has a ticket ID that links to Linear
**Steps:**
1. Navigate to `/history`
2. Click on a ticket ID link in the table
**Expected Result:** Linear issue opens in a new browser tab. Row onclick does NOT also trigger (event.stopPropagation)
**Priority:** P1

## Scenario: PR link opens in new tab
**Preconditions:** Task has an associated PR URL
**Steps:**
1. Navigate to `/history`
2. Click on a PR link in the table
**Expected Result:** PR opens in a new browser tab
**Priority:** P1

## Scenario: Long title truncation
**Preconditions:** A task exists with a title longer than 50 characters
**Steps:**
1. Navigate to `/history`
2. Locate the task with the long title
**Expected Result:** Title is truncated to 50 characters with "..." appended
**Priority:** P2

## Scenario: No tasks found
**Preconditions:** Database has no tasks, or filter returns zero results
**Steps:**
1. Navigate to `/history`
**Expected Result:** Page renders without error. Shows "0 tasks found" or empty state message. No table rows displayed
**Priority:** P1

## Scenario: Missing optional fields display gracefully
**Preconditions:** Tasks with null/missing values for turns, reviews, cost, context fill, PR URL
**Steps:**
1. Navigate to `/history`
2. Observe columns with missing data
**Expected Result:** Missing fields show "-" or blank, not errors or "null"
**Priority:** P1

## Scenario: Timestamps display as time-ago
**Preconditions:** Tasks with various started/completed timestamps
**Steps:**
1. Navigate to `/history`
2. Observe the Started and Completed columns
**Expected Result:** Timestamps display as relative time (e.g. "2h ago", "3d ago") via data-timestamp + timeago formatting
**Priority:** P2

## Scenario: Page param exceeds total pages
**Preconditions:** Database has tasks spanning 3 pages
**Steps:**
1. Navigate to `/history?page=999`
**Expected Result:** Page param is clamped to the last valid page. No error displayed
**Priority:** P2
