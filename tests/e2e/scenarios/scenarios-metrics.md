# E2E Test Scenarios: Metrics Page (`/metrics`)

## Scenario: Overview KPI cards render correctly
**Preconditions:** Database has completed and failed tasks
**Steps:**
1. Navigate to `/metrics`
2. Observe the overview section
**Expected Result:** Cards display: Total tasks, Completed count, Failed count, Success rate (%). Values match database aggregations
**Priority:** P0

## Scenario: Averages section displays correctly
**Preconditions:** Database has completed tasks with timing and iteration data
**Steps:**
1. Navigate to `/metrics`
2. Observe the averages section
**Expected Result:** Cards display: Avg wall time (human-readable format like "2h30m"), Avg turns per task (rounded integer), Avg review iterations (rounded float)
**Priority:** P0

## Scenario: Tasks completed section — time windows
**Preconditions:** Database has tasks completed at various times
**Steps:**
1. Navigate to `/metrics`
2. Observe the tasks completed section
**Expected Result:** Cards display correct counts for: Today, Last 7 days, Last 30 days
**Priority:** P0

## Scenario: Token usage and cost section
**Preconditions:** Database has tasks with token usage and cost data
**Steps:**
1. Navigate to `/metrics`
2. Observe the token usage & cost section
**Expected Result:** Cards display: Total input tokens (with thousands separator), Total output tokens (with thousands separator), Total cost ($, 2 decimals), Avg context fill % (1 decimal), Tasks > 80% fill (count)
**Priority:** P0

## Scenario: Extra usage cost highlighted
**Preconditions:** Tasks with extra usage cost > 0
**Steps:**
1. Navigate to `/metrics`
2. Observe the token usage & cost section
**Expected Result:** Extra usage cost card is visible and highlighted in yellow
**Priority:** P1

## Scenario: Project filter — all projects (default)
**Preconditions:** Tasks exist across multiple projects
**Steps:**
1. Navigate to `/metrics` (no query params)
2. Observe the project filter dropdown
**Expected Result:** "All Projects" is selected by default. Metrics reflect all projects combined
**Priority:** P0

## Scenario: Project filter — specific project
**Preconditions:** Tasks exist across multiple projects
**Steps:**
1. Navigate to `/metrics`
2. Select a specific project from the dropdown
3. Submit the filter
**Expected Result:** URL updates to `/metrics?project={name}`. All metric values update to reflect only the selected project's data
**Priority:** P0

## Scenario: Common failure reasons table
**Preconditions:** Database has failed tasks with failure reasons
**Steps:**
1. Navigate to `/metrics`
2. Observe the failure reasons table
**Expected Result:** Table shows columns: Reason, Count. Reasons are listed with their occurrence count
**Priority:** P1

## Scenario: Long failure reason truncation
**Preconditions:** A failure reason exceeding 120 characters
**Steps:**
1. Navigate to `/metrics`
2. Locate the long failure reason in the table
**Expected Result:** Reason text is truncated to 120 characters with a collapsible details/summary element to view the full text
**Priority:** P2

## Scenario: No failure reasons
**Preconditions:** All tasks completed successfully (no failures)
**Steps:**
1. Navigate to `/metrics`
**Expected Result:** Failure reasons section is empty or shows "No failure reasons" message. No table rendered
**Priority:** P2

## Scenario: No tasks in database
**Preconditions:** Database has zero tasks
**Steps:**
1. Navigate to `/metrics`
**Expected Result:** Page renders without error. All counts show 0. Percentages show 0% or "-". Averages show appropriate default (0 or "-")
**Priority:** P1

## Scenario: All tasks failed
**Preconditions:** Database has tasks but all are failed
**Steps:**
1. Navigate to `/metrics`
**Expected Result:** Success rate shows 0%. Completed count shows 0. Failure reasons table is populated
**Priority:** P2

## Scenario: Avg context fill null handling
**Preconditions:** Tasks exist but have no context fill data
**Steps:**
1. Navigate to `/metrics`
2. Observe the avg context fill card
**Expected Result:** Shows "-" instead of "null" or error
**Priority:** P2
