# E2E Test Scenarios: Usage Trends Page (`/usage`)

## Scenario: Page renders with current usage panel
**Preconditions:** Usage data is available
**Steps:**
1. Navigate to `/usage`
2. Observe the current usage section
**Expected Result:** Current usage panel displays (same as Live Status page usage partial) with 5-hour and 7-day utilization bars and reset timers
**Priority:** P0

## Scenario: Default time range is 7 days
**Preconditions:** Usage snapshots exist for the past 7+ days
**Steps:**
1. Navigate to `/usage` (no query params)
2. Observe the time range buttons
**Expected Result:** "Last 7d" button is highlighted as current (aria-current="true"). Charts show 7 days of data
**Priority:** P0

## Scenario: Time range button — 24 hours
**Preconditions:** Usage snapshots exist for the past 24+ hours
**Steps:**
1. Navigate to `/usage`
2. Click "Last 24h" button
**Expected Result:** URL updates to `/usage?range=24h`. Button state changes to current. Charts update to show only last 24 hours of data
**Priority:** P0

## Scenario: Time range button — 7 days
**Preconditions:** Usage snapshots exist for the past 7+ days
**Steps:**
1. Navigate to `/usage`
2. Click "Last 7d" button
**Expected Result:** URL updates to `/usage?range=7d`. Charts show 7 days of data
**Priority:** P0

## Scenario: Time range button — 30 days
**Preconditions:** Usage snapshots exist for the past 30+ days
**Steps:**
1. Navigate to `/usage`
2. Click "Last 30d" button
**Expected Result:** URL updates to `/usage?range=30d`. Charts show 30 days of data
**Priority:** P0

## Scenario: Usage chart renders correctly
**Preconditions:** Usage snapshots exist for the selected range
**Steps:**
1. Navigate to `/usage`
2. Observe the usage chart
**Expected Result:** Chart.js line chart renders with: X-axis showing timestamps (MM-DD HH:MM), Y-axis 0-100%. Two datasets: "5-hour %" (blue line with light blue fill) and "7-day %" (orange line with light orange fill). Points at each snapshot, tension 0.3. Chart is responsive
**Priority:** P0

## Scenario: Usage chart tooltip
**Preconditions:** Usage chart is rendered with data points
**Steps:**
1. Navigate to `/usage`
2. Hover over a data point on the usage chart
**Expected Result:** Tooltip shows the label (timestamp) and percentage value for the hovered dataset
**Priority:** P2

## Scenario: Extra usage chart renders when enabled
**Preconditions:** Extra usage is enabled and has snapshot data
**Steps:**
1. Navigate to `/usage`
2. Observe the extra usage chart
**Expected Result:** Second chart renders with: "Extra Usage ($)" as red line with light red fill, "Monthly Limit ($)" as gray dashed line (no fill). Y-axis in USD. Tooltip shows $X.XX format
**Priority:** P1

## Scenario: Extra usage chart hidden when not enabled
**Preconditions:** Extra usage is not enabled in configuration
**Steps:**
1. Navigate to `/usage`
**Expected Result:** Only the main usage chart is displayed. No extra usage chart section
**Priority:** P1

## Scenario: Raw snapshot data table — expandable
**Preconditions:** Usage snapshots exist
**Steps:**
1. Navigate to `/usage`
2. Click to expand the raw snapshot data section
**Expected Result:** Table shows columns: Timestamp, 5-hour (%), 7-day (%), 5h Resets At, 7d Resets At. Rows are in reverse chronological order (newest first). Timestamps use data-timestamp for timeago formatting
**Priority:** P1

## Scenario: Raw data table with extra usage columns
**Preconditions:** Extra usage is enabled and has data
**Steps:**
1. Navigate to `/usage`
2. Expand raw data table
**Expected Result:** Additional columns visible: Extra Usage ($), Extra Limit ($)
**Priority:** P1

## Scenario: No snapshots for selected range
**Preconditions:** No usage snapshots exist in the selected time range
**Steps:**
1. Navigate to `/usage?range=24h` when no data exists for last 24h
**Expected Result:** Page renders without error. Charts show empty/no data state. Raw data table is empty or shows appropriate message
**Priority:** P1

## Scenario: Single snapshot data point
**Preconditions:** Only one usage snapshot exists in the selected range
**Steps:**
1. Navigate to `/usage`
**Expected Result:** Chart renders with a single point. No errors from Chart.js
**Priority:** P2

## Scenario: Charts are responsive
**Preconditions:** Usage data exists
**Steps:**
1. Navigate to `/usage`
2. Resize browser window to various widths
**Expected Result:** Charts resize responsively to fit their container. Labels remain readable
**Priority:** P2
