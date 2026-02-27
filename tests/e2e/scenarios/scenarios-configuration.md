# E2E Test Scenarios: Configuration Page (`/config`)

## Scenario: View tab shows config sections
**Preconditions:** Valid configuration is loaded
**Steps:**
1. Navigate to `/config`
2. Observe the View tab (default active)
**Expected Result:** Read-only config displayed in sections: Projects, Linear, Agents, Usage Limits, Notifications, Dashboard, Database. All values rendered correctly
**Priority:** P0

## Scenario: View tab — projects section
**Preconditions:** Config has at least one project configured
**Steps:**
1. Navigate to `/config`
2. Observe the Projects section in View tab
**Expected Result:** Card per project showing: Name, Linear team, Linear project (or "-"), base_dir (code style), worktree_prefix (code style), slots (tag list)
**Priority:** P1

## Scenario: View tab — API keys masked
**Preconditions:** Config has Linear API key
**Steps:**
1. Navigate to `/config`
2. Observe the Linear section in View tab
**Expected Result:** API key is masked (not shown in plaintext). Other Linear fields visible: workspace, poll interval, exclude tags, statuses, comment flags (shown as yes/no)
**Priority:** P0

## Scenario: View tab — agents section
**Preconditions:** Config has agent settings
**Steps:**
1. Navigate to `/config`
2. Observe the Agents section
**Expected Result:** Shows: Max review iterations, max CI retries, timeout grace (seconds), timeout per stage (minutes for implement, review, fix, pr_checks)
**Priority:** P1

## Scenario: View tab — usage limits section
**Preconditions:** Config has usage limit settings
**Steps:**
1. Navigate to `/config`
2. Observe the Usage Limits section
**Expected Result:** Shows: Enabled (boolean), 5-hour threshold, 7-day threshold
**Priority:** P1

## Scenario: Tab switching — View to Edit
**Preconditions:** Valid configuration loaded
**Steps:**
1. Navigate to `/config`
2. Click the "Edit" tab button
**Expected Result:** View tab content hides. Edit tab content shows. Edit tab button becomes active (highlighted). JavaScript toggle, no page reload
**Priority:** P0

## Scenario: Tab switching — Edit to View
**Preconditions:** Edit tab is active
**Steps:**
1. Navigate to `/config` and switch to Edit tab
2. Click the "View" tab button
**Expected Result:** Edit tab content hides. View tab content shows. View tab button becomes active
**Priority:** P0

## Scenario: Edit tab — Linear immediate settings
**Preconditions:** Edit tab is active
**Steps:**
1. Switch to Edit tab
2. Observe the Linear section
**Expected Result:** Editable fields: Poll interval (number input), Checkboxes for comment_on_failure, comment_on_completion, comment_on_limit_pause
**Priority:** P0

## Scenario: Edit tab — Usage limits settings
**Preconditions:** Edit tab is active
**Steps:**
1. Switch to Edit tab
2. Observe the Usage Limits section
**Expected Result:** Editable fields: Enable toggle, 5-hour threshold (0-1, step 0.01), 7-day threshold (0-1, step 0.01)
**Priority:** P0

## Scenario: Edit tab — Agents settings
**Preconditions:** Edit tab is active
**Steps:**
1. Switch to Edit tab
2. Observe the Agents section
**Expected Result:** Editable fields: Max review iterations (number), Max CI retries (number), Timeout grace (seconds), Timeout per stage for implement, review, fix, pr_checks (minutes)
**Priority:** P0

## Scenario: Edit tab — Projects structural settings
**Preconditions:** Edit tab is active, projects configured
**Steps:**
1. Switch to Edit tab
2. Observe the Projects section
**Expected Result:** Per-project cards with: Linear project filter (text input), Slot chips with x buttons to remove, Add slot input + button. Save button at bottom
**Priority:** P0

## Scenario: Successful config save — immediate fields
**Preconditions:** Edit tab is active
**Steps:**
1. Modify an immediate field (e.g. change poll interval)
2. Click save
**Expected Result:** POST to `/config` with JSON body. Success feedback (green message) appears. Message auto-hides after 3 seconds. Config is applied immediately
**Priority:** P0

## Scenario: Config save validation error
**Preconditions:** Edit tab is active
**Steps:**
1. Enter an invalid value (e.g. negative poll interval or out-of-range threshold)
2. Click save
**Expected Result:** POST returns 422 with error list. Error feedback (red message) displays with validation details
**Priority:** P0

## Scenario: Structural changes show restart banner
**Preconditions:** Edit tab is active
**Steps:**
1. Make a structural change (e.g. modify project slots or notifications)
2. Click save
**Expected Result:** Yellow restart banner appears indicating structural changes require a restart. Changes are applied in-memory but full effect requires restart
**Priority:** P0

## Scenario: Slot chip removal
**Preconditions:** Edit tab is active, project has multiple slots
**Steps:**
1. Switch to Edit tab
2. Click the x button on a slot chip
**Expected Result:** Slot chip is removed from the UI. Change is not persisted until save is clicked
**Priority:** P1

## Scenario: Add slot to project
**Preconditions:** Edit tab is active
**Steps:**
1. Switch to Edit tab
2. Enter a slot number in the add slot input
3. Click "Add" button
**Expected Result:** New slot chip appears in the project's slot list. Validated before adding
**Priority:** P1

## Scenario: Duplicate slot number rejected
**Preconditions:** Edit tab is active, project already has slot 1
**Steps:**
1. Enter "1" in the add slot input for a project that already has slot 1
2. Click "Add"
**Expected Result:** Duplicate is rejected. Appropriate feedback shown
**Priority:** P2

## Scenario: Edit tab — Notifications structural settings
**Preconditions:** Edit tab is active
**Steps:**
1. Observe the Notifications section
**Expected Result:** Editable fields: Webhook URL (text input), Format (dropdown: slack, discord), Rate limit (number)
**Priority:** P1

## Scenario: Save button shows loading state
**Preconditions:** Edit tab is active
**Steps:**
1. Click save button
2. Observe button during request
**Expected Result:** Button shows aria-busy state during the POST request. Returns to normal after response
**Priority:** P2

## Scenario: Config save network error
**Preconditions:** Network is unreachable or server error occurs
**Steps:**
1. Modify a config field
2. Click save (with simulated network failure)
**Expected Result:** Error feedback displayed. Config not modified. No crash
**Priority:** P2

## Scenario: Feedback messages auto-hide
**Preconditions:** A config save operation has been performed
**Steps:**
1. Save a config change (success or error)
2. Observe the feedback message
3. Wait 3 seconds
**Expected Result:** Feedback message automatically disappears after 3 seconds
**Priority:** P2

## Scenario: No config object
**Preconditions:** Config could not be loaded (edge case)
**Steps:**
1. Navigate to `/config` when no config is available
**Expected Result:** Page is disabled or shows appropriate error state. No crash
**Priority:** P2
