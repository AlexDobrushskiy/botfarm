# E2E Test Scenarios: Identities Page (`/identities`)

## Scenario: Page renders with View tab active
**Preconditions:** Valid identity configuration is loaded
**Steps:**
1. Navigate to `/identities`
2. Observe the page layout
**Expected Result:** Page title "Identities" displayed. Two tabs: View (active) and Edit. View tab shows Coder Identity and Reviewer Identity cards
**Priority:** P0

## Scenario: View tab — Coder Identity card
**Preconditions:** Coder identity is fully configured (GitHub token, SSH key, Git author, Linear API key)
**Steps:**
1. Navigate to `/identities`
2. Observe the Coder Identity card
**Expected Result:** Card shows rows for: GitHub Token (masked with "Set" badge), SSH Key Path (path shown with "File exists" badge), Git Author Name, Git Author Email, Linear API Key (masked with "Set" badge)
**Priority:** P0

## Scenario: View tab — Reviewer Identity card
**Preconditions:** Reviewer identity is configured
**Steps:**
1. Navigate to `/identities`
2. Observe the Reviewer Identity card
**Expected Result:** Card shows rows for: GitHub Token (masked with "Set" badge), Linear API Key (masked with "Set" badge)
**Priority:** P0

## Scenario: View tab — credential "Set" badge (green)
**Preconditions:** A secret field (GitHub token, Linear API key) has a value set
**Steps:**
1. Navigate to `/identities`
2. Observe the credential row
**Expected Result:** Masked value displayed in code style. Green "Set" badge appears next to it
**Priority:** P1

## Scenario: View tab — credential "Not set" badge (red)
**Preconditions:** A secret field is not configured
**Steps:**
1. Navigate to `/identities`
2. Observe the unset credential row
**Expected Result:** Red "Not set" badge displayed. No masked value shown
**Priority:** P1

## Scenario: View tab — SSH key "File exists" badge
**Preconditions:** Coder SSH key path is set and file exists on disk
**Steps:**
1. Navigate to `/identities`
2. Observe the SSH Key Path row
**Expected Result:** Path displayed in code style. Green "File exists" badge shown
**Priority:** P1

## Scenario: View tab — SSH key "File not found" warning badge
**Preconditions:** Coder SSH key path is set but file does not exist
**Steps:**
1. Navigate to `/identities`
2. Observe the SSH Key Path row
**Expected Result:** Path displayed in code style. Yellow "File not found" warning badge shown
**Priority:** P1

## Scenario: Tab switching — View to Edit
**Preconditions:** Page is loaded with View tab active
**Steps:**
1. Click the "Edit" tab button
**Expected Result:** View tab content hides. Edit tab content shows with restart banner and identity forms. Edit tab button becomes active. JavaScript toggle, no page reload
**Priority:** P0

## Scenario: Tab switching — Edit to View
**Preconditions:** Edit tab is active
**Steps:**
1. Click the "View" tab button
**Expected Result:** Edit tab content hides. View tab content shows. View tab button becomes active
**Priority:** P0

## Scenario: Edit tab — restart banner always visible
**Preconditions:** Edit tab is active
**Steps:**
1. Switch to Edit tab
2. Observe the banner at the top
**Expected Result:** Yellow restart banner displays: "All identity changes require a supervisor restart to take effect. Secrets are stored in .env, not in config.yaml."
**Priority:** P0

## Scenario: Edit tab — Coder Identity form fields
**Preconditions:** Edit tab is active
**Steps:**
1. Observe the Coder Identity form
**Expected Result:** Form shows: GitHub Token (password input), SSH Key Path (text input), Git Author Name (text input), Git Author Email (email input), Linear API Key (password input). Password fields show placeholder with masked current value. "Save Coder Identity" button at bottom
**Priority:** P0

## Scenario: Edit tab — Reviewer Identity form fields
**Preconditions:** Edit tab is active
**Steps:**
1. Observe the Reviewer Identity form
**Expected Result:** Form shows: GitHub Token (password input), Linear API Key (password input). Password fields show placeholder with masked current value. "Save Reviewer Identity" button at bottom
**Priority:** P0

## Scenario: Successful identity save
**Preconditions:** Edit tab is active
**Steps:**
1. Enter a new value in a coder identity field (e.g. Git Author Name)
2. Click "Save Coder Identity"
**Expected Result:** POST to `/identities` with JSON body. Success feedback (green message) appears. Password fields are cleared. Message auto-hides after 5 seconds
**Priority:** P0

## Scenario: Empty password fields preserve current value
**Preconditions:** Edit tab is active, secrets are already set
**Steps:**
1. Leave all password fields empty
2. Modify only a non-secret field (e.g. SSH Key Path)
3. Click save
**Expected Result:** Only non-empty fields are sent in the payload. Existing secrets are preserved (not overwritten with empty values)
**Priority:** P0

## Scenario: Save with no changes
**Preconditions:** Edit tab is active
**Steps:**
1. Do not modify any fields (password fields empty, text fields unchanged)
2. Click save
**Expected Result:** Warning feedback (yellow): "No changes to save." Message auto-hides after 3 seconds
**Priority:** P1

## Scenario: Save button shows loading state
**Preconditions:** Edit tab is active
**Steps:**
1. Enter a value and click save
2. Observe button during request
**Expected Result:** Button shows aria-busy state during the POST request. Returns to normal after response
**Priority:** P2

## Scenario: Identity save network error
**Preconditions:** Network is unreachable or server error occurs
**Steps:**
1. Modify a field
2. Click save (with simulated network failure)
**Expected Result:** Error feedback displayed: "Network error." No crash
**Priority:** P2

## Scenario: No identity configuration available
**Preconditions:** Identity config could not be loaded (no config object)
**Steps:**
1. Navigate to `/identities`
**Expected Result:** Page shows message: "Identity configuration is not available (no config object provided)." No tabs or forms rendered
**Priority:** P2

## Scenario: Secret masking in View tab
**Preconditions:** GitHub tokens and Linear API keys are set
**Steps:**
1. Navigate to `/identities`
2. Inspect the masked secret values
**Expected Result:** Secrets are displayed in masked form (not full plaintext). Masking is done server-side before rendering
**Priority:** P1

## Scenario: SSH key path "Not set" state
**Preconditions:** Coder SSH key path is not configured
**Steps:**
1. Navigate to `/identities`
2. Observe the SSH Key Path row
**Expected Result:** Red "Not set" badge displayed. No path shown
**Priority:** P2
