# E2E Test Scenarios: Navigation & Responsive Design

## Navigation

### Scenario: All nav links work
**Preconditions:** Application is running
**Steps:**
1. Navigate to `/`
2. Click each navigation link in order: Live Status, Task History, Usage Trends, Metrics, Configuration, Identities
**Expected Result:** Each link navigates to the correct page. No 404 or error responses
**Priority:** P0

### Scenario: Active page highlighted in nav
**Preconditions:** Application is running
**Steps:**
1. Navigate to `/`
2. Observe the "Live Status" nav link
3. Navigate to `/history`
4. Observe the "Task History" nav link
5. Repeat for each page
**Expected Result:** Current page's nav link has aria-current="page" attribute. Styled with bold text and primary color underline. Only one link is active at a time
**Priority:** P0

### Scenario: Supervisor badge in nav bar
**Preconditions:** Application is running
**Steps:**
1. Navigate to any page
2. Observe the nav bar
**Expected Result:** Supervisor badge is visible in the nav bar alongside the logo. Badge refreshes via htmx every 5 seconds
**Priority:** P1

### Scenario: Nav links accessible from all pages
**Preconditions:** Application is running
**Steps:**
1. Navigate to `/task/{id}` (a non-nav-listed page)
2. Observe the navigation bar
**Expected Result:** All navigation links are still present and functional from nested pages
**Priority:** P1

## Responsive Design

### Scenario: Desktop layout (>768px)
**Preconditions:** Application is running
**Steps:**
1. Set viewport width to 1280px
2. Navigate through all pages
**Expected Result:** Full desktop layout. Metric cards in standard grid. Tables show all columns. Pipeline stepper at full size. Navigation is horizontal
**Priority:** P0

### Scenario: Tablet layout (768px)
**Preconditions:** Application is running
**Steps:**
1. Set viewport width to 768px
2. Navigate through all pages
**Expected Result:** Metric cards display in 3-column grid. Tables remain usable (may show horizontal scroll). Content fits within viewport
**Priority:** P1

### Scenario: Mobile layout (480px)
**Preconditions:** Application is running
**Steps:**
1. Set viewport width to 480px
2. Navigate through all pages
**Expected Result:** Metric cards display in 2-column grid. Pipeline nodes shrink (1.8rem). Pipeline connector adjusts. Font sizes reduce. Content remains readable and functional
**Priority:** P1

### Scenario: Mobile layout — tables
**Preconditions:** Application is running, viewport at 480px
**Steps:**
1. Navigate to `/history`
2. Observe the task table
3. Navigate to `/task/{id}` and observe stage runs table
**Expected Result:** Tables are scrollable horizontally if needed. Content does not overflow or break layout
**Priority:** P1

### Scenario: Mobile layout — forms
**Preconditions:** Application is running, viewport at 480px
**Steps:**
1. Navigate to `/history` (filter form)
2. Navigate to `/config` Edit tab (config forms)
3. Navigate to `/metrics` (project filter)
**Expected Result:** Form inputs are full-width and usable. Dropdowns and buttons are tappable. No elements overlap
**Priority:** P1

### Scenario: Charts responsive on resize
**Preconditions:** Usage data exists
**Steps:**
1. Navigate to `/usage`
2. Observe the charts at desktop width
3. Resize window to tablet width
4. Resize to mobile width
**Expected Result:** Chart.js charts resize responsively at each breakpoint. Labels remain readable. No overflow
**Priority:** P2

### Scenario: Log terminal responsive
**Preconditions:** Task with logs exists
**Steps:**
1. Navigate to `/task/{id}/logs/{stage}` at various viewport widths
**Expected Result:** Log terminal maintains dark theme styling. Long lines wrap (pre-wrap). Min-height 300px preserved. Max-height 70vh adjusts with viewport
**Priority:** P2

## Dark Theme

### Scenario: Dark theme applied globally
**Preconditions:** Application is running
**Steps:**
1. Navigate to any page
2. Inspect the HTML element
**Expected Result:** data-theme="dark" is set on the root element. Pico CSS dark theme is active. Background is dark, text is light
**Priority:** P0

### Scenario: Custom styles consistent with dark theme
**Preconditions:** Application is running
**Steps:**
1. Navigate through all pages
2. Observe color-coded elements (status badges, context fill, pipeline stepper)
**Expected Result:** All custom color-coded elements are legible against the dark background. No contrast issues
**Priority:** P1
