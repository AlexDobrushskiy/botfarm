"""E2E tests for the Live Status page (/)."""

import pytest


@pytest.mark.playwright
class TestSlotsTable:
    """Slots table rendering and data correctness."""

    def test_slots_table_renders(self, page):
        """P0: Slots table renders with correct columns."""
        table = page.locator("#slots-panel table")
        table.wait_for(state="visible")
        headers = table.locator("thead th").all_text_contents()
        assert "Project" in headers
        assert "Slot" in headers
        assert "Status" in headers
        assert "Ticket" in headers
        assert "Progress" in headers

    def test_slots_table_has_rows(self, page):
        """P0: Slots table has expected rows from seed data."""
        rows = page.locator("#slots-panel table tbody tr")
        # Seed data has 8 slots across 3 projects
        assert rows.count() == 8

    def test_slot_status_color_coding(self, page):
        """P1: Each slot status uses the correct CSS class."""
        panel = page.locator("#slots-panel")
        assert panel.locator(".status-busy").count() >= 1
        assert panel.locator(".status-free").count() >= 1
        assert panel.locator(".status-failed").count() >= 1
        assert panel.locator(".status-paused_manual").count() >= 1
        assert panel.locator(".status-paused_limit").count() >= 1
        assert panel.locator(".status-completed_pending_cleanup").count() >= 1

    def test_slot_ticket_link(self, page):
        """P0: Busy slot shows ticket ID as a link."""
        panel = page.locator("#slots-panel")
        # SMA-101 is a busy slot
        link = panel.locator("a", has_text="SMA-101")
        assert link.count() >= 1

    def test_slot_pipeline_progress(self, page):
        """P1: Busy slot shows pipeline progress dots."""
        panel = page.locator("#slots-panel")
        pipelines = panel.locator(".slot-pipeline")
        assert pipelines.count() >= 1

    def test_slot_detail_link(self, page):
        """P1: Slot with a task has a 'detail' link."""
        panel = page.locator("#slots-panel")
        detail_links = panel.locator("a", has_text="detail")
        assert detail_links.count() >= 1
        # Verify it navigates to task detail
        href = detail_links.first.get_attribute("href")
        assert href.startswith("/task/")

    def test_slot_logs_link_for_busy(self, page):
        """P1: Busy slot has a 'logs' link."""
        panel = page.locator("#slots-panel")
        logs_links = panel.locator("a", has_text="logs")
        assert logs_links.count() >= 1

    def test_paused_limit_resume_countdown(self, page):
        """P1: paused_limit slot has a resume countdown element."""
        panel = page.locator("#slots-panel")
        countdown = panel.locator(".resume-countdown")
        assert countdown.count() >= 1
        assert countdown.first.get_attribute("data-resume") is not None


@pytest.mark.playwright
class TestUsagePanel:
    """Usage panel rendering."""

    def test_usage_panel_renders(self, page):
        """P0: Usage panel shows 5-hour and 7-day utilization."""
        panel = page.locator("#usage-panel")
        text = panel.inner_text()
        assert "5-hour utilization" in text
        assert "7-day utilization" in text

    def test_usage_progress_bars(self, page):
        """P0: Usage panel contains progress bar elements."""
        panel = page.locator("#usage-panel")
        progress_bars = panel.locator("progress")
        assert progress_bars.count() >= 2

    def test_usage_percentage_values(self, page):
        """P0: Usage percentages are displayed."""
        panel = page.locator("#usage-panel")
        # Seed data has 100% 5h utilization and 85% 7d
        text = panel.inner_text()
        assert "%" in text

    def test_extra_usage_displayed(self, page):
        """P1: Extra usage section is visible when enabled."""
        panel = page.locator("#usage-panel")
        text = panel.inner_text()
        assert "Extra Usage" in text

    def test_usage_reset_countdown(self, page):
        """P1: Reset countdown is present."""
        panel = page.locator("#usage-panel")
        countdowns = panel.locator(".reset-countdown")
        assert countdowns.count() >= 1


@pytest.mark.playwright
class TestQueueSection:
    """Queue section rendering."""

    def test_queue_renders_grouped_by_project(self, page):
        """P0: Queue entries are grouped by project."""
        panel = page.locator("#queue-panel")
        figcaptions = panel.locator("figcaption")
        # Seed data has entries for bot-farm and web-app
        assert figcaptions.count() >= 2
        text = panel.inner_text()
        assert "bot-farm" in text
        assert "web-app" in text

    def test_queue_entries_exist(self, page):
        """P0: Queue tables have rows."""
        panel = page.locator("#queue-panel")
        rows = panel.locator("table tbody tr")
        assert rows.count() >= 3

    def test_queue_priority_column(self, page):
        """P0: Queue entries show priority labels."""
        panel = page.locator("#queue-panel")
        text = panel.inner_text()
        assert "Urgent" in text or "High" in text or "Normal" in text

    def test_queue_next_marker(self, page):
        """P0: First unblocked entry has 'Next' marker."""
        panel = page.locator("#queue-panel")
        next_marker = panel.locator("mark", has_text="Next")
        assert next_marker.count() >= 1

    def test_queue_blocked_entry(self, page):
        """P1: Blocked entry shows blocked indicator."""
        panel = page.locator("#queue-panel")
        # Blocked entries have opacity style and blocked_by text
        text = panel.inner_text()
        assert "blocked by" in text


@pytest.mark.playwright
class TestSupervisorBadge:
    """Supervisor badge in nav bar."""

    def test_supervisor_badge_visible(self, page):
        """P0: Supervisor badge is visible in nav."""
        badge = page.locator("#supervisor-badge")
        badge.wait_for(state="visible")
        assert badge.is_visible()

    def test_supervisor_badge_shows_state(self, page):
        """P0: Badge shows running or stopped state."""
        badge = page.locator("#supervisor-badge")
        text = badge.inner_text()
        assert "running" in text.lower() or "stopped" in text.lower()


@pytest.mark.playwright
class TestPageStructure:
    """Overall page structure and headings."""

    def test_page_title(self, page):
        """P0: Page title is 'Live Status - Botfarm'."""
        assert page.title() == "Live Status - Botfarm"

    def test_main_heading(self, page):
        """P0: Page has 'Live Status' heading."""
        heading = page.locator("h1", has_text="Live Status")
        assert heading.is_visible()

    def test_section_headings(self, page):
        """P0: Page has Slots, Usage, Queue section headings."""
        assert page.locator("h2", has_text="Slots").is_visible()
        assert page.locator("h2", has_text="Usage").is_visible()
        assert page.locator("h2", has_text="Queue").is_visible()

    def test_dark_theme_applied(self, page):
        """P0: Dark theme is active (data-theme='dark')."""
        html_el = page.locator("html")
        assert html_el.get_attribute("data-theme") == "dark"
