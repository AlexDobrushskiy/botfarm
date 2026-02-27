"""E2E tests for the Usage Trends page (/usage)."""

import pytest


@pytest.mark.playwright
class TestUsageTrendsPage:
    """Usage Trends page rendering."""

    def test_page_title(self, live_server, page):
        """P0: Page title is correct."""
        page.goto(f"{live_server}/usage")
        assert page.title() == "Usage Trends - Botfarm"

    def test_heading(self, live_server, page):
        """P0: Page has Usage Trends heading."""
        page.goto(f"{live_server}/usage")
        assert page.locator("h1", has_text="Usage Trends").is_visible()

    def test_current_usage_panel(self, live_server, page):
        """P0: Current usage section is displayed."""
        page.goto(f"{live_server}/usage")
        text = page.locator("main").inner_text()
        assert "5-hour utilization" in text
        assert "7-day utilization" in text


@pytest.mark.playwright
class TestTimeRangeButtons:
    """Time range selection buttons."""

    def test_default_range_7d(self, live_server, page):
        """P0: Default time range is 7d (highlighted)."""
        page.goto(f"{live_server}/usage")
        btn_7d = page.locator('a[role="button"]', has_text="Last 7d")
        assert btn_7d.get_attribute("aria-current") == "true"

    def test_24h_button(self, live_server, page):
        """P0: Clicking '24h' updates the view."""
        page.goto(f"{live_server}/usage")
        page.click('a[role="button"]:has-text("Last 24h")')
        page.wait_for_load_state("networkidle")
        assert "range=24h" in page.url
        btn = page.locator('a[role="button"]', has_text="Last 24h")
        assert btn.get_attribute("aria-current") == "true"

    def test_30d_button(self, live_server, page):
        """P0: Clicking '30d' updates the view."""
        page.goto(f"{live_server}/usage")
        page.click('a[role="button"]:has-text("Last 30d")')
        page.wait_for_load_state("networkidle")
        assert "range=30d" in page.url

    def test_7d_button_explicit(self, live_server, page):
        """P0: Clicking '7d' works."""
        page.goto(f"{live_server}/usage?range=24h")
        page.click('a[role="button"]:has-text("Last 7d")')
        page.wait_for_load_state("networkidle")
        assert "range=7d" in page.url


@pytest.mark.playwright
class TestUsageChart:
    """Usage chart rendering."""

    def test_chart_canvas_exists(self, live_server, page):
        """P0: Chart canvas element is rendered."""
        page.goto(f"{live_server}/usage")
        canvas = page.locator("#usage-chart")
        assert canvas.is_visible()

    def test_extra_usage_chart_exists(self, live_server, page):
        """P1: Extra usage chart is shown when data has extra usage."""
        page.goto(f"{live_server}/usage")
        # Seed data includes extra usage snapshots
        extra_canvas = page.locator("#extra-usage-chart")
        assert extra_canvas.is_visible()


@pytest.mark.playwright
class TestRawSnapshotData:
    """Raw snapshot data table."""

    def test_raw_data_expandable(self, live_server, page):
        """P1: Raw snapshot data is in a collapsible details element."""
        page.goto(f"{live_server}/usage")
        details = page.locator("details", has_text="Raw snapshot data")
        assert details.count() >= 1

    def test_raw_data_table_columns(self, live_server, page):
        """P1: Raw data table has expected columns."""
        page.goto(f"{live_server}/usage")
        details = page.locator("details", has_text="Raw snapshot data")
        details.locator("summary").click()
        headers = details.locator("thead th").all_text_contents()
        assert "Timestamp" in headers
        assert "5-hour (%)" in headers
        assert "7-day (%)" in headers

    def test_raw_data_has_rows(self, live_server, page):
        """P1: Raw data table contains snapshot rows."""
        page.goto(f"{live_server}/usage")
        details = page.locator("details", has_text="Raw snapshot data")
        details.locator("summary").click()
        rows = details.locator("table tbody tr")
        assert rows.count() >= 1

    def test_extra_usage_columns_present(self, live_server, page):
        """P1: Extra usage columns appear when data has extra usage."""
        page.goto(f"{live_server}/usage")
        details = page.locator("details", has_text="Raw snapshot data")
        details.locator("summary").click()
        headers = details.locator("thead th").all_text_contents()
        assert "Extra Usage ($)" in headers
        assert "Extra Limit ($)" in headers
