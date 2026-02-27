"""E2E tests for the Metrics page (/metrics)."""

import pytest


@pytest.mark.playwright
class TestMetricsOverview:
    """Metrics page overview section."""

    def test_page_title(self, live_server, page):
        """P0: Page title is correct."""
        page.goto(f"{live_server}/metrics")
        assert page.title() == "Metrics - Botfarm"

    def test_heading(self, live_server, page):
        """P0: Page has Metrics heading."""
        page.goto(f"{live_server}/metrics")
        assert page.locator("h1", has_text="Metrics").is_visible()

    def test_overview_kpi_cards(self, live_server, page):
        """P0: Overview cards show Total Tasks, Completed, Failed, Success Rate."""
        page.goto(f"{live_server}/metrics")
        text = page.locator("main").inner_text()
        assert "Total Tasks" in text
        assert "Completed" in text
        assert "Failed" in text
        assert "Success Rate" in text

    def test_total_tasks_count(self, live_server, page):
        """P0: Total tasks count is > 0."""
        page.goto(f"{live_server}/metrics")
        card = page.locator(".metric-card", has_text="Total Tasks")
        assert card.count() >= 1
        value = card.first.locator("h2").inner_text()
        assert int(value) > 0

    def test_success_rate_percentage(self, live_server, page):
        """P0: Success rate displays as percentage."""
        page.goto(f"{live_server}/metrics")
        card = page.locator(".metric-card", has_text="Success Rate")
        assert card.count() >= 1
        value = card.first.locator("h2").inner_text()
        assert "%" in value


@pytest.mark.playwright
class TestMetricsAverages:
    """Metrics averages section."""

    def test_averages_section_visible(self, live_server, page):
        """P0: Averages section is rendered."""
        page.goto(f"{live_server}/metrics")
        assert page.locator("h2", has_text="Averages").is_visible()

    def test_averages_cards(self, live_server, page):
        """P0: Averages include Wall Time, Turns, Review Iterations."""
        page.goto(f"{live_server}/metrics")
        text = page.locator("main").inner_text()
        assert "Avg Wall Time" in text
        assert "Avg Turns" in text
        assert "Avg Review Iterations" in text


@pytest.mark.playwright
class TestMetricsCompletedWindows:
    """Tasks completed time windows."""

    def test_completed_time_windows(self, live_server, page):
        """P0: Shows Today, Last 7 Days, Last 30 Days."""
        page.goto(f"{live_server}/metrics")
        text = page.locator("main").inner_text()
        assert "Today" in text
        assert "Last 7 Days" in text
        assert "Last 30 Days" in text


@pytest.mark.playwright
class TestMetricsTokenUsage:
    """Token usage and cost section."""

    def test_token_usage_section(self, live_server, page):
        """P0: Token usage section displays correctly."""
        page.goto(f"{live_server}/metrics")
        text = page.locator("main").inner_text()
        assert "Total Input Tokens" in text
        assert "Total Output Tokens" in text
        assert "Total Cost" in text
        assert "Avg Context Fill" in text

    def test_total_cost_with_dollar(self, live_server, page):
        """P0: Total cost shows dollar amount."""
        page.goto(f"{live_server}/metrics")
        card = page.locator(".metric-card", has_text="Total Cost")
        assert card.count() >= 1
        value = card.first.locator("h2").inner_text()
        assert "$" in value

    def test_extra_usage_cost_highlighted(self, live_server, page):
        """P1: Extra usage cost card has yellow highlight."""
        page.goto(f"{live_server}/metrics")
        extra_cost = page.locator(".metric-card .status-busy")
        if extra_cost.count() == 0:
            pytest.skip("No extra usage cost highlight present in seed data")
        text = page.locator("main").inner_text()
        assert "Extra Usage Cost" in text


@pytest.mark.playwright
class TestMetricsProjectFilter:
    """Project filter on metrics page."""

    def test_project_filter_dropdown(self, live_server, page):
        """P0: Project filter dropdown exists with 'All Projects' default."""
        page.goto(f"{live_server}/metrics")
        select = page.locator('select[name="project"]')
        assert select.is_visible()
        options = select.locator("option").all_text_contents()
        assert "All Projects" in options

    def test_project_filter_has_seeded_projects(self, live_server, page):
        """P0: Project filter contains projects from seed data."""
        page.goto(f"{live_server}/metrics")
        select = page.locator('select[name="project"]')
        options = select.locator("option").all_text_contents()
        assert "bot-farm" in options
        assert "web-app" in options

    def test_project_filter_works(self, live_server, page):
        """P0: Filtering by project updates metrics."""
        page.goto(f"{live_server}/metrics")
        page.select_option('select[name="project"]', "bot-farm")
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")
        assert "project=bot-farm" in page.url

    def test_project_filter_changes_total_tasks(self, live_server, page):
        """P0: Filtering by project changes the Total Tasks count."""
        # Get all-projects total
        page.goto(f"{live_server}/metrics")
        all_card = page.locator(".metric-card", has_text="Total Tasks")
        all_total = int(all_card.first.locator("h2").inner_text())

        # Filter to one project
        page.select_option('select[name="project"]', "bot-farm")
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")
        filtered_card = page.locator(".metric-card", has_text="Total Tasks")
        filtered_total = int(filtered_card.first.locator("h2").inner_text())

        # Filtered count should be less than or equal to all
        assert filtered_total <= all_total
        assert filtered_total > 0

    def test_clear_project_filter_restores_all(self, live_server, page):
        """P0: Clearing filter restores all-project metrics."""
        # Start filtered
        page.goto(f"{live_server}/metrics?project=bot-farm")
        filtered_card = page.locator(".metric-card", has_text="Total Tasks")
        filtered_total = int(filtered_card.first.locator("h2").inner_text())

        # Clear filter
        page.select_option('select[name="project"]', "")
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")
        all_card = page.locator(".metric-card", has_text="Total Tasks")
        all_total = int(all_card.first.locator("h2").inner_text())

        assert all_total >= filtered_total


@pytest.mark.playwright
class TestMetricsFailureReasons:
    """Common failure reasons table."""

    def test_failure_reasons_table(self, live_server, page):
        """P1: Failure reasons table is shown when failures exist."""
        page.goto(f"{live_server}/metrics")
        text = page.locator("main").inner_text()
        # Seed data has failed tasks
        assert "Failure Reason" in text or "Common Failure" in text
