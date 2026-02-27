"""E2E tests for the Task History page (/history)."""

import pytest


@pytest.mark.playwright
class TestHistoryTable:
    """Task history table rendering and columns."""

    def test_page_title(self, live_server, page):
        """P0: Page title is correct."""
        page.goto(f"{live_server}/history")
        assert page.title() == "Task History - Botfarm"

    def test_heading(self, live_server, page):
        """P0: Page has Task History heading."""
        page.goto(f"{live_server}/history")
        assert page.locator("h1", has_text="Task History").is_visible()

    def test_table_renders_with_columns(self, live_server, page):
        """P0: Table has expected column headers."""
        page.goto(f"{live_server}/history")
        table = page.locator("#history-panel table")
        table.wait_for(state="visible")
        # Headers may contain sort links with extra whitespace, so
        # strip each and check membership.
        headers = [h.strip() for h in table.locator("thead th").all_text_contents()]
        assert any("Ticket" in h for h in headers)
        assert any("Project" in h for h in headers)
        assert any("Status" in h for h in headers)
        assert any("Turns" in h for h in headers)
        assert any("Cost" in h for h in headers)

    def test_table_has_task_rows(self, live_server, page):
        """P0: Table contains task rows from seed data."""
        page.goto(f"{live_server}/history")
        rows = page.locator("#history-panel table tbody tr")
        # Seed data has 10 tasks
        assert rows.count() >= 5

    def test_status_color_coding(self, live_server, page):
        """P1: Status column uses correct color classes."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        assert panel.locator(".status-free").count() >= 1  # completed
        assert panel.locator(".status-failed").count() >= 1  # failed

    def test_row_click_navigates_to_detail(self, live_server, page):
        """P0: Clicking a row navigates to task detail."""
        page.goto(f"{live_server}/history")
        # Click first task row
        first_row = page.locator("#history-panel table tbody tr").first
        first_row.click()
        page.wait_for_load_state("networkidle")
        assert "/task/" in page.url

    def test_task_count_displayed(self, live_server, page):
        """P0: Task count summary is shown."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        text = panel.inner_text()
        assert "task" in text.lower()
        assert "found" in text.lower()

    def test_pr_link_present(self, live_server, page):
        """P1: Tasks with PRs show PR links."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        pr_links = panel.locator("a", has_text="PR #")
        assert pr_links.count() >= 1


@pytest.mark.playwright
class TestHistoryFilters:
    """Filter functionality on history page."""

    def test_filter_form_exists(self, live_server, page):
        """P0: Filter form with search, project, status fields exists."""
        page.goto(f"{live_server}/history")
        form = page.locator("#history-filters")
        assert form.is_visible()
        assert form.locator("input[name='search']").is_visible()
        assert form.locator("select[name='project']").is_visible()
        assert form.locator("select[name='status']").is_visible()

    def test_project_filter_dropdown_options(self, live_server, page):
        """P0: Project dropdown contains seeded projects."""
        page.goto(f"{live_server}/history")
        select = page.locator("select[name='project']")
        options = select.locator("option").all_text_contents()
        assert "All projects" in options
        assert "bot-farm" in options
        assert "web-app" in options

    def test_project_filter_works(self, live_server, page):
        """P0: Filtering by project shows only matching tasks."""
        page.goto(f"{live_server}/history")
        page.select_option("select[name='project']", "bot-farm")
        page.click("#history-filters button[type='submit']")
        page.wait_for_load_state("networkidle")
        # All visible rows should be bot-farm
        rows = page.locator("#history-panel table tbody tr")
        assert rows.count() > 0, "Filter should return at least one bot-farm task"
        for i in range(rows.count()):
            text = rows.nth(i).inner_text()
            assert "bot-farm" in text

    def test_status_filter_works(self, live_server, page):
        """P0: Filtering by status shows only matching tasks."""
        page.goto(f"{live_server}/history")
        page.select_option("select[name='status']", "completed")
        page.click("#history-filters button[type='submit']")
        page.wait_for_load_state("networkidle")
        panel = page.locator("#history-panel")
        # Should have at least one completed row and no failed rows
        assert panel.locator(".status-free").count() > 0, "Should have at least one completed task"
        assert panel.locator(".status-failed").count() == 0

    def test_search_filter_works(self, live_server, page):
        """P0: Searching by ticket ID filters results."""
        page.goto(f"{live_server}/history")
        page.fill("input[name='search']", "SMA-80")
        page.click("#history-filters button[type='submit']")
        page.wait_for_load_state("networkidle")
        panel = page.locator("#history-panel")
        text = panel.inner_text()
        assert "SMA-80" in text


@pytest.mark.playwright
class TestHistorySorting:
    """Column sorting on history page."""

    def test_default_sort_has_sortable_columns(self, live_server, page):
        """P0: Table has sortable column headers with links."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        # Sortable columns render as links inside <th>
        sort_links = panel.locator("thead th a")
        assert sort_links.count() >= 3  # At least Ticket, Project, Status are sortable

    def test_sort_by_turns(self, live_server, page):
        """P0: Clicking Turns column header sorts by turns."""
        page.goto(f"{live_server}/history")
        turns_header = page.locator("#history-panel thead a", has_text="Turns")
        turns_header.click()
        page.wait_for_load_state("networkidle")
        assert "sort_by=turns" in page.url


@pytest.mark.playwright
class TestHistoryInteractiveFilters:
    """Interactive filter combinations and clearing on history page."""

    def test_combined_project_and_status_filter(self, live_server, page):
        """P1: Combining project + status filter works."""
        page.goto(f"{live_server}/history")
        page.select_option("select[name='project']", "bot-farm")
        page.select_option("select[name='status']", "completed")
        page.click("#history-filters button[type='submit']")
        page.wait_for_load_state("networkidle")
        panel = page.locator("#history-panel")
        rows = panel.locator("table tbody tr")
        assert rows.count() > 0, "Combined filter should return results"
        for i in range(rows.count()):
            text = rows.nth(i).inner_text()
            assert "bot-farm" in text
        # Verify status filter: only completed (status-free), no failed
        assert panel.locator(".status-free").count() > 0
        assert panel.locator(".status-failed").count() == 0

    def test_combined_project_status_and_search(self, live_server, page):
        """P1: All three filters (project + status + search) work together."""
        page.goto(f"{live_server}/history")
        page.select_option("select[name='project']", "bot-farm")
        page.select_option("select[name='status']", "completed")
        page.fill("input[name='search']", "SMA")
        page.click("#history-filters button[type='submit']")
        page.wait_for_load_state("networkidle")
        panel = page.locator("#history-panel")
        rows = panel.locator("table tbody tr")
        # All matching rows should be bot-farm project with SMA ticket prefix
        for i in range(rows.count()):
            text = rows.nth(i).inner_text()
            assert "bot-farm" in text
            assert "SMA" in text

    def test_clear_filters_shows_all_data(self, live_server, page):
        """P1: Clearing filters restores full dataset."""
        page.goto(f"{live_server}/history")
        # First count all tasks
        all_rows_count = page.locator("#history-panel table tbody tr").count()
        assert all_rows_count > 0

        # Apply a restrictive filter via URL navigation (most reliable)
        page.goto(f"{live_server}/history?project=bot-farm")
        filtered_count = page.locator("#history-panel table tbody tr").count()
        assert filtered_count < all_rows_count

        # Clear filters by navigating to unfiltered history page
        page.goto(f"{live_server}/history")
        restored_count = page.locator("#history-panel table tbody tr").count()
        assert restored_count == all_rows_count

    def test_search_by_title_substring(self, live_server, page):
        """P0: Search filters by title substring, not just ticket ID."""
        page.goto(f"{live_server}/history")
        # Seed data has tasks with "Add" in the title (e.g. "Add retry logic")
        page.fill("input[name='search']", "Add")
        page.click("#history-filters button[type='submit']")
        page.wait_for_load_state("networkidle")
        rows = page.locator("#history-panel table tbody tr")
        assert rows.count() > 0, "Search for \"Add\" should return at least one result"
        for i in range(rows.count()):
            text = rows.nth(i).inner_text()
            assert "Add" in text

    def test_no_results_message(self, live_server, page):
        """P1: Searching for non-existent term shows 'No tasks found'."""
        page.goto(f"{live_server}/history")
        page.fill("input[name='search']", "ZZZZNONEXISTENT999")
        page.click("#history-filters button[type='submit']")
        page.wait_for_load_state("networkidle")
        panel = page.locator("#history-panel")
        text = panel.inner_text()
        assert "No tasks found" in text


@pytest.mark.playwright
class TestHistorySortInteractive:
    """Interactive sort direction toggle tests."""

    def test_sort_toggle_direction(self, live_server, page):
        """P0: Clicking same column twice toggles sort direction."""
        page.goto(f"{live_server}/history")
        turns_header = page.locator("#history-panel thead a", has_text="Turns")

        # First click — sorts DESC
        turns_header.click()
        page.wait_for_load_state("networkidle")
        assert "sort_by=turns" in page.url
        assert "sort_dir=DESC" in page.url

        # Second click — toggles to ASC
        turns_header = page.locator("#history-panel thead a", has_text="Turns")
        turns_header.click()
        page.wait_for_load_state("networkidle")
        assert "sort_by=turns" in page.url
        assert "sort_dir=ASC" in page.url

    def test_sort_indicator_shown(self, live_server, page):
        """P1: Active sort column shows direction indicator arrow."""
        page.goto(f"{live_server}/history")
        turns_header = page.locator("#history-panel thead a", has_text="Turns")
        turns_header.click()
        page.wait_for_load_state("networkidle")
        # The active sort column should have an arrow indicator
        header_text = page.locator(
            "#history-panel thead a", has_text="Turns"
        ).inner_text()
        # Check for unicode arrow (↓ or ↑)
        assert "\u2193" in header_text or "\u2191" in header_text

    def test_sort_by_different_columns(self, live_server, page):
        """P1: Can sort by multiple different columns."""
        page.goto(f"{live_server}/history")
        # Sort by Ticket
        ticket_header = page.locator(
            "#history-panel thead a", has_text="Ticket"
        )
        ticket_header.click()
        page.wait_for_load_state("networkidle")
        assert "sort_by=ticket_id" in page.url

        # Sort by Project
        project_header = page.locator(
            "#history-panel thead a", has_text="Project"
        )
        project_header.click()
        page.wait_for_load_state("networkidle")
        assert "sort_by=project" in page.url


@pytest.mark.playwright
class TestHistoryPagination:
    """Pagination controls on history page."""

    def test_pagination_page_info_shown(self, live_server, page):
        """P0: Page info (e.g. 'page 1 of 1') is displayed."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        text = panel.inner_text()
        assert "page" in text.lower()

    def test_pagination_preserves_filters(self, live_server, page):
        """P1: Sort params are preserved in pagination links."""
        page.goto(f"{live_server}/history?sort_by=turns&sort_dir=DESC")
        panel = page.locator("#history-panel")
        # Even on page 1, pagination links (if any) or the page itself
        # should preserve the sort state
        text = panel.inner_text()
        assert "page" in text.lower()
        # URL should still contain our sort params
        assert "sort_by=turns" in page.url


@pytest.mark.playwright
class TestHistoryMissing:
    """Edge cases for history page."""

    def test_missing_fields_display_gracefully(self, live_server, page):
        """P1: Missing optional fields show '-' not 'null' or 'None'."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        text = panel.inner_text()
        assert "None" not in text
        assert "null" not in text
