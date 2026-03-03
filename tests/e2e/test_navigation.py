"""E2E tests for navigation and responsive design."""

import pytest

NAV_LINKS = [
    ("Live Status", "/", "Live Status - Botfarm"),
    ("Task History", "/history", "Task History - Botfarm"),
    ("Usage Trends", "/usage", "Usage Trends - Botfarm"),
    ("Metrics", "/metrics", "Metrics - Botfarm"),
    ("Configuration", "/config", "Configuration - Botfarm"),
    ("Identities", "/identities", "Identities - Botfarm"),
]


@pytest.mark.playwright
class TestNavigation:
    """Navigation bar link tests."""

    @pytest.mark.parametrize("label,path,expected_title", NAV_LINKS)
    def test_nav_link_works(self, live_server, page, label, path, expected_title):
        """P0: Each nav link navigates to the correct page."""
        link = page.locator(f'nav a:has-text("{label}")')
        link.click()
        page.wait_for_load_state("networkidle")
        assert page.title() == expected_title
        assert page.url.endswith(path) or page.url.endswith(path + "/")

    def test_active_page_highlighted(self, live_server, page):
        """P0: Current page's nav link has aria-current='page'."""
        # On index page, Live Status should be active
        active_link = page.locator('nav a[aria-current="page"]')
        assert active_link.count() == 1
        assert active_link.inner_text() == "Live Status"

    def test_active_page_changes_on_navigation(self, live_server, page):
        """P0: Active nav link updates when navigating."""
        page.click('nav a:has-text("Task History")')
        page.wait_for_load_state("networkidle")
        active_link = page.locator('nav a[aria-current="page"]')
        assert active_link.count() == 1
        assert active_link.inner_text() == "Task History"

    def test_nav_accessible_from_nested_page(self, live_server, page):
        """P1: Nav links work from nested task detail pages."""
        # Navigate to a task detail page
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        # All nav links should still be present
        for label, _, _ in NAV_LINKS:
            assert page.locator(f'nav a:has-text("{label}")').is_visible()

    def test_supervisor_badge_present_on_all_pages(self, live_server, page):
        """P1: Supervisor badge is in nav on every page."""
        for _, path, _ in NAV_LINKS:
            page.goto(f"{live_server}{path}")
            page.wait_for_load_state("networkidle")
            badge = page.locator("#supervisor-badge")
            assert badge.is_visible(), f"Badge missing on {path}"


@pytest.mark.playwright
class TestDarkTheme:
    """Dark theme verification."""

    def test_dark_theme_on_all_pages(self, live_server, page):
        """P0: data-theme='dark' is set on every page."""
        for _, path, _ in NAV_LINKS:
            page.goto(f"{live_server}{path}")
            page.wait_for_load_state("networkidle")
            html_el = page.locator("html")
            assert html_el.get_attribute("data-theme") == "dark", \
                f"Dark theme missing on {path}"


@pytest.mark.playwright
class TestResponsive:
    """Responsive layout tests for Live Status page."""

    def test_widescreen_layout(self, live_server, page):
        """P0: At 1920px, full layout renders correctly."""
        page.set_viewport_size({"width": 1920, "height": 1080})
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        table = page.locator("#slots-panel table")
        assert table.is_visible()
        headers = table.locator("thead th").all_text_contents()
        assert len(headers) == 9

    def test_desktop_layout(self, live_server, page):
        """P0: At 1024px, full layout renders correctly."""
        page.set_viewport_size({"width": 1024, "height": 768})
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        table = page.locator("#slots-panel table")
        assert table.is_visible()
        assert page.locator("h1", has_text="Live Status").is_visible()

    def test_tablet_layout(self, live_server, page):
        """P1: At 768px, page renders without errors."""
        page.set_viewport_size({"width": 768, "height": 1024})
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        assert page.title() == "Live Status - Botfarm"
        assert page.locator("h1", has_text="Live Status").is_visible()
        # Table should still be accessible (may have horizontal scroll)
        assert page.locator("#slots-panel table").is_visible()

    def test_mobile_layout(self, live_server, page):
        """P1: At 480px, content is readable."""
        page.set_viewport_size({"width": 480, "height": 812})
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        assert page.title() == "Live Status - Botfarm"
        assert page.locator("h1", has_text="Live Status").is_visible()


@pytest.mark.playwright
class TestResponsiveTaskDetail:
    """Responsive layout tests for Task Detail page."""

    def test_task_detail_widescreen(self, live_server, page):
        """P0: Task detail renders at 1920px."""
        page.set_viewport_size({"width": 1920, "height": 1080})
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        assert "SMA-80" in page.title()
        cards = page.locator(".metric-card")
        assert cards.count() >= 6

    def test_task_detail_desktop(self, live_server, page):
        """P0: Task detail renders at 1024px."""
        page.set_viewport_size({"width": 1024, "height": 768})
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        assert "SMA-80" in page.title()
        assert page.locator(".pipeline-stepper").is_visible()

    def test_task_detail_tablet(self, live_server, page):
        """P1: Task detail renders at 768px with metric cards reflowing."""
        page.set_viewport_size({"width": 768, "height": 1024})
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        assert "SMA-80" in page.title()
        cards = page.locator(".metric-card")
        assert cards.count() >= 6
        # All cards should still be visible even on smaller viewport
        for i in range(min(cards.count(), 6)):
            assert cards.nth(i).is_visible()

    def test_task_detail_mobile(self, live_server, page):
        """P1: Task detail renders at 480px, content is readable."""
        page.set_viewport_size({"width": 480, "height": 812})
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        assert "SMA-80" in page.title()
        # Heading still visible
        heading = page.locator("h1")
        assert heading.is_visible()
        # Pipeline stepper still visible
        assert page.locator(".pipeline-stepper").is_visible()

    def test_metric_cards_reflow_narrow(self, live_server, page):
        """P1: Metric cards reflow correctly on narrow viewport."""
        page.set_viewport_size({"width": 480, "height": 812})
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        cards = page.locator(".metric-card")
        assert cards.count() >= 6
        # Each card should be visible and not clipped
        for i in range(min(cards.count(), 6)):
            card = cards.nth(i)
            assert card.is_visible()
            box = card.bounding_box()
            # Card should be within the viewport width
            assert box["x"] + box["width"] <= 480 + 20

    def test_stage_runs_table_mobile(self, live_server, page):
        """P1: Stage runs table is accessible on mobile (scroll or column hiding)."""
        page.set_viewport_size({"width": 480, "height": 812})
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        stage_detail = page.locator("details", has_text="Stage Runs")
        assert stage_detail.is_visible()
        table = stage_detail.locator("table")
        assert table.is_visible()


@pytest.mark.playwright
class TestCrossPageNavigation:
    """Tests for navigation between related pages."""

    def test_history_row_to_task_detail(self, live_server, page):
        """P0: Clicking a history row navigates to the correct task detail."""
        page.goto(f"{live_server}/history")
        page.wait_for_load_state("networkidle")
        first_row = page.locator("#history-panel table tbody tr").first
        first_row.click()
        page.wait_for_load_state("networkidle")
        assert "/task/" in page.url
        # Task detail page should load
        assert page.locator(".metric-card").count() >= 1

    def test_task_detail_to_log_viewer(self, live_server, page):
        """P0: Stage run log link navigates to log viewer."""
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        stage_detail = page.locator("details", has_text="Stage Runs")
        view_links = stage_detail.locator("a", has_text="view")
        if view_links.count() >= 1:
            view_links.first.click()
            page.wait_for_load_state("networkidle")
            assert "/logs" in page.url
            assert "SMA-80" in page.title()
        else:
            pytest.skip("No stage run log links available")

    def test_log_viewer_back_to_detail(self, live_server, page):
        """P1: Back link on log viewer navigates to task detail."""
        page.goto(f"{live_server}/task/SMA-80/logs")
        page.wait_for_load_state("networkidle")
        back = page.locator("a", has_text="Back to Task Detail")
        back.click()
        page.wait_for_load_state("networkidle")
        assert page.url.endswith("/task/SMA-80")
        assert "SMA-80" in page.title()

    def test_task_detail_back_to_history(self, live_server, page):
        """P1: Back link on task detail navigates to history."""
        page.goto(f"{live_server}/task/SMA-80")
        page.wait_for_load_state("networkidle")
        back = page.locator("a", has_text="Back to Task History")
        back.click()
        page.wait_for_load_state("networkidle")
        assert page.url.endswith("/history")
        assert page.title() == "Task History - Botfarm"

    def test_browser_back_navigation(self, live_server, page):
        """P1: Browser back button works after navigation."""
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        page.click('nav a:has-text("Task History")')
        page.wait_for_load_state("networkidle")
        assert page.url.endswith("/history")
        page.go_back()
        page.wait_for_load_state("networkidle")
        assert page.title() == "Live Status - Botfarm"
