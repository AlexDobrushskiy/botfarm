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
    """Responsive layout tests."""

    def test_desktop_layout(self, live_server, page):
        """P0: At 1280px, full layout renders correctly."""
        page.set_viewport_size({"width": 1280, "height": 800})
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        # Slots table should be visible with all columns
        table = page.locator("#slots-panel table")
        assert table.is_visible()
        headers = table.locator("thead th").all_text_contents()
        assert len(headers) == 8

    def test_tablet_layout(self, live_server, page):
        """P1: At 768px, content fits without overflow."""
        page.set_viewport_size({"width": 768, "height": 1024})
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        # Page should render without errors
        assert page.title() == "Live Status - Botfarm"

    def test_mobile_layout(self, live_server, page):
        """P1: At 480px, content is readable."""
        page.set_viewport_size({"width": 480, "height": 812})
        page.goto(live_server)
        page.wait_for_load_state("networkidle")
        assert page.title() == "Live Status - Botfarm"
        # Main heading should still be visible
        assert page.locator("h1", has_text="Live Status").is_visible()
