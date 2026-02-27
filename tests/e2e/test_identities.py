"""E2E tests for the Identities page (/identities)."""

import pytest


@pytest.mark.playwright
class TestIdentitiesPage:
    """Identities page rendering."""

    def test_page_title(self, live_server, page):
        """P0: Page title is correct."""
        page.goto(f"{live_server}/identities")
        assert page.title() == "Identities - Botfarm"

    def test_heading(self, live_server, page):
        """P0: Page has Identities heading."""
        page.goto(f"{live_server}/identities")
        assert page.locator("h1", has_text="Identities").is_visible()

    def test_no_config_message(self, live_server, page):
        """P0: Shows unavailable message when no config object."""
        page.goto(f"{live_server}/identities")
        text = page.locator("main").inner_text()
        # In test mode (no config), should show not available message
        assert "not available" in text.lower() or "Coder Identity" in text


@pytest.mark.playwright
class TestIdentitiesTabSwitching:
    """Tab switching on identities page."""

    def test_tabs_exist(self, live_server, page):
        """P0: View and Edit tabs exist when config is available."""
        page.goto(f"{live_server}/identities")
        tabs = page.locator(".config-tab")
        if tabs.count() >= 2:
            tab_texts = tabs.all_text_contents()
            assert "View" in tab_texts
            assert "Edit" in tab_texts
        else:
            pytest.skip("Config tabs not available in test mode")

    def test_view_tab_active_by_default(self, live_server, page):
        """P0: View tab is active by default."""
        page.goto(f"{live_server}/identities")
        tabs = page.locator(".config-tab")
        if tabs.count() >= 2:
            view_content = page.locator("#tab-view")
            assert view_content.is_visible()
        else:
            pytest.skip("Config tabs not available in test mode")

    def test_switch_to_edit(self, live_server, page):
        """P0: Clicking Edit shows edit forms."""
        page.goto(f"{live_server}/identities")
        tabs = page.locator(".config-tab")
        if tabs.count() >= 2:
            tabs.nth(1).click()
            edit_content = page.locator("#tab-edit")
            assert edit_content.is_visible()
        else:
            pytest.skip("Config tabs not available in test mode")

    def test_edit_tab_restart_banner(self, live_server, page):
        """P0: Edit tab shows restart banner."""
        page.goto(f"{live_server}/identities")
        tabs = page.locator(".config-tab")
        if tabs.count() >= 2:
            tabs.nth(1).click()
            banner = page.locator(".restart-banner")
            assert banner.is_visible()
            assert "restart" in banner.inner_text().lower()
        else:
            pytest.skip("Config tabs not available in test mode")
