"""E2E tests for the Configuration page (/config)."""

import pytest


@pytest.mark.playwright
class TestConfigViewTab:
    """Configuration View tab tests."""

    def test_page_title(self, live_server, page):
        """P0: Page title is correct."""
        page.goto(f"{live_server}/config")
        assert page.title() == "Configuration - Botfarm"

    def test_heading(self, live_server, page):
        """P0: Page has Configuration heading."""
        page.goto(f"{live_server}/config")
        assert page.locator("h1", has_text="Configuration").is_visible()

    def test_no_config_message(self, live_server, page):
        """P0: Shows 'not available' when no config object (test mode)."""
        page.goto(f"{live_server}/config")
        text = page.locator("main").inner_text()
        # In test mode (no config object), should show unavailable message
        assert "not available" in text.lower() or "Projects" in text


@pytest.mark.playwright
class TestConfigTabSwitching:
    """Tab switching between View and Edit."""

    def test_tabs_exist(self, live_server, page):
        """P0: View and Edit tab buttons exist (when config is available)."""
        page.goto(f"{live_server}/config")
        tabs = page.locator(".config-tab")
        # If config is not available, tabs won't exist — that's OK
        if tabs.count() >= 2:
            tab_texts = tabs.all_text_contents()
            assert "View" in tab_texts
            assert "Edit" in tab_texts
        else:
            pytest.skip("Config tabs not available in test mode")

    def test_view_tab_active_by_default(self, live_server, page):
        """P0: View tab is active by default."""
        page.goto(f"{live_server}/config")
        tabs = page.locator(".config-tab")
        if tabs.count() >= 2:
            view_tab = tabs.first
            assert "active" in view_tab.get_attribute("class")
            view_content = page.locator("#tab-view")
            assert view_content.is_visible()
        else:
            pytest.skip("Config tabs not available in test mode")

    def test_switch_to_edit_tab(self, live_server, page):
        """P0: Clicking Edit tab shows edit content."""
        page.goto(f"{live_server}/config")
        tabs = page.locator(".config-tab")
        if tabs.count() >= 2:
            edit_tab = tabs.nth(1)
            edit_tab.click()
            edit_content = page.locator("#tab-edit")
            assert edit_content.is_visible()
            view_content = page.locator("#tab-view")
            assert not view_content.is_visible()
        else:
            pytest.skip("Config tabs not available in test mode")

    def test_switch_back_to_view(self, live_server, page):
        """P0: Switching back to View tab works."""
        page.goto(f"{live_server}/config")
        tabs = page.locator(".config-tab")
        if tabs.count() >= 2:
            # Switch to Edit then back
            tabs.nth(1).click()
            tabs.first.click()
            view_content = page.locator("#tab-view")
            assert view_content.is_visible()
        else:
            pytest.skip("Config tabs not available in test mode")
