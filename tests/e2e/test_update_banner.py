"""E2E tests for the Update Banner feature."""

import pytest


@pytest.mark.playwright
class TestUpdateBanner:
    """Update banner — visibility and interaction."""

    def test_update_banner_shows_update_available(self, live_server, page):
        """P0: Update banner appears with 'Update available' text."""
        page.goto(live_server)
        # The banner container loads content via htmx. Wait for inner content.
        banner_text = page.locator("#update-banner", has_text="Update available")
        banner_text.wait_for(state="visible", timeout=10000)
        assert "Update available" in banner_text.inner_text()

    def test_update_banner_shows_commit_count(self, live_server, page):
        """P0: Banner shows how many commits behind."""
        page.goto(live_server)
        banner = page.locator("#update-banner", has_text="Update available")
        banner.wait_for(state="visible", timeout=10000)
        text = banner.inner_text()
        # conftest patches commits_behind to return 3
        assert "3" in text
        assert "commit" in text.lower()

    def test_update_button_present(self, live_server, page):
        """P0: 'Update & Restart' button is visible in the banner."""
        page.goto(live_server)
        banner = page.locator("#update-banner", has_text="Update available")
        banner.wait_for(state="visible", timeout=10000)
        update_btn = banner.locator("button", has_text="Update")
        assert update_btn.is_visible()

    def test_update_button_has_confirmation(self, live_server, page):
        """P0: Update button triggers a confirmation dialog via hx-confirm."""
        page.goto(live_server)
        banner = page.locator("#update-banner", has_text="Update available")
        banner.wait_for(state="visible", timeout=10000)
        update_btn = banner.locator("button", has_text="Update")
        confirm_text = update_btn.get_attribute("hx-confirm")
        assert confirm_text is not None
        assert "restart" in confirm_text.lower()

    def test_update_button_posts_to_api(self, live_server, page):
        """P1: Update button targets the correct API endpoint."""
        page.goto(live_server)
        banner = page.locator("#update-banner", has_text="Update available")
        banner.wait_for(state="visible", timeout=10000)
        update_btn = banner.locator("button", has_text="Update")
        assert update_btn.get_attribute("hx-post") == "/api/update"
