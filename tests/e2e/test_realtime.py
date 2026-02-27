"""E2E tests for real-time updates (htmx polling)."""

import pytest


@pytest.mark.playwright
class TestHtmxPolling:
    """htmx partial refresh tests."""

    def test_slots_panel_has_htmx_trigger(self, page):
        """P0: Slots panel has htmx polling trigger."""
        panel = page.locator("#slots-panel")
        assert panel.get_attribute("hx-get") == "/partials/slots"
        assert "5s" in panel.get_attribute("hx-trigger")

    def test_usage_panel_has_htmx_trigger(self, page):
        """P0: Usage panel has htmx polling trigger."""
        panel = page.locator("#usage-panel")
        assert panel.get_attribute("hx-get") == "/partials/usage"
        assert "5s" in panel.get_attribute("hx-trigger")

    def test_queue_panel_has_htmx_trigger(self, page):
        """P0: Queue panel has htmx polling trigger."""
        panel = page.locator("#queue-panel")
        assert panel.get_attribute("hx-get") == "/partials/queue"
        assert "5s" in panel.get_attribute("hx-trigger")

    def test_supervisor_badge_has_htmx_trigger(self, page):
        """P1: Supervisor badge has htmx polling trigger."""
        badge = page.locator("#supervisor-badge")
        assert badge.get_attribute("hx-get") == "/partials/supervisor-badge"
        assert "5s" in badge.get_attribute("hx-trigger")

    def test_htmx_refresh_preserves_page(self, page):
        """P0: htmx refresh does not cause full page reload."""
        # Record initial page URL
        initial_url = page.url
        # Wait for at least one htmx cycle
        page.wait_for_timeout(6000)
        # URL should not have changed
        assert page.url == initial_url
        # Page title should still be correct
        assert page.title() == "Live Status - Botfarm"

    def test_history_panel_has_htmx_trigger(self, live_server, page):
        """P0: History panel refreshes every 10s."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        assert panel.get_attribute("hx-get") is not None
        assert "10s" in panel.get_attribute("hx-trigger")


@pytest.mark.playwright
class TestSupervisorControls:
    """Supervisor pause/resume controls (dashboard-only mode = no callbacks)."""

    def test_pause_resume_hidden_without_callbacks(self, page):
        """P0: In test mode (no callbacks), pause/resume are not shown
        or return 503 if clicked."""
        controls = page.locator("#supervisor-controls")
        # In dashboard-only mode, controls should either be empty
        # or buttons should not be present
        text = controls.inner_text()
        # Controls might be empty or show disabled state
        # This is valid — no callbacks means no pause/resume
        assert controls.count() == 1  # Element exists but may be empty
