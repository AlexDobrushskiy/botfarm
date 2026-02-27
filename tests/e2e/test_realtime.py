"""E2E tests for real-time updates (htmx polling) and supervisor controls."""

import sqlite3

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
        # Wait for at least one htmx polling response
        with page.expect_response("**/partials/slots"):
            pass
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
    """Supervisor pause/resume controls with callbacks."""

    def _set_db_state(self, db_path, *, paused, busy_to_free=False):
        """Helper to set dispatch and slot state in the test DB."""
        conn = sqlite3.connect(db_path)
        if paused:
            conn.execute(
                "UPDATE dispatch_state "
                "SET paused = 1, pause_reason = 'manual_pause'"
            )
        else:
            conn.execute(
                "UPDATE dispatch_state "
                "SET paused = 0, pause_reason = NULL"
            )
        if busy_to_free:
            conn.execute(
                "UPDATE slots SET status = 'free' WHERE status = 'busy'"
            )
            conn.execute(
                "UPDATE slots SET status = 'free' "
                "WHERE status = 'paused_manual'"
            )
        conn.commit()
        conn.close()

    def _restore_db_state(self, db_path):
        """Restore original seed DB state.

        NOTE: Ticket IDs below are coupled to seed_comprehensive_db in
        conftest.py.  If seed data changes, update these accordingly.
        """
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE dispatch_state SET paused = 0, pause_reason = NULL"
        )
        conn.execute(
            "UPDATE slots SET status = 'paused_manual' "
            "WHERE ticket_id = 'WEB-48'"
        )
        conn.execute(
            "UPDATE slots SET status = 'busy' "
            "WHERE ticket_id IN ('SMA-101', 'WEB-50')"
        )
        conn.commit()
        conn.close()

    def test_controls_container_exists(self, page):
        """P0: Supervisor controls container is present."""
        controls = page.locator("#supervisor-controls")
        assert controls.count() == 1

    def test_controls_have_buttons_with_callbacks(self, page):
        """P0: With callbacks, supervisor controls show action buttons."""
        controls = page.locator("#supervisor-controls")
        buttons = controls.locator("button")
        assert buttons.count() >= 1

    def test_pause_button_visible_when_running(self, live_server, page, seeded_db):
        """P0: When running (no paused/busy), Pause button is visible."""
        self._set_db_state(seeded_db, paused=False, busy_to_free=True)
        try:
            page.goto(live_server)
            controls = page.locator("#supervisor-controls")
            pause_btn = controls.locator("button", has_text="Pause")
            pause_btn.wait_for(state="visible", timeout=5000)
            assert pause_btn.is_visible()
        finally:
            self._restore_db_state(seeded_db)

    def test_resume_button_visible_when_paused(self, live_server, page, seeded_db):
        """P0: When fully paused (no busy workers), Resume button is visible."""
        # Set paused with no busy slots → true "paused" state
        self._set_db_state(seeded_db, paused=True, busy_to_free=True)
        try:
            page.goto(live_server)
            controls = page.locator("#supervisor-controls")
            resume_btn = controls.locator("button", has_text="Resume")
            resume_btn.wait_for(state="visible", timeout=5000)
            assert resume_btn.is_visible()
        finally:
            self._restore_db_state(seeded_db)

    def test_pausing_state_shows_cancel_and_count(
        self, live_server, page, seeded_db,
    ):
        """P1: Pausing state (paused + busy workers) shows Pausing and Cancel."""
        # Set dispatch paused but keep busy slots → "pausing:N" state
        self._set_db_state(seeded_db, paused=True, busy_to_free=False)
        try:
            page.goto(live_server)
            controls = page.locator("#supervisor-controls")
            # Should show "Pausing..." button and worker count in hint
            pausing_btn = controls.locator("button", has_text="Pausing")
            pausing_btn.wait_for(state="visible", timeout=5000)
            controls_text = controls.inner_text()
            assert "worker" in controls_text.lower()
            # Should also show Cancel button
            cancel_btn = controls.locator("button", has_text="Cancel")
            assert cancel_btn.is_visible()
        finally:
            self._restore_db_state(seeded_db)

    def test_pause_button_triggers_callback(
        self, live_server, page, seeded_db, pause_resume_state,
    ):
        """P0: Clicking Pause triggers the on_pause callback."""
        self._set_db_state(seeded_db, paused=False, busy_to_free=True)
        pause_resume_state["paused"] = False
        try:
            page.goto(live_server)
            controls = page.locator("#supervisor-controls")
            pause_btn = controls.locator("button", has_text="Pause")
            pause_btn.wait_for(state="visible", timeout=5000)
            pause_btn.click()
            page.wait_for_timeout(1000)
            assert pause_resume_state["paused"] is True
        finally:
            self._restore_db_state(seeded_db)

    def test_resume_button_triggers_callback(
        self, live_server, page, seeded_db, pause_resume_state,
    ):
        """P0: Clicking Resume triggers the on_resume callback."""
        self._set_db_state(seeded_db, paused=True, busy_to_free=True)
        pause_resume_state["resumed"] = False
        try:
            page.goto(live_server)
            controls = page.locator("#supervisor-controls")
            resume_btn = controls.locator("button", has_text="Resume")
            resume_btn.wait_for(state="visible", timeout=5000)
            resume_btn.click()
            page.wait_for_timeout(1000)
            assert pause_resume_state["resumed"] is True
        finally:
            self._restore_db_state(seeded_db)
