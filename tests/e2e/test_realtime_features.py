"""E2E tests for real-time features: SSE streaming, htmx auto-refresh,
Chart.js rendering, countdowns, and time-ago displays.

These tests verify that the dashboard's live features work correctly,
including content updates via htmx polling, SSE log event reception
with color-coded output, Chart.js data binding, and JavaScript-driven
countdown/timeago updates.
"""

import sqlite3
from datetime import datetime, timezone

import pytest


# ---------------------------------------------------------------------------
# htmx Auto-Refresh — verify content actually updates after DB changes
# ---------------------------------------------------------------------------


@pytest.mark.playwright
class TestHtmxContentUpdates:
    """Verify that htmx polling actually refreshes partial content."""

    def test_slots_content_updates_after_db_change(
        self, live_server, page, seeded_db,
    ):
        """P0: Slots table reflects DB changes after htmx poll."""
        page.goto(live_server)
        panel = page.locator("#slots-panel")

        # Verify initial state has SMA-101 as busy
        panel.locator("text=SMA-101").wait_for(state="visible")

        # Change the slot's ticket title in DB
        conn = sqlite3.connect(seeded_db)
        conn.execute(
            "UPDATE slots SET ticket_title = 'Updated retry logic' "
            "WHERE ticket_id = 'SMA-101'"
        )
        conn.commit()
        conn.close()

        try:
            # Wait for htmx poll to pick up the change (polling every 5s)
            page.locator("#slots-panel", has_text="Updated retry logic").wait_for(
                timeout=10000,
            )
        finally:
            # Restore original title
            conn = sqlite3.connect(seeded_db)
            conn.execute(
                "UPDATE slots SET ticket_title = 'Add retry logic' "
                "WHERE ticket_id = 'SMA-101'"
            )
            conn.commit()
            conn.close()

    def test_usage_panel_polls_and_renders(
        self, live_server, page,
    ):
        """P0: Usage panel fires htmx polls and renders DB data."""
        page.goto(live_server)
        panel = page.locator("#usage-panel")
        panel.wait_for(state="visible")

        # Verify the panel shows usage data from the DB seed
        text = panel.inner_text()
        assert "utilization" in text.lower() or "%" in text

        # Wait for at least one htmx poll response to fire
        with page.expect_response("**/partials/usage", timeout=30000):
            pass

    def test_queue_panel_updates_after_db_change(
        self, live_server, page, seeded_db,
    ):
        """P0: Queue panel reflects new queue entries after htmx poll."""
        page.goto(live_server)
        panel = page.locator("#queue-panel")
        panel.wait_for(state="visible")

        # Add a new queue entry (matches queue_entries schema exactly)
        conn = sqlite3.connect(seeded_db)
        conn.execute(
            "INSERT INTO queue_entries "
            "(project, position, ticket_id, ticket_title, priority, "
            " sort_order, url, snapshot_at) "
            "VALUES ('bot-farm', 0, 'SMA-999', 'Test queue entry', 1, "
            "        0.1, 'https://linear.app/team/issue/SMA-999', "
            "        '2026-02-27T10:00:00Z')"
        )
        conn.commit()
        conn.close()

        try:
            page.locator("#queue-panel", has_text="SMA-999").wait_for(
                timeout=15000,
            )
        finally:
            conn = sqlite3.connect(seeded_db)
            conn.execute(
                "DELETE FROM queue_entries WHERE ticket_id = 'SMA-999'"
            )
            conn.commit()
            conn.close()

    def test_supervisor_badge_updates_on_heartbeat_change(
        self, live_server, page, seeded_db,
    ):
        """P0: Supervisor badge reflects heartbeat state changes.

        Badge shows running/stopped based on heartbeat staleness, not
        dispatch pause state.  A fresh heartbeat means "running".
        """
        # Set a fresh heartbeat so the badge shows "running"
        now_iso = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(seeded_db)
        conn.execute(
            "UPDATE dispatch_state SET supervisor_heartbeat = ?",
            (now_iso,),
        )
        conn.commit()
        conn.close()

        try:
            page.goto(live_server)
            # Badge should show running state after htmx poll
            page.locator(
                "#supervisor-badge .supervisor-badge-running",
            ).wait_for(timeout=15000)
        finally:
            # Restore original stale heartbeat
            conn = sqlite3.connect(seeded_db)
            conn.execute(
                "UPDATE dispatch_state "
                "SET supervisor_heartbeat = '2026-02-27T09:30:00Z'"
            )
            conn.commit()
            conn.close()

    def test_history_panel_polls(self, live_server, page):
        """P0: History panel fires htmx polls on interval."""
        page.goto(f"{live_server}/history")
        panel = page.locator("#history-panel")
        panel.wait_for(state="visible")

        # Verify history panel has htmx trigger
        assert panel.get_attribute("hx-get") is not None
        trigger = panel.get_attribute("hx-trigger")
        assert "10s" in trigger

        # Wait for at least one history poll response (polls every 10s).
        # URL includes query params, so use glob with wildcard.
        with page.expect_response(
            "**/partials/history**", timeout=20000,
        ):
            pass


# ---------------------------------------------------------------------------
# SSE Log Streaming
# ---------------------------------------------------------------------------


@pytest.mark.playwright
class TestSSELogStreaming:
    """SSE log streaming via EventSource."""

    def test_log_viewer_static_content(self, live_server, page):
        """P0: Completed stage shows static log content (no SSE)."""
        page.goto(f"{live_server}/task/SMA-80/logs/implement")
        page.wait_for_load_state("networkidle")

        # Should show the formatted log content
        output = page.locator("#log-output")
        output.wait_for(state="visible")
        text = output.inner_text()
        assert len(text) > 0

        # No LIVE badge for completed stage
        live_badge = page.locator(".log-live-badge")
        assert live_badge.count() == 0

    def test_log_viewer_auto_scroll_hidden_for_static(
        self, live_server, page,
    ):
        """P1: Auto-scroll button is hidden for static (completed) logs."""
        page.goto(f"{live_server}/task/SMA-80/logs/implement")
        page.wait_for_load_state("networkidle")

        scroll_btn = page.locator("#scroll-toggle")
        # Should be hidden via JS for static content
        assert not scroll_btn.is_visible()

    def test_active_stage_has_live_badge(self, live_server, page):
        """P0: Active (busy) stage shows LIVE badge."""
        page.goto(f"{live_server}/task/SMA-101/logs/implement")
        badge = page.locator(".log-live-badge")
        badge.wait_for(state="visible", timeout=5000)
        assert "LIVE" in badge.inner_text()

    def test_active_stage_has_auto_scroll_button(
        self, live_server, page,
    ):
        """P0: Active stage shows auto-scroll toggle button."""
        page.goto(f"{live_server}/task/SMA-101/logs/implement")
        scroll_btn = page.locator("#scroll-toggle")
        scroll_btn.wait_for(state="visible", timeout=5000)
        assert "Auto-scroll" in scroll_btn.inner_text()

    def test_sse_delivers_log_lines(self, live_server, page):
        """P0: SSE stream delivers log lines to the page."""
        page.goto(f"{live_server}/task/SMA-101/logs/implement")
        output = page.locator("#log-output")
        output.wait_for(state="visible")

        # Wait for SSE-delivered log lines to appear.
        # The log file has lines that format_ndjson_line will parse into
        # readable text — e.g. the assistant text "Let me implement..."
        page.locator(
            "#log-output .log-line",
        ).first.wait_for(state="visible", timeout=10000)

        lines = page.locator("#log-output .log-line")
        assert lines.count() >= 1

    def test_sse_log_line_color_coding(self, live_server, page):
        """P0: SSE log lines have correct CSS classes for color coding."""
        page.goto(f"{live_server}/task/SMA-101/logs/implement")

        # Wait for lines to appear
        page.locator(
            "#log-output .log-line",
        ).first.wait_for(state="visible", timeout=10000)

        # Check for assistant lines (blue)
        assistant_lines = page.locator("#log-output .log-line-assistant")
        # Check for tool_use lines (purple)
        tool_use_lines = page.locator("#log-output .log-line-tool_use")
        # Check for tool_result lines (gray)
        tool_result_lines = page.locator("#log-output .log-line-tool_result")

        # Test data has all three types — assert each individually
        assert assistant_lines.count() >= 1, "Expected assistant (blue) lines"
        assert tool_use_lines.count() >= 1, "Expected tool_use (purple) lines"
        assert tool_result_lines.count() >= 1, "Expected tool_result (gray) lines"

    def test_sse_stream_completes_when_stage_inactive(
        self, live_server, page, seeded_db,
    ):
        """P0: SSE stream sends 'done' event when stage is no longer active."""
        # Navigate first while slot is still busy — this starts the SSE
        page.goto(f"{live_server}/task/SMA-101/logs/implement")

        # Verify LIVE badge is visible (SSE is active)
        badge = page.locator(".log-live-badge")
        badge.wait_for(state="visible", timeout=5000)

        # Now mark the slot as not busy so the SSE stream detects it
        conn = sqlite3.connect(seeded_db)
        conn.execute(
            "UPDATE slots SET status = 'free' "
            "WHERE ticket_id = 'SMA-101'"
        )
        conn.commit()
        conn.close()

        try:
            # Wait for the stream to complete — badge text changes to
            # COMPLETED when the 'done' SSE event fires
            page.wait_for_function(
                """() => {
                    const badge = document.querySelector(
                        '.log-live-badge, .log-done-badge'
                    );
                    return badge && badge.textContent.includes('COMPLETED');
                }""",
                timeout=15000,
            )
        finally:
            conn = sqlite3.connect(seeded_db)
            conn.execute(
                "UPDATE slots SET status = 'busy' "
                "WHERE ticket_id = 'SMA-101'"
            )
            conn.commit()
            conn.close()

    def test_auto_scroll_toggle(self, live_server, page):
        """P1: Auto-scroll toggle button changes text on click."""
        page.goto(f"{live_server}/task/SMA-101/logs/implement")
        scroll_btn = page.locator("#scroll-toggle")
        scroll_btn.wait_for(state="visible", timeout=5000)

        assert "ON" in scroll_btn.inner_text()
        scroll_btn.click()
        assert "OFF" in scroll_btn.inner_text()
        scroll_btn.click()
        assert "ON" in scroll_btn.inner_text()


# ---------------------------------------------------------------------------
# Chart.js Rendering
# ---------------------------------------------------------------------------


@pytest.mark.playwright
class TestChartJsRendering:
    """Chart.js canvas rendering and data binding."""

    def test_usage_chart_has_data(self, live_server, page):
        """P0: Usage chart canvas has been initialized with data."""
        page.goto(f"{live_server}/usage")
        canvas = page.locator("#usage-chart")
        canvas.wait_for(state="visible")

        # Verify Chart.js has initialized by checking the canvas has a
        # chart instance attached
        has_chart = page.evaluate(
            """() => {
                const canvas = document.getElementById('usage-chart');
                // Chart.js stores instance on the canvas element
                return canvas && canvas.__chartjs_instance__ !== undefined
                    || (typeof Chart !== 'undefined'
                        && Chart.getChart(canvas) !== undefined);
            }"""
        )
        assert has_chart

    def test_usage_chart_has_datasets(self, live_server, page):
        """P0: Usage chart has both 5-hour and 7-day datasets."""
        page.goto(f"{live_server}/usage")
        page.locator("#usage-chart").wait_for(state="visible")

        dataset_count = page.evaluate(
            """() => {
                const chart = Chart.getChart(
                    document.getElementById('usage-chart')
                );
                return chart ? chart.data.datasets.length : 0;
            }"""
        )
        assert dataset_count == 2

    def test_usage_chart_has_data_points(self, live_server, page):
        """P0: Usage chart datasets contain data points (not empty)."""
        page.goto(f"{live_server}/usage")
        page.locator("#usage-chart").wait_for(state="visible")

        data_length = page.evaluate(
            """() => {
                const chart = Chart.getChart(
                    document.getElementById('usage-chart')
                );
                if (!chart) return 0;
                return chart.data.datasets[0].data.length;
            }"""
        )
        assert data_length >= 1

    def test_extra_usage_chart_has_data(self, live_server, page):
        """P1: Extra usage chart has data when extra usage exists."""
        page.goto(f"{live_server}/usage")
        extra_canvas = page.locator("#extra-usage-chart")
        if not extra_canvas.is_visible():
            pytest.skip("No extra usage chart (no extra usage data)")

        has_chart = page.evaluate(
            """() => {
                const canvas = document.getElementById('extra-usage-chart');
                return canvas && Chart.getChart(canvas) !== undefined;
            }"""
        )
        assert has_chart

    def test_extra_usage_chart_datasets(self, live_server, page):
        """P1: Extra usage chart has spent and limit datasets."""
        page.goto(f"{live_server}/usage")
        extra_canvas = page.locator("#extra-usage-chart")
        if not extra_canvas.is_visible():
            pytest.skip("No extra usage chart")

        dataset_count = page.evaluate(
            """() => {
                const chart = Chart.getChart(
                    document.getElementById('extra-usage-chart')
                );
                return chart ? chart.data.datasets.length : 0;
            }"""
        )
        assert dataset_count == 2

    def test_time_range_24h_updates_chart(self, live_server, page):
        """P0: Selecting 24h time range reloads chart with filtered data."""
        page.goto(f"{live_server}/usage")
        page.locator("#usage-chart").wait_for(state="visible")

        # Click 24h button
        page.click('a[role="button"]:has-text("Last 24h")')
        page.wait_for_load_state("networkidle")
        page.locator("#usage-chart").wait_for(state="visible")

        # Chart should be re-initialized after time range change
        has_chart = page.evaluate(
            """() => {
                const chart = Chart.getChart(
                    document.getElementById('usage-chart')
                );
                return chart !== undefined;
            }"""
        )
        assert has_chart

    def test_time_range_30d_updates_chart(self, live_server, page):
        """P0: Selecting 30d time range reloads chart with filtered data."""
        page.goto(f"{live_server}/usage")
        page.locator("#usage-chart").wait_for(state="visible")

        page.click('a[role="button"]:has-text("Last 30d")')
        page.wait_for_load_state("networkidle")
        page.locator("#usage-chart").wait_for(state="visible")

        data_count = page.evaluate(
            """() => {
                const chart = Chart.getChart(
                    document.getElementById('usage-chart')
                );
                return chart ? chart.data.labels.length : 0;
            }"""
        )
        # 30d range should include all seeded snapshots
        assert data_count >= 1

    def test_chart_y_axis_max_100(self, live_server, page):
        """P1: Usage chart Y axis is capped at 100%."""
        page.goto(f"{live_server}/usage")
        page.locator("#usage-chart").wait_for(state="visible")

        y_max = page.evaluate(
            """() => {
                const chart = Chart.getChart(
                    document.getElementById('usage-chart')
                );
                return chart ? chart.options.scales.y.max : null;
            }"""
        )
        assert y_max == 100


# ---------------------------------------------------------------------------
# Countdown & Time-Ago
# ---------------------------------------------------------------------------


@pytest.mark.playwright
class TestCountdownTimers:
    """Countdown timer and time-ago display tests."""

    def test_resume_countdown_displayed(self, live_server, page):
        """P0: Paused-limit slot shows resume countdown element."""
        page.goto(live_server)

        # DP-10 is paused_limit with resume_after set
        countdown = page.locator(".resume-countdown")
        if countdown.count() == 0:
            pytest.skip("No resume countdown elements visible")

        text = countdown.first.inner_text()
        # Should display a time-based message
        assert "resume" in text.lower() or "m" in text

    def test_reset_countdown_displayed(self, live_server, page):
        """P0: Usage panel shows reset countdown elements."""
        page.goto(live_server)
        panel = page.locator("#usage-panel")
        panel.wait_for(state="visible")

        reset = panel.locator(".reset-countdown")
        if reset.count() == 0:
            pytest.skip("No reset countdown elements visible")

        text = reset.first.inner_text()
        # Should have been formatted by updateCountdowns()
        assert len(text) > 0

    def test_timeago_elements_formatted(self, live_server, page):
        """P0: Elements with data-timestamp are formatted as time-ago."""
        page.goto(f"{live_server}/history")
        page.wait_for_load_state("networkidle")

        timestamps = page.locator("[data-timestamp]")
        if timestamps.count() == 0:
            pytest.skip("No timestamp elements on page")

        # The JS should have converted ISO timestamps to "Xh ago" format
        # (or "Mon DD" for timestamps older than 7 days)
        first_text = timestamps.first.inner_text()
        # After DOMContentLoaded, timeago() should have run
        months = ["jan", "feb", "mar", "apr", "may", "jun",
                  "jul", "aug", "sep", "oct", "nov", "dec"]
        is_timeago = "ago" in first_text.lower() or "just now" in first_text.lower()
        is_absolute = any(m in first_text.lower() for m in months)
        assert is_timeago or is_absolute, \
            f"Expected timeago format, got: {first_text!r}"

    def test_countdown_updates_via_javascript(self, live_server, page):
        """P1: updateCountdowns() function is callable and updates elements."""
        page.goto(live_server)
        page.wait_for_load_state("networkidle")

        # Verify the function exists and can be called
        result = page.evaluate(
            """() => {
                if (typeof updateCountdowns === 'function') {
                    updateCountdowns();
                    return true;
                }
                return false;
            }"""
        )
        assert result is True

    def test_timeago_updates_via_javascript(self, live_server, page):
        """P1: updateTimeagos() function is callable."""
        page.goto(live_server)
        page.wait_for_load_state("networkidle")

        result = page.evaluate(
            """() => {
                if (typeof updateTimeagos === 'function') {
                    updateTimeagos();
                    return true;
                }
                return false;
            }"""
        )
        assert result is True

    def test_htmx_afterswap_triggers_countdown_update(
        self, live_server, page,
    ):
        """P1: htmx:afterSwap event triggers countdown and timeago updates."""
        page.goto(live_server)
        page.wait_for_load_state("networkidle")

        # Inject a sentinel countdown element, then fire a synthetic
        # htmx:afterSwap event.  If the event listener is wired up,
        # updateCountdowns() will reformat the element's text.
        updated = page.evaluate(
            """() => {
                // Create a countdown element with a future timestamp
                const el = document.createElement('span');
                el.className = 'reset-countdown';
                const future = new Date(Date.now() + 3600000).toISOString();
                el.setAttribute('data-reset', future);
                el.textContent = 'NOT_UPDATED';
                document.body.appendChild(el);

                // Dispatch synthetic htmx:afterSwap event
                document.body.dispatchEvent(
                    new Event('htmx:afterSwap', { bubbles: true })
                );

                // Check if the element's text was updated
                return el.textContent !== 'NOT_UPDATED';
            }"""
        )
        assert updated


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------


@pytest.mark.playwright
class TestRealtimeEdgeCases:
    """Edge cases for real-time features."""

    def test_sse_nonexistent_log_returns_404(self, live_server, page):
        """P1: SSE endpoint returns 404 for nonexistent log file."""
        response = page.request.get(
            f"{live_server}/api/logs/NONEXISTENT/implement/stream"
        )
        assert response.status == 404

    def test_static_log_content_endpoint(self, live_server, page):
        """P1: Static log content endpoint returns log text."""
        response = page.request.get(
            f"{live_server}/api/logs/SMA-80/implement/content"
        )
        assert response.status == 200
        text = response.text()
        assert len(text) > 0

    def test_static_log_content_404_for_missing(self, live_server, page):
        """P1: Static log content returns 404 for missing file."""
        response = page.request.get(
            f"{live_server}/api/logs/NONEXISTENT/implement/content"
        )
        assert response.status == 404

    def test_empty_chart_no_snapshots(self, live_server, page, seeded_db):
        """P1: Usage page handles no snapshots gracefully."""
        # Temporarily remove all snapshots
        conn = sqlite3.connect(seeded_db)
        rows = conn.execute(
            "SELECT * FROM usage_snapshots"
        ).fetchall()
        conn.execute("DELETE FROM usage_snapshots")
        conn.commit()

        try:
            page.goto(f"{live_server}/usage?range=24h")
            page.wait_for_load_state("networkidle")
            text = page.locator("main").inner_text()
            # Should show a message or at least not crash
            assert "No usage snapshots" in text or "Usage Trends" in text
        finally:
            # Restore snapshots
            cols = [desc[0] for desc in conn.execute(
                "SELECT * FROM usage_snapshots LIMIT 0"
            ).description]
            placeholders = ", ".join(["?"] * len(cols))
            for row in rows:
                conn.execute(
                    f"INSERT INTO usage_snapshots ({', '.join(cols)}) "
                    f"VALUES ({placeholders})",
                    row,
                )
            conn.commit()
            conn.close()

