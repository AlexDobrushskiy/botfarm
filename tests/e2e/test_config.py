"""E2E tests for the Configuration page (/config) — interactive features.

Tests cover: View tab content, Edit tab form fields, saving config changes,
validation errors, and structural change restart banners.
"""

import pytest


@pytest.mark.playwright
class TestConfigViewTab:
    """Configuration View tab — read-only display."""

    def test_page_title(self, live_server, page):
        """P0: Page title is correct."""
        page.goto(f"{live_server}/config")
        assert page.title() == "Configuration - Botfarm"

    def test_heading(self, live_server, page):
        """P0: Page has Configuration heading."""
        page.goto(f"{live_server}/config")
        assert page.locator("h1", has_text="Configuration").is_visible()

    def test_view_tab_shows_config_sections(self, live_server, page):
        """P0: View tab displays all config sections."""
        page.goto(f"{live_server}/config")
        view = page.locator("#tab-view")
        text = view.inner_text()
        for section in ["Projects", "Linear", "Agents", "Usage Limits",
                        "Notifications", "Dashboard", "Database"]:
            assert section in text, f"Section '{section}' not found in View tab"

    def test_view_tab_shows_project_details(self, live_server, page):
        """P1: Project cards display name, team, slots."""
        page.goto(f"{live_server}/config")
        view = page.locator("#tab-view")
        text = view.inner_text()
        assert "bot-farm" in text
        assert "Smart AI Coach" in text

    def test_api_key_is_masked(self, live_server, page):
        """P0: API key is masked, not shown in plaintext."""
        page.goto(f"{live_server}/config")
        view = page.locator("#tab-view")
        text = view.inner_text()
        # The full key should not appear; masked version should have ****
        assert "lin_api_1234567890abcdef1234567890abcdef" not in text
        assert "****" in text

    def test_view_tab_shows_linear_settings(self, live_server, page):
        """P1: Linear section shows poll interval and comment flags."""
        page.goto(f"{live_server}/config")
        view = page.locator("#tab-view")
        text = view.inner_text()
        assert "Poll interval" in text
        assert "Comment on failure" in text
        assert "Comment on completion" in text

    def test_view_tab_shows_usage_limits(self, live_server, page):
        """P1: Usage limits section shows enabled state and thresholds."""
        page.goto(f"{live_server}/config")
        view = page.locator("#tab-view")
        text = view.inner_text()
        assert "5-hour pause threshold" in text
        assert "7-day pause threshold" in text
        assert "0.85" in text
        assert "0.9" in text


@pytest.mark.playwright
class TestConfigTabSwitching:
    """Tab switching between View and Edit."""

    def test_tabs_exist(self, live_server, page):
        """P0: View and Edit tab buttons exist."""
        page.goto(f"{live_server}/config")
        tabs = page.locator(".config-tab")
        assert tabs.count() >= 2
        tab_texts = tabs.all_text_contents()
        assert "View" in tab_texts
        assert "Edit" in tab_texts

    def test_view_tab_active_by_default(self, live_server, page):
        """P0: View tab is active by default."""
        page.goto(f"{live_server}/config")
        view_tab = page.locator(".config-tab").first
        assert "active" in view_tab.get_attribute("class")
        assert page.locator("#tab-view").is_visible()
        assert not page.locator("#tab-edit").is_visible()

    def test_switch_to_edit_tab(self, live_server, page):
        """P0: Clicking Edit tab shows edit content and hides view."""
        page.goto(f"{live_server}/config")
        edit_tab = page.locator(".config-tab").nth(1)
        edit_tab.click()
        assert page.locator("#tab-edit").is_visible()
        assert not page.locator("#tab-view").is_visible()

    def test_switch_back_to_view(self, live_server, page):
        """P0: Switching back to View tab works."""
        page.goto(f"{live_server}/config")
        tabs = page.locator(".config-tab")
        tabs.nth(1).click()
        tabs.first.click()
        assert page.locator("#tab-view").is_visible()
        assert not page.locator("#tab-edit").is_visible()


@pytest.mark.playwright
class TestConfigEditTab:
    """Edit tab — form fields and sections."""

    def test_edit_tab_has_linear_form(self, live_server, page):
        """P0: Edit tab shows Linear section with poll interval and switches."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()
        edit = page.locator("#tab-edit")
        assert edit.locator("#linear-poll_interval_seconds").is_visible()
        assert edit.locator("#linear-comment_on_failure").is_visible()
        assert edit.locator("#linear-comment_on_completion").is_visible()
        assert edit.locator("#linear-comment_on_limit_pause").is_visible()

    def test_edit_tab_has_usage_limits_form(self, live_server, page):
        """P0: Edit tab shows Usage Limits section with enable toggle and thresholds."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()
        edit = page.locator("#tab-edit")
        assert edit.locator("#usage_limits-enabled").is_visible()
        assert edit.locator("#usage_limits-pause_five_hour_threshold").is_visible()
        assert edit.locator("#usage_limits-pause_seven_day_threshold").is_visible()

    def test_edit_tab_has_agents_form(self, live_server, page):
        """P0: Edit tab shows Agents section with iterations and timeout fields."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()
        edit = page.locator("#tab-edit")
        assert edit.locator("#agents-max_review_iterations").is_visible()
        assert edit.locator("#agents-max_ci_retries").is_visible()
        assert edit.locator("#agents-timeout_grace_seconds").is_visible()

    def test_edit_tab_has_projects_section(self, live_server, page):
        """P0: Edit tab shows project cards with slot chips."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()
        edit = page.locator("#tab-edit")
        project_cards = edit.locator(".project-card")
        assert project_cards.count() >= 1
        # Should have slot chips
        assert edit.locator(".slot-chip").count() >= 1

    def test_edit_tab_has_notifications_form(self, live_server, page):
        """P1: Edit tab shows Notifications section."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()
        edit = page.locator("#tab-edit")
        assert edit.locator("#notifications-webhook_url").is_visible()
        assert edit.locator("#notifications-webhook_format").is_visible()
        assert edit.locator("#notifications-rate_limit_seconds").is_visible()

    def test_edit_fields_have_current_values(self, live_server, page):
        """P0: Edit form fields are pre-populated with current config values."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()
        # Poll interval should be 30
        poll = page.locator("#linear-poll_interval_seconds")
        assert poll.input_value() == "30"
        # Max review iterations should be 3
        max_review = page.locator("#agents-max_review_iterations")
        assert max_review.input_value() == "3"


@pytest.mark.playwright
class TestConfigSave:
    """Saving configuration changes — success and error paths."""

    def test_save_runtime_field_shows_success(self, live_server, page):
        """P0: Saving a runtime field shows success feedback."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()

        # Change poll interval
        poll = page.locator("#linear-poll_interval_seconds")
        poll.fill("45")

        # Submit the Linear section form
        page.locator("#tab-edit form").first.locator(
            "button[type='submit']"
        ).click()

        # Wait for the AJAX response
        feedback = page.locator("#feedback-linear .config-feedback.success")
        feedback.wait_for(state="visible", timeout=5000)
        assert feedback.is_visible()

        # Restore original value
        poll.fill("30")
        page.locator("#tab-edit form").first.locator(
            "button[type='submit']"
        ).click()
        page.wait_for_timeout(500)

    def test_save_validation_error_shown(self, live_server, page):
        """P0: Invalid value shows validation error feedback."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()

        # Remove HTML5 min attribute to bypass browser validation,
        # then set an invalid value (0, below server-side min of 1).
        poll = page.locator("#linear-poll_interval_seconds")
        page.evaluate(
            "document.getElementById('linear-poll_interval_seconds')"
            ".removeAttribute('min')"
        )
        poll.fill("0")

        # Submit
        page.locator("#tab-edit form").first.locator(
            "button[type='submit']"
        ).click()

        # Should show error feedback
        feedback = page.locator("#feedback-linear .config-feedback.error")
        feedback.wait_for(state="visible", timeout=5000)
        assert feedback.is_visible()

        # Restore valid value
        poll.fill("30")

    def test_structural_change_shows_restart_banner(self, live_server, page):
        """P0: Saving a structural change (notifications) shows restart banner."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()

        # Find the notifications form and change rate limit
        rate_input = page.locator("#notifications-rate_limit_seconds")
        original = rate_input.input_value()
        rate_input.fill("600")

        # Submit the notifications form
        page.locator(
            "#tab-edit form", has=page.locator("#notifications-webhook_url")
        ).locator("button[type='submit']").click()

        # Wait for response and check for restart banner
        banner = page.locator("#restart-banner")
        banner.wait_for(state="visible", timeout=5000)
        assert "restart required" in banner.inner_text().lower()

        # Restore original value and submit the form
        rate_input.fill(original)
        page.locator(
            "#tab-edit form", has=page.locator("#notifications-webhook_url")
        ).locator("button[type='submit']").click()
        page.wait_for_timeout(500)

    def test_multiple_field_changes_in_one_save(self, live_server, page):
        """P1: Changing multiple fields and saving works."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()

        # Change multiple agent settings
        max_review = page.locator("#agents-max_review_iterations")
        max_ci = page.locator("#agents-max_ci_retries")
        orig_review = max_review.input_value()
        orig_ci = max_ci.input_value()
        max_review.fill("5")
        max_ci.fill("3")

        # Submit agents form
        page.locator(
            "#tab-edit form", has=page.locator("#agents-max_review_iterations")
        ).locator("button[type='submit']").click()

        feedback = page.locator("#feedback-agents .config-feedback.success")
        feedback.wait_for(state="visible", timeout=5000)
        assert feedback.is_visible()

        # Restore
        max_review.fill(orig_review)
        max_ci.fill(orig_ci)
        page.locator(
            "#tab-edit form", has=page.locator("#agents-max_review_iterations")
        ).locator("button[type='submit']").click()
        page.wait_for_timeout(500)


@pytest.mark.playwright
class TestConfigSlotManagement:
    """Slot chip add/remove in edit tab."""

    def test_remove_slot_chip(self, live_server, page):
        """P1: Clicking x on a slot chip removes it from the UI."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()

        chips_container = page.locator("#slots-0")
        initial_count = chips_container.locator(".slot-chip").count()
        assert initial_count > 0

        # Click the remove button on the first chip
        chips_container.locator(".slot-chip button").first.click()
        assert chips_container.locator(".slot-chip").count() == initial_count - 1

    def test_add_slot(self, live_server, page):
        """P1: Adding a new slot number creates a chip."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()

        chips_container = page.locator("#slots-0")
        initial_count = chips_container.locator(".slot-chip").count()

        # Add slot 99
        page.locator("#new-slot-0").fill("99")
        page.locator("#tab-edit .add-slot-row button").first.click()
        assert chips_container.locator(".slot-chip").count() == initial_count + 1

    def test_duplicate_slot_rejected(self, live_server, page):
        """P2: Adding a duplicate slot number is rejected."""
        page.goto(f"{live_server}/config")
        page.locator(".config-tab").nth(1).click()

        chips_container = page.locator("#slots-0")
        # Get the first existing slot number
        first_chip = chips_container.locator(".slot-chip").first
        existing_slot = first_chip.get_attribute("data-slot")

        initial_count = chips_container.locator(".slot-chip").count()
        page.locator("#new-slot-0").fill(existing_slot)
        page.locator("#tab-edit .add-slot-row button").first.click()

        # Count should not increase
        assert chips_container.locator(".slot-chip").count() == initial_count
        # Input should be marked invalid
        assert page.locator("#new-slot-0").get_attribute("aria-invalid") == "true"
