"""E2E tests for the Task Detail page (/task/{id})."""

import pytest


@pytest.mark.playwright
class TestTaskDetailCompleted:
    """Task detail page for a completed task (SMA-80)."""

    def test_page_title(self, live_server, page):
        """P0: Page title includes ticket ID."""
        page.goto(f"{live_server}/task/SMA-80")
        assert "SMA-80" in page.title()

    def test_header_with_ticket_link(self, live_server, page):
        """P0: Header shows ticket ID as a link."""
        page.goto(f"{live_server}/task/SMA-80")
        link = page.locator("h1 a", has_text="SMA-80")
        assert link.is_visible()
        assert link.get_attribute("target") == "_blank"

    def test_header_with_title(self, live_server, page):
        """P0: Header shows task title."""
        page.goto(f"{live_server}/task/SMA-80")
        heading = page.locator("h1")
        assert "Add config validation" in heading.inner_text()

    def test_summary_cards_render(self, live_server, page):
        """P0: Summary cards grid renders with expected cards."""
        page.goto(f"{live_server}/task/SMA-80")
        cards = page.locator(".metric-card")
        assert cards.count() >= 6  # Status, Wall Time, Turns, Reviews, Limit, Cost, Context

    def test_status_card_completed(self, live_server, page):
        """P0: Status card shows 'completed' in green."""
        page.goto(f"{live_server}/task/SMA-80")
        status_span = page.locator(".metric-card .status-free")
        assert status_span.is_visible()
        assert "completed" in status_span.inner_text()

    def test_turns_displayed(self, live_server, page):
        """P0: Turns count is displayed."""
        page.goto(f"{live_server}/task/SMA-80")
        # SMA-80 has 65 turns
        text = page.locator("main").inner_text()
        assert "65" in text

    def test_review_iterations_displayed(self, live_server, page):
        """P0: Review iterations count is displayed."""
        page.goto(f"{live_server}/task/SMA-80")
        # SMA-80 has 2 review iterations
        cards_text = page.locator(".grid").first.inner_text()
        assert "2" in cards_text

    def test_cost_displayed_with_dollar(self, live_server, page):
        """P0: Total cost is displayed with $ sign."""
        page.goto(f"{live_server}/task/SMA-80")
        text = page.locator("main").inner_text()
        assert "$" in text

    def test_pipeline_stepper_completed(self, live_server, page):
        """P0: All pipeline nodes show green checkmark for completed task."""
        page.goto(f"{live_server}/task/SMA-80")
        stepper = page.locator(".pipeline-stepper")
        assert stepper.is_visible()
        completed_nodes = stepper.locator(".pipeline-node-completed")
        assert completed_nodes.count() == 5  # All 5 stages completed

    def test_pipeline_connectors_completed(self, live_server, page):
        """P0: Pipeline connectors are green for completed stages."""
        page.goto(f"{live_server}/task/SMA-80")
        stepper = page.locator(".pipeline-stepper")
        completed_connectors = stepper.locator(".pipeline-connector-completed")
        assert completed_connectors.count() == 4  # 4 connectors between 5 stages

    def test_pipeline_labels(self, live_server, page):
        """P0: Pipeline stage labels are shown."""
        page.goto(f"{live_server}/task/SMA-80")
        labels = page.locator(".pipeline-label").all_text_contents()
        assert "implement" in labels
        assert "review" in labels
        assert "merge" in labels

    def test_pipeline_iteration_badge(self, live_server, page):
        """P1: Review stage shows iteration badge (x2) for SMA-80."""
        page.goto(f"{live_server}/task/SMA-80")
        badges = page.locator(".pipeline-badge")
        badge_texts = badges.all_text_contents()
        assert any("\u00d72" in t for t in badge_texts)  # ×2

    def test_task_info_section(self, live_server, page):
        """P1: Task info section shows project, slot, timestamps."""
        page.goto(f"{live_server}/task/SMA-80")
        info = page.locator("details", has_text="Task Info")
        assert info.is_visible()
        text = info.inner_text()
        assert "bot-farm" in text
        assert "Project" in text

    def test_task_info_pr_link(self, live_server, page):
        """P1: Task info shows PR URL."""
        page.goto(f"{live_server}/task/SMA-80")
        info = page.locator("details", has_text="Task Info")
        pr_link = info.locator("a", has_text="github.com")
        assert pr_link.count() >= 1

    def test_stage_runs_table(self, live_server, page):
        """P1: Stage runs table lists all stage runs."""
        page.goto(f"{live_server}/task/SMA-80")
        stage_detail = page.locator("details", has_text="Stage Runs")
        assert stage_detail.is_visible()
        rows = stage_detail.locator("table tbody tr")
        # SMA-80 has 6 stage runs
        assert rows.count() == 6

    def test_stage_runs_columns(self, live_server, page):
        """P1: Stage runs table has expected columns."""
        page.goto(f"{live_server}/task/SMA-80")
        stage_detail = page.locator("details", has_text="Stage Runs")
        headers = stage_detail.locator("thead th").all_text_contents()
        assert "Stage" in headers
        assert "Iteration" in headers
        assert "Duration" in headers
        assert "Context Fill" in headers
        assert "Cost" in headers

    def test_stage_runs_log_links(self, live_server, page):
        """P1: Each stage run has a log 'view' link."""
        page.goto(f"{live_server}/task/SMA-80")
        stage_detail = page.locator("details", has_text="Stage Runs")
        view_links = stage_detail.locator("a", has_text="view")
        assert view_links.count() >= 1

    def test_event_log_table(self, live_server, page):
        """P1: Event log table shows task events."""
        page.goto(f"{live_server}/task/SMA-80")
        event_detail = page.locator("details", has_text="Event Log")
        assert event_detail.is_visible()
        rows = event_detail.locator("table tbody tr")
        assert rows.count() >= 5

    def test_back_link_to_history(self, live_server, page):
        """P1: Back link navigates to history."""
        page.goto(f"{live_server}/task/SMA-80")
        back = page.locator("a", has_text="Back to Task History")
        assert back.is_visible()
        assert back.get_attribute("href") == "/history"

    def test_no_live_output_for_completed(self, live_server, page):
        """P1: Completed task has no LIVE badge."""
        page.goto(f"{live_server}/task/SMA-80")
        live_badges = page.locator(".log-live-badge")
        assert live_badges.count() == 0


@pytest.mark.playwright
class TestTaskDetailFailed:
    """Task detail page for a failed task (SMA-99)."""

    def test_status_card_failed(self, live_server, page):
        """P0: Status card shows 'failed' in red."""
        page.goto(f"{live_server}/task/SMA-99")
        status = page.locator(".metric-card .status-failed")
        assert status.is_visible()
        assert "failed" in status.inner_text()

    def test_pipeline_stepper_failed(self, live_server, page):
        """P0: Failed pipeline shows failed node."""
        page.goto(f"{live_server}/task/SMA-99")
        stepper = page.locator(".pipeline-stepper")
        assert stepper.is_visible()
        failed_nodes = stepper.locator(".pipeline-node-failed")
        assert failed_nodes.count() >= 1

    def test_failure_reason_shown(self, live_server, page):
        """P1: Task info section shows failure reason."""
        page.goto(f"{live_server}/task/SMA-99")
        info = page.locator("details", has_text="Task Info")
        text = info.inner_text()
        assert "Failure Reason" in text


@pytest.mark.playwright
class TestTaskDetailPending:
    """Task detail page for a pending/active task (SMA-101).

    Note: SMA-101 has task status 'pending' in the DB — the slot is busy
    but task records use 'pending' until completion/failure.
    """

    def test_status_card_pending(self, live_server, page):
        """P0: Status card shows 'pending' for active task."""
        page.goto(f"{live_server}/task/SMA-101")
        cards_text = page.locator(".grid").first.inner_text()
        assert "pending" in cards_text

    def test_pipeline_stepper_active(self, live_server, page):
        """P0: Active pipeline shows pulsing active node."""
        page.goto(f"{live_server}/task/SMA-101")
        stepper = page.locator(".pipeline-stepper")
        assert stepper.is_visible()
        active = stepper.locator(".pipeline-node-active")
        assert active.count() >= 1

    def test_task_has_stage_runs(self, live_server, page):
        """P1: Task has at least one stage run recorded."""
        page.goto(f"{live_server}/task/SMA-101")
        stage_detail = page.locator("details", has_text="Stage Runs")
        assert stage_detail.is_visible()
        rows = stage_detail.locator("table tbody tr")
        assert rows.count() >= 1


@pytest.mark.playwright
class TestTaskDetailExtraUsage:
    """Task detail for a task with extra usage (SMA-85)."""

    def test_extra_usage_cost_card(self, live_server, page):
        """P1: Extra usage cost card is visible."""
        page.goto(f"{live_server}/task/SMA-85")
        text = page.locator("main").inner_text()
        assert "Extra Usage Cost" in text

    def test_extra_usage_warning(self, live_server, page):
        """P1: Extra usage warning note is displayed."""
        page.goto(f"{live_server}/task/SMA-85")
        text = page.locator("main").inner_text()
        assert "extra usage" in text.lower()

    def test_limit_restart_badge(self, live_server, page):
        """P1: Pipeline shows limit restart badge."""
        page.goto(f"{live_server}/task/SMA-85")
        limit_badges = page.locator(".pipeline-badge-limit")
        assert limit_badges.count() >= 1


@pytest.mark.playwright
class TestTaskDetailNotFound:
    """Task detail for nonexistent task."""

    def test_task_not_found(self, live_server, page):
        """P1: Nonexistent task shows 'not found' message."""
        page.goto(f"{live_server}/task/NONEXISTENT-999")
        text = page.locator("main").inner_text()
        assert "not found" in text.lower()

    def test_back_link_on_not_found(self, live_server, page):
        """P1: Not found page has back link to history."""
        page.goto(f"{live_server}/task/NONEXISTENT-999")
        back = page.locator("a", has_text="Back to Task History")
        assert back.is_visible()
