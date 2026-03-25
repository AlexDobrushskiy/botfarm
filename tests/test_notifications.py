"""Tests for botfarm.notifications — webhook notifications."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import NotificationsConfig
from botfarm.notifications import Notifier, _detect_format


# ---------------------------------------------------------------------------
# _detect_format
# ---------------------------------------------------------------------------


class TestDetectFormat:
    def test_slack_default(self):
        assert _detect_format("https://hooks.slack.com/services/xxx", "slack") == "slack"

    def test_explicit_slack_respected_even_for_discord_url(self):
        assert _detect_format("https://discord.com/api/webhooks/xxx", "slack") == "slack"

    def test_discord_url_auto_detect_when_no_format(self):
        assert _detect_format("https://discord.com/api/webhooks/xxx", "") == "discord"

    def test_discordapp_url_auto_detect_when_no_format(self):
        assert _detect_format("https://discordapp.com/api/webhooks/xxx", "") == "discord"

    def test_explicit_discord_overrides(self):
        assert _detect_format("https://hooks.slack.com/services/xxx", "discord") == "discord"

    def test_empty_format_defaults_slack(self):
        assert _detect_format("https://example.com/webhook", "") == "slack"


# ---------------------------------------------------------------------------
# Notifier — disabled (no URL)
# ---------------------------------------------------------------------------


class TestNotifierDisabled:
    def test_not_enabled_when_no_url(self):
        n = Notifier(NotificationsConfig(webhook_url=""))
        assert not n.enabled
        n.close()

    def test_send_noop_when_disabled(self):
        n = Notifier(NotificationsConfig(webhook_url=""))
        # Should not raise
        n.notify_task_completed(ticket_id="X-1", title="t")
        n.notify_task_failed(ticket_id="X-1", title="t")
        n.notify_limit_hit(reason="test")
        n.notify_limit_cleared()
        n.notify_capacity_warning(count=700, limit=1000, percentage=70.0)
        n.notify_capacity_critical(count=850, limit=1000, percentage=85.0)
        n.notify_capacity_blocked(count=950, limit=1000, percentage=95.0)
        n.notify_capacity_cleared(count=890, limit=1000, percentage=89.0)
        n.notify_all_idle()
        n.notify_supervisor_shutdown(reason="test")
        n.notify_refactoring_all_clear(
            month="March", year=2026,
            ticket_url="https://linear.app/test/issue/X-1",
        )
        n.notify_refactoring_action_needed(
            month="March", year=2026, num_tickets=2,
            parent_ticket_id="X-1", brief_list="test",
            ticket_url="https://linear.app/test/issue/X-1",
        )
        n.notify_human_blocker(
            blocker_id="X-1",
            blocker_title="Manual step",
            blocked_tickets=["X-2", "X-3"],
        )
        n.notify_auth_failure(consecutive_failures=5, minutes_since_success=30)
        n.notify_auth_recovered()
        n.close()


# ---------------------------------------------------------------------------
# Notifier — enabled
# ---------------------------------------------------------------------------


class TestNotifierSlack:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
            rate_limit_seconds=300,
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_task_completed_sends_slack_payload(self, notifier):
        notifier.notify_task_completed(
            ticket_id="SMA-42",
            title="Add widget",
            duration_seconds=3600,
            pr_url="https://github.com/org/repo/pull/99",
        )
        notifier._client.post.assert_called_once()
        args, kwargs = notifier._client.post.call_args
        assert args[0] == "https://hooks.slack.com/services/xxx"
        payload = kwargs["json"]
        assert "text" in payload
        assert "SMA-42" in payload["text"]
        assert "60m" in payload["text"]
        assert "pull/99" in payload["text"]

    def test_task_completed_minimal(self, notifier):
        notifier.notify_task_completed(ticket_id="SMA-1", title="Simple")
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "SMA-1" in payload["text"]
        assert "Simple" in payload["text"]

    def test_task_completed_short_duration(self, notifier):
        notifier.notify_task_completed(
            ticket_id="SMA-1", title="Quick", duration_seconds=45,
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "45s" in payload["text"]

    def test_task_failed_sends_reason(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-5",
            title="Broken",
            failure_reason="implement: tests failed",
        )
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "SMA-5" in payload["text"]
        assert "failed" in payload["text"].lower()
        assert "tests failed" in payload["text"]

    def test_limit_hit_sends_reason(self, notifier):
        notifier.notify_limit_hit(
            reason="5h utilization 90%",
            resume_after="2025-01-01T12:00:00",
        )
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "limit" in payload["text"].lower()
        assert "5h utilization 90%" in payload["text"]
        assert "2025-01-01T12:00:00" in payload["text"]

    def test_limit_cleared(self, notifier):
        notifier.notify_limit_cleared()
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "cleared" in payload["text"].lower()

    def test_all_idle(self, notifier):
        notifier.notify_all_idle()
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "idle" in payload["text"].lower()

    def test_supervisor_shutdown_sends_reason(self, notifier):
        notifier.notify_supervisor_shutdown(reason="SIGTERM/SIGINT received")
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "shut down" in payload["text"].lower()
        assert "SIGTERM/SIGINT received" in payload["text"]
        assert "Workers may still be running" in payload["text"]

    def test_supervisor_shutdown_not_rate_limited(self, notifier):
        notifier.notify_supervisor_shutdown(reason="unexpected error")
        notifier.notify_supervisor_shutdown(reason="unexpected error")
        assert notifier._client.post.call_count == 2


# ---------------------------------------------------------------------------
# Notifier — capacity threshold notifications
# ---------------------------------------------------------------------------


class TestCapacityNotifications:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
            rate_limit_seconds=300,
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_capacity_warning_sends_percentage_and_counts(self, notifier):
        notifier.notify_capacity_warning(count=700, limit=1000, percentage=70.0)
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "70%" in payload["text"]
        assert "700/1000" in payload["text"]
        assert "warning" in payload["text"].lower()

    def test_capacity_warning_includes_actionable_text(self, notifier):
        notifier.notify_capacity_warning(count=700, limit=1000, percentage=70.0)
        payload = notifier._client.post.call_args[1]["json"]
        assert "Archive completed issues" in payload["text"]

    def test_capacity_warning_is_rate_limited(self, notifier):
        notifier.notify_capacity_warning(count=700, limit=1000, percentage=70.0)
        assert notifier._client.post.call_count == 1
        notifier.notify_capacity_warning(count=720, limit=1000, percentage=72.0)
        assert notifier._client.post.call_count == 1  # rate-limited

    def test_capacity_critical_sends_percentage_and_counts(self, notifier):
        notifier.notify_capacity_critical(count=850, limit=1000, percentage=85.0)
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "85%" in payload["text"]
        assert "850/1000" in payload["text"]
        assert "critical" in payload["text"].lower()

    def test_capacity_critical_includes_actionable_text(self, notifier):
        notifier.notify_capacity_critical(count=850, limit=1000, percentage=85.0)
        payload = notifier._client.post.call_args[1]["json"]
        assert "Archive completed issues" in payload["text"]

    def test_capacity_critical_is_rate_limited(self, notifier):
        notifier.notify_capacity_critical(count=850, limit=1000, percentage=85.0)
        assert notifier._client.post.call_count == 1
        notifier.notify_capacity_critical(count=860, limit=1000, percentage=86.0)
        assert notifier._client.post.call_count == 1  # rate-limited

    def test_capacity_blocked_sends_percentage_and_counts(self, notifier):
        notifier.notify_capacity_blocked(count=950, limit=1000, percentage=95.0)
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "95%" in payload["text"]
        assert "950/1000" in payload["text"]
        assert "blocked" in payload["text"].lower()
        assert "dispatch paused" in payload["text"]

    def test_capacity_blocked_includes_actionable_text(self, notifier):
        notifier.notify_capacity_blocked(count=950, limit=1000, percentage=95.0)
        payload = notifier._client.post.call_args[1]["json"]
        assert "Archive completed issues" in payload["text"]

    def test_capacity_blocked_not_rate_limited(self, notifier):
        """Blocked is critical — always sent."""
        notifier.notify_capacity_blocked(count=950, limit=1000, percentage=95.0)
        notifier.notify_capacity_blocked(count=955, limit=1000, percentage=95.5)
        assert notifier._client.post.call_count == 2

    def test_capacity_cleared_sends_percentage_and_counts(self, notifier):
        notifier.notify_capacity_cleared(count=890, limit=1000, percentage=89.0)
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "89%" in payload["text"]
        assert "890/1000" in payload["text"]
        assert "cleared" in payload["text"].lower()
        assert "dispatch resumed" in payload["text"]

    def test_capacity_cleared_not_rate_limited(self, notifier):
        """Cleared is a state change — always sent."""
        notifier.notify_capacity_cleared(count=890, limit=1000, percentage=89.0)
        notifier.notify_capacity_cleared(count=880, limit=1000, percentage=88.0)
        assert notifier._client.post.call_count == 2

    def test_capacity_notifications_discord_format(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://discord.com/api/webhooks/xxx",
            webhook_format="discord",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response

        n.notify_capacity_warning(count=700, limit=1000, percentage=70.0)
        payload = n._client.post.call_args[1]["json"]
        assert "content" in payload
        assert "text" not in payload
        assert "700/1000" in payload["content"]

        n.close()


class TestNotifierDiscord:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://discord.com/api/webhooks/xxx",
            webhook_format="discord",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_discord_payload_uses_content_key(self, notifier):
        notifier.notify_task_completed(ticket_id="SMA-1", title="Test")
        payload = notifier._client.post.call_args[1]["json"]
        assert "content" in payload
        assert "text" not in payload


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class TestRateLimiting:
    def test_rate_limited_event_not_sent_twice(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            rate_limit_seconds=300,
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response

        # First call should send
        n.notify_limit_hit(reason="test")
        assert n._client.post.call_count == 1

        # Second call should be rate-limited
        n.notify_limit_hit(reason="test again")
        assert n._client.post.call_count == 1

        n.close()

    def test_non_rate_limited_events_always_send(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            rate_limit_seconds=300,
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response

        n.notify_task_completed(ticket_id="SMA-1", title="A")
        n.notify_task_completed(ticket_id="SMA-2", title="B")
        assert n._client.post.call_count == 2

        n.close()

    def test_rate_limit_expires(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            rate_limit_seconds=0,  # Expire immediately
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response

        n.notify_limit_hit(reason="test")
        n.notify_limit_hit(reason="test")
        # Both should have been sent since rate_limit_seconds=0
        assert n._client.post.call_count == 2

        n.close()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_post_failure_does_not_raise(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
        ))
        n._client = MagicMock()
        n._client.post.side_effect = Exception("connection refused")

        # Should not raise
        n.notify_task_completed(ticket_id="SMA-1", title="Test")
        n.close()

    def test_raise_for_status_failure_does_not_raise(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status.side_effect = Exception("404 Not Found")
        n._client.post.return_value = response

        # Should not raise
        n.notify_task_failed(ticket_id="SMA-1", title="Test")
        n.close()

    def test_rate_limited_event_not_retried_on_failure(self):
        """_last_sent is updated even on failure to prevent retry storms."""
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            rate_limit_seconds=300,
        ))
        n._client = MagicMock()
        n._client.post.side_effect = Exception("connection refused")

        # First call fails
        n.notify_limit_hit(reason="test")
        assert n._client.post.call_count == 1

        # Second call should still be rate-limited despite the first failure
        n.notify_limit_hit(reason="test")
        assert n._client.post.call_count == 1

        n.close()


# ---------------------------------------------------------------------------
# Review summary in notifications (Codex reviewer verdicts)
# ---------------------------------------------------------------------------


class TestNotificationWithCodexVerdict:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_task_completed_includes_review_summary(self, notifier):
        notifier.notify_task_completed(
            ticket_id="SMA-42",
            title="Add feature",
            review_summary="Review: Claude APPROVED, Codex APPROVED",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "Review: Claude APPROVED, Codex APPROVED" in payload["text"]
        assert "SMA-42" in payload["text"]

    def test_task_completed_review_summary_with_changes_requested(self, notifier):
        notifier.notify_task_completed(
            ticket_id="SMA-10",
            title="Fix bug",
            review_summary="Review: Claude APPROVED, Codex CHANGES_REQUESTED",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "Claude APPROVED" in payload["text"]
        assert "Codex CHANGES_REQUESTED" in payload["text"]

    def test_task_failed_includes_review_summary(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-5",
            title="Broken",
            failure_reason="review: max iterations",
            review_summary="Review: Claude APPROVED, Codex CHANGES_REQUESTED",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "Review: Claude APPROVED, Codex CHANGES_REQUESTED" in payload["text"]
        assert "max iterations" in payload["text"]

    def test_task_failed_codex_failed_summary(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-7",
            title="Issue",
            review_summary="Review: Claude APPROVED, Codex FAILED (fell back to Claude-only)",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "Codex FAILED" in payload["text"]
        assert "fell back to Claude-only" in payload["text"]


class TestNotificationWithoutCodex:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_task_completed_no_review_summary(self, notifier):
        notifier.notify_task_completed(
            ticket_id="SMA-42",
            title="Add feature",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "SMA-42" in payload["text"]
        assert "Review:" not in payload["text"]
        assert "Codex" not in payload["text"]

    def test_task_failed_no_review_summary(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-5",
            title="Broken",
            failure_reason="implement: tests failed",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "tests failed" in payload["text"]
        assert "Review:" not in payload["text"]
        assert "Codex" not in payload["text"]

    def test_task_failed_env_category_adds_label(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-10",
            title="Broken env",
            failure_reason="ModuleNotFoundError: No module named 'flask'",
            failure_category="env_missing_package",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "Environment: missing package/module" in payload["text"]
        assert "no auto-retry" in payload["text"]

    def test_task_failed_code_failure_no_env_label(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-11",
            title="Code bug",
            failure_reason="tests failed",
            failure_category="code_failure",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "Environment:" not in payload["text"]
        assert "no auto-retry" not in payload["text"]

    def test_task_failed_no_category_no_env_label(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-12",
            title="Old task",
            failure_reason="tests failed",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "Environment:" not in payload["text"]

    def test_task_failed_auth_failure_label_and_hint(self, notifier):
        notifier.notify_task_failed(
            ticket_id="SMA-20",
            title="Auth issue",
            failure_reason="HTTP 401 Unauthorized",
            failure_category="auth_failure",
        )
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "Authentication: Claude subprocess auth error" in text
        assert "check oauth token" in text.lower()
        assert "no auto-retry" not in text

    def test_task_completed_none_review_summary_unchanged(self, notifier):
        notifier.notify_task_completed(
            ticket_id="SMA-1",
            title="Simple",
            review_summary=None,
        )
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "SMA-1" in text
        assert "Review:" not in text


# ---------------------------------------------------------------------------
# Refactoring analysis notifications
# ---------------------------------------------------------------------------


class TestRefactoringAnalysisNotifications:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_all_clear_message_format(self, notifier):
        notifier.notify_refactoring_all_clear(
            month="March",
            year=2026,
            ticket_url="https://linear.app/test/issue/SMA-100",
        )
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "Refactoring Analysis (March 2026)" in text
        assert "no action needed" in text
        assert "https://linear.app/test/issue/SMA-100" in text

    def test_action_needed_message_format(self, notifier):
        notifier.notify_refactoring_action_needed(
            month="March",
            year=2026,
            num_tickets=3,
            parent_ticket_id="SMA-100",
            brief_list="duplicated auth logic, oversized utils module",
            ticket_url="https://linear.app/test/issue/SMA-100",
        )
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "Refactoring Analysis (March 2026)" in text
        assert "3 refactoring tickets" in text
        assert "SMA-100" in text
        assert "duplicated auth logic" in text
        assert "https://linear.app/test/issue/SMA-100" in text

    def test_all_clear_not_rate_limited(self, notifier):
        notifier.notify_refactoring_all_clear(
            month="March", year=2026,
            ticket_url="https://linear.app/test/issue/SMA-100",
        )
        notifier.notify_refactoring_all_clear(
            month="April", year=2026,
            ticket_url="https://linear.app/test/issue/SMA-101",
        )
        assert notifier._client.post.call_count == 2

    def test_action_needed_not_rate_limited(self, notifier):
        notifier.notify_refactoring_action_needed(
            month="March", year=2026, num_tickets=2,
            parent_ticket_id="SMA-100", brief_list="test",
            ticket_url="https://linear.app/test/issue/SMA-100",
        )
        notifier.notify_refactoring_action_needed(
            month="April", year=2026, num_tickets=1,
            parent_ticket_id="SMA-101", brief_list="test2",
            ticket_url="https://linear.app/test/issue/SMA-101",
        )
        assert notifier._client.post.call_count == 2

    def test_all_clear_discord_format(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://discord.com/api/webhooks/xxx",
            webhook_format="discord",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response

        n.notify_refactoring_all_clear(
            month="March", year=2026,
            ticket_url="https://linear.app/test/issue/SMA-100",
        )
        payload = n._client.post.call_args[1]["json"]
        assert "content" in payload
        assert "text" not in payload
        assert "no action needed" in payload["content"]

        n.close()

    def test_disabled_notifier_noop(self):
        n = Notifier(NotificationsConfig(webhook_url=""))
        # Should not raise
        n.notify_refactoring_all_clear(
            month="March", year=2026,
            ticket_url="https://linear.app/test/issue/SMA-100",
        )
        n.notify_refactoring_action_needed(
            month="March", year=2026, num_tickets=2,
            parent_ticket_id="SMA-100", brief_list="test",
            ticket_url="https://linear.app/test/issue/SMA-100",
        )
        n.close()


# ---------------------------------------------------------------------------
# Human blocker notifications
# ---------------------------------------------------------------------------


class TestHumanBlockerNotifications:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
            human_blocker_cooldown_seconds=3600,
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_message_format(self, notifier):
        notifier.notify_human_blocker(
            blocker_id="SMA-100",
            blocker_title="Create separate GitHub accounts",
            blocked_tickets=["SMA-101", "SMA-102"],
        )
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "Queue blocked by human action" in text
        assert "SMA-100" in text
        assert "Create separate GitHub accounts" in text
        assert "SMA-101" in text
        assert "SMA-102" in text

    def test_rate_limited_per_blocker(self, notifier):
        notifier.notify_human_blocker(
            blocker_id="SMA-100",
            blocker_title="Manual step",
            blocked_tickets=["SMA-101"],
        )
        assert notifier._client.post.call_count == 1
        # Same blocker — should be rate-limited
        notifier.notify_human_blocker(
            blocker_id="SMA-100",
            blocker_title="Manual step",
            blocked_tickets=["SMA-101"],
        )
        assert notifier._client.post.call_count == 1

    def test_different_blockers_not_rate_limited(self, notifier):
        notifier.notify_human_blocker(
            blocker_id="SMA-100",
            blocker_title="Step A",
            blocked_tickets=["SMA-101"],
        )
        notifier.notify_human_blocker(
            blocker_id="SMA-200",
            blocker_title="Step B",
            blocked_tickets=["SMA-201"],
        )
        assert notifier._client.post.call_count == 2

    def test_cooldown_expiry_allows_resend(self, notifier):
        notifier.notify_human_blocker(
            blocker_id="SMA-100",
            blocker_title="Manual step",
            blocked_tickets=["SMA-101"],
        )
        assert notifier._client.post.call_count == 1
        # Simulate cooldown expiry
        notifier._last_sent["human_blocker:SMA-100"] = time.monotonic() - 3601
        notifier.notify_human_blocker(
            blocker_id="SMA-100",
            blocker_title="Manual step",
            blocked_tickets=["SMA-101"],
        )
        assert notifier._client.post.call_count == 2

    def test_single_blocked_ticket(self, notifier):
        notifier.notify_human_blocker(
            blocker_id="SMA-50",
            blocker_title="Needs human input",
            blocked_tickets=["SMA-51"],
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "SMA-51" in payload["text"]
        assert "SMA-50" in payload["text"]


# ---------------------------------------------------------------------------
# Auth failure notifications
# ---------------------------------------------------------------------------


class TestAuthFailureNotifications:
    @pytest.fixture()
    def notifier(self):
        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
            rate_limit_seconds=300,
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_auth_failure_message_format(self, notifier):
        notifier.notify_auth_failure(
            consecutive_failures=5,
            minutes_since_success=45,
        )
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "auth failure" in text.lower()
        assert "5 consecutive 401" in text
        assert "45m ago" in text
        assert "OAuth token" in text

    def test_auth_failure_hours_format(self, notifier):
        notifier.notify_auth_failure(
            consecutive_failures=10,
            minutes_since_success=125,
        )
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "2h5m ago" in text

    def test_auth_failure_no_time_since_success(self, notifier):
        notifier.notify_auth_failure(
            consecutive_failures=3,
        )
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "3 consecutive 401" in text
        assert "Last successful poll" not in text

    def test_auth_failure_rate_limited_at_1_hour(self, notifier):
        notifier.notify_auth_failure(consecutive_failures=3)
        assert notifier._client.post.call_count == 1
        # Second call within the hour should be rate-limited
        notifier.notify_auth_failure(consecutive_failures=4)
        assert notifier._client.post.call_count == 1

    def test_auth_failure_resends_after_cooldown(self, notifier):
        notifier.notify_auth_failure(consecutive_failures=3)
        assert notifier._client.post.call_count == 1
        # Simulate 1 hour passing
        notifier._last_sent["auth_failure"] = time.monotonic() - 3601
        notifier.notify_auth_failure(consecutive_failures=6)
        assert notifier._client.post.call_count == 2

    def test_auth_recovered_message_format(self, notifier):
        notifier.notify_auth_recovered()
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        text = payload["text"]
        assert "auth recovered" in text.lower()
        assert "polling resumed" in text.lower()

    def test_auth_recovered_not_rate_limited(self, notifier):
        notifier.notify_auth_recovered()
        notifier.notify_auth_recovered()
        assert notifier._client.post.call_count == 2

    def test_auth_recovered_clears_failure_rate_limit(self, notifier):
        """Recovery should clear the auth_failure cooldown so a new outage
        gets an immediate notification."""
        notifier.notify_auth_failure(consecutive_failures=3)
        assert notifier._client.post.call_count == 1
        # Second failure is rate-limited
        notifier.notify_auth_failure(consecutive_failures=4)
        assert notifier._client.post.call_count == 1

        # Recovery clears the cooldown
        notifier.notify_auth_recovered()
        assert notifier._client.post.call_count == 2

        # New outage should get through immediately
        notifier.notify_auth_failure(consecutive_failures=3)
        assert notifier._client.post.call_count == 3

    def test_disabled_notifier_noop(self):
        n = Notifier(NotificationsConfig(webhook_url=""))
        n.notify_auth_failure(consecutive_failures=5, minutes_since_success=30)
        n.notify_auth_recovered()
        n.close()
