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
        n.notify_all_idle()
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
            cost_usd=1.50,
            duration_seconds=3600,
            pr_url="https://github.com/org/repo/pull/99",
        )
        notifier._client.post.assert_called_once()
        args, kwargs = notifier._client.post.call_args
        assert args[0] == "https://hooks.slack.com/services/xxx"
        payload = kwargs["json"]
        assert "text" in payload
        assert "SMA-42" in payload["text"]
        assert "$1.50" in payload["text"]
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
