"""Webhook notifications for key supervisor events.

Sends formatted messages to Slack or Discord webhooks on:
- Task completed
- Task failed
- Usage limit hit (dispatch paused)
- Usage limit cleared (dispatch resumed)
- Capacity warning (configurable warning threshold, default 70%)
- Capacity critical (configurable critical threshold, default 85%)
- Capacity blocked (configurable pause threshold, default 95% — dispatch paused)
- Capacity cleared (dropped below resume threshold — dispatch resumed)
- All slots idle

Notifications are non-blocking and fire-and-forget — failures never
affect supervisor operation.  Repeated limit_hit, capacity_warning,
and capacity_critical events are rate-limited to avoid spam during
frequent polling.
"""

from __future__ import annotations

import logging
import time

import httpx

from botfarm.config import NotificationsConfig

logger = logging.getLogger(__name__)


def _detect_format(url: str, configured_format: str) -> str:
    """Auto-detect webhook format from URL when not explicitly configured.

    If the user provided an explicit format, it is always respected.
    Auto-detection from URL only applies when no format was configured
    (empty string).
    """
    if configured_format:
        return configured_format
    if "discord.com" in url or "discordapp.com" in url:
        return "discord"
    return "slack"


class Notifier:
    """Sends webhook notifications for supervisor events.

    Thread-safe for use from the supervisor main loop.  All public
    methods catch exceptions internally — callers never need to
    handle errors.
    """

    def __init__(self, config: NotificationsConfig) -> None:
        self._config = config
        self._url = config.webhook_url
        self._format = _detect_format(self._url, config.webhook_format)
        self._last_sent: dict[str, float] = {}
        self._client = httpx.Client(timeout=10)

    @property
    def enabled(self) -> bool:
        return bool(self._url)

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Public event methods
    # ------------------------------------------------------------------

    def notify_task_completed(
        self,
        *,
        ticket_id: str,
        title: str,
        duration_seconds: float | None = None,
        pr_url: str | None = None,
        review_summary: str | None = None,
    ) -> None:
        """Notify that a task completed successfully."""
        lines = [f"*Task completed:* {ticket_id} — {title}"]
        details = []
        if duration_seconds is not None:
            minutes = int(duration_seconds) // 60
            if minutes > 0:
                details.append(f"Duration: {minutes}m")
            else:
                details.append(f"Duration: {int(duration_seconds)}s")
        if pr_url:
            details.append(f"PR: {pr_url}")
        if details:
            lines.append(" | ".join(details))
        if review_summary:
            lines.append(review_summary)
        self._send("task_completed", "\n".join(lines))

    def notify_task_failed(
        self,
        *,
        ticket_id: str,
        title: str,
        failure_reason: str | None = None,
        review_summary: str | None = None,
    ) -> None:
        """Notify that a task failed."""
        lines = [f"*Task failed:* {ticket_id} — {title}"]
        if failure_reason:
            lines.append(f"Reason: {failure_reason[:200]}")
        if review_summary:
            lines.append(review_summary)
        self._send("task_failed", "\n".join(lines))

    def notify_limit_hit(
        self,
        *,
        reason: str,
        resume_after: str | None = None,
    ) -> None:
        """Notify that dispatch was paused due to usage limits."""
        lines = [f"*Usage limit hit* — dispatch paused"]
        lines.append(f"Reason: {reason}")
        if resume_after:
            lines.append(f"Estimated resume: {resume_after}")
        self._send("limit_hit", "\n".join(lines), rate_limited=True)

    def notify_limit_cleared(self) -> None:
        """Notify that usage limits have cleared and dispatch resumed."""
        self._send("limit_cleared", "*Usage limit cleared* — dispatch resumed")

    def notify_capacity_warning(
        self,
        *,
        count: int,
        limit: int,
        percentage: float,
    ) -> None:
        """Notify that Linear issue count crossed the warning threshold (default 70%)."""
        lines = [
            f"*Capacity warning* — {percentage:.0f}% used ({count}/{limit})",
            "Archive completed issues to free capacity",
        ]
        self._send("capacity_warning", "\n".join(lines), rate_limited=True)

    def notify_capacity_critical(
        self,
        *,
        count: int,
        limit: int,
        percentage: float,
    ) -> None:
        """Notify that Linear issue count crossed the critical threshold (default 85%)."""
        lines = [
            f"*Capacity critical* — {percentage:.0f}% used ({count}/{limit})",
            "Archive completed issues to free capacity",
        ]
        self._send("capacity_critical", "\n".join(lines), rate_limited=True)

    def notify_capacity_blocked(
        self,
        *,
        count: int,
        limit: int,
        percentage: float,
    ) -> None:
        """Notify that Linear issue count crossed the pause threshold (default 95%) — dispatch paused."""
        lines = [
            f"*Capacity blocked* — {percentage:.0f}% used ({count}/{limit}), dispatch paused",
            "Archive completed issues to free capacity",
        ]
        self._send("capacity_blocked", "\n".join(lines))

    def notify_capacity_cleared(
        self,
        *,
        count: int,
        limit: int,
        percentage: float,
    ) -> None:
        """Notify that Linear issue count dropped below resume threshold — dispatch resumed."""
        lines = [
            f"*Capacity cleared* — {percentage:.0f}% used ({count}/{limit}), dispatch resumed",
        ]
        self._send("capacity_cleared", "\n".join(lines))

    def notify_all_idle(self) -> None:
        """Notify that all slots are idle (no more work)."""
        self._send("all_idle", "*All slots idle* — no more work to dispatch", rate_limited=True)

    def notify_refactoring_all_clear(
        self,
        *,
        month: str,
        year: int,
        linear_ticket_url: str,
    ) -> None:
        """Notify that a refactoring analysis found no action needed."""
        self._send(
            "refactoring_all_clear",
            f"Refactoring Analysis ({month} {year}): Code quality is good enough "
            f"— no action needed. Details: {linear_ticket_url}",
        )

    def notify_refactoring_action_needed(
        self,
        *,
        month: str,
        year: int,
        num_tickets: int,
        parent_ticket_id: str,
        brief_list: str,
        linear_ticket_url: str,
    ) -> None:
        """Notify that a refactoring analysis created follow-up tickets."""
        self._send(
            "refactoring_action_needed",
            f"Refactoring Analysis ({month} {year}): {num_tickets} refactoring "
            f"tickets created under {parent_ticket_id}. "
            f"Top concerns: {brief_list}. Details: {linear_ticket_url}",
        )

    def notify_supervisor_shutdown(self, *, reason: str) -> None:
        """Notify that the supervisor is shutting down unexpectedly."""
        self._send(
            "supervisor_shutdown",
            f"*Botfarm supervisor shut down:* {reason}. Workers may still be running.",
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _is_rate_limited(self, event_type: str) -> bool:
        """Check if this event type was sent recently."""
        last = self._last_sent.get(event_type)
        if last is None:
            return False
        return (time.monotonic() - last) < self._config.rate_limit_seconds

    def _send(self, event_type: str, message: str, *, rate_limited: bool = False) -> None:
        """Format and POST the webhook payload. Never raises."""
        if not self.enabled:
            return

        if rate_limited and self._is_rate_limited(event_type):
            logger.debug("Rate-limited notification: %s", event_type)
            return

        try:
            payload = self._format_payload(message)
            resp = self._client.post(self._url, json=payload)
            resp.raise_for_status()
            logger.debug("Sent %s notification", event_type)
        except Exception:
            logger.debug("Failed to send %s notification", event_type, exc_info=True)
        finally:
            if rate_limited:
                self._last_sent[event_type] = time.monotonic()

    def _format_payload(self, message: str) -> dict:
        """Build the webhook payload for the configured format."""
        if self._format == "discord":
            return {"content": message}
        # Slack (default)
        return {"text": message}
