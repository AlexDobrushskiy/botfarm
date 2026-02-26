"""Usage limit polling and snapshot storage.

Periodically polls the Anthropic usage API and stores snapshots in the
database for trend analysis.  Exposes current utilization so the supervisor
can pause dispatch when limits are close.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from dataclasses import dataclass, field

import httpx

from botfarm.credentials import CredentialManager, fetch_usage
from botfarm.db import insert_usage_snapshot

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 300  # seconds
DEFAULT_RETENTION_DAYS = 30
# Default thresholds (from ticket SMA-79)
DEFAULT_PAUSE_5H_THRESHOLD = 0.85
DEFAULT_PAUSE_7D_THRESHOLD = 0.90


@dataclass
class UsageState:
    """In-memory snapshot of the most recent usage data."""

    utilization_5h: float | None = None
    utilization_7d: float | None = None
    resets_at_5h: str | None = None
    resets_at_7d: str | None = None
    extra_usage_enabled: bool = False
    extra_usage_monthly_limit: float | None = None
    extra_usage_used_credits: float | None = None
    extra_usage_utilization: float | None = None

    def should_pause_with_thresholds(
        self,
        five_hour_threshold: float = DEFAULT_PAUSE_5H_THRESHOLD,
        seven_day_threshold: float = DEFAULT_PAUSE_7D_THRESHOLD,
        enabled: bool = True,
    ) -> tuple[bool, str | None]:
        """Check whether dispatch should be paused based on configurable thresholds.

        Returns (should_pause, reason_string_or_None).
        When ``enabled`` is False, always returns (False, None).
        """
        if not enabled:
            return False, None
        if (
            self.utilization_5h is not None
            and self.utilization_5h >= five_hour_threshold
        ):
            return True, (
                f"5-hour utilization {self.utilization_5h * 100:.1f}% "
                f">= {five_hour_threshold * 100:.0f}% threshold"
            )
        if (
            self.utilization_7d is not None
            and self.utilization_7d >= seven_day_threshold
        ):
            return True, (
                f"7-day utilization {self.utilization_7d * 100:.1f}% "
                f">= {seven_day_threshold * 100:.0f}% threshold"
            )
        return False, None

    @property
    def is_on_extra_usage(self) -> bool:
        """Return True if extra usage is enabled and included limits are exhausted."""
        if not self.extra_usage_enabled:
            return False
        return (
            (self.utilization_5h is not None and self.utilization_5h >= 1.0)
            or (self.utilization_7d is not None and self.utilization_7d >= 1.0)
        )

    def to_dict(self) -> dict:
        return {
            "utilization_5h": self.utilization_5h,
            "utilization_7d": self.utilization_7d,
            "resets_at_5h": self.resets_at_5h,
            "resets_at_7d": self.resets_at_7d,
            "extra_usage_enabled": self.extra_usage_enabled,
            "extra_usage_monthly_limit": self.extra_usage_monthly_limit,
            "extra_usage_used_credits": self.extra_usage_used_credits,
            "extra_usage_utilization": self.extra_usage_utilization,
        }


@dataclass
class UsagePoller:
    """Polls the Anthropic usage API and stores snapshots.

    The poller is designed to be called from a synchronous supervisor loop.
    It tracks elapsed time internally and only makes an API call when
    the configured interval has passed.
    """

    credential_manager: CredentialManager = field(default_factory=CredentialManager)
    poll_interval: int = DEFAULT_POLL_INTERVAL
    retention_days: int = DEFAULT_RETENTION_DAYS

    _state: UsageState = field(default_factory=UsageState)
    _last_poll: float = 0.0
    _last_polled_fresh: bool = field(default=False, repr=False)

    @property
    def state(self) -> UsageState:
        return self._state

    @property
    def last_polled_fresh(self) -> bool:
        """Whether the most recent ``poll()`` call actually fetched new data."""
        return self._last_polled_fresh

    def poll(self, conn: sqlite3.Connection) -> UsageState:
        """Poll the usage API if the interval has elapsed.

        Stores a snapshot in the database and purges old records.
        Returns the current (possibly unchanged) usage state.
        """
        now = time.monotonic()
        if now - self._last_poll < self.poll_interval:
            self._last_polled_fresh = False
            return self._state

        self._last_poll = now
        self._last_polled_fresh = True
        self._do_poll(conn)
        return self._state

    def force_poll(self, conn: sqlite3.Connection) -> UsageState:
        """Poll immediately, ignoring the interval timer."""
        self._last_poll = time.monotonic()
        self._last_polled_fresh = True
        self._do_poll(conn)
        return self._state

    def close(self) -> None:
        """Clean up resources (currently a no-op)."""

    def _do_poll(self, conn: sqlite3.Connection) -> None:
        """Execute one poll cycle: fetch → parse → store → purge."""
        token = self.credential_manager.get_token()
        if token is None:
            logger.warning("No OAuth token available — skipping usage poll")
            return

        try:
            data = self._fetch(token)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 401:
                logger.warning("Usage API returned 401 — refreshing token")
                token = self.credential_manager.refresh_token()
                if token is None:
                    logger.warning("Token refresh failed — skipping usage poll")
                    return
                try:
                    data = self._fetch(token)
                except Exception:
                    logger.exception("Usage API call failed after token refresh")
                    return
            else:
                logger.warning(
                    "Usage API returned HTTP %d — using last known values",
                    exc.response.status_code,
                )
                return
        except Exception:
            logger.exception("Usage API call failed — using last known values")
            return

        self._parse_and_store(data, conn)
        self._purge_old_snapshots(conn)
        conn.commit()

    def _fetch(self, token: str) -> dict:
        """Call the usage API synchronously via asyncio."""
        loop = _get_or_create_event_loop()
        return loop.run_until_complete(fetch_usage(token))

    def _parse_and_store(self, data: dict, conn: sqlite3.Connection) -> None:
        """Parse the API response and update in-memory state + database."""
        five_hour = data.get("five_hour", {})
        seven_day = data.get("seven_day", {})

        raw_5h = five_hour.get("utilization")
        self._state.utilization_5h = raw_5h / 100 if raw_5h is not None else None
        raw_7d = seven_day.get("utilization")
        self._state.utilization_7d = raw_7d / 100 if raw_7d is not None else None
        self._state.resets_at_5h = five_hour.get("resets_at")
        self._state.resets_at_7d = seven_day.get("resets_at")

        extra = data.get("extra_usage") or {}
        self._state.extra_usage_enabled = bool(extra.get("is_enabled"))
        self._state.extra_usage_monthly_limit = extra.get("monthly_limit")
        self._state.extra_usage_used_credits = extra.get("used_credits")
        self._state.extra_usage_utilization = extra.get("utilization")

        insert_usage_snapshot(
            conn,
            utilization_5h=self._state.utilization_5h,
            utilization_7d=self._state.utilization_7d,
            resets_at=self._state.resets_at_5h,
            resets_at_7d=self._state.resets_at_7d,
            extra_usage_enabled=self._state.extra_usage_enabled,
            extra_usage_monthly_limit=self._state.extra_usage_monthly_limit,
            extra_usage_used_credits=self._state.extra_usage_used_credits,
            extra_usage_utilization=self._state.extra_usage_utilization,
        )

        extra_msg = ""
        if self._state.extra_usage_enabled:
            extra_msg = (
                f", extra_usage=${self._state.extra_usage_used_credits or 0:.2f}"
                f"/${self._state.extra_usage_monthly_limit or 0:.0f}"
            )
        logger.info(
            "Usage snapshot: 5h=%.1f%%, 7d=%.1f%%, resets=%s%s",
            (self._state.utilization_5h or 0) * 100,
            (self._state.utilization_7d or 0) * 100,
            self._state.resets_at_5h or "unknown",
            extra_msg,
        )

    def _purge_old_snapshots(self, conn: sqlite3.Connection) -> None:
        """Delete usage snapshots older than the retention period."""
        conn.execute(
            "DELETE FROM usage_snapshots "
            "WHERE created_at < datetime('now', ?)",
            (f"-{self.retention_days} days",),
        )


def refresh_usage_snapshot(conn: sqlite3.Connection) -> UsageState | None:
    """Fetch fresh usage data from the API and store a snapshot.

    Returns the new ``UsageState`` on success, or ``None`` if the API call
    fails (e.g. no credentials, network error).  Callers can fall back to
    the latest DB snapshot when ``None`` is returned.
    """
    poller = UsagePoller()
    try:
        poller.force_poll(conn)
    except Exception:
        logger.warning("Failed to refresh usage data from API", exc_info=True)
        return None
    if poller.last_polled_fresh and poller.state.utilization_5h is not None:
        return poller.state
    return None


def _get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Return the running event loop, or create a new one if none exists."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop
