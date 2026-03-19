"""Codex usage polling via the ChatGPT backend API.

Periodically polls ``chatgpt.com/backend-api/wham/usage`` using
credentials from ``~/.codex/auth.json`` and stores rate-limit snapshots
in the database.  Exposes current utilization so the supervisor can
pause dispatch when limits are close.

Uses ``curl_cffi`` with browser TLS impersonation to bypass Cloudflare
bot detection on chatgpt.com.

The feature is fully optional — if ``enabled`` is False or credentials
are missing the poller is a no-op.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from curl_cffi import requests as cffi_requests

from botfarm.config import CodexUsageConfig
from botfarm.credentials import CredentialError, load_codex_credentials
from botfarm.db import insert_codex_usage_snapshot

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 300  # seconds
DEFAULT_RETENTION_DAYS = 30
CODEX_USAGE_URL = "https://chatgpt.com/backend-api/wham/usage"
API_TIMEOUT = 30


@dataclass
class CodexUsageState:
    """In-memory snapshot of the most recent Codex rate-limit data."""

    plan_type: str | None = None
    primary_used_pct: float | None = None       # 0.0–1.0
    primary_reset_at: str | None = None         # ISO timestamp
    primary_window_seconds: int | None = None
    secondary_used_pct: float | None = None     # 0.0–1.0
    secondary_reset_at: str | None = None
    secondary_window_seconds: int | None = None
    rate_limit_allowed: bool = True
    last_polled_at: str | None = None

    def should_pause(
        self,
        primary_threshold: float = 0.85,
        secondary_threshold: float = 0.90,
        enabled: bool = True,
    ) -> tuple[bool, str | None]:
        """Check whether dispatch should be paused based on utilization thresholds.

        Returns (should_pause, reason_string_or_None).
        """
        if not enabled:
            return False, None
        if not self.rate_limit_allowed:
            return True, "Codex rate limit reached (allowed=false)"
        if (
            self.primary_used_pct is not None
            and self.primary_used_pct >= primary_threshold
        ):
            return True, (
                f"Codex primary utilization {self.primary_used_pct * 100:.1f}% "
                f">= {primary_threshold * 100:.0f}% threshold"
            )
        if (
            self.secondary_used_pct is not None
            and self.secondary_used_pct >= secondary_threshold
        ):
            return True, (
                f"Codex secondary utilization {self.secondary_used_pct * 100:.1f}% "
                f">= {secondary_threshold * 100:.0f}% threshold"
            )
        return False, None

    def to_dict(self) -> dict:
        return {
            "plan_type": self.plan_type,
            "primary_used_pct": self.primary_used_pct,
            "primary_reset_at": self.primary_reset_at,
            "primary_window_seconds": self.primary_window_seconds,
            "secondary_used_pct": self.secondary_used_pct,
            "secondary_reset_at": self.secondary_reset_at,
            "secondary_window_seconds": self.secondary_window_seconds,
            "rate_limit_allowed": self.rate_limit_allowed,
            "last_polled_at": self.last_polled_at,
        }


@dataclass
class CodexUsagePoller:
    """Polls the ChatGPT backend API for Codex rate-limit status.

    Designed to be called from the synchronous supervisor loop.
    """

    config: CodexUsageConfig = field(default_factory=CodexUsageConfig)
    retention_days: int = DEFAULT_RETENTION_DAYS

    _state: CodexUsageState = field(default_factory=CodexUsageState)
    _last_poll: float = 0.0
    _last_polled_fresh: bool = field(default=False, repr=False)

    @property
    def state(self) -> CodexUsageState:
        return self._state

    @property
    def last_polled_fresh(self) -> bool:
        return self._last_polled_fresh

    @property
    def enabled(self) -> bool:
        return bool(self.config.enabled)

    def poll(self, conn: sqlite3.Connection) -> CodexUsageState:
        """Poll if the interval has elapsed.  Returns current state."""
        if not self.enabled:
            self._last_polled_fresh = False
            return self._state

        now = time.monotonic()
        interval = self.config.poll_interval_seconds or DEFAULT_POLL_INTERVAL
        if self._last_poll > 0 and now - self._last_poll < interval:
            self._last_polled_fresh = False
            return self._state

        self._last_poll = now
        self._last_polled_fresh = True
        self._do_poll(conn)
        return self._state

    def force_poll(self, conn: sqlite3.Connection) -> CodexUsageState:
        """Poll immediately, ignoring the interval timer."""
        if not self.enabled:
            self._last_polled_fresh = False
            return self._state

        self._last_poll = time.monotonic()
        self._last_polled_fresh = True
        self._do_poll(conn)
        return self._state

    def close(self) -> None:
        """No-op — curl_cffi sessions are not persistent."""

    def _do_poll(self, conn: sqlite3.Connection) -> None:
        """Execute one poll cycle: fetch → parse → store → purge."""
        try:
            creds = load_codex_credentials()
        except CredentialError as exc:
            logger.warning("Codex credentials unavailable — skipping poll: %s", exc)
            return

        try:
            data = self._fetch(creds.access_token, creds.account_id)
        except Exception:
            logger.exception("Codex usage API call failed — using last known values")
            return

        self._parse_and_store(data, conn)
        self._purge_old_snapshots(conn)
        conn.commit()

    def _fetch(self, access_token: str, account_id: str) -> dict:
        """Call the ChatGPT backend usage API.

        Uses curl_cffi with Chrome TLS impersonation to bypass
        Cloudflare bot detection on chatgpt.com.
        """
        resp = cffi_requests.get(
            CODEX_USAGE_URL,
            headers={
                "Authorization": f"Bearer {access_token}",
                "ChatGPT-Account-Id": account_id,
            },
            impersonate="chrome",
            timeout=API_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_and_store(self, data: dict, conn: sqlite3.Connection) -> None:
        """Parse the API response and update in-memory state + database."""
        plan_type = data.get("plan_type")
        rate_limit = data.get("rate_limit") or {}
        primary = rate_limit.get("primary_window") or {}
        secondary = rate_limit.get("secondary_window") or {}

        raw_primary_pct = primary.get("used_percent")
        raw_secondary_pct = secondary.get("used_percent")

        self._state.plan_type = plan_type
        self._state.primary_used_pct = (
            raw_primary_pct / 100 if raw_primary_pct is not None else None
        )
        self._state.primary_window_seconds = primary.get("limit_window_seconds")
        self._state.primary_reset_at = _unix_to_iso(primary.get("reset_at"))
        self._state.secondary_used_pct = (
            raw_secondary_pct / 100 if raw_secondary_pct is not None else None
        )
        self._state.secondary_window_seconds = secondary.get("limit_window_seconds")
        self._state.secondary_reset_at = _unix_to_iso(secondary.get("reset_at"))
        self._state.rate_limit_allowed = rate_limit.get("allowed", True)
        self._state.last_polled_at = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        insert_codex_usage_snapshot(
            conn,
            plan_type=plan_type,
            primary_used_pct=self._state.primary_used_pct,
            primary_reset_at=self._state.primary_reset_at,
            primary_window_seconds=self._state.primary_window_seconds,
            secondary_used_pct=self._state.secondary_used_pct,
            secondary_reset_at=self._state.secondary_reset_at,
            secondary_window_seconds=self._state.secondary_window_seconds,
            rate_limit_allowed=self._state.rate_limit_allowed,
            raw_json=json.dumps(data),
        )

        logger.info(
            "Codex usage snapshot: primary=%.0f%%, secondary=%.0f%%, plan=%s",
            (self._state.primary_used_pct or 0) * 100,
            (self._state.secondary_used_pct or 0) * 100,
            plan_type or "unknown",
        )

    def _purge_old_snapshots(self, conn: sqlite3.Connection) -> None:
        """Delete codex usage snapshots older than the retention period."""
        conn.execute(
            "DELETE FROM codex_usage_snapshots "
            "WHERE created_at < datetime('now', ?)",
            (f"-{self.retention_days} days",),
        )


def _unix_to_iso(ts: int | None) -> str | None:
    """Convert a unix timestamp to an ISO 8601 string, or None."""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
