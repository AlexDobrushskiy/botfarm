"""OpenAI Costs API polling for Codex spending visibility.

Periodically polls the OpenAI ``/v1/organization/costs`` endpoint using an
Admin API Key and stores snapshots in the database.  Exposes current spend
so the supervisor can pause dispatch when a budget threshold is reached.

The feature is fully optional — if no admin key is configured the poller
is a no-op.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx

from botfarm.config import CodexUsageConfig
from botfarm.db import insert_codex_usage_snapshot

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 300  # seconds
DEFAULT_RETENTION_DAYS = 30
OPENAI_COSTS_URL = "https://api.openai.com/v1/organization/costs"
API_TIMEOUT = 30.0


@dataclass
class CodexUsageState:
    """In-memory snapshot of the most recent OpenAI spending data."""

    daily_spend: float | None = None
    monthly_spend: float | None = None
    monthly_budget: float = 0.0
    budget_utilization: float | None = None
    last_polled_at: str | None = None

    def should_pause(
        self,
        monthly_budget: float = 0.0,
        pause_threshold: float = 0.90,
        enabled: bool = True,
    ) -> tuple[bool, str | None]:
        """Check whether dispatch should be paused based on budget threshold.

        Returns (should_pause, reason_string_or_None).
        When ``enabled`` is False or budget is 0, always returns (False, None).
        """
        if not enabled or monthly_budget <= 0:
            return False, None
        if (
            self.budget_utilization is not None
            and self.budget_utilization >= pause_threshold
        ):
            return True, (
                f"Codex monthly spend ${self.monthly_spend or 0:.2f}"
                f"/${monthly_budget:.2f} "
                f"({self.budget_utilization * 100:.1f}%) "
                f">= {pause_threshold * 100:.0f}% threshold"
            )
        return False, None

    def to_dict(self) -> dict:
        return {
            "daily_spend": self.daily_spend,
            "monthly_spend": self.monthly_spend,
            "monthly_budget": self.monthly_budget,
            "budget_utilization": self.budget_utilization,
            "last_polled_at": self.last_polled_at,
        }


@dataclass
class CodexUsagePoller:
    """Polls the OpenAI Costs API and stores spending snapshots.

    Designed to be called from the synchronous supervisor loop.
    If ``admin_api_key`` is empty the poller silently does nothing.
    """

    config: CodexUsageConfig = field(default_factory=CodexUsageConfig)
    retention_days: int = DEFAULT_RETENTION_DAYS

    _state: CodexUsageState = field(default_factory=CodexUsageState)
    _last_poll: float = 0.0
    _last_polled_fresh: bool = field(default=False, repr=False)
    _client: httpx.Client | None = field(default=None, repr=False)

    @property
    def state(self) -> CodexUsageState:
        return self._state

    @property
    def last_polled_fresh(self) -> bool:
        return self._last_polled_fresh

    @property
    def enabled(self) -> bool:
        """Return True only if the feature is configured and has a key."""
        return bool(self.config.enabled and self.config.admin_api_key)

    def poll(self, conn: sqlite3.Connection) -> CodexUsageState:
        """Poll the Costs API if the interval has elapsed.

        Returns the current (possibly unchanged) state.
        """
        if not self.enabled:
            self._last_polled_fresh = False
            return self._state

        now = time.monotonic()
        interval = self.config.poll_interval_seconds or DEFAULT_POLL_INTERVAL
        if now - self._last_poll < interval:
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
        """Clean up the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            self._client.close()
            self._client = None

    def _get_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            self._client = httpx.Client(timeout=API_TIMEOUT)
        return self._client

    def _do_poll(self, conn: sqlite3.Connection) -> None:
        """Execute one poll cycle: fetch → parse → store → purge."""
        try:
            data = self._fetch()
        except Exception:
            logger.exception("OpenAI Costs API call failed — using last known values")
            return

        self._parse_and_store(data, conn)
        self._purge_old_snapshots(conn)
        conn.commit()

    def _fetch(self) -> dict:
        """Call the OpenAI Costs API."""
        client = self._get_client()
        now = datetime.now(timezone.utc)
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(start_of_month.timestamp())

        resp = client.get(
            OPENAI_COSTS_URL,
            headers={
                "Authorization": f"Bearer {self.config.admin_api_key}",
            },
            params={
                "start_time": start_ts,
                "bucket_width": "1d",
            },
        )
        resp.raise_for_status()
        return resp.json()

    def _parse_and_store(self, data: dict, conn: sqlite3.Connection) -> None:
        """Parse the API response and update in-memory state + database."""
        buckets = data.get("data", [])

        monthly_spend = 0.0
        daily_spend = 0.0
        for bucket in buckets:
            results = bucket.get("results", [])
            bucket_total = sum(
                r.get("amount", {}).get("value", 0.0) for r in results
            )
            monthly_spend += bucket_total
            # Last bucket is the most recent day
            daily_spend = bucket_total

        budget = self.config.monthly_budget
        utilization = (monthly_spend / budget) if budget > 0 else None

        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        self._state.daily_spend = daily_spend
        self._state.monthly_spend = monthly_spend
        self._state.monthly_budget = budget
        self._state.budget_utilization = utilization
        self._state.last_polled_at = now_iso

        insert_codex_usage_snapshot(
            conn,
            daily_spend=daily_spend,
            monthly_spend=monthly_spend,
            monthly_budget=budget,
            budget_utilization=utilization,
            raw_json=json.dumps(data),
        )

        budget_str = ""
        if budget > 0 and utilization is not None:
            budget_str = f", budget={utilization * 100:.1f}%"
        logger.info(
            "Codex usage snapshot: daily=$%.2f, monthly=$%.2f%s",
            daily_spend,
            monthly_spend,
            budget_str,
        )

    def _purge_old_snapshots(self, conn: sqlite3.Connection) -> None:
        """Delete codex usage snapshots older than the retention period."""
        conn.execute(
            "DELETE FROM codex_usage_snapshots "
            "WHERE created_at < datetime('now', ?)",
            (f"-{self.retention_days} days",),
        )
