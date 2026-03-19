"""Usage limit polling and snapshot storage.

Periodically polls the Anthropic usage API and stores snapshots in the
database for trend analysis.  Exposes current utilization so the supervisor
can pause dispatch when limits are close.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
import hashlib
import logging
import math
import random
import sqlite3
import time
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime

import httpx

from botfarm.credentials import USAGE_API_TIMEOUT, CredentialManager, fetch_usage
from botfarm.db import (
    clear_auth_failure_state,
    clear_backoff_state,
    get_active_key_session,
    insert_usage_api_call,
    insert_usage_snapshot,
    load_auth_failure_state,
    load_backoff_state,
    mark_key_session_replaced,
    purge_old_usage_api_calls,
    save_auth_failure_state,
    save_backoff_state,
    upsert_usage_api_key_session,
)

if TYPE_CHECKING:
    from botfarm.notifications import Notifier

_async_sleep = asyncio.sleep  # local alias for testability

logger = logging.getLogger(__name__)

DEFAULT_POLL_INTERVAL = 600  # seconds
DEFAULT_RETENTION_DAYS = 30
# Default thresholds (from ticket SMA-79)
DEFAULT_PAUSE_5H_THRESHOLD = 0.85
DEFAULT_PAUSE_7D_THRESHOLD = 0.90

# Retry configuration for transient connection errors
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = (2, 5)  # backoff delays between retries
TRANSIENT_EXCEPTIONS = (
    httpx.ConnectTimeout,
    httpx.ConnectError,
    httpx.PoolTimeout,
)

MAX_ADAPTIVE_POLL_INTERVAL = 1800  # 30 minutes cap for adaptive interval
MAX_401_BACKOFF_INTERVAL = 3600  # 1 hour cap for 401 auth error backoff
BACKOFF_JITTER_FRACTION = 0.5  # add up to 50% random jitter to backoff intervals
FORCE_POLL_COOLDOWN = 30  # minimum seconds between force_poll API calls

# Auth failure alerting thresholds
AUTH_FAILURE_NOTIFY_THRESHOLD = 3  # consecutive 401s before first notification


def token_fingerprint(token: str) -> str:
    """Compute a non-reversible fingerprint for key rotation detection.

    Uses SHA-256 of the last 8 characters, truncated to 16 hex chars.
    This is enough to detect when a token changes without leaking it.
    """
    return hashlib.sha256(token[-8:].encode()).hexdigest()[:16]


def _categorize_error(exc: Exception) -> str:
    """Map an exception to an error_type string for audit logging."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code == 429:
            return "rate_limit"
        elif code == 401:
            return "auth_error"
        elif code >= 500:
            return "server_error"
    elif isinstance(exc, (httpx.ConnectTimeout, httpx.PoolTimeout)):
        return "timeout"
    elif isinstance(exc, httpx.ConnectError):
        return "connection_error"
    return "other"


def parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header value into seconds.

    Handles both formats per RFC 9110:
    - Integer seconds (e.g. ``"120"``)
    - HTTP-date (e.g. ``"Fri, 13 Mar 2026 12:00:00 GMT"``)

    Returns ``None`` if the value is missing or unparseable.
    """
    if not value:
        return None
    # Try integer seconds first (most common for APIs)
    try:
        seconds = int(value)
        return max(seconds, 0)
    except ValueError:
        pass
    # Try HTTP-date format
    try:
        dt = parsedate_to_datetime(value)
        delta = (dt - dt.now(tz=dt.tzinfo)).total_seconds()
        return max(delta, 0)
    except Exception:
        logger.warning("Could not parse Retry-After header: %r", value)
        return None


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
    notifier: Notifier | None = None

    _state: UsageState = field(default_factory=UsageState)
    _last_poll: float = 0.0
    _last_polled_fresh: bool = field(default=False, repr=False)
    _client: httpx.AsyncClient | None = field(default=None, repr=False)
    _consecutive_429s: int = field(default=0, repr=False)
    _consecutive_401s: int = field(default=0, repr=False)
    _active_poll_interval: int | None = field(default=None, repr=False)
    _first_401_time: float | None = field(default=None, repr=False)
    _last_force_poll: float = field(default=0.0, repr=False)
    _current_caller: str | None = field(default=None, repr=False)
    _attempt_records: list[dict] = field(default_factory=list, repr=False)

    @property
    def state(self) -> UsageState:
        return self._state

    @property
    def last_polled_fresh(self) -> bool:
        """Whether the most recent ``poll()`` call actually fetched new data."""
        return self._last_polled_fresh

    @property
    def effective_poll_interval(self) -> int:
        """Return the current poll interval, which may be inflated due to 429s."""
        return self._active_poll_interval if self._active_poll_interval is not None else self.poll_interval

    def poll(self, conn: sqlite3.Connection) -> UsageState:
        """Poll the usage API if the interval has elapsed.

        Stores a snapshot in the database and purges old records.
        Returns the current (possibly unchanged) usage state.
        """
        now = time.monotonic()
        if now - self._last_poll < self.effective_poll_interval:
            self._last_polled_fresh = False
            return self._state

        self._last_poll = now
        self._last_polled_fresh = True
        self._current_caller = "poll"
        self._do_poll(conn)
        return self._state

    @property
    def in_429_backoff(self) -> bool:
        """Return True if we're in a 429 adaptive backoff period."""
        if self._consecutive_429s <= 0:
            return False
        elapsed = time.monotonic() - self._last_poll
        return elapsed < self.effective_poll_interval

    def force_poll(
        self,
        conn: sqlite3.Connection,
        *,
        bypass_cooldown: bool = False,
        caller: str | None = None,
    ) -> UsageState:
        """Poll immediately, ignoring the interval timer.

        Applies a cooldown of FORCE_POLL_COOLDOWN seconds to prevent
        rapid-fire API calls when multiple callers invoke force_poll()
        in quick succession.

        Pass ``bypass_cooldown=True`` for safety-critical paths that need
        a guaranteed fresh reading (e.g. limit checks, resume decisions).

        Even with ``bypass_cooldown=True``, 429 and 401 adaptive backoff is
        always respected — hammering a rate-limited or auth-failing API
        makes the situation worse.
        """
        now = time.monotonic()

        # NEVER bypass 429 adaptive backoff — we must respect rate limits
        if self._consecutive_429s > 0:
            if now - self._last_poll < self.effective_poll_interval:
                logger.debug(
                    "force_poll() suppressed — in 429 backoff (%ds remaining)",
                    self.effective_poll_interval - (now - self._last_poll),
                )
                self._last_polled_fresh = False
                return self._state

        # Also enforce 401 backoff — repeated auth failures cause 429 cascades
        if self._consecutive_401s > 0:
            if now - self._last_poll < self.effective_poll_interval:
                logger.debug(
                    "force_poll() suppressed — in 401 backoff (%ds remaining)",
                    self.effective_poll_interval - (now - self._last_poll),
                )
                self._last_polled_fresh = False
                return self._state

        if (
            not bypass_cooldown
            and self._last_force_poll > 0
            and now - self._last_force_poll < FORCE_POLL_COOLDOWN
        ):
            logger.debug(
                "force_poll() cooldown active (%.0fs remaining) — returning cached data",
                FORCE_POLL_COOLDOWN - (now - self._last_force_poll),
            )
            self._last_polled_fresh = False
            return self._state

        self._last_poll = now
        self._last_force_poll = now
        self._last_polled_fresh = True
        if caller:
            self._current_caller = caller
        elif bypass_cooldown:
            self._current_caller = "force_poll_bypass"
        else:
            self._current_caller = "force_poll"
        self._do_poll(conn)
        return self._state

    def manual_refresh(self, conn: sqlite3.Connection) -> UsageState:
        """Fetch, parse, and store fresh usage data, raising on errors.

        Unlike ``_do_poll`` / ``force_poll`` which swallow errors internally,
        this method propagates exceptions so callers (e.g. the dashboard
        endpoint) can map them to user-facing messages.

        On 429, backoff state is updated via ``_handle_429()`` before
        re-raising, so the poller's rate-limit safety is maintained.

        Raises:
            ValueError: No OAuth token available.
            httpx.HTTPStatusError: API returned non-2xx.
            httpx.ConnectTimeout / ConnectError / PoolTimeout: Connection issues.
        """
        token = self.credential_manager.get_token()
        if token is None:
            raise ValueError("No OAuth credentials available. Check credential configuration.")

        fp = token_fingerprint(token)
        self._check_key_rotation(conn, fp)
        self._attempt_records = []
        self._current_caller = "manual_refresh"
        self._last_poll = time.monotonic()
        self._last_force_poll = time.monotonic()

        try:
            data = self._fetch(token)
        except httpx.HTTPStatusError as exc:
            self._flush_audit_records(conn, fp)
            if exc.response.status_code == 429:
                retry_after = exc.response.headers.get("retry-after")
                self._handle_429(conn, retry_after)
            raise
        except Exception:
            self._flush_audit_records(conn, fp)
            raise
        else:
            self._flush_audit_records(conn, fp)

        self._reset_rate_limit_state(conn)
        self._reset_auth_failure_state(conn)
        self._parse_and_store(data, conn)
        self._purge_old_snapshots(conn)
        conn.commit()
        return self._state

    def _get_client(self) -> httpx.AsyncClient:
        """Return the persistent HTTP client, creating it on first use."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=USAGE_API_TIMEOUT)
        return self._client

    def close(self) -> None:
        """Clean up the persistent HTTP client."""
        if self._client is not None and not self._client.is_closed:
            loop = _get_or_create_event_loop()
            loop.run_until_complete(self._client.aclose())
            self._client = None

    def restore_backoff_state(self, conn: sqlite3.Connection) -> None:
        """Restore 429 backoff state from the database after a restart.

        If ``backoff_until`` is still in the future, the poller resumes
        the backoff so it doesn't immediately hammer a rate-limited API.
        If the backoff has expired, the persisted state is cleared.
        """
        state = load_backoff_state(conn)
        if state is None:
            return
        remaining = state["backoff_until"] - time.time()
        if remaining <= 0:
            clear_backoff_state(conn)
            return
        self._consecutive_429s = state["consecutive_429s"]
        # Use the remaining time from the persisted backoff_until directly.
        # This preserves server-specified Retry-After values that may exceed
        # MAX_ADAPTIVE_POLL_INTERVAL.
        self._active_poll_interval = math.ceil(remaining)
        # Align monotonic _last_poll so that the remaining backoff is respected
        self._last_poll = time.monotonic() - (self._active_poll_interval - remaining)
        logger.info(
            "Restored 429 backoff state: %d consecutive 429(s), "
            "backing off for %ds more",
            self._consecutive_429s,
            int(remaining),
        )

    def restore_auth_failure_state(self, conn: sqlite3.Connection) -> None:
        """Restore 401 auth failure state from the database after a restart.

        If ``backoff_until`` is still in the future, the poller resumes
        the backoff so it doesn't immediately hammer an auth-failing API.
        If the backoff has expired, the persisted state is cleared.
        """
        state = load_auth_failure_state(conn)
        if state is None:
            return
        remaining = state["backoff_until"] - time.time()
        if remaining <= 0:
            clear_auth_failure_state(conn)
            return
        self._consecutive_401s = state["consecutive_401s"]
        self._active_poll_interval = math.ceil(remaining)
        # Convert persisted wall-clock first_failure_time to monotonic
        elapsed_since_first = time.time() - state["first_failure_time"]
        self._first_401_time = time.monotonic() - elapsed_since_first
        # Align monotonic _last_poll so that the remaining backoff is respected
        self._last_poll = time.monotonic() - (self._active_poll_interval - remaining)
        logger.info(
            "Restored 401 auth failure state: %d consecutive 401(s), "
            "backing off for %ds more",
            self._consecutive_401s,
            int(remaining),
        )

    def _do_poll(self, conn: sqlite3.Connection) -> None:
        """Execute one poll cycle: fetch → parse → store → purge."""
        token = self.credential_manager.get_token()
        if token is None:
            logger.warning("No OAuth token available — skipping usage poll")
            return

        fp = token_fingerprint(token)
        self._check_key_rotation(conn, fp)
        self._attempt_records = []

        try:
            data = self._fetch(token)
        except httpx.HTTPStatusError as exc:
            self._flush_audit_records(conn, fp)
            if exc.response.status_code == 429:
                retry_after_header = exc.response.headers.get("retry-after")
                self._handle_429(conn, retry_after_header)
                return
            elif exc.response.status_code == 401:
                expires_at = self.credential_manager.get_expires_at()
                logger.warning(
                    "Usage API returned 401 — token expiresAt: %s, refreshing",
                    expires_at or "unknown",
                )
                original_fp = fp
                token = self.credential_manager.refresh_token()
                if token is None:
                    logger.warning("Token refresh failed — skipping usage poll")
                    self._handle_401(conn)
                    return
                new_fp = token_fingerprint(token)
                if new_fp == original_fp:
                    logger.warning(
                        "Token fingerprint unchanged after refresh — "
                        "skipping retry (token not yet rotated)",
                    )
                    self._handle_401(conn)
                    return
                fp = new_fp
                self._check_key_rotation(conn, fp)
                self._attempt_records = []
                try:
                    data = self._fetch(token)
                except Exception:
                    self._flush_audit_records(conn, fp)
                    logger.exception("Usage API call failed after token refresh")
                    self._handle_401(conn)
                    return
                self._flush_audit_records(conn, fp)
            else:
                logger.warning(
                    "Usage API returned HTTP %d — using last known values",
                    exc.response.status_code,
                )
                return
        except Exception:
            self._flush_audit_records(conn, fp)
            logger.exception("Usage API call failed — using last known values")
            return
        else:
            self._flush_audit_records(conn, fp)

        self._reset_rate_limit_state(conn)
        self._reset_auth_failure_state(conn)
        self._parse_and_store(data, conn)
        self._purge_old_snapshots(conn)
        conn.commit()

    def _handle_429(
        self, conn: sqlite3.Connection, retry_after_header: str | None = None,
    ) -> None:
        """Handle a 429 rate-limit response by increasing the poll interval.

        If the response includes a ``Retry-After`` header, the backoff is at
        least that many seconds.  The final interval is the greater of the
        parsed header value and the exponential backoff formula.

        The backoff state is persisted to the database so that it survives
        supervisor restarts.
        """
        self._consecutive_429s += 1
        exponential = self.poll_interval * (2 ** self._consecutive_429s)
        retry_after = parse_retry_after(retry_after_header)
        if retry_after is not None:
            # Server-specified Retry-After is authoritative — never cap it.
            # Only cap the exponential backoff component.
            new_interval = max(retry_after, min(exponential, MAX_ADAPTIVE_POLL_INTERVAL))
            jittered = new_interval * (1 + random.uniform(0, BACKOFF_JITTER_FRACTION))
            self._active_poll_interval = math.ceil(jittered)
        else:
            new_interval = min(exponential, MAX_ADAPTIVE_POLL_INTERVAL)
            jittered = new_interval * (1 + random.uniform(0, BACKOFF_JITTER_FRACTION))
            self._active_poll_interval = math.ceil(
                min(jittered, MAX_ADAPTIVE_POLL_INTERVAL)
            )
        logger.warning(
            "Usage API returned 429 (consecutive: %d, retry-after: %s) — "
            "increasing poll interval to %ds",
            self._consecutive_429s,
            retry_after_header or "absent",
            self._active_poll_interval,
        )
        save_backoff_state(
            conn,
            consecutive_429s=self._consecutive_429s,
            backoff_until=time.time() + self._active_poll_interval,
        )

    def _handle_401(self, conn: sqlite3.Connection) -> None:
        """Handle a 401 auth error by applying progressive linear backoff
        and sending webhook notifications after repeated failures.

        Uses a gentler curve than 429 (linear vs exponential) because 401
        failures are self-inflicted — we're waiting for token refresh, not
        for rate limit clearance.

        The backoff state is persisted to the database so that it survives
        supervisor restarts.
        """
        self._consecutive_401s += 1
        if self._first_401_time is None:
            self._first_401_time = time.monotonic()
        new_interval = self.poll_interval * (1 + self._consecutive_401s)
        new_interval = min(new_interval, MAX_401_BACKOFF_INTERVAL)
        # Don't reduce interval if 429 backoff is active and larger
        current = self._active_poll_interval or 0
        self._active_poll_interval = max(current, new_interval)
        logger.warning(
            "Usage API 401 backoff (consecutive: %d) — "
            "poll interval now %ds",
            self._consecutive_401s,
            self._active_poll_interval,
        )
        save_auth_failure_state(
            conn,
            consecutive_401s=self._consecutive_401s,
            backoff_until=time.time() + self._active_poll_interval,
            first_failure_time=time.time() - (time.monotonic() - self._first_401_time),
        )
        if (
            self._consecutive_401s >= AUTH_FAILURE_NOTIFY_THRESHOLD
            and self.notifier is not None
        ):
            minutes_since = int(
                (time.monotonic() - self._first_401_time) / 60,
            )
            self.notifier.notify_auth_failure(
                consecutive_failures=self._consecutive_401s,
                minutes_since_success=minutes_since if minutes_since > 0 else None,
            )

    def _reset_rate_limit_state(self, conn: sqlite3.Connection) -> None:
        """Reset 429 rate-limit backoff after a successful API response."""
        if self._consecutive_429s > 0:
            logger.info(
                "Usage API recovered after %d consecutive 429(s) — "
                "resetting poll interval to %ds",
                self._consecutive_429s,
                self.poll_interval,
            )
            self._consecutive_429s = 0
            clear_backoff_state(conn)
            self._active_poll_interval = None

    def _reset_auth_failure_state(self, conn: sqlite3.Connection) -> None:
        """Reset auth failure tracking after a successful poll.

        Clears 401 backoff (both in-memory and persisted) and sends a
        recovery notification if we were in a 401 outage.
        """
        if self._consecutive_401s > 0:
            logger.info(
                "Usage API auth recovered after %d consecutive 401(s) — "
                "resetting poll interval to %ds",
                self._consecutive_401s,
                self.poll_interval,
            )
            if (
                self._consecutive_401s >= AUTH_FAILURE_NOTIFY_THRESHOLD
                and self.notifier is not None
            ):
                self.notifier.notify_auth_recovered()
            self._consecutive_401s = 0
            self._first_401_time = None
            self._active_poll_interval = None
            clear_auth_failure_state(conn)

    def _fetch(self, token: str) -> dict:
        """Call the usage API synchronously via asyncio, with retry for transient errors."""
        loop = _get_or_create_event_loop()
        return loop.run_until_complete(self._fetch_with_retry(token))

    async def _fetch_with_retry(self, token: str) -> dict:
        """Fetch usage data, retrying on transient connection errors only.

        429 responses are NOT retried here — they are raised immediately
        so that ``_do_poll`` can handle them via ``_handle_429()``.

        Each attempt (success or failure) is recorded in
        ``self._attempt_records`` for audit logging by ``_do_poll``.
        """
        client = self._get_client()
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            start_time = time.monotonic()
            try:
                result = await fetch_usage(token, client=client)
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._attempt_records.append({
                    "status_code": 200,
                    "success": True,
                    "error_type": None,
                    "error_detail": None,
                    "response_time_ms": elapsed_ms,
                    "retry_after": None,
                })
                return result
            except httpx.HTTPStatusError as exc:
                elapsed_ms = (time.monotonic() - start_time) * 1000
                retry_after = exc.response.headers.get("retry-after")
                self._attempt_records.append({
                    "status_code": exc.response.status_code,
                    "success": False,
                    "error_type": _categorize_error(exc),
                    "error_detail": str(exc)[:500],
                    "response_time_ms": elapsed_ms,
                    "retry_after": retry_after,
                })
                if exc.response.status_code == 429:
                    raise  # Let _do_poll handle 429 via _handle_429()
                raise
            except TRANSIENT_EXCEPTIONS as exc:
                elapsed_ms = (time.monotonic() - start_time) * 1000
                self._attempt_records.append({
                    "status_code": None,
                    "success": False,
                    "error_type": _categorize_error(exc),
                    "error_detail": str(exc)[:500],
                    "response_time_ms": elapsed_ms,
                    "retry_after": None,
                })
                last_exc = exc
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_BACKOFF_SECONDS[attempt]
                    logger.warning(
                        "Transient error on usage API (attempt %d/%d): %s — "
                        "retrying in %ds",
                        attempt + 1,
                        MAX_RETRIES,
                        exc,
                        delay,
                    )
                    await _async_sleep(delay)
                else:
                    logger.warning(
                        "Transient error on usage API (attempt %d/%d): %s — "
                        "all retries exhausted",
                        attempt + 1,
                        MAX_RETRIES,
                        exc,
                    )
        raise last_exc  # type: ignore[misc]

    def _parse_and_store(self, data: dict, conn: sqlite3.Connection) -> None:
        """Parse the API response and update in-memory state + database."""
        five_hour = data.get("five_hour") or {}
        seven_day = data.get("seven_day") or {}

        raw_5h = five_hour.get("utilization")
        self._state.utilization_5h = raw_5h / 100 if raw_5h is not None else None
        raw_7d = seven_day.get("utilization")
        self._state.utilization_7d = raw_7d / 100 if raw_7d is not None else None
        self._state.resets_at_5h = five_hour.get("resets_at")
        self._state.resets_at_7d = seven_day.get("resets_at")

        extra = data.get("extra_usage") or {}
        self._state.extra_usage_enabled = bool(extra.get("is_enabled"))
        raw_limit = extra.get("monthly_limit")
        self._state.extra_usage_monthly_limit = raw_limit / 100 if raw_limit is not None else None
        raw_credits = extra.get("used_credits")
        self._state.extra_usage_used_credits = raw_credits / 100 if raw_credits is not None else None
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
                f"/${self._state.extra_usage_monthly_limit or 0:.2f}"
            )
        logger.info(
            "Usage snapshot: 5h=%.1f%%, 7d=%.1f%%, resets=%s%s",
            (self._state.utilization_5h or 0) * 100,
            (self._state.utilization_7d or 0) * 100,
            self._state.resets_at_5h or "unknown",
            extra_msg,
        )

    def _check_key_rotation(self, conn: sqlite3.Connection, fingerprint: str) -> None:
        """Detect and record when the API token has been rotated."""
        try:
            active = get_active_key_session(conn)
            if active and active["token_fingerprint"] != fingerprint:
                mark_key_session_replaced(
                    conn, token_fingerprint=active["token_fingerprint"],
                )
                conn.commit()
        except Exception:
            logger.warning("Failed to check key rotation", exc_info=True)

    def _flush_audit_records(self, conn: sqlite3.Connection, fingerprint: str) -> None:
        """Write collected attempt records to the audit tables."""
        for rec in self._attempt_records:
            try:
                insert_usage_api_call(
                    conn,
                    token_fingerprint=fingerprint,
                    status_code=rec["status_code"],
                    success=rec["success"],
                    error_type=rec["error_type"],
                    error_detail=rec["error_detail"],
                    response_time_ms=rec["response_time_ms"],
                    retry_after=rec["retry_after"],
                    caller=self._current_caller,
                )
                upsert_usage_api_key_session(
                    conn,
                    token_fingerprint=fingerprint,
                    success=rec["success"],
                )
            except Exception:
                logger.warning("Failed to write usage API audit record", exc_info=True)
        try:
            conn.commit()
        except Exception:
            logger.warning("Failed to commit usage API audit records", exc_info=True)
        self._attempt_records = []

    def _purge_old_snapshots(self, conn: sqlite3.Connection) -> None:
        """Delete usage snapshots and old audit rows beyond the retention period."""
        conn.execute(
            "DELETE FROM usage_snapshots "
            "WHERE created_at < datetime('now', ?)",
            (f"-{self.retention_days} days",),
        )
        try:
            purge_old_usage_api_calls(conn, retention_days=self.retention_days)
        except Exception:
            logger.warning("Failed to purge old usage API audit rows", exc_info=True)


def refresh_usage_snapshot(
    conn: sqlite3.Connection,
    *,
    caller: str = "cli_refresh",
    poller: UsagePoller | None = None,
) -> UsageState | None:
    """Fetch fresh usage data from the API and store a snapshot.

    Returns the new ``UsageState`` on success, or ``None`` if the API call
    fails (e.g. no credentials, network error).  Callers can fall back to
    the latest DB snapshot when ``None`` is returned.

    When *poller* is provided, it is reused across calls so that 429
    backoff state (``_consecutive_429s``, ``_active_poll_interval``)
    persists.  When omitted, a throwaway poller is created — acceptable
    for short-lived CLI invocations but NOT for long-lived processes
    like the dashboard.
    """
    owns_poller = poller is None
    if owns_poller:
        poller = UsagePoller()
    try:
        poller.force_poll(conn, caller=caller)
    except Exception:
        logger.warning("Failed to refresh usage data from API", exc_info=True)
        return None
    finally:
        if owns_poller:
            poller.close()
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
