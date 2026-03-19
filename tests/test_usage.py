"""Tests for botfarm.usage — usage limit polling and snapshot storage."""

from __future__ import annotations

import asyncio
import math
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from botfarm.credentials import CredentialManager
from botfarm.db import (
    get_active_key_session,
    get_usage_api_call_history,
    get_usage_snapshots,
)
from botfarm.usage import (
    AUTH_FAILURE_NOTIFY_THRESHOLD,
    BACKOFF_JITTER_FRACTION,
    DEFAULT_PAUSE_5H_THRESHOLD,
    DEFAULT_PAUSE_7D_THRESHOLD,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_RETENTION_DAYS,
    FORCE_POLL_COOLDOWN,
    MAX_ADAPTIVE_POLL_INTERVAL,
    MAX_RETRIES,
    TRANSIENT_EXCEPTIONS,
    UsagePoller,
    UsageState,
    _categorize_error,
    parse_retry_after,
    refresh_usage_snapshot,
    token_fingerprint,
)

# --- Sample API response ---

SAMPLE_USAGE_RESPONSE = {
    "five_hour": {
        "utilization": 42,
        "resets_at": "2026-02-12T22:00:00Z",
    },
    "seven_day": {
        "utilization": 15,
        "resets_at": "2026-02-18T00:00:00Z",
    },
}

HIGH_USAGE_RESPONSE = {
    "five_hour": {
        "utilization": 95,
        "resets_at": "2026-02-12T22:00:00Z",
    },
    "seven_day": {
        "utilization": 60,
        "resets_at": "2026-02-18T00:00:00Z",
    },
}

EXTRA_USAGE_RESPONSE = {
    "five_hour": {
        "utilization": 100,
        "resets_at": "2026-02-12T22:00:00Z",
    },
    "seven_day": {
        "utilization": 58,
        "resets_at": "2026-02-18T00:00:00Z",
    },
    "extra_usage": {
        "is_enabled": True,
        "monthly_limit": 5000,
        "used_credits": 2344.0,
        "utilization": 46.88,
    },
}

EXTRA_USAGE_DISABLED_RESPONSE = {
    "five_hour": {
        "utilization": 100,
        "resets_at": "2026-02-12T22:00:00Z",
    },
    "seven_day": {
        "utilization": 58,
        "resets_at": "2026-02-18T00:00:00Z",
    },
    "extra_usage": {
        "is_enabled": False,
    },
}


# ---------------------------------------------------------------------------
# UsageState
# ---------------------------------------------------------------------------


class TestUsageState:
    def test_defaults(self):
        state = UsageState()
        assert state.utilization_5h is None
        assert state.utilization_7d is None
        assert state.resets_at_5h is None
        assert state.resets_at_7d is None

    def test_extra_usage_defaults(self):
        state = UsageState()
        assert state.extra_usage_enabled is False
        assert state.extra_usage_monthly_limit is None
        assert state.extra_usage_used_credits is None
        assert state.extra_usage_utilization is None

    def test_to_dict(self):
        state = UsageState(
            utilization_5h=0.42,
            utilization_7d=0.15,
            resets_at_5h="2026-02-12T22:00:00Z",
            resets_at_7d="2026-02-18T00:00:00Z",
        )
        d = state.to_dict()
        assert d["utilization_5h"] == 0.42
        assert d["utilization_7d"] == 0.15
        assert d["resets_at_5h"] == "2026-02-12T22:00:00Z"
        assert d["resets_at_7d"] == "2026-02-18T00:00:00Z"
        assert d["extra_usage_enabled"] is False

    def test_to_dict_with_extra_usage(self):
        state = UsageState(
            utilization_5h=1.0,
            utilization_7d=0.58,
            extra_usage_enabled=True,
            extra_usage_monthly_limit=50.0,
            extra_usage_used_credits=23.44,
            extra_usage_utilization=46.88,
        )
        d = state.to_dict()
        assert d["extra_usage_enabled"] is True
        assert d["extra_usage_monthly_limit"] == 50.0
        assert d["extra_usage_used_credits"] == 23.44
        assert d["extra_usage_utilization"] == 46.88

    def test_to_dict_none_values(self):
        state = UsageState()
        d = state.to_dict()
        assert d["utilization_5h"] is None
        assert d["utilization_7d"] is None
        assert d["extra_usage_enabled"] is False

    # --- should_pause_with_thresholds ---

    def test_pause_with_thresholds_none_values(self):
        state = UsageState()
        paused, reason = state.should_pause_with_thresholds()
        assert paused is False
        assert reason is None

    def test_pause_with_thresholds_5h_above(self):
        state = UsageState(utilization_5h=0.86)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85
        )
        assert paused is True
        assert "5-hour" in reason
        assert "86.0%" in reason

    def test_pause_with_thresholds_5h_at_threshold(self):
        state = UsageState(utilization_5h=0.85)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85
        )
        assert paused is True

    def test_pause_with_thresholds_5h_below(self):
        state = UsageState(utilization_5h=0.50)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85
        )
        assert paused is False
        assert reason is None

    def test_pause_with_thresholds_7d_above(self):
        state = UsageState(utilization_5h=0.50, utilization_7d=0.92)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85, seven_day_threshold=0.90,
        )
        assert paused is True
        assert "7-day" in reason
        assert "92.0%" in reason

    def test_pause_with_thresholds_7d_at_threshold(self):
        state = UsageState(utilization_5h=0.50, utilization_7d=0.90)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85, seven_day_threshold=0.90,
        )
        assert paused is True

    def test_pause_with_thresholds_5h_checked_first(self):
        """5h threshold is checked before 7d — 5h reason takes priority."""
        state = UsageState(utilization_5h=0.90, utilization_7d=0.95)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85, seven_day_threshold=0.90,
        )
        assert paused is True
        assert "5-hour" in reason

    def test_pause_with_thresholds_both_below(self):
        state = UsageState(utilization_5h=0.50, utilization_7d=0.60)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85, seven_day_threshold=0.90,
        )
        assert paused is False
        assert reason is None

    def test_pause_with_thresholds_custom(self):
        state = UsageState(utilization_5h=0.71)
        paused, _ = state.should_pause_with_thresholds(
            five_hour_threshold=0.70,
        )
        assert paused is True

    def test_pause_with_thresholds_7d_none(self):
        """If 7d utilization is None, only 5h is checked."""
        state = UsageState(utilization_5h=0.50, utilization_7d=None)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85, seven_day_threshold=0.90,
        )
        assert paused is False

    def test_pause_with_thresholds_disabled(self):
        """When enabled=False, never pauses even if utilization exceeds thresholds."""
        state = UsageState(utilization_5h=0.99, utilization_7d=0.99)
        paused, reason = state.should_pause_with_thresholds(
            five_hour_threshold=0.85, seven_day_threshold=0.90,
            enabled=False,
        )
        assert paused is False
        assert reason is None

    # --- is_on_extra_usage ---

    def test_is_on_extra_usage_disabled(self):
        state = UsageState(utilization_5h=1.0, extra_usage_enabled=False)
        assert state.is_on_extra_usage is False

    def test_is_on_extra_usage_below_100(self):
        state = UsageState(utilization_5h=0.85, extra_usage_enabled=True)
        assert state.is_on_extra_usage is False

    def test_is_on_extra_usage_5h_at_100(self):
        state = UsageState(utilization_5h=1.0, utilization_7d=0.5, extra_usage_enabled=True)
        assert state.is_on_extra_usage is True

    def test_is_on_extra_usage_7d_at_100(self):
        state = UsageState(utilization_5h=0.5, utilization_7d=1.0, extra_usage_enabled=True)
        assert state.is_on_extra_usage is True

    def test_is_on_extra_usage_both_none(self):
        state = UsageState(extra_usage_enabled=True)
        assert state.is_on_extra_usage is False


# ---------------------------------------------------------------------------
# UsagePoller — basic construction
# ---------------------------------------------------------------------------


# conn fixture provided by tests/conftest.py


@pytest.fixture()
def poller():
    """Return a UsagePoller with a mocked CredentialManager."""
    cred_mgr = MagicMock(spec=CredentialManager)
    cred_mgr.get_token.return_value = "test-token"
    return UsagePoller(credential_manager=cred_mgr, poll_interval=10)


class TestUsagePollerConstruction:
    def test_defaults(self):
        p = UsagePoller()
        assert p.poll_interval == DEFAULT_POLL_INTERVAL
        assert p.retention_days == DEFAULT_RETENTION_DAYS
        assert p.state.utilization_5h is None

    def test_custom_interval(self):
        p = UsagePoller(poll_interval=60)
        assert p.poll_interval == 60


# ---------------------------------------------------------------------------
# UsagePoller — polling
# ---------------------------------------------------------------------------


class TestUsagePollerPoll:
    def test_poll_fetches_and_stores_snapshot(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            state = poller.force_poll(conn)

        assert state.utilization_5h == 0.42
        assert state.utilization_7d == 0.15
        assert state.resets_at_5h == "2026-02-12T22:00:00Z"
        assert state.resets_at_7d == "2026-02-18T00:00:00Z"

        # Check DB snapshot was stored
        snapshots = get_usage_snapshots(conn, limit=10)
        assert len(snapshots) == 1
        assert snapshots[0]["utilization_5h"] == pytest.approx(0.42)
        assert snapshots[0]["utilization_7d"] == pytest.approx(0.15)

    def test_poll_respects_interval(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.force_poll(conn)
            # Immediately polling again should be skipped
            poller.poll(conn)
            assert mock_fetch.call_count == 1

    def test_poll_after_interval_elapsed(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.force_poll(conn)
            # Simulate time passing
            poller._last_poll = time.monotonic() - 20  # interval is 10
            poller.poll(conn)
            assert mock_fetch.call_count == 2

    def test_force_poll_ignores_interval(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.force_poll(conn)
            # Reset cooldown to allow second force_poll
            poller._last_force_poll = 0
            poller.force_poll(conn)
            assert mock_fetch.call_count == 2

    def test_poll_no_token_skips(self, conn):
        cred_mgr = MagicMock(spec=CredentialManager)
        cred_mgr.get_token.return_value = None
        p = UsagePoller(credential_manager=cred_mgr)

        state = p.force_poll(conn)
        assert state.utilization_5h is None
        snapshots = get_usage_snapshots(conn, limit=10)
        assert len(snapshots) == 0


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestUsagePollerErrors:
    def test_network_error_uses_last_known_values(self, poller, conn):
        # First successful poll
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.force_poll(conn)

        # Second poll fails
        with patch.object(poller, "_fetch", side_effect=Exception("network error")):
            poller._last_poll = 0  # reset interval
            state = poller.poll(conn)

        # State should retain last known values
        assert state.utilization_5h == 0.42
        assert state.utilization_7d == 0.15

    def test_401_triggers_token_refresh(self, poller, conn):
        response = httpx.Response(
            401,
            request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
        )
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        call_count = 0

        def fetch_side_effect(token):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise error
            return SAMPLE_USAGE_RESPONSE

        poller.credential_manager.refresh_token.return_value = "new-token"

        with patch.object(poller, "_fetch", side_effect=fetch_side_effect):
            state = poller.force_poll(conn)

        poller.credential_manager.refresh_token.assert_called_once()
        assert state.utilization_5h == 0.42

    def test_401_refresh_fails_skips_poll(self, poller, conn):
        response = httpx.Response(
            401,
            request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
        )
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        poller.credential_manager.refresh_token.return_value = None

        with patch.object(poller, "_fetch", side_effect=error):
            state = poller.force_poll(conn)

        assert state.utilization_5h is None

    def test_non_401_http_error_uses_last_known(self, poller, conn):
        response = httpx.Response(
            500,
            request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
        )
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            state = poller.force_poll(conn)

        assert state.utilization_5h is None


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------


class TestUsagePollerParsing:
    def test_partial_response_missing_seven_day(self, poller, conn):
        response = {
            "five_hour": {"utilization": 50, "resets_at": "2026-02-12T22:00:00Z"},
        }
        with patch.object(poller, "_fetch", return_value=response):
            state = poller.force_poll(conn)

        assert state.utilization_5h == 0.5
        assert state.utilization_7d is None

    def test_empty_response(self, poller, conn):
        with patch.object(poller, "_fetch", return_value={}):
            state = poller.force_poll(conn)

        assert state.utilization_5h is None
        assert state.utilization_7d is None

    def test_extra_usage_parsed(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=EXTRA_USAGE_RESPONSE):
            state = poller.force_poll(conn)

        assert state.utilization_5h == 1.0
        assert state.extra_usage_enabled is True
        assert state.extra_usage_monthly_limit == pytest.approx(50.0)
        assert state.extra_usage_used_credits == pytest.approx(23.44)
        assert state.extra_usage_utilization == 46.88

        # Verify DB snapshot includes extra usage
        snapshots = get_usage_snapshots(conn, limit=1)
        assert len(snapshots) == 1
        assert snapshots[0]["extra_usage_enabled"] == 1
        assert snapshots[0]["extra_usage_monthly_limit"] == pytest.approx(50.0)
        assert snapshots[0]["extra_usage_used_credits"] == pytest.approx(23.44)
        assert snapshots[0]["extra_usage_utilization"] == pytest.approx(46.88)

    def test_extra_usage_disabled_parsed(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=EXTRA_USAGE_DISABLED_RESPONSE):
            state = poller.force_poll(conn)

        assert state.extra_usage_enabled is False
        assert state.extra_usage_monthly_limit is None
        assert state.extra_usage_used_credits is None

    def test_no_extra_usage_field(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            state = poller.force_poll(conn)

        assert state.extra_usage_enabled is False
        assert state.extra_usage_monthly_limit is None

    def test_extra_usage_null_response(self, poller, conn):
        response = {**SAMPLE_USAGE_RESPONSE, "extra_usage": None}
        with patch.object(poller, "_fetch", return_value=response):
            state = poller.force_poll(conn)

        assert state.extra_usage_enabled is False


# ---------------------------------------------------------------------------
# Snapshot retention / purge
# ---------------------------------------------------------------------------


class TestSnapshotRetention:
    def test_old_snapshots_purged(self, poller, conn):
        # Insert an old snapshot (manually, with old timestamp)
        conn.execute(
            "INSERT INTO usage_snapshots (utilization_5h, utilization_7d, created_at) "
            "VALUES (?, ?, datetime('now', '-60 days'))",
            (0.1, 0.05),
        )
        conn.commit()

        # Insert via a fresh poll
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.force_poll(conn)

        snapshots = get_usage_snapshots(conn, limit=100)
        # Old one should be purged, only the new one remains
        assert len(snapshots) == 1
        assert snapshots[0]["utilization_5h"] == pytest.approx(0.42)

    def test_recent_snapshots_kept(self, poller, conn):
        # Insert a recent snapshot
        conn.execute(
            "INSERT INTO usage_snapshots (utilization_5h, utilization_7d, created_at) "
            "VALUES (?, ?, datetime('now', '-1 day'))",
            (0.2, 0.1),
        )
        conn.commit()

        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.force_poll(conn)

        snapshots = get_usage_snapshots(conn, limit=100)
        assert len(snapshots) == 2


# ---------------------------------------------------------------------------
# refresh_usage_snapshot — standalone refresh function
# ---------------------------------------------------------------------------


class TestRefreshUsageSnapshot:
    def test_returns_fresh_state_on_success(self, conn):
        with patch("botfarm.usage.UsagePoller.force_poll") as mock_poll:
            # Simulate a successful poll that sets state
            def fake_poll(c, **kwargs):
                pass

            mock_poll.side_effect = fake_poll

            with patch("botfarm.usage.UsagePoller.last_polled_fresh", new_callable=lambda: property(lambda self: True)):
                with patch("botfarm.usage.UsagePoller.state", new_callable=lambda: property(
                    lambda self: UsageState(utilization_5h=0.42, utilization_7d=0.15)
                )):
                    result = refresh_usage_snapshot(conn)

        assert result is not None
        assert result.utilization_5h == 0.42
        assert result.utilization_7d == 0.15

    def test_returns_none_on_api_failure(self, conn):
        with patch("botfarm.usage.UsagePoller.force_poll", side_effect=Exception("API error")):
            result = refresh_usage_snapshot(conn)
        assert result is None

    def test_returns_none_when_not_fresh(self, conn):
        with patch("botfarm.usage.UsagePoller.force_poll"):
            with patch("botfarm.usage.UsagePoller.last_polled_fresh", new_callable=lambda: property(lambda self: False)):
                result = refresh_usage_snapshot(conn)
        assert result is None

    def test_stores_snapshot_in_db(self, conn):
        """Verify that a successful refresh stores a new snapshot in the DB."""
        with patch("botfarm.usage.UsagePoller._fetch", return_value=SAMPLE_USAGE_RESPONSE):
            with patch("botfarm.usage.CredentialManager.get_token", return_value="test-token"):
                result = refresh_usage_snapshot(conn)

        assert result is not None
        assert result.utilization_5h == pytest.approx(0.42)
        snapshots = get_usage_snapshots(conn, limit=10)
        assert len(snapshots) == 1
        assert snapshots[0]["utilization_5h"] == pytest.approx(0.42)

    def test_shared_poller_preserves_backoff(self, conn):
        """When a shared poller is passed, 429 backoff state persists across calls."""
        shared = UsagePoller()
        resp_429 = httpx.Response(429, request=httpx.Request("GET", "https://x"))

        async def _raise_429(token, *, client=None):
            raise httpx.HTTPStatusError("rate limited", request=resp_429.request, response=resp_429)

        with patch("botfarm.usage.fetch_usage", side_effect=_raise_429):
            with patch("botfarm.usage.CredentialManager.get_token", return_value="t"):
                # First call — triggers 429, consecutive count → 1
                refresh_usage_snapshot(conn, poller=shared)
                assert shared._consecutive_429s == 1

                # Reset timers so force_poll doesn't skip due to backoff/cooldown
                shared._last_poll = 0.0
                shared._last_force_poll = 0.0

                # Second call with same poller — should increment to 2
                refresh_usage_snapshot(conn, poller=shared)
                assert shared._consecutive_429s == 2

    def test_throwaway_poller_resets_backoff(self, conn):
        """Without a shared poller, backoff state resets every call (the bug)."""
        resp_429 = httpx.Response(429, request=httpx.Request("GET", "https://x"))

        async def _raise_429(token, *, client=None):
            raise httpx.HTTPStatusError("rate limited", request=resp_429.request, response=resp_429)

        consecutive_counts = []

        orig_handle = UsagePoller._handle_429

        def spy_handle(self, conn, retry_after_header=None):
            orig_handle(self, conn, retry_after_header)
            consecutive_counts.append(self._consecutive_429s)

        with patch("botfarm.usage.fetch_usage", side_effect=_raise_429):
            with patch("botfarm.usage.CredentialManager.get_token", return_value="t"):
                with patch.object(UsagePoller, "_handle_429", spy_handle):
                    refresh_usage_snapshot(conn)
                    refresh_usage_snapshot(conn)

        # Without shared poller, each call creates a fresh instance → always 1
        assert consecutive_counts == [1, 1]

    def test_shared_poller_not_closed(self, conn):
        """A shared poller's HTTP client is NOT closed after the call."""
        shared = UsagePoller()

        with patch("botfarm.usage.UsagePoller._fetch", return_value=SAMPLE_USAGE_RESPONSE):
            with patch("botfarm.usage.CredentialManager.get_token", return_value="t"):
                refresh_usage_snapshot(conn, poller=shared)

        # The shared poller should still be usable (not closed)
        assert shared._client is None or not shared._client.is_closed
        shared.close()


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestUsagePollerRetry:
    def test_retry_succeeds_after_transient_error(self, poller, conn):
        """A transient ConnectTimeout on attempt 1 is retried and succeeds."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectTimeout("connection timed out")
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                state = poller.force_poll(conn)

        assert call_count == 2
        assert state.utilization_5h == 0.42

    def test_retry_exhausted_falls_back(self, poller, conn):
        """After MAX_RETRIES transient errors, _do_poll falls back to last known values."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        async def always_fail(token, *, client=None):
            raise httpx.ConnectTimeout("connection timed out")

        with patch("botfarm.usage.fetch_usage", side_effect=always_fail):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                state = poller.force_poll(conn)

        # State should remain at defaults (None) since no successful poll occurred
        assert state.utilization_5h is None

    @pytest.mark.asyncio
    async def test_fetch_with_retry_retries_on_connect_timeout(self, poller):
        """_fetch_with_retry retries on ConnectTimeout."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            if call_count < MAX_RETRIES:
                raise httpx.ConnectTimeout("timed out")
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                result = await poller._fetch_with_retry("test-token")

        assert result == SAMPLE_USAGE_RESPONSE
        assert call_count == MAX_RETRIES

    @pytest.mark.asyncio
    async def test_fetch_with_retry_retries_on_connect_error(self, poller):
        """_fetch_with_retry retries on ConnectError."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("connection refused")
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                result = await poller._fetch_with_retry("test-token")

        assert result == SAMPLE_USAGE_RESPONSE
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_fetch_with_retry_retries_on_pool_timeout(self, poller):
        """_fetch_with_retry retries on PoolTimeout."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.PoolTimeout("pool timed out")
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                result = await poller._fetch_with_retry("test-token")

        assert result == SAMPLE_USAGE_RESPONSE
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_fetch_with_retry_does_not_retry_http_status_error(self, poller):
        """Non-transient errors like HTTPStatusError are NOT retried."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        async def mock_fetch(token, *, client=None):
            response = httpx.Response(
                500,
                request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
            )
            raise httpx.HTTPStatusError("", request=response.request, response=response)

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with pytest.raises(httpx.HTTPStatusError):
                await poller._fetch_with_retry("test-token")

    @pytest.mark.asyncio
    async def test_fetch_with_retry_all_attempts_fail(self, poller):
        """When all retries are exhausted, the last transient error is raised."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        async def mock_fetch(token, *, client=None):
            raise httpx.ConnectTimeout("timed out")

        sleep_delays: list[float] = []

        async def _tracking_sleep(delay):
            sleep_delays.append(delay)

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", _tracking_sleep):
                with pytest.raises(httpx.ConnectTimeout):
                    await poller._fetch_with_retry("test-token")

        # Should have slept between retries (MAX_RETRIES - 1 times)
        assert len(sleep_delays) == MAX_RETRIES - 1

    @pytest.mark.asyncio
    async def test_fetch_with_retry_backoff_delays(self, poller):
        """Verify the correct backoff delays are used between retries."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        async def mock_fetch(token, *, client=None):
            raise httpx.ConnectTimeout("timed out")

        sleep_delays: list[float] = []

        async def _tracking_sleep(delay):
            sleep_delays.append(delay)

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", _tracking_sleep):
                with pytest.raises(httpx.ConnectTimeout):
                    await poller._fetch_with_retry("test-token")

        assert sleep_delays == [2, 5]


# ---------------------------------------------------------------------------
# Persistent HTTP client
# ---------------------------------------------------------------------------


class TestUsagePollerPersistentClient:
    async def test_get_client_creates_client(self, poller):
        """_get_client creates a new AsyncClient on first use."""
        assert poller._client is None
        client = poller._get_client()
        assert client is not None
        assert isinstance(client, httpx.AsyncClient)
        await client.aclose()

    async def test_get_client_reuses_existing(self, poller):
        """_get_client returns the same client on subsequent calls."""
        client1 = poller._get_client()
        client2 = poller._get_client()
        assert client1 is client2
        await client1.aclose()

    async def test_get_client_recreates_if_closed(self, poller):
        """_get_client creates a new client if the existing one is closed."""
        client1 = poller._get_client()
        await client1.aclose()

        client2 = poller._get_client()
        assert client2 is not client1
        assert not client2.is_closed
        await client2.aclose()

    def test_close_shuts_down_client(self, poller):
        """close() closes the persistent client."""
        poller._get_client()
        assert poller._client is not None
        poller.close()
        assert poller._client is None

    def test_close_noop_when_no_client(self, poller):
        """close() is safe to call when no client exists."""
        poller.close()  # Should not raise

    def test_fetch_uses_persistent_client(self, poller, conn):
        """The persistent client is passed to fetch_usage."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        async def mock_fetch(token, *, client=None):
            assert client is mock_client
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                state = poller.force_poll(conn)

        assert state.utilization_5h == 0.42


# ---------------------------------------------------------------------------
# 429 rate-limit retry in _fetch_with_retry
# ---------------------------------------------------------------------------


def _make_429_response(retry_after: str | None = None) -> httpx.Response:
    """Create a mock 429 response, optionally with Retry-After header."""
    headers = {}
    if retry_after is not None:
        headers["retry-after"] = retry_after
    return httpx.Response(
        429,
        request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
        headers=headers,
    )


def _make_429_error(retry_after: str | None = None) -> httpx.HTTPStatusError:
    resp = _make_429_response(retry_after)
    return httpx.HTTPStatusError("", request=resp.request, response=resp)


class TestFetchWithRetry429:
    @pytest.mark.asyncio
    async def test_429_raises_immediately_no_retry(self, poller):
        """A 429 is raised immediately without retrying."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            raise _make_429_error()

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with pytest.raises(httpx.HTTPStatusError) as exc_info:
                await poller._fetch_with_retry("test-token")

        assert exc_info.value.response.status_code == 429
        assert call_count == 1  # No retries — raised on first attempt

    @pytest.mark.asyncio
    async def test_429_not_retried_even_with_retry_after(self, poller):
        """429 with Retry-After header is still raised immediately."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            raise _make_429_error(retry_after="10")

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with pytest.raises(httpx.HTTPStatusError):
                await poller._fetch_with_retry("test-token")

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_non_429_http_error_not_retried(self, poller):
        """A 500 HTTPStatusError is raised immediately, not retried."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            resp = httpx.Response(
                500,
                request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
            )
            raise httpx.HTTPStatusError("", request=resp.request, response=resp)

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with pytest.raises(httpx.HTTPStatusError):
                await poller._fetch_with_retry("test-token")

        assert call_count == 1


# ---------------------------------------------------------------------------
# Adaptive poll interval on repeated 429s
# ---------------------------------------------------------------------------


class TestAdaptivePollInterval:
    @pytest.fixture(autouse=True)
    def _no_jitter(self):
        with patch("botfarm.usage.random.uniform", return_value=0):
            yield

    def test_429_increases_poll_interval(self, poller, conn):
        """A 429 from _do_poll increases the effective poll interval."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        assert poller.effective_poll_interval == poller.poll_interval * 2

    def test_consecutive_429s_double_interval(self, poller, conn):
        """Each consecutive 429 doubles the interval."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)
            # Reset both cooldown and backoff timer to allow next force_poll.
            # Use a far-past timestamp so the backoff guard never triggers
            # (time.monotonic() can be small in fresh CI containers).
            far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
            poller._last_force_poll = far_past
            poller._last_poll = far_past
            poller.force_poll(conn)
            poller._last_force_poll = far_past
            poller._last_poll = far_past
            poller.force_poll(conn)

        assert poller._consecutive_429s == 3
        assert poller.effective_poll_interval == poller.poll_interval * 8

    def test_interval_capped_at_max(self, poller, conn):
        """Adaptive interval is capped at MAX_ADAPTIVE_POLL_INTERVAL."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        # Simulate many 429s to exceed cap.
        # Use a far-past timestamp so the backoff guard never triggers
        # (time.monotonic() can be small in fresh CI containers).
        far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
        with patch.object(poller, "_fetch", side_effect=error):
            for _ in range(20):
                poller._last_force_poll = far_past
                poller._last_poll = far_past
                poller.force_poll(conn)

        assert poller.effective_poll_interval == MAX_ADAPTIVE_POLL_INTERVAL

    def test_success_resets_interval(self, poller, conn):
        """A successful response after 429s resets the interval."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        # First: trigger 429
        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        assert poller.effective_poll_interval > poller.poll_interval

        # Then: successful poll (backoff expired)
        far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller._last_force_poll = far_past
            poller._last_poll = far_past
            poller.force_poll(conn)

        assert poller._consecutive_429s == 0
        assert poller.effective_poll_interval == poller.poll_interval

    def test_effective_poll_interval_default(self, poller):
        """Without 429s, effective_poll_interval equals configured poll_interval."""
        assert poller.effective_poll_interval == poller.poll_interval

    def test_poll_uses_effective_interval(self, poller, conn):
        """poll() checks against effective_poll_interval, not poll_interval."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        # Trigger 429 to inflate interval
        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        inflated = poller.effective_poll_interval
        # Set _last_poll to "poll_interval ago" — still within inflated interval
        poller._last_poll = time.monotonic() - poller.poll_interval

        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.poll(conn)

        # Should NOT have polled because inflated interval hasn't elapsed
        mock_fetch.assert_not_called()
        assert poller.last_polled_fresh is False


# ---------------------------------------------------------------------------
# force_poll() cooldown
# ---------------------------------------------------------------------------


class TestForcePollCooldown:
    def test_first_force_poll_goes_through(self, poller, conn):
        """The very first force_poll() always executes."""
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.force_poll(conn)

        mock_fetch.assert_called_once()
        assert poller.last_polled_fresh is True

    def test_rapid_force_poll_returns_cached(self, poller, conn):
        """A second force_poll() within cooldown returns cached data."""
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.force_poll(conn)
            state = poller.force_poll(conn)

        mock_fetch.assert_called_once()
        assert poller.last_polled_fresh is False
        assert state.utilization_5h == 0.42

    def test_force_poll_after_cooldown_goes_through(self, poller, conn):
        """After cooldown expires, force_poll() executes again."""
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.force_poll(conn)
            # Simulate cooldown expiry
            poller._last_force_poll = time.monotonic() - FORCE_POLL_COOLDOWN - 1
            poller.force_poll(conn)

        assert mock_fetch.call_count == 2

    def test_cooldown_preserves_existing_state(self, poller, conn):
        """When cooldown blocks, existing state is preserved."""
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.force_poll(conn)

        # Second call during cooldown
        with patch.object(poller, "_fetch", return_value=HIGH_USAGE_RESPONSE) as mock_fetch:
            state = poller.force_poll(conn)

        mock_fetch.assert_not_called()
        # Should still have old values, not HIGH_USAGE_RESPONSE
        assert state.utilization_5h == 0.42

    def test_bypass_cooldown_forces_fresh_poll(self, poller, conn):
        """bypass_cooldown=True ignores the cooldown and fetches fresh data."""
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.force_poll(conn)

        # Second call within cooldown, but with bypass
        with patch.object(poller, "_fetch", return_value=HIGH_USAGE_RESPONSE) as mock_fetch:
            state = poller.force_poll(conn, bypass_cooldown=True)

        mock_fetch.assert_called_once()
        assert poller.last_polled_fresh is True
        assert state.utilization_5h == 0.95


# ---------------------------------------------------------------------------
# Existing transient-error retry preserved
# ---------------------------------------------------------------------------


class TestTransientRetryPreserved:
    def test_connect_timeout_still_retried_alongside_429(self, poller, conn):
        """ConnectTimeout is still retried even with 429 handling in place."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectTimeout("timed out")
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                state = poller.force_poll(conn)

        assert call_count == 2
        assert state.utilization_5h == 0.42


# ---------------------------------------------------------------------------
# force_poll() respects 429 backoff (SMA-376)
# ---------------------------------------------------------------------------


class TestForcePoll429Backoff:
    def test_force_poll_suppressed_during_429_backoff(self, poller, conn):
        """force_poll(bypass_cooldown=True) is suppressed during 429 backoff."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        # Trigger 429 to enter backoff
        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        assert poller.in_429_backoff is True

        # Now force_poll with bypass_cooldown=True should be suppressed
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            state = poller.force_poll(conn, bypass_cooldown=True)

        mock_fetch.assert_not_called()
        assert poller.last_polled_fresh is False

    def test_force_poll_allowed_after_429_backoff_expires(self, poller, conn):
        """force_poll works once the 429 backoff period has elapsed."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        # Trigger 429
        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1

        # Simulate backoff expiry
        poller._last_poll = time.monotonic() - poller.effective_poll_interval - 1

        assert poller.in_429_backoff is False

        # Now force_poll should go through
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            state = poller.force_poll(conn, bypass_cooldown=True)

        mock_fetch.assert_called_once()
        assert poller.last_polled_fresh is True
        assert state.utilization_5h == 0.42
        # Should have reset 429 state after successful poll
        assert poller._consecutive_429s == 0

    def test_in_429_backoff_false_when_no_429s(self, poller):
        """in_429_backoff is False when there are no 429 errors."""
        assert poller.in_429_backoff is False

    def test_in_429_backoff_true_within_interval(self, poller, conn):
        """in_429_backoff is True when within the adaptive backoff interval."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller.in_429_backoff is True

    def test_in_429_backoff_false_after_interval(self, poller, conn):
        """in_429_backoff is False when the backoff interval has elapsed."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        # Simulate time passing beyond the backoff
        poller._last_poll = time.monotonic() - poller.effective_poll_interval - 1
        assert poller.in_429_backoff is False

    def test_consecutive_429s_increment_across_force_polls(self, poller, conn):
        """_consecutive_429s increments correctly even when force_poll backoff
        expires between calls."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
        with patch.object(poller, "_fetch", side_effect=error):
            # First 429
            poller.force_poll(conn)
            assert poller._consecutive_429s == 1

            # Expire the backoff, then trigger another 429
            poller._last_poll = far_past
            poller._last_force_poll = far_past
            poller.force_poll(conn)
            assert poller._consecutive_429s == 2

            # Expire again, trigger a third
            poller._last_poll = far_past
            poller._last_force_poll = far_past
            poller.force_poll(conn)
            assert poller._consecutive_429s == 3

    def test_normal_poll_also_blocked_during_429_backoff(self, poller, conn):
        """Regular poll() also respects the inflated interval during 429 backoff."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        # Try normal poll — should be blocked by inflated interval
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            poller.poll(conn)

        mock_fetch.assert_not_called()
        assert poller.last_polled_fresh is False


# ---------------------------------------------------------------------------
# Token fingerprint
# ---------------------------------------------------------------------------


class TestTokenFingerprint:
    def test_deterministic(self):
        assert token_fingerprint("abc12345") == token_fingerprint("abc12345")

    def test_different_tokens(self):
        assert token_fingerprint("token-aaa") != token_fingerprint("token-bbb")

    def test_returns_16_hex_chars(self):
        fp = token_fingerprint("my-secret-token")
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)

    def test_only_last_8_chars_matter(self):
        assert token_fingerprint("XXXXXXXX12345678") == token_fingerprint("YYYYYYYY12345678")


# ---------------------------------------------------------------------------
# Error categorization
# ---------------------------------------------------------------------------


class TestCategorizeError:
    def _http_error(self, status: int) -> httpx.HTTPStatusError:
        resp = httpx.Response(
            status,
            request=httpx.Request("GET", "https://example.com"),
        )
        return httpx.HTTPStatusError("", request=resp.request, response=resp)

    def test_429_rate_limit(self):
        assert _categorize_error(self._http_error(429)) == "rate_limit"

    def test_401_auth_error(self):
        assert _categorize_error(self._http_error(401)) == "auth_error"

    def test_500_server_error(self):
        assert _categorize_error(self._http_error(500)) == "server_error"

    def test_503_server_error(self):
        assert _categorize_error(self._http_error(503)) == "server_error"

    def test_400_other(self):
        assert _categorize_error(self._http_error(400)) == "other"

    def test_connect_timeout(self):
        assert _categorize_error(httpx.ConnectTimeout("timeout")) == "timeout"

    def test_pool_timeout(self):
        assert _categorize_error(httpx.PoolTimeout("pool")) == "timeout"

    def test_connect_error(self):
        assert _categorize_error(httpx.ConnectError("refused")) == "connection_error"

    def test_generic_exception(self):
        assert _categorize_error(RuntimeError("boom")) == "other"


# ---------------------------------------------------------------------------
# Usage API audit instrumentation
# ---------------------------------------------------------------------------


class TestUsageAuditInstrumentation:
    @pytest.fixture()
    def audit_poller(self):
        """Poller with mock client — patches fetch_usage, not _fetch."""
        cred_mgr = MagicMock(spec=CredentialManager)
        cred_mgr.get_token.return_value = "test-token"
        p = UsagePoller(credential_manager=cred_mgr, poll_interval=10)
        mock_client = AsyncMock()
        mock_client.is_closed = False
        p._client = mock_client
        return p

    def _poll_with_fetch(self, poller, conn, response=None, side_effect=None, **kwargs):
        """Force poll using a patched fetch_usage (not _fetch)."""
        if response is not None:
            async def _ok(token, *, client=None):
                return response
            side_effect = _ok
        with patch("botfarm.usage.fetch_usage", side_effect=side_effect):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                return poller.force_poll(conn, **kwargs)

    def test_successful_poll_creates_audit_row(self, audit_poller, conn):
        """A successful poll inserts a row in usage_api_calls with success=1."""
        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["success"] == 1
        assert rows[0]["status_code"] == 200
        assert rows[0]["error_type"] is None
        assert rows[0]["response_time_ms"] is not None
        assert rows[0]["token_fingerprint"] is not None

    def test_429_creates_audit_row_with_rate_limit(self, audit_poller, conn):
        """A 429 response creates an audit row with error_type=rate_limit."""
        async def _raise_429(token, *, client=None):
            raise _make_429_error()

        self._poll_with_fetch(audit_poller, conn, side_effect=_raise_429)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["success"] == 0
        assert rows[0]["status_code"] == 429
        assert rows[0]["error_type"] == "rate_limit"

    def test_connection_error_creates_audit_rows(self, audit_poller, conn):
        """Connection errors create audit rows for each retry attempt."""
        async def always_fail(token, *, client=None):
            raise httpx.ConnectError("connection refused")

        self._poll_with_fetch(audit_poller, conn, side_effect=always_fail)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == MAX_RETRIES
        for row in rows:
            assert row["success"] == 0
            assert row["error_type"] == "connection_error"
            assert row["status_code"] is None

    def test_caller_context_recorded_poll(self, audit_poller, conn):
        """poll() records caller='poll'."""
        audit_poller._last_poll = 0

        async def _ok(token, *, client=None):
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=_ok):
            with patch("botfarm.usage._async_sleep", new_callable=AsyncMock):
                audit_poller.poll(conn)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["caller"] == "poll"

    def test_caller_context_recorded_force_poll(self, audit_poller, conn):
        """force_poll() records caller='force_poll'."""
        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["caller"] == "force_poll"

    def test_caller_context_recorded_force_poll_bypass(self, audit_poller, conn):
        """force_poll(bypass_cooldown=True) records caller='force_poll_bypass'."""
        self._poll_with_fetch(
            audit_poller, conn, response=SAMPLE_USAGE_RESPONSE, bypass_cooldown=True,
        )

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["caller"] == "force_poll_bypass"

    def test_caller_context_custom(self, audit_poller, conn):
        """force_poll(caller='custom') overrides the default."""
        self._poll_with_fetch(
            audit_poller, conn, response=SAMPLE_USAGE_RESPONSE, caller="cli_refresh",
        )

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["caller"] == "cli_refresh"

    def test_key_session_created_on_success(self, audit_poller, conn):
        """A successful poll creates/updates a key session."""
        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        session = get_active_key_session(conn)
        assert session is not None
        assert session["status"] == "active"
        assert session["total_successes"] == 1
        assert session["consecutive_errors"] == 0

    def test_key_session_error_then_recovery(self, audit_poller, conn):
        """Error followed by success transitions session to recovered."""
        # First: a 500 error
        async def _raise_500(token, *, client=None):
            resp = httpx.Response(
                500,
                request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
            )
            raise httpx.HTTPStatusError("", request=resp.request, response=resp)

        self._poll_with_fetch(audit_poller, conn, side_effect=_raise_500)

        session = get_active_key_session(conn)
        assert session["status"] == "erroring"
        assert session["total_errors"] == 1

        # Then: success (reset cooldown)
        audit_poller._last_force_poll = 0
        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        session = get_active_key_session(conn)
        assert session["status"] == "recovered"
        assert session["total_successes"] == 1
        assert session["consecutive_errors"] == 0

    def test_key_rotation_detection(self, audit_poller, conn):
        """Changing token between polls marks old session as replaced."""
        # First poll with token A
        audit_poller.credential_manager.get_token.return_value = "token-AAAAAAAA"
        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        fp_a = token_fingerprint("token-AAAAAAAA")
        session = get_active_key_session(conn)
        assert session["token_fingerprint"] == fp_a

        # Second poll with token B
        audit_poller._last_force_poll = 0
        audit_poller.credential_manager.get_token.return_value = "token-BBBBBBBB"
        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        # Old session should be replaced
        old = conn.execute(
            "SELECT * FROM usage_api_key_sessions WHERE token_fingerprint = ?",
            (fp_a,),
        ).fetchone()
        assert old["status"] == "replaced"

        # New session should be active
        fp_b = token_fingerprint("token-BBBBBBBB")
        new_session = get_active_key_session(conn)
        assert new_session["token_fingerprint"] == fp_b
        assert new_session["status"] == "active"

    def test_audit_write_failure_does_not_break_polling(self, audit_poller, conn):
        """If audit DB writes fail, polling still succeeds."""
        with patch("botfarm.usage.insert_usage_api_call", side_effect=Exception("DB locked")):
            self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        # Polling should still have succeeded
        assert audit_poller.state.utilization_5h == 0.42

    def test_429_with_retry_after_recorded(self, audit_poller, conn):
        """A 429 with Retry-After header records the value."""
        async def _raise_429(token, *, client=None):
            raise _make_429_error(retry_after="30")

        self._poll_with_fetch(audit_poller, conn, side_effect=_raise_429)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["retry_after"] == "30"

    def test_transient_retry_then_success_records_all_attempts(self, audit_poller, conn):
        """When a transient error is retried successfully, all attempts are recorded."""
        call_count = 0

        async def mock_fetch(token, *, client=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectTimeout("timed out")
            return SAMPLE_USAGE_RESPONSE

        self._poll_with_fetch(audit_poller, conn, side_effect=mock_fetch)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 2
        # Most recent first (newest first in get_usage_api_call_history)
        assert rows[0]["success"] == 1
        assert rows[1]["success"] == 0
        assert rows[1]["error_type"] == "timeout"

    def test_purge_includes_audit_rows(self, audit_poller, conn):
        """_purge_old_snapshots also purges old usage_api_calls."""
        # Insert an old audit row
        conn.execute(
            "INSERT INTO usage_api_calls (created_at, success, caller) "
            "VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-60 days'), 1, 'test')",
        )
        conn.commit()

        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        # Only the fresh row should remain (old one purged)
        rows = get_usage_api_call_history(conn, limit=100)
        assert len(rows) == 1
        assert rows[0]["caller"] == "force_poll"

    def test_purge_survives_missing_audit_table(self, audit_poller, conn):
        """_purge_old_snapshots doesn't break when usage_api_calls table is missing."""
        conn.execute("DROP TABLE IF EXISTS usage_api_calls")
        conn.commit()

        # Should succeed — the purge failure is logged but doesn't propagate
        self._poll_with_fetch(audit_poller, conn, response=SAMPLE_USAGE_RESPONSE)

        # Usage data was still stored despite the purge warning
        row = conn.execute(
            "SELECT * FROM usage_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        assert row is not None
        assert row["utilization_5h"] is not None


# ---------------------------------------------------------------------------
# refresh_usage_snapshot caller parameter
# ---------------------------------------------------------------------------


class TestRefreshUsageSnapshotCaller:
    def test_default_caller_cli_refresh(self, conn):
        """Default caller is 'cli_refresh'."""
        async def _ok(token, *, client=None):
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=_ok):
            with patch("botfarm.usage.CredentialManager.get_token", return_value="test-token"):
                refresh_usage_snapshot(conn)

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["caller"] == "cli_refresh"

    def test_custom_caller_dashboard(self, conn):
        """Custom caller='dashboard_refresh' is recorded."""
        async def _ok(token, *, client=None):
            return SAMPLE_USAGE_RESPONSE

        with patch("botfarm.usage.fetch_usage", side_effect=_ok):
            with patch("botfarm.usage.CredentialManager.get_token", return_value="test-token"):
                refresh_usage_snapshot(conn, caller="dashboard_refresh")

        rows = get_usage_api_call_history(conn, limit=10)
        assert len(rows) == 1
        assert rows[0]["caller"] == "dashboard_refresh"


# ---------------------------------------------------------------------------
# parse_retry_after
# ---------------------------------------------------------------------------


class TestParseRetryAfter:
    def test_none_returns_none(self):
        assert parse_retry_after(None) is None

    def test_empty_string_returns_none(self):
        assert parse_retry_after("") is None

    def test_integer_seconds(self):
        assert parse_retry_after("120") == 120

    def test_zero_seconds(self):
        assert parse_retry_after("0") == 0

    def test_negative_clamped_to_zero(self):
        assert parse_retry_after("-5") == 0

    def test_non_numeric_non_date_returns_none(self):
        assert parse_retry_after("not-a-number") is None

    def test_http_date_in_past_returns_zero(self):
        """An HTTP-date in the past should clamp to 0."""
        assert parse_retry_after("Fri, 01 Jan 2010 00:00:00 GMT") == 0


# ---------------------------------------------------------------------------
# _handle_429 with Retry-After header
# ---------------------------------------------------------------------------


class TestHandle429RetryAfter:
    @pytest.fixture(autouse=True)
    def _no_jitter(self):
        with patch("botfarm.usage.random.uniform", return_value=0):
            yield

    def test_429_with_retry_after_uses_header_when_larger(self, poller, conn):
        """When Retry-After exceeds exponential backoff, use the header value."""
        # poll_interval=10, first 429 → exponential = 10 * 2^1 = 20
        # Retry-After: 600 → should use 600
        response = _make_429_response(retry_after="600")
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        assert poller.effective_poll_interval == 600

    def test_429_with_retry_after_uses_exponential_when_larger(self, poller, conn):
        """When exponential backoff exceeds Retry-After, use the exponential value."""
        # poll_interval=10, first 429 → exponential = 10 * 2^1 = 20
        # Retry-After: 5 → should use 20
        response = _make_429_response(retry_after="5")
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        assert poller.effective_poll_interval == 20

    def test_429_without_retry_after_uses_exponential(self, poller, conn):
        """Without a Retry-After header, fall back to pure exponential backoff."""
        response = _make_429_response()  # no retry-after
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        assert poller.effective_poll_interval == poller.poll_interval * 2

    def test_429_with_retry_after_above_max_is_respected(self, poller, conn):
        """Server-specified Retry-After above MAX_ADAPTIVE_POLL_INTERVAL is not capped."""
        response = _make_429_response(retry_after="3600")
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller.effective_poll_interval == 3600

    def test_429_without_retry_after_exponential_capped_at_max(self, poller, conn):
        """Without Retry-After, exponential backoff is still capped at MAX."""
        response = _make_429_response()  # no retry-after
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
        # Drive exponential well past MAX with many consecutive 429s
        for _ in range(20):
            poller._last_force_poll = far_past
            poller._last_poll = far_past
            with patch.object(poller, "_fetch", side_effect=error):
                poller.force_poll(conn)

        assert poller.effective_poll_interval == MAX_ADAPTIVE_POLL_INTERVAL

    def test_429_with_unparseable_retry_after_falls_back(self, poller, conn):
        """An unparseable Retry-After header falls back to exponential backoff."""
        response = _make_429_response(retry_after="not-a-number")
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        # Falls back to exponential: 10 * 2^1 = 20
        assert poller.effective_poll_interval == poller.poll_interval * 2

    def test_consecutive_429s_with_retry_after(self, poller, conn):
        """Consecutive 429s with Retry-After header use max of header and exponential."""
        far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1

        # First 429: Retry-After=30, exponential=10*2=20 → use 30
        response1 = _make_429_response(retry_after="30")
        error1 = httpx.HTTPStatusError("", request=response1.request, response=response1)

        with patch.object(poller, "_fetch", side_effect=error1):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 1
        assert poller.effective_poll_interval == 30

        # Second 429: Retry-After=30, exponential=10*4=40 → use 40
        poller._last_force_poll = far_past
        poller._last_poll = far_past
        response2 = _make_429_response(retry_after="30")
        error2 = httpx.HTTPStatusError("", request=response2.request, response=response2)

        with patch.object(poller, "_fetch", side_effect=error2):
            poller.force_poll(conn)

        assert poller._consecutive_429s == 2
        assert poller.effective_poll_interval == 40


# ---------------------------------------------------------------------------
# 429 backoff state persistence across restarts
# ---------------------------------------------------------------------------


class TestBackoffStatePersistence:
    def test_429_persists_backoff_to_db(self, poller, conn):
        """A 429 response persists backoff state to the database."""
        from botfarm.db import load_backoff_state

        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        state = load_backoff_state(conn)
        assert state is not None
        assert state["consecutive_429s"] == 1
        assert state["backoff_until"] > time.time()

    def test_success_clears_persisted_backoff(self, poller, conn):
        """A successful poll after 429s clears the persisted backoff state."""
        from botfarm.db import load_backoff_state

        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        # Trigger 429
        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert load_backoff_state(conn) is not None

        # Successful poll (backoff expired)
        far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller._last_force_poll = far_past
            poller._last_poll = far_past
            poller.force_poll(conn)

        assert load_backoff_state(conn) is None

    def test_restore_active_backoff(self, conn):
        """New poller restores unexpired backoff from DB."""
        from botfarm.db import save_backoff_state

        save_backoff_state(conn, consecutive_429s=3, backoff_until=time.time() + 300)

        p = UsagePoller(poll_interval=10)
        p.restore_backoff_state(conn)

        assert p._consecutive_429s == 3
        assert p._active_poll_interval is not None
        assert p.in_429_backoff is True

    def test_restore_expired_backoff_polls_normally(self, conn):
        """New poller ignores expired backoff and polls normally."""
        from botfarm.db import load_backoff_state, save_backoff_state

        save_backoff_state(conn, consecutive_429s=2, backoff_until=time.time() - 10)

        p = UsagePoller(poll_interval=10)
        p.restore_backoff_state(conn)

        assert p._consecutive_429s == 0
        assert p._active_poll_interval is None
        # Expired state should be cleared from DB
        assert load_backoff_state(conn) is None

    def test_restore_no_state_is_noop(self, conn):
        """restore_backoff_state is a no-op when no state is stored."""
        p = UsagePoller(poll_interval=10)
        p.restore_backoff_state(conn)

        assert p._consecutive_429s == 0
        assert p._active_poll_interval is None

    def test_restore_respects_interval_above_max(self, conn):
        """Restored backoff respects intervals above MAX_ADAPTIVE_POLL_INTERVAL."""
        from botfarm.db import save_backoff_state

        # Simulate a server retry-after of 3600s that was persisted
        save_backoff_state(conn, consecutive_429s=1, backoff_until=time.time() + 3600)

        p = UsagePoller(poll_interval=10)
        p.restore_backoff_state(conn)

        # Should be ~3600, not capped at MAX_ADAPTIVE_POLL_INTERVAL (1800)
        assert p._active_poll_interval > MAX_ADAPTIVE_POLL_INTERVAL
        assert p._active_poll_interval == 3600

    def test_restored_backoff_blocks_poll(self, conn):
        """After restoring active backoff, poll() returns cached data."""
        from botfarm.db import save_backoff_state

        save_backoff_state(conn, consecutive_429s=2, backoff_until=time.time() + 300)

        cred_mgr = MagicMock(spec=CredentialManager)
        cred_mgr.get_token.return_value = "test-token"
        p = UsagePoller(credential_manager=cred_mgr, poll_interval=10)
        p.restore_backoff_state(conn)

        with patch.object(p, "_fetch", return_value=SAMPLE_USAGE_RESPONSE) as mock_fetch:
            p.poll(conn)

        mock_fetch.assert_not_called()
        assert p.last_polled_fresh is False


# ---------------------------------------------------------------------------
# Backoff jitter (SMA-420)
# ---------------------------------------------------------------------------


class TestBackoffJitter:
    def test_jitter_within_expected_range(self, poller, conn):
        """Jittered interval is between base and base * (1 + JITTER_FRACTION)."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        base = poller.poll_interval * 2  # first 429 → 2^1

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller.effective_poll_interval >= base
        assert poller.effective_poll_interval <= math.ceil(
            base * (1 + BACKOFF_JITTER_FRACTION)
        )

    def test_successive_intervals_not_identical(self, poller, conn):
        """Successive 429 backoff intervals are not all identical (jitter varies)."""
        response = _make_429_response()
        error = httpx.HTTPStatusError("", request=response.request, response=response)

        intervals = []
        far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
        cred_mgr = MagicMock(spec=CredentialManager)
        cred_mgr.get_token.return_value = "test-token"
        for _ in range(20):
            p = UsagePoller(credential_manager=cred_mgr, poll_interval=10)
            with patch.object(p, "_fetch", side_effect=error):
                p._last_force_poll = far_past
                p._last_poll = far_past
                p.force_poll(conn)
            intervals.append(p.effective_poll_interval)

        # With 20 samples and up to 50% jitter, not all should be equal
        assert len(set(intervals)) > 1

    def test_restore_uses_remaining_time_directly(self, conn):
        """Restored backoff uses remaining time from DB, not re-computed exponential."""
        from botfarm.db import save_backoff_state

        save_backoff_state(conn, consecutive_429s=2, backoff_until=time.time() + 300)

        p = UsagePoller(poll_interval=10)
        p.restore_backoff_state(conn)

        # Should use remaining time (~300s), not re-compute from exponential (40s)
        assert p._active_poll_interval == 300

    def test_jitter_still_capped_at_max(self, poller, conn):
        """Even with maximum jitter, the interval cannot exceed MAX."""
        with patch("botfarm.usage.random.uniform", return_value=BACKOFF_JITTER_FRACTION):
            response = _make_429_response()
            error = httpx.HTTPStatusError("", request=response.request, response=response)

            far_past = time.monotonic() - MAX_ADAPTIVE_POLL_INTERVAL - 1
            with patch.object(poller, "_fetch", side_effect=error):
                for _ in range(20):
                    poller._last_force_poll = far_past
                    poller._last_poll = far_past
                    poller.force_poll(conn)

            assert poller.effective_poll_interval == MAX_ADAPTIVE_POLL_INTERVAL

    def test_no_jitter_when_zero_fraction(self, poller, conn):
        """With jitter fraction = 0, interval equals the base value exactly."""
        with patch("botfarm.usage.random.uniform", return_value=0):
            response = _make_429_response()
            error = httpx.HTTPStatusError("", request=response.request, response=response)

            with patch.object(poller, "_fetch", side_effect=error):
                poller.force_poll(conn)

            assert poller.effective_poll_interval == poller.poll_interval * 2


# ---------------------------------------------------------------------------
# Auth failure (401) tracking and notifications
# ---------------------------------------------------------------------------


def _make_401_error():
    """Create an httpx.HTTPStatusError for a 401 response."""
    response = httpx.Response(
        401,
        request=httpx.Request("GET", "https://api.anthropic.com/api/oauth/usage"),
    )
    return httpx.HTTPStatusError("", request=response.request, response=response)


class TestAuthFailureTracking:
    @pytest.fixture()
    def notifier(self):
        n = MagicMock()
        n.enabled = True
        return n

    @pytest.fixture()
    def poller_with_notifier(self, notifier):
        cred_mgr = MagicMock(spec=CredentialManager)
        cred_mgr.get_token.return_value = "test-token"
        return UsagePoller(
            credential_manager=cred_mgr,
            poll_interval=10,
            notifier=notifier,
        )

    def test_401_increments_consecutive_count(self, poller_with_notifier, conn):
        poller = poller_with_notifier
        error = _make_401_error()
        poller.credential_manager.refresh_token.return_value = None

        with patch.object(poller, "_fetch", side_effect=error):
            poller.force_poll(conn)

        assert poller._consecutive_401s == 1

    def test_401_below_threshold_no_notification(self, poller_with_notifier, notifier, conn):
        poller = poller_with_notifier
        error = _make_401_error()
        poller.credential_manager.refresh_token.return_value = None

        # Do fewer than threshold 401s
        for i in range(AUTH_FAILURE_NOTIFY_THRESHOLD - 1):
            with patch.object(poller, "_fetch", side_effect=error):
                poller._last_poll = 0
                poller._last_force_poll = 0
                poller.force_poll(conn)

        assert poller._consecutive_401s == AUTH_FAILURE_NOTIFY_THRESHOLD - 1
        notifier.notify_auth_failure.assert_not_called()

    def test_401_at_threshold_triggers_notification(self, poller_with_notifier, notifier, conn):
        poller = poller_with_notifier
        error = _make_401_error()
        poller.credential_manager.refresh_token.return_value = None

        for i in range(AUTH_FAILURE_NOTIFY_THRESHOLD):
            with patch.object(poller, "_fetch", side_effect=error):
                poller._last_poll = 0
                poller._last_force_poll = 0
                poller.force_poll(conn)

        assert poller._consecutive_401s == AUTH_FAILURE_NOTIFY_THRESHOLD
        notifier.notify_auth_failure.assert_called_once()
        call_kwargs = notifier.notify_auth_failure.call_args[1]
        assert call_kwargs["consecutive_failures"] == AUTH_FAILURE_NOTIFY_THRESHOLD

    def test_401_continued_failures_keep_notifying(self, poller_with_notifier, notifier, conn):
        poller = poller_with_notifier
        error = _make_401_error()
        poller.credential_manager.refresh_token.return_value = None

        for i in range(AUTH_FAILURE_NOTIFY_THRESHOLD + 2):
            with patch.object(poller, "_fetch", side_effect=error):
                poller._last_poll = 0
                poller._last_force_poll = 0
                poller.force_poll(conn)

        # Called for each poll at or above the threshold
        assert notifier.notify_auth_failure.call_count == 3

    def test_successful_poll_resets_401_counter(self, poller_with_notifier, notifier, conn):
        poller = poller_with_notifier
        error = _make_401_error()
        poller.credential_manager.refresh_token.return_value = None

        # Accumulate some 401s
        for i in range(AUTH_FAILURE_NOTIFY_THRESHOLD):
            with patch.object(poller, "_fetch", side_effect=error):
                poller._last_poll = 0
                poller._last_force_poll = 0
                poller.force_poll(conn)

        assert poller._consecutive_401s == AUTH_FAILURE_NOTIFY_THRESHOLD

        # Successful poll resets the counter
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller._last_poll = 0
            poller._last_force_poll = 0
            poller.force_poll(conn)

        assert poller._consecutive_401s == 0
        assert poller._first_401_time is None

    def test_recovery_notification_sent_after_401_outage(self, poller_with_notifier, notifier, conn):
        poller = poller_with_notifier
        error = _make_401_error()
        poller.credential_manager.refresh_token.return_value = None

        # Build up 401s
        for i in range(AUTH_FAILURE_NOTIFY_THRESHOLD):
            with patch.object(poller, "_fetch", side_effect=error):
                poller._last_poll = 0
                poller._last_force_poll = 0
                poller.force_poll(conn)

        # Now succeed
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller._last_poll = 0
            poller._last_force_poll = 0
            poller.force_poll(conn)

        notifier.notify_auth_recovered.assert_called_once()

    def test_no_recovery_notification_if_no_prior_401s(self, poller_with_notifier, notifier, conn):
        # Successful poll with no prior 401s — no recovery notification
        with patch.object(poller_with_notifier, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller_with_notifier.force_poll(conn)

        notifier.notify_auth_recovered.assert_not_called()

    def test_401_refresh_succeeds_counts_as_recovery(self, poller_with_notifier, notifier, conn):
        poller = poller_with_notifier
        error = _make_401_error()
        poller.credential_manager.refresh_token.return_value = None

        # Build up 401 failures
        for i in range(AUTH_FAILURE_NOTIFY_THRESHOLD):
            with patch.object(poller, "_fetch", side_effect=error):
                poller._last_poll = 0
                poller._last_force_poll = 0
                poller.force_poll(conn)

        # Now 401 again, but refresh succeeds and retry works
        call_count = 0

        def fetch_side_effect(token):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise error
            return SAMPLE_USAGE_RESPONSE

        poller.credential_manager.refresh_token.return_value = "new-token"

        with patch.object(poller, "_fetch", side_effect=fetch_side_effect):
            poller._last_poll = 0
            poller._last_force_poll = 0
            poller.force_poll(conn)

        notifier.notify_auth_recovered.assert_called_once()
        assert poller._consecutive_401s == 0

    def test_no_notifier_does_not_crash(self, conn):
        """UsagePoller without a notifier should track 401s without crashing."""
        cred_mgr = MagicMock(spec=CredentialManager)
        cred_mgr.get_token.return_value = "test-token"
        cred_mgr.refresh_token.return_value = None
        poller = UsagePoller(credential_manager=cred_mgr, poll_interval=10)

        error = _make_401_error()

        for i in range(AUTH_FAILURE_NOTIFY_THRESHOLD + 1):
            with patch.object(poller, "_fetch", side_effect=error):
                poller._last_poll = 0
                poller._last_force_poll = 0
                poller.force_poll(conn)

        assert poller._consecutive_401s == AUTH_FAILURE_NOTIFY_THRESHOLD + 1

        # Recovery also doesn't crash
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller._last_poll = 0
            poller._last_force_poll = 0
            poller.force_poll(conn)

        assert poller._consecutive_401s == 0
