"""Tests for botfarm.usage — usage limit polling and snapshot storage."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from botfarm.credentials import CredentialManager
from botfarm.db import get_usage_snapshots, init_db
from botfarm.usage import (
    DEFAULT_PAUSE_5H_THRESHOLD,
    DEFAULT_PAUSE_7D_THRESHOLD,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_RETENTION_DAYS,
    MAX_RETRIES,
    TRANSIENT_EXCEPTIONS,
    UsagePoller,
    UsageState,
    refresh_usage_snapshot,
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


@pytest.fixture()
def conn(tmp_path):
    db_file = tmp_path / "test.db"
    connection = init_db(db_file)
    yield connection
    connection.close()


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
            def fake_poll(c):
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
            with patch("asyncio.sleep", new_callable=AsyncMock):
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
            with patch("asyncio.sleep", new_callable=AsyncMock):
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
            with patch("asyncio.sleep", new_callable=AsyncMock):
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
            with patch("asyncio.sleep", new_callable=AsyncMock):
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
            with patch("asyncio.sleep", new_callable=AsyncMock):
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

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                with pytest.raises(httpx.ConnectTimeout):
                    await poller._fetch_with_retry("test-token")

        # Should have slept between retries (MAX_RETRIES - 1 times)
        assert mock_sleep.call_count == MAX_RETRIES - 1

    @pytest.mark.asyncio
    async def test_fetch_with_retry_backoff_delays(self, poller):
        """Verify the correct backoff delays are used between retries."""
        mock_client = AsyncMock()
        mock_client.is_closed = False
        poller._client = mock_client

        async def mock_fetch(token, *, client=None):
            raise httpx.ConnectTimeout("timed out")

        with patch("botfarm.usage.fetch_usage", side_effect=mock_fetch):
            with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                with pytest.raises(httpx.ConnectTimeout):
                    await poller._fetch_with_retry("test-token")

        delays = [call.args[0] for call in mock_sleep.call_args_list]
        assert delays == [2, 5]


# ---------------------------------------------------------------------------
# Persistent HTTP client
# ---------------------------------------------------------------------------


class TestUsagePollerPersistentClient:
    def test_get_client_creates_client(self, poller):
        """_get_client creates a new AsyncClient on first use."""
        assert poller._client is None
        client = poller._get_client()
        assert client is not None
        assert isinstance(client, httpx.AsyncClient)
        # Clean up
        loop = asyncio.new_event_loop()
        loop.run_until_complete(client.aclose())
        loop.close()

    def test_get_client_reuses_existing(self, poller):
        """_get_client returns the same client on subsequent calls."""
        client1 = poller._get_client()
        client2 = poller._get_client()
        assert client1 is client2
        # Clean up
        loop = asyncio.new_event_loop()
        loop.run_until_complete(client1.aclose())
        loop.close()

    def test_get_client_recreates_if_closed(self, poller):
        """_get_client creates a new client if the existing one is closed."""
        client1 = poller._get_client()
        loop = asyncio.new_event_loop()
        loop.run_until_complete(client1.aclose())
        loop.close()

        client2 = poller._get_client()
        assert client2 is not client1
        assert not client2.is_closed
        # Clean up
        loop = asyncio.new_event_loop()
        loop.run_until_complete(client2.aclose())
        loop.close()

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
            with patch("asyncio.sleep", new_callable=AsyncMock):
                state = poller.force_poll(conn)

        assert state.utilization_5h == 0.42


