"""Tests for botfarm.usage — usage limit polling and snapshot storage."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from botfarm.credentials import CredentialManager
from botfarm.db import get_usage_snapshots, init_db
from botfarm.usage import (
    DEFAULT_PAUSE_5H_THRESHOLD,
    DEFAULT_PAUSE_7D_THRESHOLD,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_RETENTION_DAYS,
    PAUSE_THRESHOLD,
    UsagePoller,
    UsageState,
)

# --- Sample API response ---

SAMPLE_USAGE_RESPONSE = {
    "five_hour": {
        "utilization": 0.42,
        "resets_at": "2026-02-12T22:00:00Z",
    },
    "seven_day": {
        "utilization": 0.15,
        "resets_at": "2026-02-18T00:00:00Z",
    },
}

HIGH_USAGE_RESPONSE = {
    "five_hour": {
        "utilization": 0.95,
        "resets_at": "2026-02-12T22:00:00Z",
    },
    "seven_day": {
        "utilization": 0.60,
        "resets_at": "2026-02-18T00:00:00Z",
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

    def test_should_pause_false_when_none(self):
        state = UsageState()
        assert state.should_pause is False

    def test_should_pause_false_below_threshold(self):
        state = UsageState(utilization_5h=0.5)
        assert state.should_pause is False

    def test_should_pause_true_at_threshold(self):
        state = UsageState(utilization_5h=PAUSE_THRESHOLD)
        assert state.should_pause is True

    def test_should_pause_true_above_threshold(self):
        state = UsageState(utilization_5h=0.99)
        assert state.should_pause is True

    def test_to_dict(self):
        state = UsageState(
            utilization_5h=0.42,
            utilization_7d=0.15,
            resets_at_5h="2026-02-12T22:00:00Z",
            resets_at_7d="2026-02-18T00:00:00Z",
        )
        d = state.to_dict()
        assert d == {
            "utilization_5h": 0.42,
            "utilization_7d": 0.15,
            "resets_at_5h": "2026-02-12T22:00:00Z",
            "resets_at_7d": "2026-02-18T00:00:00Z",
        }

    def test_to_dict_none_values(self):
        state = UsageState()
        d = state.to_dict()
        assert all(v is None for v in d.values())

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
            "five_hour": {"utilization": 0.5, "resets_at": "2026-02-12T22:00:00Z"},
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
# Dispatch gating (should_pause)
# ---------------------------------------------------------------------------


class TestDispatchGating:
    def test_high_utilization_pauses_dispatch(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=HIGH_USAGE_RESPONSE):
            poller.force_poll(conn)

        assert poller.state.should_pause is True

    def test_normal_utilization_allows_dispatch(self, poller, conn):
        with patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.force_poll(conn)

        assert poller.state.should_pause is False
