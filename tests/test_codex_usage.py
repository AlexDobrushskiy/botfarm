"""Tests for botfarm.codex_usage — ChatGPT backend API polling for Codex rate limits."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from botfarm.codex_usage import (
    CODEX_USAGE_URL,
    CodexUsagePoller,
    CodexUsageState,
    _unix_to_iso,
)
from botfarm.config import CodexUsageConfig
from botfarm.credentials import CodexCredentials
from botfarm.db import get_codex_usage_snapshots

# --- Sample API responses ---

SAMPLE_USAGE_RESPONSE = {
    "plan_type": "pro",
    "rate_limit": {
        "allowed": True,
        "limit_reached": False,
        "primary_window": {
            "used_percent": 42,
            "limit_window_seconds": 18000,
            "reset_after_seconds": 120,
            "reset_at": 1735689720,
        },
        "secondary_window": {
            "used_percent": 5,
            "limit_window_seconds": 604800,
            "reset_after_seconds": 43200,
            "reset_at": 1735693200,
        },
    },
}

EMPTY_USAGE_RESPONSE = {
    "plan_type": "pro",
    "rate_limit": {
        "allowed": True,
        "limit_reached": False,
    },
}

LIMIT_REACHED_RESPONSE = {
    "plan_type": "pro",
    "rate_limit": {
        "allowed": False,
        "limit_reached": True,
        "primary_window": {
            "used_percent": 100,
            "limit_window_seconds": 18000,
            "reset_after_seconds": 3600,
            "reset_at": 1735700000,
        },
    },
}


# --- Helper ---

FAKE_CREDS = CodexCredentials(access_token="test-token", account_id="acct-123")


# --- CodexUsageState tests ---


class TestCodexUsageState:
    def test_should_pause_disabled(self):
        state = CodexUsageState(primary_used_pct=0.90)
        pause, reason = state.should_pause(enabled=False)
        assert not pause
        assert reason is None

    def test_should_pause_below_threshold(self):
        state = CodexUsageState(primary_used_pct=0.50, secondary_used_pct=0.30)
        pause, reason = state.should_pause(
            primary_threshold=0.85, secondary_threshold=0.90, enabled=True,
        )
        assert not pause

    def test_should_pause_primary_at_threshold(self):
        state = CodexUsageState(primary_used_pct=0.85, secondary_used_pct=0.30)
        pause, reason = state.should_pause(
            primary_threshold=0.85, secondary_threshold=0.90, enabled=True,
        )
        assert pause
        assert "primary" in reason.lower()
        assert "85" in reason

    def test_should_pause_secondary_at_threshold(self):
        state = CodexUsageState(primary_used_pct=0.50, secondary_used_pct=0.90)
        pause, reason = state.should_pause(
            primary_threshold=0.85, secondary_threshold=0.90, enabled=True,
        )
        assert pause
        assert "secondary" in reason.lower()

    def test_should_pause_rate_limit_not_allowed(self):
        state = CodexUsageState(
            primary_used_pct=0.10, rate_limit_allowed=False,
        )
        pause, reason = state.should_pause(enabled=True)
        assert pause
        assert "allowed=false" in reason.lower()

    def test_should_pause_none_utilization(self):
        state = CodexUsageState()
        pause, reason = state.should_pause(enabled=True)
        assert not pause

    def test_to_dict(self):
        state = CodexUsageState(
            plan_type="pro",
            primary_used_pct=0.42,
            primary_reset_at="2025-01-01T00:02:00Z",
            primary_window_seconds=18000,
            secondary_used_pct=0.05,
            secondary_reset_at="2025-01-01T01:00:00Z",
            secondary_window_seconds=604800,
            rate_limit_allowed=True,
            last_polled_at="2026-03-07T00:00:00Z",
        )
        d = state.to_dict()
        assert d["plan_type"] == "pro"
        assert d["primary_used_pct"] == 0.42
        assert d["secondary_used_pct"] == 0.05
        assert d["rate_limit_allowed"] is True


# --- CodexUsagePoller tests ---


class TestCodexUsagePoller:
    def _make_poller(self, *, enabled=True, interval=300):
        config = CodexUsageConfig(
            enabled=enabled,
            poll_interval_seconds=interval,
        )
        return CodexUsagePoller(config=config)

    def test_disabled_poller_noop(self, conn):
        poller = self._make_poller(enabled=False)
        state = poller.poll(conn)
        assert not poller.last_polled_fresh
        assert state.primary_used_pct is None

    def test_enabled_property(self):
        poller = self._make_poller(enabled=True)
        assert poller.enabled
        poller2 = self._make_poller(enabled=False)
        assert not poller2.enabled

    def test_poll_respects_interval(self, conn):
        poller = self._make_poller(interval=300)

        with patch.object(poller, "_do_poll"):
            poller.poll(conn)
            assert poller.last_polled_fresh

            poller.poll(conn)
            assert not poller.last_polled_fresh

    def test_poll_parses_response(self, conn):
        poller = self._make_poller()

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.poll(conn)

        state = poller.state
        assert state.plan_type == "pro"
        assert state.primary_used_pct == pytest.approx(0.42)
        assert state.secondary_used_pct == pytest.approx(0.05)
        assert state.primary_window_seconds == 18000
        assert state.secondary_window_seconds == 604800
        assert state.rate_limit_allowed is True
        assert state.last_polled_at is not None

    def test_poll_stores_snapshot(self, conn):
        poller = self._make_poller()

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.poll(conn)

        snapshots = get_codex_usage_snapshots(conn, limit=1)
        assert len(snapshots) == 1
        snap = snapshots[0]
        assert snap["primary_used_pct"] == pytest.approx(0.42)
        assert snap["secondary_used_pct"] == pytest.approx(0.05)
        assert snap["plan_type"] == "pro"
        raw = json.loads(snap["raw_json"])
        assert "rate_limit" in raw

    def test_poll_empty_windows(self, conn):
        poller = self._make_poller()

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=EMPTY_USAGE_RESPONSE):
            poller.poll(conn)

        state = poller.state
        assert state.primary_used_pct is None
        assert state.secondary_used_pct is None

    def test_poll_limit_reached(self, conn):
        poller = self._make_poller()

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=LIMIT_REACHED_RESPONSE):
            poller.poll(conn)

        state = poller.state
        assert state.rate_limit_allowed is False
        assert state.primary_used_pct == pytest.approx(1.0)

    def test_poll_api_failure_uses_last_known(self, conn):
        poller = self._make_poller()

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.poll(conn)

        assert poller.state.primary_used_pct == pytest.approx(0.42)

        poller._last_poll = 0.0

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", side_effect=ConnectionError("fail")):
            poller.poll(conn)

        assert poller.state.primary_used_pct == pytest.approx(0.42)

    def test_poll_missing_credentials(self, conn):
        poller = self._make_poller()

        from botfarm.credentials import CredentialError
        with patch(
            "botfarm.codex_usage.load_codex_credentials",
            side_effect=CredentialError("no file"),
        ):
            poller.poll(conn)

        assert poller.state.primary_used_pct is None

    def test_force_poll_ignores_interval(self, conn):
        poller = self._make_poller(interval=9999)

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.poll(conn)
            assert poller.last_polled_fresh

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=EMPTY_USAGE_RESPONSE):
            poller.force_poll(conn)
            assert poller.last_polled_fresh

    def test_force_poll_disabled_noop(self, conn):
        poller = self._make_poller(enabled=False)
        state = poller.force_poll(conn)
        assert not poller.last_polled_fresh
        assert state.primary_used_pct is None

    def test_purge_old_snapshots(self, conn):
        poller = self._make_poller()
        poller.retention_days = 0

        with patch(
            "botfarm.codex_usage.load_codex_credentials", return_value=FAKE_CREDS,
        ), patch.object(poller, "_fetch", return_value=SAMPLE_USAGE_RESPONSE):
            poller.poll(conn)

        snapshots = get_codex_usage_snapshots(conn)
        assert isinstance(snapshots, list)

    def test_close_is_noop(self):
        poller = self._make_poller()
        poller.close()  # should not raise

    def test_fetch_calls_chatgpt_api(self):
        poller = self._make_poller()

        mock_resp = MagicMock()
        mock_resp.json.return_value = SAMPLE_USAGE_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        with patch("botfarm.codex_usage.cffi_requests") as mock_cffi:
            mock_cffi.get.return_value = mock_resp
            result = poller._fetch("test-token", "acct-123")

        assert result == SAMPLE_USAGE_RESPONSE
        call_kwargs = mock_cffi.get.call_args
        assert call_kwargs[0][0] == CODEX_USAGE_URL
        headers = call_kwargs[1]["headers"]
        assert headers["Authorization"] == "Bearer test-token"
        assert headers["ChatGPT-Account-Id"] == "acct-123"
        assert call_kwargs[1]["impersonate"] == "chrome"


class TestCodexUsageConfigValidation:
    def test_valid_config(self):
        config = CodexUsageConfig(
            enabled=True,
            poll_interval_seconds=300,
            pause_primary_threshold=0.85,
            pause_secondary_threshold=0.90,
        )
        assert config.enabled

    def test_default_config_disabled(self):
        config = CodexUsageConfig()
        assert not config.enabled
        assert config.poll_interval_seconds == 300


class TestUnixToIso:
    def test_none(self):
        assert _unix_to_iso(None) is None

    def test_valid(self):
        result = _unix_to_iso(1735689720)
        assert result == "2025-01-01T00:02:00Z"

    def test_zero(self):
        result = _unix_to_iso(0)
        assert result == "1970-01-01T00:00:00Z"
