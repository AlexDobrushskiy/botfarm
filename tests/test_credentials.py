"""Tests for botfarm.credentials module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from botfarm.credentials import (
    USAGE_API_TIMEOUT,
    USAGE_API_URL,
    CredentialError,
    CredentialManager,
    OAuthToken,
    _extract_token,
    _load_token,
    _load_token_linux,
    _load_token_macos,
    fetch_usage,
)

# --- Sample credential data ---

VALID_CREDENTIALS = {
    "claudeAiOauth": {
        "accessToken": "test-access-token-123",
        "refreshToken": "test-refresh-token",
        "expiresAt": "2026-12-31T23:59:59Z",
        "scopes": ["read", "write"],
    },
    "mcpOAuth": {},
}

VALID_CREDENTIALS_MINIMAL = {
    "claudeAiOauth": {
        "accessToken": "minimal-token",
    },
}


# --- _extract_token ---


def test_extract_token_valid():
    token = _extract_token(VALID_CREDENTIALS)
    assert token.access_token == "test-access-token-123"
    assert token.expires_at == "2026-12-31T23:59:59Z"


def test_extract_token_minimal():
    token = _extract_token(VALID_CREDENTIALS_MINIMAL)
    assert token.access_token == "minimal-token"
    assert token.expires_at is None


def test_extract_token_missing_oauth_section():
    with pytest.raises(CredentialError, match="claudeAiOauth"):
        _extract_token({"other": "data"})


def test_extract_token_oauth_not_dict():
    with pytest.raises(CredentialError, match="claudeAiOauth"):
        _extract_token({"claudeAiOauth": "not-a-dict"})


def test_extract_token_missing_access_token():
    with pytest.raises(CredentialError, match="accessToken"):
        _extract_token({"claudeAiOauth": {"refreshToken": "abc"}})


def test_extract_token_empty_access_token():
    with pytest.raises(CredentialError, match="accessToken"):
        _extract_token({"claudeAiOauth": {"accessToken": ""}})


def test_extract_token_non_string_access_token():
    with pytest.raises(CredentialError, match="accessToken"):
        _extract_token({"claudeAiOauth": {"accessToken": 12345}})


# --- _load_token_linux ---


def test_load_token_linux_success(tmp_path, monkeypatch):
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    token = _load_token_linux()
    assert token.access_token == "test-access-token-123"


def test_load_token_linux_file_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH",
        tmp_path / "nonexistent.json",
    )
    with pytest.raises(CredentialError, match="not found"):
        _load_token_linux()


def test_load_token_linux_invalid_json(tmp_path, monkeypatch):
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text("not json {{{")
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    with pytest.raises(CredentialError, match="Failed to read"):
        _load_token_linux()


def test_load_token_linux_missing_oauth_key(tmp_path, monkeypatch):
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps({"other": "data"}))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    with pytest.raises(CredentialError, match="claudeAiOauth"):
        _load_token_linux()


# --- _load_token_macos ---


def test_load_token_macos_success():
    mock_result = type("Result", (), {
        "returncode": 0,
        "stdout": json.dumps(VALID_CREDENTIALS),
        "stderr": "",
    })()
    with patch("botfarm.credentials.subprocess.run", return_value=mock_result):
        token = _load_token_macos()
    assert token.access_token == "test-access-token-123"


def test_load_token_macos_command_not_found():
    with patch(
        "botfarm.credentials.subprocess.run",
        side_effect=FileNotFoundError("security not found"),
    ):
        with pytest.raises(CredentialError, match="Failed to run"):
            _load_token_macos()


def test_load_token_macos_timeout():
    import subprocess

    with patch(
        "botfarm.credentials.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd="security", timeout=10),
    ):
        with pytest.raises(CredentialError, match="Failed to run"):
            _load_token_macos()


def test_load_token_macos_nonzero_exit():
    mock_result = type("Result", (), {
        "returncode": 44,
        "stdout": "",
        "stderr": "The specified item could not be found",
    })()
    with patch("botfarm.credentials.subprocess.run", return_value=mock_result):
        with pytest.raises(CredentialError, match="Keychain lookup failed"):
            _load_token_macos()


def test_load_token_macos_invalid_json():
    mock_result = type("Result", (), {
        "returncode": 0,
        "stdout": "not-json",
        "stderr": "",
    })()
    with patch("botfarm.credentials.subprocess.run", return_value=mock_result):
        with pytest.raises(CredentialError, match="not valid JSON"):
            _load_token_macos()


# --- _load_token (OS dispatch) ---


def test_load_token_linux(monkeypatch, tmp_path):
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    token = _load_token()
    assert token.access_token == "test-access-token-123"


def test_load_token_macos_dispatch(monkeypatch):
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Darwin")
    mock_result = type("Result", (), {
        "returncode": 0,
        "stdout": json.dumps(VALID_CREDENTIALS),
        "stderr": "",
    })()
    with patch("botfarm.credentials.subprocess.run", return_value=mock_result):
        token = _load_token()
    assert token.access_token == "test-access-token-123"


def test_load_token_unsupported_os(monkeypatch):
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Windows")
    with pytest.raises(CredentialError, match="Unsupported platform"):
        _load_token()


# --- CredentialManager ---


def test_credential_manager_get_token(monkeypatch, tmp_path):
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.get_token() == "test-access-token-123"


def test_credential_manager_reads_fresh_on_every_call(monkeypatch, tmp_path):
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.get_token() == "test-access-token-123"

    # Update the file — next call should pick up the new token
    new_creds = {
        "claudeAiOauth": {"accessToken": "rotated-token-789"},
    }
    cred_file.write_text(json.dumps(new_creds))
    assert mgr.get_token() == "rotated-token-789"


def test_credential_manager_refresh_token(monkeypatch, tmp_path):
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    mgr.get_token()

    # Update the credential file with a new token
    new_creds = {
        "claudeAiOauth": {"accessToken": "refreshed-token-456"},
    }
    cred_file.write_text(json.dumps(new_creds))

    token = mgr.refresh_token()
    assert token == "refreshed-token-456"


def test_credential_manager_returns_none_on_error(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH",
        tmp_path / "nonexistent.json",
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.get_token() is None


def test_credential_manager_returns_none_does_not_cache(monkeypatch, tmp_path):
    """After a failed load, a subsequent call should retry (not cache None)."""
    cred_file = tmp_path / ".credentials.json"
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()

    # First call — file doesn't exist
    assert mgr.get_token() is None

    # Now create the file
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    assert mgr.get_token() == "test-access-token-123"


# --- fetch_usage ---


@pytest.mark.asyncio
async def test_fetch_usage_success():
    expected = {"daily_cost_usd": 1.5, "monthly_cost_usd": 30.0}

    mock_response = httpx.Response(
        200,
        json=expected,
        request=httpx.Request("GET", USAGE_API_URL),
    )

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("botfarm.credentials.httpx.AsyncClient", return_value=mock_client):
        result = await fetch_usage("test-token")

    assert result == expected
    mock_client.get.assert_called_once_with(
        USAGE_API_URL,
        headers={
            "Authorization": "Bearer test-token",
            "anthropic-beta": "oauth-2025-04-20",
        },
        timeout=USAGE_API_TIMEOUT,
    )


@pytest.mark.asyncio
async def test_fetch_usage_401():
    mock_response = httpx.Response(
        401,
        request=httpx.Request("GET", USAGE_API_URL),
    )

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("botfarm.credentials.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_usage("expired-token")


@pytest.mark.asyncio
async def test_fetch_usage_with_external_client():
    """When a caller provides their own client, it is used directly."""
    expected = {"daily_cost_usd": 2.0}
    mock_response = httpx.Response(
        200,
        json=expected,
        request=httpx.Request("GET", USAGE_API_URL),
    )

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response

    result = await fetch_usage("test-token", client=mock_client)

    assert result == expected
    mock_client.get.assert_called_once()


# --- Timeout configuration ---


def test_usage_api_timeout_has_separate_connect_and_read():
    assert USAGE_API_TIMEOUT.connect == 10
    assert USAGE_API_TIMEOUT.read == 30
