"""Tests for botfarm.credentials module."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from botfarm.credentials import (
    OAUTH_CLIENT_ID,
    OAUTH_SCOPES,
    OAUTH_TOKEN_URL,
    TOKEN_EXPIRY_BUFFER_MS,
    USAGE_API_TIMEOUT,
    USAGE_API_URL,
    CodexCredentials,
    CredentialError,
    CredentialManager,
    OAuthToken,
    _extract_token,
    _load_token,
    _load_token_linux,
    _refresh_oauth_token,
    _save_token_linux,
    load_codex_credentials,
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
    assert token.refresh_token == "test-refresh-token"


def test_extract_token_minimal():
    token = _extract_token(VALID_CREDENTIALS_MINIMAL)
    assert token.access_token == "minimal-token"
    assert token.expires_at is None
    assert token.refresh_token is None


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


def test_credential_manager_refresh_token_oauth(monkeypatch, tmp_path):
    """refresh_token() performs an actual OAuth refresh when refreshToken is present."""
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mock_response = httpx.Response(
        200,
        json={
            "access_token": "new-oauth-token",
            "refresh_token": "new-refresh-token",
            "expires_in": 3600,
        },
        request=httpx.Request("POST", OAUTH_TOKEN_URL),
    )

    mgr = CredentialManager()
    with patch("botfarm.credentials.httpx.post", return_value=mock_response):
        token = mgr.refresh_token()

    assert token == "new-oauth-token"

    # Verify the new token was written back to the credential file
    saved = json.loads(cred_file.read_text())
    assert saved["claudeAiOauth"]["accessToken"] == "new-oauth-token"
    assert saved["claudeAiOauth"]["refreshToken"] == "new-refresh-token"


def test_credential_manager_refresh_no_refresh_token(monkeypatch, tmp_path):
    """refresh_token() falls back to disk read when no refreshToken exists."""
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS_MINIMAL))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    token = mgr.refresh_token()
    assert token == "minimal-token"


def test_credential_manager_refresh_oauth_failure_falls_back(monkeypatch, tmp_path):
    """On OAuth refresh failure, falls back to re-reading from disk."""
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS))
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    with patch(
        "botfarm.credentials._refresh_oauth_token",
        side_effect=CredentialError("refresh failed"),
    ):
        token = mgr.refresh_token()

    # Should fall back to disk read
    assert token == "test-access-token-123"


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


# --- is_token_expired ---


def test_is_token_expired_future(monkeypatch, tmp_path):
    """Token with far-future expiresAt is not expired."""
    future_ms = int(time.time() * 1000) + 3600_000  # 1 hour from now
    creds = {
        "claudeAiOauth": {
            "accessToken": "tok",
            "refreshToken": "rt",
            "expiresAt": future_ms,
        },
    }
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(creds))
    monkeypatch.setattr("botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file)
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.is_token_expired() is False


def test_is_token_expired_past(monkeypatch, tmp_path):
    """Token with past expiresAt is expired."""
    past_ms = int(time.time() * 1000) - 3600_000  # 1 hour ago
    creds = {
        "claudeAiOauth": {
            "accessToken": "tok",
            "refreshToken": "rt",
            "expiresAt": past_ms,
        },
    }
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(creds))
    monkeypatch.setattr("botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file)
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.is_token_expired() is True


def test_is_token_expired_within_buffer(monkeypatch, tmp_path):
    """Token expiring within the buffer period is considered expired."""
    # Expires 2 minutes from now, but buffer is 5 minutes
    near_future_ms = int(time.time() * 1000) + 2 * 60 * 1000
    creds = {
        "claudeAiOauth": {
            "accessToken": "tok",
            "refreshToken": "rt",
            "expiresAt": near_future_ms,
        },
    }
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(creds))
    monkeypatch.setattr("botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file)
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.is_token_expired() is True


def test_is_token_expired_no_expires_at(monkeypatch, tmp_path):
    """Token without expiresAt is assumed valid."""
    cred_file = tmp_path / ".credentials.json"
    cred_file.write_text(json.dumps(VALID_CREDENTIALS_MINIMAL))
    monkeypatch.setattr("botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file)
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.is_token_expired() is False


def test_is_token_expired_no_credentials(monkeypatch, tmp_path):
    """Missing credential file means expired (conservative)."""
    monkeypatch.setattr(
        "botfarm.credentials.LINUX_CREDENTIALS_PATH", tmp_path / "missing.json"
    )
    monkeypatch.setattr("botfarm.credentials.platform.system", lambda: "Linux")

    mgr = CredentialManager()
    assert mgr.is_token_expired() is True


# --- _refresh_oauth_token ---


def test_refresh_oauth_token_success():
    """Successful OAuth refresh returns new token data."""
    mock_response = httpx.Response(
        200,
        json={
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 7200,
            "token_type": "bearer",
        },
        request=httpx.Request("POST", OAUTH_TOKEN_URL),
    )

    with patch("botfarm.credentials.httpx.post", return_value=mock_response) as mock_post:
        token = _refresh_oauth_token("old-refresh-token")

    assert token.access_token == "new-access"
    assert token.refresh_token == "new-refresh"
    assert token.expires_at is not None
    # Verify request payload
    call_kwargs = mock_post.call_args
    assert call_kwargs[1]["json"]["grant_type"] == "refresh_token"
    assert call_kwargs[1]["json"]["refresh_token"] == "old-refresh-token"
    assert call_kwargs[1]["json"]["client_id"] == OAUTH_CLIENT_ID
    assert call_kwargs[1]["json"]["scope"] == OAUTH_SCOPES


def test_refresh_oauth_token_http_error():
    """Network error during refresh raises CredentialError."""
    with patch(
        "botfarm.credentials.httpx.post",
        side_effect=httpx.ConnectError("connection refused"),
    ):
        with pytest.raises(CredentialError, match="OAuth refresh request failed"):
            _refresh_oauth_token("some-refresh-token")


def test_refresh_oauth_token_non_200():
    """Non-200 response raises CredentialError with status code."""
    mock_response = httpx.Response(
        401,
        text="unauthorized",
        request=httpx.Request("POST", OAUTH_TOKEN_URL),
    )
    with patch("botfarm.credentials.httpx.post", return_value=mock_response):
        with pytest.raises(CredentialError, match="HTTP 401"):
            _refresh_oauth_token("expired-refresh-token")


def test_refresh_oauth_token_invalid_json():
    """Invalid JSON in response raises CredentialError."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.side_effect = json.JSONDecodeError("err", "", 0)
    with patch("botfarm.credentials.httpx.post", return_value=mock_response):
        with pytest.raises(CredentialError, match="invalid JSON"):
            _refresh_oauth_token("refresh-token")


def test_refresh_oauth_token_missing_access_token():
    """Response without access_token raises CredentialError."""
    mock_response = httpx.Response(
        200,
        json={"refresh_token": "new-refresh"},
        request=httpx.Request("POST", OAUTH_TOKEN_URL),
    )
    with patch("botfarm.credentials.httpx.post", return_value=mock_response):
        with pytest.raises(CredentialError, match="missing access_token"):
            _refresh_oauth_token("refresh-token")


def test_refresh_oauth_token_no_expires_in():
    """Response without expires_in still succeeds with expires_at=None."""
    mock_response = httpx.Response(
        200,
        json={"access_token": "new-access"},
        request=httpx.Request("POST", OAUTH_TOKEN_URL),
    )
    with patch("botfarm.credentials.httpx.post", return_value=mock_response):
        token = _refresh_oauth_token("refresh-token")
    assert token.access_token == "new-access"
    assert token.expires_at is None
    assert token.refresh_token is None


# --- _save_token_linux ---


def test_save_token_linux_writes_correctly(tmp_path, monkeypatch):
    """Saving a token updates the credential file atomically."""
    cred_file = tmp_path / ".credentials.json"
    existing = {
        "claudeAiOauth": {
            "accessToken": "old-token",
            "refreshToken": "old-refresh",
            "expiresAt": 1000,
            "scopes": ["user:profile"],
        },
        "mcpOAuth": {"some": "data"},
    }
    cred_file.write_text(json.dumps(existing))
    monkeypatch.setattr("botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file)

    new_token = OAuthToken(
        access_token="new-access",
        refresh_token="new-refresh",
        expires_at=9999999,
    )
    _save_token_linux(new_token)

    saved = json.loads(cred_file.read_text())
    assert saved["claudeAiOauth"]["accessToken"] == "new-access"
    assert saved["claudeAiOauth"]["refreshToken"] == "new-refresh"
    assert saved["claudeAiOauth"]["expiresAt"] == 9999999
    # Preserves other fields
    assert saved["mcpOAuth"] == {"some": "data"}
    assert saved["claudeAiOauth"]["scopes"] == ["user:profile"]


def test_save_token_linux_creates_if_no_existing(tmp_path, monkeypatch):
    """Saving when no credential file exists creates it."""
    cred_file = tmp_path / ".credentials.json"
    monkeypatch.setattr("botfarm.credentials.LINUX_CREDENTIALS_PATH", cred_file)

    new_token = OAuthToken(
        access_token="fresh-token",
        refresh_token="fresh-refresh",
        expires_at=12345,
    )
    _save_token_linux(new_token)

    assert cred_file.exists()
    saved = json.loads(cred_file.read_text())
    assert saved["claudeAiOauth"]["accessToken"] == "fresh-token"


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
    call_headers = mock_client.get.call_args[1]["headers"]
    assert call_headers["Authorization"] == "Bearer test-token"
    assert call_headers["anthropic-beta"] == "oauth-2025-04-20"
    assert call_headers["User-Agent"].startswith("claude-code/")


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


# --- Codex credentials ---

VALID_CODEX_AUTH = {
    "auth_mode": "chatgpt",
    "tokens": {
        "access_token": "eyJ-test-codex-token",
        "refresh_token": "rt_test",
        "account_id": "acct-12345",
    },
}


def test_load_codex_credentials_success(tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(json.dumps(VALID_CODEX_AUTH))

    creds = load_codex_credentials(auth_path=auth_file)
    assert creds.access_token == "eyJ-test-codex-token"
    assert creds.account_id == "acct-12345"


def test_load_codex_credentials_file_not_found(tmp_path):
    with pytest.raises(CredentialError, match="not found"):
        load_codex_credentials(auth_path=tmp_path / "missing.json")


def test_load_codex_credentials_missing_tokens(tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(json.dumps({"auth_mode": "chatgpt"}))

    with pytest.raises(CredentialError, match="tokens"):
        load_codex_credentials(auth_path=auth_file)


def test_load_codex_credentials_missing_access_token(tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(json.dumps({
        "tokens": {"account_id": "acct-123"},
    }))

    with pytest.raises(CredentialError, match="access_token"):
        load_codex_credentials(auth_path=auth_file)


def test_load_codex_credentials_missing_account_id(tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(json.dumps({
        "tokens": {"access_token": "eyJ-token"},
    }))

    with pytest.raises(CredentialError, match="account_id"):
        load_codex_credentials(auth_path=auth_file)


def test_load_codex_credentials_reads_fresh(tmp_path):
    auth_file = tmp_path / "auth.json"
    auth_file.write_text(json.dumps(VALID_CODEX_AUTH))

    creds1 = load_codex_credentials(auth_path=auth_file)
    assert creds1.access_token == "eyJ-test-codex-token"

    # Update file — next read should pick up new token
    updated = {
        "tokens": {
            "access_token": "eyJ-rotated",
            "account_id": "acct-12345",
        },
    }
    auth_file.write_text(json.dumps(updated))
    creds2 = load_codex_credentials(auth_path=auth_file)
    assert creds2.access_token == "eyJ-rotated"
