"""Retrieve OAuth tokens for the usage API.

Claude Code: macOS (system keychain) / Linux (~/.claude/.credentials.json).
Codex CLI: ~/.codex/auth.json (file-based, all platforms).
"""

from __future__ import annotations

import json
import logging
import os
import platform
import re
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import shutil

import httpx

logger = logging.getLogger(__name__)

USAGE_API_URL = "https://api.anthropic.com/api/oauth/usage"
USAGE_API_BETA_HEADER = "oauth-2025-04-20"
USAGE_API_TIMEOUT = httpx.Timeout(30, connect=10)

# OAuth token refresh configuration
OAUTH_TOKEN_URL = "https://platform.claude.com/v1/oauth/token"
OAUTH_CLIENT_ID = "9d1c250a-e61b-44d9-88ed-5944d1962f5e"
OAUTH_SCOPES = "user:profile user:inference user:sessions:claude_code user:mcp_servers user:file_upload"
OAUTH_REFRESH_TIMEOUT = httpx.Timeout(15, connect=10)

# Buffer before actual expiry to trigger proactive refresh (5 minutes)
TOKEN_EXPIRY_BUFFER_MS = 5 * 60 * 1000

# Fallback version if `claude --version` is unavailable
_FALLBACK_CLAUDE_VERSION = "2.1.79"
_cached_claude_version: str | None = None


def _get_claude_version() -> str:
    """Detect the installed Claude Code version, cached after first call."""
    global _cached_claude_version
    if _cached_claude_version is not None:
        return _cached_claude_version

    if not shutil.which("claude"):
        _cached_claude_version = _FALLBACK_CLAUDE_VERSION
        return _cached_claude_version

    try:
        result = subprocess.run(
            ["claude", "--version"],
            capture_output=True, text=True, timeout=5,
        )
        # Output format: "2.1.79 (Claude Code)"
        version = result.stdout.strip().split()[0] if result.returncode == 0 else None
    except Exception:
        version = None

    _cached_claude_version = version or _FALLBACK_CLAUDE_VERSION
    return _cached_claude_version

# Linux credential file location
LINUX_CREDENTIALS_PATH = Path.home() / ".claude" / ".credentials.json"

# macOS keychain service name
MACOS_KEYCHAIN_SERVICE = "Claude Code-credentials"


class CredentialError(Exception):
    """Raised when credentials cannot be retrieved."""


@dataclass
class OAuthToken:
    """Holds a cached OAuth access token with optional refresh capability."""

    access_token: str
    expires_at: int | str | None = None
    refresh_token: str | None = None


@dataclass
class CredentialManager:
    """Retrieves Claude Code OAuth tokens from the OS credential store.

    Always re-reads credentials from disk to pick up tokens refreshed by
    concurrent ``claude -p`` worker sessions.  This is called at most once
    per usage poll (~10 min), so the I/O cost is negligible.
    """

    def get_token(self) -> str | None:
        """Load and return the current OAuth access token.

        Returns None if credentials are unavailable (logs a warning).
        """
        try:
            token = _load_token()
            return token.access_token
        except CredentialError as exc:
            logger.warning("Could not load OAuth credentials: %s", exc)
            return None

    def refresh_token(self) -> str | None:
        """Perform an OAuth token refresh and return the new access token.

        Attempts to use the refreshToken from the credential store to
        obtain a new access token from the Anthropic OAuth endpoint.
        On success, writes the new tokens back to the credential store.
        On failure, falls back to re-reading from disk (in case a
        concurrent Claude CLI session has already refreshed).
        """
        try:
            token = _load_token()
        except CredentialError as exc:
            logger.warning("Could not load credentials for refresh: %s", exc)
            return None

        if not token.refresh_token:
            logger.debug("No refresh token available — falling back to disk read")
            return token.access_token

        try:
            new_token = _refresh_oauth_token(token.refresh_token)
            _save_token(new_token)
            logger.info("OAuth token refreshed successfully")
            return new_token.access_token
        except Exception as exc:
            logger.warning(
                "OAuth token refresh failed (%s) — falling back to disk read",
                exc,
            )
            # Re-read from disk: a concurrent claude -p session may have
            # refreshed the token while we were trying
            return self.get_token()

    def is_token_expired(self) -> bool:
        """Check if the current token is expired or about to expire.

        Returns True if the token expires within TOKEN_EXPIRY_BUFFER_MS,
        or if expiry info is unavailable (conservative).
        """
        try:
            token = _load_token()
        except CredentialError:
            return True

        if token.expires_at is None:
            return False  # No expiry info — assume valid

        try:
            expires_at_ms = int(token.expires_at)
        except (ValueError, TypeError):
            return False  # Unparseable — don't trigger unnecessary refresh

        now_ms = int(time.time() * 1000)
        return now_ms >= (expires_at_ms - TOKEN_EXPIRY_BUFFER_MS)

    def get_expires_at(self) -> int | str | None:
        """Return the expiresAt field from the credential store, or None."""
        try:
            token = _load_token()
            return token.expires_at
        except CredentialError:
            return None


def _load_token() -> OAuthToken:
    """Load the OAuth token from the OS-appropriate credential store."""
    system = platform.system()
    if system == "Darwin":
        return _load_token_macos()
    if system == "Linux":
        return _load_token_linux()
    raise CredentialError(f"Unsupported platform: {system}")


def _load_token_linux() -> OAuthToken:
    """Read credentials from ~/.claude/.credentials.json on Linux."""
    try:
        data = json.loads(LINUX_CREDENTIALS_PATH.read_text())
    except FileNotFoundError:
        raise CredentialError(
            f"Credential file not found: {LINUX_CREDENTIALS_PATH}"
        )
    except (json.JSONDecodeError, OSError) as exc:
        raise CredentialError(f"Failed to read credential file: {exc}") from exc

    return _extract_token(data)


def _load_token_macos() -> OAuthToken:
    """Read credentials from the macOS system keychain."""
    try:
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                MACOS_KEYCHAIN_SERVICE,
                "-w",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired) as exc:
        raise CredentialError(f"Failed to run security command: {exc}") from exc

    if result.returncode != 0:
        raise CredentialError(
            f"Keychain lookup failed (exit {result.returncode}): "
            f"{result.stderr.strip()}"
        )

    raw = result.stdout.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CredentialError(f"Keychain value is not valid JSON: {exc}") from exc

    return _extract_token(data)


def _extract_token(data: dict) -> OAuthToken:
    """Extract the claudeAiOauth access token from the parsed credential data."""
    oauth = data.get("claudeAiOauth")
    if not isinstance(oauth, dict):
        raise CredentialError(
            "Credential data missing 'claudeAiOauth' section"
        )
    access_token = oauth.get("accessToken")
    if not access_token or not isinstance(access_token, str):
        raise CredentialError(
            "No accessToken found in claudeAiOauth credentials"
        )
    refresh_token = oauth.get("refreshToken")
    if isinstance(refresh_token, str) and not refresh_token:
        refresh_token = None
    return OAuthToken(
        access_token=access_token,
        expires_at=oauth.get("expiresAt"),
        refresh_token=refresh_token if isinstance(refresh_token, str) else None,
    )


# ---------------------------------------------------------------------------
# OAuth token refresh
# ---------------------------------------------------------------------------


def _refresh_oauth_token(refresh_token_value: str) -> OAuthToken:
    """Exchange a refresh token for a new access token via the Anthropic OAuth endpoint.

    Raises ``CredentialError`` on network errors or non-200 responses.
    """
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token_value,
        "client_id": OAUTH_CLIENT_ID,
        "scope": OAUTH_SCOPES,
    }
    try:
        resp = httpx.post(
            OAUTH_TOKEN_URL,
            json=payload,
            timeout=OAUTH_REFRESH_TIMEOUT,
        )
    except httpx.HTTPError as exc:
        raise CredentialError(f"OAuth refresh request failed: {exc}") from exc

    if resp.status_code != 200:
        raise CredentialError(
            f"OAuth refresh failed (HTTP {resp.status_code}): "
            f"{resp.text[:200]}"
        )

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError) as exc:
        raise CredentialError(
            f"OAuth refresh returned invalid JSON: {exc}"
        ) from exc

    new_access = data.get("access_token")
    if not new_access or not isinstance(new_access, str):
        raise CredentialError("OAuth refresh response missing access_token")

    new_refresh = data.get("refresh_token")
    expires_in = data.get("expires_in")
    expires_at = None
    if isinstance(expires_in, (int, float)) and expires_in > 0:
        expires_at = int(time.time() * 1000) + int(expires_in * 1000)

    return OAuthToken(
        access_token=new_access,
        expires_at=expires_at,
        refresh_token=new_refresh if isinstance(new_refresh, str) else None,
    )


def _save_token(token: OAuthToken) -> None:
    """Write the refreshed token back to the OS credential store.

    On Linux, updates ~/.claude/.credentials.json atomically (temp + rename).
    On macOS, updates the system keychain.
    """
    system = platform.system()
    if system == "Linux":
        _save_token_linux(token)
    elif system == "Darwin":
        _save_token_macos(token)
    else:
        logger.warning("Cannot save refreshed token on %s", system)


def _save_token_linux(token: OAuthToken) -> None:
    """Atomically update the Linux credential file with new token data."""
    cred_path = LINUX_CREDENTIALS_PATH
    try:
        existing = json.loads(cred_path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        existing = {}

    oauth = existing.get("claudeAiOauth", {})
    if not isinstance(oauth, dict):
        oauth = {}

    oauth["accessToken"] = token.access_token
    if token.refresh_token:
        oauth["refreshToken"] = token.refresh_token
    if token.expires_at is not None:
        oauth["expiresAt"] = token.expires_at

    existing["claudeAiOauth"] = oauth

    # Atomic write: temp file in same directory, then rename
    fd, tmp_path = tempfile.mkstemp(
        dir=str(cred_path.parent),
        prefix=".credentials-",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(existing, f, indent=2)
        os.replace(tmp_path, str(cred_path))
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _get_keychain_account(service: str) -> str | None:
    """Extract the account name from an existing macOS keychain entry.

    Runs ``security find-generic-password -s <service>`` (without ``-w``)
    and parses the ``acct`` attribute from the output.
    """
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", service],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return None
        output = result.stdout + result.stderr
        match = re.search(r'"acct"<blob>="([^"]*)"', output)
        return match.group(1) if match else None
    except Exception:
        return None


def _save_token_macos(token: OAuthToken) -> None:
    """Update the macOS keychain with new token data."""
    # Extract the account used by the existing keychain entry so we update
    # the correct item (add-generic-password matches by service + account).
    account = _get_keychain_account(MACOS_KEYCHAIN_SERVICE)

    # Read current keychain data
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", MACOS_KEYCHAIN_SERVICE, "-w"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            existing = json.loads(result.stdout.strip())
        else:
            existing = {}
    except Exception:
        existing = {}

    oauth = existing.get("claudeAiOauth", {})
    if not isinstance(oauth, dict):
        oauth = {}

    oauth["accessToken"] = token.access_token
    if token.refresh_token:
        oauth["refreshToken"] = token.refresh_token
    if token.expires_at is not None:
        oauth["expiresAt"] = token.expires_at

    existing["claudeAiOauth"] = oauth
    payload = json.dumps(existing)

    cmd = [
        "security", "add-generic-password",
        "-U",  # update if exists
        "-s", MACOS_KEYCHAIN_SERVICE,
    ]
    if account is not None:
        cmd.extend(["-a", account])
    cmd.extend(["-w", payload])

    try:
        subprocess.run(
            cmd,
            capture_output=True, text=True, timeout=10, check=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as exc:
        logger.warning("Failed to update macOS keychain: %s", exc)


# ---------------------------------------------------------------------------
# Codex CLI credentials (ChatGPT OAuth via ~/.codex/auth.json)
# ---------------------------------------------------------------------------

CODEX_AUTH_PATH = Path.home() / ".codex" / "auth.json"


@dataclass
class CodexCredentials:
    """ChatGPT OAuth credentials used by the Codex CLI."""

    access_token: str
    account_id: str


def load_codex_credentials(
    *, auth_path: Path | None = None,
) -> CodexCredentials:
    """Load Codex credentials from ``~/.codex/auth.json``.

    Always re-reads from disk (the Codex CLI refreshes tokens).
    Raises ``CredentialError`` if the file is missing or malformed.
    """
    path = auth_path or CODEX_AUTH_PATH
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        raise CredentialError(f"Codex auth file not found: {path}")
    except (json.JSONDecodeError, OSError) as exc:
        raise CredentialError(f"Failed to read Codex auth file: {exc}") from exc

    tokens = data.get("tokens")
    if not isinstance(tokens, dict):
        raise CredentialError("Codex auth.json missing 'tokens' section")
    access_token = tokens.get("access_token")
    if not access_token or not isinstance(access_token, str):
        raise CredentialError("No access_token in Codex auth.json tokens")
    account_id = tokens.get("account_id")
    if not account_id or not isinstance(account_id, str):
        raise CredentialError("No account_id in Codex auth.json tokens")
    return CodexCredentials(access_token=access_token, account_id=account_id)


# ---------------------------------------------------------------------------
# Anthropic usage API helper
# ---------------------------------------------------------------------------

async def fetch_usage(
    token: str, *, client: httpx.AsyncClient | None = None
) -> dict:
    """Call the Anthropic OAuth usage API and return the parsed response.

    If *client* is provided it is used as-is (caller manages its lifecycle).
    Otherwise a throwaway ``AsyncClient`` is created for the single request.

    Raises httpx.HTTPStatusError on non-2xx responses.
    """

    async def _do_request(c: httpx.AsyncClient) -> dict:
        resp = await c.get(
            USAGE_API_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "anthropic-beta": USAGE_API_BETA_HEADER,
                "User-Agent": f"claude-code/{_get_claude_version()}",
            },
            timeout=USAGE_API_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    if client is not None:
        return await _do_request(client)

    async with httpx.AsyncClient() as c:
        return await _do_request(c)
