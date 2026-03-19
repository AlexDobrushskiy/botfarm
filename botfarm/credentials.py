"""Retrieve OAuth tokens for the usage API.

Claude Code: macOS (system keychain) / Linux (~/.claude/.credentials.json).
Codex CLI: ~/.codex/auth.json (file-based, all platforms).
"""

from __future__ import annotations

import json
import logging
import platform
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import shutil

import httpx

logger = logging.getLogger(__name__)

USAGE_API_URL = "https://api.anthropic.com/api/oauth/usage"
USAGE_API_BETA_HEADER = "oauth-2025-04-20"
USAGE_API_TIMEOUT = httpx.Timeout(30, connect=10)

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
    """Holds a cached OAuth access token.

    ``expires_at`` is stored for informational purposes only.  This module
    does **not** perform proactive expiry checks — callers should handle
    HTTP 401 responses and call ``CredentialManager.refresh_token()``.
    """

    access_token: str
    expires_at: str | None = None


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
        """Reload the token from the OS credential store.

        Kept for API compatibility with callers that explicitly refresh
        after a 401, but behaves identically to ``get_token()`` since
        we always read fresh from disk.
        """
        return self.get_token()

    def get_expires_at(self) -> str | None:
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
    return OAuthToken(
        access_token=access_token,
        expires_at=oauth.get("expiresAt"),
    )


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
