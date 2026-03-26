"""Setup status API, partial endpoints, and configuration forms for the setup wizard."""

from __future__ import annotations

import asyncio
import html
import logging
import os
import pty
import re
import select
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import yaml

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from botfarm.config import LONG_LIVED_TOKEN_ENV_VAR, VALID_AUTH_MODES, write_yaml_atomic
from botfarm.credentials import CredentialError, _load_token

from .routes_config import _resolve_env_path, _write_env_file

if TYPE_CHECKING:
    from botfarm.config import BotfarmConfig

logger = logging.getLogger(__name__)

router = APIRouter()


@dataclass
class SetupStep:
    """A single setup checklist item."""

    id: str
    label: str
    done: bool


def _get_env_var(app, key: str) -> str:
    """Return the value of *key* from ``os.environ`` or the ``.env`` file.

    If the variable is present in ``.env`` but not yet in the process
    environment, it is loaded into ``os.environ`` so that downstream code
    (e.g. supervisor workers) can read it without a restart.
    """
    value = os.environ.get(key, "")
    if value:
        return value

    env_path = _resolve_env_path(app)
    if not env_path.exists():
        return ""

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        k, _, v = stripped.partition("=")
        if k.strip() == key:
            v = v.strip()
            # Strip surrounding quotes
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            if v:
                os.environ[key] = v
            return v

    return ""


def _check_bugtracker_type(config: BotfarmConfig) -> SetupStep:
    """Check whether a bugtracker type has been selected."""
    bt = config.bugtracker
    done = bool(bt.type and bt.type in ("linear", "jira"))
    return SetupStep(id="bugtracker_type", label="Bugtracker type selected", done=done)


def _check_bugtracker_api_key(config: BotfarmConfig) -> SetupStep:
    """Check whether the bugtracker API key is configured."""
    done = bool(config.bugtracker.api_key)
    return SetupStep(
        id="bugtracker_api_key", label="Bugtracker API key configured", done=done
    )


def _check_github_auth() -> SetupStep:
    """Check whether GitHub CLI authentication is available.

    Checks ``GH_TOKEN`` / ``GITHUB_TOKEN`` env vars first, then falls back
    to looking for ``gh`` on PATH with a ``hosts.yml`` config file.
    """
    done = False
    if os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN"):
        done = True
    elif shutil.which("gh"):
        # gh stores auth in ~/.config/gh/hosts.yml (Linux/macOS)
        hosts_path = Path.home() / ".config" / "gh" / "hosts.yml"
        if hosts_path.is_file() and hosts_path.stat().st_size > 0:
            done = True
    return SetupStep(id="github_auth", label="GitHub authentication", done=done)


def _check_claude_auth(config: BotfarmConfig | None = None, app=None) -> SetupStep:
    """Check whether Claude Code credentials are available.

    The check varies by auth mode:
    - **oauth** (default): looks for OAuth credentials on disk via
      ``botfarm.credentials._load_token``.
    - **long_lived_token**: checks ``CLAUDE_LONG_LIVED_TOKEN`` env var.
    - **api_key**: checks ``ANTHROPIC_API_KEY`` env var.

    When *app* is provided, environment variables are resolved via
    ``_get_env_var`` which also reads from the ``.env`` file, keeping
    behaviour consistent with ``_build_credentials_context``.
    """
    auth_mode = config.auth_mode if config else "oauth"
    done = False

    def _env(key: str) -> str:
        if app is not None:
            return _get_env_var(app, key)
        return os.environ.get(key, "")

    if auth_mode == "long_lived_token":
        done = bool(_env(LONG_LIVED_TOKEN_ENV_VAR))
    elif auth_mode == "api_key":
        done = bool(_env("ANTHROPIC_API_KEY"))
    else:
        try:
            _load_token()
            done = True
        except CredentialError:
            pass

    return SetupStep(id="claude_auth", label="Claude Code authentication", done=done)


def _check_project_configured(config: BotfarmConfig) -> SetupStep:
    """Check whether at least one project is configured."""
    done = bool(config.projects)
    return SetupStep(
        id="project_configured",
        label="At least one project configured",
        done=done,
    )


def _check_repos_cloned(config: BotfarmConfig) -> SetupStep:
    """Check whether all configured project repos have been cloned."""
    if not config.projects:
        return SetupStep(
            id="repos_cloned", label="Project repositories cloned", done=False
        )
    all_cloned = all(
        (Path(p.base_dir).expanduser() / ".git").exists() for p in config.projects
    )
    return SetupStep(
        id="repos_cloned", label="Project repositories cloned", done=all_cloned
    )


def get_setup_steps(config: BotfarmConfig, app=None) -> list[SetupStep]:
    """Return the full ordered checklist of setup steps."""
    return [
        _check_bugtracker_type(config),
        _check_bugtracker_api_key(config),
        _check_github_auth(),
        _check_claude_auth(config, app=app),
        _check_project_configured(config),
        _check_repos_cloned(config),
    ]


@router.get("/api/setup/status")
def api_setup_status(request: Request):
    """Return a JSON checklist of setup completion status."""
    config = request.app.state.botfarm_config
    if config is None:
        return JSONResponse(
            {"error": "No configuration loaded"}, status_code=503
        )

    steps = get_setup_steps(config, app=request.app)
    setup_complete = all(s.done for s in steps)
    return JSONResponse({
        "setup_complete": setup_complete,
        "steps": [asdict(s) for s in steps],
    })


@router.get("/partials/setup-status", response_class=HTMLResponse)
def partial_setup_status(request: Request):
    """Render the setup checklist as an htmx-polled partial."""
    config = request.app.state.botfarm_config
    steps: list[SetupStep] = []
    setup_complete = False
    if config is not None:
        steps = get_setup_steps(config, app=request.app)
        setup_complete = all(s.done for s in steps)

    templates = request.app.state.templates
    return templates.TemplateResponse(request, "partials/setup_status.html", {
        "steps": steps,
        "setup_complete": setup_complete,
    })


def _section_done_map(steps: list[SetupStep]) -> dict[str, bool]:
    """Compute per-section done status from the flat step list.

    Sections:
    - bugtracker: bugtracker_type + bugtracker_api_key
    - credentials: github_auth + claude_auth
    - project: project_configured + repos_cloned
    - verification: all steps done (setup complete)
    """
    by_id = {s.id: s.done for s in steps}
    return {
        "bugtracker": by_id.get("bugtracker_type", False) and by_id.get("bugtracker_api_key", False),
        "credentials": by_id.get("github_auth", False) and by_id.get("claude_auth", False),
        "project": by_id.get("project_configured", False) and by_id.get("repos_cloned", False),
        "verification": all(s.done for s in steps),
    }


def _build_credentials_context(cfg: BotfarmConfig | None, app=None) -> dict:
    """Build template context dict for the credentials partial.

    Shared by ``setup_page`` (initial render) and ``partial_setup_credentials``
    (htmx refresh) to avoid duplicating the same assembly logic.
    """
    github_done = _check_github_auth().done
    claude_done = _check_claude_auth(cfg, app=app).done

    ssh_key_path = ""
    ssh_key_exists = False
    if cfg and cfg.identities.coder.ssh_key_path:
        ssh_key_path = cfg.identities.coder.ssh_key_path
        try:
            ssh_key_exists = Path(ssh_key_path).expanduser().exists()
        except (OSError, ValueError):
            pass

    bt_type = ""
    bt_workspace = ""
    bt_api_key_set = False
    jira_url = ""
    jira_email = ""
    if cfg:
        bt_type = cfg.bugtracker.type or ""
        bt_workspace = cfg.bugtracker.workspace or ""
        bt_api_key_set = bool(cfg.bugtracker.api_key)
        from botfarm.config import JiraBugtrackerConfig
        if isinstance(cfg.bugtracker, JiraBugtrackerConfig):
            jira_url = cfg.bugtracker.url or ""
            jira_email = cfg.bugtracker.email or ""

    auth_mode = cfg.auth_mode if cfg else "oauth"

    return {
        "github_done": github_done,
        "claude_done": claude_done,
        "ssh_key_path": ssh_key_path,
        "ssh_key_exists": ssh_key_exists,
        "bt_type": bt_type,
        "bt_workspace": bt_workspace,
        "bt_api_key_set": bt_api_key_set,
        "jira_url": jira_url,
        "jira_email": jira_email,
        "auth_mode": auth_mode,
        "long_lived_token_set": bool(
            _get_env_var(app, LONG_LIVED_TOKEN_ENV_VAR) if app
            else os.environ.get(LONG_LIVED_TOKEN_ENV_VAR, "")
        ),
        "api_key_set": bool(
            _get_env_var(app, "ANTHROPIC_API_KEY") if app
            else os.environ.get("ANTHROPIC_API_KEY", "")
        ),
    }


@router.get("/setup", response_class=HTMLResponse)
def setup_page(request: Request):
    """Render the setup wizard page."""
    app = request.app
    config = app.state.botfarm_config
    templates = app.state.templates

    steps: list[SetupStep] = []
    setup_complete = False
    section_done: dict[str, bool] = {
        "bugtracker": False, "credentials": False,
        "project": False, "verification": False,
    }
    has_projects = False
    project_count = 0
    repos_cloned = False

    degraded_getter = app.state.get_degraded
    degraded = degraded_getter() if degraded_getter else False

    if config is not None:
        steps = get_setup_steps(config, app=app)
        all_steps_done = all(s.done for s in steps)
        setup_complete = all_steps_done and not degraded
        section_done = _section_done_map(steps)
        has_projects = bool(config.projects)
        project_count = len(config.projects)
        by_id = {s.id: s.done for s in steps}
        repos_cloned = by_id.get("repos_cloned", False)

    # Credential partial context (shared helper avoids duplication)
    cred_ctx = _build_credentials_context(config, app=app)

    bt_type = ""
    if config is not None:
        bt_type = config.bugtracker.type or ""

    return templates.TemplateResponse(request, "setup.html", {
        "steps": steps,
        "setup_complete": setup_complete,
        "section_done": section_done,
        "has_projects": has_projects,
        "project_count": project_count,
        "repos_cloned": repos_cloned,
        "degraded": degraded,
        "bt_type": bt_type,
        # Credential partial context (for initial include)
        **cred_ctx,
    })


@router.post("/api/setup/complete")
def api_setup_complete(request: Request):
    """Trigger preflight re-run after project creation.

    Called by the setup wizard after a project is successfully added.
    Triggers the preflight callback so the supervisor can re-evaluate
    whether to exit setup/degraded mode.  The callback is non-blocking
    (it only sets a threading event), so this endpoint cannot report
    actual completion — callers should refresh the page to see the
    updated state.
    """
    app = request.app

    # Trigger preflight re-run if callback is available
    cb = app.state.on_rerun_preflight
    triggered = False
    if cb is not None:
        try:
            cb()
            triggered = True
        except Exception:
            logger.exception("Failed to trigger preflight re-run from setup complete")

    return JSONResponse({"preflight_triggered": triggered})


@router.get("/partials/setup-preflight", response_class=HTMLResponse)
def partial_setup_preflight(request: Request):
    """Render preflight check results for the setup wizard."""
    from .routes_api import _get_preflight_data

    app = request.app
    templates = app.state.templates
    data = _get_preflight_data(app)
    return templates.TemplateResponse(request, "partials/setup_preflight.html", {
        **data,
    })


# ---------------------------------------------------------------------------
# Bugtracker configuration form
# ---------------------------------------------------------------------------

def _feedback(msg: str, level: str = "success", status: int = 200) -> HTMLResponse:
    """Return a styled feedback HTML fragment."""
    return HTMLResponse(
        f'<div class="config-feedback {html.escape(level)}" role="alert">'
        f"{msg}</div>",
        status_code=status,
    )


def _validate_bugtracker_api_key(
    bt_type: str,
    api_key: str,
    *,
    jira_url: str = "",
    jira_email: str = "",
) -> str | None:
    """Test a bugtracker API key by making a lightweight API call.

    Returns ``None`` on success, or an error message string on failure.
    """
    from botfarm.bugtracker import create_client

    try:
        if bt_type == "linear":
            client = create_client(api_key=api_key, bugtracker_type="linear")
        elif bt_type == "jira":
            from botfarm.bugtracker.jira.client import JiraClient

            client = JiraClient(url=jira_url, email=jira_email, api_token=api_key)
        else:
            return f"Unknown bugtracker type: {bt_type!r}"

        client.get_viewer_id()
    except Exception as exc:
        return f"API key validation failed: {exc}"
    return None


@router.post("/api/setup/bugtracker", response_class=HTMLResponse)
async def setup_bugtracker(request: Request):
    """Configure bugtracker type, workspace, and API key during setup."""
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return _feedback("No configuration loaded.", "error", 400)

    try:
        body = await request.json()
    except Exception:
        return _feedback("Invalid JSON body.", "error", 400)
    if not isinstance(body, dict):
        return _feedback("Request body must be a JSON object.", "error", 400)

    bt_type = str(body.get("type", "")).strip().lower()
    workspace = str(body.get("workspace", "")).strip()
    api_key = str(body.get("api_key", "")).strip()

    # Jira-specific fields
    jira_url = str(body.get("jira_url", "")).strip()
    jira_email = str(body.get("jira_email", "")).strip()

    # --- Validation ---
    errors: list[str] = []
    if bt_type not in ("linear", "jira"):
        errors.append("Bugtracker type must be 'linear' or 'jira'.")
    if not workspace:
        errors.append("Workspace name is required.")
    if not api_key:
        errors.append("API key is required.")
    if bt_type == "jira":
        if not jira_url:
            errors.append("Jira URL is required.")
        if not jira_email:
            errors.append("Jira email is required.")

    if errors:
        error_html = "".join(f"<li>{html.escape(e)}</li>" for e in errors)
        return _feedback(
            f"<strong>Validation errors:</strong><ul>{error_html}</ul>",
            "error", 422,
        )

    # --- Validate API key by calling the bugtracker ---
    validation_error = await asyncio.to_thread(
        _validate_bugtracker_api_key,
        bt_type, api_key,
        jira_url=jira_url, jira_email=jira_email,
    )
    if validation_error:
        return _feedback(html.escape(validation_error), "error", 422)

    # --- Persist: write API key to .env, config to YAML ---
    env_path = _resolve_env_path(app)
    config_path = Path(cfg.source_path) if cfg.source_path else None

    # Determine env var name for the API key
    env_var = "LINEAR_API_KEY" if bt_type == "linear" else "JIRA_API_TOKEN"
    try:
        _write_env_file(env_path, {env_var: api_key})
    except OSError as exc:
        return _feedback(
            f"Failed to write .env file: {html.escape(str(exc))}", "error", 500,
        )

    # --- Update in-memory config ---
    from botfarm.config import JiraBugtrackerConfig, LinearBugtrackerConfig

    if bt_type == "jira":
        cfg.bugtracker = JiraBugtrackerConfig(
            type=bt_type, workspace=workspace, api_key=api_key,
            url=jira_url, email=jira_email,
        )
    else:
        cfg.bugtracker = LinearBugtrackerConfig(
            type=bt_type, workspace=workspace, api_key=api_key,
        )

    # Write bugtracker section to config.yaml
    if not config_path or not config_path.exists():
        logger.warning("No config.yaml found at %s — skipping YAML write", config_path)
        return _feedback(
            "API key saved to .env but config.yaml could not be updated "
            "(file not found). Bugtracker settings may not persist across restarts.",
            "warning", 200,
        )

    try:
        raw = config_path.read_text()
        data = yaml.safe_load(raw)
        if not isinstance(data, dict):
            data = {}
        bt_data: dict = data.setdefault("bugtracker", {})
        bt_data["type"] = bt_type
        bt_data["workspace"] = workspace
        bt_data["api_key"] = f"${{{env_var}}}"
        if bt_type == "jira":
            bt_data["url"] = jira_url
            bt_data["email"] = jira_email
        else:
            bt_data.pop("url", None)
            bt_data.pop("email", None)
        write_yaml_atomic(config_path, data)
    except Exception:
        logger.exception("Failed to write bugtracker config to YAML")
        return _feedback(
            "API key saved to .env but failed to update config.yaml.",
            "warning", 200,
        )

    return _feedback("Bugtracker configured successfully.")


# ---------------------------------------------------------------------------
# GitHub PAT configuration form
# ---------------------------------------------------------------------------

def _validate_github_token(token: str) -> str | None:
    """Validate a GitHub token by calling ``gh api user``.

    Returns ``None`` on success, or an error message on failure.
    """
    try:
        result = subprocess.run(
            ["gh", "api", "user"],
            capture_output=True,
            text=True,
            timeout=15,
            env={**os.environ, "GH_TOKEN": token},
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            return f"GitHub token validation failed: {stderr or 'unknown error'}"
    except FileNotFoundError:
        return "GitHub CLI (gh) is not installed or not on PATH."
    except subprocess.TimeoutExpired:
        return "GitHub token validation timed out."
    return None


@router.post("/api/setup/github", response_class=HTMLResponse)
async def setup_github(request: Request):
    """Configure GitHub PAT during setup."""
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return _feedback("No configuration loaded.", "error", 400)

    try:
        body = await request.json()
    except Exception:
        return _feedback("Invalid JSON body.", "error", 400)
    if not isinstance(body, dict):
        return _feedback("Request body must be a JSON object.", "error", 400)

    token = str(body.get("github_token", "")).strip()
    if not token:
        return _feedback("GitHub token is required.", "error", 422)

    # Validate the token
    validation_error = await asyncio.to_thread(_validate_github_token, token)
    if validation_error:
        return _feedback(html.escape(validation_error), "error", 422)

    # Write to .env
    env_path = _resolve_env_path(app)
    try:
        _write_env_file(env_path, {"GH_TOKEN": token})
    except OSError as exc:
        return _feedback(
            f"Failed to write .env file: {html.escape(str(exc))}", "error", 500,
        )

    # Update the process environment so the setup check passes immediately
    os.environ["GH_TOKEN"] = token

    return _feedback("GitHub token validated and saved.")


# ---------------------------------------------------------------------------
# GitHub device code flow
# ---------------------------------------------------------------------------

# GitHub CLI OAuth App client ID (public, used by `gh auth login --web`).
_GH_CLIENT_ID = "178c6fc778ccc68e1d6a"
_GH_DEVICE_CODE_URL = "https://github.com/login/device/code"
_GH_TOKEN_URL = "https://github.com/login/oauth/access_token"


@router.post("/api/setup/github/device-code")
async def start_device_code_flow(request: Request):
    """Start GitHub OAuth device code flow.

    Calls the GitHub device authorization endpoint and returns a user code
    and verification URL that the user opens in their browser.
    """
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                _GH_DEVICE_CODE_URL,
                data={"client_id": _GH_CLIENT_ID, "scope": "repo read:org"},
                headers={"Accept": "application/json"},
            )
    except httpx.HTTPError as exc:
        logger.warning("Device code request failed: %s", exc)
        return JSONResponse(
            {"error": f"Failed to contact GitHub: {html.escape(str(exc))}"}, status_code=502,
        )

    if resp.status_code != 200:
        logger.warning("Device code request returned %s: %s", resp.status_code, resp.text)
        return JSONResponse(
            {"error": "GitHub returned an error. Try again later."}, status_code=502,
        )

    data = resp.json()
    if "user_code" not in data:
        return JSONResponse(
            {"error": html.escape(data.get("error_description", "Unexpected response from GitHub."))},
            status_code=502,
        )

    return JSONResponse({
        "user_code": data["user_code"],
        "verification_uri": data["verification_uri"],
        "device_code": data["device_code"],
        "interval": data.get("interval", 5),
        "expires_in": data.get("expires_in", 900),
    })


@router.post("/api/setup/github/device-code/poll")
async def poll_device_code_flow(request: Request):
    """Poll for GitHub device code flow completion.

    The frontend calls this periodically with the ``device_code`` obtained
    from the start endpoint.  When the user completes authorization in
    their browser, GitHub returns an access token which is saved to ``.env``
    and the process environment.
    """
    app = request.app

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            {"status": "error", "message": "Invalid JSON body."}, status_code=400,
        )
    device_code = str(body.get("device_code", "")).strip()
    if not device_code:
        return JSONResponse(
            {"status": "error", "message": "device_code is required."}, status_code=400,
        )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                _GH_TOKEN_URL,
                data={
                    "client_id": _GH_CLIENT_ID,
                    "device_code": device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                },
                headers={"Accept": "application/json"},
            )
    except httpx.HTTPError as exc:
        logger.warning("Device code poll failed: %s", exc)
        return JSONResponse({"status": "error", "message": html.escape(str(exc))})

    data = resp.json()

    if "access_token" in data:
        token = data["access_token"]
        # Persist to .env and process environment
        env_path = _resolve_env_path(app)
        try:
            _write_env_file(env_path, {"GH_TOKEN": token})
        except OSError as exc:
            logger.error("Failed to write GH_TOKEN to .env: %s", exc)
            return JSONResponse({"status": "error", "message": "Failed to save token."})
        os.environ["GH_TOKEN"] = token
        return JSONResponse({"status": "complete"})

    error = data.get("error", "")
    if error == "authorization_pending":
        return JSONResponse({"status": "pending"})
    if error == "slow_down":
        return JSONResponse({
            "status": "slow_down",
            "interval": data.get("interval", 10),
        })
    if error == "expired_token":
        return JSONResponse({"status": "expired"})
    if error == "access_denied":
        return JSONResponse({"status": "denied"})

    return JSONResponse({
        "status": "error",
        "message": html.escape(data.get("error_description", error or "Unknown error")),
    })


# ---------------------------------------------------------------------------
# Claude Code authentication (browser-based, no terminal required)
# ---------------------------------------------------------------------------

# Module-level state for the claude auth subprocess
_claude_auth_state: dict | None = None
_claude_auth_lock = threading.Lock()
_CLAUDE_AUTH_TIMEOUT = 15 * 60  # 15 minutes


def _extract_auth_url(text: str) -> str | None:
    """Extract an authentication URL from Claude CLI output.

    Looks for HTTPS URLs in the text.  Strips common trailing punctuation
    that might have been captured as part of the match.
    """
    match = re.search(r"https://[^\s<>\"'\x00-\x1f]+", text)
    if match:
        return match.group(0).rstrip(".,;:!?)")
    return None


def _cleanup_claude_auth() -> None:
    """Terminate and clean up any active Claude auth subprocess.

    Must be called while holding ``_claude_auth_lock``.
    """
    global _claude_auth_state
    if _claude_auth_state is None:
        return

    state = _claude_auth_state
    _claude_auth_state = None

    master_fd = state.get("master_fd")
    if master_fd is not None:
        try:
            os.close(master_fd)
        except OSError:
            pass

    proc = state.get("proc")
    if proc is not None and proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=2)
        except Exception:
            try:
                proc.kill()
                proc.wait(timeout=1)
            except Exception:
                pass


def _start_claude_auth_process() -> tuple[str | None, str | None]:
    """Start ``claude auth login`` and extract the OAuth URL.

    Returns ``(url, None)`` on success or ``(None, error_message)`` on
    failure.  The subprocess is stored in module state so it can continue
    polling for auth completion in the background.
    """
    global _claude_auth_state

    if not shutil.which("claude"):
        return None, "Claude Code CLI is not installed or not on PATH."

    with _claude_auth_lock:
        _cleanup_claude_auth()

        master_fd, slave_fd = pty.openpty()
        try:
            env = os.environ.copy()
            env["TERM"] = "dumb"
            # Prevent the CLI from opening a browser
            env.pop("DISPLAY", None)
            env.pop("WAYLAND_DISPLAY", None)
            env.pop("BROWSER", None)

            proc = subprocess.Popen(
                ["claude", "auth", "login"],
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                close_fds=True,
                env=env,
            )
        except Exception as exc:
            os.close(master_fd)
            os.close(slave_fd)
            return None, f"Failed to start Claude auth: {exc}"

        os.close(slave_fd)

        _claude_auth_state = {
            "proc": proc,
            "master_fd": master_fd,
            "started_at": time.monotonic(),
        }

    # Read output until we find a URL (or timeout)
    output = ""
    url = None
    deadline = time.monotonic() + 30

    while time.monotonic() < deadline:
        try:
            ready, _, _ = select.select([master_fd], [], [], 1.0)
        except (OSError, ValueError):
            break
        if ready:
            try:
                data = os.read(master_fd, 4096)
                if not data:
                    break
                output += data.decode("utf-8", errors="replace")
                url = _extract_auth_url(output)
                if url:
                    break
            except OSError:
                break
        if proc.poll() is not None:
            break

    if url:
        return url, None

    # Process exited early — try to drain remaining output
    if proc.poll() is not None:
        try:
            while True:
                ready, _, _ = select.select([master_fd], [], [], 0.1)
                if not ready:
                    break
                data = os.read(master_fd, 4096)
                if not data:
                    break
                output += data.decode("utf-8", errors="replace")
        except OSError:
            pass
        url = _extract_auth_url(output)
        if url:
            return url, None

    # No URL found — clean up
    with _claude_auth_lock:
        _cleanup_claude_auth()

    return None, "Could not extract authentication URL from Claude output."


@router.post("/api/setup/claude/auth")
async def start_claude_auth(request: Request):
    """Start Claude Code authentication and return the OAuth URL.

    Launches ``claude auth login`` in a pty subprocess, captures the
    OAuth URL from its output, and returns it so the frontend can show
    a clickable link.  The subprocess continues running in the background
    to complete the OAuth flow when the user authorises in their browser.
    """
    if _check_claude_auth().done:
        return JSONResponse({"status": "already_authenticated"})

    url, error = await asyncio.to_thread(_start_claude_auth_process)
    if error:
        return JSONResponse({"error": error}, status_code=400)

    return JSONResponse({"url": url})


@router.get("/api/setup/claude/auth/status")
def claude_auth_status(request: Request):
    """Check whether Claude Code authentication has completed.

    The frontend polls this endpoint after the user has been shown the
    OAuth URL.  When the credentials file appears (written by the
    ``claude`` subprocess), authentication is complete and the subprocess
    is cleaned up.
    """
    authenticated = _check_claude_auth().done

    expired = False
    with _claude_auth_lock:
        if authenticated:
            _cleanup_claude_auth()
        elif (
            _claude_auth_state is not None
            and time.monotonic() - _claude_auth_state["started_at"]
            > _CLAUDE_AUTH_TIMEOUT
        ):
            logger.info("Claude auth process timed out — cleaning up")
            _cleanup_claude_auth()
            expired = True
        elif _claude_auth_state is not None:
            proc = _claude_auth_state.get("proc")
            if proc is not None and proc.poll() is not None:
                logger.info("Claude auth process exited unexpectedly — cleaning up")
                _cleanup_claude_auth()
                expired = True

    # No active auth session and not authenticated — flow is dead
    if not authenticated and not expired and _claude_auth_state is None:
        expired = True

    return JSONResponse({
        "authenticated": authenticated,
        "expired": expired,
    })


# ---------------------------------------------------------------------------
# Auth method configuration
# ---------------------------------------------------------------------------

@router.post("/api/setup/claude/auth-method")
async def save_auth_method(request: Request):
    """Save the selected Claude auth method to config.yaml."""
    app = request.app
    cfg = app.state.botfarm_config

    body = await request.json()
    method = body.get("auth_method", "oauth")

    if method not in VALID_AUTH_MODES:
        return _feedback(
            f"Invalid auth method: {html.escape(method)!r}. Must be one of {sorted(VALID_AUTH_MODES)}.",
            "error", 400,
        )

    # Validate token availability for the selected mode.
    # Check both os.environ and the .env file so users don't need a restart.
    if method == "long_lived_token" and not _get_env_var(app, LONG_LIVED_TOKEN_ENV_VAR):
        return _feedback(
            "CLAUDE_LONG_LIVED_TOKEN is not set. "
            "Add it to your .env file before selecting this mode.",
            "error", 400,
        )
    if method == "api_key" and not _get_env_var(app, "ANTHROPIC_API_KEY"):
        return _feedback(
            "ANTHROPIC_API_KEY is not set. "
            "Add it to your .env file before selecting this mode.",
            "error", 400,
        )

    config_path = Path(cfg.source_path) if cfg and cfg.source_path else None
    if not config_path or not config_path.exists():
        return _feedback(
            "config.yaml not found — cannot persist auth method change.",
            "error", 400,
        )

    try:
        raw = config_path.read_text()
        data = yaml.safe_load(raw)
        if not isinstance(data, dict):
            data = {}

        # Write as claude_auth_method (preferred name); remove legacy auth_mode
        # to avoid ambiguity.
        data["claude_auth_method"] = method
        data.pop("auth_mode", None)
        write_yaml_atomic(config_path, data)
    except Exception:
        logger.exception("Failed to write auth method to config.yaml")
        return _feedback("Failed to update config.yaml.", "error", 500)

    # Update in-memory config and app state
    if cfg:
        cfg.auth_mode = method
    app.state.auth_mode = method

    return _feedback(f"Auth method set to '{method}'. New workers will use this mode; in-flight workers are unaffected.")


# ---------------------------------------------------------------------------
# Credential status partial
# ---------------------------------------------------------------------------

@router.get("/partials/setup-credentials", response_class=HTMLResponse)
def partial_setup_credentials(request: Request):
    """Render credential status cards for the setup wizard."""
    cfg = request.app.state.botfarm_config
    templates = request.app.state.templates
    return templates.TemplateResponse(request, "partials/setup_credentials.html", {
        **_build_credentials_context(cfg, app=request.app),
    })
