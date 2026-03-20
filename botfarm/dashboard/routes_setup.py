"""Setup status API, partial endpoints, and configuration forms for the setup wizard."""

from __future__ import annotations

import asyncio
import html
import logging
import os
import shutil
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from botfarm.config import write_yaml_atomic
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


def _check_claude_auth() -> SetupStep:
    """Check whether Claude Code credentials are available.

    Uses the credential loading mechanism from ``botfarm.credentials``,
    which checks ``~/.claude/.credentials.json`` on Linux and the system
    keychain on macOS.
    """
    done = False
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


def get_setup_steps(config: BotfarmConfig) -> list[SetupStep]:
    """Return the full ordered checklist of setup steps."""
    return [
        _check_bugtracker_type(config),
        _check_bugtracker_api_key(config),
        _check_github_auth(),
        _check_claude_auth(),
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

    steps = get_setup_steps(config)
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
        steps = get_setup_steps(config)
        setup_complete = all(s.done for s in steps)

    templates = request.app.state.templates
    return templates.TemplateResponse("partials/setup_status.html", {
        "request": request,
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


def _build_credentials_context(cfg: BotfarmConfig | None) -> dict:
    """Build template context dict for the credentials partial.

    Shared by ``setup_page`` (initial render) and ``partial_setup_credentials``
    (htmx refresh) to avoid duplicating the same assembly logic.
    """
    github_done = _check_github_auth().done
    claude_done = _check_claude_auth().done

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
        steps = get_setup_steps(config)
        all_steps_done = all(s.done for s in steps)
        setup_complete = all_steps_done and not degraded
        section_done = _section_done_map(steps)
        has_projects = bool(config.projects)
        project_count = len(config.projects)
        by_id = {s.id: s.done for s in steps}
        repos_cloned = by_id.get("repos_cloned", False)

    # Credential partial context (shared helper avoids duplication)
    cred_ctx = _build_credentials_context(config)

    return templates.TemplateResponse("setup.html", {
        "request": request,
        "steps": steps,
        "setup_complete": setup_complete,
        "section_done": section_done,
        "has_projects": has_projects,
        "project_count": project_count,
        "repos_cloned": repos_cloned,
        "degraded": degraded,
        # Credential partial context (for initial include)
        **cred_ctx,
    })


@router.get("/partials/setup-preflight", response_class=HTMLResponse)
def partial_setup_preflight(request: Request):
    """Render preflight check results for the setup wizard."""
    from .routes_api import _get_preflight_data

    app = request.app
    templates = app.state.templates
    data = _get_preflight_data(app)
    return templates.TemplateResponse("partials/setup_preflight.html", {
        "request": request,
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
# Credential status partial
# ---------------------------------------------------------------------------

@router.get("/partials/setup-credentials", response_class=HTMLResponse)
def partial_setup_credentials(request: Request):
    """Render credential status cards for the setup wizard."""
    cfg = request.app.state.botfarm_config
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/setup_credentials.html", {
        "request": request,
        **_build_credentials_context(cfg),
    })
