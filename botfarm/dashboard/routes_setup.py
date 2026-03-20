"""Setup status API and partial endpoints for the setup wizard."""

from __future__ import annotations

import platform
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

router = APIRouter()


@dataclass
class SetupStep:
    """A single setup checklist item."""

    id: str
    label: str
    done: bool


def _check_bugtracker_type(config) -> SetupStep:
    """Check whether a bugtracker type has been selected."""
    bt = config.bugtracker
    done = bool(bt.type and bt.type in ("linear", "jira"))
    return SetupStep(id="bugtracker_type", label="Bugtracker type selected", done=done)


def _check_bugtracker_api_key(config) -> SetupStep:
    """Check whether the bugtracker API key is configured."""
    done = bool(config.bugtracker.api_key)
    return SetupStep(
        id="bugtracker_api_key", label="Bugtracker API key configured", done=done
    )


def _check_github_auth() -> SetupStep:
    """Check whether GitHub CLI authentication is available (file check only).

    Looks for ``gh`` on PATH and checks the hosts.yml config file exists
    with content — no API calls.
    """
    done = False
    if shutil.which("gh"):
        # gh stores auth in ~/.config/gh/hosts.yml (Linux/macOS)
        hosts_path = Path.home() / ".config" / "gh" / "hosts.yml"
        if hosts_path.is_file() and hosts_path.stat().st_size > 0:
            done = True
    return SetupStep(id="github_auth", label="GitHub authentication", done=done)


def _check_claude_auth() -> SetupStep:
    """Check whether Claude Code credentials exist on disk (no API call).

    Linux: ``~/.claude/.credentials.json``
    macOS: presence of ``claude`` binary (keychain can't be checked cheaply).
    """
    done = False
    system = platform.system()
    if system == "Linux":
        creds = Path.home() / ".claude" / ".credentials.json"
        done = creds.is_file() and creds.stat().st_size > 0
    elif system == "Darwin":
        # On macOS credentials live in the system keychain; checking them
        # requires a subprocess call.  As a lightweight proxy, just verify
        # the ``claude`` binary is installed.
        done = shutil.which("claude") is not None
    return SetupStep(id="claude_auth", label="Claude Code authentication", done=done)


def _check_project_configured(config) -> SetupStep:
    """Check whether at least one project is configured."""
    done = bool(config.projects)
    return SetupStep(
        id="project_configured",
        label="At least one project configured",
        done=done,
    )


def _check_repos_cloned(config) -> SetupStep:
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


def get_setup_steps(config) -> list[SetupStep]:
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
