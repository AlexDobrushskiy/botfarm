"""API routes for adding projects: Linear team/project lookups, project creation, SSE progress."""

from __future__ import annotations

import asyncio
import logging
import re
import uuid
from pathlib import Path
from threading import Lock, Thread

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse

from botfarm.bugtracker import create_client
from botfarm.project_setup import (
    extract_repo_name,
    setup_project,
    setup_project_git,
    ProjectSetupError,
)

from .state import get_capacity_data, manual_pause_state, read_state, supervisor_status

logger = logging.getLogger(__name__)

router = APIRouter()

# In-memory store for background setup tasks.
# Keys are task_id (str), values are dicts with progress info.
_setup_tasks: dict[str, dict] = {}
_setup_tasks_lock = Lock()

# Simple git URL pattern — accepts SSH and HTTPS URLs.
_GIT_URL_RE = re.compile(
    r"^(https?://|git@|ssh://|git://)"
)


def _get_linear_client(app):
    cfg = app.state.botfarm_config
    if cfg is None or not cfg.bugtracker.api_key:
        return None
    return create_client(cfg)


# --- Linear data endpoints ---


@router.get("/api/linear/teams")
def api_linear_teams(request: Request):
    """Return Linear teams for the dropdown."""
    client = _get_linear_client(request.app)
    if client is None:
        return JSONResponse(
            {"error": "Linear API key not configured"}, status_code=503,
        )
    try:
        teams = client.list_teams()
        return JSONResponse([
            {"key": t["key"], "name": t["name"]} for t in teams
        ])
    except Exception as exc:
        logger.warning("Failed to fetch Linear teams: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.get("/api/linear/projects")
def api_linear_projects(request: Request, team: str = ""):
    """Return Linear projects for a team (for the dropdown)."""
    if not team:
        return JSONResponse(
            {"error": "team query parameter is required"}, status_code=400,
        )
    client = _get_linear_client(request.app)
    if client is None:
        return JSONResponse(
            {"error": "Linear API key not configured"}, status_code=503,
        )
    try:
        team_id = client.get_team_id(team)
        projects = client.list_team_projects(team_id)
        return JSONResponse([
            {"id": p["id"], "name": p["name"]} for p in projects
        ])
    except Exception as exc:
        logger.warning("Failed to fetch Linear projects for team %s: %s", team, exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


# --- Project creation ---


@router.post("/api/project/create")
async def api_project_create(request: Request):
    """Validate inputs and start background project setup."""
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return JSONResponse(
            {"error": "Botfarm config not available"}, status_code=503,
        )

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"error": "Expected a JSON object"}, status_code=400)

    repo_url = (body.get("repo_url") or "").strip()
    name = (body.get("name") or "").strip()
    team = (body.get("team") or "").strip()
    tracker_project = (body.get("tracker_project") or "").strip()
    create_linear_project = bool(body.get("create_linear_project", False))
    slots_count = body.get("slots", 1)
    create_github = bool(body.get("create_github", False))

    # Validation
    errors = []
    if repo_url and not _GIT_URL_RE.match(repo_url):
        errors.append("Repository URL must be a valid git URL (SSH or HTTPS)")

    if not name:
        errors.append("Project name is required")
    elif any(p.name == name for p in cfg.projects):
        errors.append(f"Project '{name}' already exists")

    if tracker_project and any(
        p.tracker_project == tracker_project for p in cfg.projects
    ):
        errors.append(
            f"Tracker project '{tracker_project}' is already used by another project"
        )

    if not team:
        errors.append("Team is required")

    if not isinstance(slots_count, int) or isinstance(slots_count, bool):
        errors.append("Slots must be an integer")
    elif slots_count < 1 or slots_count > 20:
        errors.append("Slots must be between 1 and 20")

    if errors:
        return JSONResponse({"errors": errors}, status_code=400)

    task_id = str(uuid.uuid4())
    slot_ids = list(range(1, slots_count + 1))

    task_state = {
        "messages": [],
        "done": False,
        "error": None,
        "project_name": name,
    }
    with _setup_tasks_lock:
        _setup_tasks[task_id] = task_state

    def _run_setup():
        def _on_progress(msg: str):
            with _setup_tasks_lock:
                task_state["messages"].append(msg)

        try:
            if create_linear_project and tracker_project:
                _on_progress(f"Creating Linear project '{tracker_project}'...")
                try:
                    client = _get_linear_client(app)
                    if client is None:
                        raise ProjectSetupError(
                            "Linear API key not configured — cannot create project"
                        )
                    result = client.get_or_create_project(team, tracker_project)
                    _on_progress(
                        f"Linear project '{tracker_project}' ready (id: {result['id']})"
                    )
                except ProjectSetupError:
                    raise
                except Exception as exc:
                    raise ProjectSetupError(
                        f"Failed to create Linear project: {exc}"
                    ) from exc

            config_path = Path(cfg.source_path)
            setup_project(
                repo_url=repo_url,
                name=name,
                team=team,
                tracker_project=tracker_project,
                slots=slot_ids,
                config_path=config_path,
                create_github=create_github,
                progress_callback=_on_progress,
            )
            # Notify supervisor to register the new project
            cb = app.state.on_add_project
            if cb is not None:
                cb(name)
                _on_progress("Registered project with supervisor")

            with _setup_tasks_lock:
                task_state["done"] = True

        except ProjectSetupError as exc:
            with _setup_tasks_lock:
                task_state["error"] = str(exc)
                task_state["done"] = True
        except Exception as exc:
            logger.exception("Unexpected error in project setup for %s", name)
            with _setup_tasks_lock:
                task_state["error"] = str(exc)
                task_state["done"] = True

    thread = Thread(target=_run_setup, daemon=True, name=f"setup-{name}")
    thread.start()

    return JSONResponse({"status": "started", "task_id": task_id})


@router.get("/api/project/create/progress")
async def api_project_create_progress(request: Request, task_id: str = ""):
    """SSE endpoint streaming progress events from the background setup thread."""
    if not task_id:
        return JSONResponse(
            {"error": "task_id query parameter is required"}, status_code=400,
        )
    with _setup_tasks_lock:
        task_state = _setup_tasks.get(task_id)
    if task_state is None:
        return JSONResponse({"error": "Unknown task_id"}, status_code=404)

    async def event_generator():
        cursor = 0
        try:
            while True:
                with _setup_tasks_lock:
                    messages = task_state["messages"][cursor:]
                    done = task_state["done"]
                    error = task_state["error"]
                cursor += len(messages)

                for msg in messages:
                    yield {"event": "progress", "data": msg}

                if done:
                    if error:
                        yield {"event": "error", "data": error}
                    else:
                        yield {"event": "done", "data": ""}
                    break

                await asyncio.sleep(0.3)
        finally:
            with _setup_tasks_lock:
                if _setup_tasks.get(task_id, {}).get("done"):
                    _setup_tasks.pop(task_id, None)

    return EventSourceResponse(event_generator())


# --- Git setup (retry) endpoint ---


@router.post("/api/project/setup-git")
async def api_project_setup_git(request: Request):
    """Set up or repair git repo and worktrees for an existing project.

    Reads the project from config and ensures the repo directory and all
    worktrees exist.  Useful when the initial setup failed partway through.
    """
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return JSONResponse(
            {"error": "Botfarm config not available"}, status_code=503,
        )

    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"error": "Expected a JSON object"}, status_code=400)

    name = (body.get("name") or "").strip()
    create_github = bool(body.get("create_github", False))

    if not name:
        return JSONResponse({"error": "Project name is required"}, status_code=400)

    # Verify project exists in config
    if not any(p.name == name for p in cfg.projects):
        return JSONResponse(
            {"error": f"Project '{name}' not found in config"}, status_code=404,
        )

    task_id = str(uuid.uuid4())
    task_state = {
        "messages": [],
        "done": False,
        "error": None,
        "project_name": name,
    }
    with _setup_tasks_lock:
        _setup_tasks[task_id] = task_state

    def _run_setup():
        def _on_progress(msg: str):
            with _setup_tasks_lock:
                task_state["messages"].append(msg)

        try:
            config_path = Path(cfg.source_path)
            setup_project_git(
                name=name,
                config_path=config_path,
                create_github=create_github,
                progress_callback=_on_progress,
            )
            with _setup_tasks_lock:
                task_state["done"] = True

        except ProjectSetupError as exc:
            with _setup_tasks_lock:
                task_state["error"] = str(exc)
                task_state["done"] = True
        except Exception as exc:
            logger.exception("Unexpected error in git setup for %s", name)
            with _setup_tasks_lock:
                task_state["error"] = str(exc)
                task_state["done"] = True

    thread = Thread(target=_run_setup, daemon=True, name=f"setup-git-{name}")
    thread.start()

    return JSONResponse({"status": "started", "task_id": task_id})


# --- Add Project page ---


@router.get("/projects/add", response_class=HTMLResponse)
def add_project_page(request: Request):
    """Render the Add Project form page."""
    app = request.app
    templates = app.state.templates
    state = read_state(app)
    return templates.TemplateResponse("add_project.html", {
        "request": request,
        "active_page": "add_project",
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
        "capacity": get_capacity_data(app),
    })


# --- Utility endpoint ---


@router.get("/api/project/suggest-name")
def api_suggest_name(request: Request, repo_url: str = ""):
    """Return a suggested project name from a repo URL."""
    if not repo_url:
        return JSONResponse({"name": ""})
    return JSONResponse({"name": extract_repo_name(repo_url)})
