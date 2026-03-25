"""API routes for adding and removing projects."""

from __future__ import annotations

import asyncio
import logging
import re
import shutil
import uuid
from pathlib import Path
from threading import Lock, Thread

import yaml
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from sse_starlette.sse import EventSourceResponse

from botfarm.bugtracker import create_client
from botfarm.config import DEFAULT_CONFIG_DIR, write_yaml_atomic
from botfarm.db import delete_project_data, init_db, load_all_slots
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


def _get_bugtracker_client(app):
    cfg = app.state.botfarm_config
    if cfg is None or not cfg.bugtracker.api_key:
        return None
    return create_client(cfg)


# --- Bugtracker data endpoints ---


def _api_teams(app) -> JSONResponse:
    """Shared logic for fetching bugtracker teams."""
    client = _get_bugtracker_client(app)
    if client is None:
        return JSONResponse(
            {"error": "Bugtracker API key not configured"}, status_code=503,
        )
    try:
        teams = client.list_teams()
        return JSONResponse([
            {"key": t["key"], "name": t["name"]} for t in teams
        ])
    except Exception as exc:
        logger.warning("Failed to fetch teams: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


def _api_projects(app, team: str) -> JSONResponse:
    """Shared logic for fetching bugtracker projects for a team."""
    if not team:
        return JSONResponse(
            {"error": "team query parameter is required"}, status_code=400,
        )
    client = _get_bugtracker_client(app)
    if client is None:
        return JSONResponse(
            {"error": "Bugtracker API key not configured"}, status_code=503,
        )
    try:
        team_id = client.get_team_id(team)
        projects = client.list_team_projects(team_id)
        return JSONResponse([
            {"id": p["id"], "name": p["name"]} for p in projects
        ])
    except Exception as exc:
        logger.warning("Failed to fetch projects for team %s: %s", team, exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.get("/api/linear/teams")
def api_linear_teams(request: Request):
    """Return bugtracker teams for the dropdown (legacy alias)."""
    return _api_teams(request.app)


@router.get("/api/linear/projects")
def api_linear_projects(request: Request, team: str = ""):
    """Return bugtracker projects for a team (legacy alias)."""
    return _api_projects(request.app, team)


@router.get("/api/bugtracker/teams")
def api_bugtracker_teams(request: Request):
    """Return bugtracker teams for the dropdown."""
    return _api_teams(request.app)


@router.get("/api/bugtracker/projects")
def api_bugtracker_projects(request: Request, team: str = ""):
    """Return bugtracker projects for a team."""
    return _api_projects(request.app, team)


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
    project_type_raw = body.get("project_type")
    if not isinstance(project_type_raw, str):
        project_type_raw = ""
    project_type = project_type_raw.strip()
    setup_commands_raw = body.get("setup_commands")

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

    # Parse setup_commands — accept list or newline-separated string
    setup_commands: list[str] = []
    if isinstance(setup_commands_raw, list):
        setup_commands = [str(c).strip() for c in setup_commands_raw if str(c).strip()]
    elif isinstance(setup_commands_raw, str) and setup_commands_raw.strip():
        setup_commands = [
            line.strip() for line in setup_commands_raw.strip().splitlines()
            if line.strip()
        ]

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
                    client = _get_bugtracker_client(app)
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
                project_type=project_type,
                setup_commands=setup_commands or None,
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
    return templates.TemplateResponse(request, "add_project.html", {
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


# --- Remove project ---


@router.get("/api/project/{name}/remove-info")
def api_project_remove_info(request: Request, name: str):
    """Return project details for the remove confirmation dialog.

    Returns active slots, repo directory info, and whether the repo is
    inside the managed projects path (eligible for cleanup).
    """
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return JSONResponse(
            {"error": "Botfarm config not available"}, status_code=503,
        )

    # Find project in config
    project_cfg = None
    for p in cfg.projects:
        if p.name == name:
            project_cfg = p
            break
    if project_cfg is None:
        return JSONResponse(
            {"error": f"Project '{name}' not found in config"}, status_code=404,
        )

    # Gather active slot info from database
    active_slots = []
    conn = None
    try:
        conn = init_db(app.state.db_path)
        rows = load_all_slots(conn)
        for row in rows:
            if row["project"] == name and row["status"] in (
                "busy", "paused_limit", "paused_manual",
            ):
                active_slots.append({
                    "slot_id": row["slot_id"],
                    "status": row["status"],
                    "ticket_id": row["ticket_id"] or None,
                })
    except Exception as exc:
        logger.warning("Failed to read slots for remove-info: %s", exc)
    finally:
        if conn:
            conn.close()

    # Determine repo directory and whether it's manageable
    base_dir = getattr(project_cfg, "base_dir", "")
    repo_path = Path(base_dir).expanduser().resolve() if base_dir else None
    managed_root = (DEFAULT_CONFIG_DIR / "projects").expanduser().resolve()
    projects_dir = None
    can_clean = False
    if repo_path and repo_path.parent != repo_path:
        candidate = repo_path.parent
        try:
            rel = candidate.relative_to(managed_root)
            if rel.parts:
                projects_dir = str(candidate)
                can_clean = candidate.exists()
        except ValueError:
            pass

    return JSONResponse({
        "name": name,
        "team": project_cfg.team,
        "slots": project_cfg.slots,
        "active_slots": active_slots,
        "repo_dir": projects_dir,
        "can_clean": can_clean,
    })


@router.post("/api/project/remove")
async def api_project_remove(request: Request):
    """Remove a project from config, clean up database, optionally remove repo.

    Expects JSON body: ``{"name": "...", "force": false, "clean": false}``
    """
    app = request.app
    cfg = app.state.botfarm_config
    if cfg is None:
        return JSONResponse(
            {"ok": False, "errors": ["Botfarm config not available"]},
            status_code=503,
        )

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            {"ok": False, "errors": ["Invalid JSON body"]}, status_code=400,
        )

    name = (body.get("name") or "").strip()
    force = bool(body.get("force", False))
    clean = bool(body.get("clean", False))

    if not name:
        return JSONResponse(
            {"ok": False, "errors": ["Project name is required"]},
            status_code=400,
        )

    # Verify project exists in config (use source_path to read raw YAML)
    config_path = Path(cfg.source_path) if cfg.source_path else None
    if not config_path or not config_path.exists():
        return JSONResponse(
            {"ok": False, "errors": ["Config file not found"]},
            status_code=500,
        )

    raw = config_path.read_text()
    data = yaml.safe_load(raw) or {}
    projects = data.get("projects") or []

    project_entry = None
    project_index = None
    for i, p in enumerate(projects):
        if isinstance(p, dict) and p.get("name") == name:
            project_entry = p
            project_index = i
            break

    if project_entry is None:
        return JSONResponse(
            {"ok": False, "errors": [f"Project '{name}' not found in config"]},
            status_code=404,
        )

    # Check for active slots unless force
    conn = None
    try:
        conn = init_db(app.state.db_path)
    except Exception as exc:
        logger.warning("Failed to open database for remove: %s", exc)

    try:
        if conn and not force:
            rows = load_all_slots(conn)
            active = [
                r for r in rows
                if r["project"] == name
                and r["status"] in ("busy", "paused_limit", "paused_manual")
            ]
            if active:
                details = [
                    f"Slot {r['slot_id']}: {r['status']} ({r['ticket_id'] or 'no ticket'})"
                    for r in active
                ]
                return JSONResponse(
                    {
                        "ok": False,
                        "errors": [
                            f"Project '{name}' has {len(active)} active slot(s): "
                            + "; ".join(details)
                            + ". Use force to remove anyway."
                        ],
                    },
                    status_code=409,
                )

        # Determine repo directory for cleanup
        base_dir = project_entry.get("base_dir", "")
        repo_path = Path(base_dir).expanduser().resolve() if base_dir else None
        managed_root = (DEFAULT_CONFIG_DIR / "projects").expanduser().resolve()
        projects_dir = None
        if repo_path and repo_path.parent != repo_path:
            candidate = repo_path.parent
            try:
                rel = candidate.relative_to(managed_root)
                if rel.parts:
                    projects_dir = candidate
            except ValueError:
                pass

        # 1. Remove from config.yaml
        projects.pop(project_index)
        data["projects"] = projects
        write_yaml_atomic(config_path, data)

        # 2. Clean up database
        db_summary = {}
        if conn:
            counts = delete_project_data(conn, name)
            conn.commit()
            db_summary = counts
    finally:
        if conn:
            conn.close()

    # 3. Optionally remove repo directory
    cleaned_dir = False
    if clean and projects_dir and projects_dir.exists():
        shutil.rmtree(projects_dir)
        cleaned_dir = True

    # 4. Notify supervisor to drop in-memory state
    cb = app.state.on_remove_project
    if cb is not None:
        try:
            cb(name)
        except Exception:
            logger.exception("on_remove_project callback failed for %s", name)

    return JSONResponse({
        "ok": True,
        "data": {
            "db_cleanup": db_summary,
            "cleaned_dir": cleaned_dir,
        },
    })
