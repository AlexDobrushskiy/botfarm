"""Dev server API routes and SSE log streaming."""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse

from .state import collect_devserver_statuses

logger = logging.getLogger(__name__)

router = APIRouter()

SSE_HEARTBEAT_INTERVAL = 15  # seconds

# Strip ANSI escape sequences from log lines
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _get_manager(request: Request):
    """Return the DevServerManager from app state, or None."""
    return getattr(request.app.state, "devserver_manager", None)


def _get_log_path(request: Request, project: str) -> Path | None:
    """Return the log file path for a project's dev server."""
    mgr = _get_manager(request)
    if mgr is None:
        return None
    return mgr.log_path(project)


def _has_run_command(request: Request, project: str) -> bool:
    """Check if a project has a run_command configured."""
    mgr = _get_manager(request)
    if mgr is None:
        return False
    return project in mgr.project_names


@router.post("/api/devserver/{project}/start")
def api_devserver_start(request: Request, project: str):
    """Start the dev server for a project."""
    mgr = _get_manager(request)
    if mgr is None:
        return JSONResponse(
            {"error": "Dev server manager not available"},
            status_code=503,
        )
    if not _has_run_command(request, project):
        return JSONResponse(
            {"error": f"No run_command configured for project {project!r}"},
            status_code=400,
        )
    # Check if already running
    status = mgr.status(project)
    if status["status"] == "running":
        return JSONResponse(
            {"error": f"Dev server for {project!r} is already running"},
            status_code=409,
        )
    try:
        mgr.start(project)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse({"status": "ok", "message": f"Dev server started for {project}"})


@router.post("/api/devserver/{project}/stop")
def api_devserver_stop(request: Request, project: str):
    """Stop the dev server for a project."""
    mgr = _get_manager(request)
    if mgr is None:
        return JSONResponse(
            {"error": "Dev server manager not available"},
            status_code=503,
        )
    status = mgr.status(project)
    if status["status"] == "stopped":
        return JSONResponse(
            {"error": f"Dev server for {project!r} is not running"},
            status_code=409,
        )
    mgr.stop(project)
    return JSONResponse({"status": "ok", "message": f"Dev server stopped for {project}"})


@router.post("/api/devserver/{project}/restart")
def api_devserver_restart(request: Request, project: str):
    """Restart the dev server for a project."""
    mgr = _get_manager(request)
    if mgr is None:
        return JSONResponse(
            {"error": "Dev server manager not available"},
            status_code=503,
        )
    if not _has_run_command(request, project):
        return JSONResponse(
            {"error": f"No run_command configured for project {project!r}"},
            status_code=400,
        )
    try:
        mgr.restart(project)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    return JSONResponse({"status": "ok", "message": f"Dev server restarted for {project}"})


@router.get("/api/devserver/status")
def api_devserver_status(request: Request):
    """Return status of all projects' dev servers."""
    mgr = _get_manager(request)
    if mgr is None:
        return JSONResponse({"servers": []})
    return JSONResponse({"servers": collect_devserver_statuses(mgr)})


def _is_devserver_running(request: Request, project: str) -> bool:
    """Check if a dev server is currently running."""
    mgr = _get_manager(request)
    if mgr is None:
        return False
    status = mgr.status(project)
    return status["status"] == "running"


@router.get("/api/devserver/{project}/stream")
async def stream_devserver_log(request: Request, project: str):
    """SSE endpoint that tails the dev server log file."""
    log_path = _get_log_path(request, project)
    if log_path is None or not log_path.exists():
        return JSONResponse({"error": "No log file found"}, status_code=404)

    async def event_generator():
        try:
            idle_elapsed = 0.0
            with open(log_path, errors="replace") as f:
                while True:
                    line = await asyncio.to_thread(f.readline)
                    if line:
                        idle_elapsed = 0.0
                        stripped = _ANSI_RE.sub("", line.rstrip("\n"))
                        if stripped:
                            yield {
                                "event": "stdout",
                                "data": stripped,
                            }
                    else:
                        running = await asyncio.to_thread(
                            _is_devserver_running, request, project
                        )
                        if not running:
                            yield {"event": "system", "data": "Dev server stopped"}
                            yield {"event": "done", "data": ""}
                            break
                        await asyncio.sleep(0.5)
                        idle_elapsed += 0.5
                        if idle_elapsed >= SSE_HEARTBEAT_INTERVAL:
                            idle_elapsed = 0.0
                            yield {"event": "heartbeat", "data": ""}
        except OSError:
            yield {"event": "error", "data": "Failed to read log file"}

    return EventSourceResponse(event_generator())
