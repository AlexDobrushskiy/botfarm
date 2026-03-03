"""Log viewer routes: page, SSE streaming, static content."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from sse_starlette.sse import EventSourceResponse

from botfarm.db import get_task, get_task_by_ticket, load_all_slots
from botfarm.worker import STAGES

from .formatters import format_codex_ndjson_line, format_ndjson_line
from .state import get_db, linear_url, manual_pause_state, read_state, supervisor_status

logger = logging.getLogger(__name__)

router = APIRouter()

MAX_LOG_DISPLAY = 2 * 1024 * 1024  # 2 MB


def _read_log_file(path: Path) -> str:
    """Read a log file with size guard — tail-truncate if over MAX_LOG_DISPLAY."""
    try:
        stat = path.stat()
        if stat.st_size > MAX_LOG_DISPLAY:
            with open(path, errors="replace") as f:
                f.seek(stat.st_size - MAX_LOG_DISPLAY)
                f.readline()  # skip partial line
                return "... (truncated, showing last 2 MB) ...\n" + f.read()
        return path.read_text(errors="replace")
    except OSError:
        return ""


def _find_log_files(app, ticket_id: str, stage: str | None = None) -> list[Path]:
    """Find log files for a ticket, optionally filtered by stage."""
    logs_base = app.state.logs_dir
    if not logs_base:
        return []
    ticket_dir = logs_base / ticket_id
    if not ticket_dir.resolve().is_relative_to(logs_base.resolve()):
        return []
    if not ticket_dir.is_dir():
        return []
    files = []
    for f in ticket_dir.iterdir():
        if not f.is_file() or not f.name.endswith(".log"):
            continue
        if stage:
            if not f.name.startswith(stage):
                continue
            rest = f.name[len(stage):]
            if rest and rest[0] not in ("-", "."):
                continue
        files.append(f)
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _find_latest_log(app, ticket_id: str, stage: str) -> Path | None:
    """Find the most recent log file for a ticket + stage."""
    files = _find_log_files(app, ticket_id, stage)
    return files[0] if files else None


_ALL_LOG_STAGES = list(STAGES) + ["ci_fix", "codex_review", "resolve_conflict"]


def _available_stages_with_logs(app, ticket_id: str) -> list[str]:
    """Return stage names that have log files, in canonical order."""
    files = _find_log_files(app, ticket_id)
    if not files:
        return []
    found = set()
    for f in files:
        name = f.name
        for s in _ALL_LOG_STAGES:
            if name.startswith(s) and (
                len(name) == len(s)
                or name[len(s)] in ("-", ".")
            ):
                found.add(s)
                break
    return [s for s in _ALL_LOG_STAGES if s in found]


def _is_stage_active(app, ticket_id: str, stage: str) -> bool:
    """Check if a stage is currently running for this ticket."""
    conn = get_db(app)
    if not conn:
        return False
    try:
        rows = load_all_slots(conn)
        for row in rows:
            if (
                row["ticket_id"] == ticket_id
                and row["stage"] == stage
                and row["status"] == "busy"
            ):
                return True
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()
    return False


def _resolve_task(app, task_id: str) -> tuple[dict | None, str]:
    """Resolve task_id (numeric or ticket_id) to (task_dict, ticket_id)."""
    conn = get_db(app)
    if not conn:
        return None, task_id
    try:
        task_row = None
        try:
            task_row = get_task(conn, int(task_id))
        except ValueError:
            pass
        if task_row is None:
            task_row = get_task_by_ticket(conn, task_id)
        if task_row is not None:
            task = dict(task_row)
            return task, task["ticket_id"]
        return None, task_id
    finally:
        conn.close()


@router.get("/task/{task_id}/logs", response_class=HTMLResponse)
def log_viewer_page(request: Request, task_id: str):
    """Log viewer page — shows available stages, redirects to latest."""
    app = request.app
    templates = request.app.state.templates
    task, ticket_id = _resolve_task(app, task_id)

    available = _available_stages_with_logs(app, ticket_id)
    state = read_state(app)
    if not available:
        return templates.TemplateResponse("log_viewer.html", {
            "request": request,
            "task": task,
            "ticket_id": ticket_id,
            "stages_with_logs": [],
            "current_stage": None,
            "log_content": None,
            "is_live": False,
            "linear_url": lambda tid: linear_url(app, tid),
            "supervisor": supervisor_status(app, state),
            "pause_state": manual_pause_state(state),
        })

    # Default to the most recent active stage, or the last available
    active_stage = None
    for s in reversed(available):
        if _is_stage_active(app, ticket_id, s):
            active_stage = s
            break
    default_stage = active_stage or available[-1]
    is_live = _is_stage_active(app, ticket_id, default_stage)

    # Load content for completed stages
    log_content = None
    if not is_live:
        log_file = _find_latest_log(app, ticket_id, default_stage)
        if log_file:
            log_content = _read_log_file(log_file) or None

    return templates.TemplateResponse("log_viewer.html", {
        "request": request,
        "task": task,
        "ticket_id": ticket_id,
        "stages_with_logs": available,
        "current_stage": default_stage,
        "log_content": log_content,
        "is_live": is_live,
        "linear_url": lambda tid: linear_url(app, tid),
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/task/{task_id}/logs/{stage}", response_class=HTMLResponse)
def log_viewer_stage_page(request: Request, task_id: str, stage: str):
    """Log viewer page for a specific stage."""
    app = request.app
    templates = request.app.state.templates
    task, ticket_id = _resolve_task(app, task_id)

    available = _available_stages_with_logs(app, ticket_id)
    is_live = _is_stage_active(app, ticket_id, stage)

    log_content = None
    if not is_live:
        log_file = _find_latest_log(app, ticket_id, stage)
        if log_file:
            log_content = _read_log_file(log_file) or None

    state = read_state(app)
    return templates.TemplateResponse("log_viewer.html", {
        "request": request,
        "task": task,
        "ticket_id": ticket_id,
        "stages_with_logs": available,
        "current_stage": stage,
        "log_content": log_content,
        "is_live": is_live,
        "linear_url": lambda tid: linear_url(app, tid),
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/api/logs/{ticket_id}/{stage}/stream")
async def stream_log(request: Request, ticket_id: str, stage: str):
    """SSE endpoint that tails the active log file for a running stage."""
    app = request.app
    log_file = _find_latest_log(app, ticket_id, stage)
    if log_file is None:
        return PlainTextResponse("No log file found", status_code=404)

    # Use Codex formatter for codex_review stage logs
    line_formatter = format_codex_ndjson_line if stage == "codex_review" else format_ndjson_line

    async def event_generator():
        try:
            with open(log_file, errors="replace") as f:
                while True:
                    line = await asyncio.to_thread(f.readline)
                    if line:
                        event_type, formatted = line_formatter(line)
                        if formatted:
                            yield {
                                "event": event_type,
                                "data": formatted,
                            }
                    else:
                        if not await asyncio.to_thread(
                            _is_stage_active, app, ticket_id, stage
                        ):
                            yield {"event": "done", "data": ""}
                            break
                        await asyncio.sleep(0.5)
        except OSError:
            yield {"event": "error", "data": "Failed to read log file"}

    return EventSourceResponse(event_generator())


@router.get("/api/logs/{ticket_id}/{stage}/content")
def get_log_content(request: Request, ticket_id: str, stage: str):
    """Return the full log content for a completed stage."""
    app = request.app
    log_file = _find_latest_log(app, ticket_id, stage)
    if log_file is None:
        return PlainTextResponse("No log file found", status_code=404)
    content = _read_log_file(log_file)
    if not content:
        return PlainTextResponse("Failed to read log file", status_code=500)
    return PlainTextResponse(content)
