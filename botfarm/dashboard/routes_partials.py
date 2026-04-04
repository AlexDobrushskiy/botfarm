"""Partial (htmx) route handlers for dashboard polling endpoints."""

from __future__ import annotations

import asyncio
import sqlite3
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from .state import (
    check_commits_behind,
    collect_devserver_statuses,
    context_fill_class,
    elapsed,
    get_capacity_data,
    get_db,
    linear_url,
    manual_pause_state,
    read_state,
    refresh_and_get_usage,
    supervisor_status,
    usage_is_stale,
)

router = APIRouter()


def _apply_resume_transition(app_state, pause_state: str) -> str:
    """Override pause_state to 'resuming' while resume is in flight."""
    resume_at = getattr(app_state, "resume_requested_at", 0.0)
    if pause_state in ("start_paused", "paused") and resume_at:
        if time.monotonic() - resume_at < 10:
            return "resuming"
        app_state.resume_requested_at = 0.0
    elif pause_state == "running" and resume_at:
        app_state.resume_requested_at = 0.0
    return pause_state


@router.get("/partials/slots", response_class=HTMLResponse)
def partial_slots(request: Request):
    from .routes_main import (
        _enrich_slots_with_codex_review,
        _enrich_slots_with_context_fill,
        _enrich_slots_with_pipeline,
    )

    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    slots = _enrich_slots_with_context_fill(app, state.get("slots", []))
    slots = _enrich_slots_with_pipeline(slots)
    slots = _enrich_slots_with_codex_review(app, slots)
    dispatch_paused = state.get("dispatch_paused", False)
    dispatch_pause_reason = state.get("dispatch_pause_reason")
    project_pauses = state.get("project_pauses", {})
    # Collect project names from config for the "Add Slot" buttons
    cfg = app.state.botfarm_config
    projects = [p.name for p in cfg.projects] if cfg else []
    return templates.TemplateResponse(request, "partials/slots.html", {
        "slots": slots,
        "dispatch_paused": dispatch_paused,
        "dispatch_pause_reason": dispatch_pause_reason,
        "project_pauses": project_pauses,
        "projects": projects,
        "elapsed": elapsed,
        "linear_url": lambda tid: linear_url(app, tid),
        "context_fill_class": context_fill_class,
        "supervisor": supervisor_status(app, state),
    })


@router.get("/partials/supervisor-badge", response_class=HTMLResponse)
def partial_supervisor_badge(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    return templates.TemplateResponse(request, "partials/supervisor_badge.html", {
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/partials/start-paused-banner", response_class=HTMLResponse)
def partial_start_paused_banner(request: Request):
    app = request.app
    templates = app.state.templates
    state = read_state(app)
    pause_state = _apply_resume_transition(app.state, manual_pause_state(state))
    return templates.TemplateResponse(request, "partials/start_paused_banner.html", {
        "pause_state": pause_state,
        "has_callbacks": app.state.on_pause is not None,
    })


@router.get("/partials/usage", response_class=HTMLResponse)
def partial_usage(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    fresh, snapshot_at = refresh_and_get_usage(app)
    usage = fresh if fresh is not None else state.get("usage", {})
    dispatch_paused = state.get("dispatch_paused", False)
    dispatch_pause_reason = state.get("dispatch_pause_reason")
    last_usage_check = snapshot_at or state.get("last_usage_check")
    stale = usage_is_stale(last_usage_check)
    codex_usage = state.get("codex_usage", {})
    auth_mode = getattr(app.state, "auth_mode", "oauth")
    return templates.TemplateResponse(request, "partials/usage.html", {
        "usage": usage,
        "codex_usage": codex_usage,
        "dispatch_paused": dispatch_paused,
        "dispatch_pause_reason": dispatch_pause_reason,
        "last_usage_check": last_usage_check,
        "usage_stale": stale,
        "elapsed": elapsed,
        "auth_mode": auth_mode,
    })


@router.get("/partials/tracker-capacity", response_class=HTMLResponse)
def partial_tracker_capacity(request: Request):
    app = request.app
    templates = request.app.state.templates
    return templates.TemplateResponse(request, "partials/tracker_capacity.html", {
        "capacity": get_capacity_data(app),
        "elapsed": elapsed,
    })


@router.get("/partials/queue", response_class=HTMLResponse)
def partial_queue(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    queue = state.get("queue")
    project_pauses = state.get("project_pauses", {})
    return templates.TemplateResponse(request, "partials/queue.html", {
        "queue": queue,
        "project_pauses": project_pauses,
        "linear_url": lambda tid: linear_url(app, tid),
        "elapsed": elapsed,
        "has_callbacks": app.state.on_pause is not None,
    })


@router.get("/partials/history", response_class=HTMLResponse)
def partial_history(request: Request):
    from .routes_main import _history_context

    templates = request.app.state.templates
    req, ctx = _history_context(request)
    return templates.TemplateResponse(req, "partials/history.html", ctx)


@router.get("/partials/tickets", response_class=HTMLResponse)
def partial_tickets(request: Request):
    from .routes_main import _tickets_context

    templates = request.app.state.templates
    req, ctx = _tickets_context(request)
    return templates.TemplateResponse(req, "partials/tickets.html", ctx)


@router.get("/partials/supervisor-controls", response_class=HTMLResponse)
def partial_supervisor_controls(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    pause_state = _apply_resume_transition(app.state, manual_pause_state(state))
    busy_slots = [
        s for s in state.get("slots", []) if s["status"] == "busy"
    ] if pause_state.startswith("pausing") else []
    return templates.TemplateResponse(request, "partials/supervisor_controls.html", {
        "pause_state": pause_state,
        "busy_slots": busy_slots,
        "supervisor": supervisor_status(app, state),
        "has_callbacks": app.state.on_pause is not None,
    })


@router.get("/partials/update-banner", response_class=HTMLResponse)
async def partial_update_banner(request: Request):
    app = request.app
    templates = request.app.state.templates
    # Check if supervisor signalled that update failed
    update_error = ""
    failed_evt = app.state.update_failed_event
    if failed_evt is not None and failed_evt.is_set():
        failed_evt.clear()
        app.state.update_in_progress = False
        getter = app.state.get_update_failed_message
        if getter is not None:
            update_error = getter()

    if update_error:
        count = await asyncio.to_thread(check_commits_behind, app)
        return templates.TemplateResponse(request, "partials/update_banner.html", {
            "update_status": "failed",
            "update_error": update_error,
            "commits_behind": count,
            "auto_restart": app.state.auto_restart,
        })

    if app.state.update_in_progress:
        return templates.TemplateResponse(request, "partials/update_banner.html", {
            "update_status": "updating",
            "commits_behind": 0,
            "auto_restart": app.state.auto_restart,
        })
    count = await asyncio.to_thread(check_commits_behind, app)
    return templates.TemplateResponse(request, "partials/update_banner.html", {
        "update_status": "idle",
        "commits_behind": count,
        "auto_restart": app.state.auto_restart,
    })


@router.get("/partials/preflight-banner", response_class=HTMLResponse)
def partial_preflight_banner(request: Request):
    from .routes_api import _get_preflight_data

    templates = request.app.state.templates
    data = _get_preflight_data(request.app)
    return templates.TemplateResponse(request, "partials/preflight_banner.html", {
        **data,
    })


@router.get("/partials/health-checks", response_class=HTMLResponse)
def partial_health_checks(request: Request):
    from .routes_api import _get_preflight_data

    templates = request.app.state.templates
    data = _get_preflight_data(request.app)
    return templates.TemplateResponse(request, "partials/health_checks.html", {
        **data,
    })


@router.get("/partials/devserver-status", response_class=HTMLResponse)
def partial_devserver_status(request: Request):
    app = request.app
    templates = request.app.state.templates
    mgr = getattr(app.state, "devserver_manager", None)
    devservers = collect_devserver_statuses(mgr) if mgr is not None else []
    return templates.TemplateResponse(request, "partials/devserver_status.html", {
        "devservers": devservers,
    })


@router.get("/partials/health-badge", response_class=HTMLResponse)
def partial_health_badge(request: Request):
    from .routes_api import _get_preflight_data

    templates = request.app.state.templates
    data = _get_preflight_data(request.app)
    return templates.TemplateResponse(request, "partials/health_badge.html", {
        **data,
    })
