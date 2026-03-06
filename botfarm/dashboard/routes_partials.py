"""Partial (htmx) route handlers for dashboard polling endpoints."""

from __future__ import annotations

import sqlite3
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from .state import (
    check_commits_behind,
    context_fill_class,
    elapsed,
    get_capacity_data,
    get_dashboard_last_fresh_time,
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
    return templates.TemplateResponse("partials/slots.html", {
        "request": request,
        "slots": slots,
        "dispatch_paused": dispatch_paused,
        "dispatch_pause_reason": dispatch_pause_reason,
        "project_pauses": project_pauses,
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
    return templates.TemplateResponse("partials/supervisor_badge.html", {
        "request": request,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/partials/start-paused-banner", response_class=HTMLResponse)
def partial_start_paused_banner(request: Request):
    app = request.app
    templates = app.state.templates
    state = read_state(app)
    pause_state = _apply_resume_transition(app.state, manual_pause_state(state))
    return templates.TemplateResponse("partials/start_paused_banner.html", {
        "request": request,
        "pause_state": pause_state,
        "has_callbacks": app.state.on_pause is not None,
    })


@router.get("/partials/usage", response_class=HTMLResponse)
def partial_usage(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    fresh = refresh_and_get_usage(app)
    usage = fresh if fresh is not None else state.get("usage", {})
    dispatch_paused = state.get("dispatch_paused", False)
    dispatch_pause_reason = state.get("dispatch_pause_reason")
    dashboard_checked = get_dashboard_last_fresh_time(app)
    last_usage_check = dashboard_checked or state.get("last_usage_check")
    stale = usage_is_stale(last_usage_check)
    return templates.TemplateResponse("partials/usage.html", {
        "request": request,
        "usage": usage,
        "dispatch_paused": dispatch_paused,
        "dispatch_pause_reason": dispatch_pause_reason,
        "last_usage_check": last_usage_check,
        "usage_stale": stale,
        "elapsed": elapsed,
    })


@router.get("/partials/linear-capacity", response_class=HTMLResponse)
def partial_linear_capacity(request: Request):
    app = request.app
    templates = request.app.state.templates
    return templates.TemplateResponse("partials/linear_capacity.html", {
        "request": request,
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
    return templates.TemplateResponse("partials/queue.html", {
        "request": request,
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
    ctx = _history_context(request)
    return templates.TemplateResponse("partials/history.html", ctx)


@router.get("/partials/tickets", response_class=HTMLResponse)
def partial_tickets(request: Request):
    from .routes_main import _tickets_context

    templates = request.app.state.templates
    ctx = _tickets_context(request)
    return templates.TemplateResponse("partials/tickets.html", ctx)


@router.get("/partials/supervisor-controls", response_class=HTMLResponse)
def partial_supervisor_controls(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    pause_state = _apply_resume_transition(app.state, manual_pause_state(state))
    busy_slots = [
        s for s in state.get("slots", []) if s["status"] == "busy"
    ] if pause_state.startswith("pausing") else []
    return templates.TemplateResponse("partials/supervisor_controls.html", {
        "request": request,
        "pause_state": pause_state,
        "busy_slots": busy_slots,
        "supervisor": supervisor_status(app, state),
        "has_callbacks": app.state.on_pause is not None,
    })


@router.get("/partials/update-banner", response_class=HTMLResponse)
def partial_update_banner(request: Request):
    app = request.app
    templates = request.app.state.templates
    # Check if supervisor signalled that update failed
    failed_evt = app.state.update_failed_event
    if failed_evt is not None and failed_evt.is_set():
        failed_evt.clear()
        app.state.update_in_progress = False

    if app.state.update_in_progress:
        return templates.TemplateResponse("partials/update_banner.html", {
            "request": request,
            "update_status": "updating",
            "commits_behind": 0,
            "auto_restart": app.state.auto_restart,
        })
    count = check_commits_behind(app)
    return templates.TemplateResponse("partials/update_banner.html", {
        "request": request,
        "update_status": "idle",
        "commits_behind": count,
        "auto_restart": app.state.auto_restart,
    })


@router.get("/partials/preflight-banner", response_class=HTMLResponse)
def partial_preflight_banner(request: Request):
    from .routes_api import _get_preflight_data

    templates = request.app.state.templates
    data = _get_preflight_data(request.app)
    return templates.TemplateResponse("partials/preflight_banner.html", {
        "request": request,
        **data,
    })


@router.get("/partials/health-checks", response_class=HTMLResponse)
def partial_health_checks(request: Request):
    from .routes_api import _get_preflight_data

    templates = request.app.state.templates
    data = _get_preflight_data(request.app)
    return templates.TemplateResponse("partials/health_checks.html", {
        "request": request,
        **data,
    })


@router.get("/partials/health-badge", response_class=HTMLResponse)
def partial_health_badge(request: Request):
    from .routes_api import _get_preflight_data

    templates = request.app.state.templates
    data = _get_preflight_data(request.app)
    return templates.TemplateResponse("partials/health_badge.html", {
        "request": request,
        **data,
    })
