"""API routes: pause, resume, update, preflight, health, workflow CRUD, cleanup."""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from botfarm.db import (
    get_cleanup_batch,
    get_cleanup_batch_items,
    get_last_cleanup_batch_time,
    init_db,
    list_cleanup_batches,
    load_all_slots,
    save_project_pause_state,
)
from botfarm.bugtracker import CleanupService, CooldownError, create_client
from botfarm.workflow import (
    create_loop,
    create_pipeline,
    create_stage,
    delete_loop,
    delete_pipeline,
    delete_stage,
    duplicate_pipeline,
    reorder_stages,
    update_loop,
    update_pipeline,
    update_stage,
    validate_pipeline,
)

from .state import (
    elapsed,
    get_capacity_data,
    get_db,
    manual_pause_state,
    read_state,
    supervisor_status,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# --- Preflight / System Health ---

# Actionable guidance for common preflight check failures
_PREFLIGHT_GUIDANCE: dict[str, str] = {
    "git_repo": "Verify base_dir path in config and that git remote 'origin' is accessible",
    "linear_api": "Check bugtracker.api_key in config and verify team/status names match your Linear workspace",
    "linear_team": "Check bugtracker.api_key in config and verify team/status names match your Linear workspace",
    "linear_status": "Check bugtracker.api_key in config and verify team/status names match your Linear workspace",
    "jira_project": "Check bugtracker.api_key in config and verify project/status names match your Jira instance",
    "jira_status": "Check bugtracker.api_key in config and verify project/status names match your Jira instance",
    "identity_github_token": "Verify GitHub token is valid and the associated user has collaborator access to the repository",
    "identity_ssh_key": "Check SSH key path in config, verify file exists and has correct permissions (0600)",
    "identity_tracker_key": "Verify the identity's bugtracker API key is valid",
    "database": "Check DB path permissions or schema version",
    "config_consistency": "Review config for duplicate slot IDs or invalid project settings",
    "credentials": "Verify Claude OAuth credentials are loaded correctly",
    "notifications_webhook": "Check the webhook URL in config is valid and reachable",
    "worktree_dirs": "Verify worktree parent directories exist and are writable",
    "identity_cross_validation": "Review identity config for inconsistent or partial credential sets",
}


def _get_preflight_data(app) -> dict:
    """Build preflight template context from supervisor callbacks."""
    getter = app.state.get_preflight_results
    degraded_getter = app.state.get_degraded
    results = getter() if getter else []
    degraded = degraded_getter() if degraded_getter else False
    checks = []
    for r in results:
        name_prefix = r.name.split(":")[0] if ":" in r.name else r.name
        guidance = _PREFLIGHT_GUIDANCE.get(name_prefix, "")
        checks.append({
            "name": r.name,
            "passed": r.passed,
            "message": r.message,
            "critical": r.critical,
            "guidance": guidance,
        })
    failed_critical = sum(1 for c in checks if not c["passed"] and c["critical"])
    return {
        "degraded": degraded,
        "checks": checks,
        "failed_critical": failed_critical,
    }


# --- Stop Slot API ---

@router.post("/api/slot/stop")
async def api_stop_slot(request: Request):
    """Request a slot stop via the supervisor's thread-safe callback."""
    cb = request.app.state.on_stop_slot
    if cb is None:
        return JSONResponse(
            {"error": "Stop not available (supervisor not connected)"},
            status_code=503,
        )
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"error": "Expected a JSON object"}, status_code=400)
    project = body.get("project", "")
    slot_id = body.get("slot_id")
    if not project or slot_id is None:
        return JSONResponse(
            {"error": "project and slot_id are required"}, status_code=400,
        )
    if not isinstance(project, str):
        return JSONResponse(
            {"error": "project must be a string"}, status_code=400,
        )
    # Reject non-integral types (bool is a subclass of int, float silently truncates)
    if isinstance(slot_id, bool) or not isinstance(slot_id, (int, str)):
        return JSONResponse(
            {"error": "slot_id must be an integer"}, status_code=400,
        )
    if isinstance(slot_id, str):
        try:
            slot_id = int(slot_id)
        except ValueError:
            return JSONResponse(
                {"error": "slot_id must be an integer"}, status_code=400,
            )
    # Validate ticket_id against current slot state to prevent stopping a
    # reassigned slot (the modal snapshot may be stale).
    expected_ticket = body.get("ticket_id")
    if expected_ticket is not None:
        if not isinstance(expected_ticket, str):
            return JSONResponse(
                {"error": "ticket_id must be a string"}, status_code=400,
            )
        conn = None
        try:
            conn = init_db(request.app.state.db_path)
            rows = load_all_slots(conn)
            current_ticket = None
            current_status = None
            for row in rows:
                if row["project"] == project and row["slot_id"] == slot_id:
                    current_ticket = row["ticket_id"]
                    current_status = row["status"]
                    break
            # Reject if the slot is no longer stoppable
            stoppable = {"busy", "paused_manual", "paused_limit"}
            if current_status is not None and current_status not in stoppable:
                return JSONResponse(
                    {"error": f"Slot is no longer stoppable (status: {current_status})"},
                    status_code=409,
                )
            # Reject if the ticket has changed since the modal was opened
            if expected_ticket and current_ticket != expected_ticket:
                return JSONResponse(
                    {"error": f"Slot ticket has changed (expected {expected_ticket}, current {current_ticket})"},
                    status_code=409,
                )
        finally:
            if conn is not None:
                conn.close()
    cb(project, slot_id)
    return JSONResponse({
        "status": "requested",
        "message": f"Stop requested for {project}/{slot_id}",
    })


# --- Add Slot API ---

@router.post("/api/slot/add")
async def api_add_slot(request: Request):
    """Request a new slot be added via the supervisor's thread-safe callback."""
    cb = request.app.state.on_add_slot
    if cb is None:
        return JSONResponse(
            {"error": "Not available (supervisor not connected)"},
            status_code=503,
        )
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"error": "Expected a JSON object"}, status_code=400)
    project = body.get("project", "")
    if not isinstance(project, str):
        return JSONResponse(
            {"error": "project must be a string"}, status_code=400,
        )
    if not project:
        return JSONResponse(
            {"error": "project is required"}, status_code=400,
        )
    cb(project)
    return JSONResponse({
        "status": "requested",
        "message": f"Slot addition requested for {project}",
    })


# --- Pause / Resume API ---

@router.post("/api/pause")
def api_pause(request: Request):
    cb = request.app.state.on_pause
    if cb is None:
        return JSONResponse(
            {"error": "Pause not available (supervisor not connected)"},
            status_code=503,
        )
    cb()
    return JSONResponse({"status": "ok"})


@router.post("/api/resume")
def api_resume(request: Request):
    cb = request.app.state.on_resume
    if cb is None:
        return JSONResponse(
            {"error": "Resume not available (supervisor not connected)"},
            status_code=503,
        )
    cb()
    request.app.state.resume_requested_at = time.monotonic()
    return JSONResponse({"status": "ok"})


@router.post("/api/project/pause")
def api_project_pause(request: Request, project: str = "", reason: str = ""):
    """Pause dispatch for a specific project."""
    if not project:
        return JSONResponse({"error": "project is required"}, status_code=400)
    conn = None
    try:
        conn = init_db(request.app.state.db_path)
        save_project_pause_state(conn, project=project, paused=True, reason=reason or None)
        conn.commit()
        return JSONResponse({"status": "ok", "project": project, "paused": True})
    except Exception as exc:
        logger.warning("Failed to pause project %s: %s", project, exc)
        return JSONResponse({"error": str(exc)}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.post("/api/project/resume")
def api_project_resume(request: Request, project: str = ""):
    """Resume dispatch for a specific project."""
    if not project:
        return JSONResponse({"error": "project is required"}, status_code=400)
    conn = None
    try:
        conn = init_db(request.app.state.db_path)
        save_project_pause_state(conn, project=project, paused=False)
        conn.commit()
        return JSONResponse({"status": "ok", "project": project, "paused": False})
    except Exception as exc:
        logger.warning("Failed to resume project %s: %s", project, exc)
        return JSONResponse({"error": str(exc)}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


# --- Update API ---

@router.post("/api/update")
def api_update(request: Request):
    app = request.app
    if not app.state.auto_restart:
        return JSONResponse(
            {"error": "Auto-restart is disabled. Update manually on the server."},
            status_code=409,
        )
    cb = app.state.on_update
    if cb is None:
        return JSONResponse(
            {"error": "Update not available (supervisor not connected)"},
            status_code=503,
        )
    app.state.update_in_progress = True
    cb()
    return JSONResponse({"status": "ok"})


# --- Health page ---

@router.get("/health", response_class=HTMLResponse)
def health_page(request: Request):
    app = request.app
    templates = app.state.templates
    data = _get_preflight_data(app)
    state = read_state(app)
    return templates.TemplateResponse(request, "health.html", {
        "active_page": "health",
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
        **data,
    })


@router.post("/api/rerun-preflight")
def api_rerun_preflight(request: Request):
    cb = request.app.state.on_rerun_preflight
    if cb is None:
        return JSONResponse(
            {"error": "Preflight re-run not available (supervisor not connected)"},
            status_code=503,
        )
    cb()
    return JSONResponse({"status": "ok"})


@router.get("/api/preflight-results")
def api_preflight_results(request: Request):
    """Return preflight check results as JSON (used by CLI ``botfarm preflight``)."""
    data = _get_preflight_data(request.app)
    return JSONResponse(data)


# --- Usage Refresh API ---

@router.post("/api/usage/refresh")
async def api_usage_refresh(request: Request):
    """Manually trigger a usage stats refresh from the Anthropic API.

    Uses the shared ``app.state._usage_poller`` so that 429 backoff state
    persists across requests.  Returns fresh usage data on success, or a
    descriptive error message (including HTTP status codes like 401/429)
    so the dashboard can surface API problems to the user.
    """
    poller = request.app.state._usage_poller

    # Respect 429 backoff — never hammer a rate-limited API
    if poller.in_429_backoff:
        remaining = int(
            poller.effective_poll_interval - (time.monotonic() - poller._last_poll)
        )
        return JSONResponse(
            {"error": f"Rate limited by Anthropic. Retry in {max(remaining, 1)}s."},
            status_code=429,
        )

    def _refresh():
        conn = init_db(request.app.state.db_path)
        try:
            return poller.manual_refresh(conn)
        finally:
            conn.close()

    try:
        state = await asyncio.to_thread(_refresh)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=503)
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        if code == 401:
            msg = "Anthropic API returned 401 Unauthorized — check API credentials"
        elif code == 429:
            msg = "Anthropic API returned 429 Too Many Requests — try again later"
        elif code >= 500:
            msg = f"Anthropic API returned {code} — server error"
        else:
            msg = f"Anthropic API returned HTTP {code}"
        return JSONResponse({"error": msg, "status_code": code}, status_code=502)
    except (httpx.ConnectTimeout, httpx.PoolTimeout):
        return JSONResponse(
            {"error": "Connection to Anthropic API timed out"},
            status_code=504,
        )
    except httpx.ConnectError:
        return JSONResponse(
            {"error": "Could not connect to Anthropic API"},
            status_code=502,
        )
    except Exception as exc:
        return JSONResponse(
            {"error": f"API call failed: {exc}"},
            status_code=502,
        )

    # Invalidate usage cache so htmx partials pick up fresh data
    with request.app.state._usage_refresh_lock:
        request.app.state._last_usage_refresh["time"] = None

    return JSONResponse({"status": "ok", "usage": state.to_dict()})


# --- Workflow API ---

def _pipeline_to_dict(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    """Build a JSON-serialisable dict for a pipeline including stage/loop IDs."""
    row = conn.execute(
        "SELECT * FROM pipeline_templates WHERE id = ?", (pipeline_id,)
    ).fetchone()
    if row is None:
        return None
    stages = [
        {
            "id": s["id"],
            "name": s["name"],
            "stage_order": s["stage_order"],
            "executor_type": s["executor_type"],
            "identity": s["identity"],
            "prompt_template": s["prompt_template"],
            "max_turns": s["max_turns"],
            "timeout_minutes": s["timeout_minutes"],
            "shell_command": s["shell_command"],
            "result_parser": s["result_parser"],
        }
        for s in conn.execute(
            "SELECT * FROM stage_templates WHERE pipeline_id = ? ORDER BY stage_order",
            (pipeline_id,),
        ).fetchall()
    ]
    loops = [
        {
            "id": lp["id"],
            "name": lp["name"],
            "start_stage": lp["start_stage"],
            "end_stage": lp["end_stage"],
            "max_iterations": lp["max_iterations"],
            "config_key": lp["config_key"],
            "exit_condition": lp["exit_condition"],
            "on_failure_stage": lp["on_failure_stage"],
        }
        for lp in conn.execute(
            "SELECT * FROM stage_loops WHERE pipeline_id = ?", (pipeline_id,)
        ).fetchall()
    ]
    raw_mcp = row["mcp_servers"]
    mcp_servers = json.loads(raw_mcp) if raw_mcp else None
    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"],
        "ticket_label": row["ticket_label"],
        "is_default": bool(row["is_default"]),
        "mcp_servers": mcp_servers,
        "stages": stages,
        "loops": loops,
    }


@router.get("/api/workflow/pipelines")
def api_list_pipelines(request: Request):
    conn = None
    try:
        conn = init_db(request.app.state.db_path)
        rows = conn.execute(
            "SELECT id FROM pipeline_templates ORDER BY is_default DESC, name"
        ).fetchall()
        pipelines = []
        for r in rows:
            p = _pipeline_to_dict(conn, r["id"])
            if p:
                pipelines.append(p)
        return JSONResponse({"ok": True, "data": pipelines})
    except Exception as exc:
        logger.exception("Failed to list pipelines")
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.get("/api/workflow/pipelines/{pipeline_id}")
def api_get_pipeline(request: Request, pipeline_id: int):
    conn = None
    try:
        conn = init_db(request.app.state.db_path)
        data = _pipeline_to_dict(conn, pipeline_id)
        if data is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Pipeline {pipeline_id} not found"]},
                status_code=404,
            )
        return JSONResponse({"ok": True, "data": data})
    except Exception as exc:
        logger.exception("Failed to get pipeline %s", pipeline_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.post("/api/workflow/pipelines")
async def api_create_pipeline(request: Request):
    conn = None
    try:
        body = await request.json()
        conn = init_db(request.app.state.db_path)
        new_id = create_pipeline(
            conn,
            name=body.get("name", ""),
            description=body.get("description"),
            ticket_label=body.get("ticket_label"),
            is_default=body.get("is_default", False),
            mcp_servers=body.get("mcp_servers"),
        )
        data = _pipeline_to_dict(conn, new_id)
        return JSONResponse({"ok": True, "data": data})
    except Exception as exc:
        logger.exception("Failed to create pipeline")
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.patch("/api/workflow/pipelines/{pipeline_id}")
async def api_update_pipeline(request: Request, pipeline_id: int):
    conn = None
    try:
        body = await request.json()
        conn = init_db(request.app.state.db_path)
        row = conn.execute(
            "SELECT id FROM pipeline_templates WHERE id = ?", (pipeline_id,)
        ).fetchone()
        if row is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Pipeline {pipeline_id} not found"]},
                status_code=404,
            )
        update_pipeline(conn, pipeline_id, **body)
        warnings = validate_pipeline(conn, pipeline_id)
        data = _pipeline_to_dict(conn, pipeline_id)
        resp: dict = {"ok": True, "data": data}
        if warnings:
            resp["warnings"] = warnings
        return JSONResponse(resp)
    except ValueError as exc:
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=400)
    except Exception as exc:
        logger.exception("Failed to update pipeline %s", pipeline_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.delete("/api/workflow/pipelines/{pipeline_id}")
def api_delete_pipeline(request: Request, pipeline_id: int):
    conn = None
    try:
        conn = init_db(request.app.state.db_path)
        delete_pipeline(conn, pipeline_id)
        return JSONResponse({"ok": True, "data": None})
    except ValueError as exc:
        status = 404 if "not found" in str(exc) else 400
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=status)
    except Exception as exc:
        logger.exception("Failed to delete pipeline %s", pipeline_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.post("/api/workflow/pipelines/{pipeline_id}/duplicate")
async def api_duplicate_pipeline(request: Request, pipeline_id: int):
    conn = None
    try:
        body = await request.json()
        new_name = body.get("name", "")
        if not new_name:
            return JSONResponse(
                {"ok": False, "errors": ["name is required"]}, status_code=400
            )
        conn = init_db(request.app.state.db_path)
        new_id = duplicate_pipeline(conn, pipeline_id, new_name)
        data = _pipeline_to_dict(conn, new_id)
        return JSONResponse({"ok": True, "data": data})
    except ValueError as exc:
        status = 404 if "not found" in str(exc) else 400
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=status)
    except sqlite3.IntegrityError as exc:
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=400)
    except Exception as exc:
        logger.exception("Failed to duplicate pipeline %s", pipeline_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


# -- Stage endpoints --

@router.post("/api/workflow/pipelines/{pipeline_id}/stages")
async def api_create_stage(request: Request, pipeline_id: int):
    conn = None
    try:
        body = await request.json()
        conn = init_db(request.app.state.db_path)
        row = conn.execute(
            "SELECT id FROM pipeline_templates WHERE id = ?", (pipeline_id,)
        ).fetchone()
        if row is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Pipeline {pipeline_id} not found"]},
                status_code=404,
            )
        new_id = create_stage(
            conn,
            pipeline_id=pipeline_id,
            name=body.get("name", ""),
            stage_order=body.get("stage_order", 1),
            executor_type=body.get("executor_type", ""),
            identity=body.get("identity"),
            prompt_template=body.get("prompt_template"),
            max_turns=body.get("max_turns"),
            timeout_minutes=body.get("timeout_minutes"),
            shell_command=body.get("shell_command"),
            result_parser=body.get("result_parser"),
        )
        errors = validate_pipeline(conn, pipeline_id)
        if errors:
            delete_stage(conn, new_id)
            return JSONResponse({"ok": False, "errors": errors}, status_code=400)
        stage_row = conn.execute(
            "SELECT * FROM stage_templates WHERE id = ?", (new_id,)
        ).fetchone()
        data = {
            "id": stage_row["id"],
            "name": stage_row["name"],
            "stage_order": stage_row["stage_order"],
            "executor_type": stage_row["executor_type"],
            "identity": stage_row["identity"],
            "prompt_template": stage_row["prompt_template"],
            "max_turns": stage_row["max_turns"],
            "timeout_minutes": stage_row["timeout_minutes"],
            "shell_command": stage_row["shell_command"],
            "result_parser": stage_row["result_parser"],
        }
        return JSONResponse({"ok": True, "data": data})
    except Exception as exc:
        logger.exception("Failed to create stage for pipeline %s", pipeline_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.patch("/api/workflow/stages/{stage_id}")
async def api_update_stage(request: Request, stage_id: int):
    conn = None
    try:
        body = await request.json()
        conn = init_db(request.app.state.db_path)
        row = conn.execute(
            "SELECT pipeline_id FROM stage_templates WHERE id = ?", (stage_id,)
        ).fetchone()
        if row is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Stage {stage_id} not found"]},
                status_code=404,
            )
        pipeline_id = row["pipeline_id"]
        update_stage(conn, stage_id, **body)
        warnings = validate_pipeline(conn, pipeline_id)
        stage_row = conn.execute(
            "SELECT * FROM stage_templates WHERE id = ?", (stage_id,)
        ).fetchone()
        data = {
            "id": stage_row["id"],
            "name": stage_row["name"],
            "stage_order": stage_row["stage_order"],
            "executor_type": stage_row["executor_type"],
            "identity": stage_row["identity"],
            "prompt_template": stage_row["prompt_template"],
            "max_turns": stage_row["max_turns"],
            "timeout_minutes": stage_row["timeout_minutes"],
            "shell_command": stage_row["shell_command"],
            "result_parser": stage_row["result_parser"],
        }
        resp: dict = {"ok": True, "data": data}
        if warnings:
            resp["warnings"] = warnings
        return JSONResponse(resp)
    except ValueError as exc:
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=400)
    except Exception as exc:
        logger.exception("Failed to update stage %s", stage_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.delete("/api/workflow/stages/{stage_id}")
def api_delete_stage(request: Request, stage_id: int):
    conn = None
    try:
        conn = init_db(request.app.state.db_path)
        row = conn.execute(
            "SELECT pipeline_id FROM stage_templates WHERE id = ?", (stage_id,)
        ).fetchone()
        if row is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Stage {stage_id} not found"]},
                status_code=404,
            )
        delete_stage(conn, stage_id)
        return JSONResponse({"ok": True, "data": None})
    except ValueError as exc:
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=400)
    except Exception as exc:
        logger.exception("Failed to delete stage %s", stage_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.put("/api/workflow/pipelines/{pipeline_id}/stages/order")
async def api_reorder_stages(request: Request, pipeline_id: int):
    conn = None
    try:
        body = await request.json()
        stage_ids = body.get("stage_ids")
        if not isinstance(stage_ids, list):
            return JSONResponse(
                {"ok": False, "errors": ["stage_ids must be a list"]},
                status_code=400,
            )
        conn = init_db(request.app.state.db_path)
        row = conn.execute(
            "SELECT id FROM pipeline_templates WHERE id = ?", (pipeline_id,)
        ).fetchone()
        if row is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Pipeline {pipeline_id} not found"]},
                status_code=404,
            )
        reorder_stages(conn, pipeline_id, stage_ids)
        warnings = validate_pipeline(conn, pipeline_id)
        data = _pipeline_to_dict(conn, pipeline_id)
        resp: dict = {"ok": True, "data": data}
        if warnings:
            resp["warnings"] = warnings
        return JSONResponse(resp)
    except ValueError as exc:
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=400)
    except Exception as exc:
        logger.exception("Failed to reorder stages for pipeline %s", pipeline_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


# -- Loop endpoints --

@router.post("/api/workflow/pipelines/{pipeline_id}/loops")
async def api_create_loop(request: Request, pipeline_id: int):
    conn = None
    try:
        body = await request.json()
        conn = init_db(request.app.state.db_path)
        row = conn.execute(
            "SELECT id FROM pipeline_templates WHERE id = ?", (pipeline_id,)
        ).fetchone()
        if row is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Pipeline {pipeline_id} not found"]},
                status_code=404,
            )
        new_id = create_loop(
            conn,
            pipeline_id=pipeline_id,
            name=body.get("name", ""),
            start_stage=body.get("start_stage", ""),
            end_stage=body.get("end_stage", ""),
            max_iterations=body.get("max_iterations", 1),
            config_key=body.get("config_key"),
            exit_condition=body.get("exit_condition"),
            on_failure_stage=body.get("on_failure_stage"),
        )
        errors = validate_pipeline(conn, pipeline_id)
        if errors:
            conn.execute("DELETE FROM stage_loops WHERE id = ?", (new_id,))
            conn.commit()
            return JSONResponse({"ok": False, "errors": errors}, status_code=400)
        loop_row = conn.execute(
            "SELECT * FROM stage_loops WHERE id = ?", (new_id,)
        ).fetchone()
        data = {
            "id": loop_row["id"],
            "name": loop_row["name"],
            "start_stage": loop_row["start_stage"],
            "end_stage": loop_row["end_stage"],
            "max_iterations": loop_row["max_iterations"],
            "config_key": loop_row["config_key"],
            "exit_condition": loop_row["exit_condition"],
            "on_failure_stage": loop_row["on_failure_stage"],
        }
        return JSONResponse({"ok": True, "data": data})
    except Exception as exc:
        logger.exception("Failed to create loop for pipeline %s", pipeline_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.patch("/api/workflow/loops/{loop_id}")
async def api_update_loop(request: Request, loop_id: int):
    conn = None
    try:
        body = await request.json()
        conn = init_db(request.app.state.db_path)
        row = conn.execute(
            "SELECT pipeline_id FROM stage_loops WHERE id = ?", (loop_id,)
        ).fetchone()
        if row is None:
            return JSONResponse(
                {"ok": False, "errors": [f"Loop {loop_id} not found"]},
                status_code=404,
            )
        pipeline_id = row["pipeline_id"]
        update_loop(conn, loop_id, **body)
        warnings = validate_pipeline(conn, pipeline_id)
        loop_row = conn.execute(
            "SELECT * FROM stage_loops WHERE id = ?", (loop_id,)
        ).fetchone()
        data = {
            "id": loop_row["id"],
            "name": loop_row["name"],
            "start_stage": loop_row["start_stage"],
            "end_stage": loop_row["end_stage"],
            "max_iterations": loop_row["max_iterations"],
            "config_key": loop_row["config_key"],
            "exit_condition": loop_row["exit_condition"],
            "on_failure_stage": loop_row["on_failure_stage"],
        }
        resp: dict = {"ok": True, "data": data}
        if warnings:
            resp["warnings"] = warnings
        return JSONResponse(resp)
    except ValueError as exc:
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=400)
    except Exception as exc:
        logger.exception("Failed to update loop %s", loop_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


@router.delete("/api/workflow/loops/{loop_id}")
def api_delete_loop(request: Request, loop_id: int):
    conn = None
    try:
        conn = init_db(request.app.state.db_path)
        delete_loop(conn, loop_id)
        return JSONResponse({"ok": True, "data": None})
    except ValueError as exc:
        status = 404 if "not found" in str(exc).lower() else 400
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=status)
    except Exception as exc:
        logger.exception("Failed to delete loop %s", loop_id)
        return JSONResponse({"ok": False, "errors": [str(exc)]}, status_code=500)
    finally:
        if conn is not None:
            conn.close()


# --- Cleanup UI ---

def _get_cleanup_service(
    app,
    conn: sqlite3.Connection,
    *,
    min_age_days: int = 7,
) -> CleanupService | None:
    """Build a CleanupService from the current config, or None."""
    cfg = app.state.botfarm_config
    if cfg is None or not cfg.bugtracker.api_key:
        return None
    if not cfg.projects:
        return None
    client = create_client(cfg)
    team_key = cfg.projects[0].team
    project_name = cfg.projects[0].tracker_project
    return CleanupService(
        client, conn, team_key=team_key, project_name=project_name,
        min_age_days=min_age_days,
    )


def _cleanup_cooldown_remaining(conn: sqlite3.Connection) -> float:
    """Return seconds remaining in cooldown, or 0 if ready."""
    last_time = get_last_cleanup_batch_time(conn)
    if last_time is None:
        return 0
    last = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
    elapsed_secs = (datetime.now(timezone.utc) - last).total_seconds()
    remaining = CleanupService.COOLDOWN_SECONDS - elapsed_secs
    return max(0, remaining)


@router.get("/cleanup", response_class=HTMLResponse)
def cleanup_page(request: Request):
    app = request.app
    templates = request.app.state.templates
    conn = get_db(app)
    batches = []
    cooldown = 0.0
    has_config = False
    if conn is not None:
        try:
            batches = [dict(row) for row in list_cleanup_batches(conn)]
            cooldown = _cleanup_cooldown_remaining(conn)
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    cfg = app.state.botfarm_config
    if cfg and cfg.bugtracker.api_key:
        has_config = True
    state = read_state(app)
    return templates.TemplateResponse(request, "cleanup.html", {
        "batches": batches,
        "cooldown_seconds": cooldown,
        "has_config": has_config,
        "capacity": get_capacity_data(app),
        "elapsed": elapsed,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/api/cleanup/preview")
async def api_cleanup_preview(
    request: Request,
    limit: int = 50,
    min_age_days: int = 7,
):
    """Fetch candidate issues for cleanup preview."""
    app = request.app

    def _fetch():
        conn = init_db(app.state.db_path)
        try:
            svc = _get_cleanup_service(app, conn, min_age_days=min_age_days)
            if svc is None:
                return None
            return svc.fetch_candidates(limit=limit)
        finally:
            conn.close()

    try:
        candidates = await asyncio.to_thread(_fetch)
        if candidates is None:
            return JSONResponse(
                {"error": "Linear API key not configured"},
                status_code=503,
            )
        return JSONResponse({
            "candidates": [
                {
                    "linear_uuid": c.linear_uuid,
                    "identifier": c.identifier,
                    "title": c.title,
                    "updated_at": c.updated_at,
                    "completed_at": c.completed_at,
                    "labels": c.labels,
                }
                for c in candidates
            ],
            "total": len(candidates),
        })
    except Exception as exc:
        logger.warning("Cleanup preview failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.post("/api/cleanup/execute")
async def api_cleanup_execute(request: Request):
    """Execute a bulk cleanup operation."""
    app = request.app
    try:
        body = await request.json()
    except Exception:
        body = {}

    action = body.get("action", "archive")
    limit = body.get("limit", 50)
    min_age_days = body.get("min_age_days", 7)
    selected_ids = body.get("selected_ids")

    if action not in ("archive", "delete"):
        return JSONResponse(
            {"error": "Invalid action (must be 'archive' or 'delete')"},
            status_code=400,
        )

    def _run_cleanup_in_thread():
        conn = None
        try:
            conn = init_db(app.state.db_path)
            svc = _get_cleanup_service(app, conn, min_age_days=min_age_days)
            if svc is None:
                return None, "no_config"
            result = svc.run_cleanup(
                action=action,
                limit=limit,
                issue_ids=selected_ids if isinstance(selected_ids, list) else None,
            )
            return result, None
        finally:
            if conn is not None:
                conn.close()

    try:
        result, err = await asyncio.to_thread(_run_cleanup_in_thread)
        if err == "no_config":
            return JSONResponse(
                {"error": "Linear API key not configured"},
                status_code=503,
            )
        return JSONResponse({
            "batch_id": result.batch_id,
            "action": result.action,
            "total": result.total_candidates,
            "succeeded": result.succeeded,
            "failed": result.failed,
            "skipped": result.skipped,
            "errors": result.errors,
        })
    except CooldownError as exc:
        return JSONResponse({"error": str(exc)}, status_code=429)
    except Exception as exc:
        logger.warning("Cleanup execute failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.post("/api/cleanup/undo/{batch_id}")
async def api_cleanup_undo(request: Request, batch_id: str):
    """Undo an archive batch."""
    app = request.app

    def _undo():
        conn = init_db(app.state.db_path)
        try:
            svc = _get_cleanup_service(app, conn)
            if svc is None:
                return None
            return svc.undo_batch(batch_id)
        finally:
            conn.close()

    try:
        result = await asyncio.to_thread(_undo)
        if result is None:
            return JSONResponse(
                {"error": "Linear API key not configured"},
                status_code=503,
            )
        return JSONResponse({
            "batch_id": result.batch_id,
            "total": result.total,
            "succeeded": result.succeeded,
            "failed": result.failed,
            "errors": result.errors,
        })
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception as exc:
        logger.warning("Cleanup undo failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.get("/api/cleanup/batch/{batch_id}")
def api_cleanup_batch_detail(request: Request, batch_id: str):
    """Get details of a specific cleanup batch."""
    conn = get_db(request.app)
    if conn is None:
        return JSONResponse({"error": "Database not available"}, status_code=503)
    try:
        batch = get_cleanup_batch(conn, batch_id)
        if batch is None:
            return JSONResponse({"error": "Batch not found"}, status_code=404)
        items = get_cleanup_batch_items(conn, batch_id)
        return JSONResponse({
            "batch": dict(batch),
            "items": [dict(item) for item in items],
        })
    except sqlite3.OperationalError as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    finally:
        conn.close()
