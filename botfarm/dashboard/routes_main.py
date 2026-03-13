"""Main page routes: index, history, tickets, task_detail, usage, metrics, workflow."""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from botfarm.db import (
    count_tasks,
    count_ticket_history,
    get_distinct_projects,
    get_distinct_ticket_projects,
    get_distinct_ticket_statuses,
    get_downsampled_codex_usage_snapshots,
    get_downsampled_usage_snapshots,
    get_events,
    get_latest_context_fill_by_ticket,
    get_stage_run_aggregates,
    get_stage_runs,
    get_task,
    get_task_by_ticket,
    get_task_history,
    get_ticket_history_entry,
    get_ticket_history_list,
)
from botfarm.worker import STAGES
from botfarm.workflow import load_all_pipelines, resolve_max_iterations

from .formatters import build_pipeline_state, review_display_status
from .state import (
    context_fill_class,
    elapsed,
    format_duration,
    get_capacity_data,
    get_db,
    linear_url,
    manual_pause_state,
    read_state,
    refresh_and_get_usage,
    supervisor_status,
    usage_is_stale,
)

logger = logging.getLogger(__name__)

router = APIRouter()


_EMPTY_TASK_AGGREGATES: dict = {
    "total_input_tokens": 0,
    "total_output_tokens": 0,
    "total_cost_usd": 0.0,
    "max_context_fill_pct": None,
    "extra_usage_cost_usd": 0.0,
}


def _enrich_slots_with_context_fill(app, slots: list[dict]) -> list[dict]:
    """Attach latest context_fill_pct to busy slots from the DB."""
    busy_tickets = [
        s["ticket_id"] for s in slots
        if s.get("status") == "busy" and s.get("ticket_id")
    ]
    if not busy_tickets:
        return slots
    conn = get_db(app)
    if not conn:
        return slots
    try:
        fills = get_latest_context_fill_by_ticket(conn, busy_tickets)
        for slot in slots:
            tid = slot.get("ticket_id")
            if tid and tid in fills:
                slot["context_fill_pct"] = fills[tid]
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()
    return slots


def _compute_slot_pipeline(slot: dict) -> list[dict]:
    """Compute compact pipeline visualization state for a slot row."""
    completed = set(slot.get("stages_completed", []))
    current = slot.get("stage")
    # resolve_conflict is a loop-internal sub-stage of merge; show merge
    # as active when the worker is resolving conflicts.
    if current == "resolve_conflict":
        current = "merge"
    is_failed = slot.get("status") == "failed"

    pipeline = []
    prev_completed = False
    for stage_name in STAGES:
        if stage_name in completed:
            state = "completed"
        elif stage_name == current:
            state = "failed" if is_failed else "active"
        else:
            state = "pending"

        connector = "completed" if prev_completed else "pending"
        pipeline.append({
            "name": stage_name,
            "state": state,
            "connector": connector,
        })
        prev_completed = (state == "completed")
    return pipeline


def _enrich_slots_with_pipeline(slots: list[dict]) -> list[dict]:
    """Add pipeline visualization data to non-free slots."""
    for slot in slots:
        if slot.get("stage") and slot.get("status") != "free":
            slot["pipeline"] = _compute_slot_pipeline(slot)
        else:
            slot["pipeline"] = []
    return slots


def _enrich_slots_with_codex_review(app, slots: list[dict]) -> list[dict]:
    """Attach Codex review status to busy slots in the review stage."""
    review_tickets = [
        s["ticket_id"] for s in slots
        if s.get("status") == "busy"
        and s.get("stage") == "review"
        and s.get("ticket_id")
    ]
    if not review_tickets:
        return slots
    conn = get_db(app)
    if not conn:
        return slots
    try:
        for slot in slots:
            tid = slot.get("ticket_id")
            if tid not in review_tickets:
                continue
            task_row = conn.execute(
                "SELECT id FROM tasks WHERE ticket_id = ? ORDER BY id DESC LIMIT 1",
                (tid,),
            ).fetchone()
            if not task_row:
                continue
            codex_row = conn.execute(
                "SELECT exit_subtype FROM stage_runs "
                "WHERE task_id = ? AND stage = 'codex_review' "
                "ORDER BY id DESC LIMIT 1",
                (task_row["id"],),
            ).fetchone()
            if codex_row:
                slot["codex_review_status"] = review_display_status(
                    codex_row["exit_subtype"]
                )
                claude_row = conn.execute(
                    "SELECT exit_subtype FROM stage_runs "
                    "WHERE task_id = ? AND stage = 'review' "
                    "ORDER BY id DESC LIMIT 1",
                    (task_row["id"],),
                ).fetchone()
                slot["claude_review_status"] = review_display_status(
                    claude_row["exit_subtype"] if claude_row else None
                )
    except sqlite3.OperationalError:
        pass
    finally:
        conn.close()
    return slots


def _enrich_tasks(
    app, tasks: list[dict], conn: sqlite3.Connection | None = None,
) -> list[dict]:
    """Add computed fields to task dicts."""
    aggregates: dict[int, dict] = {}
    if conn is not None:
        task_ids = [t["id"] for t in tasks if t.get("id") is not None]
        if task_ids:
            try:
                aggregates = get_stage_run_aggregates(conn, task_ids)
            except sqlite3.OperationalError:
                pass
    for task in tasks:
        task["duration"] = "-"
        if task.get("started_at") and task.get("completed_at"):
            try:
                start = datetime.fromisoformat(
                    task["started_at"].replace("Z", "+00:00")
                )
                end = datetime.fromisoformat(
                    task["completed_at"].replace("Z", "+00:00")
                )
                task["duration"] = format_duration(
                    int((end - start).total_seconds())
                )
            except (ValueError, TypeError):
                pass
        agg = aggregates.get(task.get("id"), _EMPTY_TASK_AGGREGATES)
        task["total_cost_usd"] = agg["total_cost_usd"]
        task["max_context_fill_pct"] = agg["max_context_fill_pct"]
        task["extra_usage_cost_usd"] = agg["extra_usage_cost_usd"]
    return tasks


PAGE_SIZE = 25


def _fetch_tasks_filtered(
    app,
    conn: sqlite3.Connection,
    *,
    project: str | None = None,
    status: str | None = None,
    search: str | None = None,
    sort_by: str = "created_at",
    sort_dir: str = "DESC",
    page: int = 1,
) -> tuple[list[dict], int, int]:
    """Fetch tasks with filters and pagination."""
    try:
        total = count_tasks(conn, project=project, status=status, search=search)
        total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = max(1, min(page, total_pages))
        offset = (page - 1) * PAGE_SIZE
        rows = get_task_history(
            conn,
            limit=PAGE_SIZE,
            offset=offset,
            project=project,
            status=status,
            search=search,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        return _enrich_tasks(app, [dict(r) for r in rows], conn), total, total_pages
    except sqlite3.OperationalError:
        return [], 0, 1


ALLOWED_SORT_COLS = {
    "ticket_id", "title", "project", "status", "turns",
    "review_iterations", "limit_interruptions", "created_at",
    "started_at", "completed_at",
}


def _extract_history_params(request: Request) -> dict:
    """Extract filter/sort/page query params from request."""
    params = request.query_params
    project = params.get("project") or None
    status = params.get("status") or None
    search = params.get("search") or None
    sort_by = params.get("sort_by", "created_at")
    if sort_by not in ALLOWED_SORT_COLS:
        sort_by = "created_at"
    sort_dir = params.get("sort_dir", "DESC")
    if sort_dir.upper() not in ("ASC", "DESC"):
        sort_dir = "DESC"
    try:
        page = int(params.get("page", "1"))
    except ValueError:
        page = 1
    return {
        "project": project,
        "status": status,
        "search": search,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "page": page,
    }


def _history_context(request: Request) -> dict:
    """Build the full template context for history views."""
    app = request.app
    hp = _extract_history_params(request)
    conn = get_db(app)
    tasks: list[dict] = []
    total = 0
    total_pages = 1
    projects: list[str] = []
    if conn:
        try:
            tasks, total, total_pages = _fetch_tasks_filtered(app, conn, **hp)
            projects = get_distinct_projects(conn)
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    page = max(1, min(hp["page"], total_pages))
    state = read_state(app)
    return {
        "request": request,
        "tasks": tasks,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "projects": projects,
        "filter_project": hp["project"] or "",
        "filter_status": hp["status"] or "",
        "filter_search": hp["search"] or "",
        "sort_by": hp["sort_by"],
        "sort_dir": hp["sort_dir"],
        "linear_url": lambda tid: linear_url(app, tid),
        "context_fill_class": context_fill_class,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    }


# --- Ticket History Browser ---

TICKET_PAGE_SIZE = 25

ALLOWED_TICKET_SORT_COLS = {
    "ticket_id", "title", "project_name", "status", "priority",
    "captured_at", "linear_created_at",
}


def _extract_ticket_params(request: Request) -> dict:
    """Extract filter/sort/page query params for ticket browser."""
    params = request.query_params
    project = params.get("project") or None
    status = params.get("status") or None
    search = params.get("search") or None
    deleted = params.get("deleted") or None
    sort_by = params.get("sort_by", "captured_at")
    if sort_by not in ALLOWED_TICKET_SORT_COLS:
        sort_by = "captured_at"
    sort_dir = params.get("sort_dir", "DESC")
    if sort_dir.upper() not in ("ASC", "DESC"):
        sort_dir = "DESC"
    try:
        page = int(params.get("page", "1"))
    except ValueError:
        page = 1
    deleted_from_linear: bool | None = None
    if deleted == "yes":
        deleted_from_linear = True
    elif deleted == "no":
        deleted_from_linear = False
    return {
        "project": project,
        "status": status,
        "search": search,
        "deleted_from_linear": deleted_from_linear,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "page": page,
        "deleted_raw": deleted or "",
    }


def _tickets_context(request: Request) -> dict:
    """Build the full template context for ticket browser views."""
    app = request.app
    tp = _extract_ticket_params(request)
    conn = get_db(app)
    tickets: list[dict] = []
    total = 0
    total_pages = 1
    projects: list[str] = []
    statuses: list[str] = []
    if conn:
        try:
            filter_kwargs = {
                k: tp[k] for k in ("project", "status", "search", "deleted_from_linear")
            }
            total = count_ticket_history(conn, **filter_kwargs)
            total_pages = max(1, (total + TICKET_PAGE_SIZE - 1) // TICKET_PAGE_SIZE)
            page = max(1, min(tp["page"], total_pages))
            offset = (page - 1) * TICKET_PAGE_SIZE
            rows = get_ticket_history_list(
                conn,
                limit=TICKET_PAGE_SIZE,
                offset=offset,
                sort_by=tp["sort_by"],
                sort_dir=tp["sort_dir"],
                **filter_kwargs,
            )
            tickets = [dict(r) for r in rows]
            # Parse JSON fields for display
            for t in tickets:
                for field in ("labels", "children_ids", "blocked_by", "blocks"):
                    val = t.get(field)
                    if isinstance(val, str):
                        try:
                            t[field] = json.loads(val)
                        except (json.JSONDecodeError, ValueError):
                            t[field] = []
                    elif val is None:
                        t[field] = []
            projects = get_distinct_ticket_projects(conn)
            statuses = get_distinct_ticket_statuses(conn)
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    page = max(1, min(tp["page"], total_pages))
    state = read_state(app)
    return {
        "request": request,
        "tickets": tickets,
        "total": total,
        "page": page,
        "total_pages": total_pages,
        "projects": projects,
        "statuses": statuses,
        "filter_project": tp["project"] or "",
        "filter_status": tp["status"] or "",
        "filter_search": tp["search"] or "",
        "filter_deleted": tp["deleted_raw"],
        "sort_by": tp["sort_by"],
        "sort_dir": tp["sort_dir"],
        "linear_url": lambda tid: linear_url(app, tid),
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    }


EVENT_LOG_LIMIT = 500


def _compute_task_totals(stages: list[dict]) -> dict:
    """Aggregate token usage and cost from stage runs."""
    total_input = sum(s.get("input_tokens") or 0 for s in stages)
    total_output = sum(s.get("output_tokens") or 0 for s in stages)
    total_cost = sum(s.get("total_cost_usd") or 0.0 for s in stages)
    extra_usage_cost = sum(
        s.get("total_cost_usd") or 0.0
        for s in stages if s.get("on_extra_usage")
    )
    fills = [s["context_fill_pct"] for s in stages if s.get("context_fill_pct") is not None]

    # Codex review aggregates
    codex_stages = [s for s in stages if s.get("stage") == "codex_review"]
    codex_input = sum(s.get("input_tokens") or 0 for s in codex_stages)
    codex_output = sum(s.get("output_tokens") or 0 for s in codex_stages)
    codex_cache_read = sum(s.get("cache_read_input_tokens") or 0 for s in codex_stages)
    codex_cost_usd = sum(s.get("total_cost_usd") or 0.0 for s in codex_stages)
    codex_runs = len(codex_stages)

    return {
        "total_input_tokens": total_input,
        "total_output_tokens": total_output,
        "total_cost": total_cost,
        "extra_usage_cost": extra_usage_cost,
        "max_context_fill": max(fills) if fills else None,
        "codex_input_tokens": codex_input,
        "codex_output_tokens": codex_output,
        "codex_cache_read_tokens": codex_cache_read,
        "codex_cost_usd": codex_cost_usd,
        "codex_runs": codex_runs,
        "codex_stages": codex_stages,
    }


USAGE_RANGE_HOURS = {"24h": 24, "7d": 168, "30d": 720}
# Bucket intervals (minutes) for downsampling usage chart data.
# None = raw data (no aggregation).
USAGE_BUCKET_MINUTES: dict[str, int | None] = {"24h": None, "7d": 30, "30d": 180}

_EMPTY_METRICS: dict = {
    "total_tasks": 0, "completed_tasks": 0, "failed_tasks": 0,
    "avg_turns": 0, "avg_review_iterations": 0.0,
    "avg_wall_time_seconds": 0, "success_rate": 0.0,
    "completed_today": 0, "completed_week": 0,
    "completed_month": 0,
    "failure_reasons": [], "failure_categories": [],
    "total_input_tokens": 0, "total_output_tokens": 0,
    "total_cost_usd": 0.0, "extra_usage_cost_usd": 0.0,
    "avg_context_fill_pct": None,
    "tasks_over_80_pct_fill": 0,
    "codex_input_tokens": 0, "codex_output_tokens": 0,
    "codex_cost_usd": 0.0,
    "codex_runs": 0, "codex_approved": 0, "codex_errors": 0,
    "codex_approval_rate": 0.0, "codex_error_rate": 0.0,
}


def _compute_metrics(
    conn: sqlite3.Connection, project: str | None = None,
    failure_category: str | None = None,
) -> dict:
    """Compute all metrics, optionally filtered by project and failure category."""
    metrics: dict = {**_EMPTY_METRICS, "failure_reasons": [], "failure_categories": []}
    where = " WHERE 1=1"
    params: list[object] = []
    if project:
        where += " AND project = ?"
        params.append(project)

    # Core aggregates
    row = conn.execute(
        "SELECT COUNT(*) as total, "
        "SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed, "
        "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed "
        "FROM tasks" + where,
        params,
    ).fetchone()
    if row:
        metrics["total_tasks"] = row["total"] or 0
        metrics["completed_tasks"] = row["completed"] or 0
        metrics["failed_tasks"] = row["failed"] or 0
        if metrics["total_tasks"] > 0:
            metrics["success_rate"] = round(
                metrics["completed_tasks"] / metrics["total_tasks"] * 100, 1,
            )

    # Averages over completed tasks only
    avg_row = conn.execute(
        "SELECT COALESCE(AVG(turns), 0) as avg_turns, "
        "COALESCE(AVG(review_iterations), 0) as avg_reviews "
        "FROM tasks" + where + " AND status = 'completed'",
        params,
    ).fetchone()
    if avg_row:
        metrics["avg_turns"] = round(avg_row["avg_turns"])
        metrics["avg_review_iterations"] = round(avg_row["avg_reviews"], 1)

    # Average wall time
    wt_row = conn.execute(
        "SELECT AVG("
        "  (julianday(completed_at) - julianday(started_at)) * 86400"
        ") as avg_wt "
        "FROM tasks" + where
        + " AND started_at IS NOT NULL AND completed_at IS NOT NULL",
        params,
    ).fetchone()
    if wt_row and wt_row["avg_wt"] is not None:
        metrics["avg_wall_time_seconds"] = int(wt_row["avg_wt"])

    # Time-bucketed counts
    for label, interval in [
        ("today", "start of day"),
        ("week", "-6 days"),
        ("month", "-29 days"),
    ]:
        bucket_row = conn.execute(
            "SELECT COUNT(*) as cnt "
            "FROM tasks" + where
            + " AND status = 'completed'"
            " AND completed_at >= datetime('now', ?)",
            [*params, interval],
        ).fetchone()
        if bucket_row:
            metrics[f"completed_{label}"] = bucket_row["cnt"] or 0

    # Token usage & cost aggregates from stage_runs
    try:
        token_row = conn.execute(
            "SELECT SUM(sr.input_tokens) as total_in, "
            "SUM(sr.output_tokens) as total_out, "
            "SUM(sr.total_cost_usd) as total_cost, "
            "SUM(CASE WHEN sr.on_extra_usage THEN sr.total_cost_usd ELSE 0 END) as extra_cost, "
            "AVG(sr.context_fill_pct) as avg_fill, "
            "COUNT(DISTINCT CASE WHEN sr.context_fill_pct > 80 THEN sr.task_id END) as tasks_over_80 "
            "FROM stage_runs sr "
            "JOIN tasks t ON sr.task_id = t.id" + where,
            params,
        ).fetchone()
        if token_row:
            metrics["total_input_tokens"] = token_row["total_in"] or 0
            metrics["total_output_tokens"] = token_row["total_out"] or 0
            metrics["total_cost_usd"] = token_row["total_cost"] or 0.0
            metrics["extra_usage_cost_usd"] = token_row["extra_cost"] or 0.0
            metrics["avg_context_fill_pct"] = token_row["avg_fill"]
            metrics["tasks_over_80_pct_fill"] = token_row["tasks_over_80"] or 0
    except sqlite3.OperationalError:
        pass

    # Codex review aggregates
    try:
        codex_where = where + " AND sr.stage = 'codex_review'"
        codex_row = conn.execute(
            "SELECT SUM(sr.input_tokens) as codex_in, "
            "SUM(sr.output_tokens) as codex_out, "
            "SUM(sr.total_cost_usd) as codex_cost, "
            "COUNT(*) as codex_runs, "
            "SUM(CASE WHEN sr.exit_subtype = 'approved' THEN 1 ELSE 0 END) as codex_approved, "
            "SUM(CASE WHEN sr.exit_subtype IN ('failed', 'error', 'timeout') THEN 1 ELSE 0 END) as codex_errors "
            "FROM stage_runs sr "
            "JOIN tasks t ON sr.task_id = t.id" + codex_where,
            params,
        ).fetchone()
        if codex_row and codex_row["codex_runs"]:
            metrics["codex_input_tokens"] = codex_row["codex_in"] or 0
            metrics["codex_output_tokens"] = codex_row["codex_out"] or 0
            metrics["codex_cost_usd"] = codex_row["codex_cost"] or 0.0
            metrics["codex_runs"] = codex_row["codex_runs"] or 0
            metrics["codex_approved"] = codex_row["codex_approved"] or 0
            metrics["codex_errors"] = codex_row["codex_errors"] or 0
            total_codex = metrics["codex_runs"]
            if total_codex > 0:
                metrics["codex_approval_rate"] = round(
                    metrics["codex_approved"] / total_codex * 100, 1,
                )
                metrics["codex_error_rate"] = round(
                    metrics["codex_errors"] / total_codex * 100, 1,
                )
    except sqlite3.OperationalError:
        pass

    # Failure categories summary
    try:
        cat_rows = conn.execute(
            "SELECT COALESCE(failure_category, 'code_failure') as cat, COUNT(*) as cnt "
            "FROM tasks" + where
            + " AND failure_reason IS NOT NULL AND failure_reason != '' "
            "GROUP BY cat ORDER BY cnt DESC",
            params,
        ).fetchall()
        metrics["failure_categories"] = [
            {"category": r["cat"], "count": r["cnt"]}
            for r in cat_rows
        ]
    except sqlite3.OperationalError:
        pass

    # Most common failure reasons (optionally filtered by category)
    reason_where = where + " AND failure_reason IS NOT NULL AND failure_reason != '' "
    reason_params = list(params)
    if failure_category:
        reason_where += " AND COALESCE(failure_category, 'code_failure') = ? "
        reason_params.append(failure_category)
    reason_rows = conn.execute(
        "SELECT failure_reason, COALESCE(failure_category, 'code_failure') as cat, COUNT(*) as cnt "
        "FROM tasks" + reason_where
        + "GROUP BY failure_reason, cat ORDER BY cnt DESC LIMIT 5",
        reason_params,
    ).fetchall()
    metrics["failure_reasons"] = [
        {"reason": r["failure_reason"], "category": r["cat"], "count": r["cnt"]}
        for r in reason_rows
    ]

    return metrics


# --- Route handlers ---


@router.get("/", response_class=HTMLResponse)
def index(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    slots = _enrich_slots_with_context_fill(app, state.get("slots", []))
    slots = _enrich_slots_with_pipeline(slots)
    slots = _enrich_slots_with_codex_review(app, slots)
    dispatch_paused = state.get("dispatch_paused", False)
    dispatch_pause_reason = state.get("dispatch_pause_reason")
    usage = state.get("usage", {})
    codex_usage = state.get("codex_usage", {})
    queue = state.get("queue")
    last_usage_check = state.get("last_usage_check")
    _linear_url = lambda tid: linear_url(app, tid)
    cfg = app.state.botfarm_config
    projects = [p.name for p in cfg.projects] if cfg else []
    return templates.TemplateResponse("index.html", {
        "request": request,
        "slots": slots,
        "dispatch_paused": dispatch_paused,
        "dispatch_pause_reason": dispatch_pause_reason,
        "usage": usage,
        "codex_usage": codex_usage,
        "queue": queue,
        "projects": projects,
        "last_usage_check": last_usage_check,
        "usage_stale": usage_is_stale(last_usage_check),
        "elapsed": elapsed,
        "linear_url": _linear_url,
        "context_fill_class": context_fill_class,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
        "has_callbacks": app.state.on_pause is not None,
        "capacity": get_capacity_data(app),
    })


@router.get("/history", response_class=HTMLResponse)
def history_page(request: Request):
    templates = request.app.state.templates
    ctx = _history_context(request)
    return templates.TemplateResponse("history.html", ctx)


@router.get("/tickets", response_class=HTMLResponse)
def tickets_page(request: Request):
    templates = request.app.state.templates
    ctx = _tickets_context(request)
    return templates.TemplateResponse("tickets.html", ctx)


@router.get("/tickets/{ticket_id}", response_class=HTMLResponse)
def ticket_detail_page(request: Request, ticket_id: str):
    app = request.app
    templates = request.app.state.templates
    ticket = None
    task = None
    conn = get_db(app)
    if conn:
        try:
            row = get_ticket_history_entry(conn, ticket_id)
            if row:
                ticket = dict(row)
                for field in ("labels", "children_ids", "blocked_by", "blocks", "comments_json"):
                    val = ticket.get(field)
                    if isinstance(val, str):
                        try:
                            ticket[field] = json.loads(val)
                        except (json.JSONDecodeError, ValueError):
                            ticket[field] = []
                    elif val is None:
                        ticket[field] = []
            task_row = get_task_by_ticket(conn, ticket_id)
            if task_row:
                task = dict(task_row)
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    state = read_state(app)
    return templates.TemplateResponse("ticket_detail.html", {
        "request": request,
        "ticket": ticket,
        "task": task,
        "linear_url": lambda tid: linear_url(app, tid),
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/task/{task_id}", response_class=HTMLResponse)
def task_detail_page(request: Request, task_id: str):
    app = request.app
    templates = request.app.state.templates
    task = None
    stages: list[dict] = []
    events: list[dict] = []
    pipeline: list[dict] = []
    ticket_content = None
    conn = get_db(app)
    if conn:
        try:
            task_row = None
            try:
                int_id = int(task_id)
                task_row = get_task(conn, int_id)
            except ValueError:
                pass
            if task_row is None:
                task_row = get_task_by_ticket(conn, task_id)
            if task_row is not None:
                task = _enrich_tasks(app, [dict(task_row)])[0]
                db_task_id = task["id"]
                stages = [dict(r) for r in get_stage_runs(conn, db_task_id)]
                events = [dict(r) for r in get_events(
                    conn, task_id=db_task_id, limit=EVENT_LOG_LIMIT,
                )]
                events.reverse()
                pipeline = build_pipeline_state(
                    stages, task.get("status"), STAGES,
                )
                try:
                    th_row = get_ticket_history_entry(conn, task["ticket_id"])
                    if th_row:
                        ticket_content = dict(th_row)
                except sqlite3.OperationalError:
                    pass
        finally:
            conn.close()
    task_totals = _compute_task_totals(stages)
    state = read_state(app)
    return templates.TemplateResponse("task_detail.html", {
        "request": request,
        "task": task,
        "stages": stages,
        "events": events,
        "pipeline": pipeline,
        "task_totals": task_totals,
        "ticket_content": ticket_content,
        "linear_url": lambda tid: linear_url(app, tid),
        "format_duration": format_duration,
        "context_fill_class": context_fill_class,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/usage", response_class=HTMLResponse)
def usage_page(request: Request):
    app = request.app
    templates = request.app.state.templates
    state = read_state(app)
    fresh, _snapshot_at = refresh_and_get_usage(app)
    usage = fresh if fresh is not None else state.get("usage", {})
    time_range = request.query_params.get("range", "7d")
    if time_range not in USAGE_RANGE_HOURS:
        time_range = "7d"
    hours = USAGE_RANGE_HOURS[time_range]
    bucket_minutes = USAGE_BUCKET_MINUTES[time_range]
    snapshots = []
    codex_snapshots = []
    codex_stage_cost = 0.0
    conn = get_db(app)
    if conn:
        try:
            try:
                snapshots = get_downsampled_usage_snapshots(
                    conn, hours=hours, bucket_minutes=bucket_minutes,
                )
            except sqlite3.OperationalError:
                pass
            try:
                codex_snapshots = get_downsampled_codex_usage_snapshots(
                    conn, hours=hours, bucket_minutes=bucket_minutes,
                )
            except sqlite3.OperationalError:
                pass
            try:
                cost_row = conn.execute(
                    "SELECT SUM(total_cost_usd) as total "
                    "FROM stage_runs WHERE stage = 'codex_review'"
                ).fetchone()
                if cost_row and cost_row["total"]:
                    codex_stage_cost = cost_row["total"]
            except sqlite3.OperationalError:
                pass
        finally:
            conn.close()
    return templates.TemplateResponse("usage.html", {
        "request": request,
        "usage": usage,
        "snapshots": snapshots,
        "codex_snapshots": codex_snapshots,
        "codex_stage_cost": codex_stage_cost,
        "time_range": time_range,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/metrics", response_class=HTMLResponse)
def metrics_page(request: Request):
    app = request.app
    templates = request.app.state.templates
    filter_project = request.query_params.get("project") or ""
    filter_failure_category = request.query_params.get("failure_category") or ""
    conn = get_db(app)
    metrics = dict(_EMPTY_METRICS)
    projects: list[str] = []
    if conn:
        try:
            metrics = _compute_metrics(
                conn,
                project=filter_project or None,
                failure_category=filter_failure_category or None,
            )
            projects = get_distinct_projects(conn)
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    state = read_state(app)
    return templates.TemplateResponse("metrics.html", {
        "request": request,
        "metrics": metrics,
        "projects": projects,
        "filter_project": filter_project,
        "filter_failure_category": filter_failure_category,
        "format_duration": format_duration,
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })


@router.get("/workflow", response_class=HTMLResponse)
def workflow_page(request: Request):
    app = request.app
    templates = request.app.state.templates
    conn = get_db(app)
    pipelines_data: list[dict] = []
    if conn:
        try:
            pipelines = load_all_pipelines(conn)
            agents_cfg = (
                app.state.botfarm_config.agents
                if app.state.botfarm_config
                else None
            )
            for pipeline in pipelines:
                loop_managed: set[str] = set()
                for loop in pipeline.loops:
                    if loop.on_failure_stage:
                        loop_managed.add(loop.start_stage)
                    else:
                        loop_managed.add(loop.end_stage)

                main_stages = [
                    s for s in pipeline.stages
                    if s.name not in loop_managed
                ]

                stages_list = [
                    {
                        "id": s.id,
                        "name": s.name,
                        "executor_type": s.executor_type,
                        "identity": s.identity,
                        "prompt_template": s.prompt_template,
                        "timeout_minutes": s.timeout_minutes,
                        "max_turns": s.max_turns,
                        "shell_command": s.shell_command,
                        "result_parser": s.result_parser,
                    }
                    for s in pipeline.stages
                ]

                main_stages_list = [
                    {
                        "id": s.id,
                        "name": s.name,
                        "executor_type": s.executor_type,
                        "identity": s.identity,
                        "prompt_template": s.prompt_template,
                        "timeout_minutes": s.timeout_minutes,
                        "max_turns": s.max_turns,
                        "shell_command": s.shell_command,
                        "result_parser": s.result_parser,
                    }
                    for s in main_stages
                ]

                loops_list = []
                for loop in pipeline.loops:
                    eff_max = (
                        resolve_max_iterations(loop, agents_cfg)
                        if agents_cfg
                        else loop.max_iterations
                    )
                    if loop.on_failure_stage:
                        decision_stage = loop.end_stage
                        fix_stage_name = loop.start_stage
                    else:
                        decision_stage = loop.start_stage
                        fix_stage_name = loop.end_stage

                    fix_stage_obj = next(
                        (s for s in pipeline.stages if s.name == fix_stage_name),
                        None,
                    )

                    condition = loop.exit_condition or ""
                    if "review" in condition:
                        question = "Approved?"
                    elif "ci" in condition:
                        question = "CI passed?"
                    else:
                        question = "Continue?"

                    loops_list.append({
                        "id": loop.id,
                        "name": loop.name,
                        "start_stage": loop.start_stage,
                        "end_stage": loop.end_stage,
                        "config_key": loop.config_key,
                        "on_failure_stage": loop.on_failure_stage,
                        "decision_stage": decision_stage,
                        "fix_stage_name": fix_stage_name,
                        "fix_stage": {
                            "id": fix_stage_obj.id,
                            "name": fix_stage_obj.name,
                            "executor_type": fix_stage_obj.executor_type,
                            "identity": fix_stage_obj.identity,
                            "prompt_template": fix_stage_obj.prompt_template,
                            "timeout_minutes": fix_stage_obj.timeout_minutes,
                            "max_turns": fix_stage_obj.max_turns,
                            "shell_command": fix_stage_obj.shell_command,
                            "result_parser": fix_stage_obj.result_parser,
                        } if fix_stage_obj else None,
                        "max_iterations": eff_max,
                        "raw_max_iterations": loop.max_iterations,
                        "question": question,
                        "exit_condition": loop.exit_condition,
                    })

                pipelines_data.append({
                    "id": pipeline.id,
                    "name": pipeline.name,
                    "description": pipeline.description,
                    "is_default": pipeline.is_default,
                    "ticket_label": pipeline.ticket_label,
                    "stages": stages_list,
                    "main_stages": main_stages_list,
                    "loops": loops_list,
                })
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
    state = read_state(app)
    return templates.TemplateResponse("workflow.html", {
        "request": request,
        "pipelines": pipelines_data,
        "active_page": "workflow",
        "supervisor": supervisor_status(app, state),
        "pause_state": manual_pause_state(state),
    })
