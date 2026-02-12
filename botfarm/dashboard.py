"""Web dashboard for botfarm — FastAPI + Jinja2 + htmx.

Serves a lightweight server-rendered dashboard that auto-refreshes via htmx
polling. Designed to run inside the supervisor process as a background thread.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from botfarm.config import DashboardConfig
from botfarm.db import (
    count_tasks,
    get_distinct_projects,
    get_events,
    get_stage_runs,
    get_task,
    get_task_history,
)

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


def create_app(
    *,
    state_file: str | Path,
    db_path: str | Path,
    linear_workspace: str = "",
) -> FastAPI:
    """Create the FastAPI dashboard application.

    Parameters
    ----------
    state_file:
        Path to the supervisor state.json file.
    db_path:
        Path to the SQLite database.
    linear_workspace:
        Linear workspace slug used for building ticket URLs.
    """
    app = FastAPI(title="Botfarm Dashboard", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # Store paths on app state for route handlers
    app.state.state_file = Path(state_file).expanduser()
    app.state.db_path = Path(db_path).expanduser()
    app.state.linear_workspace = linear_workspace

    # --- Helpers ---

    def _read_state() -> dict:
        """Read the supervisor state.json file."""
        try:
            data = json.loads(app.state.state_file.read_text())
            return data if isinstance(data, dict) else {"slots": data}
        except (json.JSONDecodeError, OSError):
            return {}

    def _get_db() -> sqlite3.Connection | None:
        """Open a read-only database connection.

        Note: the exists() check is intentional here — unlike file reads,
        sqlite3.connect() creates the file if missing rather than raising.
        """
        if not app.state.db_path.exists():
            return None
        try:
            conn = sqlite3.connect(str(app.state.db_path))
            conn.row_factory = sqlite3.Row
            return conn
        except sqlite3.Error:
            return None

    def _format_duration(total_seconds: int) -> str:
        if total_seconds < 0:
            return "-"
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours}h{minutes:02d}m"
        if minutes:
            return f"{minutes}m{seconds:02d}s"
        return f"{seconds}s"

    def _elapsed(started_at: str | None) -> str:
        if not started_at:
            return "-"
        try:
            start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
            delta = datetime.now(timezone.utc) - start
            return _format_duration(int(delta.total_seconds()))
        except (ValueError, TypeError):
            return "-"

    def _linear_url(ticket_id: str) -> str:
        """Build a Linear issue URL from a ticket identifier."""
        ws = app.state.linear_workspace
        if ws:
            return f"https://linear.app/{ws}/issue/{ticket_id}"
        return f"https://linear.app/issue/{ticket_id}"

    # Make helpers available to templates
    @app.middleware("http")
    async def add_template_globals(request: Request, call_next):
        request.state.elapsed = _elapsed
        request.state.format_duration = _format_duration
        return await call_next(request)

    # --- Routes ---

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        state = _read_state()
        slots = state.get("slots", [])
        dispatch_paused = state.get("dispatch_paused", False)
        dispatch_pause_reason = state.get("dispatch_pause_reason")
        usage = state.get("usage", {})
        queue = state.get("queue")
        last_usage_check = state.get("last_usage_check")
        return templates.TemplateResponse("index.html", {
            "request": request,
            "slots": slots,
            "dispatch_paused": dispatch_paused,
            "dispatch_pause_reason": dispatch_pause_reason,
            "usage": usage,
            "queue": queue,
            "last_usage_check": last_usage_check,
            "elapsed": _elapsed,
            "linear_url": _linear_url,
        })

    @app.get("/partials/slots", response_class=HTMLResponse)
    def partial_slots(request: Request):
        state = _read_state()
        slots = state.get("slots", [])
        dispatch_paused = state.get("dispatch_paused", False)
        dispatch_pause_reason = state.get("dispatch_pause_reason")
        return templates.TemplateResponse("partials/slots.html", {
            "request": request,
            "slots": slots,
            "dispatch_paused": dispatch_paused,
            "dispatch_pause_reason": dispatch_pause_reason,
            "elapsed": _elapsed,
            "linear_url": _linear_url,
        })

    @app.get("/partials/usage", response_class=HTMLResponse)
    def partial_usage(request: Request):
        state = _read_state()
        usage = state.get("usage", {})
        dispatch_paused = state.get("dispatch_paused", False)
        dispatch_pause_reason = state.get("dispatch_pause_reason")
        last_usage_check = state.get("last_usage_check")
        return templates.TemplateResponse("partials/usage.html", {
            "request": request,
            "usage": usage,
            "dispatch_paused": dispatch_paused,
            "dispatch_pause_reason": dispatch_pause_reason,
            "last_usage_check": last_usage_check,
            "elapsed": _elapsed,
        })

    @app.get("/partials/queue", response_class=HTMLResponse)
    def partial_queue(request: Request):
        state = _read_state()
        queue = state.get("queue")
        return templates.TemplateResponse("partials/queue.html", {
            "request": request,
            "queue": queue,
            "linear_url": _linear_url,
        })

    def _enrich_tasks(tasks: list[dict]) -> list[dict]:
        """Add computed 'duration' field to task dicts."""
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
                    task["duration"] = _format_duration(
                        int((end - start).total_seconds())
                    )
                except (ValueError, TypeError):
                    pass
        return tasks

    PAGE_SIZE = 25

    def _fetch_tasks_filtered(
        conn: sqlite3.Connection,
        *,
        project: str | None = None,
        status: str | None = None,
        search: str | None = None,
        sort_by: str = "created_at",
        sort_dir: str = "DESC",
        page: int = 1,
    ) -> tuple[list[dict], int, int]:
        """Fetch tasks with filters and pagination.

        Returns (tasks, total_count, total_pages).
        """
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
            return _enrich_tasks([dict(r) for r in rows]), total, total_pages
        except sqlite3.OperationalError:
            return [], 0, 1

    ALLOWED_SORT_COLS = {
        "ticket_id", "title", "project", "status", "cost_usd", "turns",
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
        hp = _extract_history_params(request)
        conn = _get_db()
        tasks: list[dict] = []
        total = 0
        total_pages = 1
        projects: list[str] = []
        if conn:
            try:
                tasks, total, total_pages = _fetch_tasks_filtered(conn, **hp)
                projects = get_distinct_projects(conn)
            except sqlite3.OperationalError:
                pass
            finally:
                conn.close()
        page = max(1, min(hp["page"], total_pages))
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
            "linear_url": _linear_url,
        }

    @app.get("/history", response_class=HTMLResponse)
    def history_page(request: Request):
        ctx = _history_context(request)
        return templates.TemplateResponse("history.html", ctx)

    @app.get("/partials/history", response_class=HTMLResponse)
    def partial_history(request: Request):
        ctx = _history_context(request)
        return templates.TemplateResponse("partials/history.html", ctx)

    EVENT_LOG_LIMIT = 500

    @app.get("/task/{task_id}", response_class=HTMLResponse)
    def task_detail_page(request: Request, task_id: int):
        task = None
        stages: list[dict] = []
        events: list[dict] = []
        conn = _get_db()
        if conn:
            try:
                task_row = get_task(conn, task_id)
                if task_row is not None:
                    task = _enrich_tasks([dict(task_row)])[0]
                    stages = [dict(r) for r in get_stage_runs(conn, task_id)]
                    events = [dict(r) for r in get_events(
                        conn, task_id=task_id, limit=EVENT_LOG_LIMIT,
                    )]
                    # Events come newest-first from DB; reverse for chronological display
                    events.reverse()
            finally:
                conn.close()
        return templates.TemplateResponse("task_detail.html", {
            "request": request,
            "task": task,
            "stages": stages,
            "events": events,
            "linear_url": _linear_url,
            "format_duration": _format_duration,
        })

    @app.get("/usage", response_class=HTMLResponse)
    def usage_page(request: Request):
        state = _read_state()
        usage = state.get("usage", {})
        snapshots = []
        conn = _get_db()
        if conn:
            try:
                rows = conn.execute(
                    "SELECT * FROM usage_snapshots ORDER BY created_at DESC LIMIT 100"
                ).fetchall()
                snapshots = [dict(r) for r in rows]
            except sqlite3.OperationalError:
                pass
            finally:
                conn.close()
        return templates.TemplateResponse("usage.html", {
            "request": request,
            "usage": usage,
            "snapshots": snapshots,
        })

    @app.get("/metrics", response_class=HTMLResponse)
    def metrics_page(request: Request):
        conn = _get_db()
        metrics: dict = {
            "total_tasks": 0,
            "completed_tasks": 0,
            "failed_tasks": 0,
            "total_cost": 0.0,
            "total_turns": 0,
            "avg_cost": 0.0,
            "avg_turns": 0,
        }
        if conn:
            try:
                row = conn.execute(
                    "SELECT COUNT(*) as total, "
                    "SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed, "
                    "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed, "
                    "SUM(cost_usd) as total_cost, "
                    "SUM(turns) as total_turns "
                    "FROM tasks"
                ).fetchone()
                if row:
                    metrics["total_tasks"] = row["total"] or 0
                    metrics["completed_tasks"] = row["completed"] or 0
                    metrics["failed_tasks"] = row["failed"] or 0
                    metrics["total_cost"] = row["total_cost"] or 0.0
                    metrics["total_turns"] = row["total_turns"] or 0
                    if metrics["total_tasks"] > 0:
                        metrics["avg_cost"] = metrics["total_cost"] / metrics["total_tasks"]
                        metrics["avg_turns"] = metrics["total_turns"] // metrics["total_tasks"]
            except sqlite3.OperationalError:
                pass
            finally:
                conn.close()
        return templates.TemplateResponse("metrics.html", {
            "request": request,
            "metrics": metrics,
        })

    return app


def start_dashboard(
    config: DashboardConfig,
    *,
    state_file: str | Path,
    db_path: str | Path,
    linear_workspace: str = "",
) -> threading.Thread | None:
    """Start the dashboard server in a background daemon thread.

    Returns the thread if started, or None if the dashboard is disabled.
    """
    if not config.enabled:
        return None

    app = create_app(
        state_file=state_file, db_path=db_path,
        linear_workspace=linear_workspace,
    )

    def _run():
        import uvicorn
        uvicorn.run(
            app,
            host=config.host,
            port=config.port,
            log_level="warning",
        )

    thread = threading.Thread(target=_run, daemon=True, name="dashboard")
    thread.start()
    logger.info("Dashboard started on http://%s:%d", config.host, config.port)
    return thread
