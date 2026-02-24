"""Web dashboard for botfarm — FastAPI + Jinja2 + htmx.

Serves a lightweight server-rendered dashboard that auto-refreshes via htmx
polling. Designed to run inside the supervisor process as a background thread.
"""

from __future__ import annotations

import html
import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from botfarm.config import (
    BotfarmConfig,
    DashboardConfig,
    EDITABLE_FIELDS,
    STRUCTURAL_FIELDS,
    apply_config_updates,
    validate_config_updates,
    validate_structural_config_updates,
    write_config_updates,
    write_structural_config_updates,
)
from botfarm.db import (
    count_tasks,
    get_distinct_projects,
    get_events,
    get_stage_runs,
    get_task,
    get_task_history,
    init_db,
)
from botfarm.usage import refresh_usage_snapshot

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


def create_app(
    *,
    state_file: str | Path,
    db_path: str | Path,
    linear_workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
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
    botfarm_config:
        Live BotfarmConfig object for runtime editing. If ``None``, the
        config page is disabled.
    """
    app = FastAPI(title="Botfarm Dashboard", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # Store paths on app state for route handlers
    app.state.state_file = Path(state_file).expanduser()
    app.state.db_path = Path(db_path).expanduser()
    app.state.linear_workspace = linear_workspace
    app.state.botfarm_config = botfarm_config
    app.state.restart_required = False

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
        dashboard_checked = _dashboard_last_fresh["time"]
        last_usage_check = dashboard_checked or state.get("last_usage_check")
        return templates.TemplateResponse("index.html", {
            "request": request,
            "slots": slots,
            "dispatch_paused": dispatch_paused,
            "dispatch_pause_reason": dispatch_pause_reason,
            "usage": usage,
            "queue": queue,
            "last_usage_check": last_usage_check,
            "usage_stale": _usage_is_stale(last_usage_check),
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

    _usage_refresh_lock = threading.Lock()
    _last_usage_refresh: dict = {"time": None, "data": None}
    _USAGE_REFRESH_INTERVAL = 60  # seconds — rate-limit API calls
    # Track when the dashboard itself last got fresh data (wall-clock ISO str)
    _dashboard_last_fresh: dict = {"time": None}

    def _refresh_and_get_usage() -> dict | None:
        """Call the usage API and return fresh data as a dict, or None on failure.

        Rate-limited to at most one API call per ``_USAGE_REFRESH_INTERVAL``
        seconds to avoid hammering the API (htmx polls every 5 s).
        """
        import time

        now = time.monotonic()
        with _usage_refresh_lock:
            last = _last_usage_refresh["time"]
            if last is not None and now - last < _USAGE_REFRESH_INTERVAL:
                return _last_usage_refresh["data"]
            # Don't claim the slot yet — wait for the API call to succeed
            in_flight_time = now

        conn = None
        try:
            conn = init_db(app.state.db_path)
            state = refresh_usage_snapshot(conn)
            if state is not None:
                result = state.to_dict()
                with _usage_refresh_lock:
                    _last_usage_refresh["time"] = in_flight_time
                    _last_usage_refresh["data"] = result
                _dashboard_last_fresh["time"] = (
                    datetime.now(timezone.utc).isoformat()
                )
                return result
        except Exception:
            logger.warning("Dashboard usage refresh failed", exc_info=True)
        finally:
            if conn is not None:
                conn.close()
        return None

    def _usage_is_stale(last_fresh_iso: str | None) -> bool:
        """Return True when dashboard usage data is older than 2x refresh."""
        if not last_fresh_iso:
            return False
        try:
            last_dt = datetime.fromisoformat(
                last_fresh_iso.replace("Z", "+00:00")
            )
            age = (datetime.now(timezone.utc) - last_dt).total_seconds()
            return age > _USAGE_REFRESH_INTERVAL * 2
        except (ValueError, TypeError):
            return False

    @app.get("/partials/usage", response_class=HTMLResponse)
    def partial_usage(request: Request):
        state = _read_state()
        # Try to get fresh data from the API; fall back to state.json
        fresh = _refresh_and_get_usage()
        usage = fresh if fresh is not None else state.get("usage", {})
        dispatch_paused = state.get("dispatch_paused", False)
        dispatch_pause_reason = state.get("dispatch_pause_reason")
        # Use the dashboard's own refresh timestamp; fall back to supervisor's
        dashboard_checked = _dashboard_last_fresh["time"]
        last_usage_check = dashboard_checked or state.get("last_usage_check")
        stale = _usage_is_stale(last_usage_check)
        return templates.TemplateResponse("partials/usage.html", {
            "request": request,
            "usage": usage,
            "dispatch_paused": dispatch_paused,
            "dispatch_pause_reason": dispatch_pause_reason,
            "last_usage_check": last_usage_check,
            "usage_stale": stale,
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

    USAGE_RANGE_HOURS = {"24h": 24, "7d": 168, "30d": 720}

    @app.get("/usage", response_class=HTMLResponse)
    def usage_page(request: Request):
        state = _read_state()
        fresh = _refresh_and_get_usage()
        usage = fresh if fresh is not None else state.get("usage", {})
        time_range = request.query_params.get("range", "7d")
        if time_range not in USAGE_RANGE_HOURS:
            time_range = "7d"
        hours = USAGE_RANGE_HOURS[time_range]
        snapshots = []
        conn = _get_db()
        if conn:
            try:
                rows = conn.execute(
                    "SELECT * FROM usage_snapshots "
                    "WHERE created_at >= datetime('now', ?)"
                    " ORDER BY created_at ASC",
                    (f"-{hours} hours",),
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
            "time_range": time_range,
        })

    def _compute_metrics(
        conn: sqlite3.Connection, project: str | None = None,
    ) -> dict:
        """Compute all metrics, optionally filtered by project."""
        metrics: dict = {**_EMPTY_METRICS, "failure_reasons": []}
        where = " WHERE 1=1"
        params: list[object] = []
        if project:
            where += " AND project = ?"
            params.append(project)

        # Core aggregates
        row = conn.execute(
            "SELECT COUNT(*) as total, "
            "SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed, "
            "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) as failed, "
            "COALESCE(SUM(cost_usd), 0) as total_cost "
            "FROM tasks" + where,
            params,
        ).fetchone()
        if row:
            metrics["total_tasks"] = row["total"] or 0
            metrics["completed_tasks"] = row["completed"] or 0
            metrics["failed_tasks"] = row["failed"] or 0
            metrics["total_cost"] = row["total_cost"]
            if metrics["total_tasks"] > 0:
                metrics["success_rate"] = round(
                    metrics["completed_tasks"] / metrics["total_tasks"] * 100, 1,
                )

        # Averages over completed tasks only
        avg_row = conn.execute(
            "SELECT COALESCE(AVG(cost_usd), 0) as avg_cost, "
            "COALESCE(AVG(turns), 0) as avg_turns, "
            "COALESCE(AVG(review_iterations), 0) as avg_reviews "
            "FROM tasks" + where + " AND status = 'completed'",
            params,
        ).fetchone()
        if avg_row:
            metrics["avg_cost"] = avg_row["avg_cost"]
            metrics["avg_turns"] = round(avg_row["avg_turns"])
            metrics["avg_review_iterations"] = round(avg_row["avg_reviews"], 1)

        # Average wall time (only for tasks with both timestamps)
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

        # Time-bucketed counts & costs (completed tasks only)
        for label, interval in [
            ("today", "start of day"),
            ("week", "-6 days"),
            ("month", "-29 days"),
        ]:
            bucket_row = conn.execute(
                "SELECT COUNT(*) as cnt, "
                "COALESCE(SUM(cost_usd), 0) as cost "
                "FROM tasks" + where
                + " AND status = 'completed'"
                " AND completed_at >= datetime('now', ?)",
                [*params, interval],
            ).fetchone()
            if bucket_row:
                metrics[f"completed_{label}"] = bucket_row["cnt"] or 0
                metrics[f"cost_{label}"] = bucket_row["cost"]

        # Most common failure reasons
        reason_rows = conn.execute(
            "SELECT failure_reason, COUNT(*) as cnt "
            "FROM tasks" + where
            + " AND failure_reason IS NOT NULL AND failure_reason != '' "
            "GROUP BY failure_reason ORDER BY cnt DESC LIMIT 5",
            params,
        ).fetchall()
        metrics["failure_reasons"] = [
            {"reason": r["failure_reason"], "count": r["cnt"]}
            for r in reason_rows
        ]

        return metrics

    _EMPTY_METRICS: dict = {
        "total_tasks": 0, "completed_tasks": 0, "failed_tasks": 0,
        "total_cost": 0.0, "avg_cost": 0.0, "avg_turns": 0,
        "avg_review_iterations": 0.0, "avg_wall_time_seconds": 0,
        "success_rate": 0.0, "completed_today": 0, "completed_week": 0,
        "completed_month": 0, "cost_today": 0.0, "cost_week": 0.0,
        "cost_month": 0.0, "failure_reasons": [],
    }

    @app.get("/metrics", response_class=HTMLResponse)
    def metrics_page(request: Request):
        filter_project = request.query_params.get("project") or ""
        conn = _get_db()
        metrics = dict(_EMPTY_METRICS)
        projects: list[str] = []
        if conn:
            try:
                metrics = _compute_metrics(conn, project=filter_project or None)
                projects = get_distinct_projects(conn)
            except sqlite3.OperationalError:
                pass
            finally:
                conn.close()
        return templates.TemplateResponse("metrics.html", {
            "request": request,
            "metrics": metrics,
            "projects": projects,
            "filter_project": filter_project,
            "format_duration": _format_duration,
        })

    # --- Read-only config view ---

    def _mask_secret(value: str) -> str:
        """Mask a secret string, showing first 4 + last 4 chars."""
        if not value:
            return ""
        if len(value) <= 8:
            return "****"
        return value[:4] + "****" + value[-4:]

    def _full_config_values() -> dict:
        """Extract the full running config as a nested dict for display."""
        cfg = app.state.botfarm_config
        if cfg is None:
            return {}
        return {
            "projects": [
                {
                    "name": p.name,
                    "linear_team": p.linear_team,
                    "linear_project": p.linear_project,
                    "base_dir": p.base_dir,
                    "worktree_prefix": p.worktree_prefix,
                    "slots": list(p.slots),
                }
                for p in cfg.projects
            ],
            "linear": {
                "api_key": _mask_secret(cfg.linear.api_key),
                "workspace": cfg.linear.workspace,
                "poll_interval_seconds": cfg.linear.poll_interval_seconds,
                "exclude_tags": list(cfg.linear.exclude_tags),
                "todo_status": cfg.linear.todo_status,
                "in_progress_status": cfg.linear.in_progress_status,
                "done_status": cfg.linear.done_status,
                "in_review_status": cfg.linear.in_review_status,
                "failed_status": cfg.linear.failed_status,
                "comment_on_failure": cfg.linear.comment_on_failure,
                "comment_on_completion": cfg.linear.comment_on_completion,
                "comment_on_limit_pause": cfg.linear.comment_on_limit_pause,
            },
            "agents": {
                "max_review_iterations": cfg.agents.max_review_iterations,
                "max_ci_retries": cfg.agents.max_ci_retries,
                "timeout_minutes": dict(cfg.agents.timeout_minutes),
                "timeout_grace_seconds": cfg.agents.timeout_grace_seconds,
            },
            "usage_limits": {
                "pause_five_hour_threshold": cfg.usage_limits.pause_five_hour_threshold,
                "pause_seven_day_threshold": cfg.usage_limits.pause_seven_day_threshold,
            },
            "notifications": {
                "webhook_url": _mask_secret(cfg.notifications.webhook_url),
                "webhook_format": cfg.notifications.webhook_format,
                "rate_limit_seconds": cfg.notifications.rate_limit_seconds,
            },
            "dashboard": {
                "enabled": cfg.dashboard.enabled,
                "host": cfg.dashboard.host,
                "port": cfg.dashboard.port,
            },
            "database": {
                "path": cfg.database.path,
            },
            "state_file": cfg.state_file,
        }

    @app.get("/config/view", response_class=HTMLResponse)
    def config_view_page(request: Request):
        cfg = app.state.botfarm_config
        enabled = cfg is not None
        return templates.TemplateResponse("config_view.html", {
            "request": request,
            "config_enabled": enabled,
            "config_values": _full_config_values(),
        })

    # --- Config editing ---

    def _config_values() -> dict:
        """Extract current editable config values as a nested dict."""
        cfg = app.state.botfarm_config
        if cfg is None:
            return {}
        return {
            "linear": {
                "poll_interval_seconds": cfg.linear.poll_interval_seconds,
                "comment_on_failure": cfg.linear.comment_on_failure,
                "comment_on_completion": cfg.linear.comment_on_completion,
                "comment_on_limit_pause": cfg.linear.comment_on_limit_pause,
            },
            "usage_limits": {
                "pause_five_hour_threshold": cfg.usage_limits.pause_five_hour_threshold,
                "pause_seven_day_threshold": cfg.usage_limits.pause_seven_day_threshold,
            },
            "agents": {
                "max_review_iterations": cfg.agents.max_review_iterations,
                "max_ci_retries": cfg.agents.max_ci_retries,
                "timeout_minutes": dict(cfg.agents.timeout_minutes),
                "timeout_grace_seconds": cfg.agents.timeout_grace_seconds,
            },
            "notifications": {
                "webhook_url": cfg.notifications.webhook_url,
                "webhook_format": cfg.notifications.webhook_format,
                "rate_limit_seconds": cfg.notifications.rate_limit_seconds,
            },
            "projects": [
                {
                    "name": p.name,
                    "slots": list(p.slots),
                    "linear_project": p.linear_project,
                }
                for p in cfg.projects
            ],
        }

    @app.get("/config", response_class=HTMLResponse)
    def config_page(request: Request):
        cfg = app.state.botfarm_config
        enabled = cfg is not None
        return templates.TemplateResponse("config.html", {
            "request": request,
            "config_enabled": enabled,
            "config_values": _config_values(),
            "editable_fields": EDITABLE_FIELDS,
            "restart_required": app.state.restart_required,
        })

    @app.post("/config", response_class=HTMLResponse)
    async def config_update(request: Request):
        cfg = app.state.botfarm_config
        if cfg is None:
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                "Config editing is not available.</div>",
                status_code=400,
            )

        try:
            updates = await request.json()
        except Exception:
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                "Invalid JSON body.</div>",
                status_code=400,
            )

        if not isinstance(updates, dict):
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                "Request body must be a JSON object.</div>",
                status_code=400,
            )

        # Split into runtime-editable and structural updates
        structural_sections = {"notifications", "projects"}
        runtime_updates = {
            k: v for k, v in updates.items() if k not in structural_sections
        }
        structural_updates = {
            k: v for k, v in updates.items() if k in structural_sections
        }

        # Validate runtime updates
        all_errors: list[str] = []
        if runtime_updates:
            all_errors.extend(validate_config_updates(runtime_updates))

        # Validate structural updates
        if structural_updates:
            all_errors.extend(
                validate_structural_config_updates(structural_updates, cfg)
            )

        if all_errors:
            error_html = "".join(
                f"<li>{html.escape(e)}</li>" for e in all_errors
            )
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                f"<strong>Validation errors:</strong><ul>{error_html}</ul></div>",
                status_code=422,
            )

        config_path = Path(cfg.source_path) if cfg.source_path else None

        # Apply runtime updates to in-memory config + YAML
        if runtime_updates:
            apply_config_updates(cfg, runtime_updates)
            if config_path and config_path.exists():
                try:
                    write_config_updates(config_path, runtime_updates)
                except Exception:
                    logger.exception("Failed to write config file")
                    return HTMLResponse(
                        '<div class="config-feedback warning" role="alert">'
                        "Applied to running config but failed to save to file. "
                        "Changes will be lost on restart.</div>",
                        status_code=200,
                    )

        # Write structural updates to YAML only (NOT in-memory)
        if structural_updates:
            if config_path and config_path.exists():
                try:
                    write_structural_config_updates(
                        config_path, structural_updates,
                    )
                    app.state.restart_required = True
                except Exception:
                    logger.exception("Failed to write structural config")
                    return HTMLResponse(
                        '<div class="config-feedback error" role="alert">'
                        "Failed to save structural changes to file.</div>",
                        status_code=500,
                    )
            else:
                return HTMLResponse(
                    '<div class="config-feedback error" role="alert">'
                    "Cannot save structural changes: no config file path.</div>",
                    status_code=400,
                )

        msg = "Config updated successfully."
        if structural_updates:
            msg = (
                "Config saved to file. "
                "Restart required to apply structural changes."
            )
        return HTMLResponse(
            f'<div class="config-feedback success" role="alert">'
            f"{msg}</div>",
            status_code=200,
        )

    return app


def start_dashboard(
    config: DashboardConfig,
    *,
    state_file: str | Path,
    db_path: str | Path,
    linear_workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
) -> threading.Thread | None:
    """Start the dashboard server in a background daemon thread.

    Returns the thread if started, or None if the dashboard is disabled.
    """
    if not config.enabled:
        return None

    app = create_app(
        state_file=state_file, db_path=db_path,
        linear_workspace=linear_workspace,
        botfarm_config=botfarm_config,
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
