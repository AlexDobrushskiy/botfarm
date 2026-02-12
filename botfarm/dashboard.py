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

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


def create_app(
    *,
    state_file: str | Path,
    db_path: str | Path,
) -> FastAPI:
    """Create the FastAPI dashboard application.

    Parameters
    ----------
    state_file:
        Path to the supervisor state.json file.
    db_path:
        Path to the SQLite database.
    """
    app = FastAPI(title="Botfarm Dashboard", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # Store paths on app state for route handlers
    app.state.state_file = Path(state_file).expanduser()
    app.state.db_path = Path(db_path).expanduser()

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
        return templates.TemplateResponse("index.html", {
            "request": request,
            "slots": slots,
            "dispatch_paused": dispatch_paused,
            "dispatch_pause_reason": dispatch_pause_reason,
            "usage": usage,
            "elapsed": _elapsed,
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
        })

    @app.get("/partials/usage", response_class=HTMLResponse)
    def partial_usage(request: Request):
        state = _read_state()
        usage = state.get("usage", {})
        return templates.TemplateResponse("partials/usage.html", {
            "request": request,
            "usage": usage,
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

    def _fetch_tasks(conn: sqlite3.Connection) -> list[dict]:
        try:
            rows = conn.execute(
                "SELECT * FROM tasks ORDER BY created_at DESC LIMIT 50"
            ).fetchall()
            return _enrich_tasks([dict(r) for r in rows])
        except sqlite3.OperationalError:
            return []

    @app.get("/history", response_class=HTMLResponse)
    def history_page(request: Request):
        conn = _get_db()
        tasks = []
        if conn:
            try:
                tasks = _fetch_tasks(conn)
            finally:
                conn.close()
        return templates.TemplateResponse("history.html", {
            "request": request,
            "tasks": tasks,
        })

    @app.get("/partials/history", response_class=HTMLResponse)
    def partial_history(request: Request):
        conn = _get_db()
        tasks = []
        if conn:
            try:
                tasks = _fetch_tasks(conn)
            finally:
                conn.close()
        return templates.TemplateResponse("partials/history.html", {
            "request": request,
            "tasks": tasks,
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
) -> threading.Thread | None:
    """Start the dashboard server in a background daemon thread.

    Returns the thread if started, or None if the dashboard is disabled.
    """
    if not config.enabled:
        return None

    app = create_app(state_file=state_file, db_path=db_path)

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
