"""Database and state helpers for the dashboard."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from itertools import groupby
from pathlib import Path

from fastapi import FastAPI

from botfarm.db import (
    init_db,
    load_all_project_pause_states,
    load_all_slots,
    load_capacity_state,
    load_dispatch_state,
)

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"


def get_db(app: FastAPI) -> sqlite3.Connection | None:
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


def read_state(app: FastAPI) -> dict:
    """Read slot and dispatch state from the database."""
    conn = get_db(app)
    if conn is None:
        return {}
    try:
        slots_rows = load_all_slots(conn)
        slots = []
        for row in slots_rows:
            stages_raw = row["stages_completed"]
            stages = json.loads(stages_raw) if stages_raw else []
            slots.append({
                "project": row["project"],
                "slot_id": row["slot_id"],
                "status": row["status"],
                "ticket_id": row["ticket_id"],
                "ticket_title": row["ticket_title"],
                "branch": row["branch"],
                "pr_url": row["pr_url"],
                "stage": row["stage"],
                "stage_iteration": row["stage_iteration"],
                "current_session_id": row["current_session_id"],
                "started_at": row["started_at"],
                "stage_started_at": row["stage_started_at"],
                "pid": row["pid"],
                "interrupted_by_limit": bool(row["interrupted_by_limit"]),
                "resume_after": row["resume_after"],
                "stages_completed": stages,
            })

        paused, reason, heartbeat = load_dispatch_state(conn)

        # Read latest usage snapshot as fallback for when API refresh
        # is unavailable (replaces the old state.json usage field).
        usage = {}
        last_usage_check = None
        usage_row = conn.execute(
            "SELECT * FROM usage_snapshots ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if usage_row:
            usage = {
                "utilization_5h": usage_row["utilization_5h"],
                "utilization_7d": usage_row["utilization_7d"],
                "resets_at": usage_row["resets_at"],
                "resets_at_5h": usage_row["resets_at"],
                "resets_at_7d": usage_row["resets_at_7d"],
                "extra_usage_enabled": bool(usage_row["extra_usage_enabled"]) if usage_row["extra_usage_enabled"] is not None else False,
                "extra_usage_monthly_limit": usage_row["extra_usage_monthly_limit"],
                "extra_usage_used_credits": usage_row["extra_usage_used_credits"],
                "extra_usage_utilization": usage_row["extra_usage_utilization"],
            }
            last_usage_check = usage_row["created_at"]

        # Read queue entries grouped by project
        queue_rows = conn.execute(
            "SELECT project, position, ticket_id, ticket_title, priority, url, snapshot_at, blocked_by "
            "FROM queue_entries ORDER BY project, position"
        ).fetchall()

        projects_queue = []
        for project_name, rows in groupby(queue_rows, key=lambda r: r["project"]):
            entries = list(rows)
            projects_queue.append({
                "name": project_name,
                "todo_count": len(entries),
                "snapshot_at": entries[0]["snapshot_at"] if entries else None,
                "entries": [
                    {
                        "position": r["position"],
                        "ticket_id": r["ticket_id"],
                        "ticket_title": r["ticket_title"],
                        "priority": r["priority"],
                        "url": r["url"],
                        "blocked_by": json.loads(r["blocked_by"]) if r["blocked_by"] else None,
                    }
                    for r in entries
                ],
            })

        # Include configured projects with zero queue entries
        cfg = app.state.botfarm_config
        if cfg:
            existing_names = {p["name"] for p in projects_queue}
            for proj_cfg in cfg.projects:
                if proj_cfg.name not in existing_names:
                    projects_queue.append({
                        "name": proj_cfg.name,
                        "todo_count": 0,
                        "snapshot_at": None,
                        "entries": [],
                    })
            # Sort so configured projects appear in stable order
            projects_queue.sort(key=lambda p: p["name"])

        project_pauses = load_all_project_pause_states(conn)

        return {
            "slots": slots,
            "dispatch_paused": paused,
            "dispatch_pause_reason": reason,
            "supervisor_heartbeat": heartbeat,
            "usage": usage,
            "last_usage_check": last_usage_check,
            "queue": {"projects": projects_queue} if projects_queue else None,
            "project_pauses": project_pauses,
        }
    except sqlite3.OperationalError:
        return {}
    finally:
        conn.close()


def format_duration(total_seconds: int) -> str:
    if total_seconds < 0:
        return "-"
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{seconds:02d}s"
    return f"{seconds}s"


def elapsed(started_at: str | None) -> str:
    if not started_at:
        return "-"
    try:
        start = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - start
        return format_duration(int(delta.total_seconds()))
    except (ValueError, TypeError):
        return "-"


def context_fill_class(pct: float | None) -> str:
    """Return CSS class for context fill percentage color coding."""
    if pct is None:
        return ""
    if pct < 50:
        return "ctx-fill-green"
    if pct < 75:
        return "ctx-fill-yellow"
    if pct < 90:
        return "ctx-fill-orange"
    return "ctx-fill-red"


def linear_url(app: FastAPI, ticket_id: str) -> str:
    """Build a Linear issue URL from a ticket identifier."""
    ws = app.state.linear_workspace
    if ws:
        return f"https://linear.app/{ws}/issue/{ticket_id}"
    return f"https://linear.app/issue/{ticket_id}"


# Extra grace period on top of poll_interval_seconds before considering
# the supervisor heartbeat stale.
_HEARTBEAT_GRACE_SECONDS = 60


def supervisor_status(app: FastAPI, state: dict) -> dict:
    """Compute supervisor liveness from the heartbeat field.

    The staleness threshold is ``poll_interval_seconds + grace`` so that
    the badge stays green during the normal sleep between ticks.

    Returns a dict with 'running' (bool) and 'heartbeat' (ISO str or None).
    """
    heartbeat = state.get("supervisor_heartbeat")
    if not heartbeat:
        return {"running": False, "heartbeat": None}
    try:
        cfg = app.state.botfarm_config
        poll_interval = cfg.linear.poll_interval_seconds if cfg else 120
        stale_threshold = poll_interval + _HEARTBEAT_GRACE_SECONDS

        hb_dt = datetime.fromisoformat(heartbeat.replace("Z", "+00:00"))
        age = (datetime.now(timezone.utc) - hb_dt).total_seconds()
        return {"running": age <= stale_threshold, "heartbeat": heartbeat}
    except (ValueError, TypeError):
        return {"running": False, "heartbeat": None}


def manual_pause_state(state: dict) -> str:
    """Determine the manual pause UI state from current state.

    Returns one of: "running", "pausing", "paused".
    """
    dispatch_paused = state.get("dispatch_paused", False)
    pause_reason = state.get("dispatch_pause_reason")
    if not dispatch_paused or pause_reason not in ("manual_pause", "start_paused"):
        slots = state.get("slots", [])
        has_manual_paused = any(s["status"] == "paused_manual" for s in slots)
        if has_manual_paused:
            return "paused"
        return "running"
    # start_paused only blocks new dispatches; recovered busy slots keep running
    # normally, so show "paused" (Resume button) rather than "pausing" UI.
    if pause_reason == "start_paused":
        return "paused"
    # Dispatch is paused for manual_pause — check if workers are still busy
    slots = state.get("slots", [])
    has_busy = any(s["status"] == "busy" for s in slots)
    if has_busy:
        busy_count = sum(1 for s in slots if s["status"] == "busy")
        return f"pausing:{busy_count}"
    return "paused"


def get_capacity_data(app: FastAPI) -> dict | None:
    """Load and enrich capacity data from the DB for template rendering."""
    conn = get_db(app)
    if conn is None:
        return None
    try:
        capacity = load_capacity_state(conn)
    except sqlite3.OperationalError:
        capacity = None
    finally:
        conn.close()

    if capacity is not None:
        cfg = app.state.botfarm_config
        cap_cfg = cfg.linear.capacity_monitoring if cfg else None
        limit = capacity["limit"]
        count = capacity["issue_count"]
        ratio = count / limit if limit else 0
        capacity["pct"] = ratio * 100
        warn = cap_cfg.warning_threshold if cap_cfg else 0.70
        crit = cap_cfg.critical_threshold if cap_cfg else 0.85
        pause = cap_cfg.pause_threshold if cap_cfg else 0.95
        if ratio >= pause:
            capacity["color_class"] = "status-failed"
            capacity["severity"] = "blocked"
        elif ratio >= crit:
            capacity["color_class"] = "status-failed"
            capacity["severity"] = "critical"
        elif ratio >= warn:
            capacity["color_class"] = "status-busy"
            capacity["severity"] = "warning"
        else:
            capacity["color_class"] = "status-free"
            capacity["severity"] = "ok"

    return capacity


# --- Constants for rate-limiting ---

_USAGE_REFRESH_INTERVAL = 60  # seconds — rate-limit API calls
_UPDATE_CHECK_INTERVAL = 60  # seconds


def init_caches(app: FastAPI) -> None:
    """Initialise per-app rate-limit caches on ``app.state``.

    Called by ``create_app()`` so each app instance gets isolated caches
    (important for tests and when multiple apps coexist in one process).
    """
    app.state._usage_refresh_lock = threading.Lock()
    app.state._last_usage_refresh = {"time": None, "data": None}
    app.state._dashboard_last_fresh = {"time": None}
    app.state._update_check_lock = threading.Lock()
    app.state._last_update_check = {"time": None, "commits_behind": 0}


def get_dashboard_last_fresh_time(app: FastAPI) -> str | None:
    return app.state._dashboard_last_fresh["time"]


def refresh_and_get_usage(app: FastAPI) -> dict | None:
    """Call the usage API and return fresh data as a dict, or None on failure.

    Rate-limited to at most one API call per ``_USAGE_REFRESH_INTERVAL``
    seconds to avoid hammering the API (htmx polls every 5 s).
    """
    import time

    now = time.monotonic()
    lock = app.state._usage_refresh_lock
    cache = app.state._last_usage_refresh
    with lock:
        last = cache["time"]
        if last is not None and now - last < _USAGE_REFRESH_INTERVAL:
            return cache["data"]
        # Don't claim the slot yet — wait for the API call to succeed
        in_flight_time = now

    conn = None
    try:
        conn = init_db(app.state.db_path)
        # Look up via the package module so tests can mock.patch
        # "botfarm.dashboard.refresh_usage_snapshot"
        import botfarm.dashboard as _pkg
        state = _pkg.refresh_usage_snapshot(conn)
        if state is not None:
            result = state.to_dict()
            with lock:
                cache["time"] = in_flight_time
                cache["data"] = result
            app.state._dashboard_last_fresh["time"] = (
                datetime.now(timezone.utc).isoformat()
            )
            return result
    except Exception:
        logger.warning("Dashboard usage refresh failed", exc_info=True)
    finally:
        if conn is not None:
            conn.close()
    return None


def usage_is_stale(last_fresh_iso: str | None) -> bool:
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


def check_commits_behind(app: FastAPI) -> int:
    """Check how far behind origin/main we are, rate-limited."""
    import time as _time

    now = _time.monotonic()
    lock = app.state._update_check_lock
    cache = app.state._last_update_check
    with lock:
        last = cache["time"]
        if last is not None and now - last < _UPDATE_CHECK_INTERVAL:
            return cache["commits_behind"]

    try:
        # Look up via the package module so tests can mock.patch
        # "botfarm.dashboard.commits_behind"
        import botfarm.dashboard as _pkg
        count = _pkg.commits_behind(env=app.state.git_env)
    except Exception:
        logger.warning("Update check failed", exc_info=True)
        count = 0

    with lock:
        cache["time"] = now
        cache["commits_behind"] = count
    return count
