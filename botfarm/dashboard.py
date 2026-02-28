"""Web dashboard for botfarm — FastAPI + Jinja2 + htmx.

Serves a lightweight server-rendered dashboard that auto-refreshes via htmx
polling. Designed to run inside the supervisor process as a background thread.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import sqlite3
import threading
import yaml
from datetime import datetime, timezone
from itertools import groupby
from collections.abc import Callable
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

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
    write_yaml_atomic,
)
from botfarm.db import (
    count_tasks,
    get_distinct_projects,
    get_events,
    get_latest_context_fill_by_ticket,
    get_stage_run_aggregates,
    get_stage_runs,
    get_task,
    get_task_by_ticket,
    get_task_history,
    init_db,
    load_all_project_pause_states,
    load_all_slots,
    load_dispatch_state,
    save_project_pause_state,
)
from botfarm.git_update import commits_behind
from botfarm.worker import STAGES
from botfarm.workflow import load_all_pipelines, resolve_max_iterations
from botfarm.usage import refresh_usage_snapshot

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"


# ---------------------------------------------------------------------------
# NDJSON line formatting for SSE log streaming
# ---------------------------------------------------------------------------


def format_ndjson_line(raw_line: str) -> tuple[str, str]:
    """Parse a raw NDJSON line and return ``(event_type, formatted_text)``.

    *event_type* is one of: ``assistant``, ``tool_use``, ``tool_result``,
    ``result``, ``system``, or ``log`` (fallback for non-JSON lines).

    *formatted_text* is a human-readable summary of the event suitable for
    display in a terminal-style log viewer.
    """
    stripped = raw_line.strip()
    if not stripped:
        return ("log", "")

    try:
        event = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        # Not JSON — pass through as-is
        return ("log", stripped)

    if not isinstance(event, dict):
        return ("log", stripped)

    msg_type = event.get("type", "")

    if msg_type == "assistant":
        return _format_assistant(event)
    if msg_type == "user":
        return _format_user(event)
    if msg_type == "result":
        return _format_result(event)

    # Other NDJSON event types (e.g. system messages) — show type as prefix
    if msg_type:
        return ("system", f"[{msg_type}]")

    return ("log", stripped)


def _format_assistant(event: dict) -> tuple[str, str]:
    """Format an ``assistant`` NDJSON event.

    Extracts text content and lists tool_use calls.
    """
    message = event.get("message") or {}
    content_blocks = message.get("content") or []

    parts: list[str] = []
    tool_lines: list[str] = []

    for block in content_blocks:
        block_type = block.get("type", "")
        if block_type == "text":
            text = block.get("text", "").strip()
            if text:
                parts.append(text)
        elif block_type == "tool_use":
            tool_name = block.get("name", "unknown")
            tool_input = block.get("input") or {}
            summary = _summarize_tool_input(tool_name, tool_input)
            tool_lines.append(f"  -> {tool_name}({summary})")

    lines: list[str] = []
    if parts:
        lines.append("\n".join(parts))
    if tool_lines:
        lines.extend(tool_lines)

    if not lines:
        return ("assistant", "[assistant turn]")

    # When only tool_use blocks are present (no text), categorise as "tool_use"
    # so the dashboard can apply distinct purple styling.
    event_type = "tool_use" if (tool_lines and not parts) else "assistant"
    return (event_type, "\n".join(lines))


def _format_user(event: dict) -> tuple[str, str]:
    """Format a ``user`` NDJSON event (tool results)."""
    message = event.get("message") or {}
    content_blocks = message.get("content") or []

    results: list[str] = []
    for block in content_blocks:
        block_type = block.get("type", "")
        if block_type == "tool_result":
            is_error = block.get("is_error", False)
            status = "ERROR" if is_error else "ok"
            # Content may be a string or list of content blocks
            content = block.get("content", "")
            snippet = ""
            if isinstance(content, str):
                snippet = content[:120]
            elif isinstance(content, list):
                for item in content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        snippet = item.get("text", "")[:120]
                        break
            if snippet:
                results.append(f"  [{status}] {snippet}")
            else:
                results.append(f"  [{status}]")

    if not results:
        return ("tool_result", "[tool results]")
    return ("tool_result", "\n".join(results))


def _format_result(event: dict) -> tuple[str, str]:
    """Format a ``result`` NDJSON event (stage completion)."""
    num_turns = event.get("num_turns", 0)
    duration_ms = event.get("duration_ms", 0)
    duration_s = duration_ms / 1000.0 if duration_ms else 0
    subtype = event.get("subtype", "")
    is_error = event.get("is_error", False)
    result_text = (event.get("result") or "")[:200]

    parts = [f"Completed in {num_turns} turns ({duration_s:.1f}s)"]
    if subtype:
        parts[0] += f" [{subtype}]"
    if is_error:
        parts[0] += " [ERROR]"
    if result_text:
        parts.append(result_text)
    return ("result", "\n".join(parts))


def _summarize_tool_input(tool_name: str, tool_input: dict) -> str:
    """Produce a short summary of tool input for display."""
    if tool_name in ("Read", "Glob"):
        return tool_input.get("file_path") or tool_input.get("pattern", "")
    if tool_name == "Edit":
        fp = tool_input.get("file_path", "")
        return fp
    if tool_name == "Write":
        return tool_input.get("file_path", "")
    if tool_name == "Bash":
        cmd = tool_input.get("command", "")
        return cmd[:80] if cmd else ""
    if tool_name == "Grep":
        return tool_input.get("pattern", "")
    # Generic: show first key=value
    for k, v in tool_input.items():
        sv = str(v)[:60]
        return f"{k}={sv}"
    return ""


def format_codex_ndjson_line(raw_line: str) -> tuple[str, str]:
    """Parse a raw Codex JSONL line and return ``(event_type, formatted_text)``.

    Handles Codex Responses API event types:
    ``thread.started``, ``turn.started``, ``item.completed``,
    ``turn.completed``.

    For ``item.completed`` events, ``agent_message`` text is shown
    prominently, ``reasoning`` is dimmed, and ``shell_command`` /
    ``shell_result`` are shown in code block style.
    """
    stripped = raw_line.strip()
    if not stripped:
        return ("log", "")

    try:
        event = json.loads(stripped)
    except (json.JSONDecodeError, ValueError):
        return ("log", stripped)

    if not isinstance(event, dict):
        return ("log", stripped)

    event_type = event.get("type", "")

    if event_type == "thread.started":
        return ("system", "[Codex thread started]")

    if event_type == "turn.started":
        return ("system", "[Codex turn started]")

    if event_type == "turn.completed":
        status = event.get("status", "")
        return ("result", f"[Codex turn completed: {status}]" if status else "[Codex turn completed]")

    if event_type == "item.completed":
        item = event.get("item") or {}
        item_type = item.get("type", "")

        if item_type == "agent_message":
            text = ""
            for block in item.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    text += block.get("text", "")
            return ("assistant", text.strip() if text.strip() else "[agent message]")

        if item_type == "reasoning":
            text = ""
            for block in item.get("content", []):
                if isinstance(block, dict) and block.get("type") == "text":
                    text += block.get("text", "")
            return ("system", text.strip() if text.strip() else "[reasoning]")

        if item_type == "shell_command":
            cmd = item.get("command", "")
            return ("tool_use", f"$ {cmd}" if cmd else "[shell command]")

        if item_type == "shell_result":
            output = item.get("output", "")
            snippet = output[:500] if output else ""
            return ("tool_result", snippet if snippet else "[shell result]")

        # Other item types — show type
        return ("system", f"[{item_type}]" if item_type else "[item]")

    # Unknown event type — show raw type
    if event_type:
        return ("system", f"[{event_type}]")

    return ("log", stripped)


def _review_display_status(exit_subtype: str | None) -> str:
    """Map a review stage exit_subtype to a human-readable display status."""
    exit_sub = (exit_subtype or "").lower()
    if exit_sub in ("approved", "changes_requested"):
        return exit_sub.upper()
    if exit_sub == "skipped":
        return "Skipped"
    if exit_sub in ("failed", "error"):
        return "Failed"
    return "In Progress"


def build_pipeline_state(
    stage_runs: list[dict], task_status: str | None,
) -> list[dict]:
    """Aggregate stage runs into per-stage pipeline state for the stepper.

    Returns a list of dicts (one per canonical stage) with keys:
        name, status, iteration_count, has_limit_restart, codex_review
    """
    # Collect info per stage, excluding codex_review from pipeline stages
    stage_info: dict[str, dict] = {}
    codex_runs: list[dict] = []
    for run in stage_runs:
        name = run["stage"]
        if name == "codex_review":
            codex_runs.append(run)
            continue
        if name not in stage_info:
            stage_info[name] = {
                "count": 0,
                "has_limit_restart": False,
            }
        info = stage_info[name]
        info["count"] += 1
        if run.get("was_limit_restart"):
            info["has_limit_restart"] = True

    # Summarise Codex review runs for attachment to the review stage
    codex_summary = None
    if codex_runs:
        last = codex_runs[-1]
        codex_status = _review_display_status(last.get("exit_subtype"))
        codex_summary = {"status": codex_status, "count": len(codex_runs)}

    # Find the last stage that has runs (by canonical order)
    last_run_idx = -1
    for i, stage_name in enumerate(STAGES):
        if stage_name in stage_info:
            last_run_idx = i

    result = []
    for i, stage_name in enumerate(STAGES):
        info = stage_info.get(stage_name)
        if info is None:
            # No runs for this stage — but may have been skipped
            status = "completed" if i < last_run_idx else "pending"
        elif i < last_run_idx:
            # A later stage has runs, so this one completed
            status = "completed"
        elif i == last_run_idx:
            # This is the last stage with runs — depends on task status
            if task_status == "completed":
                status = "completed"
            elif task_status == "failed":
                status = "failed"
            else:
                status = "active"
        else:
            status = "pending"

        entry: dict = {
            "name": stage_name,
            "status": status,
            "iteration_count": info["count"] if info else 0,
            "has_limit_restart": info["has_limit_restart"] if info else False,
        }
        # Attach Codex review summary to the review stage
        if stage_name == "review" and codex_summary:
            entry["codex_review"] = codex_summary
        result.append(entry)

    return result


def create_app(
    *,
    db_path: str | Path,
    linear_workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
    state_file: str | Path | None = None,
    logs_dir: str | Path | None = None,
    on_pause: Callable[[], None] | None = None,
    on_resume: Callable[[], None] | None = None,
    on_update: Callable[[], None] | None = None,
    on_rerun_preflight: Callable[[], None] | None = None,
    get_preflight_results: Callable[[], list] | None = None,
    get_degraded: Callable[[], bool] | None = None,
    update_failed_event: threading.Event | None = None,
    git_env: dict[str, str] | None = None,
    auto_restart: bool = True,
) -> FastAPI:
    """Create the FastAPI dashboard application.

    Parameters
    ----------
    db_path:
        Path to the SQLite database.
    linear_workspace:
        Linear workspace slug used for building ticket URLs.
    botfarm_config:
        Live BotfarmConfig object for runtime editing. If ``None``, the
        config page is disabled.
    state_file:
        Deprecated. Ignored. Kept only for backward compatibility during
        the transition period.
    logs_dir:
        Base directory for per-ticket log files (e.g. ``~/.botfarm/logs``).
        When set, the log viewer feature is enabled.
    on_pause:
        Callback invoked when the user clicks Pause. Should be a callable
        with no arguments (e.g. ``supervisor.request_pause``).
    on_resume:
        Callback invoked when the user clicks Resume. Should be a callable
        with no arguments (e.g. ``supervisor.request_resume``).
    on_update:
        Callback invoked when the user clicks Update & Restart. Should be
        a callable with no arguments (e.g. ``supervisor.request_update``).
    on_rerun_preflight:
        Callback invoked when the user triggers a manual preflight re-run
        from the dashboard (e.g. ``supervisor.request_rerun_preflight``).
    get_preflight_results:
        Callable that returns the latest list of preflight ``CheckResult``
        objects (e.g. ``supervisor.get_preflight_results``).
    get_degraded:
        Callable that returns whether the supervisor is in degraded mode.
    update_failed_event:
        Threading event set by the supervisor when an update fails.
        The banner endpoint checks this to reset the \"Updating...\" state.
    """
    app = FastAPI(title="Botfarm Dashboard", docs_url=None, redoc_url=None)
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # Store paths on app state for route handlers
    app.state.db_path = Path(db_path).expanduser()
    app.state.linear_workspace = linear_workspace
    app.state.botfarm_config = botfarm_config
    app.state.restart_required = False
    app.state.on_pause = on_pause
    app.state.on_resume = on_resume
    app.state.on_update = on_update
    app.state.on_rerun_preflight = on_rerun_preflight
    app.state.get_preflight_results = get_preflight_results
    app.state.get_degraded = get_degraded
    app.state.update_in_progress = False
    app.state.update_failed_event = update_failed_event
    app.state.auto_restart = auto_restart
    app.state.logs_dir = Path(logs_dir).expanduser() if logs_dir else None
    app.state.git_env = git_env

    # --- Helpers ---

    def _read_state() -> dict:
        """Read slot and dispatch state from the database."""
        conn = _get_db()
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

    def _context_fill_class(pct: float | None) -> str:
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

    def _linear_url(ticket_id: str) -> str:
        """Build a Linear issue URL from a ticket identifier."""
        ws = app.state.linear_workspace
        if ws:
            return f"https://linear.app/{ws}/issue/{ticket_id}"
        return f"https://linear.app/issue/{ticket_id}"

    # Extra grace period on top of poll_interval_seconds before considering
    # the supervisor heartbeat stale.  The heartbeat is written once per tick,
    # then the supervisor sleeps for poll_interval_seconds.  The buffer
    # accounts for tick execution time and minor scheduling jitter.
    _HEARTBEAT_GRACE_SECONDS = 60

    def _supervisor_status(state: dict) -> dict:
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
        slots = _enrich_slots_with_context_fill(state.get("slots", []))
        slots = _enrich_slots_with_pipeline(slots)
        slots = _enrich_slots_with_codex_review(slots)
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
            "context_fill_class": _context_fill_class,
            "supervisor": _supervisor_status(state),
            "pause_state": _manual_pause_state(state),
            "has_callbacks": app.state.on_pause is not None,
        })

    def _enrich_slots_with_context_fill(slots: list[dict]) -> list[dict]:
        """Attach latest context_fill_pct to busy slots from the DB."""
        busy_tickets = [
            s["ticket_id"] for s in slots
            if s.get("status") == "busy" and s.get("ticket_id")
        ]
        if not busy_tickets:
            return slots
        conn = _get_db()
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

    def _enrich_slots_with_codex_review(slots: list[dict]) -> list[dict]:
        """Attach Codex review status to busy slots in the review stage."""
        review_tickets = [
            s["ticket_id"] for s in slots
            if s.get("status") == "busy"
            and s.get("stage") == "review"
            and s.get("ticket_id")
        ]
        if not review_tickets:
            return slots
        conn = _get_db()
        if not conn:
            return slots
        try:
            for slot in slots:
                tid = slot.get("ticket_id")
                if tid not in review_tickets:
                    continue
                # Look up task_id for this ticket
                task_row = conn.execute(
                    "SELECT id FROM tasks WHERE ticket_id = ? ORDER BY id DESC LIMIT 1",
                    (tid,),
                ).fetchone()
                if not task_row:
                    continue
                # Find the latest codex_review stage run
                codex_row = conn.execute(
                    "SELECT exit_subtype FROM stage_runs "
                    "WHERE task_id = ? AND stage = 'codex_review' "
                    "ORDER BY id DESC LIMIT 1",
                    (task_row["id"],),
                ).fetchone()
                if codex_row:
                    slot["codex_review_status"] = _review_display_status(
                        codex_row["exit_subtype"]
                    )
                    # Also check the latest Claude review status
                    claude_row = conn.execute(
                        "SELECT exit_subtype FROM stage_runs "
                        "WHERE task_id = ? AND stage = 'review' "
                        "ORDER BY id DESC LIMIT 1",
                        (task_row["id"],),
                    ).fetchone()
                    slot["claude_review_status"] = _review_display_status(
                        claude_row["exit_subtype"] if claude_row else None
                    )
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()
        return slots

    @app.get("/partials/slots", response_class=HTMLResponse)
    def partial_slots(request: Request):
        state = _read_state()
        slots = _enrich_slots_with_context_fill(state.get("slots", []))
        slots = _enrich_slots_with_pipeline(slots)
        slots = _enrich_slots_with_codex_review(slots)
        dispatch_paused = state.get("dispatch_paused", False)
        dispatch_pause_reason = state.get("dispatch_pause_reason")
        project_pauses = state.get("project_pauses", {})
        return templates.TemplateResponse("partials/slots.html", {
            "request": request,
            "slots": slots,
            "dispatch_paused": dispatch_paused,
            "dispatch_pause_reason": dispatch_pause_reason,
            "project_pauses": project_pauses,
            "elapsed": _elapsed,
            "linear_url": _linear_url,
            "context_fill_class": _context_fill_class,
            "supervisor": _supervisor_status(state),
        })

    @app.get("/partials/supervisor-badge", response_class=HTMLResponse)
    def partial_supervisor_badge(request: Request):
        state = _read_state()
        return templates.TemplateResponse("partials/supervisor_badge.html", {
            "request": request,
            "supervisor": _supervisor_status(state),
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
        # Try to get fresh data from the API; fall back to DB snapshot
        fresh = _refresh_and_get_usage()
        usage = fresh if fresh is not None else state.get("usage", {})
        dispatch_paused = state.get("dispatch_paused", False)
        dispatch_pause_reason = state.get("dispatch_pause_reason")
        # Use the dashboard's own refresh timestamp; fall back to DB snapshot
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
        project_pauses = state.get("project_pauses", {})
        return templates.TemplateResponse("partials/queue.html", {
            "request": request,
            "queue": queue,
            "project_pauses": project_pauses,
            "linear_url": _linear_url,
            "elapsed": _elapsed,
            "has_callbacks": app.state.on_pause is not None,
        })

    _EMPTY_TASK_AGGREGATES: dict = {
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "total_cost_usd": 0.0,
        "max_context_fill_pct": None,
        "extra_usage_cost_usd": 0.0,
    }

    def _enrich_tasks(
        tasks: list[dict], conn: sqlite3.Connection | None = None,
    ) -> list[dict]:
        """Add computed fields to task dicts.

        When *conn* is provided, also attaches aggregated token usage from
        stage_runs (total_cost_usd, max_context_fill_pct, etc.).
        """
        # Aggregate token data in a single query when possible
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
                    task["duration"] = _format_duration(
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
            return _enrich_tasks([dict(r) for r in rows], conn), total, total_pages
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
            "context_fill_class": _context_fill_class,
            "supervisor": _supervisor_status(_read_state()),
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
        return {
            "total_input_tokens": total_input,
            "total_output_tokens": total_output,
            "total_cost": total_cost,
            "extra_usage_cost": extra_usage_cost,
            "max_context_fill": max(fills) if fills else None,
        }

    @app.get("/task/{task_id}", response_class=HTMLResponse)
    def task_detail_page(request: Request, task_id: str):
        task = None
        stages: list[dict] = []
        events: list[dict] = []
        pipeline: list[dict] = []
        conn = _get_db()
        if conn:
            try:
                # Polymorphic lookup: try integer ID first, fall back to ticket ID
                task_row = None
                try:
                    int_id = int(task_id)
                    task_row = get_task(conn, int_id)
                except ValueError:
                    pass
                if task_row is None:
                    task_row = get_task_by_ticket(conn, task_id)
                if task_row is not None:
                    task = _enrich_tasks([dict(task_row)])[0]
                    db_task_id = task["id"]
                    stages = [dict(r) for r in get_stage_runs(conn, db_task_id)]
                    events = [dict(r) for r in get_events(
                        conn, task_id=db_task_id, limit=EVENT_LOG_LIMIT,
                    )]
                    # Events come newest-first from DB; reverse for chronological display
                    events.reverse()
                    pipeline = build_pipeline_state(
                        stages, task.get("status"),
                    )
            finally:
                conn.close()
        task_totals = _compute_task_totals(stages)
        return templates.TemplateResponse("task_detail.html", {
            "request": request,
            "task": task,
            "stages": stages,
            "events": events,
            "pipeline": pipeline,
            "task_totals": task_totals,
            "linear_url": _linear_url,
            "format_duration": _format_duration,
            "context_fill_class": _context_fill_class,
            "supervisor": _supervisor_status(_read_state()),
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
            "supervisor": _supervisor_status(state),
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
        "avg_turns": 0, "avg_review_iterations": 0.0,
        "avg_wall_time_seconds": 0, "success_rate": 0.0,
        "completed_today": 0, "completed_week": 0,
        "completed_month": 0, "failure_reasons": [],
        "total_input_tokens": 0, "total_output_tokens": 0,
        "total_cost_usd": 0.0, "extra_usage_cost_usd": 0.0,
        "avg_context_fill_pct": None,
        "tasks_over_80_pct_fill": 0,
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
            "supervisor": _supervisor_status(_read_state()),
        })

    # --- Workflow ---

    @app.get("/workflow", response_class=HTMLResponse)
    def workflow_page(request: Request):
        conn = _get_db()
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
                    # Determine loop-managed stages (not on happy path)
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
                            "name": s.name,
                            "executor_type": s.executor_type,
                            "identity": s.identity,
                            "timeout_minutes": s.timeout_minutes,
                            "max_turns": s.max_turns,
                            "result_parser": s.result_parser,
                        }
                        for s in pipeline.stages
                    ]

                    main_stages_list = [
                        {
                            "name": s.name,
                            "executor_type": s.executor_type,
                            "identity": s.identity,
                            "timeout_minutes": s.timeout_minutes,
                            "max_turns": s.max_turns,
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
                        # Identify the decision stage and fix stage
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
                            "name": loop.name,
                            "decision_stage": decision_stage,
                            "fix_stage_name": fix_stage_name,
                            "fix_stage": {
                                "name": fix_stage_obj.name,
                                "executor_type": fix_stage_obj.executor_type,
                                "identity": fix_stage_obj.identity,
                                "timeout_minutes": fix_stage_obj.timeout_minutes,
                                "max_turns": fix_stage_obj.max_turns,
                                "result_parser": fix_stage_obj.result_parser,
                            } if fix_stage_obj else None,
                            "max_iterations": eff_max,
                            "question": question,
                            "exit_condition": loop.exit_condition,
                        })

                    pipelines_data.append({
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
        return templates.TemplateResponse("workflow.html", {
            "request": request,
            "pipelines": pipelines_data,
            "active_page": "workflow",
            "supervisor": _supervisor_status(_read_state()),
        })

    # --- Pause / Resume API ---

    def _manual_pause_state(state: dict) -> str:
        """Determine the manual pause UI state from current state.

        Returns one of: "running", "pausing", "paused".
        """
        dispatch_paused = state.get("dispatch_paused", False)
        pause_reason = state.get("dispatch_pause_reason")
        if not dispatch_paused or pause_reason != "manual_pause":
            # Check if any slots are paused_manual (already paused even if
            # dispatch was resumed by usage threshold changes)
            slots = state.get("slots", [])
            has_manual_paused = any(s["status"] == "paused_manual" for s in slots)
            if has_manual_paused:
                return "paused"
            return "running"
        # Dispatch is paused for manual reason — check if workers are still busy
        slots = state.get("slots", [])
        has_busy = any(s["status"] == "busy" for s in slots)
        if has_busy:
            busy_count = sum(1 for s in slots if s["status"] == "busy")
            return f"pausing:{busy_count}"
        return "paused"

    @app.post("/api/pause")
    def api_pause():
        cb = app.state.on_pause
        if cb is None:
            return JSONResponse(
                {"error": "Pause not available (supervisor not connected)"},
                status_code=503,
            )
        cb()
        return JSONResponse({"status": "ok"})

    @app.post("/api/resume")
    def api_resume():
        cb = app.state.on_resume
        if cb is None:
            return JSONResponse(
                {"error": "Resume not available (supervisor not connected)"},
                status_code=503,
            )
        cb()
        return JSONResponse({"status": "ok"})

    @app.post("/api/project/pause")
    def api_project_pause(request: Request, project: str = "", reason: str = ""):
        """Pause dispatch for a specific project."""
        if not project:
            return JSONResponse({"error": "project is required"}, status_code=400)
        conn = None
        try:
            conn = init_db(app.state.db_path)
            save_project_pause_state(conn, project=project, paused=True, reason=reason or None)
            conn.commit()
            return JSONResponse({"status": "ok", "project": project, "paused": True})
        except Exception as exc:
            logger.warning("Failed to pause project %s: %s", project, exc)
            return JSONResponse({"error": str(exc)}, status_code=500)
        finally:
            if conn is not None:
                conn.close()

    @app.post("/api/project/resume")
    def api_project_resume(request: Request, project: str = ""):
        """Resume dispatch for a specific project."""
        if not project:
            return JSONResponse({"error": "project is required"}, status_code=400)
        conn = None
        try:
            conn = init_db(app.state.db_path)
            save_project_pause_state(conn, project=project, paused=False)
            conn.commit()
            return JSONResponse({"status": "ok", "project": project, "paused": False})
        except Exception as exc:
            logger.warning("Failed to resume project %s: %s", project, exc)
            return JSONResponse({"error": str(exc)}, status_code=500)
        finally:
            if conn is not None:
                conn.close()

    @app.get("/partials/supervisor-controls", response_class=HTMLResponse)
    def partial_supervisor_controls(request: Request):
        state = _read_state()
        pause_state = _manual_pause_state(state)
        busy_slots = [
            s for s in state.get("slots", []) if s["status"] == "busy"
        ] if pause_state.startswith("pausing") else []
        return templates.TemplateResponse("partials/supervisor_controls.html", {
            "request": request,
            "pause_state": pause_state,
            "busy_slots": busy_slots,
            "supervisor": _supervisor_status(state),
            "has_callbacks": app.state.on_pause is not None,
        })

    # --- Update banner ---

    _update_check_lock = threading.Lock()
    _last_update_check: dict = {"time": None, "commits_behind": 0}
    _UPDATE_CHECK_INTERVAL = 60  # seconds

    def _check_commits_behind() -> int:
        """Check how far behind origin/main we are, rate-limited."""
        import time as _time

        now = _time.monotonic()
        with _update_check_lock:
            last = _last_update_check["time"]
            if last is not None and now - last < _UPDATE_CHECK_INTERVAL:
                return _last_update_check["commits_behind"]

        try:
            count = commits_behind(env=app.state.git_env)
        except Exception:
            logger.warning("Update check failed", exc_info=True)
            count = 0

        with _update_check_lock:
            _last_update_check["time"] = now
            _last_update_check["commits_behind"] = count
        return count

    @app.get("/partials/update-banner", response_class=HTMLResponse)
    def partial_update_banner(request: Request):
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
        count = _check_commits_behind()
        return templates.TemplateResponse("partials/update_banner.html", {
            "request": request,
            "update_status": "idle",
            "commits_behind": count,
            "auto_restart": app.state.auto_restart,
        })

    @app.post("/api/update")
    def api_update():
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

    # --- Preflight / System Health ---

    # Actionable guidance for common preflight check failures
    _PREFLIGHT_GUIDANCE: dict[str, str] = {
        "git_repo": "Verify base_dir path in config and that git remote 'origin' is accessible",
        "linear_api": "Check linear.api_key in config and verify team/status names match your Linear workspace",
        "linear_status": "Check linear.api_key in config and verify team/status names match your Linear workspace",
        "identity_github_token": "Verify GitHub token is valid and the associated user has collaborator access to the repository",
        "identity_ssh_key": "Check SSH key path in config, verify file exists and has correct permissions (0600)",
        "identity_linear_key": "Verify the identity's Linear API key is valid",
        "database": "Check DB path permissions or schema version",
        "config_consistency": "Review config for duplicate slot IDs or invalid project settings",
        "credentials": "Verify Claude OAuth credentials are loaded correctly",
        "notifications_webhook": "Check the webhook URL in config is valid and reachable",
        "worktree_dirs": "Verify worktree parent directories exist and are writable",
        "identity_cross_validation": "Review identity config for inconsistent or partial credential sets",
    }

    def _get_preflight_data() -> dict:
        """Build preflight template context from supervisor callbacks."""
        getter = app.state.get_preflight_results
        degraded_getter = app.state.get_degraded
        results = getter() if getter else []
        degraded = degraded_getter() if degraded_getter else False
        checks = []
        for r in results:
            # Match guidance by prefix (e.g. "git_repo:myproject" -> "git_repo")
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

    @app.get("/partials/preflight-banner", response_class=HTMLResponse)
    def partial_preflight_banner(request: Request):
        data = _get_preflight_data()
        return templates.TemplateResponse("partials/preflight_banner.html", {
            "request": request,
            **data,
        })

    @app.get("/health", response_class=HTMLResponse)
    def health_page(request: Request):
        data = _get_preflight_data()
        return templates.TemplateResponse("health.html", {
            "request": request,
            "active_page": "health",
            **data,
        })

    @app.get("/partials/health-checks", response_class=HTMLResponse)
    def partial_health_checks(request: Request):
        data = _get_preflight_data()
        return templates.TemplateResponse("partials/health_checks.html", {
            "request": request,
            **data,
        })

    @app.get("/partials/health-badge", response_class=HTMLResponse)
    def partial_health_badge(request: Request):
        data = _get_preflight_data()
        return templates.TemplateResponse("partials/health_badge.html", {
            "request": request,
            **data,
        })

    @app.post("/api/rerun-preflight")
    def api_rerun_preflight():
        cb = app.state.on_rerun_preflight
        if cb is None:
            return JSONResponse(
                {"error": "Preflight re-run not available (supervisor not connected)"},
                status_code=503,
            )
        cb()
        return JSONResponse({"status": "ok"})

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
                "timeout_overrides": {
                    label: dict(stages)
                    for label, stages in cfg.agents.timeout_overrides.items()
                },
                "timeout_grace_seconds": cfg.agents.timeout_grace_seconds,
                "codex_reviewer_enabled": cfg.agents.codex_reviewer_enabled,
                "codex_reviewer_model": cfg.agents.codex_reviewer_model,
                "codex_reviewer_timeout_minutes": cfg.agents.codex_reviewer_timeout_minutes,
            },
            "usage_limits": {
                "enabled": cfg.usage_limits.enabled,
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
                "path": str(app.state.db_path),
            },
        }

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
                "enabled": cfg.usage_limits.enabled,
                "pause_five_hour_threshold": cfg.usage_limits.pause_five_hour_threshold,
                "pause_seven_day_threshold": cfg.usage_limits.pause_seven_day_threshold,
            },
            "agents": {
                "max_review_iterations": cfg.agents.max_review_iterations,
                "max_ci_retries": cfg.agents.max_ci_retries,
                "timeout_minutes": dict(cfg.agents.timeout_minutes),
                "timeout_overrides": {
                    label: dict(stages)
                    for label, stages in cfg.agents.timeout_overrides.items()
                },
                "timeout_grace_seconds": cfg.agents.timeout_grace_seconds,
                "codex_reviewer_enabled": cfg.agents.codex_reviewer_enabled,
                "codex_reviewer_model": cfg.agents.codex_reviewer_model,
                "codex_reviewer_timeout_minutes": cfg.agents.codex_reviewer_timeout_minutes,
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
            "full_config_values": _full_config_values(),
            "editable_fields": EDITABLE_FIELDS,
            "restart_required": app.state.restart_required,
            "supervisor": _supervisor_status(_read_state()),
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

    # --- Identity credentials management ---

    # Maps (role, field) to the env var name used in .env / config.yaml
    _IDENTITY_SECRET_FIELDS: dict[tuple[str, str], str] = {
        ("coder", "github_token"): "CODER_GITHUB_TOKEN",
        ("coder", "linear_api_key"): "CODER_LINEAR_API_KEY",
        ("reviewer", "github_token"): "REVIEWER_GITHUB_TOKEN",
        ("reviewer", "linear_api_key"): "REVIEWER_LINEAR_API_KEY",
    }

    # Non-secret fields written directly to config.yaml
    _IDENTITY_PLAIN_FIELDS: dict[str, set[str]] = {
        "coder": {"ssh_key_path", "git_author_name", "git_author_email"},
    }

    def _identity_status() -> dict:
        """Build identity status for template display."""
        cfg = app.state.botfarm_config
        if cfg is None:
            return {"configured": False}

        coder = cfg.identities.coder
        reviewer = cfg.identities.reviewer

        ssh_path = coder.ssh_key_path
        ssh_exists = False
        if ssh_path:
            try:
                ssh_exists = Path(ssh_path).expanduser().exists()
            except (OSError, ValueError):
                pass

        return {
            "configured": True,
            "coder": {
                "github_token": _mask_secret(coder.github_token),
                "github_token_set": bool(coder.github_token),
                "ssh_key_path": ssh_path,
                "ssh_key_exists": ssh_exists,
                "git_author_name": coder.git_author_name,
                "git_author_email": coder.git_author_email,
                "linear_api_key": _mask_secret(coder.linear_api_key),
                "linear_api_key_set": bool(coder.linear_api_key),
            },
            "reviewer": {
                "github_token": _mask_secret(reviewer.github_token),
                "github_token_set": bool(reviewer.github_token),
                "linear_api_key": _mask_secret(reviewer.linear_api_key),
                "linear_api_key_set": bool(reviewer.linear_api_key),
            },
        }

    def _resolve_env_path() -> Path:
        """Resolve the .env file path from config source dir."""
        cfg = app.state.botfarm_config
        if cfg and cfg.source_path:
            return Path(cfg.source_path).parent / ".env"
        return Path.home() / ".botfarm" / ".env"

    def _write_env_file(env_path: Path, data: dict[str, str]) -> None:
        """Write key-value pairs to a .env file, preserving comments."""
        lines: list[str] = []
        existing_keys: set[str] = set()

        if env_path.exists():
            for line in env_path.read_text().splitlines():
                stripped = line.strip()
                if stripped and not stripped.startswith("#") and "=" in stripped:
                    key = stripped.partition("=")[0].strip()
                    if key in data:
                        lines.append(f'{key}="{data[key]}"')
                        existing_keys.add(key)
                        continue
                lines.append(line)

        # Append new keys not already in the file
        for key, value in data.items():
            if key not in existing_keys:
                lines.append(f'{key}="{value}"')

        env_path.parent.mkdir(parents=True, exist_ok=True)
        content = "\n".join(lines) + "\n"
        # Atomic write: temp file + os.replace to avoid partial writes
        import tempfile
        import os
        fd, tmp_path = tempfile.mkstemp(
            dir=str(env_path.parent), suffix=".env.tmp",
        )
        try:
            with os.fdopen(fd, "w") as f:
                f.write(content)
            os.replace(tmp_path, str(env_path))
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    @app.get("/identities", response_class=HTMLResponse)
    def identities_page(request: Request):
        return templates.TemplateResponse("identities.html", {
            "request": request,
            "identity": _identity_status(),
            "supervisor": _supervisor_status(_read_state()),
        })

    @app.post("/identities", response_class=HTMLResponse)
    async def identities_update(request: Request):
        cfg = app.state.botfarm_config
        if cfg is None:
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                "Identity editing is not available.</div>",
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

        # Validate: only known roles and fields
        errors: list[str] = []
        allowed_roles = {"coder", "reviewer"}
        for role, fields in updates.items():
            if role not in allowed_roles:
                errors.append(f"Unknown role: {role!r}")
                continue
            if not isinstance(fields, dict):
                errors.append(f"'{role}' must be a mapping")
                continue
            for field, value in fields.items():
                if not isinstance(value, str):
                    errors.append(f"'{role}.{field}' must be a string")
                    continue
                if "\n" in value or "\r" in value:
                    errors.append(f"'{role}.{field}' must not contain newlines")
                    continue
                is_secret = (role, field) in _IDENTITY_SECRET_FIELDS
                is_plain = field in _IDENTITY_PLAIN_FIELDS.get(role, set())
                if not is_secret and not is_plain:
                    errors.append(f"'{role}.{field}' is not an editable identity field")

        if errors:
            error_html = "".join(f"<li>{html.escape(e)}</li>" for e in errors)
            return HTMLResponse(
                '<div class="config-feedback error" role="alert">'
                f"<strong>Validation errors:</strong><ul>{error_html}</ul></div>",
                status_code=422,
            )

        config_path = Path(cfg.source_path) if cfg.source_path else None
        env_path = _resolve_env_path()

        # Separate secret vs plain-text updates
        env_updates: dict[str, str] = {}
        yaml_updates: dict[str, dict[str, str]] = {}  # role -> {field: value}
        yaml_env_refs: dict[str, dict[str, str]] = {}  # role -> {field: ${VAR}}

        for role, fields in updates.items():
            for field, value in fields.items():
                if (role, field) in _IDENTITY_SECRET_FIELDS:
                    env_var = _IDENTITY_SECRET_FIELDS[(role, field)]
                    env_updates[env_var] = value
                    yaml_env_refs.setdefault(role, {})[field] = f"${{{env_var}}}"
                else:
                    yaml_updates.setdefault(role, {})[field] = value

        # Write secrets to .env
        if env_updates:
            try:
                _write_env_file(env_path, env_updates)
            except OSError as exc:
                return HTMLResponse(
                    '<div class="config-feedback error" role="alert">'
                    f"Failed to write .env file: {html.escape(str(exc))}</div>",
                    status_code=500,
                )

        # Write config.yaml: plain fields as values, secret fields as ${VAR} refs
        if (yaml_updates or yaml_env_refs) and config_path and config_path.exists():
            try:
                raw = config_path.read_text()
                data = yaml.safe_load(raw)
                if not isinstance(data, dict):
                    data = {}

                if "identities" not in data or not isinstance(data.get("identities"), dict):
                    data["identities"] = {}

                for role in allowed_roles:
                    plain = yaml_updates.get(role, {})
                    refs = yaml_env_refs.get(role, {})
                    if not plain and not refs:
                        continue
                    if role not in data["identities"] or not isinstance(data["identities"].get(role), dict):
                        data["identities"][role] = {}
                    data["identities"][role].update(plain)
                    data["identities"][role].update(refs)

                write_yaml_atomic(config_path, data)
            except Exception:
                logger.exception("Failed to write identity config to YAML")
                if env_updates:
                    return HTMLResponse(
                        '<div class="config-feedback warning" role="alert">'
                        "Secrets saved to .env but failed to update config.yaml. "
                        "You may need to manually add env var references.</div>",
                        status_code=200,
                    )
                return HTMLResponse(
                    '<div class="config-feedback error" role="alert">'
                    "Failed to save identity config.</div>",
                    status_code=500,
                )

        app.state.restart_required = True
        return HTMLResponse(
            '<div class="config-feedback success" role="alert">'
            "Identity credentials saved. Restart required to apply changes.</div>",
            status_code=200,
        )

    # --- Log viewer ---

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

    def _find_log_files(ticket_id: str, stage: str | None = None) -> list[Path]:
        """Find log files for a ticket, optionally filtered by stage.

        Returns log files sorted by modification time (newest first).
        """
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
                # Match files like "implement-20260226-123456.log" or
                # "implement-iter2-20260226-123456.log"
                if not f.name.startswith(stage):
                    continue
                # Make sure we don't match "implement" when looking for "fix"
                # by checking the character after the stage name
                rest = f.name[len(stage):]
                if rest and rest[0] not in ("-", "."):
                    continue
            files.append(f)
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return files

    def _find_latest_log(ticket_id: str, stage: str) -> Path | None:
        """Find the most recent log file for a ticket + stage."""
        files = _find_log_files(ticket_id, stage)
        return files[0] if files else None

    _ALL_LOG_STAGES = list(STAGES) + ["ci_fix", "codex_review"]

    def _available_stages_with_logs(ticket_id: str) -> list[str]:
        """Return stage names that have log files, in canonical order."""
        files = _find_log_files(ticket_id)
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

    def _is_stage_active(ticket_id: str, stage: str) -> bool:
        """Check if a stage is currently running for this ticket."""
        conn = _get_db()
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

    def _resolve_task(task_id: str) -> tuple[dict | None, str]:
        """Resolve task_id (numeric or ticket_id) to (task_dict, ticket_id)."""
        conn = _get_db()
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

    @app.get("/task/{task_id}/logs", response_class=HTMLResponse)
    def log_viewer_page(request: Request, task_id: str):
        """Log viewer page — shows available stages, redirects to latest."""
        task, ticket_id = _resolve_task(task_id)

        available = _available_stages_with_logs(ticket_id)
        if not available:
            return templates.TemplateResponse("log_viewer.html", {
                "request": request,
                "task": task,
                "ticket_id": ticket_id,
                "stages_with_logs": [],
                "current_stage": None,
                "log_content": None,
                "is_live": False,
                "linear_url": _linear_url,
                "supervisor": _supervisor_status(_read_state()),
            })

        # Default to the most recent active stage, or the last available
        active_stage = None
        for s in reversed(available):
            if _is_stage_active(ticket_id, s):
                active_stage = s
                break
        default_stage = active_stage or available[-1]
        is_live = _is_stage_active(ticket_id, default_stage)

        # Load content for completed stages
        log_content = None
        if not is_live:
            log_file = _find_latest_log(ticket_id, default_stage)
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
            "linear_url": _linear_url,
            "supervisor": _supervisor_status(_read_state()),
        })

    @app.get("/task/{task_id}/logs/{stage}", response_class=HTMLResponse)
    def log_viewer_stage_page(request: Request, task_id: str, stage: str):
        """Log viewer page for a specific stage."""
        task, ticket_id = _resolve_task(task_id)

        available = _available_stages_with_logs(ticket_id)
        is_live = _is_stage_active(ticket_id, stage)

        # For completed stages, load the full log content for static display
        log_content = None
        if not is_live:
            log_file = _find_latest_log(ticket_id, stage)
            if log_file:
                log_content = _read_log_file(log_file) or None

        return templates.TemplateResponse("log_viewer.html", {
            "request": request,
            "task": task,
            "ticket_id": ticket_id,
            "stages_with_logs": available,
            "current_stage": stage,
            "log_content": log_content,
            "is_live": is_live,
            "linear_url": _linear_url,
            "supervisor": _supervisor_status(_read_state()),
        })

    @app.get("/api/logs/{ticket_id}/{stage}/stream")
    async def stream_log(ticket_id: str, stage: str):
        """SSE endpoint that tails the active log file for a running stage.

        Each NDJSON line is parsed and transformed into a human-readable
        event.  The SSE ``event`` field reflects the message type
        (``assistant``, ``tool_use``, ``tool_result``, ``result``,
        ``system``, or ``log`` for non-JSON lines) so the client can
        style each category differently.
        """
        log_file = _find_latest_log(ticket_id, stage)
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
                            # Check if the stage is still active
                            if not await asyncio.to_thread(
                                _is_stage_active, ticket_id, stage
                            ):
                                yield {"event": "done", "data": ""}
                                break
                            await asyncio.sleep(0.5)
            except OSError:
                yield {"event": "error", "data": "Failed to read log file"}

        return EventSourceResponse(event_generator())

    @app.get("/api/logs/{ticket_id}/{stage}/content")
    def get_log_content(ticket_id: str, stage: str):
        """Return the full log content for a completed stage."""
        log_file = _find_latest_log(ticket_id, stage)
        if log_file is None:
            return PlainTextResponse("No log file found", status_code=404)
        content = _read_log_file(log_file)
        if not content:
            return PlainTextResponse("Failed to read log file", status_code=500)
        return PlainTextResponse(content)

    return app


def start_dashboard(
    config: DashboardConfig,
    *,
    db_path: str | Path,
    linear_workspace: str = "",
    botfarm_config: BotfarmConfig | None = None,
    state_file: str | Path | None = None,
    logs_dir: str | Path | None = None,
    on_pause: Callable[[], None] | None = None,
    on_resume: Callable[[], None] | None = None,
    on_update: Callable[[], None] | None = None,
    on_rerun_preflight: Callable[[], None] | None = None,
    get_preflight_results: Callable[[], list] | None = None,
    get_degraded: Callable[[], bool] | None = None,
    update_failed_event: threading.Event | None = None,
    git_env: dict[str, str] | None = None,
    auto_restart: bool = True,
) -> threading.Thread | None:
    """Start the dashboard server in a background daemon thread.

    Returns the thread if started, or None if the dashboard is disabled.
    """
    if not config.enabled:
        return None

    app = create_app(
        db_path=db_path,
        linear_workspace=linear_workspace,
        botfarm_config=botfarm_config,
        logs_dir=logs_dir,
        on_pause=on_pause,
        on_resume=on_resume,
        on_update=on_update,
        on_rerun_preflight=on_rerun_preflight,
        get_preflight_results=get_preflight_results,
        get_degraded=get_degraded,
        update_failed_event=update_failed_event,
        git_env=git_env,
        auto_restart=auto_restart,
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
