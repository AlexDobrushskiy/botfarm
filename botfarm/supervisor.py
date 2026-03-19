"""Supervisor daemon: poll Linear, dispatch workers, manage slots.

The supervisor runs as a long-lived process. Each iteration it:
1. Reconciles slot state against reality (dead PIDs → failed).
2. Handles completed/failed workers (update Linear, free slots).
3. Polls Linear for Todo tickets.
4. Dispatches workers to free slots.
5. Sleeps for the configured poll interval.

Workers run in isolated subprocesses — a single worker failure never
brings down the supervisor.

This module was split into four files for maintainability:

* ``supervisor.py`` — Supervisor class core (init, main loop, tick,
  dispatch, capacity, update, logging)
* ``supervisor_workers.py`` — Worker subprocess entry point, dataclasses,
  WorkerLifecycleManager, PauseResumeManager
* ``supervisor_recovery.py`` — RecoveryMixin (startup recovery,
  reconciliation, PR status checking)
* ``supervisor_ops.py`` — OperationsMixin (slot cleanup, stop-slot,
  comments, notifications, limit handling)
"""

from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
import os
import shutil
import signal
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

from botfarm.config import BotfarmConfig, ProjectConfig, resolve_stage_timeout, sync_agent_config_to_db
from botfarm.db import (
    get_task,
    init_db,
    insert_event,
    read_runtime_config,
    resolve_db_path,
    save_capacity_state,
    save_queue_entries,
)
from botfarm.bugtracker import BugtrackerClient, BugtrackerPoller, create_client, create_pollers
from botfarm.notifications import Notifier
from botfarm.preflight import CheckResult, log_preflight_summary, repair_core_bare, run_preflight_checks
from botfarm.slots import SlotManager, SlotState, _is_pid_alive
from botfarm.codex_usage import CodexUsagePoller
from botfarm.devserver import DevServerManager
from botfarm.usage import DEFAULT_PAUSE_5H_THRESHOLD, DEFAULT_PAUSE_7D_THRESHOLD, UsagePoller
from botfarm.worker import STAGES, PipelineResult, build_git_env, run_pipeline

# Import mixin classes
from botfarm.supervisor_ops import OperationsMixin
from botfarm.supervisor_recovery import RecoveryMixin
from botfarm.supervisor_workers import (
    FAILURE_CATEGORIES,
    PauseResumeManager,
    PriorContext,
    WorkerLifecycleManager,
    _StallInfo,
    _WorkerResult,
    StopSlotResult,
    _check_limit_hit,
    _classify_failure,
    _collect_descendant_pids,
    _kill_descendant_sessions,
    _kill_pids,
    _kill_process_tree,
    _setup_worker_logging,
    _truncate_for_comment,
    _worker_entry,
    build_prior_context,
)

logger = logging.getLogger(__name__)

DEFAULT_LOG_DIR = Path.home() / ".botfarm" / "logs"

# Re-export for backward compatibility — other modules import from here:
#   from botfarm.supervisor import (
#       DEFAULT_LOG_DIR, StopSlotResult, Supervisor,
#       _StallInfo, _WorkerResult, _check_limit_hit,
#       _setup_worker_logging, _worker_entry, setup_logging,
#   )
__all__ = [
    "DEFAULT_LOG_DIR",
    "DEFAULT_PAUSE_5H_THRESHOLD",
    "DEFAULT_PAUSE_7D_THRESHOLD",
    "FAILURE_CATEGORIES",
    "PipelineResult",
    "STAGES",
    "StopSlotResult",
    "Supervisor",
    "_StallInfo",
    "_WorkerResult",
    "_check_limit_hit",
    "_classify_failure",
    "_collect_descendant_pids",
    "_is_pid_alive",
    "_kill_descendant_sessions",
    "_kill_pids",
    "_kill_process_tree",
    "_setup_worker_logging",
    "_truncate_for_comment",
    "_worker_entry",
    "get_task",
    "read_runtime_config",
    "resolve_stage_timeout",
    "run_pipeline",
    "build_prior_context",
    "setup_logging",
]


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------


class Supervisor(RecoveryMixin, OperationsMixin):
    """Long-lived daemon that dispatches and monitors worker subprocesses."""

    def __init__(
        self,
        config: BotfarmConfig,
        *,
        log_dir: str | Path | None = None,
        auto_restart: bool = True,
    ) -> None:
        self._config = config
        self._log_dir = Path(log_dir or DEFAULT_LOG_DIR).expanduser()
        self._shutdown_requested = False
        self._auto_restart = auto_restart

        # Lookup: project name -> ProjectConfig
        self._projects: dict[str, ProjectConfig] = {
            p.name: p for p in config.projects
        }

        # Bugtracker pollers keyed by project name
        pollers = create_pollers(config)
        self._pollers: dict[str, BugtrackerPoller] = {
            p.project_name: p for p in pollers
        }

        # Database — path comes from BOTFARM_DB_PATH env var (loaded from .env)
        db_path = resolve_db_path()
        self._db_path = str(db_path)
        self._conn: sqlite3.Connection = init_db(db_path, allow_migration=True)

        # Seed runtime_config table with current agent settings
        sync_agent_config_to_db(self._conn, config.agents)

        # Slot manager (shares the supervisor's DB connection)
        self._slot_manager = SlotManager(
            db_path=self._db_path,
            conn=self._conn,
        )
        for project in config.projects:
            for sid in project.slots:
                self._slot_manager.register_slot(project.name, sid)
        self._slot_manager.load()

        # Webhook notifier (initialized early — used by UsagePoller)
        self._notifier = Notifier(config.notifications)
        self._was_busy = False  # Track busy→idle transition for notifications

        # Usage poller
        self._usage_poller = UsagePoller(
            poll_interval=config.usage_limits.poll_interval_seconds,
            notifier=self._notifier,
        )
        self._usage_poller.restore_backoff_state(self._conn)

        # Codex (OpenAI) usage poller — optional, no-op if not configured
        self._codex_usage_poller = CodexUsagePoller(config=config.codex_usage)

        # Bugtracker client for capacity monitoring (uses owner API key)
        self._bugtracker_client = create_client(config)

        # Capacity monitoring state
        self._capacity_level = "normal"  # normal | warning | critical | blocked

        # Track child processes: (project, slot_id) -> Process
        self._workers: dict[tuple[str, int], multiprocessing.Process] = {}

        # Per-worker pause events: (project, slot_id) -> Event
        self._pause_events: dict[tuple[str, int], multiprocessing.Event] = {}

        # Manual pause/resume signals (set by dashboard thread, read by supervisor)
        self._manual_pause_event = threading.Event()
        self._manual_resume_event = threading.Event()
        self._manual_pause_requested = False

        # Update-and-restart signal (set by dashboard thread, read by supervisor)
        self._update_event = threading.Event()
        self._update_failed_event = threading.Event()
        self._exit_code = 0

        # Wake event — set by interactive requests to break out of _sleep() early
        self._wake_event = threading.Event()

        # Stop-slot requests from the dashboard thread.
        self._stop_slot_requests: list[tuple[str, int, str | None]] = []
        self._stop_slot_lock = threading.Lock()

        # Add-slot requests from the dashboard thread.
        self._add_slot_requests: list[str] = []
        self._add_slot_lock = threading.Lock()

        # Add-project requests from the dashboard thread.
        self._add_project_requests: list[str] = []
        self._add_project_lock = threading.Lock()

        # Queue for worker results — workers send _WorkerResult here
        self._result_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Tracks whether the supervisor started in paused mode
        self._startup_paused: bool = False

        # Timestamp of last ticket log cleanup (runs at most once per hour)
        self._last_log_cleanup: float = 0.0

        # Usage stall detection: (project, slot_id) -> _StallInfo
        self._stall_tracking: dict[tuple[str, int], _StallInfo] = {}

        # Transient result text from workers
        self._pending_result_texts: dict[tuple[str, int], str] = {}

        # Degraded mode: preflight checks failed
        self._degraded = False
        self._preflight_results: list[CheckResult] = []
        self._preflight_lock = threading.Lock()
        self._rerun_preflight_event = threading.Event()
        self._last_preflight_rerun: float = 0.0

        # Environment overrides for supervisor-level git/gh commands
        self._git_env = build_git_env(config.identities)

        # Coder bugtracker self-assignment
        coder_key = config.identities.coder.tracker_api_key
        if coder_key:
            try:
                coder_client = create_client(config, api_key=coder_key)
                self._coder_viewer_id: str | None = coder_client.get_viewer_id()
                self._coder_tracker: BugtrackerClient | None = coder_client
                logger.info("Cached coder bugtracker viewer ID: %s", self._coder_viewer_id)
            except Exception:
                logger.warning("Failed to resolve coder viewer ID — auto-assignment disabled")
                self._coder_viewer_id = None
                self._coder_tracker: BugtrackerClient | None = None
        else:
            self._coder_viewer_id: str | None = None
            self._coder_tracker: BugtrackerClient | None = None

        # Sub-managers
        self._worker_mgr = WorkerLifecycleManager(self)
        self._pause_resume_mgr = PauseResumeManager(self)

        # Dev server manager — manages dev server processes for web projects
        self._devserver_mgr = DevServerManager(
            log_dir=self._log_dir / "devserver",
        )
        for project in config.projects:
            self._devserver_mgr.register_project(project)
        self._devserver_tick_count = 0

    @property
    def slot_manager(self) -> SlotManager:
        return self._slot_manager

    @property
    def devserver_manager(self) -> DevServerManager:
        return self._devserver_mgr

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _handle_signal(self, signum: int, _frame: FrameType | None) -> None:
        sig_name = signal.Signals(signum).name
        logger.info("Received %s — requesting graceful shutdown", sig_name)
        self._shutdown_requested = True

    def install_signal_handlers(self) -> None:
        """Install SIGTERM/SIGINT handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Run the supervisor main loop until shutdown is requested.

        Returns an exit code. ``42`` means "restart after update".
        """
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self.install_signal_handlers()

        logger.info(
            "Supervisor starting — %d project(s), %d slot(s)",
            len(self._config.projects),
            len(self._slot_manager.all_slots()),
        )
        insert_event(
            self._conn,
            event_type="supervisor_start",
            detail=f"projects={len(self._config.projects)}, "
            f"slots={len(self._slot_manager.all_slots())}",
        )
        self._conn.commit()

        # Start dashboard BEFORE preflight
        self._dashboard_thread = None
        if self._config.dashboard.enabled:
            from botfarm.dashboard import start_dashboard
            self._dashboard_thread = start_dashboard(
                self._config.dashboard,
                db_path=self._db_path,
                workspace=self._config.bugtracker.workspace,
                botfarm_config=self._config,
                logs_dir=self._log_dir,
                on_pause=self.request_pause,
                on_resume=self.request_resume,
                on_update=self.request_update,
                on_rerun_preflight=self.request_rerun_preflight,
                on_stop_slot=self.request_stop_slot,
                on_add_slot=self.request_add_slot,
                on_add_project=self.request_add_project,
                get_preflight_results=self.get_preflight_results,
                get_degraded=lambda: self.degraded,
                update_failed_event=self._update_failed_event,
                git_env=self._git_env,
                auto_restart=self._auto_restart,
                devserver_manager=self._devserver_mgr,
            )

        # Auto-repair core.bare=true corruption before preflight
        repaired = repair_core_bare(self._config)
        if repaired:
            insert_event(
                self._conn,
                event_type="core_bare_repaired",
                detail=f"projects={','.join(repaired)}",
            )
            self._conn.commit()

        # Run pre-flight health checks
        preflight_results = run_preflight_checks(self._config, env=self._git_env)
        preflight_ok = log_preflight_summary(preflight_results)

        with self._preflight_lock:
            self._preflight_results = preflight_results

        if not preflight_ok:
            insert_event(
                self._conn,
                event_type="preflight_failed",
                detail="; ".join(
                    f"{r.name}: {r.message}"
                    for r in preflight_results
                    if not r.passed and r.critical
                ),
            )
            self._conn.commit()
            self._degraded = True
            self._last_preflight_rerun = time.monotonic()
            logger.warning(
                "Entering degraded mode — dashboard is running, "
                "dispatch paused until preflight checks pass"
            )
        else:
            self._recover_on_startup()
            self._apply_start_paused()

        # Initial usage poll
        if not self._degraded:
            try:
                self._usage_poller.force_poll(self._conn)
            except Exception:
                logger.exception("Initial usage poll failed — continuing without usage data")

        try:
            while not self._shutdown_requested:
                if self._degraded:
                    self._tick_degraded()
                else:
                    self._tick()
                if not self._shutdown_requested:
                    self._sleep(self._config.bugtracker.poll_interval_seconds)
        finally:
            self._shutdown()

        return self._exit_code

    # Interval between automatic preflight re-runs in degraded mode (seconds)
    _PREFLIGHT_RERUN_INTERVAL = 30

    def _tick_degraded(self) -> None:
        """Tick while in degraded mode — no dispatch, just re-run preflight."""
        self._handle_stop_requests()

        manual_rerun = self._rerun_preflight_event.is_set()
        if manual_rerun:
            self._rerun_preflight_event.clear()

        now = time.monotonic()
        elapsed = now - self._last_preflight_rerun
        if manual_rerun or elapsed >= self._PREFLIGHT_RERUN_INTERVAL:
            self._rerun_preflight()

    def _rerun_preflight(self) -> None:
        """Re-run preflight checks and transition out of degraded mode on success."""
        logger.info("Re-running preflight checks…")
        results = run_preflight_checks(self._config, env=self._git_env)
        ok = log_preflight_summary(results)
        self._last_preflight_rerun = time.monotonic()

        with self._preflight_lock:
            self._preflight_results = results

        if ok:
            logger.info(
                "All preflight checks passed — transitioning from degraded to normal mode"
            )
            insert_event(
                self._conn,
                event_type="preflight_recovered",
                detail="all critical checks now pass",
            )
            self._conn.commit()
            self._degraded = False
            self._recover_on_startup()
            self._apply_start_paused()

            try:
                self._usage_poller.force_poll(self._conn)
            except Exception:
                logger.exception("Usage poll after recovery failed — continuing without usage data")
        else:
            logger.warning(
                "Preflight checks still failing — remaining in degraded mode"
            )

    def get_preflight_results(self) -> list[CheckResult]:
        """Return the latest preflight results (thread-safe, for dashboard)."""
        with self._preflight_lock:
            return list(self._preflight_results)

    @property
    def degraded(self) -> bool:
        """Whether the supervisor is in degraded mode."""
        return self._degraded

    def _handle_preflight_rerun(self) -> None:
        """Handle manual preflight rerun request in normal mode."""
        if not self._rerun_preflight_event.is_set():
            return
        self._rerun_preflight_event.clear()
        logger.info("Re-running preflight checks (manual request)…")
        results = run_preflight_checks(self._config, env=self._git_env)
        log_preflight_summary(results)
        self._last_preflight_rerun = time.monotonic()

        with self._preflight_lock:
            self._preflight_results = results

    def _tick(self) -> None:
        """One iteration of the supervisor loop."""
        # Merge worker-written stage updates from disk before any state writes
        self._slot_manager.refresh_stages_from_disk()
        # Reload per-project pause state (may be changed by CLI or dashboard)
        self._slot_manager.refresh_project_pauses()
        # Reload global dispatch state (CLI `botfarm resume` may clear start_paused)
        if self._slot_manager.refresh_dispatch_state():
            self._startup_paused = False
            logger.info("start_paused cleared externally (CLI resume)")
            insert_event(
                self._conn,
                event_type="start_paused_cleared",
                detail="cleared by CLI resume",
            )
            self._conn.commit()

        for phase in (
            self._reconcile_workers,
            self._handle_stop_requests,
            self._handle_add_slot_requests,
            self._handle_add_project_requests,
            self._handle_manual_pause_resume,
            self._handle_update_request,
            self._handle_preflight_rerun,
            self._check_timeouts,
            self._handle_finished_slots,
            self._handle_paused_slots,
            self._poll_usage,
            self._poll_capacity,
            self._poll_and_dispatch,
            self._cleanup_old_ticket_logs,
            self._check_devservers,
            self._maybe_create_refactoring_analysis_ticket,
            self._maybe_send_daily_summary,
        ):
            try:
                phase()
            except Exception:
                logger.exception("%s failed", getattr(phase, "__name__", repr(phase)))

        # Always update heartbeat
        self._slot_manager.save()

        self._log_tick_summary()

    # ------------------------------------------------------------------
    # Update handling
    # ------------------------------------------------------------------

    def _handle_update_request(self) -> None:
        """Check for update request and execute update-and-restart flow."""
        if not self._update_event.is_set():
            return

        from botfarm.git_update import UPDATE_EXIT_CODE, pull_and_install

        # Step 1: Pause dispatch
        if not self._slot_manager.dispatch_paused:
            logger.info("Update requested — pausing dispatch for update")
            self._slot_manager.set_dispatch_paused(True, "update_in_progress")
            insert_event(
                self._conn,
                event_type="update_started",
                detail="pausing workers for update",
            )
            self._conn.commit()
        elif self._slot_manager.dispatch_pause_reason != "update_in_progress":
            prev = self._slot_manager.dispatch_pause_reason
            logger.info(
                "Update requested — taking over existing pause (was: %s)", prev,
            )
            self._slot_manager.set_dispatch_paused(True, "update_in_progress")
            insert_event(
                self._conn,
                event_type="update_started",
                detail=f"taking over existing pause (was: {prev})",
            )
            self._conn.commit()

        # Signal all active workers to pause after current stage
        for key, proc in self._workers.items():
            if proc.is_alive():
                evt = self._pause_events.get(key)
                if evt is not None:
                    evt.set()

        # Step 2: Check if all workers are idle
        has_busy = any(
            s.status == "busy"
            for s in self._slot_manager.all_slots()
        )
        if has_busy:
            return  # Wait for next tick

        # Step 3: All workers idle — run update
        logger.info("All workers idle — pulling latest code")
        insert_event(
            self._conn,
            event_type="update_pulling",
            detail="git pull origin main && pip install -e .",
        )
        self._conn.commit()

        if not pull_and_install(env=self._git_env):
            logger.error("Update failed — aborting restart")
            insert_event(
                self._conn,
                event_type="update_failed",
                detail="git pull or pip install failed",
            )
            self._conn.commit()
            self._update_event.clear()
            self._update_failed_event.set()
            if self._startup_paused:
                self._slot_manager.set_dispatch_paused(True, "start_paused")
                logger.info("Update failed — restoring start_paused state")
            else:
                self._slot_manager.set_dispatch_paused(False)
            return

        # Step 4: Exit with special code for restart
        logger.info("Update complete — exiting with code %d for restart", UPDATE_EXIT_CODE)
        insert_event(
            self._conn,
            event_type="update_complete",
            detail=f"exiting with code {UPDATE_EXIT_CODE}",
        )
        self._conn.commit()
        self._exit_code = UPDATE_EXIT_CODE
        self._shutdown_requested = True

    # ------------------------------------------------------------------
    # Usage polling
    # ------------------------------------------------------------------

    def _poll_usage(self) -> None:
        """Poll the usage API and update slot manager state."""
        # Sync poll interval from config (may be changed at runtime via dashboard)
        self._usage_poller.poll_interval = self._config.usage_limits.poll_interval_seconds
        state = self._usage_poller.poll(self._conn)
        if self._usage_poller.last_polled_fresh:
            self._slot_manager.set_usage(state.to_dict())
        self._codex_usage_poller.poll(self._conn)
        self._check_usage_stalls()

    def _check_usage_stalls(self) -> None:
        """Warn if a busy worker's usage% hasn't moved since dispatch."""
        self._pause_resume_mgr.check_usage_stalls()

    # ------------------------------------------------------------------
    # Capacity polling
    # ------------------------------------------------------------------

    def _compute_capacity_level(self, utilization: float) -> str:
        """Map a utilization ratio to a capacity level string."""
        cap = self._config.bugtracker.capacity_monitoring
        if utilization >= cap.pause_threshold:
            return "blocked"
        if utilization >= cap.critical_threshold:
            return "critical"
        if utilization >= cap.warning_threshold:
            return "warning"
        return "normal"

    def _poll_capacity(self) -> None:
        """Poll Linear issue count and auto-pause/resume dispatch at thresholds."""
        cap_config = self._config.bugtracker.capacity_monitoring
        if not cap_config.enabled:
            return

        result = self._bugtracker_client.count_active_issues()
        if result is None:
            logger.warning("Capacity poll failed — skipping this tick")
            return

        limit = self._config.bugtracker.issue_limit
        checked_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        save_capacity_state(
            self._conn,
            issue_count=result.total,
            limit=limit,
            by_project=result.by_project,
            checked_at=checked_at,
        )
        self._conn.commit()

        utilization = result.total / limit if limit > 0 else 0.0

        # Apply hysteresis
        if self._capacity_level == "blocked" and utilization >= cap_config.resume_threshold:
            new_level = "blocked"
        else:
            new_level = self._compute_capacity_level(utilization)

        old_level = self._capacity_level
        if new_level == old_level:
            return

        detail = (
            f"{result.total}/{limit} issues "
            f"({utilization * 100:.1f}%)"
        )

        percentage = utilization * 100

        if new_level == "blocked":
            logger.warning(
                "Linear capacity %s — auto-pausing dispatch", detail,
            )
            insert_event(
                self._conn, event_type="capacity_blocked", detail=detail,
            )
            self._conn.commit()
            self._slot_manager.set_dispatch_paused(True, "capacity_blocked")
            self._notifier.notify_capacity_blocked(
                count=result.total, limit=limit, percentage=percentage,
            )

        elif new_level == "critical":
            logger.warning("Linear capacity %s — critical", detail)
            insert_event(
                self._conn, event_type="capacity_critical", detail=detail,
            )
            self._conn.commit()
            self._notifier.notify_capacity_critical(
                count=result.total, limit=limit, percentage=percentage,
            )

        elif new_level == "warning":
            logger.warning("Linear capacity %s — approaching limits", detail)
            insert_event(
                self._conn, event_type="capacity_warning", detail=detail,
            )
            self._conn.commit()
            self._notifier.notify_capacity_warning(
                count=result.total, limit=limit, percentage=percentage,
            )

        # Clearing from blocked state — resume dispatch
        if old_level == "blocked" and new_level != "blocked":
            logger.info("Linear capacity %s — resuming dispatch", detail)
            insert_event(
                self._conn, event_type="capacity_cleared", detail=detail,
            )
            self._conn.commit()
            if self._slot_manager.dispatch_pause_reason == "capacity_blocked":
                if self._startup_paused:
                    self._slot_manager.set_dispatch_paused(True, "start_paused")
                    logger.info("Capacity cleared — restoring start_paused state")
                else:
                    self._slot_manager.set_dispatch_paused(False)
            self._notifier.notify_capacity_cleared(
                count=result.total, limit=limit, percentage=percentage,
            )

        self._capacity_level = new_level

    # ------------------------------------------------------------------
    # Polling and dispatch
    # ------------------------------------------------------------------

    def _poll_and_dispatch(self) -> None:
        """Poll each project for tickets and dispatch workers to free slots."""
        thresholds = self._config.usage_limits
        should_pause, reason = self._usage_poller.state.should_pause_with_thresholds(
            five_hour_threshold=thresholds.pause_five_hour_threshold,
            seven_day_threshold=thresholds.pause_seven_day_threshold,
            enabled=thresholds.enabled,
        )

        # Check Codex budget threshold
        if not should_pause:
            cu = self._config.codex_usage
            should_pause, reason = self._codex_usage_poller.state.should_pause(
                primary_threshold=cu.pause_primary_threshold,
                secondary_threshold=cu.pause_secondary_threshold,
                enabled=cu.enabled,
            )

        if should_pause:
            if not self._slot_manager.dispatch_paused:
                logger.warning("Dispatch paused: %s", reason)
                insert_event(
                    self._conn,
                    event_type="dispatch_paused",
                    detail=reason,
                )
                self._conn.commit()
                self._slot_manager.set_dispatch_paused(True, reason)
                self._notifier.notify_limit_hit(reason=reason)
            return

        if self._slot_manager.dispatch_paused:
            if self._slot_manager.dispatch_pause_reason in (
                "manual_pause",
                "update_in_progress",
                "capacity_blocked",
                "start_paused",
            ):
                return
            prev_reason = self._slot_manager.dispatch_pause_reason
            logger.info("Dispatch resumed — utilization dropped below thresholds")
            self._slot_manager.set_dispatch_paused(False)
            insert_event(
                self._conn,
                event_type="dispatch_resumed",
                detail=f"previous_reason={prev_reason}",
            )
            self._conn.commit()
            self._notifier.notify_limit_cleared()

        self._dispatch_available_work()
        self._check_busy_idle_transition()

    def _dispatch_available_work(self) -> None:
        """Poll each project and dispatch one candidate per free slot."""
        active_ids = self._slot_manager.active_ticket_ids()

        for project_name, poller in self._pollers.items():
            if self._slot_manager.is_project_paused(project_name):
                logger.debug("Skipping paused project %s", project_name)
                continue

            try:
                poll_result = poller.poll(active_ticket_ids=active_ids)
            except Exception:
                logger.exception(
                    "Failed to poll Linear for project %s", project_name,
                )
                continue

            self._persist_queue_entries(project_name, poll_result)
            self._auto_close_parent_issues(poller, poll_result)

            candidates = poll_result.candidates
            if not candidates:
                slot = self._slot_manager.find_free_slot_for_project(project_name)
                if slot is None:
                    continue
                logger.debug("No candidates for %s", project_name)
                if poll_result.blocked:
                    self._check_human_blockers(poll_result.blocked)
                continue

            issue = candidates[0]

            # Build prior-work context and determine slot affinity
            prior = build_prior_context(self._conn, issue.identifier)
            slot = self._slot_manager.find_free_slot_for_project(
                project_name, preferred_slot_id=prior.prior_slot,
            )
            if slot is None:
                continue

            if prior.context_str:
                logger.info(
                    "Retrying %s with prior-work context (prior slot %s, assigned slot %d)",
                    issue.identifier, prior.prior_slot, slot.slot_id,
                )

            self._dispatch_worker(project_name, slot, issue, poller, prior=prior)
            active_ids.add(issue.identifier)

    def _persist_queue_entries(self, project_name: str, poll_result) -> None:
        """Save polled queue entries to DB for dashboard visibility."""
        all_queue_issues = list(poll_result.candidates) + list(poll_result.blocked)
        all_queue_issues.sort(key=lambda i: i.sort_order)
        try:
            save_queue_entries(self._conn, project_name, all_queue_issues)
        except Exception:
            self._conn.rollback()
            logger.exception("Failed to persist queue entries for %s", project_name)

    def _auto_close_parent_issues(self, poller: BugtrackerPoller, poll_result) -> None:
        """Move parent issues to Done when all children are complete."""
        done_status = self._config.bugtracker.done_status
        for parent in poll_result.auto_close_parents:
            try:
                poller.move_issue(parent.identifier, done_status)
                logger.info(
                    "Auto-closed parent %s — all children done",
                    parent.identifier,
                )
                insert_event(
                    self._conn,
                    event_type="parent_auto_closed",
                    detail=f"ticket={parent.identifier}, title={parent.title}",
                )
                self._conn.commit()
            except Exception:
                logger.exception(
                    "Failed to auto-close parent %s", parent.identifier,
                )

    def _check_human_blockers(self, blocked_issues: list) -> None:
        """Check if any blocked issues are held up by a Human-labeled ticket.

        For each unique blocker identifier across all blocked issues,
        fetch its labels and notify if the 'Human' label is present.
        """
        # Collect blocker -> list of blocked ticket identifiers
        blocker_to_blocked: dict[str, list[str]] = {}
        for issue in blocked_issues:
            for blocker_id in (issue.blocked_by or []):
                blocker_to_blocked.setdefault(blocker_id, []).append(issue.identifier)

        for blocker_id, blocked_ids in blocker_to_blocked.items():
            try:
                result = self._bugtracker_client.fetch_issue_labels(blocker_id)
            except Exception:
                logger.debug("Failed to fetch labels for blocker %s", blocker_id)
                continue
            if result is None:
                continue
            title, labels = result
            label_names_lower = {l.lower() for l in labels}
            if "human" in label_names_lower:
                self._notifier.notify_human_blocker(
                    blocker_id=blocker_id,
                    blocker_title=title,
                    blocked_tickets=blocked_ids,
                )

    def _check_busy_idle_transition(self) -> None:
        """Send a notification on the busy→idle transition."""
        currently_busy = bool(
            self._slot_manager.busy_slots() or self._slot_manager.paused_limit_slots()
        )
        if currently_busy:
            self._was_busy = True
        elif self._was_busy:
            self._was_busy = False
            self._notifier.notify_all_idle()

    def _dispatch_worker(
        self,
        project_name: str,
        slot: SlotState,
        issue,
        poller: BugtrackerPoller,
        prior: PriorContext | None = None,
    ) -> None:
        """Assign a ticket to a slot and spawn a worker subprocess."""
        self._worker_mgr.dispatch_worker(
            project_name, slot, issue, poller, prior=prior,
        )

    def _spawn_worker(self, **kwargs) -> multiprocessing.Process:
        """Spawn a worker subprocess — delegates to WorkerLifecycleManager."""
        return self._worker_mgr.spawn_worker(**kwargs)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _shutdown(self) -> None:
        """Graceful shutdown: persist state, don't kill workers."""
        logger.info("Supervisor shutting down")

        from botfarm.git_update import UPDATE_EXIT_CODE

        if self._exit_code != UPDATE_EXIT_CODE:
            reason = "SIGTERM/SIGINT received" if self._shutdown_requested else "unexpected error"
            self._notifier.notify_supervisor_shutdown(reason=reason)

        # Stop all dev servers before final cleanup
        self._devserver_mgr.stop_all()

        self._slot_manager.save()
        self._usage_poller.close()
        self._codex_usage_poller.close()
        self._notifier.close()

        running = [
            (k, p) for k, p in self._workers.items() if p.is_alive()
        ]
        if running:
            logger.info(
                "Leaving %d worker(s) running: %s",
                len(running),
                ", ".join(
                    f"{k[0]}/{k[1]} (PID {p.pid})" for k, p in running
                ),
            )

        insert_event(
            self._conn,
            event_type="supervisor_stop",
            detail=f"running_workers={len(running)}",
        )
        self._conn.commit()
        self._conn.close()

        logger.info("Supervisor stopped")

    # ------------------------------------------------------------------
    # Dev server health check
    # ------------------------------------------------------------------

    _DEVSERVER_CHECK_INTERVAL = 5  # check every N ticks

    def _check_devservers(self) -> None:
        """Periodic liveness check for dev server processes."""
        self._devserver_tick_count += 1
        if self._devserver_tick_count % self._DEVSERVER_CHECK_INTERVAL != 0:
            return
        self._devserver_mgr.health_check()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _subprocess_env(self) -> dict[str, str] | None:
        """Build environment dict for subprocess calls, merging git env."""
        if self._git_env:
            return {**os.environ, **self._git_env}
        return None

    @staticmethod
    def _slot_worktree_cwd(project_cfg: ProjectConfig, slot_id: int) -> str:
        """Compute the working directory for a slot's worktree."""
        base = Path(project_cfg.base_dir).expanduser()
        return str(base.parent / f"{project_cfg.worktree_prefix}{slot_id}")

    @staticmethod
    def _slot_placeholder_branch(slot_id: int) -> str:
        """Return the placeholder branch name for a slot."""
        return f"slot-{slot_id}-placeholder"

    @staticmethod
    def _slot_db_path(project_name: str, slot_id: int) -> str:
        """Compute the sandboxed DB path for a slot."""
        base = Path.home() / ".botfarm" / "slots" / f"{project_name}-{slot_id}"
        return str(base / "botfarm.db")

    def _seed_slot_db(self, project_name: str, slot_id: int) -> str:
        """Create a sandboxed DB for a slot by copying the production DB."""
        slot_db = Path(self._slot_db_path(project_name, slot_id))
        slot_db.parent.mkdir(parents=True, exist_ok=True)
        prod_db = Path(self._db_path)
        if prod_db.exists():
            src = sqlite3.connect(str(prod_db))
            try:
                dst = sqlite3.connect(str(slot_db))
                try:
                    src.backup(dst)
                finally:
                    dst.close()
            finally:
                src.close()
            logger.info(
                "Seeded slot DB for %s/%d: %s", project_name, slot_id, slot_db,
            )
        else:
            logger.warning(
                "Production DB %s not found — slot %s/%d will start with empty DB",
                prod_db, project_name, slot_id,
            )
        return str(slot_db)

    @staticmethod
    def _cleanup_slot_db(project_name: str, slot_id: int) -> None:
        """Remove the sandboxed DB directory for a slot."""
        slot_dir = Path(Supervisor._slot_db_path(project_name, slot_id)).parent
        if slot_dir.exists():
            shutil.rmtree(str(slot_dir), ignore_errors=True)
            logger.info(
                "Cleaned up slot DB for %s/%d: %s", project_name, slot_id, slot_dir,
            )

    # ------------------------------------------------------------------
    # Tick summary
    # ------------------------------------------------------------------

    def _log_tick_summary(self) -> None:
        """Log a one-line summary of slot states at the end of each tick."""
        busy = self._slot_manager.busy_slots()
        free_count = len(self._slot_manager.free_slots())
        paused_limit_count = len(self._slot_manager.paused_limit_slots())
        paused_manual_count = len(self._slot_manager.paused_manual_slots())

        if not busy and not paused_limit_count and not paused_manual_count:
            logger.debug(
                "Tick: all slots free (%d)", free_count,
            )
            return

        now = datetime.now(timezone.utc)
        parts: list[str] = []
        for slot in busy:
            elapsed_str = "?"
            if slot.started_at:
                try:
                    started = datetime.fromisoformat(
                        slot.started_at.replace("Z", "+00:00"),
                    )
                    elapsed_min = int((now - started).total_seconds() / 60)
                    if elapsed_min >= 60:
                        elapsed_str = f"{elapsed_min // 60}h{elapsed_min % 60}m"
                    else:
                        elapsed_str = f"{elapsed_min}m"
                except (ValueError, TypeError):
                    pass
            stage = slot.stage or "starting"
            parts.append(
                f"{slot.project}/{slot.slot_id}: {slot.ticket_id} {stage} {elapsed_str}",
            )

        busy_desc = f"{len(busy)} busy ({', '.join(parts)})"

        counts: list[str] = [f"{free_count} free"]
        if paused_limit_count:
            counts.append(f"{paused_limit_count} paused_limit")
        if paused_manual_count:
            counts.append(f"{paused_manual_count} paused_manual")

        usage_str = ""
        state = self._usage_poller.state
        if state.utilization_5h is not None:
            usage_str = f" | usage 5h={state.utilization_5h * 100:.0f}%"

        codex_str = ""
        cu_state = self._codex_usage_poller.state
        if cu_state.primary_used_pct is not None:
            codex_str = (
                f" | codex={cu_state.primary_used_pct * 100:.0f}%"
                f"/{cu_state.secondary_used_pct * 100:.0f}%"
                if cu_state.secondary_used_pct is not None
                else f" | codex={cu_state.primary_used_pct * 100:.0f}%"
            )

        capacity_str = ""
        if self._capacity_level != "normal":
            capacity_str = f" | capacity={self._capacity_level}"

        logger.info(
            "Tick: %s, %s%s%s%s",
            busy_desc, ", ".join(counts), usage_str, codex_str, capacity_str,
        )

    # ------------------------------------------------------------------
    # Log maintenance
    # ------------------------------------------------------------------

    _LOG_CLEANUP_INTERVAL = 3600  # run at most once per hour

    def _cleanup_old_ticket_logs(self) -> None:
        """Remove per-ticket log directories older than the retention period."""
        now = time.time()
        if now - self._last_log_cleanup < self._LOG_CLEANUP_INTERVAL:
            return
        self._last_log_cleanup = now

        retention_days = self._config.logging.ticket_log_retention_days
        cutoff = now - retention_days * 86400

        if not self._log_dir.is_dir():
            return

        active_tickets: set[str] = set()
        for slot in self._slot_manager.all_slots():
            if slot.ticket_id and slot.status not in ("free",):
                active_tickets.add(slot.ticket_id)

        removed = 0
        for entry in self._log_dir.iterdir():
            if not entry.is_dir():
                continue
            if entry.name in ("slot_dbs", "devserver"):
                continue
            if entry.name in active_tickets:
                continue
            try:
                mtime = entry.stat().st_mtime
            except OSError:
                continue
            if mtime < cutoff:
                shutil.rmtree(str(entry), ignore_errors=True)
                removed += 1

        if removed:
            logger.info(
                "Cleaned up %d old ticket log dir(s) (retention=%d days)",
                removed, retention_days,
            )

    def _sleep(self, seconds: int) -> None:
        """Interruptible sleep — checks shutdown and wake flags each second."""
        for _ in range(seconds):
            if self._shutdown_requested or self._wake_event.is_set():
                break
            time.sleep(1)
        self._wake_event.clear()


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def setup_logging(
    log_dir: str | Path = DEFAULT_LOG_DIR,
    *,
    level: int = logging.INFO,
    console: bool = True,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """Configure logging to file and optionally console.

    Log file: ``<log_dir>/supervisor.log``

    Uses :class:`~logging.handlers.RotatingFileHandler` so the log file
    is automatically rotated when it reaches *max_bytes*, keeping at most
    *backup_count* rotated copies.
    """
    log_dir = Path(log_dir).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "supervisor.log"

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger()
    root.setLevel(level)

    # Guard against duplicate handlers on repeated calls
    has_file = any(
        isinstance(h, logging.FileHandler)
        and getattr(h, "baseFilename", None) == str(log_file)
        for h in root.handlers
    )
    has_console = any(
        isinstance(h, logging.StreamHandler)
        and not isinstance(h, logging.FileHandler)
        for h in root.handlers
    )

    # Rotating file handler
    if not has_file:
        fh = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        fh.setLevel(level)
        fh.setFormatter(fmt)
        root.addHandler(fh)

    # Console handler (for foreground mode)
    if console and not has_console:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        root.addHandler(ch)

    # Suppress noisy HTTP client logs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
