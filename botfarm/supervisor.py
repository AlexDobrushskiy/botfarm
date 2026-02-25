"""Supervisor daemon: poll Linear, dispatch workers, manage slots.

The supervisor runs as a long-lived process. Each iteration it:
1. Reconciles slot state against reality (dead PIDs → failed).
2. Handles completed/failed workers (update Linear, free slots).
3. Polls Linear for Todo tickets.
4. Dispatches workers to free slots.
5. Sleeps for the configured poll interval.

Workers run in isolated subprocesses — a single worker failure never
brings down the supervisor.
"""

from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
import os
import queue as queue_mod
import shutil
import signal
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

from botfarm.config import BotfarmConfig, ProjectConfig
from botfarm.db import get_task, init_db, insert_event, insert_task, resolve_db_path, save_queue_entries, update_task
from botfarm.linear import LinearPoller, create_pollers
from botfarm.notifications import Notifier
from botfarm.preflight import log_preflight_summary, run_preflight_checks
from botfarm.slots import SlotManager, SlotState, _is_pid_alive
from botfarm.usage import DEFAULT_PAUSE_5H_THRESHOLD, DEFAULT_PAUSE_7D_THRESHOLD, UsagePoller
from botfarm.worker import STAGES, PipelineResult, run_pipeline

logger = logging.getLogger(__name__)

DEFAULT_LOG_DIR = Path.home() / ".botfarm" / "logs"


# ---------------------------------------------------------------------------
# Worker subprocess entry point
# ---------------------------------------------------------------------------


@dataclass
class _WorkerResult:
    """Message sent from a worker subprocess back to the supervisor."""

    project: str
    slot_id: int
    success: bool
    failure_stage: str | None = None
    failure_reason: str | None = None
    limit_hit: bool = False
    paused: bool = False
    stages_completed: list[str] | None = None


def _worker_entry(
    *,
    ticket_id: str,
    ticket_title: str,
    task_id: int,
    project_name: str,
    slot_id: int,
    cwd: str,
    log_dir: str | None = None,
    log_max_bytes: int = 10 * 1024 * 1024,
    log_backup_count: int = 5,
    result_queue: multiprocessing.Queue,
    max_turns: dict[str, int] | None,
    max_review_iterations: int = 3,
    max_ci_retries: int = 2,
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,
    placeholder_branch: str | None = None,
    slot_db_path: str | None = None,
    pause_event: multiprocessing.Event | None = None,
) -> None:
    """Entry point for a worker subprocess.

    This runs ``run_pipeline`` with its own DB connection. Results are
    communicated back to the supervisor via ``result_queue`` so only
    the supervisor writes to the shared state file.

    The database path is resolved from the ``BOTFARM_DB_PATH`` environment
    variable (inherited from the supervisor process).

    When ``resume_from_stage`` is set, the pipeline skips stages that
    were already completed and resumes from the specified stage.
    """
    # Ignore SIGINT so only the supervisor handles Ctrl-C
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # Set up per-ticket Python logging to a file so worker log messages
    # (info, warnings, errors) are captured alongside the subprocess logs.
    if log_dir:
        _setup_worker_logging(
            Path(log_dir),
            max_bytes=log_max_bytes,
            backup_count=log_backup_count,
        )

    db_path = resolve_db_path()
    conn = init_db(db_path)

    try:
        result: PipelineResult = run_pipeline(
            ticket_id=ticket_id,
            task_id=task_id,
            cwd=cwd,
            conn=conn,
            db_path=db_path,
            project=project_name,
            slot_id=slot_id,
            log_dir=log_dir,
            placeholder_branch=placeholder_branch,
            max_turns=max_turns,
            max_review_iterations=max_review_iterations,
            max_ci_retries=max_ci_retries,
            resume_from_stage=resume_from_stage,
            resume_session_id=resume_session_id,
            slot_db_path=slot_db_path,
            pause_event=pause_event,
        )
        if result.paused:
            result_queue.put(_WorkerResult(
                project=project_name, slot_id=slot_id, success=False,
                paused=True,
                stages_completed=result.stages_completed,
            ))
            logger.info(
                "Worker %s/%d paused by manual request for %s",
                project_name, slot_id, ticket_id,
            )
        elif result.success:
            result_queue.put(_WorkerResult(
                project=project_name, slot_id=slot_id, success=True,
            ))
            logger.info(
                "Worker %s/%d finished successfully for %s",
                project_name, slot_id, ticket_id,
            )
        else:
            # Check if the failure looks like a limit hit
            limit_hit = _check_limit_hit(result)
            result_queue.put(_WorkerResult(
                project=project_name, slot_id=slot_id, success=False,
                failure_stage=result.failure_stage,
                failure_reason=result.failure_reason,
                limit_hit=limit_hit,
            ))
            if limit_hit:
                logger.warning(
                    "Worker %s/%d hit usage limit at stage %s for %s",
                    project_name, slot_id, result.failure_stage, ticket_id,
                )
            else:
                logger.warning(
                    "Worker %s/%d pipeline failed at %s for %s: %s",
                    project_name, slot_id, result.failure_stage,
                    ticket_id, result.failure_reason,
                )
    except Exception as exc:
        logger.exception(
            "Worker %s/%d crashed for %s", project_name, slot_id, ticket_id,
        )
        result_queue.put(_WorkerResult(
            project=project_name, slot_id=slot_id, success=False,
            failure_stage="worker_entry",
            failure_reason=str(exc)[:500],
        ))
    finally:
        conn.close()


def _setup_worker_logging(
    log_dir: Path,
    *,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> None:
    """Configure a rotating file handler for the worker subprocess.

    Writes worker-level Python log messages to ``<log_dir>/worker.log``
    so they persist alongside the per-stage subprocess logs.  Uses
    :class:`~logging.handlers.RotatingFileHandler` to cap disk usage.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "worker.log"

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.handlers.RotatingFileHandler(
        str(log_file),
        maxBytes=max_bytes,
        backupCount=backup_count,
    )
    handler.setFormatter(fmt)
    handler.setLevel(logging.DEBUG)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)


def _check_limit_hit(result: PipelineResult) -> bool:
    """Heuristic: detect if a pipeline failure was caused by a usage limit.

    Matches specific limit-related strings in the failure reason.
    """
    reason = (result.failure_reason or "").lower()
    limit_indicators = [
        "rate limit",
        "usage limit",
        "max_tokens_exceeded",
    ]
    return any(indicator in reason for indicator in limit_indicators)


# ---------------------------------------------------------------------------
# Supervisor
# ---------------------------------------------------------------------------


class Supervisor:
    """Long-lived daemon that dispatches and monitors worker subprocesses."""

    def __init__(
        self,
        config: BotfarmConfig,
        *,
        log_dir: str | Path | None = None,
    ) -> None:
        self._config = config
        self._log_dir = Path(log_dir or DEFAULT_LOG_DIR).expanduser()
        self._shutdown_requested = False

        # Lookup: project name -> ProjectConfig
        self._projects: dict[str, ProjectConfig] = {
            p.name: p for p in config.projects
        }

        # Linear pollers keyed by project name
        pollers = create_pollers(config)
        self._pollers: dict[str, LinearPoller] = {
            p.project_name: p for p in pollers
        }

        # Database — path comes from BOTFARM_DB_PATH env var (loaded from .env)
        db_path = resolve_db_path()
        self._db_path = str(db_path)
        self._conn: sqlite3.Connection = init_db(db_path, allow_migration=True)

        # Slot manager (shares the supervisor's DB connection)
        self._slot_manager = SlotManager(
            db_path=self._db_path,
            conn=self._conn,
        )
        for project in config.projects:
            for sid in project.slots:
                self._slot_manager.register_slot(project.name, sid)
        self._slot_manager.load()

        # Usage poller
        self._usage_poller = UsagePoller()

        # Webhook notifier
        self._notifier = Notifier(config.notifications)
        self._was_busy = False  # Track busy→idle transition for notifications

        # Track child processes: (project, slot_id) -> Process
        self._workers: dict[tuple[str, int], multiprocessing.Process] = {}

        # Per-worker pause events: (project, slot_id) -> Event
        # Set by the supervisor to signal workers to stop after current stage
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

        # Queue for worker results — workers send _WorkerResult here
        self._result_queue: multiprocessing.Queue = multiprocessing.Queue()

        # Timestamp of last ticket log cleanup (runs at most once per hour)
        self._last_log_cleanup: float = 0.0

    @property
    def slot_manager(self) -> SlotManager:
        return self._slot_manager

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
    # Crash recovery on startup
    # ------------------------------------------------------------------

    def _recover_on_startup(self) -> None:
        """Recover gracefully from persisted state after a restart.

        For each slot that was busy:
          - If the PID is still alive, re-attach monitoring.
          - If the PID is dead, check if a PR was created/merged to
            determine outcome; otherwise mark as failed.
        For each slot that was paused_limit:
          - These will be handled by ``_handle_paused_slots`` in the
            normal tick loop — no special startup action needed.
        Finally, reconcile SQLite task records.
        """
        # NOTE: SlotManager.reconcile() (called every tick via _reconcile_workers)
        # also handles dead PIDs for busy/paused slots. After this startup
        # recovery runs, the first tick's reconcile() will see slots already
        # marked failed/completed and skip them. For reattached workers whose
        # PID dies between here and the first tick, reconcile() will handle it.
        recovered_count = 0
        for slot in self._slot_manager.all_slots():
            if slot.status == "busy" and slot.pid is not None:
                if slot.sigterm_sent_at:
                    # Timeout system owned this slot — PID is likely dead
                    self._recover_timed_out_slot(slot)
                    recovered_count += 1
                elif _is_pid_alive(slot.pid):
                    self._reattach_worker(slot)
                    recovered_count += 1
                else:
                    self._recover_dead_worker(slot)
                    recovered_count += 1

        # Reconcile DB task records with slot state
        self._reconcile_db_tasks()

        if recovered_count:
            logger.info(
                "Startup recovery processed %d slot(s)", recovered_count,
            )
            insert_event(
                self._conn,
                event_type="startup_recovery",
                detail=f"slots_processed={recovered_count}",
            )

        # Single commit for all recovery operations.
        # Exception: _resume_recovered_worker commits early to record
        # the recovery_resumed event before spawning.
        self._conn.commit()

    def _reattach_worker(self, slot: SlotState) -> None:
        """Re-attach monitoring for a still-alive worker process.

        We cannot get a ``multiprocessing.Process`` handle for an
        externally running PID, but we can track the PID so that
        ``_reconcile_workers`` will detect when it exits.  The result
        queue from the previous supervisor is gone, so we rely on
        PID liveness checks and reconciliation to detect completion.
        """
        logger.info(
            "Re-attaching to alive worker PID %d for %s/%d (ticket %s, stage %s)",
            slot.pid, slot.project, slot.slot_id, slot.ticket_id, slot.stage,
        )
        insert_event(
            self._conn,
            event_type="worker_reattached",
            detail=f"project={slot.project}, slot={slot.slot_id}, "
            f"pid={slot.pid}, ticket={slot.ticket_id}, stage={slot.stage}",
        )

    def _recover_dead_worker(self, slot: SlotState) -> None:
        """Determine outcome for a dead worker and update slot state.

        Uses stage-aware recovery when the worker made progress
        (``stages_completed`` is non-empty). Falls back to PR-status-only
        logic for workers that died before completing any stage.
        """
        project = slot.project
        slot_id = slot.slot_id
        ticket_id = slot.ticket_id
        old_pid = slot.pid
        pr_status = self._check_pr_status(slot)

        # --- Stage-aware recovery ---
        if slot.stages_completed:
            next_stage = self._next_stage_after(slot.stages_completed)
            if next_stage == "merge" and pr_status == "merged":
                # Worker died during merge but PR is already merged
                logger.info(
                    "Dead worker PID %d for %s/%d: PR already merged — marking completed",
                    old_pid, project, slot_id,
                )
                self._mark_recovery_completed(slot, reason="pr_merged_during_merge")
                return
            if next_stage:
                logger.info(
                    "Dead worker PID %d for %s/%d: resuming from stage '%s' "
                    "(completed: %s)",
                    old_pid, project, slot_id, next_stage,
                    ", ".join(slot.stages_completed),
                )
                self._resume_recovered_worker(slot, resume_from_stage=next_stage)
                return
            # All stages completed — should not happen, but treat as completed
            logger.info(
                "Dead worker PID %d for %s/%d: all stages completed — marking completed",
                old_pid, project, slot_id,
            )
            self._mark_recovery_completed(slot, reason="all_stages_completed")
            return

        # --- No stages completed: check if implement produced a PR ---
        if pr_status == "merged":
            logger.info(
                "Dead worker PID %d for %s/%d: PR was merged — marking completed",
                old_pid, project, slot_id,
            )
            self._mark_recovery_completed(slot, reason="pr_merged")
            return

        if pr_status == "open":
            # Worker died during implement but managed to create a PR.
            # Resume from review instead of discarding the work.
            logger.info(
                "Dead worker PID %d for %s/%d: PR is open (no stages completed) "
                "— resuming from review",
                old_pid, project, slot_id,
            )
            self._resume_recovered_worker(slot, resume_from_stage="review")
            return

        if pr_status == "closed":
            logger.warning(
                "Dead worker PID %d for %s/%d: PR was closed without merging — marking failed",
                old_pid, project, slot_id,
            )
            self._mark_recovery_failed(
                slot, reason="pr_closed",
                failure_msg=f"worker PID {old_pid} died (PR closed without merging)",
            )
            return

        # No PR, no stages completed — nothing to resume from
        logger.warning(
            "Dead worker PID %d for %s/%d: no PR found — marking failed",
            old_pid, project, slot_id,
        )
        self._mark_recovery_failed(
            slot, reason="no_pr",
            failure_msg=f"worker PID {old_pid} died (no PR found)",
        )

    def _mark_recovery_completed(self, slot: SlotState, *, reason: str) -> None:
        """Mark a recovered slot as completed and record the event."""
        self._slot_manager.mark_completed(slot.project, slot.slot_id)
        task_id = self._find_task_id(slot.ticket_id)
        if task_id is not None:
            update_task(self._conn, task_id, status="completed")
        insert_event(
            self._conn,
            task_id=task_id,
            event_type="recovery_completed",
            detail=f"project={slot.project}, slot={slot.slot_id}, "
            f"pid={slot.pid}, reason={reason}",
        )

    def _mark_recovery_failed(
        self, slot: SlotState, *, reason: str, failure_msg: str,
    ) -> None:
        """Mark a recovered slot as failed and record the event."""
        self._slot_manager.mark_failed(slot.project, slot.slot_id, reason=failure_msg)
        task_id = self._find_task_id(slot.ticket_id)
        if task_id is not None:
            update_task(
                self._conn, task_id,
                status="failed",
                failure_reason=failure_msg,
            )
        insert_event(
            self._conn,
            task_id=task_id,
            event_type="recovery_failed",
            detail=f"project={slot.project}, slot={slot.slot_id}, "
            f"pid={slot.pid}, reason={reason}",
        )

    @staticmethod
    def _next_stage_after(stages_completed: list[str]) -> str | None:
        """Return the first stage in STAGES not in *stages_completed*.

        Returns ``None`` when all stages have been completed.
        """
        completed_set = set(stages_completed)
        for stage in STAGES:
            if stage not in completed_set:
                return stage
        return None

    def _resume_recovered_worker(
        self, slot: SlotState, *, resume_from_stage: str,
    ) -> None:
        """Resume a crash-recovered worker by spawning a new subprocess.

        Similar to ``_resume_paused_worker`` but for workers that died
        due to SIGINT or unexpected crash. Sets the slot back to busy
        and spawns a new worker starting from *resume_from_stage*.
        """
        project_name = slot.project
        project_cfg = self._projects.get(project_name)
        if not project_cfg:
            logger.error("No project config for %s — cannot resume recovered worker", project_name)
            self._mark_recovery_failed(
                slot, reason="no_project_config",
                failure_msg=f"No project config for {project_name}",
            )
            return

        task_id = self._find_task_id(slot.ticket_id)

        insert_event(
            self._conn,
            task_id=task_id,
            event_type="recovery_resumed",
            detail=f"project={project_name}, slot={slot.slot_id}, "
            f"pid={slot.pid}, resume_from={resume_from_stage}, "
            f"stages_completed={','.join(slot.stages_completed)}",
        )
        # Commit here because _recover_on_startup does a single commit at
        # the end, but we need the event recorded before spawning.
        self._conn.commit()

        # Set slot back to busy (it's still in "busy" status with a dead PID)
        slot.pid = None
        self._slot_manager.update_stage(
            project_name, slot.slot_id, stage=resume_from_stage,
        )

        cwd = self._slot_worktree_cwd(project_cfg, slot.slot_id)

        ticket_log_dir = self._log_dir / slot.ticket_id
        ticket_log_dir.mkdir(parents=True, exist_ok=True)

        slot_db = self._slot_db_path(project_name, slot.slot_id)
        if not Path(slot_db).exists():
            slot_db = self._seed_slot_db(project_name, slot.slot_id)

        # Create a pause event so recovered workers can respond to manual pause
        pause_event = multiprocessing.Event()
        key = (project_name, slot.slot_id)
        self._pause_events[key] = pause_event

        proc = multiprocessing.Process(
            target=_worker_entry,
            kwargs={
                "ticket_id": slot.ticket_id,
                "ticket_title": slot.ticket_title or "",
                "task_id": task_id or 0,
                "project_name": project_name,
                "slot_id": slot.slot_id,
                "cwd": cwd,
                "log_dir": str(ticket_log_dir),
                "log_max_bytes": self._config.logging.max_bytes,
                "log_backup_count": self._config.logging.backup_count,
                "result_queue": self._result_queue,
                "max_turns": None,
                "max_review_iterations": self._config.agents.max_review_iterations,
                "max_ci_retries": self._config.agents.max_ci_retries,
                "resume_from_stage": resume_from_stage,
                "resume_session_id": slot.current_session_id,
                "placeholder_branch": self._slot_placeholder_branch(slot.slot_id),
                "slot_db_path": slot_db,
                "pause_event": pause_event,
            },
            daemon=False,
        )
        proc.start()
        self._workers[key] = proc
        self._slot_manager.set_pid(project_name, slot.slot_id, proc.pid)

        logger.info(
            "Resumed recovered worker PID %d for %s in slot %s/%d at stage %s",
            proc.pid, slot.ticket_id, project_name, slot.slot_id,
            resume_from_stage,
        )

    def _recover_timed_out_slot(self, slot: SlotState) -> None:
        """Handle a slot that had a SIGTERM sent before the supervisor crashed."""
        project = slot.project
        slot_id = slot.slot_id
        old_pid = slot.pid

        # If the process is still alive (e.g. supervisor crashed right after
        # sending SIGTERM and the worker is still doing cleanup), send SIGKILL
        # to ensure it's terminated before marking the slot as failed.
        if _is_pid_alive(old_pid):
            logger.warning(
                "Timed-out worker PID %d for %s/%d is still alive — sending SIGKILL",
                old_pid, project, slot_id,
            )
            try:
                os.kill(old_pid, signal.SIGKILL)
            except OSError:
                pass  # already dead by now

        reason = f"timeout in stage {slot.stage} (recovered after restart)"
        self._slot_manager.mark_failed(project, slot_id, reason=reason)

        task_id = self._find_task_id(slot.ticket_id)
        if task_id is not None:
            update_task(
                self._conn, task_id,
                status="failed",
                failure_reason=reason,
            )
        insert_event(
            self._conn,
            task_id=task_id,
            event_type="recovery_timeout",
            detail=f"project={project}, slot={slot_id}, "
            f"pid={old_pid}, stage={slot.stage}",
        )
        logger.warning(
            "Recovered timed-out slot %s/%d (PID %s, stage %s) — marked failed",
            project, slot_id, old_pid, slot.stage,
        )

    def _check_pr_status(self, slot: SlotState) -> str | None:
        """Check whether a PR exists for the slot's branch.

        Returns ``"merged"``, ``"open"``, ``"closed"``, or ``None``
        (no PR found).

        PR URL resolution order:
        1. Tasks DB record (persisted by worker cross-process)
        2. Slot state (``slot.pr_url``)
        3. ``gh pr view`` branch lookup in the project working directory
        """
        pr_ref: str | None = None
        source: str | None = None

        # 1. Try tasks DB (most reliable — persisted cross-process by worker)
        if slot.ticket_id:
            task_id = self._find_task_id(slot.ticket_id)
            if task_id is not None:
                task = get_task(self._conn, task_id)
                if task and task["pr_url"]:
                    pr_ref = task["pr_url"]
                    source = "tasks_db"

        # 2. Fall back to slot state
        if not pr_ref and slot.pr_url:
            pr_ref = slot.pr_url
            source = "slot_state"

        # 3. Fall back to gh pr view branch lookup
        if not pr_ref:
            project_cfg = self._projects.get(slot.project)
            if project_cfg:
                cwd = self._slot_worktree_cwd(project_cfg, slot.slot_id)
                pr_ref = self._gh_pr_url_for_branch(slot.branch, cwd)
                if pr_ref:
                    source = "gh_branch_lookup"

        if not pr_ref:
            logger.debug(
                "No PR URL found for %s/%d (ticket %s)",
                slot.project, slot.slot_id, slot.ticket_id,
            )
            return None

        state = self._gh_pr_state(pr_ref)
        logger.info(
            "PR status for %s/%d: url=%s, source=%s, state=%s",
            slot.project, slot.slot_id, pr_ref, source, state,
        )
        return state

    @staticmethod
    def _gh_pr_url_for_branch(branch: str | None, cwd: str) -> str | None:
        """Get the PR URL for a branch via ``gh pr view``."""
        if not branch:
            return None
        try:
            proc = subprocess.run(
                ["gh", "pr", "view", branch, "--json", "url", "--jq", ".url"],
                capture_output=True, text=True, cwd=cwd, timeout=15,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                url = proc.stdout.strip()
                if "/pull/" in url:
                    return url
        except Exception as exc:
            logger.debug("gh pr view failed for branch %s: %s", branch, exc)
        return None

    @staticmethod
    def _gh_pr_state(pr_ref: str) -> str | None:
        """Query a PR's state (``OPEN``, ``MERGED``, ``CLOSED``)."""
        try:
            proc = subprocess.run(
                ["gh", "pr", "view", pr_ref, "--json", "state", "--jq", ".state"],
                capture_output=True, text=True, timeout=15,
            )
            if proc.returncode == 0:
                state = proc.stdout.strip().upper()
                if state == "MERGED":
                    return "merged"
                if state == "OPEN":
                    return "open"
                if state == "CLOSED":
                    return "closed"
        except Exception as exc:
            logger.debug("gh pr view failed for %s: %s", pr_ref, exc)
        return None

    def _reconcile_db_tasks(self) -> None:
        """Ensure SQLite task records are consistent with slot state.

        For any task that is still ``in_progress`` in the DB but whose
        slot has already been marked ``failed`` or ``completed``, update
        the DB to match.
        """
        from botfarm.db import get_task_by_ticket

        for slot in self._slot_manager.all_slots():
            if not slot.ticket_id:
                continue
            row = get_task_by_ticket(self._conn, slot.ticket_id)
            if row is None:
                continue

            db_status = row["status"]
            slot_status = slot.status

            if db_status == "in_progress" and slot_status == "failed":
                update_task(self._conn, row["id"], status="failed",
                            failure_reason="marked failed during recovery")
                logger.info(
                    "Reconciled task %s: DB in_progress -> failed", slot.ticket_id,
                )
            elif db_status == "in_progress" and slot_status == "completed_pending_cleanup":
                update_task(self._conn, row["id"], status="completed")
                logger.info(
                    "Reconciled task %s: DB in_progress -> completed", slot.ticket_id,
                )

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

        # Run pre-flight health checks before entering main loop
        preflight_results = run_preflight_checks(self._config)
        if not log_preflight_summary(preflight_results):
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
            raise SystemExit(1)

        # Recover from previous state before entering main loop
        self._recover_on_startup()

        # Start dashboard if enabled
        self._dashboard_thread = None
        if self._config.dashboard.enabled:
            from botfarm.dashboard import start_dashboard
            self._dashboard_thread = start_dashboard(
                self._config.dashboard,
                db_path=self._db_path,
                linear_workspace=self._config.linear.workspace,
                botfarm_config=self._config,
                on_pause=self.request_pause,
                on_resume=self.request_resume,
                on_update=self.request_update,
                update_failed_event=self._update_failed_event,
            )

        # Initial usage poll so we have data before the first dispatch
        try:
            self._usage_poller.force_poll(self._conn)
        except Exception:
            logger.exception("Initial usage poll failed — continuing without usage data")

        try:
            while not self._shutdown_requested:
                self._tick()
                if not self._shutdown_requested:
                    self._sleep(self._config.linear.poll_interval_seconds)
        finally:
            self._shutdown()

        return self._exit_code

    def _tick(self) -> None:
        """One iteration of the supervisor loop."""
        # Merge worker-written stage updates from disk before any state writes
        self._slot_manager.refresh_stages_from_disk()

        for phase in (
            self._reconcile_workers,
            self._handle_manual_pause_resume,
            self._handle_update_request,
            self._check_timeouts,
            self._handle_finished_slots,
            self._handle_paused_slots,
            self._poll_usage,
            self._poll_and_dispatch,
            self._cleanup_old_ticket_logs,
        ):
            try:
                phase()
            except Exception:
                logger.exception("%s failed", getattr(phase, "__name__", repr(phase)))

        # Always update heartbeat so the dashboard knows the supervisor is alive,
        # even when no state mutations occurred during this tick.
        self._slot_manager.save()

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    def _reconcile_workers(self) -> None:
        """Check worker subprocesses and reconcile slot state."""
        # Drain worker results from the queue.
        # Use a short timeout because multiprocessing.Queue.put() is
        # asynchronous — items may not be immediately available.
        while True:
            try:
                wr: _WorkerResult = self._result_queue.get(timeout=0.05)
            except queue_mod.Empty:
                break
            if wr.paused:
                # Sync stages_completed from the worker subprocess
                if wr.stages_completed:
                    for stage in wr.stages_completed:
                        self._slot_manager.complete_stage(
                            wr.project, wr.slot_id, stage,
                        )
                self._slot_manager.mark_paused_manual(wr.project, wr.slot_id)
                logger.info(
                    "Worker result: %s/%d paused by manual request",
                    wr.project, wr.slot_id,
                )
            elif wr.success:
                self._slot_manager.mark_completed(wr.project, wr.slot_id)
                logger.info(
                    "Worker result: %s/%d completed", wr.project, wr.slot_id,
                )
            elif wr.limit_hit:
                self._usage_poller.force_poll(self._conn)
                self._handle_limit_hit(wr)
            else:
                # String matching didn't flag a limit hit — verify via
                # the usage API in case the error message was unexpected.
                limit_detected = self._check_usage_api_for_limit()
                if limit_detected:
                    logger.info(
                        "Worker %s/%d: usage API confirms limit hit "
                        "(not detected by string matching)",
                        wr.project, wr.slot_id,
                    )
                    self._handle_limit_hit(wr)
                else:
                    reason = f"{wr.failure_stage}: {wr.failure_reason}"
                    # Capture ticket_id before mark_failed in case it
                    # ever clears ticket state (like free_slot does).
                    slot = self._slot_manager.get_slot(wr.project, wr.slot_id)
                    ticket_id = slot.ticket_id if slot else None
                    self._slot_manager.mark_failed(
                        wr.project, wr.slot_id, reason=reason,
                    )
                    # Update DB task with failure info
                    task_id = self._find_task_id(ticket_id)
                    if task_id is not None:
                        update_task(
                            self._conn, task_id,
                            status="failed",
                            failure_reason=reason,
                        )
                        self._conn.commit()
                    logger.warning(
                        "Worker result: %s/%d failed — %s",
                        wr.project, wr.slot_id, reason,
                    )
            # Clean up pause event for this worker regardless of outcome
            self._pause_events.pop((wr.project, wr.slot_id), None)

        # Clean up any tracked processes that have exited
        finished = []
        for key, proc in self._workers.items():
            if not proc.is_alive():
                proc.join(timeout=1)
                finished.append(key)
        for key in finished:
            del self._workers[key]

        # Then reconcile via SlotManager (checks PIDs we didn't track)
        messages = self._slot_manager.reconcile()
        for msg in messages:
            insert_event(
                self._conn,
                event_type="slot_reconciled",
                detail=msg,
            )
        if messages:
            self._conn.commit()

    # ------------------------------------------------------------------
    # Timeout enforcement
    # ------------------------------------------------------------------

    def _check_timeouts(self) -> None:
        """Kill worker processes that have exceeded their stage timeout.

        Non-blocking: on the first tick after timeout, sends SIGTERM and
        records ``sigterm_sent_at`` on the slot.  On a subsequent tick,
        if the grace period has elapsed and the process is still alive,
        escalates to SIGKILL.  This avoids blocking the supervisor loop.
        """
        timeout_cfg = self._config.agents.timeout_minutes
        grace = self._config.agents.timeout_grace_seconds
        now = datetime.now(timezone.utc)

        for slot in self._slot_manager.busy_slots():
            if slot.pid is None or slot.stage is None:
                continue

            # --- Phase 2: escalate SIGTERM → SIGKILL after grace period ---
            if slot.sigterm_sent_at:
                self._maybe_escalate_kill(slot, now=now, grace=grace)
                continue

            # --- Phase 1: detect timeout and send SIGTERM ---
            if not slot.stage_started_at:
                continue

            limit_minutes = timeout_cfg.get(slot.stage)
            if limit_minutes is None:
                continue

            try:
                stage_start = datetime.fromisoformat(
                    slot.stage_started_at.replace("Z", "+00:00"),
                )
            except (ValueError, TypeError):
                logger.warning(
                    "Failed to parse stage_started_at for %s/%d: %r",
                    slot.project, slot.slot_id, slot.stage_started_at,
                )
                continue

            elapsed = (now - stage_start).total_seconds()
            limit_seconds = limit_minutes * 60
            if elapsed < limit_seconds:
                continue

            self._send_sigterm(slot, elapsed=elapsed, limit=limit_seconds)

    def _send_sigterm(
        self,
        slot: SlotState,
        *,
        elapsed: float,
        limit: float,
    ) -> None:
        """Send SIGTERM to a timed-out worker and record the time."""
        pid = slot.pid
        stage = slot.stage
        project = slot.project

        logger.warning(
            "Worker PID %d for %s/%d timed out in stage '%s' "
            "(%.0fs elapsed, %ds limit) — sending SIGTERM",
            pid, project, slot.slot_id, stage, elapsed, int(limit),
        )

        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass  # already dead

        # Record SIGTERM time so _maybe_escalate_kill can check grace period
        from botfarm.slots import _now_iso
        slot.sigterm_sent_at = _now_iso()
        self._slot_manager.save()

    def _maybe_escalate_kill(
        self,
        slot: SlotState,
        *,
        now: datetime,
        grace: int,
    ) -> None:
        """If the grace period has elapsed since SIGTERM, escalate to SIGKILL."""
        try:
            sigterm_time = datetime.fromisoformat(
                slot.sigterm_sent_at.replace("Z", "+00:00"),
            )
        except (ValueError, TypeError):
            # Can't parse — escalate immediately
            sigterm_time = now

        if (now - sigterm_time).total_seconds() < grace:
            return  # still within grace period

        pid = slot.pid
        stage = slot.stage
        project = slot.project

        try:
            os.killpg(os.getpgid(pid), 0)  # check if still alive
            os.killpg(os.getpgid(pid), signal.SIGKILL)
            logger.warning(
                "Worker PID %d did not exit after SIGTERM, sent SIGKILL", pid,
            )
        except (OSError, ProcessLookupError):
            pass  # exited during grace period

        # Clean up the process from our tracking
        key = (project, slot.slot_id)
        proc = self._workers.pop(key, None)
        if proc is not None:
            proc.join(timeout=2)

        # Compute elapsed from stage start for event detail
        elapsed = 0.0
        limit = 0
        try:
            stage_start = datetime.fromisoformat(
                slot.stage_started_at.replace("Z", "+00:00"),
            )
            elapsed = (now - stage_start).total_seconds()
            limit_minutes = self._config.agents.timeout_minutes.get(stage, 0)
            limit = limit_minutes * 60
        except (ValueError, TypeError, AttributeError):
            pass

        # Mark slot as failed
        reason = f"timeout in stage {stage}"
        self._slot_manager.mark_failed(project, slot.slot_id, reason=reason)

        # Log event and update task
        task_id = self._find_task_id(slot.ticket_id)
        insert_event(
            self._conn,
            task_id=task_id,
            event_type="timeout",
            detail=f"stage={stage}, elapsed={elapsed:.0f}s, limit={int(limit)}s",
        )
        if task_id is not None:
            update_task(
                self._conn,
                task_id,
                status="failed",
                failure_reason=reason,
            )
        self._conn.commit()

    # ------------------------------------------------------------------
    # Handle finished slots
    # ------------------------------------------------------------------

    def _handle_finished_slots(self) -> None:
        """Process completed and failed slots — update Linear, free slots."""
        for slot in self._slot_manager.all_slots():
            if slot.status == "completed_pending_cleanup":
                self._handle_completed_slot(slot)
            elif slot.status == "failed":
                self._handle_failed_slot(slot)

    def _handle_completed_slot(self, slot: SlotState) -> None:
        """Update Linear for a completed slot and free it.

        Checks if the PR was merged to determine the correct status:
        - PR merged → move to done_status (e.g. "Done")
        - PR open/other → move to in_review_status (e.g. "In Review")
        """
        project = slot.project
        linear_cfg = self._config.linear
        poller = self._pollers.get(project)
        if poller and slot.ticket_id:
            # Determine target status based on PR state
            pr_status = self._check_pr_status(slot)
            if pr_status == "merged":
                target_status = linear_cfg.done_status
            else:
                target_status = linear_cfg.in_review_status

            try:
                poller.move_issue(slot.ticket_id, target_status)
                logger.info(
                    "Moved %s to '%s' after successful pipeline",
                    slot.ticket_id, target_status,
                )
            except Exception:
                logger.exception(
                    "Failed to move %s to '%s'", slot.ticket_id, target_status,
                )

            # Post completion comment if enabled
            if linear_cfg.comment_on_completion:
                self._post_completion_comment(poller, slot)

        # Webhook notification
        self._notify_task_completed(slot)

        insert_event(
            self._conn,
            event_type="slot_completed",
            detail=f"project={project}, slot={slot.slot_id}, "
            f"ticket={slot.ticket_id}",
        )
        self._conn.commit()

        self._slot_manager.free_slot(project, slot.slot_id)
        self._cleanup_slot_db(project, slot.slot_id)
        logger.info("Freed slot %s/%d after completion", project, slot.slot_id)

    def _handle_failed_slot(self, slot: SlotState) -> None:
        """Update Linear for a failed slot and free it.

        Before moving the ticket to failed status, checks if the PR was
        actually merged. If so, treats it as a successful completion
        instead of a failure (prevents infinite retry loops for
        already-merged PRs).
        """
        # Check if the PR was actually merged despite the reported failure.
        # This prevents infinite retry loops when merge reports failure
        # but the PR is already merged (e.g. post-merge checkout fails).
        pr_status = self._check_pr_status(slot)
        if pr_status == "merged":
            logger.info(
                "Failed slot %s/%d has a merged PR — treating as completed",
                slot.project, slot.slot_id,
            )
            # Update task status to completed in DB
            task_id = self._find_task_id(slot.ticket_id)
            if task_id is not None:
                update_task(self._conn, task_id, status="completed")
                self._conn.commit()
            self._handle_completed_slot(slot)
            return

        project = slot.project
        linear_cfg = self._config.linear
        poller = self._pollers.get(project)
        if poller and slot.ticket_id:
            target_status = linear_cfg.failed_status
            try:
                poller.move_issue(slot.ticket_id, target_status)
                logger.info(
                    "Moved %s to '%s' after failure",
                    slot.ticket_id, target_status,
                )
            except Exception:
                logger.exception(
                    "Failed to move %s to '%s'",
                    slot.ticket_id, target_status,
                )

            if linear_cfg.comment_on_failure:
                self._post_failure_comment(poller, slot)

        # Webhook notification
        self._notify_task_failed(slot)

        insert_event(
            self._conn,
            event_type="slot_failed",
            detail=f"project={project}, slot={slot.slot_id}, "
            f"ticket={slot.ticket_id}",
        )
        self._conn.commit()

        self._slot_manager.free_slot(project, slot.slot_id)
        self._cleanup_slot_db(project, slot.slot_id)
        logger.info("Freed slot %s/%d after failure", project, slot.slot_id)

    def _post_failure_comment(self, poller: LinearPoller, slot: SlotState) -> None:
        """Post a comment on the Linear issue with failure details."""
        stage = slot.stage or "unknown"
        reason = "unknown"

        # Get failure reason from DB task record
        task_id = self._find_task_id(slot.ticket_id)
        if task_id is not None:
            task = get_task(self._conn, task_id)
            if task and task["failure_reason"]:
                reason = task["failure_reason"]

        lines = [
            "**Botfarm worker failed**",
            "",
            f"- **Stage:** `{stage}`",
            f"- **Reason:** {reason}",
        ]
        try:
            poller.add_comment(slot.ticket_id, "\n".join(lines))
        except Exception:
            logger.exception(
                "Failed to post failure comment on %s", slot.ticket_id,
            )

    def _post_completion_comment(self, poller: LinearPoller, slot: SlotState) -> None:
        """Post a summary comment on the Linear issue after completion."""
        task_id = self._find_task_id(slot.ticket_id)
        lines = ["**Botfarm task completed**"]

        if task_id is not None:
            task = get_task(self._conn, task_id)
            if task:
                if task["started_at"] and task["completed_at"]:
                    try:
                        started = datetime.fromisoformat(
                            task["started_at"].replace("Z", "+00:00"),
                        )
                        completed = datetime.fromisoformat(
                            task["completed_at"].replace("Z", "+00:00"),
                        )
                        duration = completed - started
                        total_secs = int(duration.total_seconds())
                        if total_secs < 60:
                            lines.append(f"- **Duration:** {total_secs}s")
                        else:
                            lines.append(
                                f"- **Duration:** {total_secs // 60}m"
                            )
                    except (ValueError, TypeError):
                        pass

        # PR URL: prefer DB (cross-process reliable), fall back to slot state
        pr_url = None
        if task_id is not None and task and task["pr_url"]:
            pr_url = task["pr_url"]
        elif slot.pr_url:
            pr_url = slot.pr_url
        if pr_url:
            lines.append(f"- **PR:** {pr_url}")

        try:
            poller.add_comment(slot.ticket_id, "\n".join(lines))
        except Exception:
            logger.exception(
                "Failed to post completion comment on %s", slot.ticket_id,
            )

    # ------------------------------------------------------------------
    # Webhook notification helpers
    # ------------------------------------------------------------------

    def _notify_task_completed(self, slot: SlotState) -> None:
        """Send a webhook notification for a completed task."""
        task_id = self._find_task_id(slot.ticket_id)
        duration = None
        if task_id is not None:
            task = get_task(self._conn, task_id)
            if task:
                if task["started_at"] and task["completed_at"]:
                    try:
                        started = datetime.fromisoformat(
                            task["started_at"].replace("Z", "+00:00"),
                        )
                        completed = datetime.fromisoformat(
                            task["completed_at"].replace("Z", "+00:00"),
                        )
                        duration = (completed - started).total_seconds()
                    except (ValueError, TypeError):
                        pass
        # PR URL: prefer DB, fall back to slot state
        pr_url = None
        if task_id is not None:
            task = get_task(self._conn, task_id)
            if task and task["pr_url"]:
                pr_url = task["pr_url"]
        if not pr_url:
            pr_url = slot.pr_url
        self._notifier.notify_task_completed(
            ticket_id=slot.ticket_id or "unknown",
            title=slot.ticket_title or "unknown",
            duration_seconds=duration,
            pr_url=pr_url,
        )

    def _notify_task_failed(self, slot: SlotState) -> None:
        """Send a webhook notification for a failed task."""
        reason = None
        task_id = self._find_task_id(slot.ticket_id)
        if task_id is not None:
            task = get_task(self._conn, task_id)
            if task:
                reason = task["failure_reason"]
        self._notifier.notify_task_failed(
            ticket_id=slot.ticket_id or "unknown",
            title=slot.ticket_title or "unknown",
            failure_reason=reason,
        )

    # ------------------------------------------------------------------
    # Limit interruption handling
    # ------------------------------------------------------------------

    def _check_usage_api_for_limit(self) -> bool:
        """Force-poll the usage API and return True if thresholds are exceeded."""
        self._usage_poller.force_poll(self._conn)
        thresholds = self._config.usage_limits
        should_pause, _ = self._usage_poller.state.should_pause_with_thresholds(
            five_hour_threshold=thresholds.pause_five_hour_threshold,
            seven_day_threshold=thresholds.pause_seven_day_threshold,
            enabled=thresholds.enabled,
        )
        return should_pause

    def _handle_limit_hit(self, wr: _WorkerResult) -> None:
        """Handle a worker result that indicates a usage limit hit.

        Callers must call ``force_poll`` before invoking this method so that
        ``_compute_resume_after`` uses fresh usage data.
        """
        slot = self._slot_manager.get_slot(wr.project, wr.slot_id)

        # Compute resume_after from usage state
        resume_after = self._compute_resume_after()

        self._slot_manager.mark_paused_limit(
            wr.project, wr.slot_id, resume_after=resume_after,
        )

        # Look up task_id for this ticket
        task_id = self._find_task_id(slot.ticket_id) if slot else None

        # Increment limit_interruptions counter
        if task_id is not None:
            self._increment_limit_interruptions(task_id)

        insert_event(
            self._conn,
            task_id=task_id,
            event_type="limit_hit",
            detail=f"project={wr.project}, slot={wr.slot_id}, "
            f"stage={wr.failure_stage}, resume_after={resume_after}",
        )
        self._conn.commit()

        # Post limit pause comment if enabled
        if self._config.linear.comment_on_limit_pause and slot and slot.ticket_id:
            poller = self._pollers.get(wr.project)
            if poller:
                try:
                    poller.add_comment(
                        slot.ticket_id,
                        f"Botfarm worker paused due to usage limit hit "
                        f"at stage `{wr.failure_stage}`. "
                        f"Will resume automatically after limits reset.",
                    )
                except Exception:
                    logger.exception(
                        "Failed to post limit pause comment on %s",
                        slot.ticket_id,
                    )

        logger.warning(
            "Worker %s/%d paused due to limit hit at stage %s, "
            "resume_after=%s",
            wr.project, wr.slot_id, wr.failure_stage, resume_after,
        )

    def _compute_resume_after(self) -> str | None:
        """Compute the earliest time to resume based on usage resets + buffer.

        Adds a 2-minute buffer to the resets_at time from the usage API.
        """
        from datetime import timedelta

        state = self._usage_poller.state
        BUFFER_SECONDS = 120

        # Prefer the 5h window reset (shorter, more common). If only the 7d
        # window is set we use that; if the 7d limit was actually hit and
        # resets later, _handle_paused_slots re-checks usage before resuming.
        resets_at = state.resets_at_5h or state.resets_at_7d
        if not resets_at:
            return None

        try:
            reset_dt = datetime.fromisoformat(resets_at.replace("Z", "+00:00"))
            resume_dt = reset_dt + timedelta(seconds=BUFFER_SECONDS)
            return resume_dt.isoformat()
        except (ValueError, TypeError):
            return None

    def _find_task_id(self, ticket_id: str | None) -> int | None:
        """Look up the task_id for a ticket identifier."""
        if not ticket_id:
            return None
        from botfarm.db import get_task_by_ticket
        row = get_task_by_ticket(self._conn, ticket_id)
        return row["id"] if row else None

    def _increment_limit_interruptions(self, task_id: int) -> None:
        """Atomically increment the limit_interruptions counter on a task."""
        from botfarm.db import increment_limit_interruptions
        increment_limit_interruptions(self._conn, task_id)

    # ------------------------------------------------------------------
    # Resume paused slots
    # ------------------------------------------------------------------

    def _handle_paused_slots(self) -> None:
        """Check paused slots and resume them if their resume_after time has passed."""
        now = datetime.now(timezone.utc)
        paused = self._slot_manager.paused_limit_slots()

        polled = False
        for slot in paused:
            if not self._is_ready_to_resume(slot, now):
                continue

            # Force-poll once so all resume decisions use fresh data
            if not polled:
                self._usage_poller.force_poll(self._conn)
                polled = True

            # Re-check usage API to confirm limits have actually reset
            thresholds = self._config.usage_limits
            should_pause, _ = self._usage_poller.state.should_pause_with_thresholds(
                five_hour_threshold=thresholds.pause_five_hour_threshold,
                seven_day_threshold=thresholds.pause_seven_day_threshold,
                enabled=thresholds.enabled,
            )
            if should_pause:
                logger.debug(
                    "Slot %s/%d ready to resume but usage still high — waiting",
                    slot.project, slot.slot_id,
                )
                continue

            self._resume_paused_worker(slot)

    def _is_ready_to_resume(self, slot: SlotState, now: datetime) -> bool:
        """Check if a paused slot's resume_after time has passed."""
        if not slot.resume_after:
            # No resume_after set — allow resume immediately
            return True
        try:
            resume_dt = datetime.fromisoformat(
                slot.resume_after.replace("Z", "+00:00"),
            )
            return now >= resume_dt
        except (ValueError, TypeError):
            return True

    def _resume_paused_worker(self, slot: SlotState) -> None:
        """Resume a paused slot by re-spawning the worker for the interrupted stage."""
        project_name = slot.project
        project_cfg = self._projects.get(project_name)
        if not project_cfg:
            logger.error("No project config for %s — cannot resume", project_name)
            return

        task_id = self._find_task_id(slot.ticket_id)

        # Log the resume event
        insert_event(
            self._conn,
            task_id=task_id,
            event_type="limit_resumed",
            detail=f"project={project_name}, slot={slot.slot_id}, "
            f"stage={slot.stage}",
        )
        self._conn.commit()

        # Resume the slot back to busy
        self._slot_manager.resume_slot(project_name, slot.slot_id)

        cwd = self._slot_worktree_cwd(project_cfg, slot.slot_id)

        # Create per-ticket log directory (may already exist from prior run)
        ticket_log_dir = self._log_dir / slot.ticket_id
        ticket_log_dir.mkdir(parents=True, exist_ok=True)

        # Use the existing slot DB (seeded during initial dispatch).
        # Re-seed if the DB was cleaned up (e.g. after a crash).
        slot_db = self._slot_db_path(project_name, slot.slot_id)
        if not Path(slot_db).exists():
            slot_db = self._seed_slot_db(project_name, slot.slot_id)

        # Create a pause event for the resumed worker
        pause_event = multiprocessing.Event()
        key = (project_name, slot.slot_id)
        self._pause_events[key] = pause_event

        # Spawn a new worker that resumes from the interrupted stage
        proc = multiprocessing.Process(
            target=_worker_entry,
            kwargs={
                "ticket_id": slot.ticket_id,
                "ticket_title": slot.ticket_title or "",
                "task_id": task_id or 0,
                "project_name": project_name,
                "slot_id": slot.slot_id,
                "cwd": cwd,
                "log_dir": str(ticket_log_dir),
                "log_max_bytes": self._config.logging.max_bytes,
                "log_backup_count": self._config.logging.backup_count,
                "result_queue": self._result_queue,
                "max_turns": None,
                "max_review_iterations": self._config.agents.max_review_iterations,
                "max_ci_retries": self._config.agents.max_ci_retries,
                "resume_from_stage": slot.stage,
                "resume_session_id": slot.current_session_id,
                "placeholder_branch": self._slot_placeholder_branch(slot.slot_id),
                "slot_db_path": slot_db,
                "pause_event": pause_event,
            },
            daemon=False,
        )
        proc.start()
        self._workers[key] = proc
        self._slot_manager.set_pid(project_name, slot.slot_id, proc.pid)

        logger.info(
            "Resumed worker PID %d for %s in slot %s/%d at stage %s",
            proc.pid, slot.ticket_id, project_name, slot.slot_id, slot.stage,
        )

    # ------------------------------------------------------------------
    # Manual pause / resume
    # ------------------------------------------------------------------

    def _handle_manual_pause_resume(self) -> None:
        """Process manual pause/resume requests from the dashboard."""
        # Handle resume first — if both are set, resume cancels the pause
        if self._manual_resume_event.is_set():
            self._manual_resume_event.clear()
            self._manual_pause_event.clear()

            if self._manual_pause_requested:
                # Cancel in-flight pause: clear events so workers continue
                for event in self._pause_events.values():
                    event.clear()
                self._manual_pause_requested = False

            # Resume all manually-paused slots
            for slot in self._slot_manager.paused_manual_slots():
                self._resume_manual_paused_worker(slot)

            # Clear dispatch pause if it was set for manual pause
            if (
                self._slot_manager.dispatch_paused
                and self._slot_manager.dispatch_pause_reason == "manual_pause"
            ):
                logger.info("Manual resume: dispatch unpaused")
                self._slot_manager.set_dispatch_paused(False)
                insert_event(
                    self._conn,
                    event_type="manual_resumed",
                )
                self._conn.commit()
            return

        # Handle pause request
        if self._manual_pause_event.is_set():
            self._manual_pause_event.clear()
            self._manual_pause_requested = True

            # Stop new dispatches (or upgrade reason if already paused)
            if not self._slot_manager.dispatch_paused:
                logger.info("Manual pause requested — pausing dispatch")
                self._slot_manager.set_dispatch_paused(True, "manual_pause")
                insert_event(
                    self._conn,
                    event_type="manual_pause_requested",
                )
                self._conn.commit()
            elif self._slot_manager.dispatch_pause_reason != "manual_pause":
                logger.info(
                    "Manual pause requested — upgrading dispatch pause "
                    "reason from %s to manual_pause",
                    self._slot_manager.dispatch_pause_reason,
                )
                self._slot_manager.set_dispatch_paused(True, "manual_pause")
                insert_event(
                    self._conn,
                    event_type="manual_pause_requested",
                )
                self._conn.commit()

        # If manual pause is in progress, signal busy workers to stop
        if self._manual_pause_requested:
            busy = self._slot_manager.busy_slots()
            if not busy:
                # All workers have finished — pause is complete
                self._manual_pause_requested = False
                logger.info("Manual pause complete — all workers idle")
                insert_event(
                    self._conn,
                    event_type="manual_pause_complete",
                )
                self._conn.commit()
            else:
                # Signal busy workers to stop after current stage
                for slot in busy:
                    key = (slot.project, slot.slot_id)
                    event = self._pause_events.get(key)
                    if event is not None:
                        event.set()

    def _resume_manual_paused_worker(self, slot: SlotState) -> None:
        """Resume a manually-paused slot by re-spawning its worker."""
        project_name = slot.project
        project_cfg = self._projects.get(project_name)
        if not project_cfg:
            logger.error("No project config for %s — cannot resume", project_name)
            return

        task_id = self._find_task_id(slot.ticket_id)

        # Determine the next stage to resume from
        resume_stage = self._next_stage_after(slot.stages_completed)
        if resume_stage is None:
            # All stages were completed — shouldn't be paused_manual
            logger.warning(
                "Slot %s/%d has all stages completed but is paused_manual — freeing",
                project_name, slot.slot_id,
            )
            self._slot_manager.free_slot(project_name, slot.slot_id)
            return

        insert_event(
            self._conn,
            task_id=task_id,
            event_type="manual_resumed",
            detail=f"project={project_name}, slot={slot.slot_id}, "
            f"resume_stage={resume_stage}",
        )
        self._conn.commit()

        # Resume the slot back to busy
        self._slot_manager.resume_slot(project_name, slot.slot_id)

        cwd = self._slot_worktree_cwd(project_cfg, slot.slot_id)

        # Create per-ticket log directory (may already exist)
        ticket_log_dir = self._log_dir / slot.ticket_id
        ticket_log_dir.mkdir(parents=True, exist_ok=True)

        # Use the existing slot DB (seeded during initial dispatch)
        slot_db = self._slot_db_path(project_name, slot.slot_id)
        if not Path(slot_db).exists():
            slot_db = self._seed_slot_db(project_name, slot.slot_id)

        # Create a new pause event for the resumed worker
        pause_event = multiprocessing.Event()
        key = (project_name, slot.slot_id)
        self._pause_events[key] = pause_event

        proc = multiprocessing.Process(
            target=_worker_entry,
            kwargs={
                "ticket_id": slot.ticket_id,
                "ticket_title": slot.ticket_title or "",
                "task_id": task_id or 0,
                "project_name": project_name,
                "slot_id": slot.slot_id,
                "cwd": cwd,
                "log_dir": str(ticket_log_dir),
                "log_max_bytes": self._config.logging.max_bytes,
                "log_backup_count": self._config.logging.backup_count,
                "result_queue": self._result_queue,
                "max_turns": None,
                "max_review_iterations": self._config.agents.max_review_iterations,
                "max_ci_retries": self._config.agents.max_ci_retries,
                "resume_from_stage": resume_stage,
                "resume_session_id": slot.current_session_id,
                "placeholder_branch": self._slot_placeholder_branch(slot.slot_id),
                "slot_db_path": slot_db,
                "pause_event": pause_event,
            },
            daemon=False,
        )
        proc.start()
        self._workers[key] = proc
        self._slot_manager.set_pid(project_name, slot.slot_id, proc.pid)

        logger.info(
            "Resumed manually-paused worker PID %d for %s in slot %s/%d at stage %s",
            proc.pid, slot.ticket_id, project_name, slot.slot_id, resume_stage,
        )

    def request_pause(self) -> None:
        """Request a manual pause (thread-safe, called from dashboard)."""
        self._manual_pause_event.set()
        self._wake_event.set()

    def request_resume(self) -> None:
        """Request a manual resume (thread-safe, called from dashboard)."""
        self._manual_resume_event.set()
        self._wake_event.set()

    def request_update(self) -> None:
        """Request an update-and-restart cycle (thread-safe, called from dashboard)."""
        self._update_event.set()
        self._wake_event.set()

    def _handle_update_request(self) -> None:
        """Check for update request and execute update-and-restart flow.

        Flow: pause dispatch → wait for all workers to finish current stage
        and become idle → git pull + pip install → exit with code 42.
        """
        if not self._update_event.is_set():
            return

        from botfarm.git_update import UPDATE_EXIT_CODE, pull_and_install

        # Step 1: Pause dispatch if not already paused
        if not self._slot_manager.dispatch_paused:
            logger.info("Update requested — pausing dispatch for update")
            self._slot_manager.set_dispatch_paused(True, "update_in_progress")
            insert_event(
                self._conn,
                event_type="update_started",
                detail="pausing workers for update",
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

        if not pull_and_install():
            logger.error("Update failed — aborting restart")
            insert_event(
                self._conn,
                event_type="update_failed",
                detail="git pull or pip install failed",
            )
            self._conn.commit()
            self._update_event.clear()
            self._update_failed_event.set()
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
        state = self._usage_poller.poll(self._conn)
        if self._usage_poller.last_polled_fresh:
            self._slot_manager.set_usage(state.to_dict())

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

        # Utilization is below thresholds — resume if previously paused
        # (but don't override a manual pause)
        if self._slot_manager.dispatch_paused:
            if self._slot_manager.dispatch_pause_reason == "manual_pause":
                return  # Manual pause takes priority — don't auto-resume
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

        active_ids = self._slot_manager.active_ticket_ids()

        for project_name, poller in self._pollers.items():
            try:
                poll_result = poller.poll(active_ticket_ids=active_ids)
            except Exception:
                logger.exception(
                    "Failed to poll Linear for project %s", project_name,
                )
                continue

            # Persist queue for dashboard visibility
            save_queue_entries(self._conn, project_name, poll_result.candidates)

            # Auto-close parent issues whose children are all done.
            done_status = self._config.linear.done_status
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

            slot = self._slot_manager.find_free_slot_for_project(project_name)
            if slot is None:
                continue

            candidates = poll_result.candidates
            if not candidates:
                logger.debug("No candidates for %s", project_name)
                continue

            issue = candidates[0]
            self._dispatch_worker(project_name, slot, issue, poller)
            # Update active IDs so subsequent projects don't pick same ticket
            active_ids.add(issue.identifier)

        # Notify only on busy→idle transition (not every idle tick)
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
        poller: LinearPoller,
    ) -> None:
        """Assign a ticket to a slot and spawn a worker subprocess."""
        project_cfg = self._projects[project_name]
        cwd = self._slot_worktree_cwd(project_cfg, slot.slot_id)
        branch = f"{project_cfg.worktree_prefix}{slot.slot_id}"

        # Move issue to In Progress on Linear
        in_progress = self._config.linear.in_progress_status
        try:
            poller.move_issue(issue.identifier, in_progress)
        except Exception:
            logger.exception(
                "Failed to move %s to '%s' — skipping dispatch",
                issue.identifier, in_progress,
            )
            return

        # Assign ticket to slot
        self._slot_manager.assign_ticket(
            project_name,
            slot.slot_id,
            ticket_id=issue.identifier,
            ticket_title=issue.title,
            branch=branch,
        )

        # Create task record in DB
        task_id = insert_task(
            self._conn,
            ticket_id=issue.identifier,
            title=issue.title,
            project=project_name,
            slot=slot.slot_id,
            status="in_progress",
        )
        update_task(
            self._conn,
            task_id,
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        self._conn.commit()

        insert_event(
            self._conn,
            event_type="worker_dispatched",
            task_id=task_id,
            detail=f"ticket={issue.identifier}, project={project_name}, "
            f"slot={slot.slot_id}",
        )
        self._conn.commit()

        # Create per-ticket log directory
        ticket_log_dir = self._log_dir / issue.identifier
        ticket_log_dir.mkdir(parents=True, exist_ok=True)

        # Seed a sandboxed DB for this slot so the Claude subprocess's
        # schema migrations don't affect the production DB.
        slot_db = self._seed_slot_db(project_name, slot.slot_id)

        # Create a pause event for this worker
        pause_event = multiprocessing.Event()
        key = (project_name, slot.slot_id)
        self._pause_events[key] = pause_event

        # Spawn worker subprocess
        proc = multiprocessing.Process(
            target=_worker_entry,
            kwargs={
                "ticket_id": issue.identifier,
                "ticket_title": issue.title,
                "task_id": task_id,
                "project_name": project_name,
                "slot_id": slot.slot_id,
                "cwd": cwd,
                "log_dir": str(ticket_log_dir),
                "log_max_bytes": self._config.logging.max_bytes,
                "log_backup_count": self._config.logging.backup_count,
                "result_queue": self._result_queue,
                "max_turns": None,
                "max_review_iterations": self._config.agents.max_review_iterations,
                "max_ci_retries": self._config.agents.max_ci_retries,
                "placeholder_branch": self._slot_placeholder_branch(slot.slot_id),
                "slot_db_path": slot_db,
                "pause_event": pause_event,
            },
            daemon=False,  # Not daemon — survives supervisor exit
        )
        proc.start()
        self._workers[key] = proc
        self._slot_manager.set_pid(project_name, slot.slot_id, proc.pid)

        logger.info(
            "Dispatched worker PID %d for %s in slot %s/%d",
            proc.pid, issue.identifier, project_name, slot.slot_id,
        )

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def _shutdown(self) -> None:
        """Graceful shutdown: persist state, don't kill workers."""
        logger.info("Supervisor shutting down")

        # Persist state
        self._slot_manager.save()
        self._usage_poller.close()
        self._notifier.close()

        # Log running workers (we don't kill them)
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
    # Helpers
    # ------------------------------------------------------------------

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
        """Compute the sandboxed DB path for a slot.

        Returns ``~/.botfarm/slots/<project>-<slot_id>/botfarm.db``.
        """
        base = Path.home() / ".botfarm" / "slots" / f"{project_name}-{slot_id}"
        return str(base / "botfarm.db")

    def _seed_slot_db(self, project_name: str, slot_id: int) -> str:
        """Create a sandboxed DB for a slot by copying the production DB.

        Returns the slot DB path.
        """
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
    # Log maintenance
    # ------------------------------------------------------------------

    _LOG_CLEANUP_INTERVAL = 3600  # run at most once per hour

    def _cleanup_old_ticket_logs(self) -> None:
        """Remove per-ticket log directories older than the retention period.

        Runs at most once per hour to avoid unnecessary I/O.  Only
        directories directly under ``self._log_dir`` are considered;
        sub-items that are not directories (e.g. ``supervisor.log``) are
        skipped.
        """
        now = time.time()
        if now - self._last_log_cleanup < self._LOG_CLEANUP_INTERVAL:
            return
        self._last_log_cleanup = now

        retention_days = self._config.logging.ticket_log_retention_days
        cutoff = now - retention_days * 86400

        if not self._log_dir.is_dir():
            return

        # Collect ticket IDs currently being worked on so we never delete
        # logs for active slots.
        active_tickets: set[str] = set()
        for slot in self._slot_manager.all_slots():
            if slot.ticket_id and slot.status not in ("free",):
                active_tickets.add(slot.ticket_id)

        removed = 0
        for entry in self._log_dir.iterdir():
            if not entry.is_dir():
                continue
            # Skip known non-ticket directories (e.g. slot DB dirs)
            if entry.name in ("slot_dbs",):
                continue
            # Never delete logs for currently active tickets
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
