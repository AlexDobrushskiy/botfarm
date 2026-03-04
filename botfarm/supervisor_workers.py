"""Worker subprocess entry point and lifecycle managers.

Contains standalone helpers, dataclasses, and the two manager classes
that were already extracted from the Supervisor god-class:

* :class:`WorkerLifecycleManager` — worker dispatch, resume, timeout
* :class:`PauseResumeManager` — pause/resume state, stall detection
"""

from __future__ import annotations

import logging
import logging.handlers
import multiprocessing
import os
import queue as queue_mod
import signal
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from botfarm.config import IdentitiesConfig, resolve_stage_timeout
from botfarm.db import (
    init_db,
    insert_event,
    read_runtime_config,
    resolve_db_path,
    update_task,
)
from botfarm.slots import SlotState, _is_pid_alive
from botfarm.usage import UsagePoller
from botfarm.worker import STAGES, PipelineResult, run_pipeline

logger = logging.getLogger(__name__)

_COMMENT_MAX_LEN = 2000


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass
class _WorkerResult:
    """Message sent from a worker subprocess back to the supervisor."""

    project: str
    slot_id: int
    success: bool
    ticket_id: str = ""
    failure_stage: str | None = None
    failure_reason: str | None = None
    limit_hit: bool = False
    paused: bool = False
    stages_completed: list[str] | None = None
    no_pr_reason: str | None = None
    result_text: str | None = None


@dataclass
class _StallInfo:
    """Per-worker usage stall tracking data."""

    dispatch_usage_5h: float | None
    dispatch_time: float
    warned: bool = False


@dataclass
class StopSlotResult:
    """Result of a stop_slot() call."""

    success: bool
    pr_was_merged: bool = False
    pr_closed: bool = False
    ticket_id: str | None = None
    message: str = ""


# ---------------------------------------------------------------------------
# Process management helpers
# ---------------------------------------------------------------------------


def _collect_descendant_pids(pid: int) -> list[int]:
    """Return direct child PIDs of *pid* by reading /proc.

    This is Linux-specific (``/proc/<pid>/task/<pid>/children``).
    On other platforms the list will be empty.

    Note: only collects one level of children.  Grandchildren in their
    own sessions could theoretically be missed, but ``_kill_pids`` sends
    signals to the process *group* of each child which covers most
    practical cases (stage subprocesses use ``start_new_session=True``).
    """
    try:
        children_file = Path(f"/proc/{pid}/task/{pid}/children")
        if children_file.exists():
            return [int(p) for p in children_file.read_text().split() if p]
    except (OSError, ValueError):
        pass
    return []


def _kill_pids(pids: list[int], sig: int = signal.SIGKILL) -> None:
    """Send *sig* to each PID's process group (falling back to the PID itself).

    Used with a pre-collected list of descendant PIDs so that cleanup works
    even after the parent process has already exited (making /proc unavailable).
    """
    for cpid in pids:
        try:
            os.killpg(os.getpgid(cpid), sig)
        except (OSError, ProcessLookupError):
            try:
                os.kill(cpid, sig)
            except (OSError, ProcessLookupError):
                pass


def _kill_descendant_sessions(pid: int, sig: int = signal.SIGKILL) -> None:
    """Send *sig* to every descendant session/process-group of *pid*.

    Stage subprocesses use ``start_new_session=True``, so they live in
    their own session groups that survive the death of the worker.  This
    helper signals each child's process group to clean them up.
    """
    _kill_pids(_collect_descendant_pids(pid), sig)


def _kill_process_tree(pid: int) -> None:
    """SIGKILL a process and all its descendants.

    The worker creates its own process group (``os.setpgrp()``), and stage
    subprocesses use ``start_new_session=True`` which puts them in
    separate session/process groups.  We first kill child session groups,
    then SIGKILL the worker's own process group.
    """
    _kill_descendant_sessions(pid, signal.SIGKILL)

    # Kill the worker's own process group
    try:
        os.killpg(pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        try:
            os.kill(pid, signal.SIGKILL)
        except (OSError, ProcessLookupError):
            pass


def _truncate_for_comment(text: str) -> str:
    """Truncate text for a Linear comment, adding an indicator if trimmed."""
    if len(text) <= _COMMENT_MAX_LEN:
        return text
    return text[:_COMMENT_MAX_LEN] + "\n\n… *(truncated)*"


# ---------------------------------------------------------------------------
# Worker subprocess entry point
# ---------------------------------------------------------------------------


def _worker_entry(
    *,
    ticket_id: str,
    ticket_title: str,
    ticket_labels: list[str] | None = None,
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
    max_merge_conflict_retries: int = 2,
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,
    placeholder_branch: str | None = None,
    slot_db_path: str | None = None,
    pause_event: multiprocessing.Event | None = None,
    identities: IdentitiesConfig | None = None,
    codex_reviewer_enabled: bool = False,
    codex_reviewer_model: str = "",
    codex_reviewer_timeout_minutes: int = 15,
    codex_reviewer_skip_on_reiteration: bool = True,
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
    # Ignore SIGINT so only the supervisor handles Ctrl-C.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    # Move worker into its own process group so process-group signals
    # (kill, terminal close, systemd stop) don't reach it.  The
    # supervisor can still send SIGTERM directly via os.kill(pid, ...).
    os.setpgrp()

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

    # Read latest runtime config from DB to pick up dashboard changes
    # that occurred between supervisor snapshot and worker spawn.
    try:
        rt = read_runtime_config(conn)
        if "max_review_iterations" in rt:
            max_review_iterations = int(rt["max_review_iterations"])
        if "max_ci_retries" in rt:
            max_ci_retries = int(rt["max_ci_retries"])
        if "codex_reviewer_enabled" in rt:
            codex_reviewer_enabled = bool(rt["codex_reviewer_enabled"])
        if "codex_reviewer_model" in rt:
            codex_reviewer_model = str(rt["codex_reviewer_model"])
        if "codex_reviewer_timeout_minutes" in rt:
            codex_reviewer_timeout_minutes = int(rt["codex_reviewer_timeout_minutes"])
        if "codex_reviewer_skip_on_reiteration" in rt:
            codex_reviewer_skip_on_reiteration = bool(rt["codex_reviewer_skip_on_reiteration"])
        if "max_merge_conflict_retries" in rt:
            max_merge_conflict_retries = int(rt["max_merge_conflict_retries"])
    except Exception:
        logger.debug("Could not read runtime config from DB, using defaults")

    try:
        result: PipelineResult = run_pipeline(
            ticket_id=ticket_id,
            ticket_labels=ticket_labels or [],
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
            max_merge_conflict_retries=max_merge_conflict_retries,
            resume_from_stage=resume_from_stage,
            resume_session_id=resume_session_id,
            slot_db_path=slot_db_path,
            pause_event=pause_event,
            identities=identities,
            codex_reviewer_enabled=codex_reviewer_enabled,
            codex_reviewer_model=codex_reviewer_model,
            codex_reviewer_timeout_minutes=codex_reviewer_timeout_minutes,
            codex_reviewer_skip_on_reiteration=codex_reviewer_skip_on_reiteration,
        )
        if result.paused:
            result_queue.put(_WorkerResult(
                project=project_name, slot_id=slot_id, ticket_id=ticket_id,
                success=False,
                paused=True,
                stages_completed=result.stages_completed,
            ))
            logger.info(
                "Worker %s/%d paused by manual request for %s",
                project_name, slot_id, ticket_id,
            )
        elif result.success:
            result_queue.put(_WorkerResult(
                project=project_name, slot_id=slot_id, ticket_id=ticket_id,
                success=True,
                no_pr_reason=result.no_pr_reason,
                result_text=result.result_text,
            ))
            logger.info(
                "Worker %s/%d finished successfully for %s",
                project_name, slot_id, ticket_id,
            )
        else:
            # Check if the failure looks like a limit hit
            limit_hit = _check_limit_hit(result)
            result_queue.put(_WorkerResult(
                project=project_name, slot_id=slot_id, ticket_id=ticket_id,
                success=False,
                failure_stage=result.failure_stage,
                failure_reason=result.failure_reason,
                limit_hit=limit_hit,
                result_text=result.result_text,
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
            project=project_name, slot_id=slot_id, ticket_id=ticket_id,
            success=False,
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
# WorkerLifecycleManager — owns worker dispatch, resume, timeout, spawn
# ---------------------------------------------------------------------------


class WorkerLifecycleManager:
    """Manages worker dispatch, resume (limit/crash/manual), timeout enforcement,
    and the shared ``_spawn_worker()`` helper.

    This is an internal implementation detail of :class:`Supervisor`.
    """

    def __init__(self, supervisor: "Supervisor") -> None:
        self._sup = supervisor

    # -- Convenience accessors for shared state ----------------------------

    @property
    def _slot_manager(self):
        return self._sup._slot_manager

    @property
    def _conn(self):
        return self._sup._conn

    @property
    def _config(self):
        return self._sup._config

    @property
    def _projects(self):
        return self._sup._projects

    @property
    def _workers(self):
        return self._sup._workers

    @property
    def _pause_events(self):
        return self._sup._pause_events

    @property
    def _log_dir(self):
        return self._sup._log_dir

    @property
    def _result_queue(self):
        return self._sup._result_queue

    # -- Shared worker spawn -----------------------------------------------

    def spawn_worker(
        self,
        *,
        project_name: str,
        slot_id: int,
        ticket_id: str,
        ticket_title: str,
        ticket_labels: list[str],
        task_id: int,
        resume_from_stage: str | None = None,
        resume_session_id: str | None = None,
        slot_db: str | None = None,
    ) -> multiprocessing.Process:
        """Spawn a worker subprocess — shared logic for dispatch and resume paths.

        Handles: cwd resolution, log directory creation, pause event setup,
        ``multiprocessing.Process`` construction, ``start()``, PID tracking.

        When *slot_db* is ``None`` a fresh sandboxed DB is seeded from the
        production DB.  Callers that want to reuse an existing slot DB
        should pass the path explicitly.

        Returns the started ``Process``.
        """
        from botfarm.supervisor import Supervisor

        project_cfg = self._projects[project_name]
        cwd = Supervisor._slot_worktree_cwd(project_cfg, slot_id)

        ticket_log_dir = self._log_dir / ticket_id
        ticket_log_dir.mkdir(parents=True, exist_ok=True)

        if slot_db is None:
            slot_db = self._sup._seed_slot_db(project_name, slot_id)

        pause_event = multiprocessing.Event()
        key = (project_name, slot_id)
        self._pause_events[key] = pause_event

        proc = multiprocessing.Process(
            target=_worker_entry,
            kwargs={
                "ticket_id": ticket_id,
                "ticket_title": ticket_title,
                "ticket_labels": ticket_labels,
                "task_id": task_id,
                "project_name": project_name,
                "slot_id": slot_id,
                "cwd": cwd,
                "log_dir": str(ticket_log_dir),
                "log_max_bytes": self._config.logging.max_bytes,
                "log_backup_count": self._config.logging.backup_count,
                "result_queue": self._result_queue,
                "max_turns": None,
                "max_review_iterations": self._config.agents.max_review_iterations,
                "max_ci_retries": self._config.agents.max_ci_retries,
                "max_merge_conflict_retries": self._config.agents.max_merge_conflict_retries,
                "resume_from_stage": resume_from_stage,
                "resume_session_id": resume_session_id,
                "placeholder_branch": Supervisor._slot_placeholder_branch(slot_id),
                "slot_db_path": slot_db,
                "pause_event": pause_event,
                "identities": self._config.identities,
                "codex_reviewer_enabled": self._config.agents.codex_reviewer_enabled,
                "codex_reviewer_model": self._config.agents.codex_reviewer_model,
                "codex_reviewer_timeout_minutes": self._config.agents.codex_reviewer_timeout_minutes,
                "codex_reviewer_skip_on_reiteration": self._config.agents.codex_reviewer_skip_on_reiteration,
            },
            daemon=False,
        )
        proc.start()
        self._workers[key] = proc
        self._slot_manager.set_pid(project_name, slot_id, proc.pid)

        return proc

    # -- Dispatch ----------------------------------------------------------

    def dispatch_worker(
        self,
        project_name: str,
        slot: SlotState,
        issue,
        poller,
    ) -> None:
        """Assign a ticket to a slot and spawn a worker subprocess."""
        from botfarm.db import insert_task

        sup = self._sup
        project_cfg = self._projects[project_name]
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

        # Auto-assign ticket to coder bot (best-effort)
        if sup._coder_linear:
            try:
                sup._coder_linear.assign_issue(issue.identifier, sup._coder_viewer_id)
            except Exception:
                logger.warning("Failed to assign %s to coder bot — continuing", issue.identifier)

        # Assign ticket to slot (with labels for timeout overrides)
        self._slot_manager.assign_ticket(
            project_name,
            slot.slot_id,
            ticket_id=issue.identifier,
            ticket_title=issue.title,
            branch=branch,
            ticket_labels=issue.labels or [],
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
        on_extra = sup._usage_poller.state.is_on_extra_usage
        update_task(
            self._conn,
            task_id,
            started_at=datetime.now(timezone.utc).isoformat(),
            started_on_extra_usage=int(on_extra),
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

        # Capture full ticket details for history
        sup._capture_ticket_history(
            issue.identifier, "dispatch",
            client=poller._client, branch_name=branch,
        )

        # Spawn worker subprocess
        proc = self.spawn_worker(
            project_name=project_name,
            slot_id=slot.slot_id,
            ticket_id=issue.identifier,
            ticket_title=issue.title,
            ticket_labels=issue.labels or [],
            task_id=task_id,
        )

        logger.info(
            "Dispatched worker PID %d for %s in slot %s/%d",
            proc.pid, issue.identifier, project_name, slot.slot_id,
        )

        # Record usage at dispatch for stall detection
        sup._stall_tracking[(project_name, slot.slot_id)] = _StallInfo(
            dispatch_usage_5h=sup._usage_poller.state.utilization_5h,
            dispatch_time=time.time(),
        )

    # -- Resume (limit-paused) --------------------------------------------

    def resume_paused_worker(self, slot: SlotState) -> None:
        """Resume a paused slot by re-spawning the worker for the interrupted stage."""
        from botfarm.supervisor import Supervisor

        project_name = slot.project
        project_cfg = self._projects.get(project_name)
        if not project_cfg:
            logger.error("No project config for %s — cannot resume", project_name)
            return

        task_id = self._sup._find_task_id(slot.ticket_id)

        insert_event(
            self._conn,
            task_id=task_id,
            event_type="limit_resumed",
            detail=f"project={project_name}, slot={slot.slot_id}, "
            f"stage={slot.stage}",
        )
        self._conn.commit()

        self._slot_manager.resume_slot(project_name, slot.slot_id)

        slot_db = Supervisor._slot_db_path(project_name, slot.slot_id)
        if not Path(slot_db).exists():
            slot_db = self._sup._seed_slot_db(project_name, slot.slot_id)

        proc = self.spawn_worker(
            project_name=project_name,
            slot_id=slot.slot_id,
            ticket_id=slot.ticket_id,
            ticket_title=slot.ticket_title or "",
            ticket_labels=slot.ticket_labels or [],
            task_id=task_id or 0,
            resume_from_stage=slot.stage,
            resume_session_id=slot.current_session_id,
            slot_db=slot_db,
        )

        logger.info(
            "Resumed worker PID %d for %s in slot %s/%d at stage %s",
            proc.pid, slot.ticket_id, project_name, slot.slot_id, slot.stage,
        )

    # -- Resume (crash-recovered) -----------------------------------------

    def resume_recovered_worker(
        self, slot: SlotState, *, resume_from_stage: str,
    ) -> None:
        """Resume a crash-recovered worker by spawning a new subprocess."""
        from botfarm.supervisor import Supervisor

        project_name = slot.project
        project_cfg = self._projects.get(project_name)
        if not project_cfg:
            logger.error("No project config for %s — cannot resume recovered worker", project_name)
            self._sup._mark_recovery_failed(
                slot, reason="no_project_config",
                failure_msg=f"No project config for {project_name}",
            )
            return

        task_id = self._sup._find_task_id(slot.ticket_id)

        insert_event(
            self._conn,
            task_id=task_id,
            event_type="recovery_resumed",
            detail=f"project={project_name}, slot={slot.slot_id}, "
            f"pid={slot.pid}, resume_from={resume_from_stage}, "
            f"stages_completed={','.join(slot.stages_completed)}",
        )
        self._conn.commit()

        slot.pid = None
        self._slot_manager.update_stage(
            project_name, slot.slot_id, stage=resume_from_stage,
        )

        slot_db = Supervisor._slot_db_path(project_name, slot.slot_id)
        if not Path(slot_db).exists():
            slot_db = self._sup._seed_slot_db(project_name, slot.slot_id)

        proc = self.spawn_worker(
            project_name=project_name,
            slot_id=slot.slot_id,
            ticket_id=slot.ticket_id,
            ticket_title=slot.ticket_title or "",
            ticket_labels=slot.ticket_labels or [],
            task_id=task_id or 0,
            resume_from_stage=resume_from_stage,
            resume_session_id=slot.current_session_id,
            slot_db=slot_db,
        )

        logger.info(
            "Resumed recovered worker PID %d for %s in slot %s/%d at stage %s",
            proc.pid, slot.ticket_id, project_name, slot.slot_id,
            resume_from_stage,
        )

    # -- Resume (manual-paused) -------------------------------------------

    def resume_manual_paused_worker(self, slot: SlotState) -> None:
        """Resume a manually-paused slot by re-spawning its worker."""
        from botfarm.supervisor import Supervisor

        project_name = slot.project
        project_cfg = self._projects.get(project_name)
        if not project_cfg:
            logger.error("No project config for %s — cannot resume", project_name)
            return

        task_id = self._sup._find_task_id(slot.ticket_id)

        resume_stage = Supervisor._next_stage_after(slot.stages_completed)
        if resume_stage is None:
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

        self._slot_manager.resume_slot(project_name, slot.slot_id)

        slot_db = Supervisor._slot_db_path(project_name, slot.slot_id)
        if not Path(slot_db).exists():
            slot_db = self._sup._seed_slot_db(project_name, slot.slot_id)

        proc = self.spawn_worker(
            project_name=project_name,
            slot_id=slot.slot_id,
            ticket_id=slot.ticket_id,
            ticket_title=slot.ticket_title or "",
            ticket_labels=slot.ticket_labels or [],
            task_id=task_id or 0,
            resume_from_stage=resume_stage,
            resume_session_id=slot.current_session_id,
            slot_db=slot_db,
        )

        logger.info(
            "Resumed manually-paused worker PID %d for %s in slot %s/%d at stage %s",
            proc.pid, slot.ticket_id, project_name, slot.slot_id, resume_stage,
        )

    # -- Timeout enforcement -----------------------------------------------

    def check_timeouts(self) -> None:
        """Kill worker processes that have exceeded their stage timeout."""
        agents_cfg = self._config.agents
        grace = agents_cfg.timeout_grace_seconds
        now = datetime.now(timezone.utc)

        for slot in self._slot_manager.busy_slots():
            if slot.pid is None or slot.stage is None:
                continue

            if slot.sigterm_sent_at:
                self._maybe_escalate_kill(slot, now=now, grace=grace)
                continue

            if not slot.stage_started_at:
                continue

            limit_minutes = resolve_stage_timeout(
                agents_cfg, slot.stage, slot.ticket_labels,
            )
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
            os.kill(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            pass

        from botfarm.slots import _now_iso
        slot.sigterm_sent_at = _now_iso()
        self._slot_manager.save()

        task_id = self._sup._find_task_id(slot.ticket_id)
        insert_event(
            self._conn,
            task_id=task_id,
            event_type="timeout",
            detail=f"stage={stage}, elapsed={elapsed:.0f}s, limit={int(limit)}s",
        )
        self._conn.commit()

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
            sigterm_time = now

        if (now - sigterm_time).total_seconds() < grace:
            return

        pid = slot.pid
        stage = slot.stage
        project = slot.project

        try:
            os.kill(pid, 0)
            os.kill(pid, signal.SIGKILL)
            logger.warning(
                "Worker PID %d did not exit after SIGTERM, sent SIGKILL", pid,
            )
        except (OSError, ProcessLookupError):
            pass

        key = (project, slot.slot_id)
        proc = self._workers.pop(key, None)
        if proc is not None:
            proc.join(timeout=2)

        reason = f"timeout in stage {stage}"
        self._slot_manager.mark_failed(project, slot.slot_id, reason=reason)

        task_id = self._sup._find_task_id(slot.ticket_id)
        if task_id is not None:
            update_task(
                self._conn,
                task_id,
                status="failed",
                failure_reason=reason,
            )
        self._conn.commit()


# ---------------------------------------------------------------------------
# PauseResumeManager — owns pause/resume state and stall tracking
# ---------------------------------------------------------------------------


class PauseResumeManager:
    """Manages paused-slot resumption, manual pause/resume, and usage stall detection.

    This is an internal implementation detail of :class:`Supervisor`.
    It is constructed with references to shared state and the owning
    supervisor instance.
    """

    _STALL_WARN_MINUTES = 30
    _STALL_EPSILON = 0.005  # 0.5%

    def __init__(self, supervisor: "Supervisor") -> None:
        self._sup = supervisor

    # -- Convenience accessors for shared state ----------------------------

    @property
    def _slot_manager(self):
        return self._sup._slot_manager

    @property
    def _usage_poller(self):
        return self._sup._usage_poller

    @property
    def _conn(self):
        return self._sup._conn

    @property
    def _config(self):
        return self._sup._config

    @property
    def _pollers(self):
        return self._sup._pollers

    @property
    def _pause_events(self):
        return self._sup._pause_events

    @property
    def _stall_tracking(self):
        return self._sup._stall_tracking

    # -- Paused-limit slot handling ----------------------------------------

    def handle_paused_slots(self) -> None:
        """Check paused slots and resume them if their resume_after time has passed."""
        now = datetime.now(timezone.utc)
        paused = self._slot_manager.paused_limit_slots()
        thresholds = self._config.usage_limits

        polled = False
        for slot in paused:
            # Check if the ticket was resolved externally while paused
            if self._sup._is_ticket_externally_done(slot):
                self._conn.commit()
                continue

            # If limits are disabled entirely, resume immediately
            if not thresholds.enabled:
                self._sup._resume_paused_worker(slot)
                continue

            if not self.is_ready_to_resume(slot, now):
                continue

            # Force-poll once so all resume decisions use fresh data
            if not polled:
                self._usage_poller.force_poll(self._conn)
                polled = True

            # Re-check usage API to confirm limits have actually reset
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

            self._sup._resume_paused_worker(slot)

    @staticmethod
    def is_ready_to_resume(slot: SlotState, now: datetime) -> bool:
        """Check if a paused slot's resume_after time has passed."""
        if not slot.resume_after:
            return True
        try:
            resume_dt = datetime.fromisoformat(
                slot.resume_after.replace("Z", "+00:00"),
            )
            return now >= resume_dt
        except (ValueError, TypeError):
            return True

    @staticmethod
    def compute_resume_after(usage_poller: UsagePoller) -> str | None:
        """Compute the earliest time to resume based on usage resets + buffer."""
        from datetime import timedelta

        state = usage_poller.state
        BUFFER_SECONDS = 120

        resets_at = state.resets_at_5h or state.resets_at_7d
        if not resets_at:
            return None

        try:
            reset_dt = datetime.fromisoformat(resets_at.replace("Z", "+00:00"))
            resume_dt = reset_dt + timedelta(seconds=BUFFER_SECONDS)
            return resume_dt.isoformat()
        except (ValueError, TypeError):
            return None

    # -- Manual pause / resume ---------------------------------------------

    def handle_manual_pause_resume(self) -> None:
        """Process manual pause/resume requests from the dashboard."""
        sup = self._sup

        # Handle resume first — if both are set, resume cancels the pause
        if sup._manual_resume_event.is_set():
            sup._manual_resume_event.clear()
            sup._manual_pause_event.clear()

            if sup._manual_pause_requested:
                # Cancel in-flight pause: clear events so workers continue
                for event in self._pause_events.values():
                    event.clear()
                sup._manual_pause_requested = False

            # Resume all manually-paused slots
            for slot in self._slot_manager.paused_manual_slots():
                sup._resume_manual_paused_worker(slot)

            # Clear dispatch pause if it was set for manual pause or start_paused
            if (
                self._slot_manager.dispatch_paused
                and self._slot_manager.dispatch_pause_reason
                in ("manual_pause", "start_paused")
            ):
                logger.info("Manual resume: dispatch unpaused")
                self._slot_manager.set_dispatch_paused(False)
                self._sup._startup_paused = False
                insert_event(
                    self._conn,
                    event_type="manual_resumed",
                )
                self._conn.commit()
            return

        # Handle pause request
        if sup._manual_pause_event.is_set():
            sup._manual_pause_event.clear()
            sup._manual_pause_requested = True

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
        if sup._manual_pause_requested:
            busy = self._slot_manager.busy_slots()
            if not busy:
                sup._manual_pause_requested = False
                logger.info("Manual pause complete — all workers idle")
                insert_event(
                    self._conn,
                    event_type="manual_pause_complete",
                )
                self._conn.commit()
            else:
                for slot in busy:
                    key = (slot.project, slot.slot_id)
                    event = self._pause_events.get(key)
                    if event is not None:
                        event.set()

    # -- Usage stall detection ---------------------------------------------

    def check_usage_stalls(self) -> None:
        """Warn if a busy worker's usage% hasn't moved since dispatch."""
        current_5h = self._usage_poller.state.utilization_5h
        if current_5h is None:
            return

        now = time.time()
        busy_keys = {
            (s.project, s.slot_id) for s in self._slot_manager.busy_slots()
        }

        # Clean up tracking for slots that are no longer busy
        stale = [k for k in self._stall_tracking if k not in busy_keys]
        for k in stale:
            del self._stall_tracking[k]

        for key, info in self._stall_tracking.items():
            if key not in busy_keys:
                continue
            if info.warned:
                continue
            if info.dispatch_usage_5h is None:
                continue

            elapsed_min = (now - info.dispatch_time) / 60
            if elapsed_min < self._STALL_WARN_MINUTES:
                continue

            delta = abs(current_5h - info.dispatch_usage_5h)
            if delta < self._STALL_EPSILON:
                info.warned = True
                slot = self._slot_manager.get_slot(key[0], key[1])
                ticket = slot.ticket_id if slot else "?"
                logger.warning(
                    "Worker %s (%s/%d) usage stalled: "
                    "5h=%.1f%% unchanged for %d minutes since dispatch",
                    ticket,
                    key[0],
                    key[1],
                    current_5h * 100,
                    int(elapsed_min),
                )
