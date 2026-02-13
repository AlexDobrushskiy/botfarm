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
import multiprocessing
import os
import queue as queue_mod
import signal
import sqlite3
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

from botfarm.config import BotfarmConfig, ProjectConfig
from botfarm.db import init_db, insert_event, insert_task, update_task
from botfarm.linear import LinearPoller, create_pollers
from botfarm.slots import SlotManager, SlotState, _is_pid_alive
from botfarm.usage import DEFAULT_PAUSE_5H_THRESHOLD, DEFAULT_PAUSE_7D_THRESHOLD, UsagePoller
from botfarm.worker import PipelineResult, run_pipeline

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


def _worker_entry(
    *,
    ticket_id: str,
    ticket_title: str,
    task_id: int,
    project_name: str,
    slot_id: int,
    cwd: str,
    db_path: str,
    result_queue: multiprocessing.Queue,
    max_turns: dict[str, int] | None,
    max_review_iterations: int = 3,
    max_ci_retries: int = 2,
    resume_from_stage: str | None = None,
    resume_session_id: str | None = None,
) -> None:
    """Entry point for a worker subprocess.

    This runs ``run_pipeline`` with its own DB connection. Results are
    communicated back to the supervisor via ``result_queue`` so only
    the supervisor writes to the shared state file.

    When ``resume_from_stage`` is set, the pipeline skips stages that
    were already completed and resumes from the specified stage.
    """
    # Ignore SIGINT so only the supervisor handles Ctrl-C
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    conn = init_db(db_path)

    try:
        result: PipelineResult = run_pipeline(
            ticket_id=ticket_id,
            task_id=task_id,
            cwd=cwd,
            conn=conn,
            max_turns=max_turns,
            max_review_iterations=max_review_iterations,
            max_ci_retries=max_ci_retries,
            resume_from_stage=resume_from_stage,
            resume_session_id=resume_session_id,
        )
        if result.success:
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

        # Slot manager
        self._slot_manager = SlotManager(
            state_path=config.state_file,
        )
        for project in config.projects:
            for sid in project.slots:
                self._slot_manager.register_slot(project.name, sid)
        self._slot_manager.load()

        # Database
        db_path = Path(config.database.path).expanduser()
        self._db_path = str(db_path)
        self._conn: sqlite3.Connection = init_db(db_path)

        # Usage poller
        self._usage_poller = UsagePoller()

        # Track child processes: (project, slot_id) -> Process
        self._workers: dict[tuple[str, int], multiprocessing.Process] = {}

        # Queue for worker results — workers send _WorkerResult here
        self._result_queue: multiprocessing.Queue = multiprocessing.Queue()

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
        Finally, reconcile SQLite task records with state.json.
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

        # Single commit for all recovery operations (sub-methods do not commit individually)
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
        """Determine outcome for a dead worker and update slot state."""
        project = slot.project
        slot_id = slot.slot_id
        ticket_id = slot.ticket_id
        old_pid = slot.pid

        # Check if a PR was created (indicates the worker got far enough)
        pr_status = self._check_pr_status(slot)

        if pr_status == "merged":
            logger.info(
                "Dead worker PID %d for %s/%d: PR was merged — marking completed",
                old_pid, project, slot_id,
            )
            self._slot_manager.mark_completed(project, slot_id)
            task_id = self._find_task_id(ticket_id)
            if task_id is not None:
                update_task(self._conn, task_id, status="completed")
            insert_event(
                self._conn,
                task_id=task_id,
                event_type="recovery_completed",
                detail=f"project={project}, slot={slot_id}, "
                f"pid={old_pid}, reason=pr_merged",
            )
        elif pr_status == "open":
            logger.info(
                "Dead worker PID %d for %s/%d: PR is open — marking completed",
                old_pid, project, slot_id,
            )
            self._slot_manager.mark_completed(project, slot_id)
            task_id = self._find_task_id(ticket_id)
            if task_id is not None:
                update_task(self._conn, task_id, status="completed")
            insert_event(
                self._conn,
                task_id=task_id,
                event_type="recovery_completed",
                detail=f"project={project}, slot={slot_id}, "
                f"pid={old_pid}, reason=pr_open",
            )
        elif pr_status == "closed":
            logger.warning(
                "Dead worker PID %d for %s/%d: PR was closed without merging — marking failed",
                old_pid, project, slot_id,
            )
            self._slot_manager.mark_failed(
                project, slot_id,
                reason=f"worker PID {old_pid} died (PR closed without merging)",
            )
            task_id = self._find_task_id(ticket_id)
            if task_id is not None:
                update_task(
                    self._conn, task_id,
                    status="failed",
                    failure_reason=f"worker PID {old_pid} died (PR closed without merging)",
                )
            insert_event(
                self._conn,
                task_id=task_id,
                event_type="recovery_failed",
                detail=f"project={project}, slot={slot_id}, "
                f"pid={old_pid}, reason=pr_closed",
            )
        else:
            logger.warning(
                "Dead worker PID %d for %s/%d: no PR found — marking failed",
                old_pid, project, slot_id,
            )
            self._slot_manager.mark_failed(
                project, slot_id,
                reason=f"worker PID {old_pid} died (no PR found)",
            )
            task_id = self._find_task_id(ticket_id)
            if task_id is not None:
                update_task(
                    self._conn, task_id,
                    status="failed",
                    failure_reason=f"worker PID {old_pid} died during recovery",
                )
            insert_event(
                self._conn,
                task_id=task_id,
                event_type="recovery_failed",
                detail=f"project={project}, slot={slot_id}, "
                f"pid={old_pid}, reason=no_pr",
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
        (no PR found).  Uses the PR URL from state if available,
        otherwise checks via ``gh pr view`` in the project working
        directory.
        """
        # Try the stored PR URL first
        pr_ref = slot.pr_url
        if not pr_ref:
            # Fall back to checking via gh in the project directory
            project_cfg = self._projects.get(slot.project)
            if project_cfg:
                cwd = str(Path(project_cfg.base_dir).expanduser())
                pr_ref = self._gh_pr_url_for_branch(slot.branch, cwd)

        if not pr_ref:
            return None

        return self._gh_pr_state(pr_ref)

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

    def run(self) -> None:
        """Run the supervisor main loop until shutdown is requested."""
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

        # Recover from previous state before entering main loop
        self._recover_on_startup()

        # Start dashboard if enabled
        self._dashboard_thread = None
        if self._config.dashboard.enabled:
            from botfarm.dashboard import start_dashboard
            self._dashboard_thread = start_dashboard(
                self._config.dashboard,
                state_file=self._config.state_file,
                db_path=self._config.database.path,
                linear_workspace=self._config.linear.workspace,
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

    def _tick(self) -> None:
        """One iteration of the supervisor loop."""
        for phase in (
            self._reconcile_workers,
            self._check_timeouts,
            self._handle_finished_slots,
            self._handle_paused_slots,
            self._poll_usage,
            self._poll_and_dispatch,
        ):
            try:
                phase()
            except Exception:
                logger.exception("%s failed", getattr(phase, "__name__", repr(phase)))

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
            if wr.success:
                self._slot_manager.mark_completed(wr.project, wr.slot_id)
                logger.info(
                    "Worker result: %s/%d completed", wr.project, wr.slot_id,
                )
            elif wr.limit_hit:
                self._handle_limit_hit(wr)
            else:
                reason = f"{wr.failure_stage}: {wr.failure_reason}"
                self._slot_manager.mark_failed(
                    wr.project, wr.slot_id, reason=reason,
                )
                logger.warning(
                    "Worker result: %s/%d failed — %s",
                    wr.project, wr.slot_id, reason,
                )

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
        """Update Linear for a completed slot and free it."""
        project = slot.project
        poller = self._pollers.get(project)
        if poller and slot.ticket_id:
            try:
                poller.move_issue(slot.ticket_id, "In Review")
                logger.info(
                    "Moved %s to 'In Review' after successful pipeline",
                    slot.ticket_id,
                )
            except Exception:
                logger.exception(
                    "Failed to move %s to 'In Review'", slot.ticket_id,
                )

        insert_event(
            self._conn,
            event_type="slot_completed",
            detail=f"project={project}, slot={slot.slot_id}, "
            f"ticket={slot.ticket_id}",
        )
        self._conn.commit()

        self._slot_manager.free_slot(project, slot.slot_id)
        logger.info("Freed slot %s/%d after completion", project, slot.slot_id)

    def _handle_failed_slot(self, slot: SlotState) -> None:
        """Update Linear for a failed slot and free it."""
        project = slot.project
        poller = self._pollers.get(project)
        if poller and slot.ticket_id:
            try:
                poller.move_issue(slot.ticket_id, "Todo")
                poller.add_comment(
                    slot.ticket_id,
                    "Botfarm worker failed. Moving back to Todo for retry.",
                )
                logger.info(
                    "Moved %s back to 'Todo' after failure", slot.ticket_id,
                )
            except Exception:
                logger.exception(
                    "Failed to update Linear for failed ticket %s",
                    slot.ticket_id,
                )

        insert_event(
            self._conn,
            event_type="slot_failed",
            detail=f"project={project}, slot={slot.slot_id}, "
            f"ticket={slot.ticket_id}",
        )
        self._conn.commit()

        self._slot_manager.free_slot(project, slot.slot_id)
        logger.info("Freed slot %s/%d after failure", project, slot.slot_id)

    # ------------------------------------------------------------------
    # Limit interruption handling
    # ------------------------------------------------------------------

    def _handle_limit_hit(self, wr: _WorkerResult) -> None:
        """Handle a worker result that indicates a usage limit hit."""
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

        for slot in paused:
            if not self._is_ready_to_resume(slot, now):
                continue

            # Re-check usage API to confirm limits have actually reset
            thresholds = self._config.usage_limits
            should_pause, _ = self._usage_poller.state.should_pause_with_thresholds(
                five_hour_threshold=thresholds.pause_five_hour_threshold,
                seven_day_threshold=thresholds.pause_seven_day_threshold,
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

        cwd = str(Path(project_cfg.base_dir).expanduser())

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
                "db_path": self._db_path,
                "result_queue": self._result_queue,
                "max_turns": None,
                "max_review_iterations": self._config.agents.max_review_iterations,
                "max_ci_retries": self._config.agents.max_ci_retries,
                "resume_from_stage": slot.stage,
                "resume_session_id": slot.current_session_id,
            },
            daemon=False,
        )
        proc.start()
        self._workers[(project_name, slot.slot_id)] = proc
        self._slot_manager.set_pid(project_name, slot.slot_id, proc.pid)

        logger.info(
            "Resumed worker PID %d for %s in slot %s/%d at stage %s",
            proc.pid, slot.ticket_id, project_name, slot.slot_id, slot.stage,
        )

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
            return

        # Utilization is below thresholds — resume if previously paused
        if self._slot_manager.dispatch_paused:
            prev_reason = self._slot_manager.dispatch_pause_reason
            logger.info("Dispatch resumed — utilization dropped below thresholds")
            self._slot_manager.set_dispatch_paused(False)
            insert_event(
                self._conn,
                event_type="dispatch_resumed",
                detail=f"previous_reason={prev_reason}",
            )
            self._conn.commit()

        active_ids = self._slot_manager.active_ticket_ids()

        for project_name, poller in self._pollers.items():
            slot = self._slot_manager.find_free_slot_for_project(project_name)
            if slot is None:
                continue

            try:
                candidates = poller.poll(active_ticket_ids=active_ids)
            except Exception:
                logger.exception(
                    "Failed to poll Linear for project %s", project_name,
                )
                continue

            if not candidates:
                logger.debug("No candidates for %s", project_name)
                continue

            issue = candidates[0]
            self._dispatch_worker(project_name, slot, issue, poller)
            # Update active IDs so subsequent projects don't pick same ticket
            active_ids.add(issue.identifier)

    def _dispatch_worker(
        self,
        project_name: str,
        slot: SlotState,
        issue,
        poller: LinearPoller,
    ) -> None:
        """Assign a ticket to a slot and spawn a worker subprocess."""
        project_cfg = self._projects[project_name]
        cwd = str(Path(project_cfg.base_dir).expanduser())
        branch = f"{project_cfg.worktree_prefix}{slot.slot_id}"

        # Move issue to In Progress on Linear
        try:
            poller.move_issue(issue.identifier, "In Progress")
        except Exception:
            logger.exception(
                "Failed to move %s to 'In Progress' — skipping dispatch",
                issue.identifier,
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
                "db_path": self._db_path,
                "result_queue": self._result_queue,
                "max_turns": None,
                "max_review_iterations": self._config.agents.max_review_iterations,
                "max_ci_retries": self._config.agents.max_ci_retries,
            },
            daemon=False,  # Not daemon — survives supervisor exit
        )
        proc.start()
        self._workers[(project_name, slot.slot_id)] = proc
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

    def _sleep(self, seconds: int) -> None:
        """Interruptible sleep — checks shutdown flag each second."""
        for _ in range(seconds):
            if self._shutdown_requested:
                break
            time.sleep(1)


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def setup_logging(
    log_dir: str | Path = DEFAULT_LOG_DIR,
    *,
    level: int = logging.INFO,
    console: bool = True,
) -> None:
    """Configure logging to file and optionally console.

    Log file: ``<log_dir>/supervisor.log``
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

    # File handler
    if not has_file:
        fh = logging.FileHandler(log_file)
        fh.setLevel(level)
        fh.setFormatter(fmt)
        root.addHandler(fh)

    # Console handler (for foreground mode)
    if console and not has_console:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(fmt)
        root.addHandler(ch)
