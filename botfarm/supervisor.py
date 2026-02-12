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
import queue as queue_mod
import signal
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

from botfarm.config import BotfarmConfig, ProjectConfig
from botfarm.db import init_db, insert_event, insert_task, update_task
from botfarm.linear import LinearPoller, create_pollers
from botfarm.slots import SlotManager, SlotState
from botfarm.usage import UsagePoller
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
) -> None:
    """Entry point for a worker subprocess.

    This runs ``run_pipeline`` with its own DB connection. Results are
    communicated back to the supervisor via ``result_queue`` so only
    the supervisor writes to the shared state file.
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
            result_queue.put(_WorkerResult(
                project=project_name, slot_id=slot_id, success=False,
                failure_stage=result.failure_stage,
                failure_reason=result.failure_reason,
            ))
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
            self._handle_finished_slots,
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
