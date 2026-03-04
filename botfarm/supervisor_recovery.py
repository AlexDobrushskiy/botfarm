"""Supervisor recovery mixin: startup recovery, worker reconciliation, PR status.

Split from ``supervisor.py`` — mixed into the ``Supervisor`` class via
``class Supervisor(RecoveryMixin, ...)``.  All methods use ``self`` and
have full access to supervisor state.
"""

from __future__ import annotations

import logging
import os
import queue as queue_mod
import signal
import subprocess
from datetime import datetime, timezone

from botfarm.db import (
    get_task,
    get_task_by_ticket,
    insert_event,
    update_task,
    upsert_ticket_history,
)
from botfarm.slots import SlotState, _is_pid_alive
from botfarm.worker import STAGES

logger = logging.getLogger(__name__)


class RecoveryMixin:
    """Recovery, reconciliation, and state-inspection methods for Supervisor."""

    # ------------------------------------------------------------------
    # Crash recovery on startup
    # ------------------------------------------------------------------

    def _recover_on_startup(self) -> None:
        """Recover gracefully from persisted state after a restart.

        For each slot that was busy:
          - If the PID is still alive, re-attach monitoring.
          - If the PID is dead, check if a PR was created/merged to
            determine outcome; otherwise mark as failed.
        For each slot that was failed or completed_pending_cleanup:
          - Run cleanup immediately (move Linear ticket, free slot).
          - If cleanup fails (e.g. network error), log a warning and
            let ``_handle_finished_slots`` retry on the first tick.
        For each slot that was paused_limit:
          - These will be handled by ``_handle_paused_slots`` in the
            normal tick loop — no special startup action needed.
        Finally, reconcile SQLite task records.
        """
        recovered_count = 0
        for slot in self._slot_manager.all_slots():
            if slot.status == "busy" and slot.pid is not None:
                if self._is_ticket_externally_done(slot):
                    recovered_count += 1
                    continue

                if slot.sigterm_sent_at:
                    self._recover_timed_out_slot(slot)
                    recovered_count += 1
                elif _is_pid_alive(slot.pid):
                    self._reattach_worker(slot)
                    recovered_count += 1
                else:
                    self._recover_dead_worker(slot)
                    recovered_count += 1
            elif slot.status in ("paused_limit", "paused_manual"):
                if self._is_ticket_externally_done(slot):
                    recovered_count += 1
                    continue
            elif slot.status == "failed":
                try:
                    self._handle_failed_slot(slot)
                    logger.info(
                        "Recovered failed slot %s/%d during startup",
                        slot.project, slot.slot_id,
                    )
                except Exception:
                    logger.warning(
                        "Failed to clean up slot %s/%d during startup "
                        "— will retry on first tick",
                        slot.project, slot.slot_id,
                        exc_info=True,
                    )
                recovered_count += 1
            elif slot.status == "completed_pending_cleanup":
                try:
                    self._handle_completed_slot(slot)
                    logger.info(
                        "Recovered completed slot %s/%d during startup",
                        slot.project, slot.slot_id,
                    )
                except Exception:
                    logger.warning(
                        "Failed to clean up slot %s/%d during startup "
                        "— will retry on first tick",
                        slot.project, slot.slot_id,
                        exc_info=True,
                    )
                recovered_count += 1

        # Reconcile DB task records with slot state
        self._reconcile_db_tasks()

        # Clear stale update_in_progress pause
        if (
            self._slot_manager.dispatch_paused
            and self._slot_manager.dispatch_pause_reason == "update_in_progress"
        ):
            logger.info("Clearing update_in_progress pause after restart")
            self._slot_manager.set_dispatch_paused(False)
            insert_event(
                self._conn,
                event_type="dispatch_resumed",
                detail="previous_reason=update_in_progress (post-update startup)",
            )

        if recovered_count:
            logger.info(
                "Startup recovery processed %d slot(s)", recovered_count,
            )
            insert_event(
                self._conn,
                event_type="startup_recovery",
                detail=f"slots_processed={recovered_count}",
            )

        self._conn.commit()

    def _apply_start_paused(self) -> None:
        """Pause dispatch on startup if configured and not already paused."""
        if not self._config.start_paused:
            if (
                self._slot_manager.dispatch_paused
                and self._slot_manager.dispatch_pause_reason == "start_paused"
            ):
                logger.info(
                    "start_paused disabled in config — clearing persisted "
                    "startup pause"
                )
                self._slot_manager.set_dispatch_paused(False)
                insert_event(
                    self._conn,
                    event_type="start_paused_cleared",
                    detail="config changed to start_paused=false",
                )
                self._conn.commit()
            return
        if not self._slot_manager.dispatch_paused:
            self._slot_manager.set_dispatch_paused(True, "start_paused")
            self._startup_paused = True
            logger.info(
                "Supervisor started in paused mode "
                "— use the dashboard to start dispatching"
            )
            insert_event(
                self._conn,
                event_type="start_paused",
            )
            self._conn.commit()
        elif self._slot_manager.dispatch_pause_reason == "start_paused":
            self._startup_paused = True

    # ------------------------------------------------------------------
    # Ticket external state check
    # ------------------------------------------------------------------

    def _is_ticket_externally_done(self, slot: SlotState) -> bool:
        """Check if a ticket was moved to Done/Cancelled in Linear externally.

        If the ticket is in a terminal state, marks the slot as completed,
        kills the worker process if still alive, and returns ``True``.
        Returns ``False`` when the ticket is still active or when the
        status cannot be determined (API error).
        """
        if not slot.ticket_id:
            return False

        poller = self._pollers.get(slot.project)
        if not poller:
            return False

        if not poller.is_issue_terminal(slot.ticket_id):
            return False

        logger.info(
            "Ticket %s in slot %s/%d was moved to a terminal state "
            "externally — clearing slot instead of recovering",
            slot.ticket_id, slot.project, slot.slot_id,
        )

        if slot.pid and _is_pid_alive(slot.pid):
            logger.info(
                "Sending SIGTERM to worker PID %d for externally-done ticket %s",
                slot.pid, slot.ticket_id,
            )
            try:
                os.kill(slot.pid, signal.SIGTERM)
            except OSError:
                pass

        self._mark_recovery_completed(slot, reason="ticket_externally_done")
        return True

    # ------------------------------------------------------------------
    # Worker reattach / dead worker recovery
    # ------------------------------------------------------------------

    def _reattach_worker(self, slot: SlotState) -> None:
        """Re-attach monitoring for a still-alive worker process."""
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
        pr_status, _pr_url = self._check_pr_status(slot)

        # --- Stage-aware recovery ---
        if slot.stages_completed:
            next_stage = self._next_stage_after(slot.stages_completed)
            if next_stage == "merge" and pr_status == "merged":
                logger.info(
                    "Dead worker PID %d for %s/%d: PR already merged — marking completed",
                    old_pid, project, slot_id,
                )
                self._mark_recovery_completed(slot, reason="pr_merged_during_merge")
                return
            if next_stage == "merge":
                task_row = self._conn.execute(
                    "SELECT merge_conflict_retries FROM tasks WHERE ticket_id = ?",
                    (ticket_id,),
                ).fetchone()
                if task_row and task_row["merge_conflict_retries"] and task_row["merge_conflict_retries"] > 0:
                    if slot.stage == "resolve_conflict":
                        next_stage = "resolve_conflict"
                        logger.info(
                            "Dead worker PID %d for %s/%d: crashed during "
                            "resolve_conflict (retries=%d) — resuming from "
                            "'resolve_conflict'",
                            old_pid, project, slot_id,
                            task_row["merge_conflict_retries"],
                        )
                    else:
                        next_stage = "review"
                        logger.info(
                            "Dead worker PID %d for %s/%d: mid-merge-conflict-loop "
                            "(retries=%d) — resuming from 'review' to re-run review/CI",
                            old_pid, project, slot_id,
                            task_row["merge_conflict_retries"],
                        )
            if next_stage:
                logger.info(
                    "Dead worker PID %d for %s/%d: resuming from stage '%s' "
                    "(completed: %s)",
                    old_pid, project, slot_id, next_stage,
                    ", ".join(slot.stages_completed),
                )
                self._resume_recovered_worker(slot, resume_from_stage=next_stage)
                return
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

        logger.warning(
            "Dead worker PID %d for %s/%d: no PR found — marking failed",
            old_pid, project, slot_id,
        )
        self._mark_recovery_failed(
            slot, reason="no_pr",
            failure_msg=f"worker PID {old_pid} died (no PR found)",
        )

    # ------------------------------------------------------------------
    # Recovery helpers
    # ------------------------------------------------------------------

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
        """Resume a crash-recovered worker by spawning a new subprocess."""
        self._worker_mgr.resume_recovered_worker(slot, resume_from_stage=resume_from_stage)

    def _recover_timed_out_slot(self, slot: SlotState) -> None:
        """Handle a slot that had a SIGTERM sent before the supervisor crashed."""
        project = slot.project
        slot_id = slot.slot_id
        old_pid = slot.pid

        if _is_pid_alive(old_pid):
            logger.warning(
                "Timed-out worker PID %d for %s/%d is still alive — sending SIGKILL",
                old_pid, project, slot_id,
            )
            try:
                os.kill(old_pid, signal.SIGKILL)
            except OSError:
                pass

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

    # ------------------------------------------------------------------
    # PR status checking
    # ------------------------------------------------------------------

    def _check_pr_status(
        self, slot: SlotState,
    ) -> tuple[str | None, str | None]:
        """Check whether a PR exists for the slot's branch.

        Returns ``(state, pr_url)`` where *state* is ``"merged"``,
        ``"open"``, ``"closed"``, or ``None`` (no PR found), and *pr_url*
        is the resolved PR URL (or ``None``).

        PR URL resolution order:
        1. Tasks DB record (persisted by worker cross-process)
        2. Slot state (``slot.pr_url``)
        3. ``gh pr view`` branch lookup in the project working directory
        """
        from botfarm.supervisor import Supervisor

        pr_ref: str | None = None
        source: str | None = None

        # 1. Try tasks DB
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
                cwd = Supervisor._slot_worktree_cwd(project_cfg, slot.slot_id)
                pr_ref = self._gh_pr_url_for_branch(slot.branch, cwd, env=self._git_env)
                if pr_ref:
                    source = "gh_branch_lookup"

        if not pr_ref:
            logger.debug(
                "No PR URL found for %s/%d (ticket %s)",
                slot.project, slot.slot_id, slot.ticket_id,
            )
            return None, None

        state = self._gh_pr_state(pr_ref, env=self._git_env)
        logger.info(
            "PR status for %s/%d: url=%s, source=%s, state=%s",
            slot.project, slot.slot_id, pr_ref, source, state,
        )
        return state, pr_ref

    @staticmethod
    def _gh_pr_url_for_branch(
        branch: str | None, cwd: str,
        *, env: dict[str, str] | None = None,
    ) -> str | None:
        """Get the PR URL for a branch via ``gh pr view``."""
        if not branch:
            return None
        subprocess_env = {**os.environ, **env} if env else None
        try:
            proc = subprocess.run(
                ["gh", "pr", "view", branch, "--json", "url", "--jq", ".url"],
                capture_output=True, text=True, cwd=cwd, timeout=15,
                env=subprocess_env,
            )
            if proc.returncode == 0 and proc.stdout.strip():
                url = proc.stdout.strip()
                if "/pull/" in url:
                    return url
        except Exception as exc:
            logger.debug("gh pr view failed for branch %s: %s", branch, exc)
        return None

    @staticmethod
    def _gh_pr_state(
        pr_ref: str,
        *, env: dict[str, str] | None = None,
    ) -> str | None:
        """Query a PR's state (``OPEN``, ``MERGED``, ``CLOSED``)."""
        subprocess_env = {**os.environ, **env} if env else None
        try:
            proc = subprocess.run(
                ["gh", "pr", "view", pr_ref, "--json", "state", "--jq", ".state"],
                capture_output=True, text=True, timeout=15,
                env=subprocess_env,
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

    # ------------------------------------------------------------------
    # DB reconciliation
    # ------------------------------------------------------------------

    def _reconcile_db_tasks(self) -> None:
        """Ensure SQLite task records are consistent with slot state."""
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
    # Worker reconciliation (per-tick)
    # ------------------------------------------------------------------

    def _reconcile_workers(self) -> None:
        """Check worker subprocesses and reconcile slot state."""
        from botfarm.supervisor_workers import _WorkerResult

        while True:
            try:
                wr: _WorkerResult = self._result_queue.get(timeout=0.05)
            except queue_mod.Empty:
                break
            slot = self._slot_manager.get_slot(wr.project, wr.slot_id)
            if wr.ticket_id and slot and wr.ticket_id != slot.ticket_id:
                logger.warning(
                    "Ignoring stale result for %s/%d: result ticket=%s, "
                    "current ticket=%s (status=%s)",
                    wr.project, wr.slot_id, wr.ticket_id,
                    slot.ticket_id, slot.status,
                )
                continue
            if wr.paused:
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
                if wr.no_pr_reason:
                    slot = self._slot_manager.get_slot(wr.project, wr.slot_id)
                    if slot:
                        slot.no_pr_reason = wr.no_pr_reason
                self._slot_manager.mark_completed(wr.project, wr.slot_id)
                logger.info(
                    "Worker result: %s/%d completed", wr.project, wr.slot_id,
                )
            elif wr.limit_hit:
                self._usage_poller.force_poll(self._conn)
                self._handle_limit_hit(wr)
            else:
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
                    slot = self._slot_manager.get_slot(wr.project, wr.slot_id)
                    ticket_id = slot.ticket_id if slot else None
                    self._slot_manager.mark_failed(
                        wr.project, wr.slot_id, reason=reason,
                    )
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
            # Stash result_text for terminal comment posting
            if wr.result_text:
                self._pending_result_texts[(wr.project, wr.slot_id)] = wr.result_text
                ticket_id_for_rt = slot.ticket_id if slot else None
                rt_task_id = self._find_task_id(ticket_id_for_rt)
                if rt_task_id is not None:
                    update_task(
                        self._conn, rt_task_id,
                        result_text=wr.result_text,
                    )
                    self._conn.commit()
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

    def _check_timeouts(self) -> None:
        """Kill worker processes that have exceeded their stage timeout."""
        self._worker_mgr.check_timeouts()

    # ------------------------------------------------------------------
    # DB helpers (shared by recovery and operations)
    # ------------------------------------------------------------------

    def _find_task_id(self, ticket_id: str | None) -> int | None:
        """Look up the task_id for a ticket identifier."""
        if not ticket_id:
            return None
        row = get_task_by_ticket(self._conn, ticket_id)
        return row["id"] if row else None

    def _increment_limit_interruptions(self, task_id: int) -> None:
        """Atomically increment the limit_interruptions counter on a task."""
        from botfarm.db import increment_limit_interruptions
        increment_limit_interruptions(self._conn, task_id)

    def _capture_ticket_history(
        self,
        ticket_id: str,
        capture_source: str,
        *,
        client=None,
        pr_url: str | None = None,
        branch_name: str | None = None,
    ) -> None:
        """Fetch full ticket details from Linear and upsert into ticket_history."""
        if client is None:
            poller = next(iter(self._pollers.values()), None)
            if poller is None:
                logger.warning("No poller available for ticket history capture of %s", ticket_id)
                return
            client = poller._client

        try:
            details = client.fetch_issue_details(ticket_id)
            details["capture_source"] = capture_source
            if pr_url:
                details["pr_url"] = pr_url
            if branch_name:
                details["branch_name"] = branch_name
            upsert_ticket_history(self._conn, **details)
            self._conn.commit()
            logger.info("Captured ticket history for %s (source=%s)", ticket_id, capture_source)
        except Exception:
            logger.warning(
                "Failed to capture ticket history for %s — continuing",
                ticket_id,
                exc_info=True,
            )
