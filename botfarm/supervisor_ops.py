"""Supervisor operations mixin: slot cleanup, stop, comments, notifications, limits.

Split from ``supervisor.py`` — mixed into the ``Supervisor`` class via
``class Supervisor(..., OperationsMixin)``.  All methods use ``self`` and
have full access to supervisor state.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import signal
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from botfarm.config import DailySummaryConfig

from botfarm.db import (
    count_completed_tasks_since,
    get_daily_summary_data,
    get_events,
    get_last_refactoring_analysis_date,
    get_last_scheduled_refactoring_ticket,
    get_task,
    insert_event,
    record_refactoring_analysis_created,
    update_task,
)
from botfarm.slots import SlotState, _is_pid_alive
from botfarm.supervisor_workers import (
    StopSlotResult,
    _WorkerResult,
    _collect_descendant_pids,
    _kill_pids,
    _kill_process_tree,
    _truncate_for_comment,
)
from botfarm.worker import is_protected_branch

logger = logging.getLogger(__name__)


class OperationsMixin:
    """Slot cleanup, stop-slot, comments, notifications, and limits for Supervisor."""

    # ------------------------------------------------------------------
    # Handle finished slots
    # ------------------------------------------------------------------

    def _handle_finished_slots(self) -> None:
        """Process completed and failed slots — update Linear, free slots."""
        for slot in self._slot_manager.all_slots():
            if slot.status == "completed_pending_cleanup":
                self._verify_base_dir_clean(slot.project)
                self._handle_completed_slot(slot)
            elif slot.status == "failed":
                self._verify_base_dir_clean(slot.project)
                self._handle_failed_slot(slot)

    def _handle_completed_slot(self, slot: SlotState) -> None:
        """Update Linear for a completed slot and free it."""
        project = slot.project
        linear_cfg = self._config.linear
        poller = self._pollers.get(project)
        if poller and slot.ticket_id:
            if poller.is_issue_terminal(slot.ticket_id):
                logger.info(
                    "Completed slot %s/%d: ticket %s already in terminal "
                    "state — skipping status move",
                    slot.project, slot.slot_id, slot.ticket_id,
                )
            else:
                no_pr = slot.no_pr_reason
                if not no_pr and not slot.pr_url:
                    task_id = self._find_task_id(slot.ticket_id)
                    if task_id is not None:
                        task = get_task(self._conn, task_id)
                        if task and task["comments"]:
                            from botfarm.worker import _detect_no_pr_needed
                            no_pr = _detect_no_pr_needed(task["comments"])
                if no_pr:
                    target_status = linear_cfg.done_status
                elif self._check_pr_status(slot)[0] == "merged":
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

            if linear_cfg.comment_on_completion and not slot.no_pr_reason:
                self._post_completion_comment(poller, slot)

            if slot.no_pr_reason:
                self._post_no_pr_comment(poller, slot)

        # Webhook notification
        self._notify_task_completed(slot)

        # Refactoring analysis notification
        self._maybe_send_refactoring_notification(slot)

        # Capture ticket history at completion
        if slot.ticket_id:
            client = poller._client if poller else None
            self._capture_ticket_history(
                slot.ticket_id, "completion",
                client=client, pr_url=slot.pr_url, branch_name=slot.branch,
            )

        insert_event(
            self._conn,
            event_type="slot_completed",
            detail=f"project={project}, slot={slot.slot_id}, "
            f"ticket={slot.ticket_id}",
        )
        self._conn.commit()

        self._slot_manager.free_slot(project, slot.slot_id)
        self._cleanup_slot_db(project, slot.slot_id)
        self._pending_result_texts.pop((project, slot.slot_id), None)
        logger.info("Freed slot %s/%d after completion", project, slot.slot_id)

    def _handle_failed_slot(self, slot: SlotState) -> None:
        """Update Linear for a failed slot and free it."""
        pr_status, resolved_pr_url = self._check_pr_status(slot)
        # Persist the resolved PR URL so failure comment and history capture
        # can reference it (slot.pr_url may be empty if worker didn't set it).
        if resolved_pr_url and not slot.pr_url:
            slot.pr_url = resolved_pr_url
        if pr_status == "merged":
            logger.info(
                "Failed slot %s/%d has a merged PR — treating as completed",
                slot.project, slot.slot_id,
            )
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
            if poller.is_issue_terminal(slot.ticket_id):
                logger.info(
                    "Failed slot %s/%d: ticket %s already in terminal state "
                    "— skipping label update",
                    slot.project, slot.slot_id, slot.ticket_id,
                )
            else:
                # Don't move the ticket — keep it in its current status.
                # Add "Failed" and "Human" labels so the poller skips it
                # (Human is in exclude_tags) and humans can triage.
                try:
                    poller.add_labels(slot.ticket_id, ["Failed", "Human"])
                except Exception:
                    logger.exception(
                        "Failed to add failure labels to %s",
                        slot.ticket_id,
                    )

                if linear_cfg.comment_on_failure:
                    self._post_failure_comment(poller, slot)

        # Webhook notification
        self._notify_task_failed(slot)

        # Capture ticket history at failure
        if slot.ticket_id:
            client = poller._client if poller else None
            self._capture_ticket_history(
                slot.ticket_id, "failure",
                client=client, pr_url=slot.pr_url, branch_name=slot.branch,
            )

        insert_event(
            self._conn,
            event_type="slot_failed",
            detail=f"project={project}, slot={slot.slot_id}, "
            f"ticket={slot.ticket_id}",
        )
        self._conn.commit()

        self._slot_manager.free_slot(project, slot.slot_id)
        self._cleanup_slot_db(project, slot.slot_id)
        self._pending_result_texts.pop((project, slot.slot_id), None)
        logger.info("Freed slot %s/%d after failure", project, slot.slot_id)

    # ------------------------------------------------------------------
    # Base directory guardrail
    # ------------------------------------------------------------------

    def _verify_base_dir_clean(self, project: str) -> None:
        """Check that the base repo directory is still on ``main``.

        A misbehaving agent may ``cd`` into the base repo (discovered via
        ``git rev-parse --git-common-dir``) and switch its branch.  If
        detected, reset the base dir back to ``main`` and log a warning.
        """
        project_cfg = self._projects.get(project)
        if not project_cfg:
            return
        base = Path(project_cfg.base_dir).expanduser()
        if not base.exists():
            return
        try:
            # Strip GIT_* env vars so subprocesses target the correct repo,
            # not a parent repo whose vars leaked into the environment.
            clean_env = {
                k: v for k, v in os.environ.items()
                if not k.startswith("GIT_")
            }
            result = subprocess.run(
                ["git", "branch", "--show-current"],
                cwd=str(base),
                capture_output=True,
                text=True,
                timeout=10,
                env=clean_env,
            )
            branch = result.stdout.strip()
            if branch and branch != "main":
                logger.warning(
                    "Base dir %s is on branch '%s' (expected 'main') — "
                    "resetting to main",
                    base, branch,
                )
                checkout = subprocess.run(
                    ["git", "checkout", "main"],
                    cwd=str(base),
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env=clean_env,
                )
                if checkout.returncode == 0:
                    insert_event(
                        self._conn,
                        event_type="base_dir_reset",
                        detail=f"project={project}, was_on={branch}",
                    )
                    self._conn.commit()
                else:
                    logger.error(
                        "Failed to reset base dir %s to main: %s",
                        base, checkout.stderr.strip(),
                    )
                    insert_event(
                        self._conn,
                        event_type="base_dir_reset_failed",
                        detail=f"project={project}, was_on={branch}, error={checkout.stderr.strip()}",
                    )
                    self._conn.commit()
        except Exception:
            logger.debug(
                "Could not verify base dir branch for %s", project,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Linear comments
    # ------------------------------------------------------------------

    def _post_failure_comment(self, poller, slot: SlotState) -> None:
        """Post a comment on the Linear issue with failure details."""
        stage = slot.stage or "unknown"
        reason = "unknown"

        task_id = self._find_task_id(slot.ticket_id)
        if task_id is not None:
            task = get_task(self._conn, task_id)
            if task and task["failure_reason"]:
                reason = task["failure_reason"]

        review_iter = slot.stage_iteration if slot.stage in ("review", "fix") else None

        lines = [
            "**Bot outcome: failed** — requires manual attention",
            "",
            f"- **Stage:** `{stage}`",
            f"- **Failure reason:** {reason}",
        ]

        if review_iter is not None:
            lines.append(f"- **Review iteration:** {review_iter}")
        if slot.pr_url:
            lines.append(f"- **PR:** {slot.pr_url}")

        result_text = self._pending_result_texts.get(
            (slot.project, slot.slot_id)
        )
        if result_text:
            lines.append("")
            lines.append(_truncate_for_comment(result_text))

        try:
            poller.add_comment(slot.ticket_id, "\n".join(lines))
        except Exception:
            logger.exception(
                "Failed to post failure comment on %s", slot.ticket_id,
            )

    def _post_completion_comment(self, poller, slot: SlotState) -> None:
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

    def _post_no_pr_comment(self, poller, slot: SlotState) -> None:
        """Post a summary comment when a task completes without a PR."""
        lines = ["**Bot outcome: completed_no_pr**"]

        result_text = self._pending_result_texts.get(
            (slot.project, slot.slot_id)
        )
        if result_text:
            lines.append("")
            lines.append(_truncate_for_comment(result_text))

        try:
            poller.add_comment(slot.ticket_id, "\n".join(lines))
        except Exception:
            logger.warning(
                "Failed to post no-PR comment on %s", slot.ticket_id,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Webhook notification helpers
    # ------------------------------------------------------------------

    def _build_review_summary(self, task_id: int | None) -> str | None:
        """Build a review summary string from DB events when Codex is enabled."""
        if not self._config.agents.codex_reviewer_enabled:
            return None
        if task_id is None:
            return None

        claude_events = get_events(
            self._conn, task_id=task_id, event_type="claude_review_completed",
        )
        claude_str = "UNKNOWN"
        if claude_events:
            detail = claude_events[0]["detail"] or ""
            if "approved" in detail and "changes_requested" not in detail:
                claude_str = "APPROVED"
            else:
                claude_str = "CHANGES_REQUESTED"

        codex_events = get_events(
            self._conn, task_id=task_id, event_type="codex_review_completed",
        )
        codex_failed_events = get_events(
            self._conn, task_id=task_id, event_type="codex_review_failed",
        )
        codex_skipped_events = get_events(
            self._conn, task_id=task_id, event_type="codex_review_skipped",
        )

        if codex_events:
            # If Codex was skipped more recently than it completed
            # (e.g. skip_on_reiteration), the completed verdict is stale.
            codex_completed_at = codex_events[0]["created_at"]
            if codex_skipped_events and codex_skipped_events[0]["created_at"] > codex_completed_at:
                return f"Review: Claude {claude_str}, Codex skipped"

            detail = codex_events[0]["detail"] or ""
            if "approved" in detail and "changes_requested" not in detail:
                codex_str = "APPROVED"
            else:
                codex_str = "CHANGES_REQUESTED"
            return f"Review: Claude {claude_str}, Codex {codex_str}"

        if codex_failed_events:
            # If Codex was skipped more recently than it failed, show skipped.
            codex_failed_at = codex_failed_events[0]["created_at"]
            if codex_skipped_events and codex_skipped_events[0]["created_at"] > codex_failed_at:
                return f"Review: Claude {claude_str}, Codex skipped"
            return f"Review: Claude {claude_str}, Codex FAILED (fell back to Claude-only)"

        return None

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
            review_summary=self._build_review_summary(task_id),
        )

    def _notify_task_failed(self, slot: SlotState) -> None:
        """Send a webhook notification for a failed task."""
        reason = None
        category = None
        task_id = self._find_task_id(slot.ticket_id)
        if task_id is not None:
            task = get_task(self._conn, task_id)
            if task:
                reason = task["failure_reason"]
                category = task["failure_category"]
        self._notifier.notify_task_failed(
            ticket_id=slot.ticket_id or "unknown",
            title=slot.ticket_title or "unknown",
            failure_reason=reason,
            failure_category=category,
            review_summary=self._build_review_summary(task_id),
        )

    def _maybe_send_refactoring_notification(self, slot: SlotState) -> None:
        """Send a refactoring-analysis-specific notification if applicable."""
        ra_label = self._config.refactoring_analysis.linear_label.lower()
        labels = slot.ticket_labels or []
        if not any(lbl.lower() == ra_label for lbl in labels):
            return

        ticket_id = slot.ticket_id or "unknown"
        workspace = self._config.linear.workspace
        if workspace:
            linear_url = f"https://linear.app/{workspace}/issue/{ticket_id}"
        else:
            linear_url = ticket_id

        now = datetime.now(timezone.utc)
        month = now.strftime("%B")
        year = now.year

        result_text = self._pending_result_texts.get(
            (slot.project, slot.slot_id), ""
        )
        if not result_text and slot.ticket_id:
            task_id = self._find_task_id(slot.ticket_id)
            if task_id is not None:
                task = get_task(self._conn, task_id)
                if task and task["result_text"]:
                    result_text = task["result_text"]

        ticket_count_match = re.search(
            r"(\d+)\s+refactoring\s+ticket", result_text, re.IGNORECASE
        )
        num_tickets = int(ticket_count_match.group(1)) if ticket_count_match else 0

        if num_tickets > 0:
            parent_match = re.search(
                r"under\s+([A-Z]+-\d+)", result_text, re.IGNORECASE
            )
            parent_id = parent_match.group(1) if parent_match else ticket_id

            concerns_match = re.search(
                r"[Tt]op concerns?:\s*(.+?)(?:\.\s*[A-Z]|\n|$)", result_text
            )
            brief_list = (
                concerns_match.group(1).strip().rstrip(".")
                if concerns_match
                else "see ticket for details"
            )

            self._notifier.notify_refactoring_action_needed(
                month=month,
                year=year,
                num_tickets=num_tickets,
                parent_ticket_id=parent_id,
                brief_list=brief_list,
                linear_ticket_url=linear_url,
            )
        elif re.search(
            r"no action needed|no refactoring needed|all clear|"
            r"code quality is (?:good|acceptable|satisfactory|sufficient|healthy|fine)",
            result_text,
            re.IGNORECASE,
        ):
            self._notifier.notify_refactoring_all_clear(
                month=month,
                year=year,
                linear_ticket_url=linear_url,
            )
        else:
            logger.warning(
                "Refactoring analysis ticket %s completed but result text "
                "could not be classified — skipping notification",
                ticket_id,
            )

    # ------------------------------------------------------------------
    # Periodic refactoring analysis ticket scheduler
    # ------------------------------------------------------------------

    _REFACTORING_ANALYSIS_TICKET_TEMPLATE = """\
## Periodic Codebase Refactoring Analysis

This is an automatically scheduled investigation ticket. Analyze the current state of \
the codebase and determine whether refactoring is needed.

### Instructions

Follow the Refactoring Analysis Procedure documented in AGENTS.md (or docs/refactoring-analysis.md).

### Expected Outcomes

**If code quality is "good enough":**
1. Post a concise summary comment on this ticket (2-5 bullet points)
2. Send a notification: "Refactoring analysis complete — no action needed"

**If refactoring is needed:**
1. Create a parent ticket "Codebase Refactoring — MONTH YEAR" with findings summary
2. Create child implementation tickets for each refactoring area with dependencies
3. Post a summary comment on this investigation ticket linking to the parent
4. Send a notification: "Refactoring analysis complete — N tickets created under PARENT-ID"

Note: The supervisor handles status transitions automatically — do not move this ticket manually.
"""

    def _maybe_create_refactoring_analysis_ticket(self) -> None:
        """Check if it's time to create a new refactoring analysis ticket.

        Two independent triggers:
        1. Time-based (cadence_days) — creates ticket in Todo state
        2. Ticket-count-based (cadence_tickets) — creates ticket in Backlog
           state and sends a webhook notification

        Either trigger fires independently.  Shared dedup checks (open
        tickets, previous ticket still open) prevent duplicate creation.
        """
        ra_config = self._config.refactoring_analysis
        if not ra_config.enabled:
            return

        now = datetime.now(timezone.utc)

        # --- Check which triggers have fired ---
        time_triggered = False
        ticket_count_triggered = False

        last_created = get_last_refactoring_analysis_date(self._conn)
        last_created_iso = None

        if last_created:
            days_since = (now - last_created).total_seconds() / 86400
            if days_since >= ra_config.cadence_days:
                time_triggered = True
            last_created_iso = last_created.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            # No previous analysis — time trigger fires immediately
            time_triggered = True

        if ra_config.cadence_tickets > 0:
            since = last_created_iso or "1970-01-01T00:00:00.000000Z"
            completed = count_completed_tasks_since(self._conn, since)
            if completed >= ra_config.cadence_tickets:
                ticket_count_triggered = True

        if not time_triggered and not ticket_count_triggered:
            return

        # --- Shared dedup: previous ticket still open? ---
        last_ticket_id = get_last_scheduled_refactoring_ticket(self._conn)
        if last_ticket_id:
            for poller in self._pollers.values():
                if poller.is_issue_terminal(last_ticket_id):
                    break
            else:
                logger.debug(
                    "Skipping refactoring analysis — previous ticket %s still open",
                    last_ticket_id,
                )
                return

        # --- Shared dedup: any open tickets with the RA label? ---
        any_check_succeeded = False
        checked_teams: set[str] = set()
        for poller in self._pollers.values():
            if poller.team_key in checked_teams:
                continue
            checked_teams.add(poller.team_key)
            try:
                open_issues = self._linear_client.fetch_open_issues_with_label(
                    poller.team_key,
                    ra_config.linear_label,
                )
                any_check_succeeded = True
                if open_issues:
                    logger.debug(
                        "Skipping refactoring analysis — found %d open ticket(s) "
                        "with label '%s'",
                        len(open_issues),
                        ra_config.linear_label,
                    )
                    return
            except Exception as exc:
                logger.warning(
                    "Failed to check Linear for open refactoring analysis tickets: %s",
                    exc,
                )

        if not any_check_succeeded:
            logger.warning(
                "Could not verify open refactoring analysis tickets — "
                "skipping creation"
            )
            return

        # Ticket-count trigger → Backlog state; time trigger → Todo state
        use_backlog = ticket_count_triggered and not time_triggered
        self._create_refactoring_analysis_ticket(now, use_backlog=use_backlog)

    def _create_refactoring_analysis_ticket(
        self, now: datetime, *, use_backlog: bool = False,
    ) -> None:
        """Create a refactoring analysis investigation ticket in Linear.

        When ``use_backlog`` is True, the ticket is placed in Backlog state
        (ticket-count trigger) and a notification is sent.  Otherwise it
        goes to Todo state (time-based trigger).
        """
        ra_config = self._config.refactoring_analysis
        month = now.strftime("%B")
        year = now.year
        title = f"Refactoring Analysis — {month} {year}"

        first_project = self._config.projects[0]
        team_key = first_project.linear_team

        try:
            team_id = self._linear_client.get_team_id(team_key)

            label_ids = []
            try:
                ra_label_id = self._linear_client.get_or_create_label(
                    team_key, ra_config.linear_label
                )
                label_ids.append(ra_label_id)
            except Exception as exc:
                logger.warning(
                    "Failed to resolve/create label '%s' — aborting ticket "
                    "creation: %s",
                    ra_config.linear_label,
                    exc,
                )
                return

            try:
                inv_label_id = self._linear_client.get_or_create_label(
                    team_key, "Investigation"
                )
                label_ids.append(inv_label_id)
            except Exception as exc:
                logger.warning(
                    "Failed to resolve/create label 'Investigation': %s",
                    exc,
                )

            project_id: str | None = None
            if first_project.linear_project:
                try:
                    project_id = self._linear_client.get_project_id(
                        first_project.linear_project
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to resolve project '%s': %s",
                        first_project.linear_project,
                        exc,
                    )
                    return
                if project_id is None:
                    logger.warning(
                        "Project '%s' not found in Linear — skipping "
                        "refactoring analysis ticket creation",
                        first_project.linear_project,
                    )
                    return

            target_status = "Backlog" if use_backlog else self._config.linear.todo_status
            state_id: str | None = None
            try:
                states = self._linear_client.get_team_states(team_key)
                state_id = states.get(target_status)
            except Exception as exc:
                logger.warning(
                    "Failed to resolve state '%s': %s",
                    target_status,
                    exc,
                )
                return

            if state_id is None:
                logger.warning(
                    "State '%s' not found in team '%s' — skipping "
                    "refactoring analysis ticket creation",
                    target_status,
                    team_key,
                )
                return

            description = self._REFACTORING_ANALYSIS_TICKET_TEMPLATE

            issue = self._linear_client.create_issue(
                team_id=team_id,
                title=title,
                description=description,
                priority=ra_config.priority,
                label_ids=label_ids or None,
                project_id=project_id,
                state_id=state_id,
            )

            identifier = issue.get("identifier")
            if not identifier:
                logger.error(
                    "Linear returned issue without identifier — "
                    "cannot track ticket for dedup"
                )
                return

            record_refactoring_analysis_created(self._conn, identifier)
            self._conn.commit()

            logger.info(
                "Created refactoring analysis ticket: %s (%s)",
                identifier,
                issue.get("url", ""),
            )

            if use_backlog:
                self._notifier.notify_refactoring_due(
                    ticket_id=identifier,
                    ticket_url=issue.get("url", ""),
                )

        except Exception:
            logger.exception("Failed to create refactoring analysis ticket")

    # ------------------------------------------------------------------
    # Limit interruption handling
    # ------------------------------------------------------------------

    def _check_usage_api_for_limit(self) -> bool:
        """Force-poll the usage API and return True if thresholds are exceeded."""
        self._usage_poller.force_poll(self._conn, bypass_cooldown=True)
        thresholds = self._config.usage_limits
        should_pause, _ = self._usage_poller.state.should_pause_with_thresholds(
            five_hour_threshold=thresholds.pause_five_hour_threshold,
            seven_day_threshold=thresholds.pause_seven_day_threshold,
            enabled=thresholds.enabled,
        )
        return should_pause

    def _handle_limit_hit(self, wr: _WorkerResult) -> None:
        """Handle a worker result that indicates a usage limit hit."""
        slot = self._slot_manager.get_slot(wr.project, wr.slot_id)

        resume_after = self._compute_resume_after()

        self._slot_manager.mark_paused_limit(
            wr.project, wr.slot_id, resume_after=resume_after,
        )

        task_id = self._find_task_id(slot.ticket_id) if slot else None

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

        if self._config.linear.comment_on_limit_pause and slot and slot.ticket_id:
            poller = self._pollers.get(wr.project)
            if poller:
                try:
                    poller.add_comment_as_owner(
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
        """Compute the earliest time to resume based on usage resets + buffer."""
        from botfarm.supervisor_workers import PauseResumeManager
        return PauseResumeManager.compute_resume_after(self._usage_poller)

    # ------------------------------------------------------------------
    # Stop slot
    # ------------------------------------------------------------------

    def request_stop_slot(self, project: str, slot_id: int) -> None:
        """Thread-safe request to stop a slot (called from dashboard)."""
        slot = self._slot_manager.get_slot(project, slot_id)
        ticket_id = slot.ticket_id if slot else None
        with self._stop_slot_lock:
            self._stop_slot_requests.append((project, slot_id, ticket_id))
        self._wake_event.set()

    def _handle_stop_requests(self) -> None:
        """Drain pending stop-slot requests and execute each."""
        with self._stop_slot_lock:
            requests = list(self._stop_slot_requests)
            self._stop_slot_requests.clear()
        for project, slot_id, expected_ticket in requests:
            slot = self._slot_manager.get_slot(project, slot_id)
            if slot is not None and slot.ticket_id != expected_ticket:
                logger.warning(
                    "Skipping stale stop request for %s/%d: expected "
                    "ticket=%s, current ticket=%s",
                    project, slot_id, expected_ticket, slot.ticket_id,
                )
                continue
            try:
                result = self.stop_slot(project, slot_id)
                logger.info(
                    "Stop-slot result for %s/%d: %s",
                    project, slot_id, result.message,
                )
            except Exception:
                logger.exception(
                    "Failed to stop slot %s/%d", project, slot_id,
                )

    def stop_slot(self, project: str, slot_id: int) -> StopSlotResult:
        """Stop a single slot: kill worker, clean up git/PR/Linear, free slot."""
        slot = self._slot_manager.get_slot(project, slot_id)
        if slot is None:
            return StopSlotResult(
                success=False,
                message=f"Slot {project}/{slot_id} not found",
            )

        if slot.status == "free":
            return StopSlotResult(
                success=False,
                message=f"Slot {project}/{slot_id} is not busy",
            )

        if slot.status == "completed_pending_cleanup":
            return StopSlotResult(
                success=False,
                message=f"Slot {project}/{slot_id} has completed work "
                f"pending cleanup — use normal cleanup instead",
            )

        ticket_id = slot.ticket_id
        branch = slot.branch
        pr_was_merged = False
        pr_closed = False

        # --- Kill worker process ---
        self._stop_slot_kill_worker(slot)

        # --- PR cleanup ---
        pr_was_merged, pr_closed = self._stop_slot_pr_cleanup(slot)

        # --- Git cleanup ---
        checkout_ok = self._stop_slot_git_cleanup(project, slot_id, branch)

        # --- Linear + DB cleanup ---
        task_id = self._find_task_id(ticket_id)
        if pr_was_merged:
            self._stop_slot_linear_done(slot)
            if task_id is not None:
                update_task(
                    self._conn, task_id,
                    status="completed",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                )
        else:
            self._stop_slot_linear_cleanup(slot)
            if task_id is not None:
                update_task(
                    self._conn, task_id,
                    status="failed",
                    failure_reason="stopped by user",
                )
        insert_event(
            self._conn,
            task_id=task_id,
            event_type="slot_stopped",
            detail=f"project={project}, slot={slot_id}, "
            f"ticket={ticket_id}, branch={branch}, "
            f"pr_merged={pr_was_merged}, pr_closed={pr_closed}, "
            f"checkout_ok={checkout_ok}",
        )
        self._conn.commit()

        if checkout_ok:
            self._slot_manager.free_slot(project, slot_id)
            self._cleanup_slot_db(project, slot_id)
            self._pending_result_texts.pop((project, slot_id), None)
        else:
            self._slot_manager.mark_failed(
                project, slot_id,
                reason="stopped but checkout to placeholder failed",
            )
            logger.warning(
                "Slot %s/%d not freed after stop — checkout to placeholder "
                "branch failed, worktree may be dirty",
                project, slot_id,
            )

        merged_note = " (PR was already merged)" if pr_was_merged else ""
        closed_note = " (PR closed)" if pr_closed else ""
        checkout_note = " (worktree dirty — slot not freed)" if not checkout_ok else ""
        message = (
            f"Stopped slot {project}/{slot_id} "
            f"(ticket {ticket_id}){merged_note}{closed_note}{checkout_note}"
        )
        logger.info(message)

        return StopSlotResult(
            success=checkout_ok,
            pr_was_merged=pr_was_merged,
            pr_closed=pr_closed,
            ticket_id=ticket_id,
            message=message,
        )

    def _stop_slot_kill_worker(self, slot: SlotState) -> None:
        """Kill the worker process and its child process tree."""
        key = (slot.project, slot.slot_id)
        pid = slot.pid
        grace = self._config.agents.timeout_grace_seconds

        self._pause_events.pop(key, None)

        if pid is None or not _is_pid_alive(pid):
            proc = self._workers.pop(key, None)
            if proc is not None:
                proc.join(timeout=1)
            self._drain_results_for_slot(slot.project, slot.slot_id)
            return

        descendant_pids = _collect_descendant_pids(pid)

        try:
            os.killpg(pid, signal.SIGTERM)
        except (OSError, ProcessLookupError):
            try:
                os.kill(pid, signal.SIGTERM)
            except (OSError, ProcessLookupError):
                pass

        proc = self._workers.get(key)
        if proc is not None:
            proc.join(timeout=grace)
            if proc.is_alive():
                _kill_process_tree(pid)
                proc.join(timeout=2)
            else:
                _kill_pids(descendant_pids, signal.SIGKILL)
            self._workers.pop(key, None)
        else:
            deadline = time.time() + grace
            while _is_pid_alive(pid) and time.time() < deadline:
                time.sleep(0.5)
            if _is_pid_alive(pid):
                _kill_process_tree(pid)
            else:
                _kill_pids(descendant_pids, signal.SIGKILL)

        self._drain_results_for_slot(slot.project, slot.slot_id)

    def _drain_results_for_slot(self, project: str, slot_id: int) -> None:
        """Drain any pending _WorkerResult from the queue for this slot."""
        drained: list[_WorkerResult] = []
        while True:
            try:
                wr: _WorkerResult = self._result_queue.get(timeout=0.05)
            except queue.Empty:
                break
            if wr.project == project and wr.slot_id == slot_id:
                continue
            drained.append(wr)
        for wr in drained:
            self._result_queue.put(wr)

    def _stop_slot_pr_cleanup(self, slot: SlotState) -> tuple[bool, bool]:
        """Close PR if open, detect if merged. Returns (pr_was_merged, pr_closed)."""
        pr_status, resolved_pr_url = self._check_pr_status(slot)
        if pr_status is None:
            return False, False

        if pr_status == "merged":
            return True, False

        if pr_status == "open":
            pr_url = resolved_pr_url
            closed = False
            if pr_url:
                project_cfg = self._projects.get(slot.project)
                if project_cfg:
                    cwd = self._slot_worktree_cwd(project_cfg, slot.slot_id)
                    subprocess_env = self._subprocess_env()
                    try:
                        result = subprocess.run(
                            ["gh", "pr", "close", pr_url],
                            capture_output=True, text=True, cwd=cwd, timeout=30,
                            env=subprocess_env,
                        )
                        if result.returncode == 0:
                            closed = True
                            logger.info("Closed PR %s for stopped slot", pr_url)
                        else:
                            logger.warning(
                                "gh pr close %s exited %d: %s",
                                pr_url, result.returncode, result.stderr,
                            )
                    except Exception:
                        logger.warning(
                            "Failed to close PR %s for stopped slot",
                            pr_url, exc_info=True,
                        )
            return False, closed

        return False, False

    def _stop_slot_git_cleanup(
        self, project: str, slot_id: int, branch: str | None,
    ) -> bool:
        """Reset worktree to placeholder branch and delete feature branch."""
        project_cfg = self._projects.get(project)
        if not project_cfg:
            return False
        cwd = self._slot_worktree_cwd(project_cfg, slot_id)
        placeholder = self._slot_placeholder_branch(slot_id)
        subprocess_env = self._subprocess_env()

        lock_file = Path(cwd) / ".git" / "index.lock"
        if lock_file.exists():
            lock_file.unlink(missing_ok=True)
            logger.info("Removed stale .git/index.lock in %s", cwd)

        try:
            subprocess.run(
                ["git", "reset", "--hard"],
                capture_output=True, text=True, cwd=cwd, timeout=15,
                env=subprocess_env,
            )
        except Exception:
            logger.warning("git reset --hard failed in %s", cwd, exc_info=True)

        try:
            subprocess.run(
                ["git", "clean", "-fd"],
                capture_output=True, text=True, cwd=cwd, timeout=15,
                env=subprocess_env,
            )
        except Exception:
            logger.warning("git clean -fd failed in %s", cwd, exc_info=True)

        checkout_ok = False
        try:
            result = subprocess.run(
                ["git", "checkout", placeholder],
                capture_output=True, text=True, cwd=cwd, timeout=15,
                env=subprocess_env,
            )
            checkout_ok = result.returncode == 0
        except Exception:
            logger.warning(
                "Failed to checkout %s in %s", placeholder, cwd, exc_info=True,
            )

        if branch and not is_protected_branch(branch) and checkout_ok:
            try:
                subprocess.run(
                    ["git", "branch", "-D", branch],
                    capture_output=True, text=True, cwd=cwd, timeout=15,
                    env=subprocess_env,
                )
            except Exception:
                logger.warning(
                    "Failed to delete local branch %s", branch, exc_info=True,
                )

            try:
                subprocess.run(
                    ["git", "push", "origin", "--delete", branch],
                    capture_output=True, text=True, cwd=cwd, timeout=30,
                    env=subprocess_env,
                )
            except Exception:
                logger.debug(
                    "Failed to delete remote branch %s (may not exist)",
                    branch,
                )
        elif branch and not is_protected_branch(branch):
            logger.warning(
                "Skipping branch deletion for %s — checkout to %s failed",
                branch, placeholder,
            )

        return checkout_ok

    def _stop_slot_linear_cleanup(self, slot: SlotState) -> None:
        """Move ticket back to todo status and post a comment."""
        if not slot.ticket_id:
            return

        poller = self._pollers.get(slot.project)
        if not poller:
            return

        target_status = self._config.linear.todo_status
        try:
            poller.move_issue(slot.ticket_id, target_status)
            logger.info(
                "Moved %s to '%s' after stop", slot.ticket_id, target_status,
            )
        except Exception:
            logger.exception(
                "Failed to move %s to '%s' after stop",
                slot.ticket_id, target_status,
            )

        try:
            poller.add_comment(
                slot.ticket_id,
                "**Stopped by user** — work discarded",
            )
        except Exception:
            logger.warning(
                "Failed to post stop comment on %s", slot.ticket_id,
                exc_info=True,
            )

    def _stop_slot_linear_done(self, slot: SlotState) -> None:
        """Move ticket to done status (used when PR was already merged)."""
        if not slot.ticket_id:
            return

        poller = self._pollers.get(slot.project)
        if not poller:
            return

        target_status = self._config.linear.done_status
        try:
            poller.move_issue(slot.ticket_id, target_status)
            logger.info(
                "Moved %s to '%s' (PR already merged, stop treated as completion)",
                slot.ticket_id, target_status,
            )
        except Exception:
            logger.exception(
                "Failed to move %s to '%s' after stop (merged PR)",
                slot.ticket_id, target_status,
            )

    # ------------------------------------------------------------------
    # Add slot
    # ------------------------------------------------------------------

    def request_add_slot(self, project: str) -> None:
        """Thread-safe request to add a slot (called from dashboard)."""
        with self._add_slot_lock:
            self._add_slot_requests.append(project)
        self._wake_event.set()

    def _handle_add_slot_requests(self) -> None:
        """Drain pending add-slot requests and execute each."""
        with self._add_slot_lock:
            requests = list(dict.fromkeys(self._add_slot_requests))
            self._add_slot_requests.clear()
        for project_name in requests:
            try:
                self.add_slot(project_name)
            except Exception:
                logger.exception(
                    "Failed to add slot for project %s", project_name,
                )

    def add_slot(self, project_name: str) -> int:
        """Add a new slot to a running project.

        Creates a git worktree, registers the slot with SlotManager,
        persists to config.yaml, and updates in-memory config.

        Returns the new slot_id.

        Raises ``ValueError`` if the project is not found.
        Raises ``RuntimeError`` if worktree creation fails.
        """
        from botfarm.config import write_structural_config_updates

        project_cfg = self._projects.get(project_name)
        if project_cfg is None:
            raise ValueError(f"Project '{project_name}' not found")

        # Auto-pick slot_id: max(existing) + 1
        existing_ids = [
            s.slot_id
            for s in self._slot_manager.all_slots()
            if s.project == project_name
        ]
        new_slot_id = max(existing_ids) + 1 if existing_ids else 1

        worktree_path = self._slot_worktree_cwd(project_cfg, new_slot_id)

        # Validate worktree path doesn't already exist
        wt_path = Path(worktree_path)
        if wt_path.exists():
            raise RuntimeError(
                f"Worktree path already exists: {worktree_path}"
            )

        # Validate parent dir is writable
        parent = wt_path.parent
        if not parent.exists() or not os.access(str(parent), os.W_OK):
            raise RuntimeError(
                f"Worktree parent directory is not writable: {parent}"
            )

        # Create git worktree
        base_dir = project_cfg.base_dir
        env = self._subprocess_env()
        try:
            subprocess.run(
                ["git", "worktree", "add", "--detach", worktree_path, "origin/main"],
                cwd=base_dir,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"Failed to create git worktree: {exc.stderr.strip()}"
            ) from exc

        # Create placeholder branch in the new worktree
        placeholder = self._slot_placeholder_branch(new_slot_id)
        try:
            subprocess.run(
                ["git", "checkout", "-b", placeholder],
                cwd=worktree_path,
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            logger.warning(
                "Failed to create placeholder branch %s: %s",
                placeholder, exc.stderr.strip(),
            )

        # Persist config, register slot, and log event — with rollback on failure
        try:
            # Persist to config.yaml first (source of truth on restart)
            config_path = self._config.source_path
            if config_path:
                updated_slots = project_cfg.slots + [new_slot_id]
                write_structural_config_updates(
                    Path(config_path),
                    {"projects": [{"name": project_name, "slots": updated_slots}]},
                )

            # Register with SlotManager
            self._slot_manager.register_slot(project_name, new_slot_id)
            self._slot_manager.save()

            # Update in-memory config
            if new_slot_id not in project_cfg.slots:
                project_cfg.slots.append(new_slot_id)

            # Log event
            insert_event(
                self._conn,
                event_type="slot_added",
                detail=json.dumps({
                    "project": project_name,
                    "slot_id": new_slot_id,
                    "worktree": worktree_path,
                }),
            )
            self._conn.commit()
        except Exception:
            # Clean up orphaned worktree
            try:
                subprocess.run(
                    ["git", "worktree", "remove", "--force", worktree_path],
                    cwd=base_dir,
                    env=env,
                    capture_output=True,
                    text=True,
                )
            except Exception:
                logger.warning(
                    "Failed to clean up worktree %s after add_slot failure",
                    worktree_path,
                )
            raise

        logger.info(
            "Added slot %s/%d with worktree at %s",
            project_name, new_slot_id, worktree_path,
        )
        return new_slot_id

    # ------------------------------------------------------------------
    # Resume paused slots (delegation)
    # ------------------------------------------------------------------

    def _handle_paused_slots(self) -> None:
        """Check paused slots and resume them if their resume_after time has passed."""
        self._pause_resume_mgr.handle_paused_slots()

    def _is_ready_to_resume(self, slot: SlotState, now: datetime) -> bool:
        """Check if a paused slot's resume_after time has passed."""
        from botfarm.supervisor_workers import PauseResumeManager
        return PauseResumeManager.is_ready_to_resume(slot, now)

    def _resume_paused_worker(self, slot: SlotState) -> None:
        """Resume a paused slot by re-spawning the worker for the interrupted stage."""
        self._worker_mgr.resume_paused_worker(slot)

    # ------------------------------------------------------------------
    # Manual pause / resume (delegation)
    # ------------------------------------------------------------------

    def _handle_manual_pause_resume(self) -> None:
        """Process manual pause/resume requests from the dashboard."""
        self._pause_resume_mgr.handle_manual_pause_resume()

    def _resume_manual_paused_worker(self, slot: SlotState) -> None:
        """Resume a manually-paused slot by re-spawning its worker."""
        self._worker_mgr.resume_manual_paused_worker(slot)

    def request_pause(self) -> None:
        """Request a manual pause (thread-safe, called from dashboard)."""
        self._manual_pause_event.set()
        self._wake_event.set()

    def request_resume(self) -> None:
        """Request a manual resume (thread-safe, called from dashboard)."""
        self._manual_resume_event.set()
        self._wake_event.set()

    def request_rerun_preflight(self) -> None:
        """Request an immediate preflight re-run (thread-safe, called from dashboard)."""
        self._rerun_preflight_event.set()
        self._wake_event.set()

    def request_update(self) -> None:
        """Request an update-and-restart cycle (thread-safe, called from dashboard)."""
        self._update_event.set()
        self._wake_event.set()

    # ------------------------------------------------------------------
    # Daily work summary
    # ------------------------------------------------------------------

    def _maybe_send_daily_summary(self) -> None:
        """Check if it's time to send a daily work summary digest."""
        ds_config = self._config.daily_summary
        if not ds_config.enabled:
            return

        now = datetime.now(timezone.utc)
        if now.hour != ds_config.send_hour:
            return

        # Check if we already sent a summary in the last 20 hours
        recent_events = get_events(
            self._conn, event_type="daily_summary_sent", limit=1,
        )
        if recent_events:
            last_sent_str = recent_events[0]["created_at"]
            try:
                last_sent = datetime.fromisoformat(
                    last_sent_str.replace("Z", "+00:00")
                )
                hours_since = (now - last_sent).total_seconds() / 3600
                if hours_since < 20:
                    return
            except (ValueError, TypeError):
                pass

        self._send_daily_summary(ds_config)

    def _send_daily_summary(self, ds_config: DailySummaryConfig) -> None:
        """Gather data, invoke Claude for summarization, and send notification."""
        summary_data = get_daily_summary_data(self._conn)
        tasks = summary_data["tasks"]
        total_completed = summary_data["total_completed"]
        total_failed = summary_data["total_failed"]
        total_tasks = total_completed + total_failed

        # Check min_tasks_for_summary threshold
        if ds_config.min_tasks_for_summary > 0 and total_tasks < ds_config.min_tasks_for_summary:
            logger.debug(
                "Daily summary skipped: %d tasks < min_tasks_for_summary=%d",
                total_tasks, ds_config.min_tasks_for_summary,
            )
            insert_event(
                self._conn, event_type="daily_summary_sent",
                detail="skipped: below min_tasks threshold",
            )
            self._conn.commit()
            return

        # Build quiet-day message if no tasks
        if total_tasks == 0:
            self._notifier.notify_daily_summary(
                "Daily Summary — Quiet Day",
                "No tasks completed or failed in the last 24 hours.",
                webhook_url=ds_config.webhook_url,
            )
            insert_event(
                self._conn, event_type="daily_summary_sent",
                detail="quiet day — no tasks",
            )
            self._conn.commit()
            return

        # Build context for Claude summarization
        task_lines = []
        for t in tasks:
            pr_url = t.get("pr_url") or t.get("th_pr_url") or ""
            desc_snippet = (t.get("description") or "")[:200]
            duration = t.get("duration_seconds")
            duration_str = f"{int(duration // 60)}m" if duration else "N/A"
            task_lines.append(
                f"- [{t['status'].upper()}] {t['ticket_id']}: {t['title']}\n"
                f"  PR: {pr_url or 'none'} | Duration: {duration_str}\n"
                f"  Description: {desc_snippet}"
            )
            if t.get("failure_reason"):
                task_lines.append(f"  Failure: {t['failure_reason'][:150]}")

        context = (
            f"Tasks completed in the last 24 hours: {total_completed}\n"
            f"Tasks failed in the last 24 hours: {total_failed}\n"
            f"Total duration: {int(summary_data['total_duration_seconds'] // 60)} minutes\n\n"
            + "\n".join(task_lines)
        )

        prompt = (
            "Write a concise daily summary for a Slack channel. Focus on features "
            "that impact the product and are worth trying. If only a few items, "
            "give slightly more detail per item. If many items, keep each brief. "
            "Use bullet points. Include PR URLs as plain links where available "
            "(not markdown-formatted — bare URLs auto-link in both Slack and Discord). "
            "Keep it under 500 words.\n\n"
            "Group by theme (features, fixes, investigations) rather than "
            "chronological order. Highlight what changed from the user's "
            "perspective, not implementation details. Mention failures only if "
            "noteworthy (e.g., blocked on something). Adapt verbosity: few items "
            "→ more detail per item; many items → terse bullets.\n\n"
            "Return ONLY valid JSON with two keys:\n"
            '{"headline": "one-line summary", "summary": "the full summary text"}\n\n'
            f"Here are today's tasks:\n\n{context}"
        )

        try:
            proc = subprocess.run(
                ["claude", "-p", "--output-format", "json"],
                input=prompt,
                capture_output=True,
                text=True,
                timeout=120,
            )
            if proc.returncode != 0:
                logger.warning(
                    "Daily summary Claude subprocess failed (rc=%d): %s",
                    proc.returncode, proc.stderr[:300],
                )
                self._send_fallback_daily_summary(summary_data, ds_config)
                insert_event(
                    self._conn, event_type="daily_summary_sent",
                    detail=f"fallback: completed={total_completed} failed={total_failed}",
                )
                self._conn.commit()
                return

            # Parse Claude's response — extract the result text from the JSON envelope
            try:
                envelope = json.loads(proc.stdout)
                result_text = envelope.get("result", proc.stdout)
            except (json.JSONDecodeError, TypeError):
                result_text = proc.stdout

            # Try to parse the inner JSON from Claude's response
            try:
                parsed = json.loads(result_text)
                headline = parsed.get("headline", "Daily Work Summary")
                summary = parsed.get("summary", result_text)
            except (json.JSONDecodeError, TypeError):
                headline = "Daily Work Summary"
                summary = result_text

            self._notifier.notify_daily_summary(
                headline,
                summary,
                webhook_url=ds_config.webhook_url,
            )

        except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
            logger.warning("Daily summary Claude invocation failed: %s", exc)
            self._send_fallback_daily_summary(summary_data, ds_config)
            insert_event(
                self._conn, event_type="daily_summary_sent",
                detail=f"fallback: completed={total_completed} failed={total_failed}",
            )
            self._conn.commit()
            return

        insert_event(
            self._conn, event_type="daily_summary_sent",
            detail=f"completed={total_completed} failed={total_failed}",
        )
        self._conn.commit()

    def _send_fallback_daily_summary(self, summary_data: dict, ds_config: DailySummaryConfig) -> None:
        """Send a simple non-Claude summary when Claude invocation fails."""
        total_completed = summary_data["total_completed"]
        total_failed = summary_data["total_failed"]
        tasks = summary_data["tasks"]
        duration_min = int(summary_data["total_duration_seconds"] // 60)

        lines = [f"{total_completed} completed, {total_failed} failed | Total: {duration_min}m"]
        for t in tasks[:10]:
            status_icon = "+" if t["status"] == "completed" else "-"
            pr_url = t.get("pr_url") or t.get("th_pr_url") or ""
            pr_part = f" — {pr_url}" if pr_url else ""
            lines.append(f"{status_icon} {t['ticket_id']}: {t['title']}{pr_part}")
        if len(tasks) > 10:
            lines.append(f"... and {len(tasks) - 10} more")

        self._notifier.notify_daily_summary(
            "Daily Work Summary",
            "\n".join(lines),
            webhook_url=ds_config.webhook_url,
        )
