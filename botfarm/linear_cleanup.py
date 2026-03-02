"""Bulk archive/delete service for completed Linear issues."""

from __future__ import annotations

import logging
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from botfarm.db import (
    get_last_cleanup_batch_time,
    insert_cleanup_batch,
    insert_cleanup_batch_item,
    insert_event,
    update_cleanup_batch,
    upsert_ticket_history,
    get_cleanup_batch,
    get_cleanup_batch_items,
)
from botfarm.linear import LinearAPIError, LinearClient

logger = logging.getLogger(__name__)


@dataclass
class CleanupCandidate:
    """An issue eligible for cleanup (archive or delete)."""

    linear_uuid: str
    identifier: str
    title: str
    updated_at: str
    completed_at: str | None
    labels: list[str]
    has_active_children: bool


@dataclass
class CleanupResult:
    """Summary of a bulk cleanup operation."""

    batch_id: str
    action: str
    total_candidates: int
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass
class UndoResult:
    """Summary of an undo operation."""

    batch_id: str
    total: int
    succeeded: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


class CooldownError(Exception):
    """Raised when a cleanup operation is attempted before the cooldown expires."""


class CleanupService:
    """Bulk archive/delete service for completed Linear issues.

    Fetches candidate issues, backs them up to the local DB, then
    archives or deletes them with rate limiting and error handling.
    """

    COOLDOWN_SECONDS = 300  # 5 minutes between bulk operations
    INTER_OPERATION_DELAY = 0.2  # 200ms between API calls
    RATE_LIMIT_PAUSE_THRESHOLD = 50  # Pause when remaining requests below this
    RATE_LIMIT_PAUSE_SECONDS = 60  # How long to pause when rate-limited

    def __init__(
        self,
        client: LinearClient,
        conn: sqlite3.Connection,
        *,
        team_key: str = "",
        project_name: str = "",
        protected_label: str = "Do Not Archive",
        min_age_days: int = 7,
        default_limit: int = 50,
    ) -> None:
        self._client = client
        self._conn = conn
        self._team_key = team_key
        self._project_name = project_name
        self._protected_label = protected_label
        self._min_age_days = min_age_days
        self._default_limit = default_limit

    def _check_cooldown(self) -> None:
        """Raise CooldownError if the cooldown has not elapsed."""
        last_time = get_last_cleanup_batch_time(self._conn)
        if last_time is None:
            return
        last = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        remaining = self.COOLDOWN_SECONDS - elapsed
        if remaining > 0:
            raise CooldownError(
                f"Cooldown active: {remaining:.0f}s remaining "
                f"(minimum {self.COOLDOWN_SECONDS}s between operations)"
            )

    def _is_old_enough(self, updated_at: str, completed_at: str | None) -> bool:
        """Check if an issue is older than min_age_days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._min_age_days)
        # Use completedAt if available, otherwise updatedAt
        check_date = completed_at or updated_at
        try:
            dt = datetime.fromisoformat(check_date.replace("Z", "+00:00"))
            return dt < cutoff
        except (ValueError, AttributeError):
            return False

    def fetch_candidates(self, limit: int | None = None) -> list[CleanupCandidate]:
        """Fetch completed/canceled issues filtered by age and label.

        Returns issues that pass all filters:
        - Status is completed or canceled
        - Updated/completed more than min_age_days ago
        - Not labeled with the protected label
        - Not a parent with active (non-completed/canceled) children
        """
        fetch_limit = limit or self._default_limit
        nodes = self._client.fetch_completed_issues(
            team_key=self._team_key,
            first=fetch_limit,
            project_name=self._project_name,
        )

        protected_lower = self._protected_label.lower()
        candidates = []

        for node in nodes:
            identifier = node.get("identifier", "")
            linear_uuid = node.get("id", "")
            title = node.get("title", "")
            updated_at = node.get("updatedAt", "")
            completed_at = node.get("completedAt")

            # Label filter
            label_nodes = node.get("labels", {}).get("nodes", [])
            labels = [ln.get("name", "") for ln in label_nodes]
            if any(lbl.lower() == protected_lower for lbl in labels):
                logger.debug("Skipping %s: protected label", identifier)
                continue

            # Age filter
            if not self._is_old_enough(updated_at, completed_at):
                logger.debug("Skipping %s: too recent", identifier)
                continue

            # Parent with active children filter
            child_nodes = node.get("children", {}).get("nodes", [])
            has_active_children = False
            if child_nodes:
                for child in child_nodes:
                    state_type = (child.get("state") or {}).get("type", "")
                    if state_type not in ("completed", "canceled"):
                        has_active_children = True
                        break
            if has_active_children:
                logger.debug(
                    "Skipping %s: parent with active children", identifier
                )
                continue

            candidates.append(
                CleanupCandidate(
                    linear_uuid=linear_uuid,
                    identifier=identifier,
                    title=title,
                    updated_at=updated_at,
                    completed_at=completed_at,
                    labels=labels,
                    has_active_children=False,
                )
            )

        logger.info(
            "Found %d cleanup candidates from %d completed/canceled issues",
            len(candidates),
            len(nodes),
        )
        return candidates

    def _backup_issue(self, identifier: str) -> bool:
        """Back up issue details to ticket_history. Returns True on success."""
        try:
            details = self._client.fetch_issue_details(identifier)
            details["capture_source"] = "cleanup_backup"
            upsert_ticket_history(self._conn, **details)
            self._conn.commit()
            return True
        except (LinearAPIError, sqlite3.Error) as exc:
            logger.warning("Backup failed for %s: %s", identifier, exc)
            return False

    def _check_rate_limit(self) -> None:
        """Pause if approaching rate limit."""
        remaining = self._client.rate_limit_remaining
        if remaining is not None and remaining < self.RATE_LIMIT_PAUSE_THRESHOLD:
            logger.warning(
                "Rate limit low (%d remaining), pausing %ds",
                remaining,
                self.RATE_LIMIT_PAUSE_SECONDS,
            )
            time.sleep(self.RATE_LIMIT_PAUSE_SECONDS)

    def run_cleanup(
        self,
        action: str = "archive",
        limit: int | None = None,
        dry_run: bool = False,
    ) -> CleanupResult:
        """Run a bulk archive or delete operation.

        Args:
            action: "archive" or "delete"
            limit: Max issues to process (defaults to default_limit)
            dry_run: If True, fetch candidates but don't execute

        Returns:
            CleanupResult with operation summary

        Raises:
            CooldownError: If minimum gap between operations hasn't elapsed
            ValueError: If action is not "archive" or "delete"
        """
        if action not in ("archive", "delete"):
            raise ValueError(f"Invalid action: {action!r} (must be 'archive' or 'delete')")

        if not dry_run:
            self._check_cooldown()

        candidates = self.fetch_candidates(limit=limit)
        batch_id = str(uuid.uuid4())
        result = CleanupResult(
            batch_id=batch_id,
            action=action,
            total_candidates=len(candidates),
        )

        if not candidates:
            logger.info("No candidates found for cleanup")
            # No batch record inserted — cooldown only starts after real operations
            return result

        if dry_run:
            logger.info(
                "Dry run: would %s %d issues", action, len(candidates)
            )
            # No batch record inserted — dry runs don't trigger cooldown
            return result

        # Record the batch in the DB
        insert_cleanup_batch(
            self._conn,
            batch_id=batch_id,
            action=action,
            team_key=self._team_key,
            project_name=self._project_name,
            total=len(candidates),
        )
        self._conn.commit()

        for candidate in candidates:
            # Backup step: if backup fails, skip this issue
            if not self._backup_issue(candidate.identifier):
                result.skipped += 1
                insert_cleanup_batch_item(
                    self._conn,
                    batch_id=batch_id,
                    linear_uuid=candidate.linear_uuid,
                    identifier=candidate.identifier,
                    action=action,
                    success=False,
                    error="backup_failed",
                )
                self._conn.commit()
                logger.warning(
                    "Skipping %s: backup failed", candidate.identifier
                )
                continue

            # Check rate limit before each operation
            self._check_rate_limit()

            # Execute archive or delete
            try:
                if action == "archive":
                    success = self._client.archive_issue(candidate.linear_uuid)
                else:
                    success = self._client.delete_issue(candidate.linear_uuid)

                if success:
                    result.succeeded += 1
                    insert_cleanup_batch_item(
                        self._conn,
                        batch_id=batch_id,
                        linear_uuid=candidate.linear_uuid,
                        identifier=candidate.identifier,
                        action=action,
                        success=True,
                    )
                    # Audit logging
                    insert_event(
                        self._conn,
                        event_type=f"cleanup_{action}",
                        detail=f"{candidate.identifier} ({candidate.title}) batch={batch_id}",
                    )
                else:
                    result.failed += 1
                    error_msg = f"{action} returned success=false"
                    result.errors.append(
                        f"{candidate.identifier}: {error_msg}"
                    )
                    insert_cleanup_batch_item(
                        self._conn,
                        batch_id=batch_id,
                        linear_uuid=candidate.linear_uuid,
                        identifier=candidate.identifier,
                        action=action,
                        success=False,
                        error=error_msg,
                    )
            except LinearAPIError as exc:
                result.failed += 1
                error_msg = str(exc)
                result.errors.append(f"{candidate.identifier}: {error_msg}")
                insert_cleanup_batch_item(
                    self._conn,
                    batch_id=batch_id,
                    linear_uuid=candidate.linear_uuid,
                    identifier=candidate.identifier,
                    action=action,
                    success=False,
                    error=error_msg,
                )
                logger.error(
                    "Failed to %s %s: %s",
                    action,
                    candidate.identifier,
                    exc,
                )

            self._conn.commit()

            # Inter-operation delay
            time.sleep(self.INTER_OPERATION_DELAY)

        # Update batch with final counts
        update_cleanup_batch(
            self._conn,
            batch_id,
            succeeded=result.succeeded,
            failed=result.failed,
            skipped=result.skipped,
        )
        self._conn.commit()

        logger.info(
            "Cleanup %s complete: %d succeeded, %d failed, %d skipped (batch %s)",
            action,
            result.succeeded,
            result.failed,
            result.skipped,
            batch_id,
        )
        return result

    def undo_batch(self, batch_id: str) -> UndoResult:
        """Undo an archive batch by unarchiving all successfully archived issues.

        Only works for archive batches — delete operations cannot be undone.

        Raises:
            ValueError: If the batch is not found or was a delete operation
        """
        batch = get_cleanup_batch(self._conn, batch_id)
        if batch is None:
            raise ValueError(f"Batch {batch_id!r} not found")
        if batch["action"] != "archive":
            raise ValueError(
                f"Cannot undo {batch['action']!r} batch — only archive batches can be undone"
            )

        items = get_cleanup_batch_items(
            self._conn, batch_id, success_only=True
        )
        result = UndoResult(batch_id=batch_id, total=len(items))

        for item in items:
            self._check_rate_limit()

            try:
                success = self._client.unarchive_issue(item["linear_uuid"])
                if success:
                    result.succeeded += 1
                    insert_event(
                        self._conn,
                        event_type="cleanup_unarchive",
                        detail=f"{item['identifier']} batch={batch_id}",
                    )
                else:
                    result.failed += 1
                    result.errors.append(
                        f"{item['identifier']}: unarchive returned success=false"
                    )
            except LinearAPIError as exc:
                result.failed += 1
                result.errors.append(f"{item['identifier']}: {exc}")
                logger.error(
                    "Failed to unarchive %s: %s", item["identifier"], exc
                )

            self._conn.commit()
            time.sleep(self.INTER_OPERATION_DELAY)

        logger.info(
            "Undo batch %s: %d unarchived, %d failed",
            batch_id,
            result.succeeded,
            result.failed,
        )
        return result
