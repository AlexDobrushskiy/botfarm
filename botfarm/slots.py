"""Slot lifecycle management and runtime state persistence.

Slots are the core resource — the supervisor tracks which slots are free,
busy, paused, or failed. Each slot is bound to a specific project (worktree).

Runtime state is persisted to the SQLite database (``slots`` and
``dispatch_state`` tables). A ``state_path`` can optionally be provided
for one-time migration of legacy state.json data into the database.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from botfarm.db import (
    clear_slot_stage,
    init_db,
    load_all_project_pause_states,
    load_all_slots,
    load_dispatch_state,
    save_dispatch_state,
    save_project_pause_state,
    upsert_slot,
)

logger = logging.getLogger(__name__)

# Valid slot statuses
SLOT_STATUSES = frozenset(
    {"free", "busy", "paused_limit", "paused_manual", "failed", "completed_pending_cleanup"}
)



@dataclass
class SlotState:
    """Runtime state for a single slot."""

    project: str
    slot_id: int
    status: str = "free"
    ticket_id: str | None = None
    ticket_title: str | None = None
    branch: str | None = None
    pr_url: str | None = None
    stage: str | None = None
    stage_iteration: int = 0
    current_session_id: str | None = None
    started_at: str | None = None
    stage_started_at: str | None = None
    sigterm_sent_at: str | None = None
    pid: int | None = None
    interrupted_by_limit: bool = False
    resume_after: str | None = None
    stages_completed: list[str] = field(default_factory=list)
    ticket_labels: list[str] = field(default_factory=list)
    no_pr_reason: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> SlotState:
        # Only pass known fields to avoid errors from extra/removed keys
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)

    @classmethod
    def from_db_row(cls, row: sqlite3.Row) -> SlotState:
        """Reconstruct a SlotState from a database row."""
        stages_raw = row["stages_completed"]
        stages = json.loads(stages_raw) if stages_raw else []
        # ticket_labels may not exist in older schemas
        labels_raw = None
        try:
            labels_raw = row["ticket_labels"]
        except (IndexError, KeyError):
            pass
        labels = json.loads(labels_raw) if labels_raw else []
        return cls(
            project=row["project"],
            slot_id=row["slot_id"],
            status=row["status"],
            ticket_id=row["ticket_id"],
            ticket_title=row["ticket_title"],
            branch=row["branch"],
            pr_url=row["pr_url"],
            stage=row["stage"],
            stage_iteration=row["stage_iteration"],
            current_session_id=row["current_session_id"],
            started_at=row["started_at"],
            stage_started_at=row["stage_started_at"],
            sigterm_sent_at=row["sigterm_sent_at"],
            pid=row["pid"],
            interrupted_by_limit=bool(row["interrupted_by_limit"]),
            resume_after=row["resume_after"],
            stages_completed=stages,
            ticket_labels=labels,
        )


def _is_pid_alive(pid: int) -> bool:
    """Check whether a process with the given PID is still running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


class SlotManager:
    """Manages slot lifecycle, state transitions, and persistence.

    Each slot is identified by (project_name, slot_id). The manager
    persists state to the SQLite database after every mutation so the
    supervisor can recover from crashes.
    """

    def __init__(
        self,
        db_path: str | Path,
        state_path: str | Path | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> None:
        self._db_path = Path(db_path).expanduser()
        if conn is not None:
            self._conn = conn
        else:
            self._conn = init_db(self._db_path)
        # state_path is kept only for one-time migration from state.json → DB
        self._state_path: Path | None = (
            Path(state_path).expanduser() if state_path else None
        )
        self._slots: dict[tuple[str, int], SlotState] = {}
        self._usage: dict | None = None
        self._dispatch_paused: bool = False
        self._dispatch_pause_reason: str | None = None
        # Per-project pause state: {project_name: (paused, reason)}
        self._project_pauses: dict[str, tuple[bool, str | None]] = {}

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def slots(self) -> dict[tuple[str, int], SlotState]:
        return dict(self._slots)

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def register_slot(self, project: str, slot_id: int) -> None:
        """Register a slot from config. Does NOT overwrite if already loaded."""
        key = (project, slot_id)
        if key not in self._slots:
            self._slots[key] = SlotState(project=project, slot_id=slot_id)

    def remove_project_slots(self, project: str) -> int:
        """Remove all slots for a project and persist the change.

        Returns the number of slots removed.
        """
        keys = [k for k in self._slots if k[0] == project]
        for key in keys:
            del self._slots[key]
        if keys:
            self._save()
        return len(keys)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def get_slot(self, project: str, slot_id: int) -> SlotState | None:
        return self._slots.get((project, slot_id))

    def free_slots(self, project: str | None = None) -> list[SlotState]:
        """Return all free slots, optionally filtered by project."""
        return [
            s
            for s in self._slots.values()
            if s.status == "free" and (project is None or s.project == project)
        ]

    def busy_slots(self) -> list[SlotState]:
        return [s for s in self._slots.values() if s.status == "busy"]

    def paused_limit_slots(self) -> list[SlotState]:
        """Return all slots paused due to rate limiting."""
        return [s for s in self._slots.values() if s.status == "paused_limit"]

    def paused_manual_slots(self) -> list[SlotState]:
        """Return all slots paused by a manual user-initiated pause."""
        return [s for s in self._slots.values() if s.status == "paused_manual"]

    def active_ticket_ids(self) -> set[str]:
        """Return the set of Linear issue IDs currently assigned to busy/paused slots."""
        return {
            s.ticket_id
            for s in self._slots.values()
            if s.status in ("busy", "paused_limit", "paused_manual")
            and s.ticket_id is not None
        }

    def all_slots(self) -> list[SlotState]:
        return list(self._slots.values())

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def assign_ticket(
        self,
        project: str,
        slot_id: int,
        *,
        ticket_id: str,
        ticket_title: str,
        branch: str,
        ticket_labels: list[str] | None = None,
    ) -> SlotState:
        """Assign a ticket to a free slot, setting it to busy."""
        slot = self._require_slot(project, slot_id)
        if slot.status != "free":
            raise SlotError(
                f"Slot ({project}, {slot_id}) is '{slot.status}', expected 'free'"
            )
        slot.status = "busy"
        slot.ticket_id = ticket_id
        slot.ticket_title = ticket_title
        slot.branch = branch
        slot.started_at = _now_iso()
        slot.stage = None
        slot.stage_iteration = 0
        slot.current_session_id = None
        slot.stage_started_at = None
        slot.pr_url = None
        slot.interrupted_by_limit = False
        slot.stages_completed = []
        slot.ticket_labels = ticket_labels or []
        self._save()
        clear_slot_stage(self._conn, project, slot_id)
        self._conn.commit()
        return slot

    def set_pid(self, project: str, slot_id: int, pid: int) -> None:
        """Record the worker PID for a busy slot."""
        slot = self._require_slot(project, slot_id)
        slot.pid = pid
        self._save()

    def update_stage(
        self,
        project: str,
        slot_id: int,
        *,
        stage: str,
        iteration: int = 1,
        session_id: str | None = None,
    ) -> None:
        """Update the current stage of a busy slot."""
        slot = self._require_slot(project, slot_id)
        slot.stage = stage
        slot.stage_iteration = iteration
        slot.current_session_id = session_id
        slot.stage_started_at = _now_iso()
        self._save()

    def complete_stage(self, project: str, slot_id: int, stage: str) -> None:
        """Mark a stage as completed on a busy slot."""
        slot = self._require_slot(project, slot_id)
        if stage not in slot.stages_completed:
            slot.stages_completed.append(stage)
        self._save()

    def set_pr_url(self, project: str, slot_id: int, pr_url: str) -> None:
        """Record the PR URL for a slot."""
        slot = self._require_slot(project, slot_id)
        slot.pr_url = pr_url
        self._save()

    def mark_paused_limit(
        self, project: str, slot_id: int, *, resume_after: str | None = None,
    ) -> None:
        """Pause a slot due to rate limiting."""
        slot = self._require_slot(project, slot_id)
        slot.status = "paused_limit"
        slot.interrupted_by_limit = True
        slot.resume_after = resume_after
        slot.pid = None
        slot.sigterm_sent_at = None
        self._save()

    def mark_paused_manual(self, project: str, slot_id: int) -> None:
        """Pause a slot due to a user-initiated manual pause."""
        slot = self._require_slot(project, slot_id)
        slot.status = "paused_manual"
        slot.pid = None
        self._save()

    def mark_completed(self, project: str, slot_id: int) -> None:
        """Mark a slot as completed, pending cleanup."""
        slot = self._require_slot(project, slot_id)
        slot.status = "completed_pending_cleanup"
        slot.pid = None
        self._save()

    def mark_failed(
        self, project: str, slot_id: int, reason: str | None = None
    ) -> None:
        """Mark a slot as failed."""
        slot = self._require_slot(project, slot_id)
        slot.status = "failed"
        slot.pid = None
        self._save()
        logger.error(
            "Slot (%s, %d) failed: %s", project, slot_id, reason or "unknown"
        )

    def free_slot(self, project: str, slot_id: int) -> None:
        """Reset a slot to free, clearing all ticket state."""
        slot = self._require_slot(project, slot_id)
        slot.status = "free"
        slot.ticket_id = None
        slot.ticket_title = None
        slot.branch = None
        slot.pr_url = None
        slot.stage = None
        slot.stage_iteration = 0
        slot.current_session_id = None
        slot.started_at = None
        slot.stage_started_at = None
        slot.sigterm_sent_at = None
        slot.pid = None
        slot.interrupted_by_limit = False
        slot.resume_after = None
        slot.stages_completed = []
        slot.ticket_labels = []
        self._save()
        clear_slot_stage(self._conn, project, slot_id)
        self._conn.commit()

    def resume_slot(self, project: str, slot_id: int) -> None:
        """Resume a paused slot back to busy."""
        slot = self._require_slot(project, slot_id)
        if slot.status not in ("paused_limit", "paused_manual"):
            raise SlotError(
                f"Slot ({project}, {slot_id}) is '{slot.status}', "
                "expected 'paused_limit' or 'paused_manual'"
            )
        slot.status = "busy"
        slot.resume_after = None
        slot.interrupted_by_limit = False
        self._save()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self) -> None:
        """Persist current state to DB (public API for explicit saves)."""
        self._save()

    def refresh_stages_from_disk(self) -> None:
        """Read the database and merge worker-written stage fields.

        Worker subprocesses update stage fields directly in the database
        via ``update_slot_stage``.  The supervisor should call this
        periodically (e.g. at the start of each tick) so that in-memory
        state reflects worker progress before any writes.
        Only busy slots are refreshed.
        """
        rows = load_all_slots(self._conn)
        row_map: dict[tuple[str, int], sqlite3.Row] = {}
        for row in rows:
            row_map[(row["project"], row["slot_id"])] = row

        stage_fields = ("stage", "stage_iteration", "stage_started_at", "current_session_id")
        for key, slot in self._slots.items():
            if slot.status != "busy":
                continue
            db_row = row_map.get(key)
            if not db_row:
                continue
            for fld in stage_fields:
                db_val = db_row[fld]
                if db_val is not None:
                    setattr(slot, fld, db_val)

    def _save(self) -> None:
        """Persist slot and dispatch state to the database."""
        for slot in self._slots.values():
            upsert_slot(self._conn, slot.to_dict())
        save_dispatch_state(
            self._conn,
            paused=self._dispatch_paused,
            reason=self._dispatch_pause_reason,
            supervisor_heartbeat=_now_iso(),
        )
        self._conn.commit()

    def load(self) -> None:
        """Load state from DB if it exists. Falls back to state.json migration."""
        # Always load per-project pause state (may exist even without slots)
        self._project_pauses = load_all_project_pause_states(self._conn)

        # Try loading from database first
        rows = load_all_slots(self._conn)
        if rows:
            self._load_from_db(rows)
            return

        # DB is empty — check for state.json to auto-migrate
        if self._state_path and self._state_path.exists():
            logger.info(
                "Migrating slot state from %s to database", self._state_path,
            )
            self._migrate_from_state_json()
            return

        logger.info("No existing state found — starting fresh")

    def _load_from_db(self, rows: list[sqlite3.Row]) -> None:
        """Populate in-memory slots from database rows."""
        for row in rows:
            key = (row["project"], row["slot_id"])
            if key in self._slots:
                self._slots[key] = SlotState.from_db_row(row)

        paused, reason, _heartbeat = load_dispatch_state(self._conn)
        self._dispatch_paused = paused
        self._dispatch_pause_reason = reason

        self._project_pauses = load_all_project_pause_states(self._conn)

    def _migrate_from_state_json(self) -> None:
        """One-time migration: read state.json, populate DB, then proceed."""
        try:
            raw = self._state_path.read_text()
            data = json.loads(raw)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to read state file %s: %s", self._state_path, exc)
            return

        # Support both old format (bare list) and new format (dict with "slots" key)
        if isinstance(data, dict):
            slot_list = data.get("slots", [])
            self._usage = data.get("usage")
            self._dispatch_paused = data.get("dispatch_paused", False)
            self._dispatch_pause_reason = data.get("dispatch_pause_reason")
        elif isinstance(data, list):
            slot_list = data
        else:
            logger.warning("Invalid state file format — expected a JSON object or array")
            return

        for entry in slot_list:
            if not isinstance(entry, dict):
                continue
            project = entry.get("project")
            slot_id = entry.get("slot_id")
            if project is None or slot_id is None:
                continue
            key = (project, slot_id)
            if key in self._slots:
                self._slots[key] = SlotState.from_dict(entry)

        # Persist migrated state to DB
        self._save()
        logger.info("Migration from state.json to database complete")

    def reconcile(self) -> list[str]:
        """Check persisted state against reality after a restart.

        For busy/paused slots whose PID is no longer alive, mark them failed.
        Slots with ``sigterm_sent_at`` set are being handled by the timeout
        system and are skipped here to preserve the descriptive timeout reason.
        Returns a list of human-readable messages describing what changed.
        """
        messages: list[str] = []
        for slot in self._slots.values():
            if slot.status in ("busy", "paused_limit") and slot.pid is not None:
                if slot.sigterm_sent_at:
                    continue  # timeout system owns this slot
                if not _is_pid_alive(slot.pid):
                    old_pid = slot.pid
                    old_status = slot.status
                    slot.status = "failed"
                    slot.pid = None
                    msg = (
                        f"Slot ({slot.project}, {slot.slot_id}): "
                        f"PID {old_pid} gone, "
                        f"{old_status} -> failed"
                    )
                    messages.append(msg)
                    logger.warning(msg)
        if messages:
            self._save()
        return messages

    # ------------------------------------------------------------------
    # Usage state
    # ------------------------------------------------------------------

    def set_usage(self, usage: dict) -> None:
        """Store current usage data."""
        self._usage = usage
        self._save()

    # ------------------------------------------------------------------
    # Dispatch pause state
    # ------------------------------------------------------------------

    @property
    def dispatch_paused(self) -> bool:
        return self._dispatch_paused

    @property
    def dispatch_pause_reason(self) -> str | None:
        return self._dispatch_pause_reason

    def set_dispatch_paused(self, paused: bool, reason: str | None = None) -> None:
        """Set or clear the dispatch pause state."""
        self._dispatch_paused = paused
        self._dispatch_pause_reason = reason if paused else None
        self._save()

    # ------------------------------------------------------------------
    # Per-project pause state
    # ------------------------------------------------------------------

    def is_project_paused(self, project: str) -> bool:
        """Return True if the given project is paused."""
        entry = self._project_pauses.get(project)
        return entry[0] if entry else False

    def get_project_pause_reason(self, project: str) -> str | None:
        """Return the pause reason for a project, or None."""
        entry = self._project_pauses.get(project)
        return entry[1] if entry and entry[0] else None

    def set_project_paused(
        self, project: str, paused: bool, reason: str | None = None,
    ) -> None:
        """Pause or unpause a specific project."""
        self._project_pauses[project] = (paused, reason if paused else None)
        save_project_pause_state(
            self._conn, project=project, paused=paused, reason=reason,
        )
        self._conn.commit()

    def get_all_project_pauses(self) -> dict[str, tuple[bool, str | None]]:
        """Return a copy of the per-project pause state dict."""
        return dict(self._project_pauses)

    def refresh_project_pauses(self) -> None:
        """Reload per-project pause state from the database.

        Called by the supervisor at the start of each tick so that
        changes made by CLI or dashboard are picked up promptly.
        """
        self._project_pauses = load_all_project_pause_states(self._conn)

    def refresh_dispatch_state(self) -> bool:
        """Reload global dispatch pause state from the database.

        Called by the supervisor at the start of each tick so that
        changes made by the CLI (``botfarm resume``) are picked up.

        Returns ``True`` if an external actor cleared ``start_paused``,
        indicating the supervisor should also clear its
        ``_startup_paused`` flag.
        """
        paused, reason, _heartbeat = load_dispatch_state(self._conn)
        if (
            self._dispatch_paused
            and self._dispatch_pause_reason == "start_paused"
            and not paused
        ):
            self._dispatch_paused = False
            self._dispatch_pause_reason = None
            return True
        return False

    # ------------------------------------------------------------------
    # Dispatch helpers
    # ------------------------------------------------------------------

    def find_free_slot_for_project(
        self,
        project: str,
        preferred_slot_id: int | None = None,
    ) -> SlotState | None:
        """Return a free slot for a given project, or None.

        When *preferred_slot_id* is provided and that slot is free,
        it is returned (slot affinity for retried tickets).  Otherwise
        falls back to the first available free slot.
        """
        free = self.free_slots(project=project)
        if not free:
            return None
        if preferred_slot_id is not None:
            for slot in free:
                if slot.slot_id == preferred_slot_id:
                    return slot
        return free[0]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _require_slot(self, project: str, slot_id: int) -> SlotState:
        key = (project, slot_id)
        slot = self._slots.get(key)
        if slot is None:
            raise SlotError(f"Slot ({project}, {slot_id}) not registered")
        return slot


class SlotError(Exception):
    """Raised for invalid slot operations."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def update_slot_stage(
    db_path: str | Path,
    project: str,
    slot_id: int,
    *,
    stage: str,
    iteration: int = 1,
    session_id: str | None = None,
) -> None:
    """Update the stage fields of a specific slot directly in the database.

    This is designed for use by worker subprocesses that don't have access
    to the full SlotManager. It opens its own DB connection for safe
    concurrent access via WAL mode.
    """
    path = Path(db_path).expanduser()
    if not path.exists():
        logger.warning("Database %s does not exist — skipping stage update", path)
        return

    now = _now_iso()
    try:
        conn = sqlite3.connect(str(path))
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            if session_id is not None:
                conn.execute(
                    """UPDATE slots SET stage=?, stage_iteration=?, stage_started_at=?,
                       current_session_id=?, updated_at=?
                       WHERE project=? AND slot_id=?""",
                    (stage, iteration, now, session_id, now, project, slot_id),
                )
            else:
                conn.execute(
                    """UPDATE slots SET stage=?, stage_iteration=?, stage_started_at=?,
                       updated_at=?
                       WHERE project=? AND slot_id=?""",
                    (stage, iteration, now, now, project, slot_id),
                )
            conn.commit()
        finally:
            conn.close()
    except sqlite3.Error as exc:
        logger.warning("Failed to update slot stage in database: %s", exc)
