"""Slot lifecycle management and runtime state persistence.

Slots are the core resource — the supervisor tracks which slots are free,
busy, paused, or failed. Each slot is bound to a specific project (worktree).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

# Valid slot statuses
SLOT_STATUSES = frozenset(
    {"free", "busy", "paused_limit", "failed", "completed_pending_cleanup"}
)

DEFAULT_STATE_PATH = Path.home() / ".botfarm" / "state.json"


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

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> SlotState:
        # Only pass known fields to avoid errors from extra/removed keys
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)


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
    persists state to a JSON file after every mutation so the supervisor
    can recover from crashes.
    """

    def __init__(self, state_path: str | Path = DEFAULT_STATE_PATH) -> None:
        self._state_path = Path(state_path).expanduser()
        self._slots: dict[tuple[str, int], SlotState] = {}
        self._usage: dict | None = None
        self._dispatch_paused: bool = False
        self._dispatch_pause_reason: str | None = None

    @property
    def state_path(self) -> Path:
        return self._state_path

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

    def active_ticket_ids(self) -> set[str]:
        """Return the set of Linear issue IDs currently assigned to busy slots."""
        return {
            s.ticket_id
            for s in self._slots.values()
            if s.status in ("busy", "paused_limit") and s.ticket_id is not None
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
        slot.pr_url = None
        slot.interrupted_by_limit = False
        slot.stages_completed = []
        self._save()
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
        self._save()

    def resume_slot(self, project: str, slot_id: int) -> None:
        """Resume a paused slot back to busy."""
        slot = self._require_slot(project, slot_id)
        if slot.status != "paused_limit":
            raise SlotError(
                f"Slot ({project}, {slot_id}) is '{slot.status}', "
                "expected 'paused_limit'"
            )
        slot.status = "busy"
        slot.resume_after = None
        self._save()

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self) -> None:
        """Persist current state to disk (public API for explicit saves)."""
        self._save()

    def _save(self) -> None:
        """Write state.json atomically (write-then-rename)."""
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict = {
            "slots": [s.to_dict() for s in self._slots.values()],
        }
        if self._usage is not None:
            payload["usage"] = self._usage
        payload["dispatch_paused"] = self._dispatch_paused
        if self._dispatch_pause_reason is not None:
            payload["dispatch_pause_reason"] = self._dispatch_pause_reason
        tmp_path = self._state_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2) + "\n")
        tmp_path.replace(self._state_path)

    def load(self) -> None:
        """Load state from disk if it exists. Merges with registered slots."""
        if not self._state_path.exists():
            logger.info("No state file found at %s — starting fresh", self._state_path)
            return

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
                # Overwrite the registered placeholder with persisted state
                self._slots[key] = SlotState.from_dict(entry)

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
        """Store current usage data (written to state.json on next save)."""
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
    # Dispatch helpers
    # ------------------------------------------------------------------

    def find_free_slot_for_project(self, project: str) -> SlotState | None:
        """Return the first free slot for a given project, or None."""
        free = self.free_slots(project=project)
        return free[0] if free else None

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
