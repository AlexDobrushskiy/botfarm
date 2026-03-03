"""Tests for botfarm.slots — slot lifecycle management and state persistence."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

from botfarm.db import init_db, load_all_project_pause_states, load_all_slots, load_dispatch_state, save_dispatch_state
from botfarm.slots import (
    SLOT_STATUSES,
    SlotError,
    SlotManager,
    SlotState,
    _is_pid_alive,
    update_slot_stage,
)


# ---------------------------------------------------------------------------
# SlotState dataclass
# ---------------------------------------------------------------------------


class TestSlotState:
    def test_defaults(self):
        s = SlotState(project="proj", slot_id=1)
        assert s.status == "free"
        assert s.ticket_id is None
        assert s.stages_completed == []
        assert s.interrupted_by_limit is False
        assert s.pid is None

    def test_to_dict(self):
        s = SlotState(project="proj", slot_id=1, status="busy", ticket_id="SMA-10")
        d = s.to_dict()
        assert d["project"] == "proj"
        assert d["slot_id"] == 1
        assert d["status"] == "busy"
        assert d["ticket_id"] == "SMA-10"
        assert isinstance(d["stages_completed"], list)

    def test_from_dict(self):
        data = {
            "project": "proj",
            "slot_id": 2,
            "status": "busy",
            "ticket_id": "SMA-5",
            "stages_completed": ["implement", "review"],
        }
        s = SlotState.from_dict(data)
        assert s.project == "proj"
        assert s.slot_id == 2
        assert s.status == "busy"
        assert s.stages_completed == ["implement", "review"]

    def test_from_dict_ignores_unknown_keys(self):
        data = {
            "project": "proj",
            "slot_id": 1,
            "unknown_field": "should_be_ignored",
        }
        s = SlotState.from_dict(data)
        assert s.project == "proj"
        assert not hasattr(s, "unknown_field")

    def test_roundtrip(self):
        original = SlotState(
            project="proj",
            slot_id=3,
            status="busy",
            ticket_id="SMA-42",
            ticket_title="Fix bug",
            branch="fix/bug",
            stage="implement",
            stage_iteration=2,
            pid=12345,
            interrupted_by_limit=True,
            stages_completed=["implement"],
        )
        restored = SlotState.from_dict(original.to_dict())
        assert original == restored

    def test_from_db_row(self, tmp_path: Path):
        """SlotState.from_db_row reconstructs from a sqlite3.Row."""
        conn = init_db(tmp_path / "test.db")
        conn.execute(
            """INSERT INTO slots (project, slot_id, status, ticket_id,
               stage_iteration, interrupted_by_limit, stages_completed)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("proj", 1, "busy", "SMA-1", 2, 1, '["implement"]'),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM slots WHERE project='proj' AND slot_id=1").fetchone()
        s = SlotState.from_db_row(row)
        assert s.project == "proj"
        assert s.slot_id == 1
        assert s.status == "busy"
        assert s.ticket_id == "SMA-1"
        assert s.stage_iteration == 2
        assert s.interrupted_by_limit is True
        assert s.stages_completed == ["implement"]
        conn.close()


# ---------------------------------------------------------------------------
# SlotManager — registration and queries
# ---------------------------------------------------------------------------


@pytest.fixture()
def mgr(tmp_path: Path) -> SlotManager:
    """Return a SlotManager backed by a temp database."""
    return SlotManager(db_path=tmp_path / "test.db")


class TestRegistration:
    def test_register_slot(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        slot = mgr.get_slot("proj", 1)
        assert slot is not None
        assert slot.status == "free"
        assert slot.project == "proj"
        assert slot.slot_id == 1

    def test_register_does_not_overwrite(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        slot = mgr.get_slot("proj", 1)
        slot.status = "busy"
        mgr.register_slot("proj", 1)  # should NOT reset to free
        assert mgr.get_slot("proj", 1).status == "busy"

    def test_register_multiple_slots(self, mgr: SlotManager):
        mgr.register_slot("alpha", 1)
        mgr.register_slot("alpha", 2)
        mgr.register_slot("beta", 1)
        assert len(mgr.all_slots()) == 3

    def test_get_slot_missing(self, mgr: SlotManager):
        assert mgr.get_slot("nope", 99) is None


class TestQueries:
    def test_free_slots_all(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.register_slot("proj", 2)
        assert len(mgr.free_slots()) == 2

    def test_free_slots_by_project(self, mgr: SlotManager):
        mgr.register_slot("alpha", 1)
        mgr.register_slot("beta", 1)
        assert len(mgr.free_slots(project="alpha")) == 1

    def test_busy_slots(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.register_slot("proj", 2)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        assert len(mgr.busy_slots()) == 1
        assert len(mgr.free_slots()) == 1

    def test_active_ticket_ids(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.register_slot("proj", 2)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        ids = mgr.active_ticket_ids()
        assert ids == {"T-1"}

    def test_active_ticket_ids_includes_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_limit("proj", 1)
        ids = mgr.active_ticket_ids()
        assert "T-1" in ids

    def test_find_free_slot_for_project(self, mgr: SlotManager):
        mgr.register_slot("alpha", 1)
        mgr.register_slot("beta", 1)
        slot = mgr.find_free_slot_for_project("alpha")
        assert slot is not None
        assert slot.project == "alpha"

    def test_find_free_slot_none_available(self, mgr: SlotManager):
        mgr.register_slot("alpha", 1)
        mgr.assign_ticket(
            "alpha", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        assert mgr.find_free_slot_for_project("alpha") is None


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


class TestAssignTicket:
    def test_assign_sets_busy(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        slot = mgr.assign_ticket(
            "proj", 1, ticket_id="SMA-10", ticket_title="Do stuff", branch="feat/stuff"
        )
        assert slot.status == "busy"
        assert slot.ticket_id == "SMA-10"
        assert slot.ticket_title == "Do stuff"
        assert slot.branch == "feat/stuff"
        assert slot.started_at is not None

    def test_assign_to_non_free_raises(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        with pytest.raises(SlotError, match="expected 'free'"):
            mgr.assign_ticket(
                "proj", 1, ticket_id="T-2", ticket_title="T2", branch="b2"
            )

    def test_assign_to_unregistered_raises(self, mgr: SlotManager):
        with pytest.raises(SlotError, match="not registered"):
            mgr.assign_ticket(
                "proj", 99, ticket_id="T-1", ticket_title="T1", branch="b1"
            )

    def test_assign_resets_previous_fields(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.free_slot("proj", 1)
        slot = mgr.assign_ticket(
            "proj", 1, ticket_id="T-2", ticket_title="T2", branch="b2"
        )
        assert slot.ticket_id == "T-2"
        assert slot.stages_completed == []
        assert slot.interrupted_by_limit is False


class TestSetPid:
    def test_set_pid(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 42)
        assert mgr.get_slot("proj", 1).pid == 42


class TestUpdateStage:
    def test_update_stage(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.update_stage("proj", 1, stage="implement", iteration=1, session_id="s1")
        slot = mgr.get_slot("proj", 1)
        assert slot.stage == "implement"
        assert slot.stage_iteration == 1
        assert slot.current_session_id == "s1"

    def test_update_stage_sets_stage_started_at(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.update_stage("proj", 1, stage="implement")
        slot = mgr.get_slot("proj", 1)
        assert slot.stage_started_at is not None
        # Should be a valid ISO timestamp
        from datetime import datetime
        datetime.fromisoformat(slot.stage_started_at.replace("Z", "+00:00"))


class TestCompleteStage:
    def test_complete_stage(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.complete_stage("proj", 1, "implement")
        assert "implement" in mgr.get_slot("proj", 1).stages_completed

    def test_complete_stage_idempotent(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.complete_stage("proj", 1, "implement")
        mgr.complete_stage("proj", 1, "implement")
        assert mgr.get_slot("proj", 1).stages_completed.count("implement") == 1


class TestSetPrUrl:
    def test_set_pr_url(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pr_url("proj", 1, "https://github.com/org/repo/pull/42")
        assert mgr.get_slot("proj", 1).pr_url == "https://github.com/org/repo/pull/42"


class TestMarkPausedLimit:
    def test_mark_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 100)
        mgr.mark_paused_limit("proj", 1)
        slot = mgr.get_slot("proj", 1)
        assert slot.status == "paused_limit"
        assert slot.interrupted_by_limit is True
        assert slot.pid is None

    def test_mark_paused_with_resume_after(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        resume_time = "2026-02-12T22:00:00+00:00"
        mgr.mark_paused_limit("proj", 1, resume_after=resume_time)
        slot = mgr.get_slot("proj", 1)
        assert slot.resume_after == resume_time
        assert slot.status == "paused_limit"


class TestMarkCompleted:
    def test_mark_completed(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_completed("proj", 1)
        slot = mgr.get_slot("proj", 1)
        assert slot.status == "completed_pending_cleanup"
        assert slot.pid is None


class TestMarkFailed:
    def test_mark_failed(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_failed("proj", 1, reason="worker crashed")
        slot = mgr.get_slot("proj", 1)
        assert slot.status == "failed"
        assert slot.pid is None


class TestFreeSlot:
    def test_free_clears_all_state(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 42)
        mgr.update_stage("proj", 1, stage="implement")
        mgr.complete_stage("proj", 1, "implement")
        mgr.set_pr_url("proj", 1, "https://example.com")
        mgr.free_slot("proj", 1)

        slot = mgr.get_slot("proj", 1)
        assert slot.status == "free"
        assert slot.ticket_id is None
        assert slot.ticket_title is None
        assert slot.branch is None
        assert slot.pr_url is None
        assert slot.stage is None
        assert slot.stage_iteration == 0
        assert slot.current_session_id is None
        assert slot.started_at is None
        assert slot.stage_started_at is None
        assert slot.sigterm_sent_at is None
        assert slot.pid is None
        assert slot.interrupted_by_limit is False
        assert slot.resume_after is None
        assert slot.stages_completed == []


class TestPausedLimitSlots:
    def test_paused_limit_slots_returns_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.register_slot("proj", 2)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_limit("proj", 1)
        paused = mgr.paused_limit_slots()
        assert len(paused) == 1
        assert paused[0].slot_id == 1

    def test_paused_limit_slots_empty_when_none_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        assert mgr.paused_limit_slots() == []


class TestResumeSlot:
    def test_resume_paused_to_busy(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_limit("proj", 1)
        mgr.resume_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "busy"

    def test_resume_clears_resume_after(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_limit("proj", 1, resume_after="2026-02-12T22:00:00+00:00")
        mgr.resume_slot("proj", 1)
        assert mgr.get_slot("proj", 1).resume_after is None

    def test_resume_non_paused_raises(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        with pytest.raises(SlotError, match="expected 'paused_limit' or 'paused_manual'"):
            mgr.resume_slot("proj", 1)

    def test_resume_paused_manual_to_busy(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_manual("proj", 1)
        assert mgr.get_slot("proj", 1).status == "paused_manual"
        mgr.resume_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "busy"


class TestMarkPausedManual:
    def test_mark_paused_manual(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 100)
        mgr.mark_paused_manual("proj", 1)
        slot = mgr.get_slot("proj", 1)
        assert slot.status == "paused_manual"
        assert slot.pid is None

    def test_paused_manual_preserves_ticket_info(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.complete_stage("proj", 1, "implement")
        mgr.mark_paused_manual("proj", 1)
        slot = mgr.get_slot("proj", 1)
        assert slot.ticket_id == "T-1"
        assert slot.branch == "b1"
        assert slot.stages_completed == ["implement"]


class TestPausedManualSlots:
    def test_returns_manually_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.register_slot("proj", 2)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_manual("proj", 1)
        paused = mgr.paused_manual_slots()
        assert len(paused) == 1
        assert paused[0].slot_id == 1

    def test_empty_when_none_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        assert mgr.paused_manual_slots() == []

    def test_does_not_include_paused_limit(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_limit("proj", 1)
        assert mgr.paused_manual_slots() == []


class TestActiveTicketIdsManualPause:
    def test_includes_paused_manual(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_manual("proj", 1)
        ids = mgr.active_ticket_ids()
        assert "T-1" in ids


# ---------------------------------------------------------------------------
# Persistence — save / load (database-backed)
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_save_writes_to_db(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.save()
        rows = load_all_slots(mgr._conn)
        assert len(rows) == 1
        assert rows[0]["project"] == "proj"
        assert rows[0]["status"] == "free"

    def test_state_saved_on_assign(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        rows = load_all_slots(mgr._conn)
        assert rows[0]["status"] == "busy"
        assert rows[0]["ticket_id"] == "T-1"

    def test_load_restores_state(self, tmp_path: Path):
        db_path = tmp_path / "test.db"

        # Create and save
        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr1.set_pid("proj", 1, 999)
        mgr1.update_stage("proj", 1, stage="review", iteration=2)

        # Load into a new manager
        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()

        slot = mgr2.get_slot("proj", 1)
        assert slot.status == "busy"
        assert slot.ticket_id == "T-1"
        assert slot.pid == 999
        assert slot.stage == "review"
        assert slot.stage_iteration == 2

    def test_load_empty_db(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.load()  # should not raise
        assert mgr.get_slot("proj", 1).status == "free"

    def test_load_ignores_unregistered_slots(self, tmp_path: Path):
        db_path = tmp_path / "test.db"

        # Save a slot for "unknown" project
        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("unknown", 99)
        mgr1.assign_ticket(
            "unknown", 99, ticket_id="X-1", ticket_title="X", branch="x"
        )

        # Load into manager with only "proj" registered
        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()
        assert mgr2.get_slot("unknown", 99) is None
        assert mgr2.get_slot("proj", 1).status == "free"

    def test_stages_completed_roundtrip(self, tmp_path: Path):
        """stages_completed (JSON list) survives save/load."""
        db_path = tmp_path / "test.db"
        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr1.complete_stage("proj", 1, "implement")
        mgr1.complete_stage("proj", 1, "review")

        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()
        assert mgr2.get_slot("proj", 1).stages_completed == ["implement", "review"]

    def test_interrupted_by_limit_roundtrip(self, tmp_path: Path):
        """interrupted_by_limit (bool↔int) survives save/load."""
        db_path = tmp_path / "test.db"
        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr1.mark_paused_limit("proj", 1)

        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()
        assert mgr2.get_slot("proj", 1).interrupted_by_limit is True

    def test_supervisor_heartbeat_written_to_db(self, mgr: SlotManager):
        """Each save writes a supervisor_heartbeat to dispatch_state."""
        from botfarm.db import load_dispatch_state
        mgr.register_slot("proj", 1)
        mgr.save()
        _paused, _reason, heartbeat = load_dispatch_state(mgr._conn)
        assert heartbeat is not None
        # Should be a valid ISO timestamp
        from datetime import datetime
        datetime.fromisoformat(heartbeat.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# State.json migration
# ---------------------------------------------------------------------------


class TestStateJsonMigration:
    def test_migrate_from_state_json(self, tmp_path: Path):
        """If DB is empty and state.json exists, data migrates automatically."""
        db_path = tmp_path / "test.db"
        state_path = tmp_path / "state.json"

        # Write a state.json with slot data
        data = {
            "slots": [
                {"project": "proj", "slot_id": 1, "status": "busy", "ticket_id": "T-1",
                 "stage": "implement", "stages_completed": ["implement"]},
            ],
            "dispatch_paused": True,
            "dispatch_pause_reason": "limit hit",
        }
        state_path.write_text(json.dumps(data))

        mgr = SlotManager(db_path=db_path, state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()

        slot = mgr.get_slot("proj", 1)
        assert slot.status == "busy"
        assert slot.ticket_id == "T-1"
        assert slot.stage == "implement"
        assert mgr.dispatch_paused is True
        assert mgr.dispatch_pause_reason == "limit hit"

        # Data should now be in the DB
        rows = load_all_slots(mgr._conn)
        assert len(rows) == 1
        assert rows[0]["status"] == "busy"

    def test_migrate_legacy_list_format(self, tmp_path: Path):
        """Old state files (bare list) should still migrate correctly."""
        db_path = tmp_path / "test.db"
        state_path = tmp_path / "state.json"
        data = [{"project": "proj", "slot_id": 1, "status": "busy", "ticket_id": "T-1"}]
        state_path.write_text(json.dumps(data))

        mgr = SlotManager(db_path=db_path, state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()
        assert mgr.get_slot("proj", 1).status == "busy"

    def test_no_migration_when_db_has_data(self, tmp_path: Path):
        """If DB already has slot data, state.json is not read."""
        db_path = tmp_path / "test.db"
        state_path = tmp_path / "state.json"

        # Populate DB first
        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.save()

        # Write a conflicting state.json
        data = {"slots": [{"project": "proj", "slot_id": 1, "status": "busy"}]}
        state_path.write_text(json.dumps(data))

        # Load from DB — should NOT pick up the busy status from state.json
        mgr2 = SlotManager(db_path=db_path, state_path=state_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()
        assert mgr2.get_slot("proj", 1).status == "free"


# ---------------------------------------------------------------------------
# Reconcile (crash recovery)
# ---------------------------------------------------------------------------


class TestReconcile:
    def test_dead_pid_marked_failed(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 999999)  # almost certainly not a real PID

        with patch("botfarm.slots._is_pid_alive", return_value=False):
            messages = mgr.reconcile()

        assert len(messages) == 1
        assert "failed" in messages[0]
        assert mgr.get_slot("proj", 1).status == "failed"

    def test_alive_pid_stays_busy(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, os.getpid())  # current process is alive

        with patch("botfarm.slots._is_pid_alive", return_value=True):
            messages = mgr.reconcile()

        assert len(messages) == 0
        assert mgr.get_slot("proj", 1).status == "busy"

    def test_free_slot_not_reconciled(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        with patch("botfarm.slots._is_pid_alive") as mock_alive:
            messages = mgr.reconcile()
        assert len(messages) == 0
        mock_alive.assert_not_called()

    def test_paused_with_dead_pid(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 999999)
        mgr.mark_paused_limit("proj", 1)  # this clears pid
        # paused_limit with no pid — no reconciliation action needed
        with patch("botfarm.slots._is_pid_alive", return_value=False):
            messages = mgr.reconcile()
        assert len(messages) == 0

    def test_reconcile_saves_state(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 999999)

        with patch("botfarm.slots._is_pid_alive", return_value=False):
            mgr.reconcile()

        # Verify persisted to DB
        rows = load_all_slots(mgr._conn)
        assert rows[0]["status"] == "failed"


# ---------------------------------------------------------------------------
# _is_pid_alive utility
# ---------------------------------------------------------------------------


class TestIsPidAlive:
    def test_current_process_is_alive(self):
        assert _is_pid_alive(os.getpid()) is True

    def test_nonexistent_pid(self):
        # PID 4194304 is beyond typical max PID
        assert _is_pid_alive(4194304) is False


# ---------------------------------------------------------------------------
# SLOT_STATUSES constant
# ---------------------------------------------------------------------------


class TestDispatchPaused:
    def test_defaults(self, mgr: SlotManager):
        assert mgr.dispatch_paused is False
        assert mgr.dispatch_pause_reason is None

    def test_set_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "5-hour utilization 87.0% >= 85% threshold")
        assert mgr.dispatch_paused is True
        assert "5-hour" in mgr.dispatch_pause_reason

    def test_clear_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "some reason")
        mgr.set_dispatch_paused(False)
        assert mgr.dispatch_paused is False
        assert mgr.dispatch_pause_reason is None

    def test_paused_persisted_in_db(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "test reason")
        paused, reason, _heartbeat = load_dispatch_state(mgr._conn)
        assert paused is True
        assert reason == "test reason"

    def test_paused_cleared_in_db(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "reason")
        mgr.set_dispatch_paused(False)
        paused, reason, _heartbeat = load_dispatch_state(mgr._conn)
        assert paused is False

    def test_paused_loaded_from_db(self, tmp_path: Path):
        db_path = tmp_path / "test.db"

        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.set_dispatch_paused(True, "7-day utilization 92.0% >= 90% threshold")

        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()
        assert mgr2.dispatch_paused is True
        assert "7-day" in mgr2.dispatch_pause_reason

    def test_paused_defaults_on_load_if_missing(self, tmp_path: Path):
        db_path = tmp_path / "test.db"

        # Save without setting dispatch paused
        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.save()

        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()
        assert mgr2.dispatch_paused is False
        assert mgr2.dispatch_pause_reason is None


class TestRefreshDispatchState:
    def test_detects_external_clear_of_start_paused(self, mgr: SlotManager):
        """refresh_dispatch_state picks up CLI clearing start_paused."""
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "start_paused")

        # Simulate CLI writing paused=False directly to DB
        save_dispatch_state(mgr._conn, paused=False)
        mgr._conn.commit()

        result = mgr.refresh_dispatch_state()
        assert result is True
        assert mgr.dispatch_paused is False
        assert mgr.dispatch_pause_reason is None

    def test_ignores_non_start_paused_reason(self, mgr: SlotManager):
        """refresh_dispatch_state does NOT clear in-memory state for other reasons."""
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "manual_pause")

        # Even if DB shows unpaused, we don't clear manual_pause
        save_dispatch_state(mgr._conn, paused=False)
        mgr._conn.commit()

        result = mgr.refresh_dispatch_state()
        assert result is False
        assert mgr.dispatch_paused is True
        assert mgr.dispatch_pause_reason == "manual_pause"

    def test_no_change_when_still_paused_in_db(self, mgr: SlotManager):
        """refresh_dispatch_state returns False when DB still shows paused."""
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "start_paused")

        result = mgr.refresh_dispatch_state()
        assert result is False
        assert mgr.dispatch_paused is True

    def test_no_change_when_not_paused(self, mgr: SlotManager):
        """refresh_dispatch_state returns False when not paused at all."""
        mgr.register_slot("proj", 1)

        result = mgr.refresh_dispatch_state()
        assert result is False
        assert mgr.dispatch_paused is False


class TestSlotStatuses:
    def test_expected_values(self):
        assert SLOT_STATUSES == {
            "free",
            "busy",
            "paused_limit",
            "paused_manual",
            "failed",
            "completed_pending_cleanup",
        }


# ---------------------------------------------------------------------------
# Full lifecycle integration
# ---------------------------------------------------------------------------


class TestLifecycleIntegration:
    def test_full_happy_path(self, tmp_path: Path):
        """Simulate a full ticket lifecycle: assign -> stages -> complete -> free."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("my-project", 1)
        mgr.register_slot("my-project", 2)

        # Assign ticket to slot 1
        slot = mgr.assign_ticket(
            "my-project",
            1,
            ticket_id="SMA-42",
            ticket_title="Add feature X",
            branch="adobrushskiy/sma-42-add-feature-x",
        )
        assert slot.status == "busy"

        # Worker starts
        mgr.set_pid("my-project", 1, 12345)

        # Implement stage
        mgr.update_stage("my-project", 1, stage="implement", iteration=1, session_id="s-1")
        mgr.complete_stage("my-project", 1, "implement")

        # Review stage
        mgr.update_stage("my-project", 1, stage="review", iteration=1, session_id="s-2")
        mgr.complete_stage("my-project", 1, "review")

        # PR created
        mgr.set_pr_url("my-project", 1, "https://github.com/org/repo/pull/10")

        # Completed
        mgr.mark_completed("my-project", 1)
        assert mgr.get_slot("my-project", 1).status == "completed_pending_cleanup"

        # Cleanup
        mgr.free_slot("my-project", 1)
        assert mgr.get_slot("my-project", 1).status == "free"
        assert len(mgr.free_slots()) == 2

    def test_pause_resume_cycle(self, mgr: SlotManager):
        """Test pause -> resume -> complete cycle."""
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 123)
        mgr.update_stage("proj", 1, stage="implement")

        # Hit rate limit
        mgr.mark_paused_limit("proj", 1)
        assert mgr.get_slot("proj", 1).status == "paused_limit"
        assert mgr.get_slot("proj", 1).interrupted_by_limit is True

        # Resume
        mgr.resume_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "busy"

        # Complete
        mgr.mark_completed("proj", 1)
        mgr.free_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "free"

    def test_manual_pause_resume_cycle(self, mgr: SlotManager):
        """Test manual pause -> resume -> complete cycle."""
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.set_pid("proj", 1, 123)
        mgr.update_stage("proj", 1, stage="implement")
        mgr.complete_stage("proj", 1, "implement")

        # Manual pause
        mgr.mark_paused_manual("proj", 1)
        assert mgr.get_slot("proj", 1).status == "paused_manual"
        assert mgr.get_slot("proj", 1).pid is None
        # interrupted_by_limit should NOT be set for manual pause
        assert mgr.get_slot("proj", 1).interrupted_by_limit is False

        # Resume
        mgr.resume_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "busy"

        # Complete
        mgr.mark_completed("proj", 1)
        mgr.free_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "free"

    def test_failure_and_recovery(self, mgr: SlotManager):
        """Test fail -> free cycle."""
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_failed("proj", 1, reason="timeout")
        assert mgr.get_slot("proj", 1).status == "failed"

        mgr.free_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "free"

    def test_crash_recovery_roundtrip(self, tmp_path: Path):
        """Simulate crash and recovery via save/load/reconcile."""
        db_path = tmp_path / "test.db"

        # First run: assign and start working
        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.register_slot("proj", 2)
        mgr1.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr1.set_pid("proj", 1, 999999)  # fake PID that won't be alive
        mgr1.update_stage("proj", 1, stage="implement", iteration=1)

        # "Crash" — just drop the manager

        # Second run: load and reconcile
        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.register_slot("proj", 2)
        mgr2.load()

        assert mgr2.get_slot("proj", 1).status == "busy"
        assert mgr2.get_slot("proj", 1).stage == "implement"

        with patch("botfarm.slots._is_pid_alive", return_value=False):
            messages = mgr2.reconcile()
        assert len(messages) == 1
        assert mgr2.get_slot("proj", 1).status == "failed"
        assert mgr2.get_slot("proj", 2).status == "free"


# ---------------------------------------------------------------------------
# update_slot_stage — database-based stage updates from worker subprocesses
# ---------------------------------------------------------------------------


class TestUpdateSlotStage:
    def test_updates_stage_in_db(self, tmp_path: Path):
        """update_slot_stage should write the stage to the database."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        update_slot_stage(db_path, "proj", 1, stage="implement")

        # Read directly from DB to verify
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] == "implement"
        assert row["stage_iteration"] == 1
        assert row["stage_started_at"] is not None
        conn.close()

    def test_updates_iteration_and_session_id(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        update_slot_stage(
            db_path, "proj", 1,
            stage="review", iteration=2, session_id="sess-123",
        )

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] == "review"
        assert row["stage_iteration"] == 2
        assert row["current_session_id"] == "sess-123"
        conn.close()

    def test_no_op_when_db_missing(self, tmp_path: Path):
        """Should not raise when the database doesn't exist."""
        db_path = tmp_path / "nonexistent.db"
        update_slot_stage(db_path, "proj", 1, stage="implement")
        assert not db_path.exists()

    def test_no_op_when_slot_not_found(self, tmp_path: Path):
        """Should not raise when the slot isn't in the database."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.save()

        # Update a non-existent slot — should be a silent no-op
        update_slot_stage(db_path, "other-proj", 99, stage="implement")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] is None
        conn.close()

    def test_updates_correct_slot_among_multiple(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.register_slot("proj", 2)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.assign_ticket(
            "proj", 2, ticket_id="T-2", ticket_title="T2", branch="b2"
        )

        update_slot_stage(db_path, "proj", 2, stage="review")

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row1 = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        row2 = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=2"
        ).fetchone()
        assert row1["stage"] is None  # unchanged
        assert row2["stage"] == "review"
        conn.close()


# ---------------------------------------------------------------------------
# refresh_stages_from_disk — supervisor reads worker stage updates from DB
# ---------------------------------------------------------------------------


class TestRefreshStagesFromDisk:
    def test_refresh_merges_worker_stage_into_memory(self, tmp_path: Path):
        """refresh_stages_from_disk should merge worker-written stage data
        into in-memory state so that subsequent saves don't lose it."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        # Simulate worker subprocess updating stage directly in the DB
        update_slot_stage(db_path, "proj", 1, stage="review", iteration=2)

        # Supervisor's in-memory state still has stage=None
        assert mgr.get_slot("proj", 1).stage is None

        # Refresh from DB (as the supervisor tick would do)
        mgr.refresh_stages_from_disk()

        # In-memory state should now reflect the worker's update
        assert mgr.get_slot("proj", 1).stage == "review"
        assert mgr.get_slot("proj", 1).stage_iteration == 2

        # A subsequent save should preserve the refreshed values
        mgr.save()
        rows = load_all_slots(mgr._conn)
        slot_row = [r for r in rows if r["slot_id"] == 1][0]
        assert slot_row["stage"] == "review"
        assert slot_row["stage_iteration"] == 2

    def test_refresh_only_affects_busy_slots(self, tmp_path: Path):
        """Free slots should not have their stage fields refreshed."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.save()

        # Manually write stage to DB for a free slot
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "UPDATE slots SET stage='bogus' WHERE project='proj' AND slot_id=1"
        )
        conn.commit()
        conn.close()

        # Refresh — free slot should NOT pick up the bogus stage
        mgr.refresh_stages_from_disk()
        assert mgr.get_slot("proj", 1).stage is None


# ---------------------------------------------------------------------------
# Stage race condition: _save() must not overwrite worker-written stage
# ---------------------------------------------------------------------------


class TestStageRaceProtection:
    """Verify that upsert_slot() COALESCE prevents supervisor _save()
    from overwriting worker-written stage fields with NULL."""

    def test_save_does_not_overwrite_worker_stage(self, tmp_path: Path):
        """Core race scenario: worker writes stage, then supervisor _save()
        with stage=None must NOT overwrite to NULL."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        # Worker subprocess writes stage directly to DB
        update_slot_stage(db_path, "proj", 1, stage="implement", session_id="sess-1")

        # Supervisor's in-memory slot still has stage=None (hasn't refreshed yet)
        slot = mgr.get_slot("proj", 1)
        assert slot.stage is None

        # Supervisor triggers _save() (e.g. from set_pid or another mutation)
        mgr.set_pid("proj", 1, 12345)

        # DB should still have the worker's stage value
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] == "implement", (
            "supervisor _save() overwrote worker stage with NULL"
        )
        assert row["current_session_id"] == "sess-1"
        assert row["stage_started_at"] is not None
        conn.close()

    def test_save_preserves_stage_across_multiple_saves(self, tmp_path: Path):
        """Multiple _save() calls must not erode the worker's stage."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        update_slot_stage(db_path, "proj", 1, stage="implement")

        # Multiple supervisor saves (simulating several ticks)
        mgr.save()
        mgr.save()
        mgr.save()

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT stage FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] == "implement"
        conn.close()

    def test_free_slot_clears_stage(self, tmp_path: Path):
        """free_slot() must explicitly clear stage fields despite COALESCE."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        update_slot_stage(
            db_path, "proj", 1, stage="implement", session_id="sess-1"
        )
        mgr.refresh_stages_from_disk()

        # Free the slot — should clear stage fields
        mgr.free_slot("proj", 1)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] is None
        assert row["stage_started_at"] is None
        assert row["current_session_id"] is None
        assert row["stage_iteration"] == 0
        conn.close()

    def test_assign_ticket_clears_previous_stage(self, tmp_path: Path):
        """assign_ticket() must clear stage fields from a previous assignment."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        update_slot_stage(
            db_path, "proj", 1, stage="review", iteration=3, session_id="sess-old"
        )
        mgr.refresh_stages_from_disk()

        # Free then re-assign
        mgr.free_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-2", ticket_title="T2", branch="b2"
        )

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] is None
        assert row["stage_started_at"] is None
        assert row["current_session_id"] is None
        assert row["stage_iteration"] == 0
        assert row["ticket_id"] == "T-2"
        conn.close()

    def test_refresh_then_save_preserves_stage(self, tmp_path: Path):
        """After refresh_stages_from_disk, _save() should write the
        real stage value (not NULL) since the in-memory slot was updated."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )

        update_slot_stage(db_path, "proj", 1, stage="implement")
        mgr.refresh_stages_from_disk()

        # Now in-memory has stage="implement", so save should write it
        mgr.save()

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT stage FROM slots WHERE project='proj' AND slot_id=1"
        ).fetchone()
        assert row["stage"] == "implement"
        conn.close()


# ---------------------------------------------------------------------------
# Per-project pause
# ---------------------------------------------------------------------------


class TestProjectPause:
    def test_defaults_not_paused(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        assert mgr.is_project_paused("proj") is False
        assert mgr.get_project_pause_reason("proj") is None

    def test_unknown_project_not_paused(self, mgr: SlotManager):
        assert mgr.is_project_paused("nonexistent") is False

    def test_pause_project(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_project_paused("proj", True, "manual testing")
        assert mgr.is_project_paused("proj") is True
        assert mgr.get_project_pause_reason("proj") == "manual testing"

    def test_unpause_project(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_project_paused("proj", True, "reason")
        mgr.set_project_paused("proj", False)
        assert mgr.is_project_paused("proj") is False
        assert mgr.get_project_pause_reason("proj") is None

    def test_get_all_project_pauses(self, mgr: SlotManager):
        mgr.register_slot("proj-a", 1)
        mgr.register_slot("proj-b", 1)
        mgr.set_project_paused("proj-a", True, "reason-a")
        mgr.set_project_paused("proj-b", False)
        pauses = mgr.get_all_project_pauses()
        assert pauses["proj-a"] == (True, "reason-a")
        assert pauses["proj-b"] == (False, None)

    def test_persisted_in_db(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_project_paused("proj", True, "db test")
        states = load_all_project_pause_states(mgr._conn)
        assert states["proj"] == (True, "db test")

    def test_loaded_from_db(self, tmp_path: Path):
        db_path = tmp_path / "test.db"

        mgr1 = SlotManager(db_path=db_path)
        mgr1.register_slot("proj", 1)
        mgr1.set_project_paused("proj", True, "persist test")

        mgr2 = SlotManager(db_path=db_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()
        assert mgr2.is_project_paused("proj") is True
        assert mgr2.get_project_pause_reason("proj") == "persist test"

    def test_refresh_project_pauses(self, tmp_path: Path):
        """refresh_project_pauses picks up external DB changes."""
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        assert mgr.is_project_paused("proj") is False

        # Write directly to DB (simulating CLI)
        from botfarm.db import save_project_pause_state
        save_project_pause_state(mgr._conn, project="proj", paused=True, reason="cli")
        mgr._conn.commit()

        # Before refresh — still reads old in-memory state
        assert mgr.is_project_paused("proj") is False

        # After refresh — picks up the DB change
        mgr.refresh_project_pauses()
        assert mgr.is_project_paused("proj") is True
        assert mgr.get_project_pause_reason("proj") == "cli"

    def test_multiple_projects_independent(self, mgr: SlotManager):
        """Pausing one project doesn't affect another."""
        mgr.register_slot("proj-a", 1)
        mgr.register_slot("proj-b", 1)
        mgr.set_project_paused("proj-a", True, "reason")
        assert mgr.is_project_paused("proj-a") is True
        assert mgr.is_project_paused("proj-b") is False
