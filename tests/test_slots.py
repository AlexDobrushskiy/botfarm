"""Tests for botfarm.slots — slot lifecycle management and state persistence."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from botfarm.slots import (
    SLOT_STATUSES,
    SlotError,
    SlotManager,
    SlotState,
    _is_pid_alive,
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


# ---------------------------------------------------------------------------
# SlotManager — registration and queries
# ---------------------------------------------------------------------------


@pytest.fixture()
def mgr(tmp_path: Path) -> SlotManager:
    """Return a SlotManager with a temp state file."""
    return SlotManager(state_path=tmp_path / "state.json")


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
        assert slot.pid is None
        assert slot.interrupted_by_limit is False
        assert slot.stages_completed == []


class TestResumeSlot:
    def test_resume_paused_to_busy(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr.mark_paused_limit("proj", 1)
        mgr.resume_slot("proj", 1)
        assert mgr.get_slot("proj", 1).status == "busy"

    def test_resume_non_paused_raises(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        with pytest.raises(SlotError, match="expected 'paused_limit'"):
            mgr.resume_slot("proj", 1)


# ---------------------------------------------------------------------------
# Persistence — save / load
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_save_creates_file(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.save()
        assert mgr.state_path.exists()
        data = json.loads(mgr.state_path.read_text())
        assert isinstance(data, dict)
        assert "slots" in data
        assert len(data["slots"]) == 1
        assert data["slots"][0]["project"] == "proj"

    def test_state_saved_on_assign(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        data = json.loads(mgr.state_path.read_text())
        assert data["slots"][0]["status"] == "busy"
        assert data["slots"][0]["ticket_id"] == "T-1"

    def test_load_restores_state(self, tmp_path: Path):
        state_path = tmp_path / "state.json"

        # Create and save
        mgr1 = SlotManager(state_path=state_path)
        mgr1.register_slot("proj", 1)
        mgr1.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr1.set_pid("proj", 1, 999)
        mgr1.update_stage("proj", 1, stage="review", iteration=2)

        # Load into a new manager
        mgr2 = SlotManager(state_path=state_path)
        mgr2.register_slot("proj", 1)
        mgr2.load()

        slot = mgr2.get_slot("proj", 1)
        assert slot.status == "busy"
        assert slot.ticket_id == "T-1"
        assert slot.pid == 999
        assert slot.stage == "review"
        assert slot.stage_iteration == 2

    def test_load_missing_file(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.load()  # should not raise
        assert mgr.get_slot("proj", 1).status == "free"

    def test_load_corrupt_json(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state_path.write_text("not valid json!!!")
        mgr = SlotManager(state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()  # should not raise
        assert mgr.get_slot("proj", 1).status == "free"

    def test_load_wrong_format(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        state_path.write_text(json.dumps("just a string"))
        mgr = SlotManager(state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()
        assert mgr.get_slot("proj", 1).status == "free"

    def test_load_ignores_unregistered_slots(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        data = {"slots": [{"project": "unknown", "slot_id": 99, "status": "busy"}]}
        state_path.write_text(json.dumps(data))

        mgr = SlotManager(state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()
        assert mgr.get_slot("unknown", 99) is None
        assert mgr.get_slot("proj", 1).status == "free"

    def test_load_legacy_list_format(self, tmp_path: Path):
        """Old state files (bare list) should still load correctly."""
        state_path = tmp_path / "state.json"
        data = [{"project": "proj", "slot_id": 1, "status": "busy", "ticket_id": "T-1"}]
        state_path.write_text(json.dumps(data))

        mgr = SlotManager(state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()
        assert mgr.get_slot("proj", 1).status == "busy"

    def test_atomic_write(self, mgr: SlotManager):
        """Verify the .tmp intermediate file is cleaned up."""
        mgr.register_slot("proj", 1)
        mgr.save()
        tmp_file = mgr.state_path.with_suffix(".tmp")
        assert not tmp_file.exists()
        assert mgr.state_path.exists()


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

        data = json.loads(mgr.state_path.read_text())
        assert data["slots"][0]["status"] == "failed"


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

    def test_paused_persisted_in_state_file(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "test reason")
        data = json.loads(mgr.state_path.read_text())
        assert data["dispatch_paused"] is True
        assert data["dispatch_pause_reason"] == "test reason"

    def test_paused_not_persisted_cleared(self, mgr: SlotManager):
        mgr.register_slot("proj", 1)
        mgr.set_dispatch_paused(True, "reason")
        mgr.set_dispatch_paused(False)
        data = json.loads(mgr.state_path.read_text())
        assert data["dispatch_paused"] is False
        assert "dispatch_pause_reason" not in data

    def test_paused_loaded_from_state(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        data = {
            "slots": [{"project": "proj", "slot_id": 1, "status": "free"}],
            "dispatch_paused": True,
            "dispatch_pause_reason": "7-day utilization 92.0% >= 90% threshold",
        }
        state_path.write_text(json.dumps(data))

        mgr = SlotManager(state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()
        assert mgr.dispatch_paused is True
        assert "7-day" in mgr.dispatch_pause_reason

    def test_paused_defaults_on_load_if_missing(self, tmp_path: Path):
        state_path = tmp_path / "state.json"
        data = {"slots": [{"project": "proj", "slot_id": 1, "status": "free"}]}
        state_path.write_text(json.dumps(data))

        mgr = SlotManager(state_path=state_path)
        mgr.register_slot("proj", 1)
        mgr.load()
        assert mgr.dispatch_paused is False
        assert mgr.dispatch_pause_reason is None


class TestSlotStatuses:
    def test_expected_values(self):
        assert SLOT_STATUSES == {
            "free",
            "busy",
            "paused_limit",
            "failed",
            "completed_pending_cleanup",
        }


# ---------------------------------------------------------------------------
# Full lifecycle integration
# ---------------------------------------------------------------------------


class TestLifecycleIntegration:
    def test_full_happy_path(self, tmp_path: Path):
        """Simulate a full ticket lifecycle: assign -> stages -> complete -> free."""
        state_path = tmp_path / "state.json"
        mgr = SlotManager(state_path=state_path)
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
        state_path = tmp_path / "state.json"

        # First run: assign and start working
        mgr1 = SlotManager(state_path=state_path)
        mgr1.register_slot("proj", 1)
        mgr1.register_slot("proj", 2)
        mgr1.assign_ticket(
            "proj", 1, ticket_id="T-1", ticket_title="T1", branch="b1"
        )
        mgr1.set_pid("proj", 1, 999999)  # fake PID that won't be alive
        mgr1.update_stage("proj", 1, stage="implement", iteration=1)

        # "Crash" — just drop the manager

        # Second run: load and reconcile
        mgr2 = SlotManager(state_path=state_path)
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
