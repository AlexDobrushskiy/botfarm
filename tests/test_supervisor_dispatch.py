"""Tests for supervisor dispatch, polling, reconciliation, and core lifecycle."""

from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import (
    BotfarmConfig,
    CoderIdentity,
    DatabaseConfig,
    IdentitiesConfig,
    LinearConfig,
    ProjectConfig,
)
from botfarm.db import get_events, get_task, init_db, insert_task, update_task
from botfarm.bugtracker import PollResult
from botfarm.supervisor import (
    Supervisor,
    _WorkerResult,
)
from tests.helpers import make_config, make_issue



# ---------------------------------------------------------------------------
# Supervisor construction
# ---------------------------------------------------------------------------


class TestSupervisorInit:
    def test_creates_slot_manager_with_registered_slots(self, supervisor):
        sm = supervisor.slot_manager
        assert sm.get_slot("test-project", 1) is not None
        assert sm.get_slot("test-project", 2) is not None
        assert len(sm.all_slots()) == 2

    def test_all_slots_start_free(self, supervisor):
        sm = supervisor.slot_manager
        assert all(s.status == "free" for s in sm.all_slots())

    def test_shutdown_not_requested_initially(self, supervisor):
        assert supervisor.shutdown_requested is False


class TestSetupMode:
    """Supervisor starts with minimal config (no projects, no API key)."""

    @pytest.fixture()
    def setup_supervisor(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        monkeypatch.setattr(
            Supervisor, "_slot_db_path",
            staticmethod(lambda pn, sid: str(tmp_path / "slots" / f"{pn}-{sid}" / "botfarm.db")),
        )
        config = BotfarmConfig(
            projects=[],
            bugtracker=LinearConfig(api_key="", poll_interval_seconds=10),
        )
        assert config.setup_mode is True
        sup = Supervisor(config, log_dir=tmp_path / "logs")
        return sup

    def test_no_slots_registered(self, setup_supervisor):
        assert setup_supervisor.slot_manager.all_slots() == []

    def test_no_pollers(self, setup_supervisor):
        assert setup_supervisor._pollers == {}

    def test_no_bugtracker_client(self, setup_supervisor):
        assert setup_supervisor._bugtracker_client is None

    def test_run_enters_degraded_mode(self, setup_supervisor):
        """Supervisor.run() enters degraded mode immediately in setup mode."""
        sup = setup_supervisor
        # Make it shut down after one tick
        sup._shutdown_requested = True
        exit_code = sup.run()
        assert exit_code == 0
        assert sup._degraded is True




# ---------------------------------------------------------------------------
# Signal handling
# ---------------------------------------------------------------------------


class TestSignalHandling:
    def test_handle_sigterm_sets_shutdown(self, supervisor):
        supervisor._handle_signal(signal.SIGTERM, None)
        assert supervisor.shutdown_requested is True

    def test_handle_sigint_sets_shutdown(self, supervisor):
        supervisor._handle_signal(signal.SIGINT, None)
        assert supervisor.shutdown_requested is True

    def test_install_signal_handlers(self, supervisor):
        with patch("botfarm.supervisor.signal.signal") as mock_signal:
            supervisor.install_signal_handlers()
            calls = mock_signal.call_args_list
            sig_nums = [c[0][0] for c in calls]
            assert signal.SIGTERM in sig_nums
            assert signal.SIGINT in sig_nums




# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------


class TestReconcileWorkers:
    def test_cleans_up_finished_processes(self, supervisor):
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False
        supervisor._workers[("test-project", 1)] = mock_proc

        supervisor._reconcile_workers()

        mock_proc.join.assert_called_once_with(timeout=1)
        assert ("test-project", 1) not in supervisor._workers

    def test_keeps_alive_processes(self, supervisor):
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        supervisor._workers[("test-project", 1)] = mock_proc

        supervisor._reconcile_workers()

        assert ("test-project", 1) in supervisor._workers

    def test_drains_success_from_result_queue(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=True,
        ))

        supervisor._reconcile_workers()

        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"

    def test_drains_failure_from_result_queue(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="crash",
        ))

        with patch.object(supervisor, "_check_usage_api_for_limit", return_value=False):
            supervisor._reconcile_workers()

        assert sm.get_slot("test-project", 1).status == "failed"




# ---------------------------------------------------------------------------
# Handle finished slots
# ---------------------------------------------------------------------------


class TestHandleFinishedSlots:
    def test_completed_slot_moved_to_in_review_and_freed(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "In Review")
        assert sm.get_slot("test-project", 1).status == "free"

    def test_completed_slot_merged_pr_moved_to_done(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")):
            supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Done")
        assert sm.get_slot("test-project", 1).status == "free"

    def test_failed_slot_labeled_and_freed(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        poller.move_issue.assert_not_called()
        poller.add_labels.assert_called_once_with("TST-1", ["Failed", "Human"])
        poller.add_comment.assert_called_once()
        assert sm.get_slot("test-project", 1).status == "free"

    def test_completed_slot_linear_failure_still_frees_slot(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]
        poller.move_issue.side_effect = Exception("API down")

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        assert sm.get_slot("test-project", 1).status == "free"

    def test_failed_slot_linear_failure_still_frees_slot(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]
        poller.move_issue.side_effect = Exception("API down")

        supervisor._handle_finished_slots()

        assert sm.get_slot("test-project", 1).status == "free"

    def test_failed_slot_with_merged_pr_treated_as_completed(self, supervisor):
        """Failed slot whose PR is merged should be treated as completed, not retried."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="post-merge checkout failed")

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")):
            supervisor._handle_finished_slots()

        # Should move to Done (not Todo/failed_status)
        poller.move_issue.assert_called_once_with("TST-1", "Done")
        assert sm.get_slot("test-project", 1).status == "free"

    def test_failed_slot_no_merged_pr_adds_labels(self, supervisor):
        """Failed slot without a merged PR should add Failed+Human labels, not move ticket."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        poller.move_issue.assert_not_called()
        poller.add_labels.assert_called_once_with("TST-1", ["Failed", "Human"])
        assert sm.get_slot("test-project", 1).status == "free"

    def test_failed_slot_merged_pr_updates_task_to_completed(self, supervisor):
        """Failed slot with merged PR should update the DB task status to completed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="post-merge checkout failed")

        # Insert a task so we can verify it gets updated
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1",
            title="Test task",
            project="test-project",
            slot=1,
            status="failed",
        )
        supervisor._conn.commit()

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")):
            supervisor._handle_finished_slots()

        task = get_task(supervisor._conn, task_id)
        assert task["status"] == "completed"

    def test_events_recorded_for_completed(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        events = get_events(supervisor._conn, event_type="slot_completed")
        assert len(events) == 1
        assert "TST-1" in events[0]["detail"]

    def test_events_recorded_for_failed(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        supervisor._handle_finished_slots()

        events = get_events(supervisor._conn, event_type="slot_failed")
        assert len(events) == 1
        assert "TST-1" in events[0]["detail"]




class TestPollAndDispatch:
    def test_no_candidates_no_dispatch(self, supervisor):
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

    def test_no_free_slots_no_dispatch(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="T1", branch="b1",
        )
        sm.assign_ticket(
            "test-project", 2,
            ticket_id="TST-2", ticket_title="T2", branch="b2",
        )

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

    def test_dispatches_to_free_slot(self, supervisor):
        issue = make_issue()
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[issue], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_called_once()
            args = mock_dispatch.call_args
            assert args[0][0] == "test-project"
            assert args[0][2] == issue

    def test_poll_failure_does_not_crash(self, supervisor):
        poller = supervisor._pollers["test-project"]
        poller.poll.side_effect = Exception("API error")

        # Should not raise
        supervisor._poll_and_dispatch()


class TestHumanBlockerDetection:
    @pytest.fixture(autouse=True)
    def _mock_notifier(self, supervisor):
        supervisor._notifier.notify_human_blocker = MagicMock()

    def test_notifies_when_human_blocker_found(self, supervisor):
        """When no candidates, free slot exists, and a blocked issue is
        blocked by a Human-labeled ticket, a notification is sent."""
        blocked = make_issue(
            identifier="TST-10", title="Blocked task",
            blocked_by=["TST-99"],
        )
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[], blocked=[blocked], auto_close_parents=[],
        )

        supervisor._bugtracker_client.fetch_issue_labels = MagicMock(
            return_value=("Create GitHub accounts", ["Human"]),
        )

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        supervisor._notifier.notify_human_blocker.assert_called_once_with(
            blocker_id="TST-99",
            blocker_title="Create GitHub accounts",
            blocked_tickets=["TST-10"],
        )

    def test_no_notification_when_blocker_lacks_human_label(self, supervisor):
        """If the blocker doesn't have the Human label, no notification."""
        blocked = make_issue(
            identifier="TST-10", title="Blocked",
            blocked_by=["TST-99"],
        )
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[], blocked=[blocked], auto_close_parents=[],
        )

        supervisor._bugtracker_client.fetch_issue_labels = MagicMock(
            return_value=("Some other task", ["Bug"]),
        )

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        supervisor._notifier.notify_human_blocker.assert_not_called()

    def test_no_notification_when_candidates_exist(self, supervisor):
        """If there are candidates, don't check for human blockers."""
        candidate = make_issue(identifier="TST-1", title="Ready")
        blocked = make_issue(
            identifier="TST-10", title="Blocked",
            blocked_by=["TST-99"],
        )
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[candidate], blocked=[blocked], auto_close_parents=[],
        )

        supervisor._bugtracker_client.fetch_issue_labels = MagicMock()

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        supervisor._bugtracker_client.fetch_issue_labels.assert_not_called()

    def test_multiple_blocked_by_same_human_blocker(self, supervisor):
        """Multiple blocked issues sharing one Human blocker → single notification."""
        blocked1 = make_issue(
            identifier="TST-10", blocked_by=["TST-99"],
        )
        blocked2 = make_issue(
            identifier="TST-11", blocked_by=["TST-99"],
        )
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[], blocked=[blocked1, blocked2], auto_close_parents=[],
        )

        supervisor._bugtracker_client.fetch_issue_labels = MagicMock(
            return_value=("Manual step", ["Human"]),
        )

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        supervisor._notifier.notify_human_blocker.assert_called_once_with(
            blocker_id="TST-99",
            blocker_title="Manual step",
            blocked_tickets=["TST-10", "TST-11"],
        )

    def test_label_lookup_failure_does_not_crash(self, supervisor):
        """API failure during label lookup is handled gracefully."""
        blocked = make_issue(
            identifier="TST-10", blocked_by=["TST-99"],
        )
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[], blocked=[blocked], auto_close_parents=[],
        )

        supervisor._bugtracker_client.fetch_issue_labels = MagicMock(
            side_effect=Exception("API error"),
        )

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        supervisor._notifier.notify_human_blocker.assert_not_called()

    def test_no_check_when_no_free_slot(self, supervisor):
        """If no free slot, blocked issues are not checked for human blockers."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="T1", branch="b1",
        )
        sm.assign_ticket(
            "test-project", 2,
            ticket_id="TST-2", ticket_title="T2", branch="b2",
        )

        blocked = make_issue(
            identifier="TST-10", blocked_by=["TST-99"],
        )
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[], blocked=[blocked], auto_close_parents=[],
        )

        supervisor._bugtracker_client.fetch_issue_labels = MagicMock()

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        supervisor._bugtracker_client.fetch_issue_labels.assert_not_called()


class TestParentAutoClose:
    def test_auto_closes_parent_with_all_children_done(self, supervisor):
        """Parent issues with all children done are auto-moved to Done."""
        parent_issue = make_issue(identifier="TST-100", title="Parent")
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[],
            blocked=[],
            auto_close_parents=[parent_issue],
        )

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

        poller.move_issue.assert_called_once_with("TST-100", "Done")

        events = get_events(supervisor._conn, event_type="parent_auto_closed")
        assert len(events) == 1
        assert "TST-100" in events[0]["detail"]

    def test_auto_close_failure_does_not_crash(self, supervisor):
        """If auto-close fails, dispatch continues normally."""
        parent = make_issue(identifier="TST-100", title="Parent")
        normal = make_issue(identifier="TST-1", title="Normal")
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[normal],
            blocked=[],
            auto_close_parents=[parent],
        )
        poller.move_issue.side_effect = Exception("API error")

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_called_once()

    def test_auto_close_does_not_consume_slot(self, supervisor):
        """Auto-closing a parent doesn't use a worker slot."""
        parent = make_issue(identifier="TST-100", title="Parent")
        normal = make_issue(identifier="TST-1", title="Normal")
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[normal],
            blocked=[],
            auto_close_parents=[parent],
        )

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_called_once()
            # Dispatched the normal issue, not the parent
            assert mock_dispatch.call_args[0][2] == normal

    def test_multiple_parents_auto_closed(self, supervisor):
        """Multiple auto-closeable parents are all processed."""
        p1 = make_issue(identifier="TST-100", title="Parent 1")
        p2 = make_issue(identifier="TST-200", title="Parent 2")
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[],
            blocked=[],
            auto_close_parents=[p1, p2],
        )

        supervisor._poll_and_dispatch()

        assert poller.move_issue.call_count == 2
        poller.move_issue.assert_any_call("TST-100", "Done")
        poller.move_issue.assert_any_call("TST-200", "Done")

        events = get_events(supervisor._conn, event_type="parent_auto_closed")
        assert len(events) == 2




# ---------------------------------------------------------------------------
# Per-project pause in dispatch
# ---------------------------------------------------------------------------


class TestPerProjectPause:
    def test_paused_project_skipped_in_dispatch(self, supervisor):
        """A paused project should not have its poller called."""
        supervisor.slot_manager.set_project_paused("test-project", True, "manual")
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()
            poller.poll.assert_not_called()

    def test_unpaused_project_dispatched(self, supervisor):
        """After unpausing, the project is dispatched normally."""
        supervisor.slot_manager.set_project_paused("test-project", True, "manual")
        supervisor.slot_manager.set_project_paused("test-project", False)

        issue = make_issue()
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[issue], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_called_once()

    def test_paused_project_does_not_affect_other_projects(self, supervisor, tmp_path):
        """Pausing one project doesn't stop others from dispatching.

        This test uses a single-project config, so we verify the paused
        project's poller is skipped. The architectural guarantee is that
        each project is checked independently in the loop.
        """
        supervisor.slot_manager.set_project_paused("test-project", True)
        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            poller.poll.assert_not_called()
            mock_dispatch.assert_not_called()

    def test_refresh_picks_up_external_pause(self, supervisor):
        """Supervisor picks up per-project pause set via CLI/dashboard."""
        from botfarm.db import save_project_pause_state
        save_project_pause_state(
            supervisor._conn, project="test-project", paused=True, reason="cli",
        )
        supervisor._conn.commit()

        # Simulate what _tick does
        supervisor.slot_manager.refresh_project_pauses()

        assert supervisor.slot_manager.is_project_paused("test-project") is True
        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()
            poller.poll.assert_not_called()




# ---------------------------------------------------------------------------
# Dispatch worker
# ---------------------------------------------------------------------------


class TestDispatchWorker:
    def test_creates_task_and_spawns_process(self, supervisor):
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc

            supervisor._dispatch_worker("test-project", slot, issue, poller)

            # Process started
            mock_proc.start.assert_called_once()

            # Slot assigned
            updated_slot = supervisor.slot_manager.get_slot("test-project", 1)
            assert updated_slot.status == "busy"
            assert updated_slot.ticket_id == "TST-1"
            assert updated_slot.pid == 12345

            # Linear moved to In Progress
            poller.move_issue.assert_called_once_with(issue.identifier, "In Progress")

            # Task in DB
            task = get_task(supervisor._conn, 1)
            assert task is not None
            assert task["ticket_id"] == "TST-1"
            assert task["status"] == "in_progress"

    def test_linear_move_failure_skips_dispatch(self, supervisor):
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]
        poller.move_issue.side_effect = Exception("API down")

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            supervisor._dispatch_worker("test-project", slot, issue, poller)
            MockProc.assert_not_called()

        # Slot remains free
        assert supervisor.slot_manager.get_slot("test-project", 1).status == "free"

    def test_worker_dispatched_event_recorded(self, supervisor):
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 99
            MockProc.return_value = mock_proc

            supervisor._dispatch_worker("test-project", slot, issue, poller)

        events = get_events(supervisor._conn, event_type="worker_dispatched")
        assert len(events) == 1
        assert "TST-1" in events[0]["detail"]

    def test_process_not_daemon(self, supervisor):
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 99
            MockProc.return_value = mock_proc

            supervisor._dispatch_worker("test-project", slot, issue, poller)

            # Verify daemon=False so workers survive supervisor exit
            assert MockProc.call_args.kwargs["daemon"] is False




# ---------------------------------------------------------------------------
# Coder auto-assignment on dispatch
# ---------------------------------------------------------------------------


def _make_config_with_coder(tmp_path: Path) -> BotfarmConfig:
    """Build a config with coder identity (tracker_api_key set)."""
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project",
                team="TST",
                base_dir=str(tmp_path / "repo"),
                worktree_prefix="test-project-slot-",
                slots=[1, 2],
            ),
        ],
        bugtracker=LinearConfig(
            api_key="owner-key",
            poll_interval_seconds=10,
            exclude_tags=["Human"],
        ),
        database=DatabaseConfig(),
        identities=IdentitiesConfig(
            coder=CoderIdentity(tracker_api_key="coder-key"),
        ),
    )




class TestCoderAutoAssignment:
    """Tests for SMA-194: auto-assign tickets to coder bot on dispatch."""

    @pytest.fixture()
    def supervisor_with_coder(self, tmp_path, monkeypatch):
        """Supervisor with coder identity configured and viewer ID cached."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = _make_config_with_coder(tmp_path)
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(
            candidates=[], blocked=[], auto_close_parents=[],
        )
        mock_poller.is_issue_terminal.return_value = False

        mock_coder_client = MagicMock()
        mock_coder_client.get_viewer_id.return_value = "coder-user-id-123"

        with (
            patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]),
            patch("botfarm.supervisor.create_client", return_value=mock_coder_client),
        ):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        # Attach the mock so tests can assert on it
        sup._coder_tracker = mock_coder_client
        return sup

    def test_init_caches_coder_viewer_id(self, supervisor_with_coder):
        """Supervisor caches coder viewer ID at startup."""
        assert supervisor_with_coder._coder_viewer_id == "coder-user-id-123"
        assert supervisor_with_coder._coder_tracker is not None

    def test_init_no_coder_key_skips_caching(self, supervisor):
        """Without coder tracker_api_key, no coder client is created."""
        assert supervisor._coder_viewer_id is None
        assert supervisor._coder_tracker is None

    def test_init_coder_viewer_id_failure_disables_assignment(self, tmp_path, monkeypatch):
        """If get_viewer_id() fails at startup, auto-assignment is disabled."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = _make_config_with_coder(tmp_path)
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(
            candidates=[], blocked=[], auto_close_parents=[],
        )
        mock_poller.is_issue_terminal.return_value = False

        mock_coder_client = MagicMock()
        mock_coder_client.get_viewer_id.side_effect = Exception("API down")

        with (
            patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]),
            patch("botfarm.supervisor.create_client", return_value=mock_coder_client),
        ):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        assert sup._coder_viewer_id is None
        assert sup._coder_tracker is None

    def test_dispatch_assigns_to_coder(self, supervisor_with_coder):
        """Dispatch auto-assigns the ticket to the coder bot."""
        issue = make_issue()
        slot = supervisor_with_coder.slot_manager.get_slot("test-project", 1)
        poller = supervisor_with_coder._pollers["test-project"]

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 42
            MockProc.return_value = mock_proc

            supervisor_with_coder._dispatch_worker("test-project", slot, issue, poller)

        supervisor_with_coder._coder_tracker.assign_issue.assert_called_once_with(
            issue.identifier, "coder-user-id-123",
        )

    def test_dispatch_continues_on_assignment_failure(self, supervisor_with_coder):
        """Assignment failure does not block dispatch."""
        issue = make_issue()
        slot = supervisor_with_coder.slot_manager.get_slot("test-project", 1)
        poller = supervisor_with_coder._pollers["test-project"]
        supervisor_with_coder._coder_tracker.assign_issue.side_effect = Exception("API error")

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 42
            MockProc.return_value = mock_proc

            supervisor_with_coder._dispatch_worker("test-project", slot, issue, poller)

            # Worker still spawned despite assignment failure
            mock_proc.start.assert_called_once()

    def test_dispatch_skips_assignment_without_coder(self, supervisor):
        """Without coder identity, no assignment call is made."""
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 42
            MockProc.return_value = mock_proc

            supervisor._dispatch_worker("test-project", slot, issue, poller)

        # No coder_linear means no assign_issue call — verify via attribute
        assert supervisor._coder_tracker is None




# ---------------------------------------------------------------------------
# Tick
# ---------------------------------------------------------------------------


class TestTick:
    def test_tick_calls_all_phases(self, supervisor):
        with (
            patch.object(supervisor, "_reconcile_workers") as mock_reconcile,
            patch.object(supervisor, "_handle_finished_slots") as mock_handle,
            patch.object(supervisor, "_poll_usage") as mock_usage,
            patch.object(supervisor, "_poll_and_dispatch") as mock_poll,
        ):
            supervisor._tick()
            mock_reconcile.assert_called_once()
            mock_handle.assert_called_once()
            mock_usage.assert_called_once()
            mock_poll.assert_called_once()

    def test_tick_exception_does_not_propagate(self, supervisor):
        with patch.object(
            supervisor, "_reconcile_workers", side_effect=RuntimeError("boom")
        ):
            # Should not raise
            supervisor._tick()

    def test_tick_phase_failure_does_not_skip_others(self, supervisor):
        with (
            patch.object(
                supervisor, "_reconcile_workers", side_effect=RuntimeError("boom")
            ),
            patch.object(supervisor, "_handle_finished_slots") as mock_handle,
            patch.object(supervisor, "_poll_usage") as mock_usage,
            patch.object(supervisor, "_poll_and_dispatch") as mock_poll,
        ):
            supervisor._tick()
            # Other phases still run despite _reconcile_workers failure
            mock_handle.assert_called_once()
            mock_usage.assert_called_once()
            mock_poll.assert_called_once()




# ---------------------------------------------------------------------------
# Run / main loop
# ---------------------------------------------------------------------------


class TestRun:
    def test_run_loop_ticks_and_sleeps(self, supervisor, tmp_path):
        tick_count = 0

        def fake_tick():
            nonlocal tick_count
            tick_count += 1
            if tick_count >= 2:
                supervisor._shutdown_requested = True

        with (
            patch.object(supervisor, "_tick", side_effect=fake_tick),
            patch.object(supervisor, "_sleep") as mock_sleep,
            patch.object(supervisor, "install_signal_handlers"),
            patch("botfarm.supervisor.run_preflight_checks", return_value=[]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=True),
        ):
            supervisor.run()

        assert tick_count == 2
        # Sleep called between iterations (once, since shutdown on 2nd tick)
        assert mock_sleep.call_count == 1

    def test_run_records_start_and_stop_events(self, supervisor, tmp_path):
        supervisor._shutdown_requested = True

        with (
            patch.object(supervisor, "install_signal_handlers"),
            patch("botfarm.supervisor.run_preflight_checks", return_value=[]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=True),
        ):
            supervisor.run()

        # Re-open conn since _shutdown closes it
        conn = init_db(supervisor._db_path)
        start_events = get_events(conn, event_type="supervisor_start")
        assert len(start_events) >= 1

        stop_events = get_events(conn, event_type="supervisor_stop")
        assert len(stop_events) >= 1
        conn.close()




# ---------------------------------------------------------------------------
# Shutdown
# ---------------------------------------------------------------------------


class TestShutdown:
    def test_shutdown_persists_state(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._shutdown()

        # Open fresh connection — shutdown closes the original
        from botfarm.db import init_db, load_all_slots
        conn = init_db(sm._db_path)
        rows = load_all_slots(conn)
        busy = [dict(r) for r in rows if r["status"] == "busy"]
        assert len(busy) == 1
        conn.close()

    def test_shutdown_logs_running_workers(self, supervisor):
        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = True
        mock_proc.pid = 42
        supervisor._workers[("test-project", 1)] = mock_proc

        supervisor._shutdown()

        # Verify stop event was recorded
        conn = init_db(supervisor._db_path)
        events = get_events(conn, event_type="supervisor_stop")
        assert len(events) >= 1
        assert "running_workers=1" in events[0]["detail"]
        conn.close()

    def test_shutdown_sends_notification_on_signal(self, supervisor):
        supervisor._notifier = MagicMock()
        supervisor._shutdown_requested = True
        supervisor._exit_code = 0
        supervisor._shutdown()
        supervisor._notifier.notify_supervisor_shutdown.assert_called_once_with(
            reason="SIGTERM/SIGINT received",
        )

    def test_shutdown_sends_notification_on_unexpected_error(self, supervisor):
        supervisor._notifier = MagicMock()
        supervisor._shutdown_requested = False
        supervisor._exit_code = 0
        supervisor._shutdown()
        supervisor._notifier.notify_supervisor_shutdown.assert_called_once_with(
            reason="unexpected error",
        )

    def test_shutdown_skips_notification_on_update_restart(self, supervisor):
        from botfarm.git_update import UPDATE_EXIT_CODE

        supervisor._notifier = MagicMock()
        supervisor._exit_code = UPDATE_EXIT_CODE
        supervisor._shutdown()
        supervisor._notifier.notify_supervisor_shutdown.assert_not_called()




# ---------------------------------------------------------------------------
# Sleep
# ---------------------------------------------------------------------------


class TestSleep:
    def test_sleep_can_be_interrupted(self, supervisor):
        import time

        supervisor._shutdown_requested = True
        start = time.monotonic()
        supervisor._sleep(10)
        elapsed = time.monotonic() - start
        assert elapsed < 2  # Should exit almost immediately

    def test_sleep_respects_duration(self, supervisor):
        with patch("botfarm.supervisor.time.sleep") as mock_sleep:
            supervisor._sleep(3)
            assert mock_sleep.call_count == 3




# ---------------------------------------------------------------------------
# Multi-project support
# ---------------------------------------------------------------------------


class TestMultiProject:
    def test_dispatches_across_projects(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="alpha",
                    team="ALPHA",
                    base_dir=str(tmp_path / "alpha"),
                    worktree_prefix="alpha-slot-",
                    slots=[1],
                ),
                ProjectConfig(
                    name="beta",
                    team="BETA",
                    base_dir=str(tmp_path / "beta"),
                    worktree_prefix="beta-slot-",
                    slots=[2],
                ),
            ],
            bugtracker=LinearConfig(
                api_key="test-key",
                poll_interval_seconds=10,
            ),
            database=DatabaseConfig(),
        )
        (tmp_path / "alpha").mkdir()
        (tmp_path / "beta").mkdir()

        mock_alpha = MagicMock()
        mock_alpha.project_name = "alpha"
        mock_alpha.poll.return_value = PollResult(candidates=[make_issue(id="a1", identifier="A-1")], blocked=[], auto_close_parents=[])

        mock_beta = MagicMock()
        mock_beta.project_name = "beta"
        mock_beta.poll.return_value = PollResult(candidates=[make_issue(id="b1", identifier="B-1")], blocked=[], auto_close_parents=[])

        with patch(
            "botfarm.supervisor.create_pollers",
            return_value=[mock_alpha, mock_beta],
        ):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 100
            MockProc.return_value = mock_proc

            sup._poll_and_dispatch()

        # Both projects should have dispatched
        sm = sup.slot_manager
        assert sm.get_slot("alpha", 1).status == "busy"
        assert sm.get_slot("beta", 2).status == "busy"
        sup._conn.close()




class TestNextStageAfter:
    """Tests for _next_stage_after — the stage ordering helper."""

    def test_no_stages_returns_implement(self):
        assert Supervisor._next_stage_after([]) == "implement"

    def test_implement_completed_returns_review(self):
        assert Supervisor._next_stage_after(["implement"]) == "review"

    def test_through_fix_returns_pr_checks(self):
        assert Supervisor._next_stage_after(["implement", "review", "fix"]) == "pr_checks"

    def test_through_pr_checks_returns_merge(self):
        assert Supervisor._next_stage_after(
            ["implement", "review", "fix", "pr_checks"]
        ) == "merge"

    def test_all_completed_returns_none(self):
        assert Supervisor._next_stage_after(
            ["implement", "review", "fix", "pr_checks", "merge"]
        ) is None

    def test_out_of_order_still_finds_gaps(self):
        # review completed but implement not — implement is the gap
        assert Supervisor._next_stage_after(["review", "fix"]) == "implement"




class TestReconcileDbTasks:
    """Tests for _reconcile_db_tasks — DB/state consistency."""

    def test_in_progress_task_with_failed_slot_reconciled(self, supervisor):
        """DB task in_progress + slot failed -> DB updated to failed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._reconcile_db_tasks()

        task = get_task(supervisor._conn, task_id)
        assert task["status"] == "failed"

    def test_in_progress_task_with_completed_slot_reconciled(self, supervisor):
        """DB task in_progress + slot completed -> DB updated to completed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._reconcile_db_tasks()

        task = get_task(supervisor._conn, task_id)
        assert task["status"] == "completed"

    def test_already_consistent_not_changed(self, supervisor):
        """DB task that already matches slot status is not modified."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._reconcile_db_tasks()

        # busy slot + in_progress task = consistent, no change
        task = get_task(supervisor._conn, task_id)
        assert task["status"] == "in_progress"

    def test_no_task_in_db_no_crash(self, supervisor):
        """Slot with ticket_id that has no DB record doesn't crash."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-UNKNOWN", ticket_title="Test", branch="b1",
        )
        # No insert_task call — ticket has no DB record

        # Should not raise
        supervisor._reconcile_db_tasks()




# ---------------------------------------------------------------------------
# Configurable Linear status names
# ---------------------------------------------------------------------------


def _make_config_custom_statuses(tmp_path: Path) -> BotfarmConfig:
    """Build a config with custom Linear status names."""
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project",
                team="TST",
                base_dir=str(tmp_path / "repo"),
                worktree_prefix="test-project-slot-",
                slots=[1, 2],
            ),
        ],
        bugtracker=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=10,
            exclude_tags=["Human"],
            todo_status="Backlog",
            in_progress_status="Working",
            done_status="Shipped",
            in_review_status="Review",
            comment_on_failure=True,
            comment_on_completion=True,
            comment_on_limit_pause=True,
        ),
        database=DatabaseConfig(),
    )




@pytest.fixture()
def supervisor_custom_statuses(tmp_path, monkeypatch):
    """Create a Supervisor with custom status names."""
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
    config = _make_config_custom_statuses(tmp_path)
    (tmp_path / "repo").mkdir()

    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
    mock_poller.is_issue_terminal.return_value = False

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(config, log_dir=tmp_path / "logs")

    return sup




class TestConfigurableStatusNames:
    def test_dispatch_uses_custom_in_progress_status(self, supervisor_custom_statuses, tmp_path):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        poller = sup._pollers["test-project"]
        issue = make_issue()

        slot = sm.get_slot("test-project", 1)

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc
            sup._dispatch_worker("test-project", slot, issue, poller)

        poller.move_issue.assert_called_once_with("TST-1", "Working")

    def test_completed_slot_uses_custom_in_review_status(self, supervisor_custom_statuses):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = sup._pollers["test-project"]

        with patch.object(sup, "_check_pr_status", return_value=(None, None)):
            sup._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Review")

    def test_completed_merged_slot_uses_custom_done_status(self, supervisor_custom_statuses):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = sup._pollers["test-project"]

        with patch.object(sup, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")):
            sup._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Shipped")

    def test_failed_slot_adds_labels_regardless_of_config(self, supervisor_custom_statuses):
        """Failed slot should add labels rather than moving to any status."""
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = sup._pollers["test-project"]

        sup._handle_finished_slots()

        poller.move_issue.assert_not_called()
        poller.add_labels.assert_called_once_with("TST-1", ["Failed", "Human"])




# ---------------------------------------------------------------------------
# Comment posting behavior
# ---------------------------------------------------------------------------


class TestCommentPosting:
    def test_failure_comment_posted_by_default(self, supervisor):
        """comment_on_failure is True by default."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        poller.add_comment.assert_called_once()
        comment_body = poller.add_comment.call_args[0][1]
        assert "failed" in comment_body.lower()

    def test_failure_comment_includes_stage_and_reason(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        # Create a DB task record with failure info
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project",
            slot=1, status="failed",
        )
        update_task(
            supervisor._conn, task_id,
            failure_reason="implement: some error",
        )
        supervisor._conn.commit()

        sm.mark_failed("test-project", 1, reason="implement: some error")
        # Set the stage on the slot so it appears in the comment
        slot = sm.get_slot("test-project", 1)
        slot.stage = "implement"

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        comment_body = poller.add_comment.call_args[0][1]
        assert "implement" in comment_body
        assert "some error" in comment_body

    def test_failure_comment_disabled(self, tmp_path, monkeypatch):
        """comment_on_failure=False suppresses failure comments."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test-project",
                    team="TST",
                    base_dir=str(tmp_path / "repo"),
                    worktree_prefix="test-project-slot-",
                    slots=[1],
                ),
            ],
            bugtracker=LinearConfig(
                api_key="test-key",
                poll_interval_seconds=10,
                comment_on_failure=False,
            ),
            database=DatabaseConfig(),
        )
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        sup._handle_finished_slots()

        mock_poller.add_comment.assert_not_called()

    def test_completion_comment_posted_when_enabled(self, supervisor_custom_statuses):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = sup._pollers["test-project"]

        with patch.object(sup, "_check_pr_status", return_value=(None, None)):
            sup._handle_finished_slots()

        # Should have two calls: move_issue and add_comment
        poller.add_comment.assert_called_once()
        comment_body = poller.add_comment.call_args[0][1]
        assert "completed" in comment_body.lower()

    def test_completion_comment_not_posted_by_default(self, supervisor):
        """comment_on_completion defaults to False."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        poller.add_comment.assert_not_called()

    def test_completion_comment_includes_pr_url(self, supervisor_custom_statuses):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)
        slot = sm.get_slot("test-project", 1)
        slot.pr_url = "https://github.com/org/repo/pull/42"

        poller = sup._pollers["test-project"]

        with patch.object(sup, "_check_pr_status", return_value=(None, None)):
            sup._handle_finished_slots()

        comment_body = poller.add_comment.call_args[0][1]
        assert "https://github.com/org/repo/pull/42" in comment_body

    def test_limit_pause_comment_posted_when_enabled(self, supervisor_custom_statuses):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        wr = _WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="rate limit hit",
            limit_hit=True,
        )

        sup._handle_limit_hit(wr)

        poller = sup._pollers["test-project"]
        poller.add_comment_as_owner.assert_called_once()
        comment_body = poller.add_comment_as_owner.call_args[0][1]
        assert "usage limit" in comment_body.lower()
        assert "implement" in comment_body

    def test_limit_pause_comment_not_posted_by_default(self, supervisor):
        """comment_on_limit_pause defaults to False."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        wr = _WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="rate limit hit",
            limit_hit=True,
        )

        supervisor._handle_limit_hit(wr)

        poller = supervisor._pollers["test-project"]
        poller.add_comment_as_owner.assert_not_called()

    def test_failure_comment_error_does_not_break_handling(self, supervisor):
        """If posting the failure comment fails, the slot is still freed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]
        poller.add_comment.side_effect = Exception("comment API down")

        supervisor._handle_finished_slots()

        assert sm.get_slot("test-project", 1).status == "free"

    def test_failure_comment_includes_result_text(self, supervisor):
        """Failed task comment includes the agent's last result text."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project",
            slot=1, status="failed",
        )
        update_task(
            supervisor._conn, task_id,
            failure_reason="implement: tests failed",
        )
        supervisor._conn.commit()

        sm.mark_failed("test-project", 1, reason="implement: tests failed")
        slot = sm.get_slot("test-project", 1)
        slot.stage = "implement"

        # Stash result text as the supervisor would after draining the queue
        supervisor._pending_result_texts[("test-project", 1)] = (
            "I tried to fix the tests but they kept failing on assertion X."
        )

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        comment_body = poller.add_comment.call_args[0][1]
        assert "Bot outcome: failed" in comment_body
        assert "tests failed" in comment_body
        assert "assertion X" in comment_body
        # result_text should be cleaned up
        assert ("test-project", 1) not in supervisor._pending_result_texts

    def test_no_pr_comment_posted_on_success_without_pr(self, supervisor):
        """Success-without-PR tasks get a comment with the agent summary."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.no_pr_reason = "All acceptance criteria already met on main"
        sm.mark_completed("test-project", 1)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project",
            slot=1, status="completed",
        )
        supervisor._conn.commit()

        supervisor._pending_result_texts[("test-project", 1)] = (
            "I verified all 5 acceptance criteria against the main branch."
        )

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        comment_body = poller.add_comment.call_args[0][1]
        assert "Bot outcome: completed_no_pr" in comment_body
        assert "acceptance criteria" in comment_body
        # result_text should be cleaned up
        assert ("test-project", 1) not in supervisor._pending_result_texts

    def test_no_comment_on_normal_pr_completion(self, supervisor):
        """Normal PR-based completions do NOT get a terminal comment."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)
        slot = sm.get_slot("test-project", 1)
        slot.pr_url = "https://github.com/org/repo/pull/42"

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project",
            slot=1, status="completed",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")):
            supervisor._handle_finished_slots()

        # comment_on_completion is False by default, and no no_pr_reason
        # means no terminal comment should be posted
        poller.add_comment.assert_not_called()

    def test_no_pr_comment_error_is_non_blocking(self, supervisor):
        """If posting the no-PR comment fails, the slot is still freed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.no_pr_reason = "Investigation complete"
        sm.mark_completed("test-project", 1)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project",
            slot=1, status="completed",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.add_comment.side_effect = Exception("Linear API down")

        supervisor._handle_finished_slots()

        assert sm.get_slot("test-project", 1).status == "free"




# ---------------------------------------------------------------------------
# Tick summary logging
# ---------------------------------------------------------------------------


class TestLogTickSummary:
    """Tests for _log_tick_summary() — one-line worker status per tick."""

    def _make_busy_slot(self, supervisor, ticket_id="TST-1", stage="implement",
                        started_minutes_ago=45, slot_id=1):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", slot_id,
            ticket_id=ticket_id, ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", slot_id, stage=stage)
        slot = sm.get_slot("test-project", slot_id)
        started = (datetime.now(timezone.utc) - timedelta(minutes=started_minutes_ago))
        slot.started_at = started.isoformat()
        return slot

    def test_all_free_logs_debug(self, supervisor, caplog):
        """When all slots are free, log at DEBUG level (not INFO)."""
        with caplog.at_level(logging.DEBUG, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        debug_msgs = [r for r in caplog.records if r.levelno == logging.DEBUG]
        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any("all slots free" in r.message for r in debug_msgs)
        assert not any("Tick:" in r.message for r in info_msgs)

    def test_busy_slot_logs_info(self, supervisor, caplog):
        """A busy slot produces an INFO log with ticket, stage, elapsed."""
        self._make_busy_slot(supervisor, ticket_id="TST-1", stage="implement",
                            started_minutes_ago=45)

        with caplog.at_level(logging.INFO, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        assert len(info_msgs) == 1
        msg = info_msgs[0].message
        assert "1 busy" in msg
        assert "TST-1" in msg
        assert "implement" in msg
        assert "45m" in msg
        assert "1 free" in msg

    def test_elapsed_hours_format(self, supervisor, caplog):
        """Elapsed time >= 60 minutes shows hours+minutes format."""
        self._make_busy_slot(supervisor, started_minutes_ago=125)

        with caplog.at_level(logging.INFO, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        msg = info_msgs[0].message
        assert "2h5m" in msg

    def test_multiple_busy_slots(self, supervisor, caplog):
        """Multiple busy slots are listed in summary."""
        self._make_busy_slot(supervisor, ticket_id="TST-1", stage="implement",
                            started_minutes_ago=10, slot_id=1)
        self._make_busy_slot(supervisor, ticket_id="TST-2", stage="review",
                            started_minutes_ago=5, slot_id=2)

        with caplog.at_level(logging.INFO, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        msg = info_msgs[0].message
        assert "2 busy" in msg
        assert "TST-1" in msg
        assert "TST-2" in msg
        assert "0 free" in msg

    def test_includes_usage_percentage(self, supervisor, caplog):
        """Usage 5h percentage is included when available."""
        self._make_busy_slot(supervisor)
        supervisor._usage_poller._state.utilization_5h = 0.52

        with caplog.at_level(logging.INFO, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        msg = info_msgs[0].message
        assert "usage 5h=52%" in msg

    def test_no_usage_when_unavailable(self, supervisor, caplog):
        """No usage string when utilization_5h is None."""
        self._make_busy_slot(supervisor)
        supervisor._usage_poller._state.utilization_5h = None

        with caplog.at_level(logging.INFO, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        msg = info_msgs[0].message
        assert "usage" not in msg

    def test_paused_limit_slots_counted(self, supervisor, caplog):
        """Paused-limit slots are counted in the summary."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        sm.mark_paused_limit("test-project", 1)

        with caplog.at_level(logging.INFO, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        msg = info_msgs[0].message
        assert "1 paused_limit" in msg

    def test_paused_manual_slots_counted(self, supervisor, caplog):
        """Paused-manual slots are counted in the summary."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        sm.mark_paused_manual("test-project", 1)

        with caplog.at_level(logging.INFO, logger="botfarm.supervisor"):
            supervisor._log_tick_summary()

        info_msgs = [r for r in caplog.records if r.levelno == logging.INFO]
        msg = info_msgs[0].message
        assert "1 paused_manual" in msg

    def test_tick_calls_log_tick_summary(self, supervisor):
        """_tick() calls _log_tick_summary()."""
        with patch.object(supervisor, "_log_tick_summary") as mock_summary:
            supervisor._tick()
            mock_summary.assert_called_once()
