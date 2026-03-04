"""Tests for botfarm.supervisor — daemon loop, dispatch, signal handling."""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from botfarm.config import (
    AgentsConfig,
    BotfarmConfig,
    CapacityConfig,
    CoderIdentity,
    DatabaseConfig,
    IdentitiesConfig,
    LinearConfig,
    ProjectConfig,
)
from botfarm.db import get_events, get_task, get_ticket_history_entry, init_db, insert_task, load_capacity_state, update_task
from botfarm.linear import ActiveIssuesCount, LinearIssue, PollResult
from botfarm.supervisor import (
    DEFAULT_LOG_DIR,
    StopSlotResult,
    Supervisor,
    _StallInfo,
    _WorkerResult,
    _check_limit_hit,
    _setup_worker_logging,
    _worker_entry,
    setup_logging,
)
from botfarm.worker import PipelineResult, STAGES
from tests.helpers import make_config, make_issue


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def tmp_config(tmp_path):
    """Return a BotfarmConfig and ensure directories exist."""
    config = make_config(tmp_path)
    (tmp_path / "repo").mkdir()
    return config


@pytest.fixture()
def supervisor(tmp_config, tmp_path, monkeypatch):
    """Create a Supervisor with mocked pollers (no real Linear calls)."""
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
    mock_poller.is_issue_terminal.return_value = False

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

    return sup


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

    def test_failed_slot_moved_back_to_todo_and_freed(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Todo")
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

    def test_failed_slot_no_merged_pr_moves_to_failed_status(self, supervisor):
        """Failed slot without a merged PR should move to failed_status as before."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Todo")
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


# ---------------------------------------------------------------------------
# Poll and dispatch
# ---------------------------------------------------------------------------


class TestDispatchPauseResume:
    def test_5h_threshold_pauses_dispatch(self, supervisor):
        """Dispatch is paused when 5h utilization >= threshold."""
        supervisor._usage_poller._state.utilization_5h = 0.86
        supervisor._usage_poller._state.utilization_7d = 0.50

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

        assert supervisor.slot_manager.dispatch_paused is True
        assert "5-hour" in supervisor.slot_manager.dispatch_pause_reason

    def test_7d_threshold_pauses_dispatch(self, supervisor):
        """Dispatch is paused when 7d utilization >= threshold."""
        supervisor._usage_poller._state.utilization_5h = 0.50
        supervisor._usage_poller._state.utilization_7d = 0.91

        supervisor._poll_and_dispatch()

        assert supervisor.slot_manager.dispatch_paused is True
        assert "7-day" in supervisor.slot_manager.dispatch_pause_reason

    def test_pause_event_logged_only_once(self, supervisor):
        """The dispatch_paused event is logged only on transition to paused."""
        supervisor._usage_poller._state.utilization_5h = 0.90

        supervisor._poll_and_dispatch()  # first call — logs event
        supervisor._poll_and_dispatch()  # second call — no new event

        events = get_events(supervisor._conn, event_type="dispatch_paused")
        assert len(events) == 1

    def test_resume_when_utilization_drops(self, supervisor):
        """Dispatch resumes when utilization drops below thresholds."""
        # First: pause
        supervisor._usage_poller._state.utilization_5h = 0.90
        supervisor._poll_and_dispatch()
        assert supervisor.slot_manager.dispatch_paused is True

        # Then: utilization drops
        supervisor._usage_poller._state.utilization_5h = 0.50
        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        assert supervisor.slot_manager.dispatch_paused is False
        events = get_events(supervisor._conn, event_type="dispatch_resumed")
        assert len(events) == 1

    def test_below_threshold_no_dispatch_paused_state(self, supervisor):
        """When utilization is below thresholds, dispatch_paused is False."""
        supervisor._usage_poller._state.utilization_5h = 0.50
        supervisor._usage_poller._state.utilization_7d = 0.60

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        assert supervisor.slot_manager.dispatch_paused is False

    def test_none_utilization_allows_dispatch(self, supervisor):
        """When utilization is None, dispatch proceeds normally."""
        supervisor._usage_poller._state.utilization_5h = None
        supervisor._usage_poller._state.utilization_7d = None

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_called_once()


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
    """Build a config with coder identity (linear_api_key set)."""
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project",
                linear_team="TST",
                base_dir=str(tmp_path / "repo"),
                worktree_prefix="test-project-slot-",
                slots=[1, 2],
            ),
        ],
        linear=LinearConfig(
            api_key="owner-key",
            poll_interval_seconds=10,
            exclude_tags=["Human"],
        ),
        database=DatabaseConfig(),
        identities=IdentitiesConfig(
            coder=CoderIdentity(linear_api_key="coder-key"),
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
            patch("botfarm.supervisor.LinearClient", return_value=mock_coder_client),
        ):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        # Attach the mock so tests can assert on it
        sup._coder_linear = mock_coder_client
        return sup

    def test_init_caches_coder_viewer_id(self, supervisor_with_coder):
        """Supervisor caches coder viewer ID at startup."""
        assert supervisor_with_coder._coder_viewer_id == "coder-user-id-123"
        assert supervisor_with_coder._coder_linear is not None

    def test_init_no_coder_key_skips_caching(self, supervisor):
        """Without coder linear_api_key, no coder client is created."""
        assert supervisor._coder_viewer_id is None
        assert supervisor._coder_linear is None

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
            patch("botfarm.supervisor.LinearClient", return_value=mock_coder_client),
        ):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        assert sup._coder_viewer_id is None
        assert sup._coder_linear is None

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

        supervisor_with_coder._coder_linear.assign_issue.assert_called_once_with(
            issue.identifier, "coder-user-id-123",
        )

    def test_dispatch_continues_on_assignment_failure(self, supervisor_with_coder):
        """Assignment failure does not block dispatch."""
        issue = make_issue()
        slot = supervisor_with_coder.slot_manager.get_slot("test-project", 1)
        poller = supervisor_with_coder._pollers["test-project"]
        supervisor_with_coder._coder_linear.assign_issue.side_effect = Exception("API error")

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
        assert supervisor._coder_linear is None


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
# Worker entry point
# ---------------------------------------------------------------------------


class TestWorkerEntry:
    @patch("botfarm.supervisor.run_pipeline")
    def test_worker_ignores_sigint_and_isolates_process_group(self, mock_pipeline, tmp_path, monkeypatch):
        """_worker_entry must ignore SIGINT and call os.setpgrp() to isolate
        from the parent process group (so group SIGTERM doesn't reach it,
        but the supervisor can still send SIGTERM directly)."""
        import multiprocessing
        db_path = str(tmp_path / "test.db")
        monkeypatch.setenv("BOTFARM_DB_PATH", db_path)
        q = multiprocessing.Queue()

        conn = init_db(db_path, allow_migration=True)
        task_id = insert_task(
            conn,
            ticket_id="TST-SIG",
            title="Test",
            project="proj",
            slot=1,
            status="in_progress",
        )
        conn.commit()
        conn.close()

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.paused = False
        mock_result.no_pr_reason = None
        mock_result.result_text = None
        mock_pipeline.return_value = mock_result

        with patch("botfarm.supervisor.signal.signal") as mock_signal, \
             patch("botfarm.supervisor.os.setpgrp") as mock_setpgrp:
            _worker_entry(
                ticket_id="TST-SIG",
                ticket_title="Test",
                task_id=task_id,
                project_name="proj",
                slot_id=1,
                cwd=str(tmp_path),
                result_queue=q,
                max_turns=None,
            )

            mock_signal.assert_any_call(signal.SIGINT, signal.SIG_IGN)
            mock_setpgrp.assert_called_once()

    @patch("botfarm.supervisor.run_pipeline")
    def test_successful_pipeline_sends_success(self, mock_pipeline, tmp_path, monkeypatch):
        import multiprocessing
        db_path = str(tmp_path / "test.db")
        monkeypatch.setenv("BOTFARM_DB_PATH", db_path)
        q = multiprocessing.Queue()

        conn = init_db(db_path, allow_migration=True)
        task_id = insert_task(
            conn,
            ticket_id="TST-1",
            title="Test",
            project="proj",
            slot=1,
            status="in_progress",
        )
        conn.commit()
        conn.close()

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.paused = False
        mock_result.no_pr_reason = None
        mock_result.result_text = None
        mock_pipeline.return_value = mock_result

        _worker_entry(
            ticket_id="TST-1",
            ticket_title="Test",
            task_id=task_id,
            project_name="proj",
            slot_id=1,
            cwd=str(tmp_path),
            result_queue=q,
            max_turns=None,
        )

        wr = q.get(timeout=1)
        assert isinstance(wr, _WorkerResult)
        assert wr.success is True
        assert wr.project == "proj"
        assert wr.slot_id == 1

    @patch("botfarm.supervisor.run_pipeline")
    def test_failed_pipeline_sends_failure(self, mock_pipeline, tmp_path, monkeypatch):
        import multiprocessing
        db_path = str(tmp_path / "test.db")
        monkeypatch.setenv("BOTFARM_DB_PATH", db_path)
        q = multiprocessing.Queue()

        conn = init_db(db_path, allow_migration=True)
        task_id = insert_task(
            conn,
            ticket_id="TST-2",
            title="Test",
            project="proj",
            slot=1,
            status="in_progress",
        )
        conn.commit()
        conn.close()

        mock_result = MagicMock()
        mock_result.success = False
        mock_result.paused = False
        mock_result.failure_stage = "implement"
        mock_result.failure_reason = "PR not created"
        mock_result.result_text = None
        mock_pipeline.return_value = mock_result

        _worker_entry(
            ticket_id="TST-2",
            ticket_title="Test",
            task_id=task_id,
            project_name="proj",
            slot_id=1,
            cwd=str(tmp_path),
            result_queue=q,
            max_turns=None,
        )

        wr = q.get(timeout=1)
        assert isinstance(wr, _WorkerResult)
        assert wr.success is False
        assert wr.failure_stage == "implement"

    @patch("botfarm.supervisor.run_pipeline")
    def test_exception_sends_failure(self, mock_pipeline, tmp_path, monkeypatch):
        import multiprocessing
        db_path = str(tmp_path / "test.db")
        monkeypatch.setenv("BOTFARM_DB_PATH", db_path)
        q = multiprocessing.Queue()

        conn = init_db(db_path, allow_migration=True)
        task_id = insert_task(
            conn,
            ticket_id="TST-3",
            title="Test",
            project="proj",
            slot=1,
            status="in_progress",
        )
        conn.commit()
        conn.close()

        mock_pipeline.side_effect = RuntimeError("crash")

        _worker_entry(
            ticket_id="TST-3",
            ticket_title="Test",
            task_id=task_id,
            project_name="proj",
            slot_id=1,
            cwd=str(tmp_path),
            result_queue=q,
            max_turns=None,
        )

        wr = q.get(timeout=1)
        assert isinstance(wr, _WorkerResult)
        assert wr.success is False
        assert wr.failure_stage == "worker_entry"


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------


class TestSetupLogging:
    def test_creates_log_dir(self, tmp_path):
        log_dir = tmp_path / "logs"
        setup_logging(log_dir, console=False)
        assert log_dir.exists()

    def test_creates_log_file(self, tmp_path):
        log_dir = tmp_path / "logs"
        setup_logging(log_dir, console=False)
        log_file = log_dir / "supervisor.log"
        assert log_file.exists()

    def test_file_handler_added(self, tmp_path):
        log_dir = tmp_path / "logs"
        # Reset handlers to isolate test
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            setup_logging(log_dir, console=False)
            assert any(
                isinstance(h, logging.handlers.RotatingFileHandler)
                for h in root.handlers
            )
        finally:
            root.handlers = original_handlers

    def test_rotating_handler_params(self, tmp_path):
        log_dir = tmp_path / "logs"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            setup_logging(
                log_dir, console=False, max_bytes=5_000_000, backup_count=3,
            )
            rfh = [
                h for h in root.handlers
                if isinstance(h, logging.handlers.RotatingFileHandler)
            ]
            assert len(rfh) == 1
            assert rfh[0].maxBytes == 5_000_000
            assert rfh[0].backupCount == 3
        finally:
            root.handlers = original_handlers

    def test_console_handler_when_enabled(self, tmp_path):
        log_dir = tmp_path / "logs"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            setup_logging(log_dir, console=True)
            assert any(
                isinstance(h, logging.StreamHandler)
                and not isinstance(h, logging.FileHandler)
                for h in root.handlers
            )
        finally:
            root.handlers = original_handlers

    def test_suppresses_httpx_httpcore_logging(self, tmp_path):
        log_dir = tmp_path / "logs"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            setup_logging(log_dir, console=False)
            assert logging.getLogger("httpx").level == logging.WARNING
            assert logging.getLogger("httpcore").level == logging.WARNING
        finally:
            root.handlers = original_handlers


# ---------------------------------------------------------------------------
# _setup_worker_logging
# ---------------------------------------------------------------------------


class TestSetupWorkerLogging:
    def test_creates_worker_log_file(self, tmp_path):
        log_dir = tmp_path / "logs" / "SMA-99"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            _setup_worker_logging(log_dir)
            log_file = log_dir / "worker.log"
            assert log_file.exists()
        finally:
            root.handlers = original_handlers

    def test_adds_rotating_file_handler(self, tmp_path):
        log_dir = tmp_path / "logs" / "SMA-99"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            _setup_worker_logging(log_dir)
            assert any(
                isinstance(h, logging.handlers.RotatingFileHandler)
                for h in root.handlers
            )
        finally:
            root.handlers = original_handlers

    def test_rotating_handler_params(self, tmp_path):
        log_dir = tmp_path / "logs" / "SMA-99"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            _setup_worker_logging(
                log_dir, max_bytes=2_000_000, backup_count=2,
            )
            rfh = [
                h for h in root.handlers
                if isinstance(h, logging.handlers.RotatingFileHandler)
            ]
            assert len(rfh) == 1
            assert rfh[0].maxBytes == 2_000_000
            assert rfh[0].backupCount == 2
        finally:
            root.handlers = original_handlers

    def test_creates_parent_dirs(self, tmp_path):
        log_dir = tmp_path / "a" / "b" / "c"
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        try:
            root.handlers = []
            _setup_worker_logging(log_dir)
            assert log_dir.exists()
        finally:
            root.handlers = original_handlers


# ---------------------------------------------------------------------------
# _cleanup_old_ticket_logs
# ---------------------------------------------------------------------------


class TestCleanupOldTicketLogs:
    """Tests for Supervisor._cleanup_old_ticket_logs."""

    def test_removes_old_directories(self, supervisor, tmp_path):
        """Directories older than retention period are removed."""
        log_dir = supervisor._log_dir
        log_dir.mkdir(parents=True, exist_ok=True)

        old_dir = log_dir / "SMA-OLD"
        old_dir.mkdir()
        # Set mtime to 60 days ago
        old_time = time.time() - 60 * 86400
        os.utime(str(old_dir), (old_time, old_time))

        supervisor._config.logging.ticket_log_retention_days = 30
        supervisor._last_log_cleanup = 0.0
        supervisor._cleanup_old_ticket_logs()

        assert not old_dir.exists()

    def test_preserves_recent_directories(self, supervisor, tmp_path):
        """Directories newer than retention period are kept."""
        log_dir = supervisor._log_dir
        log_dir.mkdir(parents=True, exist_ok=True)

        recent_dir = log_dir / "SMA-RECENT"
        recent_dir.mkdir()

        supervisor._config.logging.ticket_log_retention_days = 30
        supervisor._last_log_cleanup = 0.0
        supervisor._cleanup_old_ticket_logs()

        assert recent_dir.exists()

    def test_skips_active_ticket_dirs(self, supervisor, tmp_path):
        """Directories for active tickets are never removed."""
        log_dir = supervisor._log_dir
        log_dir.mkdir(parents=True, exist_ok=True)

        # Assign a ticket to a slot
        supervisor._slot_manager.assign_ticket(
            "test-project", 1,
            ticket_id="SMA-ACTIVE",
            ticket_title="SMA-ACTIVE title",
            branch="branch-active",
        )

        active_dir = log_dir / "SMA-ACTIVE"
        active_dir.mkdir()
        old_time = time.time() - 60 * 86400
        os.utime(str(active_dir), (old_time, old_time))

        supervisor._config.logging.ticket_log_retention_days = 30
        supervisor._last_log_cleanup = 0.0
        supervisor._cleanup_old_ticket_logs()

        assert active_dir.exists()

    def test_skips_non_directories(self, supervisor, tmp_path):
        """Regular files (e.g. supervisor.log) are not removed."""
        log_dir = supervisor._log_dir
        log_dir.mkdir(parents=True, exist_ok=True)

        log_file = log_dir / "supervisor.log"
        log_file.write_text("test")
        old_time = time.time() - 60 * 86400
        os.utime(str(log_file), (old_time, old_time))

        supervisor._config.logging.ticket_log_retention_days = 30
        supervisor._last_log_cleanup = 0.0
        supervisor._cleanup_old_ticket_logs()

        assert log_file.exists()

    def test_throttled_by_interval(self, supervisor, tmp_path):
        """Cleanup runs at most once per hour."""
        log_dir = supervisor._log_dir
        log_dir.mkdir(parents=True, exist_ok=True)

        old_dir = log_dir / "SMA-THROTTLE"
        old_dir.mkdir()
        old_time = time.time() - 60 * 86400
        os.utime(str(old_dir), (old_time, old_time))

        supervisor._config.logging.ticket_log_retention_days = 30
        # Pretend cleanup just ran
        supervisor._last_log_cleanup = time.time()
        supervisor._cleanup_old_ticket_logs()

        # Dir should still exist because cleanup was throttled
        assert old_dir.exists()


# ---------------------------------------------------------------------------
# _check_limit_hit
# ---------------------------------------------------------------------------


class TestCheckLimitHit:
    def test_calledprocesserror_alone_not_detected(self):
        """CalledProcessError without limit-specific text is not a limit hit."""
        result = PipelineResult(
            ticket_id="T-1", success=False, stages_completed=[],
            failure_stage="implement",
            failure_reason="CalledProcessError: exit code 1",
        )
        assert _check_limit_hit(result) is False

    def test_max_tokens_exceeded_detected(self):
        result = PipelineResult(
            ticket_id="T-1", success=False, stages_completed=[],
            failure_stage="implement",
            failure_reason="CalledProcessError: max_tokens_exceeded",
        )
        assert _check_limit_hit(result) is True

    def test_rate_limit_detected(self):
        result = PipelineResult(
            ticket_id="T-1", success=False, stages_completed=[],
            failure_stage="implement",
            failure_reason="Claude was killed due to rate limit",
        )
        assert _check_limit_hit(result) is True

    def test_usage_limit_detected(self):
        result = PipelineResult(
            ticket_id="T-1", success=False, stages_completed=[],
            failure_stage="review",
            failure_reason="Hit usage limit for this period",
        )
        assert _check_limit_hit(result) is True

    def test_normal_failure_not_detected(self):
        result = PipelineResult(
            ticket_id="T-1", success=False, stages_completed=[],
            failure_stage="implement",
            failure_reason="PR URL not found in output",
        )
        assert _check_limit_hit(result) is False

    def test_none_reason_not_detected(self):
        result = PipelineResult(
            ticket_id="T-1", success=False, stages_completed=[],
            failure_stage="implement",
            failure_reason=None,
        )
        assert _check_limit_hit(result) is False


# ---------------------------------------------------------------------------
# Limit hit handling
# ---------------------------------------------------------------------------


class TestLimitHitHandling:
    def test_limit_hit_pauses_slot(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        # Create a task in DB so _find_task_id works
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="CalledProcessError",
            limit_hit=True,
        ))

        supervisor._reconcile_workers()

        assert sm.get_slot("test-project", 1).status == "paused_limit"
        assert sm.get_slot("test-project", 1).interrupted_by_limit is True

    def test_limit_hit_records_event(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="CalledProcessError",
            limit_hit=True,
        ))

        supervisor._reconcile_workers()

        events = get_events(supervisor._conn, event_type="limit_hit")
        assert len(events) == 1
        assert "implement" in events[0]["detail"]

    def test_limit_hit_increments_counter(self, supervisor):
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

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="CalledProcessError",
            limit_hit=True,
        ))

        supervisor._reconcile_workers()

        from botfarm.db import get_task
        task = get_task(supervisor._conn, task_id)
        assert task["limit_interruptions"] == 1

    def test_limit_hit_computes_resume_after(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Set resets_at on usage state
        supervisor._usage_poller._state.resets_at_5h = "2026-02-12T22:00:00Z"

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="CalledProcessError",
            limit_hit=True,
        ))

        # Mock force_poll to preserve pre-set state (avoid real API call)
        with patch.object(supervisor._usage_poller, "force_poll"):
            supervisor._reconcile_workers()

        slot = sm.get_slot("test-project", 1)
        assert slot.resume_after is not None
        # Should be 2 minutes after resets_at
        assert "22:02" in slot.resume_after

    def test_reconcile_force_polls_before_handle_limit_hit(self, supervisor):
        """force_poll is called before _handle_limit_hit so resume_after uses fresh data."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement", failure_reason="rate limit hit",
            limit_hit=True,
        ))

        with patch.object(
            supervisor._usage_poller, "force_poll",
            wraps=supervisor._usage_poller.force_poll,
        ) as mock_fp:
            supervisor._reconcile_workers()
            mock_fp.assert_called_once_with(supervisor._conn)

    def test_usage_api_detects_limit_when_string_match_misses(self, supervisor):
        """Worker failure without limit strings is caught by usage API check."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Usage API indicates limits are exceeded
        supervisor._usage_poller._state.utilization_5h = 0.95
        supervisor._usage_poller._state.resets_at_5h = "2026-02-24T22:00:00Z"

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement",
            failure_reason="Unexpected process exit code 1",
            limit_hit=False,  # String matching did NOT detect it
        ))

        # Mock force_poll to preserve pre-set state (avoid real API call)
        with patch.object(supervisor._usage_poller, "force_poll"):
            supervisor._reconcile_workers()

        # Should be treated as limit hit, not a normal failure
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "paused_limit"
        assert slot.interrupted_by_limit is True

    def test_normal_failure_when_usage_api_shows_low_utilization(self, supervisor):
        """Worker failure is treated as normal when usage is below thresholds."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement",
            failure_reason="PR URL not found in output",
            limit_hit=False,
        ))

        # Mock _check_usage_api_for_limit to return False (low utilization)
        # instead of making a real API call that could return high usage.
        with patch.object(supervisor, "_check_usage_api_for_limit", return_value=False):
            supervisor._reconcile_workers()

        # Should be treated as a normal failure
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "failed"


# ---------------------------------------------------------------------------
# Handle paused slots / resume
# ---------------------------------------------------------------------------


class TestHandlePausedSlots:
    def test_resume_when_time_passed_and_usage_low(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement", session_id="sess-1")
        # Past resume_after time
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2020-01-01T00:00:00+00:00",
        )

        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Usage is low
        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        with (
            patch.object(supervisor._usage_poller, "force_poll"),
            patch("botfarm.supervisor.multiprocessing.Process") as MockProc,
        ):
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

            mock_proc.start.assert_called_once()

        assert sm.get_slot("test-project", 1).status == "busy"

    def test_no_resume_when_time_not_passed(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        # Future resume_after time
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2099-01-01T00:00:00+00:00",
        )

        supervisor._handle_paused_slots()

        assert sm.get_slot("test-project", 1).status == "paused_limit"

    def test_no_resume_when_usage_still_high(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        # Past resume_after time
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2020-01-01T00:00:00+00:00",
        )

        # Usage still high
        supervisor._usage_poller._state.utilization_5h = 0.95

        # Mock force_poll to preserve pre-set state (avoid real API call)
        with patch.object(supervisor._usage_poller, "force_poll"):
            supervisor._handle_paused_slots()

        assert sm.get_slot("test-project", 1).status == "paused_limit"

    def test_resume_records_limit_resumed_event(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="review")
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2020-01-01T00:00:00+00:00",
        )

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        with (
            patch.object(supervisor._usage_poller, "force_poll"),
            patch("botfarm.supervisor.multiprocessing.Process") as MockProc,
        ):
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

        events = get_events(supervisor._conn, event_type="limit_resumed")
        assert len(events) == 1
        assert "review" in events[0]["detail"]

    def test_resume_passes_stage_and_session_id(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="review", session_id="sess-abc")
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2020-01-01T00:00:00+00:00",
        )

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        with (
            patch.object(supervisor._usage_poller, "force_poll"),
            patch("botfarm.supervisor.multiprocessing.Process") as MockProc,
        ):
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

            # Check the kwargs passed to Process
            call_kwargs = MockProc.call_args.kwargs["kwargs"]
            assert call_kwargs["resume_from_stage"] == "review"
            assert call_kwargs["resume_session_id"] == "sess-abc"

    def test_resume_with_no_resume_after_resumes_immediately(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        sm.mark_paused_limit("test-project", 1)  # No resume_after

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        with (
            patch.object(supervisor._usage_poller, "force_poll"),
            patch("botfarm.supervisor.multiprocessing.Process") as MockProc,
        ):
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

            mock_proc.start.assert_called_once()

        assert sm.get_slot("test-project", 1).status == "busy"

    def test_handle_paused_slots_force_polls_before_threshold_check(self, supervisor):
        """force_poll is called so the resume decision uses fresh usage data."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2020-01-01T00:00:00+00:00",
        )

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        with (
            patch.object(supervisor._usage_poller, "force_poll") as mock_fp,
            patch("botfarm.supervisor.multiprocessing.Process") as MockProc,
        ):
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

            mock_fp.assert_called_once_with(supervisor._conn)

    def test_resume_immediately_when_limits_disabled(self, supervisor):
        """Disabling usage limits resumes paused slots even if resume_after is in the future."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement", session_id="sess-1")
        # Far-future resume_after — would normally block resume
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2099-01-01T00:00:00+00:00",
        )

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Usage is high, but limits are disabled
        supervisor._usage_poller._state.utilization_5h = 0.95
        supervisor._usage_poller._state.utilization_7d = 0.95
        supervisor._config.usage_limits.enabled = False

        with (
            patch.object(supervisor._usage_poller, "force_poll") as mock_fp,
            patch("botfarm.supervisor.multiprocessing.Process") as MockProc,
        ):
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

            # Should resume without polling or checking thresholds
            mock_proc.start.assert_called_once()
            mock_fp.assert_not_called()

        assert sm.get_slot("test-project", 1).status == "busy"


# ---------------------------------------------------------------------------
# Tick includes paused slots phase
# ---------------------------------------------------------------------------


class TestTickWithPausedSlots:
    def test_tick_calls_handle_paused_slots(self, supervisor):
        with (
            patch.object(supervisor, "_reconcile_workers"),
            patch.object(supervisor, "_handle_finished_slots"),
            patch.object(supervisor, "_handle_paused_slots") as mock_paused,
            patch.object(supervisor, "_poll_usage"),
            patch.object(supervisor, "_poll_and_dispatch"),
        ):
            supervisor._tick()
            mock_paused.assert_called_once()


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
                    linear_team="ALPHA",
                    base_dir=str(tmp_path / "alpha"),
                    worktree_prefix="alpha-slot-",
                    slots=[1],
                ),
                ProjectConfig(
                    name="beta",
                    linear_team="BETA",
                    base_dir=str(tmp_path / "beta"),
                    worktree_prefix="beta-slot-",
                    slots=[2],
                ),
            ],
            linear=LinearConfig(
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


# ---------------------------------------------------------------------------
# Timeout enforcement
# ---------------------------------------------------------------------------


class TestCheckTimeouts:
    def _make_busy_slot(self, supervisor, stage_started_at, stage="implement"):
        """Helper: assign a ticket, set stage and stage_started_at."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage=stage)
        # Override stage_started_at to a controlled value
        slot = sm.get_slot("test-project", 1)
        slot.stage_started_at = stage_started_at
        slot.pid = 99999  # fake PID
        return slot

    def _run_full_timeout(self, supervisor):
        """Run both phases of timeout: SIGTERM then SIGKILL after grace."""
        with patch("botfarm.supervisor.os.kill") as mock_kill:
            supervisor._check_timeouts()  # phase 1: sends SIGTERM

        # Phase 2: simulate grace period elapsed
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        # Set sigterm_sent_at far in the past so grace period has elapsed
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.supervisor.os.kill", side_effect=ProcessLookupError):
            supervisor._check_timeouts()  # phase 2: escalates to SIGKILL

    def test_no_timeout_when_within_limit(self, supervisor):
        """Worker within time limit is not killed."""
        now = datetime.now(timezone.utc)
        # Started 10 minutes ago, limit is 120 minutes
        started = (now - timedelta(minutes=10)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        with patch("botfarm.supervisor.os.kill") as mock_kill:
            supervisor._check_timeouts()
            mock_kill.assert_not_called()

        assert supervisor.slot_manager.get_slot("test-project", 1).status == "busy"

    def test_timeout_sends_sigterm_first(self, supervisor):
        """First call sends SIGTERM and records sigterm_sent_at."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        with patch("botfarm.supervisor.os.kill") as mock_kill:
            supervisor._check_timeouts()
            mock_kill.assert_called_once_with(99999, signal.SIGTERM)

        slot = supervisor.slot_manager.get_slot("test-project", 1)
        assert slot.sigterm_sent_at is not None
        # Slot is still busy after SIGTERM — not yet marked failed
        assert slot.status == "busy"

    def test_timeout_event_recorded_at_sigterm_time(self, supervisor):
        """Timeout event is persisted at SIGTERM time (phase 1), not at SIGKILL.

        This ensures the event survives supervisor crashes between phases.
        """
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Phase 1 only: send SIGTERM — do NOT escalate to SIGKILL
        with patch("botfarm.supervisor.os.kill"):
            supervisor._check_timeouts()

        # Event should already be recorded even though slot is still busy
        events = get_events(supervisor._conn, event_type="timeout")
        assert len(events) == 1
        assert "stage=implement" in events[0]["detail"]
        assert "elapsed=" in events[0]["detail"]
        assert "limit=" in events[0]["detail"]

        # Slot is still busy — only SIGTERM sent, not yet marked failed
        assert supervisor.slot_manager.get_slot("test-project", 1).status == "busy"

    def test_timeout_escalates_to_sigkill_after_grace(self, supervisor):
        """After grace period, SIGKILL is sent and slot marked failed."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        self._run_full_timeout(supervisor)

        assert supervisor.slot_manager.get_slot("test-project", 1).status == "failed"

    def test_timeout_no_escalate_within_grace(self, supervisor):
        """Within grace period, SIGKILL is not sent."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        # Phase 1: send SIGTERM
        with patch("botfarm.supervisor.os.kill"):
            supervisor._check_timeouts()

        # Phase 2: sigterm_sent_at is very recent (just set), grace=10s not elapsed
        with patch("botfarm.supervisor.os.kill") as mock_kill:
            supervisor._check_timeouts()
            mock_kill.assert_not_called()

        # Still busy — not yet escalated
        assert supervisor.slot_manager.get_slot("test-project", 1).status == "busy"

    def test_timeout_records_event(self, supervisor):
        """Timeout records a task_events(timeout) entry."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        self._run_full_timeout(supervisor)

        events = get_events(supervisor._conn, event_type="timeout")
        assert len(events) == 1
        assert "stage=implement" in events[0]["detail"]
        assert "elapsed=" in events[0]["detail"]
        assert "limit=" in events[0]["detail"]

    def test_timeout_updates_task_status_to_failed(self, supervisor):
        """Timeout marks the task as failed with a descriptive reason."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        self._run_full_timeout(supervisor)

        task = get_task(supervisor._conn, task_id)
        assert task["status"] == "failed"
        assert "timeout in stage implement" in task["failure_reason"]

    def test_timeout_uses_stage_specific_limit(self, supervisor):
        """Review stage uses its own timeout (30 min default)."""
        now = datetime.now(timezone.utc)
        # 35 minutes — over review limit (30) but under implement limit (120)
        started = (now - timedelta(minutes=35)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started, stage="review")

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        self._run_full_timeout(supervisor)

        assert supervisor.slot_manager.get_slot("test-project", 1).status == "failed"

    def test_no_timeout_for_stage_without_config(self, supervisor):
        """Stages not in timeout_minutes config (e.g. pr_checks, merge) are skipped."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(hours=10)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started, stage="pr_checks")

        with patch("botfarm.supervisor.os.kill") as mock_kill:
            supervisor._check_timeouts()
            mock_kill.assert_not_called()

        assert supervisor.slot_manager.get_slot("test-project", 1).status == "busy"

    def test_no_timeout_without_stage_started_at(self, supervisor):
        """If stage_started_at is not set, skip timeout check."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.stage = "implement"
        slot.stage_started_at = None
        slot.pid = 99999

        with patch("botfarm.supervisor.os.kill") as mock_kill:
            supervisor._check_timeouts()
            mock_kill.assert_not_called()

    def test_tick_calls_check_timeouts(self, supervisor):
        """_check_timeouts is called as part of _tick."""
        with (
            patch.object(supervisor, "_reconcile_workers"),
            patch.object(supervisor, "_check_timeouts") as mock_timeouts,
            patch.object(supervisor, "_handle_finished_slots"),
            patch.object(supervisor, "_handle_paused_slots"),
            patch.object(supervisor, "_poll_usage"),
            patch.object(supervisor, "_poll_and_dispatch"),
        ):
            supervisor._tick()
            mock_timeouts.assert_called_once()

    def test_timeout_with_custom_config(self, tmp_path, monkeypatch):
        """Custom timeout_minutes values are respected."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = make_config(tmp_path)
        config.agents.timeout_minutes = {"implement": 10, "review": 5, "fix": 8}
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        now = datetime.now(timezone.utc)
        # 15 minutes — over custom 10-minute limit
        slot.stage_started_at = (now - timedelta(minutes=15)).isoformat()
        slot.pid = 99999

        insert_task(
            sup._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        sup._conn.commit()

        # Phase 1: SIGTERM
        with patch("botfarm.supervisor.os.kill"):
            sup._check_timeouts()

        # Phase 2: set sigterm_sent_at to past for grace period expiry
        slot = sm.get_slot("test-project", 1)
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.supervisor.os.kill", side_effect=ProcessLookupError):
            sup._check_timeouts()

        assert sm.get_slot("test-project", 1).status == "failed"
        sup._conn.close()

    def test_reconcile_skips_sigterm_slots(self, supervisor):
        """Reconcile skips slots with sigterm_sent_at to preserve timeout reason."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.slots._is_pid_alive", return_value=False):
            messages = sm.reconcile()

        # Should not be reconciled — timeout system owns it
        assert messages == []
        assert slot.status == "busy"

    def test_timeout_override_uses_label_timeout(self, tmp_path, monkeypatch):
        """Tickets with label-based timeout overrides use the shorter timeout."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = make_config(tmp_path)
        config.agents.timeout_overrides = {"Investigation": {"implement": 20}}
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
            ticket_labels=["Investigation"],
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        now = datetime.now(timezone.utc)
        # 25 minutes — over the Investigation override (20) but under default (120)
        slot.stage_started_at = (now - timedelta(minutes=25)).isoformat()
        slot.pid = 99999

        insert_task(
            sup._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        sup._conn.commit()

        with patch("botfarm.supervisor.os.kill") as mock_kill:
            sup._check_timeouts()
            # Should have sent SIGTERM because 25 > 20 (override limit)
            mock_kill.assert_called_once_with(99999, signal.SIGTERM)

        sup._conn.close()

    def test_timeout_override_no_match_uses_default(self, tmp_path, monkeypatch):
        """Tickets without matching labels use the default timeout."""
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = make_config(tmp_path)
        config.agents.timeout_overrides = {"Investigation": {"implement": 20}}
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
            ticket_labels=["Bug"],
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        now = datetime.now(timezone.utc)
        # 25 minutes — under default (120), over Investigation override (20)
        slot.stage_started_at = (now - timedelta(minutes=25)).isoformat()
        slot.pid = 99999

        with patch("botfarm.supervisor.os.kill") as mock_kill:
            sup._check_timeouts()
            # Should NOT timeout — default is 120 and we're at 25 minutes
            mock_kill.assert_not_called()

        sup._conn.close()


# ---------------------------------------------------------------------------
# Crash recovery on startup
# ---------------------------------------------------------------------------


class TestRecoverOnStartup:
    """Tests for _recover_on_startup — the crash recovery logic."""

    def test_dead_worker_no_pr_marked_failed(self, supervisor):
        """Dead worker with no PR is marked failed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
        ):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "failed"
        events = get_events(supervisor._conn, event_type="recovery_failed")
        assert len(events) == 1
        assert "no_pr" in events[0]["detail"]

    def test_dead_worker_pr_merged_marked_completed(self, supervisor):
        """Dead worker whose PR was merged is marked completed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")),
        ):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"
        events = get_events(supervisor._conn, event_type="recovery_completed")
        assert len(events) == 1
        assert "pr_merged" in events[0]["detail"]

    def test_dead_worker_pr_open_resumes_from_review(self, supervisor):
        """Dead worker whose PR is open (no stages completed) resumes from review."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch.object(supervisor, "_resume_recovered_worker") as mock_resume,
        ):
            supervisor._recover_on_startup()

        mock_resume.assert_called_once_with(slot, resume_from_stage="review")
        assert sm.get_slot("test-project", 1).status == "busy"
        events = get_events(supervisor._conn, event_type="recovery_resumed")
        assert len(events) == 0  # resume event is logged inside _resume_recovered_worker (mocked)

    def test_dead_worker_pr_closed_marked_failed(self, supervisor):
        """Dead worker whose PR was closed without merging is marked failed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("closed", "https://github.com/test/repo/pull/1")),
        ):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "failed"
        events = get_events(supervisor._conn, event_type="recovery_failed")
        assert len(events) == 1
        assert "pr_closed" in events[0]["detail"]

    def test_alive_worker_reattached(self, supervisor):
        """Alive worker is re-attached — slot stays busy."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        with patch("botfarm.supervisor._is_pid_alive", return_value=True):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "busy"
        events = get_events(supervisor._conn, event_type="worker_reattached")
        assert len(events) == 1
        assert "99999" in events[0]["detail"]

    def test_timed_out_slot_recovered_as_failed(self, supervisor):
        """Slot with sigterm_sent_at is recovered as failed (timeout)."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "failed"
        events = get_events(supervisor._conn, event_type="recovery_timeout")
        assert len(events) == 1
        assert "implement" in events[0]["detail"]

    def test_timed_out_slot_alive_pid_gets_sigkill(self, supervisor):
        """Timed-out slot with alive PID sends SIGKILL before marking failed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=True),
            patch("os.kill") as mock_kill,
        ):
            supervisor._recover_on_startup()

        import signal as sig
        mock_kill.assert_called_once_with(99999, sig.SIGKILL)
        assert sm.get_slot("test-project", 1).status == "failed"

    def test_free_slots_not_affected(self, supervisor):
        """Free slots are not touched during recovery."""
        sm = supervisor.slot_manager
        assert sm.get_slot("test-project", 1).status == "free"
        assert sm.get_slot("test-project", 2).status == "free"

        supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "free"
        assert sm.get_slot("test-project", 2).status == "free"
        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert len(events) == 0

    def test_paused_slots_not_affected(self, supervisor):
        """Paused slots are left as-is for _handle_paused_slots to handle."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2099-01-01T00:00:00+00:00",
        )

        supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "paused_limit"

    def test_startup_recovery_event_logged(self, supervisor):
        """When slots are recovered, a startup_recovery event is logged."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
        ):
            supervisor._recover_on_startup()

        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert len(events) == 1
        assert "slots_processed=1" in events[0]["detail"]

    def test_multiple_slots_recovered(self, supervisor):
        """Multiple busy slots with dead workers are all recovered."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test 1", branch="b1",
        )
        sm.assign_ticket(
            "test-project", 2,
            ticket_id="TST-2", ticket_title="Test 2", branch="b2",
        )
        sm.get_slot("test-project", 1).pid = 99998
        sm.get_slot("test-project", 2).pid = 99999

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
        ):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "failed"
        assert sm.get_slot("test-project", 2).status == "failed"
        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert "slots_processed=2" in events[0]["detail"]

    def test_run_calls_recover_on_startup(self, supervisor, tmp_path):
        """run() calls _recover_on_startup before entering the main loop."""
        supervisor._shutdown_requested = True

        with (
            patch.object(supervisor, "_recover_on_startup") as mock_recover,
            patch.object(supervisor, "install_signal_handlers"),
            patch("botfarm.supervisor.run_preflight_checks", return_value=[]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=True),
        ):
            supervisor.run()

        mock_recover.assert_called_once()

    # --- Stage-aware recovery tests ---

    def test_dead_worker_with_stages_resumes_from_next_stage(self, supervisor):
        """Dead worker with stages_completed resumes from the next stage."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.stages_completed = ["implement"]

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch.object(supervisor, "_resume_recovered_worker") as mock_resume,
        ):
            supervisor._recover_on_startup()

        mock_resume.assert_called_once_with(slot, resume_from_stage="review")

    def test_dead_worker_with_review_completed_resumes_fix(self, supervisor):
        """Dead worker that completed implement+review resumes from fix."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.stages_completed = ["implement", "review"]

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch.object(supervisor, "_resume_recovered_worker") as mock_resume,
        ):
            supervisor._recover_on_startup()

        mock_resume.assert_called_once_with(slot, resume_from_stage="fix")

    def test_dead_worker_with_pr_checks_completed_resumes_merge(self, supervisor):
        """Dead worker that completed through pr_checks resumes from merge."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.stages_completed = ["implement", "review", "fix", "pr_checks"]

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch.object(supervisor, "_resume_recovered_worker") as mock_resume,
        ):
            supervisor._recover_on_startup()

        mock_resume.assert_called_once_with(slot, resume_from_stage="merge")

    def test_dead_worker_during_merge_pr_merged_marks_completed(self, supervisor):
        """Dead worker during merge stage with merged PR is marked completed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.stages_completed = ["implement", "review", "fix", "pr_checks"]

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")),
        ):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"
        events = get_events(supervisor._conn, event_type="recovery_completed")
        assert len(events) == 1
        assert "pr_merged_during_merge" in events[0]["detail"]

    def test_dead_worker_all_stages_completed_marks_completed(self, supervisor):
        """Dead worker with all stages completed is marked completed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.stages_completed = ["implement", "review", "fix", "pr_checks", "merge"]

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")),
        ):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"
        events = get_events(supervisor._conn, event_type="recovery_completed")
        assert len(events) == 1
        assert "all_stages_completed" in events[0]["detail"]

    def test_dead_worker_stages_completed_resumes_pr_checks(self, supervisor):
        """Dead worker that completed through fix resumes from pr_checks."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999
        slot.stages_completed = ["implement", "review", "fix"]

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch.object(supervisor, "_resume_recovered_worker") as mock_resume,
        ):
            supervisor._recover_on_startup()

        mock_resume.assert_called_once_with(slot, resume_from_stage="pr_checks")

    # --- Failed / completed_pending_cleanup recovery tests ---

    def test_failed_slot_cleaned_up_during_recovery(self, supervisor):
        """Failed slot is cleaned up immediately during startup recovery."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="failed",
        )
        supervisor._conn.commit()

        with patch.object(supervisor, "_handle_failed_slot") as mock_handle:
            supervisor._recover_on_startup()

        mock_handle.assert_called_once()
        assert mock_handle.call_args[0][0].ticket_id == "TST-1"
        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert len(events) == 1
        assert "slots_processed=1" in events[0]["detail"]

    def test_completed_pending_cleanup_slot_cleaned_up_during_recovery(self, supervisor):
        """Completed slot pending cleanup is cleaned up during startup recovery."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with patch.object(supervisor, "_handle_completed_slot") as mock_handle:
            supervisor._recover_on_startup()

        mock_handle.assert_called_once()
        assert mock_handle.call_args[0][0].ticket_id == "TST-1"
        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert len(events) == 1

    def test_failed_slot_cleanup_error_does_not_crash_recovery(self, supervisor):
        """If _handle_failed_slot raises, recovery continues without crashing."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        with patch.object(
            supervisor, "_handle_failed_slot",
            side_effect=RuntimeError("Linear API down"),
        ):
            # Should not raise — the error is caught and logged
            supervisor._recover_on_startup()

        # Slot is still failed (cleanup didn't succeed), but recovery didn't crash
        assert sm.get_slot("test-project", 1).status == "failed"
        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert len(events) == 1
        assert "slots_processed=1" in events[0]["detail"]

    def test_completed_pending_cleanup_error_does_not_crash_recovery(self, supervisor):
        """If _handle_completed_slot raises, recovery continues without crashing."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        with patch.object(
            supervisor, "_handle_completed_slot",
            side_effect=RuntimeError("Linear API down"),
        ):
            supervisor._recover_on_startup()

        # Slot is still completed_pending_cleanup (cleanup didn't succeed)
        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"
        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert len(events) == 1

    def test_mixed_recovery_counts_all_slot_types(self, supervisor):
        """Recovery processes busy, failed, and completed_pending_cleanup slots."""
        sm = supervisor.slot_manager
        # Slot 1: failed
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test 1", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")
        # Slot 2: completed_pending_cleanup
        sm.assign_ticket(
            "test-project", 2,
            ticket_id="TST-2", ticket_title="Test 2", branch="b2",
        )
        sm.mark_completed("test-project", 2)

        with (
            patch.object(supervisor, "_handle_failed_slot") as mock_failed,
            patch.object(supervisor, "_handle_completed_slot") as mock_completed,
        ):
            supervisor._recover_on_startup()

        mock_failed.assert_called_once()
        mock_completed.assert_called_once()
        events = get_events(supervisor._conn, event_type="startup_recovery")
        assert "slots_processed=2" in events[0]["detail"]

    def test_update_in_progress_pause_cleared_on_startup(self, supervisor):
        """Stale update_in_progress pause is cleared during startup recovery."""
        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "update_in_progress")

        supervisor._recover_on_startup()

        assert sm.dispatch_paused is False
        assert sm.dispatch_pause_reason is None
        events = get_events(supervisor._conn, event_type="dispatch_resumed")
        assert len(events) == 1
        assert "update_in_progress" in events[0]["detail"]

    def test_manual_pause_not_cleared_on_startup(self, supervisor):
        """Manual pause should NOT be cleared during startup recovery."""
        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "manual_pause")

        supervisor._recover_on_startup()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "manual_pause"


class TestExternallyDoneTicket:
    """Tests for detecting tickets moved to Done/Cancelled externally."""

    def test_busy_slot_with_externally_done_ticket_marked_completed(self, supervisor):
        """A busy slot whose ticket was moved to Done externally is marked completed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = True

        with patch("botfarm.supervisor._is_pid_alive", return_value=False):
            supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"
        events = get_events(supervisor._conn, event_type="recovery_completed")
        assert len(events) == 1
        assert "ticket_externally_done" in events[0]["detail"]

    def test_busy_slot_alive_pid_killed_when_ticket_done(self, supervisor):
        """An alive worker is killed when the ticket is externally done."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = True

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=True),
            patch("os.kill") as mock_kill,
        ):
            supervisor._recover_on_startup()

        mock_kill.assert_called_once_with(99999, signal.SIGTERM)
        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"

    def test_paused_slot_with_externally_done_ticket_marked_completed(self, supervisor):
        """A paused slot whose ticket was moved to Done externally is marked completed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_paused_limit("test-project", 1, resume_after="2099-01-01T00:00:00Z")

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = True

        supervisor._recover_on_startup()

        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"
        events = get_events(supervisor._conn, event_type="recovery_completed")
        assert len(events) == 1
        assert "ticket_externally_done" in events[0]["detail"]

    def test_non_terminal_ticket_proceeds_with_normal_recovery(self, supervisor):
        """When ticket is not terminal, normal recovery path is used."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = False

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
        ):
            supervisor._recover_on_startup()

        # Normal recovery: no PR found → failed
        assert sm.get_slot("test-project", 1).status == "failed"

    def test_failed_slot_skips_move_when_ticket_terminal(self, supervisor):
        """_handle_failed_slot doesn't move a ticket that's already Done."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="failed",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = True

        slot = sm.get_slot("test-project", 1)
        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_failed_slot(slot)

        # move_issue should NOT have been called
        poller.move_issue.assert_not_called()
        # Slot should still be freed
        assert sm.get_slot("test-project", 1).status == "free"

    def test_completed_slot_skips_move_when_ticket_terminal(self, supervisor):
        """_handle_completed_slot doesn't move a ticket that's already Done."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="completed",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = True

        slot = sm.get_slot("test-project", 1)
        supervisor._handle_completed_slot(slot)

        # move_issue should NOT have been called
        poller.move_issue.assert_not_called()
        # Slot should still be freed
        assert sm.get_slot("test-project", 1).status == "free"

    def test_paused_slot_not_resumed_when_ticket_done_in_tick_loop(self, supervisor):
        """_handle_paused_slots frees the slot instead of resuming when ticket is Done."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2020-01-01T00:00:00Z",  # past time
        )

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = True

        with patch.object(supervisor, "_resume_paused_worker") as mock_resume:
            supervisor._handle_paused_slots()

        mock_resume.assert_not_called()
        assert sm.get_slot("test-project", 1).status == "completed_pending_cleanup"


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


class TestCheckPrStatus:
    """Tests for _check_pr_status and its helpers."""

    def test_uses_stored_pr_url(self, supervisor):
        """When slot has pr_url, uses it to check state."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.set_pr_url("test-project", 1, "https://github.com/org/repo/pull/42")
        slot = sm.get_slot("test-project", 1)

        with patch.object(
            Supervisor, "_gh_pr_state", return_value="merged",
        ):
            result = supervisor._check_pr_status(slot)

        assert result[0] == "merged"

    def test_falls_back_to_branch_lookup(self, supervisor):
        """When no pr_url stored, looks up PR by branch."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        assert slot.pr_url is None

        with (
            patch.object(
                Supervisor, "_gh_pr_url_for_branch",
                return_value="https://github.com/org/repo/pull/99",
            ),
            patch.object(Supervisor, "_gh_pr_state", return_value="open"),
        ):
            result = supervisor._check_pr_status(slot)

        assert result[0] == "open"

    def test_returns_none_when_no_pr(self, supervisor):
        """When no PR found by any method, returns None."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)

        with patch.object(
            Supervisor, "_gh_pr_url_for_branch", return_value=None,
        ):
            result = supervisor._check_pr_status(slot)

        assert result == (None, None)

    def test_prefers_db_pr_url_over_slot_state(self, supervisor):
        """PR URL from tasks DB takes priority over slot.pr_url."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        # Set a different URL on slot state
        sm.set_pr_url("test-project", 1, "https://github.com/org/repo/pull/slot-url")
        slot = sm.get_slot("test-project", 1)

        # Store a different URL in the tasks DB
        db_pr_url = "https://github.com/org/repo/pull/db-url"
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
        )
        update_task(supervisor._conn, task_id, pr_url=db_pr_url)
        supervisor._conn.commit()

        with patch.object(
            Supervisor, "_gh_pr_state", return_value="merged",
        ) as mock_state:
            result = supervisor._check_pr_status(slot)

        assert result[0] == "merged"
        # Verify it used the DB URL, not the slot URL
        mock_state.assert_called_once_with(db_pr_url, env=supervisor._git_env)

    def test_falls_back_to_slot_pr_url_when_db_has_none(self, supervisor):
        """When DB has no pr_url, falls back to slot.pr_url."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot_url = "https://github.com/org/repo/pull/slot-url"
        sm.set_pr_url("test-project", 1, slot_url)
        slot = sm.get_slot("test-project", 1)

        # DB task exists but has no pr_url
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
        )
        supervisor._conn.commit()

        with patch.object(
            Supervisor, "_gh_pr_state", return_value="open",
        ) as mock_state:
            result = supervisor._check_pr_status(slot)

        assert result[0] == "open"
        mock_state.assert_called_once_with(slot_url, env=supervisor._git_env)

    def test_db_pr_url_used_for_merged_detection(self, supervisor):
        """_handle_completed_slot uses DB pr_url to detect merged PR and move to Done."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        # Store pr_url in DB (simulating what worker does cross-process)
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
        )
        update_task(supervisor._conn, task_id, pr_url="https://github.com/org/repo/pull/42")
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]

        with patch.object(
            Supervisor, "_gh_pr_state", return_value="merged",
        ):
            supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Done")
        assert sm.get_slot("test-project", 1).status == "free"

    def test_no_pr_reason_moves_ticket_to_done(self, supervisor):
        """_handle_completed_slot moves ticket to Done when no_pr_reason is set."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.no_pr_reason = "All criteria already met on main"
        sm.mark_completed("test-project", 1)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="completed",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Done")
        assert sm.get_slot("test-project", 1).status == "free"

    def test_no_pr_reason_recovered_from_task_comments(self, supervisor):
        """Crash recovery: no_pr_reason lost in memory but recovered from task comments."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        # Simulate crash recovery: no_pr_reason is NOT set on the slot
        assert slot.no_pr_reason is None
        # But the task comments have the persisted signal
        sm.mark_completed("test-project", 1)

        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="completed",
        )
        update_task(
            supervisor._conn, task_id,
            comments="NO_PR_NEEDED: All criteria already met on main",
        )
        supervisor._conn.commit()

        poller = supervisor._pollers["test-project"]

        supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Done")
        assert sm.get_slot("test-project", 1).status == "free"

    def test_gh_pr_state_merged(self):
        """_gh_pr_state returns 'merged' for merged PRs."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "MERGED\n"

        with patch("botfarm.supervisor.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result == "merged"

    def test_gh_pr_state_open(self):
        """_gh_pr_state returns 'open' for open PRs."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "OPEN\n"

        with patch("botfarm.supervisor.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result == "open"

    def test_gh_pr_state_failure(self):
        """_gh_pr_state returns None on command failure."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = ""

        with patch("botfarm.supervisor.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result is None

    def test_gh_pr_url_for_branch_success(self):
        """_gh_pr_url_for_branch returns URL on success."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "https://github.com/org/repo/pull/42\n"

        with patch("botfarm.supervisor.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_url_for_branch("my-branch", "/tmp/repo")

        assert result == "https://github.com/org/repo/pull/42"

    def test_gh_pr_url_for_branch_no_pr(self):
        """_gh_pr_url_for_branch returns None when no PR exists."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = ""

        with patch("botfarm.supervisor.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_url_for_branch("my-branch", "/tmp/repo")

        assert result is None

    def test_gh_pr_state_closed(self):
        """_gh_pr_state returns 'closed' for closed-not-merged PRs."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "CLOSED\n"

        with patch("botfarm.supervisor.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result == "closed"

    def test_gh_pr_url_for_branch_none_branch(self):
        """_gh_pr_url_for_branch returns None for None branch."""
        result = Supervisor._gh_pr_url_for_branch(None, "/tmp/repo")
        assert result is None


# ---------------------------------------------------------------------------
# Configurable Linear status names
# ---------------------------------------------------------------------------


def _make_config_custom_statuses(tmp_path: Path) -> BotfarmConfig:
    """Build a config with custom Linear status names."""
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project",
                linear_team="TST",
                base_dir=str(tmp_path / "repo"),
                worktree_prefix="test-project-slot-",
                slots=[1, 2],
            ),
        ],
        linear=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=10,
            exclude_tags=["Human"],
            todo_status="Backlog",
            in_progress_status="Working",
            done_status="Shipped",
            in_review_status="Review",
            failed_status="Needs Attention",
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

    def test_failed_slot_uses_custom_failed_status(self, supervisor_custom_statuses):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = sup._pollers["test-project"]

        sup._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "Needs Attention")


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
                    linear_team="TST",
                    base_dir=str(tmp_path / "repo"),
                    worktree_prefix="test-project-slot-",
                    slots=[1],
                ),
            ],
            linear=LinearConfig(
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
# Worktree cwd computation (SMA-102)
# ---------------------------------------------------------------------------


class TestSlotWorktreeCwd:
    """Tests for _slot_worktree_cwd and its usage in dispatch/resume/PR check."""

    def test_computes_worktree_path_from_base_dir_parent(self):
        """Worktree cwd = base_dir's parent / worktree_prefix + slot_id."""
        cfg = ProjectConfig(
            name="proj",
            linear_team="T",
            base_dir="/home/user/myproject",
            worktree_prefix="myproject-slot-",
            slots=[1],
        )
        result = Supervisor._slot_worktree_cwd(cfg, 1)
        assert result == "/home/user/myproject-slot-1"

    def test_computes_worktree_path_with_tilde(self):
        """Tilde in base_dir is expanded before computing worktree path."""
        cfg = ProjectConfig(
            name="proj",
            linear_team="T",
            base_dir="~/myproject",
            worktree_prefix="myproject-slot-",
            slots=[2],
        )
        result = Supervisor._slot_worktree_cwd(cfg, 2)
        home = str(Path.home())
        assert result == f"{home}/myproject-slot-2"

    def test_dispatch_worker_uses_worktree_cwd(self, supervisor, tmp_path):
        """_dispatch_worker passes the slot worktree path, not base_dir."""
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc

            supervisor._dispatch_worker("test-project", slot, issue, poller)

            call_kwargs = MockProc.call_args.kwargs["kwargs"]
            expected_cwd = str(tmp_path / "test-project-slot-1")
            assert call_kwargs["cwd"] == expected_cwd

    def test_resume_paused_worker_uses_worktree_cwd(self, supervisor, tmp_path):
        """_resume_paused_worker passes the slot worktree path, not base_dir."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement", session_id="sess-1")
        sm.mark_paused_limit(
            "test-project", 1,
            resume_after="2020-01-01T00:00:00+00:00",
        )

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        with (
            patch.object(supervisor._usage_poller, "force_poll"),
            patch("botfarm.supervisor.multiprocessing.Process") as MockProc,
        ):
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

            call_kwargs = MockProc.call_args.kwargs["kwargs"]
            expected_cwd = str(tmp_path / "test-project-slot-1")
            assert call_kwargs["cwd"] == expected_cwd

    def test_check_pr_status_uses_worktree_cwd_for_branch_lookup(self, supervisor, tmp_path):
        """_check_pr_status passes the slot worktree cwd to gh pr view."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)

        with (
            patch.object(
                Supervisor, "_gh_pr_url_for_branch",
                return_value="https://github.com/org/repo/pull/99",
            ) as mock_branch_lookup,
            patch.object(Supervisor, "_gh_pr_state", return_value="open"),
        ):
            supervisor._check_pr_status(slot)

            expected_cwd = str(tmp_path / "test-project-slot-1")
            mock_branch_lookup.assert_called_once_with("b1", expected_cwd, env=supervisor._git_env)

    def test_dispatch_worker_seeds_slot_db(self, supervisor, tmp_path):
        """_dispatch_worker creates a sandboxed slot DB and passes it to worker."""
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]

        with patch("botfarm.supervisor.Path.home", return_value=tmp_path), \
             patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc

            supervisor._dispatch_worker("test-project", slot, issue, poller)

            call_kwargs = MockProc.call_args.kwargs["kwargs"]
            assert "slot_db_path" in call_kwargs
            assert call_kwargs["slot_db_path"] is not None
            # The slot DB file should have been seeded (copied from prod DB)
            slot_db = Path(call_kwargs["slot_db_path"])
            assert slot_db.exists()

    def test_dispatch_worker_slot_db_path_format(self, supervisor, tmp_path):
        """Slot DB path follows the expected format."""
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]

        with patch("botfarm.supervisor.Path.home", return_value=tmp_path), \
             patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc

            supervisor._dispatch_worker("test-project", slot, issue, poller)

            call_kwargs = MockProc.call_args.kwargs["kwargs"]
            slot_db = call_kwargs["slot_db_path"]
            assert "test-project-1" in slot_db
            assert slot_db.endswith("botfarm.db")

    def test_cleanup_slot_db_removes_directory(self, supervisor, tmp_path):
        """_cleanup_slot_db removes the slot DB directory."""
        # Create the slot DB dir under tmp_path instead of real home
        slot_dir = tmp_path / ".botfarm" / "slots" / "test-project-1"
        slot_dir.mkdir(parents=True, exist_ok=True)
        (slot_dir / "botfarm.db").write_text("fake")
        assert slot_dir.exists()

        with patch("botfarm.supervisor.Path.home", return_value=tmp_path):
            supervisor._cleanup_slot_db("test-project", 1)
        assert not slot_dir.exists()

    def test_cleanup_slot_db_noop_when_missing(self, supervisor, tmp_path):
        """_cleanup_slot_db does not error when directory doesn't exist."""
        # Should not raise
        supervisor._cleanup_slot_db("nonexistent-project", 99)

    def test_completed_slot_cleans_up_db(self, supervisor, tmp_path):
        """When a completed slot is freed, its sandboxed DB is cleaned up."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)
        slot = sm.get_slot("test-project", 1)

        with patch.object(supervisor, "_cleanup_slot_db") as mock_cleanup:
            supervisor._handle_completed_slot(slot)
            mock_cleanup.assert_called_once_with("test-project", 1)

    def test_failed_slot_cleans_up_db(self, supervisor, tmp_path):
        """When a failed slot is freed, its sandboxed DB is cleaned up."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1)
        slot = sm.get_slot("test-project", 1)

        with patch.object(supervisor, "_cleanup_slot_db") as mock_cleanup:
            supervisor._handle_failed_slot(slot)
            mock_cleanup.assert_called_once_with("test-project", 1)


# ---------------------------------------------------------------------------
# Manual pause / resume
# ---------------------------------------------------------------------------


class TestNextStageAfter:
    def test_empty_stages_returns_first(self):
        assert Supervisor._next_stage_after([]) == "implement"

    def test_after_implement(self):
        assert Supervisor._next_stage_after(["implement"]) == "review"

    def test_after_implement_review_fix(self):
        assert Supervisor._next_stage_after(["implement", "review", "fix"]) == "pr_checks"

    def test_all_stages_returns_none(self):
        assert Supervisor._next_stage_after(list(STAGES)) is None

    def test_after_partial_stages(self):
        assert Supervisor._next_stage_after(["implement", "review"]) == "fix"


class TestWorkerResultPaused:
    def test_paused_field_default_false(self):
        wr = _WorkerResult(project="p", slot_id=1, success=False)
        assert wr.paused is False

    def test_paused_field_set(self):
        wr = _WorkerResult(project="p", slot_id=1, success=False, paused=True)
        assert wr.paused is True


class TestReconcilePausedResult:
    def test_paused_result_marks_slot_paused_manual(self, supervisor):
        """When a worker sends paused=True result, slot becomes paused_manual."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.complete_stage("test-project", 1, "implement")

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            paused=True,
        ))

        supervisor._reconcile_workers()

        assert sm.get_slot("test-project", 1).status == "paused_manual"


class TestManualPauseResume:
    def test_request_pause_sets_dispatch_paused(self, supervisor):
        """request_pause() sets the event; _handle_manual_pause_resume processes it."""
        supervisor.request_pause()
        supervisor._handle_manual_pause_resume()

        sm = supervisor.slot_manager
        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "manual_pause"

        events = get_events(supervisor._conn, event_type="manual_pause_requested")
        assert len(events) == 1

    def test_pause_with_no_busy_slots_completes_immediately(self, supervisor):
        """If no slots are busy when pause is handled, pause completes right away."""
        supervisor.request_pause()
        supervisor._handle_manual_pause_resume()

        # Should have completed immediately since no workers were busy
        events = get_events(supervisor._conn, event_type="manual_pause_complete")
        assert len(events) == 1
        assert supervisor._manual_pause_requested is False

    def test_pause_signals_busy_workers(self, supervisor):
        """Pause sets the pause_event for busy workers."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.set_pid("test-project", 1, os.getpid())

        # Create a pause event for this worker
        import multiprocessing
        pause_evt = multiprocessing.Event()
        supervisor._pause_events[("test-project", 1)] = pause_evt

        supervisor.request_pause()
        supervisor._handle_manual_pause_resume()

        assert pause_evt.is_set()
        assert supervisor._manual_pause_requested is True

    def test_resume_clears_dispatch_pause(self, supervisor):
        """request_resume() clears manual pause and dispatch state."""
        # First pause
        supervisor.request_pause()
        supervisor._handle_manual_pause_resume()

        # Then resume
        supervisor.request_resume()
        supervisor._handle_manual_pause_resume()

        sm = supervisor.slot_manager
        assert sm.dispatch_paused is False

        events = get_events(supervisor._conn, event_type="manual_resumed")
        assert len(events) >= 1

    def test_resume_cancels_in_flight_pause(self, supervisor):
        """If resume is called while pause is still in flight, it cancels the pause."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.set_pid("test-project", 1, os.getpid())

        import multiprocessing
        pause_evt = multiprocessing.Event()
        supervisor._pause_events[("test-project", 1)] = pause_evt

        # Pause — signals worker
        supervisor.request_pause()
        supervisor._handle_manual_pause_resume()
        assert pause_evt.is_set()

        # Resume before worker finishes — should clear the event
        supervisor.request_resume()
        supervisor._handle_manual_pause_resume()

        assert not pause_evt.is_set()
        assert supervisor._manual_pause_requested is False

    def test_resume_resumes_paused_manual_slots(self, supervisor):
        """Resume re-spawns workers for paused_manual slots."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.complete_stage("test-project", 1, "implement")
        sm.mark_paused_manual("test-project", 1)

        # Set up manual pause state
        sm.set_dispatch_paused(True, "manual_pause")

        supervisor.request_resume()
        with patch.object(supervisor, "_resume_manual_paused_worker") as mock_resume:
            supervisor._handle_manual_pause_resume()
            mock_resume.assert_called_once()
            # Verify it was called with the paused slot
            called_slot = mock_resume.call_args[0][0]
            assert called_slot.ticket_id == "TST-1"

    def test_manual_pause_does_not_auto_resume(self, supervisor):
        """_poll_and_dispatch should not resume dispatch when paused for manual_pause."""
        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "manual_pause")

        # Set utilization to low values that would normally trigger resume
        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

        # Should still be paused
        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "manual_pause"

    def test_update_in_progress_does_not_auto_resume(self, supervisor):
        """_poll_and_dispatch should not resume dispatch when paused for update_in_progress."""
        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "update_in_progress")

        # Set utilization to low values that would normally trigger resume
        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

        # Should still be paused
        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "update_in_progress"

    def test_update_request_overrides_usage_pause_reason(self, supervisor):
        """Update requested while usage-paused should take over the pause reason.

        Regression: if the supervisor is already paused for utilization and an
        update is requested, _handle_update_request must override the reason to
        "update_in_progress" so that _poll_and_dispatch won't auto-resume when
        utilization drops.
        """
        sm = supervisor.slot_manager
        # Start with a usage-based pause (contains "utilization")
        sm.set_dispatch_paused(True, "5-hour utilization at 85% exceeds threshold 75%")

        # Need a busy worker so _handle_update_request waits (returns early)
        # instead of proceeding to git pull which would fail and unpause.
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        # Request an update — _handle_update_request should take over the reason
        supervisor.request_update()
        supervisor._handle_update_request()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "update_in_progress"

        # Verify update_started event was recorded for the take-over
        events = get_events(supervisor._conn, event_type="update_started")
        assert len(events) == 1
        assert "taking over" in events[0]["detail"]

        # Now simulate utilization dropping — _poll_and_dispatch should NOT resume
        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "update_in_progress"

    def test_update_dispatch_race_condition_across_ticks(self, supervisor):
        """Update pause must survive repeated _poll_and_dispatch calls.

        Regression test for SMA-269: when an update is requested and workers
        are busy, _poll_and_dispatch must not unpause dispatch on each tick,
        which would cause an infinite pause/unpause loop.
        """
        sm = supervisor.slot_manager

        # Need a busy worker so the update handler waits each tick
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        # Low utilization — would normally trigger resume of a usage pause
        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[], blocked=[], auto_close_parents=[],
        )

        # Request an update
        supervisor.request_update()

        # Simulate several ticks: _handle_update_request sets the pause,
        # then _poll_and_dispatch must NOT undo it.
        for _ in range(5):
            supervisor._handle_update_request()
            supervisor._poll_and_dispatch()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "update_in_progress"

    def test_paused_result_propagates_stages_completed(self, supervisor):
        """Paused worker result syncs stages_completed to supervisor slot."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            paused=True,
            stages_completed=["implement", "review"],
        ))

        supervisor._reconcile_workers()

        slot = sm.get_slot("test-project", 1)
        assert slot.status == "paused_manual"
        assert slot.stages_completed == ["implement", "review"]

    def test_pause_upgrades_reason_from_usage_limit(self, supervisor):
        """Manual pause upgrades dispatch_pause_reason from usage_limit."""
        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "usage_limit")

        supervisor.request_pause()
        supervisor._handle_manual_pause_resume()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "manual_pause"

    def test_pause_events_cleaned_for_failed_workers(self, supervisor):
        """_pause_events is cleaned up even when workers fail."""
        import multiprocessing
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        pause_evt = multiprocessing.Event()
        supervisor._pause_events[("test-project", 1)] = pause_evt

        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=False,
            failure_stage="implement",
            failure_reason="something went wrong",
        ))

        supervisor._reconcile_workers()

        assert ("test-project", 1) not in supervisor._pause_events


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


# ---------------------------------------------------------------------------
# Usage stall detection
# ---------------------------------------------------------------------------


class TestUsageStallDetection:
    def test_no_warning_before_threshold(self, supervisor, caplog):
        """No stall warning if less than _STALL_WARN_MINUTES have elapsed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=0.52,
            dispatch_time=time.time() - 10 * 60,  # 10 minutes ago
        )
        supervisor._usage_poller.state.utilization_5h = 0.52

        with caplog.at_level(logging.WARNING, logger="botfarm.supervisor"):
            supervisor._check_usage_stalls()

        assert not any("usage stalled" in r.message for r in caplog.records)
        assert not supervisor._stall_tracking[("test-project", 1)].warned

    def test_warning_after_threshold_with_flat_usage(self, supervisor, caplog):
        """Stall warning when usage unchanged for > _STALL_WARN_MINUTES."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=0.52,
            dispatch_time=time.time() - 35 * 60,  # 35 minutes ago
        )
        supervisor._usage_poller.state.utilization_5h = 0.52

        with caplog.at_level(logging.WARNING, logger="botfarm.supervisor"):
            supervisor._check_usage_stalls()

        warnings = [r for r in caplog.records if "usage stalled" in r.message]
        assert len(warnings) == 1
        assert "TST-1" in warnings[0].message
        assert "52.0%" in warnings[0].message
        assert supervisor._stall_tracking[("test-project", 1)].warned

    def test_no_warning_when_usage_moved(self, supervisor, caplog):
        """No warning if usage has moved beyond epsilon since dispatch."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=0.50,
            dispatch_time=time.time() - 35 * 60,
        )
        supervisor._usage_poller.state.utilization_5h = 0.56  # moved 6%

        with caplog.at_level(logging.WARNING, logger="botfarm.supervisor"):
            supervisor._check_usage_stalls()

        assert not any("usage stalled" in r.message for r in caplog.records)

    def test_warning_emitted_only_once(self, supervisor, caplog):
        """Once stall warning is emitted, it's not repeated on subsequent checks."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=0.52,
            dispatch_time=time.time() - 35 * 60,
        )
        supervisor._usage_poller.state.utilization_5h = 0.52

        with caplog.at_level(logging.WARNING, logger="botfarm.supervisor"):
            supervisor._check_usage_stalls()
            caplog.clear()
            supervisor._check_usage_stalls()

        assert not any("usage stalled" in r.message for r in caplog.records)

    def test_tracking_cleared_when_slot_freed(self, supervisor):
        """Stall tracking is cleaned up when a slot is no longer busy."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=0.52,
            dispatch_time=time.time() - 35 * 60,
        )
        supervisor._usage_poller.state.utilization_5h = 0.52

        # Free the slot
        sm.free_slot("test-project", 1)

        supervisor._check_usage_stalls()

        assert ("test-project", 1) not in supervisor._stall_tracking

    def test_no_crash_when_utilization_is_none(self, supervisor):
        """No crash when usage data is not available yet."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=0.52,
            dispatch_time=time.time() - 35 * 60,
        )
        supervisor._usage_poller.state.utilization_5h = None

        # Should not raise
        supervisor._check_usage_stalls()

    def test_no_crash_when_dispatch_usage_is_none(self, supervisor, caplog):
        """No warning when dispatch-time usage was unavailable."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=None,
            dispatch_time=time.time() - 35 * 60,
        )
        supervisor._usage_poller.state.utilization_5h = 0.52

        with caplog.at_level(logging.WARNING, logger="botfarm.supervisor"):
            supervisor._check_usage_stalls()

        assert not any("usage stalled" in r.message for r in caplog.records)

    def test_dispatch_worker_records_stall_tracking(self, supervisor, tmp_path):
        """_dispatch_worker records stall tracking data."""
        sm = supervisor.slot_manager
        slot = sm.get_slot("test-project", 1)
        issue = make_issue()
        poller = supervisor._pollers["test-project"]

        supervisor._usage_poller.state.utilization_5h = 0.42

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc
            supervisor._dispatch_worker("test-project", slot, issue, poller)

        key = ("test-project", 1)
        assert key in supervisor._stall_tracking
        info = supervisor._stall_tracking[key]
        assert info.dispatch_usage_5h == 0.42
        assert not info.warned

    def test_epsilon_boundary(self, supervisor, caplog):
        """Usage change exactly at epsilon boundary does not trigger warning."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        supervisor._stall_tracking[("test-project", 1)] = _StallInfo(
            dispatch_usage_5h=0.500,
            dispatch_time=time.time() - 35 * 60,
        )
        # Delta is exactly 0.005 = epsilon, so should NOT trigger (< epsilon)
        supervisor._usage_poller.state.utilization_5h = 0.505

        with caplog.at_level(logging.WARNING, logger="botfarm.supervisor"):
            supervisor._check_usage_stalls()

        assert not any("usage stalled" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Degraded mode
# ---------------------------------------------------------------------------


class TestDegradedMode:
    """Tests for graceful degraded mode when preflight checks fail."""

    def test_run_enters_degraded_mode_on_preflight_failure(self, supervisor, tmp_path):
        """run() enters degraded mode and does NOT call _recover_on_startup."""
        tick_count = 0

        def fake_tick_degraded():
            nonlocal tick_count
            tick_count += 1
            supervisor._shutdown_requested = True

        failed_result = MagicMock()
        failed_result.passed = False
        failed_result.critical = True
        failed_result.name = "git_repo:test"
        failed_result.message = "base_dir missing"

        with (
            patch.object(supervisor, "_tick_degraded", side_effect=fake_tick_degraded),
            patch.object(supervisor, "_sleep"),
            patch.object(supervisor, "install_signal_handlers"),
            patch.object(supervisor, "_recover_on_startup") as mock_recover,
            patch("botfarm.supervisor.run_preflight_checks", return_value=[failed_result]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=False),
        ):
            supervisor.run()

        assert tick_count == 1
        mock_recover.assert_not_called()
        assert supervisor._degraded is True

    def test_run_records_preflight_failed_event_in_degraded_mode(self, supervisor, tmp_path):
        """Entering degraded mode logs a preflight_failed event."""
        failed_result = MagicMock()
        failed_result.passed = False
        failed_result.critical = True
        failed_result.name = "git_repo:test"
        failed_result.message = "base_dir missing"

        supervisor._shutdown_requested = True

        with (
            patch.object(supervisor, "install_signal_handlers"),
            patch("botfarm.supervisor.run_preflight_checks", return_value=[failed_result]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=False),
        ):
            supervisor.run()

        conn = init_db(supervisor._db_path)
        events = get_events(conn, event_type="preflight_failed")
        assert len(events) >= 1
        assert "git_repo:test" in events[0]["detail"]
        conn.close()

    def test_rerun_preflight_transitions_to_normal_on_success(self, supervisor):
        """_rerun_preflight clears degraded flag when all checks pass."""
        supervisor._degraded = True

        with (
            patch("botfarm.supervisor.run_preflight_checks", return_value=[]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=True),
            patch.object(supervisor, "_recover_on_startup") as mock_recover,
        ):
            supervisor._rerun_preflight()

        assert supervisor._degraded is False
        mock_recover.assert_called_once()

    def test_rerun_preflight_logs_recovered_event(self, supervisor):
        """_rerun_preflight logs a preflight_recovered event on success."""
        supervisor._degraded = True

        with (
            patch("botfarm.supervisor.run_preflight_checks", return_value=[]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=True),
            patch.object(supervisor, "_recover_on_startup"),
        ):
            supervisor._rerun_preflight()

        events = get_events(supervisor._conn, event_type="preflight_recovered")
        assert len(events) >= 1

    def test_rerun_preflight_stays_degraded_on_failure(self, supervisor):
        """_rerun_preflight keeps degraded=True when checks still fail."""
        supervisor._degraded = True

        failed_result = MagicMock()
        failed_result.passed = False
        failed_result.critical = True
        failed_result.name = "linear_api"
        failed_result.message = "key invalid"

        with (
            patch("botfarm.supervisor.run_preflight_checks", return_value=[failed_result]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=False),
        ):
            supervisor._rerun_preflight()

        assert supervisor._degraded is True

    def test_tick_degraded_reruns_after_interval(self, supervisor):
        """_tick_degraded triggers rerun when interval has elapsed."""
        supervisor._degraded = True
        supervisor._last_preflight_rerun = 0.0  # Long ago

        with patch.object(supervisor, "_rerun_preflight") as mock_rerun:
            supervisor._tick_degraded()

        mock_rerun.assert_called_once()

    def test_tick_degraded_skips_when_interval_not_elapsed(self, supervisor):
        """_tick_degraded does NOT rerun before interval elapses."""
        import time
        supervisor._degraded = True
        supervisor._last_preflight_rerun = time.monotonic()  # Just now

        with patch.object(supervisor, "_rerun_preflight") as mock_rerun:
            supervisor._tick_degraded()

        mock_rerun.assert_not_called()

    def test_tick_degraded_manual_rerun_triggers_immediately(self, supervisor):
        """_tick_degraded triggers rerun when manual event is set."""
        import time
        supervisor._degraded = True
        supervisor._last_preflight_rerun = time.monotonic()  # Just now
        supervisor._rerun_preflight_event.set()

        with patch.object(supervisor, "_rerun_preflight") as mock_rerun:
            supervisor._tick_degraded()

        mock_rerun.assert_called_once()

    def test_tick_degraded_drains_stop_requests(self, supervisor):
        """_tick_degraded drains pending stop-slot requests."""
        import time
        supervisor._degraded = True
        supervisor._last_preflight_rerun = time.monotonic()  # Just now

        with patch.object(supervisor, "_handle_stop_requests") as mock_handle:
            with patch.object(supervisor, "_rerun_preflight"):
                supervisor._tick_degraded()

        mock_handle.assert_called_once()

    def test_request_rerun_preflight_sets_events(self, supervisor):
        """request_rerun_preflight sets both the rerun and wake events."""
        supervisor.request_rerun_preflight()
        assert supervisor._rerun_preflight_event.is_set()
        assert supervisor._wake_event.is_set()

    def test_get_preflight_results_returns_copy(self, supervisor):
        """get_preflight_results returns a copy of results."""
        result = MagicMock()
        supervisor._preflight_results = [result]
        got = supervisor.get_preflight_results()
        assert got == [result]
        assert got is not supervisor._preflight_results

    def test_degraded_property(self, supervisor):
        """degraded property reflects _degraded state."""
        assert supervisor.degraded is False
        supervisor._degraded = True
        assert supervisor.degraded is True

    def test_preflight_results_stored_on_run(self, supervisor, tmp_path):
        """run() stores preflight results accessible via get_preflight_results."""
        passed_result = MagicMock()
        passed_result.passed = True
        passed_result.critical = True
        passed_result.name = "database"
        passed_result.message = "OK"

        supervisor._shutdown_requested = True

        with (
            patch.object(supervisor, "install_signal_handlers"),
            patch("botfarm.supervisor.run_preflight_checks", return_value=[passed_result]),
            patch("botfarm.supervisor.log_preflight_summary", return_value=True),
        ):
            supervisor.run()

        # After shutdown, supervisor._conn is closed, but we can check
        # the preflight results were stored
        assert len(supervisor._preflight_results) == 1


# ---------------------------------------------------------------------------
# Ticket history capture
# ---------------------------------------------------------------------------


class TestTicketHistoryCapture:
    def _mock_fetch_issue_details(self, identifier):
        """Return a dict mimicking LinearClient.fetch_issue_details()."""
        return {
            "ticket_id": identifier,
            "linear_uuid": f"uuid-{identifier}",
            "title": f"Title for {identifier}",
            "description": "Some description",
            "status": "In Progress",
            "priority": 2,
            "url": f"https://linear.app/test/{identifier}",
            "assignee_name": "Bot",
            "assignee_email": "bot@test.com",
            "creator_name": "User",
            "project_name": "test-project",
            "team_name": "TST",
            "estimate": None,
            "due_date": None,
            "parent_id": None,
            "children_ids": json.dumps([]),
            "blocked_by": json.dumps([]),
            "blocks": json.dumps([]),
            "labels": json.dumps([]),
            "comments_json": json.dumps([]),
            "linear_created_at": "2026-01-01T00:00:00Z",
            "linear_updated_at": "2026-02-01T00:00:00Z",
            "linear_completed_at": None,
            "raw_json": json.dumps({"id": f"uuid-{identifier}"}),
        }

    def test_dispatch_captures_ticket_history(self, supervisor):
        """Dispatch should capture ticket details into ticket_history."""
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = self._mock_fetch_issue_details

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc
            supervisor._dispatch_worker("test-project", slot, issue, poller)

        row = get_ticket_history_entry(supervisor._conn, "TST-1")
        assert row is not None
        assert row["title"] == "Title for TST-1"
        assert row["capture_source"] == "dispatch"
        assert row["branch_name"] == "test-project-slot-1"

    def test_completion_captures_ticket_history(self, supervisor):
        """Completed slot should capture ticket details into ticket_history."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pr_url = "https://github.com/test/pr/1"
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = self._mock_fetch_issue_details

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        row = get_ticket_history_entry(supervisor._conn, "TST-1")
        assert row is not None
        assert row["capture_source"] == "completion"
        assert row["pr_url"] == "https://github.com/test/pr/1"
        assert row["branch_name"] == "b1"

    def test_failure_captures_ticket_history(self, supervisor):
        """Failed slot should capture ticket details into ticket_history."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = self._mock_fetch_issue_details

        supervisor._handle_finished_slots()

        row = get_ticket_history_entry(supervisor._conn, "TST-1")
        assert row is not None
        assert row["capture_source"] == "failure"

    def test_capture_failure_does_not_block_dispatch(self, supervisor):
        """If ticket history capture fails, dispatch should still proceed."""
        issue = make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = Exception("API down")

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 12345
            MockProc.return_value = mock_proc
            supervisor._dispatch_worker("test-project", slot, issue, poller)

        # Dispatch still happened
        updated_slot = supervisor.slot_manager.get_slot("test-project", 1)
        assert updated_slot.status == "busy"
        assert updated_slot.ticket_id == "TST-1"

        # No ticket history entry
        row = get_ticket_history_entry(supervisor._conn, "TST-1")
        assert row is None

    def test_capture_failure_does_not_block_completion(self, supervisor):
        """If ticket history capture fails at completion, slot is still freed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = Exception("API down")

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        assert sm.get_slot("test-project", 1).status == "free"

    def test_capture_failure_does_not_block_failure_handling(self, supervisor):
        """If ticket history capture fails at failure, slot is still freed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_failed("test-project", 1, reason="crash")

        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = Exception("API down")

        supervisor._handle_finished_slots()

        assert sm.get_slot("test-project", 1).status == "free"

    def test_completion_updates_dispatch_capture(self, supervisor):
        """Completion capture should update an existing dispatch capture."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )

        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = self._mock_fetch_issue_details

        # Simulate dispatch capture
        supervisor._capture_ticket_history("TST-1", "dispatch", branch_name="b1")
        row = get_ticket_history_entry(supervisor._conn, "TST-1")
        assert row["capture_source"] == "dispatch"

        # Simulate completion capture
        supervisor._capture_ticket_history(
            "TST-1", "completion",
            pr_url="https://github.com/test/pr/1", branch_name="b1",
        )
        row = get_ticket_history_entry(supervisor._conn, "TST-1")
        assert row["capture_source"] == "completion"
        assert row["pr_url"] == "https://github.com/test/pr/1"


# ---------------------------------------------------------------------------
# Capacity polling
# ---------------------------------------------------------------------------


class TestPollCapacity:
    """Tests for _poll_capacity() — Linear issue count monitoring."""

    def test_disabled_config_skips_polling(self, supervisor):
        """When capacity_monitoring.enabled is False, nothing happens."""
        supervisor._config.linear.capacity_monitoring = CapacityConfig(enabled=False)
        supervisor._linear_client = MagicMock()

        supervisor._poll_capacity()

        supervisor._linear_client.count_active_issues.assert_not_called()

    def test_api_failure_skips_tick(self, supervisor, caplog):
        """When count_active_issues returns None, the tick is skipped."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = None

        with caplog.at_level(logging.WARNING):
            supervisor._poll_capacity()

        assert "Capacity poll failed" in caplog.text
        assert supervisor._capacity_level == "normal"

    def test_normal_level_no_events(self, supervisor):
        """Below warning threshold, no events are emitted."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=100, by_project={"proj": 100},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "normal"
        events = get_events(supervisor._conn, event_type="capacity_warning")
        assert len(events) == 0

    def test_warning_threshold_emits_event(self, supervisor):
        """Crossing the warning threshold emits a capacity_warning event."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=180, by_project={"proj": 180},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "warning"
        events = get_events(supervisor._conn, event_type="capacity_warning")
        assert len(events) == 1
        assert "180/250" in events[0]["detail"]

    def test_critical_threshold_emits_event(self, supervisor):
        """Crossing the critical threshold emits a capacity_critical event."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=215, by_project={"proj": 215},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "critical"
        events = get_events(supervisor._conn, event_type="capacity_critical")
        assert len(events) == 1

    def test_blocked_threshold_pauses_dispatch(self, supervisor):
        """At >=95% capacity, dispatch is auto-paused with reason capacity_blocked."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=238, by_project={"proj": 238},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "blocked"
        assert supervisor.slot_manager.dispatch_paused is True
        assert supervisor.slot_manager.dispatch_pause_reason == "capacity_blocked"
        events = get_events(supervisor._conn, event_type="capacity_blocked")
        assert len(events) == 1

    def test_hysteresis_stays_blocked_above_resume_threshold(self, supervisor):
        """Once blocked, stays blocked until utilization drops below resume_threshold."""
        supervisor._linear_client = MagicMock()

        # First: enter blocked state
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"

        # Drop to 92% — above resume_threshold (90%), should stay blocked
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=230, by_project={"proj": 230},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"
        assert supervisor.slot_manager.dispatch_paused is True

    def test_resume_below_resume_threshold(self, supervisor):
        """Dispatch resumes when utilization drops below resume_threshold."""
        supervisor._linear_client = MagicMock()

        # Enter blocked state
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"

        # Drop below resume threshold (90% of 250 = 225)
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=220, by_project={"proj": 220},
        )
        supervisor._poll_capacity()

        assert supervisor._capacity_level == "critical"
        assert supervisor.slot_manager.dispatch_paused is False
        events = get_events(supervisor._conn, event_type="capacity_cleared")
        assert len(events) == 1

    def test_event_emitted_only_on_transition(self, supervisor):
        """Repeated polls at same level don't produce duplicate events."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=180, by_project={"proj": 180},
        )

        supervisor._poll_capacity()
        supervisor._poll_capacity()
        supervisor._poll_capacity()

        events = get_events(supervisor._conn, event_type="capacity_warning")
        assert len(events) == 1

    def test_capacity_state_saved_to_db(self, supervisor):
        """Each poll persists the capacity snapshot to dispatch_state."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=150, by_project={"proj-a": 100, "proj-b": 50},
        )

        supervisor._poll_capacity()

        state = load_capacity_state(supervisor._conn)
        assert state is not None
        assert state["issue_count"] == 150
        assert state["limit"] == 250
        assert state["by_project"] == {"proj-a": 100, "proj-b": 50}
        assert state["checked_at"] is not None

    def test_blocked_event_on_jump_from_normal(self, supervisor):
        """Jumping from normal directly to blocked emits capacity_blocked."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=245, by_project={"proj": 245},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "blocked"
        # Should have capacity_blocked event, not capacity_warning or capacity_critical
        blocked_events = get_events(supervisor._conn, event_type="capacity_blocked")
        assert len(blocked_events) == 1
        warning_events = get_events(supervisor._conn, event_type="capacity_warning")
        assert len(warning_events) == 0

    def test_tick_includes_poll_capacity_phase(self, supervisor):
        """_poll_capacity is called as part of the _tick() phase list."""
        with (
            patch.object(supervisor, "_reconcile_workers"),
            patch.object(supervisor, "_handle_manual_pause_resume"),
            patch.object(supervisor, "_handle_update_request"),
            patch.object(supervisor, "_check_timeouts"),
            patch.object(supervisor, "_handle_finished_slots"),
            patch.object(supervisor, "_handle_paused_slots"),
            patch.object(supervisor, "_poll_usage"),
            patch.object(supervisor, "_poll_capacity") as mock_cap,
            patch.object(supervisor, "_poll_and_dispatch"),
            patch.object(supervisor, "_cleanup_old_ticket_logs"),
        ):
            supervisor._tick()
            mock_cap.assert_called_once()


class TestCapacityNotificationIntegration:
    """Tests that _poll_capacity() fires webhook notifications on transitions."""

    def test_warning_transition_notifies(self, supervisor):
        """Transitioning to warning level calls notify_capacity_warning."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=180, by_project={"proj": 180},
        )
        supervisor._notifier = MagicMock()

        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_warning.assert_called_once_with(
            count=180, limit=250, percentage=pytest.approx(72.0),
        )

    def test_critical_transition_notifies(self, supervisor):
        """Transitioning to critical level calls notify_capacity_critical."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=215, by_project={"proj": 215},
        )
        supervisor._notifier = MagicMock()

        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_critical.assert_called_once_with(
            count=215, limit=250, percentage=pytest.approx(86.0),
        )

    def test_blocked_transition_notifies(self, supervisor):
        """Transitioning to blocked level calls notify_capacity_blocked."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=238, by_project={"proj": 238},
        )
        supervisor._notifier = MagicMock()

        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_blocked.assert_called_once_with(
            count=238, limit=250, percentage=pytest.approx(95.2),
        )

    def test_cleared_transition_notifies(self, supervisor):
        """Resuming from blocked calls notify_capacity_cleared."""
        supervisor._linear_client = MagicMock()
        supervisor._notifier = MagicMock()

        # Enter blocked state
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        supervisor._notifier.reset_mock()

        # Drop below resume threshold (90% of 250 = 225)
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=220, by_project={"proj": 220},
        )
        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_cleared.assert_called_once_with(
            count=220, limit=250, percentage=pytest.approx(88.0),
        )

    def test_no_notification_on_same_level(self, supervisor):
        """Repeated polls at the same level do not fire notifications."""
        supervisor._linear_client = MagicMock()
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=180, by_project={"proj": 180},
        )
        supervisor._notifier = MagicMock()

        supervisor._poll_capacity()
        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_warning.assert_called_once()


class TestCapacityBlockedDispatchInteraction:
    """Tests for capacity_blocked interaction with _poll_and_dispatch."""

    def test_usage_resume_does_not_clear_capacity_blocked(self, supervisor):
        """Usage auto-resume should not clear a capacity_blocked pause."""
        # Set up capacity-blocked state
        supervisor._capacity_level = "blocked"
        supervisor.slot_manager.set_dispatch_paused(True, "capacity_blocked")

        # Usage says all clear
        supervisor._usage_poller._state.utilization_5h = 0.50
        supervisor._usage_poller._state.utilization_7d = 0.50

        supervisor._poll_and_dispatch()

        # Dispatch should still be paused with capacity_blocked
        assert supervisor.slot_manager.dispatch_paused is True
        assert supervisor.slot_manager.dispatch_pause_reason == "capacity_blocked"

    def test_usage_pause_when_capacity_already_blocked(self, supervisor):
        """Usage pause does not overwrite capacity_blocked reason when dispatch is already paused."""
        supervisor._capacity_level = "blocked"
        supervisor.slot_manager.set_dispatch_paused(True, "capacity_blocked")

        # Usage also says pause
        supervisor._usage_poller._state.utilization_5h = 0.90

        supervisor._poll_and_dispatch()

        # Still paused — usage doesn't overwrite because dispatch is already paused
        assert supervisor.slot_manager.dispatch_paused is True
        assert supervisor.slot_manager.dispatch_pause_reason == "capacity_blocked"


# ---------------------------------------------------------------------------
# start_paused
# ---------------------------------------------------------------------------


class TestStartPaused:
    """Tests for the start_paused config option and supervisor startup logic."""

    def test_start_paused_true_pauses_dispatch_on_startup(self, tmp_config, tmp_path, monkeypatch):
        """Supervisor starts with dispatch paused when start_paused=True."""
        tmp_config.start_paused = True
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        assert sm.dispatch_paused is False  # Not paused yet

        sup._recover_on_startup()
        sup._apply_start_paused()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "start_paused"
        assert sup._startup_paused is True

    def test_start_paused_false_does_not_pause(self, tmp_config, tmp_path, monkeypatch):
        """Supervisor starts with dispatch active when start_paused=False."""
        tmp_config.start_paused = False
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        sup._recover_on_startup()
        sup._apply_start_paused()

        assert sm.dispatch_paused is False
        assert sup._startup_paused is False

    def test_start_paused_not_auto_resumed_by_usage(self, supervisor):
        """_poll_and_dispatch should not auto-resume dispatch paused for start_paused."""
        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "start_paused")

        # Set utilization to low values that would normally trigger resume
        supervisor._usage_poller._state.utilization_5h = 0.10
        supervisor._usage_poller._state.utilization_7d = 0.10

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(
            candidates=[make_issue()], blocked=[], auto_close_parents=[],
        )

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "start_paused"

    def test_resume_clears_start_paused(self, supervisor):
        """request_resume (manual resume) clears start_paused dispatch pause."""
        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "start_paused")
        supervisor._startup_paused = True

        # Trigger resume
        supervisor.request_resume()
        supervisor._handle_manual_pause_resume()

        assert sm.dispatch_paused is False
        assert sm.dispatch_pause_reason is None
        assert supervisor._startup_paused is False

    def test_start_paused_skipped_when_already_paused(self, tmp_config, tmp_path, monkeypatch):
        """start_paused does not override an existing pause (e.g. from crash recovery)."""
        tmp_config.start_paused = True
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        # Pre-set a different pause reason (e.g. from persisted crash state)
        sm.set_dispatch_paused(True, "manual_pause")

        sup._recover_on_startup()
        sup._apply_start_paused()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "manual_pause"

    def test_start_paused_resyncs_flag_from_persisted_state(self, tmp_config, tmp_path, monkeypatch):
        """_startup_paused flag is set when persisted reason is already start_paused."""
        tmp_config.start_paused = True
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        # Simulate persisted start_paused from a previous run
        sm.set_dispatch_paused(True, "start_paused")
        assert sup._startup_paused is False  # Not yet synced

        sup._apply_start_paused()

        # Flag should be synced from persisted state
        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "start_paused"
        assert sup._startup_paused is True

    def test_restart_with_start_paused_false_clears_persisted_pause(
        self, tmp_config, tmp_path, monkeypatch,
    ):
        """Restarting with start_paused=false clears a persisted start_paused pause."""
        tmp_config.start_paused = False
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
        mock_poller.is_issue_terminal.return_value = False

        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

        sm = sup.slot_manager
        # Simulate persisted start_paused from a previous run
        sm.set_dispatch_paused(True, "start_paused")
        assert sm.dispatch_paused is True

        sup._apply_start_paused()

        # Config says false — persisted pause should be cleared
        assert sm.dispatch_paused is False
        assert sm.dispatch_pause_reason is None
        assert sup._startup_paused is False

    def test_capacity_blocked_restores_start_paused(self, supervisor):
        """When capacity_blocked clears and _startup_paused is set, restore start_paused."""
        sm = supervisor.slot_manager
        supervisor._startup_paused = True
        supervisor._linear_client = MagicMock()

        # Enter blocked state
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"
        assert sm.dispatch_pause_reason == "capacity_blocked"

        # Drop below resume threshold — should restore start_paused, not unpause
        supervisor._linear_client.count_active_issues.return_value = ActiveIssuesCount(
            total=220, by_project={"proj": 220},
        )
        supervisor._poll_capacity()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "start_paused"

    def test_update_failed_restores_start_paused(self, supervisor):
        """When update fails and _startup_paused is set, restore start_paused."""
        sm = supervisor.slot_manager
        supervisor._startup_paused = True
        sm.set_dispatch_paused(True, "update_in_progress")
        supervisor._update_event.set()

        with patch("botfarm.git_update.pull_and_install", return_value=False):
            supervisor._handle_update_request()

        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "start_paused"

    def test_cli_resume_clears_start_paused_on_next_tick(self, supervisor):
        """Simulate CLI `botfarm resume` clearing start_paused in DB;
        the next supervisor tick should pick it up and clear in-memory state."""
        from botfarm.db import save_dispatch_state, load_dispatch_state

        sm = supervisor.slot_manager
        sm.set_dispatch_paused(True, "start_paused")
        supervisor._startup_paused = True

        # Simulate CLI writing paused=False directly to the DB
        # (preserving heartbeat, as the real CLI now does)
        _paused, _reason, heartbeat = load_dispatch_state(supervisor._conn)
        save_dispatch_state(
            supervisor._conn, paused=False, supervisor_heartbeat=heartbeat,
        )
        supervisor._conn.commit()

        # In-memory state is still paused
        assert sm.dispatch_paused is True
        assert supervisor._startup_paused is True

        # Run a tick — the supervisor should detect the external change
        supervisor._tick()

        assert sm.dispatch_paused is False
        assert sm.dispatch_pause_reason is None
        assert supervisor._startup_paused is False

        # Verify a start_paused_cleared event was recorded
        events = get_events(supervisor._conn, event_type="start_paused_cleared")
        assert len(events) >= 1
        assert "CLI resume" in events[-1]["detail"]


# ---------------------------------------------------------------------------
# Stop slot
# ---------------------------------------------------------------------------


class TestStopSlot:
    """Tests for Supervisor.stop_slot() and related helpers."""

    def test_stop_free_slot_returns_error(self, supervisor):
        """Stopping a free slot returns an error result."""
        result = supervisor.stop_slot("test-project", 1)
        assert result.success is False
        assert "not busy" in result.message

    def test_stop_nonexistent_slot_returns_error(self, supervisor):
        """Stopping a slot that doesn't exist returns an error result."""
        result = supervisor.stop_slot("test-project", 99)
        assert result.success is False
        assert "not found" in result.message

    def test_stop_busy_slot_no_pr(self, supervisor):
        """Stopping a busy slot with no PR: kills worker, cleans git, moves ticket."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        # Mock out external calls
        with (
            patch.object(supervisor, "_stop_slot_kill_worker") as mock_kill,
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True) as mock_git,
            patch.object(supervisor, "_stop_slot_linear_cleanup") as mock_linear,
        ):
            result = supervisor.stop_slot("test-project", 1)

        assert result.success is True
        assert result.pr_was_merged is False
        assert result.pr_closed is False
        assert result.ticket_id == "TST-1"
        mock_kill.assert_called_once()
        mock_git.assert_called_once_with("test-project", 1, "feat-1")
        mock_linear.assert_called_once()

        # Slot should be freed
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "free"

    def test_stop_busy_slot_with_open_pr(self, supervisor):
        """Stopping a busy slot with an open PR closes the PR."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        sm.set_pr_url("test-project", 1, "https://github.com/org/repo/pull/1")

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch("subprocess.run") as mock_run,
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_cleanup"),
        ):
            mock_run.return_value = MagicMock(returncode=0)
            result = supervisor.stop_slot("test-project", 1)

        assert result.success is True
        assert result.pr_closed is True
        assert result.pr_was_merged is False

    def test_stop_busy_slot_with_merged_pr(self, supervisor):
        """Stopping a slot with a merged PR runs completion cleanup, not failure cleanup."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_cleanup") as mock_linear,
            patch.object(supervisor, "_stop_slot_linear_done") as mock_done,
        ):
            result = supervisor.stop_slot("test-project", 1)

        assert result.pr_was_merged is True
        assert result.pr_closed is False
        # Failure cleanup should be skipped for merged PRs
        mock_linear.assert_not_called()
        # Completion cleanup should run instead
        mock_done.assert_called_once()

    def test_stop_merged_pr_marks_task_completed(self, supervisor):
        """Stopping a slot with merged PR marks task as completed, not failed."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
        )
        supervisor._conn.commit()

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_done"),
        ):
            result = supervisor.stop_slot("test-project", 1)

        assert result.pr_was_merged is True
        task = get_task(supervisor._conn, task_id)
        assert task["status"] == "completed"  # marked completed, not failed
        assert task["completed_at"] is not None  # timestamp must be set

    def test_stop_slot_updates_db_task(self, supervisor):
        """Stopping a slot marks the DB task as failed with 'stopped by user'."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        # Insert a task record
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
        )
        supervisor._conn.commit()

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_cleanup"),
        ):
            supervisor.stop_slot("test-project", 1)

        task = get_task(supervisor._conn, task_id)
        assert task["status"] == "failed"
        assert task["failure_reason"] == "stopped by user"

    def test_stop_slot_inserts_event(self, supervisor):
        """Stopping a slot inserts a slot_stopped event."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_cleanup"),
        ):
            supervisor.stop_slot("test-project", 1)

        events = get_events(supervisor._conn, event_type="slot_stopped")
        assert len(events) == 1
        assert "TST-1" in events[0]["detail"]

    def test_stop_paused_manual_slot(self, supervisor):
        """Stopping a manually paused slot works (no PID to kill)."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        sm.mark_paused_manual("test-project", 1)

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_cleanup"),
        ):
            result = supervisor.stop_slot("test-project", 1)

        assert result.success is True
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "free"

    def test_stop_paused_limit_slot(self, supervisor):
        """Stopping a limit-paused slot works."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        sm.mark_paused_limit("test-project", 1, resume_after="2099-01-01T00:00:00Z")

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_cleanup"),
        ):
            result = supervisor.stop_slot("test-project", 1)

        assert result.success is True
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "free"

    def test_stop_slot_cleans_pending_result_texts(self, supervisor):
        """Stopping a slot removes any pending result text."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        supervisor._pending_result_texts[("test-project", 1)] = "some output"

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=True),
            patch.object(supervisor, "_stop_slot_linear_cleanup"),
        ):
            supervisor.stop_slot("test-project", 1)

        assert ("test-project", 1) not in supervisor._pending_result_texts


class TestStopSlotGitCleanup:
    """Tests for _stop_slot_git_cleanup."""

    def test_git_cleanup_runs_commands_in_order(self, supervisor):
        """Git cleanup runs reset, clean, checkout, branch -D, push --delete."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = supervisor._stop_slot_git_cleanup("test-project", 1, "feat-1")

        assert result is True
        commands = [c[0][0] for c in mock_run.call_args_list]
        assert commands[0] == ["git", "reset", "--hard"]
        assert commands[1] == ["git", "clean", "-fd"]
        assert commands[2] == ["git", "checkout", "slot-1-placeholder"]
        assert commands[3] == ["git", "branch", "-D", "feat-1"]
        assert commands[4] == ["git", "push", "origin", "--delete", "feat-1"]

    def test_git_cleanup_skips_protected_branch(self, supervisor):
        """Git cleanup does not delete protected branches (main, placeholder)."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            supervisor._stop_slot_git_cleanup("test-project", 1, "main")

        commands = [c[0][0] for c in mock_run.call_args_list]
        # Should only have reset, clean, checkout — no branch -D or push --delete
        assert len(commands) == 3
        assert commands[0][1] == "reset"
        assert commands[1][1] == "clean"
        assert commands[2][1] == "checkout"

    def test_git_cleanup_skips_none_branch(self, supervisor):
        """Git cleanup handles None branch (worker crashed early)."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            supervisor._stop_slot_git_cleanup("test-project", 1, None)

        commands = [c[0][0] for c in mock_run.call_args_list]
        assert len(commands) == 3  # checkout, clean, reset only

    def test_git_cleanup_ignores_push_delete_error(self, supervisor):
        """Remote branch deletion failure doesn't raise."""
        def side_effect(cmd, **kwargs):
            result = MagicMock(returncode=0)
            if cmd[1] == "push":
                raise subprocess.CalledProcessError(1, cmd)
            return result

        import subprocess as sp
        with patch("subprocess.run", side_effect=side_effect):
            # Should not raise
            supervisor._stop_slot_git_cleanup("test-project", 1, "feat-1")


class TestStopSlotLinearCleanup:
    """Tests for _stop_slot_linear_cleanup."""

    def test_moves_ticket_to_todo(self, supervisor):
        """Linear cleanup moves the ticket to todo status."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)

        poller = supervisor._pollers["test-project"]
        supervisor._stop_slot_linear_cleanup(slot)

        poller.move_issue.assert_called_once_with("TST-1", "Todo")

    def test_posts_stop_comment(self, supervisor):
        """Linear cleanup posts a 'Stopped by user' comment."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)

        poller = supervisor._pollers["test-project"]
        supervisor._stop_slot_linear_cleanup(slot)

        poller.add_comment.assert_called_once()
        comment_body = poller.add_comment.call_args[0][1]
        assert "Stopped by user" in comment_body

    def test_no_ticket_id_skips_cleanup(self, supervisor):
        """Linear cleanup is a no-op when ticket_id is None."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.ticket_id = None

        poller = supervisor._pollers["test-project"]
        supervisor._stop_slot_linear_cleanup(slot)

        poller.move_issue.assert_not_called()
        poller.add_comment.assert_not_called()


class TestStopSlotKillWorker:
    """Tests for _stop_slot_kill_worker."""

    def test_kill_worker_sends_sigterm_then_joins(self, supervisor):
        """Kill sends SIGTERM to process group, waits, and removes process."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 12345

        mock_proc = MagicMock()
        mock_proc.is_alive.return_value = False  # exits after join
        supervisor._workers[("test-project", 1)] = mock_proc

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=True),
            patch("os.killpg") as mock_killpg,
        ):
            supervisor._stop_slot_kill_worker(slot)

        mock_killpg.assert_called_once_with(12345, signal.SIGTERM)
        mock_proc.join.assert_called()
        assert ("test-project", 1) not in supervisor._workers

    def test_kill_worker_escalates_to_sigkill_tree(self, supervisor):
        """If worker doesn't exit after grace period, kills entire process tree."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 12345

        mock_proc = MagicMock()
        # First join returns but process is still alive, then second join works
        mock_proc.is_alive.return_value = True
        supervisor._workers[("test-project", 1)] = mock_proc

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=True),
            patch("os.killpg") as mock_killpg,
            patch("botfarm.supervisor._kill_process_tree") as mock_kill_tree,
        ):
            supervisor._stop_slot_kill_worker(slot)

        # SIGTERM to process group, then _kill_process_tree for escalation
        mock_killpg.assert_called_once_with(12345, signal.SIGTERM)
        mock_kill_tree.assert_called_once_with(12345)

    def test_kill_worker_skips_dead_pid(self, supervisor):
        """If PID is already dead, skips kill and just cleans up."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = 12345

        with (
            patch("botfarm.supervisor._is_pid_alive", return_value=False),
            patch("os.kill") as mock_kill,
        ):
            supervisor._stop_slot_kill_worker(slot)

        mock_kill.assert_not_called()

    def test_kill_worker_no_pid(self, supervisor):
        """If no PID (e.g. paused slot), skips kill."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = None

        with patch("os.kill") as mock_kill:
            supervisor._stop_slot_kill_worker(slot)

        mock_kill.assert_not_called()

    def test_kill_worker_cleans_pause_event(self, supervisor):
        """Kill removes the pause event for the slot."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.pid = None

        import multiprocessing
        supervisor._pause_events[("test-project", 1)] = multiprocessing.Event()

        with patch("os.kill"):
            supervisor._stop_slot_kill_worker(slot)

        assert ("test-project", 1) not in supervisor._pause_events


class TestStopSlotPrCleanup:
    """Tests for _stop_slot_pr_cleanup."""

    def test_no_pr_returns_false_false(self, supervisor):
        """No PR → (False, False)."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            merged, closed = supervisor._stop_slot_pr_cleanup(slot)

        assert merged is False
        assert closed is False

    def test_merged_pr_returns_true_false(self, supervisor):
        """Merged PR → (True, False) — no close attempt."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", "https://github.com/test/repo/pull/1")):
            merged, closed = supervisor._stop_slot_pr_cleanup(slot)

        assert merged is True
        assert closed is False

    def test_open_pr_closes_it(self, supervisor):
        """Open PR gets closed via gh pr close."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        sm.set_pr_url("test-project", 1, "https://github.com/org/repo/pull/1")
        slot = sm.get_slot("test-project", 1)

        with (
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            merged, closed = supervisor._stop_slot_pr_cleanup(slot)

        assert merged is False
        assert closed is True
        # Verify gh pr close was called without --delete-branch
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert cmd[0:3] == ["gh", "pr", "close"]
        assert "--delete-branch" not in cmd

    def test_open_pr_close_failure_returns_false(self, supervisor):
        """When gh pr close fails, pr_closed is False."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        sm.set_pr_url("test-project", 1, "https://github.com/org/repo/pull/1")
        slot = sm.get_slot("test-project", 1)

        with (
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch("subprocess.run") as mock_run,
        ):
            mock_run.return_value = MagicMock(returncode=1, stderr="error")
            merged, closed = supervisor._stop_slot_pr_cleanup(slot)

        assert merged is False
        assert closed is False

    def test_open_pr_close_exception_returns_false(self, supervisor):
        """When gh pr close throws, pr_closed is False."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        sm.set_pr_url("test-project", 1, "https://github.com/org/repo/pull/1")
        slot = sm.get_slot("test-project", 1)

        with (
            patch.object(supervisor, "_check_pr_status", return_value=("open", "https://github.com/test/repo/pull/1")),
            patch("subprocess.run", side_effect=OSError("timeout")),
        ):
            merged, closed = supervisor._stop_slot_pr_cleanup(slot)

        assert merged is False
        assert closed is False

    def test_closed_pr_returns_false_false(self, supervisor):
        """Already-closed PR → (False, False)."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        slot = sm.get_slot("test-project", 1)

        with patch.object(supervisor, "_check_pr_status", return_value=("closed", "https://github.com/test/repo/pull/1")):
            merged, closed = supervisor._stop_slot_pr_cleanup(slot)

        assert merged is False
        assert closed is False


class TestRequestStopSlot:
    """Tests for the thread-safe request_stop_slot / _handle_stop_requests."""

    def test_request_stop_slot_appends_and_wakes(self, supervisor):
        """request_stop_slot adds to the list and sets wake event."""
        # Assign a ticket so the request captures ticket_id
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        supervisor.request_stop_slot("test-project", 1)

        with supervisor._stop_slot_lock:
            assert len(supervisor._stop_slot_requests) == 1
            proj, sid, tid = supervisor._stop_slot_requests[0]
            assert proj == "test-project"
            assert sid == 1
            assert tid == "TST-1"
        assert supervisor._wake_event.is_set()

    def test_handle_stop_requests_drains_list(self, supervisor):
        """_handle_stop_requests processes all pending requests."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        supervisor.request_stop_slot("test-project", 1)

        with (
            patch.object(supervisor, "stop_slot", return_value=StopSlotResult(
                success=True, message="ok",
            )) as mock_stop,
        ):
            supervisor._handle_stop_requests()

        mock_stop.assert_called_once_with("test-project", 1)

        # List should be drained
        with supervisor._stop_slot_lock:
            assert len(supervisor._stop_slot_requests) == 0

    def test_handle_stop_requests_no_requests_is_noop(self, supervisor):
        """_handle_stop_requests is a no-op when there are no requests."""
        with patch.object(supervisor, "stop_slot") as mock_stop:
            supervisor._handle_stop_requests()
        mock_stop.assert_not_called()

    def test_handle_stop_requests_in_tick(self, supervisor):
        """_handle_stop_requests is called during _tick."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        supervisor.request_stop_slot("test-project", 1)

        with (
            patch.object(supervisor, "stop_slot", return_value=StopSlotResult(
                success=True, message="ok",
            )) as mock_stop,
            patch.object(supervisor, "_reconcile_workers"),
            patch.object(supervisor, "_handle_manual_pause_resume"),
            patch.object(supervisor, "_handle_update_request"),
            patch.object(supervisor, "_check_timeouts"),
            patch.object(supervisor, "_handle_finished_slots"),
            patch.object(supervisor, "_handle_paused_slots"),
            patch.object(supervisor, "_poll_usage"),
            patch.object(supervisor, "_poll_capacity"),
            patch.object(supervisor, "_poll_and_dispatch"),
            patch.object(supervisor, "_cleanup_old_ticket_logs"),
            patch.object(supervisor._slot_manager, "refresh_stages_from_disk"),
            patch.object(supervisor._slot_manager, "refresh_project_pauses"),
        ):
            supervisor._tick()

        mock_stop.assert_called_once_with("test-project", 1)

    def test_handle_stop_skips_when_ticket_reassigned(self, supervisor):
        """Stop is skipped when the slot's ticket changed between request and processing."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        # Queue the stop request (captures expected_ticket=TST-1)
        supervisor.request_stop_slot("test-project", 1)

        # Simulate slot being freed and reassigned before processing
        sm.free_slot("test-project", 1)
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-2", ticket_title="New", branch="feat-2",
        )

        with patch.object(supervisor, "stop_slot") as mock_stop:
            supervisor._handle_stop_requests()

        # stop_slot should NOT be called — the ticket was reassigned
        mock_stop.assert_not_called()

    def test_handle_stop_skips_when_free_slot_reassigned(self, supervisor):
        """Stop is skipped when a free slot (expected_ticket=None) gets reassigned."""
        # Slot starts free — request_stop_slot captures expected_ticket=None
        supervisor.request_stop_slot("test-project", 1)

        # Slot gets assigned before the tick processes the stop
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        with patch.object(supervisor, "stop_slot") as mock_stop:
            supervisor._handle_stop_requests()

        # stop_slot should NOT be called — ticket_id changed from None to TST-1
        mock_stop.assert_not_called()


class TestDrainResultsForSlot:
    """Tests for _drain_results_for_slot."""

    def test_discards_results_for_target_slot(self, supervisor):
        """Results for the stopped slot are discarded."""
        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, success=True,
        ))
        supervisor._drain_results_for_slot("test-project", 1)

        # Queue should be empty
        import queue as queue_mod
        with pytest.raises(queue_mod.Empty):
            supervisor._result_queue.get(timeout=0.05)

    def test_preserves_results_for_other_slots(self, supervisor):
        """Results for other slots are put back."""
        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=2, success=True,
        ))
        supervisor._drain_results_for_slot("test-project", 1)

        # Should still have the result for slot 2
        wr = supervisor._result_queue.get(timeout=0.1)
        assert wr.slot_id == 2


class TestStaleResultRejection:
    """Regression test: stale results from a stopped worker must not affect a new ticket."""

    def test_stale_result_ignored_after_stop_and_reassign(self, supervisor):
        """A late-arriving result for old ticket is ignored when slot has a new ticket."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="NEW-1", ticket_title="New ticket", branch="feat-new",
        )

        # Enqueue a stale result from the old ticket
        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, ticket_id="OLD-1",
            success=False,
            failure_stage="implement", failure_reason="crash",
        ))

        supervisor._reconcile_workers()

        # Slot should still be busy with NEW-1 (not marked failed)
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "busy"
        assert slot.ticket_id == "NEW-1"

    def test_stale_result_ignored_for_free_slot(self, supervisor):
        """A late-arriving result is ignored when slot has been freed (no ticket)."""
        sm = supervisor.slot_manager
        # Slot starts free (default)
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "free"
        assert slot.ticket_id is None

        # Enqueue a stale result from a previously-stopped ticket
        supervisor._result_queue.put(_WorkerResult(
            project="test-project", slot_id=1, ticket_id="OLD-1",
            success=False,
            failure_stage="implement", failure_reason="crash",
        ))

        supervisor._reconcile_workers()

        # Slot should still be free (not marked failed)
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "free"

    def test_stop_completed_pending_cleanup_slot_returns_error(self, supervisor):
        """Stopping a completed_pending_cleanup slot returns an error."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )
        sm.mark_completed("test-project", 1)
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "completed_pending_cleanup"

        result = supervisor.stop_slot("test-project", 1)
        assert result.success is False
        assert "completed work" in result.message

    def test_git_cleanup_skips_branch_delete_on_checkout_failure(self, supervisor):
        """Branch deletion is skipped when checkout to placeholder fails."""
        def side_effect(cmd, **kwargs):
            result = MagicMock(returncode=0)
            if cmd[1] == "checkout":
                result.returncode = 1  # checkout fails
            return result

        with patch("subprocess.run", side_effect=side_effect):
            checkout_ok = supervisor._stop_slot_git_cleanup("test-project", 1, "feat-1")

        assert checkout_ok is False
        # Should not raise; branch -D and push --delete should not be called

    def test_stop_slot_marks_failed_on_checkout_failure(self, supervisor):
        """stop_slot marks slot as failed (not freed) when checkout to placeholder fails."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="feat-1",
        )

        with (
            patch.object(supervisor, "_stop_slot_kill_worker"),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
            patch.object(supervisor, "_stop_slot_git_cleanup", return_value=False),
        ):
            result = supervisor.stop_slot("test-project", 1)

        assert result.success is False
        assert "dirty" in result.message.lower() or "checkout" in result.message.lower()
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "failed"


# ---------------------------------------------------------------------------
# Refactoring analysis notification integration
# ---------------------------------------------------------------------------


class TestRefactoringAnalysisNotification:
    """Tests for _maybe_send_refactoring_notification on completed slots."""

    def test_skipped_when_no_refactoring_label(self, supervisor):
        """No refactoring notification when ticket lacks the label."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Bug"]

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_not_called()
        supervisor._notifier.notify_refactoring_action_needed.assert_not_called()

    def test_skipped_when_no_labels(self, supervisor):
        """No refactoring notification when ticket has no labels."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = []

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_not_called()
        supervisor._notifier.notify_refactoring_action_needed.assert_not_called()

    def test_all_clear_when_no_ticket_creation_signal(self, supervisor):
        """Sends all_clear when result text has no ticket creation signals."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Analysis complete. Code quality is acceptable."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_called_once()
        kwargs = supervisor._notifier.notify_refactoring_all_clear.call_args[1]
        assert kwargs["linear_ticket_url"] == "https://linear.app//issue/SMA-50"
        supervisor._notifier.notify_refactoring_action_needed.assert_not_called()

    def test_action_needed_when_ticket_creation_detected(self, supervisor):
        """Sends action_needed when result text mentions ticket creation."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Created 3 refactoring tickets under SMA-50. "
            "Top concerns: duplicated auth logic, oversized utils module."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_action_needed.assert_called_once()
        kwargs = supervisor._notifier.notify_refactoring_action_needed.call_args[1]
        assert kwargs["num_tickets"] == 3
        assert kwargs["parent_ticket_id"] == "SMA-50"
        assert "duplicated auth logic" in kwargs["brief_list"]
        supervisor._notifier.notify_refactoring_all_clear.assert_not_called()

    def test_label_matching_is_case_insensitive(self, supervisor):
        """Label matching works regardless of case."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["refactoring analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Analysis complete. No action needed."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_called_once()

    def test_skips_notification_when_no_result_text(self, supervisor):
        """Skips notification when no result text is available."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        # No entry in _pending_result_texts

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_not_called()
        supervisor._notifier.notify_refactoring_action_needed.assert_not_called()

    def test_action_needed_falls_back_to_ticket_id_as_parent(self, supervisor):
        """Uses the ticket_id as parent when no ticket ID found in text."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Created 2 refactoring tickets."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        kwargs = supervisor._notifier.notify_refactoring_action_needed.call_args[1]
        assert kwargs["parent_ticket_id"] == "SMA-50"
        assert kwargs["brief_list"] == "see ticket for details"

    def test_zero_tickets_sends_all_clear(self, supervisor):
        """Treats '0 refactoring tickets' as all-clear, not action-needed."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Found 0 refactoring tickets needed. No action needed."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_called_once()
        supervisor._notifier.notify_refactoring_action_needed.assert_not_called()

    def test_skips_notification_when_unclassifiable(self, supervisor):
        """Skips notification when result text can't be classified."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Analysis complete. Some findings documented."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_not_called()
        supervisor._notifier.notify_refactoring_action_needed.assert_not_called()
