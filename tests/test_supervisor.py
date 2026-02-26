"""Tests for botfarm.supervisor — daemon loop, dispatch, signal handling."""

from __future__ import annotations

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
    DatabaseConfig,
    LinearConfig,
    ProjectConfig,
)
from botfarm.db import get_events, get_task, init_db, insert_task, update_task
from botfarm.linear import LinearIssue, PollResult
from botfarm.supervisor import (
    DEFAULT_LOG_DIR,
    Supervisor,
    _WorkerResult,
    _check_limit_hit,
    _setup_worker_logging,
    _worker_entry,
    setup_logging,
)
from botfarm.worker import PipelineResult, STAGES


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_config(tmp_path: Path) -> BotfarmConfig:
    """Build a minimal valid config rooted in tmp_path."""
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
        ),
        database=DatabaseConfig(),
    )


def _make_issue(
    id: str = "issue-uuid-1",
    identifier: str = "TST-1",
    title: str = "Test ticket",
    priority: int = 2,
) -> LinearIssue:
    return LinearIssue(
        id=id,
        identifier=identifier,
        title=title,
        priority=priority,
        url=f"https://linear.app/team/issue/{identifier}",
    )


@pytest.fixture()
def tmp_config(tmp_path):
    """Return a BotfarmConfig and ensure directories exist."""
    config = _make_config(tmp_path)
    (tmp_path / "repo").mkdir()
    return config


@pytest.fixture()
def supervisor(tmp_config, tmp_path, monkeypatch):
    """Create a Supervisor with mocked pollers (no real Linear calls)."""
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

    return sup


@pytest.fixture()
def conn(tmp_path):
    db_file = tmp_path / "test.db"
    connection = init_db(db_file, allow_migration=True)
    yield connection
    connection.close()


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

        with patch.object(supervisor, "_check_pr_status", return_value=None):
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

        with patch.object(supervisor, "_check_pr_status", return_value="merged"):
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

        with patch.object(supervisor, "_check_pr_status", return_value=None):
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

        with patch.object(supervisor, "_check_pr_status", return_value="merged"):
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

        with patch.object(supervisor, "_check_pr_status", return_value=None):
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

        with patch.object(supervisor, "_check_pr_status", return_value="merged"):
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

        with patch.object(supervisor, "_check_pr_status", return_value=None):
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
        poller.poll.return_value = PollResult(candidates=[_make_issue()], blocked=[], auto_close_parents=[])

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
        poller.poll.return_value = PollResult(candidates=[_make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        assert supervisor.slot_manager.dispatch_paused is False

    def test_none_utilization_allows_dispatch(self, supervisor):
        """When utilization is None, dispatch proceeds normally."""
        supervisor._usage_poller._state.utilization_5h = None
        supervisor._usage_poller._state.utilization_7d = None

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = PollResult(candidates=[_make_issue()], blocked=[], auto_close_parents=[])

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
        poller.poll.return_value = PollResult(candidates=[_make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

    def test_dispatches_to_free_slot(self, supervisor):
        issue = _make_issue()
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
        parent_issue = _make_issue(identifier="TST-100", title="Parent")
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
        parent = _make_issue(identifier="TST-100", title="Parent")
        normal = _make_issue(identifier="TST-1", title="Normal")
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
        parent = _make_issue(identifier="TST-100", title="Parent")
        normal = _make_issue(identifier="TST-1", title="Normal")
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
        p1 = _make_issue(identifier="TST-100", title="Parent 1")
        p2 = _make_issue(identifier="TST-200", title="Parent 2")
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
# Dispatch worker
# ---------------------------------------------------------------------------


class TestDispatchWorker:
    def test_creates_task_and_spawns_process(self, supervisor):
        issue = _make_issue()
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
        issue = _make_issue()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        poller = supervisor._pollers["test-project"]
        poller.move_issue.side_effect = Exception("API down")

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            supervisor._dispatch_worker("test-project", slot, issue, poller)
            MockProc.assert_not_called()

        # Slot remains free
        assert supervisor.slot_manager.get_slot("test-project", 1).status == "free"

    def test_worker_dispatched_event_recorded(self, supervisor):
        issue = _make_issue()
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
        issue = _make_issue()
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
        mock_alpha.poll.return_value = PollResult(candidates=[_make_issue(id="a1", identifier="A-1")], blocked=[], auto_close_parents=[])

        mock_beta = MagicMock()
        mock_beta.project_name = "beta"
        mock_beta.poll.return_value = PollResult(candidates=[_make_issue(id="b1", identifier="B-1")], blocked=[], auto_close_parents=[])

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
        config = _make_config(tmp_path)
        config.agents.timeout_minutes = {"implement": 10, "review": 5, "fix": 8}
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])

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
            patch.object(supervisor, "_check_pr_status", return_value=None),
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
            patch.object(supervisor, "_check_pr_status", return_value="merged"),
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
            patch.object(supervisor, "_check_pr_status", return_value="open"),
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
            patch.object(supervisor, "_check_pr_status", return_value="closed"),
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
            patch.object(supervisor, "_check_pr_status", return_value=None),
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
            patch.object(supervisor, "_check_pr_status", return_value=None),
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
            patch.object(supervisor, "_check_pr_status", return_value="open"),
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
            patch.object(supervisor, "_check_pr_status", return_value="open"),
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
            patch.object(supervisor, "_check_pr_status", return_value="open"),
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
            patch.object(supervisor, "_check_pr_status", return_value="merged"),
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
            patch.object(supervisor, "_check_pr_status", return_value="merged"),
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
            patch.object(supervisor, "_check_pr_status", return_value="open"),
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

        assert result == "merged"

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

        assert result == "open"

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

        assert result is None

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

        assert result == "merged"
        # Verify it used the DB URL, not the slot URL
        mock_state.assert_called_once_with(db_pr_url)

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

        assert result == "open"
        mock_state.assert_called_once_with(slot_url)

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

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(config, log_dir=tmp_path / "logs")

    return sup


class TestConfigurableStatusNames:
    def test_dispatch_uses_custom_in_progress_status(self, supervisor_custom_statuses, tmp_path):
        sup = supervisor_custom_statuses
        sm = sup.slot_manager
        poller = sup._pollers["test-project"]
        issue = _make_issue()

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

        with patch.object(sup, "_check_pr_status", return_value=None):
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

        with patch.object(sup, "_check_pr_status", return_value="merged"):
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

        with patch.object(sup, "_check_pr_status", return_value=None):
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

        with patch.object(supervisor, "_check_pr_status", return_value=None):
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

        with patch.object(sup, "_check_pr_status", return_value=None):
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
        poller.add_comment.assert_called_once()
        comment_body = poller.add_comment.call_args[0][1]
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
        poller.add_comment.assert_not_called()

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
        issue = _make_issue()
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
            mock_branch_lookup.assert_called_once_with("b1", expected_cwd)

    def test_dispatch_worker_seeds_slot_db(self, supervisor, tmp_path):
        """_dispatch_worker creates a sandboxed slot DB and passes it to worker."""
        issue = _make_issue()
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
        issue = _make_issue()
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
        poller.poll.return_value = PollResult(candidates=[_make_issue()], blocked=[], auto_close_parents=[])

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

        # Should still be paused
        assert sm.dispatch_paused is True
        assert sm.dispatch_pause_reason == "manual_pause"

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
