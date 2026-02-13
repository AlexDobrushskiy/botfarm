"""Tests for botfarm.supervisor — daemon loop, dispatch, signal handling."""

from __future__ import annotations

import logging
import signal
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
from botfarm.db import get_events, get_task, init_db, insert_task
from botfarm.linear import LinearIssue
from botfarm.supervisor import (
    DEFAULT_LOG_DIR,
    Supervisor,
    _WorkerResult,
    _check_limit_hit,
    _worker_entry,
    setup_logging,
)
from botfarm.worker import PipelineResult


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
        max_total_slots=5,
        linear=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=10,
            exclude_tags=["Human"],
        ),
        database=DatabaseConfig(path=str(tmp_path / "test.db")),
        state_file=str(tmp_path / "state.json"),
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
def supervisor(tmp_config, tmp_path):
    """Create a Supervisor with mocked pollers (no real Linear calls)."""
    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.poll.return_value = []

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(tmp_config, log_dir=tmp_path / "logs")

    return sup


@pytest.fixture()
def conn(tmp_path):
    db_file = tmp_path / "test.db"
    connection = init_db(db_file)
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

        supervisor._handle_finished_slots()

        poller.move_issue.assert_called_once_with("TST-1", "In Review")
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

    def test_events_recorded_for_completed(self, supervisor):
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.mark_completed("test-project", 1)

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
        poller.poll.return_value = [_make_issue()]

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
        poller.poll.return_value = [_make_issue()]

        with patch.object(supervisor, "_dispatch_worker"):
            supervisor._poll_and_dispatch()

        assert supervisor.slot_manager.dispatch_paused is False

    def test_none_utilization_allows_dispatch(self, supervisor):
        """When utilization is None, dispatch proceeds normally."""
        supervisor._usage_poller._state.utilization_5h = None
        supervisor._usage_poller._state.utilization_7d = None

        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = [_make_issue()]

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_called_once()


class TestPollAndDispatch:
    def test_no_candidates_no_dispatch(self, supervisor):
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = []

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
        poller.poll.return_value = [_make_issue()]

        with patch.object(supervisor, "_dispatch_worker") as mock_dispatch:
            supervisor._poll_and_dispatch()
            mock_dispatch.assert_not_called()

    def test_dispatches_to_free_slot(self, supervisor):
        issue = _make_issue()
        poller = supervisor._pollers["test-project"]
        poller.poll.return_value = [issue]

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
        ):
            supervisor.run()

        assert tick_count == 2
        # Sleep called between iterations (once, since shutdown on 2nd tick)
        assert mock_sleep.call_count == 1

    def test_run_records_start_and_stop_events(self, supervisor, tmp_path):
        supervisor._shutdown_requested = True

        with patch.object(supervisor, "install_signal_handlers"):
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

        # State file should exist and have the busy slot
        import json
        data = json.loads(sm.state_path.read_text())
        busy = [d for d in data["slots"] if d["status"] == "busy"]
        assert len(busy) == 1

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
    def test_successful_pipeline_sends_success(self, mock_pipeline, tmp_path):
        import multiprocessing
        db_path = str(tmp_path / "test.db")
        q = multiprocessing.Queue()

        conn = init_db(db_path)
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
        mock_pipeline.return_value = mock_result

        _worker_entry(
            ticket_id="TST-1",
            ticket_title="Test",
            task_id=task_id,
            project_name="proj",
            slot_id=1,
            cwd=str(tmp_path),
            db_path=db_path,
            result_queue=q,
            max_turns=None,
        )

        wr = q.get(timeout=1)
        assert isinstance(wr, _WorkerResult)
        assert wr.success is True
        assert wr.project == "proj"
        assert wr.slot_id == 1

    @patch("botfarm.supervisor.run_pipeline")
    def test_failed_pipeline_sends_failure(self, mock_pipeline, tmp_path):
        import multiprocessing
        db_path = str(tmp_path / "test.db")
        q = multiprocessing.Queue()

        conn = init_db(db_path)
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
            db_path=db_path,
            result_queue=q,
            max_turns=None,
        )

        wr = q.get(timeout=1)
        assert isinstance(wr, _WorkerResult)
        assert wr.success is False
        assert wr.failure_stage == "implement"

    @patch("botfarm.supervisor.run_pipeline")
    def test_exception_sends_failure(self, mock_pipeline, tmp_path):
        import multiprocessing
        db_path = str(tmp_path / "test.db")
        q = multiprocessing.Queue()

        conn = init_db(db_path)
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
            db_path=db_path,
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
                isinstance(h, logging.FileHandler) for h in root.handlers
            )
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

        supervisor._reconcile_workers()

        slot = sm.get_slot("test-project", 1)
        assert slot.resume_after is not None
        # Should be 2 minutes after resets_at
        assert "22:02" in slot.resume_after


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

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
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

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
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

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
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

        with patch("botfarm.supervisor.multiprocessing.Process") as MockProc:
            mock_proc = MagicMock()
            mock_proc.pid = 555
            MockProc.return_value = mock_proc

            supervisor._handle_paused_slots()

            mock_proc.start.assert_called_once()

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
    def test_dispatches_across_projects(self, tmp_path):
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
            max_total_slots=5,
            linear=LinearConfig(
                api_key="test-key",
                poll_interval_seconds=10,
            ),
            database=DatabaseConfig(path=str(tmp_path / "test.db")),
            state_file=str(tmp_path / "state.json"),
        )
        (tmp_path / "alpha").mkdir()
        (tmp_path / "beta").mkdir()

        mock_alpha = MagicMock()
        mock_alpha.project_name = "alpha"
        mock_alpha.poll.return_value = [_make_issue(id="a1", identifier="A-1")]

        mock_beta = MagicMock()
        mock_beta.project_name = "beta"
        mock_beta.poll.return_value = [_make_issue(id="b1", identifier="B-1")]

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

    def test_timeout_kills_worker_and_marks_failed(self, supervisor):
        """Worker that exceeds timeout is killed and slot marked failed."""
        now = datetime.now(timezone.utc)
        # Started 130 minutes ago, limit is 120 minutes for implement
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor.os.kill") as mock_kill,
            patch("botfarm.supervisor.time.sleep"),
        ):
            # After SIGTERM, process is dead (os.kill(pid, 0) raises)
            mock_kill.side_effect = [
                None,           # SIGTERM succeeds
                ProcessLookupError,  # pid check — process is gone
            ]

            supervisor._check_timeouts()

        assert supervisor.slot_manager.get_slot("test-project", 1).status == "failed"

    def test_timeout_sends_sigterm_then_sigkill(self, supervisor):
        """When SIGTERM doesn't work, SIGKILL is sent after grace period."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        with (
            patch("botfarm.supervisor.os.kill") as mock_kill,
            patch("botfarm.supervisor.time.sleep") as mock_sleep,
        ):
            # SIGTERM succeeds, process still alive, SIGKILL succeeds
            mock_kill.side_effect = [
                None,  # SIGTERM
                None,  # os.kill(pid, 0) — still alive
                None,  # SIGKILL
            ]

            supervisor._check_timeouts()

        # Verify SIGTERM, check, SIGKILL sequence
        calls = mock_kill.call_args_list
        assert calls[0] == call(99999, signal.SIGTERM)
        assert calls[1] == call(99999, 0)  # alive check
        assert calls[2] == call(99999, signal.SIGKILL)
        # Grace period sleep
        mock_sleep.assert_called_once_with(
            supervisor._config.agents.timeout_grace_seconds,
        )

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

        with (
            patch("botfarm.supervisor.os.kill", side_effect=ProcessLookupError),
            patch("botfarm.supervisor.time.sleep"),
        ):
            supervisor._check_timeouts()

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

        with (
            patch("botfarm.supervisor.os.kill", side_effect=ProcessLookupError),
            patch("botfarm.supervisor.time.sleep"),
        ):
            supervisor._check_timeouts()

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

        with (
            patch("botfarm.supervisor.os.kill", side_effect=ProcessLookupError),
            patch("botfarm.supervisor.time.sleep"),
        ):
            supervisor._check_timeouts()

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

    def test_timeout_with_custom_config(self, tmp_path):
        """Custom timeout_minutes values are respected."""
        from datetime import timedelta

        config = _make_config(tmp_path)
        config.agents.timeout_minutes = {"implement": 10, "review": 5, "fix": 8}
        (tmp_path / "repo").mkdir()

        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = []

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

        with (
            patch("botfarm.supervisor.os.kill", side_effect=ProcessLookupError),
            patch("botfarm.supervisor.time.sleep"),
        ):
            sup._check_timeouts()

        assert sm.get_slot("test-project", 1).status == "failed"
        sup._conn.close()
