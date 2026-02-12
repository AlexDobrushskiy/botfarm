"""Tests for botfarm.supervisor — daemon loop, dispatch, signal handling."""

from __future__ import annotations

import logging
import signal
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from botfarm.config import (
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
    _worker_entry,
    setup_logging,
)


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
