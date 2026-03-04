"""Tests for worker entry, logging, worktrees, stop-slot operations, and refactoring analysis."""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import signal
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import ProjectConfig
from botfarm.db import get_events, get_task, get_ticket_history_entry, init_db, insert_task, update_task
from botfarm.linear import PollResult
from botfarm.supervisor import (
    StopSlotResult,
    Supervisor,
    _WorkerResult,
    _setup_worker_logging,
    _worker_entry,
    setup_logging,
)
from botfarm.worker import STAGES
from tests.helpers import make_config, make_issue



# ---------------------------------------------------------------------------
# Worker entry point
# ---------------------------------------------------------------------------


class TestWorkerEntry:
    @patch("botfarm.supervisor_workers.run_pipeline")
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

        with patch("botfarm.supervisor_workers.signal.signal") as mock_signal, \
             patch("botfarm.supervisor_workers.os.setpgrp") as mock_setpgrp:
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

    @patch("botfarm.supervisor_workers.run_pipeline")
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

    @patch("botfarm.supervisor_workers.run_pipeline")
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

    @patch("botfarm.supervisor_workers.run_pipeline")
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


class TestNextStageAfterWorker:
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
            patch("botfarm.supervisor_ops._is_pid_alive", return_value=True),
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
            patch("botfarm.supervisor_ops._is_pid_alive", return_value=True),
            patch("os.killpg") as mock_killpg,
            patch("botfarm.supervisor_ops._kill_process_tree") as mock_kill_tree,
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
            patch("botfarm.supervisor_ops._is_pid_alive", return_value=False),
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
        assert kwargs["linear_ticket_url"] == "SMA-50"
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

    def test_negative_code_quality_does_not_send_all_clear(self, supervisor):
        """'code quality is poor' should not trigger an all-clear notification."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Analysis complete. Code quality is poor."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_not_called()
        supervisor._notifier.notify_refactoring_action_needed.assert_not_called()

    def test_parent_id_extracted_from_under_pattern(self, supervisor):
        """Parent ID is extracted from 'under X-123', not the first ticket ID."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        supervisor._pending_result_texts[("test-project", 1)] = (
            "Created SMA-101, SMA-102. "
            "Created 2 refactoring tickets under SMA-50."
        )

        supervisor._maybe_send_refactoring_notification(slot)

        kwargs = supervisor._notifier.notify_refactoring_action_needed.call_args[1]
        assert kwargs["parent_ticket_id"] == "SMA-50"

    def test_falls_back_to_db_result_text_on_restart(self, supervisor):
        """After a restart, result_text is read from DB when not in memory."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]

        # Persist result_text to DB (simulating what _reconcile_workers does)
        task_id = insert_task(
            supervisor._conn,
            ticket_id="SMA-50",
            title="Refactoring analysis",
            project="test-project",
            slot=1,
            status="completed",
        )
        update_task(supervisor._conn, task_id, result_text="No action needed.")
        supervisor._conn.commit()

        # _pending_result_texts is empty (simulating supervisor restart)
        assert (slot.project, slot.slot_id) not in supervisor._pending_result_texts

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_all_clear.assert_called_once()

    def test_falls_back_to_db_action_needed_on_restart(self, supervisor):
        """After a restart, action-needed is correctly classified from DB."""
        supervisor._notifier = MagicMock()
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]

        task_id = insert_task(
            supervisor._conn,
            ticket_id="SMA-50",
            title="Refactoring analysis",
            project="test-project",
            slot=1,
            status="completed",
        )
        update_task(
            supervisor._conn, task_id,
            result_text="Created 3 refactoring tickets under SMA-50. "
                        "Top concerns: dead code, oversized modules.",
        )
        supervisor._conn.commit()

        # No in-memory result text
        assert (slot.project, slot.slot_id) not in supervisor._pending_result_texts

        supervisor._maybe_send_refactoring_notification(slot)

        supervisor._notifier.notify_refactoring_action_needed.assert_called_once()
        kwargs = supervisor._notifier.notify_refactoring_action_needed.call_args[1]
        assert kwargs["num_tickets"] == 3

    def test_custom_label_triggers_notification(self, tmp_path, monkeypatch):
        """Notification fires when ticket has a non-default custom label."""
        from botfarm.config import RefactoringAnalysisConfig

        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = make_config(tmp_path)
        config.refactoring_analysis = RefactoringAnalysisConfig(
            enabled=True,
            linear_label="My Custom Label",
        )
        (tmp_path / "repo").mkdir(exist_ok=True)
        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(
            candidates=[], blocked=[], auto_close_parents=[]
        )
        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        sup._notifier = MagicMock()
        slot = sup.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["My Custom Label"]
        sup._pending_result_texts[("test-project", 1)] = "No action needed."

        sup._maybe_send_refactoring_notification(slot)

        sup._notifier.notify_refactoring_all_clear.assert_called_once()

    def test_custom_label_skips_default_label(self, tmp_path, monkeypatch):
        """Default label 'Refactoring Analysis' doesn't trigger when custom label set."""
        from botfarm.config import RefactoringAnalysisConfig

        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        config = make_config(tmp_path)
        config.refactoring_analysis = RefactoringAnalysisConfig(
            enabled=True,
            linear_label="My Custom Label",
        )
        (tmp_path / "repo").mkdir(exist_ok=True)
        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.poll.return_value = PollResult(
            candidates=[], blocked=[], auto_close_parents=[]
        )
        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            sup = Supervisor(config, log_dir=tmp_path / "logs")

        sup._notifier = MagicMock()
        slot = sup.slot_manager.get_slot("test-project", 1)
        slot.ticket_id = "SMA-50"
        slot.ticket_labels = ["Refactoring Analysis"]
        sup._pending_result_texts[("test-project", 1)] = "No action needed."

        sup._maybe_send_refactoring_notification(slot)

        sup._notifier.notify_refactoring_all_clear.assert_not_called()


# ---------------------------------------------------------------------------
# Base dir verification
# ---------------------------------------------------------------------------


class TestVerifyBaseDirClean:
    """Tests for _verify_base_dir_clean() guardrail."""

    def test_resets_base_dir_to_main(self, supervisor, tmp_path):
        """If the base dir is on a feature branch, reset it to main."""
        base_dir = tmp_path / "repo"
        base_dir.mkdir(exist_ok=True)
        # Initialize a git repo so git branch --show-current works
        import subprocess
        subprocess.run(["git", "init"], cwd=str(base_dir), capture_output=True)
        subprocess.run(["git", "checkout", "-b", "main"], cwd=str(base_dir), capture_output=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=str(base_dir), capture_output=True)
        subprocess.run(["git", "checkout", "-b", "feature-branch"], cwd=str(base_dir), capture_output=True)

        supervisor._verify_base_dir_clean("test-project")

        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=str(base_dir), capture_output=True, text=True,
        )
        assert result.stdout.strip() == "main"

        # Verify event was logged
        events = get_events(supervisor._conn)
        base_dir_events = [e for e in events if e["event_type"] == "base_dir_reset"]
        assert len(base_dir_events) == 1
        assert "was_on=feature-branch" in base_dir_events[0]["detail"]

    def test_no_reset_when_on_main(self, supervisor, tmp_path):
        """No reset or event when base dir is already on main."""
        base_dir = tmp_path / "repo"
        base_dir.mkdir(exist_ok=True)
        import subprocess
        subprocess.run(["git", "init"], cwd=str(base_dir), capture_output=True)
        subprocess.run(["git", "checkout", "-b", "main"], cwd=str(base_dir), capture_output=True)
        subprocess.run(["git", "commit", "--allow-empty", "-m", "init"], cwd=str(base_dir), capture_output=True)

        supervisor._verify_base_dir_clean("test-project")

        events = get_events(supervisor._conn)
        base_dir_events = [e for e in events if e["event_type"] == "base_dir_reset"]
        assert len(base_dir_events) == 0

    def test_unknown_project_is_noop(self, supervisor):
        """Unknown project name should not raise."""
        supervisor._verify_base_dir_clean("nonexistent-project")

    def test_missing_base_dir_is_noop(self, supervisor, tmp_path):
        """Missing base dir path should not raise."""
        # Remove the base dir
        base_dir = tmp_path / "repo"
        if base_dir.exists():
            import shutil
            shutil.rmtree(base_dir)
        supervisor._verify_base_dir_clean("test-project")
