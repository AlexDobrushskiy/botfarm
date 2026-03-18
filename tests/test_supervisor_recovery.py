"""Tests for supervisor timeouts, startup recovery, externally-done tickets, and PR status."""

from __future__ import annotations

import os
import signal
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from botfarm.db import get_events, get_task, insert_task, update_task
from botfarm.bugtracker import PollResult
from botfarm.supervisor import Supervisor
from botfarm.supervisor_workers import (
    _collect_descendant_pids,
    _kill_pids,
    _kill_process_tree,
    _worker_entry,
)
from tests.helpers import make_config



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
        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
            supervisor._check_timeouts()  # phase 1: sends SIGTERM

        # Phase 2: simulate grace period elapsed
        slot = supervisor.slot_manager.get_slot("test-project", 1)
        # Set sigterm_sent_at far in the past so grace period has elapsed
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.supervisor_workers.os.kill", side_effect=ProcessLookupError):
            supervisor._check_timeouts()  # phase 2: escalates to SIGKILL

    def test_no_timeout_when_within_limit(self, supervisor):
        """Worker within time limit is not killed."""
        now = datetime.now(timezone.utc)
        # Started 10 minutes ago, limit is 120 minutes
        started = (now - timedelta(minutes=10)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
            supervisor._check_timeouts()
            mock_kill.assert_not_called()

        assert supervisor.slot_manager.get_slot("test-project", 1).status == "busy"

    def test_timeout_sends_sigterm_first(self, supervisor):
        """First call sends SIGTERM and records sigterm_sent_at."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        self._make_busy_slot(supervisor, stage_started_at=started)

        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
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
        with patch("botfarm.supervisor_workers.os.kill"):
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
        with patch("botfarm.supervisor_workers.os.kill"):
            supervisor._check_timeouts()

        # Phase 2: sigterm_sent_at is very recent (just set), grace=10s not elapsed
        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
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

        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
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

        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
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
        with patch("botfarm.supervisor_workers.os.kill"):
            sup._check_timeouts()

        # Phase 2: set sigterm_sent_at to past for grace period expiry
        slot = sm.get_slot("test-project", 1)
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.supervisor_workers.os.kill", side_effect=ProcessLookupError):
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

        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
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

        with patch("botfarm.supervisor_workers.os.kill") as mock_kill:
            sup._check_timeouts()
            # Should NOT timeout — default is 120 and we're at 25 minutes
            mock_kill.assert_not_called()

        sup._conn.close()


class TestSigtermHandler:
    """Tests for the SIGTERM handler installed in _worker_entry."""

    def test_sigterm_handler_raises_systemexit(self):
        """The SIGTERM handler installed in _worker_entry converts the signal
        to a SystemExit so that finally blocks run."""
        # We can't easily run _worker_entry (it needs a full pipeline),
        # so we test the handler logic directly: SIGTERM (15) → SystemExit(143).
        handler = None

        def capture_handler(signum, handler_fn):
            nonlocal handler
            if signum == signal.SIGTERM:
                handler = handler_fn

        with patch("botfarm.supervisor_workers.signal.signal", side_effect=capture_handler):
            with patch("botfarm.supervisor_workers.os.setpgrp"):
                with patch("botfarm.supervisor_workers.run_pipeline") as mock_pipeline:
                    mock_pipeline.side_effect = Exception("should not reach pipeline")
                    with patch("botfarm.supervisor_workers.init_db"):
                        with patch("botfarm.supervisor_workers.resolve_db_path"):
                            try:
                                import multiprocessing
                                q = multiprocessing.Queue()
                                _worker_entry(
                                    ticket_id="TST-1",
                                    ticket_title="Test",
                                    task_id=1,
                                    project_name="test",
                                    slot_id=1,
                                    cwd="/tmp",
                                    result_queue=q,
                                    max_turns=None,
                                )
                            except Exception:
                                pass  # expected — pipeline mock raises

        assert handler is not None, "SIGTERM handler was not installed"
        with pytest.raises(SystemExit) as exc_info:
            handler(signal.SIGTERM, None)
        assert exc_info.value.code == 128 + signal.SIGTERM

    def test_send_sigterm_also_kills_descendants(self, supervisor):
        """_send_sigterm sends SIGTERM to both the worker and its descendants."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        slot.stage_started_at = started
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        fake_descendants = [11111, 22222]
        with patch("botfarm.supervisor_workers._collect_descendant_pids", return_value=fake_descendants) as mock_collect, \
             patch("botfarm.supervisor_workers._kill_pids") as mock_kill_pids, \
             patch("botfarm.supervisor_workers.os.kill") as mock_kill:
            supervisor._check_timeouts()
            # Worker receives SIGTERM
            mock_kill.assert_called_once_with(99999, signal.SIGTERM)
            # Descendants are collected from the worker PID
            mock_collect.assert_called_once_with(99999)
            # Descendants receive SIGTERM too
            mock_kill_pids.assert_called_once_with(fake_descendants, signal.SIGTERM)

    def test_escalate_kill_uses_kill_process_tree(self, supervisor):
        """_maybe_escalate_kill uses _kill_process_tree for full cleanup."""
        now = datetime.now(timezone.utc)
        started = (now - timedelta(minutes=130)).isoformat()
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        slot.stage_started_at = started
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Phase 1: send SIGTERM
        with patch("botfarm.supervisor_workers._collect_descendant_pids", return_value=[]):
            with patch("botfarm.supervisor_workers.os.kill"):
                supervisor._check_timeouts()

        # Phase 2: grace period elapsed
        slot = sm.get_slot("test-project", 1)
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.supervisor_workers._kill_process_tree") as mock_tree:
            supervisor._check_timeouts()
            mock_tree.assert_called_once_with(99999)

        assert sm.get_slot("test-project", 1).status == "failed"



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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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

        with patch("botfarm.supervisor_recovery._is_pid_alive", return_value=True):
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=True),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
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

        with patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False):
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=True),
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
            patch("botfarm.supervisor_recovery._is_pid_alive", return_value=False),
            patch.object(supervisor, "_check_pr_status", return_value=(None, None)),
        ):
            supervisor._recover_on_startup()

        # Normal recovery: no PR found → failed
        assert sm.get_slot("test-project", 1).status == "failed"

    def test_failed_slot_skips_labels_when_ticket_terminal(self, supervisor):
        """_handle_failed_slot doesn't add labels to a ticket that's already Done."""
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

        # Neither move_issue nor add_labels should have been called
        poller.move_issue.assert_not_called()
        poller.add_labels.assert_not_called()
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

        with patch("botfarm.supervisor_recovery.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result == "merged"

    def test_gh_pr_state_open(self):
        """_gh_pr_state returns 'open' for open PRs."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "OPEN\n"

        with patch("botfarm.supervisor_recovery.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result == "open"

    def test_gh_pr_state_failure(self):
        """_gh_pr_state returns None on command failure."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = ""

        with patch("botfarm.supervisor_recovery.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result is None

    def test_gh_pr_url_for_branch_success(self):
        """_gh_pr_url_for_branch returns URL on success."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "https://github.com/org/repo/pull/42\n"

        with patch("botfarm.supervisor_recovery.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_url_for_branch("my-branch", "/tmp/repo")

        assert result == "https://github.com/org/repo/pull/42"

    def test_gh_pr_url_for_branch_no_pr(self):
        """_gh_pr_url_for_branch returns None when no PR exists."""
        mock_proc = MagicMock()
        mock_proc.returncode = 1
        mock_proc.stdout = ""

        with patch("botfarm.supervisor_recovery.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_url_for_branch("my-branch", "/tmp/repo")

        assert result is None

    def test_gh_pr_state_closed(self):
        """_gh_pr_state returns 'closed' for closed-not-merged PRs."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_proc.stdout = "CLOSED\n"

        with patch("botfarm.supervisor_recovery.subprocess.run", return_value=mock_proc):
            result = Supervisor._gh_pr_state("https://github.com/org/repo/pull/1")

        assert result == "closed"

    def test_gh_pr_url_for_branch_none_branch(self):
        """_gh_pr_url_for_branch returns None for None branch."""
        result = Supervisor._gh_pr_url_for_branch(None, "/tmp/repo")
        assert result is None
