"""Tests for supervisor rate limits, capacity, pause/resume, stall detection, and degraded mode."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import CapacityConfig
from botfarm.db import get_events, get_task, init_db, insert_task, load_capacity_state
from botfarm.bugtracker import ActiveIssuesCount, PollResult
from botfarm.supervisor import (
    Supervisor,
    _StallInfo,
    _WorkerResult,
    _check_limit_hit,
    _classify_failure,
    _text_indicates_limit_hit,
)
from botfarm.worker import PipelineResult
from tests.helpers import make_issue



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
# _classify_failure
# ---------------------------------------------------------------------------


class TestClassifyFailure:
    """Tests for _classify_failure() failure reason categorization."""

    # -- limit_hit --
    def test_rate_limit(self):
        assert _classify_failure("Claude was killed due to rate limit") == "limit_hit"

    def test_usage_limit(self):
        assert _classify_failure("Hit usage limit for this period") == "limit_hit"

    def test_max_tokens_exceeded(self):
        assert _classify_failure("CalledProcessError: max_tokens_exceeded") == "limit_hit"

    # -- timeout --
    def test_timeout_error(self):
        assert _classify_failure("TimeoutError: stage implement") == "timeout"

    def test_timed_out(self):
        assert _classify_failure("Worker timed out waiting for response") == "timeout"

    def test_timeout_in_stage(self):
        assert _classify_failure("timeout in stage implement") == "timeout"

    # -- env_missing_runtime --
    def test_command_not_found(self):
        assert _classify_failure("bash: node: command not found") == "env_missing_runtime"

    def test_no_such_file(self):
        assert _classify_failure("/usr/bin/python3: No such file or directory") == "env_missing_runtime"

    # -- env_missing_package --
    def test_module_not_found_error(self):
        assert _classify_failure("ModuleNotFoundError: No module named 'flask'") == "env_missing_package"

    def test_cannot_find_module(self):
        assert _classify_failure("Error: Cannot find module 'express'") == "env_missing_package"

    def test_import_error(self):
        assert _classify_failure("ImportError: cannot import name 'foo'") == "env_missing_package"

    def test_no_module_named(self):
        assert _classify_failure("No module named 'requests'") == "env_missing_package"

    # -- env_missing_service --
    def test_econnrefused(self):
        assert _classify_failure("Error: connect ECONNREFUSED 127.0.0.1:5432") == "env_missing_service"

    def test_connection_refused(self):
        assert _classify_failure("psycopg2.OperationalError: connection refused") == "env_missing_service"

    def test_role_does_not_exist(self):
        assert _classify_failure('role "myapp" does not exist') == "env_missing_service"

    def test_database_does_not_exist(self):
        assert _classify_failure('database "mydb" does not exist') == "env_missing_service"

    # -- env_missing_config --
    def test_env_file_not_found(self):
        assert _classify_failure(".env file not found") == "env_missing_config"

    def test_env_variable_not_set(self):
        assert _classify_failure("environment variable DATABASE_URL not set") == "env_missing_config"

    def test_env_var_not_set(self):
        assert _classify_failure("env var API_KEY not set") == "env_missing_config"

    # -- code_failure (default) --
    def test_generic_failure(self):
        assert _classify_failure("PR URL not found in output") == "code_failure"

    def test_test_failure(self):
        assert _classify_failure("implement: tests failed after 3 attempts") == "code_failure"

    def test_none_reason(self):
        assert _classify_failure(None) == "code_failure"

    def test_empty_reason(self):
        assert _classify_failure("") == "code_failure"

    # -- auth_failure --
    def test_authentication_error(self):
        assert _classify_failure("authentication_error: invalid token") == "auth_failure"

    def test_invalid_authentication(self):
        assert _classify_failure("Error: invalid authentication credentials") == "auth_failure"

    def test_401_unauthorized(self):
        assert _classify_failure("HTTP 401 Unauthorized") == "auth_failure"

    def test_status_401(self):
        assert _classify_failure("status 401: token expired") == "auth_failure"

    def test_bare_401_not_matched(self):
        """Bare '401' without auth context should not classify as auth_failure."""
        assert _classify_failure("Error on line 401 of parser.py") == "code_failure"

    def test_case_insensitive_authentication_error(self):
        assert _classify_failure("AUTHENTICATION_ERROR: expired") == "auth_failure"

    # -- case insensitivity --
    def test_case_insensitive_module_error(self):
        assert _classify_failure("MODULENOTFOUNDERROR: No module named 'foo'") == "env_missing_package"

    def test_case_insensitive_command(self):
        assert _classify_failure("COMMAND NOT FOUND") == "env_missing_runtime"


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
            mock_fp.assert_called_once_with(supervisor._conn, bypass_cooldown=True)

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
# _text_indicates_limit_hit
# ---------------------------------------------------------------------------


class TestTextIndicatesLimitHit:
    """Tests for _text_indicates_limit_hit() — detecting limits in Claude output text."""

    def test_none_returns_false(self):
        assert _text_indicates_limit_hit(None) is False

    def test_empty_returns_false(self):
        assert _text_indicates_limit_hit("") is False

    def test_normal_output_returns_false(self):
        assert _text_indicates_limit_hit("I implemented the feature successfully") is False

    def test_youve_hit_your_limit(self):
        assert _text_indicates_limit_hit("You've hit your limit · resets 5am") is True

    def test_you_have_hit_your_limit(self):
        assert _text_indicates_limit_hit("You have hit your limit for this period") is True

    def test_hit_your_limit(self):
        assert _text_indicates_limit_hit("Sorry, hit your limit. Try again later.") is True

    def test_usage_limit(self):
        assert _text_indicates_limit_hit("Error: usage limit exceeded") is True

    def test_rate_limit(self):
        assert _text_indicates_limit_hit("rate limit reached, please wait") is True

    def test_max_tokens_exceeded(self):
        assert _text_indicates_limit_hit("Error: max_tokens_exceeded") is True

    def test_case_insensitive(self):
        assert _text_indicates_limit_hit("YOU'VE HIT YOUR LIMIT") is True


# ---------------------------------------------------------------------------
# Timeout-kill limit detection
# ---------------------------------------------------------------------------


class TestEscalateKillLimitDetection:
    """Tests for _maybe_escalate_kill() detecting limits instead of hard-failing."""

    def test_timeout_with_interrupted_by_limit_pauses(self, supervisor):
        """Timeout-killed worker with interrupted_by_limit flag pauses instead of failing."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="fix")
        slot = sm.get_slot("test-project", 1)
        now = datetime.now(timezone.utc)
        slot.stage_started_at = (now - timedelta(minutes=130)).isoformat()
        slot.pid = 99999
        slot.interrupted_by_limit = True  # Previously paused for limits

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Phase 1: SIGTERM
        with patch("botfarm.supervisor_workers._collect_descendant_pids", return_value=[]):
            with patch("botfarm.supervisor_workers.os.kill"):
                supervisor._check_timeouts()

        # Phase 2: grace period elapsed → SIGKILL
        slot = sm.get_slot("test-project", 1)
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.supervisor_workers._kill_process_tree"):
            supervisor._check_timeouts()

        # Should be paused_limit, not failed
        slot = sm.get_slot("test-project", 1)
        assert slot.status == "paused_limit"
        assert slot.interrupted_by_limit is True

    def test_timeout_with_usage_api_limit_pauses(self, supervisor):
        """Timeout-killed worker pauses when usage API shows limits exceeded."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        now = datetime.now(timezone.utc)
        slot.stage_started_at = (now - timedelta(minutes=130)).isoformat()
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Phase 1: SIGTERM
        with patch("botfarm.supervisor_workers._collect_descendant_pids", return_value=[]):
            with patch("botfarm.supervisor_workers.os.kill"):
                supervisor._check_timeouts()

        # Phase 2: grace period elapsed
        slot = sm.get_slot("test-project", 1)
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with (
            patch("botfarm.supervisor_workers._kill_process_tree"),
            patch.object(
                supervisor, "_check_usage_api_for_limit", return_value=True,
            ),
        ):
            supervisor._check_timeouts()

        slot = sm.get_slot("test-project", 1)
        assert slot.status == "paused_limit"

    def test_timeout_without_limit_indicators_fails_normally(self, supervisor):
        """Timeout-killed worker without limit indicators is marked failed as before."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="implement")
        slot = sm.get_slot("test-project", 1)
        now = datetime.now(timezone.utc)
        slot.stage_started_at = (now - timedelta(minutes=130)).isoformat()
        slot.pid = 99999

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Phase 1: SIGTERM
        with patch("botfarm.supervisor_workers._collect_descendant_pids", return_value=[]):
            with patch("botfarm.supervisor_workers.os.kill"):
                supervisor._check_timeouts()

        # Phase 2: grace period elapsed
        slot = sm.get_slot("test-project", 1)
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with (
            patch("botfarm.supervisor_workers._kill_process_tree"),
            patch.object(
                supervisor, "_check_usage_api_for_limit", return_value=False,
            ),
        ):
            supervisor._check_timeouts()

        slot = sm.get_slot("test-project", 1)
        assert slot.status == "failed"

    def test_timeout_limit_pause_records_event(self, supervisor):
        """Timeout-to-limit-pause records a limit_hit event."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Test", branch="b1",
        )
        sm.update_stage("test-project", 1, stage="fix")
        slot = sm.get_slot("test-project", 1)
        now = datetime.now(timezone.utc)
        slot.stage_started_at = (now - timedelta(minutes=130)).isoformat()
        slot.pid = 99999
        slot.interrupted_by_limit = True

        insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="Test", project="test-project", slot=1,
            status="in_progress",
        )
        supervisor._conn.commit()

        # Phase 1: SIGTERM
        with patch("botfarm.supervisor_workers._collect_descendant_pids", return_value=[]):
            with patch("botfarm.supervisor_workers.os.kill"):
                supervisor._check_timeouts()

        slot = sm.get_slot("test-project", 1)
        slot.sigterm_sent_at = "2020-01-01T00:00:00.000000Z"

        with patch("botfarm.supervisor_workers._kill_process_tree"):
            supervisor._check_timeouts()

        events = get_events(supervisor._conn, event_type="limit_hit")
        assert len(events) >= 1
        assert "fix" in events[-1]["detail"]


# ---------------------------------------------------------------------------
# result_text limit detection in reconcile
# ---------------------------------------------------------------------------


class TestResultTextLimitDetection:
    """Tests for _reconcile_workers() detecting limits via result_text."""

    def test_result_text_limit_detected_when_api_misses(self, supervisor):
        """result_text with limit indicators triggers pause even when usage API says no."""
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
            failure_reason="Unexpected process exit code 1",
            limit_hit=False,
            result_text="Error: You've hit your limit · resets 5am",
        ))

        with patch.object(supervisor, "_check_usage_api_for_limit", return_value=False):
            supervisor._reconcile_workers()

        slot = sm.get_slot("test-project", 1)
        assert slot.status == "paused_limit"
        assert slot.interrupted_by_limit is True

    def test_result_text_without_limit_indicators_fails(self, supervisor):
        """result_text without limit indicators does not prevent normal failure."""
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
            failure_reason="Tests failed with exit code 1",
            limit_hit=False,
            result_text="I ran the tests but they failed.",
        ))

        with patch.object(supervisor, "_check_usage_api_for_limit", return_value=False):
            supervisor._reconcile_workers()

        slot = sm.get_slot("test-project", 1)
        assert slot.status == "failed"

    def test_result_text_none_does_not_cause_errors(self, supervisor):
        """None result_text does not cause errors in reconcile path."""
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
            failure_reason="Something went wrong",
            limit_hit=False,
            result_text=None,
        ))

        with patch.object(supervisor, "_check_usage_api_for_limit", return_value=False):
            supervisor._reconcile_workers()

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

            mock_fp.assert_called_once_with(supervisor._conn, bypass_cooldown=True)

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
# Capacity polling
# ---------------------------------------------------------------------------


class TestPollCapacity:
    """Tests for _poll_capacity() — Linear issue count monitoring."""

    def test_disabled_config_skips_polling(self, supervisor):
        """When capacity_monitoring.enabled is False, nothing happens."""
        supervisor._config.bugtracker.capacity_monitoring = CapacityConfig(enabled=False)
        supervisor._bugtracker_client = MagicMock()

        supervisor._poll_capacity()

        supervisor._bugtracker_client.count_active_issues.assert_not_called()

    def test_api_failure_skips_tick(self, supervisor, caplog):
        """When count_active_issues returns None, the tick is skipped."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = None

        with caplog.at_level(logging.WARNING):
            supervisor._poll_capacity()

        assert "Capacity poll failed" in caplog.text
        assert supervisor._capacity_level == "normal"

    def test_normal_level_no_events(self, supervisor):
        """Below warning threshold, no events are emitted."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=100, by_project={"proj": 100},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "normal"
        events = get_events(supervisor._conn, event_type="capacity_warning")
        assert len(events) == 0

    def test_warning_threshold_emits_event(self, supervisor):
        """Crossing the warning threshold emits a capacity_warning event."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=180, by_project={"proj": 180},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "warning"
        events = get_events(supervisor._conn, event_type="capacity_warning")
        assert len(events) == 1
        assert "180/250" in events[0]["detail"]

    def test_critical_threshold_emits_event(self, supervisor):
        """Crossing the critical threshold emits a capacity_critical event."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=215, by_project={"proj": 215},
        )

        supervisor._poll_capacity()

        assert supervisor._capacity_level == "critical"
        events = get_events(supervisor._conn, event_type="capacity_critical")
        assert len(events) == 1

    def test_blocked_threshold_pauses_dispatch(self, supervisor):
        """At >=95% capacity, dispatch is auto-paused with reason capacity_blocked."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
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
        supervisor._bugtracker_client = MagicMock()

        # First: enter blocked state
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"

        # Drop to 92% — above resume_threshold (90%), should stay blocked
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=230, by_project={"proj": 230},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"
        assert supervisor.slot_manager.dispatch_paused is True

    def test_resume_below_resume_threshold(self, supervisor):
        """Dispatch resumes when utilization drops below resume_threshold."""
        supervisor._bugtracker_client = MagicMock()

        # Enter blocked state
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"

        # Drop below resume threshold (90% of 250 = 225)
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=220, by_project={"proj": 220},
        )
        supervisor._poll_capacity()

        assert supervisor._capacity_level == "critical"
        assert supervisor.slot_manager.dispatch_paused is False
        events = get_events(supervisor._conn, event_type="capacity_cleared")
        assert len(events) == 1

    def test_event_emitted_only_on_transition(self, supervisor):
        """Repeated polls at same level don't produce duplicate events."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=180, by_project={"proj": 180},
        )

        supervisor._poll_capacity()
        supervisor._poll_capacity()
        supervisor._poll_capacity()

        events = get_events(supervisor._conn, event_type="capacity_warning")
        assert len(events) == 1

    def test_capacity_state_saved_to_db(self, supervisor):
        """Each poll persists the capacity snapshot to dispatch_state."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
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
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
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
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=180, by_project={"proj": 180},
        )
        supervisor._notifier = MagicMock()

        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_warning.assert_called_once_with(
            count=180, limit=250, percentage=pytest.approx(72.0),
        )

    def test_critical_transition_notifies(self, supervisor):
        """Transitioning to critical level calls notify_capacity_critical."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=215, by_project={"proj": 215},
        )
        supervisor._notifier = MagicMock()

        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_critical.assert_called_once_with(
            count=215, limit=250, percentage=pytest.approx(86.0),
        )

    def test_blocked_transition_notifies(self, supervisor):
        """Transitioning to blocked level calls notify_capacity_blocked."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=238, by_project={"proj": 238},
        )
        supervisor._notifier = MagicMock()

        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_blocked.assert_called_once_with(
            count=238, limit=250, percentage=pytest.approx(95.2),
        )

    def test_cleared_transition_notifies(self, supervisor):
        """Resuming from blocked calls notify_capacity_cleared."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._notifier = MagicMock()

        # Enter blocked state
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        supervisor._notifier.reset_mock()

        # Drop below resume threshold (90% of 250 = 225)
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=220, by_project={"proj": 220},
        )
        supervisor._poll_capacity()

        supervisor._notifier.notify_capacity_cleared.assert_called_once_with(
            count=220, limit=250, percentage=pytest.approx(88.0),
        )

    def test_no_notification_on_same_level(self, supervisor):
        """Repeated polls at the same level do not fire notifications."""
        supervisor._bugtracker_client = MagicMock()
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
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
        supervisor._bugtracker_client = MagicMock()

        # Enter blocked state
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
            total=240, by_project={"proj": 240},
        )
        supervisor._poll_capacity()
        assert supervisor._capacity_level == "blocked"
        assert sm.dispatch_pause_reason == "capacity_blocked"

        # Drop below resume threshold — should restore start_paused, not unpause
        supervisor._bugtracker_client.count_active_issues.return_value = ActiveIssuesCount(
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

        with patch("botfarm.git_update.pull_and_install", return_value="git pull failed"):
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
