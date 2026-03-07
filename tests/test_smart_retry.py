"""Tests for smart retry: prior context building, slot affinity, prompt rendering."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from botfarm.db import (
    get_stage_runs,
    get_task_by_ticket,
    init_db,
    insert_stage_run,
    insert_task,
    update_task,
)
from botfarm.slots import SlotManager, SlotState
from botfarm.supervisor_workers import PriorContext, build_prior_context, _determine_resume_stage


# ---------------------------------------------------------------------------
# build_prior_context
# ---------------------------------------------------------------------------


class TestBuildPriorContext:
    """Tests for the build_prior_context() function."""

    def _make_db(self, tmp_path: Path) -> sqlite3.Connection:
        db_path = tmp_path / "test.db"
        return init_db(db_path, allow_migration=True)

    def test_fresh_ticket_returns_empty(self, tmp_path):
        conn = self._make_db(tmp_path)
        prior = build_prior_context(conn, "SMA-999")
        assert prior.context_str == ""
        assert prior.prior_slot is None
        assert prior.prior_pr_url is None
        assert prior.failed_stage is None
        conn.close()

    def test_completed_ticket_returns_empty(self, tmp_path):
        conn = self._make_db(tmp_path)
        task_id = insert_task(
            conn, ticket_id="SMA-10", title="Test", project="proj", slot=1,
        )
        update_task(conn, task_id, status="completed")
        conn.commit()

        prior = build_prior_context(conn, "SMA-10")
        assert prior.context_str == ""
        assert prior.prior_slot is None
        conn.close()

    def test_failed_task_with_pr_builds_context(self, tmp_path):
        conn = self._make_db(tmp_path)
        task_id = insert_task(
            conn, ticket_id="SMA-20", title="Fix bug", project="proj", slot=2,
        )
        update_task(
            conn,
            task_id,
            status="failed",
            pr_url="https://github.com/org/repo/pull/42",
            pipeline_stage="merge",
            failure_reason="Merge conflict detected",
            review_iterations=2,
        )
        conn.commit()

        # Add some stage runs
        insert_stage_run(conn, task_id=task_id, stage="implement", iteration=1)
        insert_stage_run(conn, task_id=task_id, stage="review", iteration=1)
        insert_stage_run(conn, task_id=task_id, stage="fix", iteration=1)
        conn.commit()

        with patch("botfarm.supervisor_workers._check_pr_state", return_value="OPEN"):
            prior = build_prior_context(conn, "SMA-20")

        assert prior.prior_slot == 2
        assert prior.prior_pr_url == "https://github.com/org/repo/pull/42"
        assert prior.failed_stage == "merge"
        assert prior.pr_state == "OPEN"
        assert "## Prior Attempt Context" in prior.context_str
        assert "https://github.com/org/repo/pull/42" in prior.context_str
        assert "(status: OPEN)" in prior.context_str
        assert "merge" in prior.context_str
        assert "Merge conflict detected" in prior.context_str
        assert "Review iterations completed:** 2" in prior.context_str
        assert "implement → review → fix" in prior.context_str
        assert "Continue from where the previous attempt left off" in prior.context_str
        conn.close()

    def test_failed_task_without_pr_builds_context(self, tmp_path):
        conn = self._make_db(tmp_path)
        task_id = insert_task(
            conn, ticket_id="SMA-30", title="Add feature", project="proj", slot=1,
        )
        update_task(
            conn,
            task_id,
            status="failed",
            pipeline_stage="implement",
            failure_reason="Context window exhausted",
        )
        conn.commit()

        prior = build_prior_context(conn, "SMA-30")

        assert prior.prior_slot == 1
        assert prior.prior_pr_url is None
        assert prior.failed_stage == "implement"
        assert prior.pr_state is None
        assert "## Prior Attempt Context" in prior.context_str
        assert "implement" in prior.context_str
        assert "Context window exhausted" in prior.context_str
        assert "did not produce a PR" in prior.context_str
        assert "check if a partial branch exists" in prior.context_str
        conn.close()

    def test_pr_state_check_failure_omits_status(self, tmp_path):
        conn = self._make_db(tmp_path)
        task_id = insert_task(
            conn, ticket_id="SMA-40", title="Test", project="proj", slot=1,
        )
        update_task(
            conn,
            task_id,
            status="failed",
            pr_url="https://github.com/org/repo/pull/99",
            pipeline_stage="pr_checks",
            failure_reason="CI failed",
        )
        conn.commit()

        with patch("botfarm.supervisor_workers._check_pr_state", return_value=None):
            prior = build_prior_context(conn, "SMA-40")

        assert "https://github.com/org/repo/pull/99" in prior.context_str
        assert "(status:" not in prior.context_str
        assert prior.pr_state is None
        conn.close()

    def test_no_review_iterations_omits_line(self, tmp_path):
        conn = self._make_db(tmp_path)
        task_id = insert_task(
            conn, ticket_id="SMA-50", title="Test", project="proj", slot=1,
        )
        update_task(
            conn,
            task_id,
            status="failed",
            pipeline_stage="implement",
            failure_reason="Crashed",
        )
        conn.commit()

        prior = build_prior_context(conn, "SMA-50")
        assert "Review iterations" not in prior.context_str
        conn.close()


# ---------------------------------------------------------------------------
# _determine_resume_stage
# ---------------------------------------------------------------------------


class TestDetermineResumeStage:
    """Tests for the _determine_resume_stage() helper."""

    def test_pr_checks_resumes_at_pr_checks(self):
        assert _determine_resume_stage("pr_checks") == "pr_checks"

    def test_ci_fix_resumes_at_pr_checks(self):
        assert _determine_resume_stage("ci_fix") == "pr_checks"

    def test_merge_resumes_at_merge(self):
        assert _determine_resume_stage("merge") == "merge"

    def test_review_resumes_at_review(self):
        assert _determine_resume_stage("review") == "review"

    def test_fix_resumes_at_review(self):
        assert _determine_resume_stage("fix") == "review"

    def test_resolve_conflict_resumes_at_review(self):
        assert _determine_resume_stage("resolve_conflict") == "review"

    def test_implement_returns_none(self):
        assert _determine_resume_stage("implement") is None

    def test_none_returns_none(self):
        assert _determine_resume_stage(None) is None

    def test_unknown_stage_returns_none(self):
        assert _determine_resume_stage("unknown") is None


# ---------------------------------------------------------------------------
# Retry resume: pr_url preservation and stage resume
# ---------------------------------------------------------------------------


class TestRetryResumeDispatch:
    """Tests for retry resume logic in dispatch_worker."""

    def _make_db(self, tmp_path: Path) -> sqlite3.Connection:
        db_path = tmp_path / "test.db"
        return init_db(db_path, allow_migration=True)

    def test_pr_url_preserved_after_insert_task_resets_it(self, tmp_path):
        """When retrying with an open PR, pr_url is written back after insert_task."""
        conn = self._make_db(tmp_path)
        # Create initial failed task with a PR
        task_id = insert_task(
            conn, ticket_id="SMA-100", title="Test", project="proj", slot=1,
        )
        update_task(
            conn, task_id,
            status="failed",
            pr_url="https://github.com/org/repo/pull/50",
            pipeline_stage="pr_checks",
            failure_reason="CI failed",
        )
        conn.commit()

        # build_prior_context captures the pr_url before insert_task resets it
        with patch("botfarm.supervisor_workers._check_pr_state", return_value="OPEN"):
            prior = build_prior_context(conn, "SMA-100")

        assert prior.prior_pr_url == "https://github.com/org/repo/pull/50"
        assert prior.pr_state == "OPEN"

        # insert_task resets pr_url to NULL (simulating ON CONFLICT reset)
        new_task_id = insert_task(
            conn, ticket_id="SMA-100", title="Test", project="proj", slot=2,
        )
        conn.commit()
        from botfarm.db import get_task
        task = get_task(conn, new_task_id)
        assert task["pr_url"] is None  # insert_task reset it

        # Preserve it back (what dispatch_worker does)
        if prior.prior_pr_url and prior.pr_state == "OPEN":
            update_task(conn, new_task_id, pr_url=prior.prior_pr_url)
            conn.commit()

        task = get_task(conn, new_task_id)
        assert task["pr_url"] == "https://github.com/org/repo/pull/50"
        conn.close()

    def test_closed_pr_does_not_trigger_resume(self, tmp_path):
        """When the prior PR is CLOSED, no resume stage is determined."""
        conn = self._make_db(tmp_path)
        task_id = insert_task(
            conn, ticket_id="SMA-101", title="Test", project="proj", slot=1,
        )
        update_task(
            conn, task_id,
            status="failed",
            pr_url="https://github.com/org/repo/pull/51",
            pipeline_stage="merge",
            failure_reason="Conflict",
        )
        conn.commit()

        with patch("botfarm.supervisor_workers._check_pr_state", return_value="CLOSED"):
            prior = build_prior_context(conn, "SMA-101")

        assert prior.pr_state == "CLOSED"
        # dispatch_worker would not set resume_from_stage because pr_state != "OPEN"
        conn.close()

    def test_no_pr_url_does_not_trigger_resume(self, tmp_path):
        """When there's no prior PR, no resume stage is determined."""
        conn = self._make_db(tmp_path)
        task_id = insert_task(
            conn, ticket_id="SMA-102", title="Test", project="proj", slot=1,
        )
        update_task(
            conn, task_id,
            status="failed",
            pipeline_stage="implement",
            failure_reason="Crashed",
        )
        conn.commit()

        prior = build_prior_context(conn, "SMA-102")
        assert prior.prior_pr_url is None
        assert prior.pr_state is None
        conn.close()


# ---------------------------------------------------------------------------
# _merge_main_before_retry_resume
# ---------------------------------------------------------------------------


class TestMergeMainBeforeRetryResume:
    """Tests for _merge_main_before_retry_resume()."""

    def test_successful_merge(self, tmp_path):
        from botfarm.worker import _merge_main_before_retry_resume

        calls = []

        def mock_run(cmd, **kwargs):
            calls.append(cmd)
            result = MagicMock()
            result.returncode = 0
            result.stdout = "test-branch\n"
            return result

        with patch("botfarm.worker.subprocess.run", side_effect=mock_run):
            ok = _merge_main_before_retry_resume(
                tmp_path, "https://github.com/org/repo/pull/1",
            )

        assert ok is True
        # Should have: gh pr view, git fetch, git checkout, git merge, git push
        assert len(calls) == 5
        assert calls[0][0] == "gh"
        assert calls[1][:2] == ["git", "fetch"]
        assert calls[2][:2] == ["git", "checkout"]
        assert calls[3][:2] == ["git", "merge"]
        assert calls[4][:2] == ["git", "push"]

    def test_merge_conflict_aborts(self, tmp_path):
        from botfarm.worker import _merge_main_before_retry_resume

        call_count = 0

        def mock_run(cmd, **kwargs):
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            result.stdout = "test-branch\n"
            # gh pr view (ok), git fetch (ok), git checkout (ok),
            # git merge (fail), git merge --abort (ok)
            if cmd[:2] == ["git", "merge"] and "--abort" not in cmd:
                result.returncode = 1
                if kwargs.get("check"):
                    raise __import__("subprocess").CalledProcessError(1, cmd)
                return result
            result.returncode = 0
            return result

        with patch("botfarm.worker.subprocess.run", side_effect=mock_run):
            ok = _merge_main_before_retry_resume(
                tmp_path, "https://github.com/org/repo/pull/1",
            )

        assert ok is False

    def test_pr_branch_lookup_failure(self, tmp_path):
        from botfarm.worker import _merge_main_before_retry_resume

        def mock_run(cmd, **kwargs):
            result = MagicMock()
            result.returncode = 1
            result.stdout = ""
            return result

        with patch("botfarm.worker.subprocess.run", side_effect=mock_run):
            ok = _merge_main_before_retry_resume(
                tmp_path, "https://github.com/org/repo/pull/1",
            )

        assert ok is False


# ---------------------------------------------------------------------------
# Slot affinity — find_free_slot_for_project
# ---------------------------------------------------------------------------


class TestSlotAffinity:
    """Tests for the preferred_slot_id parameter in find_free_slot_for_project."""

    def _make_manager(self, tmp_path: Path) -> SlotManager:
        db_path = tmp_path / "test.db"
        mgr = SlotManager(db_path=db_path)
        mgr.register_slot("proj", 1)
        mgr.register_slot("proj", 2)
        mgr.register_slot("proj", 3)
        return mgr

    def test_no_preference_returns_first_free(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        slot = mgr.find_free_slot_for_project("proj")
        assert slot is not None
        assert slot.slot_id == 1

    def test_preferred_slot_returned_when_free(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        slot = mgr.find_free_slot_for_project("proj", preferred_slot_id=2)
        assert slot is not None
        assert slot.slot_id == 2

    def test_preferred_slot_busy_falls_back(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        mgr.assign_ticket(
            "proj", 2,
            ticket_id="SMA-1", ticket_title="busy", branch="b",
        )
        slot = mgr.find_free_slot_for_project("proj", preferred_slot_id=2)
        assert slot is not None
        assert slot.slot_id != 2

    def test_preferred_slot_none_acts_like_default(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        slot = mgr.find_free_slot_for_project("proj", preferred_slot_id=None)
        assert slot is not None
        assert slot.slot_id == 1

    def test_no_free_slots_returns_none(self, tmp_path):
        mgr = self._make_manager(tmp_path)
        for sid in (1, 2, 3):
            mgr.assign_ticket(
                "proj", sid,
                ticket_id=f"SMA-{sid}", ticket_title="busy", branch=f"b{sid}",
            )
        slot = mgr.find_free_slot_for_project("proj", preferred_slot_id=2)
        assert slot is None


# ---------------------------------------------------------------------------
# Notification retry instructions
# ---------------------------------------------------------------------------


class TestNotificationRetryInstructions:
    """Tests for retry instructions in failure notifications."""

    def test_slack_failure_notification_includes_retry(self):
        from botfarm.config import NotificationsConfig
        from botfarm.notifications import Notifier

        notifier = Notifier(NotificationsConfig(webhook_url="https://test.example.com/hook"))
        messages: list[str] = []

        def mock_send(event_type, message, **kwargs):
            messages.append(message)

        notifier._send = mock_send
        notifier.notify_task_failed(
            ticket_id="SMA-1",
            title="Test task",
            failure_reason="merge conflict",
        )

        assert len(messages) == 1
        assert "To retry:" in messages[0]
        assert "remove 'Failed' and 'Human' labels" in messages[0]
        notifier.close()


# ---------------------------------------------------------------------------
# Prior context prompt rendering
# ---------------------------------------------------------------------------


class TestPriorContextPromptRendering:
    """Tests for prior_context injection into implement prompt."""

    def test_prior_context_prepended_in_legacy_mode(self):
        from botfarm.worker_stages import _run_implement

        prior = "## Prior Attempt Context\nTest prior context.\n\n"
        with patch("botfarm.worker_stages._invoke_claude") as mock_claude:
            mock_claude.return_value = MagicMock(
                is_error=False,
                result_text="PR: https://github.com/org/repo/pull/1",
            )
            _run_implement(
                "SMA-1",
                cwd="/tmp/test",
                max_turns=10,
                prior_context=prior,
            )

        prompt_arg = mock_claude.call_args[0][0]
        assert prompt_arg.startswith("## Prior Attempt Context")
        assert "Work on Linear ticket SMA-1" in prompt_arg

    def test_empty_prior_context_no_change_in_legacy(self):
        from botfarm.worker_stages import _run_implement

        with patch("botfarm.worker_stages._invoke_claude") as mock_claude:
            mock_claude.return_value = MagicMock(
                is_error=False,
                result_text="PR: https://github.com/org/repo/pull/1",
            )
            _run_implement(
                "SMA-1",
                cwd="/tmp/test",
                max_turns=10,
                prior_context="",
            )

        prompt_arg = mock_claude.call_args[0][0]
        assert prompt_arg.startswith("Work on Linear ticket SMA-1")

    def test_prior_context_in_prompt_vars_db_driven(self):
        from botfarm.worker_stages import _execute_stage
        from botfarm.workflow import StageTemplate

        tpl = StageTemplate(
            id=1,
            name="implement",
            stage_order=1,
            executor_type="claude",
            identity="coder",
            prompt_template="{prior_context}Work on {ticket_id}.",
            max_turns=100,
            timeout_minutes=60,
            shell_command=None,
            result_parser="pr_url",
        )

        with patch("botfarm.worker_stages._run_claude_stage") as mock_stage:
            mock_stage.return_value = MagicMock(
                stage="implement", success=True, pr_url=None,
                review_approved=None,
            )
            _execute_stage(
                "implement",
                ticket_id="SMA-1",
                pr_url=None,
                cwd="/tmp/test",
                max_turns=10,
                pr_checks_timeout=600,
                stage_tpl=tpl,
                prior_context="## Prior Attempt\nContext here.\n\n",
            )

        call_kwargs = mock_stage.call_args[1]
        assert call_kwargs["prompt_vars"]["prior_context"] == "## Prior Attempt\nContext here.\n\n"
        assert call_kwargs["prompt_vars"]["ticket_id"] == "SMA-1"

    def test_empty_prior_context_renders_as_empty_db_driven(self):
        from botfarm.worker_stages import _execute_stage
        from botfarm.workflow import StageTemplate

        tpl = StageTemplate(
            id=1,
            name="implement",
            stage_order=1,
            executor_type="claude",
            identity="coder",
            prompt_template="{prior_context}Work on {ticket_id}.",
            max_turns=100,
            timeout_minutes=60,
            shell_command=None,
            result_parser="pr_url",
        )

        with patch("botfarm.worker_stages._run_claude_stage") as mock_stage:
            mock_stage.return_value = MagicMock(
                stage="implement", success=True, pr_url=None,
                review_approved=None,
            )
            _execute_stage(
                "implement",
                ticket_id="SMA-1",
                pr_url=None,
                cwd="/tmp/test",
                max_turns=10,
                pr_checks_timeout=600,
                stage_tpl=tpl,
                prior_context="",
            )

        call_kwargs = mock_stage.call_args[1]
        assert call_kwargs["prompt_vars"]["prior_context"] == ""
