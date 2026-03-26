"""Tests for post-QA supervisor actions (report, bug tickets, labels)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from botfarm.bugtracker import PollResult
from botfarm.bugtracker.types import CreatedIssue
from botfarm.config import (
    BotfarmConfig,
    DatabaseConfig,
    LinearConfig,
    ProjectConfig,
)
from botfarm.db import get_events, insert_task, update_task
from botfarm.supervisor import Supervisor
from botfarm.supervisor_ops import (
    QaBug,
    QaReport,
    _SEVERITY_TO_PRIORITY,
    parse_qa_report,
)
from tests.helpers import make_config


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def supervisor(tmp_path, monkeypatch):
    """Create a Supervisor with mocked pollers for QA testing."""
    config = make_config(tmp_path)
    (tmp_path / "repo").mkdir()
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
    monkeypatch.setattr(
        Supervisor, "_slot_db_path",
        staticmethod(lambda pn, sid: str(tmp_path / "slots" / f"{pn}-{sid}" / "botfarm.db")),
    )
    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.poll.return_value = PollResult(candidates=[], blocked=[], auto_close_parents=[])
    mock_poller.is_issue_terminal.return_value = False
    mock_poller.create_issue.return_value = CreatedIssue(
        id="bug-uuid-1", identifier="TST-99", url="https://linear.app/test/TST-99",
    )

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(config, log_dir=tmp_path / "logs")

    return sup


def _qa_report_json(
    passed: bool = True,
    bugs: list[dict] | None = None,
    summary: str = "QA complete",
    report_text: str = "Detailed report here",
) -> str:
    """Build a JSON QA report string."""
    return json.dumps({
        "type": "qa_report",
        "passed": passed,
        "summary": summary,
        "bugs": bugs or [],
        "report_text": report_text,
    })


# ---------------------------------------------------------------------------
# parse_qa_report unit tests
# ---------------------------------------------------------------------------


class TestParseQaReport:
    def test_valid_passed_report(self):
        raw = _qa_report_json(passed=True)
        report = parse_qa_report(raw)
        assert report is not None
        assert report.passed is True
        assert report.bugs == []
        assert report.summary == "QA complete"

    def test_valid_failed_report_with_bugs(self):
        bugs = [
            {"title": "Button broken", "description": "Click does nothing", "severity": "high"},
            {"title": "Typo", "severity": "low"},
        ]
        raw = _qa_report_json(passed=False, bugs=bugs)
        report = parse_qa_report(raw)
        assert report is not None
        assert report.passed is False
        assert len(report.bugs) == 2
        assert report.bugs[0].title == "Button broken"
        assert report.bugs[0].severity == "high"
        assert report.bugs[1].description == ""

    def test_returns_none_for_empty_input(self):
        assert parse_qa_report(None) is None
        assert parse_qa_report("") is None

    def test_returns_none_for_non_json(self):
        assert parse_qa_report("just some text") is None

    def test_returns_none_for_non_qa_json(self):
        assert parse_qa_report(json.dumps({"type": "other"})) is None
        assert parse_qa_report(json.dumps({"foo": "bar"})) is None

    def test_bugs_default_severity(self):
        bugs = [{"title": "No severity bug"}]
        raw = _qa_report_json(passed=False, bugs=bugs)
        report = parse_qa_report(raw)
        assert report.bugs[0].severity == "medium"

    def test_passed_defaults_to_true_when_no_bugs(self):
        """When 'passed' is missing and there are no bugs, defaults to True."""
        raw = json.dumps({"type": "qa_report", "summary": "All good"})
        report = parse_qa_report(raw)
        assert report.passed is True

    def test_passed_defaults_to_false_when_bugs_present(self):
        """When 'passed' is missing but bugs exist, defaults to False."""
        raw = json.dumps({
            "type": "qa_report",
            "bugs": [{"title": "A bug"}],
        })
        report = parse_qa_report(raw)
        assert report.passed is False

    def test_skips_non_dict_bugs(self):
        raw = json.dumps({
            "type": "qa_report",
            "passed": False,
            "bugs": [{"title": "Real bug"}, "not a dict", 42],
        })
        report = parse_qa_report(raw)
        assert len(report.bugs) == 1

    # --- Text-marker format tests ---

    def test_text_marker_passed(self):
        text = (
            "QA_REPORT_START\n"
            "Everything looks good.\n"
            "Verdict: PASSED\n"
            "QA_REPORT_END\n"
        )
        report = parse_qa_report(text)
        assert report is not None
        assert report.passed is True
        assert report.bugs == []
        assert "Everything looks good" in report.report_text

    def test_text_marker_failed_with_bugs(self):
        text = (
            "QA_REPORT_START\n"
            "Found issues.\n"
            "Verdict: FAILED\n"
            "QA_REPORT_END\n"
            "BUG_START\n"
            "Title: Login crash\n"
            "Severity: critical\n"
            "Description:\nApp crashes on submit\n"
            "BUG_END\n"
        )
        report = parse_qa_report(text)
        assert report is not None
        assert report.passed is False
        assert len(report.bugs) == 1
        assert report.bugs[0].title == "Login crash"
        assert report.bugs[0].severity == "critical"

    def test_text_marker_no_markers_returns_none(self):
        """Plain text without QA markers returns None."""
        assert parse_qa_report("just some text") is None

    def test_text_marker_bugs_default_severity(self):
        text = (
            "BUG_START\n"
            "Title: No severity\n"
            "BUG_END\n"
        )
        report = parse_qa_report(text)
        assert report is not None
        assert report.bugs[0].severity == "medium"


class TestSeverityToPriority:
    def test_mapping(self):
        assert _SEVERITY_TO_PRIORITY["critical"] == 1
        assert _SEVERITY_TO_PRIORITY["high"] == 2
        assert _SEVERITY_TO_PRIORITY["medium"] == 3
        assert _SEVERITY_TO_PRIORITY["low"] == 4


# ---------------------------------------------------------------------------
# _build_qa_comment unit tests
# ---------------------------------------------------------------------------


class TestBuildQaComment:
    def test_passed_report(self):
        report = QaReport(passed=True, summary="All tests passed")
        comment = Supervisor._build_qa_comment(report, None)
        assert "**QA Report — passed**" in comment
        assert "All tests passed" in comment

    def test_failed_report_with_bugs(self):
        report = QaReport(
            passed=False,
            summary="Found issues",
            bugs=[QaBug(title="Bug A", severity="high")],
        )
        comment = Supervisor._build_qa_comment(report, None)
        assert "**QA Report — failed**" in comment
        assert "**Bugs found: 1**" in comment
        assert "**[high]** Bug A" in comment

    def test_fallback_raw_text(self):
        comment = Supervisor._build_qa_comment(None, "raw output here")
        assert "**QA Report**" in comment
        assert "raw output here" in comment

    def test_no_data(self):
        comment = Supervisor._build_qa_comment(None, None)
        assert "no report data available" in comment


# ---------------------------------------------------------------------------
# _handle_qa_completion integration tests
# ---------------------------------------------------------------------------


class TestHandleQaCompletion:
    def _assign_qa_slot(self, supervisor, stages=None):
        """Assign a ticket and set it up as a QA pipeline completion."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="QA Test", branch="qa-branch",
        )
        slot = sm.get_slot("test-project", 1)
        slot.stages_completed = stages or ["qa"]
        sm.mark_completed("test-project", 1)
        return sm, slot

    def test_qa_passed_posts_comment_and_label(self, supervisor):
        sm, _ = self._assign_qa_slot(supervisor)
        poller = supervisor._pollers["test-project"]
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json(
            passed=True,
        )

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Comment posted
        poller.add_comment.assert_called_once()
        comment_body = poller.add_comment.call_args[0][1]
        assert "QA Report — passed" in comment_body

        # qa-passed label added
        poller.add_labels.assert_called_once_with("TST-1", ["qa-passed"])

        # No bug tickets created
        poller.create_issue.assert_not_called()

        # Standalone QA passed → moved to Done
        poller.move_issue.assert_called_once_with("TST-1", "Done")

        # Slot freed
        assert sm.get_slot("test-project", 1).status == "free"

    def test_qa_failed_creates_bugs_and_labels(self, supervisor):
        sm, _ = self._assign_qa_slot(supervisor)
        poller = supervisor._pollers["test-project"]
        bugs = [
            {"title": "Crash on login", "description": "App crashes", "severity": "critical"},
            {"title": "Typo in header", "description": "Minor", "severity": "low"},
        ]
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json(
            passed=False, bugs=bugs,
        )

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Comment posted
        poller.add_comment.assert_called_once()

        # qa-failed label added
        poller.add_labels.assert_called_once_with("TST-1", ["qa-failed"])

        # 2 bug tickets created
        assert poller.create_issue.call_count == 2

        # Check first bug ticket
        call1 = poller.create_issue.call_args_list[0]
        assert call1.kwargs["title"] == "[QA Bug] Crash on login"
        assert call1.kwargs["priority"] == 1  # critical → 1
        assert "Bug" in call1.kwargs["label_names"]
        assert "QA" in call1.kwargs["label_names"]

        # Check second bug ticket
        call2 = poller.create_issue.call_args_list[1]
        assert call2.kwargs["title"] == "[QA Bug] Typo in header"
        assert call2.kwargs["priority"] == 4  # low → 4

        # Standalone QA failed → NOT moved to Done
        poller.move_issue.assert_not_called()

        # Slot freed
        assert sm.get_slot("test-project", 1).status == "free"

    def test_qa_completion_with_unparseable_result(self, supervisor):
        """When result_text isn't valid QA JSON, posts raw text and adds qa-failed."""
        sm, _ = self._assign_qa_slot(supervisor)
        poller = supervisor._pollers["test-project"]
        supervisor._pending_result_texts[("test-project", 1)] = "just plain text"

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Comment posted with raw text fallback
        poller.add_comment.assert_called_once()
        comment_body = poller.add_comment.call_args[0][1]
        assert "just plain text" in comment_body

        # Defaults to qa-failed when report can't be parsed (safe default)
        poller.add_labels.assert_called_once_with("TST-1", ["qa-failed"])

    def test_qa_completion_no_result_text(self, supervisor):
        """When there's no result_text at all, still posts comment and adds qa-failed."""
        sm, _ = self._assign_qa_slot(supervisor)
        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        poller.add_comment.assert_called_once()
        poller.add_labels.assert_called_once_with("TST-1", ["qa-failed"])

    def test_qa_slot_still_emits_completed_event(self, supervisor):
        """QA pipeline completions still emit a slot_completed event."""
        sm, _ = self._assign_qa_slot(supervisor)
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json()

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        events = get_events(supervisor._conn, event_type="slot_completed")
        assert len(events) == 1
        assert "TST-1" in events[0]["detail"]

    def test_qa_terminal_ticket_skips_status_move(self, supervisor):
        """If the ticket is already terminal, skip label and status actions."""
        sm, _ = self._assign_qa_slot(supervisor)
        poller = supervisor._pollers["test-project"]
        poller.is_issue_terminal.return_value = True
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json(
            passed=True,
        )

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Comment still posted
        poller.add_comment.assert_called_once()

        # Label still added (labels are useful even on terminal tickets)
        poller.add_labels.assert_called_once()

        # But status move is skipped
        poller.move_issue.assert_not_called()

    def test_non_qa_pipeline_not_affected(self, supervisor):
        """Regular (non-QA) completed slots follow the normal flow."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Regular", branch="b1",
        )
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Normal flow: moved to In Review
        poller.move_issue.assert_called_once_with("TST-1", "In Review")
        # No qa labels
        poller.add_labels.assert_not_called()

    def test_bug_creation_failure_does_not_block(self, supervisor):
        """If creating a bug ticket fails, other bugs are still attempted."""
        sm, _ = self._assign_qa_slot(supervisor)
        poller = supervisor._pollers["test-project"]
        poller.create_issue.side_effect = [
            Exception("API error"),
            CreatedIssue(id="id2", identifier="TST-100", url="https://test/100"),
        ]
        bugs = [
            {"title": "Bug 1", "severity": "high"},
            {"title": "Bug 2", "severity": "medium"},
        ]
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json(
            passed=False, bugs=bugs,
        )

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Both bugs attempted despite first failure
        assert poller.create_issue.call_count == 2
        # Slot still freed
        assert sm.get_slot("test-project", 1).status == "free"

    def test_qa_with_implementation_stages(self, supervisor):
        """QA pipeline that also includes implementation stages — bugs tracked separately."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Impl+QA", branch="b1",
        )
        slot = sm.get_slot("test-project", 1)
        slot.stages_completed = ["implement", "review", "qa"]
        slot.pr_url = "https://github.com/test/repo/pull/1"
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]
        bugs = [{"title": "UI glitch", "severity": "medium"}]
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json(
            passed=False, bugs=bugs,
        )

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Has a PR URL → not standalone QA.  Normal completion flow runs
        # and moves the ticket to In Review (PR not merged).
        poller.move_issue.assert_called_once_with("TST-1", "In Review")

        # Bug ticket still created
        assert poller.create_issue.call_count == 1

        # qa-failed label added
        poller.add_labels.assert_called_once_with("TST-1", ["qa-failed"])

    def test_no_pr_detected_from_comments_not_standalone(self, supervisor):
        """When no-PR is detected from task comments (not slot.no_pr_reason),
        the QA handler should treat it as non-standalone and NOT try to
        move the ticket (normal flow already handled it)."""
        sm, slot = self._assign_qa_slot(supervisor)
        poller = supervisor._pollers["test-project"]
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json(
            passed=False,
        )

        # Simulate no_pr detected from task comments: insert a task with
        # comments containing the NO_PR_NEEDED marker.
        task_id = insert_task(
            supervisor._conn,
            ticket_id="TST-1", title="QA Test", project="test-project", slot=1,
        )
        update_task(supervisor._conn, task_id, comments="NO_PR_NEEDED: investigation only")
        supervisor._conn.commit()

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # Normal flow detects no_pr from comments → moves to Done.
        # QA handler sees no_pr is set → NOT standalone → does NOT try to
        # move or log "leaving open".  Only one move_issue call (from normal flow).
        poller.move_issue.assert_called_once_with("TST-1", "Done")

        # QA side effects still run (label, comment)
        poller.add_labels.assert_called_once_with("TST-1", ["qa-failed"])
        poller.add_comment.assert_called_once()


# ---------------------------------------------------------------------------
# BugtrackerPoller.create_issue tests
# ---------------------------------------------------------------------------


class TestPollerCreateIssue:
    def test_create_issue_resolves_team_and_labels(self):
        """Poller.create_issue resolves team_id and label_ids from names."""
        from botfarm.bugtracker.base import BugtrackerPoller

        mock_client = MagicMock()
        mock_client.get_team_id.return_value = "team-uuid"
        mock_client.get_or_create_label.side_effect = ["label-1", "label-2"]
        mock_client.create_issue.return_value = CreatedIssue(
            id="issue-uuid", identifier="TST-50", url="https://test/50",
        )

        mock_project = MagicMock()
        mock_project.team = "TST"

        # Create a concrete subclass since BugtrackerPoller is abstract
        class ConcretePoller(BugtrackerPoller):
            def get_state_id(self, state_name):
                return "state-id"

        poller = ConcretePoller(
            client=mock_client,
            project=mock_project,
            exclude_tags=[],
            coder_client=mock_client,
        )

        result = poller.create_issue(
            title="Test bug",
            description="Details",
            priority=2,
            label_names=["Bug", "QA"],
        )

        mock_client.get_team_id.assert_called_once_with("TST")
        assert mock_client.get_or_create_label.call_count == 2
        mock_client.create_issue.assert_called_once_with(
            team_id="team-uuid",
            title="Test bug",
            description="Details",
            priority=2,
            label_ids=["label-1", "label-2"],
            project_id=None,
        )
        assert result.identifier == "TST-50"


# ---------------------------------------------------------------------------
# _maybe_trigger_qa_ticket tests
# ---------------------------------------------------------------------------


class TestMaybeTriggerQaTicket:
    """Tests for auto-creating QA tickets after implementation completion."""

    def _assign_impl_slot(self, supervisor, *, labels=None, pr_url=None):
        """Assign a completed implementation ticket with given labels."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="Add login page", branch="b1",
            ticket_labels=labels or [],
        )
        slot = sm.get_slot("test-project", 1)
        slot.pr_url = pr_url
        slot.stages_completed = ["implement", "review"]
        sm.mark_completed("test-project", 1)
        return sm, slot

    def test_creates_qa_ticket_when_manual_qa_label_and_pr(self, supervisor):
        """A completed ticket with manual-qa label and a PR triggers QA ticket creation."""
        sm, _ = self._assign_impl_slot(
            supervisor,
            labels=["manual-qa"],
            pr_url="https://github.com/test/repo/pull/42",
        )
        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.return_value = MagicMock(priority=2)

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", None)):
            supervisor._handle_finished_slots()

        poller.create_issue.assert_called_once()
        call_kwargs = poller.create_issue.call_args.kwargs
        assert call_kwargs["title"] == "QA: Add login page"
        assert "TST-1" in call_kwargs["description"]
        assert "https://github.com/test/repo/pull/42" in call_kwargs["description"]
        assert call_kwargs["label_names"] == ["manual-qa"]
        assert call_kwargs["priority"] == 2

    def test_skips_when_no_manual_qa_label(self, supervisor):
        """Tickets without manual-qa label don't trigger QA ticket creation."""
        sm, _ = self._assign_impl_slot(
            supervisor,
            labels=["Feature"],
            pr_url="https://github.com/test/repo/pull/42",
        )
        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", None)):
            supervisor._handle_finished_slots()

        poller.create_issue.assert_not_called()

    def test_skips_when_no_pr_url(self, supervisor):
        """Tickets without a PR URL don't trigger QA (e.g. investigation tickets)."""
        sm, _ = self._assign_impl_slot(
            supervisor,
            labels=["manual-qa"],
            pr_url=None,
        )
        poller = supervisor._pollers["test-project"]

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        poller.create_issue.assert_not_called()

    def test_skips_for_qa_pipelines(self, supervisor):
        """Completed QA pipelines don't trigger another QA ticket (no infinite loop)."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title="QA: something", branch="b1",
            ticket_labels=["manual-qa"],
        )
        slot = sm.get_slot("test-project", 1)
        slot.stages_completed = ["qa"]
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]
        supervisor._pending_result_texts[("test-project", 1)] = _qa_report_json(
            passed=True,
        )

        with patch.object(supervisor, "_check_pr_status", return_value=(None, None)):
            supervisor._handle_finished_slots()

        # QA completion actions run, but no new QA ticket created
        poller.create_issue.assert_not_called()

    def test_manual_qa_label_case_insensitive(self, supervisor):
        """The manual-qa label check is case-insensitive."""
        sm, _ = self._assign_impl_slot(
            supervisor,
            labels=["Manual-QA"],
            pr_url="https://github.com/test/repo/pull/42",
        )
        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.return_value = MagicMock(priority=3)

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", None)):
            supervisor._handle_finished_slots()

        poller.create_issue.assert_called_once()
        assert poller.create_issue.call_args.kwargs["title"] == "QA: Add login page"

    def test_creation_failure_does_not_crash(self, supervisor):
        """If QA ticket creation fails, the slot still completes normally."""
        sm, _ = self._assign_impl_slot(
            supervisor,
            labels=["manual-qa"],
            pr_url="https://github.com/test/repo/pull/42",
        )
        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.return_value = MagicMock(priority=2)
        poller.create_issue.side_effect = Exception("API error")

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", None)):
            supervisor._handle_finished_slots()

        # Slot still freed despite creation failure
        assert sm.get_slot("test-project", 1).status == "free"

    def test_priority_fetch_failure_uses_none(self, supervisor):
        """If fetching priority fails, QA ticket is still created with no priority."""
        sm, _ = self._assign_impl_slot(
            supervisor,
            labels=["manual-qa"],
            pr_url="https://github.com/test/repo/pull/42",
        )
        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.side_effect = Exception("fetch failed")

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", None)):
            supervisor._handle_finished_slots()

        poller.create_issue.assert_called_once()
        assert poller.create_issue.call_args.kwargs["priority"] is None

    def test_uses_ticket_id_when_title_missing(self, supervisor):
        """When ticket_title is None, falls back to ticket_id."""
        sm = supervisor.slot_manager
        sm.assign_ticket(
            "test-project", 1,
            ticket_id="TST-1", ticket_title=None, branch="b1",
            ticket_labels=["manual-qa"],
        )
        slot = sm.get_slot("test-project", 1)
        slot.pr_url = "https://github.com/test/repo/pull/42"
        slot.stages_completed = ["implement"]
        sm.mark_completed("test-project", 1)

        poller = supervisor._pollers["test-project"]
        poller._client.fetch_issue_details.return_value = MagicMock(priority=3)

        with patch.object(supervisor, "_check_pr_status", return_value=("merged", None)):
            supervisor._handle_finished_slots()

        poller.create_issue.assert_called_once()
        assert poller.create_issue.call_args.kwargs["title"] == "QA: TST-1"
