"""Tests for the Jira poller implementation."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from botfarm.bugtracker.base import BugtrackerClient
from botfarm.bugtracker.errors import BugtrackerError
from botfarm.bugtracker.jira.poller import JiraPoller
from botfarm.config import ProjectConfig
from tests.helpers import make_issue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_project(
    name: str = "proj-a",
    team: str = "PROJ",
    slots: list[int] | None = None,
    tracker_project: str = "",
) -> ProjectConfig:
    return ProjectConfig(
        name=name,
        team=team,
        base_dir="~/proj-a",
        worktree_prefix="proj-a-slot-",
        slots=slots or [1, 2],
        tracker_project=tracker_project,
    )


def _make_poller(
    exclude_tags: list[str] | None = None,
    coder_client: BugtrackerClient | None = None,
    todo_status: str = "Todo",
) -> JiraPoller:
    client = MagicMock(spec=BugtrackerClient)
    project = _make_project()
    return JiraPoller(
        client=client,
        project=project,
        exclude_tags=exclude_tags if exclude_tags is not None else ["Human"],
        todo_status=todo_status,
        coder_client=coder_client,
    )


# ---------------------------------------------------------------------------
# JiraPoller.poll
# ---------------------------------------------------------------------------


class TestJiraPollerPoll:
    def test_returns_sorted_by_sort_order(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", sort_order=3.0),
            make_issue(id="b", identifier="PROJ-2", sort_order=1.0),
            make_issue(id="c", identifier="PROJ-3", sort_order=2.0),
        ]
        result = poller.poll()
        assert [c.identifier for c in result.candidates] == ["PROJ-2", "PROJ-3", "PROJ-1"]

    def test_filters_blocked_tickets(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", sort_order=1.0),
            make_issue(id="b", identifier="PROJ-2", sort_order=2.0, blocked_by=["PROJ-1"]),
            make_issue(id="c", identifier="PROJ-3", sort_order=3.0),
        ]
        result = poller.poll()
        assert [c.identifier for c in result.candidates] == ["PROJ-1", "PROJ-3"]
        assert [b.identifier for b in result.blocked] == ["PROJ-2"]

    def test_excludes_tags(self):
        poller = _make_poller(exclude_tags=["Human", "Manual"])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", labels=["Feature"]),
            make_issue(id="b", identifier="PROJ-2", labels=["Human"]),
            make_issue(id="c", identifier="PROJ-3", labels=["Bug", "manual"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "PROJ-1"

    def test_exclude_tags_case_insensitive(self):
        poller = _make_poller(exclude_tags=["human"])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", labels=["HUMAN"]),
            make_issue(id="b", identifier="PROJ-2", labels=["Feature"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "PROJ-2"

    def test_excludes_active_ticket_ids(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="active-1", identifier="PROJ-1"),
            make_issue(id="new-1", identifier="PROJ-2"),
        ]
        result = poller.poll(active_ticket_ids={"PROJ-1"})
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "PROJ-2"

    def test_empty_poll(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = []
        result = poller.poll()
        assert result.candidates == []
        assert result.auto_close_parents == []

    def test_none_labels_handled(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", labels=None),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1

    def test_filters_failed_label_defense_in_depth(self):
        poller = _make_poller(exclude_tags=[])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", labels=["Bug"]),
            make_issue(id="b", identifier="PROJ-2", labels=["Failed"]),
            make_issue(id="c", identifier="PROJ-3", labels=["Feature", "Failed"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "PROJ-1"

    def test_filters_failed_label_case_insensitive(self):
        poller = _make_poller(exclude_tags=[])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", labels=["FAILED"]),
            make_issue(id="b", identifier="PROJ-2", labels=["Feature"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "PROJ-2"

    def test_blocked_issues_sorted_by_sort_order(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", sort_order=3.0, blocked_by=["PROJ-10"]),
            make_issue(id="b", identifier="PROJ-2", sort_order=1.0, blocked_by=["PROJ-10"]),
            make_issue(id="c", identifier="PROJ-3", sort_order=2.0, blocked_by=["PROJ-10"]),
        ]
        result = poller.poll()
        assert [b.identifier for b in result.blocked] == ["PROJ-2", "PROJ-3", "PROJ-1"]

    def test_properties(self):
        poller = _make_poller()
        assert poller.project_name == "proj-a"
        assert poller.team_key == "PROJ"


# ---------------------------------------------------------------------------
# JiraPoller.poll — parent issue handling
# ---------------------------------------------------------------------------


class TestJiraPollerParentHandling:
    def test_parent_all_children_done_auto_close(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(
                id="p1", identifier="PROJ-100",
                children_states=[("PROJ-101", "completed"), ("PROJ-102", "completed")],
            ),
        ]
        result = poller.poll()
        assert len(result.candidates) == 0
        assert len(result.auto_close_parents) == 1
        assert result.auto_close_parents[0].identifier == "PROJ-100"

    def test_parent_children_mix_canceled_completed(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(
                id="p1", identifier="PROJ-100",
                children_states=[("PROJ-101", "completed"), ("PROJ-102", "canceled")],
            ),
        ]
        result = poller.poll()
        assert len(result.auto_close_parents) == 1

    def test_parent_children_still_open_skipped(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(
                id="p1", identifier="PROJ-100",
                children_states=[("PROJ-101", "completed"), ("PROJ-102", "started")],
            ),
        ]
        result = poller.poll()
        assert len(result.candidates) == 0
        assert len(result.auto_close_parents) == 0

    def test_no_children_states_treated_as_leaf(self):
        poller = _make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="PROJ-1", children_states=None),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1


# ---------------------------------------------------------------------------
# JiraPoller state management — case-insensitive matching
# ---------------------------------------------------------------------------


class TestJiraPollerStateManagement:
    def _make_poller(self) -> JiraPoller:
        client = MagicMock(spec=BugtrackerClient)
        client.get_team_states.return_value = {
            "To Do": "s1",
            "In Progress": "s2",
            "Done": "s3",
        }
        project = _make_project()
        return JiraPoller(client=client, project=project, exclude_tags=["Human"])

    def test_get_state_id_exact_match(self):
        poller = self._make_poller()
        assert poller.get_state_id("In Progress") == "s2"

    def test_get_state_id_case_insensitive(self):
        """Jira status names should match case-insensitively."""
        poller = self._make_poller()
        assert poller.get_state_id("in progress") == "s2"
        assert poller.get_state_id("TO DO") == "s1"
        assert poller.get_state_id("done") == "s3"

    def test_get_state_id_caches(self):
        poller = self._make_poller()
        poller.get_state_id("To Do")
        poller.get_state_id("Done")
        poller._client.get_team_states.assert_called_once()

    def test_get_state_id_unknown_raises(self):
        poller = self._make_poller()
        with pytest.raises(BugtrackerError, match="not found"):
            poller.get_state_id("Nonexistent")

    def test_move_issue(self):
        poller = self._make_poller()
        poller.move_issue("i1", "In Progress")
        poller._client.update_issue_state.assert_called_once_with("i1", "s2")

    def test_move_issue_case_insensitive(self):
        poller = self._make_poller()
        poller.move_issue("i1", "in progress")
        poller._client.update_issue_state.assert_called_once_with("i1", "s2")

    def test_add_comment(self):
        poller = self._make_poller()
        poller.add_comment("i1", "Hello world")
        poller._client.add_comment.assert_called_once_with("i1", "Hello world")

    def test_assign_issue(self):
        poller = self._make_poller()
        poller.assign_issue("PROJ-1", "user-123")
        poller._client.assign_issue.assert_called_once_with("PROJ-1", "user-123")

    def test_is_issue_terminal_completed(self):
        poller = self._make_poller()
        poller._client.fetch_issue_state_type.return_value = "completed"
        assert poller.is_issue_terminal("PROJ-1") is True

    def test_is_issue_terminal_canceled(self):
        poller = self._make_poller()
        poller._client.fetch_issue_state_type.return_value = "canceled"
        assert poller.is_issue_terminal("PROJ-1") is True

    def test_is_issue_terminal_open(self):
        poller = self._make_poller()
        poller._client.fetch_issue_state_type.return_value = "started"
        assert poller.is_issue_terminal("PROJ-1") is False

    def test_is_issue_terminal_not_found(self):
        poller = self._make_poller()
        poller._client.fetch_issue_state_type.return_value = None
        assert poller.is_issue_terminal("PROJ-1") is False


# ---------------------------------------------------------------------------
# Coder client delegation
# ---------------------------------------------------------------------------


class TestJiraPollerCoderClient:
    def test_uses_coder_client_for_write_ops(self):
        """Write operations (move, comment, labels) use coder_client."""
        coder_client = MagicMock(spec=BugtrackerClient)
        coder_client.get_team_states.return_value = {
            "To Do": "s1",
            "In Progress": "s2",
        }
        poller = _make_poller(coder_client=coder_client)

        poller.add_comment("i1", "test")
        coder_client.add_comment.assert_called_once_with("i1", "test")
        poller._client.add_comment.assert_not_called()

    def test_uses_owner_client_for_assign(self):
        """assign_issue uses the owner client (not coder)."""
        coder_client = MagicMock(spec=BugtrackerClient)
        poller = _make_poller(coder_client=coder_client)

        poller.assign_issue("i1", "u1")
        poller._client.assign_issue.assert_called_once_with("i1", "u1")
        coder_client.assign_issue.assert_not_called()

    def test_add_comment_as_owner(self):
        """add_comment_as_owner uses the owner client."""
        coder_client = MagicMock(spec=BugtrackerClient)
        poller = _make_poller(coder_client=coder_client)

        poller.add_comment_as_owner("i1", "system note")
        poller._client.add_comment.assert_called_once_with("i1", "system note")
        coder_client.add_comment.assert_not_called()

    def test_falls_back_to_owner_when_no_coder(self):
        """Without coder_client, write ops use the owner client."""
        poller = _make_poller(coder_client=None)
        poller.add_comment("i1", "test")
        poller._client.add_comment.assert_called_once_with("i1", "test")

    def test_add_labels(self):
        """add_labels resolves label names to IDs and delegates to coder client."""
        coder_client = MagicMock(spec=BugtrackerClient)
        coder_client.get_or_create_label.side_effect = lambda team, name: f"id-{name}"
        poller = _make_poller(coder_client=coder_client)

        poller.add_labels("i1", ["Bug", "Urgent"])
        coder_client.get_or_create_label.assert_any_call("PROJ", "Bug")
        coder_client.get_or_create_label.assert_any_call("PROJ", "Urgent")
        coder_client.add_labels.assert_called_once_with("i1", ["id-Bug", "id-Urgent"])


# ---------------------------------------------------------------------------
# Custom todo status
# ---------------------------------------------------------------------------


class TestJiraPollerCustomTodoStatus:
    def test_poll_uses_custom_todo_status(self):
        poller = _make_poller(todo_status="Backlog")
        poller._client.fetch_team_issues.return_value = []
        poller.poll()
        poller._client.fetch_team_issues.assert_called_once_with(
            team_key="PROJ",
            status_name="Backlog",
            project_name="",
        )
