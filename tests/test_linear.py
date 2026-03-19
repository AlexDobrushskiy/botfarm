"""Tests for botfarm.bugtracker.linear — LinearClient, LinearPoller, and create_pollers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from botfarm.config import (
    BotfarmConfig,
    CoderIdentity,
    DatabaseConfig,
    IdentitiesConfig,
    LinearConfig,
    ProjectConfig,
)
from botfarm.bugtracker.linear.client import LINEAR_API_URL, LinearAPIError, LinearClient
from botfarm.bugtracker.linear.poller import LinearPoller, create_pollers
from botfarm.bugtracker.linear.queries import (
    ACTIVE_ISSUES_COUNT_QUERY,
    ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY,
    ISSUE_DETAILS_QUERY,
)
from botfarm.bugtracker.types import ActiveIssuesCount, Issue as LinearIssue, PollResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


from tests.helpers import make_issue


def _make_project(
    name: str = "proj-a",
    team: str = "SMA",
    slots: list[int] | None = None,
) -> ProjectConfig:
    return ProjectConfig(
        name=name,
        team=team,
        base_dir="~/proj-a",
        worktree_prefix="proj-a-slot-",
        slots=slots or [1, 2],
    )


def _graphql_response(data: dict) -> httpx.Response:
    """Build a fake successful httpx.Response containing a GraphQL data payload."""
    return httpx.Response(
        status_code=200,
        json={"data": data},
        request=httpx.Request("POST", LINEAR_API_URL),
    )


def _graphql_error_response(messages: list[str]) -> httpx.Response:
    errors = [{"message": m} for m in messages]
    return httpx.Response(
        status_code=200,
        json={"errors": errors},
        request=httpx.Request("POST", LINEAR_API_URL),
    )


# ---------------------------------------------------------------------------
# LinearClient._execute
# ---------------------------------------------------------------------------


class TestLinearClientExecute:
    def test_successful_query(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({"viewer": {"id": "u1"}})

        with patch.object(httpx, "post", return_value=resp) as mock_post:
            data = client._execute("query { viewer { id } }")

        assert data == {"viewer": {"id": "u1"}}
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs.kwargs["headers"]["Authorization"] == "test-key"

    def test_sends_variables(self):
        client = LinearClient(api_key="key")
        resp = _graphql_response({"issues": {}})

        with patch.object(httpx, "post", return_value=resp) as mock_post:
            client._execute("query Q($a: String!) { issues }", {"a": "val"})

        body = mock_post.call_args.kwargs["json"]
        assert body["variables"] == {"a": "val"}

    def test_http_error_raises(self):
        client = LinearClient(api_key="key")
        with patch.object(
            httpx,
            "post",
            side_effect=httpx.ConnectError("connection refused"),
        ):
            with pytest.raises(LinearAPIError, match="HTTP request failed"):
                client._execute("query { viewer { id } }")

    def test_graphql_errors_raise(self):
        client = LinearClient(api_key="key")
        resp = _graphql_error_response(["Unauthorized", "Bad field"])
        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="Unauthorized.*Bad field"):
                client._execute("query { viewer { id } }")

    def test_http_status_error_includes_status_code(self):
        client = LinearClient(api_key="key")
        resp = httpx.Response(
            status_code=500,
            text="Internal Server Error",
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="HTTP 500"):
                client._execute("query { viewer { id } }")

    def test_timeout_error_raises(self):
        client = LinearClient(api_key="key")
        with patch.object(
            httpx,
            "post",
            side_effect=httpx.TimeoutException("timed out"),
        ):
            with pytest.raises(LinearAPIError, match="HTTP request failed"):
                client._execute("query { viewer { id } }")

    def test_invalid_json_raises(self):
        client = LinearClient(api_key="key")
        resp = httpx.Response(
            status_code=200,
            text="<html>not json</html>",
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="invalid JSON"):
                client._execute("query { viewer { id } }")

    def test_missing_data_key_raises(self):
        client = LinearClient(api_key="key")
        resp = httpx.Response(
            status_code=200,
            json={"something": "else"},
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="missing 'data' key"):
                client._execute("query { viewer { id } }")

    def test_null_data_raises(self):
        client = LinearClient(api_key="key")
        resp = httpx.Response(
            status_code=200,
            json={"data": None},
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="missing 'data' key"):
                client._execute("query { viewer { id } }")


# ---------------------------------------------------------------------------
# LinearClient.fetch_team_issues
# ---------------------------------------------------------------------------


class TestFetchTeamIssues:
    def _issues_response(self, nodes: list[dict]) -> httpx.Response:
        return _graphql_response({
            "issues": {
                "nodes": nodes,
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        })

    def test_returns_issues(self):
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "First",
                "priority": 1,
                "sortOrder": 1.5,
                "url": "https://linear.app/SMA-10",
                "assignee": {"id": "u1", "email": "bot@example.com"},
                "labels": {"nodes": [{"name": "Feature"}, {"name": "Human"}]},
                "relations": {"nodes": []},
            },
            {
                "id": "id-2",
                "identifier": "SMA-11",
                "title": "Second",
                "priority": 3,
                "sortOrder": 2.0,
                "url": "https://linear.app/SMA-11",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {"nodes": []},
            },
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")

        assert len(issues) == 2
        assert issues[0].id == "id-1"
        assert issues[0].identifier == "SMA-10"
        assert issues[0].priority == 1
        assert issues[0].sort_order == 1.5
        assert issues[0].assignee_id == "u1"
        assert issues[0].assignee_email == "bot@example.com"
        assert issues[0].labels == ["Feature", "Human"]
        assert issues[0].blocked_by is None

        assert issues[1].assignee_id is None
        assert issues[1].labels == []
        assert issues[1].sort_order == 2.0

    def test_empty_response(self):
        client = LinearClient(api_key="key")
        with patch.object(
            httpx, "post", return_value=self._issues_response([])
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues == []

    def test_missing_optional_fields(self):
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-3",
                "identifier": "SMA-12",
                "title": "Minimal",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {"nodes": []},
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].priority == 4  # default
        assert issues[0].url == ""
        assert issues[0].sort_order == 0.0

    def test_parses_blocked_by_relations(self):
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Blocked issue",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {
                    "nodes": [
                        {
                            "type": "isBlockedBy",
                            "relatedIssue": {
                                "identifier": "SMA-9",
                                "state": {"type": "started"},
                            },
                        },
                        {
                            "type": "blocks",
                            "relatedIssue": {
                                "identifier": "SMA-11",
                                "state": {"type": "unstarted"},
                            },
                        },
                    ]
                },
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].blocked_by == ["SMA-9"]

    def test_resolved_blockers_not_included(self):
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Was blocked",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {
                    "nodes": [
                        {
                            "type": "isBlockedBy",
                            "relatedIssue": {
                                "identifier": "SMA-9",
                                "state": {"type": "completed"},
                            },
                        },
                        {
                            "type": "isBlockedBy",
                            "relatedIssue": {
                                "identifier": "SMA-8",
                                "state": {"type": "canceled"},
                            },
                        },
                    ]
                },
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].blocked_by is None

    def test_parses_inverse_relations_blocks(self):
        """inverseRelations with type=blocks means this issue is blocked."""
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Blocked via inverse",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "inverseRelations": {
                    "nodes": [
                        {
                            "type": "blocks",
                            "issue": {
                                "identifier": "SMA-9",
                                "state": {"type": "started"},
                            },
                        },
                    ]
                },
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].blocked_by == ["SMA-9"]

    def test_resolved_inverse_blockers_not_included(self):
        """Resolved blockers via inverseRelations are ignored."""
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Was blocked via inverse",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "inverseRelations": {
                    "nodes": [
                        {
                            "type": "blocks",
                            "issue": {
                                "identifier": "SMA-9",
                                "state": {"type": "completed"},
                            },
                        },
                        {
                            "type": "blocks",
                            "issue": {
                                "identifier": "SMA-8",
                                "state": {"type": "canceled"},
                            },
                        },
                    ]
                },
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].blocked_by is None

    def test_both_relation_directions_combined(self):
        """Blockers from both relations and inverseRelations are combined."""
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Blocked both ways",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {
                    "nodes": [
                        {
                            "type": "isBlockedBy",
                            "relatedIssue": {
                                "identifier": "SMA-7",
                                "state": {"type": "started"},
                            },
                        },
                    ]
                },
                "inverseRelations": {
                    "nodes": [
                        {
                            "type": "blocks",
                            "issue": {
                                "identifier": "SMA-9",
                                "state": {"type": "started"},
                            },
                        },
                    ]
                },
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert sorted(issues[0].blocked_by) == ["SMA-7", "SMA-9"]

    def test_parses_children(self):
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Parent issue",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "children": {
                    "nodes": [
                        {
                            "identifier": "SMA-11",
                            "state": {"type": "completed", "name": "Done"},
                        },
                        {
                            "identifier": "SMA-12",
                            "state": {"type": "started", "name": "In Progress"},
                        },
                    ]
                },
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].children_states == [
            ("SMA-11", "completed"),
            ("SMA-12", "started"),
        ]

    def test_no_children_returns_none(self):
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Normal issue",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "children": {"nodes": []},
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].children_states is None

    def test_missing_children_field_returns_none(self):
        client = LinearClient(api_key="key")
        nodes = [
            {
                "id": "id-1",
                "identifier": "SMA-10",
                "title": "Normal issue",
                "priority": 2,
                "sortOrder": 1.0,
                "url": "https://linear.app/SMA-10",
                "assignee": None,
                "labels": {"nodes": []},
                "relations": {"nodes": []},
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].children_states is None


# ---------------------------------------------------------------------------
# LinearClient.get_team_states
# ---------------------------------------------------------------------------


class TestGetTeamStates:
    def test_returns_state_map(self):
        client = LinearClient(api_key="key")
        data = {
            "teams": {
                "nodes": [
                    {
                        "id": "t1",
                        "key": "SMA",
                        "states": {
                            "nodes": [
                                {"id": "s1", "name": "Backlog", "type": "backlog"},
                                {"id": "s2", "name": "Todo", "type": "unstarted"},
                                {"id": "s3", "name": "In Progress", "type": "started"},
                                {"id": "s4", "name": "Done", "type": "completed"},
                            ]
                        },
                    }
                ]
            }
        }
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            states = client.get_team_states("SMA")

        assert states == {
            "Backlog": "s1",
            "Todo": "s2",
            "In Progress": "s3",
            "Done": "s4",
        }

    def test_team_not_found_raises(self):
        client = LinearClient(api_key="key")
        data = {"teams": {"nodes": []}}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="not found"):
                client.get_team_states("NOPE")


# ---------------------------------------------------------------------------
# LinearClient.update_issue_state
# ---------------------------------------------------------------------------


class TestUpdateIssueState:
    def test_success(self):
        client = LinearClient(api_key="key")
        data = {
            "issueUpdate": {
                "success": True,
                "issue": {"id": "i1", "identifier": "SMA-1", "state": {"name": "In Progress"}},
            }
        }
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            client.update_issue_state("i1", "s3")  # should not raise

    def test_failure_raises(self):
        client = LinearClient(api_key="key")
        data = {"issueUpdate": {"success": False, "issue": None}}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to update"):
                client.update_issue_state("i1", "s3")

    def test_missing_top_level_key_raises(self):
        client = LinearClient(api_key="key")
        data = {"unexpected": "response"}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to update"):
                client.update_issue_state("i1", "s3")


# ---------------------------------------------------------------------------
# LinearClient.add_comment
# ---------------------------------------------------------------------------


class TestAddComment:
    def test_success(self):
        client = LinearClient(api_key="key")
        data = {"commentCreate": {"success": True}}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            client.add_comment("i1", "Hello")

    def test_failure_raises(self):
        client = LinearClient(api_key="key")
        data = {"commentCreate": {"success": False}}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to add comment"):
                client.add_comment("i1", "Hello")

    def test_missing_top_level_key_raises(self):
        client = LinearClient(api_key="key")
        data = {"unexpected": "response"}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to add comment"):
                client.add_comment("i1", "Hello")


# ---------------------------------------------------------------------------
# LinearClient.get_viewer_id
# ---------------------------------------------------------------------------


class TestGetViewerId:
    def test_returns_viewer_id(self):
        client = LinearClient(api_key="key")
        data = {"viewer": {"id": "user-123", "name": "Bot User"}}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            viewer_id = client.get_viewer_id()
        assert viewer_id == "user-123"

    def test_missing_viewer_raises(self):
        client = LinearClient(api_key="key")
        data = {"viewer": None}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to retrieve viewer ID"):
                client.get_viewer_id()

    def test_missing_id_raises(self):
        client = LinearClient(api_key="key")
        data = {"viewer": {"id": None, "name": "Bot"}}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to retrieve viewer ID"):
                client.get_viewer_id()

    def test_empty_response_raises(self):
        client = LinearClient(api_key="key")
        data = {"unexpected": "response"}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to retrieve viewer ID"):
                client.get_viewer_id()


# ---------------------------------------------------------------------------
# LinearClient.assign_issue
# ---------------------------------------------------------------------------


class TestAssignIssue:
    def test_success(self):
        client = LinearClient(api_key="key")
        data = {
            "issueUpdate": {
                "success": True,
                "issue": {
                    "id": "i1",
                    "identifier": "SMA-1",
                    "assignee": {"id": "u1", "name": "Bot"},
                },
            }
        }
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            client.assign_issue("i1", "u1")  # should not raise

    def test_failure_raises(self):
        client = LinearClient(api_key="key")
        data = {"issueUpdate": {"success": False, "issue": None}}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to assign issue"):
                client.assign_issue("i1", "u1")

    def test_missing_top_level_key_raises(self):
        client = LinearClient(api_key="key")
        data = {"unexpected": "response"}
        with patch.object(httpx, "post", return_value=_graphql_response(data)):
            with pytest.raises(LinearAPIError, match="Failed to assign issue"):
                client.assign_issue("i1", "u1")

    def test_sends_correct_variables(self):
        client = LinearClient(api_key="key")
        data = {
            "issueUpdate": {
                "success": True,
                "issue": {
                    "id": "issue-abc",
                    "identifier": "SMA-42",
                    "assignee": {"id": "user-xyz", "name": "Bot"},
                },
            }
        }
        with patch.object(httpx, "post", return_value=_graphql_response(data)) as mock_post:
            client.assign_issue("issue-abc", "user-xyz")

        body = mock_post.call_args.kwargs["json"]
        assert body["variables"] == {"issueId": "issue-abc", "assigneeId": "user-xyz"}


# ---------------------------------------------------------------------------
# LinearClient.add_labels
# ---------------------------------------------------------------------------


class TestLinearClientAddLabels:
    """Tests for LinearClient.add_labels — preserving existing labels."""

    def test_merges_new_labels_with_existing(self):
        client = LinearClient(api_key="key")
        # First call: fetch existing labels; second call: update
        fetch_resp = _graphql_response({
            "issue": {"labels": {"nodes": [{"id": "existing-1"}]}}
        })
        update_resp = _graphql_response({
            "issueUpdate": {"success": True, "issue": {"id": "i1"}}
        })
        with patch.object(httpx, "post", side_effect=[fetch_resp, update_resp]) as mock_post:
            client.add_labels("i1", ["new-1", "new-2"])

        # The update call should include both existing and new IDs
        update_body = mock_post.call_args_list[1].kwargs["json"]
        assert set(update_body["variables"]["labelIds"]) == {
            "existing-1", "new-1", "new-2",
        }

    def test_deduplicates_already_present_labels(self):
        client = LinearClient(api_key="key")
        fetch_resp = _graphql_response({
            "issue": {"labels": {"nodes": [{"id": "L1"}, {"id": "L2"}]}}
        })
        update_resp = _graphql_response({
            "issueUpdate": {"success": True, "issue": {"id": "i1"}}
        })
        with patch.object(httpx, "post", side_effect=[fetch_resp, update_resp]) as mock_post:
            client.add_labels("i1", ["L2", "L3"])

        update_body = mock_post.call_args_list[1].kwargs["json"]
        assert update_body["variables"]["labelIds"] == ["L1", "L2", "L3"]

    def test_handles_no_existing_labels(self):
        client = LinearClient(api_key="key")
        fetch_resp = _graphql_response({"issue": {"labels": {"nodes": []}}})
        update_resp = _graphql_response({
            "issueUpdate": {"success": True, "issue": {"id": "i1"}}
        })
        with patch.object(httpx, "post", side_effect=[fetch_resp, update_resp]) as mock_post:
            client.add_labels("i1", ["L1"])

        update_body = mock_post.call_args_list[1].kwargs["json"]
        assert update_body["variables"]["labelIds"] == ["L1"]

    def test_failure_raises(self):
        client = LinearClient(api_key="key")
        fetch_resp = _graphql_response({"issue": {"labels": {"nodes": []}}})
        update_resp = _graphql_response({
            "issueUpdate": {"success": False, "issue": None}
        })
        with patch.object(httpx, "post", side_effect=[fetch_resp, update_resp]):
            with pytest.raises(LinearAPIError, match="Failed to add labels"):
                client.add_labels("i1", ["L1"])


# ---------------------------------------------------------------------------
# LinearPoller.poll
# ---------------------------------------------------------------------------


class TestLinearPollerPoll:
    def _make_poller(self, exclude_tags: list[str] | None = None) -> LinearPoller:
        client = MagicMock(spec=LinearClient)
        project = _make_project()
        return LinearPoller(
            client=client,
            project=project,
            exclude_tags=exclude_tags or ["Human"],
        )

    def test_returns_sorted_by_sort_order(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", sort_order=3.0),
            make_issue(id="b", identifier="SMA-2", sort_order=1.0),
            make_issue(id="c", identifier="SMA-3", sort_order=2.0),
        ]
        result = poller.poll()
        assert [c.identifier for c in result.candidates] == ["SMA-2", "SMA-3", "SMA-1"]

    def test_sort_order_ignores_priority(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", priority=1, sort_order=3.0),
            make_issue(id="b", identifier="SMA-2", priority=4, sort_order=1.0),
            make_issue(id="c", identifier="SMA-3", priority=2, sort_order=2.0),
        ]
        result = poller.poll()
        # Sort is by sort_order, not priority
        assert [c.identifier for c in result.candidates] == ["SMA-2", "SMA-3", "SMA-1"]

    def test_filters_blocked_tickets(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", sort_order=1.0),
            make_issue(id="b", identifier="SMA-2", sort_order=2.0, blocked_by=["SMA-1"]),
            make_issue(id="c", identifier="SMA-3", sort_order=3.0),
        ]
        result = poller.poll()
        assert [c.identifier for c in result.candidates] == ["SMA-1", "SMA-3"]
        assert [b.identifier for b in result.blocked] == ["SMA-2"]

    def test_filters_inverse_blocked_tickets(self):
        """Tickets blocked via inverseRelations are also filtered out."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", sort_order=1.0),
            make_issue(id="b", identifier="SMA-2", sort_order=2.0, blocked_by=["SMA-1"]),
            make_issue(id="c", identifier="SMA-3", sort_order=3.0),
        ]
        result = poller.poll()
        # SMA-2 is blocked (regardless of how blocked_by was populated)
        assert [c.identifier for c in result.candidates] == ["SMA-1", "SMA-3"]
        assert [b.identifier for b in result.blocked] == ["SMA-2"]

    def test_unblocked_tickets_not_filtered(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", sort_order=1.0, blocked_by=None),
            make_issue(id="b", identifier="SMA-2", sort_order=2.0, blocked_by=[]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 2
        assert len(result.blocked) == 0

    def test_blocked_issues_sorted_by_sort_order(self):
        """Blocked issues should be sorted by sort_order."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", sort_order=3.0, blocked_by=["SMA-10"]),
            make_issue(id="b", identifier="SMA-2", sort_order=1.0, blocked_by=["SMA-10"]),
            make_issue(id="c", identifier="SMA-3", sort_order=2.0, blocked_by=["SMA-10"]),
        ]
        result = poller.poll()
        assert [b.identifier for b in result.blocked] == ["SMA-2", "SMA-3", "SMA-1"]

    def test_excludes_tags(self):
        poller = self._make_poller(exclude_tags=["Human", "Manual"])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", labels=["Feature"]),
            make_issue(id="b", identifier="SMA-2", labels=["Human"]),
            make_issue(id="c", identifier="SMA-3", labels=["Bug", "manual"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "SMA-1"

    def test_exclude_tags_case_insensitive(self):
        poller = self._make_poller(exclude_tags=["human"])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", labels=["HUMAN"]),
            make_issue(id="b", identifier="SMA-2", labels=["Feature"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "SMA-2"

    def test_excludes_active_ticket_ids(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="active-1", identifier="SMA-1"),
            make_issue(id="new-1", identifier="SMA-2"),
        ]
        result = poller.poll(active_ticket_ids={"SMA-1"})
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "SMA-2"

    def test_empty_poll(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = []
        result = poller.poll()
        assert result.candidates == []
        assert result.auto_close_parents == []

    def test_none_labels_handled(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", labels=None),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1

    def test_filters_failed_label_defense_in_depth(self):
        """Issues with 'Failed' label are always skipped, even without exclude_tags."""
        poller = self._make_poller(exclude_tags=[])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", labels=["Bug"]),
            make_issue(id="b", identifier="SMA-2", labels=["Failed"]),
            make_issue(id="c", identifier="SMA-3", labels=["Feature", "Failed"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "SMA-1"

    def test_filters_failed_label_case_insensitive(self):
        """'Failed' label filter is case-insensitive."""
        poller = self._make_poller(exclude_tags=[])
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", labels=["FAILED"]),
            make_issue(id="b", identifier="SMA-2", labels=["Feature"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "SMA-2"

    def test_properties(self):
        poller = self._make_poller()
        assert poller.project_name == "proj-a"
        assert poller.team_key == "SMA"


# ---------------------------------------------------------------------------
# LinearPoller.poll — parent issue handling
# ---------------------------------------------------------------------------


class TestLinearPollerParentHandling:
    def _make_poller(self, exclude_tags: list[str] | None = None) -> LinearPoller:
        client = MagicMock(spec=LinearClient)
        project = _make_project()
        return LinearPoller(
            client=client,
            project=project,
            exclude_tags=exclude_tags or ["Human"],
        )

    def test_parent_all_children_done_auto_close(self):
        """Parent with all children completed goes to auto_close_parents."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(
                id="p1", identifier="SMA-100",
                children_states=[("SMA-101", "completed"), ("SMA-102", "completed")],
            ),
        ]
        result = poller.poll()
        assert len(result.candidates) == 0
        assert len(result.auto_close_parents) == 1
        assert result.auto_close_parents[0].identifier == "SMA-100"

    def test_parent_children_mixed_canceled_completed(self):
        """Parent with children in completed/canceled goes to auto_close."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(
                id="p1", identifier="SMA-100",
                children_states=[("SMA-101", "completed"), ("SMA-102", "canceled")],
            ),
        ]
        result = poller.poll()
        assert len(result.candidates) == 0
        assert len(result.auto_close_parents) == 1

    def test_parent_some_children_not_done_skipped(self):
        """Parent with some open children is skipped entirely."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(
                id="p1", identifier="SMA-100",
                children_states=[("SMA-101", "completed"), ("SMA-102", "started")],
            ),
        ]
        result = poller.poll()
        assert len(result.candidates) == 0
        assert len(result.auto_close_parents) == 0

    def test_parent_all_children_open_skipped(self):
        """Parent with all children still open is skipped."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(
                id="p1", identifier="SMA-100",
                children_states=[("SMA-101", "unstarted"), ("SMA-102", "started")],
            ),
        ]
        result = poller.poll()
        assert len(result.candidates) == 0
        assert len(result.auto_close_parents) == 0

    def test_no_children_treated_as_normal(self):
        """Issue with no children (children_states=None) is a normal candidate."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1"),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert len(result.auto_close_parents) == 0

    def test_mixed_parents_and_normal(self):
        """Mix of parent and normal issues are properly separated."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            make_issue(id="a", identifier="SMA-1", sort_order=1.0),
            make_issue(
                id="p1", identifier="SMA-100", sort_order=2.0,
                children_states=[("SMA-101", "completed")],
            ),
            make_issue(
                id="p2", identifier="SMA-200", sort_order=3.0,
                children_states=[("SMA-201", "started")],
            ),
            make_issue(id="b", identifier="SMA-2", sort_order=4.0),
        ]
        result = poller.poll()
        assert [c.identifier for c in result.candidates] == ["SMA-1", "SMA-2"]
        assert [p.identifier for p in result.auto_close_parents] == ["SMA-100"]

    def test_poll_returns_poll_result(self):
        """poll() returns a PollResult dataclass."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = []
        result = poller.poll()
        assert isinstance(result, PollResult)
        assert result.candidates == []
        assert result.blocked == []
        assert result.auto_close_parents == []


# ---------------------------------------------------------------------------
# LinearPoller.move_issue / get_state_id
# ---------------------------------------------------------------------------


class TestLinearPollerStateManagement:
    def _make_poller(self) -> LinearPoller:
        client = MagicMock(spec=LinearClient)
        client.get_team_states.return_value = {
            "Todo": "s1",
            "In Progress": "s2",
            "Done": "s3",
        }
        project = _make_project()
        return LinearPoller(client=client, project=project, exclude_tags=["Human"])

    def test_get_state_id(self):
        poller = self._make_poller()
        assert poller.get_state_id("In Progress") == "s2"

    def test_get_state_id_caches(self):
        poller = self._make_poller()
        poller.get_state_id("Todo")
        poller.get_state_id("Done")
        # get_team_states should only have been called once
        poller._client.get_team_states.assert_called_once()

    def test_get_state_id_unknown_raises(self):
        poller = self._make_poller()
        with pytest.raises(LinearAPIError, match="not found"):
            poller.get_state_id("Nonexistent")

    def test_move_issue(self):
        poller = self._make_poller()
        poller.move_issue("i1", "In Progress")
        poller._client.update_issue_state.assert_called_once_with("i1", "s2")

    def test_add_comment(self):
        poller = self._make_poller()
        poller.add_comment("i1", "Hello world")
        poller._client.add_comment.assert_called_once_with("i1", "Hello world")

    def test_assign_issue(self):
        poller = self._make_poller()
        poller.assign_issue("SMA-1", "user-123")
        poller._client.assign_issue.assert_called_once_with("SMA-1", "user-123")


# ---------------------------------------------------------------------------
# create_pollers
# ---------------------------------------------------------------------------


class TestCreatePollers:
    def test_creates_one_per_project(self):
        config = BotfarmConfig(
            projects=[
                _make_project(name="proj-a", team="SMA", slots=[1]),
                _make_project(name="proj-b", team="TPP", slots=[2]),
            ],
            bugtracker=LinearConfig(api_key="test-key", exclude_tags=["Human", "Skip"]),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert len(pollers) == 2
        assert pollers[0].project_name == "proj-a"
        assert pollers[0].team_key == "SMA"
        assert pollers[1].project_name == "proj-b"
        assert pollers[1].team_key == "TPP"

    def test_shares_client_instance(self):
        config = BotfarmConfig(
            projects=[
                _make_project(name="a", slots=[1]),
                _make_project(name="b", slots=[2]),
            ],
            bugtracker=LinearConfig(api_key="key"),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._client is pollers[1]._client

    def test_exclude_tags_passed(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            bugtracker=LinearConfig(api_key="key", exclude_tags=["Human", "Bot"]),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._exclude_tags == {"human", "bot"}

    def test_todo_status_passed(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            bugtracker=LinearConfig(api_key="key", todo_status="Backlog"),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._todo_status == "Backlog"

    def test_todo_status_default(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            bugtracker=LinearConfig(api_key="key"),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._todo_status == "Todo"


# ---------------------------------------------------------------------------
# LinearPoller.poll with custom todo_status
# ---------------------------------------------------------------------------


class TestLinearPollerCustomTodoStatus:
    def test_poll_uses_custom_todo_status(self):
        client = MagicMock(spec=LinearClient)
        client.fetch_team_issues.return_value = []
        project = _make_project()
        poller = LinearPoller(
            client=client,
            project=project,
            exclude_tags=["Human"],
            todo_status="Backlog",
        )
        poller.poll()
        client.fetch_team_issues.assert_called_once_with(
            team_key="SMA",
            status_name="Backlog",
            project_name="",
        )

    def test_poll_uses_default_todo_status(self):
        client = MagicMock(spec=LinearClient)
        client.fetch_team_issues.return_value = []
        project = _make_project()
        poller = LinearPoller(
            client=client,
            project=project,
            exclude_tags=["Human"],
        )
        poller.poll()
        client.fetch_team_issues.assert_called_once_with(
            team_key="SMA",
            status_name="Todo",
            project_name="",
        )


# ---------------------------------------------------------------------------
# LinearClient.fetch_team_issues with project_name
# ---------------------------------------------------------------------------


class TestFetchTeamIssuesWithProject:
    def _issues_response(self, nodes: list[dict]) -> httpx.Response:
        return _graphql_response({
            "issues": {
                "nodes": nodes,
                "pageInfo": {"hasNextPage": False, "endCursor": None},
            }
        })

    def test_uses_project_query_when_project_set(self):
        from botfarm.bugtracker.linear.queries import ISSUES_WITH_PROJECT_QUERY

        client = LinearClient(api_key="key")
        with patch.object(
            httpx, "post", return_value=self._issues_response([])
        ) as mock_post:
            client.fetch_team_issues("SMA", project_name="Botfarm")

        call_args = mock_post.call_args
        sent_query = call_args.kwargs.get("json", call_args[1].get("json", {}))["query"]
        assert sent_query == ISSUES_WITH_PROJECT_QUERY
        sent_vars = call_args.kwargs.get("json", call_args[1].get("json", {}))["variables"]
        assert sent_vars["projectName"] == "Botfarm"

    def test_uses_default_query_when_project_empty(self):
        from botfarm.bugtracker.linear.queries import ISSUES_QUERY

        client = LinearClient(api_key="key")
        with patch.object(
            httpx, "post", return_value=self._issues_response([])
        ) as mock_post:
            client.fetch_team_issues("SMA", project_name="")

        call_args = mock_post.call_args
        sent_query = call_args.kwargs.get("json", call_args[1].get("json", {}))["query"]
        assert sent_query == ISSUES_QUERY
        sent_vars = call_args.kwargs.get("json", call_args[1].get("json", {}))["variables"]
        assert "projectName" not in sent_vars

    def test_uses_default_query_when_project_omitted(self):
        from botfarm.bugtracker.linear.queries import ISSUES_QUERY

        client = LinearClient(api_key="key")
        with patch.object(
            httpx, "post", return_value=self._issues_response([])
        ) as mock_post:
            client.fetch_team_issues("SMA")

        call_args = mock_post.call_args
        sent_query = call_args.kwargs.get("json", call_args[1].get("json", {}))["query"]
        assert sent_query == ISSUES_QUERY


# ---------------------------------------------------------------------------
# LinearPoller.poll with linear_project filter
# ---------------------------------------------------------------------------


class TestLinearPollerProjectFilter:
    def test_poll_passes_project_name(self):
        client = MagicMock(spec=LinearClient)
        client.fetch_team_issues.return_value = []
        project = _make_project()
        project.tracker_project = "Botfarm"
        poller = LinearPoller(
            client=client,
            project=project,
            exclude_tags=["Human"],
        )
        poller.poll()
        client.fetch_team_issues.assert_called_once_with(
            team_key="SMA",
            status_name="Todo",
            project_name="Botfarm",
        )

    def test_poll_passes_empty_project_when_unset(self):
        client = MagicMock(spec=LinearClient)
        client.fetch_team_issues.return_value = []
        project = _make_project()
        poller = LinearPoller(
            client=client,
            project=project,
            exclude_tags=["Human"],
        )
        poller.poll()
        client.fetch_team_issues.assert_called_once_with(
            team_key="SMA",
            status_name="Todo",
            project_name="",
        )


# ---------------------------------------------------------------------------
# Coder client routing
# ---------------------------------------------------------------------------


class TestCoderClientRouting:
    """When a separate coder client is provided, coder operations use it."""

    def _make_poller_with_coder(self):
        owner_client = MagicMock(spec=LinearClient)
        coder_client = MagicMock(spec=LinearClient)
        owner_client.get_team_states.return_value = {
            "Todo": "s1",
            "In Progress": "s2",
            "Done": "s3",
        }
        project = _make_project()
        poller = LinearPoller(
            client=owner_client,
            project=project,
            exclude_tags=["Human"],
            coder_client=coder_client,
        )
        return poller, owner_client, coder_client

    def test_move_issue_uses_coder_client(self):
        poller, owner, coder = self._make_poller_with_coder()
        poller.move_issue("i1", "In Progress")
        coder.update_issue_state.assert_called_once_with("i1", "s2")
        owner.update_issue_state.assert_not_called()

    def test_add_comment_uses_coder_client(self):
        poller, owner, coder = self._make_poller_with_coder()
        poller.add_comment("i1", "Hello")
        coder.add_comment.assert_called_once_with("i1", "Hello")
        owner.add_comment.assert_not_called()

    def test_add_labels_uses_coder_client(self):
        poller, owner, coder = self._make_poller_with_coder()
        coder.get_or_create_label.side_effect = lambda team, name: f"id-{name}"
        poller.add_labels("i1", ["Failed", "Human"])
        # get_or_create_label should resolve names via coder client
        assert coder.get_or_create_label.call_count == 2
        coder.add_labels.assert_called_once_with("i1", ["id-Failed", "id-Human"])
        owner.add_labels.assert_not_called()

    def test_add_comment_as_owner_uses_owner_client(self):
        poller, owner, coder = self._make_poller_with_coder()
        poller.add_comment_as_owner("i1", "System message")
        owner.add_comment.assert_called_once_with("i1", "System message")
        coder.add_comment.assert_not_called()

    def test_poll_uses_owner_client(self):
        poller, owner, coder = self._make_poller_with_coder()
        owner.fetch_team_issues.return_value = []
        poller.poll()
        owner.fetch_team_issues.assert_called_once()
        coder.fetch_team_issues.assert_not_called()

    def test_assign_issue_uses_owner_client(self):
        poller, owner, coder = self._make_poller_with_coder()
        poller.assign_issue("i1", "user-123")
        owner.assign_issue.assert_called_once_with("i1", "user-123")
        coder.assign_issue.assert_not_called()

    def test_fallback_when_no_coder_client(self):
        """Without coder_client, all operations use the owner client."""
        client = MagicMock(spec=LinearClient)
        client.get_team_states.return_value = {
            "Todo": "s1", "In Progress": "s2", "Done": "s3",
        }
        project = _make_project()
        poller = LinearPoller(
            client=client, project=project, exclude_tags=["Human"],
        )
        poller.move_issue("i1", "In Progress")
        client.update_issue_state.assert_called_once_with("i1", "s2")

        poller.add_comment("i1", "test")
        client.add_comment.assert_called_once_with("i1", "test")

        poller.add_comment_as_owner("i1", "system")
        assert client.add_comment.call_count == 2


class TestCreatePollersCoderClient:
    """create_pollers creates a separate coder client when configured."""

    def test_coder_client_created_when_key_set(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            bugtracker=LinearConfig(api_key="owner-key"),
            database=DatabaseConfig(),
            identities=IdentitiesConfig(
                coder=CoderIdentity(linear_api_key="coder-key"),
            ),
        )
        pollers = create_pollers(config)
        poller = pollers[0]
        assert poller._coder_client is not poller._client
        assert poller._coder_client._api_key == "coder-key"
        assert poller._client._api_key == "owner-key"

    def test_no_coder_client_when_key_empty(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            bugtracker=LinearConfig(api_key="owner-key"),
            database=DatabaseConfig(),
            identities=IdentitiesConfig(
                coder=CoderIdentity(linear_api_key=""),
            ),
        )
        pollers = create_pollers(config)
        poller = pollers[0]
        assert poller._coder_client is poller._client

    def test_no_coder_client_when_identities_default(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            bugtracker=LinearConfig(api_key="owner-key"),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        poller = pollers[0]
        assert poller._coder_client is poller._client

    def test_coder_client_shared_across_pollers(self):
        config = BotfarmConfig(
            projects=[
                _make_project(name="a", slots=[1]),
                _make_project(name="b", slots=[2]),
            ],
            bugtracker=LinearConfig(api_key="owner-key"),
            database=DatabaseConfig(),
            identities=IdentitiesConfig(
                coder=CoderIdentity(linear_api_key="coder-key"),
            ),
        )
        pollers = create_pollers(config)
        assert pollers[0]._coder_client is pollers[1]._coder_client
        assert pollers[0]._client is pollers[1]._client
        assert pollers[0]._coder_client is not pollers[0]._client


class TestFetchIssueStateType:
    """Tests for LinearClient.fetch_issue_state_type."""

    def test_returns_state_type(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({
            "issue": {
                "id": "issue-id",
                "identifier": "TST-1",
                "state": {"name": "Done", "type": "completed"},
            }
        })
        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_state_type("TST-1")
        assert result == "completed"

    def test_returns_none_for_missing_issue(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({"issue": None})
        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_state_type("TST-999")
        assert result is None

    def test_returns_none_on_api_error(self):
        client = LinearClient(api_key="test-key")
        with patch.object(httpx, "post", side_effect=httpx.HTTPError("fail")):
            result = client.fetch_issue_state_type("TST-1")
        assert result is None

    def test_returns_started_type(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({
            "issue": {
                "id": "issue-id",
                "identifier": "TST-1",
                "state": {"name": "In Progress", "type": "started"},
            }
        })
        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_state_type("TST-1")
        assert result == "started"


class TestCountActiveIssues:
    """Tests for LinearClient.count_active_issues."""

    def _make_page(self, nodes, has_next=False, end_cursor=None):
        return _graphql_response({
            "issues": {
                "nodes": nodes,
                "pageInfo": {
                    "hasNextPage": has_next,
                    "endCursor": end_cursor,
                },
            }
        })

    def test_single_page(self):
        client = LinearClient(api_key="test-key")
        resp = self._make_page([
            {"id": "1", "project": {"name": "Alpha"}},
            {"id": "2", "project": {"name": "Alpha"}},
            {"id": "3", "project": {"name": "Beta"}},
        ])
        with patch.object(httpx, "post", return_value=resp):
            result = client.count_active_issues()
        assert result is not None
        assert result.total == 3
        assert result.by_project == {"Alpha": 2, "Beta": 1}

    def test_pagination(self):
        client = LinearClient(api_key="test-key")
        page1 = self._make_page(
            [{"id": "1", "project": {"name": "Alpha"}}],
            has_next=True,
            end_cursor="cursor-1",
        )
        page2 = self._make_page(
            [{"id": "2", "project": {"name": "Beta"}}],
        )
        with patch.object(httpx, "post", side_effect=[page1, page2]) as mock_post:
            result = client.count_active_issues()
        assert result is not None
        assert result.total == 2
        assert result.by_project == {"Alpha": 1, "Beta": 1}
        # Second call should include the cursor
        second_call_vars = mock_post.call_args_list[1].kwargs["json"]["variables"]
        assert second_call_vars["after"] == "cursor-1"

    def test_no_project(self):
        client = LinearClient(api_key="test-key")
        resp = self._make_page([
            {"id": "1", "project": None},
            {"id": "2", "project": {"name": "Alpha"}},
        ])
        with patch.object(httpx, "post", return_value=resp):
            result = client.count_active_issues()
        assert result is not None
        assert result.total == 2
        assert result.by_project == {"(no project)": 1, "Alpha": 1}

    def test_empty_result(self):
        client = LinearClient(api_key="test-key")
        resp = self._make_page([])
        with patch.object(httpx, "post", return_value=resp):
            result = client.count_active_issues()
        assert result is not None
        assert result.total == 0
        assert result.by_project == {}

    def test_returns_none_on_api_error(self):
        client = LinearClient(api_key="test-key")
        with patch.object(httpx, "post", side_effect=httpx.HTTPError("fail")):
            result = client.count_active_issues()
        assert result is None

    def test_uses_correct_query_and_page_size(self):
        client = LinearClient(api_key="test-key")
        resp = self._make_page([])
        with patch.object(httpx, "post", return_value=resp) as mock_post:
            client.count_active_issues()
        body = mock_post.call_args.kwargs["json"]
        assert body["query"] == ACTIVE_ISSUES_COUNT_QUERY
        assert body["variables"]["first"] == 250

    def test_query_does_not_filter_by_state(self):
        """Ensure the query counts ALL non-archived issues regardless of state.

        Linear's free plan counts completed/canceled issues toward the
        250 limit, so the capacity query must not exclude them.
        """
        assert "completed" not in ACTIVE_ISSUES_COUNT_QUERY
        assert "canceled" not in ACTIVE_ISSUES_COUNT_QUERY
        assert "state" not in ACTIVE_ISSUES_COUNT_QUERY


class TestCountActiveIssuesForProject:
    """Tests for LinearClient.count_active_issues_for_project."""

    def _make_page(self, nodes, has_next=False, end_cursor=None):
        return _graphql_response({
            "issues": {
                "nodes": nodes,
                "pageInfo": {
                    "hasNextPage": has_next,
                    "endCursor": end_cursor,
                },
            }
        })

    def test_returns_count_for_project(self):
        client = LinearClient(api_key="test-key")
        resp = self._make_page([{"id": "1"}, {"id": "2"}])
        with patch.object(httpx, "post", return_value=resp) as mock_post:
            count = client.count_active_issues_for_project("Alpha")
        assert count == 2
        body = mock_post.call_args.kwargs["json"]
        assert body["query"] == ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY
        assert body["variables"]["projectName"] == "Alpha"

    def test_returns_zero_when_no_issues(self):
        client = LinearClient(api_key="test-key")
        resp = self._make_page([])
        with patch.object(httpx, "post", return_value=resp):
            count = client.count_active_issues_for_project("Unknown")
        assert count == 0

    def test_paginates(self):
        client = LinearClient(api_key="test-key")
        page1 = self._make_page(
            [{"id": "1"}], has_next=True, end_cursor="cursor-1"
        )
        page2 = self._make_page([{"id": "2"}, {"id": "3"}])
        with patch.object(httpx, "post", side_effect=[page1, page2]) as mock_post:
            count = client.count_active_issues_for_project("Alpha")
        assert count == 3
        second_call_vars = mock_post.call_args_list[1].kwargs["json"]["variables"]
        assert second_call_vars["after"] == "cursor-1"
        assert second_call_vars["projectName"] == "Alpha"

    def test_returns_none_on_api_error(self):
        client = LinearClient(api_key="test-key")
        with patch.object(httpx, "post", side_effect=httpx.HTTPError("fail")):
            count = client.count_active_issues_for_project("Alpha")
        assert count is None

    def test_query_does_not_filter_by_state(self):
        """Ensure the project query counts ALL non-archived issues.

        Linear's free plan counts completed/canceled issues toward the
        250 limit, so the capacity query must not exclude them.
        """
        assert "completed" not in ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY
        assert "canceled" not in ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY
        assert "state" not in ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY


class TestIsIssueTerminal:
    """Tests for LinearPoller.is_issue_terminal."""

    def test_true_for_completed(self):
        client = MagicMock()
        client.fetch_issue_state_type.return_value = "completed"
        poller = LinearPoller(client, _make_project(), [])
        assert poller.is_issue_terminal("TST-1") is True

    def test_true_for_canceled(self):
        client = MagicMock()
        client.fetch_issue_state_type.return_value = "canceled"
        poller = LinearPoller(client, _make_project(), [])
        assert poller.is_issue_terminal("TST-1") is True

    def test_false_for_started(self):
        client = MagicMock()
        client.fetch_issue_state_type.return_value = "started"
        poller = LinearPoller(client, _make_project(), [])
        assert poller.is_issue_terminal("TST-1") is False

    def test_false_for_unstarted(self):
        client = MagicMock()
        client.fetch_issue_state_type.return_value = "unstarted"
        poller = LinearPoller(client, _make_project(), [])
        assert poller.is_issue_terminal("TST-1") is False

    def test_false_on_api_error(self):
        """API errors should not prevent recovery — return False."""
        client = MagicMock()
        client.fetch_issue_state_type.return_value = None
        poller = LinearPoller(client, _make_project(), [])
        assert poller.is_issue_terminal("TST-1") is False


# ---------------------------------------------------------------------------
# LinearClient.fetch_issue_details
# ---------------------------------------------------------------------------


def _full_issue_response():
    """Build a complete GraphQL response for fetch_issue_details."""
    return {
        "issue": {
            "id": "uuid-1",
            "identifier": "SMA-10",
            "title": "Test issue",
            "description": "# Description\nBody text",
            "priority": 2,
            "url": "https://linear.app/test/SMA-10",
            "estimate": 3.0,
            "dueDate": "2026-03-15",
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-02-01T00:00:00Z",
            "completedAt": None,
            "state": {"name": "In Progress"},
            "creator": {"name": "Alice"},
            "assignee": {"name": "Bot", "email": "bot@test.com"},
            "project": {"name": "Test Project"},
            "team": {"name": "Team A", "key": "SMA"},
            "parent": {"identifier": "SMA-5", "title": "Parent"},
            "children": {"nodes": [{"identifier": "SMA-11"}, {"identifier": "SMA-12"}]},
            "labels": {"nodes": [{"name": "bug"}, {"name": "p1"}]},
            "relations": {
                "nodes": [
                    {"type": "isBlockedBy", "relatedIssue": {"identifier": "SMA-8"}},
                    {"type": "blocks", "relatedIssue": {"identifier": "SMA-15"}},
                ]
            },
            "inverseRelations": {
                "nodes": [
                    {"type": "blocks", "relatedIssue": None, "issue": {"identifier": "SMA-9"}},
                ]
            },
            "comments": {
                "nodes": [
                    {"body": "First comment", "user": {"name": "Alice"}, "createdAt": "2026-01-02T00:00:00Z"},
                    {"body": "Second comment", "user": {"name": "Bob"}, "createdAt": "2026-01-03T00:00:00Z"},
                ]
            },
        }
    }


class TestFetchIssueDetails:
    def test_parses_full_response(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response(_full_issue_response())

        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_details("SMA-10")

        assert result.ticket_id == "SMA-10"
        assert result.id == "uuid-1"
        assert result.title == "Test issue"
        assert result.description == "# Description\nBody text"
        assert result.status == "In Progress"
        assert result.priority == 2
        assert result.assignee_name == "Bot"
        assert result.assignee_email == "bot@test.com"
        assert result.creator_name == "Alice"
        assert result.project_name == "Test Project"
        assert result.team_name == "Team A"
        assert result.estimate == 3.0
        assert result.due_date == "2026-03-15"
        assert result.parent_id == "SMA-5"
        assert result.created_at == "2026-01-01T00:00:00Z"

    def test_parses_children_ids(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response(_full_issue_response())

        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_details("SMA-10")

        assert result.children_ids == ["SMA-11", "SMA-12"]

    def test_parses_labels(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response(_full_issue_response())

        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_details("SMA-10")

        assert result.labels == ["bug", "p1"]

    def test_parses_blocking_relations(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response(_full_issue_response())

        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_details("SMA-10")

        assert "SMA-8" in result.blocked_by
        assert "SMA-9" in result.blocked_by  # from inverseRelations
        assert "SMA-15" in result.blocks

    def test_parses_comments(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response(_full_issue_response())

        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_details("SMA-10")

        assert len(result.comments) == 2
        assert result.comments[0].body == "First comment"
        assert result.comments[0].author == "Alice"

    def test_handles_missing_optional_fields(self):
        """Issue with no parent, no comments, no assignee, etc."""
        client = LinearClient(api_key="test-key")
        minimal_issue = {
            "issue": {
                "id": "uuid-2",
                "identifier": "SMA-20",
                "title": "Minimal issue",
                "description": None,
                "priority": 4,
                "url": "https://linear.app/test/SMA-20",
                "estimate": None,
                "dueDate": None,
                "createdAt": "2026-01-01T00:00:00Z",
                "updatedAt": "2026-01-01T00:00:00Z",
                "completedAt": None,
                "state": {"name": "Todo"},
                "creator": None,
                "assignee": None,
                "project": None,
                "team": None,
                "parent": None,
                "children": {"nodes": []},
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "inverseRelations": {"nodes": []},
                "comments": {"nodes": []},
            }
        }
        resp = _graphql_response(minimal_issue)

        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_details("SMA-20")

        assert result.ticket_id == "SMA-20"
        assert result.description is None
        assert result.assignee_name is None
        assert result.assignee_email is None
        assert result.creator_name is None
        assert result.project_name is None
        assert result.parent_id is None
        assert result.children_ids == []
        assert result.labels == []
        assert result.comments == []

    def test_raises_on_not_found(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response({"issue": None})

        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="not found"):
                client.fetch_issue_details("SMA-999")

    def test_includes_raw_json(self):
        client = LinearClient(api_key="test-key")
        resp = _graphql_response(_full_issue_response())

        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_issue_details("SMA-10")

        assert result.raw["id"] == "uuid-1"
        assert result.raw["identifier"] == "SMA-10"


# ---------------------------------------------------------------------------
# LinearClient.create_project / get_or_create_project
# ---------------------------------------------------------------------------


class TestLinearClientCreateProject:
    def test_creates_project_successfully(self):
        client = LinearClient(api_key="key")
        resp = _graphql_response({
            "projectCreate": {
                "success": True,
                "project": {"id": "proj-1", "name": "My Project"},
            }
        })
        with patch.object(httpx, "post", return_value=resp) as mock_post:
            result = client.create_project("team-uuid", "My Project")

        assert result == {"id": "proj-1", "name": "My Project"}
        body = mock_post.call_args.kwargs["json"]
        assert body["variables"]["input"]["name"] == "My Project"
        assert body["variables"]["input"]["teamIds"] == ["team-uuid"]

    def test_creates_project_with_description(self):
        client = LinearClient(api_key="key")
        resp = _graphql_response({
            "projectCreate": {
                "success": True,
                "project": {"id": "proj-2", "name": "Described"},
            }
        })
        with patch.object(httpx, "post", return_value=resp) as mock_post:
            result = client.create_project("team-uuid", "Described", description="A desc")

        body = mock_post.call_args.kwargs["json"]
        assert body["variables"]["input"]["description"] == "A desc"
        assert result["id"] == "proj-2"

    def test_failure_raises(self):
        client = LinearClient(api_key="key")
        resp = _graphql_response({
            "projectCreate": {"success": False, "project": None}
        })
        with patch.object(httpx, "post", return_value=resp):
            with pytest.raises(LinearAPIError, match="Failed to create project"):
                client.create_project("team-uuid", "Bad")


class TestLinearClientGetOrCreateProject:
    def test_returns_existing_project(self):
        client = LinearClient(api_key="key")
        # 1st call: get_team_id -> team states
        team_resp = _graphql_response({
            "teams": {"nodes": [{"id": "team-uuid", "key": "SMA", "states": {"nodes": []}}]}
        })
        # 2nd call: list_team_projects -> project found in this team
        projects_resp = _graphql_response({
            "team": {"projects": {"nodes": [{"id": "existing-id", "name": "Existing"}]}}
        })
        with patch.object(httpx, "post", side_effect=[team_resp, projects_resp]):
            result = client.get_or_create_project("SMA", "Existing")

        assert result == {"id": "existing-id", "name": "Existing"}

    def test_creates_when_not_found(self):
        client = LinearClient(api_key="key")
        # 1st call: get_team_id -> team states
        team_resp = _graphql_response({
            "teams": {"nodes": [{"id": "team-uuid", "key": "SMA", "states": {"nodes": []}}]}
        })
        # 2nd call: list_team_projects -> no matching project
        projects_resp = _graphql_response({
            "team": {"projects": {"nodes": []}}
        })
        # 3rd call: create_project
        create_resp = _graphql_response({
            "projectCreate": {
                "success": True,
                "project": {"id": "new-id", "name": "New Project"},
            }
        })
        with patch.object(
            httpx, "post", side_effect=[team_resp, projects_resp, create_resp]
        ):
            result = client.get_or_create_project("SMA", "New Project")

        assert result == {"id": "new-id", "name": "New Project"}
