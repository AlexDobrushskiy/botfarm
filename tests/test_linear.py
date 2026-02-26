"""Tests for botfarm.linear — LinearClient, LinearPoller, and create_pollers."""

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
from botfarm.linear import (
    LINEAR_API_URL,
    LinearAPIError,
    LinearClient,
    LinearIssue,
    LinearPoller,
    PollResult,
    create_pollers,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_project(
    name: str = "proj-a",
    team: str = "SMA",
    slots: list[int] | None = None,
) -> ProjectConfig:
    return ProjectConfig(
        name=name,
        linear_team=team,
        base_dir="~/proj-a",
        worktree_prefix="proj-a-slot-",
        slots=slots or [1, 2],
    )


def _make_issue(
    *,
    id: str = "id-1",
    identifier: str = "SMA-10",
    title: str = "Test issue",
    priority: int = 2,
    labels: list[str] | None = None,
    assignee_id: str | None = None,
    sort_order: float = 0.0,
    blocked_by: list[str] | None = None,
    children_states: list[tuple[str, str]] | None = None,
) -> LinearIssue:
    return LinearIssue(
        id=id,
        identifier=identifier,
        title=title,
        priority=priority,
        url=f"https://linear.app/test/{identifier}",
        assignee_id=assignee_id,
        labels=labels or [],
        sort_order=sort_order,
        blocked_by=blocked_by,
        children_states=children_states,
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
            _make_issue(id="a", identifier="SMA-1", sort_order=3.0),
            _make_issue(id="b", identifier="SMA-2", sort_order=1.0),
            _make_issue(id="c", identifier="SMA-3", sort_order=2.0),
        ]
        result = poller.poll()
        assert [c.identifier for c in result.candidates] == ["SMA-2", "SMA-3", "SMA-1"]

    def test_sort_order_ignores_priority(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", priority=1, sort_order=3.0),
            _make_issue(id="b", identifier="SMA-2", priority=4, sort_order=1.0),
            _make_issue(id="c", identifier="SMA-3", priority=2, sort_order=2.0),
        ]
        result = poller.poll()
        # Sort is by sort_order, not priority
        assert [c.identifier for c in result.candidates] == ["SMA-2", "SMA-3", "SMA-1"]

    def test_filters_blocked_tickets(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", sort_order=1.0),
            _make_issue(id="b", identifier="SMA-2", sort_order=2.0, blocked_by=["SMA-1"]),
            _make_issue(id="c", identifier="SMA-3", sort_order=3.0),
        ]
        result = poller.poll()
        assert [c.identifier for c in result.candidates] == ["SMA-1", "SMA-3"]
        assert [b.identifier for b in result.blocked] == ["SMA-2"]

    def test_filters_inverse_blocked_tickets(self):
        """Tickets blocked via inverseRelations are also filtered out."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", sort_order=1.0),
            _make_issue(id="b", identifier="SMA-2", sort_order=2.0, blocked_by=["SMA-1"]),
            _make_issue(id="c", identifier="SMA-3", sort_order=3.0),
        ]
        result = poller.poll()
        # SMA-2 is blocked (regardless of how blocked_by was populated)
        assert [c.identifier for c in result.candidates] == ["SMA-1", "SMA-3"]
        assert [b.identifier for b in result.blocked] == ["SMA-2"]

    def test_unblocked_tickets_not_filtered(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", sort_order=1.0, blocked_by=None),
            _make_issue(id="b", identifier="SMA-2", sort_order=2.0, blocked_by=[]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 2
        assert len(result.blocked) == 0

    def test_blocked_issues_sorted_by_sort_order(self):
        """Blocked issues should be sorted by sort_order."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", sort_order=3.0, blocked_by=["SMA-10"]),
            _make_issue(id="b", identifier="SMA-2", sort_order=1.0, blocked_by=["SMA-10"]),
            _make_issue(id="c", identifier="SMA-3", sort_order=2.0, blocked_by=["SMA-10"]),
        ]
        result = poller.poll()
        assert [b.identifier for b in result.blocked] == ["SMA-2", "SMA-3", "SMA-1"]

    def test_excludes_tags(self):
        poller = self._make_poller(exclude_tags=["Human", "Manual"])
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", labels=["Feature"]),
            _make_issue(id="b", identifier="SMA-2", labels=["Human"]),
            _make_issue(id="c", identifier="SMA-3", labels=["Bug", "manual"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "SMA-1"

    def test_exclude_tags_case_insensitive(self):
        poller = self._make_poller(exclude_tags=["human"])
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", labels=["HUMAN"]),
            _make_issue(id="b", identifier="SMA-2", labels=["Feature"]),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "SMA-2"

    def test_excludes_active_ticket_ids(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="active-1", identifier="SMA-1"),
            _make_issue(id="new-1", identifier="SMA-2"),
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
            _make_issue(id="a", identifier="SMA-1", labels=None),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1

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
            _make_issue(
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
            _make_issue(
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
            _make_issue(
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
            _make_issue(
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
            _make_issue(id="a", identifier="SMA-1"),
        ]
        result = poller.poll()
        assert len(result.candidates) == 1
        assert len(result.auto_close_parents) == 0

    def test_mixed_parents_and_normal(self):
        """Mix of parent and normal issues are properly separated."""
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", sort_order=1.0),
            _make_issue(
                id="p1", identifier="SMA-100", sort_order=2.0,
                children_states=[("SMA-101", "completed")],
            ),
            _make_issue(
                id="p2", identifier="SMA-200", sort_order=3.0,
                children_states=[("SMA-201", "started")],
            ),
            _make_issue(id="b", identifier="SMA-2", sort_order=4.0),
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
            linear=LinearConfig(api_key="test-key", exclude_tags=["Human", "Skip"]),
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
            linear=LinearConfig(api_key="key"),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._client is pollers[1]._client

    def test_exclude_tags_passed(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            linear=LinearConfig(api_key="key", exclude_tags=["Human", "Bot"]),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._exclude_tags == {"human", "bot"}

    def test_todo_status_passed(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            linear=LinearConfig(api_key="key", todo_status="Backlog"),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._todo_status == "Backlog"

    def test_todo_status_default(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            linear=LinearConfig(api_key="key"),
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
        from botfarm.linear import ISSUES_WITH_PROJECT_QUERY

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
        from botfarm.linear import ISSUES_QUERY

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
        from botfarm.linear import ISSUES_QUERY

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
        project.linear_project = "Botfarm"
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
            linear=LinearConfig(api_key="owner-key"),
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
            linear=LinearConfig(api_key="owner-key"),
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
            linear=LinearConfig(api_key="owner-key"),
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
            linear=LinearConfig(api_key="owner-key"),
            database=DatabaseConfig(),
            identities=IdentitiesConfig(
                coder=CoderIdentity(linear_api_key="coder-key"),
            ),
        )
        pollers = create_pollers(config)
        assert pollers[0]._coder_client is pollers[1]._coder_client
        assert pollers[0]._client is pollers[1]._client
        assert pollers[0]._coder_client is not pollers[0]._client
