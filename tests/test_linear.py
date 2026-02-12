"""Tests for botfarm.linear — LinearClient, LinearPoller, and create_pollers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from botfarm.config import BotfarmConfig, DatabaseConfig, LinearConfig, ProjectConfig
from botfarm.linear import (
    LINEAR_API_URL,
    LinearAPIError,
    LinearClient,
    LinearIssue,
    LinearPoller,
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
) -> LinearIssue:
    return LinearIssue(
        id=id,
        identifier=identifier,
        title=title,
        priority=priority,
        url=f"https://linear.app/test/{identifier}",
        assignee_id=assignee_id,
        labels=labels or [],
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
                "url": "https://linear.app/SMA-10",
                "assignee": {"id": "u1", "email": "bot@example.com"},
                "labels": {"nodes": [{"name": "Feature"}, {"name": "Human"}]},
            },
            {
                "id": "id-2",
                "identifier": "SMA-11",
                "title": "Second",
                "priority": 3,
                "url": "https://linear.app/SMA-11",
                "assignee": None,
                "labels": {"nodes": []},
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
        assert issues[0].assignee_id == "u1"
        assert issues[0].assignee_email == "bot@example.com"
        assert issues[0].labels == ["Feature", "Human"]

        assert issues[1].assignee_id is None
        assert issues[1].labels == []

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
            }
        ]
        with patch.object(
            httpx, "post", return_value=self._issues_response(nodes)
        ):
            issues = client.fetch_team_issues("SMA")
        assert issues[0].priority == 4  # default
        assert issues[0].url == ""


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

    def test_returns_sorted_by_priority(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", priority=3),
            _make_issue(id="b", identifier="SMA-2", priority=1),
            _make_issue(id="c", identifier="SMA-3", priority=2),
        ]
        candidates = poller.poll()
        assert [c.identifier for c in candidates] == ["SMA-2", "SMA-3", "SMA-1"]

    def test_no_priority_sorts_last(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", priority=0),
            _make_issue(id="b", identifier="SMA-2", priority=4),
            _make_issue(id="c", identifier="SMA-3", priority=1),
        ]
        candidates = poller.poll()
        assert [c.identifier for c in candidates] == ["SMA-3", "SMA-2", "SMA-1"]

    def test_excludes_tags(self):
        poller = self._make_poller(exclude_tags=["Human", "Manual"])
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", labels=["Feature"]),
            _make_issue(id="b", identifier="SMA-2", labels=["Human"]),
            _make_issue(id="c", identifier="SMA-3", labels=["Bug", "manual"]),
        ]
        candidates = poller.poll()
        assert len(candidates) == 1
        assert candidates[0].identifier == "SMA-1"

    def test_exclude_tags_case_insensitive(self):
        poller = self._make_poller(exclude_tags=["human"])
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", labels=["HUMAN"]),
            _make_issue(id="b", identifier="SMA-2", labels=["Feature"]),
        ]
        candidates = poller.poll()
        assert len(candidates) == 1
        assert candidates[0].identifier == "SMA-2"

    def test_excludes_active_ticket_ids(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="active-1", identifier="SMA-1"),
            _make_issue(id="new-1", identifier="SMA-2"),
        ]
        candidates = poller.poll(active_ticket_ids={"active-1"})
        assert len(candidates) == 1
        assert candidates[0].identifier == "SMA-2"

    def test_empty_poll(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = []
        assert poller.poll() == []

    def test_none_labels_handled(self):
        poller = self._make_poller()
        poller._client.fetch_team_issues.return_value = [
            _make_issue(id="a", identifier="SMA-1", labels=None),
        ]
        candidates = poller.poll()
        assert len(candidates) == 1

    def test_properties(self):
        poller = self._make_poller()
        assert poller.project_name == "proj-a"
        assert poller.team_key == "SMA"


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
            max_total_slots=5,
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
            max_total_slots=5,
            linear=LinearConfig(api_key="key"),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._client is pollers[1]._client

    def test_exclude_tags_passed(self):
        config = BotfarmConfig(
            projects=[_make_project(slots=[1])],
            max_total_slots=5,
            linear=LinearConfig(api_key="key", exclude_tags=["Human", "Bot"]),
            database=DatabaseConfig(),
        )
        pollers = create_pollers(config)
        assert pollers[0]._exclude_tags == {"human", "bot"}
