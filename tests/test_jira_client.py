"""Tests for botfarm.bugtracker.jira.client — JiraClient REST API adapter."""

from __future__ import annotations

import base64
from unittest.mock import patch

import httpx
import pytest

from botfarm.bugtracker.jira.client import JiraAPIError, JiraClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

JIRA_URL = "https://jira.example.com"


def _jira_response(
    status_code: int = 200,
    json_data: dict | list | None = None,
    headers: dict | None = None,
) -> httpx.Response:
    """Build a fake httpx.Response for Jira REST API calls."""
    return httpx.Response(
        status_code=status_code,
        json=json_data,
        headers=headers or {},
        request=httpx.Request("GET", f"{JIRA_URL}/rest/api/2/test"),
    )


def _make_client(
    url: str = JIRA_URL,
    email: str = "user@example.com",
    api_token: str = "test-token",
) -> JiraClient:
    """Create a JiraClient with rank field discovery disabled."""
    client = JiraClient(url=url, email=email, api_token=api_token)
    client._rank_field_checked = True  # Skip field discovery in tests
    return client


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


class TestJiraClientAuth:
    def test_basic_auth_with_email(self):
        client = JiraClient(url=JIRA_URL, email="user@test.com", api_token="tok")
        expected = base64.b64encode(b"user@test.com:tok").decode()
        assert client._auth_header == f"Basic {expected}"

    def test_bearer_auth_without_email(self):
        client = JiraClient(url=JIRA_URL, email="", api_token="pat-token")
        assert client._auth_header == "Bearer pat-token"

    def test_trailing_slash_stripped(self):
        client = JiraClient(url="https://jira.example.com/", email="", api_token="t")
        assert client._base_url == "https://jira.example.com"


# ---------------------------------------------------------------------------
# _request error handling
# ---------------------------------------------------------------------------


class TestJiraClientRequest:
    def test_http_error_raises_jira_api_error(self):
        client = _make_client()
        with patch("httpx.request", side_effect=httpx.ConnectError("fail")):
            with pytest.raises(JiraAPIError, match="HTTP request failed"):
                client._request("GET", "/myself")

    def test_4xx_raises_jira_api_error(self):
        client = _make_client()
        resp = _jira_response(status_code=404, json_data={"errorMessages": ["Not found"]})
        with patch("httpx.request", return_value=resp):
            with pytest.raises(JiraAPIError, match="HTTP 404"):
                client._request("GET", "/issue/NOPE-1")

    def test_429_retries_with_retry_after(self):
        client = _make_client()
        rate_limit_resp = _jira_response(
            status_code=429,
            json_data={},
            headers={"Retry-After": "1"},
        )
        ok_resp = _jira_response(status_code=200, json_data={"ok": True})

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return rate_limit_resp
            return ok_resp

        with patch("httpx.request", side_effect=side_effect):
            with patch("time.sleep") as mock_sleep:
                result = client._request("GET", "/myself")
                mock_sleep.assert_called_once_with(1)
                assert result.status_code == 200

    def test_429_raises_after_max_retries(self):
        client = _make_client()
        rate_limit_resp = _jira_response(
            status_code=429,
            json_data={},
            headers={"Retry-After": "1"},
        )
        with patch("httpx.request", return_value=rate_limit_resp):
            with patch("time.sleep"):
                with pytest.raises(JiraAPIError, match="Rate limited after"):
                    client._request("GET", "/myself")


# ---------------------------------------------------------------------------
# Rank field discovery
# ---------------------------------------------------------------------------


class TestRankFieldDiscovery:
    def test_discovers_rank_field(self):
        client = JiraClient(url=JIRA_URL, email="u@t.com", api_token="t")
        assert client._rank_field_checked is False
        fields_resp = _jira_response(json_data=[
            {"id": "summary", "name": "Summary"},
            {"id": "customfield_10019", "name": "Rank"},
            {"id": "priority", "name": "Priority"},
        ])
        with patch("httpx.request", return_value=fields_resp):
            client._ensure_rank_field()
        assert client._rank_field_id == "customfield_10019"
        assert client._rank_field_checked is True

    def test_no_rank_field_sets_flag(self):
        client = JiraClient(url=JIRA_URL, email="u@t.com", api_token="t")
        fields_resp = _jira_response(json_data=[
            {"id": "summary", "name": "Summary"},
        ])
        with patch("httpx.request", return_value=fields_resp):
            client._ensure_rank_field()
        assert client._rank_field_id is None
        assert client._rank_field_checked is True

    def test_api_error_falls_back_gracefully(self):
        client = JiraClient(url=JIRA_URL, email="u@t.com", api_token="t")
        with patch("httpx.request", side_effect=httpx.ConnectError("fail")):
            client._ensure_rank_field()
        assert client._rank_field_id is None
        assert client._rank_field_checked is True


# ---------------------------------------------------------------------------
# fetch_team_issues
# ---------------------------------------------------------------------------


class TestFetchTeamIssues:
    def test_basic_fetch(self):
        client = _make_client()
        search_resp = _jira_response(json_data={
            "issues": [
                {
                    "id": "10001",
                    "key": "PROJ-1",
                    "fields": {
                        "summary": "Fix bug",
                        "status": {"name": "Todo"},
                        "labels": ["urgent"],
                        "assignee": {
                            "accountId": "acc123",
                            "emailAddress": "dev@test.com",
                        },
                        "priority": {"id": "2", "name": "High"},
                        "issuelinks": [],
                        "subtasks": [],
                    },
                }
            ],
        })
        with patch("httpx.request", return_value=search_resp) as mock_req:
            issues = client.fetch_team_issues("PROJ", status_name="Todo")

        # Verify POST /search/jql with JSON body
        call_args = mock_req.call_args
        assert call_args[0][0] == "POST"
        assert call_args[0][1].endswith("/rest/api/2/search/jql")
        body = call_args[1]["json"]
        assert isinstance(body["fields"], list)
        assert "summary" in body["fields"]
        assert "startAt" not in body  # New endpoint uses nextPageToken
        assert isinstance(body["maxResults"], int)

        assert len(issues) == 1
        issue = issues[0]
        assert issue.id == "10001"
        assert issue.identifier == "PROJ-1"
        assert issue.title == "Fix bug"
        assert issue.priority == 2
        assert issue.assignee_id == "acc123"
        assert issue.assignee_email == "dev@test.com"
        assert issue.labels == ["urgent"]
        assert issue.url == f"{JIRA_URL}/browse/PROJ-1"
        assert issue.blocked_by is None

    def test_blocked_by_parsing(self):
        client = _make_client()
        search_resp = _jira_response(json_data={
            "issues": [
                {
                    "id": "10002",
                    "key": "PROJ-2",
                    "fields": {
                        "summary": "Blocked task",
                        "status": {"name": "Todo"},
                        "labels": [],
                        "assignee": None,
                        "priority": {"id": "3"},
                        "issuelinks": [
                            {
                                "type": {"name": "Blocks", "inward": "is blocked by"},
                                "inwardIssue": {
                                    "key": "PROJ-1",
                                    "fields": {
                                        "status": {
                                            "statusCategory": {"key": "indeterminate"}
                                        }
                                    },
                                },
                            }
                        ],
                        "subtasks": [],
                    },
                }
            ],
        })
        with patch("httpx.request", return_value=search_resp):
            issues = client.fetch_team_issues("PROJ")

        assert issues[0].blocked_by == ["PROJ-1"]

    def test_done_blocker_excluded(self):
        client = _make_client()
        search_resp = _jira_response(json_data={
            "issues": [
                {
                    "id": "10003",
                    "key": "PROJ-3",
                    "fields": {
                        "summary": "Task with done blocker",
                        "status": {"name": "Todo"},
                        "labels": [],
                        "assignee": None,
                        "priority": {"id": "3"},
                        "issuelinks": [
                            {
                                "type": {"name": "Blocks"},
                                "inwardIssue": {
                                    "key": "PROJ-1",
                                    "fields": {
                                        "status": {
                                            "statusCategory": {"key": "done"}
                                        }
                                    },
                                },
                            }
                        ],
                        "subtasks": [],
                    },
                }
            ],
        })
        with patch("httpx.request", return_value=search_resp):
            issues = client.fetch_team_issues("PROJ")

        assert issues[0].blocked_by is None

    def test_subtasks_parsed_as_children_states(self):
        client = _make_client()
        search_resp = _jira_response(json_data={
            "issues": [
                {
                    "id": "10004",
                    "key": "PROJ-4",
                    "fields": {
                        "summary": "Parent task",
                        "status": {"name": "Todo"},
                        "labels": [],
                        "assignee": None,
                        "priority": {"id": "3"},
                        "issuelinks": [],
                        "subtasks": [
                            {
                                "key": "PROJ-5",
                                "fields": {
                                    "status": {
                                        "name": "Done",
                                        "statusCategory": {"key": "done"},
                                    }
                                },
                            },
                            {
                                "key": "PROJ-6",
                                "fields": {
                                    "status": {
                                        "name": "In Progress",
                                        "statusCategory": {"key": "indeterminate"},
                                    }
                                },
                            },
                        ],
                    },
                }
            ],
        })
        with patch("httpx.request", return_value=search_resp):
            issues = client.fetch_team_issues("PROJ")

        assert issues[0].children_states == [
            ("PROJ-5", "completed"),
            ("PROJ-6", "started"),
        ]

    def test_subtask_canceled_status_detected(self):
        client = _make_client()
        search_resp = _jira_response(json_data={
            "issues": [
                {
                    "id": "10004",
                    "key": "PROJ-4",
                    "fields": {
                        "summary": "Parent task",
                        "status": {"name": "Todo"},
                        "labels": [],
                        "assignee": None,
                        "priority": {"id": "3"},
                        "issuelinks": [],
                        "subtasks": [
                            {
                                "key": "PROJ-5",
                                "fields": {
                                    "status": {
                                        "name": "Canceled",
                                        "statusCategory": {"key": "done"},
                                    }
                                },
                            },
                        ],
                    },
                }
            ],
        })
        with patch("httpx.request", return_value=search_resp):
            issues = client.fetch_team_issues("PROJ")

        assert issues[0].children_states == [("PROJ-5", "canceled")]

    def test_pagination(self):
        client = _make_client()
        page1 = _jira_response(json_data={
            "issues": [
                {
                    "id": "10001",
                    "key": "PROJ-1",
                    "fields": {
                        "summary": "First",
                        "status": {"name": "Todo"},
                        "labels": [],
                        "assignee": None,
                        "priority": {"id": "3"},
                        "issuelinks": [],
                        "subtasks": [],
                    },
                }
            ],
            "nextPageToken": "token-page-2",
        })
        page2 = _jira_response(json_data={
            "issues": [
                {
                    "id": "10002",
                    "key": "PROJ-2",
                    "fields": {
                        "summary": "Second",
                        "status": {"name": "Todo"},
                        "labels": [],
                        "assignee": None,
                        "priority": {"id": "3"},
                        "issuelinks": [],
                        "subtasks": [],
                    },
                }
            ],
        })
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return page1
            return page2

        with patch("httpx.request", side_effect=side_effect) as mock_req:
            issues = client.fetch_team_issues("PROJ", first=1)

        assert len(issues) == 2
        assert issues[0].identifier == "PROJ-1"
        assert issues[1].identifier == "PROJ-2"

        # First request should not include nextPageToken
        first_body = mock_req.call_args_list[0][1]["json"]
        assert "nextPageToken" not in first_body

        # Second request should include nextPageToken from first response
        second_body = mock_req.call_args_list[1][1]["json"]
        assert second_body["nextPageToken"] == "token-page-2"


# ---------------------------------------------------------------------------
# update_issue_state (transition-based)
# ---------------------------------------------------------------------------


class TestUpdateIssueState:
    def test_successful_transition(self):
        client = _make_client()
        transitions_resp = _jira_response(json_data={
            "transitions": [
                {"id": "21", "name": "Start", "to": {"name": "In Progress", "statusCategory": {"key": "indeterminate"}}, "fields": {}},
                {"id": "31", "name": "Done", "to": {"name": "Done", "statusCategory": {"key": "done"}}, "fields": {}},
            ]
        })
        post_resp = _jira_response(status_code=204, json_data={})

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return transitions_resp
            return post_resp

        with patch("httpx.request", side_effect=side_effect) as mock_req:
            client.update_issue_state("10001", "In Progress")

        # Second call should be the POST transition
        second_call = mock_req.call_args_list[1]
        assert second_call[0][0] == "POST"
        body = second_call[1].get("json") or second_call[0][1] if len(second_call[0]) > 1 else second_call[1].get("json")
        assert body["transition"]["id"] == "21"
        assert "fields" not in body  # Not a done transition

    def test_done_transition_includes_resolution_when_on_screen(self):
        client = _make_client()
        transitions_resp = _jira_response(json_data={
            "transitions": [
                {
                    "id": "31",
                    "name": "Done",
                    "to": {"name": "Done", "statusCategory": {"key": "done"}},
                    "fields": {
                        "resolution": {
                            "required": True,
                            "name": "Resolution",
                        },
                    },
                },
            ]
        })
        post_resp = _jira_response(status_code=204, json_data={})
        responses = [transitions_resp, post_resp]

        with patch("httpx.request", side_effect=responses) as mock_req:
            client.update_issue_state("10001", "Done")

        second_call = mock_req.call_args_list[1]
        body = second_call[1].get("json")
        assert body["transition"]["id"] == "31"
        assert body["fields"]["resolution"]["name"] == "Done"

    def test_done_transition_omits_resolution_when_not_on_screen(self):
        """When the resolution field is not on the transition screen, Jira
        auto-sets it.  Sending it explicitly causes HTTP 400."""
        client = _make_client()
        transitions_resp = _jira_response(json_data={
            "transitions": [
                {
                    "id": "31",
                    "name": "Done",
                    "to": {"name": "Done", "statusCategory": {"key": "done"}},
                    "fields": {},
                },
            ]
        })
        post_resp = _jira_response(status_code=204, json_data={})
        responses = [transitions_resp, post_resp]

        with patch("httpx.request", side_effect=responses) as mock_req:
            client.update_issue_state("10001", "Done")

        second_call = mock_req.call_args_list[1]
        body = second_call[1].get("json")
        assert body["transition"]["id"] == "31"
        assert "fields" not in body

    def test_missing_transition_raises_error(self):
        client = _make_client()
        transitions_resp = _jira_response(json_data={
            "transitions": [
                {"id": "21", "name": "Start", "to": {"name": "In Progress", "statusCategory": {"key": "indeterminate"}}, "fields": {}},
            ]
        })
        with patch("httpx.request", return_value=transitions_resp):
            with pytest.raises(JiraAPIError, match="No transition to 'Done'"):
                client.update_issue_state("10001", "Done")

    def test_case_insensitive_transition_match(self):
        client = _make_client()
        transitions_resp = _jira_response(json_data={
            "transitions": [
                {"id": "31", "name": "Finish", "to": {"name": "DONE", "statusCategory": {"key": "done"}}, "fields": {}},
            ]
        })
        post_resp = _jira_response(status_code=204, json_data={})

        with patch("httpx.request", side_effect=[transitions_resp, post_resp]):
            client.update_issue_state("10001", "done")


# ---------------------------------------------------------------------------
# get_team_states
# ---------------------------------------------------------------------------


class TestGetTeamStates:
    def test_returns_flattened_states(self):
        client = _make_client()
        resp = _jira_response(json_data=[
            {
                "name": "Task",
                "statuses": [
                    {"id": "1", "name": "Todo"},
                    {"id": "2", "name": "In Progress"},
                    {"id": "3", "name": "Done"},
                ],
            },
            {
                "name": "Bug",
                "statuses": [
                    {"id": "1", "name": "Todo"},
                    {"id": "4", "name": "Verified"},
                ],
            },
        ])
        with patch("httpx.request", return_value=resp):
            states = client.get_team_states("PROJ")

        # Should deduplicate "Todo"
        assert states == {
            "Todo": "Todo",
            "In Progress": "In Progress",
            "Done": "Done",
            "Verified": "Verified",
        }


# ---------------------------------------------------------------------------
# Simple methods
# ---------------------------------------------------------------------------


class TestSimpleMethods:
    def test_add_comment(self):
        client = _make_client()
        resp = _jira_response(status_code=201, json_data={"id": "1"})
        with patch("httpx.request", return_value=resp) as mock_req:
            client.add_comment("10001", "Hello")
        _, kwargs = mock_req.call_args
        assert kwargs["json"] == {"body": "Hello"}

    def test_get_viewer_id_cloud(self):
        client = _make_client()
        resp = _jira_response(json_data={"accountId": "abc123", "displayName": "User"})
        with patch("httpx.request", return_value=resp):
            assert client.get_viewer_id() == "abc123"

    def test_get_viewer_id_server(self):
        client = _make_client()
        resp = _jira_response(json_data={"name": "jsmith", "displayName": "User"})
        with patch("httpx.request", return_value=resp):
            assert client.get_viewer_id() == "jsmith"

    def test_get_viewer_id_missing_raises(self):
        client = _make_client()
        resp = _jira_response(json_data={"displayName": "User"})
        with patch("httpx.request", return_value=resp):
            with pytest.raises(JiraAPIError, match="Failed to retrieve viewer ID"):
                client.get_viewer_id()

    def test_assign_issue_cloud(self):
        client = _make_client(email="user@example.com")
        resp = _jira_response(status_code=204, json_data={})
        with patch("httpx.request", return_value=resp) as mock_req:
            client.assign_issue("10001", "acc123")
        _, kwargs = mock_req.call_args
        assert kwargs["json"]["fields"]["assignee"]["accountId"] == "acc123"

    def test_assign_issue_server_dc(self):
        client = _make_client(email="")
        resp = _jira_response(status_code=204, json_data={})
        with patch("httpx.request", return_value=resp) as mock_req:
            client.assign_issue("10001", "jsmith")
        _, kwargs = mock_req.call_args
        assert kwargs["json"]["fields"]["assignee"]["name"] == "jsmith"

    def test_add_labels(self):
        client = _make_client()
        resp = _jira_response(status_code=204, json_data={})
        with patch("httpx.request", return_value=resp) as mock_req:
            client.add_labels("10001", ["bug", "urgent"])
        _, kwargs = mock_req.call_args
        assert kwargs["json"]["update"]["labels"] == [
            {"add": "bug"},
            {"add": "urgent"},
        ]

    def test_get_team_id(self):
        client = _make_client()
        resp = _jira_response(json_data={"id": "12345", "key": "PROJ"})
        with patch("httpx.request", return_value=resp):
            assert client.get_team_id("PROJ") == "12345"

    def test_get_team_id_not_found(self):
        client = _make_client()
        resp = _jira_response(json_data={})
        with patch("httpx.request", return_value=resp):
            with pytest.raises(JiraAPIError, match="not found"):
                client.get_team_id("NOPE")


# ---------------------------------------------------------------------------
# Labels (Jira labels are plain strings)
# ---------------------------------------------------------------------------


class TestLabels:
    def test_get_label_id_returns_name(self):
        client = _make_client()
        assert client.get_label_id("PROJ", "bug") == "bug"

    def test_get_or_create_label_returns_name(self):
        client = _make_client()
        assert client.get_or_create_label("PROJ", "new-label") == "new-label"


# ---------------------------------------------------------------------------
# fetch_issue_labels
# ---------------------------------------------------------------------------


class TestFetchIssueLabels:
    def test_returns_title_and_labels(self):
        client = _make_client()
        resp = _jira_response(json_data={
            "fields": {
                "summary": "Bug title",
                "labels": ["bug", "p1"],
            }
        })
        with patch("httpx.request", return_value=resp):
            result = client.fetch_issue_labels("PROJ-1")
        assert result == ("Bug title", ["bug", "p1"])

    def test_returns_none_on_error(self):
        client = _make_client()
        resp = _jira_response(status_code=404, json_data={})
        with patch("httpx.request", return_value=resp):
            assert client.fetch_issue_labels("NOPE-1") is None


# ---------------------------------------------------------------------------
# fetch_issue_state_type
# ---------------------------------------------------------------------------


class TestFetchIssueStateType:
    def test_done_category(self):
        client = _make_client()
        resp = _jira_response(json_data={
            "fields": {
                "status": {
                    "name": "Done",
                    "statusCategory": {"key": "done"},
                }
            }
        })
        with patch("httpx.request", return_value=resp):
            assert client.fetch_issue_state_type("PROJ-1") == "completed"

    def test_canceled_status_name(self):
        client = _make_client()
        resp = _jira_response(json_data={
            "fields": {
                "status": {
                    "name": "Canceled",
                    "statusCategory": {"key": "done"},
                }
            }
        })
        with patch("httpx.request", return_value=resp):
            assert client.fetch_issue_state_type("PROJ-1") == "canceled"

    def test_cancelled_british_spelling(self):
        client = _make_client()
        resp = _jira_response(json_data={
            "fields": {
                "status": {
                    "name": "Cancelled",
                    "statusCategory": {"key": "done"},
                }
            }
        })
        with patch("httpx.request", return_value=resp):
            assert client.fetch_issue_state_type("PROJ-1") == "canceled"

    def test_new_category(self):
        client = _make_client()
        resp = _jira_response(json_data={
            "fields": {
                "status": {
                    "name": "Open",
                    "statusCategory": {"key": "new"},
                }
            }
        })
        with patch("httpx.request", return_value=resp):
            assert client.fetch_issue_state_type("PROJ-1") == "unstarted"

    def test_indeterminate_category(self):
        client = _make_client()
        resp = _jira_response(json_data={
            "fields": {
                "status": {
                    "name": "In Progress",
                    "statusCategory": {"key": "indeterminate"},
                }
            }
        })
        with patch("httpx.request", return_value=resp):
            assert client.fetch_issue_state_type("PROJ-1") == "started"

    def test_returns_none_on_error(self):
        client = _make_client()
        resp = _jira_response(status_code=404, json_data={})
        with patch("httpx.request", return_value=resp):
            assert client.fetch_issue_state_type("NOPE-1") is None


# ---------------------------------------------------------------------------
# fetch_issue_details
# ---------------------------------------------------------------------------


class TestFetchIssueDetails:
    def test_full_details(self):
        client = _make_client()
        resp = _jira_response(json_data={
            "id": "10001",
            "key": "PROJ-1",
            "fields": {
                "summary": "Important bug",
                "description": "Fix this ASAP",
                "status": {"name": "In Progress"},
                "priority": {"id": "2", "name": "High"},
                "assignee": {
                    "displayName": "Dev User",
                    "emailAddress": "dev@test.com",
                },
                "creator": {"displayName": "PM User"},
                "project": {"name": "My Project"},
                "parent": {"key": "PROJ-0"},
                "labels": ["bug"],
                "duedate": "2026-04-01",
                "timeoriginalestimate": 3600,
                "created": "2026-03-01T10:00:00.000+0000",
                "updated": "2026-03-15T10:00:00.000+0000",
                "resolutiondate": None,
                "issuelinks": [
                    {
                        "type": {"name": "Blocks"},
                        "inwardIssue": {"key": "PROJ-10"},
                    },
                    {
                        "type": {"name": "Blocks"},
                        "outwardIssue": {"key": "PROJ-20"},
                    },
                ],
                "subtasks": [
                    {"key": "PROJ-1A"},
                    {"key": "PROJ-1B"},
                ],
                "comment": {
                    "comments": [
                        {
                            "body": "Working on it",
                            "author": {"displayName": "Dev User"},
                            "created": "2026-03-10T10:00:00.000+0000",
                        }
                    ]
                },
            },
        })

        with patch("httpx.request", return_value=resp):
            details = client.fetch_issue_details("PROJ-1")

        assert details.id == "10001"
        assert details.ticket_id == "PROJ-1"
        assert details.title == "Important bug"
        assert details.description == "Fix this ASAP"
        assert details.status == "In Progress"
        assert details.priority == 2
        assert details.url == f"{JIRA_URL}/browse/PROJ-1"
        assert details.assignee_name == "Dev User"
        assert details.assignee_email == "dev@test.com"
        assert details.creator_name == "PM User"
        assert details.project_name == "My Project"
        assert details.parent_id == "PROJ-0"
        assert details.children_ids == ["PROJ-1A", "PROJ-1B"]
        assert details.blocked_by == ["PROJ-10"]
        assert details.blocks == ["PROJ-20"]
        assert details.labels == ["bug"]
        assert details.due_date == "2026-04-01"
        assert details.estimate == 3600
        assert len(details.comments) == 1
        assert details.comments[0].body == "Working on it"
        assert details.comments[0].author == "Dev User"

    def test_not_found_raises(self):
        client = _make_client()
        resp = _jira_response(status_code=404, json_data={"errorMessages": ["not found"]})
        with patch("httpx.request", return_value=resp):
            with pytest.raises(JiraAPIError):
                client.fetch_issue_details("NOPE-1")


# ---------------------------------------------------------------------------
# create_issue
# ---------------------------------------------------------------------------


class TestCreateIssue:
    def test_basic_creation(self):
        client = _make_client()
        resp = _jira_response(
            status_code=201,
            json_data={"id": "10099", "key": "PROJ-99"},
        )
        with patch("httpx.request", return_value=resp) as mock_req:
            result = client.create_issue(
                team_id="12345",
                title="New task",
                description="Do something",
                priority=2,
                label_ids=["bug"],
            )

        assert result.id == "10099"
        assert result.identifier == "PROJ-99"
        assert result.url == f"{JIRA_URL}/browse/PROJ-99"

        _, kwargs = mock_req.call_args
        fields = kwargs["json"]["fields"]
        assert fields["project"]["id"] == "12345"
        assert fields["summary"] == "New task"
        assert fields["description"] == "Do something"
        assert fields["priority"]["id"] == "2"
        assert fields["labels"] == ["bug"]
        assert fields["issuetype"]["name"] == "Task"

    def test_minimal_creation(self):
        client = _make_client()
        resp = _jira_response(
            status_code=201,
            json_data={"id": "10100", "key": "PROJ-100"},
        )
        with patch("httpx.request", return_value=resp) as mock_req:
            result = client.create_issue(team_id="12345", title="Simple")

        _, kwargs = mock_req.call_args
        fields = kwargs["json"]["fields"]
        assert "description" not in fields
        assert "priority" not in fields
        assert "labels" not in fields


# ---------------------------------------------------------------------------
# _map_status_category
# ---------------------------------------------------------------------------


class TestMapStatusCategory:
    def test_done_completed(self):
        assert JiraClient._map_status_category("done") == "completed"

    def test_done_canceled(self):
        assert JiraClient._map_status_category("done", "Canceled") == "canceled"

    def test_done_cancelled_british(self):
        assert JiraClient._map_status_category("done", "Cancelled") == "canceled"

    def test_new_unstarted(self):
        assert JiraClient._map_status_category("new") == "unstarted"

    def test_indeterminate_started(self):
        assert JiraClient._map_status_category("indeterminate") == "started"

    def test_unknown_passthrough(self):
        assert JiraClient._map_status_category("custom") == "custom"
