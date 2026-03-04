"""Linear API client for polling tickets and updating issue status."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import httpx

from botfarm.config import BotfarmConfig, ProjectConfig

logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"

_ISSUE_FIELDS = """
      id
      identifier
      title
      priority
      sortOrder
      url
      assignee {
        id
        email
      }
      labels {
        nodes {
          name
        }
      }
      relations {
        nodes {
          type
          relatedIssue {
            identifier
            state { type }
          }
        }
      }
      inverseRelations {
        nodes {
          type
          issue {
            identifier
            state { type }
          }
        }
      }
      children {
        nodes {
          identifier
          state { type name }
        }
      }
"""

ISSUES_QUERY = """
query TeamTodoIssues($teamKey: String!, $statusName: String!, $first: Int!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
      state: { name: { eq: $statusName } }
    }
    first: $first
    orderBy: createdAt
  ) {
    nodes {
""" + _ISSUE_FIELDS + """
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

ISSUES_WITH_PROJECT_QUERY = """
query TeamProjectTodoIssues($teamKey: String!, $statusName: String!, $projectName: String!, $first: Int!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
      state: { name: { eq: $statusName } }
      project: { name: { eq: $projectName } }
    }
    first: $first
    orderBy: createdAt
  ) {
    nodes {
""" + _ISSUE_FIELDS + """
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

UPDATE_STATE_MUTATION = """
mutation UpdateIssueState($issueId: String!, $stateId: String!) {
  issueUpdate(id: $issueId, input: { stateId: $stateId }) {
    success
    issue {
      id
      identifier
      state {
        name
      }
    }
  }
}
"""

TEAM_STATES_QUERY = """
query TeamStates($teamKey: String!) {
  teams(filter: { key: { eq: $teamKey } }) {
    nodes {
      id
      key
      states {
        nodes {
          id
          name
          type
        }
      }
    }
  }
}
"""

ADD_COMMENT_MUTATION = """
mutation AddComment($issueId: String!, $body: String!) {
  commentCreate(input: { issueId: $issueId, body: $body }) {
    success
  }
}
"""

VIEWER_QUERY = """
query Viewer {
  viewer {
    id
    name
  }
}
"""

ASSIGN_ISSUE_MUTATION = """
mutation AssignIssue($issueId: String!, $assigneeId: String!) {
  issueUpdate(id: $issueId, input: { assigneeId: $assigneeId }) {
    success
    issue {
      id
      identifier
      assignee {
        id
        name
      }
    }
  }
}
"""

ISSUE_STATE_QUERY = """
query IssueState($identifier: String!) {
  issue(id: $identifier) {
    id
    identifier
    state {
      name
      type
    }
  }
}
"""

ACTIVE_ISSUES_COUNT_QUERY = """
query ActiveIssuesCount($first: Int!, $after: String) {
  issues(
    first: $first
    after: $after
    includeArchived: false
  ) {
    nodes {
      id
      project {
        name
      }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

ISSUE_DETAILS_QUERY = """
query IssueDetails($identifier: String!) {
  issue(id: $identifier) {
    id
    identifier
    title
    description
    priority
    url
    estimate
    dueDate
    createdAt
    updatedAt
    completedAt
    state { name }
    creator { name }
    assignee { name, email }
    project { name }
    team { name, key }
    parent { identifier, title }
    children { nodes { identifier } }
    labels { nodes { name } }
    relations {
      nodes {
        type
        relatedIssue { identifier }
      }
    }
    inverseRelations {
      nodes {
        type
        issue { identifier }
      }
    }
    comments(first: 50) {
      nodes {
        body
        user { name }
        createdAt
      }
    }
  }
}
"""

_COMPLETED_ISSUE_FIELDS = """
      id
      identifier
      title
      updatedAt
      completedAt
      state { type name }
      project { name }
      labels { nodes { name } }
      children {
        nodes {
          id
          state { type }
        }
      }
"""

COMPLETED_ISSUES_QUERY = """
query CompletedCanceledIssues($teamKey: String!, $first: Int!, $stateTypes: [String!]!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
      state: { type: { in: $stateTypes } }
    }
    first: $first
    orderBy: updatedAt
  ) {
    nodes {
""" + _COMPLETED_ISSUE_FIELDS + """
    }
  }
}
"""

COMPLETED_ISSUES_WITH_PROJECT_QUERY = """
query CompletedCanceledIssuesForProject($teamKey: String!, $projectName: String!, $first: Int!, $stateTypes: [String!]!) {
  issues(
    filter: {
      team: { key: { eq: $teamKey } }
      project: { name: { eq: $projectName } }
      state: { type: { in: $stateTypes } }
    }
    first: $first
    orderBy: updatedAt
  ) {
    nodes {
""" + _COMPLETED_ISSUE_FIELDS + """
    }
  }
}
"""

ISSUE_ARCHIVE_MUTATION = """
mutation IssueArchive($id: String!) {
  issueArchive(id: $id) {
    success
  }
}
"""

ISSUE_DELETE_MUTATION = """
mutation IssueDelete($id: String!) {
  issueDelete(id: $id) {
    success
  }
}
"""

ISSUE_UNARCHIVE_MUTATION = """
mutation IssueUnarchive($id: String!) {
  issueUnarchive(id: $id) {
    success
  }
}
"""

TEAM_LABELS_QUERY = """
query TeamLabels($teamKey: String!) {
  issueLabels(
    filter: {
      or: [
        { team: { key: { eq: $teamKey } } }
        { team: { null: true } }
      ]
    }
    first: 250
  ) {
    nodes { id name }
  }
}
"""

CREATE_LABEL_MUTATION = """
mutation CreateLabel($input: IssueLabelCreateInput!) {
  issueLabelCreate(input: $input) {
    success
    issueLabel { id name }
  }
}
"""

CREATE_ISSUE_MUTATION = """
mutation CreateIssue($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue {
      id
      identifier
      url
    }
  }
}
"""

ISSUES_BY_LABEL_QUERY = """
query IssuesByLabel($teamKey: String!, $labelName: String!, $first: Int!) {
  issues(
    first: $first
    includeArchived: false
    filter: {
      team: { key: { eq: $teamKey } }
      labels: { name: { eq: $labelName } }
      state: { type: { nin: ["completed", "canceled"] } }
    }
  ) {
    nodes {
      id
      identifier
      title
      state { type name }
    }
  }
}
"""

PROJECT_BY_NAME_QUERY = """
query ProjectByName($name: String!) {
  projects(filter: { name: { eq: $name } }, first: 1) {
    nodes {
      id
      name
    }
  }
}
"""

ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY = """
query ActiveIssuesForProjectCount($first: Int!, $after: String, $projectName: String!) {
  issues(
    first: $first
    after: $after
    includeArchived: false
    filter: {
      project: { name: { eq: $projectName } }
    }
  ) {
    nodes {
      id
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""


@dataclass
class ActiveIssuesCount:
    """Result of counting all non-archived issues (matches Linear's free plan usage)."""

    total: int
    by_project: dict[str, int]


@dataclass
class LinearIssue:
    """A Linear issue returned from polling."""

    id: str
    identifier: str
    title: str
    priority: int
    url: str
    assignee_id: str | None = None
    assignee_email: str | None = None
    labels: list[str] | None = None
    sort_order: float = 0.0
    blocked_by: list[str] | None = None
    # Children info: list of (identifier, state_type) tuples, or None if no children.
    children_states: list[tuple[str, str]] | None = None


@dataclass
class PollResult:
    """Result of a LinearPoller.poll() call.

    Attributes:
        candidates: Issues eligible for dispatch, sorted by sort order.
        blocked: Issues blocked by unresolved issues, sorted by sort order.
        auto_close_parents: Parent issues whose children are all done,
            ready to be auto-closed by the supervisor.
    """

    candidates: list[LinearIssue]
    blocked: list[LinearIssue]
    auto_close_parents: list[LinearIssue]


class LinearAPIError(Exception):
    """Raised when a Linear API call fails."""


class LinearClient:
    """Low-level HTTP client for the Linear GraphQL API."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._rate_limit_remaining: int | None = None

    @property
    def rate_limit_remaining(self) -> int | None:
        """Most recent ``X-RateLimit-Requests-Remaining`` value, or None."""
        return self._rate_limit_remaining

    def _execute(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query/mutation and return the data dict.

        Raises LinearAPIError on transport or GraphQL-level errors.
        """
        payload: dict = {"query": query}
        if variables:
            payload["variables"] = variables

        try:
            response = httpx.post(
                LINEAR_API_URL,
                json=payload,
                headers={
                    "Authorization": self._api_key,
                    "Content-Type": "application/json",
                },
                timeout=30.0,
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise LinearAPIError(
                f"HTTP {exc.response.status_code}: {exc.response.text[:200]}"
            ) from exc
        except httpx.HTTPError as exc:
            raise LinearAPIError(f"HTTP request failed: {exc}") from exc

        # Track rate limit headers
        rl_remaining = response.headers.get("X-RateLimit-Requests-Remaining")
        if rl_remaining is not None:
            try:
                self._rate_limit_remaining = int(rl_remaining)
            except ValueError:
                pass

        try:
            body = response.json()
        except ValueError as exc:
            raise LinearAPIError(
                f"Linear API returned invalid JSON (HTTP {response.status_code})"
            ) from exc
        if "errors" in body:
            messages = [e.get("message", str(e)) for e in body["errors"]]
            raise LinearAPIError(f"GraphQL errors: {'; '.join(messages)}")

        data = body.get("data")
        if not data:
            raise LinearAPIError(
                f"Linear API response missing 'data' key (HTTP {response.status_code})"
            )
        return data

    def fetch_team_issues(
        self,
        team_key: str,
        status_name: str = "Todo",
        first: int = 50,
        project_name: str = "",
    ) -> list[LinearIssue]:
        """Fetch issues for a team in a given workflow state.

        When *project_name* is non-empty, only issues belonging to that
        Linear project are returned.
        """
        if project_name:
            query = ISSUES_WITH_PROJECT_QUERY
            variables: dict = {
                "teamKey": team_key,
                "statusName": status_name,
                "projectName": project_name,
                "first": first,
            }
        else:
            query = ISSUES_QUERY
            variables = {
                "teamKey": team_key,
                "statusName": status_name,
                "first": first,
            }
        data = self._execute(query, variables)
        nodes = data.get("issues", {}).get("nodes", [])
        issues = []
        for node in nodes:
            assignee = node.get("assignee") or {}
            label_nodes = node.get("labels", {}).get("nodes", [])
            # Parse blocking relations: find issues that block this one.
            # In Linear's relations model, if issue A has a relation with
            # type="blocks" pointing to issue B, then B is blocked by A.
            # Conversely, relations on this issue with type="blocks" mean
            # this issue blocks the relatedIssue — we don't need those.
            # We need relations where type="isBlockedBy" on this issue.
            blocked_by = []
            for rel in node.get("relations", {}).get("nodes", []):
                if rel.get("type") != "isBlockedBy":
                    continue
                related = rel.get("relatedIssue") or {}
                state_type = (related.get("state") or {}).get("type", "")
                # Only count as blocked if the blocker is not resolved
                if state_type not in ("completed", "canceled"):
                    blocked_by.append(related.get("identifier", ""))
            # Also check inverseRelations: if another issue has type="blocks"
            # pointing at us, we are blocked by that issue.
            for rel in node.get("inverseRelations", {}).get("nodes", []):
                if rel.get("type") != "blocks":
                    continue
                related = rel.get("issue") or {}
                state_type = (related.get("state") or {}).get("type", "")
                if state_type not in ("completed", "canceled"):
                    blocked_by.append(related.get("identifier", ""))
            # Parse children to detect parent issues.
            child_nodes = node.get("children", {}).get("nodes", [])
            children_states: list[tuple[str, str]] | None = None
            if child_nodes:
                children_states = [
                    (
                        ch.get("identifier", ""),
                        (ch.get("state") or {}).get("type", ""),
                    )
                    for ch in child_nodes
                ]
            issues.append(
                LinearIssue(
                    id=node["id"],
                    identifier=node["identifier"],
                    title=node["title"],
                    priority=node.get("priority", 4),
                    url=node.get("url", ""),
                    assignee_id=assignee.get("id"),
                    assignee_email=assignee.get("email"),
                    labels=[ln["name"] for ln in label_nodes if "name" in ln],
                    sort_order=node.get("sortOrder", 0.0),
                    blocked_by=blocked_by if blocked_by else None,
                    children_states=children_states,
                )
            )
        return issues

    def get_team_states(self, team_key: str) -> dict[str, str]:
        """Return a mapping of state name -> state id for a team."""
        data = self._execute(TEAM_STATES_QUERY, {"teamKey": team_key})
        teams = data.get("teams", {}).get("nodes", [])
        if not teams:
            raise LinearAPIError(f"Team with key '{team_key}' not found")
        states = teams[0].get("states", {}).get("nodes", [])
        return {s["name"]: s["id"] for s in states}

    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        """Move an issue to a new workflow state."""
        data = self._execute(
            UPDATE_STATE_MUTATION,
            {"issueId": issue_id, "stateId": state_id},
        )
        result = data.get("issueUpdate", {})
        if not result.get("success"):
            raise LinearAPIError(
                f"Failed to update issue {issue_id} to state {state_id}"
            )

    def add_comment(self, issue_id: str, body: str) -> None:
        """Add a comment to an issue."""
        data = self._execute(
            ADD_COMMENT_MUTATION,
            {"issueId": issue_id, "body": body},
        )
        result = data.get("commentCreate", {})
        if not result.get("success"):
            raise LinearAPIError(f"Failed to add comment to issue {issue_id}")

    def get_viewer_id(self) -> str:
        """Return the Linear user ID of the authenticated API key owner."""
        data = self._execute(VIEWER_QUERY)
        viewer = data.get("viewer")
        if not viewer or not viewer.get("id"):
            raise LinearAPIError("Failed to retrieve viewer ID")
        return viewer["id"]

    def assign_issue(self, issue_id: str, assignee_id: str) -> None:
        """Assign an issue to a user by their Linear user ID."""
        data = self._execute(
            ASSIGN_ISSUE_MUTATION,
            {"issueId": issue_id, "assigneeId": assignee_id},
        )
        result = data.get("issueUpdate", {})
        if not result.get("success"):
            raise LinearAPIError(
                f"Failed to assign issue {issue_id} to user {assignee_id}"
            )

    def fetch_issue_state_type(self, identifier: str) -> str | None:
        """Fetch the workflow state type of a single issue by identifier.

        Returns the state type string (e.g. "completed", "canceled",
        "started", "unstarted") or ``None`` if the issue is not found.
        """
        try:
            data = self._execute(ISSUE_STATE_QUERY, {"identifier": identifier})
        except LinearAPIError:
            logger.warning("Failed to fetch state for issue %s", identifier)
            return None
        issue = data.get("issue")
        if not issue:
            return None
        state = issue.get("state") or {}
        return state.get("type")

    def count_active_issues(self) -> ActiveIssuesCount | None:
        """Count all non-archived issues with per-project breakdown.

        Linear's free plan counts ALL non-archived issues (including
        completed and canceled) toward the 250 issue limit, so this
        query omits state filters to match that behavior.

        Paginates through all non-archived issues requesting only
        ``id`` and ``project.name`` to minimise API complexity cost.

        Returns an ``ActiveIssuesCount`` with total count and a dict
        mapping project names to their issue counts, or ``None`` on failure.
        """
        by_project: dict[str, int] = {}
        total = 0
        cursor: str | None = None

        try:
            while True:
                variables: dict = {"first": 250}
                if cursor is not None:
                    variables["after"] = cursor
                data = self._execute(ACTIVE_ISSUES_COUNT_QUERY, variables)
                issues_data = data.get("issues", {})
                nodes = issues_data.get("nodes", [])

                for node in nodes:
                    total += 1
                    project = node.get("project")
                    project_name = project.get("name") if project else None
                    if project_name:
                        by_project[project_name] = by_project.get(project_name, 0) + 1
                    else:
                        by_project["(no project)"] = by_project.get("(no project)", 0) + 1

                page_info = issues_data.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                else:
                    break
        except LinearAPIError:
            logger.warning("Failed to count active issues")
            return None

        return ActiveIssuesCount(total=total, by_project=by_project)

    def fetch_issue_details(self, identifier: str) -> dict:
        """Fetch full details for a single issue by identifier.

        Returns a dict with all structured fields ready for
        ``upsert_ticket_history()``.
        """
        data = self._execute(ISSUE_DETAILS_QUERY, {"identifier": identifier})
        issue = data.get("issue")
        if not issue:
            raise LinearAPIError(f"Issue '{identifier}' not found")

        assignee = issue.get("assignee") or {}
        creator = issue.get("creator") or {}
        project = issue.get("project") or {}
        team = issue.get("team") or {}
        parent = issue.get("parent") or {}
        state = issue.get("state") or {}

        # Children identifiers
        child_nodes = issue.get("children", {}).get("nodes", [])
        children_ids = [ch.get("identifier", "") for ch in child_nodes]

        # Labels
        label_nodes = issue.get("labels", {}).get("nodes", [])
        labels = [ln.get("name", "") for ln in label_nodes]

        # Blocking relations
        blocked_by = []
        blocks = []
        for rel in issue.get("relations", {}).get("nodes", []):
            rel_type = rel.get("type", "")
            related_id = (rel.get("relatedIssue") or {}).get("identifier", "")
            if rel_type == "isBlockedBy":
                blocked_by.append(related_id)
            elif rel_type == "blocks":
                blocks.append(related_id)
        for rel in issue.get("inverseRelations", {}).get("nodes", []):
            rel_type = rel.get("type", "")
            related_id = (rel.get("issue") or {}).get("identifier", "")
            if rel_type == "blocks":
                blocked_by.append(related_id)
            elif rel_type == "isBlockedBy":
                blocks.append(related_id)

        # Comments
        comment_nodes = issue.get("comments", {}).get("nodes", [])
        comments = [
            {
                "body": c.get("body", ""),
                "author": (c.get("user") or {}).get("name", ""),
                "created_at": c.get("createdAt", ""),
            }
            for c in comment_nodes
        ]

        return {
            "ticket_id": issue.get("identifier", identifier),
            "linear_uuid": issue.get("id", ""),
            "title": issue.get("title", ""),
            "description": issue.get("description"),
            "status": state.get("name"),
            "priority": issue.get("priority"),
            "url": issue.get("url", ""),
            "assignee_name": assignee.get("name"),
            "assignee_email": assignee.get("email"),
            "creator_name": creator.get("name"),
            "project_name": project.get("name"),
            "team_name": team.get("name"),
            "estimate": issue.get("estimate"),
            "due_date": issue.get("dueDate"),
            "parent_id": parent.get("identifier"),
            "children_ids": json.dumps(children_ids),
            "blocked_by": json.dumps(blocked_by),
            "blocks": json.dumps(blocks),
            "labels": json.dumps(labels),
            "comments_json": json.dumps(comments),
            "linear_created_at": issue.get("createdAt"),
            "linear_updated_at": issue.get("updatedAt"),
            "linear_completed_at": issue.get("completedAt"),
            "raw_json": json.dumps(issue),
        }

    def fetch_completed_issues(
        self,
        team_key: str,
        first: int = 50,
        project_name: str = "",
        state_types: list[str] | None = None,
    ) -> list[dict]:
        """Fetch completed/canceled issues sorted by updatedAt ascending.

        Args:
            state_types: Linear state types to include, e.g.
                ``["completed"]`` or ``["canceled"]``.  Defaults to
                ``["completed", "canceled"]``.

        Returns raw dicts with id, identifier, title, updatedAt,
        completedAt, labels, and children info.
        """
        if state_types is None:
            state_types = ["completed", "canceled"]

        if project_name:
            query = COMPLETED_ISSUES_WITH_PROJECT_QUERY
            variables: dict = {
                "teamKey": team_key,
                "projectName": project_name,
                "first": first,
                "stateTypes": state_types,
            }
        else:
            query = COMPLETED_ISSUES_QUERY
            variables = {
                "teamKey": team_key,
                "first": first,
                "stateTypes": state_types,
            }

        data = self._execute(query, variables)
        return data.get("issues", {}).get("nodes", [])

    def archive_issue(self, issue_id: str) -> bool:
        """Archive an issue. Returns True on success."""
        data = self._execute(ISSUE_ARCHIVE_MUTATION, {"id": issue_id})
        return data.get("issueArchive", {}).get("success", False)

    def delete_issue(self, issue_id: str) -> bool:
        """Delete an issue. Returns True on success."""
        data = self._execute(ISSUE_DELETE_MUTATION, {"id": issue_id})
        return data.get("issueDelete", {}).get("success", False)

    def unarchive_issue(self, issue_id: str) -> bool:
        """Unarchive an issue. Returns True on success."""
        data = self._execute(ISSUE_UNARCHIVE_MUTATION, {"id": issue_id})
        return data.get("issueUnarchive", {}).get("success", False)

    def get_team_id(self, team_key: str) -> str:
        """Return the internal UUID for a team given its key.

        Reuses TEAM_STATES_QUERY (which also fetches workflow states) since
        there is no lighter team-by-key query defined. The extra state data
        is simply ignored.
        """
        data = self._execute(TEAM_STATES_QUERY, {"teamKey": team_key})
        teams = data.get("teams", {}).get("nodes", [])
        if not teams:
            raise LinearAPIError(f"Team with key '{team_key}' not found")
        return teams[0]["id"]

    def get_project_id(self, project_name: str) -> str | None:
        """Return the internal UUID for a project given its name, or None."""
        data = self._execute(PROJECT_BY_NAME_QUERY, {"name": project_name})
        nodes = data.get("projects", {}).get("nodes", [])
        if not nodes:
            return None
        return nodes[0]["id"]

    def get_label_id(self, team_key: str, label_name: str) -> str | None:
        """Look up a label by name (team-scoped or workspace-scoped). Returns id or None."""
        data = self._execute(TEAM_LABELS_QUERY, {"teamKey": team_key})
        for label in data.get("issueLabels", {}).get("nodes", []):
            if label["name"].lower() == label_name.lower():
                return label["id"]
        return None

    def get_or_create_label(self, team_key: str, label_name: str) -> str:
        """Get an existing label id or create one. Returns the label id."""
        label_id = self.get_label_id(team_key, label_name)
        if label_id:
            return label_id
        team_id = self.get_team_id(team_key)
        data = self._execute(
            CREATE_LABEL_MUTATION,
            {"input": {"name": label_name, "teamId": team_id}},
        )
        result = data.get("issueLabelCreate", {})
        if not result.get("success"):
            raise LinearAPIError(f"Failed to create label '{label_name}'")
        return result["issueLabel"]["id"]

    def create_issue(
        self,
        *,
        team_id: str,
        title: str,
        description: str = "",
        priority: int | None = None,
        label_ids: list[str] | None = None,
        project_id: str | None = None,
        state_id: str | None = None,
    ) -> dict:
        """Create a new issue. Returns dict with id, identifier, url."""
        input_data: dict = {
            "teamId": team_id,
            "title": title,
        }
        if description:
            input_data["description"] = description
        if priority is not None:
            input_data["priority"] = priority
        if label_ids:
            input_data["labelIds"] = label_ids
        if project_id:
            input_data["projectId"] = project_id
        if state_id:
            input_data["stateId"] = state_id
        data = self._execute(CREATE_ISSUE_MUTATION, {"input": input_data})
        result = data.get("issueCreate", {})
        if not result.get("success"):
            raise LinearAPIError("issueCreate returned success=false")
        return result.get("issue", {})

    def fetch_open_issues_with_label(
        self,
        team_key: str,
        label_name: str,
    ) -> list[dict]:
        """Fetch non-completed, non-canceled issues with a given label."""
        data = self._execute(
            ISSUES_BY_LABEL_QUERY,
            {"teamKey": team_key, "labelName": label_name, "first": 50},
        )
        return data.get("issues", {}).get("nodes", [])

    def count_active_issues_for_project(self, project_name: str) -> int | None:
        """Count all non-archived issues for a specific project.

        Uses a dedicated query filtered by project name to avoid
        fetching unrelated issues.  Returns the count, or ``None``
        on failure.
        """
        total = 0
        cursor: str | None = None

        try:
            while True:
                variables: dict = {"first": 250, "projectName": project_name}
                if cursor is not None:
                    variables["after"] = cursor
                data = self._execute(
                    ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY, variables
                )
                issues_data = data.get("issues", {})
                nodes = issues_data.get("nodes", [])
                total += len(nodes)

                page_info = issues_data.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                else:
                    break
        except LinearAPIError:
            logger.warning(
                "Failed to count active issues for project %s", project_name
            )
            return None

        return total


class LinearPoller:
    """Polls Linear for Todo tickets in a project's team and returns prioritized candidates.

    One LinearPoller is created per project. It filters out excluded labels
    and tickets already tracked in the database.
    """

    def __init__(
        self,
        client: LinearClient,
        project: ProjectConfig,
        exclude_tags: list[str],
        todo_status: str = "Todo",
        coder_client: LinearClient | None = None,
    ) -> None:
        self._client = client
        self._coder_client = coder_client or client
        self._project = project
        self._exclude_tags = set(tag.lower() for tag in exclude_tags)
        self._todo_status = todo_status
        # TODO: Add TTL-based invalidation if the supervisor becomes long-running
        self._state_cache: dict[str, str] | None = None

    @property
    def project_name(self) -> str:
        return self._project.name

    @property
    def team_key(self) -> str:
        return self._project.linear_team

    def poll(self, active_ticket_ids: set[str] | None = None) -> PollResult:
        """Fetch Todo issues, filter, and return sorted by manual sort order.

        Args:
            active_ticket_ids: Set of Linear issue IDs already being worked on
                across all slots. These will be excluded from the results.

        Returns:
            PollResult with candidates sorted by sortOrder ascending and
            any parent issues eligible for auto-close.
        """
        if active_ticket_ids is None:
            active_ticket_ids = set()

        issues = self._client.fetch_team_issues(
            team_key=self._project.linear_team,
            status_name=self._todo_status,
            project_name=self._project.linear_project,
        )

        pre_parent_candidates = []
        blocked_issues = []
        for issue in issues:
            issue_labels = set(
                label.lower() for label in (issue.labels or [])
            )
            if issue_labels & self._exclude_tags:
                logger.debug(
                    "Skipping %s: excluded label(s) %s",
                    issue.identifier,
                    issue_labels & self._exclude_tags,
                )
                continue

            if issue.identifier in active_ticket_ids:
                logger.debug(
                    "Skipping %s: already active",
                    issue.identifier,
                )
                continue

            if issue.blocked_by:
                logger.debug(
                    "Blocked %s: blocked by unresolved issue(s) %s",
                    issue.identifier,
                    issue.blocked_by,
                )
                blocked_issues.append(issue)
                continue

            pre_parent_candidates.append(issue)

        # Separate parent issues from regular candidates.
        candidates = []
        auto_close_parents = []
        for issue in pre_parent_candidates:
            if issue.children_states is None:
                # No children — normal issue
                candidates.append(issue)
                continue

            # Has children: check if all are done
            all_done = all(
                st in ("completed", "canceled")
                for _, st in issue.children_states
            )
            if all_done:
                auto_close_parents.append(issue)
                logger.info(
                    "Parent %s: all children done — eligible for auto-close",
                    issue.identifier,
                )
            else:
                open_children = [
                    ident for ident, st in issue.children_states
                    if st not in ("completed", "canceled")
                ]
                logger.debug(
                    "Skipping parent %s: children still open: %s",
                    issue.identifier,
                    open_children,
                )

        # Sort by sortOrder ascending — lower value = higher in the Linear UI list.
        # This respects the manual drag-and-drop ordering set by the user.
        candidates.sort(key=lambda i: i.sort_order)
        blocked_issues.sort(key=lambda i: i.sort_order)

        logger.debug(
            "Polled %s: %d Todo issues, %d candidates, %d blocked, %d auto-close parents",
            self._project.linear_team,
            len(issues),
            len(candidates),
            len(blocked_issues),
            len(auto_close_parents),
        )
        return PollResult(candidates=candidates, blocked=blocked_issues, auto_close_parents=auto_close_parents)

    def get_state_id(self, state_name: str) -> str:
        """Look up a workflow state ID by name, caching the result."""
        if self._state_cache is None:
            self._state_cache = self._client.get_team_states(
                self._project.linear_team
            )
        state_id = self._state_cache.get(state_name)
        if state_id is None:
            raise LinearAPIError(
                f"State '{state_name}' not found for team "
                f"'{self._project.linear_team}'"
            )
        return state_id

    def is_issue_terminal(self, identifier: str) -> bool:
        """Check whether an issue is in a terminal state (completed/canceled).

        Returns ``False`` on API errors so that callers don't skip
        recovery when the status is simply unknown.
        """
        state_type = self._client.fetch_issue_state_type(identifier)
        if state_type is None:
            return False
        return state_type in ("completed", "canceled")

    def move_issue(self, issue_id: str, state_name: str) -> None:
        """Move an issue to the named workflow state (uses coder client)."""
        state_id = self.get_state_id(state_name)
        self._coder_client.update_issue_state(issue_id, state_id)
        logger.info("Moved issue %s to '%s'", issue_id, state_name)

    def assign_issue(self, issue_id: str, assignee_id: str) -> None:
        """Assign an issue to a user by their Linear user ID."""
        self._client.assign_issue(issue_id, assignee_id)
        logger.info("Assigned issue %s to user %s", issue_id, assignee_id)

    def add_comment(self, issue_id: str, body: str) -> None:
        """Add a comment to an issue (uses coder client)."""
        self._coder_client.add_comment(issue_id, body)

    def add_comment_as_owner(self, issue_id: str, body: str) -> None:
        """Add a comment using the owner's client (for system-level notifications)."""
        self._client.add_comment(issue_id, body)


def create_pollers(config: BotfarmConfig) -> list[LinearPoller]:
    """Create one LinearPoller per configured project.

    When ``identities.coder.linear_api_key`` is configured, a separate
    ``LinearClient`` is created for coder-initiated operations (moving
    tickets, posting comments) so they appear under the coder bot's identity.
    Polling always uses the owner's client.
    """
    client = LinearClient(api_key=config.linear.api_key)
    coder_key = config.identities.coder.linear_api_key
    coder_client = LinearClient(api_key=coder_key) if coder_key else None
    return [
        LinearPoller(
            client=client,
            project=project,
            exclude_tags=config.linear.exclude_tags,
            todo_status=config.linear.todo_status,
            coder_client=coder_client,
        )
        for project in config.projects
    ]
