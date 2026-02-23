"""Linear API client for polling tickets and updating issue status."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import httpx

from botfarm.config import BotfarmConfig, ProjectConfig

logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"

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
      id
      identifier
      title
      priority
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
      id
      identifier
      title
      priority
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


class LinearAPIError(Exception):
    """Raised when a Linear API call fails."""


class LinearClient:
    """Low-level HTTP client for the Linear GraphQL API."""

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

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
    ) -> None:
        self._client = client
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

    def poll(self, active_ticket_ids: set[str] | None = None) -> list[LinearIssue]:
        """Fetch Todo issues, filter, and return sorted by priority (highest first).

        Args:
            active_ticket_ids: Set of Linear issue IDs already being worked on
                across all slots. These will be excluded from the results.

        Returns:
            List of LinearIssue sorted by priority (lower number = higher priority).
        """
        if active_ticket_ids is None:
            active_ticket_ids = set()

        issues = self._client.fetch_team_issues(
            team_key=self._project.linear_team,
            status_name=self._todo_status,
            project_name=self._project.linear_project,
        )

        candidates = []
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

            candidates.append(issue)

        # Sort by priority: Linear uses 0=No priority, 1=Urgent, 2=High, 3=Medium, 4=Low.
        # 0 (no priority) should sort last.
        candidates.sort(key=lambda i: i.priority if i.priority > 0 else 5)

        logger.info(
            "Polled %s: %d Todo issues, %d candidates after filtering",
            self._project.linear_team,
            len(issues),
            len(candidates),
        )
        return candidates

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

    def move_issue(self, issue_id: str, state_name: str) -> None:
        """Move an issue to the named workflow state."""
        state_id = self.get_state_id(state_name)
        self._client.update_issue_state(issue_id, state_id)
        logger.info("Moved issue %s to '%s'", issue_id, state_name)

    def add_comment(self, issue_id: str, body: str) -> None:
        """Add a comment to an issue."""
        self._client.add_comment(issue_id, body)


def create_pollers(config: BotfarmConfig) -> list[LinearPoller]:
    """Create one LinearPoller per configured project."""
    client = LinearClient(api_key=config.linear.api_key)
    return [
        LinearPoller(
            client=client,
            project=project,
            exclude_tags=config.linear.exclude_tags,
            todo_status=config.linear.todo_status,
        )
        for project in config.projects
    ]
