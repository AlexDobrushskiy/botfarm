"""Linear API client for polling tickets and updating issue status."""

from __future__ import annotations

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
