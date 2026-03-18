"""Linear GraphQL API client implementing the abstract BugtrackerClient."""

from __future__ import annotations

import json
import logging

import httpx

from botfarm.bugtracker.base import BugtrackerClient
from botfarm.bugtracker.errors import BugtrackerError
from botfarm.bugtracker.types import ActiveIssuesCount, CreatedIssue, Issue, IssueDetails, Comment

from .queries import (
    ACTIVE_ISSUES_COUNT_QUERY,
    ACTIVE_ISSUES_FOR_PROJECT_COUNT_QUERY,
    ADD_COMMENT_MUTATION,
    ADD_LABELS_MUTATION,
    ASSIGN_ISSUE_MUTATION,
    COMPLETED_ISSUES_QUERY,
    COMPLETED_ISSUES_WITH_PROJECT_QUERY,
    CREATE_ISSUE_MUTATION,
    CREATE_LABEL_MUTATION,
    CREATE_PROJECT_MUTATION,
    ISSUE_ARCHIVE_MUTATION,
    ISSUE_DELETE_MUTATION,
    ISSUE_DETAILS_QUERY,
    ISSUE_LABELS_BY_IDENTIFIER_QUERY,
    ISSUE_LABELS_QUERY,
    ISSUE_STATE_QUERY,
    ISSUE_UNARCHIVE_MUTATION,
    ISSUES_BY_LABEL_QUERY,
    ISSUES_QUERY,
    ISSUES_WITH_PROJECT_QUERY,
    LIST_TEAM_PROJECTS_QUERY,
    LIST_TEAMS_QUERY,
    ORGANIZATION_QUERY,
    PROJECT_BY_NAME_QUERY,
    TEAM_LABELS_QUERY,
    TEAM_STATES_QUERY,
    UPDATE_STATE_MUTATION,
    VIEWER_QUERY,
)

logger = logging.getLogger(__name__)

LINEAR_API_URL = "https://api.linear.app/graphql"


class LinearAPIError(BugtrackerError):
    """Raised when a Linear API call fails."""


class LinearClient(BugtrackerClient):
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

    # --- Required BugtrackerClient methods ---

    def fetch_team_issues(
        self,
        team_key: str,
        status_name: str = "Todo",
        first: int = 50,
        project_name: str = "",
    ) -> list[Issue]:
        """Fetch issues for a team in a given workflow state."""
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
            # Parse blocking relations
            blocked_by = []
            for rel in node.get("relations", {}).get("nodes", []):
                if rel.get("type") != "isBlockedBy":
                    continue
                related = rel.get("relatedIssue") or {}
                state_type = (related.get("state") or {}).get("type", "")
                if state_type not in ("completed", "canceled"):
                    blocked_by.append(related.get("identifier", ""))
            for rel in node.get("inverseRelations", {}).get("nodes", []):
                if rel.get("type") != "blocks":
                    continue
                related = rel.get("issue") or {}
                state_type = (related.get("state") or {}).get("type", "")
                if state_type not in ("completed", "canceled"):
                    blocked_by.append(related.get("identifier", ""))
            # Parse children
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
                Issue(
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

    def add_labels(self, issue_id: str, label_ids: list[str]) -> None:
        """Add labels to an issue, preserving existing labels."""
        data = self._execute(ISSUE_LABELS_QUERY, {"issueId": issue_id})
        issue = data.get("issue")
        current_ids: list[str] = []
        if issue:
            for label in issue.get("labels", {}).get("nodes", []):
                current_ids.append(label["id"])
        merged = list(current_ids)
        for lid in label_ids:
            if lid not in merged:
                merged.append(lid)
        result = self._execute(
            ADD_LABELS_MUTATION,
            {"issueId": issue_id, "labelIds": merged},
        )
        if not result.get("issueUpdate", {}).get("success"):
            raise LinearAPIError(
                f"Failed to add labels to issue {issue_id}"
            )

    def fetch_issue_labels(
        self, identifier: str
    ) -> tuple[str, list[str]] | None:
        """Fetch title and label names for a single issue by identifier."""
        try:
            data = self._execute(
                ISSUE_LABELS_BY_IDENTIFIER_QUERY, {"identifier": identifier}
            )
        except LinearAPIError:
            logger.warning("Failed to fetch labels for issue %s", identifier)
            return None
        issue = data.get("issue")
        if not issue:
            return None
        title = issue.get("title", "")
        label_nodes = issue.get("labels", {}).get("nodes", [])
        return title, [ln.get("name", "") for ln in label_nodes]

    def fetch_issue_state_type(self, identifier: str) -> str | None:
        """Fetch the workflow state type of a single issue by identifier."""
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

    def fetch_issue_details(self, identifier: str) -> IssueDetails:
        """Fetch full details for a single issue by identifier."""
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

        child_nodes = issue.get("children", {}).get("nodes", [])
        children_ids = [ch.get("identifier", "") for ch in child_nodes]

        label_nodes = issue.get("labels", {}).get("nodes", [])
        labels = [ln.get("name", "") for ln in label_nodes]

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

        comment_nodes = issue.get("comments", {}).get("nodes", [])
        comments = [
            Comment(
                body=c.get("body", ""),
                author=(c.get("user") or {}).get("name", ""),
                created_at=c.get("createdAt", ""),
            )
            for c in comment_nodes
        ]

        return IssueDetails(
            id=issue.get("id", ""),
            ticket_id=issue.get("identifier", identifier),
            title=issue.get("title", ""),
            description=issue.get("description"),
            status=state.get("name"),
            priority=issue.get("priority"),
            url=issue.get("url", ""),
            assignee_name=assignee.get("name"),
            assignee_email=assignee.get("email"),
            creator_name=creator.get("name"),
            project_name=project.get("name"),
            team_name=team.get("name"),
            estimate=issue.get("estimate"),
            due_date=issue.get("dueDate"),
            parent_id=parent.get("identifier"),
            children_ids=children_ids,
            blocked_by=blocked_by,
            blocks=blocks,
            labels=labels,
            comments=comments,
            created_at=issue.get("createdAt"),
            updated_at=issue.get("updatedAt"),
            completed_at=issue.get("completedAt"),
            raw=issue,
        )

    def get_team_id(self, team_key: str) -> str:
        """Return the internal UUID for a team given its key."""
        data = self._execute(TEAM_STATES_QUERY, {"teamKey": team_key})
        teams = data.get("teams", {}).get("nodes", [])
        if not teams:
            raise LinearAPIError(f"Team with key '{team_key}' not found")
        return teams[0]["id"]

    def get_label_id(self, team_key: str, label_name: str) -> str | None:
        """Look up a label by name (team-scoped or workspace-scoped)."""
        data = self._execute(TEAM_LABELS_QUERY, {"teamKey": team_key})
        for label in data.get("issueLabels", {}).get("nodes", []):
            if label["name"].lower() == label_name.lower():
                return label["id"]
        return None

    def get_or_create_label(self, team_key: str, label_name: str) -> str:
        """Get an existing label id or create one."""
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
    ) -> CreatedIssue:
        """Create a new issue and return its core identifiers."""
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
        issue = result.get("issue", {})
        return CreatedIssue(
            id=issue.get("id", ""),
            identifier=issue.get("identifier", ""),
            url=issue.get("url", ""),
        )

    # --- Optional BugtrackerClient methods ---

    def count_active_issues(self) -> ActiveIssuesCount | None:
        """Count all non-archived issues with per-project breakdown."""
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

    def count_active_issues_for_project(self, project_name: str) -> int | None:
        """Count all non-archived issues for a specific project."""
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

    def fetch_completed_issues(
        self,
        team_key: str,
        first: int = 50,
        project_name: str = "",
        state_types: list[str] | None = None,
    ) -> list[Issue]:
        """Fetch completed/canceled issues sorted by updatedAt ascending.

        Note: Returns Issue instances with minimal fields populated.
        The cleanup service accesses raw dicts via fetch_completed_issues_raw().
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
        nodes = data.get("issues", {}).get("nodes", [])
        return [
            Issue(
                id=n.get("id", ""),
                identifier=n.get("identifier", ""),
                title=n.get("title", ""),
                priority=0,
                url="",
            )
            for n in nodes
        ]

    def fetch_completed_issues_raw(
        self,
        team_key: str,
        first: int = 50,
        project_name: str = "",
        state_types: list[str] | None = None,
    ) -> list[dict]:
        """Fetch completed/canceled issues as raw dicts.

        Used by CleanupService which needs full node data for filtering.
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

    def fetch_open_issues_with_label(
        self,
        team_key: str,
        label_name: str,
        first: int = 50,
    ) -> list[Issue]:
        """Fetch non-completed, non-canceled issues with a given label."""
        data = self._execute(
            ISSUES_BY_LABEL_QUERY,
            {"teamKey": team_key, "labelName": label_name, "first": first},
        )
        nodes = data.get("issues", {}).get("nodes", [])
        return [
            Issue(
                id=n.get("id", ""),
                identifier=n.get("identifier", ""),
                title=n.get("title", ""),
                priority=0,
                url="",
            )
            for n in nodes
        ]

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

    def get_project_id(self, project_name: str) -> str | None:
        """Return the internal UUID for a project given its name, or None."""
        data = self._execute(PROJECT_BY_NAME_QUERY, {"name": project_name})
        nodes = data.get("projects", {}).get("nodes", [])
        if not nodes:
            return None
        return nodes[0]["id"]

    def list_teams(self) -> list[dict]:
        """List all teams accessible by the API key."""
        data = self._execute(LIST_TEAMS_QUERY)
        return data.get("teams", {}).get("nodes", [])

    def list_team_projects(self, team_id: str) -> list[dict]:
        """List all projects for a given team."""
        data = self._execute(LIST_TEAM_PROJECTS_QUERY, {"teamId": team_id})
        team = data.get("team")
        if not team:
            return []
        return team.get("projects", {}).get("nodes", [])

    def get_organization(self) -> dict:
        """Get the organization info (urlKey, name)."""
        data = self._execute(ORGANIZATION_QUERY)
        org = data.get("organization")
        if not org:
            raise LinearAPIError("Failed to retrieve organization info")
        return org

    def create_project(
        self,
        team_id: str,
        name: str,
        description: str | None = None,
    ) -> dict:
        """Create a new Linear project and return its id and name."""
        input_data: dict = {"name": name, "teamIds": [team_id]}
        if description:
            input_data["description"] = description
        data = self._execute(CREATE_PROJECT_MUTATION, {"input": input_data})
        result = data.get("projectCreate", {})
        if not result.get("success"):
            raise LinearAPIError(f"Failed to create project '{name}'")
        return result["project"]

    def get_or_create_project(
        self,
        team_key: str,
        project_name: str,
    ) -> dict:
        """Get an existing project by name or create one in the given team.

        Returns a dict with ``id`` and ``name`` keys.
        """
        existing_id = self.get_project_id(project_name)
        if existing_id is not None:
            return {"id": existing_id, "name": project_name}
        team_id = self.get_team_id(team_key)
        return self.create_project(team_id, project_name)
