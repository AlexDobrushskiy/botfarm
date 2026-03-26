"""Abstract base classes for bugtracker adapters."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from .types import ActiveIssuesCount, CreatedIssue, Issue, IssueDetails, PollResult

logger = logging.getLogger(__name__)


class BugtrackerClient(ABC):
    """Low-level client interface that all bugtracker adapters must implement."""

    # --- Required methods (must be implemented) ---

    @abstractmethod
    def fetch_team_issues(
        self,
        team_key: str,
        status_name: str = "Todo",
        first: int = 50,
        project_name: str = "",
    ) -> list[Issue]:
        """Fetch issues for a team in a given workflow state."""

    @abstractmethod
    def get_team_states(self, team_key: str) -> dict[str, str]:
        """Return a mapping of state name -> state ID for a team."""

    @abstractmethod
    def update_issue_state(self, issue_id: str, state_id: str) -> None:
        """Move an issue to a new workflow state."""

    @abstractmethod
    def add_comment(self, issue_id: str, body: str) -> None:
        """Add a comment to an issue."""

    @abstractmethod
    def get_viewer_id(self) -> str:
        """Return the user ID of the authenticated API key owner."""

    @abstractmethod
    def assign_issue(self, issue_id: str, assignee_id: str) -> None:
        """Assign an issue to a user."""

    @abstractmethod
    def add_labels(self, issue_id: str, label_ids: list[str]) -> None:
        """Add labels to an issue by their IDs."""

    @abstractmethod
    def fetch_issue_labels(self, identifier: str) -> tuple[str, list[str]] | None:
        """Fetch title and label names for an issue by identifier.

        Returns ``(title, [label_names])`` or ``None`` if not found.
        """

    @abstractmethod
    def fetch_issue_state_type(self, identifier: str) -> str | None:
        """Fetch the workflow state type of a single issue.

        Returns the state type string (e.g. ``"completed"``, ``"canceled"``)
        or ``None`` if the issue is not found.
        """

    @abstractmethod
    def fetch_issue_details(self, identifier: str) -> IssueDetails:
        """Fetch full details for a single issue by identifier."""

    @abstractmethod
    def get_team_id(self, team_key: str) -> str:
        """Return the internal ID for a team given its key."""

    @abstractmethod
    def get_label_id(self, team_key: str, label_name: str) -> str | None:
        """Look up a label ID by name within a team.

        Returns ``None`` if the label does not exist.
        """

    @abstractmethod
    def get_or_create_label(self, team_key: str, label_name: str) -> str:
        """Get a label ID by name, creating it if it doesn't exist."""

    @abstractmethod
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

    # --- Optional methods (raise NotImplementedError by default) ---

    def count_active_issues(self) -> ActiveIssuesCount | None:
        """Count all non-archived issues with per-project breakdown."""
        raise NotImplementedError

    def count_active_issues_for_project(self, project_name: str) -> int | None:
        """Count all non-archived issues for a specific project."""
        raise NotImplementedError

    def archive_issue(self, issue_id: str) -> bool:
        """Archive an issue. Returns True on success."""
        raise NotImplementedError

    def delete_issue(self, issue_id: str) -> bool:
        """Delete an issue. Returns True on success."""
        raise NotImplementedError

    def unarchive_issue(self, issue_id: str) -> bool:
        """Unarchive an issue. Returns True on success."""
        raise NotImplementedError

    def fetch_completed_issues(
        self,
        team_key: str,
        first: int = 50,
        project_name: str = "",
        state_types: list[str] | None = None,
    ) -> list[Issue]:
        """Fetch completed/canceled issues."""
        raise NotImplementedError

    def fetch_open_issues_with_label(
        self,
        team_key: str,
        label_name: str,
        first: int = 50,
    ) -> list[Issue]:
        """Fetch open issues that have a specific label."""
        raise NotImplementedError

    def list_teams(self) -> list[dict]:
        """List all teams in the organization."""
        raise NotImplementedError

    def list_team_projects(self, team_id: str) -> list[dict]:
        """List all projects belonging to a team."""
        raise NotImplementedError

    def get_organization(self) -> dict:
        """Return organization-level information."""
        raise NotImplementedError

    def get_project_id(self, project_name: str) -> str | None:
        """Look up a project ID by name. Returns ``None`` if not found."""
        raise NotImplementedError

    def create_project(
        self,
        team_id: str,
        name: str,
        description: str | None = None,
    ) -> dict:
        """Create a new project. Returns a dict with ``id`` and ``name``."""
        raise NotImplementedError

    def get_or_create_project(
        self,
        team_key: str,
        project_name: str,
    ) -> dict:
        """Get existing or create a new project. Returns dict with ``id`` and ``name``."""
        raise NotImplementedError


class BugtrackerPoller(ABC):
    """Polls a bugtracker for actionable tickets and manages issue state.

    Subclasses only need to override :meth:`get_state_id` to provide
    tracker-specific state lookup (e.g. case-insensitive matching for Jira).
    All other methods have concrete implementations that delegate to the
    underlying :class:`BugtrackerClient`.
    """

    def __init__(
        self,
        client: BugtrackerClient,
        project: object,
        exclude_tags: list[str],
        todo_status: str = "Todo",
        coder_client: BugtrackerClient | None = None,
        include_tags: list[str] | None = None,
    ) -> None:
        self._client = client
        self._coder_client = coder_client or client
        self._project = project
        self._exclude_tags = set(tag.lower() for tag in exclude_tags)
        self._include_tags = set(tag.lower() for tag in (include_tags or []))
        self._todo_status = todo_status
        self._state_cache: dict[str, str] | None = None

    @property
    def project_name(self) -> str:
        """The project name this poller is configured for."""
        return self._project.name  # type: ignore[attr-defined]

    @property
    def team_key(self) -> str:
        """The team key this poller is configured for."""
        return self._project.team  # type: ignore[attr-defined]

    def poll(self, active_ticket_ids: set[str] | None = None) -> PollResult:
        """Fetch actionable issues, filter, and return sorted results."""
        if active_ticket_ids is None:
            active_ticket_ids = set()

        issues = self._client.fetch_team_issues(
            team_key=self._project.team,  # type: ignore[attr-defined]
            status_name=self._todo_status,
            project_name=self._project.tracker_project,  # type: ignore[attr-defined]
        )

        pre_parent_candidates = []
        blocked_issues = []
        for issue in issues:
            issue_labels = set(
                label.lower() for label in (issue.labels or [])
            )
            if "failed" in issue_labels:
                logger.debug(
                    "Skipping %s: has 'Failed' label (defense-in-depth)",
                    issue.identifier,
                )
                continue

            if self._include_tags and not (issue_labels & self._include_tags):
                logger.debug(
                    "Skipping %s: no matching include label(s)",
                    issue.identifier,
                )
                continue

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

        candidates = []
        auto_close_parents = []
        for issue in pre_parent_candidates:
            if issue.children_states is None:
                candidates.append(issue)
                continue

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

        candidates.sort(key=lambda i: i.sort_order)
        blocked_issues.sort(key=lambda i: i.sort_order)

        logger.debug(
            "Polled %s: %d Todo issues, %d candidates, %d blocked, %d auto-close parents",
            self._project.team,  # type: ignore[attr-defined]
            len(issues),
            len(candidates),
            len(blocked_issues),
            len(auto_close_parents),
        )
        return PollResult(candidates=candidates, blocked=blocked_issues, auto_close_parents=auto_close_parents)

    @abstractmethod
    def get_state_id(self, state_name: str) -> str:
        """Look up a workflow state ID by name."""

    def is_issue_terminal(self, identifier: str) -> bool:
        """Check whether an issue is in a terminal state (completed/canceled)."""
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
        """Assign an issue to a user."""
        self._client.assign_issue(issue_id, assignee_id)
        logger.info("Assigned issue %s to user %s", issue_id, assignee_id)

    def add_comment(self, issue_id: str, body: str) -> None:
        """Add a comment to an issue (uses coder client)."""
        self._coder_client.add_comment(issue_id, body)

    def add_labels(self, issue_id: str, label_names: list[str]) -> None:
        """Add labels by name to an issue (uses coder client). Creates labels if needed."""
        label_ids = [
            self._coder_client.get_or_create_label(self._project.team, name)  # type: ignore[attr-defined]
            for name in label_names
        ]
        self._coder_client.add_labels(issue_id, label_ids)
        logger.info("Added labels %s to issue %s", label_names, issue_id)

    def create_issue(
        self,
        *,
        title: str,
        description: str = "",
        priority: int | None = None,
        label_names: list[str] | None = None,
        project_id: str | None = None,
    ) -> CreatedIssue:
        """Create a new issue in the poller's team (uses coder client).

        Resolves team ID and label IDs automatically from names.
        """
        team_key = self._project.team  # type: ignore[attr-defined]
        team_id = self._coder_client.get_team_id(team_key)
        label_ids = None
        if label_names:
            label_ids = [
                self._coder_client.get_or_create_label(team_key, name)
                for name in label_names
            ]
        return self._coder_client.create_issue(
            team_id=team_id,
            title=title,
            description=description,
            priority=priority,
            label_ids=label_ids,
            project_id=project_id,
        )

    def add_comment_as_owner(self, issue_id: str, body: str) -> None:
        """Add a comment using the owner's client (for system-level notifications)."""
        self._client.add_comment(issue_id, body)
