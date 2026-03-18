"""Abstract base classes for bugtracker adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod

from .types import ActiveIssuesCount, CreatedIssue, Issue, IssueDetails, PollResult


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


class BugtrackerPoller(ABC):
    """Polls a bugtracker for actionable tickets and manages issue state."""

    # --- Required properties ---

    @property
    @abstractmethod
    def project_name(self) -> str:
        """The project name this poller is configured for."""

    @property
    @abstractmethod
    def team_key(self) -> str:
        """The team key this poller is configured for."""

    # --- Required methods ---

    @abstractmethod
    def poll(self, active_ticket_ids: set[str] | None = None) -> PollResult:
        """Fetch actionable issues, filter, and return sorted results."""

    @abstractmethod
    def get_state_id(self, state_name: str) -> str:
        """Look up a workflow state ID by name."""

    @abstractmethod
    def is_issue_terminal(self, identifier: str) -> bool:
        """Check whether an issue is in a terminal state."""

    @abstractmethod
    def move_issue(self, issue_id: str, state_name: str) -> None:
        """Move an issue to the named workflow state."""

    @abstractmethod
    def assign_issue(self, issue_id: str, assignee_id: str) -> None:
        """Assign an issue to a user."""

    @abstractmethod
    def add_comment(self, issue_id: str, body: str) -> None:
        """Add a comment to an issue."""

    @abstractmethod
    def add_labels(self, issue_id: str, label_names: list[str]) -> None:
        """Add labels to an issue by name."""

    # --- Optional methods ---

    def add_comment_as_owner(self, issue_id: str, body: str) -> None:
        """Add a comment using the owner's identity."""
        raise NotImplementedError
