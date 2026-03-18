"""Linear poller implementing the abstract BugtrackerPoller."""

from __future__ import annotations

import logging

from botfarm.bugtracker.base import BugtrackerPoller
from botfarm.bugtracker.types import PollResult
from botfarm.config import BotfarmConfig, ProjectConfig

from .client import LinearAPIError, LinearClient

logger = logging.getLogger(__name__)


class LinearPoller(BugtrackerPoller):
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
        self._state_cache: dict[str, str] | None = None

    @property
    def project_name(self) -> str:
        return self._project.name

    @property
    def team_key(self) -> str:
        return self._project.team

    def poll(self, active_ticket_ids: set[str] | None = None) -> PollResult:
        """Fetch Todo issues, filter, and return sorted by manual sort order."""
        if active_ticket_ids is None:
            active_ticket_ids = set()

        issues = self._client.fetch_team_issues(
            team_key=self._project.team,
            status_name=self._todo_status,
            project_name=self._project.tracker_project,
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
            self._project.team,
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
                self._project.team
            )
        state_id = self._state_cache.get(state_name)
        if state_id is None:
            raise LinearAPIError(
                f"State '{state_name}' not found for team "
                f"'{self._project.team}'"
            )
        return state_id

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
        """Assign an issue to a user by their Linear user ID."""
        self._client.assign_issue(issue_id, assignee_id)
        logger.info("Assigned issue %s to user %s", issue_id, assignee_id)

    def add_comment(self, issue_id: str, body: str) -> None:
        """Add a comment to an issue (uses coder client)."""
        self._coder_client.add_comment(issue_id, body)

    def add_labels(self, issue_id: str, label_names: list[str]) -> None:
        """Add labels by name to an issue (uses coder client). Creates labels if needed."""
        label_ids = [
            self._coder_client.get_or_create_label(self._project.team, name)
            for name in label_names
        ]
        self._coder_client.add_labels(issue_id, label_ids)
        logger.info("Added labels %s to issue %s", label_names, issue_id)

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
    client = LinearClient(api_key=config.bugtracker.api_key)
    coder_key = config.identities.coder.linear_api_key
    coder_client = LinearClient(api_key=coder_key) if coder_key else None
    return [
        LinearPoller(
            client=client,
            project=project,
            exclude_tags=config.bugtracker.exclude_tags,
            todo_status=config.bugtracker.todo_status,
            coder_client=coder_client,
        )
        for project in config.projects
    ]
