"""Shared dataclasses for the bugtracker abstraction layer."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Issue:
    """A bug-tracker issue returned from polling.

    Drop-in replacement for ``LinearIssue``.
    """

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
    children_states: list[tuple[str, str]] | None = None


@dataclass
class PollResult:
    """Result of a poller ``poll()`` call."""

    candidates: list[Issue] = field(default_factory=list)
    blocked: list[Issue] = field(default_factory=list)
    auto_close_parents: list[Issue] = field(default_factory=list)


@dataclass
class CreatedIssue:
    """Result of creating an issue via ``BugtrackerClient.create_issue``."""

    id: str
    identifier: str
    url: str


@dataclass
class IssueDetails:
    """Full details for a single issue.

    Returned by ``BugtrackerClient.fetch_issue_details``.
    """

    ticket_id: str
    title: str
    url: str
    description: str | None = None
    status: str | None = None
    priority: int | None = None
    assignee_name: str | None = None
    assignee_email: str | None = None
    creator_name: str | None = None
    project_name: str | None = None
    team_name: str | None = None
    estimate: int | None = None
    due_date: str | None = None
    parent_id: str | None = None
    children_ids: list[str] = field(default_factory=list)
    blocked_by: list[str] = field(default_factory=list)
    blocks: list[str] = field(default_factory=list)
    labels: list[str] = field(default_factory=list)
    comments: list[dict] = field(default_factory=list)
    created_at: str | None = None
    updated_at: str | None = None
    completed_at: str | None = None
    raw: dict = field(default_factory=dict)


@dataclass
class ActiveIssuesCount:
    """Result of counting all non-archived issues."""

    total: int
    by_project: dict[str, int] = field(default_factory=dict)
