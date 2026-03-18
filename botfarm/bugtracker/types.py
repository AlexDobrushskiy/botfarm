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
class ActiveIssuesCount:
    """Result of counting all non-archived issues."""

    total: int
    by_project: dict[str, int] = field(default_factory=dict)
