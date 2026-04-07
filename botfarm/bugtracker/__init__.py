"""Abstract bugtracker interface layer.

Re-exports all public types for clean imports::

    from botfarm.bugtracker import BugtrackerClient, Issue, PollResult

Factory functions create the correct adapter based on config::

    from botfarm.bugtracker import create_client, create_pollers
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .base import BugtrackerClient, BugtrackerPoller
from .errors import BugtrackerError
from .types import (
    ActiveIssuesCount,
    Comment,
    CreatedIssue,
    Issue,
    IssueDetails,
    PollResult,
    issue_details_to_history_kwargs,
)

if TYPE_CHECKING:
    from botfarm.config import BotfarmConfig, BugtrackerConfig, ProjectConfig

__all__ = [
    "ActiveIssuesCount",
    "BugtrackerClient",
    "BugtrackerError",
    "BugtrackerPoller",
    "Comment",
    "CreatedIssue",
    "Issue",
    "IssueDetails",
    "PollResult",
    "create_client",
    "create_poller",
    "create_pollers",
    "issue_details_to_history_kwargs",
]


def create_client(
    config: BotfarmConfig | None = None,
    *,
    api_key: str | None = None,
    email: str | None = None,
    bugtracker_type: str = "linear",
    bt_config: "BugtrackerConfig | None" = None,
) -> BugtrackerClient:
    """Create a bugtracker client based on the configured tracker type.

    When *bt_config* is provided, it takes precedence over ``config.bugtracker``
    — use this for per-project bugtracker overrides.

    When *config* is provided (without *bt_config*), the tracker type and API
    key are read from ``config.bugtracker``.  *api_key* overrides the config
    value (useful for alternate identities like the coder bot).

    For Jira, *email* overrides the config email — needed when creating a
    coder client whose Jira account differs from the owner's.

    When called without *config* (e.g. during ``botfarm init``), the caller
    must supply *api_key* explicitly; *bugtracker_type* defaults to
    ``"linear"``.
    """
    from botfarm.config import BugtrackerConfig, JiraBugtrackerConfig

    effective: BugtrackerConfig | None = bt_config or (config.bugtracker if config else None)
    bt_type = effective.type if effective else bugtracker_type
    if api_key:
        key = api_key
    elif effective:
        key = effective.api_key
    else:
        raise ValueError("api_key is required when config is not provided")
    if bt_type == "linear":
        from botfarm.bugtracker.linear import LinearClient

        return LinearClient(api_key=key)
    if bt_type == "jira":
        from botfarm.bugtracker.jira.client import JiraClient

        if effective and isinstance(effective, JiraBugtrackerConfig):
            jira_email = email or effective.email
            return JiraClient(
                url=effective.url,
                email=jira_email,
                api_token=key,
            )
        raise ValueError("Jira bugtracker requires url and email in config")
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")


def create_poller(
    config: BotfarmConfig,
    project: ProjectConfig,
) -> BugtrackerPoller:
    """Create a single poller for one project based on the tracker type.

    Uses per-project bugtracker config when available, falling back to
    the global ``config.bugtracker``.
    """
    from botfarm.config import resolve_project_bugtracker

    bt = resolve_project_bugtracker(config.bugtracker, project)
    bt_type = bt.type
    if bt_type == "linear":
        from botfarm.bugtracker.linear import LinearClient, LinearPoller

        client = LinearClient(api_key=bt.api_key)
        coder_key = config.identities.coder.tracker_api_key
        coder_client = LinearClient(api_key=coder_key) if coder_key else None
        include_tags = project.include_tags if project.include_tags is not None else bt.include_tags
        return LinearPoller(
            client=client,
            project=project,
            exclude_tags=bt.exclude_tags,
            todo_status=bt.todo_status,
            coder_client=coder_client,
            include_tags=include_tags,
        )
    if bt_type == "jira":
        from botfarm.bugtracker.jira import create_poller as jira_create_poller

        return jira_create_poller(config, project)
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")


def create_pollers(config: BotfarmConfig) -> list[BugtrackerPoller]:
    """Create pollers for all configured projects based on the tracker type.

    When all projects use the same tracker type (either via global config or
    identical per-project overrides), the tracker-specific bulk factory is
    used for efficiency (shared client instances).  When projects use mixed
    tracker types, each project gets its own poller via ``create_poller()``.
    """
    from botfarm.config import resolve_project_bugtracker

    # Check if any project overrides the tracker type
    types = {
        resolve_project_bugtracker(config.bugtracker, p).type
        for p in config.projects
    }

    # Validate that all types are supported
    supported = {"linear", "jira"}
    all_types = types | {config.bugtracker.type}
    unsupported = all_types - supported
    if unsupported:
        raise ValueError(f"Unknown bugtracker type: {sorted(unsupported)[0]!r}")

    if len(types) <= 1:
        # All projects use the same tracker — use bulk factory for efficiency
        bt_type = types.pop() if types else config.bugtracker.type
        if bt_type == "linear" and not any(p.bugtracker for p in config.projects):
            from botfarm.bugtracker.linear import create_pollers as linear_create_pollers
            return list(linear_create_pollers(config))
        if bt_type == "jira" and not any(p.bugtracker for p in config.projects):
            from botfarm.bugtracker.jira import create_pollers as jira_create_pollers
            return list(jira_create_pollers(config))

    # Mixed types or per-project overrides — create individually
    return [create_poller(config, p) for p in config.projects]
