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
    from botfarm.config import BotfarmConfig, ProjectConfig

__all__ = [
    "ActiveIssuesCount",
    "BugtrackerClient",
    "BugtrackerError",
    "BugtrackerPoller",
    "CleanupCandidate",
    "CleanupResult",
    "CleanupService",
    "Comment",
    "CooldownError",
    "CreatedIssue",
    "Issue",
    "IssueDetails",
    "PollResult",
    "UndoResult",
    "create_client",
    "create_poller",
    "create_pollers",
    "issue_details_to_history_kwargs",
]


# Re-export cleanup types so consumer modules don't import from
# botfarm.bugtracker.linear.* directly.  Cleanup is currently
# Linear-specific; a future bugtracker adapter can provide its own.
from .linear.cleanup import (  # noqa: E402
    CleanupCandidate,
    CleanupResult,
    CleanupService,
    CooldownError,
    UndoResult,
)


def create_client(
    config: BotfarmConfig | None = None,
    *,
    api_key: str | None = None,
    bugtracker_type: str = "linear",
) -> BugtrackerClient:
    """Create a bugtracker client based on the configured tracker type.

    When *config* is provided, the tracker type and API key are read from
    ``config.bugtracker``.  *api_key* overrides the config value (useful
    for alternate identities like the coder bot).

    When called without *config* (e.g. during ``botfarm init``), the caller
    must supply *api_key* explicitly; *bugtracker_type* defaults to
    ``"linear"``.
    """
    bt_type = config.bugtracker.type if config else bugtracker_type
    if api_key:
        key = api_key
    elif config:
        key = config.bugtracker.api_key
    else:
        raise ValueError("api_key is required when config is not provided")
    if bt_type == "linear":
        from botfarm.bugtracker.linear import LinearClient

        return LinearClient(api_key=key)
    if bt_type == "jira":
        from botfarm.bugtracker.jira.client import JiraClient
        from botfarm.config import JiraBugtrackerConfig

        if config and isinstance(config.bugtracker, JiraBugtrackerConfig):
            return JiraClient(
                url=config.bugtracker.url,
                email=config.bugtracker.email,
                api_token=key,
            )
        raise ValueError("Jira bugtracker requires url and email in config")
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")


def create_poller(
    config: BotfarmConfig,
    project: ProjectConfig,
) -> BugtrackerPoller:
    """Create a single poller for one project based on the tracker type."""
    bt_type = config.bugtracker.type
    if bt_type == "linear":
        from botfarm.bugtracker.linear import LinearClient, LinearPoller

        client = LinearClient(api_key=config.bugtracker.api_key)
        coder_key = config.identities.coder.linear_api_key
        coder_client = LinearClient(api_key=coder_key) if coder_key else None
        return LinearPoller(
            client=client,
            project=project,
            exclude_tags=config.bugtracker.exclude_tags,
            todo_status=config.bugtracker.todo_status,
            coder_client=coder_client,
        )
    if bt_type == "jira":
        from botfarm.bugtracker.jira.client import JiraClient
        from botfarm.bugtracker.jira.poller import JiraPoller
        from botfarm.config import JiraBugtrackerConfig

        bt = config.bugtracker
        if not isinstance(bt, JiraBugtrackerConfig):
            raise ValueError("Jira bugtracker requires JiraBugtrackerConfig")
        client = JiraClient(url=bt.url, email=bt.email, api_token=bt.api_key)
        coder_token = config.identities.coder.jira_api_token
        coder_client = (
            JiraClient(url=bt.url, email=bt.email, api_token=coder_token)
            if coder_token else None
        )
        return JiraPoller(
            client=client,
            project=project,
            exclude_tags=bt.exclude_tags,
            todo_status=bt.todo_status,
            coder_client=coder_client,
        )
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")


def create_pollers(config: BotfarmConfig) -> list[BugtrackerPoller]:
    """Create pollers for all configured projects based on the tracker type."""
    bt_type = config.bugtracker.type
    if bt_type == "linear":
        from botfarm.bugtracker.linear import create_pollers as linear_create_pollers

        return list(linear_create_pollers(config))
    if bt_type == "jira":
        from botfarm.bugtracker.jira import create_pollers as jira_create_pollers

        return list(jira_create_pollers(config))
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")
