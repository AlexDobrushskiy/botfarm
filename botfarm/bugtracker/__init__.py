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
    from botfarm.config import BotfarmConfig

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
    "create_pollers",
    "issue_details_to_history_kwargs",
]


def create_client(config: BotfarmConfig) -> BugtrackerClient:
    """Create a bugtracker client based on the configured tracker type."""
    bt_type = config.bugtracker.type
    if bt_type == "linear":
        from botfarm.linear import LinearClient

        return LinearClient(api_key=config.bugtracker.api_key)
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")


def create_pollers(config: BotfarmConfig) -> list[BugtrackerPoller]:
    """Create pollers for all configured projects based on the tracker type."""
    bt_type = config.bugtracker.type
    if bt_type == "linear":
        from botfarm.linear import create_pollers as linear_create_pollers

        return list(linear_create_pollers(config))
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")
