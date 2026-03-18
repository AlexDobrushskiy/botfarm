"""Abstract bugtracker interface layer.

Re-exports all public types for clean imports::

    from botfarm.bugtracker import BugtrackerClient, Issue, PollResult
"""

from .base import BugtrackerClient, BugtrackerPoller
from .errors import BugtrackerError
from .types import ActiveIssuesCount, Issue, PollResult

__all__ = [
    "ActiveIssuesCount",
    "BugtrackerClient",
    "BugtrackerError",
    "BugtrackerPoller",
    "Issue",
    "PollResult",
]
