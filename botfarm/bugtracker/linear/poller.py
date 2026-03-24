"""Linear poller implementing the abstract BugtrackerPoller."""

from __future__ import annotations

from botfarm.bugtracker.base import BugtrackerPoller
from botfarm.config import BotfarmConfig

from .client import LinearAPIError, LinearClient


class LinearPoller(BugtrackerPoller):
    """Polls Linear for Todo tickets in a project's team and returns prioritized candidates.

    One LinearPoller is created per project. It filters out excluded labels
    and tickets already tracked in the database.

    Inherits all shared logic from :class:`BugtrackerPoller`; only overrides
    :meth:`get_state_id` for Linear-specific error handling.
    """

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


def create_pollers(config: BotfarmConfig) -> list[LinearPoller]:
    """Create one LinearPoller per configured project.

    When ``identities.coder.tracker_api_key`` is configured, a separate
    ``LinearClient`` is created for coder-initiated operations (moving
    tickets, posting comments) so they appear under the coder bot's identity.
    Polling always uses the owner's client.
    """
    client = LinearClient(api_key=config.bugtracker.api_key)
    coder_key = config.identities.coder.tracker_api_key
    coder_client = LinearClient(api_key=coder_key) if coder_key else None
    return [
        LinearPoller(
            client=client,
            project=project,
            exclude_tags=config.bugtracker.exclude_tags,
            todo_status=config.bugtracker.todo_status,
            coder_client=coder_client,
            include_tags=project.include_tags if project.include_tags else config.bugtracker.include_tags,
        )
        for project in config.projects
    ]
