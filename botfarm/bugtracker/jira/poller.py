"""Jira poller implementing the abstract BugtrackerPoller."""

from __future__ import annotations

from typing import TYPE_CHECKING

from botfarm.bugtracker.base import BugtrackerPoller
from botfarm.bugtracker.errors import BugtrackerError
from botfarm.config import BotfarmConfig, JiraBugtrackerConfig

if TYPE_CHECKING:
    from botfarm.bugtracker.jira.client import JiraClient


class JiraPoller(BugtrackerPoller):
    """Polls Jira for Todo tickets in a project's team and returns prioritized candidates.

    One JiraPoller is created per project. It filters out excluded labels
    and tickets already tracked in the database.

    Inherits all shared logic from :class:`BugtrackerPoller`; only overrides
    :meth:`get_state_id` for Jira's case-insensitive status matching.
    """

    def get_state_id(self, state_name: str) -> str:
        """Look up a workflow state ID by name, caching the result.

        Jira status names are matched case-insensitively because the Jira
        API returns exact case but JQL queries are case-insensitive.
        """
        if self._state_cache is None:
            raw_states = self._client.get_team_states(self._project.team)
            # Build cache with lowercased keys for case-insensitive lookup,
            # but also keep original-case entries for exact matches.
            self._state_cache = {}
            for name, sid in raw_states.items():
                self._state_cache[name] = sid
                self._state_cache[name.lower()] = sid
        state_id = self._state_cache.get(state_name)
        if state_id is None:
            state_id = self._state_cache.get(state_name.lower())
        if state_id is None:
            raise BugtrackerError(
                f"State '{state_name}' not found for project "
                f"'{self._project.team}'"
            )
        return state_id


def _create_jira_clients(config: BotfarmConfig) -> tuple[JiraClient, JiraClient | None]:
    """Create owner and optional coder Jira clients from config.

    Returns ``(client, coder_client)`` where *coder_client* is ``None``
    when no coder token is configured.
    """
    from botfarm.bugtracker.jira.client import JiraClient

    bt = config.bugtracker
    if not isinstance(bt, JiraBugtrackerConfig):
        raise ValueError("Jira bugtracker requires JiraBugtrackerConfig")

    client = JiraClient(url=bt.url, email=bt.email, api_token=bt.api_key)
    coder_token = config.identities.coder.jira_api_token
    coder_email = config.identities.coder.jira_email or bt.email
    coder_client = (
        JiraClient(url=bt.url, email=coder_email, api_token=coder_token)
        if coder_token else None
    )
    return client, coder_client


def _create_jira_clients_from_bt(
    bt: JiraBugtrackerConfig,
    identities,
) -> tuple[JiraClient, JiraClient | None]:
    """Create Jira clients from an explicit bugtracker config.

    Like :func:`_create_jira_clients` but takes a resolved
    :class:`JiraBugtrackerConfig` directly — used for per-project overrides.
    """
    from botfarm.bugtracker.jira.client import JiraClient

    client = JiraClient(url=bt.url, email=bt.email, api_token=bt.api_key)
    coder_token = identities.coder.jira_api_token
    coder_email = identities.coder.jira_email or bt.email
    coder_client = (
        JiraClient(url=bt.url, email=coder_email, api_token=coder_token)
        if coder_token else None
    )
    return client, coder_client


def create_poller(
    config: BotfarmConfig,
    project: object,
) -> JiraPoller:
    """Create a single JiraPoller for one project.

    Uses per-project bugtracker config when available, falling back to
    the global ``config.bugtracker``.
    """
    from botfarm.config import resolve_project_bugtracker
    bt = resolve_project_bugtracker(config.bugtracker, project)
    if not isinstance(bt, JiraBugtrackerConfig):
        raise ValueError("Jira bugtracker requires JiraBugtrackerConfig")
    client, coder_client = _create_jira_clients_from_bt(bt, config.identities)
    include_tags = project.include_tags if project.include_tags is not None else bt.include_tags
    return JiraPoller(
        client=client,
        project=project,
        exclude_tags=bt.exclude_tags,
        todo_status=bt.todo_status,
        coder_client=coder_client,
        include_tags=include_tags,
    )


def create_pollers(config: BotfarmConfig) -> list[JiraPoller]:
    """Create one JiraPoller per configured project.

    When ``identities.coder.jira_api_token`` is configured, a separate
    client is created for coder-initiated operations (moving tickets,
    posting comments) so they appear under the coder bot's identity.
    Polling always uses the owner's client.
    """
    bt = config.bugtracker
    if not isinstance(bt, JiraBugtrackerConfig):
        raise ValueError("create_pollers called with non-Jira config")

    client, coder_client = _create_jira_clients(config)
    return [
        JiraPoller(
            client=client,
            project=project,
            exclude_tags=bt.exclude_tags,
            todo_status=bt.todo_status,
            coder_client=coder_client,
            include_tags=project.include_tags if project.include_tags is not None else bt.include_tags,
        )
        for project in config.projects
    ]
