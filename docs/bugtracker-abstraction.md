# Bugtracker Abstraction

Botfarm uses an abstract bugtracker interface so that different issue trackers can be supported. Currently Linear and Jira are implemented, and the architecture supports adding new adapters (e.g. GitHub Issues).

## Architecture

```
botfarm/bugtracker/
├── __init__.py      # Re-exports, factory functions (create_client, create_pollers)
├── base.py          # Abstract base classes: BugtrackerClient, BugtrackerPoller
├── errors.py        # BugtrackerError exception
├── types.py         # Shared data types: Issue, IssueDetails, PollResult, etc.
└── linear/          # Linear adapter
    ├── __init__.py
    ├── client.py    # LinearClient (implements BugtrackerClient)
    ├── poller.py    # LinearPoller (implements BugtrackerPoller), create_pollers()
    └── queries.py   # GraphQL query/mutation strings
```

## How to Add a New Bugtracker Adapter

### 1. Create the adapter package

Create a new directory under `botfarm/bugtracker/`, e.g. `botfarm/bugtracker/jira/`.

### 2. Implement `BugtrackerClient`

Subclass `botfarm.bugtracker.base.BugtrackerClient` and implement all abstract methods:

```python
from botfarm.bugtracker.base import BugtrackerClient
from botfarm.bugtracker.types import Issue, IssueDetails, PollResult

class JiraClient(BugtrackerClient):
    def __init__(self, api_key: str, base_url: str):
        self._api_key = api_key
        self._base_url = base_url

    def fetch_team_issues(self, team_key, status_name="Todo", first=50, project_name=""):
        ...  # Query Jira API

    def get_team_states(self, team_key):
        ...  # Return {state_name: state_id} mapping

    # ... implement all other abstract methods from BugtrackerClient
```

Key abstract methods to implement:
- `fetch_team_issues()` — poll for new work
- `get_team_states()` — map workflow state names to IDs
- `update_issue_state()` — move tickets between states
- `add_comment()` — post comments on tickets
- `get_viewer_id()` — identify the authenticated user
- `assign_issue()` — assign tickets
- `add_labels()` / `get_label_id()` / `get_or_create_label()` — label management
- `fetch_issue_labels()` / `fetch_issue_state_type()` / `fetch_issue_details()` — ticket queries
- `get_team_id()` — resolve team keys to internal IDs

Optional methods (have default implementations):
- `list_teams()` — used by CLI/dashboard for team selection
- `list_team_projects()` — used by CLI/dashboard for project selection
- `get_project_id()` — resolve project names to IDs
- `create_issue()` — create new tickets (used by refactoring analysis)
- `count_active_issues()` — capacity monitoring

### 3. Implement `BugtrackerPoller`

Subclass `botfarm.bugtracker.base.BugtrackerPoller`:

```python
from botfarm.bugtracker.base import BugtrackerPoller
from botfarm.bugtracker.types import PollResult

class JiraPoller(BugtrackerPoller):
    def __init__(self, client, project, ...):
        self._client = client
        self._project = project

    @property
    def team_key(self) -> str:
        return self._project.team

    def poll(self) -> PollResult:
        ...  # Use client to fetch and return new issues

    def move_to_in_progress(self, issue_id, assignee_id):
        ...

    def move_to_done(self, issue_id):
        ...

    # ... implement all other abstract methods
```

### 4. Register in the factory

Update `botfarm/bugtracker/__init__.py` to handle the new type in the factory functions:

```python
def create_client(config=None, *, api_key=None, bugtracker_type="linear"):
    bt_type = config.bugtracker.type if config else bugtracker_type
    ...
    if bt_type == "jira":
        from botfarm.bugtracker.jira import JiraClient
        return JiraClient(api_key=key, base_url=...)
    raise ValueError(f"Unknown bugtracker type: {bt_type!r}")
```

Update `create_poller()` and `create_pollers()` similarly.

### 5. Add the type to config validation

In `botfarm/config.py`, add the new type to the `supported_types` set:

```python
supported_types = {"linear", "jira"}
```

### 6. Configuration

The `bugtracker:` section in `config.yaml` already supports a `type` field:

```yaml
bugtracker:
  type: jira          # New adapter type
  api_key: ${JIRA_API_KEY}
  workspace: my-org
  ...
```

Adapter-specific fields can be added to the config as needed.

## Jira Adapter Notes

The Jira adapter (`botfarm/bugtracker/jira/`) follows the same structure as Linear. Key differences to be aware of when implementing:

### API Differences
- **Authentication:** Jira Cloud uses API tokens (email + token) via Basic auth, not bearer tokens
- **Issue keys:** Jira uses `PROJECT-123` format (similar to Linear identifiers)
- **Branch names:** Jira doesn't have a built-in `gitBranchName` field — derive from the issue key and summary
- **Blocking relations:** Jira uses issue link types (`Blocks`/`is blocked by`) rather than dedicated relation fields
- **Status transitions:** Jira workflows may require specific transition IDs rather than setting status by name; use the transition API

### Agent Prompt Variables
Stage template prompts use `{bugtracker_type}` to reference the tracker type. When `bugtracker.type` is `"jira"`, prompts render as e.g. "Work on Jira ticket PROJECT-123" (the value is title-cased at the injection point). The variable is automatically injected by the worker pipeline.

### Workflow Mapping
| Concept | Linear | Jira |
|---|---|---|
| Team | Team key (e.g. "SMA") | Project key (e.g. "PROJ") |
| Status polling | GraphQL `issues` query | JQL search |
| Comments | Linear comment API | Jira comment REST API |
| Labels | Linear labels | Jira labels |
| Relations | `isBlockedBy`/`blocks` | Issue links with link types |
| Branch name | `gitBranchName` field | Derived from key + summary |
| MCP server | `@tacticlaunch/mcp-linear` via `npx` | `mcp-atlassian` via `uvx` |
| MCP auth | `LINEAR_API_TOKEN` env var | `JIRA_URL` + `JIRA_USERNAME` + `JIRA_API_TOKEN` env vars |

See `docs/jira-workflow.md` for the agent-facing workflow guide.

### Agent MCP Tools

Botfarm auto-configures MCP tools for agent workers. The `build_bugtracker_mcp_config()` function in `worker.py` generates a JSON config that is written to a temp file and passed to the Claude subprocess via `--mcp-config`. This gives agents direct access to bugtracker MCP tools (e.g. fetching ticket details, creating issues, posting comments) without requiring any plugin installation.

The API key used for MCP config follows this priority:
1. `identities.coder.tracker_api_key` (per-identity key)
2. `bugtracker.api_key` (shared key)

Supported MCP servers:
- **Linear:** `@tacticlaunch/mcp-linear` via `npx`, authenticated with `LINEAR_API_TOKEN`
- **Jira:** `mcp-atlassian` via `uvx`, authenticated with `JIRA_URL` + `JIRA_USERNAME` + `JIRA_API_TOKEN`

## Shared Types

All adapters use the shared types from `botfarm/bugtracker/types.py`:

- `Issue` — lightweight issue representation (id, identifier, title, priority, labels, estimate)
- `IssueDetails` — full issue details including description, assignee, git branch name
- `PollResult` — result of polling (issues found, active count info)
- `ActiveIssuesCount` — issue count and limit for capacity monitoring
- `CreatedIssue` — result of creating a new issue
- `Comment` — issue comment with author info
