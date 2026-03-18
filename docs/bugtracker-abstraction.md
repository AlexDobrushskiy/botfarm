# Bugtracker Abstraction

Botfarm uses an abstract bugtracker interface so that different issue trackers can be supported. Currently only Linear is implemented, but the architecture supports adding new adapters (e.g. Jira, GitHub Issues).

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
    ├── cleanup.py   # Linear-specific cleanup (archive/delete)
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
- `archive_issue()` / `delete_issue()` / `unarchive_issue()` — cleanup operations
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

## Shared Types

All adapters use the shared types from `botfarm/bugtracker/types.py`:

- `Issue` — lightweight issue representation (id, identifier, title, priority, labels, estimate)
- `IssueDetails` — full issue details including description, assignee, git branch name
- `PollResult` — result of polling (issues found, active count info)
- `ActiveIssuesCount` — issue count and limit for capacity monitoring
- `CreatedIssue` — result of creating a new issue
- `Comment` — issue comment with author info
