"""Configuration loading and validation for botfarm."""

from __future__ import annotations

import logging
import os
import re
import tempfile
from dataclasses import dataclass, field, replace
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_DIR = Path.home() / ".botfarm"
DEFAULT_CONFIG_PATH = DEFAULT_CONFIG_DIR / "config.yaml"

# Note: _generate_config_yaml() in cli.py has a parallel template for interactive init.
# Keep both in sync when adding/removing fields.
DEFAULT_CONFIG_TEMPLATE = """\
# Botfarm configuration
# See documentation for full reference.

# Projects are configured per-repo via 'botfarm add-project'.
# projects: []

bugtracker:
  type: linear  # "linear" (future: "jira", "github")
  api_key: ${LINEAR_API_KEY}
  workspace: my-workspace
  poll_interval_seconds: 30
  exclude_tags:
    - Human
  issue_limit: 250  # Free plan default, override for paid plans
  # Workflow status names (must match your bugtracker's workflow)
  todo_status: Todo
  in_progress_status: In Progress
  done_status: Done
  in_review_status: In Review
  # Comment posting controls
  comment_on_failure: true
  comment_on_completion: false
  comment_on_limit_pause: false
  capacity_monitoring:
    enabled: true
    warning_threshold: 0.70   # Yellow in dashboard
    critical_threshold: 0.85  # Red in dashboard + webhook
    pause_threshold: 0.95     # Auto-pause dispatch
    resume_threshold: 0.90    # Resume after capacity freed (hysteresis)
database:
  # path is ignored — set BOTFARM_DB_PATH in your .env file instead
  path: ""

usage_limits:
  enabled: true
  poll_interval_seconds: 600
  pause_five_hour_threshold: 0.85
  pause_seven_day_threshold: 0.90

dashboard:
  enabled: false
  host: 0.0.0.0
  port: 8420
  terminal_enabled: false  # set to true to enable web terminal

agents:
  max_review_iterations: 3
  max_ci_retries: 2
  pr_checks_timeout_seconds: 600  # max seconds to wait for CI checks
  timeout_minutes:
    implement: 120
    review: 30
    fix: 60
  # timeout_overrides (first matching label wins; order matters):
  #   Investigation:
  #     implement: 30
  timeout_grace_seconds: 10
  adapters:
    claude:
      enabled: true
    codex:
      enabled: false
      model: ""                    # e.g. "o3", "o4-mini", or empty for default
      timeout_minutes: 15          # separate from Claude review timeout
      reasoning_effort: "medium"   # none, low, medium, high, xhigh — or empty for default
      skip_on_reiteration: true    # skip codex on review iterations 2+

# Separate coder/reviewer GitHub identities.
# Without this, all PRs and reviews use your personal GitHub account.
# With separate accounts, reviews appear as genuine third-party feedback.
# See docs/configuration.md for full setup instructions.
# identities:
#   coder:
#     github_token: ${CODER_GITHUB_TOKEN}          # Fine-grained PAT with repo write access
#     ssh_key_path: ~/.botfarm/coder_id_ed25519     # SSH key added to coder's GitHub account
#     git_author_name: "Coder Bot"
#     git_author_email: "coder-bot@example.com"
#   reviewer:
#     github_token: ${REVIEWER_GITHUB_TOKEN}        # Fine-grained PAT with PR read/write access

# Periodic refactoring analysis — auto-creates investigation tickets
# on a configurable cadence. Disabled by default.
# refactoring_analysis:
#   enabled: true
#   cadence_days: 14
#   cadence_tickets: 20  # Also trigger after N completed tickets (0 = disabled)
#   tracker_label: "Refactoring Analysis"
#   priority: 4  # Low priority — doesn't preempt feature work

# Start with dispatch paused — use the dashboard to begin dispatching.
# Set to false to dispatch immediately on startup.
start_paused: true

# notifications:
#   webhook_url: https://hooks.slack.com/services/...
#   webhook_format: slack  # or "discord"
#   rate_limit_seconds: 300

# Daily work summary — sends a Claude-generated digest of the last 24h.
# daily_summary:
#   enabled: true
#   send_hour: 18  # UTC hour (0-23)
#   min_tasks_for_summary: 0  # 0 = always send
#   webhook_url: ""  # Falls back to notifications.webhook_url
"""


SETUP_MODE_CONFIG_TEMPLATE = """\
# Botfarm configuration — created automatically.
# Complete setup via the dashboard or edit this file manually.
# See documentation for full reference.

# projects: []

bugtracker:
  type: linear
  api_key: ""
  workspace: ""
  poll_interval_seconds: 30

dashboard:
  enabled: true
  host: 127.0.0.1
  port: 8420

start_paused: true
"""


class ProjectConfig:
    """Project configuration.

    Fields: ``team``, ``tracker_project``, ``project_type``,
    ``setup_commands``, ``run_command``, ``run_env``, ``run_port``,
    ``qa_teardown_command``.
    """

    __slots__ = ("name", "base_dir", "worktree_prefix", "slots",
                 "team", "tracker_project", "project_type",
                 "setup_commands", "run_command", "run_env", "run_port",
                 "include_tags", "bugtracker", "qa_teardown_command")

    def __init__(
        self,
        name: str,
        base_dir: str = "",
        worktree_prefix: str = "",
        slots: list[int] | None = None,
        team: str = "",
        tracker_project: str = "",
        project_type: str = "",
        setup_commands: list[str] | None = None,
        run_command: str = "",
        run_env: dict[str, str] | None = None,
        run_port: int = 0,
        include_tags: list[str] | None = None,
        bugtracker: dict | None = None,
        qa_teardown_command: str = "",
    ) -> None:
        self.name = name
        self.base_dir = base_dir
        self.worktree_prefix = worktree_prefix
        self.slots = slots if slots is not None else []
        self.team = team
        self.tracker_project = tracker_project
        self.project_type = project_type
        self.setup_commands = setup_commands if setup_commands is not None else []
        self.run_command = run_command
        self.run_env = run_env if run_env is not None else {}
        self.run_port = run_port
        self.include_tags = include_tags
        self.bugtracker = bugtracker
        self.qa_teardown_command = qa_teardown_command

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProjectConfig):
            return NotImplemented
        return (self.name == other.name and self.base_dir == other.base_dir
                and self.worktree_prefix == other.worktree_prefix
                and self.slots == other.slots and self.team == other.team
                and self.tracker_project == other.tracker_project
                and self.project_type == other.project_type
                and self.setup_commands == other.setup_commands
                and self.run_command == other.run_command
                and self.run_env == other.run_env
                and self.run_port == other.run_port
                and self.include_tags == other.include_tags
                and self.bugtracker == other.bugtracker
                and self.qa_teardown_command == other.qa_teardown_command)

    def __repr__(self) -> str:
        return (f"ProjectConfig(name={self.name!r}, team={self.team!r}, "
                f"base_dir={self.base_dir!r}, worktree_prefix={self.worktree_prefix!r}, "
                f"slots={self.slots!r}, tracker_project={self.tracker_project!r}, "
                f"project_type={self.project_type!r}, "
                f"setup_commands={self.setup_commands!r}, "
                f"run_command={self.run_command!r}, "
                f"run_env={self.run_env!r}, run_port={self.run_port!r}, "
                f"include_tags={self.include_tags!r}, "
                f"bugtracker={self.bugtracker!r}, "
                f"qa_teardown_command={self.qa_teardown_command!r})")


@dataclass
class CapacityConfig:
    enabled: bool = True
    warning_threshold: float = 0.70
    critical_threshold: float = 0.85
    pause_threshold: float = 0.95
    resume_threshold: float = 0.90


@dataclass
class BugtrackerConfig:
    """Base bugtracker configuration shared across all tracker types."""

    type: str = "linear"  # "linear", "jira", future: "github"
    api_key: str = ""
    workspace: str = ""
    poll_interval_seconds: int = 30
    exclude_tags: list[str] = field(default_factory=lambda: ["Human"])
    include_tags: list[str] = field(default_factory=list)
    todo_status: str = "Todo"
    in_progress_status: str = "In Progress"
    done_status: str = "Done"
    in_review_status: str = "In Review"
    comment_on_failure: bool = True
    comment_on_completion: bool = False
    comment_on_limit_pause: bool = False


@dataclass
class LinearBugtrackerConfig(BugtrackerConfig):
    """Linear-specific bugtracker configuration."""

    issue_limit: int = 250
    capacity_monitoring: CapacityConfig = field(default_factory=CapacityConfig)


# Keep LinearConfig as an alias for backward compatibility in imports.
LinearConfig = LinearBugtrackerConfig


@dataclass
class JiraBugtrackerConfig(BugtrackerConfig):
    """Jira-specific bugtracker configuration."""

    type: str = "jira"
    url: str = ""  # Jira instance URL, e.g. https://mycompany.atlassian.net
    email: str = ""  # Email for Jira API authentication


@dataclass
class DatabaseConfig:
    path: str = ""


VALID_AUTH_MODES = ("oauth", "api_key", "long_lived_token")

# Environment variable that supplies the token for long_lived_token auth mode.
LONG_LIVED_TOKEN_ENV_VAR = "CLAUDE_LONG_LIVED_TOKEN"


@dataclass
class UsageLimitsConfig:
    enabled: bool = True
    poll_interval_seconds: int = 600
    pause_five_hour_threshold: float = 0.85
    pause_seven_day_threshold: float = 0.90


@dataclass
class DashboardConfig:
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8420
    terminal_enabled: bool = False


@dataclass
class AdapterConfig:
    """Per-agent-adapter configuration (e.g. claude, codex)."""

    enabled: bool = False
    model: str = ""
    timeout_minutes: int | None = None
    reasoning_effort: str = ""
    skip_on_reiteration: bool = True


CODEX_ADAPTER_DEFAULTS = AdapterConfig(timeout_minutes=15, reasoning_effort="medium")


def default_adapters() -> dict[str, AdapterConfig]:
    return {
        "claude": AdapterConfig(enabled=True),
        "codex": replace(CODEX_ADAPTER_DEFAULTS),
    }


@dataclass
class AgentsConfig:
    max_review_iterations: int = 3
    max_ci_retries: int = 2
    max_merge_conflict_retries: int = 2
    pr_checks_timeout_seconds: int = 600
    timeout_minutes: dict[str, int] = field(default_factory=lambda: {
        "implement": 120,
        "review": 30,
        "fix": 60,
    })
    timeout_overrides: dict[str, dict[str, int]] = field(default_factory=dict)
    timeout_grace_seconds: int = 10
    adapters: dict[str, AdapterConfig] = field(default_factory=default_adapters)

    # Legacy accessor properties for backward compatibility.
    @property
    def codex_reviewer_enabled(self) -> bool:
        return self.adapters.get("codex", CODEX_ADAPTER_DEFAULTS).enabled

    @property
    def codex_reviewer_model(self) -> str:
        return self.adapters.get("codex", CODEX_ADAPTER_DEFAULTS).model

    @property
    def codex_reviewer_reasoning_effort(self) -> str:
        return self.adapters.get("codex", CODEX_ADAPTER_DEFAULTS).reasoning_effort

    @property
    def codex_reviewer_timeout_minutes(self) -> int:
        ac = self.adapters.get("codex", CODEX_ADAPTER_DEFAULTS)
        return ac.timeout_minutes if ac.timeout_minutes is not None else CODEX_ADAPTER_DEFAULTS.timeout_minutes

    @property
    def codex_reviewer_skip_on_reiteration(self) -> bool:
        return self.adapters.get("codex", CODEX_ADAPTER_DEFAULTS).skip_on_reiteration


@dataclass
class LoggingConfig:
    max_bytes: int = 10 * 1024 * 1024  # 10 MB per rotated file
    backup_count: int = 5  # number of rotated files to keep
    ticket_log_retention_days: int = 30  # delete ticket logs older than N days


@dataclass
class RefactoringAnalysisConfig:
    enabled: bool = False
    cadence_days: int = 14
    cadence_tickets: int = 20  # 0 = disabled
    tracker_label: str = "Refactoring Analysis"
    priority: int = 4  # Low priority


@dataclass
class NotificationsConfig:
    webhook_url: str = ""
    webhook_format: str = "slack"  # "slack" or "discord"
    rate_limit_seconds: int = 300
    human_blocker_cooldown_seconds: int = 3600  # 1 hour per blocker ticket


@dataclass
class CoderIdentity:
    github_token: str = ""
    ssh_key_path: str = ""
    git_author_name: str = ""
    git_author_email: str = ""
    tracker_api_key: str = ""
    jira_api_token: str = ""
    jira_email: str = ""


@dataclass
class ReviewerIdentity:
    github_token: str = ""
    tracker_api_key: str = ""


@dataclass
class IdentitiesConfig:
    coder: CoderIdentity = field(default_factory=CoderIdentity)
    reviewer: ReviewerIdentity = field(default_factory=ReviewerIdentity)


@dataclass
class CodexUsageConfig:
    enabled: bool = False
    poll_interval_seconds: int = 300
    pause_primary_threshold: float = 0.85
    pause_secondary_threshold: float = 0.90


@dataclass
class DailySummaryConfig:
    enabled: bool = False
    send_hour: int = 18  # UTC hour (0-23)
    min_tasks_for_summary: int = 0  # 0 = always send, even on quiet days
    webhook_url: str = ""  # Falls back to main notifications.webhook_url


class BotfarmConfig:
    """Top-level botfarm configuration.

    The ``bugtracker`` field holds the bugtracker configuration.
    """

    def __init__(
        self,
        projects: list[ProjectConfig],
        bugtracker: BugtrackerConfig | None = None,
        database: DatabaseConfig | None = None,
        usage_limits: UsageLimitsConfig | None = None,
        dashboard: DashboardConfig | None = None,
        agents: AgentsConfig | None = None,
        logging: LoggingConfig | None = None,
        notifications: NotificationsConfig | None = None,
        identities: IdentitiesConfig | None = None,
        refactoring_analysis: RefactoringAnalysisConfig | None = None,
        codex_usage: CodexUsageConfig | None = None,
        daily_summary: DailySummaryConfig | None = None,
        start_paused: bool = True,
        source_path: str = "",
        auth_mode: str = "oauth",
    ) -> None:
        self.projects = projects
        self.bugtracker = bugtracker or LinearBugtrackerConfig()
        self.database = database or DatabaseConfig()
        self.usage_limits = usage_limits or UsageLimitsConfig()
        self.dashboard = dashboard or DashboardConfig()
        self.agents = agents or AgentsConfig()
        self.logging = logging or LoggingConfig()
        self.notifications = notifications or NotificationsConfig()
        self.identities = identities or IdentitiesConfig()
        self.refactoring_analysis = refactoring_analysis or RefactoringAnalysisConfig()
        self.codex_usage = codex_usage or CodexUsageConfig()
        self.daily_summary = daily_summary or DailySummaryConfig()
        self.start_paused = start_paused
        self.source_path = source_path
        self.auth_mode = auth_mode

    @property
    def setup_mode(self) -> bool:
        """True when config is incomplete — supervisor runs dashboard only."""
        if not self.projects:
            return True
        if self.bugtracker.api_key:
            return False
        # No global API key — check if every project has its own
        return not all(
            p.bugtracker and p.bugtracker.get("api_key")
            for p in self.projects
        )


class ConfigError(Exception):
    """Raised when configuration is invalid."""


def expand_env_vars(value: str) -> str:
    """Expand ${VAR} references in a string using environment variables.

    Only the braced syntax ${VAR} is supported to avoid false positives
    with bare dollar signs in config values.

    Raises ConfigError if a referenced variable is not set.
    """

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        env_value = os.environ.get(var_name)
        if env_value is None:
            raise ConfigError(
                f"Environment variable '{var_name}' is not set"
            )
        return env_value

    return re.sub(r"\$\{(\w+)\}", _replace, value)


def _expand_env_recursive(obj: object) -> object:
    """Walk a nested dict/list structure and expand env vars in all strings."""
    if isinstance(obj, str):
        return expand_env_vars(obj)
    if isinstance(obj, dict):
        return {k: _expand_env_recursive(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_recursive(item) for item in obj]
    return obj


def create_default_config(config_path: Path = DEFAULT_CONFIG_PATH) -> Path:
    """Create a default config file if it doesn't exist. Returns the path."""
    config_path.parent.mkdir(parents=True, exist_ok=True)
    if not config_path.exists():
        config_path.write_text(DEFAULT_CONFIG_TEMPLATE)
    return config_path


# Default run_command / run_port derived from project_type when not explicitly set.
_PROJECT_TYPE_RUN_DEFAULTS: dict[str, dict[str, object]] = {
    "nextjs": {"run_command": "npm run dev", "run_port": 3000},
    "react": {"run_command": "npm start", "run_port": 3000},
    "vue": {"run_command": "npm run dev", "run_port": 5173},
    "nuxt": {"run_command": "npm run dev", "run_port": 3000},
    "django": {"run_command": "python manage.py runserver 0.0.0.0:8000", "run_port": 8000},
    "flask": {"run_command": "flask run --host=0.0.0.0", "run_port": 5000},
    "fastapi": {"run_command": "uvicorn main:app --host 0.0.0.0 --port 8000", "run_port": 8000},
    "rails": {"run_command": "bin/rails server -b 0.0.0.0", "run_port": 3000},
}


_ALLOWED_BT_KEYS = {
    "type", "api_key", "workspace", "url", "email",
    "exclude_tags", "include_tags",
    "todo_status", "in_progress_status", "done_status", "in_review_status",
    "comment_on_failure", "comment_on_completion", "comment_on_limit_pause",
    "poll_interval_seconds",
    # Linear-specific
    "issue_limit", "capacity_monitoring",
}

_SUPPORTED_BT_TYPES = {"linear", "jira"}


def _validate_bugtracker_override(
    bt_data: object, project_name: str,
) -> list[str]:
    """Validate a per-project bugtracker override dict.

    Returns a list of error strings (empty if valid).
    """
    errors: list[str] = []
    if not isinstance(bt_data, dict):
        errors.append(
            f"Project '{project_name}': bugtracker must be a mapping"
        )
        return errors
    unknown = set(bt_data.keys()) - _ALLOWED_BT_KEYS
    if unknown:
        errors.append(
            f"Project '{project_name}': unknown bugtracker fields: {sorted(unknown)}"
        )
    bt_type = bt_data.get("type")
    if bt_type is not None and bt_type not in _SUPPORTED_BT_TYPES:
        errors.append(
            f"Project '{project_name}': unsupported bugtracker type: {bt_type!r}. "
            f"Supported: {sorted(_SUPPORTED_BT_TYPES)}"
        )
    return errors


def _parse_project(data: dict) -> ProjectConfig:
    # Accept both old (linear_team) and new (team) field names.
    team = data.get("team") or data.get("linear_team", "")
    tracker_project = data.get("tracker_project") or data.get("linear_project", "")

    required_base = {"name", "base_dir", "worktree_prefix", "slots"}
    missing = required_base - set(data.keys())
    # team must be present via either name
    if not team:
        missing.add("team")
    if missing:
        raise ConfigError(
            f"Project is missing required fields: {sorted(missing)}"
        )
    slots = data["slots"]
    if not isinstance(slots, list) or not all(isinstance(s, int) for s in slots):
        raise ConfigError(
            f"Project '{data['name']}': slots must be a list of integers"
        )
    if len(slots) != len(set(slots)):
        raise ConfigError(
            f"Project '{data['name']}': slots contains duplicate values"
        )
    project_type = data.get("project_type", "")
    if not isinstance(project_type, str):
        raise ConfigError(
            f"Project '{data['name']}': project_type must be a string"
        )
    setup_commands = data.get("setup_commands")
    if setup_commands is not None:
        if (not isinstance(setup_commands, list)
                or not all(isinstance(c, str) for c in setup_commands)):
            raise ConfigError(
                f"Project '{data['name']}': setup_commands must be a list of strings"
            )

    # run_command
    run_command = data.get("run_command", "")
    if not isinstance(run_command, str):
        raise ConfigError(
            f"Project '{data['name']}': run_command must be a string"
        )

    # run_env
    run_env = data.get("run_env")
    if run_env is not None:
        if not isinstance(run_env, dict) or not all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in run_env.items()
        ):
            raise ConfigError(
                f"Project '{data['name']}': run_env must be a mapping of strings to strings"
            )

    # run_port
    run_port_raw = data.get("run_port", 0)
    if isinstance(run_port_raw, bool) or not isinstance(run_port_raw, int):
        raise ConfigError(
            f"Project '{data['name']}': run_port must be an integer"
        )
    run_port = run_port_raw
    if run_port < 0:
        raise ConfigError(
            f"Project '{data['name']}': run_port must be a non-negative integer"
        )

    # include_tags
    include_tags = data.get("include_tags")
    if include_tags is not None:
        if (not isinstance(include_tags, list)
                or not all(isinstance(t, str) for t in include_tags)):
            raise ConfigError(
                f"Project '{data['name']}': include_tags must be a list of strings"
            )

    # qa_teardown_command
    qa_teardown_command = data.get("qa_teardown_command", "")
    if not isinstance(qa_teardown_command, str):
        raise ConfigError(
            f"Project '{data['name']}': qa_teardown_command must be a string"
        )

    # Per-project bugtracker overrides
    bugtracker_data = data.get("bugtracker")
    if bugtracker_data is not None:
        errors = _validate_bugtracker_override(bugtracker_data, data["name"])
        if errors:
            raise ConfigError(errors[0])

    # Derive defaults from project_type when not explicitly set.
    if project_type and project_type in _PROJECT_TYPE_RUN_DEFAULTS:
        defaults = _PROJECT_TYPE_RUN_DEFAULTS[project_type]
        if "run_command" not in data:
            run_command = str(defaults.get("run_command", ""))
        if "run_port" not in data and defaults.get("run_port"):
            run_port = int(defaults["run_port"])

    return ProjectConfig(
        name=data["name"],
        team=str(team),
        base_dir=data["base_dir"],
        worktree_prefix=data["worktree_prefix"],
        slots=slots,
        tracker_project=str(tracker_project),
        project_type=project_type,
        setup_commands=setup_commands,
        run_command=run_command,
        run_env=run_env,
        run_port=run_port,
        include_tags=include_tags,
        bugtracker=bugtracker_data,
        qa_teardown_command=qa_teardown_command,
    )


def resolve_project_bugtracker(
    global_bt: BugtrackerConfig,
    project: ProjectConfig,
) -> BugtrackerConfig:
    """Return the effective bugtracker config for a project.

    If the project has a ``bugtracker`` dict, its values override the global
    config.  Unset fields fall back to global defaults.  When the project
    specifies a different tracker type than the global config, a new config
    instance of the appropriate subclass is created.

    Returns the global config unchanged when the project has no overrides.
    """
    bt_data = project.bugtracker
    if not bt_data:
        return global_bt

    bt_type = str(bt_data.get("type", global_bt.type))

    # Build common kwargs, falling back to global for each field.
    common = dict(
        type=bt_type,
        api_key=bt_data.get("api_key", global_bt.api_key),
        workspace=bt_data.get("workspace", global_bt.workspace),
        poll_interval_seconds=bt_data.get("poll_interval_seconds", global_bt.poll_interval_seconds),
        exclude_tags=bt_data.get("exclude_tags", global_bt.exclude_tags),
        include_tags=bt_data.get("include_tags", global_bt.include_tags),
        todo_status=str(bt_data.get("todo_status", global_bt.todo_status)),
        in_progress_status=str(bt_data.get("in_progress_status", global_bt.in_progress_status)),
        done_status=str(bt_data.get("done_status", global_bt.done_status)),
        in_review_status=str(bt_data.get("in_review_status", global_bt.in_review_status)),
        comment_on_failure=bt_data.get("comment_on_failure", global_bt.comment_on_failure),
        comment_on_completion=bt_data.get("comment_on_completion", global_bt.comment_on_completion),
        comment_on_limit_pause=bt_data.get("comment_on_limit_pause", global_bt.comment_on_limit_pause),
    )

    if bt_type == "jira":
        global_url = getattr(global_bt, "url", "")
        global_email = getattr(global_bt, "email", "")
        return JiraBugtrackerConfig(
            **common,
            url=str(bt_data.get("url", global_url)),
            email=str(bt_data.get("email", global_email)),
        )

    # Default to Linear
    global_issue_limit = getattr(global_bt, "issue_limit", 250)
    global_capacity = getattr(global_bt, "capacity_monitoring", CapacityConfig())
    cap_data = bt_data.get("capacity_monitoring")
    if isinstance(cap_data, dict):
        capacity = CapacityConfig(
            enabled=cap_data.get("enabled", global_capacity.enabled),
            warning_threshold=float(cap_data.get("warning_threshold", global_capacity.warning_threshold)),
            critical_threshold=float(cap_data.get("critical_threshold", global_capacity.critical_threshold)),
            pause_threshold=float(cap_data.get("pause_threshold", global_capacity.pause_threshold)),
            resume_threshold=float(cap_data.get("resume_threshold", global_capacity.resume_threshold)),
        )
    else:
        capacity = global_capacity
    return LinearBugtrackerConfig(
        **common,
        issue_limit=int(bt_data.get("issue_limit", global_issue_limit)),
        capacity_monitoring=capacity,
    )


_KNOWN_TIMEOUT_STAGES = {"implement", "review", "fix", "pr_checks", "ci_fix", "merge", "resolve_conflict", "qa"}


def resolve_stage_timeout(
    agents_cfg: AgentsConfig,
    stage: str,
    ticket_labels: list[str] | None = None,
) -> int | None:
    """Return the effective timeout in minutes for a stage.

    Checks ``timeout_overrides`` for any matching label (case-insensitive,
    first match wins), falling back to ``timeout_minutes``.  Returns
    ``None`` if the stage has no configured timeout.
    """
    if ticket_labels:
        labels_lower = {lbl.lower() for lbl in ticket_labels}
        for label, overrides in agents_cfg.timeout_overrides.items():
            if label.lower() in labels_lower and stage in overrides:
                return overrides[stage]
    return agents_cfg.timeout_minutes.get(stage)


def _validate_config(config: BotfarmConfig) -> None:
    """Validate cross-field constraints."""
    bt = config.bugtracker

    supported_types = {"linear", "jira"}
    if bt.type not in supported_types:
        raise ConfigError(
            f"Unsupported bugtracker type: {bt.type!r}. "
            f"Supported: {sorted(supported_types)}"
        )

    # In setup mode (no projects or no API key), skip strict checks
    # so the supervisor can start with dashboard only.
    if not config.setup_mode:
        # Global API key can be omitted when every project provides its own
        has_all_project_keys = all(
            p.bugtracker and p.bugtracker.get("api_key")
            for p in config.projects
        )
        if not bt.api_key and not has_all_project_keys:
            raise ConfigError("bugtracker.api_key must be set")

        if not config.projects:
            raise ConfigError("At least one project must be configured")

        names = [p.name for p in config.projects]
        if len(names) != len(set(names)):
            raise ConfigError("Duplicate project names found")

        # Check for duplicate tracker_project filters (ignoring empty/unset)
        seen_lp: dict[str, str] = {}  # tracker_project -> project name
        for p in config.projects:
            if p.tracker_project:
                if p.tracker_project in seen_lp:
                    raise ConfigError(
                        f"Duplicate tracker_project filter {p.tracker_project!r}: "
                        f"used by both '{seen_lp[p.tracker_project]}' and '{p.name}'. "
                        f"Each project must have a unique tracker_project filter"
                    )
                seen_lp[p.tracker_project] = p.name

    if bt.poll_interval_seconds < 1:
        raise ConfigError("poll_interval_seconds must be at least 1")

    if config.usage_limits.poll_interval_seconds < 1:
        raise ConfigError("usage_limits.poll_interval_seconds must be at least 1")

    for attr in ("pause_five_hour_threshold", "pause_seven_day_threshold"):
        val = getattr(config.usage_limits, attr)
        if not (0.0 <= val <= 1.0):
            raise ConfigError(f"usage_limits.{attr} must be between 0.0 and 1.0")

    if isinstance(bt, LinearBugtrackerConfig):
        if bt.issue_limit < 1:
            raise ConfigError("bugtracker.issue_limit must be at least 1")

        cap = bt.capacity_monitoring
        for attr in ("warning_threshold", "critical_threshold", "pause_threshold", "resume_threshold"):
            val = getattr(cap, attr)
            if not (0.0 <= val <= 1.0):
                raise ConfigError(
                    f"bugtracker.capacity_monitoring.{attr} must be between 0.0 and 1.0"
                )
        if cap.warning_threshold > cap.critical_threshold:
            raise ConfigError(
                "bugtracker.capacity_monitoring.warning_threshold must be "
                "less than or equal to critical_threshold"
            )
        if cap.critical_threshold > cap.pause_threshold:
            raise ConfigError(
                "bugtracker.capacity_monitoring.critical_threshold must be "
                "less than or equal to pause_threshold"
            )
        if cap.resume_threshold >= cap.pause_threshold:
            raise ConfigError(
                "bugtracker.capacity_monitoring.resume_threshold must be less than pause_threshold"
            )

    if isinstance(bt, JiraBugtrackerConfig) and not config.setup_mode:
        if not bt.url:
            raise ConfigError("bugtracker.url must be set for Jira")
        if not bt.email:
            raise ConfigError("bugtracker.email must be set for Jira")

    # Validate per-project bugtracker overrides
    if not config.setup_mode:
        for p in config.projects:
            if not p.bugtracker:
                continue
            resolved = resolve_project_bugtracker(bt, p)
            if not resolved.api_key:
                raise ConfigError(
                    f"Project '{p.name}': bugtracker.api_key must be set "
                    f"(either in project or global bugtracker config)"
                )
            if isinstance(resolved, JiraBugtrackerConfig):
                if not resolved.url:
                    raise ConfigError(
                        f"Project '{p.name}': bugtracker.url must be set for Jira"
                    )
                if not resolved.email:
                    raise ConfigError(
                        f"Project '{p.name}': bugtracker.email must be set for Jira"
                    )

    if config.agents.max_review_iterations < 1:
        raise ConfigError("agents.max_review_iterations must be at least 1")

    if config.agents.max_ci_retries < 0:
        raise ConfigError("agents.max_ci_retries must be at least 0")

    if config.agents.max_merge_conflict_retries < 0:
        raise ConfigError("agents.max_merge_conflict_retries must be at least 0")

    if config.agents.pr_checks_timeout_seconds < 1:
        raise ConfigError("agents.pr_checks_timeout_seconds must be at least 1")

    for stage, minutes in config.agents.timeout_minutes.items():
        if stage not in _KNOWN_TIMEOUT_STAGES:
            raise ConfigError(
                f"agents.timeout_minutes: unknown stage '{stage}'. "
                f"Valid stages: {sorted(_KNOWN_TIMEOUT_STAGES)}"
            )
        if minutes < 1:
            raise ConfigError(
                f"agents.timeout_minutes.{stage} must be at least 1"
            )

    for label, overrides in config.agents.timeout_overrides.items():
        if not isinstance(label, str) or not label:
            raise ConfigError(
                "agents.timeout_overrides: keys must be non-empty strings"
            )
        if not isinstance(overrides, dict):
            raise ConfigError(
                f"agents.timeout_overrides.{label}: must be a mapping of stage → minutes"
            )
        for stage, minutes in overrides.items():
            if stage not in _KNOWN_TIMEOUT_STAGES:
                raise ConfigError(
                    f"agents.timeout_overrides.{label}: unknown stage '{stage}'. "
                    f"Valid stages: {sorted(_KNOWN_TIMEOUT_STAGES)}"
                )
            if not isinstance(minutes, int) or isinstance(minutes, bool):
                raise ConfigError(
                    f"agents.timeout_overrides.{label}.{stage} must be an integer"
                )
            if minutes < 1:
                raise ConfigError(
                    f"agents.timeout_overrides.{label}.{stage} must be at least 1"
                )

    if config.agents.timeout_grace_seconds < 0:
        raise ConfigError("agents.timeout_grace_seconds must be at least 0")

    for adapter_name, adapter_cfg in config.agents.adapters.items():
        if adapter_cfg.timeout_minutes is not None and adapter_cfg.timeout_minutes < 1:
            raise ConfigError(
                f"agents.adapters.{adapter_name}.timeout_minutes must be at least 1"
            )

    if config.logging.max_bytes < 1:
        raise ConfigError("logging.max_bytes must be at least 1")

    if config.logging.backup_count < 0:
        raise ConfigError("logging.backup_count must be at least 0")

    if config.logging.ticket_log_retention_days < 1:
        raise ConfigError("logging.ticket_log_retention_days must be at least 1")

    ra = config.refactoring_analysis
    if not ra.tracker_label:
        raise ConfigError("refactoring_analysis.tracker_label must not be empty")
    if ra.cadence_days < 1:
        raise ConfigError("refactoring_analysis.cadence_days must be at least 1")
    if ra.cadence_tickets < 0:
        raise ConfigError("refactoring_analysis.cadence_tickets must be at least 0")
    if ra.priority not in (0, 1, 2, 3, 4):
        raise ConfigError(
            "refactoring_analysis.priority must be 0-4 "
            "(0=None, 1=Urgent, 2=High, 3=Normal, 4=Low)"
        )

    ds = config.daily_summary
    if not (0 <= ds.send_hour <= 23):
        raise ConfigError("daily_summary.send_hour must be between 0 and 23")
    if ds.min_tasks_for_summary < 0:
        raise ConfigError("daily_summary.min_tasks_for_summary must be at least 0")

    cu = config.codex_usage
    if cu.poll_interval_seconds < 1:
        raise ConfigError("codex_usage.poll_interval_seconds must be at least 1")
    if not (0.0 <= cu.pause_primary_threshold <= 1.0):
        raise ConfigError(
            "codex_usage.pause_primary_threshold must be between 0.0 and 1.0"
        )
    if not (0.0 <= cu.pause_secondary_threshold <= 1.0):
        raise ConfigError(
            "codex_usage.pause_secondary_threshold must be between 0.0 and 1.0"
        )

    if config.auth_mode not in VALID_AUTH_MODES:
        raise ConfigError(
            f"auth_mode must be one of {sorted(VALID_AUTH_MODES)}, "
            f"got: {config.auth_mode!r}"
        )

    if config.auth_mode == "long_lived_token":
        if not os.environ.get(LONG_LIVED_TOKEN_ENV_VAR):
            raise ConfigError(
                f"auth_mode is 'long_lived_token' but {LONG_LIVED_TOKEN_ENV_VAR} "
                f"environment variable is not set. Set it in your .env file or "
                f"environment before starting the supervisor."
            )

    if config.notifications.webhook_format not in ("slack", "discord"):
        raise ConfigError(
            f"notifications.webhook_format must be 'slack' or 'discord', "
            f"got: {config.notifications.webhook_format!r}"
        )

    if config.notifications.rate_limit_seconds < 0:
        raise ConfigError("notifications.rate_limit_seconds must be at least 0")


def _parse_bool(data: dict, key: str, default: bool, *, section: str = "bugtracker") -> bool:
    """Parse a boolean config value, rejecting non-boolean types."""
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(
            f"{section}.{key} must be a boolean (true/false), got: {value!r}"
        )
    return value


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> BotfarmConfig:
    """Load, expand, validate, and return the botfarm configuration."""
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    raw = config_path.read_text()
    try:
        data = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        msg = f"YAML syntax error in {config_path}"
        if hasattr(exc, "problem_mark") and exc.problem_mark is not None:
            mark = exc.problem_mark
            msg += f" at line {mark.line + 1}, column {mark.column + 1}"
        if hasattr(exc, "problem") and exc.problem:
            msg += f": {exc.problem}"
        raise ConfigError(msg) from exc

    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    data = _expand_env_recursive(data)

    known_keys = {
        "projects", "bugtracker", "linear", "database", "usage_limits",
        "dashboard", "agents", "logging", "notifications",
        "identities", "refactoring_analysis", "codex_usage",
        "daily_summary", "start_paused", "auth_mode",
    }
    unknown = set(data.keys()) - known_keys
    if unknown:
        logger.warning(
            "Unknown config keys (ignored): %s", ", ".join(sorted(unknown))
        )

    raw_projects = data.get("projects")
    if raw_projects is not None and not isinstance(raw_projects, list):
        raise ConfigError("'projects' must be a list")

    projects = [_parse_project(p) for p in (raw_projects or [])]

    # Support both 'bugtracker:' (new) and 'linear:' (legacy) YAML sections.
    # 'bugtracker:' takes precedence if both are present.
    if "bugtracker" in data:
        bt_data = data["bugtracker"]
        if not isinstance(bt_data, dict):
            bt_data = {}
    else:
        bt_data = data.get("linear", {})
        if not isinstance(bt_data, dict):
            bt_data = {}
        # Legacy linear: section implies type=linear
        bt_data.setdefault("type", "linear")

    bt_section = "bugtracker" if "bugtracker" in data else "linear"

    # Common bugtracker fields shared by all tracker types.
    bt_common = dict(
        type=str(bt_data.get("type", "linear")),
        api_key=bt_data.get("api_key", ""),
        workspace=bt_data.get("workspace", ""),
        poll_interval_seconds=bt_data.get("poll_interval_seconds", 30),
        exclude_tags=bt_data.get("exclude_tags", ["Human"]),
        include_tags=bt_data.get("include_tags", []),
        todo_status=str(bt_data.get("todo_status", "Todo")),
        in_progress_status=str(bt_data.get("in_progress_status", "In Progress")),
        done_status=str(bt_data.get("done_status", "Done")),
        in_review_status=str(bt_data.get("in_review_status", "In Review")),
        comment_on_failure=_parse_bool(bt_data, "comment_on_failure", True, section=bt_section),
        comment_on_completion=_parse_bool(bt_data, "comment_on_completion", False, section=bt_section),
        comment_on_limit_pause=_parse_bool(bt_data, "comment_on_limit_pause", False, section=bt_section),
    )

    bt_type = bt_common["type"]
    if bt_type == "jira":
        bugtracker: BugtrackerConfig = JiraBugtrackerConfig(
            **bt_common,
            url=str(bt_data.get("url", "")),
            email=str(bt_data.get("email", "")),
        )
    else:
        # Default to Linear (handles both explicit "linear" and legacy configs).
        cap_data = bt_data.get("capacity_monitoring", {})
        if not isinstance(cap_data, dict):
            cap_data = {}
        capacity_monitoring = CapacityConfig(
            enabled=_parse_bool(cap_data, "enabled", True, section=f"{bt_section}.capacity_monitoring"),
            warning_threshold=float(cap_data.get("warning_threshold", 0.70)),
            critical_threshold=float(cap_data.get("critical_threshold", 0.85)),
            pause_threshold=float(cap_data.get("pause_threshold", 0.95)),
            resume_threshold=float(cap_data.get("resume_threshold", 0.90)),
        )
        bugtracker = LinearBugtrackerConfig(
            **bt_common,
            issue_limit=int(bt_data.get("issue_limit", 250)),
            capacity_monitoring=capacity_monitoring,
        )

    db_data = data.get("database", {})
    database = DatabaseConfig(
        path=db_data.get("path", ""),
    )

    ul_data = data.get("usage_limits", {})
    usage_limits = UsageLimitsConfig(
        enabled=_parse_bool(ul_data, "enabled", True, section="usage_limits"),
        poll_interval_seconds=int(ul_data.get("poll_interval_seconds", 600)),
        pause_five_hour_threshold=float(ul_data.get("pause_five_hour_threshold", 0.85)),
        pause_seven_day_threshold=float(ul_data.get("pause_seven_day_threshold", 0.90)),
    )

    dash_data = data.get("dashboard", {})
    dashboard = DashboardConfig(
        enabled=bool(dash_data.get("enabled", False)),
        host=str(dash_data.get("host", "0.0.0.0")),
        port=int(dash_data.get("port", 8420)),
        terminal_enabled=bool(dash_data.get("terminal_enabled", False)),
    )

    agents_data = data.get("agents", {})
    timeout_defaults = {"implement": 120, "review": 30, "fix": 60, "resolve_conflict": 60}
    raw_timeouts = agents_data.get("timeout_minutes", {})
    timeout_minutes = {**timeout_defaults, **{k: int(v) for k, v in raw_timeouts.items()}}
    raw_overrides = agents_data.get("timeout_overrides", {})
    timeout_overrides: dict[str, dict[str, int]] = {}
    if isinstance(raw_overrides, dict):
        for label, stages in raw_overrides.items():
            if isinstance(stages, dict):
                try:
                    timeout_overrides[str(label)] = {
                        k: int(v) for k, v in stages.items()
                    }
                except (ValueError, TypeError) as exc:
                    raise ConfigError(f"agents.timeout_overrides.{label}: {exc}")
            else:
                # Pass through for _validate_config() to report the error
                timeout_overrides[str(label)] = stages
    elif raw_overrides:
        raise ConfigError(
            "agents.timeout_overrides must be a mapping of label names to "
            f"stage overrides, got {type(raw_overrides).__name__}"
        )

    # Parse adapter configs: support both new "adapters:" format and
    # legacy "codex_reviewer_*" flat fields (with deprecation warning).
    adapters = default_adapters()
    raw_adapters = agents_data.get("adapters")
    if isinstance(raw_adapters, dict):
        for name, adapter_data in raw_adapters.items():
            if not isinstance(adapter_data, dict):
                raise ConfigError(
                    f"agents.adapters.{name} must be a mapping, "
                    f"got {type(adapter_data).__name__}"
                )
            adapters[name] = AdapterConfig(
                enabled=_parse_bool(adapter_data, "enabled", name == "claude", section=f"agents.adapters.{name}"),
                model=str(adapter_data.get("model", "")),
                timeout_minutes=int(adapter_data["timeout_minutes"]) if "timeout_minutes" in adapter_data else None,
                reasoning_effort=str(adapter_data.get("reasoning_effort", "") or ""),
                skip_on_reiteration=_parse_bool(
                    adapter_data, "skip_on_reiteration", True,
                    section=f"agents.adapters.{name}",
                ),
            )
    elif raw_adapters is not None:
        raise ConfigError(
            f"agents.adapters must be a mapping, got {type(raw_adapters).__name__}"
        )

    # Legacy codex_reviewer_* fields → migrate to adapters["codex"]
    _LEGACY_CODEX_KEYS = {
        "codex_reviewer_enabled", "codex_reviewer_model",
        "codex_reviewer_reasoning_effort", "codex_reviewer_timeout_minutes",
        "codex_reviewer_skip_on_reiteration",
    }
    has_legacy = any(k in agents_data for k in _LEGACY_CODEX_KEYS)
    if has_legacy:
        if raw_adapters and "codex" in raw_adapters:
            logger.warning(
                "Both agents.adapters.codex and legacy codex_reviewer_* "
                "fields are present; adapters.codex takes precedence"
            )
        else:
            logger.warning(
                "Deprecated: codex_reviewer_* fields in agents config; "
                "migrate to agents.adapters.codex (see docs/configuration.md)"
            )
            _codex_defaults = default_adapters()["codex"]
            adapters["codex"] = AdapterConfig(
                enabled=_parse_bool(agents_data, "codex_reviewer_enabled", _codex_defaults.enabled, section="agents"),
                model=str(agents_data.get("codex_reviewer_model", _codex_defaults.model)),
                timeout_minutes=int(agents_data.get("codex_reviewer_timeout_minutes", _codex_defaults.timeout_minutes)),
                reasoning_effort=str(agents_data.get("codex_reviewer_reasoning_effort", _codex_defaults.reasoning_effort) or ""),
                skip_on_reiteration=_parse_bool(
                    agents_data, "codex_reviewer_skip_on_reiteration", True, section="agents",
                ),
            )

    agents = AgentsConfig(
        max_review_iterations=int(agents_data.get("max_review_iterations", 3)),
        max_ci_retries=int(agents_data.get("max_ci_retries", 2)),
        max_merge_conflict_retries=int(agents_data.get("max_merge_conflict_retries", 2)),
        pr_checks_timeout_seconds=int(agents_data.get("pr_checks_timeout_seconds", 600)),
        timeout_minutes=timeout_minutes,
        timeout_overrides=timeout_overrides,
        timeout_grace_seconds=int(agents_data.get("timeout_grace_seconds", 10)),
        adapters=adapters,
    )

    log_data = data.get("logging", {})
    logging_cfg = LoggingConfig(
        max_bytes=int(log_data.get("max_bytes", 10 * 1024 * 1024)),
        backup_count=int(log_data.get("backup_count", 5)),
        ticket_log_retention_days=int(
            log_data.get("ticket_log_retention_days", 30)
        ),
    )

    notif_data = data.get("notifications", {})
    notifications = NotificationsConfig(
        webhook_url=str(notif_data.get("webhook_url", "")),
        webhook_format=str(notif_data.get("webhook_format", "slack")),
        rate_limit_seconds=int(notif_data.get("rate_limit_seconds", 300)),
        human_blocker_cooldown_seconds=int(notif_data.get("human_blocker_cooldown_seconds", 3600)),
    )

    ident_data = data.get("identities", {})
    if ident_data and not isinstance(ident_data, dict):
        raise ConfigError("'identities' must be a mapping")
    coder_data = ident_data.get("coder", {}) if isinstance(ident_data, dict) else {}
    reviewer_data = ident_data.get("reviewer", {}) if isinstance(ident_data, dict) else {}
    identities = IdentitiesConfig(
        coder=CoderIdentity(
            github_token=str(coder_data.get("github_token", "")),
            ssh_key_path=str(coder_data.get("ssh_key_path", "")),
            git_author_name=str(coder_data.get("git_author_name", "")),
            git_author_email=str(coder_data.get("git_author_email", "")),
            tracker_api_key=str(
                coder_data.get("tracker_api_key", coder_data.get("linear_api_key", ""))
            ),
            jira_api_token=str(coder_data.get("jira_api_token", "")),
            jira_email=str(coder_data.get("jira_email", "")),
        ),
        reviewer=ReviewerIdentity(
            github_token=str(reviewer_data.get("github_token", "")),
            tracker_api_key=str(
                reviewer_data.get("tracker_api_key", reviewer_data.get("linear_api_key", ""))
            ),
        ),
    )

    ra_data = data.get("refactoring_analysis", {})
    if not isinstance(ra_data, dict):
        ra_data = {}
    refactoring_analysis = RefactoringAnalysisConfig(
        enabled=_parse_bool(ra_data, "enabled", False, section="refactoring_analysis"),
        cadence_days=int(ra_data.get("cadence_days", 14)),
        cadence_tickets=int(ra_data.get("cadence_tickets", 20)),
        tracker_label=str(
            ra_data.get("tracker_label", ra_data.get("linear_label", "Refactoring Analysis"))
        ).strip(),
        priority=int(ra_data.get("priority", 4)),
    )

    ds_data = data.get("daily_summary", {})
    if not isinstance(ds_data, dict):
        ds_data = {}
    daily_summary = DailySummaryConfig(
        enabled=_parse_bool(ds_data, "enabled", False, section="daily_summary"),
        send_hour=int(ds_data.get("send_hour", 18)),
        min_tasks_for_summary=int(ds_data.get("min_tasks_for_summary", 0)),
        webhook_url=str(ds_data.get("webhook_url", "")),
    )

    cu_data = data.get("codex_usage", {})
    if not isinstance(cu_data, dict):
        cu_data = {}
    codex_usage = CodexUsageConfig(
        enabled=_parse_bool(cu_data, "enabled", False, section="codex_usage"),
        poll_interval_seconds=int(cu_data.get("poll_interval_seconds", 300)),
        pause_primary_threshold=float(cu_data.get("pause_primary_threshold", 0.85)),
        pause_secondary_threshold=float(cu_data.get("pause_secondary_threshold", 0.90)),
    )

    if "failed_status" in bt_data:
        logger.warning(
            "The 'bugtracker.failed_status' config key is deprecated and ignored — "
            "failed tickets now keep their current status and receive "
            "'Failed' + 'Human' labels instead."
        )

    if "state_file" in data:
        logger.warning(
            "The 'state_file' config key is deprecated and ignored — all "
            "state is now persisted in the SQLite database."
        )

    start_paused = _parse_bool(data, "start_paused", True, section="top-level")

    # Auth mode: "oauth" (default), "api_key" (--bare), or "long_lived_token".
    # Accept both "claude_auth_method" (preferred) and "auth_mode" (legacy).
    # Auto-detect from environment when not explicitly set in config.
    raw_auth_mode = data.get("claude_auth_method") if "claude_auth_method" in data else data.get("auth_mode")
    if raw_auth_mode is not None:
        auth_mode = str(raw_auth_mode)
    elif os.environ.get("ANTHROPIC_API_KEY"):
        auth_mode = "api_key"
    else:
        auth_mode = "oauth"

    config = BotfarmConfig(
        projects=projects,
        bugtracker=bugtracker,
        database=database,
        usage_limits=usage_limits,
        dashboard=dashboard,
        agents=agents,
        logging=logging_cfg,
        notifications=notifications,
        identities=identities,
        refactoring_analysis=refactoring_analysis,
        codex_usage=codex_usage,
        daily_summary=daily_summary,
        start_paused=start_paused,
        auth_mode=auth_mode,
    )

    _validate_config(config)
    config.source_path = str(config_path)
    return config


# ---------------------------------------------------------------------------
# Runtime config editing
# ---------------------------------------------------------------------------

# Fields that can be edited at runtime without a restart.
# Each key is a tuple path (section, field) with validation metadata.
EDITABLE_FIELDS: dict[tuple[str, str], dict] = {
    ("bugtracker", "poll_interval_seconds"): {"type": "int", "min": 1},
    ("bugtracker", "issue_limit"): {"type": "int", "min": 1},
    ("bugtracker", "include_tags"): {"type": "str_list"},
    ("bugtracker", "comment_on_failure"): {"type": "bool"},
    ("bugtracker", "comment_on_completion"): {"type": "bool"},
    ("bugtracker", "comment_on_limit_pause"): {"type": "bool"},
    ("bugtracker.capacity_monitoring", "enabled"): {"type": "bool"},
    ("bugtracker.capacity_monitoring", "warning_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("bugtracker.capacity_monitoring", "critical_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("bugtracker.capacity_monitoring", "pause_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("bugtracker.capacity_monitoring", "resume_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("usage_limits", "enabled"): {"type": "bool"},
    ("usage_limits", "poll_interval_seconds"): {"type": "int", "min": 1},
    ("usage_limits", "pause_five_hour_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("usage_limits", "pause_seven_day_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("agents", "max_review_iterations"): {"type": "int", "min": 1},
    ("agents", "max_ci_retries"): {"type": "int", "min": 0},
    ("agents", "max_merge_conflict_retries"): {"type": "int", "min": 0},
    ("agents", "pr_checks_timeout_seconds"): {"type": "int", "min": 1},
    ("agents", "timeout_minutes"): {"type": "timeout_dict"},
    ("agents", "timeout_grace_seconds"): {"type": "int", "min": 0},
    # Legacy codex_reviewer_* keys kept as aliases for dashboard compat
    ("agents", "codex_reviewer_enabled"): {"type": "bool"},
    ("agents", "codex_reviewer_model"): {"type": "str"},
    ("agents", "codex_reviewer_reasoning_effort"): {"type": "str"},
    ("agents", "codex_reviewer_timeout_minutes"): {"type": "int", "min": 1},
    ("agents", "codex_reviewer_skip_on_reiteration"): {"type": "bool"},
    ("daily_summary", "enabled"): {"type": "bool"},
    ("daily_summary", "send_hour"): {"type": "int", "min": 0, "max": 23},
    ("daily_summary", "min_tasks_for_summary"): {"type": "int", "min": 0},
    ("daily_summary", "webhook_url"): {"type": "str"},
    ("notifications", "webhook_url"): {"type": "str"},
    ("notifications", "webhook_format"): {"type": "choice", "choices": ["slack", "discord"]},
    ("notifications", "rate_limit_seconds"): {"type": "int", "min": 0},
}

# Fields that require a supervisor restart to take effect.
# Changes are written to YAML only — NOT applied to in-memory config.
STRUCTURAL_FIELDS: dict[tuple[str, str], dict] = {}

# Maps legacy codex_reviewer_* field names → AdapterConfig attribute names.
_CODEX_LEGACY_FIELD_MAP: dict[str, str] = {
    "codex_reviewer_enabled": "enabled",
    "codex_reviewer_model": "model",
    "codex_reviewer_reasoning_effort": "reasoning_effort",
    "codex_reviewer_timeout_minutes": "timeout_minutes",
    "codex_reviewer_skip_on_reiteration": "skip_on_reiteration",
}


def validate_config_updates(
    updates: dict, config: BotfarmConfig | None = None,
) -> list[str]:
    """Validate a partial config update dict.

    ``updates`` is a nested dict like ``{"bugtracker": {"poll_interval_seconds": 60}}``.
    When ``config`` is provided, cross-field invariants are checked against the
    current in-memory values for fields not present in the update.
    Returns a list of error messages (empty means valid).
    """
    errors: list[str] = []
    for section, fields in updates.items():
        if not isinstance(fields, dict):
            errors.append(f"'{section}' must be a mapping")
            continue
        for key, value in fields.items():
            path = (section, key)
            spec = EDITABLE_FIELDS.get(path)
            if spec is None:
                errors.append(f"'{section}.{key}' is not an editable field")
                continue
            errors.extend(_validate_field(section, key, value, spec))

    # Cross-field invariants for capacity monitoring thresholds
    cap_fields = updates.get("bugtracker.capacity_monitoring", {})
    if isinstance(cap_fields, dict):
        # Resolve effective values: use update if present, fall back to config
        cap_cfg = getattr(config.bugtracker, "capacity_monitoring", None) if config else None

        def _resolve(field: str) -> float | None:
            val = cap_fields.get(field)
            if val is not None and isinstance(val, (int, float)) and not isinstance(val, bool):
                return float(val)
            if cap_cfg is not None:
                return float(getattr(cap_cfg, field))
            return None

        resume = _resolve("resume_threshold")
        pause = _resolve("pause_threshold")
        warning = _resolve("warning_threshold")
        critical = _resolve("critical_threshold")

        # resume_threshold must be less than pause_threshold
        if resume is not None and pause is not None and resume >= pause:
            errors.append(
                "bugtracker.capacity_monitoring.resume_threshold must be less than pause_threshold"
            )

        # warning_threshold <= critical_threshold <= pause_threshold
        if warning is not None and critical is not None and warning > critical:
            errors.append(
                "bugtracker.capacity_monitoring.warning_threshold must be "
                "less than or equal to critical_threshold"
            )
        if critical is not None and pause is not None and critical > pause:
            errors.append(
                "bugtracker.capacity_monitoring.critical_threshold must be "
                "less than or equal to pause_threshold"
            )

    return errors


def _validate_field(
    section: str, key: str, value: object, spec: dict,
) -> list[str]:
    """Validate a single field value against its spec."""
    errors: list[str] = []
    field_name = f"{section}.{key}"

    if spec["type"] == "str":
        if not isinstance(value, str):
            errors.append(f"'{field_name}' must be a string, got {type(value).__name__}")

    elif spec["type"] == "bool":
        if not isinstance(value, bool):
            errors.append(f"'{field_name}' must be a boolean, got {type(value).__name__}")

    elif spec["type"] == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(f"'{field_name}' must be an integer, got {type(value).__name__}")
        else:
            if "min" in spec and value < spec["min"]:
                errors.append(f"'{field_name}' must be at least {spec['min']}")
            if "max" in spec and value > spec["max"]:
                errors.append(f"'{field_name}' must be at most {spec['max']}")

    elif spec["type"] == "float":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            errors.append(f"'{field_name}' must be a number, got {type(value).__name__}")
        else:
            fval = float(value)
            if "min" in spec and fval < spec["min"]:
                errors.append(f"'{field_name}' must be at least {spec['min']}")
            if "max" in spec and fval > spec["max"]:
                errors.append(f"'{field_name}' must be at most {spec['max']}")

    elif spec["type"] == "choice":
        if value not in spec["choices"]:
            errors.append(
                f"'{field_name}' must be one of {spec['choices']}, "
                f"got {value!r}"
            )

    elif spec["type"] == "str_list":
        if not isinstance(value, list):
            errors.append(f"'{field_name}' must be a list, got {type(value).__name__}")
        elif not all(isinstance(item, str) for item in value):
            errors.append(f"'{field_name}' must be a list of strings")

    elif spec["type"] == "timeout_dict":
        if not isinstance(value, dict):
            errors.append(f"'{field_name}' must be a mapping of stage → minutes")
        else:
            for stage, minutes in value.items():
                if stage not in _KNOWN_TIMEOUT_STAGES:
                    errors.append(
                        f"'{field_name}': unknown stage '{stage}'. "
                        f"Valid stages: {sorted(_KNOWN_TIMEOUT_STAGES)}"
                    )
                elif not isinstance(minutes, int) or isinstance(minutes, bool):
                    errors.append(f"'{field_name}.{stage}' must be an integer")
                elif minutes < 1:
                    errors.append(f"'{field_name}.{stage}' must be at least 1")

    return errors


def apply_config_updates(config: BotfarmConfig, updates: dict) -> None:
    """Apply validated updates to the in-memory config object.

    ``updates`` must already be validated via ``validate_config_updates()``.

    Thread safety: this is called from the dashboard (uvicorn) thread while
    the supervisor reads the config from the main thread.  CPython's GIL
    makes simple ``setattr`` and ``dict.update()`` effectively atomic, so
    no explicit lock is needed.  If the runtime ever moves to free-threaded
    Python, a lock should be added here.
    """
    section_map: dict[str, object] = {
        "bugtracker": config.bugtracker,
        "usage_limits": config.usage_limits,
        "agents": config.agents,
        "daily_summary": config.daily_summary,
        "notifications": config.notifications,
    }
    if isinstance(config.bugtracker, LinearBugtrackerConfig):
        section_map["bugtracker.capacity_monitoring"] = config.bugtracker.capacity_monitoring
    for section, fields in updates.items():
        obj = section_map.get(section)
        if obj is None:
            continue
        for key, value in fields.items():
            if key == "timeout_minutes":
                # Merge into existing dict rather than replacing entirely
                obj.timeout_minutes.update(value)
            elif section == "agents" and key.startswith("codex_reviewer_"):
                # Route legacy codex_reviewer_* keys to adapters["codex"]
                codex_cfg = obj.adapters.setdefault("codex", replace(CODEX_ADAPTER_DEFAULTS))
                attr = _CODEX_LEGACY_FIELD_MAP.get(key)
                if attr:
                    if isinstance(value, (int, float)) and not isinstance(value, bool):
                        current = getattr(codex_cfg, attr)
                        if isinstance(current, int):
                            value = int(value)
                    setattr(codex_cfg, attr, value)
            elif hasattr(obj, key):
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    # Coerce to the field's declared type
                    current = getattr(obj, key)
                    if isinstance(current, float):
                        value = float(value)
                    elif isinstance(current, int):
                        value = int(value)
                setattr(obj, key, value)


def write_config_updates(config_path: Path, updates: dict) -> None:
    """Read the raw YAML config, apply updates, and write back atomically.

    Preserves ``${ENV_VAR}`` references by operating on the raw YAML dict
    (before environment variable expansion).
    """
    raw = config_path.read_text()
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    for section, fields in updates.items():
        # Support dotted section paths like "bugtracker.capacity_monitoring"
        parts = section.split(".")
        target = data
        for part in parts:
            if part not in target:
                target[part] = {}
            target = target[part]
            if not isinstance(target, dict):
                break
        if not isinstance(target, dict):
            continue
        for key, value in fields.items():
            if key == "timeout_minutes" and isinstance(target.get(key), dict):
                target[key].update(value)
            elif section == "agents" and key in _CODEX_LEGACY_FIELD_MAP:
                # Route legacy codex_reviewer_* keys to adapters.codex
                # so dashboard edits persist correctly when YAML uses the
                # new adapters format.
                adapter_attr = _CODEX_LEGACY_FIELD_MAP[key]
                adapters_block = target.setdefault("adapters", {})
                codex_block = adapters_block.setdefault("codex", {})
                codex_block[adapter_attr] = value
                # Also remove the flat legacy key if present, to avoid
                # the "both forms present" precedence issue on reload.
                target.pop(key, None)
            else:
                target[key] = value

    write_yaml_atomic(config_path, data)


def write_yaml_atomic(config_path: Path, data: dict) -> None:
    """Write a dict to a YAML file atomically via temp file + rename."""
    fd, tmp_path = tempfile.mkstemp(
        dir=str(config_path.parent), suffix=".yaml.tmp",
    )
    try:
        with os.fdopen(fd, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
        os.replace(tmp_path, str(config_path))
    except BaseException:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Structural config editing (restart required)
# ---------------------------------------------------------------------------


def validate_structural_config_updates(
    updates: dict, config: BotfarmConfig,
) -> list[str]:
    """Validate structural config updates (projects).

    ``updates`` may contain a ``"projects"`` list.
    Returns a list of error messages (empty = valid).
    """
    errors: list[str] = []

    if "projects" in updates:
        errors.extend(_validate_project_updates(updates["projects"], config))

    return errors


def _validate_project_updates(
    project_updates: object, config: BotfarmConfig,
) -> list[str]:
    """Validate project-level structural updates (slots, tracker_project).

    Existing projects: only ``slots`` and ``tracker_project`` may be changed.
    New projects (name not in config): all required fields must be present
    (``name``, ``team``, ``base_dir``, ``worktree_prefix``, ``slots``).
    """
    errors: list[str] = []

    if not isinstance(project_updates, list):
        errors.append("'projects' must be a list")
        return errors

    existing_names = {p.name for p in config.projects}

    _NEW_PROJECT_REQUIRED = {"name", "team", "base_dir", "worktree_prefix", "slots"}
    _NEW_PROJECT_OPTIONAL = {"tracker_project", "project_type", "setup_commands",
                              "run_command", "run_env", "run_port",
                              "include_tags", "bugtracker",
                              "qa_teardown_command"}
    _NEW_PROJECT_ALLOWED = _NEW_PROJECT_REQUIRED | _NEW_PROJECT_OPTIONAL
    seen_names: set[str] = set()

    for i, proj in enumerate(project_updates):
        if not isinstance(proj, dict):
            errors.append(f"projects[{i}]: must be a mapping")
            continue

        name = proj.get("name")
        if not name or not isinstance(name, str):
            errors.append(f"projects[{i}]: 'name' is required")
            continue

        # Reject duplicate names within the same payload
        if name in seen_names:
            errors.append(
                f"projects[{i}] '{name}': duplicate project name in request"
            )
            continue
        seen_names.add(name)

        is_new = name not in existing_names

        # --- Validate slots (shared by new and existing) ---
        if "slots" in proj:
            slots = proj["slots"]
            if not isinstance(slots, list):
                errors.append(f"projects[{i}] '{name}': slots must be a list")
            elif not all(isinstance(s, int) and not isinstance(s, bool) for s in slots):
                errors.append(
                    f"projects[{i}] '{name}': slots must be a list of integers"
                )
            elif len(slots) != len(set(slots)):
                errors.append(
                    f"projects[{i}] '{name}': slots contains duplicate values"
                )

        if "tracker_project" in proj:
            lp = proj["tracker_project"]
            if not isinstance(lp, str):
                errors.append(
                    f"projects[{i}] '{name}': tracker_project must be a string"
                )

        if "include_tags" in proj:
            it = proj["include_tags"]
            if not isinstance(it, list) or not all(isinstance(t, str) for t in it):
                errors.append(
                    f"projects[{i}] '{name}': include_tags must be a list of strings"
                )

        if "run_command" in proj:
            if not isinstance(proj["run_command"], str):
                errors.append(
                    f"projects[{i}] '{name}': run_command must be a string"
                )

        if "run_env" in proj:
            re_val = proj["run_env"]
            if not isinstance(re_val, dict) or not all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in re_val.items()
            ):
                errors.append(
                    f"projects[{i}] '{name}': run_env must be a mapping of strings to strings"
                )

        if "run_port" in proj:
            rp = proj["run_port"]
            if isinstance(rp, bool) or not isinstance(rp, int):
                errors.append(
                    f"projects[{i}] '{name}': run_port must be an integer"
                )
            elif rp < 0:
                errors.append(
                    f"projects[{i}] '{name}': run_port must be a non-negative integer"
                )

        if "bugtracker" in proj:
            errors.extend(_validate_bugtracker_override(proj["bugtracker"], name))

        if is_new:
            # New project: validate required fields and allowed keys
            missing = _NEW_PROJECT_REQUIRED - set(proj.keys())
            if missing:
                errors.append(
                    f"projects[{i}] '{name}': new project missing required "
                    f"fields: {sorted(missing)}"
                )
            extra = set(proj.keys()) - _NEW_PROJECT_ALLOWED
            if extra:
                errors.append(
                    f"projects[{i}] '{name}': "
                    f"unknown fields: {sorted(extra)}"
                )
            # Validate string fields
            for field in ("team", "base_dir", "worktree_prefix"):
                val = proj.get(field)
                if val is not None and not isinstance(val, str):
                    errors.append(
                        f"projects[{i}] '{name}': {field} must be a string"
                    )
                elif isinstance(val, str) and not val.strip():
                    errors.append(
                        f"projects[{i}] '{name}': {field} must not be empty"
                    )
        else:
            # Existing project: editable fields
            allowed_keys = {"name", "slots", "tracker_project",
                            "run_command", "run_env", "run_port",
                            "include_tags", "bugtracker",
                            "qa_teardown_command"}
            extra = set(proj.keys()) - allowed_keys
            if extra:
                errors.append(
                    f"projects[{i}] '{name}': "
                    f"cannot edit fields: {sorted(extra)}"
                )

    # Check for duplicate tracker_project values after applying updates
    if not errors:
        lp_overrides: dict[str, str] = {}
        new_projects: list[dict] = []
        for u in project_updates:
            if isinstance(u, dict):
                if u["name"] not in existing_names:
                    new_projects.append(u)
                elif "tracker_project" in u:
                    lp_overrides[u["name"]] = u["tracker_project"]

        seen_lp: dict[str, str] = {}  # tracker_project -> project name
        # Check existing projects (with overrides applied)
        for p in config.projects:
            lp = lp_overrides.get(p.name, p.tracker_project)
            if lp:
                if lp in seen_lp:
                    errors.append(
                        f"Duplicate tracker_project filter {lp!r}: "
                        f"used by both '{seen_lp[lp]}' and '{p.name}'. "
                        f"Each project must have a unique tracker_project filter"
                    )
                seen_lp[lp] = p.name
        # Check new projects
        for np in new_projects:
            lp = np.get("tracker_project", "")
            if lp:
                if lp in seen_lp:
                    errors.append(
                        f"Duplicate tracker_project filter {lp!r}: "
                        f"used by both '{seen_lp[lp]}' and '{np['name']}'. "
                        f"Each project must have a unique tracker_project filter"
                    )
                seen_lp[lp] = np["name"]

    return errors


def write_structural_config_updates(config_path: Path, updates: dict) -> None:
    """Write structural config changes to YAML without mutating in-memory config.

    Handles ``"projects"`` (merge editable fields into existing project
    entries matched by name).
    """
    raw = config_path.read_text()
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    if "projects" in updates:
        existing_projects = data.get("projects", [])
        if not isinstance(existing_projects, list):
            existing_projects = []
            data["projects"] = existing_projects
        by_name = {p["name"]: p for p in existing_projects if isinstance(p, dict)}
        for proj_update in updates["projects"]:
            name = proj_update["name"]
            if name in by_name:
                # Existing project: merge editable fields
                target = by_name[name]
                for fld in ("slots", "tracker_project", "include_tags",
                            "run_command", "run_env", "run_port", "bugtracker",
                            "qa_teardown_command"):
                    if fld in proj_update:
                        val = proj_update[fld]
                        if fld == "include_tags" and val == []:
                            target.pop("include_tags", None)
                        else:
                            target[fld] = val
            else:
                # New project: append complete dict (only if required keys present)
                required = {"team", "base_dir", "worktree_prefix", "slots"}
                if required.issubset(proj_update.keys()):
                    new_proj = {
                        "name": name,
                        "team": proj_update["team"],
                        "base_dir": proj_update["base_dir"],
                        "worktree_prefix": proj_update["worktree_prefix"],
                        "slots": proj_update["slots"],
                    }
                    if proj_update.get("tracker_project"):
                        new_proj["tracker_project"] = proj_update["tracker_project"]
                    if proj_update.get("project_type"):
                        new_proj["project_type"] = proj_update["project_type"]
                    if proj_update.get("setup_commands"):
                        new_proj["setup_commands"] = proj_update["setup_commands"]
                    if "run_command" in proj_update:
                        new_proj["run_command"] = proj_update["run_command"]
                    if "run_env" in proj_update:
                        new_proj["run_env"] = proj_update["run_env"]
                    if "run_port" in proj_update:
                        new_proj["run_port"] = proj_update["run_port"]
                    if proj_update.get("bugtracker"):
                        new_proj["bugtracker"] = proj_update["bugtracker"]
                    if proj_update.get("qa_teardown_command"):
                        new_proj["qa_teardown_command"] = proj_update["qa_teardown_command"]
                    existing_projects.append(new_proj)

    write_yaml_atomic(config_path, data)


# ---------------------------------------------------------------------------
# Runtime config DB sync
# ---------------------------------------------------------------------------


def sync_agent_config_to_db(
    conn: "sqlite3.Connection",
    agents_config: AgentsConfig,
) -> None:
    """Extract runtime-relevant fields from AgentsConfig and write to DB."""
    from botfarm.db import RUNTIME_CONFIG_KEYS, write_runtime_config_batch

    settings: dict[str, object] = {
        key: getattr(agents_config, key) for key in RUNTIME_CONFIG_KEYS
    }
    write_runtime_config_batch(conn, settings)
