"""Configuration loading and validation for botfarm."""

from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

DEFAULT_CONFIG_DIR = Path.home() / ".botfarm"
DEFAULT_CONFIG_PATH = DEFAULT_CONFIG_DIR / "config.yaml"

DEFAULT_CONFIG_TEMPLATE = """\
# Botfarm configuration
# See documentation for full reference.

projects:
  - name: my-project
    linear_team: TEAM
    base_dir: ~/my-project
    worktree_prefix: my-project-slot-
    slots: [1, 2]

linear:
  api_key: ${LINEAR_API_KEY}
  workspace: my-workspace
  poll_interval_seconds: 120
  exclude_tags:
    - Human
  # Workflow status names (must match your Linear team's workflow)
  todo_status: Todo
  in_progress_status: In Progress
  done_status: Done
  in_review_status: In Review
  failed_status: Todo
  # Comment posting controls
  comment_on_failure: true
  comment_on_completion: false
  comment_on_limit_pause: false

database:
  path: ~/.botfarm/botfarm.db

usage_limits:
  pause_five_hour_threshold: 0.85
  pause_seven_day_threshold: 0.90

dashboard:
  enabled: false
  host: 0.0.0.0
  port: 8420

agents:
  max_review_iterations: 3
  max_ci_retries: 2
  timeout_minutes:
    implement: 120
    review: 30
    fix: 60
  timeout_grace_seconds: 10

# notifications:
#   webhook_url: https://hooks.slack.com/services/...
#   webhook_format: slack  # or "discord"
#   rate_limit_seconds: 300

state_file: ~/.botfarm/state.json
"""


@dataclass
class ProjectConfig:
    name: str
    linear_team: str
    base_dir: str
    worktree_prefix: str
    slots: list[int]
    linear_project: str = ""


@dataclass
class LinearConfig:
    api_key: str = ""
    workspace: str = ""
    poll_interval_seconds: int = 120
    exclude_tags: list[str] = field(default_factory=lambda: ["Human"])
    # Configurable workflow status names
    todo_status: str = "Todo"
    in_progress_status: str = "In Progress"
    done_status: str = "Done"
    in_review_status: str = "In Review"
    failed_status: str = "Todo"
    # Comment posting controls
    comment_on_failure: bool = True
    comment_on_completion: bool = False
    comment_on_limit_pause: bool = False


@dataclass
class DatabaseConfig:
    path: str = "~/.botfarm/botfarm.db"


@dataclass
class UsageLimitsConfig:
    pause_five_hour_threshold: float = 0.85
    pause_seven_day_threshold: float = 0.90


@dataclass
class DashboardConfig:
    enabled: bool = False
    host: str = "0.0.0.0"
    port: int = 8420


@dataclass
class AgentsConfig:
    max_review_iterations: int = 3
    max_ci_retries: int = 2
    timeout_minutes: dict[str, int] = field(default_factory=lambda: {
        "implement": 120,
        "review": 30,
        "fix": 60,
    })
    timeout_grace_seconds: int = 10


@dataclass
class NotificationsConfig:
    webhook_url: str = ""
    webhook_format: str = "slack"  # "slack" or "discord"
    rate_limit_seconds: int = 300


@dataclass
class BotfarmConfig:
    projects: list[ProjectConfig]
    linear: LinearConfig = field(default_factory=LinearConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    usage_limits: UsageLimitsConfig = field(default_factory=UsageLimitsConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    agents: AgentsConfig = field(default_factory=AgentsConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    state_file: str = "~/.botfarm/state.json"
    source_path: str = ""  # Set by load_config to the source file path


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


def _parse_project(data: dict) -> ProjectConfig:
    required = {"name", "linear_team", "base_dir", "worktree_prefix", "slots"}
    missing = required - set(data.keys())
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
    return ProjectConfig(
        name=data["name"],
        linear_team=data["linear_team"],
        base_dir=data["base_dir"],
        worktree_prefix=data["worktree_prefix"],
        slots=slots,
        linear_project=str(data.get("linear_project", "")),
    )


_KNOWN_TIMEOUT_STAGES = {"implement", "review", "fix", "pr_checks", "merge"}


def _validate_config(config: BotfarmConfig) -> None:
    """Validate cross-field constraints."""
    if not config.linear.api_key:
        raise ConfigError("linear.api_key must be set")

    if config.linear.poll_interval_seconds < 1:
        raise ConfigError("poll_interval_seconds must be at least 1")

    if not config.projects:
        raise ConfigError("At least one project must be configured")

    names = [p.name for p in config.projects]
    if len(names) != len(set(names)):
        raise ConfigError("Duplicate project names found")

    for attr in ("pause_five_hour_threshold", "pause_seven_day_threshold"):
        val = getattr(config.usage_limits, attr)
        if not (0.0 <= val <= 1.0):
            raise ConfigError(f"usage_limits.{attr} must be between 0.0 and 1.0")

    if config.agents.max_review_iterations < 1:
        raise ConfigError("agents.max_review_iterations must be at least 1")

    if config.agents.max_ci_retries < 0:
        raise ConfigError("agents.max_ci_retries must be at least 0")

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

    if config.agents.timeout_grace_seconds < 0:
        raise ConfigError("agents.timeout_grace_seconds must be at least 0")

    if config.notifications.webhook_format not in ("slack", "discord"):
        raise ConfigError(
            f"notifications.webhook_format must be 'slack' or 'discord', "
            f"got: {config.notifications.webhook_format!r}"
        )

    if config.notifications.rate_limit_seconds < 0:
        raise ConfigError("notifications.rate_limit_seconds must be at least 0")


def _parse_bool(data: dict, key: str, default: bool) -> bool:
    """Parse a boolean config value, rejecting non-boolean types."""
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(
            f"linear.{key} must be a boolean (true/false), got: {value!r}"
        )
    return value


def load_config(config_path: Path = DEFAULT_CONFIG_PATH) -> BotfarmConfig:
    """Load, expand, validate, and return the botfarm configuration."""
    if not config_path.exists():
        raise ConfigError(f"Config file not found: {config_path}")

    raw = config_path.read_text()
    data = yaml.safe_load(raw)

    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    data = _expand_env_recursive(data)

    if "projects" not in data or not isinstance(data.get("projects"), list):
        raise ConfigError("Config must contain a 'projects' list")

    projects = [_parse_project(p) for p in data["projects"]]

    linear_data = data.get("linear", {})
    linear = LinearConfig(
        api_key=linear_data.get("api_key", ""),
        workspace=linear_data.get("workspace", ""),
        poll_interval_seconds=linear_data.get("poll_interval_seconds", 120),
        exclude_tags=linear_data.get("exclude_tags", ["Human"]),
        todo_status=str(linear_data.get("todo_status", "Todo")),
        in_progress_status=str(linear_data.get("in_progress_status", "In Progress")),
        done_status=str(linear_data.get("done_status", "Done")),
        in_review_status=str(linear_data.get("in_review_status", "In Review")),
        failed_status=str(linear_data.get("failed_status", "Todo")),
        comment_on_failure=_parse_bool(linear_data, "comment_on_failure", True),
        comment_on_completion=_parse_bool(linear_data, "comment_on_completion", False),
        comment_on_limit_pause=_parse_bool(linear_data, "comment_on_limit_pause", False),
    )

    db_data = data.get("database", {})
    database = DatabaseConfig(
        path=db_data.get("path", "~/.botfarm/botfarm.db"),
    )

    ul_data = data.get("usage_limits", {})
    usage_limits = UsageLimitsConfig(
        pause_five_hour_threshold=float(ul_data.get("pause_five_hour_threshold", 0.85)),
        pause_seven_day_threshold=float(ul_data.get("pause_seven_day_threshold", 0.90)),
    )

    dash_data = data.get("dashboard", {})
    dashboard = DashboardConfig(
        enabled=bool(dash_data.get("enabled", False)),
        host=str(dash_data.get("host", "0.0.0.0")),
        port=int(dash_data.get("port", 8420)),
    )

    agents_data = data.get("agents", {})
    timeout_defaults = {"implement": 120, "review": 30, "fix": 60}
    raw_timeouts = agents_data.get("timeout_minutes", {})
    timeout_minutes = {**timeout_defaults, **{k: int(v) for k, v in raw_timeouts.items()}}
    agents = AgentsConfig(
        max_review_iterations=int(agents_data.get("max_review_iterations", 3)),
        max_ci_retries=int(agents_data.get("max_ci_retries", 2)),
        timeout_minutes=timeout_minutes,
        timeout_grace_seconds=int(agents_data.get("timeout_grace_seconds", 10)),
    )

    notif_data = data.get("notifications", {})
    notifications = NotificationsConfig(
        webhook_url=str(notif_data.get("webhook_url", "")),
        webhook_format=str(notif_data.get("webhook_format", "slack")),
        rate_limit_seconds=int(notif_data.get("rate_limit_seconds", 300)),
    )

    config = BotfarmConfig(
        projects=projects,
        linear=linear,
        database=database,
        usage_limits=usage_limits,
        dashboard=dashboard,
        agents=agents,
        notifications=notifications,
        state_file=data.get("state_file", "~/.botfarm/state.json"),
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
    ("linear", "poll_interval_seconds"): {"type": "int", "min": 1},
    ("linear", "comment_on_failure"): {"type": "bool"},
    ("linear", "comment_on_completion"): {"type": "bool"},
    ("linear", "comment_on_limit_pause"): {"type": "bool"},
    ("usage_limits", "pause_five_hour_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("usage_limits", "pause_seven_day_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("agents", "max_review_iterations"): {"type": "int", "min": 1},
    ("agents", "max_ci_retries"): {"type": "int", "min": 0},
    ("agents", "timeout_minutes"): {"type": "timeout_dict"},
    ("agents", "timeout_grace_seconds"): {"type": "int", "min": 0},
}


def validate_config_updates(updates: dict) -> list[str]:
    """Validate a partial config update dict.

    ``updates`` is a nested dict like ``{"linear": {"poll_interval_seconds": 60}}``.
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
    return errors


def _validate_field(
    section: str, key: str, value: object, spec: dict,
) -> list[str]:
    """Validate a single field value against its spec."""
    errors: list[str] = []
    field_name = f"{section}.{key}"

    if spec["type"] == "bool":
        if not isinstance(value, bool):
            errors.append(f"'{field_name}' must be a boolean, got {type(value).__name__}")

    elif spec["type"] == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(f"'{field_name}' must be an integer, got {type(value).__name__}")
        elif "min" in spec and value < spec["min"]:
            errors.append(f"'{field_name}' must be at least {spec['min']}")

    elif spec["type"] == "float":
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            errors.append(f"'{field_name}' must be a number, got {type(value).__name__}")
        else:
            fval = float(value)
            if "min" in spec and fval < spec["min"]:
                errors.append(f"'{field_name}' must be at least {spec['min']}")
            if "max" in spec and fval > spec["max"]:
                errors.append(f"'{field_name}' must be at most {spec['max']}")

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
    section_map = {
        "linear": config.linear,
        "usage_limits": config.usage_limits,
        "agents": config.agents,
    }
    for section, fields in updates.items():
        obj = section_map.get(section)
        if obj is None:
            continue
        for key, value in fields.items():
            if key == "timeout_minutes":
                # Merge into existing dict rather than replacing entirely
                obj.timeout_minutes.update(value)
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
        if section not in data:
            data[section] = {}
        section_data = data[section]
        if not isinstance(section_data, dict):
            continue
        for key, value in fields.items():
            if key == "timeout_minutes" and isinstance(section_data.get(key), dict):
                section_data[key].update(value)
            else:
                section_data[key] = value

    # Atomic write: write to temp file then rename
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
