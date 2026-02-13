"""Configuration loading and validation for botfarm."""

from __future__ import annotations

import os
import re
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

max_total_slots: 5

linear:
  api_key: ${LINEAR_API_KEY}
  workspace: my-workspace
  poll_interval_seconds: 120
  exclude_tags:
    - Human

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

state_file: ~/.botfarm/state.json
"""


@dataclass
class ProjectConfig:
    name: str
    linear_team: str
    base_dir: str
    worktree_prefix: str
    slots: list[int]


@dataclass
class LinearConfig:
    api_key: str = ""
    workspace: str = ""
    poll_interval_seconds: int = 120
    exclude_tags: list[str] = field(default_factory=lambda: ["Human"])


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


@dataclass
class BotfarmConfig:
    projects: list[ProjectConfig]
    max_total_slots: int = 5
    linear: LinearConfig = field(default_factory=LinearConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    usage_limits: UsageLimitsConfig = field(default_factory=UsageLimitsConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    agents: AgentsConfig = field(default_factory=AgentsConfig)
    state_file: str = "~/.botfarm/state.json"


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
    )


def _validate_config(config: BotfarmConfig) -> None:
    """Validate cross-field constraints."""
    if config.max_total_slots < 1:
        raise ConfigError("max_total_slots must be at least 1")

    if not config.linear.api_key:
        raise ConfigError("linear.api_key must be set")

    if config.linear.poll_interval_seconds < 1:
        raise ConfigError("poll_interval_seconds must be at least 1")

    if not config.projects:
        raise ConfigError("At least one project must be configured")

    names = [p.name for p in config.projects]
    if len(names) != len(set(names)):
        raise ConfigError("Duplicate project names found")

    all_slots = []
    for p in config.projects:
        all_slots.extend(p.slots)
    if len(all_slots) != len(set(all_slots)):
        raise ConfigError("Duplicate slot assignments found across projects")

    total_slots = sum(len(p.slots) for p in config.projects)
    if total_slots > config.max_total_slots:
        raise ConfigError(
            f"Total assigned slots ({total_slots}) exceeds "
            f"max_total_slots ({config.max_total_slots})"
        )

    for attr in ("pause_five_hour_threshold", "pause_seven_day_threshold"):
        val = getattr(config.usage_limits, attr)
        if not (0.0 <= val <= 1.0):
            raise ConfigError(f"usage_limits.{attr} must be between 0.0 and 1.0")

    if config.agents.max_review_iterations < 1:
        raise ConfigError("agents.max_review_iterations must be at least 1")

    if config.agents.max_ci_retries < 0:
        raise ConfigError("agents.max_ci_retries must be at least 0")


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
    agents = AgentsConfig(
        max_review_iterations=int(agents_data.get("max_review_iterations", 3)),
        max_ci_retries=int(agents_data.get("max_ci_retries", 2)),
    )

    config = BotfarmConfig(
        projects=projects,
        max_total_slots=data.get("max_total_slots", 5),
        linear=linear,
        database=database,
        usage_limits=usage_limits,
        dashboard=dashboard,
        agents=agents,
        state_file=data.get("state_file", "~/.botfarm/state.json"),
    )

    _validate_config(config)
    return config
