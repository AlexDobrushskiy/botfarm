"""Configuration loading and validation for botfarm."""

from __future__ import annotations

import logging
import os
import re
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

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
  poll_interval_seconds: 30
  exclude_tags:
    - Human
  issue_limit: 250  # Free plan default, override for paid plans
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
  # timeout_overrides (first matching label wins; order matters):
  #   Investigation:
  #     implement: 30
  timeout_grace_seconds: 10
  codex_reviewer_enabled: false
  codex_reviewer_model: ""              # e.g. "o3", "o4-mini", or empty for default
  codex_reviewer_timeout_minutes: 15    # separate from Claude review timeout

# identities:
#   coder:
#     github_token: ${CODER_GITHUB_TOKEN}
#     ssh_key_path: ~/.botfarm/coder_id_ed25519
#     git_author_name: "Coder Bot"
#     git_author_email: "coder-bot@example.com"
#     linear_api_key: ${CODER_LINEAR_API_KEY}
#   reviewer:
#     github_token: ${REVIEWER_GITHUB_TOKEN}
#     linear_api_key: ${REVIEWER_LINEAR_API_KEY}

# notifications:
#   webhook_url: https://hooks.slack.com/services/...
#   webhook_format: slack  # or "discord"
#   rate_limit_seconds: 300
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
class CapacityConfig:
    enabled: bool = True
    warning_threshold: float = 0.70
    critical_threshold: float = 0.85
    pause_threshold: float = 0.95
    resume_threshold: float = 0.90


@dataclass
class LinearConfig:
    api_key: str = ""
    workspace: str = ""
    poll_interval_seconds: int = 30
    exclude_tags: list[str] = field(default_factory=lambda: ["Human"])
    issue_limit: int = 250
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
    capacity_monitoring: CapacityConfig = field(default_factory=CapacityConfig)


@dataclass
class DatabaseConfig:
    path: str = ""


@dataclass
class UsageLimitsConfig:
    enabled: bool = True
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
    timeout_overrides: dict[str, dict[str, int]] = field(default_factory=dict)
    timeout_grace_seconds: int = 10
    codex_reviewer_enabled: bool = False
    codex_reviewer_model: str = ""
    codex_reviewer_timeout_minutes: int = 15


@dataclass
class LoggingConfig:
    max_bytes: int = 10 * 1024 * 1024  # 10 MB per rotated file
    backup_count: int = 5  # number of rotated files to keep
    ticket_log_retention_days: int = 30  # delete ticket logs older than N days


@dataclass
class NotificationsConfig:
    webhook_url: str = ""
    webhook_format: str = "slack"  # "slack" or "discord"
    rate_limit_seconds: int = 300


@dataclass
class CoderIdentity:
    github_token: str = ""
    ssh_key_path: str = ""
    git_author_name: str = ""
    git_author_email: str = ""
    linear_api_key: str = ""


@dataclass
class ReviewerIdentity:
    github_token: str = ""
    linear_api_key: str = ""


@dataclass
class IdentitiesConfig:
    coder: CoderIdentity = field(default_factory=CoderIdentity)
    reviewer: ReviewerIdentity = field(default_factory=ReviewerIdentity)


@dataclass
class BotfarmConfig:
    projects: list[ProjectConfig]
    linear: LinearConfig = field(default_factory=LinearConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    usage_limits: UsageLimitsConfig = field(default_factory=UsageLimitsConfig)
    dashboard: DashboardConfig = field(default_factory=DashboardConfig)
    agents: AgentsConfig = field(default_factory=AgentsConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    identities: IdentitiesConfig = field(default_factory=IdentitiesConfig)
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


_KNOWN_TIMEOUT_STAGES = {"implement", "review", "fix", "pr_checks", "ci_fix", "merge"}


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

    if config.linear.issue_limit < 1:
        raise ConfigError("linear.issue_limit must be at least 1")

    cap = config.linear.capacity_monitoring
    for attr in ("warning_threshold", "critical_threshold", "pause_threshold", "resume_threshold"):
        val = getattr(cap, attr)
        if not (0.0 <= val <= 1.0):
            raise ConfigError(
                f"linear.capacity_monitoring.{attr} must be between 0.0 and 1.0"
            )
    if cap.warning_threshold > cap.critical_threshold:
        raise ConfigError(
            "linear.capacity_monitoring.warning_threshold must be "
            "less than or equal to critical_threshold"
        )
    if cap.critical_threshold > cap.pause_threshold:
        raise ConfigError(
            "linear.capacity_monitoring.critical_threshold must be "
            "less than or equal to pause_threshold"
        )
    if cap.resume_threshold >= cap.pause_threshold:
        raise ConfigError(
            "linear.capacity_monitoring.resume_threshold must be less than pause_threshold"
        )

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

    if config.agents.codex_reviewer_timeout_minutes < 1:
        raise ConfigError("agents.codex_reviewer_timeout_minutes must be at least 1")

    if config.logging.max_bytes < 1:
        raise ConfigError("logging.max_bytes must be at least 1")

    if config.logging.backup_count < 0:
        raise ConfigError("logging.backup_count must be at least 0")

    if config.logging.ticket_log_retention_days < 1:
        raise ConfigError("logging.ticket_log_retention_days must be at least 1")

    if config.notifications.webhook_format not in ("slack", "discord"):
        raise ConfigError(
            f"notifications.webhook_format must be 'slack' or 'discord', "
            f"got: {config.notifications.webhook_format!r}"
        )

    if config.notifications.rate_limit_seconds < 0:
        raise ConfigError("notifications.rate_limit_seconds must be at least 0")


def _parse_bool(data: dict, key: str, default: bool, *, section: str = "linear") -> bool:
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
    data = yaml.safe_load(raw)

    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    data = _expand_env_recursive(data)

    known_keys = {
        "projects", "linear", "database", "usage_limits",
        "dashboard", "agents", "logging", "notifications",
        "identities",
    }
    unknown = set(data.keys()) - known_keys
    if unknown:
        logger.warning(
            "Unknown config keys (ignored): %s", ", ".join(sorted(unknown))
        )

    if "projects" not in data or not isinstance(data.get("projects"), list):
        raise ConfigError("Config must contain a 'projects' list")

    projects = [_parse_project(p) for p in data["projects"]]

    linear_data = data.get("linear", {})
    cap_data = linear_data.get("capacity_monitoring", {})
    if not isinstance(cap_data, dict):
        cap_data = {}
    capacity_monitoring = CapacityConfig(
        enabled=_parse_bool(cap_data, "enabled", True, section="linear.capacity_monitoring"),
        warning_threshold=float(cap_data.get("warning_threshold", 0.70)),
        critical_threshold=float(cap_data.get("critical_threshold", 0.85)),
        pause_threshold=float(cap_data.get("pause_threshold", 0.95)),
        resume_threshold=float(cap_data.get("resume_threshold", 0.90)),
    )

    linear = LinearConfig(
        api_key=linear_data.get("api_key", ""),
        workspace=linear_data.get("workspace", ""),
        poll_interval_seconds=linear_data.get("poll_interval_seconds", 30),
        exclude_tags=linear_data.get("exclude_tags", ["Human"]),
        issue_limit=int(linear_data.get("issue_limit", 250)),
        todo_status=str(linear_data.get("todo_status", "Todo")),
        in_progress_status=str(linear_data.get("in_progress_status", "In Progress")),
        done_status=str(linear_data.get("done_status", "Done")),
        in_review_status=str(linear_data.get("in_review_status", "In Review")),
        failed_status=str(linear_data.get("failed_status", "Todo")),
        comment_on_failure=_parse_bool(linear_data, "comment_on_failure", True),
        comment_on_completion=_parse_bool(linear_data, "comment_on_completion", False),
        comment_on_limit_pause=_parse_bool(linear_data, "comment_on_limit_pause", False),
        capacity_monitoring=capacity_monitoring,
    )

    db_data = data.get("database", {})
    database = DatabaseConfig(
        path=db_data.get("path", ""),
    )

    ul_data = data.get("usage_limits", {})
    usage_limits = UsageLimitsConfig(
        enabled=_parse_bool(ul_data, "enabled", True, section="usage_limits"),
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
    raw_overrides = agents_data.get("timeout_overrides", {})
    timeout_overrides: dict[str, dict[str, int]] = {}
    if isinstance(raw_overrides, dict):
        for label, stages in raw_overrides.items():
            if not isinstance(stages, dict):
                raise ConfigError(
                    f"agents.timeout_overrides.{label}: value must be a mapping "
                    f"of stage names to minutes, got {type(stages).__name__}"
                )
            try:
                timeout_overrides[str(label)] = {
                    k: int(v) for k, v in stages.items()
                }
            except (ValueError, TypeError) as exc:
                raise ConfigError(f"agents.timeout_overrides.{label}: {exc}")
    elif raw_overrides:
        raise ConfigError(
            "agents.timeout_overrides must be a mapping of label names to "
            f"stage overrides, got {type(raw_overrides).__name__}"
        )

    agents = AgentsConfig(
        max_review_iterations=int(agents_data.get("max_review_iterations", 3)),
        max_ci_retries=int(agents_data.get("max_ci_retries", 2)),
        timeout_minutes=timeout_minutes,
        timeout_overrides=timeout_overrides,
        timeout_grace_seconds=int(agents_data.get("timeout_grace_seconds", 10)),
        codex_reviewer_enabled=_parse_bool(agents_data, "codex_reviewer_enabled", False, section="agents"),
        codex_reviewer_model=str(agents_data.get("codex_reviewer_model", "")),
        codex_reviewer_timeout_minutes=int(
            agents_data.get("codex_reviewer_timeout_minutes", 15)
        ),
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
            linear_api_key=str(coder_data.get("linear_api_key", "")),
        ),
        reviewer=ReviewerIdentity(
            github_token=str(reviewer_data.get("github_token", "")),
            linear_api_key=str(reviewer_data.get("linear_api_key", "")),
        ),
    )

    if "state_file" in data:
        logger.warning(
            "The 'state_file' config key is deprecated and ignored — all "
            "state is now persisted in the SQLite database."
        )

    config = BotfarmConfig(
        projects=projects,
        linear=linear,
        database=database,
        usage_limits=usage_limits,
        dashboard=dashboard,
        agents=agents,
        logging=logging_cfg,
        notifications=notifications,
        identities=identities,
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
    ("linear", "issue_limit"): {"type": "int", "min": 1},
    ("linear", "comment_on_failure"): {"type": "bool"},
    ("linear", "comment_on_completion"): {"type": "bool"},
    ("linear", "comment_on_limit_pause"): {"type": "bool"},
    ("linear.capacity_monitoring", "enabled"): {"type": "bool"},
    ("linear.capacity_monitoring", "warning_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("linear.capacity_monitoring", "critical_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("linear.capacity_monitoring", "pause_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("linear.capacity_monitoring", "resume_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("usage_limits", "enabled"): {"type": "bool"},
    ("usage_limits", "pause_five_hour_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("usage_limits", "pause_seven_day_threshold"): {"type": "float", "min": 0.0, "max": 1.0},
    ("agents", "max_review_iterations"): {"type": "int", "min": 1},
    ("agents", "max_ci_retries"): {"type": "int", "min": 0},
    ("agents", "timeout_minutes"): {"type": "timeout_dict"},
    ("agents", "timeout_grace_seconds"): {"type": "int", "min": 0},
    ("agents", "codex_reviewer_enabled"): {"type": "bool"},
    ("agents", "codex_reviewer_model"): {"type": "str"},
    ("agents", "codex_reviewer_timeout_minutes"): {"type": "int", "min": 1},
}

# Fields that require a supervisor restart to take effect.
# Changes are written to YAML only — NOT applied to in-memory config.
STRUCTURAL_FIELDS: dict[tuple[str, str], dict] = {
    ("notifications", "webhook_url"): {"type": "str"},
    ("notifications", "webhook_format"): {"type": "choice", "choices": ["slack", "discord"]},
    ("notifications", "rate_limit_seconds"): {"type": "int", "min": 0},
}


def validate_config_updates(
    updates: dict, config: BotfarmConfig | None = None,
) -> list[str]:
    """Validate a partial config update dict.

    ``updates`` is a nested dict like ``{"linear": {"poll_interval_seconds": 60}}``.
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
    cap_fields = updates.get("linear.capacity_monitoring", {})
    if isinstance(cap_fields, dict):
        # Resolve effective values: use update if present, fall back to config
        cap_cfg = config.linear.capacity_monitoring if config else None

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
                "linear.capacity_monitoring.resume_threshold must be less than pause_threshold"
            )

        # warning_threshold <= critical_threshold <= pause_threshold
        if warning is not None and critical is not None and warning > critical:
            errors.append(
                "linear.capacity_monitoring.warning_threshold must be "
                "less than or equal to critical_threshold"
            )
        if critical is not None and pause is not None and critical > pause:
            errors.append(
                "linear.capacity_monitoring.critical_threshold must be "
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
        "linear.capacity_monitoring": config.linear.capacity_monitoring,
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
        # Support dotted section paths like "linear.capacity_monitoring"
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
    """Validate structural config updates (projects and notifications).

    ``updates`` may contain a ``"notifications"`` dict and/or a
    ``"projects"`` list.  Returns a list of error messages (empty = valid).
    """
    errors: list[str] = []

    if "notifications" in updates:
        notif = updates["notifications"]
        if not isinstance(notif, dict):
            errors.append("'notifications' must be a mapping")
        else:
            for key, value in notif.items():
                path = ("notifications", key)
                spec = STRUCTURAL_FIELDS.get(path)
                if spec is None:
                    errors.append(
                        f"'notifications.{key}' is not an editable field"
                    )
                    continue
                errors.extend(
                    _validate_structural_field("notifications", key, value, spec)
                )

    if "projects" in updates:
        errors.extend(_validate_project_updates(updates["projects"], config))

    return errors


def _validate_structural_field(
    section: str, key: str, value: object, spec: dict,
) -> list[str]:
    """Validate a single structural field value against its spec."""
    errors: list[str] = []
    field_name = f"{section}.{key}"

    if spec["type"] == "str":
        if not isinstance(value, str):
            errors.append(
                f"'{field_name}' must be a string, got {type(value).__name__}"
            )

    elif spec["type"] == "int":
        if not isinstance(value, int) or isinstance(value, bool):
            errors.append(
                f"'{field_name}' must be an integer, got {type(value).__name__}"
            )
        elif "min" in spec and value < spec["min"]:
            errors.append(f"'{field_name}' must be at least {spec['min']}")

    elif spec["type"] == "choice":
        if value not in spec["choices"]:
            errors.append(
                f"'{field_name}' must be one of {spec['choices']}, "
                f"got {value!r}"
            )

    return errors


def _validate_project_updates(
    project_updates: object, config: BotfarmConfig,
) -> list[str]:
    """Validate project-level structural updates (slots, linear_project)."""
    errors: list[str] = []

    if not isinstance(project_updates, list):
        errors.append("'projects' must be a list")
        return errors

    existing_names = {p.name for p in config.projects}

    for i, proj in enumerate(project_updates):
        if not isinstance(proj, dict):
            errors.append(f"projects[{i}]: must be a mapping")
            continue

        name = proj.get("name")
        if not name or not isinstance(name, str):
            errors.append(f"projects[{i}]: 'name' is required")
            continue

        if name not in existing_names:
            errors.append(
                f"projects[{i}]: project '{name}' does not exist"
            )
            continue

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

        if "linear_project" in proj:
            lp = proj["linear_project"]
            if not isinstance(lp, str):
                errors.append(
                    f"projects[{i}] '{name}': linear_project must be a string"
                )

        allowed_keys = {"name", "slots", "linear_project"}
        extra = set(proj.keys()) - allowed_keys
        if extra:
            errors.append(
                f"projects[{i}] '{name}': "
                f"cannot edit fields: {sorted(extra)}"
            )

    return errors


def write_structural_config_updates(config_path: Path, updates: dict) -> None:
    """Write structural config changes to YAML without mutating in-memory config.

    Handles ``"notifications"`` (simple key-value merge) and ``"projects"``
    (merge editable fields into existing project entries matched by name).
    """
    raw = config_path.read_text()
    data = yaml.safe_load(raw)
    if not isinstance(data, dict):
        raise ConfigError("Config file must contain a YAML mapping")

    if "notifications" in updates:
        if "notifications" not in data:
            data["notifications"] = {}
        if isinstance(data["notifications"], dict):
            data["notifications"].update(updates["notifications"])

    if "projects" in updates:
        existing_projects = data.get("projects", [])
        by_name = {p["name"]: p for p in existing_projects if isinstance(p, dict)}
        for proj_update in updates["projects"]:
            name = proj_update["name"]
            if name in by_name:
                target = by_name[name]
                if "slots" in proj_update:
                    target["slots"] = proj_update["slots"]
                if "linear_project" in proj_update:
                    target["linear_project"] = proj_update["linear_project"]

    write_yaml_atomic(config_path, data)
