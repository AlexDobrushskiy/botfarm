"""Tests for botfarm.config module."""

import os
from pathlib import Path

import pytest
import yaml

from botfarm.config import (
    AgentsConfig,
    BotfarmConfig,
    CapacityConfig,
    CoderIdentity,
    ConfigError,
    IdentitiesConfig,
    LinearConfig,
    LoggingConfig,
    NotificationsConfig,
    ProjectConfig,
    ReviewerIdentity,
    UsageLimitsConfig,
    apply_config_updates,
    create_default_config,
    expand_env_vars,
    load_config,
    resolve_stage_timeout,
    validate_config_updates,
    validate_structural_config_updates,
    write_config_updates,
    write_structural_config_updates,
)


# --- Minimal valid config for reuse ---

MINIMAL_CONFIG = {
    "projects": [
        {
            "name": "test-project",
            "team": "TST",
            "base_dir": "~/test",
            "worktree_prefix": "test-slot-",
            "slots": [1],
        }
    ],
    "bugtracker": {"api_key": "test-key"},
}


def _write_config(tmp_path: Path, data: dict) -> Path:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(data))
    return config_path


# --- expand_env_vars ---


def test_expand_env_vars_braces(monkeypatch):
    monkeypatch.setenv("MY_VAR", "hello")
    assert expand_env_vars("${MY_VAR}") == "hello"


def test_expand_env_vars_bare_dollar_not_expanded(monkeypatch):
    monkeypatch.setenv("MY_VAR", "hello")
    assert expand_env_vars("prefix-$MY_VAR-suffix") == "prefix-$MY_VAR-suffix"


def test_expand_env_vars_missing_raises(monkeypatch):
    key = "BOTFARM_TEST_MISSING_VAR_12345"
    monkeypatch.delenv(key, raising=False)
    with pytest.raises(ConfigError, match="not set"):
        expand_env_vars(f"${{{key}}}")


def test_expand_env_vars_no_vars():
    assert expand_env_vars("plain string") == "plain string"


def test_expand_env_vars_multiple(monkeypatch):
    monkeypatch.setenv("A", "1")
    monkeypatch.setenv("B", "2")
    assert expand_env_vars("${A}-${B}") == "1-2"


# --- create_default_config ---


def test_create_default_config(tmp_path):
    config_path = tmp_path / "sub" / "config.yaml"
    result = create_default_config(config_path)
    assert result == config_path
    assert config_path.exists()
    content = config_path.read_text()
    assert "add-project" in content
    assert "bugtracker:" in content


def test_create_default_config_no_overwrite(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("existing content")
    create_default_config(config_path)
    assert config_path.read_text() == "existing content"


# --- load_config ---


def test_load_config_minimal(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert isinstance(config, BotfarmConfig)
    assert len(config.projects) == 1
    assert config.projects[0].name == "test-project"
    assert config.projects[0].slots == [1]


def test_load_config_file_not_found(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nonexistent.yaml")


def test_load_config_invalid_yaml(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("- not a mapping")
    with pytest.raises(ConfigError, match="YAML mapping"):
        load_config(config_path)


def test_load_config_yaml_syntax_error(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("projects:\n  - name: test\n    bad: [unclosed")
    with pytest.raises(ConfigError, match="YAML syntax error") as exc_info:
        load_config(config_path)
    assert "line" in str(exc_info.value)


def test_load_config_yaml_syntax_error_tab(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("projects:\n\t- name: test\n")
    with pytest.raises(ConfigError, match="YAML syntax error"):
        load_config(config_path)


def test_load_config_missing_projects(tmp_path):
    config_path = _write_config(tmp_path, {"bugtracker": {"api_key": "k"}})
    with pytest.raises(ConfigError, match="projects"):
        load_config(config_path)


def test_load_config_empty_projects(tmp_path):
    config_path = _write_config(
        tmp_path, {"projects": [], "bugtracker": {"api_key": "k"}}
    )
    with pytest.raises(ConfigError, match="At least one project"):
        load_config(config_path)


def test_load_config_project_missing_fields(tmp_path):
    data = {
        "projects": [{"name": "incomplete"}],
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="missing required fields"):
        load_config(config_path)


def test_load_config_slots_not_integers(tmp_path):
    data = {
        "projects": [
            {
                "name": "bad-slots",
                "team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": ["a", "b"],
            }
        ],
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="list of integers"):
        load_config(config_path)


def test_load_config_duplicate_slots(tmp_path):
    data = {
        "projects": [
            {
                "name": "dup-slots",
                "team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1, 1],
            }
        ],
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="duplicate"):
        load_config(config_path)


def test_load_config_duplicate_project_names(tmp_path):
    data = {
        "projects": [
            {
                "name": "same-name",
                "team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1],
            },
            {
                "name": "same-name",
                "team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [2],
            },
        ],
        "bugtracker": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="Duplicate project names"):
        load_config(config_path)


def test_load_config_duplicate_tracker_project(tmp_path):
    data = {
        "projects": [
            {
                "name": "proj-a",
                "team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1],
                "tracker_project": "Shared Project",
            },
            {
                "name": "proj-b",
                "team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [2],
                "tracker_project": "Shared Project",
            },
        ],
        "bugtracker": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="Duplicate tracker_project filter"):
        load_config(config_path)


def test_load_config_duplicate_tracker_project_empty_ok(tmp_path):
    """Empty tracker_project on multiple projects should not trigger duplicate error."""
    data = {
        "projects": [
            {
                "name": "proj-a",
                "team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1],
            },
            {
                "name": "proj-b",
                "team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [2],
            },
        ],
        "bugtracker": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert len(config.projects) == 2


def test_load_config_different_tracker_projects_ok(tmp_path):
    """Different tracker_project values across projects should be fine."""
    data = {
        "projects": [
            {
                "name": "proj-a",
                "team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1],
                "tracker_project": "Project Alpha",
            },
            {
                "name": "proj-b",
                "team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [2],
                "tracker_project": "Project Beta",
            },
        ],
        "bugtracker": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.projects[0].tracker_project == "Project Alpha"
    assert config.projects[1].tracker_project == "Project Beta"


def test_load_config_env_var_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_API_KEY", "sk-test-123")
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {"api_key": "${TEST_API_KEY}"},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.bugtracker.api_key == "sk-test-123"


def test_load_config_full(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_KEY", "key123")
    data = {
        "projects": [
            {
                "name": "proj-a",
                "team": "SMA",
                "base_dir": "~/proj-a",
                "worktree_prefix": "proj-a-slot-",
                "slots": [1, 2],
            }
        ],
        "bugtracker": {
            "api_key": "${TEST_KEY}",
            "poll_interval_seconds": 60,
            "exclude_tags": ["Human", "Manual"],
        },
        "database": {
            "path": "~/.botfarm/custom.db",
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)

    assert config.bugtracker.api_key == "key123"
    assert config.bugtracker.poll_interval_seconds == 60
    assert config.bugtracker.exclude_tags == ["Human", "Manual"]
    assert config.database.path == "~/.botfarm/custom.db"


def test_load_config_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)

    assert config.bugtracker.poll_interval_seconds == 30
    assert config.bugtracker.exclude_tags == ["Human"]
    assert config.database.path == ""


def test_load_config_poll_interval_zero(tmp_path):
    data = {**MINIMAL_CONFIG, "bugtracker": {"api_key": "k", "poll_interval_seconds": 0}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="at least 1"):
        load_config(config_path)


def test_load_config_empty_api_key(tmp_path):
    data = {**MINIMAL_CONFIG, "bugtracker": {"api_key": ""}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="api_key must be set"):
        load_config(config_path)


def test_load_config_missing_api_key(tmp_path):
    data = {**MINIMAL_CONFIG, "bugtracker": {}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="api_key must be set"):
        load_config(config_path)


def test_load_config_cross_project_duplicate_slots_allowed(tmp_path):
    """Slot IDs are per-project, so the same ID can appear in different projects."""
    data = {
        "projects": [
            {
                "name": "project-a",
                "team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1, 2],
            },
            {
                "name": "project-b",
                "team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [1, 2],
            },
        ],
        "bugtracker": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert len(config.projects) == 2
    assert config.projects[0].slots == [1, 2]
    assert config.projects[1].slots == [1, 2]


# --- tracker_project config ---


def test_load_config_tracker_project_default(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.projects[0].tracker_project == ""


def test_load_config_tracker_project_set(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "projects": [
            {
                "name": "test-project",
                "team": "TST",
                "tracker_project": "My Project",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1],
            }
        ],
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.projects[0].tracker_project == "My Project"


# --- CLI init command ---


# --- usage_limits config ---


def test_load_config_usage_limits_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.usage_limits.enabled is True
    assert config.usage_limits.pause_five_hour_threshold == 0.85
    assert config.usage_limits.pause_seven_day_threshold == 0.90


def test_load_config_usage_limits_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "usage_limits": {
            "pause_five_hour_threshold": 0.75,
            "pause_seven_day_threshold": 0.80,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.usage_limits.pause_five_hour_threshold == 0.75
    assert config.usage_limits.pause_seven_day_threshold == 0.80


def test_load_config_usage_limits_enabled_false(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "usage_limits": {
            "enabled": False,
            "pause_five_hour_threshold": 0.85,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.usage_limits.enabled is False


def test_load_config_usage_limits_out_of_range(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "usage_limits": {
            "pause_five_hour_threshold": 1.5,
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="between 0.0 and 1.0"):
        load_config(config_path)


def test_load_config_usage_limits_negative(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "usage_limits": {
            "pause_seven_day_threshold": -0.1,
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="between 0.0 and 1.0"):
        load_config(config_path)


# --- Agents config ---


def test_load_config_agents_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.agents.max_review_iterations == 3


def test_load_config_agents_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"max_review_iterations": 5},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.max_review_iterations == 5


def test_load_config_agents_invalid_zero(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"max_review_iterations": 0},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="max_review_iterations must be at least 1"):
        load_config(config_path)


def test_load_config_agents_max_ci_retries_default(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.agents.max_ci_retries == 2


def test_load_config_agents_max_ci_retries_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"max_ci_retries": 5},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.max_ci_retries == 5


def test_load_config_agents_max_ci_retries_zero_allowed(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"max_ci_retries": 0},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.max_ci_retries == 0


def test_load_config_agents_max_ci_retries_negative(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"max_ci_retries": -1},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="max_ci_retries must be at least 0"):
        load_config(config_path)


# --- Timeout config ---


def test_load_config_timeout_minutes_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.agents.timeout_minutes == {
        "implement": 120,
        "review": 30,
        "fix": 60,
        "resolve_conflict": 60,
    }
    assert config.agents.timeout_grace_seconds == 10


def test_load_config_timeout_minutes_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"timeout_minutes": {"implement": 60, "review": 15}},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.timeout_minutes["implement"] == 60
    assert config.agents.timeout_minutes["review"] == 15
    # fix keeps its default
    assert config.agents.timeout_minutes["fix"] == 60


def test_load_config_timeout_minutes_zero_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"timeout_minutes": {"implement": 0}},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="timeout_minutes.implement must be at least 1"):
        load_config(config_path)


def test_load_config_timeout_minutes_unknown_stage_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"timeout_minutes": {"implment": 60}},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="unknown stage 'implment'"):
        load_config(config_path)


def test_load_config_timeout_grace_seconds_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"timeout_grace_seconds": 30},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.timeout_grace_seconds == 30


def test_load_config_timeout_grace_seconds_negative_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {"timeout_grace_seconds": -1},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="timeout_grace_seconds must be at least 0"):
        load_config(config_path)


# --- Timeout overrides config ---


def test_load_config_timeout_overrides_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.agents.timeout_overrides == {}


def test_load_config_timeout_overrides_valid(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "timeout_overrides": {
                "Investigation": {"implement": 30},
                "Quick": {"implement": 15, "review": 10},
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.timeout_overrides == {
        "Investigation": {"implement": 30},
        "Quick": {"implement": 15, "review": 10},
    }


def test_load_config_timeout_overrides_string_values_coerced(tmp_path):
    """Regression: env-expanded or quoted YAML values arrive as strings and must be coerced to int."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "timeout_overrides": {
                "Investigation": {"implement": "30", "review": "10"},
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.timeout_overrides == {
        "Investigation": {"implement": 30, "review": 10},
    }


def test_load_config_timeout_overrides_unknown_stage_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "timeout_overrides": {
                "Investigation": {"implment": 30},
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="unknown stage 'implment'"):
        load_config(config_path)


def test_load_config_timeout_overrides_zero_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "timeout_overrides": {
                "Investigation": {"implement": 0},
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="must be at least 1"):
        load_config(config_path)


def test_load_config_timeout_overrides_non_int_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "timeout_overrides": {
                "Investigation": {"implement": "thirty"},
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="timeout_overrides"):
        load_config(config_path)


def test_load_config_timeout_overrides_non_dict_value_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "timeout_overrides": {
                "Investigation": 30,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="timeout_overrides.Investigation"):
        load_config(config_path)


def test_load_config_timeout_overrides_non_dict_root_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "timeout_overrides": "Investigation",
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="timeout_overrides must be a mapping"):
        load_config(config_path)


# --- resolve_stage_timeout ---


def test_resolve_stage_timeout_no_labels():
    cfg = AgentsConfig(timeout_minutes={"implement": 120, "review": 30})
    assert resolve_stage_timeout(cfg, "implement") == 120
    assert resolve_stage_timeout(cfg, "review") == 30
    assert resolve_stage_timeout(cfg, "merge") is None


def test_resolve_stage_timeout_no_overrides():
    cfg = AgentsConfig(timeout_minutes={"implement": 120})
    assert resolve_stage_timeout(cfg, "implement", ["Bug", "Feature"]) == 120


def test_resolve_stage_timeout_label_override():
    cfg = AgentsConfig(
        timeout_minutes={"implement": 120, "review": 30},
        timeout_overrides={"Investigation": {"implement": 30}},
    )
    assert resolve_stage_timeout(cfg, "implement", ["Investigation"]) == 30
    # Non-overridden stage falls back to default
    assert resolve_stage_timeout(cfg, "review", ["Investigation"]) == 30


def test_resolve_stage_timeout_case_insensitive():
    cfg = AgentsConfig(
        timeout_minutes={"implement": 120},
        timeout_overrides={"Investigation": {"implement": 30}},
    )
    assert resolve_stage_timeout(cfg, "implement", ["investigation"]) == 30
    assert resolve_stage_timeout(cfg, "implement", ["INVESTIGATION"]) == 30


def test_resolve_stage_timeout_no_matching_label():
    cfg = AgentsConfig(
        timeout_minutes={"implement": 120},
        timeout_overrides={"Investigation": {"implement": 30}},
    )
    assert resolve_stage_timeout(cfg, "implement", ["Bug"]) == 120


def test_resolve_stage_timeout_first_match_wins():
    cfg = AgentsConfig(
        timeout_minutes={"implement": 120},
        timeout_overrides={
            "Investigation": {"implement": 30},
            "Quick": {"implement": 15},
        },
    )
    # Both labels match — first override key wins
    assert resolve_stage_timeout(cfg, "implement", ["Quick", "Investigation"]) == 30


# --- Linear status and comment config ---


def test_load_config_linear_status_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.bugtracker.todo_status == "Todo"
    assert config.bugtracker.in_progress_status == "In Progress"
    assert config.bugtracker.done_status == "Done"
    assert config.bugtracker.in_review_status == "In Review"


def test_load_config_linear_status_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            "api_key": "test-key",
            "todo_status": "Backlog",
            "in_progress_status": "Working",
            "done_status": "Shipped",
            "in_review_status": "Review",
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.bugtracker.todo_status == "Backlog"
    assert config.bugtracker.in_progress_status == "Working"
    assert config.bugtracker.done_status == "Shipped"
    assert config.bugtracker.in_review_status == "Review"


def test_load_config_failed_status_deprecated(tmp_path):
    """Setting failed_status in config should not error — it's just ignored."""
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            "api_key": "test-key",
            "failed_status": "Needs Attention",
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert not hasattr(config.bugtracker, "failed_status")


def test_load_config_comment_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.bugtracker.comment_on_failure is True
    assert config.bugtracker.comment_on_completion is False
    assert config.bugtracker.comment_on_limit_pause is False


def test_load_config_comment_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            "api_key": "test-key",
            "comment_on_failure": False,
            "comment_on_completion": True,
            "comment_on_limit_pause": True,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.bugtracker.comment_on_failure is False
    assert config.bugtracker.comment_on_completion is True
    assert config.bugtracker.comment_on_limit_pause is True


def test_load_config_comment_rejects_string_bool(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            "api_key": "test-key",
            "comment_on_failure": "false",
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="must be a boolean"):
        load_config(config_path)


def test_default_config_template_includes_status_and_comment_fields():
    from botfarm.config import DEFAULT_CONFIG_TEMPLATE
    assert "todo_status:" in DEFAULT_CONFIG_TEMPLATE
    assert "in_progress_status:" in DEFAULT_CONFIG_TEMPLATE
    assert "done_status:" in DEFAULT_CONFIG_TEMPLATE
    assert "failed_status:" not in DEFAULT_CONFIG_TEMPLATE
    assert "comment_on_failure:" in DEFAULT_CONFIG_TEMPLATE
    assert "comment_on_completion:" in DEFAULT_CONFIG_TEMPLATE
    assert "comment_on_limit_pause:" in DEFAULT_CONFIG_TEMPLATE


# --- CLI init command ---


def test_cli_init_non_interactive(tmp_path):
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    runner = CliRunner()
    result = runner.invoke(main, ["init", "--non-interactive", "--path", str(config_path)])
    assert result.exit_code == 0
    assert "Created default config" in result.output
    assert config_path.exists()


def test_cli_init_non_interactive_already_exists(tmp_path):
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    config_path.write_text("existing")
    runner = CliRunner()
    result = runner.invoke(main, ["init", "--non-interactive", "--path", str(config_path)])
    assert result.exit_code == 0
    assert "already exists" in result.output


def test_cli_init_with_flags(tmp_path, monkeypatch):
    """Non-interactive init with --linear-api-key, --team, --workspace."""
    from click.testing import CliRunner

    from botfarm.cli import ENV_FILE_PATH, main

    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"
    monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

    runner = CliRunner()
    result = runner.invoke(main, [
        "init",
        "--path", str(config_path),
        "--linear-api-key", "lin_api_test123",
        "--team", "SMA",
        "--workspace", "my-workspace",
    ])
    assert result.exit_code == 0, result.output
    assert "Created config at:" in result.output
    assert "LINEAR_API_KEY" in result.output
    assert config_path.exists()
    assert env_path.exists()

    # Verify config content has substituted values
    content = config_path.read_text()
    assert "SMA" in content
    assert "my-workspace" in content

    # Verify .env has the API key
    env_content = env_path.read_text()
    assert "LINEAR_API_KEY=lin_api_test123" in env_content


def test_cli_init_with_flags_reads_env_var(tmp_path, monkeypatch):
    """--linear-api-key falls back to LINEAR_API_KEY env var."""
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    env_path = tmp_path / ".env"
    monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
    monkeypatch.setenv("LINEAR_API_KEY", "lin_api_from_env")

    runner = CliRunner()
    result = runner.invoke(main, [
        "init",
        "--path", str(config_path),
        "--team", "ABC",
        "--workspace", "ws-slug",
    ])
    assert result.exit_code == 0, result.output
    assert config_path.exists()

    env_content = env_path.read_text()
    assert "LINEAR_API_KEY=lin_api_from_env" in env_content


def test_cli_init_with_flags_missing_api_key(tmp_path, monkeypatch):
    """Error when no API key provided and env var not set."""
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    # Prevent load_dotenv from loading the real .env
    monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", tmp_path / "nonexistent.env")
    monkeypatch.delenv("LINEAR_API_KEY", raising=False)

    runner = CliRunner()
    result = runner.invoke(main, [
        "init",
        "--path", str(config_path),
        "--team", "SMA",
        "--workspace", "ws",
    ])
    assert result.exit_code != 0
    assert "Linear API key required" in result.output


def test_cli_init_with_flags_missing_team(tmp_path):
    """Error when --team not provided with other flags."""
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    runner = CliRunner()
    result = runner.invoke(main, [
        "init",
        "--path", str(config_path),
        "--linear-api-key", "lin_api_test",
        "--workspace", "ws",
    ])
    assert result.exit_code != 0
    assert "--team is required" in result.output


def test_cli_init_with_flags_missing_workspace(tmp_path):
    """Error when --workspace not provided with other flags."""
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    runner = CliRunner()
    result = runner.invoke(main, [
        "init",
        "--path", str(config_path),
        "--linear-api-key", "lin_api_test",
        "--team", "SMA",
    ])
    assert result.exit_code != 0
    assert "--workspace is required" in result.output


def test_cli_init_with_flags_config_exists(tmp_path, monkeypatch):
    """Non-interactive with flags does not overwrite existing config."""
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    config_path.write_text("existing")
    env_path = tmp_path / ".env"
    monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)

    runner = CliRunner()
    result = runner.invoke(main, [
        "init",
        "--path", str(config_path),
        "--linear-api-key", "lin_api_test",
        "--team", "SMA",
        "--workspace", "ws",
    ])
    assert result.exit_code == 0
    assert "already exists" in result.output
    assert config_path.read_text() == "existing"
    # .env should still be written even when config already exists
    assert env_path.exists()
    assert "lin_api_test" in env_path.read_text()


# --- source_path ---


def test_load_config_sets_source_path(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.source_path == str(config_path)


# --- logging config ---


def test_load_config_logging_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.logging.max_bytes == 10 * 1024 * 1024
    assert config.logging.backup_count == 5
    assert config.logging.ticket_log_retention_days == 30


def test_load_config_logging_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "logging": {
            "max_bytes": 5000000,
            "backup_count": 3,
            "ticket_log_retention_days": 7,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.logging.max_bytes == 5000000
    assert config.logging.backup_count == 3
    assert config.logging.ticket_log_retention_days == 7


def test_validate_logging_max_bytes_zero(tmp_path):
    data = {**MINIMAL_CONFIG, "logging": {"max_bytes": 0}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="max_bytes"):
        load_config(config_path)


def test_validate_logging_backup_count_negative(tmp_path):
    data = {**MINIMAL_CONFIG, "logging": {"backup_count": -1}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="backup_count"):
        load_config(config_path)


def test_validate_logging_retention_days_zero(tmp_path):
    data = {**MINIMAL_CONFIG, "logging": {"ticket_log_retention_days": 0}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="ticket_log_retention_days"):
        load_config(config_path)


# --- unknown key warnings ---


def test_load_config_warns_on_unknown_keys(tmp_path, caplog):
    data = {**MINIMAL_CONFIG, "max_total_slots": 2, "bogus_key": "value"}
    config_path = _write_config(tmp_path, data)
    import logging
    with caplog.at_level(logging.WARNING, logger="botfarm.config"):
        config = load_config(config_path)
    assert "Unknown config keys" in caplog.text
    assert "bogus_key" in caplog.text
    assert "max_total_slots" in caplog.text
    # Config still loads successfully
    assert isinstance(config, BotfarmConfig)


def test_load_config_no_warning_for_known_keys(tmp_path, caplog):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    import logging
    with caplog.at_level(logging.WARNING, logger="botfarm.config"):
        load_config(config_path)
    assert "Unknown config keys" not in caplog.text


# --- validate_config_updates ---


class TestValidateConfigUpdates:
    def test_valid_linear_updates(self):
        updates = {
            "bugtracker": {
                "poll_interval_seconds": 60,
                "comment_on_failure": False,
            },
        }
        assert validate_config_updates(updates) == []

    def test_valid_usage_limits_updates(self):
        updates = {
            "usage_limits": {
                "pause_five_hour_threshold": 0.75,
                "pause_seven_day_threshold": 0.80,
            },
        }
        assert validate_config_updates(updates) == []

    def test_valid_agents_updates(self):
        updates = {
            "agents": {
                "max_review_iterations": 5,
                "max_ci_retries": 0,
                "timeout_minutes": {"implement": 90, "review": 15},
                "timeout_grace_seconds": 30,
            },
        }
        assert validate_config_updates(updates) == []

    def test_non_editable_field_rejected(self):
        updates = {"bugtracker": {"api_key": "new-key"}}
        errors = validate_config_updates(updates)
        assert any("not an editable field" in e for e in errors)

    def test_unknown_section_field_rejected(self):
        updates = {"database": {"path": "/tmp/db"}}
        errors = validate_config_updates(updates)
        assert len(errors) > 0

    def test_section_not_mapping(self):
        updates = {"bugtracker": "not a dict"}
        errors = validate_config_updates(updates)
        assert any("must be a mapping" in e for e in errors)

    def test_poll_interval_too_low(self):
        updates = {"bugtracker": {"poll_interval_seconds": 0}}
        errors = validate_config_updates(updates)
        assert any("at least 1" in e for e in errors)

    def test_bool_field_rejects_string(self):
        updates = {"bugtracker": {"comment_on_failure": "true"}}
        errors = validate_config_updates(updates)
        assert any("boolean" in e for e in errors)

    def test_int_field_rejects_bool(self):
        updates = {"bugtracker": {"poll_interval_seconds": True}}
        errors = validate_config_updates(updates)
        assert any("integer" in e for e in errors)

    def test_float_out_of_range(self):
        updates = {"usage_limits": {"pause_five_hour_threshold": 1.5}}
        errors = validate_config_updates(updates)
        assert any("at most 1.0" in e for e in errors)

    def test_float_negative(self):
        updates = {"usage_limits": {"pause_five_hour_threshold": -0.1}}
        errors = validate_config_updates(updates)
        assert any("at least 0.0" in e for e in errors)

    def test_timeout_unknown_stage(self):
        updates = {"agents": {"timeout_minutes": {"bogus": 60}}}
        errors = validate_config_updates(updates)
        assert any("unknown stage" in e for e in errors)

    def test_timeout_minutes_too_low(self):
        updates = {"agents": {"timeout_minutes": {"implement": 0}}}
        errors = validate_config_updates(updates)
        assert any("at least 1" in e for e in errors)

    def test_timeout_minutes_not_dict(self):
        updates = {"agents": {"timeout_minutes": 60}}
        errors = validate_config_updates(updates)
        assert any("mapping" in e for e in errors)

    def test_max_review_iterations_too_low(self):
        updates = {"agents": {"max_review_iterations": 0}}
        errors = validate_config_updates(updates)
        assert any("at least 1" in e for e in errors)

    def test_max_ci_retries_negative(self):
        updates = {"agents": {"max_ci_retries": -1}}
        errors = validate_config_updates(updates)
        assert any("at least 0" in e for e in errors)

    def test_int_accepts_zero_for_retries(self):
        updates = {"agents": {"max_ci_retries": 0}}
        assert validate_config_updates(updates) == []

    def test_float_accepts_int_value(self):
        updates = {"usage_limits": {"pause_five_hour_threshold": 1}}
        assert validate_config_updates(updates) == []


# --- apply_config_updates ---


class TestApplyConfigUpdates:
    def _make_config(self):
        return BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test", team="T", base_dir="~/t",
                    worktree_prefix="t-", slots=[1],
                ),
            ],
        )

    def test_apply_linear_updates(self):
        config = self._make_config()
        apply_config_updates(config, {
            "bugtracker": {"poll_interval_seconds": 60, "comment_on_failure": False},
        })
        assert config.bugtracker.poll_interval_seconds == 60
        assert config.bugtracker.comment_on_failure is False

    def test_apply_usage_limits(self):
        config = self._make_config()
        apply_config_updates(config, {
            "usage_limits": {"pause_five_hour_threshold": 0.5},
        })
        assert config.usage_limits.pause_five_hour_threshold == 0.5

    def test_apply_agents_timeout_merges(self):
        config = self._make_config()
        original_fix = config.agents.timeout_minutes.get("fix")
        apply_config_updates(config, {
            "agents": {"timeout_minutes": {"implement": 90}},
        })
        assert config.agents.timeout_minutes["implement"] == 90
        # Other stages unchanged
        assert config.agents.timeout_minutes.get("fix") == original_fix

    def test_apply_coerces_float(self):
        config = self._make_config()
        apply_config_updates(config, {
            "usage_limits": {"pause_five_hour_threshold": 1},
        })
        assert isinstance(config.usage_limits.pause_five_hour_threshold, float)

    def test_apply_unknown_section_ignored(self):
        config = self._make_config()
        apply_config_updates(config, {"unknown": {"key": "value"}})
        # No error, config unchanged


# --- write_config_updates ---


class TestWriteConfigUpdates:
    def test_basic_write(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_config_updates(config_path, {
            "bugtracker": {"poll_interval_seconds": 60},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["bugtracker"]["poll_interval_seconds"] == 60

    def test_preserves_env_var_refs(self, tmp_path):
        """Env var references like ${API_KEY} must survive write-back."""
        raw_config = {
            **MINIMAL_CONFIG,
            "bugtracker": {"api_key": "${LINEAR_API_KEY}", "poll_interval_seconds": 120},
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(raw_config, sort_keys=False))

        write_config_updates(config_path, {
            "bugtracker": {"poll_interval_seconds": 60},
        })

        data = yaml.safe_load(config_path.read_text())
        assert data["bugtracker"]["api_key"] == "${LINEAR_API_KEY}"
        assert data["bugtracker"]["poll_interval_seconds"] == 60

    def test_timeout_minutes_merges(self, tmp_path):
        data = {
            **MINIMAL_CONFIG,
            "agents": {"timeout_minutes": {"implement": 120, "review": 30}},
        }
        config_path = _write_config(tmp_path, data)
        write_config_updates(config_path, {
            "agents": {"timeout_minutes": {"implement": 90}},
        })
        result = yaml.safe_load(config_path.read_text())
        assert result["agents"]["timeout_minutes"]["implement"] == 90
        assert result["agents"]["timeout_minutes"]["review"] == 30

    def test_creates_section_if_missing(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_config_updates(config_path, {
            "agents": {"max_ci_retries": 5},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["agents"]["max_ci_retries"] == 5

    def test_atomic_write_no_corruption(self, tmp_path):
        """File should be fully valid YAML after write."""
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_config_updates(config_path, {
            "bugtracker": {"poll_interval_seconds": 60},
            "agents": {"max_review_iterations": 5},
        })
        # Should parse without error
        data = yaml.safe_load(config_path.read_text())
        assert data["bugtracker"]["poll_interval_seconds"] == 60
        assert data["agents"]["max_review_iterations"] == 5


# --- Structural config validation ---


def _make_config_for_structural():
    """Create a BotfarmConfig for structural validation tests."""
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="project-a", team="TST",
                base_dir="~/a", worktree_prefix="a-slot-",
                slots=[1, 2],
            ),
            ProjectConfig(
                name="project-b", team="TST",
                base_dir="~/b", worktree_prefix="b-slot-",
                slots=[3], tracker_project="My Filter",
            ),
        ],
        notifications=NotificationsConfig(
            webhook_url="https://hooks.example.com/test",
            webhook_format="slack",
            rate_limit_seconds=300,
        ),
    )


class TestValidateStructuralConfigUpdates:
    def test_valid_notifications(self):
        config = _make_config_for_structural()
        updates = {
            "notifications": {
                "webhook_url": "https://new-url.example.com",
                "webhook_format": "discord",
                "rate_limit_seconds": 60,
            },
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_valid_projects(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {"name": "project-a", "slots": [1, 2, 3]},
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_valid_project_tracker_project(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {"name": "project-b", "tracker_project": "New Filter"},
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_notifications_not_mapping(self):
        config = _make_config_for_structural()
        updates = {"notifications": "bad"}
        errors = validate_structural_config_updates(updates, config)
        assert any("must be a mapping" in e for e in errors)

    def test_notifications_unknown_field(self):
        config = _make_config_for_structural()
        updates = {"notifications": {"unknown_field": "val"}}
        errors = validate_structural_config_updates(updates, config)
        assert any("not an editable field" in e for e in errors)

    def test_notifications_invalid_format(self):
        config = _make_config_for_structural()
        updates = {"notifications": {"webhook_format": "teams"}}
        errors = validate_structural_config_updates(updates, config)
        assert any("must be one of" in e for e in errors)

    def test_notifications_rate_limit_negative(self):
        config = _make_config_for_structural()
        updates = {"notifications": {"rate_limit_seconds": -1}}
        errors = validate_structural_config_updates(updates, config)
        assert any("at least 0" in e for e in errors)

    def test_notifications_webhook_url_not_string(self):
        config = _make_config_for_structural()
        updates = {"notifications": {"webhook_url": 123}}
        errors = validate_structural_config_updates(updates, config)
        assert any("must be a string" in e for e in errors)

    def test_projects_not_list(self):
        config = _make_config_for_structural()
        updates = {"projects": "bad"}
        errors = validate_structural_config_updates(updates, config)
        assert any("must be a list" in e for e in errors)

    def test_projects_unknown_project_missing_fields(self):
        config = _make_config_for_structural()
        updates = {"projects": [{"name": "nonexistent", "slots": [1]}]}
        errors = validate_structural_config_updates(updates, config)
        assert any("missing required fields" in e for e in errors)

    def test_projects_missing_name(self):
        config = _make_config_for_structural()
        updates = {"projects": [{"slots": [1]}]}
        errors = validate_structural_config_updates(updates, config)
        assert any("'name' is required" in e for e in errors)

    def test_projects_duplicate_slots(self):
        config = _make_config_for_structural()
        updates = {"projects": [{"name": "project-a", "slots": [1, 1]}]}
        errors = validate_structural_config_updates(updates, config)
        assert any("duplicate" in e for e in errors)

    def test_projects_slots_not_integers(self):
        config = _make_config_for_structural()
        updates = {"projects": [{"name": "project-a", "slots": ["a"]}]}
        errors = validate_structural_config_updates(updates, config)
        assert any("list of integers" in e for e in errors)

    def test_projects_slots_not_list(self):
        config = _make_config_for_structural()
        updates = {"projects": [{"name": "project-a", "slots": 1}]}
        errors = validate_structural_config_updates(updates, config)
        assert any("must be a list" in e for e in errors)

    def test_projects_tracker_project_not_string(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "tracker_project": 123}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("must be a string" in e for e in errors)

    def test_projects_reject_non_editable_fields(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "base_dir": "/tmp/hacked"}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("cannot edit" in e for e in errors)

    def test_projects_entry_not_mapping(self):
        config = _make_config_for_structural()
        updates = {"projects": ["bad"]}
        errors = validate_structural_config_updates(updates, config)
        assert any("must be a mapping" in e for e in errors)

    def test_projects_bool_rejected_as_slot(self):
        config = _make_config_for_structural()
        updates = {"projects": [{"name": "project-a", "slots": [True]}]}
        errors = validate_structural_config_updates(updates, config)
        assert any("list of integers" in e for e in errors)

    def test_projects_duplicate_tracker_project_via_update(self):
        config = _make_config_for_structural()
        # project-b already has tracker_project="My Filter"; updating
        # project-a to the same value should be rejected
        updates = {
            "projects": [
                {"name": "project-a", "tracker_project": "My Filter"},
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("Duplicate tracker_project" in e for e in errors)

    # --- New project creation validation ---

    def test_new_project_valid(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": "NEW",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1, 2],
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_new_project_with_tracker_project(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": "NEW",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1],
                    "tracker_project": "New Filter",
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_new_project_missing_required_fields(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {"name": "project-c", "slots": [1]},
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("missing required fields" in e for e in errors)

    def test_new_project_empty_team(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": "  ",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1],
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("must not be empty" in e for e in errors)

    def test_new_project_team_not_string(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": 123,
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1],
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("team must be a string" in e for e in errors)

    def test_new_project_unknown_field(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": "NEW",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1],
                    "unknown_field": "bad",
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("unknown fields" in e for e in errors)

    def test_new_project_duplicate_tracker_project(self):
        config = _make_config_for_structural()
        # project-b already has tracker_project="My Filter"
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": "NEW",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1],
                    "tracker_project": "My Filter",
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("Duplicate tracker_project" in e for e in errors)

    def test_new_project_duplicate_slots(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": "NEW",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1, 1],
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("duplicate" in e for e in errors)

    def test_new_project_duplicate_name_in_payload(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {
                    "name": "project-c",
                    "team": "NEW",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [1],
                },
                {
                    "name": "project-c",
                    "team": "NEW2",
                    "base_dir": "~/c2",
                    "worktree_prefix": "c2-slot-",
                    "slots": [2],
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert len(errors) == 1
        assert "duplicate project name" in errors[0]

    def test_mixed_new_and_existing_projects(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {"name": "project-a", "slots": [1, 2, 3]},
                {
                    "name": "project-c",
                    "team": "NEW",
                    "base_dir": "~/c",
                    "worktree_prefix": "c-slot-",
                    "slots": [4],
                },
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    # --- run_command / run_env / run_port validation ---

    def test_existing_project_run_command_editable(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {"name": "project-a", "run_command": "npm run dev"},
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_existing_project_run_env_editable(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {"name": "project-a", "run_env": {"NODE_ENV": "development"}},
            ],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_existing_project_run_port_editable(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "run_port": 3000}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert errors == []

    def test_run_command_not_string_rejected(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "run_command": 123}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("run_command must be a string" in e for e in errors)

    def test_run_env_not_dict_rejected(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "run_env": "bad"}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("run_env must be a mapping" in e for e in errors)

    def test_run_env_non_string_values_rejected(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "run_env": {"K": 1}}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("run_env must be a mapping" in e for e in errors)

    def test_run_port_not_int_rejected(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "run_port": "abc"}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("run_port must be an integer" in e for e in errors)

    def test_run_port_negative_rejected(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "run_port": -1}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("run_port must be a non-negative" in e for e in errors)

    def test_run_port_bool_rejected(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "run_port": True}],
        }
        errors = validate_structural_config_updates(updates, config)
        assert any("run_port must be an integer" in e for e in errors)


# --- write_structural_config_updates ---


class TestWriteStructuralConfigUpdates:
    def test_write_notifications(self, tmp_path):
        config_path = _write_config(tmp_path, {
            **MINIMAL_CONFIG,
            "notifications": {
                "webhook_url": "https://old.example.com",
                "webhook_format": "slack",
                "rate_limit_seconds": 300,
            },
        })
        write_structural_config_updates(config_path, {
            "notifications": {
                "webhook_url": "https://new.example.com",
                "rate_limit_seconds": 60,
            },
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["notifications"]["webhook_url"] == "https://new.example.com"
        assert data["notifications"]["rate_limit_seconds"] == 60
        # webhook_format preserved
        assert data["notifications"]["webhook_format"] == "slack"

    def test_write_notifications_creates_section(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["notifications"]["webhook_url"] == "https://new.example.com"

    def test_write_project_slots(self, tmp_path):
        config_path = _write_config(tmp_path, {
            **MINIMAL_CONFIG,
            "projects": [
                {
                    "name": "test-project",
                    "team": "TST",
                    "base_dir": "~/test",
                    "worktree_prefix": "test-slot-",
                    "slots": [1],
                },
            ],
        })
        write_structural_config_updates(config_path, {
            "projects": [{"name": "test-project", "slots": [1, 2, 3]}],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["slots"] == [1, 2, 3]
        # Other fields preserved
        assert data["projects"][0]["base_dir"] == "~/test"

    def test_write_project_tracker_project(self, tmp_path):
        config_path = _write_config(tmp_path, {
            **MINIMAL_CONFIG,
            "projects": [
                {
                    "name": "test-project",
                    "team": "TST",
                    "base_dir": "~/test",
                    "worktree_prefix": "test-slot-",
                    "slots": [1],
                },
            ],
        })
        write_structural_config_updates(config_path, {
            "projects": [
                {"name": "test-project", "tracker_project": "My Project"},
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["tracker_project"] == "My Project"

    def test_write_project_updates_tracker_project_key(self, tmp_path):
        """Sending tracker_project when YAML uses tracker_project — should update in place."""
        config_path = _write_config(tmp_path, {
            **MINIMAL_CONFIG,
            "projects": [
                {
                    "name": "test-project",
                    "team": "TST",
                    "base_dir": "~/test",
                    "worktree_prefix": "test-slot-",
                    "slots": [1],
                    "tracker_project": "Old",
                },
            ],
        })
        write_structural_config_updates(config_path, {
            "projects": [
                {"name": "test-project", "tracker_project": "New"},
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["tracker_project"] == "New"

    def test_write_project_unknown_name_ignored(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        original = yaml.safe_load(config_path.read_text())
        write_structural_config_updates(config_path, {
            "projects": [{"name": "nonexistent", "slots": [99]}],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"] == original["projects"]

    def test_write_preserves_env_vars(self, tmp_path):
        raw_config = {
            **MINIMAL_CONFIG,
            "bugtracker": {"api_key": "${LINEAR_API_KEY}"},
            "notifications": {"webhook_url": "https://old.example.com"},
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(raw_config, sort_keys=False))
        write_structural_config_updates(config_path, {
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["bugtracker"]["api_key"] == "${LINEAR_API_KEY}"

    def test_write_new_project(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {
                    "name": "new-project",
                    "team": "NEW",
                    "base_dir": "~/new",
                    "worktree_prefix": "new-slot-",
                    "slots": [1, 2],
                },
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert len(data["projects"]) == 2
        new = data["projects"][1]
        assert new["name"] == "new-project"
        assert new["team"] == "NEW"
        assert new["base_dir"] == "~/new"
        assert new["worktree_prefix"] == "new-slot-"
        assert new["slots"] == [1, 2]
        # Original project preserved
        assert data["projects"][0]["name"] == "test-project"

    def test_write_new_project_with_tracker_project(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {
                    "name": "new-project",
                    "team": "NEW",
                    "base_dir": "~/new",
                    "worktree_prefix": "new-slot-",
                    "slots": [1],
                    "tracker_project": "My New Filter",
                },
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        new = data["projects"][1]
        assert new["tracker_project"] == "My New Filter"

    def test_write_new_project_empty_tracker_project_omitted(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {
                    "name": "new-project",
                    "team": "NEW",
                    "base_dir": "~/new",
                    "worktree_prefix": "new-slot-",
                    "slots": [1],
                    "tracker_project": "",
                },
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        new = data["projects"][1]
        assert "tracker_project" not in new

    def test_write_mixed_new_and_existing_projects(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {"name": "test-project", "slots": [1, 2, 3]},
                {
                    "name": "new-project",
                    "team": "NEW",
                    "base_dir": "~/new",
                    "worktree_prefix": "new-slot-",
                    "slots": [4],
                },
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert len(data["projects"]) == 2
        assert data["projects"][0]["slots"] == [1, 2, 3]
        assert data["projects"][1]["name"] == "new-project"

    def test_write_project_run_command(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {"name": "test-project", "run_command": "npm run dev"},
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["run_command"] == "npm run dev"

    def test_write_project_run_env(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {"name": "test-project", "run_env": {"NODE_ENV": "development"}},
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["run_env"] == {"NODE_ENV": "development"}

    def test_write_project_run_port(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {"name": "test-project", "run_port": 3000},
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["run_port"] == 3000

    def test_write_new_project_with_run_fields(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        write_structural_config_updates(config_path, {
            "projects": [
                {
                    "name": "new-project",
                    "team": "NEW",
                    "base_dir": "~/new",
                    "worktree_prefix": "new-slot-",
                    "slots": [1],
                    "run_command": "npm run dev",
                    "run_port": 3000,
                    "run_env": {"NODE_ENV": "development"},
                },
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        new = data["projects"][1]
        assert new["run_command"] == "npm run dev"
        assert new["run_port"] == 3000
        assert new["run_env"] == {"NODE_ENV": "development"}


# --- Identities config ---


def test_load_config_identities_with_values(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "identities": {
            "coder": {
                "github_token": "ghp_coder123",
                "ssh_key_path": "~/.botfarm/coder_id_ed25519",
                "git_author_name": "Coder Bot",
                "git_author_email": "coder@example.com",
                "linear_api_key": "lin_api_coder",
            },
            "reviewer": {
                "github_token": "ghp_reviewer456",
                "linear_api_key": "lin_api_reviewer",
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)

    assert isinstance(config.identities, IdentitiesConfig)
    assert isinstance(config.identities.coder, CoderIdentity)
    assert isinstance(config.identities.reviewer, ReviewerIdentity)
    assert config.identities.coder.github_token == "ghp_coder123"
    assert config.identities.coder.ssh_key_path == "~/.botfarm/coder_id_ed25519"
    assert config.identities.coder.git_author_name == "Coder Bot"
    assert config.identities.coder.git_author_email == "coder@example.com"
    assert config.identities.coder.tracker_api_key == "lin_api_coder"
    assert config.identities.reviewer.github_token == "ghp_reviewer456"
    assert config.identities.reviewer.tracker_api_key == "lin_api_reviewer"


def test_load_config_identities_absent(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)

    assert isinstance(config.identities, IdentitiesConfig)
    assert config.identities.coder.github_token == ""
    assert config.identities.coder.ssh_key_path == ""
    assert config.identities.coder.git_author_name == ""
    assert config.identities.coder.git_author_email == ""
    assert config.identities.coder.tracker_api_key == ""
    assert config.identities.reviewer.github_token == ""
    assert config.identities.reviewer.tracker_api_key == ""


def test_load_config_identities_non_dict_raises(tmp_path):
    data = {**MINIMAL_CONFIG, "identities": "bad"}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="'identities' must be a mapping"):
        load_config(config_path)


def test_load_config_identities_env_var_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("CODER_GH_TOKEN", "ghp_expanded")
    monkeypatch.setenv("REVIEWER_GH_TOKEN", "ghp_rev_expanded")
    data = {
        **MINIMAL_CONFIG,
        "identities": {
            "coder": {
                "github_token": "${CODER_GH_TOKEN}",
            },
            "reviewer": {
                "github_token": "${REVIEWER_GH_TOKEN}",
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)

    assert config.identities.coder.github_token == "ghp_expanded"
    assert config.identities.reviewer.github_token == "ghp_rev_expanded"


# --- Codex reviewer config ---


def test_config_codex_fields(tmp_path):
    """Parse YAML with codex reviewer fields."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "codex_reviewer_enabled": True,
            "codex_reviewer_model": "o3",
            "codex_reviewer_reasoning_effort": "low",
            "codex_reviewer_timeout_minutes": 20,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.codex_reviewer_enabled is True
    assert config.agents.codex_reviewer_model == "o3"
    assert config.agents.codex_reviewer_reasoning_effort == "low"
    assert config.agents.codex_reviewer_timeout_minutes == 20


def test_config_codex_defaults(tmp_path):
    """Verify defaults when codex reviewer fields are absent."""
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.agents.codex_reviewer_enabled is False
    assert config.agents.codex_reviewer_model == ""
    assert config.agents.codex_reviewer_reasoning_effort == "medium"
    assert config.agents.codex_reviewer_timeout_minutes == 15


def test_config_codex_reasoning_effort_null(tmp_path):
    """YAML null for reasoning effort normalizes to empty string, not 'None'."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "codex_reviewer_reasoning_effort": None,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.codex_reviewer_reasoning_effort == ""


def test_config_codex_editable():
    """Verify codex reviewer fields appear in EDITABLE_FIELDS."""
    from botfarm.config import EDITABLE_FIELDS

    assert ("agents", "codex_reviewer_enabled") in EDITABLE_FIELDS
    assert ("agents", "codex_reviewer_model") in EDITABLE_FIELDS
    assert ("agents", "codex_reviewer_reasoning_effort") in EDITABLE_FIELDS
    assert ("agents", "codex_reviewer_timeout_minutes") in EDITABLE_FIELDS
    assert EDITABLE_FIELDS[("agents", "codex_reviewer_enabled")]["type"] == "bool"
    assert EDITABLE_FIELDS[("agents", "codex_reviewer_model")]["type"] == "str"
    assert EDITABLE_FIELDS[("agents", "codex_reviewer_reasoning_effort")]["type"] == "str"
    assert EDITABLE_FIELDS[("agents", "codex_reviewer_timeout_minutes")]["type"] == "int"


def test_config_codex_timeout_validation(tmp_path):
    """codex adapter timeout_minutes must be at least 1."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {"codex_reviewer_timeout_minutes": 0},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="timeout_minutes must be at least 1"):
        load_config(config_path)


def test_config_codex_enabled_rejects_string(tmp_path):
    """codex_reviewer_enabled must be a boolean."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {"codex_reviewer_enabled": "true"},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match=r"agents\.codex_reviewer_enabled must be a boolean"):
        load_config(config_path)


def test_config_codex_editable_validation():
    """Validate codex reviewer editable fields via validate_config_updates."""
    # Valid updates
    assert validate_config_updates({
        "agents": {
            "codex_reviewer_enabled": True,
            "codex_reviewer_model": "o4-mini",
            "codex_reviewer_reasoning_effort": "high",
            "codex_reviewer_timeout_minutes": 10,
        },
    }) == []

    # Invalid: timeout too low
    errors = validate_config_updates({
        "agents": {"codex_reviewer_timeout_minutes": 0},
    })
    assert any("at least 1" in e for e in errors)

    # Invalid: model not a string
    errors = validate_config_updates({
        "agents": {"codex_reviewer_model": 123},
    })
    assert any("string" in e for e in errors)

    # Invalid: enabled not a bool
    errors = validate_config_updates({
        "agents": {"codex_reviewer_enabled": "yes"},
    })
    assert any("boolean" in e for e in errors)


def test_default_config_template_includes_adapter_fields():
    from botfarm.config import DEFAULT_CONFIG_TEMPLATE
    assert "adapters:" in DEFAULT_CONFIG_TEMPLATE
    assert "claude:" in DEFAULT_CONFIG_TEMPLATE
    assert "codex:" in DEFAULT_CONFIG_TEMPLATE
    assert "enabled:" in DEFAULT_CONFIG_TEMPLATE
    assert "timeout_minutes:" in DEFAULT_CONFIG_TEMPLATE


def test_config_adapters_new_format(tmp_path):
    """New adapters: format parses correctly."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "adapters": {
                "claude": {"enabled": True},
                "codex": {
                    "enabled": True,
                    "model": "o4-mini",
                    "timeout_minutes": 20,
                    "reasoning_effort": "high",
                    "skip_on_reiteration": False,
                },
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.adapters["claude"].enabled is True
    assert config.agents.adapters["codex"].enabled is True
    assert config.agents.adapters["codex"].model == "o4-mini"
    assert config.agents.adapters["codex"].timeout_minutes == 20
    assert config.agents.adapters["codex"].reasoning_effort == "high"
    assert config.agents.adapters["codex"].skip_on_reiteration is False
    # Legacy accessors still work
    assert config.agents.codex_reviewer_enabled is True
    assert config.agents.codex_reviewer_model == "o4-mini"
    assert config.agents.codex_reviewer_timeout_minutes == 20


def test_config_adapters_backward_compat_migration(tmp_path):
    """Legacy codex_reviewer_* fields are migrated to adapters.codex."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "codex_reviewer_enabled": True,
            "codex_reviewer_model": "o3",
            "codex_reviewer_timeout_minutes": 25,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.agents.adapters["codex"].enabled is True
    assert config.agents.adapters["codex"].model == "o3"
    assert config.agents.adapters["codex"].timeout_minutes == 25


def test_config_adapters_new_format_timeout_validation(tmp_path):
    """adapters.codex.timeout_minutes must be at least 1."""
    data = {
        **MINIMAL_CONFIG,
        "agents": {
            "adapters": {
                "codex": {"timeout_minutes": 0},
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="timeout_minutes must be at least 1"):
        load_config(config_path)


# --- capacity_monitoring config ---


def test_load_config_capacity_monitoring_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.bugtracker.issue_limit == 250
    cap = config.bugtracker.capacity_monitoring
    assert cap.enabled is True
    assert cap.warning_threshold == 0.70
    assert cap.critical_threshold == 0.85
    assert cap.pause_threshold == 0.95
    assert cap.resume_threshold == 0.90


def test_load_config_capacity_monitoring_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "issue_limit": 500,
            "capacity_monitoring": {
                "enabled": False,
                "warning_threshold": 0.60,
                "critical_threshold": 0.75,
                "pause_threshold": 0.90,
                "resume_threshold": 0.80,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.bugtracker.issue_limit == 500
    cap = config.bugtracker.capacity_monitoring
    assert cap.enabled is False
    assert cap.warning_threshold == 0.60
    assert cap.critical_threshold == 0.75
    assert cap.pause_threshold == 0.90
    assert cap.resume_threshold == 0.80


def test_load_config_capacity_threshold_out_of_range(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "capacity_monitoring": {
                "warning_threshold": 1.5,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="between 0.0 and 1.0"):
        load_config(config_path)


def test_load_config_capacity_threshold_negative(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "capacity_monitoring": {
                "pause_threshold": -0.1,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="between 0.0 and 1.0"):
        load_config(config_path)


def test_load_config_capacity_resume_must_be_less_than_pause(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "capacity_monitoring": {
                "pause_threshold": 0.90,
                "resume_threshold": 0.95,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="resume_threshold must be less than pause_threshold"):
        load_config(config_path)


def test_load_config_capacity_resume_equal_pause_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "capacity_monitoring": {
                "pause_threshold": 0.90,
                "resume_threshold": 0.90,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="resume_threshold must be less than pause_threshold"):
        load_config(config_path)


def test_load_config_issue_limit_zero_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "issue_limit": 0,
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="issue_limit must be at least 1"):
        load_config(config_path)


def test_load_config_warning_above_critical_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "capacity_monitoring": {
                "warning_threshold": 0.90,
                "critical_threshold": 0.70,
                "pause_threshold": 0.95,
                "resume_threshold": 0.60,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="warning_threshold must be less than or equal to critical_threshold"):
        load_config(config_path)


def test_load_config_critical_above_pause_rejected(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "capacity_monitoring": {
                "warning_threshold": 0.70,
                "critical_threshold": 0.98,
                "pause_threshold": 0.95,
                "resume_threshold": 0.90,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="critical_threshold must be less than or equal to pause_threshold"):
        load_config(config_path)


def test_capacity_config_editable_fields():
    from botfarm.config import EDITABLE_FIELDS

    assert ("bugtracker", "issue_limit") in EDITABLE_FIELDS
    assert EDITABLE_FIELDS[("bugtracker", "issue_limit")]["type"] == "int"

    for field in ("enabled", "warning_threshold", "critical_threshold",
                  "pause_threshold", "resume_threshold"):
        key = ("bugtracker.capacity_monitoring", field)
        assert key in EDITABLE_FIELDS, f"{key} not in EDITABLE_FIELDS"


def test_capacity_config_editable_validation():
    # Valid updates
    assert validate_config_updates({
        "bugtracker": {"issue_limit": 500},
        "bugtracker.capacity_monitoring": {
            "enabled": True,
            "warning_threshold": 0.60,
            "critical_threshold": 0.75,
            "pause_threshold": 0.90,
            "resume_threshold": 0.80,
        },
    }) == []

    # Invalid: threshold out of range
    errors = validate_config_updates({
        "bugtracker.capacity_monitoring": {"warning_threshold": 1.5},
    })
    assert any("at most 1.0" in e for e in errors)

    # Invalid: threshold negative
    errors = validate_config_updates({
        "bugtracker.capacity_monitoring": {"pause_threshold": -0.1},
    })
    assert any("at least 0.0" in e for e in errors)

    # Invalid: issue_limit too low
    errors = validate_config_updates({
        "bugtracker": {"issue_limit": 0},
    })
    assert any("at least 1" in e for e in errors)

    # Invalid: enabled not a bool
    errors = validate_config_updates({
        "bugtracker.capacity_monitoring": {"enabled": "yes"},
    })
    assert any("boolean" in e for e in errors)

    # Invalid: resume_threshold >= pause_threshold (cross-field)
    errors = validate_config_updates({
        "bugtracker.capacity_monitoring": {
            "pause_threshold": 0.90,
            "resume_threshold": 0.95,
        },
    })
    assert any("resume_threshold must be less than pause_threshold" in e for e in errors)

    # Invalid: resume_threshold == pause_threshold (cross-field)
    errors = validate_config_updates({
        "bugtracker.capacity_monitoring": {
            "pause_threshold": 0.90,
            "resume_threshold": 0.90,
        },
    })
    assert any("resume_threshold must be less than pause_threshold" in e for e in errors)

    # Valid: resume_threshold < pause_threshold (cross-field)
    assert validate_config_updates({
        "bugtracker.capacity_monitoring": {
            "pause_threshold": 0.95,
            "resume_threshold": 0.90,
        },
    }) == []

    # Invalid: warning_threshold > critical_threshold (ordering)
    errors = validate_config_updates({
        "bugtracker.capacity_monitoring": {
            "warning_threshold": 0.90,
            "critical_threshold": 0.70,
        },
    })
    assert any("warning_threshold must be less than or equal to critical_threshold" in e for e in errors)

    # Invalid: critical_threshold > pause_threshold (ordering)
    errors = validate_config_updates({
        "bugtracker.capacity_monitoring": {
            "critical_threshold": 0.98,
            "pause_threshold": 0.95,
        },
    })
    assert any("critical_threshold must be less than or equal to pause_threshold" in e for e in errors)

    # Valid: warning <= critical <= pause (ordering)
    assert validate_config_updates({
        "bugtracker.capacity_monitoring": {
            "warning_threshold": 0.70,
            "critical_threshold": 0.85,
            "pause_threshold": 0.95,
        },
    }) == []


def test_capacity_cross_field_validation_with_config():
    """Cross-field validation falls back to current config when only one threshold is updated."""
    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test", team="T", base_dir="~/t",
                worktree_prefix="t-", slots=[1],
            ),
        ],
        bugtracker=LinearConfig(
            api_key="test",
            capacity_monitoring=CapacityConfig(
                resume_threshold=0.90,
                pause_threshold=0.95,
            ),
        ),
    )

    # Lowering pause_threshold below current resume_threshold (0.90) should fail
    errors = validate_config_updates(
        {"bugtracker.capacity_monitoring": {"pause_threshold": 0.85}},
        config=config,
    )
    assert any("resume_threshold must be less than pause_threshold" in e for e in errors)

    # Raising resume_threshold above current pause_threshold (0.95) should fail
    errors = validate_config_updates(
        {"bugtracker.capacity_monitoring": {"resume_threshold": 0.96}},
        config=config,
    )
    assert any("resume_threshold must be less than pause_threshold" in e for e in errors)

    # Valid: lowering pause_threshold but still above resume_threshold
    errors = validate_config_updates(
        {"bugtracker.capacity_monitoring": {"pause_threshold": 0.92}},
        config=config,
    )
    assert errors == []

    # Without config, single-field update cannot be cross-validated (no error)
    errors = validate_config_updates(
        {"bugtracker.capacity_monitoring": {"pause_threshold": 0.85}},
    )
    assert errors == []

    # Ordering: setting warning above current critical (0.85) with config
    config.bugtracker.capacity_monitoring.critical_threshold = 0.85
    errors = validate_config_updates(
        {"bugtracker.capacity_monitoring": {"warning_threshold": 0.90}},
        config=config,
    )
    assert any("warning_threshold must be less than or equal to critical_threshold" in e for e in errors)

    # Ordering: setting critical above current pause (0.95) with config
    errors = validate_config_updates(
        {"bugtracker.capacity_monitoring": {"critical_threshold": 0.97}},
        config=config,
    )
    assert any("critical_threshold must be less than or equal to pause_threshold" in e for e in errors)


def test_apply_capacity_monitoring_updates():
    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test", team="T", base_dir="~/t",
                worktree_prefix="t-", slots=[1],
            ),
        ],
    )
    apply_config_updates(config, {
        "bugtracker": {"issue_limit": 500},
        "bugtracker.capacity_monitoring": {
            "warning_threshold": 0.60,
            "pause_threshold": 0.92,
        },
    })
    assert config.bugtracker.issue_limit == 500
    assert config.bugtracker.capacity_monitoring.warning_threshold == 0.60
    assert config.bugtracker.capacity_monitoring.pause_threshold == 0.92
    # Unchanged fields keep defaults
    assert config.bugtracker.capacity_monitoring.critical_threshold == 0.85


def test_write_config_updates_capacity_monitoring(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "bugtracker": {
            **MINIMAL_CONFIG["bugtracker"],
            "capacity_monitoring": {
                "warning_threshold": 0.70,
                "critical_threshold": 0.85,
            },
        },
    }
    config_path = _write_config(tmp_path, data)
    write_config_updates(config_path, {
        "bugtracker.capacity_monitoring": {"warning_threshold": 0.60},
    })
    result = yaml.safe_load(config_path.read_text())
    assert result["bugtracker"]["capacity_monitoring"]["warning_threshold"] == 0.60
    # Other fields preserved
    assert result["bugtracker"]["capacity_monitoring"]["critical_threshold"] == 0.85


def test_write_config_updates_creates_capacity_monitoring_section(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    write_config_updates(config_path, {
        "bugtracker.capacity_monitoring": {"pause_threshold": 0.92},
    })
    result = yaml.safe_load(config_path.read_text())
    assert result["bugtracker"]["capacity_monitoring"]["pause_threshold"] == 0.92


def test_default_config_template_includes_capacity_fields():
    from botfarm.config import DEFAULT_CONFIG_TEMPLATE
    assert "issue_limit:" in DEFAULT_CONFIG_TEMPLATE
    assert "capacity_monitoring:" in DEFAULT_CONFIG_TEMPLATE
    assert "warning_threshold:" in DEFAULT_CONFIG_TEMPLATE
    assert "critical_threshold:" in DEFAULT_CONFIG_TEMPLATE
    assert "pause_threshold:" in DEFAULT_CONFIG_TEMPLATE
    assert "resume_threshold:" in DEFAULT_CONFIG_TEMPLATE


# --- start_paused ---


def test_start_paused_defaults_to_true(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.start_paused is True


def test_start_paused_false(tmp_path):
    data = {**MINIMAL_CONFIG, "start_paused": False}
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.start_paused is False


def test_start_paused_true_explicit(tmp_path):
    data = {**MINIMAL_CONFIG, "start_paused": True}
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.start_paused is True


def test_start_paused_non_bool_raises(tmp_path):
    data = {**MINIMAL_CONFIG, "start_paused": "yes"}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="must be a boolean"):
        load_config(config_path)


def test_default_config_template_includes_start_paused():
    from botfarm.config import DEFAULT_CONFIG_TEMPLATE
    assert "start_paused:" in DEFAULT_CONFIG_TEMPLATE


# ---------------------------------------------------------------------------
# Bugtracker config abstraction (SMA-464)
# ---------------------------------------------------------------------------

MINIMAL_BUGTRACKER_CONFIG = {
    "projects": [
        {
            "name": "test-project",
            "team": "TST",
            "base_dir": "~/test",
            "worktree_prefix": "test-slot-",
            "slots": [1],
        }
    ],
    "bugtracker": {"type": "linear", "api_key": "test-key"},
}


class TestBugtrackerConfigSection:
    """Test the new bugtracker: config section."""

    def test_bugtracker_section_loads(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_BUGTRACKER_CONFIG)
        config = load_config(config_path)
        assert config.bugtracker.type == "linear"
        assert config.bugtracker.api_key == "test-key"

    def test_bugtracker_is_canonical(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_BUGTRACKER_CONFIG)
        config = load_config(config_path)
        assert hasattr(config, "bugtracker")
        assert config.bugtracker is not None

    def test_bugtracker_section_with_all_fields(self, tmp_path):
        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "s-",
                    "slots": [1],
                }
            ],
            "bugtracker": {
                "type": "linear",
                "api_key": "k",
                "workspace": "ws",
                "poll_interval_seconds": 60,
                "exclude_tags": ["Bot"],
                "issue_limit": 500,
                "todo_status": "Backlog",
                "in_progress_status": "Doing",
                "done_status": "Finished",
                "in_review_status": "Review",
                "comment_on_failure": False,
                "comment_on_completion": True,
                "comment_on_limit_pause": True,
                "capacity_monitoring": {
                    "enabled": False,
                    "warning_threshold": 0.5,
                    "critical_threshold": 0.6,
                    "pause_threshold": 0.8,
                    "resume_threshold": 0.7,
                },
            },
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        bt = config.bugtracker
        assert bt.type == "linear"
        assert bt.workspace == "ws"
        assert bt.poll_interval_seconds == 60
        assert bt.exclude_tags == ["Bot"]
        assert bt.issue_limit == 500
        assert bt.todo_status == "Backlog"
        assert bt.comment_on_failure is False
        assert bt.comment_on_completion is True
        assert bt.capacity_monitoring.enabled is False
        assert bt.capacity_monitoring.pause_threshold == 0.8


    def test_unsupported_bugtracker_type_raises(self, tmp_path):
        data = {
            **MINIMAL_BUGTRACKER_CONFIG,
            "bugtracker": {"type": "github", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="Unsupported bugtracker type: 'github'"):
            load_config(config_path)


class TestBugtrackerConfigDataclasses:
    """Test the BugtrackerConfig and LinearBugtrackerConfig dataclasses."""

    def test_bugtracker_config_defaults(self):
        from botfarm.config import BugtrackerConfig
        cfg = BugtrackerConfig()
        assert cfg.type == "linear"
        assert cfg.api_key == ""
        assert cfg.poll_interval_seconds == 30
        assert cfg.exclude_tags == ["Human"]
        assert cfg.todo_status == "Todo"
        assert cfg.comment_on_failure is True

    def test_linear_bugtracker_config_defaults(self):
        from botfarm.config import LinearBugtrackerConfig
        cfg = LinearBugtrackerConfig()
        assert cfg.type == "linear"
        assert cfg.workspace == ""
        assert cfg.issue_limit == 250
        assert cfg.capacity_monitoring.enabled is True

    def test_linear_config_alias_is_same_class(self):
        from botfarm.config import LinearBugtrackerConfig, LinearConfig
        assert LinearConfig is LinearBugtrackerConfig

    def test_linear_bugtracker_inherits_from_bugtracker(self):
        from botfarm.config import BugtrackerConfig, LinearBugtrackerConfig
        assert issubclass(LinearBugtrackerConfig, BugtrackerConfig)


class TestProjectConfigFields:
    """Test ProjectConfig field names (team, tracker_project)."""

    def test_team_field(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-", slots=[1])
        assert p.team == "T"

    def test_tracker_project_field(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-", slots=[1], tracker_project="P")
        assert p.tracker_project == "P"

    def test_yaml_team_field(self, tmp_path):
        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "TST",
                    "base_dir": "~/d",
                    "worktree_prefix": "s-",
                    "slots": [1],
                    "tracker_project": "My Project",
                }
            ],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].team == "TST"
        assert config.projects[0].tracker_project == "My Project"

    def test_yaml_team_field_loads(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        config = load_config(config_path)
        assert config.projects[0].team == "TST"

    def test_project_type_field(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-",
                          slots=[1], project_type="python")
        assert p.project_type == "python"

    def test_project_type_default(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-", slots=[1])
        assert p.project_type == ""

    def test_setup_commands_field(self):
        cmds = ["pip install -r requirements.txt", "pip install -e ."]
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-",
                          slots=[1], setup_commands=cmds)
        assert p.setup_commands == cmds

    def test_setup_commands_default(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-", slots=[1])
        assert p.setup_commands == []

    def test_yaml_project_type_loads(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "project_type": "python",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].project_type == "python"

    def test_yaml_setup_commands_loads(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "setup_commands": ["npm install"],
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].setup_commands == ["npm install"]

    def test_yaml_missing_optional_fields(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        config = load_config(config_path)
        assert config.projects[0].project_type == ""
        assert config.projects[0].setup_commands == []

    def test_invalid_project_type(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "project_type": 123,
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="project_type must be a string"):
            load_config(config_path)

    def test_invalid_project_type_falsy_non_string(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "project_type": 0,
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="project_type must be a string"):
            load_config(config_path)

    def test_invalid_setup_commands_not_list(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "setup_commands": "not a list",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="setup_commands must be a list"):
            load_config(config_path)

    def test_invalid_setup_commands_not_strings(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "setup_commands": [1, 2],
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="setup_commands must be a list of strings"):
            load_config(config_path)


class TestProjectRunCommandFields:
    """Test run_command, run_env, run_port on ProjectConfig."""

    def test_run_command_field(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-",
                          slots=[1], run_command="npm run dev")
        assert p.run_command == "npm run dev"

    def test_run_command_default(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-", slots=[1])
        assert p.run_command == ""

    def test_run_env_field(self):
        env = {"NODE_ENV": "development"}
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-",
                          slots=[1], run_env=env)
        assert p.run_env == env

    def test_run_env_default(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-", slots=[1])
        assert p.run_env == {}

    def test_run_port_field(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-",
                          slots=[1], run_port=3000)
        assert p.run_port == 3000

    def test_run_port_default(self):
        p = ProjectConfig(name="p", team="T", base_dir="d", worktree_prefix="s-", slots=[1])
        assert p.run_port == 0

    def test_yaml_run_command_loads(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_command": "npm run dev",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].run_command == "npm run dev"

    def test_yaml_run_env_loads(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_env": {"NODE_ENV": "development", "PORT": "3000"},
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].run_env == {"NODE_ENV": "development", "PORT": "3000"}

    def test_yaml_run_port_loads(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_port": 3000,
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].run_port == 3000

    def test_yaml_missing_run_fields_default(self, tmp_path):
        config_path = _write_config(tmp_path, MINIMAL_CONFIG)
        config = load_config(config_path)
        assert config.projects[0].run_command == ""
        assert config.projects[0].run_env == {}
        assert config.projects[0].run_port == 0

    def test_invalid_run_command_type(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_command": 123,
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="run_command must be a string"):
            load_config(config_path)

    def test_invalid_run_env_not_dict(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_env": "not a dict",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="run_env must be a mapping"):
            load_config(config_path)

    def test_invalid_run_env_non_string_values(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_env": {"KEY": 123},
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="run_env must be a mapping"):
            load_config(config_path)

    def test_invalid_run_port_not_int(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_port": "abc",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="run_port must be an integer"):
            load_config(config_path)

    def test_invalid_run_port_negative(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_port": -1,
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="run_port must be a non-negative integer"):
            load_config(config_path)

    def test_invalid_run_port_bool(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "run_port": True,
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="run_port must be an integer"):
            load_config(config_path)

    def test_default_run_command_from_project_type(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "project_type": "nextjs",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].run_command == "npm run dev"
        assert config.projects[0].run_port == 3000

    def test_default_run_command_from_django_type(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "project_type": "django",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].run_command == "python manage.py runserver 0.0.0.0:8000"
        assert config.projects[0].run_port == 8000

    def test_explicit_run_command_overrides_project_type_default(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "project_type": "nextjs",
                "run_command": "npx next dev --turbo",
                "run_port": 4000,
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].run_command == "npx next dev --turbo"
        assert config.projects[0].run_port == 4000

    def test_unknown_project_type_no_default(self, tmp_path):
        data = {
            "projects": [{
                "name": "p", "team": "TST", "base_dir": "~/d",
                "worktree_prefix": "s-", "slots": [1],
                "project_type": "custom",
            }],
            "bugtracker": {"type": "linear", "api_key": "k"},
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.projects[0].run_command == ""
        assert config.projects[0].run_port == 0


class TestBotfarmConfigBugtracker:
    """Test BotfarmConfig.bugtracker field."""

    def test_bugtracker_init_kwarg(self):
        cfg = BotfarmConfig(
            projects=[],
            bugtracker=LinearConfig(api_key="k"),
        )
        assert cfg.bugtracker.api_key == "k"

    def test_bugtracker_default(self):
        cfg = BotfarmConfig(projects=[])
        assert cfg.bugtracker is not None
        assert cfg.bugtracker.api_key == ""


class TestDefaultConfigTemplateBugtracker:
    """Verify the config template uses the new bugtracker section."""

    def test_template_has_bugtracker_section(self):
        from botfarm.config import DEFAULT_CONFIG_TEMPLATE
        assert "bugtracker:" in DEFAULT_CONFIG_TEMPLATE
        assert "type: linear" in DEFAULT_CONFIG_TEMPLATE

    def test_template_has_linear_type(self):
        from botfarm.config import DEFAULT_CONFIG_TEMPLATE
        assert "type: linear" in DEFAULT_CONFIG_TEMPLATE


# ---------------------------------------------------------------------------
# Jira bugtracker config (SMA-497)
# ---------------------------------------------------------------------------


class TestJiraBugtrackerConfig:
    """Test JiraBugtrackerConfig dataclass and YAML loading."""

    def test_jira_config_defaults(self):
        from botfarm.config import JiraBugtrackerConfig
        cfg = JiraBugtrackerConfig()
        assert cfg.type == "jira"
        assert cfg.url == ""
        assert cfg.email == ""

    def test_jira_inherits_from_bugtracker(self):
        from botfarm.config import BugtrackerConfig, JiraBugtrackerConfig
        assert issubclass(JiraBugtrackerConfig, BugtrackerConfig)

    def test_jira_config_loads_from_yaml(self, tmp_path):
        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "PROJ",
                    "base_dir": "~/d",
                    "worktree_prefix": "s-",
                    "slots": [1],
                }
            ],
            "bugtracker": {
                "type": "jira",
                "api_key": "jira-token",
                "url": "https://mycompany.atlassian.net",
                "email": "bot@example.com",
                "poll_interval_seconds": 45,
                "exclude_tags": ["Manual"],
                "todo_status": "To Do",
                "in_progress_status": "In Progress",
                "done_status": "Done",
                "in_review_status": "In Review",
            },
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        bt = config.bugtracker
        assert bt.type == "jira"
        assert bt.api_key == "jira-token"
        assert bt.url == "https://mycompany.atlassian.net"
        assert bt.email == "bot@example.com"
        assert bt.poll_interval_seconds == 45
        assert bt.exclude_tags == ["Manual"]
        assert bt.todo_status == "To Do"

    def test_jira_missing_url_raises(self, tmp_path):
        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "PROJ",
                    "base_dir": "~/d",
                    "worktree_prefix": "s-",
                    "slots": [1],
                }
            ],
            "bugtracker": {
                "type": "jira",
                "api_key": "jira-token",
                "email": "bot@example.com",
            },
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="bugtracker.url must be set for Jira"):
            load_config(config_path)

    def test_jira_missing_email_raises(self, tmp_path):
        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "PROJ",
                    "base_dir": "~/d",
                    "worktree_prefix": "s-",
                    "slots": [1],
                }
            ],
            "bugtracker": {
                "type": "jira",
                "api_key": "jira-token",
                "url": "https://mycompany.atlassian.net",
            },
        }
        config_path = _write_config(tmp_path, data)
        with pytest.raises(ConfigError, match="bugtracker.email must be set for Jira"):
            load_config(config_path)

    def test_jira_no_capacity_monitoring(self, tmp_path):
        """Jira config should not have capacity_monitoring or issue_limit."""
        from botfarm.config import JiraBugtrackerConfig
        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "PROJ",
                    "base_dir": "~/d",
                    "worktree_prefix": "s-",
                    "slots": [1],
                }
            ],
            "bugtracker": {
                "type": "jira",
                "api_key": "jira-token",
                "url": "https://mycompany.atlassian.net",
                "email": "bot@example.com",
            },
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert isinstance(config.bugtracker, JiraBugtrackerConfig)
        assert not hasattr(config.bugtracker, "issue_limit")
        assert not hasattr(config.bugtracker, "capacity_monitoring")

    def test_botfarm_config_accepts_jira_bugtracker(self):
        from botfarm.config import JiraBugtrackerConfig
        cfg = BotfarmConfig(
            projects=[],
            bugtracker=JiraBugtrackerConfig(
                type="jira",
                api_key="k",
                url="https://x.atlassian.net",
                email="e@x.com",
            ),
        )
        assert cfg.bugtracker.type == "jira"


class TestCoderIdentityJiraToken:
    """Test jira_api_token field on CoderIdentity."""

    def test_default_empty(self):
        from botfarm.config import CoderIdentity
        c = CoderIdentity()
        assert c.jira_api_token == ""

    def test_jira_api_token_from_yaml(self, tmp_path):
        data = {
            **MINIMAL_BUGTRACKER_CONFIG,
            "identities": {
                "coder": {
                    "jira_api_token": "coder-jira-tok",
                },
            },
        }
        config_path = _write_config(tmp_path, data)
        config = load_config(config_path)
        assert config.identities.coder.jira_api_token == "coder-jira-tok"
