"""Tests for botfarm.config module."""

import os
from pathlib import Path

import pytest
import yaml

from botfarm.config import (
    AgentsConfig,
    BotfarmConfig,
    ConfigError,
    LinearConfig,
    LoggingConfig,
    NotificationsConfig,
    ProjectConfig,
    UsageLimitsConfig,
    apply_config_updates,
    create_default_config,
    expand_env_vars,
    load_config,
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
            "linear_team": "TST",
            "base_dir": "~/test",
            "worktree_prefix": "test-slot-",
            "slots": [1],
        }
    ],
    "linear": {"api_key": "test-key"},
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


def test_expand_env_vars_missing_raises():
    key = "BOTFARM_TEST_MISSING_VAR_12345"
    os.environ.pop(key, None)
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
    assert "projects:" in content
    assert "linear:" in content


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


def test_load_config_missing_projects(tmp_path):
    config_path = _write_config(tmp_path, {"linear": {"api_key": "k"}})
    with pytest.raises(ConfigError, match="projects"):
        load_config(config_path)


def test_load_config_empty_projects(tmp_path):
    config_path = _write_config(
        tmp_path, {"projects": [], "linear": {"api_key": "k"}}
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
                "linear_team": "TST",
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
                "linear_team": "TST",
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
                "linear_team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1],
            },
            {
                "name": "same-name",
                "linear_team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [2],
            },
        ],
        "linear": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="Duplicate project names"):
        load_config(config_path)


def test_load_config_env_var_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_API_KEY", "sk-test-123")
    data = {
        **MINIMAL_CONFIG,
        "linear": {"api_key": "${TEST_API_KEY}"},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.linear.api_key == "sk-test-123"


def test_load_config_full(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_KEY", "key123")
    data = {
        "projects": [
            {
                "name": "proj-a",
                "linear_team": "SMA",
                "base_dir": "~/proj-a",
                "worktree_prefix": "proj-a-slot-",
                "slots": [1, 2],
            }
        ],
        "linear": {
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

    assert config.linear.api_key == "key123"
    assert config.linear.poll_interval_seconds == 60
    assert config.linear.exclude_tags == ["Human", "Manual"]
    assert config.database.path == "~/.botfarm/custom.db"


def test_load_config_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)

    assert config.linear.poll_interval_seconds == 30
    assert config.linear.exclude_tags == ["Human"]
    assert config.database.path == ""


def test_load_config_poll_interval_zero(tmp_path):
    data = {**MINIMAL_CONFIG, "linear": {"api_key": "k", "poll_interval_seconds": 0}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="at least 1"):
        load_config(config_path)


def test_load_config_empty_api_key(tmp_path):
    data = {**MINIMAL_CONFIG, "linear": {"api_key": ""}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="api_key must be set"):
        load_config(config_path)


def test_load_config_missing_api_key(tmp_path):
    data = {**MINIMAL_CONFIG, "linear": {}}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="api_key must be set"):
        load_config(config_path)


def test_load_config_cross_project_duplicate_slots_allowed(tmp_path):
    """Slot IDs are per-project, so the same ID can appear in different projects."""
    data = {
        "projects": [
            {
                "name": "project-a",
                "linear_team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1, 2],
            },
            {
                "name": "project-b",
                "linear_team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [1, 2],
            },
        ],
        "linear": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert len(config.projects) == 2
    assert config.projects[0].slots == [1, 2]
    assert config.projects[1].slots == [1, 2]


# --- linear_project config ---


def test_load_config_linear_project_default(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.projects[0].linear_project == ""


def test_load_config_linear_project_set(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "projects": [
            {
                "name": "test-project",
                "linear_team": "TST",
                "linear_project": "My Project",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1],
            }
        ],
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.projects[0].linear_project == "My Project"


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


# --- Linear status and comment config ---


def test_load_config_linear_status_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.linear.todo_status == "Todo"
    assert config.linear.in_progress_status == "In Progress"
    assert config.linear.done_status == "Done"
    assert config.linear.in_review_status == "In Review"
    assert config.linear.failed_status == "Todo"


def test_load_config_linear_status_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "linear": {
            "api_key": "test-key",
            "todo_status": "Backlog",
            "in_progress_status": "Working",
            "done_status": "Shipped",
            "in_review_status": "Review",
            "failed_status": "Needs Attention",
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.linear.todo_status == "Backlog"
    assert config.linear.in_progress_status == "Working"
    assert config.linear.done_status == "Shipped"
    assert config.linear.in_review_status == "Review"
    assert config.linear.failed_status == "Needs Attention"


def test_load_config_comment_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
    assert config.linear.comment_on_failure is True
    assert config.linear.comment_on_completion is False
    assert config.linear.comment_on_limit_pause is False


def test_load_config_comment_custom(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "linear": {
            "api_key": "test-key",
            "comment_on_failure": False,
            "comment_on_completion": True,
            "comment_on_limit_pause": True,
        },
    }
    config_path = _write_config(tmp_path, data)
    config = load_config(config_path)
    assert config.linear.comment_on_failure is False
    assert config.linear.comment_on_completion is True
    assert config.linear.comment_on_limit_pause is True


def test_load_config_comment_rejects_string_bool(tmp_path):
    data = {
        **MINIMAL_CONFIG,
        "linear": {
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
    assert "failed_status:" in DEFAULT_CONFIG_TEMPLATE
    assert "comment_on_failure:" in DEFAULT_CONFIG_TEMPLATE
    assert "comment_on_completion:" in DEFAULT_CONFIG_TEMPLATE
    assert "comment_on_limit_pause:" in DEFAULT_CONFIG_TEMPLATE


# --- CLI init command ---


def test_cli_init(tmp_path):
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    runner = CliRunner()
    result = runner.invoke(main, ["init", "--path", str(config_path)])
    assert result.exit_code == 0
    assert "Created default config" in result.output
    assert config_path.exists()


def test_cli_init_already_exists(tmp_path):
    from click.testing import CliRunner

    from botfarm.cli import main

    config_path = tmp_path / "config.yaml"
    config_path.write_text("existing")
    runner = CliRunner()
    result = runner.invoke(main, ["init", "--path", str(config_path)])
    assert result.exit_code == 0
    assert "already exists" in result.output


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
            "linear": {
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
        updates = {"linear": {"api_key": "new-key"}}
        errors = validate_config_updates(updates)
        assert any("not an editable field" in e for e in errors)

    def test_unknown_section_field_rejected(self):
        updates = {"database": {"path": "/tmp/db"}}
        errors = validate_config_updates(updates)
        assert len(errors) > 0

    def test_section_not_mapping(self):
        updates = {"linear": "not a dict"}
        errors = validate_config_updates(updates)
        assert any("must be a mapping" in e for e in errors)

    def test_poll_interval_too_low(self):
        updates = {"linear": {"poll_interval_seconds": 0}}
        errors = validate_config_updates(updates)
        assert any("at least 1" in e for e in errors)

    def test_bool_field_rejects_string(self):
        updates = {"linear": {"comment_on_failure": "true"}}
        errors = validate_config_updates(updates)
        assert any("boolean" in e for e in errors)

    def test_int_field_rejects_bool(self):
        updates = {"linear": {"poll_interval_seconds": True}}
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
                    name="test", linear_team="T", base_dir="~/t",
                    worktree_prefix="t-", slots=[1],
                ),
            ],
        )

    def test_apply_linear_updates(self):
        config = self._make_config()
        apply_config_updates(config, {
            "linear": {"poll_interval_seconds": 60, "comment_on_failure": False},
        })
        assert config.linear.poll_interval_seconds == 60
        assert config.linear.comment_on_failure is False

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
            "linear": {"poll_interval_seconds": 60},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["linear"]["poll_interval_seconds"] == 60

    def test_preserves_env_var_refs(self, tmp_path):
        """Env var references like ${API_KEY} must survive write-back."""
        raw_config = {
            **MINIMAL_CONFIG,
            "linear": {"api_key": "${LINEAR_API_KEY}", "poll_interval_seconds": 120},
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(raw_config, sort_keys=False))

        write_config_updates(config_path, {
            "linear": {"poll_interval_seconds": 60},
        })

        data = yaml.safe_load(config_path.read_text())
        assert data["linear"]["api_key"] == "${LINEAR_API_KEY}"
        assert data["linear"]["poll_interval_seconds"] == 60

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
            "linear": {"poll_interval_seconds": 60},
            "agents": {"max_review_iterations": 5},
        })
        # Should parse without error
        data = yaml.safe_load(config_path.read_text())
        assert data["linear"]["poll_interval_seconds"] == 60
        assert data["agents"]["max_review_iterations"] == 5


# --- Structural config validation ---


def _make_config_for_structural():
    """Create a BotfarmConfig for structural validation tests."""
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="project-a", linear_team="TST",
                base_dir="~/a", worktree_prefix="a-slot-",
                slots=[1, 2],
            ),
            ProjectConfig(
                name="project-b", linear_team="TST",
                base_dir="~/b", worktree_prefix="b-slot-",
                slots=[3], linear_project="My Filter",
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

    def test_valid_project_linear_project(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [
                {"name": "project-b", "linear_project": "New Filter"},
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

    def test_projects_unknown_project(self):
        config = _make_config_for_structural()
        updates = {"projects": [{"name": "nonexistent", "slots": [1]}]}
        errors = validate_structural_config_updates(updates, config)
        assert any("does not exist" in e for e in errors)

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

    def test_projects_linear_project_not_string(self):
        config = _make_config_for_structural()
        updates = {
            "projects": [{"name": "project-a", "linear_project": 123}],
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
                    "linear_team": "TST",
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

    def test_write_project_linear_project(self, tmp_path):
        config_path = _write_config(tmp_path, {
            **MINIMAL_CONFIG,
            "projects": [
                {
                    "name": "test-project",
                    "linear_team": "TST",
                    "base_dir": "~/test",
                    "worktree_prefix": "test-slot-",
                    "slots": [1],
                },
            ],
        })
        write_structural_config_updates(config_path, {
            "projects": [
                {"name": "test-project", "linear_project": "My Project"},
            ],
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["linear_project"] == "My Project"

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
            "linear": {"api_key": "${LINEAR_API_KEY}"},
            "notifications": {"webhook_url": "https://old.example.com"},
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(raw_config, sort_keys=False))
        write_structural_config_updates(config_path, {
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["linear"]["api_key"] == "${LINEAR_API_KEY}"
