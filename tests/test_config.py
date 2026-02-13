"""Tests for botfarm.config module."""

import os
from pathlib import Path

import pytest
import yaml

from botfarm.config import (
    BotfarmConfig,
    ConfigError,
    create_default_config,
    expand_env_vars,
    load_config,
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
    "max_total_slots": 5,
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
    assert "max_total_slots:" in content


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
    assert config.max_total_slots == 5


def test_load_config_file_not_found(tmp_path):
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "nonexistent.yaml")


def test_load_config_invalid_yaml(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("- not a mapping")
    with pytest.raises(ConfigError, match="YAML mapping"):
        load_config(config_path)


def test_load_config_missing_projects(tmp_path):
    config_path = _write_config(tmp_path, {"max_total_slots": 5})
    with pytest.raises(ConfigError, match="projects"):
        load_config(config_path)


def test_load_config_empty_projects(tmp_path):
    config_path = _write_config(
        tmp_path, {"projects": [], "max_total_slots": 5, "linear": {"api_key": "k"}}
    )
    with pytest.raises(ConfigError, match="At least one project"):
        load_config(config_path)


def test_load_config_project_missing_fields(tmp_path):
    data = {
        "projects": [{"name": "incomplete"}],
        "max_total_slots": 5,
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


def test_load_config_exceeds_max_slots(tmp_path):
    data = {
        "projects": [
            {
                "name": "project-a",
                "linear_team": "TST",
                "base_dir": "~/a",
                "worktree_prefix": "a-slot-",
                "slots": [1, 2, 3],
            },
            {
                "name": "project-b",
                "linear_team": "TST",
                "base_dir": "~/b",
                "worktree_prefix": "b-slot-",
                "slots": [4, 5, 6],
            },
        ],
        "max_total_slots": 5,
        "linear": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="exceeds max_total_slots"):
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
        "max_total_slots": 5,
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
        "max_total_slots": 5,
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

    assert config.linear.poll_interval_seconds == 120
    assert config.linear.exclude_tags == ["Human"]
    assert config.database.path == "~/.botfarm/botfarm.db"


def test_load_config_max_total_slots_zero(tmp_path):
    data = {**MINIMAL_CONFIG, "max_total_slots": 0}
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="at least 1"):
        load_config(config_path)


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


def test_load_config_cross_project_duplicate_slots(tmp_path):
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
                "slots": [2, 3],
            },
        ],
        "max_total_slots": 10,
        "linear": {"api_key": "test-key"},
    }
    config_path = _write_config(tmp_path, data)
    with pytest.raises(ConfigError, match="Duplicate slot assignments"):
        load_config(config_path)


# --- CLI init command ---


# --- usage_limits config ---


def test_load_config_usage_limits_defaults(tmp_path):
    config_path = _write_config(tmp_path, MINIMAL_CONFIG)
    config = load_config(config_path)
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
