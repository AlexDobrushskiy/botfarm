"""Tests for botfarm remove-project CLI command."""

import sqlite3

import pytest
import yaml
from click.testing import CliRunner

from botfarm.cli import main
from botfarm.db import (
    delete_project_data,
    init_db,
    load_all_slots,
    save_project_pause_state,
    upsert_slot,
)
from tests.helpers import make_slot, seed_slots


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def db_and_config(tmp_path, monkeypatch):
    """Create a DB and config.yaml with one project."""
    db_path = tmp_path / "botfarm.db"
    conn = init_db(db_path, allow_migration=True)
    conn.close()

    config_path = tmp_path / "config.yaml"
    config_data = {
        "projects": [
            {
                "name": "my-project",
                "base_dir": str(tmp_path / "projects" / "my-project" / "repo"),
                "worktree_prefix": "my-project-slot-",
                "slots": [1, 2],
                "team": "TST",
            },
        ],
        "bugtracker": {"type": "linear", "api_key": "test-key"},
    }
    config_path.write_text(yaml.dump(config_data))

    monkeypatch.setenv("BOTFARM_DB_PATH", str(db_path))
    return db_path, config_path


class TestDeleteProjectData:
    """Unit tests for the db.delete_project_data helper."""

    def test_deletes_slots(self, tmp_path):
        db_path = tmp_path / "botfarm.db"
        conn = init_db(db_path, allow_migration=True)
        upsert_slot(conn, make_slot("proj-a", 1))
        upsert_slot(conn, make_slot("proj-a", 2))
        upsert_slot(conn, make_slot("proj-b", 1))
        conn.commit()

        counts = delete_project_data(conn, "proj-a")
        conn.commit()

        assert counts["slots"] == 2
        rows = load_all_slots(conn)
        assert len(rows) == 1
        assert rows[0]["project"] == "proj-b"
        conn.close()

    def test_deletes_pause_state(self, tmp_path):
        db_path = tmp_path / "botfarm.db"
        conn = init_db(db_path, allow_migration=True)
        save_project_pause_state(conn, project="proj-a", paused=True, reason="test")
        conn.commit()

        counts = delete_project_data(conn, "proj-a")
        conn.commit()

        assert counts["project_pause_state"] == 1
        conn.close()

    def test_no_rows_returns_zeros(self, tmp_path):
        db_path = tmp_path / "botfarm.db"
        conn = init_db(db_path, allow_migration=True)

        counts = delete_project_data(conn, "nonexistent")
        conn.commit()

        assert counts["slots"] == 0
        assert counts["queue_entries"] == 0
        assert counts["project_pause_state"] == 0
        conn.close()


class TestRemoveProjectCommand:
    """Tests for `botfarm remove-project`."""

    def test_removes_project_from_config(self, runner, db_and_config):
        db_path, config_path = db_and_config
        seed_slots(db_path, [make_slot("my-project", 1), make_slot("my-project", 2)])

        result = runner.invoke(
            main, ["remove-project", "my-project", "--config", str(config_path), "--yes"]
        )
        assert result.exit_code == 0, result.output

        data = yaml.safe_load(config_path.read_text())
        project_names = [p["name"] for p in (data.get("projects") or [])]
        assert "my-project" not in project_names

    def test_cleans_up_db_slots(self, runner, db_and_config):
        db_path, config_path = db_and_config
        seed_slots(db_path, [make_slot("my-project", 1), make_slot("my-project", 2)])

        result = runner.invoke(
            main, ["remove-project", "my-project", "--config", str(config_path), "--yes"]
        )
        assert result.exit_code == 0, result.output

        conn = init_db(db_path)
        rows = load_all_slots(conn)
        conn.close()
        assert len(rows) == 0

    def test_blocks_on_active_slots(self, runner, db_and_config):
        db_path, config_path = db_and_config
        seed_slots(db_path, [
            make_slot("my-project", 1, status="busy", ticket_id="TST-1"),
        ])

        result = runner.invoke(
            main, ["remove-project", "my-project", "--config", str(config_path), "--yes"]
        )
        assert result.exit_code != 0
        assert "active slot" in result.output.lower()

    def test_force_bypasses_active_check(self, runner, db_and_config):
        db_path, config_path = db_and_config
        seed_slots(db_path, [
            make_slot("my-project", 1, status="busy", ticket_id="TST-1"),
        ])

        result = runner.invoke(
            main, [
                "remove-project", "my-project",
                "--config", str(config_path), "--force", "--yes",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "removed successfully" in result.output.lower()

    def test_unknown_project_errors(self, runner, db_and_config):
        _db_path, config_path = db_and_config

        result = runner.invoke(
            main, ["remove-project", "nonexistent", "--config", str(config_path), "--yes"]
        )
        assert result.exit_code != 0
        assert "not found" in result.output.lower()

    def test_clean_removes_directory(self, runner, db_and_config, tmp_path):
        db_path, config_path = db_and_config
        # Create the projects directory
        projects_dir = tmp_path / "projects" / "my-project"
        projects_dir.mkdir(parents=True, exist_ok=True)
        (projects_dir / "repo").mkdir()
        (projects_dir / "my-project-slot-1").mkdir()

        result = runner.invoke(
            main, [
                "remove-project", "my-project",
                "--config", str(config_path), "--clean", "--yes",
            ]
        )
        assert result.exit_code == 0, result.output
        assert not projects_dir.exists()

    def test_no_clean_preserves_directory(self, runner, db_and_config, tmp_path):
        db_path, config_path = db_and_config
        projects_dir = tmp_path / "projects" / "my-project"
        projects_dir.mkdir(parents=True, exist_ok=True)
        (projects_dir / "repo").mkdir()

        result = runner.invoke(
            main, [
                "remove-project", "my-project",
                "--config", str(config_path), "--yes",
            ]
        )
        assert result.exit_code == 0, result.output
        assert projects_dir.exists()

    def test_preserves_other_projects(self, runner, tmp_path, monkeypatch):
        """Removing one project should not affect another."""
        db_path = tmp_path / "botfarm.db"
        conn = init_db(db_path, allow_migration=True)
        conn.close()

        config_path = tmp_path / "config.yaml"
        config_data = {
            "projects": [
                {"name": "proj-a", "base_dir": str(tmp_path / "a"), "team": "A", "slots": [1]},
                {"name": "proj-b", "base_dir": str(tmp_path / "b"), "team": "B", "slots": [1]},
            ],
            "bugtracker": {"type": "linear", "api_key": "test-key"},
        }
        config_path.write_text(yaml.dump(config_data))
        monkeypatch.setenv("BOTFARM_DB_PATH", str(db_path))

        seed_slots(db_path, [
            make_slot("proj-a", 1),
            make_slot("proj-b", 1),
        ])

        result = runner.invoke(
            main, ["remove-project", "proj-a", "--config", str(config_path), "--yes"]
        )
        assert result.exit_code == 0, result.output

        # proj-b still in config
        data = yaml.safe_load(config_path.read_text())
        names = [p["name"] for p in data["projects"]]
        assert names == ["proj-b"]

        # proj-b slot still in DB
        conn = init_db(db_path)
        rows = load_all_slots(conn)
        conn.close()
        assert len(rows) == 1
        assert rows[0]["project"] == "proj-b"

    def test_paused_manual_is_active(self, runner, db_and_config):
        """paused_manual slots should count as active."""
        db_path, config_path = db_and_config
        seed_slots(db_path, [
            make_slot("my-project", 1, status="paused_manual", ticket_id="TST-2"),
        ])

        result = runner.invoke(
            main, ["remove-project", "my-project", "--config", str(config_path), "--yes"]
        )
        assert result.exit_code != 0
        assert "active slot" in result.output.lower()
