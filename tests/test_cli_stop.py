"""Tests for botfarm stop CLI command."""

import os
import sqlite3
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from botfarm.cli import main
from botfarm.config import BotfarmConfig, IdentitiesConfig, ProjectConfig
from botfarm.db import init_db, insert_task
from tests.helpers import (
    make_slot as _make_slot,
    mock_resolve as _mock_resolve,
    seed_slots as _seed_slots,
)


def _make_config(tmp_path, project_name="proj"):
    """Create a minimal BotfarmConfig with a worktree dir that exists."""
    worktree_dir = tmp_path / f"botfarm-{project_name}-1"
    worktree_dir.mkdir(exist_ok=True)
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name=project_name,
                linear_team="TEST",
                base_dir=str(tmp_path / f"botfarm-{project_name}"),
                worktree_prefix=f"botfarm-{project_name}-",
                slots=[1],
            ),
        ],
        identities=IdentitiesConfig(),
    )


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def db_file(tmp_path):
    db_path = tmp_path / "botfarm.db"
    conn = init_db(db_path, allow_migration=True)
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# No database / no active slots
# ---------------------------------------------------------------------------


class TestStopNoActiveSlots:
    def test_no_database(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["stop", "--force"])
        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_no_active_slots(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["stop", "--force"])
        assert result.exit_code == 0
        assert "No active slots" in result.output

    def test_no_active_slots_for_project(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj-a", 1, "busy", ticket_id="T-1"),
            _make_slot("proj-b", 1, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["stop", "proj-b", "--force"])
        assert result.exit_code == 0
        assert "No active slots" in result.output


# ---------------------------------------------------------------------------
# Argument resolution
# ---------------------------------------------------------------------------


class TestStopArgumentResolution:
    def test_auto_selects_single_project(self, runner, db_file, monkeypatch):
        """When only one project has active slots, it is auto-selected."""
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", ticket_title="Task"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        # Mock subprocess calls to avoid real git/gh operations
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "--force"])
        assert result.exit_code == 0
        assert "Stopped slot proj/1" in result.output

    def test_auto_selects_single_slot(self, runner, db_file, monkeypatch):
        """When project has only one active slot, it is auto-selected."""
        _seed_slots(db_file, [
            _make_slot("proj", 1, "free"),
            _make_slot("proj", 2, "busy", ticket_id="T-2", ticket_title="Job"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "--force"])
        assert result.exit_code == 0
        assert "Stopped slot proj/2" in result.output

    def test_explicit_project_and_slot(self, runner, db_file, monkeypatch):
        """Explicit project + slot_id skips interactive selection."""
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
            _make_slot("proj", 2, "busy", ticket_id="T-2"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "2", "--force"])
        assert result.exit_code == 0
        assert "Stopped slot proj/2" in result.output

    def test_slot_not_active(self, runner, db_file, monkeypatch):
        """Specifying a free slot shows an error."""
        _seed_slots(db_file, [
            _make_slot("proj", 1, "free"),
            _make_slot("proj", 2, "busy", ticket_id="T-2"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert "not active" in result.output

    def test_interactive_project_selection(self, runner, db_file, monkeypatch):
        """When multiple projects are active, prompts for selection."""
        _seed_slots(db_file, [
            _make_slot("alpha", 1, "busy", ticket_id="T-1"),
            _make_slot("beta", 1, "busy", ticket_id="T-2"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            # Select project 1 (alpha), then auto-selects only slot
            result = runner.invoke(main, ["stop", "--force"], input="1\n")
        assert result.exit_code == 0
        assert "Active projects" in result.output
        assert "Stopped slot alpha/1" in result.output

    def test_interactive_slot_selection(self, runner, db_file, monkeypatch):
        """When project has multiple active slots, prompts for selection."""
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "busy",
                ticket_id="T-1", ticket_title="First task", stage="implement",
            ),
            _make_slot(
                "proj", 2, "paused_limit",
                ticket_id="T-2", ticket_title="Second task", stage="review",
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            # Select slot 2
            result = runner.invoke(main, ["stop", "proj", "--force"], input="2\n")
        assert result.exit_code == 0
        assert "Active slots" in result.output
        assert "Stopped slot proj/2" in result.output


# ---------------------------------------------------------------------------
# Confirmation prompt
# ---------------------------------------------------------------------------


class TestStopConfirmation:
    def test_abort_on_no(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", stage="implement"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["stop", "proj", "1"], input="n\n")
        assert result.exit_code == 0
        assert "Aborted" in result.output

    def test_confirmation_shows_details(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "busy",
                ticket_id="T-1", ticket_title="My task",
                stage="implement", branch="feat/my-branch",
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["stop", "proj", "1"], input="n\n")
        assert "Stop slot proj/1" in result.output
        assert "T-1" in result.output
        assert "My task" in result.output
        assert "implement" in result.output
        assert "feat/my-branch" in result.output

    def test_force_skips_confirmation(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert "Stopped slot" in result.output
        # No confirmation was asked
        assert "Continue?" not in result.output

    def test_yes_flag_skips_confirmation(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "-y"])
        assert result.exit_code == 0
        assert "Stopped slot" in result.output


# ---------------------------------------------------------------------------
# Execution and cleanup
# ---------------------------------------------------------------------------


class TestStopExecution:
    def test_kills_worker_pid(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        kill_calls = []

        def fake_kill(pid):
            kill_calls.append(pid)
            return True

        with patch("botfarm.cli._stop_kill_worker", side_effect=fake_kill), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert kill_calls == [99999]
        assert "PID 99999 killed" in result.output

    def test_closes_pr(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "busy",
                ticket_id="T-1", pr_url="https://github.com/org/repo/pull/42",
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, True)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert "PR closed" in result.output

    def test_pr_already_merged_warning(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "busy",
                ticket_id="T-1", pr_url="https://github.com/org/repo/pull/42",
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert "already merged" in result.output
        assert 'moved to "Done"' in result.output

    def test_branch_deleted_in_output(self, runner, db_file, tmp_path, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "busy",
                ticket_id="T-1", branch="feat/my-branch",
            ),
        ])
        config = _make_config(tmp_path)
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config),
        )
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, True)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert "feat/my-branch deleted" in result.output

    def test_ticket_moved_to_todo(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        linear_calls = []

        def fake_linear(config, ticket_id, project_name, pr_was_merged):
            linear_calls.append((ticket_id, pr_was_merged))
            return True

        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup", side_effect=fake_linear):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert linear_calls == [("T-1", False)]
        assert 'moved to "Todo"' in result.output


# ---------------------------------------------------------------------------
# DB state after stop
# ---------------------------------------------------------------------------


class TestStopDbState:
    def test_slot_freed_in_db(self, runner, db_file, tmp_path, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
        ])
        config = _make_config(tmp_path)
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config),
        )
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0

        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM slots WHERE project=? AND slot_id=?",
            ("proj", 1),
        ).fetchone()
        conn.close()
        assert row["status"] == "free"
        assert row["ticket_id"] is None

    def test_task_marked_failed_on_normal_stop(self, runner, db_file, monkeypatch):
        # Insert a task for the ticket
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_task(conn, ticket_id="T-1", title="Task", project="proj", slot=1, status="running")
        conn.commit()
        conn.close()

        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0

        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        task = conn.execute("SELECT * FROM tasks WHERE ticket_id='T-1'").fetchone()
        conn.close()
        assert task["status"] == "failed"
        assert task["failure_reason"] == "stopped by user"

    def test_task_marked_completed_when_pr_merged(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_task(conn, ticket_id="T-1", title="Task", project="proj", slot=1, status="running")
        conn.commit()
        conn.close()

        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0

        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        task = conn.execute("SELECT * FROM tasks WHERE ticket_id='T-1'").fetchone()
        conn.close()
        assert task["status"] == "completed"

    def test_event_inserted(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0

        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        event = conn.execute(
            "SELECT * FROM task_events WHERE event_type='slot_stopped'"
        ).fetchone()
        conn.close()
        assert event is not None
        assert "proj" in event["detail"]
        assert "T-1" in event["detail"]


# ---------------------------------------------------------------------------
# Paused slot support
# ---------------------------------------------------------------------------


class TestStopPausedSlots:
    def test_can_stop_paused_limit_slot(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "paused_limit", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert "Stopped slot proj/1" in result.output

    def test_can_stop_paused_manual_slot(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "paused_manual", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch("botfarm.cli._stop_kill_worker", return_value=False), \
             patch("botfarm.cli._stop_pr_cleanup", return_value=(False, False)), \
             patch("botfarm.cli._stop_git_cleanup", return_value=(True, False)), \
             patch("botfarm.cli._stop_linear_cleanup"):
            result = runner.invoke(main, ["stop", "proj", "1", "--force"])
        assert result.exit_code == 0
        assert "Stopped slot proj/1" in result.output
