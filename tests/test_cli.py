"""Tests for botfarm CLI status, history, and limits commands."""

import json
import sqlite3
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from botfarm.cli import _elapsed, main
from botfarm.db import (
    init_db,
    insert_stage_run,
    insert_task,
    insert_usage_snapshot,
    load_dispatch_state,
    save_dispatch_state,
    update_task,
)
from botfarm.usage import UsageState
from tests.helpers import make_slot as _make_slot, mock_resolve as _mock_resolve, seed_slots as _seed_slots


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
# _elapsed helper
# ---------------------------------------------------------------------------


class TestElapsed:
    def test_none_input(self):
        assert _elapsed(None) == "-"

    def test_empty_string(self):
        assert _elapsed("") == "-"

    def test_invalid_timestamp(self):
        assert _elapsed("not-a-date") == "-"

    def test_seconds_format(self):
        from datetime import datetime, timedelta, timezone

        ts = (datetime.now(timezone.utc) - timedelta(seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        result = _elapsed(ts)
        assert result.endswith("s")
        assert "m" not in result
        assert "h" not in result

    def test_minutes_format(self):
        from datetime import datetime, timedelta, timezone

        ts = (datetime.now(timezone.utc) - timedelta(minutes=5, seconds=30)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        result = _elapsed(ts)
        assert "m" in result
        assert "h" not in result

    def test_hours_format(self):
        from datetime import datetime, timedelta, timezone

        ts = (datetime.now(timezone.utc) - timedelta(hours=2, minutes=15)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        result = _elapsed(ts)
        assert "h" in result


# ---------------------------------------------------------------------------
# botfarm status
# ---------------------------------------------------------------------------


class TestStatusCommand:
    def test_no_database_no_config(self, runner, tmp_path, monkeypatch):
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))
        monkeypatch.setattr(
            "botfarm.cli.DEFAULT_CONFIG_PATH", tmp_path / "no-config.yaml",
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "No database found" in result.output
        assert "botfarm init" in result.output

    def test_no_database_valid_config(self, runner, tmp_path, monkeypatch):
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump({
                "projects": [
                    {
                        "name": "test",
                        "team": "TST",
                        "base_dir": "~/test",
                        "worktree_prefix": "test-slot-",
                        "slots": [1],
                    }
                ],
                "bugtracker": {"api_key": "test-key"},
            })
        )
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "Config valid" in result.output
        assert "botfarm run" in result.output

    def test_no_database_invalid_config(self, runner, tmp_path, monkeypatch):
        config_path = tmp_path / "config.yaml"
        config_path.write_text(
            yaml.dump({"projects": [{"name": "test"}]})
        )
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "Config error" in result.output

    def test_no_database_yaml_syntax_error(self, runner, tmp_path, monkeypatch):
        config_path = tmp_path / "config.yaml"
        config_path.write_text("projects:\n  - name: test\n    bad: [unclosed")
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "nonexistent.db"))
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "Config error" in result.output
        assert "YAML syntax error" in result.output

    def test_empty_slots(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "No slots configured" in result.output

    def test_free_slot(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("my-project", 1, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "my-project" in result.output
        assert "free" in result.output

    def test_busy_slot_with_ticket(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "botfarm", 2, "busy",
                ticket_id="SMA-42",
                ticket_title="Add feature X",
                stage="implement",
                stage_iteration=1,
                started_at="2026-02-12T10:00:00.000000Z",
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "botfarm" in result.output
        assert "SMA-42" in result.output
        assert "Add feature X" in result.output
        assert "implement" in result.output
        assert "busy" in result.output

    def test_stage_iteration_shown_when_gt_1(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "busy",
                ticket_id="T-1",
                stage="fix",
                stage_iteration=3,
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "iter 3" in result.output

    def test_multiple_slots(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj-a", 1, "free"),
            _make_slot(
                "proj-a", 2, "busy",
                ticket_id="T-1",
                ticket_title="Do stuff",
                stage="review",
                started_at="2026-02-12T10:00:00.000000Z",
                stage_iteration=1,
            ),
            _make_slot(
                "proj-b", 3, "failed",
                ticket_id="T-2",
                stage="implement",
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "proj-a" in result.output
        assert "proj-b" in result.output
        assert "free" in result.output
        assert "busy" in result.output
        assert "failed" in result.output

    def test_dispatch_paused_banner(self, runner, db_file, monkeypatch):
        _seed_slots(
            db_file,
            [_make_slot("proj", 1, "free")],
            dispatch_paused=True,
            dispatch_pause_reason="5-hour utilization 87.0% >= 85% threshold",
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "DISPATCH PAUSED" in result.output
        assert "5-hour" in result.output

    def test_no_dispatch_paused_banner_when_not_paused(self, runner, db_file, monkeypatch):
        _seed_slots(
            db_file,
            [_make_slot("proj", 1, "free")],
            dispatch_paused=False,
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "DISPATCH PAUSED" not in result.output

    def test_paused_limit_status(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "paused_limit",
                ticket_id="T-1",
                stage="implement",
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "paused_limit" in result.output

    def test_start_paused_shows_yellow_banner(self, runner, db_file, monkeypatch):
        _seed_slots(
            db_file,
            [_make_slot("proj", 1, "free")],
            dispatch_paused=True,
            dispatch_pause_reason="start_paused",
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "DISPATCH PAUSED" in result.output
        assert "waiting for user to start" in result.output
        assert "botfarm resume" in result.output

    def test_non_start_paused_shows_red_banner(self, runner, db_file, monkeypatch):
        _seed_slots(
            db_file,
            [_make_slot("proj", 1, "free")],
            dispatch_paused=True,
            dispatch_pause_reason="manual_pause",
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "DISPATCH PAUSED" in result.output
        assert "manual_pause" in result.output
        assert "waiting for user to start" not in result.output


# ---------------------------------------------------------------------------
# botfarm resume
# ---------------------------------------------------------------------------


class TestResumeCommand:
    def test_resume_project(self, runner, db_file, monkeypatch):
        """Resume with a project argument clears per-project pause."""
        from botfarm.db import load_all_project_pause_states, save_project_pause_state

        _seed_slots(db_file, [_make_slot("proj", 1, "free")])
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        save_project_pause_state(conn, project="proj", paused=True, reason="test")
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["resume", "proj"])
        assert result.exit_code == 0
        assert "resumed" in result.output

    def test_resume_global_clears_start_paused(self, runner, db_file, monkeypatch):
        """Resume without project clears global start_paused dispatch pause."""
        _seed_slots(
            db_file,
            [_make_slot("proj", 1, "free")],
            dispatch_paused=True,
            dispatch_pause_reason="start_paused",
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["resume"])
        assert result.exit_code == 0
        assert "Dispatch resumed" in result.output

        # Verify dispatch state was cleared in DB
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        paused, reason, _ = load_dispatch_state(conn)
        conn.close()
        assert not paused
        assert reason is None

    def test_resume_global_not_paused(self, runner, db_file, monkeypatch):
        """Resume without project when dispatch is not paused shows message."""
        _seed_slots(db_file, [_make_slot("proj", 1, "free")])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["resume"])
        assert result.exit_code == 0
        assert "not paused" in result.output

    def test_resume_global_different_reason(self, runner, db_file, monkeypatch):
        """Resume without project when paused for non-start_paused reason refuses."""
        _seed_slots(
            db_file,
            [_make_slot("proj", 1, "free")],
            dispatch_paused=True,
            dispatch_pause_reason="manual_pause",
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["resume"])
        assert result.exit_code == 0
        assert "different reason" in result.output
        assert "manual_pause" in result.output

        # Verify dispatch state was NOT cleared
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        paused, reason, _ = load_dispatch_state(conn)
        conn.close()
        assert paused
        assert reason == "manual_pause"

    def test_resume_global_no_database(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["resume"])
        assert result.exit_code == 0
        assert "No database found" in result.output


# ---------------------------------------------------------------------------
# botfarm history
# ---------------------------------------------------------------------------


class TestHistoryCommand:
    def test_no_database(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_empty_database(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "No tasks found" in result.output

    def test_shows_tasks(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        task_id = insert_task(
            conn,
            ticket_id="SMA-10",
            title="Add feature",
            project="my-proj",
            slot=1,
            status="completed",
        )
        update_task(
            conn,
            task_id,
            turns=50,
            started_at="2026-02-12T10:00:00.000000Z",
            completed_at="2026-02-12T10:30:00.000000Z",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setenv("COLUMNS", "200")
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "SMA-10" in result.output
        assert "my-proj" in result.output
        assert "completed" in result.output
        assert "50" in result.output
        assert "30m00s" in result.output

    def test_filter_by_project(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_task(
            conn,
            ticket_id="SMA-11",
            title="Task A",
            project="proj-a",
            slot=1,
            status="completed",
        )
        insert_task(
            conn,
            ticket_id="SMA-12",
            title="Task B",
            project="proj-b",
            slot=2,
            status="completed",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setenv("COLUMNS", "200")
        result = runner.invoke(main, ["history", "--project", "proj-a"])
        assert result.exit_code == 0
        assert "SMA-11" in result.output
        assert "SMA-12" not in result.output

    def test_filter_by_status(self, runner, db_file, monkeypatch):
        monkeypatch.setenv("COLUMNS", "200")
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_task(
            conn,
            ticket_id="SMA-20",
            title="Good",
            project="proj",
            slot=1,
            status="completed",
        )
        insert_task(
            conn,
            ticket_id="SMA-21",
            title="Bad",
            project="proj",
            slot=2,
            status="failed",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["history", "--status", "failed"])
        assert result.exit_code == 0
        assert "SMA-21" in result.output
        assert "SMA-20" not in result.output

    def test_limit_flag(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        for i in range(5):
            insert_task(
                conn,
                ticket_id=f"SMA-{100 + i}",
                title=f"Task {i}",
                project="proj",
                slot=1,
                status="completed",
            )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setenv("COLUMNS", "200")
        result = runner.invoke(main, ["history", "-n", "2"])
        assert result.exit_code == 0
        assert "Task History" in result.output
        # Verify exactly 2 rows shown (ticket IDs SMA-100..104 inserted, only 2 returned)
        shown = [f"SMA-{100 + i}" for i in range(5) if f"SMA-{100 + i}" in result.output]
        assert len(shown) == 2

    def test_failed_task_shows_red(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_task(
            conn,
            ticket_id="SMA-30",
            title="Broken",
            project="proj",
            slot=1,
            status="failed",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setenv("COLUMNS", "200")
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "failed" in result.output

    def test_duration_hours(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        task_id = insert_task(
            conn,
            ticket_id="SMA-40",
            title="Long task",
            project="proj",
            slot=1,
            status="completed",
        )
        update_task(
            conn,
            task_id,
            started_at="2026-02-12T08:00:00.000000Z",
            completed_at="2026-02-12T10:30:00.000000Z",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setenv("COLUMNS", "200")
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "2h30m" in result.output

    def test_shows_codex_columns(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        task_id = insert_task(
            conn, ticket_id="SMA-50", title="Codex task",
            project="proj", slot=1, status="completed",
        )
        insert_stage_run(conn, task_id=task_id, stage="implement",
                         input_tokens=10000, output_tokens=2000,
                         total_cost_usd=0.10)
        insert_stage_run(conn, task_id=task_id, stage="codex_review",
                         input_tokens=800_000, output_tokens=15_000,
                         exit_subtype="approved")
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setenv("COLUMNS", "200")
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "SMA-50" in result.output
        assert "Codex" in result.output
        assert "Cost" in result.output

    def test_no_codex_shows_dash(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_task(
            conn, ticket_id="SMA-51", title="No codex",
            project="proj", slot=1, status="completed",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setenv("COLUMNS", "200")
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "SMA-51" in result.output


# ---------------------------------------------------------------------------
# botfarm limits
# ---------------------------------------------------------------------------


class TestLimitsCommand:
    @pytest.fixture(autouse=True)
    def _mock_refresh(self, monkeypatch):
        """Prevent real API calls during limits command tests."""
        monkeypatch.setattr(
            "botfarm.cli.refresh_usage_snapshot", lambda conn: None
        )

    def test_no_database(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_no_snapshots(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "No usage snapshots" in result.output

    def test_shows_utilization(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_usage_snapshot(
            conn,
            utilization_5h=0.45,
            utilization_7d=0.30,
            resets_at="2026-02-12T15:00:00Z",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "45.0%" in result.output
        assert "30.0%" in result.output
        assert "2026-02-12T15:00:00Z" in result.output
        assert "no" in result.output  # dispatch not paused

    def test_high_utilization_paused(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_usage_snapshot(
            conn,
            utilization_5h=0.95,
            utilization_7d=0.80,
        )
        save_dispatch_state(
            conn,
            paused=True,
            reason="5-hour utilization 95.0% >= 85% threshold",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "95.0%" in result.output
        assert "80.0%" in result.output
        assert "YES" in result.output  # dispatch paused

    def test_null_utilization_values(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_usage_snapshot(conn)
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "Usage Limits" in result.output

    def test_dispatch_paused_from_db(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_usage_snapshot(
            conn,
            utilization_5h=0.50,
            utilization_7d=0.30,
        )
        save_dispatch_state(
            conn,
            paused=True,
            reason="5-hour utilization 87.0% >= 85% threshold",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "YES" in result.output
        assert "5-hour" in result.output

    def test_latest_snapshot_used(self, runner, db_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        # Insert old snapshot
        insert_usage_snapshot(
            conn,
            utilization_5h=0.10,
            utilization_7d=0.05,
        )
        # Insert newer snapshot
        insert_usage_snapshot(
            conn,
            utilization_5h=0.65,
            utilization_7d=0.50,
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        # Should show the latest values
        assert "65.0%" in result.output
        assert "50.0%" in result.output

    def test_calls_refresh_before_display(self, runner, db_file, monkeypatch):
        """limits command refreshes usage from API before reading DB."""
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        refresh_called = []

        def fake_refresh(conn):
            refresh_called.append(True)
            # Simulate a refresh that stores a fresh snapshot
            conn.execute(
                "INSERT INTO usage_snapshots (utilization_5h, utilization_7d, "
                "resets_at, created_at) VALUES (?, ?, ?, datetime('now'))",
                (0.77, 0.55, "2026-02-23T20:00:00Z"),
            )
            conn.commit()
            return UsageState(utilization_5h=0.77, utilization_7d=0.55)

        monkeypatch.setattr("botfarm.cli.refresh_usage_snapshot", fake_refresh)
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert len(refresh_called) == 1
        assert "77.0%" in result.output
        assert "55.0%" in result.output

    def test_falls_back_to_db_on_refresh_failure(self, runner, db_file, monkeypatch):
        """If API refresh fails, limits command still shows stale DB data."""
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_usage_snapshot(
            conn,
            utilization_5h=0.33,
            utilization_7d=0.22,
            resets_at="2026-02-20T10:00:00Z",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setattr(
            "botfarm.cli.refresh_usage_snapshot", lambda conn: None
        )
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "33.0%" in result.output
        assert "22.0%" in result.output


# ---------------------------------------------------------------------------
# botfarm init
# ---------------------------------------------------------------------------


class TestInitCommand:
    def test_creates_config_and_env(self, runner, tmp_path, monkeypatch):
        """init --non-interactive creates both config.yaml and .env when neither exists."""
        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
        result = runner.invoke(main, ["init", "--non-interactive"])
        assert result.exit_code == 0
        assert config_path.exists()
        assert env_path.exists()
        assert "Created default config" in result.output
        assert "Created default .env" in result.output
        assert "Linear API key" in result.output

    def test_env_file_contents(self, runner, tmp_path, monkeypatch):
        """The created .env file contains expected template content."""
        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
        runner.invoke(main, ["init", "--non-interactive"])
        content = env_path.read_text()
        assert "LINEAR_API_KEY=" in content
        assert "BOTFARM_DB_PATH" in content

    def test_skips_existing_config(self, runner, tmp_path, monkeypatch):
        """init --non-interactive skips config creation if it already exists, still creates .env."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text("existing")
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
        result = runner.invoke(main, ["init", "--non-interactive"])
        assert result.exit_code == 0
        assert "Config file already exists" in result.output
        assert "Created default .env" in result.output
        assert env_path.exists()
        # Original config content preserved
        assert config_path.read_text() == "existing"

    def test_skips_existing_env(self, runner, tmp_path, monkeypatch):
        """init --non-interactive skips .env creation if it already exists, still creates config."""
        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        env_path.write_text("existing")
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
        result = runner.invoke(main, ["init", "--non-interactive"])
        assert result.exit_code == 0
        assert "Created default config" in result.output
        assert ".env file already exists" in result.output
        # Original .env content preserved
        assert env_path.read_text() == "existing"

    def test_both_exist_no_next_step(self, runner, tmp_path, monkeypatch):
        """When both files exist, no 'Next step' message is shown."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text("existing")
        env_path = tmp_path / ".env"
        env_path.write_text("existing")
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
        result = runner.invoke(main, ["init", "--non-interactive"])
        assert result.exit_code == 0
        assert "Next step" not in result.output

    def test_next_step_message_mentions_env_path(self, runner, tmp_path, monkeypatch):
        """The 'Next step' message directs users to set their API key in .env."""
        config_path = tmp_path / "config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", config_path)
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
        result = runner.invoke(main, ["init", "--non-interactive"])
        assert result.exit_code == 0
        assert "Next step" in result.output
        assert str(env_path) in result.output

    def test_custom_config_path(self, runner, tmp_path, monkeypatch):
        """init --path creates config at custom location, .env at default."""
        custom_config = tmp_path / "custom" / "my-config.yaml"
        env_path = tmp_path / ".env"
        monkeypatch.setattr("botfarm.cli.ENV_FILE_PATH", env_path)
        result = runner.invoke(main, ["init", "--non-interactive", "--path", str(custom_config)])
        assert result.exit_code == 0
        assert custom_config.exists()
        assert env_path.exists()


# ---------------------------------------------------------------------------
# botfarm preflight
# ---------------------------------------------------------------------------


class TestPreflightCommand:
    def _make_config(self, dashboard_enabled=True, port=8420):
        """Build a minimal BotfarmConfig with dashboard settings."""
        from botfarm.config import BotfarmConfig, DashboardConfig

        cfg = BotfarmConfig.__new__(BotfarmConfig)
        cfg.dashboard = DashboardConfig(enabled=dashboard_enabled, port=port)
        return cfg

    def test_no_config_file(self, runner, db_file, monkeypatch):
        """preflight fails gracefully when config file is missing."""
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, None)
        )
        result = runner.invoke(main, ["preflight"])
        assert result.exit_code != 0
        assert "Config file not found" in result.output

    def test_dashboard_disabled(self, runner, db_file, monkeypatch):
        """preflight fails gracefully when dashboard is disabled."""
        cfg = self._make_config(dashboard_enabled=False)
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, cfg)
        )
        result = runner.invoke(main, ["preflight"])
        assert result.exit_code != 0
        assert "Dashboard is not enabled" in result.output

    def test_supervisor_not_running(self, runner, db_file, monkeypatch):
        """preflight fails gracefully when supervisor is not reachable."""
        from http.server import HTTPServer, BaseHTTPRequestHandler
        # Bind to port 0, read the assigned port, then immediately close —
        # guarantees no server is listening on that port.
        tmp_server = HTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
        unused_port = tmp_server.server_address[1]
        tmp_server.server_close()

        cfg = self._make_config(port=unused_port)
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, cfg)
        )
        result = runner.invoke(main, ["preflight"])
        assert result.exit_code != 0
        assert "Cannot reach dashboard" in result.output

    def test_successful_rerun(self, runner, db_file, monkeypatch):
        """preflight displays results after a successful re-run."""
        api_results = {
            "degraded": False,
            "failed_critical": 0,
            "checks": [
                {"name": "git_repo:proj", "passed": True, "message": "OK",
                 "critical": True, "guidance": ""},
                {"name": "credentials", "passed": True, "message": "OK",
                 "critical": False, "guidance": ""},
            ],
        }

        import json
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import threading

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "ok"}')

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(api_results).encode())

            def log_message(self, *args):
                pass  # suppress log noise

        server = HTTPServer(("127.0.0.1", 0), Handler)
        cfg = self._make_config(port=server.server_address[1])
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, cfg)
        )
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            result = runner.invoke(main, ["preflight"])
            assert result.exit_code == 0
            assert "git_repo:proj" in result.output
            assert "All critical checks passed" in result.output
        finally:
            server.shutdown()

    def test_degraded_mode_display(self, runner, db_file, monkeypatch):
        """preflight shows degraded mode warning when checks fail."""
        api_results = {
            "degraded": True,
            "failed_critical": 1,
            "checks": [
                {"name": "linear_api", "passed": False, "message": "Unreachable",
                 "critical": True, "guidance": "Check key"},
            ],
        }

        import json
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import threading

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "ok"}')

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(api_results).encode())

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        cfg = self._make_config(port=server.server_address[1])
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, cfg)
        )
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            result = runner.invoke(main, ["preflight"])
            assert result.exit_code == 0
            assert "DEGRADED MODE" in result.output
            assert "1 critical check" in result.output
        finally:
            server.shutdown()

    def test_no_rerun_flag(self, runner, db_file, monkeypatch):
        """preflight --no-rerun skips the POST and only fetches results."""
        api_results = {
            "degraded": False,
            "failed_critical": 0,
            "checks": [
                {"name": "database", "passed": True, "message": "OK",
                 "critical": True, "guidance": ""},
            ],
        }

        import json
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import threading

        post_called = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                post_called.append(True)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "ok"}')

            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(api_results).encode())

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        cfg = self._make_config(port=server.server_address[1])
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, cfg)
        )
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
        try:
            result = runner.invoke(main, ["preflight", "--no-rerun"])
            assert result.exit_code == 0
            assert "database" in result.output
            assert post_called == []  # POST should NOT have been called
        finally:
            server.shutdown()


# ---------------------------------------------------------------------------
# Setup mode — botfarm run with no/incomplete config
# ---------------------------------------------------------------------------


class TestRunSetupMode:
    def test_creates_skeleton_config_when_missing(self, runner, tmp_path, monkeypatch):
        """botfarm run creates a setup config skeleton if no config file exists."""
        cfg_path = tmp_path / "config.yaml"
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", cfg_path)
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        # Patch Supervisor to avoid running it
        with patch("botfarm.supervisor.Supervisor") as MockSup:
            MockSup.return_value.run.return_value = 0
            result = runner.invoke(main, ["run"])

        assert result.exit_code == 0
        assert cfg_path.exists()
        assert "Created setup config" in result.output
        assert "Setup mode" in result.output

        # Verify the created config is valid YAML with dashboard enabled
        data = yaml.safe_load(cfg_path.read_text())
        assert data["dashboard"]["enabled"] is True
        assert data["dashboard"]["host"] == "0.0.0.0"

    def test_setup_mode_forces_dashboard_on(self, runner, tmp_path, monkeypatch):
        """Setup mode enables dashboard and respects configured host."""
        cfg_path = tmp_path / "config.yaml"
        cfg_path.write_text(yaml.dump({
            "bugtracker": {"api_key": ""},
            "dashboard": {"enabled": False, "host": "0.0.0.0", "port": 9999},
        }))
        monkeypatch.setattr("botfarm.cli.DEFAULT_CONFIG_PATH", cfg_path)
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        with patch("botfarm.supervisor.Supervisor") as MockSup:
            mock_instance = MockSup.return_value
            mock_instance.run.return_value = 0
            result = runner.invoke(main, ["run"])

        assert result.exit_code == 0
        assert "Setup mode" in result.output
        # Verify the config passed to Supervisor has dashboard enabled + configured host preserved
        config_arg = MockSup.call_args[0][0]
        assert config_arg.dashboard.enabled is True
        assert config_arg.dashboard.host == "0.0.0.0"

    def test_explicit_config_path_errors_when_missing(self, runner, tmp_path):
        """An explicit --config path that doesn't exist raises an error instead of auto-creating."""
        missing = tmp_path / "nonexistent" / "conifg.yaml"
        result = runner.invoke(main, ["run", "--config", str(missing)])
        assert result.exit_code != 0
        assert "Config file not found" in result.output
        assert not missing.exists()
