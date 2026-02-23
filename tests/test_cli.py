"""Tests for botfarm CLI status, history, and limits commands."""

import json
import sqlite3
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from botfarm.cli import _elapsed, main
from botfarm.db import SCHEMA_SQL, insert_task, insert_usage_snapshot, update_task
from botfarm.usage import UsageState


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def state_file(tmp_path):
    return tmp_path / "state.json"


@pytest.fixture()
def db_file(tmp_path):
    db_path = tmp_path / "botfarm.db"
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA_SQL)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)"
    )
    conn.execute("INSERT INTO schema_version (version) VALUES (1)")
    conn.commit()
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
    def test_no_state_file(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.json", tmp_path / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "No state file found" in result.output

    def test_empty_slots_list(self, runner, state_file, monkeypatch):
        state_file.write_text("[]")
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "No slots configured" in result.output

    def test_invalid_json(self, runner, state_file, monkeypatch):
        state_file.write_text("{invalid json")
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code != 0
        assert "Failed to read state file" in result.output

    def test_free_slot(self, runner, state_file, monkeypatch):
        state_file.write_text(
            json.dumps(
                [
                    {
                        "project": "my-project",
                        "slot_id": 1,
                        "status": "free",
                        "ticket_id": None,
                        "ticket_title": None,
                        "stage": None,
                        "started_at": None,
                        "stage_iteration": 0,
                    }
                ]
            )
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "my-project" in result.output
        assert "free" in result.output

    def test_busy_slot_with_ticket(self, runner, state_file, monkeypatch):
        state_file.write_text(
            json.dumps(
                [
                    {
                        "project": "botfarm",
                        "slot_id": 2,
                        "status": "busy",
                        "ticket_id": "SMA-42",
                        "ticket_title": "Add feature X",
                        "stage": "implement",
                        "stage_iteration": 1,
                        "started_at": "2026-02-12T10:00:00.000000Z",
                    }
                ]
            )
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "botfarm" in result.output
        assert "SMA-42" in result.output
        assert "Add feature X" in result.output
        assert "implement" in result.output
        assert "busy" in result.output

    def test_stage_iteration_shown_when_gt_1(self, runner, state_file, monkeypatch):
        state_file.write_text(
            json.dumps(
                [
                    {
                        "project": "proj",
                        "slot_id": 1,
                        "status": "busy",
                        "ticket_id": "T-1",
                        "ticket_title": None,
                        "stage": "fix",
                        "stage_iteration": 3,
                        "started_at": None,
                    }
                ]
            )
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "iter 3" in result.output

    def test_multiple_slots(self, runner, state_file, monkeypatch):
        slots = [
            {
                "project": "proj-a",
                "slot_id": 1,
                "status": "free",
                "ticket_id": None,
                "ticket_title": None,
                "stage": None,
                "started_at": None,
                "stage_iteration": 0,
            },
            {
                "project": "proj-a",
                "slot_id": 2,
                "status": "busy",
                "ticket_id": "T-1",
                "ticket_title": "Do stuff",
                "stage": "review",
                "started_at": "2026-02-12T10:00:00.000000Z",
                "stage_iteration": 1,
            },
            {
                "project": "proj-b",
                "slot_id": 3,
                "status": "failed",
                "ticket_id": "T-2",
                "ticket_title": None,
                "stage": "implement",
                "started_at": None,
                "stage_iteration": 0,
            },
        ]
        state_file.write_text(json.dumps(slots))
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "proj-a" in result.output
        assert "proj-b" in result.output
        assert "free" in result.output
        assert "busy" in result.output
        assert "failed" in result.output

    def test_dispatch_paused_banner(self, runner, state_file, monkeypatch):
        state_file.write_text(
            json.dumps(
                {
                    "slots": [
                        {
                            "project": "proj",
                            "slot_id": 1,
                            "status": "free",
                            "ticket_id": None,
                            "ticket_title": None,
                            "stage": None,
                            "started_at": None,
                            "stage_iteration": 0,
                        }
                    ],
                    "dispatch_paused": True,
                    "dispatch_pause_reason": "5-hour utilization 87.0% >= 85% threshold",
                }
            )
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "DISPATCH PAUSED" in result.output
        assert "5-hour" in result.output

    def test_no_dispatch_paused_banner_when_not_paused(self, runner, state_file, monkeypatch):
        state_file.write_text(
            json.dumps(
                {
                    "slots": [
                        {
                            "project": "proj",
                            "slot_id": 1,
                            "status": "free",
                            "ticket_id": None,
                            "ticket_title": None,
                            "stage": None,
                            "started_at": None,
                            "stage_iteration": 0,
                        }
                    ],
                    "dispatch_paused": False,
                }
            )
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "DISPATCH PAUSED" not in result.output

    def test_paused_limit_status(self, runner, state_file, monkeypatch):
        state_file.write_text(
            json.dumps(
                [
                    {
                        "project": "proj",
                        "slot_id": 1,
                        "status": "paused_limit",
                        "ticket_id": "T-1",
                        "ticket_title": None,
                        "stage": "implement",
                        "started_at": None,
                        "stage_iteration": 0,
                    }
                ]
            )
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, state_file.parent / "botfarm.db", None),
        )
        result = runner.invoke(main, ["status"])
        assert result.exit_code == 0
        assert "paused_limit" in result.output


# ---------------------------------------------------------------------------
# botfarm history
# ---------------------------------------------------------------------------


class TestHistoryCommand:
    def test_no_database(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "state.json", tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_empty_database(self, runner, db_file, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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
            cost_usd=1.23,
            turns=50,
            started_at="2026-02-12T10:00:00.000000Z",
            completed_at="2026-02-12T10:30:00.000000Z",
        )
        conn.commit()
        conn.close()

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "SMA-10" in result.output
        assert "my-proj" in result.output
        assert "completed" in result.output
        assert "1.23" in result.output
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
        result = runner.invoke(main, ["history", "--project", "proj-a"])
        assert result.exit_code == 0
        assert "SMA-11" in result.output
        assert "SMA-12" not in result.output

    def test_filter_by_status(self, runner, db_file, monkeypatch):
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
        result = runner.invoke(main, ["history"])
        assert result.exit_code == 0
        assert "2h30m" in result.output


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
            lambda _: (tmp_path / "state.json", tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_no_snapshots(self, runner, db_file, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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
        conn.commit()
        conn.close()

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "Usage Limits" in result.output

    def test_dispatch_paused_from_state_file(self, runner, db_file, state_file, monkeypatch):
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        insert_usage_snapshot(
            conn,
            utilization_5h=0.50,
            utilization_7d=0.30,
        )
        conn.commit()
        conn.close()

        state_file.write_text(
            json.dumps(
                {
                    "slots": [],
                    "dispatch_paused": True,
                    "dispatch_pause_reason": "5-hour utilization 87.0% >= 85% threshold",
                }
            )
        )

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (state_file, db_file, None),
        )
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        # Should show the latest values
        assert "65.0%" in result.output
        assert "50.0%" in result.output

    def test_calls_refresh_before_display(self, runner, db_file, monkeypatch):
        """limits command refreshes usage from API before reading DB."""
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
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

        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file.parent / "state.json", db_file, None),
        )
        monkeypatch.setattr(
            "botfarm.cli.refresh_usage_snapshot", lambda conn: None
        )
        result = runner.invoke(main, ["limits"])
        assert result.exit_code == 0
        assert "33.0%" in result.output
        assert "22.0%" in result.output
