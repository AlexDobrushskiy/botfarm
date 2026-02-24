"""Tests for botfarm reset CLI command."""

import json
import os
import signal
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from botfarm.cli import main
from botfarm.db import init_db, load_all_slots, save_dispatch_state, upsert_slot


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def db_file(tmp_path):
    """Create an initialized database and return its path."""
    path = tmp_path / "botfarm.db"
    conn = init_db(path)
    conn.close()
    return path


def _mock_resolve(db_path):
    """Return a monkeypatch-compatible _resolve_paths replacement."""
    return lambda _: (db_path, db_path, None)


def _seed_slots(db_path, slots, *, dispatch_paused=False, dispatch_pause_reason=None):
    """Seed the database with slot rows and optional dispatch state."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    for slot in slots:
        upsert_slot(conn, slot)
    save_dispatch_state(
        conn,
        paused=dispatch_paused,
        reason=dispatch_pause_reason,
    )
    conn.commit()
    conn.close()


def _read_slots(db_path):
    """Read all slots from DB as a list of dicts."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM slots ORDER BY project, slot_id").fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


def _make_slot(project, slot_id, status="free", **overrides):
    """Create a slot dict with sensible defaults."""
    slot = {
        "project": project,
        "slot_id": slot_id,
        "status": status,
        "ticket_id": None,
        "ticket_title": None,
        "branch": None,
        "pr_url": None,
        "stage": None,
        "stage_iteration": 0,
        "current_session_id": None,
        "started_at": None,
        "stage_started_at": None,
        "sigterm_sent_at": None,
        "pid": None,
        "interrupted_by_limit": False,
        "resume_after": None,
        "stages_completed": [],
    }
    slot.update(overrides)
    return slot


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestResetHappyPath:
    def test_reset_failed_slot(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("my-proj", 1, "failed", ticket_id="SMA-10"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "my-proj"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output
        assert "SMA-10" in result.output
        assert "failed" in result.output

        # Verify DB was updated
        slots = _read_slots(db_file)
        assert slots[0]["status"] == "free"
        assert slots[0]["ticket_id"] is None
        assert slots[0]["pid"] is None

    def test_reset_multiple_non_free_slots(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "failed", ticket_id="T-1"),
            _make_slot("proj", 2, "completed_pending_cleanup", ticket_id="T-2"),
            _make_slot("proj", 3, "busy", ticket_id="T-3"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "Reset 3 slot(s)" in result.output

        slots = _read_slots(db_file)
        for slot in slots:
            assert slot["status"] == "free"
            assert slot["ticket_id"] is None

    def test_reset_preserves_free_slots(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "free"),
            _make_slot("proj", 2, "failed", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output

    def test_reset_clears_all_ticket_fields(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot(
                "proj", 1, "busy",
                ticket_id="SMA-42",
                ticket_title="Some task",
                branch="feat/branch",
                pr_url="https://github.com/org/repo/pull/1",
                stage="implement",
                stage_iteration=3,
                current_session_id="sess-abc",
                started_at="2026-02-24T10:00:00Z",
                stage_started_at="2026-02-24T11:00:00Z",
                sigterm_sent_at="2026-02-24T12:00:00Z",
                interrupted_by_limit=True,
                resume_after="2026-02-24T13:00:00Z",
                stages_completed=["implement", "review"],
            ),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0

        slots = _read_slots(db_file)
        slot = slots[0]
        assert slot["status"] == "free"
        assert slot["ticket_id"] is None
        assert slot["ticket_title"] is None
        assert slot["branch"] is None
        assert slot["pr_url"] is None
        assert slot["stage"] is None
        assert slot["stage_iteration"] == 0
        assert slot["current_session_id"] is None
        assert slot["started_at"] is None
        assert slot["stage_started_at"] is None
        assert slot["sigterm_sent_at"] is None
        assert slot["pid"] is None
        assert slot["interrupted_by_limit"] == 0
        assert slot["resume_after"] is None
        assert json.loads(slot["stages_completed"]) == []


# ---------------------------------------------------------------------------
# No-op when already free
# ---------------------------------------------------------------------------


class TestResetAlreadyFree:
    def test_all_slots_free(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "free"),
            _make_slot("proj", 2, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "already free" in result.output


# ---------------------------------------------------------------------------
# Live PID handling
# ---------------------------------------------------------------------------


class TestResetLivePid:
    def test_live_pid_with_force(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setattr("botfarm.cli._is_pid_alive", lambda pid: True)

        with patch("os.kill") as mock_kill:
            result = runner.invoke(main, ["reset", "proj", "--force"])

        assert result.exit_code == 0
        assert "SIGTERM" in result.output
        mock_kill.assert_called_once_with(99999, signal.SIGTERM)
        assert "Reset 1 slot(s)" in result.output

        slots = _read_slots(db_file)
        assert slots[0]["status"] == "free"

    def test_live_pid_confirm_yes(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setattr("botfarm.cli._is_pid_alive", lambda pid: True)

        with patch("os.kill"):
            result = runner.invoke(main, ["reset", "proj"], input="y\n")

        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output

    def test_live_pid_confirm_no(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setattr("botfarm.cli._is_pid_alive", lambda pid: True)

        result = runner.invoke(main, ["reset", "proj"], input="n\n")
        assert result.exit_code == 0
        assert "Aborted" in result.output

        # State should be unchanged
        slots = _read_slots(db_file)
        assert slots[0]["status"] == "busy"

    def test_dead_pid_no_confirmation(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        monkeypatch.setattr("botfarm.cli._is_pid_alive", lambda pid: False)

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output
        # Should not prompt when PID is dead
        assert "SIGTERM" not in result.output


# ---------------------------------------------------------------------------
# --all flag
# ---------------------------------------------------------------------------


class TestResetAll:
    def test_reset_all_projects(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj-a", 1, "failed", ticket_id="T-1"),
            _make_slot("proj-b", 1, "busy", ticket_id="T-2"),
            _make_slot("proj-b", 2, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "--all"])
        assert result.exit_code == 0
        assert "Reset 2 slot(s)" in result.output

        slots = _read_slots(db_file)
        for slot in slots:
            assert slot["status"] == "free"

    def test_reset_all_already_free(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj-a", 1, "free"),
            _make_slot("proj-b", 1, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "--all"])
        assert result.exit_code == 0
        assert "already free" in result.output


# ---------------------------------------------------------------------------
# Missing database
# ---------------------------------------------------------------------------


class TestResetMissingState:
    def test_no_database(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.db", tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_missing_project(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj-a", 1, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "nonexistent"])
        assert result.exit_code == 0
        assert "not found" in result.output
        assert "proj-a" in result.output  # lists known projects


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestResetEdgeCases:
    def test_no_project_no_all_flag(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [_make_slot("proj", 1, "failed")])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset"])
        assert result.exit_code != 0
        assert "Provide a project name" in result.output

    def test_only_resets_matching_project(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj-a", 1, "failed", ticket_id="T-1"),
            _make_slot("proj-b", 1, "failed", ticket_id="T-2"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "proj-a"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output

        slots = _read_slots(db_file)
        proj_a = [s for s in slots if s["project"] == "proj-a"][0]
        proj_b = [s for s in slots if s["project"] == "proj-b"][0]
        assert proj_a["status"] == "free"
        assert proj_b["status"] == "failed"  # untouched

    def test_preserves_dispatch_state(self, runner, db_file, monkeypatch):
        """Dispatch state (paused, reason) is preserved across reset."""
        _seed_slots(
            db_file,
            [_make_slot("proj", 1, "failed", ticket_id="T-1")],
            dispatch_paused=True,
            dispatch_pause_reason="test reason",
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0

        # Verify dispatch_state is unchanged
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT paused, pause_reason FROM dispatch_state WHERE id = 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert bool(row["paused"]) is True
        assert row["pause_reason"] == "test reason"

    def test_summary_table_shows_previous_status(self, runner, db_file, monkeypatch):
        _seed_slots(db_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
            _make_slot("proj", 2, "completed_pending_cleanup", ticket_id="T-2"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "busy" in result.output
        assert "completed_pending_cleanup" in result.output
        assert "T-1" in result.output
        assert "T-2" in result.output
