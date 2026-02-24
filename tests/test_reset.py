"""Tests for botfarm reset CLI command."""

import json
import os
import signal
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from botfarm.cli import main


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def state_file(tmp_path):
    return tmp_path / "state.json"


def _mock_resolve(state_file):
    """Return a monkeypatch-compatible _resolve_paths replacement."""
    return lambda _: (state_file, state_file.parent / "botfarm.db", None)


def _write_state(state_file, slots, **extra):
    """Write a state.json with slots and optional extra top-level keys."""
    data = {"slots": slots, **extra}
    state_file.write_text(json.dumps(data, indent=2))
    return data


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
    def test_reset_failed_slot(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("my-proj", 1, "failed", ticket_id="SMA-10"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "my-proj"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output
        assert "SMA-10" in result.output
        assert "failed" in result.output

        # Verify state file was updated
        data = json.loads(state_file.read_text())
        assert data["slots"][0]["status"] == "free"
        assert data["slots"][0]["ticket_id"] is None
        assert data["slots"][0]["pid"] is None

    def test_reset_multiple_non_free_slots(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "failed", ticket_id="T-1"),
            _make_slot("proj", 2, "completed_pending_cleanup", ticket_id="T-2"),
            _make_slot("proj", 3, "busy", ticket_id="T-3"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "Reset 3 slot(s)" in result.output

        data = json.loads(state_file.read_text())
        for slot in data["slots"]:
            assert slot["status"] == "free"
            assert slot["ticket_id"] is None

    def test_reset_preserves_free_slots(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "free"),
            _make_slot("proj", 2, "failed", ticket_id="T-1"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output

    def test_reset_clears_all_ticket_fields(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
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
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0

        data = json.loads(state_file.read_text())
        slot = data["slots"][0]
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
        assert slot["interrupted_by_limit"] is False
        assert slot["resume_after"] is None
        assert slot["stages_completed"] == []


# ---------------------------------------------------------------------------
# No-op when already free
# ---------------------------------------------------------------------------


class TestResetAlreadyFree:
    def test_all_slots_free(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "free"),
            _make_slot("proj", 2, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "already free" in result.output


# ---------------------------------------------------------------------------
# Live PID handling
# ---------------------------------------------------------------------------


class TestResetLivePid:
    def test_live_pid_with_force(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))
        monkeypatch.setattr("botfarm.cli._is_pid_alive", lambda pid: True)

        with patch("os.kill") as mock_kill:
            result = runner.invoke(main, ["reset", "proj", "--force"])

        assert result.exit_code == 0
        assert "SIGTERM" in result.output
        mock_kill.assert_called_once_with(99999, signal.SIGTERM)
        assert "Reset 1 slot(s)" in result.output

        data = json.loads(state_file.read_text())
        assert data["slots"][0]["status"] == "free"

    def test_live_pid_confirm_yes(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))
        monkeypatch.setattr("botfarm.cli._is_pid_alive", lambda pid: True)

        with patch("os.kill"):
            result = runner.invoke(main, ["reset", "proj"], input="y\n")

        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output

    def test_live_pid_confirm_no(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))
        monkeypatch.setattr("botfarm.cli._is_pid_alive", lambda pid: True)

        result = runner.invoke(main, ["reset", "proj"], input="n\n")
        assert result.exit_code == 0
        assert "Aborted" in result.output

        # State should be unchanged
        data = json.loads(state_file.read_text())
        assert data["slots"][0]["status"] == "busy"

    def test_dead_pid_no_confirmation(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1", pid=99999),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))
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
    def test_reset_all_projects(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj-a", 1, "failed", ticket_id="T-1"),
            _make_slot("proj-b", 1, "busy", ticket_id="T-2"),
            _make_slot("proj-b", 2, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "--all"])
        assert result.exit_code == 0
        assert "Reset 2 slot(s)" in result.output

        data = json.loads(state_file.read_text())
        for slot in data["slots"]:
            assert slot["status"] == "free"

    def test_reset_all_already_free(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj-a", 1, "free"),
            _make_slot("proj-b", 1, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "--all"])
        assert result.exit_code == 0
        assert "already free" in result.output


# ---------------------------------------------------------------------------
# Missing state file
# ---------------------------------------------------------------------------


class TestResetMissingState:
    def test_no_state_file(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.json", tmp_path / "botfarm.db", None),
        )
        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "No state file" in result.output

    def test_missing_project(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj-a", 1, "free"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "nonexistent"])
        assert result.exit_code == 0
        assert "not found" in result.output
        assert "proj-a" in result.output  # lists known projects


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestResetEdgeCases:
    def test_no_project_no_all_flag(self, runner, state_file, monkeypatch):
        _write_state(state_file, [_make_slot("proj", 1, "failed")])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset"])
        assert result.exit_code != 0
        assert "Provide a project name" in result.output

    def test_only_resets_matching_project(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj-a", 1, "failed", ticket_id="T-1"),
            _make_slot("proj-b", 1, "failed", ticket_id="T-2"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj-a"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output

        data = json.loads(state_file.read_text())
        proj_a = [s for s in data["slots"] if s["project"] == "proj-a"][0]
        proj_b = [s for s in data["slots"] if s["project"] == "proj-b"][0]
        assert proj_a["status"] == "free"
        assert proj_b["status"] == "failed"  # untouched

    def test_preserves_other_state_fields(self, runner, state_file, monkeypatch):
        """Non-slot top-level state fields (dispatch_paused, usage) are preserved."""
        _write_state(
            state_file,
            [_make_slot("proj", 1, "failed", ticket_id="T-1")],
            dispatch_paused=True,
            dispatch_pause_reason="test reason",
            usage={"some": "data"},
        )
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0

        data = json.loads(state_file.read_text())
        assert data["dispatch_paused"] is True
        assert data["dispatch_pause_reason"] == "test reason"
        assert data["usage"] == {"some": "data"}

    def test_legacy_list_format(self, runner, state_file, monkeypatch):
        """Reset works with the old bare-list state format."""
        state_file.write_text(json.dumps([
            _make_slot("proj", 1, "failed", ticket_id="T-1"),
        ]))
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "Reset 1 slot(s)" in result.output

    def test_summary_table_shows_previous_status(self, runner, state_file, monkeypatch):
        _write_state(state_file, [
            _make_slot("proj", 1, "busy", ticket_id="T-1"),
            _make_slot("proj", 2, "completed_pending_cleanup", ticket_id="T-2"),
        ])
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(state_file))

        result = runner.invoke(main, ["reset", "proj"])
        assert result.exit_code == 0
        assert "busy" in result.output
        assert "completed_pending_cleanup" in result.output
        assert "T-1" in result.output
        assert "T-2" in result.output
