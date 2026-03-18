"""Tests for botfarm backfill-history CLI command."""

import sqlite3
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from botfarm.bugtracker.types import IssueDetails
from botfarm.cli import main
from botfarm.db import (
    get_ticket_history_entry,
    init_db,
    insert_task,
    upsert_ticket_history,
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


from tests.helpers import mock_resolve as _mock_resolve


def _seed_tasks(db_path, tasks):
    """Insert tasks into the database."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    for task in tasks:
        insert_task(conn, **task)
    conn.commit()
    conn.close()


def _make_config():
    """Create a minimal mock config with a Linear API key."""
    config = MagicMock()
    config.linear.api_key = "test-api-key"
    return config


def _fake_issue_details(ticket_id):
    """Return an IssueDetails mimicking LinearClient.fetch_issue_details()."""
    return IssueDetails(
        id=f"uuid-{ticket_id}",
        ticket_id=ticket_id,
        title=f"Title for {ticket_id}",
        url=f"https://linear.app/test/{ticket_id}",
        description=f"Description for {ticket_id}",
        status="Done",
        priority=2,
        assignee_name="Test User",
        assignee_email="test@example.com",
        creator_name="Creator",
        project_name="Bot farm",
        team_name="Smart AI Coach",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-02T00:00:00Z",
        completed_at="2026-01-03T00:00:00Z",
    )


class TestBackfillHistory:
    def test_no_config_raises_error(self, runner, db_file, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config=None)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-1", "title": "Task 1", "project": "proj", "slot": 1},
        ])
        result = runner.invoke(main, ["backfill-history"])
        assert result.exit_code != 0
        assert "Config file required" in result.output

    def test_no_database(self, runner, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.db", None),
        )
        result = runner.invoke(main, ["backfill-history"])
        assert result.exit_code == 0
        assert "No database found" in result.output

    def test_no_tasks(self, runner, db_file, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config=None)
        )
        result = runner.invoke(main, ["backfill-history"])
        assert result.exit_code == 0
        assert "No tickets found" in result.output

    @patch("botfarm.bugtracker.create_client")
    def test_discovers_tickets_from_tasks(self, mock_client_cls, runner, db_file, monkeypatch):
        """Command discovers ticket_ids from the tasks table and fetches them."""
        config = _make_config()
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-1", "title": "Task 1", "project": "proj-a", "slot": 1},
            {"ticket_id": "SMA-2", "title": "Task 2", "project": "proj-a", "slot": 2},
        ])

        mock_client = MagicMock()
        mock_client.fetch_issue_details.side_effect = lambda tid: _fake_issue_details(tid)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(main, ["backfill-history"])
        assert result.exit_code == 0
        assert "Backfilling 2 ticket(s)" in result.output
        assert "2 fetched" in result.output
        assert "0 failed" in result.output

        # Verify ticket_history was populated
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        entry = get_ticket_history_entry(conn, "SMA-1")
        assert entry is not None
        assert entry["capture_source"] == "backfill"
        assert entry["title"] == "Title for SMA-1"
        conn.close()

    @patch("botfarm.bugtracker.create_client")
    def test_skips_already_captured(self, mock_client_cls, runner, db_file, monkeypatch):
        """Tickets already in ticket_history are skipped by default."""
        config = _make_config()
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-10", "title": "Task 10", "project": "proj", "slot": 1},
            {"ticket_id": "SMA-11", "title": "Task 11", "project": "proj", "slot": 2},
        ])

        # Pre-populate SMA-10 in ticket_history
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        upsert_ticket_history(
            conn,
            ticket_id="SMA-10",
            title="Existing",
            capture_source="dispatch",
        )
        conn.commit()
        conn.close()

        mock_client = MagicMock()
        mock_client.fetch_issue_details.side_effect = lambda tid: _fake_issue_details(tid)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(main, ["backfill-history"])
        assert result.exit_code == 0
        assert "Backfilling 1 ticket(s)" in result.output
        assert "1 already in ticket_history" in result.output
        # Only SMA-11 should have been fetched
        mock_client.fetch_issue_details.assert_called_once_with("SMA-11")

    @patch("botfarm.bugtracker.create_client")
    def test_force_refetches_existing(self, mock_client_cls, runner, db_file, monkeypatch):
        """--force re-fetches tickets even if they already exist."""
        config = _make_config()
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-10", "title": "Task 10", "project": "proj", "slot": 1},
        ])

        # Pre-populate SMA-10
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        upsert_ticket_history(
            conn,
            ticket_id="SMA-10",
            title="Old title",
            capture_source="dispatch",
        )
        conn.commit()
        conn.close()

        mock_client = MagicMock()
        mock_client.fetch_issue_details.side_effect = lambda tid: _fake_issue_details(tid)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(main, ["backfill-history", "--force"])
        assert result.exit_code == 0
        assert "Backfilling 1 ticket(s)" in result.output
        mock_client.fetch_issue_details.assert_called_once_with("SMA-10")

        # Verify the title was updated
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        entry = get_ticket_history_entry(conn, "SMA-10")
        assert entry["title"] == "Title for SMA-10"
        assert entry["capture_source"] == "backfill"
        conn.close()

    @patch("botfarm.bugtracker.create_client")
    def test_handles_deleted_tickets(self, mock_client_cls, runner, db_file, monkeypatch):
        """Gracefully handles tickets that no longer exist in Linear."""
        from botfarm.linear import LinearAPIError

        config = _make_config()
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-1", "title": "Exists", "project": "proj", "slot": 1},
            {"ticket_id": "SMA-2", "title": "Deleted", "project": "proj", "slot": 2},
        ])

        mock_client = MagicMock()

        def side_effect(tid):
            if tid == "SMA-2":
                raise LinearAPIError(f"Issue '{tid}' not found")
            return _fake_issue_details(tid)

        mock_client.fetch_issue_details.side_effect = side_effect
        mock_client_cls.return_value = mock_client

        result = runner.invoke(main, ["backfill-history"])
        assert result.exit_code == 0
        assert "Warning: failed to fetch SMA-2 from Linear" in result.output
        assert "1 fetched" in result.output
        assert "1 failed" in result.output

    @patch("botfarm.bugtracker.create_client")
    def test_project_filter(self, mock_client_cls, runner, db_file, monkeypatch):
        """--project filters to only tickets from the specified project."""
        config = _make_config()
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-1", "title": "Task A", "project": "proj-a", "slot": 1},
            {"ticket_id": "SMA-2", "title": "Task B", "project": "proj-b", "slot": 2},
        ])

        mock_client = MagicMock()
        mock_client.fetch_issue_details.side_effect = lambda tid: _fake_issue_details(tid)
        mock_client_cls.return_value = mock_client

        result = runner.invoke(main, ["backfill-history", "--project", "proj-a"])
        assert result.exit_code == 0
        assert "Backfilling 1 ticket(s)" in result.output
        mock_client.fetch_issue_details.assert_called_once_with("SMA-1")

    @patch("botfarm.bugtracker.create_client")
    def test_dry_run(self, mock_client_cls, runner, db_file, monkeypatch):
        """--dry-run shows what would be fetched without actually fetching."""
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config=None)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-1", "title": "Task 1", "project": "proj", "slot": 1},
            {"ticket_id": "SMA-2", "title": "Task 2", "project": "proj", "slot": 2},
        ])

        result = runner.invoke(main, ["backfill-history", "--dry-run"])
        assert result.exit_code == 0
        assert "Dry run" in result.output
        assert "SMA-1" in result.output
        assert "SMA-2" in result.output
        # LinearClient should not have been instantiated
        mock_client_cls.assert_not_called()

        # Verify nothing was written to ticket_history
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        assert get_ticket_history_entry(conn, "SMA-1") is None
        conn.close()

    def test_all_already_backfilled(self, runner, db_file, monkeypatch):
        """When all tickets are already in ticket_history, shows 'nothing to do'."""
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", _mock_resolve(db_file, config=None)
        )
        _seed_tasks(db_file, [
            {"ticket_id": "SMA-1", "title": "Task 1", "project": "proj", "slot": 1},
        ])

        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        upsert_ticket_history(
            conn, ticket_id="SMA-1", title="Already here", capture_source="dispatch"
        )
        conn.commit()
        conn.close()

        result = runner.invoke(main, ["backfill-history"])
        assert result.exit_code == 0
        assert "Nothing to backfill" in result.output
