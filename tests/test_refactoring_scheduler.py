"""Tests for the periodic refactoring analysis ticket scheduler."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import (
    BotfarmConfig,
    DatabaseConfig,
    LinearConfig,
    ProjectConfig,
    RefactoringAnalysisConfig,
)
from botfarm.db import (
    count_completed_tasks_since,
    get_events,
    get_last_refactoring_analysis_date,
    get_last_scheduled_refactoring_ticket,
    init_db,
    insert_event,
    insert_task,
    record_refactoring_analysis_created,
    update_task,
)
from botfarm.bugtracker.types import CreatedIssue
from botfarm.bugtracker import BugtrackerError as LinearAPIError, PollResult
from botfarm.supervisor import Supervisor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    tmp_path,
    *,
    ra_enabled: bool = True,
    cadence_days: int = 14,
    cadence_tickets: int = 20,
    priority: int = 4,
    tracker_label: str = "Refactoring Analysis",
) -> BotfarmConfig:
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project",
                team="TST",
                base_dir=str(tmp_path / "repo"),
                worktree_prefix="test-project-slot-",
                slots=[1],
            ),
        ],
        bugtracker=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=10,
        ),
        database=DatabaseConfig(),
        refactoring_analysis=RefactoringAnalysisConfig(
            enabled=ra_enabled,
            cadence_days=cadence_days,
            cadence_tickets=cadence_tickets,
            priority=priority,
            tracker_label=tracker_label,
        ),
    )


def _make_supervisor(tmp_path, monkeypatch, config=None):
    """Create a Supervisor with mocked pollers."""
    monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
    config = config or _make_config(tmp_path)
    (tmp_path / "repo").mkdir(exist_ok=True)

    mock_poller = MagicMock()
    mock_poller.project_name = "test-project"
    mock_poller.team_key = "TST"
    mock_poller.poll.return_value = PollResult(
        candidates=[], blocked=[], auto_close_parents=[]
    )
    mock_poller.is_issue_terminal.return_value = False

    with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
        sup = Supervisor(config, log_dir=tmp_path / "logs")

    return sup, mock_poller


def _setup_linear_mocks(sup, *, states=None):
    """Set up standard Linear client mocks for ticket creation."""
    if states is None:
        states = {"Todo": "todo-state-id", "Backlog": "backlog-state-id"}
    sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
    sup._bugtracker_client.get_or_create_label = MagicMock(
        return_value="label-uuid"
    )
    sup._bugtracker_client.get_team_states = MagicMock(return_value=states)
    sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
    sup._bugtracker_client.create_issue = MagicMock(
        return_value=CreatedIssue(
            id="issue-uuid",
            identifier="TST-100",
            url="https://linear.app/test/TST-100",
        )
    )
    sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
        return_value=[]
    )


def _insert_completed_tasks(conn, count, *, completed_at=None):
    """Insert N completed tasks for testing ticket-count trigger."""
    if completed_at is None:
        completed_at = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )
    for i in range(count):
        task_id = insert_task(
            conn,
            ticket_id=f"TST-C{i}",
            title=f"Completed task {i}",
            project="test-project",
            slot=1,
            status="completed",
        )
        update_task(conn, task_id, status="completed", completed_at=completed_at)
    conn.commit()


# ---------------------------------------------------------------------------
# DB helper tests
# ---------------------------------------------------------------------------


class TestDBHelpers:
    def test_get_last_refactoring_analysis_date_empty(self, conn):
        assert get_last_refactoring_analysis_date(conn) is None

    def test_record_and_get_last_date(self, conn):
        record_refactoring_analysis_created(conn, "TST-42")
        conn.commit()
        dt = get_last_refactoring_analysis_date(conn)
        assert dt is not None
        assert (datetime.now(timezone.utc) - dt).total_seconds() < 10

    def test_get_last_scheduled_ticket_empty(self, conn):
        result = get_last_scheduled_refactoring_ticket(conn)
        assert result is None

    def test_get_last_scheduled_ticket_after_record(self, conn):
        record_refactoring_analysis_created(conn, "TST-99")
        conn.commit()
        result = get_last_scheduled_refactoring_ticket(conn)
        assert result == "TST-99"

    def test_count_completed_tasks_since_empty(self, conn):
        assert count_completed_tasks_since(conn, "1970-01-01T00:00:00Z") == 0

    def test_count_completed_tasks_since_with_tasks(self, conn):
        _insert_completed_tasks(conn, 5)
        assert count_completed_tasks_since(conn, "1970-01-01T00:00:00Z") == 5

    def test_count_completed_tasks_since_respects_timestamp(self, conn):
        old_time = (
            datetime.now(timezone.utc) - timedelta(days=10)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        _insert_completed_tasks(conn, 3, completed_at=old_time)

        recent_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=5)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        assert count_completed_tasks_since(conn, recent_cutoff) == 0

        old_cutoff = (
            datetime.now(timezone.utc) - timedelta(days=15)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        assert count_completed_tasks_since(conn, old_cutoff) == 3

    def test_count_completed_tasks_since_ignores_non_completed(self, conn):
        task_id = insert_task(
            conn,
            ticket_id="TST-PEND",
            title="Pending",
            project="test-project",
            slot=1,
            status="pending",
        )
        conn.commit()
        assert count_completed_tasks_since(conn, "1970-01-01T00:00:00Z") == 0

    def test_count_completed_tasks_since_null_completed_at(self, conn):
        """Tasks completed via recovery paths (NULL completed_at) are counted
        using COALESCE fallback to started_at."""
        started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        task_id = insert_task(
            conn,
            ticket_id="TST-RECOV",
            title="Recovered task",
            project="test-project",
            slot=1,
            status="completed",
        )
        # Simulate recovery path: status=completed, started_at set, but no completed_at
        update_task(conn, task_id, status="completed", started_at=started)
        conn.commit()
        assert count_completed_tasks_since(conn, "1970-01-01T00:00:00Z") == 1


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------


class TestRefactoringAnalysisConfig:
    def test_defaults(self):
        cfg = RefactoringAnalysisConfig()
        assert cfg.enabled is False
        assert cfg.cadence_days == 14
        assert cfg.cadence_tickets == 20
        assert cfg.tracker_label == "Refactoring Analysis"
        assert cfg.priority == 4

    def test_config_loads_refactoring_analysis(self, tmp_path):
        import yaml

        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "bugtracker": {"api_key": "key"},
            "refactoring_analysis": {
                "enabled": True,
                "cadence_days": 7,
                "cadence_tickets": 10,
                "linear_label": "Custom Label",
                "priority": 2,
            },
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        from botfarm.config import load_config

        cfg = load_config(path)
        assert cfg.refactoring_analysis.enabled is True
        assert cfg.refactoring_analysis.cadence_days == 7
        assert cfg.refactoring_analysis.cadence_tickets == 10
        assert cfg.refactoring_analysis.tracker_label == "Custom Label"
        assert cfg.refactoring_analysis.priority == 2

    def test_config_defaults_when_missing(self, tmp_path):
        import yaml

        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "bugtracker": {"api_key": "key"},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        from botfarm.config import load_config

        cfg = load_config(path)
        assert cfg.refactoring_analysis.enabled is False
        assert cfg.refactoring_analysis.cadence_days == 14
        assert cfg.refactoring_analysis.cadence_tickets == 20

    def test_validation_rejects_negative_cadence_tickets(self, tmp_path):
        import yaml
        from botfarm.config import ConfigError, load_config

        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "bugtracker": {"api_key": "key"},
            "refactoring_analysis": {"cadence_tickets": -1},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        with pytest.raises(ConfigError, match="cadence_tickets"):
            load_config(path)

    def test_validation_allows_zero_cadence_tickets(self, tmp_path):
        import yaml
        from botfarm.config import load_config

        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "bugtracker": {"api_key": "key"},
            "refactoring_analysis": {"cadence_tickets": 0},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        cfg = load_config(path)
        assert cfg.refactoring_analysis.cadence_tickets == 0

    def test_validation_rejects_bad_cadence(self, tmp_path):
        import yaml
        from botfarm.config import ConfigError, load_config

        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "bugtracker": {"api_key": "key"},
            "refactoring_analysis": {"cadence_days": 0},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        with pytest.raises(ConfigError, match="cadence_days"):
            load_config(path)

    def test_validation_rejects_empty_label(self, tmp_path):
        import yaml
        from botfarm.config import ConfigError, load_config

        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "bugtracker": {"api_key": "key"},
            "refactoring_analysis": {"linear_label": "  "},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        with pytest.raises(ConfigError, match="tracker_label"):
            load_config(path)

    def test_validation_rejects_bad_priority(self, tmp_path):
        import yaml
        from botfarm.config import ConfigError, load_config

        data = {
            "projects": [
                {
                    "name": "p",
                    "team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "bugtracker": {"api_key": "key"},
            "refactoring_analysis": {"priority": 5},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        with pytest.raises(ConfigError, match="priority"):
            load_config(path)


# ---------------------------------------------------------------------------
# Supervisor scheduler tests
# ---------------------------------------------------------------------------


class TestRefactoringScheduler:
    def test_disabled_does_nothing(self, tmp_path, monkeypatch):
        config = _make_config(tmp_path, ra_enabled=False)
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_creates_ticket_when_no_previous(self, tmp_path, monkeypatch):
        sup, mock_poller = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
        sup._bugtracker_client.create_issue = MagicMock(
            return_value=CreatedIssue(
                id="issue-uuid",
                identifier="TST-100",
                url="https://linear.app/test/TST-100",
            )
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        sup._bugtracker_client.create_issue.assert_called_once()
        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        assert call_kwargs["team_id"] == "team-uuid"
        assert "Refactoring Analysis" in call_kwargs["title"]
        assert call_kwargs["priority"] == 4
        assert call_kwargs["state_id"] == "todo-state-id"

        # Verify DB tracking
        events = get_events(
            sup._conn, event_type="refactoring_analysis_scheduled"
        )
        assert len(events) == 1
        assert events[0]["detail"] == "TST-100"

    def test_respects_cadence(self, tmp_path, monkeypatch):
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        # Record a recent creation
        record_refactoring_analysis_created(sup._conn, "TST-50")
        sup._conn.commit()

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_creates_after_cadence_expires(self, tmp_path, monkeypatch):
        config = _make_config(tmp_path, cadence_days=1)
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        # Insert an old event (2 days ago)
        old_time = (
            datetime.now(timezone.utc) - timedelta(days=2)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        sup._conn.execute(
            "INSERT INTO task_events (event_type, detail, created_at) "
            "VALUES ('refactoring_analysis_scheduled', 'TST-OLD', ?)",
            (old_time,),
        )
        sup._conn.commit()

        # The old ticket is terminal
        for poller in sup._pollers.values():
            poller.is_issue_terminal.return_value = True

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
        sup._bugtracker_client.create_issue = MagicMock(
            return_value=CreatedIssue(
                id="new-uuid",
                identifier="TST-101",
                url="https://linear.app/test/TST-101",
            )
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        sup._bugtracker_client.create_issue.assert_called_once()

    def test_skips_when_open_ticket_in_linear(self, tmp_path, monkeypatch):
        config = _make_config(tmp_path, cadence_days=1)
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        # Insert an old event
        old_time = (
            datetime.now(timezone.utc) - timedelta(days=2)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        sup._conn.execute(
            "INSERT INTO task_events (event_type, detail, created_at) "
            "VALUES ('refactoring_analysis_scheduled', 'TST-OLD', ?)",
            (old_time,),
        )
        sup._conn.commit()

        # Old ticket is terminal in DB check
        for poller in sup._pollers.values():
            poller.is_issue_terminal.return_value = True

        # But Linear shows open tickets with the label
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[{"id": "x", "identifier": "TST-55"}]
        )

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_skips_when_previous_ticket_still_open(self, tmp_path, monkeypatch):
        sup, mock_poller = _make_supervisor(tmp_path, monkeypatch)

        # Insert an old event (cadence expired)
        old_time = (
            datetime.now(timezone.utc) - timedelta(days=30)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        sup._conn.execute(
            "INSERT INTO task_events (event_type, detail, created_at) "
            "VALUES ('refactoring_analysis_scheduled', 'TST-OPEN', ?)",
            (old_time,),
        )
        sup._conn.commit()

        # Poller says ticket is NOT terminal
        mock_poller.is_issue_terminal.return_value = False

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_ticket_content_matches_template(self, tmp_path, monkeypatch):
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
        sup._bugtracker_client.create_issue = MagicMock(
            return_value=CreatedIssue(
                id="issue-uuid",
                identifier="TST-200",
                url="https://linear.app/test/TST-200",
            )
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        desc = call_kwargs["description"]
        assert "Periodic Codebase Refactoring Analysis" in desc
        assert "automatically scheduled investigation" in desc
        assert "Expected Outcomes" in desc
        assert "good enough" in desc

    def test_creates_both_labels(self, tmp_path, monkeypatch):
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        label_calls = []
        sup._bugtracker_client.get_or_create_label = MagicMock(
            side_effect=lambda tk, name: (
                label_calls.append(name) or f"label-{name}"
            )
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
        sup._bugtracker_client.create_issue = MagicMock(
            return_value=CreatedIssue(
                id="issue-uuid",
                identifier="TST-300",
                url="https://linear.app/test/TST-300",
            )
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        assert "Investigation" in label_calls
        assert "Refactoring Analysis" in label_calls

    def test_handles_linear_api_failure_gracefully(
        self, tmp_path, monkeypatch
    ):
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )
        sup._bugtracker_client.get_team_id = MagicMock(
            side_effect=LinearAPIError("API down")
        )

        # Should not raise
        sup._maybe_create_refactoring_analysis_ticket()

        # No event recorded since creation failed
        events = get_events(
            sup._conn, event_type="refactoring_analysis_scheduled"
        )
        assert len(events) == 0

    def test_skips_when_all_linear_checks_fail(self, tmp_path, monkeypatch):
        """Does not create a ticket if all Linear API checks fail."""
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            side_effect=Exception("API unreachable")
        )

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_passes_project_id_when_configured(self, tmp_path, monkeypatch):
        """Passes projectId to create_issue when linear_project is set."""
        config = _make_config(tmp_path)
        config.projects[0].tracker_project = "My Project"
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(
            return_value="project-uuid"
        )
        sup._bugtracker_client.create_issue = MagicMock(
            return_value=CreatedIssue(
                id="issue-uuid",
                identifier="TST-400",
                url="https://linear.app/test/TST-400",
            )
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        assert call_kwargs["project_id"] == "project-uuid"
        assert call_kwargs["state_id"] == "todo-state-id"

    def test_skips_when_project_not_found(self, tmp_path, monkeypatch):
        """Skips ticket creation when configured project is not found in Linear."""
        config = _make_config(tmp_path)
        config.projects[0].tracker_project = "Nonexistent Project"
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_skips_when_todo_state_not_found(self, tmp_path, monkeypatch):
        """Skips ticket creation when configured todo state doesn't exist."""
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Backlog": "backlog-state-id"}  # No "Todo" state
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_aborts_when_ra_label_fails(self, tmp_path, monkeypatch):
        """Aborts ticket creation when the refactoring analysis label fails."""
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            side_effect=Exception("label API down")
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_continues_when_investigation_label_fails(
        self, tmp_path, monkeypatch
    ):
        """Creates ticket even if Investigation label fails, as long as RA label succeeds."""
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")

        def label_side_effect(team_key, name):
            if name == "Investigation":
                raise Exception("label API error")
            return "ra-label-id"

        sup._bugtracker_client.get_or_create_label = MagicMock(
            side_effect=label_side_effect
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
        sup._bugtracker_client.create_issue = MagicMock(
            return_value=CreatedIssue(
                id="issue-uuid",
                identifier="TST-500",
                url="https://linear.app/test/TST-500",
            )
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        sup._bugtracker_client.create_issue.assert_called_once()
        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        assert call_kwargs["label_ids"] == ["ra-label-id"]

    def test_skips_when_identifier_missing(self, tmp_path, monkeypatch):
        """Does not record event when Linear returns no identifier."""
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._bugtracker_client.get_project_id = MagicMock(return_value=None)
        sup._bugtracker_client.create_issue = MagicMock(
            return_value=CreatedIssue(id="issue-uuid", identifier="", url="")
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        events = get_events(
            sup._conn, event_type="refactoring_analysis_scheduled"
        )
        assert len(events) == 0

    def test_skips_when_project_api_fails(self, tmp_path, monkeypatch):
        """Skips ticket creation when project lookup raises an exception."""
        config = _make_config(tmp_path)
        config.projects[0].tracker_project = "My Project"
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        sup._bugtracker_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._bugtracker_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._bugtracker_client.get_project_id = MagicMock(
            side_effect=Exception("API error")
        )
        sup._bugtracker_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()


# ---------------------------------------------------------------------------
# Linear client method tests
# ---------------------------------------------------------------------------


class TestLinearClientMethods:
    def test_create_issue_builds_correct_input(self):
        from botfarm.bugtracker.linear.client import LinearClient

        client = LinearClient(api_key="test")
        mock_response = {
            "issueCreate": {
                "success": True,
                "issue": {
                    "id": "uuid",
                    "identifier": "TST-1",
                    "url": "https://linear.app/test/TST-1",
                },
            }
        }
        with patch.object(client, "_execute", return_value=mock_response):
            result = client.create_issue(
                team_id="team-1",
                title="Test Title",
                description="Test body",
                priority=3,
                label_ids=["lbl-1", "lbl-2"],
            )

        assert result.identifier == "TST-1"

    def test_create_issue_passes_project_and_state(self):
        from botfarm.bugtracker.linear.client import LinearClient

        client = LinearClient(api_key="test")
        mock_response = {
            "issueCreate": {
                "success": True,
                "issue": {
                    "id": "uuid",
                    "identifier": "TST-2",
                    "url": "https://linear.app/test/TST-2",
                },
            }
        }
        with patch.object(client, "_execute", return_value=mock_response) as mock_exec:
            client.create_issue(
                team_id="team-1",
                title="Test",
                project_id="proj-uuid",
                state_id="state-uuid",
            )

        input_data = mock_exec.call_args[0][1]["input"]
        assert input_data["projectId"] == "proj-uuid"
        assert input_data["stateId"] == "state-uuid"

    def test_create_issue_includes_priority_zero(self):
        from botfarm.bugtracker.linear.client import LinearClient

        client = LinearClient(api_key="test")
        mock_response = {
            "issueCreate": {
                "success": True,
                "issue": {
                    "id": "uuid",
                    "identifier": "TST-PRI0",
                    "url": "https://linear.app/test/TST-PRI0",
                },
            }
        }
        with patch.object(client, "_execute", return_value=mock_response) as mock_exec:
            client.create_issue(
                team_id="team-1",
                title="Test",
                priority=0,
            )

        input_data = mock_exec.call_args[0][1]["input"]
        assert "priority" in input_data
        assert input_data["priority"] == 0

    def test_create_issue_raises_on_failure(self):
        from botfarm.bugtracker.linear.client import LinearClient

        client = LinearClient(api_key="test")
        mock_response = {"issueCreate": {"success": False}}
        with patch.object(client, "_execute", return_value=mock_response):
            with pytest.raises(LinearAPIError, match="success=false"):
                client.create_issue(
                    team_id="team-1",
                    title="Test",
                )

    def test_fetch_open_issues_with_label(self):
        from botfarm.bugtracker.linear.client import LinearClient

        client = LinearClient(api_key="test")
        mock_response = {
            "issues": {
                "nodes": [
                    {
                        "id": "i1",
                        "identifier": "TST-5",
                        "title": "Refactoring",
                        "state": {"type": "started", "name": "In Progress"},
                    }
                ]
            }
        }
        with patch.object(client, "_execute", return_value=mock_response):
            result = client.fetch_open_issues_with_label("TST", "Refactoring Analysis")

        assert len(result) == 1
        assert result[0].identifier == "TST-5"

    def test_get_or_create_label_returns_existing(self):
        from botfarm.bugtracker.linear.client import LinearClient

        client = LinearClient(api_key="test")
        mock_labels_response = {
            "issueLabels": {
                "nodes": [
                    {"id": "existing-id", "name": "Refactoring Analysis"}
                ]
            }
        }
        with patch.object(
            client, "_execute", return_value=mock_labels_response
        ):
            result = client.get_or_create_label("TST", "Refactoring Analysis")

        assert result == "existing-id"

    def test_get_or_create_label_creates_new(self):
        from botfarm.bugtracker.linear.client import LinearClient

        client = LinearClient(api_key="test")

        # First call: no existing labels; second: team lookup; third: create
        responses = [
            {"issueLabels": {"nodes": []}},  # get_label_id
            {
                "teams": {
                    "nodes": [{"id": "team-uuid", "key": "TST", "states": {"nodes": []}}]
                }
            },  # get_team_id
            {
                "issueLabelCreate": {
                    "success": True,
                    "issueLabel": {"id": "new-id", "name": "Custom"},
                }
            },  # create label
        ]
        with patch.object(
            client, "_execute", side_effect=responses
        ):
            result = client.get_or_create_label("TST", "Custom")

        assert result == "new-id"


# ---------------------------------------------------------------------------
# Ticket-count trigger tests
# ---------------------------------------------------------------------------


class TestTicketCountTrigger:
    def _insert_recent_analysis(self, sup):
        """Insert a recent analysis record and mark previous ticket terminal."""
        record_refactoring_analysis_created(sup._conn, "TST-PREV")
        sup._conn.commit()
        for poller in sup._pollers.values():
            poller.is_issue_terminal.return_value = True

    def test_ticket_count_trigger_fires(self, tmp_path, monkeypatch):
        """Creates ticket when cadence_tickets threshold is reached."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=5,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        self._insert_recent_analysis(sup)
        _insert_completed_tasks(sup._conn, 5)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        sup._bugtracker_client.create_issue.assert_called_once()

    def test_ticket_count_trigger_does_not_fire_below_threshold(
        self, tmp_path, monkeypatch,
    ):
        """Does not create ticket when below cadence_tickets threshold."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=10,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        self._insert_recent_analysis(sup)
        _insert_completed_tasks(sup._conn, 9)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        sup._bugtracker_client.create_issue.assert_not_called()

    def test_ticket_count_trigger_uses_backlog_state(
        self, tmp_path, monkeypatch,
    ):
        """Ticket-count trigger creates ticket in Backlog state, not Todo."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=5,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        self._insert_recent_analysis(sup)
        _insert_completed_tasks(sup._conn, 5)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        assert call_kwargs["state_id"] == "backlog-state-id"

    def test_ticket_count_trigger_sends_notification(
        self, tmp_path, monkeypatch,
    ):
        """Ticket-count trigger sends notify_refactoring_due notification."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=5,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        self._insert_recent_analysis(sup)
        _insert_completed_tasks(sup._conn, 5)
        _setup_linear_mocks(sup)

        with patch.object(sup._notifier, "notify_refactoring_due") as mock_notify:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_notify.assert_called_once_with(
                ticket_id="TST-100",
                ticket_url="https://linear.app/test/TST-100",
            )

    def test_time_trigger_does_not_send_refactoring_due(
        self, tmp_path, monkeypatch,
    ):
        """Time-based trigger does NOT send notify_refactoring_due."""
        config = _make_config(
            tmp_path, cadence_days=14, cadence_tickets=0,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        _setup_linear_mocks(sup)

        with patch.object(sup._notifier, "notify_refactoring_due") as mock_notify:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_notify.assert_not_called()

    def test_time_trigger_uses_todo_state(
        self, tmp_path, monkeypatch,
    ):
        """Time-based trigger creates ticket in Todo state."""
        config = _make_config(
            tmp_path, cadence_days=14, cadence_tickets=0,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        assert call_kwargs["state_id"] == "todo-state-id"

    def test_cadence_tickets_zero_disables_count_trigger(
        self, tmp_path, monkeypatch,
    ):
        """cadence_tickets=0 disables the ticket-count trigger."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=0,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        self._insert_recent_analysis(sup)
        _insert_completed_tasks(sup._conn, 100)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        # No trigger fires: time hasn't elapsed and count trigger disabled
        sup._bugtracker_client.create_issue.assert_not_called()

    def test_both_triggers_fire_time_takes_priority(
        self, tmp_path, monkeypatch,
    ):
        """When both triggers fire, time trigger wins (ticket goes to Todo)."""
        config = _make_config(
            tmp_path, cadence_days=14, cadence_tickets=5,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        _insert_completed_tasks(sup._conn, 10)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        assert call_kwargs["state_id"] == "todo-state-id"

    def test_triggers_fire_independently(self, tmp_path, monkeypatch):
        """Ticket-count trigger fires even when time trigger hasn't elapsed."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=3,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        # Record a recent creation so time trigger won't fire
        record_refactoring_analysis_created(sup._conn, "TST-PREV")
        sup._conn.commit()
        # But mark previous ticket as terminal
        for poller in sup._pollers.values():
            poller.is_issue_terminal.return_value = True

        _insert_completed_tasks(sup._conn, 3)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        sup._bugtracker_client.create_issue.assert_called_once()
        call_kwargs = sup._bugtracker_client.create_issue.call_args[1]
        assert call_kwargs["state_id"] == "backlog-state-id"

    def test_dedup_prevents_duplicate_creation(self, tmp_path, monkeypatch):
        """Dedup check prevents ticket creation even when count trigger fires."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=5,
        )
        sup, mock_poller = _make_supervisor(tmp_path, monkeypatch, config)
        _insert_completed_tasks(sup._conn, 10)

        # Previous ticket still open
        record_refactoring_analysis_created(sup._conn, "TST-OPEN")
        sup._conn.commit()
        mock_poller.is_issue_terminal.return_value = False

        with patch.object(sup._bugtracker_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_count_resets_after_analysis_created(self, tmp_path, monkeypatch):
        """Tasks before last analysis don't count toward next trigger."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=5,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        # Insert old tasks, then record analysis
        old_time = (
            datetime.now(timezone.utc) - timedelta(days=2)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        _insert_completed_tasks(sup._conn, 10, completed_at=old_time)

        record_refactoring_analysis_created(sup._conn, "TST-PREV")
        sup._conn.commit()
        # Previous ticket is terminal
        for poller in sup._pollers.values():
            poller.is_issue_terminal.return_value = True

        # Only 2 new tasks since the analysis
        _insert_completed_tasks(sup._conn, 2)
        _setup_linear_mocks(sup)

        sup._maybe_create_refactoring_analysis_ticket()

        # 2 < 5 threshold, so no creation
        sup._bugtracker_client.create_issue.assert_not_called()

    def test_backlog_state_not_found_skips_creation(
        self, tmp_path, monkeypatch,
    ):
        """Skips creation when Backlog state doesn't exist in team."""
        config = _make_config(
            tmp_path, cadence_days=999, cadence_tickets=5,
        )
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)
        self._insert_recent_analysis(sup)
        _insert_completed_tasks(sup._conn, 5)
        _setup_linear_mocks(sup, states={"Todo": "todo-state-id"})  # No Backlog

        sup._maybe_create_refactoring_analysis_ticket()

        sup._bugtracker_client.create_issue.assert_not_called()


# ---------------------------------------------------------------------------
# Notification tests for refactoring_due
# ---------------------------------------------------------------------------


class TestRefactoringDueNotification:
    @pytest.fixture()
    def notifier(self):
        from botfarm.config import NotificationsConfig
        from botfarm.notifications import Notifier

        n = Notifier(NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/xxx",
            webhook_format="slack",
        ))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        yield n
        n.close()

    def test_sends_message_with_ticket_info(self, notifier):
        notifier.notify_refactoring_due(
            ticket_id="TST-100",
            ticket_url="https://linear.app/test/TST-100",
        )
        notifier._client.post.assert_called_once()
        payload = notifier._client.post.call_args[1]["json"]
        assert "TST-100" in payload["text"]
        assert "Backlog" in payload["text"]
        assert "https://linear.app/test/TST-100" in payload["text"]

    def test_not_rate_limited(self, notifier):
        notifier.notify_refactoring_due(
            ticket_id="TST-1",
            ticket_url="https://linear.app/test/TST-1",
        )
        notifier.notify_refactoring_due(
            ticket_id="TST-2",
            ticket_url="https://linear.app/test/TST-2",
        )
        assert notifier._client.post.call_count == 2

    def test_noop_when_disabled(self):
        from botfarm.config import NotificationsConfig
        from botfarm.notifications import Notifier

        n = Notifier(NotificationsConfig(webhook_url=""))
        n.notify_refactoring_due(
            ticket_id="TST-1",
            ticket_url="https://linear.app/test/TST-1",
        )
        n.close()
