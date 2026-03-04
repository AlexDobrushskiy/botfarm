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
    get_events,
    get_last_refactoring_analysis_date,
    get_last_scheduled_refactoring_ticket,
    init_db,
    insert_event,
    record_refactoring_analysis_created,
)
from botfarm.linear import LinearAPIError, PollResult
from botfarm.supervisor import Supervisor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(
    tmp_path,
    *,
    ra_enabled: bool = True,
    cadence_days: int = 14,
    priority: int = 4,
    linear_label: str = "Refactoring Analysis",
) -> BotfarmConfig:
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project",
                linear_team="TST",
                base_dir=str(tmp_path / "repo"),
                worktree_prefix="test-project-slot-",
                slots=[1],
            ),
        ],
        linear=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=10,
        ),
        database=DatabaseConfig(),
        refactoring_analysis=RefactoringAnalysisConfig(
            enabled=ra_enabled,
            cadence_days=cadence_days,
            priority=priority,
            linear_label=linear_label,
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


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------


class TestRefactoringAnalysisConfig:
    def test_defaults(self):
        cfg = RefactoringAnalysisConfig()
        assert cfg.enabled is False
        assert cfg.cadence_days == 14
        assert cfg.linear_label == "Refactoring Analysis"
        assert cfg.priority == 4

    def test_config_loads_refactoring_analysis(self, tmp_path):
        import yaml

        data = {
            "projects": [
                {
                    "name": "p",
                    "linear_team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "linear": {"api_key": "key"},
            "refactoring_analysis": {
                "enabled": True,
                "cadence_days": 7,
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
        assert cfg.refactoring_analysis.linear_label == "Custom Label"
        assert cfg.refactoring_analysis.priority == 2

    def test_config_defaults_when_missing(self, tmp_path):
        import yaml

        data = {
            "projects": [
                {
                    "name": "p",
                    "linear_team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "linear": {"api_key": "key"},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        from botfarm.config import load_config

        cfg = load_config(path)
        assert cfg.refactoring_analysis.enabled is False
        assert cfg.refactoring_analysis.cadence_days == 14

    def test_validation_rejects_bad_cadence(self, tmp_path):
        import yaml
        from botfarm.config import ConfigError, load_config

        data = {
            "projects": [
                {
                    "name": "p",
                    "linear_team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "linear": {"api_key": "key"},
            "refactoring_analysis": {"cadence_days": 0},
        }
        path = tmp_path / "config.yaml"
        path.write_text(yaml.dump(data))

        with pytest.raises(ConfigError, match="cadence_days"):
            load_config(path)

    def test_validation_rejects_bad_priority(self, tmp_path):
        import yaml
        from botfarm.config import ConfigError, load_config

        data = {
            "projects": [
                {
                    "name": "p",
                    "linear_team": "T",
                    "base_dir": "~/d",
                    "worktree_prefix": "p-",
                    "slots": [1],
                }
            ],
            "linear": {"api_key": "key"},
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

        with patch.object(sup._linear_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_creates_ticket_when_no_previous(self, tmp_path, monkeypatch):
        sup, mock_poller = _make_supervisor(tmp_path, monkeypatch)

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._linear_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._linear_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._linear_client.get_project_id = MagicMock(return_value=None)
        sup._linear_client.create_issue = MagicMock(
            return_value={
                "id": "issue-uuid",
                "identifier": "TST-100",
                "url": "https://linear.app/test/TST-100",
            }
        )
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        sup._linear_client.create_issue.assert_called_once()
        call_kwargs = sup._linear_client.create_issue.call_args[1]
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

        with patch.object(sup._linear_client, "create_issue") as mock_create:
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

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._linear_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._linear_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._linear_client.get_project_id = MagicMock(return_value=None)
        sup._linear_client.create_issue = MagicMock(
            return_value={
                "id": "new-uuid",
                "identifier": "TST-101",
                "url": "https://linear.app/test/TST-101",
            }
        )
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        sup._linear_client.create_issue.assert_called_once()

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
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[{"id": "x", "identifier": "TST-55"}]
        )

        with patch.object(sup._linear_client, "create_issue") as mock_create:
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

        with patch.object(sup._linear_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_ticket_content_matches_template(self, tmp_path, monkeypatch):
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._linear_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._linear_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._linear_client.get_project_id = MagicMock(return_value=None)
        sup._linear_client.create_issue = MagicMock(
            return_value={
                "id": "issue-uuid",
                "identifier": "TST-200",
                "url": "https://linear.app/test/TST-200",
            }
        )
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        call_kwargs = sup._linear_client.create_issue.call_args[1]
        desc = call_kwargs["description"]
        assert "Periodic Codebase Refactoring Analysis" in desc
        assert "automatically scheduled investigation" in desc
        assert "Expected Outcomes" in desc
        assert "good enough" in desc

    def test_creates_both_labels(self, tmp_path, monkeypatch):
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        label_calls = []
        sup._linear_client.get_or_create_label = MagicMock(
            side_effect=lambda tk, name: (
                label_calls.append(name) or f"label-{name}"
            )
        )
        sup._linear_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._linear_client.get_project_id = MagicMock(return_value=None)
        sup._linear_client.create_issue = MagicMock(
            return_value={
                "id": "issue-uuid",
                "identifier": "TST-300",
                "url": "https://linear.app/test/TST-300",
            }
        )
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        assert "Investigation" in label_calls
        assert "Refactoring Analysis" in label_calls

    def test_handles_linear_api_failure_gracefully(
        self, tmp_path, monkeypatch
    ):
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )
        sup._linear_client.get_team_id = MagicMock(
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

        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            side_effect=Exception("API unreachable")
        )

        with patch.object(sup._linear_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_passes_project_id_when_configured(self, tmp_path, monkeypatch):
        """Passes projectId to create_issue when linear_project is set."""
        config = _make_config(tmp_path)
        config.projects[0].linear_project = "My Project"
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._linear_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._linear_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._linear_client.get_project_id = MagicMock(
            return_value="project-uuid"
        )
        sup._linear_client.create_issue = MagicMock(
            return_value={
                "id": "issue-uuid",
                "identifier": "TST-400",
                "url": "https://linear.app/test/TST-400",
            }
        )
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        sup._maybe_create_refactoring_analysis_ticket()

        call_kwargs = sup._linear_client.create_issue.call_args[1]
        assert call_kwargs["project_id"] == "project-uuid"
        assert call_kwargs["state_id"] == "todo-state-id"

    def test_skips_when_project_not_found(self, tmp_path, monkeypatch):
        """Skips ticket creation when configured project is not found in Linear."""
        config = _make_config(tmp_path)
        config.projects[0].linear_project = "Nonexistent Project"
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._linear_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._linear_client.get_team_states = MagicMock(
            return_value={"Todo": "todo-state-id"}
        )
        sup._linear_client.get_project_id = MagicMock(return_value=None)
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        with patch.object(sup._linear_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_skips_when_todo_state_not_found(self, tmp_path, monkeypatch):
        """Skips ticket creation when configured todo state doesn't exist."""
        sup, _ = _make_supervisor(tmp_path, monkeypatch)

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._linear_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._linear_client.get_team_states = MagicMock(
            return_value={"Backlog": "backlog-state-id"}  # No "Todo" state
        )
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        with patch.object(sup._linear_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()

    def test_skips_when_project_api_fails(self, tmp_path, monkeypatch):
        """Skips ticket creation when project lookup raises an exception."""
        config = _make_config(tmp_path)
        config.projects[0].linear_project = "My Project"
        sup, _ = _make_supervisor(tmp_path, monkeypatch, config)

        sup._linear_client.get_team_id = MagicMock(return_value="team-uuid")
        sup._linear_client.get_or_create_label = MagicMock(
            return_value="label-uuid"
        )
        sup._linear_client.get_project_id = MagicMock(
            side_effect=Exception("API error")
        )
        sup._linear_client.fetch_open_issues_with_label = MagicMock(
            return_value=[]
        )

        with patch.object(sup._linear_client, "create_issue") as mock_create:
            sup._maybe_create_refactoring_analysis_ticket()
            mock_create.assert_not_called()


# ---------------------------------------------------------------------------
# Linear client method tests
# ---------------------------------------------------------------------------


class TestLinearClientMethods:
    def test_create_issue_builds_correct_input(self):
        from botfarm.linear import LinearClient

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

        assert result["identifier"] == "TST-1"

    def test_create_issue_passes_project_and_state(self):
        from botfarm.linear import LinearClient

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
        from botfarm.linear import LinearClient

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
        from botfarm.linear import LinearClient

        client = LinearClient(api_key="test")
        mock_response = {"issueCreate": {"success": False}}
        with patch.object(client, "_execute", return_value=mock_response):
            with pytest.raises(LinearAPIError, match="success=false"):
                client.create_issue(
                    team_id="team-1",
                    title="Test",
                )

    def test_fetch_open_issues_with_label(self):
        from botfarm.linear import LinearClient

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
        assert result[0]["identifier"] == "TST-5"

    def test_get_or_create_label_returns_existing(self):
        from botfarm.linear import LinearClient

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
        from botfarm.linear import LinearClient

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
