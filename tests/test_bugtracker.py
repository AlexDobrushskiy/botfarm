"""Tests for the abstract bugtracker interface layer."""

from __future__ import annotations

import pytest

from botfarm.bugtracker import (
    ActiveIssuesCount,
    BugtrackerClient,
    BugtrackerError,
    BugtrackerPoller,
    Comment,
    CreatedIssue,
    Issue,
    IssueDetails,
    PollResult,
)
from botfarm.bugtracker.base import BugtrackerClient as BaseClient
from botfarm.bugtracker.base import BugtrackerPoller as BasePoller


# ---------------------------------------------------------------------------
# ABC enforcement — can't instantiate without implementing required methods
# ---------------------------------------------------------------------------


class TestBugtrackerClientABC:
    """Verify BugtrackerClient cannot be instantiated without all required methods."""

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BugtrackerClient()  # type: ignore[abstract]

    def test_cannot_instantiate_with_partial_implementation(self):
        class PartialClient(BugtrackerClient):
            def fetch_team_issues(self, team_key, status_name="Todo", first=50, project_name=""):
                return []

        with pytest.raises(TypeError):
            PartialClient()

    def test_can_instantiate_full_implementation(self):
        class FullClient(BugtrackerClient):
            def fetch_team_issues(self, team_key, status_name="Todo", first=50, project_name=""):
                return []

            def get_team_states(self, team_key):
                return {}

            def update_issue_state(self, issue_id, state_id):
                pass

            def add_comment(self, issue_id, body):
                pass

            def get_viewer_id(self):
                return "viewer-1"

            def assign_issue(self, issue_id, assignee_id):
                pass

            def add_labels(self, issue_id, label_ids):
                pass

            def fetch_issue_labels(self, identifier):
                return ("Title", ["bug"])

            def fetch_issue_state_type(self, identifier):
                return None

            def fetch_issue_details(self, identifier):
                return IssueDetails(id="uuid-1", ticket_id="T-1", title="Test", url="https://example.com")

            def get_team_id(self, team_key):
                return "team-1"

            def get_label_id(self, team_key, label_name):
                return None

            def get_or_create_label(self, team_key, label_name):
                return "label-1"

            def create_issue(self, *, team_id, title, description="", priority=None, label_ids=None, project_id=None, state_id=None):
                return CreatedIssue(id="issue-1", identifier="T-1", url="https://example.com")

        client = FullClient()
        assert isinstance(client, BugtrackerClient)

    def test_optional_methods_raise_not_implemented(self):
        """Optional methods should raise NotImplementedError by default."""

        class MinimalClient(BugtrackerClient):
            def fetch_team_issues(self, team_key, status_name="Todo", first=50, project_name=""):
                return []

            def get_team_states(self, team_key):
                return {}

            def update_issue_state(self, issue_id, state_id):
                pass

            def add_comment(self, issue_id, body):
                pass

            def get_viewer_id(self):
                return "v"

            def assign_issue(self, issue_id, assignee_id):
                pass

            def add_labels(self, issue_id, label_ids):
                pass

            def fetch_issue_labels(self, identifier):
                return None

            def fetch_issue_state_type(self, identifier):
                return None

            def fetch_issue_details(self, identifier):
                return IssueDetails(id="uuid-1", ticket_id="T-1", title="Test", url="")

            def get_team_id(self, team_key):
                return "t"

            def get_label_id(self, team_key, label_name):
                return None

            def get_or_create_label(self, team_key, label_name):
                return "l"

            def create_issue(self, *, team_id, title, description="", priority=None, label_ids=None, project_id=None, state_id=None):
                return CreatedIssue(id="i", identifier="T-1", url="")

        client = MinimalClient()

        with pytest.raises(NotImplementedError):
            client.count_active_issues()
        with pytest.raises(NotImplementedError):
            client.count_active_issues_for_project("proj")
        with pytest.raises(NotImplementedError):
            client.archive_issue("id")
        with pytest.raises(NotImplementedError):
            client.delete_issue("id")
        with pytest.raises(NotImplementedError):
            client.unarchive_issue("id")
        with pytest.raises(NotImplementedError):
            client.fetch_completed_issues("team")
        with pytest.raises(NotImplementedError):
            client.fetch_open_issues_with_label("team", "label")
        with pytest.raises(NotImplementedError):
            client.list_teams()
        with pytest.raises(NotImplementedError):
            client.list_team_projects("team-id")
        with pytest.raises(NotImplementedError):
            client.get_organization()
        with pytest.raises(NotImplementedError):
            client.get_project_id("proj")


class TestBugtrackerPollerABC:
    """Verify BugtrackerPoller cannot be instantiated without all required methods."""

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BugtrackerPoller()  # type: ignore[abstract]

    def test_cannot_instantiate_with_partial_implementation(self):
        class PartialPoller(BugtrackerPoller):
            @property
            def project_name(self):
                return "proj"

        with pytest.raises(TypeError):
            PartialPoller()

    def test_can_instantiate_full_implementation(self):
        class FullPoller(BugtrackerPoller):
            @property
            def project_name(self):
                return "proj"

            @property
            def team_key(self):
                return "TEAM"

            def poll(self, active_ticket_ids=None):
                return PollResult()

            def get_state_id(self, state_name):
                return "state-1"

            def is_issue_terminal(self, identifier):
                return False

            def move_issue(self, issue_id, state_name):
                pass

            def assign_issue(self, issue_id, assignee_id):
                pass

            def add_comment(self, issue_id, body):
                pass

            def add_labels(self, issue_id, label_names):
                pass

        poller = FullPoller()
        assert isinstance(poller, BugtrackerPoller)

    def test_optional_add_comment_as_owner_raises(self):
        class MinPoller(BugtrackerPoller):
            @property
            def project_name(self):
                return "p"

            @property
            def team_key(self):
                return "T"

            def poll(self, active_ticket_ids=None):
                return PollResult()

            def get_state_id(self, state_name):
                return "s"

            def is_issue_terminal(self, identifier):
                return False

            def move_issue(self, issue_id, state_name):
                pass

            def assign_issue(self, issue_id, assignee_id):
                pass

            def add_comment(self, issue_id, body):
                pass

            def add_labels(self, issue_id, label_names):
                pass

        poller = MinPoller()
        with pytest.raises(NotImplementedError):
            poller.add_comment_as_owner("id", "body")


# ---------------------------------------------------------------------------
# Shared dataclass tests
# ---------------------------------------------------------------------------


class TestIssue:
    def test_minimal_construction(self):
        issue = Issue(id="1", identifier="TEST-1", title="Test", priority=1, url="https://example.com")
        assert issue.id == "1"
        assert issue.identifier == "TEST-1"
        assert issue.assignee_id is None
        assert issue.labels is None
        assert issue.sort_order == 0.0
        assert issue.blocked_by is None
        assert issue.children_states is None

    def test_full_construction(self):
        issue = Issue(
            id="2",
            identifier="TEST-2",
            title="Full",
            priority=2,
            url="https://example.com/2",
            assignee_id="user-1",
            assignee_email="user@example.com",
            labels=["bug", "urgent"],
            sort_order=1.5,
            blocked_by=["TEST-1"],
            children_states=[("TEST-3", "completed")],
        )
        assert issue.assignee_email == "user@example.com"
        assert issue.labels == ["bug", "urgent"]
        assert issue.blocked_by == ["TEST-1"]
        assert issue.children_states == [("TEST-3", "completed")]


class TestPollResult:
    def test_defaults(self):
        result = PollResult()
        assert result.candidates == []
        assert result.blocked == []
        assert result.auto_close_parents == []

    def test_with_issues(self):
        issue = Issue(id="1", identifier="T-1", title="T", priority=1, url="")
        result = PollResult(candidates=[issue], blocked=[], auto_close_parents=[])
        assert len(result.candidates) == 1
        assert result.candidates[0].identifier == "T-1"


class TestCreatedIssue:
    def test_construction(self):
        ci = CreatedIssue(id="id-1", identifier="T-1", url="https://example.com")
        assert ci.id == "id-1"
        assert ci.identifier == "T-1"
        assert ci.url == "https://example.com"


class TestComment:
    def test_construction(self):
        c = Comment(body="hello", author="Alice")
        assert c.body == "hello"
        assert c.author == "Alice"
        assert c.created_at is None

    def test_with_created_at(self):
        c = Comment(body="hi", author="Bob", created_at="2026-01-01T00:00:00Z")
        assert c.created_at == "2026-01-01T00:00:00Z"


class TestIssueDetails:
    def test_minimal_construction(self):
        details = IssueDetails(id="uuid-1", ticket_id="T-1", title="Test", url="https://example.com")
        assert details.id == "uuid-1"
        assert details.ticket_id == "T-1"
        assert details.description is None
        assert details.children_ids == []
        assert details.labels == []
        assert details.comments == []
        assert details.raw == {}

    def test_full_construction(self):
        details = IssueDetails(
            id="uuid-2",
            ticket_id="T-2",
            title="Full",
            url="https://example.com/2",
            description="desc",
            status="Todo",
            priority=1,
            assignee_name="Alice",
            labels=["bug"],
            blocked_by=["T-1"],
            comments=[Comment(body="note", author="Alice")],
            raw={"key": "val"},
        )
        assert details.labels == ["bug"]
        assert details.blocked_by == ["T-1"]
        assert details.comments[0].body == "note"
        assert details.raw == {"key": "val"}


class TestActiveIssuesCount:
    def test_construction(self):
        count = ActiveIssuesCount(total=10, by_project={"proj": 5, "other": 5})
        assert count.total == 10
        assert count.by_project["proj"] == 5

    def test_defaults(self):
        count = ActiveIssuesCount(total=0)
        assert count.by_project == {}


# ---------------------------------------------------------------------------
# Error hierarchy
# ---------------------------------------------------------------------------


class TestBugtrackerError:
    def test_is_exception(self):
        assert issubclass(BugtrackerError, Exception)

    def test_can_raise_and_catch(self):
        with pytest.raises(BugtrackerError, match="test error"):
            raise BugtrackerError("test error")


# ---------------------------------------------------------------------------
# Package imports
# ---------------------------------------------------------------------------


class TestPackageImports:
    def test_import_from_package(self):
        """All public types are importable from botfarm.bugtracker."""
        from botfarm.bugtracker import (  # noqa: F401
            ActiveIssuesCount,
            BugtrackerClient,
            BugtrackerError,
            BugtrackerPoller,
            Comment,
            CreatedIssue,
            Issue,
            IssueDetails,
            PollResult,
        )

    def test_import_from_submodules(self):
        from botfarm.bugtracker.base import BugtrackerClient, BugtrackerPoller  # noqa: F401
        from botfarm.bugtracker.errors import BugtrackerError  # noqa: F401
        from botfarm.bugtracker.types import ActiveIssuesCount, Comment, CreatedIssue, Issue, IssueDetails, PollResult  # noqa: F401

    def test_factory_functions_importable(self):
        from botfarm.bugtracker import create_client, create_pollers  # noqa: F401


# ---------------------------------------------------------------------------
# Factory function tests
# ---------------------------------------------------------------------------


class TestCreateClient:
    """Test bugtracker.create_client factory."""

    def test_creates_linear_client(self):
        from botfarm.bugtracker import create_client
        from botfarm.config import BotfarmConfig, LinearBugtrackerConfig
        from botfarm.bugtracker.linear.client import LinearClient

        config = BotfarmConfig(
            projects=[],
            bugtracker=LinearBugtrackerConfig(api_key="test-key"),
        )
        client = create_client(config)
        assert isinstance(client, LinearClient)

    def test_unknown_type_raises(self):
        from botfarm.bugtracker import create_client
        from botfarm.config import BotfarmConfig, BugtrackerConfig

        config = BotfarmConfig(
            projects=[],
            bugtracker=BugtrackerConfig(type="github", api_key="k"),
        )
        with pytest.raises(ValueError, match="Unknown bugtracker type"):
            create_client(config)


class TestCreatePollers:
    """Test bugtracker.create_pollers factory."""

    def test_creates_linear_pollers(self, monkeypatch):
        from botfarm.bugtracker import create_pollers
        from botfarm.config import BotfarmConfig, LinearBugtrackerConfig, ProjectConfig
        from botfarm.bugtracker.linear.poller import LinearPoller

        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj",
                    team="TST",
                    base_dir="~/d",
                    worktree_prefix="s-",
                    slots=[1],
                ),
            ],
            bugtracker=LinearBugtrackerConfig(api_key="test-key"),
        )
        pollers = create_pollers(config)
        assert len(pollers) == 1
        assert isinstance(pollers[0], LinearPoller)

    def test_unknown_type_raises(self):
        from botfarm.bugtracker import create_pollers
        from botfarm.config import BotfarmConfig, BugtrackerConfig

        config = BotfarmConfig(
            projects=[],
            bugtracker=BugtrackerConfig(type="github", api_key="k"),
        )
        with pytest.raises(ValueError, match="Unknown bugtracker type"):
            create_pollers(config)
