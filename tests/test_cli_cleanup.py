"""Tests for the botfarm cleanup CLI command."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from botfarm.cli import main
from botfarm.db import init_db
from botfarm.bugtracker.linear.cleanup import CleanupCandidate, CleanupResult, CleanupService, CooldownError


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def db_file(tmp_path):
    db_path = tmp_path / "botfarm.db"
    conn = init_db(db_path, allow_migration=True)
    conn.close()
    return db_path


def _old_iso(days_ago: int = 30) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _make_config(api_key="test-key", team_key="SMA"):
    """Build a minimal mock BotfarmConfig."""
    from botfarm.config import (
        AgentsConfig,
        BotfarmConfig,
        CoderIdentity,
        DatabaseConfig,
        IdentitiesConfig,
        LinearConfig,
        ProjectConfig,
        ReviewerIdentity,
        UsageLimitsConfig,
    )

    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-proj",
                team=team_key,
                base_dir="/tmp/test",
                worktree_prefix="test-slot-",
                slots=[1],
            )
        ],
        bugtracker=LinearConfig(api_key=api_key),
        identities=IdentitiesConfig(
            coder=CoderIdentity(),
            reviewer=ReviewerIdentity(),
        ),
    )


def _mock_resolve(db_path, config=None):
    """Return a monkeypatch-compatible _resolve_paths replacement."""
    cfg = config or _make_config()
    return lambda _: (db_path, cfg)


def _make_candidate(
    identifier="SMA-100",
    title="Old issue",
    status="Done",
    project_name="TestProj",
    days_ago=30,
) -> CleanupCandidate:
    return CleanupCandidate(
        linear_uuid=f"uuid-{identifier}",
        identifier=identifier,
        title=title,
        updated_at=_old_iso(days_ago),
        completed_at=_old_iso(days_ago),
        labels=[],
        has_active_children=False,
        status=status,
        project_name=project_name,
    )


# ---------------------------------------------------------------------------
# No config / no database
# ---------------------------------------------------------------------------


class TestCleanupErrors:
    def test_no_config(self, runner, db_file, monkeypatch):
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file, None),
        )
        result = runner.invoke(main, ["cleanup"])
        assert result.exit_code != 0
        assert "Config file not found" in result.output

    def test_no_api_key(self, runner, db_file, monkeypatch):
        cfg = _make_config(api_key="")
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (db_file, cfg),
        )
        result = runner.invoke(main, ["cleanup"])
        assert result.exit_code != 0
        assert "api_key" in result.output

    def test_no_database(self, runner, tmp_path, monkeypatch):
        cfg = _make_config()
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths",
            lambda _: (tmp_path / "nonexistent.db", cfg),
        )
        result = runner.invoke(main, ["cleanup"])
        assert result.exit_code != 0
        assert "No database found" in result.output


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------


class TestCleanupDryRun:
    def test_dry_run_shows_candidates(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [
            _make_candidate("SMA-100", "Fix login bug", "Done", "Proj A"),
            _make_candidate("SMA-101", "Add feature", "Canceled", "Proj B"),
        ]
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            result = runner.invoke(main, ["cleanup", "--dry-run"])

        assert result.exit_code == 0
        assert "SMA-100" in result.output
        assert "SMA-101" in result.output
        assert "Fix login bug" in result.output
        assert "Dry run" in result.output
        assert "2 candidate(s)" in result.output

    def test_dry_run_no_candidates(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch.object(
            CleanupService, "fetch_candidates", return_value=[]
        ):
            result = runner.invoke(main, ["cleanup", "--dry-run"])

        assert result.exit_code == 0
        assert "No cleanup candidates" in result.output

    def test_dry_run_shows_status_column(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [_make_candidate("SMA-100", status="Done")]
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            result = runner.invoke(main, ["cleanup", "--dry-run"])

        assert result.exit_code == 0
        assert "Done" in result.output

    def test_dry_run_shows_project_column(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [_make_candidate("SMA-100", project_name="My Project")]
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            result = runner.invoke(main, ["cleanup", "--dry-run"])

        assert result.exit_code == 0
        assert "My Project" in result.output


# ---------------------------------------------------------------------------
# Filter options
# ---------------------------------------------------------------------------


class TestCleanupFilterOptions:
    def test_status_filter_passed_to_service(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        captured_kwargs = {}

        def mock_fetch(self, limit=None, status_filter="all"):
            captured_kwargs["status_filter"] = status_filter
            return []

        with patch.object(CleanupService, "fetch_candidates", mock_fetch):
            result = runner.invoke(main, ["cleanup", "--dry-run", "--status", "done"])

        assert result.exit_code == 0
        assert captured_kwargs["status_filter"] == "done"

    def test_count_option(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        captured_kwargs = {}

        def mock_fetch(self, limit=None, status_filter="all"):
            captured_kwargs["limit"] = limit
            return []

        with patch.object(CleanupService, "fetch_candidates", mock_fetch):
            result = runner.invoke(main, ["cleanup", "--dry-run", "--count", "25"])

        assert result.exit_code == 0
        assert captured_kwargs["limit"] == 25

    def test_count_zero_rejected(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["cleanup", "--dry-run", "--count", "0"])
        assert result.exit_code != 0

    def test_count_negative_rejected(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        result = runner.invoke(main, ["cleanup", "--dry-run", "--count", "-1"])
        assert result.exit_code != 0

    def test_project_resolves_team_from_config(self, runner, db_file, monkeypatch):
        """When --project matches a configured project, its team is used."""
        from botfarm.config import (
            BotfarmConfig,
            IdentitiesConfig,
            CoderIdentity,
            LinearConfig,
            ProjectConfig,
            ReviewerIdentity,
        )

        cfg = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj-a",
                    team="TEAM-A",
                    tracker_project="Project Alpha",
                    base_dir="/tmp/a",
                    worktree_prefix="a-slot-",
                    slots=[1],
                ),
                ProjectConfig(
                    name="proj-b",
                    team="TEAM-B",
                    tracker_project="Project Beta",
                    base_dir="/tmp/b",
                    worktree_prefix="b-slot-",
                    slots=[2],
                ),
            ],
            bugtracker=LinearConfig(api_key="test-key"),
            identities=IdentitiesConfig(
                coder=CoderIdentity(),
                reviewer=ReviewerIdentity(),
            ),
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, cfg)
        )
        captured_kwargs = {}

        original_init = CleanupService.__init__

        def mock_init(self, client, conn, **kwargs):
            captured_kwargs.update(kwargs)
            original_init(self, client, conn, **kwargs)

        with patch.object(CleanupService, "__init__", mock_init):
            with patch.object(CleanupService, "fetch_candidates", return_value=[]):
                result = runner.invoke(
                    main, ["cleanup", "--dry-run", "--project", "Project Beta"]
                )

        assert result.exit_code == 0
        assert captured_kwargs["team_key"] == "TEAM-B"
        assert captured_kwargs["project_name"] == "Project Beta"

    def test_project_name_normalized_to_linear_project(
        self, runner, db_file, monkeypatch
    ):
        """When --project matches by p.name, project is rewritten to p.tracker_project."""
        from botfarm.config import (
            BotfarmConfig,
            IdentitiesConfig,
            CoderIdentity,
            LinearConfig,
            ProjectConfig,
            ReviewerIdentity,
        )

        cfg = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="proj-b",
                    team="TEAM-B",
                    tracker_project="Project Beta",
                    base_dir="/tmp/b",
                    worktree_prefix="b-slot-",
                    slots=[1],
                ),
            ],
            bugtracker=LinearConfig(api_key="test-key"),
            identities=IdentitiesConfig(
                coder=CoderIdentity(),
                reviewer=ReviewerIdentity(),
            ),
        )
        monkeypatch.setattr(
            "botfarm.cli._resolve_paths", lambda _: (db_file, cfg)
        )
        captured_kwargs = {}

        original_init = CleanupService.__init__

        def mock_init(self, client, conn, **kwargs):
            captured_kwargs.update(kwargs)
            original_init(self, client, conn, **kwargs)

        with patch.object(CleanupService, "__init__", mock_init):
            with patch.object(CleanupService, "fetch_candidates", return_value=[]):
                result = runner.invoke(
                    main, ["cleanup", "--dry-run", "--project", "proj-b"]
                )

        assert result.exit_code == 0
        assert captured_kwargs["team_key"] == "TEAM-B"
        # Should be normalized to the Linear project name, not the config name
        assert captured_kwargs["project_name"] == "Project Beta"

    def test_unmatched_project_warns(self, runner, db_file, monkeypatch):
        """When --project doesn't match any configured project, a warning is shown."""
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        with patch.object(CleanupService, "fetch_candidates", return_value=[]):
            result = runner.invoke(
                main, ["cleanup", "--dry-run", "--project", "nonexistent"]
            )

        assert result.exit_code == 0
        assert "Warning" in result.output
        assert "nonexistent" in result.output

    def test_min_age_option(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        captured_kwargs = {}

        original_init = CleanupService.__init__

        def mock_init(self, client, conn, **kwargs):
            captured_kwargs.update(kwargs)
            original_init(self, client, conn, **kwargs)

        with patch.object(CleanupService, "__init__", mock_init):
            with patch.object(CleanupService, "fetch_candidates", return_value=[]):
                result = runner.invoke(
                    main, ["cleanup", "--dry-run", "--min-age", "14"]
                )

        assert result.exit_code == 0
        assert captured_kwargs["min_age_days"] == 14

    def test_project_option(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        captured_kwargs = {}

        original_init = CleanupService.__init__

        def mock_init(self, client, conn, **kwargs):
            captured_kwargs.update(kwargs)
            original_init(self, client, conn, **kwargs)

        with patch.object(CleanupService, "__init__", mock_init):
            with patch.object(CleanupService, "fetch_candidates", return_value=[]):
                result = runner.invoke(
                    main, ["cleanup", "--dry-run", "--project", "My Project"]
                )

        assert result.exit_code == 0
        assert captured_kwargs["project_name"] == "My Project"


# ---------------------------------------------------------------------------
# Confirmation prompt
# ---------------------------------------------------------------------------


class TestCleanupConfirmation:
    def test_prompts_for_confirmation(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [_make_candidate()]
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            # Say no to confirmation
            result = runner.invoke(main, ["cleanup"], input="n\n")

        assert result.exit_code == 0
        assert "Aborted" in result.output

    def test_yes_flag_skips_confirmation(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [_make_candidate()]
        cleanup_result = CleanupResult(
            batch_id="batch-1",
            action="archive",
            total_candidates=1,
            succeeded=1,
        )
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            with patch.object(
                CleanupService, "run_cleanup", return_value=cleanup_result
            ):
                result = runner.invoke(main, ["cleanup", "--yes"])

        assert result.exit_code == 0
        assert "Archived 1/1" in result.output


# ---------------------------------------------------------------------------
# Execution and summary
# ---------------------------------------------------------------------------


class TestCleanupExecution:
    def test_archive_success_summary(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [
            _make_candidate("SMA-100"),
            _make_candidate("SMA-101"),
        ]
        cleanup_result = CleanupResult(
            batch_id="batch-1",
            action="archive",
            total_candidates=2,
            succeeded=2,
        )
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            with patch.object(
                CleanupService, "run_cleanup", return_value=cleanup_result
            ):
                result = runner.invoke(main, ["cleanup", "--yes"])

        assert result.exit_code == 0
        assert "Archived 2/2" in result.output
        assert "successfully" in result.output

    def test_delete_success_summary(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [_make_candidate("SMA-100")]
        cleanup_result = CleanupResult(
            batch_id="batch-1",
            action="delete",
            total_candidates=1,
            succeeded=1,
        )
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            with patch.object(
                CleanupService, "run_cleanup", return_value=cleanup_result
            ):
                result = runner.invoke(
                    main, ["cleanup", "--action", "delete", "--yes"]
                )

        assert result.exit_code == 0
        assert "Deleted 1/1" in result.output

    def test_skipped_issues_shown(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [
            _make_candidate("SMA-100"),
            _make_candidate("SMA-101"),
            _make_candidate("SMA-102"),
        ]
        cleanup_result = CleanupResult(
            batch_id="batch-1",
            action="archive",
            total_candidates=3,
            succeeded=2,
            skipped=1,
        )
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            with patch.object(
                CleanupService, "run_cleanup", return_value=cleanup_result
            ):
                result = runner.invoke(main, ["cleanup", "--yes"])

        assert result.exit_code == 0
        assert "Archived 2/3" in result.output
        assert "1 skipped (backup failed)" in result.output

    def test_failed_issues_shown(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [_make_candidate("SMA-100")]
        cleanup_result = CleanupResult(
            batch_id="batch-1",
            action="archive",
            total_candidates=1,
            succeeded=0,
            failed=1,
            errors=["SMA-100: API error"],
        )
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            with patch.object(
                CleanupService, "run_cleanup", return_value=cleanup_result
            ):
                result = runner.invoke(main, ["cleanup", "--yes"])

        assert result.exit_code == 0
        assert "1 failed" in result.output
        assert "SMA-100: API error" in result.output

    def test_cooldown_error_shown(self, runner, db_file, monkeypatch):
        monkeypatch.setattr("botfarm.cli._resolve_paths", _mock_resolve(db_file))
        candidates = [_make_candidate()]
        with patch.object(
            CleanupService, "fetch_candidates", return_value=candidates
        ):
            with patch.object(
                CleanupService,
                "run_cleanup",
                side_effect=CooldownError("Cooldown active: 200s remaining"),
            ):
                result = runner.invoke(main, ["cleanup", "--yes"])

        assert result.exit_code != 0
        assert "Cooldown active" in result.output


# ---------------------------------------------------------------------------
# Help text
# ---------------------------------------------------------------------------


class TestCleanupHelp:
    def test_help_output(self, runner):
        result = runner.invoke(main, ["cleanup", "--help"])
        assert result.exit_code == 0
        assert "archive" in result.output
        assert "delete" in result.output
        assert "--dry-run" in result.output
        assert "--count" in result.output
        assert "--min-age" in result.output
        assert "--status" in result.output
        assert "--project" in result.output
        assert "--yes" in result.output
