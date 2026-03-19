"""Tests for botfarm.bugtracker.linear.cleanup — CleanupService bulk archive/delete."""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import httpx
import pytest

from botfarm.db import (
    get_cleanup_batch,
    get_cleanup_batch_items,
    get_events,
    get_last_cleanup_batch_time,
    get_ticket_history_entry,
    insert_cleanup_batch,
    insert_cleanup_batch_item,
    update_cleanup_batch,
)
from botfarm.bugtracker.linear.client import LINEAR_API_URL, LinearAPIError, LinearClient
from botfarm.bugtracker.linear.cleanup import (
    CleanupCandidate,
    CleanupResult,
    CleanupService,
    CooldownError,
    UndoResult,
)


# ---------------------------------------------------------------------------
# Fixtures — conn provided by tests/conftest.py
# ---------------------------------------------------------------------------


@pytest.fixture()
def client():
    """Return a LinearClient with a fake API key."""
    return LinearClient(api_key="test-key")


def _graphql_response(data: dict, rate_limit_remaining: int | None = None) -> httpx.Response:
    """Build a fake successful httpx.Response containing a GraphQL data payload."""
    headers = {}
    if rate_limit_remaining is not None:
        headers["X-RateLimit-Requests-Remaining"] = str(rate_limit_remaining)
    return httpx.Response(
        status_code=200,
        json={"data": data},
        headers=headers,
        request=httpx.Request("POST", LINEAR_API_URL),
    )


def _old_iso(days_ago: int = 30) -> str:
    """Return an ISO timestamp N days in the past."""
    dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _recent_iso() -> str:
    """Return an ISO timestamp from 1 day ago (too recent for default 7-day filter)."""
    dt = datetime.now(timezone.utc) - timedelta(days=1)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _make_completed_issue_node(
    *,
    id: str = "uuid-1",
    identifier: str = "SMA-100",
    title: str = "Old issue",
    updated_at: str | None = None,
    completed_at: str | None = None,
    labels: list[str] | None = None,
    child_states: list[str] | None = None,
) -> dict:
    """Build a GraphQL issue node for completed/canceled issues query."""
    label_nodes = [{"name": lbl} for lbl in (labels or [])]
    children_nodes = []
    if child_states:
        for i, state in enumerate(child_states):
            children_nodes.append({"id": f"child-{i}", "state": {"type": state}})

    return {
        "id": id,
        "identifier": identifier,
        "title": title,
        "updatedAt": updated_at or _old_iso(),
        "completedAt": completed_at or _old_iso(),
        "labels": {"nodes": label_nodes},
        "children": {"nodes": children_nodes},
    }


# ---------------------------------------------------------------------------
# LinearClient — rate limit tracking
# ---------------------------------------------------------------------------


class TestRateLimitTracking:
    def test_rate_limit_initially_none(self, client):
        assert client.rate_limit_remaining is None

    def test_rate_limit_extracted_from_header(self, client):
        resp = _graphql_response({"viewer": {"id": "u1"}}, rate_limit_remaining=150)
        with patch.object(httpx, "post", return_value=resp):
            client._execute("query { viewer { id } }")
        assert client.rate_limit_remaining == 150

    def test_rate_limit_updated_on_each_call(self, client):
        resp1 = _graphql_response({"viewer": {"id": "u1"}}, rate_limit_remaining=100)
        resp2 = _graphql_response({"viewer": {"id": "u1"}}, rate_limit_remaining=50)
        with patch.object(httpx, "post", side_effect=[resp1, resp2]):
            client._execute("q1")
            client._execute("q2")
        assert client.rate_limit_remaining == 50


# ---------------------------------------------------------------------------
# LinearClient — archive/delete/unarchive mutations
# ---------------------------------------------------------------------------


class TestArchiveDeleteMutations:
    def test_archive_issue_success(self, client):
        resp = _graphql_response({"issueArchive": {"success": True}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.archive_issue("uuid-1")
        assert result is True

    def test_archive_issue_failure(self, client):
        resp = _graphql_response({"issueArchive": {"success": False}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.archive_issue("uuid-1")
        assert result is False

    def test_delete_issue_success(self, client):
        resp = _graphql_response({"issueDelete": {"success": True}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.delete_issue("uuid-1")
        assert result is True

    def test_delete_issue_failure(self, client):
        resp = _graphql_response({"issueDelete": {"success": False}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.delete_issue("uuid-1")
        assert result is False

    def test_unarchive_issue_success(self, client):
        resp = _graphql_response({"issueUnarchive": {"success": True}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.unarchive_issue("uuid-1")
        assert result is True

    def test_unarchive_issue_api_error(self, client):
        with patch.object(
            httpx, "post", side_effect=httpx.ConnectError("connection refused")
        ):
            with pytest.raises(LinearAPIError):
                client.unarchive_issue("uuid-1")


# ---------------------------------------------------------------------------
# LinearClient — fetch_completed_issues
# ---------------------------------------------------------------------------


class TestFetchCompletedIssues:
    def test_returns_nodes(self, client):
        nodes = [_make_completed_issue_node()]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_completed_issues(team_key="SMA")
        assert len(result) == 1
        assert result[0].identifier == "SMA-100"

    def test_with_project_name(self, client):
        nodes = [_make_completed_issue_node()]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp) as mock_post:
            client.fetch_completed_issues(
                team_key="SMA", project_name="My Project"
            )
        body = mock_post.call_args.kwargs["json"]
        assert "projectName" in body.get("variables", {})

    def test_empty_result(self, client):
        resp = _graphql_response({"issues": {"nodes": []}})
        with patch.object(httpx, "post", return_value=resp):
            result = client.fetch_completed_issues(team_key="SMA")
        assert result == []


# ---------------------------------------------------------------------------
# DB helpers for cleanup batches
# ---------------------------------------------------------------------------


class TestCleanupBatchDb:
    def test_insert_and_get_batch(self, conn):
        insert_cleanup_batch(
            conn,
            batch_id="batch-1",
            action="archive",
            team_key="SMA",
            project_name="proj",
            total=5,
        )
        conn.commit()
        batch = get_cleanup_batch(conn, "batch-1")
        assert batch is not None
        assert batch["action"] == "archive"
        assert batch["total"] == 5

    def test_update_batch_counts(self, conn):
        insert_cleanup_batch(conn, batch_id="batch-2", action="archive", total=10)
        update_cleanup_batch(conn, "batch-2", succeeded=7, failed=2, skipped=1)
        conn.commit()
        batch = get_cleanup_batch(conn, "batch-2")
        assert batch["succeeded"] == 7
        assert batch["failed"] == 2
        assert batch["skipped"] == 1

    def test_insert_and_get_batch_items(self, conn):
        insert_cleanup_batch(conn, batch_id="batch-3", action="archive")
        insert_cleanup_batch_item(
            conn,
            batch_id="batch-3",
            tracker_uuid="uuid-1",
            identifier="SMA-100",
            action="archive",
            success=True,
        )
        insert_cleanup_batch_item(
            conn,
            batch_id="batch-3",
            tracker_uuid="uuid-2",
            identifier="SMA-101",
            action="archive",
            success=False,
            error="api error",
        )
        conn.commit()

        all_items = get_cleanup_batch_items(conn, "batch-3")
        assert len(all_items) == 2

        success_items = get_cleanup_batch_items(conn, "batch-3", success_only=True)
        assert len(success_items) == 1
        assert success_items[0]["identifier"] == "SMA-100"

    def test_get_last_cleanup_batch_time(self, conn):
        assert get_last_cleanup_batch_time(conn) is None

        insert_cleanup_batch(conn, batch_id="batch-old", action="archive")
        conn.commit()
        ts = get_last_cleanup_batch_time(conn)
        assert ts is not None

    def test_get_nonexistent_batch(self, conn):
        assert get_cleanup_batch(conn, "nonexistent") is None


# ---------------------------------------------------------------------------
# CleanupService — candidate filtering
# ---------------------------------------------------------------------------


class TestCleanupServiceCandidates:
    def _make_service(self, client, conn, **kwargs):
        defaults = {
            "team_key": "SMA",
            "protected_label": "Do Not Archive",
            "min_age_days": 7,
            "default_limit": 50,
        }
        defaults.update(kwargs)
        return CleanupService(client, conn, **defaults)

    def test_fetch_candidates_filters_protected_label(self, client, conn):
        nodes = [
            _make_completed_issue_node(
                id="uuid-1", identifier="SMA-100", labels=["Do Not Archive"]
            ),
            _make_completed_issue_node(
                id="uuid-2", identifier="SMA-101", labels=["Feature"]
            ),
        ]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp):
            svc = self._make_service(client, conn)
            candidates = svc.fetch_candidates()
        assert len(candidates) == 1
        assert candidates[0].identifier == "SMA-101"

    def test_fetch_candidates_filters_recent_issues(self, client, conn):
        nodes = [
            _make_completed_issue_node(
                id="uuid-1",
                identifier="SMA-100",
                updated_at=_recent_iso(),
                completed_at=_recent_iso(),
            ),
            _make_completed_issue_node(
                id="uuid-2",
                identifier="SMA-101",
                updated_at=_old_iso(30),
                completed_at=_old_iso(30),
            ),
        ]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp):
            svc = self._make_service(client, conn)
            candidates = svc.fetch_candidates()
        assert len(candidates) == 1
        assert candidates[0].identifier == "SMA-101"

    def test_fetch_candidates_filters_parents_with_active_children(self, client, conn):
        nodes = [
            _make_completed_issue_node(
                id="uuid-1",
                identifier="SMA-100",
                child_states=["completed", "started"],
            ),
            _make_completed_issue_node(
                id="uuid-2",
                identifier="SMA-101",
                child_states=["completed", "canceled"],
            ),
            _make_completed_issue_node(
                id="uuid-3",
                identifier="SMA-102",
            ),
        ]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp):
            svc = self._make_service(client, conn)
            candidates = svc.fetch_candidates()
        # SMA-100 has an active child, SMA-101 has all done children, SMA-102 has no children
        identifiers = {c.identifier for c in candidates}
        assert identifiers == {"SMA-101", "SMA-102"}

    def test_fetch_candidates_case_insensitive_label(self, client, conn):
        nodes = [
            _make_completed_issue_node(
                id="uuid-1", identifier="SMA-100", labels=["do not archive"]
            ),
        ]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp):
            svc = self._make_service(client, conn)
            candidates = svc.fetch_candidates()
        assert len(candidates) == 0

    def test_fetch_candidates_custom_age(self, client, conn):
        # Issue 3 days old with min_age_days=2 should be included
        three_days_ago = _old_iso(3)
        nodes = [
            _make_completed_issue_node(
                id="uuid-1",
                identifier="SMA-100",
                updated_at=three_days_ago,
                completed_at=three_days_ago,
            ),
        ]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp):
            svc = self._make_service(client, conn, min_age_days=2)
            candidates = svc.fetch_candidates()
        assert len(candidates) == 1


# ---------------------------------------------------------------------------
# CleanupService — cooldown
# ---------------------------------------------------------------------------


class TestCleanupServiceCooldown:
    def test_cooldown_no_previous_batch(self, client, conn):
        svc = CleanupService(client, conn, team_key="SMA")
        # Should not raise
        svc._check_cooldown()

    def test_cooldown_enforced(self, client, conn):
        insert_cleanup_batch(conn, batch_id="recent-batch", action="archive")
        conn.commit()
        svc = CleanupService(client, conn, team_key="SMA")
        with pytest.raises(CooldownError, match="Cooldown active"):
            svc._check_cooldown()


# ---------------------------------------------------------------------------
# CleanupService — run_cleanup
# ---------------------------------------------------------------------------


class TestCleanupServiceRun:
    def _make_service(self, client, conn, **kwargs):
        defaults = {
            "team_key": "SMA",
            "min_age_days": 7,
            "default_limit": 50,
        }
        defaults.update(kwargs)
        return CleanupService(client, conn, **defaults)

    def _mock_fetch_and_archive(self, nodes, archive_success=True):
        """Return side_effect list for fetch + detail-backup + archive calls."""
        # 1. fetch completed issues
        fetch_resp = _graphql_response({"issues": {"nodes": nodes}})
        # 2. fetch_issue_details for backup (per candidate)
        detail_resp = _graphql_response({
            "issue": {
                "id": "uuid-1",
                "identifier": "SMA-100",
                "title": "Old issue",
                "description": "desc",
                "priority": 3,
                "url": "https://linear.app/test/SMA-100",
                "estimate": None,
                "dueDate": None,
                "createdAt": _old_iso(),
                "updatedAt": _old_iso(),
                "completedAt": _old_iso(),
                "state": {"name": "Done"},
                "creator": {"name": "User"},
                "assignee": {"name": "Bot", "email": "bot@test.com"},
                "project": {"name": "TestProj"},
                "team": {"name": "SMA", "key": "SMA"},
                "parent": None,
                "children": {"nodes": []},
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "inverseRelations": {"nodes": []},
                "comments": {"nodes": []},
            }
        })
        # 3. archive/delete mutation
        archive_resp = _graphql_response(
            {"issueArchive": {"success": archive_success}}
        )
        return [fetch_resp, detail_resp, archive_resp]

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_archive_single_issue(self, mock_sleep, client, conn):
        nodes = [_make_completed_issue_node()]
        responses = self._mock_fetch_and_archive(nodes)
        with patch.object(httpx, "post", side_effect=responses):
            svc = self._make_service(client, conn)
            result = svc.run_cleanup(action="archive")

        assert result.succeeded == 1
        assert result.failed == 0
        assert result.skipped == 0
        assert result.batch_id

        # Check batch is in DB
        batch = get_cleanup_batch(conn, result.batch_id)
        assert batch is not None
        assert batch["succeeded"] == 1

        # Check batch items
        items = get_cleanup_batch_items(conn, result.batch_id)
        assert len(items) == 1
        assert items[0]["success"] == 1

        # Check audit event
        events = get_events(conn, event_type="cleanup_archive")
        assert len(events) == 1

        # Check ticket backed up
        ticket = get_ticket_history_entry(conn, "SMA-100")
        assert ticket is not None

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_delete_single_issue(self, mock_sleep, client, conn):
        nodes = [_make_completed_issue_node()]
        fetch_resp = _graphql_response({"issues": {"nodes": nodes}})
        detail_resp = _graphql_response({
            "issue": {
                "id": "uuid-1", "identifier": "SMA-100", "title": "Old issue",
                "description": None, "priority": 3,
                "url": "https://linear.app/test/SMA-100",
                "estimate": None, "dueDate": None,
                "createdAt": _old_iso(), "updatedAt": _old_iso(),
                "completedAt": _old_iso(),
                "state": {"name": "Done"},
                "creator": {"name": "User"},
                "assignee": {"name": "Bot", "email": "bot@test.com"},
                "project": {"name": "TestProj"},
                "team": {"name": "SMA", "key": "SMA"},
                "parent": None, "children": {"nodes": []},
                "labels": {"nodes": []},
                "relations": {"nodes": []},
                "inverseRelations": {"nodes": []},
                "comments": {"nodes": []},
            }
        })
        delete_resp = _graphql_response({"issueDelete": {"success": True}})

        with patch.object(httpx, "post", side_effect=[fetch_resp, detail_resp, delete_resp]):
            svc = self._make_service(client, conn)
            result = svc.run_cleanup(action="delete")

        assert result.action == "delete"
        assert result.succeeded == 1

        events = get_events(conn, event_type="cleanup_delete")
        assert len(events) == 1

    def test_invalid_action_raises(self, client, conn):
        svc = self._make_service(client, conn)
        with pytest.raises(ValueError, match="Invalid action"):
            svc.run_cleanup(action="purge")

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_dry_run_does_not_execute(self, mock_sleep, client, conn):
        nodes = [_make_completed_issue_node()]
        resp = _graphql_response({"issues": {"nodes": nodes}})
        with patch.object(httpx, "post", return_value=resp):
            svc = self._make_service(client, conn)
            result = svc.run_cleanup(action="archive", dry_run=True)

        assert result.total_candidates == 1
        assert result.succeeded == 0
        # No batch should be in DB
        assert get_cleanup_batch(conn, result.batch_id) is None

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_no_candidates_returns_empty_result(self, mock_sleep, client, conn):
        resp = _graphql_response({"issues": {"nodes": []}})
        with patch.object(httpx, "post", return_value=resp):
            svc = self._make_service(client, conn)
            result = svc.run_cleanup(action="archive")

        assert result.total_candidates == 0
        assert result.succeeded == 0

    def test_cooldown_blocks_cleanup(self, client, conn):
        insert_cleanup_batch(conn, batch_id="recent", action="archive")
        conn.commit()
        svc = self._make_service(client, conn)
        with pytest.raises(CooldownError):
            svc.run_cleanup(action="archive")

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_backup_failure_skips_issue(self, mock_sleep, client, conn):
        nodes = [_make_completed_issue_node()]
        fetch_resp = _graphql_response({"issues": {"nodes": nodes}})
        # Backup (fetch_issue_details) will fail
        with patch.object(
            httpx,
            "post",
            side_effect=[
                fetch_resp,
                httpx.ConnectError("connection refused"),
            ],
        ):
            svc = self._make_service(client, conn)
            result = svc.run_cleanup(action="archive")

        assert result.skipped == 1
        assert result.succeeded == 0
        items = get_cleanup_batch_items(conn, result.batch_id)
        assert len(items) == 1
        assert items[0]["error"] == "backup_failed"

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_archive_api_failure_continues(self, mock_sleep, client, conn):
        nodes = [
            _make_completed_issue_node(id="uuid-1", identifier="SMA-100"),
            _make_completed_issue_node(id="uuid-2", identifier="SMA-101"),
        ]
        fetch_resp = _graphql_response({"issues": {"nodes": nodes}})

        def _make_detail(ident):
            return _graphql_response({
                "issue": {
                    "id": f"uuid-{ident}", "identifier": ident, "title": "T",
                    "description": None, "priority": 3,
                    "url": f"https://linear.app/test/{ident}",
                    "estimate": None, "dueDate": None,
                    "createdAt": _old_iso(), "updatedAt": _old_iso(),
                    "completedAt": _old_iso(),
                    "state": {"name": "Done"},
                    "creator": {"name": "U"},
                    "assignee": {"name": "B", "email": "b@t.com"},
                    "project": {"name": "P"},
                    "team": {"name": "SMA", "key": "SMA"},
                    "parent": None, "children": {"nodes": []},
                    "labels": {"nodes": []},
                    "relations": {"nodes": []},
                    "inverseRelations": {"nodes": []},
                    "comments": {"nodes": []},
                }
            })

        archive_fail = httpx.Response(
            status_code=200,
            json={"errors": [{"message": "not found"}]},
            request=httpx.Request("POST", LINEAR_API_URL),
        )
        archive_success = _graphql_response({"issueArchive": {"success": True}})

        responses = [
            fetch_resp,
            _make_detail("SMA-100"),
            archive_fail,
            _make_detail("SMA-101"),
            archive_success,
        ]
        with patch.object(httpx, "post", side_effect=responses):
            svc = self._make_service(client, conn)
            result = svc.run_cleanup(action="archive")

        assert result.failed == 1
        assert result.succeeded == 1
        assert len(result.errors) == 1

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_rate_limit_pause(self, mock_sleep, client, conn):
        """When rate limit is low, service pauses before next operation."""
        nodes = [_make_completed_issue_node()]
        # Fetch: rate limit ok. Backup: rate limit low. Archive.
        fetch_resp = _graphql_response(
            {"issues": {"nodes": nodes}}, rate_limit_remaining=100
        )
        detail_resp = _graphql_response(
            {
                "issue": {
                    "id": "uuid-1", "identifier": "SMA-100", "title": "T",
                    "description": None, "priority": 3,
                    "url": "u", "estimate": None, "dueDate": None,
                    "createdAt": _old_iso(), "updatedAt": _old_iso(),
                    "completedAt": _old_iso(),
                    "state": {"name": "Done"},
                    "creator": {"name": "U"},
                    "assignee": {"name": "B", "email": "b@t.com"},
                    "project": {"name": "P"},
                    "team": {"name": "SMA", "key": "SMA"},
                    "parent": None, "children": {"nodes": []},
                    "labels": {"nodes": []},
                    "relations": {"nodes": []},
                    "inverseRelations": {"nodes": []},
                    "comments": {"nodes": []},
                }
            },
            rate_limit_remaining=30,
        )
        archive_resp = _graphql_response(
            {"issueArchive": {"success": True}}, rate_limit_remaining=29
        )

        with patch.object(httpx, "post", side_effect=[fetch_resp, detail_resp, archive_resp]):
            svc = self._make_service(client, conn)
            result = svc.run_cleanup(action="archive")

        assert result.succeeded == 1
        # Check that sleep was called — once for rate limit pause, once for inter-op delay
        assert any(
            call.args[0] == CleanupService.RATE_LIMIT_PAUSE_SECONDS
            for call in mock_sleep.call_args_list
        )


# ---------------------------------------------------------------------------
# CleanupService — undo
# ---------------------------------------------------------------------------


class TestCleanupServiceUndo:
    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_undo_archive_batch(self, mock_sleep, conn):
        client = LinearClient(api_key="test-key")
        # Set up a batch with two archived items
        insert_cleanup_batch(conn, batch_id="undo-batch", action="archive", total=2)
        insert_cleanup_batch_item(
            conn,
            batch_id="undo-batch",
            tracker_uuid="uuid-1",
            identifier="SMA-100",
            action="archive",
            success=True,
        )
        insert_cleanup_batch_item(
            conn,
            batch_id="undo-batch",
            tracker_uuid="uuid-2",
            identifier="SMA-101",
            action="archive",
            success=True,
        )
        conn.commit()

        unarchive_resp = _graphql_response({"issueUnarchive": {"success": True}})

        with patch.object(httpx, "post", return_value=unarchive_resp):
            svc = CleanupService(client, conn, team_key="SMA")
            result = svc.undo_batch("undo-batch")

        assert result.succeeded == 2
        assert result.failed == 0

        events = get_events(conn, event_type="cleanup_unarchive")
        assert len(events) == 2

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_undo_skips_failed_items(self, mock_sleep, conn):
        client = LinearClient(api_key="test-key")
        insert_cleanup_batch(conn, batch_id="undo-batch-2", action="archive", total=2)
        insert_cleanup_batch_item(
            conn,
            batch_id="undo-batch-2",
            tracker_uuid="uuid-1",
            identifier="SMA-100",
            action="archive",
            success=True,
        )
        insert_cleanup_batch_item(
            conn,
            batch_id="undo-batch-2",
            tracker_uuid="uuid-2",
            identifier="SMA-101",
            action="archive",
            success=False,
            error="some error",
        )
        conn.commit()

        unarchive_resp = _graphql_response({"issueUnarchive": {"success": True}})
        with patch.object(httpx, "post", return_value=unarchive_resp):
            svc = CleanupService(client, conn, team_key="SMA")
            result = svc.undo_batch("undo-batch-2")

        # Only 1 item (the successful one) should be unarchived
        assert result.total == 1
        assert result.succeeded == 1

    def test_undo_nonexistent_batch(self, conn):
        client = LinearClient(api_key="test-key")
        svc = CleanupService(client, conn, team_key="SMA")
        with pytest.raises(ValueError, match="not found"):
            svc.undo_batch("nonexistent")

    def test_undo_delete_batch_raises(self, conn):
        client = LinearClient(api_key="test-key")
        insert_cleanup_batch(conn, batch_id="del-batch", action="delete")
        conn.commit()
        svc = CleanupService(client, conn, team_key="SMA")
        with pytest.raises(ValueError, match="only archive batches"):
            svc.undo_batch("del-batch")

    @patch("botfarm.bugtracker.linear.cleanup.time.sleep")
    def test_undo_partial_failure(self, mock_sleep, conn):
        client = LinearClient(api_key="test-key")
        insert_cleanup_batch(conn, batch_id="undo-partial", action="archive", total=2)
        insert_cleanup_batch_item(
            conn,
            batch_id="undo-partial",
            tracker_uuid="uuid-1",
            identifier="SMA-100",
            action="archive",
            success=True,
        )
        insert_cleanup_batch_item(
            conn,
            batch_id="undo-partial",
            tracker_uuid="uuid-2",
            identifier="SMA-101",
            action="archive",
            success=True,
        )
        conn.commit()

        success_resp = _graphql_response({"issueUnarchive": {"success": True}})
        fail_resp = httpx.Response(
            status_code=200,
            json={"errors": [{"message": "issue not found"}]},
            request=httpx.Request("POST", LINEAR_API_URL),
        )

        with patch.object(httpx, "post", side_effect=[success_resp, fail_resp]):
            svc = CleanupService(client, conn, team_key="SMA")
            result = svc.undo_batch("undo-partial")

        assert result.succeeded == 1
        assert result.failed == 1
        assert len(result.errors) == 1
