"""Tests for the daily work summary feature (SMA-360)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from botfarm.config import (
    BotfarmConfig,
    DailySummaryConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
    load_config,
)
from botfarm.db import (
    get_daily_summary_data,
    get_events,
    init_db,
    insert_event,
    insert_task,
    update_task,
    upsert_ticket_history,
)
from botfarm.notifications import Notifier


# ---------------------------------------------------------------------------
# Config parsing and validation
# ---------------------------------------------------------------------------


class TestDailySummaryConfig:
    def test_defaults(self):
        ds = DailySummaryConfig()
        assert ds.enabled is False
        assert ds.send_hour == 18
        assert ds.min_tasks_for_summary == 0
        assert ds.webhook_url == ""

    def test_config_includes_daily_summary(self):
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="p", team="T", base_dir="/tmp",
                    worktree_prefix="p-slot-", slots=[1],
                ),
            ],
            bugtracker=LinearConfig(api_key="key"),
            daily_summary=DailySummaryConfig(enabled=True, send_hour=9),
        )
        assert config.daily_summary.enabled is True
        assert config.daily_summary.send_hour == 9

    def test_config_loads_daily_summary_from_yaml(self, tmp_path):
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("""\
projects:
  - name: test
    team: TST
    base_dir: /tmp/test
    worktree_prefix: test-slot-
    slots: [1]
bugtracker:
  api_key: fake-key
daily_summary:
  enabled: true
  send_hour: 9
  min_tasks_for_summary: 3
  webhook_url: https://hooks.slack.com/test
""")
        config = load_config(config_yaml)
        assert config.daily_summary.enabled is True
        assert config.daily_summary.send_hour == 9
        assert config.daily_summary.min_tasks_for_summary == 3
        assert config.daily_summary.webhook_url == "https://hooks.slack.com/test"

    def test_config_defaults_when_not_specified(self, tmp_path):
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("""\
projects:
  - name: test
    team: TST
    base_dir: /tmp/test
    worktree_prefix: test-slot-
    slots: [1]
bugtracker:
  api_key: fake-key
""")
        config = load_config(config_yaml)
        assert config.daily_summary.enabled is False
        assert config.daily_summary.send_hour == 18

    def test_invalid_send_hour_rejected(self, tmp_path):
        from botfarm.config import ConfigError
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("""\
projects:
  - name: test
    team: TST
    base_dir: /tmp/test
    worktree_prefix: test-slot-
    slots: [1]
bugtracker:
  api_key: fake-key
daily_summary:
  send_hour: 25
""")
        with pytest.raises(ConfigError, match="send_hour"):
            load_config(config_yaml)

    def test_negative_min_tasks_rejected(self, tmp_path):
        from botfarm.config import ConfigError
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("""\
projects:
  - name: test
    team: TST
    base_dir: /tmp/test
    worktree_prefix: test-slot-
    slots: [1]
bugtracker:
  api_key: fake-key
daily_summary:
  min_tasks_for_summary: -1
""")
        with pytest.raises(ConfigError, match="min_tasks_for_summary"):
            load_config(config_yaml)

    def test_daily_summary_in_known_keys(self, tmp_path):
        """Ensure daily_summary doesn't trigger 'unknown key' warning."""
        config_yaml = tmp_path / "config.yaml"
        config_yaml.write_text("""\
projects:
  - name: test
    team: TST
    base_dir: /tmp/test
    worktree_prefix: test-slot-
    slots: [1]
bugtracker:
  api_key: fake-key
daily_summary:
  enabled: false
""")
        import logging
        with patch.object(logging.getLogger("botfarm.config"), "warning") as mock_warn:
            load_config(config_yaml)
        for call in mock_warn.call_args_list:
            assert "daily_summary" not in str(call)


# ---------------------------------------------------------------------------
# DB helper: get_daily_summary_data
# ---------------------------------------------------------------------------


class TestGetDailySummaryData:
    def test_empty_db_returns_zeros(self, conn):
        result = get_daily_summary_data(conn)
        assert result["tasks"] == []
        assert result["total_completed"] == 0
        assert result["total_failed"] == 0
        assert result["total_duration_seconds"] == 0.0

    def test_returns_recent_completed_tasks(self, conn):
        now = datetime.now(timezone.utc)
        tid = insert_task(
            conn, ticket_id="TST-1", title="Fix bug",
            project="proj", slot=1, status="completed",
        )
        update_task(
            conn, tid,
            status="completed",
            started_at=(now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            completed_at=(now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()

        result = get_daily_summary_data(conn)
        assert result["total_completed"] == 1
        assert result["total_failed"] == 0
        assert len(result["tasks"]) == 1
        assert result["tasks"][0]["ticket_id"] == "TST-1"

    def test_returns_failed_tasks(self, conn):
        now = datetime.now(timezone.utc)
        tid = insert_task(
            conn, ticket_id="TST-2", title="Broken",
            project="proj", slot=1, status="failed",
        )
        update_task(
            conn, tid,
            status="failed",
            failure_reason="tests failed",
            started_at=(now - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            completed_at=(now - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()

        result = get_daily_summary_data(conn)
        assert result["total_failed"] == 1
        assert result["tasks"][0]["failure_reason"] == "tests failed"

    def test_excludes_old_tasks(self, conn):
        old_time = (datetime.now(timezone.utc) - timedelta(hours=25))
        tid = insert_task(
            conn, ticket_id="TST-OLD", title="Old task",
            project="proj", slot=1, status="completed",
        )
        update_task(
            conn, tid,
            status="completed",
            started_at=(old_time - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            completed_at=old_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()

        result = get_daily_summary_data(conn)
        assert result["tasks"] == []

    def test_excludes_in_progress_tasks(self, conn):
        now = datetime.now(timezone.utc)
        tid = insert_task(
            conn, ticket_id="TST-IP", title="In progress",
            project="proj", slot=1, status="in_progress",
        )
        update_task(
            conn, tid,
            started_at=(now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()

        result = get_daily_summary_data(conn)
        assert result["tasks"] == []

    def test_joins_ticket_history(self, conn):
        now = datetime.now(timezone.utc)
        tid = insert_task(
            conn, ticket_id="TST-TH", title="With history",
            project="proj", slot=1, status="completed",
        )
        update_task(
            conn, tid,
            status="completed",
            started_at=(now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            completed_at=(now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        upsert_ticket_history(
            conn,
            ticket_id="TST-TH",
            title="With history",
            description="A detailed description",
            capture_source="test",
        )
        conn.commit()

        result = get_daily_summary_data(conn)
        assert result["tasks"][0]["description"] == "A detailed description"

    def test_calculates_duration(self, conn):
        now = datetime.now(timezone.utc)
        tid = insert_task(
            conn, ticket_id="TST-DUR", title="Duration test",
            project="proj", slot=1, status="completed",
        )
        update_task(
            conn, tid,
            status="completed",
            started_at=(now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            completed_at=(now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()

        result = get_daily_summary_data(conn)
        assert result["total_duration_seconds"] == pytest.approx(3600.0, abs=2)


# ---------------------------------------------------------------------------
# Notifier: notify_daily_summary
# ---------------------------------------------------------------------------


class TestNotifyDailySummary:
    @pytest.fixture()
    def notifier(self):
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

    def test_sends_slack_payload(self, notifier):
        notifier.notify_daily_summary("Summary Title", "- Item 1\n- Item 2")
        notifier._client.post.assert_called_once()
        args, kwargs = notifier._client.post.call_args
        assert args[0] == "https://hooks.slack.com/services/xxx"
        payload = kwargs["json"]
        assert "text" in payload
        assert "*Summary Title*" in payload["text"]
        assert "Item 1" in payload["text"]

    def test_uses_override_webhook_url(self, notifier):
        notifier.notify_daily_summary(
            "Test", "body",
            webhook_url="https://hooks.slack.com/other",
        )
        args, _ = notifier._client.post.call_args
        assert args[0] == "https://hooks.slack.com/other"

    def test_discord_format_for_discord_url(self, notifier):
        notifier.notify_daily_summary(
            "Test", "body",
            webhook_url="https://discord.com/api/webhooks/xxx",
        )
        payload = notifier._client.post.call_args[1]["json"]
        assert "content" in payload
        assert "text" not in payload

    def test_not_rate_limited(self, notifier):
        notifier.notify_daily_summary("A", "first")
        notifier.notify_daily_summary("B", "second")
        assert notifier._client.post.call_count == 2

    def test_noop_when_disabled(self):
        n = Notifier(NotificationsConfig(webhook_url=""))
        n._client = MagicMock()
        n.notify_daily_summary("Test", "body")
        n._client.post.assert_not_called()
        n.close()

    def test_noop_when_no_url_and_no_override(self):
        n = Notifier(NotificationsConfig(webhook_url=""))
        n._client = MagicMock()
        n.notify_daily_summary("Test", "body", webhook_url="")
        n._client.post.assert_not_called()
        n.close()

    def test_sends_via_override_even_when_main_url_empty(self):
        n = Notifier(NotificationsConfig(webhook_url=""))
        n._client = MagicMock()
        response = MagicMock()
        response.raise_for_status = MagicMock()
        n._client.post.return_value = response
        n.notify_daily_summary(
            "Test", "body",
            webhook_url="https://hooks.slack.com/override",
        )
        n._client.post.assert_called_once()
        n.close()

    def test_error_does_not_raise(self, notifier):
        notifier._client.post.side_effect = Exception("connection refused")
        notifier.notify_daily_summary("Test", "body")  # Should not raise


# ---------------------------------------------------------------------------
# Supervisor integration: _maybe_send_daily_summary
# ---------------------------------------------------------------------------


def _make_config_with_summary(tmp_path, *, enabled=True, send_hour=12):
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
        bugtracker=LinearConfig(api_key="test-key", poll_interval_seconds=10),
        daily_summary=DailySummaryConfig(
            enabled=enabled,
            send_hour=send_hour,
            webhook_url="https://hooks.slack.com/test",
        ),
    )


class TestMaybeSendDailySummary:
    @pytest.fixture()
    def supervisor(self, tmp_path, monkeypatch):
        monkeypatch.setenv("BOTFARM_DB_PATH", str(tmp_path / "test.db"))
        (tmp_path / "repo").mkdir(exist_ok=True)

        from botfarm.bugtracker.types import PollResult
        mock_poller = MagicMock()
        mock_poller.project_name = "test-project"
        mock_poller.team_key = "TST"
        mock_poller.poll.return_value = PollResult(
            candidates=[], blocked=[], auto_close_parents=[],
        )

        config = _make_config_with_summary(tmp_path, send_hour=12)
        with patch("botfarm.supervisor.create_pollers", return_value=[mock_poller]):
            from botfarm.supervisor import Supervisor
            sup = Supervisor(config, log_dir=tmp_path / "logs")
        sup._notifier = MagicMock()
        sup._notifier.enabled = True
        return sup

    def test_skipped_when_disabled(self, supervisor):
        supervisor._config.daily_summary.enabled = False
        with patch("botfarm.supervisor_ops.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            supervisor._maybe_send_daily_summary()
        supervisor._notifier.notify_daily_summary.assert_not_called()

    def test_skipped_when_wrong_hour(self, supervisor):
        with patch("botfarm.supervisor_ops.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 6, 15, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            supervisor._maybe_send_daily_summary()
        supervisor._notifier.notify_daily_summary.assert_not_called()

    def test_sends_quiet_day_when_no_tasks(self, supervisor):
        with patch("botfarm.supervisor_ops.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            supervisor._maybe_send_daily_summary()

        supervisor._notifier.notify_daily_summary.assert_called_once()
        call_args = supervisor._notifier.notify_daily_summary.call_args
        assert "Quiet Day" in call_args[0][0]

    def test_records_event_on_send(self, supervisor):
        with patch("botfarm.supervisor_ops.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            supervisor._maybe_send_daily_summary()

        events = get_events(supervisor._conn, event_type="daily_summary_sent")
        assert len(events) == 1

    def test_dedup_prevents_double_send(self, supervisor):
        # First send
        with patch("botfarm.supervisor_ops.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            mock_dt.fromisoformat = datetime.fromisoformat
            supervisor._maybe_send_daily_summary()

        assert supervisor._notifier.notify_daily_summary.call_count == 1

        # Second call — should be deduped (event was recorded < 20h ago)
        with patch("botfarm.supervisor_ops.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 30, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            mock_dt.fromisoformat = datetime.fromisoformat
            supervisor._maybe_send_daily_summary()

        assert supervisor._notifier.notify_daily_summary.call_count == 1

    def test_invokes_claude_for_tasks(self, supervisor):
        now = datetime.now(timezone.utc)
        tid = insert_task(
            supervisor._conn, ticket_id="TST-1", title="Add feature",
            project="test-project", slot=1, status="completed",
        )
        update_task(
            supervisor._conn, tid,
            status="completed",
            started_at=(now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            completed_at=(now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            pr_url="https://github.com/org/repo/pull/1",
        )
        supervisor._conn.commit()

        claude_response = json.dumps({
            "result": json.dumps({
                "headline": "New feature added",
                "summary": "- Added a cool feature",
            }),
        })

        with patch("botfarm.supervisor_ops.datetime") as mock_dt, \
             patch("botfarm.supervisor_ops.subprocess") as mock_subprocess:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            mock_proc = MagicMock()
            mock_proc.returncode = 0
            mock_proc.stdout = claude_response
            mock_subprocess.run.return_value = mock_proc
            mock_subprocess.TimeoutExpired = TimeoutError

            supervisor._maybe_send_daily_summary()

        supervisor._notifier.notify_daily_summary.assert_called_once()
        headline = supervisor._notifier.notify_daily_summary.call_args[0][0]
        assert "New feature added" in headline

    def test_fallback_on_claude_failure(self, supervisor):
        now = datetime.now(timezone.utc)
        tid = insert_task(
            supervisor._conn, ticket_id="TST-F", title="Some task",
            project="test-project", slot=1, status="completed",
        )
        update_task(
            supervisor._conn, tid,
            status="completed",
            started_at=(now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            completed_at=(now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        supervisor._conn.commit()

        with patch("botfarm.supervisor_ops.datetime") as mock_dt, \
             patch("botfarm.supervisor_ops.subprocess") as mock_subprocess:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            mock_proc = MagicMock()
            mock_proc.returncode = 1
            mock_proc.stderr = "error"
            mock_subprocess.run.return_value = mock_proc
            mock_subprocess.TimeoutExpired = TimeoutError

            supervisor._maybe_send_daily_summary()

        supervisor._notifier.notify_daily_summary.assert_called_once()
        headline = supervisor._notifier.notify_daily_summary.call_args[0][0]
        assert headline == "Daily Work Summary"

    def test_min_tasks_threshold_skips(self, supervisor):
        supervisor._config.daily_summary.min_tasks_for_summary = 5

        with patch("botfarm.supervisor_ops.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 6, 12, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            supervisor._maybe_send_daily_summary()

        # Should skip — 0 tasks < min_tasks_for_summary=5
        supervisor._notifier.notify_daily_summary.assert_not_called()
        # But should still record event (to prevent retries)
        events = get_events(supervisor._conn, event_type="daily_summary_sent")
        assert len(events) == 1
        assert "below min_tasks" in events[0]["detail"]
