"""Tests for dashboard history, task detail, usage/metrics, config, pipeline state, supervisor badge pages."""

import json
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from botfarm.config import (
    BotfarmConfig,
    DashboardConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
)
from botfarm.dashboard import build_pipeline_state, create_app, start_dashboard
from botfarm.db import (
    init_db,
    insert_stage_run,
    insert_task,
    save_dispatch_state,
    update_task,
)
from tests.helpers import seed_slot as _seed_slot


class TestHistoryPage:
    def test_returns_200(self, client):
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_contains_task_data(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "TST-1" in body
        assert "my-project" in body
        assert "completed" in body
        assert "1.25" in body

    def test_contains_filter_form(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "history-filters" in body
        assert "Search" in body
        assert "Project" in body
        assert "Status" in body

    def test_contains_all_columns(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "Ticket" in body
        assert "Title" in body
        assert "Wall Time" in body
        assert "Reviews" in body
        assert "Limit Hits" in body
        assert "Cost" in body
        assert "Max Ctx Fill" in body

    def test_history_shows_cost_and_context_fill(self, client):
        """History rows should display aggregated cost and max context fill."""
        resp = client.get("/history")
        body = resp.text
        # TST-1: total cost = 0.1234 + 0.0456 = 0.1690
        assert "$0.1690" in body
        # TST-1: max context fill = 72.5%
        assert "72.5%" in body

    def test_project_dropdown_populated(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "my-project" in body
        assert "other-project" in body

    def test_task_rows_are_clickable(self, client):
        resp = client.get("/history")
        assert "/task/" in resp.text

    def test_task_rows_use_ticket_id_urls(self, client):
        resp = client.get("/history")
        assert "/task/TST-1" in resp.text
        assert "/task/TST-2" in resp.text

    def test_ticket_links_to_linear(self, db_file):
        app = create_app(
            db_path=db_file,
            workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/history")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_history_no_db(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "No tasks found" in resp.text


class TestHistoryFilters:
    def test_filter_by_project(self, client):
        resp = client.get("/history?project=my-project")
        body = resp.text
        assert "TST-1" in body
        assert "TST-2" not in body

    def test_filter_by_status(self, client):
        resp = client.get("/history?status=failed")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body

    def test_search_by_ticket_id(self, client):
        resp = client.get("/history?search=TST-2")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body

    def test_search_by_title(self, client):
        resp = client.get("/history?search=Add feature")
        body = resp.text
        assert "TST-2" in body

    def test_sort_by_turns_asc(self, client):
        resp = client.get("/history?sort_by=turns&sort_dir=ASC")
        assert resp.status_code == 200
        body = resp.text
        # TST-2 (10 turns) should appear before TST-1 (42 turns)
        assert body.index("TST-2") < body.index("TST-1")

    def test_sort_by_turns_desc(self, client):
        resp = client.get("/history?sort_by=turns&sort_dir=DESC")
        assert resp.status_code == 200
        body = resp.text
        assert body.index("TST-1") < body.index("TST-2")

    def test_invalid_sort_column_defaults_gracefully(self, client):
        resp = client.get("/history?sort_by=DROP TABLE&sort_dir=ASC")
        assert resp.status_code == 200

    def test_combined_filters(self, client):
        resp = client.get("/history?project=other-project&status=failed")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body


class TestHistoryPagination:
    def test_pagination_info_shown(self, client):
        resp = client.get("/history")
        body = resp.text
        assert "page 1 of 1" in body
        assert "2 tasks found" in body

    def test_page_param(self, client):
        resp = client.get("/history?page=1")
        assert resp.status_code == 200

    def test_invalid_page_defaults_to_1(self, client):
        resp = client.get("/history?page=abc")
        assert resp.status_code == 200

    def test_pagination_with_many_tasks(self, tmp_path):
        """When there are more than 25 tasks, pagination links appear."""
        path = tmp_path / "big.db"
        conn = init_db(path)
        for i in range(30):
            insert_task(
                conn,
                ticket_id=f"BIG-{i}",
                title=f"Task {i}",
                project="bulk",
                slot=1,
            )
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/history")
        body = resp.text
        assert "page 1 of 2" in body
        assert "Next" in body

        resp2 = client.get("/history?page=2")
        body2 = resp2.text
        assert "page 2 of 2" in body2
        assert "Prev" in body2

    def test_poll_url_includes_current_page(self, tmp_path):
        """The htmx polling URL on #history-panel must include the current page
        so that auto-refresh doesn't reset to page 1."""
        path = tmp_path / "poll.db"
        conn = init_db(path)
        for i in range(30):
            insert_task(
                conn,
                ticket_id=f"POLL-{i}",
                title=f"Task {i}",
                project="bulk",
                slot=1,
            )
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/history?page=2")
        body = resp.text
        assert 'hx-get="/partials/history?' in body
        assert "page=2" in body.split('id="history-panel"')[1].split(">")[0]


class TestPartialHistory:
    def test_returns_200(self, client):
        resp = client.get("/partials/history")
        assert resp.status_code == 200

    def test_contains_task_data(self, client):
        resp = client.get("/partials/history")
        assert "TST-1" in resp.text

    def test_partial_respects_filters(self, client):
        resp = client.get("/partials/history?project=other-project")
        body = resp.text
        assert "TST-2" in body
        assert "TST-1" not in body


class TestTaskDetailPage:
    def test_returns_200(self, client):
        resp = client.get("/task/1")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_contains_task_summary(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "TST-1" in body
        assert "Fix bug" in body
        assert "completed" in body
        assert "1.25" in body
        assert "my-project" in body

    def test_contains_metric_cards(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "Wall Time" in body
        assert "Turns" in body
        assert "Review Iterations" in body

    def test_contains_stage_runs(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "Stage Runs" in body
        assert "implement" in body
        assert "review" in body
        assert "sess-abc123d" in body  # truncated session id
        assert "approved" in body

    def test_contains_event_log(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "Event Log" in body
        assert "stage_started" in body
        assert "stage_completed" in body
        assert "pr_created" in body

    def test_back_link(self, client):
        resp = client.get("/task/1")
        assert "Back to Task History" in resp.text

    def test_ticket_links_to_linear(self, db_file):
        app = create_app(
            db_path=db_file,
            workspace="my-team",
        )
        client = TestClient(app)
        resp = client.get("/task/1")
        assert "linear.app/my-team/issue/TST-1" in resp.text

    def test_task_not_found(self, client):
        resp = client.get("/task/9999")
        assert resp.status_code == 200
        assert "Task not found" in resp.text

    def test_task_detail_no_db(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/task/1")
        assert resp.status_code == 200
        assert "Task not found" in resp.text

    def test_failed_task_shows_failure_reason(self, client):
        resp = client.get("/task/2")
        body = resp.text
        assert "TST-2" in body
        assert "Tests failed" in body

    def test_failed_task_shows_failure_category_badge(self, tmp_path):
        path = tmp_path / "cat.db"
        conn = init_db(path)
        tid = insert_task(conn, ticket_id="CAT-1", title="Env fail", project="p", slot=1, status="failed")
        update_task(conn, tid, failure_reason="ModuleNotFoundError: flask", failure_category="env_missing_package")
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/task/1")
        body = resp.text
        assert "Failure Category" in body
        assert "failure-cat-env_missing_package" in body
        assert "env missing package" in body

    def test_task_with_no_stages_or_events(self, tmp_path):
        path = tmp_path / "sparse.db"
        conn = init_db(path)
        insert_task(conn, ticket_id="BARE-1", title="Bare task", project="p", slot=1)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/task/1")
        body = resp.text
        assert "BARE-1" in body
        assert "No stage runs recorded" in body
        assert "No events recorded" in body

    def test_ticket_id_url_resolves(self, client):
        """GET /task/TST-1 resolves via ticket_id lookup."""
        resp = client.get("/task/TST-1")
        assert resp.status_code == 200
        body = resp.text
        assert "TST-1" in body
        assert "Fix bug" in body

    def test_integer_id_still_works(self, client):
        """GET /task/1 still resolves via integer ID (backward compat)."""
        resp = client.get("/task/1")
        assert resp.status_code == 200
        body = resp.text
        assert "TST-1" in body

    def test_nonexistent_ticket_id_shows_not_found(self, client):
        """GET /task/NOPE-999 shows not-found state gracefully."""
        resp = client.get("/task/NOPE-999")
        assert resp.status_code == 200
        assert "Task not found" in resp.text

    def test_second_task_by_ticket_id(self, client):
        """GET /task/TST-2 resolves the failed task."""
        resp = client.get("/task/TST-2")
        assert resp.status_code == 200
        body = resp.text
        assert "TST-2" in body
        assert "Add feature" in body

    def test_contains_token_usage_columns(self, client):
        """Stage runs table should show token usage and cost columns."""
        resp = client.get("/task/1")
        body = resp.text
        assert "Context Fill" in body
        assert "Cost" in body
        assert "Input Tokens" in body
        assert "Output Tokens" in body

    def test_stage_run_token_values(self, client):
        """Stage runs should display actual token/cost values from DB."""
        resp = client.get("/task/1")
        body = resp.text
        assert "50,000" in body  # input_tokens formatted
        assert "15,000" in body  # output_tokens formatted
        assert "$0.1234" in body  # cost
        assert "72.5%" in body  # context_fill_pct

    def test_context_fill_color_coding(self, client):
        """Context fill should be color-coded based on percentage."""
        resp = client.get("/task/1")
        body = resp.text
        # 72.5% should be yellow (50-75%)
        assert "ctx-fill-yellow" in body
        # 45.3% should be green (<50%)
        assert "ctx-fill-green" in body

    def test_task_totals_summary(self, client):
        """Task detail should show total cost and max context fill."""
        resp = client.get("/task/1")
        body = resp.text
        assert "Total Cost" in body
        assert "Max Context Fill" in body
        # Total cost = 0.1234 + 0.0456 = 0.1690
        assert "$0.1690" in body
        # Max context fill = 72.5%
        assert "72.5%" in body


# --- Usage Trends ---


class TestUsagePage:
    def test_returns_200(self, client):
        resp = client.get("/usage")
        assert resp.status_code == 200

    def test_contains_snapshot_data(self, client):
        resp = client.get("/usage")
        body = resp.text
        assert "45.0" in body
        assert "72.0" in body

    def test_contains_chart(self, client):
        resp = client.get("/usage")
        assert "usage-chart" in resp.text
        assert "chart.js" in resp.text.lower()

    def test_time_range_selector(self, client):
        resp = client.get("/usage")
        body = resp.text
        assert "Last 24h" in body
        assert "Last 7d" in body
        assert "Last 30d" in body

    def test_default_range_is_7d(self, client):
        resp = client.get("/usage")
        body = resp.text
        # 7d button should be active (aria-current)
        assert 'range=7d" role="button" aria-current="true"' in body

    def test_range_24h(self, client):
        resp = client.get("/usage?range=24h")
        assert resp.status_code == 200
        body = resp.text
        assert 'range=24h" role="button" aria-current="true"' in body

    def test_range_30d(self, client):
        resp = client.get("/usage?range=30d")
        assert resp.status_code == 200

    def test_invalid_range_defaults_to_7d(self, client):
        resp = client.get("/usage?range=invalid")
        assert resp.status_code == 200
        assert 'range=7d" role="button" aria-current="true"' in resp.text

    def test_raw_data_in_details(self, client):
        resp = client.get("/usage")
        assert "<details>" in resp.text
        assert "Raw snapshot data" in resp.text

    def test_chart_auto_scales_y_axis(self, client):
        """Y-axis should auto-scale based on data, not fixed at 100."""
        resp = client.get("/usage")
        body = resp.text
        # Should contain auto-scaling JS logic, not hardcoded max: 100
        assert "yMax" in body
        assert "dataMax" in body
        # Should NOT have hardcoded max: 100 on the y scale
        assert "max: 100," not in body

    def test_chart_includes_datalabels_plugin(self, client):
        """Chart.js datalabels plugin should be included for value labels."""
        resp = client.get("/usage")
        body = resp.text
        assert "chartjs-plugin-datalabels" in body

    def test_no_low_data_message_with_high_values(self, client):
        """No 'not enough data' message when utilization values are high."""
        resp = client.get("/usage")
        assert "Not enough data yet" not in resp.text


class TestUsageChartLowData:
    """Tests for usage chart with low utilization values (fresh instance)."""

    @pytest.fixture()
    def low_data_client(self, tmp_path):
        from botfarm.db import init_db, insert_usage_snapshot, save_dispatch_state
        from tests.helpers import seed_slot as _seed_slot

        path = tmp_path / "botfarm.db"
        conn = init_db(path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(conn, paused=False)
        # Insert a few snapshots with very low utilization (fresh instance)
        for i in range(3):
            insert_usage_snapshot(
                conn,
                utilization_5h=0.02 + i * 0.01,
                utilization_7d=0.01 + i * 0.005,
            )
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        return TestClient(app)

    def test_low_data_message_shown(self, low_data_client):
        """Fresh instances with low values and few snapshots show a help message."""
        resp = low_data_client.get("/usage")
        assert "Not enough data yet" in resp.text

    def test_chart_still_renders(self, low_data_client):
        """Chart should still render even with low data."""
        resp = low_data_client.get("/usage")
        assert "usage-chart" in resp.text
        assert "chart.js" in resp.text.lower()


# --- Metrics ---


class TestMetricsPage:
    def test_returns_200(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200

    def test_contains_aggregate_data(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Total Tasks" in body
        assert "Completed" in body
        assert "Failed" in body

    def test_contains_success_rate(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Success Rate" in body
        # 1 completed / 2 total = 50%
        assert "50.0%" in body

    def test_contains_averages(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Avg Wall Time" in body
        assert "Avg Turns / Task" in body
        assert "Avg Review Iterations" in body

    def test_contains_time_buckets(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Tasks Completed" in body
        assert "Today" in body
        assert "Last 7 Days" in body
        assert "Last 30 Days" in body

    def test_failure_reasons_displayed(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Common Failure Reasons" in body
        assert "Tests failed" in body

    def test_failure_categories_displayed(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "Failures by Category" in body
        # The test data has "Tests failed" without category → COALESCE gives "code_failure"
        assert "code failure" in body

    def test_failure_category_filter(self, tmp_path):
        path = tmp_path / "cat_metrics.db"
        conn = init_db(path)
        tid1 = insert_task(conn, ticket_id="M-1", title="t1", project="p", slot=1, status="failed")
        update_task(conn, tid1, failure_reason="ModuleNotFoundError", failure_category="env_missing_package")
        tid2 = insert_task(conn, ticket_id="M-2", title="t2", project="p", slot=1, status="failed")
        update_task(conn, tid2, failure_reason="tests failed", failure_category="code_failure")
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        # Filter by env_missing_package
        resp = client.get("/metrics?failure_category=env_missing_package")
        body = resp.text
        assert "ModuleNotFoundError" in body
        # code_failure reason should NOT appear (filtered out)
        assert "tests failed" not in body

    def test_project_filter(self, client):
        resp = client.get("/metrics?project=my-project")
        body = resp.text
        assert resp.status_code == 200
        # Only 1 task in my-project (TST-1, completed)
        assert "100.0%" in body  # 1/1 success rate

    def test_project_filter_dropdown(self, client):
        resp = client.get("/metrics")
        body = resp.text
        assert "my-project" in body
        assert "other-project" in body

    def test_metrics_no_db(self, tmp_path):
        app = create_app(
            db_path=tmp_path / "nonexistent.db",
        )
        client = TestClient(app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "Total Tasks" in resp.text

    def test_avg_wall_time_formatted(self, client):
        resp = client.get("/metrics")
        body = resp.text
        # TST-1 has 1.5h wall time => 1h30m
        assert "1h30m" in body

    def test_contains_token_usage_section(self, client):
        """Metrics page should show token usage & cost aggregates."""
        resp = client.get("/metrics")
        body = resp.text
        assert "Token Usage" in body
        assert "Total Input Tokens" in body
        assert "Total Output Tokens" in body
        assert "Total Cost" in body
        assert "Avg Context Fill" in body

    def test_token_aggregate_values(self, client):
        """Token totals should aggregate from all stage runs."""
        resp = client.get("/metrics")
        body = resp.text
        # 50000 + 20000 = 70000 total input tokens
        assert "70,000" in body
        # 15000 + 5000 = 20000 total output tokens
        assert "20,000" in body
        # 0.1234 + 0.0456 = 0.17 total cost
        assert "$0.17" in body


class TestCodexMetrics:
    """Tests for Codex review metrics on the metrics page and task detail."""

    @pytest.fixture()
    def codex_db(self, tmp_path):
        path = tmp_path / "botfarm_codex.db"
        conn = init_db(path)
        _seed_slot(conn, "proj", 1, status="free")
        save_dispatch_state(conn, paused=False)

        tid = insert_task(
            conn, ticket_id="CDX-1", title="Codex task",
            project="proj", slot=1, status="completed",
        )
        update_task(conn, tid,
                    started_at="2026-02-12T10:00:00+00:00",
                    completed_at="2026-02-12T11:00:00+00:00",
                    turns=20)
        insert_stage_run(conn, task_id=tid, stage="implement",
                         input_tokens=10000, output_tokens=2000,
                         total_cost_usd=0.10)
        insert_stage_run(conn, task_id=tid, stage="codex_review", iteration=1,
                         input_tokens=800_000, output_tokens=15_000,
                         cache_read_input_tokens=300_000,
                         duration_seconds=60.0, exit_subtype="changes_requested")
        insert_stage_run(conn, task_id=tid, stage="codex_review", iteration=2,
                         input_tokens=900_000, output_tokens=18_000,
                         cache_read_input_tokens=400_000,
                         duration_seconds=55.0, exit_subtype="approved")
        conn.commit()
        conn.close()
        return path

    @pytest.fixture()
    def codex_client(self, codex_db):
        app = create_app(db_path=codex_db)
        return TestClient(app)

    def test_metrics_page_shows_codex_section(self, codex_client):
        resp = codex_client.get("/metrics")
        body = resp.text
        assert resp.status_code == 200
        assert "Codex Review" in body
        assert "Codex Input Tokens" in body
        assert "Codex Output Tokens" in body
        assert "Codex Runs" in body
        assert "Approval Rate" in body
        assert "Error/Timeout Rate" in body

    def test_metrics_page_codex_token_values(self, codex_client):
        resp = codex_client.get("/metrics")
        body = resp.text
        # 800000 + 900000 = 1700000
        assert "1,700,000" in body
        # 15000 + 18000 = 33000
        assert "33,000" in body

    def test_metrics_page_codex_approval_rate(self, codex_client):
        resp = codex_client.get("/metrics")
        body = resp.text
        # 1 approved out of 2 = 50%
        assert "50.0%" in body

    def test_task_detail_shows_codex_section(self, codex_client):
        resp = codex_client.get("/task/CDX-1")
        body = resp.text
        assert resp.status_code == 200
        assert "Codex Review" in body
        assert "Codex Input Tokens" in body
        assert "Codex Output Tokens" in body

    def test_task_detail_codex_per_iteration(self, codex_client):
        resp = codex_client.get("/task/CDX-1")
        body = resp.text
        assert "Changes Requested" in body
        assert "Approved" in body
        assert "800,000" in body
        assert "900,000" in body

    def test_metrics_page_no_codex_section_without_data(self, client):
        resp = client.get("/metrics")
        body = resp.text
        # The main client has no codex runs
        assert "Codex Review" not in body


# --- start_dashboard ---


class TestStartDashboard:
    def test_disabled_returns_none(self, db_file):
        config = DashboardConfig(enabled=False)
        result = start_dashboard(
            config, db_path=db_file,
        )
        assert result is None

    def test_enabled_returns_thread(self, db_file):
        config = DashboardConfig(enabled=True, host="127.0.0.1", port=0)
        thread = start_dashboard(
            config, db_path=db_file,
        )
        assert thread is not None
        assert thread.is_alive()
        assert thread.daemon is True


# --- Config integration ---


class TestDashboardConfig:
    def test_default_values(self):
        cfg = DashboardConfig()
        assert cfg.enabled is False
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 8420

    def test_custom_values(self):
        cfg = DashboardConfig(enabled=True, host="127.0.0.1", port=9000)
        assert cfg.enabled is True
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 9000


# --- Edge cases ---


class TestEdgeCases:
    def test_empty_db_index(self, tmp_path):
        """With an empty DB (no slots, no state), index renders gracefully."""
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200

    def test_empty_database(self, tmp_path):
        db_path = tmp_path / "empty.db"
        conn = init_db(db_path)
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "No tasks found" in resp.text

    def test_usage_no_data(self, tmp_path):
        """With no usage snapshots in DB, usage partial renders without error."""
        db_path = tmp_path / "no_usage.db"
        conn = init_db(db_path)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/usage")
        assert resp.status_code == 200


# --- Config page ---


def _make_botfarm_config(tmp_path):
    """Create a BotfarmConfig with a real YAML source file for testing."""
    config_data = {
        "projects": [
            {
                "name": "test-project",
                "team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1],
            }
        ],
        "bugtracker": {
            "api_key": "${LINEAR_API_KEY}",
            "poll_interval_seconds": 120,
            "comment_on_failure": True,
            "comment_on_completion": False,
            "comment_on_limit_pause": False,
        },
        "usage_limits": {
            "pause_five_hour_threshold": 0.85,
            "pause_seven_day_threshold": 0.90,
        },
        "agents": {
            "max_review_iterations": 3,
            "max_ci_retries": 2,
            "timeout_minutes": {"implement": 120, "review": 30, "fix": 60},
            "timeout_grace_seconds": 10,
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_data, sort_keys=False))

    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project", team="TST",
                base_dir="~/test", worktree_prefix="test-slot-", slots=[1],
            ),
        ],
        bugtracker=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=120,
            comment_on_failure=True,
            comment_on_completion=False,
            comment_on_limit_pause=False,
        ),
    )
    config.source_path = str(config_path)
    return config, config_path


class TestConfigPage:
    @pytest.fixture()
    def config_client(self, db_file, tmp_path):
        config, _ = _make_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        return TestClient(app)

    def test_config_page_returns_200(self, config_client):
        resp = config_client.get("/config")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_config_page_contains_sections(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "Linear" in body
        assert "Usage Limits" in body
        assert "Agents" in body

    def test_config_page_contains_current_values(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "120" in body  # poll_interval_seconds
        assert "Save" in body

    def test_config_page_contains_nav_link(self, config_client):
        resp = config_client.get("/")
        assert "Config" in resp.text

    def test_config_page_disabled_without_config(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "not available" in resp.text


class TestConfigUpdate:
    @pytest.fixture()
    def setup(self, db_file, tmp_path):
        config, config_path = _make_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        return client, config, config_path

    def test_update_linear_setting(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "bugtracker": {"poll_interval_seconds": 60},
        })
        assert resp.status_code == 200
        assert "successfully" in resp.text
        assert config.bugtracker.poll_interval_seconds == 60

    def test_update_bool_setting(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "bugtracker": {"comment_on_completion": True},
        })
        assert resp.status_code == 200
        assert config.bugtracker.comment_on_completion is True

    def test_update_usage_limits(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "usage_limits": {"pause_five_hour_threshold": 0.75},
        })
        assert resp.status_code == 200
        assert config.usage_limits.pause_five_hour_threshold == 0.75

    def test_update_agents(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "agents": {"max_review_iterations": 5},
        })
        assert resp.status_code == 200
        assert config.agents.max_review_iterations == 5

    def test_update_timeout_minutes(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "agents": {"timeout_minutes": {"implement": 90}},
        })
        assert resp.status_code == 200
        assert config.agents.timeout_minutes["implement"] == 90
        # Other stages preserved
        assert config.agents.timeout_minutes["review"] == 30

    def test_update_writes_to_file(self, setup):
        client, _, config_path = setup
        client.post("/config", json={
            "bugtracker": {"poll_interval_seconds": 60},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["bugtracker"]["poll_interval_seconds"] == 60

    def test_update_preserves_env_vars_in_file(self, setup):
        client, _, config_path = setup
        client.post("/config", json={
            "bugtracker": {"poll_interval_seconds": 60},
        })
        data = yaml.safe_load(config_path.read_text())
        assert data["bugtracker"]["api_key"] == "${LINEAR_API_KEY}"

    def test_validation_error_returns_422(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "bugtracker": {"poll_interval_seconds": 0},
        })
        assert resp.status_code == 422
        assert "at least 1" in resp.text
        # Config unchanged
        assert config.bugtracker.poll_interval_seconds == 120

    def test_non_editable_field_rejected(self, setup):
        client, _, _ = setup
        resp = client.post("/config", json={
            "bugtracker": {"api_key": "hacked"},
        })
        assert resp.status_code == 422
        assert "not an editable field" in resp.text

    def test_invalid_json_returns_400(self, setup):
        client, _, _ = setup
        resp = client.post(
            "/config",
            content=b"not json",
            headers={"content-type": "application/json"},
        )
        assert resp.status_code == 400

    def test_non_dict_body_returns_400(self, setup):
        client, _, _ = setup
        resp = client.post("/config", json=["not", "a", "dict"])
        assert resp.status_code == 400
        assert "JSON object" in resp.text

    def test_update_without_config_returns_400(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.post("/config", json={"bugtracker": {"poll_interval_seconds": 60}})
        assert resp.status_code == 400
        assert "not available" in resp.text

    def test_multiple_sections_in_one_update(self, setup):
        client, config, _ = setup
        resp = client.post("/config", json={
            "bugtracker": {"poll_interval_seconds": 60},
            "agents": {"max_ci_retries": 5},
        })
        assert resp.status_code == 200
        assert config.bugtracker.poll_interval_seconds == 60
        assert config.agents.max_ci_retries == 5


# --- Structural config editing ---


def _make_structural_botfarm_config(tmp_path):
    """Create a BotfarmConfig with structural fields for testing."""
    config_data = {
        "projects": [
            {
                "name": "test-project",
                "team": "TST",
                "base_dir": "~/test",
                "worktree_prefix": "test-slot-",
                "slots": [1, 2],
                "tracker_project": "My Filter",
            },
        ],
        "bugtracker": {
            "api_key": "${LINEAR_API_KEY}",
            "poll_interval_seconds": 120,
        },
        "notifications": {
            "webhook_url": "https://hooks.example.com/old",
            "webhook_format": "slack",
            "rate_limit_seconds": 300,
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config_data, sort_keys=False))

    config = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project", team="TST",
                base_dir="~/test", worktree_prefix="test-slot-",
                slots=[1, 2], tracker_project="My Filter",
            ),
        ],
        bugtracker=LinearConfig(api_key="test-key", poll_interval_seconds=120),
        notifications=NotificationsConfig(
            webhook_url="https://hooks.example.com/old",
            webhook_format="slack",
            rate_limit_seconds=300,
        ),
    )
    config.source_path = str(config_path)
    return config, config_path


class TestStructuralConfigPage:
    @pytest.fixture()
    def structural_client(self, db_file, tmp_path):
        config, _ = _make_structural_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        return TestClient(app)

    def test_config_page_contains_projects_section(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        assert "Projects" in body
        assert "test-project" in body
        assert "restart" in body.lower()

    def test_config_page_contains_notifications_section(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        assert "Notifications" in body
        assert "Webhook URL" in body
        assert "webhook_format" in body.lower() or "Webhook format" in body

    def test_config_page_shows_project_slots(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        # Slot chips for slots [1, 2]
        assert 'data-slot="1"' in body
        assert 'data-slot="2"' in body

    def test_config_page_shows_linear_project_filter(self, structural_client):
        resp = structural_client.get("/config")
        body = resp.text
        assert "My Filter" in body

    def test_no_restart_banner_initially(self, structural_client):
        resp = structural_client.get("/config")
        assert 'id="restart-banner"' not in resp.text


class TestStructuralConfigUpdate:
    @pytest.fixture()
    def setup(self, db_file, tmp_path):
        config, config_path = _make_structural_botfarm_config(tmp_path)
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        return client, config, config_path, app

    def test_update_notifications_writes_yaml_only(self, setup):
        client, config, config_path, _ = setup
        resp = client.post("/config", json={
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        assert resp.status_code == 200
        assert "Restart required" in resp.text
        # YAML file updated
        data = yaml.safe_load(config_path.read_text())
        assert data["notifications"]["webhook_url"] == "https://new.example.com"
        # In-memory config NOT updated
        assert config.notifications.webhook_url == "https://hooks.example.com/old"

    def test_update_notifications_sets_restart_flag(self, setup):
        client, _, _, app = setup
        assert app.state.restart_required is False
        client.post("/config", json={
            "notifications": {"rate_limit_seconds": 60},
        })
        assert app.state.restart_required is True

    def test_restart_banner_shown_after_structural_update(self, setup):
        client, _, _, _ = setup
        client.post("/config", json={
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        resp = client.get("/config")
        assert 'id="restart-banner"' in resp.text
        assert "Restart required" in resp.text

    def test_update_project_slots_writes_yaml_only(self, setup):
        client, config, config_path, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "slots": [1, 2, 3]},
            ],
        })
        assert resp.status_code == 200
        # YAML file updated
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["slots"] == [1, 2, 3]
        # In-memory config NOT updated
        assert config.projects[0].slots == [1, 2]

    def test_update_project_tracker_project_writes_yaml_only(self, setup):
        client, config, config_path, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "tracker_project": "New Filter"},
            ],
        })
        assert resp.status_code == 200
        data = yaml.safe_load(config_path.read_text())
        assert data["projects"][0]["tracker_project"] == "New Filter"
        assert config.projects[0].tracker_project == "My Filter"

    def test_structural_validation_error_returns_422(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "slots": [1, 1]},
            ],
        })
        assert resp.status_code == 422
        assert "duplicate" in resp.text

    def test_structural_unknown_project_missing_fields(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "nonexistent", "slots": [1]},
            ],
        })
        assert resp.status_code == 422
        assert "missing required fields" in resp.text

    def test_structural_non_editable_project_field_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "projects": [
                {"name": "test-project", "base_dir": "/tmp/hacked"},
            ],
        })
        assert resp.status_code == 422
        assert "cannot edit" in resp.text

    def test_notifications_invalid_format_rejected(self, setup):
        client, _, _, _ = setup
        resp = client.post("/config", json={
            "notifications": {"webhook_format": "teams"},
        })
        assert resp.status_code == 422
        assert "must be one of" in resp.text

    def test_mixed_runtime_and_structural_update(self, setup):
        client, config, config_path, app = setup
        resp = client.post("/config", json={
            "bugtracker": {"poll_interval_seconds": 60},
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        assert resp.status_code == 200
        # Runtime applied in-memory
        assert config.bugtracker.poll_interval_seconds == 60
        # Structural written to file only
        data = yaml.safe_load(config_path.read_text())
        assert data["notifications"]["webhook_url"] == "https://new.example.com"
        assert config.notifications.webhook_url == "https://hooks.example.com/old"
        # Restart required because of structural update
        assert app.state.restart_required is True

    def test_structural_update_no_config_path(self, db_file):
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test", team="T", base_dir="~/t",
                    worktree_prefix="t-", slots=[1],
                ),
            ],
        )
        # No source_path set
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        resp = client.post("/config", json={
            "notifications": {"webhook_url": "https://new.example.com"},
        })
        assert resp.status_code == 400
        assert "config file path" in resp.text.lower() or "Cannot save" in resp.text


class TestConfigViewPage:
    @pytest.fixture()
    def config_client(self, db_file, tmp_path):
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="test-project", team="TST",
                    base_dir="~/test", worktree_prefix="test-slot-",
                    slots=[1, 2], tracker_project="My Project",
                ),
            ],
            bugtracker=LinearConfig(
                api_key="lin_api_1234567890abcdef",
                workspace="my-workspace",
                poll_interval_seconds=60,
                exclude_tags=["Human", "Manual"],
                comment_on_failure=True,
            ),
            notifications=NotificationsConfig(
                webhook_url="https://hooks.slack.com/services/T00/B00/xxx",
                webhook_format="slack",
                rate_limit_seconds=300,
            ),
        )
        config.source_path = str(tmp_path / "config.yaml")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        return TestClient(app)

    def test_config_view_returns_200(self, config_client):
        resp = config_client.get("/config")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_config_page_has_view_and_edit_tabs(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "tab-view" in body
        assert "tab-edit" in body
        assert "switchTab" in body

    def test_config_view_contains_all_sections(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        for section in [
            "Projects", "Linear", "Agents", "Usage Limits",
            "Notifications", "Dashboard", "Database",
        ]:
            assert section in body

    def test_config_view_shows_project_details(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "test-project" in body
        assert "TST" in body
        assert "~/test" in body
        assert "test-slot-" in body
        assert "My Project" in body

    def test_config_view_masks_api_key(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        # View tab should show masked key (first 4 + **** + last 4)
        assert "lin_****cdef" in body
        # Full key must not appear (API key is not editable, so not in edit tab either)
        assert "lin_api_1234567890abcdef" not in body

    def test_config_view_masks_webhook_url(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        # View tab should show masked version
        assert "http****/xxx" in body

    def test_config_view_shows_linear_settings(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "my-workspace" in body
        assert "60" in body  # poll_interval_seconds
        assert "Human" in body
        assert "Manual" in body

    def test_config_view_shows_boolean_values(self, config_client):
        resp = config_client.get("/config")
        body = resp.text
        assert "Yes" in body  # comment_on_failure = True
        assert "No" in body   # comment_on_completion = False

    def test_config_view_nav_link(self, config_client):
        resp = config_client.get("/")
        assert "Configuration" in resp.text
        assert "/config" in resp.text

    def test_config_view_disabled_without_config(self, db_file):
        app = create_app(db_path=db_file)
        client = TestClient(app)
        resp = client.get("/config")
        assert resp.status_code == 200
        assert "not available" in resp.text

    def test_config_view_masks_short_api_key(self, tmp_path):
        # Use a db in a separate temp dir so the db path displayed on
        # the config page doesn't contain the test name substring.
        import tempfile
        with tempfile.TemporaryDirectory(prefix="botfarm_mask_") as td:
            db = Path(td) / "botfarm.db"
            init_db(db)
            config = BotfarmConfig(
                projects=[
                    ProjectConfig(
                        name="p", team="T",
                        base_dir="~/p", worktree_prefix="p-", slots=[1],
                    ),
                ],
                bugtracker=LinearConfig(api_key="short"),
            )
            config.source_path = str(tmp_path / "config.yaml")
            app = create_app(
                db_path=db,
                botfarm_config=config,
            )
            client = TestClient(app)
            resp = client.get("/config")
            body = resp.text
            assert "short" not in body
            assert "****" in body

    def test_config_view_empty_webhook_shows_dash(self, db_file, tmp_path):
        config = BotfarmConfig(
            projects=[
                ProjectConfig(
                    name="p", team="T",
                    base_dir="~/p", worktree_prefix="p-", slots=[1],
                ),
            ],
        )
        config.source_path = str(tmp_path / "config.yaml")
        app = create_app(
            db_path=db_file,
            botfarm_config=config,
        )
        client = TestClient(app)
        resp = client.get("/config")
        body = resp.text
        # Empty webhook_url should show "-"
        assert "Webhook URL" in body


# --- Pipeline stepper ---


class TestBuildPipelineState:
    def test_no_stage_runs(self):
        result = build_pipeline_state([], None)
        assert len(result) == 5
        assert all(s["status"] == "pending" for s in result)
        assert all(s["iteration_count"] == 0 for s in result)

    def test_completed_task_all_stages(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "fix", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": "passed", "was_limit_restart": 0},
            {"stage": "merge", "exit_subtype": "merged", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        assert all(s["status"] == "completed" for s in result)

    def test_failed_at_review(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "rejected", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "failed")
        assert result[0]["status"] == "completed"  # implement
        assert result[1]["status"] == "failed"  # review
        assert result[2]["status"] == "pending"  # fix
        assert result[3]["status"] == "pending"  # pr_checks
        assert result[4]["status"] == "pending"  # merge

    def test_active_in_progress(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        assert result[0]["status"] == "completed"  # implement
        assert result[1]["status"] == "active"  # review
        assert result[2]["status"] == "pending"  # fix

    def test_iteration_count(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "changes_requested", "was_limit_restart": 0},
            {"stage": "fix", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "fix", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        assert result[0]["iteration_count"] == 1  # implement
        assert result[1]["iteration_count"] == 2  # review
        assert result[2]["iteration_count"] == 2  # fix
        assert result[3]["iteration_count"] == 1  # pr_checks

    def test_limit_restart_flag(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 1},
            {"stage": "review", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        assert result[0]["has_limit_restart"] is True
        assert result[1]["has_limit_restart"] is False

    def test_skipped_stage_shows_completed(self):
        """fix is skipped when review approves on first try."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": "passed", "was_limit_restart": 0},
            {"stage": "merge", "exit_subtype": "merged", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        assert result[2]["name"] == "fix"
        assert result[2]["status"] == "completed"  # skipped but should show completed
        assert result[2]["iteration_count"] == 0

    def test_stage_names_match_canonical_order(self):
        result = build_pipeline_state([], None)
        names = [s["name"] for s in result]
        assert names == ["implement", "review", "fix", "pr_checks", "merge"]

    def test_codex_review_not_a_pipeline_stage(self):
        """codex_review runs should NOT create a separate pipeline stage."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        names = [s["name"] for s in result]
        assert "codex_review" not in names
        assert len(result) == 5  # only canonical stages

    def test_codex_review_attached_to_review_stage(self):
        """codex_review info should be attached to the review stage entry."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "pr_checks", "exit_subtype": None, "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        review = next(s for s in result if s["name"] == "review")
        assert "codex_review" in review
        assert review["codex_review"]["status"] == "APPROVED"
        assert review["codex_review"]["count"] == 1

    def test_codex_review_changes_requested(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "changes_requested", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        review = next(s for s in result if s["name"] == "review")
        assert review["codex_review"]["status"] == "CHANGES_REQUESTED"

    def test_codex_review_skipped(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "skipped", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        review = next(s for s in result if s["name"] == "review")
        assert review["codex_review"]["status"] == "Skipped"

    def test_codex_review_failed(self):
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
            {"stage": "codex_review", "exit_subtype": "failed", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "in_progress")
        review = next(s for s in result if s["name"] == "review")
        assert review["codex_review"]["status"] == "Failed"

    def test_no_codex_review_no_key(self):
        """When there are no codex_review runs, review stage has no codex_review key."""
        runs = [
            {"stage": "implement", "exit_subtype": "done", "was_limit_restart": 0},
            {"stage": "review", "exit_subtype": "approved", "was_limit_restart": 0},
        ]
        result = build_pipeline_state(runs, "completed")
        review = next(s for s in result if s["name"] == "review")
        assert "codex_review" not in review


class TestTaskDetailPipeline:
    def test_stepper_renders_on_task_page(self, client):
        resp = client.get("/task/1")
        assert resp.status_code == 200
        body = resp.text
        assert "pipeline-stepper" in body
        assert "pipeline-node" in body

    def test_stepper_shows_stage_labels(self, client):
        resp = client.get("/task/1")
        body = resp.text
        assert "implement" in body
        assert "review" in body
        assert "pr checks" in body
        assert "merge" in body

    def test_stepper_not_shown_for_missing_task(self, client):
        resp = client.get("/task/9999")
        assert resp.status_code == 200
        body = resp.text
        # The CSS class exists in <style>, but the actual stepper div should not render
        assert 'class="pipeline-stepper"' not in body


# --- Supervisor status badge ---


class TestSupervisorBadge:
    def test_index_shows_stopped_when_no_heartbeat(self, tmp_path):
        """Without supervisor_heartbeat, badge should show Stopped."""
        db_path = tmp_path / "no_hb.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(conn, paused=False)
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Stopped" in resp.text

    def test_index_shows_running_with_fresh_heartbeat(self, tmp_path):
        """A recent heartbeat should show Supervisor Running."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "fresh_hb.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_index_shows_stopped_with_stale_heartbeat(self, tmp_path):
        """A heartbeat older than poll_interval + grace should show Stopped.

        The default poll_interval is 120s and grace is 60s, so a heartbeat
        older than 180s should be considered stale.
        """
        from datetime import datetime, timedelta, timezone
        stale = datetime.now(timezone.utc) - timedelta(seconds=200)
        db_path = tmp_path / "stale_hb.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False,
            supervisor_heartbeat=stale.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Stopped" in resp.text

    def test_index_shows_running_during_poll_sleep(self, tmp_path):
        """A heartbeat within the poll interval should show Running.

        With default poll_interval=120s, a heartbeat 60s old is well
        within the expected range and must NOT show Stopped.
        """
        from datetime import datetime, timedelta, timezone
        recent = datetime.now(timezone.utc) - timedelta(seconds=60)
        db_path = tmp_path / "poll_sleep.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False,
            supervisor_heartbeat=recent.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_supervisor_badge_partial_endpoint(self, tmp_path):
        """The /partials/supervisor-badge endpoint returns the badge."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "badge_partial.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/supervisor-badge")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_slots_partial_no_duplicate_badge(self, tmp_path):
        """The /partials/slots response should NOT contain the supervisor badge."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "no_dup_badge.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/partials/slots")
        assert resp.status_code == 200
        assert 'hx-swap-oob' not in resp.text

    def test_badge_on_history_page(self, tmp_path):
        """History page should also show the supervisor badge."""
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        db_path = tmp_path / "hist_badge.db"
        conn = init_db(db_path)
        _seed_slot(conn, "my-project", 1, status="free")
        save_dispatch_state(
            conn, paused=False, supervisor_heartbeat=now_iso,
        )
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/history")
        assert resp.status_code == 200
        assert "Supervisor Running" in resp.text

    def test_backward_compat_no_heartbeat_field(self, tmp_path):
        """Dashboard should not crash when no dispatch_state row exists."""
        db_path = tmp_path / "compat.db"
        conn = init_db(db_path)
        _seed_slot(conn, "p", 1, status="free")
        # No save_dispatch_state call -- no heartbeat row
        conn.commit()
        conn.close()
        app = create_app(db_path=db_path)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Supervisor Stopped" in resp.text


# --- Relative timestamps (timeago) ---


class TestRelativeTimestamps:
    """Verify that timestamps use <time data-timestamp=...> elements for JS-based
    relative time display, with raw ISO fallback text and title attributes."""

    def test_timeago_js_in_base(self, client):
        """The timeago() function and updateTimeagos() must be in the base template."""
        resp = client.get("/")
        body = resp.text
        assert "function timeago(" in body
        assert "function updateTimeagos(" in body
        assert "data-timestamp" in body or "updateTimeagos" in body

    def test_task_detail_timestamps_have_data_attr(self, client):
        """Task detail page timestamps should use <time data-timestamp=...>."""
        resp = client.get("/task/1")
        body = resp.text
        # created_at, started_at, completed_at should have data-timestamp
        assert 'data-timestamp="2026-02-12T10:00:00+00:00"' in body  # started_at
        assert 'data-timestamp="2026-02-12T11:30:00+00:00"' in body  # completed_at

    def test_task_detail_timestamps_have_title(self, client):
        """Task detail timestamps should have title attributes with full datetime."""
        resp = client.get("/task/1")
        body = resp.text
        assert 'title="2026-02-12T10:00:00"' in body
        assert 'title="2026-02-12T11:30:00"' in body

    def test_task_detail_event_log_timestamps(self, client):
        """Event log timestamps should use <time data-timestamp=...>."""
        resp = client.get("/task/1")
        body = resp.text
        # Event log entries should have data-timestamp attributes
        assert body.count("data-timestamp") >= 3  # at least task info + events

    def test_history_timestamps_have_data_attr(self, client):
        """History table started_at/completed_at should use <time data-timestamp=...>."""
        resp = client.get("/partials/history")
        body = resp.text
        assert "data-timestamp" in body
        # started_at and completed_at for TST-1
        assert 'data-timestamp="2026-02-12T10:00:00+00:00"' in body
        assert 'data-timestamp="2026-02-12T11:30:00+00:00"' in body

    def test_history_timestamps_have_title(self, client):
        """History timestamps should have title attributes."""
        resp = client.get("/partials/history")
        body = resp.text
        assert 'title="2026-02-12T10:00:00"' in body

    def test_usage_snapshot_timestamps(self, client):
        """Usage page snapshot timestamps should use <time data-timestamp=...>."""
        resp = client.get("/usage")
        body = resp.text
        assert "data-timestamp" in body

    def test_usage_resets_at_has_data_attr(self, client):
        """Usage page resets_at should use <time data-timestamp=...>."""
        resp = client.get("/usage")
        body = resp.text
        assert 'data-timestamp="2026-02-12T15:00:00+00:00"' in body

    def test_null_timestamps_show_dash(self, tmp_path):
        """Null timestamps should show '-' without a <time> wrapper."""
        path = tmp_path / "null_ts.db"
        conn = init_db(path)
        insert_task(conn, ticket_id="NULL-1", title="No dates", project="p", slot=1)
        conn.commit()
        conn.close()
        app = create_app(db_path=path)
        client = TestClient(app)
        resp = client.get("/task/1")
        body = resp.text
        assert "NULL-1" in body
        # Null started_at and completed_at should show "-" not wrapped in <time>
        assert "<td>-</td>" in body

    def test_timeago_updates_on_htmx_swap(self, client):
        """The updateTimeagos function should be registered for htmx:afterSwap."""
        resp = client.get("/")
        body = resp.text
        assert 'addEventListener("htmx:afterSwap", updateTimeagos)' in body

    def test_timeago_periodic_refresh(self, client):
        """updateTimeagos should run on a 60-second interval."""
        resp = client.get("/")
        body = resp.text
        assert "setInterval(updateTimeagos, 60000)" in body


# ---------------------------------------------------------------------------
# Manual pause / resume API
# ---------------------------------------------------------------------------


