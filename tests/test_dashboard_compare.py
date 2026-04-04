"""Tests for the A/B comparison dashboard page and export endpoints."""

import json

import pytest
from fastapi.testclient import TestClient

from botfarm.dashboard import create_app
from botfarm.db import (
    get_comparable_tickets,
    get_recent_tasks_for_picker,
    get_tasks_for_comparison,
    init_db,
    insert_redispatch_task,
    insert_stage_run,
    insert_task,
    update_task,
)


@pytest.fixture()
def compare_db(tmp_path):
    """Create a database with two tasks for the same ticket (A/B comparison)."""
    path = tmp_path / "botfarm.db"
    conn = init_db(path, allow_migration=True)

    # First run (pipeline 1)
    t1 = insert_task(
        conn,
        ticket_id="CMP-1",
        title="Comparison task",
        project="proj-a",
        slot=1,
        pipeline_id=1,
    )
    update_task(
        conn,
        t1,
        started_at="2026-03-01T10:00:00+00:00",
        completed_at="2026-03-01T11:00:00+00:00",
        status="completed",
    )
    insert_stage_run(
        conn,
        task_id=t1,
        stage="implement",
        iteration=1,
        turns=20,
        duration_seconds=2400.0,
        input_tokens=40000,
        output_tokens=10000,
        total_cost_usd=0.0800,
        context_fill_pct=60.0,
    )
    insert_stage_run(
        conn,
        task_id=t1,
        stage="review",
        iteration=1,
        turns=5,
        duration_seconds=600.0,
        exit_subtype="approved",
        input_tokens=10000,
        output_tokens=3000,
        total_cost_usd=0.0200,
        context_fill_pct=35.0,
    )

    # Second run (pipeline 2) via redispatch
    t2 = insert_redispatch_task(
        conn,
        ticket_id="CMP-1",
        title="Comparison task",
        project="proj-a",
        slot=2,
        pipeline_id=2,
    )
    update_task(
        conn,
        t2,
        started_at="2026-03-01T12:00:00+00:00",
        completed_at="2026-03-01T13:30:00+00:00",
        status="completed",
    )
    insert_stage_run(
        conn,
        task_id=t2,
        stage="implement",
        iteration=1,
        turns=30,
        duration_seconds=4200.0,
        input_tokens=60000,
        output_tokens=18000,
        total_cost_usd=0.1500,
        context_fill_pct=80.0,
    )
    insert_stage_run(
        conn,
        task_id=t2,
        stage="review",
        iteration=1,
        turns=8,
        duration_seconds=900.0,
        exit_subtype="changes_requested",
        input_tokens=15000,
        output_tokens=5000,
        total_cost_usd=0.0400,
        context_fill_pct=50.0,
    )
    insert_stage_run(
        conn,
        task_id=t2,
        stage="fix",
        iteration=1,
        turns=10,
        duration_seconds=1200.0,
        input_tokens=20000,
        output_tokens=8000,
        total_cost_usd=0.0500,
        context_fill_pct=65.0,
    )

    # A separate standalone task (no re-dispatch)
    t3 = insert_task(
        conn,
        ticket_id="CMP-2",
        title="Solo task",
        project="proj-a",
        slot=1,
    )
    insert_stage_run(
        conn,
        task_id=t3,
        stage="implement",
        iteration=1,
        turns=10,
        duration_seconds=1800.0,
        input_tokens=20000,
        output_tokens=5000,
        total_cost_usd=0.0300,
    )

    conn.commit()
    conn.close()
    return path


@pytest.fixture()
def compare_client(compare_db):
    app = create_app(db_path=compare_db, workspace="test-ws")
    return TestClient(app)


class TestComparePageBasic:
    def test_compare_returns_200(self, compare_client):
        resp = compare_client.get("/compare")
        assert resp.status_code == 200
        assert "text/html" in resp.headers["content-type"]

    def test_compare_shows_comparable_tickets(self, compare_client):
        resp = compare_client.get("/compare")
        body = resp.text
        assert "CMP-1" in body
        assert "Compare by Ticket" in body

    def test_compare_shows_task_picker(self, compare_client):
        resp = compare_client.get("/compare")
        body = resp.text
        assert "Compare by Task" in body
        assert "CMP-1" in body
        assert "CMP-2" in body

    def test_compare_no_db(self, tmp_path):
        app = create_app(db_path=tmp_path / "nonexistent.db")
        client = TestClient(app)
        resp = client.get("/compare")
        assert resp.status_code == 200


class TestCompareByTicket:
    def test_compare_by_ticket(self, compare_client):
        resp = compare_client.get("/compare?ticket=CMP-1")
        body = resp.text
        assert resp.status_code == 200
        assert "Summary" in body
        assert "CMP-1" in body
        # Should show cost data
        assert "$" in body

    def test_compare_shows_stage_breakdown(self, compare_client):
        resp = compare_client.get("/compare?ticket=CMP-1")
        body = resp.text
        assert "Stage Breakdown" in body
        assert "implement" in body
        assert "review" in body

    def test_compare_shows_color_coding(self, compare_client):
        """The cheaper/faster run should have 'compare-better' class."""
        resp = compare_client.get("/compare?ticket=CMP-1")
        body = resp.text
        assert "compare-better" in body
        assert "compare-worse" in body


class TestCompareByTaskIds:
    def test_compare_by_task_ids(self, compare_client, compare_db):
        conn = init_db(compare_db)
        try:
            tasks = conn.execute("SELECT id FROM tasks ORDER BY id").fetchall()
            ids = [str(t[0]) for t in tasks[:2]]
        finally:
            conn.close()
        resp = compare_client.get(f"/compare?tasks={','.join(ids)}")
        assert resp.status_code == 200
        assert "Summary" in resp.text

    def test_compare_invalid_task_ids_graceful(self, compare_client):
        resp = compare_client.get("/compare?tasks=abc,xyz")
        assert resp.status_code == 200

    def test_compare_single_task_no_comparison(self, compare_client):
        resp = compare_client.get("/compare?tasks=1")
        assert resp.status_code == 200


class TestCompareExport:
    def test_export_json(self, compare_client):
        resp = compare_client.get("/compare/export?ticket=CMP-1&format=json")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        assert data[0]["ticket_id"] == "CMP-1"
        assert "total_cost_usd" in data[0]
        assert "duration_seconds" in data[0]

    def test_export_csv(self, compare_client):
        resp = compare_client.get("/compare/export?ticket=CMP-1&format=csv")
        assert resp.status_code == 200
        assert "text/csv" in resp.headers["content-type"]
        lines = resp.text.strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows
        assert "ticket_id" in lines[0]

    def test_export_csv_unequal_stage_counts(self, compare_client):
        # Task 3 has 1 stage, task 2 has 3 — first row has fewer columns
        resp = compare_client.get("/compare/export?tasks=3,2&format=csv")
        assert resp.status_code == 200
        lines = resp.text.strip().split("\n")
        assert len(lines) == 3  # header + 2 data rows
        header = lines[0]
        # Header should contain stage columns from the task with more stages
        assert "stage_2_name" in header

    def test_export_json_default_format(self, compare_client):
        resp = compare_client.get("/compare/export?ticket=CMP-1")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)

    def test_export_empty(self, compare_client):
        resp = compare_client.get("/compare/export?ticket=NONEXISTENT&format=json")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_export_no_db(self, tmp_path):
        app = create_app(db_path=tmp_path / "nonexistent.db")
        client = TestClient(app)
        resp = client.get("/compare/export?ticket=CMP-1&format=json")
        assert resp.status_code == 503

    def test_export_includes_stage_detail(self, compare_client):
        resp = compare_client.get("/compare/export?ticket=CMP-1&format=json")
        data = resp.json()
        # First run has 2 stages, second has 3
        assert "stage_0_name" in data[0]
        assert data[0]["stage_0_name"] == "implement"


class TestDbComparisonQueries:
    def test_get_comparable_tickets(self, compare_db):
        conn = init_db(compare_db)
        try:
            tickets = get_comparable_tickets(conn)
            assert len(tickets) == 1
            assert tickets[0]["ticket_id"] == "CMP-1"
            assert tickets[0]["run_count"] == 2
        finally:
            conn.close()

    def test_get_tasks_for_comparison(self, compare_db):
        conn = init_db(compare_db)
        try:
            all_tasks = conn.execute("SELECT id FROM tasks ORDER BY id").fetchall()
            ids = [t[0] for t in all_tasks[:2]]
            tasks = get_tasks_for_comparison(conn, ids)
            assert len(tasks) == 2
            # Order preserved
            assert tasks[0]["id"] == ids[0]
            assert tasks[1]["id"] == ids[1]
        finally:
            conn.close()

    def test_get_tasks_for_comparison_empty(self, compare_db):
        conn = init_db(compare_db)
        try:
            tasks = get_tasks_for_comparison(conn, [])
            assert tasks == []
        finally:
            conn.close()

    def test_get_recent_tasks_for_picker(self, compare_db):
        conn = init_db(compare_db)
        try:
            tasks = get_recent_tasks_for_picker(conn)
            assert len(tasks) == 3  # CMP-1 run1, CMP-1 run2, CMP-2
        finally:
            conn.close()


class TestCompareNavLink:
    def test_nav_contains_compare_link(self, compare_client):
        resp = compare_client.get("/compare")
        assert '/compare"' in resp.text or "/compare'" in resp.text
        assert "Compare" in resp.text
