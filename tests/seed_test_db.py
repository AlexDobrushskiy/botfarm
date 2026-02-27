"""Comprehensive DB seed script for dashboard testing.

Creates deterministic test data covering all dashboard UI states.
Importable by pytest fixtures AND runnable as a standalone script.

Usage:
    # As a module
    from tests.seed_test_db import seed_comprehensive_db
    seed_comprehensive_db(db_path)

    # As a standalone script
    python -m tests.seed_test_db [db_path]
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

from botfarm.db import (
    init_db,
    insert_event,
    insert_stage_run,
    insert_task,
    insert_usage_snapshot,
    save_dispatch_state,
    save_queue_entries,
    update_task,
    upsert_slot,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECTS = ["bot-farm", "web-app", "data-pipeline"]

SLOT_STATUSES = [
    "free", "busy", "failed", "paused_manual", "paused_limit",
    "completed_pending_cleanup",
]

TASK_STATUSES = ["completed", "failed", "pending"]

STAGES = ["implement", "review", "fix", "pr_checks", "merge"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _upsert_slot(conn, project, slot_id, status="free", **overrides):
    """Seed a slot row with sensible defaults."""
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
        "ticket_labels": [],
    }
    slot.update(overrides)
    upsert_slot(conn, slot)


# ---------------------------------------------------------------------------
# Seed function
# ---------------------------------------------------------------------------


def seed_comprehensive_db(db_path: Path) -> None:
    """Seed a SQLite database with comprehensive test data.

    Uses ``init_db`` to create the schema, then populates every table
    with deterministic data covering all dashboard UI states.
    """
    conn = init_db(db_path, allow_migration=True)

    # Clean tables that use plain INSERTs (no ON CONFLICT) so re-seeding
    # the same database is idempotent.  Tables with upsert semantics
    # (slots, tasks, dispatch_state, queue_entries) are safe already.
    conn.execute("DELETE FROM stage_runs")
    conn.execute("DELETE FROM task_events")
    conn.execute("DELETE FROM usage_snapshots")

    _seed_slots(conn)
    _seed_tasks_and_stages(conn)
    _seed_usage_snapshots(conn)
    _seed_queue_entries(conn)
    _seed_dispatch_state(conn)

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Slots — all statuses across multiple projects
# ---------------------------------------------------------------------------


def _seed_slots(conn):
    # bot-farm: 3 slots — busy, free, failed
    _upsert_slot(
        conn, "bot-farm", 1, status="busy",
        ticket_id="SMA-101", ticket_title="Add retry logic",
        branch="sma-101-add-retry-logic",
        stage="implement", stage_iteration=1,
        current_session_id="sess-bf-101",
        started_at="2026-02-27T08:00:00Z",
        stage_started_at="2026-02-27T08:00:00Z",
        pid=10001,
        stages_completed=[],
        ticket_labels=["enhancement", "priority"],
    )
    _upsert_slot(conn, "bot-farm", 2, status="free")
    _upsert_slot(
        conn, "bot-farm", 3, status="failed",
        ticket_id="SMA-99", ticket_title="Fix flaky test",
        branch="sma-99-fix-flaky-test",
        started_at="2026-02-27T06:00:00Z",
        stages_completed=["implement", "review"],
        ticket_labels=["bug"],
    )

    # web-app: 2 slots — busy (mid-fix after first review), paused_manual
    _upsert_slot(
        conn, "web-app", 1, status="busy",
        ticket_id="WEB-50", ticket_title="Dark mode toggle",
        branch="web-50-dark-mode-toggle",
        stage="fix", stage_iteration=1,
        current_session_id="sess-wa-50",
        started_at="2026-02-27T07:30:00Z",
        stage_started_at="2026-02-27T09:15:00Z",
        pid=10002,
        stages_completed=["implement"],
        ticket_labels=["feature"],
    )
    _upsert_slot(
        conn, "web-app", 2, status="paused_manual",
        ticket_id="WEB-48", ticket_title="Migrate to React 19",
        branch="web-48-migrate-react-19",
        stage="fix", stage_iteration=1,
        started_at="2026-02-27T05:00:00Z",
        stage_started_at="2026-02-27T08:45:00Z",
        stages_completed=["implement", "review"],
        interrupted_by_limit=True,
        resume_after="limit_reset",
        ticket_labels=["migration"],
    )

    # data-pipeline: 3 slots — paused_limit, completed_pending_cleanup, free
    _upsert_slot(
        conn, "data-pipeline", 1, status="paused_limit",
        ticket_id="DP-10", ticket_title="Add data validation step",
        branch="dp-10-add-data-validation-step",
        stage="implement", stage_iteration=1,
        started_at="2026-02-27T07:00:00Z",
        stage_started_at="2026-02-27T07:00:00Z",
        stages_completed=[],
        interrupted_by_limit=True,
        resume_after="2026-02-27T12:00:00Z",
        ticket_labels=["enhancement"],
    )
    _upsert_slot(
        conn, "data-pipeline", 2, status="completed_pending_cleanup",
        ticket_id="DP-11", ticket_title="Fix CSV parser edge case",
        branch="dp-11-fix-csv-parser-edge-case",
        pr_url="https://github.com/org/data-pipeline/pull/11",
        stage="merge", stage_iteration=1,
        started_at="2026-02-27T02:00:00Z",
        stages_completed=["implement", "review", "pr_checks", "merge"],
        ticket_labels=["bug"],
    )
    _upsert_slot(conn, "data-pipeline", 3, status="free")

    conn.commit()


# ---------------------------------------------------------------------------
# Tasks + stage runs + task events — varied statuses, turns, iterations
# ---------------------------------------------------------------------------


def _seed_tasks_and_stages(conn):
    # ---- Task 1: Completed successfully, full pipeline ----
    t1 = insert_task(
        conn, ticket_id="SMA-80", title="Add config validation",
        project="bot-farm", slot=1, status="completed",
    )
    update_task(
        conn, t1,
        started_at="2026-02-26T10:00:00Z",
        completed_at="2026-02-26T12:30:00Z",
        turns=65, review_iterations=2,
        pr_url="https://github.com/org/bot-farm/pull/80",
        pipeline_stage="merge", review_state="approved",
    )
    insert_stage_run(
        conn, task_id=t1, stage="implement", iteration=1,
        session_id="sess-t1-impl", turns=35,
        duration_seconds=3600.0, exit_subtype="completed",
        input_tokens=120000, output_tokens=40000,
        cache_read_input_tokens=50000, cache_creation_input_tokens=10000,
        total_cost_usd=0.28, context_fill_pct=68.5,
    )
    insert_stage_run(
        conn, task_id=t1, stage="review", iteration=1,
        session_id="sess-t1-rev1", turns=10,
        duration_seconds=900.0, exit_subtype="needs_changes",
        input_tokens=30000, output_tokens=8000,
        total_cost_usd=0.06, context_fill_pct=42.0,
    )
    insert_stage_run(
        conn, task_id=t1, stage="fix", iteration=1,
        session_id="sess-t1-fix1", turns=12,
        duration_seconds=1200.0, exit_subtype="completed",
        input_tokens=45000, output_tokens=15000,
        total_cost_usd=0.10, context_fill_pct=55.2,
    )
    insert_stage_run(
        conn, task_id=t1, stage="review", iteration=2,
        session_id="sess-t1-rev2", turns=5,
        duration_seconds=600.0, exit_subtype="approved",
        input_tokens=20000, output_tokens=3000,
        total_cost_usd=0.03, context_fill_pct=35.0,
    )
    insert_stage_run(
        conn, task_id=t1, stage="pr_checks", iteration=1,
        session_id="sess-t1-pr", turns=2,
        duration_seconds=300.0, exit_subtype="passed",
        input_tokens=5000, output_tokens=1000,
        total_cost_usd=0.01, context_fill_pct=15.0,
    )
    insert_stage_run(
        conn, task_id=t1, stage="merge", iteration=1,
        session_id="sess-t1-merge", turns=1,
        duration_seconds=60.0, exit_subtype="merged",
        input_tokens=2000, output_tokens=500,
        total_cost_usd=0.005, context_fill_pct=10.0,
    )
    # Full audit trail for task 1
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 1"),
        ("stage_started", "implement"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — needs_changes"),
        ("stage_started", "fix"),
        ("stage_completed", "fix"),
        ("stage_started", "review"),
        ("stage_completed", "review — approved"),
        ("stage_started", "pr_checks"),
        ("stage_completed", "pr_checks — passed"),
        ("pr_created", "https://github.com/org/bot-farm/pull/80"),
        ("stage_started", "merge"),
        ("stage_completed", "merge — merged"),
        ("task_completed", "All stages passed"),
    ]:
        insert_event(conn, task_id=t1, event_type=ev_type, detail=detail)

    # ---- Task 2: Completed with limit interruption, extra usage ----
    t2 = insert_task(
        conn, ticket_id="SMA-85", title="Refactor worker module",
        project="bot-farm", slot=2, status="completed",
    )
    update_task(
        conn, t2,
        started_at="2026-02-26T14:00:00Z",
        completed_at="2026-02-26T18:00:00Z",
        turns=90, review_iterations=1, limit_interruptions=2,
        pr_url="https://github.com/org/bot-farm/pull/85",
        pipeline_stage="merge", review_state="approved",
        started_on_extra_usage=1,
    )
    insert_stage_run(
        conn, task_id=t2, stage="implement", iteration=1,
        session_id="sess-t2-impl1", turns=40,
        duration_seconds=5400.0, exit_subtype="limit_reached",
        was_limit_restart=False,
        input_tokens=200000, output_tokens=70000,
        total_cost_usd=0.50, context_fill_pct=92.0,
        on_extra_usage=True,
    )
    insert_stage_run(
        conn, task_id=t2, stage="implement", iteration=2,
        session_id="sess-t2-impl2", turns=30,
        duration_seconds=3600.0, exit_subtype="completed",
        was_limit_restart=True,
        input_tokens=150000, output_tokens=50000,
        total_cost_usd=0.35, context_fill_pct=85.0,
        on_extra_usage=True,
    )
    insert_stage_run(
        conn, task_id=t2, stage="review", iteration=1,
        session_id="sess-t2-rev", turns=8,
        duration_seconds=800.0, exit_subtype="approved",
        input_tokens=25000, output_tokens=5000,
        total_cost_usd=0.05, context_fill_pct=38.0,
        on_extra_usage=True,
    )
    insert_stage_run(
        conn, task_id=t2, stage="pr_checks", iteration=1,
        session_id="sess-t2-pr", turns=3,
        duration_seconds=400.0, exit_subtype="passed",
        input_tokens=8000, output_tokens=2000,
        total_cost_usd=0.015, context_fill_pct=20.0,
    )
    insert_stage_run(
        conn, task_id=t2, stage="merge", iteration=1,
        session_id="sess-t2-merge", turns=1,
        duration_seconds=45.0, exit_subtype="merged",
        input_tokens=2000, output_tokens=500,
        total_cost_usd=0.004, context_fill_pct=8.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 2"),
        ("stage_started", "implement"),
        ("limit_interruption", "5h utilization at 100%"),
        ("stage_started", "implement (restart)"),
        ("limit_interruption", "5h utilization at 100%"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — approved"),
        ("stage_started", "pr_checks"),
        ("stage_completed", "pr_checks — passed"),
        ("pr_created", "https://github.com/org/bot-farm/pull/85"),
        ("stage_started", "merge"),
        ("stage_completed", "merge — merged"),
        ("task_completed", "All stages passed"),
    ]:
        insert_event(conn, task_id=t2, event_type=ev_type, detail=detail)

    # ---- Task 3: Failed during review ----
    t3 = insert_task(
        conn, ticket_id="SMA-99", title="Fix flaky test",
        project="bot-farm", slot=3, status="failed",
    )
    update_task(
        conn, t3,
        started_at="2026-02-27T06:00:00Z",
        completed_at="2026-02-27T07:30:00Z",
        turns=25, review_iterations=3,
        failure_reason="Max review iterations exceeded",
        pipeline_stage="review", review_state="needs_changes",
    )
    insert_stage_run(
        conn, task_id=t3, stage="implement", iteration=1,
        session_id="sess-t3-impl", turns=15,
        duration_seconds=1800.0, exit_subtype="completed",
        input_tokens=60000, output_tokens=20000,
        total_cost_usd=0.13, context_fill_pct=50.0,
    )
    insert_stage_run(
        conn, task_id=t3, stage="review", iteration=1,
        session_id="sess-t3-rev1", turns=4,
        duration_seconds=500.0, exit_subtype="needs_changes",
        input_tokens=15000, output_tokens=4000,
        total_cost_usd=0.03, context_fill_pct=30.0,
    )
    insert_stage_run(
        conn, task_id=t3, stage="fix", iteration=1,
        session_id="sess-t3-fix1", turns=8,
        duration_seconds=700.0, exit_subtype="completed",
        input_tokens=35000, output_tokens=12000,
        total_cost_usd=0.08, context_fill_pct=48.0,
    )
    insert_stage_run(
        conn, task_id=t3, stage="review", iteration=2,
        session_id="sess-t3-rev2", turns=3,
        duration_seconds=400.0, exit_subtype="needs_changes",
        input_tokens=12000, output_tokens=3000,
        total_cost_usd=0.02, context_fill_pct=28.0,
    )
    insert_stage_run(
        conn, task_id=t3, stage="fix", iteration=2,
        session_id="sess-t3-fix2", turns=6,
        duration_seconds=600.0, exit_subtype="completed",
        input_tokens=30000, output_tokens=10000,
        total_cost_usd=0.07, context_fill_pct=45.0,
    )
    insert_stage_run(
        conn, task_id=t3, stage="review", iteration=3,
        session_id="sess-t3-rev3", turns=3,
        duration_seconds=350.0, exit_subtype="needs_changes",
        input_tokens=10000, output_tokens=2500,
        total_cost_usd=0.02, context_fill_pct=25.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 3"),
        ("stage_started", "implement"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — needs_changes"),
        ("stage_started", "fix"),
        ("stage_completed", "fix"),
        ("stage_started", "review"),
        ("stage_completed", "review — needs_changes"),
        ("stage_started", "fix"),
        ("stage_completed", "fix"),
        ("stage_started", "review"),
        ("stage_completed", "review — needs_changes"),
        ("task_failed", "Max review iterations exceeded"),
    ]:
        insert_event(conn, task_id=t3, event_type=ev_type, detail=detail)

    # ---- Task 4: Completed — web-app project, single review pass ----
    t4 = insert_task(
        conn, ticket_id="WEB-45", title="Add search endpoint",
        project="web-app", slot=1, status="completed",
    )
    update_task(
        conn, t4,
        started_at="2026-02-25T09:00:00Z",
        completed_at="2026-02-25T10:15:00Z",
        turns=30, review_iterations=1,
        pr_url="https://github.com/org/web-app/pull/45",
        pipeline_stage="merge", review_state="approved",
    )
    insert_stage_run(
        conn, task_id=t4, stage="implement", iteration=1,
        session_id="sess-t4-impl", turns=20,
        duration_seconds=2400.0, exit_subtype="completed",
        input_tokens=80000, output_tokens=25000,
        total_cost_usd=0.18, context_fill_pct=60.0,
    )
    insert_stage_run(
        conn, task_id=t4, stage="review", iteration=1,
        session_id="sess-t4-rev", turns=6,
        duration_seconds=700.0, exit_subtype="approved",
        input_tokens=18000, output_tokens=4000,
        total_cost_usd=0.04, context_fill_pct=32.0,
    )
    insert_stage_run(
        conn, task_id=t4, stage="pr_checks", iteration=1,
        session_id="sess-t4-pr", turns=2,
        duration_seconds=250.0, exit_subtype="passed",
        input_tokens=4000, output_tokens=800,
        total_cost_usd=0.008, context_fill_pct=12.0,
    )
    insert_stage_run(
        conn, task_id=t4, stage="merge", iteration=1,
        session_id="sess-t4-merge", turns=1,
        duration_seconds=30.0, exit_subtype="merged",
        input_tokens=1500, output_tokens=400,
        total_cost_usd=0.003, context_fill_pct=8.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 1"),
        ("stage_started", "implement"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — approved"),
        ("stage_started", "pr_checks"),
        ("stage_completed", "pr_checks — passed"),
        ("pr_created", "https://github.com/org/web-app/pull/45"),
        ("stage_started", "merge"),
        ("stage_completed", "merge — merged"),
        ("task_completed", "All stages passed"),
    ]:
        insert_event(conn, task_id=t4, event_type=ev_type, detail=detail)

    # ---- Task 5: In-progress in web-app (paused_manual mid-fix) ----
    # Matches web-app/2 slot: paused_manual in fix stage after completing
    # implement and review.  Paused by usage limit during fix.
    t5 = insert_task(
        conn, ticket_id="WEB-48", title="Migrate to React 19",
        project="web-app", slot=2, status="pending",
    )
    update_task(
        conn, t5,
        started_at="2026-02-27T05:00:00Z",
        turns=38, review_iterations=1,
        pipeline_stage="fix", review_state="needs_changes",
    )
    insert_stage_run(
        conn, task_id=t5, stage="implement", iteration=1,
        session_id="sess-t5-impl", turns=25,
        duration_seconds=5400.0, exit_subtype="completed",
        input_tokens=100000, output_tokens=35000,
        total_cost_usd=0.24, context_fill_pct=72.0,
    )
    insert_stage_run(
        conn, task_id=t5, stage="review", iteration=1,
        session_id="sess-t5-rev1", turns=6,
        duration_seconds=700.0, exit_subtype="needs_changes",
        input_tokens=20000, output_tokens=5000,
        total_cost_usd=0.04, context_fill_pct=35.0,
    )
    insert_stage_run(
        conn, task_id=t5, stage="fix", iteration=1,
        session_id="sess-t5-fix", turns=7,
        duration_seconds=900.0,
        input_tokens=30000, output_tokens=10000,
        total_cost_usd=0.07, context_fill_pct=48.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 2"),
        ("stage_started", "implement"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — needs_changes"),
        ("stage_started", "fix"),
        ("limit_interruption", "5h utilization at 100%"),
    ]:
        insert_event(conn, task_id=t5, event_type=ev_type, detail=detail)

    # ---- Task 6: In-progress, paused by usage limit ----
    # Matches data-pipeline/1 slot: paused_limit during implement.
    t6 = insert_task(
        conn, ticket_id="DP-10", title="Add data validation step",
        project="data-pipeline", slot=1, status="pending",
    )
    update_task(
        conn, t6,
        started_at="2026-02-27T07:00:00Z",
        turns=18, limit_interruptions=1,
        pipeline_stage="implement",
    )
    insert_stage_run(
        conn, task_id=t6, stage="implement", iteration=1,
        session_id="sess-t6-impl", turns=18,
        duration_seconds=2700.0, exit_subtype="limit_reached",
        input_tokens=65000, output_tokens=20000,
        total_cost_usd=0.15, context_fill_pct=52.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 1"),
        ("stage_started", "implement"),
        ("limit_interruption", "5h utilization at 100%"),
    ]:
        insert_event(conn, task_id=t6, event_type=ev_type, detail=detail)

    # ---- Task 7: Completed — data-pipeline project ----
    t7 = insert_task(
        conn, ticket_id="DP-8", title="Optimize batch processing",
        project="data-pipeline", slot=1, status="completed",
    )
    update_task(
        conn, t7,
        started_at="2026-02-24T14:00:00Z",
        completed_at="2026-02-24T15:00:00Z",
        turns=22, review_iterations=1,
        pr_url="https://github.com/org/data-pipeline/pull/8",
        pipeline_stage="merge", review_state="approved",
    )
    insert_stage_run(
        conn, task_id=t7, stage="implement", iteration=1,
        session_id="sess-t7-impl", turns=15,
        duration_seconds=2000.0, exit_subtype="completed",
        input_tokens=55000, output_tokens=18000,
        total_cost_usd=0.12, context_fill_pct=45.0,
    )
    insert_stage_run(
        conn, task_id=t7, stage="review", iteration=1,
        session_id="sess-t7-rev", turns=4,
        duration_seconds=500.0, exit_subtype="approved",
        input_tokens=12000, output_tokens=3000,
        total_cost_usd=0.025, context_fill_pct=22.0,
    )
    insert_stage_run(
        conn, task_id=t7, stage="pr_checks", iteration=1,
        session_id="sess-t7-pr", turns=2,
        duration_seconds=200.0, exit_subtype="passed",
        input_tokens=3000, output_tokens=600,
        total_cost_usd=0.006, context_fill_pct=10.0,
    )
    insert_stage_run(
        conn, task_id=t7, stage="merge", iteration=1,
        session_id="sess-t7-merge", turns=1,
        duration_seconds=25.0, exit_subtype="merged",
        input_tokens=1200, output_tokens=300,
        total_cost_usd=0.002, context_fill_pct=6.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 1"),
        ("stage_started", "implement"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — approved"),
        ("stage_started", "pr_checks"),
        ("stage_completed", "pr_checks — passed"),
        ("pr_created", "https://github.com/org/data-pipeline/pull/8"),
        ("stage_started", "merge"),
        ("stage_completed", "merge — merged"),
        ("task_completed", "All stages passed"),
    ]:
        insert_event(conn, task_id=t7, event_type=ev_type, detail=detail)

    # ---- Task 8: Currently in progress (busy slot) ----
    t8 = insert_task(
        conn, ticket_id="SMA-101", title="Add retry logic",
        project="bot-farm", slot=1, status="pending",
    )
    update_task(
        conn, t8,
        started_at="2026-02-27T08:00:00Z",
        turns=12,
        pipeline_stage="implement",
    )
    insert_stage_run(
        conn, task_id=t8, stage="implement", iteration=1,
        session_id="sess-bf-101", turns=12,
        duration_seconds=1800.0,
        input_tokens=40000, output_tokens=12000,
        total_cost_usd=0.09, context_fill_pct=35.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 1"),
        ("stage_started", "implement"),
    ]:
        insert_event(conn, task_id=t8, event_type=ev_type, detail=detail)

    # ---- Task 9: In-progress in web-app (mid-fix after first review) ----
    t9 = insert_task(
        conn, ticket_id="WEB-50", title="Dark mode toggle",
        project="web-app", slot=1, status="pending",
    )
    update_task(
        conn, t9,
        started_at="2026-02-27T07:30:00Z",
        turns=28, review_iterations=1,
        pipeline_stage="fix", review_state="needs_changes",
    )
    insert_stage_run(
        conn, task_id=t9, stage="implement", iteration=1,
        session_id="sess-wa-50-impl", turns=18,
        duration_seconds=2800.0, exit_subtype="completed",
        input_tokens=70000, output_tokens=22000,
        total_cost_usd=0.16, context_fill_pct=58.0,
    )
    insert_stage_run(
        conn, task_id=t9, stage="review", iteration=1,
        session_id="sess-wa-50-rev1", turns=5,
        duration_seconds=600.0, exit_subtype="needs_changes",
        input_tokens=16000, output_tokens=3500,
        total_cost_usd=0.03, context_fill_pct=28.0,
    )
    insert_stage_run(
        conn, task_id=t9, stage="fix", iteration=1,
        session_id="sess-wa-50-fix", turns=5,
        duration_seconds=500.0,
        input_tokens=20000, output_tokens=6000,
        total_cost_usd=0.04, context_fill_pct=32.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 1"),
        ("stage_started", "implement"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — needs_changes"),
        ("stage_started", "fix"),
    ]:
        insert_event(conn, task_id=t9, event_type=ev_type, detail=detail)

    # ---- Task 10: Completed, pending slot cleanup ----
    # Matches data-pipeline/2 slot: completed_pending_cleanup.
    t10 = insert_task(
        conn, ticket_id="DP-11", title="Fix CSV parser edge case",
        project="data-pipeline", slot=2, status="completed",
    )
    update_task(
        conn, t10,
        started_at="2026-02-27T02:00:00Z",
        completed_at="2026-02-27T03:30:00Z",
        turns=28, review_iterations=1,
        pr_url="https://github.com/org/data-pipeline/pull/11",
        pipeline_stage="merge", review_state="approved",
    )
    insert_stage_run(
        conn, task_id=t10, stage="implement", iteration=1,
        session_id="sess-t10-impl", turns=18,
        duration_seconds=2400.0, exit_subtype="completed",
        input_tokens=70000, output_tokens=22000,
        total_cost_usd=0.16, context_fill_pct=55.0,
    )
    insert_stage_run(
        conn, task_id=t10, stage="review", iteration=1,
        session_id="sess-t10-rev", turns=5,
        duration_seconds=500.0, exit_subtype="approved",
        input_tokens=15000, output_tokens=3500,
        total_cost_usd=0.03, context_fill_pct=25.0,
    )
    insert_stage_run(
        conn, task_id=t10, stage="pr_checks", iteration=1,
        session_id="sess-t10-pr", turns=2,
        duration_seconds=200.0, exit_subtype="passed",
        input_tokens=4000, output_tokens=800,
        total_cost_usd=0.008, context_fill_pct=12.0,
    )
    insert_stage_run(
        conn, task_id=t10, stage="merge", iteration=1,
        session_id="sess-t10-merge", turns=1,
        duration_seconds=30.0, exit_subtype="merged",
        input_tokens=1500, output_tokens=400,
        total_cost_usd=0.003, context_fill_pct=8.0,
    )
    for ev_type, detail in [
        ("task_dispatched", "Dispatched to slot 2"),
        ("stage_started", "implement"),
        ("stage_completed", "implement"),
        ("stage_started", "review"),
        ("stage_completed", "review — approved"),
        ("stage_started", "pr_checks"),
        ("stage_completed", "pr_checks — passed"),
        ("pr_created", "https://github.com/org/data-pipeline/pull/11"),
        ("stage_started", "merge"),
        ("stage_completed", "merge — merged"),
        ("task_completed", "All stages passed"),
    ]:
        insert_event(conn, task_id=t10, event_type=ev_type, detail=detail)

    conn.commit()


# ---------------------------------------------------------------------------
# Usage snapshots — multiple over time with varying utilization
# ---------------------------------------------------------------------------


def _seed_usage_snapshots(conn):
    snapshots = [
        # Low utilization period
        dict(
            utilization_5h=0.10, utilization_7d=0.15,
            resets_at="2026-02-25T08:00:00Z",
            resets_at_7d="2026-03-01T00:00:00Z",
        ),
        dict(
            utilization_5h=0.25, utilization_7d=0.20,
            resets_at="2026-02-25T13:00:00Z",
            resets_at_7d="2026-03-01T00:00:00Z",
        ),
        # Medium utilization
        dict(
            utilization_5h=0.55, utilization_7d=0.40,
            resets_at="2026-02-26T08:00:00Z",
            resets_at_7d="2026-03-02T00:00:00Z",
        ),
        dict(
            utilization_5h=0.72, utilization_7d=0.55,
            resets_at="2026-02-26T13:00:00Z",
            resets_at_7d="2026-03-02T00:00:00Z",
        ),
        # High utilization — approaching limit
        dict(
            utilization_5h=0.90, utilization_7d=0.70,
            resets_at="2026-02-27T06:00:00Z",
            resets_at_7d="2026-03-03T00:00:00Z",
        ),
        # At limit — triggers extra usage
        dict(
            utilization_5h=1.0, utilization_7d=0.82,
            resets_at="2026-02-27T08:00:00Z",
            resets_at_7d="2026-03-03T00:00:00Z",
            extra_usage_enabled=True,
            extra_usage_monthly_limit=50.0,
            extra_usage_used_credits=12.50,
            extra_usage_utilization=0.25,
        ),
        # Extra usage active, moderate spend
        dict(
            utilization_5h=1.0, utilization_7d=0.85,
            resets_at="2026-02-27T10:00:00Z",
            resets_at_7d="2026-03-03T00:00:00Z",
            extra_usage_enabled=True,
            extra_usage_monthly_limit=50.0,
            extra_usage_used_credits=18.75,
            extra_usage_utilization=0.375,
        ),
        # Latest snapshot — utilization dropping after reset
        dict(
            utilization_5h=0.35, utilization_7d=0.88,
            resets_at="2026-02-27T15:00:00Z",
            resets_at_7d="2026-03-03T00:00:00Z",
            extra_usage_enabled=True,
            extra_usage_monthly_limit=50.0,
            extra_usage_used_credits=18.75,
            extra_usage_utilization=0.375,
        ),
    ]
    for snap in snapshots:
        insert_usage_snapshot(conn, **snap)

    conn.commit()


# ---------------------------------------------------------------------------
# Queue entries — multi-project with priorities and blockers
# ---------------------------------------------------------------------------


def _seed_queue_entries(conn):
    # bot-farm queue
    bf_candidates = [
        SimpleNamespace(
            identifier="SMA-102", title="Add webhook support",
            priority=2, sort_order=1.0,
            url="https://linear.app/team/issue/SMA-102",
        ),
        SimpleNamespace(
            identifier="SMA-103", title="Improve error messages",
            priority=3, sort_order=2.0,
            url="https://linear.app/team/issue/SMA-103",
            blocked_by=["SMA-102"],
        ),
        SimpleNamespace(
            identifier="SMA-104", title="Update documentation",
            priority=4, sort_order=3.0,
            url="https://linear.app/team/issue/SMA-104",
        ),
    ]
    save_queue_entries(
        conn, "bot-farm", bf_candidates,
        snapshot_at="2026-02-27T09:00:00Z",
    )

    # web-app queue
    wa_candidates = [
        SimpleNamespace(
            identifier="WEB-51", title="Fix login redirect",
            priority=1, sort_order=0.5,
            url="https://linear.app/team/issue/WEB-51",
        ),
        SimpleNamespace(
            identifier="WEB-52", title="Add password reset flow",
            priority=2, sort_order=1.5,
            url="https://linear.app/team/issue/WEB-52",
        ),
    ]
    save_queue_entries(
        conn, "web-app", wa_candidates,
        snapshot_at="2026-02-27T09:00:00Z",
    )

    # data-pipeline queue (empty — no pending tickets)
    save_queue_entries(
        conn, "data-pipeline", [],
        snapshot_at="2026-02-27T09:00:00Z",
    )


# ---------------------------------------------------------------------------
# Dispatch state — configurable pause state
# ---------------------------------------------------------------------------


def _seed_dispatch_state(conn):
    save_dispatch_state(
        conn,
        paused=False,
        supervisor_heartbeat="2026-02-27T09:30:00Z",
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    else:
        path = Path("test_dashboard.db")
    seed_comprehensive_db(path)
    print(f"Seeded database at {path}")
