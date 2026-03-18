"""Shared test utility functions and data builders.

These are plain functions (not pytest fixtures) that construct test data.
Import them explicitly in test files that need them.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from botfarm.agent import AgentResult
from botfarm.agent_claude import _claude_result_to_agent_result
from botfarm.agent_codex import _codex_result_to_agent_result as _codex_to_ar
from botfarm.codex import CodexResult
from botfarm.config import (
    BotfarmConfig,
    DatabaseConfig,
    LinearConfig,
    ProjectConfig,
)
from botfarm.db import save_dispatch_state, upsert_slot
from botfarm.bugtracker import Issue as LinearIssue
from botfarm.worker import ClaudeResult


# ---------------------------------------------------------------------------
# Config builders
# ---------------------------------------------------------------------------


def make_config(tmp_path: Path) -> BotfarmConfig:
    """Build a minimal valid BotfarmConfig rooted in tmp_path."""
    return BotfarmConfig(
        projects=[
            ProjectConfig(
                name="test-project",
                linear_team="TST",
                base_dir=str(tmp_path / "repo"),
                worktree_prefix="test-project-slot-",
                slots=[1, 2],
            ),
        ],
        linear=LinearConfig(
            api_key="test-key",
            poll_interval_seconds=10,
            exclude_tags=["Human"],
        ),
        database=DatabaseConfig(),
    )


# ---------------------------------------------------------------------------
# Linear issue builder
# ---------------------------------------------------------------------------


def make_issue(
    *,
    id: str = "issue-uuid-1",
    identifier: str = "TST-1",
    title: str = "Test ticket",
    priority: int = 2,
    labels: list[str] | None = None,
    assignee_id: str | None = None,
    sort_order: float = 0.0,
    blocked_by: list[str] | None = None,
    children_states: list[tuple[str, str]] | None = None,
) -> LinearIssue:
    """Build a LinearIssue with sensible defaults."""
    return LinearIssue(
        id=id,
        identifier=identifier,
        title=title,
        priority=priority,
        url=f"https://linear.app/test/{identifier}",
        assignee_id=assignee_id,
        labels=labels or [],
        sort_order=sort_order,
        blocked_by=blocked_by,
        children_states=children_states,
    )


# ---------------------------------------------------------------------------
# Claude / Codex result builders
# ---------------------------------------------------------------------------


def make_claude_json(
    session_id="sess-abc",
    num_turns=5,
    duration_ms=12000,
    subtype="tool_use",
    result="Done",
    is_error=False,
) -> str:
    """Build a JSON string mimicking Claude subprocess output."""
    return json.dumps(
        {
            "session_id": session_id,
            "num_turns": num_turns,
            "duration_ms": duration_ms,
            "subtype": subtype,
            "result": result,
            "is_error": is_error,
        }
    )


def make_claude_result(text: str = "done", is_error: bool = False) -> ClaudeResult:
    """Build a ClaudeResult with sensible defaults."""
    return ClaudeResult(
        session_id="s-test",
        num_turns=5,
        duration_seconds=15.0,
        exit_subtype="tool_use",
        result_text=text,
        is_error=is_error,
    )


def make_codex_result(text: str = "done", is_error: bool = False, thread_id: str = "t-test") -> CodexResult:
    """Build a CodexResult with sensible defaults."""
    return CodexResult(
        thread_id=thread_id,
        num_turns=3,
        duration_seconds=10.0,
        result_text=text,
        is_error=is_error,
        input_tokens=1000,
        output_tokens=500,
        cached_input_tokens=200,
    )


def make_agent_result(
    text: str = "done",
    is_error: bool = False,
    session_id: str = "s-test",
    num_turns: int = 5,
    duration_seconds: float = 15.0,
    **extra_fields,
) -> AgentResult:
    """Build an AgentResult with sensible defaults."""
    return AgentResult(
        session_id=session_id,
        num_turns=num_turns,
        duration_seconds=duration_seconds,
        result_text=text,
        is_error=is_error,
        extra={"exit_subtype": "tool_use", **extra_fields},
    )


def claude_result_to_agent(cr: "ClaudeResult") -> AgentResult:
    """Convert a ClaudeResult to AgentResult (convenience for tests)."""
    return _claude_result_to_agent_result(cr)


def codex_result_to_agent(cr: CodexResult, model: str | None = None) -> AgentResult:
    """Convert a CodexResult to AgentResult (convenience for tests)."""
    return _codex_to_ar(cr, model)


# ---------------------------------------------------------------------------
# Slot builders and seeders
# ---------------------------------------------------------------------------


def make_slot(project, slot_id, status="free", **overrides):
    """Create a slot dict with sensible defaults."""
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
    }
    slot.update(overrides)
    return slot


def seed_slot(conn, project, slot_id, status="free", **overrides):
    """Seed a single slot row in the DB."""
    slot = make_slot(project, slot_id, status, **overrides)
    upsert_slot(conn, slot)


def seed_slots(db_path, slots, *, dispatch_paused=False, dispatch_pause_reason=None):
    """Seed the database with slot rows and optional dispatch state (path-based)."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    for slot in slots:
        upsert_slot(conn, slot)
    save_dispatch_state(
        conn,
        paused=dispatch_paused,
        reason=dispatch_pause_reason,
    )
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# CLI test helpers
# ---------------------------------------------------------------------------


def mock_resolve(db_path, config=None):
    """Return a monkeypatch-compatible _resolve_paths replacement."""
    return lambda _: (db_path, config)
