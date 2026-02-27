"""Playwright E2E test fixtures for the botfarm dashboard.

Provides session-scoped fixtures that:
- Seed a comprehensive test database
- Start the dashboard on a random available port via uvicorn in a daemon thread
- Supply a Playwright page pointed at the test dashboard
- Provide helpers for modifying test state (e.g. dispatch pause)
- Provide a BotfarmConfig so the config page is fully functional
- Provide pause/resume/update callbacks for supervisor control tests
- Create test log files for SSE streaming tests
"""

from __future__ import annotations

import json
import socket
import sqlite3
import threading
import time
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
import uvicorn
import yaml

from botfarm.config import (
    AgentsConfig,
    BotfarmConfig,
    DashboardConfig,
    DatabaseConfig,
    LinearConfig,
    NotificationsConfig,
    ProjectConfig,
    UsageLimitsConfig,
)
from botfarm.dashboard import create_app
from tests.seed_test_db import seed_comprehensive_db

# All tests in this directory require the playwright marker.
pytestmark = pytest.mark.playwright

TEST_HOST = "127.0.0.1"


def _find_free_port() -> int:
    """Find a free TCP port by binding to port 0 and reading the assignment."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def _make_test_config(config_path: Path) -> BotfarmConfig:
    """Create a BotfarmConfig for E2E tests with sensible defaults."""
    cfg = BotfarmConfig(
        projects=[
            ProjectConfig(
                name="bot-farm",
                linear_team="Smart AI Coach",
                base_dir="/tmp/bot-farm",
                worktree_prefix="botfarm-slot-",
                slots=[1, 2, 3],
                linear_project="Bot farm",
            ),
            ProjectConfig(
                name="web-app",
                linear_team="Smart AI Coach",
                base_dir="/tmp/web-app",
                worktree_prefix="webapp-slot-",
                slots=[1, 2],
            ),
        ],
        linear=LinearConfig(
            api_key="lin_api_1234567890abcdef1234567890abcdef",
            workspace="test-workspace",
            poll_interval_seconds=30,
            exclude_tags=["Human"],
            comment_on_failure=True,
            comment_on_completion=False,
            comment_on_limit_pause=False,
        ),
        database=DatabaseConfig(path="/tmp/test.db"),
        usage_limits=UsageLimitsConfig(
            enabled=True,
            pause_five_hour_threshold=0.85,
            pause_seven_day_threshold=0.90,
        ),
        dashboard=DashboardConfig(enabled=True, host="0.0.0.0", port=8420),
        agents=AgentsConfig(
            max_review_iterations=3,
            max_ci_retries=2,
            timeout_minutes={"implement": 120, "review": 30, "fix": 60},
            timeout_grace_seconds=10,
        ),
        notifications=NotificationsConfig(
            webhook_url="https://hooks.slack.com/services/test",
            webhook_format="slack",
            rate_limit_seconds=300,
        ),
        source_path=str(config_path),
    )
    # Write the initial config YAML so saves work
    config_dict = {
        "projects": [
            {
                "name": p.name,
                "linear_team": p.linear_team,
                "base_dir": p.base_dir,
                "worktree_prefix": p.worktree_prefix,
                "slots": list(p.slots),
                "linear_project": p.linear_project,
            }
            for p in cfg.projects
        ],
        "linear": {
            "api_key": cfg.linear.api_key,
            "workspace": cfg.linear.workspace,
            "poll_interval_seconds": cfg.linear.poll_interval_seconds,
            "exclude_tags": list(cfg.linear.exclude_tags),
            "comment_on_failure": cfg.linear.comment_on_failure,
            "comment_on_completion": cfg.linear.comment_on_completion,
            "comment_on_limit_pause": cfg.linear.comment_on_limit_pause,
        },
        "usage_limits": {
            "enabled": cfg.usage_limits.enabled,
            "pause_five_hour_threshold": cfg.usage_limits.pause_five_hour_threshold,
            "pause_seven_day_threshold": cfg.usage_limits.pause_seven_day_threshold,
        },
        "agents": {
            "max_review_iterations": cfg.agents.max_review_iterations,
            "max_ci_retries": cfg.agents.max_ci_retries,
            "timeout_minutes": dict(cfg.agents.timeout_minutes),
            "timeout_grace_seconds": cfg.agents.timeout_grace_seconds,
        },
        "notifications": {
            "webhook_url": cfg.notifications.webhook_url,
            "webhook_format": cfg.notifications.webhook_format,
            "rate_limit_seconds": cfg.notifications.rate_limit_seconds,
        },
        "dashboard": {"enabled": True, "host": "0.0.0.0", "port": 8420},
        "database": {"path": ""},
    }
    config_path.write_text(yaml.dump(config_dict, default_flow_style=False))
    return cfg


@pytest.fixture(scope="session")
def seeded_db(tmp_path_factory):
    """Create a comprehensive test database using the seed script."""
    db_path = tmp_path_factory.mktemp("e2e") / "test_dashboard.db"
    seed_comprehensive_db(db_path)
    return db_path


@pytest.fixture(scope="session")
def test_config(tmp_path_factory):
    """Create a test BotfarmConfig with a backing YAML file."""
    config_dir = tmp_path_factory.mktemp("e2e_config")
    config_path = config_dir / "config.yaml"
    return _make_test_config(config_path)


@pytest.fixture(scope="session")
def pause_resume_state():
    """Shared mutable state for tracking pause/resume callback invocations."""
    return {"paused": False, "resumed": False, "updated": False}


@pytest.fixture(scope="session")
def test_logs_dir(tmp_path_factory):
    """Create test log files for SSE streaming tests.

    Produces NDJSON log files matching the expected directory layout:
    ``logs_dir/TICKET_ID/STAGE-TIMESTAMP.log``
    """
    logs_dir = tmp_path_factory.mktemp("e2e_logs")

    # SMA-101 is the "busy" slot in implement stage — create a log file for it.
    sma101_dir = logs_dir / "SMA-101"
    sma101_dir.mkdir()
    log_lines = [
        json.dumps({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Let me implement the retry logic."},
                ],
            },
        }),
        json.dumps({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "tool_use", "name": "Read",
                     "input": {"file_path": "/src/worker.py"}},
                ],
            },
        }),
        json.dumps({
            "type": "user",
            "message": {
                "content": [
                    {"type": "tool_result", "is_error": False,
                     "content": "def retry(): pass"},
                ],
            },
        }),
        json.dumps({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "I see the current implementation."},
                    {"type": "tool_use", "name": "Edit",
                     "input": {"file_path": "/src/worker.py",
                               "old_string": "pass", "new_string": "retry()"}},
                ],
            },
        }),
    ]
    impl_log = sma101_dir / "implement-20260227-080000.log"
    impl_log.write_text("\n".join(log_lines) + "\n")

    # SMA-80 is a completed task — create a log file for static display.
    sma80_dir = logs_dir / "SMA-80"
    sma80_dir.mkdir()
    completed_lines = [
        json.dumps({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Adding config validation."},
                ],
            },
        }),
        json.dumps({
            "type": "result",
            "num_turns": 35, "duration_ms": 3600000,
            "subtype": "completed", "is_error": False,
        }),
    ]
    (sma80_dir / "implement-20260226-100000.log").write_text(
        "\n".join(completed_lines) + "\n"
    )
    (sma80_dir / "review-20260226-110000.log").write_text(
        json.dumps({
            "type": "assistant",
            "message": {"content": [{"type": "text", "text": "Reviewing changes."}]},
        }) + "\n"
    )

    return logs_dir


@pytest.fixture(scope="session")
def live_server(seeded_db, test_config, pause_resume_state, test_logs_dir):
    """Start the dashboard on a random free port in a daemon thread.

    Yields the base URL. The server shuts down after the test session.
    Provides config, pause/resume callbacks, and mocked update checking.
    """
    port = _find_free_port()

    def on_pause():
        pause_resume_state["paused"] = True

    def on_resume():
        pause_resume_state["resumed"] = True

    def on_update():
        pause_resume_state["updated"] = True

    app = create_app(
        db_path=seeded_db,
        botfarm_config=test_config,
        logs_dir=test_logs_dir,
        on_pause=on_pause,
        on_resume=on_resume,
        on_update=on_update,
    )

    # Patch commits_behind persistently so the update banner shows
    # "3 commits behind" at runtime (not just at app creation time).
    patcher = patch("botfarm.dashboard.commits_behind", return_value=3)
    patcher.start()

    config = uvicorn.Config(
        app,
        host=TEST_HOST,
        port=port,
        log_level="warning",
    )
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for the server to be ready
    base_url = f"http://{TEST_HOST}:{port}"
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(base_url, timeout=1)
            if resp.status_code == 200:
                break
        except (httpx.ConnectError, httpx.TimeoutException):
            time.sleep(0.1)
    else:
        raise RuntimeError(f"Dashboard did not start within 10s on {base_url}")

    yield base_url

    server.should_exit = True
    thread.join(timeout=5)
    patcher.stop()


@pytest.fixture()
def page(live_server, page):
    """Playwright page navigated to the test dashboard."""
    page.goto(live_server)
    return page


@pytest.fixture()
def set_dispatch_paused(seeded_db):
    """Temporarily set dispatch state to paused in the test database.

    Restores the original running state after the test completes.
    """
    conn = sqlite3.connect(seeded_db)
    conn.execute(
        "UPDATE dispatch_state SET paused = 1, pause_reason = 'manual_pause'"
    )
    conn.commit()
    yield
    conn.execute(
        "UPDATE dispatch_state SET paused = 0, pause_reason = NULL"
    )
    conn.commit()
    conn.close()
