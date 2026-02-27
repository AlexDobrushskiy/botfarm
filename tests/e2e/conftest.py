"""Playwright E2E test fixtures for the botfarm dashboard.

Provides session-scoped fixtures that:
- Seed a comprehensive test database
- Start the dashboard on a random available port via uvicorn in a daemon thread
- Supply a Playwright page pointed at the test dashboard
- Provide helpers for modifying test state (e.g. dispatch pause)
"""

from __future__ import annotations

import socket
import sqlite3
import threading
import time

import httpx
import pytest
import uvicorn

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


@pytest.fixture(scope="session")
def seeded_db(tmp_path_factory):
    """Create a comprehensive test database using the seed script."""
    db_path = tmp_path_factory.mktemp("e2e") / "test_dashboard.db"
    seed_comprehensive_db(db_path)
    return db_path


@pytest.fixture(scope="session")
def live_server(seeded_db):
    """Start the dashboard on a random free port in a daemon thread.

    Yields the base URL. The server shuts down after the test session.
    """
    port = _find_free_port()
    app = create_app(db_path=seeded_db)

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
